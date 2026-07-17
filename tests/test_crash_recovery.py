"""The reason this project exists.

Every test here kills a worker (or the network's honesty) at a specific
instant around the broadcast, lets recovery run, and then asks the only
question that matters: did the money move exactly once?

Recovery is never a special code path in these tests - it is just the next
worker claiming an operation whose lease ran out. If these pass, "the
process died mid-operation" is an inconvenience, not an incident.
"""

import unittest

from sluice import crashpoints as cp
from sluice.states import OpState
from sluice.tron.mocknet import Fault

from .helpers import World


def money_moved_exactly_once(test: unittest.TestCase, w: World, op_id) -> None:
    """The invariant, spelled out: of every txid this operation was EVER
    assigned (event log, not current row), exactly one is on chain with a
    SUCCESS receipt, and no txid was accepted by the network twice."""
    txids = w.store.all_txids(op_id)
    landed = w.node.successful_txids() & txids
    test.assertEqual(len(landed), 1, f"expected exactly one landed tx, got {landed}")
    for t in txids:
        test.assertLessEqual(w.node.accepted_count(t), 1)


class TestCrashAroundBroadcast(unittest.TestCase):
    def finish(self, w: World, op_id) -> None:
        w.drain("second-shift")
        w.settle_chain()
        w.drain("second-shift")
        self.assertEqual(w.op(op_id).state, OpState.CONFIRMED)
        money_moved_exactly_once(self, w, op_id)

    def test_crash_after_write_ahead_before_send(self):
        """DB says `broadcasting`, the wire was never touched. Recovery must
        figure out the tx never left and re-send the SAME bytes."""
        w = World()
        op_id = w.submit().operation.operation_id
        w.crash.arm(cp.BROADCAST_PRE_SEND)

        crashes = w.run_worker_expecting_crash()
        self.assertEqual(len(crashes), 1)
        self.assertEqual(w.op(op_id).state, OpState.BROADCASTING)
        self.assertEqual(w.node.accepted_count(w.op(op_id).txid), 0)  # never sent

        w.expire_leases()   # the dead worker's lease has to run out first
        self.finish(w, op_id)
        # and no rebuild happened: one txid start to finish
        self.assertEqual(len(w.store.all_txids(op_id)), 1)

    def test_crash_after_send_before_recording_result(self):
        """The classic: tx accepted by the network, process dies before the
        DB learns it. The txid was persisted BEFORE the send, so recovery
        looks it up, finds it, and simply carries on. No second spend."""
        w = World()
        op_id = w.submit().operation.operation_id
        w.crash.arm(cp.BROADCAST_POST_SEND)

        crashes = w.run_worker_expecting_crash()
        self.assertEqual(len(crashes), 1)
        op = w.op(op_id)
        self.assertEqual(op.state, OpState.BROADCASTING)          # DB is behind...
        self.assertEqual(w.node.accepted_count(op.txid), 1)       # ...reality

        w.expire_leases()
        self.finish(w, op_id)
        self.assertEqual(len(w.store.all_txids(op_id)), 1)

    def test_timeout_that_was_secretly_an_accept(self):
        """Network says 'timeout', node kept the tx anyway. The worker stays
        alive here - the point is that it must NOT rebuild, NOT resign, and
        must resolve by asking the chain."""
        w = World()
        op_id = w.submit().operation.operation_id
        w.node.script("broadcast", Fault.TIMEOUT_ACCEPTED)

        w.drain()
        op = w.op(op_id)
        self.assertEqual(op.state, OpState.BROADCAST)  # reconciled within the same claim
        self.assertEqual(w.node.accepted_count(op.txid), 1)

        w.settle_chain()
        w.drain()
        money_moved_exactly_once(self, w, op_id)
        self.assertEqual(len(w.store.all_txids(op_id)), 1)

    def test_timeout_that_really_was_a_drop(self):
        """Network says 'timeout' and means it. Same bytes go out again -
        which is safe precisely because they ARE the same bytes."""
        w = World()
        op_id = w.submit().operation.operation_id
        w.node.script("broadcast", Fault.TIMEOUT_DROPPED)

        w.drain()
        w.settle_chain()
        w.drain()
        self.assertEqual(w.op(op_id).state, OpState.CONFIRMED)
        money_moved_exactly_once(self, w, op_id)
        self.assertEqual(len(w.store.all_txids(op_id)), 1)

    def test_server_busy_then_fine(self):
        w = World()
        op_id = w.submit().operation.operation_id
        w.node.script("broadcast", Fault.BUSY)
        w.drain()
        w.settle_chain()
        w.drain()
        self.assertEqual(w.op(op_id).state, OpState.CONFIRMED)
        money_moved_exactly_once(self, w, op_id)


    def test_unknown_broadcast_code_degrades_to_ambiguous(self):
        """A node answering with a code minted after this code shipped must
        not crash the pipeline - and absolutely must not trigger an unproven
        rebuild. The only safe reading of gibberish is 'ambiguous'."""
        w = World()
        op_id = w.submit().operation.operation_id
        w.node.script("broadcast", Fault.UNKNOWN_CODE)

        w.drain()
        w.settle_chain()
        w.drain()

        op = w.op(op_id)
        self.assertEqual(op.state, OpState.CONFIRMED)
        self.assertEqual(op.attempts, 0)                      # no rebuild
        self.assertEqual(len(w.store.all_txids(op_id)), 1)    # no re-sign
        money_moved_exactly_once(self, w, op_id)


class TestRebuildOnlyOnProof(unittest.TestCase):
    def test_rebuild_carries_reviewable_evidence(self):
        """Every rebuild decision must file its proof: the solid chain time
        it saw, the expiration it compared against, the lookup result. An
        auditor should be able to re-run the argument years later."""
        w = World()
        op_id = w.submit().operation.operation_id
        w.drain()                       # accepted, waiting in the mempool
        w.pass_expiration_deadline()    # mempool drops it; chain time passes proof line
        w.drain()                       # rebuild happens here

        rebuilds = [e for e in w.store.events(op_id) if "expiry_evidence" in e.detail]
        self.assertEqual(len(rebuilds), 1)
        ev = rebuilds[0].detail["expiry_evidence"]
        self.assertEqual(ev["node_lookup"], "unknown")
        # the recorded chain time really is past expiration + margin
        from datetime import datetime, timedelta
        solid_t = datetime.fromisoformat(ev["solid_chain_time"])
        deadline = (datetime.fromisoformat(ev["tx_expiration"])
                    + timedelta(seconds=ev["safety_margin_s"]))
        self.assertGreaterEqual(solid_t, deadline)
    def test_dropped_tx_worker_dead_past_expiration_rebuilds_safely(self):
        """Worker dies pre-send; nobody comes back until well after the tx
        expired. The old envelope is now PROVABLY unacceptable to the
        network, so recovery mints a new txid - and only now."""
        w = World()
        op_id = w.submit().operation.operation_id
        w.crash.arm(cp.BROADCAST_PRE_SEND)
        w.run_worker_expecting_crash()
        old_txid = w.op(op_id).txid

        w.pass_expiration_deadline()  # also comfortably expires the lease

        w.drain("night-shift")
        w.settle_chain()
        w.drain("night-shift")

        op = w.op(op_id)
        self.assertEqual(op.state, OpState.CONFIRMED)
        self.assertNotEqual(op.txid, old_txid)
        self.assertEqual(op.attempts, 1)
        self.assertEqual(w.store.all_txids(op_id), {old_txid, op.txid})
        # the retired txid never reached the chain, the new one landed once
        self.assertNotIn(old_txid, w.node.included_txids())
        money_moved_exactly_once(self, w, op_id)

    def test_accepted_but_never_mined_rebuilds_after_expiry(self):
        """Node took the tx, then the mempool ate it (restart, congestion).
        `broadcast` state notices it vanished, waits out the expiration,
        then rebuilds. The dead tx cannot land later - the network's own
        expiration rule guarantees it, and the mock enforces that rule."""
        w = World()
        op_id = w.submit().operation.operation_id
        w.drain()   # accepted, sitting in the mempool
        old_txid = w.op(op_id).txid
        self.assertEqual(w.op(op_id).state, OpState.BROADCAST)

        # no blocks come; the tx expires in the mempool
        w.pass_expiration_deadline()
        w.drain()   # reconcile: unknown + past deadline -> rebuild

        w.settle_chain()
        w.drain()
        op = w.op(op_id)
        self.assertEqual(op.state, OpState.CONFIRMED)
        self.assertNotEqual(op.txid, old_txid)
        self.assertNotIn(old_txid, w.node.included_txids())
        money_moved_exactly_once(self, w, op_id)

    def test_nodes_word_is_not_proof(self):
        """The node answers TRANSACTION_EXPIRATION_ERROR. Tempting to rebuild
        on the spot - and forbidden: the node only vouches that it won't take
        the tx NOW, not that an earlier attempt isn't sitting in a mempool
        somewhere. The rebuild must wait for the full chain-time proof."""
        w = World()
        op_id = w.submit().operation.operation_id
        w.node.script("broadcast", Fault.TIMEOUT_DROPPED, Fault.TIMEOUT_DROPPED)
        w.worker("shift-1").run_once()   # send lost twice; parked in `broadcasting`
        old_txid = w.op(op_id).txid

        # past the tx's expiration, but NOT past expiration + safety margin
        w.clock.advance(w.cfg.tx_ttl_seconds + 1)
        w.node.solidify()
        w.worker("shift-1").run_once()   # resend -> node says EXPIRED

        op = w.op(op_id)
        self.assertEqual(op.state, OpState.BROADCASTING)
        self.assertEqual(op.attempts, 0)                     # held the line
        self.assertIn("awaiting proof", op.last_error)

        # now the proof window closes for real - rebuild becomes legal
        w.pass_expiration_deadline()
        w.drain()
        w.settle_chain()
        w.drain()
        op = w.op(op_id)
        self.assertEqual(op.state, OpState.CONFIRMED)
        self.assertEqual(op.attempts, 1)
        self.assertNotIn(old_txid, w.node.included_txids())
        money_moved_exactly_once(self, w, op_id)

    def test_no_rebuild_before_the_deadline_ever(self):
        """Inside the expiration window a missing tx is 'somewhere, maybe',
        and the only legal moves are wait or re-send the same bytes."""
        w = World()
        op_id = w.submit().operation.operation_id
        w.node.script("broadcast", Fault.TIMEOUT_DROPPED, Fault.TIMEOUT_DROPPED)
        w.drain()
        # two timeouts in, still the same txid, zero rebuilds
        op = w.op(op_id)
        self.assertEqual(op.attempts, 0)
        self.assertEqual(len(w.store.all_txids(op_id)), 1)

    def test_signing_crash_recovers_by_rebuilding(self):
        """Mock signing is local, so a crash between build and sign recovers
        by building fresh. (With a remote signer this becomes a reconcile,
        as the pipeline comments explain at length.)"""
        w = World()
        op_id = w.submit().operation.operation_id
        w.crash.arm(cp.SIGN_MID)
        w.run_worker_expecting_crash()
        self.assertEqual(w.op(op_id).state, OpState.SIGNING)

        w.expire_leases()
        w.drain()
        w.settle_chain()
        w.drain()
        self.assertEqual(w.op(op_id).state, OpState.CONFIRMED)
        money_moved_exactly_once(self, w, op_id)

    def test_gives_up_after_max_build_attempts(self):
        w = World()
        w.cfg.max_build_attempts = 2
        op_id = w.submit().operation.operation_id
        for _ in range(3):
            # every envelope dies unseen in the mempool: accept, expire, repeat
            w.drain()
            w.pass_expiration_deadline()
        w.drain()
        op = w.op(op_id)
        self.assertEqual(op.state, OpState.FAILED)
        self.assertIn("gave up", op.failure_reason)
        # crucially: nothing ever landed, so nothing was paid
        self.assertEqual(len(w.node.included_txids() & w.store.all_txids(op_id)), 0)


if __name__ == "__main__":
    unittest.main()
