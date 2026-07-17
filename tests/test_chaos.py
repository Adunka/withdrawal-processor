"""Chaos: the closest a unit test gets to a bad week in production.

Sixty withdrawals, a fleet of workers on real threads, a network that lies
on a seeded schedule, workers executed at random crash points and replaced,
duplicate submissions thrown in mid-flight - then the faults stop, the
survivors drain the queue, and a verifier cross-examines the database
against the chain's own records.

The invariants are named, because "assert stuff" is how chaos tests rot:

  I1  liveness      every accepted operation reached a terminal state
  I2  accounting    every tx the chain ever accepted belongs to exactly one
                    operation's history (no orphan sends)
  I3  THE invariant at most one on-chain tx per operation, and successful
                    transfers == 1 exactly for CONFIRMED operations
  I4  conservation  sum of on-chain transfer amounts == sum of confirmed
                    operation amounts
  I5  intake        one row per operation_id despite duplicate submissions
  I6  audit         every recorded transition is legal per the state table;
                    every rebuild carries expiry evidence

Deterministic-ish by seed; the seed prints on failure so a bad run can be
replayed instead of shrugged at.
"""

import os
import random
import uuid
import threading
import time
import unittest

from sluice import crashpoints as cp
from sluice.crashpoints import SimulatedCrash
from sluice.service import submit_withdrawal
from sluice.states import TERMINAL, OpState, can_transition
from sluice.tron.mocknet import Fault
from sluice.worker import Worker

from .helpers import World, test_address

SEED = int(os.environ.get("SLUICE_CHAOS_SEED", "20260717"))
N_OPS = 60
N_WORKERS = 6
DUP_EVERY = 7          # every 7th submission is repeated concurrently
# Generous on purpose: a healthy run settles in a couple of seconds, but CI
# runners are shared machines that occasionally stall for tens of seconds,
# and a liveness check must not mistake a noisy neighbour for a livelock.
WALL_TIMEOUT_S = 120.0


class TestChaos(unittest.TestCase):
    maxDiff = None

    def test_sixty_withdrawals_versus_a_hostile_universe(self):
        rng = random.Random(SEED)
        w = World()

        # -- a network with opinions ---------------------------------------
        menu = [Fault.TIMEOUT_ACCEPTED, Fault.TIMEOUT_DROPPED, Fault.BUSY,
                Fault.UNKNOWN_CODE, Fault.QUERY_TIMEOUT]
        # the accept-then-timeout fault is the whole reason this project
        # exists, so one is dealt unconditionally - a random hand may
        # legally contain none, and the final assert would flake by seed
        w.node.script("broadcast", Fault.TIMEOUT_ACCEPTED,
                      *(rng.choice(menu[:4]) for _ in range(24)))
        w.node.script("query", *(Fault.QUERY_TIMEOUT for _ in range(10)))
        w.node.script("receipt", Fault.REVERT_RECEIPT, Fault.REVERT_RECEIPT)

        # -- workers that die at work and get replaced ----------------------
        for point in (cp.BROADCAST_PRE_SEND, cp.BROADCAST_POST_SEND,
                      cp.SIGN_MID, cp.CONFIRM_PRE_WRITE):
            w.crash.arm(point, times=2)

        stop = threading.Event()
        badge = iter(range(10_000))

        def employ():
            """Run workers until told to stop; a SimulatedCrash kills the
            current one dead (no cleanup!) and a replacement is hired."""
            while not stop.is_set():
                worker = Worker(w.store, w.pipeline, w.cfg, name=f"chaos-{next(badge)}")
                try:
                    while not stop.is_set():
                        if worker.run_once() == 0:
                            time.sleep(0.001)
                except SimulatedCrash:
                    continue  # rest in peace; next hire picks up the leases later

        crew = [threading.Thread(target=employ, daemon=True) for _ in range(N_WORKERS)]
        [t.start() for t in crew]

        # -- load, with duplicates fired concurrently -----------------------
        submitted = []
        for i in range(N_OPS):
            payload = w.payload(
                to_address=test_address(i + 10),
                amount=str(rng.randint(1, 5000)),
            )
            if i % DUP_EVERY == 0:
                dup = threading.Thread(
                    target=submit_withdrawal, args=(w.store, w.cfg, dict(payload))
                )
                dup.start()
                w.submit(**payload)
                dup.join()
            else:
                w.submit(**payload)
            submitted.append(uuid.UUID(payload["operation_id"]))

        # -- let time and blocks pass until everything settles ---------------
        deadline = time.monotonic() + WALL_TIMEOUT_S
        while time.monotonic() < deadline:
            w.clock.advance(rng.choice([2, 3, 5, 40]))  # 40s jumps push some
            w.node.produce_blocks(2)                    # txs into expiry paths
            time.sleep(0.005)
            ops = [w.op(oid) for oid in submitted]
            if all(o.state in TERMINAL for o in ops):
                break
        stop.set()
        [t.join(timeout=30) for t in crew]
        # The verifier reads shared state without locks - legitimate ONLY if
        # every worker is provably dead first. A wedged thread here must be
        # its own loud failure, not a source of corrupted-looking invariants.
        self.assertFalse([t.name for t in crew if t.is_alive()],
                         "workers still alive after stop - verifier would race them")

        # ================= the verifier =================
        ops = {oid: w.op(oid) for oid in submitted}
        chain_success = w.node.successful_txids()
        chain_included = w.node.included_txids()

        # I1 liveness
        stuck = {oid: o.state for oid, o in ops.items() if o.state not in TERMINAL}
        self.assertFalse(stuck, f"seed={SEED}: never finished: {stuck}")

        # I2 accounting: chain knows no txid that our histories don't
        all_history = {}
        for oid in submitted:
            for t in w.store.all_txids(oid):
                self.assertNotIn(t, all_history, f"seed={SEED}: txid shared by two ops")
                all_history[t] = oid
        accepted_ever = {e.txid for e in w.node.broadcast_log
                         if e.outcome in ("accepted", "timeout-accepted")}
        self.assertTrue(accepted_ever <= set(all_history),
                        f"seed={SEED}: chain accepted txids nobody admits to")

        # I3 the whole point
        for oid, o in ops.items():
            history = w.store.all_txids(oid)
            self.assertLessEqual(len(chain_included & history), 1,
                                 f"seed={SEED}: DOUBLE SPEND on {oid}")
            landed = len(chain_success & history)
            if o.state == OpState.CONFIRMED:
                self.assertEqual(landed, 1, f"seed={SEED}: confirmed {oid} paid {landed} times")
            else:
                self.assertEqual(landed, 0,
                                 f"seed={SEED}: {o.state} op {oid} moved money anyway")

        # I4 conservation of money
        onchain_total = sum(
            int(w.node.tx_amount(t)) for t in chain_success if t in all_history
        )
        confirmed_total = sum(
            o.amount_units for o in ops.values() if o.state == OpState.CONFIRMED
        )
        self.assertEqual(onchain_total, confirmed_total, f"seed={SEED}")

        # I5 intake held under duplicate fire
        self.assertEqual(len(w.store.all_operations()), N_OPS)

        # I6 audit log is coherent
        for oid in submitted:
            events = w.store.events(oid)
            prev = None
            for e in events:
                if prev is not None and e.to_state != prev:
                    self.assertTrue(can_transition(prev, e.to_state),
                                    f"seed={SEED}: illegal {prev}->{e.to_state} on {oid}")
                prev = e.to_state
                if "retired_txid" in e.detail:
                    self.assertIn("expiry_evidence", e.detail,
                                  f"seed={SEED}: rebuild without evidence on {oid}")

        # a chaos run that never actually hurt anything proves nothing;
        # make sure the universe really was hostile
        self.assertGreaterEqual(len(w.crash.tripped), 4, "crash points never fired")
        outcomes = {e.outcome for e in w.node.broadcast_log}
        self.assertIn("timeout-accepted", outcomes, "the nastiest fault never fired")


if __name__ == "__main__":
    unittest.main()
