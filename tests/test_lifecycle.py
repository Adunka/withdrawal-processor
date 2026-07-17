import unittest

from sluice.states import OpState
from sluice.tron.mocknet import Fault

from .helpers import World


class TestHappyPath(unittest.TestCase):
    def test_requested_to_confirmed(self):
        w = World()
        op_id = w.submit(amount="42").operation.operation_id

        w.drain()                      # -> broadcast, parked waiting for a block
        self.assertEqual(w.op(op_id).state, OpState.BROADCAST)

        w.settle_chain()               # mine + solidify
        w.drain()
        op = w.op(op_id)
        self.assertEqual(op.state, OpState.CONFIRMED)
        self.assertIsNotNone(op.confirmed_at)
        self.assertIsNotNone(op.included_block)

        # exactly one transaction ever existed for this operation, and it
        # landed exactly once
        txids = w.store.all_txids(op_id)
        self.assertEqual(len(txids), 1)
        self.assertEqual(w.node.accepted_count(op.txid), 1)
        self.assertEqual(w.node.included_txids() & txids, {op.txid})

    def test_event_log_tells_the_whole_story(self):
        w = World()
        op_id = w.submit().operation.operation_id
        w.drain()
        w.settle_chain()
        w.drain()

        states = [e.to_state for e in w.store.events(op_id)]
        self.assertEqual(
            states,
            [OpState.REQUESTED, OpState.VALIDATED, OpState.SIGNING, OpState.SIGNING,
             OpState.SIGNED, OpState.BROADCASTING, OpState.BROADCAST, OpState.CONFIRMED],
        )

    def test_solidity_is_respected(self):
        # a block that exists but is not yet solid must not confirm anything
        w = World()
        op_id = w.submit().operation.operation_id
        w.drain()
        w.node.produce_blocks(1)   # included, head advances, NOT solid yet
        w.drain()
        self.assertEqual(w.op(op_id).state, OpState.BROADCAST)
        w.settle_chain()
        w.drain()
        self.assertEqual(w.op(op_id).state, OpState.CONFIRMED)


class TestUnhappyEndings(unittest.TestCase):
    def test_worker_side_rejection(self):
        w = World()
        # sneak a bad amount past intake by inserting directly (as if limits
        # changed between intake and processing)
        res = w.submit(amount="5")
        bad = w.store.get(res.operation.operation_id)
        w.cfg.max_amount_units = 1_000_000  # tighten limits under it
        w.drain()
        op = w.op(bad.operation_id)
        self.assertEqual(op.state, OpState.REJECTED)
        self.assertIn("amount", op.failure_reason)

    def test_reverted_on_chain_is_failed_not_confirmed(self):
        w = World()
        op_id = w.submit().operation.operation_id
        w.node.script("receipt", Fault.REVERT_RECEIPT)
        w.drain()
        w.settle_chain()
        w.drain()
        op = w.op(op_id)
        self.assertEqual(op.state, OpState.FAILED)
        self.assertIn("reverted", op.failure_reason)
        # the tx IS on chain - it just did nothing. The invariant cares about
        # successful transfers, and there were none.
        self.assertEqual(len(w.node.successful_txids() & w.store.all_txids(op_id)), 0)

    def test_terminal_states_are_bricks(self):
        w = World()
        op_id = w.submit().operation.operation_id
        w.drain()
        w.settle_chain()
        w.drain()
        op = w.op(op_id)
        self.assertEqual(op.state, OpState.CONFIRMED)

        # even a correctly-fenced same-state write must bounce off terminal
        got = w.store.cas(
            op.operation_id, op.fencing_token,
            from_state=op.state, to_state=op.state,
            sets={"last_error": "should never land"},
        )
        self.assertIsNone(got)
        self.assertIsNone(w.op(op_id).last_error)


if __name__ == "__main__":
    unittest.main()
