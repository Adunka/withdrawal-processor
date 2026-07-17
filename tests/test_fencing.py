"""Leases say who PROBABLY owns an operation; fencing tokens settle who
ACTUALLY does. These tests are about the gap between the two: a worker that
stalls (GC pause, network partition, laptop lid) past its lease, then wakes
up convinced it still owns the row."""

import unittest

from sluice.pipeline import LostClaim
from sluice.states import CLAIMABLE, OpState

from .helpers import World


class TestZombieWorkers(unittest.TestCase):
    def test_zombie_write_lands_on_nothing(self):
        w = World()
        op_id = w.submit().operation.operation_id

        # worker A claims and immediately stalls (we just... don't run it)
        [a_snapshot] = w.store.claim(CLAIMABLE, 1, "worker-A", w.cfg.lease_seconds)
        self.assertEqual(a_snapshot.fencing_token, 1)

        # lease runs out; worker B legitimately reclaims and does real work
        w.expire_leases()
        [b_snapshot] = w.store.claim(CLAIMABLE, 1, "worker-B", w.cfg.lease_seconds)
        self.assertEqual(b_snapshot.fencing_token, 2)
        b_after = w.pipeline.step(b_snapshot, "worker-B")
        self.assertEqual(b_after.state, OpState.VALIDATED)

        # A wakes up and tries to 'finish' its validation with token 1
        ghost_write = w.store.cas(
            op_id, a_snapshot.fencing_token,
            from_state=OpState.REQUESTED, to_state=OpState.REJECTED,
            sets={"failure_reason": "zombie says no"},
            worker_id="worker-A",
        )
        self.assertIsNone(ghost_write)

        op = w.op(op_id)
        self.assertEqual(op.state, OpState.VALIDATED)     # B's work stands
        self.assertIsNone(op.failure_reason)
        # the audit trail contains no write from A after B took over
        tokens = [e.fencing_token for e in w.store.events(op_id)]
        self.assertNotIn(1, tokens[1:])  # token 1 never wrote past intake
        # and - the network-side half of the guarantee - the node's own log
        # shows worker-A never so much as touched the wire
        self.assertNotIn("worker-A", [e.worker for e in w.node.broadcast_log])

    def test_pipeline_surfaces_the_fence_as_lost_claim(self):
        w = World()
        w.submit()
        [a] = w.store.claim(CLAIMABLE, 1, "worker-A", w.cfg.lease_seconds)
        w.expire_leases()
        [b] = w.store.claim(CLAIMABLE, 1, "worker-B", w.cfg.lease_seconds)

        # A tries to drive the stale snapshot through the pipeline
        with self.assertRaises(LostClaim):
            w.pipeline.step(a, "worker-A")
        # B is unbothered
        self.assertEqual(w.pipeline.step(b, "worker-B").state, OpState.VALIDATED)

    def test_two_workers_one_operation_exactly_one_wins_the_claim(self):
        w = World()
        w.submit()
        first = w.store.claim(CLAIMABLE, 5, "worker-A", w.cfg.lease_seconds)
        second = w.store.claim(CLAIMABLE, 5, "worker-B", w.cfg.lease_seconds)
        self.assertEqual(len(first), 1)
        self.assertEqual(len(second), 0)  # SKIP LOCKED in spirit: no waiting, no sharing

    def test_release_lets_someone_else_in_without_expiry(self):
        w = World()
        w.submit()
        [a] = w.store.claim(CLAIMABLE, 1, "worker-A", w.cfg.lease_seconds)
        w.store.release(a.operation_id, a.fencing_token)
        [b] = w.store.claim(CLAIMABLE, 1, "worker-B", w.cfg.lease_seconds)
        self.assertEqual(b.fencing_token, 2)

    def test_stale_release_is_a_noop(self):
        w = World()
        w.submit()
        [a] = w.store.claim(CLAIMABLE, 1, "worker-A", w.cfg.lease_seconds)
        w.expire_leases()
        [b] = w.store.claim(CLAIMABLE, 1, "worker-B", w.cfg.lease_seconds)
        # A's deferred release must not evict B's fresh claim
        w.store.release(a.operation_id, a.fencing_token)
        again = w.store.claim(CLAIMABLE, 1, "worker-C", w.cfg.lease_seconds)
        self.assertEqual(again, [])  # B still holds it


if __name__ == "__main__":
    unittest.main()
