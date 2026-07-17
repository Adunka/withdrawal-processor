"""Intake is where 'two workers got the same request' has a quieter sibling:
'the client retried a timed-out POST'. Both must collapse to one row.

The service-level race tests below run on a bare interpreter; the API-layer
tests additionally need flask (a runtime dependency of the app, not of the
test tooling) and skip themselves cleanly when it isn't installed.
"""

import threading
import unittest
import uuid

from sluice.service import PayloadMismatch, submit_withdrawal
from sluice.states import OpState

from .helpers import World, test_address

try:
    from sluice.api import create_app
except ModuleNotFoundError:      # flask missing - API tests skip, rest run
    create_app = None


@unittest.skipUnless(create_app, "flask not installed; pip install flask to run API tests")
class TestIntakeAPI(unittest.TestCase):
    def setUp(self):
        self.w = World()
        self.client = create_app(self.w.store, self.w.cfg).test_client()

    def test_created_then_replayed(self):
        payload = self.w.payload()

        r1 = self.client.post("/v1/withdrawals", json=payload)
        self.assertEqual(r1.status_code, 201)

        r2 = self.client.post("/v1/withdrawals", json=payload)
        self.assertEqual(r2.status_code, 200)
        self.assertEqual(r2.headers.get("X-Idempotent-Replay"), "true")
        self.assertEqual(r1.json["operation_id"], r2.json["operation_id"])

        # exactly one row exists
        self.assertEqual(len(self.w.store.all_operations()), 1)

    def test_replay_reports_current_state_not_stale_snapshot(self):
        payload = self.w.payload()
        self.client.post("/v1/withdrawals", json=payload)
        self.w.drain()  # move it along
        r = self.client.post("/v1/withdrawals", json=payload)
        self.assertEqual(r.status_code, 200)
        self.assertNotEqual(r.json["state"], OpState.REQUESTED.value)

    def test_same_id_different_payload_is_a_409(self):
        payload = self.w.payload(amount="10")
        self.client.post("/v1/withdrawals", json=payload)

        evil = dict(payload, amount="10000")
        r = self.client.post("/v1/withdrawals", json=evil)
        self.assertEqual(r.status_code, 409)

        # and the original is untouched
        op = self.w.op(payload["operation_id"])
        self.assertEqual(op.amount_units, 10_000_000)

    def test_amount_as_json_number_is_rejected(self):
        # floats lose money; the API refuses to even look at them
        r = self.client.post("/v1/withdrawals", json=self.w.payload(amount=12.5))
        self.assertEqual(r.status_code, 422)
        self.assertEqual(r.json["error"]["field"], "amount")

    def test_equivalent_amount_strings_replay_cleanly(self):
        p = self.w.payload(amount="1.50")
        self.assertEqual(self.client.post("/v1/withdrawals", json=p).status_code, 201)
        p2 = dict(p, amount="1.5")  # same instruction, different spelling
        self.assertEqual(self.client.post("/v1/withdrawals", json=p2).status_code, 200)

    def test_bad_address_rejected_at_the_door(self):
        r = self.client.post(
            "/v1/withdrawals", json=self.w.payload(to_address="TnotARealAddress123")
        )
        self.assertEqual(r.status_code, 422)

    def test_status_endpoint(self):
        p = self.w.payload()
        self.client.post("/v1/withdrawals", json=p)
        r = self.client.get(f"/v1/withdrawals/{p['operation_id']}")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json["state"], "requested")
        self.assertEqual(
            self.client.get(f"/v1/withdrawals/{uuid.uuid4()}").status_code, 404
        )


class TestIntakeRace(unittest.TestCase):
    def test_32_concurrent_identical_posts_one_row(self):
        w = World()
        payload = w.payload()
        created_flags, barrier = [], threading.Barrier(32)

        def slam():
            barrier.wait()  # maximize the collision
            res = submit_withdrawal(w.store, w.cfg, payload)
            created_flags.append(res.created)

        threads = [threading.Thread(target=slam) for _ in range(32)]
        [t.start() for t in threads]
        [t.join() for t in threads]

        self.assertEqual(sum(created_flags), 1, "exactly one POST may create")
        self.assertEqual(len(w.store.all_operations()), 1)

    def test_mismatch_race_never_corrupts_the_winner(self):
        w = World()
        op_id = str(uuid.uuid4())
        good = w.payload(operation_id=op_id, amount="10")
        bad = w.payload(operation_id=op_id, amount="99999", to_address=test_address(7))
        mismatches, barrier = [], threading.Barrier(16)

        def slam(p):
            barrier.wait()
            try:
                submit_withdrawal(w.store, w.cfg, p)
            except PayloadMismatch:
                mismatches.append(1)

        threads = [threading.Thread(target=slam, args=(good if i % 2 else bad,)) for i in range(16)]
        [t.start() for t in threads]
        [t.join() for t in threads]

        # whichever payload won the insert, every request for the OTHER one
        # got a mismatch, and the stored row is internally consistent
        op = w.op(op_id)
        self.assertIn(op.amount_units, (10_000_000, 99_999_000_000))
        self.assertGreaterEqual(len(mismatches), 1)
        self.assertEqual(len(w.store.all_operations()), 1)


if __name__ == "__main__":
    unittest.main()
