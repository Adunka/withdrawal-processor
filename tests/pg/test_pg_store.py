"""Integration tier: proves the guarantees that live in Postgres itself.

Skipped unless DATABASE_URL is set. `make db-up` starts a disposable
Postgres with the schema applied; `make test-pg` runs this.

The fast tier already proved the *algorithm*; this tier proves the parts
the algorithm outsources to the database: SKIP LOCKED claim exclusivity,
the transition trigger, ON CONFLICT intake, fencing enforced by raw SQL
(no Python in the loop), and NUMERIC(38,0) not flinching at silly numbers.
"""

from __future__ import annotations

import os
import threading
import unittest
import uuid

DSN = os.environ.get("DATABASE_URL")

if DSN:
    import psycopg

    from sluice.canonical import request_hash
    from sluice.model import Operation
    from sluice.states import CLAIMABLE, TRANSITIONS, OpState
    from sluice.store.postgres import PostgresStore


def _mk_op(amount: int = 1_000_000) -> "Operation":
    oid = uuid.uuid4()
    addr = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
    return Operation(
        operation_id=oid,
        request_hash=request_hash(addr, amount, "USDT-TRC20"),
        state=OpState.REQUESTED,
        to_address=addr,
        asset="USDT-TRC20",
        amount_units=amount,
    )


@unittest.skipUnless(DSN, "set DATABASE_URL to run the Postgres tier")
class PGCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.store = PostgresStore(DSN, pool_size=6)
        # Fresh table per class: claim() orders by created_at, so leftovers
        # from an earlier run would get picked ahead of this run's rows and
        # turn deterministic tests into coin flips.
        with psycopg.connect(DSN) as conn:
            conn.execute("TRUNCATE operation_events, operations")

    @classmethod
    def tearDownClass(cls):
        cls.store.close()

    # raw-SQL helper to place a row into an arbitrary state, bypassing the
    # trigger (INSERT is unguarded on purpose - only UPDATE transitions are)
    def _plant(self, state: OpState) -> uuid.UUID:
        op = _mk_op()
        with psycopg.connect(DSN) as conn:
            conn.execute(
                """INSERT INTO operations
                   (operation_id, request_hash, state, to_address, asset, amount_units)
                   VALUES (%s, %s, %s::op_state, %s, %s, %s)""",
                (op.operation_id, op.request_hash, state.value,
                 op.to_address, op.asset, op.amount_units),
            )
        return op.operation_id


class TestIntakeAndTypes(PGCase):
    def test_on_conflict_race_one_row(self):
        op = _mk_op()
        results, barrier = [], threading.Barrier(8)

        def slam():
            barrier.wait()
            _, created = self.store.insert_new(op)
            results.append(created)

        ts = [threading.Thread(target=slam) for _ in range(8)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        self.assertEqual(sum(results), 1)

    def test_numeric_38_0_takes_absurd_amounts_exactly(self):
        op = _mk_op(amount=10**30 + 7)
        stored, _ = self.store.insert_new(op)
        self.assertEqual(stored.amount_units, 10**30 + 7)  # to the unit

    def test_positive_amount_check(self):
        op = _mk_op()
        op.amount_units = 0
        with self.assertRaises(Exception):
            self.store.insert_new(op)


class TestTransitionTrigger(PGCase):
    def test_full_matrix_against_the_python_table(self):
        """Walk every (from, to) pair. The trigger and states.py must agree
        cell for cell - this is the drift alarm for the duplicated table."""
        for src in OpState:
            for dst in OpState:
                if src == dst:
                    continue
                oid = self._plant(src)
                legal = dst in TRANSITIONS[src]
                with psycopg.connect(DSN) as conn:
                    if legal:
                        conn.execute(
                            "UPDATE operations SET state = %s::op_state WHERE operation_id = %s",
                            (dst.value, oid),
                        )
                    else:
                        with self.assertRaises(psycopg.errors.CheckViolation,
                                               msg=f"{src}->{dst} should be illegal"):
                            conn.execute(
                                "UPDATE operations SET state = %s::op_state WHERE operation_id = %s",
                                (dst.value, oid),
                            )

    def test_terminal_rows_resist_even_innocent_updates(self):
        oid = self._plant(OpState.CONFIRMED)
        with psycopg.connect(DSN) as conn:
            with self.assertRaises(psycopg.errors.CheckViolation):
                conn.execute(
                    "UPDATE operations SET last_error = 'oops' WHERE operation_id = %s",
                    (oid,),
                )


class TestClaimAndFencing(PGCase):
    def test_skip_locked_two_claimants_no_overlap_no_blocking(self):
        ops = [_mk_op() for _ in range(10)]
        for o in ops:
            self.store.insert_new(o)

        got: dict[str, list] = {"a": [], "b": []}
        barrier = threading.Barrier(2)

        def grab(name):
            barrier.wait()
            got[name] = self.store.claim(CLAIMABLE, 10, name, lease_seconds=60)

        ts = [threading.Thread(target=grab, args=(n,)) for n in ("a", "b")]
        [t.start() for t in ts]
        [t.join(timeout=10) for t in ts]

        ids_a = {o.operation_id for o in got["a"]}
        ids_b = {o.operation_id for o in got["b"]}
        self.assertEqual(ids_a & ids_b, set(), "same row claimed twice")
        mine = {o.operation_id for o in ops}
        self.assertEqual((ids_a | ids_b) & mine, mine, "rows went unclaimed")

    def test_stale_fencing_token_updates_zero_rows_in_raw_sql(self):
        op = _mk_op()
        self.store.insert_new(op)
        [claimed] = [
            o for o in self.store.claim(CLAIMABLE, 100, "first", 0.0)
            if o.operation_id == op.operation_id
        ]
        # lease of 0s: instantly reclaimable; second claim bumps the token
        [reclaimed] = [
            o for o in self.store.claim(CLAIMABLE, 100, "second", 60)
            if o.operation_id == op.operation_id
        ]
        self.assertEqual(reclaimed.fencing_token, claimed.fencing_token + 1)

        # the zombie's write, in raw SQL, with its old token: rowcount 0
        with psycopg.connect(DSN) as conn:
            cur = conn.execute(
                """UPDATE operations SET state = 'validated'::op_state
                   WHERE operation_id = %s AND fencing_token = %s
                     AND state = 'requested'::op_state""",
                (op.operation_id, claimed.fencing_token),
            )
            self.assertEqual(cur.rowcount, 0)

    def test_cas_and_event_commit_together(self):
        op = _mk_op()
        self.store.insert_new(op)
        [c] = [o for o in self.store.claim(CLAIMABLE, 100, "w", 60)
               if o.operation_id == op.operation_id]
        after = self.store.cas(
            c.operation_id, c.fencing_token,
            from_state=OpState.REQUESTED, to_state=OpState.VALIDATED,
            worker_id="w", detail={"note": "pg tier"},
        )
        self.assertEqual(after.state, OpState.VALIDATED)
        evs = self.store.events(c.operation_id)
        self.assertEqual(evs[-1].to_state, OpState.VALIDATED)
        self.assertEqual(evs[-1].detail["note"], "pg tier")

    def test_txid_uniqueness_is_schema_law(self):
        a, b = self._plant(OpState.SIGNING), self._plant(OpState.SIGNING)
        with psycopg.connect(DSN) as conn:
            conn.execute(
                "UPDATE operations SET txid = 'deadbeef' WHERE operation_id = %s", (a,)
            )
        with psycopg.connect(DSN) as conn:
            with self.assertRaises(psycopg.errors.UniqueViolation):
                conn.execute(
                    "UPDATE operations SET txid = 'deadbeef' WHERE operation_id = %s", (b,)
                )


if __name__ == "__main__":
    unittest.main()
