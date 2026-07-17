"""Postgres Store (psycopg 3).

Three statements carry the whole design; everything else is plumbing.

1. Intake - the race between two identical POSTs dies on the primary key:

       INSERT ... ON CONFLICT (operation_id) DO NOTHING

2. Claim - the race between two workers dies inside one statement. Locked
   rows are invisible to other claimants, not blockers, so N workers spread
   across the queue instead of convoying:

       WITH picked AS (
           SELECT operation_id FROM operations
           WHERE state = ANY(...claimable...)
             AND (lease_expires_at IS NULL OR lease_expires_at < now())
           ORDER BY created_at
           LIMIT %s
           FOR UPDATE SKIP LOCKED
       )
       UPDATE operations o SET claimed_by = ..., lease_expires_at = ...,
              fencing_token = o.fencing_token + 1
       FROM picked WHERE o.operation_id = picked.operation_id
       RETURNING o.*

3. Every later write - the zombie with an expired lease dies on the WHERE:

       UPDATE operations SET ...
       WHERE operation_id = %s AND fencing_token = %s AND state = %s

   Zero rows updated == you are not the owner anymore == stop.

The event row is appended in the same transaction as the state write, so
the audit log can't drift from reality even under a crash.
"""

from __future__ import annotations

import uuid
from typing import Any, Iterable

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from ..model import Event, Operation
from ..states import TERMINAL, IllegalTransition, OpState, can_transition
from . import MUTABLE_FIELDS

_JSONB_FIELDS = {"unsigned_tx", "signed_tx"}


def _row_to_op(r: dict[str, Any]) -> Operation:
    return Operation(
        operation_id=r["operation_id"],
        request_hash=bytes(r["request_hash"]),
        state=OpState(r["state"]),
        to_address=r["to_address"],
        asset=r["asset"],
        amount_units=int(r["amount_units"]),
        unsigned_tx=r["unsigned_tx"],
        txid=r["txid"],
        tx_expiration=r["tx_expiration"],
        signed_tx=r["signed_tx"],
        attempts=r["attempts"],
        claimed_by=r["claimed_by"],
        lease_expires_at=r["lease_expires_at"],
        fencing_token=r["fencing_token"],
        last_error=r["last_error"],
        failure_reason=r["failure_reason"],
        broadcast_at=r["broadcast_at"],
        included_block=r["included_block"],
        confirmed_at=r["confirmed_at"],
        created_at=r["created_at"],
        updated_at=r["updated_at"],
    )


class PostgresStore:
    def __init__(self, dsn: str, pool_size: int = 10):
        self.pool = ConnectionPool(
            dsn,
            min_size=1,
            max_size=pool_size,
            kwargs={"row_factory": dict_row, "autocommit": False},
        )
        self.pool.wait()

    def close(self) -> None:
        self.pool.close()

    # -- intake ------------------------------------------------------------

    def insert_new(self, op: Operation) -> tuple[Operation, bool]:
        with self.pool.connection() as conn:
            with conn.transaction():
                row = conn.execute(
                    """
                    INSERT INTO operations
                        (operation_id, request_hash, state, to_address, asset, amount_units)
                    VALUES (%s, %s, %s::op_state, %s, %s, %s)
                    ON CONFLICT (operation_id) DO NOTHING
                    RETURNING *
                    """,
                    (op.operation_id, op.request_hash, op.state.value,
                     op.to_address, op.asset, op.amount_units),
                ).fetchone()
                if row is not None:
                    conn.execute(
                        """
                        INSERT INTO operation_events (operation_id, from_state, to_state, detail)
                        VALUES (%s, NULL, %s::op_state, %s)
                        """,
                        (op.operation_id, op.state.value, Jsonb({"intake": True})),
                    )
                    return _row_to_op(row), True
            # lost the insert race (or an honest retry): read whoever won
            existing = conn.execute(
                "SELECT * FROM operations WHERE operation_id = %s", (op.operation_id,)
            ).fetchone()
            return _row_to_op(existing), False

    def get(self, operation_id: uuid.UUID) -> Operation | None:
        with self.pool.connection() as conn:
            row = conn.execute(
                "SELECT * FROM operations WHERE operation_id = %s", (operation_id,)
            ).fetchone()
            return _row_to_op(row) if row else None

    # -- claiming -------------------------------------------------------------

    def claim(
        self, states: Iterable[OpState], limit: int, worker_id: str, lease_seconds: float
    ) -> list[Operation]:
        wanted = [s.value for s in states]
        with self.pool.connection() as conn:
            with conn.transaction():
                rows = conn.execute(
                    """
                    WITH picked AS (
                        SELECT operation_id
                        FROM operations
                        WHERE state = ANY(%s::op_state[])
                          AND (lease_expires_at IS NULL OR lease_expires_at < now())
                        ORDER BY created_at
                        LIMIT %s
                        FOR UPDATE SKIP LOCKED
                    )
                    UPDATE operations o
                    SET claimed_by       = %s,
                        lease_expires_at = now() + make_interval(secs => %s),
                        fencing_token    = o.fencing_token + 1
                    FROM picked
                    WHERE o.operation_id = picked.operation_id
                    RETURNING o.*
                    """,
                    (wanted, limit, worker_id, lease_seconds),
                ).fetchall()
        return [_row_to_op(r) for r in rows]

    # -- the one true write path ----------------------------------------------

    def cas(
        self,
        operation_id: uuid.UUID,
        fencing_token: int,
        *,
        from_state: OpState,
        to_state: OpState,
        sets: dict[str, Any] | None = None,
        detail: dict[str, Any] | None = None,
        renew_lease_seconds: float | None = None,
        worker_id: str | None = None,
    ) -> Operation | None:
        sets = sets or {}
        bad = set(sets) - MUTABLE_FIELDS
        if bad:
            raise ValueError(f"cas() may not touch {sorted(bad)}")
        if not can_transition(from_state, to_state):
            # The trigger would catch it too, but a programming error should
            # blow up in the app with a stack trace, not as SQLSTATE noise.
            raise IllegalTransition(from_state, to_state)

        clauses = ["state = %(to_state)s::op_state"]
        params: dict[str, Any] = {
            "id": operation_id,
            "token": fencing_token,
            "from_state": from_state.value,
            "to_state": to_state.value,
        }
        for col in sorted(sets):  # sorted: deterministic SQL, saner logs
            clauses.append(f"{col} = %({col})s")
            v = sets[col]
            params[col] = Jsonb(v) if col in _JSONB_FIELDS and v is not None else v
        if renew_lease_seconds is not None:
            clauses.append("lease_expires_at = now() + make_interval(secs => %(lease)s)")
            params["lease"] = renew_lease_seconds
        if to_state in TERMINAL:
            clauses.append("claimed_by = NULL")
            clauses.append("lease_expires_at = NULL")

        sql = f"""
            UPDATE operations
            SET {', '.join(clauses)}
            WHERE operation_id = %(id)s
              AND fencing_token = %(token)s
              AND state = %(from_state)s::op_state
            RETURNING *
        """
        with self.pool.connection() as conn:
            with conn.transaction():
                row = conn.execute(sql, params).fetchone()
                if row is None:
                    return None  # fenced out, or the state moved under us
                conn.execute(
                    """
                    INSERT INTO operation_events
                        (operation_id, from_state, to_state, worker_id, fencing_token, detail)
                    VALUES (%s, %s::op_state, %s::op_state, %s, %s, %s)
                    """,
                    (operation_id, from_state.value, to_state.value,
                     worker_id, fencing_token, Jsonb(detail or {})),
                )
        return _row_to_op(row)

    def release(self, operation_id: uuid.UUID, fencing_token: int) -> None:
        with self.pool.connection() as conn:
            conn.execute(
                """
                UPDATE operations
                SET claimed_by = NULL, lease_expires_at = NULL
                WHERE operation_id = %s
                  AND fencing_token = %s
                  AND state NOT IN ('confirmed', 'failed', 'rejected')
                """,
                (operation_id, fencing_token),
            )
            conn.commit()

    # -- event log ------------------------------------------------------------

    def events(self, operation_id: uuid.UUID) -> list[Event]:
        with self.pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT * FROM operation_events
                WHERE operation_id = %s ORDER BY event_id
                """,
                (operation_id,),
            ).fetchall()
        return [
            Event(
                operation_id=r["operation_id"],
                from_state=OpState(r["from_state"]) if r["from_state"] else None,
                to_state=OpState(r["to_state"]),
                worker_id=r["worker_id"],
                fencing_token=r["fencing_token"],
                detail=r["detail"],
                at=r["created_at"],
                seq=r["event_id"],
            )
            for r in rows
        ]

    def all_txids(self, operation_id: uuid.UUID) -> set[str]:
        with self.pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT detail->>'txid' AS txid
                FROM operation_events
                WHERE operation_id = %s AND detail ? 'txid'
                """,
                (operation_id,),
            ).fetchall()
        return {r["txid"] for r in rows if r["txid"]}
