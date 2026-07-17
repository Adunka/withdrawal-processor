"""Storage contract.

Two implementations, one set of semantics:

  * MemoryStore   - single-process, lock-based. Exists so the whole failure
                    matrix (crashes, zombies, chaos) runs anywhere in
                    milliseconds with zero dependencies.
  * PostgresStore - the production shape. Claiming rides on
                    SELECT ... FOR UPDATE SKIP LOCKED, transitions are
                    re-checked by a trigger, intake races collapse on the
                    primary key. tests/pg/ proves the DB-level guarantees.

The rules every implementation must honor:

  claim()  atomically takes ownership: sets claimed_by + lease and increments
           the operation's fencing_token. The returned snapshot carries the
           token; it is the worker's proof of ownership.

  cas()    is the ONLY way to write an operation after intake. It applies
           iff (operation_id, fencing_token, state) all still match - so a
           worker whose lease was stolen writes precisely nothing - and it
           appends an Event in the same atomic step. Illegal transitions
           raise; they are bugs, not races.

  Terminal states are immutable. cas() into a terminal state clears the
  lease as part of the same write.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Iterable, Protocol

from ..model import Event, Operation
from ..states import OpState


class Store(Protocol):
    def insert_new(self, op: Operation) -> tuple[Operation, bool]:
        """Insert iff operation_id is unseen. Returns (row, created). On
        conflict returns the EXISTING row untouched - hash comparison is the
        caller's job."""
        ...

    def get(self, operation_id: uuid.UUID) -> Operation | None: ...

    def claim(
        self, states: Iterable[OpState], limit: int, worker_id: str, lease_seconds: float
    ) -> list[Operation]: ...

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
    ) -> Operation | None: ...

    def release(self, operation_id: uuid.UUID, fencing_token: int) -> None:
        """Give up a claim early (worker finished or is backing off)."""
        ...

    def events(self, operation_id: uuid.UUID) -> list[Event]: ...

    def all_txids(self, operation_id: uuid.UUID) -> set[str]:
        """Every txid this operation was EVER assigned, from the event log.
        The invariant checker's raw material."""
        ...


# Columns cas() is allowed to touch. Both stores enforce the same whitelist;
# in Postgres it doubles as the guard that keeps dynamic SET clauses boring.
MUTABLE_FIELDS = frozenset(
    {
        "unsigned_tx",
        "txid",
        "tx_expiration",
        "signed_tx",
        "attempts",
        "last_error",
        "failure_reason",
        "broadcast_at",
        "included_block",
        "confirmed_at",
    }
)
