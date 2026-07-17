"""In-memory Store.

Faithful to the Postgres semantics down to the sharp edges: claims are
atomic, fencing tokens are bumped on every claim, cas() is a compare-and-set
on (id, token, state), illegal transitions raise, terminal rows are bricks.
One big lock stands in for row locks - claim() under it behaves exactly like
FOR UPDATE SKIP LOCKED from the caller's point of view: no two claimants
ever walk away with the same row.
"""

from __future__ import annotations

import copy
import itertools
import threading
import uuid
from datetime import timedelta
from typing import Any, Iterable

from ..config import Clock
from ..model import Event, Operation
from ..states import TERMINAL, IllegalTransition, OpState, can_transition
from . import MUTABLE_FIELDS


class MemoryStore:
    def __init__(self, clock: Clock):
        self._clock = clock
        self._lock = threading.RLock()
        self._ops: dict[uuid.UUID, Operation] = {}
        self._events: list[Event] = []
        self._order = itertools.count()          # stable FIFO under a fake clock
        self._arrival: dict[uuid.UUID, int] = {}
        self._event_seq = itertools.count(1)

    # -- intake ------------------------------------------------------------

    def insert_new(self, op: Operation) -> tuple[Operation, bool]:
        with self._lock:
            existing = self._ops.get(op.operation_id)
            if existing is not None:
                return copy.deepcopy(existing), False
            now = self._clock.now()
            row = copy.deepcopy(op)
            row.created_at = now
            row.updated_at = now
            self._ops[op.operation_id] = row
            self._arrival[op.operation_id] = next(self._order)
            self._append_event(row, None, row.state, None, 0, {"intake": True})
            return copy.deepcopy(row), True

    def get(self, operation_id: uuid.UUID) -> Operation | None:
        with self._lock:
            row = self._ops.get(operation_id)
            return copy.deepcopy(row) if row else None

    # -- claiming -------------------------------------------------------------

    def claim(
        self, states: Iterable[OpState], limit: int, worker_id: str, lease_seconds: float
    ) -> list[Operation]:
        wanted = set(states)
        now = self._clock.now()
        out: list[Operation] = []
        with self._lock:
            candidates = sorted(
                (op for op in self._ops.values() if op.state in wanted),
                key=lambda o: self._arrival[o.operation_id],
            )
            for op in candidates:
                if len(out) >= limit:
                    break
                if op.lease_expires_at is not None and op.lease_expires_at > now:
                    continue  # somebody holds it; SKIP LOCKED in spirit
                op.claimed_by = worker_id
                op.lease_expires_at = now + timedelta(seconds=lease_seconds)
                op.fencing_token += 1
                op.updated_at = now
                out.append(copy.deepcopy(op))
        return out

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
            raise IllegalTransition(from_state, to_state)

        with self._lock:
            op = self._ops.get(operation_id)
            if op is None:
                return None
            # The fencing check. A zombie with yesterday's token stops here,
            # which is the entire reason leases are allowed to expire early.
            if op.fencing_token != fencing_token or op.state != from_state:
                return None
            if op.state in TERMINAL:
                # Unreachable through can_transition(), but the DB trigger has
                # this belt too, and the stores agree with the DB. Always.
                return None

            now = self._clock.now()
            for k, v in sets.items():
                setattr(op, k, copy.deepcopy(v))
            op.state = to_state
            op.updated_at = now
            if renew_lease_seconds is not None:
                op.lease_expires_at = now + timedelta(seconds=renew_lease_seconds)
            if to_state in TERMINAL:
                op.claimed_by = None
                op.lease_expires_at = None
            self._append_event(op, from_state, to_state, worker_id, fencing_token, detail or {})
            return copy.deepcopy(op)

    def release(self, operation_id: uuid.UUID, fencing_token: int) -> None:
        with self._lock:
            op = self._ops.get(operation_id)
            if op is None or op.fencing_token != fencing_token:
                return
            op.claimed_by = None
            op.lease_expires_at = None
            op.updated_at = self._clock.now()

    # -- event log ------------------------------------------------------------

    def _append_event(self, op, from_state, to_state, worker_id, token, detail):
        self._events.append(
            Event(
                operation_id=op.operation_id,
                from_state=from_state,
                to_state=to_state,
                worker_id=worker_id,
                fencing_token=token,
                detail=copy.deepcopy(detail),
                at=self._clock.now(),
                seq=next(self._event_seq),
            )
        )

    def events(self, operation_id: uuid.UUID) -> list[Event]:
        with self._lock:
            return [copy.deepcopy(e) for e in self._events if e.operation_id == operation_id]

    def all_txids(self, operation_id: uuid.UUID) -> set[str]:
        with self._lock:
            return {
                e.detail["txid"]
                for e in self._events
                if e.operation_id == operation_id and "txid" in e.detail
            }

    # test convenience
    def all_operations(self) -> list[Operation]:
        with self._lock:
            return [copy.deepcopy(o) for o in self._ops.values()]
