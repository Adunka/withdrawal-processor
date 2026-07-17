"""The operation state machine.

This table is the contract of the whole service. It exists in exactly two
places: here (enforced by both stores in Python) and in the transition-guard
trigger inside migrations/001_schema.sql (enforced by Postgres itself, so
even a buggy release or a stray psql session cannot corrupt an operation).
If you touch one, touch the other; tests/pg/test_pg_store.py walks the full
cartesian product of states against the trigger to keep them honest.

    requested ──> validated ──> signing ──> signed ──> broadcasting ──> broadcast ──> confirmed
        │             │            │           │             │              │
        v             v            v           └──> signing  ├──> signing   ├──> signing
     rejected      rejected     failed      (expired before  │  (provably   │ (expired,
                                             it was sent)    v   dead tx)   v  never mined)
                                                           failed         failed
                                                        (hard reject)   (reverted)

Backward edges into `signing` are the recovery paths: they are taken only
when the current transaction is *provably* unable to land (see pipeline.py
for the proof obligations). Terminal states are immutable, full stop.
"""

from __future__ import annotations

from enum import StrEnum


class OpState(StrEnum):
    REQUESTED = "requested"
    VALIDATED = "validated"
    SIGNING = "signing"
    SIGNED = "signed"
    BROADCASTING = "broadcasting"
    BROADCAST = "broadcast"
    CONFIRMED = "confirmed"
    FAILED = "failed"
    REJECTED = "rejected"


S = OpState  # local shorthand, the table below is noisy enough

TRANSITIONS: dict[OpState, frozenset[OpState]] = {
    S.REQUESTED:    frozenset({S.VALIDATED, S.REJECTED}),
    S.VALIDATED:    frozenset({S.SIGNING, S.REJECTED}),
    S.SIGNING:      frozenset({S.SIGNED, S.FAILED}),
    S.SIGNED:       frozenset({S.BROADCASTING, S.SIGNING}),
    S.BROADCASTING: frozenset({S.BROADCAST, S.SIGNING, S.FAILED}),
    S.BROADCAST:    frozenset({S.CONFIRMED, S.SIGNING, S.FAILED}),
    S.CONFIRMED:    frozenset(),
    S.FAILED:       frozenset(),
    S.REJECTED:     frozenset(),
}

TERMINAL: frozenset[OpState] = frozenset(
    s for s, nxt in TRANSITIONS.items() if not nxt
)

# Anything not terminal is fair game for a worker. There is deliberately no
# special "stuck" or "recovering" state: an operation abandoned mid-flight
# simply becomes claimable again once its lease expires, and the handler for
# its state *is* the recovery procedure. Crash recovery is not a mode here,
# it is Tuesday.
CLAIMABLE: frozenset[OpState] = frozenset(OpState) - TERMINAL


def can_transition(src: OpState, dst: OpState) -> bool:
    # Same-state writes are always allowed: a handler is permitted to make
    # progress (persist an artefact, bump a counter) without changing state.
    return src == dst or dst in TRANSITIONS[src]


class IllegalTransition(Exception):
    """Raised on a transition outside the table. This is a programming error,
    never a concurrency artefact - those surface as a failed fencing check."""

    def __init__(self, src: OpState, dst: OpState):
        super().__init__(f"illegal transition {src} -> {dst}")
        self.src, self.dst = src, dst
