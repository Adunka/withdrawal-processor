"""Data shapes. One Operation row per withdrawal, append-only Events beside it."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from .states import OpState


@dataclass
class Operation:
    operation_id: uuid.UUID
    request_hash: bytes                 # sha256 of the canonical payload
    state: OpState
    to_address: str
    asset: str
    amount_units: int                   # integer minimal units; never a float, ever

    # transaction artefacts - filled in as the operation moves right
    unsigned_tx: dict[str, Any] | None = None
    txid: str | None = None             # known *before* broadcast; that's the point
    tx_expiration: datetime | None = None
    signed_tx: dict[str, Any] | None = None

    attempts: int = 0                   # build attempts; bumped on every rebuild

    # claim bookkeeping
    claimed_by: str | None = None
    lease_expires_at: datetime | None = None
    fencing_token: int = 0

    last_error: str | None = None
    failure_reason: str | None = None

    broadcast_at: datetime | None = None
    included_block: int | None = None
    confirmed_at: datetime | None = None

    created_at: datetime | None = None
    updated_at: datetime | None = None

    def public_view(self) -> dict[str, Any]:
        """What the API is willing to show a client."""
        return {
            "operation_id": str(self.operation_id),
            "state": self.state.value,
            "to_address": self.to_address,
            "asset": self.asset,
            "amount_units": str(self.amount_units),
            "txid": self.txid,
            "attempts": self.attempts,
            "failure_reason": self.failure_reason,
            "included_block": self.included_block,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "confirmed_at": self.confirmed_at.isoformat() if self.confirmed_at else None,
        }


@dataclass
class Event:
    """One row per state write. This log is how you answer 'what happened to
    operation X at 03:14 last night' without guessing, and how the invariant
    checker learns every txid an operation has ever been given."""

    operation_id: uuid.UUID
    from_state: OpState | None
    to_state: OpState
    worker_id: str | None
    fencing_token: int
    detail: dict[str, Any] = field(default_factory=dict)
    at: datetime | None = None
    seq: int = 0
