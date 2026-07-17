"""Intake: turn an HTTP-shaped request into exactly one operation row.

Idempotency here rests on the primary key, not on application logic. Two
racing POSTs with the same operation_id both attempt the insert; the store
guarantees exactly one wins and both callers see the same row. The loser is
then either a clean replay (hashes match -> return current state) or a
client bug (hashes differ -> 409, and the original row is not touched).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

from .canonical import request_hash
from .config import Config
from .model import Operation
from .money import AmountError, to_units
from .states import OpState
from .store import Store
from .tron import address


class ValidationError(ValueError):
    def __init__(self, field: str, message: str):
        super().__init__(f"{field}: {message}")
        self.field, self.message = field, message


class PayloadMismatch(Exception):
    """Same operation_id, different meaning. Refuse loudly; this is the bug
    that quietly pays the wrong person if you shrug it off."""


@dataclass
class SubmitResult:
    operation: Operation
    created: bool           # False -> idempotent replay


def submit_withdrawal(store: Store, cfg: Config, payload: dict[str, Any]) -> SubmitResult:
    if not isinstance(payload, dict):
        raise ValidationError("body", "expected a JSON object")

    # operation_id is minted by the CLIENT. That is the entire trick: the
    # caller can retry a timed-out POST with the same id forever and the
    # worst case is reading its own status back.
    raw_id = payload.get("operation_id")
    try:
        op_id = uuid.UUID(str(raw_id))
    except (ValueError, AttributeError, TypeError):
        raise ValidationError("operation_id", "must be a UUID (client-generated)")

    to_addr = payload.get("to_address")
    if not isinstance(to_addr, str) or not address.is_valid(to_addr):
        raise ValidationError("to_address", "not a valid TRON base58check address")

    asset = payload.get("asset", cfg.asset)
    if asset != cfg.asset:
        raise ValidationError("asset", f"unsupported asset {asset!r}")

    try:
        amount_units = to_units(payload.get("amount"), cfg.token_decimals)
    except AmountError as e:
        raise ValidationError("amount", str(e))
    if amount_units < cfg.min_amount_units:
        raise ValidationError("amount", "below minimum")
    if amount_units > cfg.max_amount_units:
        raise ValidationError("amount", "above per-operation maximum")

    rhash = request_hash(to_addr, amount_units, asset)
    op = Operation(
        operation_id=op_id,
        request_hash=rhash,
        state=OpState.REQUESTED,
        to_address=to_addr,
        asset=asset,
        amount_units=amount_units,
    )

    row, created = store.insert_new(op)
    if not created and row.request_hash != rhash:
        raise PayloadMismatch(
            f"operation {op_id} already exists with a different payload"
        )
    return SubmitResult(operation=row, created=created)
