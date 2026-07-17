"""Types shared between the pipeline and the (mock) TRON node.

Response codes mirror java-tron's broadcast return codes because the way you
*classify* them is the actual engineering problem: some mean "definitely not
accepted, safe to retry", some mean "definitely accepted", and a thrown
timeout means "you know nothing". The pipeline's correctness hangs on never
confusing those three.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any


class BroadcastCode(StrEnum):
    SUCCESS = "SUCCESS"
    # The node already knows this txid. For us this is *good news*: our
    # earlier attempt landed, the retry can stand down.
    DUP_TRANSACTION = "DUP_TRANSACTION"
    # Hard rejects: the transaction as signed will never be accepted.
    SIGERROR = "SIGERROR"
    CONTRACT_VALIDATE_ERROR = "CONTRACT_VALIDATE_ERROR"
    # Soft rejects: this particular envelope is stale, rebuild and try again.
    TAPOS_ERROR = "TAPOS_ERROR"
    TRANSACTION_EXPIRATION_ERROR = "TRANSACTION_EXPIRATION_ERROR"
    # Not processed at all; retrying the same bytes is safe.
    SERVER_BUSY = "SERVER_BUSY"


# Codes after which the same signed bytes may be re-sent without any risk:
# the node either never took them (BUSY) or already has them (DUP).
RETRY_SAME_BYTES = frozenset({BroadcastCode.SERVER_BUSY})
ACCEPTED = frozenset({BroadcastCode.SUCCESS, BroadcastCode.DUP_TRANSACTION})
HARD_REJECT = frozenset({BroadcastCode.SIGERROR, BroadcastCode.CONTRACT_VALIDATE_ERROR})
STALE_ENVELOPE = frozenset({BroadcastCode.TAPOS_ERROR, BroadcastCode.TRANSACTION_EXPIRATION_ERROR})


@dataclass
class BroadcastResult:
    # `str` and not just the enum, deliberately: a live node can answer with
    # a code minted after this code shipped. The classifier treats anything
    # it doesn't recognize as ambiguous, so novelty degrades to a
    # reconcile - never to a crash and never to an unproven rebuild.
    code: BroadcastCode | str
    message: str = ""


class NodeTimeout(Exception):
    """The request may or may not have reached the node. This exception is
    the villain of the entire project: after catching it you are not allowed
    to believe the transaction was NOT sent."""


class TxQuery(StrEnum):
    UNKNOWN = "unknown"      # node has never heard of this txid
    PENDING = "pending"      # accepted, sitting in the mempool
    INCLUDED = "included"    # in a block (check .solid for finality)


@dataclass
class TxStatus:
    result: TxQuery
    block: int | None = None
    solid: bool = False
    receipt: str | None = None   # "SUCCESS" | "REVERTED" once included


def compute_txid(raw_data: dict[str, Any]) -> str:
    """txid = sha256 over the serialized raw transaction.

    On the real chain the bytes are protobuf; here they are canonical JSON.
    The property the whole design leans on survives the substitution: the
    txid is fully determined by raw_data and therefore known - and durable
    in our DB - *before* the first broadcast attempt.
    """
    blob = json.dumps(raw_data, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(blob).hexdigest()


@dataclass
class UnsignedTx:
    raw_data: dict[str, Any]
    txid: str
    expiration: datetime


@dataclass
class SignedTx:
    raw_data: dict[str, Any]
    txid: str
    expiration: datetime
    signature: str

    def to_json(self) -> dict[str, Any]:
        return {
            "raw_data": self.raw_data,
            "txid": self.txid,
            "expiration": self.expiration.isoformat(),
            "signature": self.signature,
        }

    @classmethod
    def from_json(cls, d: dict[str, Any]) -> "SignedTx":
        return cls(
            raw_data=d["raw_data"],
            txid=d["txid"],
            expiration=datetime.fromisoformat(d["expiration"]),
            signature=d["signature"],
        )
