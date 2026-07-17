"""Canonical request hashing.

The operation_id alone makes retries idempotent, but it cannot tell an honest
retry from a client bug that reuses an ID with a *different* payload (which,
left undetected, silently pays the wrong person the wrong amount and returns
200 OK). So beside the ID we store a hash of what the ID is supposed to mean,
and refuse to serve a replay whose meaning has drifted.

The hash is computed over the *normalized* semantics, not the raw bytes:
amount "1.50" and "1.5" are the same instruction, so they hash the same.
Two fields that differ in meaning always hash differently. Trade-off notes
live in the README.
"""

from __future__ import annotations

import hashlib
import json


def request_hash(to_address: str, amount_units: int, asset: str) -> bytes:
    canonical = json.dumps(
        {"amount_units": amount_units, "asset": asset, "to_address": to_address},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return hashlib.sha256(canonical.encode("ascii")).digest()
