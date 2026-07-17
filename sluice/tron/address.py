"""TRON base58check addresses.

The signer in this project is a mock, but address validation is the real
thing: version byte 0x41, 20-byte payload, double-sha256 checksum. Sending
funds to a mistyped address is not a failure mode we get to have, mock keys
or not. Verified in tests against the mainnet USDT contract address.
"""

from __future__ import annotations

import hashlib

_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_INDEX = {c: i for i, c in enumerate(_ALPHABET)}
_VERSION = 0x41  # 'T...' prefix on mainnet


def _b58decode(s: str) -> bytes:
    num = 0
    for ch in s:
        if ch not in _INDEX:
            raise ValueError(f"invalid base58 character {ch!r}")
        num = num * 58 + _INDEX[ch]
    raw = num.to_bytes((num.bit_length() + 7) // 8, "big")
    # leading '1's encode leading zero bytes
    pad = len(s) - len(s.lstrip("1"))
    return b"\x00" * pad + raw


def _b58encode(raw: bytes) -> str:
    num = int.from_bytes(raw, "big")
    out = []
    while num:
        num, rem = divmod(num, 58)
        out.append(_ALPHABET[rem])
    pad = len(raw) - len(raw.lstrip(b"\x00"))
    return "1" * pad + "".join(reversed(out))


def _checksum(payload: bytes) -> bytes:
    return hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4]


def is_valid(address: str) -> bool:
    if not isinstance(address, str) or not (30 <= len(address) <= 40):
        return False
    try:
        raw = _b58decode(address)
    except ValueError:
        return False
    if len(raw) != 25 or raw[0] != _VERSION:
        return False
    payload, check = raw[:21], raw[21:]
    return _checksum(payload) == check


def from_payload(body20: bytes) -> str:
    """Build a syntactically valid address from 20 raw bytes. Test helper -
    lets fixtures mint addresses that pass real validation."""
    if len(body20) != 20:
        raise ValueError("need exactly 20 bytes")
    payload = bytes([_VERSION]) + body20
    return _b58encode(payload + _checksum(payload))
