"""Amount parsing. Strings in, integers out, floats rejected at the door.

USDT on TRON has 6 decimals, so "12.5" is 12_500_000 minimal units. We do
the conversion with plain integer arithmetic on the string - not Decimal,
not float - because there is nothing to round and therefore nothing that
should be *able* to round. 0.1 + 0.2 jokes have no jurisdiction here.
"""

from __future__ import annotations

import re

# [0-9] and not \d: \d matches any Unicode decimal digit (Arabic-Indic,
# Devanagari, ...) and int() parses them all. Amounts are ASCII or nothing.
_AMOUNT_RE = re.compile(r"^(?P<int>[0-9]+)(?:\.(?P<frac>[0-9]+))?$")


class AmountError(ValueError):
    pass


def to_units(amount: str, decimals: int) -> int:
    """Parse a human decimal string ("12.5") into integer minimal units.

    Rejects: non-strings (floats have already lost precision by the time we
    see them), negatives, scientific notation, more fractional digits than
    the token supports, and empty/garbage input. "1.50" and "1.5" normalize
    to the same value on purpose - see the idempotency notes in the README.
    """
    if not isinstance(amount, str):
        raise AmountError(
            f"amount must be a decimal string, got {type(amount).__name__} "
            "(JSON numbers are parsed as floats and floats do not do money)"
        )
    m = _AMOUNT_RE.match(amount.strip())
    if not m:
        raise AmountError(f"malformed amount: {amount!r}")
    whole, frac = m.group("int"), m.group("frac") or ""
    if len(frac) > decimals:
        raise AmountError(
            f"amount {amount!r} has {len(frac)} fractional digits, token supports {decimals}"
        )
    units = int(whole) * 10**decimals + int(frac.ljust(decimals, "0") or "0")
    return units


def to_human(units: int, decimals: int) -> str:
    """Integer units back to a decimal string, trailing zeros trimmed."""
    if units < 0:
        raise AmountError("negative units")
    whole, frac = divmod(units, 10**decimals)
    if frac == 0:
        return str(whole)
    return f"{whole}.{str(frac).rjust(decimals, '0').rstrip('0')}"
