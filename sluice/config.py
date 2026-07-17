"""Runtime configuration and the clock abstraction.

Everything time-related goes through a Clock instance instead of calling
datetime.now() directly. That single indirection is what lets the test
suite fast-forward past transaction expirations and lease timeouts in
microseconds instead of sleeping through them.
"""

from __future__ import annotations

import os
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone


class Clock:
    """Real wall clock. UTC everywhere, aware datetimes only."""

    def now(self) -> datetime:
        return datetime.now(timezone.utc)



class FakeClock(Clock):
    """Manually advanced clock for tests. Thread-safe: chaos tests read it
    from worker threads while the orchestrator pushes it forward."""

    def __init__(self, start: datetime | None = None):
        self._now = start or datetime(2026, 1, 1, tzinfo=timezone.utc)
        self._lock = threading.Lock()

    def now(self) -> datetime:
        with self._lock:
            return self._now

    def advance(self, seconds: float) -> datetime:
        with self._lock:
            self._now += timedelta(seconds=seconds)
            return self._now


# Mainnet USDT contract. The mock network doesn't care, but validation
# does, and there is no reason to invent a fake address when the real
# one is a matter of public record.
USDT_TRC20_CONTRACT = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"


@dataclass
class Config:
    # -- intake ------------------------------------------------------------
    asset: str = "USDT-TRC20"
    token_decimals: int = 6
    min_amount_units: int = 1                      # 0.000001 USDT
    max_amount_units: int = 1_000_000 * 10**6      # 1M USDT per operation

    # -- claiming / leases ---------------------------------------------------
    # A worker owns an operation for lease_seconds after claiming it. If it
    # dies (or stalls), the lease runs out and anyone may reclaim. Ownership
    # is then arbitrated by the fencing token, not by the lease alone.
    lease_seconds: float = 30.0
    claim_batch: int = 10
    poll_interval: float = 0.5

    # -- transaction lifetime ------------------------------------------------
    # TRON transactions carry an expiration timestamp in raw_data; past it the
    # network will not accept them, period. That hard deadline is the anchor
    # of the whole recovery story: a transaction that is not on chain after
    # expiration (+ margin for propagation/clock skew) is provably dead and
    # can be rebuilt without any double-spend risk.
    tx_ttl_seconds: float = 60.0
    expiration_safety_seconds: float = 30.0

    max_build_attempts: int = 5

    usdt_contract: str = USDT_TRC20_CONTRACT
    clock: Clock = field(default_factory=Clock)

    @classmethod
    def from_env(cls) -> "Config":
        cfg = cls()
        if v := os.environ.get("SLUICE_LEASE_SECONDS"):
            cfg.lease_seconds = float(v)
        if v := os.environ.get("SLUICE_TX_TTL_SECONDS"):
            cfg.tx_ttl_seconds = float(v)
        if v := os.environ.get("SLUICE_CLAIM_BATCH"):
            cfg.claim_batch = int(v)
        if v := os.environ.get("SLUICE_POLL_INTERVAL"):
            cfg.poll_interval = float(v)
        return cfg
