"""An in-memory TRON impersonator built for one purpose: lying to workers
the way a real network does.

It can accept a transaction and then throw a timeout at the caller (the
ambiguous failure every exchange eventually meets at 4am), drop requests
outright, return DUP_TRANSACTION, delay inclusion, revert receipts, and it
*hard-enforces expiration*: an expired transaction is never accepted and
never mined. That last rule is not test sugar - it is the real network
property that makes rebuild-after-expiration safe, so the mock guards it
like the tests depend on it. Because they do.

Everything is behind one lock; chaos tests hit this from many threads.
"""

from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum, auto
from typing import Any

from ..config import Clock
from .signer import MockSigner
from .types import (
    BroadcastCode,
    BroadcastResult,
    NodeTimeout,
    SignedTx,
    TxQuery,
    TxStatus,
)


class Fault(StrEnum):
    # Node never sees the request; caller gets a timeout.
    TIMEOUT_DROPPED = auto()
    # Node ACCEPTS the transaction, then the response is lost and the caller
    # gets a timeout anyway. The nastiest thing a network can do to you.
    TIMEOUT_ACCEPTED = auto()
    # Node answers SERVER_BUSY without processing.
    BUSY = auto()
    # Node answers with a code this codebase has never heard of, without
    # processing the tx. Tests that the pipeline's default is "assume
    # nothing, reconcile" rather than "crash" or - worse - "rebuild".
    UNKNOWN_CODE = auto()
    # Next included transaction gets a REVERTED receipt.
    REVERT_RECEIPT = auto()
    # Queries time out too, why not.
    QUERY_TIMEOUT = auto()


@dataclass
class _TxRecord:
    tx: SignedTx
    accepted_at_block: int
    included_block: int | None = None
    receipt: str | None = None


@dataclass
class BroadcastLogEntry:
    txid: str
    outcome: str            # accepted | dup | rejected:<code> | timeout-accepted |
                            # timeout-dropped | busy | unknown-code
    at_block: int
    worker: str | None = None   # who touched the wire - the zombie test reads this


class MockTronNode:
    def __init__(self, clock: Clock, solidity_lag: int = 2, inclusion_delay_blocks: int = 1,
                 signer: MockSigner | None = None):
        self.clock = clock
        self.solidity_lag = solidity_lag
        # blocks between acceptance and inclusion, if nobody meddles
        self.inclusion_delay = inclusion_delay_blocks
        # the node verifies signatures the same way the signer makes them;
        # inject a custom-keyed signer if you use one, or SIGERROR awaits
        self._signer = signer or MockSigner()

        self._lock = threading.RLock()
        self._head = 1000                      # arbitrary non-zero genesis
        # Block timestamps are STRICTLY increasing - the real chain's DPoS
        # slots guarantee this, and the rebuild-safety proof leans on it.
        self._block_time: dict[int, datetime] = {self._head: clock.now()}
        self._txs: dict[str, _TxRecord] = {}   # every tx the node has ever accepted
        self._faults: dict[str, deque[Fault]] = {"broadcast": deque(), "query": deque()}

        # Forensics for tests: every broadcast attempt, whatever its fate.
        self.broadcast_log: list[BroadcastLogEntry] = []

    # -- fault scripting -------------------------------------------------------

    def script(self, channel: str, *faults: Fault) -> None:
        """Queue faults; each call to the channel consumes one. FIFO."""
        with self._lock:
            self._faults.setdefault(channel, deque()).extend(faults)

    def _take_fault(self, channel: str) -> Fault | None:
        q = self._faults[channel]
        return q.popleft() if q else None

    # -- chain geometry --------------------------------------------------------

    @property
    def head_block(self) -> int:
        with self._lock:
            return self._head

    @property
    def solid_block(self) -> int:
        with self._lock:
            return self._head - self.solidity_lag

    def ref_block(self) -> dict[str, Any]:
        """What a wallet asks for before building: TaPoS reference. Taken
        from the latest SOLID block, as real wallets do - referencing a head
        block that later gets orphaned is a TAPOS_ERROR waiting to happen."""
        with self._lock:
            n = max(self.solid_block, min(self._block_time))
            return {"number": n, "hash": f"blk{n:016x}"}

    def produce_blocks(self, n: int = 1) -> None:
        """Advance the chain. Pending transactions get included after
        inclusion_delay blocks - unless they expired first, in which case
        they are silently forgotten, exactly like the real thing."""
        with self._lock:
            for _ in range(n):
                # strictly increasing block time, even if the clock stalls
                t = self.clock.now()
                prev = self._block_time[self._head]
                if t <= prev:
                    t = prev + timedelta(milliseconds=1)
                self._head += 1
                self._block_time[self._head] = t
                for rec in self._txs.values():
                    if rec.included_block is not None:
                        continue
                    if rec.tx.expiration <= t:
                        # network validity rule: a block cannot include a tx
                        # whose expiration precedes the block's timestamp
                        continue
                    if self._head - rec.accepted_at_block >= self.inclusion_delay:
                        rec.included_block = self._head
                        rec.receipt = "REVERTED" if self._pop_revert() else "SUCCESS"
            # purge nothing: the node remembers accepted txids forever so that
            # DUP_TRANSACTION behaves; only *unincluded expired* txs are inert.

    def solid_now(self) -> datetime:
        """Timestamp of the latest SOLID block - the only clock the rebuild
        proof is allowed to consult. Wall clocks drift and lie; a finalized
        block's timestamp is a fact the whole network agreed on."""
        with self._lock:
            solid = max(
                (h for h in self._block_time if h <= self.solid_block),
                default=min(self._block_time),  # chain younger than the lag
            )
            return self._block_time[solid]

    def _pop_revert(self) -> bool:
        q = self._faults.setdefault("receipt", deque())
        if q and q[0] is Fault.REVERT_RECEIPT:
            q.popleft()
            return True
        return False

    def solidify(self) -> None:
        """Produce enough blocks that everything included so far is solid."""
        self.produce_blocks(self.solidity_lag + self.inclusion_delay + 1)

    # -- node API ------------------------------------------------------------

    def broadcast(self, tx: SignedTx, worker_id: str | None = None) -> BroadcastResult:
        with self._lock:
            fault = self._take_fault("broadcast")
            now = self.clock.now()

            def logit(outcome: str) -> None:
                self.broadcast_log.append(
                    BroadcastLogEntry(tx.txid, outcome, self._head, worker_id)
                )

            if fault is Fault.TIMEOUT_DROPPED:
                logit("timeout-dropped")
                raise NodeTimeout("broadcast: connection reset by peer")

            if fault is Fault.BUSY:
                logit("busy")
                return BroadcastResult(BroadcastCode.SERVER_BUSY, "try later")

            if fault is Fault.UNKNOWN_CODE:
                # not processed; the caller has no idea what this means, and
                # the only safe reading of "no idea" is "ambiguous"
                logit("unknown-code")
                return BroadcastResult("PLEASE_INSERT_COIN", "novel failure mode")

            if not self._signer.verify(tx):
                logit("rejected:SIGERROR")
                return BroadcastResult(BroadcastCode.SIGERROR, "bad signature")

            if tx.expiration <= now:
                logit("rejected:EXPIRED")
                return BroadcastResult(BroadcastCode.TRANSACTION_EXPIRATION_ERROR, "expired")

            if tx.txid in self._txs:
                logit("dup")
                return BroadcastResult(BroadcastCode.DUP_TRANSACTION, "already known")

            # accepted for real
            self._txs[tx.txid] = _TxRecord(tx=tx, accepted_at_block=self._head)

            if fault is Fault.TIMEOUT_ACCEPTED:
                logit("timeout-accepted")
                raise NodeTimeout("broadcast: read timed out (but the node kept the tx)")

            logit("accepted")
            return BroadcastResult(BroadcastCode.SUCCESS)

    def find_tx(self, txid: str) -> TxStatus:
        with self._lock:
            if self._take_fault("query") is Fault.QUERY_TIMEOUT:
                raise NodeTimeout("query: timed out")
            rec = self._txs.get(txid)
            if rec is None:
                return TxStatus(TxQuery.UNKNOWN)
            if rec.included_block is None:
                # expired-in-mempool txs are indistinguishable from unknown
                # to a late observer; model that honestly
                if rec.tx.expiration <= self.clock.now():
                    return TxStatus(TxQuery.UNKNOWN)
                return TxStatus(TxQuery.PENDING)
            return TxStatus(
                TxQuery.INCLUDED,
                block=rec.included_block,
                solid=rec.included_block <= self.solid_block,
                receipt=rec.receipt,
            )

    # -- assertions helpers for tests ---------------------------------------------

    def included_txids(self) -> set[str]:
        with self._lock:
            return {t for t, r in self._txs.items() if r.included_block is not None}

    def successful_txids(self) -> set[str]:
        with self._lock:
            return {
                t for t, r in self._txs.items()
                if r.included_block is not None and r.receipt == "SUCCESS"
            }

    def accepted_count(self, txid: str) -> int:
        """How many times a txid was *newly accepted* (dups don't count)."""
        with self._lock:
            return sum(
                1 for e in self.broadcast_log
                if e.txid == txid and e.outcome in ("accepted", "timeout-accepted")
            )

    def tx_amount(self, txid: str) -> int:
        """Transfer amount (base units) carried by an accepted tx - lets the
        verifier check conservation of money against the chain itself."""
        with self._lock:
            return int(self._txs[txid].tx.raw_data["contract"]["amount_units"])
