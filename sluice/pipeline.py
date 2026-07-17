"""Per-state handlers. This module is the machine the README describes.

Ground rules the code below never breaks:

  1. Every write is a fenced CAS. If it returns None, some other worker owns
     the operation now; we raise LostClaim and walk away mid-sentence.
  2. State `broadcasting` is written BEFORE the network call (write-ahead
     intent). A crash can therefore leave us not-knowing whether the bytes
     left the building - but never in a state where the bytes left and the
     DB has no trace of what they were.
  3. A transaction is rebuilt (new txid) only when the previous one is
     PROVABLY dead: expiration + safety margin passed AND the node does not
     know the txid. Anything less than proof means wait, or re-send the
     exact same bytes - which is harmless by construction, because the same
     bytes carry the same txid and the node dedups on it.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

from .config import Config
from .crashpoints import CrashSwitch
from .model import Operation
from .states import OpState as S
from .store import Store
from .tron import address
from .tron.mocknet import MockTronNode
from .tron.signer import MockSigner
from .tron.types import (
    ACCEPTED,
    HARD_REJECT,
    RETRY_SAME_BYTES,
    STALE_ENVELOPE,
    NodeTimeout,
    SignedTx,
    TxQuery,
    UnsignedTx,
    compute_txid,
)

log = logging.getLogger("sluice.pipeline")


class LostClaim(Exception):
    """Our fencing token stopped matching. Somebody else is driving now."""


class Pipeline:
    def __init__(
        self,
        store: Store,
        node: MockTronNode,
        signer: MockSigner,
        cfg: Config,
        crash: CrashSwitch | None = None,
    ):
        self.store = store
        self.node = node
        self.signer = signer
        self.cfg = cfg
        self.crash = crash or CrashSwitch()

    # ----------------------------------------------------------------- plumbing

    def _cas(self, op: Operation, to_state: S, worker_id: str, **kw) -> Operation:
        updated = self.store.cas(
            op.operation_id,
            op.fencing_token,
            from_state=op.state,
            to_state=to_state,
            worker_id=worker_id,
            renew_lease_seconds=self.cfg.lease_seconds,
            **kw,
        )
        if updated is None:
            raise LostClaim(f"{op.operation_id} fenced at {op.state} -> {to_state}")
        return updated

    def step(self, op: Operation, worker_id: str) -> Operation:
        """Advance one macro-step. Dispatch is by state; the handler for a
        state doubles as its crash-recovery procedure, because a reclaimed
        operation is indistinguishable from a fresh one on purpose."""
        handler = {
            S.REQUESTED: self._validate,
            S.VALIDATED: self._build_and_sign,
            S.SIGNING: self._build_and_sign,     # recovery re-enters here safely
            S.SIGNED: self._broadcast,
            S.BROADCASTING: self._reconcile_broadcast,
            S.BROADCAST: self._await_confirmation,
        }[op.state]
        return handler(op, worker_id)

    # ----------------------------------------------------------------- validate

    def _validate(self, op: Operation, worker_id: str) -> Operation:
        # Intake already shape-checked everything; this stage re-checks under
        # worker authority (defense in depth) and is where balance/limits/
        # compliance hooks would live in a real deployment.
        if not address.is_valid(op.to_address):
            return self._cas(op, S.REJECTED, worker_id,
                             sets={"failure_reason": "invalid destination address"})
        if op.asset != self.cfg.asset:
            return self._cas(op, S.REJECTED, worker_id,
                             sets={"failure_reason": f"unsupported asset {op.asset}"})
        if not (self.cfg.min_amount_units <= op.amount_units <= self.cfg.max_amount_units):
            return self._cas(op, S.REJECTED, worker_id,
                             sets={"failure_reason": "amount out of bounds"})
        return self._cas(op, S.VALIDATED, worker_id)

    # ------------------------------------------------------------- build & sign

    def _build_and_sign(self, op: Operation, worker_id: str) -> Operation:
        if op.state == S.VALIDATED:
            op = self._cas(op, S.SIGNING, worker_id)

        if op.attempts >= self.cfg.max_build_attempts:
            return self._cas(op, S.FAILED, worker_id,
                             sets={"failure_reason": f"gave up after {op.attempts} build attempts"})

        # Build a fresh envelope: TaPoS ref block + expiration + timestamp.
        # The timestamp guarantees a rebuild yields a NEW txid: a rebuild is
        # only reachable after the previous envelope's expiration window has
        # fully passed, so the clock has provably moved since the old build
        # and the old and new txids can never be confused - on chain or in
        # our own bookkeeping.
        ref = self.node.ref_block()
        now = self.cfg.clock.now()
        expiration = now + timedelta(seconds=self.cfg.tx_ttl_seconds)
        raw_data: dict[str, Any] = {
            "contract": {
                "type": "TriggerSmartContract",
                "contract_address": self.cfg.usdt_contract,
                "call": "transfer(address,uint256)",
                "to": op.to_address,
                "amount_units": str(op.amount_units),
            },
            "ref_block_num": ref["number"],
            "ref_block_hash": ref["hash"],
            "timestamp_ms": int(now.timestamp() * 1000),
            "expiration_ms": int(expiration.timestamp() * 1000),
        }
        unsigned = UnsignedTx(raw_data=raw_data, txid=compute_txid(raw_data), expiration=expiration)

        # Persist intent-to-sign - txid is now durable BEFORE any signature
        # exists, and long before any broadcast. Same-state write.
        op = self._cas(
            op, S.SIGNING, worker_id,
            sets={
                "unsigned_tx": unsigned.raw_data,
                "txid": unsigned.txid,
                "tx_expiration": unsigned.expiration,
                "signed_tx": None,
            },
            detail={"txid": unsigned.txid, "reason": "envelope built"},
        )

        self.crash.here("sign.between_build_and_sign")

        # Mock signing is local and side-effect free, so a crash anywhere in
        # this stage recovers by simply rebuilding. NOTE: with a remote
        # HSM/MPC signer that stops being true - sign() could succeed after
        # our timeout - and this stage must grow the same query-before-retry
        # reconcile shape as _reconcile_broadcast below. The state machine
        # already leaves room for it.
        signed = self.signer.sign(unsigned)
        return self._cas(
            op, S.SIGNED, worker_id,
            sets={"signed_tx": signed.to_json()},
        )

    # ---------------------------------------------------------------- broadcast

    def _broadcast(self, op: Operation, worker_id: str) -> Operation:
        signed = SignedTx.from_json(op.signed_tx)
        now = self.cfg.clock.now()

        # Envelope already stale? Don't waste the network call. Rebuilding
        # here without the full death-proof is sound for one narrow reason:
        # an envelope in `signed` has NEVER been broadcast (the only door to
        # the wire is the write-ahead below), so there is nothing on the
        # network for a new txid to double.
        if now >= signed.expiration:
            return self._rebuild(op, worker_id, reason="expired before first send",
                                 evidence={"rule": "never-broadcast envelope; local expiry check"})

        # WRITE-AHEAD: commit `broadcasting` (and by extension the txid we
        # are about to send) before touching the wire. If we die between
        # here and the send, recovery sees a txid that provably never left -
        # or did - and can find out which. Dying with bytes on the wire and
        # nothing in the DB is the one script we refuse to be in.
        op = self._cas(op, S.BROADCASTING, worker_id,
                       detail={"txid": signed.txid, "reason": "write-ahead"})

        self.crash.here("broadcast.pre_send")
        try:
            result = self.node.broadcast(signed, worker_id=worker_id)
        except NodeTimeout:
            # The forbidden assumption would be "it failed". We assume
            # nothing: stay in `broadcasting`, let the reconciler find out.
            log.warning("broadcast timeout for %s txid=%s - reconcile later",
                        op.operation_id, signed.txid)
            return op
        self.crash.here("broadcast.post_send")

        return self._settle_broadcast_result(op, worker_id, signed, result)

    def _settle_broadcast_result(
        self, op: Operation, worker_id: str, signed: SignedTx, result
    ) -> Operation:
        code = str(result.code)
        if result.code in ACCEPTED:
            # DUP_TRANSACTION lands here too: "already know it" == "accepted".
            return self._cas(op, S.BROADCAST, worker_id,
                             sets={"broadcast_at": self.cfg.clock.now()},
                             detail={"txid": signed.txid, "code": code})
        if result.code in HARD_REJECT:
            return self._cas(op, S.FAILED, worker_id,
                             sets={"failure_reason": f"node rejected: {code}"},
                             detail={"txid": signed.txid, "code": code})
        if result.code in STALE_ENVELOPE:
            # The node CLAIMS the envelope is expired/stale. We don't rebuild
            # on a node's word - an earlier attempt with this txid could still
            # be sitting in some mempool. Park in `broadcasting`; the
            # reconciler will either find the tx or prove its death properly.
            return self._cas(op, S.BROADCASTING, worker_id,
                             sets={"last_error": f"node says {code}; awaiting proof"},
                             detail={"txid": signed.txid, "code": code})
        if result.code in RETRY_SAME_BYTES:
            log.info("node busy for %s, will re-send same bytes", op.operation_id)
            return op
        # Unrecognized code: the node said words we don't know. Assume
        # nothing - stay in `broadcasting`, let the reconciler ask the chain.
        log.warning("unclassified broadcast code %r for %s - treating as ambiguous",
                    code, op.operation_id)
        return self._cas(op, S.BROADCASTING, worker_id,
                         sets={"last_error": f"unclassified node response: {code}"},
                         detail={"txid": signed.txid, "code": code, "classified": "ambiguous"})

    # --------------------------------------------------------------- reconcile
    #
    # An operation is sitting in `broadcasting`. That means some worker - maybe
    # us a second ago, maybe a machine that no longer exists - committed the
    # intent to send txid T and we cannot trust anyone's memory of what
    # happened next. This is the paragraph that pays the rent:
    #
    #   node.find_tx(T)  ->  INCLUDED   advance; the money moved, exactly once
    #                        PENDING    advance; accepted, waiting for a block
    #                        UNKNOWN    two worlds, split by the clock:
    #                          now <  expiration+margin : T may still be in
    #                              flight somewhere. Re-send the SAME bytes
    #                              (idempotent: same txid, node dedups) and
    #                              keep waiting. NEVER re-sign here.
    #                          now >= expiration+margin : the network can no
    #                              longer accept T even in principle. T is
    #                              dead by protocol rule, not by our opinion.
    #                              Only now is a rebuild safe.

    def _reconcile_broadcast(self, op: Operation, worker_id: str) -> Operation:
        signed = SignedTx.from_json(op.signed_tx)
        try:
            status = self.node.find_tx(signed.txid)
        except NodeTimeout:
            return op  # can't even ask; keep the claim, try next cycle

        if status.result in (TxQuery.INCLUDED, TxQuery.PENDING):
            return self._cas(op, S.BROADCAST, worker_id,
                             sets={"broadcast_at": op.broadcast_at or self.cfg.clock.now()},
                             detail={"txid": signed.txid, "via": "reconcile",
                                     "found": status.result.value})

        dead, evidence = self._proof_of_death(signed)
        if not dead:
            # Not provably dead. Re-sending identical bytes is free of risk.
            try:
                result = self.node.broadcast(signed, worker_id=worker_id)
            except NodeTimeout:
                return op
            return self._settle_broadcast_result(op, worker_id, signed, result)

        return self._rebuild(op, worker_id, reason="expired unseen; provably dead",
                             evidence=evidence)

    def _proof_of_death(self, signed: SignedTx) -> tuple[bool, dict[str, Any]]:
        """The permission slip for a rebuild. Both conditions, no exceptions:

          1. the SOLID chain time (a finalized block's timestamp, fetched
             from the node - never our wall clock, which drifts, jumps and
             lies) is past expiration + safety margin, and
          2. the node does not know the txid (checked by our caller).

        Why that proves the old txid can never land: a block whose timestamp
        exceeds a tx's expiration cannot include it (network validity rule),
        block timestamps rise strictly with height, and solid blocks are
        irreversible. So every future block is timestamped past the deadline
        and every past block was checked. There is no third place."""
        solid_time = self.node.solid_now()
        deadline = signed.expiration + timedelta(seconds=self.cfg.expiration_safety_seconds)
        evidence = {
            "solid_chain_time": solid_time.isoformat(),
            "solid_block": self.node.solid_block,
            "tx_expiration": signed.expiration.isoformat(),
            "safety_margin_s": self.cfg.expiration_safety_seconds,
            "node_lookup": "unknown",
        }
        return solid_time >= deadline, evidence

    # ------------------------------------------------------------- confirmation

    def _await_confirmation(self, op: Operation, worker_id: str) -> Operation:
        signed = SignedTx.from_json(op.signed_tx)
        try:
            status = self.node.find_tx(signed.txid)
        except NodeTimeout:
            return op

        if status.result == TxQuery.INCLUDED:
            if not status.solid:
                return op  # in a block, waiting for solidity
            self.crash.here("confirm.pre_write")
            if status.receipt == "SUCCESS":
                return self._cas(op, S.CONFIRMED, worker_id,
                                 sets={"included_block": status.block,
                                       "confirmed_at": self.cfg.clock.now()},
                                 detail={"txid": signed.txid, "block": status.block})
            return self._cas(op, S.FAILED, worker_id,
                             sets={"included_block": status.block,
                                   "failure_reason": "transaction reverted on chain"},
                             detail={"txid": signed.txid, "receipt": status.receipt})

        if status.result == TxQuery.PENDING:
            return op

        # UNKNOWN after the node said "accepted"? Nodes drop mempools on
        # restart; congestion outlives expirations. Same proof rule applies.
        dead, evidence = self._proof_of_death(signed)
        if dead:
            return self._rebuild(op, worker_id, reason="accepted but never mined; expired",
                                 evidence=evidence)
        return op

    # ------------------------------------------------------------------ rebuild

    def _rebuild(self, op: Operation, worker_id: str, reason: str,
                 evidence: dict[str, Any] | None = None) -> Operation:
        """Retire the current envelope and go back to SIGNING for a fresh one.
        Callers must have PROVEN the old txid can never land, and the proof
        travels with the decision: `expiry_evidence` lands in the event log,
        where the chaos verifier (and any auditor with a grudge) can re-check
        it against the chain after the fact."""
        log.info("rebuilding %s: %s (attempt %d)", op.operation_id, reason, op.attempts + 1)
        return self._cas(
            op, S.SIGNING, worker_id,
            sets={"attempts": op.attempts + 1, "last_error": reason,
                  "signed_tx": None, "unsigned_tx": None,
                  "txid": None, "tx_expiration": None},
            detail={"retired_txid": op.txid, "reason": reason,
                    "expiry_evidence": evidence or {}},
        )
