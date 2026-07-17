"""Mock signer.

Deliberately fake, deliberately shaped like the real thing. sign() takes an
unsigned transaction and returns bytes-over-the-wire; verify() is what the
mock network uses to reject garbage. The "signature" is an HMAC over the
txid with a per-key secret - deterministic, stateless, and impossible to
forge by accident in tests.

What changes with a real KMS/HSM/MPC signer: sign() becomes a remote call
that can time out AFTER producing a signature, which turns the `signing`
state into the same reconcile-don't-assume problem as broadcasting. The
recovery hook for that lives in pipeline.py next to a note; the state
machine already has room for it.
"""

from __future__ import annotations

import hashlib
import hmac

from .types import SignedTx, UnsignedTx


class MockSigner:
    def __init__(self, key_id: str = "hotwallet-1", secret: bytes = b"not-a-real-key"):
        self.key_id = key_id
        self._secret = secret

    def sign(self, tx: UnsignedTx) -> SignedTx:
        sig = hmac.new(self._secret, tx.txid.encode(), hashlib.sha256).hexdigest()
        return SignedTx(
            raw_data=tx.raw_data,
            txid=tx.txid,
            expiration=tx.expiration,
            signature=f"{self.key_id}:{sig}",
        )

    def verify(self, tx: SignedTx) -> bool:
        try:
            key_id, sig = tx.signature.split(":", 1)
        except ValueError:
            return False
        want = hmac.new(self._secret, tx.txid.encode(), hashlib.sha256).hexdigest()
        return key_id == self.key_id and hmac.compare_digest(sig, want)
