"""The worker loop.

Claim a batch (fenced), drive each operation forward until it parks or
finishes, release, sleep with jitter, repeat. Boring on purpose - all the
cleverness lives in the store's claim semantics and the pipeline's handlers.

Failure philosophy: an exception from a handler marks the operation's
last_error (best effort) and moves on; a SimulatedCrash is a BaseException
and rips straight through, leaving leases un-released and half-finished
state behind - exactly the debris a real crash leaves, which is exactly
what the recovery paths are tested against.
"""

from __future__ import annotations

import logging
import os
import random
import socket
import threading

from .config import Config
from .pipeline import LostClaim, Pipeline
from .states import CLAIMABLE, TERMINAL
from .store import Store

log = logging.getLogger("sluice.worker")

# An operation can legally move through at most ~7 states in one sitting;
# anything past that means a handler is treading water and should yield.
MAX_STEPS_PER_CLAIM = 8


class Worker:
    def __init__(self, store: Store, pipeline: Pipeline, cfg: Config, name: str | None = None):
        self.store = store
        self.pipeline = pipeline
        self.cfg = cfg
        self.worker_id = name or f"{socket.gethostname()}:{os.getpid()}:{id(self) & 0xffff:x}"
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    # -- one pass ------------------------------------------------------------

    def run_once(self) -> int:
        """Claim once, work everything claimed. Returns how many operations
        actually ADVANCED (changed state or finished) - parked ones waiting
        on the chain don't count, so idle loops can tell they're idle."""
        ops = self.store.claim(
            CLAIMABLE, self.cfg.claim_batch, self.worker_id, self.cfg.lease_seconds
        )
        return sum(self._drive(op) for op in ops)

    def _drive(self, op) -> int:
        advanced = 0
        try:
            for _ in range(MAX_STEPS_PER_CLAIM):
                before = op.state
                op = self.pipeline.step(op, self.worker_id)
                if op.state != before:
                    advanced = 1
                if op.state in TERMINAL:
                    return 1  # cas() into a terminal state already dropped the lease
                if op.state == before:
                    break  # parked (waiting on chain/time); let it go for now
        except LostClaim:
            # Someone fenced us out mid-flight. They own it; nothing to undo
            # because every write we made was token-checked and theirs won.
            log.info("%s lost claim on %s", self.worker_id, op.operation_id)
            return advanced
        except Exception:
            log.exception("%s handler error on %s", self.worker_id, op.operation_id)
            # best effort breadcrumb; if we've been fenced meanwhile, so be it
            self.store.cas(
                op.operation_id, op.fencing_token,
                from_state=op.state, to_state=op.state,
                sets={"last_error": "handler exception (see logs)"},
                worker_id=self.worker_id,
            )
        self.store.release(op.operation_id, op.fencing_token)
        return advanced

    # -- forever ------------------------------------------------------------

    def run_forever(self) -> None:
        log.info("worker %s up", self.worker_id)
        while not self._stop.is_set():
            try:
                touched = self.run_once()
            except Exception:
                log.exception("claim cycle failed; backing off")
                touched = 0
            if touched == 0:
                # jitter so a fleet doesn't hammer the table in lockstep
                self._stop.wait(self.cfg.poll_interval * random.uniform(0.5, 1.5))


def drain(store: Store, pipeline: Pipeline, cfg: Config, worker_id: str = "drain") -> None:
    """Test/demo helper: run single-threaded until nothing can advance.
    Parked operations (waiting for blocks or the clock) don't keep it
    spinning - callers advance the mock chain/clock and drain again."""
    w = Worker(store, pipeline, cfg, name=worker_id)
    for _ in range(1000):
        if w.run_once() == 0:
            return
    raise RuntimeError("drain did not settle in 1000 cycles - livelock?")
