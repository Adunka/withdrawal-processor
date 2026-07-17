"""Shared test rig.

World() wires a fake clock, the in-memory store, the mock chain, a crash
switch and a pipeline into one fixture. Time only moves when a test says so,
blocks only get mined when a test says so - which is what makes scenarios
like "the transaction expired while every worker was dead" a three-line
test instead of a flaky sleep-fest.
"""

from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field

from sluice.config import Config, FakeClock
from sluice.crashpoints import CrashSwitch, SimulatedCrash
from sluice.model import Operation
from sluice.pipeline import Pipeline
from sluice.service import SubmitResult, submit_withdrawal
from sluice.store.memory import MemoryStore
from sluice.tron import address
from sluice.tron.mocknet import MockTronNode
from sluice.tron.signer import MockSigner
from sluice.worker import Worker, drain


def test_address(n: int = 1) -> str:
    """Mint a deterministic, checksum-valid TRON address."""
    return address.from_payload(n.to_bytes(20, "big"))


@dataclass
class World:
    clock: FakeClock = field(default_factory=FakeClock)

    def __post_init__(self):
        self.cfg = Config(clock=self.clock)
        self.store = MemoryStore(self.clock)
        self.node = MockTronNode(self.clock, solidity_lag=2, inclusion_delay_blocks=1)
        self.signer = MockSigner()
        self.crash = CrashSwitch()
        self.pipeline = Pipeline(self.store, self.node, self.signer, self.cfg, self.crash)

    # -- intake ------------------------------------------------------------

    def payload(self, **over) -> dict:
        p = {
            "operation_id": str(uuid.uuid4()),
            "to_address": test_address(1),
            "amount": "125.5",
            "asset": "USDT-TRC20",
        }
        p.update(over)
        return p

    def submit(self, **over) -> SubmitResult:
        return submit_withdrawal(self.store, self.cfg, self.payload(**over))

    # -- running workers -----------------------------------------------------

    def worker(self, name: str = "w1") -> Worker:
        return Worker(self.store, self.pipeline, self.cfg, name=name)

    def drain(self, name: str = "drain") -> None:
        drain(self.store, self.pipeline, self.cfg, worker_id=name)

    def run_worker_expecting_crash(self, name: str = "doomed") -> list[SimulatedCrash]:
        """Run one claim cycle in a real thread and let an armed crash point
        kill it. The wrapper catches SimulatedCrash purely to report it -
        by then the worker has already died mid-write with its lease held
        and zero cleanup, which is the whole point."""
        crashes: list[SimulatedCrash] = []
        w = self.worker(name)

        def target():
            try:
                w.run_once()
            except SimulatedCrash as e:  # noqa: PERF203 - single iteration
                crashes.append(e)

        t = threading.Thread(target=target, name=name)
        t.start()
        t.join(timeout=10)
        assert not t.is_alive(), "crashing worker wedged instead of dying"
        return crashes

    # -- little conveniences ---------------------------------------------------

    def op(self, operation_id) -> Operation:
        got = self.store.get(uuid.UUID(str(operation_id)))
        assert got is not None
        return got

    def settle_chain(self) -> None:
        """Mine + solidify whatever is pending."""
        self.node.solidify()

    def expire_leases(self) -> None:
        self.clock.advance(self.cfg.lease_seconds + 1)

    def pass_expiration_deadline(self) -> None:
        """Jump past tx expiration + safety margin AND let the chain notice:
        the death-proof reads solid block timestamps, not the wall clock, so
        blocks must actually be produced at the later time. (Any tx still
        waiting in the mempool expires and is silently dropped here - that's
        the network rule, and the mock enforces it.)"""
        self.clock.advance(self.cfg.tx_ttl_seconds + self.cfg.expiration_safety_seconds + 1)
        self.node.solidify()
