#!/usr/bin/env python3
"""A ninety-second story with no dependencies:  python3 scripts/demo.py

    1. two withdrawals come in; one of them is accidentally submitted twice
    2. the network accepts a transaction and then times out anyway
    3. a worker is killed mid-broadcast, kill -9 style
    4. a fresh worker inherits the mess, asks the chain what really
       happened, and finishes the job
    5. the full event timeline is printed - the same audit trail the
       invariant checks read

Run it, read the timeline bottom-up, and the design mostly explains itself.
"""

import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sluice import crashpoints as cp
from sluice.config import Config, FakeClock
from sluice.crashpoints import CrashSwitch, SimulatedCrash
from sluice.pipeline import Pipeline
from sluice.service import submit_withdrawal
from sluice.states import TERMINAL
from sluice.store.memory import MemoryStore
from sluice.tron import address
from sluice.tron.mocknet import Fault, MockTronNode
from sluice.tron.signer import MockSigner
from sluice.worker import Worker


def say(text: str) -> None:
    print(f"\n\033[1m» {text}\033[0m")


def main() -> None:
    clock = FakeClock()
    cfg = Config(clock=clock)
    store = MemoryStore(clock)
    node = MockTronNode(clock, solidity_lag=2, inclusion_delay_blocks=1)
    crash = CrashSwitch()
    pipeline = Pipeline(store, node, MockSigner(), cfg, crash)

    say("Two withdrawal requests arrive over the API")
    pay_alice = {
        "operation_id": str(uuid.uuid4()),
        "to_address": address.from_payload(b"\x0a" * 20),
        "amount": "1250.75",
        "asset": "USDT-TRC20",
    }
    pay_bob = {
        "operation_id": str(uuid.uuid4()),
        "to_address": address.from_payload(b"\x0b" * 20),
        "amount": "9.99",
        "asset": "USDT-TRC20",
    }
    r1 = submit_withdrawal(store, cfg, pay_alice)
    r2 = submit_withdrawal(store, cfg, pay_bob)
    print(f"  alice -> {r1.operation.operation_id}  created={r1.created}")
    print(f"  bob   -> {r2.operation.operation_id}  created={r2.created}")

    say("Alice's client times out and RETRIES the exact same request")
    r3 = submit_withdrawal(store, cfg, pay_alice)
    print(f"  replay detected: created={r3.created}, same row, {len(store.all_operations())} rows total")

    say("The network is scripted to ACCEPT the next broadcast and then time out anyway")
    node.script("broadcast", Fault.TIMEOUT_ACCEPTED)

    say("Worker w1 is rigged to die kill -9 style right after Alice's write-ahead commit")
    crash.arm(cp.BROADCAST_PRE_SEND)

    w1 = Worker(store, pipeline, cfg, name="w1")
    try:
        while w1.run_once():
            pass
    except SimulatedCrash:
        print("  w1 crashed mid-broadcast. No cleanup ran. Its lease is still held.")

    for oid in (r1.operation.operation_id, r2.operation.operation_id):
        op = store.get(oid)
        print(f"  {op.claimed_by or '-':>3} | {op.state.value:<13} txid={str(op.txid)[:16]}…")

    say(f"Nobody notices for a while… lease ({cfg.lease_seconds}s) expires")
    clock.advance(cfg.lease_seconds + 1)

    say("Worker w2 starts, claims the wreckage, and asks the CHAIN what happened")
    w2 = Worker(store, pipeline, cfg, name="w2")
    while w2.run_once():
        pass
    node.solidify()   # blocks get produced, then become solid
    while w2.run_once():
        pass

    say("Final states")
    for name, res in (("alice", r1), ("bob", r2)):
        op = store.get(res.operation.operation_id)
        assert op.state in TERMINAL
        print(f"  {name}: {op.state.value}  block={op.included_block}  txid={op.txid[:16]}…")
        assert node.accepted_count(op.txid) == 1, "network saw it exactly once"

    say("Event timeline (what the auditors and the chaos verifier read)")
    for res, who in ((r1, "alice"), (r2, "bob")):
        print(f"  --- {who} ---")
        for e in store.events(res.operation.operation_id):
            frm = e.from_state.value if e.from_state else "·"
            extra = ""
            if "txid" in e.detail:
                extra = f"  txid={e.detail['txid'][:12]}…"
            if "reason" in e.detail:
                extra += f"  ({e.detail['reason']})"
            if "found" in e.detail:
                extra += f"  [chain said: {e.detail['found']}]"
            who = e.worker_id or "intake"
            print(f"  {e.seq:>2}  {frm:>13} -> {e.to_state.value:<13} {who:<7}{extra}")

    say("Done: one crash, one lying timeout, one duplicate request - zero double spends")


if __name__ == "__main__":
    main()
