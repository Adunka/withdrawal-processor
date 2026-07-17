"""Crash points - scripted kill -9.

The pipeline is peppered with `crash.here("...")` markers at the instants
that matter (right before a broadcast hits the wire, right after, ...).
In production the switch is never armed and the call is a dict miss.
In tests, arming a point makes the worker die *there*, mid-thought, with
no cleanup, no rollback of already-committed writes, no lease release -
which is precisely what a SIGKILL or a kernel panic leaves behind.

SimulatedCrash subclasses BaseException on purpose: the worker's per-task
`except Exception` guard - the thing that keeps an ordinary bug from
killing the loop - must NOT be able to swallow a simulated power cut.
"""

from __future__ import annotations

import threading
from collections import Counter


class SimulatedCrash(BaseException):
    pass


class CrashSwitch:
    def __init__(self) -> None:
        self._armed: Counter[str] = Counter()
        self._lock = threading.Lock()
        self.tripped: list[str] = []

    def arm(self, point: str, times: int = 1) -> None:
        with self._lock:
            self._armed[point] += times

    def here(self, point: str) -> None:
        with self._lock:
            if self._armed.get(point, 0) > 0:
                self._armed[point] -= 1
                self.tripped.append(point)
                raise SimulatedCrash(point)


# Documented crash points, so tests reference names instead of string typos.
SIGN_MID = "sign.between_build_and_sign"
BROADCAST_PRE_SEND = "broadcast.pre_send"       # state says broadcasting, wire untouched
BROADCAST_POST_SEND = "broadcast.post_send"     # wire touched, DB doesn't know yet
CONFIRM_PRE_WRITE = "confirm.pre_write"
