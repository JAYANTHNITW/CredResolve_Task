"""Time sources for persisted timestamps and deterministic simulations."""

from time import time_ns
from typing import Protocol


class Clock(Protocol):
    def now_ms(self) -> int: ...


class SystemClock:
    def now_ms(self) -> int:
        return time_ns() // 1_000_000


class FakeClock:
    def __init__(self, start_ms: int = 0):
        if type(start_ms) is not int:
            raise TypeError("start_ms must be integer milliseconds")
        self._now_ms = start_ms

    def now_ms(self) -> int:
        return self._now_ms

    def advance(self, ms: int) -> None:
        if type(ms) is not int:
            raise TypeError("ms must be integer milliseconds")
        if ms < 0:
            raise ValueError("advance requires a nonnegative duration")
        self._now_ms += ms
