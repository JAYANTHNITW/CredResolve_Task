"""Worker-local circuit breaker driven by placement outcomes."""

from collections import deque
from enum import StrEnum
from threading import Lock

from smartdialer.clock import Clock


class BreakerState(StrEnum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class Outcome(StrEnum):
    SUCCESS = "SUCCESS"
    REJECTED = "REJECTED"
    TIMEOUT = "TIMEOUT"


class CircuitBreaker:
    def __init__(self, clock: Clock):
        self._clock = clock
        self._outcomes: deque[Outcome] = deque(maxlen=20)
        self._timeouts = 0
        self._opened_at: int | None = None
        self._probe_in_flight = False
        self._lock = Lock()

    @property
    def state(self) -> BreakerState:
        if self._opened_at is None:
            return BreakerState.CLOSED
        if self._clock.now_ms() - self._opened_at >= 10_000:
            return BreakerState.HALF_OPEN
        return BreakerState.OPEN

    @property
    def failure_rate(self) -> float:
        return (
            1 - self._outcomes.count(Outcome.SUCCESS) / len(self._outcomes)
            if self._outcomes
            else 0.0
        )

    def allows(self) -> bool:
        with self._lock:
            state = self.state
            if state == BreakerState.CLOSED:
                return True
            if state == BreakerState.OPEN or self._probe_in_flight:
                return False
            self._probe_in_flight = True
            return True

    def record(self, outcome: Outcome) -> None:
        with self._lock:
            if self._opened_at is not None:
                if self._probe_in_flight:
                    self._probe_in_flight = False
                    if outcome == Outcome.SUCCESS:
                        self._opened_at = None
                        self._outcomes.clear()
                        self._timeouts = 0
                    else:
                        self._opened_at = self._clock.now_ms()
                return
            self._outcomes.append(outcome)
            self._timeouts = self._timeouts + 1 if outcome == Outcome.TIMEOUT else 0
            if self._timeouts >= 5 or (
                len(self._outcomes) >= 10 and self.failure_rate > 0.5
            ):
                self._opened_at = self._clock.now_ms()
