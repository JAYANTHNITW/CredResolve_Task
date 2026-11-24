"""Provider contract and deterministic simulation primitives. All times are milliseconds."""

import heapq
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from functools import partial
from itertools import count
from random import Random
from typing import Protocol

from smartdialer.clock import Clock
from smartdialer.models import CallState, FailReason


@dataclass(frozen=True)
class Accepted:
    provider_ref: str


@dataclass(frozen=True)
class Rejected:
    reason: FailReason


PlaceResult = Accepted | Rejected


class ProviderTimeout(TimeoutError):
    """Placement acknowledgement was lost. The call may exist."""


class ProviderStatus(StrEnum):
    UNKNOWN = "UNKNOWN"
    INITIATED = "INITIATED"
    RINGING = "RINGING"
    ANSWERED = "ANSWERED"
    ENDED = "ENDED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class ProviderEvent:
    provider: str
    provider_event_id: str | None
    call_id: str
    type: CallState
    occurred_at: int
    fail_reason: FailReason | None = None

    @property
    def target_state(self) -> CallState:
        return self.type


class Provider(Protocol):
    name: str

    def place_call(
        self, call_id: str, to: str, idempotency_key: str
    ) -> PlaceResult: ...
    def hangup(self, call_id: str) -> None: ...
    def get_status(self, call_id: str) -> ProviderStatus: ...
    def set_event_sink(self, sink: Callable[[ProviderEvent], None]) -> None: ...


class Scheduler:
    def __init__(self):
        self._queue: list[tuple[int, int, Callable[[], None]]] = []
        self._order = count()

    def schedule(self, due_ms: int, callback: Callable[[], None]) -> None:
        heapq.heappush(self._queue, (due_ms, next(self._order), callback))

    def run_until(self, now_ms: int) -> None:
        while self._queue and self._queue[0][0] <= now_ms:
            _, _, callback = heapq.heappop(self._queue)
            callback()


class MockProvider:
    """Latency delays ringing. Delivery chaos affects notifications independently of status."""

    name: str
    latency_ms: tuple[int, int]
    default_failure_rate: float
    timeout_on_failure = False
    duplicate_rate = reorder_rate = loss_rate = 0.0

    def __init__(
        self,
        clock: Clock,
        rng: Random,
        scheduler: Scheduler,
        *,
        answer_rate_fn: Callable[[int], float] = lambda now: 0.5,
        ring_time_dist: Callable[[Random], int] = lambda rng: rng.randint(1000, 5000),
        talk_time_dist: Callable[[Random], int] = lambda rng: rng.randint(
            30000, 120000
        ),
    ):
        self.clock, self.rng, self.scheduler = clock, rng, scheduler
        self.answer_rate_fn = answer_rate_fn
        self.ring_time_dist, self.talk_time_dist = ring_time_dist, talk_time_dist
        self._failure_rate = self.default_failure_rate
        self._extra_latency = 0
        self._outage = False
        self._accepted: dict[str, Accepted] = {}
        self._status: dict[str, ProviderStatus] = {}
        self._sink: Callable[[ProviderEvent], None] = lambda event: None
        self._event_ids = count()

    @property
    def call_count(self) -> int:
        return len(self._accepted)

    def set_failure_rate(self, rate: float) -> None:
        if not 0 <= rate <= 1:
            raise ValueError("failure rate must be between 0 and 1")
        self._failure_rate = rate

    def set_outage(self, outage: bool) -> None:
        self._outage = outage

    def set_extra_latency(self, ms: int) -> None:
        if ms < 0:
            raise ValueError("latency must be nonnegative")
        self._extra_latency = ms

    def set_event_sink(self, sink: Callable[[ProviderEvent], None]) -> None:
        self._sink = sink

    def get_status(self, call_id: str) -> ProviderStatus:
        return self._status.get(call_id, ProviderStatus.UNKNOWN)

    def place_call(self, call_id: str, to: str, idempotency_key: str) -> PlaceResult:
        if idempotency_key in self._accepted:
            return self._accepted[idempotency_key]
        if self._outage or call_id in self._status:
            return Rejected(FailReason.PROVIDER_ERROR)
        failed = self.rng.random() < self._failure_rate
        if failed:
            if not self.timeout_on_failure:
                return Rejected(FailReason.PROVIDER_ERROR)
            if self.rng.random() < 0.5:
                raise ProviderTimeout(self.name)

        now = self.clock.now_ms()
        ringing = now + self.rng.randint(*self.latency_ms) + self._extra_latency
        answered = self.rng.random() < self.answer_rate_fn(now)
        pickup = ringing + self.ring_time_dist(self.rng)
        end = pickup + self.talk_time_dist(self.rng) if answered else pickup
        if not ringing <= pickup <= end:
            raise ValueError("ring and talk durations must be nonnegative")
        result = Accepted(f"{self.name}-{call_id}")
        self._accepted[idempotency_key] = result
        self._status[call_id] = ProviderStatus.INITIATED
        self._schedule(call_id, ringing, CallState.RINGING, ProviderStatus.RINGING, end)
        if answered:
            self._schedule(
                call_id, pickup, CallState.ANSWERED, ProviderStatus.ANSWERED, end
            )
            self._schedule(call_id, end, CallState.COMPLETED, ProviderStatus.ENDED, end)
        else:
            self._schedule(call_id, end, CallState.FAILED, ProviderStatus.FAILED, end)
        if failed:
            raise ProviderTimeout(self.name)
        return result

    def _schedule(self, call_id, due, state, status, end):
        self.scheduler.schedule(
            due, partial(self._transition, call_id, due, state, status, end)
        )

    def _transition(self, call_id, due, state, status, end):
        if self._status[call_id] in (ProviderStatus.ENDED, ProviderStatus.FAILED):
            return
        self._status[call_id] = status
        event = ProviderEvent(
            self.name,
            f"{self.name}-{next(self._event_ids)}",
            call_id,
            state,
            due,
            FailReason.NO_ANSWER if state == CallState.FAILED else None,
        )
        if self.rng.random() < self.loss_rate:
            return
        delivery = end + 1 if self.rng.random() < self.reorder_rate else due
        self.scheduler.schedule(delivery, partial(self._deliver, event))
        if self.rng.random() < self.duplicate_rate:
            self.scheduler.schedule(delivery + 1, partial(self._deliver, event))

    def _deliver(self, event):
        self._sink(event)

    def hangup(self, call_id: str) -> None:
        if self.get_status(call_id) not in (
            ProviderStatus.UNKNOWN,
            ProviderStatus.ENDED,
            ProviderStatus.FAILED,
        ):
            self._status[call_id] = ProviderStatus.ENDED
