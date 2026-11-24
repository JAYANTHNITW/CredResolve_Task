from collections import Counter, defaultdict
from random import Random

import pytest

from smartdialer.clock import FakeClock
from smartdialer.models import CallState, FailReason
from smartdialer.providers.base import (
    Accepted,
    ProviderStatus,
    ProviderTimeout,
    Rejected,
    Scheduler,
)
from smartdialer.providers.mock_a import MockProviderA
from smartdialer.providers.mock_b import MockProviderB


def make(provider_type, seed=4, answer=1):
    clock = FakeClock()
    scheduler = Scheduler()
    provider = provider_type(
        clock,
        Random(seed),
        scheduler,
        answer_rate_fn=lambda now: answer,
        ring_time_dist=lambda rng: 10,
        talk_time_dist=lambda rng: 20,
    )
    events = []
    provider.set_event_sink(events.append)
    return clock, scheduler, provider, events


@pytest.mark.parametrize("provider_type", [MockProviderA, MockProviderB])
def test_idempotency_and_chaos_controls(provider_type):
    _, scheduler, p, events = make(provider_type)
    p.set_failure_rate(0)
    p.set_extra_latency(200)
    accepted = p.place_call("c", "123", "key")
    p.set_outage(True)
    assert p.place_call("c", "123", "key") == accepted == Accepted(f"{p.name}-c")
    assert isinstance(p.place_call("d", "123", "other"), Rejected)
    scheduler.run_until(p.latency_ms[0] + 199)
    assert not events
    assert p.get_status("c") == ProviderStatus.INITIATED
    scheduler.run_until(3000)
    assert p.get_status("c") == ProviderStatus.ENDED
    assert p.call_count == 1


@pytest.mark.parametrize("answer", [0, 1])
def test_clean_provider_lifecycle(answer):
    clock, scheduler, p, events = make(MockProviderA, answer=answer)
    p.set_failure_rate(0)
    assert isinstance(p.place_call("c", "123", "key"), Accepted)
    states = []
    for _ in range(201):
        scheduler.run_until(clock.now_ms())
        state = p.get_status("c")
        if not states or states[-1] != state:
            states.append(state)
        clock.advance(1)
    assert 50 <= events[0].occurred_at <= 150
    if answer:
        assert [e.type for e in events] == [
            CallState.RINGING,
            CallState.ANSWERED,
            CallState.COMPLETED,
        ]
        assert states == [
            ProviderStatus.INITIATED,
            ProviderStatus.RINGING,
            ProviderStatus.ANSWERED,
            ProviderStatus.ENDED,
        ]
    else:
        assert [e.type for e in events] == [CallState.RINGING, CallState.FAILED]
        assert events[-1].fail_reason == FailReason.NO_ANSWER
        assert states[-1] == ProviderStatus.FAILED


def test_hangup_suppresses_future_transitions():
    _, scheduler, p, events = make(MockProviderA)
    p.set_failure_rate(0)
    p.place_call("c", "123", "key")
    p.hangup("c")
    p.hangup("c")
    p.hangup("missing")
    scheduler.run_until(10000)
    assert events == []
    assert p.get_status("c") == ProviderStatus.ENDED
    assert p.get_status("missing") == ProviderStatus.UNKNOWN


def noisy_run():
    _, scheduler, p, events = make(MockProviderB)
    timeouts = []
    for i in range(300):
        key = str(i)
        try:
            p.place_call(key, "123", key)
        except ProviderTimeout:
            timeouts.append(p.get_status(key))
    scheduler.run_until(10000)
    return events, timeouts, p.call_count


def test_seeded_chaos_is_reproducible_and_real():
    events, timeouts, count = noisy_run()
    assert (events, timeouts, count) == noisy_run()
    assert set(timeouts) == {ProviderStatus.UNKNOWN, ProviderStatus.INITIATED}
    ids = Counter(e.provider_event_id for e in events)
    assert max(ids.values()) == 2
    assert len(ids) < count * 3
    times = defaultdict(list)
    for event in events:
        times[event.call_id].append(event.occurred_at)
    assert any(seq != sorted(seq) for seq in times.values())
