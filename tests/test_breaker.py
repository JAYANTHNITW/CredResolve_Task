from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

import pytest

from smartdialer.breaker import BreakerState, CircuitBreaker, Outcome
from smartdialer.clock import FakeClock
from smartdialer.models import FailReason
from smartdialer.providers.base import Accepted, ProviderTimeout, Rejected
from smartdialer.router import ProviderRouter


def test_window_and_strict_failure_threshold():
    b = CircuitBreaker(FakeClock())
    for outcome in [Outcome.SUCCESS] * 20 + [Outcome.REJECTED] * 10:
        b.record(outcome)
    assert b.state == BreakerState.CLOSED
    b.record(Outcome.REJECTED)
    assert b.state == BreakerState.OPEN
    b = CircuitBreaker(FakeClock())
    for _ in range(9):
        b.record(Outcome.REJECTED)
    assert b.allows()
    b.record(Outcome.SUCCESS)
    assert not b.allows()


def test_timeout_streak_and_single_recovery_probe():
    clock = FakeClock()
    b = CircuitBreaker(clock)
    for outcome in [Outcome.TIMEOUT] * 4 + [Outcome.SUCCESS] + [Outcome.TIMEOUT] * 4:
        b.record(outcome)
    assert b.allows()
    b.record(Outcome.TIMEOUT)
    clock.advance(9999)
    assert not b.allows()
    clock.advance(1)
    assert b.state == BreakerState.HALF_OPEN
    with ThreadPoolExecutor(8) as pool:
        assert sum(pool.map(lambda _: b.allows(), range(20))) == 1
    b.record(Outcome.REJECTED)
    assert not b.allows()
    clock.advance(10000)
    assert b.allows()
    b.record(Outcome.SUCCESS)
    assert b.state == BreakerState.CLOSED
    assert b.failure_rate == 0


def setup_router():
    a, b = Mock(), Mock()
    a.name, b.name = "A", "B"
    return a, b, ProviderRouter([a, b], FakeClock())


def test_router_health_and_open_breakers():
    a, b, router = setup_router()
    assert router.pick() is a
    router.breakers["A"].record(Outcome.REJECTED)
    assert router.pick() is b
    for name in ("A", "B"):
        for _ in range(5):
            router.breakers[name].record(Outcome.TIMEOUT)
        assert router.pick() is (b if name == "A" else None)


def test_definite_rejection_fails_over_and_remembers_acceptance():
    a, b, router = setup_router()
    a.place_call.return_value = Rejected(FailReason.PROVIDER_ERROR)
    b.place_call.return_value = Accepted("ref")
    for _ in range(2):
        assert router.place_call("c", "123", "key") == Accepted("ref")
    a.place_call.assert_called_once_with("c", "123", "key")
    assert b.place_call.call_count == 2


def test_timeout_stays_on_same_provider_and_key_across_invocations():
    a, b, router = setup_router()
    a.place_call.side_effect = [ProviderTimeout()] * 3 + [
        Rejected(FailReason.PROVIDER_ERROR),
        Accepted("ref"),
    ]
    for _ in range(2):
        with pytest.raises(ProviderTimeout):
            router.place_call("c", "123", "key")
    assert router.place_call("c", "123", "key") == Accepted("ref")
    assert [call.args for call in a.place_call.call_args_list] == [
        ("c", "123", "key")
    ] * 5
    b.place_call.assert_not_called()
