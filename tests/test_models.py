from itertools import permutations, product

import pytest

from smartdialer.models import (
    AGENT_TRANSITIONS,
    BorrowerEffect,
    RetryPolicy,
    agent_state_for_call,
    borrower_effect_for_call,
    plan_call_transition,
)
from smartdialer.models import (
    AgentState as A,
)
from smartdialer.models import (
    BorrowerStatus as B,
)
from smartdialer.models import (
    CallState as C,
)
from smartdialer.models import (
    FailReason as F,
)


@pytest.mark.parametrize("current,target", list(product(A, repeat=2)))
def test_agent_transition_matrix(current, target):
    # Group by destination to check the incoming edges independently of the model.
    sources = {
        A.OFFLINE: {A.AVAILABLE, A.PAUSED, A.WRAP_UP},
        A.AVAILABLE: {
            A.OFFLINE,
            A.RESERVED,
            A.DIALING,
            A.CONNECTED,
            A.WRAP_UP,
            A.PAUSED,
        },
        A.RESERVED: {A.AVAILABLE},
        A.DIALING: {A.RESERVED},
        A.CONNECTED: {A.DIALING},
        A.WRAP_UP: {A.CONNECTED},
        A.PAUSED: {A.AVAILABLE, A.WRAP_UP},
    }
    assert (target in AGENT_TRANSITIONS[current]) == (current in sources[target])


@pytest.mark.parametrize("current,target", list(product(C, repeat=2)))
def test_call_transition_matrix(current, target):
    path = [
        C.QUEUED,
        C.RESERVED,
        C.INITIATED,
        C.RINGING,
        C.ANSWERED,
        C.CONNECTED,
        C.COMPLETED,
    ]
    if current in (C.COMPLETED, C.FAILED, C.CANCELLED):
        expected = []
    elif target in (C.FAILED, C.CANCELLED):
        expected = [target]
    else:
        expected = [
            s for s in path if path.index(current) < path.index(s) <= path.index(target)
        ]
    assert plan_call_transition(current, target) == expected


def fold_events(events):
    visited = [C.INITIATED]
    applied = 0
    for event in events:
        steps = plan_call_transition(visited[-1], event)
        if steps:
            visited.extend(steps)
            applied += 1
    return visited, applied


def test_reordered_and_duplicated_events():
    events = (C.RINGING, C.ANSWERED, C.COMPLETED) * 2
    for ordering in set(permutations(events)):
        visited, _ = fold_events(ordering)
        assert visited == [C.INITIATED, C.RINGING, C.ANSWERED, C.CONNECTED, C.COMPLETED]


@pytest.mark.parametrize(
    ("events", "applied"),
    [
        ([C.ANSWERED, C.ANSWERED, C.ANSWERED, C.COMPLETED], 2),
        ([C.COMPLETED, C.ANSWERED, C.RINGING], 1),
    ],
)
def test_applied_event_count(events, applied):
    assert fold_events(events)[1] == applied


@pytest.mark.parametrize("pending", [None, A.PAUSED, A.OFFLINE])
def test_agent_effects_and_deferred_release(pending):
    expected = {
        C.QUEUED: None,
        C.RESERVED: None,
        C.INITIATED: A.DIALING,
        C.RINGING: None,
        C.ANSWERED: A.CONNECTED,
        C.CONNECTED: None,
        C.COMPLETED: pending or A.WRAP_UP,
        C.FAILED: pending or A.AVAILABLE,
        C.CANCELLED: pending or A.AVAILABLE,
    }
    assert {state: agent_state_for_call(state, pending) for state in C} == expected


@pytest.mark.parametrize("reason", [F.NO_ANSWER, F.BUSY])
def test_counted_failure_exhausts_only_on_last_attempt(reason):
    assert borrower_effect_for_call(
        C.FAILED, attempts=1, max_attempts=3, reason=reason
    ) == (BorrowerEffect(B.PENDING, 1, RetryPolicy.HOURS))
    assert borrower_effect_for_call(
        C.FAILED, attempts=2, max_attempts=3, reason=reason
    ) == (BorrowerEffect(B.EXHAUSTED, 1))


@pytest.mark.parametrize(
    ("state", "reason"),
    [
        (C.FAILED, F.PROVIDER_ERROR),
        (C.FAILED, F.AGENT_LOST),
        (C.FAILED, F.RESERVATION_EXPIRED),
        (C.CANCELLED, None),
    ],
)
def test_short_retry_preserves_attempts(state, reason):
    assert borrower_effect_for_call(
        state, attempts=2, max_attempts=3, reason=reason
    ) == (BorrowerEffect(B.PENDING, 0, RetryPolicy.SECONDS_WITH_JITTER))


@pytest.mark.parametrize(
    ("state", "effect"),
    [
        (C.QUEUED, None),
        (C.RESERVED, None),
        (C.INITIATED, None),
        (C.RINGING, None),
        (C.ANSWERED, BorrowerEffect(B.IN_CALL)),
        (C.CONNECTED, None),
        (C.COMPLETED, BorrowerEffect(B.DONE)),
    ],
)
def test_non_failure_borrower_effects(state, effect):
    assert borrower_effect_for_call(state, attempts=0, max_attempts=3) == effect


def test_failure_requires_reason():
    with pytest.raises(ValueError, match="reason"):
        borrower_effect_for_call(C.FAILED, attempts=0, max_attempts=3)
