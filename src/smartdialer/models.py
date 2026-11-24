"""Pure lifecycle rules for agents, calls, and borrowers."""

from dataclasses import dataclass
from enum import StrEnum


class AgentState(StrEnum):
    OFFLINE = "OFFLINE"
    AVAILABLE = "AVAILABLE"
    RESERVED = "RESERVED"
    DIALING = "DIALING"
    CONNECTED = "CONNECTED"
    WRAP_UP = "WRAP_UP"
    PAUSED = "PAUSED"


class CallState(StrEnum):
    QUEUED = "QUEUED"
    RESERVED = "RESERVED"
    INITIATED = "INITIATED"
    RINGING = "RINGING"
    ANSWERED = "ANSWERED"
    CONNECTED = "CONNECTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class BorrowerStatus(StrEnum):
    PENDING = "PENDING"
    RESERVED = "RESERVED"
    IN_CALL = "IN_CALL"
    DONE = "DONE"
    EXHAUSTED = "EXHAUSTED"


class FailReason(StrEnum):
    NO_ANSWER = "NO_ANSWER"
    BUSY = "BUSY"
    PROVIDER_ERROR = "PROVIDER_ERROR"
    AGENT_LOST = "AGENT_LOST"
    RESERVATION_EXPIRED = "RESERVATION_EXPIRED"


AGENT_TRANSITIONS: dict[AgentState, frozenset[AgentState]] = {
    AgentState.OFFLINE: frozenset({AgentState.AVAILABLE}),
    AgentState.AVAILABLE: frozenset(
        {AgentState.RESERVED, AgentState.PAUSED, AgentState.OFFLINE}
    ),
    AgentState.RESERVED: frozenset({AgentState.DIALING, AgentState.AVAILABLE}),
    AgentState.DIALING: frozenset({AgentState.CONNECTED, AgentState.AVAILABLE}),
    AgentState.CONNECTED: frozenset({AgentState.WRAP_UP, AgentState.AVAILABLE}),
    AgentState.WRAP_UP: frozenset(
        {AgentState.AVAILABLE, AgentState.PAUSED, AgentState.OFFLINE}
    ),
    AgentState.PAUSED: frozenset({AgentState.AVAILABLE, AgentState.OFFLINE}),
}

FORWARD = [
    CallState.QUEUED,
    CallState.RESERVED,
    CallState.INITIATED,
    CallState.RINGING,
    CallState.ANSWERED,
    CallState.CONNECTED,
    CallState.COMPLETED,
]
RANK = {state: rank for rank, state in enumerate(FORWARD)}
TERMINAL = frozenset({CallState.COMPLETED, CallState.FAILED, CallState.CANCELLED})


def plan_call_transition(current: CallState, target: CallState) -> list[CallState]:
    """Return each implied forward step, or an empty list for an ignored event."""
    if current in TERMINAL:
        return []
    if target in (CallState.FAILED, CallState.CANCELLED):
        return [target]
    return FORWARD[RANK[current] + 1 : RANK[target] + 1]


AGENT_STATE_FOR_CALL = {
    CallState.INITIATED: AgentState.DIALING,
    CallState.ANSWERED: AgentState.CONNECTED,
    CallState.COMPLETED: AgentState.WRAP_UP,
    CallState.FAILED: AgentState.AVAILABLE,
    CallState.CANCELLED: AgentState.AVAILABLE,
}


def agent_state_for_call(
    state: CallState, pending_state: AgentState | None = None
) -> AgentState | None:
    """Resolve an applied call step. Honor deferred presence requests on release."""
    if pending_state not in (None, AgentState.PAUSED, AgentState.OFFLINE):
        raise ValueError("pending_state must be PAUSED or OFFLINE")
    if state in TERMINAL and pending_state is not None:
        return pending_state
    return AGENT_STATE_FOR_CALL.get(state)


class RetryPolicy(StrEnum):
    HOURS = "HOURS"
    SECONDS_WITH_JITTER = "SECONDS_WITH_JITTER"


@dataclass(frozen=True)
class BorrowerEffect:
    status: BorrowerStatus
    attempts_delta: int = 0
    retry: RetryPolicy | None = None


_SHORT_RETRY = BorrowerEffect(
    BorrowerStatus.PENDING, retry=RetryPolicy.SECONDS_WITH_JITTER
)
_COUNTED_RETRY = BorrowerEffect(
    BorrowerStatus.PENDING, attempts_delta=1, retry=RetryPolicy.HOURS
)

BORROWER_EFFECT_FOR_CALL = {
    CallState.ANSWERED: BorrowerEffect(BorrowerStatus.IN_CALL),
    CallState.COMPLETED: BorrowerEffect(BorrowerStatus.DONE),
    CallState.CANCELLED: _SHORT_RETRY,
}

_FAILURE_EFFECT = {
    FailReason.NO_ANSWER: _COUNTED_RETRY,
    FailReason.BUSY: _COUNTED_RETRY,
    FailReason.PROVIDER_ERROR: _SHORT_RETRY,
    FailReason.AGENT_LOST: _SHORT_RETRY,
    FailReason.RESERVATION_EXPIRED: _SHORT_RETRY,
}


def borrower_effect_for_call(
    state: CallState,
    *,
    attempts: int,
    max_attempts: int,
    reason: FailReason | None = None,
) -> BorrowerEffect | None:
    """Return the change to apply to the borrower, using the pre-event attempt count."""
    if state != CallState.FAILED:
        return BORROWER_EFFECT_FOR_CALL.get(state)
    if reason is None:
        raise ValueError("FAILED requires a failure reason")
    effect = _FAILURE_EFFECT[reason]
    if effect.attempts_delta and attempts + effect.attempts_delta >= max_attempts:
        return BorrowerEffect(
            BorrowerStatus.EXHAUSTED, attempts_delta=effect.attempts_delta
        )
    return effect
