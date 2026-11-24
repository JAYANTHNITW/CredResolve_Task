"""Apply a provider notification and its audit record in one transaction."""

from enum import StrEnum
from sqlite3 import Connection, Row
from zlib import crc32

from smartdialer import repository as repo
from smartdialer.db import tx
from smartdialer.models import (
    AgentState,
    BorrowerStatus,
    CallState,
    RetryPolicy,
    agent_state_for_call,
    borrower_effect_for_call,
    plan_call_transition,
)
from smartdialer.providers.base import ProviderEvent


class Outcome(StrEnum):
    APPLIED = "APPLIED"
    DUPLICATE = "DUPLICATE"
    STALE = "STALE"
    UNKNOWN_CALL = "UNKNOWN_CALL"


def _apply_agent(
    conn: Connection, call: Row, step: CallState, now_ms: int, wrap_up_ms: int
) -> None:
    target = agent_state_for_call(step)
    if target is None:
        return
    if target == AgentState.AVAILABLE:
        changed = repo.release_agent(conn, call["agent_id"], call["id"], now_ms)
    elif target == AgentState.WRAP_UP:
        changed = repo.finish_agent_call(conn, call, now_ms + wrap_up_ms)
    else:
        expected = (
            AgentState.RESERVED if target == AgentState.DIALING else AgentState.DIALING
        )
        changed = repo.advance_assigned_agent(conn, call, expected, target)
    if not changed:
        raise RuntimeError("agent assignment or state changed during call")


def _apply_borrower(
    conn: Connection, call: Row, step: CallState, event: ProviderEvent, now_ms: int
) -> None:
    if step not in (
        CallState.ANSWERED,
        CallState.COMPLETED,
        CallState.FAILED,
        CallState.CANCELLED,
    ):
        return
    borrower = repo.assigned_borrower(conn, call)
    effect = borrower_effect_for_call(
        step,
        attempts=borrower["attempts"],
        max_attempts=borrower["max_attempts"],
        reason=event.fail_reason,
    )
    if effect is None:
        return
    if step in (CallState.FAILED, CallState.CANCELLED):
        # Stable per-call jitter makes replays reproducible.
        delay = 2000 + crc32(call["id"].encode()) % 3001
        if effect.retry == RetryPolicy.HOURS:
            delay = 2 * 60 * 60 * 1000
        deadline = 0 if effect.status == BorrowerStatus.EXHAUSTED else now_ms + delay
        changed = repo.requeue_borrower(
            conn,
            call["borrower_id"],
            call["id"],
            deadline,
            consume_attempt=bool(effect.attempts_delta),
        )
    else:
        expected = (
            BorrowerStatus.RESERVED
            if step == CallState.ANSWERED
            else BorrowerStatus.IN_CALL
        )
        changed = repo.advance_assigned_borrower(conn, call, expected, effect.status)
    if not changed:
        raise RuntimeError("borrower assignment or status changed during call")


def handle_event(
    conn: Connection, ev: ProviderEvent, now_ms: int, *, wrap_up_ms: int = 30_000
) -> Outcome:
    with tx(conn):
        return apply_event(conn, ev, now_ms, wrap_up_ms=wrap_up_ms)


def apply_event(
    conn: Connection, ev: ProviderEvent, now_ms: int, *, wrap_up_ms: int = 30_000
) -> Outcome:
    """Compose the event and recovery bookkeeping in the caller's write transaction."""
    if not conn.in_transaction:
        raise RuntimeError("event processing requires an active transaction")
    event_id = repo.record_event(conn, ev, now_ms)
    if event_id is None:
        return Outcome.DUPLICATE
    call = repo.get_call(conn, ev.call_id)
    if call is None:
        outcome = Outcome.UNKNOWN_CALL
    else:
        current = CallState(call["state"])
        version = call["version"]
        steps = plan_call_transition(current, ev.target_state)
        for step in steps:
            reason = (
                ev.fail_reason
                if step in (CallState.FAILED, CallState.CANCELLED)
                else None
            )
            if not repo.cas_call(
                conn, ev.call_id, current, version, step, now_ms, reason
            ):
                raise RuntimeError("call changed during event transaction")
            _apply_agent(conn, call, step, now_ms, wrap_up_ms)
            _apply_borrower(conn, call, step, ev, now_ms)
            current, version = step, version + 1
        outcome = Outcome.APPLIED if steps else Outcome.STALE
    repo.mark_event(conn, event_id, outcome)
    return outcome
