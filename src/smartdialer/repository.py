"""Atomic reservations and campaign queries on a worker-owned connection."""

from collections import Counter
from dataclasses import dataclass
from sqlite3 import Connection, Row
from uuid import uuid4

from smartdialer.db import tx
from smartdialer.models import AgentState, BorrowerStatus, CallState, FailReason
from smartdialer.providers.base import ProviderEvent


@dataclass(frozen=True)
class Reservation:
    call_id: str
    agent_id: str
    borrower_id: str
    phone: str


@dataclass
class CampaignSnapshot:
    agents_by_state: Counter[AgentState]
    calls_by_state: Counter[CallState]

    @property
    def available_agents(self) -> int:
        return self.agents_by_state[AgentState.AVAILABLE]


def try_reserve_agent(
    conn: Connection, agent_id: str, call_id: str, now_ms: int
) -> bool:
    result = conn.execute(
        """UPDATE agents SET state='RESERVED', current_call_id=?, version=version+1,
               available_since=NULL
           WHERE id=? AND state='AVAILABLE' AND current_call_id IS NULL""",
        (call_id, agent_id),
    )
    return result.rowcount == 1


def reserve_batch(
    conn: Connection,
    campaign_id: str,
    n: int,
    now_ms: int,
    lease_ms: int,
    heartbeat_ttl_ms: int,
) -> list[Reservation]:
    if n <= 0:
        return []
    if lease_ms <= 0 or heartbeat_ttl_ms <= 0:
        raise ValueError("lease and heartbeat TTL must be positive")
    reservations = []
    with tx(conn):
        agents = conn.execute(
            """SELECT id FROM agents WHERE campaign_id=? AND state='AVAILABLE'
                   AND current_call_id IS NULL AND last_heartbeat_at>?
               ORDER BY available_since, id LIMIT ?""",
            (campaign_id, now_ms - heartbeat_ttl_ms, n),
        ).fetchall()
        borrowers = conn.execute(
            """SELECT id, phone FROM borrowers WHERE campaign_id=? AND status='PENDING'
                   AND active_call_id IS NULL AND next_attempt_at<=? AND attempts<max_attempts
               ORDER BY priority DESC, next_attempt_at, id LIMIT ?""",
            (campaign_id, now_ms, len(agents)),
        ).fetchall()
        for agent, borrower in zip(agents, borrowers):
            call_id = uuid4().hex
            conn.execute("SAVEPOINT pair")
            if try_reserve_agent(conn, agent["id"], call_id, now_ms):
                claim = conn.execute(
                    """UPDATE borrowers SET status='RESERVED', active_call_id=?
                       WHERE id=? AND status='PENDING' AND active_call_id IS NULL""",
                    (call_id, borrower["id"]),
                )
                if claim.rowcount == 1:
                    call = conn.execute(
                        """INSERT INTO calls (id, campaign_id, agent_id, borrower_id, state,
                               idempotency_key, created_at, lease_expires_at)
                           VALUES (?, ?, ?, ?, 'RESERVED', ?, ?, ?) ON CONFLICT DO NOTHING""",
                        (
                            call_id,
                            campaign_id,
                            agent["id"],
                            borrower["id"],
                            call_id,
                            now_ms,
                            now_ms + lease_ms,
                        ),
                    )
                    if call.rowcount == 1:
                        reservations.append(
                            Reservation(
                                call_id, agent["id"], borrower["id"], borrower["phone"]
                            )
                        )
                        conn.execute("RELEASE pair")
                        continue
            conn.execute("ROLLBACK TO pair")
            conn.execute("RELEASE pair")
    return reservations


def release_agent(conn: Connection, agent_id: str, call_id: str, now_ms: int) -> bool:
    """Release the matching assignment inside the caller's settlement transaction."""
    result = conn.execute(
        """UPDATE agents SET state=COALESCE(pending_state, 'AVAILABLE'),
               available_since=CASE WHEN pending_state IS NULL THEN ? END,
               pending_state=NULL, current_call_id=NULL, wrap_up_until=NULL, version=version+1
           WHERE id=? AND current_call_id=? AND state IN ('RESERVED','DIALING','CONNECTED')""",
        (now_ms, agent_id, call_id),
    )
    return result.rowcount == 1


def requeue_borrower(
    conn: Connection,
    borrower_id: str,
    call_id: str,
    next_attempt_at: int,
    *,
    consume_attempt: bool = False,
) -> bool:
    """Settle a matching borrower assignment with the caller's retry deadline."""
    result = conn.execute(
        """UPDATE borrowers SET status=CASE WHEN :counted AND attempts+1>=max_attempts
               THEN 'EXHAUSTED' ELSE 'PENDING' END, attempts=attempts+:counted,
               active_call_id=NULL, next_attempt_at=:retry
           WHERE id=:borrower AND active_call_id=:call AND status IN ('RESERVED','IN_CALL')""",
        {
            "counted": consume_attempt,
            "retry": next_attempt_at,
            "borrower": borrower_id,
            "call": call_id,
        },
    )
    return result.rowcount == 1


def get_call(conn: Connection, call_id: str) -> Row | None:
    return conn.execute("SELECT * FROM calls WHERE id=?", (call_id,)).fetchone()


def campaign_snapshot(conn: Connection, campaign_id: str) -> CampaignSnapshot:
    # One SELECT gives both aggregates the same SQLite read snapshot.
    rows = conn.execute(
        """SELECT 'agent' AS kind, state, COUNT(*) AS total FROM agents
           WHERE campaign_id=? GROUP BY state
           UNION ALL SELECT 'call', state, COUNT(*) FROM calls
           WHERE campaign_id=? GROUP BY state""",
        (campaign_id, campaign_id),
    )
    snapshot = CampaignSnapshot(Counter(), Counter())
    for kind, state, count in rows:
        if kind == "agent":
            snapshot.agents_by_state[AgentState(state)] = count
        else:
            snapshot.calls_by_state[CallState(state)] = count
    return snapshot


def record_event(conn: Connection, event: ProviderEvent, now_ms: int) -> int | None:
    cur = conn.execute(
        """INSERT INTO call_events
           (call_id, provider, provider_event_id, type, occurred_at, received_at, outcome)
           VALUES (?, ?, ?, ?, ?, ?, 'PROCESSING')
           ON CONFLICT(provider, provider_event_id) DO NOTHING""",
        (
            event.call_id,
            event.provider,
            event.provider_event_id,
            event.type,
            event.occurred_at,
            now_ms,
        ),
    )
    return cur.lastrowid if cur.rowcount == 1 else None


def mark_event(conn: Connection, event_id: int, outcome: str) -> None:
    if (
        conn.execute(
            "UPDATE call_events SET outcome=? WHERE id=? AND outcome='PROCESSING'",
            (outcome, event_id),
        ).rowcount
        != 1
    ):
        raise RuntimeError("event audit row changed during processing")


_TIMESTAMP_COLUMN = {
    CallState.INITIATED: "initiated_at",
    CallState.RINGING: "ringing_at",
    CallState.ANSWERED: "answered_at",
    CallState.COMPLETED: "ended_at",
    CallState.FAILED: "ended_at",
    CallState.CANCELLED: "ended_at",
}


def cas_call(
    conn: Connection,
    call_id: str,
    current: CallState,
    version: int,
    target: CallState,
    now_ms: int,
    reason: FailReason | None = None,
) -> bool:
    column = _TIMESTAMP_COLUMN.get(target)
    timestamp = f", {column}=:now" if column else ""
    # An allocator's RESERVED -> INITIATED write can lose to an early RINGING
    # event. An already-advanced call means placement succeeded.
    result = conn.execute(
        f"""UPDATE calls SET state=:target, version=version+1, fail_reason=:reason{timestamp}
            WHERE id=:id AND state=:current AND version=:version""",
        {
            "target": target,
            "reason": reason,
            "now": now_ms,
            "id": call_id,
            "current": current,
            "version": version,
        },
    )
    return result.rowcount == 1


def advance_assigned_agent(
    conn: Connection, call: Row, current: AgentState, target: AgentState
) -> bool:
    return (
        conn.execute(
            """UPDATE agents SET state=?, version=version+1
           WHERE id=? AND state=? AND current_call_id=?""",
            (target, call["agent_id"], current, call["id"]),
        ).rowcount
        == 1
    )


def finish_agent_call(conn: Connection, call: Row, wrap_up_until: int) -> bool:
    return (
        conn.execute(
            """UPDATE agents SET state=COALESCE(pending_state,'WRAP_UP'),
               wrap_up_until=CASE WHEN pending_state IS NULL THEN ? END,
               pending_state=NULL, current_call_id=NULL, available_since=NULL, version=version+1
           WHERE id=? AND state='CONNECTED' AND current_call_id=?""",
            (wrap_up_until, call["agent_id"], call["id"]),
        ).rowcount
        == 1
    )


def assigned_borrower(conn: Connection, call: Row) -> Row:
    row = conn.execute(
        "SELECT attempts,max_attempts FROM borrowers WHERE id=? AND active_call_id=?",
        (call["borrower_id"], call["id"]),
    ).fetchone()
    if row is None:
        raise RuntimeError("borrower assignment changed during call")
    return row


def advance_assigned_borrower(
    conn: Connection, call: Row, current: BorrowerStatus, target: BorrowerStatus
) -> bool:
    return (
        conn.execute(
            """UPDATE borrowers SET status=?, active_call_id=CASE WHEN ?='DONE' THEN NULL ELSE active_call_id END
           WHERE id=? AND status=? AND active_call_id=?""",
            (target, target, call["borrower_id"], current, call["id"]),
        ).rowcount
        == 1
    )
