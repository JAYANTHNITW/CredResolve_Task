from contextlib import closing
from itertools import permutations

import pytest

from smartdialer import repository as repo
from smartdialer.db import connect, init_schema
from smartdialer.event_handler import Outcome, handle_event
from smartdialer.models import CallState as C
from smartdialer.models import FailReason
from smartdialer.providers.base import ProviderEvent


@pytest.fixture
def reserved():
    with closing(connect(":memory:")) as conn:
        init_schema(conn)
        conn.execute("""INSERT INTO agents(id,campaign_id,state,last_heartbeat_at)
                     VALUES ('a','campaign','AVAILABLE',100)""")
        conn.execute("""INSERT INTO borrowers(id,campaign_id,phone,status)
                     VALUES ('b','campaign','123','PENDING')""")
        (call,) = repo.reserve_batch(conn, "campaign", 1, 100, 500, 50)
        yield conn, call.call_id


def notify(conn, call, target, event_id=None, *, reason=None, now=250):
    return handle_event(
        conn, ProviderEvent("A", event_id, call, target, 150, reason), now
    )


def row(conn, table):
    return conn.execute(f"SELECT * FROM {table}").fetchone()


@pytest.mark.parametrize(
    "event_id,repeat", [("e", Outcome.DUPLICATE), (None, Outcome.STALE)]
)
def test_five_deliveries_one_agent_transition(reserved, event_id, repeat):
    conn, call = reserved
    assert [notify(conn, call, C.INITIATED, event_id) for _ in range(5)] == [
        Outcome.APPLIED
    ] + [repeat] * 4
    assert row(conn, "agents")["version"] == 2  # one reservation and one initiation
    audits = conn.execute(
        "SELECT outcome,occurred_at,received_at FROM call_events ORDER BY id"
    ).fetchall()
    assert [r["outcome"] for r in audits] == ["APPLIED"] + (
        [] if event_id else ["STALE"] * 4
    )
    assert all((r["occurred_at"], r["received_at"]) == (150, 250) for r in audits)


@pytest.mark.parametrize(
    "order", sorted(set(permutations((C.RINGING, C.ANSWERED, C.COMPLETED) * 2)))
)
def test_permutations_converge(reserved, order):
    conn, call = reserved
    for state in order:
        notify(conn, call, state, state.value)
    saved = repo.get_call(conn, call)
    assert (saved["state"], saved["version"]) == ("COMPLETED", 5)
    assert all(
        saved[col] == 250
        for col in ("initiated_at", "ringing_at", "answered_at", "ended_at")
    )
    agent, borrower = row(conn, "agents"), row(conn, "borrowers")
    assert (
        agent["state"],
        agent["version"],
        agent["current_call_id"],
        agent["wrap_up_until"],
    ) == ("WRAP_UP", 4, None, 30250)
    assert (borrower["status"], borrower["active_call_id"], borrower["attempts"]) == (
        "DONE",
        None,
        0,
    )


def test_completed_first_and_place_race(reserved):
    conn, call = reserved
    assert notify(conn, call, C.RINGING) == Outcome.APPLIED
    assert not repo.cas_call(conn, call, C.RESERVED, 0, C.INITIATED, 260)
    assert row(conn, "agents")["state"] == "DIALING"
    assert notify(conn, call, C.COMPLETED) == Outcome.APPLIED
    for target in (C.ANSWERED, C.RINGING, C.FAILED):
        assert notify(conn, call, target) == Outcome.STALE


@pytest.mark.parametrize(
    "state,reason,attempts,status,new_attempts",
    [
        (C.FAILED, FailReason.NO_ANSWER, 0, "PENDING", 1),
        (C.FAILED, FailReason.BUSY, 2, "EXHAUSTED", 3),
        (C.FAILED, FailReason.PROVIDER_ERROR, 1, "PENDING", 1),
        (C.FAILED, FailReason.AGENT_LOST, 1, "PENDING", 1),
        (C.CANCELLED, None, 1, "PENDING", 1),
    ],
)
def test_settlement(reserved, state, reason, attempts, status, new_attempts):
    conn, call = reserved
    conn.execute("UPDATE borrowers SET attempts=?", (attempts,))
    notify(conn, call, C.ANSWERED if reason == FailReason.AGENT_LOST else C.RINGING)
    assert notify(conn, call, state, reason=reason) == Outcome.APPLIED
    agent, borrower = row(conn, "agents"), row(conn, "borrowers")
    assert (agent["state"], agent["current_call_id"], agent["available_since"]) == (
        "AVAILABLE",
        None,
        250,
    )
    assert (borrower["status"], borrower["active_call_id"], borrower["attempts"]) == (
        status,
        None,
        new_attempts,
    )
    assert repo.get_call(conn, call)["fail_reason"] == reason
    retry = borrower["next_attempt_at"]
    if status == "EXHAUSTED":
        assert retry == 0
    elif new_attempts > attempts:
        assert retry == 250 + 7200000
    else:
        assert 2250 <= retry <= 5250


@pytest.mark.parametrize(
    "state,pending", [(C.COMPLETED, "PAUSED"), (C.CANCELLED, "OFFLINE")]
)
def test_deferred_presence(reserved, state, pending):
    conn, call = reserved
    notify(conn, call, C.ANSWERED)
    conn.execute(
        "UPDATE agents SET pending_state=? WHERE current_call_id=?", (pending, call)
    )
    notify(conn, call, state)
    agent = row(conn, "agents")
    assert (
        agent["state"],
        agent["current_call_id"],
        agent["pending_state"],
        agent["wrap_up_until"],
    ) == (pending, None, None, None)


def test_crash_and_redelivery(reserved, monkeypatch):
    conn, call = reserved

    def crash(*args):
        raise RuntimeError("worker crashed")

    with monkeypatch.context() as patch:
        patch.setattr(repo, "advance_assigned_borrower", crash)
        with pytest.raises(RuntimeError, match="worker crashed"):
            notify(conn, call, C.ANSWERED, "same")
    assert row(conn, "call_events") is None
    assert repo.get_call(conn, call)["state"] == "RESERVED"
    assert row(conn, "agents")["version"] == 1
    assert row(conn, "borrowers")["status"] == "RESERVED"
    assert notify(conn, call, C.ANSWERED, "same") == Outcome.APPLIED
    assert row(conn, "borrowers")["status"] == "IN_CALL"


def test_old_event_cannot_release_new_call(reserved):
    conn, old = reserved
    notify(conn, old, C.CANCELLED)
    (new,) = repo.reserve_batch(conn, "campaign", 1, 6000, 500, 10000)
    assert notify(conn, old, C.FAILED, reason=FailReason.NO_ANSWER) == Outcome.STALE
    assert row(conn, "agents")["current_call_id"] == new.call_id
    assert row(conn, "borrowers")["active_call_id"] == new.call_id


def test_unknown_call_is_audited(reserved):
    conn, _ = reserved
    assert notify(conn, "unknown", C.RINGING, "id") == Outcome.UNKNOWN_CALL
    assert row(conn, "call_events")["outcome"] == "UNKNOWN_CALL"
    assert notify(conn, "unknown", C.RINGING, "id") == Outcome.DUPLICATE
