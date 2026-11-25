import sqlite3
from contextlib import closing

import pytest

from smartdialer import db
from smartdialer.clock import FakeClock, SystemClock


@pytest.fixture
def conn(tmp_path):
    with closing(db.connect(tmp_path / "dialer.db")) as connection:
        db.init_schema(connection)
        yield connection


def test_connection_and_schema(conn):
    assert conn.isolation_level is None
    assert conn.row_factory is sqlite3.Row
    for pragma, value in {
        "journal_mode": "wal",
        "busy_timeout": 5000,
        "foreign_keys": 1,
        "synchronous": 1,
    }.items():
        assert conn.execute(f"PRAGMA {pragma}").fetchone()[0] == value
    db.init_schema(conn)
    tables = {
        row["name"]
        for row in conn.execute("SELECT name FROM sqlite_schema WHERE type='table'")
    }
    assert {
        "agents",
        "borrowers",
        "calls",
        "call_events",
        "safety_decisions",
        "incidents",
    } <= tables


@pytest.mark.parametrize("terminal", ["COMPLETED", "FAILED", "CANCELLED"])
@pytest.mark.parametrize(
    "agent,borrower,column", [("a1", "b2", "agent_id"), ("a2", "b1", "borrower_id")]
)
def test_only_one_active_call(conn, agent, borrower, column, terminal):
    insert = """INSERT INTO calls
        (id, campaign_id, agent_id, borrower_id, state, idempotency_key, lease_expires_at, created_at)
        VALUES (?, 'campaign', ?, ?, 'RESERVED', ?, 100, 0)"""
    conn.execute(insert, ("c1", "a1", "b1", "key1"))
    with pytest.raises(sqlite3.IntegrityError, match=column):
        conn.execute(insert, ("c2", agent, borrower, "key2"))
    assert (
        conn.execute(
            "UPDATE calls SET state=? WHERE id='c1' AND state='RESERVED'", (terminal,)
        ).rowcount
        == 1
    )
    conn.execute(insert, ("c2", agent, borrower, "key2"))


def test_provider_event_uniqueness(conn):
    insert = """INSERT INTO call_events
        (call_id, provider, provider_event_id, type, received_at, outcome)
        VALUES ('call', ?, ?, 'RINGING', 1, 'APPLIED')"""
    conn.execute(insert, ("A", "event1"))
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(insert, ("A", "event1"))
    conn.executemany(insert, [("B", "event1"), ("A", None), ("A", None)])


def test_commit_and_rollback(conn):
    insert = "INSERT INTO incidents(kind) VALUES (?)"
    with db.tx(conn):
        conn.execute(insert, ("committed",))
    with pytest.raises(RuntimeError), db.tx(conn):
        conn.execute(insert, ("rolled back",))
        raise RuntimeError
    assert not conn.in_transaction
    assert [row["kind"] for row in conn.execute("SELECT kind FROM incidents")] == [
        "committed"
    ]


@pytest.mark.parametrize("release", [True, False])
def test_busy_writer_retry(tmp_path, conn, monkeypatch, release):
    sleeps = []

    def pause(delay):
        sleeps.append(delay)
        if release:
            conn.rollback()

    monkeypatch.setattr(db.time, "sleep", pause)
    with closing(db.connect(tmp_path / "dialer.db")) as contender:
        contender.execute("PRAGMA busy_timeout=0")
        conn.execute("BEGIN IMMEDIATE")
        if release:
            with db.tx(contender):
                assert contender.in_transaction
            assert len(sleeps) == 1
        else:
            with pytest.raises(sqlite3.OperationalError) as error, db.tx(contender):
                pytest.fail("writer should still be locked")
            assert error.value.sqlite_errorcode == sqlite3.SQLITE_BUSY
            assert len(sleeps) == 2
        assert contender.busy_retries == len(sleeps)
        assert not contender.in_transaction


def test_clocks():
    clock = FakeClock(100)
    clock.advance(25)
    assert clock.now_ms() == 125
    with pytest.raises(ValueError):
        clock.advance(-1)
    assert clock.now_ms() == 125
    assert isinstance(SystemClock().now_ms(), int)
