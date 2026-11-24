import multiprocessing
import random
import threading
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from contextlib import closing

import pytest

from smartdialer.db import connect, init_schema, tx
from smartdialer.repository import (
    campaign_snapshot,
    get_call,
    release_agent,
    requeue_borrower,
    reserve_batch,
    try_reserve_agent,
)


@pytest.fixture
def database(tmp_path):
    path = tmp_path / "dialer.db"
    with closing(connect(path)) as conn:
        init_schema(conn)
        yield path, conn


def seed(conn, agents, borrowers):
    with tx(conn):
        conn.executemany(
            """INSERT INTO agents
            (id,campaign_id,state,available_since,last_heartbeat_at)
            VALUES (?,'campaign','AVAILABLE',?,1000)""",
            [(f"a{i}", i) for i in range(agents)],
        )
        conn.executemany(
            """INSERT INTO borrowers (id,campaign_id,phone,status,priority)
            VALUES (?,'campaign',?,'PENDING',?)""",
            [(f"b{i}", f"phone{i}", i) for i in range(borrowers)],
        )


def batch(conn, n=10):
    return reserve_batch(conn, "campaign", n, 1000, 500, 100)


def test_thread_race(database):
    path, conn = database
    seed(conn, 1, 0)
    barrier = threading.Barrier(20)

    def compete(i):
        with closing(connect(path)) as worker:
            barrier.wait(timeout=30)
            return try_reserve_agent(worker, "a0", str(i), 1000)

    with ThreadPoolExecutor(20) as pool:
        wins = list(pool.map(compete, range(20)))
    assert sum(wins) == 1
    assert tuple(
        conn.execute("SELECT current_call_id, version FROM agents").fetchone()
    ) == (str(wins.index(True)), 1)


def run_wave(path):
    with closing(connect(path)) as conn:
        worker_barrier.wait(timeout=30)
        total = 0
        while reservations := batch(conn):
            total += len(reservations)
        return total


def test_process_race(database):
    path, conn = database
    seed(conn, 50, 500)
    ctx = multiprocessing.get_context("spawn")

    with ProcessPoolExecutor(
        8, mp_context=ctx, initializer=set_barrier, initargs=(ctx.Barrier(8),)
    ) as pool:
        totals = list(pool.map(run_wave, [path] * 8))
    assert sum(totals) == 50
    assert tuple(
        conn.execute("""SELECT COUNT(*), COUNT(DISTINCT agent_id),
        COUNT(DISTINCT borrower_id) FROM calls WHERE state='RESERVED'""").fetchone()
    ) == (50, 50, 50)
    assert (
        conn.execute("""SELECT COUNT(*) FROM calls c JOIN agents a ON a.current_call_id=c.id
        AND a.id=c.agent_id JOIN borrowers b ON b.active_call_id=c.id AND b.id=c.borrower_id
        WHERE a.state='RESERVED' AND b.status='RESERVED'""").fetchone()[0]
        == 50
    )


def set_barrier(barrier):
    global worker_barrier
    worker_barrier = barrier


@pytest.mark.parametrize("agents,borrowers", [(3, 1), (1, 3)])
def test_capacity(database, agents, borrowers):
    _, conn = database
    seed(conn, agents, borrowers)
    assert not batch(conn, 0)
    reservations = batch(conn)
    assert len(reservations) == 1
    r = reservations[0]
    assert (r.agent_id, r.borrower_id, r.phone) == (
        "a0",
        f"b{borrowers - 1}",
        f"phone{borrowers - 1}",
    )
    call = get_call(conn, r.call_id)
    assert (call["created_at"], call["lease_expires_at"], call["idempotency_key"]) == (
        1000,
        1500,
        r.call_id,
    )
    assert not batch(conn)


def test_eligibility(database):
    _, conn = database
    seed(conn, 4, 5)
    conn.execute("UPDATE agents SET last_heartbeat_at=900 WHERE id='a0'")
    conn.execute("UPDATE agents SET campaign_id='other' WHERE id='a1'")
    conn.execute("UPDATE borrowers SET next_attempt_at=1001 WHERE id='b4'")
    conn.execute("UPDATE borrowers SET attempts=max_attempts WHERE id='b3'")
    conn.execute("UPDATE borrowers SET campaign_id='other' WHERE id='b2'")
    assert [(r.agent_id, r.borrower_id) for r in batch(conn)] == [
        ("a2", "b1"),
        ("a3", "b0"),
    ]
    snap = campaign_snapshot(conn, "campaign")
    assert snap.available_agents == 1
    assert snap.calls_by_state["RESERVED"] == 2


def test_pair_conflict_undoes_only_that_pair(database):
    _, conn = database
    seed(conn, 2, 2)
    conn.execute("""INSERT INTO calls (id,campaign_id,agent_id,borrower_id,state,
        idempotency_key,created_at,lease_expires_at) VALUES
        ('old','campaign','a0','old-b','RESERVED','old',0,20)""")
    assert [(r.agent_id, r.borrower_id) for r in batch(conn)] == [("a1", "b0")]
    assert tuple(
        conn.execute(
            "SELECT state,current_call_id,version FROM agents WHERE id='a0'"
        ).fetchone()
    ) == ("AVAILABLE", None, 0)
    assert tuple(
        conn.execute(
            "SELECT status,active_call_id FROM borrowers WHERE id='b1'"
        ).fetchone()
    ) == ("PENDING", None)


def settle(conn, r, now):
    with tx(conn):
        assert (
            conn.execute(
                "UPDATE calls SET state='CANCELLED' WHERE id=? AND state='RESERVED'",
                (r.call_id,),
            ).rowcount
            == 1
        )
        assert release_agent(conn, r.agent_id, r.call_id, now)
        assert requeue_borrower(conn, r.borrower_id, r.call_id, now)


def test_random_reserve_release(database):
    _, conn = database
    seed(conn, 5, 10)
    rng = random.Random(712)
    active = []
    for _ in range(250):
        if active and rng.random() < 0.55:
            settle(conn, active.pop(rng.randrange(len(active))), 1000)
        else:
            active.extend(batch(conn, rng.randint(1, 8)))
        snap = campaign_snapshot(conn, "campaign")
        assert snap.calls_by_state["RESERVED"] == len(active) <= 5
        assert snap.available_agents + len(active) == 5


@pytest.mark.parametrize("pending", ["PAUSED", "OFFLINE", None])
def test_guarded_release_and_requeue(database, pending):
    _, conn = database
    seed(conn, 1, 1)
    (old,) = batch(conn)
    settle(conn, old, 1000)
    (new,) = batch(conn)
    conn.execute(
        "UPDATE agents SET pending_state=? WHERE id=? AND current_call_id=?",
        (pending, new.agent_id, new.call_id),
    )
    conn.execute("UPDATE borrowers SET max_attempts=1 WHERE id=?", (new.borrower_id,))
    assert not release_agent(conn, old.agent_id, old.call_id, 1001)
    assert not requeue_borrower(
        conn, old.borrower_id, old.call_id, 5000, consume_attempt=True
    )
    assert release_agent(conn, new.agent_id, new.call_id, 1002)
    assert requeue_borrower(
        conn, new.borrower_id, new.call_id, 5000, consume_attempt=True
    )
    assert tuple(
        conn.execute(
            "SELECT state, available_since, pending_state FROM agents"
        ).fetchone()
    ) == (pending or "AVAILABLE", None if pending else 1002, None)
    assert tuple(
        conn.execute(
            "SELECT status, attempts, active_call_id FROM borrowers"
        ).fetchone()
    ) == ("EXHAUSTED", 1, None)
