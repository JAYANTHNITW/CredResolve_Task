import json
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from contextlib import closing
from dataclasses import replace
from datetime import UTC
from random import Random

import pytest

from smartdialer import repository as repo
from smartdialer.breaker import Outcome
from smartdialer.clock import FakeClock
from smartdialer.db import connect, init_schema, tx
from smartdialer.dispatcher import Dispatcher
from smartdialer.models import CallState
from smartdialer.providers.base import (
    Accepted,
    ProviderEvent,
    ProviderTimeout,
    Scheduler,
)
from smartdialer.providers.mock_a import MockProviderA
from smartdialer.providers.mock_b import MockProviderB
from smartdialer.router import ProviderRouter
from smartdialer.safety import Action, ProviderLimit

NOON = 12 * 3600 * 1000


@pytest.fixture
def db(tmp_path):
    path = tmp_path / "dispatch.db"
    with closing(connect(path)) as conn:
        init_schema(conn)
        yield conn, path


def seed(conn, n=1, borrowers=None):
    with tx(conn):
        conn.executemany(
            """INSERT INTO agents(id,campaign_id,state,last_heartbeat_at)
            VALUES (?,'c','AVAILABLE',?)""",
            ((str(i), NOON) for i in range(n)),
        )
        conn.executemany(
            """INSERT INTO borrowers(id,campaign_id,phone,status)
            VALUES (?,'c','123','PENDING')""",
            ((str(i),) for i in range(borrowers or n)),
        )


def worker(conn, cap=100, cps=100):
    clock = FakeClock(NOON)
    scheduler = Scheduler()
    providers = [
        cls(clock, Random(i), scheduler)
        for i, cls in enumerate((MockProviderA, MockProviderB))
    ]
    for p in providers:
        p.set_failure_rate(0)
    return Dispatcher(
        conn,
        "c",
        clock,
        ProviderRouter(providers, clock),
        timezone=UTC,
        limits={p.name: ProviderLimit(cap, cps) for p in providers},
        backoff=clock.advance,
    )


def opened(breaker):
    for _ in range(5):
        breaker.record(Outcome.TIMEOUT)


def set_barrier(barrier):
    global start
    start = barrier


def tick_process(path):
    with closing(connect(path)) as conn:
        d = worker(conn)
        start.wait(timeout=60)
        d.tick()
        return sum(p.call_count for p in d.router.providers)


@pytest.mark.parametrize("workers", [1, 8])
def test_progressive_bound(db, workers):
    conn, path = db
    seed(conn, 50, 10000)
    if workers == 1:
        assert worker(conn).tick().approved == 50
    else:
        ctx = multiprocessing.get_context("spawn")
        with ProcessPoolExecutor(
            workers,
            mp_context=ctx,
            initializer=set_barrier,
            initargs=(ctx.Barrier(workers),),
        ) as pool:
            assert sum(pool.map(tick_process, [path] * workers)) == 50
    assert tuple(
        conn.execute(
            "SELECT COUNT(*),COUNT(DISTINCT agent_id),COUNT(DISTINCT borrower_id) FROM calls WHERE state='INITIATED'"
        ).fetchone()
    ) == (50, 50, 50)


@pytest.mark.parametrize(
    "reason",
    ["KILL_SWITCH", "CAMPAIGN_PAUSED", "OUTSIDE_WINDOW", "NO_HEALTHY_PROVIDER"],
)
def test_rejections_are_audited(db, reason):
    conn, _ = db
    seed(conn)
    d = worker(conn)
    if reason == "KILL_SWITCH":
        repo.set_dialing_stopped(conn, True)
    elif reason == "CAMPAIGN_PAUSED":
        repo.set_dialing_stopped(conn, True, "c")
    elif reason == "OUTSIDE_WINDOW":
        d.clock.advance(7 * 3600 * 1000)
    else:
        for b in d.router.breakers.values():
            opened(b)
    decision = d.tick()
    assert decision.action == Action.REJECT and decision.permit is None
    row = conn.execute("SELECT * FROM safety_decisions").fetchone()
    assert reason in json.loads(row["reason_codes"])
    assert json.loads(row["snapshot"])["campaign_id"] == "c"
    assert conn.execute("SELECT COUNT(*) FROM calls").fetchone()[0] == 0


def test_degraded_and_caps(db):
    conn, _ = db
    seed(conn, 8)
    d = worker(conn, cap=2, cps=1)
    opened(d.router.breakers["A"])
    first = d.tick()
    assert first.action == Action.REDUCE and first.approved == 1
    assert d.tick().approved == 0
    d.clock.advance(1000)
    assert d.tick().approved == 1
    d.clock.advance(1000)
    assert d.tick().approved == 0
    assert [p.call_count for p in d.router.providers] == [0, 2]


def test_budget_rechecked_after_overlapping_permits(db):
    conn, _ = db
    seed(conn, 8)
    a, b = worker(conn, 1, 1), worker(conn, 1, 1)

    def reserve(d):
        snap = d.snapshot()
        return d.allocator.reserve(
            d.safety.evaluate(d.pacing.request(snap), snap, NOON).permit
        )

    first, second = reserve(a), reserve(b)
    assert len(first) == len(second) == 2
    for d, batch in ((a, first), (b, second)):
        for r in batch:
            d.allocator.place(r)
    assert conn.execute("SELECT COUNT(*) FROM placement_attempts").fetchone()[0] == 2
    assert (
        conn.execute("SELECT COUNT(*) FROM calls WHERE state='INITIATED'").fetchone()[0]
        == 2
    )


def test_incomplete_stale_expired_and_reused(db):
    conn, _ = db
    seed(conn, 4)
    d = worker(conn)
    s = d.snapshot()
    req = d.pacing.request(s)
    for invalid in (
        replace(s, available_agents=None),
        replace(s, captured_at=NOON - 1001),
    ):
        assert d.safety.evaluate(req, invalid, NOON).action == Action.REJECT
    permit = d.safety.evaluate(req, s, NOON).permit
    with pytest.raises(ValueError):
        d.allocator.reserve(replace(permit, count=100))
    assert len(d.allocator.reserve(permit)) == 4
    with pytest.raises(ValueError):
        d.allocator.reserve(permit)
    expired = d.safety.evaluate(req, s, NOON).permit
    d.clock.advance(1001)
    with pytest.raises(ValueError):
        d.allocator.reserve(expired)


def test_all_rejected(db):
    conn, _ = db
    seed(conn)
    d = worker(conn)
    for p in d.router.providers:
        p.set_outage(True)
    d.tick()
    assert tuple(conn.execute("SELECT state,fail_reason FROM calls").fetchone()) == (
        "FAILED",
        "PROVIDER_ERROR",
    )
    assert conn.execute("SELECT state FROM agents").fetchone()[0] == "AVAILABLE"
    assert tuple(conn.execute("SELECT status,attempts FROM borrowers").fetchone()) == (
        "PENDING",
        0,
    )


def test_timeout_after_actual_placement(db, monkeypatch):
    conn, _ = db
    seed(conn)
    d = worker(conn)
    a, b = d.router.providers
    original = a.place_call
    keys = []

    def lost_ack(call, phone, key):
        assert not conn.in_transaction
        assert repo.get_call(conn, call)["provider"] == a.name
        keys.append(key)
        original(call, phone, key)
        raise ProviderTimeout()

    monkeypatch.setattr(a, "place_call", lost_ack)
    d.tick()
    assert len(keys) == 3 and len(set(keys)) == 1
    assert d.clock.now_ms() == NOON + 75
    assert (a.call_count, b.call_count) == (1, 0)
    assert tuple(conn.execute("SELECT state,provider FROM calls").fetchone()) == (
        "RESERVED",
        "A",
    )
    assert conn.execute("SELECT state FROM agents").fetchone()[0] == "RESERVED"
    assert conn.execute("SELECT status FROM borrowers").fetchone()[0] == "RESERVED"


def test_definite_failover_and_fast_ringing(db, monkeypatch):
    conn, _ = db
    seed(conn)
    d = worker(conn)
    a, b = d.router.providers
    a.set_outage(True)

    def fast(call, phone, key):
        assert not conn.in_transaction
        d._on_event(ProviderEvent("B", "fast", call, CallState.RINGING, NOON))
        return Accepted("ref")

    monkeypatch.setattr(b, "place_call", fast)
    d.tick()
    assert tuple(conn.execute("SELECT state,provider FROM calls").fetchone()) == (
        "RINGING",
        "B",
    )
    assert conn.execute("SELECT state FROM agents").fetchone()[0] == "DIALING"


def test_kill_switch_between_reservation_and_placement(db):
    conn, _ = db
    seed(conn)
    d = worker(conn)
    s = d.snapshot()
    decision = d.safety.evaluate(d.pacing.request(s), s, NOON)
    (reservation,) = d.allocator.reserve(decision.permit)
    repo.set_dialing_stopped(conn, True)
    d.allocator.place(reservation)
    assert all(p.call_count == 0 for p in d.router.providers)
    assert conn.execute("SELECT COUNT(*) FROM placement_attempts").fetchone()[0] == 0
    with pytest.raises(ValueError, match="unused reservation"):
        d.allocator.place(reservation)


def test_tick_recovers_when_permit_expires_before_reservation(db, monkeypatch):
    conn, _ = db
    seed(conn)
    d = worker(conn)
    reserve = d.allocator.reserve

    def delayed(permit):
        d.clock.advance(1001)
        return reserve(permit)

    with monkeypatch.context() as patch:
        patch.setattr(d.allocator, "reserve", delayed)
        d.tick()
    assert conn.execute("SELECT COUNT(*) FROM calls").fetchone()[0] == 0
    assert d.tick().approved == 1
    assert (
        conn.execute("SELECT COUNT(*) FROM calls WHERE state='INITIATED'").fetchone()[0]
        == 1
    )
