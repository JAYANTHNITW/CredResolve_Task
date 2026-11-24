import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing

import pytest

from smartdialer import allocator
from smartdialer import repository as repo
from smartdialer.db import connect
from smartdialer.event_handler import handle_event
from smartdialer.models import CallState
from smartdialer.providers.base import ProviderEvent, ProviderStatus, ProviderTimeout
from smartdialer.recovery import Recovery


class WorkerCrash(Exception):
    pass


@pytest.mark.parametrize("stage", ["reserve", "place", "initiate", "answer"])
def test_crash_matrix(campaign, stage, monkeypatch):
    c = campaign
    c.seed()
    r = c.reserve()
    with pytest.raises(WorkerCrash), monkeypatch.context() as patch:
        if stage == "reserve":
            raise WorkerCrash
        if stage == "place":

            def crash(*args):
                raise WorkerCrash

            patch.setattr(allocator, "handle_event", crash)
        c.worker.allocator.place(r)
        if stage == "answer":
            c.advance(500)
            assert c.call(r)["state"] == "ANSWERED"
        raise WorkerCrash
    c.a.set_event_sink(lambda ev: None)
    with closing(connect(c.path)) as conn:
        recovery = Recovery(
            conn, "demo", c.clock, [c.a], heartbeat_ttl_ms=100000, quiet_ms=1000
        )
        c.advance(45001 if stage == "initiate" else 10001)
        recovery.sweep()
        expected = {
            "reserve": "CANCELLED",
            "place": "COMPLETED",
            "initiate": "FAILED",
            "answer": "COMPLETED",
        }
        assert c.call(r)["state"] == expected[stage]
        c.advance(30001)
        recovery.sweep()
    agent = repo.get_agent(c.conn, "a0")
    assert (agent["state"], agent["current_call_id"]) == ("AVAILABLE", None)
    assert c.conn.execute("SELECT active_call_id FROM borrowers").fetchone()[0] is None
    assert c.a.call_count == int(stage != "reserve")


def test_concurrent_sweepers_and_hangup_retry(campaign, monkeypatch):
    c = campaign
    c.seed()
    r = c.reserve()
    c.worker.allocator.place(r)
    c.advance(500)
    c.clock.advance(5000)

    def unavailable(call_id):
        raise ProviderTimeout

    monkeypatch.setattr(c.a, "hangup", unavailable)
    barrier = threading.Barrier(2)

    def sweep(_):
        with closing(connect(c.path)) as conn:
            barrier.wait(10)
            Recovery(conn, "demo", c.clock, [c.a]).sweep()

    with ThreadPoolExecutor(2) as pool:
        list(pool.map(sweep, range(2)))
    assert tuple(c.conn.execute("SELECT kind,call_id FROM incidents").fetchone()) == (
        "ABANDONED_AGENT_LOST",
        r.call_id,
    )
    assert c.conn.execute("SELECT COUNT(*) FROM incidents").fetchone()[0] == 1
    assert c.call(r)["fail_reason"] == "AGENT_LOST"
    assert repo.get_agent(c.conn, "a0")["state"] == "OFFLINE"
    assert c.conn.execute("SELECT COUNT(*) FROM pending_hangups").fetchone()[0] == 1
    monkeypatch.undo()
    c.worker.recovery.sweep()
    assert c.a.get_status(r.call_id) == ProviderStatus.ENDED
    assert not c.conn.execute("SELECT 1 FROM pending_hangups").fetchall()


def test_heartbeat_filters_before_sweep(campaign):
    c = campaign
    c.seed(100, 100)
    c.clock.advance(5000)
    for i in range(60):
        assert c.worker.recovery.heartbeat(f"a{i}")
    assert (
        len(repo.reserve_batch(c.conn, "demo", 100, c.clock.now_ms(), 10000, 5000))
        == 60
    )
    c.worker.recovery.sweep()
    assert repo.campaign_snapshot(c.conn, "demo").agents_by_state["OFFLINE"] == 40


def test_pause_deferred_and_logout_immediate(campaign):
    c = campaign
    c.seed()
    r = c.reserve()
    c.worker.allocator.place(r)
    c.advance(500)
    recovery = c.worker.recovery
    assert recovery.pause("a0")
    agent = repo.get_agent(c.conn, "a0")
    assert (agent["state"], agent["pending_state"]) == ("CONNECTED", "PAUSED")
    c.advance(1000)
    assert repo.get_agent(c.conn, "a0")["state"] == "PAUSED"
    assert recovery.login("a0")
    recovery.logout("a0")
    assert not recovery.heartbeat("a0")
    assert repo.get_agent(c.conn, "a0")["state"] == "OFFLINE"


@pytest.mark.parametrize("busy", ["RESERVED", "DIALING", "CONNECTED"])
def test_logout_during_call(campaign, busy):
    c = campaign
    c.seed()
    r = c.reserve()
    if busy != "RESERVED":
        c.worker.allocator.place(r)
    if busy == "CONNECTED":
        c.advance(500)
    c.worker.recovery.logout("a0")
    c.worker.recovery.logout("a0")
    assert c.call(r)["state"] == ("FAILED" if busy == "CONNECTED" else "CANCELLED")
    assert c.conn.execute("SELECT COUNT(*) FROM incidents").fetchone()[0] == int(
        busy == "CONNECTED"
    )
    assert tuple(
        c.conn.execute("SELECT status,attempts FROM borrowers").fetchone()
    ) == ("PENDING", 0)
    assert repo.get_agent(c.conn, "a0")["state"] == "OFFLINE"


def test_expired_reservation_resumes_and_stale_poll_cannot_overwrite_event(
    campaign, monkeypatch
):
    c = campaign
    c.seed()
    r = c.reserve()
    c.conn.execute(
        "UPDATE calls SET provider='A' WHERE id=? AND state='RESERVED'", (r.call_id,)
    )
    c.clock.advance(10001)
    recovery = Recovery(c.conn, "demo", c.clock, [c.a], heartbeat_ttl_ms=100000)

    def status(cid):
        assert not c.conn.in_transaction
        handle_event(
            c.conn,
            ProviderEvent("A", "answer", cid, CallState.ANSWERED, c.clock.now_ms()),
            c.clock.now_ms(),
        )
        return ProviderStatus.UNKNOWN

    monkeypatch.setattr(c.a, "get_status", status)
    recovery.sweep()
    assert c.call(r)["state"] == "ANSWERED"


def test_logout_while_placement_is_in_flight(campaign, monkeypatch):
    c = campaign
    c.seed()
    reservation = c.reserve()
    place = c.a.place_call

    def delayed(*args):
        c.worker.recovery.logout("a0")
        return place(*args)

    monkeypatch.setattr(c.a, "place_call", delayed)
    c.worker.allocator.place(reservation)
    c.worker.recovery.sweep()
    assert c.a.get_status(reservation.call_id) == ProviderStatus.ENDED
    assert c.call(reservation)["state"] == "CANCELLED"
