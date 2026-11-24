import multiprocessing
from contextlib import closing
from datetime import UTC

from smartdialer.clock import FakeClock
from smartdialer.db import connect
from smartdialer.dispatcher import Dispatcher
from smartdialer.router import ProviderRouter
from smartdialer.safety import ProviderLimit


class RemoteProvider:
    name = "A"

    def __init__(self, pipe):
        self.pipe = pipe

    def set_event_sink(self, sink):
        pass

    def place_call(self, call_id, to, key):
        self.pipe.send((call_id, to, key))
        return self.pipe.recv()


def dispatch_in_child(path, pipe, now):
    with closing(connect(path)) as conn:
        clock = FakeClock(now)
        router = ProviderRouter([RemoteProvider(pipe)], clock)
        Dispatcher(
            conn,
            "demo",
            clock,
            router,
            timezone=UTC,
            limits={"A": ProviderLimit(10, 10)},
        ).tick()


def test_worker_crash(campaign):
    c = campaign
    c.seed()
    ctx = multiprocessing.get_context("spawn")
    parent, child = ctx.Pipe()
    process = ctx.Process(target=dispatch_in_child, args=(c.path, child, c.start))
    process.start()
    child.close()
    try:
        assert parent.poll(30)
        call_id, phone, key = parent.recv()
        c.a.set_extra_latency(12000)
        c.a.place_call(call_id, phone, key)
        # Kill the dispatcher while it is waiting for the placement acknowledgement.
        c.line(
            "Provider A accepted the call; worker stopped before recording INITIATED."
        )
    finally:
        process.terminate()
        process.join(10)
        parent.close()
    assert not process.is_alive()
    process.close()
    assert c.conn.execute("SELECT state FROM calls").fetchone()[0] == "RESERVED"
    c.advance(10001)
    c.worker.recovery.heartbeat("a0")
    c.worker.recovery.sweep()
    assert c.conn.execute("SELECT state FROM calls").fetchone()[0] == "INITIATED"
    c.line("Lease expired. Recovery polled A and resumed the same call.")
    c.advance(14000)
    assert c.conn.execute("SELECT status FROM borrowers").fetchone()[0] == "DONE"
    assert c.a.call_count == 1
    c.line("Borrower finished; provider-side call count = 1.")
