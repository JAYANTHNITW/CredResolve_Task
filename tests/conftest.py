"""Small virtual-time campaign shared by the recovery tests and failure demos."""

from contextlib import closing
from datetime import UTC
from random import Random

import pytest

from smartdialer import repository as repo
from smartdialer.clock import FakeClock
from smartdialer.db import connect, init_schema, tx
from smartdialer.dispatcher import Dispatcher
from smartdialer.providers.base import Scheduler
from smartdialer.providers.mock_a import MockProviderA
from smartdialer.providers.mock_b import MockProviderB
from smartdialer.safety import ProviderLimit


class Campaign:
    def __init__(self, conn, path):
        self.conn, self.path = conn, path
        self.start = 12 * 3600000
        self.clock = FakeClock(self.start)
        self.scheduler = Scheduler()

        def provider(cls):
            p = cls(
                self.clock,
                Random(4),
                self.scheduler,
                answer_rate_fn=lambda now: 1,
                ring_time_dist=lambda rng: 200,
                talk_time_dist=lambda rng: 1000,
            )
            p.set_failure_rate(0)
            return p

        self.a, self.b = provider(MockProviderA), provider(MockProviderB)
        self.worker = self.new_worker(conn)

    def new_worker(self, conn):
        from smartdialer.router import ProviderRouter

        return Dispatcher(
            conn,
            "demo",
            self.clock,
            ProviderRouter([self.a, self.b], self.clock),
            timezone=UTC,
            limits={p.name: ProviderLimit(500, 500) for p in [self.a, self.b]},
            backoff=self.clock.advance,
        )

    def seed(self, agents=1, borrowers=1):
        with tx(self.conn):
            self.conn.executemany(
                """INSERT INTO agents(id,campaign_id,state,last_heartbeat_at)
                VALUES (?,'demo','AVAILABLE',?)""",
                [(f"a{i}", self.clock.now_ms()) for i in range(agents)],
            )
            self.conn.executemany(
                """INSERT INTO borrowers(id,campaign_id,phone,status)
                VALUES (?,'demo','123','PENDING')""",
                [(f"b{i}",) for i in range(borrowers)],
            )

    def reserve(self):
        snap = self.worker.snapshot()
        dec = self.worker.safety.evaluate(
            self.worker.pacing.request(snap), snap, self.clock.now_ms()
        )
        return self.worker.allocator.reserve(dec.permit)[0]

    def advance(self, ms):
        self.clock.advance(ms)
        self.scheduler.run_until(self.clock.now_ms())

    def call(self, r):
        return repo.get_call(self.conn, r.call_id)

    def line(self, message):
        print(f"t={(self.clock.now_ms() - self.start) / 1000:.3f}s | {message}")


@pytest.fixture
def campaign(tmp_path):
    path = tmp_path / "campaign.db"
    with closing(connect(path)) as conn:
        init_schema(conn)
        yield Campaign(conn, path)
