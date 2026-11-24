"""One progressive dialing worker. All workers share the same SQLite database."""

from random import Random
from threading import Event

from smartdialer import repository as repo
from smartdialer.allocator import Allocator, InvalidPermit, wait_ms
from smartdialer.db import tx
from smartdialer.event_handler import handle_event
from smartdialer.pacing import ProgressivePacing
from smartdialer.safety import ProviderCapacity, SafetyController, Snapshot


class Dispatcher:
    def __init__(
        self,
        conn,
        campaign_id,
        clock,
        router,
        *,
        timezone,
        limits,
        heartbeat_ttl_ms=5000,
        lease_ms=10000,
        backoff=wait_ms,
    ):
        self.conn, self.campaign_id, self.clock, self.router = (
            conn,
            campaign_id,
            clock,
            router,
        )
        self.pacing = ProgressivePacing()
        self.safety = SafetyController(conn, campaign_id, timezone, limits)
        self.allocator = Allocator(
            conn,
            clock,
            router,
            self.safety,
            heartbeat_ttl_ms=heartbeat_ttl_ms,
            lease_ms=lease_ms,
            backoff=backoff,
        )
        for provider in router.providers:
            provider.set_event_sink(self._on_event)

    def _on_event(self, event):
        handle_event(self.conn, event, self.clock.now_ms())

    def snapshot(self):
        with tx(self.conn):
            now = self.clock.now_ms()
            killed, paused = repo.dialing_switches(self.conn, self.campaign_id)
            agents = repo.available_count(
                self.conn, self.campaign_id, now - self.allocator.heartbeat_ttl_ms
            )
            providers = {}
            for name, breaker in self.router.breakers.items():
                limit = self.safety.limits.get(name)
                if limit is None:
                    # Missing provider configuration must fail the completeness check.
                    providers[name] = ProviderCapacity("UNKNOWN", 0, 0)
                    continue
                active, recent = repo.provider_usage(self.conn, name, now)
                providers[name] = ProviderCapacity(
                    breaker.state,
                    max(0, limit.concurrency - active),
                    max(0, limit.cps - recent),
                )
            return Snapshot(
                self.campaign_id,
                now,
                agents,
                providers,
                paused,
                killed,
                self.safety.calling_window(now),
            )

    def tick(self):
        snapshot = self.snapshot()
        decision = self.safety.evaluate(
            self.pacing.request(snapshot), snapshot, self.clock.now_ms()
        )
        if decision.permit is not None:
            try:
                reservations = self.allocator.reserve(decision.permit)
            except InvalidPermit:
                # Contention can outlast the permit. The next tick takes a new snapshot.
                return decision
            for reservation in reservations:
                self.allocator.place(reservation)
        return decision

    def run(self, stop: Event, rng: Random):
        while not stop.is_set():
            self.tick()
            stop.wait(rng.uniform(0.18, 0.22))
