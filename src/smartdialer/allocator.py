"""Consume audited permits and place the resulting reservations outside transactions."""

from collections.abc import Callable
from sqlite3 import Connection
from threading import Event

from smartdialer import repository as repo
from smartdialer.clock import Clock
from smartdialer.db import tx
from smartdialer.event_handler import handle_event
from smartdialer.models import CallState, FailReason
from smartdialer.providers.base import Accepted, ProviderEvent, ProviderTimeout
from smartdialer.router import ProviderRouter
from smartdialer.safety import Permit, SafetyController


def wait_ms(ms: int) -> None:
    Event().wait(ms / 1000)


class InvalidPermit(ValueError):
    """No current authority to reserve."""


class Allocator:
    def __init__(
        self,
        conn: Connection,
        clock: Clock,
        router: ProviderRouter,
        safety: SafetyController,
        *,
        heartbeat_ttl_ms=5000,
        lease_ms=10000,
        backoff: Callable[[int], None] = wait_ms,
    ):
        self.conn, self.clock, self.router, self.safety = conn, clock, router, safety
        self.heartbeat_ttl_ms, self.lease_ms, self.backoff = (
            heartbeat_ttl_ms,
            lease_ms,
            backoff,
        )
        self._pending: dict[str, repo.Reservation] = {}

    def reserve(self, permit: Permit) -> list[repo.Reservation]:
        if (
            not isinstance(permit, Permit)
            or permit.campaign_id != self.safety.campaign_id
        ):
            raise InvalidPermit("invalid campaign permit")
        with tx(self.conn):
            now = self.clock.now_ms()
            if now >= permit.expires_at or not repo.consume_permit(
                self.conn, permit.token, permit.campaign_id, permit.count, now
            ):
                raise InvalidPermit("expired, unknown or used permit")
            if any(
                repo.dialing_switches(self.conn, permit.campaign_id)
            ) or not self.safety.calling_window(now):
                return []
            reservations = repo.reserve_in_transaction(
                self.conn,
                permit.campaign_id,
                permit.count,
                now,
                self.lease_ms,
                self.heartbeat_ttl_ms,
            )
        self._pending.update((r.call_id, r) for r in reservations)
        return reservations

    def place(self, reservation: repo.Reservation):
        if self.conn.in_transaction:
            raise RuntimeError("placement requires a committed reservation")
        if self._pending.get(reservation.call_id) != reservation:
            raise ValueError(
                "placement requires an unused reservation from this allocator"
            )
        del self._pending[reservation.call_id]
        call = repo.get_call(self.conn, reservation.call_id)
        if call["state"] != CallState.RESERVED:
            return
        previous = None

        def admit(name, retry):
            nonlocal previous
            limit = self.safety.limits.get(name)
            if limit is None:
                return False
            with tx(self.conn):
                now = self.clock.now_ms()
                if not self.safety.calling_window(now) or not repo.claim_placement(
                    self.conn,
                    reservation.call_id,
                    name,
                    previous,
                    now,
                    limit.concurrency,
                    limit.cps,
                    retry=retry,
                    campaign_id=self.safety.campaign_id,
                ):
                    return False
            previous = name
            return True

        try:
            result = self.router.place_call(
                reservation.call_id,
                reservation.phone,
                call["idempotency_key"],
                before_attempt=admit,
                backoff=self.backoff,
            )
        except ProviderTimeout:
            # The persisted provider and lease allow recovery to reconcile an ambiguous dial.
            return
        now = self.clock.now_ms()
        accepted = isinstance(result, Accepted)
        event = ProviderEvent(
            previous or "allocator",
            None,
            reservation.call_id,
            CallState.INITIATED if accepted else CallState.FAILED,
            now,
            None if accepted else FailReason.PROVIDER_ERROR,
        )
        handle_event(self.conn, event, now)
