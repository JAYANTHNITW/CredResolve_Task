"""Evaluate and audit dialing authority for one campaign."""

import json
from dataclasses import InitVar, asdict, dataclass
from datetime import datetime, tzinfo
from enum import StrEnum
from sqlite3 import Connection
from uuid import uuid4

from smartdialer import repository as repo
from smartdialer.db import tx
from smartdialer.pacing import PacingRequest


class Action(StrEnum):
    APPROVE = "APPROVE"
    REDUCE = "REDUCE"
    REJECT = "REJECT"


@dataclass(frozen=True)
class ProviderLimit:
    concurrency: int
    cps: int

    def __post_init__(self):
        if min(self.concurrency, self.cps) < 0:
            raise ValueError("provider limits must be nonnegative")


@dataclass(frozen=True)
class ProviderCapacity:
    state: str
    headroom: int
    rate_budget: int


@dataclass(frozen=True)
class Snapshot:
    campaign_id: str
    captured_at: int
    available_agents: int | None
    providers: dict[str, ProviderCapacity]
    paused: bool | None
    killed: bool | None
    within_window: bool | None


_SECRET = object()


@dataclass(frozen=True)
class Permit:
    campaign_id: str
    count: int
    expires_at: int
    token: str
    _authority: InitVar[object]

    def __post_init__(self, _authority):
        if _authority is not _SECRET:
            raise TypeError("permit requires SafetyController authority")


@dataclass(frozen=True)
class Decision:
    action: Action
    approved: int
    permit: Permit | None
    reasons: tuple[str, ...]


class SafetyController:
    def __init__(
        self,
        conn: Connection,
        campaign_id: str,
        timezone: tzinfo,
        limits: dict[str, ProviderLimit],
    ):
        self.conn, self.campaign_id = conn, campaign_id
        self.timezone, self.limits = timezone, dict(limits)

    def calling_window(self, now_ms: int) -> bool:
        # Each campaign groups borrowers in one explicitly configured local timezone.
        return (
            self.timezone is not None
            and 8 <= datetime.fromtimestamp(now_ms / 1000, self.timezone).hour < 19
        )

    def evaluate(
        self, request: PacingRequest, snapshot: Snapshot, now_ms: int
    ) -> Decision:
        s = snapshot
        reasons = []
        complete = (
            s.campaign_id == self.campaign_id
            and s.available_agents is not None
            and s.available_agents >= 0
            and request.requested >= 0
            and None not in (s.paused, s.killed, s.within_window)
            and s.providers.keys() == self.limits.keys()
            and all(
                p.state in ("OPEN", "HALF_OPEN", "CLOSED")
                and min(p.headroom, p.rate_budget) >= 0
                for p in s.providers.values()
            )
        )
        if not complete:
            reasons.append("INCOMPLETE_SNAPSHOT")
        if not 0 <= now_ms - s.captured_at <= 1000:
            reasons.append("STALE_SNAPSHOT")
        if s.paused:
            reasons.append("CAMPAIGN_PAUSED")
        if s.killed:
            reasons.append("KILL_SWITCH")
        if s.within_window is False or not self.calling_window(now_ms):
            reasons.append("OUTSIDE_WINDOW")
        usable = [p for p in s.providers.values() if p.state != "OPEN"]
        if not usable:
            reasons.append("NO_HEALTHY_PROVIDER")
        approved = 0
        if reasons:
            action = Action.REJECT
        else:
            capacity = sum(
                min(
                    p.headroom,
                    p.rate_budget,
                    1 if p.state == "HALF_OPEN" else p.headroom,
                )
                for p in usable
            )
            approved = min(request.requested, s.available_agents, capacity)
            if len(usable) < len(s.providers):
                reasons.append("PROVIDER_DEGRADED")
                approved = min(
                    approved,
                    (request.requested * len(usable) + len(s.providers) - 1)
                    // len(s.providers),
                )
            if any(p.headroom == 0 for p in usable):
                reasons.append("CONCURRENCY_CAP")
            if any(p.rate_budget == 0 for p in usable):
                reasons.append("CPS_CAP")
            if approved < request.requested:
                if s.available_agents < request.requested:
                    reasons.append("AGENT_LIMIT")
                if capacity < request.requested:
                    reasons.append("PROVIDER_CAPACITY")
            action = Action.REDUCE if reasons else Action.APPROVE
            if not reasons:
                reasons.append("WITHIN_LIMITS")
        permit = None
        with tx(self.conn):
            repo.audit_decision(
                self.conn,
                now_ms,
                self.campaign_id,
                request.requested,
                approved,
                action,
                json.dumps(reasons),
                json.dumps({**asdict(s), "request_reasons": request.reasons}),
            )
            if approved:
                expiry = s.captured_at + 1001
                permit = Permit(
                    self.campaign_id, approved, expiry, uuid4().hex, _SECRET
                )
                repo.issue_permit(
                    self.conn, permit.token, self.campaign_id, approved, expiry, now_ms
                )
        return Decision(action, approved, permit, tuple(reasons))
