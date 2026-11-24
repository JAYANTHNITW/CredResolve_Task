"""Campaign recovery and presence. Any worker may sweep the same campaign."""

from smartdialer import repository as repo
from smartdialer.db import tx
from smartdialer.event_handler import Outcome, apply_event
from smartdialer.models import AgentState, CallState, FailReason
from smartdialer.providers.base import ProviderEvent, ProviderStatus, ProviderTimeout


class Recovery:
    def __init__(
        self,
        conn,
        campaign_id,
        clock,
        providers,
        *,
        heartbeat_ttl_ms=5000,
        max_ring_ms=45000,
        quiet_ms=300000,
    ):
        self.conn, self.campaign_id, self.clock = conn, campaign_id, clock
        self.providers = {p.name: p for p in providers}
        self.heartbeat_ttl_ms, self.max_ring_ms, self.quiet_ms = (
            heartbeat_ttl_ms,
            max_ring_ms,
            quiet_ms,
        )

    def _agent(self, agent_id):
        agent = repo.get_agent(self.conn, agent_id)
        if agent is None or agent["campaign_id"] != self.campaign_id:
            raise KeyError(agent_id)
        return agent

    def heartbeat(self, agent_id):
        with tx(self.conn):
            self._agent(agent_id)
            return repo.heartbeat(self.conn, agent_id, self.clock.now_ms())

    def pause(self, agent_id):
        with tx(self.conn):
            agent = self._agent(agent_id)
            if agent["state"] in (AgentState.OFFLINE, AgentState.PAUSED):
                return False
            return repo.change_presence(
                self.conn, agent, AgentState.PAUSED, self.clock.now_ms()
            )

    def login(self, agent_id):
        with tx(self.conn):
            agent = self._agent(agent_id)
            if agent["state"] not in (AgentState.OFFLINE, AgentState.PAUSED):
                return False
            now = self.clock.now_ms()
            if not repo.change_presence(self.conn, agent, AgentState.AVAILABLE, now):
                return False
            return repo.heartbeat(self.conn, agent_id, now)

    def logout(self, agent_id):
        with tx(self.conn):
            self._offline(self._agent(agent_id), self.clock.now_ms())
        self._drain_hangups()

    def _event(self, call, state, now, reason=None):
        ev = ProviderEvent(
            call["provider"] or "recovery", None, call["id"], state, now, reason
        )
        return apply_event(self.conn, ev, now)

    def _offline(self, agent, now):
        if agent["state"] == AgentState.OFFLINE:
            return
        if not repo.change_presence(self.conn, agent, AgentState.OFFLINE, now):
            return
        if agent["current_call_id"] is None:
            return
        call = repo.get_call(self.conn, agent["current_call_id"])
        connected = agent["state"] == AgentState.CONNECTED
        state = CallState.FAILED if connected else CallState.CANCELLED
        if self._event(call, state, now, FailReason.AGENT_LOST) == Outcome.APPLIED:
            repo.enqueue_hangup(self.conn, call)
            if connected:
                repo.record_agent_loss(self.conn, call, now)

    def _drain_hangups(self):
        for call_id, name in repo.outstanding_hangups(self.conn, self.campaign_id):
            provider = self.providers.get(name)
            if provider is None:
                continue
            try:
                provider.hangup(call_id)
            except (ProviderTimeout, ConnectionError):
                continue
            repo.finish_hangup(self.conn, call_id)

    def sweep(self):
        now = self.clock.now_ms()
        cutoff = now - self.heartbeat_ttl_ms
        for (agent_id,) in repo.agents_due(self.conn, self.campaign_id, now, cutoff):
            with tx(self.conn):
                agent = self._agent(agent_id)
                if agent["last_heartbeat_at"] <= cutoff:
                    self._offline(agent, now)
                elif (
                    agent["state"] == AgentState.WRAP_UP
                    and agent["wrap_up_until"] is not None
                    and agent["wrap_up_until"] <= now
                ) and not repo.change_presence(
                    self.conn,
                    agent,
                    agent["pending_state"] or AgentState.AVAILABLE,
                    now,
                ):
                    raise RuntimeError("agent changed during wrap-up release")
        for call in repo.calls_due(
            self.conn,
            self.campaign_id,
            now,
            now - self.max_ring_ms,
            now - self.quiet_ms,
        ):
            self._reconcile(call, now)
        self._drain_hangups()

    def _reconcile(self, call, now):
        ring_timeout = call["state"] in (CallState.INITIATED, CallState.RINGING)
        reason = None
        if ring_timeout:
            target, reason = CallState.FAILED, FailReason.NO_ANSWER
        else:
            provider = self.providers.get(call["provider"])
            if call["provider"] is not None and provider is None:
                return
            try:
                status = (
                    provider.get_status(call["id"])
                    if provider
                    else ProviderStatus.UNKNOWN
                )
            except (ProviderTimeout, ConnectionError):
                return
            target = {
                ProviderStatus.INITIATED: CallState.INITIATED,
                ProviderStatus.RINGING: CallState.RINGING,
                ProviderStatus.ANSWERED: CallState.ANSWERED,
                ProviderStatus.ENDED: CallState.COMPLETED,
                ProviderStatus.FAILED: CallState.FAILED,
                ProviderStatus.UNKNOWN: CallState.CANCELLED,
            }[status]
            if call["state"] != CallState.RESERVED and target not in (
                CallState.COMPLETED,
                CallState.FAILED,
                CallState.CANCELLED,
            ):
                return
            if target == CallState.CANCELLED:
                reason = (
                    FailReason.RESERVATION_EXPIRED
                    if call["state"] == CallState.RESERVED
                    else FailReason.PROVIDER_ERROR
                )
            if target == CallState.FAILED:
                reason = FailReason.NO_ANSWER
        with tx(self.conn):
            current = repo.get_call(self.conn, call["id"])
            if (current["version"], current["provider"]) != (
                call["version"],
                call["provider"],
            ):
                return
            if (
                self._event(call, target, now, reason) == Outcome.APPLIED
                and ring_timeout
            ):
                repo.enqueue_hangup(self.conn, call)
