from smartdialer import repository as repo
from smartdialer.event_handler import Outcome, handle_event
from smartdialer.models import CallState
from smartdialer.providers.base import ProviderEvent


def test_duplicate_events(campaign):
    c = campaign
    c.seed()
    r = c.reserve()
    c.worker.allocator.place(r)
    v = repo.get_agent(c.conn, r.agent_id)["version"]
    event = ProviderEvent(
        "A", "answered-1", r.call_id, CallState.ANSWERED, c.clock.now_ms()
    )
    outcomes = []
    for _ in range(5):
        c.clock.advance(100)
        outcomes.append(handle_event(c.conn, event, c.clock.now_ms()))
    assert outcomes == [Outcome.APPLIED] + [Outcome.DUPLICATE] * 4
    assert repo.get_agent(c.conn, r.agent_id)["version"] == v + 1
    c.line("ANSWERED delivered 5 times: 1 applied, 4 duplicates, 1 agent transition.")
