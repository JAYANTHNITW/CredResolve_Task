from smartdialer import repository as repo
from smartdialer.event_handler import Outcome, handle_event
from smartdialer.models import CallState
from smartdialer.providers.base import ProviderEvent


def test_out_of_order(campaign):
    c = campaign
    c.seed()
    r = c.reserve()
    c.worker.allocator.place(r)
    for state, expected in [
        (CallState.COMPLETED, Outcome.APPLIED),
        (CallState.ANSWERED, Outcome.STALE),
        (CallState.RINGING, Outcome.STALE),
    ]:
        c.clock.advance(100)
        ev = ProviderEvent("A", str(state), r.call_id, state, c.clock.now_ms())
        outcome = handle_event(c.conn, ev, c.clock.now_ms())
        assert outcome == expected
        c.line(f"{state} arrived: {outcome}; call is {c.call(r)['state']}.")
    assert c.call(r)["state"] == "COMPLETED"
    assert repo.get_agent(c.conn, r.agent_id)["state"] == "WRAP_UP"
    assert c.conn.execute("SELECT status FROM borrowers").fetchone()[0] == "DONE"
