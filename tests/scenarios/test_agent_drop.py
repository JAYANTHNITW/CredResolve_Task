from smartdialer import repository as repo


def test_agent_drop(campaign):
    c = campaign
    c.seed(100, 100)
    s = c.worker.snapshot()
    before = c.worker.safety.evaluate(c.worker.pacing.request(s), s, c.clock.now_ms())
    assert before.approved == 100
    c.line("100 agents heartbeating; safety approved 100 calls.")
    c.advance(5000)
    for i in range(60):
        c.worker.recovery.heartbeat(f"a{i}")
    c.line("40 heartbeats reached the 5 s TTL.")
    c.advance(200)
    after = c.worker.tick()
    assert after.approved == 60
    counts = repo.campaign_snapshot(c.conn, "demo").agents_by_state
    assert counts["OFFLINE"] == 40 and counts["DIALING"] == 60
    c.line("Next tick approved 60; 60 dialing, 40 offline.")
