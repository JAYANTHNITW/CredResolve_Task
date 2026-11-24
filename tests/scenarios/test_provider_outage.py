from smartdialer import repository as repo
from smartdialer.breaker import BreakerState
from smartdialer.recovery import Recovery
from smartdialer.safety import Action


def test_provider_outage(campaign):
    c = campaign
    c.seed(4, 1)
    d = c.worker
    # Prefer B on a health tie so it has an active call when the outage starts.
    d.router.providers = (c.b, c.a)
    c.b.talk_time_dist = lambda rng: 4000
    c.b.reorder_rate = c.b.loss_rate = 0
    d.tick()
    call = c.conn.execute("SELECT id FROM calls WHERE provider='B'").fetchone()[0]
    c.advance(2250)
    assert repo.get_call(c.conn, call)["state"] == "ANSWERED"
    c.b.set_event_sink(lambda event: None)
    c.b.set_outage(True)
    c.line("B stopped accepting calls. Its connected call lost its event feed.")
    c.conn.execute(
        "INSERT INTO borrowers(id,campaign_id,phone,status) VALUES ('extra','demo','123','PENDING')"
    )
    d.tick()
    assert c.a.call_count == 1
    c.line("B rejected the next placement; A accepted it.")
    c.a.set_outage(True)
    c.conn.executemany(
        "INSERT INTO borrowers(id,campaign_id,phone,status) VALUES (?,'demo','123','PENDING')",
        [(f"more{i}",) for i in range(100)],
    )
    for _ in range(30):
        c.advance(200)
        for i in range(4):
            d.recovery.heartbeat(f"a{i}")
        decision = d.tick()
        if decision.action == Action.REJECT:
            break
    assert "NO_HEALTHY_PROVIDER" in decision.reasons
    assert all(b.state == BreakerState.OPEN for b in d.router.breakers.values())
    free = d.snapshot().available_agents
    assert free > 0
    delays = [
        r[0]
        for r in c.conn.execute("""SELECT b.next_attempt_at-c.ended_at FROM borrowers b JOIN calls c
          ON c.borrower_id=b.id WHERE c.fail_reason='PROVIDER_ERROR' AND b.status='PENDING'""")
    ]
    assert delays and all(2000 <= delay <= 5000 for delay in delays)
    c.line(
        f"Both breakers open: safety rejected new calls; {free} agents available. Borrowers scheduled for jittered retries."
    )
    c.advance(10001)
    for i in range(4):
        d.recovery.heartbeat(f"a{i}")
    Recovery(c.conn, "demo", c.clock, [c.a, c.b], quiet_ms=1000).sweep()
    assert repo.get_call(c.conn, call)["state"] == "COMPLETED"
    c.b.set_outage(False)
    d.tick()
    assert d.router.breakers["B"].state == BreakerState.CLOSED and c.b.call_count > 1
    c.line(
        "Status polling recovered the lost completion. B accepted its cooldown probe and dialing resumed."
    )
