from smartdialer.scenarios import Scenario
from smartdialer.simulator import simulate


def test_clean_progressive_utilization_matches_closed_form():
    s = Scenario("clean", 0.5, 40, agents=12, ring_time_s=4, failure_rate=0)
    result = simulate(s, duration_s=900)
    # R includes 100 ms mean setup latency before ringing.
    expected = 40 / (40 + 30 + 4.1 / 0.5)
    assert abs(result.talk_pct / 100 - expected) <= 0.1 * expected
    assert result.calls[1] == result.calls[2] > 0
    assert result.lost == result.abandoned == 0


def test_midshift_drop_reports_connected_agent_losses():
    s = Scenario(
        "drop", 1, 120, agents=5, ring_time_s=1, failure_rate=0, drop_fraction=0.4
    )
    result = simulate(s, duration_s=30)
    assert result.calls == (5, 5, 5)
    assert result.abandoned == 0
    assert result.lost == 2


def test_step_change_at_midpoint():
    s = Scenario("change", 0.7, 120, second_half_rate=0.1)
    assert s.rate(499, 1000) == 0.7
    assert s.rate(500, 1000) == 0.1
