"""Inputs for the six progressive-dialing experiments. Durations are seconds."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Scenario:
    name: str
    answer_rate: float
    avg_talk_s: int
    agents: int = 10
    ring_time_s: int = 20
    second_half_rate: float | None = None
    b_outage: bool = False
    drop_fraction: float = 0
    extra_latency_ms: int = 0
    failure_rate: float = 0.01

    def rate(self, elapsed_ms: int, duration_ms: int) -> float:
        if self.second_half_rate is not None and elapsed_ms * 2 >= duration_ms:
            return self.second_half_rate
        return self.answer_rate


SCENARIOS = (
    Scenario("A", 0.2, 120),
    Scenario("B", 0.5, 90),
    Scenario("C", 0.7, 180),
    Scenario("D", 0.7, 120, second_half_rate=0.1),
    Scenario("E", 0.4, 120, b_outage=True),
    Scenario("F", 0.4, 120, drop_fraction=0.4),
)
