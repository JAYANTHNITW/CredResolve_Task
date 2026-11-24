"""Run the real dispatcher on a 100 ms virtual clock and report measured outcomes."""

import argparse
from collections import Counter
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC
from pathlib import Path
from random import Random

from smartdialer import repository as repo
from smartdialer.clock import FakeClock
from smartdialer.db import connect, init_schema
from smartdialer.dispatcher import Dispatcher
from smartdialer.providers.base import Scheduler
from smartdialer.providers.mock_a import MockProviderA
from smartdialer.providers.mock_b import MockProviderB
from smartdialer.router import ProviderRouter
from smartdialer.safety import ProviderLimit
from smartdialer.scenarios import SCENARIOS, Scenario

START = 8 * 3600_000
STEP = 100


@dataclass
class Result:
    name: str
    talk_pct: float
    occupied_pct: float
    calls: tuple[int, int, int]
    abandoned: int
    lost: int
    actions: Counter
    reasons: Counter
    pacing: list


def simulate(
    s: Scenario, duration_s: int = 28800, workers: int = 2, seed: int = 42
) -> Result:
    if duration_s < 1 or workers < 1 or s.agents < 1 or s.ring_time_s <= 0:
        raise ValueError(
            "duration, worker count, agent count and ring duration must be positive"
        )
    duration = duration_s * 1000
    end = START + duration
    clock = FakeClock(START)
    scheduler = Scheduler()
    with closing(connect(":memory:")) as conn:
        init_schema(conn)
        repo.seed_campaign(
            conn,
            s.name,
            s.agents,
            s.agents * (duration_s // s.ring_time_s + 3),
            START,
            offline=True,
        )
        repo.simulation_observers(conn)
        args = {
            "answer_rate_fn": lambda now: s.rate(now - START, duration),
            "ring_time_dist": lambda rng: s.ring_time_s * 1000,
            "talk_time_dist": lambda rng: rng.randint(
                s.avg_talk_s * 500, s.avg_talk_s * 1500
            ),
        }
        a = MockProviderA(clock, Random(seed), scheduler, **args)
        b = MockProviderB(clock, Random(seed + 1), scheduler, **args)
        a.set_failure_rate(s.failure_rate)
        a.set_extra_latency(s.extra_latency_ms)
        b.set_extra_latency(s.extra_latency_ms + (1500 if s.b_outage else 0))
        b.set_failure_rate(0.25 if s.b_outage else 0.10)
        providers = [b, a] if s.b_outage else [a, b]
        pool = [
            Dispatcher(
                conn,
                s.name,
                clock,
                ProviderRouter(providers, clock),
                timezone=UTC,
                limits={
                    p.name: ProviderLimit(s.agents, s.agents * 2) for p in providers
                },
                backoff=clock.advance,
            )
            for _ in range(workers)
        ]
        for i in range(s.agents):
            pool[0].recovery.login(f"{s.name}-a{i}")
        talk = wrap = logged = 0
        reasons, actions = Counter(), Counter()
        changed = False
        tick = 0
        while clock.now_ms() < end:
            now = clock.now_ms()
            if not changed and (now - START) * 2 >= duration:
                changed = True
                if s.b_outage:
                    b.set_outage(True)
                if s.drop_fraction:
                    repo.simulation_drop(conn, round(s.agents * s.drop_fraction))
            repo.simulation_heartbeats(conn, now)
            scheduler.run_until(now)
            # Stagger independent workers on the same campaign.
            if tick % 2 == 0:
                decision = pool[(tick // 2) % workers].tick()
                actions[decision.action] += 1
                reasons.update(decision.reasons)
            counts = repo.simulation_agent_counts(conn)
            next_time = min(end, max(now + STEP, clock.now_ms()))
            dt = next_time - now
            logged += (s.agents - counts.get("OFFLINE", 0)) * dt
            talk += counts.get("CONNECTED", 0) * dt
            wrap += counts.get("WRAP_UP", 0) * dt
            clock.advance(max(0, next_time - clock.now_ms()))
            tick += 1
        calls, abandoned, lost = repo.simulation_totals(conn)
        mid = START + duration // 2
        pacing = [
            repo.simulation_pacing(conn, lo, hi)
            for lo, hi in ((START, end), (START, mid), (mid, end))
        ]
        return Result(
            s.name,
            100 * talk / logged,
            100 * (talk + wrap) / logged,
            calls,
            abandoned,
            lost,
            actions,
            reasons,
            pacing,
        )


def scenario_notes():
    return """Each campaign has 10 agents, 20 s of ringing per attempt and 30 s of wrap-up. Talk durations are uniformly distributed from half to one-and-a-half times the scenario mean. Answered and connected counts come from the call and agent state transitions observed by the dispatcher.

| Scenario | Answer probability | Mean talk time | Change halfway through shift |
|---|---:|---:|---|
| A | 20% | 120 s | None |
| B | 50% | 90 s | None |
| C | 70% | 180 s | None |
| D | 70% then 10% | 120 s | Answer probability falls |
| E | 40% | 120 s | B starts with 1.5 s extra latency and a 25% timeout rate, then stops accepting new calls |
| F | 40% | 120 s | Four agents stop heartbeating |"""


def report(results, duration, workers, seed):
    lines = [
        "# Progressive dialing simulation",
        "",
        f"Each scenario ran for {duration:,} virtual seconds starting at 08:00 UTC, with {workers} interleaved workers and provider seed {seed}. The clock steps by 100 ms. One worker dispatches every other step in round-robin order. Agents heartbeat on each step until they drop.",
        "",
        scenario_notes(),
        "",
        "Talk utilization counts only connected time. Occupancy includes connected and wrap-up time. Both divide by logged-in agent time. Calls still active at the cutoff contribute their observed time.",
        "",
        "| Scenario | Talk % | Occupancy % | Initiated | Answered | Connected | Unassigned answers | Abandoned on agent loss |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for r in results:
        lines.append(
            f"| {r.name} | {r.talk_pct:.2f} | {r.occupied_pct:.2f} | "
            + " | ".join(map(str, (*r.calls, r.abandoned, r.lost)))
            + " |"
        )
    lines += [
        "",
        "| Scenario | Approve | Reduce | Reject | Most frequent reason codes |",
        "|---|---:|---:|---:|---|",
    ]
    for r in results:
        lines.append(
            f"| {r.name} | {r.actions['APPROVE']} | {r.actions['REDUCE']} | {r.actions['REJECT']} | "
            + ", ".join(f"{k} ({v})" for k, v in r.reasons.most_common(3))
            + " |"
        )
    lines += [
        "",
        "Pacing values are calls per decision, including idle ticks.",
        "",
        "| Scenario | Period | Requested mean [min, max] | Approved mean [min, max] |",
        "|---|---|---:|---:|",
    ]
    for r in results:
        for period, values in zip(
            ("Full shift", "First half", "Second half"), r.pacing
        ):
            a, b, c, d, e, f = values
            if a is not None:
                lines.append(
                    f"| {r.name} | {period} | {a:.4f} [{b}, {c}] | {d:.4f} [{e}, {f}] |"
                )
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=int, default=28800)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--output", type=Path, default=Path("docs/simulation-results.md")
    )
    args = parser.parse_args()
    results = []
    for s in SCENARIOS:
        results.append(simulate(s, args.seconds, args.workers, args.seed))
        print(f"Scenario {s.name} complete", flush=True)
    content = report(results, args.seconds, args.workers, args.seed)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(content, encoding="utf-8")
    print(content)


if __name__ == "__main__":
    main()
