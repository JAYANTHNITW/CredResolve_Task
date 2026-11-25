"""Measure SQLite WAL contention using the real dispatcher and event handler."""

import argparse
import math
import multiprocessing
import platform
import sqlite3
from concurrent.futures import ProcessPoolExecutor
from contextlib import closing
from datetime import UTC
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter_ns, sleep

from smartdialer import repository as repo
from smartdialer.clock import FakeClock
from smartdialer.db import connect, init_schema
from smartdialer.dispatcher import Dispatcher
from smartdialer.event_handler import Outcome, handle_event
from smartdialer.models import CallState
from smartdialer.pacing import PacingRequest, ProgressivePacing
from smartdialer.providers.base import Accepted, ProviderEvent, ProviderStatus
from smartdialer.router import ProviderRouter
from smartdialer.safety import ProviderLimit

NOW = 12 * 3600_000
WORKERS = 8
BORROWERS = 100_000
EVENTS = 100_000


class InstantProvider:
    name = "instant"

    def __init__(self):
        self.statuses = {}

    def set_event_sink(self, sink):
        # The separate replay phase supplies the synthetic provider events.
        pass

    def get_status(self, call_id):
        return self.statuses.get(call_id, ProviderStatus.UNKNOWN)

    def hangup(self, call_id):
        if call_id in self.statuses:
            self.statuses[call_id] = ProviderStatus.ENDED

    def place_call(self, call_id, to, idempotency_key):
        self.statuses.setdefault(call_id, ProviderStatus.INITIATED)
        return Accepted(call_id)


class BatchPacing(ProgressivePacing):
    """Bound the request so the wave exercises multiple contending transactions."""

    def request(self, snap):
        request = super().request(snap)
        return PacingRequest(min(10, request.requested), request.reasons)


def init_worker(path, barrier):
    global db, ready
    db = connect(path)
    db.execute("PRAGMA busy_timeout=60000")
    ready = barrier


def dispatch_wave(_):
    clock = FakeClock(NOW)
    router = ProviderRouter([InstantProvider()], clock)
    worker = Dispatcher(
        db,
        "load",
        clock,
        router,
        timezone=UTC,
        limits={"instant": ProviderLimit(2000, 2000)},
    )
    worker.pacing = BatchPacing()
    latencies = []
    reserved = 0
    reserve = worker.allocator.reserve

    def timed(permit):
        nonlocal reserved
        start = perf_counter_ns()
        batch = reserve(permit)
        latencies.append((perf_counter_ns() - start) / 1e6)
        reserved += len(batch)
        return batch

    worker.allocator.reserve = timed
    retries = db.busy_retries
    ready.wait(timeout=120)
    start = perf_counter_ns()
    while worker.tick().approved:
        sleep(0.2)
    return start, perf_counter_ns(), reserved, latencies, db.busy_retries - retries


def replay(ids):
    states = (
        CallState.RINGING,
        CallState.ANSWERED,
        CallState.CONNECTED,
        CallState.COMPLETED,
    )
    retries = db.busy_retries
    ready.wait(timeout=120)
    start = perf_counter_ns()
    count = 0
    for cid in ids:
        for i, state in enumerate(states):
            event = ProviderEvent("instant", f"{cid}/{state}", cid, state, NOW + i)
            if handle_event(db, event, NOW + i) != Outcome.APPLIED:
                raise RuntimeError(f"event did not advance {cid}: {state}")
            count += 1
    return start, perf_counter_ns(), count, db.busy_retries - retries


def elapsed(rows):
    return (max(r[1] for r in rows) - min(r[0] for r in rows)) / 1e9


def percentile(sorted_values, p):
    return sorted_values[math.ceil(p * len(sorted_values)) - 1]


def measure(path, agents):
    ctx = multiprocessing.get_context("spawn")
    with closing(connect(path)) as conn:
        init_schema(conn)
        repo.seed_campaign(conn, "load", agents, BORROWERS, NOW)
        with ProcessPoolExecutor(
            WORKERS,
            mp_context=ctx,
            initializer=init_worker,
            initargs=(path, ctx.Barrier(WORKERS)),
        ) as pool:
            rows = list(pool.map(dispatch_wave, range(WORKERS)))
            count = sum(r[2] for r in rows)
            if count != agents:
                raise RuntimeError(f"expected {agents} reservations, got {count}")
            reserve_rate = count / elapsed(rows)
            latencies = sorted(t for r in rows for t in r[3])
            reserve_busy = sum(r[4] for r in rows)
            ids = [
                r[0]
                for r in conn.execute("SELECT id FROM calls WHERE state='INITIATED'")
            ]
            if len(ids) != agents:
                raise RuntimeError("dispatch wave did not initiate every reservation")
            total = busy = 0
            seconds = 0
            while total < EVENTS:
                if not ids:
                    raise RuntimeError("replay exhausted its borrowers")
                ids = ids[: (EVENTS - total) // 4]
                rows = list(pool.map(replay, [ids[i::WORKERS] for i in range(WORKERS)]))
                total += sum(r[2] for r in rows)
                busy += sum(r[3] for r in rows)
                seconds += elapsed(rows)
                if total < EVENTS:
                    # Fixture reset between measured waves: reuse the same agents and
                    # claim fresh borrowers. Excluded from handler-only throughput.
                    changed = conn.execute(
                        """UPDATE agents SET state='AVAILABLE',available_since=?,
                        wrap_up_until=NULL,version=version+1 WHERE state='WRAP_UP' AND current_call_id IS NULL""",
                        (NOW,),
                    ).rowcount
                    if changed != len(ids):
                        raise RuntimeError("replay failed to release all agents")
                    ids = [
                        r.call_id
                        for r in repo.reserve_batch(
                            conn, "load", agents, NOW, 10000, 5000
                        )
                    ]
            if (
                total != EVENTS
                or conn.execute(
                    "SELECT COUNT(*) FROM calls WHERE state='COMPLETED'"
                ).fetchone()[0]
                != EVENTS // 4
            ):
                raise RuntimeError("unexpected event or completion count")
            return (
                agents,
                count,
                reserve_rate,
                len(latencies),
                percentile(latencies, 0.5),
                percentile(latencies, 0.99),
                reserve_busy,
                total,
                total / seconds,
                busy,
            )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("docs/scale.md"))
    args = parser.parse_args()
    lines = [
        "# SQLite load test",
        "",
        f"Measured on {platform.system()} {platform.machine()}, Python {platform.python_version()}, SQLite {sqlite3.sqlite_version}. Both runs used a fresh file-backed WAL database, eight spawned processes and 100,000 synthetic borrowers.",
        "",
        "| Agents | Reserved | Reservations/s | Reserve samples | p50 ms | p99 ms | Dispatch busy retries | Events | Events/s | Replay busy retries |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    measurements = []
    with TemporaryDirectory() as directory:
        for agents in (100, 1000):
            row = measure(Path(directory) / f"{agents}.db", agents)
            measurements.append(row)
            line = "| {} | {} | {:.1f} | {} | {:.2f} | {:.2f} | {} | {} | {:.1f} | {} |".format(
                *row
            )
            lines.append(line)
            print(line, flush=True)
    lines += [
        "",
        "Dispatch throughput covers one complete wave after process startup, including safety, reservation, stub placement and initiation. Requests are capped at ten and workers pause 200 ms between ticks. Latencies cover the allocator reservation transaction, including waiting for the writer lock and commit. Percentiles use nearest rank and include empty claims after another worker wins.",
        "",
        "Replay applies ringing, answered, connected and completed events to 25,000 calls per scale. Each event has its own transaction. Each wave is timed from the first worker's start to the last worker's finish. Throughput divides the applied event count by the sum of these wave durations. Database seeding, process startup and replenishment between waves are excluded.",
        "",
        "SQLite serializes writers. More agents provide more batches to amortize fixed costs, while contending transactions can increase tail latency. The retry columns count explicit application retries after SQLite returns BUSY. Time spent waiting inside the benchmark workers' 60-second busy timeout is already included in elapsed time and latency.",
    ]
    small, large = measurements
    lines.extend(
        [
            "",
            f"At 1,000 agents, dispatch throughput was {large[2] / small[2]:.2f} times the 100-agent result. Reserve p99 moved from {small[5]:.2f} ms to {large[5]:.2f} ms. Each replay applied the same number of events. The larger wave achieved {large[8] / small[8]:.2f} times the smaller wave's throughput.",
            "",
            "A fixed business clock keeps agents fresh and the calling window open for both dispatch runs. Throughput and latency use the monotonic performance counter. These are short saturation waves on one host. The p99 estimate is especially sensitive to sample size at 100 agents.",
        ]
    )
    report = "\n".join(lines) + "\n"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    # Refresh measurements while retaining the maintained capacity-planning notes.
    if args.output.is_file():
        heading = "\n## Scaling beyond the benchmark\n"
        _, marker, notes = args.output.read_text(encoding="utf-8").partition(heading)
        if marker:
            report += marker + notes
    args.output.write_text(report, encoding="utf-8")
    print(report)


if __name__ == "__main__":
    main()
