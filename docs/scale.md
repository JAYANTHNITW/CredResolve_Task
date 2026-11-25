# SQLite load test

Measured on Windows AMD64, Python 3.11.5, SQLite 3.41.2. Both runs used a fresh file-backed WAL database, eight spawned processes and 100,000 synthetic borrowers.

| Agents | Reserved | Reservations/s | Reserve samples | p50 ms | p99 ms | Dispatch busy retries | Events | Events/s | Replay busy retries |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 | 100 | 43.6 | 10 | 133.48 | 171.23 | 0 | 100000 | 555.6 | 0 |
| 1000 | 1000 | 40.6 | 100 | 165.97 | 556.40 | 0 | 100000 | 669.0 | 0 |

Dispatch throughput covers one complete wave after process startup, including safety, reservation, stub placement and initiation. Requests are capped at ten and workers pause 200 ms between ticks. Latencies cover the allocator reservation transaction, including waiting for the writer lock and commit. Percentiles use nearest rank and include empty claims after another worker wins.

Replay applies ringing, answered, connected and completed events to 25,000 calls per scale. Each event has its own transaction. Each wave is timed from the first worker's start to the last worker's finish. Throughput divides the applied event count by the sum of these wave durations. Database seeding, process startup and replenishment between waves are excluded.

SQLite serializes writers. More agents provide more batches to amortize fixed costs, while contending transactions can increase tail latency. The retry columns count explicit application retries after SQLite returns BUSY. Time spent waiting inside the benchmark workers' 60-second busy timeout is already included in elapsed time and latency.

At 1,000 agents, dispatch throughput was 0.93 times the 100-agent result. Reserve p99 moved from 171.23 ms to 556.40 ms. Each replay applied the same number of events. The larger wave achieved 1.20 times the smaller wave's throughput.

A fixed business clock keeps agents fresh and the calling window open for both dispatch runs. Throughput and latency use the monotonic performance counter. These are short saturation waves on one host. The p99 estimate is especially sensitive to sample size at 100 agents.
