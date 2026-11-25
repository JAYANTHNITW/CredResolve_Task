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

## Scaling beyond the benchmark

| Agent count | First pressure to investigate | Evidence or assumption | Engineering response |
|---|---|---|---|
| 100 | Reservation latency during bursts | All 100 reservations succeeded. Ten latency samples give a coarse p99. | Measure longer runs with representative campaign traffic before changing storage. |
| 1,000 | Contention for SQLite's writer | Eight processes share a writer for reservations, safety audits and events. The measured reservation p99 rose from 171 to 556 ms. | Batch reservations. For more write concurrency, move to PostgreSQL and select candidates with `FOR UPDATE SKIP LOCKED`. |
| 10,000 (projection) | Agent heartbeat writes | One heartbeat per agent every five seconds implies 2,000 updates/s on the agents table. For comparison, a workload of one reservation per agent per minute gives about 167 reservations/s and six events per call gives about 1,000 events/s. | Isolate presence in a narrow table or TTL store, with a freshness check at reservation. Keep assignment changes transactional. |
| Above 10,000 (projection) | Workers scanning the same available agents | More dispatchers can compete for the same ordered candidate set. | Partition by campaign or agent hash and lease each shard to a worker. |
| Any scale | Provider concurrency and calls-per-second limits | Provider quotas can bind before database throughput does. | Preserve per-provider admission checks. Budget traffic across healthy providers and consider a shared token bucket for burst control. |

The heartbeat estimate describes a write-rate scenario. A five-second heartbeat interval would leave no margin for scheduling or network delay with the current five-second expiry threshold. Measure interarrival jitter and choose the interval and expiry together.

### What remains uncertain

- **Provider ambiguity.** Safe recovery depends on the provider's idempotency retention period and when a status lookup becomes authoritative. Test delayed acknowledgements, stale status responses and retry windows against a real provider before treating an unknown call as safe to cancel.
- **Heartbeat expiry.** Short expiry prevents assignments to missing agents sooner, but a delayed heartbeat can disconnect a live borrower. Measure false offline transitions alongside time to stop new dials.

### With another week

Prototype Erlang-style predictive pacing behind the existing Safety Controller with abandonment feedback. Benchmark a PostgreSQL reservation backend against the same race tests, validate placement and reconciliation with a real Plivo adapter, and add property-based event-handler tests over crashes, retries and reordered delivery.
