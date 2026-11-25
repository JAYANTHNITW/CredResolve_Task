# Architecture decisions

| Decision | Why | What it solves | Cost | Alternative considered |
|---|---|---|---|---|
| 1. Keep operational state and the audit trail in SQLite WAL. | A transaction can commit a call, its assignments and its event together. | Workers agree on ownership after restart. | One writer limits throughput across campaigns. | PostgreSQL for concurrent writers. A separate cache or queue would also need a consistency protocol. |
| 2. Guard updates and enforce partial unique indexes on active calls. | Expected state, version or assignment ID identifies which transition may win. The indexes independently enforce exclusivity for agents and borrowers. | Two workers cannot book one agent twice. A late release cannot free a newer assignment. | Each caller checks affected rows and handles a lost claim. | Rely on the SQLite writer lock alone. |
| 3. Run workers without a leader and give reservations a lease. | Any worker with the database and provider access can reconcile a crashed worker's calls. | Expired reservations, missing completions and stale presence have a recovery path. | Recovery is delayed by deadlines and requires an authoritative provider status service. | A coordinator with exclusive campaign ownership and explicit leader failover. |
| 4. Rank call states and apply missing forward steps in order. | Events may be duplicated, reordered or delivered without IDs. | A late completion can still settle the agent and borrower exactly once. Provider/event IDs accelerate duplicate detection. | An implied transition receives processing time, so some intermediate timestamps are estimates. | Require an ordered, exactly-once provider stream. |
| 5. Treat placement timeouts as ambiguous. | A provider can accept a call and lose its acknowledgement. | Same-provider, same-key retries avoid a second dial. A definite rejection permits failover. | The agent remains reserved while a persistent ambiguity is reconciled. | Fail over immediately on timeout, risking two live calls. |
| 6. Only the Safety Controller issues a single-use permit. | Pacing can request volume without authority to place a call. | The allocator consumes a persisted grant and rechecks shared capacity before placement. A reason-coded audit explains approvals and reductions. | Extra database writes and short-lived grants can cost capacity under contention. | Have each pacing strategy implement its own limits. |
| 7. Inject the clock and schedule mock provider events in virtual time. | Lease expiry, breaker cooldown and callback ordering must be reproducible in tests. | A test can advance directly to the failure boundary. The simulator can replay an entire shift. | Callers must pass time consistently. Real concurrency and throughput still need separate process tests. | Sleep through scenarios using wall-clock timers. |

## Atomic reservation

SQLite serializes writers when a worker starts a batch with `BEGIN IMMEDIATE`. Each agent claim also checks its expected state and current assignment in a single conditional update. The affected-row count identifies the winner. Partial unique indexes on active calls enforce exclusivity for both agents and borrowers. A savepoint lets a failed pair release its claims while other pairs in the batch commit.

The writer lock limits throughput across campaigns. On PostgreSQL, workers could select candidates with `FOR UPDATE SKIP LOCKED` while retaining the conditional updates and unique indexes. The single-agent thread test exercises the conditional update directly in autocommit mode.

## How could predictive pacing improve utilization safely?

Progressive dialing claims an agent before placing a call. That bounds simultaneous assignments, even when the answer-rate estimate is wrong. Agent loss after connection remains possible and is counted explicitly.

A future predictive strategy could request permission to dial ahead. The Safety Controller would own a hard cap on calls beyond free agents and hold some agents in reserve for simultaneous answers. It would track the live abandonment rate, tighten the ratio toward one dial per free agent as abandonment rises, and fall back to progressive mode when provider health or estimate quality degrades. The cap bounds exposure even during the delay before the feedback loop reacts.
