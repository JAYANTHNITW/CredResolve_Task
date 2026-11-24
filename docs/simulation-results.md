# Progressive dialing simulation

Each scenario ran for 28,800 virtual seconds starting at 08:00 UTC, with 2 interleaved workers and provider seed 42. The clock steps by 100 ms. One worker dispatches every other step in round-robin order. Agents heartbeat on each step until they drop.

Each campaign has 10 agents, 20 s of ringing per attempt and 30 s of wrap-up. Talk durations are uniformly distributed from half to one-and-a-half times the scenario mean. Answered and connected counts come from the call and agent state transitions observed by the dispatcher.

| Scenario | Answer probability | Mean talk time | Change halfway through shift |
|---|---:|---:|---|
| A | 20% | 120 s | None |
| B | 50% | 90 s | None |
| C | 70% | 180 s | None |
| D | 70% then 10% | 120 s | Answer probability falls |
| E | 40% | 120 s | B starts with 1.5 s extra latency and a 25% timeout rate, then stops accepting new calls |
| F | 40% | 120 s | Four agents stop heartbeating |

Talk utilization counts only connected time. Occupancy includes connected and wrap-up time. Both divide by logged-in agent time. Calls still active at the cutoff contribute their observed time.

| Scenario | Talk % | Occupancy % | Initiated | Answered | Connected | Unassigned answers | Abandoned on agent loss |
|---|---:|---:|---:|---:|---:|---:|---:|
| A | 48.23 | 60.44 | 5636 | 1161 | 1161 | 0 | 0 |
| B | 55.87 | 74.77 | 3598 | 1796 | 1796 | 0 | 0 |
| C | 75.08 | 87.73 | 1734 | 1207 | 1207 | 0 | 0 |
| D | 50.96 | 63.82 | 5154 | 1223 | 1223 | 0 | 0 |
| E | 60.32 | 75.57 | 3485 | 1449 | 1449 | 0 | 0 |
| F | 59.69 | 74.92 | 2858 | 1164 | 1164 | 0 | 3 |

| Scenario | Approve | Reduce | Reject | Most frequent reason codes |
|---|---:|---:|---:|---|
| A | 110105 | 33895 | 0 | WITHIN_LIMITS (110105), CONCURRENCY_CAP (33895) |
| B | 126870 | 17130 | 0 | WITHIN_LIMITS (126870), CONCURRENCY_CAP (17130) |
| C | 111595 | 32405 | 0 | WITHIN_LIMITS (111595), CONCURRENCY_CAP (32405) |
| D | 110282 | 33718 | 0 | WITHIN_LIMITS (110282), CONCURRENCY_CAP (33718) |
| E | 115086 | 28914 | 0 | WITHIN_LIMITS (115086), CONCURRENCY_CAP (23414), PROVIDER_DEGRADED (6792) |
| F | 131999 | 12001 | 0 | WITHIN_LIMITS (131999), CONCURRENCY_CAP (12001) |

Pacing values are calls per decision, including idle ticks.

| Scenario | Period | Requested mean [min, max] | Approved mean [min, max] |
|---|---|---:|---:|
| A | Full shift | 0.0391 [0, 10] | 0.0391 [0, 10] |
| A | First half | 0.0393 [0, 10] | 0.0393 [0, 10] |
| A | Second half | 0.0389 [0, 2] | 0.0389 [0, 2] |
| B | Full shift | 0.0250 [0, 10] | 0.0250 [0, 10] |
| B | First half | 0.0259 [0, 10] | 0.0259 [0, 10] |
| B | Second half | 0.0240 [0, 3] | 0.0240 [0, 3] |
| C | Full shift | 0.0120 [0, 10] | 0.0120 [0, 10] |
| C | First half | 0.0119 [0, 10] | 0.0119 [0, 10] |
| C | Second half | 0.0121 [0, 3] | 0.0121 [0, 3] |
| D | Full shift | 0.0358 [0, 10] | 0.0358 [0, 10] |
| D | First half | 0.0158 [0, 10] | 0.0158 [0, 10] |
| D | Second half | 0.0558 [0, 2] | 0.0558 [0, 2] |
| E | Full shift | 0.0244 [0, 10] | 0.0243 [0, 10] |
| E | First half | 0.0242 [0, 10] | 0.0242 [0, 10] |
| E | Second half | 0.0245 [0, 2] | 0.0244 [0, 2] |
| F | Full shift | 0.0198 [0, 10] | 0.0198 [0, 10] |
| F | First half | 0.0247 [0, 10] | 0.0247 [0, 10] |
| F | Second half | 0.0150 [0, 2] | 0.0150 [0, 2] |
