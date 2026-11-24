# Architecture decisions

## Atomic reservation

SQLite serializes writers when a worker starts a batch with `BEGIN IMMEDIATE`. Each agent claim also checks its expected state and current assignment in a single conditional update. The affected-row count identifies the winner. Partial unique indexes on active calls enforce exclusivity for both agents and borrowers. A savepoint lets a failed pair release its claims while other pairs in the batch commit.

The writer lock limits throughput across campaigns. On PostgreSQL, workers could select candidates with `FOR UPDATE SKIP LOCKED` while retaining the conditional updates and unique indexes. The single-agent thread test exercises the conditional update directly in autocommit mode.
