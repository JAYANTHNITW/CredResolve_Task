# SmartDialer

SmartDialer is a progressive dialer that reserves one available agent before placing each call. The project targets Python 3.11 and SQLite in WAL mode, with standard-library runtime code. The implementation sequence is in [plan.md](plan.md).

## Ground rules

1. Put correctness first. A double-booked agent is worse than a missing dashboard.
2. Make every state change a conditional write such as `UPDATE ... WHERE state = :expected` and check `rowcount`. Always guard a write that follows a read.
3. Guard agent updates with `current_call_id` as well. A late or stale release must leave an agent's newer call assignment intact.
4. Keep provider calls outside database transactions. Reserve, commit, then call the provider.
5. Move state forward only. Duplicate, stale, and reordered events that would move state backward are no-ops.
6. Get all business-logic time from an injected `Clock` so the simulator and crash tests are deterministic.
7. Pacing can request capacity. Only the Safety Controller can issue a `Permit`, and only the allocator can reserve and place calls with one.
8. Fail closed. Reject a request whenever the Safety Controller cannot establish that it is safe.
9. Document the problem a new technology solves before adding it.

## Development setup

From the repository root on Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e '.[dev]'
```

On macOS or Linux:

```sh
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
```

Run `make test` or `python -m pytest -q`. A Windows MinGW installation can use `mingw32-make test`. Phase 0 has an empty test suite, so pytest reports zero collected tests and exits with status 5.

The Makefile also defines `sim`, `load`, and `demo` for the simulator, load test, and scenario tests scheduled in [plan.md](plan.md).
