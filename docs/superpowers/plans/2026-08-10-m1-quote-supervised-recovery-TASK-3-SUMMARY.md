# M1 Quote Supervised Recovery — Task 3 Summary

## Delivered

- Added `quote` to the shell-free `ProducerSupervisor` command authority.
- Added an isolated Quote CLI owner.  It claims child heartbeat authority,
  emits a durable progress heartbeat at the start of every collection cycle,
  and never builds the unrelated upstream-fault runtime.
- The isolated Quote worker exits with code `75` after its configured first
  consecutive hard child timeout.  The outer supervisor records the terminal
  receipt, starts bounded recovery, and escalates after its existing restart
  budget; there is no inner infinite retry loop.
- Added Quote to the isolated daemon topology.  In isolated mode this is the
  sole collection owner; the HTTP parent only hydrates the certified feed.

## Verification

`uv run pytest tests/perception/test_supervisor.py tests/daemon/test_quote_worker.py -q`

Result: `89 passed`.

`uv run ruff check src/polyarb/perception/worker_cli.py src/polyarb/perception/supervisor.py src/polyarb/daemon/main.py src/polyarb/daemon/quote_worker.py tests/perception/test_supervisor.py tests/daemon/test_quote_worker.py`

Result: all checks passed.

## Operational effect

A hard Quote timeout is still immediately recorded by the Quote incident
lifecycle.  It now also terminates the affected outer child deterministically,
so the durable producer receipt/incident chain proves the restart decision and
can reach an explicit escalation instead of silently retrying forever.
