# Runtime Watchdog Dashboard Ledger — Task 1 Summary

Implemented the first safe delivery boundary: `run_watchdog_service` now calls
an optional transition persister before Telegram for both incident and recovery
state changes.  The runtime CLI posts only a redacted event envelope to a
private bearer-authenticated writer endpoint and emits a separate Telegram
warning if that ledger is unavailable, preserving watchdog operation.

Added `runtime_event_writer.py`, a private Starlette writer that accepts only
bounded failure codes and appends idempotent detected/recovered events to the
existing M1 incident ledger.  It is not yet deployed; the next task adds its
contract tests, API projection, Fly template/role, and the Next dashboard page.

Verification: `uv run pytest tests/m1-perception/test_control_plane_watchdog.py -q`
and `uv run ruff check src/polyarb/control_plane/runtime_event_writer.py
src/polyarb/cli_control_plane.py` passed.
