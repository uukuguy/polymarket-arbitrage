# Phase 03 Deferred Items

Items discovered during Phase 03 execution that are OUT OF SCOPE for the
current plan and need to be addressed separately.

## Pre-existing m1-perception test failures (NOT caused by Plan 03-04)

Discovered during Plan 03-04 regression sweep (2026-05-24).

### 1. `tests/m1-perception/test_health_endpoint.py::test_pass_when_fresh`
- **Assertion**: `body["status"] == "pass"` got `"warn"`
- **Likely cause**: Phase 02.1 D-05 strict IETF semantics may have shifted
  thresholds; not related to L2 daemon work.
- **Action**: Defer to Phase 02 fix-up backlog.

### 2. `tests/m1-perception/test_makefile_contract.py::test_make_smoke_health_local_dry_run_recipe`
- **Assertion**: expects literal `127.0.0.1:8080/health` in recipe
- **Reality**: Makefile uses `127.0.0.1:$PORT` where `PORT=${POLYARB_HTTP_PORT:-19080}` (port discipline 19080 default).
- **Action**: Update test to match port discipline (feedback_port-numbers-2026-05).

### 3. `tests/m1-perception/test_r2_sync.py::test_r2_retry_config_applied`
- Pre-existing; cause not investigated.
- **Action**: Defer to a maintenance plan.

## Notes
None of the above failures involve modules touched by Plan 03-04
(ws_market_client.py, ws_watchdog.py, ws_consumer.py, l2_main.py).
The Plan 03-04 + Plan 03-03 test suites (18 tests) pass cleanly.
