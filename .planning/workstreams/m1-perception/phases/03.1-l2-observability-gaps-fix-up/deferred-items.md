# Deferred Items — Phase 03.1

## D-DEFER-1 (Plan 02, 2026-05-26) — pytest-asyncio plugin not loading

**Symptom**: `tests/m1-perception/test_orchestrator.py` async tests skip with
"async def functions are not natively supported". 26 tests collected as FAILED.

**Scope**: NOT introduced by Plan 02. Existed before this plan; verified by
checking `tests/m1-perception/test_alerts.py` and others that all warn
`PytestUnknownMarkWarning: Unknown pytest.mark.asyncio`.

**Probable cause**: `pyproject.toml` has `asyncio_mode` but the plugin isn't
registered or installed. `uv sync --extra dev` may be missing
`pytest-asyncio` from the dev extras.

**Why not fixed here**: Out of scope for Plan 02 (chain-truth wiring +
notes derivation). Would need a separate `chore(deps)` plan to add the
plugin to pyproject.toml dev extras and re-pin lockfile.

**Verified non-impact on Plan 02**: targeted tests for Plan 02 deliverables
all run synchronously and pass 31/31:
- tests/m1-perception/test_l2_health_mirror_check.py (9/9)
- tests/m1-perception/test_orchestrator_notes_write.py (5/5)
- tests/m1-perception/test_sqlite_store_l2_getters.py (Plan 01 regression — 9/9)
- tests/m1-perception/test_l2_supabase_mirror_persist.py (Plan 01 regression — 4/4)
- tests/m1-perception/test_l2_health_endpoint.py (4/4)

## D-DEFER-2 (Plan 02, 2026-05-26) — psutil missing for memory budget tests

`tests/m1-perception/test_streaming_memory_budget.py` +
`tests/m1-perception/test_streaming_memory_calibration.py` fail at collection
with `ModuleNotFoundError: No module named 'psutil'`. Same fix vector as
D-DEFER-1 (uv sync dev extras gap). Out of scope here.

## D-DEFER-3 (Plan 06, 2026-05-26) — Makefile contract test port mismatch

- **test_makefile_contract::test_make_smoke_health_local_dry_run_recipe** (FAIL, pre-existing): asserts `127.0.0.1:8080/health` but recipe uses `$POLYARB_HTTP_PORT:-19080`. Discovered during Plan 06 execution 2026-05-26. Out of Plan 06 scope — fix in a separate Makefile-contract sync plan.
