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

## GAP-200 (Plan 07, 2026-05-27) — mirror-disabled-by-config is silent

- **Discovered during**: Inj L2-2 re-run (Plan 07 Task 2)
- **Issue**: when POLYARB_SUPABASE_SERVICE_KEY is unset, daemon disables mirror entirely;
  /health drops the `mirror:l2_tob_age_seconds` sub-check from `.checks` entirely (not just
  stale-and-failing). Operator unsetting key by accident → silent loss of mirror writes,
  only visible in container startup log line `l2-mirror disabled (POLYARB_SUPABASE_URL or _SERVICE_KEY missing)`.
- **Phase 03's Inj L2-2 lesson**: chain-truth must surface for fail-soft paths. The mirror-disabled
  path is a different shape of fail-soft (config-disable instead of runtime-fail).
- **Possible fix (deferred to Phase 04 or m1-perception backlog)**:
  When `settings.supabase_url` is set but `service_key` is empty, /health should still register
  `mirror:l2_tob_age_seconds` with `status=fail` and `output="mirror disabled by config (service_key empty)"`.
  Turns config-disable into a chain-truth signal too.
- **Workaround**: Polywatch healthz-watcher already alarms on /health overall != pass; mirror-disable
  doesn't surface there because overall stays at warn (from ws state). Out-of-band detection
  requires log scraping — acceptable for now since this state requires deliberate operator action.

## GAP-201 (Plan 07, 2026-05-27) — Fly secret quoting trap in cleanup commands

**STATUS: ✅ RESOLVED 2026-05-28** (SESSION 31, m1-perception backlog clean-up)

- **Discovered during**: Inj L2-2 re-run (Plan 07 Task 2 recovery)
- **Issue**: `.env` values wrapped in single quotes (`KEY='value'`); naïve `grep + sed` extraction
  can leave quote chars in the value, leading to off-by-2-byte values that authenticate as a
  different key (or fail 401). Subtle because `flyctl secrets set` happily accepts the bad value.
- **Symptom**: 401 Unauthorized errors despite "key restored" cleanup; long debug to identify
  219 vs 221 byte mismatch.
- **Fix shipped**: audited all chaos-l2-* targets in Makefile (lines 681-888). All shell-native
  `set -a; . ./.env; set +a` blocks confirmed to use shell-native parsing (no grep+sed survives).
  Found three lines in chaos-l2-inj4 (872, 877, 886) that were missing the
  `unset FLY_API_TOKEN` invariant per Makefile:670-679 — added. Now all 9 chaos-l2-* env-sourcing
  blocks (689, 715, 731, 749, 760, 770, 872, 877, 886) consistently include the unset.
- **Resolution commit**: (see git log — fix(chaos): GAP-201)

## GAP-202 (Plan 07, 2026-05-27) — /scan endpoint 500 on NaN values

**STATUS: ✅ RESOLVED 2026-05-28** (SESSION 31, m1-perception backlog clean-up)

- **Discovered during**: Inj L2-3b (Plan 07 Task 3) attempt to manually fire snapshot via /scan
- **Issue**: `curl -X POST -d '{"recipe_name":"near-end"}' /scan` returns 500 with traceback:
  `ValueError: Out of range float values are not JSON compliant: nan` in `JSONResponse` render.
- **Root cause (confirmed)**: Starlette's `JSONResponse` renders with `json.dumps(allow_nan=False)`
  per RFC 8259 strict mode. Recipe outputs (e.g. spread, mid_price) can be NaN/Inf when bid/ask
  are missing or equal.
- **Fix shipped**: added `_sanitize_for_json` helper in `src/polyarb/http/scan.py` that walks the
  response dict and replaces NaN/+Inf/-Inf leaf floats with None. JSON null is the canonical
  representation and matches what jq / pandas consumers already expect. Applied before JSONResponse
  render; only floats in leaf positions are inspected (cheap on row counts capped at 100).
- **Regression test**: `tests/m1-perception/test_http_scan.py::test_nan_in_rows_renders_as_null`
  mocks run_recipe to return NaN/+Inf/-Inf rows, asserts 200 + null serialization.
- **Resolution commit**: (see git log — fix(http): GAP-202)
