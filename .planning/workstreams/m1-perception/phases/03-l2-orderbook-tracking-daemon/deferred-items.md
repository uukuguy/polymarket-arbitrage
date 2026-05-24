# Deferred Items — Phase 03

## Pre-existing test failures discovered during Plan 03-03 (NOT caused by Plan 03-03 work)

1. `tests/m1-perception/test_health_endpoint.py::test_pass_when_fresh`
   - Expects body["status"]=="pass" but R2 sub-check returns "warn" (no R2 URL on test snapshot)
   - Confirmed pre-existing via `git stash && uv run pytest ...` on main
   - Out of scope for Plan 03-03 (touches L1 health logic, not L2)

2. `tests/m1-perception/test_makefile_contract.py::test_make_smoke_health_local_dry_run_recipe`
   - Test expects literal `127.0.0.1:8080/health` in `make -n smoke-health-local` output
   - Actual Makefile uses `$PORT` variable defaulting to 19080 per L1 port lock
   - Test is stale relative to Makefile; not Plan 03-03 work
