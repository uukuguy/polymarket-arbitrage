# Deferred items — Phase 02 (m1-perception)

Items discovered during Plan 02-09 execution but out of scope.

## test_make_smoke_health_local_dry_run_recipe (pre-existing)

- **Found during:** Plan 02-09 T5 (running full m1-perception suite).
- **Symptom:** Test asserts recipe contains literal `127.0.0.1:8080/health`,
  but the Makefile recipe builds the URL dynamically from
  `POLYARB_HTTP_PORT=${POLYARB_HTTP_PORT:-19080}`.
- **Pre-existing:** failure is reproducible on `main` branch before Plan 02-09
  changes (verified via `git stash` round-trip).
- **Why deferred:** unrelated to Plan 02-09 scope; the assertion should be
  updated to grep for `:$PORT/health` or `:19080/health` (the new default
  per `feedback_port-numbers-2026-05`). Out of scope for this plan.
