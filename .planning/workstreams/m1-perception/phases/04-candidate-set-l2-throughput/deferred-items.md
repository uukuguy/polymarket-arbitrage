# Phase 04 — Deferred Items

Out-of-scope discoveries surfaced during Phase 04 plan execution. NOT addressed
in this phase. Log entries auto-added by executors per SCOPE BOUNDARY rule.

---

## D-Defer-01: `make smoke-health-local` contract test asserts hardcoded `127.0.0.1:8080`

**Surfaced by:** Plan 02 Task 3 regression check (2026-05-28)
**File:** `tests/m1-perception/test_makefile_contract.py:579`
**Symptom:** Test asserts `"127.0.0.1:8080/health" in result.stdout`, but the
Makefile recipe now reads `PORT=${POLYARB_HTTP_PORT:-19080}` and emits
`127.0.0.1:$PORT/health` (default 19080). Pre-existing before Plan 02 —
confirmed via `git stash` regression check.
**Disposition:** Out of Plan 02 scope (no new code introduced this divergence).
**Fix when ready:** either (a) update assertion to substring-match the
literal `127.0.0.1:$PORT/health` plus require `POLYARB_HTTP_PORT:-19080` default,
or (b) make the Makefile recipe emit a literal port for the dry-run check.
**Suggested owner:** `/gsd-quick` after Phase 04 closure.
