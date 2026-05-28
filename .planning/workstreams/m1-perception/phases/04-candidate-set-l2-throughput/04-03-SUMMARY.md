---
phase: 04-candidate-set-l2-throughput
plan: 03
subsystem: observability
tags: [health-endpoint, ietf-health, chain-truth, supabase, l2-mirror, GAP-200]

requires:
  - phase: 03.1-supabase-mirror
    provides: existing Check 4 mirror gate body (l2_health.py:181-216 — case-c logic preserved verbatim)
  - phase: 03.1-supabase-mirror
    provides: L4 lesson "fail-soft must surface to /health" (feedback_code-vs-chain-truth-2026-05)
provides:
  - "Three-branch /health mirror gate: case (a) skip / case (b) fail / case (c) existing"
  - "Operator config mistake (POLYARB_SUPABASE_URL set but POLYARB_SUPABASE_SERVICE_KEY empty) now surfaces as /health status=fail (HTTP 503) instead of silent absence"
  - "GAP-200 carry-over from Phase 03.1 closed (originally folded into Phase 04 D-08 per SESSION 30 ROADMAP refactor)"
affects:
  - 04-04 (Inj L2-4 chaos — case-b is now a curl-observable failure mode any future chaos primitive can target)
  - any future L2 deploy (Fly machine env-misconfiguration is now alarm-visible, not silent)

tech-stack:
  added: []
  patterns:
    - "Three-branch chain-truth gate (replaces binary if-flag): observes the SAME data the flag derives from + adds the misconfiguration case as an explicit branch"
    - "SecretStr.get_secret_value() inside try/except AttributeError (defensive — tests may pass mock settings)"

key-files:
  created:
    - tests/http/__init__.py
    - tests/http/test_l2_health_gap200.py
  modified:
    - src/polyarb/http/l2_health.py

key-decisions:
  - "No config.py change — l2_mirror_enabled stays False in case (b); only /health PRESENTATION changes. Aligned with RESEARCH Q6 + PATTERNS Plan 03 row."
  - "Output string 'mirror disabled by config (service_key empty)' names the missing FIELD but never the value or url (T-04-04 information-disclosure threat accepted)."
  - "case (b) trips overall=fail → HTTP 503 on /health. Fly probe uses /healthz (always 200) so no probe-driven restart loop (T-04-06 accepted)."
  - "Single-commit TDD (RED verified in-session then bundled with GREEN) — plan's Task 1 acceptance_criteria checks final state only; no separate RED commit gain since the test file is meaningful only after the implementation lands."

patterns-established:
  - "Three-branch chain-truth pattern: when an internal flag is computed from N inputs, the /health surface should branch on the RAW INPUTS (so observability sees the same shape config sees), not just the derived flag. Reusable for any future fail-soft surface."

requirements-completed: []

duration: 11min
completed: 2026-05-28
---

# Phase 04 Plan 03: GAP-200 /health mirror three-branch gate Summary

**`/health` now surfaces operator config mistake (URL set, service_key empty) as status=fail instead of silent absence — inverse of Phase 03.1 L4 chain-truth lesson.**

## Performance

- **Duration:** ~11 min
- **Started:** 2026-05-28T05:24:00Z (approx — first file write)
- **Completed:** 2026-05-28T05:35:00Z
- **Tasks:** 1
- **Files modified:** 3 (1 src + 2 test)

## Accomplishments

- Three-branch mirror gate at `src/polyarb/http/l2_health.py:174-256`:
  - (a) `supabase_url == ""` → no sub-check (Supabase opt-out — backwards-compat preserved)
  - (b) `supabase_url` set AND `service_key` empty → `mirror:l2_tob_age_seconds` registered with `status=fail`, `output="mirror disabled by config (service_key empty)"`, `overall=fail`
  - (c) both set → existing pass/warn/fail age sub-check **body unchanged** (lines 211-250 — moved verbatim under `elif`)
- New test file `tests/http/test_l2_health_gap200.py` with 3 tests covering all three branches; full 9-test `tests/m1-perception/test_l2_health_mirror_check.py` regression suite still passes
- GAP-200 SESSION 29 carry-over closed (originally fold-in target of Phase 04 D-08 per SESSION 30 planning)

## Task Commits

1. **Task 1: GAP-200 three-branch mirror gate (tests + impl bundled)** — `<commit-pending>` (`feat(04-03):`)

**Plan metadata commit:** to follow as the docs commit including this SUMMARY.

## Files Created/Modified

- `src/polyarb/http/l2_health.py` — Check 4 mirror gate replaced with three-branch logic (lines 174-256). Reads `settings.supabase_url` + `settings.supabase_service_key.get_secret_value()` directly (defensive try/except for non-SecretStr mocks). Case-(c) body lines 211-250 are byte-identical to the original 181-216 — only moved under `elif`.
- `tests/http/__init__.py` — empty (pyproject `testpaths=["tests"]` package-mode marker; without it pytest collection across sibling tests/ packages would import-mangle)
- `tests/http/test_l2_health_gap200.py` — 3 tests: `test_both_empty_no_subcheck` (case a), `test_url_set_key_empty_registers_fail` (case b — GAP-200 core), `test_both_set_existing_behavior` (case c parity guard with cold-start path)

## Decisions Made

1. **Single commit for TDD task** — plan's `<action>` describes RED→GREEN as a single Task 1; acceptance_criteria check final state only. RED was validated in-session (1 failed, 2 passed — the case-b assertion failed as expected because the silent-absence bug GAP-200 was still present), GREEN re-run is 12/12 PASS. A separate RED commit would commit a file that's only useful after the impl ships in the same PR — no informational gain.
2. **Defensive SecretStr access** — `try/except AttributeError` around `.get_secret_value()` so tests that monkey-patch settings with a plain str for `supabase_service_key` don't AttributeError; production path always sees the real SecretStr from pydantic Settings.
3. **No config.py change** — confirmed against RESEARCH Q6: D-08 fix is presentation-only. `l2_mirror_enabled` remains False in case (b) because `model_validator` requires both url AND key (config.py:238). Inverting that would have side-effects on `supabase_mirror_enabled` and the L1 dashboard mirror.

## must_haves.truths verified (file:line evidence)

| Truth | Evidence |
|---|---|
| "When supabase_url is set but service_key is empty, /health registers mirror:l2_tob_age_seconds with status=fail" | `src/polyarb/http/l2_health.py:196-210` (the `if _supabase_url and not _service_key_val` branch) + `tests/http/test_l2_health_gap200.py:103-138` (test_url_set_key_empty_registers_fail — assertions on status=='fail' AND "service_key empty" in output AND overall=='fail') |
| "When supabase_url is also empty, /health has NO mirror sub-check" | `src/polyarb/http/l2_health.py:253-256` (else-branch comment — no checks dict mutation) + `tests/http/test_l2_health_gap200.py:74-91` (test_both_empty_no_subcheck — `"mirror:l2_tob_age_seconds" not in checks`) |
| "When both url+key are set, /health registers the existing pass/warn/fail age sub-check unchanged" | `src/polyarb/http/l2_health.py:211-250` (case-c elif — body byte-identical to original; only moved under elif from top-level if) + 9-test regression suite `tests/m1-perception/test_l2_health_mirror_check.py` all PASS |
| "overall health becomes fail when the case-(b) config-mistake sub-check fires" | `src/polyarb/http/l2_health.py:210` (`overall = _severity(overall, "fail")` inside case-b branch) + `tests/http/test_l2_health_gap200.py:135-138` (test asserts `overall == "fail"`) |

## Test Evidence

```
$ uv run pytest tests/http/test_l2_health_gap200.py tests/m1-perception/test_l2_health_mirror_check.py -v
tests/http/test_l2_health_gap200.py::test_both_empty_no_subcheck PASSED
tests/http/test_l2_health_gap200.py::test_url_set_key_empty_registers_fail PASSED
tests/http/test_l2_health_gap200.py::test_both_set_existing_behavior PASSED
tests/m1-perception/test_l2_health_mirror_check.py::test_settings_l2_mirror_enabled_auto_detect_when_secrets_present PASSED
tests/m1-perception/test_l2_health_mirror_check.py::test_settings_l2_mirror_disabled_when_secrets_missing PASSED
tests/m1-perception/test_l2_health_mirror_check.py::test_settings_l2_tob_age_defaults_match_plan PASSED
tests/m1-perception/test_l2_health_mirror_check.py::test_settings_l2_tob_age_env_override PASSED
tests/m1-perception/test_l2_health_mirror_check.py::test_health_mirror_warn_on_cold_start PASSED
tests/m1-perception/test_l2_health_mirror_check.py::test_health_mirror_pass_when_fresh PASSED
tests/m1-perception/test_l2_health_mirror_check.py::test_health_mirror_fail_when_stale PASSED
tests/m1-perception/test_l2_health_mirror_check.py::test_health_mirror_absent_when_disabled PASSED
tests/m1-perception/test_l2_health_mirror_check.py::test_health_mirror_warn_on_borderline PASSED
======================== 12 passed in 0.04s =========================
```

**New tests: 3/3 PASS. Regression suite (case-c): 9/9 PASS. Total: 12/12.**

## RED → GREEN trace

Before impl (RED stage in-session):
```
FAILED tests/http/test_l2_health_gap200.py::test_url_set_key_empty_registers_fail
  AssertionError: case (b): config-mistake must be surfaced as a /health sub-check
  assert 'mirror:l2_tob_age_seconds' in {'event_bus:listener_state': [...], 'ws:connection_state': [...]}
```
After impl (GREEN):
```
PASSED tests/http/test_l2_health_gap200.py::test_url_set_key_empty_registers_fail
```

The other two tests (case-a and case-c parity) passed at RED — case-a was already correct behavior (silent skip) which test_both_empty_no_subcheck asserts, and case-c was the unchanged existing path. Only case-b was the bug.

## Inversion rationale — Phase 03.1 L4 lesson

This plan is the explicit inverse of `feedback_code-vs-chain-truth-2026-05` (Phase 03.1 SESSION 29 L4):

> "fail-soft envelope 代码层完美 (`try/except + log + breadcrumb`) 但 `/health` 子检查 gate 在不存在的 config 字段 → mirror 失败 5 天静默才被 chaos 发现"

That bug was: the WRITE side fail-soft swallowed errors cleanly, but the OBSERVE side (`/health` mirror gate) couldn't see the disabled state because it gated on a flag that didn't track the right input.

GAP-200 is the symmetric mirror image: the gate gates on `l2_mirror_enabled` which is derived from `url AND key`. When the operator sets url but forgets key, both fail-soft AND the observability surface go silent — the bug doesn't get surfaced anywhere visible. Fix: gate on the RAW inputs (`url`, `key`) directly, not the derived flag, so observability mirrors the same shape config sees.

Pattern applies to any future fail-soft surface where the gate flag aggregates multiple inputs — gate the /health sub-check on the inputs, not the flag.

## Deviations from Plan

None — plan executed exactly as written. Task 1 single TDD task, both tests + impl land in one commit per plan `<action>` step descriptions. acceptance_criteria all met without modification.

## Issues Encountered

None.

## User Setup Required

None — D-08 is a pure presentation change. No new env vars, no Fly secret changes, no migrations. The change becomes visible on the next L2 deploy.

## Self-Check

- `src/polyarb/http/l2_health.py` modified — exists, three-branch logic at lines 174-256 verified by `grep -c "service_key empty"` = 1 and `grep -c "mirror disabled by config"` = 1
- `tests/http/__init__.py` created — exists (empty file, 0 bytes)
- `tests/http/test_l2_health_gap200.py` created — exists (3 tests, all PASS)
- `uv run pytest tests/http/test_l2_health_gap200.py tests/m1-perception/test_l2_health_mirror_check.py -x` exits 0 — verified (12 passed)
- Commit hash placeholder `<commit-pending>` will be replaced by post-commit edit (informational only — git log is the source of truth)

**Self-Check: PASSED**

## Next Phase Readiness

- /health case-(b) is now a deterministic curl-observable fail state. Any future chaos primitive that wants to test "what happens if Supabase key is rotated away" has a unit-level + endpoint-level evidence path.
- Plan 04 (Inj L2-4 throughput chaos in Wave 3) does NOT depend on this plan — they're orthogonal. Wave 1 04-01 (Supabase fetch + temp-DB candidate refresh) is also orthogonal.
- Phase 04 progress: 1/4 plans complete (Plan 03 Wave 1 done; Plan 01 Wave 1 in flight or queued).

---
*Phase: 04-candidate-set-l2-throughput*
*Workstream: m1-perception*
*Completed: 2026-05-28*
