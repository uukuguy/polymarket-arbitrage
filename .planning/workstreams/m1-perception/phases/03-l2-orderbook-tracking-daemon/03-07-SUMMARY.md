---
phase: 03
plan: 07
type: execute
wave: 6
status: complete (3/5 live PASS + 2/5 deferred to Phase 03.1 with substitute evidence)
subsystem: chaos-verification
tags: [chaos-testing, fly-machine-restart, sentry-api, psql-supabase, container-localhost-fallback]
requires_satisfied: [D-09, Phase-02-L6, Phase-02-L7, Phase-02.1-L8]
provides_delivered:
  - tests/chaos/__init__.py + tests/chaos/test_l2_chaos_plan.py (354 lines, 17 invariant tests GREEN)
  - 03-SOAK-LOG.md (timestamped Inj segments + verdicts + GAP analysis)
  - Makefile chaos-l2-baseline / chaos-l2-inj1 / chaos-l2-inj2 / chaos-l2-inj3a / chaos-l2-inj3b / chaos-l2-cleanup targets
affects_landed:
  - polyarb-l2 daemon stability (3 live chaos cycles in ~10 min total, daemon survived all)
  - Phase 03.1 backlog populated: 5 GAPs (from L2-2) + 3 deferred Inj (L2-3b/L2-4/L2-5)
duration_actual: ~75 min (45 min plan-skeleton + tests + Makefile, 30 min live chaos + SOAK-LOG)
---

# Plan 03-07 SUMMARY — Phase 03 Wave 6 Chaos

## Goal vs Delivery

| Goal (frontmatter) | Delivered | Status |
|---|---|---|
| 5 chaos injections run live in prod | 3 ran live (L2-1, L2-2, L2-3a); 2 deferred to Phase 03.1 with substitute evidence (L2-3b, L2-4); 1 truly deferred to first-use (L2-5) | partial — see SOAK-LOG verdicts |
| declarative L2_CHAOS_PLAN with invariants | ✅ 354 lines, 17 tests GREEN | ✅ |
| programmatic verification (D-09) | ✅ every Inj — no UI navigation | ✅ |
| container-localhost fallback (L8) | ✅ test-asserted on every Inj | ✅ |
| 03-SOAK-LOG.md ≥5 segments + timestamps + verdicts | ✅ 6 segments (5 Inj + 1 summary) + GAP analysis | ✅ |
| Cleanup procedures documented + executed | ✅ for L2-1/L2-2/L2-3a (the 3 that ran); L2-3b/4/5 have plans in deferred sections | ✅ |

## Truth Verification (frontmatter must_haves)

| # | Truth | Verification | Status |
|---|---|---|---|
| 1 | Every Inj has `programmatic_cmds` | `pytest tests/chaos/test_l2_chaos_plan.py::test_every_injection_has_programmatic_verification` | ✅ 5/5 PASS |
| 2 | Every Inj has container-localhost fallback | `pytest …::test_every_injection_has_container_fallback` | ✅ 5/5 PASS |
| 3 | Inj L2-1 verified live | machine restart + l2_top_of_book +2 rows in 120s | ✅ PASS |
| 4 | Inj L2-2 verified live | daemon stays started + fail-soft works, but uncovered 5 observability GAPs | ✅ partial PASS |
| 5 | Inj L2-3 (both halves) | L2-3a PASS (B1 default OFF); L2-3b deferred with substitute NOTIFY evidence | ✅ partial (a PASS, b deferred) |
| 6 | Inj L2-4 verified | DEFERRED — needs POLYARB_WS_TEST_KILL flag + GAP-fix re-run | ⏳ deferred |
| 7 | Inj L2-5 verified | DEFERRED — backfill is ad-hoc path, not daemon main loop | ⏳ deferred |
| 8 | SOAK-LOG ≥5 segments | `grep -c '^### Inj L2-' …/03-SOAK-LOG.md` → 5 | ✅ |
| 9 | All cleanup executed | L2-1 natural, L2-2 SUPABASE_SERVICE_KEY restored, L2-3a read-only | ✅ for executed Inj |

## Deviations Applied (in-prod discovery)

### Deviation 1 — `pkill` not in python-slim base image

**Discovery**: Inj L2-1 first attempt called `flyctl ssh ... pkill -SIGTERM -f polyarb.daemon.l2_main`. The L2 container's `python:3.12-slim` base has **no `pkill` / `ps` / `kill` / `which`** — `exec: "pkill": executable file not found in $PATH`.

**Substitute**: `flyctl machine restart <id>` — sends SIGTERM to PID 1 (the daemon entry), triggers cold-start cycle in ~12s. Tests the **same** WS-disconnect-then-reconnect invariant PLUS exercises Phase 02 P9 server-started gate on cold start.

**Phase 03.1 follow-up**: implement `POLYARB_WS_TEST_KILL=1` code flag (~10 lines in `ws_market_client.py`) for finer-grained future chaos. Until then, machine restart is the production-realistic kill primitive.

### Deviation 2 — stale `FLY_API_TOKEN` in `.env` shadowing `flyctl auth` keychain

**Discovery**: Inj L2-2 cleanup `set -a; . ./.env; set +a; flyctl secrets set ...` failed with `Could not find App "polyarb-l2"`. Root cause: `.env` contains an old L1-only Fly API token (from Phase 02 deploy); `set -a` loads it into shell env, **overriding** the local `flyctl auth login` keychain credential. Since the old token has no L2 access, every L2 ops call returns the misleading "App not found" error.

**Workaround**: prefix flyctl commands with `FLY_API_TOKEN= ` to clear the env var, forcing keychain fallback.

**Phase 03.1 follow-up GAP-4**: update chaos Makefile + `scripts/fly_secrets_sync.sh` to drop FLY_API_TOKEN before any flyctl call (or filter it out of the .env load explicitly).

### Deviation 3 — `/health` mirror sub-check exists but never fires

**Discovery**: Inj L2-2 expected `/health = 503` after revoking SUPABASE_SERVICE_KEY (mirror writes fail). Actual: `/health = 200` because `_build_l2_health_checks` (l2_health.py:169-180) gates the `mirror:l2_tob_age_seconds` check on `settings.l2_mirror_enabled` — a flag **Plan 03-06 never added** to `config.py`. So the check is dead code in prod.

**This is the major substantive discovery of this chaos cycle**: code passes unit tests (truths 6-8 of Plan 03-06 all PASS) but the chain `mirror failure → health 503 → operator alarm` was never wired. Plan-checker iter 2 verified code-level truth but missed chain-level truth — same regression as Phase 02.1 D-09 mandated avoiding.

**Phase 03.1 follow-up GAPs 1-3 + 5**: add `l2_mirror_enabled` config, add `SqliteStore.get_l2_tob_last_mirror_at_s()`, persist `last_mirror_at_s` in mirror success path, re-run L2-2 with Sentry API breadcrumb query.

## Commits

This plan ships in 4 commits (post pre-commit-hook + planning-hygiene):

1. `feat(03-07): add tests/chaos/test_l2_chaos_plan.py — declarative L2 chaos plan + 17 invariant tests`
2. `feat(03-07): add Makefile chaos-l2-{baseline,inj1,inj2,inj3a,inj3b,cleanup} targets`
3. `docs(03-07): 03-SOAK-LOG.md — Phase 03 Wave 6 chaos cycle 2026-05-25 (3 live PASS + 2 deferred + GAPs)`
4. `docs(03-07): plan 07 SUMMARY — Wave 6 chaos closed with carry-over to Phase 03.1`

## Phase 03.1 Carry-Over

**5 GAPs (P0/P1) from Inj L2-2**:
- GAP-1 P0: add `Settings.l2_mirror_enabled` field + model_validator auto-set
- GAP-2 P0: add `SqliteStore.get_l2_tob_last_mirror_at_s()` getter
- GAP-3 P0: `L2SupabaseMirror.push_*` success path persists `last_mirror_at_s`
- GAP-4 P1: chaos Makefile + secrets sync — drop FLY_API_TOKEN before flyctl
- GAP-5 P1: re-run Inj L2-2 after GAP-1/2/3 land, Sentry API query for breadcrumb

**3 deferred Inj** (with explicit re-run plans in SOAK-LOG):
- L2-3b: opt-in L1 NOTIFY path — needs scheduled low-traffic window
- L2-4: cross-bug (storm + DSN unset) — needs POLYARB_WS_TEST_KILL flag + GAP-fix re-run
- L2-5: Data API 429 backfill — ad-hoc path, verify on first real backfill demand

**Why this is "PASS with carry-over" not "FAIL"**:
The verified surface (catchup replay + bootstrap WS + LISTEN chain + machine restart resilience + B1 invariant honored) is **sufficient to claim polyarb-l2 production-running**. The GAPs are observability gaps (operator sees less than they should when mirror breaks) not data-correctness gaps (no data loss occurred, no incorrect writes). Wave 7 (Plan 03-08) docs + dashboard can ship; Phase 03.1 surface the GAPs.

## Carry-Forward Notes for Phase 03.1

**Architecture insight from Inj L2-2 GAP analysis**: The Phase 02.1 D-09 verification-ownership discipline (Claude self-verifies via API/log/curl) discovered a deeper invariant Plan 03 should have encoded: **"every fail-soft path MUST surface to /health"**. Code-level fail-soft (envelope catches exception + logs warning) without health surfacing means operators see green dashboards while data is silently degraded. Phase 03.1 should add this as a plan-checker rule for any future workstream that ships fail-soft envelopes.
