---
phase: 04-candidate-set-l2-throughput
plan: 04
subsystem: chaos-engineering
tags: [websocket, chaos, throughput, fly-io, polywatch, instrumentation]

# Dependency graph
requires:
  - phase: 03.1-l2-observability-gaps-fix-up
    provides: chaos-l2-inj4 baseline + POLYARB_WS_TEST_KILL primitive + FLY_API_TOKEN= discipline (Pitfall 5)
  - phase: 04-candidate-set-l2-throughput
    provides: Plan 02 Supabase-driven candidate expansion (>3 assets in prod) — required for real-scale throughput
provides:
  - WsConsumer.dropped_frame_count counter (D-06 indicator 1 — measurable signal at the on_event raise site)
  - /health "ws:subscribed_count" sub-check (operator-visibility surface + Makefile precondition gate)
  - Makefile `chaos-l2-inj4-throughput` target (Tasks 1+2 scaffold for the human-verify Task 3 chaos run)
affects: [Phase 04 Plan 04 Task 3 (human-verify gate, not yet executed); future throughput regression sweeps]

# Tech tracking
tech-stack:
  added: []  # No new dependencies — uses existing curl/jq/flyctl tooling.
  patterns:
    - "Baseline-then-threshold chaos verdict (RESEARCH Q4): capture T1+T2 baseline first, then storm, then T3 recovery, then apply ratio-based pass criteria. Avoids brittle absolute thresholds."
    - "Image-aware chaos: tools run on the LOCAL dev host against public Fly endpoints; RSS via procfs (no container tool gap). flyctl ssh procfs read is image-aware safe (procfs exists in every Linux image)."
    - "Operator-visibility /health sub-check pattern: status='pass' for purely informational counters that should appear in /health output but must NOT drive overall health pass/fail (kept distinct from alerting-grade sub-checks)."

key-files:
  created:
    - "tests/daemon/test_ws_consumer_dropped_frames.py"
    - ".planning/workstreams/m1-perception/phases/04-candidate-set-l2-throughput/04-04-SUMMARY.md"
  modified:
    - "src/polyarb/daemon/ws_consumer.py"
    - "src/polyarb/http/l2_health.py"
    - "Makefile"

key-decisions:
  - "RSS measurement via `flyctl ssh console -C 'grep VmRSS /proc/1/status'` — procfs is image-aware safe, no Dockerfile change, no /health sub-check coupling. Falls back gracefully (`|| echo skipped`) if ssh unreachable so the rest of the recipe still completes."
  - "Added `ws:subscribed_count` sub-check inside Task 2 scope (plan executor note authorized) — needed by the precondition gate to abort when D-01 Supabase swap has not landed in prod. Status='pass' always (informational; ws:connection_state already drives connection health)."
  - "Snapshots saved to /tmp/inj4t-t{1,2,3}.json + /tmp/inj4t-t{1,2,3}-rss.txt so the operator can compute deltas offline without re-curling during the human-verify Task 3 window."

patterns-established:
  - "FLY_API_TOKEN= prefix on EVERY flyctl invocation in chaos recipes (Pitfall 5, fly-api-token-shadowing memory) — including the ssh console calls used for RSS."
  - "Pre-flight precondition gates in chaos recipes that abort with exit 1 when an essential expectation fails (e.g. ws:subscribed_count <= 3 → would degrade test to logic-only)."

requirements-completed: []  # plan 04-04 has requirements: [] (ROADMAP-scoped, covers D-05/D-06)

# Metrics
duration: ~45min (Tasks 1+2 scaffolding only — Task 3 human-verify pending)
completed: 2026-05-28
---

# Phase 04 Plan 04: Real Candidate-Scale WS Throughput Verification — SCAFFOLD (Tasks 1+2 only)

**Adds the `dropped_frame_count` instrumentation + `chaos-l2-inj4-throughput` orchestrator that repays the Phase 03.1 Inj L2-4 "no genuine storm rate" debt; Task 3 prod chaos run is the human-verify gate and is deferred to the operator.**

## Performance

- **Duration:** ~45 min for Tasks 1+2 (Task 3 prod chaos = separate operator window, ~7 min wall)
- **Started:** 2026-05-28T17:23Z
- **Completed (Tasks 1+2):** 2026-05-28T18:08Z
- **Tasks executed:** 2 of 3 (Task 3 = human-verify checkpoint, NOT executed by this agent)
- **Files modified:** 4 (1 test created + 3 src/Makefile edits + this SUMMARY)

## Accomplishments

- **WsConsumer.dropped_frame_count counter (D-06 indicator 1).** New `_dropped_frame_count` int counter (init 0), `dropped_frame_count` property, and `+= 1` at the on_event raise site (`src/polyarb/daemon/ws_consumer.py:160-164`). Semantic: a frame that is RECEIVED but whose downstream `on_event` raises now counts as both received (frame_count += 1) AND dropped (dropped_frame_count += 1). The existing warning log is preserved — operators get both the flyctl-logs breadcrumb AND a measurable number. Tested by 3 fresh tests + the 4 existing `test_ws_consumer.py` tests still pass (7/7).
- **/health `ws:subscribed_count` operator-visibility sub-check.** Added to `src/polyarb/http/l2_health.py` after the ws:last_event_age_seconds check. Status is always `pass` (purely informational; connection-health alerting stays driven by ws:connection_state). This is what the Makefile precondition gate reads to confirm the D-01 Supabase data-source swap is effective in prod (>3 assets subscribed, not the 3 bootstrap asset_ids).
- **Makefile `chaos-l2-inj4-throughput` target.** Copies the `chaos-l2-inj4` shell shape verbatim, extends with 8-step baseline-then-threshold orchestration per RESEARCH Q4, and writes JSON snapshots to `/tmp/inj4t-t{1,2,3}.json` + RSS reads to `/tmp/inj4t-t{1,2,3}-rss.txt`. ALL 5 flyctl invocations carry the `FLY_API_TOKEN= ` prefix (Pitfall 5 — fly-api-token-shadowing memory). RSS via `flyctl ssh console -a polyarb-l2 -C 'sh -c "grep VmRSS /proc/1/status"'` — image-aware safe (procfs is in every Linux image). Target appears in `make help` and `make -n chaos-l2-inj4-throughput` parses clean.

## Task Commits

Atomic per-task:

1. **Task 1 RED — failing tests** → `c1ce2c3` (test)
2. **Task 1 GREEN — counter implementation** → `41c71fd` (feat)
3. **Task 2 — Makefile target + ws:subscribed_count + this SUMMARY** → bundled into the next commit (Plan 04-04 pre-commit hook requires SUMMARY presence for plan-scoped commits)

## Files Created/Modified

- **`tests/daemon/test_ws_consumer_dropped_frames.py`** (CREATED, 145 lines) — three TDD tests: starts-zero / increments-on-raise / not-incremented-on-success.
- **`src/polyarb/daemon/ws_consumer.py`** (MODIFIED) — `_dropped_frame_count` init + property + `+= 1` at on_event raise site. `grep -c 'dropped_frame' src/polyarb/daemon/ws_consumer.py` → 5 (≥ 3 acceptance criterion satisfied).
- **`src/polyarb/http/l2_health.py`** (MODIFIED) — new `ws:subscribed_count` sub-check inserted after Check 2 (ws:last_event_age_seconds), guarded by `ws_consumer is not None`. Status='pass' purely informational.
- **`Makefile`** (MODIFIED) — `chaos-l2-inj4-throughput` target added between `chaos-l2-inj4` and `chaos-l2-inj5-dryrun`. Header comment + `## ` doc lines for `make help`. .PHONY line at end.

## Decisions Made

1. **RSS path:** `flyctl ssh console -C 'grep VmRSS /proc/1/status'` (procfs). Considered (a) /health memory sub-check (rejected — would couple chaos to a new alerting-grade signal); (b) Fly metrics API (rejected — heavier auth + slower iteration). Procfs is the cleanest: zero image surface, zero coupling, fails soft (`|| echo skipped`) without breaking the recipe.
2. **`ws:subscribed_count` status='pass':** Informational only. The alerting path for WS health stays driven by `ws:connection_state` (already CONNECTED/RECONNECTING/etc-mapped). Treating subscribed_count as a fail signal would create noise on every cold-start window before candidate refresh populates.
3. **Snapshots persisted to `/tmp/inj4t-t{1,2,3}.json`:** Operator can re-jq them after the run without burning a second /health request; supports offline verdict computation.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 — Missing Critical] Added `ws:subscribed_count` /health sub-check inside Task 2 scope**
- **Found during:** Task 2 (Makefile precondition gate design)
- **Issue:** The plan executor note authorized adding `ws:subscribed_count` if no field exposed the count yet (`grep` of l2_health.py confirmed none). The Makefile's precondition gate (`if [ "$N" -le 3 ]; then ABORT`) is meaningless without it.
- **Fix:** Added Check 2b `ws:subscribed_count` to `src/polyarb/http/l2_health.py` reading `len(ws_consumer.subscribed_assets)`, fail-soft to 0 on read error, status='pass' always.
- **Verification:** 39 existing health/l2_app tests still pass; no regression on alerting signals.
- **Committed in:** bundled into the Task 2 commit alongside the Makefile target.

**2. [Rule 3 — Blocking] Restored `core.hooksPath` to `.githooks`**
- **Found during:** Pre-execution state check
- **Issue:** `git config --get core.hooksPath` returned the default `.git/hooks` (worktree-local default), bypassing the project's SUMMARY-gate pre-commit hook.
- **Fix:** `git config core.hooksPath .githooks`.
- **Verification:** `git config --get core.hooksPath` → `.githooks`.
- **Committed in:** none (config-only, not a tracked change).

---

**Total deviations:** 1 auto-fix (Rule 2: missing critical operator-visibility surface needed by the Task 2 precondition gate).
**Impact on plan:** Within scope — plan explicitly authorized this addition in the Task 2 executor note ("If no field exposes the count yet, add one"). No scope creep.

## Issues Encountered

- **Pre-existing test failures unrelated to this plan:** `tests/daemon/test_ws_watchdog.py` (6 fails — async harness missing pytest-asyncio plugin), `tests/daemon/test_l2_main_startup.py` (4 fails — same async harness issue), `tests/m1-perception/test_makefile_contract.py::test_make_smoke_health_local_dry_run_recipe` (asserts hardcoded 127.0.0.1:8080 but Makefile uses $PORT default 19080). Verified pre-existing via `git stash` regression check — none caused by Plan 04-04 changes. Logged to `.planning/workstreams/m1-perception/phases/04-candidate-set-l2-throughput/deferred-items.md`.

## User Setup Required

None. The chaos target uses existing polyarb-l2 Fly secrets + keychain-resident `FLY_API_TOKEN` (via the `FLY_API_TOKEN= ` prefix that nukes the env-shadowed value). `jq` is required on the dev host (already installed: `/opt/homebrew/bin/jq` v1.7.1).

## Task 3 — PENDING (operator must run prod chaos manually)

**This scaffold delivers the instrumentation + orchestrator. The actual prod chaos run and pass/fail verdict against D-06 three criteria are deferred to the human-verify checkpoint per plan `<task type="checkpoint:human-verify" gate="blocking">`.**

### must_haves verdicts

| truth | status | evidence |
|---|---|---|
| WsConsumer exposes a dropped-frame counter that increments when on_event callback raises | ✅ VERIFIED | `src/polyarb/daemon/ws_consumer.py:160-164` + 3 tests in `tests/daemon/test_ws_consumer_dropped_frames.py` (3/3 GREEN) |
| `make chaos-l2-inj4-throughput` runs against polyarb-l2 prod with the REAL candidate set (>3 assets), not 3 bootstrap assets | ⏳ PENDING Task 3 | Precondition gate added (aborts when `ws:subscribed_count <= 3`); actual run by operator |
| Baseline frame rate + RSS captured before storm; recovery compared against baseline (D-06 three indicators) | ⏳ PENDING Task 3 | Recipe shape implemented + snapshots to /tmp; actual numbers from operator's run |
| Throughput pass = `frame_rate_recovery >= baseline*0.90 AND watchdog == WAITING_FOR_EVENT within 60s AND RSS_recovery <= baseline*1.30` | ⏳ PENDING Task 3 | Criteria printed at end of recipe; operator records verdict in 04-SOAK-LOG.md |
| Deployed prod image == latest plan-merged main BEFORE chaos | ⏳ PENDING Task 3 | Operator pre-flight: `FLY_API_TOKEN= flyctl image show -a polyarb-l2` vs `git log origin/main -1` |

### Operator pre-flight checklist (Task 3)

```bash
# 1. Verify deployed image == latest main (parallel-worktree-rebase memory)
FLY_API_TOKEN= flyctl image show -a polyarb-l2
git log origin/main -1

# 2. Confirm candidate set has expanded (>3 assets)
curl -sS https://polyarb-l2.fly.dev/health | jq '.checks["ws:subscribed_count"][0]'

# 3. Confirm D-08 mirror healthy + recent Supabase fetch
curl -sS https://polyarb-l2.fly.dev/health | jq '.checks["mirror:l2_tob_age_seconds"][0], .checks["candidates:supabase_fetch_age_seconds"][0]'

# 4. (Optional) ensure dev deps for any local RSS reads
uv sync --extra dev

# 5. Run the chaos (~7 min wall: 5min baseline + 60s storm + 30s cleanup + chatter)
make chaos-l2-inj4-throughput

# 6. Record verdict in 04-SOAK-LOG.md (extend the Inj L2-4 section):
#    - candidate_set_size, frame_rate baseline + recovery, RSS baseline + recovery
#    - watchdog state timeline (T1 → storm → T3), time-to-WAITING_FOR_EVENT
#    - dropped_frame_count delta
#    - D-06 PASS/FAIL per the three ratios
#    - If A2 (calendar low-activity) → documented-deferred with actual N captured
#    - If Pitfall 4 watchdog false-trip observed → record as finding, NOT silent pass
```

### Why this scaffold matters

Phase 03.1 SOAK-LOG explicitly noted Inj L2-4 verified only daemon logic correctness (watchdog kicks in, mirror surfaces) — "3-asset bootstrap is small enough that WS storm is really WS close + reconnect (no genuine storm rate)". Plan 02 expanded the candidate set; this plan adds the measurement surface so the operator can finally answer:

- Does the daemon actually keep up under real-scale WS storm throughput?
- Does memory grow under sustained subscription load?
- Does the watchdog false-trip during normal Supabase-mirror push windows (Pitfall 4)?

Without Tasks 1+2, the answer would be "we can't see". With them, the operator's chaos run produces concrete numbers + a recorded verdict.

## Next Phase Readiness

- **Phase 04 not yet closed** — Task 3 human-verify gate must execute first.
- **Plan 02 + 03 already merged on main** — candidate expansion + D-08 three-branch mirror gate present.
- **No blockers from this plan** for parallel Phase 04 plans or m2/m5 work.
- **Suggested next operator action:** run pre-flight checklist + `make chaos-l2-inj4-throughput`, then resume the plan executor with the recorded verdict.

---

*Phase: 04-candidate-set-l2-throughput*
*Completed (Tasks 1+2): 2026-05-28*
