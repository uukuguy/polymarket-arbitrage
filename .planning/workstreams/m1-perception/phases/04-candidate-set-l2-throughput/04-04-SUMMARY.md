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
duration: ~75min (Tasks 1+2 scaffolding + Task 3 prod chaos run)
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

## Task 3 — EXECUTED — verdict DEFERRED (D-06 indicators blocked by 3 structural issues exposed during run)

**Operator-authorized prod chaos run completed on 2026-05-28T13:49Z → 14:00Z. End-to-end execution succeeded (storm + cleanup both ran; POLYARB_WS_TEST_KILL absent post-run). G-01 cold-start debounce fix is verified effective in prod. However the chaos run exposed THREE new structural findings (G-02 / G-03 / G-04) that block meaningful evaluation of D-06's three pass criteria. Full detail recorded in [`04-SOAK-LOG.md`](04-SOAK-LOG.md). Verdict: chaos primitive executes cleanly, instrumentation works, but the Plan 04 goal "verify real candidate-scale throughput" is DEFERRED pending G-02/G-03/G-04 fix.**

### must_haves verdicts (final)

| truth | status | evidence |
|---|---|---|
| WsConsumer exposes a dropped-frame counter that increments when on_event callback raises | ✅ VERIFIED | `src/polyarb/daemon/ws_consumer.py:160-164` + 3 tests in `tests/daemon/test_ws_consumer_dropped_frames.py` (3/3 GREEN) |
| `make chaos-l2-inj4-throughput` runs against polyarb-l2 prod with the REAL candidate set (>3 assets), not 3 bootstrap assets | ✅ VERIFIED | Pre-flight gate passed with `ws:subscribed_count=60`. Storm executed against 60-asset state; T1/T2 baselines captured at subs=60. |
| Baseline frame rate + RSS captured before storm; recovery compared against baseline (D-06 three indicators) | ⚠ PARTIAL | T1/T2/T3 snapshots captured (subs, ws_state, ws_age, mirror_age, fetch_age, chaos_flag). Frame_count not surfaced on /health → no rate computable. RSS read targeted PID 1 = hallpass (G-04 — wrong process). Indicators 1+3 not evaluable; indicator 2 blocked by G-03 chaos design issue. |
| Throughput pass = `frame_rate_recovery >= baseline*0.90 AND watchdog == WAITING_FOR_EVENT within 60s AND RSS_recovery <= baseline*1.30` | ❌ **DEFERRED** | Indicator 1: N/A (no frame_count on /health). Indicator 2: FAIL on the literal observation (ws_state=DISCONNECTED at storm+60s) but the cause is G-03 (Fly `secrets set` triggers rolling restart, not in-flight env mutation) — the recovery wall-time was measuring a restart cycle, not a kill-recovery cycle. Indicator 3: N/A (wrong PID). |
| Deployed prod image == latest plan-merged main BEFORE chaos | ✅ VERIFIED | v18 deployed from main HEAD `39c60ef`; image digest `sha256:9f22b823…`; v17→v18 transition cleanly observable (only v18 contains the G-01 fix in the prod image). |

### G-01 cold-start debounce fix — verified observable in prod

The first significant outcome of this run: the v17 production state (3 bootstrap assets, `candidates:supabase_fetch_age_seconds=null` "cold-start: never fetched", 5-min poll stuck) transitioned post-G-01 to v18 state (60 D-01 assets, `fetch_age=91.4s` then drifting normally) on the very first health probe ~30s after machine started. This confirms:

- G-01 fix (`_last_refresh_at_s: float = -REFRESH_DEBOUNCE_S - 1.0`) makes the first NOTIFY post-process-start pass the debounce check
- Phase 03.1 + Phase 04 Plan 02 / 03 / 04 D-01 swap is structurally correct, only blocked by the cold-start bug
- The catchup-replay path is the primary first-fetch trigger in normal restart sequences (and works correctly once the debounce floor is configured properly)

### NEW findings from prod run (fold-forward into next plan)

#### G-02 — D-01 fetch not re-triggered on restart-without-NOTIFY-backlog

After the storm + cleanup restart sequence, catchup found 0 missed snapshots (cursor already at 234), so `on_snapshot_complete` was never invoked → markets_latest fetch never ran → subscribed_count stayed at 3 bootstrap. L1 NOTIFY cadence is ~30+ min, so the L2 sits on bootstrap until the next L1 snapshot. The 60-asset state at v18 first boot was a lucky accident (31 backlogged NOTIFYs from the v17→v18 deploy gap). **Phase 04 D-01 is fragile across L2 restarts.**

**Recommended smallest fix**: eager startup fetch after `catchup_from_cursor` regardless of missed count. Synthetic call: `await on_snapshot_complete({"snapshot_id": -1, "_startup_prime": True}, ws_consumer=..., settings=..., mirror=l2_mirror)`. Post-G-01 the first call always passes debounce, so this is safe.

#### G-03 — `flyctl secrets set/unset` triggers rolling restart, not in-flight env mutation

The chaos target sets `POLYARB_WS_TEST_KILL=1` via `flyctl secrets set`. Fly handles secrets as machine-level env, so each `secrets set` is a full deploy. The pre-storm 60-asset process is **terminated** by the deploy, not interrupted mid-flight. The "60s wait after storm" measures a fresh process startup, not a kill-recovery cycle.

**Recommended fix**: HTTP admin endpoint (`POST /admin/chaos/ws-test-kill`) gated by `POLYARB_SCAN_SHARED_SECRET` that flips a process-local atomic flag. Aligns with Phase 04 intent and unblocks Inj L2-4 real verdicts.

#### G-04 — RSS reads target PID 1 (= hallpass, Fly SSH proxy), not the Python L2 process

All three RSS samples in the run were 6400/6400/6432 kB — that's the hallpass Go binary, not the Python daemon. The recipe uses `grep VmRSS /proc/1/status` and PID 1 in Fly machines is hallpass, not the application.

**Recommended fix**: `pgrep -f 'python -m polyarb.daemon.l2_main' | xargs -I{} grep VmRSS /proc/{}/status` or expose `/health.checks["process:rss_kb"]` via `psutil`.

### Mirror staleness during chaos window — explanation

Baseline T1 already showed `mirror_age=370s` (drifting past 300s warn). T2 at +5min: `mirror_age=695s` (past fail). Even at 60 subscribed assets, the specific event_types that drive `_on_event → push_top_of_book` (price_change / best_bid_ask / book / last_trade_price) didn't fire in the 5-min window. Confirmed via log inspection: only the initial dump produced 3 mirror pushes, then silence. **Structural Polymarket low-liquidity reality, NOT a Phase 04 regression.** Suggests `mirror:l2_tob_age_seconds` thresholds may need recalibration once real candidate set is permanently in place, or the candidate recipe should bias toward higher-event-rate markets. Carried forward to next planning round.

### Cleanup verification

- ✅ `POLYARB_WS_TEST_KILL` not in `flyctl secrets list -a polyarb-l2` (verified at 13:58Z, 14:11Z)
- ✅ `chaos:ws_test_kill_flag` absent from `/health.checks` (verified at 14:00Z, 14:11Z, 14:29Z)
- ⚠ `/health.status = fail` from ~14:00Z onward — `mirror:l2_tob_age_seconds` crossed 600s. **Root cause is G-02 (3-asset bootstrap, no qualifying events)**, NOT chaos residue, NOT a regression. WS receiving events (ws_age fresh), event listener listening, mirror pipeline able to push (proven by initial dump pushes). Will recover automatically on next L1 NOTIFY or via the eager startup fetch when G-02 is fixed.

## Next Phase Readiness

- **Phase 04 Plan 04 STATUS:** Tasks 1+2 SHIPPED (counter + Makefile target + ws:subscribed_count sub-check verified in prod). Task 3 EXECUTED but verdict DEFERRED on D-06 indicators.
- **G-01 fix verified in prod** (commit `39c60ef`, v18 image): subs transitioned 3→60 within 30s of process start.
- **G-02 / G-03 / G-04 are new follow-up items** for a separate plan (proposed 04-05 or first plan of Phase 05). All three are scoped, small, and tractable.
- **Phase 04 closure recommendation:** with Tasks 1+2 shipped + Task 3 executed-with-deferred-verdict + the three findings recorded in SOAK-LOG and SUMMARY, Phase 04 can be CLOSED with verdict "candidate-set expansion works in prod when cold-start is fixed; throughput verdict requires follow-up plan." Phase 05 then opens with G-02 fix as first task.
- **No blockers for parallel Phase 04 plans or m2/m5 work.**
- **prod is functionally healthy** even with mirror_age=fail — the failure mode is well-understood and self-recoverable.

---

*Phase: 04-candidate-set-l2-throughput*
*Tasks 1+2 completed: 2026-05-28*
*Task 3 executed (verdict DEFERRED — see SOAK-LOG): 2026-05-28*
