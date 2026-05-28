# Phase 04 — SOAK / Chaos Log

> Per-injection chaos records for Phase 04 candidate-set + L2 throughput. Each section captures pre-flight evidence, raw measurements, D-06 verdict, and any deviations / new findings.

---

## Inj L2-4-throughput (Phase 04 Plan 04 Task 3)

**Window:** 2026-05-28T13:49:43Z → 2026-05-28T14:00:00Z (storm @ 13:55:39Z, cleanup @ 13:57:31Z)
**Operator:** Claude executor (operator-authorized Path A continuation, post-G-01 fix)
**Deployed image:** polyarb-l2 v18 (`sha256:9f22b823e81c6f5e83a0ab15cbcfab596c9b712b3766c95c9437a985cff367a1`) — first prod release carrying the G-01 cold-start debounce fix (`39c60ef`)

### Pre-flight evidence (post-v18 deploy)

| Check | Expected | Observed | Status |
|---|---|---|---|
| `flyctl image show -a polyarb-l2` matches latest main | digest matches main HEAD | v18 digest `9f22b823…`, deployed from main HEAD `39c60ef` | ✅ |
| `/healthz` returns 200 | 200 | 200 | ✅ |
| `/health.checks` contains all 6 Phase 04 sub-checks | 6 keys present | 6 present (event_bus / mirror / ws:state / ws:age / ws:subs / candidates:fetch) | ✅ |
| `ws:subscribed_count > 3` (D-01 effective) | > 3 | **60** (D-01 swap landed on first NOTIFY post-G-01 fix) | ✅ |
| `candidates:supabase_fetch_age_seconds` ≠ null | numeric value | **91.4s** at first probe, drifting to 422.8s by T1 (normal — no new NOTIFY in baseline window) | ✅ |
| `mirror:l2_tob_age_seconds` healthy | pass (D-08 case-c) | pass, 90.9–122.6s during baseline | ✅ |
| `ws:last_event_age_seconds` fresh | pass | pass, 2.9–7.4s | ✅ |

**G-01 fix observable in prod**: pre-fix v17 stayed cold-start `fetch_age=null`/`subscribed=3` indefinitely; v18 transitioned to `fetch_age=91.4s`/`subscribed=60` on the very first /health probe (~30s after machine started). Mechanism: catchup replayed 31 backlogged NOTIFYs (snapshot_ids 201–234); with `_last_refresh_at_s = -REFRESH_DEBOUNCE_S - 1.0`, the first replayed NOTIFY now passes debounce, runs the markets_latest fetch, and applies the diff to the WS subscription.

### Chaos sequence

```
T0    13:44:13Z  v18 deploy first health probe → subs=60, fetch_age=91.4s, mirror=126s (healthy)
T1    13:49:43Z  baseline 1 snapshot
                 ws_state=WAITING_FOR_EVENT, ws_age=13.9s, subs=60, mirror_age=369.9s, fetch_age=422.8s
                 RSS (PID 1 = hallpass not python — see Pitfall PID below): VmRSS=6400 kB
T1+5m 13:55:08Z  baseline 2 snapshot
                 ws_state=WAITING_FOR_EVENT, ws_age=0.1s, subs=60 (implied), mirror_age=694.8s, fetch_age=747.8s
                 RSS: VmRSS=6400 kB
STORM 13:55:39Z  `flyctl secrets set POLYARB_WS_TEST_KILL=1` — Fly triggered rolling restart
                 13:55:57.553Z: new process started consume loop, 3 bootstrap assets (NOT 60 — fresh start)
                 13:55:57.727Z: KILL flag detected, WS closed via WsTestKillRequested
                 Machine cycled in deploy-induced KILL=1 mode for ~117 s
T3    13:57:02Z  recovery snapshot (storm + 60 s)
                 ws_state=DISCONNECTED, ws_age=66.9s, subs=3, mirror_age=809.1s, fetch_age=null, chaos_flag=warn
                 RSS: VmRSS=6432 kB
CLEANUP 13:57:31Z  `flyctl secrets unset POLYARB_WS_TEST_KILL` — second rolling restart
                 13:57:53.911Z: fresh process started; KILL flag absent; consume loop healthy with 3 bootstrap
                 13:57:55Z: 3 mirror pushes (initial dumps) succeed
                 NO new NOTIFY arrived during catchup (cursor already at 234) → fetch never re-ran
                 → subs stuck at 3, fetch_age stuck at null for the rest of the window
14:00:00Z+      `/health.status = warn` then drifting to `fail` as mirror_age crosses 600s
                 Mirror eventually crosses fail threshold (~1080s at 14:11Z; ~1870s at 14:29Z)
                 — symptom of 3-asset low-event pattern, NOT mirror pipeline breakage
```

### D-06 verdict per three criteria

| # | Criterion | Required | Observed | Verdict |
|---|---|---|---|---|
| 1 | `frame_rate_recovery >= frame_rate_baseline * 0.90` | — | **N/A — frame_count NOT surfaced on /health, so no rate computable** | ❌ N/A (deferred) |
| 2 | `watchdog_state == WAITING_FOR_EVENT within 60 s of storm end` | — | At storm+60 s (T3=13:57:02): `ws_state=DISCONNECTED`. Machine restart-into-KILL means the process was actively closing the WS for the entire 60 s window. Recovery only occurred at 13:57:53 (post-cleanup restart, +134 s from initial storm secrets-set). | ❌ **FAIL** (test methodology issue — see Finding G-03 below) |
| 3 | `RSS_recovery <= RSS_baseline * 1.30` | — | All three RSS reads = 6400/6400/6432 kB (≤ 1.005× ratio). **HOWEVER** the procfs read targeted PID 1 = `/.fly/hallpass` (Fly SSH proxy binary), NOT the Python L2 process. So the ratio holds trivially but measures the wrong process. | ❌ N/A (instrumentation bug — see Finding G-04 below) |

**Overall verdict: DEFERRED**. The chaos run executed end-to-end and CLEANUP succeeded (POLYARB_WS_TEST_KILL is absent, `chaos:ws_test_kill_flag` sub-check absent at 14:00Z+). However the three D-06 indicators could not be evaluated against the intended question ("can the L2 keep up under real candidate-scale WS throughput?") because of three structural issues exposed during the run:

1. **G-02: D-01 fetch not re-driven on restart-without-NOTIFY-backlog** — biggest scope issue
2. **G-03: `flyctl secrets set/unset` triggers rolling restart, not in-flight env mutation** — chaos primitive design mismatch
3. **G-04: RSS reads target PID 1 (hallpass), not the Python L2 process** — instrumentation bug in the chaos target

The Plan 04 goal ("verify L2 throughput against real candidate set") **was NOT achieved by this run**. The G-01 fix is verified in prod, the candidate-set expansion landed (subs=60 pre-storm), and the chaos primitive ran cleanly, but the verdict math demands separate fixes for G-02/G-03/G-04 before a meaningful re-run.

### Findings (new — feed forward into 04-04 deferred / next plan)

#### G-02 — D-01 fetch not re-triggered on restart-without-NOTIFY-backlog

**Discovery:** Post-cleanup restart at 13:57:53. Catchup queried Postgres cursor (`l2_event_cursor.last_snapshot_id`) — found cursor already at 234, no backlog → `event-bus catchup: no missed snapshots` → `on_snapshot_complete` never invoked → markets_latest fetch never ran → subs stayed at 3 bootstrap.

L1 emits NOTIFY only on its own snapshot cycle (~30+ min cadence per current scheduler). So after any L2 restart in a quiet NOTIFY window, the L2 stays on bootstrap until the next genuine L1 snapshot.

**Impact:** Phase 04 Plan 02 D-01 ("Supabase data-source swap effective in prod") is **fragile across L2 restarts**. The 60-asset state at v18 first boot was a lucky accident — there happened to be 31 backlogged NOTIFYs from the v17-to-v18 deploy gap. Once the cursor advances past those, restarts can land in a 0-NOTIFY window and the system silently degrades to 3 bootstrap subs without any error.

**Recommended fix (separate plan, e.g. 04-05 or fold into Phase 05 plan):**
- **Eager startup fetch**: after catchup completes (with or without missed snapshots), explicitly call `on_snapshot_complete({"snapshot_id": -1, "_startup_prime": True}, ...)` once with a synthetic payload. The debounce floor allows this since post-G-01 first call always passes.
- **OR scheduled fallback**: timer-based `asyncio.create_task` that re-runs the markets_latest fetch every N minutes regardless of NOTIFY, so the candidate set drifts back to current within bounded time after a restart.
- **OR: align L2 boot to L1 snapshot cadence**: add a startup-time max-wait for next NOTIFY before declaring catchup complete; if no NOTIFY within (e.g.) 5 min, force-fetch.

Eager startup fetch is the smallest delta (one line in `main.py` after `catchup_from_cursor`).

#### G-03 — `flyctl secrets set/unset` triggers rolling restart, breaking the chaos design

**Discovery:** The `chaos-l2-inj4-throughput` target uses `flyctl secrets set POLYARB_WS_TEST_KILL=1` to inject the chaos flag. Fly's behavior: secrets ARE machine-level env vars, so setting one **triggers a rolling deploy** of the machine to pick up the new env. Each `secrets set`/`secrets unset` is a full process restart. The same applies to `chaos-l2-inj4` baseline (Phase 03.1) — it inherits the same primitive.

This means:
- The pre-storm process running with 60 subscribed assets is never tested under "WS close + reconnect" — it's stopped entirely and replaced with a NEW process that hasn't fetched yet.
- The "60s wait after storm" measures a fresh process's startup, not a kill-recovery cycle.
- True in-flight WS chaos (kill an existing WS connection on a running process and watch reconnect) is **not what this primitive does on Fly with `secrets`**.

**Recommended fix:**
- **Option A** (preferred): inject the chaos flag via the in-band HTTP endpoint. Add a `POST /admin/chaos/ws-test-kill` that flips a process-local atomic flag, no restart, no env mutation. Gate with `POLYARB_SCAN_SHARED_SECRET` or similar so it's not externally exploitable.
- **Option B**: use `flyctl ssh console -C 'kill -USR1 <pid>'` + signal handler in the daemon that sets the flag. Still process-local, no restart.
- **Option C** (low effort, low signal): document that current chaos target measures "restart-into-killed-state recovery time" not "in-flight kill recovery time". Update D-06 indicator 2 to reflect this is actually a startup-grace-period measurement.

Option A is the most aligned with Phase 04 intent and unblocks meaningful Inj L2-4 verdicts.

#### G-04 — RSS reads target PID 1 (hallpass), not the Python process

**Discovery:** All three RSS samples = 6400 kB / 6400 kB / 6432 kB. That's the size of `/.fly/hallpass` (Fly's SSH proxy Go binary), not the Python L2 daemon. The Makefile recipe uses `grep VmRSS /proc/1/status` — PID 1 in the Fly machine is hallpass, not the application.

**Why this is a soft fail not a hard fail**: the recipe's `|| echo "RSS-read skipped"` ensures the chaos still completes; it just records useless numbers. But the D-06 indicator 3 (RSS ratio) becomes meaningless.

**Recommended fix:**
- Replace `grep VmRSS /proc/1/status` with `pgrep -f 'python -m polyarb.daemon.l2_main' | xargs -I{} grep VmRSS /proc/{}/status`. Image-aware safe (pgrep + procfs both in any Linux image — verified by image-check pattern in `docs/dev/chaos-toolkit.md`).
- OR query a `/health` extension that exposes `psutil.Process().memory_info().rss` for the daemon process.

### Pitfall 4 watch — watchdog false-trips during healthy windows

**Not observed** (could not be observed). The chaos design (G-03) restarted the process before any in-flight watchdog behavior could be sampled across the storm boundary. Pitfall 4 verification is therefore **carried forward** as an open question for the post-G-03-fix re-run.

### Mirror staleness retrospective (T1/T2 + post-cleanup degradation)

T1 at 13:49:43 already showed mirror_age=370 s (drifting past warn=300 s). T2 at 13:55:08 showed mirror_age=695 s (past fail=600 s). This degradation was already in progress BEFORE the storm step. Probable cause: even at 60 subscribed assets, most Polymarket markets are illiquid — the WS keeps the connection alive with various low-priority frames but the specific event_types that drive `_on_event → push_top_of_book` (price_change / best_bid_ask / book / last_trade_price) may not arrive frequently. Confirmed via log inspection: only the initial dump at process start produced 3 mirror pushes, then silence.

This is **structural Polymarket low-liquidity reality**, not a Phase 04 regression. Suggests that:
- `mirror:l2_tob_age_seconds` warn/fail thresholds may need recalibration once the real candidate set is consistently in place, OR
- The recipe selecting candidates needs to bias toward higher-event-rate markets (volume-weighted, recent-trade-weighted), not just current population.

This is fed forward to Phase 05 / next planning round, NOT a Plan 04-04 blocker.

### Cleanup verification

- ✅ `POLYARB_WS_TEST_KILL` not in `flyctl secrets list -a polyarb-l2` (verified post-run + at 14:11Z)
- ✅ `/health.checks` does NOT contain `chaos:ws_test_kill_flag` (verified at 14:00Z, 14:11Z, 14:29Z)
- ⚠ `/health.status = fail` from ~14:00Z onward — due to `mirror:l2_tob_age_seconds` crossing 600 s. Root cause is G-02 (3 bootstrap asset low-event-rate post-restart), NOT chaos residue. **prod is functionally healthy** — WS receiving events, event listener listening, mirror pipeline able to push (proven by initial dumps at 13:57:55) — but no qualifying events arriving on the 3 bootstrap assets.

### Calendar-window analysis

UTC window 13:49 → 14:29 (US morning, EU midday — moderate activity). Observed: only 60 D-01-selected assets vs the 30–200 planner-anticipated range. The N=60 is at the lower end of expected for moderate-activity Thursday. Higher-activity windows (election cycles, sports event days) would push N closer to 200. This is normal calendar variance — NOT A2 (low-activity documented-deferral).

### Artifacts

- `/tmp/04-04-deploy-v18.log` — deploy output (v18, clean rollout, no EOF this time)
- `/tmp/04-04-postdeploy-v18.json` — first post-deploy /health snapshot (T0, subs=60, fetch=91.4s)
- `/tmp/04-04-T0-pre-chaos.json` — pre-chaos baseline
- `/tmp/inj4t-t1.json` `/tmp/inj4t-t2.json` `/tmp/inj4t-t3.json` — chaos T1/T2/T3 snapshots
- `/tmp/inj4t-t1-rss.txt` `/tmp/inj4t-t2-rss.txt` `/tmp/inj4t-t3-rss.txt` — RSS reads (WRONG PROCESS — see G-04)
- `/tmp/04-04-chaos-output.log` — full chaos target stdout
- `/tmp/04-04-postcleanup-1.json` `/tmp/04-04-stable-check.json` — post-cleanup state probes

### Verdict summary

| Question | Answer |
|---|---|
| Does G-01 cold-start debounce fix work in prod? | ✅ YES — first NOTIFY after process start now runs the fetch (verified v18 boot transitioned 3→60 within ~30 s) |
| Did the chaos primitive execute end-to-end? | ✅ YES — storm + cleanup both succeeded; POLYARB_WS_TEST_KILL absent post-run |
| Did D-06 indicators produce evaluable numbers? | ❌ NO — G-02 (post-restart cold-start no-fetch), G-03 (chaos = restart, not in-flight kill), G-04 (wrong RSS PID) all block meaningful verdict |
| Was the Phase 04 goal ("verify real candidate-scale throughput") met? | ❌ NO — deferred pending G-02/G-03/G-04 fix |
| Is prod healthy after cleanup? | ⚠ DEGRADED (mirror_age fail) but ROOT CAUSE understood (G-02) — NOT a chaos residue, NOT a regression. WS receiving events, listener listening, pipeline able to push, just no qualifying events on the 3 bootstrap assets. Will recover automatically on next L1 NOTIFY (within next ~30 min). |
