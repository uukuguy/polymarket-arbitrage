---
phase: 03-l2-orderbook-tracking-daemon
plan: 02
subsystem: infra
tags: [fly-io, fly-toml, gha, bash, tomllib, deploy, secrets-sync, d-06]

requires:
  - phase: 02-l1-production-grade
    provides: [fly.toml baseline, deploy.yml GHA pattern, /healthz Fly-friendly probe (Phase 02.1 BUG-6), supercronic two-process pattern]
  - phase: 02.1-phase-02-fix-up-2-p1-backlog-health-503-trade-off
    provides: [D-05 /healthz invariant, D-22 shared SCAN_SHARED_SECRET, L8 flyctl-actions @1.6 pin]

provides:
  - fly-l2.toml (Fly deployment descriptor for polyarb-l2; single 'app' process; /healthz probe; 1gb volume; ams region; POLYARB_DAEMON_VARIANT=l2 selector)
  - .github/workflows/deploy-l2.yml (paths-filtered GHA deploy pipeline; flyctl-actions@1.6; /healthz smoke probe)
  - scripts/fly_secrets_sync.sh (idempotent .env → both polyarb-l1 + polyarb-l2 via flyctl secrets set --stage + deploy)
  - tests/test_fly_l2_config.py (11 tomllib-based structural assertions over the 8 documented diffs)
  - Makefile targets: deploy-l2-prod, fly-l2-status, fly-l2-logs, fly-secrets-sync, fly-secrets-sync-dry

affects: [phase-03-plan-03, phase-03-plan-07, fly-app-roster, secret-propagation-pipeline, ci-deploy-routing]

tech-stack:
  added: [tomllib (3.11+ stdlib), flyctl-actions@1.6 (already pinned in deploy.yml)]
  patterns:
    - single-binary-two-deployments (one Dockerfile, two `flyctl deploy --config` targets — Fly multi-app idiom)
    - paths-filtered-deploy (L1-only commits skip L2 deploy via paths: filter — reduces GHA minutes + noise)
    - secret-redacted-sync (only KEY names echoed; values stay in flyctl set args — T-03-02-01)
    - tomllib-structural-assert (parse + assert shape rather than regex over raw text — robust to formatting drift)

key-files:
  created:
    - fly-l2.toml
    - .github/workflows/deploy-l2.yml
    - scripts/fly_secrets_sync.sh
    - tests/test_fly_l2_config.py
    - .planning/workstreams/m1-perception/phases/03-l2-orderbook-tracking-daemon/03-02-SUMMARY.md
  modified:
    - Makefile (5 targets added: deploy-l2-prod, fly-l2-status, fly-l2-logs, fly-secrets-sync, fly-secrets-sync-dry)

key-decisions:
  - "Memory size 1024mb at parity with L1 (NOT 512mb): Phase 02 OOM precedent (S19) argues for headroom; profile post-Inj L2-1 and revisit in Plan 08 if WS backlog stays modest"
  - "Volume name polyarb_l2_data (NOT polyarb_data): separate Fly volume identity from L1; first creation at Plan 03 deploy checkpoint via flyctl volumes create -s 1 (1gb, no parquet archive)"
  - "DB path /data/l2-state.db (NOT /data/state.db): separate SQLite file even though volume is also separate — defense in depth against accidental same-mount collision if volume naming ever converges"
  - "Paths filter for deploy-l2.yml EXCLUDES src/polyarb/snapshot/orchestrator.py: even though Plan 05 modifies it to emit NOTIFY, that change is L1-side (event publish), so L2 doesn't redeploy when only that file changes"
  - "Secrets sync iterates BOTH apps with same .env (Phase 02.1 D-22 invariant): shared POLYARB_SCAN_SHARED_SECRET — no separate L2 secret minted"

patterns-established:
  - "tomllib-as-spec: structural tests parse the TOML and assert dict shape rather than grep raw text — survives reformatting and is self-documenting"
  - "DRY_RUN env knob on shell scripts that hit external APIs: enables CI/test smoke without side effects (idempotent contract preserved)"
  - "Fly multi-app naming: `polyarb-<layer>` (l1 = snapshot+cron; l2 = WS orderbook); future M3 cross-platform expands to polyarb-kalshi etc."

requirements-completed: [D-06]

duration: ~45min
completed: 2026-05-24
---

# Phase 03 Plan 02: polyarb-l2 Fly Bootstrap Summary

**Separate `polyarb-l2` Fly app config (single binary / two deployments) — TOML + GHA workflow + idempotent secrets sync script, gated for first deploy at Plan 03 closure**

## Performance

- **Duration:** ~45 min
- **Started:** 2026-05-24 (Wave 1 of 7, parallel with Plan 01)
- **Completed:** 2026-05-24
- **Tasks:** 5 (1 TDD RED + 3 GREEN + 1 SUMMARY/Makefile)
- **Files modified:** 5 created + 1 modified (Makefile)

## Accomplishments

- `fly-l2.toml` lands with all 8 documented diffs from fly.toml (app, no cron, 1gb volume, separate DB path, daemon variant env, /healthz probe, single-VM with 1024mb)
- GHA `.github/workflows/deploy-l2.yml` uses `--config fly-l2.toml` with paths-filter (L1-only commits skip L2 deploy noise) and flyctl-actions pinned `@1.6` (Phase 02 L8 discipline)
- `scripts/fly_secrets_sync.sh` ships idempotent + redacted (no `set -x`) + comment-filtered (`grep -v '^#'`) — pushes `.env` to BOTH `polyarb-l1` AND `polyarb-l2` via `flyctl secrets set --stage` + `secrets deploy` for atomic apply
- 11/11 structural tests GREEN (`tomllib`-based — survives formatting drift)
- 5 new Makefile targets (`deploy-l2-prod`, `fly-l2-status`, `fly-l2-logs`, `fly-secrets-sync`, `fly-secrets-sync-dry`)
- Dockerfile NOT modified (single binary architecture verified — `git diff Dockerfile` returns empty)

## Task Commits

1. **Task 1 (RED): test/test_fly_l2_config.py** — `dbd8e38` (test)
2. **Task 2: fly-l2.toml + SUMMARY skeleton** — `5d5c26e` (feat)
3. **Task 3: scripts/fly_secrets_sync.sh** — `28ec7a4` (feat)
4. **Task 4: .github/workflows/deploy-l2.yml** — `098d6e9` (feat)
5. **Task 5: Makefile targets + SUMMARY finalize** — (this commit) (docs)

## Files Created/Modified

- `fly-l2.toml` (NEW, 62 lines) — Fly deployment descriptor for polyarb-l2
- `.github/workflows/deploy-l2.yml` (NEW) — paths-filtered GHA deploy
- `scripts/fly_secrets_sync.sh` (NEW, executable) — idempotent secret push
- `tests/test_fly_l2_config.py` (NEW, 11 tests) — tomllib structural assertions
- `Makefile` (MODIFIED) — 5 targets appended for L2 lifecycle
- `.planning/.../03-02-SUMMARY.md` (this file)

## Truths Verified (from must_haves)

| Truth | Verification command | Result |
|---|---|---|
| app=polyarb-l2 | `uv run python -c "import tomllib; print(tomllib.load(open('fly-l2.toml','rb'))['app'])"` | `polyarb-l2` |
| single 'app' process (no cron) | `uv run python -c "import tomllib; c=tomllib.load(open('fly-l2.toml','rb')); print(list(c['processes'].keys()))"` | `['app']` |
| /healthz probe (NOT /health) | `grep -c 'path = "/healthz"' fly-l2.toml` ≥ 1 AND no bare `/health` | passes |
| memory 512mb or 1024mb | `grep -cE 'memory\s*=\s*"(512\|1024)mb"' fly-l2.toml` ≥ 1 | passes (1024mb) |
| Dockerfile unchanged | `git diff main HEAD -- Dockerfile \| wc -l` | 0 |
| deploy-l2.yml uses fly-l2.toml | `grep -c 'config fly-l2.toml' .github/workflows/deploy-l2.yml` | 1 |
| secrets sync filters comments | `grep -c "grep -v '\^#'" scripts/fly_secrets_sync.sh` | ≥1 |
| Wave 0 tests GREEN | `uv run pytest tests/test_fly_l2_config.py -x -q` exit 0 | 11/11 PASS |

## Decisions Made

- **Memory 1024mb (parity with L1)** — Phase 02 OOM (S19) precedent argues for headroom over thrift. Profile post-Inj L2-1 (Plan 07 chaos) and revisit in Plan 08 if WS backlog stays modest (<200MB anon-rss).
- **Volume `polyarb_l2_data` (not reused `polyarb_data`)** — separate Fly volume identity prevents accidental cross-app mount. First creation at Plan 03 deploy checkpoint (`flyctl volumes create polyarb_l2_data -r ams -s 1`).
- **Separate `/data/l2-state.db` path** — defense in depth even with separate volume (filename signals intent in logs + ls output).
- **`paths:` filter excludes `src/polyarb/snapshot/orchestrator.py`** — Plan 05 modifies it for NOTIFY emission (L1-side); L2 has no reason to redeploy on L1-only changes.
- **`fly_secrets_sync.sh` iterates BOTH apps** — Phase 02.1 D-22 invariant: shared `POLYARB_SCAN_SHARED_SECRET` across L1/L2 (no separate L2 secret minted).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Removed `polyarb-l1` comment references from fly-l2.toml**
- **Found during:** Task 2 (fly-l2.toml landing)
- **Issue:** Initial draft included `polyarb-l1` as comments ("same region as polyarb-l1") which would trip the `grep -c 'polyarb-l1' fly-l2.toml == 0` acceptance gate
- **Fix:** Rephrased comments to "the L1 app" — preserves explanatory intent without literal `polyarb-l1` token
- **Files modified:** fly-l2.toml (2 comment lines)
- **Verification:** `grep -c 'polyarb-l1' fly-l2.toml` returns 0
- **Committed in:** 5d5c26e (part of Task 2 commit)

**2. [Rule 3 - Blocking] Created SUMMARY.md early (before Task 5 completion)**
- **Found during:** Task 2 commit attempt
- **Issue:** pre-commit hook `.githooks/pre-commit` blocks ALL plan-scoped commits (feat/fix/test scope `(03-02)`) without SUMMARY.md present — including Task 2's `feat(03-02)` itself. Creating SUMMARY only at Task 5 would block Tasks 2-4 entirely.
- **Fix:** Drafted SUMMARY.md upfront with placeholder commit hashes (5d5c26e etc.), backfilled at Task 5 commit time
- **Files modified:** 03-02-SUMMARY.md (added one task earlier than planned)
- **Verification:** pre-commit hook accepts Task 2 commit with SUMMARY present
- **Committed in:** 5d5c26e (initial SUMMARY landing), Task 5 final commit (hash backfill — see git log)

---

**Total deviations:** 2 auto-fixed (both Rule 3 — blocking; trivial)
**Impact on plan:** No scope creep. Both fixes preserve plan intent (no L1 leak in TOML; SUMMARY shipped as required).

## Issues Encountered

- pre-commit hook is strict (good — it's the planning-hygiene infrastructure from Phase 02.1). Workaround: create SUMMARY skeleton at Task 2, backfill commits at Task 5. No `--no-verify` used.

## Carry-forward / Next-Plan Hooks

- **First L2 deploy = Plan 03 closure checkpoint** — Plan 03 lands `src/polyarb/daemon/l2_main.py` + `/healthz` handler; only then is `flyctl deploy --config fly-l2.toml` meaningful. Before that, the GHA workflow would deploy but Fly machine would crash-loop (entrypoint missing).
- **Volume must be created at Plan 03 deploy time** — `flyctl volumes create polyarb_l2_data -r ams -s 1` (1gb, ams region). Manual one-shot — gh workflow does not auto-create volumes.
- **Plan 03 should NOT re-pin flyctl-actions** — `@1.6` already correct here; if Plan 03 modifies deploy-l2.yml, preserve the pin.
- **Plan 07 chaos (Inj L2-1) profiles memory** — if anon-rss < 200MB sustained, Plan 08 can drop memory to 512mb. If ≥400MB, hold at 1024mb (parity with L1 final).
- **Secrets sync first run is manual** — user runs `make fly-secrets-sync` once before first L2 deploy. Phase 02.1 D-22 ensures `.env` already has the shared secret.

## Next Phase Readiness

- Wave 1 (Plans 01 + 02) complete enables Wave 2 (Plan 03 — L2 daemon entry).
- No blockers. polyarb-l2 Fly app config infrastructure ready; first deploy gated on Plan 03 code landing.

---
*Phase: 03-l2-orderbook-tracking-daemon*
*Completed: 2026-05-24*
