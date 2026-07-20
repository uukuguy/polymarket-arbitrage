---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: market-perception
current_phase: 05
status: executing
stopped_at: Phase 05 Plan 06; L3 0/10 root-caused to candidate seed starvation plus invalid token-map schema assumptions
last_updated: "2026-07-20T15:10:00+08:00"
last_activity: 2026-07-20
progress:
  total_phases: 11
  completed_phases: 9
  total_plans: 57
  completed_plans: 56
  percent: 98
---

# M1 Perception — Current State

## Current Position

- **Phase:** 05 — WS book/prices and L3 promotion
- **Plan:** 05-06 — strict N=5 / 10-token production soak
- **Status:** Phase 05.1 is complete; L2 strict health is HTTP 200, but L3 remains `0/10`, so the 24h soak has not started
- **Active workstream:** `m1-perception`

## VERIFIED — 2026-07-18 production facts

- L1 recovered from a full 5GB Fly volume after extension to 15GB and bounded
  retention. Fresh snapshot, Supabase mirror, and M2 opportunity feed are working.

- L2 was stale for about 40 days because L1 event publishing was disabled and a dead
  asyncpg LISTEN connection could not wake the documented reconnect loop.

- Enabled `POLYARB_EVENT_BUS_ENABLED=1` on L1 and restarted the single L2 machine.
- L2 startup catch-up advanced cursor `482 → 516`; candidate, WS, and mirror ages
  returned to seconds; strict `/health` returned HTTP 200 and fresh TOB rows appeared.

- The pre-repair projection drift was 117 active database assets versus 2 WS
  assets. Plan 03 production proof confirms it is now resolved at 5 desired
  keys, 5 active keys, and 5 WS assets with zero stale/missing keys.

- `l3:active_count = 0/10`; Phase 05 Plan 06 soak remains blocked until Plan 04
  restores strict quiet-market freshness and closes Phase 05.1.

- No local polyarb/pytest/flyctl/uvicorn workflow process remains running.
- Phase 05.1 Plan 03 deployed and proved LISTEN reconnect `0 → 1`, poll-only
  cursor `520 → 521` with an exact unchanged notification anchor, L1 publishing
  restoration, and candidate/WS projection `5/5` without restarting L2.

- Final strict `/health` remains HTTP 503 because five valid subscribed markets
  were quiet: WS business-frame age and TOB mirror age exceeded their unchanged
  thresholds while socket pong liveness, cursor, reconciliation, and projection
  were healthy. This is Plan 04's blocker, not a core recovery failure.

- Plan 04 reconnect-gating code `889fab4` is deployed as image
  `deployment-01KXSMPKM78105Q9JZG58GBZYP`, digest
  `sha256:ec90d98e20c6ffe7ee48c899c939dab7a67addf45c28adda6695d13ed6269c4d`,
  on machine `85e647c4eed598`, instance `01KXSMS80B5AX2FGT5EPRC6V82`.

- That exact instance passed the >=180-second startup/candidate/WS/mirror main
  chain. During the following ten-minute read-only monitor, organic business
  frames kept `ws_age < 45s`; no natural 60-second quiet trigger occurred.

- A later credential-free read-only window from `07:25:44Z` to `07:36:20Z`
  collected 60 HTTP-200 samples against machine `85e647c4eed598`. Maximum
  WS/mirror/reconciliation/candidate ages were `3.2s/74.2s/60.1s/60.2s`;
  listener stayed `listening`, cursor lag `0`, WS assets `5`, and L3 `0/10`.
  Exact instance incarnation and quiet-refresh logs were unavailable without
  Fly read-only authentication, and no token was created.

- Authenticated monitoring then confirmed the exact same machine, instance,
  creation/start anchor, image digest, and release before and after a 72-sample
  `07:53:56Z`–`08:07:28Z` window. All probes were HTTP 200; max
  WS/mirror ages were `3.0s/84.8s`, and exact quiet log counts were
  `sending/evidenced/failed = 0/0/0`. Only ordinary mirror writes occurred.

- A subsequent log search (`08:07:30Z`–`08:13:39Z`) and 60-sample live window
  (`08:15:34Z`–`08:26:37Z`) again found quiet counts `0/0/0`. All live
  probes were HTTP 200, max WS age was `2.6s`, and exact identity remained unchanged.

- Therefore Plan 04 Task 3 is unproven, not protocol-failed. If the instance
  changes, rebuild the >=180-second baseline before accepting later quiet proof.

- Exact image matrix: present `kill/which/curl/python`; missing
  `pkill/ps/dig/ping`. Temporary Fly organization token was revoked and no token
  value was retained.

- 2026-07-20 authenticated read-only recheck confirmed the exact same machine,
  instance, start anchor, release, image, and digest. Rolling logs from
  `04:25:40Z` to `04:51:36Z` contained quiet `sending/evidenced/failed=0/0/0`,
  received-book debug `0`, and TOB/trade mirror success `0/0`.

- The forced-instance strict `/health` at `04:52:31Z` returned HTTP 503:
  WS age `0.2s`, WS assets `3`, mirror age `6306.2s`, listener `listening`,
  cursor lag `0`, reconciliation age `54.3s`, candidate age `54.4s`, and L3
  `0/10`. The first broken link is now mirror freshness, so another short
  quiet-window monitor would not establish Task 3.

- At `05:32:41.823Z` the unchanged instance naturally entered quiet refresh;
  Supabase accepted `l2_top_of_book` at `05:32:42.567Z` and the same-generation
  waiter logged `evidenced` at `05:32:42.684Z`.

- The subsequent `05:34:29Z–05:37:47Z` 198-second acceptance window returned
  strict HTTP 200 for 10/10 samples. WS age max `50.1s`, mirror age max `162.2s`,
  cursor lag `0`, listener `listening`, and machine/instance/image anchors were
  unchanged. L3 stayed `0/10` and remains Phase 05 Plan 06's strict gate.

- Pre-regression verification: Phase 05.1/quiet surface `139 passed`; focused Ruff,
  compile, climb adapter, M1 manual contract, and planning checks passed. Exact
  image matrix remains present `kill/which/curl/python`, missing
  `pkill/ps/dig/ping`; temporary registry credentials were removed.

- A fresh strict probe at `05:51:57Z` invalidated closure: HTTP 503, WS age
  `613.5s`, mirror age `834.3s`, candidate set `0`, cursor lag `0`. L2 logs show
  a successful empty `markets_latest` fetch immediately before candidate `3→0`.

## CURRENT-CALL — Phase 05 strict L3 gate

Phase 05.1 is closed. Snapshot 574 naturally restored 1942 `markets_latest`
rows; the unchanged L2 instance restored three candidates, cursor lag zero, and
strict HTTP 200. The final current-chain window ran for 258 seconds with 10/10
HTTP 200 responses, and a real TOB write reset mirror freshness after the window.
The local empty-projection guards remain undeployed defense-in-depth; the actual
production mutation source was the local test suite, which is now isolated from
all external adapters by default.

The next first broken link is L3 promotion. Read-only evidence shows:

- only three active L2 candidates, all `near-end` markets from one event;
- recent TOB spread is about `0.998` for two and incomplete for one, so the
  locked L3 recipe matches zero rows despite two `depth_yes_usd > 500` rows;
- `markets_latest` has 1942 rows, including 598 mid-band and 583 mid-band plus
  liquidity>=500 rows, so the source universe is not intrinsically starved;
- production schema contains `market_id` and `yes_token_id` but not `asset_id`
  or `no_token_id`; `_fetch_market_token_map` queries both missing columns.

Therefore the current 10-token contract is structurally unreachable even after
candidate diversity improves. A design approval is required before changing
the schema contract and candidate seed behavior.

## Remaining Work

- Approve a narrow design for two prerequisites: durable Yes/No token mapping
  and a bounded mid-market L2 seed recipe that supplies L3-observable books.
- Do not start the 24-hour soak until active count is exactly 10 tokens and the
  other Plan 06 prerequisites are present.
- Production deploy remains outside the climb adapter's authorization boundary;
  preserve strict N=5 rather than changing thresholds to manufacture readiness.
- Keep H-009 pending until separately authorized production deployment/scheduling
  and timestamped capacity evidence; Phase 05.1 completion does not promote it.

## Required Reading

1. `.planning/CURRENT.md` — cross-workstream operational truth.
2. `.planning/workstreams/m1-perception/phases/05-ws-book-prices/05-06-PLAN.md` — current acceptance gate.
3. `.planning/workstreams/m1-perception/phases/05.1-durable-l2-data-chain-recovery/05.1-LEARNINGS.md` — closed gap-phase lessons.
4. `.planning/threads/market-observation-architecture.md` §1.6 — chain-truth discipline.

## Resume

```bash
$gsd-execute-phase 05 --ws m1-perception
```

## Session Continuity

- **Last session:** 2026-07-20 15:10 (Asia/Shanghai)
- **Stopped at:** L3 prerequisite diagnosis complete; schema and candidate-seed design approval required.
- **Proceeding to:** Write the approved design/spec, then TDD the local repair; stop again before production migration/deploy.
- **Resume file:** `.planning/workstreams/m1-perception/phases/05-ws-book-prices/05-06-PLAN.md`

## Accumulated Context

### Roadmap Evolution

- Phase 05.1 inserted after Phase 05: Durable L2 data-chain recovery (URGENT).
