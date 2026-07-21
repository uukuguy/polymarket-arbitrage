---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: market-perception
current_phase: 05
status: paused_after_soak_window_evidence_incomplete
stopped_at: Formal window elapsed; T+6/T+12/T+18 were missed and T+24 was unobserved at handoff, so this run cannot PASS
last_updated: "2026-07-21T22:01:34+08:00"
last_activity: 2026-07-21
progress:
  total_phases: 12
  completed_phases: 10
  total_plans: 61
  completed_plans: 60
  percent: 98
---

# M1 Perception — Current State

## Current Position

Phase: 05 (ws-book-prices) — PAUSED
Plan: 6 of 6

- **Phase:** 05 — WS book prices
- **Plan:** 05-06 — production L3 proof and 24-hour soak
- **Status:** Plan 05-06 Task 2 is NOT-CLOSED due to incomplete observation
  evidence. T+0 was valid, but T+6/T+12/T+18 were missed and T+24 remained
  unobserved at the `2026-07-21T14:01:34Z` handoff. A late diagnostic cannot
  backfill the missing minimum-throughout-window samples; a new re-soak is needed.
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

## CURRENT-CALL — Phase 05 production prerequisite passed

Production Alembic is revision `006 (head)`. L1 release 127 produced snapshot
578 with 1946 mirrored markets and `no_token_id` populated for 1946/1946 rows.
L2 release 37 runs image `deployment-01KXZJHS9QT8T6X0J33KVPTB5V`, digest
`sha256:5da8e954897f60cf05f9d6664e99a15247d46a2bd4fd0edbb433c200af8b412c`,
on machine `85e647c4eed598`, instance `01KXZJKY9SKEJAY2DD8MMPNB2E`.

The rollout exposed and repaired three production-only observation bugs without
changing locked thresholds:

- Polymarket book arrays were farthest-first; index zero produced false spreads
  near 0.998. BUY/SELL levels are now price-ranked before TOB, depth, and full
  book projection (`7ccd2da`).
- The promoter handed repeated time-series rows to a five-row recipe limit. It
  now retains the newest row per asset before selection (`57d3fc0`).
- After release 36 subscribed both outcomes, a high-depth No row consumed a
  Yes-market recipe slot and the second tick fell `10→8`. The recipe input now
  resolves authoritative `yes_token_id` identity before LIMIT (`9451b4b`).

Release 37 started at 10/10, then its `11:00:20Z` second real promoter tick logged
`+0 -0 markets=5 tokens=10`. The `11:00:50Z` health read remained 10/10 with
promote age `30.0s`, book-level write age `59.3s`, cursor lag `0`, and 108 total
subscriptions. Production DB proof after the release showed five promoted Yes
markets, ten complete Yes/No tokens, and 280 real `l2_book_levels` rows over
eight immediately active tokens. The prerequisite is passed across the feedback
tick; this is not yet the 24-hour stability verdict.

Formal T+0 was established at `2026-07-20T13:30:55Z` from one new forced-machine
response after re-resolving the post-promoter mapping. Exact release 37 identity
was unchanged; HTTP was 200; active was 10/10 pass; promoter/book ages were
20.0s/19.9s pass; WS, mirror, candidates, listener, reconciliation, and cursor
main-chain checks were green. The current five Yes identities map to five No
pairs across markets `540819`, `562802`, `565064`, `601819`, and `665374`.
Initial exact-window SQL and zero-stale watchdog evidence are recorded in
`05-SOAK-LOG.md`; they are boundary evidence, not the T+24 verdict.

A fresh handoff read at `11:52:11Z` confirmed the same release, machine,
instance, image, and digest: HTTP 200, active 10/10, promote age `107.7s`,
book-write age `4.6s`, cursor lag `0`, and 108 subscriptions. Direct SQL from
the release start returned 3840 book rows across ten distinct token asset IDs;
all five promoted Yes markets had OHLC. A capped newest-1000 REST page exposed
only four hot assets, so soak coverage must use interval-scoped SQL aggregates.

## Remaining Work

- Preserve the completed formal window as NOT-CLOSED evidence. First capture a
  labelled late read-only exact-instance/SQL/watchdog diagnostic, then start a
  new 24-hour re-soak with retained T+0/T+6/T+12/T+18/T+24 checkpoints.
- Preserve strict N=5 and the unchanged spread/depth/recency thresholds.
- Keep H-009 pending until separately authorized production deployment/scheduling
  and timestamped capacity evidence; Phase 05.1 completion does not promote it.

## Required Reading

1. `.planning/CURRENT.md` — cross-workstream operational truth.
2. `.planning/workstreams/m1-perception/phases/05-ws-book-prices/05-06-PLAN.md` — current acceptance gate.
3. `.planning/workstreams/m1-perception/phases/05.1-durable-l2-data-chain-recovery/05.1-LEARNINGS.md` — closed gap-phase lessons.
4. `.planning/threads/market-observation-architecture.md` §1.6 — chain-truth discipline.

## Resume

```bash
/gsd-resume-work --ws m1-perception
```

## Session Continuity

- **Last session:** 2026-07-21 22:01 (Asia/Shanghai)
- **Stopped at:** Formal window ended at `2026-07-21T13:30:55Z`; T+6/T+12/T+18
  were missed and T+24 was unobserved at handoff, so the run cannot satisfy the
  strict minimum-throughout-window gate.
- **Proceeding to:** Capture a labelled late read-only snapshot plus exact-window
  SQL/watchdog evidence, render NOT-CLOSED, and begin a new re-soak without
  backfilling missing checkpoints.
- **Resume file:** `.planning/workstreams/m1-perception/phases/05-ws-book-prices/.continue-here.md`

## Accumulated Context

### Roadmap Evolution

- Phase 05.1 inserted after Phase 05: Durable L2 data-chain recovery (URGENT).
