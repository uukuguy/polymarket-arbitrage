---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: market-perception
current_phase: 05
status: ready
stopped_at: Phase 05.4 COMPLETE; Phase 05 Plan 06 reconciliation is next
last_updated: "2026-07-25T23:35:16Z"
progress:
  total_phases: 13
  completed_phases: 12
  total_plans: 71
  completed_plans: 70
  percent: 99
---

# M1 Perception — Current State

## Current Position

Phase: 05.4 (continuous-l3-soak-evidence) — COMPLETE
Plan: 5 of 5 complete

- **Phase:** 05.4 — Continuous L3 soak evidence, completed 2026-07-26
- **Plan:** 05.4-05 complete — all nine tasks and the final mechanical verifier passed.

- **Final PASS:** A7 exact window
  `[2026-07-24T16:43:01.704189Z,2026-07-25T16:43:01.704189Z)` contains one
  boot, 2,880 passing health rows, 14,400 passing market rows, and 288/288
  successful promoter ticks. All five immutable checkpoint reports share the
  manifest/soak identity; the independent final re-query reproduced raw-row
  hash `d4fc7567…` and report hash `dc2d4d7e…`.

- **Historical execution notes:** Production project `zoqsmjeejfkrokwttjbx` is Alembic 007.
  Dedicated `polyarb_l2_runtime_054` and `polyarb_l3_retention_054` credentials
  pass disjoint least-privilege proofs; Fly contains only the runtime DSN and
  the owner/retention credentials remain absent. Current production is exact
  source `64df08e…`, Fly release 70, digest `sha256:81849c56…`, instance
  `01KYACS3…`, and DB boot `cd04e515…`; it is diagnostic-only after A6
  invalidation.
  A1–A4 are immutable rejected evidence. A5 manifest `95814bf1…` was
  O_EXCL-created and bound exactly once before T0. Its exact scheduled T0
  `2026-07-24T12:43:29.274117Z` and canonical report `adbbbc4f…` passed.
  A later read-only audit found seq 201/216/217 at only 7/2/7 evidenced tokens,
  so A5 is permanently NOT-CLOSED. Its T+6 runner was cancelled and none of
  T6/T12/T18/T24 exists. Root cause is the missing Polymarket-required text
  `PING` heartbeat: protocol-level WebSocket Ping is not the application
  heartbeat. Repair, exact-SHA deployment, new boot/readiness, and a unique A6
  are required without changing strict thresholds. The RED/GREEN repair is
  committed at executable candidate `91359610242a52e62b336be41a4540a441cf7191`;
  focused/full tests, live text-PONG probe, changed-file lint, compile, image,
  docs, and planning gates passed. Exact SHA `9ce640e…` then deployed as Fly
  release 68/boot `e542fd4c…`, but that boot is permanently rejected: promoter
  run 0 began before WS generation 1 initialized and persisted
  `failed/generation_changed`, followed by 10/0/0 samples. A one-time promoter
  startup-connection gate was deployed in exact SHA `64df08e…` as release 70,
  boot `cd04e515…`. Run 0 and run 1 both passed 5/10/10; readiness then passed
  on 12 complete samples over 330 seconds with max gap 30.1 seconds and zero
  disallowed events. Unique A6 is bound for T0
  `2026-07-24T15:56:21.369231Z`; its exact T0 report passed with report hash
  `7549fa06…`, but seq 35 later failed at 10/10/8. A6 is permanently
  NOT-CLOSED; its T+6 runner is cancelled and later files do not exist.
  Root cause is destructive quiet-refresh timeout compensation creating a
  self-sustaining generation loop. Candidate
  `3be6ef6a8ceed8517020506291d474c13a6f6bc0` now gives new generations an
  initial convergence interval, performs a two-stage missing-only retry, and
  preserves a healthy socket on business-evidence timeout while retaining
  compensation for genuine control ambiguity. Forty transaction tests, 209
  focused L2/L3 tests, the full repository suite, Ruff, compile, docs,
  planning, image, and diff gates passed. It is not production evidence until
  pushed and deployed as one clean exact SHA. Exact SHA `6471d41…` now runs as
  release 72/instance `01KYAFJ…`/boot `9eeab4d5…`, with Fly/DB/GitHub identity
  matched. Two successful quiet-cycle endpoints, generation 1, two successful
  promoter rows, and 12/12 complete samples over 330 seconds passed readiness.
  Unique A7 manifest `0f6e2ffe…` was bound once before exact T0
  `2026-07-24T16:43:01.704189Z`; its declared T0 report passed with report hash
  `15a16e15…`, raw-row hash `1096275e…`, 10/10/10, five markets, and zero
  runtime events.

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

- Reconcile the completed Phase 05.4 strict evidence into legacy Phase 05 Plan
  06, including its dashboard smoke and final Phase 05 validation/closure.

- Keep H-009 pending until separately authorized production
  deployment/scheduling and timestamped capacity evidence.

- Do not run retention cleanup, production chaos, or trading as part of Phase
  05 closure.

## Required Reading

1. `.planning/CURRENT.md` — cross-workstream operational truth.
2. `.planning/workstreams/m1-perception/phases/05.4-continuous-l3-soak-evidence/05.4-CONTEXT.md` — locked decisions.
3. `.planning/workstreams/m1-perception/phases/05.4-continuous-l3-soak-evidence/05.4-05-SUMMARY.md` — completed production qualification.
4. `.planning/workstreams/m1-perception/phases/05.4-continuous-l3-soak-evidence/05.4-SOAK-MANIFEST-20260724T164301Z.json` — selected immutable A7 contract.
5. `.planning/workstreams/m1-perception/phases/05.4-continuous-l3-soak-evidence/05.4-SOAK-LOG.md` — authoritative execution timeline.
6. `.planning/threads/market-observation-architecture.md` §1.6 and §2.9–2.12 — chain-truth and observation cadence.

## Resume

```bash
/gsd-resume-work --ws m1-perception
```

## Session Continuity

- **Last session:** 2026-07-26 07:35 (Asia/Shanghai)
- **Stopped at:** Phase 05.4 A7 final PASS and planning closure.
- **Proceeding to:** Phase 05 Plan 06 reconciliation and dashboard/Phase 05
  closure, reusing—not re-running—the stricter continuous evidence.

- **Resume files:** `05.4-05-SUMMARY.md`, `05.4-SOAK-LOG.md`, and
  `../05-ws-book-prices/05-06-PLAN.md`.

## Accumulated Context

### Roadmap Evolution

- Phase 05.1 inserted after Phase 05: Durable L2 data-chain recovery (URGENT).
- Phase 05.4 inserted after completed Phase 05.3: Continuous L3 soak evidence
  (URGENT; blocks Phase 05 Plan 06 strict closure).
