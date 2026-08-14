---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: market-perception
current_phase: 05
status: in_progress
stopped_at: A controlled staging-worker restart resumed the same real Structure window from its durable market cursor; markets remain in progress pending terminal materialization and shadow certification
last_updated: "2026-08-15T19:42:00Z"
progress:
  total_phases: 14
  completed_phases: 13
  total_plans: 72
  completed_plans: 71
  percent: 99
---

# M1 Perception — Current State

## Current Position

Phase: 05.6 (self-healing Structure production) — transactional control-plane foundation in progress

- **Staging credential containment:** API health is passing with an isolated
  replacement DSN, and recovery machine `48e3104c979578` is the only active
  source worker. It completed 210 events pages, transitioned the window to
  `events-complete`, and is traversing dependent market pages without a
  Structure or Quote pointer mutation. The original primary and its Fly
  standby are stopped because the old release is lease-stuck.
- **Worker-loss evidence:** controlled restart of the recovery worker at
  market page 319 left its successor runnable with no stale owner; the
  replacement process then committed through page 333. This is live staging
  proof that source recovery resumes from transactional cursor state rather
  than local process/SQLite state.
- **Alert-delivery gate:** Telegram is no longer an M1 deployment task.
  The isolated Fly alert app remains stopped because Fly cannot persist its
  secrets and a replacement app cannot be created; do not bypass this with
  production credentials or unencrypted machine configuration.

- **Current production:** Fly L1 remains release v358. The app service check
  reports `runtime:health_read_lane=read-model-unavailable`; public `/healthz`
  has timed out and the opportunity endpoint returned 503. This is an active
  P1, not a degraded success state.
- **Observed failure chain:** repeated SQLite writer contention, Quote
  `StaleQuoteRunError`, and 75-second snapshot child timeout
  (`failure_counter=114/5`). Resident Polywatch detects the failure set and
  Telegram delivery is recorded as `ok=True`.
- **Approved replacement path:** commits `8090e65`, `da96bb8`, `07aa9b7`,
  `0ccc335`, and `b4d10e0` add the additive Alembic 009 schema, fenced
  Postgres job repository, bounded SQLite source reader, and idempotent
  shadow-to-job projection. Real PostgreSQL tests prove repeated projection
  leaves `m1_publication_pointers` empty. No production migration, pointer
  switch, or Fly deploy has occurred.
- **Open gate:** add the operator CLI/read model, apply the additive migration
  to an explicitly designated production DB, execute shadow twice with stable
  counts and pointer-nonmutation proof, then migrate Structure and Quote one
  independently fenced worker at a time. Do not claim M1 production stability
  until the P1 is recovered and the control-plane read path remains available
  under write pressure.

Phase: 05 (WS /book + /prices 增量推送) — Plan 06 operational closure in progress

- **Plan 05-07 production rejection:** L2 release 73 / source `90d72aa…` is
  permanently rejected. At `08:04:13Z`, an unchanged promoter target missed
  four book dumps, compensated generation 1, and samples 11/12 persisted
  `10/10/8`; at `08:08:01Z`, quiet refresh missed two and compensated
  generation 2.
- **Corrected repair:** commit `92797cc` preserves a control-consistent socket
  on business-evidence timeout, reuses exact current-generation evidence for
  unchanged targets, and blocks durable sampling through reconnect
  convergence while keeping live strict health fail-fast. 232 focused tests,
  changed-file Ruff, and full pytest pass.
- **Current L2 production:** release 75 runs exact executable source
  `9f385cacc104fa54dd444151a8c4ecb423e94dde`, machine
  `85e647c4eed598`, instance `01KYES89KD9WA8VV9V2B3PJV7R`, boot
  `d029c2ea-e357-4ce2-8f7c-6c4e11867254`, and digest
  `sha256:f0d39892207577bb024995d76e91f5c0b8c0a88fd8e2839e182d25125da16ad5`.
  Two real promoter ticks and a quiet refresh passed 10/10/10 on generation 1.
- **Rejected continuity attempt:** manifest `3ad69a90…` remains immutably bound
  to `[2026-07-26T08:51:13.206077Z,2026-07-27T08:51:13.206077Z)`, but its
  first disallowed `subscription_control_failed` event occurred at
  `09:28:32.793437Z`; two more occurred at `09:51:14.697072Z` and
  `10:36:26.013847Z`. Its canonical T0 PASS remains true boundary evidence,
  but this 24-hour attempt can no longer produce a strict PASS.
- **Production condition:** release 75 is currently serving usable data and
  recovers after each event. Since T0, 215/215 health samples and 1,075/1,075
  market samples pass, membership stayed at least 10/10/10, and the largest
  observed sample gap was 60.948 seconds. Current resident-monitor state is
  empty and repeated exact probes are green.
- **Root cause:** a quiet-refresh initial dump can replay a book with the same
  venue timestamp. `l2_book_levels` correctly rejects the duplicate unique key,
  but `push_book_levels` treats that durable replay as a failed write.
  `record_book_evidence` consequently ignores the frame, the barrier times out,
  and retrying obtains a later timestamp and recovers. This idempotency gap must
  be repaired and deployed before binding a new continuity manifest.
- **Checkpoint automation:** the macOS `launchd` job still evaluates the
  immutable rejected manifest every five minutes. It must not be interpreted as
  an active PASS candidate; after the idempotency repair, closure requires a
  new release, boot, manifest, and T0.

- **Implemented:** canonical production Dashboard URL, four-surface Polywatch
  monitoring, repaired R2 bucket configuration, and the M1 continuous-operation
  learning/runbook are committed and pushed through `cfdef70`.
- **Production:** L1 runtime remains release
  `21acea5c286c8b7a9599933674c1bf570316e1c2`, machine
  `6830939c0070d8`, with the first post-restart complete quote run 78 at
  `2026-07-26T03:54:09.630Z`.
- **Interim quote evidence:** runs 78–133 are 56/56 complete, 1,278/1,278
  each, with zero failed/collecting/mismatched runs and maximum start gap
  121.271 seconds.
- **Primary monitoring:** the existing Fly `cron` machine
  `8e2909a77ddd08` now runs the unified watcher every two minutes. Production
  ticks at `05:40`, `05:42`, `05:44`, and `05:46` UTC all checked L1,
  opportunity feed, L2/L3, and Dashboard successfully; state advanced with an
  empty failure set and no Telegram noise. First failure-set changes alert
  immediately, duplicates are suppressed, unresolved failures remind after
  30 minutes, and recovery is sent once.
- **Fallback evidence:** GitHub's nominal `*/15` schedule had an actual
  `01:08:21Z → 04:43:55Z` gap, so it remains an independent provider fallback
  and is not treated as the timing SLA.
- **Deploy isolation:** only the Fly `cron` machine changed to image
  `deployment-01KYEEV529S3CP9TP742WV6WGQ`. The L1 app machine
  `6830939c0070d8`, instance `01KYE8X2AXK1PWN6VW1WVF2KRR`, image
  `deployment-01KYE8VB7PT2XT0N1XDY09P0P7`, and quote anchor remained
  unchanged.
- **Hard gate:** no final quote verdict before
  `2026-07-27T03:54:09.630Z`.
- **Authenticated Dashboard:** PASS at `2026-07-26T05:02:19.364Z` in the
  dedicated persistent M1 Edge instance. `/status`, `/candidates`, `/signals`,
  and one real `/l3/<asset_id>` route rendered application content and current
  production data.
- **Evidence discipline:** Phase 05.4 A7 is accepted for L3 continuity. No
  Plan 06 SUMMARY, Phase 05 validation signature, or roadmap completion may be
  created before the remaining exact quote gate passes.
- **Deploy guard:** L1 deploy now ignores docs/planning/Markdown-only pushes,
  preventing closure evidence commits from restarting the release-bound quote
  window.

Phase: 05.5 (production-opportunity-feed) — COMPLETE
Plan: 1 of 1 complete

- **Production result:** capacity run 1 fetched 1,278/1,278 books in 1.013
  seconds. Automatic run 2→3→4 then completed 1,278/1,278 at approximately
  121-second start intervals; every sampled opportunity request returned HTTP
  200 and quote/snapshot health remained pass.
- **Runtime identity proof:** L1 release 131 first exposed exact
  `releaseId=cb0ba9c54d79ed741f847c9db08ebeda098c5342`; run 6 completed
  immediately after this release and the feed remained HTTP 200. Subsequent L1
  releases inherit the workflow contract and expose their own exact SHA.
- **Safety boundary:** public CLOB reads plus local SQLite quote persistence
  only. No wallet, signing, orders, or real-money authorization.

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

- Make exact `l2_book_levels` replay idempotent without hiding conflicting
  payloads, add regression coverage, deploy a new L2 release, and bind a new
  immutable 24-hour continuity attempt.

- Persist Polywatch's last alert reason/timestamp so a Telegram
  `unhealthy → recovered` pair remains diagnosable after Fly's short log
  retention window.

- Reconcile strict evidence into legacy Phase 05 Plan 06 only after the new
  attempt and the independent quote continuity gate pass.

- Phase 05.5/H-009 is complete; use its production feed only as
  known-universe gross-before-fees discovery input.

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

- **Last session:** 2026-07-26 18:45 (Asia/Shanghai)
- **Stopped at:** release-75 alert/recovery diagnosis; the selected 24-hour
  attempt is permanently rejected although current L2 health has recovered.
- **Proceeding to:** TDD repair for replay-idempotent book-level persistence,
  followed by an L2-only deploy and a fresh immutable continuity attempt.

- **Resume files:** `05.4-SOAK-LOG.md`,
  `src/polyarb/storage/l2_supabase_mirror.py`, and
  `tests/m1-perception/test_l2_supabase_mirror_book_levels.py`.

## Accumulated Context

### Roadmap Evolution

- Phase 05.1 inserted after Phase 05: Durable L2 data-chain recovery (URGENT).
- Phase 05.4 inserted after completed Phase 05.3: Continuous L3 soak evidence
  (URGENT; blocks Phase 05 Plan 06 strict closure).
