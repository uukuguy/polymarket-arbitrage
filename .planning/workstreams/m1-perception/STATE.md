---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: market-perception
current_phase: 05.6
status: in_progress
stopped_at: all pre-production Fly control-plane apps were removed after they were proven to carry a deleted staging Supabase DSN; the only formal Supabase project is restarting and no business collector is running
last_updated: "2026-08-18T03:28:59Z"
progress:
  total_phases: 14
  completed_phases: 13
  total_plans: 72
  completed_plans: 71
  percent: 99
---

# M1 Perception — Current State

## Current Position

- **Authority reset in progress (2026-08-18):** the only remaining Supabase
  project is `polyarb` (`zoqsmjeejfkrokwttjbx`). It is in a provider-initiated
  restart after reporting unhealthy/recovery behavior. No 24-hour acceptance
  run exists or is running.

- **Pre-production control plane removed (2026-08-18):** the former
  `polyarb-control-api`, `polyarb-control-worker`, and
  `polyarb-control-alert` all inherited the same DSN for a deleted staging
  project (`pnclgqrxhmulmmmjsgbk`). The API probe returned that exact missing
  tenant; the worker held six stopped machines and pending stale secrets.
  They were deleted together, including all associated machines. The remaining
  Fly application is `polyarb-l2`, which is outside this reset. Monitoring will
  be redeployed from the single authoritative configuration only after the
  formal database passes a bounded probe.

- **Formal read-only incident repaired (2026-08-17 21:50Z):** `formal-cloud-v1`
  failed and remains immutable failed evidence after every Worker session inherited
  `default_transaction_read_only=on` from Supabase's `postgresql.auto.conf`.
  The formal database now has its own `default_transaction_read_only=off`
  override. A fresh connection wrote only a temporary table then rolled it back;
  the five business machines were restarted and resumed forward progress. Fresh
  cloud-only run `formal-cloud-v2` began at 21:52:12Z, appended its first sample
  at 21:53:01Z, and passed the 48-second/two-tick gate. Its 24-hour window is the
  only outstanding final acceptance gate.

- **Historical failed cloud run (2026-08-16):** immutable `formal-cloud-v1`
  evidence is retained solely to prove the read-only incident and its gap. It
  is not a qualifying acceptance window and must never be resumed or relabeled.

- **Latest live repair (2026-08-15 14:49Z):** a finished 1,000-range Structure
  certification was observed holding an expired 30-second epoch during its
  eight-minute R2 parity rebuild. Commit `9a491285` adds a same-owner/epoch
  heartbeat every lease-third between R2 parity segments; only the coordinator
  was rolled to image `m1-cert-heartbeat-amd64-9a491285`. The old epoch 114
  completed safely, expired leases returned to 6, and all five machines are
  started. The next full natural certification is the long-running live proof.
- **Strengthened 24-hour soak:** commit `3e940b54` creates immutable v2
  evidence that additionally requires the cumulative succeeded-job count to
  rise across the window. A fresh baseline at `14:38:53Z` had 20,783 successes;
  its first independently scheduled sample at `14:49:00Z` had 21,364 with
  API available and counters at 6/74. v1 evidence is retained unchanged.
- **Scoped alert diagnosis (2026-08-15 15:08Z):** the isolated alert app now
  has a deployed DSN plus Telegram secrets and can claim/write its scoped
  outbox, but the configured bot token returns Telegram `401 Unauthorized`.
  The worker was stopped after bounded retries; replace that token before the
  dashboard/Telegram receipt gate can close. Collection remains independent.
- **Scoped alert acceptance (2026-08-15 15:23Z):** the app received the
  already-working L1 Polywatch token through a non-printing Fly-to-Fly secret
  transfer. Its exact recovery outbox now has `dashboard-visible` and
  `telegram:7882` receipts; the scoped worker was stopped after success.
- **Production authority audit (2026-08-15 15:31Z):** old L1's separate
  Supabase pooler fails bounded control-plane preflight and is not used by its
  Python image. A fresh reachable Postgres/R2 authority is now the explicit
  prerequisite for the production lane; staging cannot be relabeled as it.

Phase: 05.6 (self-healing Structure production) — transactional control-plane foundation in progress

- **Latest live acceptance (2026-08-15 14:00Z):** coordinator plus two Structure and two Quote
  pools are all `started`; the independent control API is `available`. In the immediately preceding
  thirty minutes Postgres recorded 147 successful `structure-fetch`, 1,159 successful
  `structure-normalize`, 135 successful `quote-batch`, and a successful `quote-certify` job. This
  is continuous durable work, not a liveness-only claim. Structure unfinished is 1,255 and falling;
  Quote unfinished is zero.
- **Real R2 process-loss evidence is complete for both job classes:** the new Quote proof crashed
  `…batch:69` after R2 verification; the alternate pool finished it with `attempt_count=2`,
  `lease_epoch=2` and exactly one receipt. The five-role topology was restored to normal commands.
  Evidence: `phases/05.6-self-healing-structure-production/evidence/`.
- **24-hour soak is active:** `com.polyarb.m1-transactional-soak-sampler` is a named user
  LaunchAgent that exits after each read-only 600-second sample. Its verifier rejects a sample gap,
  API/machine identity drift, or new expired lease/circuit relative to its baseline. It must run
  uninterrupted before the staging readiness claim can close.

- **Current staging acceptance:** machine `48e3104c979578` is 2048MB and runs
  image `m1-retry-outcome-f464d3db` with `--max-turns 8`,
  `--structure-materializer-turns 8`, `--structure-range-turns 32`, and a
  two-second interval. The extra turns are serial and lease-fenced; this is
  staging-only, and production L1/L2 remain out of scope.
- **Recovered source anomaly:** Gamma event `497034` was a real active standard
  neg-risk event with `negRiskMarketID=null`; child market `2290078` repeated
  `negRisk=true` without a group. v3 had omitted the established snapshot
  quarantine rule, so every source window stopped at page 20. Commit
  `1757a406` excludes only that unprovable child shape without inferring a
  group or weakening the truth validator. After deployment window `5955894`
  advanced from `shard-batch:00000019` to `00000027`; no new bundle or
  publication was created by that incomplete window.
- **Prior v3 chain completed:** source window `5955841` completed, certified
  Structure generation `structure:dcaedf…`, and admitted/certified its Quote
  generation. `quote:current` is the isolated staging transactional pointer;
  no production pointer was mutated.
- **Manifest recovery proof:** the final v3 admission initially exercised its
  retryable circuit because the Postgres allow-list omitted the new source kind.
  Image `m1-sharded-admission-fbdc2f42` then naturally reclaimed the preserved
  52 batch receipts and atomically committed bundle
  `dcaedf577134a31291c257656f31b58ec4312d8889e2d5e854d82b846a7415fd`.
  It enqueued 1,016 named range jobs with zero pointer mutation. Certification
  has correctly stayed retryable until those ranges finish; it has not certified
  a partial generation.
- **Restart proof:** staging worker `48e3104c979578` received a controlled
  requested restart (Fly reports `oom_killed=false`). It returned on the same
  image/configuration and range receipts increased from 13 to 26 without any
  local-state restore or pointer mutation. A precise in-flight lease takeover
  remains to be captured when a longer-running downstream job is observable.
- **Waiting classification repair:** previous certifier attempts correctly
  rejected partial receipts but wrongly created an incident circuit. Commit
  `ac5cca4b`, deployed as `m1-structure-wait-ac5cca4b`, mirrors Quote
  certification: `IncompleteStructureGenerationError` is a five-second
  fenced wait with no incident. Its first live result must wait for the prior
  circuit's already-persisted next-probe timestamp; do not edit the database
  to shortcut that recovery proof.
- **Throughput repair:** `8ba1ea0d`/`d7324952` add the bounded default-zero
  Structure range turn budget. Staging runs budget eight: each tick retains
  the original eight workers, then performs at most eight extra serial,
  leased Structure-range turns. This is not a concurrency increase.
- **Materializer recovery and throughput repair:** `4eac577f` makes a current
  lease's durable checkpoint sufficient to resolve its old retry circuit and
  incident, then `9c013ae9` adds eight default-zero-compatible serial
  materializer turns. Immediately after deployment source materializers moved
  from 59/67 to 71/75 shard-page checkpoints, while open circuits declined
  from 82 to 74 through newly recorded recovery events. No job state, receipt,
  or pointer was manually changed.
- **Restart boundary:** a controlled staging restart was issued after observing
  materializer epoch 29 at checkpoint 79. It had already checkpointed before
  the requested stop; epoch 30 continued from 83 to 87. This is clean recovery
  only, not the required active-lease/R2-upload-before-receipt takeover proof.
- **Prepared takeover boundary:** image `m1-r2-takeover-fault-cc43eb2c`
  contains a default-disabled exact-job Structure/Quote crash hook. It requires
  the target plus literal staging acknowledgement and stops only after verified
  R2 upload before receipt. The deployed command omits both arguments until a
  fresh staging range/Quote job is ready; its normal 8/8/8 serial budgets are
  otherwise unchanged.
- **Real R2 takeover proof:** fresh bundle `c20e9bf…b656` admitted a new
  Structure generation. The exact-key staging hook crashed range
  `event_tags:115` after R2 verification and before receipt; Fly reports
  `exit_code=130, requested_stop=false`. Epoch 1 left no receipt; after lease
  expiry epoch 2 committed exactly one receipt for content-addressed artifact
  `51bdef…bea3` (425 rows). The normal 8/16/8 command is restored. This proves
  fenced takeover and exactly-once receipt, not avoidance of an idempotent R2
  overwrite on retry. Quote-batch proof and continuous soak remain open.
- **Current drain configuration:** staging now runs 8 base / 8 materializer /
  32 Structure-range serial turns to keep source collection moving while this
  1,014-range generation drains. This is a staging capacity rebalance, not a
  concurrency or production change. Quote admission remains the next gate.

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
- **Large-window capacity:** the active Gamma traversal grew beyond 79,000
  market rows. Before its all-page R2 materialization, the staging-only worker
  was increased from 512MB to 1024MB and proved cursor recovery through page
  794 with the same image/command and no pointer mutation. Resource sizing for
  production remains a later evidence-based decision.
- **Source-page ceiling:** the legacy worker continued through market ordinal
  1036, exposing that the old assumed source cardinality was false. Commit
  `9cbf090` was deployed only to the isolated staging worker as image
  `m1-source-page-ceiling-9cbf090`; its first over-limit job (`markets:1037`)
  was durably quarantined before a Gamma/R2 fetch. The next 300-second bucket
  then admitted a new fenced window from `events:0`. This is live evidence for
  fail-closed containment and recovery, not evidence that a full Structure
  shadow bundle is ready.
- **Controlled pause:** once that recovery proof was captured, the isolated
  staging machine was stopped. Leaving it running would repeatedly start a
  new 1,000-page-then-quarantine window every cadence interval, consuming
  staging resources without producing a certifiable bundle. Its database/R2
  evidence and guarded image are retained for the scoped-source restart.
- **Scoped-source restart:** Alembic revision `015` was applied inside the
  isolated Fly machine using its existing secret environment, then the machine
  was restored to its normal 1024MB eight-turn/two-second command on image
  `m1-event-rooted-source-e7923ce`. It resumed durable event jobs through
  ordinals 22–27 without a schema failure or pointer mutation. The event stream
  must still seal before exact-ID market batches, materialization, and shadow
  certification can begin.
- **Retry-circuit recovery evidence:** controlled Structure retry injection
  reached three failures and an open circuit, then the restored normal worker
  completed its half-open probe and closed the circuit. Recovery created two
  acceptance-scoped outbox intents (dashboard and Telegram) without selecting
  the 1,670 historical pending messages. The finite-fault implementation
  returns a durable retryable outcome instead of restarting the scheduler.
- **Alert-delivery gate:** the isolated alert app has no current DB credential.
  Local historical DSNs fail connection tests, while the worker app's current
  DSN remains correctly contained in Fly. Fly's worker-machine write endpoint
  currently rejects the stopped one-off machine as unauthorized, so do not
  bypass the boundary by copying secrets or bulk-replaying outbox rows.

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

## Active Transactional Structure Override — 2026-08-15

- Staging worker `polyarb-control-worker-staging` / machine
  `48e3104c979578` now runs image `m1-retryable-service-407daed` at 1024MB.
- `5955818` sealed 211 event pages into 6,419 exact-ID market batches. Its
  event terminal receipt succeeded after a fenced takeover; the market receipt
  set is actively draining through eight bounded lanes, with ordinals beyond
  1,600 already successful and zero pointer mutation.
- The deployed worker has a 10,000 market-batch bound, eight source lanes, and
  a 90-second terminal artifact-read bound, plus a 105-second async scheduler
  turn bound. The 1,000-page ceiling applies only to opaque event pagination;
  quarantined-window source jobs cannot be claimed ahead of a fresh source
  window; durable source failures return as retryable outcomes rather than
  exiting the service. Do not touch Telegram or production L1/L2.
- A due retryable job is claimed ahead of new runnable work. This prevents a
  long market batch set from starving a backoff-complete source failure; the
  job still keeps its failure class, retry timing, lease epoch, and incident
  evidence. `m1-retry-fairness-52c0e1f` is the staging-only deployment that
  proved 32 reclaimed second attempts while the process remained online.
- Exact frozen market batches are fail-closed at the window boundary:
  `m1-bound-integrity-3e92d64` allows two integrity retries, then quarantines
  the full window on the third failure. Staging `5955818` demonstrated the
  actual recovery chain (quarantine with preserved evidence → scheduler admits
  fresh `5955836`), still with zero publication pointers.
- Resume from `make planning-status`, then inspect `5955818` to prove all
  market receipts, terminal materialization, and shadow recovery.

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
