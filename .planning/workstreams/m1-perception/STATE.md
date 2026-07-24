---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: market-perception
current_phase: 05.4
status: executing
stopped_at: Phase 05.4 Plan 05 A4 NOT-CLOSED and first repair boot rejected; verified sampler startup gate awaiting exact-SHA deployment
last_updated: "2026-07-24T11:51:00Z"
progress:
  total_phases: 13
  completed_phases: 11
  total_plans: 71
  completed_plans: 69
  percent: 97
---

# M1 Perception — Current State

## Current Position

Phase: 05.4 (continuous-l3-soak-evidence) — EXECUTING
Plan: 5 of 5

- **Phase:** 05.4 — Continuous L3 soak evidence
- **Plan:** next is 05.4-05 — non-autonomous production migration, credentials,
  deploy, manifest/T0, and distinct checkpoint gates

- **Status:** Waves 1–4 are complete on main. Plan 05 Task 1's local-only
  release-candidate gate is complete at `03313a9`; production cross-checks then
  found and repaired runtime-health identity, release-ID, boot-grid timing,
  stable sampler cuts, manifest mapping enforcement, T0 coverage scope, and
  per-L3 freshness chain defects. A read-only audit then invalidated A4 before
  T+6: the sampler treated minute-truncated OHLC bucket labels as observation
  timestamps, adding up to 60 seconds of artificial age. The verified repair
  reads the latest non-null source observation from `l2_top_of_book` while
  retaining `l2_ohlc_1m` for cumulative coverage. The last deployed source is
  `aaba91a`, bound to AcceptanceConfig digest `c9269392…`. The exposed owner
  password was rotated through the exact Supabase project and the replacement
  is direct-TLS verified. Production migrated exactly once from 006 to 007.
  Dedicated `polyarb_l2_runtime_054` and `polyarb_l3_retention_054` logins pass
  their disjoint capability proofs. The runtime DSN is staged in Fly, the owner
  DSN is absent from the Fly inventory, and the retention DSN exists only in
  macOS Keychain. Workflow run `30088360806`, image digest `4ce6d293…`, Fly
  instance `01KY9WX11REQC6SDZ0YK94J8FR`, and DB boot `70ed099f…` have exact
  SHA equality. Readiness passed and A4 manifest `2d29a839…` has an immutable
  PASS T0 at `2026-07-24T11:21:46.353847Z`, but health seq 34 permanently
  invalidated that window; no later A4 checkpoint may run. Next is exact-SHA
  deployment/readiness and an attempt-unique A5 manifest/T0. The first OHLC
  repair deployment (`7c01461`, boot `ba6630c2…`) was also rejected before
  readiness because sampler seq 0 raced ahead of the first promoter and emitted
  `evidence_writer_failed`. A RED/GREEN startup gate now skips only slots before
  desired membership reaches the exact ten-token input; all failures after that
  point remain strict.
  The runtime separates desired,
  control-committed, and current-generation evidenced membership; depth refresh uses an
  all-token barrier; promoter outcomes are terminal, durable, and retry-safe; and
  all direct PostgreSQL runtime paths use `POLYARB_L2_RUNTIME_DB_DSN`. Plan 03
  adds atomic five-market samples, durable runtime events, four public strict
  health chains, generation-bound book freshness, causal writer recovery, and
  cancellation-safe producer-before-writer shutdown. Plan 04 adds exact
  boot-grid verdicts, immutable manifest/five-report/raw-row hashes, credential
  and revision proofs, local full-chain chaos, and manual-only deploy gating.
  The owner/migration DSN remains Alembic-only. Plan 05 remains
  `autonomous: false` with separate migration, runtime credential, retention
  credential, deployment, manifest/T0, and wall-clock approvals.

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

- Do not start Phase 05.4 Plan 05 without its exact approval sequence. It is the
  only remaining plan and contains production migration, runtime/retention
  credentials, secrets, manual deployment, restart/readiness, manifest binding,
  and T0/T+6/T+12/T+18/T+24 gates.

- Do not resume the release-37 T+6/T+12/T+18/T+24 sequence as strict evidence.
- Preserve strict N=5 and the unchanged spread/depth/recency thresholds. Start a
  fresh exact-identity 24-hour soak only after separately authorized production
  migration/deployment and readiness proof.

- Keep H-009 pending until separately authorized production deployment/scheduling
  and timestamped capacity evidence; Phase 05.1 completion does not promote it.

## Required Reading

1. `.planning/CURRENT.md` — cross-workstream operational truth.
2. `.planning/workstreams/m1-perception/phases/05.4-continuous-l3-soak-evidence/05.4-CONTEXT.md` — locked decisions.
3. `.planning/workstreams/m1-perception/phases/05.4-continuous-l3-soak-evidence/05.4-05-PLAN.md` — next gated production plan.
4. `.planning/threads/market-observation-architecture.md` §1.6 and §2.9 — chain-truth and observation cadence.

## Resume

```bash
/gsd-resume-work --ws m1-perception
```

## Session Continuity

- **Last session:** 2026-07-24 11:46 (Asia/Shanghai)
- **Stopped at:** Phase 05.4 Plan 05 Task 1 complete. Focused/full tests,
  plan-scope Ruff and byte-identical legacy baseline, compile, image, docs, and
  planning gates passed; `05.4-SOAK-LOG.md` is committed at `03313a9`.

- **Proceeding to:** Re-prove production 007/runtime/Fly secret boundaries,
  dispatch `deploy-l2.yml` at exact source SHA `95bf1bd…`, and cross-check the
  resulting Fly release/machine/image/boot identity before readiness.

- **Resume file:** `.planning/workstreams/m1-perception/phases/05.4-continuous-l3-soak-evidence/05.4-05-PLAN.md`

## Accumulated Context

### Roadmap Evolution

- Phase 05.1 inserted after Phase 05: Durable L2 data-chain recovery (URGENT).
- Phase 05.4 inserted after completed Phase 05.3: Continuous L3 soak evidence
  (URGENT; blocks Phase 05 Plan 06 strict closure).
