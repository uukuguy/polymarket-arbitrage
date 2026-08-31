# Progress Log: Continuous L3 observability

## Session: 2026-07-22

### Phase 1: Diagnose and choose direction

- **Status:** complete
- Actions taken:
  - Distinguished automatic production data accumulation from formal checkpoint capture.
  - Audited promoter, health, mirror, WS mutation, and soak-plan evidence paths.
  - Demonstrated that six-hour spot checks cannot prove continuous membership.
  - Presented three approaches and recommended observability-first gap closure.
  - Received user approval to proceed with the recommendation.
- Files created/modified:
  - `task_plan.md` (created)
  - `findings.md` (created)
  - `progress.md` (created)

### Phase 2: Written design contract

- **Status:** complete
- Actions taken:
  - Initialized durable task planning files.
  - Wrote and self-reviewed the five-table continuous-evidence design.
  - Removed the quiet-market exception and locked per-market freshness fail-closed.
  - Reclassified the current release-37 window as diagnostic-only across canonical
    state, soak log, handoff, journal, and architecture thread.
  - Added the Phase 05 observation-cadence FAQ increment.
- Files created/modified:
  - `docs/superpowers/specs/2026-07-22-m1-continuous-l3-soak-evidence-design.md`
  - `.planning/CURRENT.md`
  - `.planning/workstreams/m1-perception/STATE.md`
  - `.planning/workstreams/m1-perception/phases/05-ws-book-prices/05-SOAK-LOG.md`
  - `.planning/workstreams/m1-perception/phases/05-ws-book-prices/.continue-here.md`
  - `.planning/JOURNAL.md`
  - `.planning/threads/market-observation-architecture.md`
  - `docs/learning/11-L3-K线.md`

### Phase 3: User review gate

- **Status:** complete
- Actions taken:
  - Prepared the committed written spec for explicit user review before planning.
  - Received explicit approval and converted the approved contract into Phase 05.4 context.

### Phase 4: Implementation planning

- **Status:** complete
- Actions taken:
  - Registered Phase 05.4 after 05.3 and locked PHASE05.4-R01 through R08.
  - Produced research, a 24-task Nyquist validation contract, five sequential
    executable plans, and a cross-plan implementation overview.
  - Ran three checker rounds. Fixed executor structure, WS→runtime→health
    chain-truth, database append-only integrity, manifest/report hashing,
    AcceptanceConfig identity, separate production approvals, retention
    privilege, future-T0 binding, and attempt-unique artifact paths.
  - Final independent checker returned `VERIFICATION PASSED`.

### Phase 5: Implementation and verification

- **Status:** complete
- Actions taken:
  - Implemented the Phase 05.4 continuous evidence chain.
  - Deployed exact runtime identity and passed T0/T6/T12/T18/T24.
  - Sealed the strict 24-hour evidence report and extracted learnings.

## Session: 2026-07-26

### Phase 6: Production neg-risk opportunity feed

- **Status:** complete
- Actions taken:
  - Probed L1/L2 health and verified the market-data base remains operational.
  - Reproduced the production opportunity-feed 503 and captured its exact body.
  - Traced the failure to zero complete quote runs, not zero market opportunity.
  - Verified H-009 code is deployed but was intentionally never scheduled.
  - Verified the cron machine has no shared volume and recently exited 137.
  - Measured the latest known universe at 1,278 tokens / 254 groups / 3 batches.
  - Presented three productionization approaches; user approved the in-process
    L1 worker (scheme A).
  - Ran the separately authorized production capacity observation: complete
    run 1, 1,278/1,278 responses, 1.013 seconds collector time.
  - Verified the public feed changed from truthful 503 to HTTP 200 immediately
    after the complete run.
  - Wrote and self-reviewed the approved production worker design and
    bite-sized RED/GREEN implementation plan.
  - Registered Phase 05.5 with one executable plan and updated ROADMAP/STATE.
  - Completed the first TDD slice for the sequential quote worker: immediate
    collection, 120-second wait, no overlap, fail-soft recovery, cancellation,
    and disabled-by-default builder.
  - Completed health/lifecycle RED/GREEN: 85 focused tests pass.
  - Full repository pytest passed with the existing one xfail and one skip.
  - Built `polyarb:quote-worker` and verified its production image imports and
    default-disabled/120-second settings under the documented local test gate.
  - Self-reviewed the complete change because the active execution policy
    disallows a separate review agent. Fixed one important observability edge:
    an unreadable quote store now produces a fail-closed health entry instead
    of raising an HTTP 500. Also replaced loose settings attribute access with
    the concrete `Settings` contract and centralized the 240-second warning
    threshold.
  - Committed exact implementation `bef53d3…`, restored the expired GitHub
    Fly credential without exposing its value, and deployed Fly L1 release 130
    via workflow `30182327847`.
  - Proved automatic production runs 2→3→4 across more than two 120-second
    intervals. Every run completed 1,278/1,278 responses; repeated feed
    diagnostics returned HTTP 200 and quote/snapshot health stayed pass.
  - Observed Python RSS at approximately 400 MB then 356 MB on the 1 GB L1
    machine, with no rising-memory or snapshot-failure signal.
  - Added a RED/GREEN L1 workflow contract to inject `GITHUB_SHA` as
    `POLYARB_RELEASE_ID`; exact SHA `cb0ba9c…` deployed as release 131 and
    `/health.releaseId` now proves runtime identity. Automatic run 6 completed
    after that restart and the feed remained HTTP 200.
  - Updated the living manual, teaching document, CURRENT, ROADMAP, STATE,
    SUMMARY, JOURNAL, observation thread, and durable planning logs.
- Files modified:
  - `task_plan.md`
  - `findings.md`
  - `progress.md`
  - `docs/superpowers/specs/2026-07-26-production-neg-risk-quote-worker-design.md`
  - `docs/superpowers/plans/2026-07-26-production-neg-risk-quote-worker.md`
  - `.planning/workstreams/m1-perception/phases/05.5-production-opportunity-feed/05.5-CONTEXT.md`
  - `.planning/workstreams/m1-perception/phases/05.5-production-opportunity-feed/05.5-01-PLAN.md`
  - `.planning/workstreams/m1-perception/ROADMAP.md`
  - `.planning/workstreams/m1-perception/STATE.md`

## Test Results

| Test | Input | Expected | Actual | Status |
|------|-------|----------|--------|--------|
| Evidence audit | Code + Phase 05 plan/log | Classify durable and ephemeral surfaces | Six-hour proof gap confirmed | ✓ |
| Plan structure | Five Phase 05.4 PLAN files | Zero errors/warnings | 5/5 valid, 24 tasks | ✓ |
| Nyquist map | PLAN tasks vs VALIDATION rows | One-to-one | 24/24 | ✓ |
| Independent checker | Spec/context/research/plans | Verification passed | Passed after revisions | ✓ |
| L1 opportunity route | Production GET | 200 or truthful bounded failure | 503 `quote run unavailable` | diagnostic |
| Production universe | Read-only SQLite query on L1 app | Capacity input | 1,278 tokens, 254 groups, 3 batches | ✓ |
| Quote capacity run 1 | Public CLOB reads + production SQLite quote run | Finish well inside 120 s | 1.013 s, 1,278/1,278 | ✓ |
| Feed after run 1 | Production GET, `limit=5` | Fresh HTTP 200 | HTTP 200, `count=5` | ✓ |
| Quote worker focused tests | `tests/daemon/test_quote_worker.py` | RED then GREEN | 5 behavior failures, then 6 passed | ✓ |
| Quote worker integration | focused worker/health/route/store/collector suites | All pass | 86 passed | ✓ |
| Full repository pytest | `uv run pytest -q` | Zero failures | Passed; 1 xfail, 1 skip | ✓ |
| Changed-file Ruff | changed source/tests | Zero findings | All checks passed | ✓ |
| Full repository Ruff | all `src tests` | Baseline audit only | 229 pre-existing findings outside changed files | legacy |
| Docker build | `docker build -t polyarb:quote-worker .` | Exit 0 | image `64214e3e…` | ✓ |
| Docker smoke | import worker/settings | Default false, interval 120 | Exact expected dict | ✓ |
| Production continuity | automatic runs across >240 s | Three increasing complete runs | 2→3→4, each 1,278/1,278 | ✓ |
| Repeated feed diagnosis | once after each sampled run | HTTP 200 valid feed | `available-opportunities` each time | ✓ |
| Quote/snapshot health | release 130 sampled health | Both remain pass | quote ages 36.7/17.9/20.3 s; snapshot pass | ✓ |
| Runtime identity | release 131 strict health | Exact deployed SHA | `cb0ba9c54d79…` | ✓ |

## Session: 2026-07-27

### Phase 7: Consolidated M1 production repair

- **Status:** in progress
- The prior long-run observation is closed as discovery evidence rather than
  an unfinished 24-hour acceptance run.
- A repeated full-snapshot OOM after the runtime memory reductions confirmed
  a capacity/topology defect. Separate evidence also identified missing direct
  snapshot-failure health/alerting, quote freshness coupling, and recovery
  notification granularity gaps.
- Next action: complete a single staged design and implementation plan before
  changing production topology or resource allocation.

### Design contract

- Wrote `docs/superpowers/specs/2026-07-27-m1-production-data-layers-design.md`.
- The design separates Structure, Quote, L2/L3, and Archive data products;
  defines a 30-minute Structure and 300-second Quote initial M1 contract;
  treats Archive as isolated research/audit work; and stages shared publication
  before any cross-machine M2 consumption.
- No production configuration, deploy, storage migration, or resource change
  has been made by this design work.

### Written-spec review

- User approved the staged data-layer design on 2026-07-27.
- The task has entered implementation planning. The approved design has three
  independently shippable scopes: M1 attempt/incident truth, M1 Structure vs
  Archive/Quote contract separation, and later M1→M2 shared publication.

### First-wave implementation plan

- Wrote and self-reviewed
  `docs/superpowers/plans/2026-07-27-m1-attempt-truth-and-component-incidents.md`.
- It covers durable scheduler attempt records, direct health exposure, per-
  component Polywatch recovery, a read-only operator command, tests, manual,
  learning documentation, and no production mutation.

### First-wave attempt and incident truth

- **Status:** complete locally; not deployed and not a new qualification run.
- Added append-only `snapshot_attempts`: every scheduler-launched run now has a
  durable terminal outcome, including SIGKILL/possible-OOM and cancellation.
- Strict health now exposes `snapshot:latest_attempt` and
  `snapshot:failure_counter` beside published market truth. A fresh old revision
  no longer conceals a new failed run.
- Replaced Polywatch's shared `active_keys` lifecycle with component-scoped
  incidents. L1, opportunity, L2, and Dashboard now alert/recover independently
  while retaining failed delivery retries.
- Added `make snapshot-attempt-status`, a local read-only JSON inspection command,
  plus learning document 26 and manual guidance.
- Verification: 34 scheduler/health tests, 37 Polywatch tests, 58
  command/manual-contract tests, changed-file Ruff, `make docs-m1-check`, and
  `git diff --check` all passed during the respective RED/GREEN commits.

### Structure / Archive separation plan

- Re-audited the current write path and confirmed `snapshots.id` already owns the
  atomic structure boundary: source coverage, event membership, neg-risk truth,
  and current market replacement commit together. A new revision table would
  duplicate and risk diverging from the existing Quote provenance chain.
- Wrote `docs/superpowers/plans/2026-07-27-m1-structure-archive-separation.md`.
  It introduces an explicit data-product marker, Gamma-only online Structure,
  CLOB/Parquet-only Archive, strict legacy rejection after cutover, and removal
  of no-volume cron snapshot jobs.
- The plan intentionally defers Archive production scheduling and any Fly memory
  resize. First prove Gamma-only Structure + Quote capacity; then set resources
  from measured peak and SLO evidence.

## Error Log

| Timestamp | Error | Attempt | Resolution |
|-----------|-------|---------|------------|
| 2026-07-21 | `state begin-phase` rewrote custom STATE fields | 1 | Restored canonical workstream STATE and documented tool-template mismatch |
| 2026-07-26 | zsh rejected assignment to read-only `status` | 1 | Renamed it to `http_code` |
| 2026-07-26 | Fly SSH requested interactive selection | 1 | Selected machine `6830939c0070d8` explicitly |
| 2026-07-26 | Remote SQL quoting produced a syntax error | 1 | Used `length(...) > 0` predicates |
| 2026-07-26 | Runtime image has no `time` executable | 1 | Used Python `time.perf_counter()` and did not install image tooling |
| 2026-07-26 | Changed-file Ruff found two legacy aliases and one long signature | 1 | Applied behavior-neutral UTC/TimeoutError aliases and multiline formatting |
| 2026-07-26 | Full pytest found two exact documentation phrase contract failures | 1 | Restored the canonical Make target spelling and fixed real-money/readiness warning phrases |
| 2026-07-26 | Full Ruff reports 229 legacy findings in unrelated files | 1 | Preserved scope; changed-file Ruff is clean and no bulk rewrite was made |
| 2026-07-26 | First image smoke lacked the required HMAC secret | 1 | Confirmed fail-closed, then used `POLYARB_ALLOW_EMPTY_SECRET=1` only for local image inspection |
| 2026-07-26 | First GHA deploy returned Fly `unauthorized` | 1 | Safely refreshed the expired repository secret and reran the exact workflow successfully |
| 2026-07-26 | Two local remote-build uploads stalled in Fly Depot | 2 | Stopped before production mutation; restored the canonical GHA path instead |
| 2026-07-26 | First local registry image was ARM64; AMD64 push hit registry errors | 2 | Production rejected the wrong architecture; abandoned the fallback after GHA recovery |
| 2026-07-26 | `/health.releaseId` remained `dev` after release 130 | 1 | Added workflow contract and injected `GITHUB_SHA`; release 131 reports exact SHA |

## 5-Question Reboot Check

| Question | Answer |
|----------|--------|
| Where am I? | Phase 6 — complete |
| Where am I going? | Legacy Phase 05 Plan 06 dashboard/closure, then M2 paper opportunity evaluation |
| What's the goal? | Turn the truthful but unavailable opportunity route into a continuously fresh known-universe feed |
| What have I learned? | See `findings.md`, especially the 2026-07-26 production section |
| What have I done? | Deployed scheme A, proved three automatic runs and HTTP 200 continuity, and restored exact release identity |
