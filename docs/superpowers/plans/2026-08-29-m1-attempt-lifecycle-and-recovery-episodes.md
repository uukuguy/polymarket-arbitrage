# M1 Attempt Lifecycle and Recovery Episodes Implementation Plan

> **For agentic workers:** Execute inline in this existing isolated worktree. Do not dispatch subagents. Complete each task with RED/GREEN evidence and an atomic commit.

**Goal:** Remove the remaining M1 timeout, sequencing and recovery single points of failure, recover the exact Structure source page, and start a fresh production qualification epoch.

**Architecture:** One durable attempt owns work lifetime; provider and DB bounds are subordinate I/O limits, scheduler timing is cadence only, and terminal grace is shutdown only. Failed source attempts replace their HTTP transport generation, durable stage-start facts identify the failing boundary, and recovery budgets are keyed by failure episode rather than target forever.

**Tech Stack:** Python 3.12, asyncio, httpx, psycopg 3, PostgreSQL 16, Alembic, Fly Machines API, pytest, uv.

## Global Constraints

- Never stage, edit or commit the three user-owned `.superpowers/sdd/` files.
- Never emit credentials, raw provider bodies or unredacted Machine env values.
- All production Fly calls unset `FLY_API_TOKEN` and `FLY_ACCESS_TOKEN`.
- Production fails closed; no pointer mutation is permitted during circuit recovery.
- Long tests and certificate windows must not receive arbitrary outer timeouts.
- Every code commit immediately receives `05.6-212-SUMMARY.md` before another plan.

---

### Task 1: Failed source attempt replaces its transport generation

**Files:**

- Modify: `src/polyarb/clients/gamma_client.py`
- Modify: `src/polyarb/control_plane/structure_source.py`
- Test: `tests/m1-perception/test_gamma_client.py`
- Test: `tests/m1-perception/test_transactional_structure_source_worker.py`

**Interfaces:**

- Produces: `GammaClient.reset_transport() -> Awaitable[None]`.
- The method preserves the existing `AsyncLimiter`, bounds closing with
  `GAMMA_CANCELLED_CLOSE_TIMEOUT_S`, installs a fresh `httpx.AsyncClient`, and
  never reuses the failed generation.

- [ ] Write a failing Gamma test whose first fake transport blocks/fails and
  whose second generation succeeds; assert the limiter identity is unchanged
  and the old client is never called again.
- [ ] Run the exact test and require RED because `reset_transport` is absent.
- [ ] Implement one `_build_http_client()` constructor and bounded
  `reset_transport()`; keep provider attempts equal to one.
- [ ] Write a failing worker test proving a retryable attempt calls reset once
  before returning and that reset failure cannot replace the original durable
  failure identity.
- [ ] Implement the minimal worker failure-path reset and run both files GREEN.
- [ ] Commit only Task 1 files with scope `fix(05.6-212)`.

### Task 2: Persist stage start before external I/O

**Files:**

- Modify: `src/polyarb/control_plane/runtime_contract.py`
- Modify: `src/polyarb/control_plane/structure_source.py`
- Test: `tests/m1-perception/test_control_plane_runtime_contract.py`
- Test: `tests/m1-perception/test_transactional_structure_source_worker.py`

**Interfaces:**

- Produces: `AttemptRuntime.current_stage -> str | None` reflecting only the
  last successfully persisted progress fact.
- Structure source records `(fetch-page,0,1)`, `(fetch-page,1,1)`,
  `(validate-page,0,1)`, `(validate-page,1,1)`, `(upload-page,0,1)`, and
  `(upload-page,1,1)` around their exact boundaries.

- [ ] Write failing runtime and worker tests for pre-I/O stage truth and
  stage-specific secret-free fingerprints.
- [ ] Run them RED: current code remains at `started` and exposes no current
  stage.
- [ ] Implement the minimal current-stage projection and ordered progress
  facts; use the durable stage when calculating the supplied fingerprint and
  incident detail.
- [ ] Run focused Structure/runtime/PostgreSQL tests GREEN.
- [ ] Commit only Task 2 files with scope `fix(05.6-212)`.

### Task 3: Make recovery budgets episode-scoped

**Files:**

- Create: `alembic/versions/035_m1_recovery_budget_episodes.py`
- Modify: `src/polyarb/control_plane/recovery_models.py`
- Modify: `src/polyarb/control_plane/recovery_records.py`
- Modify: `src/polyarb/control_plane/recovery_store.py`
- Modify: `src/polyarb/control_plane/runtime_observe.py`
- Test: `tests/alembic/test_035.py`
- Test: `tests/m1-perception/test_control_plane_runtime_observe.py`
- Test: `tests/m1-perception/test_control_plane_postgres.py`

**Interfaces:**

- Produces: `RecoveryRuntimeState.recovery_episode_key: str`.
- Circuit episode keys are exact `sha256:<64 lowercase hex>` fingerprints;
  job episode keys are exact attempt IDs.
- Budget primary key becomes `(controller_id,target_type,target_id,episode_key)`;
  legacy rows remain under `legacy`.

- [ ] Write migration RED tests for additive episode identity, preserved legacy
  counts, round trip and scoped-role privileges.
- [ ] Write scheduling RED tests: same episode exhausts at three; a different
  fingerprint receives a separate three-action budget; action detail and
  observe replay retain the episode.
- [ ] Run RED against revision 034 and target-only joins.
- [ ] Implement revision 035 and propagate episode identity through live read,
  observe serialization/replay, budget lock and budget consumption.
- [ ] Run real PostgreSQL 034→035→034→035 plus focused recovery suites GREEN.
- [ ] Commit only Task 3 files with scope `fix(05.6-212)`.

### Task 4: Enforce the timeout, sequencing and interruption inventory

**Files:**

- Create: `docs/dev/m1-runtime-boundary-inventory.md`
- Modify: `src/polyarb/control_plane/runtime_fault_matrix.py`
- Modify: `tests/m1-perception/test_control_plane_runtime_fault_matrix.py`
- Modify: `src/polyarb/control_plane/scheduler.py`
- Modify: `tests/m1-perception/test_control_plane_runtime_policy.py`
- Modify: `tests/m1-perception/test_transactional_control_plane_scheduler.py`

**Interfaces:**

- Produces: one table covering every production control-plane provider, DB,
  attempt, cadence and terminal boundary, with authority, cancellation,
  checkpoint and test evidence.
- Extends the deterministic matrix with poisoned transport replacement,
  pre-I/O stage timeout, new-episode budget and service interruption cases.

- [ ] Audit every timeout/cancel/wait call under `src/polyarb/control_plane/`,
  `src/polyarb/cli_control_plane.py`, `src/polyarb/clients/gamma_client.py` and
  `src/polyarb/storage/r2_sync.py`; classify each boundary before editing.
- [ ] Write failing matrix/static contract cases for each discovered duplicate
  authority or undocumented boundary.
- [ ] Remove or derive only findings proven redundant; do not alter legitimate
  provider, DB, cadence or terminal bounds.
- [ ] Run the deadline, lifecycle, worker and 15+ case fault matrix GREEN.
- [ ] Commit Task 4 code, tests and inventory with scope `fix(05.6-212)`.

### Task 5: Make planning-status single-pass and interruptible

**Files:**

- Modify: `scripts/planning_status.py`
- Test: `tests/test_planning_status.py`

**Interfaces:**

- Produces one in-memory Git history index shared by all 90 plan rows instead
  of three `git log` subprocesses per plan.

- [ ] Write a failing test that counts Git subprocess invocations across
  multiple plans and requires a constant number of history reads.
- [ ] Run RED against the current per-plan `--follow` implementation.
- [ ] Build the minimal single-pass creation/path/scope index while retaining
  exact workstream lifetime semantics and existing verdict output.
- [ ] Run the planning-status suite and real `make planning-status`; record
  wall time and zero drift.
- [ ] Commit Task 5 files with scope `perf(05.6-212)`.

### Task 6: Full verification, production recovery and qualification restart

**Files:**

- Create: `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/05.6-212-SUMMARY.md`
- Create: `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/evidence/attempt-lifecycle-recovery-audit.json`
- Modify: `docs/learning/00-INDEX.md`
- Modify: `docs/learning/95-超时任务序列与可恢复性审计.md`
- Modify: `.planning/threads/market-observation-architecture.md`
- Modify: `.planning/workstreams/m1-perception/STATE.md`
- Modify: `.planning/JOURNAL.md`

**Interfaces:**

- Produces the exact revision-035 release, production recovery evidence and a
  new release-bound qualification epoch.

- [ ] Run focused tests, Ruff, Pyright, climb 50/50, planning status and fresh
  complete `make test-m1` without an outer timeout.
- [ ] Build linux/amd64, verify nonroot user, Alembic 035, job DAG and immutable
  dependency checksum, then push exact worker and qualification tags.
- [ ] Stop coordinator, apply 035 transactionally, roll coordinator, and prove
  the legacy exhausted budget remains unchanged while the new fingerprint
  episode exists independently.
- [ ] Execute only the exact due circuit probe in an isolated 512MB
  auto-removed Machine; require zero pointer mutations and a successful source
  page receipt.
- [ ] Roll controller observe-only/empty-allowlist, Structure, Quote and
  qualification sequentially with fresh GET/render/update/start/verify.
- [ ] Require downstream Structure, Quote and opportunity freshness inside the
  900-second SLO, zero pending/running recovery actions and no restart loop.
- [ ] Start and record a new 86,400-second qualification epoch. Do not claim M1
  complete before the immutable certificate is independently reverified.
- [ ] Write teaching, evidence, SUMMARY, architecture thread, STATE and JOURNAL;
  run `make planning-status`, then commit only owned files.

### Task 7: Remove synchronous claim I/O from asynchronous worker lanes

**Files:**

- Modify: `src/polyarb/control_plane/service_lifecycle.py`
- Modify: `src/polyarb/control_plane/structure_source.py`
- Modify: `src/polyarb/control_plane/structure_worker.py`
- Modify: `src/polyarb/control_plane/quote_admission.py`
- Modify: `src/polyarb/control_plane/quote_worker.py`
- Modify: `tests/m1-perception/test_transactional_control_plane_scheduler.py`
- Modify: `tests/m1-perception/test_control_plane_runtime_policy.py`
- Modify: `docs/dev/m1-runtime-boundary-inventory.md`

**Interfaces:**

- Produces: `claim_worker_job(store, *, worker_id, job_types,
  lease_seconds, now) -> Awaitable[JobLease | None]` in the shared service
  lifecycle module.
- Database connection, statement and lock policies remain the only claim
  deadlines. The helper adds no timeout and uses the existing two-cancellation
  blocking bridge.

- [ ] Write a behavioral RED test with a fake synchronous `claim_job()` blocked
  on a thread event; require an independent event-loop task to advance before
  the claim is released.
- [ ] Write a static RED contract covering all audited async transactional
  worker modules; reject direct `self._control_plane.claim_job(` expressions.
- [ ] Run the exact tests and require RED against the current inline calls.
- [ ] Implement `claim_worker_job()` with `run_blocking_call()` and replace the
  five async worker claim sites; also bridge source failure-path spec lookup.
- [ ] Run the behavioral/static tests and all affected worker suites GREEN;
  run Ruff and Pyright on the modified files.
- [ ] Rebuild and push an exact revision image, roll only the coordinator first,
  release the exact circuit episode through the authorized one-shot executor,
  and require the source page's terminal receipt before rolling sibling roles.
- [ ] Update the inventory, teaching/evidence and `05.6-212-SUMMARY.md`, then
  commit only owned files with scope `fix(05.6-212)`.

### Task 8: Separate Quote pool capacity from serial scheduling turns

**Files:**

- Modify: `src/polyarb/control_plane/quote_worker.py`
- Modify: `src/polyarb/control_plane/worker_loop.py`
- Modify: `src/polyarb/cli_control_plane.py`
- Modify: `src/polyarb/config.py`
- Modify: `tests/m1-perception/test_transactional_quote_worker.py`
- Modify: `tests/m1-perception/test_transactional_control_plane_scheduler.py`
- Modify: `tests/m1-perception/test_control_plane_cli.py`

**Interfaces:**

- Produces `TransactionalQuoteBatchPool`, whose independently identified lanes
  share the existing `clob_batch_max_concurrency` resource authority.
- `pool-turns` remains a serial wave budget; it is not redefined as concurrency.

- [x] Reproduce the production capacity contradiction from 148 batches at
  roughly 11–12 seconds each versus the 900-second freshness gate.
- [x] Write RED tests for simultaneous lane entry, sibling drain on failure,
  role-service continuity and all-lane SIGTERM cancellation.
- [x] Build distinct lease lanes around the shared bounded CLOB/R2 clients and
  derive their count from `clob_batch_max_concurrency`.
- [x] Keep single-lane failures local to the role loop instead of terminating
  the Machine; preserve intentional `BaseException` crash injection semantics.
- [x] Run focused suites, Ruff and modified-file Pyright.
- [x] Build and roll the exact release, prove backlog drain inside 900 seconds,
  then restart the release-bound qualification epoch.

### Task 9: Close the reconciler incident lifecycle on durable recovery

**Files:**

- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `src/polyarb/control_plane/recovery_store.py`
- Modify: `tests/m1-perception/test_control_plane_postgres.py`

**Interfaces:**

- `record_job_recovery()` remains the lease-fenced closure authority and now
  resolves worker plus reconciler incident projections.
- `_record_recovery_incident()` reopens the current projection for a new
  immutable recovery-started event.

- [x] Reproduce terminal jobs and closed circuits with still-open production
  recovery incidents.
- [x] Write a real-PostgreSQL RED test for open → recovered → reopened ordering.
- [x] Resolve the three bounded dedupe forms atomically and emit one idempotent
  recovered event/alert per incident.
- [x] Preserve lock/statement timeout rollback and checkpoint recovery proofs.
- [x] Roll the exact release, repair the two historical projections through the
  same durable method, and require zero contradictory runtime incidents.

### Task 10: Make terminal fan-in wakeups truthful and self-repairing

**Files:**

- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `src/polyarb/control_plane/quote_worker.py`
- Modify: `src/polyarb/control_plane/structure_worker.py`
- Modify: `tests/m1-perception/test_control_plane_postgres.py`

**Interfaces:**

- A producer receipt is necessary but insufficient for successor eligibility;
  the matching producer job must also be `succeeded`.
- Concurrent terminal producers use the certifier job row for direct
  eligibility observation; Task 18 makes that optimization non-blocking after
  production disproved blocking serialization.
- `repair_ready_certifiers()` advances at most one historically lost ready
  wakeup per certifier turn from durable database facts.

- [x] Reproduce the two-final-receipt lost wakeup deterministically with a
  transaction barrier for both Structure and Quote.
- [x] Prove a checkpoint receipt cannot wake a certifier before its producer
  reaches terminal success.
- [x] Move wake authority after terminal transition, retain the historical
  `record → finish` contract, and serialize sibling observations with
  `FOR UPDATE` on the successor.
- [x] Add a bounded `FOR UPDATE SKIP LOCKED` repair sweep and invoke it before
  each certifier claim; prove incomplete generations remain waiting.
- [x] Run affected real-PostgreSQL and worker suites, Ruff and Pyright.
- [x] Roll the exact release and prove the production waiting Quote certifier
  repairs itself without manual SQL before restarting qualification.

### Task 11: Make pooled worker shutdown inherit its lane lifecycle authority

**Files:**

- Modify: `src/polyarb/control_plane/structure_source.py`
- Modify: `tests/m1-perception/test_transactional_structure_source_worker.py`

**Interfaces:**

- `TransactionalStructureSourcePool._lease_seconds` is derived from one common
  positive lane policy, matching the Quote pool contract.
- `terminal_grace_seconds("structure-source", pool)` resolves from
  `runtime_policy("structure-fetch", lease_seconds)` without a duplicate
  shutdown constant.

- [x] Capture the production SIGTERM failure: active Structure source pool had
  no declared lease and raised `ValueError` while the coordinator drained.
- [x] Add a RED lifecycle test proving the pool must expose its common lease.
- [x] Reject heterogeneous/non-positive lane policies and expose the derived
  lease on the pool.
- [x] Run source, scheduler, Ruff and Pyright gates.
- [x] Build v10, roll all roles and prove every active process exits normally
  under the same 40-second Fly kill timeout.

### Task 12: Renew cumulative lease age at every blocking-call boundary

**Files:**

- Modify: `src/polyarb/control_plane/quote_worker.py`
- Modify: `src/polyarb/control_plane/structure_worker.py`
- Modify: `tests/m1-perception/test_transactional_quote_worker.py`
- Modify: `tests/m1-perception/test_transactional_structure_worker.py`

**Interfaces:**

- The synchronous blocking bridges keep one absolute attempt deadline, but
  invoke `heartbeat_if_due()` after every successful nonterminal sub-call as
  well as during a single slow call.
- Terminal calls do not renew after their transaction has committed.

- [x] Reproduce the production contradiction: 148 individually fast R2 reads
  never enter the heartbeat wait timeout, then the cumulative 120-second lease
  expires and the Opportunity worker reports `stale-lease`.
- [x] Add RED tests proving a fast successful sub-call still reaches the
  runtime's due-check for both synchronous bridge implementations.
- [x] Check the due heartbeat after nonterminal success while retaining the
  existing slow-call polling, drain-before-error and terminal semantics.
- [x] Run the complete affected Quote, Opportunity and Structure suites, Ruff
  and Pyright.
- [x] Roll an exact release and prove the interrupted Opportunity job reclaims,
  publishes a fresh pointer, and allows one uninterrupted qualification epoch.

### Task 13: Separate platform readiness from the operator snapshot

**Files:**

- Modify: `src/polyarb/control_plane/api.py`
- Modify: `src/polyarb/control_plane/db_deadlines.py`
- Modify: `src/polyarb/control_plane/db_role_contract.py`
- Modify: `src/polyarb/control_plane/postgres.py`
- Test: `tests/m1-perception/test_control_plane_api.py`
- Test: `tests/m1-perception/test_control_plane_deployment_templates.py`
- Test: `tests/m1-perception/test_control_plane_postgres.py`

**Interfaces:**

- `/healthz` proves only minimal durable-authority readability and never builds
  `operational_snapshot()`.
- `CONTROL_PLANE_HEALTH_DB_POLICY` covers connect, session bootstrap, one
  readiness statement and transfer below the Fly five-second boundary.
- Default request and stop envelopes include mandatory session bootstrap.

- [x] Correlate recurring 16–56 second Fly health failures with the five-second
  platform check and the heavier operator-snapshot path.
- [x] Add RED tests proving health does not call the snapshot and its complete
  internal policy is strictly below the deployment check timeout.
- [x] Implement one-statement readiness with a dedicated scoped connection
  policy; detach stalled reads and return typed 503 without provider detail.
- [x] Run API, deployment, DB-role, alert-clock, Ruff, Pyright and real
  PostgreSQL focused gates.
- [x] Roll the exact API image and prove a sustained healthy platform window
  while the full operator endpoint remains available.

### Task 14: Isolate health-contract tests from host disk pressure

**Files:**

- Modify: `tests/m1-perception/conftest.py`
- Modify: `tests/m1-perception/test_quote_feed_health.py`

**Interfaces:**

- General health tests receive deterministic 50% volume headroom.
- The dedicated physical-volume contract still injects 25%, 19% and 9% and
  remains the sole test of pass/warn/fail thresholds.

- [x] Reproduce four late-suite `pass -> warn` failures and inspect the exact
  non-pass check rather than treating them as flakes.
- [x] Prove host free space was 20.18% and crossed the 20% boundary while the
  suite and Docker image used the same disk.
- [x] Scope deterministic volume evidence to HTTP/Quote health contracts while
  preserving the explicit threshold test.
- [x] Run both complete affected health files and Ruff GREEN.
- [x] Run a fresh complete M1 suite without an outer timeout.

### Task 15: Separate Structure range capacity from generational admission

**Files:**

- Modify: `src/polyarb/control_plane/structure_worker.py`
- Modify: `src/polyarb/control_plane/structure_source.py`
- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `src/polyarb/control_plane/scheduler.py`
- Modify: `src/polyarb/cli_control_plane.py`
- Modify: `src/polyarb/config.py`
- Test: `tests/m1-perception/test_transactional_structure_worker.py`
- Test: `tests/m1-perception/test_transactional_structure_source_worker.py`
- Test: `tests/m1-perception/test_control_plane_cli.py`
- Test: `tests/m1-perception/test_control_plane_postgres.py`

**Interfaces:**

- `TransactionalStructureRangePool` owns a bounded set of independently
  fenced range lanes and exposes their common lease policy to service shutdown.
- `structure_range_max_concurrency` is the sole Structure range capacity
  authority; `pool-turns` remains a wave budget.
- `structure_high_water=1` is the default generational admission barrier and
  does not alter attempt, lease or freshness deadlines.

- [x] Measure the production contradiction: 1,115 ranges at 8.2 ranges/minute
  project to roughly 136 minutes against a 15-minute freshness gate.
- [x] Prove the 2,000-job high-water admits overlapping generations.
- [x] Add RED tests for simultaneous lane entry, distinct identities, shared
  clients, common lease validation, sibling drain and default high-water.
- [x] Implement twelve bounded lanes from `ceil(136 / 15) + 2` and one-range
  generational backpressure without changing lifecycle deadlines.
- [x] Run focused worker/source/CLI/PostgreSQL/settings suites, Ruff and
  modified-file Pyright.
- [x] Run the complete M1 suite without an outer timeout.
- [ ] Build an exact release, canary only the Structure range Machine and prove
  twelve simultaneous distinct leases plus a projected generation drain below
  900 seconds before rolling sibling roles.

### Task 16: Give high-frequency SQLite drift readers explicit ownership

**Files:**

- Modify: `src/polyarb/storage/sqlite_store.py`
- Modify: `tests/m1-perception/test_structure_drift_performance.py`

**Interfaces:**

- `_connect_structure_drift_read()` owns and explicitly closes each page-local
  connection on success, early return and exception.
- Classifier benchmark observers also close their direct SQLite probes; test
  instrumentation may not become a second resource leak.

- [x] Observe roughly 600 descriptors during the 120k production-shaped gate
  and distinguish transaction context exit from connection close.
- [x] Add a RED/GREEN real-connection wrapper proof requiring ten opens and ten
  closes across ten pages.
- [x] Route high-frequency event/source/projection/member/truth/evidence reads
  through the explicit owner without changing page or transaction boundaries.
- [x] Run complete drift classification, projection and end-to-end suites,
  Ruff, and the full M1 suite without an outer timeout.
- [x] Complete the standalone 120k gate with direct benchmark observers closed
  and record bounded descriptor evidence.

### Task 17: Make worker identity a single-live-lease capacity boundary

**Files:**

- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `tests/m1-perception/test_control_plane_postgres.py`

**Interfaces:**

- `claim_job()` serializes contenders for the same `worker_id` with a
  transaction-scoped advisory try-lock.
- One fixed-round guard refuses a new claim while that identity owns an
  unexpired `leased` job. A terminal `checkpointed` attempt remains immediately
  resumable; lease expiry is the recovery authority only for a live attempt
  whose terminal database write could not complete.

- [x] Reproduce the production overlap: lanes 6 and 10 each owned two live
  Structure jobs after terminal `OperationalError` paths escaped.
- [x] Add a real-PostgreSQL RED test proving one worker identity cannot claim a
  second runnable job while its first lease is live.
- [x] Combine advisory serialization and active-lease inspection into one SQL
  round before the existing claim query.
- [x] Run the complete PostgreSQL/Structure/Quote/scheduler regression group,
  Ruff and modified-file Pyright.
- [x] Build a new exact image and repeat the isolated Structure canary; require
  live leases `<= 12`, no retry storm and projected generation drain below 900
  seconds before any sibling rollout.

### Task 18: Decouple producer commit from the shared certifier row

**Files:**

- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `tests/m1-perception/test_control_plane_postgres.py`

**Interfaces:**

- Terminal Structure/Quote producers must commit their own receipt, job and
  attempt facts even when another transaction owns the certifier row.
- Direct wake uses `FOR UPDATE SKIP LOCKED`; row absence remains a fail-closed
  invariant error, while row contention is a safe best-effort miss.
- `repair_ready_certifiers()` remains the mandatory bounded convergence path
  before a certifier claim.

- [x] Measure v13 for an exact five-minute window: 229 successes, 34
  `OperationalError`, 10 `LeaseExpired`, 45.8 successes/minute and at most 12
  live worker identities.
- [x] Correlate the failures with every terminal producer blocking on one
  certifier row under the one-second lock policy; reject timeout enlargement.
- [x] Add real-PostgreSQL RED tests holding the successor row while Structure
  and Quote terminal producers attempt to commit.
- [x] Make direct wake non-blocking and prove both producers commit, the
  successor remains waiting, and bounded repair advances it from durable facts.
- [x] Run the complete PostgreSQL/worker/full-M1 gates, build an exact image and
  repeat the Structure-only canary before any sibling rollout.

### Task 19: Close the source-to-materializer generation admission gap

**Files:**

- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `tests/m1-perception/test_control_plane_postgres.py`

**Interfaces:**

- Structure admission counts unfinished `structure-materialize`,
  `structure-normalize` and `structure-certify` jobs under the same configured
  high-water; a generation cannot disappear while changing durable shape.
- Only the oldest unfinished materializer is claimable, and no materializer is
  claimable while any prior range or certifier remains unfinished.
- Existing lease/checkpoint/retry/circuit facts remain the recovery authority;
  no attempt, lock, provider or freshness timeout changes.

- [x] Prove production had three unfinished generations: source windows admitted
  at `01:17Z` and `02:04Z` materialized only at `04:37Z` and `04:51Z`, after
  the admission-only stopgap was already active.
- [x] Stop only the coordinator before its remaining retryable materializer can
  enqueue a fourth generation; preserve all durable jobs and sibling workers.
- [x] Add real-PostgreSQL RED tests for the pre-range materializer gap,
  post-range/pre-certifier gap, two materializer contenders and prior-generation
  certification ordering.
- [x] Implement one fixed pipeline-backlog predicate in admission and
  materializer claim eligibility, then run focused and complete gates.
- [x] Restore coordinator only on an exact canary release that proves no fourth
  generation is admitted/materialized while prior generations are unfinished.

### Task 20: Make production capacity changes explicit and reversible

**Files:**

- Modify: `src/polyarb/control_plane/fly_machine_update.py`
- Modify: `tests/m1-perception/test_fly_machine_update.py`
- Modify: `Makefile`

**Interfaces:**

- A resource canary supplies `cpu_kind`, `cpus` and `memory_mb` as one complete
  guest shape; partial patches and non-positive values fail closed.
- The update still uses one fresh Machine GET, optimistic `current_version`,
  exact image and `SIGTERM/40s`; every config path outside image, stop contract
  and the explicitly requested guest shape remains hash-proven unchanged.
- Capacity is not a timeout. The 900-second generation objective and all lease,
  attempt, provider and shutdown policies remain unchanged during the A/B test.

- [x] Reject v14 shared-1x after an exact sliding five-minute window produced
  288 successes (57.6/minute) against the 74.3/minute floor, with zero window
  `OperationalError`, expired leases or open circuits.
- [x] Split 168 complete range attempts by durable stages: 2.520s claim/read,
  6.376s read/normalize, 2.747s normalize/upload, 1.430s upload/commit and
  1.589s commit/finish on average.
- [x] Add RED/GREEN contracts for one complete guest-shape update, rejection of
  partial/invalid shapes, immutable input and redacted old/new resource proof.
- [x] Run a shared-2x Structure-only A/B canary and accept it only if a fresh
  five-minute window exceeds 74.3 successes/minute without ownership growth,
  repeated operational failures, expired leases or open circuits.

### Task 21: Keep the operator snapshot bounded as history grows

**Files:**

- Create: `alembic/versions/036_m1_operator_snapshot_indexes.py`
- Create: `tests/alembic/test_036.py`
- Modify: `src/polyarb/control_plane/db_role_admin.py`
- Modify: `src/polyarb/control_plane/rollout.py`

**Interfaces:**

- `attempts LIMIT N` is backed by global `(started_at DESC, attempt_id DESC)`;
  it must not scan all historical attempts to return at most 100 rows.
- pending outbox sampling is backed by a partial
  `(created_at DESC, outbox_id DESC) WHERE state='pending'` index; delivery's
  existing `(state, next_attempt_at)` authority remains unchanged.
- Production index builds and drops use PostgreSQL `CONCURRENTLY` inside an
  Alembic autocommit block. An observation repair may not block writers.
- The five-second data-statement deadline remains unchanged; schema revision
  036 makes the work fit the boundary instead of enlarging it.

- [x] Reproduce `control-plane status` as `QueryCanceled` under 12-lane load.
- [x] Capture a read-only production `EXPLAIN ANALYZE`: 2.337s warm execution,
  including a 630.683ms pending-outbox sequential scan and a parallel
  111k-row attempt-history scan.
- [x] Add RED/GREEN revision tests for concurrent DDL, exact definitions,
  downgrade and real PostgreSQL planner use of both indexes.
- [x] Apply revision 036, repeat the production plan/status probe under load,
  and require an index-backed result within the unchanged five-second statement
  boundary before rolling the new exact runtime image.

### Task 22: Make the exact recovery Make entrypoint executable

**Files:**

- Modify: `Makefile`
- Modify: `tests/m1-perception/test_makefile_contract.py`

**Interfaces:**

- The three exact selector fields remain all-or-none and `enable=1` remains
  mandatory, but the guard must be valid POSIX `/bin/sh` when executed.
- The test executes the recipe through a fake `uv`; `make -n` is insufficient
  because it never parses the shell guard that failed in production use.

- [x] Reproduce the exact selector command failing before the CLI with an
  unescaped-parenthesis `/bin/sh` syntax error.
- [x] Replace it with a grouped all-or-none test and execute the Make contract
  through a no-I/O fake `uv`; the complete Makefile contract file passes.

### Task 23: Keep recovery candidate selection inside its own deadline

**Files:**

- Modify: `src/polyarb/control_plane/recovery_store.py`
- Modify: `tests/m1-perception/test_control_plane_postgres.py`

**Interfaces:**

- Candidate selection names the five legal nonterminal job states positively,
  allowing the existing `m1_jobs_state` index to bound the selective side of
  the join. It must not express the predicate as `NOT IN` over terminal history.
- Recovery keeps its original two-second statement and one-second lock policy.
  No controller lease, circuit probe time or action budget changes.

- [x] Prove the isolated execute one-shot uses the correct scoped DB role and
  exact candidate/action, but fails before action INSERT with PostgreSQL
  `QueryCanceled` / SQLSTATE 57014.
- [x] Reproduce `read_runtime_reconcile_states()` locally in 4.872 seconds and
  the warm production plan at 739.698ms, scanning 102,828 terminal jobs and
  7,010 runtime rows before returning 16 candidates.
- [x] Add a RED/GREEN query-shape contract and switch to the five positive
  states; production `EXPLAIN ANALYZE` falls to 19.528ms and the real read
  succeeds under the unchanged central recovery deadline.
- [ ] Build the exact image and rerun the same isolated, execute-only,
  target/action-pinned circuit probe before rolling the resident controller.

### Task 24: Apply the exact recovery selector before the sample bound

**Files:**

- Modify: `src/polyarb/control_plane/recovery_store.py`
- Modify: `src/polyarb/cli_control_plane.py`
- Modify: `tests/m1-perception/test_control_plane_postgres.py`
- Modify: `tests/m1-perception/test_control_plane_cli.py`

**Interfaces:**

- An execute-only exact target is a database predicate, not an in-memory filter
  over the normal 100-row observation sample.
- The normal observe-only path retains its bounded oldest-first sample; target
  type and expected action are still verified after reading the exact job.

- [x] Reproduce a target outside `sample_limit=1` disappearing when filtering
  occurred after SQL `LIMIT`.
- [x] Push `target_id` into the read projection, prove the exact target is
  returned with the same limit, and prove the CLI forwards it before action
  matching or scheduling.

### Task 25: Bound normal transport cleanup inside process shutdown

**Files:**

- Modify: `src/polyarb/clients/gamma_client.py`
- Modify: `tests/m1-perception/test_gamma_client.py`

**Interfaces:**

- Explicit/normal `aclose()` uses the same two-second transport cleanup bound
  as replacement and cancellation cleanup.
- Timeout or close failure is redacted cleanup evidence and cannot replace a
  completed durable stop result or keep the process alive until platform kill.

- [x] Reproduce explicit `aclose()` hanging beyond an outer 100ms test bound
  while the configured cleanup authority was 1ms.
- [x] Make explicit close bounded and fail-soft; normal and cancelled cleanup
  contracts plus the complete Gamma suite pass.

### Task 26: Make the certifier attempt ceiling independent of lease duration

**Files:**

- Modify: `src/polyarb/control_plane/runtime_deadlines.py`
- Modify: `tests/m1-perception/test_transactional_runtime_coverage.py`

**Interfaces:**

- `structure-certify` owns one explicit 3,600-second absolute attempt ceiling;
  changing its lease changes heartbeat/progress fencing, not total lifetime.
- A lease whose derived progress boundary exceeds the ceiling is rejected
  instead of silently enlarging the ceiling.

- [x] Reproduce lease 30 producing 3,600 seconds while lease 120 silently
  produced 14,400 seconds from the old multiplier.
- [x] Split relative multiplier and absolute-ceiling policy authorities and
  prove both lease values resolve to exactly 3,600 seconds.

### Task 27: Make exact runtime-image identity part of the executable entrypoint

**Files:**

- Modify: `Makefile`
- Modify: `tests/m1-perception/test_makefile_contract.py`

**Interfaces:**

- The public build target derives the full Git HEAD itself, rejects dirty
  runtime-image inputs, pushes without deploying, and writes that full SHA to
  `org.opencontainers.image.revision`.
- Image tags remain operator-selected release names; they cannot stand in for
  the independently inspectable OCI revision label.

- [x] Reproduce a successfully pushed v15 image whose linux/amd64 manifest and
  UID were correct but whose OCI revision label was null; deploy nothing.
- [x] Add an executed fake-`flyctl` RED/GREEN contract and the documented
  `make runtime-image-build image_tag=...` entrypoint; prove build-only, push,
  exact label, private-app default and ambient-token suppression.
- [ ] Rebuild the exact committed source through this entrypoint and require
  label, architecture, UID, revision 036, Supercronic checksum and eight-job
  order before any Machine update.

### Task 28: Preserve half-open recovery authority across trusted interruption

**Files:**

- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `tests/m1-perception/test_control_plane_postgres.py`
- Modify: `docs/dev/m1-runtime-boundary-inventory.md`
- Modify: `tools/climb/eval_local.py`
- Modify: `tests/climb/test_eval_local.py`

**Interfaces:**

- `finish_interrupted()` keeps an open circuit's failure identity and count,
  but renews `next_probe_at` from the existing component retry policy in the
  same fenced transaction.
- Trusted interruption never closes the circuit, creates a new episode or
  consumes another recovery action; the replacement worker must claim within
  the renewed bounded window.

- [x] Reproduce production deployment interruption after an authorized
  certifier probe had outlived its 60-second claim window: the attempt became
  retryable but could not be reclaimed without a second action.
- [x] Add a real-PostgreSQL RED/GREEN contract that runs a half-open attempt
  beyond the original window, interrupts it, immediately reclaims epoch N+1,
  and proves circuit count/state plus episode budget are unchanged.
- [x] Run the complete control-plane and Climb lifecycle gates; run the full M1 gate before
  rebuilding and rolling the exact image.

### Task 29: Remove magic and unbounded PostgreSQL concurrency barriers

**Files:**

- Modify: `tests/m1-perception/test_control_plane_postgres.py`
- Modify: `tools/climb/eval_local.py`
- Modify: `tests/climb/test_eval_local.py`
- Modify: `docs/dev/m1-runtime-boundary-inventory.md`

**Interfaces:**

- Concurrency barriers are diagnostic deadlock watchdogs, not product timing
  assertions, and use `CONTROL_PLANE_DB_POLICY.stop_grace_seconds` as their
  single named envelope.
- No real-PostgreSQL concurrency barrier is unbounded or owns an ad-hoc 5/10
  second value.

- [x] Reproduce the structure fan-in barrier failing after five seconds only
  under full-suite load; rerun it five times in isolation to distinguish a
  harness clock from a durable fan-in defect.
- [x] Replace every barrier magic number plus the unbounded barrier with one
  policy-derived diagnostic watchdog and pass all six concurrency contracts.
- [x] Confirm the new Climb gate; restart the complete M1 regression after the
  confirmed state is committed.

### Task 30: Test policy ownership without freezing consumer count

**Files:**

- Modify: `tests/m1-perception/test_transactional_runtime_coverage.py`
- Modify: `tools/climb/eval_local.py`
- Modify: `tests/climb/test_eval_local.py`

**Interfaces:**

- The static authority contract requires every PostgreSQL retry backoff use to
  come from `runtime_retry_policy()` and continues to reject copied formulas.
- Adding a legitimate lifecycle consumer does not require weakening or
  renumbering a product deadline.

- [x] Reproduce the full-suite-only failure caused by a hard-coded count of
  three after trusted interruption became the fourth policy consumer.
- [x] Replace exact consumer cardinality with lookup/use parity plus existing
  private-formula prohibitions; pass the exact authority and certifier tests.
- [x] Confirm the amended Climb gate before rerunning the full M1 suite.

## Self-review

- Spec coverage: transport lifetime, stage identity, episode budgets,
  sequencing, cancellation, event-loop claim isolation, non-blocking fan-in repair,
  inventory, operator wait, production proof, capacity/backpressure and
  connection ownership, worker-identity capacity and producer commit
  independence, cross-shape generation ordering, explicit capacity rollout,
  bounded operator observation, executable recovery entrypoints and bounded
  candidate reads, pre-limit exact selection, bounded transport cleanup and
  lease-independent attempt ceilings, exact image identity and half-open
  interruption continuity and deterministic concurrency watchdogs all map to
  Tasks 1–30.
- Placeholders: none; every task names files, interfaces, RED/GREEN commands or
  production gates.
- Type consistency: `reset_transport`, `current_stage` and
  `recovery_episode_key` are defined once and consumed under the same names.
