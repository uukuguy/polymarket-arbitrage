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
- Create: `docs/learning/93-任务生命周期与恢复回合.md`
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

## Self-review

- Spec coverage: transport lifetime, stage identity, episode budgets,
  sequencing, cancellation, inventory, operator wait and production proof all
  map to Tasks 1–6.
- Placeholders: none; every task names files, interfaces, RED/GREEN commands or
  production gates.
- Type consistency: `reset_transport`, `current_stage` and
  `recovery_episode_key` are defined once and consumed under the same names.
