# M1 Self-Healing Production Enablement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove the deterministic fault matrix, deploy observe-only runtime control, then enable fenced job and exact process/Machine recovery in explicit production gates.

**Architecture:** Separate controller and qualification Fly services use distinct least-privilege roles. Recovery action classes are independently enabled. Historical replay and controlled faults replace repeated 24-hour debugging; rolling qualification continues automatically after enablement.

**Tech Stack:** Python 3.12, Fly Machines, Cloudflare supervisor, Supabase/PostgreSQL, Docker, GitHub Actions, pytest, uv, Make.

## Global Constraints

- Execute after Plans 01-05 and deploy only exact reviewed commits.
- Run `make chaos-l2-fly-image-check` or an explicit local image command before using any image tool; do not assume `pkill`, `ps`, `dig`, `ping`, or `which` exists.
- Never expose Fly machine-update diffs or secret-bearing environment output.
- Observe-only is the default. Each action class requires an explicit enable flag and exact allowlist.
- Production fault mutation requires separate user authorization naming the exact release, target, fault, and evidence directory.
- Preserve all old evidence and the current failed run.
- Use TDD and atomic commits. End with `05.6-206-SUMMARY.md`, teaching docs, JOURNAL/thread updates, and clean `make planning-status`.

---

### Task 1: Deterministic runtime fault matrix

**Files:**
- Create: `src/polyarb/control_plane/runtime_fault_matrix.py`
- Create: `tests/m1-perception/test_control_plane_runtime_fault_matrix.py`
- Modify: `src/polyarb/cli_control_plane.py`
- Modify: `Makefile`
- Modify: `tests/m1-perception/test_makefile_contract.py`

**Interfaces:**
- Produces: `runtime-fault-matrix` local-only qualification gate and canonical JSON result.

- [x] **Step 1: Write failing matrix tests**

Require cases for task exception, R2 timeout/hang, heartbeat loss, progress
stall, stale owner, circuit probe, process exit, Machine restart decision,
database/event-writer failure, watchdog failure, duplicate delivery, and stale
action. Every case asserts detection latency, incident transitions, action,
fence result, recovery, Dashboard projection, and qualification impact.

- [x] **Step 2: Prove red**

Run: `uv run pytest tests/m1-perception/test_control_plane_runtime_fault_matrix.py -q`

Expected: FAIL because the matrix runner does not exist.

- [x] **Step 3: Implement local virtual-time runner**

`run_fault_matrix()` requires `POLYARB_CONTROL_PLANE_TEST_DSN`, creates a
uniquely named temporary schema with the existing real-Postgres test helper,
advances a deterministic clock, returns one record per named case, and drops
only that resolved schema in `finally`. It imports no Fly client and rejects a
missing or production-scoped DSN.
Add:

```make
## runtime-fault-matrix: Run the local deterministic self-healing fault matrix; never contacts production.
runtime-fault-matrix:
	@test -n "$$POLYARB_CONTROL_PLANE_TEST_DSN" || (echo "ERROR: explicitly export POLYARB_CONTROL_PLANE_TEST_DSN" >&2; exit 2)
	@uv run python -m polyarb.cli_control_plane runtime-fault-matrix --json
```

- [x] **Step 4: Verify and commit**

Run the matrix twice; expected identical ordered results and PASS. Run Ruff.

```bash
git add src/polyarb/control_plane/runtime_fault_matrix.py tests/m1-perception/test_control_plane_runtime_fault_matrix.py src/polyarb/cli_control_plane.py Makefile tests/m1-perception/test_makefile_contract.py
git commit -m "test(05.6-206): qualify runtime recovery deterministically"
```

### Task 2: Least-privilege deployment topology

**Files:**
- Create: `deploy/control-plane/fly-runtime-controller.toml.template`
- Create: `deploy/control-plane/fly-qualification-worker.toml.template`
- Modify: `src/polyarb/control_plane/rollout.py`
- Modify: `tests/m1-perception/test_control_plane_deployment_templates.py`
- Modify: `tests/m1-perception/test_control_plane_rollout.py`

**Interfaces:**
- Produces isolated `runtime-controller` and `qualification` process groups.

- [x] **Step 1: Write failing static topology tests**

Assert controller has scoped Postgres plus optional exact Fly recovery token,
no R2/Gamma/CLOB/Telegram/public HTTP, and defaults to
`POLYARB_RUNTIME_RECOVERY_MODE=observe-only`. Assert qualification has only its
scoped DSN, no Fly token, and no public HTTP. Both use restart policy `always`.

- [x] **Step 2: Prove red**

Run deployment-template and rollout tests; expected FAIL because templates are absent.

- [x] **Step 3: Implement templates and renderer**

Controller command:

```toml
[processes]
controller = "python -m polyarb.cli_control_plane runtime-reconcile-serve --enable --interval-seconds 30 --json"
```

Qualification command:

```toml
[processes]
qualification = "python -m polyarb.cli_control_plane qualification-serve --enable --interval-seconds 30 --json"
```

Render explicit app names and recovery allowlists. Never render credential
values or reuse worker/sampler credentials.

- [x] **Step 4: Verify and commit**

Run template, rollout, image-contract, and Ruff gates; expected PASS.

```bash
git add deploy/control-plane/fly-runtime-controller.toml.template deploy/control-plane/fly-qualification-worker.toml.template src/polyarb/control_plane/rollout.py tests/m1-perception/test_control_plane_deployment_templates.py tests/m1-perception/test_control_plane_rollout.py
git commit -m "feat(05.6-206): isolate runtime control and qualification"
```

### Task 3: Exact process/Machine recovery adapter

**Files:**
- Create: `src/polyarb/control_plane/fly_recovery.py`
- Create: `tests/m1-perception/test_control_plane_fly_recovery.py`
- Modify: `src/polyarb/control_plane/recovery_executor.py`

**Interfaces:**
- Produces: `FlyRecoveryAdapter.restart_exact_machine()` under dual confirmation and budget.

- [x] **Step 1: Write failing adapter tests**

Cover disabled mode, wrong app/Machine, missing independent confirmation,
active competing action, exhausted hourly/daily budget, stale action, API 4xx/
5xx, timeout, exact successful restart, and secret-free error rendering.

- [x] **Step 2: Prove red**

Run: `uv run pytest tests/m1-perception/test_control_plane_fly_recovery.py -q`

Expected: FAIL because adapter does not exist.

- [x] **Step 3: Implement capability-limited HTTPS adapter**

The adapter accepts immutable `allowed_targets: frozenset[(app,machine_id)]`, a
token provider, a bounded HTTP client, and an independent-health callback. It
never shells out to `flyctl`. Before POST restart it rechecks action/controller
epochs and independent health. Normalize results to
`restarted`, `stale-noop`, `not-confirmed`, `budget-exhausted`, or
`provider-unavailable`; never return response bodies.

- [x] **Step 4: Verify and commit**

Run adapter and executor tests; expected PASS.

```bash
git add src/polyarb/control_plane/fly_recovery.py src/polyarb/control_plane/recovery_executor.py tests/m1-perception/test_control_plane_fly_recovery.py tests/m1-perception/test_control_plane_recovery_executor.py
git commit -m "feat(05.6-206): fence exact Machine recovery"
```

### Task 4: Observe-only production gate

**Files:**
- Create: `alembic/versions/025_m1_runtime_observe.py`
- Create: `src/polyarb/control_plane/runtime_observe.py`
- Modify: `src/polyarb/config.py`
- Modify: `src/polyarb/cli_control_plane.py`
- Modify: `src/polyarb/control_plane/rollout.py`
- Modify: `Makefile`
- Create: `tests/alembic/test_025.py`
- Create: `tests/m1-perception/test_control_plane_runtime_observe.py`
- Modify: `tests/m1-perception/test_control_plane_cli.py`
- Modify: `tests/m1-perception/test_makefile_contract.py`
- Create: `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/evidence/runtime-observe-only.json`

- [x] **Step 1: Add read-only gate target contract**

Assert `runtime-observe-verify` reads controller decisions, compares them with
runtime facts, rejects missing/mismatched decisions, and invokes no mutation.

- [x] **Step 2: Add target**

```make
## runtime-observe-verify: Compare observe-only controller decisions with durable runtime facts; no recovery mutation.
runtime-observe-verify:
	@uv run python -m polyarb.cli_control_plane runtime-observe-verify --minimum-seconds "$(or $(minimum_seconds),1800)" --json
```

- [ ] **Step 3: Deploy exact observe-only release after authorization — NOT RUN**

Record exact Git SHA, rendered template digests, migration heads 022-025,
separate role grants, app/Machine identities, and 30 minutes of decision parity.
Do not enable any action class.

- [x] **Step 4: Persist local evidence and explicit NOT RUN production boundary**

The evidence must show no false mutation, bounded tick gaps, historical replay
parity, and current Dashboard/controller freshness. Commit only credential-free
evidence.

### Task 5: Job-recovery and process-recovery production gates

**Files:**
- Create: `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/evidence/runtime-job-recovery.json`
- Create: `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/evidence/runtime-process-recovery.json`
- Modify: `.planning/workstreams/m1-perception/STATE.md`
- Modify: `.planning/JOURNAL.md`
- Modify: `.planning/threads/market-observation-architecture.md`
- Create: `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/05.6-206-SUMMARY.md`

- [ ] **Step 1: Obtain exact mutation authorization — NOT GRANTED**

Before each controlled fault, present release SHA, app/Machine/job target,
fault, maximum effect, rollback, and evidence filename. Without explicit
authorization, record the gate as NOT RUN and do not mutate production.

- [ ] **Step 2: Enable and prove job recovery — NOT RUN**

Enable only heartbeat/retry/reclaim/circuit actions. Inject one bounded
job-level timeout. Require task-local detection, Dashboard/Telegram transition,
fenced action, recovery inside SLO, and automatic qualification epoch handling.

- [ ] **Step 3: Enable and prove process recovery — NOT RUN**

Enable exact allowlisted process/Machine action only after the job gate. Inject
one exact process loss. Require independent confirmation, one restart action,
budget decrement, service freshness preservation or explicit breaker, and
linked recovery.

- [ ] **Step 4: Final verification and closure**

Run `make runtime-fault-matrix`, `make runtime-controller-status`,
`make qualification-status`, authenticated Dashboard smoke, relevant full
pytest/Ruff/docs/build gates, and `make planning-status`. Update STATE/JOURNAL/
thread with VERIFIED versus NOT RUN boundaries. The SUMMARY records every plan
commit and production receipt. Rolling qualification continues automatically;
do not wait 24 hours to close implementation, and do not claim a certificate
until one is actually sealed.
