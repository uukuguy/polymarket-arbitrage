# M1 Runtime Lifecycle Hardening Implementation Plan

> Execute every task test-first. Production coordinator and qualification stay
> stopped until Tasks 1-4 are green and independently reviewed.

**Goal:** Remove competing timeout ownership, make every interruption leave an
authoritative recoverable fact, and prevent production-sized reducers from
restarting at zero.

**Architecture:** One closed runtime-policy registry feeds durable claim state
and every worker. Schedulers own cadence only. Attempt runtime owns lifecycle
deadlines. Reclaim closes the old epoch atomically. Long reducers resume from
generation-bound checkpoints, and qualification deployments use an explicit
cursor handoff.

## Task 1: Single runtime policy and scheduler ownership

**Files:**

- Create: `src/polyarb/control_plane/runtime_deadlines.py`
- Modify: `runtime_store.py`, all transactional worker modules,
  `scheduler.py`, `worker_loop.py`, `cli_control_plane.py`
- Test: runtime coverage, scheduler, worker-loop, CLI wiring tests

- [x] Add RED tests for exact eight-job policy coverage, unknown-job rejection,
  timeout ordering, and absence of local `_runtime_profile` definitions.
- [x] Add RED scheduler/role tests proving claimed work is not cancelled by an
  unrelated 105-second turn bound.
- [x] Implement the policy registry and replace every local profile.
- [x] Remove scheduler/role timeout ownership for worker attempts; move sync
  `run_once` calls off the event loop without abandoning their result.
- [x] Verify focused tests and Ruff.

## Task 2: Authoritative cancellation and expired-attempt closure

**Files:**

- Modify: `runtime_contract.py`, `postgres.py`, `runtime_models.py`
- Test: runtime contract and real PostgreSQL control-plane tests

- [x] Add RED watchdog tests for attempt deadline, progress stall, lease loss,
  and service-stop cancellation reasons.
- [x] Add RED reclaim test asserting the old attempt is no longer `running`, its
  failure event exists, and only then the next epoch starts.
- [x] Implement typed attempt cancellation and finalization.
- [x] Atomically close an expired current attempt during `claim_job` reclaim.
- [x] Verify cancellation/fencing fault matrix and PostgreSQL migration suite.

## Task 3: Production-sized reducer checkpoints

**Files:**

- Modify: `structure_worker.py`, `quote_admission.py`, `postgres.py`, artifact
  contracts as required
- Test: Structure certifier and Quote admission transactional suites

- [x] Add RED 1,117-range interruption test that must resume after the last
  verified range rather than range one.
- [x] Add RED 231-shard interruption test with the same resume requirement.
- [x] Persist generation-bound checkpoint cursor/digest and immutable partial
  proof/artifact state.
- [x] Reject checkpoints whose generation or policy version differs.
- [x] Verify crash-before-upload, crash-after-upload, stale-lease, and takeover
  matrices.

## Task 4: Task lanes and bounded graceful stop

**Files:**

- Modify: `scheduler.py`, `worker_loop.py`, `cli_control_plane.py`
- Test: scheduler service and CLI tests

- [x] Add RED tests proving a slow certifier cannot block unrelated lanes or
  SIGINT handling.
- [x] Derive lanes from declared job types and durable DAG authority.
- [x] Stop new claims immediately on service stop; persist checkpoint/failure at
  the next safe boundary; bound exit by policy shutdown grace.
- [x] Add DAG acyclicity and declared-successor invariant tests.

## Task 5: Qualification cursor handoff

**Files:**

- Modify: qualification service/store and deployment evidence contract
- Test: qualification service and real PostgreSQL tests

- [x] Add RED test rejecting a new release that labels offset-zero replay as
  continuous live evidence.
- [x] Implement predecessor cursor handoff with release/provenance verification,
  or explicitly label the epoch backfill-only.
- [x] Count only current-cursor observations toward the 24-hour gate.

## Task 6: Closure and production proof

**Files:**

- Create: `05.6-209-SUMMARY.md` and a new learning chapter
- Modify: STATE, JOURNAL, architecture thread, learning index, Plan 208 result
  evidence

- [x] Run focused suites, full `make test-m1`, Ruff, format, migration tests,
  `make planning-status`, and `make climb-check`.
- [x] Obtain independent code review with no CRITICAL/HIGH findings.
- [x] Replace the pre-rollout qualification `SELECT *` status path with revision
  029 stored bounded projections and prove a bloated predecessor cannot re-enter
  growth-bound JSON through recovering status.
- [ ] Build one immutable image and perform an image-only rollout preserving
  Machine IDs and non-image configuration hashes.
- [ ] Prove Structure certification, Quote admission, Quote certification, and
  opportunity publication complete without zero-restart or orphan attempts.
- [ ] Resume qualification only with verified cursor semantics; start the new
  86,400-second acceptance window from current live evidence.
