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
- [x] Close the second status consumer and active-writer gap with revision 030:
  operational snapshot selects fixed columns, active facts append as normalized
  rows, restart replays them in 500-row pages, and certificate verification uses
  a fixed scalar epoch projection.
- [x] Audit the complete runtime-v2 timeout/stop surface: centralize DB
  connect/statement/lock envelopes, remove default-executor shutdown joins,
  enforce first-cancel drain plus second-cancel grace expiry, and prove
  qualification/watchdog/API shutdown under stalled blocking calls.
- [x] Move barrier eligibility into PostgreSQL: Structure and Quote certifiers
  start `waiting` and become `runnable` atomically with the final terminal
  receipt; prove the boundary using independent real PostgreSQL connections.
- [ ] Build one immutable image and perform an image-only rollout preserving
  Machine IDs and non-image configuration hashes.
- [ ] Prove Structure certification, Quote admission, Quote certification, and
  opportunity publication complete without zero-restart or orphan attempts.
- [ ] Resume qualification only with verified cursor semantics; start the new
  86,400-second acceptance window from current live evidence.

## Task 7: Operation-round and evaluator recovery audit

**Files:**

- Modify: `db_role_contract.py`, `postgres.py`, `qualification_service.py`,
  `blocking_bridge.py`, `tools/climb/eval_local.py`
- Test: role contract/admin, qualification PostgreSQL/service, climb evaluator

- [x] Replace per-object authority probes with fixed-round catalog queries and
  prove production preflight stays inside the request envelope.
- [x] Install scoped search path and database timeouts through startup options
  plus one centrally bounded post-connect verification round, preserving
  Session Pooler compatibility without an unbounded bootstrap window.
- [x] Initialize qualification once per process and batch the healthy append
  path into fixed SQL rounds while retaining exact sequential terminal logic.
- [x] Send cooperative stop before grace detach and refuse later qualification
  SQL after a stop request.
- [x] Remove climb's universal 120-second outer kill and atomically checkpoint
  exact-git-head/full-argv gate progress for interruption-safe resume.
- [x] Replace runtime-controller per-candidate connection writes with one
  lease-fenced bulk decision transaction and move the synchronous turn behind
  cooperative stop plus recovery-DB grace isolation.
- [x] Move mixed async/synchronous alert turns off the signal loop and prevent
  claim/provider results from starting later finish SQL after cooperative stop.
- [x] Re-run the fresh full M1/climb/planning gates, commit the amended Plan 209
  summary, and build a new immutable image; all earlier images are superseded.

## Task 8: Provider retry multiplication and proof-loop interruption audit

**Files:**

- Modify: `runtime_deadlines.py`, `postgres.py`, `gamma_client.py`,
  `clob_client.py`, `r2_sync.py`, `cli_control_plane.py`
- Test: runtime policy/PostgreSQL, provider clients, watchdog and cloud-soak CLI

- [x] Add RED policy tests proving provider calls get one inner attempt, a
  request timeout strictly below the worker I/O envelope, and one centralized
  durable backoff formula shared by both retry transactions.
- [x] Build formal Gamma/CLOB/R2 clients from that provider policy instead of
  inheriting legacy SDK defaults or hidden retries.
- [x] Collapse watchdog Machine state/restart collection to two bounded rounds
  per app: one list read plus one parallel exact-Machine detail round, preserving
  `request.restart_count` semantics without multiplying latency by target count.
- [x] Move a cloud-soak sample off the signal loop, send a cooperative stop hint,
  and refuse the database append when stop wins before the write boundary.
- [x] Re-run focused RED/GREEN suites, the complete M1/climb/planning gates, and
  supersede the current image if any executable byte changes.

## Task 9: Resource-safe production preflight

- [x] Prohibit live-Machine interpreter diagnostics in the rollout contract;
  use operator-host fixed-round catalog proof plus required secret-name topology.
- [x] Record the diagnostic-induced controller OOM/restart as a real runtime
  effect and discard the pre-restart observe-only continuity window.
- [x] Canary the controller before sibling rollout; detect the Session Pooler
  startup-options regression and restore the exact prior digest/config.
- [x] Reintroduce active-session `set_config`/readback as one autocommit round
  bounded by the central database policy; close on timeout or mismatch.
- [ ] Build the superseding image, canary it on the unchanged controller, then
  require a fresh 1,800/90/90 gate on its new lease epoch before sibling updates.
