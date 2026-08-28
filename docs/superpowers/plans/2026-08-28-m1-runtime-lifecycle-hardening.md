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

## Task 10: Probe-release retry authority

- [x] Add a real-PostgreSQL RED test proving a due circuit probe derives its
  next holdoff from the job's central retry policy rather than five minutes.
- [x] Separate lease-independent `RuntimeRetryPolicy` from attempt deadlines;
  remove placeholder lease values from both failure transactions.
- [x] Route ordinary retry, composed recovery retry and probe release through
  the same budget/base/cap authority and current failure count.
- [ ] Re-run focused/full gates, commit, rebuild the exact image and restart the
  controller canary/1,800-second evidence window from a new lease epoch.

## Task 11: Actionable evidence-window failure semantics

- [x] Add RED tests proving an insufficient observe window reports its measured
  available seconds and required seconds without exposing the scoped DSN.
- [x] Preserve the predefined `RuntimeObserveVerificationError` reason at the
  CLI boundary while continuing to redact unknown runtime/provider failures.
- [x] Keep the gate fail-closed and nonzero; diagnostics must never auto-relax
  freshness, gap, replay, parity, recovery-action, or duration requirements.
- [x] Re-run the full M1 gate: 3,987 passed, one skipped and one expected xfail
  in 1,544.26 seconds with no outer timeout.
- [x] Pass climb 50/50, planning no-drift, Ruff, format, JSON and diff gates.
- [ ] Commit, supersede `352cb3ca`, and restart the exact
  image/controller evidence window before any sibling Machine update.

## Task 12: Snapshot-clock, request-round, and nested-shutdown audit

- [x] Reproduce the coordinator canary status failure three times and prove the
  read used an early client clock with later `READ COMMITTED` heartbeat facts,
  producing negative lease/progress ages.
- [x] Move production operator reads to one database-owned time inside one
  `REPEATABLE READ READ ONLY` snapshot; keep explicit time injection only for
  deterministic tests.
- [x] Collapse both operator snapshot and opportunity page to one PostgreSQL
  data statement and one client execute round, matching the existing
  `connect + one statement` HTTP envelope instead of increasing it.
- [x] Add structural operation-round guards plus real production-equivalent
  timing proof: snapshot 2.85–4.56s, three HTTP 200 reads at 3.53–4.81s, and
  opportunity page 5.15s.
- [x] Audit subprocess cancellation in the compatibility daemon; unify bounded
  TERM/KILL/pipe drain, bound ProducerSupervisor KILL wait/output drain, and
  remove the parent 5s timeout that preempted the child 30s cleanup contract.
- [x] Align Uvicorn graceful shutdown to the maximum child contract and make
  Fly's 40s kill timeout the sole process-level backstop.
- [x] Validate aggregated JSON scalars/arrays at the read boundary, close
  changed-line Pyright diagnostics, add behavioral execute/result-set tests,
  and accept already-exited TERM/KILL races without false supervisor failure.
- [x] Run complete focused/full/climb/planning/static gates: 3,997 passed, one
  skipped and one expected xfail in 1,555.91 seconds; climb 50/50, planning
  no-drift, Pyright/Ruff/format/JSON/diff checks pass.
- [ ] Commit the amended Plan 209 evidence, rebuild the exact image, and restart controller canary from
  a fresh lease epoch before resuming coordinator rollout.

## Task 13: Formal Fly platform-shutdown authority

**Files:**

- Modify: `deploy/control-plane/fly-control-alert-delivery.toml.template`
- Modify: `deploy/control-plane/fly-control-alert.toml.template`
- Modify: `deploy/control-plane/fly-control-api.toml.template`
- Modify: `deploy/control-plane/fly-control-worker.toml.template`
- Modify: `deploy/control-plane/fly-qualification-worker.toml.template`
- Modify: `deploy/control-plane/fly-runtime-controller.toml.template`
- Modify: `deploy/control-plane/fly-runtime-event-writer.toml.template`
- Test: `tests/m1-perception/test_control_plane_deployment_templates.py`
- Test: `tests/m1-perception/test_control_plane_rollout.py`

**Interfaces:**

- Consumes: the Task 12 internal maximum shutdown owner of 30 seconds.
- Produces: every rendered formal service declares `kill_signal = "SIGTERM"`
  and `kill_timeout = 40`; controller rollout permits only image plus those two
  lifecycle fields to differ.

- [x] Add a RED parameterized template test loading all seven TOML files and
  asserting SIGTERM plus 40 seconds, and a renderer test proving the fields
  survive substitution.
- [x] Run
  `uv run pytest -q tests/m1-perception/test_control_plane_deployment_templates.py tests/m1-perception/test_control_plane_rollout.py`
  and require failure because formal templates currently inherit Fly's
  five-second default.
- [x] Add the two top-level fields to all seven templates with a comment that
  40 = 30 seconds maximum internal owner + 10 seconds terminal/interpreter
  margin; do not change env, process, VM, restart, HTTP, secret, or role scope.
- [x] Re-run the focused tests and require all pass; then run the complete M1,
  climb, planning, Pyright/Ruff/format/JSON/diff gates without an outer timeout.
- [ ] Mark remote digest `sha256:9bb6ebde…c1989` superseded before deployment,
  commit Task 13 with the Plan 209 summary, build/push/verify the new exact
  amd64 image, and update only controller Machine `6e82036dce4958`.
- [ ] Prove controller Machine ID, region, env, guest, restart policy,
  observe-only mode and empty allowlist are unchanged; only image,
  `kill_signal`, and `kill_timeout` may differ. Start a fresh lease epoch and
  pass 120/300/1,800-second observe gates before any sibling rollout.

## Task 14: Bound and authenticate immutable build downloads

**Files:**

- Modify: `Dockerfile`
- Test: `tests/m1-perception/test_control_plane_migration_image.py`

**Interfaces:**

- Consumes: fixed Supercronic v0.2.30 linux/amd64 artifact, measured at
  12,432,517 bytes.
- Produces: one 500-second aggregate download owner, 15-second connect bound,
  240-second per-transfer bound, one transient retry, and pinned SHA256 proof.

- [x] Add a RED Dockerfile contract test for aggregate/connection/transfer
  bounds, single retry, and exact v0.2.30 checksum.
- [x] Run the focused test and require failure on the unbounded `curl -fsSL`.
- [x] Add the derived curl/owner bounds and verify SHA256 before chmod; do not
  change the Supercronic version, runtime packages, user, entrypoint, or app
  bytes.
- [ ] Re-run focused and complete relevant static/planning gates, commit Task
  14, then rebuild the exact amd64 image and verify its embedded checksum.
