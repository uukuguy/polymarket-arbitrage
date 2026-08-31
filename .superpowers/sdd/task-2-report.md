# Task 2 Report — Exact producer boot identity and safe-boundary claiming

Status: COMPLETE

## Implementation

- Added `FaultRuntime`, `PassThroughFaultRuntime`, `CleanupResult`, the runtime
  protocol, safe-boundary claim, active cleanup helper, and fail-open builder.
- Claims run through `asyncio.to_thread()` only at producer batch boundaries.
  `consume()` reads only the process-local `FaultController`.
- Cleanup uses `FaultController.clear()` with the SQLite receipt as its writer,
  so memory is cleared before the `cleaned` append and a receipt failure freezes
  later admission while leaving the data plane pass-through.
- Fault-store registration/claim/cleanup failures log only component, reason,
  and exception type; exception messages and upstream material are excluded.
- `ProducerSupervisor` generates and exports a new UUID4
  `POLYARB_PRODUCER_BOOT_ID` for every attempt.
- Isolated workers bind component, release, machine, boot, supervisor run, and
  attempt from exact process/environment identity. Missing or invalid identity
  yields degraded pass-through without blocking worker construction.
- The in-daemon path creates explicit UUID4 identities for Candidate,
  Discovery, and Reconciliation. The parent daemon creates a distinct
  `notification` runtime from its daemon boot and injects it into
  `OpportunityWatcher`.
- Candidate claims before loading/selecting its group batch. Discovery and
  Reconciliation claim after their durable cursor/resource/window decisions
  and immediately before Gamma page fetch. Notification claims before reading
  its durable pending-outbox batch.
- Candidate, Discovery, Reconciliation, and Notification invoke active cleanup
  from their run `finally` paths.
- No fault-store read was added to `GammaClient._get`,
  `ClobReaderClient.get_books`, or Telegram transport.
- Production control remains dormant: the construction seam reads the later
  `upstream_fault_control_enabled` setting with a default of `False`; this task
  did not add config, HTTP, CLI, deployment, or mutation enablement.

## Files

- `src/polyarb/perception/fault_runtime.py` (new)
- `src/polyarb/perception/supervisor.py`
- `src/polyarb/perception/worker_cli.py`
- `src/polyarb/daemon/main.py`
- `src/polyarb/daemon/opportunity_watcher.py`
- `src/polyarb/perception/candidate_watcher.py`
- `src/polyarb/perception/discovery.py`
- `src/polyarb/perception/reconciliation.py`
- `tests/perception/test_fault_runtime.py` (new)
- `tests/perception/test_supervisor.py`

The pre-existing modifications to `.superpowers/sdd/progress.md`,
`findings.md`, `progress.md`, and `task_plan.md` were neither edited nor staged.

## RED

Command:

```text
uv run pytest \
  tests/perception/test_fault_runtime.py \
  tests/perception/test_supervisor.py -q
```

Observed:

```text
ERROR tests/perception/test_fault_runtime.py
ModuleNotFoundError: No module named 'polyarb.perception.fault_runtime'
!!!!!!!!!!!!!!!!!!!! Interrupted: 1 error during collection !!!!!!!!!!!!!!!!!!!!
```

This was the expected missing-feature RED before any production implementation
of `fault_runtime.py`.

## GREEN and proportional verification

Focused runtime/supervisor:

```text
uv run pytest tests/perception/test_fault_runtime.py \
  tests/perception/test_supervisor.py -q
.........................................................                [100%]
```

Prescribed proportional suite:

```text
uv run pytest \
  tests/perception/test_fault_runtime.py \
  tests/perception/test_supervisor.py \
  tests/perception/test_candidate_watcher.py \
  tests/perception/test_discovery.py \
  tests/perception/test_reconciliation.py \
  tests/daemon/test_opportunity_watcher.py -q
```

Result: `341 tests` collected and all passed.

Static/diff verification:

```text
uv run ruff check <all Task 2 source/test files>
All checks passed!

git diff --check
# exit 0
```

Planning guard before and after commit:

```text
make planning-status
✓ no drift detected — every shipped plan has a SUMMARY.
```

## Task/subprocess cleanup evidence

- The new cancellation test waits on an `asyncio.Event` signalling the claim
  boundary, cancels the scheduler, awaits the task to completion, and verifies
  exactly one cleanup reason. It uses no fixed sleep.
- The supervisor boot-ID test runs two bounded subprocess attempts and awaits
  `ProducerSupervisor.run()` through both receipts.
- The complete prescribed suite exited normally with no pending-task,
  un-awaited-coroutine, subprocess transport, or resource leak warnings.
- Existing Candidate executor shutdown and Gamma client `aclose()` paths remain
  in their original `finally` blocks; fault cleanup executes before those
  existing closes.

## Self-review

- Verified every identity uses UUID version 4 and the exact release/machine/
  component/boot tuple.
- Verified supervisor boot identity is generated inside the attempt loop, not
  once per supervisor run.
- Verified authority access exists only in registration, safe-boundary sync,
  and cleanup; hot-path `consume()` cannot reach SQLite.
- Verified failed claim leaves the controller unchanged and warnings exclude
  exception text.
- Verified cleanup ordering delegates persistence through
  `FaultController.clear()` so its freeze-on-receipt-failure invariant is
  preserved.
- Verified test stubs without `_fault_runtime` remain constructor-compatible
  through pass-through runner fallbacks.
- Verified no later-task surfaces (Settings field, HTTP, CLI, adapters,
  deployment, feature enablement) were added.

## Commit

```text
7538717 feat(m1): bind fault intents to producer boots
```

## Concerns

- No blocking concern.
- Correction after review: the earlier report incorrectly described an armed
  cancellation as an attempted `cleaned` append that would fail and freeze the
  controller. That was not a lifecycle-valid relinquish and could be reported
  as persisted by the first implementation. The fix below now persists
  ownership-bound `abandoned` for live armed claims and `expired` for elapsed
  armed claims; it reserves `cleaned` strictly for a `contained` tail.

## Review fix — lifecycle-valid claim relinquish

Status: COMPLETE

### Findings addressed

1. Added `FaultAuthorityStore.relinquish_claim()`, an `BEGIN IMMEDIATE`
   transaction that validates the existing history, verifies the exact
   process-held ownership capability, selects a valid terminal from the actual
   tail, appends it, validates the resulting history again, then commits.
2. Locked the terminal matrix:
   - live `armed` → `abandoned`;
   - expired unconsumed `armed` → `expired`;
   - `injected` or `detected` without containment → `abandoned`;
   - `contained` → `cleaned`.
3. Generic `append_event()` now requires ownership when `abandoned` or
   `expired` follows a claimed state. Pre-claim authority expiry/abandonment
   from `authorized` remains available to the authority.
4. `CleanupResult` returns the actual durable terminal state.
   `receipt_persisted=True` is returned only after the authority transaction
   validates the complete post-append history.
5. `FaultRuntime.sync_before_batch()` now creates and shields the
   `claim_pending` task. Cancellation waits for the real SQLite call to settle,
   adopts any returned ownership capability, relinquishes the claim through
   the same valid terminal path, and only then re-raises `CancelledError`.
6. Each sync boundary detects process-local expiry. It clears memory first,
   persists ownership-bound `expired`, and can then claim a later intent for
   the same boot without a process restart.

### Review-fix RED

Command:

```text
uv run pytest \
  tests/perception/test_fault_authority.py \
  tests/perception/test_fault_runtime.py -q
```

Observed: `9 failed`. The failures were the intended missing behaviors:
`FaultAuthorityStore.relinquish_claim` absent, claimed terminal ownership not
enforced, armed cleanup lacked an actual terminal result, a cancelled committed
claim remained `armed`, and an expired unmatched claim remained active.

The cancellation test is deterministic: a `threading.Event` barrier blocks
return only after the real SQLite `claim_pending()` transaction commits. The
caller is cancelled, the barrier is released, and the test verifies a valid
durable `abandoned` tail and no process-local active fault. No fixed sleep is
used.

### Review-fix GREEN

Core controller/authority/runtime:

```text
uv run pytest \
  tests/perception/test_fault_control.py \
  tests/perception/test_fault_authority.py \
  tests/perception/test_fault_runtime.py -q
123 passed
```

Prescribed proportional suite plus store/schema regressions:

```text
uv run pytest \
  tests/perception/test_fault_control.py \
  tests/perception/test_fault_authority.py \
  tests/perception/test_fault_runtime.py \
  tests/perception/test_supervisor.py \
  tests/perception/test_candidate_watcher.py \
  tests/perception/test_discovery.py \
  tests/perception/test_reconciliation.py \
  tests/daemon/test_opportunity_watcher.py \
  tests/perception/test_store.py \
  tests/m1-perception/test_schema_lockstep.py \
  tests/m1-perception/test_sqlite_store.py -q
843 passed
```

Additional store/migration/routing regressions:

```text
uv run pytest \
  tests/m1-perception/test_l3_evidence_store.py \
  tests/m1-perception/test_sqlite_store_l2_getters.py \
  tests/m1-perception/test_sqlite_store_migration.py \
  tests/routing/test_neg_risk_quote_store.py -q
128 passed
```

Static and planning verification:

```text
uv run ruff check <all Task 2 source/test files>
All checks passed!

git diff --check
# exit 0

make planning-status
✓ no drift detected — every shipped plan has a SUMMARY.
```

No pending-task, un-awaited-coroutine, subprocess transport, or resource leak
warning appeared in any GREEN run.

### Review-fix commit

```text
96700d6 fix(m1): relinquish fault claims safely
```

### Review-fix concerns

No blocking concern. Later adapter tasks remain responsible for appending
`injected`, `detected`, and `contained`; Task 2 now relinquishes safely and
qualification-validly from whichever of those durable tails actually exists.

## Second review fix — frozen claim gate and daemon wiring proof

Status: COMPLETE

### Findings addressed

1. `FaultRuntime.sync_before_batch()` now checks
   `FaultController.frozen` as its first executable condition. A frozen
   controller returns before reading the wall/monotonic clocks, creating an
   asyncio task, scheduling `to_thread`, or calling any authority method.
2. The freeze regression uses a real SQLite authority subclass that performs
   the initial durable claim and deterministically fails relinquish
   persistence. It verifies:
   - the controller freezes and clears process memory;
   - the next boundary performs zero additional `claim_pending` calls;
   - the real SQLite history is byte-for-byte unchanged;
   - no ownerless second `armed` fact appears; and
   - hot-path consumption remains pass-through.
3. The production daemon construction path is now factored through
   `_build_daemon_perception_workers()` and `main()` calls that same helper.
   The behavioral capture test replaces the four worker builders, then proves
   Candidate, Discovery, Reconciliation, and OpportunityWatcher receive four
   distinct `FaultRuntime` instances with exact component, release, machine,
   and UUID4 boot identities. It does not inspect source text.
4. No Settings, HTTP, CLI, adapter, or deployment surface was added.

### Second-review RED

Command:

```text
uv run pytest \
  tests/perception/test_fault_runtime.py \
  tests/daemon/test_main_fault_wiring.py -q
```

Observed: `2 failed, 17 passed`.

- Frozen controller failure: expected `claim_count == 1`, observed `2`.
- Daemon wiring failure: production-used
  `_build_daemon_perception_workers` behavioral seam was absent.

### Second-review GREEN

Focused runtime/main/daemon:

```text
uv run pytest \
  tests/perception/test_fault_runtime.py \
  tests/daemon/test_main_fault_wiring.py \
  tests/daemon/test_opportunity_watcher.py -q
29 passed
```

Prescribed proportional suite plus daemon startup and store/schema regressions:

```text
uv run pytest \
  tests/perception/test_fault_control.py \
  tests/perception/test_fault_authority.py \
  tests/perception/test_fault_runtime.py \
  tests/perception/test_supervisor.py \
  tests/perception/test_candidate_watcher.py \
  tests/perception/test_discovery.py \
  tests/perception/test_reconciliation.py \
  tests/daemon/test_main_fault_wiring.py \
  tests/daemon/test_opportunity_watcher.py \
  tests/daemon/test_l2_main_startup.py \
  tests/perception/test_store.py \
  tests/m1-perception/test_schema_lockstep.py \
  tests/m1-perception/test_sqlite_store.py -q
877 passed
```

Static and planning verification:

```text
uv run ruff check <all Task 2 source/test files>
All checks passed!

git diff --check
# exit 0

make planning-status
✓ no drift detected — every shipped plan has a SUMMARY.
```

No fixed sleep was added. No pending-task, un-awaited-coroutine, subprocess
transport, main-startup harness, or resource leak warning appeared.

### Second-review commit

```text
8ab8091 fix(m1): stop claims after fault freeze
```

### Second-review concerns

No blocking concern.
