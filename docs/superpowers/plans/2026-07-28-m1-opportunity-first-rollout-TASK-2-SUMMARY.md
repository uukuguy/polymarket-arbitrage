# Task 2 Summary — Group-Certified Candidate Watcher

## Delivered

- Added a fail-closed `GroupStructureReader` for one current certified group.
- Added a Candidate Watcher with the exact `before → ordered all-leg books →
  after` membership check.
- Added atomic group quote-batch construction using the existing top-book
  normalization rules; this path never invokes the all-known-token subprocess collector.
- Added durable append-only candidate terminal facts carrying `next_due_at_ms`,
  `priority_class`, `last_result`, the effective interval, and scheduling reason.
- Added configurable high/normal/explore controller inputs and bounded failure
  backoff. A failed known candidate retains its prior priority class.
- Added a restart-safe due scheduler that serves high-priority candidates first
  while retaining durable due times for every class.
- Added a process-local runtime projection mutated from the exact durable writer
  fact and injected that runtime into the HTTP app state for Task 6.
- Added feature-flagged sibling-task wiring. The flag is off by default; the
  legacy Quote worker, focused watcher, and opportunity read path remain intact.
- Independent review remediation made Quote-batch publication and its positive
  terminal fact one transaction, with a final current-membership revalidation.
- Started terminal writes complete under cancellation and converge runtime from
  the returned durable fact before cancellation is re-raised.
- The scheduler contains/retries cycle failures with observable supervisor
  failure/recovery state rather than silently dying.
- Per-cycle bounds, reserved normal/explore slots, and configurable per-group
  timeout provide real anti-starvation while retaining high-lane priority.
- Failure backoff clamps the exponent before calculation, including arbitrarily
  large durable failure counts.

## Safety and Scope

- Observer-only: no wallet, signing, balances, order placement, or real-money behavior.
- No Discovery, Full Reconciliation, incidents, public API, Dashboard, deployment,
  or production enablement was added.
- Universe discovery remains statistical; this slice makes no zero-miss claim.
- No executable operator command was introduced, so no Make target was required.

## TDD Evidence

```text
uv run pytest tests/perception/test_group_structure.py \
  tests/perception/test_candidate_watcher.py -q
RED: ModuleNotFoundError for both new modules

uv run pytest tests/m1-perception/test_l1_quote_worker_wiring.py \
  -q -k candidate --maxfail=1
RED: create_app rejected candidate_watcher_runtime

uv run pytest tests/perception/test_group_structure.py \
  tests/perception/test_candidate_watcher.py \
  tests/routing/test_focused_quote_collector.py \
  tests/daemon/test_opportunity_watcher.py \
  tests/m1-perception/test_l1_quote_worker_wiring.py -q
GREEN: all focused and legacy-path tests passed
```

Independent-review RED → GREEN additions cover:

- membership supersession between the second Structure read and SQLite commit;
- rollback when the positive-fact insert fails after the batch insert begins;
- cancellation after both positive and unavailable commits;
- supervisor retry and recovery after a cycle-source exception;
- stuck high-priority work with bounded normal/explore progress;
- one reserved slot rotating fairly across both lower lanes; and
- a durable failure count of 100000 clamping and persisting without overflow.

The final proportional suite contains 155 passing tests across Task 1/Task 2,
legacy focused collection, opportunity ledger, quote store, watcher, and daemon wiring.

## Review Notes

Self-review caught and corrected a misplaced runtime keyword in daemon wiring
before final verification. The first independent review returned four Important
findings; deterministic race/cancellation/supervision/overflow/fairness tests now
cover their fixes. Re-review remains owned by the parent rollout workflow.
