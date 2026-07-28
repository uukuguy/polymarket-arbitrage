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

## Review Notes

Self-review caught and corrected a misplaced runtime keyword in daemon wiring
before final verification. The independent Task 2 review gate remains owned by
the parent rollout workflow after this commit.
