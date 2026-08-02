# Structure Publication Contract Supersession

## Problem

A Structure publication is durably bound one-to-one to a completed source window. Before this change, that durable identity did not include the normalization contract. A process upgrade therefore resumed publication 846 after its `issues` component was complete and certification had started, even though the new code required different normalized rows and certification evidence. Immutable certification guards correctly prevented rewriting the generation, but the scheduler could only retry the same incompatible publication forever.

## Contract identity

`structure_publications.normalization_contract_version` records the semantic normalization contract used to create every generation row. The application owns a fixed current version constant. New publications persist the current version in the same transaction that creates the snapshot and publication.

The migration adds a nullable column. `NULL` means unknown legacy contract and is never treated as compatible. Compatibility requires exact string equality with the current version. This deliberately avoids inferring compatibility from the component cursor: a version covers the whole normalization pipeline, and an old prefix cannot safely be relabelled as current.

## Atomic supersession

Before any publication normalization, certification, or pointer switch, the worker asks the store to reconcile the active publication contract. If the active publication is `writing` or `ready` and its version differs from the current version (including `NULL`), one `BEGIN IMMEDIATE` transaction:

1. changes the publication to `failed` with exact `failure_reason='publication-contract-superseded'`;
2. changes its unpublished Structure snapshot to terminal failed state without making it valid or published;
3. changes the bound completed source window to `failed` with the same exact reason;
4. leaves all generation rows and `current_structure_generation` untouched.

The compare-and-set predicates make the operation idempotent and race-safe. Repeating reconciliation returns the same superseded outcome and does not create another successor, alter evidence, or touch the current pointer.

Published publications are historical evidence and are never superseded. An exact-version active publication resumes from its existing durable cursor unchanged.

## Scheduler flow

The invocation that performs supersession returns a distinct machine-readable checkpoint, not an ordinary snapshot failure. The subprocess/scheduler contract recognizes this checkpoint, emits a warning/audit marker, and treats the attempt as controlled progress rather than incrementing an unbounded failure counter.

No successor window or snapshot is created inside the supersession transaction. On the next natural scheduler admission, normal Structure sync finds no resumable window, creates a new open window, collects a fresh complete source catalogue, and only then reserves the next snapshot id. With failed snapshot 846 preserved, the next publication id is snapshot 847. This prevents reuse of stale source rows, mixed contract identity, and fake publication success.

## Failure and migration behavior

- Missing contract column is added during schema initialization before publication work.
- Legacy snapshot-status backfill excludes snapshots bound to active `writing` or `ready` publications. A building generation has `is_valid=0` until certification and must not be mistaken for a completed failed snapshot.
- Legacy `NULL` active rows fail safe by supersession.
- Schema initialization does not supersede rows by itself; runtime reconciliation provides the warning/audit and controlled checkpoint.
- Any transaction race or impossible publication/window/snapshot state remains a real failure and follows existing scheduler failure handling.
- The current pointer remains on 845 until a fully normalized and certified fresh generation publishes.

### Repairing a pre-existing split state

Production snapshot 846 was already `failed` more than three hours before the contract-supersession deployment while its publication remained `writing` and its source window remained `complete`. The legacy snapshot-status backfill created this state by deterministically mapping every Structure row with `is_valid=0` to failed, including an unfinished generation.

Reconciliation accepts exactly this recoverable split: an active incompatible publication, an already-failed invalid/unpublished Structure snapshot, a complete bound window, and no current pointer to that snapshot. It authenticates the already-failed snapshot but does not rewrite its `finished_at_ms` or other failure evidence. In the same transaction it terminal-fails only the publication and window with `publication-contract-superseded`. Any other partial combination remains fail-closed.

The ordinary three-row supersession path remains atomic. A forced failure after the publication and snapshot compare-and-set operations must roll back every mutation, proving that the reconciliation transaction itself cannot create the split state.

## Tests

Tests must first reproduce the production state: pointer 845, publication/snapshot 846, `issues|done`, certification started, and a `NULL` or old contract. They prove one call atomically fails publication/snapshot/window with the exact reason, preserves pointer and generation rows, and returns the supersession checkpoint. A second call proves idempotence. A subsequent natural admission must create a fresh window and eventually reserve snapshot 847, never resume 846.

Additional tests cover exact-version resume, fresh publication version persistence, legacy-column migration, a `NULL` active publication, no failure-counter escalation for the controlled checkpoint, and warning/audit visibility. Existing publication, scheduler, migration, and full-repository suites remain green.

The migration regression additionally reproduces the pre-existing 846 split, verifies that schema initialization no longer changes an active building snapshot, verifies exact split-state repair without changing its historical `finished_at_ms`, injects a late transaction failure to prove rollback, and then exercises natural fresh-window recovery to 847.
