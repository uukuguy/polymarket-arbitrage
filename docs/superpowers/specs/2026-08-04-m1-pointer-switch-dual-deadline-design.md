# M1 Structure Pointer-Switch Dual-Deadline Design

**Date:** 2026-08-04  
**Status:** approved for implementation  
**Scope:** Structure publication `ready` handoff, scheduler child budgeting,
atomic pointer-switch deadline enforcement, recovery evidence, and production
acceptance

## Incident facts

Fly release 235 runs exact source
`b96478ad9797550c00851281182f47ddcee1b7c7` and image digest
`sha256:30e6bdd8094fc37f0c66e0763fe3550a4c0ee4e6470da3a8612604212902b448`.
The release fixed the generation-868 zero-row publication loop: production
children processed real 2,500-50,000-row slices, completed normalization and
certification, and sealed the full legacy/generation comparison.

The final `ready` handoff then failed deterministically. The scheduler assigns
the 15-second pointer-switch deadline to the entire
`python -m polyarb.snapshot structure-sync` child. Production attempt evidence
shows repeated `snapshot-subprocess-timeout` failures at roughly 15.3-15.9
seconds with `stderr_bytes=0`, `last_stage=NULL`, and
`chunks_processed=NULL`. The child therefore never reached the first
`snapshot-stage` marker or the atomic pointer transaction. By 07:23 UTC the
failure counter had reached 14 while generation 868 remained `ready`.

The root cause is a boundary error: a transaction safety deadline was applied
to process startup, settings/CLI composition, and pre-publication reads. The
current retry system remains active and maintenance cleanup continues, but no
retry can cross the deterministic 15-second startup boundary. This is a real
long-lived recovery failure, not a transient degraded result.

## Decision

Use two independent deadlines with different authorities:

1. The existing 75-second Structure child hard limit bounds the complete
   `ready` child lifecycle, including Python startup and CLI composition.
2. A 15-second pointer-transaction deadline begins only when
   `publish_structure_generation` enters its atomic writer transaction.
3. SQLite writer-lock acquisition is capped at five seconds within that
   transaction budget.
4. SQLite progress interruption plus explicit monotonic deadline checks abort
   and roll back work that crosses the transaction deadline.
5. Scheduler timeout/retry/alert behavior remains unchanged. Durable progress
   still resets a failure streak, while only a certified snapshot returns
   `RECOVERING` to `RUNNING`.

This does not enable Quote, switch generation readers from `legacy`, weaken
comparison/source-truth validation, or permit parent-process pointer writes.

## 1. Deadline ownership

`structure_attempt_slot_budget_s()` will no longer return 15 seconds for a
`ready` publication. Every Structure publication child receives the existing
75-second producer-lane hard limit. This remains below the production Quote
hard SLA and preserves the existing subprocess kill boundary.

The pointer transaction receives explicit parameters from the publication
layer:

- `transaction_deadline_s=15.0`;
- `writer_lock_timeout_s=5.0`.

`publish_structure_generation` records a monotonic deadline immediately before
opening the writer connection. It configures the SQLite connection's busy
timeout to the smaller writer-lock budget and installs a progress handler that
interrupts long-running SQLite virtual-machine work after the deadline.
Explicit checks run before each authority-changing statement and immediately
before `COMMIT`.

Any deadline or SQLite interruption rolls back the complete transaction. The
old singleton pointer, publication status, sync-window status, snapshot
validity, and drift identity remain unchanged. No timeout may be reported after
a successful commit.

## 2. Data and control flow

The successful path is:

1. Scheduler observes publication status `ready` and admits the shared
   producer lane.
2. Scheduler launches the normal schema-ready Structure child with a
   75-second outer timeout.
3. The child resumes the authenticated publication and enters
   `publish_structure_generation`.
4. The store validates publication counts, hashes, comparison receipt, source
   window, and drift identity under `BEGIN IMMEDIATE`.
5. The store atomically updates snapshot truth, the singleton generation
   pointer, publication status, and sync-window status, then commits within 15
   seconds.
6. The child returns a certified `SnapshotResult`; the scheduler records a
   succeeded attempt, resets the failure counter, and leaves recovery only
   after certified truth is online.

The cleanup worker and scheduler continue sharing `producer_lock`, so cleanup
cannot own its writer transaction concurrently with pointer publication. Quote
priority admission is unchanged.

## 3. Failure vocabulary and observability

Add a bounded failure kind `pointer-switch-deadline` for a transaction that is
interrupted or rejected by the 15-second budget. It crosses the child JSON
boundary and is stored in `snapshot_attempts.failure_kind`; it must not be
collapsed into `invalid-json` or a generic child error.

The scheduler health schedule output will distinguish:

- `generation_child_hard_limit_s=75`;
- `pointer_switch_transaction_deadline_s=15`;
- `pointer_switch_writer_lock_timeout_s=5`.

The previous ambiguous `pointer_switch_hard_deadline_s=15` label is removed.
`snapshot:latest_attempt`, `snapshot:failure_counter`, and scheduler state
remain the runtime recovery truth. The existing threshold alert fires once on
entry to `RECOVERING`; bounded retries continue indefinitely.

## 4. Alternatives rejected

### Raise the existing 15-second value to 75 seconds

This is the smallest patch, but it removes the intended bound from the atomic
authority switch. A blocked or unexpectedly expensive pointer transaction
would become indistinguishable from harmless Python startup.

### Add a dedicated lightweight pointer-switch CLI

This reduces startup cost but duplicates another process protocol and still
needs an independent transaction deadline. It is a larger change and does not
fix the underlying deadline-ownership mistake.

### Pause cleanup around `ready`

Cleanup already shares the producer lock and production attempts emitted zero
child stderr before entering the pointer transaction. Pausing maintenance
would mask resource pressure without correcting the deterministic outer
timeout boundary.

## 5. TDD and verification

Implementation starts with observed RED tests for:

- a `ready` publication receives the 75-second child envelope rather than the
  15-second transaction deadline;
- the health schedule reports all three unambiguous budgets;
- a transaction that exceeds its monotonic deadline rolls back every pointer,
  publication, snapshot, window, and drift mutation;
- writer-lock exhaustion returns `pointer-switch-deadline` without changing
  authority;
- a successful switch commits all authority fields atomically and cannot be
  reported as timed out afterward;
- child JSON accepts only the new bounded failure kind and retains stderr/
  attempt evidence;
- repeated pointer failures enter `RECOVERING`, and the first successful
  certified result resets the counter and state through the existing path.

Focused scheduler, CLI, publication, SQLite, health, cleanup-fairness, and Fly
configuration tests must pass before the full M1 suite, Ruff, documentation
checks, `git diff --check`, and planning drift checks.

## 6. Production acceptance

Deployment requires a new exact-SHA approval. Protected rollout remains:

- Structure enabled;
- generation reads `legacy`;
- Quote worker disabled;
- resident cleanup enabled with the current bounded settings;
- no manual pointer write, restart, or cleanup advance.

The release is accepted only after natural production evidence proves:

1. generation 868 moves from `ready` to `published` through the child path;
2. `current_structure_generation` points to 868 with matching authenticated
   comparison receipt and validation hash;
3. the scheduler records a succeeded certified snapshot, failure counter zero,
   and `RUNNING` state;
4. cleanup continues advancing and eventually respects the two-generation
   retention floor;
5. localhost and public `/healthz` latency recover after the one-time full
   comparison load, without repeated timeout or TLS failure;
6. Quote remains off and legacy reads remain active until their separate UAT
   gates are explicitly approved.

Until all six conditions hold, M1 remains in production recovery and is not
declared complete.
