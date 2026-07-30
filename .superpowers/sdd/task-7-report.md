# Task 7 Implementer Report

Status: IMPLEMENTATION GREEN — local qualification only

## Scope

Task 7 completes the typed upstream-fault orchestrator, immutable evidence
export, independent candidate/final evaluation, and evaluator-signed
finalization protocol. It supports exactly:

```text
gamma-timeout gamma-partial gamma-malformed gamma-cursor
clob-missing-leg clob-429 clob-latency telegram-failure
```

No cloud, production, deploy, wallet, order, or real-money operation occurred.
Existing SQLite/disk/load/process/restart/deploy chaos primitives remain
separate.

## Implemented truth chain

1. `perception_chaos execute` performs fail-closed preflight before network
   access, collects a green read-only baseline, resolves exact producer
   release/machine/boot/component identity, writes an exclusive typed intent,
   arms through the doubly signed HTTP control endpoint, and observes the
   matching producer-owned injection plus one Incident or Gamma partial
   coverage fact.
2. Cleanup executes for every `BaseException`, including timeout, malformed
   response, cancellation, `KeyboardInterrupt`, and `SystemExit`. A missing or
   invalid cleanup receipt freezes the remaining matrix. Recovery must be a
   newer component-specific business-writer receipt before an immutable
   `evidence.json` is exported.
3. The production transport is real HTTP, not a producer primitive shim.
   Starlette + SQLite local integration tests exercise all eight fault kinds
   through arm, observe, doubly signed cleanup, recovered state, and read-only
   export. The CLI rejects missing identity/authority arguments before creating
   a transport or touching the network.
4. `export_fault_envelope` opens SQLite read-only and preserves the complete
   canonical intent, runtime, lifecycle, hashes, recovery writer identity, and
   projection state. Candidate evidence stops at `RECOVERED`; it cannot call
   the evaluator or append `VERIFIED`.
5. The independent candidate evaluator recomputes every intent/event/tail
   digest and validates exact runtime, target, parameters, nonce, injection,
   detection/coverage, cleanup/recovery order, writer family, open-state
   counts, and integrity gates. A third, distinct evaluator secret signs only
   a production-fault PASS candidate.
6. The disabled-by-default finalizer validates double control authentication,
   the independent evaluator signature, exact recovered tail/runtime/fault
   binding, and fresh nonce before appending one `VERIFIED` event containing
   only `verdict_id` and `verdict_digest`. Exact artifact retry is idempotent;
   nonce replay and conflicting verdicts are audited and rejected.
7. The final evaluator re-reads the post-finalization evidence, validates the
   candidate signature without either control secret or HTTP mutation
   capability, and requires the exact `VERIFIED` event/digest.

## Schema and compatibility

- `FaultEventState.VERIFIED` has strict evidence validation.
- Finalization authorization was added to the authority schema.
- Existing Task 3 databases are migrated by rebuilding the auth table while
  preserving row IDs, hashes, event history, and foreign keys. The migration
  test starts with real legacy runtime/nonce/intent/event rows and requires a
  clean `foreign_key_check`.
- Settings require three non-empty, pairwise-distinct secrets when finalization
  is enabled; all new capabilities remain disabled by default.

## Operational entry points

```text
make evaluate-upstream-fault-candidate
make finalize-upstream-fault
make evaluate-upstream-fault-final
```

The candidate evaluator runs without ordinary/fault control secrets. The
finalizer runs without the evaluator secret. The final evaluator runs without
ordinary/fault control secrets and with HTTP mutation calls replaced by
fail-fast test sentinels.

## TDD and branch-wide gate remediation

RED tests were added before implementation for exact preflight, all eight CLI
dispatches, cleanup across `BaseException`, duplicate injection, matrix freeze,
read-only export, named evaluator tamper failures, signature separation,
finalization replay/conflict behavior, and legacy-schema migration.

Two pre-existing branch-wide test defects surfaced during the full gate and
were repaired transparently:

1. Commit `8ab8091` extracted producer construction from `main()` into
   `_build_daemon_perception_workers` but left a source-introspection assertion
   aimed at the old function boundary. The test now proves that `main()` calls
   the production helper, the helper applies the exact candidate feature gate
   and builder/runtime parameters, disabled mode does not call the builder,
   and `main()` still starts, cancels, and exposes the runtime.
2. The SIGSTOP reconciliation recovery test shared one two-second deadline
   across startup, stall detection, and recovery. It passed 10/10 in isolation
   with no residual child process but failed under full-suite load after the
   shared budget was exhausted. It now uses independent condition-based
   deadlines for each phase, preserves the per-phase bound, and retains the
   existing `finally` SIGCONT/terminate/reap cleanup.

## Verification

```text
Task 7 focused authority/control/orchestrator/evaluator suites: pass
Task 2 daemon/quote/candidate/fault-runtime/supervisor related suites: pass
Ruff on every changed Python file: pass
git diff --check on Makefile/scripts/src/tests: pass
make test-m1-perception:
  2750 passed, 1 skipped, 1 xfailed, 0 failed in 468.20s
make planning-status:
  82 plans, no drift
```

## Remaining boundary

No production qualification was attempted. Production use still requires an
explicit operator-controlled run of Phase A, independent candidate evaluation,
finalization, re-export, and final evaluation with the three authorities kept
separate.
