# Scoped Upstream Fault Control Summary

**Status:** in-progress
**Approved design:**
[`2026-07-30-scoped-upstream-fault-control-design.md`](../specs/2026-07-30-scoped-upstream-fault-control-design.md)

Task 1 establishes the dormant-by-default fault model and append-only SQLite
authority. It does not add HTTP, adapters, runtime bridges, deployment,
feature flags, or production mutation.

Implemented:

- Frozen fault/runtime/intent/event/projection contracts and canonical JSON
  hashing in `fault_control.py`.
- Locked call classes:
  `gamma-discovery-event-page`,
  `gamma-reconciliation-event-page`,
  `clob-candidate-book-batch`, and
  `telegram-opportunity-card`.
- Exact kind-to-call-class mapping, target normalization, parameter bounds,
  TTL validation, release/machine/boot identity validation, and digest-only
  authorization identity.
- A single-use in-memory controller with exact-scope matching, monotonic
  expiry, fail-open real calls, memory-first cleanup, and admission freeze
  after cleanup-receipt failure.
- An independent `FaultAuthorityStore` with bounded SQLite busy timeouts,
  append-only runtime registration, transactional authorization/replay
  handling, exact-runtime single-use claim, hash-chained lifecycle facts,
  deterministic fail-closed validation, read-only projection, and stale
  runtime abandonment.
- Append-only schema names:
  `neg_risk_fault_runtime_starts`,
  `neg_risk_fault_auth_nonces`,
  `neg_risk_fault_intents`, and
  `neg_risk_fault_events`.
- Lifecycle safety order is `contained -> cleaned -> recovered`; a remote
  cleanup request is not represented as a process-owned cleanup receipt.

Tests run:

- RED: `uv run pytest tests/perception/test_fault_control.py -q` failed at
  collection because `polyarb.perception.fault_control` did not exist.
- GREEN: `uv run pytest tests/perception/test_fault_control.py -q` passed,
  29 tests.
- RED: `uv run pytest tests/perception/test_fault_authority.py -q` failed at
  collection because `polyarb.perception.fault_authority` did not exist.
- GREEN: `uv run pytest tests/perception/test_fault_authority.py -q` passed,
  15 tests.
- GREEN: `uv run pytest tests/perception/test_fault_control.py
  tests/perception/test_fault_authority.py -q` passed, 44 tests.
- `uv run ruff check` for both modules and both test files passed.

Remaining plan work:

- Task 2: producer boot identity, runtime registration, and safe-boundary claim.
- Task 3: typed Gamma/CLOB/Telegram adapters.
- Task 4: control API and dedicated dual-HMAC authority.
- Task 5: process-owned cleanup and recovery binding.
- Task 6: independent evaluator and qualification evidence.
- Task 7: configuration, Makefile/operator entry points, docs, and full local
  acceptance.
- Task 8: separately authorized deployment and production qualification.
