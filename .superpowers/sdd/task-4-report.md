# Task 4 report — typed Gamma Discovery and Reconciliation adapters

## Status

Implemented locally and verified. No deployment, feature enablement, secret
access, production database mutation, Candidate/CLOB work, Telegram work, or
qualification harness work was performed.

## Delivered

- Added `FaultingGammaPageClient`, wrapping only
  `fetch_active_event_page`; it never inspects `GammaClient._get`, a URL, or
  settings.
- Bound Discovery only to
  `GAMMA_DISCOVERY_EVENT_PAGE/discovery` and Reconciliation only to
  `GAMMA_RECONCILIATION_EVENT_PAGE/reconciliation`.
- Added deterministic timeout, malformed JSON, cursor-integrity, and partial
  coverage faults. Every pass-through path calls the real Gamma client exactly
  once.
- If the real Gamma fetch fails after a transform fault was injected, the
  adapter settles owning cleanup before re-raising the original, untagged
  timeout, malformed-response, or cancellation error. The durable tail is
  `ABANDONED`, never a stranded `INJECTED`.
- Added redacted `PartialGammaPageError` evidence containing only original and
  kept counts, cursor digests, a fault ID, injection time, and a canonical
  coverage ID. The partial page is read from the real client but is rejected
  before normalization or publication; the Discovery cursor is preserved.
- Added a strict `DETECTED` evidence union: exactly one `incident_id` or one
  `coverage-<sha256>` ID. Partial coverage uses the latter and creates no
  Gamma Incident.
- Bound timeout, malformed, and cursor Incident detection to the exact
  `FaultInjectionReceipt.call_id`. A fresh detection event persists that
  `fault_call_id`; same-millisecond organic incidents and pre-existing open
  dedup incidents cannot be linked to the injected fault.
- Wired owning-process cleanup before recovery and retained the opaque
  ownership capability internally only long enough to append the next
  component writer recovery receipt.
- Replaced string recovery IDs with a typed `FaultRecoveryReceipt`. Recovery is
  appended atomically only after the authority verifies the exact fault, kind,
  call class, component, current runtime, injection event, writer timestamp,
  and real same-database writer row.
- Discovery recovery requires the latest progressing Discovery batch.
  Reconciliation recovery requires the exact window checkpoint plus its
  compacted authority checkpoint; it does not depend on batch rows that the
  store legitimately compacts after apply.
- Recovery and `OpportunityPerceptionStore.current_reconciliation` share one
  same-connection checkpoint validator. It verifies canonical anchor and
  checkpoint hashes, the live staging digest, retained-prefix absence, and
  compacted row metadata inside the recovery transaction.
- Cleanup receipt persistence failure freezes evidence, marks the runtime
  degraded, clears injected/recovery metadata, and prevents later claims or
  injection. Cancellation continues to propagate unchanged.

## Plan-versus-real-API correction

The locked Task 4 file list assumed Task 3 exposed process-owned injection and
Incident-link receipt methods. The final Task 3
`FaultRuntimeProtocol` exposed only `consume`, safe-boundary sync, and cleanup;
`FaultAuthorityStore.append_event(INJECTED)` requires the ownership capability
held privately by `FaultRuntime`.

With coordinator approval, Task 4 therefore minimally extended
`fault_runtime.py` and its focused tests with:

- `record_injection(fault_id) -> FaultInjectionReceipt | None`;
- `link_detection(fault_id, kind, detection_id) -> bool`;
- `pending_recovery_fault_id`; and
- `make_recovery_receipt(writer, writer_id, writer_occurred_at_ms)`; and
- `record_recovery(receipt: FaultRecoveryReceipt) -> bool`.

The capability never leaves the concrete runtime. Pass-through runtimes are
no-op, normal calls perform no evidence I/O, and authority write failure marks
the runtime degraded and freezes later injection.

The coordinator also approved the minimal `fault_control.py` evidence change
for canonical partial coverage IDs; no schema, HTTP, CLI, or control-plane
surface changed.

## TDD evidence

1. Adapter RED: the focused command failed collection with
   `ModuleNotFoundError: polyarb.perception.fault_adapters`.
2. Adapter GREEN: typed timeout/malformed, real-page partial rejection,
   cursor mismatch, cross-scope pass-through, runtime/store failure
   pass-through, failed receipt pass-through, and cancellation tests passed.
3. Coverage evidence RED: after temporarily removing the new whitelist shape,
   the exact one-of coverage test failed with `invalid-evidence`.
4. Coverage evidence GREEN: `DETECTED` accepts exactly one valid incident or
   coverage ID and rejects empty, dual, and malformed evidence.
5. Non-applicable partial RED/GREEN: a short complete real page initially left
   the injected controller active; the adapter now performs owning cleanup and
   returns that real page without fabricating partial coverage.
6. Lifecycle GREEN: fake-authority tests prove
   injected → detected → contained → cleaned → recovered and ownership use;
   evidence failure freezes future hot-path injection.
7. Real-store GREEN: Discovery partial and Reconciliation cursor tests run
   real fault authority, real producer runners, real Incident authority, and
   real writer stores in one SQLite database.
8. Review HIGH 1 RED/GREEN: six real-authority cases cover partial/cursor
   transform faults followed by organic timeout, malformed-response, or
   cancellation; all preserve the original error and terminate `ABANDONED`.
9. Review HIGH 2 RED/GREEN: exact call-ID receipt tests cover legitimate
   same-millisecond detection and reject both same-millisecond and older open
   Incident dedup without the injected call ID.
10. Review HIGH 3 RED/GREEN: typed recovery tests reject wrong fault, kind,
    writer type, old/missing writer row, other runtime, and fabricated future
    time. A detected-to-contained write failure freezes evidence, degrades the
    runtime, cleans to `ABANDONED`, and cannot create recovery.
11. Review HIGH 4 RED/GREEN: five real-store corruptions initially all wrote
    false `RECOVERED` events: tampered checkpoint hash, non-canonical anchor,
    re-hashed false staging digest, compacted sample count, and a retained
    prefix row. The shared validator now makes both store reads and recovery
    fail closed without a lifecycle append.
12. Review MEDIUM 1 RED/GREEN: post-injection cleanup authority failure
    initially cleared the controller while leaving the runtime non-degraded
    with stale injection metadata. It now freezes/degrades, removes stale
    recovery capability, passes producer calls through, and performs no later
    claim I/O.

## Verification

- Fault control/runtime/adapter/Incident plus reconciliation suites:
  161 tests passed.
- Store and Discovery checkpoint/compaction suites: passed.
- Repository-wide `pytest -q` completed with one unrelated pre-existing wiring
  failure in `tests/m1-perception/test_l1_quote_worker_wiring.py`: the untouched
  `polyarb.daemon.main` source lacks
  `settings.opportunity_first_watcher_enabled`.
- Focused Ruff across every changed Python source/test: passed.
- `git diff --check`: passed.
- `make planning-status`: 82 plans, no drift.

No aggregate load flake occurred in the Task 4 runs.

## Chain-truth evidence

- Discovery partial:
  authorized → armed → injected → detected(`coverage_id`) → contained →
  cleaned → recovered; zero Incident rows; only the later complete empty batch
  is published.
- Reconciliation cursor:
  authorized → armed → injected → detected(exact `incident_id`) → contained →
  cleaned → recovered; the next checkpoint is newer than injection and the
  existing Gamma Incident verifies.
- Timeout cancellation:
  injection is followed by owning cleanup; no recovery polling occurs.
- Persisted fault evidence excludes response body, event payload, URL, and raw
  cursors.

## Concerns

- Final `verified` fault state remains the responsibility of the independent
  evaluator in a later planned task; this task stops at authentic component
  recovery and preserves the existing Incident verification chain.
- If a `gamma-partial` page is already at or below `keep_events`, the adapter
  records owning cleanup and returns the complete real page. The qualification
  chain is therefore non-successful rather than stuck or falsely partial.
