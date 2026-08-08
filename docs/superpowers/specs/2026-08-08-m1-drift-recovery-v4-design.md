# M1 drift recovery and nullable-event v4 design

## Goal

Restore bounded classifier progress promptly after a child timeout without
rewriting failed-attempt evidence, and classify one production-proven nullable
ordinary-event shape under a new immutable classifier contract.

## Evidence

Release 240 ran the approved SHA with legacy reads and Quote disabled. A v3
comparison checkpointed real rows, then two 75-second child timeouts left
durable progress but made the scheduler sleep its ordinary 300-second cadence.
The v3 receipt then terminalized with exactly 11 `evidence-missing` diagnostics.
All 11 have: event `negRisk` absent/null, `enableNegRisk=false`, no event or
member group id, member `negRiskOther=false`, no market-staging row, and a
closed member. The v3 receipt is immutable and must remain so.

## Design

### A. Evidence-based timeout continuation

At drift-child admission, retain the active comparison ID and its durable
checkpoint time. When `run_structure_drift_in_subprocess` raises
`structure-drift-timeout`, first write the existing failed terminal attempt.
Then re-read drift status. Set `_checkpoint_pending=True` only when all of the
following are true: the same comparison remains active, it is in a known
non-terminal phase, and its checkpoint time is newer than the admission
checkpoint. The next resident-loop pass remains subject to all existing Quote
priority checks. Any other timeout stays on the normal retry cadence.

### B. Classifier-v4 nullable ordinary-event exclusion

Introduce `structure-drift-classifier-v4` rather than altering a v3 receipt.
For an event-only candidate, classify it as
`non-neg-risk-event-member` only when the exact raw event and member evidence
matches the 11-row shape above. A null/missing `negRisk` alone is never enough:
`enableNegRisk=false`, absent group identities, and `negRiskOther=false` are
all required. Every other malformed or ambiguous shape remains diagnostic.

## Non-goals

- Do not change generation read mode, Quote enablement, pointers, cleanup, or
  historical receipts.
- Do not globally shorten scheduler cadence or enlarge the 75-second child
  hard limit.
- Do not solve the independent cold-start schema/ANALYZE latency here.

## Acceptance

- A timeout with proven durable advance schedules 100ms continuation while its
  append-only attempt remains `failed/structure-drift-timeout`.
- No advance, identity change, terminal status, or unreadable status does not
  schedule immediate continuation.
- v4 excludes exactly the certified nullable ordinary-event fixture; ambiguous
  nullable evidence remains a diagnostic.
- v3 receipt bytes remain immutable and v4 produces a separate contract-bound
  comparison/receipt.
