# Task 6 Summary — Opportunity-First Operations

Task 6 exposes bounded durable read facts and authenticated operator wake-ups
without adding producer or trading authority.

- Six stable read envelopes distinguish valid zero opportunities from
  unavailable worker/evidence chains.
- SQLite reads are read-only, busy-bounded, execution-bounded, output-bounded,
  and reuse the existing full-history validators.
- Discovery and Reconciliation controls are timestamp/nonce/path/body-bound,
  replay-resistant, atomically coalesced, and consumed only by the existing
  serial producer loops.
- Five Make targets and the living M1 manual provide the supported cloud
  workflow.

Verification: 2627 repository tests passed (one expected xfail, one skip);
Ruff, compileall, docs, planning status, and diff checks passed.

No deployment or trading capability was introduced. Task 7 remains the
Dashboard slice and Task 8 remains production qualification.
