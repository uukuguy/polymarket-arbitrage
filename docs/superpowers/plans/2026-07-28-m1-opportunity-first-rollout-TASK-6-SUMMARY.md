# Task 6 Summary — Opportunity-First Operations

Task 6 exposes bounded durable read facts and authenticated operator wake-ups
without adding producer or trading authority.

- Six stable read envelopes distinguish valid zero opportunities from
  unavailable worker/evidence chains.
- SQLite reads are read-only, busy-bounded, execution-bounded, output-bounded,
  use one snapshot per response, and reuse the existing full-history
  validators. Candidate status checks persisted row identity, token membership,
  fact/quote authority, and receipt hashes before counting an opportunity.
- Discovery and Reconciliation controls are timestamp/nonce/path/body-bound,
  replay-resistant, append-only hash chained, atomically coalesced, and
  consumed only after terminal producer evidence by the existing serial loops.
  Active incidents, paused/unavailable producers, shed/expired resource
  authority, and corrupt history all fail closed without consuming the queue.
- Five Make targets and the living M1 manual provide the supported cloud
  workflow.

Verification: 2594 of 2596 repository tests passed (one expected xfail, one
skip); the committed Task 6 baseline collected 2586 and review remediation
added ten adversarial tests without deleting or renaming any test. Focused
API/control tests and the 321-test proportional regression passed;
Ruff, compileall, docs, planning status, and diff checks passed.

No deployment or trading capability was introduced. Task 7 remains the
Dashboard slice and Task 8 remains production qualification.
