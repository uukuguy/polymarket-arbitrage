# Task 6 Summary — Opportunity-First Operations

Task 6 exposes bounded durable read facts and authenticated operator wake-ups
without adding producer or trading authority.

- Six stable read envelopes distinguish valid zero opportunities from
  unavailable worker/evidence chains.
- SQLite reads are read-only, busy-bounded, execution-bounded, output-bounded,
  use one snapshot per response, and reuse the existing full-history
  validators. One absolute deadline interrupts SQLite and Python replay work
  and waits for worker/connection convergence. Candidate status checks the full
  Group/Quote/fact/receipt state and as-of authority before counting an
  opportunity.
- Discovery and Reconciliation controls are timestamp/nonce/path/body-bound,
  replay-resistant, append-only hash chained, atomically coalesced, and
  consumed only after terminal producer evidence by the existing serial loops.
  Active incidents, paused/unavailable producers, shed/expired resource
  authority, and corrupt history all fail closed without consuming the queue.
  A shared auth+queue deadline prevents late commits; bounded active nonces are
  safely pruned after their replay window because queue receipts retain their
  accepted auth proof. Legacy Task 6 schemas migrate atomically and
  idempotently only after their old chain validates.
- Five Make targets and the living M1 manual provide the supported cloud
  workflow.

Verification: 2616 of 2618 repository tests passed (one expected xfail, one
skip); the committed Task 6 baseline collected 2586, the first remediation
collected 2596, and the formal remediation collected 2618. All 41 focused
API/control tests and the 343-test proportional regression passed;
Ruff, compileall, docs, planning status, and diff checks passed.

No deployment or trading capability was introduced. Task 7 remains the
Dashboard slice and Task 8 remains production qualification.
