# Task 6 Summary — Opportunity-First Operations

Task 6 exposes bounded durable read facts and authenticated operator wake-ups
without adding producer or trading authority.

- Six stable read envelopes distinguish valid zero opportunities from
  unavailable worker/evidence chains.
- SQLite reads are read-only, busy-bounded, execution-bounded, output-bounded,
  use one snapshot per response, and reuse the existing full-history
  validators. One absolute deadline interrupts SQLite, Python replay, and
  slow-drip request-body work and waits for worker/connection convergence.
  Candidate status checks exact Group/Quote/fact/receipt state and as-of
  authority before counting an opportunity. A versioned rolling checkpoint
  binds each fully validated compacted prefix and retained per-group seed,
  keeping the replay suffix bounded during sustained 15-second sampling.
  Revoked historical groups leave the live seed while current watching groups
  retain their exact Quote legs.
- Discovery and Reconciliation controls are timestamp/nonce/path/body-bound,
  replay-resistant, append-only hash chained, atomically coalesced, and
  consumed only after terminal producer evidence by the existing serial loops.
  Active incidents, paused/unavailable producers, shed/expired resource
  authority, and corrupt history all fail closed without consuming the queue.
  A shared auth+queue deadline prevents late commits; bounded active nonces are
  safely pruned after their replay window because queue receipts retain their
  accepted auth proof. Fully validated queue prefixes roll atomically into
  per-component checkpoints instead of eventually reaching a permanent hard
  cap. Legacy Task 6 schemas migrate atomically and idempotently only after
  their old chain validates.
- Discovery and Reconciliation authority reads are also bounded for continuous
  operation. Discovery compacts only a fully validated completed prefix and
  retains a checkpointed completed-batch/sample/evidence/coverage anchor plus
  a bounded suffix. Reconciliation checkpoints each page with its exact window
  seed, staging digest, cursor set and cumulative metrics, then atomically
  prunes the replaced page receipts/samples. Tampering, retained-prefix
  injection, or prune failure fails closed; validated legacy histories migrate
  idempotently without changing reconciliation apply semantics. Discovery
  status reads a hash-bound current projection plus trigger-maintained
  attempt/breach counters and a bounded receipt suffix, so it no longer scans
  Group revision, admission, attempt, or Candidate fact lifecycles. Every legal
  owner writer refreshes the projection in the same transaction.
- Five Make targets and the living M1 manual provide the supported cloud
  workflow.

Verification: 2652 of 2654 repository tests passed (one expected xfail, one
skip); the committed Task 6 baseline collected 2586, the first remediation
collected 2596, the formal remediation collected 2618, and the authority
checkpoint remediation collected 2642. All 41 focused API/control tests,
10,010-success Candidate continuity, 62 Discovery status/checkpoint
regressions, 238 perception tests, tamper and atomic rollback tests passed;
Ruff, compileall, docs, planning status, and diff checks passed.

No deployment or trading capability was introduced. Task 7 remains the
Dashboard slice and Task 8 remains production qualification.
