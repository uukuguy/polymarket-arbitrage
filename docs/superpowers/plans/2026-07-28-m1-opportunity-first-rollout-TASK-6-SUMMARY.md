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
- Raw and derived authority now share one transaction-scoped owner token.
  Canonical triggers cover Candidate current authority/aggregate and Discovery
  status/group projection as well as the seven raw owner tables. The guard
  authenticates Candidate and Discovery aggregate roots and carries an
  atomically advanced retained-prefix base id/hash. Every read, initialization,
  and next writer fully replays the retained 128-event chain, including its
  base and consumed tail; changed hashes, deleted tails, broken links, direct
  derived mutations, and later attempted writes all fail closed.
- Clean state also binds SQLite's AUTOINCREMENT sequence to the consumed guard
  id, so deleting a pending journal event cannot hide it. The v2 guard requires
  `migration_state=complete` and non-NULL Candidate/Discovery roots.
  Initialization fingerprints every owner table with normalized canonical DDL
  plus ordered `table_xinfo`, and every explicit owner index with canonical
  DDL, uniqueness/origin/partial flags, and ordered `index_xinfo`, before DDL:
  only empty, exact current v2, and the explicitly encoded a527 manifest are
  accepted. Trigger discovery uses the complete 14-table raw/derived/internal
  owner attachment scope instead of a name prefix and compares the exact
  canonical `(schema, name, table, SQL)` set across main and temp catalogs on
  initialization, reads, and next writers. Arbitrary-name main/temp triggers
  attached to owner tables fail closed. A per-connection SQLite authorizer
  additionally denies any non-canonical trigger that indirectly writes a raw,
  derived, journal, guard, or writer-context table, including triggers
  attached only to non-owner tables. Direct writes still flow through the
  authenticated journal; unrelated select/log triggers remain allowed.
  Existing or newly created temp shadows of canonical names are rejected.
  A527 retained windows up to 1,025 events migrate under one write lock by full
  replay, atomic base advancement/prune to 128, root derivation, and
  transactional table rebuild into the complete v2 constraints. Unknown or
  partial manifests, semantic table/index drift, corrupt tails, deadline
  interruption, and concurrent migration cannot be washed or partially
  upgraded. The oldest-group query is pinned to the canonical covering order
  without a temporary B-tree.
- Five Make targets and the living M1 manual provide the supported cloud
  workflow.

Verification: 2936 of 2938 repository tests passed (one expected xfail, one
skip); the committed Task 6 baseline collected 2586 and successive authority
remediations collected 2596, 2618, 2642, 2654, 2723, 2765, 2787, 2799, 2842,
and 2938. All 41 focused
API/control tests, 10,010-success Candidate continuity (60.62 seconds with the
128-event proof window), 522 perception tests, raw/derived mutation, deleted
pending sequence, retained hash/tail/link tamper, v2 guard, manifest,
concurrent-writer/migration, deadline, and atomic rollback tests passed;
Ruff, compileall, docs, planning status, and diff checks passed.

No deployment or trading capability was introduced. Task 7 remains the
Dashboard slice and Task 8 remains production qualification.
