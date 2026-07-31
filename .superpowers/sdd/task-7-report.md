# Task 7 Final Fresh Re-review

Baseline: `fe89de0`
Reviewed HEAD: `0dc831d`

## Spec Compliance ❌

The second remediation closes most previously reported runtime and I/O gaps,
but Task 7 is still not production-safe. The deployed-schema migration for the
new cleanup confirmation action is missing, Gamma-partial coverage identity is
not cryptographically derived from its source facts, and the independent
evaluator still signs envelopes with fabricated detection source history.

This review used an isolated archive of `0dc831d`; shared unstaged coordinator
files were excluded. No cloud, deploy, production-fault, wallet, order, balance,
or real-money operation was performed.

Direct clean-HEAD reproductions proved:

- initializing current code over the pre-`0dc831d` fault-event schema leaves
  the old CHECK constraint in place; CLEANED commits, then
  `confirm_cleanup_commit()` fails with `sqlite3.IntegrityError`;
- `record_partial_coverage_rejection()` accepts and persists an arbitrary
  `coverage-<64 hex>` ID unrelated to the supplied counts and cursor digests;
- replacing `detection_receipt.source_history` with
  `[{"attacker":"fabricated"}]` still returns evaluator `PASS`;
- authority normalization still accepts INJECTED with only `call_id` and
  CLEANED with only `cleanup_id`.

## Strengths

- C1's exact fault ID is persisted before arm, ambiguous arm responses resolve
  admission and clean the exact ID, and oversized POST responses now fail on
  the sentinel before parsing. Original and cleanup `BaseException` identities
  are preserved in order.
- The production exporter is authenticated, read-only, single-transaction, and
  deadline-bounded. Incident-backed export now invokes the complete Incident
  writer/suffix authority validator, and missing reconciliation evidence
  produces a failing gate without a fixture exception.
- Gamma partial now has a producer-owned append-only coverage table with
  fault/call/target/runtime/count/cursor fields, a source hash, foreign key,
  uniqueness, and no-update/no-delete triggers.
- The evaluator now rejects extra intent/runtime fields, illegal state/action
  combinations, weak injected/cleaned event payloads, wrong mode, cross-fault
  history, wrong release, incorrect call binding, and top-level detection
  binding mismatches.
- On a newly created database, cleanup clears memory, commits CLEANED, samples a
  post-return confirmation time, then appends one ownership-checked,
  predecessor-bound `cleanup-confirmed` action. Transport reads that action and
  no longer aliases the CLEANED timestamp.
- Artifact reads and all temporary/link/unlink writes are relative to a
  validated owned parent descriptor. Stable file descriptors, byte limits,
  exact candidate bytes, no-replace publication, fsync, and parent-swap cleanup
  are covered.
- Upstream CLI execution rejects dummy authorization arguments; Make uses the
  Ed25519 private/public variables and no longer passes ordinary/fault dummy
  arguments. Exactly eight typed upstream faults advertise
  `execute_supported`; the old legacy flags are false.
- Ed25519 evaluator-private/finalizer-public separation, read-only authoritative
  export, finalizer replay/idempotency/source-change checks, and absolute
  SQLite deadlines remain intact.
- Six focused clean-HEAD suites covering the orchestrator, HTTP controls,
  capability contract, fault authority, runtime, and Gamma adapter passed.

## Issues

### CRITICAL

1. `src/polyarb/storage/schemas.py:1475`

   `cleanup-confirmed` was added only to the `CREATE TABLE IF NOT EXISTS`
   definition. SQLite does not update an existing table's CHECK constraint, and
   there is no table-rebuild migration from the previously deployed constraint
   that permits only `cleanup-requested`.

   I created a database with the pre-remediation fault-event constraint, ran
   current `init_schema()`, and verified the stored table SQL still omitted
   `cleanup-confirmed`. A real authorized/armed/injected/detected/contained/
   CLEANED chain then failed at `confirm_cleanup_commit()` with:

   `CHECK constraint failed: action IS NULL OR action='cleanup-requested'`.

   Production cleanup therefore clears memory and commits CLEANED but cannot
   create the mandatory post-commit proof; recovery evidence freezes after an
   upgrade. Existing migration tests build their “old” schema from the current
   DDL, so they cannot detect this.

   Fix: add an idempotent, transactional table-rebuild migration preserving IDs,
   hashes, indexes, triggers, and foreign keys; test a literal frozen
   pre-`0dc831d` schema through cleanup confirmation and subsequent recovery.

2. `scripts/perception_fault_acceptance.py:400`

   The independent evaluator validates only that `source_history` is a nonempty
   list of mappings. It does not enforce the exact coverage/Incident source
   schemas, recompute coverage `source_hash`, recompute Incident suffix hashes,
   bind every source row to the detection ID/kind/runtime, or require the
   expected Incident lifecycle.

   A candidate envelope whose source history was replaced with a single
   attacker-controlled mapping returned `QualificationVerdict(status='PASS')`.
   Since `build_candidate_artifact()` signs any evaluator PASS from a supplied
   evidence file, authoritative export can be bypassed at the independent
   signing boundary.

   Fix: define exact source-history variants. For coverage, recompute both the
   source hash and coverage ID and bind fault/call/target/runtime/count/cursors.
   For Incidents, validate exact fields, sequence, global predecessor/event
   hashes, incident identity/kind, and terminal recovery/verification semantics.

### HIGH

1. `src/polyarb/perception/fault_authority.py:422`

   Coverage IDs are checked only for the `coverage-` prefix and total length.
   Neither the writer nor exporter verifies:

   `coverage_id == "coverage-" + canonical_digest({counts, cursor digests})`.

   I persisted `coverage-ffff...` with unrelated counts/digests successfully.
   Although `source_hash` protects the stored row from later mutation, it does
   not prove that the detection ID names those source facts. This violates the
   required exact ID/count/cursor binding.

   Fix: derive the ID inside the source owner or recompute and require it in both
   `record_partial_coverage_rejection()` and export; add wrong-ID tests with an
   otherwise valid row/hash.

2. `src/polyarb/perception/fault_control.py:315`

   The authority compatibility backdoor remains. `normalize_evidence()` accepts
   any subset of allowed fields for INJECTED and CLEANED; exact-key enforcement
   still applies only to ARMED, DETECTED cardinality, and VERIFIED. Consequently
   `append_event()` and history validation can accept one-field legacy
   injection/cleanup events even though the independent evaluator later rejects
   them.

   This lets the source projection call an under-specified chain valid and
   contradicts the requirement to delete weak compatibility rather than merely
   reject it downstream.

   Fix: require exact key sets for every lifecycle state, with an explicit
   migration/quarantine policy for pre-existing weak histories; add authority
   append and restart-validation tests.

### MEDIUM

1. `docs/dev/perception-fault-runbook.md:146`

   The developer runbook still instructs upstream execution with the removed
   predictable authorization argument, names the removed
   `ordinary_authorization`/`fault_authorization` inputs, and documents the
   obsolete symmetric `POLYARB_UPSTREAM_FAULT_EVALUATOR_SECRET`. It also says
   the three legacy producer executions remain available, while their
   capability flags and dispatcher are now disabled.

   Fix: update the runbook to the environment-HMAC and Ed25519 contracts and
   clearly mark legacy producer execution unavailable.

## Verification

- PASS:
  `test_upstream_fault_e2e.py`,
  `test_perception_fault_controls.py`,
  `test_perception_chaos_contract.py`,
  `test_fault_authority.py`,
  `test_fault_runtime.py`, and
  `test_gamma_fault_adapter.py`.
- `git diff --check fe89de0..0dc831d`: PASS.
- The four adversarial reproductions listed under Spec Compliance were executed
  directly against isolated `0dc831d`.

## Assessment

**Needs fixes.**

Do not run the production upstream matrix from `0dc831d`. The missing deployed
schema migration alone breaks every upgraded cleanup-confirmation chain; the
coverage-ID and independent-evaluator false-PASS gaps also prevent trustworthy
production qualification.

## Third Remediation Resolution

Resolved on top of `0dc831d` without cloud, deploy, production-fault, wallet,
order, balance, or real-money operations:

- Added an idempotent transactional migration for the frozen pre-`0dc831d`
  `neg_risk_fault_events` action CHECK. It rebuilds the table with
  `foreign_keys=OFF` and `legacy_alter_table=ON`, preserves every event ID,
  sequence, state/action, evidence, predecessor/hash, and row order, then
  restores foreign-key enforcement. Current DDL recreates the append-only
  triggers and partial unique indexes. A literal old-schema fixture proves
  `foreign_key_check` is empty, repeated `init_schema()` is stable, history
  hashes do not change, and a real post-CLEANED `cleanup-confirmed` append
  succeeds. An authorizer-injected DROP failure proves the rebuild rolls back
  atomically.
- `normalize_evidence()` now requires the complete INJECTED and CLEANED schemas
  for every new authority append. Restart validation retains a narrow,
  version-aware read-only compatibility path for already-persisted historical
  one-field rows; it cannot be used by a new writer. Production Incident
  receipt code now uses a separate `normalize_fault_call_id()` based on the
  single canonical `_CALL_ID_RE`, so strict event validation is never reused as
  an ID sanitizer.
- Partial coverage authority derives and requires the exact
  `coverage-<digest(counts,cursors)>` identity. Both authority and independent
  evaluator recompute the semantic ID and source hash.
- Incident export now includes the complete retained global suffix, including
  predecessor rows from other incidents. The independent evaluator enforces
  exact source fields, canonical evidence JSON, monotonic event IDs, global
  predecessor/event hashes, target incident sequence/kind/state transitions,
  and terminal VERIFIED state. Coverage history receives analogous exact
  schema/hash/runtime/fault/call/target checks. Rehashed attacker mappings now
  fail.
- Cleanup request and confirmation actions receive exact nested schemas,
  digest/ID/time types, and CLEANED predecessor binding at the independent
  evaluator. Ambiguous-arm cleanup failures retain the original
  `BaseException` object rather than wrapping it. The dead public
  `legacy_execute_supported` field was removed.
- Make help, the developer runbook, and the M1 manual no longer advertise
  dummy CLI authorization, disabled producer execution, or the removed
  symmetric evaluator secret. They document the three Ed25519 phases exactly:
  candidate evaluator private key; finalizer public key plus control HMACs;
  read-only final evaluator public key only.

During full verification, systematic debugging exposed one unrelated but real
read-snapshot race: `open_incidents()` could combine a pre-commit owner guard
with a post-commit journal row because its multi-query validation lacked an
explicit SQLite read transaction. A RED/GREEN contract test now requires one
snapshot on manager-owned read-only connections. The SIGSTOP integration test
also uses a test-only five-second overall recovery window while preserving its
0.12-second stall detection threshold; production defaults are unchanged.

## Final Verification

- Focused upstream qualification/authority/runtime/control set: **316 passed**.
- Candidate, Gamma, notification, Telegram, and call-ID regression set after
  strict writer rollout: **156 passed**.
- Resource/supervisor concurrency scenarios: **10/10 paired repetitions
  passed** after the snapshot/test-SLO correction, with no child-process
  residue.
- Full suite: **3654 collected, 100% PASS**, with one expected xfail and one
  expected skip.
- Changed-file Ruff: PASS.
- `make docs-m1-check`: PASS.
- `make planning-status`: PASS, no drift.
- `git diff --check` on owned files: PASS.

Assessment after remediation: **Ready for fresh independent re-review.**
Production execution remains explicitly out of scope and was not run.

## Fourth Remediation Resolution

The fresh review of `0dc831d..4b414f6` returned two remaining Important
findings. Both are resolved after explicit RED reproductions:

1. Incident detection receipts now export an exact validated checkpoint
   payload/hash (or canonical zero genesis). The evaluator recomputes that hash,
   requires the first suffix predecessor to match its prefix, requires retained
   IDs after `through_event_id`, derives the exact component/target scope, and
   binds the target DETECTED evidence `fault_call_id` to the fault ledger's
   INJECTED call. Fully rehashed wrong-scope, wrong-call, and wrong-anchor
   suffixes fail with `detection-source-history-invalid`.
2. The old fault-events migration now runs
   `foreign_key_check(neg_risk_fault_events)` after copy but before dropping the
   old table or committing. An orphan-row fixture proves failure restores the
   byte-for-byte old table SQL, rows, indexes, triggers, and references; the
   preexisting authorizer failure and clean/idempotent migration cases remain
   green.

Post-review verification:

- Extended qualification focused set: **389 passed**.
- Full suite: **3658 collected, 100% PASS**, with one expected xfail and one
  expected skip.
- Changed-file Ruff, M1 docs gate, planning-status, and owned staged diff
  checks: PASS.
- No cloud, deploy, production-fault, wallet, order, balance, or real-money
  operation was performed.

Assessment: **Ready for one final independent re-review.**

## Fifth Remediation Resolution

The final integrity review found that an unkeyed, caller-rehashed non-genesis
checkpoint could still qualify, plus four strict-source and migration gaps.
The remediation introduces a dedicated SOURCE Ed25519 authority that is
cryptographically and operationally separate from the verdict keypair:

- The source/export service alone holds the SOURCE private key and signs the
  complete canonical envelope. Candidate and final evaluators verify it with
  SOURCE public only. The orchestrator preserves the exact authenticated HTTP
  response bytes, and the candidate artifact binds their exact SHA-256.
- The signed envelope carries both its complete digest and an immutable
  source-facts digest. The latter excludes current-time freshness projections.
  Finalization rebuilds those facts in the same `BEGIN IMMEDIATE`
  transaction, rejects authority changes as `verdict-source-mismatch`, and
  enforces the persisted recovery-writer deadline separately as
  `verdict-source-stale`.
- Incident history must start at `checkpoint.through_event_id + 1` and remain
  globally gapless. Exact scope, detected call ID, checkpoint, suffix, and
  source signature are all required.
- Gamma coverage now enforces exact fields/types/count bounds/cursor digests,
  semantic coverage ID/source hash, exact runtime/intent, and the INJECTED call
  ID. Recovery receipts use an exact eight-field schema and bind the encoded
  recovery ID, row ID, component family, fault, target, runtime, and time.
- The historical auth-nonce CHECK migration, like the event migration, now
  performs database-wide foreign-key validation before destructive drop/commit.
  Orphan self-FK failure restores the exact old schema, rows, indexes, triggers,
  pragma state, and leaves no transaction open.

Explicit RED-to-GREEN reproductions cover the reviewer's fully rehashed
non-genesis checkpoint, wrong SOURCE key, source-signed malformed coverage and
recovery receipts, shifted Incident IDs, source change before finalization,
time-only expiry, exact response-byte preservation, and orphan auth/event
migrations. No cloud or production operation was performed.

Final local qualification:

- Upstream E2E: 62 PASS.
- Fault controls: 36 PASS.
- Fault authority: 76 PASS.
- Seven-file focused/broad gate: 391 PASS.
- Full suite: 3671 collected, 100% PASS with one expected xfail and one skip.
- Changed-file Ruff, M1 docs gate, and planning-status: PASS.

Assessment: **Ready for fresh independent re-review.**

## Sixth Remediation Resolution

Fresh independent review found that the auth-nonce migration's table-scoped FK
check missed inbound references from fault intents. A parallel integrity review
also found generic terminal event writes, a current-time value in immutable
facts, and process-role/taxonomy enforcement gaps. The repair closes each
boundary:

- Auth-nonce migration now runs database-wide `PRAGMA foreign_key_check` before
  drop/commit. An old-schema intent whose reservation and attempt rows are
  missing reproduces RED and now restores the exact database schema and rows.
- Generic `append_event` rejects `CLEANED`, `RECOVERED`, and `VERIFIED`.
  Cleanup, recovery, and verdict state can be written only through
  `relinquish_claim`, `append_recovery_event`, and `finalize_verdict`
  respectively. `INJECTED` remains the runtime injection writer and still
  requires the exact ownership capability and call binding.
- Immutable source facts exclude both `freshness_gate` and the `now_ms`-derived
  `orphan_collecting_runs`; the complete SOURCE signature still covers the
  entire envelope. Current gates and deadline expiry remain independently
  evaluated.
- Finalizer source-rebuild failures normalize to `verdict-source-mismatch`;
  immutable source changes are compared before staleness, so tampering cannot
  hide behind an expired deadline.
- Finalizer settings reject a co-resident SOURCE private key. Make evaluator
  targets reject forbidden private-key environment variables, and candidate
  and final evaluator operations reject SOURCE/VERDICT keypair reuse.

Final local qualification: 77 authority, 36 controls, and 67 upstream E2E
tests PASS; the seven-file broad gate is 397 PASS; the full suite is 3677
collected and 100% PASS with one expected xfail and one skip; Ruff, M1 docs,
and planning gates PASS.

Assessment: **Ready for dual fresh independent re-review.**

## Seventh Remediation Resolution

Fresh review showed that Make enforced evaluator isolation but direct CLI
invocation did not. Root integrity review also identified a second current-time
gate: a collecting quote lease could become orphaned after candidate evidence
without changing immutable facts.

- Direct candidate evaluation now rejects SOURCE private and both control HMAC
  environments. Direct final evaluation rejects SOURCE private, VERDICT
  private, and both control HMAC environments. The Make targets enforce the
  same rows.
- Source/export Settings rejects a co-resident VERDICT private key. Finalizer
  Settings rejects both SOURCE and VERDICT private keys. This completes the
  four-role capability matrix documented in the runbook.
- Finalization explicitly rechecks `orphan_collecting_runs == 0` inside the
  same source transaction. With an unchanged database, crossing only a
  collecting-run lease returns `verdict-source-stale`; after normal expired
  lease cleanup, re-export/finalization remains possible.

Final local qualification after these changes: focused 186 PASS (77 authority,
37 controls, 72 upstream E2E); seven-file broad 403 PASS; full suite 3683
collected and 100% PASS with one expected xfail and one skip; Ruff, M1 docs,
and planning gates PASS.

Assessment: **Ready for dual fresh independent re-review.**
