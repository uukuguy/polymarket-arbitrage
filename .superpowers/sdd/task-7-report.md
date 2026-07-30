# Task 7 Fresh Re-review

Baseline: `fe89de0`
Reviewed HEAD: `799667c`

## Spec Compliance ❌

The remediation is not production-safe. C3 (asymmetric evaluator authority) is
resolved, and the exact-ID ambiguous-arm cleanup is materially improved, but C2,
H1, H2, H3, M1, and M2 still have release-blocking gaps. The committed Makefile
also cannot execute the new upstream path.

This review used an isolated archive of `799667c`; concurrent dirty worktree
changes were excluded. No cloud, deploy, wallet, order, balance, signing, or
real-money operation was performed.

Direct local reproductions proved:

- changing a non-first Incident event after candidate export still allows the
  final authoritative export and final evaluator to return `PASS`;
- an empty reconciliation table is exported as `reconciliation_gate=true`;
- fully rebound envelopes with extra intent/runtime fields or an arbitrary
  event action return `PASS`;
- a candidate with CLEANED evidence containing only `cleanup_id` returns
  `PASS`, and authority normalization also accepts legacy one-field INJECTED and
  CLEANED evidence;
- all documented upstream Make invocations contain CLI arguments that argparse
  rejects, while all three advertised legacy execute primitives return
  `adapter-not-implemented`.

## Strengths

- The caller creates and persists the exact fault ID before arm. Ambiguous arm
  failures query admission and attempt idempotent cleanup of that exact ID.
- The exporter opens SQLite read-only, validates the fault ledger, and derives
  most facts in one bounded transaction rather than accepting caller integrity
  booleans.
- Candidate signing now uses Ed25519. The evaluator alone reads the private key;
  the finalizer and final evaluator use the pinned public key.
- Fault-domain HMAC uses the shared versioned body-digest contract, and secret
  preflight occurs before network calls or evidence-directory creation.
- Wrong envelope mode, cross-fault event identity, release mismatch, replayed
  finalization, and post-candidate source changes have focused fail-closed tests.
- Exactly eight typed upstream faults set `execute_supported`; schema-migration,
  authenticated read-only GET, absolute SQLite deadline, response-lost
  finalization, and finalizer idempotency tests pass.
- The focused clean-HEAD suites for the orchestrator, control HTTP surface, and
  fault authority passed.

## Issues

### CRITICAL

1. `scripts/perception_fault_readonly.py:158`

   The “authoritative” exporter still fails open over source evidence. Gamma
   partial initializes the qualifying `source_kind` and copies `coverage_id`
   from the fault event without querying any durable coverage source. For
   Incident-backed faults, lines 161-168 read only the first event's ID/kind and
   never validate the Incident history, suffix authority, open-authority hashes,
   aggregate, replay anchors, or terminal recovery evidence. Lines 305-308 also
   treat a missing reconciliation row as a passing gate.

   I created a real Incident/fault/writer chain, exported the candidate,
   corrupted Incident sequence 2, and then finalized. The second export
   succeeded and the final evaluator returned `PASS`. The same database had
   zero reconciliation rows yet exported `reconciliation_gate=true`.

   Fix: validate the complete Incident authority in the same read transaction;
   bind Gamma-partial to a durable coverage record; make missing required
   freshness/reconciliation/integrity sources named failures rather than
   passing defaults.

### HIGH

1. `scripts/perception_fault_acceptance.py:109`

   The evaluator does not enforce an exact nested schema. It hashes but accepts
   extra intent/runtime fields, does not validate state/action combinations,
   and does not validate CLEANED evidence. Separately,
   `src/polyarb/perception/fault_control.py:314` permits any subset of allowed
   evidence fields except for ARMED, DETECTED, and VERIFIED.

   Direct reproductions returned `PASS` for a fully rebound extra intent field,
   a fully rebound extra runtime field, an arbitrary `attacker-action`, and
   CLEANED evidence containing only `cleanup_id`. `normalize_evidence()` also
   accepted INJECTED with only `call_id`. This preserves the compatibility
   backdoor M2 required deleting.

   Fix: enforce exact typed fields for intent, runtime, every state/action
   payload, receipts, and candidate artifact; require both new cleanup timings
   and the call-binding digest; add all four false-PASS cases as tests.

2. `Makefile:942`

   The operational entrypoints are inconsistent with the committed CLI.
   Makefile lines 946-947 always pass `--ordinary-authorization` and
   `--fault-authorization`, which the parser does not define, so every upstream
   `mode=execute` command exits in argparse. Lines 851 and 857 still require the
   deleted `POLYARB_UPSTREAM_FAULT_EVALUATOR_SECRET`, while the evaluator now
   requires private/public Ed25519 variables. Finally,
   `scripts/perception_chaos.py:1190` ignores `legacy_execute_supported`, so
   `candidate-exit`, `discovery-exit`, and `reconciliation-stall` are documented
   as executable but always reject.

   Fix: split upstream and legacy CLI contracts, update Makefile and the manual
   to the actual environment authorities, dispatch the legacy flag deliberately,
   and add subprocess/Make contract tests rather than plan-only flag tests.

3. `scripts/perception_chaos.py:290`

   POST control responses are read with a `+1` sentinel but the sentinel length
   is never rejected. A mapping JSON at the limit plus trailing whitespace is
   accepted, so the required oversized-response failure and cleanup path is not
   implemented even though GET responses perform this check.

   Fix: retain the raw bytes, reject `len(raw) > _MAX_HTTP_BYTES`, then parse;
   test an oversized-but-parseable arm response and exact-ID cleanup.

4. `scripts/perception_chaos.py:748`

   Cleanup failures are converted to `AdapterFailedError`. With an original
   failure, the group contains the wrapper rather than the cleanup
   `KeyboardInterrupt`, `SystemExit`, `CancelledError`, or other
   `BaseException`; with cleanup alone, the exact exception is not re-raised.
   H3 therefore still destroys cleanup exception identity and traceback.

   Fix: preserve both actual exceptions in order and derive matrix-freeze state
   without replacing either exception.

5. `src/polyarb/perception/fault_runtime.py:252`

   `receipt_persisted_at_ms` is sampled before `relinquish_claim()` starts its
   SQLite transaction and commits. It therefore is not an observed persistence
   completion time. Combined with the optional CLEANED schema above, M2's
   clear-before-durable-receipt proof remains incomplete.

   Fix: record persistence completion after a successful commit through an
   authority-owned receipt and verify strict ordering under a delayed-commit
   test.

### MEDIUM

1. `src/polyarb/safe_artifact.py:46`

   The parent directory descriptor is validated, but temp creation, hard-link
   publication, and unlink use absolute/pathname resolution at lines 51-74. A
   parent rename/replacement after validation can redirect publication into an
   unvalidated directory while `fsync` targets the old directory descriptor.
   Reads also validate only the final file, not parent ownership/stability.

   Fix: perform all operations relative to the validated `parent_fd` with
   `dir_fd` APIs and basenames, validate the read parent, and add adversarial
   parent-swap tests.

2. `scripts/perception_chaos.py:1153`

   Upstream execution still requires a predictable dummy
   `--authorization=fault:<fault>:<release>` even though real authority comes
   from environment-held HMAC keys. This contradicts H2 and the manual's claim
   that dummy authorization arguments are gone.

   Fix: make the legacy acknowledgement a legacy-only option and remove it
   entirely from the upstream path.

## Verification

- Clean-HEAD focused suites passed:
  `test_upstream_fault_e2e.py`,
  `test_perception_fault_controls.py`, and
  `test_fault_authority.py`.
- The isolated full suite completed with two non-Task-7 failures: the archive
  intentionally has no `.git` directory for the climb shell test, and one
  resource-Incident timing test failed once but passed on two immediate isolated
  reruns.
- The false-PASS and CLI reproductions listed under Spec Compliance were run
  directly against the isolated `799667c` source.

## Assessment

**Needs fixes.**

Do not run the production upstream fault matrix from `799667c`. The false-PASS
source exporter, noncanonical evaluator acceptance, broken Make/CLI surface, and
incomplete cleanup semantics remain release blockers.

---

## Second remediation against this re-review

Status: SECOND REMEDIATION GREEN — local qualification only.
No cloud, deploy, production fault, wallet, order, balance, or real-money
operation was performed.

- **C1:** POST responses now reject the 1 MiB + 1 sentinel before JSON parsing.
  Exact-ID ambiguous arm cleanup remains unchanged and adversarially covered.
- **C2 / authoritative sources:** Gamma partial-page detection now writes a
  source-owned append-only coverage rejection bound to exact fault, call,
  target, runtime, counts, cursor digests, and source hash before lifecycle
  detection. Export requires that row and validates its hash/runtime. Incident
  export validates the complete Incident writer authority inside the same
  transaction and emits the target history with sequence, evidence, state, and
  global suffix predecessor/event hashes. Reconciliation uses the store's full
  same-connection authority validator; missing evidence is fail-closed.
- **H1:** production evaluation enforces exact intent/runtime/event/action and
  per-state evidence fields. Fully rehashed extra-field, arbitrary-action,
  one-field INJECTED, and one-field CLEANED reproductions are named FAIL.
- **H2 / Make:** typed upstream execution rejects the predictable
  `--authorization` acknowledgement and reads both HMAC authorities only from
  the environment. Make no longer passes dummy ordinary/fault arguments and
  now gates candidate/final evaluation on the Ed25519 private/public variables.
  The three dead legacy producer flags are no longer advertised.
- **H3:** cleanup-only `BaseException` is re-raised with its identity intact;
  grouped failures preserve the original first and the actual cleanup
  exception second.
- **M1:** reads and every write/temp/link/unlink operation are relative to a
  validated owned parent descriptor. Parent replacement is checked before and
  after publication; an adversarial swap removes the anchored publication and
  leaves neither substituted nor moved final output.
- **M2:** after memory clear and the first CLEANED transaction returns, runtime
  samples a distinct confirmation time and appends one ownership-validated,
  hash-chained `cleanup-confirmed` action in a second transaction. The action
  binds exact CLEANED event hash, cleanup ID, memory-clear time, and commit
  confirmation time. History, transport, and evaluator require this proof.

Verification completed before the final full rerun:

- five complete fault suites: PASS;
- candidate/Gamma/Telegram adapter suites after action-history adaptation: PASS;
- changed-file Ruff: PASS;
- `make docs-m1-check`: PASS;
- `make planning-status`: 82 plans, no drift.

The first second-remediation full run reached 100% and exposed only nine stale
test assumptions that every history item has a lifecycle state; the new typed
action intentionally has `state=NULL`. Those tests were updated to inspect
lifecycle states while retaining the action in the exact history, and all three
impacted adapter suites then passed.

The final repository-wide rerun reached 100% with exit code 0, one expected
xfail, one skip, and only the repository's existing warnings.
