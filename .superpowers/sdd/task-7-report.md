# Task 7 Remediation Implementer Report

Status: REMEDIATION GREEN — local qualification only

The fresh-review findings are preserved unchanged in
`.superpowers/sdd/task-7-review-findings.md`. No cloud, production, deploy,
wallet, order, balance, signing, or real-money operation was performed.

## Review resolution

### C1 — ambiguous arm cleanup

- The orchestrator now generates the exact fault ID and durably publishes
  `intent.json` before any network request.
- Arm is inside the `BaseException` cleanup envelope.
- Lost, malformed, oversized, and non-mapping arm responses resolve the exact
  ID through status and issue idempotent cleanup. A failed status read still
  attempts cleanup.
- The original exception is re-raised unchanged when cleanup succeeds.
  Original plus cleanup failures are retained in order in a
  `BaseExceptionGroup`.

### C2 — authoritative source export

- Production export is now an ordinary-plus-fault-HMAC authenticated,
  read-only endpoint:
  `/perception/faults/{fault_id}/export`.
- One read-only SQLite transaction, under one 750 ms deadline, validates the
  fault history/projection and derives Incident/coverage binding, the exact
  component recovery-writer row and timestamp, open incidents, incomplete
  publication, current quote freshness, reconciliation state,
  cross-membership mismatches, and orphan collecting runs.
- `export_fault_envelope()` no longer accepts caller-supplied integrity facts.
- The production transport consumes only this endpoint; cached baseline values
  and hard-coded qualification booleans/counts were removed.
- Missing/tampered source rows fail export. A real Incident lifecycle and real
  candidate-success writer integration proves the clean and dirty paths.

### C3 — evaluator independence

- `cryptography>=48.0.0` is a direct runtime dependency.
- Candidate verdicts use strict Ed25519 keys encoded as
  `ed25519-v1:<kid>:<base64url-raw32>`.
- Only the evaluator reads
  `POLYARB_UPSTREAM_FAULT_EVALUATOR_PRIVATE_KEY`.
- The finalizer setting contains only the pinned public key, validates its
  version/format at startup, and verifies artifact version, kid, digest, and
  signature.
- The final evaluator deletes the private-key environment after candidate
  creation and verifies with only the public key.

### H1-H4

- The evaluator enforces exact envelope/event fields, evidence mode, typed
  runtime identity, expected release, event-to-intent fault ID, canonical state
  tail, call binding, detection source binding, and candidate/final transition
  rules. The reviewer wrong-mode and fully rehashed cross-fault reproductions
  are named FAIL cases.
- Dummy ordinary/fault authorization CLI arguments were removed. Production
  control secrets are validated before GET or filesystem mutation.
- Server, orchestrator, and finalizer CLI share the versioned
  `polyarb-fault-v2` HMAC message containing `sha256(body)`.
- Cleanup double failures preserve the original first and still freeze the
  matrix.
- Exactly the eight typed upstream faults advertise `execute_supported`;
  legacy producer primitives use `legacy_execute_supported`.

### M1-M2

- Artifact reads use one `O_NOFOLLOW` descriptor, regular-file and 1 MiB
  checks, and stable `(dev, ino, size)` verification.
- Artifact writes use a same-directory `0600` temporary file, file fsync,
  hard-link no-replace publication, directory fsync, temporary unlink, and a
  second directory fsync. A failed publication never exposes a partial final
  artifact.
- Finalizer submission transfers the exact candidate bytes as base64 plus
  SHA-256; the server validates those bytes before parsing.
- Cleanup clears process memory before the callback, records an actual
  `memory_cleared_at_ms`, then persists `receipt_persisted_at_ms`. The transport
  reads these durable fields instead of aliasing the event timestamp.

## Verification

- Focused Task 7 / authority / runtime / chaos suites: PASS.
- Ruff on every changed source and test file: PASS.
- Full repository: `3645 tests collected`; PASS with the repository's existing
  expected xfail/skip and warnings.
- `make planning-status`: 82 plans, no drift.

## Operational boundary

The upstream matrix remains disabled by default and was not run against any
deployed environment. The existing Makefile entry points remain:

```text
make evaluate-upstream-fault-candidate
make finalize-upstream-fault
make evaluate-upstream-fault-final
```
