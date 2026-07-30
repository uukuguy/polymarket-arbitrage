# Scoped Upstream Fault Control Summary

**Status:** local implementation complete; production qualification NOT RUN

**Approved design:**
[`2026-07-30-scoped-upstream-fault-control-design.md`](../specs/2026-07-30-scoped-upstream-fault-control-design.md)

## Outcome

The branch now provides a dormant-by-default, production-designed control and
qualification boundary for eight typed upstream faults:

- Gamma Discovery: timeout, partial-page rejection, malformed response;
- Gamma Reconciliation: cursor-integrity failure;
- Candidate CLOB: exact-group missing leg, 429, and latency; and
- Telegram: exact durable-outbox delivery failure.

Normal mode and any control-store/config/evidence failure pass through to the
real upstream call. Qualification is independently fail-closed. No wallet,
signing, balance, order, fill, position, or trade path is in scope.

## Implementation commits

- Preconditions and complete local gate:
  `b6b794d`, `d766308`.
- Authority model, append-only schema, ownership capability, and fail-open
  controller: `630fdbb`, `8b37e59`, `ad1cf72`.
- Exact producer boot identity, safe-boundary claim, cancellation-safe
  relinquish, and freeze behavior: `7538717`, `96700d6`, `8ab8091`.
- Dual-HMAC API/CLI, bounded read models, finalizer route, and Make entries:
  `7ad9dea`, `8ffee59`, `84237e7`, `7d57172`, `79060eb`.
- Gamma adapters and writer truth:
  `620c920`, `788602c`, `c58264a`.
- Candidate exact-group adapters and committed invocation evidence:
  `728e08a`, `a2c37d9`, `78e028f`, `f303b29`.
- Telegram exact-outbox adapter and cross-family invalidity handling:
  `9a8c4b8`, `3020b06`, `fe89de0`.
- Orchestrator, cleanup/recovery chain, evaluator/finalizer, complete source
  attestation, and capability hardening:
  `c87250d`, `799667c`, `0dc831d`, `4b414f6`, `b6a7dfa`, `b142d60`,
  `3a35a68`, `91f758a`.

## Locked contracts

### Schema and runtime

The four append-only authority tables are
`neg_risk_fault_runtime_starts`, `neg_risk_fault_auth_nonces`,
`neg_risk_fault_intents`, and `neg_risk_fault_events`. Event and authorization
migrations validate foreign keys before commit and roll back as one unit.
Accepted or rejected intent, exact runtime, single claim/injection, lifecycle
hash chain, cleanup terminal, and replay constraints are durable. Rejected
envelopes bind their exact reason plus request/auth attempt correlation and are
never active or claimable; same-fault replay appends an auth attempt without
mutating or duplicating the original envelope. The accepted-only legacy schema
migrates atomically with pre/post-drop full foreign-key checks, rollback, index
recreation, and append-only trigger recreation. The hot path reads only one
immutable producer-local `ActiveFault`.

Isolated and in-process producers use the same protocol. Every child attempt
has a new `producer_boot_id`; Candidate, Discovery, Reconciliation, and the
daemon-owned notification runtime claim only at a safe cycle/batch boundary.
A restarted process cannot inherit a prior boot's fault.

### Call and target boundary

The only call classes are Gamma Discovery page, Gamma Reconciliation page,
Candidate CLOB book batch, and Telegram opportunity card. Their target keys
are respectively `discovery`, `reconciliation`, durable `group_id`, and
decimal durable notification/outbox ID. URLs, headers, tokens, response
bodies, expressions, and shell fragments are never accepted as targets.
Intent TTL is 1,000–120,000 ms and bounded fault parameters are kind-specific.

### Cleanup and chain truth

Cleanup is memory-first. The remote API appends `cleanup-requested`, and the
producer consumes it at the next safe boundary. A never-claimed intent is
terminalized as `ABANDONED` in the claim transaction and can never inject. An
owned claim clears memory before `relinquish_claim()` appends its
lifecycle-valid terminal (`ABANDONED`, or `CLEANED` after containment);
`confirm_cleanup_commit()` supplies the post-commit confirmation for
`CLEANED`. Receipt failure degrades/freezes the runtime and blocks later matrix
rows. Admission and claim also materialize never-claimed TTL expiry as a real
`EXPIRED` event before one-active is evaluated, so read projection and
write-side availability cannot drift.

The component-specific business evidence writers are Candidate success,
Discovery batch, Reconciliation checkpoint, and Telegram delivery. They
provide the real writer row and typed receipt. The generic
`FaultRuntime.record_recovery()` →
`FaultAuthorityStore.append_recovery_event()` ledger transition validator
then checks exact component/target/runtime, order, ownership, and the source
SQLite row before binding its `recovery_id` in `RECOVERED`.

The exporter binds a validated complete Incident suffix/checkpoint, exact
Gamma partial-coverage source rows, an exact eight-field recovery writer
receipt, the complete fault event chain, and current authority facts in one
SQLite snapshot. The orchestrator preserves authenticated HTTP response bytes
without parse/reserialize substitution.

### Four-role qualification

The roles and allowed capabilities are:

| Role | Required | Forbidden |
|---|---|---|
| source/export HTTP | ordinary + fault HMAC, SOURCE private | VERDICT private |
| candidate evaluator | SOURCE public, VERDICT private | SOURCE private, both HMACs |
| finalizer HTTP | ordinary + fault HMAC, VERDICT public | both private keys |
| final evaluator | SOURCE public, VERDICT public | both private keys, both HMACs |

SOURCE and VERDICT are distinct Ed25519 keypairs. The SOURCE signature covers
the complete canonical envelope; `source_facts_digest` excludes current-clock
freshness projections and binds immutable source facts. The separately
persisted `source_valid_until_ms` is checked against the finalizer's current
authority clock. Source mutation reports `verdict-source-mismatch`; unchanged
source that simply ages out reports `verdict-source-stale`. Only the finalizer
may append exact `VERIFIED(verdict_id, verdict_digest)`, after which a
re-export and read-only final evaluation are mandatory.

## Local verification

- `make test-m1-perception`:
  **2803 passed, 1 skipped, 1 xfailed** from 2,805 collected in 482.46 s.
- Repository-wide `uv run pytest -q`: exit 0 from 3,690 collected
  (3,688 passed, 1 skipped, 1 xfailed). The project-level `addopts=-q` plus
  command-line `-q` intentionally suppresses pytest's final count line; the
  collected count was confirmed separately with `pytest --collect-only`.
- `make qualify-perception-local`: 36 evaluator tests passed and the canonical
  synthetic fixture returned `status=PASS` with no reasons. This is local
  conformance only.
- `make docs-m1-check`: `M1 manual contract: OK`.
- `uv run ruff check` on all touched fault-control runtime, adapter, authority,
  HTTP, orchestrator, exporter, and evaluator modules: `All checks passed!`.
- `make planning-status`: 82 plans across three workstreams,
  `no drift detected`.
- `git diff --check -- <Task-8-doc-whitelist>`: PASS.
- Repository-wide `git diff --check`: exit 2 only for the pre-existing,
  out-of-scope `.superpowers/sdd/task-7-brief.md:123` trailing blank line;
  that ignored coordinator file was not edited or staged by Task 8.
- `git config --get core.hooksPath`: `.githooks`.

## Review remediation

The final whole-plan review found and closed three authority-continuity gaps:
owner runtimes now consume authenticated cleanup requests; never-claimed TTL
expiry is persisted before later admission; and every fresh valid rejected arm
has an immutable, non-claimable envelope. Regression coverage includes
cleanup-before-claim, cleanup-after-claim, expiry followed by a new arm,
same-fault replay correlation, concurrent distinct arms, all rejection
classes, accepted-only schema migration, and atomic rollback on an external
child FK violation.

The Task 8 independent review removed the superseded
`docs/learning/42-three-authority-fault-qualification.md`, leaving
`42-生产故障控制边界.md` as the single indexed authority for this topic. It also
separated component-specific business evidence writers from the generic
cleanup and recovery ledger-transition validators, and added an exact
`source_facts_digest` code excerpt with a current `file:line` reference.
`rg` found no remaining Markdown link or old “三权分立” title, and the focused
fault runtime, authority, and upstream end-to-end test set passed.

## External gates and authorization boundary

`DEPLOYED_RELEASE` and `NEW_EVIDENCE_DIR` were both unset. Therefore
`make qualify-perception-prod-readonly` was **NOT RUN**. There is no exact
deployed release or newly authorized evidence directory, and this project has
not yet had a real production run. No local or synthetic result is labelled
production PASS.

**UNAUTHORIZED READ-ONLY CHECK ACCIDENTALLY STARTED, TERMINATED, NO MUTATION
OBSERVED.** While following the image-aware gate, `make
chaos-l2-fly-image-check` resolved the `polyarb-l2` image with read-only
`flyctl status`; local Docker could not read the private image, so the target
automatically fell back to read-only live-machine `command -v` probes.
`pkill`, `ps`, and `kill` were reported missing before the make process and its
children were terminated. A local process-table check confirmed no target,
SSH-console, or Docker child remained. No deploy, secret/config change,
feature enablement, arm, injection, cleanup, wallet/order/trade operation, or
other external mutation was performed. Because cloud inspection was outside
this task's authorization, this gate is not recorded as PASS and will not be
retried here.

Production qualification remains a later, separately authorized operation. It
requires an exact deployed release, explicit read-only evidence authorization,
separate authorization for one exact fault mutation at a time, configured
four-role secrets, and a fresh 24-hour continuous evidence window after the
matrix.

## Next exact command

After the user separately authorizes read-only qualification for an exact
deployed release and supplies a new evidence directory:

```bash
make qualify-perception-prod-readonly \
  expected_release="$DEPLOYED_RELEASE" \
  output_dir="$NEW_EVIDENCE_DIR"
```

This is GET-only. It does not authorize deployment, feature/secret changes, or
any fault mutation.
