# Task 3 report — scoped fault control API

## Status

Implemented and verified. No live deployment, production mutation, secret
setting, feature enabling, or fault-adapter work was performed.

## Delivered

- Added disabled-by-default Settings for a distinct upstream fault-control
  secret and bounded TTL. Enabling with an empty or ordinary-control-equivalent
  secret is invalid.
- Added dedicated fault-domain HMAC verification with domain separation,
  timestamp/nonce checks, constant-time comparison, distinct replay storage,
  bounded bodies, strict duplicate-key/unknown-field JSON rejection, and
  bounded SQLite deadlines.
- Added arm and exact-ID cleanup mutation endpoints plus read-only runtime and
  bounded redacted fault projection endpoints.
- Cleanup appends only a typed, action-only `cleanup-requested` receipt, waits
  briefly for producer-owned proof, and never writes `cleaned`, `abandoned`, or
  `expired`.
- Added the environment-only-secret CLI and all requested Make entry points.
  Existing `test-m1` semantics remain unchanged; `test-m1-perception` covers
  both `tests/perception/` and `tests/m1-perception/`.
- Preserved ordinary `/control/perception` authentication compatibility.
  The existing middleware now shares its already-authenticated bounded body
  through request state because Starlette's middleware boundary otherwise
  presented an empty downstream body.

## Authorized plan correction

The coordinator authorized the minimal additive correction to the locked
four-table design:

- `neg_risk_fault_events.state` may be NULL only for
  `action='cleanup-requested'`;
- `FaultEventAction.CLEANUP_REQUESTED` replaces free-form action strings;
- action-only receipts remain canonical, hash-chained, append-only,
  time-ordered, evidence-whitelisted, and linked to an intact fault-domain
  nonce/authorization row;
- lifecycle validation, claiming, relinquishment, and projection skip
  action-only receipts, so they cannot advance or satisfy cleanup/terminal
  proof.

No fifth table was introduced. The fresh-database constraint remains as
authorized for these undeployed tables.

## TDD evidence

1. Action-only contract RED: collection initially failed because
   `FaultEventAction` did not exist; after adding the type, focused tests failed
   because `request_cleanup` did not exist.
2. Action-only contract GREEN: typed nullable-state action receipt,
   idempotency, replay rejection, hashing, schema constraint, and projection
   behavior passed.
3. Lifecycle compatibility RED: a cleanup action at the history tail caused
   `claim_pending` to return `None`.
4. Lifecycle compatibility GREEN: lifecycle-tail selection now skips
   action-only receipts.
5. API RED: six focused Settings/API tests failed with missing validation and
   404 routes.
6. API GREEN: dedicated Settings, routes, handlers, CLI, and Make targets
   passed together with the existing ordinary perception controls.

## Verification

- `uv run pytest tests/perception/test_fault_authority.py tests/m1-perception/test_perception_fault_controls.py tests/m1-perception/test_perception_controls.py -q`
  — 84 passed.
- `uv run pytest tests/perception/ tests/m1-perception/test_perception_fault_controls.py tests/m1-perception/test_perception_controls.py -q`
  — 100% passed.
- Focused Ruff check across every changed Python file and test — passed.
- `git diff --check` — passed.
- `make help` — all five requested entries are present.
- `python -m polyarb.cli_perception_faults --help` — passed.

## Self-review

- Plaintext ownership capability never crosses HTTP.
- Neither secret is accepted on argv or included in DB rows/responses.
- Invalid targets and bodies are rejected before fault-intent append.
- Missing or corrupt read evidence returns unavailable, never an optimistic
  projection.
- HTTP cleanup cannot fabricate process-owned lifecycle proof.
- No user-owned planning/progress files were staged or modified by this task.

## Concerns

- The cleanup wait is deliberately bounded to 200 ms; a later producer-owned
  receipt is observed through the idempotent status endpoint.
- Repository-wide mypy remains noisy from pre-existing missing third-party
  stubs and unrelated baseline errors; Ruff and proportional runtime tests are
  clean.

## Security review remediation

The six review findings were addressed in a separate follow-up change.

### Schema evolution

The coordinator authorized evolving the fresh, undeployed
`neg_risk_fault_auth_nonces` table without adding a fifth table:

- the former one-row-per-nonce shape became append-only typed
  `reservation`/`attempt` rows;
- a partial unique index permits exactly one reservation per nonce;
- each authenticated attempt records operation, optional normalized fault ID,
  request digest, accepted/rejected outcome, reason, server occurrence time,
  reservation link, and a hash covering every persisted fact;
- attempts have a same-table foreign-key reservation link; reservation,
  outcome, and link shapes are enforced with SQLite CHECK constraints;
- UPDATE/DELETE triggers remain unchanged;
- accepted intents are schema-constrained to `status='accepted'` with no
  rejection reason. Rejections now exist only as auth attempt facts.

This is a fresh-schema change only. No migration or deployed database was
touched.

### Security RED/GREEN evidence

1. Rejection audit RED: runtime mismatch, replay, and active-chain rejection
   produced five intents/events instead of one accepted chain, and the nonce
   table lacked typed attempt columns.
2. Rejection audit GREEN: only the accepted arm creates intent/lifecycle rows;
   every authenticated rejection appends a hashed attempt linked to the unique
   reservation.
3. Server-time RED: valid signatures at -299 and +299 seconds persisted the
   signed client time, extending/regressing TTL history.
4. Server-time GREEN: arm/action times use server milliseconds; signed time is
   only skew/canonical authorization input. Both skew directions preserve the
   one-second TTL and monotonic event history.
5. Cleanup retry RED: a fresh valid nonce for the exact fault returned 400.
6. Cleanup retry GREEN: each fresh nonce is reserved/audited, the existing
   action-only receipt is observed without duplication, and bounded terminal
   polling continues.
7. Deadline RED: an already-expired mutation deadline was accepted; a real
   SQLite `BEGIN IMMEDIATE` barrier surfaced an unnormalized
   `database is locked`.
8. Deadline GREEN: busy timeout is capped by the shared monotonic deadline;
   checks run before mutation and COMMIT; deadline lock failures become
   `TimeoutError`; rollback leaves reservation/attempt/intent/event counts at
   zero. A Starlette barrier also proves no 409 response can race a later
   commit.
9. Terminal RED: producer-owned `cleaned` truth returned 409/pending instead
   of its current terminal state.
10. Terminal GREEN: cleanup returns explicit `already-terminal` plus
    `current_state` for every recognized terminal/non-injecting lifecycle
    outcome; the cleanup-request action alone remains non-terminal.
11. Read-path RED: status/runtime performed synchronous multi-connection
    SQLite reads and `read_snapshot` did not exist.
12. Read-path GREEN: one read-only `BEGIN` snapshot produces runtime or
    history+projection from one connection; all HTTP reads execute off-loop
    under an explicit deadline and corruption/timeout is unavailable-shaped.

### Remediation verification

- Focused authority, new Starlette fault controls, and existing perception
  controls: 0 failures.
- Proportional `tests/perception/` plus both control files: 100% passed.
- Schema lockstep and SQLite migration tests: 100% passed.
- Ruff, `git diff --check`, CLI help, Make help, docs gate, and planning-status:
  passed.
