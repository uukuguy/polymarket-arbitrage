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
