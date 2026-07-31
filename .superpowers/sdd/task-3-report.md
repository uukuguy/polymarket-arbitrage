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

## Second security re-review remediation

The remaining semantic-linkage and hard-bound findings were repaired without a
fifth table and without weakening append-only authority.

### Exact RED/GREEN evidence

1. Semantic-link RED: seven self-consistent owner-tamper fixtures dropped the
   append-only trigger, changed one accepted attempt's operation, fault ID,
   request digest, or authorization digest, and recomputed its row hash.
   Snapshot/history still reported available in all seven vulnerable cases.
2. Semantic-link GREEN: accepted intents persist exact reservation ID, attempt
   ID, and request digest; cleanup action evidence persists the same direct
   identifiers. Validation loads those exact primary-key rows and requires
   equality across nonce, authorization, operation, normalized fault ID, and
   request digest. All nine focused semantic/replay cases pass.
3. Replay GREEN: an attempt whose identity differs from its reservation is
   permitted only when it is exactly `rejected/nonce-replay`; the schema has a
   cross-row insert trigger, and fault-scoped validation rejects any other
   semantic mismatch.
4. Cleanup-bound RED: an injected 500 ms snapshot read made cleanup take
   535 ms because each poll started an independent 850 ms read budget.
5. Cleanup-bound GREEN: cleanup passes its one absolute 200 ms deadline into
   every snapshot read; SQLite busy timeout and the outer guard use only the
   remaining duration. The deterministic slow-read case returns unavailable
   within the locked tolerance.
6. Mutation-bound RED attempt 1: removing the unbounded settle await was not
   sufficient because Starlette request-loop teardown waited for
   `asyncio.to_thread`'s default-executor worker; the blocked response still
   exceeded one second.
7. Mutation-bound GREEN attempt 2: this narrow authority boundary now uses a
   daemon worker bridge plus the cooperative absolute-deadline protocol.
   Every SQL loop/query checks the same deadline, COMMIT re-caps SQLite
   `busy_timeout` to remaining time and checks immediately beforehand, and the
   HTTP guard has no unbounded settle path. Releasing a delayed worker after
   the unavailable response leaves auth, intent, and event row counts at zero.
8. Scale GREEN: 40 complete historical intent/auth chains plus one current
   chain prove snapshot validation performs no global auth `fetchall()` and
   loads at most the bounded current candidates and exact linked rows.
   `_has_active_chain` is one current-runtime/latest-state indexed query.
9. Worker-cap RED: five blocked bridge calls started five daemon threads.
10. Worker-cap GREEN: a module-wide four-slot `BoundedSemaphore` admits at
    most four workers with no queue; the fifth call fails unavailable
    immediately. Releasing the four workers restores admission, and thread
    enumeration proves no named authority worker remains. The preceding
    timed-out slow-read case also proves a closed request loop cannot leak its
    slot when the daemon finishes.
11. Sequencing regression/GREEN: the proportional run exposed that result
    delivery could wake a following request just before semaphore release, and
    that a final normal cleanup poll could start with only a few milliseconds
    remaining. Workers now release admission before loop delivery; cleanup
    reserves the final 25 ms rather than starting an under-budget read.
    Rapid sequential cleanup remains pending, while the injected 500 ms read
    remains bounded unavailable.

### Second remediation verification

- Authority suite: 64 passed.
- Fault API plus existing perception controls: 40 passed.
- Schema lockstep and SQLite migration regressions: 32 passed.
- Proportional perception suite, Ruff, `git diff --check`, Make help, M1 docs
  gate, and planning-status: recorded after final committed-tree verification.

## Third security re-review remediation

The final medium finding was closed without a new table or mutable current-state
row.

### Exact RED/GREEN evidence

1. Active-query RED: traced `_current_active_fault_ids()` used one global
   `neg_risk_fault_intents` join with correlated subqueries and ordering.
   `LIMIT` did not bound the VM work.
2. Active-query GREEN: the authority iterates the four fixed producer
   components, reads each indexed current runtime, then probes at most the two
   newest accepted intents for that exact runtime. Total candidate rows are
   bounded to eight, preserving the adversarial two-active-chain fail-closed
   check.
3. Query-plan GREEN: `EXPLAIN QUERY PLAN` reports
   `SEARCH neg_risk_fault_intents USING COVERING INDEX
   idx_neg_risk_fault_intent_active_runtime (component=? AND release_id=? AND
   machine_id=? AND boot_id=? AND status=?)`; it reports neither an intent
   table scan nor a temporary B-tree.
4. SQLite-VM RED: a 10-million-row recursive SQLite VM query with a 20 ms
   absolute deadline occupied the worker for about 1.15 seconds.
5. SQLite-VM GREEN: every deadline-aware connection installs a progress
   handler checked every 1,000 opcodes. `interrupted` is normalized to the
   authority timeout/unavailable shape, the handler is cleared before rollback
   and again in `finally`, and the deterministic worker test completes below
   150 ms then immediately reacquires the released worker slot.
6. Cleanup propagation RED: the fresh-nonce/existing-action branch called
   history validation with `deadline_monotonic=None`.
7. Cleanup propagation GREEN: that branch passes the caller's exact absolute
   deadline; the test observes identity equality, not a restarted budget.
8. Bounded-query regression: the first latest-only probe hid the existing
   two-active-chain tamper fixture. The final top-two-per-runtime design stays
   constant-bounded and restores fail-closed projection.

## Final active-projection review remediation

The raw top-two intent probe still allowed terminal chains to mask an older
active chain. This was repaired without changing the four-table authority
shape.

### Exact RED/GREEN evidence

1. Masking RED: two complete, self-consistent fixtures created an old active
   chain, respectively one and four newer terminal chains, and a newest active
   chain. Raw `ORDER BY accepted_at_ms DESC LIMIT 2` saw only the newest active
   plus a terminal chain, so the old fault snapshot incorrectly remained
   available.
2. Masking GREEN: each exact-runtime indexed query now applies an indexed
   correlated latest-lifecycle predicate first, retaining only current
   non-terminal candidates; `ORDER BY ... LIMIT 2` is applied to that projected
   active result. Both masking variants and the original adjacent two-active
   fixture return `multiple-active-chains` unavailable.
3. Query-plan GREEN: the final plan reports
   `SEARCH i USING COVERING INDEX idx_neg_risk_fault_intent_active_runtime`,
   followed by `CORRELATED SCALAR SUBQUERY` and
   `SEARCH e USING INDEX sqlite_autoindex_neg_risk_fault_events_1
   (fault_id=?)`. It reports neither an intent table scan nor a temporary
   B-tree.
4. Deadline preservation: the query may inspect terminal history for
   integrity, but the already-verified SQLite progress handler bounds VM work
   at the same absolute deadline and releases the authority worker slot on
   interruption.

### Final review verification

- Focused authority, fault API, and existing perception controls: 109 passed.
- Schema lockstep and SQLite migration regressions: 32 passed.
- The complete proportional suite ran to 100% twice and every Task 3 test
  passed. Under host load average about 21, the first run missed two unrelated
  reconciliation-supervisor two-second subprocess windows; after load dropped,
  both passed in isolation. The second run hit unrelated Candidate timing and
  Resource owner-read concurrency tests; each passed in isolation, with the
  Resource test passing immediately when run alone.
- This round therefore records a green Task 3 matrix plus green isolation for
  every proportional-suite failure, not a false claim that the loaded-host
  aggregate process exited zero. The unchanged baseline had already completed
  the same proportional command at 100% exit zero before this final query-only
  change.
- Ruff, docs, planning, and diff gates are recorded after the final
  committed-tree verification.
