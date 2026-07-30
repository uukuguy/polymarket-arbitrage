# Scoped Upstream Fault Control Implementation Plan

> **Execution:** Follow this plan task by task with TDD. Do not combine commits.

**Goal:** Add a dormant-by-default, production-grade fault-control boundary for
Gamma, Candidate CLOB, and Telegram calls, with exact runtime identity,
append-only lifecycle evidence, deterministic cleanup, and an independent
qualification verdict.

**Architecture:** The control plane appends a separately authorized intent to
SQLite. The exact producer process claims that intent only at a safe batch
boundary and installs one immutable in-memory `ActiveFault`; client calls never
query SQLite. Typed adapters can affect only their enumerated call class and
target. Cleanup clears memory first and records evidence second. A read-only
evaluator binds intent, injection, Incident, cleanup, and business recovery
facts; no adapter or affected component can declare PASS.

**Tech Stack:** Python 3.12, asyncio, Starlette, Pydantic Settings, SQLite,
httpx/py-clob-client, pytest, uv, Make.

**Approved design:**
`docs/superpowers/specs/2026-07-30-scoped-upstream-fault-control-design.md`

---

## Locked implementation decisions

### Authority and default posture

- Add `POLYARB_UPSTREAM_FAULT_CONTROL_ENABLED`, default `false`.
- Add a distinct `POLYARB_UPSTREAM_FAULT_CONTROL_SECRET`; never reuse the
  ordinary perception-control signature as sufficient mutation authority.
- Every arm/cleanup request must pass both the existing perception-control
  HMAC and a fault-domain HMAC:

  ```text
  polyarb-upstream-fault-v1
  {timestamp}
  {nonce}
  {method}
  {request_path}
  {sha256(body)}
  ```

- Missing config, damaged control state, expired intent, identity mismatch,
  replay, unsupported parameter, or evidence-store read failure means
  pass-through. It also appends a rejected-control fact when the store is
  writable.
- Missing or damaged qualification evidence means FAIL.

### Runtime identity and process boundary

Production can isolate Candidate, Discovery, and Reconciliation in child
processes. Therefore:

1. `ProducerSupervisor` generates a UUID4 `producer_boot_id` for every child
   attempt and passes it in `POLYARB_PRODUCER_BOOT_ID`.
2. `worker_cli` registers
   `(component, release_id, FLY_MACHINE_ID, producer_boot_id,
   supervisor_run_id, attempt)` as an append-only runtime-start fact.
3. The control API targets that exact registered runtime.
4. The child checks for one pending intent at a safe cycle boundary, claims it
   transactionally, and installs it in memory.
5. The adapter hot path consults only the in-memory controller.
6. A child never reloads an old `armed` or `injected` intent after restart.
7. The daemon registers a separate `notification` runtime for the
   `OpportunityWatcher`; Telegram is not incorrectly attributed to a producer
   child.

The non-isolated daemon path uses the same runtime registration and claim
protocol; it does not get a second implementation.

### Typed surface

```python
class FaultCallClass(StrEnum):
    GAMMA_DISCOVERY_EVENT_PAGE = "gamma-discovery-event-page"
    GAMMA_RECONCILIATION_EVENT_PAGE = "gamma-reconciliation-event-page"
    CLOB_CANDIDATE_BOOK_BATCH = "clob-candidate-book-batch"
    TELEGRAM_OPPORTUNITY_CARD = "telegram-opportunity-card"


class FaultKind(StrEnum):
    GAMMA_TIMEOUT = "gamma-timeout"
    GAMMA_PARTIAL = "gamma-partial"
    GAMMA_MALFORMED = "gamma-malformed"
    GAMMA_CURSOR = "gamma-cursor"
    CLOB_MISSING_LEG = "clob-missing-leg"
    CLOB_429 = "clob-429"
    CLOB_LATENCY = "clob-latency"
    TELEGRAM_FAILURE = "telegram-failure"
```

Allowed mapping:

| Call class | Exact target key | Allowed fault kinds |
|---|---|---|
| Gamma Discovery page | `discovery` | timeout, partial, malformed |
| Gamma Reconciliation page | `reconciliation` | cursor |
| Candidate books | durable `group_id` | missing-leg, 429, latency |
| Telegram card | decimal durable `notification.id` | failure |

No raw URL, hostname, token, header, cookie, response body, Python expression,
or shell fragment is accepted or persisted.

### Parameter bounds

```python
FAULT_PARAMETER_RULES = {
    FaultKind.GAMMA_TIMEOUT: {"delay_ms": (1, 30_000)},
    FaultKind.GAMMA_PARTIAL: {"keep_events": (0, 99)},
    FaultKind.GAMMA_MALFORMED: {},
    FaultKind.GAMMA_CURSOR: {},
    FaultKind.CLOB_MISSING_LEG: {"leg_index": (0, 499)},
    FaultKind.CLOB_429: {},
    FaultKind.CLOB_LATENCY: {"delay_ms": (1, 30_000)},
    FaultKind.TELEGRAM_FAILURE: {},
}
```

Intent TTL is integer milliseconds in `[1_000, 120_000]`. One intent injects
once. After the matching call consumes it, later calls pass through even before
cleanup.

### Append-only schema

Add these tables to `src/polyarb/storage/schemas.py`:

- `neg_risk_fault_runtime_starts`: exact child or notification-runtime
  identity.
- `neg_risk_fault_auth_nonces`: separate fault-domain replay protection.
- `neg_risk_fault_intents`: immutable accepted or rejected intent envelope.
- `neg_risk_fault_events`: hash-chained lifecycle facts.

`neg_risk_fault_events.state` allows:
`authorized`, `armed`, `injected`, `detected`, `contained`, `recovered`,
`cleaned`, `verified`, `rejected`, `expired`, `abandoned`,
`cleanup-failed`, `recovery-timeout`, `evidence-invalid`, `escalated`.

Do not create a mutable “current fault” row. Current state is a deterministic
projection from the immutable intent/event chain. Enforce unique accepted
nonce, unique injection per fault, one claim per intent, and one cleanup
terminal per fault with constraints/indexes.

### Chain-truth matrix

| Fault | Injection boundary | Existing Incident writer | Recovery writer |
|---|---|---|---|
| Gamma timeout/malformed | `DiscoveryWorker.run_batch` page call | `GammaBatchIncidents(scope="discovery")` | completed `neg_risk_discovery_batches.id` |
| Gamma partial | transformed `EventPage`, rejected by coverage contract | coverage rejection fact plus fault event | later completed Discovery batch |
| Gamma cursor | `ReconciliationRunner` page call | `GammaBatchIncidents(scope="reconciliation")` | advancing/terminal reconciliation window |
| CLOB missing-leg/429/latency | `CandidateWatcher.run_once(group_id)` books call | `CandidateGroupIncidents` | exact group success receipt with current membership hash |
| Telegram failure | `OpportunityWatcher.deliver_pending_notifications`, exact `notification.id` | `NotificationIncidents` | exact delivered notification-attempt row |

The new fault event may reference the Incident ID only after reading the
existing Incident authority. It must not invent a parallel Incident.

---

## Task 1: Define the fault model, append-only authority, and plan summary

**Files:**

- Create: `src/polyarb/perception/fault_control.py`
- Create: `src/polyarb/perception/fault_authority.py`
- Modify: `src/polyarb/storage/schemas.py`
- Create: `tests/perception/test_fault_control.py`
- Create: `tests/perception/test_fault_authority.py`
- Create:
  `docs/superpowers/plans/2026-07-30-scoped-upstream-fault-control-SUMMARY.md`

**Step 1: Write RED model contracts**

In `tests/perception/test_fault_control.py`, cover:

- every `FaultKind` maps to exactly one `FaultCallClass`;
- target normalization rejects empty, oversized, URL-like, secret-like, and
  cross-class targets;
- parameters reject unknown keys, bool-as-int, non-finite values, and values
  outside locked bounds;
- TTL bounds;
- release, machine, boot UUID, and nonce digest validation;
- one controller admits at most one active fault;
- exact call class and target are required;
- first matching call consumes the single-use fault;
- expired intent passes through;
- invalid controller input never blocks the supplied real-call coroutine;
- `clear()` swaps active state to `None` before invoking any receipt writer;
- receipt-writer failure freezes future admission but leaves calls
  pass-through;
- a fresh controller starts empty and never rearms persisted state.

Use a small explicit API:

```python
controller = FaultController(
    runtime=FaultRuntimeIdentity(
        component="candidate",
        release_id="a" * 40,
        machine_id="machine-1",
        boot_id=UUID("12345678-1234-4678-9234-567812345678"),
    ),
    monotonic=clock,
)
controller.admit(intent, claimed_at_ms=1_000)
decision = controller.consume(
    FaultCall(
        call_class=FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH,
        target_key="group-1",
    )
)
controller.clear(fault_id, receipt_writer=writer)
```

Run:

```bash
uv run pytest tests/perception/test_fault_control.py -q
```

Expected: FAIL because modules do not exist.

**Step 2: Write RED authority/schema contracts**

In `tests/perception/test_fault_authority.py`, create a real temporary
SQLite DB and assert:

- runtime registration is append-only and duplicate identity is idempotent;
- the accepted intent stores only digests/canonical whitelisted parameters;
- nonce replay, runtime mismatch, stale runtime, and second-active admission
  reject transactionally;
- a rejected request cannot be claimed;
- only the exact component/runtime can claim;
- claim is single-use under two concurrent SQLite connections;
- corrupt/missing tables return an unavailable projection, never an active
  intent;
- lifecycle hashes validate end to end and tampering is detected;
- projection rejects two active chains, missing predecessor, state regression,
  injection without claim, and cleanup before injection;
- stale `armed`/`injected` chains project as abandoned after a new runtime
  start, but never become claimable;
- read-only projection cannot mutate the DB.

Run:

```bash
uv run pytest tests/perception/test_fault_authority.py -q
```

Expected: FAIL.

**Step 3: Implement the minimal model and authority**

Implement frozen dataclasses/enums and pure canonical hash functions in
`fault_control.py`. Keep SQLite and Starlette imports out of this module.

Implement `FaultAuthorityStore` in `fault_authority.py` with bounded busy
timeouts and these public methods:

```python
register_runtime_start(identity, *, supervisor_run_id, attempt, started_at_ms)
accept_intent(request, *, auth, accepted_at_ms) -> IntentAdmission
claim_pending(identity, *, claimed_at_ms) -> FaultIntent | None
append_event(fault_id, state, *, occurred_at_ms, evidence) -> FaultEvent
project_fault(fault_id, *, now_ms) -> FaultProjection
current_runtime(component) -> FaultRuntimeIdentity | None
validate_history(fault_id) -> FaultHistory
```

All JSON uses:

```python
json.dumps(value, sort_keys=True, separators=(",", ":"),
           ensure_ascii=False, allow_nan=False)
```

Never add fault methods to the already oversized
`OpportunityPerceptionStore`.

**Step 4: Run focused tests**

```bash
uv run pytest \
  tests/perception/test_fault_control.py \
  tests/perception/test_fault_authority.py -q
```

Expected: PASS.

**Step 5: Create the plan SUMMARY and commit**

Create the SUMMARY immediately with status `in-progress`, approved design link,
locked call classes, schema names, tests run, and remaining tasks. This prevents
plan-scoped code from landing without a recovery anchor.

```bash
git add src/polyarb/perception/fault_control.py \
  src/polyarb/perception/fault_authority.py \
  src/polyarb/storage/schemas.py \
  tests/perception/test_fault_control.py \
  tests/perception/test_fault_authority.py \
  docs/superpowers/plans/2026-07-30-scoped-upstream-fault-control-SUMMARY.md
git commit -m "feat(m1): add upstream fault authority core"
make planning-status
```

Expected: commit succeeds and planning status has no DRIFT.

---

## Task 2: Bind exact producer boot identity and safe boundary claiming

**Files:**

- Modify: `src/polyarb/perception/supervisor.py`
- Modify: `src/polyarb/perception/worker_cli.py`
- Modify: `src/polyarb/daemon/main.py`
- Modify: `src/polyarb/daemon/opportunity_watcher.py`
- Modify: `src/polyarb/perception/candidate_watcher.py`
- Modify: `src/polyarb/perception/discovery.py`
- Modify: `src/polyarb/perception/reconciliation.py`
- Create: `src/polyarb/perception/fault_runtime.py`
- Create: `tests/perception/test_fault_runtime.py`
- Modify: `tests/perception/test_supervisor.py`

**Step 1: Write RED process-identity tests**

Assert:

- the supervisor generates a fresh UUID4 per attempt and passes
  `POLYARB_PRODUCER_BOOT_ID`;
- child registration binds release, machine, component, boot,
  supervisor-run, and attempt;
- absent/invalid boot ID makes fault capability pass-through while the producer
  still runs normally;
- a restarted child abandons but never claims the previous child intent;
- Candidate claims before selecting a group batch;
- Discovery/Reconciliation claim immediately before a page batch;
- store read/claim failure logs a redacted warning and executes the normal
  upstream call;
- cancellation invokes controller cleanup;
- both isolated and in-daemon builders receive the same `FaultRuntime`;
- the parent daemon registers a distinct `notification` runtime and injects it
  into `OpportunityWatcher`.

Run:

```bash
uv run pytest \
  tests/perception/test_fault_runtime.py \
  tests/perception/test_supervisor.py -q
```

Expected: FAIL.

**Step 2: Implement `FaultRuntime`**

`fault_runtime.py` owns the bridge between safe-boundary SQLite claims and the
in-memory controller:

```python
class FaultRuntime:
    async def sync_before_batch(self) -> None:
        """Claim at most one intent; store failure leaves controller unchanged."""

    async def cleanup(self, fault_id: str, reason: str) -> CleanupResult:
        """Clear memory first, append cleanup second."""

    def consume(self, call: FaultCall) -> FaultDecision:
        """Pure in-memory hot-path decision."""
```

Builder behavior:

- when the feature flag is false, use `PassThroughFaultRuntime`;
- when enabled but authority is unavailable, use pass-through and expose a
  degraded control fact;
- no producer startup depends on successful fault-store access.

**Step 3: Wire runtime registration and safe-boundary sync**

Do not put a DB read inside `GammaClient._get`, `ClobReaderClient.get_books`, or
Telegram HTTP transport.

For child workers, construct runtime identity from:

```python
FaultRuntimeIdentity(
    component=component,
    release_id=settings.release_id,
    machine_id=os.environ.get("FLY_MACHINE_ID", "local"),
    boot_id=UUID(os.environ["POLYARB_PRODUCER_BOOT_ID"]),
)
```

For the non-isolated path, create one explicit boot UUID per component in
`daemon/main.py`, register it, and pass it to the same builders. Register a
fourth `notification` runtime from the daemon boot and pass its runtime to the
existing `OpportunityWatcher`.

**Step 4: Run tests and commit**

```bash
uv run pytest \
  tests/perception/test_fault_runtime.py \
  tests/perception/test_supervisor.py \
  tests/perception/test_candidate_watcher.py \
  tests/perception/test_discovery.py \
  tests/perception/test_reconciliation.py \
  tests/daemon/test_opportunity_watcher.py -q
git add src/polyarb/perception/fault_runtime.py \
  src/polyarb/perception/supervisor.py \
  src/polyarb/perception/worker_cli.py \
  src/polyarb/daemon/main.py \
  src/polyarb/daemon/opportunity_watcher.py \
  src/polyarb/perception/candidate_watcher.py \
  src/polyarb/perception/discovery.py \
  src/polyarb/perception/reconciliation.py \
  tests/perception/test_fault_runtime.py \
  tests/perception/test_supervisor.py
git commit -m "feat(m1): bind fault intents to producer boots"
make planning-status
```

Expected: tests PASS; no DRIFT.

---

## Task 3: Add separate HMAC fault authority, API, CLI, and Make entries

**Files:**

- Modify: `src/polyarb/config.py`
- Create: `src/polyarb/http/perception_faults.py`
- Modify: `src/polyarb/http/app.py`
- Create: `src/polyarb/cli_perception_faults.py`
- Create: `tests/m1-perception/test_perception_fault_controls.py`
- Modify: `Makefile`

**Step 1: Write RED API/security tests**

Cover:

- disabled flag returns `409 fault-control-disabled` before mutation;
- missing distinct fault signature returns 401 even with valid ordinary HMAC;
- ordinary control nonce/signature cannot be replayed as a fault nonce;
- canonical fault signature is constant-time verified;
- timestamp skew, malformed nonce, replay, oversized body, unknown field,
  unknown kind, invalid target, TTL, and parameters fail before intent append;
- exact current runtime identity is required;
- second active intent returns 409;
- secrets/raw URLs never appear in response, logs, or DB;
- `POST /control/perception/faults/arm` returns accepted `fault_id` and digests;
- `POST /control/perception/faults/cleanup` is exact-ID and idempotent;
- read-only `GET /perception/faults/{fault_id}` returns a bounded redacted
  projection and complete-history flag;
- control-store timeout is 409/unavailable and does not affect producers.

Run:

```bash
uv run pytest tests/m1-perception/test_perception_fault_controls.py -q
```

Expected: FAIL.

**Step 2: Add disabled-by-default Settings**

Add:

```python
upstream_fault_control_enabled: bool = False
upstream_fault_control_secret: SecretStr = SecretStr("")
upstream_fault_control_max_ttl_ms: int = Field(
    default=120_000, ge=1_000, le=120_000
)
```

Validation rule: enabled + empty secret is always invalid; tests that enable
the capability must provide an explicit test secret. Never expose the secret
in model dumps or health.

**Step 3: Implement dedicated handlers and CLI**

Keep fault mutation code out of `http/control.py`. Reuse its body-size,
timestamp, and nonce conventions, but verify the second fault-domain signature
inside `perception_faults.py`.

CLI commands:

```text
python -m polyarb.cli_perception_faults runtime --component candidate
python -m polyarb.cli_perception_faults arm --intent intent.json
python -m polyarb.cli_perception_faults cleanup --fault-id "$FAULT_ID"
python -m polyarb.cli_perception_faults status --fault-id "$FAULT_ID"
```

CLI prints redacted JSON and never accepts a secret on argv; it reads
`POLYARB_UPSTREAM_FAULT_CONTROL_SECRET` from the environment.

**Step 4: Add Makefile entry points**

Add documented targets:

```make
fault-runtime-status
arm-upstream-fault
cleanup-upstream-fault
upstream-fault-status
```

`arm-upstream-fault` requires `intent="$INTENT_FILE"` and does not synthesize a
broad target. `cleanup-upstream-fault` requires `fault_id="$FAULT_ID"`.

**Step 5: Run tests and commit**

```bash
uv run pytest \
  tests/m1-perception/test_perception_fault_controls.py \
  tests/m1-perception/test_perception_controls.py -q
make help
git add src/polyarb/config.py src/polyarb/http/perception_faults.py \
  src/polyarb/http/app.py src/polyarb/cli_perception_faults.py \
  tests/m1-perception/test_perception_fault_controls.py Makefile
git commit -m "feat(m1): add scoped fault control API"
make planning-status
```

Expected: tests PASS; all four commands appear in `make help`; no DRIFT.

---

## Task 4: Implement typed Gamma Discovery and Reconciliation adapters

**Files:**

- Create: `src/polyarb/perception/fault_adapters.py`
- Modify: `src/polyarb/perception/discovery.py`
- Modify: `src/polyarb/perception/reconciliation.py`
- Create: `tests/perception/test_gamma_fault_adapter.py`
- Modify: `tests/perception/test_gamma_incidents.py`

**Step 1: Write RED exact-scope adapter tests**

Use fake Gamma clients and deterministic clocks. Assert:

- Discovery timeout raises `httpx.ReadTimeout` only for
  `GAMMA_DISCOVERY_EVENT_PAGE/discovery`;
- malformed raises a deterministic `json.JSONDecodeError`;
- partial returns a structurally valid but coverage-rejected page and cannot
  publish it as complete;
- Reconciliation cursor returns a page whose requested/next cursor violates
  the existing cursor integrity contract;
- Discovery intent cannot affect Reconciliation and vice versa;
- unmatched target, consumed intent, expired intent, controller/store failure,
  and feature-disabled state call the real fake client exactly once;
- injection appends a receipt before the existing Incident is linked;
- timeout/cancellation clears before recovery polling;
- successful post-cleanup Discovery batch and Reconciliation checkpoint are
  newer than injection and bound to the same runtime;
- no response body, event payload, or URL is persisted.

Run:

```bash
uv run pytest \
  tests/perception/test_gamma_fault_adapter.py \
  tests/perception/test_gamma_incidents.py -q
```

Expected: FAIL.

**Step 2: Implement the Gamma adapter**

Expose a protocol-compatible wrapper:

```python
class FaultingGammaPageClient:
    async def fetch_active_event_page(
        self, cursor: str | None, limit: int
    ) -> EventPage:
        decision = self._runtime.consume(
            FaultCall(self._call_class, self._target_key)
        )
        if not decision.inject:
            return await self._inner.fetch_active_event_page(cursor, limit)
        if decision.kind is FaultKind.GAMMA_TIMEOUT:
            await asyncio.sleep(decision.delay_s)
            raise httpx.ReadTimeout("qualified-gamma-timeout")
        if decision.kind is FaultKind.GAMMA_MALFORMED:
            raise json.JSONDecodeError("qualified-gamma-malformed", "", 0)
        page = await self._inner.fetch_active_event_page(cursor, limit)
        return self._transform_page(decision, page)
```

Use `dataclasses.replace` for page transformations. Do not edit
`GammaClient._get` and do not match `settings.gamma_url`.

For `gamma-partial`, add a typed `PartialGammaPageError` carrying only counts
and cursor digests. The adapter reads the real page, detects that truncation to
`keep_events` would be incomplete, then raises before `publish_discovery_batch`.
The Discovery runner records a `coverage:partial-or-rejected-page` fault fact
and preserves its cursor. This path intentionally does not manufacture a Gamma
exception Incident or publish a truncated batch.

**Step 3: Connect existing Incident and recovery truth**

After injection, read the Incident authority and require one unambiguous
matching:

- Discovery exception faults:
  scope `discovery`, kind `gamma-timeout|gamma-malformed`;
- Reconciliation:
  scope `reconciliation`, kind `gamma-cursor`.

Do not change `GammaBatchIncidents` recovery semantics except for returning the
Incident ID/receipt needed for the fault chain.

**Step 4: Run tests and commit**

```bash
uv run pytest \
  tests/perception/test_gamma_fault_adapter.py \
  tests/perception/test_gamma_incidents.py \
  tests/perception/test_discovery.py \
  tests/perception/test_reconciliation.py -q
git add src/polyarb/perception/fault_adapters.py \
  src/polyarb/perception/discovery.py \
  src/polyarb/perception/reconciliation.py \
  tests/perception/test_gamma_fault_adapter.py \
  tests/perception/test_gamma_incidents.py
git commit -m "feat(m1): inject typed Gamma qualification faults"
make planning-status
```

Expected: PASS; no DRIFT.

---

## Task 5: Implement exact-group Candidate CLOB adapters

**Files:**

- Modify: `src/polyarb/perception/fault_adapters.py`
- Modify: `src/polyarb/perception/candidate_watcher.py`
- Create: `tests/perception/test_candidate_fault_adapter.py`
- Modify: `tests/perception/test_clob_incidents.py`

**Step 1: Write RED group-scoping tests**

Assert:

- the fault is checked after `read_group(group_id)` and immediately around
  the selected `BooksReader.get_books`;
- a target for group A cannot affect group B, focused opportunity collection,
  Discovery, or the lower-priority lane for a different group;
- missing-leg removes only the bounded `leg_index` result and flows through
  `QuoteCollectionIntegrityError`;
- 429 raises `PolyApiException(status_code=429)`;
- latency waits the exact bounded monotonic delay and is classified by the
  existing Candidate timeout boundary as `clob-latency`;
- no partial quote batch or cross-membership receipt is published;
- cleanup precedes the successful retry;
- recovery requires a newer exact-group success receipt with the membership
  hash read before the recovered CLOB call;
- the fault chain links exactly one `candidate:<group_id>` Incident.

Run:

```bash
uv run pytest \
  tests/perception/test_candidate_fault_adapter.py \
  tests/perception/test_clob_incidents.py -q
```

Expected: FAIL.

**Step 2: Add typed before/after hooks**

Use a typed helper in `fault_adapters.py`:

```python
decision = candidate_fault.before_books(group_id)
books = await books_reader.get_books(token_ids, projection="top")
books = await candidate_fault.after_books(
    decision, token_ids=token_ids, books=books
)
```

This boundary has the durable `group_id` that the generic `BooksReader`
protocol lacks. Do not add a fault parameter to `BooksReader` and do not alter
`ClobReaderClient` globally.

**Step 3: Return Incident/recovery identity from existing authority**

Extend `CandidateGroupIncidents.record_failure` and `verify_success` only as
needed to return the authoritative Incident/receipt pointer. Preserve all
existing callers and classification behavior.

**Step 4: Run tests and commit**

```bash
uv run pytest \
  tests/perception/test_candidate_fault_adapter.py \
  tests/perception/test_clob_incidents.py \
  tests/perception/test_candidate_watcher.py -q
git add src/polyarb/perception/fault_adapters.py \
  src/polyarb/perception/candidate_watcher.py \
  tests/perception/test_candidate_fault_adapter.py \
  tests/perception/test_clob_incidents.py
git commit -m "feat(m1): inject group-scoped CLOB faults"
make planning-status
```

Expected: PASS; no DRIFT.

---

## Task 6: Implement exact-outbox Telegram adapter

**Files:**

- Modify: `src/polyarb/perception/fault_adapters.py`
- Modify: `src/polyarb/daemon/opportunity_watcher.py`
- Create: `tests/perception/test_telegram_fault_adapter.py`
- Create: `tests/perception/test_notification_incidents.py`

**Step 1: Write RED outbox-identity tests**

Assert:

- fault target is the decimal durable `PendingNotification.id`, not message
  text, chat ID, token, or URL;
- only the exact pending notification fails;
- another pending notification in the same delivery loop still uses the real
  sender;
- fault raises a typed deterministic transport exception before Telegram;
- watcher persists the normal failed notification attempt and existing
  `telegram-delivery-failed` Incident;
- cleanup happens before retry;
- recovery requires a later `outcome='delivered'` attempt for the exact same
  notification ID;
- card payload and credentials never enter fault tables;
- missing controller/evidence store remains pass-through.

Run:

```bash
uv run pytest \
  tests/perception/test_telegram_fault_adapter.py \
  tests/perception/test_notification_incidents.py -q
```

Expected: FAIL.

**Step 2: Put the typed hook at the durable outbox boundary**

Immediately before the existing call:

```python
decision = self._fault_runtime.consume(
    FaultCall(
        FaultCallClass.TELEGRAM_OPPORTUNITY_CARD,
        str(notification.id),
    )
)
self._telegram_fault.before_send(decision)
await self._send_telegram(self._settings, _format_card(notification))
```

Keep `SendTelegram` and `send_opportunity_alert` signatures unchanged. The
fault hook owns only failure injection; the existing sender owns transport.

**Step 3: Link the authoritative Incident and delivered attempt**

Allow `NotificationIncidents.record_failure` and `verify_delivery` to return
the authoritative Incident/attempt pointer without creating a second lifecycle.

**Step 4: Run tests and commit**

```bash
uv run pytest \
  tests/perception/test_telegram_fault_adapter.py \
  tests/perception/test_notification_incidents.py \
  tests/daemon/test_opportunity_watcher.py -q
git add src/polyarb/perception/fault_adapters.py \
  src/polyarb/daemon/opportunity_watcher.py \
  tests/perception/test_telegram_fault_adapter.py \
  tests/perception/test_notification_incidents.py
git commit -m "feat(m1): inject outbox-scoped Telegram faults"
make planning-status
```

Expected: PASS; no DRIFT.

---

## Task 7: Complete orchestrator, cleanup guarantees, and independent evaluator

**Files:**

- Modify: `scripts/perception_chaos.py`
- Modify: `scripts/perception_fault_readonly.py`
- Modify: `scripts/perception_fault_acceptance.py`
- Modify: `tests/m1-perception/test_perception_chaos_contract.py`
- Modify: `tests/m1-perception/test_perception_fault_acceptance.py`
- Create: `tests/m1-perception/test_upstream_fault_e2e.py`
- Modify: `Makefile`

**Step 1: Write RED lifecycle/orchestrator tests**

For all eight upstream faults, assert:

- execute remains blocked without exact release, target runtime, target key,
  separate authorization, and a new evidence directory;
- baseline must be green before arm;
- arm response binds exact fault/release/machine/boot/call-class/target;
- `try/finally` always calls cleanup after arm, including cancellation,
  detection timeout, malformed API response, evidence write failure, and
  `KeyboardInterrupt`;
- cleanup response proves data-plane clear before receipt persistence;
- `cleanup-failed` freezes the remaining matrix;
- orchestrator accepts exactly one expected Incident (or the explicitly
  modeled Gamma partial coverage fact);
- business recovery receipt is newer than injection and cleanup;
- resulting `evidence.json` contains a complete immutable fault history;
- fixture-generated evidence can test evaluator determinism but uses
  `scope=local-conformance`;
- evaluator rejects missing/duplicate/mismatched intent, injection, Incident,
  cleanup, recovery, runtime identity, event hash, or terminal verified event;
- evaluator cannot write to the source DB or call control endpoints.

Run:

```bash
uv run pytest \
  tests/m1-perception/test_perception_chaos_contract.py \
  tests/m1-perception/test_perception_fault_acceptance.py \
  tests/m1-perception/test_upstream_fault_e2e.py -q
```

Expected: FAIL.

**Step 2: Implement arm → observe → cleanup → recover**

For upstream faults, `perception_chaos.py` must:

1. collect the read-only green baseline;
2. resolve the current exact producer runtime;
3. create one canonical typed intent;
4. arm via the doubly signed control endpoint;
5. queue/wait for the matching component call;
6. observe injection and one authoritative Incident/coverage fact;
7. run cleanup in `finally`;
8. wait for the exact business recovery receipt;
9. export bounded read-only evidence; and
10. invoke no evaluator mutation.

Only after these paths pass tests, set `execute_supported=True` for:

```text
gamma-timeout gamma-partial gamma-malformed gamma-cursor
clob-missing-leg clob-429 clob-latency telegram-failure
```

Keep SQLite/disk/load/process/restart/deploy primitives separate.

**Step 3: Strengthen the independent evaluator**

For `scope=production-fault`, require:

```text
authorized → armed → injected → detected/coverage → contained
→ cleaned → recovered → verified
```

The evaluator must recompute every digest/hash and require exact:

- release/machine/boot;
- fault ID, call class, target digest, parameter digest, nonce digest;
- one injection;
- one Incident/coverage fact;
- cleanup before recovery;
- component-specific recovery table and row ID;
- zero open fault/Incident state;
- existing cross-membership, partial-publication, orphan-run, freshness, and
  reconciliation gates.

Any absent field is a named FAIL reason, never an implicit default.

**Step 4: Run focused and full local gates**

```bash
uv run pytest \
  tests/m1-perception/test_perception_chaos_contract.py \
  tests/m1-perception/test_perception_fault_acceptance.py \
  tests/m1-perception/test_upstream_fault_e2e.py -q
make test-m1-perception
make planning-status
```

Expected: PASS; no DRIFT.

**Step 5: Commit**

```bash
git add scripts/perception_chaos.py \
  scripts/perception_fault_readonly.py \
  scripts/perception_fault_acceptance.py \
  tests/m1-perception/test_perception_chaos_contract.py \
  tests/m1-perception/test_perception_fault_acceptance.py \
  tests/m1-perception/test_upstream_fault_e2e.py Makefile
git commit -m "feat(m1): qualify typed upstream fault lifecycles"
make planning-status
```

Expected: commit succeeds; no DRIFT.

---

## Task 8: Runbook, learning document, final verification, and summary

**Files:**

- Modify: `docs/dev/perception-fault-runbook.md`
- Create: `docs/learning/42-生产故障控制边界.md`
- Modify: `docs/learning/00-INDEX.md`
- Modify:
  `docs/superpowers/plans/2026-07-30-scoped-upstream-fault-control-SUMMARY.md`

**Step 1: Document exact operator sequence**

The runbook must show:

- read-only runtime/status inspection;
- how to create a minimal intent file for each call class;
- double-signature authority and secret handling;
- one-fault-at-a-time arm;
- automatic and manual cleanup;
- cleanup-failed freeze/escalation;
- exact evidence directory and verifier commands;
- explicit warning that local tests and synthetic evidence do not qualify a
  cloud release;
- explicit requirement for separate user authorization per exact release and
  fault mutation;
- no wallet/signing/balance/order/trade interaction.

Do not include live secrets or a copy-paste command that defaults to mutation.

**Step 2: Add the teaching document**

Follow the existing learning-doc format:

- 30-second mental model;
- key code paths with `file:line`;
- why control-plane fail-open and evidence-plane fail-closed are both needed;
- why isolated producers require claim-at-boundary;
- why Candidate uses group ID and Telegram uses outbox ID;
- cleanup ordering;
- design tradeoffs;
- 3–5 adversarial self-check questions;
- empty `FAQ 增量` section.

Update `docs/learning/00-INDEX.md`.

**Step 3: Run the final local and read-only gates**

```bash
make chaos-l2-fly-image-check
make test-m1-perception
make planning-status
git diff --check
git status --short
```

Then run only the GET/read-only cloud gate:

```bash
test -n "$DEPLOYED_RELEASE"
test -n "$NEW_EVIDENCE_DIR"
make qualify-perception-prod-readonly \
  expected_release="$DEPLOYED_RELEASE" \
  output_dir="$NEW_EVIDENCE_DIR"
```

The read-only gate may report current production faults; record the real
verdict without weakening it. Do not deploy, enable the feature flag, set
secrets, arm a fault, or run production mutation in this task.

**Step 4: Finalize SUMMARY and commit**

Update SUMMARY to include:

- implementation commits;
- exact tests and outputs;
- schema/call-class/config decisions;
- read-only cloud verdict;
- remaining production authorization boundary;
- next exact command.

```bash
git add docs/dev/perception-fault-runbook.md \
  docs/learning/42-生产故障控制边界.md \
  docs/learning/00-INDEX.md \
  docs/superpowers/plans/2026-07-30-scoped-upstream-fault-control-SUMMARY.md
git commit -m "docs(m1): document upstream fault qualification"
make planning-status
```

Expected: all local gates PASS, cloud read-only result recorded faithfully, no
DRIFT, and no production mutation performed.

---

## Final execution acceptance

Implementation is complete locally only when:

- all eight upstream adapters are typed and execute-supported;
- normal mode is proven pass-through;
- isolated and non-isolated producer paths share the same runtime protocol;
- replay, TTL, exact runtime, exact target, one-active, and single-use rules
  pass;
- cleanup-before-receipt and restart-abandonment pass;
- Incident and business recovery facts are chain-truth linked;
- independent evaluator fails every missing/tampered-evidence case;
- all executable operations have documented Make targets;
- learning/runbook/SUMMARY artifacts exist;
- `make test-m1-perception`, `make planning-status`, and `git diff --check`
  pass.

Production qualification is a later, separately authorized operation. It
requires an exact deployed release, feature/secret configuration, one explicit
fault authorization at a time, and a new 24-hour continuous evidence window
after the matrix. No local result may be relabeled as production PASS.
