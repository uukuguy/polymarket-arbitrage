# L3 Continuity Boundary Repair Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate partial L3 market-rotation publication and force bounded
WebSocket recovery when quiet-book evidence cannot remain below 120 seconds.

**Architecture:** `WsConsumer` gains a generation-scoped prepared-target
transaction: collect durable book evidence without publishing new L3 truth,
then send make-before-break controls and publish desired, committed, evidenced,
and mapping-facing membership at one local commit boundary. Quiet-refresh
timeouts become persisted generation failures and close only their captured
socket so the normal reconnect path obtains a complete initial dump.

**Tech Stack:** Python 3.12, asyncio, websockets, asyncpg, Pydantic settings,
pytest/pytest-asyncio, Ruff, uv, Fly.io, Supabase/PostgreSQL.

## Global Constraints

- Keep `POLYARB_L3_MARKET_BOOK_FRESH_S=120` and the Phase 05.4 acceptance
  configuration unchanged.
- Never mark outbound subscription control as data freshness; only successful
  durable `l2_book_levels` writes count as evidence.
- Evidence is scoped to an exact WebSocket generation and exact target token
  set.
- A failed transition leaves the last fully converged mapping published unless
  the socket becomes ambiguous; ambiguity closes only the captured socket.
- Runtime events contain operation, reason, generation, required count, and
  missing count, but no token IDs, credentials, DSNs, or raw exception text.
- Deploy only `polyarb-l2`; do not restart L1 or reset the active quote anchor.
- Use `uv`, not `pip`.
- Every production-code change follows RED → GREEN → REFACTOR.
- Every plan-scoped code commit gets the required Phase 05 plan SUMMARY before
  the next plan begins; never bypass `.githooks/pre-commit`.

---

## File Structure

- `src/polyarb/observation/l3_evidence.py`
  - Owns immutable `PreparedL3Target`, the cross-module value passed from
    evidence staging to atomic commit, plus the shared transition lock.
- `src/polyarb/daemon/ws_consumer.py`
  - Owns generation-scoped evidence barriers, quiet-refresh timeout recovery,
    and the atomic L3 target commit.
- `src/polyarb/observation/l3_promote.py`
  - Selects the target, calls prepare/commit, mirrors only committed truth, and
    refuses success without exact 10/10 evidence.
- `src/polyarb/observation/l3_sampler.py`
  - Samples under the shared transition lock so it sees either the old complete
    mapping or the new complete mapping, never the in-flight boundary.
- `tests/daemon/test_ws_quiet_refresh.py`
  - Covers timeout event/recovery behavior.
- `tests/daemon/test_ws_resubscribe_transaction.py`
  - Covers captured-generation isolation and make-before-break controls.
- `tests/m1-perception/test_l3_promoter.py`
  - Covers promoter staging, atomic publication, failure retention, and ledger
    truth.
- `tests/chaos/test_l3_evidence_chain.py`
  - Covers the full receive/write → evidence → durable sample → strict health
    chain.
- `docs/learning/13-L3-连续性事务.md`
  - Explains the operator/developer mental model and production incident.
- `docs/learning/00-INDEX.md`
  - Adds the new learning document to reading order.
- `docs/M1-市场感知平台使用手册.md`
  - Adds alert interpretation and recovery commands.
- `.planning/workstreams/m1-perception/phases/05-ws-book-prices/05-07-PLAN.md`
  - Registers this bounded repair under Phase 05.
- `.planning/workstreams/m1-perception/phases/05-ws-book-prices/05-07-SUMMARY.md`
  - Records RED/GREEN evidence, production release, and new L3 evidence anchor.
- `.planning/workstreams/m1-perception/phases/05-ws-book-prices/05-SOAK-LOG.md`
  - Records the two incidents and the repaired-release T0/T+24 evidence.
- `.planning/JOURNAL.md`
  - Records the repair and next exact checkpoint.

---

### Task 1: Register the repair and reproduce quiet-refresh timeout recovery

**Files:**
- Create:
  `.planning/workstreams/m1-perception/phases/05-ws-book-prices/05-07-PLAN.md`
- Modify: `tests/daemon/test_ws_quiet_refresh.py`
- Modify: `tests/daemon/test_ws_resubscribe_transaction.py`
- Modify: `src/polyarb/daemon/ws_consumer.py`

**Interfaces:**
- Consumes:
  `WsConsumer.request_book_refresh(required_asset_ids: frozenset[str] | None)
  -> bool`
- Produces:
  the same public return type, but an evidence timeout now persists
  `subscription_control_failed` and compensates its captured generation.

- [ ] **Step 1: Create the Phase 05 plan registration**

Create `05-07-PLAN.md` with frontmatter `phase: 05`, `plan: 07`, `type: tdd`,
`depends_on: [01, 02, 03, 04, 05]`, and the following truths:

```yaml
must_haves:
  truths:
    - "A promoter never publishes or reports success with fewer than 10 target-token book evidence identities."
    - "A quiet-refresh timeout persists failure truth and closes only its captured WebSocket generation."
    - "Strict L3 freshness remains below 120 seconds without grace periods or threshold relaxation."
    - "L1 machine identity and the active quote evidence anchor survive the L2-only deployment."
```

Reference the approved design and this implementation plan verbatim.
Create `05-07-SUMMARY.md` from the repository summary template in the same
step, mark the plan `in progress`, and record the production incidents as the
reason for the repair. This ensures the pre-commit SUMMARY guard is present
before the first plan-scoped code commit.

- [ ] **Step 2: Write failing timeout-compensation tests**

Allow the existing fixture to capture bounded runtime events:

```python
def _make_consumer(
    *,
    event_recorder: Callable[..., object] | None = None,
) -> tuple[WsConsumer, WsWatchdog, MagicMock]:
    # Preserve the existing fixture body and pass event_recorder to WsConsumer.
```

Replace the old expectation that an evidence timeout keeps the socket alive
with:

```python
@pytest.mark.asyncio
async def test_evidence_timeout_records_failure_and_compensates_captured_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[tuple[RuntimeEventKind, dict[str, object]]] = []
    consumer, _watchdog, ws = _make_consumer(
        event_recorder=lambda kind, **kwargs: events.append((kind, kwargs))
    )
    consumer.set_l3_desired(["l3-a", "l3-b"])
    consumer._l3_committed_set = {"l3-a", "l3-b"}
    monkeypatch.setattr(ws_consumer_module, "_BOOK_EVIDENCE_RETRY_AFTER_S", 0.005)
    monkeypatch.setattr(ws_consumer_module, "_BOOK_EVIDENCE_TIMEOUT_S", 0.01)

    task = asyncio.create_task(consumer.request_book_refresh())
    await _wait_until(lambda: ws.send.await_count == 4)
    consumer.record_book_evidence(
        asset_id="l3-a",
        generation=consumer._connection_generation,
        book_levels_succeeded=True,
        observed_at=datetime(2026, 7, 26, tzinfo=UTC),
    )

    assert await asyncio.wait_for(task, timeout=0.2) is False
    ws.close.assert_awaited_once()
    assert consumer._current_ws is None
    _kind, event = next(
        (kind, values)
        for kind, values in events
        if kind is RuntimeEventKind.SUBSCRIPTION_CONTROL_FAILED
        and values["reason_code"] == "evidence_timeout"
    )
    assert event["detail"]["operation"] == "book_refresh"
    assert event["detail"]["required_count"] == 2
    assert event["detail"]["missing_count"] == 1
    assert "l3-b" not in str(event["detail"])
```

Reuse and retain
`test_quiet_refresh_evidence_timeout_closes_only_captured_socket`, which already
replaces `_current_ws` and increments the generation before timeout; it must
continue to prove that the old socket closes and the replacement does not.

- [ ] **Step 3: Run the focused tests and verify RED**

Run:

```bash
uv run pytest -q \
  tests/daemon/test_ws_quiet_refresh.py::test_evidence_timeout_records_failure_and_compensates_captured_generation \
  tests/daemon/test_ws_resubscribe_transaction.py -k "timeout and captured"
```

Expected: FAIL because timeout currently returns `False` without a runtime event
or socket compensation.

- [ ] **Step 4: Implement timeout failure truth and compensation**

In the `request_book_refresh` exception path, replace the special
`evidence_timeout` return with bounded event emission and captured-generation
compensation:

```python
if failure_reason == "evidence_timeout":
    self._record_runtime_event(
        RuntimeEventKind.SUBSCRIPTION_CONTROL_FAILED,
        reason_code="evidence_timeout",
        detail={
            "operation": "book_refresh",
            "error_type": "EvidenceTimeout",
            "required_count": len(required_assets),
            "missing_count": len(missing_assets),
        },
        severity=RuntimeEventSeverity.WARNING,
        generation=generation,
    )
if ws is not None:
    await self._compensate_generation(ws, generation)
return False
```

Retain the existing `finally` waiter cleanup. Do not update evidence timestamps
or close a replacement socket.

- [ ] **Step 5: Run focused and neighboring tests and verify GREEN**

Run:

```bash
uv run pytest -q \
  tests/daemon/test_ws_quiet_refresh.py \
  tests/daemon/test_ws_resubscribe_transaction.py \
  tests/daemon/test_ws_book_evidence_chain.py
uv run ruff check src/polyarb/daemon/ws_consumer.py \
  tests/daemon/test_ws_quiet_refresh.py \
  tests/daemon/test_ws_resubscribe_transaction.py
```

Expected: all tests pass and Ruff reports no errors.

- [ ] **Step 6: Commit Task 1**

Update `05-07-SUMMARY.md` with Task 1 RED/GREEN commands and results, then:

```bash
git add \
  .planning/workstreams/m1-perception/phases/05-ws-book-prices/05-07-PLAN.md \
  .planning/workstreams/m1-perception/phases/05-ws-book-prices/05-07-SUMMARY.md \
  src/polyarb/daemon/ws_consumer.py \
  tests/daemon/test_ws_quiet_refresh.py \
  tests/daemon/test_ws_resubscribe_transaction.py
git commit -m "fix(05-07): recover timed-out L3 book refresh"
make planning-status
```

---

### Task 2: Add generation-scoped prepared L3 target transactions

**Files:**
- Modify: `src/polyarb/observation/l3_evidence.py`
- Modify: `src/polyarb/daemon/ws_consumer.py`
- Modify: `src/polyarb/observation/l3_sampler.py`
- Modify: `tests/daemon/test_ws_resubscribe_transaction.py`
- Modify: `tests/daemon/test_ws_book_evidence_chain.py`
- Modify: `tests/m1-perception/test_l3_evidence_sampler.py`

**Interfaces:**
- Produces:
  `PreparedL3Target(generation: int, asset_ids: frozenset[str],
  evidenced_at: Mapping[str, datetime])`
- Produces:
  `await WsConsumer.prepare_l3_target(asset_ids: frozenset[str])
  -> PreparedL3Target | None`
- Produces:
  `await WsConsumer.commit_l3_target(prepared: PreparedL3Target) -> bool`
- Produces:
  `L3EvidenceRuntime.transition_lock: asyncio.Lock`
- Consumes: successful durable book writes from `record_book_evidence`.

- [ ] **Step 1: Write failing prepared-target tests**

Add tests that express the intended public behavior:

```python
@pytest.mark.asyncio
async def test_prepare_target_collects_durable_evidence_without_publishing_membership() -> None:
    consumer, ws = _consumer()
    consumer.set_l3_desired(["old-a", "old-b"])
    consumer._l3_committed_set = {"old-a", "old-b"}
    before = consumer.l3_membership_snapshot()

    task = asyncio.create_task(
        consumer.prepare_l3_target(frozenset({"new-a", "new-b"}))
    )
    await _wait_until(lambda: ws.send.await_count == 2)
    for asset_id in ("new-a", "new-b"):
        consumer.record_book_evidence(
            asset_id=asset_id,
            generation=before.generation,
            book_levels_succeeded=True,
            observed_at=datetime(2026, 7, 26, tzinfo=UTC),
        )
    prepared = await asyncio.wait_for(task, timeout=0.2)

    assert prepared is not None
    assert prepared.asset_ids == frozenset({"new-a", "new-b"})
    assert set(prepared.evidenced_at) == set(prepared.asset_ids)
    assert consumer.l3_membership_snapshot() == before
```

Add tests proving:

- a generation change returns `None`;
- a failed durable write never satisfies the barrier;
- `commit_l3_target` sends subscribe-before-unsubscribe;
- membership publishes once with `desired == committed == evidenced == target`;
- stale prepared evidence cannot commit;
- a failed second control compensates the captured generation instead of
  publishing partial truth.
- `sample_once` waits while `runtime.transition_lock` is held and reads the
  mapping only after the lock is released.

- [ ] **Step 2: Run the prepared-target tests and verify RED**

Run:

```bash
uv run pytest -q \
  tests/daemon/test_ws_resubscribe_transaction.py \
  tests/daemon/test_ws_book_evidence_chain.py -k "prepare or commit_l3_target"
```

Expected: FAIL because `PreparedL3Target`, `prepare_l3_target`, and
`commit_l3_target` do not exist.

- [ ] **Step 3: Add the immutable prepared-target value**

In `l3_evidence.py`, add:

```python
@dataclass(frozen=True)
class PreparedL3Target:
    generation: int
    asset_ids: frozenset[str]
    evidenced_at: Mapping[str, datetime]

    def __post_init__(self) -> None:
        if self.generation < 0:
            raise ValueError("generation must be non-negative")
        assets = frozenset(self.asset_ids)
        evidence = _frozen_mapping(dict(self.evidenced_at))
        if not assets or set(evidence) != set(assets):
            raise ValueError("prepared evidence must exactly match target assets")
        for asset_id, observed_at in evidence.items():
            _require_nonempty("prepared asset ID", asset_id)
            _require_utc(f"prepared evidence[{asset_id!r}]", observed_at)
        object.__setattr__(self, "asset_ids", assets)
        object.__setattr__(self, "evidenced_at", evidence)
```

Initialize `self._transition_lock = asyncio.Lock()` in `L3EvidenceRuntime` and
expose it without replacement:

```python
@property
def transition_lock(self) -> asyncio.Lock:
    return self._transition_lock
```

Extend `_BookEvidenceWaiter` with
`observed_at: dict[str, datetime] = field(default_factory=dict)`.
`record_book_evidence` stores successful waiter evidence before discarding the
asset from `missing`.

- [ ] **Step 4: Implement preparation without publication**

Extract the evidence-returning core of `request_book_refresh` so the existing
boolean API remains compatible:

```python
async def prepare_l3_target(
    self,
    asset_ids: frozenset[str],
) -> PreparedL3Target | None:
    result = await self._request_book_evidence(required_asset_ids=asset_ids)
    if result is None:
        return None
    generation, evidenced_at = result
    if (
        generation != self._connection_generation
        or set(evidenced_at) != set(asset_ids)
    ):
        return None
    return PreparedL3Target(
        generation=generation,
        asset_ids=asset_ids,
        evidenced_at=evidenced_at,
    )
```

The extracted core has the exact signature:

```python
async def _request_book_evidence(
    self,
    *,
    required_asset_ids: frozenset[str] | None = None,
    operation: Literal["book_refresh", "promotion_stage"],
) -> tuple[int, Mapping[str, datetime]] | None:
```

`request_book_refresh` calls the same core with `operation="book_refresh"` and
returns `result is not None`. `prepare_l3_target` passes
`operation="promotion_stage"`. Timeout runtime events use this exact operation,
so durable evidence distinguishes routine freshness recovery from promoter
staging.
Neither path mutates `_l3_desired_set`, `_l3_committed_set`, or
`_l3_business_evidence` for non-committed target assets.

- [ ] **Step 5: Implement atomic make-before-break commit**

Add:

```python
async def commit_l3_target(self, prepared: PreparedL3Target) -> bool:
    ws: Any = None
    generation = prepared.generation
    try:
        async with self._subscription_control_lock:
            ws = self._current_ws
            if ws is None or generation != self._connection_generation:
                return False
            current = frozenset(self._l3_committed_set)
            added = sorted(prepared.asset_ids - current)
            removed = sorted(current - prepared.asset_ids)
            if added and not await self._send_control(
                ws,
                {"operation": "subscribe", "assets_ids": added, "initial_dump": True},
            ):
                raise RuntimeError("target subscribe failed")
            if removed and not await self._send_control(
                ws,
                {"operation": "unsubscribe", "assets_ids": removed},
            ):
                raise RuntimeError("target unsubscribe failed")
            if ws is not self._current_ws or generation != self._connection_generation:
                raise RuntimeError("target generation changed")
            self._l3_desired_set = set(prepared.asset_ids)
            self._l3_committed_set = set(prepared.asset_ids)
            self._l3_business_evidence = {
                asset_id: (generation, observed_at)
                for asset_id, observed_at in prepared.evidenced_at.items()
            }
            self._publish_l3_membership_locked()
            return True
    except asyncio.CancelledError:
        if ws is not None:
            await self._compensate_generation(ws, generation)
        raise
    except Exception:
        if ws is not None:
            await self._compensate_generation(ws, generation)
        return False
```

Do not call `set_l3_desired`, `add_subscriptions`, or `remove_subscriptions`
inside this transaction because those publish intermediate state.

- [ ] **Step 6: Serialize sampling across the commit boundary**

Wrap sample collection and atomic sample append in the runtime gate:

```python
async with runtime.transition_lock:
    batch = await collect_sample(
        scheduled_at=scheduled_at,
        sample_seq=sample_seq,
        settings=settings,
        ws_consumer=ws_consumer,
        reconciliation_state=reconciliation_state,
        runtime=runtime,
        store=store,
    )
    persisted = await store.append_sample(batch)
```

The sampler still enforces its existing 30-second slot and 75-second maximum
gap. The lock is an atomicity boundary, not a grace period or skipped failure.

- [ ] **Step 7: Run the target transaction tests and verify GREEN**

Run:

```bash
uv run pytest -q \
  tests/daemon/test_ws_resubscribe_transaction.py \
  tests/daemon/test_ws_book_evidence_chain.py \
  tests/daemon/test_ws_quiet_refresh.py \
  tests/m1-perception/test_l3_evidence_sampler.py
uv run ruff check \
  src/polyarb/observation/l3_evidence.py \
  src/polyarb/daemon/ws_consumer.py \
  src/polyarb/observation/l3_sampler.py \
  tests/daemon/test_ws_resubscribe_transaction.py \
  tests/daemon/test_ws_book_evidence_chain.py \
  tests/m1-perception/test_l3_evidence_sampler.py
```

Expected: all pass.

- [ ] **Step 8: Update SUMMARY and commit Task 2**

Append Task 2 RED/GREEN commands, interface decisions, and changed files to
`05-07-SUMMARY.md`, then:

```bash
git add \
  .planning/workstreams/m1-perception/phases/05-ws-book-prices/05-07-SUMMARY.md \
  src/polyarb/observation/l3_evidence.py \
  src/polyarb/daemon/ws_consumer.py \
  src/polyarb/observation/l3_sampler.py \
  tests/daemon/test_ws_resubscribe_transaction.py \
  tests/daemon/test_ws_book_evidence_chain.py \
  tests/m1-perception/test_l3_evidence_sampler.py
git commit -m "feat(05-07): stage atomic L3 membership targets"
make planning-status
```

---

### Task 3: Make promoter success require prepared 10/10 evidence

**Files:**
- Modify: `src/polyarb/observation/l3_promote.py`
- Modify: `tests/m1-perception/test_l3_promoter.py`
- Modify: `tests/chaos/test_l3_evidence_chain.py`

**Interfaces:**
- Consumes:
  `await ws_consumer.prepare_l3_target(desired) -> PreparedL3Target | None`
- Consumes:
  `await ws_consumer.commit_l3_target(prepared) -> bool`
- Produces:
  promoter terminal rows where `status=success` implies exact desired,
  committed, evidenced, mapping, and count convergence.

- [ ] **Step 1: Write the failing promoter rotation regression**

Add a test reproducing production run 450:

```python
@pytest.mark.asyncio
async def test_rotation_never_reports_success_or_publishes_partial_evidence() -> None:
    ws = _WsConsumerWithExistingFiveMarketMapping()
    ws.prepare_l3_target = AsyncMock(return_value=None)
    before = ws.l3_membership_snapshot()

    result = await _run_promote_with_one_market_rotation(ws)

    assert result.status is PromoteStatus.FAILED
    assert result.reason_code == "target_evidence_failed"
    assert result.evidenced_count == 10
    assert ws.l3_membership_snapshot() == before
    ws.commit_l3_target.assert_not_awaited()
```

Add a success test returning a `PreparedL3Target` with all 10 identities and
assert:

- `prepare_l3_target` is called before `commit_l3_target`;
- `commit_l3_target` receives that exact value;
- the terminal row has all three counts at 10 and `status=success`;
- the mapping mirror contains exactly the committed five markets.

Add a chaos-chain assertion that no durable health sample can observe a new
mapping hash with 8/10 or 9/10 evidenced identities.

- [ ] **Step 2: Run the promoter tests and verify RED**

Run:

```bash
uv run pytest -q \
  tests/m1-perception/test_l3_promoter.py -k "partial_evidence or prepared_target" \
  tests/chaos/test_l3_evidence_chain.py -k "promotion and atomic"
```

Expected: FAIL because the promoter currently calls `set_l3_desired`, removes
old tokens, adds new tokens, and can report success before evidence converges.

- [ ] **Step 3: Replace partial mutation with prepare/commit**

In `promote_run`, retain selection, underfill, dry-run, mirror, and durable
terminalization, but replace:

```python
ws_consumer.set_l3_desired(desired)
await ws_consumer.remove_subscriptions(...)
await ws_consumer.add_subscriptions(...)
```

with:

```python
prepared = await ws_consumer.prepare_l3_target(desired)
if prepared is None:
    return await finish(
        early(
            PromoteStatus.FAILED,
            "target_evidence_failed",
            desired=initial.desired,
            committed=initial.committed,
            evidenced=initial.evidenced,
            mapping=prior_mapping,
        )
    )
```

Adapt this outline to the existing `_PromoteTerminalDraft` helper rather than
introducing a second terminalization path. Preserve `added`, `removed`,
`added_markets`, and `removed_markets` as proposed/committed transition facts.

- [ ] **Step 4: Commit mapping and membership under the sampler gate**

Keep the evidence preparation outside the gate. Then enter the same lock used
by `sample_once`, reconcile the target mirror, commit the prepared WebSocket
target, build the exact terminal row, and await `finish(...)` before releasing
the lock:

```python
async with evidence_runtime.transition_lock:
    mirror_result = await asyncio.to_thread(
        _mirror_l3_promoted_at_ts,
        client,
        sorted(target_market_ids),
    )
    if not mirror_result.succeeded or mirror_result.cleanup_pending:
        return await finish(
            early(PromoteStatus.FAILED, "mirror_failed")
        )
    if not await ws_consumer.commit_l3_target(prepared):
        return await finish(
            early(PromoteStatus.FAILED, "target_commit_failed")
        )
    current = ws_consumer.l3_membership_snapshot()
    staged_market_token_map = committed_target_map
    staged_active_set = current.committed
    result = await finish(exact_terminal_draft)
    if not result.persisted:
        ws_consumer.set_l3_desired(initial.desired)
        await ws_consumer.compensate_current_generation(
            reason_code="promote_append_failed"
        )
    return result
```

Expose a bounded public
`await WsConsumer.compensate_current_generation(reason_code: str) -> None`
wrapper that snapshots the current socket/generation and delegates to the
existing `_compensate_generation`. It must not accept a socket or generation
from callers. Add a focused test proving it closes only the snapshot it
captured.

The durable terminal append and module mapping publication already happen in
`finish(...)`; holding `transition_lock` makes them indivisible from the
sampler's point of view. If the append fails after WebSocket commit, restore
the prior desired intent and compensate the ambiguous generation before
releasing the gate.

- [ ] **Step 5: Strengthen the success invariant**

Before assigning `PromoteStatus.SUCCESS`, require:

```python
exact_target = (
    current.desired == desired
    and current.committed == desired
    and current.evidenced == desired
    and len(desired) == 10
)
if not exact_target:
    status, reason = PromoteStatus.FAILED, "target_convergence_failed"
```

Mirror only `current.committed`; publish the new cached market-token mapping
only after mirror success and durable terminal-row success. On any failure,
retain the prior fully converged mapping cache.

- [ ] **Step 6: Run focused and chaos tests and verify GREEN**

Run:

```bash
uv run pytest -q \
  tests/m1-perception/test_l3_promoter.py \
  tests/chaos/test_l3_evidence_chain.py \
  tests/http/test_l2_health.py
uv run ruff check \
  src/polyarb/observation/l3_promote.py \
  tests/m1-perception/test_l3_promoter.py \
  tests/chaos/test_l3_evidence_chain.py
```

Expected: all pass; no success row can contain `evidenced_count < 10`.

- [ ] **Step 7: Update SUMMARY and commit Task 3**

Append Task 3 RED/GREEN/chaos commands and the mapping/membership atomicity
decision to `05-07-SUMMARY.md`, then:

```bash
git add \
  .planning/workstreams/m1-perception/phases/05-ws-book-prices/05-07-SUMMARY.md \
  src/polyarb/observation/l3_promote.py \
  src/polyarb/daemon/ws_consumer.py \
  tests/m1-perception/test_l3_promoter.py \
  tests/chaos/test_l3_evidence_chain.py \
  tests/daemon/test_ws_resubscribe_transaction.py
git commit -m "fix(05-07): publish only converged L3 rotations"
make planning-status
```

---

### Task 4: Verify the full chain and document operations

**Files:**
- Create: `docs/learning/13-L3-连续性事务.md`
- Modify: `docs/learning/00-INDEX.md`
- Modify: `docs/M1-市场感知平台使用手册.md`
- Modify:
  `.planning/workstreams/m1-perception/phases/05-ws-book-prices/05-SOAK-LOG.md`
- Modify: `.planning/JOURNAL.md`
- Create:
  `.planning/workstreams/m1-perception/phases/05-ws-book-prices/05-07-SUMMARY.md`

**Interfaces:**
- Consumes: repaired strict `/health`, durable evidence tables, resident
  Polywatch, and Fly release identity.
- Produces: operator commands and auditable plan completion.

- [ ] **Step 1: Run complete local verification**

Run:

```bash
uv run pytest -q
uv run ruff check .
make planning-status
make check-m1-manual
```

Expected: full pytest and Ruff pass, the manual contract passes, and planning
status reports no new Plan 07 drift.

- [ ] **Step 2: Add the learning document**

Write `docs/learning/13-L3-连续性事务.md` with:

- 30-second mental model: “prepare evidence → atomic commit → strict sample”;
- the two 2026-07-26 production incidents;
- code anchors for `prepare_l3_target`, `commit_l3_target`, promoter exact-target
  invariant, and timeout compensation;
- why grace periods and threshold relaxation were rejected;
- three adversarial self-check questions;
- an empty `FAQ 增量` section.

Add it after document 12 in `docs/learning/00-INDEX.md`.

- [ ] **Step 3: Update the M1 manual**

Add these exact operator actions:

```text
make smoke-l2-health-strict-prod
make polywatch-resident-status
make fly-l2-logs
make l3-evidence-status
```

Explain:

- `membership_convergence fail` means identities/evidence disagree, not merely
  that the socket is quiet;
- `worst_market_freshness fail` means at least one persisted market input
  crossed 120 seconds;
- Telegram sends the first failure and one recovery;
- operators inspect first and do not restart production blindly.

- [ ] **Step 4: Complete the existing Plan 07 SUMMARY before documentation commit**

Update `05-07-SUMMARY.md`. Include:

- production evidence rows 4500/4501 and the 06:12 freshness incident;
- RED commands and expected failures;
- GREEN commands and passing counts;
- commits from Tasks 1-3;
- files changed and design decisions;
- production deployment/evidence fields initially marked `pending deployment`,
  not falsely completed.

- [ ] **Step 5: Commit local verification and documentation**

```bash
git add \
  docs/learning/13-L3-连续性事务.md \
  docs/learning/00-INDEX.md \
  docs/M1-市场感知平台使用手册.md \
  .planning/workstreams/m1-perception/phases/05-ws-book-prices/05-SOAK-LOG.md \
  .planning/workstreams/m1-perception/phases/05-ws-book-prices/05-07-SUMMARY.md \
  .planning/JOURNAL.md
git commit -m "docs(05-07): record L3 continuity repair"
```

---

### Task 5: Deploy L2 only and open repaired evidence

**Files:**
- Modify:
  `.planning/workstreams/m1-perception/phases/05-ws-book-prices/05-SOAK-LOG.md`
- Modify:
  `.planning/workstreams/m1-perception/phases/05-ws-book-prices/05-07-SUMMARY.md`
- Modify: `.planning/JOURNAL.md`

**Interfaces:**
- Consumes: exact tested commit SHA and built Fly image.
- Produces: repaired `polyarb-l2` release plus immutable L3 T0/T+24 anchors.

- [ ] **Step 1: Capture pre-deploy identities**

Record without secrets:

```bash
flyctl machine list -a polyarb-l1
flyctl machine list -a polyarb-l2
curl -fsS https://polyarb-l1.fly.dev/healthz
curl -fsS https://polyarb-l2.fly.dev/health
```

Persist L1 machine ID, image/release ID, start time, and current quote-run
anchor. These values must remain unchanged.

- [ ] **Step 2: Build and deploy only L2**

Push the tested branch state, require CI, then deploy:

```bash
flyctl deploy --config fly-l2.toml --remote-only --wait-timeout 600 \
  --env POLYARB_RELEASE_ID="<tested-commit-sha>"
```

Do not run `flyctl deploy` with `fly.toml`, set L1 secrets, or restart L1.

- [ ] **Step 3: Verify production health and unchanged L1**

Run:

```bash
make smoke-l2-health-strict-prod
make polywatch-resident-status
flyctl machine list -a polyarb-l1
flyctl machine list -a polyarb-l2
```

Require:

- L2 release ID equals the tested commit;
- `l3:active_count=10`;
- `l3:membership_convergence=pass` with 5/5 rows;
- `l3:worst_market_freshness=pass`;
- L1 identity and quote anchor match Step 1;
- resident monitor reports no active alert.

- [ ] **Step 4: Observe real boundaries**

Wait for at least one five-minute promoter run and one 60-second quiet-refresh
boundary. Query the durable rows read-only:

```sql
SELECT run_seq, status, desired_count, committed_count, evidenced_count,
       add_count, remove_count, mapping_hash
FROM public.l3_promote_runs
WHERE recorded_at >= :release_started_at
ORDER BY recorded_at;

SELECT sample_seq, sampled_at, status, reason_code,
       desired_count, committed_count, evidenced_count
FROM public.l3_health_samples
WHERE sampled_at >= :release_started_at
ORDER BY sampled_at;
```

Require every successful changed-mapping run to have 10/10/10 and every
post-release health sample to pass.

- [ ] **Step 5: Create immutable repaired-release T0**

Use the existing Makefile evidence commands with a unique artifact path:

```bash
make l3-soak-manifest \
  start="<future-UTC-T0>" \
  end="<T0-plus-24h>" \
  output=".planning/evidence/l3-05-07-<release>-manifest.json"
```

Bind the exact first sample using the existing evidence workflow. Record T0,
T+24, release ID, machine ID, image digest, code version, and acceptance config
hash in the soak log.

- [ ] **Step 6: Finalize production documentation commit**

Replace `pending deployment` fields in `05-07-SUMMARY.md` with exact evidence,
append the JOURNAL session, and commit:

```bash
git add \
  .planning/workstreams/m1-perception/phases/05-ws-book-prices/05-SOAK-LOG.md \
  .planning/workstreams/m1-perception/phases/05-ws-book-prices/05-07-SUMMARY.md \
  .planning/JOURNAL.md
git commit -m "docs(05-07): open repaired L3 evidence window"
```

- [ ] **Step 7: Complete T+24 verdict**

At the exact T+24 boundary, run the existing strict verdict workflow. Require:

- exact window duration at least 24 hours;
- no failed health sample;
- no sample gap at or above 75 seconds;
- every changed-mapping success row at 10/10/10;
- every market row below 120 seconds;
- unchanged release/machine/image/config identity;
- no unresolved Polywatch alert.

Update the SUMMARY, SOAK-LOG, ROADMAP/STATE, and JOURNAL only after the verdict
passes. If it fails, record the first failing sample immediately; do not wait
another day before diagnosis.
