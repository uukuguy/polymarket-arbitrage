# L3 Quiet Refresh Non-Destructive Retry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use test-driven-development to implement this plan task-by-task. Execute inline because this production repair shares one stateful file and project instructions do not authorize parallel subagents.

**Goal:** Prevent intermittent missing quiet-refresh books from destroying a truthful live WebSocket generation while preserving strict same-generation depth-write evidence.

**Architecture:** Add generation-scoped connection-grace and missing-retry state to `WsConsumer`. Keep genuine control ambiguity on the existing compensation path, but treat a final business-evidence timeout after successful ordered control sends as a non-destructive retry condition; retry only missing token identities through a second bounded control cycle.

**Tech Stack:** Python 3.12, asyncio, websockets, pytest, unittest.mock, Loguru

## Global Constraints

- Do not change `AcceptanceConfig` cadence or freshness thresholds.
- Do not advance evidence from a control send; only a successful `book` depth write may call `record_book_evidence`.
- Preserve cancellation propagation and control-failure compensation.
- Never modify, overwrite, or extend A1–A6 checkpoint artifacts.
- Exclude trading, H-009, retention cleanup, and production chaos.

---

### Task 1: Generation-Scoped Initial Convergence Grace

**Files:**
- Modify: `src/polyarb/daemon/ws_consumer.py`
- Test: `tests/daemon/test_ws_quiet_refresh.py`
- Test: `tests/m1-perception/test_ws_consumer_dynamic_subscribe.py`

**Interfaces:**
- Consumes: `WsConsumer._initialize_connection(ws)`, `_release_connection(ws)`, `refresh_if_quiet(now_s, quiet_after_s, retry_s)`
- Produces: `_connection_initialized_at_s: float | None`, `_last_quiet_refresh_missing_generation: int | None`, generation-scoped state cleanup

- [ ] **Step 1: Write failing grace and cleanup tests**

Add a test that initializes a real consumer, forces
`_connection_initialized_at_s = BASE_S`, leaves one committed token without
current-generation evidence, and asserts:

```python
assert await consumer.refresh_if_quiet(now_s=BASE_S + 59) is None
assert ws.send.await_count == 1  # only the initial connection subscription
```

At `BASE_S + 60`, start the refresh, satisfy its waiter with real
`record_book_evidence`, and assert the refresh succeeds. Add a lifecycle test
that seeds:

```python
consumer._last_quiet_refresh_missing_assets = frozenset({"l3-a"})
consumer._last_quiet_refresh_missing_generation = consumer._connection_generation
```

then calls `_release_connection(ws)` and asserts both retry fields and
`_connection_initialized_at_s` are cleared.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
uv run pytest tests/daemon/test_ws_quiet_refresh.py \
  tests/m1-perception/test_ws_consumer_dynamic_subscribe.py -q \
  -k 'initial_connection_grace or release_clears_quiet_refresh_retry'
```

Expected: FAIL because the timestamp/generation state does not exist and the
quiet loop currently refreshes incomplete membership immediately.

- [ ] **Step 3: Implement generation-scoped state**

In `WsConsumer.__init__`, add:

```python
self._connection_initialized_at_s: float | None = None
self._last_quiet_refresh_missing_generation: int | None = None
```

In `_clear_l3_connection_state_locked`, clear the connection timestamp,
missing set, and missing generation. After `_initialize_connection` publishes
`self._current_ws = ws`, set:

```python
self._connection_initialized_at_s = time.time()
```

In `refresh_if_quiet`, compute missing current-generation committed identities.
When they exist and the published connection age is below `quiet_after_s`,
return `None`. Do not use global frame age for this L3 gate.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run the Step 2 command. Expected: both new tests PASS.

- [ ] **Step 5: Commit Task 1**

```bash
git add src/polyarb/daemon/ws_consumer.py \
  tests/daemon/test_ws_quiet_refresh.py \
  tests/m1-perception/test_ws_consumer_dynamic_subscribe.py
git commit -m "fix(l2): give websocket evidence initial convergence grace"
```

---

### Task 2: Two-Stage Missing-Only Evidence Retry

**Files:**
- Modify: `src/polyarb/daemon/ws_consumer.py`
- Test: `tests/daemon/test_ws_quiet_refresh.py`
- Test: `tests/daemon/test_ws_book_evidence_chain.py`
- Test: `tests/daemon/test_ws_resubscribe_transaction.py`

**Interfaces:**
- Consumes: `_BookEvidenceWaiter`, `_subscription_control_lock`, `_send_control`, `_compensate_generation`, `record_book_evidence`
- Produces: `_BOOK_EVIDENCE_RETRY_AFTER_S: float`, non-destructive evidence timeout, missing-only second control cycle

- [ ] **Step 1: Change the timeout contract test to RED**

Update `test_evidence_timeout_keeps_missing_identities_in_state_not_logs` so a
successful unsubscribe/final-subscribe followed only by missing evidence must:

```python
assert result is False
ws.close.assert_not_awaited()
assert consumer._current_ws is ws
assert consumer.last_quiet_refresh_missing_assets == frozenset({"l3-b"})
assert consumer._last_quiet_refresh_missing_generation == 0
```

Keep the assertions that no freshness is forged and the missing identity never
appears in logs.

- [ ] **Step 2: Run the timeout test and verify RED**

Run:

```bash
uv run pytest tests/daemon/test_ws_quiet_refresh.py::test_evidence_timeout_keeps_missing_identities_in_state_not_logs -q
```

Expected: FAIL because current code closes and compensates the generation.

- [ ] **Step 3: Add a RED missing-only second-stage test**

Use two required tokens. On the first subscribe callback, record successful
depth evidence only for `l3-a`; on the second subscribe callback, assert its
payload contains only `l3-b`, then record evidence for `l3-b`. Monkeypatch:

```python
monkeypatch.setattr(ws_consumer_module, "_BOOK_EVIDENCE_RETRY_AFTER_S", 0.01)
monkeypatch.setattr(ws_consumer_module, "_BOOK_EVIDENCE_TIMEOUT_S", 0.05)
```

Assert the four ordered payloads are:

```python
[
    {"operation": "unsubscribe", "assets_ids": ["l3-a", "l3-b"]},
    {
        "operation": "subscribe",
        "assets_ids": ["l3-a", "l3-b"],
        "initial_dump": True,
    },
    {"operation": "unsubscribe", "assets_ids": ["l3-b"]},
    {
        "operation": "subscribe",
        "assets_ids": ["l3-b"],
        "initial_dump": True,
    },
]
```

Expected result is `True`, no close, empty missing state.

- [ ] **Step 4: Run both tests and verify RED**

Run:

```bash
uv run pytest tests/daemon/test_ws_quiet_refresh.py -q \
  -k 'evidence_timeout_keeps_missing or retries_only_missing'
```

Expected: the timeout test fails on close semantics and the second-stage test
fails because only one control cycle exists.

- [ ] **Step 5: Implement the two-stage barrier**

Add:

```python
_BOOK_EVIDENCE_RETRY_AFTER_S: float = 8.0
```

Require `0 < _BOOK_EVIDENCE_RETRY_AFTER_S < _BOOK_EVIDENCE_TIMEOUT_S`.
After the first successful final subscribe, wait for the first interval with
the waiter shielded. On timeout, snapshot `waiter.missing`, re-enter the
control lock, re-check socket/generation identity, and send unsubscribe then
subscribe for only the missing identities. Wait for the remaining total
barrier.

Track whether the final control intent is a successful same-generation
subscribe. In the outer exception handler:

```python
if failure_reason == "evidence_timeout" and final_subscribe_confirmed:
    self._last_quiet_refresh_missing_assets = missing_assets
    self._last_quiet_refresh_missing_generation = generation
    return False
```

Do not call `_compensate_generation` in that branch. All other failures and
cancellation retain compensation. On success, clear both missing fields.

In `record_book_evidence`, remove organically refreshed identities from
generation-matching retry state. In `refresh_if_quiet`, prefer that exact
missing subset on the next due attempt.

- [ ] **Step 6: Run focused quiet-refresh tests and verify GREEN**

Run:

```bash
uv run pytest tests/daemon/test_ws_quiet_refresh.py \
  tests/daemon/test_ws_book_evidence_chain.py \
  tests/daemon/test_ws_resubscribe_transaction.py -q
```

Expected: all PASS, including existing send-failure, generation-change,
timeout, waiter-cleanup, and cancellation contracts.

- [ ] **Step 7: Run the full L2/L3 focused regression**

Run:

```bash
uv run pytest tests/daemon \
  tests/m1-perception/test_ws_consumer_dynamic_subscribe.py \
  tests/m1-perception/test_l3_promoter.py \
  tests/m1-perception/test_l3_evidence_sampler.py \
  tests/clients/test_ws_market_client.py -q
```

Expected: all PASS.

- [ ] **Step 8: Commit Task 2**

```bash
git add src/polyarb/daemon/ws_consumer.py \
  tests/daemon/test_ws_quiet_refresh.py \
  tests/daemon/test_ws_book_evidence_chain.py \
  tests/daemon/test_ws_resubscribe_transaction.py
git commit -m "fix(l2): retry missing quiet books without reconnect churn"
```

---

### Task 3: Qualification, Learning, and Exact Deployment Candidate

**Files:**
- Modify: `docs/learning/22-L3连续浸泡证据.md`
- Modify: `.planning/threads/market-observation-architecture.md`
- Modify: `.planning/workstreams/m1-perception/phases/05.4-continuous-l3-soak-evidence/05.4-SOAK-LOG.md`
- Modify: `.planning/workstreams/m1-perception/STATE.md`
- Modify: `.planning/CURRENT.md`
- Modify: `.planning/JOURNAL.md`

**Interfaces:**
- Consumes: committed Task 1–2 behavior and test evidence
- Produces: clean pushed exact SHA eligible for a new production deployment

- [ ] **Step 1: Run complete local qualification**

```bash
uv run ruff check src/polyarb/daemon/ws_consumer.py \
  tests/daemon/test_ws_quiet_refresh.py \
  tests/m1-perception/test_ws_consumer_dynamic_subscribe.py
uv run python -m compileall -q src scripts tests
uv run pytest -q
make docs-m1-check
make planning-status
make chaos-l2-fly-image-check
git diff --check
```

Expected: changed-file lint/compile/test/docs/planning/image gates PASS. Record
repository-wide unrelated Ruff debt separately if full-repository Ruff remains
non-zero; do not bulk-rewrite unrelated files.

- [ ] **Step 2: Update durable learning and execution state**

Document:

- A6 sample 35 and its immutable rejection;
- the difference between control ambiguity and missing business evidence;
- why evidence timeout cannot advance freshness;
- why destructive compensation creates a feedback loop;
- the exact new tests and qualification output;
- no trading/H-009/retention/chaos boundary.

- [ ] **Step 3: Commit qualification documents**

```bash
git add docs/learning/22-L3连续浸泡证据.md \
  .planning/threads/market-observation-architecture.md \
  .planning/workstreams/m1-perception/phases/05.4-continuous-l3-soak-evidence/05.4-SOAK-LOG.md \
  .planning/workstreams/m1-perception/STATE.md \
  .planning/CURRENT.md .planning/JOURNAL.md
git commit -m "docs(05.4): qualify quiet refresh repair candidate"
```

- [ ] **Step 4: Push one clean exact SHA**

```bash
git push origin main
test "$(git rev-parse HEAD)" = "$(git rev-parse origin/main)"
test -z "$(git status --porcelain=v1)"
```

Expected: local `HEAD` equals `origin/main`, with an empty worktree. Re-run the
production project/ref/revision/runtime/retention gates before deployment.

