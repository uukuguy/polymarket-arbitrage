# M1 Scoped Structure Source Throughput Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fetch independent exact-ID Structure market batches concurrently without weakening PostgreSQL lease, R2 receipt, or terminal materialization fences.

**Architecture:** Add a bounded pool of eight independently named source workers. The pool is one scheduler stage and aggregates its lane results; event pagination stays serial because it has only one runnable job, while market batches are claimed independently through existing PostgreSQL leases.

**Tech Stack:** Python 3.12, asyncio, psycopg/PostgreSQL, Gamma, R2.

## Global Constraints

- Keep 25 IDs/request and the 10,000-batch source cap.
- Use distinct lane worker IDs; no SQLite or shared in-memory cursor.
- A lane failure cannot cancel sibling lane work.
- Do not change Telegram, production L1/L2, Fly machine count, or pointers.

---

### Task 1: Implement bounded lane-pool behavior

**Files:**
- Modify: `src/polyarb/control_plane/structure_source.py:353-546`
- Test: `tests/m1-perception/test_transactional_structure_source_worker.py`

**Interfaces:**
- Produces: `TransactionalStructureSourcePool(lanes: Sequence[_Worker])`
- Produces: `async run_once() -> StructureWorkerResult`

- [ ] **Step 1: Write RED tests**

```python
async def test_source_pool_runs_lanes_concurrently() -> None:
    pool = TransactionalStructureSourcePool(
        lanes=(DelayedLane("market:2"), DelayedLane("market:1"), DelayedLane(None))
    )
    assert await pool.run_once() == StructureWorkerResult(
        job_key="market:1,market:2", outcome="succeeded:2/3"
    )

async def test_source_pool_waits_for_sibling_before_propagating_failure() -> None:
    healthy = DelayedLane("market:1")
    with pytest.raises(TimeoutError):
        await TransactionalStructureSourcePool(lanes=(FailingLane(), healthy)).run_once()
    assert healthy.calls == 1
```

- [ ] **Step 2: Verify RED**

Run `uv run pytest tests/m1-perception/test_transactional_structure_source_worker.py -k source_pool -q`.

Expected: import failure for `TransactionalStructureSourcePool`.

- [ ] **Step 3: Implement minimal pool**

```python
class TransactionalStructureSourcePool:
    def __init__(self, *, lanes: Sequence[_Worker]) -> None:
        if not lanes:
            raise ValueError("lanes must be non-empty")
        self._lanes = tuple(lanes)

    async def run_once(self) -> StructureWorkerResult:
        results = await asyncio.gather(*(lane.run_once() for lane in self._lanes),
                                       return_exceptions=True)
        errors = [item for item in results if isinstance(item, BaseException)]
        if errors:
            raise errors[0]
        completed = [item for item in results if item.job_key is not None]
        keys = sorted(str(item.job_key) for item in completed)
        return StructureWorkerResult(
            job_key=None if not keys else ",".join(keys),
            outcome="idle" if not keys else f"succeeded:{len(completed)}/{len(self._lanes)}",
        )
```

Provide `aclose()` with `asyncio.gather()` over every lane closer. Do not alter
the single-lane fetcher: its existing lease and R2 receipt behavior is the
fencing implementation.

- [ ] **Step 4: Verify GREEN and commit**

Run `uv run pytest tests/m1-perception/test_transactional_structure_source_worker.py -q && uv run ruff check src/polyarb/control_plane/structure_source.py tests/m1-perception/test_transactional_structure_source_worker.py`.

Commit: `git commit -m "feat(m1): run scoped source batches in bounded lanes"`.

### Task 2: Wire eight lane identities into the worker service

**Files:**
- Modify: `src/polyarb/cli_control_plane.py:240-325`
- Test: `tests/m1-perception/test_control_plane_cli.py`
- Test: `tests/m1-perception/test_transactional_control_plane_scheduler.py`

**Interfaces:**
- Produces: `_transactional_structure_source_worker(..., lane_count: int = 8) -> TransactionalStructureSourcePool`
- Scheduler continues to expose one `structure-source` aggregate turn.

- [ ] **Step 1: Write RED identity test**

```python
def test_transactional_scheduler_assigns_eight_distinct_source_lane_ids(monkeypatch) -> None:
    captured: list[str] = []
    monkeypatch.setattr(module, "TransactionalStructureSourceWorker", capture_worker(captured))
    module._transactional_scheduler(control_plane, worker_id="worker", max_turns=8)
    assert captured == [f"worker:structure-source:{ordinal}" for ordinal in range(8)]
```

- [ ] **Step 2: Verify RED**

Run `uv run pytest tests/m1-perception/test_control_plane_cli.py -k eight_distinct_source_lane_ids -q`.

Expected: failure because the factory creates one source worker.

- [ ] **Step 3: Implement pool construction**

```python
return TransactionalStructureSourcePool(
    lanes=tuple(
        TransactionalStructureSourceWorker(
            control_plane=control_plane, gamma=GammaClient(), object_client=object_client,
            bucket=bucket, worker_id=f"{worker_id}:{ordinal}", now=lambda: datetime.now(UTC)
        )
        for ordinal in range(lane_count)
    )
)
```

Resolve `object_client, bucket = _structure_object_client()` once before the
tuple. Validate `lane_count > 0`. Each lane owns its own Gamma client.

- [ ] **Step 4: Verify GREEN and commit**

Run `uv run pytest tests/m1-perception/test_control_plane_cli.py tests/m1-perception/test_transactional_control_plane_scheduler.py -q`.

Commit: `git commit -m "feat(m1): wire bounded source lanes into worker service"`.

### Task 3: Prove parallel completion and stage it

**Files:**
- Modify: `tests/m1-perception/test_control_plane_postgres.py`
- Modify: `.planning/JOURNAL.md`
- Modify: `.planning/workstreams/m1-perception/STATE.md`

- [ ] **Step 1: Add durable parallel-lease contract**

```python
def test_parallel_scoped_batch_leases_release_materializer_only_after_last_receipt(
    control_plane: PostgresControlPlane,
) -> None:
    # Admit three scoped batches; claim each with a distinct lane owner.
    # Record two terminal receipts and assert no materializer lease exists.
    # Record the third receipt and assert the materializer is then claimable.
```

- [ ] **Step 2: Verify contract and regression suite**

Run `uv run pytest tests/m1-perception/test_transactional_structure_source_worker.py tests/m1-perception/test_transactional_control_plane_scheduler.py tests/m1-perception/test_control_plane_postgres.py -q`.

Expected: all pass, including three distinct source leases and an after-last-only materializer.

- [ ] **Step 3: Build and update staging only**

```bash
NO_COLOR=1 flyctl deploy --app polyarb-control-worker-staging --build-only --push --image-label m1-source-lanes-<short-sha>
flyctl machine update 48e3104c979578 --app polyarb-control-worker-staging \
  --image registry.fly.io/polyarb-control-worker-staging:m1-source-lanes-<short-sha> \
  --restart always --yes
```

Read Postgres state to prove multiple scoped market leases/receipts and zero
publication pointers, then record observed evidence in JOURNAL and STATE.

- [ ] **Step 4: Commit and verify planning state**

Commit: `git commit -m "test(m1): prove parallel scoped source lease completion"`.

Run `make planning-status`; expected final line is `✓ no drift detected`.
