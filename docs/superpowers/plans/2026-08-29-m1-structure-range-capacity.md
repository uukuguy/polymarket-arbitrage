# M1 Structure Range Capacity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Drain every 1,115-range Structure generation inside the 15-minute freshness contract without overlapping unfinished generations or weakening lifecycle deadlines.

**Architecture:** A 12-lane pool reuses the existing independently fenced `TransactionalStructureWorker`; it adds no pool attempt or timeout. Source admission uses one unfinished range as its backpressure threshold, so queue depth cannot hide an incomplete prior generation.

**Tech Stack:** Python 3.12, asyncio, Pydantic Settings, PostgreSQL durable jobs, pytest, Ruff, Pyright, Fly Machines.

## Global Constraints

- Do not change the 15-minute Structure freshness contract, runtime attempt/lease policy, qualification window, artifact format or publication pointer semantics.
- Every lane has a distinct worker ID and its own PostgreSQL lease/checkpoint.
- One lane failure is local; the pool drains siblings before returning or raising.
- `SIGTERM/40s` remains the only process-level backstop.
- Production rollout starts with the exact Structure range Machine canary.

---

### Task 1: Add one Structure range capacity authority and fenced pool

**Files:**

- Modify: `src/polyarb/config.py`
- Modify: `src/polyarb/control_plane/structure_worker.py`
- Modify: `src/polyarb/cli_control_plane.py`
- Modify: `tests/m1-perception/test_transactional_structure_worker.py`
- Modify: `tests/m1-perception/test_control_plane_cli.py`
- Modify: `tests/m1-perception/test_control_plane_deployment_templates.py`

**Interfaces:**

- Produces: `Settings.structure_range_max_concurrency: int`, default 12, bounds 1..32.
- Produces: `TransactionalStructureRangePool(lanes: Sequence[_StructureRangeLane])` with `_lease_seconds` and `async run_once() -> StructureWorkerResult`.
- Changes: `_transactional_structure_worker(..., lane_count: int = 1) -> TransactionalStructureWorker | TransactionalStructureRangePool`.

- [x] **Step 1: Write the failing pool behavior tests**

```python
async def test_structure_range_pool_runs_independent_lanes_concurrently():
    release = asyncio.Event()
    entered = 0
    # Twelve fake lanes increment entered and wait for release. Assert all
    # entered before release, IDs are distinct, and aggregate is succeeded:12/12.

def test_structure_range_pool_rejects_mixed_lease_policies():
    with pytest.raises(ValueError, match="share one positive lease"):
        TransactionalStructureRangePool(lanes=(Lane(120), Lane(30)))
```

Add CLI construction assertions that `Settings().structure_range_max_concurrency`
lanes are created with IDs ending `:0` through `:11`, and deployment config
continues to expose only one `structure_range` process group.

- [x] **Step 2: Run RED**

Run:

```bash
uv run pytest -q \
  tests/m1-perception/test_transactional_structure_worker.py \
  tests/m1-perception/test_control_plane_cli.py \
  tests/m1-perception/test_control_plane_deployment_templates.py --maxfail=5
```

Expected: imports/construction fail because the Settings field and pool do not exist.

- [x] **Step 3: Implement the minimal pool and CLI wiring**

```python
structure_range_max_concurrency: int = Field(default=12, ge=1, le=32)

class TransactionalStructureRangePool:
    def __init__(self, *, lanes: Sequence[_StructureRangeLane]) -> None:
        # require non-empty lanes and one positive shared lease

    async def run_once(self) -> StructureWorkerResult:
        results = await asyncio.gather(
            *(lane.run_once() for lane in self._lanes),
            return_exceptions=True,
        )
        # drain all siblings, raise first error, otherwise aggregate exact keys
```

Build lanes over one existing object client/control-plane instance. Do not add
an executor, semaphore, heartbeat, timeout or retry at pool level.

- [x] **Step 4: Run GREEN and static checks**

Run the RED command, then:

```bash
uv run ruff check src/polyarb/config.py src/polyarb/control_plane/structure_worker.py \
  src/polyarb/cli_control_plane.py tests/m1-perception/test_transactional_structure_worker.py \
  tests/m1-perception/test_control_plane_cli.py
uv run pyright src/polyarb/config.py src/polyarb/control_plane/structure_worker.py \
  src/polyarb/cli_control_plane.py
```

Expected: all pass with zero Ruff/Pyright findings.

### Task 2: Make one unfinished range the admission backpressure authority

**Files:**

- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `src/polyarb/control_plane/structure_source.py`
- Modify: `src/polyarb/cli_control_plane.py`
- Modify: `tests/m1-perception/test_control_plane_postgres.py`
- Modify: `tests/m1-perception/test_transactional_structure_source_worker.py`
- Modify: `tests/m1-perception/test_control_plane_cli.py`

**Interfaces:**

- Changes default `structure_high_water` from 2,000 to 1 at the PostgreSQL,
  admitter and CLI scheduler construction boundaries.
- `SourceAdmissionDecision(state="backpressured:structure", job_key=None)` is
  returned whenever at least one Structure range is unfinished.

- [x] **Step 1: Write the failing admission tests**

```python
def test_default_admission_blocks_on_one_unfinished_structure_range(control_plane):
    # Seed one runnable structure-normalize job.
    decision = control_plane.admit_due_structure_source_window(
        cadence_seconds=300,
        now=NOW,
    )
    assert decision.state == "backpressured:structure"
    assert decision.job_key is None
```

Assert the CLI scheduler forwards `structure_high_water=1` and the Quote
high-water remains 512.

- [x] **Step 2: Run RED**

Run:

```bash
uv run pytest -q \
  tests/m1-perception/test_control_plane_postgres.py::test_default_admission_blocks_on_one_unfinished_structure_range \
  tests/m1-perception/test_transactional_structure_source_worker.py \
  tests/m1-perception/test_control_plane_cli.py --maxfail=5
```

Expected: the one range is admitted under the old 2,000-job default.

- [x] **Step 3: Change only the shared defaults and callers**

Replace the three default boundaries with `structure_high_water: int = 1` and
update CLI assertions/help. Keep explicit test overrides working and do not
change `quote_high_water`.

- [x] **Step 4: Run GREEN**

Run the RED command plus the complete source, scheduler and PostgreSQL
admission groups. Expected: all pass; explicit high-water tests retain their
existing custom values.

### Task 3: Verify exact release locally and in production

**Files:**

- Modify: `docs/dev/m1-runtime-boundary-inventory.md`
- Modify: `docs/learning/95-超时任务序列与可恢复性审计.md`
- Modify: `.planning/threads/market-observation-architecture.md`
- Modify: `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/05.6-212-SUMMARY.md`
- Modify: `.planning/workstreams/m1-perception/STATE.md`

**Interfaces:**

- Records the measured one-lane baseline, exact image digest, canary drain
  rate, maximum concurrent leases, memory floor and publication timestamps.

- [ ] **Step 1: Run focused and full local gates**

Run affected worker/config/CLI/deployment suites, Ruff, Pyright,
`make planning-status`, `make climb-check`, then `make test-m1` with no outer
timeout. Record exact counts and duration.

- [ ] **Step 2: Build and verify the exact linux/amd64 image**

Require nonroot UID 10001, exact `org.opencontainers.image.revision`, revision
035 and the unchanged eight-job DAG before deployment.

- [ ] **Step 3: Canary only the Structure range Machine**

Use the preserved-config Machines API renderer/verifier. Require
`SIGTERM/40s`, unchanged Machine ID/region/resources, 12 simultaneous distinct
leased range jobs, and no new circuit/open incident.

- [ ] **Step 4: Accept or roll back from durable evidence**

Accept only if the existing backlog drain projects below 15 minutes per full
generation and memory remains safe. Otherwise restore the previous immutable
image and retain all lease/checkpoint facts; do not adjust freshness.

- [ ] **Step 5: Roll sibling runtime roles and resume qualification**

After canary acceptance, roll exact bytes through the standard verifier,
observe publication of the current generation and confirm the next source
window remains backpressured until unfinished range count reaches zero. The
86,400-second exact-release qualification certificate remains the final M1
completion gate.

## Self-review

- Spec coverage: lane capacity, unique fencing, failure isolation,
  cancellation, single-generation admission, production canary and rollback
  are each assigned to one task.
- Placeholder scan: no TBD/TODO or unnamed command remains.
- Type consistency: pool/result/Settings names match the design and current
  worker interfaces.
