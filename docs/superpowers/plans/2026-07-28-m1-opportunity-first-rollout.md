# M1 Opportunity-First Perception Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the universe-snapshot-gated M1 watcher with group-certified candidate tracking, bounded rolling discovery, checkpointed reconciliation, and durable incident recovery that can be qualified in production.

**Architecture:** One SQLite/WAL read model holds immutable group revisions, atomic all-leg Quote batches, scheduling state, reconciliation checkpoints, and incident history. Candidate Watcher is the hot path; Discovery and Full Reconciliation are independently bounded producers whose work yields when hot-path freshness degrades. CPU/GIL-heavy producers run outside the HTTP event loop, while Starlette and the Dashboard expose only durable facts.

**Tech Stack:** Python 3.12, asyncio, SQLite/WAL, Starlette, existing Gamma/CLOB clients, pytest/pytest-asyncio, Loguru, Fly.io, Next.js 15/React 19/TypeScript, Make.

## Global Constraints

- M1 remains observer-only: no wallet, signing, balance, order placement, or real-money execution.
- The online correctness boundary is one complete neg-risk group, not one universe snapshot.
- A Quote batch is usable only when every expected leg shares one `membership_hash` and `quote_batch_id`.
- Universe discovery is rolling and statistical; no code or UI may claim zero-miss discovery.
- Candidate Watcher freshness has priority over Discovery and Full Reconciliation progress.
- An incomplete reconciliation window never replaces certified online groups wholesale.
- Every executable operator command must have a documented `make <verb>-<noun>` target.
- All new behavior is implemented test-first; every task ends with focused tests, proportional regression, a commit, a task summary, `make planning-status`, and an independent review gate.
- Existing user-owned changes in `findings.md`, `progress.md`, and `task_plan.md` are preserved.
- The deployed adaptive Structure timer remains evidence for background reconciliation only; it is not a global opportunity-readiness gate.

## Rollout Order and Gates

| Slice | Deliverable | Production gate before next slice |
|---|---|---|
| A | Group revision and atomic Quote-batch authority | Migration on a copied production DB; old read path unchanged |
| B | Candidate Watcher hot path | Repeated group-level collections; stale/identity failures fail closed |
| C | Bounded Discovery and promotion | Rolling coverage advances without degrading Candidate Quote SLA |
| D | Checkpointed Full Reconciliation | Kill/restart resumes and closes a window without replacing hot state |
| E | Durable incident lifecycle and producer isolation | Fault matrix proves detect → contain → recover/escalate |
| F | API, Dashboard, operator workflow, production qualification | 24h post-fault continuous evidence and all acceptance gates pass |

---

### Task 1: Group Revision and Quote-Batch Authority

**Files:**
- Create: `src/polyarb/perception/__init__.py`
- Create: `src/polyarb/perception/models.py`
- Create: `src/polyarb/perception/store.py`
- Modify: `src/polyarb/storage/schemas.py`
- Modify: `src/polyarb/storage/sqlite_store.py`
- Test: `tests/perception/test_models.py`
- Test: `tests/perception/test_store.py`

**Interfaces:**
- Produces: `GroupRevision`, `GroupLeg`, `GroupQuoteBatch`, `GroupQuoteLeg`, `OpportunityPerceptionStore`.
- Produces: `OpportunityPerceptionStore.publish_group_revision(revision) -> GroupRevision`.
- Produces: `OpportunityPerceptionStore.publish_quote_batch(batch) -> GroupQuoteBatch`.
- Produces: `OpportunityPerceptionStore.current_group(group_id) -> GroupRevision | None`.
- Produces: `OpportunityPerceptionStore.current_quote_batch(group_id, now_ms, max_age_ms) -> GroupQuoteBatch | None`.
- Consumes: existing SQLite database path and WAL/busy-timeout conventions.

- [ ] **Step 1: Write model contract tests**

```python
def test_group_revision_hash_covers_ordered_complete_leg_identity() -> None:
    legs = (
        GroupLeg("m-1", "c-1", "yes-1", "First"),
        GroupLeg("m-2", "c-2", "yes-2", "Second"),
    )
    revision = GroupRevision.certified(
        group_id="g-1",
        event_id="e-1",
        revision=7,
        started_at_ms=1_000,
        observed_at_ms=2_000,
        source_cursor="cursor-2",
        legs=legs,
    )
    assert revision.status == "certified"
    assert revision.membership_hash == GroupRevision.membership_digest(legs)


def test_quote_batch_rejects_a_leg_from_another_membership() -> None:
    with pytest.raises(ValueError, match="membership-hash-mismatch"):
        GroupQuoteBatch.complete(
            group_id="g-1",
            membership_hash="hash-a",
            quote_batch_id="qb-1",
            started_at_ms=3_000,
            quoted_at_ms=3_100,
            legs=(
                GroupQuoteLeg("yes-1", "hash-b", 0.40, 10.0, "executable"),
                GroupQuoteLeg("yes-2", "hash-a", 0.50, 12.0, "executable"),
            ),
        )
```

- [ ] **Step 2: Run model tests and verify RED**

Run:

```bash
uv run pytest tests/perception/test_models.py -q
```

Expected: collection/import failure because `polyarb.perception.models` does not exist.

- [ ] **Step 3: Implement immutable model types**

Implement in `src/polyarb/perception/models.py`:

```python
@dataclass(frozen=True)
class GroupLeg:
    market_id: str
    condition_id: str
    yes_token_id: str
    title: str


@dataclass(frozen=True)
class GroupRevision:
    group_id: str
    event_id: str
    revision: int
    membership_hash: str
    started_at_ms: int
    observed_at_ms: int
    source_cursor: str
    status: Literal["discovered", "certified", "stale", "invalidated", "closed"]
    legs: tuple[GroupLeg, ...]

    @classmethod
    def certified(
        cls,
        *,
        group_id: str,
        event_id: str,
        revision: int,
        started_at_ms: int,
        observed_at_ms: int,
        source_cursor: str,
        legs: Sequence[GroupLeg],
    ) -> GroupRevision:
        normalized_legs = tuple(legs)
        if len(normalized_legs) < 2:
            raise ValueError("incomplete-group-membership")
        return cls(
            group_id=group_id,
            event_id=event_id,
            revision=revision,
            membership_hash=cls.membership_digest(normalized_legs),
            started_at_ms=started_at_ms,
            observed_at_ms=observed_at_ms,
            source_cursor=source_cursor,
            status="certified",
            legs=normalized_legs,
        )

    @staticmethod
    def membership_digest(legs: Sequence[GroupLeg]) -> str:
        identity = [
            [leg.market_id, leg.condition_id, leg.yes_token_id, leg.title]
            for leg in legs
        ]
        encoded = json.dumps(identity, ensure_ascii=False, separators=(",", ":"))
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()
```

Implement `GroupQuoteLeg` and `GroupQuoteBatch.complete()` with exact group/hash,
positive finite ask/size, all-leg uniqueness, and timestamp-order validation.

- [ ] **Step 4: Write additive schema/store RED tests**

```python
def test_membership_change_invalidates_previous_quote_atomically(tmp_path: Path) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    first = revision(group_id="g-1", revision=1, token_suffix="a")
    store.publish_group_revision(first)
    store.publish_quote_batch(batch_for(first, quote_batch_id="qb-1"))

    changed = revision(group_id="g-1", revision=2, token_suffix="b")
    store.publish_group_revision(changed)

    assert store.current_group("g-1") == changed
    assert store.current_quote_batch("g-1", now_ms=10_000, max_age_ms=60_000) is None
```

- [ ] **Step 5: Run store tests and verify RED**

Run:

```bash
uv run pytest tests/perception/test_store.py -q
```

Expected: failure because the perception tables/store do not exist.

- [ ] **Step 6: Add schema and transactional store**

Add append-only tables to `src/polyarb/storage/schemas.py`:

```sql
CREATE TABLE IF NOT EXISTS neg_risk_group_revisions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  group_id TEXT NOT NULL,
  event_id TEXT NOT NULL,
  revision INTEGER NOT NULL,
  membership_hash TEXT NOT NULL,
  started_at_ms INTEGER NOT NULL,
  observed_at_ms INTEGER NOT NULL,
  source_cursor TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN
    ('discovered','certified','stale','invalidated','closed')),
  legs_json TEXT NOT NULL,
  UNIQUE(group_id, revision)
);

CREATE TABLE IF NOT EXISTS neg_risk_group_quote_batches (
  id TEXT PRIMARY KEY,
  group_id TEXT NOT NULL,
  group_revision INTEGER NOT NULL,
  membership_hash TEXT NOT NULL,
  started_at_ms INTEGER NOT NULL,
  quoted_at_ms INTEGER NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('complete','failed','superseded')),
  failure_reason TEXT,
  legs_json TEXT NOT NULL
);
```

`publish_group_revision()` uses `BEGIN IMMEDIATE`; when the hash changes it
inserts the new revision and marks prior complete batches `superseded` in the
same transaction. `publish_quote_batch()` re-reads the current certified group
inside the write transaction and rejects an identity mismatch.

- [ ] **Step 7: Verify Slice A**

Run:

```bash
uv run pytest tests/perception/test_models.py tests/perception/test_store.py \
  tests/routing/test_opportunity_ledger.py tests/routing/test_neg_risk_quote_store.py -q
uv run ruff check src/polyarb/perception tests/perception
make planning-status
```

Expected: all tests pass, Ruff passes, no planning drift.

- [ ] **Step 8: Commit, summarize, and review**

```bash
git add src/polyarb/perception src/polyarb/storage/schemas.py \
  src/polyarb/storage/sqlite_store.py tests/perception
git commit -m "feat(m1): add group perception authority"
```

Create `docs/superpowers/plans/2026-07-28-m1-opportunity-first-rollout-TASK-1-SUMMARY.md`,
run `make planning-status`, and obtain an independent schema/transaction review
before Task 2.

---

### Task 2: Group-Certified Candidate Watcher

**Files:**
- Create: `src/polyarb/perception/group_structure.py`
- Create: `src/polyarb/perception/candidate_watcher.py`
- Modify: `src/polyarb/routing/focused_quote_collector.py`
- Modify: `src/polyarb/daemon/opportunity_watcher.py`
- Modify: `src/polyarb/daemon/main.py`
- Modify: `src/polyarb/config.py`
- Test: `tests/perception/test_group_structure.py`
- Test: `tests/perception/test_candidate_watcher.py`
- Test: `tests/m1-perception/test_l1_quote_worker_wiring.py`

**Interfaces:**
- Consumes: Task 1 `OpportunityPerceptionStore`.
- Produces: `GroupStructureReader.read_group(group_id) -> GroupRevision`.
- Produces: `CandidateWatcher.run_once(group_id) -> CandidateObservation`.
- Produces: `CandidateWatcherRuntime.snapshot() -> CandidateWatcherSnapshot`.
- Produces: durable scheduling fields `next_due_at_ms`, `priority_class`, and `last_result`.

- [ ] **Step 1: Write RED tests for independent group certification**

```python
@pytest.mark.asyncio
async def test_candidate_watcher_publishes_only_one_complete_group_batch() -> None:
    structure = certified_group("g-1", tokens=("yes-1", "yes-2"))
    reader = FakeBooksReader({"yes-1": ask(0.40, 10), "yes-2": ask(0.50, 8)})
    watcher = candidate_watcher(structure=structure, reader=reader)

    observation = await watcher.run_once("g-1")

    assert observation.status == "watching"
    assert observation.bundle_cost == 0.90
    assert observation.gross_edge_bps == 1_000
    assert observation.max_bundle_size == 8
    assert reader.requests == [("yes-1", "yes-2")]


@pytest.mark.asyncio
async def test_membership_change_during_quote_fails_closed() -> None:
    watcher = candidate_watcher(
        structure=(certified_group("g-1", revision=1), certified_group("g-1", revision=2)),
        reader=FakeBooksReader.complete(),
    )
    result = await watcher.run_once("g-1")
    assert result.status == "unavailable"
    assert result.reason == "structure-membership-changed"
```

- [ ] **Step 2: Run focused tests and verify RED**

Run:

```bash
uv run pytest tests/perception/test_group_structure.py \
  tests/perception/test_candidate_watcher.py -q
```

Expected: import failure for the new group reader/watcher.

- [ ] **Step 3: Implement group reader and watcher**

`CandidateWatcher.run_once()` follows this exact sequence:

```python
before = await self._structure_reader.read_group(group_id)
books = await self._books_reader.get_books(
    [leg.yes_token_id for leg in before.legs],
    projection="top",
)
after = await self._structure_reader.read_group(group_id)
if after.membership_hash != before.membership_hash:
    return await self._record_unavailable(before, "structure-membership-changed")
batch = build_complete_group_quote_batch(before, books, clock_ms=self._clock_ms)
self._store.publish_quote_batch(batch)
return await self._ledger.reconcile_group(before, batch)
```

Do not call the all-known-token `collect_quotes_in_subprocess()` from this path.
The watcher records one terminal fact per run and schedules the next due time
from priority class.

- [ ] **Step 4: Add scheduler priority tests**

```python
def test_priority_policy_preserves_quote_freshness_and_anti_starvation() -> None:
    assert next_interval_s(priority="high", consecutive_failures=0) == 15
    assert next_interval_s(priority="normal", consecutive_failures=0) == 60
    assert next_interval_s(priority="explore", consecutive_failures=0) == 300
    assert next_interval_s(priority="high", consecutive_failures=3) <= 90
```

Intervals are initial controller inputs, not hidden constants: persist the
effective interval and reason with every scheduling transition.

- [ ] **Step 5: Wire production without removing the legacy read path**

Add settings:

```python
opportunity_first_watcher_enabled: bool = False
candidate_high_interval_s: float = Field(default=15, gt=0)
candidate_normal_interval_s: float = Field(default=60, gt=0)
candidate_quote_hard_stale_s: float = Field(default=90, gt=0)
```

When disabled, current production behavior remains unchanged. When enabled,
`main.py` starts `CandidateWatcher` as a sibling supervised task and injects its
runtime into the HTTP app.

- [ ] **Step 6: Verify Slice B**

Run:

```bash
uv run pytest tests/perception/test_group_structure.py \
  tests/perception/test_candidate_watcher.py \
  tests/routing/test_focused_quote_collector.py \
  tests/daemon/test_opportunity_watcher.py \
  tests/m1-perception/test_l1_quote_worker_wiring.py -q
uv run ruff check src/polyarb/perception src/polyarb/daemon tests/perception
```

Expected: all pass; old all-universe Quote tests remain green.

- [ ] **Step 7: Commit, summarize, and review**

```bash
git add src/polyarb/perception src/polyarb/routing/focused_quote_collector.py \
  src/polyarb/daemon/opportunity_watcher.py src/polyarb/daemon/main.py \
  src/polyarb/config.py tests/perception \
  tests/m1-perception/test_l1_quote_worker_wiring.py
git commit -m "feat(m1): watch certified groups independently"
```

Write Task 2 summary and `docs/learning/31-opportunity-first-group-watch.md`;
update `docs/learning/00-INDEX.md` and the M1 manual in the same change.

---

### Task 3: Bounded Discovery and Promotion

**Files:**
- Create: `src/polyarb/perception/discovery.py`
- Create: `src/polyarb/perception/priority.py`
- Modify: `src/polyarb/clients/gamma_client.py`
- Modify: `src/polyarb/perception/store.py`
- Modify: `src/polyarb/config.py`
- Modify: `src/polyarb/daemon/main.py`
- Modify: `Makefile`
- Test: `tests/perception/test_priority.py`
- Test: `tests/perception/test_discovery.py`
- Test: `tests/clients/test_gamma_discovery_page.py`

**Interfaces:**
- Produces: `GammaClient.fetch_active_event_page(cursor, limit) -> EventPage`.
- Produces: `DiscoveryWorker.run_batch() -> DiscoveryBatchResult`.
- Produces: `priority_score(GroupScheduleInput, *, now_ms: int) -> Decimal`.
- Produces: `OpportunityPerceptionStore.coverage_windows(now_ms) -> CoverageWindows`.
- Make target: `make perception-discovery-status`.

- [ ] **Step 1: Write page/cursor RED tests**

```python
@pytest.mark.asyncio
async def test_fetch_event_page_returns_durable_next_cursor(respx_mock) -> None:
    respx_mock.get("https://gamma-api.polymarket.com/events/keyset").mock(
        return_value=httpx.Response(
            200,
            json={"data": [{"id": "e-1", "markets": []}], "next_cursor": "c-2"},
        )
    )
    page = await gamma.fetch_active_event_page(cursor="c-1", limit=100)
    assert page.event_ids == ("e-1",)
    assert page.next_cursor == "c-2"
    assert page.completed is False
```

- [ ] **Step 2: Run page test and verify RED**

Run:

```bash
uv run pytest tests/clients/test_gamma_discovery_page.py -q
```

Expected: `GammaClient` has no public page method.

- [ ] **Step 3: Expose one bounded Gamma page**

Implement `EventPage` as an immutable result containing `events`,
`requested_cursor`, `next_cursor`, `completed`, `started_at_ms`, and
`finished_at_ms`. Reuse the existing keyset response validation; do not duplicate
the unbounded `iter_active_events()` loop.

- [ ] **Step 4: Write priority and anti-starvation RED tests**

```python
def test_old_unvisited_group_eventually_outranks_recent_low_edge_group() -> None:
    old = schedule_input(last_visited_at_ms=0, gross_edge_bps=0)
    recent = schedule_input(last_visited_at_ms=9_900_000, gross_edge_bps=50)
    assert priority_score(old, now_ms=10_000_000) > priority_score(
        recent, now_ms=10_000_000
    )


def test_discovery_commits_cursor_only_after_batch_rows() -> None:
    result = worker.run_batch()
    assert result.groups_seen == 2
    assert store.discovery_cursor() == "c-2"
    assert store.group_schedule("g-1").last_discovered_at_ms == result.finished_at_ms
```

- [ ] **Step 5: Implement durable queue and bounded worker**

Add tables for `neg_risk_discovery_state`, `neg_risk_group_schedule`, and
`neg_risk_coverage_samples`. `run_batch()` writes normalized group scheduling
rows, promotions, the coverage sample, and the new cursor in one transaction.
On an upstream or normalization error the cursor does not advance.

The score is deterministic:

```python
score = (
    Decimal(input.edge_bps) * Decimal("0.35")
    + Decimal(input.activity_rank) * Decimal("0.20")
    + Decimal(input.liquidity_rank) * Decimal("0.15")
    + Decimal(input.change_rank) * Decimal("0.15")
    + Decimal(input.age_rank) * Decimal("0.15")
)
```

Persist inputs, output score, and reason so weights can be calibrated from
evidence rather than silently changed.

- [ ] **Step 6: Add Make status entry**

`make perception-discovery-status db_path=/path/to/state.db` runs a read-only CLI that prints cursor,
last batch, queue depth by class, oldest visit age, and 15/30/60-minute raw and
liquidity-weighted coverage. It exits non-zero only on unreadable/invalid state,
not on low coverage.

- [ ] **Step 7: Verify Slice C**

Run:

```bash
uv run pytest tests/clients/test_gamma_discovery_page.py \
  tests/perception/test_priority.py tests/perception/test_discovery.py -q
make perception-discovery-status db_path=/tmp/polyarb-perception-fixture.db
make docs-m1-check
```

Expected: deterministic cursor/priority tests pass and fixture status is valid.

- [ ] **Step 8: Commit, summarize, and review**

```bash
git add src/polyarb/clients/gamma_client.py src/polyarb/perception \
  src/polyarb/config.py src/polyarb/daemon/main.py Makefile tests
git commit -m "feat(m1): add bounded opportunity discovery"
```

Write Task 3 summary and run an independent pagination/coverage review.

---

### Task 4: Checkpointed Full Reconciliation

**Files:**
- Create: `src/polyarb/perception/reconciliation.py`
- Modify: `src/polyarb/perception/store.py`
- Modify: `src/polyarb/daemon/scheduler.py`
- Modify: `src/polyarb/http/health.py`
- Modify: `src/polyarb/config.py`
- Modify: `Makefile`
- Test: `tests/perception/test_reconciliation.py`
- Test: `tests/m1-perception/test_health_endpoint.py`

**Interfaces:**
- Produces: `ReconciliationWorker.run_batch() -> ReconciliationBatchResult`.
- Produces: `OpportunityPerceptionStore.current_reconciliation() -> ReconciliationWindow | None`.
- Produces: `OpportunityPerceptionStore.apply_reconciliation_diff(window_id) -> DiffResult`.
- Health checks: `perception:reconciliation_progress` and
  `perception:reconciliation_checkpoint_age_seconds`.
- Make targets: `make reconcile-market-map`, `make reconciliation-status`.

- [ ] **Step 1: Write checkpoint/restart RED tests**

```python
def test_restart_resumes_after_last_committed_cursor() -> None:
    first = worker.run_batch()
    assert first.next_cursor == "c-2"

    restarted = reconciliation_worker(same_db=True)
    second = restarted.run_batch()

    assert second.requested_cursor == "c-2"
    assert store.current_reconciliation().pages_completed == 2


def test_incomplete_window_cannot_replace_online_group_revision() -> None:
    store.publish_group_revision(certified_group("g-1", revision=3))
    window = store.begin_reconciliation(started_at_ms=1_000)
    store.stage_reconciliation_group(window.id, changed_group("g-1"))

    with pytest.raises(ReconciliationIncompleteError):
        store.apply_reconciliation_diff(window.id)

    assert store.current_group("g-1").revision == 3
```

- [ ] **Step 2: Run reconciliation tests and verify RED**

Run:

```bash
uv run pytest tests/perception/test_reconciliation.py -q
```

Expected: reconciliation worker/store interfaces do not exist.

- [ ] **Step 3: Implement window, staging, and diff transaction**

Add append-only window/batch/staging tables. One `run_batch()` consumes one
`EventPage`, writes staging groups plus a batch receipt, then advances the
window cursor. A terminal empty page marks the window `complete`.

`apply_reconciliation_diff()` accepts only `complete` windows and applies each
group through Task 1 `publish_group_revision()` semantics. The report records
added/changed/closed/unchanged/rejected counts and the observation
`started_at_ms`/`finished_at_ms`.

- [ ] **Step 4: Demote the legacy Structure scheduler**

The existing universe-sized scheduler remains available behind
`POLYARB_LEGACY_STRUCTURE_RECONCILIATION_ENABLED`, default `False` after Slice D
qualification. Its adaptive timing/history is preserved. It no longer controls
`/arbitrage/opportunities` availability.

- [ ] **Step 5: Add chain-truth health**

Health reads the exact window/checkpoint rows written by `run_batch()`. It
reports progress and checkpoint age without making an incomplete background
window fail the Candidate Watcher check. A stuck checkpoint opens a scoped
reconciliation incident in Task 5.

- [ ] **Step 6: Verify kill/resume and commit**

Run:

```bash
uv run pytest tests/perception/test_reconciliation.py \
  tests/m1-perception/test_health_endpoint.py -q
make reconciliation-status db_path=/tmp/polyarb-perception-fixture.db
make planning-status
```

Then:

```bash
git add src/polyarb/perception src/polyarb/daemon/scheduler.py \
  src/polyarb/http/health.py src/polyarb/config.py Makefile tests
git commit -m "feat(m1): checkpoint full market reconciliation"
```

Write Task 4 summary and independently review diff-application atomicity.

---

### Task 5: Incident Lifecycle and Resource Shedding

**Files:**
- Create: `src/polyarb/perception/incidents.py`
- Create: `src/polyarb/perception/resource_controller.py`
- Create: `src/polyarb/perception/supervisor.py`
- Create: `src/polyarb/perception/worker_cli.py`
- Modify: `src/polyarb/perception/store.py`
- Modify: `src/polyarb/perception/candidate_watcher.py`
- Modify: `src/polyarb/perception/discovery.py`
- Modify: `src/polyarb/perception/reconciliation.py`
- Modify: `src/polyarb/daemon/main.py`
- Modify: `src/polyarb/http/health.py`
- Test: `tests/perception/test_incidents.py`
- Test: `tests/perception/test_resource_controller.py`
- Test: `tests/perception/test_supervisor.py`

**Interfaces:**
- Produces: `IncidentManager.detect(scope, kind, evidence) -> Incident`.
- Produces: `IncidentManager.transition(incident_id, state, evidence) -> Incident`.
- Produces: `ResourceController.decide(sample) -> ResourceDecision`.
- Produces: `ProducerSupervisor.run(spec, stop_event) -> None`.
- Health checks: `perception:open_incidents`, `perception:resource_mode`.

- [ ] **Step 1: Write incident lifecycle RED tests**

```python
def test_incident_cannot_close_without_post_recovery_writer_evidence() -> None:
    incident = manager.detect("candidate:g-1", "clob-timeout", {"attempt_id": 7})
    manager.transition(incident.id, "contained", {"circuit_open": True})
    manager.transition(incident.id, "recovering", {"retry": 1})

    with pytest.raises(RecoveryEvidenceRequiredError):
        manager.transition(incident.id, "verified", {"elapsed_s": 30})

    verified = manager.transition(
        incident.id,
        "verified",
        {"quote_batch_id": "qb-8", "quoted_at_ms": 20_000},
    )
    assert verified.state == "verified"
```

- [ ] **Step 2: Write resource shedding RED tests**

```python
def test_hot_quote_age_sheds_reconciliation_before_discovery() -> None:
    decision = controller.decide(
        sample(candidate_quote_p95_s=25, reconciliation_running=True)
    )
    assert decision.mode == "protect-hot-path"
    assert decision.reconciliation_enabled is False
    assert decision.discovery_batch_limit < decision.previous_discovery_batch_limit


def test_empty_candidate_set_expands_discovery_without_claiming_health() -> None:
    decision = controller.decide(sample(candidate_count=0, candidate_worker_ok=True))
    assert decision.discovery_batch_limit > decision.previous_discovery_batch_limit
    assert decision.reason == "empty-candidate-exploration"
```

- [ ] **Step 3: Implement append-only incidents and decisions**

Persist incident events, resource samples, and decisions. Valid incident
transitions are:

```python
ALLOWED = {
    "detected": {"classified"},
    "classified": {"contained", "escalated"},
    "contained": {"recovering", "escalated"},
    "recovering": {"verified", "contained", "escalated"},
    "verified": set(),
    "escalated": {"recovering"},
}
```

`verified` requires component-specific writer evidence: a new complete Quote
batch, advancing Discovery cursor, advancing Reconciliation checkpoint, or
responsive HTTP probe with the expected release.

- [ ] **Step 4: Implement supervised subprocess boundary**

`ProducerSupervisor` starts Candidate, Discovery, and Reconciliation commands as
separate child processes, captures bounded terminal receipts, terminates a
stalled child, and restarts only under the component circuit-breaker policy.
The HTTP process never performs group scans, `gc.collect()` over full
projections, or universe reconciliation.

The exact child commands are:

```python
PRODUCER_COMMANDS = {
    "candidate": (
        sys.executable,
        "-m",
        "polyarb.perception.worker_cli",
        "candidate",
    ),
    "discovery": (
        sys.executable,
        "-m",
        "polyarb.perception.worker_cli",
        "discovery",
    ),
    "reconciliation": (
        sys.executable,
        "-m",
        "polyarb.perception.worker_cli",
        "reconciliation",
    ),
}
```

- [ ] **Step 5: Add failure-matrix integration tests**

Use deterministic fake children to cover timeout, non-zero exit, cancellation,
restart, escalation after the configured retry limit, and post-restart durable
recovery. Assert that one component incident does not change unrelated
component health.

- [ ] **Step 6: Verify Slice E**

Run:

```bash
uv run pytest tests/perception/test_incidents.py \
  tests/perception/test_resource_controller.py \
  tests/perception/test_supervisor.py \
  tests/m1-perception/test_health_endpoint.py -q
uv run ruff check src/polyarb/perception tests/perception
```

Expected: all lifecycle, shedding, subprocess, and health-chain tests pass.

- [ ] **Step 7: Commit, summarize, teach, and review**

```bash
git add src/polyarb/perception src/polyarb/daemon/main.py \
  src/polyarb/http/health.py tests
git commit -m "feat(m1): recover perception component incidents"
```

Write Task 5 summary and `docs/learning/32-M1异常恢复.md`. Independent review
must trace each health check from writer mutation to observer output and chaos
trigger.

---

### Task 6: Read APIs and Operator Controls

**Files:**
- Create: `src/polyarb/http/perception.py`
- Modify: `src/polyarb/http/app.py`
- Modify: `src/polyarb/http/control.py`
- Modify: `src/polyarb/cli_perception.py`
- Modify: `Makefile`
- Modify: `docs/M1-市场感知平台使用手册.md`
- Test: `tests/m1-perception/test_perception_http.py`
- Test: `tests/m1-perception/test_perception_controls.py`
- Test: `tests/m1-perception/test_make_perception_contract.py`

**Interfaces:**
- GET `/perception/status`
- GET `/perception/groups`
- GET `/perception/groups/{group_id}/history`
- GET `/perception/discovery`
- GET `/perception/reconciliation`
- GET `/perception/incidents`
- HMAC POST `/control/perception/discovery`
- HMAC POST `/control/perception/reconciliation`
- Make targets: `make perception-status`, `make perception-groups`,
  `make perception-incidents`, `make queue-discovery`,
  `make queue-reconciliation`.

- [ ] **Step 1: Write bounded read-model RED tests**

```python
def test_perception_status_distinguishes_valid_zero_from_worker_failure(client) -> None:
    seed_healthy_workers(candidate_count=0)
    response = client.get("/perception/status")
    assert response.status_code == 200
    assert response.json()["opportunities"] == {
        "status": "available",
        "count": 0,
        "reason": "no-certified-edge",
    }

    seed_candidate_worker_failure()
    response = client.get("/perception/status")
    assert response.status_code == 503
    assert response.json()["opportunities"]["status"] == "unavailable"
```

- [ ] **Step 2: Run HTTP tests and verify RED**

Run:

```bash
uv run pytest tests/m1-perception/test_perception_http.py -q
```

Expected: routes return 404.

- [ ] **Step 3: Implement bounded, read-only handlers**

Every handler uses a read-only SQLite connection, `busy_timeout=250`, a
one-second thread boundary, explicit `limit <= 500`, and stable JSON envelopes.
No endpoint derives health from process-local counters when durable writer state
exists.

- [ ] **Step 4: Implement fail-closed controls**

Controls only set one durable queue flag. They cannot call producer internals,
revive an escalated/paused component, bypass concurrency, or mutate market
facts. Missing/invalid HMAC returns 401; unavailable component returns 409;
coalesced duplicate request returns 200 `already_queued`.

- [ ] **Step 5: Add Make and manual contracts**

Each Make target uses cloud HTTP and prints JSON. Read targets need no secret;
control targets require `POLYARB_SCAN_SHARED_SECRET`. Update the living manual
and its auditable sync log in the same commit.

- [ ] **Step 6: Verify and commit**

Run:

```bash
uv run pytest tests/m1-perception/test_perception_http.py \
  tests/m1-perception/test_perception_controls.py \
  tests/m1-perception/test_make_perception_contract.py -q
make docs-m1-check
make planning-status
```

Then:

```bash
git add src/polyarb/http src/polyarb/cli_perception.py Makefile \
  docs/M1-市场感知平台使用手册.md tests/m1-perception
git commit -m "feat(m1): expose opportunity-first operations"
```

Write Task 6 summary and independently review HMAC/read-only boundaries.

---

### Task 7: Dashboard Perception and Incident Views

**Files:**
- Create: `dashboard/app/perception/page.tsx`
- Create: `dashboard/app/perception/[group_id]/page.tsx`
- Create: `dashboard/lib/perception.ts`
- Modify: `dashboard/app/layout.tsx`
- Modify: `dashboard/lib/types.ts`
- Modify: `Makefile`
- Modify: `scripts/check_m1_manual.py`
- Test: `tests/m1-perception/test_dashboard_perception_contract.py`

**Interfaces:**
- Consumes: Task 6 public GET endpoints.
- Produces: `/perception` overview and `/perception/[group_id]` history page.
- Make target: `make smoke-perception-dashboard`.

- [ ] **Step 1: Write Dashboard source-contract RED test**

```python
def test_dashboard_has_perception_and_incident_surfaces() -> None:
    overview = Path("dashboard/app/perception/page.tsx").read_text()
    assert "/perception/status" in overview
    assert "Weighted coverage" in overview
    assert "Open incidents" in overview
    assert "watching" in overview
    assert "unavailable" in overview
```

- [ ] **Step 2: Run contract test and verify RED**

Run:

```bash
uv run pytest tests/m1-perception/test_dashboard_perception_contract.py -q
```

Expected: missing page file.

- [ ] **Step 3: Implement typed fail-soft API reader**

`dashboard/lib/perception.ts` defines exact response types and uses
`fetch(url, {cache: "no-store", signal: AbortSignal.timeout(3000)})`. Transport,
HTTP, and JSON failures return a typed unavailable result rendered as a warning;
they never become a valid zero-opportunity display. The server-side base URL is
`process.env.POLYARB_L1_URL ?? "https://polyarb-l1.fly.dev"`.

- [ ] **Step 4: Implement overview and group history**

The overview renders:

- watching/stale/unavailable/invalidated counts;
- opportunities with edge, capacity, Structure age, and Quote age;
- 15/30/60-minute raw and weighted coverage;
- Discovery queue and Reconciliation progress;
- resource mode and adjustment reason; and
- open incident state/action/retry/age.

The group page renders membership revisions, Quote batches, opportunity
transitions, and incident events on one timestamped timeline.

- [ ] **Step 5: Add smoke target and manual route**

`make smoke-perception-dashboard` requests `/perception`, rejects transport,
404, and 5xx, and accepts 200 or configured Vercel SSO 302/307. Add the route to
`scripts/check_m1_manual.py` and the living manual.

- [ ] **Step 6: Verify and commit**

Run:

```bash
make dashboard-typecheck
make dashboard-build
uv run pytest tests/m1-perception/test_dashboard_perception_contract.py -q
make docs-m1-check
```

Then:

```bash
git add dashboard Makefile scripts/check_m1_manual.py \
  docs/M1-市场感知平台使用手册.md tests/m1-perception
git commit -m "feat(m1): show perception operations dashboard"
```

Write Task 7 summary and run a six-pillar visual/UI review before deployment.

---

### Task 8: Production Fault Qualification and Cutover

**Files:**
- Create: `scripts/perception_fault_acceptance.py`
- Create: `docs/dev/perception-fault-runbook.md`
- Create: `docs/superpowers/plans/2026-07-28-m1-opportunity-first-PRODUCTION-EVIDENCE.md`
- Modify: `Makefile`
- Modify: `.planning/JOURNAL.md`
- Modify: `.planning/threads/market-observation-architecture.md`
- Modify: `docs/M1-市场感知平台使用手册.md`
- Test: `tests/m1-perception/test_perception_fault_acceptance.py`
- Test: `tests/m1-perception/test_perception_deploy_contract.py`

**Interfaces:**
- Make targets: `make qualify-perception-local`,
  `make qualify-perception-prod-readonly`,
  `make chaos-perception-gamma-timeout`,
  `make chaos-perception-gamma-partial`,
  `make chaos-perception-gamma-malformed`,
  `make chaos-perception-gamma-cursor`,
  `make chaos-perception-clob-missing-leg`,
  `make chaos-perception-clob-429`,
  `make chaos-perception-clob-latency`,
  `make chaos-perception-candidate-exit`,
  `make chaos-perception-discovery-exit`,
  `make chaos-perception-reconciliation-stall`,
  `make chaos-perception-sqlite-busy`,
  `make chaos-perception-disk-pressure`,
  `make chaos-perception-telegram-failure`,
  `make chaos-perception-daemon-restart`,
  `make chaos-perception-deploy-interrupt`,
  `make chaos-perception-contention`, and
  `make verify-perception-recovery`.
- Produces immutable production evidence with release, machine, boot, time
  window, samples, incident IDs, actions, and recovery receipts.

- [ ] **Step 1: Write verdict RED tests**

```python
def test_verdict_rejects_incident_without_recovery_writer_receipt() -> None:
    evidence = fixture_evidence(
        incident_state="verified",
        recovery_receipt=None,
    )
    assert evaluate(evidence).status == "FAIL"
    assert "missing-recovery-writer-evidence" in evaluate(evidence).reasons


def test_verdict_rejects_background_pass_with_hot_sla_violation() -> None:
    evidence = fixture_evidence(candidate_quote_p95_s=31, reconciliation_complete=True)
    assert evaluate(evidence).status == "FAIL"
    assert "candidate-quote-p95" in evaluate(evidence).reasons
```

- [ ] **Step 2: Implement deterministic acceptance evaluator**

The evaluator checks:

- HTTP p95 at most two seconds;
- high-priority Quote p95 at most 30 seconds and explicit stale before 90;
- normal Quote explicit stale before 120 seconds;
- 90% liquidity-weighted active-known coverage within 15 minutes;
- known-group oldest visit at most six hours;
- promotion-to-watch at most 60 seconds;
- reconciliation closure at most 24 hours or a visibly advancing checkpoint;
- MTTD at most 30 seconds and containment at most 60 seconds;
- no cross-membership Quote batch;
- no orphan collecting producer run; and
- every verified incident has component-specific recovery writer evidence.

- [ ] **Step 3: Add image-aware fault primitives**

Before any production mutation run:

```bash
make chaos-l2-fly-image-check
```

Fault targets cover Gamma timeout/partial/malformed/cursor, CLOB missing
leg/429/latency, each producer exit/stall, SQLite busy/disk pressure, Telegram
failure, daemon restart, interrupted deployment, and resource contention.
Every target includes baseline, injection, cleanup, recovery verification, and
incident closure. A failed cleanup blocks all subsequent injections.

- [ ] **Step 4: Run local and copied-production qualification**

Run:

```bash
make qualify-perception-local
make qualify-perception-prod-readonly
uv run pytest tests/m1-perception/test_perception_fault_acceptance.py \
  tests/m1-perception/test_perception_deploy_contract.py -q
uv run pytest -q
make dashboard-typecheck
make dashboard-build
make docs-m1-check
make planning-status
```

Expected: all local/read-only gates pass before deploy.

- [ ] **Step 5: Deploy exact SHA with feature flags dark**

Push the exact commit, deploy it, verify both Fly process groups expose that
SHA, and confirm current legacy reads remain available. Enable Slice A storage,
then Candidate Watcher, then Discovery, then Reconciliation in separate
auditable changes. Do not enable the next flag until the previous production
gate in the rollout table passes.

- [ ] **Step 6: Execute authorized production faults and continuous evidence**

Run one fault at a time. Preserve baseline, incident transition rows, resource
decisions, recovery writer receipt, health/API samples, and cleanup result.
After the fault matrix, collect a 24-hour continuous window proving queues,
checkpoints, candidate Quote freshness, HTTP responsiveness, and incident
closure keep advancing.

- [ ] **Step 7: Cut over readiness and retire the global gate**

Only after the deterministic evaluator returns PASS:

- make `/arbitrage/opportunities` read group-certified batches;
- set legacy all-universe Quote/Structure readiness flags off;
- retain historical snapshot/attempt data and rollback flags;
- update CURRENT/JOURNAL/manual/learning material with the exact evidence;
- run `make planning-status`; and
- obtain final code, security, and production-evidence reviews.

- [ ] **Step 8: Commit the qualification artifacts**

```bash
git add scripts/perception_fault_acceptance.py Makefile docs \
  .planning/JOURNAL.md .planning/threads/market-observation-architecture.md \
  tests/m1-perception
git commit -m "docs(m1): qualify opportunity-first perception"
```

Write Task 8 summary with exact release, incident IDs, acceptance output, and
remaining non-blocking limitations.

---

## Final Definition of Done

- Candidate Watcher serves group-certified, all-leg Quote batches without a
  complete-universe prerequisite.
- Discovery coverage and promotion advance continuously and are measurable.
- Full Reconciliation resumes from checkpoints and cannot replace hot state
  while incomplete.
- CPU/GIL-heavy producers do not run in the HTTP event loop.
- Failures produce durable incident transitions, bounded actions, recovery
  writer evidence, or explicit escalation.
- API and Dashboard distinguish zero opportunity, no candidate, stale evidence,
  producer failure, and transport failure.
- The production fault matrix and post-fault continuous window pass the
  deterministic evaluator.
- Observer-only safety boundaries remain intact.
