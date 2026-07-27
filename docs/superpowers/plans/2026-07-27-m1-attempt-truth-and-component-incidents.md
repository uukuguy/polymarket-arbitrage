# M1 Attempt Truth and Component Incidents Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox ( - [ ] ) syntax for tracking.

**Goal:** Persist every L1 scheduler-launched snapshot attempt and make its failure/recovery independently visible in health and Telegram, so a stale successful snapshot cannot conceal a new OOM and L2 cannot conceal L1 recovery.

**Architecture:** Add an append-only snapshot_attempts table to the existing volume-local SQLite database. SnapshotScheduler creates an attempt before spawning its child and closes it on normal status, exception, or cancellation; /health combines that attempt truth with existing published-market-truth age. Polywatch migrates from one global failure set to per-component incident state but continues to group simultaneous Telegram messages.

**Tech Stack:** Python 3.12, SQLite WAL, Starlette, asyncio subprocesses, pytest, Ruff, Makefile.

## Global Constraints

- Preserve snapshots, snapshot_source_coverage, market_view_published, and quote provenance; this adds operational truth rather than a second market-truth model.
- Keep /healthz always HTTP 200; /health remains strict 200 for pass/warn and 503 for fail.
- A SIGKILL must persist the existing possible-oom classification verbatim.
- Notification delivery is fail-soft, but a failed recovery delivery must preserve that component's incident.
- Do not resize Fly, alter crontab, migrate storage, or deploy in this plan.
- Every operator command needs a Makefile target and M1 manual entry.

---

## File Structure

| File | Responsibility |
|---|---|
| src/polyarb/storage/schemas.py | snapshot_attempts DDL and index |
| src/polyarb/storage/sqlite_store.py | begin, finish, latest-attempt persistence |
| src/polyarb/daemon/scheduler.py | scheduler lifecycle bracket |
| src/polyarb/http/health.py | latest attempt and failure-counter checks |
| scripts/polywatch/healthz_watcher.py | component-scoped alert/recovery state |
| scripts/snapshot_attempt_status.py | bounded local diagnostics |
| Makefile and docs/M1-市场感知平台使用手册.md | operator entry point |
| tests/m1-perception/test_scheduler.py | attempt lifecycle tests |
| tests/m1-perception/test_health_endpoint.py | health truth-chain tests |
| tests/m1-perception/test_polywatch_healthz_watcher.py | independent recovery tests |
| tests/m1-perception/test_snapshot_attempt_status.py | CLI tests |

## Task 1: Add append-only attempt persistence

**Files:**
- Modify: src/polyarb/storage/schemas.py
- Modify: src/polyarb/storage/sqlite_store.py
- Test: tests/m1-perception/test_scheduler.py

**Interfaces:**
- Produces SQLiteStore.begin_snapshot_attempt(*, started_at_ms: int) -> int.
- Produces SQLiteStore.finish_snapshot_attempt(*, attempt_id: int, outcome: str, finished_at_ms: int, snapshot_id: int | None, failure_kind: str | None) -> None.
- Produces SQLiteStore.get_latest_snapshot_attempt() -> dict[str, object] | None.
- Terminal outcome is one of succeeded, failed, cancelled. A terminal row cannot be rewritten.

- [ ] **Step 1: Write failing lifecycle tests**

Add these tests to tests/m1-perception/test_scheduler.py:

~~~python
def test_snapshot_attempt_lifecycle_is_append_only(daemon_settings_for_test):
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()
    attempt_id = store.begin_snapshot_attempt(started_at_ms=1_000)
    store.finish_snapshot_attempt(
        attempt_id=attempt_id,
        outcome="failed",
        finished_at_ms=2_000,
        snapshot_id=None,
        failure_kind="snapshot-subprocess-signal-sigkill-possible-oom",
    )

    assert store.get_latest_snapshot_attempt() == {
        "id": attempt_id,
        "started_at_ms": 1_000,
        "finished_at_ms": 2_000,
        "outcome": "failed",
        "snapshot_id": None,
        "failure_kind": "snapshot-subprocess-signal-sigkill-possible-oom",
    }


def test_snapshot_attempt_terminal_row_cannot_be_rewritten(daemon_settings_for_test):
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()
    attempt_id = store.begin_snapshot_attempt(started_at_ms=1_000)
    store.finish_snapshot_attempt(
        attempt_id=attempt_id,
        outcome="succeeded",
        finished_at_ms=2_000,
        snapshot_id=746,
        failure_kind=None,
    )
    with pytest.raises(ValueError, match="not running"):
        store.finish_snapshot_attempt(
            attempt_id=attempt_id,
            outcome="failed",
            finished_at_ms=3_000,
            snapshot_id=None,
            failure_kind="late-rewrite",
        )
~~~

- [ ] **Step 2: Verify RED**

Run: uv run pytest tests/m1-perception/test_scheduler.py -k snapshot_attempt -v

Expected: FAIL because SQLiteStore lacks begin_snapshot_attempt.

- [ ] **Step 3: Implement table and store methods**

Directly after SCHEDULER_STATE_DDL in src/polyarb/storage/schemas.py add:

~~~python
SNAPSHOT_ATTEMPTS_DDL = """
CREATE TABLE IF NOT EXISTS snapshot_attempts (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at_ms  INTEGER NOT NULL,
    finished_at_ms INTEGER,
    outcome        TEXT NOT NULL CHECK(outcome IN ('running','succeeded','failed','cancelled')),
    snapshot_id    INTEGER REFERENCES snapshots(id),
    failure_kind   TEXT,
    CHECK(
        (outcome = 'running' AND finished_at_ms IS NULL AND snapshot_id IS NULL AND failure_kind IS NULL)
        OR (outcome = 'succeeded' AND finished_at_ms IS NOT NULL AND snapshot_id IS NOT NULL AND failure_kind IS NULL)
        OR (outcome IN ('failed','cancelled') AND finished_at_ms IS NOT NULL)
    )
);
CREATE INDEX IF NOT EXISTS idx_snapshot_attempts_started_at_ms
ON snapshot_attempts(started_at_ms DESC);
"""
~~~

Import and execute this DDL in SQLiteStore.init_schema. Add:

~~~python
def begin_snapshot_attempt(self, *, started_at_ms: int) -> int:
    con = sqlite3.connect(self._db_path, isolation_level=None)
    try:
        cur = con.execute(
            "INSERT INTO snapshot_attempts(started_at_ms,outcome) VALUES (?, 'running')",
            (started_at_ms,),
        )
        assert cur.lastrowid is not None
        return int(cur.lastrowid)
    finally:
        con.close()


def finish_snapshot_attempt(
    self, *, attempt_id: int, outcome: str, finished_at_ms: int,
    snapshot_id: int | None, failure_kind: str | None,
) -> None:
    if outcome not in {"succeeded", "failed", "cancelled"}:
        raise ValueError(f"invalid terminal snapshot attempt outcome: {outcome}")
    con = sqlite3.connect(self._db_path, isolation_level=None)
    try:
        cur = con.execute(
            "UPDATE snapshot_attempts SET finished_at_ms=?, outcome=?, snapshot_id=?, failure_kind=? "
            "WHERE id=? AND outcome='running'",
            (finished_at_ms, outcome, snapshot_id, failure_kind, attempt_id),
        )
        if cur.rowcount != 1:
            raise ValueError(f"snapshot attempt {attempt_id} is not running")
    finally:
        con.close()
~~~

Implement get_latest_snapshot_attempt with a read-only SQLite URI and return exactly
the test dictionary. Never write raw exception text into failure_kind.

- [ ] **Step 4: Verify GREEN and commit**

Run:

~~~bash
uv run pytest tests/m1-perception/test_scheduler.py -k snapshot_attempt -v
uv run ruff check src/polyarb/storage/schemas.py src/polyarb/storage/sqlite_store.py tests/m1-perception/test_scheduler.py
git add src/polyarb/storage/schemas.py src/polyarb/storage/sqlite_store.py tests/m1-perception/test_scheduler.py
git commit -m "feat(m1): persist snapshot attempt outcomes"
~~~

Expected: pytest and Ruff exit 0 before commit.

## Task 2: Bind scheduler and health to durable attempt truth

**Files:**
- Modify: src/polyarb/daemon/scheduler.py
- Modify: src/polyarb/http/health.py
- Test: tests/m1-perception/test_scheduler.py
- Test: tests/m1-perception/test_health_endpoint.py

**Interfaces:**
- Consumes Task 1 store methods.
- Produces health checks snapshot:latest_attempt and snapshot:failure_counter.
- A failed latest attempt is warn only while published market truth is fresh; it becomes fail when published truth is not pass.

- [ ] **Step 1: Write failing scheduler and health tests**

Add this scheduler test:

~~~python
@pytest.mark.asyncio
async def test_scheduler_persists_sigkill_attempt_failure(daemon_settings_for_test):
    from polyarb.daemon.scheduler import SnapshotScheduler, SnapshotSubprocessError
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()
    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)
    scheduler._run_snapshot = AsyncMock(
        side_effect=SnapshotSubprocessError("signal-sigkill-possible-oom")
    )

    await scheduler._tick()

    attempt = store.get_latest_snapshot_attempt()
    assert attempt["outcome"] == "failed"
    assert attempt["failure_kind"] == "snapshot-subprocess-signal-sigkill-possible-oom"
~~~

In test_health_endpoint.py insert a fresh published snapshot, then use the Task 1
store API to write a failed possible-OOM attempt. Assert:

~~~python
response = http_test_client.get("/health")
checks = response.json()["checks"]
assert checks["market_truth:last_complete_age_seconds"][0]["status"] == "pass"
assert checks["snapshot:latest_attempt"][0]["observedValue"] == "failed"
assert checks["snapshot:latest_attempt"][0]["status"] == "warn"
assert "possible-oom" in checks["snapshot:latest_attempt"][0]["output"]
~~~

Add a second test that writes FAILURE_THRESHOLD failed scheduler attempts and
asserts snapshot:failure_counter is fail and /health returns 503.

- [ ] **Step 2: Verify RED**

Run: uv run pytest tests/m1-perception/test_scheduler.py tests/m1-perception/test_health_endpoint.py -k 'sigkill_attempt or latest_attempt or failure_counter' -v

Expected: FAIL because scheduler and health do not expose attempt truth.

- [ ] **Step 3: Implement lifecycle and health checks**

At the start of every non-paused SnapshotScheduler._tick, call through
asyncio.to_thread:

~~~python
attempt_id = await asyncio.to_thread(
    self._sqlite_store.begin_snapshot_attempt,
    started_at_ms=int(time.time() * 1000),
)
~~~

Use one private async _finish_attempt helper that calls finish_snapshot_attempt
through asyncio.to_thread and logs a warning on its own write failure without
changing the scheduler counter. Close exactly once:

- OK or DEGRADED: succeeded, result.snapshot_id, no failure_kind.
- FAILED status: failed, result.snapshot_id, snapshot-status-failed.
- SnapshotSubprocessError: failed, no snapshot_id, str(error).
- other Exception: failed, no snapshot_id, exception-{type(error).__name__}.
- CancelledError: cancelled, no snapshot_id, scheduler-cancelled; then re-raise.

In _build_health_checks read latest_attempt and scheduler_state. Add
snapshot:latest_attempt with observedValue latest outcome or never-started.
For failed/cancelled attempts choose warn when market_truth:last_complete_age_seconds
is pass, otherwise fail; output only failure_kind. Add snapshot:failure_counter:
0 is pass, 1 through SnapshotScheduler.FAILURE_THRESHOLD - 1 is warn, and
threshold-or-greater is fail. Use a local import of SnapshotScheduler to avoid
duplicating literal 5.

- [ ] **Step 4: Verify GREEN and commit**

Run:

~~~bash
uv run pytest tests/m1-perception/test_scheduler.py tests/m1-perception/test_health_endpoint.py -v
uv run ruff check src/polyarb/daemon/scheduler.py src/polyarb/http/health.py tests/m1-perception/test_scheduler.py tests/m1-perception/test_health_endpoint.py
git add src/polyarb/daemon/scheduler.py src/polyarb/http/health.py tests/m1-perception/test_scheduler.py tests/m1-perception/test_health_endpoint.py
git commit -m "fix(m1): surface latest snapshot attempt failures"
~~~

Expected: pytest and Ruff exit 0 before commit.

## Task 3: Make Polywatch notifications component-scoped

**Files:**
- Modify: scripts/polywatch/healthz_watcher.py
- Test: tests/m1-perception/test_polywatch_healthz_watcher.py

**Interfaces:**
- Consumes Task 2 health checks.
- Produces state shaped as {"incidents": {component: {"active": bool, "last_alert_at_s": float}}}.
- Component names are l1, l2, opportunity, dashboard.

- [ ] **Step 1: Write failing watcher tests**

Add:

~~~python
def test_failed_snapshot_attempt_pushes_even_when_last_success_is_fresh() -> None:
    health = _health(checks={
        "snapshot:last_success_age_seconds": _check(60.0),
        "snapshot:latest_attempt": _check("failed", status="warn"),
        "snapshot:failure_counter": _check(1, status="warn"),
        "market_truth:coverage": _check("complete"),
        "quote_feed:last_complete_age_seconds": _check(20.0),
        "quote_feed:collector_state": _check("running"),
    })
    assert WATCHER.decide_l1(health)[0] == "push"


def test_l1_recovery_is_sent_while_l2_remains_active() -> None:
    decisions = WATCHER.component_notification_decisions(
        {"l1": False, "l2": True},
        {"incidents": {
            "l1": {"active": True, "last_alert_at_s": 1_000.0},
            "l2": {"active": True, "last_alert_at_s": 1_000.0},
        }},
        now_s=1_100.0,
        reminder_s=1_800,
    )
    assert decisions["l1"] == "recovery"
    assert decisions["l2"] == "suppress"
~~~

Also test legacy active_keys conversion and a failed L1 recovery delivery retaining
only the l1 incident.

- [ ] **Step 2: Verify RED**

Run: uv run pytest tests/m1-perception/test_polywatch_healthz_watcher.py -k 'failed_snapshot_attempt or l1_recovery' -v

Expected: FAIL because component_notification_decisions does not exist.

- [ ] **Step 3: Implement component state and messages**

Extend decide_l1 to push on a missing, failed, or cancelled snapshot:latest_attempt,
and on a fail-status snapshot:failure_counter. Preserve existing market-truth and
quote checks.

Replace global notification_decision with:

~~~python
def component_notification_decisions(
    active_by_component: Mapping[str, bool],
    state: Mapping[str, object],
    *,
    now_s: float,
    reminder_s: int,
) -> dict[str, str]:
    ...
~~~

It returns alert, suppress, recovery, or noop independently for each component.
In _load_notification_state, convert legacy active_keys and last_alert_at_s into
the equivalent incidents mapping. In main, construct active_by_component from
the four existing decisions, group every alert into one Telegram message, group
every recovery into one Telegram message, and update each component only with
its own delivery success. A continuing l2 incident must never block l1 recovery.

- [ ] **Step 4: Verify GREEN and commit**

Run:

~~~bash
uv run pytest tests/m1-perception/test_polywatch_healthz_watcher.py -v
uv run ruff check scripts/polywatch/healthz_watcher.py tests/m1-perception/test_polywatch_healthz_watcher.py
git add scripts/polywatch/healthz_watcher.py tests/m1-perception/test_polywatch_healthz_watcher.py
git commit -m "fix(polywatch): recover incidents per component"
~~~

Expected: pytest and Ruff exit 0 before commit.

## Task 4: Add read-only operator visibility and documentation

**Files:**
- Create: scripts/snapshot_attempt_status.py
- Modify: Makefile
- Modify: docs/M1-市场感知平台使用手册.md
- Create: tests/m1-perception/test_snapshot_attempt_status.py
- Modify: tests/m1-perception/test_m1_manual_contract.py
- Create: docs/learning/25-M1数据层与失败事实.md
- Modify: docs/learning/00-INDEX.md
- Modify: .planning/JOURNAL.md
- Modify: .planning/threads/market-observation-architecture.md

**Interfaces:**
- Produces make snapshot-attempt-status, a local read-only command.

- [ ] **Step 1: Write failing command and manual tests**

Create a subprocess test that writes one failed attempt to a temporary SQLite DB,
runs the script with POLYARB_DB_PATH set to that path, and asserts JSON is:

~~~python
{
    "latest": {
        "outcome": "failed",
        "failure_kind": "snapshot-subprocess-signal-sigkill-possible-oom",
    }
}
~~~

Add a no-history test expecting {"latest": null} and exit 0. Extend Makefile and
manual contract tests to require the target and the phrase latest snapshot attempt.

- [ ] **Step 2: Verify RED**

Run: uv run pytest tests/m1-perception/test_snapshot_attempt_status.py tests/m1-perception/test_m1_manual_contract.py -v

Expected: FAIL because the command and manual entry do not exist.

- [ ] **Step 3: Implement command, Makefile, manual, and teaching handoff**

Create scripts/snapshot_attempt_status.py:

~~~python
from __future__ import annotations

import json

from polyarb.config import load_settings
from polyarb.storage.sqlite_store import SQLiteStore


def main() -> int:
    settings = load_settings()
    print(json.dumps(
        {"latest": SQLiteStore(settings.db_path).get_latest_snapshot_attempt()},
        sort_keys=True,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
~~~

Add beside snapshot-status:

~~~make
## snapshot-attempt-status: Show the latest L1 scheduler snapshot attempt. Read-only.
snapshot-attempt-status:
	@uv run python scripts/snapshot_attempt_status.py
~~~

The manual must explain that snapshot:last_success describes published truth while
snapshot:latest_attempt describes the newest scheduler run; the latter can show
OOM while the former remains fresh. Add a read-only local command example.

Create docs/learning/25-M1数据层与失败事实.md with sections 30 秒心智模型,
关键代码, 设计取舍, 自检题, and FAQ 增量. Add it as item 25 in the index.
Its self-test asks why L2 must not suppress L1 recovery.

Append this verified boundary to JOURNAL and the market-observation thread:

~~~markdown
- [LEARNING] Published market truth and latest scheduler attempt are separate
  operational facts. A current published revision may remain readable, but a
  newly failed attempt is immediately visible and independently alertable.
~~~

State that this wave neither makes the legacy all-in-one snapshot a production
Structure service nor starts a new 24-hour qualification.

- [ ] **Step 4: Verify all first-wave gates and commit**

Run:

~~~bash
uv run pytest tests/m1-perception/test_scheduler.py tests/m1-perception/test_health_endpoint.py tests/m1-perception/test_polywatch_healthz_watcher.py tests/m1-perception/test_snapshot_attempt_status.py tests/m1-perception/test_m1_manual_contract.py tests/test_makefile.py -v
uv run ruff check src/polyarb/storage/schemas.py src/polyarb/storage/sqlite_store.py src/polyarb/daemon/scheduler.py src/polyarb/http/health.py scripts/polywatch/healthz_watcher.py scripts/snapshot_attempt_status.py tests/m1-perception/test_scheduler.py tests/m1-perception/test_health_endpoint.py tests/m1-perception/test_polywatch_healthz_watcher.py tests/m1-perception/test_snapshot_attempt_status.py
make planning-status
git add scripts/snapshot_attempt_status.py Makefile docs/M1-市场感知平台使用手册.md tests/m1-perception/test_snapshot_attempt_status.py tests/m1-perception/test_m1_manual_contract.py docs/learning/00-INDEX.md docs/learning/25-M1数据层与失败事实.md .planning/JOURNAL.md .planning/threads/market-observation-architecture.md
git commit -m "docs(m1): expose snapshot attempt diagnostics"
~~~

Expected: pytest, Ruff, and planning-status exit 0 before commit.

## Completion Criteria

1. A scheduler SIGKILL/OOM has a durable failed attempt row even when no new snapshots row exists.
2. /health exposes last complete market truth, latest attempt, and failure counter; fresh old truth cannot hide new failure.
3. Polywatch sends L1 recovery while L2 remains unhealthy and retries only a component whose recovery delivery failed.
4. make snapshot-attempt-status is documented, local, bounded, and read-only.
5. This wave does not deploy, resize, or claim a new 24-hour qualification.
