# Phase 04: Candidate Set 扩容 + L2 Throughput 验证 + 投影 Gap 收尾 - Pattern Map

**Mapped:** 2026-05-28
**Files analyzed:** 10 new/modified files
**Analogs found:** 10 / 10

---

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `src/polyarb/observation/l2_candidate_refresh.py` | service | request-response (NOTIFY → Supabase fetch → temp DB → scan) | self (modify) | exact |
| `src/polyarb/observation/l2_temp_db.py` | utility | transform (Supabase rows → named-temp SQLite) | `tests/observation/test_l2_candidate_refresh.py:22-82` (_create_minimal_sqlite) | role-match |
| `src/polyarb/http/l2_health.py` | middleware | request-response | self (modify) | exact |
| `alembic/versions/004_add_yes_token_id.py` | migration | batch | `alembic/versions/003_l2_tables.py` | exact |
| `src/polyarb/storage/supabase_mirror.py` | service | CRUD | self (modify) | exact |
| `tests/observation/test_l2_temp_db.py` | test | CRUD | `tests/observation/test_l2_candidate_refresh.py` | role-match |
| `tests/observation/test_l2_candidate_refresh.py` | test | request-response | self (modify) | exact |
| `tests/alembic/test_004.py` | test | batch | `tests/alembic/test_003.py` | exact |
| `tests/http/test_l2_health_gap200.py` | test | request-response | `tests/m1-perception/test_l2_health_mirror_check.py` | exact |
| `Makefile` (new `chaos-l2-inj4-throughput` target) | config | event-driven | `Makefile:855-888` (chaos-l2-inj4) | exact |

---

## Pattern Assignments

### `src/polyarb/observation/l2_temp_db.py` (utility, transform)

**Purpose:** NEW FILE — builds a named temp SQLite file from Supabase `markets_latest` rows, populated with the full markets DDL schema so `run_recipe` can open it via `file:{tmp_path}?mode=ro` URI.

**Analog:** `tests/observation/test_l2_candidate_refresh.py:22-82` (`_create_minimal_sqlite`) — same pattern of executescript + INSERT per row.

**Critical constraint from RESEARCH.md:** `run_recipe` uses `file:{db_path}?mode=ro` URI (scanner.py:142). A pure `:memory:` connection is per-connection scoped — a second `sqlite3.connect(":memory:")` is a different empty database. Use `tempfile.NamedTemporaryFile(suffix='.db', delete=False)` instead.

**Imports pattern** (copy from test analog + add project imports):
```python
import os
import sqlite3
import tempfile
from pathlib import Path

from loguru import logger

from polyarb.storage.schemas import DDL
```

**Core pattern — DB construction** (from `tests/observation/test_l2_candidate_refresh.py:28-82`):
```python
def _create_minimal_sqlite(db_path: Path, markets: list[dict]) -> None:
    con = sqlite3.connect(db_path)
    con.executescript(
        """
        CREATE TABLE markets (
            market_id TEXT PRIMARY KEY,
            condition_id TEXT,
            slug TEXT,
            question TEXT,
            yes_token_id TEXT,
            ...
            event_id TEXT
        );
        CREATE TABLE question_translations (...);
        CREATE TABLE validation_issues (...);
        """
    )
    for m in markets:
        cols = ",".join(m.keys())
        placeholders = ",".join("?" * len(m))
        con.execute(f"INSERT INTO markets ({cols}) VALUES ({placeholders})", list(m.values()))
    con.commit()
    con.close()
```

**Phase 04 adaptation** — replace the minimal DDL with the authoritative `schemas.py` DDL, add the `event_tags` table (needed by `by-tag` recipe), and use a temp file path:

```python
# PRODUCTION PATTERN for build_temp_db():
def build_temp_db(markets_rows: list[dict]) -> Path:
    """Build a named-temp SQLite from Supabase markets_latest rows.

    Uses tempfile.NamedTemporaryFile — NOT :memory: — because run_recipe
    opens a SEPARATE sqlite3 connection via file:path?mode=ro URI, and two
    :memory: connections are two independent empty databases (RESEARCH Pitfall 1).

    Caller is responsible for os.unlink(path) after use (use try/finally).
    """
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmp_path = Path(f.name)

    con = sqlite3.connect(tmp_path)
    try:
        # Create ALL tables scanner touches: markets, question_translations,
        # validation_issues, event_tags. Use full DDL from schemas.py (not a
        # minimal subset) — this avoids SQL errors from missing tables.
        con.executescript(_TEMP_DB_DDL)  # derived from schemas.DDL, strip PRAGMAs
        # Insert narrow rows with NULL-fill for absent columns.
        for row in markets_rows:
            _insert_narrow_row(con, row)
        con.commit()
    finally:
        con.close()
    return tmp_path
```

**Column mapping pattern** (how narrow → full DDL):
```python
# _NARROW_TO_MARKETS maps markets_latest column names to markets DDL column names.
# event_slug in markets_latest → event_id in markets DDL (same mapping as supabase_mirror.py:57-59)
_NARROW_TO_MARKETS: dict[str, str] = {
    "market_id": "market_id",
    "question":  "question",
    "slug":      "slug",
    "event_slug": "event_id",      # key rename — mirror uses event_slug for display
    "mid_price": "mid_price",
    "liquidity_usd": "liquidity_usd",
    "volume_usd": "volume_usd",
    "end_time_ms": "end_time_ms",
    "snapshot_id": "snapshot_id",
    "question_zh": None,           # not a markets column — injected via question_translations
    "yes_token_id": "yes_token_id", # D-07 adds this to narrow projection
}

# Columns PRESENT in markets DDL but ABSENT from narrow projection.
# Split by DDL nullability — NOT-NULL columns CANNOT be NULL-filled (INSERT
# constraint violation); they MUST get a sentinel value. See Plan 02 Task 1
# action (authoritative). This corrects an earlier over-broad _NULL_FILLED_COLS.

# Nullable columns → safe to NULL-fill:
_NULL_FILLED_COLS = frozenset([
    "no_token_id",
    "best_bid_price", "best_bid_size", "best_ask_price", "best_ask_size",
    "active", "closed", "neg_risk", "neg_risk_market_id",
    "page_fetched_at_ms",
])

# NOT-NULL columns in schemas.DDL → MUST sentinel-fill, never NULL:
_SENTINEL_FILL = {
    "condition_id": "",      # NOT NULL TEXT
    "fetched_at_ms": 0,      # NOT NULL INTEGER
    "snapshot_id": 0,        # NOT NULL INTEGER (FK — temp DB uses PRAGMA foreign_keys=OFF)
    "incomplete": 0,         # NOT NULL INTEGER (default 0)
}
```

**Fail-loud warning pattern** (from RESEARCH Q2):
```python
def warn_null_filled_recipe_columns(recipe: "Recipe") -> None:
    """Log WARNING if recipe WHERE/ORDER BY references NULL-filled columns.

    Does NOT raise — NULL-filled columns cause 0-row results (not crashes).
    Missing tables (validation_issues, event_tags) are always created, so
    no SQL errors. This is an informational warning for the operator.
    """
    text = recipe.where + " " + recipe.order_by
    for col in _NULL_FILLED_COLS:
        if col in text:
            logger.warning(
                f"recipe {recipe.name!r} uses NULL-filled column {col!r} "
                f"in temp DB — will return 0 rows (markets_latest has no {col!r})"
            )
```

---

### `src/polyarb/observation/l2_candidate_refresh.py` (service, request-response)

**Purpose:** MODIFY — add Supabase fetch + temp DB path to `compute_candidates` and `on_snapshot_complete`.

**Analog:** self (existing file, lines 1-291)

**Imports to add** (after existing imports, `src/polyarb/observation/l2_candidate_refresh.py:29-41`):
```python
# Add these to existing imports:
import os
import tempfile
from supabase import create_client
from polyarb.observation.l2_temp_db import build_temp_db  # NEW module
```

**Module-level state pattern** (lines 50-51 — follow existing `_last_refresh_at_s` pattern):
```python
# Existing (line 50-51):
_last_refresh_at_s: float = 0.0

# D-01 adds (same module-level pattern):
_last_known_markets_rows: list[dict] | None = None  # fail-soft state
```

**Supabase fetch pattern** (from RESEARCH Q1 — pagination required because markets_latest has ~6729 rows):
```python
def _fetch_all_markets_latest(client) -> list[dict]:
    """Fetch all markets_latest rows with pagination.

    PostgREST default limit = 1000 rows. markets_latest has ~6729 rows.
    A plain .select("*").execute() silently truncates to 1000 (RESEARCH Pitfall 2).
    """
    rows: list[dict] = []
    offset = 0
    page_size = 1000
    while True:
        resp = (
            client.table("markets_latest")
            .select("*")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch: list[dict] = resp.data or []
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return rows
```

**compute_candidates signature change** (lines 83-104, add `markets_rows` parameter):
```python
def compute_candidates(
    settings: Any,
    scanner_yaml: Path | None = None,
    watchlist_yaml: Path | None = None,
    markets_rows: list[dict] | None = None,  # D-01: pre-fetched from Supabase
) -> list[CandidateRow]:
    # D-01: if markets_rows provided, build temp DB; otherwise fall back to settings.db_path
    if markets_rows is not None:
        tmp_path = build_temp_db(markets_rows)
        db_path = tmp_path
        cleanup_tmp = True
    else:
        db_path = Path(settings.db_path)
        tmp_path = None
        cleanup_tmp = False
    try:
        out: dict[str, CandidateRow] = {}
        # ... existing recipe loop and watchlist logic unchanged ...
    finally:
        if cleanup_tmp and tmp_path is not None:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
```

**on_snapshot_complete fail-soft fetch pattern** (lines 213-291 — follow existing error envelope at lines 116-118):
```python
async def on_snapshot_complete(payload, *, ws_consumer, settings, mirror=None) -> bool:
    global _last_refresh_at_s, _last_known_markets_rows
    # ... existing debounce check (lines 239-247) unchanged ...

    # D-01 Supabase fetch (fail-soft — same envelope as existing recipe failure at line 116-118)
    markets_rows: list[dict] | None = None
    supabase_url = getattr(settings, "supabase_url", "")
    try:
        service_key = settings.supabase_service_key.get_secret_value()
    except AttributeError:
        service_key = ""
    if supabase_url and service_key:
        try:
            client = create_client(supabase_url, service_key)
            markets_rows = _fetch_all_markets_latest(client)
            _last_known_markets_rows = markets_rows
            logger.info(f"candidate refresh: fetched {len(markets_rows)} rows from markets_latest")
        except Exception as e:  # noqa: BLE001
            logger.error(f"candidate refresh: supabase fetch failed: {e!r} — using last known rows")
            markets_rows = _last_known_markets_rows

    new_rows = compute_candidates(
        settings,
        getattr(settings, "candidate_scanner_yaml", None),
        getattr(settings, "candidate_watchlist_yaml", None),
        markets_rows=markets_rows,
    )
    # ... rest of mutation logic (lines 255-291) unchanged ...
```

---

### `src/polyarb/http/l2_health.py` (middleware, request-response)

**Purpose:** MODIFY — D-08 three-branch mirror gate at line 180.

**Analog:** self (existing file `src/polyarb/http/l2_health.py:174-216`)

**Existing gate pattern** (lines 174-216 — the target for D-08 modification):
```python
# CURRENT (line 180):
if getattr(settings, "l2_mirror_enabled", False):
    # ... full sub-check wiring (lines 181-216) ...
```

**D-08 replacement — three-branch pattern** (from RESEARCH Q6):
```python
# AFTER D-08 (replaces lines 174-216 gate + adds case (b)):
_supabase_url = getattr(settings, "supabase_url", "")
_service_key_val = ""
try:
    _service_key_val = settings.supabase_service_key.get_secret_value()
except AttributeError:
    pass

if _supabase_url and not _service_key_val:
    # Case (b): URL set but service_key missing — config mistake, surface as fail
    checks["mirror:l2_tob_age_seconds"] = [{
        "componentId": "supabase-l2-mirror",
        "componentType": "datastore",
        "observedValue": None,
        "status": "fail",
        "output": "mirror disabled by config (service_key empty)",
        "time": _utc_now_iso(),
    }]
    overall = _severity(overall, "fail")
elif getattr(settings, "l2_mirror_enabled", False):
    # Case (c): both url + key set — existing full sub-check logic (lines 181-216 unchanged)
    warn_s = int(getattr(settings, "l2_tob_age_warn_s", _MIRROR_PASS_S_DEFAULT))
    fail_s = int(getattr(settings, "l2_tob_age_fail_s", _MIRROR_FAIL_S_DEFAULT))
    # ... (existing body lines 183-216) ...
# else: case (a) url also empty — no sub-check (correct, Supabase not configured at all)
```

**Sub-check dict shape** (copy from lines 207-215 for the new case-b entry):
```python
checks["mirror:l2_tob_age_seconds"] = [{
    "componentId": "supabase-l2-mirror",
    "componentType": "datastore",
    "observedValue": None,           # no age measurement possible when disabled
    "observedUnit": "s",             # keep schema consistent
    "status": "fail",
    "output": "mirror disabled by config (service_key empty)",
    "time": _utc_now_iso(),
}]
overall = _severity(overall, "fail")
```

---

### `alembic/versions/004_add_yes_token_id.py` (migration, batch)

**Purpose:** NEW FILE — D-07 add-only migration adding `yes_token_id` nullable column to `markets_latest`.

**Analog:** `alembic/versions/003_l2_tables.py` (most recent migration — copy header + structure)

**Header boilerplate** (lines 53-60 of 003, adapt for 004):
```python
"""Add yes_token_id nullable column to markets_latest (Phase 04 D-07)

Revision ID: 004
Revises: 003
Create Date: 2026-05-28

Phase 04 D-07: markets_latest.yes_token_id (nullable TEXT).
Alembic add-only discipline (Phase 02 L15): upgrade() uses only op.add_column.
No DROP / RENAME in upgrade(). downgrade() reverses with op.drop_column for
replay test safety.
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "004"
down_revision = "003"
branch_labels = None
depends_on = None
```

**upgrade() pattern** (add-only, from 003_l2_tables.py discipline):
```python
def upgrade() -> None:
    op.add_column(
        "markets_latest",
        sa.Column("yes_token_id", sa.Text, nullable=True),
    )

def downgrade() -> None:
    op.drop_column("markets_latest", "yes_token_id")
```

**Test static checks** (from `tests/alembic/test_003.py:34-64` — copy the three static text checks):
- `test_down_revision_chain_to_003` — `down_revision = "003"` literal present
- `test_no_drop_in_upgrade` — `op.drop_` absent in upgrade() body
- `test_revision_id_is_004` — `revision = "004"` literal present

---

### `src/polyarb/storage/supabase_mirror.py` (service, CRUD)

**Purpose:** MODIFY — D-07 add `yes_token_id` to `_NARROW_MARKET_COLUMNS` and `narrow_market_row()`.

**Analog:** self (lines 31-61)

**_NARROW_MARKET_COLUMNS modification** (lines 31-42 — add one entry):
```python
# BEFORE (10 columns, lines 31-42):
_NARROW_MARKET_COLUMNS = (
    "market_id",
    "question",
    "slug",
    "event_slug",
    "mid_price",
    "liquidity_usd",
    "volume_usd",
    "end_time_ms",
    "snapshot_id",
    "question_zh",
)

# AFTER (11 columns, D-07):
_NARROW_MARKET_COLUMNS = (
    "market_id",
    "question",
    "slug",
    "event_slug",
    "mid_price",
    "liquidity_usd",
    "volume_usd",
    "end_time_ms",
    "snapshot_id",
    "question_zh",
    "yes_token_id",    # D-07: nullable; source = normalizer.py:107 clobTokenIds[0]
)
```

**narrow_market_row() modification** (lines 45-61 — the `col == "event_slug"` branch pattern):
```python
# Existing special-case pattern (lines 56-59):
elif col == "event_slug":
    out[col] = full_row.get("event_slug") or full_row.get("event_id")

# D-07 adds no special-case for yes_token_id — the default `.get(col)` branch
# (line 60) handles it correctly:
else:
    out[col] = full_row.get(col)   # returns None if key absent → nullable correct
```

No additional logic needed — `yes_token_id` maps directly via the existing `full_row.get(col)` fallback in the loop.

---

### `tests/observation/test_l2_temp_db.py` (test, transform)

**Purpose:** NEW FILE — unit tests for D-01 pagination, D-02 adapter schema, D-02 fail-loud, D-02 ghost-suspicious, D-03 near-end.

**Analog:** `tests/observation/test_l2_candidate_refresh.py` (same module, same test style)

**File header + env guard** (lines 1-16 of test_l2_candidate_refresh.py):
```python
"""Tests for polyarb.observation.l2_temp_db.

D-01 Supabase pagination — unit-mocked.
D-02 adapter schema — schema completeness + NULL-fill.
D-02 fail-loud warn — NULL-filled column logs WARNING.
D-02 ghost-suspicious — validation_issues table present, empty.
D-03 near-end recipe — runs against temp DB populated from narrow rows.
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")
os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
```

**Mock supabase client pattern** (from `tests/m1-perception/test_supabase_mirror.py:59-85`):
```python
def _make_supabase_mock_with_pages(pages: list[list[dict]]) -> MagicMock:
    """Mock supabase client that returns paginated batches.

    Each call to .execute() pops one page from `pages`.
    When pages are exhausted, returns empty list (triggers pagination loop exit).
    """
    mock_client = MagicMock()
    page_iter = iter(pages)

    def _execute():
        m = MagicMock()
        try:
            m.data = next(page_iter)
        except StopIteration:
            m.data = []
        return m

    mock_client.table.return_value.select.return_value.range.return_value.execute = _execute
    return mock_client
```

**Pagination test** (RED-first pattern from RESEARCH Validation Architecture):
```python
def test_fetch_pagination():
    """_fetch_all_markets_latest must paginate when rows > 1000."""
    from polyarb.observation.l2_candidate_refresh import _fetch_all_markets_latest

    page1 = [{"market_id": f"m{i}"} for i in range(1000)]
    page2 = [{"market_id": f"m{i}"} for i in range(1000, 1500)]
    mock_client = _make_supabase_mock_with_pages([page1, page2])

    rows = _fetch_all_markets_latest(mock_client)
    assert len(rows) == 1500
    assert rows[0]["market_id"] == "m0"
    assert rows[1000]["market_id"] == "m1000"
```

**Adapter schema test pattern**:
```python
def test_build_temp_db_schema(tmp_path):
    """build_temp_db must create markets table with all expected columns."""
    from polyarb.observation.l2_temp_db import build_temp_db

    narrow_rows = [{"market_id": "m1", "question": "Q?", "slug": "q",
                    "event_slug": "evt", "mid_price": 0.5,
                    "liquidity_usd": 1000.0, "volume_usd": 500.0,
                    "end_time_ms": 1800000000000, "snapshot_id": 1,
                    "question_zh": None, "yes_token_id": "YES-1"}]
    tmp = build_temp_db(narrow_rows)
    try:
        con = sqlite3.connect(tmp)
        cur = con.execute("PRAGMA table_info(markets)")
        cols = {row[1] for row in cur.fetchall()}
        for expected in ("market_id", "yes_token_id", "best_bid_price",
                         "neg_risk_market_id", "fetched_at_ms"):
            assert expected in cols, f"column {expected!r} missing from temp DB markets table"
        # All auxiliary tables must exist
        for table in ("question_translations", "validation_issues", "event_tags"):
            cur2 = con.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
            )
            assert cur2.fetchone() is not None, f"auxiliary table {table!r} missing"
        con.close()
    finally:
        import os; os.unlink(tmp)
```

---

### `tests/observation/test_l2_candidate_refresh.py` (test, request-response)

**Purpose:** MODIFY — add Supabase fetch integration tests for D-01 fail-soft + D-03 near-end from Supabase rows.

**Analog:** self (lines 1-120 — existing fixture + test patterns)

**Existing _reset_debounce_state fixture pattern** (lines 95-101 — extend to reset new module-level state):
```python
@pytest.fixture(autouse=True)
def _reset_debounce_state():
    """Reset module-level debounce + last_known_markets_rows between tests."""
    import polyarb.observation.l2_candidate_refresh as mod
    mod._last_refresh_at_s = 0.0
    mod._last_known_markets_rows = None  # D-01 new state
    yield
    mod._last_refresh_at_s = 0.0
    mod._last_known_markets_rows = None
```

**New test — fail-soft fetch uses last known rows** (RED-first, from RESEARCH Validation Architecture):
```python
def test_supabase_fetch_fail_uses_last_known(monkeypatch):
    """D-01 fail-soft: when supabase fetch raises, last known rows are used."""
    import polyarb.observation.l2_candidate_refresh as mod
    from polyarb.observation.l2_candidate_refresh import on_snapshot_complete

    # Seed last known rows with 1 market
    mock_row = {"market_id": "m1", "question": "Q?", "slug": "s",
                "event_slug": "e", "mid_price": 0.5, "liquidity_usd": 1000.0,
                "volume_usd": 500.0, "end_time_ms": 9999999999999, "snapshot_id": 1,
                "question_zh": None, "yes_token_id": "YES-1"}
    mod._last_known_markets_rows = [mock_row]

    settings = MagicMock()
    settings.supabase_url = "https://x.supabase.co"
    settings.supabase_service_key.get_secret_value.return_value = "key"
    settings.db_path = "/nonexistent"
    settings.candidate_scanner_yaml = None
    settings.candidate_watchlist_yaml = None

    with patch("polyarb.observation.l2_candidate_refresh.create_client") as mock_create:
        mock_create.side_effect = RuntimeError("network error")
        ws = MagicMock()
        ws.subscribed_assets = []
        import asyncio
        asyncio.run(on_snapshot_complete(
            {"snapshot_id": 42}, ws_consumer=ws, settings=settings
        ))
    # No exception raised — uses last known rows (fail-soft)
    # Candidate set may be empty (yes_token_id is set → near-end may not pass)
    # but the important thing is no crash
```

---

### `tests/alembic/test_004.py` (test, batch)

**Purpose:** NEW FILE — D-07 migration static checks + live-DB column existence.

**Analog:** `tests/alembic/test_003.py` (exact same structure)

**Complete file structure** (copy from `tests/alembic/test_003.py:1-238`, adapt for 004):
```python
"""Alembic 004 tests — yes_token_id column + add-only discipline.

Static checks (no Docker): revision chain, no op.drop_* in upgrade.
Live-DB checks (require Docker): column exists in markets_latest after migration.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")
os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")

MIGRATION_PATH = Path("alembic/versions/004_add_yes_token_id.py")


def test_down_revision_chain_to_003() -> None:
    """004 must chain after 003_l2_tables."""
    content = MIGRATION_PATH.read_text()
    assert 'down_revision = "003"' in content


def test_no_drop_in_upgrade() -> None:
    """upgrade() body must not contain op.drop_* (add-only discipline)."""
    content = MIGRATION_PATH.read_text()
    upgrade_start = content.find("def upgrade(")
    downgrade_start = content.find("def downgrade(")
    upgrade_body = content[upgrade_start:downgrade_start]
    assert "op.drop_" not in upgrade_body


def test_revision_id_is_004() -> None:
    content = MIGRATION_PATH.read_text()
    assert 'revision = "004"' in content
```

**Live-DB column check** (from `test_003.py:134-142` pattern, adapted):
```python
@pytest.mark.slow
def test_004_up(pg_dsn):
    """After upgrade head, markets_latest must have yes_token_id column."""
    r = _run_alembic(pg_dsn, "upgrade head")
    assert r.returncode == 0, f"alembic upgrade failed:\n{r.stdout}\n{r.stderr}"
    rows = _q(
        pg_dsn,
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name='markets_latest' AND column_name='yes_token_id'"
    )
    assert rows, "yes_token_id column missing from markets_latest after migration 004"
    # Confirm nullable
    rows2 = _q(
        pg_dsn,
        "SELECT is_nullable FROM information_schema.columns "
        "WHERE table_name='markets_latest' AND column_name='yes_token_id'"
    )
    assert rows2[0]["is_nullable"] == "YES", "yes_token_id must be nullable"
```

**pg_dsn + _run_alembic + _q helpers** — copy verbatim from `tests/alembic/test_003.py:88-131`.

---

### `tests/http/test_l2_health_gap200.py` (test, request-response)

**Purpose:** NEW FILE — D-08 three-branch /health mirror gate tests.

**Analog:** `tests/m1-perception/test_l2_health_mirror_check.py` (exact same structure — uses `_build_l2_health_checks` directly)

**File header + autouse fixture** (lines 1-49 of test_l2_health_mirror_check.py):
```python
"""Tests for D-08 GAP-200 — /health mirror:l2_tob_age_seconds three-branch gate.

Tests the new three-branch logic at l2_health.py:180:
- Case (a): supabase_url empty → no mirror sub-check (backwards compat)
- Case (b): supabase_url set but service_key empty → sub-check registered, status=fail
- Case (c): both url+key set → existing full sub-check (pass/warn/fail by age)
"""
from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest


@pytest.fixture(autouse=True)
def _allow_empty_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POLYARB_ALLOW_EMPTY_SECRET", "1")
    monkeypatch.setenv("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
    for var in (
        "POLYARB_SUPABASE_URL",
        "POLYARB_SUPABASE_SERVICE_KEY",
    ):
        monkeypatch.delenv(var, raising=False)
```

**Test pattern** (from `test_l2_health_mirror_check.py:172-187` — absent sub-check test):
```python
def test_both_empty_no_subcheck(monkeypatch: pytest.MonkeyPatch) -> None:
    """Case (a): url empty → mirror sub-check absent (backwards compat)."""
    from polyarb.config import Settings
    from polyarb.http.l2_health import _build_l2_health_checks

    settings = Settings(supabase_url="", supabase_service_key="")
    store = MagicMock()
    now_s = time.time()

    checks, overall = _build_l2_health_checks(
        store, settings, ws_consumer=None, event_listener=None, now_s=now_s
    )
    assert "mirror:l2_tob_age_seconds" not in checks


def test_url_set_key_empty_registers_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    """Case (b): url set but service_key empty → status=fail surfaced to /health."""
    from polyarb.config import Settings
    from polyarb.http.l2_health import _build_l2_health_checks

    settings = Settings(supabase_url="https://x.supabase.co", supabase_service_key="")
    # l2_mirror_enabled is False because service_key is empty (model_validator)
    assert settings.l2_mirror_enabled is False
    store = MagicMock()
    now_s = time.time()

    checks, overall = _build_l2_health_checks(
        store, settings, ws_consumer=None, event_listener=None, now_s=now_s
    )
    assert "mirror:l2_tob_age_seconds" in checks, "sub-check must be registered for case (b)"
    entry = checks["mirror:l2_tob_age_seconds"][0]
    assert entry["status"] == "fail"
    assert "service_key empty" in entry["output"]
    assert overall == "fail"
```

---

### `Makefile` — `chaos-l2-inj4-throughput` target (config, event-driven)

**Purpose:** NEW TARGET — D-05/D-06 throughput baseline + storm with frame_count + RSS measurement.

**Analog:** `Makefile:855-888` (chaos-l2-inj4) — copy the exact shell structure + FLY_API_TOKEN discipline.

**Target header pattern** (lines 840-854 comment block):
```makefile
## chaos-l2-inj4-throughput: D-05/D-06 — real candidate set WS storm + throughput verification
##
## Extends chaos-l2-inj4 with throughput measurement (Phase 04):
##   1. precondition: /healthz 200 + candidate set > 3 assets (confirms D-01 data source swap)
##   2. baseline: capture frame_count_t1, RSS_t1 at T=0
##   3. wait 5min: capture frame_count_t2, RSS_t2 (baseline frame rate)
##   4. storm: POLYARB_WS_TEST_KILL=1 (same as chaos-l2-inj4)
##   5. wait 60s: observe RECONNECTING transition
##   6. recovery check: frame_count_t3, watchdog=WAITING_FOR_EVENT, RSS_t3
##   7. pass criteria: frame_rate_t3 >= frame_rate_t1*0.90, RSS_t3 <= RSS_t1*1.30
##   8. cleanup: unset POLYARB_WS_TEST_KILL
```

**FLY_API_TOKEN discipline** (mandatory, from lines 672-684 comment + line 861):
```makefile
# ALL flyctl calls MUST be prefixed with `FLY_API_TOKEN= ` (no value)
# to force fallback to keychain credential.
# WRONG:  flyctl secrets set POLYARB_WS_TEST_KILL=1 -a polyarb-l2
# RIGHT:  FLY_API_TOKEN= flyctl secrets set POLYARB_WS_TEST_KILL=1 -a polyarb-l2
```

**core shell pattern** (from lines 855-888, replicate structure):
```makefile
chaos-l2-inj4-throughput:
	@echo "=== Inj L2-4-throughput: real candidate set WS storm ($$(date -u +%FT%TZ)) ==="
	@echo "→ Precondition: /healthz must be 200"
	@HZ=$$(curl -sS -o /dev/null -w '%{http_code}' https://polyarb-l2.fly.dev/healthz); \
	if [ "$$HZ" != "200" ]; then echo "ABORT: /healthz=$$HZ (expected 200)"; exit 1; fi
	@echo "→ Baseline: frame_count + RSS at T=0"
	@curl -sS https://polyarb-l2.fly.dev/health | jq '.checks["ws:connection_state"][0],.checks["ws:last_event_age_seconds"][0]'
	@echo "→ Waiting 5min for baseline frame rate..."
	@sleep 300
	@echo "→ Storm: POLYARB_WS_TEST_KILL=1"
	FLY_API_TOKEN= flyctl secrets set POLYARB_WS_TEST_KILL=1 -a polyarb-l2
	@sleep 60
	@echo "→ Recovery check: watchdog state (expect WAITING_FOR_EVENT within 60s)"
	@curl -sS https://polyarb-l2.fly.dev/health | jq '.status,.checks["ws:connection_state"][0]'
	@echo "→ CLEANUP"
	FLY_API_TOKEN= flyctl secrets unset POLYARB_WS_TEST_KILL -a polyarb-l2
	@sleep 30
	@echo "→ Final health (expect pass):"
	@curl -sS -o /dev/null -w 'HTTP %{http_code}\n' https://polyarb-l2.fly.dev/health
.PHONY: chaos-l2-inj4-throughput
```

---

## Shared Patterns

### Supabase Client Creation
**Source:** `src/polyarb/storage/supabase_mirror.py:81-88`
**Apply to:** `l2_candidate_refresh.py` (Supabase fetch in `on_snapshot_complete`)
```python
from supabase import Client, create_client

# Long-lived pattern (SupabaseMirror.__init__):
self._client: Client = create_client(url, service_key)

# Ephemeral pattern for per-refresh fetch (l2_candidate_refresh):
client = create_client(supabase_url, service_key)  # create per-refresh, not cached
rows = _fetch_all_markets_latest(client)
```

### Fail-Soft Error Envelope
**Source:** `src/polyarb/storage/supabase_mirror.py:115-118` + `src/polyarb/observation/l2_candidate_refresh.py:116-118`
**Apply to:** All new Supabase fetch paths in `l2_candidate_refresh.py`
```python
# Pattern (supabase_mirror.py:115-118):
except Exception as e:  # noqa: BLE001 — fail-soft
    logger.error(
        f"Supabase mirror failed snapshot_id={snapshot_id}: {str(e)[:200]}"
    )
    return False

# Pattern (l2_candidate_refresh.py:116-118):
except Exception as e:  # noqa: BLE001
    logger.warning(f"recipe {name!r} failed during candidate refresh: {e!r}")
    continue
```

### Health Sub-Check Dict Shape
**Source:** `src/polyarb/http/l2_health.py:207-215`
**Apply to:** D-08 new case-(b) sub-check in `l2_health.py`
```python
checks["mirror:l2_tob_age_seconds"] = [{
    "componentId": "supabase-l2-mirror",
    "componentType": "datastore",
    "observedValue": round(mirror_age, 1) if mirror_age is not None else None,
    "observedUnit": "s",
    "status": mirror_status,
    "output": mirror_output,
    "time": _utc_now_iso(),
}]
overall = _severity(overall, mirror_status)
```

### _severity() and _utc_now_iso() Utilities
**Source:** `src/polyarb/http/l2_health.py:48-56`
**Apply to:** `l2_health.py` D-08 case-(b) branch (already in-file, no import needed)
```python
def _severity(a: str, b: str) -> str:
    """Return worst of two health statuses (fail > warn > pass)."""
    order = {"pass": 0, "warn": 1, "fail": 2}
    return a if order.get(a, 0) >= order.get(b, 0) else b
```

### Alembic Add-Only Migration Structure
**Source:** `alembic/versions/003_l2_tables.py:53-60` (header) + `alembic/versions/001_initial_dashboard_schema.py` (add-column pattern)
**Apply to:** `alembic/versions/004_add_yes_token_id.py`
```python
revision = "004"
down_revision = "003"   # always chain to most recent
branch_labels = None
depends_on = None

def upgrade() -> None:
    op.add_column("markets_latest", sa.Column("yes_token_id", sa.Text, nullable=True))
    # NEVER op.drop_column / op.alter_column in upgrade() — Phase 02 L15 discipline

def downgrade() -> None:
    op.drop_column("markets_latest", "yes_token_id")  # only in downgrade, for replay
```

### Test _create_minimal_sqlite / DB Setup Pattern
**Source:** `tests/observation/test_l2_candidate_refresh.py:22-82`
**Apply to:** `tests/observation/test_l2_temp_db.py` (for schema completeness assertions)
```python
def _create_minimal_sqlite(db_path: Path, markets: list[dict]) -> None:
    con = sqlite3.connect(db_path)
    con.executescript("""
        CREATE TABLE markets (market_id TEXT PRIMARY KEY, ..., event_id TEXT);
        CREATE TABLE question_translations (...);
        CREATE TABLE validation_issues (...);
    """)
    for m in markets:
        cols = ",".join(m.keys())
        placeholders = ",".join("?" * len(m))
        con.execute(f"INSERT INTO markets ({cols}) VALUES ({placeholders})", list(m.values()))
    con.commit()
    con.close()
```

### Module-Level Debounce State Reset Fixture
**Source:** `tests/observation/test_l2_candidate_refresh.py:95-101`
**Apply to:** All new tests in `test_l2_candidate_refresh.py` (modify existing autouse fixture to also reset `_last_known_markets_rows`)
```python
@pytest.fixture(autouse=True)
def _reset_debounce_state():
    import polyarb.observation.l2_candidate_refresh as mod
    mod._last_refresh_at_s = 0.0
    mod._last_known_markets_rows = None  # D-01 new state
    yield
    mod._last_refresh_at_s = 0.0
    mod._last_known_markets_rows = None
```

### FLY_API_TOKEN=  Prefix (Mandatory Chaos Discipline)
**Source:** `Makefile:672-684` (comment) + `Makefile:861` (usage)
**Apply to:** ALL `flyctl` calls in `chaos-l2-inj4-throughput` target
```makefile
# MANDATORY pattern — prefix every flyctl with `FLY_API_TOKEN= ` (empty value)
# to prevent .env L1-only token from shadowing keychain credential.
FLY_API_TOKEN= flyctl secrets set POLYARB_WS_TEST_KILL=1 -a polyarb-l2
FLY_API_TOKEN= flyctl secrets unset POLYARB_WS_TEST_KILL -a polyarb-l2
```

---

## No Analog Found

No files in this phase lack a close codebase analog. All patterns are directly copy-adaptable from existing files.

---

## Implementation Notes for Planner

### Pitfall 1: :memory: SQLite is connection-scoped (RESEARCH Pitfall 1)
`run_recipe` opens `file:{db_path}?mode=ro` (scanner.py:142). A pure `:memory:` write on connection A is invisible to connection B. Use `tempfile.NamedTemporaryFile(suffix='.db', delete=False)` + `os.unlink` in `finally`.

### Pitfall 2: PostgREST 1000-row default limit (RESEARCH Pitfall 2)
`.select("*").execute()` without `.range()` silently truncates to 1000 rows. `markets_latest` has ~6729 rows. Always use the `_fetch_all_markets_latest` pagination loop.

### Pitfall 3: D-07 migration requires correct DB DSN auth
Run alembic with `POLYARB_SUPABASE_DB_DSN` (postgres:// DSN with service_role). Same DSN used for 001-003 migrations. Verify with `psql $POLYARB_SUPABASE_DB_DSN -c "SELECT current_user"` before running.

### D-08 does NOT require config.py changes
`l2_mirror_enabled` stays False when service_key is empty — the model_validator (config.py:238-240) logic is unchanged. D-08 only changes how `l2_health.py` presents the False case when `supabase_url` is non-empty.

### D-02 auxiliary tables
The temp DB MUST include `validation_issues` and `event_tags` tables (even if empty). `ghost-suspicious` recipe does a subquery against `validation_issues` (scanner.py trusted path); `by-tag` does a JOIN against `event_tags`. Missing tables = SQL error, not 0 rows. Always create them from `schemas.DDL` (which uses `CREATE TABLE IF NOT EXISTS`).

---

## Metadata

**Analog search scope:** `src/polyarb/`, `tests/`, `alembic/versions/`, `Makefile`
**Files scanned:** 14 files read + 8 grep operations
**Pattern extraction date:** 2026-05-28
