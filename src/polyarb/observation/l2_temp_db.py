"""Named-temp-file SQLite adapter — Supabase markets_latest rows → scanner-readable DB.

Phase 04 Plan 02 — D-02. Reframes the candidate compute path: instead of reading
from the (empty) L2 local SQLite, this adapter materialises a throwaway SQLite
file from Supabase ``markets_latest`` rows so ``scanner.run_recipe`` can run the
existing recipe SQL unchanged.

═══ Why a NAMED TEMP FILE, not :memory: ═════════════════════════════════════
``scanner.run_recipe`` opens a SEPARATE connection via
``sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)`` (scanner.py:142-143).
Two ``:memory:`` connections are two independent empty databases — so any
writes the adapter made would be invisible to ``run_recipe``. A real file
(``tempfile.NamedTemporaryFile(suffix='.db', delete=False)``) is shared
across processes, so the read-only URI from ``run_recipe`` sees the rows.
(RESEARCH 04-RESEARCH.md Pitfall 1.)

═══ NOT-NULL columns ═════════════════════════════════════════════════════════
The DDL declares ``condition_id``, ``fetched_at_ms``, ``snapshot_id`` as NOT
NULL. The narrow Supabase projection (11 cols) does NOT include
``condition_id`` or ``fetched_at_ms``. Inserting NULL into a NOT NULL column
raises ``IntegrityError`` — so we sentinel-fill ("" / 0) instead. ``incomplete``
has DEFAULT 0 but we still sentinel-fill for explicit determinism.

═══ Foreign-key handling (Option A) ══════════════════════════════════════════
``schemas.DDL`` starts with ``PRAGMA foreign_keys = ON`` AND declares
``markets.snapshot_id INTEGER NOT NULL REFERENCES snapshots(id)``. A markets
INSERT with snapshot_id absent in snapshots would raise FK violation.

We pick Option A from the plan: disable FK enforcement on the temp DB
(``PRAGMA foreign_keys = OFF``). The temp DB is throwaway and the scanner
opens it read-only — FK integrity adds no value. Option B (seeding snapshots)
would be more code with zero practical benefit.

═══ Fail-loud, not fail-silent ═══════════════════════════════════════════════
NULL-filled (truly nullable) columns referenced in a recipe's WHERE / ORDER BY
will yield 0 rows — that is correct, but operators may misread as "no
candidates" rather than "recipe references column not in narrow projection".
``warn_null_filled_recipe_columns`` logs a WARNING so the cause is visible.
Missing auxiliary TABLES would crash scanner — so this adapter ALWAYS creates
all DDL tables (``CREATE TABLE IF NOT EXISTS`` semantics in schemas.DDL).
"""
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

from loguru import logger

from polyarb.storage.schemas import DDL

# ─────────────────────────────────────────────────────────────────────────────
# Narrow ``markets_latest`` column → markets DDL column mapping.
# (Matches supabase_mirror.narrow_market_row mapping at supabase_mirror.py:31-61.)
# ─────────────────────────────────────────────────────────────────────────────
_NARROW_TO_MARKETS: dict[str, str] = {
    "market_id": "market_id",
    "question": "question",
    "slug": "slug",
    "event_slug": "event_id",  # rename — mirror uses event_slug for display
    "mid_price": "mid_price",
    "liquidity_usd": "liquidity_usd",
    "volume_usd": "volume_usd",
    "end_time_ms": "end_time_ms",
    "snapshot_id": "snapshot_id",
    "yes_token_id": "yes_token_id",  # D-07 added (Plan 01)
    "no_token_id": "no_token_id",  # Phase 05.3 durable outcome-pair projection
    # "question_zh" is NOT a markets column — it lives in question_translations.
}

# Columns present in markets DDL but absent from narrow projection AND truly
# nullable (REAL / INTEGER nullable). Recipes referencing these get 0 rows
# (and a WARNING via warn_null_filled_recipe_columns) — never a SQL error.
_NULL_FILLED_COLS: frozenset[str] = frozenset(
    [
        "best_bid_price",
        "best_bid_size",
        "best_ask_price",
        "best_ask_size",
        "active",
        "closed",
        "neg_risk",
        "neg_risk_market_id",
        "page_fetched_at_ms",
    ]
)

# NOT-NULL columns in DDL absent from narrow → MUST sentinel-fill (no NULLs).
_SENTINEL_FILL: dict[str, object] = {
    "condition_id": "",  # NOT NULL TEXT
    "fetched_at_ms": 0,  # NOT NULL INTEGER
    "snapshot_id": 0,  # NOT NULL INTEGER REFERENCES snapshots(id) (FK disabled)
    "incomplete": 0,  # NOT NULL INTEGER DEFAULT 0
}


def build_temp_db(markets_rows: list[dict]) -> Path:
    """Build a named-temp-file SQLite from Supabase ``markets_latest`` rows.

    Returns the Path to the temp file. **Caller MUST** ``os.unlink(path)`` after
    use (typically inside a ``try/finally`` block in ``compute_candidates``).

    Why a named temp file (not ``:memory:``): see module docstring.

    The DB is populated with the FULL ``schemas.DDL`` (markets + events +
    event_tags + validation_issues + question_translations + snapshots) so
    every recipe SQL — including ghost-suspicious (subqueries
    validation_issues) and by-tag (JOINs event_tags via events) — runs without
    "table does not exist" errors. PRAGMA foreign_keys is disabled on the temp
    DB so a sentinel ``snapshot_id`` does not trigger an FK violation.
    """
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        tmp_path = Path(f.name)

    con = sqlite3.connect(tmp_path)
    try:
        con.executescript(DDL)  # full DDL — all aux tables created.
        # Disable FK enforcement on this throwaway DB (Option A — see module docstring).
        con.execute("PRAGMA foreign_keys = OFF")
        for row in markets_rows:
            _insert_narrow_row(con, row)
            _maybe_insert_question_translation(con, row)
        con.commit()
    finally:
        con.close()
    return tmp_path


def _insert_narrow_row(con: sqlite3.Connection, narrow: dict) -> None:
    """Insert a single narrow markets_latest row into the temp DB markets table.

    Maps narrow keys → DDL columns via ``_NARROW_TO_MARKETS``. Sentinel-fills
    any absent NOT-NULL column from ``_SENTINEL_FILL``. NULL-fillable columns
    (``_NULL_FILLED_COLS``) are simply omitted — SQLite stores NULL.
    """
    mapped: dict[str, object | None] = {}
    for narrow_key, ddl_col in _NARROW_TO_MARKETS.items():
        if narrow_key == "event_slug":
            # Some narrow rows may carry event_id directly; prefer event_slug first.
            v = narrow.get("event_slug") or narrow.get("event_id")
        else:
            v = narrow.get(narrow_key)
        mapped[ddl_col] = v

    # Sentinel-fill NOT-NULL columns absent or None in the narrow row.
    for col, sentinel in _SENTINEL_FILL.items():
        existing = mapped.get(col)
        if existing is None:
            mapped[col] = sentinel

    cols = ",".join(mapped.keys())
    placeholders = ",".join("?" * len(mapped))
    # Note: column names come from a FIXED allowlist (_NARROW_TO_MARKETS +
    # _SENTINEL_FILL keys), NEVER from row data. VALUES are parameterised.
    # No SQL-injection surface even when row contents are attacker-controlled
    # (T-04-07 mitigation in plan threat model).
    con.execute(
        f"INSERT OR REPLACE INTO markets ({cols}) VALUES ({placeholders})",
        list(mapped.values()),
    )


def _maybe_insert_question_translation(con: sqlite3.Connection, narrow: dict) -> None:
    """If the narrow row carries ``question_zh``, mirror it into
    question_translations so the LEFT JOIN in run_recipe finds it."""
    question_zh = narrow.get("question_zh")
    question_en = narrow.get("question")
    if not question_zh or not question_en:
        return
    # Cheap deterministic key — production CRUD uses sha256(question_en) but
    # the temp DB only needs uniqueness for INSERT OR IGNORE semantics.
    import hashlib

    question_hash = hashlib.sha256(question_en.encode("utf-8")).hexdigest()
    con.execute(
        "INSERT OR IGNORE INTO question_translations "
        "(question_hash, question_en, question_zh, translator_model, "
        "translated_at_ms, token_cost, retry_count, is_dead) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (question_hash, question_en, question_zh, "supabase-mirror", 0, None, 0, 0),
    )


def warn_null_filled_recipe_columns(recipe) -> None:
    """Log a WARNING if a recipe's WHERE / ORDER BY references a NULL-filled column.

    Does NOT raise. NULL-filled columns are nullable in the DDL, so the recipe
    will execute — it will simply return 0 rows. The warning is purely
    informational so operators understand WHY a recipe that previously worked
    against the L1 SQLite now returns 0 rows against the L2 temp DB.

    Auxiliary TABLES (validation_issues, event_tags, events) are always present
    via ``build_temp_db`` — so no SQL error path here.
    """
    text = f"{getattr(recipe, 'where', '')} {getattr(recipe, 'order_by', '')}"
    for col in _NULL_FILLED_COLS:
        if col in text:
            logger.warning(
                f"recipe {getattr(recipe, 'name', '<unnamed>')!r} uses NULL-filled "
                f"column {col!r} in temp DB — will return 0 rows (markets_latest "
                f"narrow projection omits {col!r})"
            )
