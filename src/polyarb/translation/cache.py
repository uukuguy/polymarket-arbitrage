"""SQLite CRUD for the question_translations append-only cache.

Pattern (analog: src/polyarb/storage/sqlite_store.py):
    - stdlib sqlite3 + parameterized SQL (NEVER string interpolation)
    - BEGIN IMMEDIATE around batched writes
    - isolation_level=None gives explicit transaction control

CRITICAL INVARIANT — append-only:
    `question_translations` is a CUMULATIVE cache across snapshots. Markets table
    semantics (D-C1 full overwrite via DELETE FROM) does NOT apply here.
    NEVER `DELETE FROM question_translations` — translations survive across
    snapshots so we don't burn tokens re-translating the same English question.

Schema (defined in storage/schemas.py, owned there):
    question_hash     PK = sha256(question_en)
    question_en       NOT NULL
    question_zh       NOT NULL
    translator_model  NOT NULL
    translated_at_ms  NOT NULL
    token_cost        prompt_tokens + completion_tokens (sum)
    retry_count       starts 0, increments on TransientError
    is_dead           1 when retry_count > 3 (manual reset to retry)

Hash join strategy (CONTEXT.md Open Question #1, decided plan 02):
    SQLite has no built-in sha256 UDF; question_hash is computed in Python at
    upsert time and used as PK to dedupe. Scan-time joins use the UNIQUE index
    `idx_qt_question_en` for `LEFT JOIN ON m.question = qt.question_en` —
    string equality on the original text, not hash.
"""

from __future__ import annotations

import hashlib
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path

from loguru import logger

from polyarb.storage.schemas import DDL


@dataclass(frozen=True)
class TranslationRow:
    """One translation result, ready to upsert.

    `question_hash` is auto-derived from question_en if not provided. Callers
    should pass it explicitly (computed once at list_untranslated time) so the
    hash returned to the caller and the hash inserted are guaranteed identical.
    """

    question_hash: str
    question_en: str
    question_zh: str
    translator_model: str
    translated_at_ms: int
    token_cost: int


def question_hash_for(question_en: str) -> str:
    """Stable hash of the English question — used as PK and as the retry-key.

    sha256 hex digest. UTF-8 encoded so non-ASCII (rare but possible) is stable.
    """
    return hashlib.sha256(question_en.encode("utf-8")).hexdigest()


_UPSERT_SQL = (
    "INSERT OR IGNORE INTO question_translations("
    "question_hash, question_en, question_zh, translator_model, "
    "translated_at_ms, token_cost, retry_count, is_dead) "
    "VALUES (?, ?, ?, ?, ?, ?, 0, 0)"
)


class TranslationCache:
    """Single-connection CRUD for question_translations.

    Schema is created by SQLiteStore.init_schema() (which executes the shared
    DDL block). This class assumes the table exists; for tests / standalone
    usage, call ``init_schema()`` once on a fresh db.
    """

    def __init__(self, db_path: Path) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def db_path(self) -> Path:
        return self._db_path

    def init_schema(self) -> None:
        """Idempotent schema creation — safe to re-run.

        Delegates to the shared DDL so events / markets / question_translations
        all stay in lockstep.
        """
        con = sqlite3.connect(self._db_path, isolation_level=None)
        try:
            con.executescript(DDL)
        finally:
            con.close()

    # ── Read ─────────────────────────────────────────────────────────────────

    def list_untranslated(self, limit: int | None = None) -> list[tuple[str, str]]:
        """Return ``[(question_hash, question_en), ...]`` for markets lacking a translation.

        A question is "untranslated" if either:
          (a) NO row exists in question_translations for this question_en, OR
          (b) A row exists with question_zh='' (retry placeholder) AND is_dead=0
              — the question was attempted but failed and is still retryable.

        Dead translations (is_dead=1) are NEVER re-attempted automatically;
        manually clear is_dead=0 to retry. Successful translations
        (question_zh != '') are NEVER re-attempted.

        Returns at most ``limit`` rows when given (None = unlimited). Hash is
        computed in Python (sha256) so it matches the upsert path exactly.
        """
        con = sqlite3.connect(f"file:{self._db_path}?mode=ro", uri=True)
        try:
            sql = (
                "SELECT DISTINCT m.question FROM markets m "
                "LEFT JOIN question_translations qt "
                "  ON qt.question_en = m.question "
                "WHERE m.question IS NOT NULL "
                "  AND ("
                "    qt.question_hash IS NULL "  # never attempted
                "    OR (qt.question_zh = '' AND qt.is_dead = 0)"  # placeholder, retryable
                "  )"
            )
            if limit is not None:
                sql += f" LIMIT {int(limit)}"
            rows = con.execute(sql).fetchall()
        finally:
            con.close()

        return [(question_hash_for(q), q) for (q,) in rows]

    def translated_count(self) -> int:
        """Count of LIVE translations (is_dead=0). Used by sample-first guard.

        Returns 0 when the table is empty / fresh — the CLI uses this to decide
        whether to enforce the sample-first prompt.
        """
        con = sqlite3.connect(f"file:{self._db_path}?mode=ro", uri=True)
        try:
            row = con.execute(
                "SELECT COUNT(*) FROM question_translations WHERE is_dead = 0"
            ).fetchone()
        finally:
            con.close()
        return int(row[0]) if row else 0

    def count_dead(self) -> int:
        """Count of permanently-failed translations (is_dead=1)."""
        con = sqlite3.connect(f"file:{self._db_path}?mode=ro", uri=True)
        try:
            row = con.execute(
                "SELECT COUNT(*) FROM question_translations WHERE is_dead = 1"
            ).fetchone()
        finally:
            con.close()
        return int(row[0]) if row else 0

    def stats(self) -> list[dict]:
        """GROUP BY translator_model — accumulated token / count / time bounds.

        Used by ``make translation-stats``. Excludes dead translations.
        """
        con = sqlite3.connect(f"file:{self._db_path}?mode=ro", uri=True)
        try:
            rows = con.execute(
                "SELECT translator_model, "
                "       COUNT(*) AS n_questions, "
                "       SUM(token_cost) AS total_tokens, "
                "       MIN(translated_at_ms) AS first_at, "
                "       MAX(translated_at_ms) AS last_at "
                "FROM question_translations "
                "WHERE is_dead = 0 "
                "GROUP BY translator_model"
            ).fetchall()
        finally:
            con.close()

        return [
            {
                "translator_model": m,
                "n_questions": int(n or 0),
                "total_tokens": int(t or 0),
                "first_at_ms": int(f) if f is not None else None,
                "last_at_ms": int(la) if la is not None else None,
            }
            for (m, n, t, f, la) in rows
        ]

    # ── Write ────────────────────────────────────────────────────────────────

    def upsert_batch(self, rows: list[TranslationRow]) -> int:
        """INSERT OR IGNORE one batch of translation results.

        Idempotent on question_hash PK — repeating the same hash is a no-op.
        Wrapped in BEGIN IMMEDIATE so a partial failure rolls back cleanly.

        Returns the number of NEWLY inserted rows (rowcount semantics).
        """
        if not rows:
            return 0

        con = sqlite3.connect(self._db_path, isolation_level=None)
        try:
            con.execute("PRAGMA journal_mode=WAL")
            con.execute("PRAGMA synchronous=NORMAL")
            con.execute("BEGIN IMMEDIATE")
            try:
                tuples = [
                    (
                        r.question_hash,
                        r.question_en,
                        r.question_zh,
                        r.translator_model,
                        r.translated_at_ms,
                        r.token_cost,
                    )
                    for r in rows
                ]
                cur = con.executemany(_UPSERT_SQL, tuples)
                inserted = cur.rowcount if cur.rowcount is not None else 0
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                logger.exception("TranslationCache.upsert_batch rolled back")
                raise
        finally:
            con.close()

        return int(inserted)

    def increment_retry(self, question_hashes: list[str]) -> None:
        """Increment retry_count for the given hashes; mark is_dead=1 when > 3.

        For hashes that don't yet have a row in question_translations (e.g.
        the FIRST batch failed before any successful insert), we INSERT a
        retry-tracking placeholder with empty zh — schema requires zh NOT NULL,
        so we use empty string + retry_count tracked anyway. This keeps the
        retry counter durable across runs.
        """
        if not question_hashes:
            return

        # We need question_en to insert a placeholder row, but increment_retry
        # is called on hashes from list_untranslated which returns (hash, en).
        # The translator passes hashes only — to keep the API tight, we update
        # ONLY existing rows here. The first-batch-failure case is handled by
        # the translator inserting a placeholder via upsert_batch with
        # question_zh="" before increment_retry.
        #
        # Simpler design: increment_retry only updates; upsert_batch creates
        # the placeholder. But we don't yet have the placeholder. So: we
        # compose an "INSERT OR IGNORE placeholder + UPDATE retry" pair driven
        # by translator passing (hash, en) pairs. To avoid breaking the API,
        # accept hashes here and assume rows exist; translator MUST insert
        # placeholders before calling. See translator.translate_pending docs.

        con = sqlite3.connect(self._db_path, isolation_level=None)
        try:
            con.execute("BEGIN IMMEDIATE")
            try:
                placeholders = ",".join("?" for _ in question_hashes)
                con.execute(
                    "UPDATE question_translations "
                    "SET retry_count = retry_count + 1, "
                    "    is_dead = CASE WHEN retry_count + 1 > 3 THEN 1 ELSE 0 END "
                    f"WHERE question_hash IN ({placeholders})",
                    question_hashes,
                )
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                logger.exception("TranslationCache.increment_retry rolled back")
                raise
        finally:
            con.close()

    def insert_retry_placeholders(
        self, pending: list[tuple[str, str]], translator_model: str
    ) -> None:
        """Ensure each (hash, en) has a row so increment_retry can update it.

        Called by translator BEFORE attempting a batch — establishes a durable
        retry counter for the batch. INSERT OR IGNORE means previously-translated
        rows are untouched (idempotent).
        """
        if not pending:
            return

        now_ms = int(time.time() * 1000)
        con = sqlite3.connect(self._db_path, isolation_level=None)
        try:
            con.execute("BEGIN IMMEDIATE")
            try:
                tuples = [(h, en, "", translator_model, now_ms, 0) for (h, en) in pending]
                con.executemany(_UPSERT_SQL, tuples)
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                logger.exception("TranslationCache.insert_retry_placeholders rolled back")
                raise
        finally:
            con.close()
