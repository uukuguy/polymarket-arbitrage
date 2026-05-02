"""TranslationCache CRUD tests — append-only invariant + retry counter.

Anti-patterns the cache must NOT exhibit:
  - DELETE FROM question_translations (cumulative, never overwrite)
  - String-interpolated SQL (always parameterized)
"""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import pytest

from polyarb.translation.cache import (
    TranslationCache,
    TranslationRow,
    question_hash_for,
)


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures — fresh schema per test
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def fresh_cache(tmp_path: Path) -> TranslationCache:
    cache = TranslationCache(tmp_path / "cache.db")
    cache.init_schema()
    return cache


@pytest.fixture
def cache_with_3_markets(tmp_path: Path) -> TranslationCache:
    """A cache whose underlying DB has 3 known market questions, no translations yet."""
    cache = TranslationCache(tmp_path / "cache.db")
    cache.init_schema()
    # Insert minimum-required market rows. We must satisfy NOT NULL on
    # market_id, condition_id, fetched_at_ms, snapshot_id. snapshot_id has a
    # FK to snapshots(id) — insert a parent snapshot row first.
    con = sqlite3.connect(cache.db_path, isolation_level=None)
    try:
        con.execute(
            "INSERT INTO snapshots(taken_at_ms, finished_at_ms, mode, "
            "market_count, is_valid, parquet_path) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (1, 2, "subset", 3, 1, "/tmp/x.parquet"),
        )
        snapshot_id = con.execute("SELECT last_insert_rowid()").fetchone()[0]
        for i, q in enumerate(
            [
                "Will Trump win the 2024 election?",
                "Will Bitcoin hit $100k by 2025?",
                "Will it rain in Paris on April 30, 2026?",
            ]
        ):
            con.execute(
                "INSERT INTO markets(market_id, condition_id, question, "
                "fetched_at_ms, snapshot_id) "
                "VALUES (?, ?, ?, ?, ?)",
                (f"M{i}", f"C{i}", q, 100 + i, snapshot_id),
            )
    finally:
        con.close()
    return cache


def _make_row(en: str, zh: str, model: str = "deepseek-chat", tokens: int = 50) -> TranslationRow:
    return TranslationRow(
        question_hash=question_hash_for(en),
        question_en=en,
        question_zh=zh,
        translator_model=model,
        translated_at_ms=int(time.time() * 1000),
        token_cost=tokens,
    )


# ─────────────────────────────────────────────────────────────────────────────
# question_hash_for — pure function
# ─────────────────────────────────────────────────────────────────────────────


def test_question_hash_for_is_stable() -> None:
    h1 = question_hash_for("Will it rain?")
    h2 = question_hash_for("Will it rain?")
    assert h1 == h2


def test_question_hash_for_distinguishes_variants() -> None:
    h1 = question_hash_for("Will it rain?")
    h2 = question_hash_for("Will it rain")  # no question mark
    assert h1 != h2


# ─────────────────────────────────────────────────────────────────────────────
# list_untranslated — read-side correctness
# ─────────────────────────────────────────────────────────────────────────────


def test_list_untranslated_returns_all_when_none_translated(
    cache_with_3_markets: TranslationCache,
) -> None:
    pending = cache_with_3_markets.list_untranslated()
    assert len(pending) == 3
    # Each tuple is (hash, en); hash matches the local computation
    for h, en in pending:
        assert h == question_hash_for(en)


def test_list_untranslated_excludes_translated(
    cache_with_3_markets: TranslationCache,
) -> None:
    cache_with_3_markets.upsert_batch(
        [_make_row("Will Trump win the 2024 election?", "特朗普会赢吗？")]
    )
    pending = cache_with_3_markets.list_untranslated()
    assert len(pending) == 2
    questions_left = {en for _, en in pending}
    assert "Will Trump win the 2024 election?" not in questions_left


def test_list_untranslated_respects_limit(
    cache_with_3_markets: TranslationCache,
) -> None:
    pending = cache_with_3_markets.list_untranslated(limit=2)
    assert len(pending) == 2


def test_list_untranslated_excludes_dead(
    cache_with_3_markets: TranslationCache,
) -> None:
    """is_dead=1 rows must NOT be re-attempted automatically.

    Even though a dead translation has empty zh, the JOIN filter (qt.question_hash
    IS NULL) returns NO row for that question — i.e. the dead question is
    treated as "already attempted, leave it alone".
    """
    # Insert a placeholder + mark dead by incrementing 4 times.
    pending = cache_with_3_markets.list_untranslated()
    cache_with_3_markets.insert_retry_placeholders(
        pending[:1], translator_model="m"
    )
    h = pending[0][0]
    for _ in range(4):
        cache_with_3_markets.increment_retry([h])

    # is_dead=1 now; list_untranslated should exclude it (qt row exists, JOIN
    # finds it, WHERE qt.question_hash IS NULL filters it out)
    after = cache_with_3_markets.list_untranslated()
    after_questions = {en for _, en in after}
    assert pending[0][1] not in after_questions


# ─────────────────────────────────────────────────────────────────────────────
# upsert_batch — write-side idempotence + token cost
# ─────────────────────────────────────────────────────────────────────────────


def test_upsert_batch_persists_token_cost(
    fresh_cache: TranslationCache,
) -> None:
    fresh_cache.upsert_batch(
        [_make_row("q1", "翻1", model="m", tokens=123)]
    )
    con = sqlite3.connect(fresh_cache.db_path)
    try:
        row = con.execute(
            "SELECT token_cost FROM question_translations"
        ).fetchone()
    finally:
        con.close()
    assert row[0] == 123


def test_upsert_batch_idempotent(fresh_cache: TranslationCache) -> None:
    """Re-upserting the same hash is a no-op — INSERT OR IGNORE."""
    row = _make_row("q1", "翻1")
    fresh_cache.upsert_batch([row])
    fresh_cache.upsert_batch([row])
    con = sqlite3.connect(fresh_cache.db_path)
    try:
        n = con.execute("SELECT COUNT(*) FROM question_translations").fetchone()[0]
    finally:
        con.close()
    assert n == 1


def test_upsert_batch_empty_list_is_noop(
    fresh_cache: TranslationCache,
) -> None:
    n = fresh_cache.upsert_batch([])
    assert n == 0


# ─────────────────────────────────────────────────────────────────────────────
# increment_retry — dead-letter behavior
# ─────────────────────────────────────────────────────────────────────────────


def test_increment_retry_marks_dead_after_3(
    fresh_cache: TranslationCache,
) -> None:
    """retry_count goes 0 → 1 → 2 → 3 → 4; on the 4th increment, is_dead=1."""
    fresh_cache.insert_retry_placeholders(
        [(question_hash_for("q1"), "q1")], translator_model="m"
    )
    h = question_hash_for("q1")

    # Three retries: counter at 3, still alive
    for _ in range(3):
        fresh_cache.increment_retry([h])
    con = sqlite3.connect(fresh_cache.db_path)
    try:
        row = con.execute(
            "SELECT retry_count, is_dead FROM question_translations WHERE question_hash = ?",
            (h,),
        ).fetchone()
    finally:
        con.close()
    assert row == (3, 0)

    # Fourth retry: counter goes to 4, is_dead=1
    fresh_cache.increment_retry([h])
    con = sqlite3.connect(fresh_cache.db_path)
    try:
        row = con.execute(
            "SELECT retry_count, is_dead FROM question_translations WHERE question_hash = ?",
            (h,),
        ).fetchone()
    finally:
        con.close()
    assert row == (4, 1)


def test_increment_retry_empty_list_is_noop(
    fresh_cache: TranslationCache,
) -> None:
    fresh_cache.increment_retry([])  # must not raise


# ─────────────────────────────────────────────────────────────────────────────
# translated_count + count_dead + stats — read aggregates
# ─────────────────────────────────────────────────────────────────────────────


def test_translated_count_zero_for_fresh_db(fresh_cache: TranslationCache) -> None:
    """Sample-first guard relies on this returning 0 on a brand-new cache."""
    assert fresh_cache.translated_count() == 0


def test_translated_count_after_upsert(fresh_cache: TranslationCache) -> None:
    fresh_cache.upsert_batch(
        [_make_row(f"q{i}", f"翻{i}") for i in range(5)]
    )
    assert fresh_cache.translated_count() == 5


def test_translated_count_excludes_dead(fresh_cache: TranslationCache) -> None:
    fresh_cache.insert_retry_placeholders(
        [(question_hash_for("q1"), "q1")], translator_model="m"
    )
    h = question_hash_for("q1")
    for _ in range(4):
        fresh_cache.increment_retry([h])
    # dead row exists but is excluded
    assert fresh_cache.translated_count() == 0
    assert fresh_cache.count_dead() == 1


def test_stats_grouped_by_model(fresh_cache: TranslationCache) -> None:
    """Two different translator models → 2 stats rows, each with own counts."""
    fresh_cache.upsert_batch(
        [
            _make_row("a", "甲", model="deepseek-chat", tokens=100),
            _make_row("b", "乙", model="deepseek-chat", tokens=200),
            _make_row("c", "丙", model="qwen-plus", tokens=300),
        ]
    )
    stats = fresh_cache.stats()
    by_model = {row["translator_model"]: row for row in stats}
    assert set(by_model) == {"deepseek-chat", "qwen-plus"}
    assert by_model["deepseek-chat"]["n_questions"] == 2
    assert by_model["deepseek-chat"]["total_tokens"] == 300
    assert by_model["qwen-plus"]["n_questions"] == 1
    assert by_model["qwen-plus"]["total_tokens"] == 300


# ─────────────────────────────────────────────────────────────────────────────
# Append-only invariant — DELETE FROM is forbidden in production code
# ─────────────────────────────────────────────────────────────────────────────


def test_cache_module_does_not_delete_from_question_translations() -> None:
    """Self-invalidating grep gate: the source must never DELETE FROM the cache.

    Excludes commentary / docstrings to avoid the regex matching its own
    explanatory text.
    """
    src = (
        Path(__file__).parent.parent.parent
        / "src"
        / "polyarb"
        / "translation"
        / "cache.py"
    ).read_text()
    code_lines = [
        ln
        for ln in src.splitlines()
        if not ln.strip().startswith("#")
        and not ln.strip().startswith('"')
        and "DELETE" not in ln  # noqa: E501 — we accept that string-literal occurrences elsewhere fail this test
    ]
    # Re-parse: we want the production *executable* code to never contain
    # the literal "DELETE FROM question_translations". docstring text in the
    # module top docstring contains a description of the invariant — exclude
    # those by checking only lines outside triple-quoted blocks.
    in_doc = False
    forbidden = []
    for ln in src.splitlines():
        stripped = ln.strip()
        if stripped.startswith('"""') or stripped.startswith("'''"):
            in_doc = not in_doc
            continue
        if in_doc:
            continue
        if "DELETE FROM question_translations" in ln:
            forbidden.append(ln)
    assert not forbidden, (
        f"cache.py contains DELETE FROM question_translations in production code: "
        f"{forbidden}"
    )
