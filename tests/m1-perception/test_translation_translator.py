"""translator.translate_pending — two-path error handling + retry semantics.

Strategy: patch ``polyarb.translation.translator.TranslationClient`` to
inject controlled responses without touching network. The cache is a real
SQLite db on tmp_path so retry counters / token costs / dead semantics
are observed end-to-end.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from openai import (
    APIConnectionError,
    AuthenticationError,
    BadRequestError,
    NotFoundError,
)
from pydantic import SecretStr
from typer.testing import CliRunner

from polyarb.cli_translation import app as cli_app
from polyarb.translation.cache import (
    TranslationCache,
    TranslationRow,
    question_hash_for,
)
from polyarb.translation.client import TranslationResult
from polyarb.translation.config import TranslationConfig
from polyarb.translation.translator import (
    ConfigError,
    TranslateSummary,
    translate_pending,
)


def _mk_request() -> httpx.Request:
    return httpx.Request("POST", "https://api.example.com/v1/chat/completions")


def _mk_response(status_code: int = 401, body: dict | None = None) -> httpx.Response:
    return httpx.Response(
        status_code=status_code,
        request=_mk_request(),
        json=body or {"error": {"code": "test"}},
    )


@pytest.fixture
def cache_with_3_markets(tmp_path: Path) -> TranslationCache:
    """Real SQLite db with 3 untranslated market rows + a snapshots parent."""
    cache = TranslationCache(tmp_path / "cache.db")
    cache.init_schema()
    con = sqlite3.connect(cache.db_path, isolation_level=None)
    try:
        con.execute(
            "INSERT INTO snapshots(taken_at_ms, finished_at_ms, mode, "
            "market_count, is_valid, parquet_path) VALUES (?,?,?,?,?,?)",
            (1, 2, "subset", 3, 1, "/tmp/x.parquet"),
        )
        sid = con.execute("SELECT last_insert_rowid()").fetchone()[0]
        for i, q in enumerate(["Will Trump win 2024?", "Will BTC hit 100k?", "Rain in Paris?"]):
            con.execute(
                "INSERT INTO markets(market_id, condition_id, question, "
                "fetched_at_ms, snapshot_id) VALUES (?,?,?,?,?)",
                (f"M{i}", f"C{i}", q, 100 + i, sid),
            )
    finally:
        con.close()
    return cache


def _cfg(model: str = "test-model", batch_size: int = 20) -> TranslationConfig:
    """Build a TranslationConfig directly (bypassing env loading)."""
    return TranslationConfig(
        api_base="https://api.example.com/v1",
        api_key=SecretStr("sk-test"),
        model=model,
        max_concurrency=1,
        batch_size=batch_size,
        max_retries=3,
        request_timeout_s=10.0,
    )


@pytest.fixture
def patched_client():
    """Patch TranslationClient at the translator's import site.

    Yields a MagicMock for the client *instance* so each test can set
    `instance.translate_batch.side_effect = ...` (it's an AsyncMock).
    """
    with patch("polyarb.translation.translator.TranslationClient") as ClassMock:
        instance = MagicMock()
        instance.translate_batch = AsyncMock()
        instance.aclose = AsyncMock()
        instance.__aenter__ = AsyncMock(return_value=instance)
        instance.__aexit__ = AsyncMock(return_value=None)
        ClassMock.return_value = instance
        yield instance


# ─────────────────────────────────────────────────────────────────────────────
# Success path
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_translate_pending_persists_token_cost(
    cache_with_3_markets: TranslationCache, patched_client
) -> None:
    """Successful batch → translations + tokens land in cache."""
    patched_client.translate_batch.return_value = TranslationResult(
        translations=["特朗普赢吗", "BTC到10万", "巴黎下雨"],
        prompt_tokens=60,
        completion_tokens=30,
    )

    summary = await translate_pending(_cfg(), cache_with_3_markets.db_path, sample_limit=None)

    assert summary.translated == 3
    assert summary.skipped == 0
    assert summary.dead == 0
    assert summary.total_tokens == 90  # 60 + 30

    # Verify the rows actually have the zh + token_cost
    con = sqlite3.connect(cache_with_3_markets.db_path)
    try:
        rows = con.execute(
            "SELECT question_zh, token_cost, retry_count, is_dead "
            "FROM question_translations ORDER BY question_en"
        ).fetchall()
    finally:
        con.close()
    assert len(rows) == 3
    for zh, tokens, retry, dead in rows:
        assert zh != ""
        assert tokens > 0
        assert retry == 0
        assert dead == 0


@pytest.mark.asyncio
async def test_translate_pending_sample_limit(
    cache_with_3_markets: TranslationCache, patched_client
) -> None:
    """sample_limit=2 → only 2 questions translated."""
    patched_client.translate_batch.return_value = TranslationResult(
        translations=["甲", "乙"], prompt_tokens=20, completion_tokens=10
    )

    summary = await translate_pending(_cfg(), cache_with_3_markets.db_path, sample_limit=2)

    assert summary.translated == 2
    # The 3rd market is still untranslated
    assert cache_with_3_markets.translated_count() == 2


@pytest.mark.asyncio
async def test_translate_pending_skips_already_translated(
    cache_with_3_markets: TranslationCache, patched_client
) -> None:
    """Pre-translated questions are NOT re-translated."""
    cache_with_3_markets.upsert_batch(
        [
            TranslationRow(
                question_hash=question_hash_for("Will Trump win 2024?"),
                question_en="Will Trump win 2024?",
                question_zh="特朗普会赢吗",
                translator_model="prev-model",
                translated_at_ms=999,
                token_cost=10,
            )
        ]
    )

    patched_client.translate_batch.return_value = TranslationResult(
        translations=["BTC到10万", "巴黎下雨"],
        prompt_tokens=20,
        completion_tokens=10,
    )

    summary = await translate_pending(_cfg(), cache_with_3_markets.db_path)

    assert summary.translated == 2  # only 2 new
    # Verify the pre-existing translation was NOT clobbered
    con = sqlite3.connect(cache_with_3_markets.db_path)
    try:
        zh = con.execute(
            "SELECT question_zh FROM question_translations "
            "WHERE question_en = 'Will Trump win 2024?'"
        ).fetchone()[0]
    finally:
        con.close()
    assert zh == "特朗普会赢吗"  # unchanged


@pytest.mark.asyncio
async def test_translate_pending_empty_db_returns_zero(tmp_path: Path) -> None:
    """No untranslated markets → no client calls, summary all zero."""
    cache = TranslationCache(tmp_path / "empty.db")
    cache.init_schema()

    summary = await translate_pending(_cfg(), cache.db_path)
    assert summary.translated == 0
    assert summary.skipped == 0


# ─────────────────────────────────────────────────────────────────────────────
# ConfigError mapping
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_translate_pending_raises_config_error_on_auth(
    cache_with_3_markets: TranslationCache, patched_client
) -> None:
    patched_client.translate_batch.side_effect = AuthenticationError(
        message="bad key", response=_mk_response(401), body=None
    )

    with pytest.raises(ConfigError):
        await translate_pending(_cfg(), cache_with_3_markets.db_path)


@pytest.mark.asyncio
async def test_translate_pending_raises_config_error_on_invalid_model(
    cache_with_3_markets: TranslationCache, patched_client
) -> None:
    patched_client.translate_batch.side_effect = NotFoundError(
        message="model not found", response=_mk_response(404), body=None
    )

    with pytest.raises(ConfigError):
        await translate_pending(_cfg(), cache_with_3_markets.db_path)


@pytest.mark.asyncio
async def test_translate_pending_raises_config_error_on_invalid_api_key_badrequest(
    cache_with_3_markets: TranslationCache, patched_client
) -> None:
    """BadRequestError with code='invalid_api_key' → ConfigError."""
    patched_client.translate_batch.side_effect = BadRequestError(
        message="invalid api key",
        response=_mk_response(400),
        body={"error": {"code": "invalid_api_key"}},
    )

    with pytest.raises(ConfigError):
        await translate_pending(_cfg(), cache_with_3_markets.db_path)


# ─────────────────────────────────────────────────────────────────────────────
# Transient path — does NOT raise; cache.retry_count++
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_translate_pending_does_not_raise_on_transient(
    cache_with_3_markets: TranslationCache, patched_client
) -> None:
    """APIConnectionError → retry++ on this batch's hashes, summary.skipped += N."""
    patched_client.translate_batch.side_effect = APIConnectionError(request=_mk_request())

    summary = await translate_pending(_cfg(), cache_with_3_markets.db_path)
    # All 3 markets in one batch (batch_size 20) → all 3 retried.
    assert summary.translated == 0
    assert summary.skipped == 3

    con = sqlite3.connect(cache_with_3_markets.db_path)
    try:
        rows = con.execute("SELECT retry_count, is_dead FROM question_translations").fetchall()
    finally:
        con.close()
    assert len(rows) == 3
    # First failure increments retry_count to 1
    assert all(r[0] == 1 for r in rows)
    assert all(r[1] == 0 for r in rows)


@pytest.mark.asyncio
async def test_translate_pending_marks_dead_after_4_retries(
    cache_with_3_markets: TranslationCache, patched_client
) -> None:
    """Run translate_pending 4 times with persistent transient errors → is_dead=1."""
    patched_client.translate_batch.side_effect = APIConnectionError(request=_mk_request())

    for _ in range(4):
        await translate_pending(_cfg(), cache_with_3_markets.db_path)

    con = sqlite3.connect(cache_with_3_markets.db_path)
    try:
        rows = con.execute("SELECT retry_count, is_dead FROM question_translations").fetchall()
    finally:
        con.close()
    # After 4 increments, retry_count=4 and is_dead=1
    assert all(r[0] == 4 for r in rows)
    assert all(r[1] == 1 for r in rows)


@pytest.mark.asyncio
async def test_translate_pending_does_not_raise_on_value_error(
    cache_with_3_markets: TranslationCache, patched_client
) -> None:
    """ValueError (count mismatch / parse error) → transient, retry++."""
    patched_client.translate_batch.side_effect = ValueError("count mismatch")

    summary = await translate_pending(_cfg(), cache_with_3_markets.db_path)
    assert summary.skipped == 3


@pytest.mark.asyncio
async def test_translate_pending_non_config_badrequest_is_transient(
    cache_with_3_markets: TranslationCache, patched_client
) -> None:
    """BadRequestError without config code → transient (not ConfigError)."""
    patched_client.translate_batch.side_effect = BadRequestError(
        message="prompt issue",
        response=_mk_response(400),
        body={"error": {"code": "prompt_too_long"}},
    )

    summary = await translate_pending(_cfg(), cache_with_3_markets.db_path)
    # Did NOT raise; treated as transient
    assert summary.skipped == 3


# ─────────────────────────────────────────────────────────────────────────────
# CLI tests (cli_translation app) — sample-first guard + ConfigError handling
# ─────────────────────────────────────────────────────────────────────────────


runner = CliRunner(mix_stderr=False)


def _setup_cli_settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Make load_settings() resolve to tmp_path/state.db.

    Sets POLYARB_DB_PATH (env-overridable Settings field) so the CLI uses our
    tmp DB instead of the project-default data/state.db.
    """
    db = tmp_path / "state.db"
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("POLYARB_DB_PATH", str(db))
    monkeypatch.setenv("POLYARB_PARQUET_ROOT", str(tmp_path / "snapshots"))
    monkeypatch.setenv("POLYARB_CACHE_ROOT", str(tmp_path / ".cache"))
    return db


def test_cli_first_run_without_force_full_exits_1(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Empty cache + no --force-full → exit 1 with sample-first hint."""
    _setup_cli_settings(tmp_path, monkeypatch)
    # Strip TRANSLATION_* env so we don't even reach config loading
    for k in list(__import__("os").environ.keys()):
        if k.startswith("TRANSLATION_"):
            monkeypatch.delenv(k, raising=False)

    result = runner.invoke(cli_app, ["translate-pending"])
    assert result.exit_code == 1
    assert "first run detected" in result.stderr
    assert "translate-pending-sample" in result.stderr


def test_cli_first_run_with_force_full_proceeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Empty cache + --force-full → guard bypassed, translate_pending runs.

    With no real translations to do (empty markets table), translate_pending
    returns a zero-summary and the CLI exits 0.
    """
    _setup_cli_settings(tmp_path, monkeypatch)
    monkeypatch.setenv("TRANSLATION_API_BASE", "https://api.example.com/v1")
    monkeypatch.setenv("TRANSLATION_API_KEY", "sk-test")
    monkeypatch.setenv("TRANSLATION_MODEL", "test-model")

    # Patch translate_pending to verify it WAS called.
    async def fake_pending(cfg, db_path, sample_limit=None):
        return TranslateSummary(translated=0, skipped=0, dead=0, total_tokens=0)

    with patch(
        "polyarb.cli_translation.translate_pending",
        side_effect=fake_pending,
    ) as mock_pending:
        result = runner.invoke(cli_app, ["translate-pending", "--force-full"])

    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert mock_pending.called
    assert "OK | translated=0" in result.stdout


def test_cli_with_limit_does_not_trigger_first_run_guard(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """--limit N is the sample-first path; guard does not fire."""
    _setup_cli_settings(tmp_path, monkeypatch)
    monkeypatch.setenv("TRANSLATION_API_BASE", "https://api.example.com/v1")
    monkeypatch.setenv("TRANSLATION_API_KEY", "sk-test")
    monkeypatch.setenv("TRANSLATION_MODEL", "test-model")

    async def fake_pending(cfg, db_path, sample_limit=None):
        return TranslateSummary(translated=0, skipped=0, dead=0, total_tokens=0)

    with patch(
        "polyarb.cli_translation.translate_pending",
        side_effect=fake_pending,
    ):
        result = runner.invoke(cli_app, ["translate-pending", "--limit", "10"])

    assert result.exit_code == 0
    assert "first run detected" not in result.stderr


def test_cli_config_error_exits_1(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """ConfigError from translate_pending → exit 1 + .env hint on stderr."""
    _setup_cli_settings(tmp_path, monkeypatch)
    monkeypatch.setenv("TRANSLATION_API_BASE", "https://api.example.com/v1")
    monkeypatch.setenv("TRANSLATION_API_KEY", "sk-bad")
    monkeypatch.setenv("TRANSLATION_MODEL", "test-model")

    async def fake_pending(cfg, db_path, sample_limit=None):
        raise ConfigError("simulated bad api_key")

    with patch(
        "polyarb.cli_translation.translate_pending",
        side_effect=fake_pending,
    ):
        result = runner.invoke(cli_app, ["translate-pending", "--force-full"])

    assert result.exit_code == 1
    assert "translation config error" in result.stderr
    assert "TRANSLATION_API_KEY" in result.stderr


def test_cli_validation_error_exits_1(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing TRANSLATION_API_BASE → ValidationError → exit 1."""
    _setup_cli_settings(tmp_path, monkeypatch)
    # Strip TRANSLATION_* env to force ValidationError
    for k in list(__import__("os").environ.keys()):
        if k.startswith("TRANSLATION_"):
            monkeypatch.delenv(k, raising=False)

    # --force-full bypasses sample-first guard so we reach the config load
    result = runner.invoke(cli_app, ["translate-pending", "--force-full"])
    assert result.exit_code == 1
    assert "TranslationConfig invalid" in result.stderr


def test_cli_translation_stats_empty(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """translation-stats on a fresh cache prints '(no translations yet)'."""
    _setup_cli_settings(tmp_path, monkeypatch)
    result = runner.invoke(cli_app, ["translation-stats"])
    assert result.exit_code == 0
    assert "no translations yet" in result.stdout
