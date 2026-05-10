"""Batch translation orchestrator with TWO-PATH error handling.

Two error classes — they map to dramatically different downstream behavior:

    ConfigError (FATAL — caller MUST exit 1 if standalone CLI):
      - openai.AuthenticationError       (bad api_key)
      - openai.NotFoundError             (model name wrong)
      - openai.BadRequestError where the .code is one of
        {"invalid_api_key", "model_not_found"}.
      - pydantic.ValidationError when TranslationConfig() itself can't load
        (.env missing / wrong types) — caller catches at construction time.

    TransientError (RECOVERABLE — caller logs WARNING, retry_count++ in cache):
      - openai.APIConnectionError, APITimeoutError, RateLimitError
        (post-SDK-retry — SDK has already done max_retries=3),
      - openai.InternalServerError,
      - BadRequestError where .code is NOT a config code (prompt-level error),
      - ValueError (count mismatch, empty content, JSONDecodeError).

Why this split matters:

    The snapshot orchestrator runs translate_pending as a SIDECAR step (8/8) —
    a translation failure must NOT flip is_valid on the snapshot. But:
      * config errors are user-actionable: the .env is misconfigured. Standalone
        CLI must exit 1 so the user sees something is wrong; orchestrator records
        translation_skipped_reason="config_invalid" but keeps is_valid=True.
      * transient errors are not user-actionable: log a WARNING, increment
        retry_count, and let the next run try again. After 3 retries, is_dead=1
        and the question is permanently skipped.

The shared `_phase` context manager (snapshot.orchestrator._phase) is reused
to keep timing log conventions consistent.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from loguru import logger
from openai import (
    APIConnectionError,
    APITimeoutError,
    AuthenticationError,
    BadRequestError,
    InternalServerError,
    NotFoundError,
    RateLimitError,
)

from tqdm import tqdm

from polyarb.translation.cache import (
    TranslationCache,
    TranslationRow,
    question_hash_for,
)
from polyarb.translation.client import TranslationClient
from polyarb.translation.config import TranslationConfig


class ConfigError(Exception):
    """Raised when translation config (api_key/model/base_url) is invalid.

    Standalone CLI MUST exit 1 (user can fix .env). Snapshot orchestrator
    records translation_skipped_reason='config_invalid' but does NOT fail
    the snapshot — translation is a sidecar.
    """


class TransientError(Exception):
    """Raised when translation hits a recoverable error after SDK retries.

    Caller logs WARNING; cache.increment_retry handles dead-letter accumulation
    (is_dead=1 when retry_count > 3).
    """


# BadRequestError codes that should map to ConfigError. Other BadRequest codes
# (e.g. "invalid_request_error" with a prompt-level diagnostic) stay transient.
_CONFIG_ERROR_BAD_REQUEST_CODES = frozenset({"invalid_api_key", "model_not_found"})


@dataclass
class TranslateSummary:
    """Aggregate result of one translate_pending run."""

    translated: int
    skipped: int
    dead: int
    total_tokens: int


def _badrequest_code(e: BadRequestError) -> str | None:
    """Extract the .code field from a BadRequestError (handles both shapes).

    OpenAI SDK exposes .code directly; for OpenAI-compat services the code may
    only be present in .body['error']['code']. Handle both.
    """
    code = getattr(e, "code", None)
    if code:
        return str(code)
    body = getattr(e, "body", None)
    if isinstance(body, dict):
        err = body.get("error")
        if isinstance(err, dict):
            inner = err.get("code")
            if inner:
                return str(inner)
    return None


async def translate_pending(
    cfg: TranslationConfig,
    db_path: Path,
    sample_limit: int | None = None,
) -> TranslateSummary:
    """Translate every untranslated market question, batched.

    The flow per batch:
        1. cache.insert_retry_placeholders — establishes the durable retry
           counter row (with empty zh) so increment_retry has a row to bump.
        2. client.translate_batch — calls the LLM with concurrency-gated SDK.
        3. On success: cache.upsert_batch overwrites the placeholder via PK
           collision (INSERT OR IGNORE → no-op since the placeholder already
           exists, BUT we then run an UPDATE on translated rows). NOTE:
           because INSERT OR IGNORE skips existing PK, we use a separate
           UPDATE for the success path.
        4. On ConfigError: bail immediately (re-raise to the caller).
        5. On TransientError: cache.increment_retry on this batch's hashes,
           summary.skipped += len(batch).

    Sample-first guard is the CALLER's responsibility (see cli_translation.py);
    this function will translate whatever cache.list_untranslated returns.
    """
    cache = TranslationCache(db_path)

    pending = cache.list_untranslated(limit=sample_limit)
    if not pending:
        logger.info("translate_pending: nothing to do (cache up to date)")
        return TranslateSummary(translated=0, skipped=0, dead=cache.count_dead(), total_tokens=0)

    summary = TranslateSummary(translated=0, skipped=0, dead=0, total_tokens=0)

    async with TranslationClient(
        base_url=cfg.api_base,
        api_key=cfg.secret_api_key(),
        model=cfg.model,
        max_concurrency=cfg.max_concurrency,
        max_retries=cfg.max_retries,
        request_timeout_s=cfg.request_timeout_s,
    ) as client:
        # Slice pending into batches of cfg.batch_size.
        batches: list[list[tuple[str, str]]] = [
            pending[i : i + cfg.batch_size]
            for i in range(0, len(pending), cfg.batch_size)
        ]

        pbar = tqdm(
            total=len(pending),
            desc="translating",
            unit="q",
            dynamic_ncols=True,
        )

        for batch in batches:
            hashes = [h for h, _ in batch]
            ens = [en for _, en in batch]

            try:
                result = await client.translate_batch(ens)
            except (AuthenticationError, NotFoundError) as e:
                raise ConfigError(
                    f"invalid translation config (auth/model): {e}"
                ) from e
            except BadRequestError as e:
                code = _badrequest_code(e)
                if code in _CONFIG_ERROR_BAD_REQUEST_CODES:
                    raise ConfigError(
                        f"bad request (config invalid, code={code}): {e}"
                    ) from e
                # Non-config BadRequest → transient, retry++
                logger.warning(
                    f"batch BadRequest (non-config, code={code}): {e!r}"
                )
                cache.insert_retry_placeholders(batch, translator_model=cfg.model)
                cache.increment_retry(hashes)
                summary.skipped += len(batch)
                pbar.update(len(batch))
                pbar.set_postfix(ok=summary.translated, skip=summary.skipped)
                continue
            except (
                APIConnectionError,
                APITimeoutError,
                RateLimitError,
                InternalServerError,
            ) as e:
                logger.warning(f"batch transient ({type(e).__name__}): {e!r}")
                cache.insert_retry_placeholders(batch, translator_model=cfg.model)
                cache.increment_retry(hashes)
                summary.skipped += len(batch)
                pbar.update(len(batch))
                pbar.set_postfix(ok=summary.translated, skip=summary.skipped)
                continue
            except ValueError as e:
                # Schema/parse error from translate_batch (empty content,
                # count mismatch, JSONDecodeError) — transient.
                logger.warning(f"batch parse error: {e!r}")
                cache.insert_retry_placeholders(batch, translator_model=cfg.model)
                cache.increment_retry(hashes)
                summary.skipped += len(batch)
                pbar.update(len(batch))
                pbar.set_postfix(ok=summary.translated, skip=summary.skipped)
                continue

            # Success path: write the real zh translations.
            import time as _time

            now_ms = int(_time.time() * 1000)
            tokens = result.prompt_tokens + result.completion_tokens
            rows = [
                TranslationRow(
                    question_hash=question_hash_for(en),
                    question_en=en,
                    question_zh=zh,
                    translator_model=cfg.model,
                    translated_at_ms=now_ms,
                    token_cost=tokens // max(len(batch), 1),  # per-question avg
                )
                for en, zh in zip(ens, result.translations, strict=True)
            ]
            # First-time inserts; if a placeholder existed (rare path: the
            # SAME question came up in a prior failed batch), its PK collides
            # and INSERT OR IGNORE leaves the dead row alone — but for the
            # success path we WANT to overwrite the placeholder. So we pre-clean.
            _overwrite_placeholders(cache, rows)
            summary.translated += len(rows)
            summary.total_tokens += tokens
            pbar.update(len(rows))
            pbar.set_postfix(ok=summary.translated, skip=summary.skipped)

    pbar.close()
    summary.dead = cache.count_dead()
    return summary


def _overwrite_placeholders(
    cache: TranslationCache, rows: list[TranslationRow]
) -> None:
    """Force-set the (zh, model, translated_at_ms, token_cost) for each row.

    upsert_batch's INSERT OR IGNORE preserves any prior placeholder (with
    empty zh and retry_count>0) — but on the success path we want the real
    zh to win. Use an UPDATE that ALSO resets retry_count + is_dead so a
    previously-failed question that just succeeded becomes alive.
    """
    if not rows:
        return
    import sqlite3

    con = sqlite3.connect(cache.db_path, isolation_level=None)
    try:
        con.execute("BEGIN IMMEDIATE")
        try:
            # Try INSERT OR IGNORE first (covers the new-row path)
            from polyarb.translation.cache import _UPSERT_SQL  # type: ignore[attr-defined]

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
            con.executemany(_UPSERT_SQL, tuples)
            # Then UPDATE for the placeholder-overwrite path
            for r in rows:
                con.execute(
                    "UPDATE question_translations "
                    "SET question_zh = ?, "
                    "    translator_model = ?, "
                    "    translated_at_ms = ?, "
                    "    token_cost = ?, "
                    "    retry_count = 0, "
                    "    is_dead = 0 "
                    "WHERE question_hash = ? AND question_zh = ''",
                    (
                        r.question_zh,
                        r.translator_model,
                        r.translated_at_ms,
                        r.token_cost,
                        r.question_hash,
                    ),
                )
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
    finally:
        con.close()
