"""OpenAI-compatible translation client (AsyncOpenAI + Semaphore).

Pattern (analog: src/polyarb/clients/gamma_client.py):
    Long-lived AsyncOpenAI instance owned for the duration of one batch run.
    Concurrency is gated by an asyncio.Semaphore (CONTEXT.md max_concurrency=10
    default, configurable via TRANSLATION_MAX_CONCURRENCY).

Key invariants (from RESEARCH §1):

    Pitfall 1 — NEVER wrap AsyncOpenAI with tenacity. The SDK has built-in
        max_retries=3 with exponential backoff for 408/409/429/5xx; layering
        tenacity on top multiplies the retry count and explodes wait time.
        This module's `from openai import AsyncOpenAI` is the ONLY retry layer.

    RESEARCH §1.2 #1 — SYSTEM_PROMPT must contain the literal "JSON". DeepSeek's
        json_object response_format silently rejects the request when the prompt
        omits this keyword (BadRequestError). The constant below is asserted by
        unit test test_system_prompt_contains_json_keyword.

    RESEARCH §1.2 #2 — max_tokens=6000 covers 20 questions × 300 token zh budget.
        DeepSeek can truncate to invalid JSON when max_tokens is too tight.

    RESEARCH §1.5 — request timeout 30s + connect 5s; default 600s causes the
        whole concurrency slot pool to lock up on a network hiccup.

    Pitfall 6 — supports `async with TranslationClient(...) as t:` so resource
        cleanup is automatic even if a batch raises mid-flight.

API key handling (T-01.1-04):
    The constructor takes a plain str (the caller already unwrapped SecretStr
    via TranslationConfig.secret_api_key()). The startup info log prints model
    + base_url ONLY — never the api_key.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass

import httpx
from loguru import logger
from openai import (
    AsyncOpenAI,
    AuthenticationError,
    BadRequestError,
    NotFoundError,
)


@dataclass
class TranslationResult:
    """One batch's worth of zh translations + token usage from server."""

    translations: list[str]
    prompt_tokens: int
    completion_tokens: int


class TranslationClient:
    """OpenAI-compatible async batch translator.

    Lifecycle:
        async with TranslationClient(base_url=..., api_key=..., model=...) as t:
            result = await t.translate_batch(["q1", "q2", ...])

    `translate_batch` raises:
        - AuthenticationError / NotFoundError / BadRequestError → caller maps
          to ConfigError (translator.py)
        - APIConnectionError / APITimeoutError / RateLimitError /
          InternalServerError → after SDK exhausts max_retries, propagates and
          caller maps to TransientError (cache retry++)
        - ValueError → empty content / count mismatch / JSON decode failure
          (caller maps to TransientError)
    """

    # NB: the literal substring "JSON" is REQUIRED for DeepSeek json_object mode
    # (RESEARCH §1.2 #1). Removing it silently breaks DeepSeek; unit-tested.
    SYSTEM_PROMPT = (
        "You are a translator for Polymarket prediction-market questions. "
        "Translate each English question to Simplified Chinese. "
        "Preserve date hedges (e.g. 'by April 30, 2026'), conditional language "
        "('according to official data'), and proper nouns verbatim. "
        'Return a JSON object: {"translations": ["zh1", "zh2", ...]} '
        "preserving the input array order exactly."
    )

    def __init__(
        self,
        base_url: str,
        api_key: str,
        model: str,
        max_concurrency: int = 10,
        max_retries: int = 3,
        request_timeout_s: float = 30.0,
    ) -> None:
        self._client = AsyncOpenAI(
            base_url=base_url,
            api_key=api_key,
            max_retries=max_retries,
            # Default 600s would lock all max_concurrency slots on one hung request.
            timeout=httpx.Timeout(request_timeout_s, connect=5.0),
        )
        self._model = model
        self._sem = asyncio.Semaphore(max_concurrency)
        self._closed = False
        # T-01.1-04: log model + base_url ONLY. Do NOT log api_key.
        logger.info(
            f"translator init: model={model} base_url={base_url} "
            f"max_concurrency={max_concurrency} max_retries={max_retries} "
            f"timeout={request_timeout_s}s"
        )

    async def aclose(self) -> None:
        """Close the underlying HTTP client. Idempotent."""
        if self._closed:
            return
        await self._client.close()
        self._closed = True

    async def __aenter__(self) -> TranslationClient:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.aclose()

    async def translate_batch(self, questions: list[str]) -> TranslationResult:
        """Translate one batch of questions → zh array of identical length.

        Raises:
            AuthenticationError / NotFoundError / BadRequestError — config issue,
                caller MUST NOT retry; map to ConfigError.
            APIConnectionError / APITimeoutError / RateLimitError /
            InternalServerError — transient, SDK already retried max_retries
                times before propagating; caller maps to TransientError.
            ValueError — schema/parse error (empty content, count mismatch,
                JSONDecodeError); caller maps to TransientError.
        """
        if not questions:
            return TranslationResult(translations=[], prompt_tokens=0, completion_tokens=0)

        async with self._sem:
            try:
                resp = await self._client.chat.completions.create(
                    model=self._model,
                    response_format={"type": "json_object"},
                    messages=[
                        {"role": "system", "content": self.SYSTEM_PROMPT},
                        {
                            "role": "user",
                            "content": json.dumps({"questions": questions}, ensure_ascii=False),
                        },
                    ],
                    max_tokens=6000,
                    temperature=0.0,
                )
            except (AuthenticationError, NotFoundError):
                # ConfigError surface — let translator.py classify.
                raise
            except BadRequestError:
                # Most BadRequestError instances here mean "model name wrong" or
                # "missing 'json' keyword" — both config-level. translator.py
                # decides config vs transient by inspecting the .code field.
                raise

        # Parse the JSON object response (response_format guarantees it's an object).
        content = resp.choices[0].message.content or ""
        if not content.strip():
            # DeepSeek edge case (RESEARCH §1.2 #3): occasional empty content.
            raise ValueError("translation returned empty content (DeepSeek edge case)")

        try:
            parsed = json.loads(content)
        except json.JSONDecodeError as e:
            raise ValueError(f"translation returned invalid JSON: {e}") from e

        translations = parsed.get("translations")
        if not isinstance(translations, list):
            raise ValueError(
                f"translation response missing 'translations' array; got keys: "
                f"{list(parsed.keys())}"
            )
        if len(translations) != len(questions):
            raise ValueError(
                f"translation count mismatch: got {len(translations)}, expected {len(questions)}"
            )

        usage = resp.usage
        prompt_tokens = int(usage.prompt_tokens) if usage and usage.prompt_tokens else 0
        completion_tokens = int(usage.completion_tokens) if usage and usage.completion_tokens else 0

        return TranslationResult(
            translations=[str(t) for t in translations],
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
        )
