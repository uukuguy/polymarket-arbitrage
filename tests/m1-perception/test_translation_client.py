"""TranslationClient — JSON parsing + exception classification + lifecycle.

Mocks AsyncOpenAI by patching the ``openai.AsyncOpenAI`` class symbol on
``polyarb.translation.client``. The mock returns a SimpleNamespace shaped
like ChatCompletion(choices=[Choice(message=Message(content=...))]).

Critical invariants tested:
    - SYSTEM_PROMPT contains "JSON" literal (DeepSeek silently rejects without)
    - count mismatch / empty content / JSON decode → ValueError (caller maps to TransientError)
    - AuthenticationError / NotFoundError / BadRequestError propagate (caller maps to ConfigError)
    - aclose() is idempotent
    - async context manager calls aclose on exit
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from openai import AuthenticationError, BadRequestError, NotFoundError

from polyarb.translation.client import TranslationClient, TranslationResult


def _mk_chat_response(content: str, prompt_tokens: int = 30, completion_tokens: int = 25):
    """Shape a mock response that matches what AsyncOpenAI.chat.completions.create returns."""
    msg = SimpleNamespace(content=content)
    choice = SimpleNamespace(message=msg)
    usage = SimpleNamespace(
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        total_tokens=prompt_tokens + completion_tokens,
    )
    return SimpleNamespace(choices=[choice], usage=usage)


def _mk_request() -> httpx.Request:
    """Minimal httpx.Request for openai exceptions that need a request arg."""
    return httpx.Request("POST", "https://api.example.com/v1/chat/completions")


def _mk_response(status_code: int = 401, body: dict | None = None) -> httpx.Response:
    """Minimal httpx.Response for openai exceptions that need a response arg."""
    return httpx.Response(
        status_code=status_code,
        request=_mk_request(),
        json=body or {"error": {"message": "test", "code": "test_code"}},
    )


@pytest.fixture
def mock_openai_class():
    """Patch ``polyarb.translation.client.AsyncOpenAI`` so we never hit network.

    Yields the AsyncMock for the .chat.completions.create method so tests can
    set side_effect / return_value per case.
    """
    with patch("polyarb.translation.client.AsyncOpenAI") as ClassMock:
        instance = MagicMock()
        instance.chat = MagicMock()
        instance.chat.completions = MagicMock()
        instance.chat.completions.create = AsyncMock()
        instance.close = AsyncMock()
        ClassMock.return_value = instance
        yield instance.chat.completions.create, instance


# ─────────────────────────────────────────────────────────────────────────────
# SYSTEM_PROMPT invariant — DeepSeek json_object requires "JSON" keyword
# ─────────────────────────────────────────────────────────────────────────────


def test_system_prompt_contains_json_keyword() -> None:
    """RESEARCH §1.2 #1: DeepSeek BadRequest's prompt without 'json' keyword.

    SYSTEM_PROMPT (uppercased) must contain 'JSON' as a substring.
    """
    assert "JSON" in TranslationClient.SYSTEM_PROMPT.upper(), (
        "SYSTEM_PROMPT missing 'JSON' literal — DeepSeek json_object will reject"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Successful parse path
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_translate_batch_parses_json(mock_openai_class) -> None:
    create_mock, _ = mock_openai_class
    create_mock.return_value = _mk_chat_response(
        json.dumps({"translations": ["甲", "乙"]})
    )

    async with TranslationClient(
        base_url="https://api.example.com/v1",
        api_key="sk-test",
        model="test-model",
    ) as t:
        result = await t.translate_batch(["q1", "q2"])

    assert isinstance(result, TranslationResult)
    assert result.translations == ["甲", "乙"]
    assert result.prompt_tokens == 30
    assert result.completion_tokens == 25


@pytest.mark.asyncio
async def test_translate_batch_empty_input_returns_empty(mock_openai_class) -> None:
    """Empty input → empty result, no API call."""
    create_mock, _ = mock_openai_class

    async with TranslationClient(
        base_url="https://api.example.com/v1",
        api_key="sk-test",
        model="test-model",
    ) as t:
        result = await t.translate_batch([])

    assert result.translations == []
    create_mock.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# Schema-violation paths → ValueError (TransientError downstream)
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_translate_batch_count_mismatch_raises(mock_openai_class) -> None:
    create_mock, _ = mock_openai_class
    create_mock.return_value = _mk_chat_response(
        json.dumps({"translations": ["only one"]})
    )

    async with TranslationClient(
        base_url="https://api.example.com/v1",
        api_key="sk-test",
        model="test-model",
    ) as t:
        with pytest.raises(ValueError, match="count mismatch"):
            await t.translate_batch(["q1", "q2"])


@pytest.mark.asyncio
async def test_translate_batch_empty_content_raises(mock_openai_class) -> None:
    """DeepSeek edge case: API returns empty content."""
    create_mock, _ = mock_openai_class
    create_mock.return_value = _mk_chat_response("")

    async with TranslationClient(
        base_url="https://api.example.com/v1",
        api_key="sk-test",
        model="test-model",
    ) as t:
        with pytest.raises(ValueError, match="empty content"):
            await t.translate_batch(["q1"])


@pytest.mark.asyncio
async def test_translate_batch_invalid_json_raises(mock_openai_class) -> None:
    create_mock, _ = mock_openai_class
    create_mock.return_value = _mk_chat_response("this is not json at all")

    async with TranslationClient(
        base_url="https://api.example.com/v1",
        api_key="sk-test",
        model="test-model",
    ) as t:
        with pytest.raises(ValueError, match="invalid JSON"):
            await t.translate_batch(["q1"])


@pytest.mark.asyncio
async def test_translate_batch_missing_translations_key(mock_openai_class) -> None:
    create_mock, _ = mock_openai_class
    create_mock.return_value = _mk_chat_response(json.dumps({"wrong_key": []}))

    async with TranslationClient(
        base_url="https://api.example.com/v1",
        api_key="sk-test",
        model="test-model",
    ) as t:
        with pytest.raises(ValueError, match="missing 'translations'"):
            await t.translate_batch(["q1"])


# ─────────────────────────────────────────────────────────────────────────────
# Config-error paths → openai exception types propagate
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_auth_error_propagates(mock_openai_class) -> None:
    create_mock, _ = mock_openai_class
    create_mock.side_effect = AuthenticationError(
        message="bad api key",
        response=_mk_response(401),
        body={"error": {"code": "invalid_api_key"}},
    )

    async with TranslationClient(
        base_url="https://api.example.com/v1",
        api_key="sk-bad",
        model="test-model",
    ) as t:
        with pytest.raises(AuthenticationError):
            await t.translate_batch(["q1"])


@pytest.mark.asyncio
async def test_not_found_error_propagates(mock_openai_class) -> None:
    create_mock, _ = mock_openai_class
    create_mock.side_effect = NotFoundError(
        message="model not found",
        response=_mk_response(404),
        body={"error": {"code": "model_not_found"}},
    )

    async with TranslationClient(
        base_url="https://api.example.com/v1",
        api_key="sk-test",
        model="bogus-model",
    ) as t:
        with pytest.raises(NotFoundError):
            await t.translate_batch(["q1"])


@pytest.mark.asyncio
async def test_bad_request_error_propagates(mock_openai_class) -> None:
    create_mock, _ = mock_openai_class
    create_mock.side_effect = BadRequestError(
        message="missing json keyword",
        response=_mk_response(400),
        body={"error": {"code": "invalid_request_error"}},
    )

    async with TranslationClient(
        base_url="https://api.example.com/v1",
        api_key="sk-test",
        model="test-model",
    ) as t:
        with pytest.raises(BadRequestError):
            await t.translate_batch(["q1"])


# ─────────────────────────────────────────────────────────────────────────────
# Lifecycle
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_aclose_idempotent(mock_openai_class) -> None:
    """Calling aclose twice must not raise — context manager exit + manual close."""
    create_mock, instance = mock_openai_class
    t = TranslationClient(
        base_url="https://api.example.com/v1",
        api_key="sk-test",
        model="test-model",
    )
    await t.aclose()
    await t.aclose()  # second call — must be no-op
    instance.close.assert_called_once()


@pytest.mark.asyncio
async def test_async_context_manager_closes(mock_openai_class) -> None:
    create_mock, instance = mock_openai_class
    async with TranslationClient(
        base_url="https://api.example.com/v1",
        api_key="sk-test",
        model="test-model",
    ) as t:
        assert t is not None
    # After exit, close was called once.
    instance.close.assert_called_once()


# ─────────────────────────────────────────────────────────────────────────────
# Anti-tenacity invariant — RESEARCH Pitfall 1
# ─────────────────────────────────────────────────────────────────────────────


def test_client_module_does_not_import_tenacity() -> None:
    """RESEARCH Pitfall 1: tenacity wrapping AsyncOpenAI multiplies retries.

    The translation/client.py source MUST NOT have an import statement for
    tenacity (mentioning the name in a docstring as anti-pattern documentation
    is fine and even desirable).
    """
    from pathlib import Path

    src = (
        Path(__file__).parent.parent.parent
        / "src"
        / "polyarb"
        / "translation"
        / "client.py"
    ).read_text()

    # We allow the word "tenacity" inside docstrings / comments (anti-pattern doc)
    # but DISALLOW any actual import statement. This is a self-invalidating gate.
    forbidden_imports = []
    for line in src.splitlines():
        stripped = line.strip()
        if stripped.startswith("#") or stripped.startswith('"'):
            continue
        if stripped.startswith("import tenacity") or stripped.startswith(
            "from tenacity"
        ):
            forbidden_imports.append(stripped)

    assert not forbidden_imports, (
        f"translation/client.py imports tenacity (Pitfall 1 — AsyncOpenAI's "
        f"built-in retry is sufficient): {forbidden_imports}"
    )
