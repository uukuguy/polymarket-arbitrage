"""TranslationConfig — env loading + isolation + secret hygiene.

env_prefix is TRANSLATION_ (NOT POLYARB_). We test the namespace is
genuinely isolated so a future Phase-1 env var can't accidentally leak in.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from polyarb.translation.config import TranslationConfig


def _clear_translation_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Strip any TRANSLATION_* env that the host may have set so each test
    is hermetic. We use delenv(raising=False) for keys that may not exist."""
    for key in (
        "TRANSLATION_API_BASE",
        "TRANSLATION_API_KEY",
        "TRANSLATION_MODEL",
        "TRANSLATION_MAX_CONCURRENCY",
        "TRANSLATION_BATCH_SIZE",
        "TRANSLATION_MAX_RETRIES",
        "TRANSLATION_REQUEST_TIMEOUT_S",
    ):
        monkeypatch.delenv(key, raising=False)


def test_loads_from_env(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """All required vars present → TranslationConfig() builds successfully."""
    _clear_translation_env(monkeypatch)
    # Point env_file at a non-existent path so a stray .env in repo root
    # cannot influence the test's outcome.
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TRANSLATION_API_BASE", "https://api.deepseek.com/v1")
    monkeypatch.setenv("TRANSLATION_API_KEY", "sk-test-abc")
    monkeypatch.setenv("TRANSLATION_MODEL", "deepseek-chat")

    cfg = TranslationConfig()
    assert cfg.api_base == "https://api.deepseek.com/v1"
    assert cfg.model == "deepseek-chat"
    # Defaults applied
    assert cfg.max_concurrency == 10
    assert cfg.batch_size == 20
    assert cfg.max_retries == 3
    assert cfg.request_timeout_s == 30.0


def test_missing_api_base_raises(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """Without TRANSLATION_API_BASE the constructor fails fast."""
    _clear_translation_env(monkeypatch)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TRANSLATION_API_KEY", "sk-test-abc")
    monkeypatch.setenv("TRANSLATION_MODEL", "deepseek-chat")

    with pytest.raises(ValidationError):
        TranslationConfig()


def test_missing_api_key_raises(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """Without TRANSLATION_API_KEY the constructor fails fast."""
    _clear_translation_env(monkeypatch)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TRANSLATION_API_BASE", "https://api.deepseek.com/v1")
    monkeypatch.setenv("TRANSLATION_MODEL", "deepseek-chat")

    with pytest.raises(ValidationError):
        TranslationConfig()


def test_missing_model_raises(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """Without TRANSLATION_MODEL the constructor fails fast."""
    _clear_translation_env(monkeypatch)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TRANSLATION_API_BASE", "https://api.deepseek.com/v1")
    monkeypatch.setenv("TRANSLATION_API_KEY", "sk-test-abc")

    with pytest.raises(ValidationError):
        TranslationConfig()


def test_does_not_pickup_polyarb_prefix(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """env_prefix=TRANSLATION_ must NOT pick up POLYARB_* env.

    Verifies namespace isolation — Phase 1's POLYARB_DB_PATH or any other
    POLYARB_* var cannot accidentally satisfy TranslationConfig's required
    fields.
    """
    _clear_translation_env(monkeypatch)
    monkeypatch.chdir(tmp_path)
    # Note: we set POLYARB_API_BASE but NOT TRANSLATION_API_BASE.
    monkeypatch.setenv("POLYARB_API_BASE", "https://wrong.example.com")
    monkeypatch.setenv("POLYARB_API_KEY", "wrong-key")
    monkeypatch.setenv("POLYARB_MODEL", "wrong-model")

    with pytest.raises(ValidationError):
        TranslationConfig()


def test_repr_does_not_leak_api_key(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """SecretStr wrapping → repr(cfg) does not contain the secret."""
    _clear_translation_env(monkeypatch)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TRANSLATION_API_BASE", "https://api.example.com/v1")
    monkeypatch.setenv("TRANSLATION_API_KEY", "sk-super-secret-12345")
    monkeypatch.setenv("TRANSLATION_MODEL", "test-model")

    cfg = TranslationConfig()
    rendered = repr(cfg)
    assert "sk-super-secret-12345" not in rendered
    # secret_api_key() is the explicit reveal path
    assert cfg.secret_api_key() == "sk-super-secret-12345"


def test_str_does_not_leak_api_key(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """str(cfg) and the model_dump default also redact the secret."""
    _clear_translation_env(monkeypatch)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TRANSLATION_API_BASE", "https://api.example.com/v1")
    monkeypatch.setenv("TRANSLATION_API_KEY", "sk-super-secret-12345")
    monkeypatch.setenv("TRANSLATION_MODEL", "test-model")

    cfg = TranslationConfig()
    assert "sk-super-secret-12345" not in str(cfg)


def test_max_concurrency_override_via_env(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """User can dial max_concurrency via .env (e.g. DeepSeek free tier)."""
    _clear_translation_env(monkeypatch)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TRANSLATION_API_BASE", "https://api.example.com/v1")
    monkeypatch.setenv("TRANSLATION_API_KEY", "sk-test")
    monkeypatch.setenv("TRANSLATION_MODEL", "test-model")
    monkeypatch.setenv("TRANSLATION_MAX_CONCURRENCY", "3")

    cfg = TranslationConfig()
    assert cfg.max_concurrency == 3
