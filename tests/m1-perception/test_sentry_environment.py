"""Tests for Settings.sentry_environment + init_sentry environment derivation.

Phase 03.1-05 D-03 step 2 (GAP-102).

Previous derivation (`environment = "prod" if settings.release_id != "dev" else "dev"`)
silently failed to flip production deploys to environment=production, causing
Sentry alert routing rules with env filters to potentially silence prod issues.

New contract:
- Settings.sentry_environment is an explicit field, default "dev"
- env var POLYARB_SENTRY_ENV controls it
- init_sentry passes it through verbatim to sentry_sdk.init(environment=...)
- A W-6 typo guard logs a warning for non-canonical values (dev/staging/production)
  WITHOUT refusing — preserves opt-in for custom envs.
"""
from __future__ import annotations

import logging
from typing import Any
from unittest.mock import MagicMock

import pytest


# ---------------------------------------------------------------------------
# Test 1: Settings field exists with default "dev"
# ---------------------------------------------------------------------------


def test_settings_has_sentry_environment_default_dev(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Default-constructed Settings exposes sentry_environment == 'dev'."""
    monkeypatch.setenv("POLYARB_ALLOW_EMPTY_SECRET", "1")
    # Ensure no env var is leaking from CI / shell
    monkeypatch.delenv("POLYARB_SENTRY_ENV", raising=False)

    from polyarb.config import Settings

    s = Settings()
    assert hasattr(s, "sentry_environment")
    assert s.sentry_environment == "dev"


# ---------------------------------------------------------------------------
# Test 2: POLYARB_SENTRY_ENV env var → settings.sentry_environment
# ---------------------------------------------------------------------------


def test_polyarb_sentry_env_propagates_to_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """POLYARB_SENTRY_ENV=production → settings.sentry_environment == 'production'."""
    monkeypatch.setenv("POLYARB_ALLOW_EMPTY_SECRET", "1")
    monkeypatch.setenv("POLYARB_SENTRY_ENV", "production")

    from polyarb.config import Settings

    s = Settings()
    assert s.sentry_environment == "production"


# ---------------------------------------------------------------------------
# Test 3: init_sentry forwards sentry_environment to sentry_sdk.init
# ---------------------------------------------------------------------------


def test_init_sentry_forwards_environment_kwarg(
    daemon_settings_with_observability: Any,
    mocked_sentry: Any,
) -> None:
    """init_sentry(settings.sentry_environment='production') → sentry_sdk.init(environment='production')."""
    from polyarb.observability.sentry import init_sentry

    settings = daemon_settings_with_observability.model_copy(
        update={"sentry_environment": "production"}
    )
    init_sentry(settings)

    assert mocked_sentry.init.call_count == 1
    kwargs = mocked_sentry.init.call_args.kwargs
    assert kwargs.get("environment") == "production", (
        f"expected environment='production', got {kwargs.get('environment')!r}"
    )


# ---------------------------------------------------------------------------
# Test 4: empty DSN → init_sentry no-op (regression check on Plan 02-05 behavior)
# ---------------------------------------------------------------------------


def test_init_sentry_noop_when_dsn_empty(
    daemon_settings_with_observability: Any,
    mocked_sentry: Any,
) -> None:
    """Empty sentry_dsn → init_sentry returns without calling sentry_sdk.init."""
    from polyarb.observability.sentry import init_sentry

    settings = daemon_settings_with_observability.model_copy(
        update={"sentry_dsn": "", "sentry_environment": "production"}
    )
    init_sentry(settings)

    mocked_sentry.init.assert_not_called()


# ---------------------------------------------------------------------------
# Test 5: arbitrary string accepted (no enum lock-in)
# ---------------------------------------------------------------------------


def test_settings_accepts_arbitrary_environment_string(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Settings(sentry_environment='staging') is valid — no enum lock-in."""
    monkeypatch.setenv("POLYARB_ALLOW_EMPTY_SECRET", "1")
    monkeypatch.delenv("POLYARB_SENTRY_ENV", raising=False)

    from polyarb.config import Settings

    s = Settings(sentry_environment="staging")
    assert s.sentry_environment == "staging"


# ---------------------------------------------------------------------------
# Test 6 (W-6): typo guard — non-canonical value logs warning but still inits
# ---------------------------------------------------------------------------


def test_init_sentry_typo_guard_warns_for_non_canonical(
    daemon_settings_with_observability: Any,
    mocked_sentry: Any,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """init_sentry with sentry_environment='produciton' (typo) logs warning, still inits.

    W-6 hard gate from plan: silent typo → never-matched alert filter would be
    invisible until the next outage. Loud warning gives operator a chance to
    catch it in the boot logs.
    """
    from polyarb.observability.sentry import init_sentry

    settings = daemon_settings_with_observability.model_copy(
        update={"sentry_environment": "produciton"}  # intentional typo
    )

    # loguru -> caplog bridge: install a propagation handler so loguru records
    # also flow through stdlib logging which caplog captures.
    from loguru import logger

    handler_id = logger.add(
        lambda msg: logging.getLogger("loguru-bridge").warning(msg.record["message"]),
        level="WARNING",
    )
    try:
        with caplog.at_level(logging.WARNING, logger="loguru-bridge"):
            init_sentry(settings)
    finally:
        logger.remove(handler_id)

    # sentry_sdk.init MUST still be called (warning, not refusal)
    assert mocked_sentry.init.call_count == 1
    kwargs = mocked_sentry.init.call_args.kwargs
    assert kwargs.get("environment") == "produciton"

    # And the warning must have been logged
    warning_text = " ".join(r.message for r in caplog.records if r.levelno >= logging.WARNING)
    assert "not in canonical set" in warning_text or "canonical set" in warning_text, (
        f"expected typo-guard warning, got: {warning_text!r}"
    )
