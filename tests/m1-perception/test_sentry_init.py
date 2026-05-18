"""Tests for polyarb.observability.sentry — init + before_send redact.

Plan 02-05 — D-15 (Sentry Developer Free) + T-02-08 (Sentry breadcrumb PII).

Coverage:
- init_sentry skipped when DSN empty (dev mode)
- init_sentry calls sentry_sdk.init with send_default_pii=False + before_send + LoguruIntegration
- before_send strips Bearer / token=/ secret=/ api_key= patterns from request body
- before_send strips secret-named extra fields
- before_send leaves normal exception events unchanged
- release_id propagated to sentry_sdk.init(release=...)
"""
from __future__ import annotations

from typing import Any


# ---------------------------------------------------------------------------
# init_sentry behaviour
# ---------------------------------------------------------------------------


def test_init_sentry_skipped_when_dsn_empty(
    daemon_settings_with_observability: Any,
    mocked_sentry: Any,
) -> None:
    """Empty DSN → init_sentry does NOT call sentry_sdk.init."""
    from polyarb.observability.sentry import init_sentry

    # Build a settings copy with empty DSN
    settings = daemon_settings_with_observability.model_copy(update={"sentry_dsn": ""})

    init_sentry(settings)

    mocked_sentry.init.assert_not_called()


def test_init_sentry_calls_sdk_with_pii_false(
    daemon_settings_with_observability: Any,
    mocked_sentry: Any,
) -> None:
    """Real DSN → init called with send_default_pii=False + before_send callable + LoguruIntegration."""
    from polyarb.observability.sentry import init_sentry

    init_sentry(daemon_settings_with_observability)

    assert mocked_sentry.init.call_count == 1
    kwargs = mocked_sentry.init.call_args.kwargs
    assert kwargs["dsn"] == daemon_settings_with_observability.sentry_dsn
    assert kwargs["send_default_pii"] is False
    assert callable(kwargs["before_send"])

    # LoguruIntegration must be among integrations
    integrations = kwargs.get("integrations", [])
    integration_names = [type(i).__name__ for i in integrations]
    assert "LoguruIntegration" in integration_names


def test_init_sentry_release_id_propagated(
    daemon_settings_with_observability: Any,
    mocked_sentry: Any,
) -> None:
    """settings.release_id is forwarded to sentry_sdk.init(release=...)."""
    from polyarb.observability.sentry import init_sentry

    init_sentry(daemon_settings_with_observability)

    kwargs = mocked_sentry.init.call_args.kwargs
    assert kwargs.get("release") == "v0.2.0-abc123"


# ---------------------------------------------------------------------------
# before_send redact filter
# ---------------------------------------------------------------------------


def test_before_send_strips_token_pattern() -> None:
    """Event request.data containing `Bearer abc123` is redacted to `Bearer [REDACTED]`."""
    from polyarb.observability.sentry import _before_send

    event = {
        "request": {
            "data": "POST /scan Bearer abc123secret_value_here"
        }
    }

    cleaned = _before_send(event, hint=None)

    data = cleaned["request"]["data"]
    assert "abc123secret_value_here" not in data, (
        f"raw token leaked through redact filter: {data}"
    )
    assert "[REDACTED]" in data


def test_before_send_strips_extra_fields() -> None:
    """Event extra={'api_key': 'sk-abc'} is redacted (key-name match)."""
    from polyarb.observability.sentry import _before_send

    event = {
        "extra": {
            "api_key": "sk-abcdefghijklmnopqrstuvwxyz",
            "snapshot_id": 42,  # non-secret → keep
        }
    }

    cleaned = _before_send(event, hint=None)

    assert cleaned["extra"]["api_key"] == "[REDACTED]"
    assert cleaned["extra"]["snapshot_id"] == 42


def test_before_send_keeps_normal_messages() -> None:
    """Event with no secret patterns passes through unchanged."""
    from polyarb.observability.sentry import _before_send

    event = {
        "request": {"data": "POST /snapshot taken_at_ms=1715600000000"},
        "extra": {"snapshot_id": 1, "market_count": 20000},
    }

    cleaned = _before_send(event, hint=None)

    assert cleaned["request"]["data"] == "POST /snapshot taken_at_ms=1715600000000"
    assert cleaned["extra"]["market_count"] == 20000


def test_before_send_strips_breadcrumb_message() -> None:
    """Breadcrumb messages also pass through the redact filter."""
    from polyarb.observability.sentry import _before_send

    event = {
        "breadcrumbs": {
            "values": [
                {
                    "category": "http",
                    "message": "GET /events token=secret_token_abc",
                    "data": {"url": "https://x?token=secret_value_xyz"},
                }
            ]
        }
    }

    cleaned = _before_send(event, hint=None)

    crumb = cleaned["breadcrumbs"]["values"][0]
    assert "secret_token_abc" not in crumb["message"]
    assert "[REDACTED]" in crumb["message"]
    # data field is also redacted
    assert "secret_value_xyz" not in str(crumb["data"])


def test_before_send_strips_jwt_pattern() -> None:
    """JWT-like tokens (eyJ... three-part) are redacted from request body."""
    from polyarb.observability.sentry import _before_send

    jwt = (
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
        ".eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIn0"
        ".SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
    )
    event = {"request": {"data": f"Authorization: Bearer {jwt}"}}

    cleaned = _before_send(event, hint=None)

    assert jwt not in cleaned["request"]["data"]


# ---------------------------------------------------------------------------
# Defence-in-depth: known sensitive Settings keys are stripped from extra
# ---------------------------------------------------------------------------


def test_before_send_strips_known_secret_field_names() -> None:
    """Sensitive Settings field names trigger key-based redaction."""
    from polyarb.observability.sentry import _before_send

    event = {
        "extra": {
            "telegram_bot_token": "7012345:real_bot_token",
            "sentry_dsn": "https://abc@sentry.io/123",
            "scan_shared_secret": "a1b2c3d4e5f6",
            "supabase_service_key": "sb-secret-key-xyz",
        }
    }

    cleaned = _before_send(event, hint=None)
    for k in ("telegram_bot_token", "sentry_dsn", "scan_shared_secret", "supabase_service_key"):
        assert cleaned["extra"][k] == "[REDACTED]", (
            f"key {k} should have been redacted by name match"
        )
