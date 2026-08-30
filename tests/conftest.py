"""Repository-wide safety defaults for tests.

Project Settings loads ``.env`` automatically. Tests must never inherit real
write-capable credentials merely because they construct ``Settings()`` without
overriding every cloud field. Individual tests may still opt in explicitly with
localhost/dummy values or a Testcontainer DSN.
"""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _isolate_production_cloud_adapters(monkeypatch: pytest.MonkeyPatch) -> None:
    """Disable external write and alert adapters unless a test opts in."""
    empty_vars = (
        "POLYARB_SUPABASE_URL",
        "POLYARB_SUPABASE_DB_DSN",
        "POLYARB_SUPABASE_SERVICE_KEY",
        "POLYARB_R2_ENDPOINT",
        "POLYARB_R2_ACCESS_KEY_ID",
        "POLYARB_R2_SECRET_ACCESS_KEY",
        "POLYARB_SENTRY_DSN",
        "POLYARB_BETTER_STACK_HEARTBEAT_URL",
        "POLYARB_TELEGRAM_BOT_TOKEN",
        "POLYARB_TELEGRAM_CHAT_ID",
    )
    false_vars = (
        "POLYARB_SUPABASE_MIRROR_ENABLED",
        "POLYARB_L2_MIRROR_ENABLED",
        "POLYARB_R2_ENABLED",
        "POLYARB_EVENT_BUS_ENABLED",
    )
    for name in empty_vars:
        monkeypatch.setenv(name, "")
    monkeypatch.delenv("POLYARB_DB_POOL_MAX_SIZE", raising=False)
    for name in false_vars:
        monkeypatch.setenv(name, "false")
