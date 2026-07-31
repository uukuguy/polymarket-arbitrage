"""Phase 02.1 Plan 01 (D-01 / BUG-7) — fail-soft visibility tests.

When ``settings.supabase_mirror_enabled`` is False (secret missing /
config-disabled), the orchestrator's step 7.5 previously took a fully
silent path:

    - branch A: ``supabase_mirror_enabled and not is_valid`` → F-05 guard log
    - branch B: ``supabase_mirror_enabled`` → real mirror push
    - branch C: ``not supabase_mirror_enabled`` → 0 log, 0 breadcrumb (Bug #7)

These tests pin branch C: it MUST now emit an audit log line and a Sentry
breadcrumb (category="mirror", level="info"). D-12 fail-soft contract is
preserved — the snapshot still completes successfully.

Test pattern mirrors PATTERNS.md § "tests/m1-perception/test_orchestrator.py"
(lines 491-524). ``_make_settings`` is local because the analogous helper
lives in ``test_orchestrator.py`` (not conftest) and copying is the cheapest
path; the upstream fixture refactor is out of D-07 scope.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, patch

# F-3 escape hatch: tmp_path is outside project root by design.
os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")

from pydantic import SecretStr  # noqa: E402

from polyarb.config import Settings  # noqa: E402
from polyarb.snapshot.orchestrator import run_snapshot  # noqa: E402

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _make_settings(tmp_path: Path) -> Settings:
    """Build a Settings instance with mirror DISABLED.

    Note: ``Settings`` loads ``.env`` by default (model_config env_file=".env"),
    so a dev machine with a real Supabase service key would auto-enable
    ``supabase_mirror_enabled``. Force the credentials empty in the constructor
    so the post-validator's auto-enable branch (config.py line 151) keeps
    ``supabase_mirror_enabled=False``.
    """
    return Settings(
        db_path=tmp_path / "state.db",
        parquet_root=tmp_path / "snapshots",
        liquidity_threshold_usd=100.0,
        supabase_url="",
        supabase_service_key=SecretStr(""),
        # Also force R2 off so the test doesn't make a real network upload
        # when the dev .env has R2 creds. Test scope = step 7.5 only.
        r2_endpoint="",
        r2_access_key_id=SecretStr(""),
        r2_secret_access_key=SecretStr(""),
    )


def _books_as_objects(book_dicts: list[dict]) -> list[SimpleNamespace]:
    return [SimpleNamespace(**bd) for bd in book_dicts]


def _make_fake_gamma(markets: list[dict], events: list[dict] | None = None) -> AsyncMock:
    fake = AsyncMock()
    fake.fetch_all_active_markets.return_value = markets
    fake.fetch_all_active_events.return_value = events if events is not None else []

    def _make_iter(items):
        async def _iter(coverage):
            for item in items:
                yield item
            coverage.result = type(coverage.result)(len(items), 1, True, None)

        return _iter

    fake.iter_active_markets = _make_iter(markets)
    fake.iter_active_events = _make_iter(events if events is not None else [])
    fake.aclose = AsyncMock()
    fake.__aenter__.return_value = fake
    fake.__aexit__.return_value = None
    return fake


def _load_fixtures() -> tuple[list[dict], dict]:
    import json

    gamma = json.loads((FIXTURES_DIR / "gamma_sample.json").read_text())
    clob = json.loads((FIXTURES_DIR / "clob_sample.json").read_text())
    return gamma, clob


def _run(settings: Settings) -> Any:
    """Run a single snapshot end-to-end against the recorded fixtures.

    Mirror is intentionally disabled (no supabase_url / service key in settings)
    so step 7.5 takes the new ``else`` branch (D-01).
    """
    gamma_data, clob_data = _load_fixtures()
    fake_gamma = _make_fake_gamma(gamma_data)

    with (
        patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma),
        patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock,
    ):
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(return_value=_books_as_objects(clob_data["books"]))
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={
                "buy": clob_data["prices_buy"],
                "sell": clob_data["prices_sell"],
            }
        )
        return asyncio.run(run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000))


# ─────────────────────────────────────────────────────────────────────────────
# D-01 Test 1 — audit log line emitted when mirror_enabled=False
# ─────────────────────────────────────────────────────────────────────────────


def test_mirror_disabled_logs_audit_entry(tmp_path: Path) -> None:
    """When supabase_mirror_enabled=False, step 7.5 emits an audit-log line
    containing 'mirror disabled' / 'config-disabled' (D-01, BUG-7).

    Previously this branch was silent — no log, no breadcrumb. The new ``else``
    must produce a loguru INFO line so Fly log shipping can audit the skip.

    Note: project logs via loguru (not stdlib), so pytest ``caplog`` cannot
    see the message. Add a loguru sink that writes to a StringIO buffer and
    grep that.
    """
    import io

    from loguru import logger as _loguru_logger

    buf = io.StringIO()
    sink_id = _loguru_logger.add(buf, level="INFO", format="{message}")
    try:
        settings = _make_settings(tmp_path)
        assert settings.supabase_mirror_enabled is False, (
            "test precondition: mirror must be disabled (no service key)"
        )

        result = _run(settings)

        # D-12 invariant: snapshot still completes (fail-soft contract unchanged).
        assert result is not None
    finally:
        _loguru_logger.remove(sink_id)

    output = buf.getvalue()
    assert "mirror disabled" in output or "config-disabled" in output, (
        f"expected an audit log line containing 'mirror disabled' / "
        f"'config-disabled' — got tail:\n{output[-1500:]}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# D-01 Test 2 — sentry breadcrumb emitted when mirror_enabled=False
# ─────────────────────────────────────────────────────────────────────────────


def test_mirror_disabled_adds_sentry_breadcrumb(tmp_path: Path, mocked_sentry: Any) -> None:
    """When supabase_mirror_enabled=False, sentry_sdk.add_breadcrumb must be
    called with category='mirror', level='info', message containing
    'config-disabled' (D-01, BUG-7).

    Manual sentry_sdk.add_breadcrumb is NOT filtered by the LoguruIntegration
    level — it always records (see RESEARCH.md Area 5).
    """
    settings = _make_settings(tmp_path)
    assert settings.supabase_mirror_enabled is False

    _run(settings)

    calls = mocked_sentry.add_breadcrumb.call_args_list
    mirror_crumbs = [c for c in calls if c.kwargs.get("category") == "mirror"]
    assert mirror_crumbs, (
        f"expected at least one breadcrumb with category='mirror' — "
        f"got categories={[c.kwargs.get('category') for c in calls]}"
    )
    crumb = mirror_crumbs[0].kwargs
    assert crumb.get("level") == "info", f"expected level=info, got {crumb!r}"
    assert "config-disabled" in (crumb.get("message") or ""), (
        f"expected 'config-disabled' in breadcrumb message — got {crumb!r}"
    )
    # data dict should expose the disabled state + snapshot context.
    data = crumb.get("data") or {}
    assert data.get("supabase_mirror_enabled") is False, (
        f"expected data.supabase_mirror_enabled=False — got {data!r}"
    )
    assert "snapshot_id" in data, f"expected data.snapshot_id field — got {data!r}"
