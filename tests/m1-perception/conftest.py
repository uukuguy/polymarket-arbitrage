"""Shared fixtures for m1-perception phase 01 tests.

Plan 01-5 T1 — All fixtures live here so test files don't repeat themselves.
The fixtures here MUST stay backward-compatible with tests that already set
``POLYARB_ALLOW_EXTERNAL_PATHS=1`` themselves at module top — pytest's
``os.environ.setdefault`` is idempotent so double-set is harmless.

F-3 SECURITY (escape hatch):
    pytest's ``tmp_path`` lives outside the project root by design (e.g.
    ``/private/var/folders/...``). The Settings ``_within_project`` field
    validator would otherwise raise. We set ``POLYARB_ALLOW_EXTERNAL_PATHS=1``
    at module-import time so any subsequent ``Settings()`` accepts external
    paths. This env var MUST NEVER appear in production deployment configs.

F-4 SECURITY (credential-leak regression guard):
    The recorded fixtures (gamma_sample.json, clob_sample.json) are committed
    to git. If a future re-recording leaks an Authorization header / Cookie /
    API key, this scanner fails the entire session at collection time so a
    bad fixture cannot reach a CI test report or stay quietly on disk.

Plan 02-02: Added daemon_settings_for_test, http_test_client, make_signed_request
    fixtures for HTTP server + scheduler tests.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
from pathlib import Path
from typing import Any, Callable
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import respx
from httpx import Response

# F-3 SECURITY ESCAPE HATCH: pytest tmp_path lives outside project root by design.
# Set BEFORE any Settings import so the field_validator picks it up at class-build time.
os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
# Plan 02-02: allow empty HMAC secret in tests (no prod deploy config)
os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")

from polyarb.config import Settings  # noqa: E402  (must follow env-var setup)


FIXTURES_DIR = Path(__file__).parent / "fixtures"

# F-4 SECURITY: fixtures are committed to git and recorded from real APIs.
# Run a credential-leak grep at collection time so a bad fixture fails fast.
_CRED_RE = re.compile(
    r"authorization|cookie|x-api-key|bearer|secret|private[_-]?key",
    re.IGNORECASE,
)
for _fp in (FIXTURES_DIR / "gamma_sample.json", FIXTURES_DIR / "clob_sample.json"):
    if _fp.exists() and _CRED_RE.search(_fp.read_text()):
        raise RuntimeError(
            f"F-4 SECURITY: credential-like field detected in {_fp}. "
            f"Re-record or sanitize before running tests."
        )


# ─────────────────────────────────────────────────────────────────────────────
# Fixture-data loaders (session-scoped — read once per pytest run)
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def gamma_fixture() -> list[dict]:
    """Recorded Gamma /markets response (5 real-shape market dicts)."""
    return json.loads((FIXTURES_DIR / "gamma_sample.json").read_text())


@pytest.fixture(scope="session")
def clob_fixture() -> dict:
    """Recorded CLOB response: ``{token_ids, books, prices_buy, prices_sell}``."""
    return json.loads((FIXTURES_DIR / "clob_sample.json").read_text())


# ─────────────────────────────────────────────────────────────────────────────
# Filesystem fixtures (function-scoped — fresh per test for isolation)
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def tmp_db_path(tmp_path: Path) -> Path:
    """Per-test SQLite path under tmp_path (auto-cleaned by pytest)."""
    return tmp_path / "test_state.db"


@pytest.fixture
def tmp_parquet_root(tmp_path: Path) -> Path:
    """Per-test Parquet root under tmp_path."""
    return tmp_path / "snapshots"


@pytest.fixture
def tmp_cache_root(tmp_path: Path) -> Path:
    """Per-test cache root under tmp_path (CLOB chunk cache)."""
    return tmp_path / ".cache"


@pytest.fixture
def settings_for_test(
    tmp_db_path: Path, tmp_parquet_root: Path, tmp_cache_root: Path
) -> Settings:
    """Settings tuned for fast tests: tiny retries + low liquidity threshold.

    The lowered ``liquidity_threshold_usd=100.0`` ensures all 5 fixture
    markets pass the subset filter (their min liquidity is ~20k, but keeping
    the threshold at 100 leaves headroom for any future fixture re-record).
    """
    return Settings(
        db_path=tmp_db_path,
        parquet_root=tmp_parquet_root,
        cache_root=tmp_cache_root,
        retry_attempts=2,
        retry_min_wait_s=0.001,
        retry_max_wait_s=0.005,
        http_timeout_s=2.0,
        liquidity_threshold_usd=100.0,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Mocked client fixtures
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def mocked_gamma(gamma_fixture: list[dict], settings_for_test: Settings) -> Any:
    """respx mock for Gamma ``/markets`` returning fixture data then [] (paginate-stop).

    Use this fixture when you want HTTP-level mocking (i.e. exercising the real
    GammaClient code path). For most orchestrator tests, prefer patching the
    GammaClient class symbol on ``polyarb.snapshot.orchestrator`` instead.
    """
    with respx.mock(
        base_url=settings_for_test.gamma_url, assert_all_called=False
    ) as mock:
        route = mock.get("/markets")
        # First call: real fixture data; second call: empty list to terminate pagination.
        route.side_effect = [
            Response(200, json=gamma_fixture),
            Response(200, json=[]),
        ]
        yield mock


@pytest.fixture
def mocked_clob(clob_fixture: dict) -> Any:
    """Patch ``ClobReaderClient`` at the orchestrator's import site.

    Returns a dict ``{"books": <AsyncMock>, "prices": <AsyncMock>}`` so tests
    can assert call counts and arguments. The mock yields exactly the recorded
    fixture's books + buy/sell price dicts on each invocation.

    Note: the orchestrator imports ``ClobReaderClient`` from
    ``polyarb.clients.clob_client`` and uses ``clob.get_books`` /
    ``clob.get_prices_buy_sell`` (not the raw SDK methods). We patch the
    high-level wrapper to keep tests decoupled from the SDK's internal API.
    """
    from types import SimpleNamespace

    books_objs = [SimpleNamespace(**bd) for bd in clob_fixture["books"]]
    get_books_mock = AsyncMock(return_value=books_objs)
    get_prices_mock = AsyncMock(
        return_value={
            "buy": clob_fixture["prices_buy"],
            "sell": clob_fixture["prices_sell"],
        }
    )

    with patch(
        "polyarb.snapshot.orchestrator.ClobReaderClient"
    ) as ClobMock:
        clob_inst = ClobMock.return_value
        clob_inst.get_books = get_books_mock
        clob_inst.get_prices_buy_sell = get_prices_mock
        yield {"books": get_books_mock, "prices": get_prices_mock, "class": ClobMock}


# =============================================================================
# Plan 02-02: HTTP daemon + scheduler fixtures
# =============================================================================

# A stable 64-char hex test secret (32 random bytes encoded as hex).
_TEST_SCAN_SECRET = "a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456"


@pytest.fixture
def daemon_settings_for_test(
    tmp_db_path: Path, tmp_parquet_root: Path, tmp_cache_root: Path
) -> Settings:
    """Settings for HTTP daemon tests — includes scan_shared_secret and empty-secret bypass."""
    from pydantic import SecretStr
    return Settings(
        db_path=tmp_db_path,
        parquet_root=tmp_parquet_root,
        cache_root=tmp_cache_root,
        retry_attempts=1,
        retry_min_wait_s=0.001,
        retry_max_wait_s=0.005,
        http_timeout_s=2.0,
        liquidity_threshold_usd=100.0,
        scan_shared_secret=SecretStr(_TEST_SCAN_SECRET),
    )


@pytest.fixture
def http_test_client(daemon_settings_for_test: Settings) -> Any:
    """Starlette TestClient built via create_app factory.

    Uses a mock scheduler so no real snapshot runs occur in tests.
    """
    from starlette.testclient import TestClient
    from polyarb.http.app import create_app
    from polyarb.storage.sqlite_store import SQLiteStore

    # Ensure DB schema is initialized (some tests insert data before calling this)
    sqlite_store = SQLiteStore(daemon_settings_for_test.db_path)
    sqlite_store.init_schema()

    mock_scheduler = MagicMock()
    app = create_app(
        scheduler=mock_scheduler,
        sqlite_store=sqlite_store,
        settings=daemon_settings_for_test,
    )
    return TestClient(app, raise_server_exceptions=True)


def make_http_test_client(settings: Settings) -> Any:
    """Non-fixture helper for creating a TestClient directly (used in test helpers)."""
    from starlette.testclient import TestClient
    from polyarb.http.app import create_app
    from polyarb.storage.sqlite_store import SQLiteStore

    sqlite_store = SQLiteStore(settings.db_path)
    sqlite_store.init_schema()
    mock_scheduler = MagicMock()
    app = create_app(
        scheduler=mock_scheduler,
        sqlite_store=sqlite_store,
        settings=settings,
    )
    return TestClient(app, raise_server_exceptions=True)


@pytest.fixture
def make_signed_request() -> Callable:
    """Helper fixture: compute HMAC-SHA256 of body JSON bytes + send signed POST request.

    Usage:
        resp = make_signed_request(client, "/scan", {"recipe_name": "..."})
    """
    def _make_signed_request(
        client: Any,
        path: str,
        body_dict: dict,
        secret: str = _TEST_SCAN_SECRET,
    ) -> Any:
        body_bytes = json.dumps(body_dict).encode("utf-8")
        sig = hmac.new(secret.encode("utf-8"), body_bytes, hashlib.sha256).hexdigest()
        return client.post(
            path,
            content=body_bytes,
            headers={
                "Content-Type": "application/json",
                "X-Signature": sig,
            },
        )

    return _make_signed_request


@pytest.fixture
def mocked_gamma_orchestrator(gamma_fixture: list[dict]) -> Any:
    """Patch ``GammaClient`` at the orchestrator's import site (high-level mock).

    Returns the AsyncMock instance. The mocked client supports the async
    context-manager protocol (``__aenter__`` / ``__aexit__``) and exposes
    ``fetch_all_active_markets`` returning the recorded fixture.

    Phase 1.1 Amendment 01: also exposes ``fetch_all_active_events`` returning
    a synthesized list of events that maps each fixture market to a synthetic
    event so event_id flows through end-to-end. This keeps tests realistic
    (event_id is populated for every fixture market) while not requiring a
    separate events_sample.json fixture.
    """
    fake = AsyncMock()
    fake.fetch_all_active_markets.return_value = gamma_fixture
    # Synthesize one event per fixture market so each market gets event_id.
    synthetic_events = [
        {
            "id": f"EV-{m['id']}",
            "slug": f"event-{m['id']}",
            "title": f"Event for {m.get('slug', m['id'])}",
            "ticker": "TKR",
            "active": True,
            "closed": False,
            "liquidity": 1000.0,
            "volume": 5000.0,
            "endDate": "2026-12-31T00:00:00Z",
            "tags": [
                {"id": "120", "label": "Test", "slug": "test"},
            ],
            "markets": [{"id": m["id"]}],
        }
        for m in gamma_fixture
    ]
    fake.fetch_all_active_events.return_value = synthetic_events
    fake.aclose = AsyncMock()
    fake.__aenter__.return_value = fake
    fake.__aexit__.return_value = None

    with patch(
        "polyarb.snapshot.orchestrator.GammaClient", return_value=fake
    ):
        yield fake
