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
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any
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
def settings_for_test(tmp_db_path: Path, tmp_parquet_root: Path, tmp_cache_root: Path) -> Settings:
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
    with respx.mock(base_url=settings_for_test.gamma_url, assert_all_called=False) as mock:
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

    with patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock:
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
        # Explicitly empty so model_validator does NOT auto-enable mirror/R2
        # when .env has POLYARB_SUPABASE_URL / POLYARB_R2_ENDPOINT set.
        supabase_url="",
        supabase_service_key=SecretStr(""),
        supabase_db_dsn=SecretStr("postgresql://test.invalid/test"),
        r2_endpoint="",
        r2_access_key_id=SecretStr(""),
        r2_secret_access_key=SecretStr(""),
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


# =============================================================================
# Plan 02-03: Supabase mirror + R2 sync fixtures
# =============================================================================


@pytest.fixture
def mocked_supabase() -> Any:
    """MagicMock supabase client supporting .table(name).upsert/insert/delete/select chain."""
    client = MagicMock()

    def _table_mock(name: str) -> MagicMock:
        tbl = MagicMock()
        tbl.upsert.return_value = tbl
        tbl.insert.return_value = tbl
        tbl.delete.return_value = tbl
        tbl.select.return_value = tbl
        tbl.neq.return_value = tbl
        tbl.eq.return_value = tbl
        tbl.order.return_value = tbl
        tbl.limit.return_value = tbl
        tbl.execute.return_value = MagicMock(data=[])
        return tbl

    client.table.side_effect = _table_mock
    return client


@pytest.fixture
def mocked_r2_stubber() -> Any:
    """Yield a botocore Stubber wrapping a boto3 s3 client with dummy creds."""
    import boto3
    from botocore.stub import Stubber

    client = boto3.client(
        "s3",
        endpoint_url="https://test.r2.cloudflarestorage.com",
        aws_access_key_id="dummy-access-key",
        aws_secret_access_key="dummy-secret-key",
        region_name="auto",
    )
    with Stubber(client) as stubber:
        yield stubber


@pytest.fixture
def settings_for_test_with_supabase_mirror(
    tmp_db_path: Path, tmp_parquet_root: Path, tmp_cache_root: Path
) -> Settings:
    """Settings with Supabase mirror config enabled (mocked — not real endpoint)."""
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
        supabase_url="http://localhost:0",
        supabase_service_key=SecretStr("dummy-service-key"),
    )


@pytest.fixture
def settings_for_test_with_r2(
    tmp_db_path: Path, tmp_parquet_root: Path, tmp_cache_root: Path
) -> Settings:
    """Settings with R2 config enabled (mocked — not real endpoint)."""
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
        r2_endpoint="https://test.r2.cloudflarestorage.com",
        r2_access_key_id=SecretStr("dummy-access-key"),
        r2_secret_access_key=SecretStr("dummy-secret-key"),
        r2_bucket="test-bucket",
    )


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

    # Plan 02-09 (D-23): orchestrator now uses iter_active_markets (async
    # iterator). AsyncMock returns a coroutine by default which is NOT an
    # async iterator. Supply real async-generator stubs that yield the
    # fixture entries one at a time, matching the streaming contract.
    def _make_iter_markets(items):
        async def _iter(coverage):
            for m in items:
                yield m
            coverage.result = type(coverage.result)(len(items), 1, True, None)

        return _iter

    def _make_iter_events(items):
        async def _iter(coverage):
            for e in items:
                yield e
            coverage.result = type(coverage.result)(len(items), 1, True, None)

        return _iter

    fake.iter_active_markets = _make_iter_markets(gamma_fixture)
    fake.iter_active_events = _make_iter_events(synthetic_events)
    fake.aclose = AsyncMock()
    fake.__aenter__.return_value = fake
    fake.__aexit__.return_value = None

    with patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake):
        yield fake


# =============================================================================
# Plan 02-09 (D-23): realistic Gamma payload factories for memory regression
# =============================================================================


@pytest.fixture
def gamma_payload_factory() -> tuple[Any, Any]:
    """Yield ``(make_realistic_market, make_realistic_event)`` factories.

    W-3 fix from Plan 02-09: ``tests/m1-perception/`` contains a hyphen so
    direct module imports (``from tests.m1-perception.fixtures import ...``)
    do NOT work. The fixture module is loaded by path through this fixture.
    """
    import importlib.util
    from pathlib import Path

    payload_path = Path(__file__).parent / "fixtures" / "gamma_streaming_payload.py"
    spec = importlib.util.spec_from_file_location("gamma_streaming_payload", payload_path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.make_realistic_market, mod.make_realistic_event


# =============================================================================
# Plan 02-05: Observability fixtures (Sentry + Better Stack + Telegram)
# =============================================================================


@pytest.fixture
def mocked_sentry(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Monkeypatch sentry_sdk.init / capture_message / capture_exception / add_breadcrumb.

    Returns a SimpleNamespace with the mock objects so tests can assert on calls.
    """
    from types import SimpleNamespace

    import sentry_sdk

    init_mock = MagicMock()
    capture_message_mock = MagicMock()
    capture_exception_mock = MagicMock()
    add_breadcrumb_mock = MagicMock()

    monkeypatch.setattr(sentry_sdk, "init", init_mock)
    monkeypatch.setattr(sentry_sdk, "capture_message", capture_message_mock)
    monkeypatch.setattr(sentry_sdk, "capture_exception", capture_exception_mock)
    monkeypatch.setattr(sentry_sdk, "add_breadcrumb", add_breadcrumb_mock)

    # Also patch the references that observability.sentry / daemon.alerts import.
    # Imports done lazily inside fixture so the test can opt-in without triggering
    # module load at conftest import time.
    try:
        import polyarb.observability.sentry as obs_sentry  # noqa: WPS433

        monkeypatch.setattr(obs_sentry, "sentry_sdk", sentry_sdk, raising=False)
    except ImportError:
        pass  # module not yet implemented in RED phase
    try:
        import polyarb.daemon.alerts as alerts_mod  # noqa: WPS433

        monkeypatch.setattr(alerts_mod, "sentry_sdk", sentry_sdk, raising=False)
    except ImportError:
        pass

    return SimpleNamespace(
        init=init_mock,
        capture_message=capture_message_mock,
        capture_exception=capture_exception_mock,
        add_breadcrumb=add_breadcrumb_mock,
    )


@pytest.fixture
def mocked_better_stack(monkeypatch: pytest.MonkeyPatch) -> Any:
    """httpx AsyncClient mock for the Better Stack heartbeat URL + Telegram fallback.

    Returns a SimpleNamespace:
      - calls: list of (method, url, json_body) tuples observed
      - set_response(method, url_substring, status_code): override response for a url
      - reset(): clear calls + responses

    Default behaviour: every request returns 200 OK.
    """
    from types import SimpleNamespace

    state: dict = {
        "calls": [],
        "overrides": [],  # list of (method, url_substring, status_code)
        "default_status": 200,
    }

    def set_response(method: str, url_substring: str, status_code: int) -> None:
        state["overrides"].append((method.upper(), url_substring, status_code))

    def reset() -> None:
        state["calls"].clear()
        state["overrides"].clear()
        state["default_status"] = 200

    class _FakeResponse:
        def __init__(self, status_code: int) -> None:
            self.status_code = status_code

        def json(self) -> dict:
            return {"ok": self.status_code < 400}

    class _FakeAsyncClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        async def __aenter__(self) -> _FakeAsyncClient:
            return self

        async def __aexit__(self, *args: Any) -> None:
            return None

        async def post(self, url: str, json: dict | None = None, **kwargs: Any) -> _FakeResponse:
            state["calls"].append(("POST", url, json))
            for m, sub, sc in state["overrides"]:
                if m == "POST" and sub in url:
                    return _FakeResponse(sc)
            return _FakeResponse(state["default_status"])

        async def get(self, url: str, **kwargs: Any) -> _FakeResponse:
            state["calls"].append(("GET", url, None))
            for m, sub, sc in state["overrides"]:
                if m == "GET" and sub in url:
                    return _FakeResponse(sc)
            return _FakeResponse(state["default_status"])

    import httpx

    monkeypatch.setattr(httpx, "AsyncClient", _FakeAsyncClient)

    return SimpleNamespace(
        calls=state["calls"],
        set_response=set_response,
        reset=reset,
    )


# =============================================================================
# Phase 03 Plan 03: L2 daemon fixtures (mock-shaped WsConsumer + EventListener)
# =============================================================================


@pytest.fixture
def mock_ws_consumer() -> Any:
    """MagicMock WS consumer shaped like Plan 04 WsConsumer interface.

    Plan 04 will replace this mock with a real `polyarb.daemon.ws.WsConsumer`.
    For Plan 03, attributes match what `_build_l2_health_checks` reads.
    """
    consumer = MagicMock()
    consumer.current_state = "CONNECTED"
    consumer.last_event_at_s = time.time()
    consumer.subscribed_assets = ["0xabc", "0xdef"]
    return consumer


@pytest.fixture
def mock_event_listener() -> Any:
    """Complete live event-chain state consumed by Phase 05.1 health checks."""
    listener = MagicMock()
    listener.is_listening = True
    listener.is_connected = True
    listener.last_event_received_s = time.time()
    listener.last_notification_s = time.time()
    listener.last_reconciliation_success_s = time.time()
    listener.latest_snapshot_id = 0
    listener.committed_cursor = 0
    listener.cursor_lag = 0
    listener.cursor_lag_since_s = None
    listener.reconnect_count = 0
    return listener


@pytest.fixture
def l2_http_test_client(
    daemon_settings_for_test: Any,
    mock_ws_consumer: Any,
    mock_event_listener: Any,
) -> Any:
    """Starlette TestClient using create_l2_app factory + injected mocks."""
    from starlette.testclient import TestClient

    from polyarb.http.l2_app import create_l2_app
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()
    app = create_l2_app(
        sqlite_store=store,
        settings=daemon_settings_for_test,
        ws_consumer=mock_ws_consumer,
        event_listener=mock_event_listener,
    )
    with TestClient(app) as client:
        yield client


@pytest.fixture
def daemon_settings_with_observability(
    tmp_db_path: Path, tmp_parquet_root: Path, tmp_cache_root: Path
) -> Settings:
    """Settings with all Plan 02-05 observability fields populated.

    Includes plausible-but-fake values for sentry_dsn / axiom / better_stack / telegram.
    Used by observability tests that need a fully-populated Settings.
    """
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
        sentry_dsn="https://abcdef0123456789@o000000.ingest.sentry.io/123456",
        axiom_token=SecretStr("axiom-test-token"),
        axiom_dataset="polyarb-test",
        better_stack_heartbeat_url=("https://uptime.betterstack.com/api/v1/heartbeat/test"),
        telegram_bot_token=SecretStr("7012345:test-bot-token"),
        telegram_chat_id="-1001234567890",
        alert_dedupe_window_seconds=300,
        release_id="v0.2.0-abc123",
    )
