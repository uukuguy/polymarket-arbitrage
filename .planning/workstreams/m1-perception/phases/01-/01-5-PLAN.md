---
phase: 01
plan: 5
type: execute
wave: 4
depends_on: [01-4]
files_modified:
  - tests/m1-perception/conftest.py
  - tests/m1-perception/test_normalizer.py
  - tests/m1-perception/test_orchestrator.py
  - tests/m1-perception/test_integration.py
  - tests/m1-perception/test_cli.py
autonomous: true
requirements: []
must_haves:
  truths:
    - "conftest.py exposes shared fixtures: tmp_db_path, tmp_parquet_root, settings_for_test, gamma_fixture, clob_fixture, mocked_gamma (respx), mocked_clob (unittest.mock)"
    - "test_normalizer covers JSON-string parsing, missing-id handling, end_time conversion, liquidity field fallback"
    - "test_orchestrator runs run_snapshot end-to-end with both clients mocked, asserts SQLite + Parquet outputs"
    - "test_orchestrator covers: subset filter, full mode, API_UNREACHABLE handling, is_valid policy, per-row fetched_at_ms set"
    - "test_integration runs CLI via typer.testing.CliRunner with mocked clients, asserts exit code 0/1, stdout summary, stderr on invalid"
    - "test_integration verifies SQLite has expected counts + Parquet readable by pyarrow"
    - "test_cli verifies --full flag, --verbose flag, --config flag plumbing"
    - "Full pytest suite (tests/m1-perception) passes in <30s and runs offline (no network)"
    - "make -n snapshot-markets and make help dry-runs verify Makefile contract"
  artifacts:
    - path: tests/m1-perception/conftest.py
      provides: "Shared pytest fixtures for all m1-perception tests"
    - path: tests/m1-perception/test_normalizer.py
      provides: "Unit tests for snapshot/normalizer.py"
    - path: tests/m1-perception/test_orchestrator.py
      provides: "Unit tests for snapshot/orchestrator.py:run_snapshot"
    - path: tests/m1-perception/test_integration.py
      provides: "End-to-end test: CLI invocation → SQLite + Parquet outputs"
    - path: tests/m1-perception/test_cli.py
      provides: "Tests for CLI flag parsing + exit codes + stdout/stderr behavior"
  key_links:
    - from: "tests/m1-perception/test_integration.py"
      to: "polyarb.snapshot.cli:app via typer.testing.CliRunner"
      via: "runner.invoke(app, ['snapshot', '--config', tmp_yaml])"
      pattern: "CliRunner"
    - from: "tests/m1-perception/conftest.py"
      to: "fixtures/gamma_sample.json + clob_sample.json (Plan 2 T1)"
      via: "fixture loads JSON via json.load(open(...))"
      pattern: "fixtures/gamma_sample.json"
---

<objective>
Build the test layer that proves the full Phase 1 pipeline works end-to-end with mocked APIs and that the Makefile contract holds. This is the gate that fails Phase 1 if any earlier plan regressed: a green `pytest tests/m1-perception` + green `make -n snapshot-markets` is the phase exit criterion.

Critical invariants:
- All tests run offline (no real network) — uses respx + unittest.mock + recorded fixtures
- Full suite completes in <30s (per phase outcome 1: end-to-end mocked)
- Integration test exercises CLI → orchestrator → clients (mocked) → validator → storage → exit code path
- API_UNREACHABLE behavior verified: snapshot still persists with is_valid=false (D-D3)
- Subset vs full mode behavior verified
- `make -n snapshot-markets` smoke confirms Makefile target contract

Output: 5 test files (conftest + normalizer + orchestrator + integration + cli).
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/workstreams/m1-perception/phases/01-/01-CONTEXT.md
@.planning/workstreams/m1-perception/phases/01-/01-RESEARCH.md
@.planning/workstreams/m1-perception/phases/01-/01-PATTERNS.md
@.planning/workstreams/m1-perception/phases/01-/01-1-SUMMARY.md
@.planning/workstreams/m1-perception/phases/01-/01-2-SUMMARY.md
@.planning/workstreams/m1-perception/phases/01-/01-3-SUMMARY.md
@.planning/workstreams/m1-perception/phases/01-/01-4-SUMMARY.md
@src/polyarb/config.py
@src/polyarb/snapshot/normalizer.py
@src/polyarb/snapshot/orchestrator.py
@src/polyarb/snapshot/cli.py
@tests/m1-perception/fixtures/gamma_sample.json
@tests/m1-perception/fixtures/clob_sample.json
@Makefile
</context>

<interfaces>
From Plan 1-4 — primary types tests will mock or assert on:
- Settings, load_settings (config.py)
- GammaClient, ClobReaderClient (clients/)
- normalize_market (snapshot/normalizer.py)
- run_snapshot, SnapshotResult (snapshot/orchestrator.py)
- app (snapshot/cli.py)
- SQLiteStore, write_parquet_atomic, compute_snapshot_path (storage/)
- Category, Issue, layer1_count, layer2_fields, layer4_cross_source, is_valid_overall (validator/)
- DDL, MARKETS_COLUMN_ORDER (storage/schemas.py)

Mock points:
- httpx (Gamma): respx
- py_clob_client.client.ClobClient methods: unittest.mock.patch.object on the SDK class methods (NOT respx — SDK doesn't use httpx for sync calls in the way respx can intercept across asyncio.to_thread)
</interfaces>

## Goal

A test layer that the orchestrator (Plan 4) runs through end-to-end without touching the network. Use the recorded fixtures from Plan 2 T1 as the source of truth for response shapes. Cover both happy path (valid snapshot) and failure path (API_UNREACHABLE → is_valid=false but snapshot persists).

<tasks>

<task type="auto">
  <id>T1</id>
  <name>Task 1: Implement conftest.py (shared fixtures for all m1-perception tests)</name>
  <files>tests/m1-perception/conftest.py</files>
  <read_first>
    - tests/m1-perception/fixtures/gamma_sample.json (Plan 2 T1 — actual recorded shape)
    - tests/m1-perception/fixtures/clob_sample.json (Plan 2 T1)
    - src/polyarb/config.py
    - .planning/workstreams/m1-perception/phases/01-/01-RESEARCH.md (Validation Architecture — Wave 0 Gaps fixture list)
  </read_first>
  <action>
    Create `tests/m1-perception/conftest.py` exposing these pytest fixtures:

    ```python
    """Shared fixtures for m1-perception phase 01 tests.

    All fixtures are session- or function-scoped per pytest defaults.
    """
    from __future__ import annotations

    import json
    import os
    import re
    from pathlib import Path
    from unittest.mock import patch, MagicMock

    import pytest
    import respx
    from httpx import Response

    # F-3 SECURITY ESCAPE HATCH: pytest tmp_path lives outside project root by design.
    # Set BEFORE any Settings import so the field_validator picks it up at class build time.
    os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")

    from polyarb.config import Settings


    FIXTURES_DIR = Path(__file__).parent / "fixtures"

    # F-4 SECURITY: fixtures are committed to git and recorded from real APIs.
    # Run a credential-leak grep at collection time so a bad fixture fails fast.
    _CRED_RE = re.compile(r"authorization|cookie|x-api-key|bearer|secret|private[_-]?key", re.IGNORECASE)
    for _fp in (FIXTURES_DIR / "gamma_sample.json", FIXTURES_DIR / "clob_sample.json"):
        if _fp.exists() and _CRED_RE.search(_fp.read_text()):
            raise RuntimeError(
                f"F-4 SECURITY: credential-like field detected in {_fp}. "
                f"Re-record or sanitize before running tests."
            )


    @pytest.fixture(scope="session")
    def gamma_fixture() -> list[dict]:
        """Recorded Gamma /markets response (list of N market dicts)."""
        return json.loads((FIXTURES_DIR / "gamma_sample.json").read_text())


    @pytest.fixture(scope="session")
    def clob_fixture() -> dict:
        """Recorded CLOB response (books + prices_buy + prices_sell + token_ids)."""
        return json.loads((FIXTURES_DIR / "clob_sample.json").read_text())


    @pytest.fixture
    def tmp_db_path(tmp_path) -> Path:
        return tmp_path / "test_state.db"


    @pytest.fixture
    def tmp_parquet_root(tmp_path) -> Path:
        return tmp_path / "snapshots"


    @pytest.fixture
    def settings_for_test(tmp_db_path, tmp_parquet_root) -> Settings:
        """Settings with fast retries + temp paths."""
        return Settings(
            db_path=tmp_db_path,
            parquet_root=tmp_parquet_root,
            retry_attempts=2,
            retry_min_wait_s=0.001,
            retry_max_wait_s=0.005,
            http_timeout_s=2.0,
            liquidity_threshold_usd=100.0,  # lower bar so fixture markets pass subset filter
        )


    @pytest.fixture
    def mocked_gamma(gamma_fixture, settings_for_test):
        """respx mock that returns gamma_fixture for the first /markets page, then [] (terminator)."""
        with respx.mock(base_url=settings_for_test.gamma_url, assert_all_called=False) as mock:
            route = mock.get("/markets")
            # First call: real fixture data; second call: empty list to terminate pagination
            route.side_effect = [
                Response(200, json=gamma_fixture),
                Response(200, json=[]),
            ]
            yield mock


    @pytest.fixture
    def mocked_clob(clob_fixture):
        """Patch py-clob-client sync methods on the ClobClient class.

        Returns a MagicMock so tests can assert call counts / args.
        """
        # Patch the methods on the imported ClobClient class so any instantiation gets the mock.
        from py_clob_client.client import ClobClient

        with patch.object(ClobClient, "get_order_books") as books_mock, \
             patch.object(ClobClient, "get_prices") as prices_mock:
            books_mock.return_value = clob_fixture["books"]
            # get_prices is called twice (BUY then SELL). side_effect lets us alternate.
            prices_mock.side_effect = [
                clob_fixture["prices_buy"],
                clob_fixture["prices_sell"],
            ]
            # If a test calls get_prices more than 2 times, fall back to empty dict.
            yield {"books": books_mock, "prices": prices_mock}
    ```

    Key design choices:
    - `gamma_fixture` and `clob_fixture` are session-scoped (loaded once)
    - `mocked_gamma` uses respx with `side_effect` list for pagination termination
    - `mocked_clob` patches ClobClient class methods (not instance) so any new ClobReaderClient picks up the mock
    - `settings_for_test` lowers liquidity_threshold so fixture data (likely real-market liquidity) passes subset filter — CRITICAL: confirm fixture markets have liquidity above 100; if not, lower further and document
  </action>
  <verify>
    <automated>cd /Users/sujiangwen/sandbox/hacker2026/PolyMarket/polymarket-arbitrage && python -c "
import pytest, sys
sys.path.insert(0, 'tests/m1-perception')
import conftest
# Fixtures defined?
fixtures = ['gamma_fixture', 'clob_fixture', 'tmp_db_path', 'tmp_parquet_root', 'settings_for_test', 'mocked_gamma', 'mocked_clob']
for fname in fixtures:
    assert hasattr(conftest, fname), f'Missing fixture: {fname}'
print('CONFTEST_OK')
" && grep -q "POLYARB_ALLOW_EXTERNAL_PATHS" tests/m1-perception/conftest.py && grep -q "F-4 SECURITY" tests/m1-perception/conftest.py && echo F4_SCAN_PRESENT</automated>
  </verify>
  <done>conftest.py exposes 7 fixtures; gamma_fixture and clob_fixture load from JSON files; mocked_gamma uses respx; mocked_clob patches ClobClient class methods; F-3 escape-hatch env var set; F-4 credential-leak scanner runs at import</done>
</task>

<task type="auto">
  <id>T2</id>
  <name>Task 2: Implement test_normalizer.py (Gamma raw dict parsing edge cases)</name>
  <files>tests/m1-perception/test_normalizer.py</files>
  <read_first>
    - src/polyarb/snapshot/normalizer.py (Plan 4 T1)
    - tests/m1-perception/fixtures/gamma_sample.json (Plan 2 T1)
  </read_first>
  <action>
    Create `tests/m1-perception/test_normalizer.py`:

    ```python
    """Tests for polyarb.snapshot.normalizer."""
    from polyarb.snapshot.normalizer import normalize_market


    def make_raw(**overrides) -> dict:
        base = {
            "id": "M-1",
            "conditionId": "0xabc",
            "slug": "test",
            "question": "Q?",
            "clobTokenIds": '["' + "1" * 70 + '", "' + "2" * 70 + '"]',
            "outcomePrices": '["0.6", "0.4"]',
            "liquidityNum": 1500.0,
            "volumeNum": 50000.0,
            "endDate": "2026-12-31T00:00:00Z",
            "active": True,
            "closed": False,
            "negRisk": False,
        }
        base.update(overrides)
        return base
    ```

    Tests:

    1. `test_normalize_happy_path` — pass make_raw(); assert all 20 expected keys present; market_id="M-1"; yes_token_id is 70-char string; no_token_id is different 70-char string; mid_price==0.6; liquidity_usd==1500.0; volume_usd==50000.0; end_time_ms is int > 1_700_000_000_000; CLOB-derived fields are None; incomplete=False

    2. `test_normalize_missing_id_returns_none` — pass make_raw(id=None) and {}; both return None

    3. `test_normalize_clobTokenIds_string_form` — assert json.loads applied (Pitfall 2 — JSON-string field)

    4. `test_normalize_clobTokenIds_already_list` — pass `clobTokenIds=["t1", "t2"]` (list directly, not string); assert it works (defensive parsing)

    5. `test_normalize_clobTokenIds_malformed_json` — pass `clobTokenIds="not-valid-json"`; assert yes_token_id is None, no_token_id is None (no exception)

    6. `test_normalize_outcomePrices_malformed` — pass `outcomePrices="bad"`; assert mid_price is None

    7. `test_normalize_liquidity_fallback_to_string_field` — pass `liquidityNum=None, liquidity="2500.5"`; assert liquidity_usd==2500.5

    8. `test_normalize_liquidity_neither_field_set` — pass `liquidityNum=None`, no `liquidity` key; assert liquidity_usd is None

    9. `test_normalize_endDate_naive_iso` — pass `endDate="2026-12-31T00:00:00"` (no Z, no tz); assert end_time_ms is set (treated as UTC per normalizer code)

    10. `test_normalize_endDate_malformed` — pass `endDate="invalid"`; assert end_time_ms is None

    11. `test_normalize_token_id_preserves_uint256_string` — pass long token id like "1" * 75 in clobTokenIds; assert yes_token_id is exact string; assert isinstance(yes_token_id, str) (Pitfall 3)

    12. `test_normalize_real_fixture_sample` — for each raw item in gamma_fixture (loaded via session fixture), call normalize_market; assert no None unless market_id missing; assert all returned dicts have the 20 expected keys (validates against real-shape data)

    Use `gamma_fixture` from conftest for test 12.
  </action>
  <verify>
    <automated>cd /Users/sujiangwen/sandbox/hacker2026/PolyMarket/polymarket-arbitrage && pytest tests/m1-perception/test_normalizer.py -xvs 2>&1 | tail -30</automated>
  </verify>
  <done>All 12 tests pass; JSON-string parsing (Pitfall 2) verified for clobTokenIds + outcomePrices; uint256 preserved as str (Pitfall 3); liquidity field fallback verified (Open Q#5); endDate ISO parsing verified including malformed; real fixture data passes normalization</done>
</task>

<task type="auto">
  <id>T3</id>
  <name>Task 3: Implement test_orchestrator.py (run_snapshot with mocked clients)</name>
  <files>tests/m1-perception/test_orchestrator.py</files>
  <read_first>
    - src/polyarb/snapshot/orchestrator.py (Plan 4 T2)
    - tests/m1-perception/conftest.py (T1)
  </read_first>
  <action>
    Create `tests/m1-perception/test_orchestrator.py`:

    ```python
    """Tests for polyarb.snapshot.orchestrator.run_snapshot.

    All tests use mocked Gamma + CLOB clients (no real network).
    """
    import sqlite3
    from pathlib import Path

    import pytest
    import pyarrow.parquet as pq

    from polyarb.snapshot.orchestrator import run_snapshot, SnapshotResult
    from polyarb.validator.category import Category
    ```

    Tests (all `@pytest.mark.asyncio` because asyncio_mode=auto):

    1. `test_subset_mode_happy_path(settings_for_test, mocked_gamma, mocked_clob)` —
       result = await run_snapshot(settings_for_test, mode="subset", now_ms=1_714_435_200_000)
       Asserts:
       - isinstance(result, SnapshotResult)
       - result.mode == "subset"
       - result.market_count > 0
       - result.is_valid is True (or False if Layer 1 jitter — log if Layer 1 issue exists)
       - result.snapshot_id >= 1
       - result.taken_at_ms == 1_714_435_200_000
       - result.parquet_path.exists()
       - settings_for_test.db_path.exists()

    2. `test_full_mode_uses_all_markets(settings_for_test, mocked_gamma, mocked_clob)` —
       result = await run_snapshot(settings_for_test, mode="full", now_ms=1_714_435_200_000)
       Assert mocked_clob["books"].call_count >= 1 (clob was called)
       Assert result.mode == "full"

    3. `test_writes_to_sqlite(settings_for_test, mocked_gamma, mocked_clob)` —
       result = await run_snapshot(settings_for_test, mode="subset", now_ms=1_714_435_200_000)
       con = sqlite3.connect(settings_for_test.db_path)
       Assert COUNT(*) FROM snapshots == 1
       Assert COUNT(*) FROM markets == result.market_count
       Assert SELECT is_valid FROM snapshots WHERE id = result.snapshot_id == int(result.is_valid)

    4. `test_writes_to_parquet_with_correct_schema(settings_for_test, mocked_gamma, mocked_clob)` —
       result = await run_snapshot(...)
       table = pq.read_table(result.parquet_path)
       Assert table.num_rows == result.market_count
       Assert "yes_token_id" in table.column_names
       Assert table.schema.field("yes_token_id").type == pa.string()  # Pitfall 3

    5. `test_per_row_fetched_at_ms_set(settings_for_test, mocked_gamma, mocked_clob)` —
       After run, query SQLite: SELECT DISTINCT fetched_at_ms FROM markets
       Assert exactly 1 distinct value (all rows share clob completion time)
       Assert that value > 0 (not None)

    6. `test_gamma_failure_writes_invalid_snapshot(settings_for_test, mocked_clob)` —
       Use respx to mock /markets returning 500 on every call:
       ```python
       import respx
       from httpx import Response
       with respx.mock(base_url=settings_for_test.gamma_url) as mock:
           mock.get("/markets").mock(return_value=Response(500))
           result = await run_snapshot(settings_for_test, mode="subset")
       ```
       Asserts:
       - result.is_valid is False
       - result.snapshot_id >= 1 (snapshot still persisted — D-D3)
       - "api_unreachable" in result.issue_categories
       - settings_for_test.db_path.exists()
       - result.market_count == 0

    7. `test_clob_failure_writes_invalid_snapshot_but_persists_markets(settings_for_test, mocked_gamma)` —
       Use unittest.mock to make ClobClient.get_order_books raise:
       ```python
       from unittest.mock import patch
       from py_clob_client.client import ClobClient
       with patch.object(ClobClient, "get_order_books", side_effect=RuntimeError("clob down")):
           # also patch get_prices to raise
           with patch.object(ClobClient, "get_prices", side_effect=RuntimeError("clob down")):
               result = await run_snapshot(settings_for_test, mode="subset")
       ```
       Asserts:
       - result.market_count > 0 (Gamma data still persisted)
       - "api_unreachable" in result.issue_categories OR "clob_missing" in result.issue_categories
       - result.snapshot_id >= 1

    8. `test_subset_filter_excludes_low_liquidity(mocked_gamma, mocked_clob, tmp_db_path, tmp_parquet_root)` —
       Build a Settings with liquidity_threshold_usd=999_999_999 (impossibly high)
       Run subset mode
       Assert mocked_clob["books"] was called with EMPTY token list (no markets passed filter)
       (Gamma still fetched; just no CLOB calls)

    9. `test_invalid_mode_raises(settings_for_test, mocked_gamma, mocked_clob)` —
       with pytest.raises(AssertionError):
           await run_snapshot(settings_for_test, mode="weekly")

    10. `test_validation_issues_persisted_with_categories(settings_for_test, mocked_gamma, mocked_clob)` —
        Run successfully (or with intentional gamma jitter mismatch)
        Query: SELECT category, COUNT(*) FROM validation_issues WHERE snapshot_id = ? GROUP BY category
        Assert results — at minimum, every Issue has a non-empty category string

    Top-of-file imports may need: `import respx`, `from httpx import Response`, `from unittest.mock import patch`, `from py_clob_client.client import ClobClient`, `import pyarrow as pa`.
  </action>
  <verify>
    <automated>cd /Users/sujiangwen/sandbox/hacker2026/PolyMarket/polymarket-arbitrage && pytest tests/m1-perception/test_orchestrator.py -xvs 2>&1 | tail -60</automated>
  </verify>
  <done>All 10 tests pass; happy path produces valid snapshot in <5s; gamma failure → is_valid=False but persisted; clob failure → markets persist + issue categories include unreachable/missing; subset filter respects liquidity threshold; per-row fetched_at_ms set</done>
</task>

<task type="auto">
  <id>T4</id>
  <name>Task 4: Implement test_cli.py (typer flag parsing + exit codes)</name>
  <files>tests/m1-perception/test_cli.py</files>
  <read_first>
    - src/polyarb/snapshot/cli.py (Plan 4 T3)
    - tests/m1-perception/conftest.py (T1)
  </read_first>
  <action>
    Create `tests/m1-perception/test_cli.py`:

    ```python
    """Tests for polyarb.snapshot.cli (typer command)."""
    from pathlib import Path

    import pytest
    from typer.testing import CliRunner

    from polyarb.snapshot.cli import app


    runner = CliRunner(mix_stderr=False)
    ```

    Tests:

    1. `test_help_works` — `result = runner.invoke(app, ["snapshot", "--help"])`; assert exit_code == 0; assert "--full" in result.stdout; assert "--verbose" in result.stdout; assert "--config" in result.stdout

    2. `test_default_mode_is_subset(mocked_gamma, mocked_clob, tmp_path, tmp_db_path, tmp_parquet_root)` —
       Build a YAML with low liquidity threshold + temp paths:
       ```python
       yaml_path = tmp_path / "test.yaml"
       yaml_path.write_text(f"db_path: {tmp_db_path}\nparquet_root: {tmp_parquet_root}\nliquidity_threshold_usd: 100.0\nretry_attempts: 1\nretry_min_wait_s: 0.001\nretry_max_wait_s: 0.005\n")
       result = runner.invoke(app, ["snapshot", "--config", str(yaml_path)])
       ```
       Assert exit_code in (0, 1)
       Assert "mode=subset" in result.stdout
       Assert "OK" in result.stdout or "INVALID" in result.stdout

    3. `test_full_flag_sets_full_mode(mocked_gamma, mocked_clob, tmp_path, tmp_db_path, tmp_parquet_root)` —
       Same yaml as above
       result = runner.invoke(app, ["snapshot", "--full", "--config", str(yaml_path)])
       Assert "mode=full" in result.stdout

    4. `test_exit_code_zero_on_valid(mocked_gamma, mocked_clob, tmp_path, tmp_db_path, tmp_parquet_root)` —
       Run with mocks producing valid snapshot
       Assert exit_code == 0 (if Gamma fixture count matches normalized count — confirm assumption holds)

    5. `test_exit_code_one_on_invalid(tmp_path, tmp_db_path, tmp_parquet_root)` —
       Mock gamma to return 500 (no mocked_gamma fixture; do inline respx)
       result = runner.invoke(app, ["snapshot", "--config", str(yaml_path)])
       Assert exit_code == 1
       Assert "INVALID" in result.stdout
       Assert "VALIDATION FAILED" in result.stderr

    6. `test_summary_format(mocked_gamma, mocked_clob, tmp_path, tmp_db_path, tmp_parquet_root)` —
       Assert summary line matches regex like `^(OK|INVALID) \| \d+ markets \| mode=(subset|full) \| \d+ issues \| -> .*\.parquet$`

    7. `test_verbose_emits_debug_logs(mocked_gamma, mocked_clob, tmp_path, tmp_db_path, tmp_parquet_root)` —
       Run with `--verbose` and without; assert verbose stderr is longer (DEBUG > INFO)
       (If loguru's level filtering is hard to capture in CliRunner, simplify: just assert no crash and exit_code is reasonable)

    8. `test_invalid_config_path_uses_defaults(mocked_gamma, mocked_clob)` —
       Provide a --config path that doesn't exist; verify load_settings tolerates it (returns defaults). Either:
       - Document expected behavior (raise vs default)
       - If load_settings raises on missing config_path: this test asserts exit code != 0 and helpful message

       Note: From Plan 1 T3 spec: "If a path resolved and exists: open ... If no path: return Settings()". If `--config X.yaml` is passed and X.yaml doesn't exist, current behavior is undefined. Document the choice in plan summary; if it raises, this test should assert that.

    Notes:
    - `mix_stderr=False` separates stdout from stderr in CliRunner.result
    - Mocked fixtures must apply during runner.invoke — they should because patch is contextmanaged at the test function scope and runner.invoke runs in-process synchronously
    - asyncio.run inside CLI works fine inside CliRunner (it creates its own loop)
  </action>
  <verify>
    <automated>cd /Users/sujiangwen/sandbox/hacker2026/PolyMarket/polymarket-arbitrage && pytest tests/m1-perception/test_cli.py -xvs 2>&1 | tail -40</automated>
  </verify>
  <done>All ≥7 CLI tests pass; --help shows all 3 flags; --full flag sets mode=full; exit code 0/1 matches is_valid; stdout summary line matches expected format; stderr emits validation failure detail</done>
</task>

<task type="auto">
  <id>T5</id>
  <name>Task 5: Implement test_integration.py (full pipeline + Makefile dry-run)</name>
  <files>tests/m1-perception/test_integration.py</files>
  <read_first>
    - all Plan 4 outputs
    - tests/m1-perception/conftest.py (T1)
    - Makefile
  </read_first>
  <action>
    Create `tests/m1-perception/test_integration.py`:

    ```python
    """End-to-end integration: CLI → orchestrator → SQLite + Parquet outputs.

    These tests verify the FULL phase 1 pipeline works with mocked external APIs.
    They are the gate that fails Phase 1 if any earlier plan regresses.
    """
    import json
    import os
    import sqlite3
    import subprocess
    from pathlib import Path

    import pyarrow.parquet as pq
    import pytest
    from typer.testing import CliRunner

    from polyarb.snapshot.cli import app


    runner = CliRunner(mix_stderr=False)


    @pytest.fixture
    def yaml_config(tmp_path, tmp_db_path, tmp_parquet_root) -> Path:
        path = tmp_path / "snapshot.yaml"
        path.write_text(
            f"db_path: {tmp_db_path}\n"
            f"parquet_root: {tmp_parquet_root}\n"
            f"liquidity_threshold_usd: 100.0\n"
            f"retry_attempts: 1\n"
            f"retry_min_wait_s: 0.001\n"
            f"retry_max_wait_s: 0.005\n"
            f"http_timeout_s: 2.0\n"
        )
        return path
    ```

    Tests:

    1. `test_full_pipeline_subset_mode(mocked_gamma, mocked_clob, yaml_config, tmp_db_path, tmp_parquet_root)` —
       result = runner.invoke(app, ["snapshot", "--config", str(yaml_config)])
       Assert exit_code in (0, 1)  # 0 if no Layer 1 jitter; 1 if jitter present
       Assert tmp_db_path.exists()
       Assert any(tmp_parquet_root.rglob("*.parquet"))  # at least one parquet file
       # Verify SQLite has expected structure
       con = sqlite3.connect(tmp_db_path)
       row = con.execute("SELECT mode, market_count, parquet_path FROM snapshots ORDER BY id DESC LIMIT 1").fetchone()
       assert row[0] == "subset"
       assert row[1] >= 1  # at least one market made it through
       parquet_path = Path(row[2])
       assert parquet_path.exists()
       # Read parquet back
       table = pq.read_table(parquet_path)
       assert table.num_rows == row[1]

    2. `test_full_pipeline_full_mode(mocked_gamma, mocked_clob, yaml_config, tmp_db_path, tmp_parquet_root)` —
       result = runner.invoke(app, ["snapshot", "--full", "--config", str(yaml_config)])
       Same assertions as test 1 but assert SQLite mode column is "full"

    3. `test_full_pipeline_completes_in_under_30s(mocked_gamma, mocked_clob, yaml_config)` —
       import time
       start = time.monotonic()
       runner.invoke(app, ["snapshot", "--config", str(yaml_config)])
       elapsed = time.monotonic() - start
       assert elapsed < 30.0, f"Pipeline took {elapsed:.1f}s, expected < 30s"

    4. `test_validation_issues_in_sqlite(mocked_gamma, mocked_clob, yaml_config, tmp_db_path)` —
       runner.invoke(app, ["snapshot", "--config", str(yaml_config)])
       con = sqlite3.connect(tmp_db_path)
       # Every issue must have non-empty category (D-D4)
       cats = con.execute("SELECT DISTINCT category FROM validation_issues").fetchall()
       for (cat,) in cats:
           assert cat and len(cat) > 0, f"Empty category found"

    5. `test_makefile_target_dry_run_subset()` —
       result = subprocess.run(["make", "-n", "snapshot-markets"], capture_output=True, text=True, cwd=Path(__file__).parent.parent.parent)
       assert result.returncode == 0
       assert "python -m polyarb.snapshot" in result.stdout
       assert "--full" not in result.stdout

    6. `test_makefile_target_dry_run_full()` —
       result = subprocess.run(["make", "-n", "snapshot-markets-full"], capture_output=True, text=True, cwd=Path(__file__).parent.parent.parent)
       assert result.returncode == 0
       assert "python -m polyarb.snapshot --full" in result.stdout

    7. `test_make_help_lists_new_targets()` —
       result = subprocess.run(["make", "help"], capture_output=True, text=True, cwd=Path(__file__).parent.parent.parent)
       assert result.returncode == 0
       assert "snapshot-markets:" in result.stdout
       assert "snapshot-markets-full:" in result.stdout

    8. `test_parquet_token_id_string_preserved(mocked_gamma, mocked_clob, yaml_config, tmp_parquet_root)` —
       Run pipeline; find parquet file; read; assert yes_token_id column type is string (Pitfall 3 end-to-end)

    9. `test_idempotent_re_run_overwrites_markets(mocked_gamma, mocked_clob, yaml_config, tmp_db_path)` —
       Run pipeline twice with same config
       Query: SELECT COUNT(*) FROM snapshots — should be 2
       Query: SELECT COUNT(*) FROM markets — should equal latest snapshot's market_count, NOT cumulative
       (Verifies D-C1 overwrite semantics end-to-end)

       Note: re-running through CliRunner re-mocks; ensure fixture pagination still terminates on second run. May need to re-prime mocked_gamma's side_effect inside this test.

    Important note about test 9: the `mocked_gamma` and `mocked_clob` fixtures are function-scoped and reset per test. To run twice within ONE test, the fixtures may not provide enough side_effect entries. Either:
    - Set up a custom respx mock inside this test with enough side_effects for 2 runs
    - Use `mock.return_value = gamma_fixture` (not side_effect) so it returns the same data N times — but that breaks pagination termination

    Resolution: in test 9, set up mocks inline (not via fixture) and configure them to return fixture data + empty list on alternating calls so pagination terminates each time.
  </action>
  <verify>
    <automated>cd /Users/sujiangwen/sandbox/hacker2026/PolyMarket/polymarket-arbitrage && pytest tests/m1-perception/test_integration.py -xvs 2>&1 | tail -50</automated>
  </verify>
  <done>All 9 integration tests pass; full pipeline completes in <30s; SQLite has snapshot rows with correct mode; Parquet readable; markets table overwritten on second run; Makefile targets resolve and appear in `make help`</done>
</task>

<task type="auto">
  <id>T6</id>
  <name>Task 6: Run full m1-perception test suite + final smoke checks</name>
  <files></files>
  <read_first>
    - all Plan 1-5 source files
    - Makefile
  </read_first>
  <action>
    No new files. Final validation step — runs all m1-perception tests + Makefile dry-runs + import smoke checks.

    Execute these commands sequentially. If ANY fail, STOP and surface the failure to the user (do not auto-fix — the failure indicates a regression in an earlier plan that needs deliberate revision):

    ```bash
    # Phase 1 test suite — full run, must complete green in < 30s
    pytest tests/m1-perception -xvs --durations=10

    # Module imports clean
    python -c "
    from polyarb import config, cli
    from polyarb.clients import gamma_client, clob_client
    from polyarb.storage import schemas, sqlite_store, parquet_writer
    from polyarb.validator import category, layers
    from polyarb.snapshot import normalizer, orchestrator, cli as snap_cli
    print('ALL_IMPORTS_OK')
    "

    # CLI help
    python -m polyarb.snapshot --help
    polyarb --help
    polyarb snapshot --help

    # Makefile contract
    make help | grep snapshot-markets
    make -n snapshot-markets
    make -n snapshot-markets-full

    # Ruff check
    ruff check src/polyarb tests/m1-perception
    ```

    DO NOT run `make snapshot-markets` (would hit live APIs). The user runs that as a manual real-world verification step after this phase ships.

    If all commands succeed, write 01-5-SUMMARY.md with:
    - Total test count + breakdown
    - Total duration
    - Any flaky tests / known issues
    - Confirmation that the manual `make snapshot-markets` smoke step is now safe to run
  </action>
  <verify>
    <automated>cd /Users/sujiangwen/sandbox/hacker2026/PolyMarket/polymarket-arbitrage && pytest tests/m1-perception -x --durations=10 2>&1 | tail -20 && python -c "from polyarb.snapshot.cli import app; from polyarb.snapshot.orchestrator import run_snapshot; print('OK')" && make -n snapshot-markets > /dev/null && make -n snapshot-markets-full > /dev/null && echo PHASE_1_GATE_OK</automated>
  </verify>
  <done>Full pytest suite green (≥60 tests across all 5 plans); duration < 30s; all module imports succeed; CLI help works on both invocation paths; both Makefile targets resolve; ruff passes</done>
</task>

</tasks>

## Verification

```bash
# The phase 1 acceptance gate:
pytest tests/m1-perception -x --durations=10
python -m polyarb.snapshot --help
polyarb snapshot --help
make help | grep snapshot
make -n snapshot-markets
make -n snapshot-markets-full
ruff check src/polyarb tests/m1-perception
```

All must pass. Total wall-clock time for the test suite: <30s.

The user will then run `make snapshot-markets` against live APIs as a separate manual verification step (intentionally outside the automated gate per CONTEXT.md scope — Phase 1 ships when the mocked-pipeline gate is green).

## Success Criteria

- 5 test files exist, all populated
- Full pytest suite green: ≥60 tests across all phase 1 plans (~5 skeleton + 11 clients + 34 storage/validator + 12 normalizer + 10 orchestrator + 7+ CLI + 9 integration)
- Suite completes in <30s
- No real network calls during test suite
- Makefile contract verified end-to-end (test_makefile_target_dry_run + make help inspection)
- Pyarrow + SQLite outputs verified by direct read-back
- D-D3 (is_valid=false still persists) verified end-to-end
- D-C1 (overwrite semantics) verified end-to-end (test 9)

## must_haves (this plan delivers)

- Phase outcomes 1, 2, 8 (test suite green, mocked-pipeline contract holds, Makefile gates verified)

<output>
Create `.planning/workstreams/m1-perception/phases/01-/01-5-SUMMARY.md` documenting:
- Final test count breakdown by file (skeleton/clients/storage/validator/normalizer/orchestrator/cli/integration)
- Total suite wall-clock duration on the dev machine
- Any tests that needed adjustment to match actual mocked-data shape (likely test 1 in test_orchestrator if Layer 1 jitter occurs because the recorded gamma_sample only has 5 markets but the orchestrator may fetch a paginated empty page that throws off counts)
- Whether the recorded fixtures from Plan 2 T1 needed re-recording during Plan 5
- Confirmation: `make snapshot-markets` is now safe for the user to run as the manual real-world verification gate
- Suggested follow-up: did any open question from RESEARCH.md / PATTERNS.md get answered during integration testing? (Likely Q3 — CLOB rate limit interaction; Q5 — liquidity field actual values)
</output>
