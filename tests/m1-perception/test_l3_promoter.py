"""RED → GREEN tests for the Phase 05 Plan 04 L3 promoter.

Covers:

- l3-promote.yaml schema + Blocker #2 ts predicate (INTEGER epoch ms)
- scanner.py docstring documents trusted-yaml tier (Warning #10)
- promote_run happy / freeze / fail-soft paths
- 5 markets → 10 tokens Yes+No expansion (D-05 / Warning #13)
- l2_candidates.l3_promoted_at_ts write-through (Blocker #1)
- run_periodic raw asyncio.wait_for loop (no apscheduler — Warning #4)

The recipe runs against scanner.run_recipe which is HARD-CODED to query
``FROM markets m LEFT JOIN question_translations qt`` (see scanner.py:142-156).
Therefore the L3 promoter's temp DB must create a ``markets`` table holding
the tob columns (asset_id, ts, spread, depth_yes_usd, …) plus an empty
``question_translations`` table for the LEFT JOIN — this is the Plan 04
adapter pattern documented in l3_promote._build_tob_temp_db.
"""

from __future__ import annotations

import asyncio
import inspect
import os
import sqlite3
import threading
import time
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest
import yaml

from polyarb.observation.l3_evidence import AcceptanceConfig, PreparedL3Target

# Test-mode env hatches (matches conftest.py module-top setdefault).
os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")


RECIPE_PATH = (
    Path(__file__).resolve().parents[2] / "src" / "polyarb" / "scan_recipes" / "l3-promote.yaml"
)


@pytest.fixture(autouse=True)
def _reset_l3_promote_state() -> Any:
    """Reset l3_promote module-level state between tests (state is global)."""
    from polyarb.observation import l3_promote

    l3_promote._l3_active_set = set()
    l3_promote._last_promote_at_s = None
    l3_promote._last_book_levels_write_at_s = None
    # Plan 04 augment additions — only assert presence if defined
    if hasattr(l3_promote, "_last_known_tob_rows"):
        l3_promote._last_known_tob_rows = None
    if hasattr(l3_promote, "_last_known_market_token_map"):
        l3_promote._last_known_market_token_map = None
    if hasattr(l3_promote, "_last_mirrored_market_ids"):
        l3_promote._last_mirrored_market_ids = frozenset()
    yield


def _make_settings(
    *,
    supabase_url: str = "https://x.supabase.co",
    service_key: str = "test-service-key",
) -> Any:
    """Settings with auto-detected Supabase enabled (matches conftest pattern)."""
    from pydantic import SecretStr

    from polyarb.config import Settings

    return Settings(
        supabase_url=supabase_url,
        supabase_service_key=SecretStr(service_key) if service_key else "",
    )


def _make_runtime(settings: Any):
    from polyarb.observation.l3_evidence import (
        AcceptanceConfig,
        L3EvidenceRuntime,
        RuntimeIdentity,
    )

    acceptance = AcceptanceConfig.from_settings(settings, RECIPE_PATH, "test-code")
    return L3EvidenceRuntime(
        RuntimeIdentity(
            machine_id="test-machine",
            machine_version="v1",
            image_ref="test-image",
            release_id="test-release",
            code_version="test-code",
            recipe_sha256=acceptance.recipe_sha256,
            acceptance_config_hash=acceptance.digest(),
        ),
        started_at=datetime(2026, 7, 23, tzinfo=UTC),
    )


class _RecordingEvidenceStore:
    def __init__(
        self,
        *,
        succeeds: bool = True,
        mapping_lock: Any = None,
        lock_error: BaseException | None = None,
    ) -> None:
        self.succeeds = succeeds
        self.mapping_lock = mapping_lock
        self.lock_error = lock_error
        self.records: list[Any] = []
        self.lock_reads: list[tuple[Any, datetime]] = []

    async def append_promote_run(self, record: Any) -> bool:
        self.records.append(record)
        return self.succeeds

    async def fetch_active_soak_mapping_lock(
        self,
        *,
        boot_id: Any,
        observed_at: datetime,
    ) -> Any:
        self.lock_reads.append((boot_id, observed_at))
        if self.lock_error is not None:
            raise self.lock_error
        return self.mapping_lock


def _truthful_consumer(
    *,
    initial_committed: set[str] | None = None,
    add_succeeds: bool = True,
    remove_succeeds: bool = True,
) -> MagicMock:
    from polyarb.observation.l3_evidence import WsMembershipSnapshot

    state: dict[str, Any] = {
        "generation": 7,
        "desired": set(initial_committed or ()),
        "committed": set(initial_committed or ()),
        "evidenced": set(),
        "evidenced_at": {},
    }
    consumer = MagicMock()

    def _set_desired(asset_ids: Any) -> None:
        state["desired"] = set(asset_ids)

    async def _add(asset_ids: list[str]) -> bool:
        if add_succeeds:
            state["committed"].update(asset_ids)
        return add_succeeds

    async def _remove(asset_ids: list[str]) -> bool:
        if remove_succeeds:
            state["committed"].difference_update(asset_ids)
        return remove_succeeds

    async def _prepare(asset_ids: frozenset[str]) -> PreparedL3Target:
        observed_at = datetime.now(UTC)
        return PreparedL3Target(
            generation=state["generation"],
            asset_ids=asset_ids,
            evidenced_at={asset_id: observed_at for asset_id in asset_ids},
        )

    async def _commit(prepared: PreparedL3Target) -> bool:
        added = prepared.asset_ids - state["committed"]
        removed = state["committed"] - prepared.asset_ids
        if (added and not add_succeeds) or (removed and not remove_succeeds):
            return False
        state["desired"] = set(prepared.asset_ids)
        state["committed"] = set(prepared.asset_ids)
        state["evidenced"] = set(prepared.asset_ids)
        state["evidenced_at"] = dict(prepared.evidenced_at)
        return True

    async def _compensate_current_generation(*, reason_code: str) -> None:
        del reason_code
        state["committed"] = set()
        state["evidenced"] = set()
        state["evidenced_at"] = {}

    def _snapshot() -> Any:
        return WsMembershipSnapshot(
            generation=state["generation"],
            desired=frozenset(state["desired"]),
            committed=frozenset(state["committed"]),
            evidenced=frozenset(state["evidenced"]),
            evidenced_at=state["evidenced_at"],
        )

    consumer._test_state = state
    consumer.set_l3_desired = MagicMock(side_effect=_set_desired)
    consumer.add_subscriptions = AsyncMock(side_effect=_add)
    consumer.remove_subscriptions = AsyncMock(side_effect=_remove)
    consumer.prepare_l3_target = AsyncMock(side_effect=_prepare)
    consumer.commit_l3_target = AsyncMock(side_effect=_commit)
    consumer.compensate_current_generation = AsyncMock(
        side_effect=_compensate_current_generation
    )
    consumer.l3_membership_snapshot = MagicMock(side_effect=_snapshot)
    return consumer


async def _promote_mutating(module: Any, *, settings: Any, **kwargs: Any) -> Any:
    return await module.promote_run(
        settings=settings,
        evidence_store=_RecordingEvidenceStore(),
        evidence_runtime=_make_runtime(settings),
        **kwargs,
    )


# ────────────────────────────────────────────────────────────────────────
# Test 1 — l3-promote.yaml schema
# ────────────────────────────────────────────────────────────────────────


def test_l3_promote_yaml_parses() -> None:
    """yaml file exists with D-13 thresholds + INTEGER epoch ms ts predicate."""
    assert RECIPE_PATH.exists(), f"l3-promote.yaml missing at {RECIPE_PATH}"

    with RECIPE_PATH.open() as f:
        data = yaml.safe_load(f)

    assert "recipes" in data, "yaml must have top-level 'recipes' key"
    assert "l3-promote" in data["recipes"], "recipes must include 'l3-promote'"

    body = data["recipes"]["l3-promote"]
    assert "description" in body, "recipe must have description"

    where = body["where"]
    assert "spread < 0.02" in where, "D-13 spread threshold missing"
    assert "depth_yes_usd > 500" in where, "D-13 depth threshold missing"
    assert "strftime" in where, "Blocker #2 — strftime epoch ms predicate missing"
    assert "* 1000" in where, "Blocker #2 — epoch-ms scaling missing"
    assert "datetime('now'" not in where, (
        "Blocker #2 anti-regression — lex comparison anti-pattern present"
    )

    assert body["order_by"].strip() == "depth_yes_usd DESC", body["order_by"]
    assert int(body["limit"]) == 5, body["limit"]


# ────────────────────────────────────────────────────────────────────────
# Test 2 — scanner.py docstring documents trusted-yaml tier (Warning #10)
# ────────────────────────────────────────────────────────────────────────


def test_scanner_documents_trusted_yaml_tier() -> None:
    """scanner.py module docstring must document the 3rd recipe trust tier."""
    scanner_path = (
        Path(__file__).resolve().parents[2] / "src" / "polyarb" / "observation" / "scanner.py"
    )
    text = scanner_path.read_text()
    assert "Source-controlled yaml" in text or "trusted-yaml" in text, (
        "scanner.py must document trusted-yaml tier per Warning #10"
    )


# ────────────────────────────────────────────────────────────────────────
# Test 3 — recipe ts predicate filters synthetic rows (Blocker #2)
# ────────────────────────────────────────────────────────────────────────


def test_recipe_ts_predicate_filters_synthetic_rows_correctly() -> None:
    """Build a temp DB with one row 30min ago, one row 90min ago. The recipe
    predicate `ts > strftime('%s','now','-1 hour') * 1000` must keep the
    30min row and drop the 90min row.
    """
    from polyarb.observation import l3_promote

    now_ms = int(time.time() * 1000)
    rows = [
        {
            "asset_id": "a",
            "ts": now_ms - 30 * 60 * 1000,  # 30 min ago — within 1h window
            "best_bid": 0.50,
            "best_ask": 0.51,
            "spread": 0.01,
            "mid_price": 0.505,
            "depth_yes_usd": 1000.0,
            "depth_no_usd": 1000.0,
        },
        {
            "asset_id": "b",
            "ts": now_ms - 90 * 60 * 1000,  # 90 min ago — outside window
            "best_bid": 0.50,
            "best_ask": 0.51,
            "spread": 0.01,
            "mid_price": 0.505,
            "depth_yes_usd": 1000.0,
            "depth_no_usd": 1000.0,
        },
    ]

    db_path = l3_promote._build_tob_temp_db(rows)
    try:
        recipe = l3_promote._load_recipe(RECIPE_PATH)
        from polyarb.observation.scanner import run_recipe

        df = run_recipe(db_path, recipe)
    finally:
        try:
            os.unlink(db_path)
        except OSError:
            pass

    # Row B must be filtered out (90min > 1h window)
    asset_ids = sorted(str(x) for x in df["asset_id"].tolist())
    assert asset_ids == ["a"], f"expected only 'a' (recent), got {asset_ids}"


def test_runtime_database_numeric_rows_are_sqlite_compatible() -> None:
    from decimal import Decimal

    from polyarb.observation import l3_promote

    observed_at = datetime.now(UTC)
    db_path = l3_promote._build_tob_temp_db(
        [
            {
                "asset_id": "decimal-asset",
                "ts": observed_at,
                "best_bid": Decimal("0.50"),
                "best_ask": Decimal("0.51"),
                "spread": Decimal("0.01"),
                "mid_price": Decimal("0.505"),
                "depth_yes_usd": Decimal("1000"),
                "depth_no_usd": Decimal("900"),
            }
        ]
    )
    try:
        with sqlite3.connect(db_path) as connection:
            row = connection.execute(
                "SELECT ts,best_bid,depth_yes_usd FROM markets"
            ).fetchone()
        assert row == (int(observed_at.timestamp() * 1000), 0.5, 1000.0)
    finally:
        os.unlink(db_path)


def test_fetch_latest_tob_rows_keeps_one_newest_row_per_asset() -> None:
    """The recipe limit counts markets, not repeated snapshots of one market."""
    from polyarb.observation import l3_promote

    rows = [
        {"asset_id": "a", "ts": "2026-07-20T10:02:00Z", "spread": 0.01},
        {"asset_id": "a", "ts": "2026-07-20T10:01:00Z", "spread": 0.02},
        {"asset_id": "b", "ts": "2026-07-20T10:00:00Z", "spread": 0.03},
    ]
    client = _make_supabase_client_mock(rows, [])

    latest = l3_promote._fetch_latest_tob_rows_from_supabase(client)

    assert latest == [rows[0], rows[2]]


# ────────────────────────────────────────────────────────────────────────
# Test 3b — Quick task 260602-diag-depth: env override for depth threshold
# ────────────────────────────────────────────────────────────────────────


def test_load_recipe_default_threshold_is_yaml_baseline(monkeypatch) -> None:
    """Without POLYARB_L3_DEPTH_MIN_USD env, recipe WHERE keeps yaml default 500.

    Baseline invariant (CLAUDE.md "experiment values never touch baseline defaults"):
    the yaml file is the source of truth; absence of env must not change anything.
    """
    from polyarb.observation import l3_promote

    monkeypatch.delenv("POLYARB_L3_DEPTH_MIN_USD", raising=False)
    recipe = l3_promote._load_recipe(RECIPE_PATH)
    assert "depth_yes_usd > 500" in recipe.where, (
        f"baseline must remain depth_yes_usd > 500 when env unset; got: {recipe.where!r}"
    )


def test_load_recipe_env_override_substitutes_threshold(monkeypatch) -> None:
    """With POLYARB_L3_DEPTH_MIN_USD=0, recipe WHERE swaps to depth_yes_usd > 0.

    Diagnostic surface for prod bug 2 (Phase 05 Wave 5 carry): we cannot start
    24h soak with N=0 active and the only way to learn what's actually in the
    tob table is to drop the depth filter and observe. Override path keeps the
    yaml default clean.
    """
    from polyarb.observation import l3_promote

    monkeypatch.setenv("POLYARB_L3_DEPTH_MIN_USD", "0")
    recipe = l3_promote._load_recipe(RECIPE_PATH)
    assert "depth_yes_usd > 0" in recipe.where, (
        f"override should rewrite threshold to 0; got: {recipe.where!r}"
    )
    assert "depth_yes_usd > 500" not in recipe.where, (
        f"baseline 500 must be replaced, not kept alongside; got: {recipe.where!r}"
    )


def test_load_recipe_env_override_invalid_falls_back_to_yaml(monkeypatch) -> None:
    """An unparseable env value must NOT silently break the recipe — fall back
    to yaml baseline + log a warning. Defensive: a typo in fly secret shouldn't
    turn off the L3 promoter."""
    from polyarb.observation import l3_promote

    monkeypatch.setenv("POLYARB_L3_DEPTH_MIN_USD", "not-a-number")
    recipe = l3_promote._load_recipe(RECIPE_PATH)
    assert "depth_yes_usd > 500" in recipe.where, (
        f"invalid env must fall back to yaml baseline; got: {recipe.where!r}"
    )


# ────────────────────────────────────────────────────────────────────────
# Test 4 — Supabase creds missing → _l3_active_set frozen (Open Q #5)
# ────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_promote_run_no_supabase_keeps_l3_active_set() -> None:
    from polyarb.observation import l3_promote

    l3_promote._l3_active_set = {"x", "y"}
    l3_promote._last_promote_at_s = 1000.0

    settings = _make_settings(supabase_url="", service_key="")
    mock_consumer = _truthful_consumer(initial_committed={"x", "y"})

    result = await _promote_mutating(
        l3_promote, settings=settings, ws_consumer=mock_consumer, recipe_yaml_path=RECIPE_PATH
    )

    assert l3_promote._l3_active_set == {"x", "y"}, "set must freeze"
    assert l3_promote._last_promote_at_s > 1000.0, "durable frozen terminal advances anchor"
    mock_consumer.add_subscriptions.assert_not_called()
    mock_consumer.remove_subscriptions.assert_not_called()
    assert result.get("skipped"), f"result should indicate skip: {result}"


# ────────────────────────────────────────────────────────────────────────
# Test 5 — Supabase outage → _l3_active_set frozen
# ────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_promote_run_supabase_outage_freezes_set() -> None:
    from polyarb.observation import l3_promote

    l3_promote._l3_active_set = {"a"}
    settings = _make_settings()

    mock_consumer = _truthful_consumer(initial_committed={"a"})

    # create_client raises — simulates Supabase outage at client init.
    with patch(
        "polyarb.observation.l3_promote.create_client",
        side_effect=RuntimeError("503 Service Unavailable"),
    ):
        # No crash; _l3_active_set frozen.
        await _promote_mutating(
            l3_promote,
            settings=settings,
            ws_consumer=mock_consumer,
            recipe_yaml_path=RECIPE_PATH,
        )

    assert l3_promote._l3_active_set == {"a"}, "set must freeze on outage"


# ────────────────────────────────────────────────────────────────────────
# Helpers for happy-path / diff tests — mock supabase client
# ────────────────────────────────────────────────────────────────────────


def _with_market_ids(rows: list[dict]) -> list[dict]:
    return [
        {
            "market_id": row.get("market_id") or f"market:{row['yes_token_id']}",
            **row,
        }
        for row in rows
    ]


def _make_supabase_client_mock(
    tob_rows: list[dict],
    token_map_rows: list[dict],
    capture_updates: list[dict] | None = None,
) -> MagicMock:
    """Build a MagicMock supabase client that yields tob_rows for
    l2_top_of_book, token_map_rows for markets_latest, and captures any
    update() calls on l2_candidates into capture_updates (if provided)."""
    capture = capture_updates if capture_updates is not None else []

    def _table_side_effect(name: str) -> MagicMock:
        tbl = MagicMock()
        if name == "l2_top_of_book":
            tbl.select.return_value = tbl
            tbl.gte.return_value = tbl
            tbl.order.return_value = tbl
            tbl.limit.return_value = tbl
            tbl.execute.return_value = MagicMock(data=list(tob_rows))
        elif name == "markets_latest":
            tbl.select.return_value = tbl
            tbl.in_.return_value = tbl
            tbl.execute.return_value = MagicMock(data=_with_market_ids(token_map_rows))
        elif name == "l2_candidates":
            # update(...).in_(...).execute() — capture the update dict + ids.
            class _UpdateChain:
                def __init__(self, payload: dict) -> None:
                    self._payload = payload

                def in_(self, col: str, ids: list[str]) -> _UpdateChain:
                    capture.append({"payload": self._payload, "col": col, "ids": list(ids)})
                    return self

                def execute(self) -> MagicMock:
                    return MagicMock(data=[])

            tbl.update.side_effect = lambda payload: _UpdateChain(payload)
        else:
            tbl.execute.return_value = MagicMock(data=[])
        return tbl

    client = MagicMock()
    client.table.side_effect = _table_side_effect
    return client


def _make_bounded_reconciliation_client(
    tob_rows: list[dict],
    token_map_rows: list[dict],
    promoted_rows: list[str],
    capture_updates: list[dict],
    capture_queries: list[dict],
    *,
    fail_query_once: bool = False,
) -> MagicMock:
    """Stateful candidate-table double for bounded badge reconciliation.

    ``promoted_rows`` deliberately remains a list so tests can model multiple
    historical ``l2_candidates`` rows for one asset identity.
    """
    remaining_query_failures = 1 if fail_query_once else 0

    class _CandidateUpdate:
        def __init__(self, payload: dict) -> None:
            self._payload = payload
            self._ids: list[str] = []

        def in_(self, column: str, ids: list[str]) -> _CandidateUpdate:
            assert column == "asset_id"
            self._ids = list(ids)
            capture_updates.append({"payload": self._payload, "col": column, "ids": list(ids)})
            return self

        def execute(self) -> MagicMock:
            value = self._payload.get("l3_promoted_at_ts")
            if value is None:
                stale = set(self._ids)
                promoted_rows[:] = [row for row in promoted_rows if row not in stale]
            else:
                existing = set(promoted_rows)
                promoted_rows.extend(row for row in self._ids if row not in existing)
            return MagicMock(data=[])

    class _CandidateSelect:
        def __init__(self) -> None:
            self._exclude: set[str] = set()
            self._limit: int | None = None
            self._ordered_by: str | None = None
            self._negate_next = False

        def select(self, columns: str) -> _CandidateSelect:
            assert columns == "asset_id"
            return self

        @property
        def not_(self) -> _CandidateSelect:
            self._negate_next = True
            return self

        def is_(self, column: str, value: Any) -> _CandidateSelect:
            assert self._negate_next
            assert (column, value) == ("l3_promoted_at_ts", "null")
            self._negate_next = False
            return self

        def in_(self, column: str, ids: list[str]) -> _CandidateSelect:
            assert self._negate_next
            assert column == "asset_id"
            self._exclude = set(ids)
            self._negate_next = False
            return self

        def order(self, column: str) -> _CandidateSelect:
            self._ordered_by = column
            return self

        def limit(self, size: int) -> _CandidateSelect:
            self._limit = size
            return self

        def execute(self) -> MagicMock:
            nonlocal remaining_query_failures
            if remaining_query_failures:
                remaining_query_failures -= 1
                raise RuntimeError("candidate badge query failed")
            assert self._ordered_by == "id"
            assert self._limit is not None
            rows = [row for row in promoted_rows if row not in self._exclude]
            returned = rows[: self._limit]
            capture_queries.append(
                {
                    "limit": self._limit,
                    "excluded": set(self._exclude),
                    "returned": list(returned),
                }
            )
            return MagicMock(data=[{"asset_id": row} for row in returned])

    def _table_side_effect(name: str) -> Any:
        if name == "l2_candidates":
            table = _CandidateSelect()
            table.update = lambda payload: _CandidateUpdate(payload)  # type: ignore[attr-defined]
            return table
        return _make_supabase_client_mock(tob_rows, token_map_rows).table(name)

    client = MagicMock()
    client.table.side_effect = _table_side_effect
    return client


def test_fetch_market_token_map_queries_real_production_columns() -> None:
    from polyarb.observation import l3_promote

    table = MagicMock()
    table.select.return_value = table
    table.in_.return_value = table
    table.execute.return_value = MagicMock(
        data=[
            {
                "market_id": "market-1",
                "yes_token_id": "yes-1",
                "no_token_id": "no-1",
            }
        ]
    )
    client = MagicMock()
    client.table.return_value = table

    result = l3_promote._fetch_market_token_map(client, ["yes-1"])

    client.table.assert_called_once_with("markets_latest")
    table.select.assert_called_once_with("market_id, yes_token_id, no_token_id")
    table.in_.assert_called_once_with("yes_token_id", ["yes-1"])
    assert result == {"yes-1": ("market-1", "yes-1", "no-1")}
    assert l3_promote._mapping_rows({"yes-1"}, result) == (
        {
            "market_id": "market-1",
            "yes_token_id": "yes-1",
            "no_token_id": "no-1",
        },
    )
    assert l3_promote._mapping_rows(
        {"yes-a", "yes-z"},
        {
            "yes-a": ("market-z", "yes-a", "no-a"),
            "yes-z": ("market-a", "yes-z", "no-z"),
        },
    ) == (
        {
            "market_id": "market-a",
            "yes_token_id": "yes-z",
            "no_token_id": "no-z",
        },
        {
            "market_id": "market-z",
            "yes_token_id": "yes-a",
            "no_token_id": "no-a",
        },
    )


@pytest.mark.asyncio
async def test_soak_mapping_lock_keeps_bound_mapping_when_dynamic_top_five_changes() -> None:
    from polyarb.observation import l3_promote
    from polyarb.observation.l3_evidence import SoakMappingLock, stable_sha256

    settings = _make_settings()
    runtime = _make_runtime(settings)
    locked_tokens = {
        token for index in range(5) for token in (f"yes_locked_{index}", f"no_locked_{index}")
    }
    locked_map = {
        f"yes_locked_{index}": (
            f"market-locked-{index}",
            f"yes_locked_{index}",
            f"no_locked_{index}",
        )
        for index in range(5)
    }
    locked_rows = l3_promote._mapping_rows(set(locked_map), locked_map)
    mapping_hash = stable_sha256(list(locked_rows))
    now = datetime.now(UTC)
    store = _RecordingEvidenceStore(
        mapping_lock=SoakMappingLock(
            mapping_hash=mapping_hash,
            t0=now - timedelta(minutes=1),
            t24=now + timedelta(hours=24),
        )
    )
    consumer = _truthful_consumer(initial_committed=locked_tokens)
    consumer._test_state["evidenced"] = set(locked_tokens)
    consumer._test_state["evidenced_at"] = {token: now for token in locked_tokens}
    l3_promote._l3_active_set = set(locked_tokens)
    l3_promote._last_known_market_token_map = locked_map
    dynamic_tob, dynamic_mapping = _five_market_inputs("dynamic")

    with patch.object(
        l3_promote,
        "create_client",
        return_value=_make_supabase_client_mock(dynamic_tob, dynamic_mapping),
    ):
        result = await l3_promote.promote_run(
            settings=settings,
            ws_consumer=consumer,
            recipe_yaml_path=RECIPE_PATH,
            evidence_store=store,
            evidence_runtime=runtime,
            run_seq=100,
        )

    assert (result.status.value, result.reason_code) == ("success", "ok")
    assert result.desired == frozenset(locked_tokens)
    assert result.committed == frozenset(locked_tokens)
    assert result.evidenced == frozenset(locked_tokens)
    assert store.records[0].mapping_hash == mapping_hash
    assert store.records[0].selected_count == 5
    assert store.lock_reads == [(runtime.snapshot().boot_id, store.lock_reads[0][1])]
    consumer.add_subscriptions.assert_not_awaited()
    consumer.remove_subscriptions.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["mismatch", "read_error"])
async def test_soak_mapping_lock_failure_terminalizes_without_control_mutation(
    failure: str,
) -> None:
    from polyarb.observation import l3_promote
    from polyarb.observation.l3_evidence import SoakMappingLock
    from polyarb.storage.l3_evidence_store import L3EvidenceReadError

    settings = _make_settings()
    runtime = _make_runtime(settings)
    tokens = {
        token for index in range(5) for token in (f"yes_locked_{index}", f"no_locked_{index}")
    }
    consumer = _truthful_consumer(initial_committed=tokens)
    now = datetime.now(UTC)
    store = _RecordingEvidenceStore(
        mapping_lock=SoakMappingLock(
            mapping_hash="f" * 64,
            t0=now - timedelta(minutes=1),
            t24=now + timedelta(hours=24),
        ),
        lock_error=(L3EvidenceReadError("redacted") if failure == "read_error" else None),
    )

    result = await l3_promote.promote_run(
        settings=settings,
        ws_consumer=consumer,
        recipe_yaml_path=RECIPE_PATH,
        evidence_store=store,
        evidence_runtime=runtime,
        run_seq=101,
    )

    assert result.status.value == "failed"
    assert result.reason_code in {
        "soak_mapping_lock_mismatch",
        "soak_mapping_lock_read_failed",
    }
    assert len(store.records) == 1
    consumer.set_l3_desired.assert_not_called()
    consumer.add_subscriptions.assert_not_awaited()
    consumer.remove_subscriptions.assert_not_awaited()


# ────────────────────────────────────────────────────────────────────────
# Test 6 — happy path: 5 markets → 10 tokens, l2_candidates write-through
# ────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_promote_run_happy_path_top_5_markets_expanded_to_10_tokens_yes_no() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings()
    now_ms = int(time.time() * 1000)

    # 5 markets passing thresholds, varying depths so ORDER BY DESC is deterministic.
    tob_rows = [
        {
            "asset_id": f"yes_m{i}",
            "ts": now_ms - 5 * 60 * 1000,
            "best_bid": 0.50,
            "best_ask": 0.51,
            "spread": 0.01,
            "mid_price": 0.505,
            "depth_yes_usd": 5000.0 - i * 100,  # m0 > m1 > m2 > m3 > m4
            "depth_no_usd": 5000.0,
        }
        for i in range(5)
    ]
    # Add 3 markets that fail thresholds — must be filtered out.
    tob_rows.extend(
        [
            {
                "asset_id": "fail-spread",
                "ts": now_ms - 5 * 60 * 1000,
                "best_bid": 0.50,
                "best_ask": 0.55,
                "spread": 0.05,  # > 0.02 → filtered
                "mid_price": 0.525,
                "depth_yes_usd": 10000.0,
                "depth_no_usd": 10000.0,
            },
            {
                "asset_id": "fail-depth",
                "ts": now_ms - 5 * 60 * 1000,
                "best_bid": 0.50,
                "best_ask": 0.51,
                "spread": 0.01,
                "mid_price": 0.505,
                "depth_yes_usd": 100.0,  # < 500 → filtered
                "depth_no_usd": 100.0,
            },
            {
                "asset_id": "fail-stale",
                "ts": now_ms - 120 * 60 * 1000,  # > 1h ago → filtered
                "best_bid": 0.50,
                "best_ask": 0.51,
                "spread": 0.01,
                "mid_price": 0.505,
                "depth_yes_usd": 10000.0,
                "depth_no_usd": 10000.0,
            },
        ]
    )

    token_map_rows = [{"yes_token_id": f"yes_m{i}", "no_token_id": f"no_m{i}"} for i in range(5)]
    capture_updates: list[dict] = []
    client = _make_supabase_client_mock(tob_rows, token_map_rows, capture_updates)

    mock_consumer = _truthful_consumer()

    with patch("polyarb.observation.l3_promote.create_client", return_value=client):
        before = time.time()
        await _promote_mutating(
            l3_promote,
            settings=settings,
            ws_consumer=mock_consumer,
            recipe_yaml_path=RECIPE_PATH,
        )
        after = time.time()

    # The exact 10-token target is evidenced before one atomic commit.
    mock_consumer.prepare_l3_target.assert_awaited_once()
    added_tokens = mock_consumer.prepare_l3_target.await_args.args[0]
    assert len(added_tokens) == 10, f"expected 10 tokens, got {len(added_tokens)}: {added_tokens}"
    expected = {f"yes_m{i}" for i in range(5)} | {f"no_m{i}" for i in range(5)}
    assert set(added_tokens) == expected
    prepared = mock_consumer.commit_l3_target.await_args.args[0]
    assert prepared.asset_ids == added_tokens
    mock_consumer.add_subscriptions.assert_not_awaited()
    mock_consumer.remove_subscriptions.assert_not_awaited()

    # State mutated.
    assert len(l3_promote._l3_active_set) == 10
    assert set(l3_promote._l3_active_set) == expected
    assert l3_promote._last_promote_at_s is not None
    assert before <= l3_promote._last_promote_at_s <= after

    # l2_candidates write-through (Blocker #1) — one update payload with
    # `l3_promoted_at_ts: <iso>` and 5 market asset_ids.
    add_updates = [u for u in capture_updates if u["payload"].get("l3_promoted_at_ts") is not None]
    assert len(add_updates) == 1, (
        f"expected 1 add-update, got {len(add_updates)}: {capture_updates}"
    )
    assert sorted(add_updates[0]["ids"]) == sorted(f"yes_m{i}" for i in range(5))
    assert add_updates[0]["col"] == "asset_id"
    # iso string check
    iso_val = add_updates[0]["payload"]["l3_promoted_at_ts"]
    assert isinstance(iso_val, str) and "T" in iso_val and iso_val.endswith("Z")


@pytest.mark.asyncio
async def test_promote_run_filters_no_side_tob_before_recipe_limit() -> None:
    """A promoted No token must not consume one of five Yes-market slots."""
    from polyarb.observation import l3_promote

    now_ms = int(time.time() * 1000)
    tob_rows = [
        {
            "asset_id": f"yes_{i}",
            "ts": now_ms - 60_000,
            "best_bid": 0.50,
            "best_ask": 0.51,
            "spread": 0.01,
            "mid_price": 0.505,
            "depth_yes_usd": 5_000.0 - i,
            "depth_no_usd": 5_000.0,
        }
        for i in range(5)
    ]
    tob_rows.append(
        {
            "asset_id": "no_0",
            "ts": now_ms,
            "best_bid": 0.49,
            "best_ask": 0.50,
            "spread": 0.01,
            "mid_price": 0.495,
            "depth_yes_usd": 10_000.0,
            "depth_no_usd": 10_000.0,
        }
    )
    token_rows = [{"yes_token_id": f"yes_{i}", "no_token_id": f"no_{i}"} for i in range(5)]
    consumer = _truthful_consumer()

    with patch(
        "polyarb.observation.l3_promote.create_client",
        return_value=_make_supabase_client_mock(tob_rows, token_rows),
    ):
        settings = _make_settings()
        result = await _promote_mutating(
            l3_promote,
            settings=settings,
            ws_consumer=consumer,
            recipe_yaml_path=RECIPE_PATH,
        )

    assert len(result["active"]) == 10
    assert set(result["active"]) == {token for i in range(5) for token in (f"yes_{i}", f"no_{i}")}


@pytest.mark.asyncio
async def test_promote_run_rejects_incomplete_or_duplicate_token_pairs() -> None:
    from polyarb.observation import l3_promote

    now_ms = int(time.time() * 1000)
    tob_rows = [
        {
            "asset_id": f"yes_{i}",
            "ts": now_ms - 60_000,
            "best_bid": 0.50,
            "best_ask": 0.51,
            "spread": 0.01,
            "mid_price": 0.505,
            "depth_yes_usd": 5_000.0 - i,
            "depth_no_usd": 5_000.0,
        }
        for i in range(5)
    ]
    token_rows = [
        {"yes_token_id": "yes_0", "no_token_id": "no_0"},
        {"yes_token_id": "yes_1", "no_token_id": "no_0"},
        {"yes_token_id": "yes_2", "no_token_id": None},
        {"yes_token_id": "yes_3", "no_token_id": "yes_3"},
        # yes_4 is intentionally missing.
    ]
    consumer = _truthful_consumer()

    with patch(
        "polyarb.observation.l3_promote.create_client",
        return_value=_make_supabase_client_mock(tob_rows, token_rows),
    ):
        settings = _make_settings()
        await _promote_mutating(
            l3_promote,
            settings=settings,
            ws_consumer=consumer,
            recipe_yaml_path=RECIPE_PATH,
        )

    assert l3_promote._l3_active_set == set(), "underfilled proposal must not mutate membership"
    assert "yes_1" not in l3_promote._l3_active_set
    assert "yes_2" not in l3_promote._l3_active_set
    assert "yes_3" not in l3_promote._l3_active_set
    assert "yes_4" not in l3_promote._l3_active_set


@pytest.mark.asyncio
async def test_promote_run_dry_run_has_zero_mutations() -> None:
    from polyarb.observation import l3_promote

    now_ms = int(time.time() * 1000)
    tob_rows = [
        {
            "asset_id": f"yes_dry_{i}",
            "ts": now_ms - 60_000,
            "best_bid": 0.50,
            "best_ask": 0.51,
            "spread": 0.01,
            "mid_price": 0.505,
            "depth_yes_usd": 5_000.0 - i,
            "depth_no_usd": 5_000.0,
        }
        for i in range(5)
    ]
    token_rows = [{"yes_token_id": f"yes_dry_{i}", "no_token_id": f"no_dry_{i}"} for i in range(5)]
    capture_updates: list[dict] = []
    consumer = _truthful_consumer(initial_committed={"old-active"})

    before_active = {"old-active"}
    before_tob = [{"sentinel": "tob"}]
    before_map = {"old-yes": ("old-yes", "old-no")}
    before_promote = 123.0
    before_mirrored = frozenset({"old-yes"})
    l3_promote._l3_active_set = before_active
    l3_promote._last_known_tob_rows = before_tob
    l3_promote._last_known_market_token_map = before_map
    l3_promote._last_promote_at_s = before_promote
    l3_promote._last_mirrored_market_ids = before_mirrored

    trap_store = _RecordingEvidenceStore()

    with patch(
        "polyarb.observation.l3_promote.create_client",
        return_value=_make_supabase_client_mock(tob_rows, token_rows, capture_updates),
    ):
        result = await l3_promote.promote_run(
            settings=_make_settings(),
            ws_consumer=consumer,
            recipe_yaml_path=RECIPE_PATH,
            evidence_store=trap_store,
            evidence_runtime=_make_runtime(_make_settings()),
            apply_mutations=False,
        )

    consumer.add_subscriptions.assert_not_awaited()
    consumer.remove_subscriptions.assert_not_awaited()
    consumer.set_l3_desired.assert_not_called()
    assert capture_updates == []
    assert l3_promote._l3_active_set is before_active
    assert l3_promote._last_known_tob_rows is before_tob
    assert l3_promote._last_known_market_token_map is before_map
    assert l3_promote._last_mirrored_market_ids is before_mirrored
    assert l3_promote._last_promote_at_s == before_promote
    assert trap_store.records == []
    assert isinstance(result, Mapping)
    assert len(result) == len(tuple(result))
    assert tuple(result) == tuple(dict(result))
    assert result["dry_run"] is True
    assert result["persisted"] is False
    assert result.persisted is False
    assert len(result["proposed_active"]) == 10
    assert result["active"] == result["proposed_active"], (
        "legacy dry-run active must remain the proposed desired membership"
    )


# ────────────────────────────────────────────────────────────────────────
# Test 7 — diff calls add AND remove with Yes/No expansion
# ────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_promote_run_diff_calls_add_AND_remove_with_yes_no_expansion() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings()
    now_ms = int(time.time() * 1000)

    # Seed: 2 markets (old1 stays, old2 removed). Pre-populate the reverse map
    # so the promoter knows the old token→market mapping for the diff.
    l3_promote._l3_active_set = {"yes_old1", "no_old1", "yes_old2", "no_old2"}
    l3_promote._last_known_market_token_map = {
        "yes_old1": ("yes_old1", "no_old1"),
        "yes_old2": ("yes_old2", "no_old2"),
    }

    # New top-5: old1 + 4 new markets.
    tob_rows = [
        {
            "asset_id": aid,
            "ts": now_ms - 5 * 60 * 1000,
            "best_bid": 0.50,
            "best_ask": 0.51,
            "spread": 0.01,
            "mid_price": 0.505,
            "depth_yes_usd": d,
            "depth_no_usd": 5000.0,
        }
        for aid, d in [
            ("yes_old1", 5000),
            ("yes_new1", 4900),
            ("yes_new2", 4800),
            ("yes_new3", 4700),
            ("yes_new4", 4600),
        ]
    ]
    token_map_rows = [
        {"yes_token_id": "yes_old1", "no_token_id": "no_old1"},
        {"yes_token_id": "yes_new1", "no_token_id": "no_new1"},
        {"yes_token_id": "yes_new2", "no_token_id": "no_new2"},
        {"yes_token_id": "yes_new3", "no_token_id": "no_new3"},
        {"yes_token_id": "yes_new4", "no_token_id": "no_new4"},
    ]
    capture_updates: list[dict] = []
    client = _make_supabase_client_mock(tob_rows, token_map_rows, capture_updates)

    mock_consumer = _truthful_consumer(
        initial_committed={"yes_old1", "no_old1", "yes_old2", "no_old2"}
    )

    with patch("polyarb.observation.l3_promote.create_client", return_value=client):
        await _promote_mutating(
            l3_promote,
            settings=settings,
            ws_consumer=mock_consumer,
            recipe_yaml_path=RECIPE_PATH,
        )

    prepared = mock_consumer.commit_l3_target.await_args.args[0]
    target = prepared.asset_ids
    # 4 new markets × 2 tokens = 8 added.
    added = target - {"yes_old1", "no_old1", "yes_old2", "no_old2"}
    expected_added = {f"yes_new{i}" for i in range(1, 5)} | {f"no_new{i}" for i in range(1, 5)}
    assert set(added) == expected_added

    # 1 removed market × 2 tokens = 2 removed.
    removed = {"yes_old1", "no_old1", "yes_old2", "no_old2"} - target
    assert set(removed) == {"yes_old2", "no_old2"}
    mock_consumer.add_subscriptions.assert_not_awaited()
    mock_consumer.remove_subscriptions.assert_not_awaited()


# ────────────────────────────────────────────────────────────────────────
# Test 8 — Blocker #1: write-through l3_promoted_at_ts on add + None on remove
# ────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_promote_run_writes_l3_promoted_at_ts_on_add_and_clears_on_remove() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings()
    now_ms = int(time.time() * 1000)

    # Seed: market m1 active.
    l3_promote._l3_active_set = {"yes_m1", "no_m1"}
    l3_promote._last_known_market_token_map = {"yes_m1": ("yes_m1", "no_m1")}

    # New top: five complete markets. m1 is removed.
    tob_rows = [
        {
            "asset_id": f"yes_m{i}",
            "ts": now_ms - 5 * 60 * 1000,
            "best_bid": 0.50,
            "best_ask": 0.51,
            "spread": 0.01,
            "mid_price": 0.505,
            "depth_yes_usd": 1000.0 - i,
            "depth_no_usd": 1000.0,
        }
        for i in range(2, 7)
    ]
    token_map_rows = [{"yes_token_id": f"yes_m{i}", "no_token_id": f"no_m{i}"} for i in range(2, 7)]
    capture_updates: list[dict] = []
    capture_queries: list[dict] = []
    promoted_rows = ["yes_m1"]
    client = _make_bounded_reconciliation_client(
        tob_rows,
        token_map_rows,
        promoted_rows,
        capture_updates,
        capture_queries,
    )

    mock_consumer = _truthful_consumer(initial_committed={"yes_m1", "no_m1"})

    with patch("polyarb.observation.l3_promote.create_client", return_value=client):
        await _promote_mutating(
            l3_promote,
            settings=settings,
            ws_consumer=mock_consumer,
            recipe_yaml_path=RECIPE_PATH,
        )

    # Find the add update and the remove update.
    add_ups = [u for u in capture_updates if u["payload"].get("l3_promoted_at_ts") is not None]
    rm_ups = [u for u in capture_updates if u["payload"].get("l3_promoted_at_ts") is None]
    assert len(add_ups) == 1
    assert add_ups[0]["ids"] == [f"yes_m{i}" for i in range(2, 7)]
    assert len(rm_ups) == 1
    assert rm_ups[0]["ids"] == ["yes_m1"]
    assert capture_queries[0]["returned"] == ["yes_m1"]

    # Lint: helper contains `category="l2-mirror"` (chain-truth breadcrumb).
    src = inspect.getsource(l3_promote._mirror_l3_promoted_at_ts)
    assert 'category="l2-mirror"' in src or "category='l2-mirror'" in src


# ────────────────────────────────────────────────────────────────────────
# Test 9 — write-through failure does not abort promote_run
# ────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_promote_run_write_through_failure_does_not_abort_promote_run() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings()
    now_ms = int(time.time() * 1000)

    l3_promote._l3_active_set = {"yes_m1", "no_m1"}
    l3_promote._last_known_market_token_map = {"yes_m1": ("yes_m1", "no_m1")}

    tob_rows = [
        {
            "asset_id": f"yes_m{i}",
            "ts": now_ms - 5 * 60 * 1000,
            "best_bid": 0.50,
            "best_ask": 0.51,
            "spread": 0.01,
            "mid_price": 0.505,
            "depth_yes_usd": 1000.0 - i,
            "depth_no_usd": 1000.0,
        }
        for i in range(2, 7)
    ]
    token_map_rows = [{"yes_token_id": f"yes_m{i}", "no_token_id": f"no_m{i}"} for i in range(2, 7)]

    # Build a client where l2_candidates update.in_().execute() raises.
    def _table_side_effect(name: str) -> MagicMock:
        tbl = MagicMock()
        if name == "l2_top_of_book":
            tbl.select.return_value = tbl
            tbl.gte.return_value = tbl
            tbl.order.return_value = tbl
            tbl.limit.return_value = tbl
            tbl.execute.return_value = MagicMock(data=list(tob_rows))
        elif name == "markets_latest":
            tbl.select.return_value = tbl
            tbl.in_.return_value = tbl
            tbl.execute.return_value = MagicMock(data=_with_market_ids(token_map_rows))
        elif name == "l2_candidates":

            class _UpdateChainFails:
                def __init__(self, _payload: dict) -> None:
                    pass

                def in_(self, _col: str, _ids: list[str]) -> _UpdateChainFails:
                    return self

                def execute(self) -> Any:
                    raise RuntimeError("PostgREST 503")

            tbl.update.side_effect = lambda payload: _UpdateChainFails(payload)
        return tbl

    client = MagicMock()
    client.table.side_effect = _table_side_effect

    mock_consumer = _truthful_consumer(initial_committed={"yes_m1", "no_m1"})

    sentry_calls: list[dict] = []
    log_messages: list[str] = []

    def _capture_breadcrumb(**kw: Any) -> None:
        sentry_calls.append(kw)

    sink_id = l3_promote.logger.add(
        lambda message: log_messages.append(str(message)), level="WARNING"
    )
    try:
        with (
            patch("polyarb.observation.l3_promote.create_client", return_value=client),
            patch(
                "polyarb.observation.l3_promote.sentry_sdk.add_breadcrumb",
                side_effect=_capture_breadcrumb,
            ),
        ):
            # MUST not raise.
            await _promote_mutating(
                l3_promote,
                settings=settings,
                ws_consumer=mock_consumer,
                recipe_yaml_path=RECIPE_PATH,
            )
    finally:
        l3_promote.logger.remove(sink_id)

    # A failed mirror now blocks the atomic commit and retains last-known-good.
    assert l3_promote._l3_active_set == {"yes_m1", "no_m1"}
    mock_consumer.commit_l3_target.assert_not_awaited()

    # At least one warning-level breadcrumb with category=l2-mirror.
    mirror_warn = [
        b for b in sentry_calls if b.get("category") == "l2-mirror" and b.get("level") == "warning"
    ]
    assert len(mirror_warn) >= 1, f"expected mirror-warning breadcrumb, got: {sentry_calls}"
    warning = next(message for message in log_messages if "mirror failed" in message)
    assert "reason=write_through_failed" in warning
    assert "error_type=RuntimeError" in warning
    assert "committed_count=5" in warning
    assert "PostgREST 503" not in warning
    breadcrumb_data = mirror_warn[-1]["data"]
    assert breadcrumb_data["reason_code"] == "write_through_failed"
    assert breadcrumb_data["error_type"] == "RuntimeError"
    assert breadcrumb_data["committed_count"] == 5
    assert "error" not in breadcrumb_data


# ────────────────────────────────────────────────────────────────────────
# Test 10 — fail-soft on scanner exception
# ────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_promote_run_fail_soft_on_scanner_exception() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings()
    l3_promote._l3_active_set = {"a"}

    client = _make_supabase_client_mock([], [])

    mock_consumer = _truthful_consumer(initial_committed={"a"})

    with (
        patch("polyarb.observation.l3_promote.create_client", return_value=client),
        patch(
            "polyarb.observation.scanner.run_recipe",
            side_effect=ValueError("recipe invalid"),
        ),
    ):
        # No raise.
        await _promote_mutating(
            l3_promote,
            settings=settings,
            ws_consumer=mock_consumer,
            recipe_yaml_path=RECIPE_PATH,
        )

    # _l3_active_set frozen.
    assert l3_promote._l3_active_set == {"a"}


def _five_market_inputs(prefix: str = "terminal") -> tuple[list[dict], list[dict]]:
    now_ms = int(time.time() * 1000)
    tob_rows = [
        {
            "asset_id": f"yes_{prefix}_{i}",
            "ts": now_ms - 60_000,
            "best_bid": 0.50,
            "best_ask": 0.51,
            "spread": 0.01,
            "mid_price": 0.505,
            "depth_yes_usd": 5_000.0 - i,
            "depth_no_usd": 5_000.0,
        }
        for i in range(5)
    ]
    token_rows = [
        {
            "yes_token_id": f"yes_{prefix}_{i}",
            "no_token_id": f"no_{prefix}_{i}",
        }
        for i in range(5)
    ]
    return tob_rows, token_rows


@pytest.mark.asyncio
async def test_rotation_never_reports_success_or_publishes_partial_evidence() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings()
    runtime = _make_runtime(settings)
    store = _RecordingEvidenceStore()
    old_tokens = {
        token
        for index in range(5)
        for token in (f"yes_old_{index}", f"no_old_{index}")
    }
    consumer = _truthful_consumer(initial_committed=old_tokens)
    now = datetime.now(UTC)
    consumer._test_state["evidenced"] = set(old_tokens)
    consumer._test_state["evidenced_at"] = {token: now for token in old_tokens}
    before = consumer.l3_membership_snapshot()
    consumer.prepare_l3_target = AsyncMock(return_value=None)
    tob_rows, token_rows = _five_market_inputs("rotation")

    with patch(
        "polyarb.observation.l3_promote.create_client",
        return_value=_make_supabase_client_mock(tob_rows, token_rows),
    ):
        result = await l3_promote.promote_run(
            settings=settings,
            ws_consumer=consumer,
            recipe_yaml_path=RECIPE_PATH,
            evidence_store=store,
            evidence_runtime=runtime,
        )

    assert (result.status.value, result.reason_code) == (
        "failed",
        "target_evidence_failed",
    )
    assert consumer.l3_membership_snapshot() == before
    consumer.commit_l3_target.assert_not_awaited()
    assert store.records[-1].evidenced_count == 10


@pytest.mark.asyncio
async def test_prepared_target_success_is_exact_10_10_10() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings()
    runtime = _make_runtime(settings)
    store = _RecordingEvidenceStore()
    consumer = _truthful_consumer()
    tob_rows, token_rows = _five_market_inputs("prepared")

    with patch(
        "polyarb.observation.l3_promote.create_client",
        return_value=_make_supabase_client_mock(tob_rows, token_rows),
    ):
        result = await l3_promote.promote_run(
            settings=settings,
            ws_consumer=consumer,
            recipe_yaml_path=RECIPE_PATH,
            evidence_store=store,
            evidence_runtime=runtime,
        )

    target = consumer.prepare_l3_target.await_args.args[0]
    prepared = consumer.commit_l3_target.await_args.args[0]
    assert prepared.asset_ids == target
    consumer.commit_l3_target.assert_awaited_once_with(prepared)
    assert (result.status.value, result.reason_code) == ("success", "ok")
    record = store.records[-1]
    assert (
        record.desired_count,
        record.committed_count,
        record.evidenced_count,
    ) == (10, 10, 10)


@pytest.mark.asyncio
async def test_terminal_promote_success_appends_one_truthful_record() -> None:
    from polyarb.observation import l3_promote
    from polyarb.observation.l3_evidence import PromoteStatus, stable_sha256

    settings = _make_settings()
    runtime = _make_runtime(settings)
    store = _RecordingEvidenceStore()
    consumer = _truthful_consumer()
    tob_rows, token_rows = _five_market_inputs()
    scheduled_at = datetime(2026, 7, 23, 1, 0, tzinfo=UTC)

    with patch(
        "polyarb.observation.l3_promote.create_client",
        return_value=_make_supabase_client_mock(tob_rows, token_rows),
    ):
        result = await l3_promote.promote_run(
            settings=settings,
            ws_consumer=consumer,
            recipe_yaml_path=RECIPE_PATH,
            evidence_store=store,
            evidence_runtime=runtime,
            scheduled_at=scheduled_at,
            run_seq=19,
        )

    expected = frozenset(
        token for i in range(5) for token in (f"yes_terminal_{i}", f"no_terminal_{i}")
    )
    assert result.status is PromoteStatus.SUCCESS
    assert result.persisted is True
    assert result["persisted"] is True
    assert result.desired == result.committed == expected
    assert result.run_seq == 19
    assert result.scheduled_at == scheduled_at
    assert len(store.records) == 1
    record = store.records[0]
    assert record.status is PromoteStatus.SUCCESS
    assert record.selected_count == 5
    assert record.desired_count == record.committed_count == 10
    assert record.evidenced_count == 10
    assert record.add_count == 10
    assert record.remove_count == 0
    assert record.add_succeeded is True
    assert record.remove_succeeded is None
    assert record.mirror_succeeded is True
    assert record.desired_hash == stable_sha256(sorted(expected))
    assert record.committed_hash == stable_sha256(sorted(expected))
    assert record.acceptance_config_hash == runtime.snapshot().acceptance_config_hash
    assert runtime.snapshot().last_promote_persisted_at == record.finished_at
    assert l3_promote.get_last_promote_at_s() == record.finished_at.timestamp()


@pytest.mark.asyncio
async def test_promoter_supabase_fetch_does_not_block_event_loop() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings()
    runtime = _make_runtime(settings)
    store = _RecordingEvidenceStore()
    consumer = _truthful_consumer()
    tob_rows, token_rows = _five_market_inputs("offloop")
    client = _make_supabase_client_mock(tob_rows, token_rows)
    started = threading.Event()
    release = threading.Event()

    def _blocking_fetch(_client):
        started.set()
        release.wait(timeout=1)
        return tob_rows

    safety = threading.Timer(0.2, release.set)
    safety.start()
    try:
        with (
            patch.object(l3_promote, "create_client", return_value=client),
            patch.object(
                l3_promote,
                "_fetch_latest_tob_rows_from_supabase",
                side_effect=_blocking_fetch,
            ),
        ):
            task = asyncio.create_task(
                l3_promote.promote_run(
                    settings=settings,
                    ws_consumer=consumer,
                    recipe_yaml_path=RECIPE_PATH,
                    evidence_store=store,
                    evidence_runtime=runtime,
                    run_seq=20,
                )
            )
            await asyncio.wait_for(asyncio.to_thread(started.wait, 0.3), timeout=0.4)
            await asyncio.wait_for(asyncio.sleep(0.01), timeout=0.05)
            assert task.done() is False
            release.set()
            result = await asyncio.wait_for(task, timeout=0.5)
    finally:
        release.set()
        safety.cancel()

    assert result.status.value == "success"


@pytest.mark.asyncio
async def test_promoter_retries_transient_cold_start_tob_fetch() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings()
    runtime = _make_runtime(settings)
    store = _RecordingEvidenceStore()
    consumer = _truthful_consumer()
    tob_rows, token_rows = _five_market_inputs("cold-retry")
    client = _make_supabase_client_mock(tob_rows, token_rows)

    with (
        patch.object(l3_promote, "create_client", return_value=client),
        patch.object(
            l3_promote,
            "_fetch_latest_tob_rows_from_supabase",
            side_effect=[TimeoutError, tob_rows],
        ) as fetch,
        patch.object(l3_promote.asyncio, "sleep", new=AsyncMock()) as sleep,
    ):
        result = await l3_promote.promote_run(
            settings=settings,
            ws_consumer=consumer,
            recipe_yaml_path=RECIPE_PATH,
            evidence_store=store,
            evidence_runtime=runtime,
            run_seq=21,
        )

    assert result.status.value == "success"
    assert fetch.call_count == 2
    sleep.assert_awaited_once_with(1.0)


@pytest.mark.asyncio
async def test_promoter_recovers_with_candidate_scoped_tob_read() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings()
    runtime = _make_runtime(settings)
    store = _RecordingEvidenceStore()
    consumer = _truthful_consumer()
    tob_rows, token_rows = _five_market_inputs("scoped-retry")
    consumer.candidate_assets_snapshot = MagicMock(
        return_value=frozenset(row["asset_id"] for row in tob_rows)
    )
    client = _make_supabase_client_mock(tob_rows, token_rows)

    def _fetch(_client, asset_ids=None):
        if asset_ids is None:
            raise TimeoutError
        return tob_rows

    with (
        patch.object(l3_promote, "create_client", return_value=client),
        patch.object(
            l3_promote,
            "_fetch_latest_tob_rows_from_supabase",
            side_effect=_fetch,
        ) as fetch,
        patch.object(l3_promote.asyncio, "sleep", new=AsyncMock()) as sleep,
    ):
        result = await l3_promote.promote_run(
            settings=settings,
            ws_consumer=consumer,
            recipe_yaml_path=RECIPE_PATH,
            evidence_store=store,
            evidence_runtime=runtime,
            run_seq=22,
        )

    assert result.status.value == "success"
    assert fetch.call_count == 4
    assert fetch.call_args.args[1] == sorted(consumer.candidate_assets_snapshot())
    assert sleep.await_count == 2


@pytest.mark.asyncio
async def test_promoter_recovers_with_runtime_database_inputs() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings()
    runtime = _make_runtime(settings)
    store = _RecordingEvidenceStore()
    consumer = _truthful_consumer()
    tob_rows, token_rows = _five_market_inputs("direct-retry")
    token_map = {
        row["yes_token_id"]: (
            f"market-direct-retry-{index}",
            row["yes_token_id"],
            row["no_token_id"],
        )
        for index, row in enumerate(token_rows)
    }
    store.fetch_promoter_inputs = AsyncMock(return_value=(tob_rows, token_map))
    consumer.candidate_assets_snapshot = MagicMock(
        return_value=frozenset(row["asset_id"] for row in tob_rows)
    )
    client = _make_supabase_client_mock(tob_rows, token_rows)

    with (
        patch.object(l3_promote, "create_client", return_value=client),
        patch.object(
            l3_promote,
            "_fetch_latest_tob_rows_from_supabase",
            side_effect=TimeoutError,
        ) as fetch,
        patch.object(l3_promote.asyncio, "sleep", new=AsyncMock()),
    ):
        result = await l3_promote.promote_run(
            settings=settings,
            ws_consumer=consumer,
            recipe_yaml_path=RECIPE_PATH,
            evidence_store=store,
            evidence_runtime=runtime,
            run_seq=23,
        )

    assert result.status.value == "success"
    assert fetch.call_count == 0
    store.fetch_promoter_inputs.assert_awaited_once()


@pytest.mark.asyncio
async def test_empty_runtime_inputs_do_not_trigger_global_rest_sort() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings()
    runtime = _make_runtime(settings)
    store = _RecordingEvidenceStore()
    store.fetch_promoter_inputs = AsyncMock(return_value=([], {}))
    consumer = _truthful_consumer()
    consumer.candidate_assets_snapshot = MagicMock(
        return_value=frozenset({"bootstrap-only"})
    )

    with (
        patch.object(l3_promote, "create_client", return_value=MagicMock()),
        patch.object(
            l3_promote,
            "_fetch_latest_tob_rows_from_supabase",
        ) as rest_fetch,
    ):
        result = await l3_promote.promote_run(
            settings=settings,
            ws_consumer=consumer,
            recipe_yaml_path=RECIPE_PATH,
            evidence_store=store,
            evidence_runtime=runtime,
            run_seq=24,
        )

    assert result.reason_code in {"empty_token_map", "underfilled"}
    rest_fetch.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("case", "expected_status", "expected_reason"),
    [
        ("frozen", "frozen", "no_supabase_creds"),
        ("underfilled", "underfilled", "underfilled"),
        ("selection_exception", "failed", "selection_failed"),
        ("add_false", "failed", "target_commit_failed"),
        ("remove_false", "failed", "target_commit_failed"),
        ("mirror_false", "failed", "mirror_failed"),
    ],
)
async def test_terminal_promote_outcomes_append_exactly_once(
    case: str,
    expected_status: str,
    expected_reason: str,
) -> None:
    from polyarb.observation import l3_promote
    from polyarb.observation.l3_evidence import stable_sha256

    settings = _make_settings(
        supabase_url="" if case == "frozen" else "https://x.supabase.co",
        service_key="" if case == "frozen" else "test-service-key",
    )
    runtime = _make_runtime(settings)
    store = _RecordingEvidenceStore()
    old_tokens = {"yes_old", "no_old"} if case == "remove_false" else set()
    consumer = _truthful_consumer(
        initial_committed=old_tokens,
        add_succeeds=case != "add_false",
        remove_succeeds=case != "remove_false",
    )
    l3_promote._l3_active_set = set(old_tokens)
    l3_promote._last_known_market_token_map = (
        {"yes_old": ("yes_old", "no_old")} if old_tokens else None
    )
    tob_rows, token_rows = _five_market_inputs(case)
    if case == "underfilled":
        tob_rows = tob_rows[:4]
        token_rows = token_rows[:4]

    patches = [
        patch(
            "polyarb.observation.l3_promote.create_client",
            return_value=_make_supabase_client_mock(tob_rows, token_rows),
        )
    ]
    if case == "selection_exception":
        patches.append(
            patch(
                "polyarb.observation.scanner.run_recipe",
                side_effect=RuntimeError("selection exploded"),
            )
        )
    if case == "mirror_false":
        patches.append(
            patch(
                "polyarb.observation.l3_promote._mirror_l3_promoted_at_ts",
                return_value=False,
            )
        )

    with patches[0]:
        if len(patches) == 2:
            with patches[1]:
                result = await l3_promote.promote_run(
                    settings=settings,
                    ws_consumer=consumer,
                    recipe_yaml_path=RECIPE_PATH,
                    evidence_store=store,
                    evidence_runtime=runtime,
                    run_seq=23,
                )
        else:
            result = await l3_promote.promote_run(
                settings=settings,
                ws_consumer=consumer,
                recipe_yaml_path=RECIPE_PATH,
                evidence_store=store,
                evidence_runtime=runtime,
                run_seq=23,
            )

    assert result.status.value == expected_status
    assert result.reason_code == expected_reason
    assert result.persisted is True
    assert len(store.records) == 1
    record = store.records[0]
    assert record.run_seq == 23
    assert record.status.value == expected_status
    assert record.desired_count == len(result.desired)
    assert record.committed_count == len(result.committed)
    assert record.evidenced_count == len(result.evidenced) == 0
    assert record.desired_hash == stable_sha256(sorted(result.desired))
    assert record.committed_hash == stable_sha256(sorted(result.committed))
    assert record.acceptance_config_hash == runtime.snapshot().acceptance_config_hash
    expected_counts = {
        "frozen": (0, 0, 0, 0),
        "underfilled": (8, 0, 0, 0),
        "selection_exception": (0, 0, 0, 0),
        "add_false": (0, 0, 10, 0),
        "remove_false": (2, 2, 10, 2),
        "mirror_false": (0, 0, 10, 0),
    }
    assert (
        record.desired_count,
        record.committed_count,
        record.add_count,
        record.remove_count,
    ) == expected_counts[case]
    expected_control = {
        "frozen": (None, None, False),
        "underfilled": (None, None, False),
        "selection_exception": (None, None, False),
        "add_false": (False, None, True),
        "remove_false": (False, False, True),
        "mirror_false": (None, None, False),
    }
    assert (
        record.add_succeeded,
        record.remove_succeeded,
        record.mirror_succeeded,
    ) == expected_control[case]
    if case in {"add_false", "remove_false", "mirror_false"}:
        assert result.desired == result.committed


@pytest.mark.asyncio
async def test_terminal_promote_writer_false_leaves_persisted_anchors_stale() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings()
    runtime = _make_runtime(settings)
    store = _RecordingEvidenceStore(succeeds=False)
    consumer = _truthful_consumer()
    tob_rows, token_rows = _five_market_inputs("writer")
    l3_promote._last_promote_at_s = 123.0
    before_active = {"persisted-active"}
    before_tob = [{"sentinel": "persisted-tob"}]
    before_map = {"persisted-yes": ("persisted-yes", "persisted-no")}
    before_mirrored = frozenset({"persisted-yes"})
    l3_promote._l3_active_set = before_active
    l3_promote._last_known_tob_rows = before_tob
    l3_promote._last_known_market_token_map = before_map
    l3_promote._last_mirrored_market_ids = before_mirrored

    with patch(
        "polyarb.observation.l3_promote.create_client",
        return_value=_make_supabase_client_mock(tob_rows, token_rows),
    ):
        result = await l3_promote.promote_run(
            settings=settings,
            ws_consumer=consumer,
            recipe_yaml_path=RECIPE_PATH,
            evidence_store=store,
            evidence_runtime=runtime,
            run_seq=29,
        )

    assert result.status.value == "success"
    assert result.persisted is False
    assert len(store.records) == 1, "a failed writer is not retried as a duplicate"
    assert runtime.snapshot().writer_ok is False
    assert runtime.snapshot().last_promote_persisted_at is None
    assert l3_promote.get_last_promote_at_s() == 123.0
    assert l3_promote._l3_active_set is before_active
    assert l3_promote._last_known_tob_rows is before_tob
    assert l3_promote._last_known_market_token_map is before_map
    assert l3_promote._last_mirrored_market_ids is before_mirrored
    consumer.set_l3_desired.assert_called_once_with(frozenset())
    consumer.compensate_current_generation.assert_awaited_once_with(
        reason_code="promote_append_failed"
    )


@pytest.mark.asyncio
async def test_acceptance_config_construction_failure_terminalizes_before_effects() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings()
    runtime = _make_runtime(settings)
    store = _RecordingEvidenceStore()
    consumer = _truthful_consumer(initial_committed={"old-yes", "old-no"})

    with (
        patch.object(
            l3_promote.AcceptanceConfig,
            "from_settings",
            side_effect=ValueError("malformed acceptance input"),
        ),
        patch.object(l3_promote, "create_client") as create,
        patch.object(l3_promote, "_mirror_l3_promoted_at_ts") as mirror,
    ):
        result = await l3_promote.promote_run(
            settings=settings,
            ws_consumer=consumer,
            recipe_yaml_path=RECIPE_PATH,
            evidence_store=store,
            evidence_runtime=runtime,
            run_seq=31,
        )

    assert result.status.value == "failed"
    assert result.reason_code == "acceptance_config_invalid"
    assert result.persisted is True
    assert len(store.records) == 1
    assert store.records[0].acceptance_config_hash == runtime.snapshot().acceptance_config_hash
    create.assert_not_called()
    mirror.assert_not_called()
    consumer.set_l3_desired.assert_not_called()
    consumer.add_subscriptions.assert_not_awaited()
    consumer.remove_subscriptions.assert_not_awaited()


@pytest.mark.asyncio
async def test_post_selection_malformed_frame_terminalizes_once_without_mutation() -> None:
    import pandas as pd

    from polyarb.observation import l3_promote

    settings = _make_settings()
    runtime = _make_runtime(settings)
    store = _RecordingEvidenceStore()
    consumer = _truthful_consumer(initial_committed={"old-yes", "old-no"})
    tob_rows, token_rows = _five_market_inputs("malformed")
    before_active = {"old-yes", "old-no"}
    before_tob = [{"sentinel": "old"}]
    before_map = {"old-yes": ("old-yes", "old-no")}
    l3_promote._l3_active_set = before_active
    l3_promote._last_known_tob_rows = before_tob
    l3_promote._last_known_market_token_map = before_map

    with (
        patch.object(
            l3_promote,
            "create_client",
            return_value=_make_supabase_client_mock(tob_rows, token_rows),
        ),
        patch(
            "polyarb.observation.scanner.run_recipe",
            return_value=pd.DataFrame({"wrong_column": ["malformed"]}),
        ),
        patch.object(l3_promote, "_mirror_l3_promoted_at_ts") as mirror,
    ):
        result = await l3_promote.promote_run(
            settings=settings,
            ws_consumer=consumer,
            recipe_yaml_path=RECIPE_PATH,
            evidence_store=store,
            evidence_runtime=runtime,
            run_seq=32,
        )

    assert result.status.value == "failed"
    assert result.reason_code == "selection_failed"
    assert result.persisted is True
    assert len(store.records) == 1
    record = store.records[0]
    assert record.desired_count == record.committed_count == 2
    assert record.add_succeeded is None
    assert record.remove_succeeded is None
    assert record.mirror_succeeded is False
    consumer.set_l3_desired.assert_not_called()
    consumer.add_subscriptions.assert_not_awaited()
    consumer.remove_subscriptions.assert_not_awaited()
    mirror.assert_not_called()
    assert l3_promote._l3_active_set == before_active


@pytest.mark.asyncio
async def test_mirror_false_retries_complete_committed_target_and_pending_cleanup() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings()
    runtime = _make_runtime(settings)
    consumer = _truthful_consumer(initial_committed={"yes_old", "no_old"})
    tob_rows, token_rows = _five_market_inputs("retry")
    old_map = {"yes_old": ("yes_old", "no_old")}
    l3_promote._last_known_market_token_map = old_map
    l3_promote._last_mirrored_market_ids = frozenset({"yes_old"})
    mirror_calls: list[list[str]] = []

    def _mirror(_client: Any, target: list[str]) -> bool:
        mirror_calls.append(list(target))
        if len(mirror_calls) == 2:
            assert "yes_old" in (l3_promote._last_known_market_token_map or {}), (
                "a fresh token-map fetch must not discard pending cleanup identity"
            )
        return len(mirror_calls) > 1

    with (
        patch.object(
            l3_promote,
            "create_client",
            return_value=_make_supabase_client_mock(tob_rows, token_rows),
        ),
        patch.object(l3_promote, "_mirror_l3_promoted_at_ts", side_effect=_mirror),
    ):
        first_store = _RecordingEvidenceStore()
        first = await l3_promote.promote_run(
            settings=settings,
            ws_consumer=consumer,
            recipe_yaml_path=RECIPE_PATH,
            evidence_store=first_store,
            evidence_runtime=runtime,
            run_seq=40,
        )
        second_store = _RecordingEvidenceStore()
        second = await l3_promote.promote_run(
            settings=settings,
            ws_consumer=consumer,
            recipe_yaml_path=RECIPE_PATH,
            evidence_store=second_store,
            evidence_runtime=runtime,
            run_seq=41,
        )

    expected_current = sorted(f"yes_retry_{i}" for i in range(5))
    assert first.reason_code == "mirror_failed"
    assert first.persisted is True
    assert second.status.value == "success"
    assert second.persisted is True
    assert mirror_calls == [
        expected_current,
        expected_current,
    ]
    assert l3_promote._last_mirrored_market_ids == frozenset(expected_current)
    assert "yes_old" not in (l3_promote._last_known_market_token_map or {})
    assert len(first_store.records) == len(second_store.records) == 1


@pytest.mark.asyncio
async def test_remove_false_keeps_committed_mirror_then_recovery_clears_old() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings()
    runtime = _make_runtime(settings)
    consumer = _truthful_consumer(initial_committed={"yes_old", "no_old"})
    commit_attempts = 0
    control_calls: list[str] = []
    normal_commit = consumer.commit_l3_target.side_effect

    async def _commit_then_recover(prepared: PreparedL3Target) -> bool:
        nonlocal commit_attempts
        control_calls.append("commit")
        commit_attempts += 1
        if commit_attempts == 1:
            return False
        return await normal_commit(prepared)

    consumer.commit_l3_target.side_effect = _commit_then_recover
    tob_rows, token_rows = _five_market_inputs("remove-retry")
    l3_promote._last_known_market_token_map = {"yes_old": ("yes_old", "no_old")}
    l3_promote._last_mirrored_market_ids = frozenset({"yes_old"})
    mirror_calls: list[list[str]] = []

    def _mirror(_client: Any, target: list[str]) -> bool:
        mirror_calls.append(list(target))
        return True

    with (
        patch.object(
            l3_promote,
            "create_client",
            return_value=_make_supabase_client_mock(tob_rows, token_rows),
        ),
        patch.object(l3_promote, "_mirror_l3_promoted_at_ts", side_effect=_mirror),
    ):
        first_store = _RecordingEvidenceStore()
        first = await l3_promote.promote_run(
            settings=settings,
            ws_consumer=consumer,
            recipe_yaml_path=RECIPE_PATH,
            evidence_store=first_store,
            evidence_runtime=runtime,
            run_seq=50,
        )
        second_store = _RecordingEvidenceStore()
        second = await l3_promote.promote_run(
            settings=settings,
            ws_consumer=consumer,
            recipe_yaml_path=RECIPE_PATH,
            evidence_store=second_store,
            evidence_runtime=runtime,
            run_seq=51,
        )

    current = sorted(f"yes_remove-retry_{i}" for i in range(5))
    assert first.reason_code == "target_commit_failed"
    assert first_store.records[0].committed_count == 2
    assert first_store.records[0].add_succeeded is False
    assert first_store.records[0].remove_succeeded is False
    assert mirror_calls[:2] == [current, ["yes_old"]]
    assert second.status.value == "success"
    assert second_store.records[0].committed_count == 10
    assert second_store.records[0].add_succeeded is True
    assert second_store.records[0].remove_succeeded is True
    assert mirror_calls[2] == current
    assert control_calls == ["commit", "commit"]


@pytest.mark.asyncio
async def test_repeated_rotations_with_remove_false_never_add_or_grow_state() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings()
    runtime = _make_runtime(settings)
    original_tokens = {
        token for index in range(5) for token in (f"yes_original_{index}", f"no_original_{index}")
    }
    original_map = {
        f"yes_original_{index}": (
            f"yes_original_{index}",
            f"no_original_{index}",
        )
        for index in range(5)
    }
    consumer = _truthful_consumer(
        initial_committed=original_tokens,
        remove_succeeds=False,
    )
    l3_promote._last_known_market_token_map = original_map
    l3_promote._last_mirrored_market_ids = frozenset(original_map)
    l3_promote._l3_active_set = set(original_tokens)

    records: list[Any] = []
    mirror_targets: list[list[str]] = []
    for tick in range(6):
        tob_rows, token_rows = _five_market_inputs(f"rotation_{tick}")
        store = _RecordingEvidenceStore()
        with (
            patch.object(
                l3_promote,
                "create_client",
                return_value=_make_supabase_client_mock(tob_rows, token_rows),
            ),
            patch.object(
                l3_promote,
                "_mirror_l3_promoted_at_ts",
                side_effect=lambda _client, target: mirror_targets.append(list(target)) or True,
            ),
        ):
            result = await l3_promote.promote_run(
                settings=settings,
                ws_consumer=consumer,
                recipe_yaml_path=RECIPE_PATH,
                evidence_store=store,
                evidence_runtime=runtime,
                run_seq=60 + tick,
            )

        assert result.status.value == "failed"
        assert result.reason_code == "target_commit_failed"
        assert result.committed == frozenset(original_tokens)
        assert len(store.records) == 1
        assert store.records[0].status.value == "failed"
        records.extend(store.records)
        assert len(l3_promote._l3_active_set) == 10
        assert len(l3_promote._last_known_market_token_map or {}) <= 10
        assert len(l3_promote._last_mirrored_market_ids) == 5

    consumer.commit_l3_target.assert_awaited()
    consumer.add_subscriptions.assert_not_awaited()
    assert len(records) == 6
    assert mirror_targets[1::2] == [sorted(original_map)] * 6


@pytest.mark.asyncio
async def test_oversized_precontrol_identity_state_is_removed_before_bounded_mirror() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings()
    runtime = _make_runtime(settings)
    oversized_map = {
        f"yes_oversized_{index}": (
            f"yes_oversized_{index}",
            f"no_oversized_{index}",
        )
        for index in range(l3_promote._MAX_TOKEN_MAP_CACHE + 1)
    }
    oversized_tokens = {
        token for pair in oversized_map.values() for token in pair if token is not None
    }
    consumer = _truthful_consumer(initial_committed=oversized_tokens)
    l3_promote._last_known_market_token_map = oversized_map
    tob_rows, token_rows = _five_market_inputs("bounded_recovery")

    with (
        patch.object(
            l3_promote,
            "create_client",
            return_value=_make_supabase_client_mock(tob_rows, token_rows),
        ),
        patch.object(l3_promote, "_mirror_l3_promoted_at_ts") as mirror,
    ):
        first_store = _RecordingEvidenceStore()
        first = await l3_promote.promote_run(
            settings=settings,
            ws_consumer=consumer,
            recipe_yaml_path=RECIPE_PATH,
            evidence_store=first_store,
            evidence_runtime=runtime,
            run_seq=70,
        )

    assert first.status.value == "success"
    assert first.reason_code == "ok"
    assert len(first.committed) == 10, "successful remove-first control is bounded"
    assert len(first_store.records) == 1
    expected_current = sorted(f"yes_bounded_recovery_{index}" for index in range(5))
    mirror.assert_called_once_with(ANY, expected_current)
    assert len(l3_promote._last_known_market_token_map or {}) <= (l3_promote._MAX_TOKEN_MAP_CACHE)
    assert l3_promote._last_mirrored_market_ids == frozenset(expected_current)

    capture_updates: list[dict] = []
    with patch.object(
        l3_promote,
        "create_client",
        return_value=_make_supabase_client_mock(
            tob_rows,
            token_rows,
            capture_updates,
        ),
    ):
        second_store = _RecordingEvidenceStore()
        second = await l3_promote.promote_run(
            settings=settings,
            ws_consumer=consumer,
            recipe_yaml_path=RECIPE_PATH,
            evidence_store=second_store,
            evidence_runtime=runtime,
            run_seq=71,
        )

    assert second.status.value == "success"
    assert len(second_store.records) == 1
    assert len(l3_promote._last_known_market_token_map or {}) <= (l3_promote._MAX_TOKEN_MAP_CACHE)
    assert len(l3_promote._last_mirrored_market_ids) == 5
    assert capture_updates
    assert max(len(update["ids"]) for update in capture_updates) <= (
        l3_promote._MAX_TOKEN_MAP_CACHE
    )


@pytest.mark.asyncio
async def test_oversized_preexisting_state_with_failed_remove_never_grows() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings()
    runtime = _make_runtime(settings)
    oversized_map = {
        f"yes_stuck_{index}": (
            f"yes_stuck_{index}",
            f"no_stuck_{index}",
        )
        for index in range(l3_promote._MAX_TOKEN_MAP_CACHE + 1)
    }
    oversized_tokens = {
        token for pair in oversized_map.values() for token in pair if token is not None
    }
    consumer = _truthful_consumer(
        initial_committed=oversized_tokens,
        remove_succeeds=False,
    )
    l3_promote._last_known_market_token_map = oversized_map
    l3_promote._last_mirrored_market_ids = frozenset(oversized_map)
    before_cache = l3_promote._last_known_market_token_map
    before_mirror = l3_promote._last_mirrored_market_ids
    tob_rows, token_rows = _five_market_inputs("stuck_bounded")

    with (
        patch.object(
            l3_promote,
            "create_client",
            return_value=_make_supabase_client_mock(tob_rows, token_rows),
        ),
        patch.object(l3_promote, "_mirror_l3_promoted_at_ts") as mirror,
    ):
        store = _RecordingEvidenceStore()
        result = await l3_promote.promote_run(
            settings=settings,
            ws_consumer=consumer,
            recipe_yaml_path=RECIPE_PATH,
            evidence_store=store,
            evidence_runtime=runtime,
            run_seq=72,
        )

    assert result.status.value == "failed"
    assert result.reason_code == "target_commit_failed"
    assert result.committed == frozenset(oversized_tokens)
    assert len(store.records) == 1
    assert store.records[0].committed_count == len(oversized_tokens)
    assert store.records[0].add_succeeded is False
    assert store.records[0].remove_succeeded is False
    consumer.add_subscriptions.assert_not_awaited()
    assert mirror.call_count == 2
    assert l3_promote._last_known_market_token_map is before_cache
    assert l3_promote._last_mirrored_market_ids is before_mirror


@pytest.mark.asyncio
async def test_db_driven_badge_cleanup_converges_over_more_than_two_bounded_batches() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings()
    runtime = _make_runtime(settings)
    consumer = _truthful_consumer()
    tob_rows, token_rows = _five_market_inputs("paged_cleanup")
    current = {f"yes_paged_cleanup_{index}" for index in range(5)}
    stale = {
        f"yes_stale_{index:04d}" for index in range(l3_promote._MIRROR_RECONCILE_BATCH_SIZE * 2 + 7)
    }
    promoted_rows = sorted(current | stale)
    updates: list[dict] = []
    queries: list[dict] = []
    client = _make_bounded_reconciliation_client(
        tob_rows,
        token_rows,
        promoted_rows,
        updates,
        queries,
    )
    # Deliberately unrelated memory proves the database, not the cache, owns
    # stale cleanup recovery.
    l3_promote._last_mirrored_market_ids = frozenset(
        f"legacy_only_{index}" for index in range(l3_promote._MIRROR_RECONCILE_BATCH_SIZE * 4)
    )

    results = []
    stores = []
    cache_sizes: list[tuple[int, int]] = []
    with patch.object(l3_promote, "create_client", return_value=client):
        for run_seq in range(80, 83):
            store = _RecordingEvidenceStore()
            stores.append(store)
            results.append(
                await l3_promote.promote_run(
                    settings=settings,
                    ws_consumer=consumer,
                    recipe_yaml_path=RECIPE_PATH,
                    evidence_store=store,
                    evidence_runtime=runtime,
                    run_seq=run_seq,
                )
            )
            cache_sizes.append(
                (
                    len(l3_promote._last_mirrored_market_ids),
                    len(l3_promote._last_known_market_token_map or {}),
                )
            )

    assert [result.reason_code for result in results] == [
        "mirror_cleanup_pending",
        "mirror_cleanup_pending",
        "ok",
    ]
    assert [result.status.value for result in results] == ["failed", "failed", "success"]
    assert all(len(store.records) == 1 for store in stores), "exactly one append per tick"
    assert len(queries) == 3
    assert all(
        query["limit"] == l3_promote._MIRROR_RECONCILE_BATCH_SIZE
        and len(query["returned"]) <= l3_promote._MIRROR_RECONCILE_BATCH_SIZE
        for query in queries
    )

    current_updates = [
        update for update in updates if update["payload"].get("l3_promoted_at_ts") is not None
    ]
    stale_updates = [
        update for update in updates if update["payload"].get("l3_promoted_at_ts") is None
    ]
    assert len(current_updates) == 3
    assert all(set(update["ids"]) == current for update in current_updates)
    assert all(len(update["ids"]) <= l3_promote._MIRROR_RECONCILE_BATCH_SIZE for update in updates)
    assert set().union(*(set(update["ids"]) for update in stale_updates)) == stale
    assert all(current.isdisjoint(update["ids"]) for update in stale_updates)
    assert all(query["excluded"] == current for query in queries)
    assert not (set(promoted_rows) - current)
    assert [mirror_size for mirror_size, _token_map_size in cache_sizes] == [
        l3_promote._MIRROR_RECONCILE_BATCH_SIZE * 4,
        l3_promote._MIRROR_RECONCILE_BATCH_SIZE * 4,
        5,
    ]
    assert cache_sizes[-1][1] <= l3_promote._MAX_TOKEN_MAP_CACHE


@pytest.mark.asyncio
async def test_oversized_legacy_mirror_cache_converges_from_empty_stale_db_page() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings()
    runtime = _make_runtime(settings)
    tob_rows, token_rows = _five_market_inputs("legacy_recovery")
    current_tokens = {
        token
        for index in range(5)
        for token in (
            f"yes_legacy_recovery_{index}",
            f"no_legacy_recovery_{index}",
        )
    }
    current_yes = {f"yes_legacy_recovery_{index}" for index in range(5)}
    consumer = _truthful_consumer(initial_committed=current_tokens)
    oversized_map = {
        f"yes_legacy_{index}": (f"yes_legacy_{index}", f"no_legacy_{index}")
        for index in range(l3_promote._MAX_TOKEN_MAP_CACHE + 17)
    }
    l3_promote._last_known_market_token_map = oversized_map
    l3_promote._last_mirrored_market_ids = frozenset(oversized_map)
    promoted_rows = sorted(current_yes)
    updates: list[dict] = []
    queries: list[dict] = []
    client = _make_bounded_reconciliation_client(
        tob_rows,
        token_rows,
        promoted_rows,
        updates,
        queries,
    )
    store = _RecordingEvidenceStore()

    with patch.object(l3_promote, "create_client", return_value=client):
        result = await l3_promote.promote_run(
            settings=settings,
            ws_consumer=consumer,
            recipe_yaml_path=RECIPE_PATH,
            evidence_store=store,
            evidence_runtime=runtime,
            run_seq=90,
        )

    assert result.status.value == "success"
    assert result.reason_code == "ok"
    assert len(store.records) == 1
    assert queries == [
        {
            "limit": l3_promote._MIRROR_RECONCILE_BATCH_SIZE,
            "excluded": current_yes,
            "returned": [],
        }
    ]
    assert len(updates) == 1
    assert set(updates[0]["ids"]) == current_yes
    assert len(updates[0]["ids"]) <= l3_promote._MIRROR_RECONCILE_BATCH_SIZE
    assert l3_promote._last_mirrored_market_ids == frozenset(current_yes)
    assert len(l3_promote._last_known_market_token_map or {}) <= (l3_promote._MAX_TOKEN_MAP_CACHE)


@pytest.mark.asyncio
async def test_badge_reconciliation_query_failure_is_durable_and_retryable() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings()
    runtime = _make_runtime(settings)
    consumer = _truthful_consumer()
    tob_rows, token_rows = _five_market_inputs("query_retry")
    stale = {"yes_stale_query"}
    promoted_rows = sorted(stale)
    updates: list[dict] = []
    queries: list[dict] = []
    client = _make_bounded_reconciliation_client(
        tob_rows,
        token_rows,
        promoted_rows,
        updates,
        queries,
        fail_query_once=True,
    )

    with patch.object(l3_promote, "create_client", return_value=client):
        first_store = _RecordingEvidenceStore()
        first = await l3_promote.promote_run(
            settings=settings,
            ws_consumer=consumer,
            recipe_yaml_path=RECIPE_PATH,
            evidence_store=first_store,
            evidence_runtime=runtime,
            run_seq=91,
        )
        second_store = _RecordingEvidenceStore()
        second = await l3_promote.promote_run(
            settings=settings,
            ws_consumer=consumer,
            recipe_yaml_path=RECIPE_PATH,
            evidence_store=second_store,
            evidence_runtime=runtime,
            run_seq=92,
        )

    assert (first.status.value, first.reason_code) == ("failed", "mirror_failed")
    assert (second.status.value, second.reason_code) == ("success", "ok")
    assert len(first_store.records) == len(second_store.records) == 1
    assert not (set(promoted_rows) - {f"yes_query_retry_{index}" for index in range(5)})


@pytest.mark.asyncio
async def test_terminal_promote_generation_change_cannot_succeed() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings()
    runtime = _make_runtime(settings)
    store = _RecordingEvidenceStore()
    consumer = _truthful_consumer()
    normal_commit = consumer.commit_l3_target.side_effect

    async def _commit_then_reconnect(prepared: PreparedL3Target) -> bool:
        succeeded = await normal_commit(prepared)
        consumer._test_state["generation"] += 1
        return succeeded

    consumer.commit_l3_target.side_effect = _commit_then_reconnect
    tob_rows, token_rows = _five_market_inputs("generation")
    with patch(
        "polyarb.observation.l3_promote.create_client",
        return_value=_make_supabase_client_mock(tob_rows, token_rows),
    ):
        result = await l3_promote.promote_run(
            settings=settings,
            ws_consumer=consumer,
            recipe_yaml_path=RECIPE_PATH,
            evidence_store=store,
            evidence_runtime=runtime,
        )

    assert result.status.value == "failed"
    assert result.reason_code == "target_convergence_failed"
    assert len(store.records) == 1


@pytest.mark.asyncio
async def test_terminal_promote_acceptance_mismatch_fails_before_remote_effects() -> None:
    from polyarb.observation import l3_promote

    runtime_settings = _make_settings()
    changed_settings = _make_settings()
    changed_settings.l3_promote_interval_s = 301
    runtime = _make_runtime(runtime_settings)
    store = _RecordingEvidenceStore()
    consumer = _truthful_consumer()

    with patch("polyarb.observation.l3_promote.create_client") as create:
        result = await l3_promote.promote_run(
            settings=changed_settings,
            ws_consumer=consumer,
            recipe_yaml_path=RECIPE_PATH,
            evidence_store=store,
            evidence_runtime=runtime,
        )

    assert result.status.value == "failed"
    assert result.reason_code == "acceptance_config_mismatch"
    assert len(store.records) == 1
    assert store.records[0].acceptance_config_hash == runtime.snapshot().acceptance_config_hash
    create.assert_not_called()
    consumer.set_l3_desired.assert_not_called()


@pytest.mark.asyncio
async def test_mutation_mode_requires_runtime_and_store_before_remote_effects() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings()
    consumer = _truthful_consumer()
    with patch("polyarb.observation.l3_promote.create_client") as create:
        with pytest.raises(ValueError, match="evidence_store.*evidence_runtime"):
            await l3_promote.promote_run(
                settings=settings,
                ws_consumer=consumer,
                recipe_yaml_path=RECIPE_PATH,
            )
    create.assert_not_called()
    consumer.set_l3_desired.assert_not_called()


# ────────────────────────────────────────────────────────────────────────
# Test 11 — run_periodic loops until stop_event
# ────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_run_periodic_waits_for_active_connection_before_run_zero() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings()
    runtime = _make_runtime(settings)
    store = _RecordingEvidenceStore()
    stop_event = asyncio.Event()
    consumer = MagicMock()
    consumer.has_active_connection = False
    boot_started_at = runtime.snapshot().started_at
    calls: list[dict[str, Any]] = []

    async def _fake_promote(**kwargs: Any) -> dict:
        calls.append(kwargs)
        stop_event.set()
        return {"added": [], "removed": []}

    with (
        patch.object(l3_promote, "promote_run", side_effect=_fake_promote),
        patch.object(l3_promote, "_utc_now", return_value=boot_started_at),
    ):
        task = asyncio.create_task(
            l3_promote.run_periodic(
                stop_event=stop_event,
                settings=settings,
                ws_consumer=consumer,
                recipe_yaml_path=RECIPE_PATH,
                evidence_store=store,
                evidence_runtime=runtime,
            )
        )
        await asyncio.sleep(0)
        assert calls == []
        consumer.has_active_connection = True
        await asyncio.wait_for(task, timeout=0.2)

    assert len(calls) == 1
    assert calls[0]["run_seq"] == 0
    assert calls[0]["scheduled_at"] == boot_started_at


@pytest.mark.asyncio
async def test_run_periodic_waits_for_real_candidate_set_before_run_zero() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings()
    runtime = _make_runtime(settings)
    store = _RecordingEvidenceStore()
    stop_event = asyncio.Event()
    consumer = MagicMock()
    consumer.has_active_connection = True
    candidates = {"value": frozenset({"bootstrap-a", "bootstrap-b", "bootstrap-c"})}
    consumer.candidate_assets_snapshot.side_effect = lambda: candidates["value"]
    calls: list[dict[str, Any]] = []

    async def _fake_promote(**kwargs: Any) -> dict:
        calls.append(kwargs)
        stop_event.set()
        return {}

    with patch.object(l3_promote, "promote_run", side_effect=_fake_promote):
        task = asyncio.create_task(
            l3_promote.run_periodic(
                stop_event=stop_event,
                settings=settings,
                ws_consumer=consumer,
                recipe_yaml_path=RECIPE_PATH,
                evidence_store=store,
                evidence_runtime=runtime,
            )
        )
        await asyncio.sleep(0.01)
        assert calls == []
        candidates["value"] = frozenset(f"candidate-{index}" for index in range(10))
        await asyncio.wait_for(task, timeout=0.3)

    assert len(calls) == 1
    assert calls[0]["run_seq"] == 0


@pytest.mark.asyncio
async def test_run_periodic_stop_while_waiting_for_connection_emits_no_run() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings()
    runtime = _make_runtime(settings)
    store = _RecordingEvidenceStore()
    stop_event = asyncio.Event()
    consumer = MagicMock()
    consumer.has_active_connection = False

    with patch.object(l3_promote, "promote_run", new_callable=AsyncMock) as promote:
        task = asyncio.create_task(
            l3_promote.run_periodic(
                stop_event=stop_event,
                settings=settings,
                ws_consumer=consumer,
                recipe_yaml_path=RECIPE_PATH,
                evidence_store=store,
                evidence_runtime=runtime,
            )
        )
        await asyncio.sleep(0)
        stop_event.set()
        await asyncio.wait_for(task, timeout=0.2)

    promote.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_periodic_uses_boot_grid_and_contiguous_sequences_when_late() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings()
    settings.l3_promote_interval_s = 300
    runtime = _make_runtime(settings)
    store = _RecordingEvidenceStore()
    acceptance_config = AcceptanceConfig.from_settings(settings, RECIPE_PATH, "test-code")
    stop_event = asyncio.Event()
    mock_consumer = MagicMock()
    calls: list[dict[str, Any]] = []

    async def _fake_promote(**kwargs: Any) -> dict:
        calls.append(kwargs)
        if len(calls) == 3:
            stop_event.set()
        return {"added": [], "removed": []}

    boot_started_at = runtime.snapshot().started_at
    with (
        patch.object(l3_promote, "promote_run", side_effect=_fake_promote),
        patch.object(
            l3_promote,
            "_utc_now",
            return_value=boot_started_at + timedelta(seconds=1_000),
        ),
    ):
        await asyncio.wait_for(
            l3_promote.run_periodic(
                stop_event=stop_event,
                settings=settings,
                ws_consumer=mock_consumer,
                recipe_yaml_path=RECIPE_PATH,
                evidence_store=store,
                evidence_runtime=runtime,
                acceptance_config=acceptance_config,
            ),
            timeout=2.0,
        )

    assert [call["run_seq"] for call in calls] == [0, 1, 2]
    assert [call["scheduled_at"] for call in calls] == [
        boot_started_at,
        boot_started_at + timedelta(seconds=300),
        boot_started_at + timedelta(seconds=600),
    ]
    assert all(call["evidence_store"] is store for call in calls)
    assert all(call["evidence_runtime"] is runtime for call in calls)
    assert all(call["acceptance_config"] is acceptance_config for call in calls)


@pytest.mark.asyncio
async def test_run_periodic_rechecks_wall_clock_after_early_timer_timeout() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings()
    settings.l3_promote_interval_s = 300
    runtime = _make_runtime(settings)
    store = _RecordingEvidenceStore()
    stop_event = asyncio.Event()
    boot_started_at = runtime.snapshot().started_at
    wall_clock = {"now": boot_started_at}
    calls: list[tuple[datetime, datetime]] = []
    wait_count = 0

    async def _fake_promote(**kwargs: Any) -> dict:
        calls.append((kwargs["scheduled_at"], wall_clock["now"]))
        if len(calls) == 1:
            wall_clock["now"] = boot_started_at + timedelta(seconds=299.999)
        else:
            stop_event.set()
        return {"added": [], "removed": []}

    async def _early_timer(awaitable: Any, *, timeout: float) -> None:
        nonlocal wait_count
        del timeout
        awaitable.close()
        wait_count += 1
        if wait_count == 2:
            wall_clock["now"] = boot_started_at + timedelta(seconds=300)
        raise TimeoutError

    with (
        patch.object(l3_promote, "promote_run", side_effect=_fake_promote),
        patch.object(l3_promote, "_utc_now", side_effect=lambda: wall_clock["now"]),
        patch.object(l3_promote.asyncio, "wait_for", side_effect=_early_timer),
    ):
        await l3_promote.run_periodic(
            stop_event=stop_event,
            settings=settings,
            ws_consumer=MagicMock(),
            recipe_yaml_path=RECIPE_PATH,
            evidence_store=store,
            evidence_runtime=runtime,
        )

    assert wait_count == 2
    assert calls == [
        (boot_started_at, boot_started_at),
        (
            boot_started_at + timedelta(seconds=300),
            boot_started_at + timedelta(seconds=300),
        ),
    ]


@pytest.mark.asyncio
async def test_promote_run_uses_supplied_canonical_acceptance_without_rebuilding() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings(supabase_url="", service_key="")
    acceptance_config = AcceptanceConfig.from_settings(settings, RECIPE_PATH, "test-code")
    runtime = _make_runtime(settings)
    store = _RecordingEvidenceStore()

    with patch.object(
        l3_promote.AcceptanceConfig,
        "from_settings",
        side_effect=AssertionError("production tick must not rebuild acceptance config"),
    ):
        await l3_promote.promote_run(
            settings=settings,
            ws_consumer=_truthful_consumer(),
            recipe_yaml_path=RECIPE_PATH,
            evidence_store=store,
            evidence_runtime=runtime,
            acceptance_config=acceptance_config,
            run_seq=0,
        )

    assert len(store.records) == 1
    assert store.records[0].acceptance_config_hash == acceptance_config.digest()


@pytest.mark.asyncio
async def test_unexpected_dependency_exception_before_append_terminalizes_once() -> None:
    from polyarb.observation import l3_promote
    from polyarb.observation.l3_evidence import WsMembershipSnapshot, stable_sha256

    settings = _make_settings()
    runtime = _make_runtime(settings)
    acceptance_config = AcceptanceConfig.from_settings(settings, RECIPE_PATH, "test-code")
    store = _RecordingEvidenceStore()
    scheduled_at = runtime.snapshot().started_at + timedelta(seconds=300)
    runtime_membership = WsMembershipSnapshot(
        generation=19,
        desired=frozenset({"runtime-desired"}),
        committed=frozenset({"runtime-committed"}),
        evidenced=frozenset({"runtime-committed"}),
        evidenced_at={"runtime-committed": datetime(2026, 7, 23, tzinfo=UTC)},
    )
    runtime.update_membership(runtime_membership)
    settings.supabase_service_key = MagicMock()
    settings.supabase_service_key.get_secret_value.side_effect = RuntimeError(
        "private dependency payload"
    )

    result = await l3_promote.promote_run(
        settings=settings,
        ws_consumer=_truthful_consumer(initial_committed={"consumer-only"}),
        recipe_yaml_path=RECIPE_PATH,
        evidence_store=store,
        evidence_runtime=runtime,
        acceptance_config=acceptance_config,
        scheduled_at=scheduled_at,
        run_seq=101,
    )

    assert (result.status.value, result.reason_code) == (
        "failed",
        "unexpected_exception",
    )
    assert result.persisted is True
    assert len(store.records) == 1
    record = store.records[0]
    assert (record.run_seq, record.scheduled_at, record.ws_generation) == (
        101,
        scheduled_at,
        19,
    )
    assert record.acceptance_config_hash == acceptance_config.digest()
    assert record.desired_hash == stable_sha256(["runtime-desired"])
    assert record.committed_hash == stable_sha256(["runtime-committed"])


@pytest.mark.asyncio
async def test_run_periodic_stops_after_first_append_attempt_exception() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings()
    settings.supabase_url = ""
    settings.supabase_service_key = ""
    runtime = _make_runtime(settings)
    acceptance_config = AcceptanceConfig.from_settings(settings, RECIPE_PATH, "test-code")
    stop_event = asyncio.Event()

    class _AppendRaises:
        def __init__(self) -> None:
            self.attempted_run_seqs: list[int] = []

        async def append_promote_run(self, record: Any) -> bool:
            self.attempted_run_seqs.append(record.run_seq)
            raise RuntimeError("ambiguous writer payload must not be retried")

    store = _AppendRaises()

    with patch.object(l3_promote, "_utc_now", return_value=runtime.snapshot().started_at):
        await asyncio.wait_for(
            l3_promote.run_periodic(
                stop_event=stop_event,
                settings=settings,
                ws_consumer=MagicMock(),
                recipe_yaml_path=RECIPE_PATH,
                evidence_store=store,
                evidence_runtime=runtime,
                acceptance_config=acceptance_config,
            ),
            timeout=1.0,
        )

    assert store.attempted_run_seqs == [0]
    status = runtime.snapshot()
    assert status.writer_ok is False
    assert status.status.value == "fail"
    assert status.writer_reason_code == "promote_run_unexpected_exception"


@pytest.mark.asyncio
async def test_run_periodic_propagates_tick_cancellation_without_writer_failure() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings()
    runtime = _make_runtime(settings)
    store = _RecordingEvidenceStore()
    acceptance_config = AcceptanceConfig.from_settings(settings, RECIPE_PATH, "test-code")

    with (
        patch.object(l3_promote, "promote_run", side_effect=asyncio.CancelledError),
        patch.object(l3_promote, "_utc_now", return_value=runtime.snapshot().started_at),
    ):
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(
                l3_promote.run_periodic(
                    stop_event=asyncio.Event(),
                    settings=settings,
                    ws_consumer=MagicMock(),
                    recipe_yaml_path=RECIPE_PATH,
                    evidence_store=store,
                    evidence_runtime=runtime,
                    acceptance_config=acceptance_config,
                ),
                timeout=1.0,
            )

    assert runtime.snapshot().writer_ok is None


@pytest.mark.asyncio
async def test_run_periodic_cancellation_during_future_grid_wait_is_prompt() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings()
    runtime = _make_runtime(settings)
    store = _RecordingEvidenceStore()
    stop_event = asyncio.Event()
    boot_started_at = runtime.snapshot().started_at

    with patch.object(
        l3_promote,
        "_utc_now",
        return_value=boot_started_at - timedelta(hours=1),
    ):
        task = asyncio.create_task(
            l3_promote.run_periodic(
                stop_event=stop_event,
                settings=settings,
                ws_consumer=MagicMock(),
                recipe_yaml_path=RECIPE_PATH,
                evidence_store=store,
                evidence_runtime=runtime,
            )
        )
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=0.2)


@pytest.mark.asyncio
async def test_promote_record_preserves_scheduled_to_start_delay() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings(supabase_url="", service_key="")
    runtime = _make_runtime(settings)
    store = _RecordingEvidenceStore()
    scheduled_at = datetime.now(UTC) - timedelta(seconds=7)

    await l3_promote.promote_run(
        settings=settings,
        ws_consumer=_truthful_consumer(),
        recipe_yaml_path=RECIPE_PATH,
        evidence_store=store,
        evidence_runtime=runtime,
        run_seq=0,
        scheduled_at=scheduled_at,
    )

    assert len(store.records) == 1
    assert store.records[0].scheduled_at == scheduled_at
    assert store.records[0].started_at >= scheduled_at + timedelta(seconds=7)


# ────────────────────────────────────────────────────────────────────────
# Test 12 — run_periodic uses raw asyncio.wait_for pattern (lint)
# ────────────────────────────────────────────────────────────────────────


def test_run_periodic_uses_wait_for_pattern() -> None:
    from polyarb.observation import l3_promote

    src = inspect.getsource(l3_promote.run_periodic)
    assert "asyncio.wait_for" in src, "must use asyncio.wait_for (cross-pattern decision #4)"
    assert "stop_event.wait" in src, "must wait on stop_event (matches ws_consumer.run)"
    # Anti-regression — no apscheduler import.
    assert "apscheduler" not in src.lower(), "raw asyncio loop only — no apscheduler"
