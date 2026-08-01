from __future__ import annotations

import sqlite3
import time

import pytest

from polyarb.routing.neg_risk_quote_store import (
    NegRiskQuoteStore,
    PersistedQuote,
    QuoteUniverseUnavailableError,
)
from polyarb.routing.opportunity_scanner import (
    QuoteRunUnavailableError,
    StaleQuoteRunError,
    StaleUniverseError,
    assess_certified_neg_risk_quote_projection,
    scan_certified_neg_risk_quote_projection,
    scan_neg_risk_buy_all,
    scan_neg_risk_quote_run,
    scan_verified_neg_risk_quote_run,
)
from polyarb.storage.sqlite_store import SQLiteStore


@pytest.fixture
def market_db(tmp_path):
    path = tmp_path / "state.db"
    SQLiteStore(path).init_schema()
    with sqlite3.connect(path) as con:
        con.execute(
            "INSERT INTO snapshots("
            "taken_at_ms, finished_at_ms, mode, market_count, is_valid, parquet_path"
            ") VALUES (?, ?, 'subset', 2, 1, 'fixture.parquet')",
            (int(time.time() * 1000), int(time.time() * 1000)),
        )
        snapshot_id = int(con.execute("SELECT last_insert_rowid()").fetchone()[0])
        con.executemany(
            "INSERT INTO markets("
            "market_id, condition_id, slug, yes_token_id, best_ask_price, "
            "best_ask_size, active, closed, incomplete, neg_risk_market_id, "
            "fetched_at_ms, snapshot_id"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "m1",
                    "c1",
                    "alpha",
                    "yes-1",
                    0.40,
                    12,
                    1,
                    0,
                    0,
                    "g1",
                    int(time.time() * 1000),
                    snapshot_id,
                ),
                (
                    "m2",
                    "c2",
                    "beta",
                    "yes-2",
                    0.55,
                    8,
                    1,
                    0,
                    0,
                    "g1",
                    int(time.time() * 1000),
                    snapshot_id,
                ),
            ],
        )
    return path


QUOTE_NOW_S = 10_000.0


def _seed_quote_universe(path, *, taken_at_ms: int = 9_900_000) -> int:
    with sqlite3.connect(path) as con:
        con.execute(
            "INSERT INTO snapshots("
            "taken_at_ms,finished_at_ms,mode,market_count,market_view_published,"
            "data_product,is_valid,parquet_path"
            ") VALUES (?,?,'subset',4,1,'structure',1,'quote-universe.parquet')",
            (taken_at_ms, taken_at_ms),
        )
        snapshot_id = int(con.execute("SELECT last_insert_rowid()").fetchone()[0])
        con.executemany(
            "INSERT INTO markets("
            "market_id, condition_id, slug, yes_token_id, best_ask_price, "
            "best_ask_size, active, closed, incomplete, neg_risk_market_id, "
            "fetched_at_ms, snapshot_id,event_id"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "m1", "c1", "alpha", "yes-1", 0.01, 99, 1, 0, 0,
                    "g1", taken_at_ms, snapshot_id, "e1",
                ),
                (
                    "m2", "c2", "beta", "yes-2", 0.01, 99, 1, 0, 0,
                    "g1", taken_at_ms, snapshot_id, "e1",
                ),
                (
                    "m3", "c3", "gamma", "yes-3", 0.01, 99, 1, 0, 0,
                    "g2", taken_at_ms, snapshot_id, "e2",
                ),
                (
                    "m4", "c4", "delta", "yes-4", 0.01, 99, 1, 0, 0,
                    "g2", taken_at_ms, snapshot_id, "e2",
                ),
            ],
        )
        con.execute(
            "INSERT INTO snapshot_source_coverage("
            "snapshot_id,completed,market_items,event_items"
            ") VALUES (?,1,4,2)",
            (snapshot_id,),
        )
        con.executemany(
            "INSERT INTO event_market_memberships("
            "snapshot_id,event_id,neg_risk_market_id,market_id,member_kind,active,closed"
            ") VALUES (?,?,?,?, 'named',1,0)",
            [
                (snapshot_id, "e1", "g1", "m1"),
                (snapshot_id, "e1", "g1", "m2"),
                (snapshot_id, "e2", "g2", "m3"),
                (snapshot_id, "e2", "g2", "m4"),
            ],
        )
        con.executemany(
            "INSERT INTO neg_risk_group_truth("
            "snapshot_id,event_id,neg_risk_market_id,neg_risk_type,"
            "expected_member_count,active_named_count,membership_hash,quality,reason"
            ") VALUES (?,?,?,'standard',2,2,?,'complete-supported',NULL)",
            [
                (snapshot_id, "e1", "g1", "hash-g1"),
                (snapshot_id, "e2", "g2", "hash-g2"),
            ],
        )
        # Direct fixture publication boundary; production writers clear this
        # only after all source rows are ready to commit.
        con.execute("DELETE FROM legacy_structure_revision_dirty WHERE id=1")
    return snapshot_id


@pytest.fixture
def quote_db(tmp_path):
    path = tmp_path / "quote-state.db"
    SQLiteStore(path).init_schema()
    _seed_quote_universe(path)
    return path


def _complete_quote_run(
    path,
    *,
    quoted_at_ms: int = 9_900_000,
    terminal_states: dict[str, str] | None = None,
    asks: dict[str, tuple[float, float]] | None = None,
) -> int:
    store = NegRiskQuoteStore(path)
    universe = store.latest_verified_universe()
    legs = universe.legs
    run_id = store.begin_verified_run(universe, quoted_at_ms=quoted_at_ms)
    terminal_states = terminal_states or {}
    asks = asks or {
        "yes-1": (0.40, 12.0),
        "yes-2": (0.55, 8.0),
        "yes-3": (0.42, 7.0),
        "yes-4": (0.40, 9.0),
    }
    quotes = []
    for leg in legs:
        state = terminal_states.get(leg.yes_token_id, "executable")
        price, size = asks[leg.yes_token_id] if state == "executable" else (None, None)
        quotes.append(
            PersistedQuote(
                leg.neg_risk_market_id,
                leg.market_id,
                leg.condition_id,
                leg.slug,
                leg.yes_token_id,
                state,
                price,
                size,
                leg.event_id,
                leg.membership_hash,
            )
        )
    store.record_terminal_quotes(run_id, tuple(quotes))
    store.complete_run(
        run_id,
        completed_at_ms=quoted_at_ms + 1,
        successful_response_count=len(quotes),
    )
    return run_id


def test_scan_returns_executable_buy_all_bundle(market_db) -> None:
    opportunities = scan_neg_risk_buy_all(market_db, min_edge_bps=100)

    assert len(opportunities) == 1
    opportunity = opportunities[0]
    assert opportunity.group_id == "g1"
    assert opportunity.sum_asks == pytest.approx(0.95)
    assert opportunity.gross_edge_bps == pytest.approx(500)
    assert opportunity.executable_quantity == pytest.approx(8)
    assert opportunity.gross_profit == pytest.approx(0.4)
    assert [leg.yes_token_id for leg in opportunity.legs] == ["yes-1", "yes-2"]


def test_scan_rejects_group_when_any_sibling_has_no_executable_ask(market_db) -> None:
    with sqlite3.connect(market_db) as con:
        con.execute("UPDATE markets SET best_ask_size = NULL WHERE market_id = 'm2'")

    assert scan_neg_risk_buy_all(market_db, min_edge_bps=0) == []


def test_scan_applies_gross_edge_threshold(market_db) -> None:
    assert scan_neg_risk_buy_all(market_db, min_edge_bps=501) == []


def test_scan_rejects_non_finite_threshold(market_db) -> None:
    with pytest.raises(ValueError, match="min_edge_bps must be finite"):
        scan_neg_risk_buy_all(market_db, min_edge_bps=float("nan"))


def test_quote_run_scan_uses_one_complete_run_and_preserves_arithmetic_and_order(
    quote_db,
) -> None:
    run_id = _complete_quote_run(quote_db)

    opportunities = scan_neg_risk_quote_run(quote_db, min_edge_bps=100, now_s=lambda: QUOTE_NOW_S)

    assert [item.group_id for item in opportunities] == ["g2", "g1"]
    first, second = opportunities
    assert (first.sum_asks, first.executable_quantity, first.gross_edge_bps) == pytest.approx(
        (0.82, 7.0, 1800.0)
    )
    assert (second.sum_asks, second.executable_quantity, second.gross_edge_bps) == pytest.approx(
        (0.95, 8.0, 500.0)
    )
    assert first.quote_run_id == second.quote_run_id == run_id
    assert first.quote_age_seconds == second.quote_age_seconds == pytest.approx(100.0)
    assert first.universe_snapshot_id == second.universe_snapshot_id == 1
    assert first.universe_age_seconds == second.universe_age_seconds == pytest.approx(100.0)
    assert first.to_dict()["quote_run_id"] == run_id


def test_quote_run_scan_rejects_an_entire_group_with_a_non_executable_sibling(
    quote_db,
) -> None:
    _complete_quote_run(quote_db, terminal_states={"yes-2": "missing-ask"})

    opportunities = scan_neg_risk_quote_run(quote_db, now_s=lambda: QUOTE_NOW_S)

    assert [item.group_id for item in opportunities] == ["g2"]


def test_verified_scan_exposes_exact_identity_and_bounded_rejections(quote_db) -> None:
    with sqlite3.connect(quote_db) as con:
        con.execute(
            "INSERT INTO neg_risk_group_truth("
            "snapshot_id,event_id,neg_risk_market_id,neg_risk_type,"
            "expected_member_count,active_named_count,membership_hash,quality,reason"
            ") VALUES (1,'e3','g3','augmented',1,1,'hash-g3',"
            "'complete-unsupported','augmented-neg-risk-not-supported')"
        )
        con.execute("DELETE FROM legacy_structure_revision_dirty WHERE id=1")
    run_id = _complete_quote_run(
        quote_db,
        terminal_states={"yes-2": "missing-ask"},
    )
    projection = NegRiskQuoteStore(quote_db).latest_complete_projection()
    assert projection is not None

    result = scan_verified_neg_risk_quote_run(
        quote_db,
        now_s=lambda: QUOTE_NOW_S,
    )

    assert [item.group_id for item in result.opportunities] == ["g2"]
    candidate = result.opportunities[0]
    assert candidate.event_id == "e2"
    assert candidate.membership_hash == "hash-g2"
    assert candidate.quality == "complete-supported"
    assert candidate.quote_run_id == run_id
    assert result.source_snapshot_id == 1
    assert result.universe_hash == projection.universe_hash
    assert result.quote_run_id == run_id
    assert result.rejections == {
        "augmented-neg-risk-not-supported": 1,
        "incomplete-quotes": 1,
    }


def test_cached_projection_scan_matches_database_verified_scan(quote_db) -> None:
    _complete_quote_run(quote_db)
    projection = NegRiskQuoteStore(quote_db).latest_complete_projection()
    assert projection is not None

    cached = scan_certified_neg_risk_quote_projection(
        projection,
        min_edge_bps=100,
        now_s=lambda: QUOTE_NOW_S,
    )
    database = scan_verified_neg_risk_quote_run(
        quote_db,
        min_edge_bps=100,
        now_s=lambda: QUOTE_NOW_S,
    )

    assert cached == database


def test_verified_scan_recomputes_invalid_group_rejections_for_exact_source(
    quote_db,
) -> None:
    with sqlite3.connect(quote_db) as con:
        con.execute("DELETE FROM markets WHERE market_id='m2'")
        con.execute("DELETE FROM legacy_structure_revision_dirty WHERE id=1")
    _complete_quote_run(quote_db)

    result = scan_verified_neg_risk_quote_run(
        quote_db,
        now_s=lambda: QUOTE_NOW_S,
    )

    assert result.rejections == {"membership-market-mismatch": 1}


def test_verified_scan_fails_closed_on_incomplete_source_truth(quote_db) -> None:
    with sqlite3.connect(quote_db) as con:
        con.execute(
            "INSERT INTO neg_risk_group_truth("
            "snapshot_id,event_id,neg_risk_market_id,neg_risk_type,"
            "expected_member_count,active_named_count,membership_hash,quality,reason"
            ") VALUES (1,'e3','g3','standard',0,0,'',"
            "'incomplete-source','source-membership-missing')"
        )
        con.execute("DELETE FROM legacy_structure_revision_dirty WHERE id=1")
    _complete_quote_run(quote_db)

    with pytest.raises(QuoteUniverseUnavailableError):
        scan_verified_neg_risk_quote_run(
            quote_db,
            now_s=lambda: QUOTE_NOW_S,
        )


def test_verified_scan_keeps_certified_run_when_new_source_is_still_dirty(
    quote_db,
) -> None:
    _complete_quote_run(quote_db)
    with sqlite3.connect(quote_db) as con:
        con.execute(
            "UPDATE neg_risk_group_truth SET membership_hash='changed' "
            "WHERE neg_risk_market_id='g1'"
        )

    result = scan_verified_neg_risk_quote_run(
        quote_db,
        now_s=lambda: QUOTE_NOW_S,
    )

    assert result.quote_run_id == 1


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("best_ask_price", float("nan")),
        ("best_ask_price", float("inf")),
        ("best_ask_size", float("nan")),
        ("best_ask_size", float("inf")),
        ("best_ask_size", "not-a-number"),
    ],
)
def test_verified_scan_fails_closed_on_unusable_executable_numeric_truth(
    quote_db,
    column: str,
    value: object,
) -> None:
    _complete_quote_run(quote_db)
    with sqlite3.connect(quote_db) as con, pytest.raises(
        sqlite3.IntegrityError, match="complete quotes are immutable"
    ):
        con.execute("PRAGMA ignore_check_constraints=ON")
        con.execute(
            f"UPDATE neg_risk_quotes SET {column}=? WHERE yes_token_id='yes-1'",
            (value,),
        )

    assert scan_verified_neg_risk_quote_run(
        quote_db,
        now_s=lambda: QUOTE_NOW_S,
    ).quote_run_id == 1


def test_quote_run_scan_ignores_newer_failed_and_collecting_runs(quote_db) -> None:
    complete_id = _complete_quote_run(quote_db, quoted_at_ms=9_900_000)
    store = NegRiskQuoteStore(quote_db)
    universe = store.latest_verified_universe()
    snapshot_id, taken_at_ms, legs = (
        universe.snapshot_id,
        universe.taken_at_ms,
        universe.legs,
    )
    failed_id = store.begin_run(
        universe_snapshot_id=snapshot_id,
        universe_taken_at_ms=taken_at_ms,
        legs=legs,
        quoted_at_ms=9_950_000,
    )
    store.fail_run(failed_id, failure_reason="fixture")
    store.begin_run(
        universe_snapshot_id=snapshot_id,
        universe_taken_at_ms=taken_at_ms,
        legs=legs,
        quoted_at_ms=9_960_000,
    )

    opportunities = scan_neg_risk_quote_run(quote_db, now_s=lambda: QUOTE_NOW_S)

    assert {item.quote_run_id for item in opportunities} == {complete_id}


def test_quote_run_scan_quote_sla_boundary_and_exact_error(quote_db) -> None:
    _complete_quote_run(quote_db, quoted_at_ms=9_700_000)

    assert scan_neg_risk_quote_run(quote_db, now_s=lambda: QUOTE_NOW_S)
    with pytest.raises(
        StaleQuoteRunError,
        match=r"^quote age 300\.1s exceeds 300\.0s$",
    ):
        scan_neg_risk_quote_run(quote_db, now_s=lambda: 10_000.1)


def test_quote_run_scan_universe_sla_boundary_and_exact_error(tmp_path) -> None:
    quote_db = tmp_path / "stale-universe.db"
    SQLiteStore(quote_db).init_schema()
    _seed_quote_universe(quote_db, taken_at_ms=-40_400_000)
    _complete_quote_run(quote_db, quoted_at_ms=QUOTE_NOW_S * 1000)

    assert scan_neg_risk_quote_run(quote_db, now_s=lambda: QUOTE_NOW_S)
    with pytest.raises(
        StaleUniverseError,
        match=r"^universe age 50400\.1s exceeds 50400\.0s$",
    ):
        scan_neg_risk_quote_run(quote_db, now_s=lambda: 10_000.1)


def test_quote_run_scan_is_unavailable_without_a_complete_run(quote_db) -> None:
    with pytest.raises(QuoteRunUnavailableError, match=r"^quote run unavailable$"):
        scan_neg_risk_quote_run(quote_db, now_s=lambda: QUOTE_NOW_S)


@pytest.mark.parametrize("legacy_hash", ["", "a" * 64])
def test_quote_run_scan_ignores_legacy_unverified_complete_run(
    quote_db,
    legacy_hash: str,
) -> None:
    with sqlite3.connect(quote_db) as con:
        cursor = con.execute(
            "INSERT INTO neg_risk_quote_runs("
            "universe_snapshot_id,universe_taken_at_ms,universe_hash,quoted_at_ms,"
            "requested_token_count,successful_response_count,lease_expires_at_ms,"
            "status,completed_at_ms"
            ") VALUES (1,?,?,?,1,0,?,'collecting',NULL)",
            (9_900_000, legacy_hash, 9_990_000, 10_000_000),
        )
        legacy_id = int(cursor.lastrowid)
        con.execute(
            "INSERT INTO neg_risk_quote_run_legs("
            "quote_run_id,neg_risk_market_id,event_id,membership_hash,"
            "market_id,condition_id,slug,yes_token_id"
            ") VALUES (?,'g1','','','legacy-market','legacy-condition',"
            "'legacy','legacy-token')",
            (legacy_id,),
        )
        con.execute(
            "INSERT INTO neg_risk_quotes("
            "quote_run_id,neg_risk_market_id,event_id,membership_hash,"
            "market_id,condition_id,slug,yes_token_id,terminal_state,"
            "best_ask_price,best_ask_size"
            ") VALUES (?,'g1','','','legacy-market','legacy-condition',"
            "'legacy','legacy-token','missing-book',NULL,NULL)",
            (legacy_id,),
        )
        con.execute(
            "UPDATE neg_risk_quote_runs SET status='complete',completed_at_ms=? WHERE id=?",
            (9_990_001, legacy_id),
        )

    with pytest.raises(QuoteRunUnavailableError, match=r"^quote run unavailable$"):
        scan_neg_risk_quote_run(quote_db, now_s=lambda: QUOTE_NOW_S)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"min_edge_bps": True},
        {"max_quote_age_s": True},
        {"max_universe_age_s": True},
        {"limit": True},
    ],
)
def test_quote_run_scan_rejects_ambiguous_boolean_arguments(quote_db, kwargs) -> None:
    with pytest.raises(ValueError):
        scan_neg_risk_quote_run(quote_db, **kwargs)


def test_assessment_distinguishes_observe_no_edge_and_unavailable(quote_db) -> None:
    _complete_quote_run(
        quote_db,
        asks={
            "yes-1": (0.55, 12.0),
            "yes-2": (0.45, 8.0),
            "yes-3": (0.42, 7.0),
            "yes-4": (0.40, 9.0),
        },
    )
    first_projection = NegRiskQuoteStore(quote_db).latest_complete_projection()
    assert first_projection is not None

    first = assess_certified_neg_risk_quote_projection(
        first_projection,
        min_edge_bps=100,
        now_s=lambda: QUOTE_NOW_S,
    )
    first_by_group = {item.group_id: item for item in first.assessments}
    assert first_by_group["g1"].status == "no-edge"
    assert first_by_group["g2"].status == "observe"

    _complete_quote_run(
        quote_db,
        quoted_at_ms=9_901_000,
        terminal_states={"yes-2": "missing-ask"},
    )
    incomplete_projection = NegRiskQuoteStore(quote_db).latest_complete_projection()
    assert incomplete_projection is not None

    incomplete = assess_certified_neg_risk_quote_projection(
        incomplete_projection,
        min_edge_bps=100,
        now_s=lambda: QUOTE_NOW_S,
    )
    by_group = {item.group_id: item for item in incomplete.assessments}
    assert by_group["g1"].status == "unavailable"
    assert by_group["g1"].reason == "incomplete-quotes"
