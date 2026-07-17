from __future__ import annotations

import sqlite3
import time

import pytest

from polyarb.routing.opportunity_scanner import scan_neg_risk_buy_all


@pytest.fixture
def market_db(tmp_path):
    path = tmp_path / "state.db"
    with sqlite3.connect(path) as con:
        con.executescript(
            """
            CREATE TABLE snapshots (id INTEGER PRIMARY KEY, taken_at_ms INTEGER);
            CREATE TABLE markets (
              market_id TEXT PRIMARY KEY, condition_id TEXT, slug TEXT,
              yes_token_id TEXT, best_ask_price REAL, best_ask_size REAL,
              active INTEGER, closed INTEGER, incomplete INTEGER,
              neg_risk_market_id TEXT, snapshot_id INTEGER
            );
            """
        )
        con.execute(
            "INSERT INTO snapshots VALUES (1, ?)", (int(time.time() * 1000),)
        )
        con.executemany(
            "INSERT INTO markets VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            [
                ("m1", "c1", "alpha", "yes-1", 0.40, 12, 1, 0, 0, "g1", 1),
                ("m2", "c2", "beta", "yes-2", 0.55, 8, 1, 0, 0, "g1", 1),
            ],
        )
    return path


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
