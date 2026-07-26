"""Tests for polyarb.observation.watchlist — yaml.safe_load + restricted AST eval.

Plan 05 Task 2 — covers:
- load_watchlist: safe_load, missing file, invalid alert_when
- evaluate_alert: simple/compound, rejection of Call/Attribute/unknown var
- check_alerts: triggered, missing markets
- Warning #9: None bid/ask/spread → skip
- Security invariants: no yaml.load(, no Python builtin eval/exec
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from polyarb.observation.watchlist import (
    ALLOWED_VARS,
    check_alerts,
    evaluate_alert,
    load_watchlist,
)


@pytest.fixture
def watchlist_yaml(tmp_path: Path) -> Path:
    p = tmp_path / "watchlist.yaml"
    p.write_text("""
- slug: test-slug-1
  reason: test market
  alert_when: spread < 0.02
  added: 2026-05-01

- slug: test-slug-2
  reason: another
  alert_when: mid > 0.6 and liq > 1000
  added: 2026-05-02

- slug: test-slug-3
  reason: no alert
  added: 2026-05-03
""")
    return p


@pytest.fixture
def db_with_markets(tmp_path: Path) -> Path:
    db_path = tmp_path / "state.db"
    con = sqlite3.connect(str(db_path))
    con.executescript("""
        CREATE TABLE markets (
            market_id TEXT PRIMARY KEY, condition_id TEXT, slug TEXT, question TEXT,
            yes_token_id TEXT, no_token_id TEXT, mid_price REAL, liquidity_usd REAL,
            volume_usd REAL, best_bid_price REAL, best_bid_size REAL,
            best_ask_price REAL, best_ask_size REAL, end_time_ms INTEGER,
            active INTEGER, closed INTEGER, neg_risk INTEGER, neg_risk_market_id TEXT,
            fetched_at_ms INTEGER, snapshot_id INTEGER, incomplete INTEGER,
            event_id TEXT
        );
    """)
    con.execute(
        "INSERT INTO markets VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "m1",
            "c1",
            "test-slug-1",
            "Q1",
            "ty",
            "tn",
            0.50,
            1000.0,
            100.0,
            0.49,
            10.0,
            0.50,
            10.0,
            1800000000000,
            1,
            0,
            0,
            None,
            1700000000000,
            1,
            0,
            None,
        ),
    )
    con.execute(
        "INSERT INTO markets VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "m2",
            "c2",
            "test-slug-2",
            "Q2",
            "ty",
            "tn",
            0.70,
            5000.0,
            200.0,
            0.68,
            10.0,
            0.72,
            10.0,
            1800000000000,
            1,
            0,
            0,
            None,
            1700000000000,
            1,
            0,
            None,
        ),
    )
    con.commit()
    con.close()
    return db_path


# load_watchlist


def test_load_watchlist_returns_entries(watchlist_yaml: Path) -> None:
    entries = load_watchlist(watchlist_yaml)
    assert len(entries) == 3
    assert entries[0].slug == "test-slug-1"
    assert entries[0].alert_when == "spread < 0.02"
    assert entries[2].alert_when is None


def test_load_watchlist_missing_file_returns_empty(tmp_path: Path) -> None:
    assert load_watchlist(tmp_path / "nonexistent.yaml") == []


def test_load_watchlist_safe_load_rejects_python_object(tmp_path: Path) -> None:
    p = tmp_path / "bad.yaml"
    p.write_text(
        "- slug: x\n  reason: r\n"
        "  alert_when: !!python/object:os.system\n    args: [echo h]\n  added: 2026-01-01\n"
    )
    with pytest.raises(Exception):
        load_watchlist(p)


def test_load_watchlist_invalid_alert_when_disabled(tmp_path: Path) -> None:
    p = tmp_path / "bad.yaml"
    p.write_text(
        "- slug: x\n  reason: r\n  alert_when: mid.__class__.__bases__\n  added: 2026-01-01\n"
    )
    entries = load_watchlist(p)
    assert len(entries) == 1
    assert entries[0].alert_when is None


# evaluate_alert


def test_evaluate_alert_simple() -> None:
    assert evaluate_alert("spread < 0.02", {"best_ask_price": 0.51, "best_bid_price": 0.50}) is True


def test_evaluate_alert_simple_false() -> None:
    assert (
        evaluate_alert("spread < 0.02", {"best_ask_price": 0.51, "best_bid_price": 0.40}) is False
    )


def test_evaluate_alert_compound_and() -> None:
    assert (
        evaluate_alert("mid > 0.6 and liq > 1000", {"mid_price": 0.70, "liquidity_usd": 5000.0})
        is True
    )


def test_evaluate_alert_compound_and_false() -> None:
    assert (
        evaluate_alert("mid > 0.6 and liq > 1000", {"mid_price": 0.70, "liquidity_usd": 500.0})
        is False
    )


def test_evaluate_alert_compound_or() -> None:
    assert (
        evaluate_alert("mid < 0.4 or mid > 0.6", {"mid_price": 0.30, "liquidity_usd": 5000.0})
        is True
    )


def test_evaluate_alert_rejects_function_call() -> None:
    with pytest.raises(ValueError, match=r"unknown variable|not allowed"):
        evaluate_alert("len(spread) > 0", {"best_bid_price": 0.50, "best_ask_price": 0.51})


def test_evaluate_alert_rejects_attribute() -> None:
    with pytest.raises(ValueError, match="not allowed"):
        evaluate_alert("spread.real > 0", {"best_bid_price": 0.50, "best_ask_price": 0.51})


def test_evaluate_alert_rejects_unknown_var() -> None:
    with pytest.raises(ValueError, match="unknown variable"):
        evaluate_alert("foo > 1", {"mid_price": 0.5})


def test_evaluate_alert_rejects_string_constant() -> None:
    with pytest.raises(ValueError, match="numeric constants"):
        evaluate_alert("'malicious' < 1", {"mid_price": 0.5})


def test_evaluate_alert_long_expression_capped() -> None:
    long_expr = "mid > 0.5 " + "and mid > 0.5 " * 50
    with pytest.raises(ValueError, match="too long"):
        evaluate_alert(long_expr.strip(), {"mid_price": 0.5})


def test_evaluate_alert_skips_when_bid_none() -> None:
    assert (
        evaluate_alert("spread < 0.02", {"best_bid_price": None, "best_ask_price": 0.51}) is False
    )


def test_evaluate_alert_skips_when_ask_none() -> None:
    assert (
        evaluate_alert("spread < 0.02", {"best_bid_price": 0.50, "best_ask_price": None}) is False
    )


def test_evaluate_alert_skips_when_mid_none() -> None:
    assert evaluate_alert("mid > 0.5", {"mid_price": None}) is False


def test_evaluate_alert_aliases_to_fullnames() -> None:
    row = {"mid_price": 0.55}
    assert evaluate_alert("mid > 0.5", row) is True
    assert evaluate_alert("mid_price > 0.5", row) is True


def test_evaluate_alert_not_unary() -> None:
    assert evaluate_alert("not mid > 0.5", {"mid_price": 0.30}) is True


def test_evaluate_alert_negation() -> None:
    assert evaluate_alert("mid < 0", {"mid_price": -0.10}) is True


def test_evaluate_alert_arithmetic() -> None:
    assert evaluate_alert("mid * liq > 500", {"mid_price": 0.50, "liquidity_usd": 2000.0}) is True


# check_alerts


def test_check_alerts_returns_triggered(watchlist_yaml: Path, db_with_markets: Path) -> None:
    entries = load_watchlist(watchlist_yaml)
    triggered = check_alerts(entries, db_with_markets)
    # test-slug-1: spread=0.01 < 0.02, test-slug-2: mid=0.7>0.6 and liq=5000>1000
    assert len(triggered) == 2
    slugs = {e.slug for e, _ in triggered}
    assert "test-slug-1" in slugs
    assert "test-slug-2" in slugs


def test_check_alerts_skips_missing_market(watchlist_yaml: Path, tmp_path: Path) -> None:
    db_path = tmp_path / "empty.db"
    con = sqlite3.connect(str(db_path))
    con.executescript("""
        CREATE TABLE markets (
            market_id TEXT PRIMARY KEY, condition_id TEXT, slug TEXT, question TEXT,
            yes_token_id TEXT, no_token_id TEXT, mid_price REAL, liquidity_usd REAL,
            volume_usd REAL, best_bid_price REAL, best_bid_size REAL,
            best_ask_price REAL, best_ask_size REAL, end_time_ms INTEGER,
            active INTEGER, closed INTEGER, neg_risk INTEGER, neg_risk_market_id TEXT,
            fetched_at_ms INTEGER, snapshot_id INTEGER, incomplete INTEGER,
            event_id TEXT
        );
    """)
    con.close()
    entries = load_watchlist(watchlist_yaml)
    triggered = check_alerts(entries, db_path)
    assert len(triggered) == 0


# Security invariant source scans


def test_yaml_safe_load_invariant_in_source() -> None:
    src = (
        Path(__file__).parent.parent.parent / "src" / "polyarb" / "observation" / "watchlist.py"
    ).read_text()
    assert "yaml.safe_load" in src
    assert "yaml.load(" not in src


def test_no_python_eval_in_source() -> None:
    import re

    src = (
        Path(__file__).parent.parent.parent / "src" / "polyarb" / "observation" / "watchlist.py"
    ).read_text()
    # Strip docstrings so mentions of eval/exec in comments don't false-positive
    code = re.sub(r'""".*?"""', "", src, flags=re.DOTALL)
    code = re.sub(r"'''.*?'''", "", code, flags=re.DOTALL)
    assert "eval(" not in code
    assert "exec(" not in code


# ALLOWED_VARS


def test_allowed_vars_has_11_keys() -> None:
    assert len(ALLOWED_VARS) == 11
    for k in ("mid", "bid", "ask", "spread", "liq", "vol"):
        assert k in ALLOWED_VARS
