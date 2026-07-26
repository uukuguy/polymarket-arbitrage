"""Tests for polyarb.observation.scanner — 4 layer defense + e2e queries.

Plan 03 Task 1 — covers:
- _validate_where (trusted=True bypass / trusted=False strict blacklist)
- _validate_order_by (trusted bypass / strict whitelist; bare column accepted)
- _validate_limit (int range, rejects bool / non-int)
- read-only sqlite URI engine-level write rejection
- run_recipe e2e: LEFT JOIN question_translations
- run_recipe_grouped: same validators called (Blocker #3 no-bypass)
- load_yaml_recipes: safe_load only / fail-fast / drops group_by / does not override builtin
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import pytest
import yaml as _yaml

from polyarb.observation.recipes import BUILTIN_RECIPES, Recipe
from polyarb.observation.scanner import (
    _validate_limit,
    _validate_order_by,
    _validate_where,
    list_all_recipes,
    load_yaml_recipes,
    run_recipe,
    run_recipe_grouped,
)
from polyarb.storage.schemas import DDL

# =============================================================================
# Fixture: tmp DB seeded with markets + question_translations + validation_issues
# =============================================================================


@pytest.fixture
def tmp_db_with_seed(tmp_path: Path) -> Path:
    """Seed a tmp SQLite DB with the project schema + 5 markets, 1 translation,
    1 ghost_book validation_issue.
    """
    db_path = tmp_path / "test.db"
    con = sqlite3.connect(str(db_path), isolation_level=None)
    try:
        con.executescript(DDL)
        # snapshot row
        con.execute(
            "INSERT INTO snapshots(id, taken_at_ms, finished_at_ms, mode, "
            "market_count, is_valid, parquet_path) "
            "VALUES (1, 1700000000000, 1700000060000, 'subset', 5, 1, '/tmp/x.parquet')"
        )
        # 1 event so by-tag can JOIN something
        con.execute(
            "INSERT INTO events(id, slug, title, ticker, active, closed, "
            "liquidity_usd, volume_usd, end_time_ms, fetched_at_ms, snapshot_id) "
            "VALUES ('EV-1', 'ev-slug', 'Event 1', 'TKR', 1, 0, 1000, 5000, "
            "1800000000000, 1700000000000, 1)"
        )
        con.execute(
            "INSERT INTO event_tags(event_id, tag_id, tag_label, tag_slug, snapshot_id) "
            "VALUES ('EV-1', '120', 'Crypto', 'crypto', 1)"
        )
        # 5 markets — covers thick / coin-flip / ghost / generic
        markets = [
            # market_id, slug, question, mid, liq, vol, bid, ask, end, neg_risk_id, event_id
            (
                "M1",
                "thick-1",
                "Will X happen?",
                0.5,
                200000,
                50000,
                0.40,
                0.55,
                1900000000000,
                None,
                "EV-1",
            ),
            (
                "M2",
                "near-1",
                "Will Y resolve?",
                0.5,
                5000,
                1000,
                0.49,
                0.51,
                1700100000000,
                None,
                "EV-1",
            ),
            (
                "M3",
                "ghost-1",
                "Will Z trigger?",
                0.5,
                50000,
                10000,
                0.45,
                0.55,
                2000000000000,
                None,
                "EV-1",
            ),
            (
                "M4",
                "coin-1",
                "Will A win?",
                0.50,
                10000,
                20000,
                0.49,
                0.51,
                1700050000000,
                None,
                "EV-1",
            ),
            (
                "M5",
                "neg-1",
                "Will B occur?",
                0.30,
                8000,
                4000,
                0.29,
                0.31,
                1900000000000,
                "NRG-1",
                "EV-1",
            ),
        ]
        for (
            mid_,
            slug,
            q,
            mid,
            liq,
            vol,
            bid,
            ask,
            end,
            nrg,
            eid,
        ) in markets:
            con.execute(
                "INSERT INTO markets(market_id, condition_id, slug, question, "
                "yes_token_id, no_token_id, mid_price, liquidity_usd, volume_usd, "
                "best_bid_price, best_bid_size, best_ask_price, best_ask_size, "
                "end_time_ms, active, closed, neg_risk, neg_risk_market_id, "
                "fetched_at_ms, snapshot_id, incomplete, event_id) "
                "VALUES (?, 'COND', ?, ?, NULL, NULL, ?, ?, ?, ?, 100, ?, 100, "
                "?, 1, 0, 0, ?, 1700000000000, 1, 0, ?)",
                (mid_, slug, q, mid, liq, vol, bid, ask, end, nrg, eid),
            )
        # one translation for M1
        con.execute(
            "INSERT INTO question_translations(question_hash, question_en, "
            "question_zh, translator_model, translated_at_ms, token_cost, "
            "retry_count, is_dead) "
            "VALUES ('hash1', 'Will X happen?', 'X 会发生吗？', 'test-model', "
            "1700000000000, 100, 0, 0)"
        )
        # one ghost_book validation_issue → M3
        con.execute(
            "INSERT INTO validation_issues(snapshot_id, layer, category, market_id, detail) "
            "VALUES (1, 4, 'ghost_book', 'M3', 'CLOB book absent')"
        )
    finally:
        con.close()
    return db_path


# =============================================================================
# _validate_where — trusted=False (yaml path)
# =============================================================================


def test_validate_where_rejects_semicolon() -> None:
    with pytest.raises(ValueError):
        _validate_where("liquidity_usd > 1 ; DROP TABLE markets", trusted=False)


def test_validate_where_rejects_double_dash_comment() -> None:
    with pytest.raises(ValueError):
        _validate_where("liquidity_usd > 1 -- evil", trusted=False)


def test_validate_where_rejects_block_comment() -> None:
    with pytest.raises(ValueError):
        _validate_where("/* evil */ liquidity_usd > 1", trusted=False)


def test_validate_where_rejects_drop() -> None:
    with pytest.raises(ValueError):
        _validate_where("DROP TABLE markets", trusted=False)


def test_validate_where_rejects_delete() -> None:
    with pytest.raises(ValueError):
        _validate_where("a DELETE b", trusted=False)


def test_validate_where_rejects_insert() -> None:
    with pytest.raises(ValueError):
        _validate_where("INSERT INTO markets VALUES (1)", trusted=False)


def test_validate_where_rejects_update() -> None:
    with pytest.raises(ValueError):
        _validate_where("UPDATE markets SET a=1", trusted=False)


def test_validate_where_rejects_attach() -> None:
    with pytest.raises(ValueError):
        _validate_where("ATTACH DATABASE 'evil.db' AS evil", trusted=False)


def test_validate_where_rejects_pragma() -> None:
    with pytest.raises(ValueError):
        _validate_where("PRAGMA writable_schema=1", trusted=False)


def test_validate_where_rejects_union() -> None:
    with pytest.raises(ValueError):
        _validate_where("a > 1 UNION ALL b", trusted=False)


def test_validate_where_rejects_select_in_yaml_path() -> None:
    """Blocker #2: yaml WHERE may not contain SELECT subqueries."""
    with pytest.raises(ValueError):
        _validate_where(
            "market_id IN (SELECT market_id FROM validation_issues)",
            trusted=False,
        )


def test_validate_where_accepts_valid_simple() -> None:
    # No exception
    _validate_where("liquidity_usd > 1000 AND mid_price BETWEEN 0.4 AND 0.6", trusted=False)


def test_validate_where_case_insensitive() -> None:
    with pytest.raises(ValueError):
        _validate_where("dRoP TABLE markets", trusted=False)


# =============================================================================
# _validate_where — trusted=True (builtin path)
# =============================================================================


def test_validate_where_trusted_bypasses_blacklist() -> None:
    """ghost-suspicious's WHERE contains 'SELECT' — must pass for trusted=True."""
    _validate_where(
        "market_id IN (SELECT v.market_id FROM validation_issues v WHERE v.category='ghost_book')",
        trusted=True,
    )


def test_validate_where_trusted_strftime_passes() -> None:
    _validate_where(
        "end_time_ms BETWEEN strftime('%s','now')*1000 AND strftime('%s','now','+72 hours')*1000",
        trusted=True,
    )


def test_validate_where_trusted_subquery_passes() -> None:
    """builtin ghost-suspicious shape — IN (SELECT ...) — must pass when trusted."""
    _validate_where("a IN (SELECT b FROM c WHERE d)", trusted=True)


def test_validate_where_trusted_drop_still_passes() -> None:
    """Trust-bypass is total — even DROP passes when trusted=True. This is by
    design: the bypass is only entered for git-controlled BUILTIN_RECIPES
    source code (which never contains DROP)."""
    _validate_where("DROP TABLE markets", trusted=True)


# =============================================================================
# _validate_order_by — trusted=False (yaml path)
# =============================================================================


def test_validate_order_by_yaml_accepts_simple_col() -> None:
    _validate_order_by("liquidity_usd DESC", trusted=False)


def test_validate_order_by_yaml_accepts_multi_col() -> None:
    _validate_order_by("category, liquidity_usd DESC", trusted=False)


def test_validate_order_by_accepts_bare_column() -> None:
    """Warning #6 / Blocker #2: bare column name (no ASC/DESC) must be accepted."""
    _validate_order_by("liquidity_usd", trusted=False)


def test_validate_order_by_yaml_rejects_arithmetic_expr() -> None:
    """Yaml strict path rejects arithmetic — only builtins can use it."""
    with pytest.raises(ValueError):
        _validate_order_by(
            "liquidity_usd * (best_ask_price - best_bid_price) DESC",
            trusted=False,
        )


def test_validate_order_by_yaml_rejects_paren() -> None:
    with pytest.raises(ValueError):
        _validate_order_by("(SELECT 1)", trusted=False)


def test_validate_order_by_yaml_rejects_semicolon() -> None:
    with pytest.raises(ValueError):
        _validate_order_by("liquidity_usd; DROP", trusted=False)


def test_validate_order_by_yaml_rejects_function_call() -> None:
    with pytest.raises(ValueError):
        _validate_order_by("ABS(SUM(mid_price) - 1.0) DESC", trusted=False)


# =============================================================================
# _validate_order_by — trusted=True (builtin path)
# =============================================================================


def test_validate_order_by_trusted_accepts_arithmetic() -> None:
    _validate_order_by(
        "liquidity_usd * (best_ask_price - best_bid_price) DESC",
        trusted=True,
    )


def test_validate_order_by_trusted_accepts_abs() -> None:
    _validate_order_by("ABS(SUM(mid_price) - 1.0) DESC", trusted=True)


# =============================================================================
# _validate_limit
# =============================================================================


def test_validate_limit_accepts_50() -> None:
    assert _validate_limit(50) == 50


def test_validate_limit_accepts_1() -> None:
    assert _validate_limit(1) == 1


def test_validate_limit_accepts_10000() -> None:
    assert _validate_limit(10000) == 10000


def test_validate_limit_rejects_zero() -> None:
    with pytest.raises(ValueError):
        _validate_limit(0)


def test_validate_limit_rejects_negative() -> None:
    with pytest.raises(ValueError):
        _validate_limit(-1)


def test_validate_limit_rejects_million() -> None:
    with pytest.raises(ValueError):
        _validate_limit(1_000_000)


def test_validate_limit_rejects_string() -> None:
    with pytest.raises(ValueError):
        _validate_limit("abc")  # type: ignore[arg-type]


def test_validate_limit_rejects_float() -> None:
    with pytest.raises(ValueError):
        _validate_limit(50.0)  # type: ignore[arg-type]


def test_validate_limit_rejects_bool() -> None:
    """bool is a subclass of int in Python — explicitly reject to prevent
    `limit: True` (=1) silently sneaking through yaml."""
    with pytest.raises(ValueError):
        _validate_limit(True)  # type: ignore[arg-type]


# =============================================================================
# Read-only SQLite URI — engine-level Layer 1 defense
# =============================================================================


def test_readonly_connection_rejects_insert(tmp_db_with_seed: Path) -> None:
    uri = f"file:{tmp_db_with_seed}?mode=ro"
    con = sqlite3.connect(uri, uri=True)
    try:
        with pytest.raises(sqlite3.OperationalError) as ei:
            con.execute(
                "INSERT INTO markets(market_id, condition_id, fetched_at_ms, "
                "snapshot_id) VALUES ('XX','C',1,1)"
            )
        assert "readonly" in str(ei.value).lower() or "read-only" in str(ei.value).lower()
    finally:
        con.close()


def test_readonly_connection_rejects_drop(tmp_db_with_seed: Path) -> None:
    uri = f"file:{tmp_db_with_seed}?mode=ro"
    con = sqlite3.connect(uri, uri=True)
    try:
        with pytest.raises(sqlite3.OperationalError):
            con.execute("DROP TABLE markets")
    finally:
        con.close()


# =============================================================================
# run_recipe e2e
# =============================================================================


def test_run_recipe_returns_dataframe(tmp_db_with_seed: Path) -> None:
    """A simple recipe over fixture DB returns at least 1 row."""
    r = Recipe.from_builtin(
        name="any",
        description="d",
        where="liquidity_usd > 1000",
        order_by="liquidity_usd DESC",
        limit=10,
    )
    df = run_recipe(tmp_db_with_seed, r)
    assert isinstance(df, pd.DataFrame)
    assert len(df) >= 1


def test_run_recipe_left_joins_question_zh(tmp_db_with_seed: Path) -> None:
    """Markets joined to question_translations expose question_zh column."""
    r = Recipe.from_builtin(
        name="any",
        description="d",
        where="market_id = 'M1'",
        order_by="liquidity_usd DESC",
        limit=10,
    )
    df = run_recipe(tmp_db_with_seed, r)
    assert "question_zh" in df.columns
    assert df.iloc[0]["question_zh"] == "X 会发生吗？"


def test_run_recipe_question_zh_null_when_missing(tmp_db_with_seed: Path) -> None:
    """Markets without translation get NULL question_zh, no crash."""
    r = Recipe.from_builtin(
        name="any",
        description="d",
        where="market_id = 'M2'",
        order_by="liquidity_usd DESC",
        limit=10,
    )
    df = run_recipe(tmp_db_with_seed, r)
    assert "question_zh" in df.columns
    # None (sqlite NULL) → pandas NaN
    assert pd.isna(df.iloc[0]["question_zh"])


def test_run_recipe_yaml_recipe_validated(tmp_db_with_seed: Path) -> None:
    """A yaml recipe (from_yaml → _is_trusted=False) with malicious WHERE
    is rejected before reaching SQL execution."""
    r = Recipe.from_yaml(
        "evil",
        {"where": "1=1; DROP TABLE markets", "order_by": "liquidity_usd"},
    )
    with pytest.raises(ValueError):
        run_recipe(tmp_db_with_seed, r)


def test_run_recipe_uses_readonly_uri(tmp_db_with_seed: Path) -> None:
    """Smoke: the recipe runs against the read-only URI without error."""
    r = BUILTIN_RECIPES["thick-but-slippery"]
    df = run_recipe(tmp_db_with_seed, r)
    assert isinstance(df, pd.DataFrame)


# =============================================================================
# run_recipe_grouped — Blocker #3 no-bypass invariant
# =============================================================================


def test_run_recipe_grouped_validates_where(tmp_db_with_seed: Path) -> None:
    """grouped path also validates where with recipe._is_trusted."""
    # Construct an untrusted (yaml-shaped) recipe with malicious where + group_by
    # via direct constructor (bypass from_yaml's group_by drop).
    r = Recipe(
        name="evil",
        description="d",
        where="1=1; DROP TABLE markets",
        order_by="liquidity_usd",
        limit=10,
        group_by="tag_label",
        _is_trusted=False,
    )
    with pytest.raises(ValueError):
        run_recipe_grouped(tmp_db_with_seed, r)


def test_run_recipe_grouped_validates_order_by(tmp_db_with_seed: Path) -> None:
    """grouped path also validates order_by with recipe._is_trusted."""
    r = Recipe(
        name="evil2",
        description="d",
        where="1=1",
        order_by="liquidity_usd * spread DESC",  # arithmetic → yaml strict reject
        limit=10,
        group_by="tag_label",
        _is_trusted=False,
    )
    with pytest.raises(ValueError):
        run_recipe_grouped(tmp_db_with_seed, r)


def test_run_recipe_grouped_by_tag_e2e(tmp_db_with_seed: Path) -> None:
    """by-tag builtin recipe runs against the seeded fixture and yields a row
    for the seeded 'Crypto' tag."""
    r = BUILTIN_RECIPES["by-tag"]
    df = run_recipe_grouped(tmp_db_with_seed, r)
    assert "tag_label" in df.columns
    assert "market_count" in df.columns
    assert "total_liq" in df.columns
    assert len(df) >= 1
    assert df.iloc[0]["tag_label"] == "Crypto"


def test_run_recipe_grouped_neg_risk_e2e(tmp_db_with_seed: Path) -> None:
    """neg-risk-incomplete uses the embedded HAVING tail. With single neg-risk
    market in fixture (sum_mid = 0.30, deviation = 0.70 > 0.02), it should
    appear in output."""
    r = BUILTIN_RECIPES["neg-risk-incomplete"]
    df = run_recipe_grouped(tmp_db_with_seed, r)
    # Single neg-risk group → 1 row
    assert len(df) >= 1
    assert "deviation" in df.columns


def test_run_recipe_grouped_unknown_template_raises(tmp_db_with_seed: Path) -> None:
    """Unknown group_by template (not 'tag_label' / not starts-with 'neg_risk')
    raises ValueError."""
    r = Recipe.from_builtin(
        name="weird",
        description="d",
        where="1=1",
        order_by="liquidity_usd DESC",
        group_by="some_random_col",
    )
    with pytest.raises(ValueError, match="unknown group_by template"):
        run_recipe_grouped(tmp_db_with_seed, r)


# =============================================================================
# load_yaml_recipes
# =============================================================================


def test_load_yaml_uses_safe_load_rejects_unsafe_tag(tmp_path: Path) -> None:
    """yaml.safe_load rejects unsafe Python-object tags. We use a benign tag
    (datetime via apply) that safe_load refuses; no command execution is
    triggered — yaml.YAMLError surfaces during the constructor lookup."""
    yaml_path = tmp_path / "recipes.yaml"
    # !!python/object/apply tag is rejected by safe_load before any constructor
    # is even resolved. Using a known-safe class name (datetime.datetime) makes
    # the test intent unambiguous: we are testing the SAFE-LOAD GATE itself,
    # not exercising any callable.
    yaml_path.write_text("recipes:\n  forbidden: !!python/object/apply:datetime.datetime [2024]\n")
    with pytest.raises(_yaml.YAMLError):
        load_yaml_recipes(yaml_path)


def test_load_yaml_returns_dict_of_recipes(tmp_path: Path) -> None:
    yaml_path = tmp_path / "recipes.yaml"
    yaml_path.write_text(
        "recipes:\n"
        "  my-watch:\n"
        "    description: my watchlist\n"
        "    where: liquidity_usd > 50000\n"
        "    order_by: liquidity_usd DESC\n"
        "    limit: 25\n"
    )
    result = load_yaml_recipes(yaml_path)
    assert "my-watch" in result
    r = result["my-watch"]
    assert r._is_trusted is False
    assert r.limit == 25
    assert r.where == "liquidity_usd > 50000"


def test_load_yaml_recipe_validated_at_load_time(tmp_path: Path) -> None:
    """Fail-fast: bad WHERE rejected at load, not at first execution."""
    yaml_path = tmp_path / "recipes.yaml"
    yaml_path.write_text(
        "recipes:\n  evil:\n    where: 1=1; DROP TABLE markets\n    order_by: liquidity_usd\n"
    )
    with pytest.raises(ValueError):
        load_yaml_recipes(yaml_path)


def test_load_yaml_missing_returns_empty(tmp_path: Path) -> None:
    """Missing yaml file is not an error — return {} (cli still runs builtins)."""
    result = load_yaml_recipes(tmp_path / "nonexistent.yaml")
    assert result == {}


def test_load_yaml_empty_returns_empty(tmp_path: Path) -> None:
    """Empty yaml file is not an error."""
    yaml_path = tmp_path / "empty.yaml"
    yaml_path.write_text("")
    result = load_yaml_recipes(yaml_path)
    assert result == {}


def test_load_yaml_recipe_has_is_trusted_false(tmp_path: Path) -> None:
    yaml_path = tmp_path / "recipes.yaml"
    yaml_path.write_text(
        "recipes:\n  r1:\n    where: liquidity_usd > 10\n    order_by: liquidity_usd\n"
    )
    result = load_yaml_recipes(yaml_path)
    assert result["r1"]._is_trusted is False


def test_load_yaml_drops_group_by_key(tmp_path: Path) -> None:
    """yaml with group_by key → key is dropped + warning logged."""
    yaml_path = tmp_path / "recipes.yaml"
    yaml_path.write_text(
        "recipes:\n"
        "  r1:\n"
        "    where: liquidity_usd > 10\n"
        "    order_by: liquidity_usd\n"
        "    group_by: tag_label\n"
    )
    result = load_yaml_recipes(yaml_path)
    assert result["r1"].group_by is None


def test_load_yaml_skips_non_dict_body(tmp_path: Path) -> None:
    """Defensive: yaml `recipes: { name: 'string-not-dict' }` → skip with warning."""
    yaml_path = tmp_path / "recipes.yaml"
    yaml_path.write_text(
        "recipes:\n"
        "  bad: just-a-string\n"
        "  good:\n"
        "    where: liquidity_usd > 1\n"
        "    order_by: liquidity_usd\n"
    )
    result = load_yaml_recipes(yaml_path)
    assert "bad" not in result
    assert "good" in result


# =============================================================================
# list_all_recipes (builtin + yaml merge)
# =============================================================================


def test_list_all_recipes_no_yaml() -> None:
    result = list_all_recipes(None)
    assert set(result) == set(BUILTIN_RECIPES)


def test_list_all_recipes_merges_yaml(tmp_path: Path) -> None:
    yaml_path = tmp_path / "recipes.yaml"
    yaml_path.write_text(
        "recipes:\n  user-only:\n    where: liquidity_usd > 1\n    order_by: liquidity_usd\n"
    )
    result = list_all_recipes(yaml_path)
    assert "user-only" in result
    assert result["user-only"]._is_trusted is False
    # builtins still present
    assert "thick-but-slippery" in result


def test_list_all_recipes_yaml_does_not_override_builtin(tmp_path: Path) -> None:
    yaml_path = tmp_path / "recipes.yaml"
    yaml_path.write_text(
        "recipes:\n"
        "  thick-but-slippery:\n"
        "    where: liquidity_usd > 1\n"
        "    order_by: liquidity_usd\n"
    )
    result = list_all_recipes(yaml_path)
    # Still the builtin (trusted=True)
    assert result["thick-but-slippery"]._is_trusted is True


# =============================================================================
# Module-level invariants — grep gates
# =============================================================================


def test_scanner_module_uses_safe_load_only() -> None:
    """The scanner module must use yaml.safe_load — never yaml.load (T-01.1-10)."""
    src = Path(__file__).parent.parent.parent / "src" / "polyarb" / "observation" / "scanner.py"
    content = src.read_text()
    # Strip line comments
    code_lines = [ln for ln in content.splitlines() if not ln.lstrip().startswith("#")]
    code = "\n".join(code_lines)
    assert "yaml.safe_load" in code
    assert "yaml.load(" not in code


def test_scanner_module_has_mode_ro() -> None:
    """Layer 1 invariant: module contains read-only URI string."""
    src = Path(__file__).parent.parent.parent / "src" / "polyarb" / "observation" / "scanner.py"
    assert "mode=ro" in src.read_text()


def test_scanner_grouped_path_validators_invariant() -> None:
    """Blocker #3: both run_recipe AND run_recipe_grouped call _validate_where
    + _validate_order_by → at least 3 occurrences each in scanner.py source
    (1 def + 2 call sites — run_recipe + run_recipe_grouped + load_yaml_recipes
    use them)."""
    src = Path(__file__).parent.parent.parent / "src" / "polyarb" / "observation" / "scanner.py"
    content = src.read_text()
    where_calls = content.count("_validate_where(")
    order_by_calls = content.count("_validate_order_by(")
    assert where_calls >= 3, f"_validate_where appearances: {where_calls}"
    assert order_by_calls >= 3, f"_validate_order_by appearances: {order_by_calls}"
