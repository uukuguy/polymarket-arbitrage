"""Tests for polyarb.observation.recipes — BUILTIN_RECIPES + Recipe trust split.

Plan 03 Task 1 — covers:
- Recipe dataclass _is_trusted invariant (from_builtin=True / from_yaml=False)
- BUILTIN_RECIPES 6 entries, all _is_trusted=True
- ghost-suspicious 笔误纠正（走 validation_issues, 不走 incomplete=1）
- neg-risk-incomplete 0.02 容差体现在 group_by 字段
- by-tag 替代 by-category（amendment 01）
"""
from __future__ import annotations

import pytest

from polyarb.observation.recipes import BUILTIN_RECIPES, Recipe


# =============================================================================
# Recipe dataclass — trust split
# =============================================================================


def test_recipe_default_is_not_trusted() -> None:
    """Defensive default: a Recipe constructed without specifying trust is untrusted."""
    r = Recipe(
        name="x", description="d", where="liq > 1", order_by="liq DESC"
    )
    assert r._is_trusted is False


def test_recipe_from_builtin_sets_trusted_true() -> None:
    r = Recipe.from_builtin(
        name="x", description="d", where="liq > 1", order_by="liq DESC"
    )
    assert r._is_trusted is True


def test_recipe_from_yaml_sets_trusted_false() -> None:
    r = Recipe.from_yaml("x", {"where": "liq > 1", "order_by": "liq DESC"})
    assert r._is_trusted is False


def test_recipe_from_yaml_default_order_by() -> None:
    r = Recipe.from_yaml("x", {"where": "liq > 1"})
    assert r.order_by == "liquidity_usd DESC"


def test_recipe_from_yaml_default_limit() -> None:
    r = Recipe.from_yaml("x", {"where": "liq > 1"})
    assert r.limit == 50


def test_recipe_from_yaml_drops_group_by() -> None:
    """from_yaml itself does not enforce drop — load_yaml_recipes does. But
    from_yaml MUST always set group_by=None regardless of body content (the
    invariant is yaml never has group_by)."""
    r = Recipe.from_yaml(
        "x", {"where": "liq > 1", "group_by": "category"}
    )
    assert r.group_by is None


def test_recipe_from_yaml_required_where() -> None:
    with pytest.raises(KeyError):
        Recipe.from_yaml("x", {})  # no 'where' key


def test_recipe_is_frozen() -> None:
    r = BUILTIN_RECIPES["thick-but-slippery"]
    with pytest.raises((AttributeError, Exception)):
        r.name = "mutated"  # type: ignore[misc]


# =============================================================================
# BUILTIN_RECIPES — 6 entries, all trusted
# =============================================================================


def test_builtin_recipes_count_is_6() -> None:
    assert len(BUILTIN_RECIPES) == 6, list(BUILTIN_RECIPES)


def test_builtin_recipes_names() -> None:
    expected = {
        "thick-but-slippery",
        "near-end",
        "ghost-suspicious",
        "coin-flip",
        "neg-risk-incomplete",
        "by-tag",  # Amendment 01: replaces by-category
    }
    assert set(BUILTIN_RECIPES) == expected


def test_all_builtins_have_trusted_flag() -> None:
    """Blocker #2 invariant: every builtin recipe is trusted."""
    for name, r in BUILTIN_RECIPES.items():
        assert r._is_trusted is True, f"{name} should be trusted"


# =============================================================================
# Per-recipe content asserts
# =============================================================================


def test_thick_but_slippery_recipe_valid() -> None:
    r = BUILTIN_RECIPES["thick-but-slippery"]
    assert r._is_trusted is True
    # Uses arithmetic in ORDER BY (only allowed for trusted)
    assert "liquidity_usd * (best_ask_price - best_bid_price)" in r.order_by
    assert "100000" in r.where


def test_near_end_recipe_uses_strftime() -> None:
    r = BUILTIN_RECIPES["near-end"]
    assert r._is_trusted is True
    assert "strftime" in r.where
    assert "+72 hours" in r.where


def test_ghost_suspicious_uses_validation_issues() -> None:
    """笔误纠正：CONTEXT.md 写 'incomplete = 1'，但实际数据在 validation_issues
    表的 category='ghost_book' 行。"""
    r = BUILTIN_RECIPES["ghost-suspicious"]
    assert r._is_trusted is True
    assert "validation_issues" in r.where
    assert "ghost_book" in r.where
    # Must NOT confuse with the Layer 2 incomplete signal
    assert "incomplete = 1" not in r.where


def test_coin_flip_recipe_valid() -> None:
    r = BUILTIN_RECIPES["coin-flip"]
    assert r._is_trusted is True
    assert "0.45 AND 0.55" in r.where
    assert "+7 days" in r.where


def test_neg_risk_incomplete_uses_group_by() -> None:
    r = BUILTIN_RECIPES["neg-risk-incomplete"]
    assert r._is_trusted is True
    assert r.group_by is not None
    assert r.group_by.startswith("neg_risk_market_id")


def test_neg_risk_tolerance_002() -> None:
    """CONTEXT Open Question #2 decision: ±0.02 tolerance."""
    r = BUILTIN_RECIPES["neg-risk-incomplete"]
    assert "0.02" in r.group_by


def test_by_tag_uses_group_by_tag_label() -> None:
    """Amendment 01: by-tag (not by-category) groups on event_tags.tag_label."""
    r = BUILTIN_RECIPES["by-tag"]
    assert r._is_trusted is True
    assert r.group_by == "tag_label"


def test_by_category_was_removed() -> None:
    """Amendment 01: the original by-category recipe no longer exists."""
    assert "by-category" not in BUILTIN_RECIPES
