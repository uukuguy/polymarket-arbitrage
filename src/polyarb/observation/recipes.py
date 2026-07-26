"""Built-in scan recipes — Batch 1 (6 recipes from CONTEXT.md §T3).

Two categories:
- Row-level (4): thick-but-slippery, near-end, ghost-suspicious, coin-flip
                (run_recipe applies WHERE → ORDER → LIMIT)
- Group-level (2): neg-risk-incomplete, by-tag
                   (run_recipe_grouped builds GROUP BY query)

Trust model (Blocker #2 decision):
All builtins are constructed via Recipe.from_builtin(...) which sets
_is_trusted=True. Strict ORDER BY whitelist + WHERE blacklist are
skipped for builtins (because we authored them in source). Yaml-loaded
recipes use Recipe.from_yaml(...) → _is_trusted=False → full strict
validation.

Amendment 01 note (2026-05-02):
    The original CONTEXT.md plan listed `by-category` which assumed
    markets.category was a column. Wave 1 amendment showed Polymarket only
    surfaces tags via the /events endpoint, so the schema now stores tags in
    the event_tags table. The recipe was renamed to `by-tag` and its
    aggregation path joins markets → event_tags by event_id (see
    run_recipe_grouped in scanner.py).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Recipe:
    """Declarative scan recipe.

    Fields:
        name        — stable identifier (matches Makefile target / CLI arg)
        description — short Chinese 1-line description shown in `list-recipes`
        where       — SQL expression (used in WHERE clause; trust-split via _is_trusted)
        order_by    — SQL ORDER BY tail (column name(s); trust-split)
        limit       — int row cap; ALWAYS validated to [1, 10000] regardless of trust
        group_by    — when set, recipe goes through run_recipe_grouped (builtin-only)
        _is_trusted — TRUE for builtin recipes (bypass blacklist/whitelist),
                      FALSE for yaml-loaded recipes (strict validation)
    """

    name: str
    description: str
    where: str
    order_by: str
    limit: int = 50
    group_by: str | None = None
    _is_trusted: bool = False

    @classmethod
    def from_builtin(cls, **kw) -> Recipe:
        """Constructor for built-in recipes — sets _is_trusted=True automatically."""
        return cls(**{**kw, "_is_trusted": True})

    @classmethod
    def from_yaml(cls, name: str, body: dict) -> Recipe:
        """Constructor for user-supplied yaml recipes — _is_trusted=False (strict).

        Yaml MAY NOT set group_by — that key is silently dropped by load_yaml_recipes
        (with a logger.warning). Group_by templates require trusted SQL fragments
        (HAVING clauses with arithmetic) which the strict validator cannot accept.
        """
        return cls(
            name=name,
            description=body.get("description", ""),
            where=body["where"],
            order_by=body.get("order_by", "liquidity_usd DESC"),
            limit=int(body.get("limit", 50)),
            group_by=None,
            _is_trusted=False,
        )


# ─────────────────────────────────────────────────────────────────────────────
# BUILTIN_RECIPES — Batch 1, all _is_trusted=True via from_builtin().
# ─────────────────────────────────────────────────────────────────────────────

BUILTIN_RECIPES: dict[str, Recipe] = {
    "l3-seed": Recipe.from_builtin(
        name="l3-seed",
        description="L3 观察覆盖：流动性中间价市场（上限 100）",
        where=(
            "yes_token_id IS NOT NULL AND mid_price BETWEEN 0.1 AND 0.9 AND liquidity_usd >= 500"
        ),
        order_by="liquidity_usd DESC, market_id ASC",
        limit=100,
    ),
    "thick-but-slippery": Recipe.from_builtin(
        name="thick-but-slippery",
        description="陷阱市场：厚但价差大（liq>$100k, spread>$0.10）",
        where="liquidity_usd > 100000 AND (best_ask_price - best_bid_price) > 0.10",
        order_by="liquidity_usd * (best_ask_price - best_bid_price) DESC",
    ),
    "near-end": Recipe.from_builtin(
        name="near-end",
        description="即将结算（72h 内）— 套利窗口最密的市场",
        where=(
            "end_time_ms BETWEEN strftime('%s','now')*1000 "
            "AND strftime('%s','now','+72 hours')*1000 "
            "AND liquidity_usd > 1000"
        ),
        order_by="liquidity_usd DESC",
    ),
    "ghost-suspicious": Recipe.from_builtin(
        name="ghost-suspicious",
        # CONTEXT.md typo correction (PATTERNS §0 + §5.1):
        # Real ghost_book signal lives in validation_issues.category='ghost_book'
        # (Layer 4 cross-source check), NOT markets.incomplete=1 (Layer 2 missing-field).
        description="数据异常：CLOB 与 Gamma 交叉验证失败（ghost_book）",
        where=(
            "market_id IN ("
            "SELECT v.market_id FROM validation_issues v "
            "WHERE v.category = 'ghost_book') "
            "AND liquidity_usd > 10000"
        ),
        order_by="liquidity_usd DESC",
    ),
    "coin-flip": Recipe.from_builtin(
        name="coin-flip",
        description="高不确定性：mid 0.45-0.55 + 7 天内结算",
        where=(
            "mid_price BETWEEN 0.45 AND 0.55 "
            "AND end_time_ms < strftime('%s','now','+7 days')*1000 "
            "AND liquidity_usd > 5000"
        ),
        order_by="volume_usd DESC",
    ),
    "neg-risk-incomplete": Recipe.from_builtin(
        name="neg-risk-incomplete",
        # CONTEXT Open Question #2 decision: tolerance ±0.02
        description="neg-risk 组 mid 加和偏离 1.0 ±0.02（M2 套利直接信号）",
        where="neg_risk_market_id IS NOT NULL",
        order_by="ABS(SUM(mid_price) - 1.0) DESC",
        # group_by carries the HAVING tail too (builtin-only — yaml is forbidden
        # from setting group_by). The 0.02 tolerance is encoded here.
        group_by="neg_risk_market_id HAVING ABS(SUM(mid_price) - 1.0) > 0.02",
    ),
    "by-tag": Recipe.from_builtin(
        name="by-tag",
        # Phase 1.1 Amendment 01: replaces the original `by-category` recipe.
        # Aggregation joins markets → event_tags via event_id; see
        # run_recipe_grouped's group_by="tag_label" branch.
        description="标签统计：每个 event tag 的市场数 / 总 liq / 平均 spread",
        where="1=1",
        order_by="market_count DESC",
        group_by="tag_label",
    ),
}
