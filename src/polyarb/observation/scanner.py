"""Recipe scanner — 4-layer defense against malicious yaml.

Layer 1: read-only sqlite URI (mode=ro) — engine rejects DDL/DML/ATTACH
Layer 2: _FORBIDDEN regex char-level rejection — friendlier error
Layer 3: _ORDER_BY_OK regex whitelist — block subquery / function calls
Layer 4: _validate_limit int range [1, 10000]

═══ TRUST SPLIT INVARIANT ═══════════════════════════════════════════════
All validators take a ``trusted: bool`` keyword:
- trusted=True (BUILTIN path)  → bypass blacklist + whitelist (Layers 2/3)
                                  because BUILTIN_RECIPES is git-controlled
                                  source code; we authored it.
- trusted=False (YAML path)    → enforce ALL layers strictly. Yaml comes
                                  from forums / AI / external sources.
Layer 1 (read-only URI) and Layer 4 (limit cap) apply to BOTH paths
— these are ENGINE-level / RESOURCE-level guards, not text validators.

═══ GROUPED-PATH NO-BYPASS INVARIANT ═════════════════════════════════════
``run_recipe_grouped`` MUST call ``_validate_where`` + ``_validate_order_by``
with the recipe's ``_is_trusted`` flag, exactly like ``run_recipe``. Yaml
recipes are forbidden from setting ``group_by`` (load_yaml_recipes drops
the key); this invariant is the second line of defense if a future change
ever allows yaml group_by — even then, the strict validator still runs.

Threat model: defense-in-depth against users copy-pasting yaml from
forums/AI tools. Not against the user's own intent (they own their DB).
See RESEARCH.md §2.1-2.4 for the full threat analysis.

Amendment 01 note: the by-tag grouped path joins
``markets m`` → ``events e`` (via m.event_id = e.id) → ``event_tags et``
(via et.event_id = e.id). markets.category was deleted by amendment 01;
event_tags.tag_label is the new tag surface.
"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pandas as pd
import yaml
from loguru import logger

from polyarb.observation.recipes import BUILTIN_RECIPES, Recipe


# ─────────────────────────────────────────────────────────────────────────────
# Layer 2 — character-level blacklist (yaml path only; bypassed for trusted)
# ─────────────────────────────────────────────────────────────────────────────
# SELECT is included so yaml subqueries (`market_id IN (SELECT ...)`) are also
# rejected on the strict path. Builtin recipes (e.g. ghost-suspicious) that
# legitimately use a SELECT subquery go through the trusted=True bypass.
_FORBIDDEN = re.compile(
    r"(;|--|/\*|\bDROP\b|\bDELETE\b|\bUPDATE\b|\bINSERT\b"
    r"|\bALTER\b|\bCREATE\b|\bATTACH\b|\bDETACH\b|\bPRAGMA\b"
    r"|\bUNION\b|\bTRUNCATE\b|\bVACUUM\b|\bREINDEX\b|\bSELECT\b)",
    re.IGNORECASE,
)

# ─────────────────────────────────────────────────────────────────────────────
# Layer 3 — ORDER BY whitelist (yaml path only; bypassed for trusted)
# ─────────────────────────────────────────────────────────────────────────────
# Accepts: bare column ("liquidity_usd"), column + ASC/DESC, comma-separated
#          ("category, volume_usd DESC")
# Rejects: arithmetic ("liq * spread"), parens ("(SELECT 1)"), function calls,
#          string literals.
# Greedy + (not lazy +?) — matches consecutive identifier chars correctly.
_ORDER_BY_OK = re.compile(
    r"^[\w_]+(\s+(ASC|DESC))?(\s*,\s*[\w_]+(\s+(ASC|DESC))?)*$",
    re.IGNORECASE,
)


# ─────────────────────────────────────────────────────────────────────────────
# Validators (trusted-aware)
# ─────────────────────────────────────────────────────────────────────────────


def _validate_where(where: str, *, trusted: bool) -> None:
    """Validate WHERE clause string.

    trusted=True (builtin): no-op (caller authored this in source).
    trusted=False (yaml): strict ``_FORBIDDEN`` blacklist.
    """
    if trusted:
        return
    if _FORBIDDEN.search(where):
        raise ValueError(
            f"forbidden token in where: {where!r}. "
            f"Yaml recipes must not contain DDL/DML keywords, semicolons, "
            f"comments, UNION, or SELECT subqueries."
        )


def _validate_order_by(order_by: str, *, trusted: bool) -> None:
    """Validate ORDER BY clause string.

    trusted=True (builtin): no-op (allows arithmetic / functions / parens).
    trusted=False (yaml): _FORBIDDEN + _ORDER_BY_OK strict whitelist
                          (only bare columns + optional ASC/DESC).
    """
    if trusted:
        return
    if _FORBIDDEN.search(order_by):
        raise ValueError(f"forbidden token in order_by: {order_by!r}")
    if not _ORDER_BY_OK.match(order_by.strip()):
        raise ValueError(
            f"order_by must be column(s) with optional ASC/DESC. "
            f"Got: {order_by!r}. (Yaml recipes cannot use expressions; "
            f"file an issue if a builtin pattern would help you.)"
        )


def _validate_limit(limit: int) -> int:
    """Validate LIMIT — int in [1, 10000]. No trust concept (DoS guard universal).

    Rejects bool because bool is a subclass of int in Python.
    """
    if isinstance(limit, bool) or not isinstance(limit, int):
        raise ValueError(f"limit must be int, got {type(limit).__name__}")
    if limit < 1 or limit > 10000:
        raise ValueError(f"limit must be in [1, 10000], got {limit}")
    return limit


# ─────────────────────────────────────────────────────────────────────────────
# Recipe execution — read-only SQLite URI for engine-level Layer 1 defense
# ─────────────────────────────────────────────────────────────────────────────


def run_recipe(db_path: Path, recipe: Recipe) -> pd.DataFrame:
    """Execute a row-level recipe, returning a DataFrame with question_zh joined.

    Layer-1 (engine read-only): ``mode=ro`` URI rejects all writes.
    Layer-2/3: ``_validate_where`` / ``_validate_order_by`` (trusted-aware).
    Layer-4: ``_validate_limit`` (universal).
    """
    _validate_where(recipe.where, trusted=recipe._is_trusted)
    _validate_order_by(recipe.order_by, trusted=recipe._is_trusted)
    limit = _validate_limit(recipe.limit)

    uri = f"file:{db_path}?mode=ro"
    con = sqlite3.connect(uri, uri=True)
    try:
        # LEFT JOIN question_translations on literal question text (Open Question
        # #1 path c — hash is PK only; SQLite has no sha256 builtin).
        sql = (
            "SELECT m.*, qt.question_zh "
            "FROM markets m "
            "LEFT JOIN question_translations qt ON qt.question_en = m.question "
            f"WHERE ({recipe.where}) "
            f"ORDER BY {recipe.order_by} "
            f"LIMIT {limit}"
        )
        return pd.read_sql_query(sql, con)
    finally:
        con.close()


def run_recipe_grouped(db_path: Path, recipe: Recipe) -> pd.DataFrame:
    """GROUP BY recipe runner.

    Blocker #3 invariant: still validates where/order_by with recipe._is_trusted,
    no bypass. Although yaml-禁 group_by today, defense-in-depth: if future
    change allows yaml group_by, the strict validator still applies.

    Two builtin templates supported:
    - group_by="tag_label"           → by-tag (Amendment 01)
    - group_by="neg_risk_market_id…" → neg-risk-incomplete
    """
    _validate_where(recipe.where, trusted=recipe._is_trusted)
    _validate_order_by(recipe.order_by, trusted=recipe._is_trusted)
    limit = _validate_limit(recipe.limit)

    uri = f"file:{db_path}?mode=ro"
    con = sqlite3.connect(uri, uri=True)
    try:
        if recipe.group_by == "tag_label":
            # Amendment 01: by-tag joins via event_id.
            # Latest snapshot only (avoid double-counting across snapshots).
            sql = (
                "SELECT et.tag_label, "
                "COUNT(DISTINCT m.market_id) AS market_count, "
                "SUM(m.liquidity_usd) AS total_liq, "
                "AVG(m.best_ask_price - m.best_bid_price) AS avg_spread "
                "FROM markets m "
                "INNER JOIN event_tags et "
                "  ON et.event_id = m.event_id "
                "  AND et.snapshot_id = m.snapshot_id "
                "WHERE m.snapshot_id = (SELECT MAX(id) FROM snapshots) "
                "GROUP BY et.tag_label "
                f"ORDER BY {recipe.order_by} "
                f"LIMIT {limit}"
            )
        elif recipe.group_by and recipe.group_by.startswith("neg_risk_market_id"):
            # The recipe.group_by string carries an embedded HAVING tail (builtin
            # only — yaml is forbidden from setting group_by).
            sql = (
                "SELECT neg_risk_market_id, "
                "GROUP_CONCAT(slug) AS slugs, "
                "COUNT(*) AS leg_count, "
                "SUM(mid_price) AS sum_mid, "
                "ABS(SUM(mid_price) - 1.0) AS deviation "
                "FROM markets WHERE neg_risk_market_id IS NOT NULL "
                f"GROUP BY {recipe.group_by} "
                f"ORDER BY {recipe.order_by} "
                f"LIMIT {limit}"
            )
        else:
            raise ValueError(f"unknown group_by template: {recipe.group_by!r}")
        return pd.read_sql_query(sql, con)
    finally:
        con.close()


# ─────────────────────────────────────────────────────────────────────────────
# YAML loading + builtin/yaml merge
# ─────────────────────────────────────────────────────────────────────────────


def load_yaml_recipes(yaml_path: Path) -> dict[str, Recipe]:
    """Load user recipes from yaml. fail-fast strict validation at load time.

    - Uses ``yaml.safe_load`` (T-01.1-10 mitigation: no arbitrary Python objects).
    - Drops ``group_by`` keys if present (yaml-禁 — builtin-only).
    - Each recipe's where/order_by/limit is validated immediately so misconfig
      surfaces at startup, not in the middle of an execution.
    """
    if not yaml_path.exists():
        return {}
    with yaml_path.open() as f:
        data = yaml.safe_load(f) or {}
    recipes_raw = data.get("recipes") or {}
    result: dict[str, Recipe] = {}
    for name, body in recipes_raw.items():
        if not isinstance(body, dict):
            logger.warning(f"yaml recipe {name!r}: body is not a dict, skipping")
            continue
        if "group_by" in body:
            logger.warning(
                f"yaml recipe {name!r}: group_by is builtin-only, dropping key"
            )
            body = {k: v for k, v in body.items() if k != "group_by"}
        r = Recipe.from_yaml(name, body)
        # fail-fast strict validation (Layer 2/3/4 — Layer 1 only at execution time).
        _validate_where(r.where, trusted=False)
        _validate_order_by(r.order_by, trusted=False)
        _validate_limit(r.limit)
        result[name] = r
    return result


def list_all_recipes(yaml_path: Path | None = None) -> dict[str, Recipe]:
    """Merge BUILTIN_RECIPES with user yaml recipes.

    A yaml recipe with the same name as a builtin is dropped + warned —
    user must rename. This protects against malicious yaml replacing
    e.g. ``thick-but-slippery`` with a DROP-laden where clause that
    would then be granted trusted=True implicitly via the name match.
    (Note: Recipe.from_yaml always sets _is_trusted=False, so this is
    belt-and-suspenders, but the warning is still useful UX.)
    """
    merged = dict(BUILTIN_RECIPES)
    if yaml_path is not None:
        for name, r in load_yaml_recipes(yaml_path).items():
            if name in BUILTIN_RECIPES:
                logger.warning(
                    f"yaml recipe {name!r} cannot override builtin, ignoring"
                )
                continue
            merged[name] = r
    return merged
