"""Watchlist management with safe expression evaluation (T7).

yaml.safe_load — rejects !!python/object (T-01.1-19).
alert_when expressions are parsed via ast.parse(mode='eval') and walked
node-by-node against a strict whitelist. Python builtins eval()/exec()
are NOT used at any point. (T-01.1-20)

Whitelist nodes: Expression, Constant(numeric only), Name(in ALLOWED_VARS),
UnaryOp(Not/USub), BinOp(+ - * /), Compare(< <= > >= == !=), BoolOp(And/Or).
Forbidden: Call, Attribute, Subscript, Lambda, ListComp, DictComp, SetComp,
GeneratorExp, Starred, anything else.

Warning #9: if any variable referenced by alert_when resolves to None in
the market row, evaluate_alert returns False + logs warning (skip, not throw).
"""

from __future__ import annotations

import ast
import operator
from dataclasses import dataclass
from pathlib import Path
from sqlite3 import connect

import yaml
from loguru import logger

_MAX_EXPR_LEN = 200

_ALLOWED_AST_NODES = frozenset(
    {
        ast.Expression,
        ast.Constant,
        ast.Name,
        ast.UnaryOp,
        ast.BinOp,
        ast.Compare,
        ast.BoolOp,
    }
)

ALLOWED_VARS: dict[str, str] = {
    "mid": "mid_price",
    "bid": "best_bid_price",
    "ask": "best_ask_price",
    "spread": "spread",
    "liq": "liquidity_usd",
    "vol": "volume_usd",
    "mid_price": "mid_price",
    "best_bid_price": "best_bid_price",
    "best_ask_price": "best_ask_price",
    "liquidity_usd": "liquidity_usd",
    "volume_usd": "volume_usd",
}

_BIN_OPS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
}

_CMP_OPS = {
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
}


@dataclass(frozen=True)
class WatchlistEntry:
    slug: str
    reason: str
    alert_when: str | None
    added: str


def _collect_names(node: ast.AST) -> set[str]:
    names: set[str] = set()
    for n in ast.walk(node):
        if isinstance(n, ast.Name):
            names.add(n.id)
    return names


def _eval_node(node: ast.AST, env: dict[str, float]) -> float | bool:
    t = type(node)
    if t not in _ALLOWED_AST_NODES:
        raise ValueError(f"AST node {t.__name__} not allowed")

    if t is ast.Expression:
        return _eval_node(node.body, env)  # type: ignore[attr-defined]
    if t is ast.Constant:
        if not isinstance(node.value, (int, float)):  # type: ignore[attr-defined]
            raise ValueError("only numeric constants allowed")
        return node.value  # type: ignore[attr-defined]
    if t is ast.Name:
        if node.id not in ALLOWED_VARS:  # type: ignore[attr-defined]
            raise ValueError(f"unknown variable: {node.id!r}")  # type: ignore[attr-defined]
        return env[node.id]  # type: ignore[attr-defined]
    if t is ast.UnaryOp:
        op = node.op  # type: ignore[attr-defined]
        if isinstance(op, ast.USub):
            return -_eval_node(node.operand, env)  # type: ignore[attr-defined]
        if isinstance(op, ast.Not):
            return not _eval_node(node.operand, env)  # type: ignore[attr-defined]
        raise ValueError(f"unary op {type(op).__name__} not allowed")
    if t is ast.BinOp:
        op = node.op  # type: ignore[attr-defined]
        if type(op) not in _BIN_OPS:
            raise ValueError(f"binary op {type(op).__name__} not allowed")
        left = _eval_node(node.left, env)  # type: ignore[attr-defined]
        right = _eval_node(node.right, env)  # type: ignore[attr-defined]
        return _BIN_OPS[type(op)](left, right)
    if t is ast.Compare:
        left = _eval_node(node.left, env)  # type: ignore[attr-defined]
        for op, comp in zip(node.ops, node.comparators):  # type: ignore[attr-defined]
            if type(op) not in _CMP_OPS:
                raise ValueError(f"cmp op {type(op).__name__} not allowed")
            right = _eval_node(comp, env)
            if not _CMP_OPS[type(op)](left, right):
                return False
            left = right
        return True
    if t is ast.BoolOp:
        if isinstance(node.op, ast.And):  # type: ignore[attr-defined]
            for v in node.values:  # type: ignore[attr-defined]
                if not _eval_node(v, env):
                    return False
            return True
        if isinstance(node.op, ast.Or):  # type: ignore[attr-defined]
            for v in node.values:  # type: ignore[attr-defined]
                if _eval_node(v, env):
                    return True
            return False
        raise ValueError(f"bool op {type(node.op).__name__} not allowed")  # type: ignore[attr-defined]
    raise ValueError(f"AST node {t.__name__} not allowed")


def evaluate_alert(expression: str, market_row: dict) -> bool:
    if len(expression) > _MAX_EXPR_LEN:
        raise ValueError(f"expression too long: {len(expression)} > {_MAX_EXPR_LEN}")

    tree = ast.parse(expression, mode="eval")
    refd = _collect_names(tree)
    unknown = refd - set(ALLOWED_VARS)
    if unknown:
        raise ValueError(f"unknown variable(s): {sorted(unknown)}")

    env: dict[str, float] = {}
    for name in refd:
        col = ALLOWED_VARS[name]
        if col == "spread":
            bid = market_row.get("best_bid_price")
            ask = market_row.get("best_ask_price")
            if bid is None or ask is None:
                logger.warning(
                    f"evaluate_alert skipped: spread requires bid and ask "
                    f"(bid={bid!r}, ask={ask!r})"
                )
                return False
            env["spread"] = float(ask) - float(bid)
        else:
            val = market_row.get(col)
            if val is None:
                logger.warning(f"evaluate_alert skipped: {col} ({name}) is None")
                return False
            env[name] = float(val)

    return bool(_eval_node(tree, env))


def load_watchlist(yaml_path: Path) -> list[WatchlistEntry]:
    if not yaml_path.exists():
        return []
    data = yaml.safe_load(yaml_path.read_text()) or []
    if not isinstance(data, list):
        raise ValueError("watchlist yaml must be a list of entries")
    dummy = {
        "mid_price": 0.5,
        "best_bid_price": 0.4,
        "best_ask_price": 0.5,
        "liquidity_usd": 1000.0,
        "volume_usd": 100.0,
    }
    entries: list[WatchlistEntry] = []
    for i, d in enumerate(data):
        alert = d.get("alert_when")
        if alert is not None:
            try:
                evaluate_alert(alert, dummy)
            except (ValueError, SyntaxError) as e:
                logger.warning(
                    f"watchlist entry {i} ({d.get('slug', '?')}): "
                    f"invalid alert_when {alert!r} — {e}; disabling alert"
                )
                alert = None
        entries.append(
            WatchlistEntry(
                slug=d["slug"],
                reason=d.get("reason", ""),
                alert_when=alert,
                added=d.get("added", ""),
            )
        )
    return entries


def check_alerts(
    watchlist: list[WatchlistEntry], db_path: Path
) -> list[tuple[WatchlistEntry, dict]]:
    con = connect(f"file:{db_path}?mode=ro", uri=True)
    con.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
    try:
        triggered: list[tuple[WatchlistEntry, dict]] = []
        for entry in watchlist:
            if entry.alert_when is None:
                continue
            row = con.execute(
                "SELECT * FROM markets WHERE slug = ?",
                (entry.slug,),
            ).fetchone()
            if row is None:
                logger.warning(f"watchlist entry {entry.slug}: not in db, skipped")
                continue
            try:
                if evaluate_alert(entry.alert_when, row):
                    triggered.append((entry, row))
            except ValueError as e:
                logger.warning(f"watchlist entry {entry.slug}: eval error — {e}")
    finally:
        con.close()
    return triggered
