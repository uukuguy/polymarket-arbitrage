"""Single-market multi-source detail view (T5).

Composition: markets row + question_translations + neg-risk siblings +
5-snapshot recent history (via tracker.track_market).

Read-only SQLite + parameterized queries throughout.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from polyarb.observation.tracker import track_market


def _read_market(slug: str, db_path: Path) -> dict:
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    try:
        row = con.execute(
            """SELECT m.*, qt.question_zh
            FROM markets m
            LEFT JOIN question_translations qt ON qt.question_en = m.question
            WHERE m.slug = ?""",
            (slug,),
        ).fetchone()
    finally:
        con.close()
    if row is None:
        raise ValueError(f"market not found: {slug!r}")
    return dict(row)


def show_question_bilingual(row: dict) -> str:
    en = row.get("question") or "(no question)"
    zh = row.get("question_zh") or "(未翻译 — 跑 make translate-pending)"
    return f"EN: {en}\n中文: {zh}"


def _format_duration(ms: int) -> str:
    total_seconds = ms // 1000
    days, remainder = divmod(total_seconds, 86400)
    hours = remainder // 3600
    if days > 0:
        return f"{days} 天 {hours} 小时"
    return f"{hours} 小时"


def show_time_dimension(row: dict) -> str:
    end_ms = row.get("end_time_ms")
    if end_ms is None:
        return "无固定结算时间 (perpetual)"
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    if end_ms <= now_ms:
        return "已结算"
    remaining = end_ms - now_ms
    return f"距结算还有 {_format_duration(remaining)}"


def show_neg_risk_siblings(row: dict, db_path: Path) -> list[dict]:
    neg_id = row.get("neg_risk_market_id")
    slug = row.get("slug")
    if not neg_id:
        return []
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute(
            """SELECT m.slug, m.question, qt.question_zh,
                   m.mid_price, m.best_bid_price, m.best_ask_price,
                   m.liquidity_usd, m.volume_usd
            FROM markets m
            LEFT JOIN question_translations qt ON qt.question_en = m.question
            WHERE m.neg_risk_market_id = ? AND m.slug != ?
            ORDER BY m.slug""",
            (neg_id, slug),
        ).fetchall()
    finally:
        con.close()
    return [dict(r) for r in rows]


def show_recent_history(
    slug: str, parquet_root: Path, n: int = 5
) -> pd.DataFrame:
    df = track_market(slug, parquet_root)
    if df.empty:
        return df
    return df.tail(n)


def show_market(
    slug: str, db_path: Path, parquet_root: Path
) -> dict:
    """Orchestrate multi-source detail for a single market.

    Returns dict with keys: market, bilingual, time_dim, neg_risk_siblings,
    recent_history.
    """
    market = _read_market(slug, db_path)
    return {
        "market": market,
        "bilingual": show_question_bilingual(market),
        "time_dim": show_time_dimension(market),
        "neg_risk_siblings": show_neg_risk_siblings(market, db_path),
        "recent_history": show_recent_history(slug, parquet_root),
    }
