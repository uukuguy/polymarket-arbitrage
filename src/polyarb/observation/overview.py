"""Market overview dashboard — one-command whole-picture view.

Read-only SQLite queries. No writes. Designed for `make overview`.
"""

from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path


@dataclass
class OverviewData:
    snapshot_summary: dict = field(default_factory=dict)
    market_breakdown: dict = field(default_factory=dict)
    top_tags: list[dict] = field(default_factory=list)
    time_distribution: list[dict] = field(default_factory=list)
    top_movers: list[dict] = field(default_factory=list)
    translation_coverage: dict = field(default_factory=dict)
    active_alerts_count: int = 0


def _ro_connect(db_path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)


def _compute_status(is_valid: int, l1_issues: list[tuple[str, str]]) -> str:
    """Compute OK/DEGRADED/FAILED from is_valid and Layer 1 issues."""
    if not is_valid:
        for cat, detail in l1_issues:
            if cat == "api_unreachable":
                return "FAILED"
            if cat == "api_jitter":
                m = re.search(r"reported (\d+).*?fetched (\d+)", detail)
                if m:
                    reported = int(m.group(1))
                    fetched = int(m.group(2))
                    if reported > 0 and abs(reported - fetched) / reported <= 0.01:
                        return "DEGRADED"
        return "FAILED"
    for cat, detail in l1_issues:
        if cat == "api_jitter":
            m = re.search(r"reported (\d+).*?fetched (\d+)", detail)
            if m:
                reported = int(m.group(1))
                fetched = int(m.group(2))
                if reported > 0 and abs(reported - fetched) / reported <= 0.01:
                    return "DEGRADED"
    return "OK"


def query_snapshot_summary(db_path: Path) -> dict:
    """Latest snapshot: when taken, market count, status."""
    con = _ro_connect(db_path)
    try:
        row = con.execute(
            "SELECT id, taken_at_ms, finished_at_ms, mode, market_count, is_valid "
            "FROM snapshots WHERE market_count > 0 ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if not row:
            return {"error": "no valid snapshots found"}

        sid, taken_ms, finished_ms, mode, count, is_valid = row
        # Get L1 issues for status computation
        l1_rows = con.execute(
            "SELECT category, detail FROM validation_issues "
            "WHERE snapshot_id = ? AND layer = 1",
            (sid,),
        ).fetchall()
        status = _compute_status(is_valid, l1_rows)

        issue_counts = con.execute(
            "SELECT layer, COUNT(*) FROM validation_issues "
            "WHERE snapshot_id = ? GROUP BY layer",
            (sid,),
        ).fetchall()
        issue_summary = {f"L{layer}": cnt for layer, cnt in issue_counts}

        taken_dt = datetime.fromtimestamp(taken_ms / 1000, tz=timezone.utc)
        return {
            "snapshot_id": sid,
            "taken_at": taken_dt.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "mode": mode,
            "market_count": count,
            "status": status,
            "issues": issue_summary,
        }
    finally:
        con.close()


def query_market_breakdown(db_path: Path) -> dict:
    """Total markets, total liquidity, total volume."""
    con = _ro_connect(db_path)
    try:
        row = con.execute(
            "SELECT COUNT(*), COALESCE(SUM(liquidity_usd), 0), COALESCE(SUM(volume_usd), 0) "
            "FROM markets"
        ).fetchone()
        if not row or row[0] == 0:
            return {"total_markets": 0, "total_liquidity_usd": 0, "total_volume_usd": 0}
        return {
            "total_markets": row[0],
            "total_liquidity_usd": int(row[1]),
            "total_volume_usd": int(row[2]),
        }
    finally:
        con.close()


def query_top_tags(db_path: Path, limit: int = 10) -> list[dict]:
    """Top tags by market count and total liquidity."""
    con = _ro_connect(db_path)
    try:
        rows = con.execute(
            "SELECT et.tag_label, COUNT(DISTINCT m.market_id) AS n_markets, "
            "COALESCE(SUM(m.liquidity_usd), 0) AS total_liq "
            "FROM event_tags et "
            "JOIN markets m ON m.event_id = et.event_id "
            "WHERE et.tag_label IS NOT NULL "
            "GROUP BY et.tag_label "
            "ORDER BY n_markets DESC "
            f"LIMIT {int(limit)}"
        ).fetchall()
        return [
            {"tag": r[0], "markets": r[1], "liquidity_usd": int(r[2])} for r in rows
        ]
    finally:
        con.close()


def query_time_distribution(db_path: Path) -> list[dict]:
    """Counts by resolution window: <24h, 1-7d, 7-30d, 30d+, perpetual."""
    con = _ro_connect(db_path)
    try:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        day_ms = 24 * 60 * 60 * 1000
        buckets = [
            ("< 24h", f"end_time_ms > {now_ms} AND end_time_ms <= {now_ms + day_ms}"),
            ("1-7d", f"end_time_ms > {now_ms + day_ms} AND end_time_ms <= {now_ms + 7 * day_ms}"),
            ("7-30d", f"end_time_ms > {now_ms + 7 * day_ms} AND end_time_ms <= {now_ms + 30 * day_ms}"),
            ("30d+", f"end_time_ms > {now_ms + 30 * day_ms}"),
            ("perpetual", "end_time_ms IS NULL"),
            ("expired", f"end_time_ms <= {now_ms}"),
        ]
        result = []
        for label, where in buckets:
            cnt = con.execute(
                f"SELECT COUNT(*) FROM markets WHERE {where}"
            ).fetchone()[0]
            result.append({"bucket": label, "count": cnt})
        return result
    finally:
        con.close()


def query_top_movers(db_path: Path, parquet_root: Path, limit: int = 10) -> list[dict]:
    """Largest mid_price drift from N-1 to N snapshot."""
    from polyarb.observation.diff import latest_snapshot_pair, resolve_snapshot_path, compare_snapshots

    try:
        older_id, newer_id = latest_snapshot_pair(db_path)
    except ValueError:
        return []

    from_path = resolve_snapshot_path(older_id, db_path)
    to_path = resolve_snapshot_path(newer_id, db_path)

    df = compare_snapshots(from_path, to_path)
    if df.empty:
        return []

    # Filter to persistent markets with material drift
    df = df[df["state"] == "persistent"].copy()
    df["abs_drift"] = df["mid_drift"].apply(abs)  # type: ignore[arg-type]
    df = df.nlargest(limit, "abs_drift")  # type: ignore[call-overload]

    # Join with SQLite translations so Chinese questions show in the table
    slugs = [str(row["slug"]) for _, row in df.iterrows()]
    zh_map: dict[str, str] = {}
    if slugs:
        con = _ro_connect(db_path)
        try:
            placeholders = ",".join("?" for _ in slugs)
            rows = con.execute(
                "SELECT m.slug, qt.question_zh FROM markets m "
                "JOIN question_translations qt ON qt.question_en = m.question "
                f"WHERE m.slug IN ({placeholders}) AND qt.question_zh != ''",
                slugs,
            ).fetchall()
            zh_map = {r[0]: r[1] for r in rows}
        finally:
            con.close()

    result: list[dict] = []
    for _, row in df.iterrows():
        slug = str(row["slug"])
        zh = zh_map.get(slug, "")
        question = zh if zh else str(row.get("question", ""))[:80]
        result.append({
            "slug": slug,
            "question": question,
            "mid_from": float(row["mid_from"]),
            "mid_to": float(row["mid_to"]),
            "drift": float(row["mid_drift"]),
        })
    return result


def query_translation_coverage(db_path: Path) -> dict:
    """What % of markets have Chinese translations."""
    con = _ro_connect(db_path)
    try:
        total = con.execute("SELECT COUNT(*) FROM markets").fetchone()[0]
        if total == 0:
            return {"total_markets": 0, "translated": 0, "pct": 0}
        translated = con.execute(
            "SELECT COUNT(DISTINCT m.market_id) FROM markets m "
            "JOIN question_translations qt ON qt.question_en = m.question "
            "WHERE qt.question_zh != '' AND qt.is_dead = 0"
        ).fetchone()[0]
        return {
            "total_markets": total,
            "translated": translated,
            "pct": round(translated / total * 100, 1),
        }
    finally:
        con.close()


def build_overview(db_path: Path, parquet_root: Path) -> OverviewData:
    """Orchestrate all queries into a single OverviewData."""
    return OverviewData(
        snapshot_summary=query_snapshot_summary(db_path),
        market_breakdown=query_market_breakdown(db_path),
        top_tags=query_top_tags(db_path),
        time_distribution=query_time_distribution(db_path),
        top_movers=query_top_movers(db_path, parquet_root),
        translation_coverage=query_translation_coverage(db_path),
    )
