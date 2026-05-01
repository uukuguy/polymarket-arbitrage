"""Gamma raw market dict → storage row dict.

Per Pitfall 2 of RESEARCH.md, Gamma's `/markets` endpoint returns ``clobTokenIds``
and ``outcomePrices`` as **JSON-encoded strings**, not Python lists. We
``json.loads`` them here.

Per Pitfall 3, token IDs MUST stay as ``str`` — Polymarket's uint256 token IDs
have 70+ decimal digits and overflow ``int64``.

F-8 SECURITY (timezone): ``endDate`` is parsed via ``datetime.fromisoformat``
after replacing trailing ``Z`` with ``+00:00``. If the parsed datetime is
naive (no tzinfo), we **treat it as UTC** rather than as local time — Polymarket
markets are UTC-rooted and ambiguous local interpretation would shift end_time_ms
by hours depending on the host clock.

Output contract:
    A dict whose keys are a SUPERSET of ``MARKETS_COLUMN_ORDER`` MINUS ``snapshot_id``.
    The CLOB-derived columns (``best_*``, ``fetched_at_ms``) are present as ``None``
    placeholders; the orchestrator overwrites them after CLOB fetch completes.
    Returns ``None`` if the raw dict is unrecoverable (no ``id`` key).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from loguru import logger


def _parse_json_list(raw: Any) -> list[Any]:
    """Best-effort decode of a JSON-string-encoded list. Empty list on any failure."""
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            decoded = json.loads(raw)
            return decoded if isinstance(decoded, list) else []
        except (json.JSONDecodeError, TypeError):
            return []
    return []


def _safe_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _parse_end_time_ms(raw: Any) -> int | None:
    """ISO-8601 (with ``Z`` or ``+00:00``) → epoch ms (UTC).

    Per F-8: naive datetimes are treated as UTC, not local time.
    """
    if not raw or not isinstance(raw, str):
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    try:
        return int(dt.timestamp() * 1000)
    except (OverflowError, OSError, ValueError):
        return None


def normalize_market(raw: dict) -> dict | None:
    """Convert a Gamma ``/markets`` raw response item to a storage row dict.

    Returns ``None`` if ``market_id`` (Gamma's ``id``) is missing — those rows
    are unrecoverable for storage (the markets table requires market_id PK).

    The returned dict has every key from ``MARKETS_COLUMN_ORDER`` except
    ``snapshot_id`` (the SQLiteStore injects that). CLOB-derived columns
    (``best_bid_price``, ``best_bid_size``, ``best_ask_price``, ``best_ask_size``,
    ``fetched_at_ms``) are ``None`` placeholders and the orchestrator overwrites
    them after the CLOB fetch completes.
    """
    market_id_raw = raw.get("id")
    if market_id_raw is None or market_id_raw == "":
        logger.warning(f"normalize_market: missing 'id' in raw payload (slug={raw.get('slug')})")
        return None

    # ── token IDs (Pitfall 2 + Pitfall 3) ────────────────────────────────────
    token_list = _parse_json_list(raw.get("clobTokenIds"))
    yes_token_id = str(token_list[0]) if len(token_list) > 0 else None
    no_token_id = str(token_list[1]) if len(token_list) > 1 else None

    # ── outcome prices (Pitfall 2) → mid_price = price[0] (YES side) ─────────
    price_list = _parse_json_list(raw.get("outcomePrices"))
    mid_price = _safe_float(price_list[0]) if len(price_list) > 0 else None

    # ── liquidity / volume: prefer numeric *Num field, fall back to str ──────
    liq_raw = raw.get("liquidityNum")
    if liq_raw is None:
        liq_raw = raw.get("liquidity")
    liquidity_usd = _safe_float(liq_raw)

    vol_raw = raw.get("volumeNum")
    if vol_raw is None:
        vol_raw = raw.get("volume")
    volume_usd = _safe_float(vol_raw)

    # ── endDate ISO → epoch ms (F-8 UTC handling) ─────────────────────────────
    end_iso = raw.get("endDate") or raw.get("end_date_iso")
    end_time_ms = _parse_end_time_ms(end_iso)

    return {
        "market_id": str(market_id_raw),
        "condition_id": str(raw.get("conditionId") or ""),
        "slug": raw.get("slug"),
        "question": raw.get("question"),
        "yes_token_id": yes_token_id,
        "no_token_id": no_token_id,
        "mid_price": mid_price,
        "liquidity_usd": liquidity_usd,
        "volume_usd": volume_usd,
        # Filled by orchestrator after CLOB fetch completes:
        "best_bid_price": None,
        "best_bid_size": None,
        "best_ask_price": None,
        "best_ask_size": None,
        "end_time_ms": end_time_ms,
        "active": bool(raw.get("active", False)),
        "closed": bool(raw.get("closed", False)),
        "neg_risk": bool(raw.get("negRisk", False)),
        "neg_risk_market_id": raw.get("negRiskMarketID"),
        # Stamped by orchestrator at CLOB-fetch completion (Pitfall 6):
        "fetched_at_ms": None,
        "incomplete": False,
        # Phase 1.1 T1 — Gamma already returns these; previous Phase 1 dropped them.
        # category is a single string; tags is a list[str] serialized as JSON.
        # ensure_ascii=False keeps CJK readable; T-01.1-01 tampering risk accepted
        # (we don't parse/render here — just store).
        "category": raw.get("category"),
        "tags": json.dumps(raw.get("tags") or [], ensure_ascii=False),
    }
