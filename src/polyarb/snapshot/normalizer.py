"""Gamma raw market/event dict → storage row dict.

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

Phase 1.1 Amendment 01:
    Gamma `/markets` does NOT return category or tags (verified live 2026-05-02).
    Those fields only live on `/events`. ``normalize_market`` therefore no longer
    extracts category/tags. Instead it accepts an optional
    ``market_to_event_map: dict[market_id → event_id]`` and writes ``event_id`` on
    each market row. ``normalize_events`` builds that map (and the events /
    event_tags storage rows) from the /events endpoint's response.

Output contract (markets):
    A dict whose keys are a SUPERSET of ``MARKETS_COLUMN_ORDER`` MINUS ``snapshot_id``.
    The CLOB-derived columns (``best_*``, ``fetched_at_ms``) are present as ``None``
    placeholders; the orchestrator overwrites them after CLOB fetch completes.
    Returns ``None`` if the raw dict is unrecoverable (no ``id`` key).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
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
        dt = dt.replace(tzinfo=UTC)
    try:
        return int(dt.timestamp() * 1000)
    except (OverflowError, OSError, ValueError):
        return None


def normalize_market(raw: dict, market_to_event_map: dict[str, str] | None = None) -> dict | None:
    """Convert a Gamma ``/markets`` raw response item to a storage row dict.

    Returns ``None`` if ``market_id`` (Gamma's ``id``) is missing — those rows
    are unrecoverable for storage (the markets table requires market_id PK).

    The returned dict has every key from ``MARKETS_COLUMN_ORDER`` except
    ``snapshot_id`` (the SQLiteStore injects that). CLOB-derived columns
    (``best_bid_price``, ``best_bid_size``, ``best_ask_price``, ``best_ask_size``,
    ``fetched_at_ms``) are ``None`` placeholders and the orchestrator overwrites
    them after the CLOB fetch completes.

    Phase 1.1 Amendment 01: ``market_to_event_map`` (default empty) maps
    market_id → event_id; if unavailable for a given market, ``event_id`` is
    None (acceptable — orphan markets exist when /events doesn't list them).
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

    # ── Phase 1.1 Amendment 01: event_id from reverse lookup ──────────────────
    market_id_str = str(market_id_raw)
    event_id: str | None = None
    if market_to_event_map is not None:
        event_id = market_to_event_map.get(market_id_str)

    return {
        "market_id": market_id_str,
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
        # Phase 02: stage stamp filled by orchestrator stage 5; see schemas.py for semantic note
        "fetched_at_ms": None,
        # Phase 02 Plan 01: per-page real fetch time from _page_fetched_at_ms private key
        # injected by GammaClient._paginate(). None for rows from pre-02 snapshots.
        "page_fetched_at_ms": raw.get("_page_fetched_at_ms"),
        "incomplete": False,
        # Phase 1.1 Amendment 01 — FK to events(id), or None if not in /events response
        "event_id": event_id,
    }


def normalize_events(
    raw_events: list[dict],
) -> tuple[list[dict], list[dict], dict[str, str]]:
    """Normalize Gamma /events raw response into storage rows + market→event map.

    Phase 1.1 Amendment 01.

    Returns:
        events_rows: list of dicts matching EVENTS_COLUMN_ORDER (minus snapshot_id —
            orchestrator injects that). One row per event.
        event_tags_rows: list of dicts matching EVENT_TAGS_COLUMN_ORDER (minus
            snapshot_id). One row per (event, tag) pair. Multiple rows per event.
        market_to_event_map: dict[market_id → event_id]. Used by normalize_market
            to populate the markets.event_id FK column. A market that appears
            multiple times across events keeps the FIRST event_id seen.

    Events with no ``id`` are skipped (unrecoverable — events PK requires it).
    Events whose ``markets`` list is missing/non-list contribute no map entries.
    Events whose ``tags`` list is missing/non-list contribute no event_tags rows.
    """
    events_rows: list[dict] = []
    event_tags_rows: list[dict] = []
    market_to_event_map: dict[str, str] = {}
    seen_event_ids: set[str] = set()

    for raw in raw_events:
        event_id_raw = raw.get("id")
        if event_id_raw is None or event_id_raw == "":
            logger.warning(f"normalize_events: missing 'id' (slug={raw.get('slug')}) — skipped")
            continue
        event_id = str(event_id_raw)

        # Dedupe by event_id — Gamma /events can return duplicates across pagination
        # boundaries (similar to /markets ~4% dup rate observed in Phase 1).
        # Without this, the events table PK (id, snapshot_id) rejects the insert
        # and rolls back the whole snapshot.
        if event_id in seen_event_ids:
            continue
        seen_event_ids.add(event_id)

        # Slug is required by the schema (NOT NULL); fall back to event_id if absent
        # (defensive — real events always have a slug).
        slug = raw.get("slug") or event_id

        liq_raw = raw.get("liquidity")
        if liq_raw is None:
            liq_raw = raw.get("liquidityNum")
        liquidity_usd = _safe_float(liq_raw)

        vol_raw = raw.get("volume")
        if vol_raw is None:
            vol_raw = raw.get("volumeNum")
        volume_usd = _safe_float(vol_raw)

        end_iso = raw.get("endDate")
        end_time_ms = _parse_end_time_ms(end_iso)

        events_rows.append(
            {
                "id": event_id,
                "slug": slug,
                "title": raw.get("title"),
                "ticker": raw.get("ticker"),
                "active": bool(raw.get("active", False)),
                "closed": bool(raw.get("closed", False)),
                "liquidity_usd": liquidity_usd,
                "volume_usd": volume_usd,
                "end_time_ms": end_time_ms,
                # fetched_at_ms is stamped by orchestrator (None placeholder).
                "fetched_at_ms": None,
                # Phase 02 Plan 01: per-page real fetch time from _page_fetched_at_ms
                # private key injected by GammaClient._paginate(). None for pre-02 snapshots.
                "page_fetched_at_ms": raw.get("_page_fetched_at_ms"),
            }
        )

        # ── tags (many-to-many) ──────────────────────────────────────────────
        tags = raw.get("tags")
        if isinstance(tags, list):
            for tag in tags:
                if not isinstance(tag, dict):
                    continue
                tag_id_raw = tag.get("id")
                tag_label = tag.get("label")
                tag_slug = tag.get("slug")
                # All three are required by NOT NULL schema; skip incomplete tags.
                if tag_id_raw is None or not tag_label or not tag_slug:
                    continue
                event_tags_rows.append(
                    {
                        "event_id": event_id,
                        "tag_id": str(tag_id_raw),
                        "tag_label": str(tag_label),
                        "tag_slug": str(tag_slug),
                    }
                )

        # ── nested markets → market→event reverse lookup ──────────────────────
        markets = raw.get("markets")
        if isinstance(markets, list):
            for m in markets:
                if not isinstance(m, dict):
                    continue
                m_id = m.get("id")
                if m_id is None or m_id == "":
                    continue
                m_id_str = str(m_id)
                # Keep FIRST mapping if a market appears in multiple events
                # (shouldn't normally happen, but be defensive).
                if m_id_str not in market_to_event_map:
                    market_to_event_map[m_id_str] = event_id

    # Dedupe event_tags within a single batch on (event_id, tag_id) — duplicate
    # tag_ids on one event can occur if the API returns the same tag twice
    # (rare but observed in the wild). The schema's PRIMARY KEY (event_id, tag_id,
    # snapshot_id) would reject duplicates at insert time, so we filter here for
    # cleaner upstream semantics.
    seen: set[tuple[str, str]] = set()
    deduped_tags: list[dict] = []
    for et in event_tags_rows:
        key = (et["event_id"], et["tag_id"])
        if key in seen:
            continue
        seen.add(key)
        deduped_tags.append(et)

    return events_rows, deduped_tags, market_to_event_map
