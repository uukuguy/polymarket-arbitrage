"""Bounded event-centric business projection for certified Structure rows."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class StructureIntelligenceEvent:
    event_id: str
    payload: dict[str, object]


@dataclass(frozen=True, slots=True)
class StructureIntelligenceGroup:
    group_id: str
    payload: dict[str, object]


@dataclass(frozen=True, slots=True)
class StructureIntelligenceBundle:
    generation_key: str
    events: tuple[StructureIntelligenceEvent, ...]
    groups: tuple[StructureIntelligenceGroup, ...]
    summary: dict[str, object]


def build_structure_intelligence(
    *, generation_key: str, rows_by_component: Mapping[str, Sequence[Mapping[str, object]]]
) -> StructureIntelligenceBundle:
    """Aggregate normalized source rows without inventing unavailable facts."""
    tags_by_event: dict[str, list[str]] = defaultdict(list)
    for row in rows_by_component.get("event_tags", ()):
        event_id, label = _text(row.get("event_id")), _text(row.get("tag_label"))
        if event_id is not None and label is not None and label not in tags_by_event[event_id]:
            tags_by_event[event_id].append(label)

    markets_by_event: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    for row in rows_by_component.get("markets", ()):
        event_id = _text(row.get("event_id"))
        if event_id is not None:
            markets_by_event[event_id].append(row)

    groups_by_event: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    detached_groups: list[Mapping[str, object]] = []
    for row in rows_by_component.get("group_truth", ()):
        event_id = _text(row.get("event_id"))
        if event_id is None:
            detached_groups.append(row)
        else:
            groups_by_event[event_id].append(row)

    events: list[StructureIntelligenceEvent] = []
    for row in rows_by_component.get("events", ()):
        event_id = _text(row.get("id"))
        if event_id is None:
            continue
        market_rows = markets_by_event[event_id]
        group_rows = groups_by_event[event_id]
        payload = _event_payload(row, tags_by_event[event_id], market_rows, group_rows)
        events.append(StructureIntelligenceEvent(event_id=event_id, payload=payload))

    groups = tuple(
        StructureIntelligenceGroup(group_id=group_id, payload=_group_payload(row))
        for row in detached_groups
        if (group_id := _text(row.get("neg_risk_market_id"))) is not None
    )
    events.sort(key=lambda event: event.event_id)
    summary = {
        "event_count": len(events),
        "market_count": sum(int(event.payload["market_count"]) for event in events),
        "open_event_count": sum(event.payload["is_open"] is True for event in events),
        "detached_group_count": len(groups),
    }
    return StructureIntelligenceBundle(
        generation_key=generation_key,
        events=tuple(events),
        groups=groups,
        summary=summary,
    )


def _event_payload(
    row: Mapping[str, object], tags: Sequence[str], markets: Sequence[Mapping[str, object]], groups: Sequence[Mapping[str, object]]
) -> dict[str, object]:
    active = _bool(row.get("active"))
    closed = _bool(row.get("closed"))
    end_time_ms = _int(row.get("end_time_ms"))
    liquidity = _number(row.get("liquidity_usd"))
    volume = _number(row.get("volume_usd"))
    primary_group = groups[0] if groups else None
    missing_fields = [
        name for name, value in (("end_time_ms", end_time_ms), ("liquidity", liquidity), ("volume", volume)) if value is None
    ]
    return {
        "title": _text(row.get("title")),
        "slug": _text(row.get("slug")),
        "ticker": _text(row.get("ticker")),
        "is_open": active is True and closed is False,
        "active": active,
        "closed": closed,
        "end_time_ms": end_time_ms,
        "liquidity": liquidity,
        "volume": volume,
        "tags": sorted(tags),
        "market_count": len(markets),
        "active_market_count": sum(_bool(market.get("active")) is True for market in markets),
        "closed_market_count": sum(_bool(market.get("closed")) is True for market in markets),
        "neg_risk_quality": _text(primary_group.get("quality")) if primary_group else None,
        "neg_risk_reason": _text(primary_group.get("reason")) if primary_group else None,
        "missing_fields": missing_fields,
    }


def _group_payload(row: Mapping[str, object]) -> dict[str, object]:
    return {
        "event_id": _text(row.get("event_id")),
        "neg_risk_type": _text(row.get("neg_risk_type")),
        "expected_member_count": _int(row.get("expected_member_count")),
        "active_named_count": _int(row.get("active_named_count")),
        "quality": _text(row.get("quality")),
        "reason": _text(row.get("reason")),
    }


def _text(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _number(value: object) -> float | None:
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def _bool(value: object) -> bool | None:
    return value if isinstance(value, bool) else None
