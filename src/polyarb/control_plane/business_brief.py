"""Canonical, read-only business brief projection for M1 authorities."""

from __future__ import annotations

from collections.abc import Mapping, Sequence


class BusinessBriefUnavailable(ValueError):
    """The status or opportunity authority is unavailable or malformed."""


def build_business_brief(
    status: Mapping[str, object], opportunities: Mapping[str, object]
) -> dict[str, object]:
    """Combine complete authority reads into the fixed business-brief shape."""
    status = _mapping(status, field="status")
    opportunities = _mapping(opportunities, field="opportunities")
    if _required_text(status, field="status.status") != "available":
        raise BusinessBriefUnavailable("status-authority-unavailable")
    if _required_text(opportunities, field="opportunities.status") != "available":
        raise BusinessBriefUnavailable("opportunity-authority-unavailable")

    qualification = _mapping(
        _required(status, field="status.qualification"), field="status.qualification"
    )
    eligibility_state = _required_text(
        qualification, field="status.qualification.eligibility_state"
    )
    eligibility_reason = qualification.get("eligibility_reason")
    if eligibility_reason is not None and not isinstance(eligibility_reason, str):
        raise BusinessBriefUnavailable("qualification-eligibility-reason-malformed")

    structure = _mapping(
        _required(status, field="status.structure"), field="status.structure"
    )
    quote = _mapping(_required(status, field="status.quote"), field="status.quote")
    open_incidents = _sequence(
        _required(status, field="status.open_incidents"), field="status.open_incidents"
    )
    runtime_incidents, runtime_incident_total = _items_total_mapping(
        _required(status, field="status.runtime_incidents"),
        field="status.runtime_incidents",
    )
    recovery_actions, _ = _items_total_mapping(
        _required(status, field="status.recovery_actions"),
        field="status.recovery_actions",
    )
    watchdog = _mapping(
        _required(status, field="status.runtime_watchdog"), field="status.runtime_watchdog"
    )
    opportunity_count = _required_count(opportunities, field="current_opportunity_count")
    items = _sequence(
        _required(opportunities, field="opportunities.items"),
        field="opportunities.items",
    )

    return {
        "status": "available",
        "conclusion": {
            "eligibility_state": eligibility_state,
            "eligibility_reason": eligibility_reason,
            "escalate": eligibility_state == "paused"
            or bool(open_incidents)
            or runtime_incident_total > 0,
        },
        "structure": structure,
        "quote": quote,
        "opportunities": {"count": opportunity_count, "items": items[:5]},
        "incidents": {
            "open": open_incidents,
            "runtime": runtime_incidents,
            "recovery_actions": recovery_actions,
            "watchdog": watchdog,
        },
    }


def render_business_brief(brief: Mapping[str, object]) -> str:
    """Render the fixed five-section operator view without derived economics."""
    brief = _mapping(brief, field="brief")
    conclusion = _mapping(_required(brief, field="brief.conclusion"), field="brief.conclusion")
    structure = _mapping(_required(brief, field="brief.structure"), field="brief.structure")
    quote = _mapping(_required(brief, field="brief.quote"), field="brief.quote")
    opportunities = _mapping(
        _required(brief, field="brief.opportunities"), field="brief.opportunities"
    )
    incidents = _mapping(_required(brief, field="brief.incidents"), field="brief.incidents")
    latest_manifest = _optional_mapping(structure.get("latest_manifest"))
    current_pointer = _optional_mapping(quote.get("current_pointer"))
    opportunity_items = _optional_sequence(opportunities.get("items"))
    open_incidents = _optional_sequence(incidents.get("open"))
    runtime_incidents = _optional_mapping(incidents.get("runtime"))
    runtime_incident_items = _optional_sequence(runtime_incidents.get("items"))
    recovery_actions = _optional_mapping(incidents.get("recovery_actions"))
    watchdog = _optional_mapping(incidents.get("watchdog"))
    watchdog_current = _optional_mapping(watchdog.get("current"))
    watchdog_recent = _first_mapping(_optional_sequence(watchdog.get("recent_events")))

    lines = [
            "今日结论",
            f"资格：{_display(conclusion.get('eligibility_state'))}",
            f"资格原因：{_display(conclusion.get('eligibility_reason'))}",
            f"需要升级：{_display(conclusion.get('escalate'))}",
            f"认证机会数：{_display(opportunities.get('count'))}",
            "",
            "市场覆盖（Structure）",
            f"Structure 最新 generation：{_display(latest_manifest.get('generation_key'))}",
            f"Structure record_count：{_display(latest_manifest.get('record_count'))}",
            f"Structure published_at：{_display(latest_manifest.get('published_at'))}",
            "",
            "报价（Quote）",
            f"Quote current generation：{_display(current_pointer.get('generation_key'))}",
            f"Quote parent：{_display(current_pointer.get('parent_structure_generation_key'))}",
            f"Quote record_count：{_display(current_pointer.get('record_count'))}",
            f"Quote published_at：{_display(current_pointer.get('published_at'))}",
            "",
            "资格与机会",
        ]
    for index, item in enumerate(opportunity_items[:5], start=1):
        opportunity = _optional_mapping(item)
        lines.append(
            "机会 "
            f"{index}：group={_display(opportunity.get('group_id'))}；"
            f"event={_display(opportunity.get('event_id'))}；"
            f"gross_edge_bps={_display(opportunity.get('gross_edge_bps'))}；"
            f"max_bundle_size={_display(opportunity.get('max_bundle_size'))}"
        )
    if not opportunity_items:
        lines.append("暂无认证机会")
    lines.extend(
        (
            "",
            "异常与恢复",
            f"Open incidents：{len(open_incidents)}",
            f"Runtime incidents：{_display(runtime_incidents.get('total'))}",
            f"Recovery actions：{_display(recovery_actions.get('total'))}",
        )
    )
    for item in runtime_incident_items[:3]:
        incident = _optional_mapping(item)
        lines.append(
            "Runtime incident："
            f"{_display(incident.get('component'))} / "
            f"{_display(incident.get('severity'))} — "
            f"{_display(incident.get('summary'))}"
        )
    lines.extend(
        (
            f"Watchdog current kind：{_display(watchdog_current.get('kind'))}",
            f"Watchdog recent kind：{_display(watchdog_recent.get('kind'))}",
        )
    )
    return "\n".join(lines)


def _optional_mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def _optional_sequence(value: object) -> Sequence[object]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        return ()
    return value


def _first_mapping(values: Sequence[object]) -> Mapping[str, object]:
    return _optional_mapping(values[0]) if values else {}


def _display(value: object) -> str:
    """Display only a scalar authority value; absent or composite values stay explicit."""
    if value is None or isinstance(value, (Mapping, Sequence)) and not isinstance(value, str):
        return "未提供"
    return str(value)


def _required(mapping: Mapping[str, object], *, field: str) -> object:
    try:
        return mapping[field.rsplit(".", maxsplit=1)[-1]]
    except KeyError as error:
        raise BusinessBriefUnavailable(f"{field}-missing") from error


def _mapping(value: object, *, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise BusinessBriefUnavailable(f"{field}-malformed")
    return value


def _sequence(value: object, *, field: str) -> Sequence[object]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise BusinessBriefUnavailable(f"{field}-malformed")
    return value


def _items_total_mapping(value: object, *, field: str) -> tuple[Mapping[str, object], int]:
    """Validate a bounded control-plane collection without changing its shape."""
    mapping = _mapping(value, field=field)
    items = _sequence(_required(mapping, field=f"{field}.items"), field=f"{field}.items")
    total = _required_count(mapping, field=f"{field}.total")
    if total < len(items):
        raise BusinessBriefUnavailable(f"{field}.total-malformed")
    return mapping, total


def _required_text(mapping: Mapping[str, object], *, field: str) -> str:
    value = _required(mapping, field=field)
    if not isinstance(value, str):
        raise BusinessBriefUnavailable(f"{field}-malformed")
    return value


def _required_count(mapping: Mapping[str, object], *, field: str) -> int:
    value = _required(mapping, field=field)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise BusinessBriefUnavailable(f"{field}-malformed")
    return value
