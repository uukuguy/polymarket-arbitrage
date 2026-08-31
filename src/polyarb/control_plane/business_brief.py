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

    structure = _mapping(_required(status, field="status.structure"), field="status.structure")
    quote = _mapping(_required(status, field="status.quote"), field="status.quote")
    open_incidents = _sequence(
        _required(status, field="status.open_incidents"), field="status.open_incidents"
    )
    runtime_incidents = _sequence(
        _required(status, field="status.runtime_incidents"), field="status.runtime_incidents"
    )
    recovery_actions = _sequence(
        _required(status, field="status.recovery_actions"), field="status.recovery_actions"
    )
    watchdog = _mapping(
        _required(status, field="status.runtime_watchdog"), field="status.runtime_watchdog"
    )
    opportunity_count = _required_count(opportunities, field="current_opportunity_count")
    items = _sequence(_required(opportunities, field="opportunities.items"), field="opportunities.items")

    return {
        "status": "available",
        "conclusion": {
            "eligibility_state": eligibility_state,
            "eligibility_reason": eligibility_reason,
            "escalate": eligibility_state == "paused"
            or bool(open_incidents)
            or bool(runtime_incidents),
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
    opportunities = _mapping(
        _required(brief, field="brief.opportunities"), field="brief.opportunities"
    )
    incidents = _mapping(_required(brief, field="brief.incidents"), field="brief.incidents")

    return "\n".join(
        (
            "今日结论",
            f"资格：{_required(conclusion, field='brief.conclusion.eligibility_state')}",
            f"原因：{conclusion.get('eligibility_reason')}",
            f"需要升级：{_required(conclusion, field='brief.conclusion.escalate')}",
            f"认证机会：{_required(opportunities, field='brief.opportunities.count')}",
            "",
            "市场覆盖（Structure）",
            str(_required(brief, field="brief.structure")),
            "",
            "报价（Quote）",
            str(_required(brief, field="brief.quote")),
            "",
            "资格与机会",
            str(_required(opportunities, field="brief.opportunities.items")),
            "",
            "异常与恢复",
            str(incidents),
        )
    )


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
