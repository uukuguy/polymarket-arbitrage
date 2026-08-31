"""Contracts for the canonical M1 business brief."""

from __future__ import annotations

import json
from collections.abc import Mapping
from types import SimpleNamespace
from urllib.request import Request

import pytest

from polyarb import cli_control_plane
from polyarb.control_plane.business_brief import (
    BusinessBriefUnavailable,
    build_business_brief,
    render_business_brief,
)


def _status_fixture() -> dict[str, object]:
    return {
        "status": "available",
        "structure": {"generation": "structure-1", "record_count": 12},
        "quote": {"generation": "quote-1", "record_count": 12},
        "qualification": {
            "eligibility_state": "paused",
            "eligibility_reason": "freshness.quote",
        },
        "open_incidents": [],
        "runtime_incidents": {"items": [], "total": 0},
        "recovery_actions": {"items": [], "total": 0},
        "runtime_watchdog": {"current": None, "recent_events": []},
    }


def _opportunities_fixture() -> dict[str, object]:
    return {
        "status": "available",
        "current_opportunity_count": 6,
        "items": [
            {"group_id": f"group-{index}", "gross_edge_bps": index}
            for index in range(1, 7)
        ],
    }


def test_builds_available_brief_from_canonical_authorities() -> None:
    status = _status_fixture()
    opportunities = _opportunities_fixture()
    opportunity_items = opportunities["items"]
    assert isinstance(opportunity_items, list)

    brief = build_business_brief(status, opportunities)

    assert brief == {
        "status": "available",
        "conclusion": {
            "eligibility_state": "paused",
            "eligibility_reason": "freshness.quote",
            "escalate": True,
        },
        "structure": status["structure"],
        "quote": status["quote"],
        "opportunities": {
            "count": 6,
            "items": opportunity_items[:5],
        },
        "incidents": {
            "open": [],
            "runtime": {"items": [], "total": 0},
            "recovery_actions": {"items": [], "total": 0},
            "watchdog": status["runtime_watchdog"],
        },
    }
    brief_opportunities = brief["opportunities"]
    assert isinstance(brief_opportunities, Mapping)
    assert brief_opportunities["count"] == 6
    items = brief_opportunities["items"]
    assert isinstance(items, list)
    assert len(items) == 5


def test_preserves_real_runtime_mappings_and_escalates_for_total() -> None:
    status = _status_fixture()
    status["qualification"] = {
        "eligibility_state": "qualified",
        "eligibility_reason": None,
    }
    status["runtime_incidents"] = {"items": [], "total": 1}
    status["recovery_actions"] = {
        "items": [{"action_id": "action-1", "state": "pending"}],
        "total": 1,
    }

    brief = build_business_brief(status, _opportunities_fixture())

    incidents = brief["incidents"]
    assert isinstance(incidents, Mapping)
    assert incidents["runtime"] == status["runtime_incidents"]
    assert incidents["recovery_actions"] == status["recovery_actions"]
    conclusion = brief["conclusion"]
    assert isinstance(conclusion, Mapping)
    assert conclusion["escalate"] is True


def test_renderer_emits_five_business_sections() -> None:
    text = render_business_brief(build_business_brief(_status_fixture(), _opportunities_fixture()))

    assert "今日结论" in text
    assert "市场覆盖（Structure）" in text
    assert "报价（Quote）" in text
    assert "资格与机会" in text
    assert "异常与恢复" in text


@pytest.mark.parametrize(
    ("status", "opportunities"),
    [
        (_status_fixture(), {"status": "unavailable"}),
        (
            {
                key: value
                for key, value in _status_fixture().items()
                if key != "qualification"
            },
            _opportunities_fixture(),
        ),
    ],
)
def test_rejects_unavailable_or_malformed_authority(
    status: dict[str, object], opportunities: dict[str, object]
) -> None:
    with pytest.raises(BusinessBriefUnavailable):
        build_business_brief(status, opportunities)


class _OpportunityResponse:
    def __init__(self, payload: Mapping[str, object], *, status: int = 200) -> None:
        self.status = status
        self._payload = payload

    def __enter__(self) -> _OpportunityResponse:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def read(self) -> bytes:
        return json.dumps(self._payload).encode()


def test_business_brief_json_reads_bounded_status_and_public_opportunities(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    status = _status_fixture()
    opportunities = _opportunities_fixture()
    calls: dict[str, object] = {}

    class _ControlPlane:
        def operational_snapshot(self, *, sample_limit: int) -> dict[str, object]:
            calls["sample_limit"] = sample_limit
            return status

        def close(self) -> None:
            calls["closed"] = True

    def fake_urlopen(request: Request, *, timeout: float) -> _OpportunityResponse:
        calls["request"] = request
        calls["timeout"] = timeout
        return _OpportunityResponse(opportunities)

    monkeypatch.setattr(cli_control_plane, "_control_plane_from_env", lambda: _ControlPlane())
    monkeypatch.setattr(cli_control_plane, "urlopen", fake_urlopen)

    assert cli_control_plane.main(["business-brief", "--format", "json"]) == 0

    request = calls["request"]
    assert isinstance(request, Request)
    assert request.method == "GET"
    assert request.full_url == (
        "https://polyarb-control-api.fly.dev/perception/opportunities?limit=50"
    )
    assert calls["sample_limit"] == 20
    assert calls["timeout"] == 10
    assert calls["closed"] is True
    assert json.loads(capsys.readouterr().out) == build_business_brief(status, opportunities)


def test_business_brief_redacts_unavailable_authority(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        cli_control_plane,
        "_control_plane_from_env",
        lambda: SimpleNamespace(operational_snapshot=lambda *, sample_limit: _status_fixture()),
    )
    monkeypatch.setattr(
        cli_control_plane,
        "urlopen",
        lambda request, *, timeout: _OpportunityResponse({"status": "unavailable"}),
    )

    assert cli_control_plane.main(["business-brief"]) == 2
    captured = capsys.readouterr()
    assert "业务数据不可用" in captured.err
    assert captured.out == ""


def test_business_brief_parser_rejects_unknown_format() -> None:
    with pytest.raises(SystemExit) as error:
        cli_control_plane._parser().parse_args(["business-brief", "--format", "xml"])

    assert error.value.code == 2


@pytest.mark.parametrize("limit", (0, -1, 501))
def test_business_brief_parser_rejects_limit_outside_local_bound(limit: int) -> None:
    with pytest.raises(SystemExit) as error:
        cli_control_plane._parser().parse_args(["business-brief", "--limit", str(limit)])

    assert error.value.code == 2
