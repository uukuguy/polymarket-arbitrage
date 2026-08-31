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


def test_renderer_uses_labelled_scalar_lines_instead_of_mapping_reprs() -> None:
    status = _status_fixture()
    status["structure"] = {
        "latest_manifest": {
            "generation_key": "structure-1",
            "record_count": 12,
            "published_at": "2026-08-31T08:00:00+00:00",
        }
    }
    status["quote"] = {
        "current_pointer": {
            "generation_key": "quote-1",
            "parent_structure_generation_key": "structure-1",
            "record_count": 12,
            "published_at": "2026-08-31T08:01:00+00:00",
        }
    }
    status["open_incidents"] = [
        {"component": "quote", "severity": "critical", "summary": "projection unavailable"}
    ]
    status["runtime_incidents"] = {
        "items": [
            {"component": "runtime", "severity": "warning", "summary": "retrying"},
            {"component": "structure", "severity": "critical", "summary": "stalled"},
            {"component": "quote", "severity": "warning", "summary": "late"},
            {"component": "extra", "severity": "warning", "summary": "hidden"},
        ],
        "total": 4,
    }
    status["recovery_actions"] = {"items": [], "total": 2}
    status["runtime_watchdog"] = {
        "current": {"kind": "current-kind"},
        "recent_events": [{"kind": "recent-kind"}],
    }
    opportunities = _opportunities_fixture()
    opportunity_items = opportunities["items"]
    assert isinstance(opportunity_items, list)
    opportunity_items[0] = {
        "group_id": "group-1",
        "event_id": "event-1",
        "gross_edge_bps": 123.4,
        "max_bundle_size": 5.0,
    }

    text = render_business_brief(build_business_brief(status, opportunities))

    assert "Structure 最新 generation：structure-1" in text
    assert "Structure record_count：12" in text
    assert "Structure published_at：2026-08-31T08:00:00+00:00" in text
    assert "Quote current generation：quote-1" in text
    assert "Quote parent：structure-1" in text
    assert "Quote record_count：12" in text
    assert "Quote published_at：2026-08-31T08:01:00+00:00" in text
    assert "资格原因：freshness.quote" in text
    assert "认证机会数：6" in text
    assert "机会 1：group=group-1；event=event-1；gross_edge_bps=123.4；max_bundle_size=5.0" in text
    assert "Open incidents：1" in text
    assert "Runtime incidents：4" in text
    assert "Recovery actions：2" in text
    assert "Runtime incident：runtime / warning — retrying" in text
    assert "Runtime incident：structure / critical — stalled" in text
    assert "Runtime incident：quote / warning — late" in text
    assert "hidden" not in text
    assert "Watchdog current kind：current-kind" in text
    assert "Watchdog recent kind：recent-kind" in text
    assert "{'" not in text


def test_renderer_displays_missing_or_none_selected_values_as_not_provided() -> None:
    status = _status_fixture()
    status["qualification"] = {"eligibility_state": "paused", "eligibility_reason": None}
    brief = build_business_brief(status, _opportunities_fixture())

    text = render_business_brief(brief)

    assert "Structure 最新 generation：未提供" in text
    assert "Quote current generation：未提供" in text
    assert "资格原因：未提供" in text
    assert "Watchdog current kind：未提供" in text
    assert "Watchdog recent kind：未提供" in text


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


def test_business_brief_json_reads_one_atomic_business_overview(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    overview = {"schema_version": "m1.business-overview.v1", "status": "available"}
    calls: dict[str, object] = {}

    def fake_urlopen(request: Request, *, timeout: float) -> _OpportunityResponse:
        calls["request"] = request
        calls["timeout"] = timeout
        return _OpportunityResponse(overview)

    monkeypatch.setattr(cli_control_plane, "urlopen", fake_urlopen)

    assert cli_control_plane.main(["business-brief", "--format", "json"]) == 0

    request = calls["request"]
    assert isinstance(request, Request)
    assert request.full_url == "https://polyarb-control-api.fly.dev/perception/business-overview"
    assert calls["timeout"] == 10
    assert json.loads(capsys.readouterr().out) == overview


def test_business_brief_redacts_unavailable_authority(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        cli_control_plane,
        "_control_plane_from_env",
        lambda: SimpleNamespace(business_overview=lambda: (_ for _ in ()).throw(RuntimeError())),
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
