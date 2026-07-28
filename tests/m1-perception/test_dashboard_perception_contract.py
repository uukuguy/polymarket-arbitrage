from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _source(relative_path: str) -> str:
    path = ROOT / relative_path
    assert path.is_file(), f"missing Task 7 Dashboard source: {relative_path}"
    return path.read_text()


def test_dashboard_has_server_component_perception_pages() -> None:
    overview = _source("dashboard/app/perception/page.tsx")
    history = _source("dashboard/app/perception/[group_id]/page.tsx")

    for source in (overview, history):
        assert '"use client"' not in source
        assert "force-dynamic" in source
        assert "revalidate = 0" in source

    assert "readPerceptionOverview" in overview
    assert "readPerceptionGroupHistory" in history


def test_typed_reader_uses_only_task6_public_get_contracts() -> None:
    reader = _source("dashboard/lib/perception.ts")
    types = _source("dashboard/lib/types.ts")

    for endpoint in (
        "/perception/status",
        "/perception/groups?limit=",
        "/perception/discovery",
        "/perception/reconciliation",
        "/perception/incidents?limit=",
        "/perception/groups/",
        "/history?limit=",
    ):
        assert endpoint in reader

    assert "/control/" not in reader
    assert 'method: "POST"' not in reader
    assert 'process.env.POLYARB_L1_URL ?? "https://polyarb-l1.fly.dev"' in reader
    assert "PerceptionReadResult" in types
    assert '"available" | "unavailable"' in types


def test_typed_reader_is_no_store_and_has_one_three_second_deadline() -> None:
    reader = _source("dashboard/lib/perception.ts")

    assert 'cache: "no-store"' in reader
    assert "AbortSignal.timeout(3000)" in reader
    assert "response.ok" in reader
    assert "await response.json()" in reader


def test_typed_reader_validates_nested_json_before_returning_available() -> None:
    reader = _source("dashboard/lib/perception.ts")

    for validator in (
        "isStatusEnvelope",
        "isGroupsEnvelope",
        "isDiscoveryEnvelope",
        "isReconciliationEnvelope",
        "isIncidentsEnvelope",
        "isGroupHistoryEnvelope",
    ):
        assert validator in reader
    assert "return body as T" not in reader
    assert "invalid JSON contract" in reader


def test_reader_accepts_failed_reconciliation_and_binds_history_group() -> None:
    reader = _source("dashboard/lib/perception.ts")
    types = _source("dashboard/lib/types.ts")
    history = _source("dashboard/app/perception/[group_id]/page.tsx")

    assert '"failed"' in types
    assert '"failed"' in reader
    assert "expectedGroupId" in reader
    assert "item.group_id === expectedGroupId" in reader
    assert "next_before_revision" in history
    assert "bounded page" in history


def test_unavailable_transport_never_renders_as_zero_opportunities() -> None:
    reader = _source("dashboard/lib/perception.ts")
    overview = _source("dashboard/app/perception/page.tsx")

    assert 'status: "unavailable"' in reader
    assert "HTTP " in reader
    assert "invalid JSON" in reader
    assert 'overview.status === "unavailable"' in overview
    assert "Perception unavailable" in overview
    assert "Unavailable is not zero opportunities" in overview
    assert "No certified edge right now" in overview


def test_overview_contains_operator_perception_and_incident_vocabulary() -> None:
    overview = _source("dashboard/app/perception/page.tsx")

    for phrase in (
        "Perception overview",
        "watching",
        "stale",
        "unavailable",
        "invalidated",
        "Current opportunities",
        "Structure age",
        "Quote age",
        "Raw coverage",
        "Weighted coverage",
        "15m",
        "30m",
        "60m",
        "Discovery",
        "Reconciliation",
        "Resource mode",
        "Open incidents",
    ):
        assert phrase in overview


def test_dashboard_validates_and_renders_progress_evidence() -> None:
    reader = _source("dashboard/lib/perception.ts")
    types = _source("dashboard/lib/types.ts")
    overview = _source("dashboard/app/perception/page.tsx")

    for field in (
        "coverage",
        "load_state",
        "admission_proof",
        "candidate_attempt_start_count",
        "candidate_start_deadline_breach_count",
        "duration_ms",
        "added_count",
        "changed_count",
        "closed_count",
        "unchanged_count",
        "applied_rejected_count",
    ):
        assert field in reader
        assert field in types
        assert field in overview

    assert "<NotExposed />" not in overview.split("<h2 style={{ marginTop: 0 }}>Coverage windows</h2>", 1)[1].split("</section>", 1)[0]
    assert "Historical duration distribution is not tracked" in overview


def test_group_page_builds_one_timestamped_operator_timeline() -> None:
    history = _source("dashboard/app/perception/[group_id]/page.tsx")

    for phrase in (
        "Group timeline",
        "Membership revision",
        "Quote batch",
        "Opportunity transition",
        "Incident event",
        "occurredAtMs",
        ".sort(",
    ):
        assert phrase in history
    assert "encodeURIComponent" in _source("dashboard/lib/perception.ts")


def test_group_page_decodes_route_identity_before_reader_reencodes_it() -> None:
    history = _source("dashboard/app/perception/[group_id]/page.tsx")
    reader = _source("dashboard/lib/perception.ts")

    assert history.count("decodeURIComponent") == 1
    assert "catch" in history
    assert "return encodedGroupId" in history
    assert "belongsToGroup(incident, groupId)" in history
    assert "encodeURIComponent(groupId)" in reader


def test_overview_labels_bounded_group_counts_and_filters_verified_incidents() -> None:
    overview = _source("dashboard/app/perception/page.tsx")

    assert "bounded page" in overview
    assert "next_after" in overview
    assert 'incident.state !== "verified"' in overview
    assert "Latest incident states" in overview


def test_root_navigation_links_to_perception_overview() -> None:
    layout = _source("dashboard/app/layout.tsx")

    assert 'href="/perception"' in layout
    assert "/perception" in layout


def test_make_smoke_and_living_manual_are_synchronized() -> None:
    result = subprocess.run(
        ["make", "-n", "smoke-perception-dashboard"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 0, result.stderr
    assert "/perception" in result.stdout
    assert "curl" in result.stdout

    makefile = _source("Makefile")
    checker = _source("scripts/check_m1_manual.py")
    manual = _source("docs/M1-市场感知平台使用手册.md")
    assert ".PHONY: smoke-perception-dashboard" in makefile
    assert '"smoke-perception-dashboard"' in checker
    assert "`make smoke-perception-dashboard`" in manual
    assert (
        "<!-- m1-contract: route=/perception "
        "file=dashboard/app/perception/page.tsx -->"
    ) in manual
    assert (
        "<!-- m1-contract: route=/perception/[group_id] "
        "file=dashboard/app/perception/[group_id]/page.tsx -->"
    ) in manual
