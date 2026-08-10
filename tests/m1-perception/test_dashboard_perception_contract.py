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


def test_dashboard_promotes_structure_p1_with_bounded_budget_evidence() -> None:
    """A stale Structure map remains an actionable P1 in the Vercel view."""
    overview = _source("dashboard/app/perception/page.tsx")
    reader = _source("dashboard/lib/perception.ts")

    assert "p1StructureIncidents" in overview
    assert "P1 Structure publication incident" in overview
    assert "cooperative checkpoint target" in overview
    assert 'value.diagnosis.impact === "market-map-stale"' in reader


def test_typed_reader_uses_only_task6_public_get_contracts() -> None:
    reader = _source("dashboard/lib/perception.ts")
    types = _source("dashboard/lib/types.ts")

    for endpoint in (
        "/perception/status",
        "/perception/opportunities?limit=",
        "/perception/groups?limit=",
        "/perception/discovery",
        "/perception/reconciliation",
        "/perception/incidents?limit=",
        "/perception/resources?limit=",
        "/perception/groups/",
        "/timeline?limit=",
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
        "isResourcesEnvelope",
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
    assert "next_before" in history
    assert "history_complete" in history
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
        "Global Candidate state",
        "Bounded Structure page",
        "Showing",
        "No edge",
        "Bundle cost",
        "Max bundle size",
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


def test_dashboard_promotes_supervisor_p1_with_actionable_evidence() -> None:
    overview = _source("dashboard/app/perception/page.tsx")

    assert 'incident.scope === "quote"' in overview
    assert "P1 quote feed incident" in overview
    assert "Recovery evidence" in overview


def test_dashboard_validates_and_renders_bounded_resource_history() -> None:
    reader = _source("dashboard/lib/perception.ts")
    types = _source("dashboard/lib/types.ts")
    overview = _source("dashboard/app/perception/page.tsx")

    for field in (
        "candidate_quote_p95_ms",
        "candidate_missing_quote_count",
        "discovery_worker_ok",
        "reconciliation_running",
        "decision_ttl_ms",
        "valid_until_ms",
        "mode_changed_at_ms",
        "next_before_sequence",
        "history_floor",
    ):
        assert field in reader
        assert field in types
        assert field in overview
    resource_panel = overview.split(
        '<h2 style={{ marginTop: 0 }}>Resource mode</h2>',
        1,
    )[1].split("</section>", 1)[0]
    assert "<NotExposed />" not in resource_panel
    assert "Policy age" in resource_panel
    assert "TTL remaining" in resource_panel
    assert "Recent mode transitions" in resource_panel


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

    coverage_panel = overview.split(
        "<h2 style={{ marginTop: 0 }}>Coverage windows</h2>", 1
    )[1].split("</section>", 1)[0]
    assert "<NotExposed />" not in coverage_panel
    assert "Historical duration distribution is not tracked" in overview
    assert "Date.now()" not in overview
    assert "status.server_time_ms - resources.current.decided_at_ms" in overview
    assert "resources.current.valid_until_ms - status.server_time_ms" in overview
    assert "href={`/perception/${encodeURIComponent(item.group_id)}`}" in overview


def test_visual_fixture_validates_path_before_creating_database() -> None:
    fixture = _source("scripts/perception_dashboard_fixture.py")
    main = fixture.split("def main() -> None:", 1)[1]
    assert main.index("settings = Settings(") < main.index("store = _seed(args.db)")
    assert "store.begin_reconciliation" in fixture
    assert "store.apply_reconciliation_diff" in fixture


def test_nested_contract_validators_fail_closed() -> None:
    result = subprocess.run(
        [
            "node",
            "--no-warnings",
            "tests/m1-perception/perception_contract_cases.mjs",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr


def test_group_page_builds_one_timestamped_operator_timeline() -> None:
    history = _source("dashboard/app/perception/[group_id]/page.tsx")
    reader = _source("dashboard/lib/perception.ts")
    types = _source("dashboard/lib/types.ts")

    for phrase in (
        "Group timeline",
        "Membership revision",
        "Quote batch",
        "Opportunity transition",
        "Incident event",
        "occurredAtMs",
        ".sort(",
        "incidentEvidenceFields",
        "Raw evidence",
        "<details",
    ):
        assert phrase in history
    for class_name in (
        "membership_revision",
        "quote_batch",
        "opportunity_transition",
        "incident_event",
    ):
        assert class_name in reader
        assert class_name in types
    assert "/timeline?limit=" in reader
    assert "/history?limit=" not in reader
    assert "/incidents?limit=" not in reader.split(
        "export async function readPerceptionGroupHistory", 1
    )[1]
    assert "history_complete" in history
    assert "history_floor" in history
    assert "encodeURIComponent" in reader


def test_group_page_decodes_route_identity_before_reader_reencodes_it() -> None:
    history = _source("dashboard/app/perception/[group_id]/page.tsx")
    reader = _source("dashboard/lib/perception.ts")

    assert history.count("decodeURIComponent") == 1
    assert "catch" in history
    assert "return encodedGroupId" in history
    assert "belongsToGroup" not in history
    assert "candidate:${expectedGroupId}" in reader
    assert "encodeURIComponent(groupId)" in reader


def test_perception_pages_keep_long_ids_inside_mobile_viewport() -> None:
    overview = _source("dashboard/app/perception/page.tsx")
    history = _source("dashboard/app/perception/[group_id]/page.tsx")
    makefile = _source("Makefile")
    fixture = _source("scripts/perception_dashboard_fixture.py")

    assert 'overflowX: "auto"' in overview
    assert "minWidth: 900" in overview
    assert overview.count('overflowWrap: "anywhere"') >= 2
    assert 'overflowWrap: "anywhere"' in history
    assert "dashboard-fixture-api:" in makefile
    assert "refuses to overwrite an existing fixture DB" in makefile
    assert "FIXTURE_GROUP_ID" in fixture
    assert "UNAVAILABLE_GROUP_ID" in fixture
    assert "FixtureUnavailableMiddleware" in fixture


def test_overview_labels_bounded_group_counts_and_filters_verified_incidents() -> None:
    overview = _source("dashboard/app/perception/page.tsx")

    assert "bounded page" in overview
    assert "next_after" in overview
    assert 'incident.state !== "verified"' in overview
    assert "Latest incident states" in overview
    assert "incident.recovery_start_evidence" in overview


def test_overview_has_prominent_p1_quote_incident_panel() -> None:
    overview = _source("dashboard/app/perception/page.tsx")

    assert "P1 quote feed incident" in overview
    assert "p1QuoteIncidents" in overview
    assert "Automatic action:" in overview
    assert "Next action:" in overview
    assert "P1 failure:" in overview
    assert "next automatic retry" in overview


def test_overview_has_prominent_p1_capacity_incident_panel() -> None:
    overview = _source("dashboard/app/perception/page.tsx")
    reader = _source("dashboard/lib/perception.ts")

    assert "P1 storage capacity incident" in overview
    assert "p1CapacityIncidents" in overview
    assert "free space" in overview
    assert "storage-exhaustion-risk" in reader
    assert "inspect-capacity-receipts" in reader


def test_overview_binds_candidate_envelopes_before_rendering() -> None:
    reader = _source("dashboard/lib/perception.ts")
    types = _source("dashboard/lib/types.ts")
    compact_reader = " ".join(reader.split())

    assert "candidate_authority_hash" in reader
    assert "candidate_authority_hash" in types
    assert "candidateEnvelopesAgree" in reader
    assert (
        "status.candidate_authority_hash === "
        "currentOpportunities.candidate_authority_hash"
    ) in compact_reader
    assert (
        "status.opportunities.count === "
        "currentOpportunities.current_opportunity_count"
    ) in compact_reader


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
    assert ".PHONY: smoke-operator-console" in makefile
    assert "`make smoke-operator-console`" in manual
    assert "OPERATOR_CONSOLE_URL" in makefile
    assert (
        "<!-- m1-contract: route=/perception "
        "file=dashboard/app/perception/page.tsx -->"
    ) in manual
    assert (
        "<!-- m1-contract: route=/perception/[group_id] "
        "file=dashboard/app/perception/[group_id]/page.tsx -->"
    ) in manual
