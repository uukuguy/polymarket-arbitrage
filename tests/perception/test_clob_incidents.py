from __future__ import annotations

import sqlite3
import time

import httpx
import pytest
from py_clob_client.exceptions import PolyApiException

from polyarb.perception.clob_incidents import (
    CandidateGroupIncidents,
    QualifiedCandidateIncidentReceipt,
    clob_incident_kind,
)
from polyarb.perception.models import (
    GroupLeg,
    GroupQuoteBatch,
    GroupQuoteLeg,
    GroupRevision,
)
from polyarb.perception.store import OpportunityPerceptionStore
from polyarb.routing.neg_risk_quote_collector import (
    QuoteCollectionIntegrityError,
)


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (QuoteCollectionIntegrityError(), "clob-missing-leg"),
        (TimeoutError(), "clob-latency"),
        (sqlite3.OperationalError("database is locked"), "sqlite-busy"),
        (RuntimeError("sqlite busy"), None),
    ],
)
def test_clob_incident_kind_is_conservative(
    error: BaseException,
    expected: str | None,
) -> None:
    assert clob_incident_kind(error) == expected


def test_clob_incident_kind_recognizes_sdk_429_only() -> None:
    request = httpx.Request("GET", "https://clob.example.test/books")
    rate_limited = PolyApiException(
        resp=httpx.Response(429, request=request, json={"error": "rate"})
    )
    server_error = PolyApiException(
        resp=httpx.Response(500, request=request, json={"error": "server"})
    )

    assert clob_incident_kind(rate_limited) == "clob-429"
    assert clob_incident_kind(server_error) is None
    assert clob_incident_kind(sqlite3.OperationalError("no such table")) is None


@pytest.mark.parametrize(
    ("error", "kind"),
    [
        (QuoteCollectionIntegrityError(), "clob-missing-leg"),
        (sqlite3.OperationalError("database is locked"), "sqlite-busy"),
    ],
)
def test_candidate_group_incident_requires_exact_success_receipt_to_verify(
    tmp_path,
    error: BaseException,
    kind: str,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    revision = GroupRevision.certified(
        group_id="g-1",
        event_id="e-1",
        revision=1,
        started_at_ms=1,
        observed_at_ms=2,
        source_cursor="c",
        legs=(
            GroupLeg("m1", "c1", "t1", "one"),
            GroupLeg("m2", "c2", "t2", "two"),
        ),
    )
    store.publish_group_revision(revision)
    tracker = CandidateGroupIncidents(store)
    tracker.record_failure("g-1", error)

    incident = store.open_incidents()[0]
    assert incident.scope == "candidate:g-1"
    assert incident.kind == kind
    assert incident.state == "recovering"

    time.sleep(0.01)
    now_ms = int(time.time() * 1_000)
    quote = GroupQuoteBatch.complete(
        group_id="g-1",
        membership_hash=revision.membership_hash,
        quote_batch_id="qb-recovered",
        started_at_ms=now_ms - 1,
        quoted_at_ms=now_ms,
        legs=(
            GroupQuoteLeg("t1", revision.membership_hash, 0.4, 10, "executable"),
            GroupQuoteLeg("t2", revision.membership_hash, 0.5, 10, "executable"),
        ),
    )
    store.publish_candidate_success(
        quote,
        observed_at_ms=now_ms,
        last_result="watching",
        reason=None,
        bundle_cost=0.9,
        gross_edge_bps=1_000,
        max_bundle_size=10,
        priority_class="high",
        consecutive_failures=0,
        effective_interval_s=15,
        schedule_reason="test",
        next_due_at_ms=now_ms + 15_000,
    )
    tracker.verify_success(
        group_id="g-1",
        membership_hash=revision.membership_hash,
        quote_batch_id=quote.quote_batch_id,
    )

    assert store.open_incidents() == ()


def test_qualified_candidate_incident_binds_exact_group_and_call_id(
    tmp_path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "qualified.db")
    store.init_schema()
    tracker = CandidateGroupIncidents(store, clock_ms=lambda: 1_000)
    error = QuoteCollectionIntegrityError()
    error._polyarb_fault_call_id = "call-qualified"

    receipt = tracker.record_qualified_failure("g-1", error)

    assert isinstance(receipt, QualifiedCandidateIncidentReceipt)
    assert receipt.scope == "candidate:g-1"
    assert receipt.kind == "clob-missing-leg"
    assert receipt.fault_call_id == "call-qualified"
    assert tracker.validate_qualified_receipt(receipt) is True


@pytest.mark.parametrize("existing_at_ms", [1_000, 999])
def test_qualified_candidate_incident_rejects_organic_open_dedup(
    tmp_path,
    existing_at_ms: int,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "ambiguous.db")
    store.init_schema()
    organic = CandidateGroupIncidents(store, clock_ms=lambda: existing_at_ms)
    assert organic.record_failure("g-1", QuoteCollectionIntegrityError()) is not None
    qualified = CandidateGroupIncidents(store, clock_ms=lambda: 1_000)
    error = QuoteCollectionIntegrityError()
    error._polyarb_fault_call_id = "call-qualified"

    assert qualified.record_qualified_failure("g-1", error) is None
