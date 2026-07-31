"""Serve deterministic observer-only M1 data for Dashboard visual acceptance."""

from __future__ import annotations

import argparse
from pathlib import Path
from types import SimpleNamespace

import uvicorn
from pydantic import SecretStr
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from polyarb.config import Settings
from polyarb.http.app import create_app
from polyarb.perception.incidents import IncidentManager
from polyarb.perception.models import (
    GroupLeg,
    GroupQuoteBatch,
    GroupQuoteLeg,
    GroupRevision,
)
from polyarb.perception.store import OpportunityPerceptionStore
from polyarb.storage.sqlite_store import SQLiteStore

FIXTURE_GROUP_ID = (
    "fixture-neg-risk-weather-resolution-with-a-deliberately-long-"
    "operator-visible-group-identity-2026"
)
UNAVAILABLE_GROUP_ID = (
    "unavailable-neg-risk-group-with-a-deliberately-long-identity-"
    "that-must-wrap-on-mobile"
)


class FixtureUnavailableMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if (
            request.method == "GET"
            and request.url.path.endswith("/timeline")
            and f"/{UNAVAILABLE_GROUP_ID}/" in request.url.path
        ):
            return JSONResponse(
                {
                    "status": "unavailable",
                    "reason": "deterministic-fixture-evidence-unavailable",
                },
                status_code=503,
            )
        return await call_next(request)


def _seed(db_path: Path) -> OpportunityPerceptionStore:
    sqlite_store = SQLiteStore(db_path)
    sqlite_store.init_schema()
    store = OpportunityPerceptionStore(db_path)
    store.init_schema()
    reconciliation = store.begin_reconciliation(started_at_ms=1_774_915_000_000)
    store.publish_reconciliation_batch(
        window_id=reconciliation.id,
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        started_at_ms=1_774_915_000_000,
        finished_at_ms=1_774_915_006_250,
        page_event_count=0,
        candidates=(),
    )
    store.apply_reconciliation_diff(reconciliation.id)
    legs = (
        GroupLeg("market-a", "condition-a", "yes-token-a", "Candidate A"),
        GroupLeg("market-b", "condition-b", "yes-token-b", "Candidate B"),
        GroupLeg("market-c", "condition-c", "yes-token-c", "Candidate C"),
    )
    group = GroupRevision.certified(
        group_id=FIXTURE_GROUP_ID,
        event_id="fixture-event-weather-resolution",
        revision=1,
        started_at_ms=1_774_915_100_000,
        observed_at_ms=1_774_915_200_000,
        source_cursor="fixture-cursor-0001",
        legs=legs,
    )
    store.publish_group_revision(group)
    quote = GroupQuoteBatch.complete(
        group_id=group.group_id,
        membership_hash=group.membership_hash,
        quote_batch_id="fixture-quote-batch-0001",
        started_at_ms=1_774_915_201_000,
        quoted_at_ms=1_774_915_204_000,
        legs=tuple(
            GroupQuoteLeg(
                leg.yes_token_id,
                group.membership_hash,
                0.31,
                125.0,
                "executable",
            )
            for leg in legs
        ),
    )
    store.publish_candidate_success(
        quote,
        observed_at_ms=quote.quoted_at_ms,
        last_result="watching",
        reason=None,
        bundle_cost=0.93,
        gross_edge_bps=700,
        max_bundle_size=125,
        priority_class="high",
        consecutive_failures=0,
        effective_interval_s=15,
        schedule_reason="fixture-edge",
        next_due_at_ms=quote.quoted_at_ms + 15_000,
    )
    incident_clock = [1_774_915_205_000]
    incidents = IncidentManager(store, clock_ms=lambda: incident_clock[0])
    incident = incidents.detect(
        f"candidate:{group.group_id}",
        "quote-latency-warning",
        {
            "group_id": group.group_id,
            "action": "retry-producer",
            "retry_count": 1,
            "next_retry_at_ms": 1_774_915_219_000,
        },
    )
    for state, evidence in (
        ("classified", {"classification": "fixture-latency"}),
        ("contained", {"action": "retry-producer"}),
        ("recovering", {"action": "retry-producer"}),
    ):
        incident_clock[0] += 1_000
        incidents.transition(incident.id, state, evidence)
    recovered_quote = GroupQuoteBatch.complete(
        group_id=group.group_id,
        membership_hash=group.membership_hash,
        quote_batch_id="fixture-quote-batch-0002",
        started_at_ms=incident_clock[0] + 1_000,
        quoted_at_ms=incident_clock[0] + 2_000,
        legs=quote.legs,
    )
    store.publish_candidate_success(
        recovered_quote,
        observed_at_ms=recovered_quote.quoted_at_ms,
        last_result="watching",
        reason=None,
        bundle_cost=0.92,
        gross_edge_bps=800,
        max_bundle_size=125,
        priority_class="high",
        consecutive_failures=0,
        effective_interval_s=15,
        schedule_reason="fixture-recovered",
        next_due_at_ms=recovered_quote.quoted_at_ms + 15_000,
    )
    incident_clock[0] = recovered_quote.quoted_at_ms + 1_000
    incidents.transition(
        incident.id,
        "verified",
        {
            "verification": "fixture-post-recovery-success",
            "group_id": group.group_id,
            "quote_batch_id": recovered_quote.quote_batch_id,
            "membership_hash": group.membership_hash,
        },
    )
    return store


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    args.db.parent.mkdir(parents=True, exist_ok=True)
    if args.db.exists():
        raise SystemExit(f"fixture db already exists: {args.db}")
    settings = Settings(
        db_path=args.db,
        scan_shared_secret=SecretStr("dashboard-fixture-secret"),
        supabase_url="",
        supabase_service_key=SecretStr(""),
        r2_endpoint="",
        r2_access_key_id=SecretStr(""),
        r2_secret_access_key=SecretStr(""),
        supabase_mirror_enabled=False,
        r2_enabled=False,
    )
    store = _seed(args.db)
    app = create_app(
        scheduler=SimpleNamespace(),
        sqlite_store=SQLiteStore(store.db_path),
        settings=settings,
    )
    app.add_middleware(FixtureUnavailableMiddleware)
    print(f"fixture_group_id={FIXTURE_GROUP_ID}", flush=True)
    print(f"unavailable_group_id={UNAVAILABLE_GROUP_ID}", flush=True)
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
