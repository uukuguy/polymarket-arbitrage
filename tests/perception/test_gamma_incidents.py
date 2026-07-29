from __future__ import annotations

import asyncio
import json
import sqlite3
import time

import httpx
import pytest

from polyarb.clients.gamma_client import PaginationIntegrityError
from polyarb.perception.discovery import DiscoveryBatchResult, DiscoveryRunner
from polyarb.perception.gamma_incidents import (
    GammaBatchIncidents,
    gamma_incident_kind,
)
from polyarb.perception.reconciliation import (
    ReconciliationBatchResult,
    ReconciliationRunner,
)
from polyarb.perception.store import (
    DiscoveryAdmissionProof,
    OpportunityPerceptionStore,
)


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (httpx.ReadTimeout("slow"), "gamma-timeout"),
        (
            json.JSONDecodeError("bad", "x", 0),
            "gamma-malformed",
        ),
        (
            PaginationIntegrityError("/events/keyset repeated cursor"),
            "gamma-cursor",
        ),
        (
            PaginationIntegrityError(
                "/events/keyset keyset member has invalid state"
            ),
            "gamma-malformed",
        ),
        (
            ValueError("reconciliation-page-cursor-mismatch"),
            "gamma-cursor",
        ),
        (RuntimeError("sqlite write failed"), None),
    ],
)
def test_gamma_incident_kind_is_conservative(
    error: BaseException,
    expected: str | None,
) -> None:
    assert gamma_incident_kind(error) == expected


def test_reconciliation_gamma_incident_requires_new_checkpoint_to_verify(
    tmp_path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    tracker = GammaBatchIncidents(store, scope="reconciliation")

    tracker.record_failure(
        PaginationIntegrityError("/events/keyset repeated cursor")
    )

    incident = store.open_incidents()[0]
    assert incident.kind == "gamma-cursor"
    assert incident.state == "recovering"
    assert incident.evidence["pages_completed"] == 0

    time.sleep(0.01)
    now_ms = int(time.time() * 1_000)
    window = store.begin_reconciliation(started_at_ms=now_ms)
    store.publish_reconciliation_batch(
        window_id=window.id,
        requested_cursor=None,
        next_cursor="page-2",
        completed=False,
        started_at_ms=now_ms,
        finished_at_ms=now_ms,
        page_event_count=0,
        candidates=(),
    )
    tracker.verify_reconciliation(window.id)

    assert store.open_incidents() == ()


def test_discovery_gamma_incident_requires_new_batch_to_verify(tmp_path) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    store.configure_discovery_admission(
        DiscoveryAdmissionProof(
            effective_capacity=2,
            candidate_max_wait_ms=60_000,
            selection_budget_ms=6_000,
            poll_interval_ms=1_000,
            group_timeout_ms=10_000,
            terminal_write_budget_ms=5_000,
            high_burst_groups=1,
            reserved_non_high_slots=2,
        ),
        now_ms=0,
    )
    tracker = GammaBatchIncidents(store, scope="discovery")
    tracker.record_failure(httpx.ReadTimeout("slow"))

    time.sleep(0.01)
    now_ms = int(time.time() * 1_000)
    store.publish_discovery_batch(
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        started_at_ms=now_ms,
        finished_at_ms=now_ms,
        page_event_count=0,
        candidates=(),
        admission_proof=store.discovery_admission_proof(),
    )
    with store._connect() as con:
        batch_id = int(
            con.execute(
                "SELECT MAX(id) FROM neg_risk_discovery_batches"
            ).fetchone()[0]
        )
    tracker.verify_discovery(batch_id)

    assert store.open_incidents() == ()


@pytest.mark.asyncio
async def test_discovery_runner_persists_timeout_and_verifies_next_batch(
    tmp_path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    proof = DiscoveryAdmissionProof(
        effective_capacity=2,
        candidate_max_wait_ms=60_000,
        selection_budget_ms=6_000,
        poll_interval_ms=1_000,
        group_timeout_ms=10_000,
        terminal_write_budget_ms=5_000,
        high_burst_groups=1,
        reserved_non_high_slots=2,
    )
    store.configure_discovery_admission(proof, now_ms=0)
    stop = asyncio.Event()

    class Worker:
        _require_resource_decision = False
        calls = 0

        async def run_batch(self):
            self.calls += 1
            if self.calls == 1:
                raise httpx.ReadTimeout("slow")
            time.sleep(0.01)
            now_ms = int(time.time() * 1_000)
            store.publish_discovery_batch(
                requested_cursor=None,
                next_cursor=None,
                completed=True,
                started_at_ms=now_ms,
                finished_at_ms=now_ms,
                page_event_count=0,
                candidates=(),
                admission_proof=proof,
            )
            with store._connect() as con:
                batch_id = int(
                    con.execute(
                        "SELECT MAX(id) FROM neg_risk_discovery_batches"
                    ).fetchone()[0]
                )
            stop.set()
            return DiscoveryBatchResult(
                batch_id=batch_id,
                requested_cursor=None,
                next_cursor=None,
                completed=True,
                page_event_count=0,
                groups_seen=0,
                promoted_group_ids=(),
                started_at_ms=now_ms,
                finished_at_ms=now_ms,
            )

    class Gamma:
        async def aclose(self) -> None:
            return None

    await DiscoveryRunner(
        worker=Worker(),
        gamma=Gamma(),
        interval_s=0.01,
        store=store,
    ).run(stop)

    assert store.open_incidents() == ()
    with sqlite3.connect(store.db_path) as con:
        states = [
            row[0]
            for row in con.execute(
                "SELECT state FROM neg_risk_incident_events "
                "WHERE kind='gamma-timeout' ORDER BY sequence"
            )
        ]
    assert states == [
        "detected",
        "classified",
        "contained",
        "recovering",
        "verified",
    ]


@pytest.mark.asyncio
async def test_reconciliation_runner_persists_malformed_and_verifies_next_page(
    tmp_path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    stop = asyncio.Event()

    class Worker:
        calls = 0

        async def run_batch(self):
            self.calls += 1
            if self.calls == 1:
                raise json.JSONDecodeError("bad", "x", 0)
            time.sleep(0.01)
            now_ms = int(time.time() * 1_000)
            window = store.begin_reconciliation(started_at_ms=now_ms)
            store.publish_reconciliation_batch(
                window_id=window.id,
                requested_cursor=None,
                next_cursor="page-2",
                completed=False,
                started_at_ms=now_ms,
                finished_at_ms=now_ms,
                page_event_count=0,
                candidates=(),
            )
            stop.set()
            return ReconciliationBatchResult(
                window_id=window.id,
                requested_cursor=None,
                next_cursor="page-2",
                completed=False,
                page_event_count=0,
                groups_staged=0,
                rejected_count=0,
                started_at_ms=now_ms,
                finished_at_ms=now_ms,
                diff=None,
            )

    class Gamma:
        async def aclose(self) -> None:
            return None

    await ReconciliationRunner(
        worker=Worker(),
        gamma=Gamma(),
        interval_s=0.01,
        store=store,
    ).run(stop)

    assert store.open_incidents() == ()
    with sqlite3.connect(store.db_path) as con:
        states = [
            row[0]
            for row in con.execute(
                "SELECT state FROM neg_risk_incident_events "
                "WHERE kind='gamma-malformed' ORDER BY sequence"
            )
        ]
    assert states == [
        "detected",
        "classified",
        "contained",
        "recovering",
        "verified",
    ]
