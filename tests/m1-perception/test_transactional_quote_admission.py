from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from polyarb.control_plane.models import JobLease, JobState
from polyarb.control_plane.quote_admission import (
    QuoteAdmissionError,
    TransactionalQuoteAdmitter,
)
from polyarb.control_plane.structure_artifact import (
    StructureBundleArtifact,
    StructureBundleIdentity,
    canonical_structure_bundle_bytes,
)

NOW = datetime(2030, 1, 1, tzinfo=UTC)


class _Body:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return self._payload


class _Objects:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def get_object(self, **kwargs: object) -> dict[str, object]:
        assert kwargs == {"Bucket": "artifacts", "Key": "bundles/current.ndjson"}
        return {"Body": _Body(self._payload)}


class _ControlPlane:
    def __init__(self, digest: str) -> None:
        self.digest = digest
        self.admitted: dict[str, object] | None = None
        self.finished: list[JobState] = []

    def claim_job(self, **kwargs: object) -> JobLease:
        assert kwargs["job_types"] == ("quote-admit",)
        return JobLease(
            job_key="structure:digest:quote-admit",
            job_type="quote-admit",
            input_identity="structure:digest:bundles/current.ndjson:" + self.digest,
            lease_owner="quote-admitter",
            lease_epoch=1,
            lease_expires_at=NOW,
            checkpoint_cursor=None,
            checkpoint_digest=None,
        )

    def quote_admission_input(self, job_key: str) -> tuple[str, str, str]:
        assert job_key == "structure:digest:quote-admit"
        return ("structure:digest", "bundles/current.ndjson", self.digest)

    def admit_quote_generation(self, lease: JobLease, **kwargs: object) -> None:
        self.admitted = kwargs

    def finish(self, lease: JobLease, *, state: JobState, **kwargs: object) -> None:
        self.finished.append(state)


def _bundle() -> StructureBundleArtifact:
    components = {
        "events": ({"id": "event-a"},),
        "event_tags": (),
        "memberships": (),
        "group_truth": (),
        "markets": (
            {
                "market_id": "market-active",
                "condition_id": "condition-active",
                "slug": "active-market",
                "yes_token_id": "yes-active",
                "event_id": "event-a",
                "active": True,
                "closed": False,
                "neg_risk": True,
                "neg_risk_market_id": "neg-risk-a",
            },
            {
                "market_id": "market-closed",
                "condition_id": "condition-closed",
                "slug": "closed-market",
                "yes_token_id": "yes-closed",
                "event_id": "event-a",
                "active": True,
                "closed": True,
                "neg_risk": True,
                "neg_risk_market_id": "neg-risk-a",
            },
        ),
        "issues": (),
    }
    identity = StructureBundleIdentity(
        publication_id="p",
        window_id="w",
        snapshot_id=1,
        comparison_receipt_digest="a" * 64,
        normalization_contract_version="v",
        component_counts={key: len(value) for key, value in components.items()},
    )
    return StructureBundleArtifact.from_bytes(
        canonical_structure_bundle_bytes(identity=identity, components=components)
    )


def test_quote_admitter_derives_active_neg_risk_legs_from_authenticated_bundle() -> None:
    bundle = _bundle()
    control_plane = _ControlPlane(bundle.sha256)
    worker = TransactionalQuoteAdmitter(
        control_plane=control_plane,
        object_client=_Objects(bundle.payload),
        bucket="artifacts",
        worker_id="quote-admitter",
        now=lambda: NOW,
        batch_size=100,
    )

    assert asyncio.run(worker.run_once()).outcome == "admitted"
    assert control_plane.admitted is not None
    legs = control_plane.admitted["legs"]
    assert len(legs) == 1
    assert legs[0].yes_token_id == "yes-active"
    assert control_plane.admitted["structure_receipt_digest"] == bundle.sha256
    assert control_plane.finished == []


def test_quote_admitter_retries_without_batches_when_bundle_digest_is_wrong() -> None:
    bundle = _bundle()
    control_plane = _ControlPlane("b" * 64)
    worker = TransactionalQuoteAdmitter(
        control_plane=control_plane,
        object_client=_Objects(bundle.payload),
        bucket="artifacts",
        worker_id="quote-admitter",
        now=lambda: NOW,
        batch_size=100,
    )

    with pytest.raises(QuoteAdmissionError, match="digest"):
        asyncio.run(worker.run_once())
    assert control_plane.admitted is None
    assert control_plane.finished == [JobState.RETRYABLE]
