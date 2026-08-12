from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from polyarb.control_plane.models import (
    JobLease,
    JobState,
    QuoteBatchLeg,
    QuoteBatchSpec,
)
from polyarb.control_plane.quote_worker import TransactionalQuoteBatchWorker

NOW = datetime(2030, 1, 1, tzinfo=UTC)


class FakeControlPlane:
    def __init__(self, batch: QuoteBatchSpec, *, prior: object | None = None) -> None:
        self.batch = batch
        self.prior = prior
        self.finished: list[JobState] = []
        self.recorded: dict[str, object] | None = None

    def claim_job(self, **kwargs):
        return JobLease(
            job_key=self.batch.job_key,
            job_type="quote-batch",
            input_identity=self.batch.input_identity,
            lease_owner="worker-a",
            lease_epoch=1,
            lease_expires_at=NOW,
            checkpoint_cursor=None,
            checkpoint_digest=None,
        )

    def quote_batch_receipt(self, job_key: str):
        assert job_key == self.batch.job_key
        return self.prior

    def quote_batch_spec(self, job_key: str):
        assert job_key == self.batch.job_key
        return self.batch

    def record_quote_batch(self, lease, **kwargs):
        self.recorded = kwargs

    def finish(self, lease, *, state: JobState, **kwargs):
        self.finished.append(state)


class FakeReader:
    def __init__(self) -> None:
        self.calls = 0

    async def get_books(self, token_ids: list[str], *, projection: str = "full"):
        self.calls += 1
        assert token_ids == ["token-a"]
        assert projection == "full"
        return [{"asset_id": "token-a", "asks": [{"price": "0.41", "size": "20"}]}]


class FakeObjectClient:
    def __init__(self) -> None:
        self.object: dict[str, object] = {}

    def put_object(self, **kwargs):
        self.object = kwargs

    def head_object(self, **kwargs):
        return {
            "ContentLength": len(self.object["Body"]),
            "Metadata": self.object["Metadata"],
        }


def _batch() -> QuoteBatchSpec:
    return QuoteBatchSpec.from_legs(
        structure_receipt_digest="a" * 64,
        universe_hash="b" * 64,
        ordinal=0,
        legs=(
            QuoteBatchLeg(
                neg_risk_market_id="neg-risk-a",
                market_id="market-a",
                condition_id="condition-a",
                slug="market-a",
                yes_token_id="token-a",
                event_id="event-a",
                membership_hash="membership-a",
            ),
        ),
    )


def test_transactional_worker_uploads_then_records_and_finishes() -> None:
    control_plane = FakeControlPlane(_batch())
    reader = FakeReader()
    objects = FakeObjectClient()
    worker = TransactionalQuoteBatchWorker(
        control_plane=control_plane,
        reader=reader,
        object_client=objects,
        bucket="quotes",
        worker_id="worker-a",
        now=lambda: NOW,
    )

    result = asyncio.run(worker.run_once())

    assert result.outcome == "succeeded"
    assert reader.calls == 1
    assert control_plane.recorded is not None
    assert control_plane.recorded["artifact_key"] == objects.object["Key"]
    assert control_plane.recorded["artifact_digest"] == objects.object["Metadata"]["sha256"]
    assert control_plane.finished == [JobState.SUCCEEDED]


def test_transactional_worker_finishes_existing_receipt_without_refetch() -> None:
    control_plane = FakeControlPlane(_batch(), prior=object())
    reader = FakeReader()
    worker = TransactionalQuoteBatchWorker(
        control_plane=control_plane,
        reader=reader,
        object_client=FakeObjectClient(),
        bucket="quotes",
        worker_id="worker-a",
        now=lambda: NOW,
    )

    result = asyncio.run(worker.run_once())

    assert result.outcome == "recovered"
    assert reader.calls == 0
    assert control_plane.recorded is None
    assert control_plane.finished == [JobState.SUCCEEDED]
