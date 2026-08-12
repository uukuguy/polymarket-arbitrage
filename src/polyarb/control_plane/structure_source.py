"""Fenced single-page Gamma source collection for transactional Structure."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Protocol

from polyarb.clients.gamma_client import EventPage, MarketPage

from .models import JobState, StructureSourcePageSpec
from .postgres import PostgresControlPlane, StaleLeaseError
from .structure_worker import StructureWorkerResult


class StructureSourceError(RuntimeError):
    """A source page cannot safely become durable Structure evidence."""


class _GammaReader(Protocol):
    async def fetch_active_event_page(self, cursor: str | None, limit: int) -> EventPage: ...

    async def fetch_active_market_page(self, cursor: str | None, limit: int) -> MarketPage: ...


class _ObjectClient(Protocol):
    def put_object(self, **kwargs: Any) -> Any: ...

    def head_object(self, **kwargs: Any) -> dict[str, Any]: ...


@dataclass(frozen=True, slots=True)
class StructureSourcePageArtifact:
    """Immutable raw Gamma page evidence, content-addressed in R2."""

    payload: bytes
    sha256: str
    key: str

    @classmethod
    def from_page(
        cls,
        *,
        spec: StructureSourcePageSpec,
        records: Sequence[Mapping[str, object]],
        next_cursor: str | None,
        completed: bool,
        started_at_ms: int,
        finished_at_ms: int,
    ) -> StructureSourcePageArtifact:
        payload = canonical_structure_source_page_bytes(
            spec=spec,
            records=records,
            next_cursor=next_cursor,
            completed=completed,
            started_at_ms=started_at_ms,
            finished_at_ms=finished_at_ms,
        )
        digest = hashlib.sha256(payload).hexdigest()
        return cls(
            payload=payload,
            sha256=digest,
            key=f"structure-source-pages/{digest}/page.ndjson",
        )


def canonical_structure_source_page_bytes(
    *,
    spec: StructureSourcePageSpec,
    records: Sequence[Mapping[str, object]],
    next_cursor: str | None,
    completed: bool,
    started_at_ms: int,
    finished_at_ms: int,
) -> bytes:
    """Encode one response without interpreting its opaque continuation."""
    if completed and next_cursor is not None:
        raise StructureSourceError("terminal source page names a successor cursor")
    if not completed and not next_cursor:
        raise StructureSourceError("incomplete source page has no successor cursor")
    if isinstance(started_at_ms, bool) or isinstance(finished_at_ms, bool):
        raise StructureSourceError("source page timing must be integer milliseconds")
    if not isinstance(started_at_ms, int) or not isinstance(finished_at_ms, int):
        raise StructureSourceError("source page timing must be integer milliseconds")
    if started_at_ms < 0 or finished_at_ms < started_at_ms:
        raise StructureSourceError("source page timing is invalid")
    if any(not isinstance(row, Mapping) for row in records):
        raise StructureSourceError("source page records must be objects")
    header = {
        "completed": completed,
        "finished_at_ms": finished_at_ms,
        "kind": "structure-source-page",
        "next_cursor": next_cursor,
        "ordinal": spec.ordinal,
        "record_count": len(records),
        "requested_cursor": spec.requested_cursor,
        "started_at_ms": started_at_ms,
        "stream": spec.stream,
        "window_key": spec.window_key,
    }
    return b"".join(
        _canonical_json(record) + b"\n"
        for record in (header, *({"row": dict(row)} for row in records))
    )


def upload_structure_source_page_artifact(
    client: _ObjectClient,
    *,
    bucket: str,
    artifact: StructureSourcePageArtifact,
) -> StructureSourcePageArtifact:
    """PUT then HEAD authenticate raw source evidence before its DB receipt."""
    if not bucket:
        raise ValueError("bucket must be non-empty")
    client.put_object(
        Bucket=bucket,
        Key=artifact.key,
        Body=artifact.payload,
        ContentType="application/x-ndjson",
        Metadata={"sha256": artifact.sha256},
    )
    head = client.head_object(Bucket=bucket, Key=artifact.key)
    if (
        int(head.get("ContentLength", -1)) != len(artifact.payload)
        or str(head.get("Metadata", {}).get("sha256", "")) != artifact.sha256
    ):
        raise StructureSourceError("structure-source-page-head-verification-failed")
    return artifact


class TransactionalStructureSourceWorker:
    """Claim exactly one Gamma page; source evidence never touches SQLite."""

    def __init__(
        self,
        *,
        control_plane: PostgresControlPlane,
        gamma: _GammaReader,
        object_client: _ObjectClient,
        bucket: str,
        worker_id: str,
        now: Callable[[], datetime],
        page_limit: int = 100,
        lease_seconds: int = 120,
        retry_delay: timedelta = timedelta(seconds=15),
    ) -> None:
        if not bucket or not worker_id:
            raise ValueError("bucket and worker_id must be non-empty")
        if not 1 <= page_limit <= 100:
            raise ValueError("page_limit must be within 1..100")
        if lease_seconds <= 0 or retry_delay.total_seconds() <= 0:
            raise ValueError("lease_seconds and retry_delay must be positive")
        self._control_plane = control_plane
        self._gamma = gamma
        self._object_client = object_client
        self._bucket = bucket
        self._worker_id = worker_id
        self._now = now
        self._page_limit = page_limit
        self._lease_seconds = lease_seconds
        self._retry_delay = retry_delay

    async def run_once(self) -> StructureWorkerResult:
        lease = self._control_plane.claim_job(
            worker_id=self._worker_id,
            job_types=("structure-fetch",),
            lease_seconds=self._lease_seconds,
            now=self._now(),
        )
        if lease is None:
            return StructureWorkerResult(job_key=None, outcome="idle")
        try:
            spec = self._control_plane.structure_source_page_spec(lease.job_key)
            artifact, next_cursor, completed, record_count = await self._fetch_artifact(spec)
            self._control_plane.record_structure_source_page(
                lease,
                artifact_key=artifact.key,
                artifact_digest=artifact.sha256,
                next_cursor=next_cursor,
                completed=completed,
                record_count=record_count,
                now=self._now(),
            )
            return StructureWorkerResult(job_key=lease.job_key, outcome="succeeded")
        except StaleLeaseError:
            raise
        except Exception as error:
            self._control_plane.finish(
                lease,
                state=JobState.RETRYABLE,
                next_attempt_at=self._now() + self._retry_delay,
                error_class=type(error).__name__,
                now=self._now(),
            )
            raise

    async def _fetch_artifact(
        self, spec: StructureSourcePageSpec
    ) -> tuple[StructureSourcePageArtifact, str | None, bool, int]:
        page: EventPage | MarketPage
        if spec.stream == "events":
            page = await self._gamma.fetch_active_event_page(
                spec.requested_cursor, self._page_limit
            )
            records = page.events
        else:
            page = await self._gamma.fetch_active_market_page(
                spec.requested_cursor, self._page_limit
            )
            records = page.markets
        if page.requested_cursor != spec.requested_cursor:
            raise StructureSourceError("source page requested cursor mismatch")
        artifact = StructureSourcePageArtifact.from_page(
            spec=spec,
            records=records,
            next_cursor=page.next_cursor,
            completed=page.completed,
            started_at_ms=page.started_at_ms,
            finished_at_ms=page.finished_at_ms,
        )
        upload_structure_source_page_artifact(
            self._object_client, bucket=self._bucket, artifact=artifact
        )
        return artifact, page.next_cursor, page.completed, len(records)


def _canonical_json(value: Mapping[str, object]) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()
