"""Fenced single-page Gamma source collection for transactional Structure."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Any, Protocol

from polyarb.clients.gamma_client import EventPage, MarketPage
from polyarb.perception.market_truth import market_truth_mismatch_reason
from polyarb.snapshot.normalizer import normalize_events, normalize_market

from .models import JobState, StructureSourcePageSpec
from .postgres import PostgresControlPlane, StaleLeaseError
from .structure_artifact import (
    StructureBundleArtifact,
    StructureBundleIdentity,
    canonical_structure_bundle_bytes,
    upload_structure_bundle_artifact,
)
from .structure_shadow import plan_structure_ranges
from .structure_worker import StructureWorkerResult


class StructureSourceError(RuntimeError):
    """A source page cannot safely become durable Structure evidence."""


class _GammaReader(Protocol):
    async def fetch_active_event_page(self, cursor: str | None, limit: int) -> EventPage: ...

    async def fetch_active_market_page(self, cursor: str | None, limit: int) -> MarketPage: ...


class _ObjectClient(Protocol):
    def get_object(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def put_object(self, **kwargs: Any) -> Any: ...

    def head_object(self, **kwargs: Any) -> dict[str, Any]: ...


type _DecodedSourcePage = tuple[
    StructureSourcePageSpec,
    tuple[dict[str, object], ...],
    str | None,
    bool,
    str,
]


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


def parse_structure_source_page_bytes(
    payload: bytes,
    *,
    expected_sha256: str,
) -> tuple[StructureSourcePageSpec, tuple[dict[str, object], ...], str | None, bool]:
    """Re-authenticate and decode a source artifact before materialization."""
    if hashlib.sha256(payload).hexdigest() != expected_sha256:
        raise StructureSourceError("structure-source-page-digest-mismatch")
    try:
        lines = [json.loads(line) for line in payload.splitlines()]
        header = lines[0]
        if not isinstance(header, dict) or header.get("kind") != "structure-source-page":
            raise ValueError("header")
        spec = StructureSourcePageSpec(
            window_key=str(header["window_key"]),
            stream=str(header["stream"]),
            ordinal=int(header["ordinal"]),
            requested_cursor=(
                None if header.get("requested_cursor") is None else str(header["requested_cursor"])
            ),
        )
        next_cursor = (
            None if header.get("next_cursor") is None else str(header["next_cursor"])
        )
        completed = header.get("completed")
        if type(completed) is not bool:
            raise ValueError("completed")
        records: list[dict[str, object]] = []
        for record in lines[1:]:
            if not isinstance(record, dict) or set(record) != {"row"}:
                raise ValueError("record")
            row = record["row"]
            if not isinstance(row, dict):
                raise ValueError("row")
            records.append(row)
        if header.get("record_count") != len(records):
            raise ValueError("record_count")
        if canonical_structure_source_page_bytes(
            spec=spec,
            records=records,
            next_cursor=next_cursor,
            completed=completed,
            started_at_ms=header["started_at_ms"],
            finished_at_ms=header["finished_at_ms"],
        ) != payload:
            raise ValueError("noncanonical")
    except (IndexError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise StructureSourceError("structure-source-page-malformed") from error
    return spec, tuple(records), next_cursor, completed


def materialize_structure_source_pages(
    pages: Sequence[tuple[StructureSourcePageSpec, StructureSourcePageArtifact]],
) -> StructureBundleArtifact:
    """Build a fail-closed six-component bundle using only sealed page evidence."""
    if not pages:
        raise StructureSourceError("source window has no pages")
    decoded: dict[str, list[_DecodedSourcePage]] = {
        "events": [],
        "markets": [],
    }
    window_key: str | None = None
    for external_spec, artifact in pages:
        spec, records, next_cursor, completed = parse_structure_source_page_bytes(
            artifact.payload, expected_sha256=artifact.sha256
        )
        if spec != external_spec:
            raise StructureSourceError("source page input/header mismatch")
        if window_key is None:
            window_key = spec.window_key
        elif spec.window_key != window_key:
            raise StructureSourceError("source pages name different windows")
        decoded[spec.stream].append((spec, records, next_cursor, completed, artifact.sha256))
    if window_key is None:
        raise StructureSourceError("source window identity unavailable")
    raw_streams: dict[str, list[dict[str, object]]] = {}
    source_receipts: list[dict[str, object]] = []
    for stream in ("events", "markets"):
        ordered = sorted(decoded[stream], key=lambda item: item[0].ordinal)
        if not ordered:
            raise StructureSourceError(f"source stream unavailable:{stream}")
        rows: list[dict[str, object]] = []
        for ordinal, (spec, records, next_cursor, completed, digest) in enumerate(ordered):
            if spec.ordinal != ordinal:
                raise StructureSourceError("source page ordinal gap")
            if ordinal + 1 < len(ordered):
                successor = ordered[ordinal + 1][0]
                if completed or next_cursor != successor.requested_cursor:
                    raise StructureSourceError("source page cursor chain is invalid")
            elif not completed or next_cursor is not None:
                raise StructureSourceError("source stream terminal receipt is invalid")
            rows.extend(records)
            source_receipts.append(
                {"stream": stream, "ordinal": ordinal, "artifact_digest": digest}
            )
        raw_streams[stream] = rows
    source_digest = hashlib.sha256(_canonical_json({"pages": source_receipts})).hexdigest()
    try:
        event_rows, event_tags, market_to_event, members, group_truths = normalize_events(
            raw_streams["events"]
        )
        market_rows: list[dict[str, object]] = []
        for raw in raw_streams["markets"]:
            normalized = normalize_market(raw, market_to_event)
            if normalized is None:
                raise StructureSourceError("source market normalization failed")
            market_rows.append(normalized)
        mismatch = market_truth_mismatch_reason(members, group_truths, market_rows)
        if mismatch is not None:
            raise StructureSourceError(f"source market truth mismatch:{mismatch}")
    except StructureSourceError:
        raise
    except Exception as error:
        raise StructureSourceError("source page normalization failed") from error
    components: dict[str, tuple[dict[str, object], ...]] = {
        "events": tuple(event_rows),
        "event_tags": tuple(event_tags),
        "memberships": tuple(
            {
                "event_id": member.event_id,
                "neg_risk_market_id": member.group_id,
                "market_id": member.market_id,
                "member_kind": member.member_kind,
                "active": member.active,
                "closed": member.closed,
            }
            for member in members
        ),
        "group_truth": tuple(
            {
                "event_id": truth.event_id,
                "neg_risk_market_id": truth.group_id,
                **asdict(truth),
            }
            for truth in group_truths
        ),
        "markets": tuple(market_rows),
        "issues": (),
    }
    identity = StructureBundleIdentity(
        publication_id=f"source-window:{window_key}",
        window_id=window_key,
        snapshot_id=0,
        comparison_receipt_digest=source_digest,
        normalization_contract_version="gamma-source-window-v1",
        component_counts={component: len(rows) for component, rows in components.items()},
        source_kind="gamma-source-window-v1",
    )
    return StructureBundleArtifact.from_bytes(
        canonical_structure_bundle_bytes(identity=identity, components=components)
    )


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


class TransactionalStructureSourceMaterializer:
    """Turn one terminal source window into fenced Structure range work."""

    def __init__(
        self,
        *,
        control_plane: PostgresControlPlane,
        object_client: _ObjectClient,
        bucket: str,
        worker_id: str,
        now: Callable[[], datetime],
        range_max_rows: int,
        lease_seconds: int = 120,
        retry_delay: timedelta = timedelta(seconds=15),
    ) -> None:
        if not bucket or not worker_id:
            raise ValueError("bucket and worker_id must be non-empty")
        if range_max_rows <= 0 or lease_seconds <= 0 or retry_delay.total_seconds() <= 0:
            raise ValueError("materializer bounds must be positive")
        self._control_plane = control_plane
        self._object_client = object_client
        self._bucket = bucket
        self._worker_id = worker_id
        self._now = now
        self._range_max_rows = range_max_rows
        self._lease_seconds = lease_seconds
        self._retry_delay = retry_delay

    async def run_once(self) -> StructureWorkerResult:
        lease = self._control_plane.claim_job(
            worker_id=self._worker_id,
            job_types=("structure-materialize",),
            lease_seconds=self._lease_seconds,
            now=self._now(),
        )
        if lease is None:
            return StructureWorkerResult(job_key=None, outcome="idle")
        try:
            pages = tuple(
                (spec, self._read_page_artifact(key=key, digest=digest))
                for spec, key, digest in self._control_plane.structure_source_window_pages(
                    lease.input_identity
                )
            )
            bundle = materialize_structure_source_pages(pages)
            upload_structure_bundle_artifact(
                self._object_client, bucket=self._bucket, artifact=bundle
            )
            self._control_plane.admit_structure_source_bundle(
                lease,
                identity=parse_structure_bundle_identity(bundle),
                bundle=bundle,
                ranges=plan_structure_ranges(
                    parse_structure_bundle_components(bundle), max_rows=self._range_max_rows
                ),
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

    def _read_page_artifact(self, *, key: str, digest: str) -> StructureSourcePageArtifact:
        response = self._object_client.get_object(Bucket=self._bucket, Key=key)
        body = response.get("Body")
        if body is None or not hasattr(body, "read"):
            raise StructureSourceError("source page artifact body unavailable")
        payload = body.read()
        if not isinstance(payload, bytes):
            raise StructureSourceError("source page artifact body is not bytes")
        return StructureSourcePageArtifact(payload=payload, sha256=digest, key=key)


def parse_structure_bundle_identity(bundle: StructureBundleArtifact) -> StructureBundleIdentity:
    """Avoid trusting an in-memory identity after R2 upload preparation."""
    from .structure_artifact import parse_structure_bundle_bytes

    identity, _components = parse_structure_bundle_bytes(
        bundle.payload, expected_sha256=bundle.sha256
    )
    return identity


def parse_structure_bundle_components(
    bundle: StructureBundleArtifact,
) -> dict[str, tuple[dict[str, object], ...]]:
    """Return authenticated components for deterministic range planning only."""
    from .structure_artifact import parse_structure_bundle_bytes

    _identity, components = parse_structure_bundle_bytes(
        bundle.payload, expected_sha256=bundle.sha256
    )
    return components
