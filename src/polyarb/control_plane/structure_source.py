"""Fenced single-page Gamma source collection for transactional Structure."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Any, Protocol

from polyarb.clients.gamma_client import EventPage, MarketPage
from polyarb.config import Settings
from polyarb.perception.market_truth import market_truth_mismatch_reason
from polyarb.snapshot.normalizer import normalize_events, normalize_market

from .alert_delivery import incident_alert_channels
from .models import StructureSourcePageSpec
from .postgres import PostgresControlPlane, StaleLeaseError
from .structure_artifact import (
    StructureBundleArtifact,
    StructureBundleIdentity,
    canonical_structure_bundle_bytes,
    upload_structure_bundle_artifact,
)
from .structure_shadow import plan_structure_ranges
from .structure_worker import StructureWorkerResult

DEFAULT_MAX_MARKET_BATCHES = 10_000


class StructureSourceError(RuntimeError):
    """A source page cannot safely become durable Structure evidence."""


class StructureSourceBatchLimitError(StructureSourceError):
    """A sealed event scope exceeds its configured market-batch capacity."""


class _GammaReader(Protocol):
    async def fetch_active_event_page(self, cursor: str | None, limit: int) -> EventPage: ...

    async def fetch_active_market_page(self, cursor: str | None, limit: int) -> MarketPage: ...

    async def fetch_markets_by_ids(self, market_ids: tuple[str, ...]) -> tuple[dict, ...]: ...

    async def aclose(self) -> None: ...


class _ObjectClient(Protocol):
    def get_object(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def put_object(self, **kwargs: Any) -> Any: ...

    def head_object(self, **kwargs: Any) -> dict[str, Any]: ...


class _SourceLane(Protocol):
    async def run_once(self) -> StructureWorkerResult: ...

    async def aclose(self) -> None: ...


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
    if spec.market_ids:
        header["market_ids"] = list(spec.market_ids)
        header["market_ids_digest"] = spec.market_ids_digest
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
        raw_market_ids = header.get("market_ids", [])
        if not isinstance(raw_market_ids, list) or not all(
            isinstance(market_id, str) for market_id in raw_market_ids
        ):
            raise ValueError("market_ids")
        spec = StructureSourcePageSpec(
            window_key=str(header["window_key"]),
            stream=str(header["stream"]),
            ordinal=int(header["ordinal"]),
            requested_cursor=(
                None if header.get("requested_cursor") is None else str(header["requested_cursor"])
            ),
            market_ids=tuple(raw_market_ids),
        )
        if header.get("market_ids_digest") != spec.market_ids_digest:
            raise ValueError("market_ids_digest")
        next_cursor = None if header.get("next_cursor") is None else str(header["next_cursor"])
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
        if (
            canonical_structure_source_page_bytes(
                spec=spec,
                records=records,
                next_cursor=next_cursor,
                completed=completed,
                started_at_ms=header["started_at_ms"],
                finished_at_ms=header["finished_at_ms"],
            )
            != payload
        ):
            raise ValueError("noncanonical")
    except (IndexError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise StructureSourceError("structure-source-page-malformed") from error
    return spec, tuple(records), next_cursor, completed


def market_batches_from_event_records(
    records: Sequence[dict[str, object]], *, batch_size: int, max_batches: int
) -> tuple[tuple[str, ...], ...]:
    """Freeze open event members into deterministic exact-ID market batches."""
    if batch_size <= 0 or max_batches <= 0:
        raise ValueError("batch_size and max_batches must be positive")
    _, _, market_to_event, _, _ = normalize_events(list(records))
    market_ids = tuple(sorted(market_to_event))
    if not market_ids:
        raise StructureSourceError("sealed events contain no open market members")
    batches = tuple(
        market_ids[start : start + batch_size]
        for start in range(0, len(market_ids), batch_size)
    )
    if len(batches) > max_batches:
        raise StructureSourceBatchLimitError(
            f"event-rooted market batch limit exceeded:{len(batches)}>{max_batches}"
        )
    return batches


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
        max_pages: int = 1_000,
        market_batch_size: int = 25,
        max_market_batches: int = DEFAULT_MAX_MARKET_BATCHES,
        lease_seconds: int = 120,
        terminal_event_timeout_seconds: float = 90,
        object_store_timeout_seconds: float = 90,
        retry_delay: timedelta = timedelta(seconds=15),
    ) -> None:
        if not bucket or not worker_id:
            raise ValueError("bucket and worker_id must be non-empty")
        if not 1 <= page_limit <= 100:
            raise ValueError("page_limit must be within 1..100")
        if max_pages <= 0:
            raise ValueError("max_pages must be positive")
        if market_batch_size <= 0 or max_market_batches <= 0:
            raise ValueError("market batch bounds must be positive")
        if (
            lease_seconds <= 0
            or terminal_event_timeout_seconds <= 0
            or object_store_timeout_seconds <= 0
            or retry_delay.total_seconds() <= 0
        ):
            raise ValueError("source worker time bounds must be positive")
        self._control_plane = control_plane
        self._gamma = gamma
        self._object_client = object_client
        self._bucket = bucket
        self._worker_id = worker_id
        self._now = now
        self._page_limit = page_limit
        self._max_pages = max_pages
        self._market_batch_size = market_batch_size
        self._max_market_batches = max_market_batches
        self._lease_seconds = lease_seconds
        self._terminal_event_timeout_seconds = terminal_event_timeout_seconds
        self._object_store_timeout_seconds = object_store_timeout_seconds
        self._retry_delay = retry_delay

    async def aclose(self) -> None:
        """Release the long-lived Gamma transport when an operator turn ends."""
        await self._gamma.aclose()

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
            # Only cursor-driven event traversal needs this page ceiling.  Exact
            # market-ID batches are independently bounded when the terminal
            # event source admits at most ``max_market_batches`` of them.
            if spec.stream == "events" and spec.ordinal >= self._max_pages:
                self._control_plane.quarantine_structure_source_page(
                    lease,
                    error_class="StructureSourcePageLimitError",
                    now=self._now(),
                )
                return StructureWorkerResult(job_key=lease.job_key, outcome="quarantined")
            artifact, next_cursor, completed, record_count = await self._fetch_artifact(spec)
            market_batches = None
            if spec.stream == "events" and completed:
                # This reads every prior event artifact.  It cannot block the sole
                # scheduler turn indefinitely, or an expired lease becomes unable
                # to reclaim itself on the next tick.  The helper is read-only; a
                # timed-out thread has no durable authority to commit a receipt.
                market_batches = await asyncio.wait_for(
                    asyncio.to_thread(self._market_batches_for_terminal_event, spec, artifact),
                    timeout=self._terminal_event_timeout_seconds,
                )
            self._control_plane.record_structure_source_page(
                lease,
                artifact_key=artifact.key,
                artifact_digest=artifact.sha256,
                next_cursor=next_cursor,
                completed=completed,
                record_count=record_count,
                market_batches=market_batches,
                now=self._now(),
            )
            self._control_plane.record_job_recovery(
                lease,
                component="structure-fetch",
                channels=incident_alert_channels(Settings()),
                now=self._now(),
            )
            return StructureWorkerResult(job_key=lease.job_key, outcome="succeeded")
        except StructureSourceBatchLimitError:
            self._control_plane.quarantine_structure_source_page(
                lease,
                error_class="StructureSourceBatchLimitError",
                now=self._now(),
            )
            return StructureWorkerResult(job_key=lease.job_key, outcome="quarantined")
        except StaleLeaseError:
            raise
        except Exception as error:
            self._control_plane.finish_retryable_with_incident(
                lease,
                error_class=type(error).__name__,
                incident_key=f"incident:job-retry:{lease.job_key}",
                dedupe_key=f"job-retry:{lease.job_key}",
                component="structure-fetch",
                summary="structure-fetch retryable failure",
                detail={
                    "job_key": lease.job_key,
                    "lease_epoch": lease.lease_epoch,
                    "error_class": type(error).__name__,
                },
                channels=incident_alert_channels(Settings()),
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
        elif spec.market_ids:
            started_at_ms = int(self._now().timestamp() * 1_000)
            records = await self._gamma.fetch_markets_by_ids(spec.market_ids)
            finished_at_ms = int(self._now().timestamp() * 1_000)
            artifact = StructureSourcePageArtifact.from_page(
                spec=spec,
                records=records,
                next_cursor=None,
                completed=True,
                started_at_ms=started_at_ms,
                finished_at_ms=finished_at_ms,
            )
            await self._upload_artifact(artifact)
            return artifact, None, True, len(records)
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
        await self._upload_artifact(artifact)
        return artifact, page.next_cursor, page.completed, len(records)

    async def _upload_artifact(self, artifact: StructureSourcePageArtifact) -> None:
        """Keep synchronous R2 PUT/HEAD from freezing the scheduler event loop."""
        await asyncio.wait_for(
            asyncio.to_thread(
                upload_structure_source_page_artifact,
                self._object_client,
                bucket=self._bucket,
                artifact=artifact,
            ),
            timeout=self._object_store_timeout_seconds,
        )

    def _market_batches_for_terminal_event(
        self,
        current_spec: StructureSourcePageSpec,
        current_artifact: StructureSourcePageArtifact,
    ) -> tuple[tuple[str, ...], ...]:
        records: list[dict[str, object]] = []
        pages = self._control_plane.structure_source_event_pages(current_spec.window_key)
        for spec, artifact_key, artifact_digest in pages:
            if spec.stream != "events":
                continue
            response = self._object_client.get_object(Bucket=self._bucket, Key=artifact_key)
            body = response.get("Body")
            if body is None or not hasattr(body, "read"):
                raise StructureSourceError("source event artifact body is unavailable")
            payload = body.read()
            if not isinstance(payload, bytes):
                raise StructureSourceError("source event artifact body is malformed")
            parsed, page_records, _, _ = parse_structure_source_page_bytes(
                payload, expected_sha256=artifact_digest
            )
            if parsed != spec:
                raise StructureSourceError("source event artifact input mismatch")
            records.extend(page_records)
        parsed_current, current_records, _, _ = parse_structure_source_page_bytes(
            current_artifact.payload, expected_sha256=current_artifact.sha256
        )
        if parsed_current != current_spec:
            raise StructureSourceError("current source event artifact input mismatch")
        records.extend(current_records)
        return market_batches_from_event_records(
            records,
            batch_size=self._market_batch_size,
            max_batches=self._max_market_batches,
        )


class TransactionalStructureSourcePool:
    """Bound concurrent exact-ID source work without weakening durable leases."""

    def __init__(self, *, lanes: Sequence[_SourceLane]) -> None:
        if not lanes:
            raise ValueError("lanes must be non-empty")
        self._lanes = tuple(lanes)

    async def run_once(self) -> StructureWorkerResult:
        results = await asyncio.gather(
            *(lane.run_once() for lane in self._lanes), return_exceptions=True
        )
        errors = [result for result in results if isinstance(result, BaseException)]
        if errors:
            raise errors[0]
        completed = [result for result in results if result.job_key is not None]
        if not completed:
            return StructureWorkerResult(job_key=None, outcome="idle")
        keys = sorted(str(result.job_key) for result in completed)
        succeeded = sum(result.outcome == "succeeded" for result in completed)
        outcome = (
            f"succeeded:{succeeded}/{len(self._lanes)}"
            if succeeded == len(completed)
            else f"mixed:{succeeded}/{len(completed)}"
        )
        return StructureWorkerResult(job_key=",".join(keys), outcome=outcome)

    async def aclose(self) -> None:
        await asyncio.gather(*(lane.aclose() for lane in self._lanes))


class TransactionalStructureSourceAdmitter:
    """Durably open one cadence bucket; never inspects local state or Gamma."""

    def __init__(
        self,
        *,
        control_plane: PostgresControlPlane,
        cadence_seconds: int,
        now: Callable[[], datetime],
    ) -> None:
        if isinstance(cadence_seconds, bool) or cadence_seconds <= 0:
            raise ValueError("cadence_seconds must be positive")
        self._control_plane = control_plane
        self._cadence_seconds = cadence_seconds
        self._now = now

    async def run_once(self) -> StructureWorkerResult:
        spec = self._control_plane.admit_due_structure_source_window(
            cadence_seconds=self._cadence_seconds, now=self._now()
        )
        if spec is None:
            return StructureWorkerResult(job_key=None, outcome="idle")
        return StructureWorkerResult(job_key=spec.job_key, outcome="admitted")


def _canonical_json(value: Mapping[str, object]) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


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
            self._control_plane.record_job_recovery(
                lease,
                component="structure-materialize",
                channels=incident_alert_channels(Settings()),
                now=self._now(),
            )
            return StructureWorkerResult(job_key=lease.job_key, outcome="succeeded")
        except StaleLeaseError:
            raise
        except Exception as error:
            self._control_plane.finish_retryable_with_incident(
                lease,
                error_class=type(error).__name__,
                incident_key=f"incident:job-retry:{lease.job_key}",
                dedupe_key=f"job-retry:{lease.job_key}",
                component="structure-materialize",
                summary="structure-materialize retryable failure",
                detail={
                    "job_key": lease.job_key,
                    "lease_epoch": lease.lease_epoch,
                    "error_class": type(error).__name__,
                },
                channels=incident_alert_channels(Settings()),
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
