"""Fenced single-page Gamma source collection for transactional Structure."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Any, Protocol

from polyarb.clients.gamma_client import EventPage, MarketPage, PaginationIntegrityError
from polyarb.config import Settings
from polyarb.perception.market_truth import market_truth_mismatch_reason
from polyarb.snapshot.normalizer import normalize_events, normalize_market

from .alert_delivery import incident_alert_channels
from .models import JobLease, StructureSourcePageSpec
from .postgres import PostgresControlPlane, StaleLeaseError
from .structure_artifact import (
    StructureBundleArtifact,
    StructureBundleIdentity,
    StructureShardArtifact,
    StructureShardBatchArtifact,
    StructureShardReceipt,
    canonical_structure_bundle_bytes,
    canonical_structure_shard_batch_bytes,
    canonical_structure_shard_bytes,
    canonical_structure_shard_manifest_bytes,
    parse_structure_shard_batch_bytes,
    parse_structure_shard_bytes,
    upload_structure_bundle_artifact,
    upload_structure_shard_artifact,
    upload_structure_shard_batch_artifact,
)
from .structure_shadow import plan_shard_structure_ranges, plan_structure_ranges
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


def _event_embedded_market_records(
    event_records: Sequence[dict[str, object]],
) -> list[dict[str, object]]:
    """Expand market payloads sealed inside Gamma event evidence.

    Gamma's event response carries the market fields required for Structure,
    while the neg-risk group identity belongs to the enclosing event.  Copying
    that identity into the child record is deterministic and keeps the entire
    Structure source inside one immutable event-page chain.  We deliberately
    reject malformed nesting rather than inventing a second mutable lookup.
    """
    market_records: list[dict[str, object]] = []
    for event in event_records:
        markets = event.get("markets")
        if not isinstance(markets, list):
            continue
        group_id = event.get("negRiskMarketID")
        is_group_less_standard_neg_risk = (
            event.get("negRisk") is True
            and event.get("enableNegRisk") is True
            and event.get("negRiskAugmented") is False
            and group_id is None
        )
        for market in markets:
            if not isinstance(market, dict):
                raise StructureSourceError("event embedded market is malformed")
            # Gamma keeps closed historical children in an otherwise active
            # event response. The v1 companion /markets stream was active
            # only, so retaining those children would change the published
            # market contract and can introduce partial fields (notably
            # ``negRisk``). Match the source contract before normalization.
            if market.get("active") is not True or market.get("closed") is not False:
                continue
            # Gamma can explicitly call a parent standard neg-risk while
            # omitting its group ID, and repeat that unprovable claim on an
            # active child.  The legacy snapshot path quarantined precisely
            # this source shape.  Keep the same fail-closed contract here:
            # exclude the child, never infer or synthesize a group identity.
            if (
                is_group_less_standard_neg_risk
                and market.get("negRisk") is True
                and market.get("negRiskMarketID") is None
            ):
                continue
            enriched = dict(market)
            if "negRiskMarketID" not in enriched and group_id is not None:
                enriched["negRiskMarketID"] = group_id
            # Nested Gamma children omit ``negRisk`` when it is false, unlike
            # the active /markets stream. A parent group is the only durable
            # event-side fact that makes the default true; otherwise preserve
            # the active-stream semantics as explicit false.
            if "negRisk" not in enriched:
                enriched["negRisk"] = group_id is not None
            market_records.append(enriched)
    return market_records


def materialize_event_records_components(
    event_records: Sequence[dict[str, object]],
) -> dict[str, tuple[dict[str, object], ...]]:
    """Normalize one sealed event-page batch into independent v3 shard rows."""
    try:
        event_rows, event_tags, market_to_event, members, group_truths = normalize_events(
            list(event_records)
        )
        market_rows: list[dict[str, object]] = []
        for raw in _event_embedded_market_records(event_records):
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
    return {
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


def materialize_event_page_shards(
    page: tuple[StructureSourcePageSpec, StructureSourcePageArtifact], *, source_digest: str
) -> tuple[tuple[str, StructureShardArtifact], ...]:
    """Turn one sealed terminal event page into bounded component artifacts."""
    spec, artifact = page
    if spec.stream != "events" or len(source_digest) != 64:
        raise ValueError("event page shards require a source digest and event spec")
    parsed_spec, records, _next_cursor, _completed = parse_structure_source_page_bytes(
        artifact.payload, expected_sha256=artifact.sha256
    )
    if parsed_spec != spec:
        raise StructureSourceError("source page input/header mismatch")
    components = materialize_event_records_components(records)
    return tuple(
        (
            component,
            StructureShardArtifact.from_bytes(
                canonical_structure_shard_bytes(
                    window_key=spec.window_key,
                    source_digest=source_digest,
                    component=component,
                    ordinal=spec.ordinal,
                    rows=rows,
                )
            ),
        )
        for component, rows in components.items()
        if rows
    )


def materialize_sharded_source_manifest(
    *,
    window_key: str,
    source_digest: str,
    expected_page_count: int,
    batches: Sequence[tuple[str, str, str]],
    read_batch: Callable[[str], bytes],
) -> tuple[StructureBundleIdentity, StructureBundleArtifact, tuple[tuple[str, str, str], ...]]:
    """Build v3 admission input only from fenced, authenticated batch receipts."""
    if not window_key or len(source_digest) != 64 or expected_page_count <= 0:
        raise ValueError("invalid sharded source manifest identity")
    next_ordinal = 0
    shards: list[StructureShardReceipt] = []
    for checkpoint_cursor, digest, key in batches:
        if not checkpoint_cursor.startswith("shard-batch:"):
            raise StructureSourceError("materializer checkpoint cursor is invalid")
        header, batch_shards = parse_structure_shard_batch_bytes(
            read_batch(key), expected_sha256=digest
        )
        batch_window, batch_source_digest, start, end = header
        if (
            batch_window != window_key
            or batch_source_digest != source_digest
            or start != next_ordinal
            or end > expected_page_count
        ):
            raise StructureSourceError("materializer batch receipt chain is invalid")
        next_ordinal = end
        shards.extend(batch_shards)
    if next_ordinal != expected_page_count or not shards:
        raise StructureSourceError("materializer batch receipts are incomplete")
    if len({(shard.component, shard.ordinal) for shard in shards}) != len(shards):
        raise StructureSourceError("materializer shards conflict")
    components = ("events", "event_tags", "memberships", "group_truth", "markets", "issues")
    counts = {component: 0 for component in components}
    for shard in shards:
        counts[shard.component] += shard.row_count
    identity = StructureBundleIdentity(
        publication_id=f"source-window:{window_key}",
        window_id=window_key,
        snapshot_id=0,
        comparison_receipt_digest=source_digest,
        normalization_contract_version="gamma-source-window-events-v3-sharded",
        component_counts=counts,
        source_kind="gamma-source-window-events-v3-sharded",
    )
    manifest = StructureBundleArtifact.from_bytes(
        canonical_structure_shard_manifest_bytes(identity=identity, shards=shards)
    )
    return identity, manifest, plan_shard_structure_ranges(shards)


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
        if not ordered and stream == "markets":
            continue
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
    if "markets" not in raw_streams:
        components = materialize_event_records_components(raw_streams["events"])
    else:
        try:
            event_rows, event_tags, market_to_event, members, group_truths = normalize_events(
                raw_streams["events"]
            )
            market_rows = []
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
        components = {
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
        normalization_contract_version=(
            "gamma-source-window-v1"
            if "markets" in raw_streams
            else "gamma-source-window-events-v2"
        ),
        component_counts={component: len(rows) for component, rows in components.items()},
        source_kind=(
            "gamma-source-window-v1"
            if "markets" in raw_streams
            else "gamma-source-window-events-v2"
        ),
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
        object_store_timeout_seconds: float = 90,
        retry_delay: timedelta = timedelta(seconds=15),
        daily_egress_budget_bytes: int = 3_500_000_000,
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
            or object_store_timeout_seconds <= 0
            or retry_delay.total_seconds() <= 0
            or daily_egress_budget_bytes <= 0
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
        self._object_store_timeout_seconds = object_store_timeout_seconds
        self._retry_delay = retry_delay
        self._daily_egress_budget_bytes = daily_egress_budget_bytes

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
            decision = self._control_plane.record_cloud_usage(
                source="gamma", operation=f"structure-{spec.stream}-page",
                bytes_received=len(artifact.payload), item_count=record_count,
                artifact_key=artifact.key, artifact_digest=artifact.sha256,
                daily_budget_bytes=self._daily_egress_budget_bytes, now=self._now(),
            )
            if not decision.allowed:
                raise StructureSourceError("cloud-egress-budget-exhausted")
            event_embedded_markets = spec.stream == "events" and completed
            self._control_plane.record_structure_source_page(
                lease,
                artifact_key=artifact.key,
                artifact_digest=artifact.sha256,
                next_cursor=next_cursor,
                completed=completed,
                record_count=record_count,
                event_embedded_markets=event_embedded_markets,
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
            # A frozen market-ID set may contain a member that closes before
            # its exact batch is fetched.  Gamma explicitly says this response
            # is no longer open, so retrying cannot produce the same coherent
            # source window. Quarantine immediately for that explicit state;
            # other exact-batch integrity errors retain two retry attempts and
            # then quarantine on the third lease. A later admission can take a
            # fresh, internally consistent scope. Event and non-integrity
            # failures retain the normal retry/incident path below.
            exact_batch_integrity_failure = (
                isinstance(error, PaginationIntegrityError) and bool(spec.market_ids)
            )
            if exact_batch_integrity_failure and (
                str(error) == "exact-id market response is not open" or lease.lease_epoch >= 3
            ):
                self._control_plane.quarantine_structure_source_page(
                    lease,
                    error_class=(
                        "StructureSourceMemberBecameInactiveError"
                        if str(error) == "exact-id market response is not open"
                        else "StructureSourceExactBatchIntegrityError"
                    ),
                    now=self._now(),
                )
                return StructureWorkerResult(job_key=lease.job_key, outcome="quarantined")
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
            # The durable retryable receipt is the failure signal.  Re-raising
            # would terminate the whole scheduler service and prevent sibling
            # lanes and downstream transactional work from making progress.
            return StructureWorkerResult(job_key=lease.job_key, outcome="retryable")

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
        structure_high_water: int = 2_000,
        quote_high_water: int = 512,
        now: Callable[[], datetime],
    ) -> None:
        if (
            isinstance(cadence_seconds, bool)
            or cadence_seconds <= 0
            or isinstance(structure_high_water, bool)
            or structure_high_water <= 0
            or isinstance(quote_high_water, bool)
            or quote_high_water <= 0
        ):
            raise ValueError("source admission bounds must be positive")
        self._control_plane = control_plane
        self._cadence_seconds = cadence_seconds
        self._structure_high_water = structure_high_water
        self._quote_high_water = quote_high_water
        self._now = now

    async def run_once(self) -> StructureWorkerResult:
        decision = self._control_plane.admit_due_structure_source_window(
            cadence_seconds=self._cadence_seconds,
            structure_high_water=self._structure_high_water,
            quote_high_water=self._quote_high_water,
            now=self._now(),
        )
        return StructureWorkerResult(job_key=decision.job_key, outcome=decision.state)


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
        read_concurrency: int = 8,
        shard_page_batch_size: int = 4,
        retry_delay: timedelta = timedelta(seconds=15),
    ) -> None:
        if not bucket or not worker_id:
            raise ValueError("bucket and worker_id must be non-empty")
        if (
            range_max_rows <= 0
            or lease_seconds <= 0
            or read_concurrency <= 0
            or shard_page_batch_size <= 0
            or retry_delay.total_seconds() <= 0
        ):
            raise ValueError("materializer bounds must be positive")
        self._control_plane = control_plane
        self._object_client = object_client
        self._bucket = bucket
        self._worker_id = worker_id
        self._now = now
        self._range_max_rows = range_max_rows
        self._lease_seconds = lease_seconds
        self._read_concurrency = read_concurrency
        self._shard_page_batch_size = shard_page_batch_size
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
            source_pages = self._control_plane.structure_source_window_pages(lease.input_identity)
            if len(source_pages) > self._shard_page_batch_size and all(
                spec.stream == "events" for spec, _key, _digest in source_pages
            ):
                return await self._checkpoint_event_shard_batch(lease, source_pages)
            pages = await self._read_source_pages(source_pages)
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
                    "error_message": str(error)[:200],
                },
                channels=incident_alert_channels(Settings()),
                now=self._now(),
            )
            return StructureWorkerResult(job_key=lease.job_key, outcome="retryable")

    async def _checkpoint_event_shard_batch(
        self,
        lease: JobLease,
        source_pages: Sequence[tuple[StructureSourcePageSpec, str, str]],
    ) -> StructureWorkerResult:
        # Checkpoints use fixed-width offsets so lexical receipt order remains
        # the durable source order. A fresh lease begins at zero.
        checkpoint_cursor = getattr(lease, "checkpoint_cursor")
        start = (
            0
            if checkpoint_cursor is None
            else int(str(checkpoint_cursor).split(":", 1)[1]) + 1
        )
        selected = source_pages[start : start + self._shard_page_batch_size]
        if not selected:
            return await self._finalize_event_shard_manifest(lease, source_pages)
        pages = await self._read_source_pages(selected)
        source_digest = self._control_plane.structure_source_window_digest(lease.input_identity)
        receipts: list[StructureShardReceipt] = []
        for page in pages:
            page_shards = materialize_event_page_shards(
                page, source_digest=source_digest
            )
            for component, artifact in page_shards:
                await asyncio.to_thread(
                    upload_structure_shard_artifact,
                    self._object_client,
                    bucket=self._bucket,
                    artifact=artifact,
                )
                receipts.append(
                    StructureShardReceipt(
                        component=component,
                        ordinal=page[0].ordinal,
                        artifact_key=artifact.key,
                        artifact_digest=artifact.sha256,
                        row_count=len(
                            parse_structure_shard_bytes(
                                artifact.payload, expected_sha256=artifact.sha256
                            )[1]
                        ),
                    )
                )
        batch = StructureShardBatchArtifact.from_bytes(
            canonical_structure_shard_batch_bytes(
                window_key=lease.input_identity,
                source_digest=source_digest,
                start_ordinal=selected[0][0].ordinal,
                end_ordinal=selected[-1][0].ordinal + 1,
                shards=receipts,
            )
        )
        await asyncio.to_thread(
            upload_structure_shard_batch_artifact,
            self._object_client,
            bucket=self._bucket,
            artifact=batch,
        )
        self._control_plane.checkpoint(
            lease,
            checkpoint_cursor=f"shard-batch:{selected[-1][0].ordinal:08d}",
            checkpoint_digest=batch.sha256,
            artifact_key=batch.key,
            idempotency_key=(
                f"structure-materializer:{lease.job_key}:{selected[-1][0].ordinal}:{batch.sha256}"
            ),
            now=self._now(),
        )
        self._control_plane.record_job_recovery(
            lease,
            component="structure-materialize",
            channels=incident_alert_channels(Settings()),
            now=self._now(),
        )
        return StructureWorkerResult(job_key=lease.job_key, outcome="checkpointed")

    async def _finalize_event_shard_manifest(
        self,
        lease: JobLease,
        source_pages: Sequence[tuple[StructureSourcePageSpec, str, str]],
    ) -> StructureWorkerResult:
        source_digest = self._control_plane.structure_source_window_digest(lease.input_identity)
        identity, manifest, ranges = materialize_sharded_source_manifest(
            window_key=lease.input_identity,
            source_digest=source_digest,
            expected_page_count=len(source_pages),
            batches=self._control_plane.structure_materializer_batches(lease.input_identity),
            read_batch=lambda key: self._read_object_bytes(key),
        )
        await asyncio.to_thread(
            upload_structure_bundle_artifact,
            self._object_client,
            bucket=self._bucket,
            artifact=manifest,
        )
        self._control_plane.admit_structure_source_bundle(
            lease,
            identity=identity,
            bundle=manifest,
            ranges=ranges,
            now=self._now(),
        )
        self._control_plane.record_job_recovery(
            lease,
            component="structure-materialize",
            channels=incident_alert_channels(Settings()),
            now=self._now(),
        )
        return StructureWorkerResult(job_key=lease.job_key, outcome="succeeded")

    async def _read_source_pages(
        self,
        source_pages: Sequence[tuple[StructureSourcePageSpec, str, str]],
    ) -> list[tuple[StructureSourcePageSpec, StructureSourcePageArtifact]]:
        """Read immutable page evidence concurrently without changing its order."""
        semaphore = asyncio.Semaphore(self._read_concurrency)

        async def read_one(
            spec: StructureSourcePageSpec, key: str, digest: str
        ) -> tuple[StructureSourcePageSpec, StructureSourcePageArtifact]:
            async with semaphore:
                artifact = await asyncio.to_thread(
                    self._read_page_artifact, key=key, digest=digest
                )
            return spec, artifact

        return list(
            await asyncio.gather(
                *(read_one(spec, key, digest) for spec, key, digest in source_pages)
            )
        )

    def _read_page_artifact(self, *, key: str, digest: str) -> StructureSourcePageArtifact:
        payload = self._read_object_bytes(key)
        return StructureSourcePageArtifact(payload=payload, sha256=digest, key=key)

    def _read_object_bytes(self, key: str) -> bytes:
        response = self._object_client.get_object(Bucket=self._bucket, Key=key)
        body = response.get("Body")
        if body is None or not hasattr(body, "read"):
            raise StructureSourceError("source page artifact body unavailable")
        payload = body.read()
        if not isinstance(payload, bytes):
            raise StructureSourceError("source page artifact body is not bytes")
        return payload


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
