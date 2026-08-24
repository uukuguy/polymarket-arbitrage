"""Bridge certified Structure bundles into frozen transactional Quote inputs."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from datetime import datetime, timedelta
from hashlib import sha256
from typing import Any, Protocol

from polyarb.config import Settings

from .alert_delivery import incident_alert_channels
from .models import QuoteBatchLeg, QuoteBatchSpec
from .postgres import PostgresControlPlane, StaleLeaseError
from .quote_artifact import QuoteBatchInputArtifact, upload_quote_batch_artifact
from .quote_worker import QuoteBatchWorkerResult
from .runtime_contract import AsyncAttemptRuntime
from .runtime_models import RuntimeDeadlineProfile
from .structure_artifact import (
    StructureBundleError,
    parse_structure_bundle_bytes,
    parse_structure_shard_bytes,
    parse_structure_shard_manifest_bytes,
)


class QuoteAdmissionError(RuntimeError):
    """A certified Structure bundle cannot safely freeze Quote work."""


_LOGGER = logging.getLogger(__name__)


async def _drain_thread_task(task: asyncio.Task[Any]) -> Any:
    """Await a worker thread to completion, even while cancellation is pending."""
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            continue
    return task.result()


async def _to_thread(call: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Run blocking work without abandoning its thread when the owner cancels."""
    task = asyncio.create_task(asyncio.to_thread(call, *args, **kwargs))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError as cancellation:
        try:
            await _drain_thread_task(task)
        except BaseException as error:
            # Retrieve the underlying exception so the executor task is never
            # left unobserved, while preserving the caller's cancellation.
            raise cancellation from error
        raise


def _consume_cancellation() -> None:
    """Consume cancellation already delivered to a point-of-no-return owner."""
    task = asyncio.current_task()
    if task is None:
        return
    while task.cancelling():
        task.uncancel()


async def _terminal_to_thread(call: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Drain a terminal DB call and return committed success after cancellation.

    Terminal admission is the point of no return: once cancellation reaches
    the owner, the database call is allowed to finish.  A committed result
    wins over the cancellation so the scheduler cannot report a timeout for
    work that is already durable; a real terminal error remains authoritative.
    """
    task = asyncio.create_task(asyncio.to_thread(call, *args, **kwargs))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError as cancellation:
        _consume_cancellation()
        try:
            result = await _drain_thread_task(task)
        except BaseException as error:
            _consume_cancellation()
            raise error from cancellation
        _consume_cancellation()
        return result


def _runtime_profile(lease_seconds: int) -> RuntimeDeadlineProfile:
    """Match the Plan 01 derived profile without coupling to a private helper."""
    bounded_lease = max(3, int(lease_seconds))
    heartbeat = max(1, min(30, bounded_lease // 3))
    progress = max(bounded_lease, heartbeat * 3)
    attempt = max(progress, bounded_lease * 10)
    return RuntimeDeadlineProfile(
        policy_version="runtime-v1",
        lease_seconds=bounded_lease,
        heartbeat_seconds=heartbeat,
        progress_seconds=progress,
        attempt_seconds=attempt,
    )


class _ObjectClient(Protocol):
    def get_object(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def put_object(self, **kwargs: Any) -> Any: ...

    def head_object(self, **kwargs: Any) -> dict[str, Any]: ...


def quote_legs_from_structure_components(
    components: Mapping[str, Sequence[Mapping[str, object]]],
) -> tuple[QuoteBatchLeg, ...]:
    """Select the exact CLOB YES legs eligible in one immutable Structure truth."""
    return quote_legs_from_market_rows(components.get("markets", ()))


def quote_legs_from_market_rows(
    markets: Sequence[Mapping[str, object]], *, require_nonempty: bool = True
) -> tuple[QuoteBatchLeg, ...]:
    """Select Quote legs from one bounded market shard or legacy component."""
    legs: list[QuoteBatchLeg] = []
    for market in markets:
        if (
            market.get("active") is not True
            or market.get("closed") is not False
            or market.get("neg_risk") is not True
        ):
            continue
        values = {
            field: market.get(field)
            for field in (
                "neg_risk_market_id",
                "market_id",
                "condition_id",
                "yes_token_id",
                "event_id",
            )
        }
        if any(not isinstance(value, str) or not value.strip() for value in values.values()):
            continue
        slug = market.get("slug")
        if slug is not None and (not isinstance(slug, str) or not slug.strip()):
            slug = None
        legs.append(
            QuoteBatchLeg(
                neg_risk_market_id=str(values["neg_risk_market_id"]),
                market_id=str(values["market_id"]),
                condition_id=str(values["condition_id"]),
                slug=slug,
                yes_token_id=str(values["yes_token_id"]),
                event_id=str(values["event_id"]),
                membership_hash=_membership_hash(market),
            )
        )
    by_token = {leg.yes_token_id: leg for leg in legs}
    if len(by_token) != len(legs):
        raise QuoteAdmissionError("Structure bundle has duplicate YES token")
    if require_nonempty and not by_token:
        raise QuoteAdmissionError("Structure bundle has no eligible Quote legs")
    return tuple(by_token[token] for token in sorted(by_token))


def canonical_quote_universe_hash(legs: Sequence[QuoteBatchLeg]) -> str:
    """Bind every CLOB leg mapping, not only the observable token list."""
    if not legs:
        raise QuoteAdmissionError("Quote universe is empty")
    payload = [
        {
            "neg_risk_market_id": leg.neg_risk_market_id,
            "market_id": leg.market_id,
            "condition_id": leg.condition_id,
            "slug": leg.slug,
            "yes_token_id": leg.yes_token_id,
            "event_id": leg.event_id,
            "membership_hash": leg.membership_hash,
        }
        for leg in sorted(legs, key=lambda leg: leg.yes_token_id)
    ]
    return sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def quote_batches_from_legs(
    *,
    structure_receipt_digest: str,
    universe_hash: str,
    legs: Sequence[QuoteBatchLeg],
    batch_size: int,
) -> tuple[QuoteBatchSpec, ...]:
    """Build the exact deterministic batches before their R2 inputs are published."""
    normalized = tuple(sorted(legs, key=lambda leg: leg.yes_token_id))
    if not normalized or batch_size <= 0:
        raise QuoteAdmissionError("Quote batch inputs must be non-empty and bounded")
    if len({leg.yes_token_id for leg in normalized}) != len(normalized):
        raise QuoteAdmissionError("Quote batch inputs have duplicate YES token")
    return tuple(
        QuoteBatchSpec.from_legs(
            structure_receipt_digest=structure_receipt_digest,
            universe_hash=universe_hash,
            ordinal=ordinal,
            legs=normalized[start : start + batch_size],
        )
        for ordinal, start in enumerate(range(0, len(normalized), batch_size))
    )


def _membership_hash(market: Mapping[str, object]) -> str:
    return sha256(
        json.dumps(
            {
                "event_id": market["event_id"],
                "neg_risk_market_id": market["neg_risk_market_id"],
                "market_id": market["market_id"],
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


class TransactionalQuoteAdmitter:
    """Claim one certified Structure intent and atomically freeze Quote batches."""

    def __init__(
        self,
        *,
        control_plane: PostgresControlPlane,
        object_client: _ObjectClient,
        bucket: str,
        worker_id: str,
        now: Callable[[], datetime],
        batch_size: int,
        lease_seconds: int = 120,
        retry_delay: timedelta = timedelta(seconds=15),
        runtime_sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        if not bucket or not worker_id or batch_size <= 0 or lease_seconds <= 0:
            raise ValueError("Quote admission bounds and identities must be positive")
        self._control_plane = control_plane
        self._object_client = object_client
        self._bucket = bucket
        self._worker_id = worker_id
        self._now = now
        self._batch_size = batch_size
        self._lease_seconds = lease_seconds
        self._retry_delay = retry_delay
        self._runtime_sleep = runtime_sleep

    async def run_once(self) -> QuoteBatchWorkerResult:
        lease = self._control_plane.claim_job(
            worker_id=self._worker_id,
            job_types=("quote-admit",),
            lease_seconds=self._lease_seconds,
            now=self._now(),
        )
        if lease is None:
            return QuoteBatchWorkerResult(job_key=None, outcome="idle")
        runtime = AsyncAttemptRuntime(
            store=self._control_plane,
            lease=lease,
            profile=_runtime_profile(self._lease_seconds),
            clock=self._now,
            sleep=self._runtime_sleep,
        )
        try:
            async with runtime:
                return await self._run_claimed(runtime)
        except StaleLeaseError:
            raise
        except Exception as error:
            await _to_thread(
                self._control_plane.finish_retryable_with_incident,
                runtime.current_lease,
                error_class=type(error).__name__,
                incident_key=f"incident:job-retry:{lease.job_key}",
                dedupe_key=f"job-retry:{lease.job_key}",
                component="quote-admit",
                summary="quote-admit retryable failure",
                detail={
                    "job_key": lease.job_key,
                    "lease_epoch": lease.lease_epoch,
                    "error_class": type(error).__name__,
                },
                channels=incident_alert_channels(Settings()),
                now=self._now(),
            )
            if isinstance(error, QuoteAdmissionError):
                raise
            raise QuoteAdmissionError("Quote admission bundle digest or contract failed") from error
        raise AssertionError("quote admission runtime exited without a result")

    async def _run_claimed(self, runtime: AsyncAttemptRuntime) -> QuoteBatchWorkerResult:
        """Run one claimed admission while the shared runtime owns its fence."""
        lease = runtime.current_lease
        _generation, bundle_key, bundle_digest = await _to_thread(
            self._control_plane.quote_admission_input, lease.job_key
        )
        payload = await _to_thread(self._read_bundle, bundle_key)
        try:
            identity, components = parse_structure_bundle_bytes(
                payload, expected_sha256=bundle_digest
            )
        except StructureBundleError:
            identity, shards = parse_structure_shard_manifest_bytes(
                payload, expected_sha256=bundle_digest
            )
            if identity.source_kind != "gamma-source-window-events-v3-sharded":
                raise
            await self._progress(runtime, stage="read-manifest", current=1, total=1)
            legs = await self._read_v3_quote_legs_async(shards, runtime=runtime)
        else:
            await self._progress(runtime, stage="read-manifest", current=1, total=1)
            legs = quote_legs_from_structure_components(components)
            await self._progress(runtime, stage="read-shards", current=1, total=1)
        universe_hash = canonical_quote_universe_hash(legs)
        batches = quote_batches_from_legs(
            structure_receipt_digest=bundle_digest,
            universe_hash=universe_hash,
            legs=legs,
            batch_size=self._batch_size,
        )
        await self._progress(
            runtime, stage="build-batches", current=len(batches), total=len(batches)
        )
        input_artifacts = tuple(QuoteBatchInputArtifact.from_spec(batch) for batch in batches)
        for index, artifact in enumerate(input_artifacts, start=1):
            await _to_thread(
                upload_quote_batch_artifact,
                self._object_client,
                bucket=self._bucket,
                artifact=artifact,
            )
            await self._progress(
                runtime,
                stage="upload-batches",
                current=index,
                total=len(input_artifacts),
            )
        await self._progress(runtime, stage="commit-admission", current=1, total=1)
        # Stop heartbeats before the terminal transaction clears the leased
        # state.  Otherwise a heartbeat that races the commit observes the
        # newly-succeeded job as a stale lease and can cancel a successful run.
        await runtime.stop()
        await _terminal_to_thread(
            self._control_plane.admit_quote_generation,
            runtime.current_lease,
            structure_receipt_digest=bundle_digest,
            universe_hash=universe_hash,
            legs=legs,
            batch_size=self._batch_size,
            input_artifacts={
                batch.job_key: (artifact.key, artifact.sha256, len(batch.legs))
                for batch, artifact in zip(batches, input_artifacts, strict=True)
            },
            now=self._now(),
        )
        # Recovery is post-terminal finalization.  Treat it as part of the
        # same point-of-no-return envelope so a scheduler cancellation cannot
        # report timeout after admission has already committed.
        try:
            await _terminal_to_thread(
                self._control_plane.record_job_recovery,
                runtime.current_lease,
                component="quote-admit",
                channels=incident_alert_channels(Settings()),
                now=self._now(),
            )
        except Exception as error:
            # Admission is already durable and cannot be reported as failed.
            # Recovery has its own bounded transaction budget; surface the
            # pending state to the scheduler and leave the retryable incident
            # for the recovery/reconciliation path to observe.
            _LOGGER.warning(
                "quote admission committed; recovery pending job_key=%s error_class=%s",
                runtime.current_lease.job_key,
                type(error).__name__,
            )
            return QuoteBatchWorkerResult(
                job_key=runtime.current_lease.job_key,
                outcome="admitted:recovery-pending",
            )
        return QuoteBatchWorkerResult(job_key=runtime.current_lease.job_key, outcome="admitted")

    async def _progress(
        self,
        runtime: AsyncAttemptRuntime,
        *,
        stage: str,
        current: int,
        total: int | None,
    ) -> None:
        """Keep synchronous progress persistence off the event-loop thread."""
        await _to_thread(
            runtime.progress,
            stage=stage,
            current=current,
            total=total,
        )

    def _read_bundle(self, key: str) -> bytes:
        response = self._object_client.get_object(Bucket=self._bucket, Key=key)
        body = response.get("Body")
        if body is None or not hasattr(body, "read"):
            raise QuoteAdmissionError("Quote admission bundle body unavailable")
        payload = body.read()
        if not isinstance(payload, bytes):
            raise QuoteAdmissionError("Quote admission bundle body invalid")
        return payload

    def _read_v3_quote_legs(self, shards: Sequence[object]) -> tuple[QuoteBatchLeg, ...]:
        by_token: dict[str, QuoteBatchLeg] = {}
        for shard in sorted(
            (shard for shard in shards if getattr(shard, "component") == "markets"),
            key=lambda shard: getattr(shard, "ordinal"),
        ):
            payload = self._read_bundle(getattr(shard, "artifact_key"))
            header, rows = parse_structure_shard_bytes(
                payload, expected_sha256=getattr(shard, "artifact_digest")
            )
            if header.component != "markets" or header.ordinal != getattr(shard, "ordinal"):
                raise QuoteAdmissionError("Quote admission v3 shard identity invalid")
            for leg in quote_legs_from_market_rows(rows, require_nonempty=False):
                if leg.yes_token_id in by_token:
                    raise QuoteAdmissionError("Structure bundle has duplicate YES token")
                by_token[leg.yes_token_id] = leg
        if not by_token:
            raise QuoteAdmissionError("Structure bundle has no eligible Quote legs")
        return tuple(by_token[token] for token in sorted(by_token))

    async def _read_v3_quote_legs_async(
        self,
        shards: Sequence[object],
        *,
        runtime: AsyncAttemptRuntime,
    ) -> tuple[QuoteBatchLeg, ...]:
        by_token: dict[str, QuoteBatchLeg] = {}
        markets = tuple(
            sorted(
                (shard for shard in shards if getattr(shard, "component") == "markets"),
                key=lambda shard: getattr(shard, "ordinal"),
            )
        )
        for index, shard in enumerate(markets, start=1):
            payload = await _to_thread(self._read_bundle, getattr(shard, "artifact_key"))
            header, rows = parse_structure_shard_bytes(
                payload, expected_sha256=getattr(shard, "artifact_digest")
            )
            if header.component != "markets" or header.ordinal != getattr(shard, "ordinal"):
                raise QuoteAdmissionError("Quote admission v3 shard identity invalid")
            for leg in quote_legs_from_market_rows(rows, require_nonempty=False):
                if leg.yes_token_id in by_token:
                    raise QuoteAdmissionError("Structure bundle has duplicate YES token")
                by_token[leg.yes_token_id] = leg
            await self._progress(
                runtime,
                stage="read-shards",
                current=index,
                total=len(markets),
            )
        if not by_token:
            raise QuoteAdmissionError("Structure bundle has no eligible Quote legs")
        return tuple(by_token[token] for token in sorted(by_token))
