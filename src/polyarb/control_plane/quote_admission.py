"""Bridge certified Structure bundles into frozen transactional Quote inputs."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from typing import Any, Protocol

from polyarb.config import Settings

from .alert_delivery import incident_alert_channels
from .blocking_bridge import run_blocking_call
from .failure_identity import retry_failure_fingerprint
from .models import QuoteBatchLeg, QuoteBatchSpec
from .postgres import PostgresControlPlane, StaleLeaseError
from .quote_artifact import QuoteBatchInputArtifact, upload_quote_batch_artifact
from .quote_worker import QuoteBatchWorkerResult
from .runtime_contract import AsyncAttemptRuntime
from .runtime_deadlines import runtime_deadline_profile, runtime_policy
from .service_lifecycle import claim_worker_job
from .structure_artifact import (
    StructureBundleError,
    parse_structure_bundle_bytes,
    parse_structure_shard_bytes,
    parse_structure_shard_manifest_bytes,
)


class QuoteAdmissionError(RuntimeError):
    """A certified Structure bundle cannot safely freeze Quote work."""


class QuoteAdmissionShardUnavailable(QuoteAdmissionError):
    """One manifest-authorized Structure shard cannot be read safely."""

    def __init__(self, artifact_key: str) -> None:
        if not artifact_key or "\x00" in artifact_key or len(artifact_key) > 512:
            raise ValueError("missing Structure shard key is invalid")
        self.artifact_key = artifact_key
        super().__init__(f"Quote admission Structure shard unavailable: {artifact_key}")


_LOGGER = logging.getLogger(__name__)


def _checkpoint_leg_row(leg: QuoteBatchLeg) -> dict[str, object]:
    return {
        "condition_id": leg.condition_id,
        "event_id": leg.event_id,
        "market_id": leg.market_id,
        "membership_hash": leg.membership_hash,
        "neg_risk_market_id": leg.neg_risk_market_id,
        "slug": leg.slug,
        "yes_token_id": leg.yes_token_id,
    }


def _canonical_admission_checkpoint_bytes(
    *,
    structure_digest: str,
    start_index: int,
    end_index: int,
    legs: Sequence[QuoteBatchLeg],
) -> bytes:
    if len(structure_digest) != 64 or start_index < 0 or end_index <= start_index:
        raise ValueError("quote admission checkpoint identity is invalid")
    header = {
        "end_index": end_index,
        "kind": "quote-admission-checkpoint",
        "policy_version": "runtime-v2",
        "start_index": start_index,
        "structure_digest": structure_digest,
    }
    rows = (header, *(_checkpoint_leg_row(leg) for leg in legs))
    return b"".join(
        json.dumps(row, sort_keys=True, separators=(",", ":"), allow_nan=False).encode() + b"\n"
        for row in rows
    )


@dataclass(frozen=True, slots=True)
class _AdmissionCheckpointArtifact:
    payload: bytes
    sha256: str
    key: str
    start_index: int
    end_index: int
    legs: tuple[QuoteBatchLeg, ...]

    @classmethod
    def from_legs(
        cls,
        *,
        structure_digest: str,
        start_index: int,
        end_index: int,
        legs: Sequence[QuoteBatchLeg],
    ) -> _AdmissionCheckpointArtifact:
        ordered = tuple(sorted(legs, key=lambda leg: leg.yes_token_id))
        payload = _canonical_admission_checkpoint_bytes(
            structure_digest=structure_digest,
            start_index=start_index,
            end_index=end_index,
            legs=ordered,
        )
        digest = sha256(payload).hexdigest()
        return cls(
            payload=payload,
            sha256=digest,
            key=f"quote-admission-checkpoints/{digest}/legs.ndjson",
            start_index=start_index,
            end_index=end_index,
            legs=ordered,
        )

    @classmethod
    def parse(
        cls,
        payload: bytes,
        *,
        expected_sha256: str,
        expected_structure_digest: str,
    ) -> _AdmissionCheckpointArtifact:
        if sha256(payload).hexdigest() != expected_sha256:
            raise QuoteAdmissionError("Quote admission checkpoint digest invalid")
        try:
            header, *rows = [json.loads(line) for line in payload.splitlines() if line]
            if (
                not isinstance(header, dict)
                or header.get("kind") != "quote-admission-checkpoint"
                or header.get("policy_version") != "runtime-v2"
                or header.get("structure_digest") != expected_structure_digest
            ):
                raise ValueError
            legs = tuple(
                QuoteBatchLeg(
                    neg_risk_market_id=str(row["neg_risk_market_id"]),
                    market_id=str(row["market_id"]),
                    condition_id=str(row["condition_id"]),
                    slug=row.get("slug"),
                    yes_token_id=str(row["yes_token_id"]),
                    event_id=str(row.get("event_id", "")),
                    membership_hash=str(row.get("membership_hash", "")),
                )
                for row in rows
                if isinstance(row, dict)
            )
            artifact = cls.from_legs(
                structure_digest=expected_structure_digest,
                start_index=int(header["start_index"]),
                end_index=int(header["end_index"]),
                legs=legs,
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise QuoteAdmissionError("Quote admission checkpoint invalid") from error
        if artifact.payload != payload or artifact.sha256 != expected_sha256:
            raise QuoteAdmissionError("Quote admission checkpoint is noncanonical")
        return artifact


async def _to_thread(call: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Run nonterminal blocking work under the shared service boundary."""
    return await run_blocking_call(
        call,
        *args,
        thread_name="quote-admission:blocking-call",
        **kwargs,
    )


async def _terminal_to_thread(call: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Allow a terminal admission call to finish only within stop grace."""
    return await run_blocking_call(
        call,
        *args,
        point_of_no_return=True,
        thread_name="quote-admission:terminal-call",
        **kwargs,
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
    quote_generation_digest: str | None = None,
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
            quote_generation_digest=quote_generation_digest,
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
        self._runtime_sleep = runtime_sleep

    async def run_once(self) -> QuoteBatchWorkerResult:
        lease = await claim_worker_job(
            self._control_plane,
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
            profile=runtime_deadline_profile("quote-admit", self._lease_seconds),
            clock=self._now,
            sleep=self._runtime_sleep,
        )
        try:
            async with runtime:
                return await self._run_claimed(runtime)
        except asyncio.CancelledError:
            await _terminal_to_thread(
                self._control_plane.finish_interrupted,
                runtime.current_lease,
                component="quote-admit",
                now=self._now(),
            )
            raise
        except StaleLeaseError:
            raise
        except Exception as error:
            detail: dict[str, object] = {
                "job_key": lease.job_key,
                "lease_epoch": lease.lease_epoch,
                "error_class": type(error).__name__,
                "failure_fingerprint": retry_failure_fingerprint(
                    error, component="quote-admit"
                ),
            }
            if isinstance(error, QuoteAdmissionShardUnavailable):
                detail["missing_artifact_key"] = error.artifact_key
            await _to_thread(
                self._control_plane.finish_retryable_with_incident,
                runtime.current_lease,
                error_class=type(error).__name__,
                incident_key=f"incident:job-retry:{lease.job_key}",
                dedupe_key=f"job-retry:{lease.job_key}",
                component="quote-admit",
                summary="quote-admit retryable failure",
                detail=detail,
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
        _generation, bundle_key, bundle_digest, quote_generation_key = await _to_thread(
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
            legs = await self._read_v3_quote_legs_async(
                shards,
                runtime=runtime,
                structure_digest=bundle_digest,
            )
        else:
            await self._progress(runtime, stage="read-manifest", current=1, total=1)
            legs = quote_legs_from_structure_components(components)
            await self._progress(runtime, stage="read-shards", current=1, total=1)
        universe_hash = canonical_quote_universe_hash(legs)
        batches = quote_batches_from_legs(
            structure_receipt_digest=bundle_digest,
            quote_generation_digest=quote_generation_key.removeprefix("quote:"),
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
        structure_digest: str,
    ) -> tuple[QuoteBatchLeg, ...]:
        by_token: dict[str, QuoteBatchLeg] = {}
        markets = tuple(
            sorted(
                (shard for shard in shards if getattr(shard, "component") == "markets"),
                key=lambda shard: getattr(shard, "ordinal"),
            )
        )
        resume_index = 0
        for cursor, digest, artifact_key in await _to_thread(
            self._control_plane.running_checkpoints,
            runtime.current_lease.job_key,
        ):
            expected_cursor = f"runtime-v2:{structure_digest}:"
            if not cursor.startswith(expected_cursor):
                raise QuoteAdmissionError("Quote admission checkpoint cursor invalid")
            artifact = _AdmissionCheckpointArtifact.parse(
                await _to_thread(self._read_bundle, artifact_key),
                expected_sha256=digest,
                expected_structure_digest=structure_digest,
            )
            if artifact.start_index != resume_index or cursor != (
                expected_cursor + str(artifact.end_index)
            ):
                raise QuoteAdmissionError("Quote admission checkpoint sequence invalid")
            for leg in artifact.legs:
                if leg.yes_token_id in by_token:
                    raise QuoteAdmissionError("Structure bundle has duplicate YES token")
                by_token[leg.yes_token_id] = leg
            resume_index = artifact.end_index
        if resume_index > len(markets):
            raise QuoteAdmissionError("Quote admission checkpoint exceeds shard count")
        checkpoint_interval = runtime_policy(
            "quote-admit", runtime.profile.lease_seconds
        ).checkpoint_interval
        chunk_start = resume_index
        chunk_legs: list[QuoteBatchLeg] = []
        for index, shard in enumerate(markets[resume_index:], start=resume_index + 1):
            artifact_key = str(getattr(shard, "artifact_key"))
            try:
                payload = await _to_thread(self._read_bundle, artifact_key)
            except asyncio.CancelledError:
                raise
            except QuoteAdmissionShardUnavailable:
                raise
            except Exception as error:
                raise QuoteAdmissionShardUnavailable(artifact_key) from error
            header, rows = parse_structure_shard_bytes(
                payload, expected_sha256=getattr(shard, "artifact_digest")
            )
            if header.component != "markets" or header.ordinal != getattr(shard, "ordinal"):
                raise QuoteAdmissionError("Quote admission v3 shard identity invalid")
            for leg in quote_legs_from_market_rows(rows, require_nonempty=False):
                if leg.yes_token_id in by_token:
                    raise QuoteAdmissionError("Structure bundle has duplicate YES token")
                by_token[leg.yes_token_id] = leg
                chunk_legs.append(leg)
            await self._progress(
                runtime,
                stage="read-shards",
                current=index,
                total=len(markets),
            )
            if index % checkpoint_interval == 0 or index == len(markets):
                artifact = _AdmissionCheckpointArtifact.from_legs(
                    structure_digest=structure_digest,
                    start_index=chunk_start,
                    end_index=index,
                    legs=chunk_legs,
                )
                await _to_thread(self._upload_checkpoint, artifact)
                await _to_thread(
                    self._control_plane.record_running_checkpoint,
                    runtime.current_lease,
                    checkpoint_cursor=f"runtime-v2:{structure_digest}:{index}",
                    checkpoint_digest=artifact.sha256,
                    artifact_key=artifact.key,
                    idempotency_key=(
                        f"quote-admit-checkpoint:{runtime.current_lease.job_key}:runtime-v2:{index}"
                    ),
                    now=self._now(),
                )
                chunk_start = index
                chunk_legs = []
        if not by_token:
            raise QuoteAdmissionError("Structure bundle has no eligible Quote legs")
        return tuple(by_token[token] for token in sorted(by_token))

    def _upload_checkpoint(self, artifact: _AdmissionCheckpointArtifact) -> None:
        self._object_client.put_object(
            Bucket=self._bucket,
            Key=artifact.key,
            Body=artifact.payload,
            ContentType="application/x-ndjson",
            Metadata={"sha256": artifact.sha256},
        )
        head = self._object_client.head_object(Bucket=self._bucket, Key=artifact.key)
        if (
            int(head.get("ContentLength", -1)) != len(artifact.payload)
            or str(head.get("Metadata", {}).get("sha256", "")) != artifact.sha256
        ):
            raise QuoteAdmissionError("Quote admission checkpoint upload invalid")
