"""Boot-anchored, atomic L3 process and five-market evidence sampling."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

from loguru import logger

from polyarb.observation.l3_evidence import (
    EvidenceStatus,
    HealthSampleRecord,
    HealthStatus,
    MarketSampleRecord,
    SampleBatch,
    stable_sha256,
)
from polyarb.storage.l3_evidence_store import (
    L3EvidenceReadError,
    L3EvidenceStore,
    RuntimeEventIntegrityConflict,
    SamplingMarketState,
)

_AGGREGATE_READ_ATTEMPTS = 3
_AGGREGATE_READ_TIMEOUT_S = 6.0


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _grid_index(boot_started_at: datetime, at: datetime, interval: timedelta) -> int:
    if at <= boot_started_at:
        return 0
    return (at - boot_started_at) // interval


async def _wait_for_stop(stop_event: asyncio.Event, delay_s: float) -> bool:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=delay_s)
        return True
    except TimeoutError:
        return False


def _age_ms(sampled_at: datetime, observed_at: datetime | None) -> int | None:
    if observed_at is None:
        return None
    return max(0, int((sampled_at - observed_at).total_seconds() * 1000))


def _epoch_age_ms(sampled_at: datetime, observed_at_s: object) -> int | None:
    try:
        observed = datetime.fromtimestamp(float(observed_at_s), tz=UTC)
    except (TypeError, ValueError, OSError):
        return None
    return _age_ms(sampled_at, observed)


def _validate_mapping(
    markets: tuple[SamplingMarketState, ...],
) -> frozenset[str]:
    if len(markets) != 5:
        raise ValueError("sampling requires exactly five complete market pairs")
    if len({market.market_id for market in markets}) != 5:
        raise ValueError("sampling requires exactly five complete market pairs")
    yes_tokens = {market.yes_token_id for market in markets}
    no_tokens = {market.no_token_id for market in markets}
    if len(yes_tokens) != 5 or len(no_tokens) != 5 or len(yes_tokens | no_tokens) != 10:
        raise ValueError("sampling requires exactly five complete market pairs")
    return frozenset(yes_tokens | no_tokens)


def _market_reason(
    market: SamplingMarketState,
    *,
    sampled_at: datetime,
    runtime: EvidenceStatus,
    book_fresh_ms: int,
    ohlc_fresh_ms: int,
) -> tuple[HealthStatus, str]:
    token_evidence: dict[str, datetime] = {}
    for side, token_id in (
        ("yes", market.yes_token_id),
        ("no", market.no_token_id),
    ):
        if token_id not in runtime.desired:
            return HealthStatus.FAIL, "not_desired"
        if token_id not in runtime.committed:
            return HealthStatus.FAIL, "not_committed"
        if token_id not in runtime.evidenced:
            return HealthStatus.FAIL, "not_evidenced"
        evidenced_at = runtime.evidenced_at.get(token_id)
        if evidenced_at is None:
            return HealthStatus.FAIL, f"{side}_evidence_missing"
        if evidenced_at > sampled_at:
            return HealthStatus.FAIL, f"{side}_evidence_in_future"
        token_evidence[side] = evidenced_at

    checks = (
        ("yes_book", market.yes_book_at, book_fresh_ms),
        ("no_book", market.no_book_at, book_fresh_ms),
        ("yes_ohlc", market.yes_ohlc_at, ohlc_fresh_ms),
    )
    for name, observed_at, threshold_ms in checks:
        if observed_at is None:
            return HealthStatus.FAIL, f"{name}_missing"
        if observed_at > sampled_at:
            return HealthStatus.FAIL, f"{name}_in_future"
        age_ms = _age_ms(sampled_at, observed_at)
        if age_ms is None or age_ms >= threshold_ms:
            return HealthStatus.FAIL, f"{name}_stale"
        if name.endswith("_book"):
            side = name.removesuffix("_book")
            evidenced_at = token_evidence[side]
            evidence_age_ms = _age_ms(sampled_at, evidenced_at)
            if evidence_age_ms is None or evidence_age_ms >= book_fresh_ms:
                return HealthStatus.FAIL, f"{side}_evidence_stale"
            if observed_at < evidenced_at:
                return HealthStatus.FAIL, f"{side}_book_before_evidence"
    return HealthStatus.PASS, "ok"


def _build_market_record(
    market: SamplingMarketState,
    *,
    sampled_at: datetime,
    sample_seq: int,
    runtime: EvidenceStatus,
    book_fresh_ms: int,
    ohlc_fresh_ms: int,
) -> MarketSampleRecord:
    status, reason_code = _market_reason(
        market,
        sampled_at=sampled_at,
        runtime=runtime,
        book_fresh_ms=book_fresh_ms,
        ohlc_fresh_ms=ohlc_fresh_ms,
    )
    yes_age = _age_ms(sampled_at, market.yes_book_at)
    no_age = _age_ms(sampled_at, market.no_book_at)
    known_book_ages = [age for age in (yes_age, no_age) if age is not None]
    return MarketSampleRecord(
        boot_id=runtime.boot_id,
        sample_seq=sample_seq,
        sampled_at=sampled_at,
        market_id=market.market_id,
        yes_token_id=market.yes_token_id,
        no_token_id=market.no_token_id,
        yes_desired=market.yes_token_id in runtime.desired,
        no_desired=market.no_token_id in runtime.desired,
        yes_committed=market.yes_token_id in runtime.committed,
        no_committed=market.no_token_id in runtime.committed,
        yes_evidenced=market.yes_token_id in runtime.evidenced,
        no_evidenced=market.no_token_id in runtime.evidenced,
        evidence_generation=runtime.ws_generation,
        yes_book_at=market.yes_book_at,
        no_book_at=market.no_book_at,
        yes_book_age_ms=yes_age,
        no_book_age_ms=no_age,
        worst_book_age_ms=max(known_book_ages) if known_book_ages else None,
        yes_ohlc_at=market.yes_ohlc_at,
        yes_ohlc_age_ms=_age_ms(sampled_at, market.yes_ohlc_at),
        status=status,
        reason_code=reason_code,
    )


def _build_health_record(
    *,
    scheduled_at: datetime,
    sampled_at: datetime,
    sample_seq: int,
    runtime: EvidenceStatus,
    markets: tuple[MarketSampleRecord, ...],
    mapping_hash: str,
    ws_consumer: Any,
    reconciliation_state: Any,
    mapped_tokens: frozenset[str],
) -> HealthSampleRecord:
    exact_membership = (
        len(mapped_tokens) == 10
        and runtime.desired == mapped_tokens
        and runtime.committed == mapped_tokens
        and runtime.evidenced == mapped_tokens
    )
    markets_ok = all(market.status is HealthStatus.PASS for market in markets)
    if not exact_membership:
        status, reason_code = HealthStatus.FAIL, "membership_convergence_failed"
    elif not markets_ok:
        status, reason_code = HealthStatus.FAIL, "market_freshness_failed"
    else:
        status, reason_code = HealthStatus.PASS, "ok"

    book_ages = [
        age
        for market in markets
        for age in (market.yes_book_age_ms, market.no_book_age_ms)
        if age is not None
    ]
    watchdog = getattr(ws_consumer, "_watchdog", None)
    try:
        watchdog_count = max(0, int(getattr(watchdog, "reconnect_attempt", 0)))
    except (TypeError, ValueError):
        watchdog_count = 0
    try:
        reconnect_count = max(0, int(getattr(reconciliation_state, "reconnect_count", 0)))
    except (TypeError, ValueError):
        reconnect_count = 0
    try:
        cursor_lag = max(0, int(getattr(reconciliation_state, "cursor_lag", 0)))
    except (TypeError, ValueError):
        cursor_lag = 0
    listener_state = (
        "connected"
        if bool(getattr(reconciliation_state, "is_connected", False))
        else "disconnected"
    )

    from polyarb.observation.l2_candidate_refresh import get_last_fetch_success_at_s
    from polyarb.observation.l3_promote import get_last_book_levels_write_at_s

    return HealthSampleRecord(
        boot_id=runtime.boot_id,
        sample_seq=sample_seq,
        scheduled_at=scheduled_at,
        sampled_at=sampled_at,
        desired_count=len(runtime.desired),
        committed_count=len(runtime.committed),
        evidenced_count=len(runtime.evidenced),
        promote_age_ms=_age_ms(sampled_at, runtime.last_promote_persisted_at),
        global_book_age_ms=min(book_ages) if book_ages else None,
        ws_age_ms=_epoch_age_ms(sampled_at, getattr(ws_consumer, "last_event_at_s", None)),
        mirror_age_ms=_epoch_age_ms(sampled_at, get_last_book_levels_write_at_s()),
        candidate_age_ms=_epoch_age_ms(sampled_at, get_last_fetch_success_at_s()),
        reconciliation_age_ms=_epoch_age_ms(
            sampled_at,
            getattr(reconciliation_state, "last_reconciliation_success_s", None),
        ),
        listener_state=listener_state,
        cursor_lag=cursor_lag,
        watchdog_count=watchdog_count,
        reconnect_count=reconnect_count,
        ws_generation=runtime.ws_generation,
        mapping_hash=mapping_hash,
        acceptance_config_hash=runtime.acceptance_config_hash,
        status=status,
        reason_code=reason_code,
    )


async def collect_sample(
    *,
    scheduled_at: datetime,
    sample_seq: int,
    settings: Any,
    ws_consumer: Any,
    reconciliation_state: Any,
    runtime: Any,
    store: L3EvidenceStore,
) -> SampleBatch:
    """Collect one immutable membership cut and one later aggregate DB read."""
    membership_fields = (
        "boot_id",
        "acceptance_config_hash",
        "ws_generation",
        "desired",
        "committed",
        "evidenced",
    )
    for attempt in range(_AGGREGATE_READ_ATTEMPTS):
        initial_status = runtime.snapshot()
        token_ids = sorted(initial_status.desired)
        try:
            market_states = tuple(
                await asyncio.wait_for(
                    store.fetch_sampling_market_state(token_ids),
                    timeout=_AGGREGATE_READ_TIMEOUT_S,
                )
            )
        except asyncio.CancelledError:
            raise
        except (L3EvidenceReadError, TimeoutError):
            if attempt + 1 == _AGGREGATE_READ_ATTEMPTS:
                raise
            continue
        runtime_status = runtime.snapshot()
        if any(
            getattr(initial_status, field) != getattr(runtime_status, field)
            for field in membership_fields
        ):
            if attempt + 1 == _AGGREGATE_READ_ATTEMPTS:
                raise ValueError("membership changed during aggregate fetch")
            continue
        break
    sampled_at = _utc_now()
    interval_s = settings.l3_evidence_sample_interval_s
    if not scheduled_at <= sampled_at < scheduled_at + timedelta(seconds=interval_s):
        raise ValueError("sampling slot expired during aggregate fetch")
    mapped_tokens = _validate_mapping(market_states)
    book_fresh_ms = int(settings.l3_market_book_fresh_s * 1000)
    ohlc_fresh_ms = int(settings.l3_market_ohlc_fresh_s * 1000)
    markets = tuple(
        _build_market_record(
            market,
            sampled_at=sampled_at,
            sample_seq=sample_seq,
            runtime=initial_status,
            book_fresh_ms=book_fresh_ms,
            ohlc_fresh_ms=ohlc_fresh_ms,
        )
        for market in market_states
    )
    mapping_hash = stable_sha256(
        [
            {
                "market_id": market.market_id,
                "yes_token_id": market.yes_token_id,
                "no_token_id": market.no_token_id,
            }
            for market in market_states
        ]
    )
    health = _build_health_record(
        scheduled_at=scheduled_at,
        sampled_at=sampled_at,
        sample_seq=sample_seq,
        runtime=initial_status,
        markets=markets,
        mapping_hash=mapping_hash,
        ws_consumer=ws_consumer,
        reconciliation_state=reconciliation_state,
        mapped_tokens=mapped_tokens,
    )
    return SampleBatch(health=health, markets=markets)


async def sample_once(
    *,
    scheduled_at: datetime,
    sample_seq: int,
    settings: Any,
    ws_consumer: Any,
    reconciliation_state: Any,
    runtime: Any,
    store: L3EvidenceStore,
) -> bool:
    """Append one atomic batch and publish success truth only after its ACK."""
    batch = await collect_sample(
        scheduled_at=scheduled_at,
        sample_seq=sample_seq,
        settings=settings,
        ws_consumer=ws_consumer,
        reconciliation_state=reconciliation_state,
        runtime=runtime,
        store=store,
    )
    persisted = await store.append_sample(batch)
    result_at = _utc_now()
    runtime.note_writer_result(
        persisted,
        result_at,
        "ok" if persisted else "sample_append_failed",
        channel="sample",
    )
    if persisted:
        runtime.mark_sample_persisted(batch.health.sampled_at, batch.markets)
    return persisted


async def run_sampler(
    stop_event: asyncio.Event,
    *,
    settings: Any,
    ws_consumer: Any,
    reconciliation_state: Any,
    runtime: Any,
    store: L3EvidenceStore,
) -> None:
    """Sample on a boot grid while skipping elapsed and pre-mapping boundaries."""
    interval_s = settings.l3_evidence_sample_interval_s
    if isinstance(interval_s, bool) or not isinstance(interval_s, (int, float)):
        raise TypeError("l3_evidence_sample_interval_s must be numeric")
    if interval_s <= 0:
        raise ValueError("l3_evidence_sample_interval_s must be positive")
    boot_started_at = runtime.snapshot().started_at
    interval = timedelta(seconds=interval_s)
    next_boundary_index = 0
    while not stop_event.is_set():
        now = _utc_now()
        current_boundary_index = _grid_index(boot_started_at, now, interval)
        boundary_index = max(next_boundary_index, current_boundary_index)
        boundary = boot_started_at + boundary_index * interval
        delay_s = max(0.0, (boundary - _utc_now()).total_seconds())
        if delay_s > 0 and await _wait_for_stop(stop_event, delay_s):
            break
        if stop_event.is_set():
            break
        sampled_at = _utc_now()
        if sampled_at < boundary:
            continue
        sampled_boundary_index = _grid_index(boot_started_at, sampled_at, interval)
        if sampled_boundary_index > boundary_index:
            boundary_index = sampled_boundary_index
            boundary = boot_started_at + boundary_index * interval
        if not boundary <= sampled_at < boundary + interval:
            next_boundary_index = boundary_index + 1
            continue
        if len(runtime.snapshot().desired) != 10:
            next_boundary_index = boundary_index + 1
            continue
        sample_seq = boundary_index
        try:
            await sample_once(
                scheduled_at=boundary,
                sample_seq=sample_seq,
                settings=settings,
                ws_consumer=ws_consumer,
                reconciliation_state=reconciliation_state,
                runtime=runtime,
                store=store,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - missing row remains the truth
            runtime.note_writer_result(
                False,
                _utc_now(),
                "sample_collection_failed",
                channel="sample",
            )
            logger.warning(
                "l3 sampler failed sample_seq={} error_type={}",
                sample_seq,
                type(exc).__name__,
            )
        next_boundary_index = boundary_index + 1


async def run_event_writer(
    stop_event: asyncio.Event,
    *,
    runtime: Any,
    store: L3EvidenceStore,
    flush_interval_s: float = 1.0,
    producers_done: asyncio.Event | None = None,
) -> None:
    """Append queued events in sequence order without losing failed records."""
    if isinstance(flush_interval_s, bool) or not isinstance(
        flush_interval_s, (int, float)
    ):
        raise TypeError("flush_interval_s must be numeric")
    if flush_interval_s < 0:
        raise ValueError("flush_interval_s must be non-negative")

    while True:
        event = runtime.peek_pending_event()
        if event is None:
            if stop_event.is_set() and (
                producers_done is None or producers_done.is_set()
            ):
                return
            if stop_event.is_set() and producers_done is not None:
                if flush_interval_s == 0:
                    await asyncio.sleep(0)
                else:
                    try:
                        await asyncio.wait_for(
                            producers_done.wait(), timeout=flush_interval_s
                        )
                    except TimeoutError:
                        pass
                continue
            if await _wait_for_stop(stop_event, flush_interval_s):
                continue
            continue

        persisted = False
        try:
            persisted = await store.append_event(event)
        except asyncio.CancelledError:
            raise
        except RuntimeEventIntegrityConflict:
            result_at = _utc_now()
            runtime.quarantine_conflicting_event(
                event,
                at=result_at,
                reason_code="event_replay_conflict",
            )
            logger.error(
                "l3 event writer quarantined integrity conflict event_seq={}",
                event.event_seq,
            )
            continue
        except Exception as exc:  # noqa: BLE001 - retain the queue head
            logger.warning(
                "l3 event writer append raised event_seq={} error_type={}",
                event.event_seq,
                type(exc).__name__,
            )

        result_at = _utc_now()
        if persisted:
            runtime.acknowledge_pending_event(event)
            runtime.note_writer_result(
                True, result_at, "writer_ok", channel="event"
            )
            continue

        try:
            runtime.note_writer_result(
                False,
                result_at,
                "event_append_failed",
                channel="event",
            )
        except OverflowError:
            # The queue's own overflow bit is already durable process-local fail
            # truth. The unacknowledged event remains at the head for retry.
            pass
        await asyncio.sleep(flush_interval_s)
