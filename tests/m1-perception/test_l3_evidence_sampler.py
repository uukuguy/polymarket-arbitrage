"""Atomic, non-backfilling L3 process/per-market sampler tests."""

from __future__ import annotations

import asyncio
from dataclasses import fields, replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from polyarb.events.reconciliation import ReconciliationState
from polyarb.observation import l3_sampler
from polyarb.observation.l3_evidence import (
    AcceptanceConfig,
    EvidenceWindow,
    HealthStatus,
    L3EvidenceRuntime,
    RuntimeBootRecord,
    RuntimeIdentity,
    WsMembershipSnapshot,
    stable_sha256,
)
from polyarb.observation.l3_soak_verdict import (
    ManifestReport,
    SoakManifest,
    VerdictStatus,
    build_soak_report,
)
from polyarb.storage.l3_evidence_store import L3EvidenceReadError, SamplingMarketState

HASH = "a" * 64
START = datetime(2026, 7, 23, 5, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _fixed_sampler_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(l3_sampler, "_utc_now", lambda: START)


def _settings() -> SimpleNamespace:
    return SimpleNamespace(
        l3_evidence_sample_interval_s=30,
        l3_market_book_fresh_s=120,
        l3_market_ohlc_fresh_s=120,
    )


def _runtime(*, started_at: datetime = START) -> L3EvidenceRuntime:
    return L3EvidenceRuntime(
        RuntimeIdentity(
            machine_id="machine-1",
            machine_version="version-1",
            image_ref="image@sha256:test",
            release_id="release-1",
            code_version="code-1",
            recipe_sha256="b" * 64,
            acceptance_config_hash=HASH,
        ),
        started_at=started_at,
    )


def _pairs(
    sampled_at: datetime = START,
    *,
    missing_markets: frozenset[int] = frozenset(),
) -> tuple[SamplingMarketState, ...]:
    return tuple(
        SamplingMarketState(
            market_id=f"market-{index}",
            yes_token_id=f"yes-{index}",
            no_token_id=f"no-{index}",
            yes_book_at=None if index in missing_markets else sampled_at - timedelta(seconds=10),
            no_book_at=None if index in missing_markets else sampled_at - timedelta(seconds=11),
            yes_ohlc_at=sampled_at - timedelta(seconds=12),
        )
        for index in range(5)
    )


def _tokens(pairs: tuple[SamplingMarketState, ...]) -> frozenset[str]:
    return frozenset(token for pair in pairs for token in (pair.yes_token_id, pair.no_token_id))


def _publish_current_membership(
    runtime: L3EvidenceRuntime,
    pairs: tuple[SamplingMarketState, ...],
    *,
    generation: int = 4,
    desired: frozenset[str] | None = None,
    committed: frozenset[str] | None = None,
    evidenced: frozenset[str] | None = None,
    evidenced_at: datetime | None = None,
) -> None:
    all_tokens = _tokens(pairs)
    current_evidenced = all_tokens if evidenced is None else evidenced
    evidence_times = {}
    for pair in pairs:
        for token_id, book_at in (
            (pair.yes_token_id, pair.yes_book_at),
            (pair.no_token_id, pair.no_book_at),
        ):
            if token_id in current_evidenced:
                evidence_times[token_id] = (
                    evidenced_at
                    if evidenced_at is not None
                    else (book_at or START - timedelta(seconds=1))
                )
    runtime.update_membership(
        WsMembershipSnapshot(
            generation=generation,
            desired=all_tokens if desired is None else desired,
            committed=all_tokens if committed is None else committed,
            evidenced=current_evidenced,
            evidenced_at=evidence_times,
        )
    )


class _ConsumerWithoutMembershipReads:
    last_event_at_s = START.timestamp() - 5
    frame_count = 17

    def l3_membership_snapshot(self) -> None:
        raise AssertionError("sampler must use only runtime.snapshot membership")


class _CountingRuntime:
    def __init__(self, runtime: L3EvidenceRuntime) -> None:
        self.runtime = runtime
        self.snapshot_calls = 0

    def snapshot(self):
        self.snapshot_calls += 1
        return self.runtime.snapshot()


def _reconciliation() -> ReconciliationState:
    return ReconciliationState(
        is_connected=True,
        reconnect_count=2,
        last_reconciliation_success_s=START.timestamp() - 8,
        latest_snapshot_id=20,
        committed_cursor=20,
        cursor_lag=0,
    )


async def test_collect_sample_uses_one_runtime_snapshot_and_builds_atomic_five_pair_batch():
    pairs = _pairs()
    runtime = _runtime()
    _publish_current_membership(runtime, pairs)
    runtime.mark_promote_persisted(START - timedelta(seconds=15))
    counting_runtime = _CountingRuntime(runtime)
    store = SimpleNamespace(fetch_sampling_market_state=AsyncMock(return_value=pairs))

    batch = await l3_sampler.collect_sample(
        scheduled_at=START,
        sample_seq=7,
        settings=_settings(),
        ws_consumer=_ConsumerWithoutMembershipReads(),
        reconciliation_state=_reconciliation(),
        runtime=counting_runtime,
        store=store,
    )

    assert counting_runtime.snapshot_calls == 2
    store.fetch_sampling_market_state.assert_awaited_once_with(sorted(_tokens(pairs)))
    assert batch.health.boot_id == runtime.snapshot().boot_id
    assert batch.health.sample_seq == 7
    assert batch.health.acceptance_config_hash == HASH
    assert batch.health.status is HealthStatus.PASS
    assert batch.health.reason_code == "ok"
    assert len(batch.markets) == 5
    assert {row.market_id for row in batch.markets} == {f"market-{index}" for index in range(5)}
    assert {
        token for row in batch.markets for token in (row.yes_token_id, row.no_token_id)
    } == _tokens(pairs)
    assert all(row.status is HealthStatus.PASS for row in batch.markets)
    assert all(row.evidence_generation == 4 for row in batch.markets)


@pytest.mark.parametrize("count", [0, 4])
async def test_collect_sample_rejects_zero_or_fewer_than_five_complete_pairs(count: int):
    pairs = _pairs()[:count]
    runtime = _runtime()
    all_pairs = _pairs()
    _publish_current_membership(runtime, all_pairs)
    store = SimpleNamespace(fetch_sampling_market_state=AsyncMock(return_value=pairs))

    with pytest.raises(ValueError, match="exactly five complete market pairs"):
        await l3_sampler.collect_sample(
            scheduled_at=START,
            sample_seq=0,
            settings=_settings(),
            ws_consumer=_ConsumerWithoutMembershipReads(),
            reconciliation_state=_reconciliation(),
            runtime=runtime,
            store=store,
        )


async def test_one_hot_market_does_not_make_four_silent_markets_fresh():
    pairs = _pairs(missing_markets=frozenset({1, 2, 3, 4}))
    runtime = _runtime()
    _publish_current_membership(runtime, pairs)
    store = SimpleNamespace(fetch_sampling_market_state=AsyncMock(return_value=pairs))

    batch = await l3_sampler.collect_sample(
        scheduled_at=START,
        sample_seq=0,
        settings=_settings(),
        ws_consumer=_ConsumerWithoutMembershipReads(),
        reconciliation_state=_reconciliation(),
        runtime=runtime,
        store=store,
    )

    by_market = {row.market_id: row for row in batch.markets}
    assert by_market["market-0"].status is HealthStatus.PASS
    assert all(by_market[f"market-{index}"].status is HealthStatus.FAIL for index in range(1, 5))
    assert all(
        by_market[f"market-{index}"].reason_code == "yes_book_missing" for index in range(1, 5)
    )
    assert batch.health.status is HealthStatus.FAIL
    assert batch.health.reason_code == "market_freshness_failed"


@pytest.mark.parametrize(
    ("mutation", "expected_reason"),
    [
        ("stale_no", "no_book_stale"),
        ("stale_yes_ohlc", "yes_ohlc_stale"),
        ("committed_mismatch", "not_committed"),
        ("old_generation", "not_evidenced"),
    ],
)
async def test_market_status_fails_closed_on_freshness_and_membership_faults(
    mutation: str,
    expected_reason: str,
):
    pairs = list(_pairs())
    if mutation == "stale_no":
        pairs[2] = replace(pairs[2], no_book_at=START - timedelta(seconds=120))
    elif mutation == "stale_yes_ohlc":
        pairs[2] = replace(pairs[2], yes_ohlc_at=START - timedelta(seconds=120))
    frozen_pairs = tuple(pairs)
    runtime = _runtime()
    all_tokens = _tokens(frozen_pairs)
    if mutation == "committed_mismatch":
        _publish_current_membership(
            runtime,
            frozen_pairs,
            committed=all_tokens - {"no-2"},
            evidenced=all_tokens - {"no-2"},
        )
    elif mutation == "old_generation":
        _publish_current_membership(runtime, frozen_pairs, generation=4)
        runtime.update_membership(
            WsMembershipSnapshot(
                generation=5,
                desired=all_tokens,
                committed=all_tokens,
            )
        )
    else:
        _publish_current_membership(runtime, frozen_pairs)
    store = SimpleNamespace(fetch_sampling_market_state=AsyncMock(return_value=frozen_pairs))

    batch = await l3_sampler.collect_sample(
        scheduled_at=START,
        sample_seq=0,
        settings=_settings(),
        ws_consumer=_ConsumerWithoutMembershipReads(),
        reconciliation_state=_reconciliation(),
        runtime=runtime,
        store=store,
    )

    market = next(row for row in batch.markets if row.market_id == "market-2")
    assert market.status is HealthStatus.FAIL
    assert market.reason_code == expected_reason
    assert batch.health.status is HealthStatus.FAIL


@pytest.mark.parametrize(
    ("evidenced_at", "book_at", "expected_reason"),
    [
        (START - timedelta(seconds=130), START - timedelta(seconds=1), "yes_evidence_stale"),
        (START + timedelta(seconds=1), START - timedelta(seconds=1), "yes_evidence_in_future"),
        (START - timedelta(seconds=5), START - timedelta(seconds=6), "yes_book_before_evidence"),
    ],
)
async def test_market_status_requires_fresh_books_from_current_generation_evidence(
    evidenced_at: datetime,
    book_at: datetime,
    expected_reason: str,
):
    pairs = list(_pairs())
    pairs[2] = replace(pairs[2], yes_book_at=book_at)
    frozen_pairs = tuple(pairs)
    runtime = _runtime()
    _publish_current_membership(runtime, frozen_pairs, evidenced_at=evidenced_at)
    store = SimpleNamespace(fetch_sampling_market_state=AsyncMock(return_value=frozen_pairs))

    batch = await l3_sampler.collect_sample(
        scheduled_at=START,
        sample_seq=0,
        settings=_settings(),
        ws_consumer=_ConsumerWithoutMembershipReads(),
        reconciliation_state=_reconciliation(),
        runtime=runtime,
        store=store,
    )

    market = next(row for row in batch.markets if row.market_id == "market-2")
    assert market.status is HealthStatus.FAIL
    assert market.reason_code == expected_reason
    assert batch.health.status is HealthStatus.FAIL


async def test_sample_once_advances_runtime_only_after_true_append(
    monkeypatch: pytest.MonkeyPatch,
):
    pairs = _pairs()
    runtime = _runtime()
    _publish_current_membership(runtime, pairs)
    store = SimpleNamespace(
        fetch_sampling_market_state=AsyncMock(return_value=pairs),
        append_sample=AsyncMock(side_effect=[False, True]),
    )
    kwargs = dict(
        settings=_settings(),
        ws_consumer=_ConsumerWithoutMembershipReads(),
        reconciliation_state=_reconciliation(),
        runtime=runtime,
        store=store,
    )

    clock = {"now": START}
    monkeypatch.setattr(l3_sampler, "_utc_now", lambda: clock["now"])
    assert not await l3_sampler.sample_once(
        scheduled_at=START,
        sample_seq=0,
        **kwargs,
    )
    failed = runtime.snapshot()
    assert failed.last_sample_persisted_at is None
    assert failed.writer_ok is False

    persisted_at = START + timedelta(seconds=30)
    clock["now"] = persisted_at
    assert await l3_sampler.sample_once(
        scheduled_at=persisted_at,
        sample_seq=1,
        **kwargs,
    )
    succeeded = runtime.snapshot()
    assert succeeded.last_sample_persisted_at == persisted_at
    assert len(succeeded.last_market_samples) == 5
    assert [call.args[0].health.sample_seq for call in store.append_sample.await_args_list] == [
        0,
        1,
    ]


async def test_sample_once_propagates_cancellation_without_advancing_anchor():
    pairs = _pairs()
    runtime = _runtime()
    _publish_current_membership(runtime, pairs)
    store = SimpleNamespace(
        fetch_sampling_market_state=AsyncMock(return_value=pairs),
        append_sample=AsyncMock(side_effect=asyncio.CancelledError),
    )

    with pytest.raises(asyncio.CancelledError):
        await l3_sampler.sample_once(
            scheduled_at=START,
            sample_seq=0,
            settings=_settings(),
            ws_consumer=_ConsumerWithoutMembershipReads(),
            reconciliation_state=_reconciliation(),
            runtime=runtime,
            store=store,
        )

    assert runtime.snapshot().last_sample_persisted_at is None


async def test_run_sampler_emits_real_76_second_gap_and_skips_missed_boundaries(
    monkeypatch: pytest.MonkeyPatch,
):
    runtime = _runtime()
    clock = {"now": START}
    calls: list[tuple[int, datetime, datetime]] = []
    stop_event = asyncio.Event()

    async def _sample_once(**kwargs):
        calls.append(
            (kwargs["sample_seq"], kwargs["scheduled_at"], l3_sampler._utc_now())
        )
        if len(calls) == 2:
            stop_event.set()
        return True

    async def _wait_for_stop(_stop_event, delay_s):
        assert delay_s == pytest.approx(30.0)
        clock["now"] = START + timedelta(seconds=76)
        return False

    monkeypatch.setattr(l3_sampler, "_utc_now", lambda: clock["now"])
    monkeypatch.setattr(l3_sampler, "_wait_for_stop", _wait_for_stop)
    monkeypatch.setattr(l3_sampler, "sample_once", _sample_once)

    await l3_sampler.run_sampler(
        stop_event,
        settings=_settings(),
        ws_consumer=_ConsumerWithoutMembershipReads(),
        reconciliation_state=_reconciliation(),
        runtime=runtime,
        store=SimpleNamespace(),
    )

    assert calls == [
        (0, START, START),
        (2, START + timedelta(seconds=60), START + timedelta(seconds=76)),
    ]


async def test_run_sampler_rechecks_clock_after_early_timer_wakeup(
    monkeypatch: pytest.MonkeyPatch,
):
    runtime = _runtime()
    clock = {"now": START}
    calls: list[tuple[int, datetime, datetime]] = []
    delays: list[float] = []
    stop_event = asyncio.Event()

    async def _sample_once(**kwargs):
        calls.append(
            (kwargs["sample_seq"], kwargs["scheduled_at"], l3_sampler._utc_now())
        )
        if len(calls) == 2:
            stop_event.set()
        return True

    async def _wait_for_stop(_stop_event, delay_s):
        delays.append(delay_s)
        clock["now"] += timedelta(seconds=delay_s)
        if len(delays) == 1:
            clock["now"] -= timedelta(milliseconds=1)
        else:
            clock["now"] += timedelta(milliseconds=1)
        return False

    monkeypatch.setattr(l3_sampler, "_utc_now", lambda: clock["now"])
    monkeypatch.setattr(l3_sampler, "_wait_for_stop", _wait_for_stop)
    monkeypatch.setattr(l3_sampler, "sample_once", _sample_once)

    await l3_sampler.run_sampler(
        stop_event,
        settings=_settings(),
        ws_consumer=_ConsumerWithoutMembershipReads(),
        reconciliation_state=_reconciliation(),
        runtime=runtime,
        store=SimpleNamespace(),
    )

    assert calls == [
        (0, START, START),
        (1, START + timedelta(seconds=30), START + timedelta(milliseconds=30_001)),
    ]
    assert delays == pytest.approx([30.0, 0.001])


async def test_pre_t0_missed_boundary_does_not_poison_complete_later_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = AcceptanceConfig(
        recipe_sha256="b" * 64,
        sample_interval_s=30,
        max_sample_gap_s=75,
        promote_interval_s=300,
        promote_max_start_gap_s=360,
        market_book_fresh_s=120,
        market_ohlc_fresh_s=120,
        expected_market_count=5,
        expected_token_count=10,
        retention_days=30,
        schema_revision="007",
        code_version="code-1",
    )
    image_digest = "c" * 64
    runtime = L3EvidenceRuntime(
        RuntimeIdentity(
            machine_id="machine-1",
            machine_version="version-1",
            image_ref=f"image@sha256:{image_digest}",
            release_id="release-1",
            code_version=config.code_version,
            recipe_sha256=config.recipe_sha256,
            acceptance_config_hash=config.digest(),
        ),
        started_at=START,
    )
    _publish_current_membership(runtime, _pairs(), evidenced_at=START)
    clock = {"now": START}
    batches = []
    stop_event = asyncio.Event()

    async def _fetch(_token_ids):
        return _pairs(clock["now"])

    async def _sample_once(**kwargs):
        collect_kwargs = {
            **kwargs,
            "store": SimpleNamespace(fetch_sampling_market_state=_fetch),
        }
        batch = await l3_sampler.collect_sample(**collect_kwargs)
        batches.append(batch)
        if len(batches) == 2:
            stop_event.set()
        return True

    async def _wait_for_stop(_stop_event, delay_s):
        assert delay_s == pytest.approx(30.0)
        clock["now"] = START + timedelta(seconds=76)
        return False

    monkeypatch.setattr(l3_sampler, "_utc_now", lambda: clock["now"])
    monkeypatch.setattr(l3_sampler, "_wait_for_stop", _wait_for_stop)
    monkeypatch.setattr(l3_sampler, "sample_once", _sample_once)

    await l3_sampler.run_sampler(
        stop_event,
        settings=_settings(),
        ws_consumer=_ConsumerWithoutMembershipReads(),
        reconciliation_state=_reconciliation(),
        runtime=runtime,
        store=SimpleNamespace(),
    )

    later_batch = batches[1]
    t0 = START + timedelta(seconds=60)
    end = t0 + timedelta(seconds=30)
    assert (later_batch.health.sample_seq, later_batch.health.scheduled_at) == (2, t0)

    mapping_rows = tuple(
        {
            "market_id": row.market_id,
            "yes_token_id": row.yes_token_id,
            "no_token_id": row.no_token_id,
        }
        for row in later_batch.markets
    )
    mapping_hash = stable_sha256(mapping_rows)
    reports = tuple(
        ManifestReport(
            checkpoint=label,
            start=t0,
            end=(
                end
                if hours == 0
                else t0 + timedelta(hours=hours)
            ),
            path=f"reports/{label.lower().replace('+', '')}.json",
        )
        for label, hours in (
            ("T+0", 0),
            ("T+6", 6),
            ("T+12", 12),
            ("T+18", 18),
            ("T+24", 24),
        )
    )
    status = runtime.snapshot()
    manifest = SoakManifest(
        schema_version=1,
        t0=t0,
        t24=t0 + timedelta(hours=24),
        reports=reports,
        boot_id=status.boot_id,
        machine_id="machine-1",
        machine_version="version-1",
        image_ref=f"image@sha256:{image_digest}",
        image_digest=image_digest,
        release_id="release-1",
        code_version=config.code_version,
        mapping_hash=mapping_hash,
        acceptance_config=config,
        acceptance_config_hash=config.digest(),
    )
    boot = RuntimeBootRecord(
        boot_id=status.boot_id,
        started_at=START,
        machine_id=manifest.machine_id,
        machine_version=manifest.machine_version,
        image_ref=manifest.image_ref,
        release_id=manifest.release_id,
        code_version=manifest.code_version,
        acceptance_config_hash=manifest.acceptance_config_hash,
    )

    def _raw(record: object, *, recorded_at: datetime, **extra: object):
        row = {
            field.name: getattr(record, field.name)
            for field in fields(record)  # type: ignore[arg-type]
        }
        row.update(extra)
        row["recorded_at"] = recorded_at
        return row

    evidence = EvidenceWindow(
        start=t0,
        end=end,
        boots=(boot,),
        promote_runs=(),
        health_samples=(later_batch.health,),
        market_samples=later_batch.markets,
        runtime_events=(),
        book_coverage_counts={
            token_id: 1
            for row in later_batch.markets
            for token_id in (row.yes_token_id, row.no_token_id)
        },
        yes_ohlc_coverage_counts={
            row.yes_token_id: 1 for row in later_batch.markets
        },
        raw_rows_by_table={
            "l3_runtime_boots": (
                _raw(boot, recorded_at=START + timedelta(seconds=1), stopped_at=None),
            ),
            "l3_promote_runs": (),
            "l3_health_samples": (
                _raw(
                    later_batch.health,
                    recorded_at=later_batch.health.sampled_at + timedelta(seconds=1),
                ),
            ),
            "l3_market_samples": tuple(
                _raw(row, recorded_at=row.sampled_at + timedelta(seconds=1))
                for row in later_batch.markets
            ),
            "l3_runtime_events": (),
        },
    )

    report = build_soak_report(evidence, manifest, t0, end, False)
    assert report.status is VerdictStatus.PASS
    assert report.reasons == ()


async def test_collect_sample_captures_actual_time_after_aggregate_fetch(
    monkeypatch: pytest.MonkeyPatch,
):
    pairs = _pairs(START + timedelta(seconds=5))
    runtime = _runtime()
    _publish_current_membership(runtime, pairs)
    clock = {"now": START}

    async def _fetch(_token_ids):
        clock["now"] = START + timedelta(seconds=5)
        return pairs

    monkeypatch.setattr(l3_sampler, "_utc_now", lambda: clock["now"])
    batch = await l3_sampler.collect_sample(
        scheduled_at=START,
        sample_seq=0,
        settings=_settings(),
        ws_consumer=_ConsumerWithoutMembershipReads(),
        reconciliation_state=_reconciliation(),
        runtime=runtime,
        store=SimpleNamespace(fetch_sampling_market_state=_fetch),
    )

    assert batch.health.scheduled_at == START
    assert batch.health.sampled_at == START + timedelta(seconds=5)
    assert all(row.sampled_at == batch.health.sampled_at for row in batch.markets)


async def test_collect_sample_discards_slot_when_membership_changes_during_fetch():
    pairs = _pairs()
    runtime = _runtime()
    _publish_current_membership(runtime, pairs, generation=4)

    async def _fetch(_token_ids):
        runtime.update_membership(
            WsMembershipSnapshot(
                generation=5,
                desired=_tokens(pairs),
                committed=_tokens(pairs),
            )
        )
        return pairs

    with pytest.raises(ValueError, match="membership changed during aggregate fetch"):
        await l3_sampler.collect_sample(
            scheduled_at=START,
            sample_seq=0,
            settings=_settings(),
            ws_consumer=_ConsumerWithoutMembershipReads(),
            reconciliation_state=_reconciliation(),
            runtime=runtime,
            store=SimpleNamespace(fetch_sampling_market_state=_fetch),
        )


async def test_collect_sample_accepts_newer_evidence_times_in_same_generation():
    pairs = _pairs()
    runtime = _runtime()
    _publish_current_membership(
        runtime,
        pairs,
        generation=4,
        evidenced_at=START - timedelta(seconds=20),
    )

    async def _fetch(_token_ids):
        _publish_current_membership(
            runtime,
            pairs,
            generation=4,
            evidenced_at=START - timedelta(seconds=5),
        )
        return pairs

    batch = await l3_sampler.collect_sample(
        scheduled_at=START,
        sample_seq=0,
        settings=_settings(),
        ws_consumer=_ConsumerWithoutMembershipReads(),
        reconciliation_state=_reconciliation(),
        runtime=runtime,
        store=SimpleNamespace(fetch_sampling_market_state=_fetch),
    )

    assert batch.health.status is HealthStatus.PASS
    assert all(row.status is HealthStatus.PASS for row in batch.markets)


async def test_collect_sample_retries_transient_aggregate_read_within_slot():
    pairs = _pairs()
    runtime = _runtime()
    _publish_current_membership(runtime, pairs)
    store = SimpleNamespace(
        fetch_sampling_market_state=AsyncMock(
            side_effect=[
                L3EvidenceReadError("transient"),
                TimeoutError,
                pairs,
            ]
        )
    )

    batch = await l3_sampler.collect_sample(
        scheduled_at=START,
        sample_seq=0,
        settings=_settings(),
        ws_consumer=_ConsumerWithoutMembershipReads(),
        reconciliation_state=_reconciliation(),
        runtime=runtime,
        store=store,
    )

    assert store.fetch_sampling_market_state.await_count == 3
    assert batch.health.status is HealthStatus.PASS


async def test_collect_sample_bounds_hung_aggregate_read_then_retries(
    monkeypatch: pytest.MonkeyPatch,
):
    pairs = _pairs()
    runtime = _runtime()
    _publish_current_membership(runtime, pairs)
    never_returns = asyncio.Event()
    attempts = 0

    async def _fetch(_token_ids):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            await never_returns.wait()
        return pairs

    monkeypatch.setattr(l3_sampler, "_AGGREGATE_READ_TIMEOUT_S", 0.01)
    batch = await asyncio.wait_for(
        l3_sampler.collect_sample(
            scheduled_at=START,
            sample_seq=0,
            settings=_settings(),
            ws_consumer=_ConsumerWithoutMembershipReads(),
            reconciliation_state=_reconciliation(),
            runtime=runtime,
            store=SimpleNamespace(fetch_sampling_market_state=_fetch),
        ),
        timeout=0.2,
    )

    assert attempts == 2
    assert batch.health.status is HealthStatus.PASS


async def test_writer_gap_and_restart_keep_sequences_and_boot_ids_queryable(
    monkeypatch: pytest.MonkeyPatch,
):
    pairs = _pairs()
    store = SimpleNamespace(
        fetch_sampling_market_state=AsyncMock(return_value=pairs),
        append_sample=AsyncMock(side_effect=[True, False, True, True]),
    )
    runtime_one = _runtime()
    _publish_current_membership(runtime_one, pairs)
    kwargs = dict(
        settings=_settings(),
        ws_consumer=_ConsumerWithoutMembershipReads(),
        reconciliation_state=_reconciliation(),
        store=store,
    )

    clock = {"now": START}
    monkeypatch.setattr(l3_sampler, "_utc_now", lambda: clock["now"])
    for seq, scheduled_offset, actual_offset in ((0, 0, 0), (1, 30, 30), (2, 60, 76)):
        clock["now"] = START + timedelta(seconds=actual_offset)
        await l3_sampler.sample_once(
            scheduled_at=START + timedelta(seconds=scheduled_offset),
            sample_seq=seq,
            runtime=runtime_one,
            **kwargs,
        )

    runtime_two = _runtime(started_at=START + timedelta(seconds=100))
    _publish_current_membership(runtime_two, pairs)
    clock["now"] = START + timedelta(seconds=100)
    await l3_sampler.sample_once(
        scheduled_at=START + timedelta(seconds=100),
        sample_seq=0,
        runtime=runtime_two,
        **kwargs,
    )

    persisted = [
        call.args[0]
        for call, outcome in zip(
            store.append_sample.await_args_list,
            (True, False, True, True),
            strict=True,
        )
        if outcome
    ]
    first_boot = runtime_one.snapshot().boot_id
    second_boot = runtime_two.snapshot().boot_id
    assert first_boot != second_boot
    assert [(batch.health.boot_id, batch.health.sample_seq) for batch in persisted] == [
        (first_boot, 0),
        (first_boot, 2),
        (second_boot, 0),
    ]
    assert [batch.health.sampled_at for batch in persisted] == [
        START,
        START + timedelta(seconds=76),
        START + timedelta(seconds=100),
    ]
