"""Atomic, non-backfilling L3 process/per-market sampler tests."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from polyarb.events.reconciliation import ReconciliationState
from polyarb.observation import l3_sampler
from polyarb.observation.l3_evidence import (
    HealthStatus,
    L3EvidenceRuntime,
    RuntimeIdentity,
    WsMembershipSnapshot,
)
from polyarb.storage.l3_evidence_store import SamplingMarketState

HASH = "a" * 64
START = datetime(2026, 7, 23, 5, 0, tzinfo=UTC)


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
) -> None:
    all_tokens = _tokens(pairs)
    current_evidenced = all_tokens if evidenced is None else evidenced
    runtime.update_membership(
        WsMembershipSnapshot(
            generation=generation,
            desired=all_tokens if desired is None else desired,
            committed=all_tokens if committed is None else committed,
            evidenced=current_evidenced,
            evidenced_at={token: START for token in current_evidenced},
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
        sampled_at=START,
        sample_seq=7,
        settings=_settings(),
        ws_consumer=_ConsumerWithoutMembershipReads(),
        reconciliation_state=_reconciliation(),
        runtime=counting_runtime,
        store=store,
    )

    assert counting_runtime.snapshot_calls == 1
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
            sampled_at=START,
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
        sampled_at=START,
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
        sampled_at=START,
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


async def test_sample_once_advances_runtime_only_after_true_append():
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

    assert not await l3_sampler.sample_once(
        sampled_at=START,
        sample_seq=0,
        **kwargs,
    )
    failed = runtime.snapshot()
    assert failed.last_sample_persisted_at is None
    assert failed.writer_ok is False

    persisted_at = START + timedelta(seconds=30)
    assert await l3_sampler.sample_once(
        sampled_at=persisted_at,
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
            sampled_at=START,
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
    calls: list[tuple[int, datetime]] = []
    stop_event = asyncio.Event()

    async def _sample_once(**kwargs):
        calls.append((kwargs["sample_seq"], kwargs["sampled_at"]))
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

    assert calls == [(0, START), (1, START + timedelta(seconds=76))]


async def test_writer_gap_and_restart_keep_sequences_and_boot_ids_queryable():
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

    for seq, offset in ((0, 0), (1, 30), (2, 76)):
        await l3_sampler.sample_once(
            sampled_at=START + timedelta(seconds=offset),
            sample_seq=seq,
            runtime=runtime_one,
            **kwargs,
        )

    runtime_two = _runtime(started_at=START + timedelta(seconds=100))
    _publish_current_membership(runtime_two, pairs)
    await l3_sampler.sample_once(
        sampled_at=START + timedelta(seconds=100),
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
