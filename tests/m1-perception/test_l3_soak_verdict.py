from __future__ import annotations

from dataclasses import fields, replace
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from uuid import UUID, uuid5

import pytest

from polyarb.observation.l3_evidence import (
    AcceptanceConfig,
    EvidenceWindow,
    HealthSampleRecord,
    HealthStatus,
    MarketSampleRecord,
    PromoteRunRecord,
    PromoteStatus,
    RuntimeBootRecord,
    RuntimeEventKind,
    RuntimeEventRecord,
    RuntimeEventSeverity,
    stable_sha256,
)
from polyarb.observation.l3_soak_verdict import (
    ManifestReport,
    SoakManifest,
    VerdictStatus,
    build_soak_report,
    render_report,
)

SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64
BOOT_ID = UUID("00000000-0000-0000-0000-000000000054")
T0 = datetime(2026, 8, 1, tzinfo=UTC)
T24 = T0 + timedelta(hours=24)
T6 = T0 + timedelta(hours=6)
MAPPING_ROWS = tuple(
    {
        "market_id": f"market-{index}",
        "yes_token_id": f"yes-{index}",
        "no_token_id": f"no-{index}",
    }
    for index in range(5)
)
MAPPING_HASH = stable_sha256(MAPPING_ROWS)


def _acceptance() -> AcceptanceConfig:
    return AcceptanceConfig(
        recipe_sha256=SHA_A,
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
        code_version="0.1.0",
    )


def _manifest(
    *,
    boot_id: UUID = BOOT_ID,
    mapping_hash: str = MAPPING_HASH,
    acceptance: AcceptanceConfig | None = None,
    exceptions: frozenset[RuntimeEventKind] = frozenset(),
) -> SoakManifest:
    config = acceptance or _acceptance()
    reports = tuple(
        ManifestReport(
            checkpoint=label,
            start=T0,
            end=(
                T0 + timedelta(seconds=config.sample_interval_s)
                if hours == 0
                else T0 + timedelta(hours=hours)
            ),
            path=f"reports/{label.lower().replace('+', '')}.json",
        )
        for label, hours in (("T+0", 0), ("T+6", 6), ("T+12", 12), ("T+18", 18), ("T+24", 24))
    )
    return SoakManifest(
        schema_version=1,
        t0=T0,
        t24=T24,
        reports=reports,
        boot_id=boot_id,
        machine_id="machine-1",
        machine_version="v42",
        image_ref="registry.example/polyarb@sha256:" + SHA_C,
        image_digest=SHA_C,
        release_id="release-42",
        code_version=config.code_version,
        mapping_hash=mapping_hash,
        acceptance_config=config,
        acceptance_config_hash=config.digest(),
        allowed_event_kind_exceptions=exceptions,
    )


def _raw(record: object, *, recorded_at: datetime, **extra: object) -> dict[str, object]:
    row = {field.name: getattr(record, field.name) for field in fields(record)}  # type: ignore[arg-type]
    if "detail" in row:
        row["detail"] = dict(row["detail"])  # type: ignore[arg-type]
    row.update(extra)
    row["recorded_at"] = recorded_at
    return row


def _golden_window(*, events: tuple[RuntimeEventRecord, ...] = ()) -> EvidenceWindow:
    manifest = _manifest()
    config_hash = manifest.acceptance_config_hash
    boot = RuntimeBootRecord(
        boot_id=BOOT_ID,
        started_at=T0,
        machine_id=manifest.machine_id,
        machine_version=manifest.machine_version,
        image_ref=manifest.image_ref,
        release_id=manifest.release_id,
        code_version=manifest.code_version,
        acceptance_config_hash=config_hash,
    )
    promotes: list[PromoteRunRecord] = []
    for run_seq in range(72):
        scheduled = T0 + timedelta(seconds=run_seq * 300)
        promotes.append(
            PromoteRunRecord(
                boot_id=BOOT_ID,
                run_seq=run_seq,
                scheduled_at=scheduled,
                started_at=scheduled + timedelta(seconds=1),
                finished_at=scheduled + timedelta(seconds=2),
                status=PromoteStatus.SUCCESS,
                reason_code="ok",
                selected_count=5,
                desired_count=10,
                committed_count=10,
                evidenced_count=10,
                add_count=0,
                remove_count=0,
                mapping_hash=MAPPING_HASH,
                desired_hash=SHA_A,
                committed_hash=SHA_A,
                acceptance_config_hash=config_hash,
                ws_generation=1,
                add_succeeded=None,
                remove_succeeded=None,
                mirror_succeeded=True,
                duration_ms=2_000,
            )
        )

    health: list[HealthSampleRecord] = []
    markets: list[MarketSampleRecord] = []
    # Sixty seconds is deliberately below the locked 75-second maximum gap.
    for sample_seq in range(360):
        scheduled = T0 + timedelta(seconds=sample_seq * 60)
        sampled = scheduled + timedelta(seconds=5)
        health.append(
            HealthSampleRecord(
                boot_id=BOOT_ID,
                sample_seq=sample_seq,
                scheduled_at=scheduled,
                sampled_at=sampled,
                desired_count=10,
                committed_count=10,
                evidenced_count=10,
                promote_age_ms=1_000,
                global_book_age_ms=1_000,
                ws_age_ms=1_000,
                mirror_age_ms=1_000,
                candidate_age_ms=1_000,
                reconciliation_age_ms=1_000,
                listener_state="listening",
                cursor_lag=0,
                watchdog_count=0,
                reconnect_count=0,
                ws_generation=1,
                mapping_hash=MAPPING_HASH,
                acceptance_config_hash=config_hash,
                status=HealthStatus.PASS,
                reason_code="ok",
            )
        )
        for market_index in range(5):
            markets.append(
                MarketSampleRecord(
                    boot_id=BOOT_ID,
                    sample_seq=sample_seq,
                    sampled_at=sampled,
                    market_id=f"market-{market_index}",
                    yes_token_id=f"yes-{market_index}",
                    no_token_id=f"no-{market_index}",
                    yes_desired=True,
                    no_desired=True,
                    yes_committed=True,
                    no_committed=True,
                    yes_evidenced=True,
                    no_evidenced=True,
                    evidence_generation=1,
                    yes_book_at=sampled - timedelta(seconds=1),
                    no_book_at=sampled - timedelta(seconds=1),
                    yes_book_age_ms=1_000,
                    no_book_age_ms=1_000,
                    worst_book_age_ms=1_000,
                    yes_ohlc_at=sampled - timedelta(seconds=1),
                    yes_ohlc_age_ms=1_000,
                    status=HealthStatus.PASS,
                    reason_code="ok",
                )
            )

    recorded = T6
    raw_rows = {
        "l3_runtime_boots": (_raw(boot, recorded_at=recorded, stopped_at=None),),
        "l3_promote_runs": tuple(
            _raw(row, recorded_at=recorded, id=index + 1) for index, row in enumerate(promotes)
        ),
        "l3_health_samples": tuple(
            _raw(row, recorded_at=row.sampled_at + timedelta(seconds=1)) for row in health
        ),
        "l3_market_samples": tuple(
            _raw(row, recorded_at=row.sampled_at + timedelta(seconds=1)) for row in markets
        ),
        "l3_runtime_events": tuple(_raw(row, recorded_at=recorded) for row in events),
    }
    return EvidenceWindow(
        start=T0,
        end=T6,
        boots=(boot,),
        promote_runs=tuple(promotes),
        health_samples=tuple(health),
        market_samples=tuple(markets),
        runtime_events=events,
        book_coverage_counts={
            token: 1 for index in range(5) for token in (f"yes-{index}", f"no-{index}")
        },
        yes_ohlc_coverage_counts={f"yes-{index}": 1 for index in range(5)},
        raw_rows_by_table=raw_rows,
    )


def _t0_window() -> EvidenceWindow:
    golden = _golden_window()
    end = T0 + timedelta(seconds=30)
    raw = {
        "l3_runtime_boots": golden.raw_rows_by_table["l3_runtime_boots"],
        "l3_promote_runs": golden.raw_rows_by_table["l3_promote_runs"][:1],
        "l3_health_samples": golden.raw_rows_by_table["l3_health_samples"][:1],
        "l3_market_samples": golden.raw_rows_by_table["l3_market_samples"][:5],
        "l3_runtime_events": (),
    }
    return EvidenceWindow(
        start=T0,
        end=end,
        boots=golden.boots,
        promote_runs=golden.promote_runs[:1],
        health_samples=golden.health_samples[:1],
        market_samples=golden.market_samples[:5],
        runtime_events=(),
        book_coverage_counts=golden.book_coverage_counts,
        yes_ohlc_coverage_counts=golden.yes_ohlc_coverage_counts,
        raw_rows_by_table=raw,
    )


@pytest.fixture(scope="module")
def manifest() -> SoakManifest:
    return _manifest()


@pytest.fixture(scope="module")
def golden() -> EvidenceWindow:
    return _golden_window()


def _report(window: EvidenceWindow, manifest: SoakManifest, *, require_24h: bool = False):
    return build_soak_report(window, manifest, window.start, window.end, require_24h)


def _codes(report: object) -> set[str]:
    return {reason.code for reason in report.reasons}  # type: ignore[attr-defined]


def test_golden_exact_checkpoint_passes_and_renders_deterministically(
    golden: EvidenceWindow, manifest: SoakManifest
) -> None:
    report = _report(golden, manifest)
    assert report.status is VerdictStatus.PASS
    assert report.reasons == ()
    assert report.manifest_hash == manifest.manifest_hash
    assert report.max_schedule_lag_seconds == 5.0
    assert render_report(report) == render_report(report)
    assert "PASS" in render_report(report)


def test_real_t0_sample_interval_artifact_passes(manifest: SoakManifest) -> None:
    window = _t0_window()
    report = build_soak_report(window, manifest, T0, T0 + timedelta(seconds=30), False)
    assert report.status is VerdictStatus.PASS


def test_health_schedule_must_remain_on_exact_boot_grid(
    golden: EvidenceWindow, manifest: SoakManifest
) -> None:
    changed = replace(
        golden.health_samples[3],
        scheduled_at=golden.health_samples[3].scheduled_at + timedelta(microseconds=1),
    )
    raw = {key: tuple(rows) for key, rows in golden.raw_rows_by_table.items()}
    raw_health = list(raw["l3_health_samples"])
    raw_health[3] = _raw(changed, recorded_at=T6)
    raw["l3_health_samples"] = tuple(raw_health)
    report = _report(
        replace(
            golden,
            health_samples=golden.health_samples[:3]
            + (changed,)
            + golden.health_samples[4:],
            raw_rows_by_table=raw,
        ),
        manifest,
    )
    assert "sample_schedule_grid" in _codes(report)


@pytest.mark.parametrize("offset_seconds", [-1, 30])
def test_raw_sample_recording_window_is_fail_closed(
    golden: EvidenceWindow,
    manifest: SoakManifest,
    offset_seconds: int,
) -> None:
    raw = {key: tuple(rows) for key, rows in golden.raw_rows_by_table.items()}
    health = list(raw["l3_health_samples"])
    first = dict(health[0])
    first["recorded_at"] = first["sampled_at"] + timedelta(seconds=offset_seconds)
    health[0] = first
    raw["l3_health_samples"] = tuple(health)
    report = _report(replace(golden, raw_rows_by_table=raw), manifest)
    assert "sample_recording_window" in _codes(report)


def test_report_hashes_and_renders_complete_r06_operator_evidence(
    golden: EvidenceWindow, manifest: SoakManifest
) -> None:
    report = _report(golden, manifest)
    assert set(report.per_market_freshness_ms) == {f"market-{index}" for index in range(5)}
    assert report.per_market_freshness_ms["market-0"] == {
        "yes_book": 1_000,
        "no_book": 1_000,
        "worst_book": 1_000,
        "yes_ohlc": 1_000,
    }
    rendered = render_report(report)
    for expected in (
        "Require 24h",
        "Image ref",
        "Image digest",
        "Acceptance config hash",
        "Mapping hash",
        "Minimum cardinality",
        "Maximum freshness",
        "Per-market freshness",
        "Event counts",
        "Book coverage",
        "Yes OHLC coverage",
    ):
        assert expected in rendered
    assert "market-0" in rendered


def test_manifest_is_required_and_window_against_different_manifest_is_not_closed(
    golden: EvidenceWindow, manifest: SoakManifest
) -> None:
    with pytest.raises(TypeError, match="SoakManifest"):
        build_soak_report(golden, None, T0, T6, False)  # type: ignore[arg-type]
    mismatched = _manifest(mapping_hash=SHA_C)
    report = _report(golden, mismatched)
    assert report.status is VerdictStatus.NOT_CLOSED
    assert "mapping_hash_mismatch" in _codes(report)
    assert mismatched.manifest_hash != manifest.manifest_hash


def test_exact_window_bounds_are_required(golden: EvidenceWindow, manifest: SoakManifest) -> None:
    report = build_soak_report(golden, manifest, T0, T6 + timedelta(seconds=1), False)
    assert report.status is VerdictStatus.NOT_CLOSED
    assert "window_bounds_mismatch" in _codes(report)


def test_second_boot_is_not_closed(golden: EvidenceWindow, manifest: SoakManifest) -> None:
    other = replace(golden.boots[0], boot_id=UUID(int=55), started_at=T0 + timedelta(hours=1))
    report = _report(replace(golden, boots=golden.boots + (other,)), manifest)
    assert "boot_cardinality" in _codes(report)


def test_boot_must_start_no_later_than_t0(golden: EvidenceWindow, manifest: SoakManifest) -> None:
    boot = replace(golden.boots[0], started_at=T0 + timedelta(seconds=1))
    report = _report(replace(golden, boots=(boot,)), manifest)
    assert "boot_after_window_start" in _codes(report)


def test_no_decoded_occurrence_may_precede_boot(
    golden: EvidenceWindow, manifest: SoakManifest
) -> None:
    boot = replace(golden.boots[0], started_at=T0 - timedelta(microseconds=500))
    changed = replace(
        golden.health_samples[0],
        scheduled_at=T0 - timedelta(milliseconds=1),
        sampled_at=T0 - timedelta(milliseconds=1),
    )
    report = _report(
        replace(golden, boots=(boot,), health_samples=(changed,) + golden.health_samples[1:]),
        manifest,
    )
    assert "occurrence_before_boot" in _codes(report)


@pytest.mark.parametrize(
    ("field", "value", "code"),
    [
        ("machine_version", "v43", "identity_mismatch"),
        ("acceptance_config_hash", SHA_C, "acceptance_config_hash_mismatch"),
    ],
)
def test_boot_identity_and_config_change_are_not_closed(
    golden: EvidenceWindow,
    manifest: SoakManifest,
    field: str,
    value: str,
    code: str,
) -> None:
    report = _report(replace(golden, boots=(replace(golden.boots[0], **{field: value}),)), manifest)
    assert code in _codes(report)


def test_health_mapping_or_config_change_is_not_closed(
    golden: EvidenceWindow, manifest: SoakManifest
) -> None:
    changed = replace(golden.health_samples[10], mapping_hash=SHA_C)
    report = _report(
        replace(
            golden,
            health_samples=golden.health_samples[:10] + (changed,) + golden.health_samples[11:],
        ),
        manifest,
    )
    assert "mapping_hash_mismatch" in _codes(report)


def test_replacing_all_pairs_and_coverage_cannot_reuse_manifest_mapping_hash(
    golden: EvidenceWindow, manifest: SoakManifest
) -> None:
    changed_markets = tuple(
        replace(
            row,
            market_id=f"other-{row.market_id}",
            yes_token_id=f"other-{row.yes_token_id}",
            no_token_id=f"other-{row.no_token_id}",
        )
        for row in golden.market_samples
    )
    raw = dict(golden.raw_rows_by_table)
    raw["l3_market_samples"] = tuple(_raw(row, recorded_at=T6) for row in changed_markets)
    window = replace(
        golden,
        market_samples=changed_markets,
        book_coverage_counts={
            token: 1 for index in range(5) for token in (f"other-yes-{index}", f"other-no-{index}")
        },
        yes_ohlc_coverage_counts={f"other-yes-{index}": 1 for index in range(5)},
        raw_rows_by_table=raw,
    )
    assert "mapping_identity_hash_mismatch" in _codes(_report(window, manifest))


def test_76_second_sample_gap_is_not_closed(golden: EvidenceWindow, manifest: SoakManifest) -> None:
    health = tuple(row for row in golden.health_samples if row.sample_seq != 1)
    markets = tuple(row for row in golden.market_samples if row.sample_seq != 1)
    report = _report(replace(golden, health_samples=health, market_samples=markets), manifest)
    assert "sample_gap" in _codes(report)


def test_missing_or_noncontiguous_promoter_tick_is_not_closed(
    golden: EvidenceWindow, manifest: SoakManifest
) -> None:
    report = _report(replace(golden, promote_runs=golden.promote_runs[1:]), manifest)
    assert "promoter_tick_missing" in _codes(report)


def test_361_second_promoter_start_gap_is_not_closed(
    golden: EvidenceWindow, manifest: SoakManifest
) -> None:
    changed = replace(
        golden.promote_runs[2],
        started_at=golden.promote_runs[2].scheduled_at + timedelta(seconds=361),
    )
    report = _report(
        replace(
            golden, promote_runs=golden.promote_runs[:2] + (changed,) + golden.promote_runs[3:]
        ),
        manifest,
    )
    assert "promoter_start_gap" in _codes(report)


def test_non_success_promoter_is_not_closed(golden: EvidenceWindow, manifest: SoakManifest) -> None:
    changed = replace(golden.promote_runs[2], status=PromoteStatus.FROZEN, reason_code="frozen")
    report = _report(
        replace(
            golden, promote_runs=golden.promote_runs[:2] + (changed,) + golden.promote_runs[3:]
        ),
        manifest,
    )
    assert "promoter_non_success" in _codes(report)


def test_successful_promoter_requires_equal_desired_and_committed_hashes(
    golden: EvidenceWindow, manifest: SoakManifest
) -> None:
    changed = replace(golden.promote_runs[2], desired_hash=SHA_C)
    report = _report(
        replace(
            golden,
            promote_runs=golden.promote_runs[:2] + (changed,) + golden.promote_runs[3:],
        ),
        manifest,
    )
    assert "promoter_membership_hash" in _codes(report)


@pytest.mark.parametrize(
    ("count_field", "result_field"),
    [("add_count", "add_succeeded"), ("remove_count", "remove_succeeded")],
)
def test_positive_promoter_operation_count_requires_true_result(
    golden: EvidenceWindow,
    manifest: SoakManifest,
    count_field: str,
    result_field: str,
) -> None:
    changed = replace(golden.promote_runs[2], **{count_field: 1, result_field: None})
    report = _report(
        replace(
            golden,
            promote_runs=golden.promote_runs[:2] + (changed,) + golden.promote_runs[3:],
        ),
        manifest,
    )
    assert "promoter_operation_result" in _codes(report)


@pytest.mark.parametrize(
    ("field", "value"),
    [("selected_count", 4), ("desired_count", 9), ("committed_count", 9), ("evidenced_count", 9)],
)
def test_promoter_5_10_10_cardinality_is_required(
    golden: EvidenceWindow, manifest: SoakManifest, field: str, value: int
) -> None:
    changed = replace(golden.promote_runs[2], **{field: value})
    report = _report(
        replace(
            golden, promote_runs=golden.promote_runs[:2] + (changed,) + golden.promote_runs[3:]
        ),
        manifest,
    )
    assert "promoter_cardinality" in _codes(report)


@pytest.mark.parametrize("field", ["desired_count", "committed_count", "evidenced_count"])
def test_sample_10_token_cardinality_is_required(
    golden: EvidenceWindow, manifest: SoakManifest, field: str
) -> None:
    changed = replace(golden.health_samples[2], **{field: 9})
    report = _report(
        replace(
            golden,
            health_samples=golden.health_samples[:2] + (changed,) + golden.health_samples[3:],
        ),
        manifest,
    )
    assert "sample_cardinality" in _codes(report)


@pytest.mark.parametrize("field", ["worst_book_age_ms", "yes_ohlc_age_ms"])
def test_120_second_book_or_ohlc_age_is_stale(
    golden: EvidenceWindow, manifest: SoakManifest, field: str
) -> None:
    changed = replace(golden.market_samples[0], **{field: 120_000})
    report = _report(
        replace(golden, market_samples=(changed,) + golden.market_samples[1:]), manifest
    )
    assert "market_freshness" in _codes(report)


@pytest.mark.parametrize("field", ["yes_book_age_ms", "no_book_age_ms"])
def test_each_side_book_age_is_independently_strict(
    golden: EvidenceWindow, manifest: SoakManifest, field: str
) -> None:
    changed = replace(golden.market_samples[0], **{field: 120_000})
    report = _report(
        replace(golden, market_samples=(changed,) + golden.market_samples[1:]),
        manifest,
    )
    assert "market_age_contract" in _codes(report)


def test_forged_low_worst_book_age_is_not_closed(
    golden: EvidenceWindow, manifest: SoakManifest
) -> None:
    changed = replace(
        golden.market_samples[0],
        yes_book_at=T0 - timedelta(seconds=120),
        yes_book_age_ms=120_000,
        worst_book_age_ms=1_000,
    )
    assert "market_age_contract" in _codes(
        _report(replace(golden, market_samples=(changed,) + golden.market_samples[1:]), manifest)
    )


@pytest.mark.parametrize(
    ("timestamp_field", "age_field"),
    [
        ("yes_book_at", "yes_book_age_ms"),
        ("no_book_at", "no_book_age_ms"),
        ("yes_ohlc_at", "yes_ohlc_age_ms"),
    ],
)
def test_source_timestamp_and_persisted_age_must_match_sampler_rounding(
    golden: EvidenceWindow,
    manifest: SoakManifest,
    timestamp_field: str,
    age_field: str,
) -> None:
    changed = replace(
        golden.market_samples[0],
        **{
            timestamp_field: golden.market_samples[0].sampled_at - timedelta(milliseconds=1_100),
            age_field: 1_000,
        },
    )
    assert "market_age_contract" in _codes(
        _report(replace(golden, market_samples=(changed,) + golden.market_samples[1:]), manifest)
    )


@pytest.mark.parametrize(
    "kind",
    [
        RuntimeEventKind.WATCHDOG_STALE,
        RuntimeEventKind.RECONNECT_FAILED,
        RuntimeEventKind.SUBSCRIPTION_CONTROL_FAILED,
        RuntimeEventKind.EVIDENCE_WRITER_FAILED,
    ],
)
def test_each_disallowed_event_kind_rejects_even_info_severity(kind: RuntimeEventKind) -> None:
    event = RuntimeEventRecord(
        event_id=uuid5(BOOT_ID, kind.value),
        boot_id=BOOT_ID,
        event_seq=1,
        occurred_at=T0 + timedelta(minutes=1),
        kind=kind,
        severity=RuntimeEventSeverity.INFO,
        reason_code="test",
        detail={},
    )
    report = _report(_golden_window(events=(event,)), _manifest())
    assert "disallowed_runtime_event" in _codes(report)


def test_exact_manifest_event_exception_passes_and_changes_hashes() -> None:
    kind = RuntimeEventKind.WATCHDOG_STALE
    event = RuntimeEventRecord(
        event_id=uuid5(BOOT_ID, kind.value),
        boot_id=BOOT_ID,
        event_seq=1,
        occurred_at=T0 + timedelta(minutes=1),
        kind=kind,
        severity=RuntimeEventSeverity.INFO,
        reason_code="test",
        detail={},
    )
    plain = _manifest()
    excepted = _manifest(exceptions=frozenset({kind}))
    report = _report(_golden_window(events=(event,)), excepted)
    assert report.status is VerdictStatus.PASS
    assert excepted.manifest_hash != plain.manifest_hash
    assert excepted.soak_hash != plain.soak_hash


def test_asyncpg_json_string_event_detail_matches_decoded_mapping() -> None:
    kind = RuntimeEventKind.WATCHDOG_STALE
    event = RuntimeEventRecord(
        event_id=UUID(int=60),
        boot_id=BOOT_ID,
        event_seq=1,
        occurred_at=T0 + timedelta(minutes=1),
        kind=kind,
        severity=RuntimeEventSeverity.INFO,
        reason_code="test",
        detail={"stale_seconds": 1},
    )
    window = _golden_window(events=(event,))
    raw = {key: tuple(rows) for key, rows in window.raw_rows_by_table.items()}
    raw_event = dict(raw["l3_runtime_events"][0])
    raw_event["detail"] = '{"stale_seconds": 1}'
    raw["l3_runtime_events"] = (raw_event,)
    report = _report(
        replace(window, raw_rows_by_table=raw),
        _manifest(exceptions=frozenset({kind})),
    )
    assert report.status is VerdictStatus.PASS


def test_invalid_raw_event_detail_json_is_not_closed() -> None:
    event = RuntimeEventRecord(
        event_id=UUID(int=61),
        boot_id=BOOT_ID,
        event_seq=1,
        occurred_at=T0 + timedelta(minutes=1),
        kind=RuntimeEventKind.RECONNECT_SUCCEEDED,
        reason_code="ok",
        detail={},
    )
    window = _golden_window(events=(event,))
    raw = {key: tuple(rows) for key, rows in window.raw_rows_by_table.items()}
    raw_event = dict(raw["l3_runtime_events"][0])
    raw_event["detail"] = "{not-json"
    raw["l3_runtime_events"] = (raw_event,)
    report = _report(replace(window, raw_rows_by_table=raw), _manifest())
    assert report.status is VerdictStatus.NOT_CLOSED
    assert "raw_event_detail_invalid" in _codes(report)


@pytest.mark.parametrize("number", ["NaN", "Infinity", "-Infinity", "1e999"])
def test_non_finite_raw_event_json_is_not_closed_without_raising(number: str) -> None:
    kind = RuntimeEventKind.WATCHDOG_STALE
    event = RuntimeEventRecord(
        event_id=uuid5(BOOT_ID, number),
        boot_id=BOOT_ID,
        event_seq=1,
        occurred_at=T0 + timedelta(minutes=1),
        kind=kind,
        reason_code="test",
        detail={"stale_seconds": 1},
    )
    window = _golden_window(events=(event,))
    raw = {key: tuple(rows) for key, rows in window.raw_rows_by_table.items()}
    raw_event = dict(raw["l3_runtime_events"][0])
    raw_event["detail"] = f'{{"stale_seconds": {number}}}'
    raw["l3_runtime_events"] = (raw_event,)
    report = _report(
        replace(window, raw_rows_by_table=raw),
        _manifest(exceptions=frozenset({kind})),
    )
    assert report.status is VerdictStatus.NOT_CLOSED
    assert "raw_event_detail_invalid" in _codes(report)


def test_exact_10_book_and_5_yes_ohlc_coverage_is_required(
    golden: EvidenceWindow, manifest: SoakManifest
) -> None:
    report = _report(replace(golden, book_coverage_counts={"yes-0": 1}), manifest)
    assert "book_coverage" in _codes(report)
    report = _report(replace(golden, yes_ohlc_coverage_counts={"yes-0": 1}), manifest)
    assert "ohlc_coverage" in _codes(report)


def test_acceptance_config_digest_mismatch_is_not_closed(golden: EvidenceWindow) -> None:
    config = _acceptance()
    manifest = replace(_manifest(acceptance=config), acceptance_config_hash=SHA_C)
    report = _report(golden, manifest)
    assert "acceptance_config_digest_mismatch" in _codes(report)


def test_raw_row_mutation_and_addition_change_digest_and_addition_is_not_closed(
    golden: EvidenceWindow, manifest: SoakManifest
) -> None:
    original = _report(golden, manifest)
    raw = {key: tuple(rows) for key, rows in golden.raw_rows_by_table.items()}
    first = dict(raw["l3_health_samples"][0])
    first["reason_code"] = "mutated"
    raw["l3_health_samples"] = (first,) + raw["l3_health_samples"][1:]
    mutated = _report(replace(golden, raw_rows_by_table=raw), manifest)
    assert mutated.raw_row_set_hash != original.raw_row_set_hash
    assert mutated.status is VerdictStatus.NOT_CLOSED
    assert "raw_decoded_mismatch" in _codes(mutated)

    raw["l3_health_samples"] += (dict(raw["l3_health_samples"][0]),)
    added = _report(replace(golden, raw_rows_by_table=raw), manifest)
    assert added.status is VerdictStatus.NOT_CLOSED
    assert "raw_row_count_mismatch" in _codes(added)


def test_raw_rows_require_all_tables_and_server_recorded_at(
    golden: EvidenceWindow, manifest: SoakManifest
) -> None:
    missing_table = dict(golden.raw_rows_by_table)
    missing_table.pop("l3_runtime_events")
    assert "raw_tables_missing" in _codes(
        _report(replace(golden, raw_rows_by_table=missing_table), manifest)
    )
    raw = {key: tuple(rows) for key, rows in golden.raw_rows_by_table.items()}
    first = dict(raw["l3_runtime_boots"][0])
    first.pop("recorded_at")
    raw["l3_runtime_boots"] = (first,)
    assert "raw_recorded_at_missing" in _codes(
        _report(replace(golden, raw_rows_by_table=raw), manifest)
    )


@pytest.mark.parametrize("defect", ["missing", "extra"])
def test_raw_rows_require_exact_migration_007_columns(
    golden: EvidenceWindow, manifest: SoakManifest, defect: str
) -> None:
    raw = {key: tuple(rows) for key, rows in golden.raw_rows_by_table.items()}
    boot = dict(raw["l3_runtime_boots"][0])
    if defect == "missing":
        boot.pop("stopped_at")
    else:
        boot["invented"] = "not-in-007"
    raw["l3_runtime_boots"] = (boot,)
    assert "raw_schema_mismatch" in _codes(
        _report(replace(golden, raw_rows_by_table=raw), manifest)
    )


def test_duplicate_raw_physical_primary_key_is_not_closed(
    golden: EvidenceWindow, manifest: SoakManifest
) -> None:
    raw = {key: tuple(rows) for key, rows in golden.raw_rows_by_table.items()}
    raw["l3_runtime_boots"] += (dict(raw["l3_runtime_boots"][0]),)
    assert "raw_duplicate_key" in _codes(_report(replace(golden, raw_rows_by_table=raw), manifest))


def test_duplicate_promoter_logical_key_is_not_closed(
    golden: EvidenceWindow, manifest: SoakManifest
) -> None:
    duplicate = golden.promote_runs[0]
    raw = {key: tuple(rows) for key, rows in golden.raw_rows_by_table.items()}
    raw["l3_promote_runs"] += (_raw(duplicate, recorded_at=T6, id=999_999),)
    window = replace(
        golden,
        promote_runs=golden.promote_runs + (duplicate,),
        raw_rows_by_table=raw,
    )
    assert "decoded_duplicate_key" in _codes(_report(window, manifest))
    assert "raw_duplicate_key" in _codes(_report(window, manifest))


def test_duplicate_event_physical_and_logical_keys_are_not_closed(
    golden: EvidenceWindow, manifest: SoakManifest
) -> None:
    event = RuntimeEventRecord(
        event_id=UUID(int=59),
        boot_id=BOOT_ID,
        event_seq=1,
        occurred_at=T0 + timedelta(seconds=1),
        kind=RuntimeEventKind.RECONNECT_SUCCEEDED,
        reason_code="ok",
        detail={},
    )
    raw = {key: tuple(rows) for key, rows in golden.raw_rows_by_table.items()}
    raw_event = _raw(event, recorded_at=T6)
    raw["l3_runtime_events"] = (raw_event, dict(raw_event))
    window = replace(
        golden,
        runtime_events=(event, event),
        raw_rows_by_table=raw,
    )
    assert "decoded_duplicate_key" in _codes(_report(window, manifest))
    assert "raw_duplicate_key" in _codes(_report(window, manifest))


def test_duplicate_health_and_market_unique_keys_are_not_closed(
    golden: EvidenceWindow, manifest: SoakManifest
) -> None:
    health_duplicate = golden.health_samples[0]
    market_duplicate = replace(golden.market_samples[0], market_id="another-market")
    raw = {key: tuple(rows) for key, rows in golden.raw_rows_by_table.items()}
    raw["l3_health_samples"] += (_raw(health_duplicate, recorded_at=T6),)
    raw["l3_market_samples"] += (_raw(market_duplicate, recorded_at=T6),)
    window = replace(
        golden,
        health_samples=golden.health_samples + (health_duplicate,),
        market_samples=golden.market_samples + (market_duplicate,),
        raw_rows_by_table=raw,
    )
    assert "decoded_duplicate_key" in _codes(_report(window, manifest))
    assert "raw_duplicate_key" in _codes(_report(window, manifest))


def test_market_child_timestamp_must_equal_health_parent(
    golden: EvidenceWindow, manifest: SoakManifest
) -> None:
    changed = replace(golden.market_samples[0], sampled_at=T0 + timedelta(milliseconds=1))
    report = _report(
        replace(golden, market_samples=(changed,) + golden.market_samples[1:]),
        manifest,
    )
    assert "market_parent_timestamp" in _codes(report)


def test_input_order_does_not_change_raw_or_report_hashes(
    golden: EvidenceWindow, manifest: SoakManifest
) -> None:
    raw = {key: tuple(reversed(rows)) for key, rows in golden.raw_rows_by_table.items()}
    shuffled = replace(
        golden,
        boots=tuple(reversed(golden.boots)),
        promote_runs=tuple(reversed(golden.promote_runs)),
        health_samples=tuple(reversed(golden.health_samples)),
        market_samples=tuple(reversed(golden.market_samples)),
        runtime_events=tuple(reversed(golden.runtime_events)),
        book_coverage_counts=MappingProxyType(
            dict(reversed(tuple(golden.book_coverage_counts.items())))
        ),
        yes_ohlc_coverage_counts=MappingProxyType(
            dict(reversed(tuple(golden.yes_ohlc_coverage_counts.items())))
        ),
        raw_rows_by_table=raw,
    )
    expected = _report(golden, manifest)
    actual = _report(shuffled, manifest)
    assert actual.raw_row_set_hash == expected.raw_row_set_hash
    assert actual.report_hash == expected.report_hash


def test_final_verdict_requires_exactly_24_hours(
    golden: EvidenceWindow, manifest: SoakManifest
) -> None:
    report = _report(golden, manifest, require_24h=True)
    assert report.status is VerdictStatus.NOT_CLOSED
    assert "final_window_duration" in _codes(report)


def test_manifest_canonical_payload_is_hash_bound_and_immutable(manifest: SoakManifest) -> None:
    payload = manifest.to_dict()
    assert payload["manifest_hash"] == manifest.manifest_hash
    assert stable_sha256(manifest.hash_payload()) == manifest.manifest_hash
    with pytest.raises(AttributeError):
        manifest.allowed_event_kind_exceptions.add(RuntimeEventKind.WATCHDOG_STALE)  # type: ignore[attr-defined]


@pytest.mark.parametrize("defect", ["missing", "duplicate_path", "wrong_bound"])
def test_manifest_rejects_missing_duplicate_or_wrong_bound_artifacts(
    manifest: SoakManifest, defect: str
) -> None:
    reports = manifest.reports
    if defect == "missing":
        changed = reports[:-1]
    elif defect == "duplicate_path":
        changed = reports[:-1] + (replace(reports[-1], path=reports[0].path),)
    else:
        changed = reports[:-1] + (replace(reports[-1], end=reports[-1].end - timedelta(seconds=1)),)
    with pytest.raises(ValueError):
        replace(manifest, reports=changed)


def test_manifest_rejects_image_ref_digest_mismatch(manifest: SoakManifest) -> None:
    with pytest.raises(ValueError, match="image_ref"):
        replace(manifest, image_digest=SHA_A)


@pytest.mark.parametrize("table", ["promote", "health", "market", "event"])
def test_every_decoded_row_must_share_manifest_boot(
    golden: EvidenceWindow, manifest: SoakManifest, table: str
) -> None:
    other = UUID(int=56)
    if table == "promote":
        window = replace(
            golden,
            promote_runs=(replace(golden.promote_runs[0], boot_id=other),)
            + golden.promote_runs[1:],
        )
    elif table == "health":
        window = replace(
            golden,
            health_samples=(replace(golden.health_samples[0], boot_id=other),)
            + golden.health_samples[1:],
        )
    elif table == "market":
        window = replace(
            golden,
            market_samples=(replace(golden.market_samples[0], boot_id=other),)
            + golden.market_samples[1:],
        )
    else:
        event = RuntimeEventRecord(
            event_id=UUID(int=57),
            boot_id=other,
            event_seq=1,
            occurred_at=T0 + timedelta(seconds=1),
            kind=RuntimeEventKind.RECONNECT_SUCCEEDED,
            reason_code="ok",
            detail={},
        )
        window = replace(golden, runtime_events=(event,))
    assert "row_boot_mismatch" in _codes(_report(window, manifest))


@pytest.mark.parametrize("table", ["promote", "health", "market", "event"])
def test_every_occurrence_timestamp_must_be_inside_exact_window(
    golden: EvidenceWindow, manifest: SoakManifest, table: str
) -> None:
    if table == "promote":
        changed = replace(
            golden.promote_runs[-1],
            scheduled_at=T6,
            started_at=T6,
            finished_at=T6,
        )
        window = replace(golden, promote_runs=golden.promote_runs[:-1] + (changed,))
    elif table == "health":
        changed = replace(golden.health_samples[-1], scheduled_at=T6, sampled_at=T6)
        window = replace(golden, health_samples=golden.health_samples[:-1] + (changed,))
    elif table == "market":
        changed = replace(golden.market_samples[-1], sampled_at=T6)
        window = replace(golden, market_samples=golden.market_samples[:-1] + (changed,))
    else:
        event = RuntimeEventRecord(
            event_id=UUID(int=58),
            boot_id=BOOT_ID,
            event_seq=1,
            occurred_at=T6,
            kind=RuntimeEventKind.RECONNECT_SUCCEEDED,
            reason_code="ok",
            detail={},
        )
        window = replace(golden, runtime_events=(event,))
    assert "occurrence_outside_window" in _codes(_report(window, manifest))


def test_raw_duplicate_primary_key_uses_full_row_tie_breaker(
    golden: EvidenceWindow, manifest: SoakManifest
) -> None:
    raw = {key: tuple(rows) for key, rows in golden.raw_rows_by_table.items()}
    first = dict(raw["l3_health_samples"][0])
    duplicate = {**first, "reason_code": "different"}
    tail = raw["l3_health_samples"][1:]
    raw["l3_health_samples"] = (first, duplicate) + tail
    forward = _report(replace(golden, raw_rows_by_table=raw), manifest)
    raw["l3_health_samples"] = (duplicate, first) + tail
    reverse = _report(replace(golden, raw_rows_by_table=raw), manifest)
    assert forward.raw_row_set_hash == reverse.raw_row_set_hash
