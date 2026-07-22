"""Typed, immutable truth model for L3 continuous-soak evidence."""

from __future__ import annotations

import hashlib
import importlib
import json
from dataclasses import FrozenInstanceError, fields, replace
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from types import MappingProxyType
from uuid import UUID, uuid4

import pytest
from pydantic import SecretStr, ValidationError

import polyarb
from polyarb.config import Settings

NOW = datetime(2026, 7, 23, 6, 0, tzinfo=UTC)
HASH_A = "a" * 64
HASH_B = "b" * 64


def _identity(evidence) -> object:
    return evidence.RuntimeIdentity(
        "machine",
        "version",
        "image",
        "release",
        "code",
        HASH_A,
        HASH_B,
    )


def _market_sample(evidence, boot_id: UUID, index: int, *, sample_seq: int = 7):
    return evidence.MarketSampleRecord(
        boot_id,
        sample_seq,
        NOW,
        f"market-{index}",
        f"yes-{index}",
        f"no-{index}",
        True,
        True,
        True,
        True,
        True,
        True,
        4,
        NOW - timedelta(seconds=2),
        NOW - timedelta(seconds=3),
        2_000,
        3_000,
        3_000,
        NOW - timedelta(seconds=4),
        4_000,
        evidence.HealthStatus.PASS,
        "ok",
    )


def test_locked_enums_and_canonical_hash() -> None:
    evidence = importlib.import_module("polyarb.observation.l3_evidence")

    assert [member.value for member in evidence.PromoteStatus] == [
        "success",
        "frozen",
        "underfilled",
        "failed",
    ]
    assert [member.value for member in evidence.HealthStatus] == ["pass", "warn", "fail"]
    assert [member.value for member in evidence.RuntimeEventSeverity] == [
        "info",
        "warning",
        "critical",
    ]
    assert [member.value for member in evidence.RuntimeEventKind] == [
        "watchdog_stale",
        "reconnect_reserved",
        "reconnect_deferred",
        "reconnect_started",
        "reconnect_succeeded",
        "reconnect_failed",
        "ws_generation_changed",
        "subscription_control_failed",
        "subscription_compensated",
        "evidence_writer_failed",
        "evidence_writer_recovered",
        "shutdown_signal",
        "soak_manifest_bound",
        "checkpoint_report_bound",
    ]

    first = {"z": [3, 2, 1], "nested": {"b": True, "a": None}}
    reordered = {"nested": {"a": None, "b": True}, "z": [3, 2, 1]}
    expected_bytes = json.dumps(
        first, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    assert evidence.stable_sha256(first) == hashlib.sha256(expected_bytes).hexdigest()
    assert evidence.stable_sha256(first) == evidence.stable_sha256(reordered)
    assert evidence.stable_sha256(["token-b", "token-a"]) != evidence.stable_sha256(
        ["token-a", "token-b"]
    )
    assert evidence.stable_sha256(sorted(["token-b", "token-a"])) == evidence.stable_sha256(
        sorted(["token-a", "token-b"])
    )


def test_acceptance_config_defaults_digest_and_every_field_is_sensitive(tmp_path: Path) -> None:
    evidence = importlib.import_module("polyarb.observation.l3_evidence")
    recipe = tmp_path / "l3-promote.yaml"
    recipe.write_bytes(b"recipes: {}\n")
    settings = Settings()

    acceptance = evidence.AcceptanceConfig.from_settings(
        settings, recipe, code_version="9.8.7"
    )

    assert acceptance == evidence.AcceptanceConfig(
        recipe_sha256="7dd561ccf625ef3624bba67b64366b013993ef1082208de43f83c00a2908f7ea",
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
        code_version="9.8.7",
    )
    assert acceptance.digest() == "f4776422fe1a02b48893bf1647ae2cb654057173cb47be086718ff8db531c44a"

    replacements = {
        "recipe_sha256": "0" * 64,
        "sample_interval_s": 31,
        "max_sample_gap_s": 76,
        "promote_interval_s": 301,
        "promote_max_start_gap_s": 361,
        "market_book_fresh_s": 121,
        "market_ohlc_fresh_s": 121,
        "expected_market_count": 6,
        "expected_token_count": 11,
        "retention_days": 31,
        "schema_revision": "008",
        "code_version": "9.8.8",
    }
    assert replacements.keys() == {field.name for field in fields(acceptance)}
    assert all(
        replace(acceptance, **{name: value}).digest() != acceptance.digest()
        for name, value in replacements.items()
    )


@pytest.mark.parametrize(
    ("field_name", "invalid"),
    [
        ("l3_evidence_sample_interval_s", 0),
        ("l3_evidence_max_sample_gap_s", 30),
        ("l3_promote_interval_s", 0),
        ("l3_promote_max_start_gap_s", 300),
        ("l3_evidence_retention_days", 29),
        ("l3_market_book_fresh_s", 0),
        ("l3_market_ohlc_fresh_s", 0),
    ],
)
def test_settings_lock_evidence_defaults_and_strict_boundaries(
    field_name: str, invalid: int
) -> None:
    settings = Settings()
    assert (
        settings.l3_evidence_sample_interval_s,
        settings.l3_evidence_max_sample_gap_s,
        settings.l3_promote_interval_s,
        settings.l3_promote_max_start_gap_s,
        settings.l3_evidence_retention_days,
        settings.l3_market_book_fresh_s,
        settings.l3_market_ohlc_fresh_s,
    ) == (30, 75, 300, 360, 30, 120, 120)
    with pytest.raises(ValidationError):
        Settings(**{field_name: invalid})


def test_l2_runtime_database_credential_is_distinct_and_masked() -> None:
    dsn = "postgresql://daemon:daemon-password-9351@db/evidence"
    settings = Settings(l2_runtime_db_dsn=dsn)

    assert isinstance(settings.l2_runtime_db_dsn, SecretStr)
    assert settings.l2_runtime_db_dsn.get_secret_value().endswith("/evidence")
    assert dsn not in repr(settings)
    assert "daemon-password-9351" not in repr(settings)
    assert settings.supabase_db_dsn.get_secret_value() == ""


def test_runtime_identity_uses_fly_environment_and_exact_recipe_bytes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evidence = importlib.import_module("polyarb.observation.l3_evidence")
    monkeypatch.setenv("FLY_MACHINE_ID", "machine-7")
    monkeypatch.setenv("FLY_MACHINE_VERSION", "v42")
    monkeypatch.setenv("FLY_IMAGE_REF", "registry.example/image@sha256:abc")
    settings = Settings(release_id="release-123")
    recipe_path = (
        Path(evidence.__file__).resolve().parents[1]
        / "scan_recipes"
        / "l3-promote.yaml"
    )

    identity = evidence.RuntimeIdentity.from_environment(settings)
    acceptance = evidence.AcceptanceConfig.from_settings(
        settings, recipe_path, code_version=polyarb.__version__
    )

    assert identity.machine_id == "machine-7"
    assert identity.machine_version == "v42"
    assert identity.image_ref == "registry.example/image@sha256:abc"
    assert identity.release_id == "release-123"
    assert identity.code_version == polyarb.__version__
    assert identity.recipe_sha256 == hashlib.sha256(recipe_path.read_bytes()).hexdigest()
    assert identity.acceptance_config_hash == acceptance.digest()

    for key in ("FLY_MACHINE_ID", "FLY_MACHINE_VERSION", "FLY_IMAGE_REF"):
        monkeypatch.delenv(key)
    local = evidence.RuntimeIdentity.from_environment(settings)
    assert (local.machine_id, local.machine_version, local.image_ref) == (
        "local",
        "local",
        "local",
    )


@pytest.mark.parametrize(
    ("environment_name", "identity_field"),
    [
        ("FLY_MACHINE_ID", "machine_id"),
        ("FLY_MACHINE_VERSION", "machine_version"),
        ("FLY_IMAGE_REF", "image_ref"),
    ],
)
def test_runtime_identity_rejects_present_but_empty_fly_values(
    monkeypatch: pytest.MonkeyPatch,
    environment_name: str,
    identity_field: str,
) -> None:
    evidence = importlib.import_module("polyarb.observation.l3_evidence")
    for name in ("FLY_MACHINE_ID", "FLY_MACHINE_VERSION", "FLY_IMAGE_REF"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv(environment_name, "")

    with pytest.raises(ValueError, match=identity_field):
        evidence.RuntimeIdentity.from_environment(Settings())


def test_public_dataclass_field_contract_is_exact() -> None:
    evidence = importlib.import_module("polyarb.observation.l3_evidence")
    expected = {
        "MarketPair": ("market_id", "yes_token_id", "no_token_id"),
        "RuntimeIdentity": (
            "machine_id",
            "machine_version",
            "image_ref",
            "release_id",
            "code_version",
            "recipe_sha256",
            "acceptance_config_hash",
        ),
        "RuntimeBootRecord": (
            "boot_id",
            "started_at",
            "machine_id",
            "machine_version",
            "image_ref",
            "release_id",
            "code_version",
            "acceptance_config_hash",
        ),
        "PromoteRunRecord": (
            "boot_id",
            "run_seq",
            "scheduled_at",
            "started_at",
            "finished_at",
            "status",
            "reason_code",
            "selected_count",
            "desired_count",
            "committed_count",
            "evidenced_count",
            "add_count",
            "remove_count",
            "mapping_hash",
            "desired_hash",
            "committed_hash",
            "acceptance_config_hash",
            "ws_generation",
            "add_succeeded",
            "remove_succeeded",
            "mirror_succeeded",
            "duration_ms",
        ),
        "HealthSampleRecord": (
            "boot_id",
            "sample_seq",
            "sampled_at",
            "desired_count",
            "committed_count",
            "evidenced_count",
            "promote_age_ms",
            "global_book_age_ms",
            "ws_age_ms",
            "mirror_age_ms",
            "candidate_age_ms",
            "reconciliation_age_ms",
            "listener_state",
            "cursor_lag",
            "watchdog_count",
            "reconnect_count",
            "ws_generation",
            "mapping_hash",
            "acceptance_config_hash",
            "status",
            "reason_code",
        ),
        "MarketSampleRecord": (
            "boot_id",
            "sample_seq",
            "sampled_at",
            "market_id",
            "yes_token_id",
            "no_token_id",
            "yes_desired",
            "no_desired",
            "yes_committed",
            "no_committed",
            "yes_evidenced",
            "no_evidenced",
            "evidence_generation",
            "yes_book_at",
            "no_book_at",
            "yes_book_age_ms",
            "no_book_age_ms",
            "worst_book_age_ms",
            "yes_ohlc_at",
            "yes_ohlc_age_ms",
            "status",
            "reason_code",
        ),
        "SampleBatch": ("health", "markets"),
        "RuntimeEventRecord": (
            "event_id",
            "boot_id",
            "event_seq",
            "occurred_at",
            "kind",
            "severity",
            "generation",
            "reason_code",
            "detail",
        ),
        "WsMembershipSnapshot": (
            "generation",
            "desired",
            "committed",
            "evidenced",
            "evidenced_at",
        ),
        "EvidenceStatus": (
            "identity",
            "boot_id",
            "started_at",
            "acceptance_config_hash",
            "ws_generation",
            "desired",
            "committed",
            "evidenced",
            "evidenced_at",
            "last_promote_persisted_at",
            "last_sample_persisted_at",
            "last_market_samples",
            "writer_ok",
            "last_writer_result_at",
            "writer_reason_code",
            "pending_event_count",
            "event_queue_overflowed",
            "status",
            "reason_code",
        ),
        "EvidenceWindow": (
            "start",
            "end",
            "boots",
            "promote_runs",
            "health_samples",
            "market_samples",
            "runtime_events",
            "book_coverage_counts",
            "yes_ohlc_coverage_counts",
            "raw_rows_by_table",
        ),
        "RetentionBounds": (
            "oldest_recorded_at_by_table",
            "newest_recorded_at_by_table",
            "row_count_by_table",
        ),
    }
    for type_name, field_names in expected.items():
        assert tuple(field.name for field in fields(getattr(evidence, type_name))) == field_names


def test_append_records_validate_utc_nonnegative_hash_and_reason_boundaries() -> None:
    evidence = importlib.import_module("polyarb.observation.l3_evidence")
    boot_id = uuid4()
    promote = evidence.PromoteRunRecord(
        boot_id,
        0,
        NOW,
        NOW,
        NOW,
        evidence.PromoteStatus.SUCCESS,
        "ok",
        5,
        10,
        10,
        10,
        1,
        2,
        HASH_A,
        HASH_A,
        HASH_A,
        HASH_B,
        3,
        True,
        False,
        True,
        12,
    )
    health = evidence.HealthSampleRecord(
        boot_id,
        7,
        NOW,
        10,
        10,
        10,
        1,
        2,
        3,
        4,
        5,
        6,
        "LISTENING",
        0,
        1,
        2,
        3,
        HASH_A,
        HASH_B,
        evidence.HealthStatus.PASS,
        "ok",
    )
    market = _market_sample(evidence, boot_id, 0)

    for record, names in (
        (
            promote,
            (
                "run_seq",
                "selected_count",
                "desired_count",
                "committed_count",
                "evidenced_count",
                "add_count",
                "remove_count",
                "ws_generation",
                "duration_ms",
            ),
        ),
        (
            health,
            (
                "sample_seq",
                "desired_count",
                "committed_count",
                "evidenced_count",
                "promote_age_ms",
                "global_book_age_ms",
                "ws_age_ms",
                "mirror_age_ms",
                "candidate_age_ms",
                "reconciliation_age_ms",
                "cursor_lag",
                "watchdog_count",
                "reconnect_count",
                "ws_generation",
            ),
        ),
        (
            market,
            (
                "sample_seq",
                "evidence_generation",
                "yes_book_age_ms",
                "no_book_age_ms",
                "worst_book_age_ms",
                "yes_ohlc_age_ms",
            ),
        ),
    ):
        for name in names:
            with pytest.raises(ValueError, match="non-negative"):
                replace(record, **{name: -1})

    for record in (promote, health, market):
        with pytest.raises(ValueError, match="reason_code"):
            replace(record, reason_code="r" * 65)
    with pytest.raises(ValueError, match="UTC"):
        replace(promote, started_at=NOW.replace(tzinfo=None))
    with pytest.raises(ValueError, match="UTC"):
        replace(health, sampled_at=NOW.astimezone(timezone(timedelta(hours=1))))
    with pytest.raises(ValueError, match="UTC"):
        replace(market, yes_book_at=NOW.replace(tzinfo=None))
    with pytest.raises(ValueError, match="SHA-256"):
        replace(promote, mapping_hash="A" * 64)


def test_sample_batch_requires_five_complete_distinct_market_pairs() -> None:
    evidence = importlib.import_module("polyarb.observation.l3_evidence")
    boot_id = uuid4()
    health = evidence.HealthSampleRecord(
        boot_id,
        7,
        NOW,
        10,
        10,
        10,
        1,
        2,
        3,
        4,
        5,
        6,
        "LISTENING",
        0,
        1,
        2,
        3,
        HASH_A,
        HASH_B,
        evidence.HealthStatus.PASS,
        "ok",
    )
    markets = tuple(_market_sample(evidence, boot_id, index) for index in range(5))
    batch = evidence.SampleBatch(health, markets)
    assert len(batch.markets) == 5

    with pytest.raises(ValueError, match="exactly five"):
        evidence.SampleBatch(health, markets[:4])
    with pytest.raises(ValueError, match="distinct"):
        evidence.SampleBatch(health, (*markets[:4], markets[0]))
    with pytest.raises(ValueError, match="ten distinct token"):
        evidence.SampleBatch(
            health,
            (*markets[:4], replace(markets[4], no_token_id=markets[0].yes_token_id)),
        )
    with pytest.raises(ValueError, match="share"):
        evidence.SampleBatch(health, (*markets[:4], replace(markets[4], sample_seq=8)))


def test_membership_events_windows_and_retention_are_defensively_immutable() -> None:
    evidence = importlib.import_module("polyarb.observation.l3_evidence")
    boot_id = uuid4()
    desired = {"yes", "no", "old"}
    committed = {"yes", "no", "old"}
    evidenced = {"yes", "no"}
    evidence_times = {"yes": NOW, "no": NOW}
    membership = evidence.WsMembershipSnapshot(
        4, desired, committed, evidenced, evidence_times
    )
    desired.add("mutated")
    evidence_times["yes"] = NOW + timedelta(seconds=1)
    assert membership.desired == frozenset({"yes", "no", "old"})
    assert membership.evidenced_at["yes"] == NOW
    assert isinstance(membership.evidenced_at, MappingProxyType)
    with pytest.raises(TypeError):
        membership.evidenced_at["yes"] = NOW  # type: ignore[index]
    with pytest.raises(ValueError, match="subset"):
        evidence.WsMembershipSnapshot(0, frozenset(), frozenset(), frozenset({"x"}), {"x": NOW})

    mutable_detail = {"nested": {"items": ["a", "b"]}}
    event = evidence.RuntimeEventRecord(
        uuid4(),
        boot_id,
        0,
        NOW,
        evidence.RuntimeEventKind.WATCHDOG_STALE,
        detail=mutable_detail,
    )
    mutable_detail["nested"]["items"].append("mutated")
    assert event.detail["nested"]["items"] == ("a", "b")
    with pytest.raises(ValueError, match="2048"):
        replace(event, detail={"payload": "x" * 2048})
    with pytest.raises(ValueError, match="reason_code"):
        replace(event, reason_code="x" * 65)

    table_keys = {
        "l3_runtime_boots",
        "l3_promote_runs",
        "l3_health_samples",
        "l3_market_samples",
        "l3_runtime_events",
    }
    oldest = {name: None for name in table_keys}
    newest = {name: NOW for name in table_keys}
    counts = {name: 0 for name in table_keys}
    bounds = evidence.RetentionBounds(oldest, newest, counts)
    counts["l3_runtime_events"] = 99
    assert bounds.row_count_by_table["l3_runtime_events"] == 0
    with pytest.raises(ValueError, match="five evidence"):
        evidence.RetentionBounds({}, newest, counts)

    raw = {"l3_runtime_events": ({"detail": {"value": [1, 2]}},)}
    window = evidence.EvidenceWindow(NOW, NOW + timedelta(hours=1), raw_rows_by_table=raw)
    raw["l3_runtime_events"][0]["detail"]["value"].append(3)
    assert window.raw_rows_by_table["l3_runtime_events"][0]["detail"]["value"] == (1, 2)
    with pytest.raises(FrozenInstanceError):
        window.start = NOW  # type: ignore[misc]


def test_frame_dispatch_result_requires_exact_utc_datetime_or_none() -> None:
    evidence = importlib.import_module("polyarb.observation.l3_evidence")

    assert evidence.FrameDispatchResult(False, False, None).observed_at is None
    with pytest.raises(TypeError, match="observed_at must be a datetime or None"):
        evidence.FrameDispatchResult(False, False, "2026-07-23T06:00:00Z")
    with pytest.raises(ValueError, match="UTC"):
        evidence.FrameDispatchResult(False, False, NOW.replace(tzinfo=None))
    with pytest.raises(ValueError, match="UTC"):
        evidence.FrameDispatchResult(
            False,
            False,
            NOW.astimezone(timezone(timedelta(hours=1))),
        )


def test_stable_hash_normalizes_supported_values_and_rejects_credentials() -> None:
    evidence = importlib.import_module("polyarb.observation.l3_evidence")
    identifier = UUID("12345678-1234-5678-1234-567812345678")
    rich = {
        "at": NOW,
        "id": identifier,
        "kind": evidence.RuntimeEventKind.RECONNECT_STARTED,
        "tokens": ("a", "b"),
    }
    primitive = {
        "at": "2026-07-23T06:00:00Z",
        "id": "12345678-1234-5678-1234-567812345678",
        "kind": "reconnect_started",
        "tokens": ["a", "b"],
    }
    assert evidence.stable_sha256(rich) == evidence.stable_sha256(primitive)
    assert evidence.stable_sha256({"ratio": 1.25}) == hashlib.sha256(
        b'{"ratio":1.25}'
    ).hexdigest()
    with pytest.raises(TypeError, match="unsupported"):
        evidence.stable_sha256({"secret": SecretStr("do-not-hash")})
    with pytest.raises(TypeError, match="unsupported"):
        evidence.stable_sha256({"object": object()})
    with pytest.raises(TypeError, match="mapping or sequence"):
        evidence.stable_sha256("not-a-document")


def test_runtime_sequences_membership_and_persisted_success_anchors() -> None:
    evidence = importlib.import_module("polyarb.observation.l3_evidence")
    identity = _identity(evidence)
    boot_id = uuid4()
    runtime = evidence.L3EvidenceRuntime(identity, boot_id=boot_id, started_at=NOW)

    assert [runtime.next_run_seq() for _ in range(3)] == [0, 1, 2]
    assert [runtime.next_sample_seq() for _ in range(3)] == [0, 1, 2]
    assert runtime.snapshot().status is evidence.HealthStatus.WARN
    assert runtime.snapshot().acceptance_config_hash == HASH_B

    membership_source = {"yes": NOW, "no": NOW}
    runtime.update_membership(
        evidence.WsMembershipSnapshot(
            4,
            frozenset({"yes", "no"}),
            frozenset({"yes", "no", "old"}),
            frozenset({"yes", "no"}),
            membership_source,
        )
    )
    membership_source["yes"] = NOW + timedelta(minutes=1)
    status = runtime.snapshot()
    assert status.ws_generation == 4
    assert status.committed == frozenset({"yes", "no", "old"})
    assert status.evidenced_at["yes"] == NOW
    with pytest.raises(ValueError, match="rollback"):
        runtime.update_membership(evidence.WsMembershipSnapshot(generation=3))

    markets = tuple(_market_sample(evidence, boot_id, index) for index in range(5))
    runtime.mark_promote_persisted(NOW)
    runtime.mark_sample_persisted(NOW, markets)
    status = runtime.snapshot()
    assert status.last_promote_persisted_at == NOW
    assert status.last_sample_persisted_at == NOW
    assert status.last_market_samples == markets
    assert status.status is evidence.HealthStatus.PASS
    with pytest.raises(ValueError, match="backward"):
        runtime.mark_promote_persisted(NOW - timedelta(seconds=1))
    with pytest.raises(ValueError, match="exactly five"):
        runtime.mark_sample_persisted(NOW + timedelta(seconds=1), markets[:4])


def test_writer_outage_transitions_queue_one_failure_and_one_recovery() -> None:
    evidence = importlib.import_module("polyarb.observation.l3_evidence")
    runtime = evidence.L3EvidenceRuntime(_identity(evidence), started_at=NOW)
    runtime.mark_promote_persisted(NOW)
    boot_id = runtime.snapshot().boot_id
    markets = tuple(_market_sample(evidence, boot_id, index) for index in range(5))
    runtime.mark_sample_persisted(NOW, markets)
    anchors_before = (
        runtime.snapshot().last_promote_persisted_at,
        runtime.snapshot().last_sample_persisted_at,
        runtime.snapshot().last_market_samples,
    )

    runtime.note_writer_result(False, NOW + timedelta(seconds=1), "db_unavailable")
    runtime.note_writer_result(False, NOW + timedelta(seconds=2), "db_still_unavailable")
    failed = runtime.snapshot()
    assert failed.writer_ok is False
    assert failed.status is evidence.HealthStatus.FAIL
    assert failed.pending_event_count == 1
    assert (
        failed.last_promote_persisted_at,
        failed.last_sample_persisted_at,
        failed.last_market_samples,
    ) == anchors_before

    runtime.note_writer_result(True, NOW + timedelta(seconds=3), "writer_ok")
    recovered = runtime.snapshot()
    assert recovered.writer_ok is True
    assert recovered.status is evidence.HealthStatus.PASS
    assert recovered.pending_event_count == 2
    events = runtime.drain_pending_events()
    assert [event.event_seq for event in events] == [0, 1]
    assert [event.kind for event in events] == [
        evidence.RuntimeEventKind.EVIDENCE_WRITER_FAILED,
        evidence.RuntimeEventKind.EVIDENCE_WRITER_RECOVERED,
    ]
    assert runtime.drain_pending_events() == ()


def test_event_queue_preserves_first_128_and_surfaces_overflow_failure() -> None:
    evidence = importlib.import_module("polyarb.observation.l3_evidence")
    runtime = evidence.L3EvidenceRuntime(_identity(evidence), started_at=NOW)
    for index in range(128):
        event = runtime.record_event(
            evidence.RuntimeEventKind.WATCHDOG_STALE,
            occurred_at=NOW + timedelta(milliseconds=index),
            generation=index,
            reason_code="stale",
            detail={"index": index},
        )
        assert event.event_seq == index

    with pytest.raises(OverflowError, match="128"):
        runtime.record_event(
            evidence.RuntimeEventKind.WATCHDOG_STALE,
            occurred_at=NOW + timedelta(seconds=1),
        )
    status = runtime.snapshot()
    assert status.pending_event_count == 128
    assert status.event_queue_overflowed is True
    assert status.status is evidence.HealthStatus.FAIL
    assert [event.event_seq for event in runtime.drain_pending_events()] == list(range(128))
    assert runtime.record_event(
        evidence.RuntimeEventKind.RECONNECT_STARTED,
        occurred_at=NOW + timedelta(seconds=2),
    ).event_seq == 129


def test_full_event_queue_does_not_commit_unrecorded_writer_transition() -> None:
    evidence = importlib.import_module("polyarb.observation.l3_evidence")
    runtime = evidence.L3EvidenceRuntime(_identity(evidence), started_at=NOW)
    for index in range(128):
        runtime.record_event(
            evidence.RuntimeEventKind.WATCHDOG_STALE,
            occurred_at=NOW + timedelta(milliseconds=index),
        )

    before = runtime.snapshot()
    with pytest.raises(OverflowError, match="128"):
        runtime.note_writer_result(False, NOW + timedelta(seconds=1), "db_unavailable")
    overflowed = runtime.snapshot()
    assert overflowed.writer_ok is before.writer_ok is None
    assert overflowed.last_writer_result_at is before.last_writer_result_at is None
    assert overflowed.writer_reason_code == before.writer_reason_code == ""

    runtime.drain_pending_events()
    runtime.note_writer_result(False, NOW + timedelta(seconds=2), "db_unavailable")
    runtime.note_writer_result(True, NOW + timedelta(seconds=3), "writer_ok")
    transitions = runtime.drain_pending_events()
    assert [event.kind for event in transitions] == [
        evidence.RuntimeEventKind.EVIDENCE_WRITER_FAILED,
        evidence.RuntimeEventKind.EVIDENCE_WRITER_RECOVERED,
    ]


def test_shared_validators_reject_runtime_type_impostors() -> None:
    evidence = importlib.import_module("polyarb.observation.l3_evidence")
    boot_id = uuid4()
    market = _market_sample(evidence, boot_id, 0)
    health = evidence.HealthSampleRecord(
        boot_id,
        7,
        NOW,
        10,
        10,
        10,
        1,
        2,
        3,
        4,
        5,
        6,
        "LISTENING",
        0,
        1,
        2,
        3,
        HASH_A,
        HASH_B,
        evidence.HealthStatus.PASS,
        "ok",
    )
    identity = _identity(evidence)
    acceptance = evidence.AcceptanceConfig(
        HASH_A,
        30,
        75,
        300,
        360,
        120,
        120,
        5,
        10,
        30,
        "007",
        "code",
    )

    with pytest.raises(TypeError, match="int"):
        replace(market, sample_seq=0.5)
    with pytest.raises(TypeError, match="int"):
        evidence.WsMembershipSnapshot(generation=True)
    with pytest.raises(TypeError, match="str"):
        replace(market, reason_code=b"ok")
    with pytest.raises(TypeError, match="str"):
        replace(health, listener_state=b"LISTENING")
    with pytest.raises(TypeError, match="str"):
        replace(identity, recipe_sha256=b"a" * 64)
    with pytest.raises(TypeError, match="str"):
        replace(identity, machine_id=b"machine")
    with pytest.raises(ValueError, match="empty"):
        replace(identity, machine_id="")
    with pytest.raises(TypeError, match="int"):
        replace(acceptance, sample_interval_s=0.5)
    with pytest.raises(TypeError, match="int"):
        replace(acceptance, retention_days=True)


def test_every_public_append_record_rejects_uuid_enum_and_bool_impostors() -> None:
    evidence = importlib.import_module("polyarb.observation.l3_evidence")
    boot_id = uuid4()
    boot = evidence.RuntimeBootRecord(
        boot_id,
        NOW,
        "machine",
        "version",
        "image",
        "release",
        "code",
        HASH_A,
    )
    promote = evidence.PromoteRunRecord(
        boot_id,
        0,
        NOW,
        NOW,
        NOW,
        evidence.PromoteStatus.SUCCESS,
        "ok",
        5,
        10,
        10,
        10,
        1,
        2,
        HASH_A,
        HASH_A,
        HASH_A,
        HASH_B,
        3,
        True,
        None,
        False,
        12,
    )
    health = evidence.HealthSampleRecord(
        boot_id,
        7,
        NOW,
        10,
        10,
        10,
        1,
        2,
        3,
        4,
        5,
        6,
        "LISTENING",
        0,
        1,
        2,
        3,
        HASH_A,
        HASH_B,
        evidence.HealthStatus.PASS,
        "ok",
    )
    market = _market_sample(evidence, boot_id, 0)
    event = evidence.RuntimeEventRecord(
        uuid4(),
        boot_id,
        0,
        NOW,
        evidence.RuntimeEventKind.SHUTDOWN_SIGNAL,
        evidence.RuntimeEventSeverity.INFO,
    )

    for record, field_name in (
        (boot, "boot_id"),
        (promote, "boot_id"),
        (health, "boot_id"),
        (market, "boot_id"),
        (event, "event_id"),
        (event, "boot_id"),
    ):
        with pytest.raises(TypeError, match="UUID"):
            replace(record, **{field_name: str(getattr(record, field_name))})

    for record, field_name in (
        (promote, "status"),
        (health, "status"),
        (market, "status"),
        (event, "kind"),
        (event, "severity"),
    ):
        with pytest.raises(TypeError, match="enum"):
            replace(record, **{field_name: getattr(record, field_name).value})

    for field_name in ("add_succeeded", "remove_succeeded", "mirror_succeeded"):
        with pytest.raises(TypeError, match="bool"):
            replace(promote, **{field_name: 1})
    for field_name in (
        "yes_desired",
        "no_desired",
        "yes_committed",
        "no_committed",
        "yes_evidenced",
        "no_evidenced",
    ):
        with pytest.raises(TypeError, match="bool"):
            replace(market, **{field_name: 1})

    assert replace(promote, add_succeeded=None, remove_succeeded=None)


@pytest.mark.parametrize("detail", [[], (), ["root"], ("root",)])
def test_runtime_event_detail_requires_mapping_root(detail: object) -> None:
    evidence = importlib.import_module("polyarb.observation.l3_evidence")

    with pytest.raises(TypeError, match="Mapping"):
        evidence.RuntimeEventRecord(
            uuid4(),
            uuid4(),
            0,
            NOW,
            evidence.RuntimeEventKind.SHUTDOWN_SIGNAL,
            detail=detail,
        )


def test_runtime_event_detail_uses_postgres_jsonb_text_size_boundary() -> None:
    evidence = importlib.import_module("polyarb.observation.l3_evidence")
    empty_size = len(evidence._postgres_jsonb_text({"payload": ""}).encode("utf-8"))
    accepted_count = (2048 - empty_size) // len("界".encode())
    accepted = {"payload": "界" * accepted_count}
    rejected = {"payload": "界" * (accepted_count + 1)}

    assert len(evidence._postgres_jsonb_text(accepted).encode("utf-8")) <= 2048
    assert len(evidence._postgres_jsonb_text(rejected).encode("utf-8")) > 2048
    event = evidence.RuntimeEventRecord(
        uuid4(),
        uuid4(),
        0,
        NOW,
        evidence.RuntimeEventKind.SHUTDOWN_SIGNAL,
        detail=accepted,
    )
    assert event.detail["payload"] == accepted["payload"]
    with pytest.raises(ValueError, match="PostgreSQL jsonb::text"):
        replace(event, detail=rejected)


@pytest.mark.parametrize(
    ("detail", "message"),
    [
        ({"nested": [{"value": 1e20}]}, "float"),
        ({"nested": {"value": 1.25}}, "float"),
        ({"nested": {"value": "before\x00after"}}, "NUL"),
        ({"nested": {"before\x00after": "value"}}, "NUL"),
        ({"nested": ["ok", {"key": "\x00"}]}, "NUL"),
    ],
)
def test_runtime_event_detail_rejects_nested_floats_and_nul(
    detail: dict[str, object],
    message: str,
) -> None:
    evidence = importlib.import_module("polyarb.observation.l3_evidence")

    with pytest.raises(ValueError, match=message):
        evidence.RuntimeEventRecord(
            event_id=uuid4(),
            boot_id=uuid4(),
            event_seq=0,
            occurred_at=NOW,
            kind=evidence.RuntimeEventKind.SHUTDOWN_SIGNAL,
            detail=detail,
        )
