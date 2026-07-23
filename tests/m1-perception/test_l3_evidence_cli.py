"""Operator CLI contracts for immutable L3 soak evidence."""

from __future__ import annotations

import asyncio
import inspect
import json
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import NAMESPACE_URL, UUID, uuid5

import pytest

from polyarb.observation.l3_evidence import AcceptanceConfig, HealthStatus
from polyarb.observation.l3_soak_verdict import (
    L3SoakReport,
    ManifestReport,
    SoakManifest,
    VerdictStatus,
    canonical_manifest_bytes,
    parse_manifest_bytes,
)
from polyarb.storage.l3_evidence_store import RetentionCleanupResult

T0 = datetime(2030, 1, 2, 3, 4, 5, tzinfo=UTC)


def _cursor_policy_rows() -> list[dict[str, object]]:
    predicate = "(consumer = 'l2-candidate-refresh'::text)"
    return [
        {
            "policyname": "anon_read",
            "permissive": "PERMISSIVE",
            "cmd": "SELECT",
            "roles": ["anon"],
            "qual": "true",
            "with_check": None,
        },
        {
            "policyname": "l3_candidate_cursor_insert",
            "permissive": "PERMISSIVE",
            "cmd": "INSERT",
            "roles": ["l3_evidence_daemon"],
            "qual": None,
            "with_check": predicate,
        },
        {
            "policyname": "l3_candidate_cursor_select",
            "permissive": "PERMISSIVE",
            "cmd": "SELECT",
            "roles": ["l3_evidence_daemon"],
            "qual": predicate,
            "with_check": None,
        },
        {
            "policyname": "l3_candidate_cursor_update",
            "permissive": "PERMISSIVE",
            "cmd": "UPDATE",
            "roles": ["l3_evidence_daemon"],
            "qual": predicate,
            "with_check": predicate,
        },
    ]


def _manifest(tmp_path: Path, *, t0: datetime = T0) -> SoakManifest:
    config = AcceptanceConfig(
        recipe_sha256="1" * 64,
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
        code_version="abc123",
    )
    ends = (t0 + timedelta(seconds=30),) + tuple(
        t0 + timedelta(hours=hours) for hours in (6, 12, 18, 24)
    )
    labels = ("T+0", "T+6", "T+12", "T+18", "T+24")
    reports = tuple(
        ManifestReport(label, t0, end, str(tmp_path / f"{label}.json"))
        for label, end in zip(labels, ends, strict=True)
    )
    return SoakManifest(
        schema_version=1,
        t0=t0,
        t24=t0 + timedelta(hours=24),
        reports=reports,
        boot_id=UUID("00000000-0000-0000-0000-000000000001"),
        machine_id="machine-1",
        machine_version="42",
        image_ref="registry/app@sha256:" + "2" * 64,
        image_digest="2" * 64,
        release_id="release-42",
        code_version="abc123",
        mapping_hash="3" * 64,
        acceptance_config=config,
        acceptance_config_hash=config.digest(),
    )


def _manifest_boot_row(manifest: SoakManifest) -> dict[str, object]:
    return {
        "boot_id": manifest.boot_id,
        "started_at": manifest.t0 - timedelta(seconds=60),
        "stopped_at": None,
        "machine_id": manifest.machine_id,
        "machine_version": manifest.machine_version,
        "image_ref": manifest.image_ref,
        "release_id": manifest.release_id,
        "code_version": manifest.code_version,
        "acceptance_config_hash": manifest.acceptance_config_hash,
        "mapping_hash": manifest.mapping_hash,
    }


def _report(manifest: SoakManifest) -> L3SoakReport:
    return L3SoakReport(
        manifest_hash=manifest.manifest_hash,
        soak_hash=manifest.soak_hash,
        interval_hash="4" * 64,
        raw_row_set_hash="5" * 64,
        start=manifest.t0,
        end=manifest.reports[0].end,
        require_24h=False,
        boot_id=manifest.boot_id,
        machine_id=manifest.machine_id,
        machine_version=manifest.machine_version,
        image_ref=manifest.image_ref,
        image_digest=manifest.image_digest,
        release_id=manifest.release_id,
        code_version=manifest.code_version,
        mapping_hash=manifest.mapping_hash,
        acceptance_config_hash=manifest.acceptance_config_hash,
        row_counts={
            table: 1
            for table in (
                "l3_runtime_boots",
                "l3_promote_runs",
                "l3_health_samples",
                "l3_market_samples",
                "l3_runtime_events",
            )
        },
        expected_promoter_ticks=1,
        recorded_promoter_ticks=1,
        max_sample_gap_seconds=30.0,
        max_schedule_lag_seconds=0.0,
        max_promoter_start_gap_seconds=0.0,
        minimum_cardinality={
            "selected_markets": 5,
            "desired_tokens": 10,
            "committed_tokens": 10,
            "evidenced_tokens": 10,
        },
        maximum_freshness_ms={"yes_book": 1, "no_book": 1, "worst_book": 1, "yes_ohlc": 1},
        per_market_freshness_ms={},
        per_market_coverage_counts={},
        event_counts={},
        book_coverage_counts={},
        yes_ohlc_coverage_counts={},
        status=VerdictStatus.PASS,
        reasons=(),
    )


def test_manifest_round_trip_preserves_t0_interval_and_hash(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path)
    encoded = canonical_manifest_bytes(manifest)

    parsed = parse_manifest_bytes(encoded)

    assert parsed == manifest
    assert parsed.reports[0].end == parsed.t0 + timedelta(seconds=30)
    assert canonical_manifest_bytes(parsed) == encoded


def test_manifest_parse_rejects_noncanonical_or_tampered_hash(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path)
    payload = json.loads(canonical_manifest_bytes(manifest))
    payload["manifest_hash"] = "0" * 64

    with pytest.raises(ValueError, match="manifest_hash"):
        parse_manifest_bytes(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode())

    with pytest.raises(ValueError, match="canonical"):
        parse_manifest_bytes(canonical_manifest_bytes(manifest) + b"\n")


def test_manifest_create_is_future_only_and_exclusive(tmp_path: Path) -> None:
    from scripts import l3_evidence

    manifest = _manifest(tmp_path)
    output = tmp_path / "manifest.json"
    l3_evidence.write_new_manifest(manifest, output=output, now=manifest.t0 - timedelta(seconds=1))
    assert output.read_bytes() == canonical_manifest_bytes(manifest)

    with pytest.raises(FileExistsError):
        l3_evidence.write_new_manifest(
            manifest, output=output, now=manifest.t0 - timedelta(seconds=1)
        )
    with pytest.raises(ValueError, match="future"):
        l3_evidence.write_new_manifest(
            _manifest(tmp_path / "old", t0=T0 - timedelta(days=1)),
            output=tmp_path / "past.json",
            now=T0,
        )


def test_manifest_t0_must_be_an_eligible_grid_slot_with_binding_lead() -> None:
    from scripts import l3_evidence

    assert (
        l3_evidence._require_eligible_sampler_t0(
            t0=T0,
            boot_started_at=T0 - timedelta(seconds=60),
            sample_interval_s=30,
        )
        == 2
    )
    with pytest.raises(l3_evidence.OperatorError, match="sampler boundary"):
        l3_evidence._require_eligible_sampler_t0(
            t0=T0,
            boot_started_at=T0 - timedelta(seconds=60, microseconds=1),
            sample_interval_s=30,
        )
    with pytest.raises(l3_evidence.OperatorError, match="binding lead"):
        l3_evidence._require_eligible_sampler_t0(
            t0=T0,
            boot_started_at=T0 - timedelta(seconds=60),
            sample_interval_s=30,
            now=T0 - timedelta(seconds=59),
            minimum_lead_intervals=2,
        )


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2030-01-02T03:04:05Z", T0),
        ("2030-01-02T03:04:05+00:00", T0),
    ],
)
def test_rfc3339_parser_accepts_only_explicit_utc(value: str, expected: datetime) -> None:
    from scripts.l3_evidence import parse_rfc3339

    assert parse_rfc3339(value) == expected
    with pytest.raises(ValueError):
        parse_rfc3339("2030-01-02T03:04:05")
    with pytest.raises(ValueError):
        parse_rfc3339("2030-01-02T11:04:05+08:00")


def test_dsn_validation_is_allowlisted_and_redacted() -> None:
    from scripts.l3_evidence import validate_supabase_target

    target = validate_supabase_target(
        "postgresql://runtime:top-secret@db.abcdefghijklmnopqrst.supabase.co:5432/postgres"
        "?sslmode=require",
        expected_ref="abcdefghijklmnopqrst",
    )
    assert target.host == "db.abcdefghijklmnopqrst.supabase.co"
    assert target.database == "postgres"
    assert "secret" not in repr(target)
    with pytest.raises(ValueError, match="target"):
        validate_supabase_target(
            "postgresql://runtime:secret@evil.example/postgres",
            expected_ref="abcdefghijklmnopqrst",
        )
    assert (
        validate_supabase_target(
            "postgresql://runtime:secret@db.abcdefghijklmnopqrst.supabase.co/postgres"
            "?sslmode=require",
            expected_ref="abcdefghijklmnopqrst",
        ).user
        == "runtime"
    )
    for query in (
        "search_path=attacker",
        "options=-csearch_path%3Dattacker",
        "service=attacker",
        "sslmode=require&sslmode=disable",
        "unknown=value",
        "sslmode=disable",
        "sslmode=allow",
        "sslmode=prefer",
    ):
        with pytest.raises(ValueError, match="target"):
            validate_supabase_target(
                "postgresql://runtime:secret@db.abcdefghijklmnopqrst.supabase.co/postgres?" + query,
                expected_ref="abcdefghijklmnopqrst",
            )
    with pytest.raises(ValueError, match="target"):
        validate_supabase_target(
            "postgresql://runtime:secret@db.abcdefghijklmnopqrst.supabase.co/postgres",
            expected_ref="abcdefghijklmnopqrst",
        )
    with pytest.raises(ValueError, match="target"):
        validate_supabase_target(
            "postgresql://runtime:secret@db.abcdefghijklmnopqrst.supabase.co:6543/postgres",
            expected_ref="abcdefghijklmnopqrst",
        )


@pytest.mark.parametrize(
    "dsn",
    [
        "postgresql://runtime:secret@aws-0-us-east-1.pooler.supabase.com:5432/postgres",
        "postgresql://runtime.wrongprojectrefxxxxx:secret@aws-0-us-east-1.pooler.supabase.com:5432/postgres",
        "postgresql://runtime.abcdefghijklmnopqrst.evil:secret@aws-0-us-east-1.pooler.supabase.com:5432/postgres",
        "postgresql://runtime.abcdefghijklmnopqrst:secret@aws-0-us-east-1.pooler.supabase.com:6543/postgres",
        "postgresql://runtime.abcdefghijklmnopqrst:secret@aws-0-us-east-1.pooler.supabase.com/postgres",
    ],
)
def test_pooler_target_rejects_naked_wrong_or_fake_project_suffix(dsn: str) -> None:
    from scripts.l3_evidence import validate_supabase_target

    with pytest.raises(ValueError, match="target"):
        validate_supabase_target(dsn, expected_ref="abcdefghijklmnopqrst")


def test_pooler_target_accepts_custom_role_with_exact_project_suffix() -> None:
    from scripts.l3_evidence import validate_supabase_target

    target = validate_supabase_target(
        "postgresql://runtime.abcdefghijklmnopqrst:secret@"
        "aws-0-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require",
        expected_ref="abcdefghijklmnopqrst",
    )
    assert target.user == "runtime.abcdefghijklmnopqrst"
    assert target.host == "aws-0-us-east-1.pooler.supabase.com"


def test_cursor_policy_catalog_proof_is_exact_and_fail_closed() -> None:
    from scripts import l3_evidence

    rows = _cursor_policy_rows()
    assert l3_evidence._cursor_policy_catalog_is_exact(rows, rls_enabled=True)
    mutations = [
        rows[:-1],
        [{**rows[0], "roles": ["PUBLIC"]}, *rows[1:]],
        [rows[0], {**rows[1], "with_check": "true"}, *rows[2:]],
        [rows[0], rows[1], {**rows[2], "qual": "(consumer = 'other'::text)"}, rows[3]],
        [*rows[:3], {**rows[3], "cmd": "ALL"}],
        [{**rows[0], "permissive": "RESTRICTIVE"}, *rows[1:]],
        [
            *rows,
            {
                "policyname": "public_write",
                "cmd": "ALL",
                "roles": ["PUBLIC"],
                "qual": "true",
                "with_check": "true",
            },
        ],
    ]
    assert not l3_evidence._cursor_policy_catalog_is_exact(rows, rls_enabled=False)
    assert all(
        not l3_evidence._cursor_policy_catalog_is_exact(value, rls_enabled=True)
        for value in mutations
    )


def _binding_row(manifest: SoakManifest, **overrides: object) -> dict[str, object]:
    bound_at = manifest.t0 - timedelta(microseconds=1)
    row: dict[str, object] = {
        "event_id": uuid5(
            NAMESPACE_URL,
            f"polyarb:l3-soak-manifest:{manifest.manifest_hash}",
        ),
        "boot_id": manifest.boot_id,
        "event_seq": 8_000_000_000_000_000_000
        + int(manifest.manifest_hash[:16], 16) % 1_000_000_000_000_000,
        "occurred_at": bound_at,
        "recorded_at": bound_at,
        "kind": "soak_manifest_bound",
        "severity": "info",
        "generation": None,
        "reason_code": manifest.soak_hash,
        "detail": {"manifest_sha256": manifest.manifest_hash},
    }
    row.update(overrides)
    return row


class _Transaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *_args: object) -> None:
        return None


class _BindingConnection:
    def __init__(self, manifest: SoakManifest) -> None:
        self.manifest = manifest
        self.inserted = False

    def transaction(self) -> _Transaction:
        return _Transaction()

    async def fetch(self, *_args: object) -> list[object]:
        return []

    async def fetchrow(self, sql: str, *args: object) -> dict[str, object]:
        if "l3_runtime_boots" in sql:
            return _manifest_boot_row(self.manifest)
        if "SELECT event_id FROM public.l3_runtime_events" in sql:
            return None  # type: ignore[return-value]
        assert "INSERT INTO public.l3_runtime_events" in sql
        assert "clock_timestamp()" in sql
        assert "recorded_at" in sql
        assert args[3] == self.manifest.soak_hash
        assert json.loads(args[4])["manifest_sha256"] == self.manifest.manifest_hash
        assert args[5] == self.manifest.t0
        server_time = self.manifest.t0 - timedelta(microseconds=1)
        self.inserted = True
        return _binding_row(
            self.manifest,
            event_id=args[0],
            event_seq=args[2],
            occurred_at=server_time,
            recorded_at=server_time,
            detail=json.dumps({"manifest_sha256": self.manifest.manifest_hash}),
        )

    async def close(self) -> None:
        return None


@pytest.mark.asyncio
async def test_manifest_bind_appends_exact_hashes_and_refuses_at_t0(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from scripts import l3_evidence

    manifest = _manifest(tmp_path)
    connection = _BindingConnection(manifest)

    async def connect(*, dsn: str) -> _BindingConnection:
        assert dsn == "runtime-dsn"
        return connection

    monkeypatch.setattr(l3_evidence.asyncpg, "connect", connect)

    async def preflight(_dsn: str) -> None:
        return None

    monkeypatch.setattr(l3_evidence, "_runtime_preflight", preflight)
    row = await l3_evidence._bind_manifest(
        "runtime-dsn", manifest, now=manifest.t0 - timedelta(seconds=1)
    )
    assert connection.inserted
    assert row["recorded_at"] < manifest.t0
    assert row["event_seq"] >= l3_evidence._MANIFEST_EVENT_SEQ_BASE
    with pytest.raises(l3_evidence.OperatorError, match="before T0"):
        await l3_evidence._bind_manifest("runtime-dsn", manifest, now=manifest.t0)


@pytest.mark.asyncio
async def test_concurrent_manifest_bind_serializes_to_exactly_one_row(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from scripts import l3_evidence

    manifest = _manifest(tmp_path)
    lock = asyncio.Lock()
    shared: dict[str, object] = {}

    class Transaction:
        async def __aenter__(self) -> None:
            await lock.acquire()

        async def __aexit__(self, *_args: object) -> None:
            lock.release()

    class Connection:
        def transaction(self) -> Transaction:
            return Transaction()

        async def fetch(self, sql: str, *_args: object) -> list[object]:
            if "kind='soak_manifest_bound'" in sql and shared:
                return [shared]
            return []

        async def fetchrow(self, sql: str, *args: object) -> object:
            if "l3_runtime_boots" in sql:
                return _manifest_boot_row(manifest)
            if "SELECT event_id FROM public.l3_runtime_events" in sql:
                return None
            if "INSERT INTO public.l3_runtime_events" in sql:
                server_time = manifest.t0 - timedelta(microseconds=1)
                shared.update(
                    _binding_row(
                        manifest,
                        event_id=args[0],
                        event_seq=args[2],
                        occurred_at=server_time,
                        recorded_at=server_time,
                        detail=json.dumps({"manifest_sha256": manifest.manifest_hash}),
                    )
                )
                return dict(shared)
            raise AssertionError(sql)

        async def close(self) -> None:
            return None

    async def connect(*, dsn: str) -> Connection:
        assert dsn == "runtime"
        return Connection()

    async def preflight(_dsn: str) -> None:
        return None

    monkeypatch.setattr(l3_evidence.asyncpg, "connect", connect)
    monkeypatch.setattr(l3_evidence, "_runtime_preflight", preflight)
    results = await asyncio.gather(
        l3_evidence._bind_manifest("runtime", manifest, now=manifest.t0 - timedelta(seconds=1)),
        l3_evidence._bind_manifest("runtime", manifest, now=manifest.t0 - timedelta(seconds=1)),
        return_exceptions=True,
    )
    assert all(not isinstance(result, BaseException) for result in results)
    assert results[0]["event_id"] == results[1]["event_id"]  # type: ignore[index]
    assert shared["event_seq"] == l3_evidence._manifest_event_seq(manifest)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "forgery",
    (
        None,
        "event_id",
        "event_seq",
        "detail",
        "kind",
        "severity",
        "generation",
        "reason_code",
        "occurred_at",
        "recorded_at",
    ),
)
async def test_manifest_bind_lost_ack_retry_returns_existing_exact_binding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, forgery: str | None
) -> None:
    from scripts import l3_evidence

    manifest = _manifest(tmp_path)
    existing = _binding_row(manifest)
    forged_values: dict[str, object] = {
        "event_id": UUID("00000000-0000-0000-0000-000000000099"),
        "event_seq": existing["event_seq"] + 1,  # type: ignore[operator]
        "detail": {"manifest_sha256": manifest.manifest_hash, "extra": True},
        "kind": "shutdown_signal",
        "severity": "warn",
        "generation": 0,
        "reason_code": "forged",
        "occurred_at": manifest.t0 - timedelta(seconds=1),
        "recorded_at": manifest.t0,
    }
    if forgery is not None:
        existing[forgery] = forged_values[forgery]

    class Connection:
        def transaction(self) -> _Transaction:
            return _Transaction()

        async def fetch(self, *_args: object) -> list[object]:
            return [existing]

        async def fetchrow(self, sql: str, *_args: object) -> object:
            if "l3_runtime_boots" in sql:
                return _manifest_boot_row(manifest)
            raise AssertionError("lost-ACK retry must not insert another binding")

        async def close(self) -> None:
            return None

    async def connect(*, dsn: str) -> Connection:
        assert dsn == "runtime"
        return Connection()

    async def preflight(_dsn: str) -> None:
        return None

    monkeypatch.setattr(l3_evidence.asyncpg, "connect", connect)
    monkeypatch.setattr(l3_evidence, "_runtime_preflight", preflight)
    operation = l3_evidence._bind_manifest(
        "runtime", manifest, now=manifest.t0 + timedelta(hours=1)
    )
    if forgery is None:
        assert await operation == existing
    else:
        with pytest.raises(l3_evidence.OperatorError, match="binding is invalid"):
            await operation


@pytest.mark.asyncio
async def test_manifest_bind_database_clock_crossing_t0_commits_no_late_row(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from scripts import l3_evidence

    manifest = _manifest(tmp_path)
    insert_sql: list[str] = []

    class Connection:
        def transaction(self) -> _Transaction:
            return _Transaction()

        async def fetch(self, *_args: object) -> list[object]:
            return []

        async def fetchrow(self, sql: str, *_args: object) -> object:
            if "l3_runtime_boots" in sql:
                return _manifest_boot_row(manifest)
            if "SELECT event_id FROM public.l3_runtime_events" in sql:
                return None
            if "INSERT INTO public.l3_runtime_events" in sql:
                insert_sql.append(sql)
                return None
            raise AssertionError(sql)

        async def close(self) -> None:
            return None

    async def connect(*, dsn: str) -> Connection:
        assert dsn == "runtime"
        return Connection()

    async def preflight(_dsn: str) -> None:
        return None

    monkeypatch.setattr(l3_evidence.asyncpg, "connect", connect)
    monkeypatch.setattr(l3_evidence, "_runtime_preflight", preflight)
    with pytest.raises(l3_evidence.OperatorError, match="database clock reached T0"):
        await l3_evidence._bind_manifest(
            "runtime", manifest, now=manifest.t0 - timedelta(microseconds=1)
        )
    assert len(insert_sql) == 1
    assert "WHERE bound_at < $6" in insert_sql[0]


def test_binding_validation_requires_one_pre_t0_exact_soak_hash(tmp_path: Path) -> None:
    from scripts import l3_evidence

    manifest = _manifest(tmp_path)
    valid = _binding_row(manifest)
    assert l3_evidence._validate_exact_binding([valid], manifest) is valid
    forged_rows = (
        [],
        [valid, valid],
        [{**valid, "event_id": UUID("00000000-0000-0000-0000-000000000099")}],
        [{**valid, "event_seq": valid["event_seq"] + 1}],  # type: ignore[operator]
        [{**valid, "kind": "shutdown_signal"}],
        [{**valid, "severity": "warn"}],
        [{**valid, "generation": 0}],
        [{**valid, "reason_code": "forged"}],
        [{**valid, "detail": {"manifest_sha256": manifest.manifest_hash, "extra": True}}],
        [{**valid, "detail": json.dumps({"manifest_sha256": "0" * 64})}],
        [{**valid, "detail": {"manifest_sha256": manifest.manifest_hash, "x": float("nan")}}],
        [{**valid, "occurred_at": manifest.t0 - timedelta(seconds=1)}],
        [{**valid, "recorded_at": manifest.t0}],
    )
    for rows in forged_rows:
        with pytest.raises(l3_evidence.OperatorError):
            l3_evidence._validate_exact_binding(rows, manifest)


@pytest.mark.asyncio
async def test_checkpoint_refuses_manifest_not_before_without_database_access(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from scripts import l3_evidence

    manifest = _manifest(tmp_path)

    async def forbidden(*_args: object, **_kwargs: object) -> list[object]:
        raise AssertionError("not-before refusal must happen before database access")

    monkeypatch.setattr(l3_evidence, "_binding_rows", forbidden)
    with pytest.raises(l3_evidence.OperatorError, match="not available before"):
        await l3_evidence._build_checkpoint(
            dsn="redacted",
            manifest=manifest,
            start=manifest.t0,
            end=manifest.reports[1].end,
            now=manifest.reports[1].end - timedelta(microseconds=1),
        )


def test_missing_exact_t0_writes_permanent_not_closed_checkpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from scripts import l3_evidence

    manifest = _manifest(tmp_path)
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_bytes(canonical_manifest_bytes(manifest))
    report = l3_evidence._bind_t0_verdict(_report(manifest), exact_t0_sample=False)

    async def build(**_kwargs: object) -> L3SoakReport:
        return report

    monkeypatch.setattr(l3_evidence, "_build_checkpoint", build)
    monkeypatch.setenv("POLYARB_L2_RUNTIME_DB_DSN", "never-render-this-dsn")
    argv = [
        "checkpoint",
        "--manifest",
        str(manifest_path),
        "--start",
        manifest.t0.isoformat(),
        "--end",
        manifest.reports[0].end.isoformat(),
        "--output",
        manifest.reports[0].path,
    ]
    assert l3_evidence.main(argv) == l3_evidence.EXIT_NOT_CLOSED
    stored = l3_evidence.parse_report_bytes(Path(manifest.reports[0].path).read_bytes())
    assert stored.status is VerdictStatus.NOT_CLOSED
    assert {reason.code for reason in stored.reasons} == {"exact_t0_sample_missing"}
    assert l3_evidence.main(argv) == l3_evidence.EXIT_NOT_CLOSED
    assert l3_evidence.parse_report_bytes(Path(manifest.reports[0].path).read_bytes()) == stored


def test_exact_t0_sample_requires_one_pass_and_five_distinct_pairs(tmp_path: Path) -> None:
    from scripts import l3_evidence

    manifest = _manifest(tmp_path)
    actual = manifest.t0 + timedelta(seconds=5)
    health = SimpleNamespace(
        scheduled_at=manifest.t0,
        sampled_at=actual,
        sample_seq=3,
        status=HealthStatus.PASS,
    )
    markets = tuple(
        SimpleNamespace(
            sampled_at=actual,
            sample_seq=3,
            status=HealthStatus.PASS,
            market_id=f"m{i}",
            yes_token_id=f"y{i}",
            no_token_id=f"n{i}",
        )
        for i in range(5)
    )
    evidence = SimpleNamespace(health_samples=(health,), market_samples=markets)
    l3_evidence._require_exact_t0_sample(evidence, manifest)
    with pytest.raises(l3_evidence.OperatorError, match="exact scheduled T0"):
        l3_evidence._require_exact_t0_sample(
            SimpleNamespace(
                health_samples=(
                    SimpleNamespace(
                        scheduled_at=manifest.t0 + timedelta(seconds=30),
                        sampled_at=manifest.t0,
                        sample_seq=3,
                        status=HealthStatus.PASS,
                    ),
                ),
                market_samples=markets,
            ),
            manifest,
        )
    with pytest.raises(l3_evidence.OperatorError, match="incomplete"):
        l3_evidence._require_exact_t0_sample(
            SimpleNamespace(health_samples=(health,), market_samples=markets[:-1]), manifest
        )


@pytest.mark.asyncio
async def test_verify_loads_five_manifest_paths_and_requeries_each_interval(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from scripts import l3_evidence

    manifest = _manifest(tmp_path)
    encoded_by_bounds: dict[tuple[datetime, datetime], bytes] = {}
    report_by_bytes: dict[bytes, object] = {}
    for index, spec in enumerate(manifest.reports):
        encoded = f"report-{index}".encode()
        Path(spec.path).write_bytes(encoded)
        report = SimpleNamespace(
            start=spec.start,
            end=spec.end,
            manifest_hash=manifest.manifest_hash,
            soak_hash=manifest.soak_hash,
            status=VerdictStatus.PASS,
        )
        encoded_by_bounds[(spec.start, spec.end)] = encoded
        report_by_bytes[encoded] = report

    fetches: list[tuple[datetime, datetime]] = []

    class Store:
        def __init__(self, dsn: str) -> None:
            assert dsn == "runtime"

        async def fetch_window(self, start: datetime, end: datetime) -> object:
            fetches.append((start, end))
            return SimpleNamespace(start=start, end=end)

        async def append_boot(self, *_args: object) -> bool:
            raise AssertionError("read-only verify called a writer")

    async def binding(*_args: object, **_kwargs: object) -> list[dict[str, object]]:
        return [_binding_row(manifest)]

    def build(
        _evidence: object,
        _manifest: SoakManifest,
        start: datetime,
        end: datetime,
        *,
        require_24h: bool,
    ) -> object:
        assert require_24h is (end == manifest.t24)
        return report_by_bytes[encoded_by_bounds[(start, end)]]

    monkeypatch.setattr(l3_evidence, "L3EvidenceStore", Store)
    monkeypatch.setattr(l3_evidence, "_binding_rows", binding)
    monkeypatch.setattr(l3_evidence, "parse_report_bytes", report_by_bytes.__getitem__)
    monkeypatch.setattr(l3_evidence, "build_soak_report", build)
    monkeypatch.setattr(l3_evidence, "_has_exact_t0_sample", lambda *_args: True)
    monkeypatch.setattr(
        l3_evidence,
        "canonical_report_bytes",
        lambda report: encoded_by_bounds[(report.start, report.end)],
    )

    final = await l3_evidence._verify_reports(
        dsn="runtime", manifest=manifest, start=manifest.t0, end=manifest.t24
    )
    assert final.status is VerdictStatus.PASS
    assert fetches == [(manifest.t0, manifest.reports[0].end)] + [
        (spec.start, spec.end) for spec in manifest.reports
    ] + [(manifest.t0, manifest.t24)]


@pytest.mark.asyncio
async def test_verify_rejects_requeried_raw_digest_mismatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from scripts import l3_evidence

    manifest = _manifest(tmp_path)
    for spec in manifest.reports:
        Path(spec.path).write_bytes(b"stored")
    artifact = SimpleNamespace(
        start=manifest.t0,
        end=manifest.reports[0].end,
        manifest_hash=manifest.manifest_hash,
        soak_hash=manifest.soak_hash,
        status=VerdictStatus.PASS,
    )

    class Store:
        def __init__(self, _dsn: str) -> None:
            pass

        async def fetch_window(self, start: datetime, end: datetime) -> object:
            return SimpleNamespace(start=start, end=end)

    async def binding(*_args: object, **_kwargs: object) -> list[dict[str, object]]:
        return [_binding_row(manifest)]

    monkeypatch.setattr(l3_evidence, "L3EvidenceStore", Store)
    monkeypatch.setattr(l3_evidence, "_binding_rows", binding)
    monkeypatch.setattr(l3_evidence, "parse_report_bytes", lambda _data: artifact)
    monkeypatch.setattr(l3_evidence, "build_soak_report", lambda *_args, **_kwargs: artifact)
    monkeypatch.setattr(l3_evidence, "canonical_report_bytes", lambda _report: b"requeried")
    monkeypatch.setattr(l3_evidence, "_has_exact_t0_sample", lambda *_args: True)
    with pytest.raises(l3_evidence.OperatorError, match="raw evidence"):
        await l3_evidence._verify_reports(
            dsn="runtime", manifest=manifest, start=manifest.t0, end=manifest.t24
        )


def test_main_redacts_unexpected_exception_and_dsn(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from scripts import l3_evidence

    async def explode(_args: object) -> int:
        raise RuntimeError("postgresql://user:secret@db.example/postgres")

    monkeypatch.setattr(l3_evidence, "_run", explode)
    assert l3_evidence.main(["status"]) == l3_evidence.EXIT_UNAVAILABLE
    captured = capsys.readouterr()
    assert "secret" not in captured.err
    assert "postgresql" not in captured.err


def test_cleanup_missing_dedicated_dsn_never_falls_back_to_runtime(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from scripts import l3_evidence

    monkeypatch.delenv("L3_RETENTION_DSN", raising=False)
    monkeypatch.setenv("POLYARB_L2_RUNTIME_DB_DSN", "must-not-be-used")
    result = l3_evidence.main(
        [
            "retention-cleanup",
            "--cutoff",
            "2020-01-01T00:00:00Z",
            "--protected-start",
            "2030-01-01T00:00:00Z",
            "--protected-end",
            "2030-01-02T00:00:00Z",
            "--approval",
            l3_evidence.RETENTION_APPROVAL,
            "--expected-ref",
            "abcdefghijklmnopqrst",
        ]
    )
    assert result == l3_evidence.EXIT_NOT_CLOSED
    assert "L3_RETENTION_DSN" in capsys.readouterr().err


@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "missing_grant",
        "extra_grant",
        "sequence_grants",
        "elevated_field",
        "memberships",
        "rls_enabled",
        "ssl_encrypted",
    ),
    [
        (None, None, frozenset({"USAGE", "SELECT"}), None, {"l3_evidence_daemon"}, True, True),
        (
            ("markets_latest", "SELECT"),
            None,
            frozenset({"USAGE", "SELECT"}),
            None,
            {"l3_evidence_daemon"},
            True,
            True,
        ),
        (
            ("snapshots", "SELECT"),
            None,
            frozenset({"USAGE", "SELECT"}),
            None,
            {"l3_evidence_daemon"},
            True,
            True,
        ),
        (
            ("l2_event_cursor", "SELECT"),
            None,
            frozenset({"USAGE", "SELECT"}),
            None,
            {"l3_evidence_daemon"},
            True,
            True,
        ),
        (
            ("l2_event_cursor", "INSERT"),
            None,
            frozenset({"USAGE", "SELECT"}),
            None,
            {"l3_evidence_daemon"},
            True,
            True,
        ),
        (
            ("l2_event_cursor", "UPDATE"),
            None,
            frozenset({"USAGE", "SELECT"}),
            None,
            {"l3_evidence_daemon"},
            True,
            True,
        ),
        (
            None,
            ("snapshots", "UPDATE"),
            frozenset({"USAGE", "SELECT"}),
            None,
            {"l3_evidence_daemon"},
            True,
            True,
        ),
        (
            None,
            ("unexpected_table", "SELECT"),
            frozenset({"USAGE", "SELECT"}),
            None,
            {"l3_evidence_daemon"},
            True,
            True,
        ),
        (None, None, frozenset({"SELECT"}), None, {"l3_evidence_daemon"}, True, True),
        (None, None, frozenset({"USAGE"}), None, {"l3_evidence_daemon"}, True, True),
        (
            None,
            None,
            frozenset({"USAGE", "SELECT", "UPDATE"}),
            None,
            {"l3_evidence_daemon"},
            True,
            True,
        ),
        (
            None,
            None,
            frozenset({"USAGE", "SELECT"}),
            "bypass_rls",
            {"l3_evidence_daemon"},
            True,
            True,
        ),
        (
            None,
            None,
            frozenset({"USAGE", "SELECT"}),
            "can_create_db",
            {"l3_evidence_daemon"},
            True,
            True,
        ),
        (
            None,
            None,
            frozenset({"USAGE", "SELECT"}),
            "can_create_role",
            {"l3_evidence_daemon"},
            True,
            True,
        ),
        (
            None,
            None,
            frozenset({"USAGE", "SELECT"}),
            "can_replicate",
            {"l3_evidence_daemon"},
            True,
            True,
        ),
        (
            None,
            None,
            frozenset({"USAGE", "SELECT"}),
            "extra_function_acl",
            {"l3_evidence_daemon"},
            True,
            True,
        ),
        (
            None,
            None,
            frozenset({"USAGE", "SELECT"}),
            None,
            {"l3_evidence_daemon", "pg_read_all_data"},
            True,
            True,
        ),
        (
            None,
            None,
            frozenset({"USAGE", "SELECT"}),
            None,
            {"l3_evidence_daemon", "custom_role"},
            True,
            True,
        ),
        (None, None, frozenset({"USAGE", "SELECT"}), None, {"l3_evidence_daemon"}, False, True),
        (None, None, frozenset({"USAGE", "SELECT"}), None, {"l3_evidence_daemon"}, True, False),
    ],
)
async def test_runtime_credential_checks_inherited_effective_grants_and_store_preflight(
    monkeypatch: pytest.MonkeyPatch,
    missing_grant: tuple[str, str] | None,
    extra_grant: tuple[str, str] | None,
    sequence_grants: frozenset[str],
    elevated_field: str | None,
    memberships: set[str],
    rls_enabled: bool,
    ssl_encrypted: bool,
) -> None:
    from scripts import l3_evidence

    evidence_tables = {
        "l3_runtime_boots",
        "l3_promote_runs",
        "l3_health_samples",
        "l3_market_samples",
        "l3_runtime_events",
    }
    read_tables = {
        "markets_latest",
        "l2_book_levels",
        "l2_top_of_book",
        "l2_ohlc_1m",
        "snapshots",
    }
    cursor_grants = {("l2_event_cursor", privilege) for privilege in ("SELECT", "INSERT", "UPDATE")}

    class Connection:
        async def fetchrow(self, _sql: str) -> dict[str, object]:
            row = {
                "database_name": "postgres",
                "current_user": "runtime",
                "server_address": "10.0.0.1",
                "can_login": True,
                "is_superuser": False,
                "bypass_rls": False,
                "can_create_db": False,
                "can_create_role": False,
                "can_replicate": False,
                "is_database_owner": False,
                "is_evidence_owner": False,
                "service_member": False,
                "daemon_member": True,
                "retention_member": False,
                "ssl_encrypted": ssl_encrypted,
            }
            if elevated_field is not None:
                row[elevated_field] = True
            return row

        async def fetch(self, _sql: str, *_args: object) -> list[dict[str, object]]:
            if "WITH RECURSIVE inherited_roles" in _sql:
                return [{"role_name": role} for role in sorted(memberships)]
            if "aclexplode" in _sql:
                return (
                    [{"is_retention_cleanup": False, "privilege_type": "EXECUTE"}]
                    if elevated_field == "extra_function_acl"
                    else []
                )
            if "pg_policies" in _sql:
                return _cursor_policy_rows()
            if "has_sequence_privilege" in _sql:
                return [
                    {
                        "object_name": "l3_promote_runs_id_seq",
                        "privilege_type": privilege,
                        "allowed": privilege in sequence_grants,
                    }
                    for privilege in ("USAGE", "SELECT", "UPDATE")
                ]
            rows = []
            tables = evidence_tables | read_tables | {"l2_event_cursor"}
            if extra_grant is not None:
                tables.add(extra_grant[0])
            for table in tables:
                for privilege in (
                    "SELECT",
                    "INSERT",
                    "UPDATE",
                    "DELETE",
                    "TRUNCATE",
                    "REFERENCES",
                    "TRIGGER",
                ):
                    allowed = (
                        (table in evidence_tables and privilege in {"SELECT", "INSERT"})
                        or (table in read_tables and privilege == "SELECT")
                        or (table, privilege) in cursor_grants
                    )
                    if (table, privilege) == missing_grant:
                        allowed = False
                    if (table, privilege) == extra_grant:
                        allowed = True
                    rows.append(
                        {
                            "object_name": table,
                            "table_name": table,
                            "privilege_type": privilege,
                            "allowed": allowed,
                        }
                    )
            return rows

        async def fetchval(self, sql: str) -> bool:
            if "relrowsecurity" in sql:
                return rls_enabled
            if "has_sequence_privilege" in sql:
                return any(f"'{privilege}')" in sql for privilege in sequence_grants)
            return False

        async def close(self) -> None:
            return None

    async def connect(*, dsn: str) -> Connection:
        assert "secret" in dsn
        return Connection()

    preflights: list[str] = []

    async def preflight(dsn: str) -> None:
        preflights.append(dsn)

    monkeypatch.setattr(l3_evidence.asyncpg, "connect", connect)
    monkeypatch.setattr(l3_evidence, "_runtime_preflight", preflight)
    dsn = "postgresql://runtime:secret@db.abcdefghijklmnopqrst.supabase.co/postgres?sslmode=require"
    valid = (
        missing_grant is None
        and extra_grant is None
        and sequence_grants == frozenset({"USAGE", "SELECT"})
        and elevated_field is None
        and memberships == {"l3_evidence_daemon"}
        and rls_enabled
        and ssl_encrypted
    )
    if valid:
        result = await l3_evidence._credential_check(
            dsn, expected_ref="abcdefghijklmnopqrst", capability="runtime"
        )
        assert result["status"] == "PASS"
        assert preflights == [dsn]
        assert "secret" not in json.dumps(result)
    else:
        with pytest.raises(l3_evidence.OperatorError, match="NOT-CLOSED"):
            await l3_evidence._credential_check(
                dsn, expected_ref="abcdefghijklmnopqrst", capability="runtime"
            )
        assert preflights == []


@pytest.mark.asyncio
async def test_runtime_credential_pooler_identity_strips_only_exact_project_suffix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from scripts import l3_evidence

    required = {
        *(
            (table, privilege)
            for table in l3_evidence.EVIDENCE_TABLES
            for privilege in ("SELECT", "INSERT")
        ),
        *(
            (table, "SELECT")
            for table in (
                "markets_latest",
                "l2_book_levels",
                "l2_top_of_book",
                "l2_ohlc_1m",
                "snapshots",
            )
        ),
        ("l2_event_cursor", "SELECT"),
        ("l2_event_cursor", "INSERT"),
        ("l2_event_cursor", "UPDATE"),
    }

    class Connection:
        async def fetchrow(self, _sql: str) -> dict[str, object]:
            return {
                "database_name": "postgres",
                "current_user": "runtime",
                "server_address": "10.0.0.4",
                "can_login": True,
                "is_superuser": False,
                "bypass_rls": False,
                "can_create_db": False,
                "can_create_role": False,
                "can_replicate": False,
                "is_database_owner": False,
                "is_evidence_owner": False,
                "service_member": False,
                "daemon_member": True,
                "retention_member": False,
                "ssl_encrypted": True,
            }

        async def fetch(self, _sql: str, *_args: object) -> list[dict[str, object]]:
            if "WITH RECURSIVE inherited_roles" in _sql:
                return [{"role_name": "l3_evidence_daemon"}]
            if "aclexplode" in _sql:
                return []
            if "pg_policies" in _sql:
                return _cursor_policy_rows()
            if "has_sequence_privilege" in _sql:
                return [
                    {
                        "object_name": "l3_promote_runs_id_seq",
                        "privilege_type": privilege,
                        "allowed": True,
                    }
                    for privilege in ("USAGE", "SELECT")
                ]
            return [
                {"object_name": table, "privilege_type": privilege, "allowed": True}
                for table, privilege in required
            ]

        async def fetchval(self, sql: str) -> bool:
            return "relrowsecurity" in sql

        async def close(self) -> None:
            return None

    async def connect(*, dsn: str) -> Connection:
        assert dsn.startswith("postgresql://runtime.")
        return Connection()

    async def preflight(_dsn: str) -> None:
        return None

    monkeypatch.setattr(l3_evidence.asyncpg, "connect", connect)
    monkeypatch.setattr(l3_evidence, "_runtime_preflight", preflight)
    proof = await l3_evidence._credential_check(
        "postgresql://runtime.abcdefghijklmnopqrst:secret@"
        "aws-0-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require",
        expected_ref="abcdefghijklmnopqrst",
        capability="runtime",
    )
    assert proof["current_user"] == "runtime"


@pytest.mark.asyncio
async def test_retention_operator_check_requires_exclusive_rpc_capability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from scripts import l3_evidence

    class Connection:
        async def fetchrow(self, _sql: str) -> dict[str, object]:
            return {
                "database_name": "postgres",
                "current_user": "retention",
                "server_address": "10.0.0.2",
                "can_login": True,
                "is_superuser": False,
                "bypass_rls": False,
                "can_create_db": False,
                "can_create_role": False,
                "can_replicate": False,
                "is_database_owner": False,
                "is_evidence_owner": False,
                "service_member": False,
                "daemon_member": False,
                "retention_member": True,
                "ssl_encrypted": True,
            }

        async def fetch(self, _sql: str, *_args: object) -> list[dict[str, object]]:
            if "WITH RECURSIVE inherited_roles" in _sql:
                return [{"role_name": "l3_retention_operator"}]
            if "aclexplode" in _sql:
                return [{"is_retention_cleanup": True, "privilege_type": "EXECUTE"}]
            if "has_sequence_privilege" in _sql:
                return []
            return [
                {
                    "object_name": "l3_runtime_boots",
                    "privilege_type": "SELECT",
                    "allowed": False,
                }
            ]

        async def fetchval(self, sql: str) -> bool:
            return "has_function_privilege" in sql

        async def close(self) -> None:
            return None

    async def connect(*, dsn: str) -> Connection:
        assert "retention" in dsn
        return Connection()

    monkeypatch.setattr(l3_evidence.asyncpg, "connect", connect)
    result = await l3_evidence._credential_check(
        "postgresql://retention:secret@db.abcdefghijklmnopqrst.supabase.co/postgres"
        "?sslmode=require",
        expected_ref="abcdefghijklmnopqrst",
        capability="retention",
    )
    assert result["capability_role"] == "l3_retention_operator"


@pytest.mark.asyncio
async def test_retention_check_passes_fresh_schema_but_detects_provable_early_deletion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from scripts import l3_evidence

    manifest = _manifest(tmp_path)
    now = datetime(2030, 2, 15, tzinfo=UTC)
    rows = {table: 0 for table in l3_evidence.EVIDENCE_TABLES}
    oldest = {table: None for table in l3_evidence.EVIDENCE_TABLES}
    newest = {table: None for table in l3_evidence.EVIDENCE_TABLES}

    class Store:
        def __init__(self, _dsn: str) -> None:
            pass

        async def retention_bounds(self) -> object:
            return SimpleNamespace(
                row_count_by_table=rows,
                oldest_recorded_at_by_table=oldest,
                newest_recorded_at_by_table=newest,
            )

    async def preflight(_dsn: str) -> None:
        return None

    first_started = now - timedelta(days=1)

    async def anchor(_dsn: str) -> dict[str, object]:
        return {
            "first_started_at": first_started,
            "acceptance_config_hash": manifest.acceptance_config_hash,
            "code_version": manifest.code_version,
        }

    monkeypatch.setattr(l3_evidence, "L3EvidenceStore", Store)
    monkeypatch.setattr(l3_evidence, "_runtime_preflight", preflight)
    monkeypatch.setattr(l3_evidence, "_retention_policy_anchor", anchor)
    monkeypatch.setattr(
        l3_evidence.AcceptanceConfig,
        "from_settings",
        classmethod(lambda _cls, *_args, **_kwargs: manifest.acceptance_config),
    )
    result = await l3_evidence._retention_check("runtime", now=now)
    assert result["status"] == "PASS"
    assert result["history_mature"] is False
    assert result["row_count_by_table"] == rows

    first_started = now - timedelta(days=31)
    with pytest.raises(l3_evidence.OperatorError, match="premature"):
        await l3_evidence._retention_check("runtime", now=now)


def test_cleanup_invokes_only_dedicated_operator_with_exact_bounds(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from scripts import l3_evidence

    calls: list[tuple[datetime, datetime, datetime]] = []

    async def credential(dsn: str, **kwargs: object) -> dict[str, object]:
        assert dsn == "retention-only"
        assert kwargs["capability"] == "retention"
        return {"status": "PASS"}

    class Operator:
        def __init__(self, dsn: str) -> None:
            assert dsn == "retention-only"

        async def run_retention_cleanup(
            self, *, cutoff: datetime, protected_start: datetime, protected_end: datetime
        ) -> RetentionCleanupResult:
            calls.append((cutoff, protected_start, protected_end))
            return RetentionCleanupResult(1, 2, 3, 4, 5)

    monkeypatch.setenv("L3_RETENTION_DSN", "retention-only")
    monkeypatch.setenv("POLYARB_L2_RUNTIME_DB_DSN", "must-not-be-used")
    monkeypatch.setattr(l3_evidence, "_credential_check", credential)
    monkeypatch.setattr(l3_evidence, "L3RetentionOperator", Operator)
    assert (
        l3_evidence.main(
            [
                "retention-cleanup",
                "--cutoff",
                "2020-01-01T00:00:00Z",
                "--protected-start",
                "2030-01-01T00:00:00Z",
                "--protected-end",
                "2030-01-02T00:00:00Z",
                "--approval",
                l3_evidence.RETENTION_APPROVAL,
                "--expected-ref",
                "abcdefghijklmnopqrst",
            ]
        )
        == l3_evidence.EXIT_OK
    )
    assert calls == [
        (
            datetime(2020, 1, 1, tzinfo=UTC),
            datetime(2030, 1, 1, tzinfo=UTC),
            datetime(2030, 1, 2, tzinfo=UTC),
        )
    ]
    output = capsys.readouterr().out
    assert "retention-only" not in output
    assert '"runtime_events_deleted":5' in output


@pytest.mark.asyncio
async def test_prod_revision_proves_exact_ref_user_server_and_revision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from scripts import l3_evidence

    revision = "007"
    ssl_encrypted = True

    class Connection:
        async def fetchrow(self, _sql: str) -> dict[str, object]:
            return {
                "database_name": "postgres",
                "current_user": "postgres",
                "server_address": "10.0.0.3",
                "revision": revision,
                "ssl_encrypted": ssl_encrypted,
            }

        async def close(self) -> None:
            return None

    async def connect(*, dsn: str) -> Connection:
        assert "secret" in dsn
        return Connection()

    monkeypatch.setattr(l3_evidence.asyncpg, "connect", connect)
    dsn = (
        "postgresql://postgres:secret@db.abcdefghijklmnopqrst.supabase.co/postgres?sslmode=require"
    )
    proof = await l3_evidence._prod_revision(
        dsn, expected_ref="abcdefghijklmnopqrst", expected_revision="007"
    )
    assert proof["revision"] == "007"
    assert proof["server_address"] == "10.0.0.3"
    assert proof["encrypted_session"] is True
    assert proof["ssl_mode"] == "require"
    assert "secret" not in json.dumps(proof)
    revision = "006"
    with pytest.raises(l3_evidence.OperatorError, match="NOT-CLOSED"):
        await l3_evidence._prod_revision(
            dsn, expected_ref="abcdefghijklmnopqrst", expected_revision="007"
        )
    revision = "007"
    ssl_encrypted = False
    with pytest.raises(l3_evidence.OperatorError, match="NOT-CLOSED"):
        await l3_evidence._prod_revision(
            dsn, expected_ref="abcdefghijklmnopqrst", expected_revision="007"
        )


def test_all_read_only_database_paths_have_no_writer_calls() -> None:
    from scripts import l3_evidence

    functions = (
        l3_evidence._create_manifest_from_runtime,
        l3_evidence._status,
        l3_evidence._build_checkpoint,
        l3_evidence._verify_reports,
        l3_evidence._retention_check,
        l3_evidence._credential_check,
        l3_evidence._prod_revision,
    )
    for function in functions:
        source = inspect.getsource(function)
        assert ".append_" not in source
        assert ".execute(" not in source
        assert "run_retention_cleanup" not in source


def test_make_help_lists_operator_targets_and_required_args_fail_closed() -> None:
    root = Path(__file__).resolve().parents[2]
    help_result = subprocess.run(
        ["make", "help"], cwd=root, text=True, capture_output=True, check=False
    )
    assert help_result.returncode == 0
    required = {
        "l3-evidence-status",
        "l3-soak-checkpoint",
        "l3-soak-verify",
        "l3-evidence-retention-check",
    }
    assert required <= {token.rstrip(":") for token in help_result.stdout.split()}
    for target in (
        "l3-soak-manifest",
        "l3-soak-manifest-bind",
        "l3-soak-checkpoint",
        "l3-soak-verify",
        "l3-runtime-credential-check",
        "l3-retention-operator-check",
        "l3-evidence-retention-cleanup",
        "supabase-prod-revision",
    ):
        result = subprocess.run(
            ["make", target], cwd=root, text=True, capture_output=True, check=False
        )
        assert result.returncode != 0, target
