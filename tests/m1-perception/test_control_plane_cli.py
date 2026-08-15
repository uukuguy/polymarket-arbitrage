"""Operator CLI contract for the non-mutating control-plane shadow bridge."""

from __future__ import annotations

import json
from pathlib import Path

import pytest


def test_quote_control_plane_once_requires_explicit_enable(monkeypatch, capsys) -> None:
    from polyarb import cli_control_plane

    monkeypatch.setenv(
        "POLYARB_SUPABASE_DB_DSN", "postgresql://operator:secret@example.test/control"
    )
    monkeypatch.setattr(
        cli_control_plane.psycopg,
        "connect",
        lambda _dsn: (_ for _ in ()).throw(AssertionError("must not connect")),
    )

    assert cli_control_plane.main(["quote-once", "--json"]) == 2

    captured = capsys.readouterr()
    assert "--enable is required" in captured.err
    assert "postgresql://" not in captured.err


def test_structure_control_plane_once_requires_explicit_enable(monkeypatch, capsys) -> None:
    from polyarb import cli_control_plane

    monkeypatch.setenv(
        "POLYARB_SUPABASE_DB_DSN", "postgresql://operator:secret@example.test/control"
    )
    monkeypatch.setattr(
        cli_control_plane.psycopg,
        "connect",
        lambda _dsn: (_ for _ in ()).throw(AssertionError("must not connect")),
    )

    assert cli_control_plane.main(["structure-once", "--json"]) == 2

    captured = capsys.readouterr()
    assert "--enable is required" in captured.err
    assert "postgresql://" not in captured.err


def test_structure_shadow_once_requires_explicit_enable(monkeypatch, capsys, tmp_path) -> None:
    from polyarb import cli_control_plane

    monkeypatch.setenv(
        "POLYARB_SUPABASE_DB_DSN", "postgresql://operator:secret@example.test/control"
    )
    monkeypatch.setattr(
        cli_control_plane.psycopg,
        "connect",
        lambda _dsn: (_ for _ in ()).throw(AssertionError("must not connect")),
    )

    assert (
        cli_control_plane.main(
            [
                "structure-shadow-once",
                "--db-path",
                str(tmp_path / "state.db"),
                "--publication-id",
                "publication-1",
                "--json",
            ]
        )
        == 2
    )

    captured = capsys.readouterr()
    assert "--enable is required" in captured.err
    assert "postgresql://" not in captured.err


def test_structure_shadow_publish_requires_explicit_enable_before_connect(
    monkeypatch, capsys
) -> None:
    from polyarb import cli_control_plane

    monkeypatch.setenv(
        "POLYARB_SUPABASE_DB_DSN", "postgresql://operator:secret@example.test/control"
    )
    monkeypatch.setattr(
        cli_control_plane.psycopg,
        "connect",
        lambda _dsn: (_ for _ in ()).throw(AssertionError("must not connect")),
    )

    assert (
        cli_control_plane.main(
            ["structure-shadow-publish", "--generation-key", "structure:a", "--json"]
        )
        == 2
    )
    assert "--enable is required" in capsys.readouterr().err


def test_control_plane_tick_once_requires_enable_before_connect(monkeypatch, capsys) -> None:
    from polyarb import cli_control_plane

    monkeypatch.setenv(
        "POLYARB_SUPABASE_DB_DSN", "postgresql://operator:secret@example.test/control"
    )
    monkeypatch.setattr(
        cli_control_plane.psycopg,
        "connect",
        lambda _dsn: (_ for _ in ()).throw(AssertionError("must not connect")),
    )

    assert cli_control_plane.main(["tick-once", "--max-turns", "2", "--json"]) == 2
    assert "--enable is required" in capsys.readouterr().err


def test_control_plane_serve_requires_enable_before_connect(monkeypatch, capsys) -> None:
    from polyarb import cli_control_plane

    monkeypatch.setenv(
        "POLYARB_SUPABASE_DB_DSN", "postgresql://operator:secret@example.test/control"
    )
    monkeypatch.setattr(
        cli_control_plane.psycopg,
        "connect",
        lambda _dsn: (_ for _ in ()).throw(AssertionError("must not connect")),
    )

    assert cli_control_plane.main(["serve", "--interval-seconds", "15", "--json"]) == 2
    assert "--enable is required" in capsys.readouterr().err


def test_structure_source_once_requires_enable_before_connect(monkeypatch, capsys) -> None:
    from polyarb import cli_control_plane

    monkeypatch.setenv(
        "POLYARB_SUPABASE_DB_DSN", "postgresql://operator:secret@example.test/control"
    )
    monkeypatch.setattr(
        cli_control_plane.psycopg,
        "connect",
        lambda _dsn: (_ for _ in ()).throw(AssertionError("must not connect")),
    )

    assert (
        cli_control_plane.main(["structure-source-once", "--window-key", "window:one", "--json"])
        == 2
    )
    assert "--enable is required" in capsys.readouterr().err


def test_structure_source_once_admits_one_window_then_runs_one_source_page(
    monkeypatch, capsys
) -> None:
    from polyarb import cli_control_plane

    class ControlPlane:
        def admit_structure_source_window(self, *, window_key: str, now):
            assert window_key == "window:one"
            assert now.tzinfo is not None

    class SourceWorker:
        async def run_once(self):
            return type(
                "Result", (), {"job_key": "window:one:fetch:events:0", "outcome": "succeeded"}
            )()

        async def aclose(self):
            return None

    monkeypatch.setenv(
        "POLYARB_SUPABASE_DB_DSN", "postgresql://operator:secret@example.test/control"
    )
    monkeypatch.setattr(cli_control_plane, "_control_plane_from_env", lambda: ControlPlane())
    monkeypatch.setattr(
        cli_control_plane,
        "_transactional_structure_source_worker",
        lambda _control_plane, *, worker_id: SourceWorker(),
    )

    assert (
        cli_control_plane.main(
            [
                "structure-source-once",
                "--enable",
                "--window-key",
                "window:one",
                "--json",
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out) == {
        "page": {"job_key": "window:one:fetch:events:0", "outcome": "succeeded"},
        "pointer_mutations": 0,
        "status": "ok",
        "window_key": "window:one",
    }


def test_control_plane_serve_builds_one_scheduler_service(monkeypatch, capsys) -> None:
    from polyarb import cli_control_plane

    scheduler = object()
    captured: dict[str, int] = {}
    monkeypatch.setenv(
        "POLYARB_SUPABASE_DB_DSN", "postgresql://operator:secret@example.test/control"
    )
    monkeypatch.setattr(cli_control_plane, "_control_plane_from_env", lambda: object())
    def transactional_scheduler(
        _control_plane,
        *,
        worker_id,
        max_turns,
        structure_materializer_turns,
        structure_range_turns,
        crash_after_r2_upload,
    ):
        captured.update(
            max_turns=max_turns,
            structure_materializer_turns=structure_materializer_turns,
            structure_range_turns=structure_range_turns,
        )
        return scheduler

    monkeypatch.setattr(cli_control_plane, "_transactional_scheduler", transactional_scheduler)

    async def run_service(actual_scheduler, *, interval_seconds: float, as_json: bool):
        assert actual_scheduler is scheduler
        assert interval_seconds == 7.5
        assert as_json is True
        return {"status": "stopped", "ticks": 3}

    monkeypatch.setattr(cli_control_plane, "_run_scheduler_service", run_service)

    assert (
        cli_control_plane.main(
            [
                "serve",
                "--enable",
                "--interval-seconds",
                "7.5",
                "--max-turns",
                "2",
                "--structure-materializer-turns",
                "8",
                "--structure-range-turns",
                "8",
                "--json",
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out) == {"status": "stopped", "ticks": 3}
    assert captured == {
        "max_turns": 2,
        "structure_materializer_turns": 8,
        "structure_range_turns": 8,
    }


def test_r2_upload_fault_callback_requires_exact_acknowledgement_and_job_key() -> None:
    from polyarb import cli_control_plane

    with pytest.raises(ValueError, match="exact staging acknowledgement"):
        cli_control_plane._r2_upload_fault_callback(
            target_job_key="structure:one:range:0", acknowledgement=None
        )
    with pytest.raises(ValueError, match="requires a target job key"):
        cli_control_plane._r2_upload_fault_callback(
            target_job_key=None,
            acknowledgement="staging-r2-upload-before-receipt",
        )

    callback = cli_control_plane._r2_upload_fault_callback(
        target_job_key="structure:one:range:0",
        acknowledgement="staging-r2-upload-before-receipt",
    )
    assert callback is not None
    callback(type("Lease", (), {"job_key": "structure:other:range:0"})())
    with pytest.raises(KeyboardInterrupt, match="verified R2 upload"):
        callback(type("Lease", (), {"job_key": "structure:one:range:0"})())


def test_quote_control_plane_once_runs_one_batch_then_certifier(monkeypatch, capsys) -> None:
    from polyarb import cli_control_plane

    class BatchWorker:
        async def run_once(self):
            return type("Result", (), {"job_key": "quote:one:batch:0", "outcome": "succeeded"})()

    class Certifier:
        def run_once(self):
            return type("Result", (), {"job_key": "quote:one:certify", "outcome": "waiting"})()

    monkeypatch.setenv(
        "POLYARB_SUPABASE_DB_DSN", "postgresql://operator:secret@example.test/control"
    )
    monkeypatch.setattr(cli_control_plane.psycopg, "connect", lambda _dsn: object())
    monkeypatch.setattr(
        cli_control_plane,
        "_transactional_quote_workers",
        lambda _control_plane, *, worker_id: (BatchWorker(), Certifier()),
    )

    assert (
        cli_control_plane.main(["quote-once", "--enable", "--worker-id", "test-worker", "--json"])
        == 0
    )

    assert json.loads(capsys.readouterr().out) == {
        "batch": {"job_key": "quote:one:batch:0", "outcome": "succeeded"},
        "certifier": {"job_key": "quote:one:certify", "outcome": "waiting"},
        "status": "ok",
    }


def test_structure_control_plane_once_runs_one_range_without_pointer_mutation(
    monkeypatch, capsys
) -> None:
    from polyarb import cli_control_plane

    class Worker:
        async def run_once(self):
            return type(
                "Result",
                (),
                {"job_key": "structure:one:normalize:events:0", "outcome": "succeeded"},
            )()

    monkeypatch.setenv(
        "POLYARB_SUPABASE_DB_DSN", "postgresql://operator:secret@example.test/control"
    )
    monkeypatch.setattr(cli_control_plane.psycopg, "connect", lambda _dsn: object())
    monkeypatch.setattr(
        cli_control_plane,
        "_transactional_structure_worker",
        lambda _control_plane, *, worker_id: Worker(),
    )

    assert (
        cli_control_plane.main(
            ["structure-once", "--enable", "--worker-id", "test-worker", "--json"]
        )
        == 0
    )

    assert json.loads(capsys.readouterr().out) == {
        "pointer_mutations": 0,
        "range": {"job_key": "structure:one:normalize:events:0", "outcome": "succeeded"},
        "status": "ok",
    }


def test_structure_shadow_once_exports_admits_without_pointer_mutation(
    monkeypatch, capsys, tmp_path
) -> None:
    from polyarb import cli_control_plane
    from polyarb.control_plane.structure_artifact import StructureBundleIdentity

    identity = StructureBundleIdentity(
        publication_id="publication-1",
        window_id="window-1",
        snapshot_id=42,
        comparison_receipt_digest="a" * 64,
        normalization_contract_version="structure-v7",
        component_counts={
            "events": 0,
            "event_tags": 0,
            "memberships": 0,
            "group_truth": 0,
            "markets": 0,
            "issues": 0,
        },
    )

    class ObjectClient:
        pass

    class ControlPlane:
        def enqueue_structure_generation(self, **kwargs):
            assert kwargs["identity"] == identity
            assert len(kwargs["ranges"]) == 6
            return tuple(range(6))

    monkeypatch.setenv(
        "POLYARB_SUPABASE_DB_DSN", "postgresql://operator:secret@example.test/control"
    )
    monkeypatch.setattr(cli_control_plane.psycopg, "connect", lambda _dsn: object())
    monkeypatch.setattr(
        cli_control_plane,
        "read_legacy_structure_bundle",
        lambda _path, *, publication_id: (identity, {key: () for key in identity.component_counts}),
    )
    monkeypatch.setattr(
        cli_control_plane,
        "_structure_object_client",
        lambda: (ObjectClient(), "structure"),
    )
    monkeypatch.setattr(
        cli_control_plane,
        "upload_structure_bundle_artifact",
        lambda _client, *, bucket, artifact: artifact,
    )
    monkeypatch.setattr(cli_control_plane, "_control_plane_from_env", lambda: ControlPlane())

    assert (
        cli_control_plane.main(
            [
                "structure-shadow-once",
                "--enable",
                "--db-path",
                str(tmp_path / "state.db"),
                "--publication-id",
                "publication-1",
                "--json",
            ]
        )
        == 0
    )
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "ok"
    assert result["admitted_job_count"] == 6
    assert result["pointer_mutations"] == 0
    assert result["source_identity"]["publication_id"] == "publication-1"


def test_structure_shadow_publish_reports_previous_and_current_identity(
    monkeypatch, capsys
) -> None:
    from polyarb import cli_control_plane

    generation_key = "structure:" + "a" * 64

    class ControlPlane:
        def structure_shadow_pointer(self):
            return {"generation_key": "structure:" + "b" * 64}

        def publish_structure_shadow(self, *, generation_key: str, now):
            assert generation_key == globals_generation_key
            return generation_key

    globals_generation_key = generation_key
    monkeypatch.setenv(
        "POLYARB_SUPABASE_DB_DSN", "postgresql://operator:secret@example.test/control"
    )
    monkeypatch.setattr(cli_control_plane, "_control_plane_from_env", lambda: ControlPlane())

    assert (
        cli_control_plane.main(
            ["structure-shadow-publish", "--enable", "--generation-key", generation_key, "--json"]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out) == {
        "current_generation_key": generation_key,
        "legacy_pointer_mutations": 0,
        "previous_generation_key": "structure:" + "b" * 64,
        "status": "ok",
    }


def test_control_plane_tick_once_reports_bounded_turns(monkeypatch, capsys) -> None:
    from polyarb import cli_control_plane

    class Scheduler:
        async def run_tick(self):
            return {"status": "ok", "turns": [{"worker": "structure-range", "outcome": "idle"}]}

    monkeypatch.setenv(
        "POLYARB_SUPABASE_DB_DSN", "postgresql://operator:secret@example.test/control"
    )
    monkeypatch.setattr(cli_control_plane, "_control_plane_from_env", lambda: object())
    def transactional_scheduler(
        _control_plane,
        *,
        worker_id,
        max_turns,
        structure_materializer_turns,
        structure_range_turns,
        crash_after_r2_upload,
    ):
        return Scheduler()

    monkeypatch.setattr(cli_control_plane, "_transactional_scheduler", transactional_scheduler)

    assert cli_control_plane.main(["tick-once", "--enable", "--max-turns", "2", "--json"]) == 0
    assert json.loads(capsys.readouterr().out) == {
        "status": "ok",
        "turns": [{"outcome": "idle", "worker": "structure-range"}],
    }


def test_structure_source_factory_builds_eight_distinct_lease_lanes(monkeypatch) -> None:
    from polyarb import cli_control_plane

    captured: list[dict[str, object]] = []

    class SourceWorker:
        def __init__(self, **kwargs: object) -> None:
            captured.append(kwargs)

    class SourcePool:
        def __init__(self, *, lanes: tuple[SourceWorker, ...]) -> None:
            self.lanes = lanes

    object_client = object()
    monkeypatch.setattr(cli_control_plane, "TransactionalStructureSourceWorker", SourceWorker)
    monkeypatch.setattr(cli_control_plane, "TransactionalStructureSourcePool", SourcePool)
    monkeypatch.setattr(cli_control_plane, "GammaClient", lambda _settings: object())
    monkeypatch.setattr(
        cli_control_plane,
        "_structure_object_client",
        lambda: (object_client, "source-bucket"),
    )

    result = cli_control_plane._transactional_structure_source_worker(
        object(), worker_id="control:source"
    )

    assert isinstance(result, SourcePool)
    assert [lane["worker_id"] for lane in captured] == [
        f"control:source:{ordinal}" for ordinal in range(8)
    ]
    assert {id(lane["object_client"]) for lane in captured} == {id(object_client)}
    assert {lane["bucket"] for lane in captured} == {"source-bucket"}


def test_control_plane_preflight_proves_named_database_and_r2_readiness(
    monkeypatch, capsys
) -> None:
    from polyarb import cli_control_plane

    class ControlPlane:
        def deployment_preflight(self, *, expected_database: str):
            assert expected_database == "control_plane_staging"
            return {
                "database_name": expected_database,
                "postgres_version": "PostgreSQL 16.4",
                "revision_014_tables": 20,
            }

    class ObjectClient:
        def head_bucket(self, **kwargs):
            assert kwargs == {"Bucket": "control-plane-artifacts"}

    monkeypatch.setenv(
        "POLYARB_SUPABASE_DB_DSN", "postgresql://operator:secret@example.test/control"
    )
    monkeypatch.setattr(cli_control_plane, "_control_plane_from_env", lambda: ControlPlane())
    monkeypatch.setattr(
        cli_control_plane,
        "_structure_object_client",
        lambda: (ObjectClient(), "control-plane-artifacts"),
    )

    assert (
        cli_control_plane.main(
            ["preflight", "--expected-database", "control_plane_staging", "--json"]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out) == {
        "control_plane": {
            "database_name": "control_plane_staging",
            "postgres_version": "PostgreSQL 16.4",
            "revision_014_tables": 20,
        },
        "r2": {"bucket": "control-plane-artifacts", "reachable": True},
        "status": "ready-for-shadow-only",
    }


def test_render_rollout_is_explicit_and_never_connects_to_control_plane(
    monkeypatch, capsys, tmp_path
) -> None:
    from polyarb import cli_control_plane

    monkeypatch.setattr(
        cli_control_plane,
        "_control_plane_from_env",
        lambda: (_ for _ in ()).throw(AssertionError("must not connect")),
    )

    assert (
        cli_control_plane.main(
            [
                "render-rollout",
                "--enable",
                "--api-app",
                "polyarb-control-api-staging",
                "--worker-app",
                "polyarb-control-worker-staging",
                "--alert-app",
                "polyarb-control-alert-staging",
                "--expected-database",
                "control_plane_staging",
                "--output-dir",
                str(tmp_path),
                "--json",
            ]
        )
        == 0
    )
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "rendered-local-only"
    assert Path(result["checklist"]).exists()


def test_shadow_parity_verifier_reads_only_local_evidence(monkeypatch, capsys, tmp_path) -> None:
    from polyarb import cli_control_plane

    evidence = {
        "runs": [
            {
                "run_id": f"run-{index}",
                "legacy": {
                    "source_identity": {
                        "publication_id": "publication-1",
                        "window_id": "window-1",
                        "snapshot_id": 42,
                        "comparison_receipt_digest": "a" * 64,
                    },
                    "bundle_digest": "a" * 64,
                    "component_counts": {
                        "events": 0,
                        "event_tags": 0,
                        "memberships": 0,
                        "group_truth": 0,
                        "markets": 0,
                        "issues": 0,
                    },
                    "quote_universe_hash": "a" * 64,
                },
                "transactional": {
                    "source_identity": {
                        "publication_id": "publication-1",
                        "window_id": "window-1",
                        "snapshot_id": 42,
                        "comparison_receipt_digest": "a" * 64,
                    },
                    "bundle_digest": "a" * 64,
                    "manifest_digest": "b" * 64,
                    "component_counts": {
                        "events": 0,
                        "event_tags": 0,
                        "memberships": 0,
                        "group_truth": 0,
                        "markets": 0,
                        "issues": 0,
                    },
                    "quote_universe_hash": "a" * 64,
                    "legacy_pointer_mutations": 0,
                },
            }
            for index in range(3)
        ]
    }
    path = tmp_path / "shadow-parity.json"
    path.write_text(json.dumps(evidence))
    monkeypatch.setattr(
        cli_control_plane,
        "_control_plane_from_env",
        lambda: (_ for _ in ()).throw(AssertionError("must not connect")),
    )

    assert cli_control_plane.main(["verify-shadow-parity", "--evidence", str(path), "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "PASS"


def test_fault_soak_verifier_reads_only_local_evidence(monkeypatch, capsys, tmp_path) -> None:
    from polyarb import cli_control_plane

    evidence = {
        "takeovers": [
            {
                "worker": worker,
                "crash_boundary": "r2-upload-before-receipt",
                "lease_reclaimed_seconds": 90,
                "lease_reclaim_sla_seconds": 120,
                "old_certified_truth_available": True,
                "control_api_readable": True,
                "circuit": {
                    "job_key": (
                        "structure:window-a:fetch:events:0"
                        if worker == "structure"
                        else "quote:generation-a:batch:0000"
                    ),
                    "opened_after_failures": 3,
                    "probe_delays_seconds": [15, 30, 60],
                    "replacement_worker": f"{worker}-replacement-a",
                    "recovery_event_kind": "recovered",
                    "incident_resolved": True,
                    "delivery_receipts": ["dashboard", "telegram"],
                },
            }
            for worker in ("structure", "quote")
        ],
        "soak": {
            "duration_seconds": 86_400,
            "scheduler_ticks": 5_760,
            "control_api_readable": True,
            "manual_unlocks": 0,
            "silent_stops": 0,
            "permanent_degradations": 0,
            "circuit_recoveries": 2,
        },
    }
    path = tmp_path / "fault-soak.json"
    path.write_text(json.dumps(evidence))
    monkeypatch.setattr(
        cli_control_plane,
        "_control_plane_from_env",
        lambda: (_ for _ in ()).throw(AssertionError("must not connect")),
    )

    assert cli_control_plane.main(["verify-fault-soak", "--evidence", str(path), "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "PASS"


def test_shadow_sync_requires_dsn_without_printing_it(monkeypatch, capsys, tmp_path) -> None:
    from polyarb import cli_control_plane

    monkeypatch.delenv("POLYARB_SUPABASE_DB_DSN", raising=False)

    assert (
        cli_control_plane.main(["shadow-sync", "--db-path", str(tmp_path / "state.db"), "--json"])
        == 2
    )
    captured = capsys.readouterr()
    assert "POLYARB_SUPABASE_DB_DSN is required" in captured.err
    assert "postgresql://" not in captured.err


def test_shadow_sync_reports_idempotent_source_count(monkeypatch, capsys, tmp_path) -> None:
    from polyarb import cli_control_plane
    from polyarb.control_plane.shadow import ShadowSource

    monkeypatch.setenv(
        "POLYARB_SUPABASE_DB_DSN", "postgresql://operator:secret@example.test/control"
    )
    monkeypatch.setattr(
        cli_control_plane,
        "read_shadow_sources",
        lambda _path, *, limit: (ShadowSource.quote_attempt(3035),),
    )
    monkeypatch.setattr(
        cli_control_plane,
        "project_shadow_sources",
        lambda sources, *, control_plane, now: len(sources),
    )
    monkeypatch.setattr(cli_control_plane.psycopg, "connect", lambda _dsn: object())

    assert (
        cli_control_plane.main(
            ["shadow-sync", "--db-path", str(tmp_path / "state.db"), "--limit", "20", "--json"]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out) == {
        "pointer_mutations": 0,
        "projected_sources": 1,
        "status": "ok",
    }
