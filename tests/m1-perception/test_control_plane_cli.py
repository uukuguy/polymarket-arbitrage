"""Operator CLI contract for the non-mutating control-plane shadow bridge."""

from __future__ import annotations

import json
import threading
from datetime import timedelta
from pathlib import Path
from typing import cast

import pytest


class _BootstrapConnection:
    def __init__(self, captured: dict[str, object]) -> None:
        self._captured = captured
        self.autocommit = False

    def execute(self, query: str, params: tuple[str, ...]) -> _BootstrapConnection:
        self._captured["bootstrap"] = (query, params)
        return self

    def fetchone(self) -> tuple[str, str, str, list[str]]:
        return ("pg_catalog,public", "5s", "1s", ["pg_catalog", "public"])

    def cancel_safe(self, *, timeout: float) -> None:
        self._captured["bootstrap_cancel_timeout"] = timeout

    def close(self) -> None:
        self._captured["closed"] = True


@pytest.fixture(autouse=True)
def _allow_daemon_database_role(monkeypatch: pytest.MonkeyPatch) -> None:
    from polyarb import cli_control_plane

    monkeypatch.setenv("POLYARB_DB_EXPECTED_DATABASE", "control")
    monkeypatch.setattr(
        cli_control_plane,
        "verify_daemon_database_role",
        lambda *_args, **_kwargs: None,
        raising=False,
    )


def test_control_plane_connection_factory_bounds_postgres_connect_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A dead Postgres authority must not indefinitely stall a control-plane turn."""
    from polyarb import cli_control_plane

    captured: dict[str, object] = {}
    monkeypatch.setenv(
        "POLYARB_SUPABASE_DB_DSN", "postgresql://operator:secret@example.test/control"
    )
    monkeypatch.setattr(
        cli_control_plane.psycopg,
        "connect",
        lambda dsn, **kwargs: (
            captured.update(dsn=dsn, kwargs=kwargs) or _BootstrapConnection(captured)
        ),
    )

    control_plane = cli_control_plane._control_plane_from_env()

    assert control_plane is not None
    assert control_plane._connection_factory() is not None
    assert captured["dsn"] == "postgresql://operator:secret@example.test/control"
    assert captured["kwargs"] == {
        "connect_timeout": 5,
        "options": (
            "-csearch_path=pg_catalog,public -cstatement_timeout=5000ms -clock_timeout=1000ms"
        ),
    }
    bootstrap_query, bootstrap_params = cast(tuple[str, tuple[str, ...]], captured["bootstrap"])
    assert "set_config('search_path'" in bootstrap_query
    assert bootstrap_params == ("pg_catalog,public", "5000ms", "1000ms")


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
    captured: dict[str, object] = {}
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
        structure_high_water,
        quote_high_water,
        include_structure_range,
        include_quote_batch,
        crash_after_r2_upload,
        retry_fault_before_receipt,
        acceptance_run_id,
    ):
        captured.update(
            max_turns=max_turns,
            structure_materializer_turns=structure_materializer_turns,
            structure_range_turns=structure_range_turns,
            structure_high_water=structure_high_water,
            quote_high_water=quote_high_water,
            include_structure_range=include_structure_range,
            include_quote_batch=include_quote_batch,
            retry_fault_before_receipt=retry_fault_before_receipt,
            acceptance_run_id=acceptance_run_id,
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
                "--fault-retry-job-key",
                "structure:one:range:0",
                "--fault-retry-attempts",
                "3",
                "--fault-injection-ack",
                "staging-retry-before-receipt",
                "--json",
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out) == {"status": "stopped", "ticks": 3}
    assert captured | {"retry_fault_before_receipt": None} == {
        "max_turns": 2,
        "structure_materializer_turns": 8,
        "structure_range_turns": 8,
        "structure_high_water": 1,
        "quote_high_water": 512,
        "include_structure_range": True,
        "include_quote_batch": True,
        "retry_fault_before_receipt": None,
        "acceptance_run_id": None,
    }
    assert callable(captured["retry_fault_before_receipt"])


def test_control_plane_serve_coordinator_excludes_dedicated_pool_workers(
    monkeypatch, capsys
) -> None:
    from polyarb import cli_control_plane

    scheduler = object()
    captured: dict[str, object] = {}
    monkeypatch.setattr(cli_control_plane, "_control_plane_from_env", lambda: object())

    def transactional_scheduler(_control_plane, **kwargs):
        captured.update(kwargs)
        return scheduler

    monkeypatch.setattr(cli_control_plane, "_transactional_scheduler", transactional_scheduler)

    async def run_service(actual_scheduler, **_kwargs):
        assert actual_scheduler is scheduler
        return {"status": "stopped", "ticks": 1}

    monkeypatch.setattr(cli_control_plane, "_run_scheduler_service", run_service)

    assert (
        cli_control_plane.main(["serve", "--enable", "--worker-role", "coordinator", "--json"]) == 0
    )
    assert json.loads(capsys.readouterr().out) == {"status": "stopped", "ticks": 1}
    assert captured["include_structure_range"] is False
    assert captured["include_quote_batch"] is False


def test_quote_role_derives_lane_count_from_the_clob_concurrency_bound(
    monkeypatch, capsys
) -> None:
    from polyarb import cli_control_plane

    scheduler = object()
    captured: dict[str, object] = {}
    batch_pool = object()

    monkeypatch.setattr(cli_control_plane, "_control_plane_from_env", lambda: object())
    monkeypatch.setattr(
        cli_control_plane,
        "Settings",
        lambda: type("Settings", (), {"clob_batch_max_concurrency": 7})(),
    )

    def quote_workers(_control_plane, **kwargs):
        captured.update(kwargs)
        return batch_pool, object()

    monkeypatch.setattr(cli_control_plane, "_transactional_quote_workers", quote_workers)
    monkeypatch.setattr(
        cli_control_plane,
        "TransactionalWorkerLoop",
        lambda **kwargs: captured.update(loop=kwargs) or scheduler,
    )

    async def run_service(actual_scheduler, **_kwargs):
        assert actual_scheduler is scheduler
        return {"status": "stopped", "ticks": 1}

    monkeypatch.setattr(cli_control_plane, "_run_scheduler_service", run_service)

    assert (
        cli_control_plane.main(
            ["serve", "--enable", "--worker-role", "quote-batch", "--json"]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out) == {"status": "stopped", "ticks": 1}
    assert captured["lane_count"] == 7
    assert captured["loop"] == {
        "worker_name": "quote-batch",
        "worker": batch_pool,
        "turns_per_tick": 1,
    }


def test_structure_role_derives_lane_count_from_the_structure_concurrency_bound(
    monkeypatch, capsys
) -> None:
    from polyarb import cli_control_plane

    scheduler = object()
    captured: dict[str, object] = {}
    range_pool = object()

    monkeypatch.setattr(cli_control_plane, "_control_plane_from_env", lambda: object())
    monkeypatch.setattr(
        cli_control_plane,
        "Settings",
        lambda: type("Settings", (), {"structure_range_max_concurrency": 7})(),
    )

    def structure_worker(_control_plane, **kwargs):
        captured.update(kwargs)
        return range_pool

    monkeypatch.setattr(cli_control_plane, "_transactional_structure_worker", structure_worker)
    monkeypatch.setattr(
        cli_control_plane,
        "TransactionalWorkerLoop",
        lambda **kwargs: captured.update(loop=kwargs) or scheduler,
    )

    async def run_service(actual_scheduler, **_kwargs):
        assert actual_scheduler is scheduler
        return {"status": "stopped", "ticks": 1}

    monkeypatch.setattr(cli_control_plane, "_run_scheduler_service", run_service)

    assert (
        cli_control_plane.main(
            ["serve", "--enable", "--worker-role", "structure-range", "--json"]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out) == {"status": "stopped", "ticks": 1}
    assert captured["lane_count"] == 7
    assert captured["loop"] == {
        "worker_name": "structure-range",
        "worker": range_pool,
        "turns_per_tick": 1,
    }


def test_r2_upload_fault_callback_requires_exact_acknowledgement_and_job_key() -> None:
    from polyarb import cli_control_plane
    from polyarb.control_plane.models import JobLease

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
    callback(cast(JobLease, type("Lease", (), {"job_key": "structure:other:range:0"})()))
    with pytest.raises(KeyboardInterrupt, match="verified R2 upload"):
        callback(cast(JobLease, type("Lease", (), {"job_key": "structure:one:range:0"})()))


def test_retry_fault_callback_requires_bounded_exact_staging_configuration() -> None:
    from polyarb import cli_control_plane
    from polyarb.control_plane.models import JobLease

    with pytest.raises(ValueError, match="exact staging acknowledgement"):
        cli_control_plane._retry_fault_callback(
            target_job_key="structure:one:range:0", attempts=3, acknowledgement=None
        )
    with pytest.raises(ValueError, match="attempts must be positive"):
        cli_control_plane._retry_fault_callback(
            target_job_key="structure:one:range:0",
            attempts=0,
            acknowledgement="staging-retry-before-receipt",
        )

    callback = cli_control_plane._retry_fault_callback(
        target_job_key="structure:one:range:0",
        attempts=2,
        acknowledgement="staging-retry-before-receipt",
    )
    assert callback is not None
    callback(cast(JobLease, type("Lease", (), {"job_key": "structure:other:range:0"})()))
    with pytest.raises(RuntimeError, match="intentional staging retry"):
        callback(cast(JobLease, type("Lease", (), {"job_key": "structure:one:range:0"})()))
    with pytest.raises(RuntimeError, match="intentional staging retry"):
        callback(cast(JobLease, type("Lease", (), {"job_key": "structure:one:range:0"})()))
    callback(cast(JobLease, type("Lease", (), {"job_key": "structure:one:range:0"})()))


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
        retry_fault_before_receipt,
        acceptance_run_id,
    ):
        return Scheduler()

    monkeypatch.setattr(cli_control_plane, "_transactional_scheduler", transactional_scheduler)

    assert cli_control_plane.main(["tick-once", "--enable", "--max-turns", "2", "--json"]) == 0
    assert json.loads(capsys.readouterr().out) == {
        "status": "ok",
        "turns": [{"outcome": "idle", "worker": "structure-range"}],
    }


def test_alert_serve_forwards_acceptance_run_scope(monkeypatch, capsys) -> None:
    from polyarb import cli_control_plane

    captured: dict[str, object] = {}
    monkeypatch.setenv(
        "POLYARB_SUPABASE_DB_DSN", "postgresql://operator:secret@example.test/control"
    )
    monkeypatch.setattr(cli_control_plane, "_control_plane_from_env", lambda: object())

    class AlertWorker:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)

    monkeypatch.setattr(cli_control_plane, "TransactionalAlertDeliveryWorker", AlertWorker)

    async def run_alert_service(worker, *, interval_seconds: float, as_json: bool):
        assert isinstance(worker, AlertWorker)
        assert interval_seconds == 7.5
        assert as_json is True
        return {"status": "stopped", "turns": 1}

    monkeypatch.setattr(cli_control_plane, "_run_alert_service", run_alert_service)
    assert (
        cli_control_plane.main(
            [
                "alert-serve",
                "--enable",
                "--interval-seconds",
                "7.5",
                "--acceptance-run-id",
                "run-a",
                "--json",
            ]
        )
        == 0
    )
    assert captured["acceptance_run_id"] == "run-a"


def test_alert_serve_leaves_acceptance_scope_unset_for_production_claims(
    monkeypatch, capsys
) -> None:
    from polyarb import cli_control_plane

    captured: dict[str, object] = {}
    monkeypatch.setenv(
        "POLYARB_SUPABASE_DB_DSN", "postgresql://operator:secret@example.test/control"
    )
    monkeypatch.setattr(cli_control_plane, "_control_plane_from_env", lambda: object())

    class AlertWorker:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)

    monkeypatch.setattr(cli_control_plane, "TransactionalAlertDeliveryWorker", AlertWorker)

    async def run_alert_service(worker, *, interval_seconds: float, as_json: bool):
        assert isinstance(worker, AlertWorker)
        assert interval_seconds == 7.5
        assert as_json is True
        return {"status": "stopped", "turns": 1}

    monkeypatch.setattr(cli_control_plane, "_run_alert_service", run_alert_service)
    assert (
        cli_control_plane.main(["alert-serve", "--enable", "--interval-seconds", "7.5", "--json"])
        == 0
    )
    assert captured["acceptance_run_id"] is None


def test_watchdog_requires_explicit_enable_before_any_database_connect(monkeypatch, capsys) -> None:
    from polyarb import cli_control_plane

    monkeypatch.setattr(
        cli_control_plane,
        "_control_plane_from_env",
        lambda: (_ for _ in ()).throw(AssertionError("must not connect")),
    )

    assert (
        cli_control_plane.main(
            [
                "watchdog-serve",
                "--control-api-url",
                "https://control.example/perception/control-plane",
                "--fly-app",
                "polyarb-control-worker",
                "--machine-id",
                "machine-a",
            ]
        )
        == 2
    )
    assert "--enable is required" in capsys.readouterr().err


def test_watchdog_observes_a_secondary_app_with_qualified_machine_identity(monkeypatch) -> None:
    """An independent sampler is a first-class monitored runtime node."""
    from polyarb import cli_control_plane

    parser = cli_control_plane._parser()
    args = parser.parse_args(
        [
            "watchdog-serve",
            "--enable",
            "--control-api-url",
            "https://control.example/perception/control-plane",
            "--fly-app",
            "polyarb-control-worker-m1",
            "--machine-id",
            "worker-a",
            "--secondary-fly-app",
            "polyarb-control-evidence",
            "--secondary-machine-id",
            "sampler-a",
        ]
    )
    monkeypatch.setattr(
        cli_control_plane,
        "_read_soak_control_snapshot",
        lambda _url: {"status": "available"},
    )
    calls: list[tuple[str, tuple[str, ...]]] = []

    def read_states(machine_ids, *, app: str, token: str):
        assert token == "status-token"
        calls.append((app, tuple(machine_ids)))
        return {machine_id: "started" for machine_id in machine_ids}

    monkeypatch.setattr(cli_control_plane, "_read_cloud_fly_machine_states", read_states)
    monkeypatch.setattr(
        cli_control_plane,
        "_read_cloud_fly_machine_restart_counts",
        lambda machine_ids, **_kwargs: {machine_id: 0 for machine_id in machine_ids},
    )
    monkeypatch.setenv("POLYARB_FLY_API_TOKEN", "status-token")

    observation = cli_control_plane._read_runtime_watchdog_observation(args)

    assert observation.healthy is True
    assert calls == [
        ("polyarb-control-worker-m1", ("worker-a",)),
        ("polyarb-control-evidence", ("sampler-a",)),
    ]


def test_runtime_controller_status_is_read_only_and_bounded(monkeypatch, capsys) -> None:
    """The dashboard path must not claim a lease or invoke an action worker."""
    from polyarb import cli_control_plane

    calls: list[str] = []

    class ControlPlane:
        _connection_factory = object()

    monkeypatch.delenv("POLYARB_DB_EXPECTED_DATABASE", raising=False)

    def status(factory, *, controller_id, now, sample_limit):
        assert factory is ControlPlane._connection_factory
        assert controller_id == "controller-a"
        assert sample_limit == 2
        calls.append("read")
        return {
            "read_at": now.isoformat(),
            "controller": {
                "controller_id": controller_id,
                "owner_id": "owner-a",
                "lease_epoch": 4,
                "lease_active": True,
            },
            "active_runtime_incidents": [],
            "recovery_budget": [],
            "actions": {"pending": [], "running": [], "recent_completed": []},
            "last_outcome": None,
            "next_check_at": None,
        }

    monkeypatch.setattr(cli_control_plane, "_control_plane_from_env", lambda: ControlPlane())
    monkeypatch.setattr(cli_control_plane, "read_runtime_controller_status", status)
    monkeypatch.setattr(
        cli_control_plane,
        "claim_controller",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("status must not claim")),
    )
    monkeypatch.setattr(
        cli_control_plane,
        "schedule_action",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("status must not schedule")),
    )

    assert (
        cli_control_plane.main(
            [
                "runtime-controller-status",
                "--controller-id",
                "controller-a",
                "--limit",
                "2",
                "--json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "available"
    assert payload["controller"]["lease_epoch"] == 4
    assert calls == ["read"]


def test_runtime_observe_verify_derives_exact_current_identity_and_never_mutates(
    monkeypatch, capsys
) -> None:
    from datetime import UTC, datetime

    from polyarb import cli_control_plane
    from polyarb.control_plane.runtime_observe import RuntimeObserveVerification

    now = datetime(2026, 8, 25, 12, 30, tzinfo=UTC)

    class ControlPlane:
        _connection_factory = object()

    captured: dict[str, object] = {}
    monkeypatch.delenv("POLYARB_DB_EXPECTED_DATABASE", raising=False)
    monkeypatch.setenv("POLYARB_RUNTIME_ROLE", "control-plane")
    monkeypatch.setenv("POLYARB_SUPABASE_DB_DSN", "postgresql://operator@example.test/control")
    monkeypatch.setattr(cli_control_plane, "_control_plane_from_env", lambda: ControlPlane())
    monkeypatch.setattr(
        cli_control_plane,
        "read_runtime_controller_status",
        lambda *_args, **_kwargs: {
            "controller": {
                "controller_id": "controller-a",
                "owner_id": "owner-a",
                "lease_epoch": 4,
                "lease_active": True,
            }
        },
    )

    def verify(factory, **kwargs):
        assert factory is ControlPlane._connection_factory
        captured.update(kwargs)
        return RuntimeObserveVerification(
            status="pass",
            controller_id="controller-a",
            controller_owner_id="owner-a",
            controller_epoch=4,
            started_at=now - timedelta(minutes=30),
            latest_observed_at=now,
            duration_seconds=1800,
            decision_count=61,
            idle_count=3,
            recovery_action_count=0,
            current_candidate_count=2,
            max_gap_seconds=30,
            latest_decision_digest="a" * 64,
        )

    monkeypatch.setattr(
        cli_control_plane,
        "verify_runtime_observe_window",
        verify,
        raising=False,
    )
    monkeypatch.setattr(
        cli_control_plane,
        "claim_controller",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("observe verifier must not claim")
        ),
    )
    monkeypatch.setattr(
        cli_control_plane,
        "schedule_action",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("observe verifier must not schedule")
        ),
    )
    monkeypatch.setattr(
        cli_control_plane,
        "datetime",
        type("Clock", (), {"now": staticmethod(lambda *_args: now)}),
    )

    assert (
        cli_control_plane.main(
            [
                "runtime-observe-verify",
                "--controller-id",
                "controller-a",
                "--minimum-seconds",
                "1800",
                "--max-freshness-seconds",
                "90",
                "--max-gap-seconds",
                "90",
                "--json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "pass"
    assert payload["recovery_action_count"] == 0
    assert captured["controller_owner_id"] == "owner-a"
    assert captured["controller_epoch"] == 4


def test_runtime_observe_verify_reports_safe_actionable_gate_reason(monkeypatch, capsys) -> None:
    from polyarb import cli_control_plane
    from polyarb.control_plane.runtime_observe import RuntimeObserveVerificationError

    class ControlPlane:
        _connection_factory = object()

    monkeypatch.setenv(
        "POLYARB_SUPABASE_DB_DSN",
        "postgresql://operator:secret@example.test/control",
    )
    monkeypatch.setattr(cli_control_plane, "_control_plane_from_env", lambda: ControlPlane())
    monkeypatch.setattr(
        cli_control_plane,
        "read_runtime_controller_status",
        lambda *_args, **_kwargs: {
            "controller": {
                "controller_id": "controller-a",
                "owner_id": "owner-a",
                "lease_epoch": 4,
                "lease_active": True,
            }
        },
    )
    monkeypatch.setattr(
        cli_control_plane,
        "verify_runtime_observe_window",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeObserveVerificationError(
                "observe-only window lacks boundary anchor "
                "(available_seconds=101, required_seconds=120)"
            )
        ),
    )

    assert cli_control_plane.main(["runtime-observe-verify", "--json"]) == 1
    error = capsys.readouterr().err
    assert "available_seconds=101" in error
    assert "required_seconds=120" in error
    assert "postgresql://" not in error
    assert "secret" not in error


def test_runtime_reconcile_once_requires_enable_before_database_or_controller(
    monkeypatch, capsys
) -> None:
    from polyarb import cli_control_plane

    monkeypatch.setenv(
        "POLYARB_SUPABASE_DB_DSN", "postgresql://operator:secret@example.test/control"
    )
    monkeypatch.setattr(
        cli_control_plane,
        "_control_plane_from_env",
        lambda: (_ for _ in ()).throw(AssertionError("mutation guard must run first")),
    )
    assert cli_control_plane.main(["runtime-reconcile-once", "--json"]) == 2
    assert "--enable is required" in capsys.readouterr().err


def test_runtime_reconcile_once_verifies_database_role_before_claim(monkeypatch, capsys) -> None:
    from polyarb import cli_control_plane

    events: list[str] = []

    class ControlPlane:
        _connection_factory = object()

    def verify(factory, profile, *, expected_database):
        assert factory is ControlPlane._connection_factory
        assert profile == "runtime-controller"
        assert expected_database == "role_test"
        events.append("verify")

    def claim(*_args, **_kwargs):
        events.append("claim")
        raise RuntimeError("stop-after-claim")

    monkeypatch.setenv("POLYARB_SUPABASE_DB_DSN", "postgresql://runtime:secret@example/control")
    monkeypatch.setenv("POLYARB_DB_EXPECTED_DATABASE", "role_test")
    monkeypatch.setattr(cli_control_plane, "_control_plane_from_env", lambda: ControlPlane())
    monkeypatch.setattr(cli_control_plane, "verify_daemon_database_role", verify, raising=False)
    monkeypatch.setattr(cli_control_plane, "claim_controller", claim)

    assert cli_control_plane.main(["runtime-reconcile-once", "--enable", "--json"]) == 1
    assert events == ["verify", "claim"]
    assert "postgresql://" not in capsys.readouterr().err


def test_runtime_reconcile_serve_verifies_database_role_before_claim(monkeypatch, capsys) -> None:
    from polyarb import cli_control_plane

    events: list[str] = []

    class ControlPlane:
        _connection_factory = object()

    def verify(factory, profile, *, expected_database):
        assert factory is ControlPlane._connection_factory
        assert profile == "runtime-controller"
        assert expected_database == "role_test"
        events.append("verify")

    def claim(*_args, **_kwargs):
        events.append("claim")
        raise RuntimeError("stop-after-claim")

    monkeypatch.setenv("POLYARB_SUPABASE_DB_DSN", "postgresql://runtime:secret@example/control")
    monkeypatch.setenv("POLYARB_DB_EXPECTED_DATABASE", "role_test")
    monkeypatch.setattr(cli_control_plane, "_control_plane_from_env", lambda: ControlPlane())
    monkeypatch.setattr(cli_control_plane, "verify_daemon_database_role", verify, raising=False)
    monkeypatch.setattr(cli_control_plane, "claim_controller", claim)

    assert (
        cli_control_plane.main(
            ["runtime-reconcile-serve", "--enable", "--interval-seconds", "0.001", "--json"]
        )
        == 1
    )
    assert events == ["verify", "claim"]
    assert "postgresql://" not in capsys.readouterr().err


def test_runtime_reconcile_role_contract_failure_stops_before_mutation_and_sanitizes(
    monkeypatch, capsys
) -> None:
    from polyarb import cli_control_plane
    from polyarb.control_plane.db_role_contract import DatabaseRoleContractError

    class ControlPlane:
        _connection_factory = object()

    def fail_contract(*_args, **_kwargs):
        raise DatabaseRoleContractError(
            "database-role.capability-missing",
            "m1_runtime_controller_capability",
        )

    monkeypatch.setenv("POLYARB_SUPABASE_DB_DSN", "postgresql://runtime:secret@example/control")
    monkeypatch.setenv("POLYARB_DB_EXPECTED_DATABASE", "role_test")
    monkeypatch.setattr(cli_control_plane, "_control_plane_from_env", lambda: ControlPlane())
    monkeypatch.setattr(cli_control_plane, "verify_daemon_database_role", fail_contract)
    monkeypatch.setattr(
        cli_control_plane,
        "claim_controller",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not claim")),
    )
    monkeypatch.setattr(
        cli_control_plane,
        "insert_runtime_observe_decisions",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not observe")),
    )

    assert cli_control_plane.main(["runtime-reconcile-once", "--enable", "--json"]) == 1
    captured = capsys.readouterr()
    assert "database-role.capability-missing" in captured.err
    assert "m1_runtime_controller_capability" in captured.err
    assert "postgresql://" not in captured.err
    assert "secret" not in captured.err


def test_runtime_reconcile_once_evaluates_schedules_and_executes_one_action(
    monkeypatch, capsys
) -> None:
    from datetime import UTC, datetime

    from polyarb import cli_control_plane
    from polyarb.control_plane.recovery_models import (
        RecoveryActionType,
        RecoveryBudget,
        RecoveryDecision,
        RecoveryRuntimeState,
    )
    from polyarb.control_plane.recovery_records import RecoveryActionRecord, RuntimeControllerLease
    from polyarb.control_plane.runtime_models import RuntimeDeadlineProfile

    now = datetime(2026, 8, 25, 12, 0, tzinfo=UTC)
    controller = RuntimeControllerLease("controller-a", "owner-a", 1, now + timedelta(seconds=90))
    state = RecoveryRuntimeState(
        job_key="job-a",
        attempt_id="attempt-a",
        lease_epoch=2,
        owner_is_current=True,
        profile=RuntimeDeadlineProfile("test", 30, 10, 20, 30),
        attempt_started_at=now - timedelta(seconds=25),
        last_heartbeat_at=now - timedelta(seconds=5),
        last_progress_at=now - timedelta(seconds=25),
        lease_expires_at=now + timedelta(seconds=5),
        retry_count=0,
        recovery_budget=RecoveryBudget(2),
        recovery_episode_key="attempt-a",
    )
    from polyarb.control_plane.recovery_store import RuntimeReconcileCandidate

    candidate = RuntimeReconcileCandidate(
        runtime_state=state,
        job_type="structure-normalize",
        job_state="leased",
        worker_id="worker-a",
        target_type="job",
        target_id="job-a",
        component="structure-normalize",
        incident_key="recovery:job:job-a",
        channels=("dashboard", "telegram"),
        cooldown_seconds=15,
    )
    action = RecoveryActionRecord(
        action_id="action-a",
        controller_id="controller-a",
        controller_owner_id="owner-a",
        incident_key="recovery:job:job-a",
        target_type="job",
        target_id="job-a",
        action_type="heartbeat-job",
        expected_controller_epoch=1,
        expected_attempt_id="attempt-a",
        expected_lease_epoch=2,
        requested_at=now,
        started_at=now,
        finished_at=now,
        state="completed",
        result_code="succeeded",
        next_allowed_at=now + timedelta(seconds=15),
        worker_id="runtime-recovery-executor",
        worker_epoch=1,
        worker_lease_expires_at=now + timedelta(seconds=30),
        detail={"reason_code": "job.lease-at-risk", "component": "structure-normalize"},
        idempotency_key="recovery-action:a",
    )

    class ControlPlane:
        _connection_factory = object()

    class Executor:
        def __init__(self, **kwargs):
            assert kwargs["controller"] == controller
            assert kwargs["worker_id"] == "executor-a"

        def run_once(self, *, now, expected_action_id):
            assert expected_action_id == "action-a"
            return cli_control_plane.RecoveryActionResult(
                action_id="action-a",
                action_type="heartbeat-job",
                target_id="job-a",
                outcome="succeeded",
            )

    monkeypatch.setenv("POLYARB_RUNTIME_ROLE", "control-plane")
    monkeypatch.setenv("POLYARB_RUNTIME_RECOVERY_MODE", "execute")
    monkeypatch.setattr(cli_control_plane, "_control_plane_from_env", lambda: ControlPlane())
    monkeypatch.setattr(cli_control_plane, "claim_controller", lambda *a, **k: controller)
    def read_exact_target(*_args, **kwargs):
        assert kwargs["target_id"] == "job-a"
        return (candidate,)

    monkeypatch.setattr(cli_control_plane, "read_runtime_reconcile_states", read_exact_target)
    decision = RecoveryDecision(
        action=RecoveryActionType.HEARTBEAT_JOB,
        reason_code="job.lease-at-risk",
        incident_severity="warning",
        qualification_breaking=False,
        next_check_at=now,
    )
    monkeypatch.setattr(cli_control_plane.RuntimeReconciler, "evaluate", lambda *a, **k: decision)
    monkeypatch.setattr(cli_control_plane, "schedule_action", lambda *a, **k: action)
    monkeypatch.setattr(cli_control_plane, "RecoveryExecutor", Executor)
    clock = type("Clock", (), {"now": staticmethod(lambda *_: now)})
    monkeypatch.setattr(cli_control_plane, "datetime", clock)

    assert (
        cli_control_plane.main(
            [
                "runtime-reconcile-once",
                "--enable",
                "--worker-id",
                "executor-a",
                "--target-type",
                "job",
                "--target-id",
                "job-a",
                "--expected-action",
                "heartbeat-job",
                "--json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["state"] == "recovery-executed"
    assert payload["reason"] == "job.lease-at-risk"
    assert payload["action"] == "heartbeat-job"
    assert payload["outcome"] == "succeeded"
    assert payload["pointer_mutations"] == 0


def test_runtime_reconcile_once_observe_only_records_every_candidate_without_recovery_mutation(
    monkeypatch, capsys
) -> None:
    from dataclasses import replace
    from datetime import UTC, datetime

    from polyarb import cli_control_plane
    from polyarb.control_plane.recovery_models import RecoveryBudget, RecoveryRuntimeState
    from polyarb.control_plane.recovery_records import RuntimeControllerLease
    from polyarb.control_plane.recovery_store import RuntimeReconcileCandidate
    from polyarb.control_plane.runtime_models import RuntimeDeadlineProfile
    from polyarb.control_plane.runtime_observe import RuntimeObserveDecisionRecord

    now = datetime(2026, 8, 25, 12, 0, tzinfo=UTC)
    controller = RuntimeControllerLease("controller-a", "owner-a", 1, now + timedelta(seconds=90))
    state = RecoveryRuntimeState(
        job_key="job-a",
        attempt_id="attempt-a",
        lease_epoch=2,
        owner_is_current=True,
        profile=RuntimeDeadlineProfile("test", 30, 10, 20, 60),
        attempt_started_at=now - timedelta(seconds=25),
        last_heartbeat_at=now - timedelta(seconds=5),
        last_progress_at=now - timedelta(seconds=25),
        lease_expires_at=now + timedelta(seconds=5),
        retry_count=0,
        recovery_budget=RecoveryBudget(2),
        recovery_episode_key="attempt-a",
    )
    candidate_a = RuntimeReconcileCandidate(
        runtime_state=state,
        job_type="structure-normalize",
        job_state="leased",
        worker_id="worker-a",
        target_type="job",
        target_id="job-a",
        component="structure-normalize",
        incident_key="recovery:job:job-a",
        channels=("dashboard", "telegram"),
        cooldown_seconds=15,
    )
    candidate_b = replace(
        candidate_a,
        runtime_state=replace(
            state,
            job_key="job-b",
            attempt_id="attempt-b",
            recovery_episode_key="attempt-b",
        ),
        target_id="job-b",
        incident_key="recovery:job:job-b",
    )

    class ControlPlane:
        _connection_factory = object()

    recorded: list[RuntimeObserveDecisionRecord] = []
    batch_calls = 0
    monkeypatch.setenv("POLYARB_RUNTIME_ROLE", "control-plane")
    monkeypatch.setenv("POLYARB_RUNTIME_RECOVERY_MODE", "observe-only")
    monkeypatch.setattr(cli_control_plane, "_control_plane_from_env", lambda: ControlPlane())
    monkeypatch.setattr(cli_control_plane, "claim_controller", lambda *a, **k: controller)
    monkeypatch.setattr(
        cli_control_plane,
        "read_runtime_reconcile_states",
        lambda *a, **k: (candidate_a, candidate_b),
    )

    def record_batch(_factory, records, **_kwargs):
        nonlocal batch_calls
        batch_calls += 1
        recorded.extend(records)
        return tuple(records)

    monkeypatch.setattr(cli_control_plane, "insert_runtime_observe_decisions", record_batch)
    monkeypatch.setattr(
        cli_control_plane,
        "schedule_action",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("observe-only must not schedule")),
    )
    monkeypatch.setattr(
        cli_control_plane,
        "RecoveryExecutor",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("observe-only must not construct an executor")
        ),
    )
    monkeypatch.setattr(
        cli_control_plane,
        "datetime",
        type("Clock", (), {"now": staticmethod(lambda *_args: now)}),
    )

    assert cli_control_plane.main(["runtime-reconcile-once", "--enable", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["state"] == "observe-only"
    assert payload["outcome"] == "no-mutation"
    assert payload["observed_decision_count"] == 2
    assert batch_calls == 1
    assert {record.target_id for record in recorded} == {"job-a", "job-b"}


def test_runtime_reconcile_once_observe_only_records_idle_without_executor(
    monkeypatch, capsys
) -> None:
    from datetime import UTC, datetime

    from polyarb import cli_control_plane
    from polyarb.control_plane.recovery_records import RuntimeControllerLease
    from polyarb.control_plane.runtime_observe import RuntimeObserveDecisionRecord

    now = datetime(2026, 8, 25, 12, 0, tzinfo=UTC)
    controller = RuntimeControllerLease("controller-a", "owner-a", 3, now + timedelta(seconds=90))

    class ControlPlane:
        _connection_factory = object()

    recorded: list[RuntimeObserveDecisionRecord] = []
    monkeypatch.setenv("POLYARB_RUNTIME_ROLE", "control-plane")
    monkeypatch.setenv("POLYARB_RUNTIME_RECOVERY_MODE", "observe-only")
    monkeypatch.setattr(cli_control_plane, "_control_plane_from_env", lambda: ControlPlane())
    monkeypatch.setattr(cli_control_plane, "claim_controller", lambda *a, **k: controller)
    monkeypatch.setattr(cli_control_plane, "read_runtime_reconcile_states", lambda *a, **k: ())
    monkeypatch.setattr(
        cli_control_plane,
        "insert_runtime_observe_decisions",
        lambda _factory, records, **_kwargs: recorded.extend(records) or tuple(records),
    )
    monkeypatch.setattr(
        cli_control_plane,
        "RecoveryExecutor",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("observe-only idle must not construct an executor")
        ),
    )
    monkeypatch.setattr(
        cli_control_plane,
        "datetime",
        type("Clock", (), {"now": staticmethod(lambda *_args: now)}),
    )

    assert cli_control_plane.main(["runtime-reconcile-once", "--enable", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["state"] == "observe-only"
    assert payload["observed_decision_count"] == 1
    assert len(recorded) == 1
    assert recorded[0].decision_kind == "idle"


def _install_runtime_reconcile_conflict(monkeypatch, message: str) -> None:
    """Make one runtime turn reach the store conflict boundary without a DB."""
    from datetime import UTC, datetime

    from polyarb import cli_control_plane
    from polyarb.control_plane.recovery_models import (
        RecoveryActionType,
        RecoveryBudget,
        RecoveryDecision,
        RecoveryRuntimeState,
    )
    from polyarb.control_plane.recovery_records import RuntimeControllerLease
    from polyarb.control_plane.recovery_store import (
        RecoveryActionConflict,
        RuntimeReconcileCandidate,
    )
    from polyarb.control_plane.runtime_models import RuntimeDeadlineProfile

    now = datetime(2026, 8, 25, 12, 0, tzinfo=UTC)
    controller = RuntimeControllerLease("controller-a", "owner-a", 1, now + timedelta(seconds=90))
    candidate = RuntimeReconcileCandidate(
        runtime_state=RecoveryRuntimeState(
            job_key="job-a",
            attempt_id="attempt-a",
            lease_epoch=2,
            owner_is_current=True,
            profile=RuntimeDeadlineProfile("test", 30, 10, 20, 30),
            attempt_started_at=now - timedelta(seconds=25),
            last_heartbeat_at=now - timedelta(seconds=5),
            last_progress_at=now - timedelta(seconds=25),
            lease_expires_at=now + timedelta(seconds=5),
            retry_count=0,
            recovery_budget=RecoveryBudget(2),
            recovery_episode_key="attempt-a",
        ),
        job_type="structure-normalize",
        job_state="leased",
        worker_id="worker-a",
        target_type="job",
        target_id="job-a",
        component="structure-normalize",
        incident_key="recovery:job:job-a",
        channels=("dashboard",),
        cooldown_seconds=15,
    )
    decision = RecoveryDecision(
        action=RecoveryActionType.RECLAIM_JOB,
        reason_code="job.lease-expired",
        incident_severity="critical",
        qualification_breaking=True,
        next_check_at=now,
    )

    class ControlPlane:
        _connection_factory = object()

    class Executor:
        def __init__(self, **_kwargs):
            return None

        def run_once(self, *, now):
            return None

    monkeypatch.setenv("POLYARB_RUNTIME_ROLE", "control-plane")
    monkeypatch.setenv("POLYARB_RUNTIME_RECOVERY_MODE", "execute")
    monkeypatch.setattr(cli_control_plane, "_control_plane_from_env", lambda: ControlPlane())
    monkeypatch.setattr(cli_control_plane, "claim_controller", lambda *a, **k: controller)
    monkeypatch.setattr(
        cli_control_plane,
        "read_runtime_reconcile_states",
        lambda *a, **k: (candidate,),
    )
    monkeypatch.setattr(cli_control_plane.RuntimeReconciler, "evaluate", lambda *a, **k: decision)
    monkeypatch.setattr(cli_control_plane, "RecoveryExecutor", Executor)

    def conflict(*_args, **_kwargs):
        raise RecoveryActionConflict(message)

    monkeypatch.setattr(cli_control_plane, "schedule_action", conflict)


def test_runtime_reconcile_once_exact_selector_fails_before_wrong_action(
    monkeypatch, capsys
) -> None:
    from polyarb import cli_control_plane

    _install_runtime_reconcile_conflict(monkeypatch, "must not schedule a mismatched action")

    assert (
        cli_control_plane.main(
            [
                "runtime-reconcile-once",
                "--enable",
                "--target-type",
                "job",
                "--target-id",
                "job-a",
                "--expected-action",
                "probe-circuit",
                "--json",
            ]
        )
        == 1
    )
    captured = capsys.readouterr()
    assert "requested recovery action does not match current facts" in captured.err
    assert "must not schedule" not in captured.err


@pytest.mark.parametrize(
    "message",
    (
        "recovery budget changed during scheduling",
        "recovery action idempotency conflicts",
        "runtime recovery state changed during scheduling",
        "incident identity conflicts",
    ),
)
def test_runtime_reconcile_once_store_conflicts_fail_loud(
    monkeypatch, capsys, message: str
) -> None:
    from polyarb import cli_control_plane

    _install_runtime_reconcile_conflict(monkeypatch, message)

    assert (
        cli_control_plane.main(
            ["runtime-reconcile-once", "--enable", "--worker-id", "executor-a", "--json"]
        )
        == 1
    )
    captured = capsys.readouterr()
    assert message in captured.err
    assert '"status": "ok"' not in captured.out


@pytest.mark.parametrize(
    "message",
    (
        "recovery budget changed during scheduling",
        "recovery action idempotency conflicts",
        "runtime recovery state changed during scheduling",
        "incident identity conflicts",
    ),
)
def test_runtime_reconcile_serve_store_conflicts_exit_current_turn(
    monkeypatch, capsys, message: str
) -> None:
    from polyarb import cli_control_plane

    _install_runtime_reconcile_conflict(monkeypatch, message)

    assert (
        cli_control_plane.main(
            [
                "runtime-reconcile-serve",
                "--enable",
                "--interval-seconds",
                "0.001",
                "--worker-id",
                "executor-a",
                "--json",
            ]
        )
        == 1
    )
    captured = capsys.readouterr()
    assert message in captured.err
    assert '"status": "ok"' not in captured.out


def test_runtime_reconcile_serve_stops_cleanly_on_signal_and_is_sequential(monkeypatch) -> None:
    """The loop owns no deployment or process authority and exits on its stop event."""
    import asyncio
    from collections.abc import Callable
    from datetime import UTC, datetime
    from typing import Any, cast

    import psycopg

    from polyarb import cli_control_plane
    from polyarb.control_plane.recovery_records import RecoveryActionRecord, RuntimeControllerLease

    args = cli_control_plane._parser().parse_args(
        ["runtime-reconcile-serve", "--enable", "--interval-seconds", "0.001"]
    )
    stop_after_first = {"count": 0}
    now = datetime(2026, 8, 25, 12, 0, tzinfo=UTC)
    controller = RuntimeControllerLease("controller-a", "owner-a", 1, now + timedelta(seconds=90))

    class ControlPlane:
        def __init__(self) -> None:
            self._connection_factory = cast(Callable[[], psycopg.Connection[Any]], lambda: None)

        def _execute_recovery_action_cursor(
            self,
            cursor: Any,
            action: RecoveryActionRecord,
            *,
            now: datetime,
            heartbeat_lease_seconds: int,
        ) -> object:
            raise AssertionError("patched reconcile turn must not execute recovery")

    monkeypatch.setattr(cli_control_plane, "claim_controller", lambda *a, **k: controller)
    monkeypatch.setattr(
        cli_control_plane,
        "_runtime_reconcile_once",
        lambda *a, **k: {"status": "ok"},
    )
    stop = asyncio.Event()

    def stop_after_write(*_args, **_kwargs) -> None:
        stop_after_first["count"] += 1
        stop.set()

    monkeypatch.setattr(cli_control_plane, "_write", stop_after_write)
    result = asyncio.run(
        cli_control_plane._run_runtime_reconcile_service(ControlPlane(), args, stop_event=stop)
    )
    assert result == {"status": "stopped", "turns": 1}
    assert stop_after_first["count"] == 1


def test_runtime_reconcile_service_detaches_a_stalled_turn_at_database_grace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import asyncio
    import threading
    from datetime import UTC, datetime
    from time import monotonic

    from polyarb import cli_control_plane
    from polyarb.control_plane.recovery_records import RuntimeControllerLease

    args = cli_control_plane._parser().parse_args(
        ["runtime-reconcile-serve", "--enable", "--interval-seconds", "30"]
    )
    started = threading.Event()
    release = threading.Event()
    stop = asyncio.Event()
    now = datetime(2026, 8, 25, 12, 0, tzinfo=UTC)
    controller = RuntimeControllerLease("controller-a", "owner-a", 1, now + timedelta(seconds=90))

    class Policy:
        stop_grace_seconds = 0.05

    class ControlPlane:
        _connection_factory = object()

    def stalled_turn(*_args: object, **_kwargs: object) -> dict[str, object]:
        started.set()
        release.wait()
        return {"status": "late"}

    monkeypatch.setattr(cli_control_plane, "RECOVERY_DB_POLICY", Policy())
    monkeypatch.setattr(cli_control_plane, "claim_controller", lambda *a, **k: controller)
    monkeypatch.setattr(cli_control_plane, "_runtime_reconcile_once", stalled_turn)

    async def run() -> dict[str, object]:
        task = asyncio.create_task(
            cli_control_plane._run_runtime_reconcile_service(ControlPlane(), args, stop_event=stop)
        )
        while not started.is_set():
            await asyncio.sleep(0)
        stop.set()
        return await task

    safety_release = threading.Timer(0.4, release.set)
    safety_release.start()
    before = monotonic()
    try:
        assert asyncio.run(run()) == {"status": "stopped", "turns": 0}
        assert monotonic() - before < 0.2
    finally:
        release.set()
        safety_release.cancel()


def test_alert_service_detaches_a_stalled_turn_at_database_grace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import asyncio
    import threading
    from time import monotonic
    from types import SimpleNamespace

    from polyarb import cli_control_plane

    started = threading.Event()
    release = threading.Event()
    stop = asyncio.Event()

    class Policy:
        stop_grace_seconds = 0.05

    class Worker:
        async def run_once(self):
            started.set()
            release.wait()
            return SimpleNamespace(outbox_id="late", outcome="late")

    monkeypatch.setattr(cli_control_plane, "ALERT_DELIVERY_POLICY", Policy())

    async def run() -> dict[str, object]:
        task = asyncio.create_task(
            cli_control_plane._run_alert_service(
                Worker(), interval_seconds=30, as_json=True, stop_event=stop
            )
        )
        while not started.is_set():
            await asyncio.sleep(0)
        stop.set()
        return await task

    safety_release = threading.Timer(0.4, release.set)
    safety_release.start()
    before = monotonic()
    try:
        assert asyncio.run(run()) == {"status": "stopped", "turns": 0}
        assert monotonic() - before < 0.2
    finally:
        release.set()
        safety_release.cancel()


def test_watchdog_observation_applies_cloud_evidence_freshness_gate(monkeypatch) -> None:
    from polyarb import cli_control_plane
    from polyarb.control_plane.watchdog import SoakEvidenceGate

    args = cli_control_plane._parser().parse_args(
        [
            "watchdog-serve",
            "--enable",
            "--control-api-url",
            "https://control.example/perception/control-plane",
            "--fly-app",
            "polyarb-control-worker-m1",
            "--machine-id",
            "worker-a",
        ]
    )
    monkeypatch.setattr(
        cli_control_plane,
        "_read_soak_control_snapshot",
        lambda _url: {
            "status": "available",
            "job_counts": {"succeeded": 1, "runnable": 0, "leased": 0},
            "soak_evidence": {"latest_observed_at": "2026-08-18T14:00:00+00:00"},
        },
    )
    monkeypatch.setattr(
        cli_control_plane,
        "_read_cloud_fly_machine_states",
        lambda machine_ids, **_kwargs: {machine_id: "started" for machine_id in machine_ids},
    )
    monkeypatch.setattr(
        cli_control_plane,
        "_read_cloud_fly_machine_restart_counts",
        lambda machine_ids, **_kwargs: {machine_id: 0 for machine_id in machine_ids},
    )
    monkeypatch.setenv("POLYARB_FLY_API_TOKEN", "status-token")

    observation = cli_control_plane._read_runtime_watchdog_observation(
        args,
        soak_evidence_gate=SoakEvidenceGate(max_age=timedelta(minutes=15)),
    )

    assert observation.healthy is False
    assert observation.failures[0].startswith("evidence:sample-stale:")


def test_watchdog_observation_checks_additional_cross_app_targets(monkeypatch) -> None:
    from polyarb import cli_control_plane

    args = cli_control_plane._parser().parse_args(
        [
            "watchdog-serve",
            "--enable",
            "--control-api-url",
            "https://control.example/perception/control-plane",
            "--fly-app",
            "polyarb-control-worker-m1",
            "--machine-id",
            "worker-a",
            "--secondary-fly-app",
            "polyarb-control-evidence",
            "--secondary-machine-id",
            "sampler-a",
            "--secondary-target",
            "polyarb-control-runtime-event-writer/writer-a",
        ]
    )
    calls: list[tuple[str, tuple[str, ...]]] = []
    monkeypatch.setattr(
        cli_control_plane,
        "_read_soak_control_snapshot",
        lambda _url: {
            "status": "available",
            "job_counts": {"succeeded": 1, "runnable": 0, "leased": 0},
        },
    )

    def read_states(machine_ids, *, app, **_kwargs):
        calls.append((app, tuple(machine_ids)))
        return {machine_id: "started" for machine_id in machine_ids}

    monkeypatch.setattr(cli_control_plane, "_read_cloud_fly_machine_states", read_states)
    monkeypatch.setattr(
        cli_control_plane,
        "_read_cloud_fly_machine_restart_counts",
        lambda machine_ids, **_kwargs: {machine_id: 0 for machine_id in machine_ids},
    )
    monkeypatch.setenv("POLYARB_FLY_API_TOKEN", "status-token")

    observation = cli_control_plane._read_runtime_watchdog_observation(args)

    assert observation.healthy is True
    assert calls == [
        ("polyarb-control-worker-m1", ("worker-a",)),
        ("polyarb-control-evidence", ("sampler-a",)),
        ("polyarb-control-runtime-event-writer", ("writer-a",)),
    ]


def test_structure_source_factory_builds_eight_distinct_lease_lanes(monkeypatch) -> None:
    from polyarb import cli_control_plane
    from polyarb.control_plane.postgres import PostgresControlPlane

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
        cast(PostgresControlPlane, object()), worker_id="control:source"
    )

    assert isinstance(result, SourcePool)
    assert [lane["worker_id"] for lane in captured] == [
        f"control:source:{ordinal}" for ordinal in range(8)
    ]
    assert {id(lane["object_client"]) for lane in captured} == {id(object_client)}
    assert {lane["bucket"] for lane in captured} == {"source-bucket"}


def test_structure_source_factory_disables_hidden_gamma_retries(monkeypatch) -> None:
    from polyarb import cli_control_plane
    from polyarb.control_plane.postgres import PostgresControlPlane
    from polyarb.control_plane.runtime_deadlines import runtime_policy

    captured_settings = []

    class SourceWorker:
        def __init__(self, **_kwargs: object) -> None:
            pass

    class SourcePool:
        def __init__(self, *, lanes: tuple[SourceWorker, ...]) -> None:
            self.lanes = lanes

    def gamma_client(settings):
        captured_settings.append(settings)
        return object()

    monkeypatch.setattr(cli_control_plane, "TransactionalStructureSourceWorker", SourceWorker)
    monkeypatch.setattr(cli_control_plane, "TransactionalStructureSourcePool", SourcePool)
    monkeypatch.setattr(cli_control_plane, "GammaClient", gamma_client)
    monkeypatch.setattr(cli_control_plane, "_structure_object_client", lambda: (object(), "b"))

    cli_control_plane._transactional_structure_source_worker(
        cast(PostgresControlPlane, object()), worker_id="control:source", lane_count=2
    )

    assert len(captured_settings) == 2
    policy = runtime_policy("structure-fetch", 120)
    assert {settings.retry_attempts for settings in captured_settings} == {policy.provider_attempts}
    assert {settings.http_timeout_s for settings in captured_settings} == {
        policy.provider_timeout_seconds
    }


def test_quote_factory_disables_hidden_r2_retries(monkeypatch, settings_for_test_with_r2) -> None:
    from polyarb import cli_control_plane
    from polyarb.control_plane.postgres import PostgresControlPlane
    from polyarb.control_plane.runtime_deadlines import runtime_policy

    captured = {}

    class Worker:
        def __init__(self, **_kwargs) -> None:
            pass

    def build_client(*_args, **kwargs):
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(cli_control_plane, "Settings", lambda: settings_for_test_with_r2)
    monkeypatch.setattr(cli_control_plane, "_build_client", build_client)
    monkeypatch.setattr(cli_control_plane, "ClobReaderClient", lambda _settings: object())
    monkeypatch.setattr(cli_control_plane, "TransactionalQuoteBatchWorker", Worker)
    monkeypatch.setattr(cli_control_plane, "TransactionalQuoteCertifier", Worker)

    cli_control_plane._transactional_quote_workers(
        cast(PostgresControlPlane, object()), worker_id="quote-worker"
    )

    policy = runtime_policy("quote-batch", 120)
    assert captured["config"].retries["total_max_attempts"] == policy.provider_attempts
    assert (
        captured["config"].connect_timeout + captured["config"].read_timeout
        <= policy.provider_timeout_seconds
    )


def test_quote_factory_builds_distinct_lanes_from_the_existing_clob_bound(
    monkeypatch, settings_for_test_with_r2
) -> None:
    from polyarb import cli_control_plane
    from polyarb.control_plane.postgres import PostgresControlPlane

    captured: list[dict[str, object]] = []

    class Worker:
        _lease_seconds = 120

        def __init__(self, **kwargs: object) -> None:
            captured.append(kwargs)

    class Certifier:
        def __init__(self, **_kwargs: object) -> None:
            pass

    settings = settings_for_test_with_r2.model_copy(
        update={"clob_batch_max_concurrency": 4}
    )
    monkeypatch.setattr(cli_control_plane, "Settings", lambda: settings)
    monkeypatch.setattr(cli_control_plane, "_build_client", lambda *_a, **_k: object())
    monkeypatch.setattr(cli_control_plane, "ClobReaderClient", lambda _settings: object())
    monkeypatch.setattr(cli_control_plane, "TransactionalQuoteBatchWorker", Worker)
    monkeypatch.setattr(cli_control_plane, "TransactionalQuoteCertifier", Certifier)

    batch_pool, _certifier = cli_control_plane._transactional_quote_workers(
        cast(PostgresControlPlane, object()),
        worker_id="quote-worker",
        lane_count=settings.clob_batch_max_concurrency,
    )

    assert type(batch_pool).__name__ == "TransactionalQuoteBatchPool"
    assert [lane["worker_id"] for lane in captured] == [
        f"quote-worker:{ordinal}" for ordinal in range(4)
    ]
    assert len({id(lane["reader"]) for lane in captured}) == 1
    assert len({id(lane["object_client"]) for lane in captured}) == 1


def test_structure_factory_builds_distinct_fenced_lanes_with_one_object_client(
    monkeypatch,
) -> None:
    from polyarb import cli_control_plane
    from polyarb.control_plane.postgres import PostgresControlPlane

    captured: list[dict[str, object]] = []
    object_client = object()

    class Worker:
        _lease_seconds = 120

        def __init__(self, **kwargs: object) -> None:
            captured.append(kwargs)

    monkeypatch.setattr(cli_control_plane, "TransactionalStructureWorker", Worker)
    monkeypatch.setattr(
        cli_control_plane,
        "_structure_object_client",
        lambda: (object_client, "structure-bucket"),
    )

    range_pool = cli_control_plane._transactional_structure_worker(
        cast(PostgresControlPlane, object()),
        worker_id="structure-worker",
        lane_count=4,
    )

    assert type(range_pool).__name__ == "TransactionalStructureRangePool"
    assert [lane["worker_id"] for lane in captured] == [
        f"structure-worker:{ordinal}" for ordinal in range(4)
    ]
    assert {id(lane["object_client"]) for lane in captured} == {id(object_client)}
    assert {lane["bucket"] for lane in captured} == {"structure-bucket"}


def test_watchdog_reads_restart_details_in_one_parallel_round_per_app(monkeypatch) -> None:
    from polyarb import cli_control_plane

    calls = []

    detail_barrier = threading.Barrier(2, timeout=0.5)

    class Response:
        def __init__(self, payload):
            self.payload = payload

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return json.dumps(self.payload).encode()

    def open_once(request, *, timeout):
        calls.append((request.full_url, timeout))
        if request.full_url.endswith("/machines"):
            return Response(
                [
                    {"id": "worker-a", "state": "started"},
                    {"id": "worker-b", "state": "stopped"},
                ]
            )
        detail_barrier.wait()
        restart_count = 2 if request.full_url.endswith("worker-a") else 7
        machine_id = "worker-a" if restart_count == 2 else "worker-b"
        return Response(
            {
                "id": machine_id,
                "events": [{"request": {"restart_count": restart_count}}],
            }
        )

    monkeypatch.setattr(cli_control_plane, "urlopen", open_once)

    states, restarts = cli_control_plane._read_cloud_fly_machine_snapshot(
        ("worker-a", "worker-b"), app="worker-app", token="read-token"
    )

    assert states == {"worker-a": "started", "worker-b": "stopped"}
    assert restarts == {"worker-a": 2, "worker-b": 7}
    assert len(calls) == 3


def test_watchdog_rejects_an_unbounded_cross_app_operation_before_provider_reads(
    monkeypatch,
) -> None:
    from polyarb import cli_control_plane

    argv = [
        "watchdog-serve",
        "--enable",
        "--control-api-url",
        "https://control.example/perception/control-plane",
        "--fly-app",
        "primary-app",
        "--machine-id",
        "primary-machine",
    ]
    for index in range(cli_control_plane._MAX_WATCHDOG_APPS):
        argv.extend(("--secondary-target", f"extra-app-{index}/machine-{index}"))
    args = cli_control_plane._parser().parse_args(argv)
    monkeypatch.setattr(
        cli_control_plane,
        "_read_soak_control_snapshot",
        lambda _url: {"status": "available", "job_counts": {}},
    )
    provider_reads = 0

    def snapshot(*_args, **_kwargs):
        nonlocal provider_reads
        provider_reads += 1
        return {}, {}

    monkeypatch.setattr(cli_control_plane, "_read_cloud_fly_machine_snapshot", snapshot)
    monkeypatch.setenv("POLYARB_FLY_API_TOKEN", "status-token")

    observation = cli_control_plane._read_runtime_watchdog_observation(args)

    assert observation.healthy is False
    assert provider_reads == 0


def test_watchdog_control_and_app_reads_share_one_parallel_observation_round(
    monkeypatch,
) -> None:
    from polyarb import cli_control_plane

    args = cli_control_plane._parser().parse_args(
        [
            "watchdog-serve",
            "--enable",
            "--control-api-url",
            "https://control.example/perception/control-plane",
            "--fly-app",
            "worker-app",
            "--machine-id",
            "worker-a",
            "--secondary-fly-app",
            "evidence-app",
            "--secondary-machine-id",
            "evidence-a",
        ]
    )
    round_barrier = threading.Barrier(3, timeout=0.5)

    def read_control(_url):
        round_barrier.wait()
        return {"status": "available", "job_counts": {"succeeded": 1}}

    def read_app(machine_ids, *, app, token):
        assert token == "status-token"
        round_barrier.wait()
        return (
            {machine_id: "started" for machine_id in machine_ids},
            {machine_id: 0 for machine_id in machine_ids},
        )

    monkeypatch.setattr(cli_control_plane, "_read_soak_control_snapshot", read_control)
    monkeypatch.setattr(cli_control_plane, "_read_cloud_fly_machine_snapshot", read_app)
    monkeypatch.setenv("POLYARB_FLY_API_TOKEN", "status-token")

    observation = cli_control_plane._read_runtime_watchdog_observation(args)

    assert observation.healthy is True


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
                "revision_022_tables": 23,
                "runtime_event_invariants": [
                    "append_only_function",
                    "append_only_trigger",
                    "unique_attempt_event_sequence",
                    "unique_idempotency_key",
                ],
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
            "revision_022_tables": 23,
            "runtime_event_invariants": [
                "append_only_function",
                "append_only_trigger",
                "unique_attempt_event_sequence",
                "unique_idempotency_key",
            ],
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
                "--runtime-event-writer-app",
                "polyarb-control-runtime-event-writer-staging",
                "--runtime-controller-app",
                "polyarb-runtime-controller-staging",
                "--qualification-worker-app",
                "polyarb-qualification-worker-staging",
                "--release-id",
                "0123456789abcdef0123456789abcdef01234567",
                "--runtime-recovery-allowed-target",
                "polyarb-control-worker-staging/machine-a",
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
    assert Path(result["runtime_controller_config"]).exists()
    assert Path(result["qualification_worker_config"]).exists()


def test_render_rollout_requires_exact_release_id_without_printing_supplied_value(
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
                "--runtime-event-writer-app",
                "polyarb-control-runtime-event-writer-staging",
                "--runtime-controller-app",
                "polyarb-runtime-controller-staging",
                "--qualification-worker-app",
                "polyarb-qualification-worker-staging",
                "--release-id",
                "INVALID-RELEASE-VALUE",
                "--expected-database",
                "control_plane_staging",
                "--output-dir",
                str(tmp_path),
                "--json",
            ]
        )
        == 1
    )
    captured = capsys.readouterr()
    assert "rollout artifact rendering unavailable: RolloutArtifactError" in captured.err
    assert "INVALID-RELEASE-VALUE" not in captured.err


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


def test_runtime_policy_replay_is_read_only_and_reports_first_breaking_sample(
    monkeypatch, capsys
) -> None:
    from polyarb import cli_control_plane
    from polyarb.control_plane.soak_evidence import create_record

    def record(at: str, *, expired: int = 0) -> dict[str, object]:
        return create_record(
            observed_at=at,
            control_api_url="https://control.example/perception/control-plane",
            machine_states={"worker-a": "started"},
            control_snapshot={
                "status": "available",
                "expired_leases": expired,
                "open_circuit_count": 0,
                "queue_health": {},
                "job_counts": {"succeeded": 10},
            },
        )

    class ReadOnlyControlPlane:
        def __init__(self) -> None:
            self.reads = 0

        def read_soak_observations(self, run_id: str):
            assert run_id == "run-a"
            self.reads += 1
            return (
                record("2026-08-23T13:41:00Z"),
                record("2026-08-23T16:22:21Z", expired=1),
                record("2026-08-23T16:27:21Z"),
            )

        def __getattr__(self, name: str):
            raise AssertionError(f"runtime replay must not call {name}")

    control_plane = ReadOnlyControlPlane()
    monkeypatch.setattr(cli_control_plane, "_control_plane_from_env", lambda: control_plane)

    assert (
        cli_control_plane.main(
            ["runtime-policy-replay", "--run-id", "run-a", "--max-gap-seconds", "20000", "--json"]
        )
        == 0
    )
    assert control_plane.reads == 1
    assert json.loads(capsys.readouterr().out) == {
        "first_breaking_at": "2026-08-23T16:22:21+00:00",
        "max_gap_seconds": 9681.0,
        "reason_codes": ["lease.expired"],
        "sample_count": 3,
        "status": "BREAKING",
    }


def test_runtime_policy_replay_requires_scoped_dsn_without_connecting(monkeypatch, capsys) -> None:
    from polyarb import cli_control_plane

    monkeypatch.delenv("POLYARB_SUPABASE_DB_DSN", raising=False)
    monkeypatch.setattr(
        cli_control_plane.psycopg,
        "connect",
        lambda _dsn: (_ for _ in ()).throw(AssertionError("must not connect")),
    )

    assert cli_control_plane.main(["runtime-policy-replay", "--run-id", "run-a", "--json"]) == 2
    captured = capsys.readouterr()
    assert "POLYARB_SUPABASE_DB_DSN is required" in captured.err
    assert "postgresql://" not in captured.err


def test_qualification_status_uses_scoped_dsn_and_is_read_only(monkeypatch, capsys) -> None:
    from datetime import UTC, datetime

    from polyarb import cli_control_plane

    captured: dict[str, object] = {}
    monkeypatch.delenv("POLYARB_SUPABASE_DB_DSN", raising=False)
    monkeypatch.delenv("POLYARB_DB_EXPECTED_DATABASE", raising=False)
    monkeypatch.setenv(
        "POLYARB_QUALIFICATION_DB_DSN",
        "postgresql://qualification:secret@example.test/control",
    )
    monkeypatch.setattr(
        cli_control_plane.psycopg,
        "connect",
        lambda dsn, **kwargs: (
            captured.update(dsn=dsn, kwargs=kwargs) or _BootstrapConnection(captured)
        ),
    )

    class Store:
        def __init__(self, connection_factory):
            self.connection_factory = connection_factory

        def status(self, *, now):
            assert now.tzinfo is not None
            self.connection_factory()
            return {
                "epoch": {"epoch_id": "epoch-a", "state": "accumulating"},
                "duration_seconds": 12,
                "evidence_gap_seconds": 0,
                "last_fact": None,
                "last_breaker": None,
                "contained_recoveries": [],
                "certificate": None,
            }

    monkeypatch.setattr(cli_control_plane, "PostgresQualificationServiceStore", Store)
    monkeypatch.setattr(
        cli_control_plane,
        "datetime",
        type("Clock", (), {"now": staticmethod(lambda *_args: datetime(2026, 8, 25, tzinfo=UTC))}),
    )

    assert cli_control_plane.main(["qualification-status", "--json"]) == 0
    assert captured["dsn"] == "postgresql://qualification:secret@example.test/control"
    assert captured["kwargs"] == {
        "connect_timeout": 5,
        "options": (
            "-csearch_path=pg_catalog,public -cstatement_timeout=5000ms -clock_timeout=1000ms"
        ),
    }
    assert "set_config('search_path'" in cast(tuple[str, object], captured["bootstrap"])[0]
    assert json.loads(capsys.readouterr().out)["epoch"]["epoch_id"] == "epoch-a"


@pytest.mark.parametrize(
    "command,dsn_env",
    (
        ("runtime-reconcile-once", "POLYARB_SUPABASE_DB_DSN"),
        ("qualification-serve", "POLYARB_QUALIFICATION_DB_DSN"),
    ),
)
def test_daemons_reject_dsn_search_path_override_before_mutation(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    command: str,
    dsn_env: str,
) -> None:
    from polyarb import cli_control_plane

    secret = "namespace-secret"
    monkeypatch.setenv(
        dsn_env,
        f"postgresql://scoped:{secret}@example.test/control?options=-csearch_path%3Devil",
    )
    monkeypatch.setenv("POLYARB_DB_EXPECTED_DATABASE", "control")
    mutations: list[str] = []
    monkeypatch.setattr(
        cli_control_plane,
        "_runtime_reconcile_once",
        lambda *_args, **_kwargs: mutations.append("runtime") or {},
    )
    monkeypatch.setattr(
        cli_control_plane,
        "run_qualification_service",
        lambda *_args, **_kwargs: mutations.append("qualification") or {},
    )

    argv = [command, "--enable", "--json"]
    result = cli_control_plane.main(argv)

    assert result == 1
    assert mutations == []
    captured = capsys.readouterr()
    output = captured.out + captured.err
    assert "database-role.dsn-override" in output
    assert secret not in output
    assert "postgresql://" not in output


def test_qualification_certificates_reverify_read_only_limit(monkeypatch, capsys) -> None:
    from polyarb import cli_control_plane

    monkeypatch.delenv("POLYARB_DB_EXPECTED_DATABASE", raising=False)
    monkeypatch.setenv("POLYARB_QUALIFICATION_DB_DSN", "postgresql://qualification@example/control")

    class Store:
        def __init__(self, connection_factory):
            self.connection_factory = connection_factory

        def certificates(self, *, limit: int):
            assert limit == 2
            return [
                {
                    "certificate_id": "qualification-certificate:" + "a" * 64,
                    "epoch_id": "epoch-a",
                    "certificate_digest": "a" * 64,
                    "reverified": True,
                }
            ]

    monkeypatch.setattr(cli_control_plane, "PostgresQualificationServiceStore", Store)
    monkeypatch.setattr(cli_control_plane.psycopg, "connect", lambda *_args, **_kwargs: object())

    assert cli_control_plane.main(["qualification-certificates", "--limit", "2", "--json"]) == 0
    assert json.loads(capsys.readouterr().out) == {
        "certificates": [
            {
                "certificate_digest": "a" * 64,
                "certificate_id": "qualification-certificate:" + "a" * 64,
                "epoch_id": "epoch-a",
                "reverified": True,
            }
        ],
        "status": "available",
    }


def test_qualification_serve_requires_enable_before_connect(monkeypatch, capsys) -> None:
    from polyarb import cli_control_plane

    monkeypatch.setenv("POLYARB_QUALIFICATION_DB_DSN", "postgresql://qualification@example/control")
    monkeypatch.setattr(
        cli_control_plane.psycopg,
        "connect",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not connect")),
    )

    assert (
        cli_control_plane.main(["qualification-serve", "--interval-seconds", "30", "--json"]) == 2
    )
    assert "--enable is required" in capsys.readouterr().err


def test_qualification_serve_verifies_database_role_before_service_construction(
    monkeypatch, capsys
) -> None:
    from polyarb import cli_control_plane

    events: list[str] = []
    connection_factory = object()

    def verify(factory, profile, *, expected_database):
        assert factory is connection_factory
        assert profile == "qualification-worker"
        assert expected_database == "role_test"
        events.append("verify")

    class Service:
        def tick(self, now):
            assert now.tzinfo is not None
            events.append("tick")
            raise RuntimeError("stop-after-tick")

    def service_from_env(**_kwargs):
        events.append("service")
        return Service()

    monkeypatch.setenv("POLYARB_QUALIFICATION_DB_DSN", "postgresql://qualification@example/control")
    monkeypatch.setenv("POLYARB_DB_EXPECTED_DATABASE", "role_test")
    monkeypatch.setattr(
        cli_control_plane,
        "_qualification_connection_factory_from_env",
        lambda: connection_factory,
    )
    monkeypatch.setattr(cli_control_plane, "verify_daemon_database_role", verify, raising=False)
    monkeypatch.setattr(cli_control_plane, "_qualification_service_from_env", service_from_env)

    assert (
        cli_control_plane.main(
            ["qualification-serve", "--enable", "--interval-seconds", "0.001", "--json"]
        )
        == 1
    )
    assert events == ["verify", "service", "tick"]
    assert "postgresql://" not in capsys.readouterr().err


def test_qualification_serve_validates_identity_before_service_construction_and_sanitizes(
    monkeypatch, capsys
) -> None:
    from polyarb import cli_control_plane

    connection_factory = object()
    events: list[str] = []
    bad_release = "ABCDEF0123456789ABCDEF0123456789ABCDEF01"

    def verify(factory, profile, *, expected_database):
        assert factory is connection_factory
        assert profile == "qualification-worker"
        assert expected_database == "role_test"
        events.append("verify")

    monkeypatch.setenv("POLYARB_QUALIFICATION_DB_DSN", "postgresql://qualification@example/control")
    monkeypatch.setenv("POLYARB_DB_EXPECTED_DATABASE", "role_test")
    monkeypatch.setenv("POLYARB_QUALIFICATION_RELEASE_ID", bad_release)
    monkeypatch.setenv("POLYARB_QUALIFICATION_CONFIG_ID", "sha256:" + "0" * 64)
    monkeypatch.setenv(
        "POLYARB_QUALIFICATION_ROLE_IDENTITY",
        "opportunity,quote,structure",
    )
    monkeypatch.setenv("POLYARB_QUALIFICATION_RUNTIME_RECOVERY_MODE", "observe-only")
    monkeypatch.setenv("POLYARB_QUALIFICATION_RUNTIME_RECOVERY_ALLOWED_TARGETS", "")
    monkeypatch.setattr(
        cli_control_plane,
        "_qualification_connection_factory_from_env",
        lambda: connection_factory,
    )
    monkeypatch.setattr(cli_control_plane, "verify_daemon_database_role", verify, raising=False)
    monkeypatch.setattr(
        cli_control_plane,
        "RollingQualificationPolicy",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("must not construct policy")
        ),
    )
    monkeypatch.setattr(
        cli_control_plane,
        "PostgresQualificationFactSource",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("must not construct fact source")
        ),
    )
    monkeypatch.setattr(
        cli_control_plane,
        "PostgresQualificationServiceStore",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("must not construct state store")
        ),
    )

    assert (
        cli_control_plane.main(
            ["qualification-serve", "--enable", "--interval-seconds", "30.0", "--json"]
        )
        == 1
    )
    captured = capsys.readouterr()
    assert events == ["verify"]
    assert "qualification.identity.release-invalid" in captured.err
    assert bad_release not in captured.err
    assert "postgresql://" not in captured.err


def test_qualification_role_contract_failure_stops_before_service_and_sanitizes(
    monkeypatch, capsys
) -> None:
    from polyarb import cli_control_plane
    from polyarb.control_plane.db_role_contract import DatabaseRoleContractError

    connection_factory = object()
    events: list[str] = []

    def fail_contract(*_args, **_kwargs):
        events.append("verify")
        raise DatabaseRoleContractError(
            "database-role.forbidden-privilege-present",
            "m1_qualification_ingress_ledger:INSERT",
        )

    monkeypatch.setenv(
        "POLYARB_QUALIFICATION_DB_DSN",
        "postgresql://qualification:secret@example/control",
    )
    monkeypatch.setenv("POLYARB_DB_EXPECTED_DATABASE", "role_test")
    monkeypatch.setattr(
        cli_control_plane,
        "_qualification_connection_factory_from_env",
        lambda: connection_factory,
    )
    monkeypatch.setattr(cli_control_plane, "verify_daemon_database_role", fail_contract)
    monkeypatch.setattr(
        cli_control_plane,
        "_qualification_service_from_env",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("must not build service")),
    )

    assert (
        cli_control_plane.main(
            ["qualification-serve", "--enable", "--interval-seconds", "0.001", "--json"]
        )
        == 1
    )
    captured = capsys.readouterr()
    assert events == ["verify"]
    assert "database-role.forbidden-privilege-present" in captured.err
    assert "m1_qualification_ingress_ledger:INSERT" in captured.err
    assert "postgresql://" not in captured.err
    assert "secret" not in captured.err


def test_qualification_serve_stops_on_tick_error_without_overlap(monkeypatch, capsys) -> None:
    from polyarb import cli_control_plane

    monkeypatch.setenv("POLYARB_QUALIFICATION_DB_DSN", "postgresql://qualification@example/control")
    monkeypatch.setattr(cli_control_plane.psycopg, "connect", lambda *_args, **_kwargs: object())

    calls: list[str] = []

    class Service:
        def tick(self, now):
            assert now.tzinfo is not None
            calls.append("tick")
            raise RuntimeError(
                "source query timeout host=db.internal query=SELECT secret "
                "config=/etc/polyarb.env dsn=postgresql://operator:password@example/control"
            )

    monkeypatch.setattr(
        cli_control_plane,
        "_qualification_service_from_env",
        lambda **_kwargs: Service(),
    )

    assert (
        cli_control_plane.main(
            ["qualification-serve", "--enable", "--interval-seconds", "0.001", "--json"]
        )
        == 1
    )
    captured = capsys.readouterr()
    assert calls == ["tick"]
    assert "qualification-service.tick-failed" in captured.err
    for leaked in (
        "source query timeout",
        "db.internal",
        "SELECT secret",
        "/etc/polyarb.env",
        "postgresql://",
        "operator",
        "password",
    ):
        assert leaked not in captured.err
