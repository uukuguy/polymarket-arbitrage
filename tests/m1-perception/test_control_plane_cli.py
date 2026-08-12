"""Operator CLI contract for the non-mutating control-plane shadow bridge."""

from __future__ import annotations

import json


def test_quote_control_plane_once_requires_explicit_enable(monkeypatch, capsys) -> None:
    from polyarb import cli_control_plane

    monkeypatch.setenv("POLYARB_SUPABASE_DB_DSN", "postgresql://operator:secret@example.test/control")
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

    monkeypatch.setenv("POLYARB_SUPABASE_DB_DSN", "postgresql://operator:secret@example.test/control")
    monkeypatch.setattr(
        cli_control_plane.psycopg,
        "connect",
        lambda _dsn: (_ for _ in ()).throw(AssertionError("must not connect")),
    )

    assert cli_control_plane.main(["structure-once", "--json"]) == 2

    captured = capsys.readouterr()
    assert "--enable is required" in captured.err
    assert "postgresql://" not in captured.err


def test_quote_control_plane_once_runs_one_batch_then_certifier(monkeypatch, capsys) -> None:
    from polyarb import cli_control_plane

    class BatchWorker:
        async def run_once(self):
            return type("Result", (), {"job_key": "quote:one:batch:0", "outcome": "succeeded"})()

    class Certifier:
        def run_once(self):
            return type("Result", (), {"job_key": "quote:one:certify", "outcome": "waiting"})()

    monkeypatch.setenv("POLYARB_SUPABASE_DB_DSN", "postgresql://operator:secret@example.test/control")
    monkeypatch.setattr(cli_control_plane.psycopg, "connect", lambda _dsn: object())
    monkeypatch.setattr(
        cli_control_plane,
        "_transactional_quote_workers",
        lambda _control_plane, *, worker_id: (BatchWorker(), Certifier()),
    )

    assert (
        cli_control_plane.main(
            ["quote-once", "--enable", "--worker-id", "test-worker", "--json"]
        )
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

    monkeypatch.setenv("POLYARB_SUPABASE_DB_DSN", "postgresql://operator:secret@example.test/control")
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


def test_shadow_sync_requires_dsn_without_printing_it(monkeypatch, capsys, tmp_path) -> None:
    from polyarb import cli_control_plane

    monkeypatch.delenv("POLYARB_SUPABASE_DB_DSN", raising=False)

    assert (
        cli_control_plane.main(
            ["shadow-sync", "--db-path", str(tmp_path / "state.db"), "--json"]
        )
        == 2
    )
    captured = capsys.readouterr()
    assert "POLYARB_SUPABASE_DB_DSN is required" in captured.err
    assert "postgresql://" not in captured.err


def test_shadow_sync_reports_idempotent_source_count(monkeypatch, capsys, tmp_path) -> None:
    from polyarb import cli_control_plane
    from polyarb.control_plane.shadow import ShadowSource

    monkeypatch.setenv("POLYARB_SUPABASE_DB_DSN", "postgresql://operator:secret@example.test/control")
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
