from __future__ import annotations

import os
import subprocess
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest

from polyarb.control_plane import runtime_fault_matrix as matrix_module
from polyarb.control_plane.production_commissioning_harness import (
    CommissioningHarnessError,
    run_heartbeat_outage_commissioning,
    run_normalization_payload_corrupt_commissioning,
    run_progress_stall_commissioning,
    run_publication_pointer_conflict_commissioning,
    run_quote_admission_missing_shard_commissioning,
    run_quote_batch_incomplete_commissioning,
    run_r2_read_timeout_commissioning,
    run_r2_write_timeout_commissioning,
    run_retry_budget_commissioning,
    run_source_receipt_gap_commissioning,
    run_stale_owner_commissioning,
    run_stale_quote_pointer_commissioning,
    run_structure_parity_mismatch_commissioning,
    run_worker_exit_commissioning,
)
from polyarb.control_plane.runtime_fault_matrix import (
    migrated_disposable_control_plane_database,
)

RELEASE = "a" * 40
CONFIG = f"sha256:{'b' * 64}"


def _normalize_dsn(dsn: str) -> str:
    for prefix in ("postgresql+psycopg2://", "postgresql+psycopg://"):
        if dsn.startswith(prefix):
            return "postgresql://" + dsn[len(prefix) :]
    return dsn


@pytest.fixture(scope="module")
def control_plane_test_dsn() -> Iterator[str]:
    try:
        available = subprocess.run(
            ["docker", "info"], capture_output=True, text=True, timeout=5
        ).returncode == 0
    except OSError:
        available = False
    if not available:
        pytest.skip("Docker daemon unavailable; commissioning harness requires Postgres")
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as postgres:
        dsn = _normalize_dsn(postgres.get_connection_url())
        with psycopg.connect(dsn, autocommit=True) as connection:
            for role in ("anon", "authenticated", "service_role"):
                connection.execute(f"CREATE ROLE {role} NOLOGIN")
        yield dsn


def test_stale_owner_harness_requires_explicit_test_dsn_before_artifact_mutation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("POLYARB_CONTROL_PLANE_TEST_DSN", raising=False)
    root = tmp_path / "evidence"

    with pytest.raises(CommissioningHarnessError, match="POLYARB_CONTROL_PLANE_TEST_DSN"):
        run_stale_owner_commissioning(
            root=root,
            release_id=RELEASE,
            config_id=CONFIG,
        )

    assert not root.exists()


def test_progress_stall_harness_requires_explicit_test_dsn_before_artifact_mutation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("POLYARB_CONTROL_PLANE_TEST_DSN", raising=False)
    root = tmp_path / "evidence"

    with pytest.raises(CommissioningHarnessError, match="POLYARB_CONTROL_PLANE_TEST_DSN"):
        run_progress_stall_commissioning(
            root=root,
            release_id=RELEASE,
            config_id=CONFIG,
        )

    assert not root.exists()


def test_retry_budget_harness_requires_explicit_test_dsn_before_artifact_mutation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("POLYARB_CONTROL_PLANE_TEST_DSN", raising=False)
    root = tmp_path / "evidence"

    with pytest.raises(CommissioningHarnessError, match="POLYARB_CONTROL_PLANE_TEST_DSN"):
        run_retry_budget_commissioning(
            root=root,
            release_id=RELEASE,
            config_id=CONFIG,
        )

    assert not root.exists()


def test_heartbeat_outage_harness_requires_explicit_test_dsn_before_artifact_mutation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("POLYARB_CONTROL_PLANE_TEST_DSN", raising=False)
    root = tmp_path / "evidence"

    with pytest.raises(CommissioningHarnessError, match="POLYARB_CONTROL_PLANE_TEST_DSN"):
        run_heartbeat_outage_commissioning(
            root=root,
            release_id=RELEASE,
            config_id=CONFIG,
        )

    assert not root.exists()


def test_worker_exit_harness_requires_explicit_test_dsn_before_artifact_mutation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("POLYARB_CONTROL_PLANE_TEST_DSN", raising=False)
    root = tmp_path / "evidence"

    with pytest.raises(CommissioningHarnessError, match="POLYARB_CONTROL_PLANE_TEST_DSN"):
        run_worker_exit_commissioning(
            root=root,
            release_id=RELEASE,
            config_id=CONFIG,
        )

    assert not root.exists()


def test_source_receipt_gap_harness_requires_explicit_test_dsn_before_artifact_mutation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("POLYARB_CONTROL_PLANE_TEST_DSN", raising=False)
    root = tmp_path / "evidence"

    with pytest.raises(CommissioningHarnessError, match="POLYARB_CONTROL_PLANE_TEST_DSN"):
        run_source_receipt_gap_commissioning(
            root=root,
            release_id=RELEASE,
            config_id=CONFIG,
        )

    assert not root.exists()


def test_quote_batch_incomplete_harness_requires_explicit_test_dsn_before_artifact_mutation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("POLYARB_CONTROL_PLANE_TEST_DSN", raising=False)
    root = tmp_path / "evidence"

    with pytest.raises(CommissioningHarnessError, match="POLYARB_CONTROL_PLANE_TEST_DSN"):
        run_quote_batch_incomplete_commissioning(
            root=root,
            release_id=RELEASE,
            config_id=CONFIG,
        )

    assert not root.exists()


def test_quote_admission_missing_shard_harness_requires_explicit_test_dsn_before_artifact_mutation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("POLYARB_CONTROL_PLANE_TEST_DSN", raising=False)
    root = tmp_path / "evidence"

    with pytest.raises(CommissioningHarnessError, match="POLYARB_CONTROL_PLANE_TEST_DSN"):
        run_quote_admission_missing_shard_commissioning(
            root=root,
            release_id=RELEASE,
            config_id=CONFIG,
        )

    assert not root.exists()


def test_normalization_payload_corrupt_harness_requires_explicit_test_dsn_before_artifact_mutation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("POLYARB_CONTROL_PLANE_TEST_DSN", raising=False)
    root = tmp_path / "evidence"

    with pytest.raises(CommissioningHarnessError, match="POLYARB_CONTROL_PLANE_TEST_DSN"):
        run_normalization_payload_corrupt_commissioning(
            root=root,
            release_id=RELEASE,
            config_id=CONFIG,
        )

    assert not root.exists()


def test_stale_owner_harness_rejects_invalid_identity_before_artifact_mutation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv(
        "POLYARB_CONTROL_PLANE_TEST_DSN",
        "postgresql://localhost/test",
    )
    root = tmp_path / "evidence"

    with pytest.raises(CommissioningHarnessError, match="invalid-release-id"):
        run_stale_owner_commissioning(
            root=root,
            release_id="not-a-release",
            config_id=CONFIG,
        )

    assert not root.exists()


def test_stale_owner_harness_runs_real_isolated_node_and_cleans_database_and_roles(
    monkeypatch: pytest.MonkeyPatch,
    control_plane_test_dsn: str,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("POLYARB_CONTROL_PLANE_TEST_DSN", control_plane_test_dsn)
    root = tmp_path / "evidence"

    result = run_stale_owner_commissioning(
        root=root,
        release_id=RELEASE,
        config_id=CONFIG,
        node_ids=("structure-normalize",),
    )

    assert result == {
        "attack_id": "stale-owner-terminal-write",
        "execution_scope": "disposable-exact-image",
        "node_count": 1,
        "proof_count": 1,
        "status": "pass",
    }
    assert (root / "attacks/structure-normalize/stale-owner-terminal-write/proof.json").is_file()
    with psycopg.connect(control_plane_test_dsn) as connection:
        databases = connection.execute(
            "SELECT datname FROM pg_database WHERE datname LIKE 'm1_commissioning_%'"
        ).fetchall()
        roles = connection.execute(
            """
            SELECT rolname FROM pg_roles
            WHERE rolname IN (
                'l3_evidence_daemon',
                'l3_retention_operator',
                'm1_runtime_controller_capability',
                'm1_qualification_worker_capability'
            )
            ORDER BY rolname
            """
        ).fetchall()
    assert databases == []
    assert roles == []


def test_progress_stall_harness_runs_real_isolated_node_and_cleans_database_and_roles(
    monkeypatch: pytest.MonkeyPatch,
    control_plane_test_dsn: str,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("POLYARB_CONTROL_PLANE_TEST_DSN", control_plane_test_dsn)
    root = tmp_path / "evidence"

    result = run_progress_stall_commissioning(
        root=root,
        release_id=RELEASE,
        config_id=CONFIG,
        node_ids=("structure-normalize",),
    )

    assert result == {
        "attack_id": "progress-stall",
        "execution_scope": "disposable-exact-image",
        "node_count": 1,
        "proof_count": 1,
        "status": "pass",
    }
    assert (root / "attacks/structure-normalize/progress-stall/proof.json").is_file()
    with psycopg.connect(control_plane_test_dsn) as connection:
        databases = connection.execute(
            "SELECT datname FROM pg_database WHERE datname LIKE 'm1_commissioning_%'"
        ).fetchall()
        roles = connection.execute(
            """
            SELECT rolname FROM pg_roles
            WHERE rolname IN (
                'l3_evidence_daemon',
                'l3_retention_operator',
                'm1_runtime_controller_capability',
                'm1_qualification_worker_capability'
            )
            ORDER BY rolname
            """
        ).fetchall()
    assert databases == []
    assert roles == []


def test_retry_budget_harness_runs_real_isolated_node_and_cleans_database_and_roles(
    monkeypatch: pytest.MonkeyPatch,
    control_plane_test_dsn: str,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("POLYARB_CONTROL_PLANE_TEST_DSN", control_plane_test_dsn)
    root = tmp_path / "evidence"

    result = run_retry_budget_commissioning(
        root=root,
        release_id=RELEASE,
        config_id=CONFIG,
        node_ids=("structure-normalize",),
    )

    assert result == {
        "attack_id": "retry-budget-exhaustion",
        "execution_scope": "disposable-exact-image",
        "node_count": 1,
        "proof_count": 1,
        "status": "pass",
    }
    assert (root / "attacks/structure-normalize/retry-budget-exhaustion/proof.json").is_file()
    with psycopg.connect(control_plane_test_dsn) as connection:
        databases = connection.execute(
            "SELECT datname FROM pg_database WHERE datname LIKE 'm1_commissioning_%'"
        ).fetchall()
        roles = connection.execute(
            """
            SELECT rolname FROM pg_roles
            WHERE rolname IN (
                'l3_evidence_daemon',
                'l3_retention_operator',
                'm1_runtime_controller_capability',
                'm1_qualification_worker_capability'
            )
            ORDER BY rolname
            """
        ).fetchall()
    assert databases == []
    assert roles == []


def test_heartbeat_outage_harness_runs_real_isolated_node_and_cleans_database_and_roles(
    monkeypatch: pytest.MonkeyPatch,
    control_plane_test_dsn: str,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("POLYARB_CONTROL_PLANE_TEST_DSN", control_plane_test_dsn)
    root = tmp_path / "evidence"

    result = run_heartbeat_outage_commissioning(
        root=root,
        release_id=RELEASE,
        config_id=CONFIG,
        node_ids=("structure-normalize",),
    )

    assert result == {
        "attack_id": "heartbeat-outage",
        "execution_scope": "disposable-exact-image",
        "node_count": 1,
        "proof_count": 1,
        "status": "pass",
    }
    assert (root / "attacks/structure-normalize/heartbeat-outage/proof.json").is_file()
    with psycopg.connect(control_plane_test_dsn) as connection:
        databases = connection.execute(
            "SELECT datname FROM pg_database WHERE datname LIKE 'm1_commissioning_%'"
        ).fetchall()
        roles = connection.execute(
            """
            SELECT rolname FROM pg_roles
            WHERE rolname IN (
                'l3_evidence_daemon',
                'l3_retention_operator',
                'm1_runtime_controller_capability',
                'm1_qualification_worker_capability'
            )
            ORDER BY rolname
            """
        ).fetchall()
    assert databases == []
    assert roles == []


def test_worker_exit_harness_runs_real_isolated_node_and_cleans_database_and_roles(
    monkeypatch: pytest.MonkeyPatch,
    control_plane_test_dsn: str,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("POLYARB_CONTROL_PLANE_TEST_DSN", control_plane_test_dsn)
    root = tmp_path / "evidence"

    result = run_worker_exit_commissioning(
        root=root,
        release_id=RELEASE,
        config_id=CONFIG,
        node_ids=("structure-normalize",),
    )

    assert result == {
        "attack_id": "worker-exit",
        "execution_scope": "disposable-exact-image",
        "node_count": 1,
        "proof_count": 1,
        "status": "pass",
    }
    assert (root / "attacks/structure-normalize/worker-exit/proof.json").is_file()
    with psycopg.connect(control_plane_test_dsn) as connection:
        databases = connection.execute(
            "SELECT datname FROM pg_database WHERE datname LIKE 'm1_commissioning_%'"
        ).fetchall()
        roles = connection.execute(
            """
            SELECT rolname FROM pg_roles
            WHERE rolname IN (
                'l3_evidence_daemon',
                'l3_retention_operator',
                'm1_runtime_controller_capability',
                'm1_qualification_worker_capability'
            )
            ORDER BY rolname
            """
        ).fetchall()
    assert databases == []
    assert roles == []


def test_source_receipt_gap_harness_runs_target_and_cleans_database_and_roles(
    monkeypatch: pytest.MonkeyPatch,
    control_plane_test_dsn: str,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("POLYARB_CONTROL_PLANE_TEST_DSN", control_plane_test_dsn)
    root = tmp_path / "evidence"

    result = run_source_receipt_gap_commissioning(
        root=root,
        release_id=RELEASE,
        config_id=CONFIG,
    )

    assert result == {
        "attack_id": "source-receipt-gap",
        "execution_scope": "disposable-exact-image",
        "node_count": 1,
        "proof_count": 1,
        "status": "pass",
    }
    assert (
        root
        / "attacks/structure-materialize/source-receipt-gap/proof.json"
    ).is_file()
    with psycopg.connect(control_plane_test_dsn) as connection:
        databases = connection.execute(
            "SELECT datname FROM pg_database WHERE datname LIKE 'm1_commissioning_%'"
        ).fetchall()
        roles = connection.execute(
            """
            SELECT rolname FROM pg_roles
            WHERE rolname IN (
                'l3_evidence_daemon',
                'l3_retention_operator',
                'm1_runtime_controller_capability',
                'm1_qualification_worker_capability'
            )
            ORDER BY rolname
            """
        ).fetchall()
    assert databases == []
    assert roles == []


def test_quote_batch_incomplete_harness_runs_target_and_cleans_database_and_roles(
    monkeypatch: pytest.MonkeyPatch,
    control_plane_test_dsn: str,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("POLYARB_CONTROL_PLANE_TEST_DSN", control_plane_test_dsn)
    root = tmp_path / "evidence"

    result = run_quote_batch_incomplete_commissioning(
        root=root,
        release_id=RELEASE,
        config_id=CONFIG,
    )

    assert result == {
        "attack_id": "quote-batch-incomplete",
        "execution_scope": "disposable-exact-image",
        "node_count": 1,
        "proof_count": 1,
        "status": "pass",
    }
    assert (
        root / "attacks/quote-certify/quote-batch-incomplete/proof.json"
    ).is_file()
    with psycopg.connect(control_plane_test_dsn) as connection:
        databases = connection.execute(
            "SELECT datname FROM pg_database WHERE datname LIKE 'm1_commissioning_%'"
        ).fetchall()
        roles = connection.execute(
            """
            SELECT rolname FROM pg_roles
            WHERE rolname IN (
                'l3_evidence_daemon',
                'l3_retention_operator',
                'm1_runtime_controller_capability',
                'm1_qualification_worker_capability'
            )
            ORDER BY rolname
            """
        ).fetchall()
    assert databases == []
    assert roles == []


def test_quote_admission_missing_shard_harness_runs_target_and_cleans_database_and_roles(
    monkeypatch: pytest.MonkeyPatch,
    control_plane_test_dsn: str,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("POLYARB_CONTROL_PLANE_TEST_DSN", control_plane_test_dsn)
    root = tmp_path / "evidence"

    result = run_quote_admission_missing_shard_commissioning(
        root=root,
        release_id=RELEASE,
        config_id=CONFIG,
    )

    assert result == {
        "attack_id": "quote-admission-missing-shard",
        "execution_scope": "disposable-exact-image",
        "node_count": 1,
        "proof_count": 1,
        "status": "pass",
    }
    assert (
        root / "attacks/quote-admit/quote-admission-missing-shard/proof.json"
    ).is_file()
    with psycopg.connect(control_plane_test_dsn) as connection:
        databases = connection.execute(
            "SELECT datname FROM pg_database WHERE datname LIKE 'm1_commissioning_%'"
        ).fetchall()
        roles = connection.execute(
            """
            SELECT rolname FROM pg_roles
            WHERE rolname IN (
                'l3_evidence_daemon',
                'l3_retention_operator',
                'm1_runtime_controller_capability',
                'm1_qualification_worker_capability'
            )
            ORDER BY rolname
            """
        ).fetchall()
    assert databases == []
    assert roles == []


def test_normalization_payload_corrupt_harness_runs_target_and_cleans_database_and_roles(
    monkeypatch: pytest.MonkeyPatch,
    control_plane_test_dsn: str,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("POLYARB_CONTROL_PLANE_TEST_DSN", control_plane_test_dsn)
    root = tmp_path / "evidence"

    result = run_normalization_payload_corrupt_commissioning(
        root=root,
        release_id=RELEASE,
        config_id=CONFIG,
    )

    assert result == {
        "attack_id": "normalization-payload-corrupt",
        "execution_scope": "disposable-exact-image",
        "node_count": 1,
        "proof_count": 1,
        "status": "pass",
    }
    assert (
        root
        / "attacks/structure-normalize/normalization-payload-corrupt/proof.json"
    ).is_file()
    with psycopg.connect(control_plane_test_dsn) as connection:
        databases = connection.execute(
            "SELECT datname FROM pg_database WHERE datname LIKE 'm1_commissioning_%'"
        ).fetchall()
        roles = connection.execute(
            """
            SELECT rolname FROM pg_roles
            WHERE rolname IN (
                'l3_evidence_daemon',
                'l3_retention_operator',
                'm1_runtime_controller_capability',
                'm1_qualification_worker_capability'
            )
            ORDER BY rolname
            """
        ).fetchall()
    assert databases == []
    assert roles == []


def test_structure_parity_mismatch_harness_runs_target_and_cleans_database_and_roles(
    monkeypatch: pytest.MonkeyPatch,
    control_plane_test_dsn: str,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("POLYARB_CONTROL_PLANE_TEST_DSN", control_plane_test_dsn)
    root = tmp_path / "evidence"

    result = run_structure_parity_mismatch_commissioning(
        root=root,
        release_id=RELEASE,
        config_id=CONFIG,
    )

    assert result == {
        "attack_id": "structure-parity-mismatch",
        "execution_scope": "disposable-exact-image",
        "node_count": 1,
        "proof_count": 1,
        "status": "pass",
    }
    assert (
        root
        / "attacks/structure-certify/structure-parity-mismatch/proof.json"
    ).is_file()
    with psycopg.connect(control_plane_test_dsn) as connection:
        databases = connection.execute(
            "SELECT datname FROM pg_database WHERE datname LIKE 'm1_commissioning_%'"
        ).fetchall()
        roles = connection.execute(
            """
            SELECT rolname FROM pg_roles
            WHERE rolname IN (
                'l3_evidence_daemon',
                'l3_retention_operator',
                'm1_runtime_controller_capability',
                'm1_qualification_worker_capability'
            )
            ORDER BY rolname
            """
        ).fetchall()
    assert databases == []
    assert roles == []


def test_publication_pointer_conflict_harness_runs_three_targets_and_cleans_database_and_roles(
    monkeypatch: pytest.MonkeyPatch,
    control_plane_test_dsn: str,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("POLYARB_CONTROL_PLANE_TEST_DSN", control_plane_test_dsn)
    root = tmp_path / "evidence"

    result = run_publication_pointer_conflict_commissioning(
        root=root,
        release_id=RELEASE,
        config_id=CONFIG,
    )

    assert result == {
        "attack_id": "publication-pointer-conflict",
        "execution_scope": "disposable-exact-image",
        "node_count": 3,
        "proof_count": 3,
        "status": "pass",
    }
    for node_id in ("structure-certify", "quote-certify", "opportunity-certify"):
        assert (
            root / f"attacks/{node_id}/publication-pointer-conflict/proof.json"
        ).is_file()
    with psycopg.connect(control_plane_test_dsn) as connection:
        databases = connection.execute(
            "SELECT datname FROM pg_database WHERE datname LIKE 'm1_commissioning_%'"
        ).fetchall()
        roles = connection.execute(
            """
            SELECT rolname FROM pg_roles
            WHERE rolname IN (
                'l3_evidence_daemon',
                'l3_retention_operator',
                'm1_runtime_controller_capability',
                'm1_qualification_worker_capability'
            )
            ORDER BY rolname
            """
        ).fetchall()
    assert databases == []
    assert roles == []


def test_r2_read_timeout_harness_runs_six_targets_and_cleans_database_and_roles(
    monkeypatch: pytest.MonkeyPatch,
    control_plane_test_dsn: str,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("POLYARB_CONTROL_PLANE_TEST_DSN", control_plane_test_dsn)
    root = tmp_path / "evidence"

    result = run_r2_read_timeout_commissioning(
        root=root,
        release_id=RELEASE,
        config_id=CONFIG,
    )

    nodes = (
        "structure-materialize",
        "structure-normalize",
        "structure-certify",
        "quote-admit",
        "quote-certify",
        "opportunity-certify",
    )
    assert result == {
        "attack_id": "r2-read-timeout",
        "execution_scope": "disposable-exact-image",
        "node_count": 6,
        "proof_count": 6,
        "status": "pass",
    }
    for node_id in nodes:
        assert (root / f"attacks/{node_id}/r2-read-timeout/proof.json").is_file()
    with psycopg.connect(control_plane_test_dsn) as connection:
        databases = connection.execute(
            "SELECT datname FROM pg_database WHERE datname LIKE 'm1_commissioning_%'"
        ).fetchall()
    assert databases == []


def test_r2_write_timeout_harness_runs_seven_targets_and_cleans_database_and_roles(
    monkeypatch: pytest.MonkeyPatch,
    control_plane_test_dsn: str,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("POLYARB_CONTROL_PLANE_TEST_DSN", control_plane_test_dsn)
    root = tmp_path / "evidence"

    result = run_r2_write_timeout_commissioning(
        root=root,
        release_id=RELEASE,
        config_id=CONFIG,
    )

    nodes = (
        "structure-fetch",
        "structure-materialize",
        "structure-normalize",
        "structure-certify",
        "quote-admit",
        "quote-batch",
        "opportunity-certify",
    )
    assert result == {
        "attack_id": "r2-write-timeout",
        "execution_scope": "disposable-exact-image",
        "node_count": 7,
        "proof_count": 7,
        "status": "pass",
    }
    for node_id in nodes:
        assert (root / f"attacks/{node_id}/r2-write-timeout/proof.json").is_file()
    with psycopg.connect(control_plane_test_dsn) as connection:
        databases = connection.execute(
            "SELECT datname FROM pg_database WHERE datname LIKE 'm1_commissioning_%'"
        ).fetchall()
    assert databases == []


def test_stale_quote_pointer_harness_blocks_then_recovers_fresh_lineage(
    monkeypatch: pytest.MonkeyPatch,
    control_plane_test_dsn: str,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("POLYARB_CONTROL_PLANE_TEST_DSN", control_plane_test_dsn)
    root = tmp_path / "evidence"

    result = run_stale_quote_pointer_commissioning(
        root=root,
        release_id=RELEASE,
        config_id=CONFIG,
    )

    assert result == {
        "attack_id": "stale-quote-pointer",
        "execution_scope": "disposable-exact-image",
        "node_count": 1,
        "proof_count": 1,
        "status": "pass",
    }
    assert (
        root / "attacks/opportunity-certify/stale-quote-pointer/proof.json"
    ).is_file()
    with psycopg.connect(control_plane_test_dsn) as connection:
        databases = connection.execute(
            "SELECT datname FROM pg_database WHERE datname LIKE 'm1_commissioning_%'"
        ).fetchall()
    assert databases == []


def test_disposable_database_cleans_after_interrupted_attack_body(
    monkeypatch: pytest.MonkeyPatch,
    control_plane_test_dsn: str,
) -> None:
    monkeypatch.setenv("POLYARB_CONTROL_PLANE_TEST_DSN", control_plane_test_dsn)

    with pytest.raises(RuntimeError, match="simulated-interruption"):
        with migrated_disposable_control_plane_database():
            raise RuntimeError("simulated-interruption")

    with psycopg.connect(control_plane_test_dsn) as connection:
        databases = connection.execute(
            "SELECT datname FROM pg_database WHERE datname LIKE 'm1_commissioning_%'"
        ).fetchall()
        roles = connection.execute(
            """
            SELECT rolname FROM pg_roles
            WHERE rolname IN (
                'l3_evidence_daemon',
                'l3_retention_operator',
                'm1_runtime_controller_capability',
                'm1_qualification_worker_capability'
            )
            ORDER BY rolname
            """
        ).fetchall()
    assert databases == []
    assert roles == []


def test_disposable_database_never_drops_preexisting_cluster_roles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeMaintenance:
        def close(self) -> None:
            return None

    role_cleanup_calls: list[str] = []
    unlock_calls: list[str] = []
    monkeypatch.setenv(
        "POLYARB_CONTROL_PLANE_TEST_DSN",
        "postgresql://localhost/test",
    )
    monkeypatch.setattr(matrix_module.psycopg, "connect", lambda *args, **kwargs: FakeMaintenance())
    monkeypatch.setattr(matrix_module, "_lock_runtime_fault_matrix_cluster", lambda db: None)
    monkeypatch.setattr(
        matrix_module,
        "_assert_migration_cluster_roles_absent",
        lambda db: (_ for _ in ()).throw(
            matrix_module.RuntimeFaultMatrixError("pre-existing cluster role")
        ),
    )
    monkeypatch.setattr(
        matrix_module,
        "_drop_migration_created_cluster_roles_if_safe",
        lambda db: role_cleanup_calls.append("drop"),
    )
    monkeypatch.setattr(
        matrix_module,
        "_unlock_runtime_fault_matrix_cluster",
        lambda db: unlock_calls.append("unlock"),
    )

    with pytest.raises(matrix_module.RuntimeFaultMatrixError, match="pre-existing"):
        with migrated_disposable_control_plane_database():
            pytest.fail("unsafe cluster must not reach attack body")

    assert role_cleanup_calls == []
    assert unlock_calls == ["unlock"]


def test_disposable_database_preserves_attack_and_connection_cleanup_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class AttackFailure(RuntimeError):
        pass

    class ConnectionCleanupFailure(RuntimeError):
        pass

    class FakeMaintenance:
        def close(self) -> None:
            raise ConnectionCleanupFailure("close-failed")

    monkeypatch.setenv(
        "POLYARB_CONTROL_PLANE_TEST_DSN",
        "postgresql://localhost/test",
    )
    monkeypatch.setattr(matrix_module.psycopg, "connect", lambda *args, **kwargs: FakeMaintenance())
    for name in (
        "_lock_runtime_fault_matrix_cluster",
        "_assert_migration_cluster_roles_absent",
        "_create_empty_isolated_database_with_connection",
        "_run_alembic_upgrade",
        "_verify_migrated_authority",
        "_drop_isolated_database_with_connection",
        "_drop_migration_created_cluster_roles_if_safe",
        "_unlock_runtime_fault_matrix_cluster",
    ):
        monkeypatch.setattr(matrix_module, name, lambda *args: None)

    with pytest.raises(BaseExceptionGroup) as caught:
        with migrated_disposable_control_plane_database():
            raise AttackFailure("attack-failed")

    assert [type(error) for error in caught.value.exceptions] == [
        AttackFailure,
        ConnectionCleanupFailure,
    ]


def test_stale_owner_harness_does_not_use_production_database_environment(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("POLYARB_CONTROL_PLANE_TEST_DSN", raising=False)
    monkeypatch.setenv(
        "POLYARB_SUPABASE_DB_DSN",
        "postgresql://production.invalid/production",
    )

    with pytest.raises(CommissioningHarnessError, match="POLYARB_CONTROL_PLANE_TEST_DSN"):
        run_stale_owner_commissioning(
            root=tmp_path / "evidence",
            release_id=RELEASE,
            config_id=CONFIG,
        )

    assert os.environ["POLYARB_SUPABASE_DB_DSN"].endswith("/production")
