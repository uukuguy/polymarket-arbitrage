"""Contracts for the read-only SQLite-to-control-plane shadow projection."""

from __future__ import annotations

import os
import sqlite3
import subprocess
from collections.abc import Iterator
from datetime import UTC, datetime

import psycopg
import pytest

from polyarb.control_plane.postgres import PostgresControlPlane
from polyarb.control_plane.shadow import (
    ShadowSource,
    project_shadow_sources,
    read_shadow_sources,
    shadow_identity,
)


@pytest.fixture(scope="module")
def postgres_dsn() -> Iterator[str]:
    if subprocess.run(["docker", "info"], capture_output=True, timeout=5).returncode != 0:
        pytest.skip("Docker daemon unavailable; shadow integration tests skipped")
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as postgres:
        dsn = postgres.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")
        with psycopg.connect(dsn, autocommit=True) as connection:
            for role in ("anon", "authenticated", "service_role"):
                connection.execute(f"CREATE ROLE {role} NOLOGIN")
        result = subprocess.run(
            ["uv", "run", "alembic", "upgrade", "009"],
            env={**os.environ, "POLYARB_SUPABASE_DB_DSN": dsn},
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert result.returncode == 0, result.stderr
        yield dsn


def test_shadow_identities_are_source_deterministic_and_do_not_name_pointers() -> None:
    structure = ShadowSource.structure_publication("publication-892", "issues:537")
    quote = ShadowSource.quote_attempt(4312)
    incident = ShadowSource.incident("incident-17", 4)

    assert shadow_identity(structure) == "sqlite:structure-publication:publication-892:issues:537"
    assert shadow_identity(quote) == "sqlite:quote-attempt:4312"
    assert shadow_identity(incident) == "sqlite:incident:incident-17:4"
    assert "pointer" not in shadow_identity(structure)


def test_reader_extracts_bounded_source_facts_without_mutating_sqlite(tmp_path) -> None:
    db_path = tmp_path / "state.db"
    with sqlite3.connect(db_path) as connection:
        connection.executescript(
            """
            CREATE TABLE structure_publications (
                publication_id TEXT, write_component TEXT, write_row_cursor TEXT
            );
            CREATE TABLE neg_risk_quote_attempts (id INTEGER, outcome TEXT);
            CREATE TABLE neg_risk_incident_events (
                id INTEGER PRIMARY KEY, incident_id TEXT, sequence INTEGER
            );
            INSERT INTO structure_publications VALUES ('pub-892', 'issues', '537');
            INSERT INTO neg_risk_quote_attempts VALUES (4312, 'failed');
            INSERT INTO neg_risk_incident_events VALUES (1, 'incident-17', 4);
            """
        )

    assert read_shadow_sources(db_path, limit=2) == (
        ShadowSource.structure_publication("pub-892", "issues:537"),
        ShadowSource.quote_attempt(4312),
        ShadowSource.incident("incident-17", 4),
    )
    with sqlite3.connect(db_path) as connection:
        assert connection.execute("SELECT count(*) FROM structure_publications").fetchone() == (1,)


def test_projection_is_idempotent_and_never_creates_publication_pointers(postgres_dsn: str) -> None:
    control_plane = PostgresControlPlane(lambda: psycopg.connect(postgres_dsn))
    sources = (
        ShadowSource.structure_publication("pub-892", "issues:537"),
        ShadowSource.quote_attempt(4312),
        ShadowSource.incident("incident-17", 4),
    )
    now = datetime(2030, 1, 1, tzinfo=UTC)

    assert project_shadow_sources(sources, control_plane=control_plane, now=now) == 3
    assert project_shadow_sources(sources, control_plane=control_plane, now=now) == 3

    with psycopg.connect(postgres_dsn) as connection:
        assert connection.execute("SELECT count(*) FROM m1_jobs").fetchone() == (3,)
        assert connection.execute("SELECT count(*) FROM m1_publication_pointers").fetchone() == (0,)
