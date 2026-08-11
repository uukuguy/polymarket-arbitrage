"""Contracts for the read-only SQLite-to-control-plane shadow projection."""

from __future__ import annotations

import sqlite3

from polyarb.control_plane.shadow import ShadowSource, read_shadow_sources, shadow_identity


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
