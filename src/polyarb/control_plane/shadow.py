"""Read-only source identities for SQLite-to-control-plane projection."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from .postgres import PostgresControlPlane


@dataclass(frozen=True, slots=True)
class ShadowSource:
    """One immutable source fact that may be mirrored, never promoted."""

    kind: str
    identity: str

    @classmethod
    def structure_publication(cls, publication_id: str, cursor: str) -> ShadowSource:
        return cls("structure-publication", f"{publication_id}:{cursor}")

    @classmethod
    def quote_attempt(cls, attempt_id: int) -> ShadowSource:
        return cls("quote-attempt", str(attempt_id))

    @classmethod
    def incident(cls, incident_id: str, sequence: int) -> ShadowSource:
        return cls("incident", f"{incident_id}:{sequence}")


def shadow_identity(source: ShadowSource) -> str:
    """Return the deterministic, SQLite-scoped durable identity."""
    if not source.kind or not source.identity:
        raise ValueError("shadow source must have a non-empty kind and identity")
    return f"sqlite:{source.kind}:{source.identity}"


def read_shadow_sources(db_path: Path | str, *, limit: int = 100) -> tuple[ShadowSource, ...]:
    """Read a bounded evidence slice without acquiring a SQLite write lock."""
    if limit < 1 or limit > 500:
        raise ValueError("limit must be in 1..500")
    path = Path(db_path).resolve()
    sources: list[ShadowSource] = []
    with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as connection:
        publications = connection.execute(
            """
            SELECT publication_id, write_component, write_row_cursor
            FROM structure_publications
            WHERE write_component IS NOT NULL AND write_row_cursor IS NOT NULL
            ORDER BY rowid DESC LIMIT ?
            """,
            (limit,),
        ).fetchall()
        quotes = connection.execute(
            """
            SELECT id FROM neg_risk_quote_attempts
            ORDER BY id DESC LIMIT ?
            """,
            (limit,),
        ).fetchall()
        incidents = connection.execute(
            """
            SELECT incident_id, sequence FROM neg_risk_incident_events
            ORDER BY id DESC LIMIT ?
            """,
            (limit,),
        ).fetchall()
    sources.extend(
        ShadowSource.structure_publication(str(publication_id), f"{component}:{cursor}")
        for publication_id, component, cursor in publications
    )
    sources.extend(ShadowSource.quote_attempt(int(attempt_id)) for (attempt_id,) in quotes)
    sources.extend(
        ShadowSource.incident(str(incident_id), int(sequence))
        for incident_id, sequence in incidents
    )
    return tuple(sources)


def project_shadow_sources(
    sources: tuple[ShadowSource, ...],
    *,
    control_plane: PostgresControlPlane,
    now: datetime,
) -> int:
    """Idempotently project source facts into jobs without switching pointers."""
    for source in sources:
        identity = shadow_identity(source)
        control_plane.enqueue_job(
            job_key=identity,
            job_type=f"shadow:{source.kind}",
            input_identity=identity,
            now=now,
        )
    return len(sources)
