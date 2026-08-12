"""Read-only exporter from a sealed legacy Structure generation to R2 input."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from .structure_artifact import StructureBundleIdentity


class StructureShadowRefusal(RuntimeError):
    """The legacy publication is not safe to use as transactional shadow input."""


def read_legacy_structure_bundle(
    db_path: Path | str,
    *,
    publication_id: str,
) -> tuple[StructureBundleIdentity, dict[str, tuple[dict[str, object], ...]]]:
    """Export only the exact authenticated legacy current generation, read-only."""
    if not publication_id:
        raise ValueError("publication_id must be non-empty")
    path = Path(db_path).resolve()
    try:
        with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as connection:
            connection.row_factory = sqlite3.Row
            publication = connection.execute(
                """
                SELECT publication.snapshot_id, publication.window_id,
                       publication.normalization_contract_version,
                       publication.committed_counts_json,
                       current.comparison_receipt_digest
                FROM structure_publications AS publication
                JOIN current_structure_generation AS current
                  ON current.publication_id = publication.publication_id
                 AND current.snapshot_id = publication.snapshot_id
                WHERE publication.publication_id = ?
                  AND publication.status = 'published'
                """,
                (publication_id,),
            ).fetchone()
            if publication is None:
                raise StructureShadowRefusal("legacy-structure-publication-not-current")
            receipt_digest = publication[4]
            if not isinstance(receipt_digest, str) or len(receipt_digest) != 64:
                raise StructureShadowRefusal("legacy-structure-comparison-receipt-unavailable")
            counts = json.loads(str(publication[3]))
            identity = StructureBundleIdentity(
                publication_id=publication_id,
                window_id=str(publication[1]),
                snapshot_id=int(publication[0]),
                comparison_receipt_digest=receipt_digest,
                normalization_contract_version=str(publication[2]),
                component_counts=counts,
            )
            components = {
                component: _read_component(connection, component, identity.snapshot_id)
                for component in identity.component_counts
            }
    except sqlite3.Error as error:
        raise StructureShadowRefusal("legacy-structure-export-unavailable") from error
    return identity, components


def _read_component(
    connection: sqlite3.Connection,
    component: str,
    snapshot_id: int,
) -> tuple[dict[str, object], ...]:
    tables: dict[str, tuple[str, str]] = {
        "events": ("events", "id"),
        "event_tags": ("event_tags", "event_id,tag_id"),
        "memberships": ("event_market_memberships", "event_id,market_id"),
        "group_truth": ("neg_risk_group_truth", "neg_risk_market_id"),
        "markets": ("markets", "market_id"),
        "issues": ("validation_issues", "id"),
    }
    table, ordering = tables[component]
    rows = connection.execute(
        f"SELECT * FROM {table} WHERE snapshot_id=? ORDER BY {ordering}",  # noqa: S608
        (snapshot_id,),
    ).fetchall()
    return tuple(_json_row(row) for row in rows)


def _json_row(row: sqlite3.Row) -> dict[str, object]:
    result: dict[str, object] = {}
    for key in row.keys():
        value = row[key]
        if isinstance(value, bytes):
            raise StructureShadowRefusal("legacy-structure-binary-column-refused")
        result[str(key)] = value
    return result
