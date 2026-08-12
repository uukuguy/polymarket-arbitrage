from __future__ import annotations

import sqlite3

import pytest

from polyarb.control_plane.structure_artifact import canonical_structure_bundle_bytes
from polyarb.control_plane.structure_shadow import (
    StructureShadowRefusal,
    read_legacy_structure_bundle,
)


def _seed(path, *, current: bool = True, receipt: str = "a" * 64) -> None:
    with sqlite3.connect(path) as con:
        con.executescript(
            """
            CREATE TABLE structure_publications (
                publication_id TEXT, snapshot_id INTEGER, window_id TEXT, status TEXT,
                normalization_contract_version TEXT, committed_counts_json TEXT
            );
            CREATE TABLE current_structure_generation (
                publication_id TEXT, snapshot_id INTEGER, comparison_receipt_digest TEXT
            );
            CREATE TABLE events (snapshot_id INTEGER, id TEXT, title TEXT);
            CREATE TABLE event_tags (snapshot_id INTEGER, event_id TEXT, tag_id TEXT);
            CREATE TABLE event_market_memberships (
                snapshot_id INTEGER, event_id TEXT, market_id TEXT
            );
            CREATE TABLE neg_risk_group_truth (snapshot_id INTEGER, neg_risk_market_id TEXT);
            CREATE TABLE markets (snapshot_id INTEGER, market_id TEXT);
            CREATE TABLE validation_issues (snapshot_id INTEGER, id INTEGER, detail TEXT);
            """
        )
        con.execute(
            "INSERT INTO structure_publications VALUES ('p',42,'w','published','v7',?)",
            ('{"events":1,"event_tags":0,"memberships":0,"group_truth":0,"markets":1,"issues":0}',),
        )
        if current:
            con.execute("INSERT INTO current_structure_generation VALUES ('p',42,?)", (receipt,))
        con.execute("INSERT INTO events VALUES (42,'event-a','A')")
        con.execute("INSERT INTO markets VALUES (42,'market-a')")


def test_exporter_reads_only_authenticated_current_legacy_generation(tmp_path) -> None:
    path = tmp_path / "state.db"
    _seed(path)

    identity, components = read_legacy_structure_bundle(path, publication_id="p")

    assert identity.publication_id == "p"
    assert identity.snapshot_id == 42
    assert components["events"] == ({"snapshot_id": 42, "id": "event-a", "title": "A"},)
    assert components["markets"] == ({"snapshot_id": 42, "market_id": "market-a"},)
    assert b'"publication_id":"p"' in canonical_structure_bundle_bytes(
        identity=identity, components=components
    )


def test_exporter_refuses_noncurrent_or_unsealed_legacy_generation(tmp_path) -> None:
    path = tmp_path / "state.db"
    _seed(path, current=False)

    with pytest.raises(StructureShadowRefusal, match="not-current"):
        read_legacy_structure_bundle(path, publication_id="p")
