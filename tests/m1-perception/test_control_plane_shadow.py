"""Contracts for the read-only SQLite-to-control-plane shadow projection."""

from __future__ import annotations

from polyarb.control_plane.shadow import ShadowSource, shadow_identity


def test_shadow_identities_are_source_deterministic_and_do_not_name_pointers() -> None:
    structure = ShadowSource.structure_publication("publication-892", "issues:537")
    quote = ShadowSource.quote_attempt(4312)
    incident = ShadowSource.incident("incident-17", 4)

    assert shadow_identity(structure) == "sqlite:structure-publication:publication-892:issues:537"
    assert shadow_identity(quote) == "sqlite:quote-attempt:4312"
    assert shadow_identity(incident) == "sqlite:incident:incident-17:4"
    assert "pointer" not in shadow_identity(structure)
