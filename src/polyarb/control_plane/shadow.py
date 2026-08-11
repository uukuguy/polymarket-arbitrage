"""Read-only source identities for SQLite-to-control-plane projection."""

from __future__ import annotations

from dataclasses import dataclass


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
