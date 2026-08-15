# H-014 — transactional opportunity projection

## Goal

Replace the legacy SQLite-only opportunity resource with a Postgres-authoritative
projection derived only from certified Structure and Quote generations.

## Invariants

- No SQLite, legacy daemon, or live CLOB reread in the formal API path.
- A missing/corrupt projection is a structured 503, never an empty opportunity list.
- The final current pointer changes atomically only after a complete authenticated projection.

## First delivery

Expose the fail-closed formal API boundary; then add the projection schema,
certifier/worker, and repository read model under TDD.
