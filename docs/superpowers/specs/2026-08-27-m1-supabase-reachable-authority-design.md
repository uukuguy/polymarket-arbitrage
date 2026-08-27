# M1 Supabase Reachable Authority Design

**Date:** 2026-08-27

**Status:** Approved under the user's explicit full authorization

**Scope:** Repair revision 026 and the matching daemon/admin verifiers after the
first authorized production migration attempt failed closed on Supabase-managed
ambient object ACLs.

## Context

Production remained atomically at revision 025 after revision 026 raised
`m1_runtime_controller_capability authority envelope is not exact`. Read-only
catalog evidence isolated the only production/fixture difference:

- `extensions.pg_stat_statements` and
  `extensions.pg_stat_statements_info` grant `SELECT` to `PUBLIC`;
- `PUBLIC` and the proposed capability roles have no `USAGE` or `CREATE` on the
  `extensions` schema;
- no other non-system schema grants `PUBLIC` namespace authority;
- no non-system sequence grants `PUBLIC` authority;
- no non-system `SECURITY DEFINER` routine grants `PUBLIC EXECUTE`.

PostgreSQL reports the object ACL through `has_table_privilege` even though a
session cannot resolve or access the object without schema `USAGE`. The original
closed-envelope loop therefore confused an inert object ACL with executable
authority. The disposable PostgreSQL fixture had no Supabase extension views,
so it could not reproduce the mismatch.

## Considered Approaches

### 1. Reachability-aware effective authority — selected

Keep the existing all-schema namespace gate. A non-public schema must expose
neither `USAGE` nor `CREATE` to the subject role. Enumerate relation, sequence,
and `SECURITY DEFINER` authority only inside schemas where the subject has
`USAGE`. This matches PostgreSQL execution semantics: object privilege and
schema reachability are both required.

### 2. Allowlist the two Supabase views — rejected

An object-name allowlist would bind the application contract to the current
Supabase extension version and require code changes whenever managed views
change. It would encode provider inventory instead of authority.

### 3. Revoke the ambient `PUBLIC SELECT` grants — rejected

Global revocation would mutate provider-managed extension policy and could
break the original four apps or Supabase tooling. PostgreSQL has no per-role
deny that can override a `PUBLIC` grant.

## Contract

The closed authority envelope is the conjunction of these rules:

1. The subject must have `CONNECT` to the expected database and `USAGE` on
   `public`.
2. Database `CREATE`, `public CREATE`, and `USAGE` or `CREATE` on every other
   non-system schema are forbidden.
3. Within every schema the subject can use, relation privileges must equal the
   reviewed table allowlist, sequence privileges must be empty, and
   `SECURITY DEFINER EXECUTE` must equal the reviewed routine allowlist.
4. Direct ACLs to capability/login roles remain an exact closed set even when
   the target schema is unreachable. Ambient `PUBLIC` ACLs are not direct ACLs.
5. Ownership, role membership, membership options, database/role search-path
   settings, unsafe attributes, database CREATE, and DSN namespace overrides
   remain rejected across all non-system namespaces.
6. Application SQL remains `public.`-qualified and connections retain the fixed
   `pg_catalog,public` search path.

Consequently, a provider-managed object with ambient `PUBLIC SELECT` is inert
when its schema is unreachable. If `USAGE` later appears on that schema, the
namespace gate fails before the object loop can normalize the drift away.

## Changes

- `alembic/versions/026_m1_runtime_scoped_roles.py`: add a role-specific schema
  `USAGE` predicate to relation, sequence, and routine loops inside the final
  effective-authority assertion.
- `src/polyarb/control_plane/db_role_contract.py`: make the startup verifier use
  the same reachable-schema projection. Keep its separate all-schema namespace
  pass unchanged.
- `tests/alembic/test_026.py`: reproduce Supabase ambient extension views and
  prove 026 passes when the schema is unreachable, then fails when schema
  `USAGE` makes the ambient object ACL executable.
- `tests/m1-perception/test_control_plane_db_role_contract.py`: prove the daemon
  verifier ignores object ACLs only when the corresponding schema is
  unreachable and still rejects the same ACL when schema `USAGE` is present.
- `docs/learning/89-数据库能力角色与进程身份.md`: explain namespace reachability
  as part of effective authority.

## Failure and Rollback Semantics

- The failed production attempt is immutable evidence; it performed no partial
  migration and production remains at 025.
- No second production attempt may use the old `d050c829` authorization. The
  corrected executable receives a new Git SHA, a new local gate, and a refreshed
  exact authorization package.
- A second migration failure must again leave production at 025 and stop the
  rollout. No manual role/schema cleanup is permitted outside Alembic.
- The runtime-event-writer credential remediation already completed and is
  independent of revision 026 rollback.

## Verification

1. A real PostgreSQL 16 test creates a non-public `extensions` schema, grants
   `SELECT` on two views to `PUBLIC`, leaves schema `USAGE` absent, and completes
   the 025→026 migration plus scoped role verification.
2. Granting `USAGE` on that schema makes the exact migration/startup gate reject
   the authority envelope.
3. Existing adversarial tests for direct unrelated grants, ownership, extra
   membership, search-path overrides, database CREATE, non-public namespace
   authority, sequences, and `SECURITY DEFINER` functions remain green.
4. The complete Plan 207 gate, Ruff, Pyright, build, planning-status, and a fresh
   climb cycle pass before a new production authorization is requested.
