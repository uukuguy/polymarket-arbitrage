# M1 Supabase Reachable Authority Implementation Plan

> **Execution mode:** inline TDD. Production revision `026` remains blocked until a new exact release authorization is rendered and approved.

**Goal:** Make revision `026` and the daemon database-role verifier distinguish effective, reachable authority from an object ACL in a schema the role cannot use, without weakening any namespace, direct-grant, ownership, membership, or search-path gate.

**Architecture:** The existing all-schema namespace pass remains authoritative: only `public` may be usable and it may not be creatable. Relation, sequence, and SECURITY DEFINER effective-authority enumeration is then restricted to schemas for which the subject role has `USAGE`. Direct ACL inspection and ownership checks continue to scan every non-system schema.

**Tech Stack:** Python 3.12, PostgreSQL 16, Alembic, psycopg 3, pytest/testcontainers, uv.

## Constraints

- The first production `026` attempt is immutable evidence and failed closed at revision `025`; do not retry it under release `d050c8290c52e07acb72c8db7fe3fb02072d126c`.
- Do not revoke Supabase provider-owned PUBLIC privileges or hard-code provider object names.
- Do not hide direct grants, ownership, role membership, database/schema CREATE, non-public schema USAGE, or search-path authority.
- No DSN, password, provider response body, or secret value may enter tests, evidence, stdout, or commits.
- Use red-green-refactor and commit the code correction atomically.

### Task 1: Prove the reachability boundary in tests

**Files:**
- Modify: `tests/alembic/test_026.py`
- Modify: `tests/m1-perception/test_control_plane_db_role_contract.py`

- [ ] Add static contracts requiring the three effective-object catalog queries in both migration and daemon verifier to include a subject-specific `has_schema_privilege(..., 'USAGE')` predicate.
- [ ] Add a real PostgreSQL regression fixture with a PUBLIC-readable object in an `extensions` schema that PUBLIC cannot use; prove `026` accepts it, but rejects the same ACL after schema USAGE becomes effective.
- [ ] Add a fake-verifier regression proving an inaccessible ambient relation is omitted by the catalog query while an explicitly reachable non-public schema remains rejected by the independent namespace gate.
- [ ] Run the focused tests and record the expected RED failures.

### Task 2: Implement reachable effective-authority enumeration

**Files:**
- Modify: `alembic/versions/026_m1_runtime_scoped_roles.py`
- Modify: `src/polyarb/control_plane/db_role_contract.py`

- [ ] Add `has_schema_privilege(subject, namespace.oid, 'USAGE')` to relation, sequence, and SECURITY DEFINER catalog enumeration.
- [ ] Parameterize the daemon verifier queries with `subject_role`; never interpolate runtime role data.
- [ ] Preserve all-schema namespace, direct-ACL, ownership, membership, database, and search-path gates unchanged.
- [ ] Run focused unit and real-PostgreSQL tests to GREEN, then run the complete scoped-role gate set.
- [ ] Commit the correction atomically.

### Task 3: Close failed-attempt evidence and re-authorize exact production release

**Files:**
- Modify: `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/evidence/authorization-d050c829-observe-only/authorization-request.json`
- Create: `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/evidence/authorization-d050c829-observe-only/revision-026-first-attempt-failed-closed.json`
- Create: a new exact-SHA authorization directory under the same phase evidence tree
- Modify: `.planning/JOURNAL.md`
- Modify: `.planning/workstreams/m1-perception/STATE.md`

- [ ] Record the secret-free failed-closed evidence: revision remains `025`, scoped roles are absent, and no partial schema or role mutation survived.
- [ ] Run all local release gates and render a fresh exact-SHA authorization package.
- [ ] Obtain explicit authorization for that exact package before any second production migration or deployment.
- [ ] After authorization, migrate, provision scoped logins, deploy the two private observe-only apps, and begin rolling qualification under the existing M1 production plan.
