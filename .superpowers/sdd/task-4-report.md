# Task 4 Report: Safe Login-Role Operator Tooling

## RED

Command:

```bash
uv run pytest tests/m1-perception/test_control_plane_db_role_admin.py tests/m1-perception/test_makefile_contract.py -q
```

Output:

```text
FFFFFFFFFF............FFFFFFF........................................... [ 56%]
........................................................                 [100%]
17 failed, 111 passed
```

Expected failing reasons observed:

- `ModuleNotFoundError: No module named 'polyarb.control_plane.db_role_admin'`
- `make: *** No rule to make target 'control-plane-db-role-preflight'. Stop.`
- `make: *** No rule to make target 'control-plane-db-role-provision'. Stop.`
- `make: *** No rule to make target 'control-plane-db-role-verify'. Stop.`
- `make: *** No rule to make target 'control-plane-db-role-disable'. Stop.`

## GREEN / Verification

Command:

```bash
uv run pytest tests/m1-perception/test_control_plane_db_role_admin.py tests/m1-perception/test_makefile_contract.py -q
```

Output:

```text
........................................................................ [ 56%]
........................................................                 [100%]
```

This includes the disposable PostgreSQL 16/testcontainers coverage for revision
`026`, creating both logins, proving scoped daemon verification, rotating the
runtime-controller password independently, keeping the qualification-worker
credential valid, and disabling both logins with `NOLOGIN`.

Command:

```bash
make help | rg "control-plane-db-role-(preflight|provision|verify|disable)"
```

Output:

```text
  control-plane-db-role-disable: Disable both scoped logins after both apps are stopped; requires enable=1; never downgrades schema.
  control-plane-db-role-preflight: Read-only check that revision 026 capability roles are safe; no login/secret mutation.
  control-plane-db-role-provision: Explicitly create/rotate the two scoped DB logins; requires enable=1 and password env vars; never contacts Fly.
  control-plane-db-role-verify: Read-only effective-permission proof for profile=runtime-controller|qualification-worker.
```

Command:

```bash
make control-plane-db-role-provision expected_database=control
```

Output:

```text
usage: make control-plane-db-role-provision enable=1 expected_database=<name>
make: *** [control-plane-db-role-provision] Error 2
```

Command:

```bash
make control-plane-db-role-disable expected_database=control
```

Output:

```text
usage: make control-plane-db-role-disable enable=1 expected_database=<name>
make: *** [control-plane-db-role-disable] Error 2
```

Command:

```bash
uv run ruff check src/polyarb/control_plane/db_role_admin.py src/polyarb/control_plane/db_role_contract.py tests/m1-perception/test_control_plane_db_role_admin.py tests/m1-perception/test_makefile_contract.py
```

Output:

```text
All checks passed!
```

Command:

```bash
uv run pyright src/polyarb/control_plane/db_role_admin.py src/polyarb/control_plane/db_role_contract.py tests/m1-perception/test_control_plane_db_role_admin.py tests/m1-perception/test_makefile_contract.py
```

Output:

```text
0 errors, 0 warnings, 0 informations
```

Command:

```bash
uv run python -m py_compile src/polyarb/control_plane/db_role_admin.py src/polyarb/control_plane/db_role_contract.py tests/m1-perception/test_control_plane_db_role_admin.py tests/m1-perception/test_makefile_contract.py
```

Output: no output; exit code `0`.

Command:

```bash
git diff --check -- src/polyarb/control_plane/db_role_admin.py src/polyarb/control_plane/db_role_contract.py tests/m1-perception/test_control_plane_db_role_admin.py tests/m1-perception/test_makefile_contract.py Makefile docs/dev/control-plane-runbook.md
```

Output: no output; exit code `0`.

## Changes

- Added `src/polyarb/control_plane/db_role_admin.py` with fail-closed
  preflight/provision/verify/disable operations.
- Added `tests/m1-perception/test_control_plane_db_role_admin.py` covering
  missing enable, database/revision mismatch, unsafe attrs, membership mismatch,
  empty/equal passwords, rotation idempotency, transaction rollback,
  secret-free output, scoped verification, and real PostgreSQL 16 behavior.
- Added Make targets for `control-plane-db-role-preflight`,
  `control-plane-db-role-provision`, `control-plane-db-role-verify`, and
  `control-plane-db-role-disable`.
- Added `docs/dev/control-plane-runbook.md` with Keychain handling, independent
  Fly secrets, stop-before-disable order, and production authorization boundary.
- Fixed `src/polyarb/control_plane/db_role_contract.py` sequence-name query to
  escape literal percent markers for psycopg.

## Self-Review

- Role names are passed only through `psycopg.sql.Identifier`.
- Passwords are passed only through `psycopg.sql.Literal`, matching the approved
  PostgreSQL 16 exception for `ALTER ROLE ... PASSWORD`.
- No composed SQL, password, DSN, auth value, provider response, or Fly secret is
  printed or logged by the new CLI.
- Provision mutates both login roles in one transaction and rolls back on any
  failure before commit.
- The new module does not create or alter capability-role contracts; it only
  checks revision `026` capability roles and manages the two exact login roles.
- Task 3 `control-plane-render-rollout --release-id` Make entry remains intact.
- `.superpowers/sdd/progress.md` remains unstaged.

## Commit

Committed with subject:

```text
feat(05.6-207): add scoped database role operations
```

## Concerns

- `disable` intentionally fails closed if either login is already `NOLOGIN`,
  because the safety helper treats non-login login roles as an unexpected manual
  state before mutation.

## Review Fix: Recoverable scoped login disable/provision

### RED

Command:

```bash
tmp_review_fix=$(mktemp -d)
mkdir -p "$tmp_review_fix/src"
cp -R src/polyarb "$tmp_review_fix/src/"
git show 95e047a9:src/polyarb/control_plane/db_role_admin.py > "$tmp_review_fix/src/polyarb/control_plane/db_role_admin.py"
PYTHONPATH="$tmp_review_fix/src" uv run pytest tests/m1-perception/test_control_plane_db_role_admin.py::test_disable_is_repeatable_when_both_roles_are_already_nologin tests/m1-perception/test_control_plane_db_role_admin.py::test_disable_accepts_one_role_already_nologin_and_disables_the_other tests/m1-perception/test_control_plane_db_role_admin.py::test_provision_accepts_clean_nologin_roles_and_restores_login -q
```

Output:

```text
FFF                                                                      [100%]
=================================== FAILURES ===================================
________ test_disable_is_repeatable_when_both_roles_are_already_nologin ________

E       polyarb.control_plane.db_role_admin.DatabaseRoleAdminError: database-role-admin.login-unsafe: m1_runtime_controller_login

_____ test_disable_accepts_one_role_already_nologin_and_disables_the_other _____

E       polyarb.control_plane.db_role_admin.DatabaseRoleAdminError: database-role-admin.login-unsafe: m1_runtime_controller_login

________ test_provision_accepts_clean_nologin_roles_and_restores_login _________

E       polyarb.control_plane.db_role_admin.DatabaseRoleAdminError: database-role-admin.login-unsafe: m1_runtime_controller_login

=========================== short test summary info ============================
FAILED tests/m1-perception/test_control_plane_db_role_admin.py::test_disable_is_repeatable_when_both_roles_are_already_nologin
FAILED tests/m1-perception/test_control_plane_db_role_admin.py::test_disable_accepts_one_role_already_nologin_and_disables_the_other
FAILED tests/m1-perception/test_control_plane_db_role_admin.py::test_provision_accepts_clean_nologin_roles_and_restores_login
```

### GREEN / Verification

Command:

```bash
uv run pytest tests/m1-perception/test_control_plane_db_role_admin.py tests/m1-perception/test_makefile_contract.py -q
```

Output:

```text
............................................ [ 54%]
...........................................................              [100%]
```

Command:

```bash
uv run ruff check src/polyarb/control_plane/db_role_admin.py tests/m1-perception/test_control_plane_db_role_admin.py tests/m1-perception/test_makefile_contract.py
```

Output:

```text
All checks passed!
```

Command:

```bash
uv run pyright src/polyarb/control_plane/db_role_admin.py tests/m1-perception/test_control_plane_db_role_admin.py tests/m1-perception/test_makefile_contract.py
```

Output:

```text
0 errors, 0 warnings, 0 informations
```

Command:

```bash
uv run python -m py_compile src/polyarb/control_plane/db_role_admin.py tests/m1-perception/test_control_plane_db_role_admin.py tests/m1-perception/test_makefile_contract.py
```

Output: no output; exit code `0`.

Command:

```bash
git diff --check -- src/polyarb/control_plane/db_role_admin.py tests/m1-perception/test_control_plane_db_role_admin.py tests/m1-perception/test_makefile_contract.py
```

Output: no output; exit code `0`.

### Review-fix changes

- Decoupled exact safe role attrs/membership checks from `rolcanlogin` so clean
  `NOLOGIN` login roles are still considered operator-safe.
- Made repeated `disable_login_roles()` idempotent and allowed mixed
  `LOGIN`/`NOLOGIN` exact-membership states to converge to both roles disabled
  in one transaction.
- Allowed `provision_login_roles()` to restore/rotate previously disabled exact
  roles, with fake and real PostgreSQL coverage proving both scoped DSNs connect
  after reprovision.

### Remaining concerns

- None for this review item.
