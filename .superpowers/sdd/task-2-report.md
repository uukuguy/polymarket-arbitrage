# Plan 05.6-207 Task 2 Report

## Outcome

Implemented the fail-closed daemon database identity contract for the runtime
controller and qualification worker profiles.

- Added `src/polyarb/control_plane/db_role_contract.py` with immutable
  `runtime-controller` and `qualification-worker` profiles, exact revision-026
  capability role names, exact login role names, explicit table/function
  allowlists, positive privilege checks, forbidden privilege checks, database
  identity checks, unsafe role-attribute checks, cross-role checks, and
  sanitized `DatabaseRoleContractError` surfaces.
- Gated `runtime-reconcile-once`, `runtime-reconcile-serve`, and
  `qualification-serve` after connection-factory creation and before claim,
  service construction, observe/freshness/epoch writes, or service-loop work.
- Kept `runtime-controller-status`, `runtime-observe-verify`,
  `qualification-status`, and `qualification-certificates` usable without
  `POLYARB_DB_EXPECTED_DATABASE`.

## RED

Command:

```text
uv run pytest tests/m1-perception/test_control_plane_db_role_contract.py tests/m1-perception/test_control_plane_cli.py -q
```

Output summary:

```text
FFFFFFFFF.........................FFF..........................FF.       [100%]
14 failed, 52 passed
```

Expected failures observed:

- `ModuleNotFoundError: No module named 'polyarb.control_plane.db_role_contract'`
  for the new verifier contract tests and role-contract CLI failure tests.
- Runtime reconcile once/serve ordering tests saw `events == ["claim"]` instead
  of `["verify", "claim"]`.
- Qualification serve ordering test saw `events == ["service", "tick"]` instead
  of `["verify", "service", "tick"]`.

## GREEN / Verification

Focused behavior and runtime observe regression:

```text
uv run pytest tests/m1-perception/test_control_plane_db_role_contract.py tests/m1-perception/test_control_plane_cli.py tests/m1-perception/test_control_plane_runtime_observe.py -q
........................................................................ [100%]
```

Lint:

```text
uv run ruff check src/polyarb/control_plane/db_role_contract.py src/polyarb/cli_control_plane.py tests/m1-perception/test_control_plane_db_role_contract.py tests/m1-perception/test_control_plane_cli.py
All checks passed!
```

Syntax:

```text
uv run python -m py_compile src/polyarb/control_plane/db_role_contract.py src/polyarb/cli_control_plane.py tests/m1-perception/test_control_plane_db_role_contract.py tests/m1-perception/test_control_plane_cli.py
```

Pyright:

```text
uv run pyright src/polyarb/control_plane/db_role_contract.py src/polyarb/cli_control_plane.py tests/m1-perception/test_control_plane_db_role_contract.py tests/m1-perception/test_control_plane_cli.py
0 errors, 0 warnings, 0 informations
```

Diff check:

```text
git diff --check -- src/polyarb/control_plane/db_role_contract.py src/polyarb/cli_control_plane.py tests/m1-perception/test_control_plane_db_role_contract.py tests/m1-perception/test_control_plane_cli.py
```

Exit status: `0`.

## Self-Review

- Verified every contract rejection path uses fake read-only connections and
  records zero write-shaped SQL.
- Verified daemon gates run before runtime lease claim and qualification
  service construction.
- Verified role-contract CLI errors expose only reason code plus object
  identifier, and do not expose connection strings or credential material.
- Verified the verifier uses a transaction plus `SET TRANSACTION READ ONLY`.
- Checked `git diff --check` and searched modified files for debug leftovers.

## Commit

Committed with subject:

```text
feat(05.6-207): fail closed on daemon database identity
```

## Concerns

- No production database was connected to or mutated.
- `.superpowers/sdd/progress.md` had a pre-existing unstaged modification and
  was not staged.

## Review Fix: Complete Role Envelope

Addressed both REQUEST CHANGES findings from `.superpowers/sdd/task-2-review.md`.

- Added explicit forbidden sequence checks with `pg_get_serial_sequence` and
  `has_sequence_privilege` for runtime-controller sequence-bearing table
  columns if present and the qualification ledger `ingest_seq` identity
  sequence.
- Added full login/capability role attribute snapshots. Login roles must be
  `LOGIN INHERIT` with safe attributes; capability roles must be
  `NOLOGIN NOINHERIT` with the same safe attributes.
- Added targeted fake-contract tests for forbidden sequence privileges from
  public/inherited effective access, login `NOLOGIN`, login `NOINHERIT`,
  capability `LOGIN`, capability `INHERIT`, and capability unsafe attributes.

### RED

Command:

```text
uv run pytest tests/m1-perception/test_control_plane_db_role_contract.py -q
```

Output:

```text
.........FFFFFFFFF                                                       [100%]
=================================== FAILURES ===================================
FAILED tests/m1-perception/test_control_plane_db_role_contract.py::test_database_role_contract_rejects_forbidden_sequence_privilege[runtime-controller-public]
FAILED tests/m1-perception/test_control_plane_db_role_contract.py::test_database_role_contract_rejects_forbidden_sequence_privilege[runtime-controller-inherited]
FAILED tests/m1-perception/test_control_plane_db_role_contract.py::test_database_role_contract_rejects_forbidden_sequence_privilege[qualification-worker-public]
FAILED tests/m1-perception/test_control_plane_db_role_contract.py::test_database_role_contract_rejects_forbidden_sequence_privilege[qualification-worker-inherited]
FAILED tests/m1-perception/test_control_plane_db_role_contract.py::test_database_role_contract_rejects_role_attribute_violations[login-nologin-database-role.login-attribute]
FAILED tests/m1-perception/test_control_plane_db_role_contract.py::test_database_role_contract_rejects_role_attribute_violations[login-noinherit-database-role.login-attribute]
FAILED tests/m1-perception/test_control_plane_db_role_contract.py::test_database_role_contract_rejects_role_attribute_violations[capability-login-database-role.capability-attribute]
FAILED tests/m1-perception/test_control_plane_db_role_contract.py::test_database_role_contract_rejects_role_attribute_violations[capability-inherit-database-role.capability-attribute]
FAILED tests/m1-perception/test_control_plane_db_role_contract.py::test_database_role_contract_rejects_role_attribute_violations[capability-unsafe-database-role.capability-attribute]
9 failed, 9 passed
```

Each failure was `Failed: DID NOT RAISE`, confirming the previous verifier did
not enforce the reviewed sequence and role-attribute envelopes.

### GREEN / Verification

Focused contract tests:

```text
uv run pytest tests/m1-perception/test_control_plane_db_role_contract.py -q
..................                                                       [100%]
```

Focused behavior and runtime observe regression:

```text
uv run pytest tests/m1-perception/test_control_plane_db_role_contract.py tests/m1-perception/test_control_plane_cli.py tests/m1-perception/test_control_plane_runtime_observe.py -q
........................................................................ [ 88%]
.........                                                                [100%]
```

Lint:

```text
uv run ruff check src/polyarb/control_plane/db_role_contract.py src/polyarb/cli_control_plane.py tests/m1-perception/test_control_plane_db_role_contract.py tests/m1-perception/test_control_plane_cli.py
All checks passed!
```

Pyright:

```text
uv run pyright src/polyarb/control_plane/db_role_contract.py src/polyarb/cli_control_plane.py tests/m1-perception/test_control_plane_db_role_contract.py tests/m1-perception/test_control_plane_cli.py
0 errors, 0 warnings, 0 informations
```

Syntax:

```text
uv run python -m py_compile src/polyarb/control_plane/db_role_contract.py src/polyarb/cli_control_plane.py tests/m1-perception/test_control_plane_db_role_contract.py tests/m1-perception/test_control_plane_cli.py
```

Diff check:

```text
git diff --check -- src/polyarb/control_plane/db_role_contract.py src/polyarb/cli_control_plane.py tests/m1-perception/test_control_plane_db_role_contract.py tests/m1-perception/test_control_plane_cli.py .superpowers/sdd/task-2-report.md
```

Exit status: `0`.

### Review-Fix Self-Review

- Sequence checks remain deny-only; no allowed sequence privilege was added.
- Sequence privilege detection uses effective PostgreSQL privilege checks, so
  PUBLIC and inherited grants are rejected.
- Role snapshots are read inside the same read-only transaction and expose only
  closed reason codes plus role/object identifiers.
- No CLI daemon-gate behavior was broadened in this follow-up.

### Review-Fix Commit

```text
fix(05.6-207): verify complete database role envelope
```

## Rereview Fix: Runtime Public Sequence Enumeration

Addressed the remaining Important finding from
`.superpowers/sdd/task-2-rereview.md`.

- Replaced runtime-controller column-to-sequence probing with a `pg_catalog`
  enumeration of every sequence in schema `public`.
- Runtime-controller now rejects effective `USAGE`, `SELECT`, or `UPDATE` on
  each discovered public sequence, including privileges amplified through
  PUBLIC or inherited roles.
- Kept qualification-worker's explicit ledger identity-sequence denial and did
  not add any allowed sequence privileges.

### RED

Command:

```text
uv run pytest tests/m1-perception/test_control_plane_db_role_contract.py -q
```

Output:

```text
.........FF..F.....                                                      [100%]
=================================== FAILURES ===================================
FAILED tests/m1-perception/test_control_plane_db_role_contract.py::test_database_role_contract_rejects_forbidden_sequence_privilege[runtime-controller-public]
FAILED tests/m1-perception/test_control_plane_db_role_contract.py::test_database_role_contract_rejects_forbidden_sequence_privilege[runtime-controller-inherited]
FAILED tests/m1-perception/test_control_plane_db_role_contract.py::test_runtime_sequence_denial_uses_public_catalog_enumeration
3 failed, 16 passed
```

Each failure was `Failed: DID NOT RAISE`, confirming the previous runtime
verifier did not enumerate public sequences.

### GREEN / Verification

Focused contract tests:

```text
uv run pytest tests/m1-perception/test_control_plane_db_role_contract.py -q
...................                                                      [100%]
```

Focused behavior and runtime observe regression:

```text
uv run pytest tests/m1-perception/test_control_plane_db_role_contract.py tests/m1-perception/test_control_plane_cli.py tests/m1-perception/test_control_plane_runtime_observe.py -q
........................................................................ [ 87%]
..........                                                               [100%]
```

Lint:

```text
uv run ruff check src/polyarb/control_plane/db_role_contract.py src/polyarb/cli_control_plane.py tests/m1-perception/test_control_plane_db_role_contract.py tests/m1-perception/test_control_plane_cli.py
All checks passed!
```

Pyright:

```text
uv run pyright src/polyarb/control_plane/db_role_contract.py src/polyarb/cli_control_plane.py tests/m1-perception/test_control_plane_db_role_contract.py tests/m1-perception/test_control_plane_cli.py
0 errors, 0 warnings, 0 informations
WARNING: there is a new pyright version available (v1.1.408 -> v1.1.411).
Please install the new version or set PYRIGHT_PYTHON_FORCE_VERSION to `latest`
```

Syntax:

```text
uv run python -m py_compile src/polyarb/control_plane/db_role_contract.py src/polyarb/cli_control_plane.py tests/m1-perception/test_control_plane_db_role_contract.py tests/m1-perception/test_control_plane_cli.py
```

Diff check:

```text
git diff --check -- src/polyarb/control_plane/db_role_contract.py src/polyarb/cli_control_plane.py tests/m1-perception/test_control_plane_db_role_contract.py tests/m1-perception/test_control_plane_cli.py .superpowers/sdd/task-2-report.md
```

Exit status: `0`.

### Rereview Self-Review

- Runtime sequence deny is bounded to schema `public` with a parameterized
  catalog query inside the existing read-only verification transaction.
- Error output remains closed: reason code plus schema-qualified sequence and
  privilege only.
- Runtime tests now use the real-schema qualification ledger sequence returned
  by public sequence enumeration; the misleading fake runtime serial-column path
  was removed.
- Zero-mutation assertions remain on the sequence-denial failure paths.

### Rereview Commit

```text
fix(05.6-207): deny runtime access to all app sequences
```
