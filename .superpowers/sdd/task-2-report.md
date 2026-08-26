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
