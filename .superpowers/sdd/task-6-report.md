# Task 6 Report: Truthful Evidence, Teaching, and Plan 207 Closure

## Status

Task 6 closure implemented in `.worktrees/m1-self-healing` with HEAD verified
as `e3c1fc83` before edits.

No production connection or mutation was performed. No Fly deploy, app
creation, secret installation, production login provisioning, recovery
enablement, fault injection, restart, or downgrade was performed.

## Changed Docs / Evidence

- `docs/learning/89-数据库能力角色与进程身份.md` - new teaching chapter covering
  LOGIN role vs capability role, startup contract, positive/negative
  permissions, `SECURITY DEFINER`/`search_path`, release/config identity,
  operator sequence, five adversarial checks, and FAQ.
- `docs/learning/00-INDEX.md` - indexed chapter 89.
- `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/evidence/runtime-observe-only.json`
  - bumped to artifact version 2 and recorded the audited production boundary.
- `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/05.6-207-SUMMARY.md`
  - created Plan 207 closure summary.
- `.planning/workstreams/m1-perception/STATE.md` - updated local Plan 207
  closure and production boundary.
- `.planning/JOURNAL.md` - appended the session closure and next command.
- `.planning/threads/market-observation-architecture.md` - added the process
  identity/capability role architecture note.

## Task 1-5 Evidence Used

- Task 1: `e3293cbf..06388e2b` (`2ea4e6f4`, `06388e2b`), review clean after
  the hardened trigger EXECUTE fix.
- Task 2: `06388e2b..61762332` (`6ab5cb5f`, `9e9fb2a2`, `61762332`),
  rereview clean after full role-attribute and public sequence-denial fixes.
- Task 3: `61762332..504c188f` (`3faae36e`, `504c188f`), rereview clean after
  Makefile `release_id` pass-through.
- Task 4: `504c188f..ae9fe081` (`95e047a9`, `ae9fe081`), rereview approved
  after repeat disable / reprovision semantics were fixed.
- Task 5: `ae9fe081..e3c1fc83` (`e3c1fc83`), review clean.

Task review inputs read:

- `.superpowers/sdd/task-2-review.md`
- `.superpowers/sdd/task-2-rereview.md`
- `.superpowers/sdd/task-3-review.md`
- `.superpowers/sdd/task-3-rereview.md`
- `.superpowers/sdd/task-4-review.md`
- `.superpowers/sdd/task-4-rereview.md`
- `.superpowers/sdd/task-5-review.md`
- `.superpowers/sdd/progress.md` Plan 07 ledger

## Local Matrix Evidence

Task 5 matrix evidence is local-only. It used a disposable PostgreSQL
16/testcontainers loopback path because `POLYARB_CONTROL_PLANE_TEST_DSN` was
unset in that shell.

Recorded facts:

- schema version: `m1-runtime-fault-matrix-v2`
- cases: 12
- qualification facts: 77
- observe decisions: 12
- recovery actions: 0
- database leaks: 0
- role leaks: 0
- repeated canonical matrix output: `cmp` exit 0
- secret scan: no runtime-controller password, qualification-worker password,
  or `password` matches
- qualification identity digest:
  `f1f4abe704d859409d01ba1e060839abf47b039a5b5cd4898aed76110c6b860c`

This is not production evidence.

## Production Truth Boundary

- Production database: `postgres`.
- Applied production revisions: `022`, `023`, `024`, `025`.
- Revision 026: NOT APPLIED in production.
- 2026-08-25 audit: `qualification_incident_ingress_rows_at_audit = 1643`.
- 2026-08-25 post-migration worker health: pass.
- Original four apps are running.
- New runtime-controller app: does not exist.
- New qualification-worker app: does not exist.
- Scoped production login role changes: none.
- New production secrets: none.
- Recovery action enablement: none.
- Fault mutation: none.
- Observe-only window: NOT RUN.

The next production action is to prepare a fresh exact authorization package for
final Task-5 SHA `e3c1fc83`, production database `postgres`, revision 026, the
two scoped login roles, the two new private apps, observe-only mode, empty
recovery allowlist, rollback, and this evidence directory. It is not direct
migration or deploy.

## Verification Commands / Output

Command:

```bash
uv run pytest tests/alembic/test_024.py tests/alembic/test_025.py tests/alembic/test_026.py tests/m1-perception/test_control_plane_db_role_contract.py tests/m1-perception/test_control_plane_db_role_admin.py tests/m1-perception/test_control_plane_qualification_identity.py tests/m1-perception/test_control_plane_rollout.py tests/m1-perception/test_control_plane_deployment_templates.py tests/m1-perception/test_control_plane_cli.py tests/m1-perception/test_control_plane_runtime_fault_matrix.py tests/m1-perception/test_makefile_contract.py tests/climb/test_eval_local.py -q
```

Output:

```text
..........................................
.............................. [ 25%]
........................................................................ [ 50%]
........................................................................ [ 75%]
.....................................................................    [100%]
```

Exit code: 0.

Command:

```bash
uv run ruff check alembic/versions/026_m1_runtime_scoped_roles.py src/polyarb/control_plane src/polyarb/cli_control_plane.py tests/alembic/test_026.py tests/m1-perception tools/climb/eval_local.py
```

Output:

```text
All checks passed!
```

Command:

```bash
uv run pyright src/polyarb/control_plane/db_role_contract.py src/polyarb/control_plane/db_role_admin.py src/polyarb/control_plane/qualification_identity.py src/polyarb/cli_control_plane.py
```

Output:

```text
0 errors, 0 warnings, 0 informations
WARNING: there is a new pyright version available (v1.1.408 -> v1.1.411).
Please install the new version or set PYRIGHT_PYTHON_FORCE_VERSION to `latest`
```

Command:

```bash
uv build
```

Output:

```text
Building source distribution...
Building wheel from source distribution...
Successfully built dist/polyarb-0.1.0.tar.gz
Successfully built dist/polyarb-0.1.0-py3-none-any.whl
```

Command:

```bash
make planning-status
```

Output ended with:

```text
✓ no drift detected — every shipped plan has a SUMMARY.
```

Command:

```bash
python -m json.tool .planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/evidence/runtime-observe-only.json >/dev/null
```

Output: no stdout/stderr; exit code 0.

`lsp_diagnostics` was not available in this tool context; `tool_search` for
`lsp diagnostics` returned zero tools. The modified Python files relevant to
Plan 207 were checked with the required Pyright command.

## Scope Notes

- `.superpowers/sdd/progress.md` was pre-existing dirty state and was not staged.
- `.superpowers/sdd/task-5-report.md` was pre-existing dirty state and was not
  staged.
- `dist/` build artifacts are ignored and not staged.
