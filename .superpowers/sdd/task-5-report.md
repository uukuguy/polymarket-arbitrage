# Task 5 Report - Scoped-DSN runtime fault matrix

## Status

Implemented and committed locally on `feat/m1-self-healing`.

No production connection, Fly command, deploy, app creation, secret install,
production role provisioning, fault injection, recovery enablement, restart, or
downgrade was performed. The real matrix verification used a disposable
`postgres:16-alpine` testcontainers loopback DSN because
`POLYARB_CONTROL_PLANE_TEST_DSN` was not set in this shell.

## RED Evidence

Command:

```bash
uv run pytest tests/m1-perception/test_control_plane_runtime_fault_matrix.py tests/climb/test_eval_local.py -q
```

Result: exit 1.

Expected failures:

- matrix canonical assertion failed because current output still reported
  `m1-runtime-fault-matrix-v1`;
- migration-created scoped capability roles were still visible after matrix
  cleanup;
- H-018 deterministic runtime profile did not include the new scoped-role files
  or output nodes.

Key RED excerpt:

```text
AssertionError: assert 'm1-runtime-fault-matrix-v1' == 'm1-runtime-fault-matrix-v2'
KeyError: 'scoped-runtime-controller'
```

## GREEN Evidence

Focused RED/GREEN gate:

```bash
uv run pytest tests/m1-perception/test_control_plane_runtime_fault_matrix.py tests/climb/test_eval_local.py -q
```

Result: exit 0; 39 tests passed.

Exact H-018 pytest gate:

```bash
uv run pytest tests/alembic/test_026.py tests/m1-perception/test_control_plane_db_role_contract.py tests/m1-perception/test_control_plane_db_role_admin.py tests/m1-perception/test_control_plane_qualification_identity.py tests/m1-perception/test_control_plane_runtime_fault_matrix.py tests/climb/test_eval_local.py -q
```

Result: exit 0; 86 tests passed.

Exact Ruff gate:

```bash
uv run ruff check src/polyarb/control_plane tests/alembic/test_026.py tests/m1-perception/test_control_plane_runtime_fault_matrix.py tools/climb/eval_local.py
```

Result:

```text
All checks passed!
```

Type/syntax/diff checks:

```bash
uv run pyright src/polyarb/control_plane/runtime_fault_matrix.py tools/climb/eval_local.py tests/m1-perception/test_control_plane_runtime_fault_matrix.py tests/climb/test_eval_local.py
uv run python -m py_compile src/polyarb/control_plane/runtime_fault_matrix.py tools/climb/eval_local.py tests/m1-perception/test_control_plane_runtime_fault_matrix.py tests/climb/test_eval_local.py
git diff --check -- src/polyarb/control_plane/runtime_fault_matrix.py tests/m1-perception/test_control_plane_runtime_fault_matrix.py tools/climb/eval_local.py tests/climb/test_eval_local.py
```

Results:

- pyright: `0 errors, 0 warnings, 0 informations`
- py_compile: exit 0
- diff check: exit 0

## Matrix Evidence

Command path: disposable PG16 testcontainers loopback DSN, then:

```bash
make runtime-fault-matrix > /tmp/runtime-scoped-matrix-1.json
make runtime-fault-matrix > /tmp/runtime-scoped-matrix-2.json
cmp /tmp/runtime-scoped-matrix-1.json /tmp/runtime-scoped-matrix-2.json
```

Result: `cmp` exit 0.

Canonical output summary:

```json
{
  "case_count": 12,
  "schema_version": "m1-runtime-fault-matrix-v2",
  "qualification_fact_count": 77,
  "observe_decision_count": 12,
  "qualification_identity_digest": "f1f4abe704d859409d01ba1e060839abf47b039a5b5cd4898aed76110c6b860c",
  "scoped_roles": {
    "qualification_worker": {
      "facts_consumed": 77,
      "profile": "qualification-worker",
      "status": "pass"
    },
    "runtime_controller": {
      "observe_decisions": 12,
      "profile": "runtime-controller",
      "recovery_actions_created": 0,
      "status": "pass"
    }
  }
}
```

Cleanup evidence from the same disposable cluster:

```json
{
  "database_leaks": [],
  "role_leaks": []
}
```

Output secret scan:

```bash
rg -n "runtime-controller-[0-9a-f]{32}|qualification-worker-[0-9a-f]{32}|password" /tmp/runtime-scoped-matrix-1.json /tmp/runtime-scoped-matrix-2.json
```

Result: exit 1 with no matches.

## H-018 Nodes

The local H-018 profile keeps the existing planning/unit/integration/cli/restart
topology and adds these output nodes:

- `scoped-runtime-controller`
- `scoped-qualification-worker`
- `zero-recovery-actions`
- `qualification-identity-digest`

The exact profile now includes the five scoped/revision files required by the
brief:

- `tests/alembic/test_026.py`
- `tests/m1-perception/test_control_plane_db_role_contract.py`
- `tests/m1-perception/test_control_plane_db_role_admin.py`
- `tests/m1-perception/test_control_plane_qualification_identity.py`
- `tests/m1-perception/test_control_plane_runtime_fault_matrix.py`

## Commit

- `e3c1fc83` - `test(05.6-207): prove scoped runtime authority end to end`

## Concerns

- `POLYARB_CONTROL_PLANE_TEST_DSN` was unset locally, so the requested literal
  environment command was exercised through the repository's existing
  testcontainers loopback path instead of a caller-provided DSN.
- `.superpowers/sdd/progress.md` remains dirty and was intentionally not staged
  or committed.
