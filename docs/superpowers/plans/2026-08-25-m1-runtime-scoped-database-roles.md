# M1 Runtime Scoped Database Roles Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add revision `026` and the application/operator contracts required to run the private runtime controller and rolling qualification worker with separate least-privilege PostgreSQL credentials.

**Architecture:** Alembic owns two NOLOGIN capability roles and hardened SECURITY DEFINER entry points; operator tooling owns environment-specific LOGIN roles and passwords. Both daemons verify their exact database identity and effective positive/negative privileges before their first mutation, while rollout rendering binds qualification to an exact release and canonical config digest.

**Tech Stack:** Python 3.12, psycopg 3, Alembic, PostgreSQL 16/17, pytest, testcontainers, Fly.io TOML, uv, Make.

## Global Constraints

- Production is currently at revision `025`; revisions `022` through `025` are applied, the original four apps remain running, and the two new apps do not exist.
- Do not deploy, create Fly apps, install secrets, provision production login roles, inject faults, enable recovery, restart a process/Machine, or downgrade production while executing this plan.
- Runtime recovery remains `observe-only`; `POLYARB_RUNTIME_RECOVERY_ALLOWED_TARGETS` remains empty and the controller receives no Fly API token.
- Capability roles are exactly `m1_runtime_controller_capability` and `m1_qualification_worker_capability`; login roles are exactly `m1_runtime_controller_login` and `m1_qualification_worker_login`.
- All four roles are non-superuser, cannot create databases or roles, cannot replicate, and cannot bypass RLS. Capability roles are NOLOGIN; login roles are LOGIN INHERIT with exactly one matching application capability membership.
- Qualification release/config/role identity is explicit. Reject `release-unknown`, `config-unknown`, empty role identity, and config-digest mismatch before mutation.
- No credential value, DSN, SQL password literal, authorization header, or provider response body may appear in stdout, stderr, evidence, tests, or commits.
- All executable operator workflows require Makefile targets.
- Use TDD and atomic commits. Do not stage `.superpowers/sdd/progress.md`.
- Finish with `05.6-207-SUMMARY.md`, teaching document 89, JOURNAL/STATE/thread/evidence updates, and clean `make planning-status`.

---

## File structure

- `alembic/versions/026_m1_runtime_scoped_roles.py` — capability-role lifecycle, exact grants, and qualification SECURITY DEFINER hardening.
- `src/polyarb/control_plane/db_role_contract.py` — pure read-only daemon identity and effective-permission verification.
- `src/polyarb/control_plane/db_role_admin.py` — explicit operator preflight/provision/verify/disable commands; never deploys or touches Fly.
- `src/polyarb/control_plane/qualification_identity.py` — canonical qualification config payload/digest and environment validation shared by renderer and daemon.
- `src/polyarb/control_plane/qualification_service.py` — call the freshness-only database function.
- `src/polyarb/control_plane/rollout.py` — bind rendered topology to release SHA, config payload/digest, migration 026, and exact login/capability names.
- `src/polyarb/cli_control_plane.py` — run startup gates before runtime-controller or qualification mutations and accept exact rollout release identity.
- `src/polyarb/control_plane/runtime_fault_matrix.py` — exercise observe and qualification paths through scoped login DSNs while retaining admin setup authority only inside the disposable database.
- `deploy/control-plane/fly-runtime-controller.toml.template` — fixed controller role identity plus observe-only/empty-allowlist config.
- `deploy/control-plane/fly-qualification-worker.toml.template` — fixed qualification role identity plus explicit release/config/role values.
- `tests/alembic/test_026.py` — static and real-PostgreSQL migration, function, grant, collision, downgrade/reapply tests.
- `tests/m1-perception/test_control_plane_db_role_contract.py` — read-only identity verifier unit tests.
- `tests/m1-perception/test_control_plane_db_role_admin.py` — operator command safety and real-role provisioning tests.
- Existing CLI, rollout, deployment-template, Makefile, qualification-service, and fault-matrix tests — integration contracts.

---

### Task 1: Revision 026 capability roles and hardened qualification ingress

**Files:**
- Create: `alembic/versions/026_m1_runtime_scoped_roles.py`
- Create: `tests/alembic/test_026.py`
- Modify: `src/polyarb/control_plane/qualification_service.py:574-599`
- Modify: `tests/m1-perception/test_control_plane_qualification_service.py`

**Interfaces:**
- Consumes: revision `025` tables/functions and revision `024` qualification triggers.
- Produces: capability roles, exact object grants, `public.m1_record_qualification_freshness_ingress(text,text,timestamptz,jsonb)`, and hardened existing ingress/certificate functions.

- [ ] **Step 1: Write the failing static migration tests**

Add tests that read the migration source and assert the exact revision chain,
role attributes, permission table names, PUBLIC revocations, SECURITY DEFINER
search paths, and downgrade boundary:

```python
MIGRATION_PATH = Path("alembic/versions/026_m1_runtime_scoped_roles.py")


def test_026_declares_exact_capability_roles_and_chain() -> None:
    text = MIGRATION_PATH.read_text()
    assert 'revision = "026"' in text
    assert 'down_revision = "025"' in text
    for role in (
        "m1_runtime_controller_capability",
        "m1_qualification_worker_capability",
    ):
        assert role in text
    for attribute in (
        "NOLOGIN", "NOSUPERUSER", "NOCREATEDB", "NOCREATEROLE",
        "NOINHERIT", "NOREPLICATION", "NOBYPASSRLS",
    ):
        assert attribute in text


def test_026_keeps_observe_role_out_of_recovery_mutation() -> None:
    text = MIGRATION_PATH.read_text()
    assert "m1_runtime_observe_decisions" in text
    assert "m1_runtime_controller_leases" in text
    assert "m1_recovery_actions" in text
    assert "RUNTIME_CONTROLLER_WRITE_TABLES" in text
    assert "m1_recovery_actions" not in _runtime_write_table_tuple(text)
```

The helper in the last assertion parses the literal
`RUNTIME_CONTROLLER_WRITE_TABLES` assignment rather than searching all SQL, so
the necessary SELECT grant cannot create a false failure.

- [ ] **Step 2: Write real-PostgreSQL positive, negative, trigger, and replay tests**

Use `PostgresContainer("postgres:16-alpine")`, create only the Supabase fixture
roles `anon`, `authenticated`, and `service_role`, then run:

```python
_run_alembic(dsn, "upgrade", "025")
_run_alembic(dsn, "upgrade", "026")
assert _current_revision(dsn) == "026"

with psycopg.connect(dsn, autocommit=True) as admin:
    admin.execute(
        "GRANT m1_runtime_controller_capability TO m1_runtime_controller_test"
    )
    admin.execute(
        "GRANT m1_qualification_worker_capability TO m1_qualification_worker_test"
    )
```

Create disposable LOGIN test roles before those grants, connect as each role,
and prove every positive and negative permission from the design matrix using
`has_table_privilege`, `has_sequence_privilege`, and
`has_function_privilege`. Perform real operations, not catalog-only checks:

```python
with runtime_connection() as runtime:
    _seed_required_job_facts(admin_connection)
    controller = claim_controller(
        lambda: runtime_connection(),
        controller_id="scoped-controller",
        owner_id="scoped-owner",
        lease_seconds=60,
        now=NOW,
    )
    insert_runtime_observe_decision(
        lambda: runtime_connection(),
        build_runtime_observe_idle_record(
            controller_id=controller.controller_id,
            controller_owner_id=controller.owner_id,
            controller_epoch=controller.lease_epoch,
            observed_at=NOW,
            next_check_at=NOW + timedelta(seconds=30),
            observed_by=controller.owner_id,
        ),
    )

with pytest.raises(psycopg.errors.InsufficientPrivilege):
    with runtime_connection() as runtime:
        runtime.execute(
            "UPDATE m1_recovery_actions SET state = state WHERE false"
        )
```

Also prove:

- unsafe pre-existing capability-role attributes make `upgrade 026` fail;
- runtime/incident/recovery source writes project into the ingress ledger even
  when a restricted producer lacks ledger INSERT and sequence USAGE;
- the qualification role can insert `freshness` through the wrapper but cannot
  invoke the general recorder or spoof `runtime`;
- malformed data product, source prefix, JSON shape, or payload over 8,192
  bytes is rejected;
- epoch/cursor/recovery-observation/certificate paths succeed through the
  qualification login;
- immutable certificate and observation UPDATE/DELETE still fail;
- after test login roles and memberships are removed, `026 -> 025 -> 026`
  succeeds and recreates the exact grants.

- [ ] **Step 3: Run the tests and verify red**

Run:

```bash
uv run pytest tests/alembic/test_026.py tests/m1-perception/test_control_plane_qualification_service.py -q
```

Expected: FAIL because revision `026` and the freshness-only function call do
not exist.

- [ ] **Step 4: Implement the migration role contract**

Define literal allowlists so grants can be reviewed independently:

```python
RUNTIME_CONTROLLER_READ_TABLES = (
    "m1_runtime_controller_leases",
    "m1_runtime_observe_decisions",
    "m1_job_runtime_state",
    "m1_jobs",
    "m1_job_circuits",
    "m1_job_attempts",
    "m1_recovery_target_budgets",
    "m1_recovery_actions",
)
RUNTIME_CONTROLLER_WRITE_TABLES = (
    "m1_runtime_controller_leases",
    "m1_runtime_observe_decisions",
)
QUALIFICATION_READ_TABLES = (
    "m1_qualification_ingress_ledger",
    "m1_qualification_source_cursors",
    "m1_qualification_epochs",
    "m1_qualification_recovery_observations",
    "m1_qualification_certificates",
    "m1_publication_pointers",
    "m1_generation_manifests",
    "m1_opportunity_publication_pointers",
    "m1_opportunity_projections",
)
QUALIFICATION_INSERT_TABLES = (
    "m1_qualification_source_cursors",
    "m1_qualification_epochs",
    "m1_qualification_recovery_observations",
)
QUALIFICATION_UPDATE_TABLES = (
    "m1_qualification_source_cursors",
    "m1_qualification_epochs",
)
```

For each capability role, a fixed-name `DO` block must either create it with
the required attributes or raise when an existing role differs. Grant CONNECT
to `current_database()`, USAGE on `public`, and only the matrix above. Grant
controller UPDATE only on leases and INSERT only on leases/observe decisions;
do not use `GRANT ALL` or default privileges.

- [ ] **Step 5: Harden the qualification function chain**

Recreate touched functions with schema-qualified application objects and
`SET search_path = pg_catalog`. The new public wrapper must be exactly bounded:

```sql
CREATE FUNCTION public.m1_record_qualification_freshness_ingress(
    p_source_id text,
    p_source_version text,
    p_original_observed_at timestamptz,
    p_payload jsonb
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
BEGIN
    IF p_source_version NOT IN ('structure', 'quote', 'opportunity') THEN
        RAISE EXCEPTION 'qualification freshness product is unsupported';
    END IF;
    IF p_source_id NOT LIKE 'freshness:' || p_source_version || ':%' THEN
        RAISE EXCEPTION 'qualification freshness identity conflicts';
    END IF;
    IF p_payload IS NULL
       OR jsonb_typeof(p_payload) <> 'object'
       OR pg_column_size(p_payload) > 8192
       OR p_payload ->> 'data_product' <> p_source_version THEN
        RAISE EXCEPTION 'qualification freshness payload is invalid';
    END IF;
    PERFORM public.m1_record_qualification_ingress(
        'freshness', p_source_id, p_source_version,
        p_original_observed_at, p_payload
    );
END;
$$;
```

Revoke execute on the general recorder from `PUBLIC`, `anon`, `authenticated`,
`service_role`, and both new capability roles. Make the three fixed-source
projection trigger functions SECURITY DEFINER so restricted source producers
need no direct ledger permission. Harden the existing certificate insertion and
verification calls to the same schema/search-path rule, then grant the
qualification capability only the freshness wrapper and certificate insert
function.

Change the Python freshness call to:

```python
cursor.execute(
    """
    SELECT public.m1_record_qualification_freshness_ingress(
        %s, %s, %s, %s
    )
    """,
    (
        cast(str, payload["fact_id"]),
        str(payload.get("data_product", product)),
        now,
        Jsonb(payload),
    ),
)
```

The downgrade removes the wrapper and grants, restores revision-024 function
security attributes, drops capability roles only after their grants are
removed, and is documented/tested as isolated-test-only.

- [ ] **Step 6: Verify and commit**

Run:

```bash
uv run pytest tests/alembic/test_024.py tests/alembic/test_025.py tests/alembic/test_026.py tests/m1-perception/test_control_plane_qualification_service.py -q
uv run ruff check alembic/versions/026_m1_runtime_scoped_roles.py src/polyarb/control_plane/qualification_service.py tests/alembic/test_026.py
```

Expected: all tests pass; Alembic reports one head, `026`.

```bash
git add alembic/versions/026_m1_runtime_scoped_roles.py tests/alembic/test_026.py src/polyarb/control_plane/qualification_service.py tests/m1-perception/test_control_plane_qualification_service.py
git commit -m "feat(05.6-207): add scoped runtime database capabilities"
```

---

### Task 2: Fail-closed daemon database identity contract

**Files:**
- Create: `src/polyarb/control_plane/db_role_contract.py`
- Create: `tests/m1-perception/test_control_plane_db_role_contract.py`
- Modify: `src/polyarb/cli_control_plane.py:420-470,1650-1715,1815-1845`
- Modify: `tests/m1-perception/test_control_plane_cli.py`

**Interfaces:**
- Consumes: the two revision-026 capability names and exact permission matrix.
- Produces: `verify_daemon_database_role(connection_factory: ConnectionFactory, profile: str, *, expected_database: str) -> DatabaseRoleVerification` for profiles `runtime-controller` and `qualification-worker`.

- [ ] **Step 1: Write failing contract tests**

Create fake cursor/connection tests for exact success plus each fail-closed
condition:

```python
@pytest.mark.parametrize(
    "failure_code",
    (
        "database-role.login-mismatch",
        "database-role.capability-missing",
        "database-role.cross-capability",
        "database-role.unsafe-attribute",
        "database-role.required-privilege-missing",
        "database-role.forbidden-privilege-present",
    ),
)
def test_database_role_contract_fails_closed(failure_code: str) -> None:
    factory = fake_role_factory(failure_code=failure_code)
    with pytest.raises(DatabaseRoleContractError, match=failure_code):
        verify_daemon_database_role(
            factory,
            "runtime-controller",
            expected_database="role_test",
        )
    assert factory.write_count == 0
```

CLI tests must prove `runtime-reconcile-once`, `runtime-reconcile-serve`, and
`qualification-serve` call the verifier before `claim_controller`, freshness
insert, epoch insert, or service-loop construction. Status and verification
commands remain usable with operator read credentials.

- [ ] **Step 2: Run tests and verify red**

Run:

```bash
uv run pytest tests/m1-perception/test_control_plane_db_role_contract.py tests/m1-perception/test_control_plane_cli.py -q
```

Expected: FAIL because the verifier does not exist and daemon branches do not
call it.

- [ ] **Step 3: Implement the immutable role profiles**

Use explicit data rather than inferred grants:

```python
ConnectionFactory = Callable[[], psycopg.Connection[Any]]
TABLE_PRIVILEGES = (
    "SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"
)


@dataclass(frozen=True, slots=True)
class DatabaseRoleContract:
    profile: str
    login_role: str
    capability_role: str
    required_table_privileges: tuple[tuple[str, str], ...]
    forbidden_table_privileges: tuple[tuple[str, str], ...]
    required_function_privileges: tuple[str, ...] = ()
    forbidden_function_privileges: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class DatabaseRoleVerification:
    profile: str
    session_user: str
    capability_role: str
    database_name: str
    status: str = "pass"
```

Build the runtime profile from this exact allowlist and treat every other
`TABLE_PRIVILEGES` value on the listed tables as forbidden:

```python
RUNTIME_ALLOWED = {
    "m1_runtime_controller_leases": frozenset({"SELECT", "INSERT", "UPDATE"}),
    "m1_runtime_observe_decisions": frozenset({"SELECT", "INSERT"}),
    "m1_job_runtime_state": frozenset({"SELECT"}),
    "m1_jobs": frozenset({"SELECT"}),
    "m1_job_circuits": frozenset({"SELECT"}),
    "m1_job_attempts": frozenset({"SELECT"}),
    "m1_recovery_target_budgets": frozenset({"SELECT"}),
    "m1_recovery_actions": frozenset({"SELECT"}),
    "m1_job_runtime_events": frozenset(),
    "m1_incidents": frozenset(),
    "m1_incident_events": frozenset(),
    "m1_alert_outbox": frozenset(),
}
QUALIFICATION_ALLOWED = {
    "m1_qualification_ingress_ledger": frozenset({"SELECT"}),
    "m1_qualification_source_cursors": frozenset({"SELECT", "INSERT", "UPDATE"}),
    "m1_qualification_epochs": frozenset({"SELECT", "INSERT", "UPDATE"}),
    "m1_qualification_recovery_observations": frozenset({"SELECT", "INSERT"}),
    "m1_qualification_certificates": frozenset({"SELECT"}),
    "m1_publication_pointers": frozenset({"SELECT"}),
    "m1_generation_manifests": frozenset({"SELECT"}),
    "m1_opportunity_publication_pointers": frozenset({"SELECT"}),
    "m1_opportunity_projections": frozenset({"SELECT"}),
    "m1_job_runtime_events": frozenset(),
    "m1_incidents": frozenset(),
    "m1_incident_events": frozenset(),
    "m1_recovery_actions": frozenset(),
    "m1_alert_outbox": frozenset(),
}
```

Qualification function permissions allow only the exact freshness wrapper and
certificate inserter; the general ingress recorder is explicitly forbidden.

Populate the positive and negative cells verbatim from the approved design.
The verifier opens one read-only transaction and queries:

```sql
SELECT current_database(), session_user, current_user,
       role.rolsuper, role.rolcreatedb, role.rolcreaterole,
       role.rolreplication, role.rolbypassrls
FROM pg_catalog.pg_roles AS role
WHERE role.rolname = session_user;
```

Then use parameterized calls to `pg_has_role`, `has_database_privilege`,
`has_schema_privilege`, `has_table_privilege`, and
`has_function_privilege`. Require the matching capability, reject membership in
every other direct or inherited role, require every positive effective privilege,
and reject every forbidden effective privilege regardless of whether it came
from PUBLIC or another inherited role. Reject a database name different from
the explicit `expected_database` before checking application privileges.

Errors contain only a closed reason code and object identifier; never include a
DSN, exception body, or connection parameters.

- [ ] **Step 4: Gate both daemon entry points before mutation**

In `cli_control_plane.py`, immediately after the relevant connection factory is
created and before service/control-plane construction mutates anything:

```python
if args.command == "qualification-serve":
    verify_daemon_database_role(
        connection_factory,
        "qualification-worker",
        expected_database=_required_expected_database_from_env(),
    )
    service = _qualification_service_from_env(
        batch_size=args.batch_size,
        interval_seconds=args.interval_seconds,
        writer_id=args.writer_id,
    )

if args.command in {"runtime-reconcile-once", "runtime-reconcile-serve"}:
    verify_daemon_database_role(
        control_plane._connection_factory,
        "runtime-controller",
        expected_database=_required_expected_database_from_env(),
    )
```

Keep the existing top-level bounded error surfaces. Add the reason code to
stderr, but do not print database exception detail for role-contract failures.
`_required_expected_database_from_env()` reads
`POLYARB_DB_EXPECTED_DATABASE`, strips it, and rejects a missing value before
opening the daemon loop.

- [ ] **Step 5: Verify and commit**

Run:

```bash
uv run pytest tests/m1-perception/test_control_plane_db_role_contract.py tests/m1-perception/test_control_plane_cli.py tests/m1-perception/test_control_plane_runtime_observe.py -q
uv run ruff check src/polyarb/control_plane/db_role_contract.py src/polyarb/cli_control_plane.py tests/m1-perception/test_control_plane_db_role_contract.py
```

Expected: PASS, including zero mutation on every rejected identity.

```bash
git add src/polyarb/control_plane/db_role_contract.py src/polyarb/cli_control_plane.py tests/m1-perception/test_control_plane_db_role_contract.py tests/m1-perception/test_control_plane_cli.py
git commit -m "feat(05.6-207): fail closed on daemon database identity"
```

---

### Task 3: Exact qualification release and configuration identity

**Files:**
- Create: `src/polyarb/control_plane/qualification_identity.py`
- Create: `tests/m1-perception/test_control_plane_qualification_identity.py`
- Modify: `src/polyarb/control_plane/rollout.py`
- Modify: `src/polyarb/cli_control_plane.py:436-470,1530-1555,1685-1710`
- Modify: `deploy/control-plane/fly-runtime-controller.toml.template`
- Modify: `deploy/control-plane/fly-qualification-worker.toml.template`
- Modify: `tests/m1-perception/test_control_plane_rollout.py`
- Modify: `tests/m1-perception/test_control_plane_deployment_templates.py`
- Modify: `tests/m1-perception/test_control_plane_cli.py`

**Interfaces:**
- Consumes: exact Git SHA, observe-only mode, empty or exact recovery target list, and qualification service constants.
- Produces: `qualification_config_payload(interval_seconds, batch_size, role_identity, runtime_recovery_mode, runtime_recovery_allowed_targets)`, `qualification_config_id(payload)`, and `qualification_identity_from_env(interval_seconds, batch_size)`.

- [ ] **Step 1: Write failing canonical identity tests**

Require a 40-character lowercase Git SHA, deterministic JSON ordering, explicit
roles, and rejection of unknown/mismatched values:

```python
def test_qualification_config_identity_is_canonical() -> None:
    payload = qualification_config_payload(
        interval_seconds=30,
        batch_size=100,
        role_identity=("opportunity", "quote", "structure"),
        runtime_recovery_mode="observe-only",
        runtime_recovery_allowed_targets=(),
    )
    assert payload == {
        "batch_size": 100,
        "interval_seconds": 30,
        "max_gap_seconds": 900,
        "policy_version": "m1-rolling-qualification-v1",
        "required_seconds": 86400,
        "role_identity": ["opportunity", "quote", "structure"],
        "runtime_recovery_allowed_targets": [],
        "runtime_recovery_mode": "observe-only",
        "signature_budget": 3,
    }
    assert re.fullmatch(r"sha256:[0-9a-f]{64}", qualification_config_id(payload))
```

Renderer tests assert artifact version `10`, step
`revisions-022-through-026-migration`, exact role names, exact release ID,
canonical payload/digest, and no unresolved placeholders.

- [ ] **Step 2: Run tests and verify red**

Run:

```bash
uv run pytest tests/m1-perception/test_control_plane_qualification_identity.py tests/m1-perception/test_control_plane_rollout.py tests/m1-perception/test_control_plane_deployment_templates.py tests/m1-perception/test_control_plane_cli.py -q
```

Expected: FAIL because release/config identity is not rendered or validated.

- [ ] **Step 3: Implement canonical config and environment validation**

Canonical bytes are:

```python
def qualification_config_id(payload: Mapping[str, object]) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return f"sha256:{sha256(encoded).hexdigest()}"
```

`qualification_identity_from_env(interval_seconds, batch_size)` reads:

```text
POLYARB_QUALIFICATION_RELEASE_ID
POLYARB_QUALIFICATION_CONFIG_ID
POLYARB_QUALIFICATION_ROLE_IDENTITY
POLYARB_QUALIFICATION_RUNTIME_RECOVERY_MODE
POLYARB_QUALIFICATION_RUNTIME_RECOVERY_ALLOWED_TARGETS
```

It rebuilds the payload from actual CLI cadence/batch values, rejects unknown
defaults, requires ordered roles `opportunity,quote,structure`, and compares the
digest with `hmac.compare_digest`. A non-integral cadence is rejected; an
integral `30.0` is canonicalized to JSON integer `30`. Recovery targets are
validated for exact identity, deduplicated, and sorted lexicographically before
hashing.

It returns the exact release/config/roles for `RollingQualificationPolicy`:

```python
class QualificationIdentityError(ValueError):
    """Qualification release/config identity is missing or inconsistent."""


@dataclass(frozen=True, slots=True)
class QualificationIdentity:
    release_id: str
    config_id: str
    role_identity: tuple[str, ...]
    config_payload: Mapping[str, object]


def _required_env(name: str, *, allow_empty: bool = False) -> str:
    raw = os.environ.get(name)
    if raw is None:
        raise QualificationIdentityError(f"qualification.identity.missing:{name}")
    value = raw.strip()
    if not value and not allow_empty:
        raise QualificationIdentityError(f"qualification.identity.empty:{name}")
    return value


def qualification_identity_from_env(
    *,
    interval_seconds: float,
    batch_size: int,
) -> QualificationIdentity:
    release_id = _required_env("POLYARB_QUALIFICATION_RELEASE_ID")
    supplied_config_id = _required_env("POLYARB_QUALIFICATION_CONFIG_ID")
    roles = tuple(
        value.strip()
        for value in _required_env("POLYARB_QUALIFICATION_ROLE_IDENTITY").split(",")
        if value.strip()
    )
    mode = _required_env("POLYARB_QUALIFICATION_RUNTIME_RECOVERY_MODE")
    targets = tuple(
        sorted(
            value.strip()
            for value in _required_env(
                "POLYARB_QUALIFICATION_RUNTIME_RECOVERY_ALLOWED_TARGETS",
                allow_empty=True,
            ).split(",")
            if value.strip()
        )
    )
    if re.fullmatch(r"[0-9a-f]{40}", release_id) is None:
        raise QualificationIdentityError("qualification.identity.release-invalid")
    if roles != ("opportunity", "quote", "structure"):
        raise QualificationIdentityError("qualification.identity.roles-invalid")
    payload = qualification_config_payload(
        interval_seconds=interval_seconds,
        batch_size=batch_size,
        role_identity=roles,
        runtime_recovery_mode=mode,
        runtime_recovery_allowed_targets=targets,
    )
    expected_config_id = qualification_config_id(payload)
    if not hmac.compare_digest(supplied_config_id, expected_config_id):
        raise QualificationIdentityError("qualification.identity.config-mismatch")
    return QualificationIdentity(
        release_id=release_id,
        config_id=supplied_config_id,
        role_identity=roles,
        config_payload=payload,
    )
```

- [ ] **Step 4: Render the exact identity into both private apps**

Add required `release_id: str` to `render_rollout_artifacts`. Validate with
`^[0-9a-f]{40}$`, build the canonical config, and replace these placeholders:

```toml
# runtime controller
POLYARB_DB_EXPECTED_DATABASE = "__EXPECTED_DATABASE__"
POLYARB_RUNTIME_RECOVERY_MODE = "observe-only"
POLYARB_RUNTIME_RECOVERY_ALLOWED_TARGETS = "__RUNTIME_RECOVERY_ALLOWED_TARGETS__"

# qualification worker
POLYARB_DB_EXPECTED_DATABASE = "__EXPECTED_DATABASE__"
POLYARB_QUALIFICATION_RELEASE_ID = "__QUALIFICATION_RELEASE_ID__"
POLYARB_QUALIFICATION_CONFIG_ID = "__QUALIFICATION_CONFIG_ID__"
POLYARB_QUALIFICATION_ROLE_IDENTITY = "opportunity,quote,structure"
POLYARB_QUALIFICATION_RUNTIME_RECOVERY_MODE = "observe-only"
POLYARB_QUALIFICATION_RUNTIME_RECOVERY_ALLOWED_TARGETS = "__RUNTIME_RECOVERY_ALLOWED_TARGETS__"
```

The checklist records the canonical payload and digest, fixed login/capability
names, revision `026`, observe-only mode, and `cloud_actions_performed=false`.
No secret value is rendered.

Add CLI `render-rollout --release-id` and change the constructor signature to
`_qualification_service_from_env(*, batch_size: int, interval_seconds: float,
writer_id: str) -> QualificationService`, validating identity before policy
construction.

- [ ] **Step 5: Verify and commit**

Run:

```bash
uv run pytest tests/m1-perception/test_control_plane_qualification_identity.py tests/m1-perception/test_control_plane_rollout.py tests/m1-perception/test_control_plane_deployment_templates.py tests/m1-perception/test_control_plane_cli.py -q
uv run ruff check src/polyarb/control_plane/qualification_identity.py src/polyarb/control_plane/rollout.py src/polyarb/cli_control_plane.py
```

Expected: PASS; two renders with identical inputs have identical payload and
digest, while any cadence/batch/role/mode/allowlist change changes the digest.

```bash
git add src/polyarb/control_plane/qualification_identity.py src/polyarb/control_plane/rollout.py src/polyarb/cli_control_plane.py deploy/control-plane/fly-runtime-controller.toml.template deploy/control-plane/fly-qualification-worker.toml.template tests/m1-perception/test_control_plane_qualification_identity.py tests/m1-perception/test_control_plane_rollout.py tests/m1-perception/test_control_plane_deployment_templates.py tests/m1-perception/test_control_plane_cli.py
git commit -m "feat(05.6-207): bind qualification to release identity"
```

---

### Task 4: Safe login-role operator tooling and Make entries

**Files:**
- Create: `src/polyarb/control_plane/db_role_admin.py`
- Create: `tests/m1-perception/test_control_plane_db_role_admin.py`
- Modify: `Makefile`
- Modify: `tests/m1-perception/test_makefile_contract.py`
- Modify: `docs/dev/control-plane-runbook.md`

**Interfaces:**
- Consumes: admin `POLYARB_SUPABASE_DB_DSN`, two password inputs, and scoped DSNs for verification.
- Produces: `preflight`, `provision`, `verify`, and `disable` operator commands with no Fly side effects.
- Produces helpers: `_read_admin_role_snapshot(cursor)`, `_require_database_and_revision(snapshot, expected_database, expected_revision)`, `_require_capability_roles_safe(snapshot)`, `_require_login_roles_safe(snapshot)`, `_require_independent_passwords(runtime_password, qualification_password)`, and `_create_or_rotate_login(cursor, login_role, capability_role, password)`.

- [ ] **Step 1: Write failing operator safety tests**

Cover missing `--enable`, wrong database, revision not `026`, unsafe existing
login attributes, unexpected memberships, empty/equal passwords, success,
idempotent password rotation, verification, disablement, transaction rollback,
and secret-free stdout/stderr:

```python
def test_provision_never_renders_credentials(capsys: pytest.CaptureFixture[str]) -> None:
    secret = "runtime-password-that-must-not-appear"
    result = provision_login_roles(
        admin_factory(),
        expected_database="role_test",
        runtime_password=secret,
        qualification_password="independent-qualification-password",
    )
    captured = capsys.readouterr()
    assert result == {"database": "role_test", "status": "provisioned"}
    assert secret not in captured.out + captured.err
```

Real PostgreSQL tests create both logins, verify exact memberships/attributes,
connect through both credentials, rotate one password without changing the
other, and disable both with `NOLOGIN`.

- [ ] **Step 2: Run tests and verify red**

Run:

```bash
uv run pytest tests/m1-perception/test_control_plane_db_role_admin.py tests/m1-perception/test_makefile_contract.py -q
```

Expected: FAIL because the module and Make entries do not exist.

- [ ] **Step 3: Implement explicit preflight/provision/verify/disable commands**

The module exposes:

```python
def preflight_capability_roles(
    connection_factory: ConnectionFactory,
    *,
    expected_database: str,
) -> Mapping[str, object]:
    with connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SET TRANSACTION READ ONLY")
        snapshot = _read_admin_role_snapshot(cursor)
        _require_database_and_revision(snapshot, expected_database, "026")
        _require_capability_roles_safe(snapshot)
    return {"database": expected_database, "status": "ready"}


def provision_login_roles(
    connection_factory: ConnectionFactory,
    *,
    expected_database: str,
    runtime_password: str,
    qualification_password: str,
) -> Mapping[str, object]:
    _require_independent_passwords(runtime_password, qualification_password)
    with connection_factory() as connection, connection.cursor() as cursor:
        snapshot = _read_admin_role_snapshot(cursor)
        _require_database_and_revision(snapshot, expected_database, "026")
        _require_capability_roles_safe(snapshot)
        _create_or_rotate_login(
            cursor,
            login_role="m1_runtime_controller_login",
            capability_role="m1_runtime_controller_capability",
            password=runtime_password,
        )
        _create_or_rotate_login(
            cursor,
            login_role="m1_qualification_worker_login",
            capability_role="m1_qualification_worker_capability",
            password=qualification_password,
        )
        connection.commit()
    return {"database": expected_database, "status": "provisioned"}


def disable_login_roles(
    connection_factory: ConnectionFactory,
    *,
    expected_database: str,
) -> Mapping[str, object]:
    with connection_factory() as connection, connection.cursor() as cursor:
        snapshot = _read_admin_role_snapshot(cursor)
        _require_database_and_revision(snapshot, expected_database, "026")
        _require_login_roles_safe(snapshot)
        for role_name in (
            "m1_runtime_controller_login",
            "m1_qualification_worker_login",
        ):
            cursor.execute(
                sql.SQL("ALTER ROLE {} NOLOGIN").format(sql.Identifier(role_name))
            )
        connection.commit()
    return {"database": expected_database, "status": "disabled"}
```

Use `psycopg.sql.Identifier` for role names and `psycopg.sql.Literal` for
passwords inside the database call. PostgreSQL 16 rejects `ALTER ROLE ...
PASSWORD %s` at `$1`; this safely escaped literal is an explicitly approved
exception to bind-parameter use. Never interpolate, log the composed SQL, or
print either value. Before
CREATE/ALTER, check current database, Alembic `026`, capability attributes,
login attributes, and exact memberships. Provision both roles in one
transaction; on any mismatch, neither role/password/membership is changed.

The module CLI requires `--enable` for provision/disable. It reads passwords
only from `POLYARB_RUNTIME_CONTROLLER_DB_PASSWORD` and
`POLYARB_QUALIFICATION_WORKER_DB_PASSWORD`, rejects equality, and outputs only
role names/status. Verification reads the scoped DSN selected by profile and
delegates to `verify_daemon_database_role`.

- [ ] **Step 4: Add the four Makefile entrances and runbook**

Add:

```make
## control-plane-db-role-preflight: Read-only check that revision 026 capability roles are safe; no login/secret mutation.
control-plane-db-role-preflight:
	@test -n "$(expected_database)" || (echo "usage: make control-plane-db-role-preflight expected_database=<name>" >&2; exit 2)
	@uv run python -m polyarb.control_plane.db_role_admin preflight --expected-database "$(expected_database)" --json

## control-plane-db-role-provision: Explicitly create/rotate the two scoped DB logins; requires enable=1 and password env vars; never contacts Fly.
control-plane-db-role-provision:
	@test "$(enable)" = "1" -a -n "$(expected_database)" || (echo "usage: make control-plane-db-role-provision enable=1 expected_database=<name>" >&2; exit 2)
	@uv run python -m polyarb.control_plane.db_role_admin provision --enable --expected-database "$(expected_database)" --json

## control-plane-db-role-verify: Read-only effective-permission proof for profile=runtime-controller|qualification-worker.
control-plane-db-role-verify:
	@test -n "$(profile)" -a -n "$(expected_database)" || (echo "usage: make control-plane-db-role-verify profile=<profile> expected_database=<name>" >&2; exit 2)
	@uv run python -m polyarb.control_plane.db_role_admin verify --profile "$(profile)" --expected-database "$(expected_database)" --json

## control-plane-db-role-disable: Disable both scoped logins after both apps are stopped; requires enable=1; never downgrades schema.
control-plane-db-role-disable:
	@test "$(enable)" = "1" -a -n "$(expected_database)" || (echo "usage: make control-plane-db-role-disable enable=1 expected_database=<name>" >&2; exit 2)
	@uv run python -m polyarb.control_plane.db_role_admin disable --enable --expected-database "$(expected_database)" --json
```

Update `control-plane-render-rollout` to require `release_id=<40-hex-sha>` and
pass `--release-id`. The runbook specifies Keychain storage, independent Fly
secrets, app-stop-before-disable order, and exact production authorization; it
contains no sample secret.

- [ ] **Step 5: Verify and commit**

Run:

```bash
uv run pytest tests/m1-perception/test_control_plane_db_role_admin.py tests/m1-perception/test_makefile_contract.py -q
make help | rg "control-plane-db-role-(preflight|provision|verify|disable)"
uv run ruff check src/polyarb/control_plane/db_role_admin.py tests/m1-perception/test_control_plane_db_role_admin.py
```

Expected: all four Make targets are discoverable; mutation commands fail
without `enable=1`; no captured output contains test passwords.

```bash
git add src/polyarb/control_plane/db_role_admin.py tests/m1-perception/test_control_plane_db_role_admin.py Makefile tests/m1-perception/test_makefile_contract.py docs/dev/control-plane-runbook.md
git commit -m "feat(05.6-207): add scoped database role operations"
```

---

### Task 5: Scoped-DSN deterministic matrix and integrated verification

**Files:**
- Modify: `src/polyarb/control_plane/runtime_fault_matrix.py`
- Modify: `tests/m1-perception/test_control_plane_runtime_fault_matrix.py`
- Modify: `tools/climb/eval_local.py`
- Modify: `tests/climb/test_eval_local.py`

**Interfaces:**
- Consumes: revision `026`, role provisioning helpers, daemon verifier, and the existing 12 fault cases.
- Produces: matrix schema `m1-runtime-fault-matrix-v2` with explicit scoped-role verification and zero observe-controller recovery actions.

- [ ] **Step 1: Write failing scoped-matrix tests**

Extend the canonical assertions:

```python
assert first["schema_version"] == "m1-runtime-fault-matrix-v2"
assert first["scoped_roles"] == {
    "qualification_worker": {
        "facts_consumed": first["qualification_fact_count"],
        "profile": "qualification-worker",
        "status": "pass",
    },
    "runtime_controller": {
        "observe_decisions": first["observe_decision_count"],
        "profile": "runtime-controller",
        "recovery_actions_created": 0,
        "status": "pass",
    },
}
```

Assert cleanup removes both disposable login roles and both migration-created
capability roles after each matrix run. A second identical run must produce
byte-identical canonical output.

- [ ] **Step 2: Run tests and verify red**

Run:

```bash
uv run pytest tests/m1-perception/test_control_plane_runtime_fault_matrix.py tests/climb/test_eval_local.py -q
```

Expected: FAIL because the matrix still uses one admin connection factory and
reports schema v1.

- [ ] **Step 3: Add scoped login contexts without weakening setup isolation**

Keep admin authority only for disposable database creation, migrations, seed
facts, and the existing execute-mode recovery cases. After revision `026`,
create two fixed test-only login credentials through `provision_login_roles`
and construct:

```python
@dataclass(frozen=True, slots=True)
class _RuntimeContext:
    admin_dsn: str
    admin_factory: ConnectionFactory
    runtime_controller_factory: ConnectionFactory
    qualification_factory: ConnectionFactory
    control_plane: PostgresControlPlane
```

For every fault case, use the qualification factory to read/decode the new
ingress range. Run one bounded observe-only reconciliation over the resulting
runtime facts with the runtime-controller factory, append decision or idle
evidence, and compare `m1_recovery_actions` count before/after using the admin
factory. Use the qualification factory to advance cursor/epoch state. Call
`verify_daemon_database_role` on both scoped factories with the exact temporary
database name and include only bounded counts/status in canonical output.

Close scoped connections, remove login memberships/roles, drop the exact
temporary database, then call the existing safe migration-role cleanup. Add
both capability roles to `_MIGRATION_CLUSTER_ROLES`; do not relax the
pre-existing-role collision check.

- [ ] **Step 4: Bind climb H-018 to revision 026 and scoped-role nodes**

Update the exact H-018 local gate to include:

```text
tests/alembic/test_026.py
tests/m1-perception/test_control_plane_db_role_contract.py
tests/m1-perception/test_control_plane_db_role_admin.py
tests/m1-perception/test_control_plane_qualification_identity.py
tests/m1-perception/test_control_plane_runtime_fault_matrix.py
```

Require output nodes `scoped-runtime-controller`,
`scoped-qualification-worker`, `zero-recovery-actions`, and
`qualification-identity-digest`; do not replace the existing topology,
adapter, fencing, replay, CLI, or restart nodes.

- [ ] **Step 5: Run the complete local gate twice and commit**

Run:

```bash
POLYARB_CONTROL_PLANE_TEST_DSN="$POLYARB_CONTROL_PLANE_TEST_DSN" make runtime-fault-matrix > /tmp/runtime-scoped-matrix-1.json
POLYARB_CONTROL_PLANE_TEST_DSN="$POLYARB_CONTROL_PLANE_TEST_DSN" make runtime-fault-matrix > /tmp/runtime-scoped-matrix-2.json
cmp /tmp/runtime-scoped-matrix-1.json /tmp/runtime-scoped-matrix-2.json
uv run pytest tests/alembic/test_026.py tests/m1-perception/test_control_plane_db_role_contract.py tests/m1-perception/test_control_plane_db_role_admin.py tests/m1-perception/test_control_plane_qualification_identity.py tests/m1-perception/test_control_plane_runtime_fault_matrix.py tests/climb/test_eval_local.py -q
uv run ruff check src/polyarb/control_plane tests/alembic/test_026.py tests/m1-perception/test_control_plane_runtime_fault_matrix.py tools/climb/eval_local.py
```

Expected: identical matrix files, 12/12 cases pass, both scoped profiles pass,
zero controller-created recovery actions, qualification cursor advances, and
no leaked database or role.

```bash
git add src/polyarb/control_plane/runtime_fault_matrix.py tests/m1-perception/test_control_plane_runtime_fault_matrix.py tools/climb/eval_local.py tests/climb/test_eval_local.py
git commit -m "test(05.6-207): prove scoped runtime authority end to end"
```

---

### Task 6: Truthful evidence, teaching, and Plan 207 closure

**Files:**
- Create: `docs/learning/89-数据库能力角色与进程身份.md`
- Modify: `docs/learning/00-INDEX.md`
- Modify: `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/evidence/runtime-observe-only.json`
- Modify: `.planning/workstreams/m1-perception/STATE.md`
- Modify: `.planning/JOURNAL.md`
- Modify: `.planning/threads/market-observation-architecture.md`
- Create: `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/05.6-207-SUMMARY.md`

**Interfaces:**
- Consumes: commits and verification receipts from Tasks 1-5.
- Produces: searchable closure with an exact local-vs-production boundary and next production authorization command.

- [ ] **Step 1: Update production evidence without claiming deployment**

Bump `runtime-observe-only.json` to artifact version 2 and record the facts
already established on 2026-08-25:

```json
{
  "production_database": {
    "database": "postgres",
    "applied_revisions": ["022", "023", "024", "025"],
    "revision_026_applied": false,
    "post_migration_worker_health": "pass",
    "qualification_incident_ingress_rows_at_audit": 1643
  },
  "production_operations": {
    "migration_022_through_025": true,
    "migration_026": false,
    "new_app_deployment": false,
    "scoped_login_role_change": false,
    "recovery_action_enablement": false,
    "fault_mutation": false
  }
}
```

Retain status `not-run` for the observe-only window. Do not alter job/process
recovery evidence except when necessary to point their prerequisites at the
still-NOT-RUN observe gate.

- [ ] **Step 2: Write teaching document 89 and index it**

Follow the established format: 30-second model, code map with exact file:line
references, capability-vs-login role explanation, positive/negative permission
examples, SECURITY DEFINER/search-path threat model, release/config identity,
operator sequence, five adversarial self-check questions, and FAQ increment.

The 30-second model must explain:

```text
LOGIN role = who connected and whose password can be rotated/disabled.
Capability role = the reviewed bundle of allowed database actions.
Startup contract = proof that effective authority is neither missing nor broader than expected.
```

- [ ] **Step 3: Run final verification**

Run:

```bash
uv run pytest tests/alembic/test_024.py tests/alembic/test_025.py tests/alembic/test_026.py tests/m1-perception/test_control_plane_db_role_contract.py tests/m1-perception/test_control_plane_db_role_admin.py tests/m1-perception/test_control_plane_qualification_identity.py tests/m1-perception/test_control_plane_rollout.py tests/m1-perception/test_control_plane_deployment_templates.py tests/m1-perception/test_control_plane_cli.py tests/m1-perception/test_control_plane_runtime_fault_matrix.py tests/m1-perception/test_makefile_contract.py tests/climb/test_eval_local.py -q
uv run ruff check alembic/versions/026_m1_runtime_scoped_roles.py src/polyarb/control_plane src/polyarb/cli_control_plane.py tests/alembic/test_026.py tests/m1-perception tools/climb/eval_local.py
uv run pyright src/polyarb/control_plane/db_role_contract.py src/polyarb/control_plane/db_role_admin.py src/polyarb/control_plane/qualification_identity.py src/polyarb/cli_control_plane.py
uv build
make planning-status
```

Expected: all focused tests, Ruff, Pyright, and build pass; planning status has
no drift. Production controller/qualification status and Dashboard smoke remain
NOT RUN because the new apps are not deployed.

- [ ] **Step 4: Create Summary and commit closure**

`05.6-207-SUMMARY.md` records every task commit, real-PostgreSQL permission
matrix result, repeated fault-matrix digest, exact tests, production database at
025, migration 026 NOT APPLIED in production, no new apps/secrets/login roles,
and the exact next authorization boundary.

Update STATE/JOURNAL/thread with the same truth. The next production command is
not migration or deploy; it is preparation of a new exact authorization package
for the final Task-5 commit SHA, revision 026, the two login roles, the two new
apps, observe-only mode, empty allowlist, rollback, and evidence directory.

```bash
git add docs/learning/89-数据库能力角色与进程身份.md docs/learning/00-INDEX.md .planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/evidence/runtime-observe-only.json .planning/workstreams/m1-perception/STATE.md .planning/JOURNAL.md .planning/threads/market-observation-architecture.md .planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/05.6-207-SUMMARY.md
git commit -m "docs(05.6-207): close scoped runtime role implementation"
make planning-status
```

Expected: commit succeeds through the SUMMARY guard and final planning status
is clean.
