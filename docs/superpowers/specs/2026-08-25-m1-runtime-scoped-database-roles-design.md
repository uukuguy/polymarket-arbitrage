# M1 Runtime Scoped Database Roles Design

**Date:** 2026-08-25

**Status:** Approved interactively; awaiting written-spec review

**Scope:** Database authority and deployment identity for the private M1
runtime controller and rolling qualification worker

## Context

The production database is at Alembic revision `025`, but neither
`polyarb-runtime-controller-m1` nor `polyarb-qualification-worker-m1` has been
created or deployed. The rendered rollout correctly says that each app must
receive a scoped Postgres DSN, yet revisions `022` through `025` do not create
those capabilities. Deploying either app with the existing `postgres` or
`service_role` credential would contradict the rollout's least-authority
boundary.

A read-only post-migration audit established that the existing worker, runtime
event writer, and alert service remained healthy after revision `025`. The
qualification ingress ledger contained 1,643 incident facts and its latest row
matched the latest incident event. The same audit showed that the existing
control worker and runtime event writer currently connect as `postgres`. That
pre-existing credential debt is not expanded by this design and must not be
copied into the two new apps.

Revision `026` will add two NOLOGIN capability roles, harden qualification
ingress functions, and expose a constrained freshness-ingress function. Login
roles and passwords remain environment-specific operator resources and are not
embedded in Alembic history.

## Goals

1. Give the observe-only runtime controller only the table access used by its
   current code path.
2. Give the qualification worker only the reads and state transitions needed
   to build rolling epochs and immutable certificates.
3. Keep credentials independent: compromise of either app must not grant the
   other app's authority.
4. Preserve automatic runtime, incident, and recovery ingress projection for
   current and future restricted producers without granting them direct ledger
   writes.
5. Make release, qualification configuration, database login, and capability
   identity explicit and fail closed at process startup.
6. Prove both required and forbidden permissions against a real PostgreSQL
   migration path before production deployment.

## Non-goals

- Revision `026` does not enable recovery execution. The controller remains
  `observe-only`, the recovery allowlist remains empty, and it receives no Fly
  API token.
- It does not migrate the four existing production apps away from their current
  database login. That is a separate credential-rotation change with its own
  availability and rollback plan.
- It does not grant either new app R2, Telegram, Gamma, CLOB, sampler, worker,
  wallet, signing, order, or trading authority.
- It does not create production passwords, write Fly secrets, create Fly apps,
  deploy images, or downgrade the production database.
- It does not grant the observe-only role dormant execute-mode permissions.
  Enabling recovery execution requires a new capability review and migration.

## Role model

### Capability roles managed by Alembic

Revision `026` creates:

- `m1_runtime_controller_capability`
- `m1_qualification_worker_capability`

Both roles are:

```text
NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT
NOREPLICATION NOBYPASSRLS
```

The migration owns only these durable capability roles and their object
grants. It never stores a password. Existing roles with either name are
accepted only when every security attribute matches the contract; a collision
with LOGIN, SUPERUSER, CREATEDB, CREATEROLE, REPLICATION, or BYPASSRLS fails the
migration. Re-running the upgrade after a clean downgrade must reproduce the
same effective grants.

### Login roles managed by an operator command

Production uses two independent logins:

- `m1_runtime_controller_login`
- `m1_qualification_worker_login`

Each login is `LOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
NOBYPASSRLS`, belongs to exactly its matching capability role, and has an
independent randomly generated password. The provisioning command reads
passwords from process input, composes role names with `psycopg.sql.Identifier`
and password literals with `psycopg.sql.Literal`, and never prints the SQL,
password, or DSN. PostgreSQL 16 rejects a bind parameter in `ALTER ROLE ...
PASSWORD $1`, so the safely escaped literal is the approved grammar-compatible
exception to the normal bind-parameter rule. It fails if an existing login has unsafe attributes or an
unexpected role membership; it does not silently absorb a pre-existing broad
role.

Passwords are stored in the operator's macOS Keychain and as separate Fly
secrets. Admin preflight, provisioning, and disablement use the operator-only
`POLYARB_CONTROL_PLANE_DB_ADMIN_DSN`; it is never installed in either app. The
runtime controller receives `POLYARB_SUPABASE_DB_DSN`; the qualification worker
receives `POLYARB_QUALIFICATION_DB_DSN`. Neither app receives the other DSN.

## Exact permission matrix

All grants include `CONNECT` to the selected database and `USAGE` on schema
`public`. No role owns application objects. No `DELETE`, `TRUNCATE`, `REFERENCES`,
or `TRIGGER` privilege is granted.

### Observe-only runtime controller

| Object | SELECT | INSERT | UPDATE | Reason |
|---|---:|---:|---:|---|
| `m1_runtime_controller_leases` | yes | yes | yes | claim and renew the fenced controller lease |
| `m1_runtime_observe_decisions` | yes | yes | no | idempotency comparison and append-only decisions |
| `m1_job_runtime_state` | yes | no | no | reconcile persisted deadline facts |
| `m1_jobs` | yes | no | no | job type, state, retry count, and failure class |
| `m1_job_circuits` | yes | no | no | circuit state and probe timing |
| `m1_job_attempts` | yes | no | no | terminal attempt failure class |
| `m1_recovery_target_budgets` | yes | no | no | evaluate remaining budget without consuming it |
| `m1_recovery_actions` | yes | no | no | prove that an observe-only window created no actions |

The role receives no sequence privileges and no execute privilege on recovery
functions. In particular, effective `INSERT` or `UPDATE` on
`m1_recovery_actions`, `m1_recovery_target_budgets`, `m1_jobs`,
`m1_job_runtime_state`, `m1_job_runtime_events`, `m1_incidents`,
`m1_incident_events`, or `m1_alert_outbox` must be false.

### Rolling qualification worker

| Object | SELECT | INSERT | UPDATE | Reason |
|---|---:|---:|---:|---|
| `m1_qualification_ingress_ledger` | yes | no | no | consume monotonic runtime/incident/recovery/freshness facts |
| `m1_qualification_source_cursors` | yes | yes | yes | atomically fence the source cursor |
| `m1_qualification_epochs` | yes | yes | yes | create and transition rolling epochs |
| `m1_qualification_recovery_observations` | yes | yes | no | append and verify recovering-epoch evidence |
| `m1_qualification_certificates` | yes | no | no | replay and reverify immutable certificates |
| `m1_publication_pointers` | yes | no | no | derive structure and quote freshness |
| `m1_generation_manifests` | yes | no | no | derive publication time and counts |
| `m1_opportunity_publication_pointers` | yes | no | no | derive opportunity freshness |
| `m1_opportunity_projections` | yes | no | no | derive opportunity certification time and counts |

The role receives `EXECUTE` only on:

- `m1_record_qualification_freshness_ingress(text, text, timestamptz, jsonb)`;
- `m1_insert_qualification_certificate(text, text, text, text, jsonb,
  timestamptz, timestamptz, jsonb, text, text, text, text)`.

It receives no direct ledger INSERT or identity-sequence privilege. It receives
no write privilege on runtime, job, incident, recovery, alert, publication, or
opportunity tables, and no direct INSERT/UPDATE/DELETE privilege on
certificates.

## Qualification ingress hardening

Revision `024` created regular invoker-rights trigger functions that eventually
insert into `m1_qualification_ingress_ledger`. That works today because the
existing producers connect as `postgres`, but it would force future restricted
producer roles to receive ledger INSERT and sequence privileges.

Revision `026` closes that chain:

1. `m1_record_qualification_ingress(...)` becomes `SECURITY DEFINER`, remains
   owned by the migration owner, sets `search_path = pg_catalog`, and uses
   schema-qualified application objects.
2. Its `EXECUTE` privilege is revoked from `PUBLIC`, `anon`, `authenticated`,
   `service_role`, and both new capability roles.
3. The three projection trigger functions for runtime events, incident events,
   and recovery actions become `SECURITY DEFINER` with the same locked path and
   schema qualification.
   They are the only general-source callers. Direct producer writes still pass
   through the existing source-table permissions, constraints, and triggers.
4. A new `m1_record_qualification_freshness_ingress(...)` SECURITY DEFINER
   function accepts no source argument, always records source `freshness`,
   validates the data product and bounded JSON object, and delegates to the
   internal recorder. Only `m1_qualification_worker_capability` may execute it.
5. The Python qualification fact source calls the new freshness-only function.

Every SECURITY DEFINER function uses schema-qualified objects with
`search_path = pg_catalog`, is not owned by either application role, and has
PUBLIC execute revoked. Revision `026` also hardens the existing certificate
insertion and validation chain to the same search-path rule without changing
its constraints or payload contract, then adds execute permission only for the
qualification capability.

This produces an end-to-end chain:

```text
producer INSERT/UPDATE
  -> source-table trigger
  -> fixed-source SECURITY DEFINER projector
  -> internal qualification ingress recorder
  -> immutable monotonic ingress ledger

qualification freshness read
  -> freshness-only SECURITY DEFINER function
  -> internal qualification ingress recorder
  -> the same ingress ledger
```

## Explicit process identity

The generated Fly configuration must not use qualification defaults. It renders
all of the following:

```text
POLYARB_QUALIFICATION_RELEASE_ID=<exact image Git SHA>
POLYARB_QUALIFICATION_CONFIG_ID=sha256:<canonical qualification config digest>
POLYARB_QUALIFICATION_ROLE_IDENTITY=opportunity,quote,structure
```

The canonical configuration digest covers at least the policy version,
required qualification duration, maximum evidence gap, signature budget,
service interval, batch size, ordered role identity, runtime recovery mode, and
runtime recovery allowlist. The rollout checklist records both the canonical
payload and digest.

`qualification-serve` rejects `release-unknown`, `config-unknown`, an empty role
identity, or a supplied config digest that does not match the rendered payload.
The runtime controller and qualification worker each perform a read-only startup
identity check:

- `session_user` is the exact expected login role;
- the login is a member of only the expected application capability among the
  two new capabilities;
- that PostgreSQL 16 membership is exactly `ADMIN FALSE, INHERIT TRUE, SET TRUE`;
- it is not superuser and cannot bypass RLS;
- its DSN contains no `options` or `search_path` override, its active path is
  exactly `pg_catalog,public`, and neither role nor database config supplies a
  competing path;
- required effective privileges are present;
- explicitly forbidden effective privileges are absent across every non-system
  schema, including database/schema `CREATE`, object ownership, relation and
  sequence authority, and SECURITY DEFINER execution.

Every application relation and routine used by either daemon is explicitly
qualified with `public.`. Database `TEMPORARY` remains allowed for compatibility
with the original four applications and PostgreSQL's default PUBLIC posture;
it is not persistent schema authority, while database `CREATE` is forbidden.

A failed identity or permission check exits non-zero before the first lease,
observe decision, freshness fact, epoch, or certificate write. The health/log
surface reports a bounded reason code, never a DSN or password.

## Migration and provisioning behavior

Revision `026` is additive for application data:

- it creates no login or secret;
- it does not rewrite, delete, or invalidate existing facts;
- it does not enable either new daemon;
- existing source-table writes continue projecting qualification ingress;
- Alembic remains at one head.

The operator tooling provides Makefile entry points for role preflight,
provisioning, effective-permission verification, and credential disablement.
Provisioning is separate from migration so the same schema can be promoted
without carrying environment credentials in Git or Alembic logs.

The collision policy is fail closed. Capability-role attributes, login-role
attributes, membership option columns, database name, schema revision,
configured namespace, application-object ownership, and effective grants must
match exactly before a Fly secret may be installed. An unexpected broad grant
through `PUBLIC`, another inherited role, or a non-public application schema
fails verification even if the explicit public grants look correct.

## Verification

### Static and unit tests

- Migration source asserts both capability roles have the required security
  attributes and explicit grants.
- Rollout rendering asserts the three qualification identity variables are
  present and contain no unknown defaults.
- CLI tests prove both daemons exit before mutation on role mismatch, unsafe
  privilege, missing release identity, or config-digest mismatch.
- Function-source tests prove fixed search paths, PUBLIC revocation, and the
  freshness-only source boundary.

### Real PostgreSQL tests

The real-Postgres lane runs `025 -> 026`, verifies the head, removes any
disposable login membership, downgrades in the isolated test database, and
reapplies `026`. Production never exercises this downgrade. With `SET ROLE` or
disposable login roles the lane proves:

- every positive cell in the permission matrix succeeds;
- controller lease claim/renew and observe-decision append succeed;
- observe-only reconciliation cannot create a recovery action or mutate job,
  incident, recovery, or alert state;
- qualification freshness insertion records only source `freshness`;
- runtime, incident, and recovery source writes still project ingress while
  the producer lacks direct ledger INSERT and sequence privileges;
- qualification cursor, epoch, recovery-observation, and certificate paths
  succeed end to end;
- certificate and recovery-observation mutation remains rejected;
- cross-capability access and all stated forbidden writes fail with
  `InsufficientPrivilege`;
- role-name collisions with unsafe attributes abort the migration;
- no secret value appears in command output or captured logs.

The existing deterministic 12-fault matrix then runs with the two scoped DSNs.
The controller must remain observe-only with an empty allowlist and zero
recovery actions. The qualification worker must consume the resulting facts
without permission fallback.

## Production rollout

Production rollout is a separate, exact authorization against the new commit
SHA. Its order is:

1. run the post-`025` health and freshness audit;
2. apply revision `026` and verify the effective privilege contract;
3. generate and store two independent login credentials without printing them;
4. provision the two login roles and verify each scoped DSN independently;
5. render configs with an exact release SHA and qualification config digest;
6. create the two private Fly apps and install only their matching DSN secret;
7. deploy the same immutable image to both apps;
8. confirm controller lease/observe cadence, zero recovery actions,
   qualification ingress/cursor movement, Dashboard visibility, alert health,
   and bounded log silence for permission errors;
9. start the authorized observe-only evidence window.

No step installs a Fly token on the runtime controller. No fault injection,
recovery action, Machine restart, or trading action is part of this rollout.

## Rollback

Rollback is operational and additive:

1. stop both new Fly apps;
2. disable both login roles with `NOLOGIN` and terminate only their identified
   database sessions if necessary;
3. leave revision `026`, its capability roles, functions, and existing data in
   place;
4. verify the original four production apps and qualification ingress remain
   healthy.

Production does not downgrade from `026`. Removing capability roles or
functions while trigger chains or login memberships exist is intentionally not
an emergency rollback mechanism.

## Acceptance criteria

This design is complete when:

- revision `026` passes static, migration, repeat-upgrade, and real-Postgres
  positive/negative permission tests;
- the two rendered apps contain explicit release/config/role identities and no
  forbidden credentials;
- startup rejects broad or mismatched database identities before mutation;
- the 12-fault matrix passes using scoped credentials in observe-only mode;
- a new exact production deployment package is reviewed and authorized;
- production evidence proves timely observe decisions, qualification progress,
  zero recovery actions, visible incidents, healthy alerts, and no permission
  fallback.
