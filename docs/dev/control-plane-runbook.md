# Control Plane Scoped Database Role Runbook

This runbook covers only the two scoped login roles for revision `026`:
`m1_runtime_controller_login` and `m1_qualification_worker_login`. It does not
create capability roles, change schema revision, deploy Fly apps, install Fly
secrets, inject faults, enable recovery execution, restart machines, or downgrade
the database.

## Authorization Boundary

Production use requires an explicit written authorization that names the exact
database, the revision `026` target, the operator, and the time window. Without
that authorization, run these commands only against disposable PostgreSQL or a
non-production staging database.

The production default remains observe-only. The production recovery allowlist is
empty unless separately authorized and deployed by the runtime rollout process.

## Secret Handling

Store generated passwords in the local macOS Keychain or the team's approved
secret manager. Do not place passwords in shell history, `.env`, docs, tickets,
logs, screenshots, or command transcripts.

The provision command reads exactly these environment variables:

- `POLYARB_RUNTIME_CONTROLLER_DB_PASSWORD`
- `POLYARB_QUALIFICATION_WORKER_DB_PASSWORD`

Generate the two passwords independently. Reusing the same value is rejected by
the tooling.

After local provision succeeds, install the resulting scoped DSNs as independent
Fly secrets for the two apps through the approved Fly secret workflow:

- runtime controller app: `POLYARB_RUNTIME_CONTROLLER_DB_DSN`
- qualification worker app: `POLYARB_QUALIFICATION_WORKER_DB_DSN`

Do not share one scoped DSN between both apps.

## Operator Flow

1. Confirm the target database is non-production, or confirm the production
   authorization boundary above is satisfied.
2. Export only the admin DSN required for the selected database:
   `POLYARB_SUPABASE_DB_DSN`.
3. Run the read-only capability-role gate:
   `make control-plane-db-role-preflight expected_database=<database-name>`.
4. Retrieve the two independent passwords from Keychain into environment
   variables without echoing them.
5. Create or rotate both login roles in one transaction:
   `make control-plane-db-role-provision enable=1 expected_database=<database-name>`.
6. Build each scoped DSN locally and verify each profile before using it in an
   app:
   `make control-plane-db-role-verify profile=runtime-controller expected_database=<database-name>`
   and
   `make control-plane-db-role-verify profile=qualification-worker expected_database=<database-name>`.
7. Install the runtime-controller and qualification-worker DSNs as independent Fly
   secrets only after verification passes and only within the authorized
   environment.

## Disable Flow

Stop both dependent apps before disabling logins. The runtime controller and
qualification worker must be stopped first, then the admin operator may run:

`make control-plane-db-role-disable enable=1 expected_database=<database-name>`.

Disable uses `NOLOGIN` only. It does not revoke capability memberships, drop
roles, modify schema, touch Fly, or alter production app state.
