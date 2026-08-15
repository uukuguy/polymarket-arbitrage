# M1 Cloud-Resident Soak Evidence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the local-only formal soak recorder with a cloud-resident,
append-only Postgres evidence ledger and a separately scheduled Fly sampler.

**Architecture:** Migration 017 supplies a locked run plus immutable canonical
observations. `PostgresControlPlane` owns transactional ledger operations.
`cli_control_plane` adds cloud start/sample/verify commands and a bounded
sampler service. The existing worker template receives one no-volume sampler
process, separate from the five data-plane roles.

**Tech Stack:** Python 3.12, psycopg, urllib stdlib HTTPS, PostgreSQL 16,
Alembic, Fly Machines API, pytest, Ruff, Make.

## Global Constraints

- The formal data plane remains exactly coordinator + two Structure + two Quote roles.
- `soak_sampler` has no SQLite path, R2 client, public HTTP service or mount.
- Observation records retain V2 canonical digest and existing pure verification semantics.
- Sampling failures create gaps; no retry may backfill timestamps.
- All operator commands are exposed through the Makefile.

---

### Task 1: Versioned immutable cloud ledger

**Files:**
- Create: `alembic/versions/017_m1_cloud_soak_evidence.py`
- Create: `tests/alembic/test_017.py`
- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `tests/m1-perception/test_control_plane_postgres.py`

**Interfaces:** `PostgresControlPlane.start_soak_run(...)`,
`append_soak_observation(...)`, and `read_soak_observations(run_id)` make the
database the only durable writer of cloud evidence.

- [ ] Write migration and integration tests first: assert run identity/locked
  topology, idempotent exact-digest append, conflicting append rejection and
  ordered reads.
- [ ] Implement migration 017 with `m1_soak_runs` and
  `m1_soak_observations`, foreign key, uniqueness constraints and digest
  checks; implement short transaction methods with `INSERT ... ON CONFLICT`
  only for byte-identical observations.
- [ ] Run `uv run pytest tests/alembic/test_017.py
  tests/m1-perception/test_control_plane_postgres.py -k soak -q` and Ruff.
- [ ] Commit `feat(m1): persist cloud transactional soak evidence`.

### Task 2: Cloud sampler and remote verifier

**Files:**
- Modify: `src/polyarb/cli_control_plane.py`
- Modify: `src/polyarb/control_plane/soak_evidence.py`
- Modify: `tests/m1-perception/test_control_plane_cli.py`
- Modify: `tests/m1-perception/test_control_plane_soak_evidence.py`

**Interfaces:** Add `cloud-soak-start`, `cloud-soak-sample`,
`cloud-soak-serve`, and `cloud-soak-verify`.  The service receives a fixed
run ID, URL, Fly app, Machine IDs and interval; it delegates final decisions
to `verify_soak` after ledger reads.

- [ ] Write CLI tests with injected API/Machines readers and a fake ledger;
  prove failed reads do not append and remote verification produces the same
  result as JSONL verification.
- [ ] Implement a direct Fly Machines API reader using a bearer token from
  `POLYARB_FLY_API_TOKEN`, then implement the bounded signal-aware sampler
  loop with no timestamp retry/backfill.
- [ ] Run focused CLI/evidence tests and Ruff.
- [ ] Commit `feat(m1): sample and verify soak evidence in cloud`.

### Task 3: Deployment and operator contracts

**Files:**
- Modify: `deploy/control-plane/fly-control-worker.toml.template`
- Modify: `tests/m1-perception/test_control_plane_deployment_templates.py`
- Modify: `Makefile`
- Modify: `docs/learning/76-真实R2故障接管与持续证据.md`
- Modify: `docs/learning/00-INDEX.md` if a new learning note is created

**Interfaces:** `make control-plane-cloud-soak-start`,
`control-plane-cloud-soak-verify`, and `control-plane-cloud-soak-serve`
provide uniform command access; template makes `soak_sampler` a sixth,
independent process that does not change the five data-plane roles.

- [ ] Assert template includes the six named process entries, no mounts/HTTP,
  and an explicit five-minute sampler command with the five locked IDs.
- [ ] Implement targets and explain why evidence collector isolation matters.
- [ ] Run template/manual contract tests and `make help | rg cloud-soak`.
- [ ] Commit `feat(m1): operate cloud-resident soak sampler`.

### Task 4: Formal deploy and fresh cloud acceptance

**Files:**
- Create: `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/05.6-196-SUMMARY.md`
- Create: `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/evidence/formal-cloud-transactional-soak-run.json`

- [ ] Apply Alembic 017 to the existing formal Supabase authority and prove
  the table identities through a read-only preflight.
- [ ] Build/push the Worker image, add one Fly sampler Machine, set only its
  read-only Machines API token, and confirm the original five data roles
  remain started and mount-free.
- [ ] Start a fresh cloud run, unload the local LaunchAgent, and confirm two
  natural ledger observations arrive without a local computer dependency.
- [ ] After 24 continuous hours, run the remote strict verifier, retain its
  JSON result, complete the phase summary, and run `make planning-status`.
