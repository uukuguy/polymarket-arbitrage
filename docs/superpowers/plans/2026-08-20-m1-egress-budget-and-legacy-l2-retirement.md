# M1 Egress Budget and Legacy L2 Retirement Implementation Plan

**Goal:** prevent retired L2 full-table Supabase downloads and make M1 cloud inputs budgeted, durable, visible, and fail-closed.

**Architecture:** Retire the old L2 cloud refresh at its entry boundary. Add a small Postgres usage ledger and transactionally evaluated UTC-day budget decision to the M1 control plane; attach threshold incidents to the existing Dashboard/outbox model. M1 admission proceeds only after bytes and a retained R2 authority reference are accepted.

**Tech Stack:** Python 3.12, psycopg, Alembic, pytest, Starlette, existing M1 incident/outbox/R2 contracts.

## Task 1 — retire L2 cloud input

Files: `src/polyarb/observation/l2_candidate_refresh.py`, `tests/observation/test_l2_candidate_refresh.py`.

1. Write `test_cloud_configured_candidate_refresh_is_retired_without_opening_a_client` that configures either runtime DSN or Supabase REST credentials and expects `LegacyL2CloudSourceRetiredError("legacy-l2-cloud-source-retired")`.
2. Run `uv run pytest tests/observation/test_l2_candidate_refresh.py -q`; it must fail before implementation.
3. Define that exception and raise it before `L3EvidenceStore(...)` or `create_client(...)`; retain only the local test compatibility path.
4. Re-run the focused suite and commit `fix(m1): retire legacy l2 cloud reader`.

## Task 2 — transactional budget ledger

Files: new `alembic/versions/021_m1_cloud_usage_budget.py`; `src/polyarb/control_plane/models.py`; `src/polyarb/control_plane/postgres.py`; `tests/m1-perception/test_control_plane_postgres.py`.

1. Write RED tests for `record_cloud_usage`: UTC-day accumulation, threshold deduplication at 50/75%, and 90% refusal.
2. Create `m1_cloud_usage_observations` with non-secret source/operation, bytes, item count, R2 key/digest, UTC day, and timestamp.
3. In one database transaction, require an artifact reference, append the fact, calculate same-day bytes under a lock, open exactly one existing incident/outbox intent per `cloud-egress:<threshold>:<day>`, and return `CloudUsageDecision(allowed, used_bytes, threshold_percent, observation_id)`; return denied at 90% and roll back on error.
4. Run the focused Postgres tests green and commit `feat(m1): gate cloud inputs by egress budget`.

## Task 3 — M1 admission and Dashboard projection

Files: `src/polyarb/control_plane/structure_source.py`, `src/polyarb/control_plane/postgres.py`, `src/polyarb/http/control_plane.py`, `Makefile`, `tests/m1-perception/test_control_plane_postgres.py`, `tests/m1-perception/test_dashboard_perception_contract.py`.

1. Write RED tests proving a 90% decision makes structure admission retryable with no successful receipt, and `/perception/control-plane` exposes secret-free `cloud_usage` fields.
2. Measure verified response bytes before the R2 write; invoke `record_cloud_usage` after artifact verification and before admission. Meter error or denial follows the existing retryable incident path.
3. Project used/budget bytes, current threshold, latest bounded observation, and incident identity through the control-plane API and Dashboard contract.
4. Add `make control-plane-egress-preflight`, reporting only safe budget decision fields and asserting no M1 template refers to `polyarb-l2`.
5. Run `uv run pytest tests/m1-perception/test_control_plane_postgres.py tests/m1-perception/test_dashboard_perception_contract.py -q && make planning-status`; commit `feat(m1): expose and enforce cloud egress budget`.
