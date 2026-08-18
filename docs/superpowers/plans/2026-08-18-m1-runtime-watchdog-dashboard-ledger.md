# M1 Runtime Watchdog Dashboard Ledger Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use inline test-driven execution. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Show every independent runtime watchdog incident and recovery in the cloud dashboard as well as Telegram.

**Architecture:** A private least-privilege writer app accepts authenticated, idempotent watchdog transition envelopes and appends them to the existing incident ledger. The established read-only API projects bounded current/history data to a dashboard route.

**Tech Stack:** Python 3.12, Starlette, psycopg/Postgres, Fly Machines, Next.js/TypeScript, pytest.

## Global Constraints

- Watchdog must not receive Postgres, R2, Gamma, CLOB, or scheduler credentials.
- API remains read-only; writer has append-only incident-ledger permissions.
- Event details are bounded/redacted and idempotent.
- Existing formal 24-hour acceptance topology remains running while this work is deployed.

### Task 1: Runtime ledger model and write endpoint

**Files:** `tests/m1-perception/test_control_plane_runtime_ledger.py`, `src/polyarb/control_plane/postgres.py`, `src/polyarb/control_plane/runtime_event_writer.py`.

- [ ] Write failing tests for authenticated transition validation, idempotent incident/recovery append, and detail redaction.
- [ ] Run `uv run pytest tests/m1-perception/test_control_plane_runtime_ledger.py -q`; expect failure.
- [ ] Implement the scoped writer model and private Starlette endpoint.
- [ ] Re-run focused tests; commit.

### Task 2: Watchdog delivery and API projection

**Files:** `tests/m1-perception/test_control_plane_watchdog.py`, `tests/m1-perception/test_control_plane_api.py`, `src/polyarb/control_plane/watchdog.py`, `src/polyarb/cli_control_plane.py`, `src/polyarb/control_plane/api.py`, `src/polyarb/control_plane/postgres.py`.

- [ ] Write red tests for write-before-Telegram, retry/pending warning, and bounded runtime timeline.
- [ ] Implement transition publisher, structured evidence, and read projection.
- [ ] Run focused tests; commit.

### Task 3: Cloud dashboard and deployment boundary

**Files:** `dashboard/app/control-plane/page.tsx`, `dashboard/lib/control-plane.ts`, `dashboard/lib/types.ts`, `dashboard/app/layout.tsx`, deployment templates, Makefile, dashboard tests.

- [ ] Write red validation/render tests for critical active and recovery views.
- [ ] Add route, typed fail-closed reader, writer config, health check, and Makefile commands.
- [ ] Run dashboard typecheck/build and Python focused tests; deploy writer and update watchdog targets; commit.

### Task 4: Controlled end-to-end proof and learning record

**Files:** evidence and `docs/learning/`.

- [ ] Trigger the existing sampler-loss probe and verify matching Telegram, writer receipt, dashboard incident and recovery.
- [ ] Confirm minimum roles/secrets and 24-hour sampler continuity.
- [ ] Record proof, run `make planning-status`, create plan SUMMARY, and commit.
