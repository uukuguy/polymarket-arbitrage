# M1 Fail-Closed Fly Topology Audit Implementation Plan

**Goal:** Confirm climb H-020 with a credential-free Fly topology and secret-provenance audit exposed through Make.

**Architecture:** A small pure parser validates provider JSON into a closed output schema. A narrow subprocess adapter captures all provider output and converts every failure to a bounded reason code. Tests inject fake provider responses; only the final explicitly invoked production audit contacts Fly, read-only.

**Tech Stack:** Python 3.12, argparse, subprocess, JSON, pytest, Make, climb.

### Task 1: Establish red security and topology contracts

**Files:**
- Create: `tests/m1-perception/test_control_plane_fly_topology_audit.py`
- Modify: `tests/m1-perception/test_makefile_contract.py`

- [ ] Prove the success output contains only allowlisted fields and never env values.
- [ ] Prove password-bearing ordinary env keys, malformed/provider failures, unexpected Machines, and missing required secrets fail without echoing sensitive bodies.
- [ ] Prove exact Make argv and read-only command vocabulary.
- [ ] Run focused tests to RED.

### Task 2: Implement and wire the audit

**Files:**
- Create: `src/polyarb/control_plane/fly_topology_audit.py`
- Modify: `Makefile`

- [ ] Implement bounded identifiers, credential-key detection, exact topology comparison, required-secret presence, and sanitized errors.
- [ ] Force child `FLY_API_TOKEN` empty and allow only status/secrets-list commands.
- [ ] Add `make control-plane-fly-topology-audit` and `make help` visibility.
- [ ] Run focused tests and lint to GREEN.

### Task 3: Bind climb and collect read-only production evidence

**Files:**
- Modify: `tools/climb/eval_local.py`
- Modify: `tests/climb/test_eval_local.py`
- Update: `docs/status/climb/*` through `tools/climb/cycle.sh H-020`
- Create: secret-free audit evidence in the fresh exact authorization directory

- [ ] Add audit tests to the deterministic production-enablement gate profile.
- [ ] Run H-020 locally and commit the append-only 100/100 result.
- [ ] Run the exact read-only audit against the four existing apps, requiring the runtime-event-writer's canonical DSN and writer-token secret names.
- [ ] Use only the sanitized artifact in the refreshed production authorization package.
