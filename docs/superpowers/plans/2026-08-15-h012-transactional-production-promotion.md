# H-012 Transactional Production Promotion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use test-driven development. Execute inline in the isolated M1 worktree; do not deploy from this plan.

**Goal:** Make the Climb adapter prove that the existing transactional control plane can be promoted as the only formal M1 runtime, with no legacy-L1 fallback and no cloud mutation during local evaluation.

**Architecture:** Add a dedicated `transactional-production-promotion` gate profile. It validates the existing local rollout renderer, durable Postgres/R2 contracts, process-loss fencing, and the fail-closed soak verifier. H-012 records the policy decision in tracked Climb state; its local pass is pre-deployment evidence, while the final 24-hour production soak begins only after the later one-way promotion.

**Tech Stack:** Python 3.12, pytest, Makefile, existing `tools/climb` state machine.

## Global Constraints

- Existing `polyarb-control-plane-staging` Postgres/R2 authority is the promotion candidate; do not create a third Supabase project.
- `polyarb-l1` is a legacy implementation, not a runtime fallback or a rollout target.
- Gate commands must be local-only and must not contain a deploy, Fly mutation, database migration, or network URL.
- Do not put an experimental setting into a default configuration.
- Keep Climb state append-only and regenerate its research tree after state changes.

---

### Task 1: Add the H-012 local promotion gate profile

**Files:**

- Modify: `tests/climb/test_eval_local.py`
- Modify: `tools/climb/eval_local.py`

**Interfaces:**

- Consumes: `gate_commands_for({"paradigm": "transactional-production-promotion"})`
- Produces: a five-gate local-only command map with `planning`, `unit`, `integration`, `cli`, and `restart` keys.

- [ ] **Step 1: Write the failing test**

```python
def test_transactional_production_promotion_profile_uses_only_local_proof_gates() -> None:
    commands = eval_local.gate_commands_for(
        {"paradigm": "transactional-production-promotion"}
    )
    flattened = [argument for command in commands.values() for argument in command]

    assert commands["planning"] == ["make", "planning-status"]
    for required in (
        "tests/m1-perception/test_control_plane_postgres.py",
        "tests/m1-perception/test_control_plane_rollout.py",
        "tests/m1-perception/test_control_plane_shadow.py",
    ):
        assert required in flattened
    assert not {
        argument.lower()
        for argument in flattened
        if any(forbidden in argument.lower() for forbidden in ("flyctl", "deploy", "migrate", "http://", "https://"))
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/climb/test_eval_local.py::test_transactional_production_promotion_profile_uses_only_local_proof_gates -q`

Expected: FAIL because the new paradigm falls back to the generic gate profile.

- [ ] **Step 3: Write the minimal implementation**

```python
TRANSACTIONAL_PRODUCTION_PROMOTION_GATE_COMMANDS = {
    "planning": ["make", "planning-status"],
    "unit": ["uv", "run", "pytest", "tests/m1-perception/test_control_plane_postgres.py", "-q"],
    "integration": ["uv", "run", "pytest", "tests/m1-perception/test_control_plane_rollout.py", "tests/m1-perception/test_control_plane_shadow.py", "-q"],
    "cli": ["uv", "run", "pytest", "tests/m1-perception/test_control_plane_cli.py", "-k", "render_rollout or preflight", "-q"],
    "restart": ["uv", "run", "pytest", "tests/m1-perception/test_structure_generation_publication.py", "-k", "expired_read_budget or preserves_prior_checkpoint", "-q"],
}
```

Select this mapping only for `transactional-production-promotion`.

- [ ] **Step 4: Run the focused tests to verify they pass**

Run: `uv run pytest tests/climb/test_eval_local.py -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/climb/test_eval_local.py tools/climb/eval_local.py
git commit -m "feat(climb): add transactional promotion gates"
```

### Task 2: Register H-012 as the current promotion experiment

**Files:**

- Modify: `docs/status/climb/hypotheses.yaml`
- Modify: `docs/status/climb/session-state.json`
- Modify: `docs/status/climb/session-target.md`
- Generated: `docs/status/climb/research-tree.md`
- Generated: `docs/status/climb/research-tree.json`

**Interfaces:**

- Consumes: the H-012 gate profile from Task 1.
- Produces: a pending, highest-ranked H-012 whose final online evidence is a fresh 24-hour soak after one-way formal promotion.

- [ ] **Step 1: Add a failing state-contract assertion**

```python
def test_tracked_state_registers_one_way_transactional_promotion() -> None:
    hypotheses = yaml.safe_load((STATE_DIR / "hypotheses.yaml").read_text())
    by_id = {item["id"]: item for item in hypotheses["hypotheses"]}

    assert by_id["H-012"]["status"] == "pending"
    assert by_id["H-012"]["parent_paradigm"] == "transactional-production-promotion"
    assert "polyarb-l1" not in by_id["H-012"]["description"]
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/climb/test_adapter.py::test_tracked_state_registers_one_way_transactional_promotion -q`

Expected: FAIL because H-012 is absent.

- [ ] **Step 3: Register the state and regenerate the tree**

Append H-012 with a `pending` status, current session identifier, ranking `1.0`, and a precise decision record: existing transactional authority is promoted in place; old L1 is never a fallback; final production evidence is a new post-promotion 24-hour soak. Set `session-target.md` to the same authorized scope and set `session-state.json.next_action` to `run H-012 local promotion gates`.

- [ ] **Step 4: Run the state and generated-tree verification**

Run: `uv run python tools/climb/regen-tree.py && uv run pytest tests/climb/test_adapter.py -q`

Expected: PASS, with research tree listing H-012 as pending and current next action.

- [ ] **Step 5: Commit**

```bash
git add docs/status/climb tests/climb/test_adapter.py
git commit -m "docs(climb): register one-way transactional promotion"
```

### Task 3: Run the H-012 local cycle and record the pre-deployment verdict

**Files:**

- Generated (gitignored): `runs/climb/<timestamp>-h-012/`
- Modified by cycle: `docs/status/climb/hypotheses.yaml`, `docs/status/climb/runs.csv`, `docs/status/climb/session-state.json`, `docs/status/climb/research-tree.{md,json}`

**Interfaces:**

- Consumes: `tools/climb/cycle.sh H-012` and the Task 2 state.
- Produces: one append-only, local 100-point verdict or an explicit falsification with failing gate output.

- [ ] **Step 1: Run the bounded cycle**

Run: `tools/climb/cycle.sh H-012`

Expected: creates an isolated `runs/climb/*-h-012` directory and executes only local commands.

- [ ] **Step 2: Verify the generated state**

Run: `make climb-check && uv run pytest tests/climb -q`

Expected: state machine, generated research tree, and adapter contracts all pass.

- [ ] **Step 3: Commit the state transition**

```bash
git add docs/status/climb
git commit -m "test(climb): record transactional promotion preflight"
```
