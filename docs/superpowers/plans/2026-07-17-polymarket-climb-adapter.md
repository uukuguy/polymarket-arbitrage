# Polymarket Climb Adapter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a repository-tracked climb adapter that scores autonomous GSD development cycles from local quality gates and can resume from disk without relying on conversation memory.

**Architecture:** Treat each bounded implementation hypothesis as a climb run. `train.sh` creates an isolated run manifest for the code change already authored by the LLM outer loop; `eval-local.sh` runs deterministic project gates and emits a 0–100 score with five subscores. `cycle.sh` appends the run, updates hypothesis status, regenerates the research tree, and checks the best-effort target. `push.sh` only writes a local verification artifact because this phase authorizes no exchange, deploy, or external submission.

**Tech Stack:** POSIX shell, Python 3.12 stdlib, YAML state files, CSV/JSON/Markdown, pytest, uv, Makefile, git hooks.

## Global Constraints

- State lives under tracked `docs/status/climb/`; generated run artifacts live under ignored `runs/climb/`.
- State-machine history is append-only; corrections add superseding records.
- `research-tree.md` is generated deterministically with no wall-clock timestamp in its content.
- No external push, deployment, exchange request, credential use, or subprocess that contacts a third-party service.
- A failed gate scores zero for that subscore and returns non-zero; it is never hidden.
- Do not modify or stage the user's `CLAUDE.md` type change or untracked `AGENTS.md`.

## File Structure

- Create `docs/status/climb/config.yaml`: project scoring and path adapter.
- Create `docs/status/climb/session-target.md`, `hypotheses.yaml`, `runs.csv`, `calibration.json`, `pending-lb.json`, `session-state.json`, `adjudicator-log.md`, `research-tree.json`, and generated `research-tree.md`.
- Create `tools/climb/train.sh`: run directory and manifest creation.
- Create `tools/climb/eval_local.py` plus `eval-local.sh`: deterministic gate execution and scoring.
- Create `tools/climb/cycle.py` plus `cycle.sh`: append-only synchronization and next-action state.
- Create `tools/climb/regen-tree.py`, `check-target.py`, `push.sh`, `apply-lb-score.sh`, `consult-ais.sh`, and `hooks/post-commit`.
- Create `tests/climb/test_adapter.py` and `tests/climb/test_eval_local.py`.
- Modify `.gitignore` and `Makefile`.

---

### Task 1: Scaffold tracked state and validate the adapter contract

**Files:**
- Create: `docs/status/climb/config.yaml`
- Create: `docs/status/climb/session-target.md`
- Create: `docs/status/climb/hypotheses.yaml`
- Create: `docs/status/climb/runs.csv`
- Create: `docs/status/climb/calibration.json`
- Create: `docs/status/climb/pending-lb.json`
- Create: `docs/status/climb/session-state.json`
- Create: `docs/status/climb/adjudicator-log.md`
- Create: `docs/status/climb/research-tree.json`
- Create: `tests/climb/test_adapter.py`
- Modify: `.gitignore`

**Interfaces:**
- Produces the canonical paths and schema read by every later climb task.

- [ ] **Step 1: Write failing adapter contract tests**

```python
def test_config_declares_local_phase_gate_adapter(repo_root):
    cfg = yaml.safe_load((repo_root / "docs/status/climb/config.yaml").read_text())
    assert cfg == {
        "score_name": "phase_gate_score",
        "score_direction": "max",
        "subscores": ["planning", "unit", "integration", "cli", "restart"],
        "push_mode": "manual-csv",
        "state_dir": "docs/status/climb",
        "artifact_dir": "runs/climb",
        "run_tag_marker": "-climb-",
        "paradigm_field": "implementation_hypothesis",
    }


def test_state_files_exist_and_runs_header_matches_contract(repo_root):
    state = repo_root / "docs/status/climb"
    required = {
        "session-target.md", "hypotheses.yaml", "runs.csv", "calibration.json",
        "pending-lb.json", "session-state.json", "adjudicator-log.md",
        "research-tree.json",
    }
    assert required <= {p.name for p in state.iterdir()}
    assert (state / "runs.csv").read_text().splitlines()[0] == (
        "run_id,cycle,session,hypothesis_id,paradigm,parent_run,pushed_at,"
        "local_score,planning,unit,integration,cli,restart,push_decision,"
        "decision_reason,verdict,cost_h,manifest_path"
    )
```

- [ ] **Step 2: Run and confirm RED**

Run: `uv run pytest tests/climb/test_adapter.py -q`

Expected: missing adapter files.

- [ ] **Step 3: Add exact adapter configuration**

```yaml
score_name: phase_gate_score
score_direction: max
subscores: [planning, unit, integration, cli, restart]
push_mode: manual-csv
state_dir: docs/status/climb
artifact_dir: runs/climb
run_tag_marker: -climb-
paradigm_field: implementation_hypothesis
```

Initialize best-effort target with `target_value:` empty. Seed hypotheses with:

```yaml
hypotheses:
  - id: H-001
    description: Transactional SQLite repository prevents cross-process state loss
    parent_paradigm: repository-backed-domain-model
    expected_lift: "+100 phase gate points"
    cost_h: 2.0
    ranking: 1.0
    status: pending
    created_at: 2026-07-17T00:00:00+08:00
    results: []
```

Initialize JSON files with explicit empty objects/lists and `session-state.json` with `last_cycle: 0`, `in_flight: null`, and `next_action: "run H-001"`.

- [ ] **Step 4: Ignore only run artifacts**

Add:

```gitignore
# climb execution artifacts; canonical state is tracked in docs/status/climb/
runs/climb/
```

- [ ] **Step 5: Run contract tests**

Run: `uv run pytest tests/climb/test_adapter.py -q`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add .gitignore docs/status/climb tests/climb/test_adapter.py
git commit -m "feat(climb): add polymarket project adapter state"
```

### Task 2: Build deterministic run manifests and local gate scoring

**Files:**
- Create: `tools/climb/train.sh`
- Create: `tools/climb/eval_local.py`
- Create: `tools/climb/eval-local.sh`
- Create: `tests/climb/test_eval_local.py`

**Interfaces:**
- `train.sh <hypothesis_id>` prints the created run directory and writes `manifest.json`.
- `eval-local.sh <run_dir>` writes `<run_dir>/local-eval.json` and prints the same JSON.
- Local JSON fields: `total`, `subscores`, `commands`, `disaster_pattern`.

- [ ] **Step 1: Write failing scorer tests with an injected command runner**

```python
def test_score_is_mean_of_five_binary_gates():
    results = {
        "planning": GateResult(True, 0, "ok"),
        "unit": GateResult(True, 0, "ok"),
        "integration": GateResult(False, 1, "failed"),
        "cli": GateResult(True, 0, "ok"),
        "restart": GateResult(False, 1, "failed"),
    }
    payload = build_score(results)
    assert payload["total"] == 60.0
    assert payload["subscores"] == {
        "planning": 100.0, "unit": 100.0, "integration": 0.0,
        "cli": 100.0, "restart": 0.0,
    }
    assert payload["disaster_pattern"] is True
```

- [ ] **Step 2: Run and confirm RED**

Run: `uv run pytest tests/climb/test_eval_local.py -q`

Expected: `tools.climb.eval_local` is missing.

- [ ] **Step 3: Implement the five gates**

Use exact commands:

```python
GATE_COMMANDS = {
    "planning": ["make", "planning-status"],
    "unit": ["uv", "run", "pytest", "tests/routing/test_position_repository.py", "tests/routing/test_position_tracker.py", "-q"],
    "integration": ["uv", "run", "pytest", "tests/execution", "-q"],
    "cli": ["uv", "run", "pytest", "tests/cli", "-q"],
    "restart": ["uv", "run", "pytest", "tests/cli/test_arbitrage_cli_process.py", "-q"],
}
```

Missing future test files count as failed gates until their hypothesis creates them. Capture bounded stdout/stderr and exit code for each command. `total` is the arithmetic mean of five 0/100 subscores; any zero sets `disaster_pattern=true`.

- [ ] **Step 4: Implement `train.sh` manifest creation**

The script validates `H-NNN`, creates `runs/climb/YYYYMMDD-HHMMSS-hnnn`, and writes:

```json
{
  "hypothesis_id": "H-001",
  "paradigm": "repository-backed-domain-model",
  "git_head": "<resolved by git rev-parse HEAD>",
  "status": "ready-for-eval"
}
```

The timestamp belongs in the artifact path, not generated tracked state.

- [ ] **Step 5: Run tests and a manifest smoke test**

Run:

```bash
uv run pytest tests/climb/test_eval_local.py -q
tools/climb/train.sh H-001
```

Expected: tests PASS and one ignored run directory is printed.

- [ ] **Step 6: Commit**

```bash
git add tools/climb/train.sh tools/climb/eval_local.py tools/climb/eval-local.sh tests/climb/test_eval_local.py
git commit -m "feat(climb): score local gsd quality gates"
```

### Task 3: Add append-only cycle synchronization and deterministic research tree

**Files:**
- Create: `tools/climb/cycle.py`
- Create: `tools/climb/cycle.sh`
- Create: `tools/climb/regen-tree.py`
- Create: `tools/climb/check-target.py`
- Create: `tools/climb/push.sh`
- Create: `tools/climb/apply-lb-score.sh`
- Create: `tools/climb/consult-ais.sh`
- Create: `tools/climb/hooks/post-commit`
- Create: `docs/status/climb/research-tree.md`
- Modify: `tests/climb/test_adapter.py`
- Modify: `Makefile`

**Interfaces:**
- `cycle.sh <hypothesis_id>` creates/evaluates a run, appends one CSV record, updates the hypothesis, regenerates the tree, and writes `session-state.next_action`.
- `push.sh <run_dir>` writes local `verification-artifact.json`; it never contacts a network.
- Make targets: `make climb-status`, `make climb-cycle hypothesis=H-001`, `make climb-check`.

- [ ] **Step 1: Add failing deterministic synchronization tests**

```python
def test_regen_tree_is_deterministic(tmp_path, climb_fixture):
    first = regenerate(climb_fixture.state_dir)
    second = regenerate(climb_fixture.state_dir)
    assert first == second


def test_cycle_appends_exactly_one_run_and_advances_state(climb_fixture):
    sync_cycle(climb_fixture.completed_run)
    rows = list(csv.DictReader(climb_fixture.runs_csv.open()))
    assert len(rows) == 1
    assert rows[0]["hypothesis_id"] == "H-001"
    state = json.loads(climb_fixture.session_state.read_text())
    assert state["last_cycle"] == 1
    assert state["in_flight"] is None
```

- [ ] **Step 2: Run and confirm RED**

Run: `uv run pytest tests/climb -q`

Expected: missing cycle/tree functions.

- [ ] **Step 3: Implement synchronization order**

`cycle.py` must perform this order on every PUSH/SKIP/failure branch:

```python
append_run_csv(run)
append_hypothesis_result(run)
update_session_state(run)
regenerate_research_tree()
check_target()
```

Use atomic temp-file replacement for JSON/YAML generated projections. Never rewrite historical CSV rows or prior hypothesis results.

- [ ] **Step 4: Implement local-only push and stubs with explicit refusal**

`push.sh` writes `verification-artifact.json` containing the run ID, local score, git head, and `external_submission=false`. `apply-lb-score.sh` exits non-zero with `external leaderboard disabled for this adapter`. `consult-ais.sh` exits non-zero with `external AI consultation disabled`; the climb loop must treat this as unavailable and continue with local rules.

- [ ] **Step 5: Add Makefile entry points**

```make
## climb-status: Show the generated autonomous research/development tree.
climb-status:
	@cat docs/status/climb/research-tree.md

## climb-cycle: Run one local climb quality-gate cycle (hypothesis=H-NNN required).
climb-cycle:
	@test -n "$(hypothesis)" || (echo "usage: make climb-cycle hypothesis=H-NNN" >&2; exit 2)
	@tools/climb/cycle.sh "$(hypothesis)"

## climb-check: Verify climb adapter contracts and deterministic state generation.
climb-check:
	@uv run pytest tests/climb -q
```

- [ ] **Step 6: Generate the initial tree and run gates**

Run:

```bash
python tools/climb/regen-tree.py
make climb-check
make climb-status
```

Expected: tests PASS and the tree shows H-001 pending with no in-flight process.

- [ ] **Step 7: Install the post-commit hook compatibly with `.githooks`**

Do not overwrite `.githooks/pre-commit`. Add `.githooks/post-commit` from the repository template and make it executable. It regenerates and amends only when a commit changed climb storage state without the generated tree.

- [ ] **Step 8: Commit**

```bash
git add Makefile .githooks/post-commit tools/climb docs/status/climb/research-tree.md tests/climb/test_adapter.py
git commit -m "feat(climb): add deterministic autonomous cycle"
```

---

## Execution Choice

Execute inline before the position-persistence implementation plan. This adapter is the durable outer-loop state required by the user's `climb` request; it must be green before H-001 begins changing M2 domain code.
