# M1 Transactional Soak Evidence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce fail-closed, append-only staging evidence that proves a 24-hour transactional M1 control-plane soak.

**Architecture:** Keep canonical record creation and pure verification in a small control-plane module. The CLI does all external reads and writes JSONL only after a complete healthy sample; Makefile supplies explicit operator commands. Existing fault-soak verification consumes the resulting facts but is not weakened.

**Tech Stack:** Python 3.12, stdlib JSON/hashlib/pathlib/subprocess, httpx, pytest, Ruff, Make.

## Global Constraints

- Never mutate Postgres, SQLite, R2, pointers, alert outbox, or worker Machines.
- Output creation is exclusive; every later record is append-only canonical JSONL.
- Baseline historic circuit/lease counts are allowed; a later higher count fails verification.
- Exact staging URL and exact deduplicated Machine IDs are locked by the first record.
- All tests start RED and end GREEN; every new operator command has a Makefile target.

---

### Task 1: Typed canonical evidence and pure verifier

**Files:**
- Create: `src/polyarb/control_plane/soak_evidence.py`
- Create: `tests/m1-perception/test_control_plane_soak_evidence.py`

**Interfaces:**
- Produces `SoakEvidenceError(ValueError)`, `create_record(...) -> dict[str, object]`, `append_record(path, record, exclusive=False)`, `read_records(path)`, and `verify_soak(records, minimum_seconds=86_400, max_gap_seconds=900) -> dict[str, int | str]`.
- `create_record` canonicalizes fields and adds `snapshot_sha256`; `verify_soak` validates that hash before using a record.

- [ ] **Step 1: Write failing canonical and 24-hour tests**

```python
def test_soak_record_round_trip_and_verifies_24_hours(tmp_path: Path) -> None:
    from polyarb.control_plane.soak_evidence import append_record, create_record, read_records, verify_soak
    path = tmp_path / "soak.jsonl"
    first = create_record(observed_at="2030-01-01T00:00:00+00:00", control_api_url="https://control", machine_states={"a": "started"}, control_snapshot={"status": "available", "expired_leases": 6, "open_circuit_count": 74, "queue_health": {}})
    last = create_record(observed_at="2030-01-02T00:00:00+00:00", control_api_url="https://control", machine_states={"a": "started"}, control_snapshot={"status": "available", "expired_leases": 6, "open_circuit_count": 74, "queue_health": {}})
    append_record(path, first, exclusive=True); append_record(path, last)
    assert verify_soak(read_records(path))["status"] == "PASS"
```

Add parametrized negative cases for altered digest, non-started state, changed machine key, gap over 900 seconds, higher circuit count, and duration 86,399.

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/m1-perception/test_control_plane_soak_evidence.py -q`

Expected: FAIL because module is missing.

- [ ] **Step 3: Implement canonical module**

```python
def _canonical_bytes(record: Mapping[str, object]) -> bytes:
    return json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()

def create_record(...):
    payload = {...}
    return {**payload, "snapshot_sha256": sha256(_canonical_bytes(payload)).hexdigest()}
```

Use `Path.open("x")` for the first record and `Path.open("a")` for later records. Parse timestamps as timezone-aware UTC; reject malformed records before returning them.

- [ ] **Step 4: Verify GREEN and commit**

Run: `uv run pytest tests/m1-perception/test_control_plane_soak_evidence.py -q && uv run ruff check src/polyarb/control_plane/soak_evidence.py tests/m1-perception/test_control_plane_soak_evidence.py`

Commit: `feat(m1): add transactional soak evidence verifier`

### Task 2: Read-only CLI collection commands

**Files:**
- Modify: `src/polyarb/cli_control_plane.py`
- Modify: `tests/m1-perception/test_control_plane_cli.py`

**Interfaces:**
- Adds `soak-start`, `soak-sample`, `soak-verify` subcommands.
- Commands accept `--output`, `--control-api-url`, repeated `--machine-id`, and sample/start invoke injectable `_read_control_snapshot` and `_read_machine_state` helpers.

- [ ] **Step 1: Write failing CLI tests**

```python
def test_soak_start_and_sample_only_append_healthy_read_evidence(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(cli_control_plane, "_read_control_snapshot", lambda _: {"status": "available", "expired_leases": 0, "open_circuit_count": 0, "queue_health": {}})
    monkeypatch.setattr(cli_control_plane, "_read_machine_state", lambda _: "started")
    args = ["soak-start", "--output", str(tmp_path / "evidence.jsonl"), "--control-api-url", "https://control", "--machine-id", "a"]
    assert cli_control_plane.main(args) == 0
    assert cli_control_plane.main(["soak-sample", *args[1:]]) == 0
```

Test an unavailable API and a stopped machine return nonzero without appending.

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/m1-perception/test_control_plane_cli.py -k soak -q`

Expected: FAIL because parsers/helpers do not exist.

- [ ] **Step 3: Implement read-only CLI boundary**

`_read_control_snapshot` must use `httpx.get(url, timeout=5.5)` and require JSON `status == "available"`. `_read_machine_state` must invoke `flyctl machine status <id> --app <app> --json` without shell interpolation and return only `state`.

- [ ] **Step 4: Verify GREEN and commit**

Run: `uv run pytest tests/m1-perception/test_control_plane_cli.py -k soak -q && uv run ruff check src/polyarb/cli_control_plane.py tests/m1-perception/test_control_plane_cli.py`

Commit: `feat(m1): expose read-only soak evidence commands`

### Task 3: Makefile, teaching note, and staging start

**Files:**
- Modify: `Makefile`
- Create: `docs/learning/76-事务型采集连续证据.md`
- Modify: `docs/learning/00-INDEX.md`
- Modify: `docs/M1-市场感知平台使用手册.md`

**Interfaces:**
- Adds `make control-plane-soak-start`, `make control-plane-soak-sample`, and `make control-plane-soak-verify`.
- Every target requires explicit `output=`, `control_api_url=`, and comma-separated `machine_ids=` where relevant.

- [ ] **Step 1: Write failing repository command/documentation contract**

Add assertions to the existing Make/manual contract test that target names appear in both Makefile help comments and the M1 manual.

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/m1-perception/test_m1_manual_contract.py -q`

Expected: FAIL because three targets and teaching references are absent.

- [ ] **Step 3: Add targets and documentation**

Targets call `uv run python -m polyarb.cli_control_plane soak-*` and do not source secrets. The learning note explains baseline vs historical failure, append-only evidence, and why missing samples fail closed.

- [ ] **Step 4: Verify GREEN, start staging evidence, and commit**

Run: `uv run pytest tests/m1-perception/test_control_plane_soak_evidence.py tests/m1-perception/test_control_plane_cli.py tests/m1-perception/test_m1_manual_contract.py -q && make help | rg 'control-plane-soak'`

Start the real staging window with exact five live machine IDs and save under `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/evidence/`.

Commit: `feat(m1): make transactional soak evidence operable`
