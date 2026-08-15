# M1 Transactional Soak Progress Evidence Implementation Plan

**Goal:** Make the 24-hour transactional M1 soak prove durable collection progress as well as health and failure containment.

**Architecture:** Extend the read-only JSONL record with the control API's cumulative succeeded-job count and version it as v2. The pure local verifier accepts v1 historical evidence unchanged and adds v2 monotonic/strict-forward-progress gates.

**Tech Stack:** Python 3.12, existing `soak_evidence` JSONL/HMAC chain, pytest, macOS launchd.

## Constraints

- Samples remain read-only: no DSN, queue claim, receipt, pointer, or Fly Machine mutation.
- v1 JSONL remains immutable and uses its original contract.
- v2 requires a non-negative integer success count that never decreases and strictly increases over the completed window.
- Keep five-machine, API, gap, expired-lease, and circuit fail-closed gates unchanged.

### Task 1: Define the v2 evidence contract

**Files:**

- Modify `tests/m1-perception/test_control_plane_soak_evidence.py`
- Modify `src/polyarb/control_plane/soak_evidence.py`

1. Write failing tests for a v2 stream with `successful_job_count` 100 then 101, and for stalled (100 then 100) and decreasing (100 then 99) streams.
2. Run `uv run pytest tests/m1-perception/test_control_plane_soak_evidence.py -q`; it must fail because forward progress is not represented or checked.
3. Capture `job_counts.succeeded` as `successful_job_count`, validate it is a non-negative integer, version the record as `m1-transactional-soak-v2`, and in the verifier reject absent, decreasing, or non-increasing complete-window counts. Retain the v1 branch unchanged.
4. Re-run the focused suite and require PASS.
5. Commit source and test as `feat(05.6): require transactional soak progress`.

### Task 2: Start a v2 automatic window

**Files:**

- Modify `tests/m1-perception/test_control_plane_soak_cli.py`
- Modify `/Users/sujiangwen/Library/LaunchAgents/com.polyarb.m1-transactional-soak-sampler.plist`
- Create `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/evidence/staging-transactional-soak-v2.jsonl`

1. Write a failing CLI test that a missing `job_counts.succeeded` gives the existing safe operator error and no evidence file.
2. Run `uv run pytest tests/m1-perception/test_control_plane_soak_cli.py -q`; it must fail for the new contract.
3. Point the named sampler at the v2 filename, reload it, run exactly one `control-plane-soak-start` baseline, and let the scheduled LaunchAgent become the sole later writer.
4. Run evidence/CLI tests and Ruff together. Require all green.
5. After a non-manual 600-second sample, prove that the count increased, update the phase summary/JOURNAL, and commit evidence/docs with the scoped SUMMARY.

## Validation Matrix

| Requirement | Evidence |
| --- | --- |
| v1 compatibility | existing v1 tests stay green |
| missing/malformed success count | focused v2 unit and CLI RED/GREEN tests |
| no false progress | stalled/decreasing v2 test cases |
| actual production-like progress | automatic v2 baseline and later sample with increasing cumulative count |
| continuity safety | original gap/API/machine/circuit/lease verifier tests |
