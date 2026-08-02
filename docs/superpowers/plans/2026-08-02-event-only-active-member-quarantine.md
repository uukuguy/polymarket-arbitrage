# Event-only Active Member Quarantine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish a fresh, fully certified Structure generation when complete `/events` contains exact active/open neg-risk members absent from complete `/markets`, without weakening any other active-open cross-stream invariant.

**Architecture:** A shared pure projection classifies event-only members and returns filtered memberships, recomputed group truth, and authenticated issues. SQLite supplies bounded, pinned-window candidate data and independently reconstructs every receipt during certification; normalization and source certification consume the same projection while retaining separate evidence checks.

**Tech Stack:** Python 3.12, SQLite, pytest, Typer, existing resumable Structure publication pipeline.

## Global Constraints

- Production matrix is exact: global active/open neg-risk `48102`, event membership `47983`, both `47921`, global-only `181`, event-only `62`; both-present field mismatch `0`, multi-parent `0`, and generation-absent `0`.
- The existing `181` global-only rows remain exactly the two existing mutually exclusive reasons (`137` parent absent and `44` missing group); the `62` event-only rows use only the new third reason.
- Snapshot 847 uses the old contract and must be automatically superseded. A natural new window creates snapshot 848; pointer 845 and failure counter remain unchanged until 848 certifies and publishes.
- Never synthesize a generation market from an embedded event payload, never allow a dangling active/open membership, and never carry 58 legacy-845 rows into the fresh contract.
- Every read and write stays bounded at 500 source rows with durable keyset cursors.
- Do not deploy or mutate production.

---

### Task 1: Shared exact event projection and receipt

**Files:**
- Modify: `src/polyarb/perception/structure_publication.py`
- Modify: `src/polyarb/perception/structure_contract.py`
- Test: `tests/m1-perception/test_structure_generation_publication.py`

**Interfaces:**
- Produces: `EVENT_ONLY_NEG_RISK_QUARANTINE_REASON: str`.
- Produces: a pure event projection that accepts one raw event, its source ordinal, and an exact set of event-only market IDs and returns filtered `EventMember` rows, recomputed `GroupTruth` rows, and canonical issue receipts.
- Produces: a canonical receipt binding event payload hash, embedded member payload hash, event/member ordinals, event/group/market IDs.

- [ ] **Step 1: Write production-shape RED tests**

Seed one complete window containing: both-present members, `181` existing global-only reason fixtures, `62` event-only members distributed across 14 events/groups, and one partial group whose remaining members must retain exact count/hash. Assert the matrix is `48102/47983/47921/181/62` in the scale fixture or an algebraically equivalent compact fixture plus explicit production constants; assert the old implementation raises `membership-invalid`.

- [ ] **Step 2: Write projection unit RED tests**

Assert only exact active/open neg-risk event members with group identity are removed, empty truths disappear, partial truths recompute `expected_member_count`, `active_named_count`, and `membership_hash`, and inactive/closed/ordinary/missing-group members are not classified.

- [ ] **Step 3: Run RED**

Run: `uv run pytest -q tests/m1-perception/test_structure_generation_publication.py -k 'event_only or cross_stream_matrix'`

Expected: failures show event-only membership remains published and no third receipt exists.

- [ ] **Step 4: Implement the minimal pure projection**

Add the fixed reason, canonical JSON SHA-256 envelope, embedded-member ordinal resolution, filtering, and truth recomputation. Keep event/event-tag rows unchanged. Bump `STRUCTURE_NORMALIZATION_CONTRACT_VERSION` from `2026-08-02-neg-risk-quarantine-v1` to an event-only quarantine v2 value.

- [ ] **Step 5: Run GREEN and commit**

Run the Step 3 command and Ruff on the two changed source files. Commit only Task 1 files with `fix(m1): project event-only quarantine truth`.

### Task 2: Bounded source union and independent certification

**Files:**
- Modify: `src/polyarb/storage/sqlite_store.py`
- Modify: `src/polyarb/perception/structure_publication.py`
- Test: `tests/m1-perception/test_structure_generation_publication.py`

**Interfaces:**
- Produces: bounded lookup of exact event-only candidates for an event chunk.
- Produces: unified issues keyset ordered by market ID over market-side plus event-only candidates.
- Consumes: Task 1 projection and receipt helpers.

- [ ] **Step 1: Write source-authentication RED tests**

Assert normalization emits one issue per exact event-only relation and source certification reconstructs it. Mutate each evidence dimension independently (event payload, embedded payload, event ordinal, embedded ordinal, event/group/market identity, market absence, unique parent) and assert `source-truth-invalid` or `generation-validation-issues`.

- [ ] **Step 2: Write the full mutual-exclusion audit RED matrix**

Cover global-only existing quarantine, event-only new quarantine, both-present exact match, both-present active/closed/group/event field mismatch, and multi-parent. Assert each row enters exactly one terminal class and all non-production-shape mismatches remain fatal.

- [ ] **Step 3: Write bounded/resume RED tests**

Seed 501 event-only candidates. Assert the first issues call reads at most 500 source candidates, persists a market-ID cursor, the second call emits the last row, replay is idempotent, and no source query scans or materializes beyond its requested limit.

- [ ] **Step 4: Run RED**

Run: `uv run pytest -q tests/m1-perception/test_structure_generation_publication.py -k 'event_only or cross_stream or issue_keyset'`

Expected: missing event-only fetch/authentication helpers and membership/source certification failures.

- [ ] **Step 5: Implement bounded SQL and certification**

Add indexed/keyset queries that require complete pinned-window state, exact relation count one, and market anti-join. Make memberships/group-truth normalization request the classification set for only its event chunk. Make issues consume a deterministic union without changing the durable cursor grammar. Extend quarantine evidence reconstruction to the event source and require absent generation membership/market plus exact generated issue. Make `source_events` compare the filtered projection and independently require its receipts.

- [ ] **Step 6: Run GREEN and commit**

Run Task 2 focused tests plus all `test_structure_generation_publication.py`; run Ruff. Commit only Task 2 files with `fix(m1): authenticate event-only source differences`.

### Task 3: Recovery, readers, health, and bounded failure marker

**Files:**
- Modify: `src/polyarb/snapshot/cli.py`
- Modify: `src/polyarb/daemon/scheduler.py`
- Test: `tests/m1-perception/test_snapshot_cli_json.py`
- Test: `tests/m1-perception/test_scheduler.py`
- Test: `tests/m1-perception/test_health_endpoint.py`
- Test: `tests/m1-perception/test_structure_generation_readers.py`
- Test: `tests/m1-perception/test_structure_generation_publication.py`

**Interfaces:**
- Produces: bounded allowlisted `membership_kind` and `key_sha256` stderr suffix for structured membership failures while preserving the two-key stdout JSON protocol.
- Consumes: committed issue count and generation views without reader-specific quarantine exceptions.

- [ ] **Step 1: Write operational RED tests**

Assert a quarantined event-only member is absent from opportunity/market-map views and health is warning, not fatal. In a later window where the market appears globally, assert membership/truth/market return and the issue disappears.

- [ ] **Step 2: Write 847-to-848 RED recovery test**

Seed pointer 845 plus active old-contract 847 and failure counter 261. Assert reconciliation supersedes 847 without moving pointer or incrementing the counter, natural admission reserves 848, no 845 rows are copied into 848, and only successful 848 publication moves the pointer.

- [ ] **Step 3: Write bounded marker RED tests**

Raise a structured membership error and assert stderr contains only fixed `failure_kind`, allowlisted subtype, and 64-hex fingerprint within 256 bytes. Assert raw IDs and arbitrary suffixes are rejected by scheduler safe-tail parsing; stdout remains exactly `{"failed":true,"failure_kind":"membership-invalid"}`.

- [ ] **Step 4: Run RED**

Run: `uv run pytest -q tests/m1-perception/test_snapshot_cli_json.py tests/m1-perception/test_scheduler.py tests/m1-perception/test_health_endpoint.py tests/m1-perception/test_structure_generation_readers.py tests/m1-perception/test_structure_generation_publication.py -k 'event_only or contract_supersession or membership_marker'`

- [ ] **Step 5: Implement minimal operations support**

Introduce a bounded membership validation exception carrying only a fixed subtype and SHA-256 fingerprint. Format and parse the optional suffix without widening JSON. Reuse existing health/read paths; change them only if RED proves the committed issue or filtered generation is not already sufficient.

- [ ] **Step 6: Run GREEN and commit**

Run Task 3 focused suites and Ruff. Commit with `fix(m1): expose bounded membership failure evidence`.

### Task 4: Independent review and release gates

**Files:**
- Modify only files required by verified review findings.
- Record evidence in the existing R222 planning/report artifact selected by the coordinator.

**Interfaces:**
- Consumes: Tasks 1-3 commits.
- Produces: review verdict and exact focused/full verification totals; no deployment.

- [ ] **Step 1: Run focused invariant suites**

Run all Structure publication, scheduler, CLI, health, and reader tests. Confirm the production matrix constants and `847 → 848` assertions appear in collected tests.

- [ ] **Step 2: Run static gates**

Run Ruff on every changed Python file, `git diff --check f0a2cf3..HEAD`, and `make planning-status`.

- [ ] **Step 3: Request independent review**

Review `f0a2cf3..HEAD` against the approved spec, with special attention to boundedness, cross-stream mutual exclusion, evidence recomputation, fail-closed behavior, and stale-generation carry-forward.

- [ ] **Step 4: Resolve findings with RED/GREEN**

For every Important or Critical finding, add a regression test, observe RED, implement the smallest correction, and rerun the focused suite.

- [ ] **Step 5: Run full suite and commit final evidence**

Run `uv run pytest -q`, collect exact totals, rerun Ruff and diff-check, then create the required SUMMARY/report commit. Do not deploy or modify production.
