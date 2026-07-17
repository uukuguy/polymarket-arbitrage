---
phase: 8
slug: venue-truth-fill-reconciliation
status: approved
nyquist_compliant: true
wave_0_complete: true
created: 2026-07-17
---

# Phase 8 — Validation Strategy

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest + pytest-asyncio |
| **Config file** | `pyproject.toml` |
| **Quick run command** | `uv run pytest tests/routing/test_position_repository.py tests/routing/test_position_tracker.py -q` |
| **Full suite command** | `uv run pytest tests/models/test_slippage.py tests/routing tests/execution tests/cli -q` |
| **Estimated runtime** | ~10 seconds focused / ~15 seconds full |

## Sampling Rate

- After every RED commit: run the exact target test and observe expected failure.
- After every GREEN commit: run the affected module and its restart/process peer.
- Before plan closure: full M2, Makefile, climb adapter, planning status, Ruff, and diff gates.
- Max feedback latency: 30 seconds.

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 08-01-01 | 01 | 1 | H-006 | T-08-01/02/03 | exact codec + atomic fingerprint conflict | unit/restart | `uv run pytest tests/routing/test_position_repository.py -q` | ✅ | ⬜ pending |
| 08-01-02 | 01 | 1 | H-006 | T-08-01/02/04 | terminal complete venue truth overrides model | unit | `uv run pytest tests/routing/test_position_tracker.py -q` | ✅ | ⬜ pending |
| 08-01-03 | 01 | 1 | H-006 | T-08-02/03/05 | Engine/CLI subprocess replay and conflict | integration/CLI | `uv run pytest tests/execution/test_engine.py tests/cli/test_arbitrage_cli_process.py tests/test_makefile.py -q` | ✅ | ⬜ pending |
| 08-01-04 | 01 | 1 | H-006 | all | full regression, docs, and climb closure | full | `uv run pytest tests/models/test_slippage.py tests/routing tests/execution tests/cli -q` plus targeted Ruff | ✅ | ⬜ pending |

## Wave 0 Requirements

Existing pytest, asyncio, SQLite fixture, subprocess CLI, and Makefile dry-run infrastructure
covers all Phase 8 requirements.

## Manual-Only Verifications

All Phase 8 behavior is locally automated; live venue access is explicitly out of scope.

## Validation Sign-Off

- [x] All tasks have automated focused verification.
- [x] Sampling continuity has no unverified implementation task.
- [x] Existing infrastructure covers all references.
- [x] No watch-mode flags.
- [x] Feedback latency < 30 seconds.
- [x] `nyquist_compliant: true`.

Targeted Ruff closure command:

```bash
uv run ruff check src/polyarb/routing/position_repository.py src/polyarb/routing/position_tracker.py src/polyarb/execution/engine.py src/polyarb/cli_arbitrage.py tests/routing/test_position_repository.py tests/routing/test_position_tracker.py tests/execution/test_engine.py tests/cli/test_arbitrage_cli.py tests/cli/test_arbitrage_cli_process.py tests/test_makefile.py
```

**Approval:** approved 2026-07-17
