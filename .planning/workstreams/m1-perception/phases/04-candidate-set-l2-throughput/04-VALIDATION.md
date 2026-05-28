---
phase: 04
slug: candidate-set-l2-throughput
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-05-28
---

# Phase 04 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Body populated by planner from 04-RESEARCH.md § Validation Architecture.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x (existing — `tests/m1-perception/`) |
| **Config file** | pyproject.toml (asyncio_mode; requires `uv sync --extra dev` for pytest-asyncio + psutil per 03.1 D-DEFER-1/2) |
| **Quick run command** | `uv run pytest tests/m1-perception/<changed>.py -x` |
| **Full suite command** | `uv run pytest tests/m1-perception/ -q` |
| **Estimated runtime** | ~TBD (planner fills) |

---

## Sampling Rate

- **After every task commit:** Run quick command on changed test file
- **After every plan wave:** Run full m1-perception suite
- **Before verify:** Full suite must be green
- **Max feedback latency:** TBD (planner fills)

---

## Per-Task Verification Map

*Planner fills from RESEARCH § Validation Architecture. Anchors per decision:*
- D-01/D-02 (Supabase fetch + temp-DB adapter) → unit tests (pagination loop, adapter NULL-fill, fail-loud on missing recipe columns, named-temp-file scanner round-trip)
- D-03 (recipe + cap) → unit test (candidate count ≤ 500, near-end filter)
- D-05/D-06 (throughput) → chaos test + baseline measurement (prod Inj L2-4 at real scale; dropped-frame / watchdog-false-trip / memory)
- D-07 (yes_token_id) → schema test (Alembic add-only, narrow projection includes yes_token_id, NULL passthrough)
- D-08 (GAP-200) → /health test (url-set+key-empty → mirror sub-check status=fail "disabled by config")

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| TBD | — | — | — | — | — | — | — | — | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

*Planner fills. Likely:*
- [ ] `uv sync --extra dev` — ensure pytest-asyncio + psutil present (03.1 D-DEFER-1/2 gap)
- [ ] Test stubs for adapter fail-loud + GAP-200 health logic

*If none: "Existing infrastructure covers all phase requirements."*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Throughput at real candidate scale (D-05/D-06) | — | Requires prod polyarb-l2 + real WS frames + chaos injection; cannot run in CI | Planner fills from RESEARCH § Validation Architecture — baseline-then-threshold in prod |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < TBDs
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
