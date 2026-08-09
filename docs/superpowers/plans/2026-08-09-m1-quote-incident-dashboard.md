# M1 Quote Incident Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Persist Quote timeout incidents and render diagnosis, automatic recovery, and recovery proof in Dashboard.

**Architecture:** A Quote-owned adapter writes the existing SQLite incident ledger; the bounded perception API projects validated diagnosis fields; `/perception` renders the operational disposition.

**Tech Stack:** Python 3.12, SQLite, Starlette, Next.js/TypeScript, pytest, pnpm.

## Global Constraints

- Public CLOB reads only; no wallet, signer, order, secret, or infrastructure mutation.
- Retry is `recovering`; only a certified Quote publication may set `verified`.
- Diagnosis is typed, bounded, credential-free, and absent data renders `not recorded`.
- Never convert an unavailable Quote feed into zero opportunities.

### Task 1: Persist Quote timeout lifecycle

**Files:** Create `src/polyarb/daemon/quote_incidents.py`; modify `src/polyarb/daemon/quote_worker.py`, `src/polyarb/daemon/main.py`; test `tests/m1-perception/test_quote_incidents.py`.

**Interfaces:** `QuoteIncidentLifecycle.record_timeout(input) -> Incident`; `record_certified_success(result) -> Incident | None`; optional async worker callbacks.

- [ ] Write failing tests: `record_timeout(run_id=1908, requested_token_count=38972, deadline_s=120, consecutive_failures=1, last_success_age_s=3057.8)` returns scope `quote-collection`, kind `quote-collection-timeout`, state `recovering`; a second timeout has the same ID; certified success on run 1911 returns same ID in `verified`.
- [ ] Run `uv run pytest tests/m1-perception/test_quote_incidents.py -q`; expect RED because the adapter is absent.
- [ ] Implement `QuoteIncidentInput` with nullable run/token count, deadline, consecutive failures, and last success age. Implement `QuoteIncidentLifecycle` through `asyncio.to_thread(IncidentManager...)`; write `detected → classified → contained → recovering`, legally update repeated timeouts, and verify only after `publish_certified_*` plus `runtime.mark_success`.
- [ ] Run `uv run pytest tests/m1-perception/test_quote_incidents.py tests/m1-perception/test_l1_quote_worker_wiring.py -q`; expect PASS.
- [ ] Commit `feat(m1): persist quote collection incidents` with Task 1 files.

### Task 2: Project typed diagnosis through the API

**Files:** Modify `src/polyarb/http/perception.py`, `dashboard/lib/types.ts`, `dashboard/lib/perception.ts`; test `tests/m1-perception/test_perception_incidents_http.py`, `dashboard/lib/perception.test.ts`.

**Interfaces:** Optional `diagnosis` object: `impact`, `automatic_action`, `next_action`, `deadline_s`, `consecutive_failures`, `last_success_age_s`.

- [ ] Write failing API test asserting a repeated Quote timeout returns `impact=feed-unavailable`, `automatic_action=retry-immediately`, `next_action=inspect-clob-and-child-io`, and `deadline_s=120`; write TypeScript validator test rejecting `deadline_s: "120"`.
- [ ] Run `uv run pytest tests/m1-perception/test_perception_incidents_http.py -q && cd dashboard && pnpm test -- perception.test.ts`; expect RED because diagnosis is not projected.
- [ ] Implement `_quote_incident_diagnosis(evidence)`: return `None` unless exact scalar type/bounds hold; derive impact from freshness/failure evidence and never opportunity count. Add the matching optional TypeScript type and strict validator.
- [ ] Run API/parser tests and `pnpm run typecheck`; expect PASS.
- [ ] Commit `feat(m1): expose quote incident diagnosis`.

### Task 3: Dashboard operational disposition

**Files:** Modify `dashboard/app/perception/page.tsx`, `docs/M1-市场感知平台使用手册.md`; test `dashboard/app/perception/page.test.tsx`, `tests/m1-perception/test_m1_manual_contract.py`.

- [ ] Write a failing render test for an active Quote incident asserting visible `Impact: feed unavailable`, `Automatic action: retry immediately`, and `Next action: inspect CLOB and child I/O`, while the zero-opportunity success copy is absent.
- [ ] Run `cd dashboard && pnpm test -- page.test.tsx`; expect RED because the card renders generic evidence only.
- [ ] Add a Quote-only block under lifecycle data with those labels, deadline/retry/freshness evidence, and a history link `/perception/incidents/<id>/history`; render `not recorded` for old incidents. Add Dashboard/API/Telegram triage order to the manual.
- [ ] Run `pnpm test -- page.test.tsx && pnpm run typecheck && pnpm run build`, then `make docs-m1-check`; expect PASS.
- [ ] Commit `feat(m1): show quote incident disposition`.

### Task 4: End-to-end and production acceptance

**Files:** Create `tests/m1-perception/test_quote_incident_e2e.py`; modify manual and `.planning/JOURNAL.md`.

- [ ] Write a failing temporary-SQLite contract that a timeout's API `incident_id` equals its history ID and certified success is the final `verified` history state.
- [ ] Run `uv run pytest tests/m1-perception/test_quote_incident_e2e.py -q`; expect RED until Tasks 1-2 are wired.
- [ ] Build the fixture without Fly/network. Run focused Python tests, Dashboard tests/typecheck, `make docs-m1-check`, and `make planning-status`; expect PASS.
- [ ] Commit `test(m1): verify quote incident observability`, then `make deploy`.
- [ ] Accept production only after the new release is identified, one active Quote incident has matching API/Dashboard/Telegram evidence, and the same ID later receives certified recovery evidence.

## Plan self-review

- Task 1 covers durable lifecycle and recovery proof; Task 2 validation/redaction; Task 3 operator display; Task 4 cross-channel evidence.
- No task weakens Quote freshness or calls a retry recovery.
