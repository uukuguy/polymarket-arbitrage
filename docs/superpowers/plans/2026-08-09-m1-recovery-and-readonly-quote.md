# M1 Recovery and Read-only Quote Implementation Plan

> **For agentic workers:** Execute inline in the current M1 worktree; do not create another worktree.

**Goal:** Prevent cleanup from indefinitely blocking certified Structure-to-drift recovery, then activate the public-read-only Quote feed and prove live M2 input.

**Architecture:** Cleanup remains durable and low priority, but moves from an uninterruptible in-process SQLite thread to an isolated bounded child. The parent owns terminal cleanup state and only releases the shared producer lock after the child exits. Quote activation is a persisted Fly configuration change; the existing fail-closed feed contract remains unchanged.

**Tech Stack:** Python 3.12 asyncio/subprocess, SQLite, pytest, Fly.io.

## Global Constraints

- No wallet, signer, private key, order, or execution API.
- Never expose a generation to M2 before its certified drift receipt is sealed.
- A timed-out child must be reaped before producer-lock release.
- Quote reads use only public CLOB endpoints and persist the existing immutable run identity.

---

### Task 1: Isolate bounded cleanup execution

**Files:**
- Modify: `src/polyarb/daemon/generation_cleanup_worker.py`
- Modify: `src/polyarb/config.py`
- Modify: `tests/m1-perception/test_generation_cleanup_worker.py`

**Interfaces:**
- Add `structure_generation_cleanup_hard_limit_s: float` to `Settings`.
- Cleanup worker terminal timeout calls `finish_structure_generation_cleanup_attempt(state="backoff", error_kind="cleanup-timeout", increment_failure=True)`.

- [ ] Write an async failing test with a cleanup callable blocked on a thread event. Assert a deadline records `cleanup-timeout`, retains the producer lock until the child/work unit is reaped, then permits a waiting producer to acquire it.
- [ ] Run `uv run pytest tests/m1-perception/test_generation_cleanup_worker.py -q -k cleanup_timeout` and observe failure.
- [ ] Implement a bounded isolated cleanup runner; on timeout terminate/reap it, record backoff, and preserve the cancellation invariant.
- [ ] Run the focused worker suite and the Structure drift end-to-end tests.

### Task 2: Persist and prove Quote activation

**Files:**
- Modify: `fly.toml`
- Modify: `.planning/JOURNAL.md`
- Test: `tests/m1-perception/test_l1_quote_worker_wiring.py`

**Interfaces:**
- Persist `POLYARB_NEG_RISK_QUOTE_WORKER_ENABLED = "true"`.
- `/arbitrage/opportunities` must remain 503 until a complete current certified quote run exists, then return 200 with a run identity.

- [ ] Run quote worker wiring and opportunity HTTP tests before configuration activation.
- [ ] Set the persisted env flag, deploy with `FLY_BUILD_MODE=--local-only make deploy`, and confirm runtime env has Quote true.
- [ ] Verify repeated quote runs, fresh certified drift, HTTP 200 opportunities, candidate persistence, and Polywatch recovery evidence.

### Task 3: Production closure evidence

**Files:**
- Modify: `.planning/JOURNAL.md`
- Create: `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/05.6-08-SUMMARY.md`

- [ ] Run the focused and full health/quote suites plus changed-file Ruff.
- [ ] Capture Fly release, machine health, strict health, opportunity response, Quote config, and alert/recovery evidence.
- [ ] Commit code and documentation separately, then update planning status.
