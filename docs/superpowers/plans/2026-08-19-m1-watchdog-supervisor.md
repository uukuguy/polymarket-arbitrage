# M1 Watchdog External Supervisor Implementation Plan

> **For agentic workers:** execute inline task-by-task; no subagents. Each task uses test-first development and an atomic commit.

**Goal:** Add an external Cloudflare Cron supervisor that detects failure of the Fly runtime watchdog and creates matching Telegram and Dashboard evidence.

**Architecture:** A dependency-free Worker reads one exact Fly Machine and its restart events once per UTC minute. KV holds only the last transition state; the Worker sends direct Telegram and a writer request only on a state transition. The existing writer gains a bounded optional source field so the Dashboard can distinguish the external supervisor from the in-Fly watchdog.

**Tech Stack:** Cloudflare Workers Cron + KV, JavaScript Web Crypto/fetch, Wrangler 4, existing Python Starlette writer and pytest.

## Global Constraints

- Never give the Worker a database, R2, market-data, scheduler, or wallet credential.
- Use `* * * * *` in UTC; tolerate at-least-once execution with deterministic keys.
- Keep public fetch disabled; only scheduled work may run in production.
- Do not deploy until unit tests and local scheduled simulation pass.

---

### Task 1: Source-aware bounded writer events

**Files:**
- Modify: `src/polyarb/control_plane/runtime_event_writer.py`
- Modify: `tests/m1-perception/test_runtime_event_writer.py`

- [ ] Write failing tests that POST a valid `source="cloudflare-watchdog-supervisor"`, assert the SQL detail contains it, and reject a source containing whitespace.
- [ ] Run `uv run pytest tests/m1-perception/test_runtime_event_writer.py -q`; expect failure because source is not parsed or stored.
- [ ] Parse `payload.get("source", "independent-runtime-watchdog")`, validate it with the existing bounded failure-code expression, and persist it in the JSON detail.
- [ ] Run the same pytest command and `uv run ruff check src/polyarb/control_plane/runtime_event_writer.py tests/m1-perception/test_runtime_event_writer.py`; expect PASS.
- [ ] Commit only writer and writer-test changes.

### Task 2: Isolated scheduled supervisor

**Files:**
- Create: `monitoring/watchdog-supervisor/wrangler.jsonc`
- Create: `monitoring/watchdog-supervisor/src/index.js`
- Create: `monitoring/watchdog-supervisor/test/index.test.mjs`
- Create: `monitoring/watchdog-supervisor/package.json`

- [ ] Write Node tests for `observeAlertMachine`, `transitionKey`, and `runScheduled`: unchanged started state is quiet; stopped and restart-increased states create one detected event; recovery creates one recovered event; repeated scheduled time creates the identical 64-character key.
- [ ] Run `node --test monitoring/watchdog-supervisor/test/index.test.mjs`; expect missing implementation failure.
- [ ] Implement dependency-free functions using `fetch`, `crypto.subtle.digest`, and a KV binding named `WATCHDOG_STATE`. Require the exact Fly Machine ID returned by the API; a fetch/JSON/KV error must normalize to a bounded failure code. POST the existing writer envelope with `source="cloudflare-watchdog-supervisor"`; direct Telegram is sent on the same transition.
- [ ] Configure `compatibility_date: "2026-08-19"`, `triggers.crons: ["* * * * *"]`, `workers_dev: false`, and only non-secret identity/URL vars. Define secret names only in README comments, never in config.
- [ ] Run Node tests and `npx --yes wrangler deploy --dry-run --config monitoring/watchdog-supervisor/wrangler.jsonc`; expect PASS.
- [ ] Commit only the isolated Worker package and tests.

### Task 3: Provision, fault-proof, and restart final acceptance

**Files:**
- Modify: `Makefile`
- Modify: `.planning/JOURNAL.md`
- Modify: `.planning/workstreams/m1-perception/STATE.md`

- [ ] Add `make control-plane-watchdog-supervisor-deploy` and `make control-plane-watchdog-supervisor-verify` as the only operator entry points. The verify target must read the supervisor's public version/health response and never print secrets.
- [ ] Create the dedicated KV namespace and deploy through Wrangler. Set Fly read token, Telegram credentials, writer token, and chat ID with `wrangler secret put`; do not place values in repository files.
- [ ] Verify one real scheduled healthy execution through Workers logs/KV, then issue one bounded alert-Machine stop/start fault. Require Cloudflare-source Telegram and Dashboard `detected`/`recovered` records before restoring normal collection.
- [ ] Start a new uniquely named cloud-soak baseline only after the external-supervisor recovery is visible. Record machine IDs, Worker version, and verification output in JOURNAL/STATE.
- [ ] Run `make planning-status`, targeted Python/Worker tests, and commit documentation/Makefile updates.
