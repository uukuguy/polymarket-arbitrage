# M1 Structure Adaptive Operations Plan

**Goal:** Make the production Structure collector observable and self-tuning: operators can inspect durable attempt history and timing statistics in the Dashboard, while the scheduler adjusts its timeout/cadence within bounded limits to avoid repeated timeout failures.

**Non-goal:** Trading, order placement, wallet access, or treating a stale Structure revision as current market truth.

## Baseline facts — 2026-07-28

- Production release `109ce48…` persists terminal `snapshot_attempts` with `elapsed_ms` and `last_stage`.
- The latest recovered attempt failed at `gamma-events`: 240s child deadline, 332,241ms terminal parent elapsed.
- The durable history already contains 11 successful terminal attempts. Legacy
  rows lack `elapsed_ms`, but their `finished_at_ms - started_at_ms` duration
  is valid bootstrap evidence; the observed upper tail is approximately 236s.
- `make snapshot-attempt-status` reads the local worktree DB and is therefore not a production operator interface.
- Structure is currently configured at 300s and Quote at 120s; a controller must preserve non-overlap rather than assume those values remain valid.

## Task 1 — Production attempt history and statistics read model

Create read-only L1 APIs backed only by `snapshot_attempts`:

- `GET /structure/attempts?limit=50` — newest terminal/running rows with timestamps, outcome, failure kind, last stage, elapsed, snapshot id.
- `GET /structure/stats?window=30` — success/failure counts, success rate, p50/p90/p95/max successful elapsed, timeout-stage histogram, and current effective timeout/cadence.

Replace the production Make status path with the cloud API; retain a separately named local SQLite inspector for development. Test old rows with nullable diagnostics, empty history, running attempts, bounded limit/window, and no raw stderr exposure.

**Acceptance:** the failed `gamma-events` attempt is visible from a read-only production command and the same data is what statistics consume.

## Task 2 — Dashboard Structure operations panel

Add a Dashboard `/structure` page using the read-only APIs, with no execution controls:

- current attempt / latest terminal result;
- last published revision and coverage anchor;
- 24h and rolling-window timing cards (sample count, success rate, p50/p90/p95/max);
- failure-stage histogram;
- recent attempt table with timestamps, duration, outcome and revision;
- explicit unavailable state rather than fabricated zeroes.

**Acceptance:** TypeScript contract tests and production build pass; the panel renders the same terminal row as the API and never exposes secrets or raw logs.

## Task 3 — Bounded adaptive timeout/cadence controller

Persist a controller state beside attempt history. After each terminal attempt it derives a new effective configuration from a rolling window of successful durations plus timeout failures:

- bootstrap: retain configured 240s timeout / 300s cadence until 10 successful samples;
- target timeout: `p95(success_elapsed) + 30s`, clamped to `[180s, 600s]`;
- target cadence: `max(timeout + 60s, p95 + 90s)`, clamped to `[300s, 900s]`;
- a timeout immediately raises the next timeout by 20% (within max) and cadence to at least timeout + 60s;
- changes apply only after a 3-attempt cooldown and only when the target differs by at least 15s;
- each adjustment is append-only/auditable with the window statistics and reason.

The scheduler reads effective values before launching a child and before its next wait; it never launches concurrent children, lowers market-truth freshness gates, or auto-unpauses after the existing five-failure pause.

**Acceptance:** deterministic tests prove bootstrap, increase, cooldown, clamp, timeout fallback, restart recovery, and no-overlap. Health/API/Dashboard all show configured versus effective values and latest adjustment reason.

## Task 4 — Production rollout and evidence loop

1. Run focused tests, full affected M1 suite, lint, Dashboard typecheck/build, `make planning-status`.
2. Deploy the exact reviewed SHA; verify release identity and migration.
3. Use the root `.env` through the existing signed Make target for one operator unpause if scheduler is paused.
4. Observe at least 10 terminal attempts and publish a first rolling statistics record; do not label the controller validated until the 10-success bootstrap threshold is met.
5. Observe 30 terminal attempts before changing controller bounds; document any recurring last-stage failure and resulting adjustment.

**Acceptance:** API, Dashboard, and durable rows agree on statistics; no unbounded timeout/cadence changes; a failed attempt remains a failed health signal even when the controller adapts subsequent runs.

## Execution order

`Task 1 → Task 2 → Task 3 → Task 4`. Task 1 can ship independently as immediate production visibility; Tasks 2–3 must each receive TDD, independent review, a plan SUMMARY, and a learning-note update before the next task.

## 2026-07-28 priority override — execute Structure controller before Dashboard

The requested production order is now: **Task 3 controller implementation →
Task 4 production evidence loop → Task 1 history API → Task 2 Dashboard**.
Do not begin Dashboard work while the scheduler repeatedly times out. The
controller bootstraps from legacy terminal durations when `elapsed_ms` is NULL;
the existing 11 successes satisfy its 10-success bootstrap threshold.
