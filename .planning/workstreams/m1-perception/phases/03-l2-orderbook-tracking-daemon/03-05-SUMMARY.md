---
phase: 03
plan: 05
status: in-progress
subsystem: event-bus-and-candidate-refresh
tags: [asyncpg, postgres-notify, listen, candidate-refresh, scanner-reuse, watchlist]
wave: 4
requires: [D-04, D-05]
provides:
  - polyarb.events.bus.publish_snapshot_complete (asyncpg fire-and-forget NOTIFY, fail-soft)
  - polyarb.events.listener.listen_snapshot_complete + catchup_from_cursor (LISTEN consumer + drop mitigation)
  - polyarb.observation.l2_candidate_refresh.compute_candidates + diff_candidate_sets + on_snapshot_complete
  - L1 orchestrator step 7.7 — fan-out NOTIFY (feature-flag POLYARB_EVENT_BUS_ENABLED default FALSE, fail-soft)
  - asyncpg dependency >=0.31,<0.32
affects:
  - L1 orchestrator (NEW step 7.7 — gated by event_bus_enabled, doesn't break Phase 02 if disabled)
  - L2 daemon l2_main.py wiring (event_listener None → real EventListenerWrapper + listener task)
  - candidate set size cap enforced (500 assets per R9)
tech-stack-added: [asyncpg-0.31, postgres-listen-notify, scanner-reuse-from-phase-01.1]
key-files-created: []
key-files-modified: []
decisions: []
metrics:
  duration_minutes: TBD
  completed_date: 2026-05-24
  task_commits: TBD
---

# Phase 03 Plan 05: Event Bus + Candidate Refresh — Summary (DRAFT)

> Filled in during execution.

## Deliverables

TBD

## Commits

TBD

## Self-Check

TBD
