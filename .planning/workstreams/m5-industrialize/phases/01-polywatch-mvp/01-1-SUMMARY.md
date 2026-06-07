# Phase 01 Plan 1 — SUMMARY

> **Plan**: 01-1-PLAN.md (426 lines, 6 tasks)
> **Status**: 🟡 PLANNED — execution not started
> **Date**: 2026-06-07

## Plan Summary

4 trials + skill extraction for Polywatch automation framework:

| Task | What | Dependencies |
|---|---|---|
| T0 | Pre-flight gate — Phase 03.1 readiness check | None |
| T1 | Trial 1 — healthz-watcher enhancement (jsonl ledger + Sentry breadcrumb + notes field) | None |
| T2 | Trial 2 — chaos-inj-replay script + Fly cron | T0 |
| T3 | Trial 3 — memory-sanity-check shell script + ralph-loop template | T0 |
| T4 | Trial 4 — autoresearch grid search over 10 tolerance values | T0 |
| T5 | Global skill extraction (~/.claude/skills/polywatch/) + escalation wiring + phase close | T1-T4 |

## Decisions (locked from CONTEXT)

- **D-Polywatch-1**: trials state → `.planning/polywatch/trials/{name}.jsonl` (append-only)
- **D-Polywatch-2**: cron — healthz: GHA, chaos: Fly machine (UTC 18:00 nightly), ralph/autorsearch: manual
- **D-Polywatch-3**: global skill sync in this phase (user override of Claude recommendation)
- **D-Polywatch-4**: escalation — L0 silent → L1 Sentry breadcrumb (streak=3) → L2 Telegram → L3 auto-GH-issue

## Not yet executed

Plan is ready for `/gsd-execute-phase 01 --ws m5-industrialize`. All 4 trials are independent and can execute in parallel (single wave).
