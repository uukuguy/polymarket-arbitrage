---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: market-perception
current_phase: 05.6
status: objective_selection
stopped_at: Event Research Workbench complete, deployed, and reviewed for the R&D-stage contract
last_updated: "2026-09-06T04:23:00+08:00"
progress:
  total_phases: 14
  completed_phases: 13
  total_plans: 74
  completed_plans: 74
  percent: 99
---

# M1 Perception — Current State

## Current Position

- **Phase:** 05.6 — Self-healing Structure Production. Plan 269's Event Research
  Workbench is complete on `main` (`1e07b51d`): truthful candidate economics,
  current-lineage Event/group-leg authorities, strict source-aware UI, teaching,
  Fly deployment, and production-API browser validation are all complete.
- **Production fact, last verified 2026-08-31:** exact release
  `3a70cd9f5a52294fba5709f0d390421600baa5de` recovered a blocked Quote certifier
  from durable receipts without operator SQL. A later Structure → Quote →
  Opportunity successor lineage returned the existing qualification epoch to
  `eligible`; the checked control plane had zero expired leases and zero open
  circuits.
- **Current decision:** return to bounded M1 objective selection. Keep Event
  research reads bounded to existing projections and preserve current Quote →
  parent Structure lineage. A fixed REPEATABLE READ snapshot remains an explicit
  R&D follow-up; do not restart the intentionally stopped coordinator.
- **Daily-intelligence boundary:** `6640b330` and `05ff19a9` close Make query
  expansion risk for the read-only opportunity projection. The target captures
  only its own raw `CONTROL_PLANE_OPPORTUNITIES_*` values; the operator cadence
  is `08:30` daily plus `09:00–23:00` every `15` minutes, with runtime/recovery
  evidence read from `.runtime_incidents`, `.recovery_actions`, and
  `.runtime_watchdog`.
- **Business research delivery:** `BusinessOverviewV1` is a one-transaction,
  read-only authority published at `/perception/business-overview`. The daily
  brief and deployed `/business` Dashboard read it rather than composing
  independent status/opportunity reads. `/business` separates Structure, Quote,
  Analysis and final Opportunity research; only `available + count=0` means a
  real zero. The deployed UI is Vercel Access protected; anonymous curl proves
  route/access only, not an authenticated render.

## Production Invariants

- Qualification is rolling and product-local: preserve a valid epoch through a
  contained recovery; do not restart a whole qualification window unless a
  truth-breaking fact requires it.
- A Quote generation's durable fan-in is keyed by its own generation, not its
  parent Structure digest. Structure and Quote have separate clocks.
- A genuinely stale Quote is isolated; subsequent cadence work continues. Do
  not widen the 900-second freshness SLA or reset qualification history to make
  a stale product pass.
- Use the existing `orbstack` Docker context only. Do not configure Colima or
  mutate the global Docker context.
- Keep production mutations out of objective selection. Any deploy, migration,
  secret, recovery, or qualification change needs its own explicit evidence and
  authorization boundary.

## Durable Evidence

- [Plan 05.6-267 summary](phases/05.6-self-healing-structure-production/05.6-267-SUMMARY.md)
  — recurring Quote recovery and exact eight-Machine rollout.
- [Plan 05.6-268 summary](phases/05.6-self-healing-structure-production/05.6-268-SUMMARY.md)
  — scoped connection contract and daemon pool regression closure.
- [Plan 05.6-269 summary](phases/05.6-self-healing-structure-production/05.6-269-SUMMARY.md)
  — Event research workbench, truthful economics, lineage fence, and validation.
- [Production successor proof](phases/05.6-self-healing-structure-production/evidence/runtime-v36-rolling-resume/proof.json)
  — same-epoch Structure → Quote → Opportunity recovery.
- [Session 387–390](../../JOURNAL.md) — append-only chronology and the state
  alignment record.
- [Two-clock learning chapter](../../../docs/learning/105-市场全集与可执行报价必须使用两个时钟.md)
  — operational mental model for Structure/Quote lineage.

## Next Action

1. Run `make control-plane-business-brief` for a fresh read-only business view.
2. Route only a supported finding into the next bounded M1 objective; otherwise
   use `/gsd-explore` for research or record a note/thread update.
3. Keep the coordinator stopped. Treat REPEATABLE READ for the multi-query Event
   authority and explicit >200-group truncation as known R&D follow-ups, not
   reasons to reopen Plan 269 without production evidence.

## Session Continuity

Last session: 2026-09-06
Stopped at: Event Research Workbench completed, deployed, and final-reviewed
Resume file: none; use this STATE plus JOURNAL session 400
