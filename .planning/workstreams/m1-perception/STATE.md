---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: market-perception
current_phase: 05.1
status: ready_to_execute
stopped_at: Phase 05.1 planned in 3 waves; next execute 05.1-01
last_updated: "2026-07-18T00:21:46Z"
last_activity: 2026-07-18
progress:
  total_phases: 11
  completed_phases: 8
  total_plans: 56
  completed_plans: 52
  percent: 93
---

# M1 Perception — Current State

## Current Position

- **Phase:** 05.1 — Durable L2 data-chain recovery (inserted gap phase)
- **Plan:** 05.1-01 ready — listener lifecycle + durable reconciliation pump
- **Status:** production chain operationally restored, permanent self-heal not implemented
- **Active workstream:** `m1-perception`

## VERIFIED — 2026-07-18 production facts

- L1 recovered from a full 5GB Fly volume after extension to 15GB and bounded
  retention. Fresh snapshot, Supabase mirror, and M2 opportunity feed are working.
- L2 was stale for about 40 days because L1 event publishing was disabled and a dead
  asyncpg LISTEN connection could not wake the documented reconnect loop.
- Enabled `POLYARB_EVENT_BUS_ENABLED=1` on L1 and restarted the single L2 machine.
- L2 startup catch-up advanced cursor `482 → 516`; candidate, WS, and mirror ages
  returned to seconds; strict `/health` returned HTTP 200 and fresh TOB rows appeared.
- Supabase still reports 117 distinct active candidate assets while the real WS set has
  2 assets, proving candidate projection drift across cold start.
- `l3:active_count = 0/10`; Phase 05 Plan 06 soak remains blocked until Phase 05.1
  implements and proves durable self-healing.
- No local polyarb/pytest/flyctl/uvicorn workflow process remains running.

## CURRENT-CALL — approved repair

Execute `05.1-01-PLAN.md`: NOTIFY as wake-up hint, 60-second durable cursor
reconciliation, cursor commit after awaited success, and real LISTEN termination
state. Plans 02-03 then add candidate projection reconciliation, chain-truth health,
and no-restart chaos proof.
Do not relax Phase 05's strict N=5 gate merely to make the phase green.

## Remaining Work

- Implement and verify Phase 05.1 permanent self-healing and candidate reconciliation.
- Re-run GAP-401 watchdog/reconnect proof after recovery.
- Start Plan 05-06 only when `/health` is no longer failing on the L2 prerequisites.
- Complete 24h strict soak, teaching chapter 11, VALIDATION flip, SUMMARY, learnings,
  ROADMAP/STATE closure.

## Required Reading

1. `.planning/CURRENT.md` — cross-workstream operational truth.
2. `docs/superpowers/specs/2026-07-18-m1-durable-data-chain-design.md` — approved design.
3. `.planning/workstreams/m1-perception/phases/05-ws-book-prices/05-06-PLAN.md` — original soak gate; do not execute before repair.
4. `.planning/threads/market-observation-architecture.md` §1.6 — chain-truth discipline.

## Resume

```bash
/gsd-resume-work --ws m1-perception
make planning-status
/gsd-execute-phase 05.1 --ws m1-perception
```

## Session Continuity

- **Last session:** 2026-07-18 (Asia/Shanghai)
- **Stopped at:** Production chain operationally restored; permanent design approved.
- **Proceeding to:** Execute Phase 05.1 Plan 01 with RED-first tests.
- **Resume file:** `docs/superpowers/specs/2026-07-18-m1-durable-data-chain-design.md`

## Accumulated Context

### Roadmap Evolution

- Phase 05.1 inserted after Phase 05: Durable L2 data-chain recovery (URGENT).
