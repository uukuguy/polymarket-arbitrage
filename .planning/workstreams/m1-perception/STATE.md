---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: market-perception
current_phase: 05.1
status: executing
stopped_at: Phase 05.1 Plan 02 complete; next execute 05.1-03
last_updated: "2026-07-18T00:47:03Z"
last_activity: 2026-07-18
progress:
  total_phases: 11
  completed_phases: 8
  total_plans: 56
  completed_plans: 54
  percent: 96
---

# M1 Perception — Current State

## Current Position

- **Phase:** 05.1 — Durable L2 data-chain recovery (inserted gap phase)
- **Plan:** 05.1-03 ready — chaos diagnostic, deploy, production proof, teaching/closure
- **Status:** durable listener/pump/projection/health implemented locally; production not deployed
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

Plans 05.1-01/02 completed locally: NOTIFY is a wake-up hint, the 60-second durable
cursor pump is the only cursor owner, candidate history converges by composite key,
and strict health reads live listener/pump truth. Execute Plan 03 next for the
image-aware diagnostic, deploy, and no-restart production proof.
Do not relax Phase 05's strict N=5 gate merely to make the phase green.

## Remaining Work

- Deploy and prove Phase 05.1 self-healing without an L2 restart.
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
- **Stopped at:** Plans 05.1-01/02 complete locally; full phase suite 82 passed.
- **Proceeding to:** Execute Phase 05.1 Plan 03 production chaos proof and closure.
- **Resume file:** `.planning/workstreams/m1-perception/phases/05.1-durable-l2-data-chain-recovery/05.1-03-PLAN.md`

## Accumulated Context

### Roadmap Evolution

- Phase 05.1 inserted after Phase 05: Durable L2 data-chain recovery (URGENT).
