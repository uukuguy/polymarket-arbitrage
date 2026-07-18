---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: market-perception
current_phase: 05.1
status: executing
stopped_at: Phase 05.1 Plan 04 Task 3 open; await natural 60s quiet edge on instance 01KXSMS80B5AX2FGT5EPRC6V82
last_updated: "2026-07-18T07:19:05Z"
last_activity: 2026-07-18
progress:
  total_phases: 11
  completed_phases: 8
  total_plans: 57
  completed_plans: 55
  percent: 96
---

# M1 Perception — Current State

## Current Position

- **Phase:** 05.1 — Durable L2 data-chain recovery (inserted gap phase)
- **Plan:** 05.1-04 Task 3 open — production natural quiet-edge and mirror-evidence proof
- **Status:** `889fab4` deployed; >=180-second main chain passed, but no natural 60-second quiet window occurred
- **Active workstream:** `m1-perception`

## VERIFIED — 2026-07-18 production facts

- L1 recovered from a full 5GB Fly volume after extension to 15GB and bounded
  retention. Fresh snapshot, Supabase mirror, and M2 opportunity feed are working.
- L2 was stale for about 40 days because L1 event publishing was disabled and a dead
  asyncpg LISTEN connection could not wake the documented reconnect loop.
- Enabled `POLYARB_EVENT_BUS_ENABLED=1` on L1 and restarted the single L2 machine.
- L2 startup catch-up advanced cursor `482 → 516`; candidate, WS, and mirror ages
  returned to seconds; strict `/health` returned HTTP 200 and fresh TOB rows appeared.
- The pre-repair projection drift was 117 active database assets versus 2 WS
  assets. Plan 03 production proof confirms it is now resolved at 5 desired
  keys, 5 active keys, and 5 WS assets with zero stale/missing keys.
- `l3:active_count = 0/10`; Phase 05 Plan 06 soak remains blocked until Plan 04
  restores strict quiet-market freshness and closes Phase 05.1.
- No local polyarb/pytest/flyctl/uvicorn workflow process remains running.
- Phase 05.1 Plan 03 deployed and proved LISTEN reconnect `0 → 1`, poll-only
  cursor `520 → 521` with an exact unchanged notification anchor, L1 publishing
  restoration, and candidate/WS projection `5/5` without restarting L2.
- Final strict `/health` remains HTTP 503 because five valid subscribed markets
  were quiet: WS business-frame age and TOB mirror age exceeded their unchanged
  thresholds while socket pong liveness, cursor, reconciliation, and projection
  were healthy. This is Plan 04's blocker, not a core recovery failure.
- Plan 04 reconnect-gating code `889fab4` is deployed as image
  `deployment-01KXSMPKM78105Q9JZG58GBZYP`, digest
  `sha256:ec90d98e20c6ffe7ee48c899c939dab7a67addf45c28adda6695d13ed6269c4d`,
  on machine `85e647c4eed598`, instance `01KXSMS80B5AX2FGT5EPRC6V82`.
- That exact instance passed the >=180-second startup/candidate/WS/mirror main
  chain. During the following ten-minute read-only monitor, organic business
  frames kept `ws_age < 45s`; no natural 60-second quiet trigger occurred.
- Therefore Plan 04 Task 3 is unproven, not protocol-failed. If the instance
  changes, rebuild the >=180-second baseline before accepting later quiet proof.
- Exact image matrix: present `kill/which/curl/python`; missing
  `pkill/ps/dig/ping`. Temporary Fly organization token was revoked and no token
  value was retained.

## CURRENT-CALL — approved repair

Plans 05.1-01/02/03 are complete. Plan 04 implementation is deployed, but Task 3
remains open until the same instance naturally becomes quiet for 60 seconds and
the receive→mirror evidence chain is observed. Do not infer failure from an active
market that never reaches the trigger, and do not relax Phase 05's strict N=5 gate.

## Remaining Work

- Read-only monitor instance `01KXSMS80B5AX2FGT5EPRC6V82` for a natural
  60-second quiet window and the complete refresh→book→mirror evidence chain.
- If the instance changes, first rebuild the >=180-second healthy main-chain baseline.
- Keep Plan 04 Task 3 open until receive/mirror evidence exists; send success alone is not PASS.
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
$gsd-resume-work --ws m1-perception
make planning-status
```

## Session Continuity

- **Last session:** 2026-07-18 15:19 (Asia/Shanghai)
- **Stopped at:** Structured handoff restored; Plan 05.1-04 Task 3 remains open after `889fab4` deploy and >=180-second main-chain PASS.
- **Proceeding to:** Verify instance identity, then read-only monitor the same instance for a natural quiet edge; rebuild the baseline if instance identity changes.
- **Resume file:** `.planning/workstreams/m1-perception/phases/05.1-durable-l2-data-chain-recovery/05.1-04-PLAN.md`

## Accumulated Context

### Roadmap Evolution

- Phase 05.1 inserted after Phase 05: Durable L2 data-chain recovery (URGENT).
