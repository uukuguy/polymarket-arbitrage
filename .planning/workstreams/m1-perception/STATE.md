---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: market-perception
current_phase: 05
status: blocked_on_l2_freshness
stopped_at: Phase 05 Plan 06 cannot start soak while L2 WS/candidate/mirror paths are stale
last_updated: "2026-07-17T10:21:36Z"
last_activity: 2026-07-17
progress:
  total_phases: 10
  completed_phases: 8
  total_plans: 53
  completed_plans: 52
  percent: 98
---

# M1 Perception — Current State

## Current Position

- **Phase:** 05 — WS book prices / L2→L3 upgrade
- **Plan:** 05-06 not started; 05-01 through 05-05 have SUMMARY artifacts
- **Status:** blocked before the 24h soak gate because production L2 is not fresh
- **Active workstream:** `m1-perception`

## VERIFIED — 2026-07-17 production facts

- L1 recovered from a full 5GB Fly volume after extension to 15GB and bounded
  retention. Fresh snapshot, Supabase mirror, and M2 opportunity feed are working.
- L2 `/healthz` returns HTTP 200 but body status `fail`; strict `/health` returns 503.
- L2 has 60 subscribed assets and event-bus listener `listening`, but the data chain is stale:
  - `ws:last_event_age_seconds ≈ 1,764,233s` — fail
  - `mirror:l2_tob_age_seconds ≈ 1,784,287s` — fail
  - `candidates:supabase_fetch_age_seconds ≈ 3,446,382s` — fail
  - `l3:active_count = 0/10`; promote timer runs, book-level writer has never written
- Therefore Phase 05 Plan 06's 24h soak cannot begin and Phase 05 is not complete.
- No local polyarb/pytest/flyctl/uvicorn workflow process remains running.

## CURRENT-CALL — next diagnosis

The first failure boundary is upstream of L3: candidate refresh is ~40 days stale and
WS is waiting despite 60 subscriptions. Diagnose the production chain in this order:

1. candidate refresh fetch/auth/error logs and its last-success state mutation;
2. WS connection/re-subscribe behavior after receiving the candidate set;
3. mirror write freshness after the first real event;
4. only then L3 promotion/book-level thresholds and the 24h soak.

Do not relax Phase 05's strict N=5 gate merely to make the phase green. If the existing
05-06 plan assumes a healthy L2 precondition without a repair task, insert a focused
gap phase/quick fix before starting the soak.

## Remaining Work

- Restore fresh L2 candidate, WS event, and mirror chains with production evidence.
- Re-run GAP-401 watchdog/reconnect proof after recovery.
- Start Plan 05-06 only when `/health` is no longer failing on the L2 prerequisites.
- Complete 24h strict soak, teaching chapter 11, VALIDATION flip, SUMMARY, learnings,
  ROADMAP/STATE closure.

## Required Reading

1. `.planning/CURRENT.md` — cross-workstream operational truth.
2. `.planning/HANDOFF.json` — structured resume baton.
3. `.planning/workstreams/m1-perception/phases/05-ws-book-prices/.continue-here.md` — diagnosis context.
4. `.planning/workstreams/m1-perception/phases/05-ws-book-prices/05-06-PLAN.md` — original soak gate; do not execute before repair.
5. `.planning/threads/market-observation-architecture.md` §1.6 — chain-truth discipline.

## Resume

```bash
/gsd-resume-work --ws m1-perception
make planning-status
curl -sS https://polyarb-l2.fly.dev/health | uv run python -m json.tool
```
