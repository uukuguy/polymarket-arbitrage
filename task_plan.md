# Task Plan: M1 production capability closure

## Goal

Replace the logically insufficient six-hour spot-check proof with durable,
continuous evidence for every L3 promoter cycle, actual WS membership, per-market
data freshness, watchdog behavior, and exact runtime identity before restarting
the strict 24-hour Phase 05 soak.

## Current Phase

Phase 7 — Consolidated M1 production repair

## Phases

### Phase 1: Diagnose and choose direction

- [x] Separate soak duration from monitoring resolution
- [x] Audit durable versus ephemeral evidence surfaces
- [x] Identify chain-truth gaps in promoter, WS, per-market freshness, and logs
- [x] Obtain user approval for observability-first gap closure
- **Status:** complete

### Phase 2: Written design contract

- [x] Write the continuous-observability design spec
- [x] Reclassify the current re-soak as diagnostic-only in canonical state docs
- [x] Self-review the spec for ambiguity and missing acceptance criteria
- [x] Commit the design/state artifacts
- **Status:** complete

### Phase 3: User review gate

- [x] Ask the user to review the committed design spec
- [x] Apply requested corrections, if any
- [x] Obtain explicit approval for implementation planning
- **Status:** complete

### Phase 4: Implementation planning

- [x] Invoke writing-plans
- [x] Register a focused M1 gap phase in ROADMAP/STATE
- [x] Include Makefile targets and deployment/soak gates
- [x] Pass GSD plan structure, Nyquist coverage, and independent checker gates
- **Status:** complete

### Phase 5: Implementation and verification

- [x] Execute under TDD with no production mutation before its explicit gate
- [x] Verify durable event history and continuous health sampling
- [x] Deploy only with separate production authorization
- [x] Restart and pass a strict 24-hour soak with continuous evidence
- **Status:** complete

### Phase 6: Production neg-risk opportunity feed

- [x] Reproduce production HTTP 503 and identify missing quote runs
- [x] Prove the existing cron process cannot feed the HTTP service database
- [x] Obtain approval for an in-process, fail-soft quote collector
- [x] Write and self-review the production rollout spec and implementation plan
- [x] Add failing scheduler and health chain-truth tests
- [x] Implement the quote worker, lifecycle wiring, settings, and health checks
- [x] Run focused and full local verification
- [x] Deploy to L1 and record a timestamped capacity observation
- [x] Verify repeated complete runs and a fresh HTTP 200 feed
- [x] Update the M1 manual, learning docs, JOURNAL, and project state
- **Status:** complete

### Phase 7: Consolidated M1 production repair

- [x] Stop treating the old 24-hour observation as an open-ended wait
- [x] Record the repeated 1 GB snapshot OOM as a capacity/topology defect
- [x] Build a single incident inventory across snapshot, quote freshness, health, and notifications
- [x] Produce a staged M1→M2→live-arbitrage design before implementation
- [x] Self-review the staged design for ambiguity and scope drift
- [ ] Obtain written-spec review before implementation planning
- [ ] Create an executable repair plan with resource, deployment, and qualification gates
- [ ] Implement and verify the repair in dependency order
- [ ] Obtain a new clean production baseline and start a meaningful 24-hour observation
- **Status:** in progress

## Key Questions

1. What evidence is required to prove every 5-minute promoter cycle rather than
   only five human-selected instants?
2. How do we distinguish intended L3 membership from actual WS membership?
3. What sampling/retention guarantees make a 24-hour `throughout` claim valid?
4. Which production changes require a new deploy and therefore invalidate the
   current strict soak identity?

## Decisions Made

| Decision | Rationale |
|----------|-----------|
| Current re-soak becomes diagnostic-only | Its T+0 is valid, but current surfaces cannot prove interval-wide health |
| Keep 24-hour soak horizon | Duration provides operational exposure; the defect is observation resolution |
| Use continuous machine evidence plus six-hour human summaries | Detection cadence and review cadence are different concerns |
| Restart strict soak after observability deployment | A deploy changes runtime identity, and pre-deploy evidence cannot validate post-deploy behavior |
| Run quotes inside the L1 app process every 120 seconds | The HTTP route and collector must share `/data/state.db`; the cron process has no mounted volume |
| Keep the public quote SLA at 300 seconds | Operationalizing the producer must not relabel stale quotes as executable |
| Make quote collection fail-soft and non-overlapping | A quote outage must be visible without stopping snapshots or serving partial runs |

## Errors Encountered

| Error | Attempt | Resolution |
|-------|---------|------------|
| Generic `gsd-tools state begin-phase` corrupted the narrative workstream STATE template | 1 | Root-caused template mismatch, restored canonical STATE manually, and reserved STATE/ROADMAP writes for the orchestrator |
| Temporary shell probe used zsh's read-only `status` name | 1 | Renamed the variable to `http_code` |
| First Fly SSH command requested interactive VM selection | 1 | Addressed the L1 app machine explicitly with `--machine` |
| First remote SQL probe lost shell-quoted empty strings | 1 | Replaced empty-string predicates with `length(column) > 0` |

## Notes

- Do not claim that five six-hour samples prove `min(active_count) == 5`
  throughout 24 hours.
- No production deploy, restart, config/secret change, or H-009 work belongs in
  the historical Phase 05.4 design/spec-review steps.
- Production now returns a fresh HTTP 200 feed after automatic run 2→3→4;
  HTTP 503 remains the fail-closed meaning for missing/stale quote truth.
- The latest production universe has 1,278 eligible YES tokens across 254
  neg-risk groups, requiring three configured CLOB batches.
- The current repair must not reduce the scope to snapshot OOM: direct
  snapshot-failure visibility, quote freshness under snapshot load, and
  component-level recovery notifications are separate chain-truth gaps.
