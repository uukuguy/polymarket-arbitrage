# Task Plan: Phase 05 continuous observability gap closure

## Goal

Replace the logically insufficient six-hour spot-check proof with durable,
continuous evidence for every L3 promoter cycle, actual WS membership, per-market
data freshness, watchdog behavior, and exact runtime identity before restarting
the strict 24-hour Phase 05 soak.

## Current Phase

Phase 5 — Implementation and verification

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

- [ ] Execute under TDD with no production mutation before its explicit gate
- [ ] Verify durable event history and continuous health sampling
- [ ] Deploy only with separate production authorization
- [ ] Restart a strict 24-hour soak with continuous evidence
- **Status:** pending — ready to start Plan 01

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

## Errors Encountered

| Error | Attempt | Resolution |
|-------|---------|------------|
| Generic `gsd-tools state begin-phase` corrupted the narrative workstream STATE template | 1 | Root-caused template mismatch, restored canonical STATE manually, and reserved STATE/ROADMAP writes for the orchestrator |

## Notes

- Do not claim that five six-hour samples prove `min(active_count) == 5`
  throughout 24 hours.
- No production deploy, restart, config/secret change, or H-009 work belongs in
  the design/spec-review steps.
