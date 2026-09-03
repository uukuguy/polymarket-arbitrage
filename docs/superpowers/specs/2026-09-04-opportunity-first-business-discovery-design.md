# M1 Opportunity-First Business Discovery Design

## Goal

Make the Dashboard's primary business view lead with current, certified combination opportunities. Separate them from bounded Analysis candidates and raw Quote evidence so no single-leg extreme price can appear as an investment signal.

## Product Hierarchy

1. **Certified opportunities** are primary. Show an item only when its projection is current for the active Quote generation and parent Structure generation. Sort by `gross_edge_bps × max_bundle_size`, then gross edge, then group ID.
2. **Analysis candidates** are bounded, non-certified group facts: complete-leg coverage, bundle cost, gross edge, minimum executable size, rejection reason, and event-state consistency.
3. **Quote Coverage** is evidence drill-down and data-quality audit. It must not rank per-market price extremity as a business lead.

## Current-Lineage Rule

A zero is current only when the opportunity pointer names the active Quote generation, that Quote names the active Structure parent, and projection status is available. A stale projection is `lagging`, never a current zero.

## Candidate Projection

Build a bounded current-generation projection from Quote rows grouped by `neg_risk_market_id`, joined only to the exact parent Structure projection. A group is investable only when the structure group is complete-supported and every expected member has an executable, non-expired, active quote.

- `bundle_cost = SUM(YES ask)`
- `gross_edge_bps = (1 - bundle_cost) × 10,000`
- `max_bundle_size = MIN(YES ask depth)`
- State is one of `positive-edge`, `no-edge`, `incomplete-coverage`, `expired-or-closed`, or `context-unavailable`.

Only `positive-edge` appears in the candidate table. Other states are persisted funnel/rejection facts, not fabricated zeroes.

## Event Validity Gate

Exclude a quote from an investable candidate when its exact parent event is closed, ends at or before evaluation time, or conflicts with active/closed state. Keep it only in Quote Coverage with its exclusion reason.

## Dashboard Layout

- `/business/opportunities`: current Certified opportunities and explicit current/lagging/no-projection state.
- `/business/analysis`: funnel cards, positive candidates, and rejection breakdown.
- `/business/quotes`: group-linked evidence and data-quality audit.

## Safety and Acceptance

Candidate computation cannot restart workers, publish an opportunity pointer, change qualification, or trade. Only the existing fenced certifier can publish Certified opportunities.

- Old opportunity data cannot appear current after Quote advances.
- Ended, closed, or contradictory events cannot enter positive candidates.
- Bundle math and executable size have deterministic tests.
- Dashboard distinguishes certified, candidate, rejected, unavailable, and lagging.
