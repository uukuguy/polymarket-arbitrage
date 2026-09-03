# M1 Business Page Responsibility Design

## Goal

Make the three M1 business pages answer three distinct operational questions.
They form a one-way research flow, rather than three versions of a mixed market
table:

```text
Structure universe → Quote coverage → Analysis funnel → Certified opportunities
```

## Page Contracts

### Structure universe

**Question:** Which market structures are currently worth researching?

The default page contains only events that are open and whose scheduled end is
in the future. It prioritizes upcoming end time, activity/liquidity, market
breadth, and neg-risk structural completeness. It does not show per-leg Quote
facts or make opportunity claims.

Closed and expired records remain available only through an explicit historical
or archive filter. They are retained for lineage, audit, and data-quality work,
but never occupy the default operational table.

### Quote coverage

**Question:** Is the active Structure universe sufficiently current and
executable for combination analysis?

The default page is a coverage-health and exception view scoped to the active,
unexpired Structure universe. It reports coverage, executable depth, quote
freshness, missing legs, non-executable legs, and missing group context, grouped
by event and neg-risk group where possible.

It must not sort by, label, or imply business priority from a single-leg price
extremity. Extreme prices are raw evidence only. The page orders actionable data
quality defects first, then healthy coverage grouped for audit.

### Analysis funnel

**Question:** After combining valid group legs, which facts deserve further
certification?

The page shows the current fenced candidate projection and its explicit
rejection funnel. It presents positive-edge groups first, ordered by executable
economic value (executable notional times gross edge), then gross edge and
stable group identity. It separately reports no-edge, incomplete-coverage,
expired-or-closed, and context-unavailable counts.

A positive-edge candidate is still not a Certified opportunity. Certification
and execution gates remain the sole source of an actionable opportunity.

## Navigation and Truth Boundaries

- Structure may link to Quote coverage for a selected active event or structural
  gap.
- Quote coverage may link to Analysis for the associated complete group.
- Analysis may link to Certified opportunities only after the existing
  certification projection has published a current result.
- No page may silently treat historical, lagging, unavailable, or rejected data
  as a current zero or an opportunity.

## Data and Capacity Rules

- Default operational reads use the current Quote generation and its exact
  parent Structure generation.
- Closed or expired event facts may remain in bounded storage but are excluded
  from default Structure and Quote tables and from positive candidates.
- Quote Coverage remains a bounded audit projection; it must not create a raw
  order-book mirror.
- Analysis retains only the current group-level candidate projection and its
  bounded rejection facts.

## Acceptance Criteria

1. The Structure default table has no closed or expired events.
2. Quote Coverage has no price-extremity ranking or opportunity-like label.
3. Quote Coverage visibly prioritizes actionable coverage defects and explains
   the health of otherwise usable evidence.
4. Analysis orders positive candidates by executable economic value and labels
   all other states as non-opportunity outcomes.
5. The three page subtitles, headings, metrics, and table columns conform to
   their single stated question.
6. The Dashboard and API distinguish current, lagging, unavailable, historical,
   rejected, candidate, and certified facts.
