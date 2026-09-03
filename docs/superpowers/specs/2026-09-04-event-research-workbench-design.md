# Event Research Workbench Design

## Goal

Give each active, unexpired Polymarket event one focused M1 investment-research page. The page must help an operator decide whether the event deserves further work, what data is blocking that work, and which bounded group facts warrant analysis. It must not become a raw-data mirror, an execution screen, or a Certified-opportunity claim.

## Scope and navigation

The dashboard adds `/business/events/[event_id]` as the canonical event detail route.

Every Event cell in the Structure, Quote Coverage, and Analysis main tables links to that route. Links append `from=structure`, `from=quotes`, or `from=analysis`. The source parameter chooses the initially focused section only; it never changes the facts, ranking, lineage, or eligibility displayed by the page.

The route serves the current active Quote generation and its exact parent Structure generation. It rejects a requested event that is closed, expired, absent from the current Structure generation, or cannot be proven to belong to that lineage. Such a result is unavailable/not-published, never a zero-valued investment conclusion.

## Research-first information architecture

### 1. Research judgment header

The header answers the first operating question: is this an event worth spending research time on?

- Event title, tags, open state, scheduled end, and time remaining.
- Event liquidity, volume, active-market breadth, and linked neg-risk group count.
- Data freshness and exact Structure/Quote/Analysis lineage.
- An explicit next-action state: `repair coverage`, `ready for analysis`, `no positive group edge`, or `structure context unavailable`.

This is a factual routing state, not a buy/sell recommendation. It never labels an event investable or executable.

### 2. Structure section

This section answers whether the market universe is structurally understandable before prices are considered.

- Event tags and market breadth.
- Neg-risk groups associated with the event.
- Each group's type, expected versus named active members, quality, and reason code.
- Missing source fields shown as unavailable, never converted to zero.

The default Structure entry point opens this section.

### 3. Quote Coverage section

This section answers whether the event has enough valid quote legs for downstream calculation.

- Event-level totals for coverage gaps, analysis-ready groups, complete/no-edge groups, and context gaps.
- Bounded group table ordered by actionable defects first, then readiness.
- Expected quote legs, received inputs, missing inputs, structure quality, and a concrete repair action.

Price distance from 0.5, single-leg depth, and any opportunity-style score are forbidden from this section. A complete group is not implied to have value.

The Quote Coverage entry point opens this section.

### 4. Analysis section

This section answers whether a fully covered group has a positive combined-price candidate.

- Positive-edge candidates only, ordered by theoretical gross profit:
  `(1 - bundle_cost_per_bundle) × max_bundle_size`.
- Capital required, theoretical gross profit, gross ROI, bundle cost, limiting executable size, coverage, and group identifier.
- Non-positive and blocked state counts appear as funnel context, not as candidate rows.

Every candidate is labelled research-only. Certified opportunities remain exclusively in the existing Opportunities product and require their independent certification path.

The existing `executable_economic_value` field is a ranking proxy, not a valid dollar-profit label, and must not be shown as money in the workbench. The displayed economics use these explicit definitions:

- `capital_required_usd = bundle_cost_per_bundle × max_bundle_size`;
- `gross_profit_usd = (1 - bundle_cost_per_bundle) × max_bundle_size`;
- `gross_roi_bps = gross_profit_usd / capital_required_usd × 10,000`, only when capital required is positive.

Fees, slippage, top-of-book depletion, partial fills, simultaneous-leg execution, resolution wording, oracle/dispute, and settlement delay remain `not-assessed` until their own verified data contracts exist.

The Analysis entry point opens this section.

### 5. Lineage and limitations

A compact, collapsed audit area carries generation IDs, source counts, materialization timestamps, and documented missing/unsupported data. It explains that PostgreSQL contains a bounded business projection while authenticated R2 artifacts retain source detail. The page must never silently represent unavailable market-level or order-book-level evidence as empty data.

## Data contract and capacity boundary

The workbench reuses only current bounded facts already held in PostgreSQL:

- `m1_structure_intelligence_events` for event facts;
- `m1_structure_intelligence_groups` for structural-risk facts;
- `m1_analysis_candidate_rows` for exact same-lineage coverage and analysis facts;
- publication pointers and candidate projections for generation fencing and freshness.

The API returns one explicit, generation-bound event-workbench envelope. It may aggregate event-local counts at read time but does not materialize raw markets, raw order-book levels, or complete R2 artifacts. Requests remain bounded to 200 group rows per section and use server-side allowlisted ordering.

The envelope is assembled atomically from one fixed current pointer snapshot; the browser must not compose existing paginated Structure, Quote, and Analysis endpoints. Its route is:

`GET /perception/business/events/{event_id}?from=structure|quotes|analysis&focus_group_id=<optional>&observed_generation=<optional>`

`from` is a closed enum used only for initial section focus. `focus_group_id` highlights an event-owned group. `observed_generation` detects that a list changed before a click; it never authorizes a historical read. The response contains an `anchor` with exact Structure and Quote generations, alignment state, and a changed-since-entry flag.

Initial group facts expose distinct counts for `expected`, `observed`, `executable`, `non_executable`, and `missing` legs. `observed` is never treated as executable. A separate bounded legs endpoint may return at most 200 top-of-book leg summaries for the focused group; it explicitly warns that those summaries do not prove simultaneous multi-leg execution.

No new persistent projection is introduced. This preserves the active Supabase capacity guard: the database is already constrained, so a user-facing detail page cannot justify duplicating the 175k-market or 100k-membership source universe.

## Failure semantics

- Wrong generation or stale lineage: `unavailable / generation-not-current`.
- Current quote or candidate projection absent: `not-published`, not a zero count.
- Event is closed or its end time is in the past: `unavailable / event-not-operational`.
- A source field is missing: `unknown` with the source limitation visible.
- An event has no positive candidates after a complete analysis: display the explicit `no positive group edge` result.

## Acceptance criteria

1. All three main tables render an accessible event link to the same canonical route.
2. The event route serves only current, open, unexpired, same-lineage facts.
3. The source context changes initial focus only; refreshing or sharing the URL preserves truth.
4. The page visibly separates Structure, Quote Coverage, and Analysis responsibilities.
5. Quote Coverage has no price-extremity or single-leg opportunity ranking.
6. Analysis orders positive candidates by theoretical gross profit and labels them non-certified.
7. No raw-market mirror or new durable projection is added.
8. API, PostgreSQL, dashboard source-contract, typecheck/build, and desktop/mobile browser checks cover the new page and unavailable states.
9. A fixture with bundle cost `0.9` and max bundle size `15` displays capital `$13.50`, theoretical gross profit `$1.50`, and gross ROI `1111.11 bps`; it must not display the legacy ranking proxy as dollar value.
