# Quote Coverage Discovery Design

## Goal

Make Quote Coverage a bounded business-discovery view: rank current, executable
quote evidence by meaningful executable liquidity and unusual YES pricing, while
making every rank explainable and preserving the distinction between a research
lead and a certified opportunity.

## User Outcome

The first screen answers: "Which current markets have enough immediately
executable YES-side liquidity and sufficiently non-neutral pricing to deserve
research first?" It must never imply that a low price, a high score, or a
negative-risk association is itself an arbitrage opportunity.

## Scope

- Replace token-ID order for the Quote research page with a server-side,
  deterministic discovery order across the whole current fenced generation.
- Add bounded, derived discovery evidence to each returned Quote item.
- Enrich each Quote item with event title/end state and a concise neg-risk group
  context from the exact parent Structure generation when that projection is
  available and lineage-aligned.
- Make the Dashboard display the rank reason, executable notional, price
  extremity, readable event context, and concise group context.
- Preserve cursor pagination, pointer-gating, index-integrity checks, and
  current-generation semantics.

## Non-goals

- No opportunity certification, profit calculation, execution recommendation,
  or trading action.
- No complete Structure mirror or new unbounded Postgres projection.
- No dashboard-local sort of a 100-row partial page.
- No implementation of the two follow-on views below in this change.

## Discovery Contract

Only `terminal_state == "executable"` records receive a discovery score.
For valid numeric values:

```text
executable_notional_usd = best_ask_price × best_ask_size
price_extremity_bps = abs(best_ask_price - 0.5) × 10,000
discovery_score = ln(1 + executable_notional_usd) × price_extremity_bps
```

Missing/non-finite/out-of-range price or size yields a score of `0` and an
explicit data-quality reason. Non-executable rows also score `0`. The sort is:

1. `discovery_score DESC`
2. `executable_notional_usd DESC`
3. `token_id ASC`

The formula deliberately requires both economic quantity and non-neutral price:
an extreme $0.001 quote with only $0.02 immediately executable notional remains
below a reasonably deep, materially non-neutral quote. The returned evidence is
not a probability estimate and not an expected-profit estimate.

Each item returns a bounded `discovery` object:

```json
{
  "executable_notional_usd": 39.9,
  "price_extremity_bps": 2100,
  "score": 7770.0,
  "reasons": ["meaningful-executable-depth", "non-neutral-yes-price"]
}
```

Reasons use a small closed vocabulary:
`meaningful-executable-depth`, `non-neutral-yes-price`,
`insufficient-executable-depth`, `missing-or-invalid-quote`, and
`not-executable`. The API must never send a reason labelled "opportunity".

## Context Enrichment and Lineage

`business_quote_page` already knows the Quote generation. It must obtain its
parent Structure generation from `m1_quote_generation_inputs` and only join
`m1_structure_intelligence_events` and `m1_structure_intelligence_groups` for
that exact parent key. If an event/group projection is absent, the Quote row is
still returned with explicit `context_status: "not-indexed"`; no context from a
newer or unrelated Structure generation may be substituted.

The bounded additions are:

- `event_context`: title, open/closed state, end time, or `not-indexed`.
- `neg_risk_context`: short group identifier, group quality, and member count
  when the matching Structure group projection provides it, or `not-indexed`.

The API keeps schema version `m1.business-research-page.v1`: additive fields are
backward compatible. Its cursor must become a composite opaque/encoded key (or
the API must accept a new sort cursor) so the next page continues the global
server-side order rather than reverting to token ID order.

## Dashboard

The Quote table becomes a "Research leads" table. Its columns are:

1. Rank reason and score band (not a false precision score-only badge);
2. Market slug and Market ID;
3. YES ask, executable depth, and executable notional;
4. Event title plus end/open state;
5. Neg-risk short ID plus quality/member context;
6. Quote execution state/data-quality reason.

The header explains the formula in plain language and explicitly says this is a
research priority, not a certified opportunity. Raw score may be shown in a
subtle detail line; rank reasons remain the primary explanation.

## Follow-on Views (recorded, not part of this plan)

1. **Combination research:** rank exact, lineage-aligned neg-risk groups by
   group-level executable bundle evidence and relationship completeness. This
   needs persisted group-level quote facts and must not be inferred from this
   per-market rank.
2. **Market importance:** rank by liquidity, volume, event horizon and activity
   as a separate operational-research view. It serves coverage prioritization,
   not price-discovery, and must not alter the default Quote table score.

## Error Handling

- Keep current pointer/integrity failures fail-closed (`not-published` or
  `unavailable`).
- Context enrichment failure is field-local and visible as `not-indexed`; it
  cannot make a current Quote generation unavailable or fabricate context.
- Invalid quote data ranks after valid leads and identifies the exact quality
  condition in `discovery.reasons`.

## Tests and Acceptance

- Postgres tests prove whole-generation score ordering, tie-breaks, invalid and
  non-executable rows last, and pagination continuity.
- Tests prove context uses only the Quote generation's exact Structure parent
  and reports `not-indexed` for absent parent projections.
- Dashboard decoder tests reject malformed discovery/context payloads.
- Dashboard contract tests verify visible research-priority copy, score reason,
  executable notional, event context, group context, and the non-opportunity
  disclaimer.
- Typecheck/build and an authenticated Playwright inspection verify readable
  desktop and narrow-width layouts with dense but scannable content.
