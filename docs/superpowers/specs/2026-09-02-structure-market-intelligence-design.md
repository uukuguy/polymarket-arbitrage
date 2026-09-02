# Structure Market Intelligence — Design

## Goal

Replace the M1 Structure page's engineering-index presentation with a bounded,
lineage-safe market-research workspace. A user must be able to identify active
events, approaching end times, market activity, and neg-risk structural gaps
without treating missing source data as zero.

## Scope

Phase 1 materializes an event-centric projection for the current certified
Structure generation. It does not copy the full market universe into
PostgreSQL, infer executable pricing, add historical trends, or join Quote
coverage. A later phase can serve market/question/token detail from immutable
R2 event shards.

## Data contract

The current compact index has real event fields but renders the wrong columns:
`title`, `slug`, `active`, `closed`, and normalized `end_time_ms`. Group-truth
fields are `neg_risk_type`, `expected_member_count`, `active_named_count`,
`quality`, and `reason`; the obsolete `complete/supported` projection is
removed.

New pointer-gated APIs are:

- `GET /perception/business/structure/summary`
- `GET /perception/business/structure/events`
- `GET /perception/business/structure/groups`
- `GET /perception/business/structure/events/{event_id}`

Every response contains `status`, `generation_key`, `published_at`,
`materialized_at`, source/projection coverage, `reason_code`, and a keyset
cursor where paginated. The requested generation must equal the current
published Structure generation. A mismatch returns `generation-not-current`;
the dashboard may refresh its overview once but may not combine generations.

Event items contain title, slug, tags, open/closed state, end time, market
count, active/closed market counts, Gamma liquidity/volume when present, and
neg-risk quality/reason when available. Missing source fields remain `null`
and are accompanied by `missing_fields`; no UI or API converts them to zero.

## Storage and lifecycle

PostgreSQL stores at most one current-generation event projection (about
22,236 rows), small tag arrays, group risk rows that cannot be attached to an
event, and one summary row. It stores derived market counts, never all 175,979
market rows or the tag/membership relations. Each event payload is capped at
4 KiB. The projection relation plus its required B-tree indexes has a hard
45 MB steady-state budget.

Projection construction is candidate-bound and may be published only after
the source component counts and event/group coverage checks pass. Readers use
only the completed current-generation receipt. Production backfill is blocked
unless the database-capacity gate proves current usage plus the candidate peak
fits the 450 MB tier.

## User experience

The page becomes **Structure Market Intelligence**:

1. A trust strip reports freshness, published generation (short form), and
   index coverage; raw generation/artifact/cursor live in a lineage disclosure.
2. A market-universe overview reports events, markets, active/open events,
   ending within 7/30 days, unknown end time, and neg-risk quality buckets.
3. The default event table exposes title, tags, state, end time, market count,
   liquidity, volume, and neg-risk state. It supports bounded text/status/end
   window/tag/quality filters and allowlisted sorts.
4. A structural-risk queue highlights incomplete or unsupported neg-risk
   groups and their source reason.
5. Event detail exposes event metadata, tags, group truth, and an explicit
   statement that market-level detail is not yet published into this bounded
   projection.

`component`, `source_cursor`, and opaque IDs do not appear as main table
columns. They remain available in the lineage disclosure for audit.

## Safety and error semantics

- `not-published` and `research-index-incomplete` return no partial business
  rows and never imply a zero.
- All list queries use a 1–100 limit, server-side allowlisted filters and
  sorts, fixed database deadlines, and keyset cursors bound to generation plus
  filter digest.
- A missing end time, liquidity, volume, or group field renders as source
  missing—not `0`, `false`, or a fake tradability signal.
- Structure status is independent of global qualification. Qualification may
  be shown as context but must not obscure an otherwise available Structure
  data product.

## Acceptance criteria

- The primary Structure screen has no component/cursor/raw-row research table.
- With a current generation, a user can identify active events, ending-soon
  events, activity ordering, and neg-risk gaps within 30 seconds.
- Fixture data agrees field-for-field with normalized Structure rows.
- API and UI cannot mix Structure generations across a page render.
- Pagination is stable with no duplicate or skipped item for an unchanged
  generation/filter set.
- Projection storage remains within 45 MB and production materialization is
  capacity-gated.
- API/Postgres contracts, typecheck/build, and desktop/mobile Playwright
  checks pass.
