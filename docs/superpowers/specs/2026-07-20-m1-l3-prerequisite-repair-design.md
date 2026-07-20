# M1 L3 Prerequisite Repair Design

**Date:** 2026-07-20  
**Workstream:** `m1-perception`  
**Target:** unblock Phase 05 Plan 06 without weakening its strict gate  
**Status:** design approved in conversation; written-spec review pending

## 1. Outcome

Make the existing five-market L3 promotion contract reachable by repairing two
upstream prerequisites:

1. keep both Yes and No token identities in the durable `markets_latest`
   projection and look them up by the Yes token ID carried by L2 TOB rows;
2. give L2 a bounded set of liquid, mid-price markets whose books can be
   evaluated by the unchanged L3 spread/depth recipe.

The repair does not declare L3 ready. It only allows the existing strict gate to
be tested honestly: five qualifying markets must expand to exactly ten distinct
Yes/No tokens before the 24-hour production soak may start.

## 2. Verified problem

The promoter timer runs, but production remains at `l3:active_count = 0/10` and
the book-level write anchor is still cold-start.

Two independent first broken links were observed:

- The three active L2 candidates all came from the `near-end` recipe in one
  event, at mid prices `0.9935`, `0.0005`, and `0.0015`. Two recent books had
  spread about `0.998`; the third was incomplete. The unchanged L3 recipe
  therefore matched no row.
- Production `markets_latest` has `yes_token_id`, but has neither `asset_id` nor
  `no_token_id`. `_fetch_market_token_map()` nevertheless selects all three and
  filters on `asset_id`. Since `l2_top_of_book.asset_id` is the Yes token ID,
  the current five-market to ten-token expansion is structurally invalid.

This is not universe scarcity. Snapshot 574 contained 1,942 markets, including
598 in the `0.1..0.9` mid-price band and 583 that also had liquidity of at least
$500.

A read-only public-CLOB feasibility probe selected the 100 most liquid rows that
met those two snapshot predicates. All 100 books were complete and 86 already
met the existing L3 predicates (`spread < 0.02` and Yes top-10 depth > $500).
Two later, smaller probes failed during TLS handshake and were not retried again.
The successful bounded sample is sufficient to choose a seed cap of 100 without
changing an L3 threshold.

## 3. Chosen design

### 3.1 Durable token-pair projection

Add Alembic revision `006`, chained from `005`, with one add-only nullable TEXT
column: `markets_latest.no_token_id`.

Widen `SupabaseMirror._NARROW_MARKET_COLUMNS` from 11 to 12 columns. The existing
normalizer and SQLite/Parquet schemas already preserve both token IDs, so the L1
mirror only needs to pass `no_token_id` through with the same nullable semantics
as `yes_token_id`.

The L2 temporary SQLite projection must treat `no_token_id` as a real mapped
column rather than a NULL-filled compatibility column. This keeps candidate
recipes and token lookup aligned with the production projection.

Migration execution and application deployment are separate production actions.
Local code and migration tests do not authorize either action.

### 3.2 Token lookup keyed by Yes asset

Treat every selected `l2_top_of_book.asset_id` explicitly as a Yes token ID.
`_fetch_market_token_map()` will:

- select only `yes_token_id,no_token_id` from `markets_latest`;
- filter with `.in_("yes_token_id", selected_yes_asset_ids)`;
- return `{yes_token_id: (yes_token_id, no_token_id)}`.

The promoter must fail closed for an incomplete pair. It must not reuse the Yes
asset as a fallback for a missing market row, and it must not claim ten active
tokens unless all five selected rows provide two non-empty, distinct token IDs.
Under-fill remains visible through the existing `l3:active_count` health check.

The last-known-good freeze policy remains intact for genuine Supabase outages.
A successful query that returns an incomplete pair is data-contract under-fill,
not an outage and not permission to synthesize a token.

### 3.3 Bounded mid-market L2 seed

Add a built-in row-level recipe named `l3-seed` with these locked predicates:

```sql
yes_token_id IS NOT NULL
AND mid_price BETWEEN 0.1 AND 0.9
AND liquidity_usd >= 500
```

Order by `liquidity_usd DESC, market_id ASC` and limit the recipe to 100 rows.
The secondary key makes equal-liquidity selection deterministic. Place this
general seed before specialized built-ins so later recipes retain their more
specific label when an asset overlaps.

The seed enters the existing candidate union and remains subject to the global
`MAX_CANDIDATES = 500` cap, watchlist precedence, reconciliation, and dynamic WS
subscription logic. No new scheduler, process, environment knob, or config flag
is introduced.

Expose it through `make scan-l3-seed`, following the repository's command-entry
contract. The Make target is a local/read-only scanner entry; candidate refresh
continues to invoke the same recipe automatically.

### 3.4 Truthful dry-run boundary

The documented `make l3-promote-dry-run` currently calls `promote_run()`, whose
normal path can update `l2_candidates.l3_promoted_at_ts`. It is therefore not a
truthful no-mutation diagnostic.

Refactor promotion into a planning result and an apply step, or provide an
equivalent explicit `apply_mutations=False` boundary. In dry-run mode the code
may perform reads and compute the proposed token diff, but it must not:

- call WS add/remove methods;
- write `l3_promoted_at_ts`;
- mutate `_l3_active_set` or last-known-good maps;
- advance `_last_promote_at_s` or any write freshness anchor.

The default daemon path remains apply-enabled. The Make target keeps its current
name and becomes consistent with its documentation.

## 4. End-to-end data flow

```text
Gamma clobTokenIds[Yes, No]
  -> normalizer / SQLite / Parquet
  -> L1 narrow Supabase mirror (yes_token_id + no_token_id)
  -> l3-seed chooses <=100 liquid mid-price Yes assets
  -> L2 WS writes Yes-side TOB/depth
  -> unchanged L3 recipe chooses 5 qualifying Yes assets
  -> markets_latest lookup by yes_token_id
  -> 5 complete pairs become exactly 10 WS tokens
  -> book events write l2_book_levels and feed OHLC visibility
```

The strict recipe remains `spread < 0.02`, `depth_yes_usd > 500`, recent TOB,
limit five. Candidate seeding determines what is observed; promotion determines
what qualifies. Keeping those stages separate avoids calibrating two variables
at once.

## 5. Failure and observability contract

- An empty or incomplete token-pair result cannot manufacture readiness.
- A Supabase exception freezes the prior active set and leaves promote freshness
  to age normally, preserving existing fail-soft behavior.
- A successful lookup with fewer than five complete pairs yields fewer than ten
  tokens and remains under-filled in strict health.
- Seed selection errors stay isolated by the existing per-recipe envelope; the
  warning must name `l3-seed`.
- Candidate fetch, reconciliation, cursor, WS event, TOB mirror, promote age,
  active token count, and book-level write age remain the chain-truth surfaces.
- No new health check reads a field that the write path does not mutate.

## 6. Test-first verification

Implementation must proceed in RED to GREEN slices.

Required automated proofs:

1. Alembic 006 is add-only, chains from 005, creates nullable TEXT
   `no_token_id`, and survives upgrade/downgrade/re-upgrade replay locally.
2. Narrow market projection contains exactly 12 expected columns and preserves
   present, absent, and explicit-None `no_token_id` values.
3. L2 temporary DB maps both token columns from a production-shaped row.
4. `l3-seed` returns at most 100 deterministic liquid mid-band Yes assets and
   excludes boundary violations or missing Yes IDs.
5. Candidate union, watchlist precedence, global cap, and specialized-recipe
   attribution do not regress.
6. Token-map PostgREST calls select/filter only real production columns and map
   by `yes_token_id`.
7. Five complete pairs produce ten distinct tokens; a missing, blank, or
   duplicate pair fails closed and never uses the old asset fallback.
8. Dry-run computes a proposed diff while making zero WS calls, zero Supabase
   mutation calls, and zero module-state mutations.
9. Focused observation, storage, migration, Makefile, Ruff, and full repository
   tests pass before any completion claim.

## 7. Delivery sequence and hard pauses

Deliver the repair as a focused M1 prerequisite gap phase before returning to
Phase 05 Plan 06. The implementation plan must include a SUMMARY for every plan
that ships code and must keep `make planning-status` drift-free.

Local completion stops at the production boundary. The next actions require
separate authorization in this order:

1. run Alembic 006 against production;
2. deploy the L1/L2 image containing the mirror, seed, promoter, and dry-run
   corrections;
3. prove `l3:active_count == 10` and a real book-level write;
4. only then start a fresh 24-hour Phase 05 Plan 06 soak.

Neither local tests nor the earlier read-only CLOB probe count as production
readiness evidence.
