# L3 OHLC Source Freshness Design

## Production finding

Selected A4 passed T0 but failed at health sample seq 34:

- sample: `2026-07-24T11:24:16.796713Z`
- market: `565064`
- latest Yes book: `2026-07-24T11:22:45.155Z` (91.641s old)
- reported Yes OHLC: `2026-07-24T11:22:00Z` (136.796s old)
- reason: `yes_ohlc_stale`

`l2_ohlc_1m.bucket_ts` is a minute bucket label, not the timestamp of the
latest observation inside that bucket. Treating the label as freshness adds
between zero and almost 60 seconds of artificial age. It makes the locked
`<120s` OHLC gate phase-dependent and stricter than the actual source-data
freshness contract.

## Decision

For each selected Yes token, sampler freshness reads:

```sql
SELECT asset_id, max(ts)
FROM l2_top_of_book
WHERE mid_price IS NOT NULL
GROUP BY asset_id
```

This timestamp is the latest source observation that deterministically
contributes to the regular `l2_ohlc_1m` view.

The field remains `yes_ohlc_at` in the immutable evidence schema for
compatibility, but its precise meaning is “latest OHLC source observation.”
No schema or migration change is required.

## Preserved boundaries

- Book freshness continues to use `l2_book_levels`.
- OHLC source freshness continues to require a non-null mid-price and strict
  `<120s`.
- T+6/T+12/T+18/T+24 exact coverage continues to query `l2_ohlc_1m` bucket
  rows. This change cannot manufacture cumulative coverage.
- Runtime credentials already have read-only access to both
  `l2_top_of_book` and `l2_ohlc_1m`.
- A4 remains permanently invalid; the repair requires a new exact-SHA boot,
  readiness proof, manifest, and T0.

## Verification

Unit SQL-contract and real PostgreSQL tests prove sampling returns the latest
non-null base observation rather than the truncated bucket start. Existing
sampler/verdict tests continue to enforce non-null timestamps, exact computed
ages, five identities, and the strict 120-second boundary.
