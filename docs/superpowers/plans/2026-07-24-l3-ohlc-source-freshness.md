# L3 OHLC Source Freshness Implementation Plan

**Goal:** Remove minute-bucket phase error from the strict OHLC freshness chain
without weakening cumulative OHLC coverage.

1. Add a RED SQL-contract assertion that the sampling aggregate reads
   `l2_top_of_book`, filters non-null `mid_price`, uses `max(ts)`, and does not
   use `l2_ohlc_1m`.
2. Change only the sampler aggregate's OHLC freshness source.
3. Update the real PostgreSQL assertion from bucket start to the latest source
   observation timestamp.
4. Keep cumulative coverage SQL and verdict requirements unchanged.
5. Run focused/real PostgreSQL, phase-wide, full, Ruff, compile, docs, image,
   and planning gates.
6. Preserve A4's failure evidence, deploy an exact new SHA, prove new boot
   readiness, and start an attempt-unique A5 manifest/T0.
