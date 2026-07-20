# Quick 260720 — L3 Latest Row Per Asset

## Goal

Make the L3 recipe's `limit: 5` select five distinct markets rather than five
time-series rows that may repeat the same asset.

## Production Evidence

After book-order normalization reached production, mutation-free dry-run
selected five qualifying rows but expanded only three unique markets (six
tokens). The warnings named the same Yes asset twice, proving repeated recent
TOB snapshots were consuming the recipe limit.

## Scope

- RED unit test with newest-first duplicate snapshots;
- collapse the bounded PostgREST result to one newest row per non-empty asset;
- preserve fetch bounds, recipe thresholds, ordering, and mutation behavior;
- run full regression, redeploy L2, and prove strict 5-market/10-token state.

## Verification

- focused L3 promoter tests;
- mutation-free production-backed dry-run proposes 5 markets / 10 tokens;
- full repository tests and quality gates;
- production `/health` and database chain proof after L2 redeploy.
