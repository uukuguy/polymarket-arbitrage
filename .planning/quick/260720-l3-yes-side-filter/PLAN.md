# Quick 260720 — L3 Yes-Side Recipe Filter

## Goal

Keep promoted No-token TOB rows from consuming the locked five-market recipe
limit on subsequent promotion ticks.

## Production Evidence

L2 release 36 reached 10/10 on its startup tick, then fell to 8/10 five minutes
later. The mutation-free dry-run warned that a known No token was being treated
as a Yes asset. Once L3 subscribes both outcomes, both sides write TOB rows; the
recipe therefore needs an authoritative Yes-side input boundary.

## Scope

- RED test with one high-depth No row plus five valid Yes rows;
- resolve recent TOB assets through `markets_latest.yes_token_id` before scanner
  evaluation;
- keep incomplete real Yes pairs fail-closed while excluding wrong-side rows;
- preserve the recipe limit and all spread/depth/recency thresholds;
- full regression, L2 redeploy, and at least two real promotion-tick checks.

## Verification

- focused promoter tests and mutation-free production-backed dry-run;
- full repository tests and quality gates;
- production remains 10/10 across startup plus the following 5-minute tick;
- book-level write anchor and database rows remain fresh.
