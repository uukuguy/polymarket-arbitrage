# L3 T0 Coverage Scope Design

## Problem

Production rejected two immutable T0 attempts even though each exact
scheduled-T0 sample passed the locked five-market/ten-token membership,
freshness, mapping, AcceptanceConfig, and identity checks.

- `d22e8fc…` used a `:02→:32` window: book coverage was 8/10 and OHLC
  coverage was 0/5 because OHLC bucket timestamps are minute-aligned.
- `7cee08ff…` used a `:32→:02` window: OHLC coverage was 5/5 but two quiet
  book tokens produced no new raw row during those 30 seconds.

The Phase 05.4 contract accepts T0 from the one complete scheduled-T0 sampler
batch. Exact raw source churn across all identities is a strict cumulative
soak condition, not a requirement that every source emit a new row inside the
first 30 seconds.

## Decision

Treat only the exact `[manifest.t0, manifest.t0 + sample_interval)` report as a
T0 sample probe.

- It still requires the exact ten book and five Yes-OHLC identity key sets in
  the SQL coverage result.
- It records and hash-binds the raw coverage counts unchanged, including
  zeros.
- It accepts zero new raw rows because the five immutable market-sample rows
  already require non-null source timestamps, exact source ages, strict
  `<120s` freshness, five pairs, ten distinct tokens, matching mapping, and
  10/10/10 membership.
- Every cumulative T+6/T+12/T+18/T+24 report continues to require positive
  exact-window raw coverage for all ten book tokens and all five Yes-OHLC
  identities.
- `require_24h=True` never receives the T0 exception.

This changes no final 24-hour PASS threshold. It scopes the 30-second T0 gate
to the acceptance contract already written in Plan 05 Task 4.

## Verification

RED/GREEN tests prove:

1. an exact T0 probe with complete identity sets and zero raw churn passes;
2. a T0 probe with missing identity keys remains NOT-CLOSED;
3. longer checkpoints still fail when any exact-window raw coverage is absent;
4. all existing freshness, cardinality, schedule, identity, event, and final
   24-hour tests remain unchanged.
