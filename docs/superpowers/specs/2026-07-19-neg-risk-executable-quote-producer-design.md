# Neg-Risk Executable Quote Producer Design

**Date:** 2026-07-19  
**Climb hypothesis:** H-009 (`opportunity-feed-cadence-sla`)  
**Workstreams:** M1 perception producer + M2 read-only opportunity consumer

## Problem

The existing opportunity route correctly rejects an M1 snapshot older than 900 seconds. Its producer, however, runs a subset snapshot at 00:00 and 12:00 UTC. The contradiction is structural: the route can be executable for only a short window after a long-running snapshot, then correctly returns `stale-snapshot` for most of the day.

Increasing the freshness limit would only relabel stale best asks as executable. Increasing the whole snapshot cron to every 15 minutes is also unsound: a subset run itself takes about 15–30 minutes and carries unrelated Gamma, translation, mirror, R2, and Parquet work.

## Decision

Build a dedicated, read-only **Neg-Risk Executable Quote Producer**. It uses the existing low-frequency snapshot only as a versioned universe of active neg-risk group membership, then independently refreshes the YES top-of-book for every known active sibling using the existing batched CLOB client.

The public feed becomes executable only when both clocks pass:

| Clock | Meaning | Initial SLA | Failure effect |
|---|---|---:|---|
| Universe age | Age of the snapshot defining known active neg-risk sibling membership | < 14 hours | Feed unavailable; no claim of complete known-universe coverage |
| Quote age | Age of one atomic dedicated quote collection for all eligible known siblings | < 300 seconds | Feed unavailable; never return a zero result |
| Collection completeness | Every eligible sibling has an executable YES ask or an explicit non-executable state in the same quote run | 100% per group | Exclude the whole group; do not synthesize a partial bundle |

This is deliberately a **known-universe executable feed**, not a claim that newly listed neg-risk groups are discovered within five minutes. New-group discovery remains bounded by the universe snapshot cadence and is visible in the response.

## Alternatives considered

1. **Raise the current 900-second threshold to 12 hours.** Rejected: it turns delayed snapshot asks into apparently executable prices.
2. **Run the entire snapshot pipeline every 15 minutes.** Rejected: the job duration overlaps the intended interval and repeats unrelated high-cost work.
3. **Use the existing L2 candidate WebSocket set.** Rejected: it tracks a small ranked subset and currently cannot prove complete neg-risk sibling coverage.
4. **Dedicated batched CLOB quote producer.** Chosen: it reuses an existing read-only client and decouples quote freshness from market-universe extraction.

## Architecture

```text
12-hour subset snapshot
  -> active neg-risk membership universe (group + YES token)
  -> dedicated quote producer every 5 minutes
  -> one quote-run ID / captured timestamp / per-token top ask state
  -> group-complete executable opportunity projection
  -> GET /arbitrage/opportunities
```

### Producer input

The producer reads only the latest snapshot's active, non-closed rows with a non-empty `neg_risk_market_id` and `yes_token_id`. It groups rows by `neg_risk_market_id`; the snapshot is the only membership authority.

It calls `ClobClient.get_books(token_ids)` in existing configured batches. No user credentials, order placement, CLOB writes, or WebSocket subscription changes are introduced.

### Quote-run storage

A local SQLite sidecar persists:

- one `neg_risk_quote_runs` row: run ID, universe snapshot ID/timestamp, quote timestamp, token count, successful CLOB response count, terminal producer state;
- one `neg_risk_quotes` row per known YES token: group ID, market ID, token ID, best ask price/size or a bounded non-executable reason, and the parent run ID.

A run becomes `complete` only after every requested token gets one terminal row. A transient CLOB failure makes the run `failed`; it must not partially replace the prior complete run.

The opportunity query reads exactly one latest complete quote-run and its parent universe snapshot. It never mixes quote rows from multiple runs.

### Feed contract

The existing route retains its endpoint and gross-before-fees semantics, but returns:

- HTTP 200 + `count=0` only when the latest complete run and universe both meet SLA;
- HTTP 200 + `count>0` only for group-complete executable bundles;
- HTTP 503 `quote run unavailable` when no complete run exists;
- HTTP 503 `quote age ... exceeds 300s` for stale quotes;
- HTTP 503 `universe age ... exceeds 50400s` for stale membership.

The H-008 diagnostic classifier is extended with bounded quote/universe stale categories. It continues to treat every non-200 response as non-zero.

## Operational behavior

A dedicated local command performs one collection. A production scheduler invokes it every five minutes only after a deployment-specific capacity check proves that full known-universe batching finishes well inside the interval. The scheduler must use no overlapping runs: lock acquisition failure is observable and leaves the prior run intact.

Initial rollout has three gates:

1. Offline fixture tests prove atomic run selection, no mixed-run projection, stale boundaries, group completeness, and lock behavior.
2. A read-only production capacity observation records token count, batch count, elapsed time, and response coverage without promoting readiness.
3. Only after repeated complete runs with quote age below 300 seconds may the manual move the feed from conditional to verified for the **known universe**. It still does not authorize real-money execution.

## Safety and non-goals

- No threshold is relaxed.
- No real order, wallet, API credential, or signed request is used.
- No current L1 snapshot scheduler, L2 candidate set, or Phase 05.1 quiet-edge gate is modified in the first implementation.
- No claim is made about groups listed after the latest universe snapshot.
- Fees, fill probability, atomic execution, and actual trading remain outside this producer.

## Verification

Tests must cover:

- fresh complete quote run with zero and positive opportunities;
- stale quote and stale universe independently return 503;
- one missing/non-executable sibling excludes its entire group;
- failed collection never replaces the prior complete run;
- overlapping collector cannot create mixed state;
- route uses one run ID only;
- Make/CLI entry remains read-only and exposes run/universe/quote freshness;
- H-009 climb confirmation requires local gates plus a timestamped production capacity observation, using the same immutable evidence discipline as H-008.

## Follow-on question

After H-009, decide whether universe discovery itself needs a tighter cadence. That is a separate product-coverage decision; it must not be hidden inside quote freshness.

