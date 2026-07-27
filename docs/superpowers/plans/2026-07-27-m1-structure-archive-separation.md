# M1 Structure / Archive Separation — Implementation Plan

> **Goal:** Make certified Gamma market structure the only online M1→M2 input;
> move full CLOB/Parquet collection to an explicitly non-critical Archive product.
> A failed archive must never replace, stale, or pause the Structure/Quote path.

## Why this is the next repair

The current `run_snapshot()` bundles Gamma structure, full CLOB prices, Parquet,
R2, event bus, and the replacement of `markets` in one job. The deployed app
scheduler calls that bundled path, while an unmounted cron VM calls it separately
for a second time. A real 47k-market run was SIGKILLed after about 31 minutes;
the co-located Quote worker then crossed its 300-second availability boundary.

The repository already has the hard part of a Structure revision: every published
`snapshots.id` atomically commits source coverage, event membership, neg-risk
group truth, and current `markets`; quote runs bind to that snapshot and its
membership hash. Do **not** create a competing `structure_revisions` table.
Instead, classify snapshots by data product and only admit the new `structure`
product to online health and M2 quote selection.

## Non-negotiable product boundaries

| Product | Creates / updates | May publish `markets`? | May require CLOB/Parquet/R2? | Online consumer |
|---|---|---:|---:|---|
| `structure` | Gamma catalogues, source coverage, memberships, group truth, `markets` | yes, only after full Gamma/reconciliation proof | no | strict health, Quote, M2 paper |
| `archive` | immutable Parquet/R2 artifact and archive attempt/result metadata | never | yes | research, audit, backtest |
| legacy combined snapshot | historical evidence only | never accepted after cutover | historical | no new M2 input |

`structure` retains the current final Gamma member reconciliation. Skipping CLOB
must not skip the check which prevents event/market membership disagreement.
Structure's per-market `fetched_at_ms` means *Gamma structural observation time*,
not a tradable quote timestamp; all price columns remain null. Quote continues
to be the sole price fact and remains bound to the published structure snapshot.

## Implementation wave 1 — durable product identity and read paths

**Files:** `schemas.py`, `sqlite_store.py`, `health.py`,
`neg_risk_quote_store.py`, migration/health/quote tests.

1. Add `data_product TEXT NOT NULL DEFAULT 'legacy_combined'` and
   `archive_status TEXT NOT NULL DEFAULT 'legacy'` to `snapshots` using the
   existing additive `init_schema` migration discipline. New allowed values are
   `structure` and `archive`; legacy rows retain their historical identity.
2. Extend both snapshot writers with `data_product` and `archive_status`.
   A `structure` write must set `market_view_published=1` only after the existing
   source-coverage and membership publication validators pass. It must use a
   documented no-archive sentinel and purge must explicitly skip it; no generic
   `Path(...).unlink()` may touch a sentinel.
3. Change `MarketTruthHealth`'s latest/complete queries to require
   `data_product='structure'`. Before the first successful new Structure write,
   strict `/health` is deliberately not production-ready rather than accepting
   legacy combined evidence.
4. Change `_latest_completed_published_snapshot` and its provenance rechecks in
   `neg_risk_quote_store.py` to require `data_product='structure'`. Thus Quote
   and opportunity output fail closed until the new revision exists.
5. Add `archive:last_attempt`/`archive:last_success_age_seconds` health checks
   as non-blocking warn-only evidence. They must be sourced from archive rows,
   not from `snapshot:latest_attempt`; the first-wave scheduler table continues
   to represent Structure scheduler attempts only.

**RED/GREEN tests:** legacy published row rejected for current M2 input; complete
Structure row accepted; an archive row cannot supersede Structure health; purge
does not remove a no-archive sentinel; Quote run revision drift still rejects.

## Implementation wave 2 — extract Structure collection without CLOB

**Files:** `snapshot/orchestrator.py`, `snapshot/cli.py`, `daemon/scheduler.py`,
`tests/m1-perception/test_orchestrator.py`, scheduler/CLI tests, `Makefile`.

1. Factor the existing Gamma events → market keyset → normalization → bounded
   reconciliation path into a shared internal collector. Preserve all current
   fail-closed source coverage and event/group truth tests before moving code.
2. Add a `product` selection to the snapshot CLI with explicit values
   `structure` and `archive`; keep the old CLI form as a temporary explicit
   archive-compatible alias only if tests prove no operator script silently
   changes product. Expose new Make targets:
   - `make sync-structure-local` — local mutation, Gamma only, publishes a
     certified Structure revision when valid.
   - `make archive-markets-local` — local mutation, explicit full CLOB/Parquet
     research archive; never publishes online markets.
3. `structure` persists the full Gamma structural market set (not a liquidity
   subset), because M2 needs complete event membership before filtering
   opportunities. It performs Layer 1 and structural Layer 2 only; no Layer 4
   CLOB validation, price fetch, Parquet write, R2 upload, Supabase price mirror,
   or archive event bus side effect is allowed on its critical path.
4. `archive` may fetch CLOB and write Parquet/R2, but must pass
   `publish_markets=False`. A failed archive records its own outcome and leaves
   `markets`, Structure health, Quote worker, and scheduler state unchanged.
5. Change `run_snapshot_in_subprocess`/`SnapshotScheduler` to invoke only
   `snapshot --product structure --json --low-priority`. This is the online
   scheduler product. Its attempt row and health checks remain the first-wave
   `snapshot:*` contract.

**RED/GREEN tests:** Structure subprocess argv contains `--product structure`;
Structure never calls CLOB/parquet/R2/mirror functions; its complete revision is
accepted by Quote; Archive CLOB failure returns an archive failure without
changing current markets/Structure health; a Structure final Gamma reconciliation
failure is not publishable.

## Implementation wave 3 — scheduling, capacity, and production contract

**Files:** `crontab`, `fly.toml`, health/watcher/manual/learning docs, production
runbook, deployment tests.

1. Remove the volume-less cron's direct `python -m polyarb.snapshot snapshot`
   and `--full` jobs. It cannot authoritatively publish `/data/state.db`; leaving
   it in place creates false operational evidence. Keep Polywatch there.
2. Do not schedule production Archive in this wave. Archive is P1 research/audit
   and must wait for an independently budgeted batch host or durable object-store
   result channel. The local explicit archive command remains available.
3. Set Structure scheduler cadence to the approved 5-minute target only after
   measured single-run elapsed time and Quote overlap prove it can meet the
   30-minute Structure / 300-second Quote SLOs. Use the current one-hour cadence
   only as a pre-deploy compatibility default; no document may call that
   production-ready.
4. Replace obsolete Fly comment claiming 1 GB is sufficient for the bundled CLOB
   workload. Do not resize merely to make Archive run. If Gamma-only Structure
   plus HTTP+Quote fails its measured envelope, choose the smallest tested
   transition capacity with a rollback note; 4/8 GB remain permissible but are
   evidence-driven, not acceptance criteria.
5. Deploy only after local tests and a dedicated deployment authorization already
   granted by the user. Record exact release ID. Prove: a certified Structure
   revision, Quote complete run bound to it, no Archive schedule, independent
   health/Telegram behavior, and capacity samples during at least two Structure
   ticks. Only then start a new 24-hour online-data qualification observation.

## Chain-truth checklist

| Path | Writer | Reader/gate | End-to-end proof |
|---|---|---|---|
| Structure success | `write_snapshot_streaming(data_product='structure')` | market truth health + quote store | new complete revision accepted by Quote |
| Structure failure | scheduler `snapshot_attempts` terminal row | `snapshot:latest_attempt`, counter, Polywatch L1 | forced SIGKILL reports and alerts |
| Archive failure | archive row/attempt metadata | `archive:*` health + Polywatch archive component | forced CLOB/R2 failure does not alter Structure/Quote |
| Quote version binding | quote run `universe_snapshot_id` + source truth hash | scanner/HTTP | structure revision mismatch rejects candidate output |
| Recovery | component incidents | Telegram + resident state | L1 recovery sent while L2 stays active |

## Explicit non-goals / stops

- No M2 cross-machine sharing, wallet, live order, or storage migration in this
  plan; those need the Stage B revision manifest design after Stage A proves
  the single-machine contract.
- No production deployment, Fly resize, cron mutation, or 24-hour restart is
  performed by writing this plan. Those occur only in wave 3 after code tests.
- Do not preserve compatibility by allowing `legacy_combined` data to certify
  current M2 use. It is valuable history, not a post-repair production baseline.

## Verification commands at each commit

```bash
uv run pytest tests/m1-perception/test_orchestrator.py tests/m1-perception/test_health_endpoint.py \
  tests/routing/test_neg_risk_quote_store.py -v
uv run ruff check src/polyarb/snapshot src/polyarb/storage src/polyarb/http \
  src/polyarb/daemon tests/m1-perception tests/routing
make docs-m1-check
make planning-status
```
