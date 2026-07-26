# M1 Operational Closure Design

**Date:** 2026-07-26
**Workstream:** `m1-perception`
**Scope:** Close the remaining gap between working production data paths and a
continuously operable M1 market-perception platform.

## Outcome

M1 is closed only when all of the following are true at the same time:

1. The current production Dashboard has one canonical, reachable URL.
2. L1 snapshots, the opportunity quote feed, L2/L3 freshness, L3 membership,
   and Dashboard availability are checked automatically every 15 minutes.
3. A failed check reaches the existing Telegram escalation path.
4. The opportunity quote worker has at least one exact 24-hour production
   interval with no failed run and no interval above its scheduling allowance.
5. R2 archival is either working or explicitly recorded as an accepted degraded
   mode. This design chooses to repair it because the current failure is a
   malformed bucket value, not an unavailable service.
6. Phase 05 production evidence, validation, teaching material, and planning
   state agree with the code and production state.

Real-money order placement is outside M1 and is not a closure criterion.

## Approaches Considered

### A. Reclaim the old short Vercel alias

Reattach `polymarket-arbitrage.vercel.app` in Vercel project settings.

- Advantage: no documentation URL changes.
- Disadvantage: requires account-level alias management and gives no benefit
  over the already active stable project domain.

### B. Adopt the current stable Vercel project domain

Use
`https://polymarket-arbitrage-jiangwen-su-s-projects.vercel.app`
as the canonical Dashboard URL.

- Advantage: it is already bound to current successful production deployments,
  is stable across commit deployments, and is visible in Vercel/GitHub evidence.
- Disadvantage: the URL is longer.

### C. Add a custom domain

Provision a new custom domain and DNS records.

- Advantage: best operator-facing name.
- Disadvantage: expands scope into DNS ownership and creates another external
  dependency without improving M1 capability.

**Decision:** Approach B. A custom domain can be added later without changing
the monitoring contract.

## Components and Data Flow

### Canonical Dashboard probe

Polywatch probes the stable project URL without following redirects.

- HTTP 200 is healthy.
- HTTP 302/307 to Vercel SSO is healthy because deployment protection is the
  expected outer gate.
- HTTP 404 with `x-vercel-error: DEPLOYMENT_NOT_FOUND`, transport failure, or
  any 5xx response is unhealthy.

This check proves that the deployment/alias exists. Authenticated browser
acceptance separately proves that application routes render production data.

### L1 and opportunity feed

The existing L1 decision keeps its snapshot auto-unpause behavior and adds:

- `quote_feed:last_complete_age_seconds`: alert on `fail`.
- `quote_feed:collector_state`: alert on `error`, `stopped`, or check `fail`.
- `/arbitrage/opportunities?min_edge_bps=0`: alert on transport failure,
  non-object JSON, wrong strategy/profit basis, or missing opportunities list.

An empty opportunity list is healthy. It means no current candidate, not a
broken feed.

### L2/L3

The existing L2 decision continues to ignore
`ws:connection_state=WAITING_FOR_EVENT` when real event freshness is good. It
adds explicit gates that top-level health cannot express:

- `l3:active_count` must equal 10 tokens.
- `l3:membership_convergence` must pass.
- `l3:worst_market_freshness` must pass.
- evidence sample and promoter ledger checks must pass.

Any explicit `fail` remains an alert. Informational WS quiet state remains a
non-alerting warning.

### R2 repair

Production currently has `POLYARB_R2_BUCKET` containing literal quotes and
trailing whitespace. Correct it to `polyarb-snapshots`, verify read access, run
one normal snapshot, and require `r2:upload_recent_success=true`.

No object deletion or credential rotation is part of this repair.

## Failure Handling

- Polywatch keeps a single best-effort Telegram message per scheduled tick.
- Auto-unpause remains limited to the established stale L1 snapshot case.
- Opportunity, L2/L3, R2, and Dashboard failures are alert-only; no automatic
  mutation is attempted.
- If Telegram delivery fails, the workflow exits non-zero so GitHub Actions
  supplies its existing failure notification.

## Verification

1. Unit tests exercise each new decision branch before implementation.
2. Ruff, focused pytest, full pytest, Dashboard typecheck/build, documentation
   contracts, and planning-status run locally.
3. The new commit is pushed and its GitHub CI, Fly, and Vercel deployments are
   checked.
4. Polywatch workflow is dispatched once in dry-run mode and its decisions are
   inspected.
5. Production checks confirm Dashboard deployment availability, L1 opportunity
   feed freshness, L3 10/10 convergence, R2 upload success, and the exact
   24-hour quote-run aggregate.
6. An authenticated browser checks `/status`, `/candidates`, `/signals`, and a
   real `/l3/<asset_id>` page.

## Out of Scope

- Trade execution or wallet use.
- Strategy profitability claims after fees.
- New monitoring vendors.
- Custom DNS.
- Re-running the already passed Phase 05.4 L3 24-hour evidence window.
