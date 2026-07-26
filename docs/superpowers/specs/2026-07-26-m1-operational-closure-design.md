# M1 Operational Closure Design

**Date:** 2026-07-26
**Workstream:** `m1-perception`
**Scope:** Close the remaining gap between working production data paths and a
continuously operable M1 market-perception platform.

## Outcome

M1 is closed only when all of the following are true at the same time:

1. The current production Dashboard has one canonical, reachable URL.
2. L1 snapshots, the opportunity quote feed, L2/L3 freshness, L3 membership,
   and Dashboard availability are checked by a resident Fly monitor every
   2 minutes. GitHub Actions retains the 15-minute schedule as an independent
   fallback, not as the primary detector.
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

- The resident monitor records the active failure set in local machine state.
- A new or changed failure set sends Telegram immediately on the first observed
  tick. An unchanged failure is suppressed, with a reminder every 30 minutes.
- Recovery sends one Telegram notification and clears the active failure set
  only after successful delivery. A failed recovery delivery preserves state
  so the next tick retries it.
- Auto-unpause remains limited to the established stale L1 snapshot case.
- Opportunity, L2/L3, R2, and Dashboard failures are alert-only; no automatic
  mutation is attempted.
- The GitHub Actions fallback continues to exit non-zero on delivery or probe
  failure, preserving provider-independent evidence.

GitHub's declared cron expression is not a scheduling SLA: production history
on 2026-07-26 contained an approximately 3.5-hour gap between scheduled runs.
For that reason the Fly-resident 2-minute loop is the operational primary.

## Verification

1. Unit tests exercise each new decision branch before implementation.
2. Ruff, focused pytest, full pytest, Dashboard typecheck/build, documentation
   contracts, and planning-status run locally.
3. The candidate image is checked for the watcher script and a valid
   Supercronic entry before production deployment.
4. The code commit is pushed without triggering the normal all-process Fly
   deploy. Only the `cron` process group is updated; the application machine
   identity, start time, and quote evidence anchor must remain unchanged.
5. At least three consecutive resident monitor ticks are observed in
   production, with no gap above 150 seconds. State-transition tests cover
   first failure, duplicate suppression, reminder, changed failure set,
   failed-delivery retry, and one-shot recovery.
6. The GitHub Actions fallback is dispatched once in dry-run mode and its
   decisions are inspected. CI is then run against the resulting source state.
7. Production checks confirm Dashboard deployment availability, L1 opportunity
   feed freshness, L3 10/10 convergence, R2 upload success, and the exact
   24-hour quote-run aggregate.
8. An authenticated browser checks `/status`, `/candidates`, `/signals`, and a
   real `/l3/<asset_id>` page.

## Out of Scope

- Trade execution or wallet use.
- Strategy profitability claims after fees.
- New monitoring vendors.
- Custom DNS.

## Post-repair L3 continuity window

Release 73 disproved continuity at a real promoter boundary after the original
Phase 05.4 qualification had completed. The corrected release therefore needs
one new immutable continuity window without weakening or replacing the
original A7 evidence.

Three schedulers were considered:

- another Fly process would survive the operator laptop, but changing the
  production process topology during the selected boot would reset the
  identity being qualified;
- GitHub Actions is an independent fallback, but scheduled workflows run from
  the default branch and this exact repair remains isolated until the window
  closes; observed scheduling also has a multi-hour gap;
- a user-domain macOS `launchd` agent can call the existing
  `make l3-soak-checkpoint` verifier every five minutes without touching the
  production daemon.

The selected checkpoint scheduler is `launchd`. It reads the runtime password
from macOS Keychain, constructs the allowlisted direct-TLS DSN only in process
memory, validates the immutable Fly machine/instance/image/release identity,
and creates only manifest-declared reports whose not-before boundary has
passed. It commits and pushes each PASS artifact. T+24 additionally runs the
existing final verifier before recording a local completion marker.

The scheduler is not the production fault detector. Fly's resident two-minute
Polywatch remains responsible for live L1/L2/L3/Dashboard checks and Telegram
alerts. If the Mac sleeps, `launchd` catches up after wake; the database ledger
continues recording every 30-second sample, so delayed artifact generation
does not lose the underlying evidence. Any non-PASS report is retained
immutably and stops later checkpoints.
