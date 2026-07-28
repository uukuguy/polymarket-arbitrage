# Task 7 Implementer Report

Status: IMPLEMENTATION GREEN — formal independent UI audit pending

## Scope

Task 7 only: an observer-only Next.js perception overview, bounded group
history, Task 6 typed public-GET reader, navigation, read-only route smoke, and
living-manual synchronization. No production deployment, cutover, wallet,
signing, balances, orders, or real-money action occurred.

## Implemented truth chain

1. `dashboard/lib/perception.ts` consumes only the Task 6 public
   `/perception/*` GET endpoints. Each overview or group read creates one
   `AbortSignal.timeout(3000)` and shares that absolute deadline across its
   parallel no-store requests. Transport, non-2xx HTTP, invalid JSON, and a
   non-available envelope, or nested response-contract mismatch return a typed
   `unavailable` result. Reconciliation accepts the exact Task 6
   `open/complete/applied/failed` state set.
2. `/perception` distinguishes the complete-read unavailable state from the
   valid `available/count=0` state. Only the latter renders “No certified edge
   right now.” Task 6 fields that do not yet exist—edge/capacity, Structure and
   Quote age, raw/weighted coverage, resource mode—are explicitly labelled
   “not exposed,” never fabricated as zero.
3. `/perception/[group_id]` decodes the Next route segment exactly once and
   the reader re-encodes it exactly once for the public API. Membership
   revisions and matching incidents are merged and sorted by event time. The
   history envelope and every revision must bind the requested group ID.
   Quote batches and opportunity transitions remain explicit not-exposed
   timeline classes until a bounded public contract exists.
4. `make smoke-perception-dashboard` disables curlrc, uses bounded connect and
   total timeouts, requests only `/perception`, accepts application 200 or
   configured Vercel Auth 302/307, and rejects transport errors, 404, and 5xx.
   It is a route-reachability check, not data-freshness evidence.
5. The root navigation, living M1 manual, manual checker target inventory, and
   route markers are synchronized.

## TDD and browser finding

The initial source contract produced 8/8 expected RED failures because the
reader, pages, navigation, and smoke target did not exist. The GREEN
implementation reached 8/8 contracts.

A local browser review then exposed a real route-identity bug: Next supplied
`neg-risk%3Aevent-42`; the page passed it directly to a reader that correctly
encoded again, producing `%253A` and hiding the matching incident. A new
regression first failed RED, then the page decoded once with malformed-percent
fallback. Review-driven RED/GREEN added nested runtime validators, bounded-page
labels, terminal-incident filtering, the legal failed Reconciliation state,
and requested-group history binding. The final contract is 12/12, the displayed
group ID is `neg-risk:event-42`, and the matching incident count is one.

## Verification

```text
Task 7 Dashboard contract: 12 pass
make dashboard-typecheck: pass
make dashboard-build: pass; /perception and /perception/[group_id] are dynamic
make docs-m1-check: pass
make planning-status: 82 plans, no drift
git diff --check: pass
```

Browser review used a local Task 6 JSON fixture only:

- `http://127.0.0.1:3000/perception` → HTTP 200
- `http://127.0.0.1:3000/perception/neg-risk%3Aevent-42` → HTTP 200
- malformed nested JSON fixture on `http://127.0.0.1:3001/perception` → HTTP
  200 typed “Perception unavailable / invalid JSON contract,” not a 500 or zero
- desktop, 375 px overview, and group timeline screenshots were retained
  locally under `output/playwright/` and are intentionally excluded from the
  commit
- the only console error was an unrelated missing `/favicon.ico`

## Remaining boundary

The formal independent six-pillar UI audit still gates declaring Task 7
complete. The independent code reviewer reported two remediation rounds; all
Critical/Important findings must be closed before commit. Task 8 owns
production fault qualification and cutover. Nothing was deployed by Task 7.
