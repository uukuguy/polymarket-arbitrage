# Opportunity Feed Chain-Truth Design

**Date:** 2026-07-18  
**Workstream:** `m1-perception`  
**Climb hypothesis:** H-008 (`opportunity-feed-chain-truth`)

## 1. Problem and observed evidence

`make scan-arb-live min_edge_bps=0` currently uses `curl -f`. A non-2xx
response therefore proves that the feed is unavailable, but it does not give an
operator a stable classification that can be consumed by tests, documentation,
or later automation.

The 2026-07-18 read-only production trace established the immediate cause:

- `GET /arbitrage/opportunities?min_edge_bps=0` returned HTTP 503 with
  `{"error":"snapshot age 1216.9s exceeds 900.0s"}`;
- the adjacent L1 strict health request returned HTTP 200 and classified the
  same approximately 1217-second snapshot as `pass`;
- the opportunity route hard-codes a 900-second executable-data limit;
- the L1 scheduler defaults to a 3600-second interval, while its documented
  production health contract is based on a 12-hour subset cadence.

The endpoint is therefore reachable and correctly fail-closed, but its current
freshness SLA is incompatible with its producer cadence. HTTP 503 must never be
reported as a valid zero-opportunity market result.

## 2. Decision

Use two independent stages.

### Stage A — H-008: explain availability without weakening safety

Add a read-only diagnostic command that makes exactly one explicit GET request,
captures both HTTP status and response body, and emits one stable classification:

| Classification | Required evidence | Exit |
|---|---|---:|
| `available-zero` | HTTP 200, valid feed payload, `count == 0` | 0 |
| `available-opportunities` | HTTP 200, valid feed payload, `count > 0` | 0 |
| `stale-snapshot` | HTTP 503 and a parseable snapshot-age error | 2 |
| `feed-unavailable` | other non-2xx response or an error payload not recognized as stale | 2 |
| `invalid-response` | transport succeeds but body violates the expected JSON contract | 2 |
| `transport-error` | DNS, TLS, timeout, connection, or curl failure | 2 |

This stage does not change the endpoint, scheduler, health thresholds, database,
or production configuration. The existing `scan-arb-live` command remains the
compact feed consumer; the diagnostic becomes the operator-facing chain-truth
entry.

### Stage B — H-009: align producer cadence and executable-data SLA

Do not solve availability by blindly increasing the 900-second threshold. A
one-hour-old best ask is not a defensible executable quote. H-009 must select and
verify one coherent production model:

1. collect executable inputs often enough to satisfy the 15-minute SLA; or
2. source opportunity legs from the fresher L2/Supabase chain; or
3. explicitly redefine the product as delayed discovery rather than executable
   opportunity monitoring, with correspondingly different naming and safety
   claims.

H-009 is out of scope for H-008 and requires separate production evidence.

## 3. Components and data flow

The H-008 implementation has three bounded pieces:

1. A pure Python classifier accepts `(http_status, body)` and returns a typed
   diagnostic record. It owns JSON/schema validation and contains no network
   calls.
2. A small CLI accepts status/body supplied by the Make recipe (or test fixture),
   prints one JSON record, and chooses exit 0 or 2 from the classification.
3. `make diagnose-arb-feed-prod` performs a curlrc-isolated, explicit GET to the
   canonical L1 URL, records status and body without dropping 503 payloads, then
   invokes the classifier. It must not contain Fly mutations, POST, secrets,
   schema operations, restart, deploy, scale, or chaos commands.

Data flow:

```text
production GET
  -> HTTP status + raw body
  -> pure classifier
  -> stable class + selected safe metadata
  -> JSON stdout + exit 0/2
```

The diagnostic output may include snapshot age, configured maximum age, count,
strategy, and profit basis. It must not echo headers, cookies, authorization,
database paths, secrets, or arbitrary server exception details beyond a bounded
operator-safe reason.

## 4. Contract and safety boundaries

- HTTP 200 alone is insufficient: the body must be valid JSON with the expected
  feed fields.
- `count == 0` is meaningful only under HTTP 200 and a valid payload.
- HTTP 503 is never normalized to an empty opportunity list.
- The command is read-only and must use `curl --disable --request GET`.
- The production URL has a bounded default and may be overridden only through a
  documented `URL=` Make variable for local fixture testing.
- No automatic retry is added in H-008; retries could hide cadence gaps and
  blur the observation timestamp.
- The M1 manual and `.planning/CURRENT.md` remain downgraded until H-009 supplies
  new production evidence. H-008 improves diagnosis, not feed readiness.

## 5. Verification

TDD fixtures cover every classification and exit code. Integration tests run the
Make entry against a local HTTP fixture for:

- valid zero feed;
- valid non-zero feed;
- stale snapshot 503;
- unrelated 503;
- malformed JSON;
- HTTP 200 with an invalid schema;
- connection failure.

Contract tests assert exact GET/curlrc isolation, canonical endpoint routing,
absence of mutation tokens, and manual/CURRENT synchronization. The climb
evaluator for `opportunity-feed-chain-truth` scores planning, unit,
integration, CLI safety, and truth synchronization rather than falling back to
unrelated M2 gates.

H-008 is confirmed only when all gates pass and a read-only production run emits
`stale-snapshot`, `available-zero`, or `available-opportunities` consistent with
the captured status/body. It does not require production to be healthy.

## 6. Deliverables

- pure feed-response classifier and CLI;
- `make diagnose-arb-feed-prod`;
- unit, fixture integration, Make safety, and evaluator tests;
- M1 manual troubleshooting/command updates;
- CURRENT/JOURNAL/climb evidence updates;
- H-009 seed describing the unresolved cadence/SLA decision.

No deployment or production mutation is included.
