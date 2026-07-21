# Phase 05 Prod Soak Log — L3 24h strict verdict

**Soak start (T+0):** 2026-07-20 13:30:55 UTC  
**Soak end target (T+24h):** 2026-07-21 13:30:55 UTC  
**Formal window:** `[2026-07-20T13:30:55Z, 2026-07-21T13:30:55Z)`  
**Release:** `37`  
**Machine:** `85e647c4eed598`  
**Instance:** `01KXZJKY9SKEJAY2DD8MMPNB2E`  
**Image:** `deployment-01KXZJHS9QT8T6X0J33KVPTB5V`  
**Digest:** `sha256:5da8e954897f60cf05f9d6664e99a15247d46a2bd4fd0edbb433c200af8b412c`

This T+0 is intentionally later than the deploy, all prior health samples, and
the rejected `2026-07-20T12:49:25Z` candidate. No earlier observation is part of
the formal interval.

## Scheduled checkpoints

| Checkpoint | UTC target | Asia/Shanghai | Status |
|---|---|---|---|
| T+0 | 2026-07-20 13:30:55 | 2026-07-20 21:30:55 | captured |
| T+6 | 2026-07-20 19:30:55 | 2026-07-21 03:30:55 | missed — not backfilled |
| T+12 | 2026-07-21 01:30:55 | 2026-07-21 09:30:55 | missed — not backfilled |
| T+18 | 2026-07-21 07:30:55 | 2026-07-21 15:30:55 | missed — not backfilled |
| T+24 | 2026-07-21 13:30:55 | 2026-07-21 21:30:55 | overdue/unobserved at handoff 14:01Z |

Sampling may occur a few minutes after a target, but every SQL query must retain
the exact formal T+0 boundary above. Missing scheduled evidence is not backfilled.

## T+0 promoted identity — five markets / ten tokens

The set was re-resolved from current `l2_candidates.l3_promoted_at_ts` Yes rows
joined to authoritative `markets_latest.yes_token_id/no_token_id`; it was not
copied from the prior attempt. A pre-baseline read at 13:29 UTC contained market
`908713`, then the real promoter tick at `13:30:35.609Z` logged
`+2 -2 markets=5 tokens=10`. The post-tick mapping below is the mapping associated
with the strict 13:30:55 health baseline.

| Market | Yes token | No token | `l3_promoted_at_ts` |
|---|---|---|---|
| `540819` | `90435811253665578014957380826505992530054077692143838383981805324273750424057` | `92388629082681805622801622703528982922543286352927708208755887536971583436902` | `2026-07-20T10:55:20.272907Z` |
| `562802` | `83247781037352156539108067944461291821683755894607244160607042790356561625563` | `33156410999665902694791064431724433042010245771106314074312009703157423879038` | `2026-07-20T10:55:20.272907Z` |
| `565064` | `67109747366255871599717338045111308888043498275177238365560740200996017915657` | `103845791232328975452762372781150730610824357544180691092497335946993481308222` | `2026-07-20T13:30:35.527613Z` |
| `601819` | `30630994248667897740988010928640156931882346081873066002335460180076741328029` | `79191939610100241429039499950443680906623179487184628479206155805558220344190` | `2026-07-20T10:55:20.272907Z` |
| `665374` | `55115078421062885512539156303747803058407616201213034911037320915726138659123` | `1910830010387565971650098373488592514702818137344973088263643820608151819241` | `2026-07-20T10:55:20.272907Z` |

## Sub-indicator verdict — Blocker #5 strict N=5

| Sub-indicator | Threshold (D-12 strict) | Result at T+24h | Status |
|---|---|---|---|
| (a) `_l3_active_set` markets throughout 24h | every sample token count `10`, hence market count `5` | T+0 only; T+6/T+12/T+18/T+24 missing | NOT MET — insufficient samples |
| (b) `l2_book_levels` mapped market coverage | `count(DISTINCT market_id) == 5` over exact window | 48,940 rows / 10 token assets / 5 mapped markets | diagnostic PASS |
| (c) `l2_ohlc_1m` Yes-side market coverage | `count(DISTINCT market_id) == 5` over exact window | 732 rows / 5 Yes assets / 5 mapped markets | diagnostic PASS |

**Overall verdict:** NOT-CLOSED (evidence incomplete). The formal wall-clock
window elapsed, but T+6/T+12/T+18 were not captured and T+24 was still
unobserved at the `2026-07-21T14:01:34Z` handoff. Missing scheduled health
samples are not backfilled, so the strict minimum-throughout-window claim cannot
pass from this run. A late read may preserve diagnostic SQL/watchdog evidence,
but cannot turn this window into PASS. There is no YELLOW fallback and no
threshold reduction.

## Timeline

### T+0 — 2026-07-20 13:30:55 UTC

#### Exact production identity

Read-only Fly status before the sample and the machine listing after it agreed on
release `37`, machine `85e647c4eed598`, instance
`01KXZJKY9SKEJAY2DD8MMPNB2E`, image
`deployment-01KXZJHS9QT8T6X0J33KVPTB5V`, and digest
`sha256:5da8e954897f60cf05f9d6664e99a15247d46a2bd4fd0edbb433c200af8b412c`.
The machine remained `started`; its deployment anchor remained
`2026-07-20T10:55:01Z`.

#### Single forced-machine strict health sample

Request interval: `[2026-07-20T13:30:55.085Z, 2026-07-20T13:30:55.760Z]`; Fly
request ID `01KXZVHX6Y46XJFWJKR3SVW0AD-arn`; HTTP `200`. Every named main-chain
check used the same response body:

| Check | Observed | Status |
|---|---:|---|
| `ws:connection_state` | `WAITING_FOR_EVENT` | warn (informational quiet state) |
| `ws:last_event_age_seconds` | `0.0s` | pass |
| `ws:subscribed_count` | `108` assets | pass |
| `event_bus:listener_state` | `listening` | pass |
| `event_bus:last_reconciliation_age_seconds` | `44.1s` | pass |
| `event_bus:cursor_lag` | `0` | pass |
| `mirror:l2_tob_age_seconds` | `20.6s` | pass |
| `candidates:supabase_fetch_age_seconds` | `44.3s` | pass |
| `l3:active_count` | `10/10` tokens | pass |
| `l3:last_promote_at_s` | `20.0s` | pass |
| `l3:last_book_levels_write_at_s` | `19.9s` | pass (`<120s`) |

The root body said `warn` only because the quiet-state connection label is
informational; the HTTP contract and every locked named main-chain check were
green. Unlike the rejected attempt, no earlier green reading was stitched in.

#### Exact-interval SQL boundary reading

A read-only SQL transaction used the literal boundary
`[2026-07-20T13:30:55Z, 2026-07-20T13:33:07.977Z)`. It first resolved five
authoritative Yes identities and five paired No identities, then joined depth
rows through those pairs. It did not fetch a capped REST page.

| Metric in exact interval | Initial value |
|---|---:|
| promoted Yes identities | 5 |
| authoritative No pairs | 5 |
| `l2_book_levels` rows | 40 |
| token assets with book rows | 2 |
| mapped markets with book rows | 1 |
| `l2_ohlc_1m` Yes-side rows | 1 |
| mapped markets with OHLC rows | 1 |

The partial initial coverage is a boundary reading, not the T+24 verdict. The
24-hour gate still requires all five mapped markets for both depth and OHLC.

#### GAP-401 watchdog evidence

The exact-machine rolling buffer contained 43 entries in
`[2026-07-20T13:30:55Z, 2026-07-20T13:33:22.894Z)` and zero
`ws_watchdog: stale` entries. The same T+0 health response showed a quiet
`WAITING_FOR_EVENT` label with real WS event age `0.0s` pass; no reconnect was
triggered. This is only the initial watchdog sample, not the 24-hour verdict.

#### Four blocking mechanisms enforced

- **Single-sample readiness:** formal T+0 is the new 13:30:55 response only;
  neither deployment time nor the rejected sample was reused.
- **Positional book semantics:** release 37's exact digest is unchanged and
  contains commit `7ccd2da`, which ranks BUY descending and SELL ascending before
  TOB/depth projection; array index zero was not interpreted as best price.
- **Row-limit identity leakage:** identity was re-resolved after the promoter
  churn from authoritative Yes rows before any five-market aggregate.
- **Capped-page coverage:** all coverage evidence came from direct read-only SQL
  over the literal formal interval; no newest-1000 REST result was used.

### T+6 — target 2026-07-20 19:30:55 UTC

Missed. No timestamped sample was captured near this checkpoint. Do not
backfill it from a later reading.

### T+12 — target 2026-07-21 01:30:55 UTC

Missed. No timestamped sample was captured near this checkpoint. Do not
backfill it from a later reading.

### T+18 — target 2026-07-21 07:30:55 UTC

Missed. No timestamped sample was captured near this checkpoint. Do not
backfill it from a later reading.

### T+24 — target 2026-07-21 13:30:55 UTC

Overdue and unobserved at handoff `2026-07-21T14:01:34Z`. The next session may
capture a clearly labelled late read-only snapshot and exact-window SQL for
diagnosis, but the missing intermediate health samples already prevent a strict
PASS for this run.

### Late diagnostic — 2026-07-21 14:16–14:18 UTC (not T+24)

This observation was deliberately taken after the formal interval and is not a
replacement for T+6/T+12/T+18/T+24. It preserves current and reconstructable
database truth while leaving the elapsed run `NOT-CLOSED`.

#### Exact identity and one-response health

Read-only Fly status before and after the health request agreed on release `37`,
machine `85e647c4eed598`, instance `01KXZJKY9SKEJAY2DD8MMPNB2E`, image
`deployment-01KXZJHS9QT8T6X0J33KVPTB5V`, digest
`sha256:5da8e954897f60cf05f9d6664e99a15247d46a2bd4fd0edbb433c200af8b412c`,
state `started`, and deployment anchor `2026-07-20T10:55:01Z`.

The forced-machine request interval was
`[2026-07-21T14:16:50.094Z, 2026-07-21T14:16:50.916Z]`; HTTP was `200`.
All values below came from the same response body:

| Check | Observed | Status |
|---|---:|---|
| `ws:connection_state` | `WAITING_FOR_EVENT` | warn (informational quiet state) |
| `ws:last_event_age_seconds` | `0.0s` | pass |
| `ws:subscribed_count` | `105` assets | pass |
| `event_bus:listener_state` | `listening` | pass |
| `event_bus:last_reconciliation_age_seconds` | `47.3s` | pass |
| `event_bus:cursor_lag` | `0` | pass |
| `mirror:l2_tob_age_seconds` | `30.7s` | pass |
| `candidates:supabase_fetch_age_seconds` | `47.4s` | pass |
| `l3:active_count` | `10/10` tokens | pass |
| `l3:last_promote_at_s` | `160.0s` | pass |
| `l3:last_book_levels_write_at_s` | `253.8s` | **warn — strict `<120s` gate failed** |

Therefore this sample is a rejected re-soak candidate, not a new T+0. No
earlier book-fresh sample was stitched into it and no new 24-hour clock began.

#### Direct SQL over the immutable completed interval

At `2026-07-21T14:18:29.783016Z`, one read-only PostgreSQL transaction queried
the literal interval `[2026-07-20T13:30:55Z, 2026-07-21T13:30:55Z)`. The query
used the five T+0 authoritative Yes identities and their paired No tokens,
joined both tokens to `l2_book_levels`, joined only the Yes identity to
`l2_ohlc_1m`, and applied no REST page cap.

| Market | Book rows | Book token assets | Yes OHLC rows | First book | Last book | First OHLC | Last OHLC |
|---|---:|---:|---:|---|---|---|---|
| `540819` | 3,020 | 2 | 87 | `2026-07-20T13:40:32.998Z` | `2026-07-21T12:43:43.748Z` | `2026-07-20T13:40:00Z` | `2026-07-21T13:06:00Z` |
| `562802` | 4,400 | 2 | 91 | `2026-07-20T13:39:21.348Z` | `2026-07-21T13:01:58.849Z` | `2026-07-20T13:39:00Z` | `2026-07-21T13:01:00Z` |
| `565064` | 1,460 | 2 | 51 | `2026-07-20T13:40:26.802Z` | `2026-07-21T11:33:08.487Z` | `2026-07-20T13:40:00Z` | `2026-07-21T12:53:00Z` |
| `601819` | 6,060 | 2 | 110 | `2026-07-20T13:40:34.000Z` | `2026-07-21T13:16:21.996Z` | `2026-07-20T13:40:00Z` | `2026-07-21T13:16:00Z` |
| `665374` | 34,000 | 2 | 393 | `2026-07-20T13:32:09.746Z` | `2026-07-21T13:29:09.565Z` | `2026-07-20T13:32:00Z` | `2026-07-21T13:29:00Z` |
| **Total / coverage** | **48,940** | **10** | **732** | — | — | — | — |

Both reconstructable coverage indicators are 5/5 mapped markets. They do not
repair indicator (a), whose missing scheduled health samples remain
non-reconstructable.

For the next candidate identity, SQL first collapsed `l2_candidates` to the
newest row per asset, then joined authoritative
`markets_latest.yes_token_id/no_token_id` before considering cardinality. The
diagnostic mapping contained five markets (`562802`, `565064`, `601819`,
`665374`, `679021`) and ten complete tokens. This mapping was read after the
failed health sample and is diagnostic only; it is not bound to a new T+0.

#### Watchdog diagnostic and retention boundary

The read-only Fly rolling buffer query at `2026-07-21T14:17Z` returned exactly
100 rows covering only
`[2026-07-21T14:12:01.049911606Z, 2026-07-21T14:17:17.497469453Z]`.
It contained **zero rows from the formal soak interval**, so the formal-window
`ws_watchdog: stale` count is **unavailable**, not zero. The 100-row current
buffer contained no stale match, but that cannot establish the 24-hour
GAP-401 verdict.

## GAP-401 carry-over observation

- Exact T+0 `ws_watchdog: stale` count: `0`
- 24h count: unavailable — Fly rolling buffer no longer covered the interval
- Late current-buffer count: `0` over 100 rows from `14:12:01Z–14:17:17Z`
- Final verdict: NOT VERIFIED for the elapsed window

## Decisions / Follow-up

No production mutation, deploy, restart, threshold/config/secret change, trading,
external submission, or H-009 action was performed to establish this soak.

- If all three T+24 sub-indicators equal five: sign validation, complete Plan 06,
  and run `/gsd-extract-learnings 05 --ws m1-perception`.
- If any indicator is below five: Phase 05 remains open; choose a documented
  re-soak or a Phase 05.1-style gap-closure plan from the evidence.
- If identity changes, a daemon fails, or watchdog stale becomes sustained: do
  not mix observations across identities; record the break and make an explicit
  rollback/re-soak decision.

---

## Strict re-soak window 2 — immutable T+0 accepted

**Re-soak start (T+0):** `2026-07-21T14:29:47.941Z`
**Re-soak end target (T+24):** `2026-07-22T14:29:47.941Z`
**Formal window:** `[2026-07-21T14:29:47.941Z, 2026-07-22T14:29:47.941Z)`
**Release:** `37`
**Machine:** `85e647c4eed598`
**Instance:** `01KXZJKY9SKEJAY2DD8MMPNB2E`
**Image:** `deployment-01KXZJHS9QT8T6X0J33KVPTB5V`
**Digest:** `sha256:5da8e954897f60cf05f9d6664e99a15247d46a2bd4fd0edbb433c200af8b412c`

This is a separate re-soak. It does not repair or replace the permanently
`NOT-CLOSED` first window. Its clock starts only from the one fully green
forced-machine response below.

### Scheduled checkpoints

| Checkpoint | Exact UTC target | Exact Asia/Shanghai target | Status |
|---|---|---|---|
| T+0 | `2026-07-21T14:29:47.941Z` | `2026-07-21T22:29:47.941+08:00` | captured |
| T+6 | `2026-07-21T20:29:47.941Z` | `2026-07-22T04:29:47.941+08:00` | awaiting wall clock |
| T+12 | `2026-07-22T02:29:47.941Z` | `2026-07-22T10:29:47.941+08:00` | pending |
| T+18 | `2026-07-22T08:29:47.941Z` | `2026-07-22T16:29:47.941+08:00` | pending |
| T+24 | `2026-07-22T14:29:47.941Z` | `2026-07-22T22:29:47.941+08:00` | pending |

Sampling may occur a few minutes after each target, but SQL must retain the
literal formal-window boundary. A missed checkpoint remains missed; it is not
reconstructed from a later health response.

### Natural-write readiness before the candidate

A read-only SQL baseline saw `max(l2_book_levels.ts) =
2026-07-21T14:23:04.567Z` with 74,620 total rows. The bounded waiter then saw
80 genuinely newer level rows through `2026-07-21T14:28:09.474Z`. Only after
that natural write did the executor request a new strict health response.

An earlier routing diagnostic at
`[2026-07-21T14:28:57.190Z, 2026-07-21T14:28:58.172Z]` returned HTTP 400 because
the Fly force header was given the incarnation ID rather than the machine ID.
It returned no health JSON, no gate was interpreted, and it is neither a
candidate start nor T+0. The accepted request used machine ID
`85e647c4eed598`; the incarnation remained a separate identity anchor checked
before and after.

### T+0 — one-response strict health conjunction

Request interval:
`[2026-07-21T14:29:47.941Z, 2026-07-21T14:29:48.675Z]`; Fly request ID
`01KY2HAEAJSEHN01ZF4EXQ39CM-arn`; HTTP `200`.

Read-only Fly status immediately before and machine inventory immediately after
agreed on release, machine, instance, state `started`, image, digest, and
deployment anchor `2026-07-20T10:55:01Z` exactly as listed above.

| Locked gate | Observed | Status |
|---|---:|---|
| `ws:connection_state` | `WAITING_FOR_EVENT` | warn — informational quiet state |
| `ws:last_event_age_seconds` | `0.0s` | pass |
| `ws:subscribed_count` | `105` assets | pass |
| `event_bus:listener_state` | `listening` | pass |
| `event_bus:last_reconciliation_age_seconds` | `7.8s` | pass |
| `event_bus:cursor_lag` | `0` | pass |
| `mirror:l2_tob_age_seconds` | `5.5s` | pass |
| `candidates:supabase_fetch_age_seconds` | `7.9s` | pass |
| `l3:active_count` | `10/10` tokens | pass |
| `l3:last_promote_at_s` | `36.7s` | pass (`<600s`) |
| `l3:last_book_levels_write_at_s` | `23.1s` | pass (`<120s`) |

The root body was `warn` only because `WAITING_FOR_EVENT` is informational.
HTTP contract, membership, dedicated L3 clocks, WS/mirror/candidate freshness,
listener, reconciliation, and cursor all passed together in this one body. No
earlier sample was stitched into T+0.

The latest exact-machine promoter line before the request was timestamped
`2026-07-21T14:29:11.807427013Z`:

```text
l3-promote: +0 -0 markets=5 tokens=10 (chain-truth: _last_promote_at_s=1784644152)
```

### T+0 post-tick authoritative identity

A read-only repeatable-read transaction at
`2026-07-21T14:31:19.795864Z` collapsed `l2_candidates` to the newest row per
asset, retained current non-removed rows with non-null `l3_promoted_at_ts`, and
then joined `markets_latest.yes_token_id/no_token_id`. It applied no recipe or
coverage `LIMIT`. The result was five distinct markets, five complete pairs,
and ten distinct tokens:

| Market | Yes token | No token | `l3_promoted_at_ts` |
|---|---|---|---|
| `562802` | `83247781037352156539108067944461291821683755894607244160607042790356561625563` | `33156410999665902694791064431724433042010245771106314074312009703157423879038` | `2026-07-21T12:43:54.658264Z` |
| `565064` | `67109747366255871599717338045111308888043498275177238365560740200996017915657` | `103845791232328975452762372781150730610824357544180691092497335946993481308222` | `2026-07-21T13:49:03.427030Z` |
| `601819` | `30630994248667897740988010928640156931882346081873066002335460180076741328029` | `79191939610100241429039499950443680906623179487184628479206155805558220344190` | `2026-07-20T14:50:50.521370Z` |
| `665374` | `55115078421062885512539156303747803058407616201213034911037320915726138659123` | `1910830010387565971650098373488592514702818137344973088263643820608151819241` | `2026-07-20T16:31:19.735539Z` |
| `679021` | `84846382165343032223975853870946110241285484772019446686866815597350660955868` | `80456575204440643570499123641511043765364487572502085512858581574010154701650` | `2026-07-21T12:23:48.467942Z` |

Release 37 preserves shared positional normalization before all derived data:
BUY levels are ranked descending and SELL levels ascending. Neither array index
zero nor token cardinality was used as a market-identity shortcut.

### T+0 direct SQL boundary evidence

One read-only transaction queried the fixed mapping above over the literal
interval
`[2026-07-21T14:29:47.941Z, 2026-07-21T14:33:20.581467Z)`. No PostgREST page or
row cap was used.

| Market | Book rows | Book token assets | First book | Last book | Yes OHLC rows | First OHLC | Last OHLC |
|---|---:|---:|---|---|---:|---|---|
| `562802` | 0 | 0 | — | — | 0 | — | — |
| `565064` | 0 | 0 | — | — | 0 | — | — |
| `601819` | 0 | 0 | — | — | 0 | — | — |
| `665374` | 160 | 2 | `2026-07-21T14:31:31.820Z` | `2026-07-21T14:32:04.666Z` | 2 | `2026-07-21T14:31:00Z` | `2026-07-21T14:32:00Z` |
| `679021` | 0 | 0 | — | — | 0 | — | — |
| **Boundary total / coverage** | **160** | **2** | — | — | **2** | — | — |

Initial mapped coverage is one of five markets for both depth and Yes-side
OHLC. This is an honest boundary reading, not the T+24 verdict and not an early
failure; the full window still requires all five markets.

### T+0 GAP-401 watchdog boundary

The exact-machine rolling buffer query covered 100 rows from
`2026-07-21T14:23:04.952612885Z` through
`2026-07-21T14:32:43.663119572Z`. Within the re-soak interval through the
read-only query end `2026-07-21T14:33:32.328051Z`, 34 retained entries ranged
from `14:29:49.402903319Z` to `14:32:43.663119572Z` and contained zero
`ws_watchdog: stale` matches. Two successful 20-row book-level write log lines
appeared at `14:31:33.500868706Z` and `14:32:02.898212933Z`.

This is only the T+0 watchdog boundary. The strict 24-hour watchdog verdict
must be based on retained evidence at every scheduled checkpoint; an unavailable
historical buffer is never inferred as zero.

### Re-soak verdict status

| Sub-indicator | Strict threshold | Current result | Status |
|---|---|---|---|
| (a) active markets throughout | every sample `10` tokens = `5` markets | T+0 `10/10` | pending T+6/T+12/T+18/T+24 |
| (b) mapped book coverage | five markets over exact window | boundary 1/5 | accumulating |
| (c) Yes-side OHLC coverage | five markets over exact window | boundary 1/5 | accumulating |
| GAP-401 watchdog | zero stale matches with retained evidence | T+0 zero | accumulating |

**Current verdict:** IN PROGRESS. The next observation must not occur before
T+6 `2026-07-21T20:29:47.941Z` (Asia/Shanghai
`2026-07-22T04:29:47.941+08:00`). Do not sign validation, create
`05-06-SUMMARY.md`, or close Phase 05 before the T+24 strict verdict.
