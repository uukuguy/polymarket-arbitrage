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
| T+6 | 2026-07-20 19:30:55 | 2026-07-21 03:30:55 | pending |
| T+12 | 2026-07-21 01:30:55 | 2026-07-21 09:30:55 | pending |
| T+18 | 2026-07-21 07:30:55 | 2026-07-21 15:30:55 | pending |
| T+24 | 2026-07-21 13:30:55 | 2026-07-21 21:30:55 | pending |

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
| (a) `_l3_active_set` markets throughout 24h | every sample token count `10`, hence market count `5` | TBD | pending |
| (b) `l2_book_levels` mapped market coverage | `count(DISTINCT market_id) == 5` over exact window | TBD | pending |
| (c) `l2_ohlc_1m` Yes-side market coverage | `count(DISTINCT market_id) == 5` over exact window | TBD | pending |

**Overall verdict:** TBD. Phase 05 closes only if all three sub-indicators pass.
There is no YELLOW fallback and no threshold reduction.

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

Pending: exact identity, one forced-machine health sample, ten-token active
count, exact-window mapped book/OHLC coverage, and watchdog stale count.

### T+12 — target 2026-07-21 01:30:55 UTC

Pending: same evidence fields.

### T+18 — target 2026-07-21 07:30:55 UTC

Pending: same evidence fields.

### T+24 — target 2026-07-21 13:30:55 UTC

Pending: same evidence fields plus strict three-sub-indicator verdict.

## GAP-401 carry-over observation

- Exact T+0 `ws_watchdog: stale` count: `0`
- 24h count: TBD
- Final verdict: TBD

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
