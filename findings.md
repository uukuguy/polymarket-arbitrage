# Findings & Decisions: M1 production capability closure

## Requirements

- Preserve strict N=5 markets / 10 tokens and existing recipe thresholds.
- Prove every promoter cycle and every relevant membership transition during a
  24-hour soak, not merely T+0/T+6/T+12/T+18/T+24 snapshots.
- Persist actual WS mutation outcomes, desired/actual membership, per-market
  book/OHLC freshness, watchdog/reconnect events, and runtime identity.
- Retain six-hour human checkpoints as summaries, not primary evidence.
- Expose all operator commands through Makefile targets.
- Keep current production read-only until a separate deployment authorization.

## Research Findings

- `l3_promote.run_periodic` runs every 300 seconds: approximately 72 promoter
  ticks occur in each six-hour interval.
- `_l3_active_set`, `_last_promote_at_s`, and
  `_last_book_levels_write_at_s` are process-local latest-state values, not
  historical evidence.
- `l3:active_count` is informational and does not affect overall health.
- `promote_run` currently ignores Boolean failures returned by WS
  add/remove operations, then commits its intended `_l3_active_set`.
- The book freshness anchor is global: writes from one hot market can hide
  silence in four other promoted markets.
- `l2_book_levels` and OHLC are durable interval evidence, but they cannot
  reconstruct transient under-fill, failed subscription mutations, or missed
  promoter ticks.
- Fly rolling logs already lost the first soak's watchdog interval; logs without
  durable retention cannot support a strict 24-hour absence claim.
- The current plan's six-hour sampling statement therefore proves sampled-time
  health, not interval-wide `throughout` health.

## Technical Decisions

| Decision | Rationale |
|----------|-----------|
| Append one durable promoter-run record per scheduled tick | Missing, failed, frozen, under-filled, and successful ticks all become queryable |
| Persist intended and actual WS membership plus mutation results | Prevents `_l3_active_set=10` from masking failed network subscription changes |
| Track freshness per promoted market/token | A global last-write clock can be kept for compatibility but cannot be the strict gate |
| Persist health samples every 30 seconds with a 75-second maximum gap | Bounds unobserved failure duration and makes interval aggregates mechanically verifiable |
| Persist watchdog/reconnect counters or events with >24h retention | Absence claims cannot depend on a rolling CLI buffer |
| Six-hour checkpoints query aggregates over retained evidence | Human review remains lightweight without becoming the evidence source |

## Issues Encountered

| Issue | Resolution |
|-------|------------|
| Existing strict soak has a valid T+0 but insufficient observation semantics | Retain as diagnostic evidence; do not use it for Phase 05 closure |
| A telemetry deploy changes exact instance/image identity | Restart the formal 24-hour soak after deployment and readiness proof |
| Self-hashes alone cannot prevent a privileged writer from backfilling or rewriting evidence | Add server `recorded_at`, bounded late-write checks, database append-only triggers/privileges, a dedicated retention role, and raw-row re-query digests |
| A manifest created after selecting T0 cannot prove its policy was fixed before the interval | Bind an immutable attempt-unique manifest to the database before a future T0; reject the whole attempt if that exact T0 sample fails |
| Fixed checkpoint filenames can accidentally join different soak attempts | Every manifest declares attempt-unique report paths; all later checkpoints and final verification consume that exact manifest |

## Resources

- `src/polyarb/observation/l3_promote.py`
- `src/polyarb/daemon/ws_consumer.py`
- `src/polyarb/http/l2_health.py`
- `src/polyarb/storage/l2_supabase_mirror.py`
- `.planning/workstreams/m1-perception/phases/05-ws-book-prices/05-06-PLAN.md`
- `.planning/workstreams/m1-perception/phases/05-ws-book-prices/05-SOAK-LOG.md`
- `.planning/threads/market-observation-architecture.md` §1.6 and §2.9

## Visual/Browser Findings

- The connected browser runtime reported no available browser, so authenticated
  Dashboard acceptance remains externally blocked. Anonymous HTTP 200 was not
  substituted for a logged-in acceptance test.

## Production opportunity-feed findings — 2026-07-26

- `GET https://polyarb-l1.fly.dev/arbitrage/opportunities?min_edge_bps=0`
  returns HTTP 503 with exact body `{"error":"quote run unavailable"}`.
- The deployed image is built from Git SHA
  `95bf1bd8714b92056c1ca6cca2d13ac9bd3d06d5`; it includes the H-009 quote
  store, collector, scanner, and HTTP route.
- H-009 deliberately shipped local-only. `crontab` contains snapshot and
  retention jobs but no quote collection job.
- The L1 app machine owns `/data/state.db` on volume
  `vol_40olm80dgol2xqn4`. The running cron machine has no volume mount, so a
  cron-side SQLite write could never become visible to the HTTP route.
- The cron machine has 256 MB RAM and its 2026-07-26 00:00 snapshot job exited
  137. It is not a safe host for the quote producer.
- Latest production snapshot 711 contains 1,278 eligible, unique YES tokens in
  254 neg-risk groups. With `clob_batch_size=500`, one collection is three
  sequential public CLOB requests.
- Timestamped production capacity observation at 2026-07-26 00:40 UTC:
  run 1 fetched all 1,278/1,278 books in three batches, collector elapsed
  1,013 ms and wall elapsed 1.035 s. This is less than 1% of the approved
  120-second interval and comfortably below the 300-second quote SLA.
- Immediately after run 1, the public route returned HTTP 200 with five
  positive gross-before-fees candidates at `limit=5`, proving the existing
  store→scanner→HTTP chain works when a complete run exists.
- The returned candidates are discovery leads, not executable trade approval.
  Very large gross edges can indicate known-universe incompleteness or market
  semantic issues; production scheduling must not silently upgrade them into
  order instructions.
- The approved direction is a separate fail-soft task inside the L1 app
  process: immediate collection after startup, then every 120 seconds, with
  durable no-overlap enforcement and `/health` chain-truth.
- Existing production warnings discovered but kept outside this focused change:
  malformed R2 bucket configuration and an event-bus database password failure.
