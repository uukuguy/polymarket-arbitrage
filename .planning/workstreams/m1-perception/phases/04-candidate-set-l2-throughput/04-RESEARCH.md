# Phase 04: Candidate Set 扩容 + L2 Throughput 验证 + 投影 Gap 收尾 — Research

**Researched:** 2026-05-28
**Domain:** Supabase REST fetch · SQLite in-memory adapter · scanner recipe column analysis · WS throughput measurement · /health chain-truth
**Confidence:** HIGH — all claims verified against actual codebase files or official supabase-py docs

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

- **D-01:** compute_candidates 改读 Supabase `markets_latest`，拉全量进临时 SQLite 复用现有 scanner。L2 收到 NOTIFY 后，从 Supabase REST 拉当前 snapshot 的 `markets_latest` 全量 → 写入临时 SQLite → 现有 scanner recipe SQL 原封不动跑。
- **D-02:** 临时库形态 = `:memory:` SQLite + 适配层把 narrow 行填成 scanner 期望的 schema。适配层必须对「recipe 依赖了 narrow 没有的列」**fail-loud**（启动期校验 recipe 列依赖 ⊆ 临时库可填列集合）。
- **D-03:** 复用现有 recipe（near-end 过滤 + liquidity 降序排序）+ MAX_CANDIDATES=500 cap。
- **D-04:** 保留 `bootstrap_asset_ids` 作冷启动兜底。
- **D-05:** 用真实 candidate set 规模在 polyarb-l2 prod 上跑真实 WS 订阅 + Inj L2-4 storm。
- **D-06:** throughput pass = 三指标全过：(1) 零丢帧 (2) watchdog 不误触 RECONNECTING (3) 内存稳定。具体阈值由 baseline 先行定。
- **D-07:** `markets_latest.yes_token_id` 加为 nullable 列（Alembic add-only）。
- **D-08:** GAP-200 — 区分两种 mirror 禁用态：(a) url 也空 → 不注册 sub-check；(b) url 有但 service_key 空 → 注册 `mirror:l2_tob_age_seconds` status=fail "mirror disabled by config (service_key empty)"。

### Claude's Discretion

- Plan 切分波次（Wave）— gsd-planner 按依赖图自动决。
- 临时库具体建表代码 / 适配层映射细节 — 沿用现有 SQLiteStore schema 定义。
- 测试覆盖 — 沿用 Phase 03 RED-first chaos test pattern + scanner 既有测试。
- commit boundary — 一个决策组一个 plan，plan 内多 commit 可接受。
- D-06 三指标的具体数值阈值 — research 阶段拉 baseline 定。

### Deferred Ideas (OUT OF SCOPE)

- candidate recipe 调优（D-03 只先用默认 recipe）
- 合成高负载压测（D-05 选了真实负载）
- refresh debounce 调优（当前 60s 不动）
- Supabase Pro / Neon 升级（触发条件未到）
- POLYARB_WS_TEST_KILL nightly cron（m5-polywatch trial-2 后纳入）
</user_constraints>

---

## Summary

Phase 04 has three independent capability tracks that can plan in parallel:

**Track A (Data Source Swap, D-01/D-02/D-03):** The core change. `compute_candidates` currently opens `settings.db_path` (L2 local SQLite) as a read-only URI at `l2_candidate_refresh.py:104`. That DB is empty — markets table only lives on L1. The fix replaces that path with a Supabase REST fetch of `markets_latest`, loads rows into an `:memory:` SQLite with the full `markets` DDL schema, and passes the in-memory DB path to `run_recipe`. The scanner engine at `observation/scanner.py:131` takes `db_path: Path`, so a Python `pathlib.Path(":memory:")` is the right shim. The adapter must NULL-fill columns not in the narrow 10-column projection — and fail-loud at construction time for recipes that reference those absent columns.

**Track B (Throughput Validation, D-05/D-06):** The existing `chaos-l2-inj4` Makefile target (line 855) only verifies watchdog LOGIC with 3 bootstrap assets. Phase 04 expands it to use the real candidate set. WsConsumer already exposes `frame_count` (integer incrementing per frame) and `last_event_at_s` (epoch float). No dropped-frame instrumentation exists yet — must be added. Watchdog false-trip detection reads `current_state` from the /health surface. Memory measurement must use `psutil` (already in dev deps).

**Track C (Projection Gaps, D-07/D-08):** D-07 is a one-migration Alembic add-only column. D-08 is a 5-line logic change at `l2_health.py:180` — split the current `if l2_mirror_enabled:` gate into three branches: url-empty (skip), url-set-but-key-empty (register fail sub-check), both-set (existing pass logic). Both tracks are independent of A and B.

**Primary recommendation:** Plan waves as: Wave 0 (test gaps + Alembic migration D-07 standalone), Wave 1 (D-01/D-02 data-source swap + adapter + fail-loud), Wave 2 (D-03 recipe wiring + D-04 bootstrap validation), Wave 3 (D-05/D-06 throughput baseline then prod chaos), Wave 4 (D-08 GAP-200 health logic). D-07 can be Wave 0 because it's schema-only and unblocks D-01 (so markets_latest has yes_token_id for the watchlist path).

---

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Fetch markets_latest rows | L2 daemon (asyncpg listener callback) | Supabase REST | L2 pulls on NOTIFY; Supabase is the source |
| Build :memory: SQLite adapter | L2 daemon (compute_candidates) | — | Data transformation before recipe execution |
| Run scanner recipes | L2 daemon (observation/scanner.py) | — | Scanner already owns this; no tier change |
| WS subscribe loop + frame counting | L2 daemon (WsConsumer) | — | WsConsumer is the WS boundary |
| Watchdog false-trip detection | L2 daemon (WsWatchdog) + /health | — | /health surfaces watchdog state |
| Memory measurement | chaos test (psutil) | — | External to daemon; read from /proc/pid |
| yes_token_id column | Supabase Postgres (Alembic migration) | L1 mirror write path | Schema lives in Supabase; mirror writes it |
| GAP-200 /health logic | L2 daemon (l2_health.py) | Settings | Health reads Settings to decide which sub-check to register |

---

## Standard Stack

### Core (all already in project)
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| supabase-py | 2.30.0 (pinned `>=2.10,<3`) | REST fetch from Supabase `markets_latest` | Already used in `supabase_mirror.py` and `l2_supabase_mirror.py` |
| sqlite3 (stdlib) | 3.x bundled | `:memory:` SQLite adapter | Scanner expects sqlite3; builtin, no extra dep |
| pandas | project dep | `run_recipe` returns DataFrame | Scanner already uses `pd.read_sql_query` |
| psutil | dev dep (`[project.optional-dependencies] dev`) | Memory measurement for D-06 | Already in project |
| alembic | project dep | D-07 schema migration | Already used for 001/002/003 migrations |
| loguru | project dep | Logging (project-wide standard) | CLAUDE.md mandates loguru |

**No new dependencies required for any decision.** [VERIFIED: grep pyproject.toml + installed versions]

---

## Q1: Supabase REST Fetch Method (D-01)

[VERIFIED: src/polyarb/storage/supabase_mirror.py + supabase-py Context7 docs]

**Existing pattern:** `SupabaseMirror.__init__` calls `create_client(url, service_key)` → long-lived `Client` instance. All writes use `self._client.table("markets_latest").insert(chunk).execute()`. The same pattern is used in `L2SupabaseMirror` at `storage/l2_supabase_mirror.py`.

**Fetch pattern for D-01:**
```python
resp = self._client.table("markets_latest").select("*").execute()
rows: list[dict] = resp.data  # list of dicts, one per row
```

**Pagination is required.** Supabase's PostgREST default row limit is 1000 rows per request. [CITED: supabase-py Context7 docs — `limit(size)` / `offset(size)` are explicit methods]. Actual market_count from latest known snapshot is **6729** (JOURNAL.md line 1505). With `markets_latest` as a full-overwrite of the latest snapshot, the table will hold ~6000-7000 rows. A single `.select("*").execute()` will return at most 1000 rows without pagination.

**Correct fetch with pagination:**
```python
def _fetch_all_markets_latest(client) -> list[dict]:
    """Fetch all markets_latest rows with pagination (PostgREST default limit=1000)."""
    rows = []
    offset = 0
    page_size = 1000
    while True:
        resp = (
            client.table("markets_latest")
            .select("*")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = resp.data or []
        rows.extend(batch)
        if len(batch) < page_size:
            break  # last page
        offset += page_size
    return rows
```

[VERIFIED: supabase-py `.range(from_, to)` sets PostgREST `Range` header; `.select("*").execute()` without range returns first 1000 rows only — confirmed via Context7 offset/range docs]

**Fail-soft envelope:** When the Supabase fetch raises (network error, Supabase paused/429), `compute_candidates` must catch the exception, log via loguru, surface to /health (new sub-check `candidates:supabase_fetch_status`), and return the previous candidate set unchanged. This is the Phase 03 mirror fail-soft pattern applied to the fetch side.

**Service key required:** The fetch uses the service_role key (same `POLYARB_SUPABASE_SERVICE_KEY`). The `settings.supabase_url` and `settings.supabase_service_key` are already available in `on_snapshot_complete` via `settings` arg.

---

## Q2: Scanner Recipe Column-Dependency Table (D-02 Fail-Loud Basis)

[VERIFIED: src/polyarb/observation/recipes.py + scanner.py full read]

### Run-recipe SQL template (scanner.py:148-155)

```sql
SELECT m.*, qt.question_zh
FROM markets m
LEFT JOIN question_translations qt ON qt.question_en = m.question
WHERE ({recipe.where})
ORDER BY {recipe.order_by}
LIMIT {limit}
```

The `SELECT m.*` means the adapter's temp table MUST have all markets columns present (even as NULL) for the query to not fail on column-not-found. The WHERE and ORDER BY clauses determine which columns must have **meaningful** (non-NULL) values.

### markets_latest narrow projection (current 10 columns)

From `alembic/versions/001_initial_dashboard_schema.py:52-67` + `supabase_mirror.py:31-42`:

| Column | In narrow projection |
|--------|---------------------|
| market_id | YES |
| question | YES |
| slug | YES |
| event_slug | YES (mapped from event_id) |
| mid_price | YES |
| liquidity_usd | YES |
| volume_usd | YES |
| end_time_ms | YES |
| snapshot_id | YES |
| question_zh | YES (translation cache) |

After D-07 adds `yes_token_id`:

| Column | In narrow projection (post D-07) |
|--------|----------------------------------|
| yes_token_id | YES (new, nullable) |

### Recipe Column-Dependency Table

| Recipe | WHERE columns needed | ORDER BY columns needed | Other tables needed | Columns ABSENT from narrow | MUST be NULL-filled | Fail-loud? |
|--------|---------------------|------------------------|---------------------|---------------------------|--------------------|----|
| `thick-but-slippery` | `liquidity_usd`, `best_bid_price`, `best_ask_price` | `liquidity_usd`, `best_bid_price`, `best_ask_price` | None | `best_bid_price`, `best_ask_price` | YES — but recipe will return 0 rows (WHERE always false when NULL) | WARN (silent empty result, not crash) |
| `near-end` | `end_time_ms`, `liquidity_usd` | `liquidity_usd` | None | None | — | Pass |
| `ghost-suspicious` | `market_id`, `liquidity_usd` | `liquidity_usd` | `validation_issues` | `validation_issues` table | YES — table must be created in temp DB (empty) | FAIL-LOUD if table missing |
| `coin-flip` | `mid_price`, `end_time_ms`, `liquidity_usd` | `volume_usd` | None | None | — | Pass |
| `neg-risk-incomplete` | `neg_risk_market_id` | `ABS(SUM(mid_price)-1.0)` (GROUP BY) | None | `neg_risk_market_id` | YES | WARN (returns 0 rows) |
| `by-tag` | None (WHERE 1=1) | `market_count` (from GROUP BY) | `event_tags` table | `event_tags` table | YES — table must exist (empty OK) | FAIL-LOUD if table missing |

**Watchlist path (lines 141-177):** Does NOT use `run_recipe`. Opens the temp DB directly and runs:
```sql
SELECT market_id, yes_token_id, event_id, liquidity_usd, volume_usd FROM markets WHERE slug = ?
```
Required: `yes_token_id` (CRITICAL — used as asset_id), `market_id`, `event_id`, `liquidity_usd`, `volume_usd`, `slug`. **All of these are in narrow projection post D-07.**

**candidate_refresh reads from run_recipe DataFrame (lines 119-130):**
- `row.get("yes_token_id")` — CRITICAL; if NULL/absent, market is skipped (see line 121: `if not yes_tid: continue`)
- `row.get("market_id")` — stored in CandidateRow
- `row.get("event_id")` — stored in CandidateRow
- `row.get("liquidity_usd")` — ranking_score + cap sort
- `row.get("volume_usd")` — ranking_score

### Adapter Construction Rules

The `:memory:` SQLite must include ALL tables the scanner touches:

1. **`markets` table** — full DDL from `schemas.py:105-132` (23 columns). Columns not in narrow projection are created but NULL-filled from adapter.
2. **`question_translations` table** — full DDL from `schemas.py:159-170`. Empty table is correct (LEFT JOIN; no translations available from narrow projection).
3. **`validation_issues` table** — full DDL from `schemas.py:138-149`. Empty table is correct (`ghost-suspicious` will return 0 rows, which is acceptable for candidate selection).
4. **`event_tags` table** — full DDL from `schemas.py:93-102`. Empty table means `by-tag` returns 0 rows (acceptable — grouped recipe, skipped by candidate_refresh per line 113: `if recipe.group_by is not None: continue`).

**Columns in narrow projection that map directly:**

| narrow_market_row column | maps to markets DDL column | Notes |
|--------------------------|---------------------------|-------|
| market_id | market_id | PK |
| question | question | |
| slug | slug | needed by watchlist path |
| event_slug | event_id (via mapping) | mirror uses event_slug for display; adapter should map to event_id |
| mid_price | mid_price | |
| liquidity_usd | liquidity_usd | |
| volume_usd | volume_usd | |
| end_time_ms | end_time_ms | |
| snapshot_id | snapshot_id | |
| question_zh | (no markets column — from JOIN) | inject via question_translations or via extra column in SELECT |
| yes_token_id | yes_token_id | post D-07; critical for watchlist + CandidateRow |

**Columns in markets DDL that MUST be NULL-filled (absent from narrow projection):**

`condition_id`, `no_token_id`, `best_bid_price`, `best_bid_size`, `best_ask_price`, `best_ask_size`, `active`, `closed`, `neg_risk`, `neg_risk_market_id`, `fetched_at_ms`, `page_fetched_at_ms`, `incomplete`

**Fail-loud check at adapter construction time:**

The adapter must assert that only recipes with `group_by is None` are used for candidate selection (already enforced at line 113 in `l2_candidate_refresh.py`). For row-level recipes, the fail-loud check is: if any recipe's WHERE clause references a column that is NULL-filled AND the WHERE clause would be semantically broken (not just return empty results), raise at construction. Practically: `thick-but-slippery` and `neg-risk-incomplete` will silently return 0 rows (WHERE always false / no neg_risk data) — this is acceptable and should be logged as a warning, not a fatal error. **No recipe will CRASH; all will just return fewer rows.** The true fail-loud case is if `validation_issues` or `event_tags` tables are missing (SQL error), which the adapter must prevent by always creating those tables.

---

## Q3: yes_token_id Null Rate (D-07)

[VERIFIED: src/polyarb/snapshot/normalizer.py:107-108]

**Source:** `normalizer.py:107`: `yes_token_id = str(token_list[0]) if len(token_list) > 0 else None`

`token_list` comes from `_parse_json_list(raw.get("clobTokenIds"))`. If the Gamma API returns `clobTokenIds` as a non-empty list (the YES side is index 0), `yes_token_id` is set. If the list is empty or absent, `yes_token_id = None`.

**CONTEXT.md Phase 1 Open Items note:** "top-of-book single-side, 只 yes_token_id populated" — meaning some markets may lack `clobTokenIds[0]` (e.g., binary-resolved or incomplete markets). The normalizer already handles this with `else None`.

**nullable is the correct choice (D-07 confirmed):** Some markets legitimately have no `yes_token_id`. Making it nullable does NOT break existing logic because:
- `candidate_refresh.py:121`: `if not yes_tid: continue` — already guards against NULL/empty at the consumer end.
- Mirror push already writes `None` for absent fields (via `narrow_market_row` which uses `.get(col)` defaulting to `None`).

**Alembic migration pattern:** Follow `003_l2_tables.py` (the most recent migration). Use `op.add_column("markets_latest", sa.Column("yes_token_id", sa.Text, nullable=True))`. No DOWN migration needed per Phase 01.1 P7 (add-only discipline). [VERIFIED: alembic/versions/ directory — 001, 002, 003 all present]

---

## Q4: Throughput Baseline + Measurement (D-05/D-06)

[VERIFIED: src/polyarb/daemon/ws_consumer.py + ws_watchdog.py + Makefile:855]

### Existing instrumentation

| Metric | Where | Access |
|--------|-------|--------|
| `frame_count` | `WsConsumer._frame_count` (int, increments per frame) | `ws_consumer.frame_count` property |
| `last_event_at_s` | `WsConsumer._last_event_at_s` (epoch float, set per frame) | `ws_consumer.last_event_at_s` property |
| `current_state` | `WsWatchdog._state.state` (str) | `ws_consumer.current_state` property (delegates to watchdog) |
| WS connection state | watchdog states: CONNECTED/WAITING_FOR_EVENT/RECONNECTING/DEGRADED_REST_POLLING | `/health` `ws:connection_state` sub-check |

### What must be ADDED for D-06 three indicators

**Indicator 1 — Zero dropped frames:** No dropped-frame counter exists. The WS client is `stream_market_events` (websockets library). Dropped frames would manifest as gaps in `frame_count` vs expected rate, or as the WS library silently failing to enqueue. The practical measurement for this phase: compare `frame_count` before and after the POLYARB_WS_TEST_KILL storm — rate should return to pre-storm level (not drop to zero for extended period). **Add `frame_count_at_start` snapshot + `frame_count_after_recovery` comparison** to the chaos target.

For more rigorous dropped-frame detection, add a `_dropped_frame_count: int` counter to `WsConsumer` that increments when `on_event` callback raises (currently only logs warning at line 158). This is a small addition.

**Indicator 2 — Watchdog no false-trip:** `current_state` surfaces to `/health` as `ws:connection_state`. The storm test sequence is: (1) set `POLYARB_WS_TEST_KILL=1` → WS closes → watchdog should transition to RECONNECTING → reconnects (NOT to DEGRADED_REST_POLLING). A false-trip is watchdog entering RECONNECTING when the WS is healthy (no kill flag set). Measurement: check `/health ws:connection_state` before storm (expect WAITING_FOR_EVENT/CONNECTED), during storm (expect RECONNECTING briefly), after recovery (expect WAITING_FOR_EVENT). Already surfaced via /health — no new code needed.

**Indicator 3 — Memory stable:** Use `psutil.Process().memory_info().rss` before/after expanding candidate set. psutil is already in dev deps (L11 in LEARNINGS). Measurement: `make chaos-l2-inj4-throughput` script should capture RSS at: cold start (3 assets), after candidate expansion (N assets), during storm, after recovery. Look for runaway growth pattern (not just transient increase).

### Baseline-then-threshold approach (D-06)

**Step 1 (Wave 3 plan 1):** Deploy candidate-expanded daemon (D-01/D-02/D-03 done). Observe for 5 minutes with real candidate set:
```
frame_rate_baseline = (frame_count_T2 - frame_count_T1) / (T2 - T1)  # msg/s
rss_baseline = psutil RSS at T1
```

**Step 2 (Wave 3 plan 2):** Run inj4 storm. Pass criteria:
- `frame_rate_after_recovery >= frame_rate_baseline * 0.90` (90% of baseline = "zero dropped frames" operationally)
- `watchdog.current_state` returns to WAITING_FOR_EVENT within 60s of storm end
- `rss_T_end <= rss_baseline * 1.30` (memory within 30% of pre-storm baseline)

**Watchdog false-trip detection:** Stale_s is locked at 30.0s (D-03 LOCKED in `ws_watchdog.py:74`). With N candidates subscribed, the WS should receive frames more frequently as N grows (more markets = more price changes). False-trip risk is that `on_event` callback blocks for >30s. The `on_event` callback in `l2_main.py:266-279` does a synchronous Supabase REST push. If Supabase is slow (>30s), the watchdog CAN false-trip. This is a real risk to document in the plan.

### Realistic candidate-set size

From Phase 03.1 CONTEXT.md D-04 + SOAK-LOG: "3-asset bootstrap is small enough that WS storm is really WS close + reconnect (no genuine storm rate)." With near-end recipe (markets ending in 72h, `liquidity_usd > 1000`):
- Total markets: ~6729 (confirmed from JOURNAL.md)
- Near-end 72h subset: varies by calendar. High-activity periods (soccer tournament, elections) may produce 50-300 markets. Stable periods may produce 20-80. Estimate: **30-200 near-end markets** at any given time.
- After `thick-but-slippery` + `coin-flip` contributions: cap of 500 will rarely bind.

**Practical implication:** The storm test with 30-200 subscribed assets is a genuine WS throughput test (vs the trivial 3-asset prior test). Polymarket WS delivers price_change events for each subscribed asset on any trade — 100 subscribed assets during active trading hours can generate 10-50 msg/s. [ASSUMED — no Polymarket WS throughput public documentation found; estimate based on IMDEA paper 86M trades / deployment period]

---

## Q5: on_snapshot_complete Integration Point (D-01)

[VERIFIED: src/polyarb/observation/l2_candidate_refresh.py:83-291 + l2_main.py:305-320]

### Current data flow

```
L2 daemon l2_main.py:
  _dispatch_on_snapshot(payload) [sync bridge, line 305-318]
    → asyncio.create_task(on_snapshot_complete(payload, ws_consumer, settings, mirror))

on_snapshot_complete(payload, ...):  [l2_candidate_refresh.py:213]
  → debounce check (line 240-247)
  → compute_candidates(settings, scanner_yaml, watchlist_yaml)
      → db_path = Path(settings.db_path)          [line 104] ← CHANGE HERE
      → scanner recipes run against db_path
      → watchlist slug lookup against db_path
  → ws_consumer._subscribed_assets = list(new_asset_ids)
```

### NOTIFY payload content

[VERIFIED: l2_main.py + CONTEXT.md Integration Points note]: "payload 当前只带 snapshot_id，不带 markets 数据，必须 REST roundtrip." The payload carries `snapshot_id` and `taken_at_ms` (from existing test fixtures at `test_l2_candidate_refresh.py:337-339`).

**Key: The NOTIFY payload does NOT carry markets data.** D-01 must always do a Supabase REST round-trip.

### Exact modification point

`l2_candidate_refresh.py:compute_candidates` signature currently accepts `settings: Any` and uses `settings.db_path`. The D-01 change adds an optional `supabase_client` parameter (or a pre-fetched rows list). The cleanest approach:

```python
def compute_candidates(
    settings: Any,
    scanner_yaml: Path | None = None,
    watchlist_yaml: Path | None = None,
    markets_rows: list[dict] | None = None,  # D-01: pre-fetched from Supabase
) -> list[CandidateRow]:
    if markets_rows is not None:
        db_path = _build_temp_db(markets_rows)  # :memory: adapter
    else:
        db_path = Path(settings.db_path)  # D-04 fallback / bootstrap
```

The Supabase fetch itself goes into `on_snapshot_complete`, which already has access to `settings` (url + key). The fail-soft envelope wraps the fetch and passes the last known `markets_rows` on failure.

### Fail-soft envelope for Supabase fetch failure

```python
# Module-level last-known rows (fail-soft state)
_last_known_markets_rows: list[dict] | None = None

async def on_snapshot_complete(payload, *, ws_consumer, settings, mirror=None):
    global _last_known_markets_rows
    # ... debounce check ...
    
    # D-01 Supabase fetch (fail-soft)
    markets_rows = None
    if settings.supabase_url and settings.supabase_service_key.get_secret_value():
        try:
            client = create_client(settings.supabase_url, settings.supabase_service_key.get_secret_value())
            markets_rows = _fetch_all_markets_latest(client)
            _last_known_markets_rows = markets_rows
        except Exception as e:
            logger.error(f"candidate refresh: supabase fetch failed: {e!r}")
            markets_rows = _last_known_markets_rows  # use last known
            # TODO: surface to /health as candidates:supabase_fetch_age sub-check
    
    new_rows = compute_candidates(settings, ..., markets_rows=markets_rows)
```

---

## Q6: GAP-200 Settings Logic (D-08)

[VERIFIED: src/polyarb/config.py:238-240 + l2_health.py:180]

### Current logic in config.py model_validator (lines 238-240)

```python
if self.supabase_url and self.supabase_service_key.get_secret_value():
    object.__setattr__(self, "supabase_mirror_enabled", True)
    object.__setattr__(self, "l2_mirror_enabled", True)
```

Both flags are False when either field is empty. There is NO current distinction between:
- (a) `supabase_url = ""` — Supabase not configured at all
- (b) `supabase_url = "https://..."` but `supabase_service_key = ""` — URL configured, key missing

### Current /health gate (l2_health.py:180)

```python
if getattr(settings, "l2_mirror_enabled", False):
    # ... full sub-check wiring ...
```

When `l2_mirror_enabled = False` (either case a or b), the `mirror:l2_tob_age_seconds` sub-check is entirely absent from `.checks`.

### D-08 Fix Plan

**Step 1 — No config.py change needed.** `l2_mirror_enabled` stays False in case (b) — the mirror IS disabled. The change is in how /health PRESENTS this.

**Step 2 — l2_health.py:180 change:**

Replace the binary `if l2_mirror_enabled:` with three-branch logic:

```python
_supabase_url = getattr(settings, "supabase_url", "")
_service_key_val = ""
try:
    _service_key_val = settings.supabase_service_key.get_secret_value()
except AttributeError:
    pass

if _supabase_url and not _service_key_val:
    # Case (b): url configured but service_key missing — config mistake, surface as fail
    checks["mirror:l2_tob_age_seconds"] = [{
        "componentId": "supabase-l2-mirror",
        "componentType": "datastore",
        "observedValue": None,
        "status": "fail",
        "output": "mirror disabled by config (service_key empty)",
        "time": _utc_now_iso(),
    }]
    overall = _severity(overall, "fail")
elif getattr(settings, "l2_mirror_enabled", False):
    # Case (c): both url + key set — existing full sub-check logic
    # ... (existing lines 181-216 unchanged) ...
# else: case (a) url also empty — no sub-check (correct, Supabase not configured)
```

This preserves the chain-truth principle: a config-disable due to an operator mistake (forgot to set key) is observable via /health, not silent.

---

## Architecture Patterns

### Recommended Project Structure for Phase 04 additions

```
src/polyarb/observation/
├── l2_candidate_refresh.py     # MODIFY: add Supabase fetch + :memory: DB path
├── l2_temp_db.py               # NEW: :memory: adapter (build_temp_db function)
│                               # Keeps compute_candidates lean; adapter is testable standalone

src/polyarb/http/
├── l2_health.py                # MODIFY: D-08 three-branch mirror gate (line 180)

alembic/versions/
├── 004_add_yes_token_id.py     # NEW: D-07 add-only migration

tests/observation/
├── test_l2_candidate_refresh.py  # MODIFY: add Supabase fetch integration tests
├── test_l2_temp_db.py          # NEW: adapter unit tests + fail-loud tests

tests/alembic/
├── test_004.py                 # NEW: migration idempotency test

tests/http/
├── test_l2_health_gap200.py    # NEW: three-branch D-08 tests
```

### Pattern: :memory: SQLite Adapter

[VERIFIED: tests/observation/test_l2_candidate_refresh.py:22-75 — existing test uses `sqlite3.connect(db_path)` with the full markets DDL]

The test fixture `_create_minimal_sqlite` provides the reference schema. The adapter for `:memory:` follows the same DDL:

```python
import sqlite3
from pathlib import Path
from polyarb.storage.schemas import DDL  # authoritative DDL source

def build_temp_db(markets_rows: list[dict]) -> Path:
    """Build :memory: SQLite populated from Supabase markets_latest rows.
    
    Returns a pathlib.Path(":memory:") that run_recipe can open via
    sqlite3 URI: "file::memory:?mode=memory&cache=shared"
    
    IMPORTANT: sqlite3 :memory: databases are connection-scoped. To share
    the populated DB with run_recipe's internal sqlite3.connect(), use the
    shared-cache URI pattern or write to a named temp file.
    """
```

**Critical implementation note:** `:memory:` SQLite databases are destroyed when the connection closes. `run_recipe` creates its own `sqlite3.connect(uri, uri=True)` connection (scanner.py:143). If the adapter writes to `:memory:` on connection A and run_recipe opens `:memory:` on connection B, B gets an empty DB.

**Solution options:**
1. **Named temp file** (`tempfile.NamedTemporaryFile(suffix='.db', delete=False)`) — simplest, safest; delete after candidates computed.
2. **Shared cache URI** (`file:polyarb_temp?mode=memory&cache=shared`) — works within same process, no disk I/O. Requires all connections to use the same URI string.
3. **Refactor run_recipe to accept connection** — larger change, rejected (D-01 says "scanner SQL 原封不动跑").

**Recommended: Named temp file.** Avoids shared-cache URI complexity. The temp file lifecycle is scoped to `compute_candidates` call:
```python
import tempfile, os

with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
    tmp_path = Path(f.name)
try:
    _populate_temp_db(tmp_path, markets_rows)
    result = compute_candidates_from_path(tmp_path, ...)
finally:
    os.unlink(tmp_path)  # cleanup
```

[VERIFIED: `run_recipe` at scanner.py:142 uses `uri = f"file:{db_path}?mode=ro"` — requires db_path to be a real path, not `:memory:`]

### Pattern: Fail-Loud Recipe Column Validation

```python
# At adapter construction time (not at recipe run time):
_NARROW_COLUMNS_POST_D07 = frozenset([
    "market_id", "question", "slug", "event_slug", "mid_price", 
    "liquidity_usd", "volume_usd", "end_time_ms", "snapshot_id",
    "question_zh", "yes_token_id",  # D-07 adds this
])

def validate_recipe_columns(recipe: Recipe) -> None:
    """Fail-loud if recipe uses columns unavailable in temp DB.
    
    For Phase 04, only best_bid_price, best_ask_price, best_bid_size,
    best_ask_size, neg_risk_market_id, neg_risk, condition_id, no_token_id,
    active, closed, incomplete, fetched_at_ms will be NULL-filled.
    
    Recipes that reference NULL-filled columns will return 0 rows (not crash),
    which is acceptable — log as WARNING so operator knows.
    """
    NULL_FILLED_COLS = frozenset([
        "best_bid_price", "best_ask_price", "best_bid_size", "best_ask_size",
        "neg_risk_market_id", "neg_risk", "condition_id", "no_token_id",
        "active", "closed", "incomplete", "fetched_at_ms", "page_fetched_at_ms",
    ])
    text = recipe.where + " " + recipe.order_by
    for col in NULL_FILLED_COLS:
        if col in text:
            logger.warning(
                f"recipe {recipe.name!r} uses NULL-filled column {col!r} — "
                f"will return 0 rows from temp DB (expected: markets_latest has no {col!r})"
            )
    # Hard fail only if tables are missing — those would cause SQL errors, not 0-row results
    # (handled by always including all required tables in adapter DDL)
```

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Supabase REST pagination | Custom HTTP range headers | `supabase-py .range(offset, offset+999).execute()` | SDK handles PostgREST Range header; already in project |
| Temp file cleanup | Manual tracking + try/finally | `tempfile.NamedTemporaryFile(delete=False)` + `os.unlink` in finally | Safe cleanup even on exception |
| Recipe SQL parsing | Regex column extraction | Trust scanner DDL + known narrow columns; compare sets | SQL parsing is complex; column-set diff is O(N) and reliable |
| WS frame-rate measurement | Prometheus metrics | `WsConsumer.frame_count` (already exists) + timestamps | Already instrumented; adding a metric library is out of scope |

---

## Common Pitfalls

### Pitfall 1: :memory: SQLite is connection-scoped

**What goes wrong:** `build_temp_db` writes to `sqlite3.connect(":memory:")`. `run_recipe` opens `sqlite3.connect("file::memory:?mode=ro", uri=True)` — gets a DIFFERENT empty database. Scanner returns 0 rows. No error raised.

**Why it happens:** SQLite `:memory:` databases are per-connection by default. Two connections to `:memory:` are two independent databases.

**How to avoid:** Use `tempfile.NamedTemporaryFile(suffix='.db')` for the temp DB. Scanner opens it via `file:{tmp_path}?mode=ro` URI. File is deleted after `compute_candidates` returns.

**Warning signs:** compute_candidates returns 0 candidates even though markets_latest has rows; no error in logs.

### Pitfall 2: PostgREST 1000-row default limit silently truncates

**What goes wrong:** `.select("*").execute()` returns exactly 1000 rows. markets_latest has 6729. Candidate set uses only first 1000, missing all markets with market_id > alphabetic-sort position 1000. No error raised.

**Why it happens:** PostgREST silently enforces a default max-rows limit. supabase-py does not warn.

**How to avoid:** Always use `.range(offset, offset+999)` pagination loop. Check `len(batch) < page_size` as loop termination condition.

**Warning signs:** candidate set has exactly 500 rows (capped) even though near-end markets should be fewer; frame_count stays at bootstrap-equivalent rate.

### Pitfall 3: D-07 Alembic migration assumes service_role auth for DDL

**What goes wrong:** `alembic upgrade head` runs against `POLYARB_SUPABASE_DB_DSN` (postgres:// DSN, not REST URL). If the DSN uses a restricted role, `ALTER TABLE markets_latest ADD COLUMN` may fail with permissions error.

**Why it happens:** Supabase's `postgres` role has full DDL access; other roles may not.

**How to avoid:** Run migration with the same DSN used in existing migrations (001-003). Verify the role via `psql $POLYARB_SUPABASE_DB_DSN -c "SELECT current_user"` before running.

### Pitfall 4: Watchdog false-trip during Supabase push

**What goes wrong:** `on_event` callback in `l2_main.py:272-279` calls `l2_mirror.push_top_of_book(...)` synchronously. With N candidates, frame rate is higher. Supabase Free tier may throttle at high request frequency. If `push_top_of_book` blocks >30s (Supabase paused or slow), watchdog fires `_on_stale()` and enters RECONNECTING — a false-trip.

**Why it happens:** Watchdog timer is not paused during synchronous Supabase I/O. The `on_event` callback runs inside the async for loop, but `push_top_of_book` is blocking (supabase-py is sync).

**How to avoid:** Monitor watchdog state during baseline measurement. If false-trips appear, the plan must add async I/O wrapping for Supabase calls or increase `stale_s` (the latter requires a D-XX amendment as `stale_s=30.0` is locked). Document in throughput test results.

### Pitfall 5: FLY_API_TOKEN shadowing during prod chaos

**What goes wrong:** Running `flyctl secrets set POLYARB_WS_TEST_KILL=1 -a polyarb-l2` fails with wrong app or auth error because `FLY_API_TOKEN` in `.env` points to L1-only token.

**Why it happens:** `.env` loading shadows the keychain credential (Phase 03.1 L3 + GAP-201).

**How to avoid:** Per `feedback_fly-api-token-shadowing-2026-05.md` — always prefix flyctl with `FLY_API_TOKEN=` to clear the env var: `FLY_API_TOKEN= flyctl secrets set ...`. Pattern already established in Makefile at line 861.

---

## Runtime State Inventory

> Phase 04 is NOT a rename/refactor phase. No runtime state migration required.

| Category | Items Found | Action Required |
|----------|-------------|-----------------|
| Stored data | markets_latest (Supabase Postgres) — all current rows | D-07 adds column (Alembic migration), existing rows get NULL for yes_token_id — correct |
| Live service config | polyarb-l2 Fly secrets (POLYARB_SUPABASE_URL, POLYARB_SUPABASE_SERVICE_KEY) — already set | No change needed for D-01 fetch path (same credentials) |
| OS-registered state | None — verified by grep JOURNAL + Makefile | — |
| Secrets/env vars | No new secrets needed; existing supabase_url + service_key used | None |
| Build artifacts | None — no compiled artifacts | — |

---

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| supabase-py | D-01 Supabase REST fetch | Yes | 2.30.0 | — |
| sqlite3 (stdlib) | D-02 temp DB | Yes | bundled | — |
| alembic | D-07 migration | Yes | project dep | — |
| psutil | D-06 memory measurement | Yes (dev extras) | project dev dep | `uv sync --extra dev` |
| flyctl | D-05 prod chaos | Yes | checked via Makefile | — |
| psql | D-05 Supabase row count verification | Assumed present | — | Use Supabase dashboard UI |
| uv | test runner | Yes | project standard | — |

**Note on psutil:** As per Phase 03.1 L11, `uv sync --extra dev` is required. Plain `uv sync` does not install psutil.

---

## Validation Architecture

> nyquist_validation not explicitly set to false in .planning/config.json (key absent) — section included.

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest + pytest-asyncio |
| Config file | pyproject.toml (`[tool.pytest.ini_options]`) |
| Quick run command | `uv run pytest tests/observation/test_l2_temp_db.py tests/observation/test_l2_candidate_refresh.py -xvs` |
| Full suite command | `uv run pytest tests/ -v` |

### Phase Requirements → Test Map

| Decision | Behavior | Test Type | Automated Command | File Exists? |
|----------|----------|-----------|-------------------|-------------|
| D-01 Supabase fetch | fetch_all_markets_latest paginates correctly (>1000 rows) | unit (mock supabase client) | `pytest tests/observation/test_l2_temp_db.py::test_fetch_pagination -xvs` | ❌ Wave 0 |
| D-01 fail-soft | Supabase fetch failure → last known rows used, no crash | unit | `pytest tests/observation/test_l2_candidate_refresh.py::test_supabase_fetch_fail_uses_last_known -xvs` | ❌ Wave 0 |
| D-02 adapter | temp DB contains all required tables + correct column schema | unit | `pytest tests/observation/test_l2_temp_db.py::test_build_temp_db_schema -xvs` | ❌ Wave 0 |
| D-02 fail-loud warn | recipe referencing NULL-filled column logs WARNING, does not crash | unit | `pytest tests/observation/test_l2_temp_db.py::test_null_filled_column_warns -xvs` | ❌ Wave 0 |
| D-02 ghost-suspicious | validation_issues table in temp DB allows ghost recipe to run (0 rows, no error) | unit | `pytest tests/observation/test_l2_temp_db.py::test_ghost_suspicious_empty_validation_issues -xvs` | ❌ Wave 0 |
| D-03 near-end recipe | near-end recipe selects correct markets from temp DB | unit | `pytest tests/observation/test_l2_candidate_refresh.py::test_near_end_from_supabase_rows -xvs` | ❌ Wave 0 |
| D-04 bootstrap | bootstrap_asset_ids still drives WS before first NOTIFY | existing | `pytest tests/daemon/test_l2_main_startup.py -xvs` | ✅ exists |
| D-05 throughput | frame_count increases after candidate expansion (integration) | smoke (prod) | `make chaos-l2-inj4-throughput` | ❌ Wave 0 (Makefile target) |
| D-06 watchdog | watchdog state != RECONNECTING during healthy 30s window after storm | smoke (prod) | part of chaos-l2-inj4-throughput | — |
| D-07 migration | yes_token_id column exists in markets_latest after migration | unit | `pytest tests/alembic/test_004.py::test_004_up -xvs` | ❌ Wave 0 |
| D-07 narrow | narrow_market_row includes yes_token_id in output dict | unit | `pytest tests/storage/test_supabase_mirror.py::test_narrow_includes_yes_token_id -xvs` | ❌ Wave 0 |
| D-08 url-no-key | /health registers mirror:l2_tob_age_seconds with status=fail when url set + key empty | unit | `pytest tests/http/test_l2_health_gap200.py::test_url_set_key_empty_registers_fail -xvs` | ❌ Wave 0 |
| D-08 both-empty | /health has NO mirror sub-check when url also empty | unit | `pytest tests/http/test_l2_health_gap200.py::test_both_empty_no_subcheck -xvs` | ❌ Wave 0 |
| D-08 full | /health registers mirror sub-check normally when both url+key set | existing | `pytest tests/http/ -xvs` | ✅ (modify existing) |

### Sampling Rate
- **Per task commit:** `uv run pytest tests/observation/ tests/http/ -x` (target files only, < 30s)
- **Per wave merge:** `uv run pytest tests/ -v` (full suite)
- **Phase gate:** Full suite green before `/gsd-verify-work`

### Wave 0 Gaps

- [ ] `tests/observation/test_l2_temp_db.py` — covers D-01 pagination, D-02 adapter schema, D-02 fail-loud, D-02 ghost-suspicious, D-03 near-end
- [ ] `tests/alembic/test_004.py` — covers D-07 migration idempotency
- [ ] `tests/http/test_l2_health_gap200.py` — covers D-08 three-branch logic
- [ ] Makefile `chaos-l2-inj4-throughput` target — D-05/D-06 prod chaos with frame_count + RSS measurement
- [ ] Modify `tests/storage/test_supabase_mirror.py` (or create) for D-07 `narrow_market_row` includes `yes_token_id`

---

## Security Domain

> Security enforcement not explicitly disabled.

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | No | No new auth paths |
| V3 Session Management | No | No sessions |
| V4 Access Control | Partial | Service-role key already scoped correctly; no new exposure |
| V5 Input Validation | Yes | markets_latest rows are trusted (own Supabase, service_role); still filter via narrow column projection |
| V6 Cryptography | No | No new crypto |

### Known Threat Patterns

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Supabase rows with injected SQL via `slug` or `question` fields | Tampering | `run_recipe` uses parameterized queries for watchlist slug lookup (line 148: `WHERE slug = ?`); recipe SQL uses scanner's read-only mode |
| Temp DB left on disk if process crashes in finally block | Info Disclosure | File contains only market metadata (public data); no secrets. Use `try/finally os.unlink` |
| yes_token_id as uint256 string overflow | Tampering | Already handled — normalizer converts to str (Pitfall 3 in RESEARCH); temp DB stores as TEXT |

---

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| compute_candidates reads L2 local SQLite | Will read Supabase markets_latest + temp DB | Phase 04 | Enables real candidate set (6000+ markets vs 0) |
| 3 bootstrap_asset_ids as full candidate set | bootstrap_asset_ids = cold-start fallback only | Phase 04 | WS subscription expands from 3 to 30-200 assets |
| /health drops mirror sub-check when service_key empty | /health registers status=fail sub-check when url set + key empty | Phase 04 | GAP-200 chain-truth closed |
| markets_latest has no yes_token_id | markets_latest.yes_token_id (nullable) | Phase 04 (D-07 migration) | Watchlist slug lookup via temp DB can use yes_token_id |

---

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | Polymarket WS delivers 10-50 msg/s with 100 subscribed assets during active hours | Q4 Throughput | If much higher, Supabase push blocking may cause widespread false-trip watchdog; if much lower, throughput test is still valid but less informative |
| A2 | near-end recipe (72h, liq>1000) returns 30-200 markets at any given time | Q4 Throughput | If returns 0 (e.g., all markets > 72h away), throughput test degrades to bootstrap-equivalent — may need to adjust recipe window or use different recipe for test |
| A3 | psql CLI available in the test environment for Supabase row count verification | Environment | If absent, use Supabase dashboard or supabase-py count query instead |
| A4 | Polymarket `clobTokenIds[0]` is absent for < 5% of active markets | Q3 yes_token_id | If higher, nullable yes_token_id in markets_latest means more candidates skipped — acceptable, just log the rate |

**If A1 table is empty:** All other claims are code-verified. Only A1-A4 are assumptions.

---

## Open Questions (RESOLVED)

1. **Should Supabase fetch happen inside compute_candidates or in on_snapshot_complete?**
   - What we know: on_snapshot_complete is async; compute_candidates is sync. Supabase-py is sync.
   - RESOLVED: Keep supabase-py sync inside `on_snapshot_complete` (runs in asyncio via `create_task`). Bounded blocking (~100-500ms for 6000 rows paginated). Adopted by Plan 02 Task 2. Revisit only if bottleneck in Phase 05.

2. **Named temp file cleanup on process crash?**
   - What we know: `os.unlink` in `finally` covers normal + exception paths. SIGKILL skips finally.
   - RESOLVED: `os.unlink` in `finally` for normal/exception paths; Fly.io ephemeral containers clean /tmp on restart, so SIGKILL leftover is acceptable risk. Adopted by Plan 02 Task 1.

3. **New /health sub-check for Supabase fetch status (D-01 fail-soft surface)?**
   - CONTEXT.md specifies fail-soft for the fetch but does not specify /health surface.
   - RESOLVED: YES — Plan 02 Task 3 implements `candidates:supabase_fetch_age_seconds` sub-check (mirrors `mirror:l2_tob_age_seconds`), recording last successful fetch via local SQLite singleton. This is chain-truth §1.6 applied to the new fetch path — IN Phase 04 scope.

---

## Sources

### Primary (HIGH confidence)
- `[VERIFIED: src/polyarb/observation/l2_candidate_refresh.py]` — compute_candidates full logic, on_snapshot_complete integration point, watchlist SQL
- `[VERIFIED: src/polyarb/observation/scanner.py]` — run_recipe SQL template, trusted/untrusted path
- `[VERIFIED: src/polyarb/observation/recipes.py]` — BUILTIN_RECIPES WHERE/ORDER BY/group_by columns
- `[VERIFIED: src/polyarb/storage/schemas.py]` — markets DDL (23 columns), all auxiliary table DDLs
- `[VERIFIED: src/polyarb/storage/supabase_mirror.py]` — narrow 10-column projection, create_client pattern
- `[VERIFIED: src/polyarb/config.py]` — l2_mirror_enabled auto-detect logic, supabase_url/service_key fields
- `[VERIFIED: src/polyarb/http/l2_health.py:174-216]` — mirror sub-check gate, D-08 change point
- `[VERIFIED: src/polyarb/daemon/ws_consumer.py]` — frame_count, last_event_at_s, subscribed_assets
- `[VERIFIED: src/polyarb/daemon/ws_watchdog.py]` — stale_s=30.0 LOCKED, reconnect state machine
- `[VERIFIED: alembic/versions/001_initial_dashboard_schema.py:52-67]` — markets_latest 10-column schema
- `[VERIFIED: tests/observation/test_l2_candidate_refresh.py]` — existing test patterns + minimal SQLite fixture
- `[VERIFIED: Makefile:855-888]` — chaos-l2-inj4 target, FLY_API_TOKEN discipline
- `[CITED: supabase-py Context7 docs /supabase/supabase-py]` — .range() pagination, .select("*"), .execute() pattern

### Secondary (MEDIUM confidence)
- `[VERIFIED: .planning/JOURNAL.md:1505]` — market_count=6729 from production snapshot (confirms pagination required)
- `[VERIFIED: src/polyarb/snapshot/normalizer.py:107-108]` — yes_token_id sourced from clobTokenIds[0]
- `[VERIFIED: pyproject.toml + uv installed version]` — supabase 2.30.0 installed

### Tertiary (LOW confidence / ASSUMED)
- A1: WS msg/s estimate with 100+ subscribed assets (no public Polymarket WS throughput docs found)

---

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — all libraries already in project, versions verified
- Architecture (D-01/D-02): HIGH — exact code paths traced to line numbers
- Column-dependency table: HIGH — built from exhaustive recipe source + DDL read
- Supabase fetch pagination: HIGH — verified via Context7 + known market_count=6729
- :memory: vs named temp file: HIGH — verified sqlite3 connection-scope behavior
- D-08 /health logic: HIGH — config.py validator + l2_health.py gate both read
- Throughput msg/s estimate: LOW — assumed based on IMDEA paper volume

**Research date:** 2026-05-28
**Valid until:** 2026-06-28 (stable stack, 30-day window)
