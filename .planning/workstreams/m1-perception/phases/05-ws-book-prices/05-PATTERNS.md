# Phase 05: WS /book + /prices (L2→L3 升级) - Pattern Map

**Mapped:** 2026-06-01
**Files analyzed:** 13 (new + modified)
**Analogs found:** 13 / 13 (exact or role-match for every file)

---

## File Classification

| New/Modified File | New/Mod | Role | Data Flow | Closest Analog | Match Quality |
|---|---|---|---|---|---|
| `alembic/versions/005_l2_book_levels_and_ohlc.py` | NEW | migration | DDL + view-on-base | `alembic/versions/003_l2_tables.py` | **exact** (sibling migration) |
| `src/polyarb/daemon/l2_main.py` (mod) | MOD | daemon entry / dispatcher | request-response (event-driven dispatch) | self (existing `_tob_row_from_frame`) | **exact** (add sibling projector + dispatcher branch) |
| `src/polyarb/storage/l2_supabase_mirror.py` (mod) | MOD | service / writer | CRUD (REST bulk insert) | self (existing `push_top_of_book`) | **exact** (add sibling method) |
| `src/polyarb/clients/ws_market_client.py` (mod) | MOD | client | streaming + control plane (send-after-connect) | self (existing `stream_market_events`) | **exact** (extend on_connect hook surface) |
| `src/polyarb/daemon/ws_consumer.py` (mod) | MOD | consumer | streaming + control plane | self (existing `_stash_ws`/`_liveness_check` + GAP-401 pattern) | **exact** (add `add_subscriptions`/`remove_subscriptions` methods) |
| `src/polyarb/observation/l3_promote.py` | NEW | service / task | batch (5-min cron) + side-effects (WS sub-diff, mirror) | `src/polyarb/observation/l2_candidate_refresh.py` | **exact** (sibling debounced refresh) |
| `src/polyarb/scan_recipes/l3-promote.yaml` (new dir) | NEW | config | declarative SQL recipe | `config/scan_recipes.yaml` (`my-watchtower`) | role-match (yaml schema identical; new dir per RESEARCH §Recommended Project Structure) |
| `src/polyarb/http/l2_health.py` (mod) | MOD | health endpoint | request-response (read-only state surfacing) | self (existing `mirror:l2_tob_age_seconds` three-branch + `candidates:supabase_fetch_age_seconds`) | **exact** (add 3 L3 sub-checks) |
| `dashboard/app/l3/[asset_id]/page.tsx` | NEW | RSC page | request-response (server-fetch + render) | `dashboard/app/asset/[id]/tob/page.tsx` | **exact** (sibling dynamic asset page) |
| `dashboard/app/l3/[asset_id]/KlineChart.tsx` | NEW | client component | client-only canvas render | (no existing chart component) | **none** — use RESEARCH Example 6 (lightweight-charts v5 dynamic import) |
| `dashboard/lib/supabase/l2-queries.ts` (mod) | MOD | query helper | CRUD (anon SELECT) | self (existing `getTopOfBookForAsset`) | **exact** (add `getOhlcForAsset` + `getBookLevelsLatest`) |
| `dashboard/app/candidates/page.tsx` (mod) | MOD | RSC page | request-response | self (existing column rendering) | **exact** (add `l3_promoted_at_ts` badge column) |
| `Makefile` (mod) | MOD | config | command entry-points | self (existing `scan-*` / `chaos-*` targets) | **exact** (add `l3-promote-dry-run` / `ohlc-spot-check` / `smoke-l3-dashboard` / `supabase-migrate-test`) |

---

## Pattern Assignments

### 1. `alembic/versions/005_l2_book_levels_and_ohlc.py` (migration)

**Analog:** `alembic/versions/003_l2_tables.py` (sibling — 2026-05-25, last migration in Phase 03)
**Cross-ref:** `alembic/versions/004_add_yes_token_id.py` (immediate predecessor, demonstrates additive ALTER pattern for Pitfall 8 / D-08 — `l2_candidates.l3_promoted_at_ts` column)

**Imports + module headers** (`003_l2_tables.py:53-62`):
```python
from __future__ import annotations
import sqlalchemy as sa
from alembic import op

revision = "003"
down_revision = "002"
branch_labels = None
depends_on = None
```
→ For 005, mirror verbatim with `revision = "005"`, `down_revision = "004"`.

**Table creation + composite index + BRIN pattern** (`003_l2_tables.py:90-107`, the l2_top_of_book block):
```python
op.create_table("l2_top_of_book",
    sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
    sa.Column("asset_id", sa.Text, nullable=False),
    sa.Column("ts", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
    ... narrow columns ...
)
op.create_index("idx_l2_tob_asset_ts", "l2_top_of_book", ["asset_id", "ts"])
op.execute("CREATE INDEX idx_l2_tob_ts_brin ON l2_top_of_book USING BRIN (ts);")
```
→ For `l2_book_levels`: same shape; surrogate `id BIGSERIAL PRIMARY KEY` + `UniqueConstraint("asset_id","ts","side","level")` (RESEARCH Stack §Alternatives Considered chose surrogate-with-UNIQUE over composite PK to match l2_top_of_book/l2_trades style) + same btree on `(asset_id, ts)` + same BRIN `ts` raw-SQL.

**RLS anon-read pattern** (`003_l2_tables.py:153-165`):
```python
op.execute("ALTER TABLE l2_top_of_book ENABLE ROW LEVEL SECURITY;")
op.execute("CREATE POLICY anon_read ON l2_top_of_book FOR SELECT USING (true);")
```
→ 005 must apply identical RLS to `l2_book_levels`. For the 3 OHLC views, RESEARCH Example 1 lines 729-731 prescribes explicit `GRANT SELECT ON l2_ohlc_<g> TO anon;` (views don't inherit RLS the same way base tables do).

**Additive-column pattern** (`004_add_yes_token_id.py`, the only Phase 04 alembic):
→ The `l2_candidates.l3_promoted_at_ts TIMESTAMPTZ NULL` column add (Pitfall 8 Option C) uses `op.add_column(...)` + `op.create_index(...)`, same as 004's yes_token_id pattern. **Strictly add-only — never DROP an existing column** (`003_l2_tables.py:25-30` schema discipline).

**Downgrade reverse-order rule** (`003_l2_tables.py:168-174`): reverse the create order (children → parents). For 005: drop views → drop l2_candidates index → drop l2_candidates column → drop l2_book_levels.

**Critical correction (CONTEXT D-03 + RESEARCH Pitfall 1):** Use `date_trunc('minute', ts)` for OHLC views — `time_bucket` is TimescaleDB, not available on Supabase PG17. Full view SQL in RESEARCH §Example 1 (lines 679-725).

---

### 2. `src/polyarb/daemon/l2_main.py` (modify — projector + dispatcher branch)

**Analog:** self — existing `_tob_row_from_frame` (lines 97-145) and `_trade_row_from_frame` (lines 148-181) projectors, plus the `_on_event` dispatcher (lines 260-279).

**Projector signature pattern** (`l2_main.py:97-105`):
```python
def _tob_row_from_frame(frame: dict) -> dict | None:
    """Project a price_change / best_bid_ask / book frame to a l2_top_of_book row.

    Returns None if the frame lacks an asset_id (cannot index).
    """
    asset_id = frame.get("asset_id")
    if not asset_id:
        return None
    et = frame.get("event_type", "unknown")
    ...
```
→ New `_book_levels_rows_from_frame(frame: dict, max_levels: int = 10) -> list[dict]` (RESEARCH §Example 2 lines 754-793). Returns `[]` (not `None`) for empty, since dispatcher will iterate.

**Side normalization pattern** — `_trade_row_from_frame` upper-cases `side` (`l2_main.py:177`); new projector maps `bids → "BUY"`, `asks → "SELL"` (consistent with l2_trades.side).

**Timestamp normalization** — reuse existing `_isoformat_ts` helper (`l2_main.py:81-94`) verbatim. Do not duplicate.

**Dispatcher branch pattern** (`l2_main.py:260-279`):
```python
def _on_event(frame: dict) -> None:
    event_type = frame.get("event_type", "unknown")
    asset_id_raw = frame.get("asset_id") or ""
    logger.debug(f"ws frame type={event_type} asset={asset_id_raw[:16]}")
    if l2_mirror is None:
        return
    if event_type in ("price_change", "best_bid_ask", "book"):
        row = _tob_row_from_frame(frame)
        if row is not None:
            l2_mirror.push_top_of_book([row])
    elif event_type == "last_trade_price":
        ...
```
→ Phase 05 adds, inside the `if event_type ... book` branch and after the TOB push, an **L3 gate** condition `if event_type == "book" and asset_id_raw in l3_promote.get_l3_active_set():` → `rows = _book_levels_rows_from_frame(frame); if rows: l2_mirror.push_book_levels(rows)`. Per RESEARCH Pattern 2 — keep TOB write path untouched, only ADD the L3 depth path.

**Promoter task wiring** — model after Plan 05 listener_task pattern (`l2_main.py:439-456`): wrap `l3_promote.run_periodic(stop_event, ...)` in an async function, `asyncio.create_task(...)`, append to the shutdown task list (lines 474-489) so F-04 bounded shutdown (5s timeout) applies to it.

---

### 3. `src/polyarb/storage/l2_supabase_mirror.py` (modify — add `push_book_levels`)

**Analog:** self — existing `push_top_of_book` (lines 169-202).

**Narrow column projection + chunked-insert + dual-anchor breadcrumb envelope** (`l2_supabase_mirror.py:55-65, 169-202`):
```python
_NARROW_TOB_COLUMNS: tuple[str, ...] = (
    "asset_id", "ts", "best_bid", "best_ask", "spread",
    "mid_price", "depth_yes_usd", "depth_no_usd", "source_event",
)

def push_top_of_book(self, rows: list[dict]) -> bool:
    try:
        narrow = _project(rows, _NARROW_TOB_COLUMNS)
        for chunk in _chunk(narrow, _CHUNK_SIZE):
            self._client.table("l2_top_of_book").insert(chunk).execute()
        sentry_sdk.add_breadcrumb(
            category="l2-mirror", level="info",
            message=f"push_top_of_book ok rows={len(rows)}",
            data={"rows": len(rows), "table": "l2_top_of_book"},
        )
        logger.info(f"l2-mirror: pushed {len(rows)} top_of_book rows")
        self._refresh_freshness_cache()
        return True
    except Exception as e:  # noqa: BLE001 — fail-soft per D-12
        logger.error(f"l2-mirror push_top_of_book failed rows={len(rows)}: {str(e)[:200]}")
        sentry_sdk.add_breadcrumb(
            category="l2-mirror", level="warning",
            message=f"push_top_of_book failed rows={len(rows)}",
            data={"rows": len(rows), "table": "l2_top_of_book", "error": str(e)[:200]},
        )
        return False
```

→ New `push_book_levels(self, rows: list[dict]) -> bool` (RESEARCH §Example 3 lines 805-835): same envelope verbatim. Two notable differences from RESEARCH draft:
- **Use `category="l2-mirror"`** (NOT `"l3-book-levels"` as RESEARCH Example 3 suggests). Phase 02.1 P2 pattern: one distinct category per service (`l2-mirror` covers all writes from this client). The breadcrumb `message`/`data.table="l2_book_levels"` already disambiguates the table.
- **Freshness anchor update** — the L3-specific anchor `_last_book_levels_write_at_s` lives in `l3_promote.py` module state (Pattern 5 below). On success, `push_book_levels` should do `from polyarb.observation import l3_promote; l3_promote._last_book_levels_write_at_s = _time.time()` rather than the shared `_refresh_freshness_cache()` (that's a TOB/trades-specific cache). This is the chain-truth write-side mutation the L3 sub-check reads.

**Module-level chunk size + helpers** (`l2_supabase_mirror.py:92-107`): reuse `_CHUNK_SIZE=1000`, `_chunk()`, `_project()` verbatim — they are module-level utilities and the new method imports them by living in the same file.

---

### 4. `src/polyarb/clients/ws_market_client.py` (modify — extend on_connect hook surface)

**Analog:** self — existing `stream_market_events` (`ws_market_client.py:45-148`) and on_connect side-channel (lines 94-99).

**on_connect hook contract** (`ws_market_client.py:61-65, 94-99`):
```python
on_connect: Callable[[Any], None] | None = None
...
# GAP-401: notify consumer of the live ws object (side-channel).
if on_connect is not None:
    try:
        on_connect(ws)
    except Exception as _e:  # noqa: BLE001
        logger.warning(f"ws_market_client: on_connect hook raised: {_e!r}")
```
→ This is the Phase 04.1 SESSION 33 GAP-401 entry point. Phase 05 does **NOT modify `stream_market_events`** — instead, `WsConsumer._stash_ws` (already wired via this hook) stashes the live ws object; the new `add_subscriptions`/`remove_subscriptions` methods (on `WsConsumer`) call `self._current_ws.send(...)` directly. Per RESEARCH Pitfall 2 + Pitfall 7: `stream_market_events` SHOULD remain untouched to avoid regressing GAP-401.

**Critical constants (do NOT relax)** (`ws_market_client.py:36-42`):
- `PING_INTERVAL_S = 10` (Polymarket server drops at ~10s silence)
- `MAX_FRAME_SIZE = 2**22` (4 MiB, for initial_dump book snapshots)
- `async for ws in websockets.connect(...)` reconnect-iterator (NEVER `async with`)

**Subscribe payload schema** (`ws_market_client.py:85-90`):
```python
sub = {
    "type": "market",
    "assets_ids": assets_ids,
    "initial_dump": initial_dump,
}
await ws.send(json.dumps(sub))
```
→ Phase 05 mid-connection payload schema differs (per thread §2.2 Q1 + RESEARCH Example 4): `{"operation": "subscribe", "assets_ids": [...], "initial_dump": True}` — note `operation` key not `type` key.

---

### 5. `src/polyarb/daemon/ws_consumer.py` (modify — `add_subscriptions` / `remove_subscriptions`)

**Analog:** self — existing GAP-401 liveness gate (`ws_consumer.py:148-176`) is the canonical example of "mutate state via on_connect, expose getter".

**GAP-401 stash + liveness pattern** (`ws_consumer.py:138-176`):
```python
# Module:
from websockets.protocol import State as WsState

# __init__:
self._current_ws: Any = None
self._watchdog._liveness_check = self._liveness_check

# Methods:
def _stash_ws(self, ws: Any) -> None:
    """Store the current live ws object (called by on_connect hook each connect)."""
    self._current_ws = ws

def _liveness_check(self) -> bool:
    ws = self._current_ws
    if ws is None:
        return False
    try:
        return ws.state is WsState.OPEN and ws.latency > 0
    except Exception:  # noqa: BLE001
        return False
```

→ Phase 05 adds two methods to the same class (RESEARCH §Example 4 lines 845-904):
- `async def add_subscriptions(self, asset_ids: list[str]) -> bool`
- `async def remove_subscriptions(self, asset_ids: list[str]) -> bool`

Both methods read `self._current_ws` (the GAP-401 stash), check it's not None, and `await self._current_ws.send(json.dumps({"operation": "subscribe"|"unsubscribe", "assets_ids": [...]}))`. On no-live-ws they only mutate `_subscribed_assets` so the next reconnect picks up the new set (fallback path — Pitfall 2 alternative).

**Concurrent send + recv invariant** (RESEARCH Pitfall 2): websockets 15+ allows send/recv from different tasks. The consume loop `async for event in stream_market_events(...)` reads via the library's recv path; `add_subscriptions` writes via send. No lock needed.

**`_subscribed_assets` mutation contract** — existing private-attribute pattern (`ws_consumer.py:128, 192-194` + `l2_candidate_refresh.py:417` direct write to `ws_consumer._subscribed_assets`): mutate the list in place to keep `subscribed_assets` property (which returns a defensive copy) coherent.

**Race-condition warning (Pitfall 5):** `l2_candidate_refresh.on_snapshot_complete` line 417 does `ws_consumer._subscribed_assets = list(new_asset_ids)` — **full list overwrite**, which would clobber L3 tokens. RESEARCH recommends Pitfall-5 Option 1: refactor `WsConsumer` to track `_candidate_set: set[str]` + `_l3_active_set: set[str]` separately and recompute the union on every mutation. Planner decides whether to land this refactor inside the Phase 05 plan or as a sibling plan task.

**Clear stash on disconnect** (`ws_consumer.py:270, 279`): both `WsTestKillRequested` and `CancelledError` branches set `self._current_ws = None`. New `remove_subscriptions` must not crash if called between disconnect-and-reconnect — it already guards on `self._current_ws is None`.

---

### 6. `src/polyarb/observation/l3_promote.py` (NEW — promoter module)

**Analog:** `src/polyarb/observation/l2_candidate_refresh.py` (sibling — debounced refresh module).

**Module-level state pattern** (`l2_candidate_refresh.py:46-72`):
```python
MAX_CANDIDATES: int = 500            # R9 hard cap
REFRESH_DEBOUNCE_S: float = 60.0     # SP8 cross-bug check #1

# G-01 fix: init MUST be < -DEBOUNCE_S so first call passes the debounce.
# Memory: feedback_cold-start-debounce-trap-2026-05.
_last_refresh_at_s: float = -REFRESH_DEBOUNCE_S - 1.0

_last_known_markets_rows: list[dict] | None = None
_last_fetch_success_at_s: float | None = None


def _record_fetch_success() -> None:
    """Mark a successful Supabase fetch — drives /health sub-check freshness."""
    global _last_fetch_success_at_s
    _last_fetch_success_at_s = time.time()


def get_last_fetch_success_at_s() -> float | None:
    """Public getter for /health candidates:supabase_fetch_age_seconds.
    Chain-truth note (§1.6): this getter reads a field that the fetch path
    REALLY mutates — NOT a dead-code config flag (Inj L2-2 RCA prevention)."""
    return _last_fetch_success_at_s
```

→ `l3_promote.py` mirrors this pattern (RESEARCH §Example 5 lines 928-1006):
```python
_l3_active_set: set[str] = set()
_last_promote_at_s: float | None = None
_last_book_levels_write_at_s: float | None = None  # mutated by mirror.push_book_levels

def get_l3_active_set() -> set[str]: ...
def get_l3_active_count() -> int: ...
def get_last_promote_at_s() -> float | None: ...
def get_last_book_levels_write_at_s() -> float | None: ...
```

**HARD RULE — cold-start trap** (`l2_candidate_refresh.py:54-61` per memory `feedback_cold-start-debounce-trap-2026-05`): any cooldown / debounce float MUST init to `-INTERVAL_S - 1.0` (or use `None` sentinel), NEVER `0.0`. If the L3 promoter uses an internal debounce, it must follow this rule. The 5-min cron itself is APScheduler-driven (no manual debounce needed if the scheduler is the only caller), but any manual `promote_run()` debounce must follow.

**Fail-soft refresh handler envelope** (`l2_candidate_refresh.py:329-439, esp. 374-393`):
```python
markets_rows: list[dict] | None = None
supabase_url = getattr(settings, "supabase_url", "")
service_key = ""
try:
    service_key = settings.supabase_service_key.get_secret_value()
except AttributeError:
    service_key = ""
if supabase_url and service_key:
    try:
        client = create_client(supabase_url, service_key)
        markets_rows = _fetch_all_markets_latest(client)
        _last_known_markets_rows = markets_rows
        _record_fetch_success()
        logger.info(f"candidate refresh: fetched {len(markets_rows)} rows ...")
    except Exception as e:  # noqa: BLE001 — fail-soft envelope
        logger.error(f"... supabase fetch failed: {e!r} — using last known rows ...")
        markets_rows = _last_known_markets_rows
```
→ `promote_run()` uses identical envelope: fetch `l2_top_of_book` from Supabase, freeze last-known set on Supabase outage (Open Question #5 recommendation).

**Diff + ws-mutation pattern** (`l2_candidate_refresh.py:314-326, 408-417`):
```python
def diff_candidate_sets(old, new_rows) -> tuple[set[str], list[CandidateRow]]:
    new_asset_ids = {r.asset_id for r in new_rows}
    removed = old_asset_ids - new_asset_ids
    added_rows = [r for r in new_rows if r.asset_id not in old_asset_ids]
    return removed, added_rows

# in handler:
old_asset_ids = set(ws_consumer.subscribed_assets)
removed, added = diff_candidate_sets(old_asset_ids, new_rows)
logger.info(f"candidate refresh: +{len(added)} -{len(removed)} ...")
ws_consumer._subscribed_assets = list(new_asset_ids)
```
→ `promote_run()` (RESEARCH Example 5 lines 994-1006): `added = new_set - _l3_active_set; removed = _l3_active_set - new_set`; then call `await ws_consumer.add_subscriptions(sorted(added))` / `await ws_consumer.remove_subscriptions(sorted(removed))`; finally `_l3_active_set = new_set; _last_promote_at_s = time.time()`.

**Scanner+temp-DB usage** (Pitfall 3 — `l2_candidate_refresh.py:152-198` + `l2_temp_db.py` reading: the temp-DB pattern):
```python
from polyarb.observation.l2_temp_db import build_temp_db, warn_null_filled_recipe_columns
from polyarb.observation.scanner import list_all_recipes, run_recipe

# In refresh:
if markets_rows is not None:
    db_path = build_temp_db(markets_rows)
    cleanup_tmp = True
else:
    db_path = Path(settings.db_path)
    cleanup_tmp = False
try:
    ...run_recipe(db_path, recipe)...
finally:
    if cleanup_tmp:
        try:
            os.unlink(db_path)
        except OSError:
            logger.warning(f"temp DB cleanup failed: {db_path}")
```
→ L3 promoter has two options (Pitfall 3 Option 1 vs 2). Recommended (RESEARCH): build temp-DB from `l2_top_of_book` rows (mirror this pattern), then `run_recipe(tmp_path, l3_promote_recipe)`. The l3-promote recipe is constructed via `Recipe.from_builtin(...)` (with `_is_trusted=True`) since it's authored in repo source — bypasses strict validators. **Note**: `l2_temp_db.build_temp_db` is currently markets-table-specific; a sibling helper `build_temp_db_from_tob_rows(...)` may be needed (planner decides).

**Cron scheduling — use raw asyncio loop, NOT apscheduler** (pyproject.toml audit shows **no `apscheduler` dependency** — RESEARCH § Standard Stack mention is incorrect). Use the L1 daemon orchestrator pattern (`snapshot/orchestrator.py:540+` + the `scheduler_interval_s` setting in `config.py:48`): an `async def run_periodic(stop_event, interval_s=300, ...)` loop with `try: await asyncio.wait_for(stop_event.wait(), timeout=interval_s) except asyncio.TimeoutError: ...` — same shape `ws_consumer.run` uses at lines 219-228 for its empty-asset wait. F-04 propagation via raise of CancelledError.

---

### 7. `src/polyarb/scan_recipes/l3-promote.yaml` (NEW)

**Analog:** `config/scan_recipes.yaml` (`my-watchtower` recipe).

**Recipe schema** (`config/scan_recipes.yaml:22-30`):
```yaml
recipes:
  my-watchtower:
    description: 我每天早上看的市场（liq>$50k + 7 天内 + neg-risk 多腿）
    where: |
      liquidity_usd > 50000
      AND end_time_ms < strftime('%s', 'now', '+7 days') * 1000
      AND neg_risk_market_id IS NOT NULL
    order_by: liquidity_usd DESC
    limit: 50
```

→ For l3-promote.yaml (RESEARCH §Example 5 lines 909-926): same key shape — `description` (Chinese), `where` (pipe block with AND-joined predicates), `order_by` (bare column DESC — strict validator at `scanner.py:68-71`), `limit` (int).

**SQL discipline (strict yaml path — Layer 2/3 validators)** (`scanner.py:53-71`):
- `_FORBIDDEN`: no `;` / `--` / `/*` / `DROP DELETE UPDATE INSERT ALTER CREATE ATTACH DETACH PRAGMA UNION TRUNCATE VACUUM REINDEX SELECT`
- `_ORDER_BY_OK`: bare column(s) + optional `ASC|DESC`; no arithmetic, no functions
- `_validate_limit`: int in `[1, 10000]`
- D-13 thresholds (CONTEXT): `spread < 0.02 AND depth_yes_usd > 500 AND ts > (now() - interval '1 hour') ORDER BY depth_yes_usd DESC LIMIT 5`

**Recipe-location decision** (CONTEXT Claude's Discretion + RESEARCH §Recommended Project Structure): NEW directory `src/polyarb/scan_recipes/l3-promote.yaml` (RESEARCH explicit) **vs** appending to existing `config/scan_recipes.yaml`. RESEARCH chose the new dir — planner should confirm one path and update Makefile entry / settings (`settings.candidate_scanner_yaml` already accepts a Path).

---

### 8. `src/polyarb/http/l2_health.py` (modify — 3 L3 sub-checks)

**Analog:** self — `mirror:l2_tob_age_seconds` three-branch (lines 210-291) and `candidates:supabase_fetch_age_seconds` (lines 293-361).

**Chain-truth three-branch gate pattern** (`l2_health.py:224-291`):
```python
_supabase_url = getattr(settings, "supabase_url", "")
_service_key_val = ""
try:
    _service_key_val = settings.supabase_service_key.get_secret_value()
except AttributeError:
    pass

if _supabase_url and not _service_key_val:
    # Case (b) — surface config mistake as fail
    checks["mirror:l2_tob_age_seconds"] = [{...
        "status": "fail",
        "output": "mirror disabled by config (service_key empty)",
        ...}]
    overall = _severity(overall, "fail")
elif getattr(settings, "l2_mirror_enabled", False):
    # Case (c) — real age-based pass/warn/fail
    warn_s = int(getattr(settings, "l2_tob_age_warn_s", _MIRROR_PASS_S_DEFAULT))
    fail_s = int(getattr(settings, "l2_tob_age_fail_s", _MIRROR_FAIL_S_DEFAULT))
    try:
        getter = getattr(store, "get_l2_tob_last_mirror_at_s", None)
        last_mirror_at: Any = getter() if callable(getter) else None
        if last_mirror_at is None:
            mirror_status = "warn"; mirror_age = None
            mirror_output = "cold-start: never mirrored"
        else:
            mirror_age = now_s - float(last_mirror_at)
            if mirror_age >= fail_s: mirror_status = "fail"
            elif mirror_age >= warn_s: mirror_status = "warn"
            else: mirror_status = "pass"
            ...
```

**Chain-truth getter import pattern** (`l2_health.py:314-322` for the candidates sub-check):
```python
try:
    from polyarb.observation.l2_candidate_refresh import (
        get_last_fetch_success_at_s,
    )
    last_fetch_at = get_last_fetch_success_at_s()
except Exception as e:  # noqa: BLE001 — fail-soft on /health read
    logger.warning(f"... fail-soft: {e!r}")
    last_fetch_at = None
    fetch_status = "warn"
    ...
```

→ Phase 05 adds 3 sub-checks (RESEARCH Pattern 3 + Validation Architecture):

| Sub-check | Reads from | Status logic |
|---|---|---|
| `l3:active_count` | `l3_promote.get_l3_active_count()` | informational `pass`; `warn` when count < N (target 5) |
| `l3:last_promote_at_s` | `l3_promote.get_last_promote_at_s()` | `warn` at 2× cron interval (600s), `fail` at 6× (1800s); cold-start (None) → `warn` |
| `l3:last_book_levels_write_at_s` | `l3_promote.get_last_book_levels_write_at_s()` | `warn` at 2× expected event window, `fail` at 10×; cold-start (None) → `warn` |

**Chain-truth invariant (CLAUDE.md §chain-truth, code-vs-chain-truth-2026-05):** the three new getters MUST read fields that the write-side really mutates — `_last_promote_at_s` is set by `promote_run()` on success; `_last_book_levels_write_at_s` is set by `L2SupabaseMirror.push_book_levels` on success. Do NOT gate the sub-checks on a config flag that nobody writes (Phase 03 Inj L2-2 / GAP-200 lesson).

**Informational-only sub-check pattern** (`l2_health.py:171-186` `ws:subscribed_count` + lines 396-421 `process:rss_kb`): `status="pass"` always, NO `overall = _severity(...)` line. Use this shape for `l3:active_count` per RESEARCH §Pattern 3 ("informational pass").

---

### 9. `dashboard/app/l3/[asset_id]/page.tsx` (NEW — RSC page)

**Analog:** `dashboard/app/asset/[id]/tob/page.tsx` (sibling dynamic asset page).

**Server component preamble** (`tob/page.tsx:1-11`):
```typescript
import { getTopOfBookForAsset, type L2TopOfBook } from "@/lib/supabase/l2-queries";

export const dynamic = "force-dynamic";
export const revalidate = 0;
```
→ Phase 05 page imports `getOhlcForAsset` + `getBookLevelsLatest` + `KlineChart` client component. Same `dynamic = "force-dynamic"` + `revalidate = 0` (no caching for live data).

**Dynamic param pattern** (`tob/page.tsx:32-38`):
```typescript
export default async function AssetTobPage({
  params,
}: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const assetId = decodeURIComponent(id);
  ...
}
```
→ Phase 05 uses `params: Promise<{ asset_id: string }>` (RESEARCH §Example 6 line 1079 — note the URL param name `[asset_id]` not `[id]`). `decodeURIComponent` is mandatory (security — RESEARCH §Security Domain).

**Fail-soft try/catch + banner pattern** (`tob/page.tsx:40-87`):
```typescript
let rows: L2TopOfBook[] = [];
let errorMsg: string | null = null;
try {
  rows = await getTopOfBookForAsset(assetId, 24, 500);
} catch (e) {
  errorMsg = e instanceof Error ? e.message : "Supabase unreachable";
}

return (
  <main style={{ padding: 24 }}>
    ...
    {errorMsg && (
      <div style={{
        background: "#3b2a0a", border: "1px solid #6b4a10",
        padding: 12, borderRadius: 4, marginBottom: 16,
        fontSize: 13, color: "#ffd47a",
      }}>
        Supabase warning: {errorMsg}. Showing empty table (fail-soft).
      </div>
    )}
    ...
);
```
→ L3 page uses identical `try/catch` envelope + identical banner CSS (LEARNINGS P5 — never 500 on Supabase outage). RESEARCH §Example 6 lines 1083-1092 demonstrates the parallel structure for L3.

**Style language** — inline `style={{ ... }}` with hex colors (`#7fc6ff`, `#3b2a0a`, `#888`, `#1d1d1d`) — minimal/dense, Phase 02 dashboard convention (CONTEXT §specifics).

---

### 10. `dashboard/app/l3/[asset_id]/KlineChart.tsx` (NEW — client component)

**Analog:** No existing chart component in the dashboard.

**Pattern source:** RESEARCH §Example 6 lines 1121-1171 — lightweight-charts v5 dynamic import inside `useEffect` (server-side `window`/`document` unavailable; MUST be client-only).

**Critical contract (RESEARCH Anti-Patterns):**
- File must start with `"use client";`
- `import { createChart, CandlestickSeries } from "lightweight-charts"` is **dynamic** inside `useEffect`, not top-level (`const { createChart } = await import("lightweight-charts");`)
- v5 API: `chart.addSeries(CandlestickSeries, options)` (NOT v4 `chart.addCandlestickSeries(options)`)
- Cleanup: return `() => { resizeObserver?.disconnect(); chart?.remove(); }` from `useEffect`
- Time format: `Math.floor(new Date(r.bucket_ts).getTime() / 1000)` (Unix seconds, not ms)

**Dependency:** `pnpm add lightweight-charts@^5.2.0` in `dashboard/` (RESEARCH §Standard Stack — not currently in `dashboard/package.json`).

---

### 11. `dashboard/lib/supabase/l2-queries.ts` (modify — add OHLC + book_levels helpers)

**Analog:** self — `getTopOfBookForAsset` (lines 97-116) and `getTradesForAsset` (lines 122-141).

**Query helper signature pattern** (`l2-queries.ts:97-116`):
```typescript
export async function getTopOfBookForAsset(
  assetId: string,
  hours = 24,
  limit = 1000,
  supabase?: SupabaseClient,
): Promise<L2TopOfBook[]> {
  const client = supabase ?? (await getServerSupabase());
  const cutoff = new Date(Date.now() - hours * 3600 * 1000).toISOString();
  const { data, error } = await client
    .from("l2_top_of_book")
    .select("id, asset_id, ts, best_bid, best_ask, spread, mid_price, depth_yes_usd, depth_no_usd, source_event")
    .eq("asset_id", assetId)
    .gte("ts", cutoff)
    .order("ts", { ascending: false })
    .limit(limit);
  if (error) throw error;
  return (data ?? []) as L2TopOfBook[];
}
```

→ `getOhlcForAsset(assetId, granularity, hours, supabase?)` + `getBookLevelsLatest(assetId, supabase?)` — same shape, different `from()` table and select columns (RESEARCH §Example 6 lines 1033-1066).

**Row type interface pattern** (`l2-queries.ts:33-44`):
```typescript
export interface L2TopOfBook {
  id: number;
  asset_id: string;
  ts: string;
  ...
}
```
→ Add `L2OhlcRow` + `L2BookLevel` interfaces (RESEARCH §Example 6 lines 1014-1031).

**RLS / anon-key invariant** (`l2-queries.ts:1-14`): ONLY uses `NEXT_PUBLIC_SUPABASE_ANON_KEY` via `getServerSupabase()`. NEVER imports the daemon's service key. The new view GRANTs (Alembic 005, lines 729-731 in RESEARCH Example 1) make `l2_ohlc_*` anon-readable; `l2_book_levels` RLS policy makes it anon-readable.

---

### 12. `dashboard/app/candidates/page.tsx` (modify — add L3 badge column)

**Analog:** self — existing rendering (lines 75-126) demonstrates the table column add-pattern.

**Column add pattern** — for Pitfall 8 Option C (`l2_candidates.l3_promoted_at_ts` filter): add new `<th>L3</th>` and per-row `<td>{row.l3_promoted_at_ts ? "★" : ""}</td>` (or similar visual marker). Extend the `L2Candidate` interface in `l2-queries.ts:20-31` to add `l3_promoted_at_ts: string | null;`. Extend the `select(...)` projection in `getActiveCandidates` (line 84) to include `l3_promoted_at_ts`.

**Link to L3 detail page** — extend "drill" column (lines 106-119) to add `<a href={`/l3/${encodeURIComponent(row.asset_id)}`}>l3</a>` link when `row.l3_promoted_at_ts !== null`.

---

### 13. `Makefile` (modify — new command entry-points)

**Analog:** self — existing `scan-*` recipe targets (lines 138-185) and `chaos-l2-*` targets.

**Target pattern** (`Makefile:144-149`):
```makefile
## scan: Generic recipe runner — usage: make scan name=<recipe>
scan:
	uv run python -m polyarb.cli_observation scan --name $(name) --verbose

## scan-thick-but-slippery: Trap markets — high liq ($100k+) but wide spread (>$0.10)
scan-thick-but-slippery:
	uv run python -m polyarb.cli_observation scan --name thick-but-slippery --verbose
```

→ New targets (RESEARCH §Project Constraints + §Wave 0 Gaps):
- `l3-promote-dry-run` — run `polyarb.observation.l3_promote.promote_run` once without WS mutation; log the candidate set
- `ohlc-spot-check` — query `l2_ohlc_1m` for 5 L3 assets over last 24h; print row counts
- `smoke-l3-dashboard` — playwright-cli or curl smoke against `/l3/[asset_id]` (matches Phase 02 dashboard smoke pattern)
- `supabase-migrate-test` — `uv run alembic upgrade head` + `downgrade -1` + `upgrade head` against test DB (validates 005 forward+reverse)

**Naming convention (CLAUDE.md §命令入口约定):** `make <verb>-<noun>` — all four above comply. Each target gets a `##` comment line for `make help`.

---

## Shared Patterns

### Authentication / Access Control

**Source:** `src/polyarb/http/l2_control.py` (HMAC-gated control endpoints, Phase 02.1 D-03/D-04) — IF Phase 05 adds a manual L3 promote trigger endpoint (RESEARCH §Don't Hand-Roll table). Phase 05 may not need this; planner decides.

**Surface:** `ControlAuthMiddleware` — same as `/control/chaos/ws-test-kill` (Phase 04.1 G-03).

### Error Handling — Fail-Soft Envelope (Phase 02 LEARNINGS P5)

**Source:** `src/polyarb/storage/l2_supabase_mirror.py:169-202` (the canonical envelope).
**Apply to:** All Phase 05 write paths — `push_book_levels`, `l3_promote.promote_run`, `compute_l3_candidates`.

```python
try:
    ... real work ...
    sentry_sdk.add_breadcrumb(category="l2-mirror", level="info",
        message=f"<op> ok rows={len(rows)}",
        data={"rows": len(rows), "table": "<table>"})
    logger.info(f"<op> succeeded ...")
    return True
except Exception as e:  # noqa: BLE001 — fail-soft per D-12
    logger.error(f"<op> failed: {str(e)[:200]}")
    sentry_sdk.add_breadcrumb(category="l2-mirror", level="warning",
        message=f"<op> failed",
        data={"rows": len(rows), "table": "<table>", "error": str(e)[:200]})
    return False
```

**Apply also to dashboard:** `try/catch` in RSC + banner — `dashboard/app/asset/[id]/tob/page.tsx:40-87`.

### Validation — SQL Recipe 4-Layer Defense

**Source:** `src/polyarb/observation/scanner.py:53-123`.
**Apply to:** l3-promote.yaml (yaml path: `_is_trusted=False` triggers all 4 layers). If l3-promote is constructed as `Recipe.from_builtin(...)` instead, layers 2/3 bypass (allowed for source-controlled recipes) but layers 1 (read-only URI) and 4 (limit) still apply.

### Chain-Truth /health Surface

**Source:** `src/polyarb/http/l2_health.py:293-361` (`candidates:supabase_fetch_age_seconds`).
**Apply to:** All 3 Phase 05 L3 sub-checks. Critical invariant: every getter the sub-check calls MUST read a field the write path actually mutates. Verified by Inj L2-2 RCA / GAP-200 lessons.

### Cold-Start Debounce Trap

**Source:** `src/polyarb/observation/l2_candidate_refresh.py:54-61` (G-01 fix) + memory `feedback_cold-start-debounce-trap-2026-05`.
**Apply to:** Any module-level `_last_X_at_s: float` debounce/cooldown var introduced by Phase 05. Init MUST be `-INTERVAL_S - 1.0` (NEVER `0.0`) OR use `None` sentinel.

### Module-Level Mutable State + Public Getter

**Source:** `l2_candidate_refresh.py:67-92` (`_last_known_markets_rows`, `_last_fetch_success_at_s`, plus `_record_fetch_success` / `get_last_fetch_success_at_s`).
**Apply to:** `l3_promote.py` (`_l3_active_set`, `_last_promote_at_s`, `_last_book_levels_write_at_s`) — write inside the module, expose via `get_*` helpers, /health imports the getters.

### F-04 Bounded Shutdown

**Source:** `src/polyarb/daemon/l2_main.py:474-489` (5s timeout per task on shutdown).
**Apply to:** L3 promoter task — when wired into `l2_main`, must be added to the shutdown-iteration tuple list so SIGTERM gives it 5s to exit.

### Phase 04.1 GAP-401 Liveness Gate (DO NOT REGRESS)

**Source:** `src/polyarb/daemon/ws_consumer.py:138-176` + `.planning/quick/260531-gap-401-watchdog-false-trip/SUMMARY.md`.
**Apply to:** Phase 05 ws_consumer + ws_market_client modifications MUST preserve:
- `WsConsumer._current_ws` stash (set in `_stash_ws`, cleared on disconnect lines 270/279)
- `WsConsumer._liveness_check` closure (lines 156-176)
- `self._watchdog._liveness_check = self._liveness_check` wiring (line 144)
- `ws_market_client.stream_market_events` on_connect hook (lines 94-99)

**Verification:** Plan must include "regression: GAP-401 liveness test green" item — run the 10-test suite in `tests/m1-perception/test_ws_watchdog_liveness.py`.

---

## No Analog Found

| File | Role | Data Flow | Reason | Substitute |
|---|---|---|---|---|
| `dashboard/app/l3/[asset_id]/KlineChart.tsx` | client component | client-only canvas | No chart component exists in dashboard | RESEARCH §Example 6 lines 1121-1171 (lightweight-charts v5 dynamic-import pattern) |

---

## Critical Cross-Pattern Decisions

1. **`push_book_levels` Sentry category** — RESEARCH Example 3 suggests `category="l3-book-levels"`, but existing Phase 02.1 P2 pattern uses one category per service (`l2-mirror` for all writes from this mirror class). Recommendation: use `category="l2-mirror"` with `data.table="l2_book_levels"` to disambiguate. Planner decides; both are defensible but the per-service convention reduces Sentry filter complexity.

2. **L3 freshness anchor location** — `_last_book_levels_write_at_s` lives in `l3_promote.py` (module-level), NOT in `l2_supabase_mirror.py`. The mirror imports the module and mutates the field on push success. This keeps the chain-truth invariant clean: /health → `l3_promote.get_last_book_levels_write_at_s()` → field mutated by mirror.

3. **Race condition on `_subscribed_assets` (Pitfall 5)** — `l2_candidate_refresh.on_snapshot_complete:417` does a full list overwrite. Phase 05 plan must address this — either via Pitfall-5 Option 1 (refactor to `_candidate_set` + `_l3_active_set` with `_compute_active_assets()` union) or document the timing and rely on the next L3 promoter run to re-add. Option 1 is cleaner; planner picks.

4. **APScheduler vs raw asyncio loop** — RESEARCH §Standard Stack claims AsyncIOScheduler is "Phase 02 D-15 already used" but `grep apscheduler pyproject.toml` returns nothing and there is no scheduler import in the codebase. Use raw asyncio `await asyncio.wait_for(stop_event.wait(), timeout=300)` loop pattern instead — matches existing `ws_consumer.run` style at `ws_consumer.py:219-228`. No new dependency.

5. **Recipe location** — RESEARCH chose new dir `src/polyarb/scan_recipes/` over the existing `config/scan_recipes.yaml`. Planner should pick one path and update `settings.candidate_scanner_yaml` reference if new dir is used. Existing user-recipe yaml at `config/scan_recipes.yaml` should remain (it has `my-watchtower`).

---

## Metadata

**Analog search scope:**
- `src/polyarb/daemon/` (l2_main, ws_consumer, ws_watchdog)
- `src/polyarb/clients/` (ws_market_client)
- `src/polyarb/storage/` (l2_supabase_mirror)
- `src/polyarb/observation/` (l2_candidate_refresh, scanner, recipes, l2_temp_db)
- `src/polyarb/http/` (l2_health)
- `src/polyarb/snapshot/` (orchestrator scheduler loop pattern)
- `alembic/versions/` (003, 004)
- `config/scan_recipes.yaml`
- `dashboard/app/asset/[id]/tob/`
- `dashboard/app/candidates/`
- `dashboard/lib/supabase/`
- `dashboard/lib/supabase-server.ts`
- `Makefile` (scan-* + chaos-* targets)
- `pyproject.toml` (dependency audit — apscheduler absent, psutil/asyncpg/supabase/websockets present)

**Files scanned:** 14
**Pattern extraction date:** 2026-06-01
**Cross-checked against:** CONTEXT D-01..D-16 + RESEARCH §Architecture Patterns / §Common Pitfalls 1-8 / §Code Examples 1-6
