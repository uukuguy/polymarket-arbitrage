---
phase: 01
plan: 4
type: execute
wave: 3
depends_on: [01-2, 01-3]
files_modified:
  - src/polyarb/snapshot/normalizer.py
  - src/polyarb/snapshot/orchestrator.py
  - src/polyarb/snapshot/cli.py
  - src/polyarb/snapshot/__main__.py
  - Makefile
autonomous: true
requirements: []
must_haves:
  truths:
    - "normalizer.normalize_market(raw_dict) -> dict converts Gamma JSON-string fields (outcomePrices, clobTokenIds) into typed values + extracts yes_token_id/no_token_id as separate keys"
    - "normalizer outputs a dict whose keys match MARKETS_COLUMN_ORDER (minus snapshot_id which orchestrator injects)"
    - "orchestrator.run_snapshot(mode) executes 7 explicit steps (gamma fetch → mode filter → CLOB fetch → fetched_at_ms stamp → validate → parquet write → SQLite write) per RESEARCH.md Atomic SQLite + Parquet 编排 spec"
    - "orchestrator wraps GammaClient + ClobReaderClient via async context managers; aclose runs even on exception"
    - "orchestrator catches RetryError from clients and converts to Issue(category=API_UNREACHABLE) without aborting the snapshot — snapshot still written with is_valid=false"
    - "every market row gets fetched_at_ms (CLOB-fetch completion time) before storage"
    - "CLI 'polyarb snapshot' default mode is subset; --full flag enables full mode"
    - "CLI default output is single line: OK | <count> markets | mode=<mode> | <issue_count> issues | -> <parquet_path>"
    - "CLI --verbose shows progress + phase timings via tqdm"
    - "CLI exits 0 when is_valid=true, 1 otherwise; stderr summary on is_valid=false"
    - "make snapshot-markets and make snapshot-markets-full both exist with `## name: description` doc comments + appear in `make help`"
    - "python -m polyarb.snapshot --help works (via __main__.py)"
  artifacts:
    - path: src/polyarb/snapshot/normalizer.py
      provides: "Gamma raw dict → MARKETS_COLUMN_ORDER-compatible dict"
      exports: ["normalize_market"]
    - path: src/polyarb/snapshot/orchestrator.py
      provides: "async run_snapshot(settings, mode, now_ms) coordinator"
      exports: ["run_snapshot", "SnapshotResult"]
    - path: src/polyarb/snapshot/cli.py
      provides: "typer app + 'snapshot' subcommand + flag parsing"
      exports: ["app"]
    - path: src/polyarb/snapshot/__main__.py
      provides: "python -m polyarb.snapshot entry"
    - path: Makefile
      provides: "snapshot-markets + snapshot-markets-full targets with docs"
  key_links:
    - from: "src/polyarb/snapshot/orchestrator.py"
      to: "GammaClient + ClobReaderClient"
      via: "async with GammaClient(s) as g, ClobReaderClient(s) as c"
      pattern: "GammaClient.*ClobReaderClient"
    - from: "src/polyarb/snapshot/orchestrator.py"
      to: "validator/layers + storage/sqlite_store + storage/parquet_writer"
      via: "import all; call after fetch + before exit"
      pattern: "layer1_count|layer2_fields|layer4_cross_source|write_parquet_atomic|SQLiteStore"
    - from: "src/polyarb/snapshot/cli.py"
      to: "asyncio.run(run_snapshot(...))"
      via: "typer command body invokes asyncio.run"
      pattern: "asyncio\\.run"
    - from: "Makefile"
      to: "python -m polyarb.snapshot"
      via: "make target body invokes module entry"
      pattern: "python -m polyarb\\.snapshot"
---

<objective>
Wire all the parts (clients + storage + validator) together: a `normalizer` that maps raw Gamma dicts to the storage row contract, an `orchestrator` that runs the 7-step async pipeline, a `cli` that exposes the `snapshot` subcommand with `--full`/`--verbose`/`--config` flags, and Makefile targets that satisfy CONTEXT.md D-MK1/MK2/MK3.

Critical invariants:
- Normalizer parses JSON-string fields (outcomePrices, clobTokenIds) per Pitfall 2
- Normalizer keeps token_id as str (Pitfall 3 — uint256 overflows int64)
- Orchestrator collects everything, validates, then writes in one transaction (no streaming writes — Anti-Pattern #2 + D-E3)
- Per-row `fetched_at_ms` is set when CLOB fetch completes (Pitfall 6 — best-effort consistency disclaimer)
- API_UNREACHABLE handled gracefully: snapshot still persists with is_valid=false (D-D3 + D-E2)
- CLI exit code matches is_valid (0=true, 1=false) per D-F3
- Makefile uses `python -m polyarb.snapshot` form per CONTEXT.md MK1/MK2 examples + resolved Q7

Output: 4 source files (normalizer, orchestrator, cli, __main__) + Makefile additions.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/workstreams/m1-perception/phases/01-/01-CONTEXT.md
@.planning/workstreams/m1-perception/phases/01-/01-RESEARCH.md
@.planning/workstreams/m1-perception/phases/01-/01-PATTERNS.md
@.planning/workstreams/m1-perception/phases/01-/01-2-SUMMARY.md
@.planning/workstreams/m1-perception/phases/01-/01-3-SUMMARY.md
@src/polyarb/config.py
@src/polyarb/clients/gamma_client.py
@src/polyarb/clients/clob_client.py
@src/polyarb/storage/schemas.py
@src/polyarb/storage/sqlite_store.py
@src/polyarb/storage/parquet_writer.py
@src/polyarb/validator/category.py
@src/polyarb/validator/layers.py
@Makefile
</context>

<interfaces>
From Plan 1 (config.py):
- Settings(...), load_settings(config_path: Path | None = None) -> Settings

From Plan 2 (clients):
- GammaClient(settings) — async __aenter__/__aexit__; fetch_all_active_markets() -> list[dict]
- ClobReaderClient(settings) — get_books(token_ids) -> list[dict]; get_prices_buy_sell(token_ids) -> dict

From Plan 3 (storage + validator):
- DDL, SNAPSHOT_SCHEMA, MARKETS_INSERT_SQL, MARKETS_COLUMN_ORDER (schemas.py)
- SQLiteStore(db_path) — init_schema(); write_snapshot(...) -> int
- write_parquet_atomic(rows, path); compute_snapshot_path(root, taken_at_ms) -> Path
- Category, Issue (category.py)
- layer1_count, layer2_fields, layer4_cross_source, is_valid_overall (layers.py)

CLOB book dict shape (from Plan 2 SUMMARY):
- Token id field name: <as documented in 01-2-SUMMARY.md — likely "asset_id">
- Price/size types: probably str (per RESEARCH.md examples)
</interfaces>

## Goal

The orchestrator is the only place that knows about all subsystems. It calls clients, normalizes results, runs validators, computes is_valid, writes Parquet (atomic), then writes SQLite (single transaction). The CLI is a thin typer wrapper that parses flags and prints the summary line. The Makefile gives users a one-command entry point.

<tasks>

<task type="auto">
  <id>T1</id>
  <name>Task 1: Implement normalizer.py (Gamma raw dict → storage row dict)</name>
  <files>src/polyarb/snapshot/normalizer.py</files>
  <read_first>
    - .planning/workstreams/m1-perception/phases/01-/01-RESEARCH.md (Pitfall 2 — JSON string fields; Pitfall 3 — token_id is str; Open Q#5 — liquidity field name; Pattern 1 fetch_all spec)
    - .planning/workstreams/m1-perception/phases/01-/01-PATTERNS.md (Plan 2 — JSON-string field parsing pattern from btc_markets.py:99-109)
    - 3th-party/polymarket-kalshi-weather-bot/backend/data/btc_markets.py (lines 99-130 — JSON-string parsing reference)
    - src/polyarb/storage/schemas.py (MARKETS_COLUMN_ORDER — output contract)
    - src/polyarb/validator/category.py
  </read_first>
  <action>
    Create `src/polyarb/snapshot/normalizer.py` exporting one function `normalize_market`:

    Signature:
    ```python
    def normalize_market(raw: dict) -> dict | None:
        """Convert Gamma /markets raw response item to storage row dict.

        Returns None if market is unparseable beyond recovery (e.g., missing market_id).
        Returns dict whose keys are a SUPERSET of MARKETS_COLUMN_ORDER minus 'snapshot_id'
        and minus the CLOB-derived fields (best_bid_*, best_ask_*, fetched_at_ms) which
        the orchestrator attaches later.
        """
    ```

    Implementation requirements:
    - Imports: `import json`, `from datetime import datetime, timezone`, `from loguru import logger`
    - Parse `clobTokenIds` field (Gamma returns JSON string per Pitfall 2):
      ```python
      raw_token_ids = raw.get("clobTokenIds") or "[]"
      try:
          token_list = json.loads(raw_token_ids) if isinstance(raw_token_ids, str) else raw_token_ids
      except (json.JSONDecodeError, TypeError):
          token_list = []
      yes_token_id = str(token_list[0]) if len(token_list) > 0 else None
      no_token_id  = str(token_list[1]) if len(token_list) > 1 else None
      ```
      KEEP as string — uint256 cannot fit in int64.
    - Parse `outcomePrices` field (also JSON string) for `mid_price`:
      ```python
      raw_prices = raw.get("outcomePrices") or "[]"
      try:
          price_list = json.loads(raw_prices) if isinstance(raw_prices, str) else raw_prices
          mid_price = float(price_list[0]) if len(price_list) > 0 else None
      except (json.JSONDecodeError, TypeError, ValueError):
          mid_price = None
      ```
    - Parse liquidity per RESEARCH.md Open Q#5 — try `liquidityNum` first, fall back to `liquidity`:
      ```python
      liq_raw = raw.get("liquidityNum")
      if liq_raw is None:
          liq_raw = raw.get("liquidity")
      try:
          liquidity_usd = float(liq_raw) if liq_raw is not None else None
      except (TypeError, ValueError):
          liquidity_usd = None
      ```
    - Volume similarly: try `volumeNum`, then `volume`
    - Parse `endDate` (ISO string) → epoch ms:
      ```python
      end_iso = raw.get("endDate") or raw.get("end_date_iso")
      end_time_ms = None
      if end_iso:
          try:
              dt = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
              if dt.tzinfo is None:
                  dt = dt.replace(tzinfo=timezone.utc)
              end_time_ms = int(dt.timestamp() * 1000)
          except (ValueError, TypeError):
              end_time_ms = None
      ```
    - Booleans: `active`, `closed`, `negRisk` — coerce to Python bool (orchestrator/SQLite store will handle int conversion)
    - Required: `market_id` MUST be present (use `raw.get("id")`); if missing, log warning + return None
    - Output dict (every key listed; missing CLOB-derived fields are None placeholders the orchestrator overwrites):
      ```python
      return {
          "market_id":          str(raw["id"]),
          "condition_id":       str(raw.get("conditionId") or ""),
          "slug":               raw.get("slug"),
          "question":           raw.get("question"),
          "yes_token_id":       yes_token_id,
          "no_token_id":        no_token_id,
          "mid_price":          mid_price,
          "liquidity_usd":      liquidity_usd,
          "volume_usd":         volume_usd,
          "best_bid_price":     None,  # filled by orchestrator after CLOB fetch
          "best_bid_size":      None,
          "best_ask_price":     None,
          "best_ask_size":      None,
          "end_time_ms":        end_time_ms,
          "active":             bool(raw.get("active", False)),
          "closed":             bool(raw.get("closed", False)),
          "neg_risk":           bool(raw.get("negRisk", False)),
          "neg_risk_market_id": raw.get("negRiskMarketID"),
          "fetched_at_ms":      None,  # set by orchestrator after CLOB fetch completes
          "incomplete":         False,
      }
      ```
    - DO NOT add the `snapshot_id` key — SQLiteStore.write_snapshot injects it.

    Top docstring: "Per Pitfall 2 of RESEARCH.md, Gamma returns clobTokenIds and outcomePrices as JSON-encoded strings, not lists. We json.loads them here. Per Pitfall 3, token IDs stay as str — uint256 cannot fit in int64."
  </action>
  <verify>
    <automated>cd /Users/sujiangwen/sandbox/hacker2026/PolyMarket/polymarket-arbitrage && python -c "
from polyarb.snapshot.normalizer import normalize_market
import json
# Mimicking Gamma response shape (Pitfall 2: JSON-string fields)
raw = {
    'id': 'M-1',
    'conditionId': '0xabc',
    'slug': 'test-market',
    'question': 'Will X happen?',
    'clobTokenIds': '[\"' + '1'*70 + '\", \"' + '2'*70 + '\"]',
    'outcomePrices': '[\"0.55\", \"0.45\"]',
    'liquidityNum': 1500.5,
    'volumeNum': 50000.0,
    'endDate': '2026-12-31T00:00:00Z',
    'active': True,
    'closed': False,
    'negRisk': False,
}
n = normalize_market(raw)
assert n['market_id'] == 'M-1'
assert n['yes_token_id'] == '1'*70 and isinstance(n['yes_token_id'], str)
assert n['no_token_id'] == '2'*70
assert n['mid_price'] == 0.55
assert n['liquidity_usd'] == 1500.5
assert n['end_time_ms'] is not None and n['end_time_ms'] > 1_700_000_000_000
assert n['best_bid_price'] is None  # set later
assert n['fetched_at_ms'] is None
# Missing market_id returns None
assert normalize_market({}) is None
print('NORMALIZER_OK')
"</automated>
  </verify>
  <done>normalize_market handles all Gamma JSON-string fields; token_id stays as str; mid_price extracted from outcomePrices[0]; liquidityNum preferred over liquidity; endDate ISO → epoch ms; missing market_id returns None; CLOB-derived fields are None placeholders</done>
</task>

<task type="auto">
  <id>T2</id>
  <name>Task 2: Implement orchestrator.py (7-step async pipeline)</name>
  <files>src/polyarb/snapshot/orchestrator.py</files>
  <read_first>
    - .planning/workstreams/m1-perception/phases/01-/01-RESEARCH.md (Atomic SQLite + Parquet 编排 lines 720-770 — the spec; Pitfall 6 — fetched_at_ms; Pattern 2 — get_books vs get_prices_buy_sell shape)
    - .planning/workstreams/m1-perception/phases/01-/01-PATTERNS.md (Plan 4 — orchestrator)
    - .planning/workstreams/m1-perception/phases/01-/01-2-SUMMARY.md (CLOB book dict token id field name — RESOLVED)
    - .planning/workstreams/m1-perception/phases/01-/01-3-SUMMARY.md (final MARKETS_COLUMN_ORDER)
    - src/polyarb/snapshot/normalizer.py (T1)
    - src/polyarb/clients/gamma_client.py (Plan 2)
    - src/polyarb/clients/clob_client.py (Plan 2)
    - src/polyarb/storage/* (Plan 3)
    - src/polyarb/validator/* (Plan 3)
  </read_first>
  <action>
    Create `src/polyarb/snapshot/orchestrator.py` exporting:

    1. `@dataclass class SnapshotResult`:
       - `snapshot_id: int`
       - `market_count: int`
       - `is_valid: bool`
       - `mode: str`
       - `issue_count: int`
       - `issue_categories: dict[str, int]`  # {category_value: count}
       - `parquet_path: Path`
       - `taken_at_ms: int`
       - `finished_at_ms: int`

    2. `async def run_snapshot(settings: Settings, *, mode: str = "subset", now_ms: int | None = None) -> SnapshotResult`:

       Required structure (number every step in code comments per RESEARCH.md spec):

       ```python
       async def run_snapshot(settings: Settings, *, mode: str = "subset", now_ms: int | None = None) -> SnapshotResult:
           assert mode in ("subset", "full"), f"invalid mode: {mode}"
           taken_at_ms = now_ms if now_ms is not None else int(time.time() * 1000)
           issues: list[Issue] = []
           gamma_count_reported: int | None = None
           markets: list[dict] = []
           books_by_token: dict[str, dict] = {}
           prices_buy: dict = {}
           prices_sell: dict = {}

           # 1. Fetch Gamma full list (with API_UNREACHABLE fallback)
           async with GammaClient(settings) as gamma:
               try:
                   raw_markets = await gamma.fetch_all_active_markets()
                   gamma_count_reported = len(raw_markets)
               except Exception as e:
                   logger.error(f"Gamma fetch failed after retries: {e}")
                   # F-5 SECURITY: cap exception detail to 200 chars (4xx body could be huge HTML)
                   issues.append(Issue(layer=1, category=Category.API_UNREACHABLE,
                                       market_id=None, detail=f"Gamma unreachable: {str(e)[:200]}"))
                   raw_markets = []

           # 2. Normalize (drop None returns)
           markets = [m for m in (normalize_market(r) for r in raw_markets) if m is not None]

           # 3. Mode filter → token list
           if mode == "subset":
               target_markets = [m for m in markets
                                 if (m.get("liquidity_usd") or 0) > settings.liquidity_threshold_usd]
           else:
               target_markets = markets
           token_ids: list[str] = []
           for m in target_markets:
               for k in ("yes_token_id", "no_token_id"):
                   if m.get(k):
                       token_ids.append(m[k])
           logger.info(f"Mode={mode}: {len(target_markets)}/{len(markets)} markets, {len(token_ids)} tokens to fetch from CLOB")

           # 4. CLOB batch fetch
           clob = ClobReaderClient(settings)
           try:
               books = await clob.get_books(token_ids)
               prices = await clob.get_prices_buy_sell(token_ids)
               prices_buy = prices.get("buy", {})
               prices_sell = prices.get("sell", {})
               # Index books by token id (key name from 01-2-SUMMARY)
               for b in books:
                   tid = b.get("asset_id") or b.get("market") or b.get("token_id")
                   if tid:
                       books_by_token[str(tid)] = b
           except Exception as e:
               logger.error(f"CLOB fetch failed: {e}")
               # F-5 SECURITY: cap exception detail to 200 chars
               issues.append(Issue(layer=4, category=Category.API_UNREACHABLE,
                                   market_id=None, detail=f"CLOB unreachable: {str(e)[:200]}"))

           # 5. Stamp fetched_at_ms + attach top-of-book to each market row
           clob_done_ms = int(time.time() * 1000)
           for m in markets:
               m["fetched_at_ms"] = clob_done_ms
               for token_field, side in [("yes_token_id", "yes")]:
                   pass  # not used; top-of-book attaches per token below
           for m in markets:
               # Use yes_token_id top-of-book (convention: row stores YES side; NO side is symmetric on Polymarket)
               # Phase 1 simplification: attach top-of-book for whichever token is present.
               for tf in ("yes_token_id",):
                   tid = m.get(tf)
                   if tid and tid in books_by_token:
                       book = books_by_token[tid]
                       asks = book.get("asks") or []
                       bids = book.get("bids") or []
                       # F-1 SECURITY: CLOB book is attacker-controlled external input.
                       # A malformed price/size string (NaN, missing key, null) must NOT crash
                       # the snapshot — log as Issue(layer=4, category=UNKNOWN) and continue.
                       # Honors D-D3 (校验失败仍落库). raw_payload truncated to 500 bytes (F-5).
                       if asks:
                           try:
                               m["best_ask_price"] = float(asks[0]["price"])
                               m["best_ask_size"]  = float(asks[0]["size"])
                           except (KeyError, TypeError, ValueError, IndexError) as e:
                               issues.append(Issue(
                                   layer=4, category=Category.UNKNOWN,
                                   market_id=m.get("market_id"),
                                   detail=f"unparseable ask for {tid}: {str(e)[:200]}",
                                   raw_payload=json.dumps(book, default=str)[:500],
                               ))
                       if bids:
                           try:
                               m["best_bid_price"] = float(bids[0]["price"])
                               m["best_bid_size"]  = float(bids[0]["size"])
                           except (KeyError, TypeError, ValueError, IndexError) as e:
                               issues.append(Issue(
                                   layer=4, category=Category.UNKNOWN,
                                   market_id=m.get("market_id"),
                                   detail=f"unparseable bid for {tid}: {str(e)[:200]}",
                                   raw_payload=json.dumps(book, default=str)[:500],
                               ))

           # 6. Run validators
           if gamma_count_reported is not None:
               issues.extend(layer1_count(gamma_count_reported, len(markets)))
           issues.extend(layer2_fields(markets, now_ms=taken_at_ms))
           # Combine prices_buy + prices_sell into one map for layer4 reference
           prices_combined = {tid: {"buy": prices_buy.get(tid), "sell": prices_sell.get(tid)}
                              for tid in {**prices_buy, **prices_sell}}
           issues.extend(layer4_cross_source(markets, books_by_token, prices_combined))
           is_valid = is_valid_overall(issues)

           # 7. Write Parquet (atomic) THEN SQLite (transactional)
           finished_at_ms = int(time.time() * 1000)
           parquet_path = compute_snapshot_path(settings.parquet_root, taken_at_ms)
           # Add 2 parquet-only fields to each row
           parquet_rows = []
           for m in markets:
               row = dict(m)
               row["snapshot_taken_at_ms"] = taken_at_ms
               row["snapshot_id"] = 0  # placeholder (parquet doesn't have FK; will be overwritten below if needed)
               parquet_rows.append(row)
           write_parquet_atomic(parquet_rows, parquet_path)

           store = SQLiteStore(settings.db_path)
           store.init_schema()
           snapshot_id = store.write_snapshot(
               taken_at_ms=taken_at_ms,
               finished_at_ms=finished_at_ms,
               mode=mode,
               parquet_path=str(parquet_path),
               is_valid=is_valid,
               market_rows=markets,
               issues=issues,
           )

           # Aggregate issues by category for summary
           cat_counts: dict[str, int] = {}
           for i in issues:
               cat_counts[i.category.value] = cat_counts.get(i.category.value, 0) + 1

           return SnapshotResult(
               snapshot_id=snapshot_id,
               market_count=len(markets),
               is_valid=is_valid,
               mode=mode,
               issue_count=len(issues),
               issue_categories=cat_counts,
               parquet_path=parquet_path,
               taken_at_ms=taken_at_ms,
               finished_at_ms=finished_at_ms,
           )
       ```

    Imports required:
    - `from __future__ import annotations`
    - `import time`
    - `from dataclasses import dataclass`
    - `from pathlib import Path`
    - `from loguru import logger`
    - `from polyarb.config import Settings`
    - `from polyarb.clients.gamma_client import GammaClient`
    - `from polyarb.clients.clob_client import ClobReaderClient`
    - `from polyarb.snapshot.normalizer import normalize_market`
    - `from polyarb.storage.sqlite_store import SQLiteStore`
    - `from polyarb.storage.parquet_writer import write_parquet_atomic, compute_snapshot_path`
    - `from polyarb.validator.category import Category, Issue`
    - `from polyarb.validator.layers import layer1_count, layer2_fields, layer4_cross_source, is_valid_overall`

    CRITICAL: this function MUST handle GammaClient + CLOB failure as "record issue, continue, write snapshot with is_valid=false" — NEVER `sys.exit` from here, NEVER raise. The CLI is responsible for exit code based on `result.is_valid`.

    Note on book-row mapping: Phase 1 attaches top-of-book using ONLY `yes_token_id` (for Layer 4 ghost-book detection both YES and NO are checked separately — that's the validator's job). The single `best_*` columns in the markets table represent the YES side. This is a documented limitation; Phase 3 may add separate `best_no_*` columns when needed.
  </action>
  <verify>
    <automated>cd /Users/sujiangwen/sandbox/hacker2026/PolyMarket/polymarket-arbitrage && python -c "
from polyarb.snapshot.orchestrator import run_snapshot, SnapshotResult
import inspect
sig = inspect.signature(run_snapshot)
assert 'mode' in sig.parameters
assert 'now_ms' in sig.parameters
assert sig.parameters['mode'].default == 'subset'
fields = {f.name for f in __import__('dataclasses').fields(SnapshotResult)}
expected = {'snapshot_id','market_count','is_valid','mode','issue_count','issue_categories','parquet_path','taken_at_ms','finished_at_ms'}
assert expected <= fields, f'missing: {expected - fields}'
print('ORCHESTRATOR_OK')
"</automated>
  </verify>
  <done>run_snapshot signature accepts settings + mode + now_ms; SnapshotResult has all 9 fields; imports all subsystems; module imports cleanly</done>
</task>

<task type="auto">
  <id>T3</id>
  <name>Task 3: Implement cli.py (typer app + snapshot subcommand)</name>
  <files>src/polyarb/snapshot/cli.py</files>
  <read_first>
    - src/polyarb/snapshot/orchestrator.py (T2)
    - src/polyarb/config.py (Plan 1)
    - .planning/workstreams/m1-perception/phases/01-/01-CONTEXT.md (D-F1, D-F2, D-F3 — CLI output spec)
  </read_first>
  <action>
    Create `src/polyarb/snapshot/cli.py`:

    ```python
    """polyarb CLI: snapshot subcommand."""
    from __future__ import annotations

    import asyncio
    import sys
    from pathlib import Path

    import typer
    from loguru import logger

    from polyarb.config import load_settings
    from polyarb.snapshot.orchestrator import run_snapshot

    app = typer.Typer(no_args_is_help=True, add_completion=False)


    @app.command()
    def snapshot(
        full: bool = typer.Option(False, "--full", help="Fetch top-of-book for ALL markets (slower)."),
        verbose: bool = typer.Option(False, "--verbose", "-v", help="Show progress + phase timings."),
        config: Path | None = typer.Option(None, "--config", help="Path to YAML config (overrides config/snapshot.yaml)."),
    ) -> None:
        """Capture a one-shot Polymarket market snapshot."""
        # Logger level
        logger.remove()
        logger.add(sys.stderr, level="DEBUG" if verbose else "INFO",
                   format="<level>{level:<7}</level> | {message}")

        settings = load_settings(config)
        mode = "full" if full else "subset"

        result = asyncio.run(run_snapshot(settings, mode=mode))

        # D-F1: single-line summary on success
        status = "OK" if result.is_valid else "INVALID"
        summary = (f"{status} | {result.market_count} markets | mode={result.mode} | "
                   f"{result.issue_count} issues | -> {result.parquet_path}")
        print(summary)

        # D-F3: stderr summary when invalid
        if not result.is_valid:
            print("---", file=sys.stderr)
            print(f"VALIDATION FAILED: snapshot_id={result.snapshot_id}", file=sys.stderr)
            print(f"Issues by category:", file=sys.stderr)
            for cat, n in sorted(result.issue_categories.items(), key=lambda x: -x[1]):
                print(f"  {cat}: {n}", file=sys.stderr)
            sys.exit(1)
        sys.exit(0)


    if __name__ == "__main__":
        app()
    ```

    Critical:
    - `app` is a top-level module attribute (referenced by `[project.scripts] polyarb = "polyarb.cli:app"` ... but our entry is `polyarb.snapshot.cli`. Update: the pyproject.toml from Plan 1 declared `polyarb = "polyarb.cli:app"`. We need to either (a) update pyproject to point to `polyarb.snapshot.cli:app` OR (b) create a thin `src/polyarb/cli.py` that re-exports `app` from `snapshot.cli`. Choose (b) for simplicity — see T4).
    - `asyncio.run` is called once; orchestrator is fully async.
    - Default mode = subset (D-A2).
    - Exit code: 0 if valid, 1 if invalid (D-F3).
    - Verbose mode adds DEBUG logs to stderr; default INFO.
    - `print(summary)` goes to stdout (the cron-grep convention: ok-line on stdout, error detail on stderr).
  </action>
  <verify>
    <automated>cd /Users/sujiangwen/sandbox/hacker2026/PolyMarket/polymarket-arbitrage && python -c "from polyarb.snapshot.cli import app; assert app is not None; print('CLI_OK')" && python -m polyarb.snapshot --help 2>&1 | grep -qi "snapshot" && echo HELP_OK</automated>
  </verify>
  <done>cli.py exports `app`; `python -m polyarb.snapshot --help` works (depends on T4 __main__.py); snapshot command accepts --full, --verbose, --config flags</done>
</task>

<task type="auto">
  <id>T4</id>
  <name>Task 4: Create __main__.py + top-level cli.py shim (entry-point glue)</name>
  <files>
    src/polyarb/snapshot/__main__.py,
    src/polyarb/cli.py
  </files>
  <read_first>
    - src/polyarb/snapshot/cli.py (T3)
    - pyproject.toml (declares `polyarb = "polyarb.cli:app"` per Plan 1)
  </read_first>
  <action>
    Create `src/polyarb/snapshot/__main__.py`:
    ```python
    """Entry for `python -m polyarb.snapshot`."""
    from polyarb.snapshot.cli import app

    if __name__ == "__main__":
        app()
    ```

    Create `src/polyarb/cli.py` (top-level shim — pyproject.toml [project.scripts] points here):
    ```python
    """Top-level CLI entry. Re-exports the snapshot app so `polyarb` console script works.

    Future subcommands (scan-arb, watch-orderbook) will be registered on this app.
    """
    from polyarb.snapshot.cli import app

    __all__ = ["app"]
    ```

    Both files together enable two invocation paths (resolved Q7):
    - `polyarb snapshot --full` (via console script declared in pyproject.toml)
    - `python -m polyarb.snapshot --full` (via __main__.py — matches CONTEXT.md MK1/MK2 examples)
  </action>
  <verify>
    <automated>cd /Users/sujiangwen/sandbox/hacker2026/PolyMarket/polymarket-arbitrage && python -m polyarb.snapshot --help 2>&1 | grep -qi "snapshot" && python -c "from polyarb.cli import app; assert app is not None; print('SHIM_OK')" && which polyarb && polyarb --help 2>&1 | grep -qi "snapshot" && echo CONSOLE_SCRIPT_OK</automated>
  </verify>
  <done>`python -m polyarb.snapshot --help` works; `polyarb --help` console script works (after `pip install -e .` re-applied if needed); both paths point to the same typer app</done>
</task>

<task type="auto">
  <id>T5</id>
  <name>Task 5: Add Makefile targets (snapshot-markets + snapshot-markets-full)</name>
  <files>Makefile</files>
  <read_first>
    - Makefile (current contents — preserve all existing targets, replace the commented placeholder block)
    - .planning/workstreams/m1-perception/phases/01-/01-CONTEXT.md (D-MK1, D-MK2, D-MK3 — naming + doc convention)
    - CLAUDE.md (Makefile rule: every target needs `## name: description` doc comment for `make help`)
  </read_first>
  <action>
    Edit `Makefile`. Replace the existing commented placeholder block at lines 41-49:
    ```
    # ─────────────────────────────────────────────────────────────────────────────
    # Phase commands (populated as phases are implemented)
    # ─────────────────────────────────────────────────────────────────────────────

    # M1-P01 targets will be added here after discuss/plan completes.
    # Example placeholder:
    #   ## snapshot-markets: Capture full Polymarket market snapshot to parquet
    #   snapshot-markets:
    #       python -m polymarket.snapshot --output data/snapshots/$(shell date +%Y-%m-%dT%H%M).parquet
    ```

    With:
    ```
    # ─────────────────────────────────────────────────────────────────────────────
    # M1-perception Phase 01: market snapshot tool
    # ─────────────────────────────────────────────────────────────────────────────

    .PHONY: snapshot-markets snapshot-markets-full

    ## snapshot-markets: Capture Polymarket snapshot (subset mode, liquidity > $1k, ~10-20 min)
    snapshot-markets:
    	python -m polyarb.snapshot

    ## snapshot-markets-full: Capture Polymarket snapshot (FULL mode, all markets, ~1-2 hours)
    snapshot-markets-full:
    	python -m polyarb.snapshot --full
    ```

    CRITICAL: Use TAB characters (NOT spaces) for the recipe lines (`python -m polyarb.snapshot`) — Makefile syntax requires this. The Edit/Write tool MUST preserve tabs.

    Add `.PHONY: snapshot-markets snapshot-markets-full` so make doesn't get confused if files of those names appear.

    Verify `make help` parses both new targets correctly (it greps `^## ` lines).

    Do NOT add a `snapshot-markets-verbose` target — `--verbose` is a per-invocation flag, not a separate command (avoid Makefile target explosion).

    Do NOT touch the existing `help`/`status`/`journal` targets.
  </action>
  <verify>
    <automated>cd /Users/sujiangwen/sandbox/hacker2026/PolyMarket/polymarket-arbitrage && make -n snapshot-markets 2>&1 | grep -q "python -m polyarb.snapshot" && make -n snapshot-markets-full 2>&1 | grep -q "python -m polyarb.snapshot --full" && make help 2>&1 | grep -q "snapshot-markets:" && make help 2>&1 | grep -q "snapshot-markets-full:" && grep -P "^\tpython -m polyarb.snapshot" Makefile && echo MAKEFILE_OK</automated>
  </verify>
  <done>Makefile has snapshot-markets + snapshot-markets-full targets with TAB-indented recipes; both appear in `make help`; `make -n snapshot-markets` shows correct command; existing targets preserved</done>
</task>

<task type="auto">
  <id>T6</id>
  <name>Task 6: Smoke verify all wiring (CLI invokes orchestrator path end-to-end with mocked clients)</name>
  <files></files>
  <read_first>
    - all Plan 4 outputs (T1-T5)
    - tests/m1-perception/fixtures/gamma_sample.json + clob_sample.json (Plan 2 T1)
  </read_first>
  <action>
    NO new file. This is a no-network smoke verification step.

    Run a short Python check that imports everything Plan 4 created and confirms the wiring graph is complete:

    ```bash
    cd /Users/sujiangwen/sandbox/hacker2026/PolyMarket/polymarket-arbitrage && python -c "
    # All Plan 4 modules import
    from polyarb.snapshot.normalizer import normalize_market
    from polyarb.snapshot.orchestrator import run_snapshot, SnapshotResult
    from polyarb.snapshot.cli import app
    from polyarb.cli import app as top_app
    # CLI 'app' exists; typer Typer instance has registered commands
    assert app is top_app, 'CLI shim mismatch'
    cmds = [c.name for c in app.registered_commands]
    assert 'snapshot' in cmds, f'snapshot command not registered: {cmds}'
    print('WIRING_OK')
    "
    ```

    Then run `make -n snapshot-markets` and `make -n snapshot-markets-full` to confirm Makefile targets resolve.

    Then run `python -m polyarb.snapshot --help` and `polyarb snapshot --help` — both must show typer help text with `--full`, `--verbose`, `--config` flags listed.

    DO NOT actually invoke `make snapshot-markets` (it would hit live APIs). Plan 5 covers integration testing with mocks.

    If any step fails, fix the underlying file (do NOT add workarounds in this task).
  </action>
  <verify>
    <automated>cd /Users/sujiangwen/sandbox/hacker2026/PolyMarket/polymarket-arbitrage && python -c "from polyarb.snapshot.orchestrator import run_snapshot; from polyarb.snapshot.cli import app; from polyarb.cli import app as top_app; assert app is top_app; cmds=[c.name for c in app.registered_commands]; assert 'snapshot' in cmds; print('WIRING_OK')" && python -m polyarb.snapshot --help 2>&1 | grep -q -- "--full" && python -m polyarb.snapshot --help 2>&1 | grep -q -- "--verbose" && python -m polyarb.snapshot --help 2>&1 | grep -q -- "--config" && make -n snapshot-markets 2>&1 | grep -q "polyarb.snapshot$" && echo SMOKE_OK</automated>
  </verify>
  <done>All modules import; `app` is shared between top-level and snapshot CLI; `snapshot` command is registered; --full/--verbose/--config flags appear in --help; both Makefile targets resolve to correct python invocations</done>
</task>

</tasks>

## Verification

```bash
# Module wiring
python -c "from polyarb.snapshot.orchestrator import run_snapshot, SnapshotResult; from polyarb.snapshot.cli import app; from polyarb.snapshot.normalizer import normalize_market; print('OK')"

# CLI surface
python -m polyarb.snapshot --help
polyarb snapshot --help  # console script
make help | grep snapshot

# Makefile dry-runs
make -n snapshot-markets
make -n snapshot-markets-full
```

## Success Criteria

- 4 source files exist + Makefile updated
- `python -m polyarb.snapshot --help` and `polyarb snapshot --help` both succeed
- Makefile shows both new targets in `make help`
- All previous tests (Plan 1, 2, 3) still pass: `pytest tests/m1-perception -xvs --ignore=tests/m1-perception/test_integration.py`
- Wiring smoke check passes (T6)

## must_haves (this plan delivers)

- Phase outcomes 1, 2 (CLI commands working with mocks via Plan 5), 7 (per-row fetched_at_ms in orchestrator), 10 (Makefile entry per CLAUDE.md rule)

<output>
Create `.planning/workstreams/m1-perception/phases/01-/01-4-SUMMARY.md` documenting:
- Confirmed `app` registration shape (typer Typer object — was `[c.name for c in app.registered_commands]` the right introspection? document the actual API used)
- Whether `polyarb` console script picked up edits without re-running `pip install -e .` (answer: needs reinstall if pyproject changed)
- Any issues with normalizer regarding fields not present in the recorded gamma_sample.json (Open Q#5 follow-up)
- The actual CLOB book token-id key used in the orchestrator's `books_by_token` indexing (after reading 01-2-SUMMARY)
</output>
