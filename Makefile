# Polymarket Arbitrage — Unified command entry point
#
# Convention: all executable commands are exposed here. Users should never
# need to remember `python -m xxx --flag a --flag b` style invocations.
#
# Naming: make <verb>-<noun>, e.g., make snapshot-markets, make scan-arb
# Each target above has a comment explaining purpose and typical scenario.
# `make help` always lists all available commands.
#
# Package manager: uv (https://github.com/astral-sh/uv).
# `uv run` auto-syncs the lockfile and runs in the project venv — no manual
# `source .venv/bin/activate` needed. To bootstrap: `uv sync --extra dev`.

.DEFAULT_GOAL := help
.PHONY: help test

# ─────────────────────────────────────────────────────────────────────────────
# Meta
# ─────────────────────────────────────────────────────────────────────────────

## help: List all available commands with descriptions
help:
	@echo "Polymarket Arbitrage — Available commands"
	@echo ""
	@echo "Usage: make <target>"
	@echo ""
	@grep -E '^## ' $(MAKEFILE_LIST) | sed -E 's/^## /  /' | sort

# ─────────────────────────────────────────────────────────────────────────────
# Project state (gsd-aware shortcuts)
# ─────────────────────────────────────────────────────────────────────────────

## status: Show current project state (milestone / phase / next action)
status:
	@echo "=== Project Status ==="
	@grep -A 5 "## 当前状态" CLAUDE.md | head -10
	@echo ""
	@echo "=== Recent Journal Entries ==="
	@tail -20 .planning/JOURNAL.md

## journal: Open the project journal (activity timeline)
journal:
	@$${EDITOR:-cat} .planning/JOURNAL.md

## planning-status: Audit .planning/ vs git — surface "code shipped, doc didn't follow" drift
planning-status:
	@uv run python scripts/planning_status.py

# ─────────────────────────────────────────────────────────────────────────────
# M1-perception Phase 01: market snapshot tool
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: snapshot-markets snapshot-markets-v snapshot-markets-full snapshot-markets-full-v snapshot-status snapshot-fresh snapshots-purge snapshot-cache-purge

## snapshot-markets: Capture snapshot (subset, liquidity > $1k, ~15-30 min). Quiet, cron-friendly. Auto-loads .env for Supabase+R2 mirror.
snapshot-markets:
	@set -a; [ -f .env ] && . ./.env; set +a; \
	echo ">> snapshot-markets (quiet mode) — PID $$$$ — started $$(date '+%Y-%m-%d %H:%M:%S')"; \
	echo ">> tip: open another terminal and run 'make snapshot-status' to check progress"; \
	echo ""; \
	uv run python -m polyarb.snapshot snapshot

## snapshot-markets-v: Same as snapshot-markets but with progress logs (recommended for interactive runs)
snapshot-markets-v:
	@set -a; [ -f .env ] && . ./.env; set +a; \
	echo ">> snapshot-markets-v (verbose mode) — PID $$$$ — started $$(date '+%Y-%m-%d %H:%M:%S')"; \
	echo ""; \
	uv run python -m polyarb.snapshot snapshot --verbose

## snapshot-markets-full: Capture snapshot (FULL mode, all markets, ~1-2 hours). Quiet.
snapshot-markets-full:
	@set -a; [ -f .env ] && . ./.env; set +a; \
	echo ">> snapshot-markets-full (quiet mode) — PID $$$$ — started $$(date '+%Y-%m-%d %H:%M:%S')"; \
	echo ">> tip: this may take 1-2 hours. Use 'make snapshot-status' to check progress."; \
	echo ""; \
	uv run python -m polyarb.snapshot snapshot --full

## snapshot-markets-full-v: FULL mode with progress logs
snapshot-markets-full-v:
	@set -a; [ -f .env ] && . ./.env; set +a; \
	echo ">> snapshot-markets-full-v (verbose mode) — PID $$$$ — started $$(date '+%Y-%m-%d %H:%M:%S')"; \
	echo ""; \
	uv run python -m polyarb.snapshot snapshot --full --verbose

## snapshot-status: One-glance status — running process, recent SQLite rows, latest parquet (local time)
snapshot-status:
	@uv run python scripts/snapshot_status.py

## snapshot-fresh: Force full refetch (purge all caches, then run verbose)
snapshot-fresh:
	@echo ">> snapshot-fresh — purging caches, then verbose run"
	@echo ""
	uv run python -m polyarb.snapshot snapshot --no-cache --verbose

## snapshots-purge: Delete old snapshots (SQLite + Parquet). Usage: make snapshots-purge [DAYS=7] [KEEP=5]
snapshots-purge:
	uv run python -m polyarb.snapshot snapshots-purge --older-than-days $(or $(DAYS),7) --keep-last $(or $(KEEP),5) --verbose

## snapshot-cache-purge: Delete all data/.cache/snapshot-* directories without running
snapshot-cache-purge:
	@uv run python -c "from pathlib import Path; from polyarb.snapshot.cache import ChunkCache; n = ChunkCache.purge_all(Path('data/.cache')); print(f'purged {n} cache directories')"

# ─────────────────────────────────────────────────────────────────────────────
# M1-perception Phase 01.1: translation
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: translate-pending translate-pending-sample translation-stats

## translate-pending: Translate all markets missing Chinese translation. FORCE=1 to skip sample-first guard.
translate-pending:
	uv run python -m polyarb.cli_translation translate-pending --verbose $(if $(FORCE),--force-full,)

## translate-pending-sample: Dry-run — translate 50 questions to verify .env config first
translate-pending-sample:
	uv run python -m polyarb.cli_translation translate-pending --limit 50 --verbose

## translation-stats: Show cumulative translation stats grouped by translator_model
translation-stats:
	uv run python -m polyarb.cli_translation translation-stats

# ─────────────────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
# M1-perception Phase 01.1: observation toolkit + translation
#
# Typical daily workflow:
#   1. make snapshot-markets-v          # 拿当天数据
#   2. make scan-near-end               # 找候选（或其它 scan-*）
#   3. make show-market slug=<X>        # 深看候选
#   4. (edit watchlist.yaml)            # 加入自选
#   5. make watchlist-alerts            # 查触发
#   6. make track-market slug=<X>       # 看时序
#   7. make compare-snapshots           # 对比漂移
#
# See docs/learning/07-观察市场.md for detailed mental model.
#
# Plans 02-05 出处。Phase 1.2 / Phase 2 添加新段时勿混入本段。
# ─────────────────────────────────────────────────────────────────────────────
# Recipe scans (plan 03) + cross-snapshot diff & tracker (plan 04) +
# show-market & watchlist (plan 05)

.PHONY: scan scan-thick-but-slippery scan-near-end scan-ghost-suspicious scan-coin-flip scan-neg-risk-incomplete scan-by-tag list-recipes scans-purge compare-snapshots track-market show-market watchlist overview watchlist-alerts

## scan: Generic recipe runner — usage: make scan name=<recipe>
scan:
	uv run python -m polyarb.cli_observation scan --name $(name) --verbose

## scan-thick-but-slippery: Trap markets — high liq ($100k+) but wide spread (>$0.10)
scan-thick-but-slippery:
	uv run python -m polyarb.cli_observation scan --name thick-but-slippery --verbose

## scan-near-end: Markets resolving within 72h — densest arbitrage windows
scan-near-end:
	uv run python -m polyarb.cli_observation scan --name near-end --verbose

## scan-ghost-suspicious: CLOB/Gamma cross-validation failures (ghost_book signal)
scan-ghost-suspicious:
	uv run python -m polyarb.cli_observation scan --name ghost-suspicious --verbose

## scan-coin-flip: High-uncertainty markets (mid 0.45-0.55, 7-day end window)
scan-coin-flip:
	uv run python -m polyarb.cli_observation scan --name coin-flip --verbose

## scan-neg-risk-incomplete: Neg-risk groups whose mid sum deviates from 1.0 by >0.02 (M2 arb signal)
scan-neg-risk-incomplete:
	uv run python -m polyarb.cli_observation scan --name neg-risk-incomplete --verbose

## scan-by-tag: Tag-level aggregates (market count / total liq / avg spread per tag)
scan-by-tag:
	uv run python -m polyarb.cli_observation scan --name by-tag --verbose

## list-recipes: Show all available scan recipes (builtin + user yaml)
list-recipes:
	uv run python -m polyarb.cli_observation list-recipes

## scans-purge: Delete data/scans/ parquet files older than 30 days
scans-purge:
	uv run python -m polyarb.cli_observation scans-purge --older-than-days 30

## compare-snapshots: Diff two snapshots (default: N-1 → N). Usage: make compare-snapshots [from=N to=M]
compare-snapshots:
	uv run python -m polyarb.cli_observation compare-snapshots $(if $(from),--from $(from)) $(if $(to),--to $(to)) --verbose

## track-market: Single market time-series across all snapshots. Usage: make track-market slug=<X>
track-market:
	uv run python -m polyarb.cli_observation track-market --slug $(slug) --verbose

## show-market: Full detail for one market (bilingual + time-dim + neg-risk siblings + 5-snapshot history). Usage: make show-market slug=<X>
show-market:
	uv run python -m polyarb.cli_observation show-market --slug $(slug) --verbose

## watchlist: List all markets in watchlist.yaml with current status
watchlist:
	uv run python -m polyarb.cli_observation watchlist --verbose

## overview: Market overview dashboard — one-glance whole-picture view
overview:
	uv run python -m polyarb.cli_observation overview --verbose

## watchlist-alerts: Check alert_when conditions and print triggered entries
watchlist-alerts:
	uv run python -m polyarb.cli_observation watchlist-alerts --verbose

# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: test test-m1 test-observation test-slippage

## test: Run all tests
test:
	@uv run pytest -v tests/

## test-m1: Run all M1 market perception tests
test-m1:
	@uv run pytest -v tests/m1-perception/

## test-observation: Run observation module tests (scanner / diff / tracker / show / watchlist / recipes / formatter)
test-observation:
	@uv run pytest -v tests/m1-perception/test_observation_*.py

## test-slippage: Run slippage model tests
test-slippage:
	@uv run pytest -v tests/models/test_slippage.py

# ─────────────────────────────────────────────────────────────────────────────
# Phase 02 Plan 01: triple-check gate (L11/S5 silent failure prevention)
# ─────────────────────────────────────────────────────────────────────────────

## triple-check: 跑 make snapshot-markets 全链路三重契约 (exit 0 ↔ SQLite row +1 ↔ parquet file landed)
.PHONY: triple-check
triple-check:
	@echo ">> triple-check — verifying make snapshot-markets does what it claims"
	@echo ">> closes LEARNINGS L11/S5 silent failure root cause"
	@echo ""
	bash tests/m1-perception/test_makefile_triple_check.sh

# ─────────────────────────────────────────────────────────────────────────────
# Phase 02 Plan 02: daemon HTTP server (local dev)
#
# daemon-run-local    — start daemon locally (blocks; Ctrl-C to stop)
# smoke-health-local  — hit /health once and print the JSON response
# tail-logs-local     — stream daemon stdout (for a separately launched daemon)
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: daemon-run-local smoke-health-local tail-logs-local

## daemon-run-local: Start the polyarb daemon locally on :19080 (HMAC-authenticated /scan + /health). Ctrl-C to stop. Override port via POLYARB_HTTP_PORT.
daemon-run-local:
	@set -a; [ -f .env ] && . ./.env; set +a; \
	PORT=$${POLYARB_HTTP_PORT:-19080}; \
	echo ">> daemon-run-local — starting on http://127.0.0.1:$$PORT"; \
	echo ">> endpoints: GET /health  POST /scan (requires X-Signature)"; \
	echo ">> Ctrl-C to stop"; \
	echo ""; \
	POLYARB_ALLOW_EMPTY_SECRET=1 uv run python -m polyarb.daemon.main

## smoke-health-local: Hit GET /health on running local daemon and print JSON response. Requires daemon-run-local in another terminal.
smoke-health-local:
	@set -a; [ -f .env ] && . ./.env; set +a; \
	PORT=$${POLYARB_HTTP_PORT:-19080}; \
	echo ">> smoke-health-local — GET http://127.0.0.1:$$PORT/health"; \
	echo ""; \
	curl -sf http://127.0.0.1:$$PORT/health | python3 -m json.tool

# ─────────────────────────────────────────────────────────────────────────────
# Phase 02 Plan 03: Supabase mirror + R2 archive
#
# supabase-migrate    — run Alembic upgrade head (requires POLYARB_SUPABASE_DB_DSN)
# supabase-reconcile  — compare SQLite vs Supabase mirror; push missing snapshots
# r2-list             — list R2 bucket objects (dev convenience)
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: supabase-migrate supabase-reconcile r2-list

## supabase-migrate: Run Alembic upgrade head against Supabase DSN (auto-loads .env if present)
supabase-migrate:
	@echo ">> supabase-migrate — alembic upgrade head"
	@set -a; [ -f .env ] && . ./.env; set +a; \
	if [ -z "$$POLYARB_SUPABASE_DB_DSN" ]; then echo "ERROR: POLYARB_SUPABASE_DB_DSN not set in .env or shell (W6: DB DSN distinct from REST URL POLYARB_SUPABASE_URL)"; exit 1; fi; \
	uv run alembic upgrade head

## supabase-reconcile: Compare SQLite vs Supabase and push any missing snapshots (auto-loads .env)
supabase-reconcile:
	@echo ">> supabase-reconcile — comparing local SQLite vs Supabase mirror"
	@set -a; [ -f .env ] && . ./.env; set +a; \
	uv run python scripts/supabase_seed.py reconcile

## r2-list: List objects in R2 bucket (auto-loads .env; requires POLYARB_R2_ENDPOINT/BUCKET/ACCESS_KEY_ID/SECRET_ACCESS_KEY)
r2-list:
	@set -a; [ -f .env ] && . ./.env; set +a; \
	echo ">> r2-list — listing R2 bucket $${POLYARB_R2_BUCKET:-polyarb-snapshots}"; \
	if [ -z "$$POLYARB_R2_ENDPOINT" ]; then echo "ERROR: POLYARB_R2_ENDPOINT not set in .env or shell"; exit 1; fi; \
	uv run python -c "import boto3, os; c = boto3.client('s3', endpoint_url=os.environ['POLYARB_R2_ENDPOINT'], aws_access_key_id=os.environ['POLYARB_R2_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['POLYARB_R2_SECRET_ACCESS_KEY'], region_name='auto'); resp = c.list_objects_v2(Bucket=os.environ.get('POLYARB_R2_BUCKET', 'polyarb-snapshots')); [print(o['Key']) for o in resp.get('Contents', [])]"

## tail-logs-local: Stream stdout of locally running daemon process. Usage: make tail-logs-local [PID=<pid>]
tail-logs-local:
	@echo ">> tail-logs-local — streaming daemon stdout (JSON lines)"
	@if [ -n "$(PID)" ]; then \
		tail -f /proc/$(PID)/fd/1 2>/dev/null || echo ">> note: /proc not available on macOS — use 'make daemon-run-local' directly (logs print to that terminal)"; \
	else \
		echo ">> tip: run 'make daemon-run-local' in one terminal; logs appear there in real-time"; \
		echo ">> alternative: make daemon-run-local 2>&1 | tee /tmp/polyarb-daemon.log"; \
	fi
