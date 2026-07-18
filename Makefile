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

.PHONY: docs-m1-check

## docs-m1-check: Offline verification of the M1 platform manual's commands, links, routes, health names, and readiness matrix
docs-m1-check:
	@uv run python scripts/check_m1_manual.py

# ─────────────────────────────────────────────────────────────────────────────
# Project state (gsd-aware shortcuts)
# ─────────────────────────────────────────────────────────────────────────────

## status: Show current project state (milestone / phase / next action)
status:
	@cat .planning/CURRENT.md
	@echo ""
	@echo "## 当前 checkout"
	@echo ""
	@echo "- branch: $$(git branch --show-current)"
	@echo "- commit: $$(git rev-parse --short HEAD)"
	@echo "- main..HEAD commits: $$(git rev-list --count main..HEAD 2>/dev/null || echo unknown)"
	@echo "- worktree: $$(if [ -z "$$(git status --short)" ]; then echo clean; else echo dirty; fi)"

## journal: Open the project journal (activity timeline)
journal:
	@$${EDITOR:-cat} .planning/JOURNAL.md

## planning-status: Audit .planning/ vs git — surface "code shipped, doc didn't follow" drift
planning-status:
	@uv run python scripts/planning_status.py

## climb-status: Show the generated autonomous research/development tree.
climb-status:
	@cat docs/status/climb/research-tree.md

## climb-cycle: Run one local climb quality-gate cycle (hypothesis=H-NNN required).
climb-cycle:
	@test -n "$(hypothesis)" || (echo "usage: make climb-cycle hypothesis=H-NNN" >&2; exit 2)
	@tools/climb/cycle.sh "$(hypothesis)"

## climb-check: Verify climb adapter contracts and deterministic state generation.
climb-check:
	@uv run pytest tests/climb -q

.PHONY: climb-status climb-cycle climb-check

## cleanup-worktrees: Dry-run stale Claude agent worktree cleanup; use apply=1 and audited discard_unmerged="branch ..." to mutate
cleanup-worktrees:
	@uv run python scripts/cleanup_agent_worktrees.py $(if $(apply),--apply,) $(foreach branch,$(discard_unmerged),--discard-unmerged $(branch))

## patch-gsd-worktree-cleanup: Harden installed GSD worktree lifecycle; use check=1 for verification only
patch-gsd-worktree-cleanup:
	@uv run python scripts/patch_gsd_worktree_cleanup.py $(if $(check),--check,)

.PHONY: cleanup-worktrees patch-gsd-worktree-cleanup

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

## scan-arb: Find executable neg-risk buy-all bundles in an M1 SQLite snapshot. Usage: make scan-arb [db=data/state.db] [min_edge_bps=0]
scan-arb:
	uv run python -m polyarb.cli_arbitrage scan --db-path "$(or $(db),data/state.db)" --min-edge-bps "$(or $(min_edge_bps),0)"

## scan-arb-live: Query the fresh production M1→M2 opportunity feed. Usage: make scan-arb-live [min_edge_bps=0]
scan-arb-live:
	@curl -fsS "https://polyarb-l1.fly.dev/arbitrage/opportunities?min_edge_bps=$(or $(min_edge_bps),0)" | uv run python -m json.tool

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

.PHONY: daemon-run-local smoke-health-local smoke-health-prod smoke-healthz tail-logs-local

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

## smoke-health-prod: Read-only GET of prod L1 strict /health; prints HTTP status and JSON body
smoke-health-prod:
	@BODY=$$(mktemp); trap 'rm -f "$$BODY"' EXIT; \
	URL="https://polyarb-l1.fly.dev/health"; \
	echo ">> smoke-health-prod — GET $$URL"; \
	HTTP_STATUS=$$(curl --disable --request GET -sS -o "$$BODY" -w "%{http_code}" "$$URL") || { rc=$$?; echo "FAIL: request error" >&2; exit $$rc; }; \
	echo "HTTP $$HTTP_STATUS"; \
	python3 -m json.tool < "$$BODY" || cat "$$BODY"; \
	if [ "$$HTTP_STATUS" = "200" ]; then echo "PASS: L1 strict /health returned 200"; else echo "FAIL: L1 strict /health returned $$HTTP_STATUS" >&2; exit 1; fi

## smoke-healthz: Verify prod /healthz always returns 200 (Fly probe target — D-05). No auth required.
smoke-healthz:
	@echo ">> smoke-healthz — GET https://polyarb-l1.fly.dev/healthz"
	@STATUS=$$(curl -s -o /tmp/healthz_body.json -w "%{http_code}" https://polyarb-l1.fly.dev/healthz); \
	echo "HTTP $$STATUS"; \
	cat /tmp/healthz_body.json | python3 -m json.tool; \
	if [ "$$STATUS" = "200" ]; then echo "PASS: /healthz returned 200"; else echo "FAIL: expected 200 got $$STATUS"; exit 1; fi

# ─────────────────────────────────────────────────────────────────────────────
# Phase 03 Plan 03: L2 daemon (orderbook tracking) — D-06 separate process
#
# daemon-l2-run-local  — run L2 daemon locally on :19081 (dev env)
# smoke-l2-health      — curl local L2 /health + /healthz (verify Plan 03 skeleton)
# smoke-l2-health-prod        — curl prod L2 /healthz reachability only
# smoke-l2-health-strict-prod — read-only curl of prod L2 strict /health
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: daemon-l2-run-local smoke-l2-health smoke-l2-health-prod smoke-l2-health-strict-prod smoke-l2-ws smoke-event-bus

## daemon-l2-run-local: Start polyarb-l2 daemon locally on :19081 (separate from L1's :19080). Ctrl-C to stop.
daemon-l2-run-local:
	@set -a; [ -f .env ] && . ./.env; set +a; \
	PORT=$${POLYARB_HTTP_PORT:-19081}; \
	echo ">> daemon-l2-run-local — starting on http://127.0.0.1:$$PORT"; \
	echo ">> endpoints: GET /health (IETF strict)  GET /healthz (always 200)"; \
	echo ">> Ctrl-C to stop"; \
	echo ""; \
	POLYARB_DAEMON_VARIANT=l2 \
	POLYARB_DB_PATH=./data/l2-state.db \
	POLYARB_HTTP_PORT=$$PORT \
	POLYARB_ALLOW_EMPTY_SECRET=1 \
	uv run python -m polyarb.daemon.l2_main

## smoke-l2-health: Hit GET /health + /healthz on running local L2 daemon (port 19081 default)
smoke-l2-health:
	@set -a; [ -f .env ] && . ./.env; set +a; \
	PORT=$${POLYARB_HTTP_PORT:-19081}; \
	echo ">> smoke-l2-health — GET http://127.0.0.1:$$PORT/health"; \
	curl -fsS http://127.0.0.1:$$PORT/health | python3 -m json.tool || echo "L2 daemon not running locally"; \
	echo ""; echo ">> smoke-l2-health — GET http://127.0.0.1:$$PORT/healthz"; \
	curl -fsS http://127.0.0.1:$$PORT/healthz | python3 -m json.tool

## smoke-l2-health-prod: Verify prod L2 /healthz reachability only; this is not strict business health
smoke-l2-health-prod:
	@echo ">> smoke-l2-health-prod — GET https://polyarb-l2.fly.dev/healthz"
	@STATUS=$$(curl -s -o /tmp/l2_healthz_body.json -w "%{http_code}" https://polyarb-l2.fly.dev/healthz); \
	echo "HTTP $$STATUS"; \
	cat /tmp/l2_healthz_body.json | python3 -m json.tool; \
	if [ "$$STATUS" = "200" ]; then echo "PASS: L2 /healthz returned 200"; else echo "FAIL: expected 200 got $$STATUS"; exit 1; fi

## smoke-l2-health-strict-prod: Read-only GET of prod L2 strict /health; prints HTTP status and JSON body
smoke-l2-health-strict-prod:
	@BODY=$$(mktemp); trap 'rm -f "$$BODY"' EXIT; \
	URL="https://polyarb-l2.fly.dev/health"; \
	echo ">> smoke-l2-health-strict-prod — GET $$URL"; \
	HTTP_STATUS=$$(curl --disable --request GET -sS -o "$$BODY" -w "%{http_code}" "$$URL") || { rc=$$?; echo "FAIL: request error" >&2; exit $$rc; }; \
	echo "HTTP $$HTTP_STATUS"; \
	python3 -m json.tool < "$$BODY" || cat "$$BODY"; \
	if [ "$$HTTP_STATUS" = "200" ]; then echo "PASS: L2 strict /health returned 200"; else echo "FAIL: L2 strict /health returned $$HTTP_STATUS" >&2; exit 1; fi

## smoke-l2-ws: 30s WS sanity against a known liquid Polymarket asset (Phase 03 Plan 04, D-02 manual smoke)
##   Connects to wss://ws-subscriptions-clob.polymarket.com/ws/market, subscribes,
##   prints event-type counts. Override asset: make smoke-l2-ws ASSET=0x...
smoke-l2-ws:
	@echo ">> smoke-l2-ws — 30s sanity against Polymarket WS market channel"
	@uv run python scripts/smoke_l2_ws.py $(ASSET)

## smoke-event-bus: sanity-publish one pg_notify('snapshot_complete', ...) (Phase 03 Plan 05, D-05)
##   Requires POLYARB_SUPABASE_DB_DSN. Prints 'OK' if NOTIFY succeeded, 'FAIL' otherwise.
##   Auto-loads .env if present.
smoke-event-bus:
	@echo ">> smoke-event-bus — publish one snapshot_complete NOTIFY"
	@set -a; [ -f .env ] && . ./.env; set +a; \
	if [ -z "$$POLYARB_SUPABASE_DB_DSN" ]; then echo "ERROR: POLYARB_SUPABASE_DB_DSN not set"; exit 1; fi; \
	uv run python -c "import asyncio; from polyarb.config import load_settings; from polyarb.events.bus import publish_snapshot_complete; ok = asyncio.run(publish_snapshot_complete(load_settings(), snapshot_id=0, taken_at_ms=0)); print('OK' if ok else 'FAIL')"

# ─────────────────────────────────────────────────────────────────────────────
# Phase 02 Plan 03: Supabase mirror + R2 archive
#
# supabase-migrate       — run Alembic upgrade head (requires POLYARB_SUPABASE_DB_DSN)
# supabase-migrate-test  — forward+reverse+forward roundtrip (Phase 05 Plan 01 reversibility)
# supabase-reconcile     — compare SQLite vs Supabase mirror; push missing snapshots
# r2-list                — list R2 bucket objects (dev convenience)
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: supabase-migrate supabase-migrate-test supabase-reconcile r2-list

## supabase-migrate: Run Alembic upgrade head against Supabase DSN (auto-loads .env if present)
supabase-migrate:
	@echo ">> supabase-migrate — alembic upgrade head"
	@set -a; [ -f .env ] && . ./.env; set +a; \
	if [ -z "$$POLYARB_SUPABASE_DB_DSN" ]; then echo "ERROR: POLYARB_SUPABASE_DB_DSN not set in .env or shell (W6: DB DSN distinct from REST URL POLYARB_SUPABASE_URL)"; exit 1; fi; \
	uv run alembic upgrade head

## supabase-migrate-test: Forward+reverse+forward roundtrip on Alembic head (validates 005 reversibility)
## Phase 05 Plan 01: requires POLYARB_SUPABASE_DB_DSN pointing at a TEST database (never prod).
## Exits 77 (make-skip convention) if DSN unset; matches make triple-check pattern.
supabase-migrate-test:
	@echo ">> supabase-migrate-test — alembic upgrade head → downgrade -1 → upgrade head (validates reversibility)"
	@set -a; [ -f .env ] && . ./.env; set +a; \
	if [ -z "$$POLYARB_SUPABASE_DB_DSN" ]; then echo "POLYARB_SUPABASE_DB_DSN unset — skip"; exit 77; fi; \
	uv run alembic upgrade head && \
	uv run alembic downgrade -1 && \
	uv run alembic upgrade head && \
	uv run alembic current

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

# ─────────────────────────────────────────────────────────────────────────────
# Phase 02 Plan 04: Docker + Fly.io deploy
#
# docker-build       — build daemon Docker image locally
# docker-run-local   — run container locally (host:8088 → container:8080)
# docker-smoke       — build + run + /health probe + tear down (Wave 0 contract)
# deploy             — deploy to Fly.io prod (requires flyctl + FLY_API_TOKEN)
# smoke-test         — post-deploy /health probe against prod
# tail-logs          — tail Fly daemon stdout
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: docker-build docker-run-local docker-smoke deploy smoke-test tail-logs

## docker-build: Build daemon Docker image locally (no push)
docker-build:
	@echo ">> docker-build — multi-stage uv build"
	docker build -t polyarb-l1:local .

## docker-run-local: Run daemon container locally (host:8088 → container:8080)
docker-run-local:
	@echo ">> docker-run-local — daemon at http://localhost:8088/health"
	docker run --rm -p 8088:8080 -e POLYARB_ALLOW_EMPTY_SECRET=1 -e POLYARB_ALLOW_EXTERNAL_PATHS=1 polyarb-l1:local

## docker-smoke: Build image + run + curl /health + tear down (Plan 04 Wave 0 contract)
docker-smoke:
	@echo ">> docker-smoke — full build + run + health probe"
	bash tests/m1-perception/test_docker_smoke.sh

# Phase 03.1 Plan 03 (GAP-4): every flyctl invocation in this Makefile is
# prefixed with `FLY_API_TOKEN= ` to force flyctl to fall back to the
# keychain credential. Reason: .env may carry an L1-only FLY_API_TOKEN that
# silently shadows the keychain → "App not found" errors against sibling
# apps (Phase 03 Inj L2-2 cleanup precedent; memory
# `feedback_fly-api-token-shadowing-2026-05.md`). Do not remove the prefix.

## deploy: Deploy to Fly.io prod (requires flyctl + keychain auth — see GAP-4 banner above)
deploy:
	@echo ">> deploy — flyctl deploy --remote-only"
	FLY_API_TOKEN= flyctl deploy --remote-only --wait-timeout 600
	@echo ">> ensuring process scale: app=1 cron=1 (W8 Supercronic)"
	FLY_API_TOKEN= flyctl scale count app=1 cron=1 -a polyarb-l1 || true
	@echo ">> running post-deploy /health smoke probe"
	bash scripts/deploy_smoke.sh

## smoke-test: Run post-deploy smoke test against prod
smoke-test:
	@echo ">> smoke-test — post-deploy /health probe"
	bash scripts/deploy_smoke.sh

## tail-logs: Tail Fly daemon stdout
tail-logs:
	@echo ">> tail-logs — flyctl logs"
	FLY_API_TOKEN= flyctl logs --app polyarb-l1

.PHONY: memory-budget-test docker-smoke-256mb

## memory-budget-test: run streaming memory regression test (slow; D-23 acceptance gate)
memory-budget-test:
	@echo ">> memory-budget-test — T5.0 calibration + T5.1 budget test"
	uv run pytest tests/m1-perception/test_streaming_memory_calibration.py tests/m1-perception/test_streaming_memory_budget.py -xvs

## docker-smoke-256mb: build + run snapshot under hard 256MB cap with prod $1k threshold (T6 step 1)
docker-smoke-256mb: docker-build
	@echo ">> docker-smoke-256mb — hard 256MB cap, prod threshold \$$1k"
	docker run --rm --memory=256m --memory-swap=256m \
	    -e POLYARB_ALLOW_EXTERNAL_PATHS=1 \
	    -e POLYARB_ALLOW_EMPTY_SECRET=1 \
	    -e POLYARB_LIQUIDITY_THRESHOLD_USD=1000.0 \
	    -v $(PWD)/data:/data \
	    polyarb-l1 \
	    python -m polyarb.snapshot snapshot

.PHONY: sentry-test alerts-test logs-tail-axiom unpause-prod

## sentry-test: Trigger a deliberate Sentry capture_message to verify Sentry dashboard receives it
## Usage: POLYARB_SENTRY_DSN='https://...@sentry.io/...' make sentry-test
sentry-test:
	@echo ">> sentry-test — calling init_sentry then capture_message"
	uv run python -c "from polyarb.config import load_settings; from polyarb.observability.sentry import init_sentry; import sentry_sdk; s = load_settings(); init_sentry(s); sentry_sdk.capture_message('polyarb-l1 sentry-test from $$USER@$$(hostname) at $$(date -u +%FT%TZ)', level='info'); print('captured — check Sentry dashboard')"

## alerts-test: Trigger a deliberate paused-alert (Sentry + Better Stack /fail + Telegram direct)
## Usage: POLYARB_BETTER_STACK_HEARTBEAT_URL='...' POLYARB_TELEGRAM_BOT_TOKEN='...' POLYARB_TELEGRAM_CHAT_ID='...' make alerts-test
##
## NOTE 2026-05-20: init_sentry() MUST be called before send_paused_alert.
## Without init_sentry, sentry_sdk.capture_message silently no-ops (SDK is
## uninitialized in the script's process). Chaos Inj 1 root cause #4 —
## see threads/learnings-meta.md.
alerts-test:
	@echo ">> alerts-test — calling send_paused_alert (with init_sentry first)"
	uv run python -c "import asyncio; from polyarb.config import load_settings; from polyarb.observability.sentry import init_sentry; from polyarb.daemon.alerts import send_paused_alert; s = load_settings(); init_sentry(s); asyncio.run(send_paused_alert(s, reason='alerts-test from $$USER@$$(hostname)'))"

## unpause-prod: POST /control/unpause to prod daemon (HMAC-signed, empty body)
## Usage: POLYARB_SCAN_SHARED_SECRET='<secret>' make unpause-prod
## Reads POLYARB_SCAN_SHARED_SECRET from env (same secret as /scan per D-22).
## Phase 02.1 Plan 02 (BUG-8): replaces SSH + sqlite3 UPDATE + restart with one command.
unpause-prod:
	@[ -n "$$POLYARB_SCAN_SHARED_SECRET" ] || (echo "ERROR: POLYARB_SCAN_SHARED_SECRET not set"; exit 1)
	@SIG=$$(printf '' | openssl dgst -sha256 -hmac "$$POLYARB_SCAN_SHARED_SECRET" | awk '{print $$2}'); \
	echo ">> unpause-prod — POST https://polyarb-l1.fly.dev/control/unpause"; \
	curl -s -X POST https://polyarb-l1.fly.dev/control/unpause \
	  -H "X-Signature: sha256=$$SIG" \
	  -H "Content-Length: 0" | python -m json.tool

## polywatch-healthz-dry: Run polywatch healthz-watcher in dry-run mode (no Telegram/unpause)
## Usage: make polywatch-healthz-dry
## Reads POLYARB_SCAN_SHARED_SECRET / POLYARB_TELEGRAM_* from env (optional).
polywatch-healthz-dry:
	POLYWATCH_DRY_RUN=1 uv run python scripts/polywatch/healthz_watcher.py

## polywatch-healthz: Run polywatch healthz-watcher live (will push Telegram + try unpause on fail)
## Usage: POLYARB_SCAN_SHARED_SECRET=... POLYARB_TELEGRAM_BOT_TOKEN=... POLYARB_TELEGRAM_CHAT_ID=... make polywatch-healthz
polywatch-healthz:
	uv run python scripts/polywatch/healthz_watcher.py

## logs-tail-axiom: Print the Axiom dataset URL + sample APL query (convenience; opens nothing local)
logs-tail-axiom:
	@echo ">> open https://app.axiom.co/datasets/polyarb-prod in browser"
	@echo ">> APL query: '| where service == \"polyarb-l1\" | sort by _time desc | take 100'"

# ─────────────────────────────────────────────────────────────────────────────
# Dashboard (Phase 02 Plan 02-06 — Vercel Next.js)
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: dashboard-dev dashboard-build dashboard-typecheck dashboard-deploy smoke-l2-dashboard

## dashboard-dev: 本地起 dashboard (next dev :3000)
dashboard-dev:
	@echo ">> dashboard-dev — pnpm run dev"
	cd dashboard && pnpm run dev

## dashboard-build: Build dashboard production bundle (verify locally before Vercel deploy)
dashboard-build:
	@echo ">> dashboard-build — pnpm run build"
	cd dashboard && pnpm run build

## dashboard-typecheck: TS typecheck dashboard (tsc --noEmit)
dashboard-typecheck:
	@echo ">> dashboard-typecheck — pnpm tsc --noEmit"
	cd dashboard && pnpm tsc --noEmit

## dashboard-deploy: Vercel CLI deploy --prod (requires `vercel login` first time)
dashboard-deploy:
	@echo ">> dashboard-deploy — vercel deploy --prod"
	cd dashboard && pnpm dlx vercel --prod

## smoke-l2-dashboard: curl all 4 L2 Vercel pages, expect HTTP 200 each (D-07 reachability)
## Usage: make smoke-l2-dashboard                     # uses default URL
##        VERCEL_URL=https://your-url make smoke-l2-dashboard
##
## Note: the prod URL is gated behind Vercel Auth (Email whitelist from Phase 02
## Wave 4). A 200 response indicates the auth page (or the actual content if you
## pass a session cookie). Either result means the route is alive — 404/500
## means the page did not deploy.
##
## After committing dashboard changes to main, Vercel auto-deploys via webhook
## (~60-90s). Run this smoke AFTER the push has propagated. 404 across all 4 =
## deploy has not yet reached prod (or the project URL has drifted — override
## with VERCEL_URL=...).
smoke-l2-dashboard:
	@VERCEL_URL="$${VERCEL_URL:-https://polymarket-arbitrage.vercel.app}"; \
	echo ">> smoke-l2-dashboard — VERCEL_URL=$$VERCEL_URL"; \
	rc=0; \
	for path in candidates "asset/test-asset-id-12345/tob" "asset/test-asset-id-12345/trades" signals; do \
	  code=$$(curl -sS -L -o /dev/null -w "%{http_code}" "$$VERCEL_URL/$$path" 2>/dev/null); \
	  if [ -z "$$code" ]; then code="000"; fi; \
	  if [ "$$code" = "200" ]; then \
	    printf "  /%s: %s OK\n" "$$path" "$$code"; \
	  else \
	    printf "  /%s: %s FAIL\n" "$$path" "$$code"; \
	    rc=1; \
	  fi; \
	done; \
	exit $$rc

# ─────────────────────────────────────────────────────────────────────────────
# Phase 02 Plan 07 — 7-day production soak monitoring (Better Stack driven)
#
# W9 fix (2026-05-12): 无本地长跑进程。Better Stack cloud probe (Plan 05) 是
# 真正的 7×24 探针。soak_monitor.py 只做 status check + export audit trail。
# Requires: BETTERSTACK_API_TOKEN + BETTERSTACK_MONITOR_ID env vars.
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: soak-status soak-export soak-fault-inject

## soak-status: Pull 7-day uptime % from Better Stack and check Phase 02 gate (uptime >= 99%)
## Usage: BETTERSTACK_API_TOKEN=... BETTERSTACK_MONITOR_ID=... make soak-status
soak-status:
	@echo ">> soak-status — fetching Better Stack 7-day SLA history"
	uv run python scripts/soak_monitor.py status

## soak-export: Export 7-day Better Stack history to .planning/.../02-SOAK-LOG.md (audit trail)
## Usage: BETTERSTACK_API_TOKEN=... BETTERSTACK_MONITOR_ID=... make soak-export
soak-export:
	@echo ">> soak-export — dumping Better Stack history to 02-SOAK-LOG.md"
	uv run python scripts/soak_monitor.py export --days 7

## soak-fault-inject: Print instructions for deliberate fault injection (verify alert chain)
## Run on Day 3-4 of soak window to verify Telegram alert + self-healing path
soak-fault-inject:
	@echo ">> soak-fault-inject — deliberate fault injection to verify alert chain"
	@echo ""
	@echo "Option A (scale to 0 briefly):"
	@echo "  flyctl machines stop <machine_id> -a polyarb-l1"
	@echo "  # Wait 3-5 min → expect Telegram alert + Better Stack incident"
	@echo "  flyctl machines start <machine_id> -a polyarb-l1"
	@echo ""
	@echo "Option B (break R2 credentials):"
	@echo "  flyctl secrets unset POLYARB_R2_SECRET_ACCESS_KEY -a polyarb-l1"
	@echo "  # Wait for next snapshot → R2 warn in /health"
	@echo "  flyctl secrets set POLYARB_R2_SECRET_ACCESS_KEY=<original> -a polyarb-l1"
	@echo ""
	@echo "Option C (HMAC flood — 30 bad requests):"
	@echo "  for i in \$$(seq 1 30); do curl -s -X POST https://polyarb-l1.fly.dev/scan -H 'X-Signature: deadbeef' -d '{}' & done; wait"
	@echo ""
	@echo "Document each injection in 02-SOAK-LOG.md with timestamp + outcome."

# ─── Phase 03 Plan 02 — polyarb-l2 lifecycle (D-06) ─────────────────────────

## deploy-l2-prod: Manually trigger polyarb-l2 deploy via GHA workflow_dispatch (D-06)
deploy-l2-prod:
	@echo ">> deploy-l2-prod — gh workflow run deploy-l2.yml"
	gh workflow run deploy-l2.yml
	@echo "L2 deploy triggered. Watch:  gh run watch  (then 'make fly-l2-status')"
.PHONY: deploy-l2-prod

## fly-l2-status: List polyarb-l2 machines + checks (post-deploy verification)
fly-l2-status:
	@echo ">> fly-l2-status — flyctl status -a polyarb-l2"
	FLY_API_TOKEN= flyctl status -a polyarb-l2
	@echo "--- checks ---"
	FLY_API_TOKEN= flyctl checks list -a polyarb-l2
.PHONY: fly-l2-status

## fly-l2-logs: Tail polyarb-l2 daemon logs
fly-l2-logs:
	@echo ">> fly-l2-logs — flyctl logs -a polyarb-l2"
	FLY_API_TOKEN= flyctl logs --app polyarb-l2
.PHONY: fly-l2-logs

## fly-secrets-sync: Push .env to BOTH polyarb-l1 + polyarb-l2 (Phase 02.1 D-22 invariant)
fly-secrets-sync:
	@echo ">> fly-secrets-sync — pushing .env to both Fly apps"
	bash scripts/fly_secrets_sync.sh
.PHONY: fly-secrets-sync

## fly-secrets-sync-dry: DRY_RUN preview of fly-secrets-sync (no flyctl side effect)
fly-secrets-sync-dry:
	@echo ">> fly-secrets-sync-dry — DRY_RUN=1 preview"
	DRY_RUN=1 bash scripts/fly_secrets_sync.sh
.PHONY: fly-secrets-sync-dry

# ─────────────────────────────────────────────────────────────────────────────
# M1-perception Phase 03: ops surface for daily keepalive (D-01)
# ─────────────────────────────────────────────────────────────────────────────

## verify-keepalive: Show last 7 runs of supabase-keepalive workflow (D-01 ops surface)
## Surfaces silent GHA failures (Phase 02 L8 precedent: 4d silent fail observed).
## Exit 1 if ≥2 failures in window — triggers D-01 risk-surface review (upgrade to Pro?).
verify-keepalive:
	@bash scripts/check_keepalive.sh 7
.PHONY: verify-keepalive

# ─────────────────────────────────────────────────────────────────────────────
# M1-perception Phase 03 Plan 06: L2 mirror + Data API backfill ops (D-07/D-08)
# ─────────────────────────────────────────────────────────────────────────────

## migrate-l2: Apply Alembic 003 (5 L2 tables + RLS + BRIN) to Supabase Postgres
## (D-07) — alias of supabase-migrate (alembic auto-detects pending revisions).
migrate-l2:
	@echo ">> migrate-l2 — alembic upgrade head (003_l2_tables.py)"
	@set -a; [ -f .env ] && . ./.env; set +a; \
	if [ -z "$$POLYARB_SUPABASE_DB_DSN" ]; then echo "ERROR: POLYARB_SUPABASE_DB_DSN not set"; exit 1; fi; \
	uv run alembic upgrade head && uv run alembic current
.PHONY: migrate-l2

## backfill-trades: 7-day /trades backfill for one asset (D-08).
## Usage: make backfill-trades MARKET=<asset_id> [DAYS=7]
## Output: JSONL on stdout (one trade per line); pipe to jq or supabase upsert.
backfill-trades:
	@if [ -z "$(MARKET)" ]; then \
		echo "Usage: make backfill-trades MARKET=<asset_id> [DAYS=7]" >&2; exit 1; \
	fi
	@set -a; [ -f .env ] && . ./.env; set +a; \
	uv run python -m polyarb.clients.data_api_client --market $(MARKET) --days $${DAYS:-7}
.PHONY: backfill-trades

## smoke-l2-mirror: Sanity-instantiate L2SupabaseMirror against .env creds.
## Does NOT write data — only confirms the supabase-py client constructs cleanly.
## Useful as a credential-presence gate before deploying the L2 daemon.
smoke-l2-mirror:
	@set -a; [ -f .env ] && . ./.env; set +a; \
	uv run python -c "from polyarb.config import load_settings; from polyarb.storage.l2_supabase_mirror import L2SupabaseMirror; s = load_settings(); m = L2SupabaseMirror(s.supabase_url, s.supabase_service_key.get_secret_value()); print('OK l2-mirror instantiated url=', s.supabase_url)"
.PHONY: smoke-l2-mirror

# ─────────────────────────────────────────────────────────────────────────────
# M1-perception Phase 03 Wave 6 — chaos injection targets (Plan 03-07)
# ─────────────────────────────────────────────────────────────────────────────
# Each target invokes the per-Inj action commands from L2_CHAOS_PLAN
# (tests/chaos/test_l2_chaos_plan.py). Run them in sequence (L2-1 → L2-5),
# recording evidence in 03-SOAK-LOG.md. NEVER run during peak trading —
# Sentry + Telegram will alert for real.
#
# Phase 03.1 Plan 03 (GAP-4) — FLY_API_TOKEN-safe invariant:
# All chaos-l2-* targets prefix flyctl calls with `FLY_API_TOKEN= ` to force
# flyctl to fall back to the keychain credential, preventing the .env
# shadowing silent-error pattern documented in Phase 03 Inj L2-2 cleanup
# (memory `feedback_fly-api-token-shadowing-2026-05.md`). Where the recipe
# also sources `.env` (for POLYARB_SUPABASE_*), it MUST explicitly
# `unset FLY_API_TOKEN` AFTER `set +a` so the .env-resident token does not
# leak back into the shell. Do not remove the prefix or the unset.

## chaos-l2-baseline: Capture pre-chaos baseline (machine + /health + row count)
chaos-l2-baseline:
	@echo "=== Phase 03 chaos baseline ($$(date -u +%FT%TZ)) ==="
	@FLY_API_TOKEN= flyctl status -a polyarb-l2 | tail -6
	@echo ""
	@echo "/healthz HTTP: $$(curl -sS -o /dev/null -w '%{http_code}' https://polyarb-l2.fly.dev/healthz)"
	@echo "/health  HTTP: $$(curl -sS -o /dev/null -w '%{http_code}' https://polyarb-l2.fly.dev/health)"
	@echo ""
	@set -a; [ -f .env ] && . ./.env; set +a; unset FLY_API_TOKEN; \
	psql "$$POLYARB_SUPABASE_DB_DSN" -tAc "SELECT 'l2_top_of_book rows='||count(*) FROM l2_top_of_book"
.PHONY: chaos-l2-baseline

## chaos-l2-inj1: Inj L2-1 — restart machine (TCP RST forces WS close), observe reconnect
##
## Original chaos design called for `pkill` inside container, but python-slim
## base has no procps. Alternative: `flyctl machine restart` triggers SIGTERM
## to PID 1 (the daemon) → graceful shutdown → cold restart in ~10-30s.
## This is the closest in-prod analog to "kill WS connection" — actually
## stronger because it tests cold-start init order (Phase 02 P9 gate) AND
## reconnect simultaneously.
chaos-l2-inj1:
	@echo "=== Inj L2-1: machine restart ($$(date -u +%FT%TZ)) ==="
	@MID=$$(FLY_API_TOKEN= flyctl machines list -a polyarb-l2 --json | jq -r '.[0].id'); \
	echo "machine_id=$$MID"; \
	FLY_API_TOKEN= flyctl machine restart $$MID -a polyarb-l2
	@echo "→ restart issued. Polling /health for 90s…"
	@for i in 1 2 3 4 5 6 7 8 9; do \
		sleep 10; \
		echo -n "  t+$$(($$i*10))s: "; \
		curl -sS https://polyarb-l2.fly.dev/health 2>/dev/null | jq -r '.checks["ws:connection_state"][0].observedValue + " age=" + (.checks["ws:last_event_age_seconds"][0].observedValue|tostring) + "s"' 2>/dev/null || echo "(no response — daemon cold-starting)"; \
	done
	@echo ""
	@echo "=== Post-recovery: machine state + l2_top_of_book delta ==="
	@FLY_API_TOKEN= flyctl status -a polyarb-l2 | grep -E "started|stopped" | head -2
	@set -a; [ -f .env ] && . ./.env; set +a; unset FLY_API_TOKEN; \
	psql "$$POLYARB_SUPABASE_DB_DSN" -tAc "SELECT 'rows_in_last_120s='||count(*) FROM l2_top_of_book WHERE ts > now() - interval '120 seconds'"
.PHONY: chaos-l2-inj1

## chaos-l2-inj2: Inj L2-2 — revoke SUPABASE_SERVICE_KEY on L2, observe fail-soft
chaos-l2-inj2:
	@echo "=== Inj L2-2: revoke L2 SUPABASE_SERVICE_KEY ($$(date -u +%FT%TZ)) ==="
	FLY_API_TOKEN= flyctl secrets unset POLYARB_SUPABASE_SERVICE_KEY -a polyarb-l2
	@echo "→ revoked. Waiting 60s for fail-soft to manifest…"
	@sleep 60
	@echo ""
	@echo "/healthz: $$(curl -sS -o /dev/null -w '%{http_code}' https://polyarb-l2.fly.dev/healthz)  (expect 200)"
	@echo "/health:  $$(curl -sS -o /dev/null -w '%{http_code}' https://polyarb-l2.fly.dev/health)  (expect 503)"
	@FLY_API_TOKEN= flyctl status -a polyarb-l2 | grep -E "started|running" | head -2
	@echo ""
	@echo "=== restoring key from .env (cleanup) ==="
	@set -a; [ -f .env ] && . ./.env; set +a; unset FLY_API_TOKEN; \
	FLY_API_TOKEN= flyctl secrets set POLYARB_SUPABASE_SERVICE_KEY="$$POLYARB_SUPABASE_SERVICE_KEY" -a polyarb-l2
.PHONY: chaos-l2-inj2

## chaos-l2-inj3a: Inj L2-3a — confirm L1 publishes 0 NOTIFY when EVENT_BUS disabled
chaos-l2-inj3a:
	@echo "=== Inj L2-3a: default-state probe ($$(date -u +%FT%TZ)) ==="
	@FLY_API_TOKEN= flyctl secrets list -a polyarb-l1 | grep -i event_bus || echo "OK POLYARB_EVENT_BUS_ENABLED unset on L1 (B1 default OFF)"
	@echo ""
	@echo "L2 listener still in 'listening' state (no NOTIFY needed):"
	@curl -sS https://polyarb-l2.fly.dev/health 2>/dev/null | jq -r '.checks["event_bus:listener_state"][0].observedValue'
.PHONY: chaos-l2-inj3a

## chaos-l2-inj3b: Inj L2-3b — opt-in L1 NOTIFY, trigger scan, confirm L2 receives
chaos-l2-inj3b:
	@echo "=== Inj L2-3b: opt-in path ($$(date -u +%FT%TZ)) ==="
	FLY_API_TOKEN= flyctl secrets set POLYARB_EVENT_BUS_ENABLED=1 -a polyarb-l1
	@echo "→ L1 NOTIFY enabled. Triggering a snapshot via /scan…"
	@set -a; [ -f .env ] && . ./.env; set +a; unset FLY_API_TOKEN; \
	BODY='{}'; SIG=$$(printf "%s" "$$BODY" | openssl dgst -sha256 -hmac "$$POLYARB_SCAN_SHARED_SECRET" | awk '{print $$2}'); \
	curl -sS -X POST -H "X-Signature: $$SIG" -d "$$BODY" https://polyarb-l1.fly.dev/scan || true
	@echo ""
	@echo "→ waiting 120s for L1 snapshot to complete + L2 to dispatch…"
	@sleep 120
	@echo ""
	@echo "L2 candidate refresh log (last 3):"
	@FLY_API_TOKEN= flyctl logs -a polyarb-l2 --no-tail | grep -oE 'candidate refresh.*snapshot_id=[0-9]+' | tail -3 || echo "(none yet)"
	@echo ""
	@echo "l2_event_cursor advance:"
	@set -a; [ -f .env ] && . ./.env; set +a; unset FLY_API_TOKEN; \
	psql "$$POLYARB_SUPABASE_DB_DSN" -tAc "SELECT 'last_snapshot_id='||last_snapshot_id FROM l2_event_cursor WHERE consumer='l2-candidate-refresh'"
	@echo ""
	@echo "=== reverting to default OFF (B1 invariant) ==="
	FLY_API_TOKEN= flyctl secrets unset POLYARB_EVENT_BUS_ENABLED -a polyarb-l1
.PHONY: chaos-l2-inj3b

## chaos-l2-cleanup: Force restore L2 secrets from .env (use if Inj aborts mid-way)
chaos-l2-cleanup:
	@echo "=== Phase 03 chaos cleanup — restoring L2 secrets from .env ==="
	@set -a; [ -f .env ] && . ./.env; set +a; unset FLY_API_TOKEN; \
	FLY_API_TOKEN= flyctl secrets set POLYARB_SUPABASE_SERVICE_KEY="$$POLYARB_SUPABASE_SERVICE_KEY" POLYARB_SUPABASE_DB_DSN="$$POLYARB_SUPABASE_DB_DSN" -a polyarb-l2
	FLY_API_TOKEN= flyctl secrets unset POLYARB_EVENT_BUS_ENABLED -a polyarb-l1 || true
	@echo "→ done. Verify with: make chaos-l2-baseline"
.PHONY: chaos-l2-cleanup

## chaos-l2-fly-image-check: Verify chaos primitives available in current polyarb-l2 fly image
##
## Phase 03.1 Plan 03 / PROCESS-2 (image-aware chaos design). Auto-resolves
## the currently deployed image and runs `docker run --rm IMAGE which TOOL`
## for each chaos primitive we may want to use. Output is best-effort:
## requires local `docker` daemon + flyctl auth. If docker is unavailable
## the target still surfaces "ERROR: docker missing" without breaking CI.
chaos-l2-fly-image-check:
	@if ! command -v docker >/dev/null 2>&1; then \
		echo "ERROR: docker not on PATH — this is a developer-local target."; \
		echo "Install docker desktop or run from a host with the daemon."; \
		exit 2; \
	fi
	@IMAGE=$$(FLY_API_TOKEN= flyctl status -a polyarb-l2 --json 2>/dev/null | jq -r '.ImageRef.Registry + "/" + .ImageRef.Repository + ":" + .ImageRef.Tag' 2>/dev/null); \
	if [ -z "$$IMAGE" ] || [ "$$IMAGE" = "null/:null" ] || [ "$$IMAGE" = "/:" ]; then \
		echo "ERROR: cannot resolve current polyarb-l2 image (flyctl auth?)"; exit 1; \
	fi; \
	echo "Checking primitives in $$IMAGE…"; \
	rc=0; \
	for tool in pkill ps kill which dig ping curl python; do \
		if docker run --rm --entrypoint /bin/sh "$$IMAGE" -c "command -v $$tool >/dev/null 2>&1"; then \
			echo "  OK    $$tool"; \
		else \
			echo "  MISS  $$tool"; \
			rc=1; \
		fi; \
	done; \
	if [ $$rc -ne 0 ]; then \
		echo ""; \
		echo "→ Missing tools detected. See docs/dev/chaos-toolkit.md for substitute patterns."; \
	fi; \
	exit $$rc
.PHONY: chaos-l2-fly-image-check

## chaos-l2-listener-recovery: Prove L2 LISTEN reconnect or timer-only cursor recovery; usage: make chaos-l2-listener-recovery mode=listener|poll
##
## Phase 05.1. Runs on the operator host with Python/asyncpg, Fly APIs, and
## HTTP probes; it assumes no optional binary inside python:3.12-slim. The
## listener mode terminates only the uniquely identified LISTEN backend. The
## poll mode requires the operator to set L1 EVENT_BUS to 0, proves an exact
## unchanged notification anchor, and blocks until the operator restores 1.
chaos-l2-listener-recovery:
	@if [ "$(mode)" != "listener" ] && [ "$(mode)" != "poll" ]; then \
		echo "usage: make chaos-l2-listener-recovery mode=listener|poll"; exit 2; \
	fi
	@uv run python scripts/chaos_l2_listener_recovery.py --mode $(mode)
.PHONY: chaos-l2-listener-recovery

# ─────────────────────────────────────────────────────────────────────────────
# Phase 03.1 — DNS observability (Plan 04)
# ─────────────────────────────────────────────────────────────────────────────

## dns-baseline-probe: Probe Polymarket hostnames N times — Fly DNS chronic failure baseline data
## (Sentry issue 121111789). Pure stdlib script, runnable inside the polyarb-l1
## container image. Tune via POLYARB_DNS_PROBE_N / POLYARB_DNS_PROBE_INTERVAL_S.
dns-baseline-probe:
	@echo "=== DNS baseline probe ($$(date -u +%FT%TZ)) ==="
	@uv run python scripts/dns_baseline_probe.py
.PHONY: dns-baseline-probe

# ════════════════════════════════════════════════════════════════════════════
# Phase 03.1 — chaos kill flags (Plan 06)
# ════════════════════════════════════════════════════════════════════════════
#
# Two new chaos targets land here:
#   - chaos-l2-inj4: Inj L2-4 — WS storm (POLYARB_WS_TEST_KILL=1) + Supabase
#     mirror failure double-fault. Replaces Phase 03's pkill-based primitive
#     (python:3.12-slim has no procps — feedback_container-image-aware-chaos).
#   - chaos-l2-inj5-dryrun: Inj L2-5 — replay recorded 429 fixture against
#     the backfill code locally (no Polymarket calls). Live 429 chaos is
#     deferred to "实际触发时再验" per 03.1-CONTEXT.md.
#
# FLY_API_TOKEN discipline (Phase 03.1-03 GAP-4): every flyctl invocation
# below carries an explicit `FLY_API_TOKEN= ` prefix to force fallback to
# the keychain credential. The .env may contain an L1-only token that
# silently shadows the keychain entry (memory feedback_fly-api-token-
# shadowing-2026-05.md). Do NOT remove the prefix even on a refactor pass.

## chaos-l2-inj4: Inj L2-4 — WS storm (POLYARB_WS_TEST_KILL=1) + Supabase mirror failure (double-fault)
##
## Sequence:
##   1. precondition baseline (machine healthy, /healthz 200)
##   2. set POLYARB_WS_TEST_KILL=1 (forces WS connection to drop on next message — Plan 06 Task 1)
##   3. concurrently unset POLYARB_SUPABASE_SERVICE_KEY (mirror writes fail)
##   4. wait 60s — observe daemon survival, reconnect attempts, mirror surface to /health
##   5. curl /health → expect overall=fail (mirror:l2_tob_age_seconds trips when Plan 02 wired)
##       /health should also list chaos:ws_test_kill_flag with status=warn (chain-truth surface)
##   6. cleanup: unset WS_TEST_KILL + restore SUPABASE_SERVICE_KEY from .env
##   7. wait 30s → /health back to pass, l2_top_of_book new rows resume
##
## This is the cross-bug chaos deferred from Phase 03 (D-04). 3-asset bootstrap candidate set
## means we verify LOGIC CORRECTNESS (watchdog kicks in, mirror surface), NOT throughput.
## Phase 04+ revisits throughput when candidate set grows past bootstrap.
chaos-l2-inj4:
	@echo "=== Inj L2-4: WS storm + Supabase pause double-fault ($$(date -u +%FT%TZ)) ==="
	@echo "→ Precondition: /healthz must be 200"
	@HZ=$$(curl -sS -o /dev/null -w '%{http_code}' https://polyarb-l2.fly.dev/healthz); \
	if [ "$$HZ" != "200" ]; then echo "ABORT: /healthz=$$HZ (expected 200)"; exit 1; fi
	@echo "→ Step 1: enable POLYARB_WS_TEST_KILL=1 on polyarb-l2"
	FLY_API_TOKEN= flyctl secrets set POLYARB_WS_TEST_KILL=1 -a polyarb-l2
	@echo "→ Step 2: revoke POLYARB_SUPABASE_SERVICE_KEY on polyarb-l2 (concurrent fault)"
	FLY_API_TOKEN= flyctl secrets unset POLYARB_SUPABASE_SERVICE_KEY -a polyarb-l2
	@echo "→ Waiting 60s for faults to manifest…"
	@sleep 60
	@echo ""
	@echo "→ /health overall (expect fail / HTTP 503 when Plan 02 mirror sub-check wired):"
	@curl -sS -o /dev/null -w 'HTTP %{http_code}\n' https://polyarb-l2.fly.dev/health
	@curl -sS https://polyarb-l2.fly.dev/health | jq '.status, .checks["chaos:ws_test_kill_flag"][0], .checks["mirror:l2_tob_age_seconds"][0]'
	@echo ""
	@echo "→ l2_top_of_book new rows in last 60s (expect 0 — mirror dead):"
	@set -a; [ -f .env ] && . ./.env; set +a; unset FLY_API_TOKEN; \
	psql "$$POLYARB_SUPABASE_DB_DSN" -tAc "SELECT 'l2_tob rows last 60s='||count(*) FROM l2_top_of_book WHERE ts > now() - interval '60 seconds'"
	@echo ""
	@echo "→ CLEANUP — restoring secrets…"
	FLY_API_TOKEN= flyctl secrets unset POLYARB_WS_TEST_KILL -a polyarb-l2
	@set -a; [ -f .env ] && . ./.env; set +a; unset FLY_API_TOKEN; \
	FLY_API_TOKEN= flyctl secrets set POLYARB_SUPABASE_SERVICE_KEY="$$POLYARB_SUPABASE_SERVICE_KEY" -a polyarb-l2
	@echo "→ Waiting 30s for recovery…"
	@sleep 30
	@echo "→ /health overall (expect pass / 200):"
	@curl -sS -o /dev/null -w 'HTTP %{http_code}\n' https://polyarb-l2.fly.dev/health
	@curl -sS https://polyarb-l2.fly.dev/health | jq '.status, (.checks | has("chaos:ws_test_kill_flag"))'
	@echo ""
	@echo "→ l2_top_of_book new rows in last 60s (expect >0 — mirror restored):"
	@set -a; [ -f .env ] && . ./.env; set +a; unset FLY_API_TOKEN; \
	psql "$$POLYARB_SUPABASE_DB_DSN" -tAc "SELECT 'l2_tob rows last 60s='||count(*) FROM l2_top_of_book WHERE ts > now() - interval '60 seconds'"
.PHONY: chaos-l2-inj4

## chaos-ws-kill: flip the in-flight WS-kill chaos flag on prod L2 (HMAC-gated, no restart).
##
## Phase 04.1 G-03: in-band chaos primitive. Sends a signed POST to
## /control/chaos/ws-test-kill on polyarb-l2.fly.dev, flipping the process-local
## flag without restarting the Fly machine (the pre-storm 60-asset process survives).
##
## Usage:
##   make chaos-ws-kill ON=1   # enable WS kill (next WS frame triggers disconnect)
##   make chaos-ws-kill ON=0   # clear flag (normal reconnect continues)
##
## Requires POLYARB_SCAN_SHARED_SECRET in .env. Uses openssl + curl (dev host tools
## — no Fly image tooling needed; image-aware safe per CLAUDE.md).
##
## HMAC is computed over the exact body bytes (sha256=<hex>) — any body tamper
## would invalidate the signature (T-04.1-02 mitigation). Missing/wrong sig → 401.
chaos-ws-kill:
	@test -n "$(ON)" || { echo "usage: make chaos-ws-kill ON=1|0"; exit 1; }
	@set -a; . ./.env; set +a; \
	  [ -n "$$POLYARB_SCAN_SHARED_SECRET" ] || { echo "ERROR: POLYARB_SCAN_SHARED_SECRET not set (check .env) — request would 401"; exit 1; }; \
	  BODY=$$([ "$(ON)" = "1" ] && echo '{"enabled":true}' || echo '{"enabled":false}'); \
	  SIG=$$(printf '%s' "$$BODY" | openssl dgst -sha256 -hmac "$$POLYARB_SCAN_SHARED_SECRET" | sed 's/^.* //'); \
	  curl -sS -X POST https://polyarb-l2.fly.dev/control/chaos/ws-test-kill \
	    -H "X-Signature: sha256=$$SIG" -H 'Content-Type: application/json' -d "$$BODY" | jq .
.PHONY: chaos-ws-kill

## chaos-l2-inj4-throughput: Phase 04 D-05/D-06 — real candidate-scale WS storm + throughput verdict
##
## Extends chaos-l2-inj4 with baseline-then-threshold throughput measurement.
## Repays the Phase 03.1 Inj L2-4 instrumentation debt ("3-asset bootstrap is
## small enough that WS storm is really WS close + reconnect — no genuine
## storm rate"). Runs LOCALLY on the dev host (curl/jq on macOS), targeting
## the public polyarb-l2.fly.dev endpoints — no python:3.12-slim image-tool
## gap applies. Requires `jq` locally (Homebrew default).
##
## Phase 04.1 G-03 update: storm/cleanup now use the in-band HTTP endpoint
## (make chaos-ws-kill ON=1/0) instead of `flyctl secrets set/unset
## POLYARB_WS_TEST_KILL`. This means the pre-storm 60-asset process SURVIVES
## into the storm window — enabling Pitfall 4 watchdog observation.
##
## Sequence (8 steps, ~7 min wall clock):
##   1. precondition: /healthz 200
##   2. precondition: ws:subscribed_count > 3 (confirms D-01 Supabase swap is
##      effective in prod; aborts if still <= 3 bootstrap assets — would
##      degrade to Phase 03.1 logic-only test)
##   3. baseline T1: frame_count + ws state + RSS @ T=0
##   4. wait 5min for baseline frame rate accumulation
##   5. baseline T2: frame_count + RSS @ T=5min — yields baseline rate +
##      baseline RSS (the operator computes deltas from the JSON snapshots
##      saved to /tmp under known names)
##   6. storm: make chaos-ws-kill ON=1 (HMAC endpoint — no restart, same process)
##   7. wait 60s, then recovery: frame_count + ws state + RSS @ T=storm+60s
##   8. cleanup: make chaos-ws-kill ON=0 (clears flag in-band — no restart)
##
## D-06 PASS criteria (RESEARCH Q4 baseline-then-threshold):
##   (1) frame_rate_recovery >= frame_rate_baseline * 0.90  → "zero dropped frames"
##   (2) watchdog state == WAITING_FOR_EVENT within 60s of cleanup
##   (3) rss_recovery <= rss_baseline * 1.30                → memory within 30%
##
## Verdict is HUMAN-VERIFY (Task 3 checkpoint): the recipe emits the raw
## numbers and the operator records the pass/fail line in 04.1-SOAK-LOG.md.
## Snapshots are written to /tmp/inj4t-{t1,t2,t3}.json so the operator can
## diff after the run without re-curling.
chaos-l2-inj4-throughput:
	@echo "=== Inj L2-4-throughput: real candidate set WS storm ($$(date -u +%FT%TZ)) ==="
	@command -v jq >/dev/null 2>&1 || { echo "ABORT: jq not found on dev host (brew install jq)"; exit 1; }
	@echo "→ Step 1: Precondition /healthz must be 200"
	@HZ=$$(curl -sS -o /dev/null -w '%{http_code}' https://polyarb-l2.fly.dev/healthz); \
	if [ "$$HZ" != "200" ]; then echo "ABORT: /healthz=$$HZ (expected 200)"; exit 1; fi; \
	echo "→ /healthz=200 OK"
	@echo "→ Step 2: Precondition ws:subscribed_count > 3 (confirms D-01 swap deployed)"
	@N=$$(curl -sS https://polyarb-l2.fly.dev/health | jq -r '.checks["ws:subscribed_count"][0].observedValue // 0'); \
	if [ "$${N:-0}" -le 3 ]; then \
		echo "ABORT: ws:subscribed_count=$$N (<=3) — D-01 Supabase swap not effective in prod;"; \
		echo "  investigate Plan 02 deployment + candidates:supabase_fetch_age_seconds /health row before chaos."; \
		exit 1; \
	fi; \
	echo "→ ws:subscribed_count=$$N (> 3 confirmed)"
	@echo "→ Step 3: Baseline T1 snapshot @ $$(date -u +%H:%M:%SZ) → /tmp/inj4t-t1.json"
	@curl -sS https://polyarb-l2.fly.dev/health > /tmp/inj4t-t1.json
	@jq '{ts: now, ws_state: .checks["ws:connection_state"][0].observedValue, ws_age_s: .checks["ws:last_event_age_seconds"][0].observedValue, subscribed: .checks["ws:subscribed_count"][0].observedValue}' /tmp/inj4t-t1.json
	@echo "→ Baseline T1 RSS (from /health process:rss_kb — G-04 04.1 fix):"
	@jq '.checks["process:rss_kb"][0].observedValue' /tmp/inj4t-t1.json | tee /tmp/inj4t-t1-rss.txt
	@echo ""
	@echo "→ Step 4: Waiting 5min for baseline frame rate accumulation…"
	@sleep 300
	@echo "→ Step 5: Baseline T2 snapshot @ $$(date -u +%H:%M:%SZ) → /tmp/inj4t-t2.json"
	@curl -sS https://polyarb-l2.fly.dev/health > /tmp/inj4t-t2.json
	@jq '{ts: now, ws_state: .checks["ws:connection_state"][0].observedValue, ws_age_s: .checks["ws:last_event_age_seconds"][0].observedValue}' /tmp/inj4t-t2.json
	@echo "→ Baseline T2 RSS (from /health process:rss_kb — G-04 04.1 fix):"
	@jq '.checks["process:rss_kb"][0].observedValue' /tmp/inj4t-t2.json | tee /tmp/inj4t-t2-rss.txt
	@echo ""
	@echo "→ Step 6: STORM — flip ws-test-kill flag via in-band endpoint (no restart, same process)"
	$(MAKE) chaos-ws-kill ON=1
	@echo "→ Waiting 60s for reconnect + recovery…"
	@sleep 60
	@echo "→ Step 7: Recovery T3 snapshot @ $$(date -u +%H:%M:%SZ) → /tmp/inj4t-t3.json"
	@curl -sS https://polyarb-l2.fly.dev/health > /tmp/inj4t-t3.json
	@jq '{ts: now, ws_state: .checks["ws:connection_state"][0].observedValue, ws_age_s: .checks["ws:last_event_age_seconds"][0].observedValue, chaos_flag: (.checks["chaos:ws_test_kill_flag"][0].status // "absent")}' /tmp/inj4t-t3.json
	@echo "→ Recovery T3 RSS (from /health process:rss_kb — G-04 04.1 fix):"
	@jq '.checks["process:rss_kb"][0].observedValue' /tmp/inj4t-t3.json | tee /tmp/inj4t-t3-rss.txt
	@echo ""
	@echo "→ Step 8: CLEANUP — clear ws-test-kill flag in-band (no restart)"
	$(MAKE) chaos-ws-kill ON=0
	@sleep 30
	@echo "→ Final /health (expect overall=pass / HTTP 200):"
	@curl -sS -o /dev/null -w 'HTTP %{http_code}\n' https://polyarb-l2.fly.dev/health
	@curl -sS https://polyarb-l2.fly.dev/health | jq '.status, (.checks | has("chaos:ws_test_kill_flag"))'
	@echo ""
	@echo "=== Throughput verdict (operator records in 04-SOAK-LOG.md) ==="
	@echo "  Snapshots: /tmp/inj4t-t1.json /tmp/inj4t-t2.json /tmp/inj4t-t3.json"
	@echo "  RSS files: /tmp/inj4t-t{1,2,3}-rss.txt"
	@echo "  Compute:"
	@echo "    baseline_frame_rate = (?frame_count source — see below)"
	@echo "    recovery vs baseline ratio against D-06 criteria:"
	@echo "      (1) frame_rate_recovery >= baseline*0.90"
	@echo "      (2) ws_state == WAITING_FOR_EVENT within 60s ?"
	@echo "      (3) rss_recovery <= rss_baseline*1.30 ?"
	@echo "  Note: frame_count is exposed via consumer.frame_count; if /health"
	@echo "  does not surface it yet, read directly: flyctl ssh console -a polyarb-l2 -C"
	@echo "  'python -c \"import requests;print(requests.get(\\\"http://localhost:8080/health\\\").json())\"'"
.PHONY: chaos-l2-inj4-throughput

## chaos-l2-inj5-dryrun: Replay recorded 429 fixture against backfill code locally (no Polymarket calls)
##
## Live Inj L2-5 (real Polymarket /trades 429) is deferred per 03.1-CONTEXT.md
## "实际触发时再验". This target validates the code path handles 429 correctly
## using a recorded fixture, so the code can be confidently shipped without
## waiting for nature to trigger the real condition.
chaos-l2-inj5-dryrun:
	@echo "=== Inj L2-5 dry-run: 429 fixture replay (no network) ==="
	@uv run pytest tests/chaos/test_data_api_429_fixture.py -xvs
.PHONY: chaos-l2-inj5-dryrun

# ============================================================================
# Phase 03.1-05 (GAP-102) — Sentry alert routing audit
# ============================================================================

## sentry-alert-audit: Re-emit Sentry alert rule baseline as JSON-lines
##
## Hard artifact: `.planning/workstreams/m1-perception/phases/03.1-l2-observability-gaps-fix-up/sentry-audit-report.md`
## This target re-prints the audit baseline (rule IDs / actions / env filters)
## as JSON-lines so a future operator can re-run the playwright-cli navigation
## sequence and diff against baseline to detect rule drift.
##
## Typical workflow:
##   1. `make sentry-alert-audit > /tmp/sentry-audit.jsonl`
##   2. Re-run playwright-cli on the URLs listed under `"type": "steps"`
##   3. Compare live data to BASELINE_RULES in scripts/sentry_alert_audit.py
##   4. Update sentry-audit-report.md + BASELINE_RULES on confirmed drift.
sentry-alert-audit:
	@uv run python scripts/sentry_alert_audit.py
.PHONY: sentry-alert-audit

# ============================================================================
# Phase 05 Plan 05-05 — L3 promoter + OHLC + dashboard smoke ops
# ============================================================================

## l3-promote-dry-run: Run l3_promote.promote_run once locally without WS mutation (prints candidate set)
##
## Loads .env (if present) so POLYARB_SUPABASE_URL + service-role key reach
## the helper. Swaps WsConsumer for a no-op shim — `WOULD add` / `WOULD remove`
## lines show what real subscribe calls WOULD have been issued. Use this to
## sanity-check the L3 candidate selection logic against prod Supabase
## without touching real WS state.
l3-promote-dry-run:
	@echo ">> l3-promote-dry-run — single tick, no real WS mutation"
	@if [ -f .env ]; then set -a && . ./.env && set +a; fi; \
	uv run python scripts/l3_promote_dry_run.py
.PHONY: l3-promote-dry-run

## ohlc-spot-check: Query /health for L3 active set + book_levels freshness anchors
##
## Reads the local L2 daemon /health (or pass URL=https://polyarb-l2.fly.dev
## to hit prod). Prints l3:active_count + last_promote_at_s + last
## book_levels_write_at_s — the 3 anchors that prove the L3 promoter is alive
## and OHLC views have fresh source data. Use after deploy or as a daily
## sanity check; doesn't fail the build if /health unreachable.
ohlc-spot-check:
	@echo ">> ohlc-spot-check — l3:* anchors from /health"
	@URL="$${URL:-http://localhost:8080}"; \
	echo "→ hitting $$URL/health"; \
	curl -sS "$$URL/health" 2>/dev/null | uv run python scripts/ohlc_spot_check.py \
		|| echo "(daemon at $$URL/health not reachable — try URL=https://polyarb-l2.fly.dev)"
.PHONY: ohlc-spot-check

## smoke-l3-dashboard: HTTP smoke an /l3/<asset_id> page (local dev OR prod via URL=)
##
## Usage:
##   make smoke-l3-dashboard asset_id=<asset_id>
##   make smoke-l3-dashboard asset_id=<asset_id> URL=https://polymarket-arbitrage.vercel.app
##
## Default URL is http://localhost:3000 (run `cd dashboard && pnpm dev` first).
## Returns HTTP status + payload size + grep for "asset_id" marker. Fail-soft:
## echoes a hint and exit 77 if URL isn't reachable, so CI consumers can
## detect "skip" vs "fail" cleanly.
smoke-l3-dashboard:
	@echo ">> smoke-l3-dashboard — usage: make smoke-l3-dashboard asset_id=<id> [URL=...]"
	@if [ -z "$(asset_id)" ]; then echo "ABORT: missing asset_id (try: make smoke-l3-dashboard asset_id=<asset_id>)"; exit 2; fi
	@URL="$${URL:-http://localhost:3000}"; \
	echo "→ hitting $$URL/l3/$(asset_id)"; \
	curl -sS -o /tmp/l3-smoke.html -w "HTTP %{http_code} (size: %{size_download} bytes)\n" \
		"$$URL/l3/$(asset_id)" \
		|| { echo "(dashboard at $$URL not reachable — run 'cd dashboard && pnpm dev' first, or pass URL=https://...)"; exit 77; }
	@MARKERS=$$(grep -c "asset_id\|Depth ladder\|KlineChart\|l3_promoted" /tmp/l3-smoke.html 2>/dev/null || echo 0); \
	echo "→ markers found: $$MARKERS (asset_id / Depth ladder / KlineChart / l3_promoted)"
.PHONY: smoke-l3-dashboard

# ═══════════════════════════════════════════════════════════════════════════
# m2 arbitrage CLI (T7 Revision 8, SESSION 36)
# Wraps `python -m polyarb.cli_arbitrage` with sensible defaults.
# Paper-mode by default — no real venue connections (T5+ scope).
# ═══════════════════════════════════════════════════════════════════════════

## eval-arb: synth signal → RoutingEngine → print routed decision (no exec).
##
## Usage:
##   make eval-arb                                  # all defaults: mid=0.5, stake=1000, legs=2
##   make eval-arb mid=0.45 stake=500 legs=3        # override
##   make eval-arb venue=clob min_threshold_pct=0.5 # explore CLOB routing + lower gate
eval-arb:
	@MID="$${mid:-0.5}"; STAKE="$${stake:-1000}"; LEGS="$${legs:-2}"; VENUE="$${venue:-polymarket}"; THR="$${min_threshold_pct:-1.0}"; \
	echo ">> eval-arb mid=$$MID stake=$$STAKE legs=$$LEGS venue=$$VENUE threshold=$$THR%"; \
	uv run python -m polyarb.cli_arbitrage evaluate --mid $$MID --stake $$STAKE --legs $$LEGS --venue $$VENUE --min-threshold-pct $$THR
.PHONY: eval-arb

## run-arb: synth signal → route → execute via paper executor → print result.
##
## Paper-mode default; no orders go to any exchange. Use `make eval-arb` first
## if you just want the routed plan without the execution result.
##
## T5 (SESSION 37): set `paper_close=1` to synth a Fill at each leg's
## estimated_price → exercise full open→close lifecycle (zero PnL realized,
## but position closes). Without it, positions accumulate as open.
##
## Usage:
##   make run-arb db=data/m2-positions.db           # durable paper-mode state
##   make run-arb paper_close=1                     # full lifecycle: open then close at est. price
##   make run-arb mid=0.45 stake=500 retries=1      # tighten retry budget
run-arb:
	@MID="$${mid:-0.5}"; STAKE="$${stake:-1000}"; LEGS="$${legs:-2}"; VENUE="$${venue:-polymarket}"; THR="$${min_threshold_pct:-1.0}"; RETRIES="$${retries:-3}"; PAPER_CLOSE_FLAG=""; \
	if [ -n "$${paper_close}" ] && [ "$${paper_close}" != "0" ]; then PAPER_CLOSE_FLAG="--paper-close"; fi; \
	SIGNAL_FLAG=""; if [ -n "$${signal_id}" ]; then SIGNAL_FLAG="--signal-id $${signal_id}"; fi; \
	echo ">> run-arb (paper) db=$(if $(strip $(db)),$(db),data/m2-positions.db) mid=$$MID stake=$$STAKE legs=$$LEGS venue=$$VENUE retries=$$RETRIES paper_close=$${paper_close:-0}"; \
	uv run python -m polyarb.cli_arbitrage run --mid $$MID --stake $$STAKE --legs $$LEGS --venue $$VENUE --min-threshold-pct $$THR --retries $$RETRIES --retry-delay 0 --db-path "$(if $(strip $(db)),$(db),data/m2-positions.db)" $$SIGNAL_FLAG $$PAPER_CLOSE_FLAG
.PHONY: run-arb

## status-arb: dump current PositionTracker state.
##
## T5 (SESSION 37): now includes realized PnL, balance, ROI %, max exposure,
## and stop-loss event (if triggered) — not just open positions.
##
## State is shared across processes through SQLite. Override with db=<path>.
status-arb:
	@echo ">> status-arb db=$(if $(strip $(db)),$(db),data/m2-positions.db)"
	@uv run python -m polyarb.cli_arbitrage status --db-path "$(if $(strip $(db)),$(db),data/m2-positions.db)"
.PHONY: status-arb

## close-arb: close an open position via synthesized Fill (operator-driven).
##
## Uses the same durable SQLite paper account as run-arb/status-arb.
##
## Usage:
##   make close-arb db=data/m2-positions.db market_id=cond-0 exit_price=0.55
##   make close-arb db=data/m2-positions.db market_id=cond-0 exit_price=0.55 operation_id=close-001
##   make close-arb db=data/m2-positions.db market_id=cond-0 exit_price=0.55 size=30 fill_id=venue-001
##   make close-arb db=data/m2-positions.db market_id=cond-0 exit_price=0.99 size=30 fill_id=venue-001 venue_cash=13.80 venue_fee=.30 venue_status=CONFIRMED venue_ref=trade-001
close-arb:
	@if [ -z "$${market_id}" ] || [ -z "$${exit_price}" ]; then \
		echo "usage: make close-arb db=<path> market_id=<id> exit_price=<0..1> [size=<shares>] [fill_id=<venue-id> | operation_id=<immutable-id>] [venue_cash=<gross> venue_fee=<fee> venue_status=CONFIRMED venue_ref=<trade-id>]"; \
		exit 1; \
	fi; \
	echo ">> close-arb db=$(if $(strip $(db)),$(db),data/m2-positions.db) market_id=$${market_id} exit_price=$${exit_price}"; \
	SIZE_FLAG=""; \
	if [ -n "$${size}" ]; then SIZE_FLAG="--size $${size}"; fi; \
	if [ -n "$(strip $(venue_cash)$(venue_fee)$(venue_status)$(venue_ref))" ]; then \
		uv run python -m polyarb.cli_arbitrage close --market-id "$${market_id}" --exit-price $${exit_price} --db-path "$(if $(strip $(db)),$(db),data/m2-positions.db)" --fill-id "$(fill_id)" --venue-cash "$(venue_cash)" --venue-fee "$(venue_fee)" --venue-status "$(venue_status)" --venue-ref "$(venue_ref)" $$SIZE_FLAG; \
	elif [ -n "$(strip $(fill_id))" ]; then \
		uv run python -m polyarb.cli_arbitrage close --market-id "$${market_id}" --exit-price $${exit_price} --db-path "$(if $(strip $(db)),$(db),data/m2-positions.db)" --fill-id "$(fill_id)" $$SIZE_FLAG; \
	elif [ -n "$(strip $(operation_id))" ]; then \
		uv run python -m polyarb.cli_arbitrage close --market-id "$${market_id}" --exit-price $${exit_price} --db-path "$(if $(strip $(db)),$(db),data/m2-positions.db)" --operation-id "$(operation_id)" $$SIZE_FLAG; \
	else \
		uv run python -m polyarb.cli_arbitrage close --market-id "$${market_id}" --exit-price $${exit_price} --db-path "$(if $(strip $(db)),$(db),data/m2-positions.db)" $$SIZE_FLAG; \
	fi
.PHONY: close-arb
