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

# ─────────────────────────────────────────────────────────────────────────────
# M1-perception Phase 01: market snapshot tool
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: snapshot-markets snapshot-markets-v snapshot-markets-full snapshot-markets-full-v snapshot-status snapshot-fresh snapshot-cache-purge test test-snapshot test-signal test-slippage

## snapshot-markets: Capture snapshot (subset, liquidity > $1k, ~15-30 min). Quiet, cron-friendly.
snapshot-markets:
	@echo ">> snapshot-markets (quiet mode) — PID $$$$ — started $$(date '+%Y-%m-%d %H:%M:%S')"
	@echo ">> tip: open another terminal and run 'make snapshot-status' to check progress"
	@echo ""
	uv run python -m polyarb.snapshot

## snapshot-markets-v: Same as snapshot-markets but with progress logs (recommended for interactive runs)
snapshot-markets-v:
	@echo ">> snapshot-markets-v (verbose mode) — PID $$$$ — started $$(date '+%Y-%m-%d %H:%M:%S')"
	@echo ""
	uv run python -m polyarb.snapshot --verbose

## snapshot-markets-full: Capture snapshot (FULL mode, all markets, ~1-2 hours). Quiet.
snapshot-markets-full:
	@echo ">> snapshot-markets-full (quiet mode) — PID $$$$ — started $$(date '+%Y-%m-%d %H:%M:%S')"
	@echo ">> tip: this may take 1-2 hours. Use 'make snapshot-status' to check progress."
	@echo ""
	uv run python -m polyarb.snapshot --full

## snapshot-markets-full-v: FULL mode with progress logs
snapshot-markets-full-v:
	@echo ">> snapshot-markets-full-v (verbose mode) — PID $$$$ — started $$(date '+%Y-%m-%d %H:%M:%S')"
	@echo ""
	uv run python -m polyarb.snapshot --full --verbose

## snapshot-status: One-glance status — running process, recent SQLite rows, latest parquet (local time)
snapshot-status:
	@uv run python scripts/snapshot_status.py

## snapshot-fresh: Force full refetch (purge all caches, then run verbose)
snapshot-fresh:
	@echo ">> snapshot-fresh — purging caches, then verbose run"
	@echo ""
	uv run python -m polyarb.snapshot --no-cache --verbose

## snapshot-cache-purge: Delete all data/.cache/snapshot-* directories without running
snapshot-cache-purge:
	@uv run python -c "from pathlib import Path; from polyarb.snapshot.cache import ChunkCache; n = ChunkCache.purge_all(Path('data/.cache')); print(f'purged {n} cache directories')"

# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

## test: Run all tests
test:
	@uv run pytest -v tests/

## test-slippage: Run slippage model tests
test-slippage:
	@uv run pytest -v tests/models/test_slippage.py
