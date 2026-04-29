# Polymarket Arbitrage — Unified command entry point
#
# Convention: all executable commands are exposed here. Users should never
# need to remember `python -m xxx --flag a --flag b` style invocations.
#
# Naming: make <verb>-<noun>, e.g., make snapshot-markets, make scan-arb
# Each target above has a comment explaining purpose and typical scenario.
# `make help` always lists all available commands.

.DEFAULT_GOAL := help
.PHONY: help

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

.PHONY: snapshot-markets snapshot-markets-full

## snapshot-markets: Capture Polymarket snapshot (subset mode, liquidity > $1k, ~10-20 min)
snapshot-markets:
	python -m polyarb.snapshot

## snapshot-markets-full: Capture Polymarket snapshot (FULL mode, all markets, ~1-2 hours)
snapshot-markets-full:
	python -m polyarb.snapshot --full
