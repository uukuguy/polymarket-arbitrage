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
# Phase commands (populated as phases are implemented)
# ─────────────────────────────────────────────────────────────────────────────

# M1-P01 targets will be added here after discuss/plan completes.
# Example placeholder:
#   ## snapshot-markets: Capture full Polymarket market snapshot to parquet
#   snapshot-markets:
#       python -m polymarket.snapshot --output data/snapshots/$(shell date +%Y-%m-%dT%H%M).parquet
