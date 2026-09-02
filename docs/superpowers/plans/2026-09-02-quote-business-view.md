# Quote Business View Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the current Quote research page into a dense operator view of executable pricing evidence.

**Architecture:** Reuse the fenced `m1_business_quote_rows` page; add a pure dashboard projection that groups existing quote facts into meaningful columns. No raw-data mirror, new database relation, or production writer is introduced.

**Tech Stack:** Next.js 15, TypeScript, existing control-plane Quote API.

## Global Constraints

- Current Quote generation remains pointer-fenced; unavailable is never rendered as zero.
- Display missing price/depth as unknown, never as `0`.
- Preserve cursor pagination and do not add a new Postgres projection.

---

### Task 1: Quote business table

**Files:**
- Modify: `dashboard/app/business/quotes/page.tsx`
- Test: `dashboard` TypeScript build

**Interfaces:** Consumes `ResearchPage` items with `slug`, `event_id`, `market_id`, `best_ask_price`, `best_ask_size`, `terminal_state`, and `neg_risk_market_id`.

- [ ] Replace raw token/hash columns with Market, executable ask/depth, execution state, event, and neg-risk group columns.
- [ ] Render missing values as `—`; render `terminal_state === "executable"` as available.
- [ ] Retain current generation and lineage card, and make the indexed-count label distinguish loaded rows from current quote records.
- [ ] Run `make dashboard-typecheck dashboard-build` and inspect `/business/quotes` in the existing browser session.
- [ ] Commit with `feat(m1): render quote business evidence`.
