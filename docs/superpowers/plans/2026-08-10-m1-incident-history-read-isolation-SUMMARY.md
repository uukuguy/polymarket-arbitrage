# M1 Incident History Read Isolation — Summary

Date: 2026-08-10

## Production fault

During active Quote writes, the direct console's recovered-incident endpoint
intermittently returned `503 read-model-unavailable`: two of five successive
requests failed. The generic perception lane and its one-second/0.8-second
read budgets protected market projections, but made the recovery record too
fragile for an operator-facing P1 console.

## Fix

Incident list, identity history, and recovered-incident queries now use an
independent bounded `incident-read` executor lane. These small, security-
validated operator reads receive a three-second outer timeout and a 2.5-second
SQLite interrupt deadline. Broad perception projections retain their existing
one-second lane and are unable to consume incident-read slots.

## Verification

- RED: a new route-contract test failed before the specialized lane/budgets
  existed.
- GREEN: focused console, recent-history, dedicated-lane, and default-executor
  starvation tests pass; Ruff and scoped diff checks pass.
- Required production proof after deployment: repeat recent Quote history while
  Quote is collecting, then verify the direct console renders complete recovery
  history rather than returning an unavailable envelope.
