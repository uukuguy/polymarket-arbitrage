# M1 Business Research Density — Delivery Summary

## Delivered locally

- Versioned `Structure` and `Quote` research pages are pointer-gated and
  bounded to one current generation.
- Each Structure-range and Quote-batch receipt writes its dense research rows
  in the same transaction. An uncommitted range or batch cannot surface in the
  Dashboard.
- The Dashboard displays component counts, lineage and a horizontal,
  high-density 100-row research table. Missing historical materialization is
  explicit and is never rendered as a zero-row business fact.
- The runtime database capability contract grants only `SELECT, INSERT` to the
  two append-only research indexes; it grants neither update nor delete.

## Verification

- `pytest` focused on migration, role contract, Postgres page/receipt,
  transport, Structure worker and Quote worker: passed.
- `pnpm --dir dashboard run typecheck` and production `build`: passed.
- Playwright: local `/business/structure` screenshot at
  `.worktrees/m1-business-density/.playwright-cli/page-2026-09-01T09-19-49-911Z.png`;
  it confirms the current summary and explicitly labels the not-yet-deployed
  detail index as unavailable rather than zero.

## Remaining rollout boundary

The migration and six-app control-plane rollout have not been performed by
this local delivery. The already-published production generation predates the
new receipt-coupled index, so production detail must remain unavailable until
a controlled historical materialization or a subsequent fully certified
generation produces the index.
