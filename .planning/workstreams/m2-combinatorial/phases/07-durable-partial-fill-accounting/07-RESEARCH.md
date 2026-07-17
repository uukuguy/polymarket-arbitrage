# Phase 7 Research — Durable Partial Fills

Official V2 FAK orders permit immediate partial execution; open orders expose cumulative
`size_matched`, trades expose immutable IDs and share `size`, and statuses progress to
CONFIRMED/FAILED. H-005 therefore models confirmed fill events, not mutable order
snapshots. The existing repository operation ledger already supplies transactional
deduplication when fill ID becomes the canonical operation ID.

Primary references:

- https://docs.polymarket.com/trading/orders/create
- https://docs.polymarket.com/trading/orders/overview
- https://docs.polymarket.com/market-data/websocket/user-channel

