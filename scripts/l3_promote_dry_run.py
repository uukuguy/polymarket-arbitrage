"""L3 promote dry-run — single tick, no real WS subscribe mutation.

Used by `make l3-promote-dry-run` (Phase 05 Plan 05-05 Task 4).

Runs `polyarb.observation.l3_promote.promote_run` once against the prod
Supabase candidate set, but swaps in a no-op consumer so no real
add_subscriptions / remove_subscriptions call leaves the box. Prints
WOULD-add / WOULD-remove + the resulting sorted active set so the
operator can sanity-check the L3 selection logic without touching prod
WS state.

Prerequisites:
- `.env` (or environment) provides POLYARB_SUPABASE_URL + service-role key
  (the helper needs to read l2_top_of_book to seed the temp DB scan).
- The l3-promote.yaml recipe in src/polyarb/scan_recipes/.

Exit code 0 on success; non-zero on any exception (printed).
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path


class _NoopConsumer:
    """Stand-in for WsConsumer — logs intent, performs no real subscribe."""

    async def add_subscriptions(self, asset_ids):
        print(f"WOULD add_subscriptions({len(asset_ids)}): {sorted(asset_ids)}")
        return True

    async def remove_subscriptions(self, asset_ids):
        print(
            f"WOULD remove_subscriptions({len(asset_ids)}): {sorted(asset_ids)}"
        )
        return True


async def _main() -> int:
    from polyarb.config import load_settings  # noqa: WPS433 — runtime import
    from polyarb.observation import l3_promote

    settings = load_settings()

    recipe = Path("src/polyarb/scan_recipes/l3-promote.yaml")
    if not recipe.exists():
        print(f"ABORT: recipe missing: {recipe}", file=sys.stderr)
        return 2

    consumer = _NoopConsumer()
    result = await l3_promote.promote_run(
        settings=settings,
        ws_consumer=consumer,
        recipe_yaml_path=recipe,
    )
    print(f"\npromote_run result: {result}")
    print(
        "_l3_active_set after dry-run: "
        f"{sorted(l3_promote.get_l3_active_set())}"
    )
    print(f"_l3_active_count: {l3_promote.get_l3_active_count()}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(_main()))
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception as e:  # noqa: BLE001
        print(f"l3-promote-dry-run FAILED: {e!r}", file=sys.stderr)
        sys.exit(1)
