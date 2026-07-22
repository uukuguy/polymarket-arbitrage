"""L3 promote dry-run — single tick with every mutation disabled.

Used by `make l3-promote-dry-run` (Phase 05 Plan 05-05 Task 4).

Runs `polyarb.observation.l3_promote.promote_run` once against the prod
Supabase candidate set with ``apply_mutations=False``. The promoter may read
TOB and market identity, but cannot call WS methods, update candidates, or
mutate its module state/freshness anchors.

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

    def set_l3_desired(self, _asset_ids):
        raise AssertionError("dry-run must not mutate desired membership")

    def l3_membership_snapshot(self):
        raise AssertionError("dry-run must not inspect mutable consumer state")

    async def add_subscriptions(self, asset_ids):
        raise AssertionError(f"dry-run attempted add_subscriptions({len(asset_ids)})")

    async def remove_subscriptions(self, asset_ids):
        raise AssertionError(f"dry-run attempted remove_subscriptions({len(asset_ids)})")


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
        apply_mutations=False,
    )
    print(f"\npromote_run result: {result}")
    print(f"proposed_active: {result.get('proposed_active', [])}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(_main()))
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception as e:  # noqa: BLE001
        print(f"l3-promote-dry-run FAILED: {e!r}", file=sys.stderr)
        sys.exit(1)
