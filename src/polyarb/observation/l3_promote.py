"""L3 promote module — module-level state + public getters.

Phase 05 D-02/D-09/D-14. This file ships in two parts:

- **Plan 05-03 (this plan):** SCAFFOLD — state + getters + STUB
  ``promote_run`` / ``run_periodic``. The mirror needs ``_last_book_levels_write_at_s``
  to exist NOW so the chain-truth anchor mutation compiles and is observable
  from /health (Plan 04 sub-check).
- **Plan 05-04:** AUGMENT — replace stub BODIES; module-level state
  declarations and getter functions MUST remain intact (Warning #6
  wave-2-delete-wave-3 anti-pattern prevention).

**Chain-truth contract** (CLAUDE.md §chain-truth — every getter here reads a
field that the write side really mutates):

- ``_last_promote_at_s``            ← ``promote_run`` success (Plan 05-04)
- ``_last_book_levels_write_at_s``  ← ``L2SupabaseMirror.push_book_levels`` success (Plan 05-03)
- ``_l3_active_set``                ← ``promote_run`` success (Plan 05-04)

**Cold-start trap** (memory ``feedback_cold-start-debounce-trap-2026-05``):
both timestamp anchors use ``None`` as the sentinel — /health treats
``None`` as cold-start (warn), not as "fresh just now". **Never** initialize
either anchor to ``0.0`` — that would mark the daemon as healthy on the
first millisecond of process start before any successful write occurs.

Module-level state — single L2 daemon process. DURABLE: Plan 04 imports
and mutates these directly; do NOT delete or rename in any later plan.
"""
from __future__ import annotations

import asyncio
from typing import Any

# ── Module-level state (DURABLE — Plan 04 augments bodies but PRESERVES these) ──
_l3_active_set: set[str] = set()
_last_promote_at_s: float | None = None
_last_book_levels_write_at_s: float | None = None


# ── Public getters — DURABLE; consumed by /health (Plan 04) and dashboard ──


def get_l3_active_set() -> set[str]:
    """Return a defensive copy of the L3 active asset_id set.

    Callers must NOT mutate the returned set — it is a copy of the module's
    private ``_l3_active_set``. The promoter (Plan 04) is the sole writer.
    """
    return set(_l3_active_set)


def get_l3_active_count() -> int:
    """Cardinality of the L3 active set — read by /health l3:active_count."""
    return len(_l3_active_set)


def get_last_promote_at_s() -> float | None:
    """Wall-clock seconds since epoch of the last successful ``promote_run``.

    Returns ``None`` if the promoter has never run (cold-start). The /health
    sub-check (Plan 04) maps this to pass/warn/fail by age.
    """
    return _last_promote_at_s


def get_last_book_levels_write_at_s() -> float | None:
    """Wall-clock seconds since epoch of the last successful book_levels write.

    Mutated by ``L2SupabaseMirror.push_book_levels`` (Plan 05-03 success path)
    — chain-truth anchor for /health l3:last_book_levels_write_at_s.
    Returns ``None`` if no write has succeeded yet (cold-start sentinel).
    """
    return _last_book_levels_write_at_s


def is_book_levels_write_overdue(now_s: float, warn_s: float = 120.0) -> bool:
    """True when the last book_levels write is older than ``warn_s`` (or never).

    Predicate consumed by /health l3:last_book_levels_write_at_s (Plan 04).
    Exposed here so Plan 03 callers can also use it in tests or local
    diagnostics. The ``warn_s`` default is the 2-minute pass/warn threshold
    (Plan 04 may override per its /health policy).
    """
    if _last_book_levels_write_at_s is None:
        return True
    return (now_s - float(_last_book_levels_write_at_s)) >= warn_s


# ── STUB API — Plan 04 REPLACES bodies but PRESERVES state + getters above ──


async def promote_run(*args: Any, **kwargs: Any) -> dict:
    """STUB — real implementation in Plan 05-04.

    Returns a no-op diff so any early-integration caller (Plan 03 doesn't
    invoke this, but future tests might) sees a consistent shape.
    """
    return {"added": [], "removed": [], "stub": True}


async def run_periodic(*args: Any, **kwargs: Any) -> None:
    """STUB — real cron loop implementation in Plan 05-04.

    Respects a ``stop_event`` if passed via kwargs so daemon shutdown does
    not hang in early integration tests that wire this into ``asyncio.create_task``.
    """
    stop_event = kwargs.get("stop_event")
    if stop_event is not None and isinstance(stop_event, asyncio.Event):
        await stop_event.wait()
