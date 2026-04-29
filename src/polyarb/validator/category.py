"""Validator categories + Issue dataclass.

Per CONTEXT.md D-D4, every Issue MUST have a non-UNKNOWN category in steady state.
Persistent UNKNOWN issues are a system debt — see RESEARCH.md (Pattern 5) and
threads/data-quality.md ("规则 > 阈值" — categorize rather than ignore).

The (str, Enum) mixin lets each Category serialize directly to SQLite TEXT.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Category(str, Enum):
    ZOMBIE_MARKET = "zombie_market"
    RESOLVING = "resolving"
    API_JITTER = "api_jitter"
    API_UNREACHABLE = "api_unreachable"
    CLOB_MISSING = "clob_missing"
    GHOST_BOOK = "ghost_book"  # ⚠️ issue #180 defense (RESEARCH.md Pitfall 1)
    UNKNOWN = "unknown"  # never tolerate persistent unknowns — converge to specifics


@dataclass(frozen=True)
class Issue:
    """A single validation finding — one row in the validation_issues table.

    layer       — 1 (count), 2 (fields), or 4 (cross-source); Layer 3 is deferred to Phase 3
    category    — root-cause class; UNKNOWN means we don't yet know why
    market_id   — None for non-market-scoped issues (e.g. Layer 1 count mismatch)
    detail      — free-form short description (truncated by validator to 200 chars)
    raw_payload — JSON snippet of the offending payload (truncated to 1024 bytes)
    """

    layer: int
    category: Category
    market_id: str | None
    detail: str
    raw_payload: str | None = None
