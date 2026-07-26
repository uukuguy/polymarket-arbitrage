"""Tests for Phase 03.1 Plan 02 GAP-103 — snapshots.notes derivation from L1 issues.

Operators need a one-line "why did this snapshot fail" string in the snapshots
table so dashboards can `GROUP BY substr(notes, 1, 40)` and tally failure
modes. Today snapshots.notes is always NULL — `is_valid=false` is exposed but
the reason is not.

This module tests the `_derive_notes_from_issues` pure helper in isolation
(no orchestrator integration test needed — helper is small + deterministic).
"""

from __future__ import annotations


def _make_issue(layer: int, category_value: str, detail: str, market_id: str | None = None):
    """Construct an Issue from category string value (avoids fixture coupling)."""
    from polyarb.validator.category import Category, Issue

    cat = Category(category_value)  # convert "api_unreachable" → Category.API_UNREACHABLE
    return Issue(layer=layer, category=cat, market_id=market_id, detail=detail)


# ─────────────────────────────────────────────────────────────────────────────
# Test 1 — empty issues → None
# ─────────────────────────────────────────────────────────────────────────────


def test_derive_notes_empty_returns_none() -> None:
    """No issues at all → notes stays NULL in snapshots (clean snapshot)."""
    from polyarb.snapshot.orchestrator import _derive_notes_from_issues

    assert _derive_notes_from_issues([]) is None


# ─────────────────────────────────────────────────────────────────────────────
# Test 2 — single API_UNREACHABLE issue → reason string
# ─────────────────────────────────────────────────────────────────────────────


def test_derive_notes_single_api_unreachable() -> None:
    """One API_UNREACHABLE issue → returns the detail (truncated to 80 chars)."""
    from polyarb.snapshot.orchestrator import _derive_notes_from_issues

    issues = [
        _make_issue(
            layer=1,
            category_value="api_unreachable",
            detail="Gamma unreachable: ConnectError([Errno -5] EAI_NODATA)",
        )
    ]
    out = _derive_notes_from_issues(issues)
    assert out is not None
    assert "Gamma" in out
    assert "ConnectError" in out
    assert len(out) <= 200


# ─────────────────────────────────────────────────────────────────────────────
# Test 3 — multiple API_UNREACHABLE issues → semicolon-joined, <=200 chars
# ─────────────────────────────────────────────────────────────────────────────


def test_derive_notes_multiple_api_unreachable_joined() -> None:
    """Multiple unreachable issues → semicolon-joined; total stays <=200 chars."""
    from polyarb.snapshot.orchestrator import _derive_notes_from_issues

    issues = [
        _make_issue(
            layer=1, category_value="api_unreachable", detail="Gamma unreachable: ConnectError(...)"
        ),
        _make_issue(
            layer=4, category_value="api_unreachable", detail="CLOB unreachable: TimeoutError(...)"
        ),
    ]
    out = _derive_notes_from_issues(issues)
    assert out is not None
    assert ";" in out, "expected semicolon-joined reasons"
    assert "Gamma" in out and "CLOB" in out
    assert len(out) <= 200


# ─────────────────────────────────────────────────────────────────────────────
# Test 4 — non-API issues (zombie_market / ghost_book) are NOT included
# ─────────────────────────────────────────────────────────────────────────────


def test_derive_notes_excludes_non_api_issues() -> None:
    """Layer 2 validation issues (e.g. ZOMBIE_MARKET) must NOT appear in notes —
    operators want fail reasons (data source unreachable), not validation noise."""
    from polyarb.snapshot.orchestrator import _derive_notes_from_issues

    issues = [
        _make_issue(
            layer=2, category_value="zombie_market", detail="market past endDate but still active"
        ),
        _make_issue(layer=2, category_value="unknown", detail="some odd validator finding"),
    ]
    out = _derive_notes_from_issues(issues)
    assert out is None, f"non-API issues should produce no notes, got {out!r}"


# ─────────────────────────────────────────────────────────────────────────────
# Test 5 — truncation: very long detail strings stay under 200 chars total
# ─────────────────────────────────────────────────────────────────────────────


def test_derive_notes_truncates_to_200_chars() -> None:
    """Joined notes string must be <=200 chars even with many long issues."""
    from polyarb.snapshot.orchestrator import _derive_notes_from_issues

    long_detail = "X" * 300
    issues = [
        _make_issue(layer=1, category_value="api_unreachable", detail=long_detail),
        _make_issue(layer=4, category_value="api_unreachable", detail=long_detail),
        _make_issue(layer=1, category_value="api_unreachable", detail=long_detail),
    ]
    out = _derive_notes_from_issues(issues)
    assert out is not None
    assert len(out) <= 200, f"notes must be <=200 chars, got {len(out)}"
