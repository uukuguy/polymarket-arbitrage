"""Phase 03.1-04 — DNS-class ConnectError retry on Gamma /markets fetch.

Source: Sentry issue 121111789 (6 days, 3 occurrences). EAI_NODATA / EAI_AGAIN
fires on Fly machine, snapshot trips API_UNREACHABLE → after 3 ticks PAUSED.

D-01 modify A: tenacity retry the Gamma stream START, but ONLY for DNS-class
ConnectError shapes. Non-DNS ConnectError (connection refused, host
unreachable) must NOT be retried — those signal real upstream outages and the
existing fail-soft Issue(API_UNREACHABLE) path must remain intact (chain-truth
discipline; see feedback_code-vs-chain-truth-2026-05).

Coverage:
  1-4: pure predicate tests on _is_dns_jitter (fast, no network)
  5  : DNS jitter on first attempt → retry succeeds → no issue
  6  : non-DNS ConnectError → NO retry → issue appended (fail-soft preserved)
  7  : DNS jitter persists past stop_after_attempt(3) → existing except clause
       fires → API_UNREACHABLE issue appended (bounded retry, no infinite loop)
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import httpx
import pytest

os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")

from polyarb.snapshot.orchestrator import _is_dns_jitter  # noqa: E402

# ---------------------------------------------------------------------------
# Tests 1-4: pure predicate tests on _is_dns_jitter (fast)
# ---------------------------------------------------------------------------


def test_predicate_matches_eai_nodata() -> None:
    """Errno -5 shape (EAI_NODATA) — the dominant Sentry 121111789 shape."""
    exc = httpx.ConnectError("[Errno -5] No address associated with hostname")
    assert _is_dns_jitter(exc) is True


def test_predicate_matches_eai_again() -> None:
    """Errno -3 shape (EAI_AGAIN) — transient DNS resolver hiccup."""
    exc = httpx.ConnectError("[Errno -3] Temporary failure in name resolution")
    assert _is_dns_jitter(exc) is True


def test_predicate_rejects_connection_refused() -> None:
    """Connection refused is NOT DNS — must NOT be retried (real outage signal)."""
    exc = httpx.ConnectError("[Errno 111] Connection refused")
    assert _is_dns_jitter(exc) is False


def test_predicate_rejects_timeout_error() -> None:
    """TimeoutError is not even a ConnectError — must short-circuit False."""
    assert _is_dns_jitter(TimeoutError("read timed out")) is False


def test_predicate_matches_textual_variants() -> None:
    """Some httpx versions surface text-only error messages."""
    assert _is_dns_jitter(httpx.ConnectError("EAI_NODATA")) is True
    assert _is_dns_jitter(httpx.ConnectError("EAI_AGAIN")) is True
    assert _is_dns_jitter(httpx.ConnectError("Name or service not known")) is True
    assert _is_dns_jitter(httpx.ConnectError("Temporary failure in name resolution")) is True


# ---------------------------------------------------------------------------
# Tests 5-7: orchestrator-level retry behavior on Gamma /markets stream
# ---------------------------------------------------------------------------


def _make_settings(tmp_path: Path) -> Any:
    """Minimal Settings stub for orchestrator tests."""
    from pydantic import SecretStr

    from polyarb.config import Settings

    return Settings(
        db_path=tmp_path / "state.db",
        parquet_root=tmp_path / "snapshots",
        cache_root=tmp_path / "cache",
        retry_attempts=1,
        retry_min_wait_s=0.001,
        retry_max_wait_s=0.005,
        http_timeout_s=2.0,
        liquidity_threshold_usd=100.0,
        scan_shared_secret=SecretStr(
            "a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456"
        ),
    )


class _FakeGamma:
    """Async-context-managed GammaClient stub.

    fetch_all_active_events returns []. iter_active_markets behaves per
    a programmable side-effect (raise N times then yield N markets).
    """

    def __init__(self, fail_sequence: list[BaseException | None]) -> None:
        # fail_sequence: per *attempt*; None means "succeed on this attempt".
        self._fail_sequence = list(fail_sequence)
        self.attempt_count = 0

    async def __aenter__(self) -> _FakeGamma:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def fetch_all_active_events(self) -> list[dict]:
        return []

    def iter_active_markets(self):
        # Each call to iter_active_markets is one tenacity attempt.
        idx = self.attempt_count
        self.attempt_count += 1
        if idx < len(self._fail_sequence) and self._fail_sequence[idx] is not None:
            raise_exc = self._fail_sequence[idx]

            async def _gen():
                raise raise_exc
                yield  # pragma: no cover — make it a generator

            return _gen()

        async def _gen():
            return
            yield  # pragma: no cover — empty generator

        return _gen()


@pytest.mark.asyncio
async def test_dns_jitter_first_attempt_retries_and_succeeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """First iter_active_markets raises EAI_NODATA, second succeeds → no issue, retry consumed."""
    from polyarb.snapshot import orchestrator as orch

    eai_nodata = httpx.ConnectError("[Errno -5] No address associated with hostname")
    fake = _FakeGamma(fail_sequence=[eai_nodata, None])

    monkeypatch.setattr(orch, "GammaClient", lambda settings: fake)

    settings = _make_settings(tmp_path)
    result = await orch.run_snapshot(settings, mode="subset")

    # Two attempts consumed
    assert fake.attempt_count == 2, (
        f"expected 2 iter_active_markets attempts (1 retry), got {fake.attempt_count}"
    )
    # No API_UNREACHABLE issue because the retry succeeded
    cats = result.issue_categories
    assert "api_unreachable" not in cats or cats.get("api_unreachable", 0) == 0, (
        f"DNS jitter retry should have absorbed the failure, got issue_categories={cats}"
    )


@pytest.mark.asyncio
async def test_non_dns_connect_error_NOT_retried(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Connection refused → existing fail-soft path → 1 attempt, API_UNREACHABLE issue appended."""
    from polyarb.snapshot import orchestrator as orch

    refused = httpx.ConnectError("[Errno 111] Connection refused")
    fake = _FakeGamma(fail_sequence=[refused, None])  # 2nd entry never reached

    monkeypatch.setattr(orch, "GammaClient", lambda settings: fake)

    settings = _make_settings(tmp_path)
    result = await orch.run_snapshot(settings, mode="subset")

    assert fake.attempt_count == 1, (
        f"non-DNS ConnectError must NOT trigger retry; expected 1 attempt, got {fake.attempt_count}"
    )
    assert result.issue_categories.get("api_unreachable", 0) >= 1, (
        f"non-DNS ConnectError must surface as API_UNREACHABLE issue, got {result.issue_categories}"
    )


@pytest.mark.asyncio
async def test_dns_jitter_exhausts_retries_then_appends_issue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """EAI_NODATA on all 3 attempts → tenacity reraises → existing except clause appends issue."""
    from polyarb.snapshot import orchestrator as orch

    eai_nodata = httpx.ConnectError("[Errno -5] No address associated with hostname")
    # 4 entries: tenacity should stop at 3.
    fake = _FakeGamma(fail_sequence=[eai_nodata, eai_nodata, eai_nodata, eai_nodata])

    monkeypatch.setattr(orch, "GammaClient", lambda settings: fake)

    settings = _make_settings(tmp_path)
    result = await orch.run_snapshot(settings, mode="subset")

    assert fake.attempt_count == 3, (
        f"tenacity stop_after_attempt(3) must cap retries at 3, got {fake.attempt_count}"
    )
    assert result.issue_categories.get("api_unreachable", 0) >= 1, (
        f"after exhausting retries, fail-soft path must surface API_UNREACHABLE issue, "
        f"got {result.issue_categories}"
    )
