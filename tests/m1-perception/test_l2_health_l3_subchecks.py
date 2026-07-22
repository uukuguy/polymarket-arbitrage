"""Tests for Phase 05 Plan 04 — /health 3 new L3 sub-checks (chain-truth).

Chain-truth contract (CLAUDE.md §chain-truth + Phase 04 D-08, Inj L2-2 RCA):

  l3:active_count                  ← l3_promote.get_l3_active_count()
  l3:last_promote_at_s             ← l3_promote.get_last_promote_at_s()
  l3:last_book_levels_write_at_s   ← l3_promote.get_last_book_levels_write_at_s()

Each getter reads a module-level field that the WRITE side really mutates
(promote_run success path + L2SupabaseMirror.push_book_levels success
path). NO config-flag gating between mutation and surface.

active_count uses L3_EXPECTED_TOKEN_COUNT = 10 (revision-1 strict — D-05
N=5 markets × 2 Yes+No tokens). active_count < 10 → status=warn,
informational only (does NOT bump overall). last_promote_at_s + book_levels
DO bump overall when stale.
"""
from __future__ import annotations

import os
import re
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")


HEALTH_MODULE_PATH = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "polyarb"
    / "http"
    / "l2_health.py"
)


@pytest.fixture(autouse=True)
def _reset_l3_promote_state() -> Any:
    """Reset module-level state between tests."""
    from polyarb.observation import l3_promote

    l3_promote._l3_active_set = set()
    l3_promote._last_promote_at_s = None
    l3_promote._last_book_levels_write_at_s = None
    if hasattr(l3_promote, "_last_known_tob_rows"):
        l3_promote._last_known_tob_rows = None
    if hasattr(l3_promote, "_last_known_market_token_map"):
        l3_promote._last_known_market_token_map = None
    yield


def _make_settings() -> Any:
    """Minimal Settings for /health builder."""
    from polyarb.config import Settings

    return Settings()


def _call_build_checks(
    now_s: float,
    *,
    evidence_runtime: Any | None = None,
    evidence_runtime_required: bool = False,
) -> tuple[dict, str]:
    from polyarb.http.l2_health import _build_l2_health_checks

    settings = _make_settings()
    store = MagicMock()
    store.get_l2_tob_last_mirror_at_s.return_value = None
    return _build_l2_health_checks(
        store,
        settings,
        ws_consumer=None,
        event_listener=None,
        now_s=now_s,
        evidence_runtime=evidence_runtime,
        evidence_runtime_required=evidence_runtime_required,
    )


# ─────────────────────────────────────────────────────────────────────────
# l3:active_count
# ─────────────────────────────────────────────────────────────────────────


def test_health_l3_active_count_cold_start_warn() -> None:
    from polyarb.observation import l3_promote

    l3_promote._l3_active_set = set()
    checks, _overall = _call_build_checks(time.time())
    assert "l3:active_count" in checks
    entry = checks["l3:active_count"][0]
    assert entry["status"] == "warn"
    assert "0/10" in entry["output"]
    assert entry["observedValue"] == 0


def test_health_l3_active_count_full_pass_at_10_tokens() -> None:
    from polyarb.observation import l3_promote

    l3_promote._l3_active_set = {f"t{i}" for i in range(10)}
    checks, _ = _call_build_checks(time.time())
    entry = checks["l3:active_count"][0]
    assert entry["status"] == "pass"
    assert entry["observedValue"] == 10


def test_health_l3_active_count_under_filled_warn_at_9_tokens() -> None:
    from polyarb.observation import l3_promote

    l3_promote._l3_active_set = {f"t{i}" for i in range(9)}
    checks, _ = _call_build_checks(time.time())
    entry = checks["l3:active_count"][0]
    assert entry["status"] == "warn"
    assert "9/10" in entry["output"]


def test_health_l3_active_count_does_not_bump_overall_when_under_filled() -> None:
    """Informational-only — active_count < 10 must NOT bump overall to warn
    purely on its own. (Other sub-checks may still warn — we test that this
    specific sub-check is not the cause.)"""
    from polyarb.observation import l3_promote

    # Set last_promote_at + last_book_levels_at to FRESH so those sub-checks
    # are pass; if `overall` bumps, it must be l3:active_count's fault.
    now = time.time()
    l3_promote._l3_active_set = set()  # 0 tokens — under-filled
    l3_promote._last_promote_at_s = now - 5  # fresh
    l3_promote._last_book_levels_write_at_s = now - 5  # fresh

    checks, overall = _call_build_checks(now)
    assert checks["l3:active_count"][0]["status"] == "warn"
    assert checks["l3:last_promote_at_s"][0]["status"] == "pass"
    assert checks["l3:last_book_levels_write_at_s"][0]["status"] == "pass"
    # Other unrelated sub-checks (ws/event_bus = not configured = warn) may
    # still bump overall; but the l3 trio alone should NOT cause `fail`.
    assert overall != "fail", f"l3 trio must not propagate fail; got overall={overall}"


# ─────────────────────────────────────────────────────────────────────────
# l3:last_promote_at_s
# ─────────────────────────────────────────────────────────────────────────


def test_health_l3_last_promote_cold_start_warn_but_overall_not_fail() -> None:
    from polyarb.observation import l3_promote

    l3_promote._last_promote_at_s = None
    l3_promote._last_book_levels_write_at_s = time.time() - 5  # fresh
    checks, overall = _call_build_checks(time.time())
    entry = checks["l3:last_promote_at_s"][0]
    assert entry["status"] == "warn"
    assert "cold-start" in entry["output"]
    assert overall != "fail"


def test_health_l3_last_promote_fresh_pass() -> None:
    from polyarb.observation import l3_promote

    now = time.time()
    l3_promote._last_promote_at_s = now - 60
    l3_promote._last_book_levels_write_at_s = now - 5  # fresh
    checks, _ = _call_build_checks(now)
    entry = checks["l3:last_promote_at_s"][0]
    assert entry["status"] == "pass", f"got {entry['status']}: {entry['output']}"


def test_health_l3_last_promote_stale_fail() -> None:
    from polyarb.observation import l3_promote

    now = time.time()
    l3_promote._last_promote_at_s = now - 2000  # > 1800s = fail
    l3_promote._last_book_levels_write_at_s = now - 5  # fresh
    checks, overall = _call_build_checks(now)
    entry = checks["l3:last_promote_at_s"][0]
    assert entry["status"] == "fail", f"got {entry['status']}: {entry['output']}"
    assert overall == "fail", f"stale promote must propagate fail; got {overall}"


def test_health_l3_last_promote_borderline_warn() -> None:
    from polyarb.observation import l3_promote

    now = time.time()
    l3_promote._last_promote_at_s = now - 700  # > 600s warn, < 1800s fail
    l3_promote._last_book_levels_write_at_s = now - 5
    checks, _ = _call_build_checks(now)
    assert checks["l3:last_promote_at_s"][0]["status"] == "warn"


# ─────────────────────────────────────────────────────────────────────────
# l3:last_book_levels_write_at_s
# ─────────────────────────────────────────────────────────────────────────


def test_health_l3_book_levels_cold_start_warn() -> None:
    from polyarb.observation import l3_promote

    l3_promote._last_book_levels_write_at_s = None
    l3_promote._last_promote_at_s = time.time() - 5
    checks, _ = _call_build_checks(time.time())
    entry = checks["l3:last_book_levels_write_at_s"][0]
    assert entry["status"] == "warn"
    assert "cold-start" in entry["output"]


def test_health_l3_book_levels_stale_fail() -> None:
    from polyarb.observation import l3_promote

    now = time.time()
    l3_promote._last_book_levels_write_at_s = now - 1000  # > 600 fail
    l3_promote._last_promote_at_s = now - 5  # fresh so it isn't the cause
    checks, overall = _call_build_checks(now)
    entry = checks["l3:last_book_levels_write_at_s"][0]
    assert entry["status"] == "fail"
    assert overall == "fail", f"stale book_levels must propagate fail; got {overall}"


# ─────────────────────────────────────────────────────────────────────────
# Chain-truth — no config-flag gating (Inj L2-2 RCA)
# ─────────────────────────────────────────────────────────────────────────


def test_health_l3_subchecks_chain_truth_no_config_gate() -> None:
    """Lint test — within the L3 sub-check block in l2_health.py, there must
    be NO `getattr(settings, "l3_enabled"` or `settings.l3_*_enabled` gate.

    The sub-checks read getters that the WRITE side mutates — never a
    config flag (CLAUDE.md chain-truth + Phase 04 D-08 / GAP-200 RCA).
    """
    text = HEALTH_MODULE_PATH.read_text()

    # Find the L3 sub-check region (between the L3 D-08 marker comment and
    # the `return checks, overall` line). All three checks must live here.
    region_start = text.find('# ── Phase 05 Plan 04 D-08')
    assert region_start >= 0, "L3 sub-check region marker missing"
    region_end = text.find("return checks, overall", region_start)
    assert region_end >= 0
    region = text[region_start:region_end]

    # Each sub-check name present in the region.
    for name in (
        '"l3:active_count"',
        '"l3:last_promote_at_s"',
        '"l3:last_book_levels_write_at_s"',
    ):
        assert name in region, f"sub-check {name} missing from L3 region"

    # No config-flag gate in the region. Specifically reject:
    #   - getattr(settings, "l3_..._enabled"
    #   - settings.l3_..._enabled
    forbidden_patterns = [
        r'getattr\(\s*settings\s*,\s*["\']l3[_a-z]*enabled["\']',
        r'settings\.l3[_a-z]*enabled',
    ]
    for pat in forbidden_patterns:
        m = re.search(pat, region)
        assert m is None, (
            f"L3 region must not gate on a config flag; matched pattern "
            f"{pat!r}: {m.group(0) if m else None}"
        )


def test_health_l3_subchecks_expected_token_constant_is_10() -> None:
    """Revision 1 strict — L3_EXPECTED_TOKEN_COUNT must be exactly 10
    (D-05 N=5 markets × 2 tokens, not the looser '>=5' relaxation)."""
    text = HEALTH_MODULE_PATH.read_text()
    assert "L3_EXPECTED_TOKEN_COUNT = 10" in text


def test_health_l3_subchecks_use_chain_truth_getters() -> None:
    """The L3 region must call the three getters directly, not read the
    module attributes — getters are the chain-truth interface."""
    text = HEALTH_MODULE_PATH.read_text()
    region_start = text.find('# ── Phase 05 Plan 04 D-08')
    region_end = text.find("return checks, overall", region_start)
    region = text[region_start:region_end]

    for getter in (
        "get_l3_active_count",
        "get_last_promote_at_s",
        "get_last_book_levels_write_at_s",
    ):
        assert getter in region, f"chain-truth getter {getter} not called in L3 region"


# ─────────────────────────────────────────────────────────────────────────
# Phase 05.4 Plan 03 — persisted-success runtime truth
# ─────────────────────────────────────────────────────────────────────────


def _runtime(now: datetime):
    from polyarb.observation.l3_evidence import L3EvidenceRuntime, RuntimeIdentity

    return L3EvidenceRuntime(
        RuntimeIdentity(
            machine_id="machine",
            machine_version="version",
            image_ref="image",
            release_id="release",
            code_version="code",
            recipe_sha256="a" * 64,
            acceptance_config_hash="b" * 64,
        ),
        started_at=now - timedelta(minutes=5),
    )


def _market_records(
    runtime: Any,
    *,
    sampled_at: datetime,
    now: datetime,
    ages_s: tuple[tuple[float, float, float], ...] | None = None,
):
    from polyarb.observation.l3_evidence import HealthStatus, MarketSampleRecord

    effective_ages = ages_s or tuple((12.0, 13.0, 14.0) for _ in range(5))
    return tuple(
        MarketSampleRecord(
            boot_id=runtime.snapshot().boot_id,
            sample_seq=0,
            sampled_at=sampled_at,
            market_id=f"market-{index}",
            yes_token_id=f"yes-{index}",
            no_token_id=f"no-{index}",
            yes_desired=True,
            no_desired=True,
            yes_committed=True,
            no_committed=True,
            yes_evidenced=True,
            no_evidenced=True,
            evidence_generation=7,
            yes_book_at=now - timedelta(seconds=triple[0]),
            no_book_at=now - timedelta(seconds=triple[1]),
            yes_book_age_ms=int(triple[0] * 1000),
            no_book_age_ms=int(triple[1] * 1000),
            worst_book_age_ms=int(max(triple[:2]) * 1000),
            yes_ohlc_at=now - timedelta(seconds=triple[2]),
            yes_ohlc_age_ms=int(triple[2] * 1000),
            status=HealthStatus.PASS,
            reason_code="ok",
        )
        for index, triple in enumerate(effective_ages)
    )


def _seed_runtime(
    now: datetime,
    *,
    sample_age_s: float = 10,
    promote_age_s: float = 20,
    ages_s: tuple[tuple[float, float, float], ...] | None = None,
):
    from polyarb.observation.l3_evidence import WsMembershipSnapshot

    runtime = _runtime(now)
    tokens = frozenset(
        token for index in range(5) for token in (f"yes-{index}", f"no-{index}")
    )
    runtime.update_membership(
        WsMembershipSnapshot(
            generation=7,
            desired=tokens,
            committed=tokens,
            evidenced=tokens,
            evidenced_at={token: now - timedelta(seconds=5) for token in tokens},
        )
    )
    sampled_at = now - timedelta(seconds=sample_age_s)
    records = _market_records(
        runtime,
        sampled_at=sampled_at,
        now=now,
        ages_s=ages_s,
    )
    runtime.mark_sample_persisted(sampled_at, records)
    runtime.mark_promote_persisted(now - timedelta(seconds=promote_age_s))
    return runtime


class _CountingRuntime:
    def __init__(self, runtime: Any) -> None:
        self.runtime = runtime
        self.calls = 0

    def snapshot(self):
        self.calls += 1
        return self.runtime.snapshot()


def _strict_entries(checks: dict[str, list[dict[str, Any]]]) -> dict[str, dict[str, Any]]:
    names = (
        "l3:evidence_sample_age_seconds",
        "l3:promoter_ledger_age_seconds",
        "l3:membership_convergence",
        "l3:worst_market_freshness",
    )
    return {name: checks[name][0] for name in names}


def test_strict_l3_health_cold_start_fails_closed_when_runtime_is_required() -> None:
    now = datetime.now(UTC)
    checks, overall = _call_build_checks(
        now.timestamp(),
        evidence_runtime=_runtime(now),
        evidence_runtime_required=True,
    )

    assert overall == "fail"
    assert {entry["status"] for entry in _strict_entries(checks).values()} == {"fail"}


def test_strict_l3_health_fresh_pass_reads_one_immutable_snapshot() -> None:
    now = datetime.now(UTC)
    counting = _CountingRuntime(_seed_runtime(now))

    checks, overall = _call_build_checks(
        now.timestamp(),
        evidence_runtime=counting,
        evidence_runtime_required=True,
    )

    assert counting.calls == 1
    assert {entry["status"] for entry in _strict_entries(checks).values()} == {"pass"}
    assert overall != "fail"


@pytest.mark.parametrize(
    ("sample_age_s", "promote_age_s", "failed_key"),
    [
        (75, 20, "l3:evidence_sample_age_seconds"),
        (10, 360, "l3:promoter_ledger_age_seconds"),
    ],
)
def test_strict_l3_health_uses_locked_age_boundaries(
    sample_age_s: float,
    promote_age_s: float,
    failed_key: str,
) -> None:
    now = datetime.now(UTC)
    minimum_market_age = max(sample_age_s + 1, 12)
    ages = tuple(
        (minimum_market_age, minimum_market_age + 1, minimum_market_age + 2)
        for _ in range(5)
    )
    runtime = _seed_runtime(
        now,
        sample_age_s=sample_age_s,
        promote_age_s=promote_age_s,
        ages_s=ages,
    )

    checks, overall = _call_build_checks(
        now.timestamp(),
        evidence_runtime=runtime,
        evidence_runtime_required=True,
    )

    assert checks[failed_key][0]["status"] == "fail"
    assert overall == "fail"


def test_membership_convergence_uses_persisted_mapping_not_only_counts() -> None:
    from polyarb.observation.l3_evidence import WsMembershipSnapshot

    now = datetime.now(UTC)
    runtime = _seed_runtime(now)
    wrong_tokens = frozenset(
        token for index in range(5) for token in (f"other-yes-{index}", f"other-no-{index}")
    )
    runtime.update_membership(
        WsMembershipSnapshot(
            generation=8,
            desired=wrong_tokens,
            committed=wrong_tokens,
        )
    )

    checks, overall = _call_build_checks(
        now.timestamp(),
        evidence_runtime=runtime,
        evidence_runtime_required=True,
    )

    entry = checks["l3:membership_convergence"][0]
    assert entry["observedValue"] == "mismatch"
    assert entry["status"] == "fail"
    assert overall == "fail"


def test_worst_market_freshness_fails_when_one_hot_market_masks_four_silent() -> None:
    now = datetime.now(UTC)
    ages = ((5, 6, 7),) + tuple((121, 122, 123) for _ in range(4))
    runtime = _seed_runtime(now, ages_s=ages)

    checks, overall = _call_build_checks(
        now.timestamp(),
        evidence_runtime=runtime,
        evidence_runtime_required=True,
    )

    entry = checks["l3:worst_market_freshness"][0]
    assert entry["observedValue"] == pytest.approx(123.0)
    assert entry["status"] == "fail"
    assert overall == "fail"


def test_writer_failure_ages_anchor_then_successful_persistence_recovers() -> None:
    now = datetime.now(UTC)
    runtime = _seed_runtime(
        now,
        sample_age_s=76,
        ages_s=tuple((80, 81, 82) for _ in range(5)),
    )
    runtime.note_writer_result(False, now, "sample_append_failed")

    failed, failed_overall = _call_build_checks(
        now.timestamp(),
        evidence_runtime=runtime,
        evidence_runtime_required=True,
    )
    assert failed["l3:evidence_sample_age_seconds"][0]["status"] == "fail"
    assert failed_overall == "fail"

    recovered_at = now + timedelta(seconds=1)
    records = _market_records(runtime, sampled_at=recovered_at, now=recovered_at)
    runtime.note_writer_result(True, recovered_at, "ok")
    runtime.mark_sample_persisted(recovered_at, records)
    recovered, recovered_overall = _call_build_checks(
        recovered_at.timestamp(),
        evidence_runtime=runtime,
        evidence_runtime_required=True,
    )
    assert recovered["l3:evidence_sample_age_seconds"][0]["status"] == "pass"
    assert recovered_overall != "fail"


@pytest.mark.parametrize("fault", ["integrity", "overflow"])
def test_sticky_runtime_integrity_faults_surface_as_strict_failure(fault: str) -> None:
    from polyarb.observation.l3_evidence import RuntimeEventKind

    now = datetime.now(UTC)
    runtime = _seed_runtime(now)
    if fault == "overflow":
        for _ in range(128):
            runtime.record_event(RuntimeEventKind.WATCHDOG_STALE, occurred_at=now)
        with pytest.raises(OverflowError):
            runtime.record_event(RuntimeEventKind.WATCHDOG_STALE, occurred_at=now)
    else:
        event = runtime.record_event(RuntimeEventKind.WATCHDOG_STALE, occurred_at=now)
        runtime.quarantine_conflicting_event(
            event,
            at=now,
            reason_code="event_replay_conflict",
        )

    checks, overall = _call_build_checks(
        now.timestamp(),
        evidence_runtime=runtime,
        evidence_runtime_required=True,
    )
    assert checks["l3:evidence_sample_age_seconds"][0]["status"] == "fail"
    assert overall == "fail"


def test_missing_runtime_warns_only_for_implicit_local_boundary() -> None:
    from polyarb.http.l2_app import create_l2_app

    settings = _make_settings()
    store = MagicMock()
    local_app = create_l2_app(sqlite_store=store, settings=settings)
    configured_app = create_l2_app(
        sqlite_store=store,
        settings=settings,
        evidence_runtime=None,
    )

    from starlette.testclient import TestClient

    with TestClient(local_app) as client:
        local = client.get("/health")
    with TestClient(configured_app) as client:
        configured = client.get("/health")

    assert local.json()["checks"]["l3:evidence_sample_age_seconds"][0]["status"] == "warn"
    assert configured.status_code == 503


def test_strict_endpoint_returns_503_while_healthz_remains_200() -> None:
    from starlette.testclient import TestClient

    from polyarb.http.l2_app import create_l2_app

    now = datetime.now(UTC)
    runtime = _seed_runtime(
        now,
        sample_age_s=76,
        ages_s=tuple((80, 81, 82) for _ in range(5)),
    )
    app = create_l2_app(
        sqlite_store=MagicMock(),
        settings=_make_settings(),
        ws_consumer=SimpleNamespace(current_state="CONNECTED", last_event_at_s=now.timestamp()),
        evidence_runtime=runtime,
    )

    with TestClient(app) as client:
        strict = client.get("/health")
        fly_probe = client.get("/healthz")

    assert strict.status_code == 503
    assert fly_probe.status_code == 200
    assert strict.json()["status"] == fly_probe.json()["status"] == "fail"


def test_health_and_sampler_never_read_consumer_membership_directly() -> None:
    health_source = HEALTH_MODULE_PATH.read_text()
    sampler_source = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "polyarb"
        / "observation"
        / "l3_sampler.py"
    ).read_text()

    assert ".l3_membership_snapshot" not in health_source
    assert ".l3_membership_snapshot" not in sampler_source


@pytest.mark.parametrize(
    "corrupt",
    [
        {"status": "fail"},
        {"yes_evidenced": False},
    ],
)
def test_persisted_market_row_integrity_must_be_strictly_passing(
    corrupt: dict[str, Any],
) -> None:
    from polyarb.observation.l3_evidence import HealthStatus

    now = datetime.now(UTC)
    runtime = _seed_runtime(now)
    status = runtime.snapshot()
    rows = list(status.last_market_samples)
    changes: dict[str, Any]
    if corrupt == {"status": "fail"}:
        changes = {"status": HealthStatus.FAIL, "reason_code": "not_evidenced"}
    else:
        changes = corrupt
    rows[0] = replace(rows[0], **changes)
    runtime.mark_sample_persisted(status.last_sample_persisted_at, tuple(rows))

    checks, overall = _call_build_checks(
        now.timestamp(),
        evidence_runtime=runtime,
        evidence_runtime_required=True,
    )

    assert checks["l3:membership_convergence"][0]["status"] == "fail"
    assert overall == "fail"
