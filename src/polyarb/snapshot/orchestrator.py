"""Snapshot orchestrator — wires Gamma + CLOB + validator + storage together.

The 7-step pipeline:

    1. Gamma fetch       — /events first (for category/tags), then /markets
                           (Phase 1.1 Amendment 01: dual-source fetch)
    2. Normalize         — Gamma raw → storage row contract (drop unrecoverable);
                           markets get event_id FK from events' nested markets list
    3. Mode filter       — subset (liquidity>$1k) | full (all markets)
    4. CLOB batch fetch  — order books + buy/sell prices for the filtered tokens
    5. Stamp + attach    — fetched_at_ms (Pitfall 6) + best_bid/best_ask top-of-book
    6. Validate          — Layer 1 count, Layer 2 fields, Layer 4 cross-source
    7. Persist           — Parquet atomic write FIRST, then SQLite single-tx write
                           (snapshots → events → event_tags → markets FK order)

Every step records errors as ``Issue`` objects rather than raising. The
orchestrator NEVER calls ``sys.exit`` — the CLI is responsible for setting the
process exit code based on ``SnapshotResult.is_valid``.

Failure semantics (D-D3 / D-E2):
    - Gamma unreachable      → Layer 1 Issue(API_UNREACHABLE), proceed with []
    - CLOB unreachable       → Layer 4 Issue(API_UNREACHABLE), proceed without books
    - Validation issue found → row still persisted with is_valid flag derived
                                from is_valid_overall(issues) (Layer 1 only flips it
                                in Phase 1)

Security invariants applied:
    - F-1: every float() coercion of CLOB book prices/sizes is wrapped in
      try/except (KeyError, TypeError, ValueError, IndexError); failures are
      surfaced as Issue(layer=4, category=UNKNOWN) with raw_payload truncated.
    - F-5: exception details capped at 200 chars; book payloads at 500 bytes.

Phase 1 simplifications (documented for Phase 2 cleanup):
    - top-of-book attached only for ``yes_token_id`` (NO side is symmetric
      on Polymarket; Layer 4 validator still checks both tokens for ghost-book).
    - ``fetched_at_ms`` is stamped on ALL normalized markets including those
      filtered out of the subset (semantic gap from F-1 review — see SUMMARY).
"""

from __future__ import annotations

import json
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path

import httpx
import sentry_sdk
from loguru import logger
from tenacity import (
    AsyncRetrying,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from polyarb.clients.clob_client import ClobReaderClient
from polyarb.clients.gamma_client import GammaClient, PaginationCoverage
from polyarb.config import Settings
from polyarb.events.bus import publish_snapshot_complete
from polyarb.perception.market_truth import (
    ACTIVE_MEMBER_ABSENT_FROM_MARKET_KEYSET_REASON,
    MARKET_SIDE_ACTIVE_MEMBER_ABSENT_FROM_EVENT_STRUCTURE_REASON,
    EventMember,
    GroupTruth,
    MarketTruthSemanticValidator,
    SourceCoverage,
    membership_hash,
)
from polyarb.snapshot.cache import ChunkCache
from polyarb.snapshot.normalizer import normalize_events, normalize_market
from polyarb.storage.parquet_writer import compute_snapshot_path, write_parquet_streaming
from polyarb.storage.sqlite_store import SQLiteStore
from polyarb.validator.category import Category, Issue, SnapshotStatus
from polyarb.validator.layers import (
    determine_snapshot_status,
    is_valid_overall,
    layer1_count,
    layer2_fields,
    layer4_cross_source,
)

MAX_ORPHAN_PARENT_LOOKUPS = GammaClient.MAX_MARKET_PARENT_LOOKUPS
MAX_MARKET_STATE_LOOKUPS = GammaClient.MAX_MARKET_STATE_LOOKUPS


def _is_dns_jitter(exc: BaseException) -> bool:
    """Match the specific DNS-failure exception shapes seen in Fly machine production.

    Phase 03.1 D-01 modify A — Sentry issue 121111789 evidence (6 days, 3 occurrences):
      - "[Errno -5] No address associated with hostname"   (EAI_NODATA)
      - "[Errno -3] Temporary failure in name resolution"  (EAI_AGAIN)

    Strictly DNS-class: refuses to retry other ConnectErrors (connection
    refused, host unreachable) — those signal real upstream outages and the
    existing fail-soft Issue(API_UNREACHABLE) path must remain intact
    (chain-truth discipline; ref feedback_code-vs-chain-truth-2026-05).
    """
    if not isinstance(exc, httpx.ConnectError):
        return False
    s = str(exc)
    return (
        "[Errno -5]" in s
        or "[Errno -3]" in s
        or "EAI_AGAIN" in s
        or "EAI_NODATA" in s
        or "Name or service not known" in s
        or "Temporary failure in name resolution" in s
    )


def _format_elapsed(seconds: float) -> str:
    """Format a duration as '12.3s' / '1m 23s' / '1h 02m 03s' for log readability."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    if seconds < 3600:
        m = int(seconds // 60)
        s = int(seconds % 60)
        return f"{m}m {s:02d}s"
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    return f"{h}h {m:02d}m {s:02d}s"


_STRUCTURE_DIAGNOSTIC_STAGES = frozenset(
    {
        "gamma-events",
        "gamma-markets",
        "membership-recheck",
        "validate",
        "persist",
    }
)


@contextmanager
def _phase(label: str, *, stage: str | None):
    """Bracket a pipeline phase with start/done log lines and elapsed timing.

    The 'done' line uses a ► glyph so post-run grep can isolate phase summaries:
        grep '► Phase' /tmp/snap.log

    Structure snapshots additionally emit one fixed-vocabulary stderr marker at
    entry and completion. The scheduler retains only the final valid stage.
    """
    if stage is not None and stage not in _STRUCTURE_DIAGNOSTIC_STAGES:
        raise ValueError(f"invalid snapshot diagnostic stage: {stage!r}")
    logger.info(f"Phase {label} — start")
    t0 = time.monotonic()
    if stage is not None:
        print(
            f"snapshot-stage stage={stage} state=start elapsed_ms=0",
            file=sys.stderr,
            flush=True,
        )
    try:
        yield
    finally:
        elapsed_seconds = time.monotonic() - t0
        if stage is not None:
            print(
                "snapshot-stage "
                f"stage={stage} state=complete "
                f"elapsed_ms={max(0, int(elapsed_seconds * 1000))}",
                file=sys.stderr,
                flush=True,
            )
        logger.info(f"► Phase {label} — done in {_format_elapsed(elapsed_seconds)}")


def _derive_notes_from_issues(issues: list[Issue]) -> str | None:
    """Phase 03.1 Plan 02 GAP-103 — pull a one-line fail-reason for snapshots.notes.

    Operators need to know WHY a snapshot failed without joining the
    validation_issues table. Strategy: collect API_UNREACHABLE details (truncated
    to 80 chars each), semicolon-join, cap total at 200 chars.

    Returns None when no API_UNREACHABLE issues are present — clean / validator-
    only-noise snapshots keep notes=NULL (operators want fail reasons, not L2
    validation findings like zombie_market or ghost_book).

    Format chosen so dashboards can `SELECT substr(notes, 1, 40), count(*) FROM
    snapshots GROUP BY 1` to tally failure modes.
    """
    reasons: list[str] = []
    for issue in issues:
        if issue.category != Category.API_UNREACHABLE:
            continue
        detail = issue.detail or f"L{issue.layer}: unknown"
        reasons.append(detail[:80])
    if not reasons:
        return None
    joined = "; ".join(reasons)
    return joined[:200]


@dataclass
class SnapshotResult:
    """Return value of ``run_snapshot`` — what the CLI prints + what tests assert on."""

    snapshot_id: int
    market_count: int
    is_valid: bool
    status: str  # "ok" / "degraded" / "failed" (SnapshotStatus enum value)
    mode: str
    issue_count: int
    issue_categories: dict[str, int]  # {category_value: count}
    parquet_path: Path
    taken_at_ms: int
    finished_at_ms: int


def _index_books_by_token(books: list) -> dict[str, dict]:
    """Map CLOB ``OrderBookSummary`` objects to ``{token_id: book_dict}``.

    The token-id field is ``asset_id`` per 01-2-SUMMARY (resolved empirically).
    Falls back to ``market`` / ``token_id`` so a future SDK rename doesn't
    silently drop books. Each book is normalized into a plain dict (the SDK
    object is dataclass-like with ``__dict__``).
    """
    out: dict[str, dict] = {}
    for b in books:
        if b is None:
            continue
        # OrderBookSummary is dataclass-like — pull __dict__ if available, else
        # treat as already-a-dict (test mocks may pass plain dicts directly).
        bd: dict = b.__dict__ if hasattr(b, "__dict__") and not isinstance(b, dict) else b
        tid = bd.get("asset_id") or bd.get("market") or bd.get("token_id")
        if tid:
            out[str(tid)] = bd
    return out


def _include_in_snapshot(mode: str, market: dict, threshold: float) -> bool:
    """Keep liquid markets plus every neg-risk sibling needed by M2."""
    return (
        mode == "full"
        or bool(market.get("neg_risk_market_id"))
        or (market.get("liquidity_usd") or 0) > threshold
    )


def _apply_point_member_states(
    event_members: list[EventMember],
    group_truths: list[GroupTruth],
    point_states: dict[str, dict[str, bool]],
) -> tuple[list[EventMember], list[GroupTruth]]:
    """Overlay point-market status while preserving event structural membership."""
    reconciled_members: list[EventMember] = []
    for member in event_members:
        state = point_states.get(member.market_id)
        if state is None:
            reconciled_members.append(member)
            continue
        active = state["active"]
        closed = state["closed"]
        member_kind = member.member_kind
        if member_kind != "other":
            member_kind = "named" if active else "inactive-reserved"
        reconciled_members.append(
            replace(
                member,
                member_kind=member_kind,
                active=active,
                closed=closed,
            )
        )

    members_by_key: dict[tuple[str, str], list[EventMember]] = {}
    for member in reconciled_members:
        members_by_key.setdefault((member.event_id, member.group_id), []).append(member)

    reconciled_truths: list[GroupTruth] = []
    for truth in group_truths:
        members = members_by_key.get((truth.event_id, truth.group_id), [])
        quality = truth.quality
        reason = truth.reason
        if quality != "incomplete-source":
            if truth.neg_risk_type == "augmented":
                quality = "complete-unsupported"
                reason = "augmented-neg-risk-not-supported"
            elif all(
                member.member_kind == "named" and member.active and not member.closed
                for member in members
            ):
                quality = "complete-supported"
                reason = None
            else:
                quality = "complete-unsupported"
                reason = "standard-neg-risk-has-non-tradable-members"
        reconciled_truths.append(
            replace(
                truth,
                active_named_count=sum(
                    member.member_kind == "named" and member.active for member in members
                ),
                membership_hash=membership_hash(
                    truth.event_id,
                    truth.group_id,
                    members,
                ),
                quality=quality,
                reason=reason,
            )
        )
    return reconciled_members, reconciled_truths


def _quarantine_open_keyset_absent_groups(
    event_members: list[EventMember],
    group_truths: list[GroupTruth],
    trigger_market_ids: set[str],
) -> tuple[list[GroupTruth], set[str]]:
    """Reject complete groups whose point-open member is absent from the keyset.

    Structural membership and its hash remain immutable evidence. The whole
    affected group is removed from the market publication target and marked
    complete-unsupported so M2 cannot construct a partial opportunity.
    Ordinary event mappings have no GroupTruth and therefore return no group
    market IDs; their missing row is already absent from the target.
    """
    rejected_keys = {
        (member.event_id, member.group_id)
        for member in event_members
        if member.market_id in trigger_market_ids
    }
    if not rejected_keys:
        return group_truths, set()

    rejected_market_ids = {
        member.market_id
        for member in event_members
        if (member.event_id, member.group_id) in rejected_keys
    }
    reconciled_truths = [
        (
            replace(
                truth,
                quality="complete-unsupported",
                reason=ACTIVE_MEMBER_ABSENT_FROM_MARKET_KEYSET_REASON,
            )
            if (truth.event_id, truth.group_id) in rejected_keys
            else truth
        )
        for truth in group_truths
    ]
    return reconciled_truths, rejected_market_ids


def _quarantine_market_side_structure_absent_groups(
    event_members: list[EventMember],
    group_truths: list[GroupTruth],
    trigger_market_groups: dict[str, str],
) -> tuple[list[GroupTruth], set[str]]:
    """Reject groups with a point-open market-side member absent from events.

    Preserve event-side members and their immutable membership hash as source
    evidence. Publication removes both those members and the extra market rows
    because M2 cannot safely infer whether the extra row belongs in the group.
    """
    rejected_group_ids = set(trigger_market_groups.values())
    rejected_market_ids = set(trigger_market_groups)
    rejected_market_ids.update(
        member.market_id for member in event_members if member.group_id in rejected_group_ids
    )
    reconciled_truths = [
        (
            replace(
                truth,
                quality="complete-unsupported",
                reason=MARKET_SIDE_ACTIVE_MEMBER_ABSENT_FROM_EVENT_STRUCTURE_REASON,
            )
            if truth.group_id in rejected_group_ids
            else truth
        )
        for truth in group_truths
    ]
    return reconciled_truths, rejected_market_ids


def _reconcile_market_truth(
    *,
    observed_market_ids: set[str],
    semantic_reason: str | None,
    market_to_event_map: dict[str, str],
    event_members: list[EventMember],
    group_truths: list[GroupTruth],
    event_optional_market_ids: set[str],
    verified_stale_orphan_ids: set[str],
    verified_non_open_member_ids: set[str] | None = None,
) -> str | None:
    """Reconcile full Gamma identities before any subset publication claim."""
    non_open_member_ids = verified_non_open_member_ids or set()
    incomplete_truth = next(
        (truth for truth in group_truths if truth.quality == "incomplete-source"),
        None,
    )
    if incomplete_truth is not None:
        reason = incomplete_truth.reason or "unspecified"
        return f"group-incomplete-source:{incomplete_truth.group_id}:{reason}"[:160]

    truth_keys: set[tuple[str, str]] = set()
    group_ids: set[str] = set()
    for truth in group_truths:
        key = (truth.event_id, truth.group_id)
        if key in truth_keys or truth.group_id in group_ids:
            return f"duplicate-group-identity:{truth.event_id}/{truth.group_id}"[:160]
        truth_keys.add(key)
        group_ids.add(truth.group_id)

    seen_member_ids: set[str] = set()
    required_member_ids: set[str] = set()
    for member in event_members:
        if member.market_id in seen_member_ids:
            return f"duplicate-member-identity:{member.market_id}"[:160]
        seen_member_ids.add(member.market_id)
        if (member.event_id, member.group_id) not in truth_keys:
            return f"member-without-group-truth:{member.market_id}"[:160]
        if member.active and not member.closed and member.market_id not in non_open_member_ids:
            required_member_ids.add(member.market_id)
            mapped_event = market_to_event_map.get(member.market_id)
            if mapped_event != member.event_id:
                return (
                    f"event-member-identity-conflict:{member.market_id}:"
                    f"{member.event_id}!={mapped_event}"
                )[:160]

    missing_members = sorted(required_member_ids - observed_market_ids)
    if missing_members:
        return f"event-member-missing-market:{','.join(missing_members[:5])}"[:160]

    # Gamma's active market catalogue can legitimately contain ordinary
    # (non-neg-risk) markets whose parent event is inactive and therefore
    # absent from the active /events catalogue.  event_id=NULL is part of the
    # storage contract for those rows.  Never extend this exemption to a row
    # that claims neg-risk membership: M2 requires complete event/group truth.
    orphan_markets = sorted(
        observed_market_ids
        - set(market_to_event_map)
        - event_optional_market_ids
        - verified_stale_orphan_ids
    )
    if orphan_markets:
        return f"orphan-market-without-event:{','.join(orphan_markets[:5])}"[:160]

    missing_mapped_markets = sorted(
        set(market_to_event_map) - observed_market_ids - non_open_member_ids
    )
    if missing_mapped_markets:
        return f"event-map-missing-market:{','.join(missing_mapped_markets[:5])}"[:160]
    if semantic_reason is not None:
        return semantic_reason[:160]
    return None


async def run_snapshot(
    settings: Settings,
    *,
    mode: str = "subset",
    product: str = "legacy_combined",
    now_ms: int | None = None,
    use_cache: bool = True,
    gamma_client: object | None = None,
    schema_ready: bool = False,
) -> SnapshotResult:
    """Run one Polymarket snapshot end-to-end.

    Args:
        settings: Plan-1 ``Settings`` (URLs, rate caps, retry knobs, paths).
        mode: ``"subset"`` (default; only liquidity_usd > threshold)
                  or ``"full"`` (all markets).
        product: ``"structure"`` publishes Gamma-only online market truth;
                 ``"archive"`` writes research evidence only and never
                 replaces the published ``markets`` view; ``"legacy_combined"``
                 preserves the pre-separation behavior for historical tooling.
        now_ms: Override for the snapshot's ``taken_at_ms`` timestamp (test hook).
                Defaults to ``int(time.time() * 1000)`` at function entry.
        schema_ready: Skip the full migration pass when the daemon already
                initialized the database before launching this child.

    Returns:
        SnapshotResult — never raises for transport failures (those become
        Issues). Re-raises only for unexpected internal errors (e.g. SQLite
        rollback, Parquet schema mismatch — these should never happen with
        the normalizer's contract).

    use_cache:
        When True (default) the CLOB chunk cache (``settings.cache_root``)
        is consulted at startup; an in-progress run that died mid-CLOB can
        resume from the last completed chunk. The cache is cleaned up on
        successful persistence (step 7). When False, all existing caches
        under ``cache_root`` are purged at start and chunks are not saved.
    """
    if mode not in ("subset", "full"):
        raise ValueError(f"invalid mode: {mode!r} (must be 'subset' or 'full')")
    if product not in ("legacy_combined", "structure", "archive"):
        raise ValueError(f"invalid product: {product!r}")

    taken_at_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    issues: list[Issue] = []
    target_markets: list[dict] = []
    seen_ids: set[str] = set()
    event_optional_market_ids: set[str] = set()
    orphan_neg_risk_market_groups: dict[str, str] = {}
    market_side_structure_absent_groups: dict[str, str] = {}
    orphan_neg_risk_market_count = 0
    missing_group_neg_risk_count = 0
    missing_group_neg_risk_samples: list[str] = []
    market_semantic_fingerprints: dict[str, tuple[object, ...]] = {}
    raw_market_count = 0
    normalized_count = 0
    dedup_count = 0
    event_rows: list[dict] = []
    event_tag_rows: list[dict] = []
    event_members: list[EventMember] = []
    group_truths: list[GroupTruth] = []
    market_to_event_map: dict[str, str] = {}
    events_coverage = PaginationCoverage(source="events")
    markets_coverage = PaginationCoverage(source="markets")
    event_failure_reason: str | None = None
    market_failure_reason: str | None = None
    market_semantic_reason: str | None = None
    member_state_lookup_failure_reason: str | None = None
    orphan_parent_lookup_failure_reason: str | None = None
    verified_non_open_member_ids: set[str] = set()
    pending_open_missing_member_ids: set[str] = set()
    verified_stale_orphan_ids: set[str] = set()
    source_group_less_neg_risk_ids: set[str] = set()
    normalization_rejected_market_ids: set[str] = set()
    threshold = settings.liquidity_threshold_usd
    PROGRESS_EVERY = 5000  # log every N streamed markets so long fetches stay observable

    logger.info(
        f"snapshot starting — product={product}, mode={mode}, "
        f"cache={'on' if use_cache else 'off'}, "
        f"taken_at_ms={taken_at_ms}"
    )

    overall_t0 = time.monotonic()

    # ── Phases 1+2 combined: one GammaClient session, events materialized,
    # then markets STREAMED (D-23). Single `async with` so HTTP/2 keepalive
    # is shared across /events + /markets and shutdown is clean.
    gamma_source = GammaClient(settings) if gamma_client is None else gamma_client
    async with gamma_source as gamma:
        # ── Phase 1: events (fully materialized — Decision A) ─────────────
        with _phase(
            "1/7: Gamma /events fetch + normalize",
            stage="gamma-events" if product == "structure" else None,
        ):
            try:
                raw_events = [event async for event in gamma.iter_active_events(events_coverage)]
                logger.info(f"Gamma: fetched {len(raw_events)} active events")
                # Preserve the one source-side fact that normalize_events
                # intentionally cannot express as GroupTruth: a parent event
                # explicitly enables standard neg-risk but omits the group
                # identity altogether. Its nested market IDs are eligible for
                # quarantine (never for inferred/synthetic grouping).
                for raw_event in raw_events:
                    if not (
                        raw_event.get("negRisk") is True
                        and raw_event.get("enableNegRisk") is True
                        and raw_event.get("negRiskAugmented") is False
                        and raw_event.get("negRiskMarketID") is None
                        and isinstance(raw_event.get("markets"), list)
                    ):
                        continue
                    for raw_member in raw_event["markets"]:
                        if not isinstance(raw_member, dict):
                            continue
                        raw_member_id = raw_member.get("id")
                        if (
                            type(raw_member_id) is str
                            and raw_member_id
                            and raw_member_id.strip() == raw_member_id
                        ):
                            source_group_less_neg_risk_ids.add(raw_member_id)
                (
                    event_rows,
                    event_tag_rows,
                    market_to_event_map,
                    event_members,
                    group_truths,
                ) = normalize_events(raw_events)
                del raw_events  # free 10k+ raw Gamma event dicts immediately
                logger.info(
                    f"Events normalized: {len(event_rows)} events, "
                    f"{len(event_tag_rows)} event_tags, "
                    f"{len(market_to_event_map)} market→event mappings"
                )
            except Exception as e:  # noqa: BLE001 — categorize, do NOT propagate
                event_failure_reason = str(e)[:200]
                logger.error(f"Gamma /events fetch failed: {e!r}")
                issues.append(
                    Issue(
                        layer=1,
                        category=Category.API_UNREACHABLE,
                        market_id=None,
                        detail=f"Gamma /events unreachable: {str(e)[:200]}",
                    )
                )

        # ── Phase 2: stream /markets — normalize + dedupe + mode-filter ──
        # The 20k raw Gamma /markets list NEVER materializes — each `raw`
        # dict is normalized, dedup-checked, mode-filtered, and either
        # appended to `target_markets` or dropped. Non-target markets go
        # out of scope at the next iteration → GC eligible.
        #
        # Phase 03.1-04 D-01 modify A: wrap the stream-START in tenacity
        # AsyncRetrying that fires ONLY for DNS-class ConnectError (EAI_NODATA
        # / EAI_AGAIN). first_frame_seen sentinel ensures middle-of-stream
        # exceptions are NOT retried (a partial stream consumed N markets ≠
        # idempotent retry boundary). Stops at 3 attempts, exponential wait
        # 1..5s. Re-raises last exception → existing fail-soft except clause
        # below appends API_UNREACHABLE (chain-truth preserved).
        with _phase(
            "2/7: Stream /markets — normalize + dedupe + filter",
            stage="gamma-markets" if product == "structure" else None,
        ):
            first_frame_seen = False
            authoritative_member_ids = {
                member.market_id for member in event_members if member.active and not member.closed
            }
            structural_member_ids = {member.market_id for member in event_members}
            known_group_ids = {truth.group_id for truth in group_truths}
            semantic_validator = MarketTruthSemanticValidator(
                event_members,
                group_truths,
            )
            try:
                async for retry_state in AsyncRetrying(
                    retry=retry_if_exception(lambda e: _is_dns_jitter(e) and not first_frame_seen),
                    stop=stop_after_attempt(3),
                    wait=wait_exponential(multiplier=1, min=1, max=5),
                    reraise=True,
                ):
                    with retry_state:
                        async for raw in gamma.iter_active_markets(markets_coverage):
                            if not first_frame_seen:
                                first_frame_seen = True
                            raw_market_count += 1
                            raw_market_id = raw.get("id")
                            normalized = normalize_market(raw, market_to_event_map)
                            if normalized is None:
                                if type(raw_market_id) is str and raw_market_id.strip():
                                    normalization_rejected_market_ids.add(raw_market_id.strip())
                                continue
                            mid = normalized.get("market_id")
                            if mid is None:
                                continue

                            group_id = normalized.get("neg_risk_market_id")
                            is_missing_group_neg_risk = (
                                normalized.get("neg_risk") is True and group_id is None
                            )
                            # Gamma occasionally exposes an active market as
                            # negRisk=true while both the market and its parent
                            # event omit negRiskMarketID.  There is no source
                            # identity from which M2 can safely construct a
                            # group. Quarantine only when event-side structural
                            # truth also has no member: a known member missing
                            # its group on /markets remains a fatal semantic
                            # disagreement below.
                            is_group_less_neg_risk_quarantine = (
                                is_missing_group_neg_risk
                                and mid in source_group_less_neg_risk_ids
                                and mid not in structural_member_ids
                            )
                            is_unattached_neg_risk = (
                                normalized.get("event_id") is None
                                and normalized.get("neg_risk") is True
                                and isinstance(group_id, str)
                                and group_id not in known_group_ids
                            )
                            is_market_side_structure_absent = (
                                normalized.get("event_id") is None
                                and normalized.get("neg_risk") is True
                                and isinstance(group_id, str)
                                and group_id in known_group_ids
                                and mid not in structural_member_ids
                                and mid not in market_to_event_map
                            )
                            # Inspect before duplicate suppression: pagination
                            # overlap may repeat a market ID, but a later row
                            # must not contradict its authoritative event truth.
                            # Keep only the first mismatch to preserve the
                            # streaming memory bound.
                            if (
                                market_semantic_reason is None
                                and not is_unattached_neg_risk
                                and not is_group_less_neg_risk_quarantine
                                and not is_market_side_structure_absent
                            ):
                                market_semantic_reason = semantic_validator.row_mismatch_reason(
                                    normalized
                                )

                            semantic_fingerprint = (
                                normalized.get("event_id"),
                                normalized.get("neg_risk_market_id"),
                                normalized.get("neg_risk"),
                                normalized.get("active"),
                                normalized.get("closed"),
                            )
                            if mid in seen_ids:
                                if (
                                    market_semantic_reason is None
                                    and market_semantic_fingerprints[mid] != semantic_fingerprint
                                ):
                                    market_semantic_reason = (
                                        f"duplicate-market-truth-conflict:{mid}"
                                    )[:160]
                                dedup_count += 1
                                continue
                            seen_ids.add(mid)
                            market_semantic_fingerprints[mid] = semantic_fingerprint
                            if (
                                normalized.get("neg_risk") is False
                                and normalized.get("neg_risk_market_id") is None
                            ):
                                event_optional_market_ids.add(mid)
                            if is_group_less_neg_risk_quarantine:
                                missing_group_neg_risk_count += 1
                                if len(missing_group_neg_risk_samples) < 10:
                                    missing_group_neg_risk_samples.append(mid)
                                # It has no provable group or parent identity,
                                # so it is intentionally outside both event-
                                # completeness and publication obligations.
                                event_optional_market_ids.add(mid)
                            if is_unattached_neg_risk:
                                orphan_neg_risk_market_count += 1
                                if len(orphan_neg_risk_market_groups) < MAX_ORPHAN_PARENT_LOOKUPS:
                                    orphan_neg_risk_market_groups[mid] = group_id
                            if is_market_side_structure_absent:
                                market_side_structure_absent_groups[mid] = group_id
                            normalized_count += 1

                            # Mode filter (replaces the old phase-3 block).
                            if (
                                not is_unattached_neg_risk
                                and not is_missing_group_neg_risk
                                and (
                                    mid in authoritative_member_ids
                                    or _include_in_snapshot(mode, normalized, threshold)
                                )
                            ):
                                target_markets.append(normalized)
                            # Non-target markets: dropped — no buffer, no reference held.

                            if normalized_count % PROGRESS_EVERY == 0:
                                logger.info(
                                    f"streaming {normalized_count} markets normalized, "
                                    f"{len(target_markets)} target so far"
                                )
            except Exception as e:  # noqa: BLE001 — categorize, do NOT propagate
                market_failure_reason = str(e)[:200]
                logger.error(f"Gamma /markets stream failed: {e!r}")
                issues.append(
                    Issue(
                        layer=1,
                        category=Category.API_UNREACHABLE,
                        market_id=None,
                        detail=f"Gamma unreachable: {str(e)[:200]}",
                    )
                )

            if missing_group_neg_risk_count:
                bounded_ids = ",".join(sorted(missing_group_neg_risk_samples))
                remainder = missing_group_neg_risk_count - len(missing_group_neg_risk_samples)
                suffix = f" (+{remainder} more)" if remainder else ""
                issues.append(
                    Issue(
                        layer=1,
                        category=Category.API_JITTER,
                        market_id=None,
                        detail=(
                            "Gamma neg-risk market missing group identity quarantined: "
                            f"{bounded_ids}{suffix}"
                        )[:200],
                    )
                )

            if (
                events_coverage.result.completed
                and markets_coverage.result.completed
                and event_failure_reason is None
                and market_failure_reason is None
            ):
                # Reconcile every event→market identity, not only neg-risk
                # group members. Ordinary event markets can also disappear
                # from the active market keyset while their event payload still
                # carries the mapping (typically as they transition closed).
                missing_event_members = sorted(
                    (authoritative_member_ids | set(market_to_event_map)) - seen_ids
                )
                if missing_event_members:
                    try:
                        point_states = await gamma.fetch_market_states(missing_event_members)
                    except Exception as e:  # noqa: BLE001 — fail closed at source boundary
                        member_state_lookup_failure_reason = (
                            f"event-member-state-lookup-failed:{type(e).__name__}"
                        )[:160]
                        logger.error(f"Gamma missing event-member point lookup failed: {e!r}")
                    else:
                        source_absent_member_ids = {
                            market_id
                            for market_id, state in point_states.items()
                            if state.get("source_absent") is True
                        }
                        verified_non_open_member_ids = {
                            market_id
                            for market_id, state in point_states.items()
                            if not state["active"] or state["closed"]
                        }
                        pending_open_missing_member_ids = (
                            set(missing_event_members) - verified_non_open_member_ids
                        )
                        if verified_non_open_member_ids:
                            event_members, group_truths = _apply_point_member_states(
                                event_members,
                                group_truths,
                                point_states,
                            )
                            bounded_ids = ",".join(sorted(verified_non_open_member_ids)[:10])
                            issues.append(
                                Issue(
                                    layer=1,
                                    category=Category.API_JITTER,
                                    market_id=None,
                                    detail=(
                                        "Gamma event/member status disagreement: "
                                        f"point truth non-open for {bounded_ids}"
                                    )[:200],
                                )
                            )
                        if source_absent_member_ids:
                            bounded_ids = ",".join(
                                sorted(source_absent_member_ids)[:10]
                            )
                            remainder = len(source_absent_member_ids) - min(
                                len(source_absent_member_ids), 10
                            )
                            suffix = f" (+{remainder} more)" if remainder else ""
                            issues.append(
                                Issue(
                                    layer=1,
                                    category=Category.API_JITTER,
                                    market_id=None,
                                    detail=(
                                        "Gamma event member absent from both active "
                                        f"and exact market catalogues: {bounded_ids}{suffix}"
                                    )[:200],
                                )
                            )

                if orphan_neg_risk_market_count > MAX_ORPHAN_PARENT_LOOKUPS:
                    orphan_parent_lookup_failure_reason = (
                        f"orphan-parent-state-lookup-limit-exceeded:{orphan_neg_risk_market_count}"
                    )[:160]
                elif orphan_neg_risk_market_groups:
                    try:
                        parent_states = await gamma.fetch_market_parent_states(
                            dict(orphan_neg_risk_market_groups)
                        )
                    except Exception as e:  # noqa: BLE001 — fail closed at source boundary
                        orphan_parent_lookup_failure_reason = (
                            "orphan-parent-state-lookup-failed:"
                            f"{type(e).__name__}:{str(e)}"
                        )[:160]
                        logger.error(f"Gamma orphan neg-risk parent lookup failed: {e!r}")
                    else:
                        active_parent_ids = {
                            market_id
                            for market_id, state in parent_states.items()
                            if state["active"] is True
                            and state["closed"] is False
                            and state["archived"] is False
                        }
                        # Every row was strictly identity/group/parent checked
                        # by GammaClient. The market rows were already excluded
                        # from target_markets above, so neither an active parent
                        # that appeared after the event catalogue sample nor a
                        # stale parent can leak a partial group into M2.
                        verified_stale_orphan_ids = set(parent_states)
                        if active_parent_ids:
                            bounded_ids = ",".join(sorted(active_parent_ids)[:5])
                            remainder = len(active_parent_ids) - min(
                                len(active_parent_ids), 5
                            )
                            suffix = f" (+{remainder} more)" if remainder else ""
                            issues.append(
                                Issue(
                                    layer=1,
                                    category=Category.API_JITTER,
                                    market_id=None,
                                    detail=(
                                        "Gamma active neg-risk parent absent from event "
                                        f"catalogue quarantined: {bounded_ids}{suffix}"
                                    )[:200],
                                )
                            )
                        stale_parent_ids = set(parent_states) - active_parent_ids
                        if stale_parent_ids:
                            bounded_ids = ",".join(sorted(stale_parent_ids)[:5])
                            remainder = len(stale_parent_ids) - min(
                                len(stale_parent_ids), 5
                            )
                            suffix = f" (+{remainder} more)" if remainder else ""
                            issues.append(
                                Issue(
                                    layer=1,
                                    category=Category.API_JITTER,
                                    market_id=None,
                                    detail=(
                                        "Gamma stale neg-risk market quarantined: "
                                        f"{bounded_ids}{suffix}"
                                    )[:200],
                                )
                            )

    # GammaClient closed (exited async-with). httpx AsyncClient fully cleaned
    # up before the CLOB phase starts.

    unique_market_count = raw_market_count - dedup_count
    gamma_count_reported = unique_market_count if raw_market_count > 0 else None
    if dedup_count > 0:
        logger.info(f"Deduped {dedup_count} markets by market_id (Gamma pagination overlap)")
    logger.info(
        f"Streamed {normalized_count}/{raw_market_count} normalized; "
        f"{len(target_markets)} target after mode-filter (mode={mode})"
    )
    reconciliation_reason: str | None = None
    if member_state_lookup_failure_reason is not None:
        reconciliation_reason = member_state_lookup_failure_reason
        issues.append(
            Issue(
                layer=1,
                category=Category.API_UNREACHABLE,
                market_id=None,
                detail=(f"Gamma event/market reconciliation incomplete: {reconciliation_reason}")[
                    :200
                ],
            )
        )
    elif orphan_parent_lookup_failure_reason is not None:
        reconciliation_reason = orphan_parent_lookup_failure_reason
        issues.append(
            Issue(
                layer=1,
                category=Category.API_UNREACHABLE,
                market_id=None,
                detail=(f"Gamma event/market reconciliation incomplete: {reconciliation_reason}")[
                    :200
                ],
            )
        )
    elif (
        events_coverage.result.completed
        and markets_coverage.result.completed
        and event_failure_reason is None
    ):
        reconciliation_reason = _reconcile_market_truth(
            observed_market_ids=seen_ids,
            semantic_reason=market_semantic_reason,
            market_to_event_map=market_to_event_map,
            event_members=event_members,
            group_truths=group_truths,
            event_optional_market_ids=event_optional_market_ids,
            # Known-group market-side extras are deferred until the final
            # point lookup after CLOB. Exempt them from this preliminary
            # reconciliation only; the final lookup remains fail-closed.
            verified_stale_orphan_ids=(
                verified_stale_orphan_ids | set(market_side_structure_absent_groups)
            ),
            # A missing member that still looks open at the first point lookup
            # is deferred, not trusted. Long CLOB runs can span the exact
            # transition to closed, so Phase 5.5 rechecks it before publication.
            verified_non_open_member_ids=(
                verified_non_open_member_ids | pending_open_missing_member_ids
            ),
        )
        if reconciliation_reason is not None:
            issues.append(
                Issue(
                    layer=1,
                    category=Category.API_UNREACHABLE,
                    market_id=None,
                    detail=(
                        f"Gamma event/market reconciliation incomplete: {reconciliation_reason}"
                    )[:200],
                )
            )
    # These maps prove source identity only during streaming/reconciliation.
    # Releasing them before CLOB prevents the complete-universe maps from
    # overlapping with tens of thousands of order-book projections.
    seen_ids.clear()
    market_semantic_fingerprints.clear()
    event_optional_market_ids.clear()
    orphan_neg_risk_market_groups.clear()
    verified_stale_orphan_ids.clear()
    source_group_less_neg_risk_ids.clear()
    market_to_event_map.clear()
    authoritative_member_ids.clear()
    structural_member_ids.clear()

    # ── Phase 3: token list extraction (was inlined into old phase 3) ────
    with _phase("3/7: Build token list", stage=None):
        token_ids: list[str] = []
        if product != "structure":
            for m in target_markets:
                for k in ("yes_token_id", "no_token_id"):
                    tid = m.get(k)
                    if tid:
                        token_ids.append(tid)
        logger.info(
            f"Mode={mode}: {len(target_markets)} target markets, "
            f"{len(token_ids)} tokens to fetch from CLOB"
        )

    # ── 4. CLOB batch fetch (best-effort: failure → Issue, not raise) ─────────
    # Cache wires in here: try_resume() either rebinds to a reusable cache from
    # a prior interrupted run (matching settings + token list + age <30min) or
    # initializes a fresh dir. ChunkCache is per-(taken_at_ms, token_set), so an
    # entirely fresh run with new tokens never accidentally reuses stale data.
    books_by_token: dict[str, dict] = {}
    prices_buy: dict = {}
    prices_sell: dict = {}
    cache: ChunkCache | None = None
    clob_fetch_failed = False
    with _phase("4/7: CLOB fetch (books + buy/sell prices)", stage=None):
        if use_cache and product != "structure":
            cache = ChunkCache(
                cache_root=settings.cache_root,
                taken_at_ms=taken_at_ms,
                settings=settings,
                token_ids=token_ids,
                mode=mode,
            )
            cache.try_resume()
            # If we resumed an older cache, its taken_at_ms differs from ours.
            # We DON'T adopt the cached taken_at_ms — the run's taken_at_ms is
            # the moment THIS run started, used for parquet path + DB row.
            # Cache is just intermediate IO; final timestamps stay fresh.
        elif product != "structure":
            purged = ChunkCache.purge_all(settings.cache_root)
            if purged > 0:
                logger.info(f"--no-cache: purged {purged} cache directories")

        if product == "structure":
            logger.info("Structure product: CLOB fetch intentionally skipped")
        else:
            clob = ClobReaderClient(settings)
            try:
                books = await clob.get_books(
                    token_ids,
                    cache=cache,
                    projection="top",
                )
                prices = await clob.get_prices_buy_sell(token_ids, cache=cache)
                prices_buy = prices.get("buy", {})
                prices_sell = prices.get("sell", {})
                books_by_token = _index_books_by_token(books)
                del books
                logger.info(
                    f"CLOB: {len(books_by_token)} books indexed, "
                    f"{len(prices_buy)}/{len(prices_sell)} buy/sell prices"
                )
            except Exception as e:  # noqa: BLE001 — categorize, do NOT propagate
                clob_fetch_failed = True
                logger.error(f"CLOB fetch failed: {e!r}")
                # F-5: cap exception detail to 200 chars.
                issues.append(
                    Issue(
                        layer=4,
                        category=Category.API_UNREACHABLE,
                        market_id=None,
                        detail=f"CLOB unreachable: {str(e)[:200]}",
                    )
                )

    # ── 5. Stamp fetched_at_ms + attach top-of-book (yes side; F-1 wrapped) ──
    # Only target_markets are persisted, so only stamp/attach those. Filtered-out
    # markets stay in `markets` for layer-1 count comparison only — we never
    # write them anywhere. (Closes the "fetched_at_ms semantically wrong on
    # filter-excluded rows" gap from 01-4-SUMMARY.)
    clob_done_ms = int(time.time() * 1000)
    with _phase("5/7: Stamp + attach top-of-book", stage=None):
        for m in target_markets:
            m["fetched_at_ms"] = clob_done_ms

            # Attach top-of-book using yes_token_id only (single-side row).
            tid = m.get("yes_token_id")
            if not tid or tid not in books_by_token:
                continue
            book = books_by_token[tid]
            # F-1 SECURITY: book fields may be attacker-controlled non-list types.
            # Normalize asks/bids to list before any indexing — guards against
            # dict/str/None values that would raise TypeError/KeyError on [0].
            _raw_asks = book.get("asks")
            _raw_bids = book.get("bids")
            asks = _raw_asks if isinstance(_raw_asks, (list, tuple)) else []
            bids = _raw_bids if isinstance(_raw_bids, (list, tuple)) else []

            # F-1 SECURITY: CLOB book is attacker-controlled external input.
            # Malformed price/size strings (NaN, missing key, null) must NOT crash
            # the snapshot — log as Issue(layer=4, category=UNKNOWN) and continue.
            # Honors D-D3 (校验失败仍落库). raw_payload truncated to 500 bytes (F-5).
            if asks:
                try:
                    m["best_ask_price"] = float(asks[0]["price"])
                    m["best_ask_size"] = float(asks[0]["size"])
                except (KeyError, TypeError, ValueError, IndexError) as e:
                    issues.append(
                        Issue(
                            layer=4,
                            category=Category.UNKNOWN,
                            market_id=m.get("market_id"),
                            detail=f"unparseable ask for {tid}: {str(e)[:200]}",
                            raw_payload=json.dumps(book, default=str)[:500],
                        )
                    )
            if bids:
                try:
                    m["best_bid_price"] = float(bids[0]["price"])
                    m["best_bid_size"] = float(bids[0]["size"])
                except (KeyError, TypeError, ValueError, IndexError) as e:
                    issues.append(
                        Issue(
                            layer=4,
                            category=Category.UNKNOWN,
                            market_id=m.get("market_id"),
                            detail=f"unparseable bid for {tid}: {str(e)[:200]}",
                            raw_payload=json.dumps(book, default=str)[:500],
                        )
                    )

    # Gamma event and market catalogues are sampled before a multi-minute CLOB
    # phase. A member can be open at the initial point lookup, close while CLOB
    # is running, and remain absent from the active market keyset. Recheck only
    # that bounded unresolved set with a fresh, short-lived client. A strictly
    # verified point-open/keyset-absent row is quarantined as source-status
    # inconsistency; malformed responses and transport failures remain fatal.
    if pending_open_missing_member_ids and reconciliation_reason is None:
        with _phase(
            "5.5/7: Recheck pending event members",
            stage="membership-recheck" if product == "structure" else None,
        ):
            final_lookup_reason: str | None = None
            try:
                async with GammaClient(settings) as final_gamma:
                    final_states = await final_gamma.fetch_market_states(
                        sorted(pending_open_missing_member_ids)
                    )
            except Exception as e:  # noqa: BLE001 — fail closed at source boundary
                final_lookup_reason = (
                    f"event-member-final-state-lookup-failed:{type(e).__name__}"
                )[:160]
                logger.error(f"Gamma final event-member point lookup failed: {e!r}")
            else:
                final_non_open_ids = {
                    market_id
                    for market_id, state in final_states.items()
                    if not state["active"] or state["closed"]
                }
                if final_non_open_ids:
                    verified_non_open_member_ids.update(final_non_open_ids)
                    event_members, group_truths = _apply_point_member_states(
                        event_members,
                        group_truths,
                        final_states,
                    )
                    bounded_ids = ",".join(sorted(final_non_open_ids)[:10])
                    issues.append(
                        Issue(
                            layer=1,
                            category=Category.API_JITTER,
                            market_id=None,
                            detail=(
                                "Gamma event/member state changed during snapshot: "
                                f"non-open for {bounded_ids}"
                            )[:200],
                        )
                    )
                still_open_ids = pending_open_missing_member_ids - final_non_open_ids
                if still_open_ids:
                    quarantinable_ids = still_open_ids - normalization_rejected_market_ids
                    group_truths, rejected_group_market_ids = _quarantine_open_keyset_absent_groups(
                        event_members,
                        group_truths,
                        quarantinable_ids,
                    )
                    if rejected_group_market_ids:
                        target_markets = [
                            market
                            for market in target_markets
                            if market.get("market_id") not in rejected_group_market_ids
                        ]
                    if quarantinable_ids:
                        bounded_ids = ",".join(sorted(quarantinable_ids)[:10])
                        remainder = len(quarantinable_ids) - min(len(quarantinable_ids), 10)
                        suffix = f" (+{remainder} more)" if remainder else ""
                        issues.append(
                            Issue(
                                layer=1,
                                category=Category.API_JITTER,
                                market_id=None,
                                detail=(
                                    "Gamma active point market absent from active keyset "
                                    f"quarantined: {bounded_ids}{suffix}"
                                )[:200],
                            )
                        )
                    normalization_rejections = still_open_ids & normalization_rejected_market_ids
                    if normalization_rejections:
                        bounded_ids = ",".join(sorted(normalization_rejections)[:5])
                        final_lookup_reason = (
                            f"event-member-normalization-rejected:{bounded_ids}"
                        )[:160]

            if final_lookup_reason is not None:
                reconciliation_reason = final_lookup_reason
                issues.append(
                    Issue(
                        layer=1,
                        category=Category.API_UNREACHABLE,
                        market_id=None,
                        detail=(
                            f"Gamma event/market reconciliation incomplete: {reconciliation_reason}"
                        )[:200],
                    )
                )

    if market_side_structure_absent_groups and reconciliation_reason is None:
        with _phase(
            "5.5/7: Recheck market-side members absent from event structure",
            stage="membership-recheck" if product == "structure" else None,
        ):
            final_lookup_reason: str | None = None
            candidate_ids = set(market_side_structure_absent_groups)
            if len(candidate_ids) > MAX_MARKET_STATE_LOOKUPS:
                final_lookup_reason = (
                    "market-side-final-state-lookup-limit-exceeded:"
                    f"{len(candidate_ids)}>{MAX_MARKET_STATE_LOOKUPS}"
                )[:160]
            else:
                try:
                    async with GammaClient(settings) as final_gamma:
                        final_states = await final_gamma.fetch_market_states(
                            sorted(candidate_ids)
                        )
                except Exception as e:  # noqa: BLE001 — fail closed at source boundary
                    final_lookup_reason = (
                        f"market-side-final-state-lookup-failed:{type(e).__name__}"
                    )[:160]
                    logger.error(
                        "Gamma final market-side/event-structure point lookup "
                        f"failed: {e!r}"
                    )
                else:
                    non_open_ids = {
                        market_id
                        for market_id, state in final_states.items()
                        if not state["active"] or state["closed"]
                    }
                    still_open_ids = candidate_ids - non_open_ids
                    # All strictly resolved candidates are intentional
                    # reconciliation exemptions. None may survive publication.
                    verified_stale_orphan_ids.update(candidate_ids)
                    if non_open_ids:
                        target_markets = [
                            market
                            for market in target_markets
                            if market.get("market_id") not in non_open_ids
                        ]
                        bounded_ids = ",".join(sorted(non_open_ids)[:10])
                        remainder = len(non_open_ids) - min(len(non_open_ids), 10)
                        suffix = f" (+{remainder} more)" if remainder else ""
                        issues.append(
                            Issue(
                                layer=1,
                                category=Category.API_JITTER,
                                market_id=None,
                                detail=(
                                    "Gamma non-open neg-risk market absent from event "
                                    f"structure quarantined: {bounded_ids}{suffix}"
                                )[:200],
                            )
                        )
                    if still_open_ids:
                        open_market_groups = {
                            market_id: market_side_structure_absent_groups[market_id]
                            for market_id in still_open_ids
                        }
                        group_truths, rejected_market_ids = (
                            _quarantine_market_side_structure_absent_groups(
                                event_members,
                                group_truths,
                                open_market_groups,
                            )
                        )
                        target_markets = [
                            market
                            for market in target_markets
                            if market.get("market_id") not in rejected_market_ids
                        ]
                        bounded_ids = ",".join(sorted(still_open_ids)[:10])
                        remainder = len(still_open_ids) - min(len(still_open_ids), 10)
                        suffix = f" (+{remainder} more)" if remainder else ""
                        issues.append(
                            Issue(
                                layer=1,
                                category=Category.API_JITTER,
                                market_id=None,
                                detail=(
                                    "Gamma active neg-risk market absent from event "
                                    f"structure quarantined: {bounded_ids}{suffix}"
                                )[:200],
                            )
                        )

            if final_lookup_reason is not None:
                reconciliation_reason = final_lookup_reason
                issues.append(
                    Issue(
                        layer=1,
                        category=Category.API_UNREACHABLE,
                        market_id=None,
                        detail=(
                            f"Gamma event/market reconciliation incomplete: {reconciliation_reason}"
                        )[:200],
                    )
                )

    # ── 6. Validate (Layer 1 / 2 / 4) ─────────────────────────────────────────
    with _phase(
        "6/7: Validate (Layer 1/2/4)",
        stage="validate" if product == "structure" else None,
    ):
        if gamma_count_reported is not None:
            # Layer 1 compares Gamma's reported active count vs how many we kept
            # post-normalize. A diff means either a bug in normalize OR API jitter.
            issues.extend(layer1_count(gamma_count_reported, normalized_count))

        # Layer 2/4 validate ONLY persisted markets. Filtered-out markets aren't
        # part of this snapshot's "completeness" claim — they'd flood
        # validation_issues with thousands of phantom warnings.
        issues.extend(layer2_fields(target_markets, now_ms=taken_at_ms))

        # Layer 4 expects {token_id: {"buy": <price-as-str-or-num>, "sell": ...}}
        # The CLOB SDK gives us {tid: {"BUY": "0.46"}} on each side — unwrap that
        # inner side-keyed dict so the validator can _safe_float() the value
        # directly. This shape contract is verified by validator tests.
        all_tids = set(prices_buy) | set(prices_sell)

        def _unwrap_side(side_dict: dict | None, key: str) -> str | None:
            if not isinstance(side_dict, dict):
                return None
            return side_dict.get(key)

        prices_combined = {
            tid: {
                "buy": _unwrap_side(prices_buy.get(tid), "BUY"),
                "sell": _unwrap_side(prices_sell.get(tid), "SELL"),
            }
            for tid in all_tids
        }
        # A global fetch failure already carries the bounded, actionable L4
        # signal. Expanding it into one CLOB_MISSING row per token duplicates
        # no information and can OOM a complete 90k+ token universe.
        if product != "structure" and not clob_fetch_failed:
            issues.extend(layer4_cross_source(target_markets, books_by_token, prices_combined))

        status = determine_snapshot_status(issues)
        is_valid = is_valid_overall(issues)  # True for OK/DEGRADED, False for FAILED
        if product == "archive" and any(issue.layer == 4 for issue in issues):
            # An archive is only useful as a complete quote-bearing artifact.
            # Its failure is non-critical to Structure, but it is not a
            # successful Archive result merely because the online product can
            # tolerate Layer-4 degradation.
            status = SnapshotStatus.FAILED
            is_valid = False
        source_complete = (
            events_coverage.result.completed
            and markets_coverage.result.completed
            and reconciliation_reason is None
            and event_failure_reason is None
        )
        publish_markets = (
            source_complete
            and is_valid
            and not any(issue.category == Category.API_UNREACHABLE for issue in issues)
        )
        if product == "archive":
            # Archive is a non-critical evidence product.  Its catalog is
            # useful to interpret the parquet artifact, but it must never
            # replace the Structure revision that Quote and M2 consume.
            publish_markets = False
        if not publish_markets:
            if product != "archive":
                status = SnapshotStatus.FAILED
                is_valid = False
        logger.info(
            f"Validated: status={status.value}, is_valid={is_valid}, "
            f"publish_markets={publish_markets}, {len(issues)} total issues "
            f"({sum(1 for i in issues if i.layer == 1)} L1, "
            f"{sum(1 for i in issues if i.layer == 2)} L2, "
            f"{sum(1 for i in issues if i.layer == 4)} L4)"
        )

    # ── 7. Persist (Parquet atomic FIRST, then SQLite single-tx) ──────────────
    finished_at_ms = int(time.time() * 1000)
    parquet_path = (
        compute_snapshot_path(settings.parquet_root, taken_at_ms)
        if product != "structure"
        else Path("not-requested")
    )
    with _phase(
        "7/7: Persist (Parquet then SQLite)",
        stage="persist" if product == "structure" else None,
    ):
        # Plan 02-09 (D-23): streaming writes. Parquet via ParquetWriter chunked
        # write; SQLite via batched executemany in a single BEGIN IMMEDIATE
        # transaction. Both consume `target_markets`. Complete neg-risk
        # membership can make this much larger than the historical ≤8k liquid
        # subset, so Phase 4 retains only compact top-of-book projections.
        # The raw Gamma list and full-depth CLOB books never materialize here.

        def _parquet_row_iter():
            """Generator: stamp snapshot metadata on each target market dict."""
            for m in target_markets:
                row = dict(m)
                row["snapshot_taken_at_ms"] = taken_at_ms
                row["snapshot_id"] = 0  # SQLite assigns the real id
                row.setdefault("fetched_at_ms", clob_done_ms)
                yield row

        if product != "structure":
            write_parquet_streaming(_parquet_row_iter(), parquet_path, batch_size=500)

        # Phase 1.1 Amendment 01: stamp events with finished_at_ms (NOT
        # clob_done_ms — events are fetched by Gamma in phase 1, not CLOB).
        for ev in event_rows:
            if ev.get("fetched_at_ms") is None:
                ev["fetched_at_ms"] = finished_at_ms

        store = SQLiteStore(settings.db_path)
        if not schema_ready:
            store.init_schema()
        if source_complete:
            source_coverage = SourceCoverage.complete(
                markets_coverage.result.items_yielded,
                events_coverage.result.items_yielded,
            )
        elif reconciliation_reason is not None:
            source_coverage = SourceCoverage.incomplete(
                "events",
                markets_coverage.result.items_yielded,
                events_coverage.result.items_yielded,
                reconciliation_reason,
            )
        elif event_failure_reason is not None or not events_coverage.result.completed:
            source_coverage = SourceCoverage.incomplete(
                "events",
                markets_coverage.result.items_yielded,
                events_coverage.result.items_yielded,
                event_failure_reason or "event-pagination-incomplete",
            )
        else:
            source_coverage = SourceCoverage.incomplete(
                "markets",
                markets_coverage.result.items_yielded,
                events_coverage.result.items_yielded,
                market_failure_reason or "market-pagination-incomplete",
            )
        snapshot_id, market_count = store.write_snapshot_streaming(
            taken_at_ms=taken_at_ms,
            finished_at_ms=finished_at_ms,
            mode=mode,
            parquet_path=str(parquet_path),
            is_valid=is_valid,
            market_rows=target_markets,
            issues=issues,
            source_coverage=source_coverage,
            event_members=event_members,
            group_truths=group_truths,
            publish_markets=publish_markets,
            notes=_derive_notes_from_issues(issues),  # Plan 03.1-02 GAP-103
            event_rows=event_rows,
            event_tag_rows=event_tag_rows,
            batch_size=500,
            data_product=product,
            snapshot_status=status.value,
            archive_status=(
                "not_requested"
                if product == "structure"
                else "local_complete"
                if product == "archive" and is_valid
                else "failed"
                if product == "archive"
                else "legacy"
            ),
        )

    # ── 7.5. Supabase mirror (D-02 dashboard) — fail-soft post-write ─────────
    # SQLite + Parquet are the source of truth (D-12 amendment). Mirror failure
    # → DEGRADED (not FAILED). Does NOT increment scheduler's failure_counter.
    #
    # F-05 (Plan 02-08): pre-empt the whole mirror block when the snapshot
    # is invalid (e.g. 0-market case caused by an API_UNREACHABLE on /markets).
    # The validator marks is_valid=False there; mirroring such a degenerate
    # row would land a status="failed" / market_count=0 row in Supabase that
    # is_valid_overall already says we shouldn't trust. Fail-soft policy says
    # "skip, don't corrupt".
    mirror = None  # type: ignore[assignment]
    if product == "structure":
        logger.info(f"step 7.5: mirror skipped for Structure snapshot_id={snapshot_id}")
    elif settings.supabase_mirror_enabled and not publish_markets:
        logger.info(
            f"step 7.5: skip Supabase mirror — market truth was not published "
            f"(snapshot_id={snapshot_id}, status={status.value})"
        )
    elif settings.supabase_mirror_enabled and not is_valid:
        logger.info(
            f"step 7.5: skip Supabase mirror — snapshot is_valid=False "
            f"(snapshot_id={snapshot_id}, status={status.value}); F-05 guard"
        )
    elif settings.supabase_mirror_enabled:
        from polyarb.storage.supabase_mirror import SupabaseMirror, narrow_market_row

        try:
            mirror = SupabaseMirror(
                settings.supabase_url,
                settings.supabase_service_key.get_secret_value(),
            )
            narrow_rows = [narrow_market_row(m, snapshot_id) for m in target_markets]
            snapshot_meta = {
                "id": snapshot_id,
                "taken_at_ms": taken_at_ms,
                "finished_at_ms": finished_at_ms,
                "mode": mode,
                "status": status.value,
                "market_count": len(target_markets),
                "parquet_url": None,  # Updated in step 7.6 if R2 upload succeeds
            }
            ok = mirror.push_snapshot(snapshot_id, snapshot_meta, narrow_rows)
            if ok:
                # Record successful mirror timestamp in SQLite (non-critical; ignore failure)
                try:
                    store.update_snapshot_mirror_fields(
                        snapshot_id,
                        supabase_mirror_at_ms=int(time.time() * 1000),
                    )
                except Exception:  # noqa: BLE001
                    pass
            else:
                issues.append(
                    Issue(
                        layer=4,
                        category=Category.UNKNOWN,
                        market_id=None,
                        detail=(
                            "Supabase mirror push returned False "
                            f"(fail-soft, snapshot_id={snapshot_id})"
                        ),
                    )
                )
        except Exception as e:  # noqa: BLE001
            logger.error(f"Supabase mirror init failed: {e!r}")
            # Plan 05: Sentry breadcrumb (warning level) — adds context to
            # the NEXT real Sentry event without opening a new issue. Mirror
            # failures are fail-soft; we don't want them spamming Sentry.
            sentry_sdk.add_breadcrumb(
                category="storage",
                message=f"supabase_mirror_failed snapshot_id={snapshot_id}",
                level="warning",
                data={"error": str(e)[:200]},
            )
            issues.append(
                Issue(
                    layer=4,
                    category=Category.UNKNOWN,
                    market_id=None,
                    detail=f"Supabase mirror init failed: {str(e)[:200]}",
                )
            )
            mirror = None  # type: ignore[assignment]
    else:
        # D-01 (Phase 02.1, BUG-7): audit log + breadcrumb for config-disabled skip.
        # Previously this branch was completely silent — daemon log had nothing,
        # Sentry events had no breadcrumb context. The 2026-05 chaos Inj 3
        # (撤 POLYARB_SUPABASE_SERVICE_KEY → pydantic flips mirror_enabled=False)
        # surfaced this as Bug #7: fail-soft path collapsed to a black hole.
        #
        # D-12 invariant: fail-soft contract unchanged — snapshot still completes.
        logger.info(
            f"step 7.5: mirror disabled — reason=config-disabled "
            f"(snapshot_id={snapshot_id}). "
            "Supabase dashboard will not update until mirror is re-enabled."
        )
        sentry_sdk.add_breadcrumb(
            category="mirror",
            level="info",
            message="mirror skipped: reason=config-disabled",
            data={"supabase_mirror_enabled": False, "snapshot_id": snapshot_id},
        )

    # ── 7.6. R2 parquet archive (D-03) — fail-soft post-write ────────────────
    # Upload the already-written parquet to Cloudflare R2. Failure → DEGRADED.
    if settings.r2_enabled and product != "structure":
        from polyarb.storage.r2_sync import R2UploadError, compute_r2_key, upload_parquet_to_r2

        r2_url: str | None = None
        try:
            r2_key = compute_r2_key(taken_at_ms)
            r2_url = upload_parquet_to_r2(
                parquet_path=parquet_path,
                bucket=settings.r2_bucket,
                key=r2_key,
                endpoint=settings.r2_endpoint,
                access_key=settings.r2_access_key_id.get_secret_value(),
                secret_key=settings.r2_secret_access_key.get_secret_value(),
            )
            # Record R2 URL in SQLite (non-critical; ignore failure)
            try:
                store.update_snapshot_mirror_fields(snapshot_id, parquet_r2_url=r2_url)
            except Exception:  # noqa: BLE001
                pass
        except R2UploadError as e:
            logger.error(f"R2 upload failed: {e!r}")
            # Plan 05: Sentry breadcrumb (warning level) — captures storage
            # failure context for the next real Sentry event without
            # opening a separate issue (fail-soft path, don't pollute Sentry).
            sentry_sdk.add_breadcrumb(
                category="storage",
                message=f"r2_upload_failed snapshot_id={snapshot_id}",
                level="warning",
                data={"error": str(e)[:200]},
            )
            issues.append(
                Issue(
                    layer=4,
                    category=Category.UNKNOWN,
                    market_id=None,
                    detail=f"R2 upload failed: {str(e)[:200]}",
                )
            )
            r2_url = None

        # Update Supabase snapshots.parquet_url if both mirror and R2 succeeded
        if r2_url is not None and mirror is not None and settings.supabase_mirror_enabled:
            try:
                mirror.update_parquet_url(snapshot_id, r2_url)
            except Exception:  # noqa: BLE001
                logger.warning(
                    "update_parquet_url post-r2 failed; snapshots.parquet_url stays NULL"
                )

    # ── 7.7. Event bus fan-out (Plan 03-05, D-05) — fail-soft post-write ─────
    # L1 → L2 cross-process NOTIFY so the L2 daemon can refresh its
    # candidate WS subscription set. Feature-flag `event_bus_enabled`
    # default FALSE per B1 spawn constraint — opt-in via Fly secret
    # `POLYARB_EVENT_BUS_ENABLED=1` ONLY after Plan 07 chaos PASS for
    # Inj L2-3. Wrapped in try/except so a NOTIFY failure NEVER blocks
    # snapshot completion (D-12 invariant). publish_snapshot_complete
    # itself is fail-soft, but we belt-and-suspender the import call too.
    if getattr(settings, "event_bus_enabled", False) and publish_markets:
        try:
            await publish_snapshot_complete(
                settings,
                snapshot_id=snapshot_id,
                taken_at_ms=taken_at_ms,
            )
        except Exception as e:  # noqa: BLE001
            logger.warning(f"event bus publish failed (fail-soft): {e!r}")
            sentry_sdk.add_breadcrumb(
                category="event-bus",
                level="warning",
                message=f"orchestrator step 7.7 publish failed: {snapshot_id}",
                data={"error": str(e)[:200]},
            )

    # ── Cache cleanup — MUST run unconditionally (after step 7.5 + 7.6) ──────
    # Even if mirror/R2 failed, the local write succeeded — clean up cache.
    # Cache cleanup happens ONLY after a successful SQLite commit. If step 7
    # failed mid-way, the cache is left intact so the next run can resume.
    if cache is not None:
        cache.cleanup()

    logger.info(
        f"Snapshot complete in {_format_elapsed(time.monotonic() - overall_t0)} "
        f"(snapshot_id={snapshot_id})"
    )

    # Aggregate issues by category for the summary line.
    cat_counts: dict[str, int] = {}
    for i in issues:
        cat_counts[i.category.value] = cat_counts.get(i.category.value, 0) + 1

    return SnapshotResult(
        snapshot_id=snapshot_id,
        market_count=len(target_markets),  # what got persisted, not full normalize count
        is_valid=is_valid,
        status=status.value,
        mode=mode,
        issue_count=len(issues),
        issue_categories=cat_counts,
        parquet_path=parquet_path,
        taken_at_ms=taken_at_ms,
        finished_at_ms=finished_at_ms,
    )
