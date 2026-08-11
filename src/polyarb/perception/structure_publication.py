"""Resumable normalization, certification, and publication of Structure windows."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from polyarb.config import Settings
from polyarb.perception.market_truth import EventMember, GroupTruth, membership_hash
from polyarb.perception.structure_contract import (
    STRUCTURE_COMPARISON_MAX_CHUNKS_PER_SLICE,
    STRUCTURE_COMPONENTS,
    STRUCTURE_NORMALIZATION_CONTRACT_VERSION,
    STRUCTURE_POINTER_SWITCH_TRANSACTION_DEADLINE_S,
    STRUCTURE_POINTER_SWITCH_WRITER_LOCK_TIMEOUT_S,
    STRUCTURE_PUBLICATION_MAX_ROWS,
    STRUCTURE_PUBLICATION_MIN_CHUNK_REMAINING_S,
)
from polyarb.snapshot.normalizer import normalize_events, normalize_market
from polyarb.snapshot.orchestrator import SnapshotResult
from polyarb.storage.sqlite_store import SQLiteStore, StructurePublicationState

ORPHAN_NEG_RISK_QUARANTINE_REASON = (
    "active-open-neg-risk-market-parent-absent-from-active-event-catalogue"
)
MISSING_GROUP_NEG_RISK_QUARANTINE_REASON = (
    "active-open-neg-risk-market-missing-group-identity"
)
EVENT_ONLY_NEG_RISK_QUARANTINE_REASON = (
    "active-open-neg-risk-event-member-absent-from-complete-market-catalogue"
)
STRUCTURE_PUBLICATION_CHUNK_WRITER_TIMEOUT_MAX_S = 5.0


class StructurePublicationDeadlineReached(RuntimeError):
    """The current publication chunk exhausted its cooperative slice budget."""


def structure_market_source_hash(raw: dict) -> str:
    encoded = json.dumps(raw, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode()).hexdigest()


def market_quarantine_issue(
    market_id: str,
    raw: dict,
    event_ids: tuple[str, ...],
) -> dict[str, object] | None:
    """Return authenticated evidence only for one exact cross-catalogue orphan."""
    if not (
        raw.get("active") is True
        and raw.get("closed") is False
        and raw.get("negRisk") is True
    ):
        return None
    group_id = raw.get("negRiskMarketID")
    if not (isinstance(group_id, str) and bool(group_id.strip())):
        reason = MISSING_GROUP_NEG_RISK_QUARANTINE_REASON
        detail = f"Gamma neg-risk market missing group identity quarantined: {market_id}"
    elif not event_ids:
        reason = ORPHAN_NEG_RISK_QUARANTINE_REASON
        detail = (
            "Gamma active neg-risk market parent absent from active event catalogue "
            f"quarantined: {market_id}"
        )
    else:
        return None
    return {
        "layer": 1,
        "category": "api_jitter",
        "market_id": market_id,
        "detail": detail[:200],
        "raw_payload": (
            f"{reason}:{structure_market_source_hash(raw)}"
        ),
    }


def event_only_member_quarantine_issue(
    raw_event: dict,
    *,
    event_source_ordinal: int,
    market_id: str,
) -> dict[str, object] | None:
    """Build a fixed-size receipt for one exact event-only active member."""
    event_id = raw_event.get("id")
    group_id = raw_event.get("negRiskMarketID")
    markets = raw_event.get("markets")
    if not (
        isinstance(event_id, str)
        and event_id
        and isinstance(group_id, str)
        and group_id.strip() == group_id
        and group_id
        and raw_event.get("negRisk") is True
        and raw_event.get("enableNegRisk") is True
        and isinstance(markets, list)
    ):
        return None
    matches = [
        (index, member)
        for index, member in enumerate(markets)
        if isinstance(member, dict) and member.get("id") == market_id
    ]
    if len(matches) != 1:
        return None
    member_ordinal, member = matches[0]
    if not (
        member.get("active") is True
        and member.get("closed") is False
        and type(member.get("negRiskOther")) is bool
    ):
        return None
    envelope = {
        "event_id": event_id,
        "event_payload_sha256": structure_market_source_hash(raw_event),
        "event_source_ordinal": event_source_ordinal,
        "group_id": group_id,
        "market_id": market_id,
        "member_ordinal": member_ordinal,
        "member_payload_sha256": structure_market_source_hash(member),
    }
    return {
        "layer": 1,
        "category": "api_jitter",
        "market_id": market_id,
        "detail": (
            "Gamma active event member absent from market catalogue quarantined: "
            f"{market_id}"
        )[:200],
        "raw_payload": (
            f"{EVENT_ONLY_NEG_RISK_QUARANTINE_REASON}:"
            f"{structure_market_source_hash(envelope)}"
        ),
    }


def project_event_structure(
    raw_event: dict,
    quarantined_market_ids: set[str] | frozenset[str],
) -> tuple[list[EventMember], list[GroupTruth]]:
    """Remove only pre-authenticated event-only members and repair group truth."""
    _events, _tags, _mapping, members, truths = normalize_events([raw_event])
    authenticated_ids = {
        market_id
        for market_id in quarantined_market_ids
        if event_only_member_quarantine_issue(
            raw_event, event_source_ordinal=0, market_id=market_id
        )
        is not None
    }
    filtered = [
        member for member in members if member.market_id not in authenticated_ids
    ]
    projected_truths: list[GroupTruth] = []
    for truth in truths:
        group_members = [
            member
            for member in filtered
            if member.event_id == truth.event_id and member.group_id == truth.group_id
        ]
        if not group_members:
            continue
        projected_truths.append(
            GroupTruth(
                event_id=truth.event_id,
                group_id=truth.group_id,
                neg_risk_type=truth.neg_risk_type,
                expected_member_count=len(group_members),
                active_named_count=sum(
                    member.member_kind == "named" and member.active
                    for member in group_members
                ),
                membership_hash=membership_hash(
                    truth.event_id, truth.group_id, group_members
                ),
                quality=truth.quality,
                reason=truth.reason,
            )
        )
    return filtered, projected_truths


@dataclass(frozen=True)
class NormalizationChunk:
    component: str
    source_rows: int
    canonical_rows: int
    cursor: str | None
    completed: bool


@dataclass(frozen=True)
class StructurePublicationCheckpoint:
    stage: str
    component: str | None
    rows_processed: int
    cursor: str | None
    publication_id: str
    chunks_processed: int = 1
    elapsed_ms: int = 0


def _encoded_cursor(component: str, source_key: str | None, *, complete: bool) -> str:
    return f"{component}|{'done' if complete else source_key}"


def _source_cursor(component: str, durable_cursor: str | None) -> str | None:
    if durable_cursor is None or not durable_cursor.startswith(f"{component}|"):
        return None
    value = durable_cursor.split("|", 1)[1]
    return None if value == "done" else value


def _checkpoint_component(component: str) -> str:
    """Project the durable comparison transition as its first concrete phase."""
    return "legacy-universe" if component == "comparison" else component


def _member_row(member: EventMember) -> dict[str, object]:
    return {
        "event_id": member.event_id,
        "neg_risk_market_id": member.group_id,
        "market_id": member.market_id,
        "member_kind": member.member_kind,
        "active": member.active,
        "closed": member.closed,
    }


def _truth_row(truth: GroupTruth) -> dict[str, object]:
    row = asdict(truth)
    row["neg_risk_market_id"] = row.pop("group_id")
    return row


def normalize_structure_component_chunk(
    store: SQLiteStore,
    publication: StructurePublicationState,
    component: str,
    after_source_key: str | None,
    max_source_rows: int,
    *,
    writer_timeout_s: float | None = None,
    deadline_monotonic: float | None = None,
) -> NormalizationChunk:
    """Normalize one bounded raw keyset and atomically advance its cursor."""
    if (
        component not in STRUCTURE_COMPONENTS
        or not 1 <= max_source_rows <= STRUCTURE_PUBLICATION_MAX_ROWS
        or (writer_timeout_s is not None and writer_timeout_s <= 0)
    ):
        raise ValueError("invalid-structure-normalization-chunk")
    source = "markets" if component == "markets" else "events"
    progress = store.get_structure_publication_progress(publication.window_id)
    if progress is None or progress.publication.publication_id != publication.publication_id:
        raise ValueError("structure-publication-not-found")
    try:
        if component == "issues":
            rows = store.fetch_structure_issue_source_chunk(
                window_id=publication.window_id,
                after_market_id=after_source_key,
                limit=max_source_rows,
                deadline_monotonic=deadline_monotonic,
            )
        else:
            rows = store.fetch_structure_staging_chunk(
                window_id=publication.window_id,
                source=source,
                after_key=after_source_key,
                limit=max_source_rows,
                deadline_monotonic=deadline_monotonic,
            )
    except sqlite3.OperationalError as error:
        if deadline_monotonic is not None and "interrupted" in str(error).lower():
            raise StructurePublicationDeadlineReached() from error
        raise
    duplicate_event_ids = (
        store.structure_events_with_duplicate_markets(
            publication.publication_id,
            [str(source_key) for source_key, _raw in rows],
        )
        if component == "group_truth"
        else set()
    )
    event_only_market_ids = (
        store.structure_event_only_market_ids(
            publication.publication_id,
            [str(source_key) for source_key, _raw in rows],
        )
        if component in {"memberships", "group_truth"}
        else {}
    )
    market_event_ids = (
        store.structure_event_ids_for_markets(
            publication.publication_id,
            [str(source_key) for source_key, _raw in rows],
            deadline_monotonic=deadline_monotonic,
        )
        if component == "markets"
        else {}
    )
    taken_at_ms = store.structure_publication_taken_at_ms(publication.publication_id)
    canonical: list[dict[str, object]] = []
    for _source_key, raw in rows:
        if component in {"events", "event_tags", "memberships", "group_truth"}:
            event_rows, tag_rows, _mapping, members, truths = normalize_events([raw])
            if component == "events":
                for event in event_rows:
                    event["fetched_at_ms"] = taken_at_ms
                canonical.extend(event_rows)
            elif component == "event_tags":
                canonical.extend(tag_rows)
            elif component == "memberships":
                projected_members, _projected_truths = project_event_structure(
                    raw, event_only_market_ids.get(str(_source_key), frozenset())
                )
                canonical.extend(_member_row(member) for member in projected_members)
            else:
                _projected_members, projected_truths = project_event_structure(
                    raw, event_only_market_ids.get(str(_source_key), frozenset())
                )
                for truth in projected_truths:
                    row = _truth_row(truth)
                    if truth.event_id in duplicate_event_ids:
                        row["quality"] = "incomplete-source"
                        row["reason"] = "market-id-conflict-across-events"
                    canonical.append(row)
        elif component == "markets":
            market_id = str(raw.get("id") or "")
            event_id = market_event_ids.get(market_id)
            normalized = normalize_market(raw, {market_id: event_id} if event_id else {})
            if normalized is not None:
                if market_quarantine_issue(
                    market_id,
                    raw,
                    (() if event_id is None else (event_id,)),
                ) is not None:
                    continue
                normalized["fetched_at_ms"] = taken_at_ms
                canonical.append(normalized)
        elif component == "issues":
            if raw.get("source_kind") == "event_only":
                issue = event_only_member_quarantine_issue(
                    raw["raw_event"],
                    event_source_ordinal=int(raw["event_source_ordinal"]),
                    market_id=_source_key,
                )
                if issue is not None:
                    canonical.append(issue)
                continue
            event_ids = tuple(str(item) for item in raw["event_ids"])
            if len(event_ids) > 1:
                canonical.append(
                    {
                        "layer": 1,
                        "category": "api_jitter",
                        "market_id": _source_key,
                        "detail": (
                            "market-id-conflict-across-events:"
                            f"{','.join(event_ids)}"
                        )[:200],
                    }
                )
            else:
                issue = market_quarantine_issue(
                    _source_key, raw["raw"], event_ids
                )
                if issue is not None:
                    canonical.append(issue)
    sort_keys = {
        "events": lambda row: (str(row["id"]),),
        "event_tags": lambda row: (str(row["event_id"]), str(row["tag_id"])),
        "memberships": lambda row: (str(row["event_id"]), str(row["market_id"])),
        "group_truth": lambda row: (str(row["neg_risk_market_id"]),),
        "markets": lambda row: (str(row["market_id"]),),
        "issues": lambda row: (str(row.get("issue_index", "")),),
    }
    canonical.sort(key=sort_keys[component])
    completed = len(rows) < max_source_rows
    source_cursor = None if not rows else rows[-1][0]
    next_cursor = _encoded_cursor(component, source_cursor, complete=completed)
    store.append_structure_publication_chunk(
        publication_id=publication.publication_id,
        component=component,
        rows=canonical,
        expected_prior_cursor=progress.cursor,
        next_cursor=next_cursor,
        now_ms=int(time.time() * 1_000),
        writer_timeout_s=writer_timeout_s,
    )
    return NormalizationChunk(
        component, len(rows), len(canonical), source_cursor, completed
    )


def _result(
    store: SQLiteStore,
    publication: StructurePublicationState,
) -> SnapshotResult:
    current = store.current_structure_generation()
    assert current is not None
    metadata = store.structure_publication_result_metadata(publication.publication_id)
    return SnapshotResult(
        snapshot_id=int(metadata["snapshot_id"]),
        market_count=int(metadata["market_count"]),
        is_valid=bool(metadata["is_valid"]),
        status=str(metadata["status"]),
        mode=str(metadata["mode"]),
        issue_count=int(metadata["issue_count"]),
        issue_categories=dict(metadata["issue_categories"]),
        parquet_path=Path(str(metadata["parquet_path"])),
        taken_at_ms=int(metadata["taken_at_ms"]),
        finished_at_ms=int(metadata["finished_at_ms"]),
    )


def run_structure_publication_step(
    settings: Settings,
    window_id: str,
    max_rows: int,
    max_elapsed_s: float,
    *,
    store: SQLiteStore | None = None,
    deadline_monotonic: float | None = None,
) -> StructurePublicationCheckpoint | SnapshotResult:
    """Advance at most one normalization/certification chunk or pointer switch."""
    if not 1 <= max_rows <= STRUCTURE_PUBLICATION_MAX_ROWS or max_elapsed_s <= 0:
        raise ValueError("invalid-structure-publication-budget")
    started_at = time.monotonic()
    writer_timeout_s = min(
        STRUCTURE_PUBLICATION_CHUNK_WRITER_TIMEOUT_MAX_S,
        max(0.001, max_elapsed_s - STRUCTURE_PUBLICATION_MIN_CHUNK_REMAINING_S),
    )
    if store is None:
        store = SQLiteStore(settings.db_path)
        store.init_structure_sync_schema()
    progress = store.get_structure_publication_progress(window_id)
    now_ms = int(time.time() * 1_000)
    if progress is None:
        snapshot_id = store.next_structure_snapshot_id()
        publication = store.begin_structure_publication(
            window_id=window_id,
            snapshot_metadata={
                "snapshot_id": snapshot_id,
                "taken_at_ms": now_ms,
                "mode": "full",
                "data_product": "structure",
                "expected_counts": {component: 0 for component in STRUCTURE_COMPONENTS},
            },
            now_ms=now_ms,
            writer_timeout_s=writer_timeout_s,
        )
        progress = store.get_structure_publication_progress(window_id)
        assert progress is not None
    else:
        publication = progress.publication
    reconciliation = store.reconcile_structure_publication_contract(
        window_id,
        STRUCTURE_NORMALIZATION_CONTRACT_VERSION,
        now_ms,
        writer_timeout_s=writer_timeout_s,
        # Only ready→publish needs this extra authority boundary. Earlier
        # publication checkpoints retain the caller's cooperative slice.
        transaction_deadline_s=(writer_timeout_s if publication.status == "ready" else None),
    )
    if reconciliation.superseded:
        return StructurePublicationCheckpoint(
            "superseded",
            None,
            0,
            None,
            reconciliation.publication_id,
        )
    if publication.status == "published":
        return _result(store, publication)
    if time.monotonic() - started_at >= max_elapsed_s:
        certification = store.structure_certification_checkpoint(
            publication.publication_id
        )
        return StructurePublicationCheckpoint(
            "certifying" if certification is not None else "normalizing",
            (
                _checkpoint_component(certification[0])
                if certification is not None
                else progress.component or STRUCTURE_COMPONENTS[0]
            ),
            0,
            certification[1] if certification is not None else progress.cursor,
            publication.publication_id,
        )
    if publication.status == "ready":
        snapshot_id = store.publish_structure_generation(
            publication.publication_id,
            now_ms,
            transaction_deadline_s=STRUCTURE_POINTER_SWITCH_TRANSACTION_DEADLINE_S,
            writer_lock_timeout_s=STRUCTURE_POINTER_SWITCH_WRITER_LOCK_TIMEOUT_S,
        )
        refreshed = store.get_structure_publication_progress(window_id)
        assert refreshed is not None and snapshot_id == publication.snapshot_id
        return _result(store, refreshed.publication)

    component = progress.component or STRUCTURE_COMPONENTS[0]
    if progress.cursor is not None and progress.cursor.endswith("|done"):
        index = STRUCTURE_COMPONENTS.index(component)
        if index + 1 == len(STRUCTURE_COMPONENTS):
            certification_checkpoint = store.structure_certification_checkpoint(
                publication.publication_id
            )
            if certification_checkpoint is None:
                store.seal_structure_publication_counts(
                    publication.publication_id,
                    now_ms=now_ms,
                    writer_timeout_s=writer_timeout_s,
                )
                return StructurePublicationCheckpoint(
                    "certifying",
                    STRUCTURE_COMPONENTS[0],
                    0,
                    None,
                    publication.publication_id,
                )
            certification = store.advance_structure_certification_chunk(
                publication.publication_id,
                max_rows=max_rows,
                now_ms=now_ms,
                writer_timeout_s=writer_timeout_s,
            )
            return StructurePublicationCheckpoint(
                "ready" if certification.ready else "certifying",
                _checkpoint_component(certification.component),
                certification.rows_processed,
                certification.cursor,
                publication.publication_id,
            )
        component = STRUCTURE_COMPONENTS[index + 1]
    cursor = _source_cursor(component, progress.cursor)
    writer_timeout_s = min(
        STRUCTURE_PUBLICATION_CHUNK_WRITER_TIMEOUT_MAX_S,
        max(0.001, max_elapsed_s - STRUCTURE_PUBLICATION_MIN_CHUNK_REMAINING_S),
    )
    chunk = normalize_structure_component_chunk(
        store,
        publication,
        component,
        cursor,
        max_rows,
        writer_timeout_s=writer_timeout_s,
        deadline_monotonic=deadline_monotonic,
    )
    return StructurePublicationCheckpoint(
        "normalizing",
        component,
        chunk.source_rows,
        chunk.cursor,
        publication.publication_id,
    )


def run_structure_publication_slice(
    settings: Settings,
    window_id: str,
    *,
    max_rows: int,
    max_elapsed_s: float,
    max_chunks: int = 100,
    store: SQLiteStore | None = None,
) -> StructurePublicationCheckpoint | SnapshotResult:
    """Advance independently committed chunks within one cooperative process slice."""
    if (
        not 1 <= max_rows <= STRUCTURE_PUBLICATION_MAX_ROWS
        or max_elapsed_s <= 0
        or not 1 <= max_chunks <= 100
    ):
        raise ValueError("invalid-structure-publication-slice-budget")
    if store is None:
        store = SQLiteStore(settings.db_path)
        store.init_structure_sync_schema()
    started_at = time.monotonic()
    deadline_monotonic = started_at + max_elapsed_s
    print(
        "snapshot-stage stage=persist state=start elapsed_ms=0",
        file=sys.stderr,
        flush=True,
    )
    rows_processed = 0
    chunks_processed = 0
    comparison_chunks_processed = 0
    publication_id: str | None = None
    final_checkpoint: StructurePublicationCheckpoint | None = None

    while chunks_processed < max_chunks:
        elapsed_s = time.monotonic() - started_at
        remaining_s = max_elapsed_s - elapsed_s
        if remaining_s < STRUCTURE_PUBLICATION_MIN_CHUNK_REMAINING_S:
            break
        try:
            result = run_structure_publication_step(
                settings,
                window_id,
                max_rows,
                remaining_s,
                store=store,
                deadline_monotonic=deadline_monotonic,
            )
        except StructurePublicationDeadlineReached:
            if final_checkpoint is None:
                raise
            break
        if isinstance(result, SnapshotResult):
            return result
        if publication_id is None:
            publication_id = result.publication_id
        elif result.publication_id != publication_id:
            raise ValueError("structure-publication-identity-changed")
        chunks_processed += 1
        rows_processed += result.rows_processed
        final_checkpoint = result
        if result.component in {
            "legacy-universe",
            "generation-universe",
            "legacy-rejections",
            "generation-rejections",
        }:
            comparison_chunks_processed += 1
        if result.stage != "superseded":
            print(
                "structure-publication-progress "
                f"stage={result.stage} component={result.component or 'none'} "
                f"chunks={chunks_processed} rows={rows_processed}",
                file=sys.stderr,
                flush=True,
            )
        elapsed_s = time.monotonic() - started_at
        if (
            result.stage in {"ready", "superseded"}
            or elapsed_s >= max_elapsed_s
            or comparison_chunks_processed >= STRUCTURE_COMPARISON_MAX_CHUNKS_PER_SLICE
        ):
            break

    if final_checkpoint is None:
        raise ValueError("structure-publication-slice-made-no-progress")
    elapsed_ms = max(0, int(elapsed_s * 1_000))
    return StructurePublicationCheckpoint(
        stage=final_checkpoint.stage,
        component=final_checkpoint.component,
        rows_processed=rows_processed,
        cursor=final_checkpoint.cursor,
        publication_id=final_checkpoint.publication_id,
        chunks_processed=chunks_processed,
        elapsed_ms=elapsed_ms,
    )
