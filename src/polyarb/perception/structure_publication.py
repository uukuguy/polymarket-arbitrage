"""Resumable normalization, certification, and publication of Structure windows."""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass
from pathlib import Path

from polyarb.config import Settings
from polyarb.perception.market_truth import EventMember, GroupTruth
from polyarb.perception.structure_contract import STRUCTURE_COMPONENTS
from polyarb.snapshot.normalizer import normalize_events, normalize_market
from polyarb.snapshot.orchestrator import SnapshotResult
from polyarb.storage.sqlite_store import SQLiteStore, StructurePublicationState


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
) -> NormalizationChunk:
    """Normalize one bounded raw keyset and atomically advance its cursor."""
    if component not in STRUCTURE_COMPONENTS or max_source_rows < 1:
        raise ValueError("invalid-structure-normalization-chunk")
    source = "markets" if component == "markets" else "events"
    progress = store.get_structure_publication_progress(publication.window_id)
    if progress is None or progress.publication.publication_id != publication.publication_id:
        raise ValueError("structure-publication-not-found")
    if component == "issues":
        rows = store.fetch_structure_duplicate_market_chunk(
            window_id=publication.window_id,
            after_market_id=after_source_key,
            limit=max_source_rows,
        )
    else:
        rows = store.fetch_structure_staging_chunk(
            window_id=publication.window_id,
            source=source,
            after_key=after_source_key,
            limit=max_source_rows,
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
                canonical.extend(_member_row(member) for member in members)
            else:
                for truth in truths:
                    row = _truth_row(truth)
                    if store.structure_event_has_duplicate_market(
                        publication.publication_id, truth.event_id
                    ):
                        row["quality"] = "incomplete-source"
                        row["reason"] = "market-id-conflict-across-events"
                    canonical.append(row)
        elif component == "markets":
            market_id = str(raw.get("id") or "")
            event_id = store.structure_event_id_for_market(
                publication.publication_id, market_id
            )
            normalized = normalize_market(raw, {market_id: event_id} if event_id else {})
            if normalized is not None:
                normalized["fetched_at_ms"] = taken_at_ms
                canonical.append(normalized)
        elif component == "issues":
            canonical.append(
                {
                    "layer": 1,
                    "category": "api_jitter",
                    "market_id": _source_key,
                    "detail": f"market-id-conflict-across-events:{raw}"[:200],
                }
            )
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
) -> StructurePublicationCheckpoint | SnapshotResult:
    """Advance at most one normalization/certification chunk or pointer switch."""
    if max_rows < 1 or max_elapsed_s <= 0:
        raise ValueError("invalid-structure-publication-budget")
    started_at = time.monotonic()
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
        )
        progress = store.get_structure_publication_progress(window_id)
        assert progress is not None
    else:
        publication = progress.publication
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
        snapshot_id = store.publish_structure_generation(publication.publication_id, now_ms)
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
                    publication.publication_id, now_ms=now_ms
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
    chunk = normalize_structure_component_chunk(
        store, publication, component, cursor, max_rows
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
    if max_rows < 1 or max_elapsed_s <= 0 or not 1 <= max_chunks <= 100:
        raise ValueError("invalid-structure-publication-slice-budget")
    if store is None:
        store = SQLiteStore(settings.db_path)
        store.init_structure_sync_schema()
    started_at = time.monotonic()
    rows_processed = 0
    chunks_processed = 0
    publication_id: str | None = None
    final_checkpoint: StructurePublicationCheckpoint | None = None

    while chunks_processed < max_chunks:
        elapsed_s = time.monotonic() - started_at
        if elapsed_s >= max_elapsed_s:
            break
        result = run_structure_publication_step(
            settings,
            window_id,
            max_rows,
            max_elapsed_s - elapsed_s,
            store=store,
        )
        if isinstance(result, SnapshotResult):
            return result
        if publication_id is None:
            publication_id = result.publication_id
        elif result.publication_id != publication_id:
            raise ValueError("structure-publication-identity-changed")
        chunks_processed += 1
        rows_processed += result.rows_processed
        final_checkpoint = result
        elapsed_s = time.monotonic() - started_at
        if result.stage == "ready" or elapsed_s >= max_elapsed_s:
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
