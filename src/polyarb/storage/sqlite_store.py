"""SQLite writer for the snapshot pipeline.

Per CONTEXT.md D-D3, write_snapshot persists the row even when is_valid=False so
validation failures become queryable. The caller (orchestrator) is responsible for
setting a non-zero process exit code based on is_valid.

Design notes (anti-patterns avoided):
- Uses stdlib sqlite3 + parameterized DDL/SQL — NO SQLAlchemy ORM.
- BEGIN IMMEDIATE + DELETE FROM markets + executemany INSERT — never INSERT OR
  REPLACE alone, which would leak rows from prior snapshots.
- isolation_level=None gives explicit transaction control (Pitfall 4).
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from loguru import logger

from polyarb.perception.market_truth import (
    EventMember,
    GroupTruth,
    SourceCoverage,
    market_truth_mismatch_reason,
    membership_hash,
)
from polyarb.storage.schemas import (
    DDL,
    EVENT_TAGS_COLUMN_ORDER,
    EVENT_TAGS_INSERT_SQL,
    EVENTS_COLUMN_ORDER,
    EVENTS_INSERT_SQL,
    L2_MIRROR_STATE_DDL,
    MARKETS_COLUMN_ORDER,
    MARKETS_INSERT_SQL,
    SCHEDULER_STATE_DDL,
    SNAPSHOT_ATTEMPTS_DDL,
    STRUCTURE_DEFER_RECEIPTS_DDL,
    STRUCTURE_GENERATIONS_DDL,
    STRUCTURE_SCHEDULE_ADJUSTMENTS_DDL,
    STRUCTURE_SYNC_WINDOWS_DDL,
    migrate_fault_auth_finalize,
    migrate_fault_intent_status,
)
from polyarb.validator.category import Category, Issue, SnapshotStatus
from polyarb.validator.layers import determine_snapshot_status

_VALID_MODES = ("subset", "full")
# Structure publication and Quote collection share one WAL database. Their
# bounded bulk transactions can legitimately overlap for tens of seconds on
# the production volume, so SQLite's five-second default is too short.
SQLITE_BUSY_TIMEOUT_S = 120.0

# Booleans that are stored as INTEGER 0/1 in SQLite — convert before insert.
_BOOL_COLUMNS = ("active", "closed", "neg_risk", "incomplete")
# events table also has bool fields stored as INTEGER 0/1.
_EVENT_BOOL_COLUMNS = ("active", "closed")

_STRUCTURE_COMPONENTS = (
    "events",
    "event_tags",
    "memberships",
    "group_truth",
    "markets",
    "issues",
)
_STRUCTURE_SOURCE_COMPONENTS = ("source_events", "source_markets")
_STRUCTURE_CERTIFICATION_COMPONENTS = (
    *_STRUCTURE_COMPONENTS,
    *_STRUCTURE_SOURCE_COMPONENTS,
)


def _install_structure_generation_freeze_triggers(con: sqlite3.Connection) -> None:
    """Reject every generation-row mutation after certification starts."""
    for component in _STRUCTURE_COMPONENTS:
        table = f"structure_generation_{component}"
        for operation, reference in (("insert", "NEW"), ("delete", "OLD")):
            con.execute(
                f"CREATE TRIGGER IF NOT EXISTS trg_{table}_frozen_{operation} "
                f"BEFORE {operation.upper()} ON {table} WHEN EXISTS (SELECT 1 FROM "
                "structure_publications p WHERE "
                f"p.snapshot_id={reference}.snapshot_id AND "
                "p.certification_component IS NOT NULL) "
                "BEGIN SELECT RAISE(ABORT,'structure-generation-frozen'); END"  # noqa: S608
            )
        con.execute(
            f"CREATE TRIGGER IF NOT EXISTS trg_{table}_frozen_update_v2 "
            f"BEFORE UPDATE ON {table} WHEN EXISTS (SELECT 1 FROM "
            "structure_publications p WHERE p.certification_component IS NOT NULL "
            "AND (p.snapshot_id=OLD.snapshot_id OR p.snapshot_id=NEW.snapshot_id)) "
            "BEGIN SELECT RAISE(ABORT,'structure-generation-frozen'); END"  # noqa: S608
        )


class StructurePublicationCursorError(ValueError):
    """A bounded write did not continue the publication's durable cursor."""


class StructureGenerationReadError(RuntimeError):
    """A generation reader could not prove one complete immutable identity."""


@dataclass(frozen=True)
class StructureReadComparison:
    legacy_snapshot_id: int | None
    generation_snapshot_id: int | None
    legacy_market_count: int | None
    generation_market_count: int | None
    legacy_universe_hash: str | None
    generation_universe_hash: str | None
    legacy_source_truth_hash: str | None
    generation_source_truth_hash: str | None
    mismatch_reasons: tuple[str, ...]

    @property
    def matches(self) -> bool:
        return not self.mismatch_reasons


@dataclass(frozen=True)
class StructureReadContext:
    """One resolved Structure identity held by one SQLite read transaction."""

    connection: sqlite3.Connection
    mode: str
    snapshot_id: int
    taken_at_ms: int
    finished_at_ms: int
    comparison: StructureReadComparison | None = None

    def table(self, component: str) -> str:
        tables = (
            {
                "events": "structure_generation_events",
                "event_tags": "structure_generation_event_tags",
                "memberships": "structure_generation_memberships",
                "group_truth": "structure_generation_group_truth",
                "markets": "structure_generation_markets",
                "issues": "structure_generation_issues",
            }
            if self.mode == "generation"
            else {
                "events": "events",
                "event_tags": "event_tags",
                "memberships": "event_market_memberships",
                "group_truth": "neg_risk_group_truth",
                "markets": "markets",
                "issues": "validation_issues",
            }
        )
        try:
            return tables[component]
        except KeyError as error:
            raise ValueError(f"unknown-structure-component:{component}") from error


@dataclass(frozen=True)
class _StructureIdentity:
    snapshot_id: int
    taken_at_ms: int
    finished_at_ms: int
    market_count: int
    publication_id: str | None = None
    validation_hash: str | None = None


def _structure_universe_hash(
    con: sqlite3.Connection,
    *,
    snapshot_id: int,
    generation: bool,
) -> tuple[str, str]:
    """Stream a certified universe receipt without materializing its row set."""
    prefix = "structure_generation_" if generation else ""
    truth_table = f"{prefix}group_truth" if generation else "neg_risk_group_truth"
    membership_table = (
        f"{prefix}memberships" if generation else "event_market_memberships"
    )
    market_table = f"{prefix}markets"
    universe_digest = hashlib.sha256()
    universe_digest.update(b"[")
    first = True
    identities = con.execute(
        "SELECT t.neg_risk_market_id,t.membership_hash,k.market_id,k.yes_token_id "
        f"FROM {truth_table} t JOIN {membership_table} m ON "
        "m.snapshot_id=t.snapshot_id AND m.event_id=t.event_id AND "
        "m.neg_risk_market_id=t.neg_risk_market_id JOIN "
        f"{market_table} k ON k.snapshot_id=m.snapshot_id AND "
        "k.market_id=m.market_id AND k.event_id=t.event_id AND "
        "k.neg_risk_market_id=t.neg_risk_market_id WHERE t.snapshot_id=? AND "
        "t.neg_risk_type='standard' AND t.quality='complete-supported' AND "
        "m.member_kind='named' AND m.active=1 AND m.closed=0 AND k.active=1 AND "
        "k.closed=0 AND k.incomplete=0 AND trim(k.yes_token_id)!='' "
        "ORDER BY t.neg_risk_market_id,t.membership_hash,k.market_id,k.yes_token_id",
        (snapshot_id,),
    )
    for row in identities:
        if not first:
            universe_digest.update(b",")
        universe_digest.update(
            json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode()
        )
        first = False
    universe_digest.update(b"]")
    universe_hash = universe_digest.hexdigest()

    source_digest = hashlib.sha256()
    source_digest.update(b"[")
    source_digest.update(
        json.dumps(universe_hash, ensure_ascii=False, separators=(",", ":")).encode()
    )
    source_digest.update(b",[")
    first = True
    rejections = con.execute(
        "SELECT neg_risk_market_id,quality,COALESCE(reason,"
        "'neg-risk-group-not-supported') "
        f"FROM {truth_table} WHERE snapshot_id=? AND "
        "(neg_risk_type!='standard' OR quality!='complete-supported') "
        "ORDER BY neg_risk_market_id,quality,reason",
        (snapshot_id,),
    )
    for row in rejections:
        if not first:
            source_digest.update(b",")
        source_digest.update(
            json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode()
        )
        first = False
    source_digest.update(b"]]")
    return universe_hash, source_digest.hexdigest()


def _resolve_legacy_structure(
    con: sqlite3.Connection,
    snapshot_id: int | None,
    *,
    latest_snapshot: bool = False,
) -> _StructureIdentity:
    where = "AND s.id=?" if snapshot_id is not None else ""
    order = "" if snapshot_id is not None else "ORDER BY s.id DESC LIMIT 1"
    params = (snapshot_id,) if snapshot_id is not None else ()
    exact_legacy = snapshot_id is not None and latest_snapshot
    row = con.execute(
        (
            "SELECT s.id,s.taken_at_ms,s.finished_at_ms,s.market_count FROM snapshots s "
            f"WHERE 1=1 {where} {order}"
            if latest_snapshot or exact_legacy
            else "SELECT s.id,s.taken_at_ms,s.finished_at_ms,s.market_count "
            "FROM snapshots s JOIN snapshot_source_coverage c "
            "ON c.snapshot_id=s.id AND c.completed=1 WHERE s.data_product='structure' "
            "AND s.market_view_published=1 AND s.is_valid=1 "
            f"{where} {order}"
        ),
        params,
    ).fetchone()
    if row is None:
        raise StructureGenerationReadError("legacy-structure-unavailable")
    return _StructureIdentity(
        int(row[0]),
        int(row[1]),
        int(row[2]),
        int(row[3]),
    )


def _resolve_generation_structure(
    con: sqlite3.Connection,
    snapshot_id: int | None,
) -> _StructureIdentity:
    current = snapshot_id is None
    if current:
        row = con.execute(
            "SELECT g.snapshot_id,g.publication_id,g.validation_hash,g.counts_json,"
            "g.certification_component,s.taken_at_ms,s.finished_at_ms,s.market_count,"
            "p.window_id,p.expected_counts_json,p.committed_counts_json,p.validation_hash,"
            "p.certification_component,p.certification_hash FROM "
            "current_structure_generation g JOIN structure_publications p "
            "ON p.publication_id=g.publication_id AND p.snapshot_id=g.snapshot_id "
            "JOIN snapshots s ON s.id=g.snapshot_id WHERE g.id=1 AND "
            "p.status='published' AND s.data_product='structure' AND "
            "s.market_view_published=1 AND s.is_valid=1"
        ).fetchone()
        if row is None:
            pointer_exists = con.execute(
                "SELECT 1 FROM current_structure_generation WHERE id=1"
            ).fetchone()
            raise StructureGenerationReadError(
                "generation-pointer-missing"
                if pointer_exists is None
                else "generation-identity-mismatch"
            )
        resolved_id, publication_id = int(row[0]), str(row[1])
        pointer_hash, pointer_counts, pointer_marker = row[2], row[3], row[4]
        metadata = row[5:]
    else:
        row = con.execute(
            "SELECT s.taken_at_ms,s.finished_at_ms,s.market_count,p.window_id,"
            "p.expected_counts_json,p.committed_counts_json,p.validation_hash,"
            "p.certification_component,p.certification_hash,p.publication_id "
            "FROM structure_publications p JOIN snapshots s ON s.id=p.snapshot_id "
            "WHERE p.snapshot_id=? AND p.status='published' AND "
            "s.data_product='structure' AND s.market_view_published=1 AND s.is_valid=1",
            (snapshot_id,),
        ).fetchone()
        if row is None:
            raise StructureGenerationReadError("generation-identity-mismatch")
        resolved_id, publication_id = int(snapshot_id), str(row[9])
        pointer_hash = pointer_counts = pointer_marker = None
        metadata = row[:9]
    (
        taken_at_ms,
        finished_at_ms,
        snapshot_market_count,
        window_id,
        expected_json,
        committed_json,
        validation_hash,
        certification_component,
        certification_hash,
    ) = metadata
    authenticated_marker = (
        "backfill-authenticated"
        if str(window_id).startswith("backfill:")
        else "bounded-complete"
    )
    if certification_component != authenticated_marker:
        raise StructureGenerationReadError("generation-identity-mismatch")
    if (
        not isinstance(validation_hash, str)
        or len(validation_hash) != 64
        or validation_hash != certification_hash
    ):
        raise StructureGenerationReadError("generation-validation-hash-mismatch")
    expected = json.loads(str(expected_json))
    committed = json.loads(str(committed_json))
    if expected != committed or set(expected) != set(_STRUCTURE_COMPONENTS):
        raise StructureGenerationReadError("generation-count-mismatch")
    if int(snapshot_market_count) != int(committed["markets"]):
        raise StructureGenerationReadError("generation-market-count-mismatch")
    canonical_counts = json.dumps(
        committed,
        sort_keys=True,
        separators=(",", ":"),
    )
    if current:
        if pointer_hash != validation_hash:
            raise StructureGenerationReadError("generation-validation-hash-mismatch")
        if pointer_counts != canonical_counts:
            raise StructureGenerationReadError("generation-count-mismatch")
        if pointer_marker != authenticated_marker:
            raise StructureGenerationReadError("generation-identity-mismatch")
    return _StructureIdentity(
        resolved_id,
        int(taken_at_ms),
        int(finished_at_ms),
        int(committed["markets"]),
        publication_id,
        validation_hash,
    )


def _compare_structure_identities(
    con: sqlite3.Connection,
    legacy: _StructureIdentity,
    generation: _StructureIdentity | None,
    generation_error: str | None,
) -> StructureReadComparison:
    reasons: list[str] = []
    receipt = None
    if generation is None:
        reasons.append(generation_error or "generation-unavailable")
    else:
        receipt = con.execute(
            "SELECT publication_id,legacy_snapshot_id,legacy_market_count,"
            "generation_market_count,legacy_universe_hash,generation_universe_hash,"
            "legacy_source_truth_hash,generation_source_truth_hash,"
            "generation_validation_hash FROM structure_generation_comparison_receipts "
            "WHERE generation_snapshot_id=?",
            (generation.snapshot_id,),
        ).fetchone()
        if receipt is None:
            reasons.append("comparison-receipt-missing")
        elif (
            receipt[0] != generation.publication_id
            or int(receipt[1]) != legacy.snapshot_id
        ):
            reasons.append("comparison-receipt-identity-mismatch")
        elif receipt[8] != generation.validation_hash:
            reasons.append("comparison-receipt-validation-hash-mismatch")
        else:
            if int(receipt[2]) != int(receipt[3]):
                reasons.append("market-count-mismatch")
            if receipt[4] != receipt[5]:
                reasons.append("universe-hash-mismatch")
            if receipt[6] != receipt[7]:
                reasons.append("source-truth-hash-mismatch")
    return StructureReadComparison(
        legacy.snapshot_id,
        generation.snapshot_id if generation is not None else None,
        int(receipt[2]) if receipt is not None else legacy.market_count,
        int(receipt[3]) if receipt is not None else (
            generation.market_count if generation is not None else None
        ),
        str(receipt[4]) if receipt is not None else None,
        str(receipt[5]) if receipt is not None else None,
        str(receipt[6]) if receipt is not None else None,
        str(receipt[7]) if receipt is not None else None,
        tuple(reasons),
    )


@contextmanager
def structure_read_transaction(
    db_path: Path | str,
    *,
    mode: str = "legacy",
    snapshot_id: int | None = None,
    legacy_latest_snapshot: bool = False,
    trace_callback: Callable[[str], None] | None = None,
) -> Iterator[StructureReadContext]:
    """Resolve and hold one Structure identity for a complete consumer operation."""
    if mode not in {"legacy", "compare", "generation"}:
        raise ValueError("invalid-structure-generation-read-mode")
    con = sqlite3.connect(f"file:{Path(db_path)}?mode=ro", uri=True)
    con.execute("PRAGMA query_only=ON")
    if trace_callback is not None:
        con.set_trace_callback(trace_callback)
    try:
        con.execute("BEGIN")
        context = resolve_structure_read_context(
            con,
            mode=mode,
            snapshot_id=snapshot_id,
            legacy_latest_snapshot=legacy_latest_snapshot,
        )
        yield context
        con.execute("COMMIT")
    except BaseException:
        if con.in_transaction:
            con.execute("ROLLBACK")
        raise
    finally:
        con.close()


def resolve_structure_read_context(
    con: sqlite3.Connection,
    *,
    mode: str = "legacy",
    snapshot_id: int | None = None,
    legacy_latest_snapshot: bool = False,
) -> StructureReadContext:
    """Resolve Structure on a caller-owned transaction without opening another read."""
    if mode not in {"legacy", "compare", "generation"}:
        raise ValueError("invalid-structure-generation-read-mode")
    if mode == "generation":
        identity = _resolve_generation_structure(con, snapshot_id)
        return StructureReadContext(
            con,
            "generation",
            identity.snapshot_id,
            identity.taken_at_ms,
            identity.finished_at_ms,
        )
    identity = _resolve_legacy_structure(
        con,
        snapshot_id,
        latest_snapshot=legacy_latest_snapshot,
    )
    comparison = None
    if mode == "compare":
        generation = None
        error = None
        try:
            generation = _resolve_generation_structure(con, snapshot_id)
        except StructureGenerationReadError as exc:
            error = str(exc)
        comparison = _compare_structure_identities(con, identity, generation, error)
    return StructureReadContext(
        con,
        "legacy",
        identity.snapshot_id,
        identity.taken_at_ms,
        identity.finished_at_ms,
        comparison,
    )


def compare_current_structure_generation(
    db_path: Path | str,
) -> StructureReadComparison:
    """Return the deterministic dual-read result consumed by strict health."""
    with structure_read_transaction(db_path, mode="compare") as read:
        assert read.comparison is not None
        return read.comparison


@dataclass(frozen=True)
class StructurePublicationState:
    publication_id: str
    snapshot_id: int
    window_id: str
    status: str
    committed_counts: dict[str, int]


@dataclass(frozen=True)
class StructurePublicationProgress:
    publication: StructurePublicationState
    component: str | None
    cursor: str | None
    validation_hash: str | None


@dataclass(frozen=True)
class StructureCertificationChunk:
    component: str | None
    cursor: str | None
    rows_processed: int
    ready: bool


@dataclass(frozen=True)
class BackfillCheckpoint:
    snapshot_id: int | None
    copied_rows: int
    cursor: str | None
    complete: bool


def _backfill_structure_snapshot_statuses(con: sqlite3.Connection) -> None:
    """Derive persisted status for Structure rows created before that column existed.

    The source of truth is the same Layer-1 issue policy used by the
    orchestrator.  Re-running this is safe: Structure status is a deterministic
    projection of its immutable validation issues and ``is_valid`` result.
    Archive and legacy-combined rows intentionally remain outside this contract.
    """
    rows = con.execute(
        "SELECT s.id,s.is_valid,vi.layer,vi.category,vi.market_id,vi.detail,"
        "vi.raw_payload "
        "FROM snapshots s "
        "LEFT JOIN validation_issues vi ON vi.snapshot_id=s.id "
        "WHERE s.data_product='structure' "
        "ORDER BY s.id,vi.id"
    ).fetchall()
    if not rows:
        return

    snapshots: dict[int, tuple[bool, list[Issue]]] = {}
    for snapshot_id, is_valid, layer, category, market_id, detail, raw_payload in rows:
        if snapshot_id not in snapshots:
            snapshots[snapshot_id] = (bool(is_valid), [])
        if layer is None:
            continue
        try:
            parsed_category = Category(category)
        except ValueError:
            parsed_category = Category.UNKNOWN
        snapshots[snapshot_id][1].append(
            Issue(
                layer=int(layer),
                category=parsed_category,
                market_id=market_id,
                detail=detail or "",
                raw_payload=raw_payload,
            )
        )

    for snapshot_id, (is_valid, issues) in snapshots.items():
        status = (
            SnapshotStatus.FAILED
            if not is_valid
            else determine_snapshot_status(issues)
        )
        con.execute(
            "UPDATE snapshots SET snapshot_status=? WHERE id=?",
            (status.value, snapshot_id),
        )


def _truth_error(prefix: str, reason: str) -> ValueError:
    """Return a stable, bounded publication-boundary error."""
    return ValueError(f"{prefix}:{reason}"[:200])


def _validate_market_truth(
    event_members: list[EventMember],
    group_truths: list[GroupTruth],
) -> None:
    """Reject projections that cannot be authoritative before touching SQLite."""
    truths_by_key: dict[tuple[str, str], GroupTruth] = {}
    seen_group_ids: set[str] = set()
    for truth in group_truths:
        key = (truth.event_id, truth.group_id)
        if key in truths_by_key or truth.group_id in seen_group_ids:
            raise _truth_error(
                "market-truth-invalid",
                f"duplicate-group-identity:{truth.event_id}/{truth.group_id}",
            )
        truths_by_key[key] = truth
        seen_group_ids.add(truth.group_id)

    members_by_key: dict[tuple[str, str], list[EventMember]] = {}
    seen_member_ids: set[str] = set()
    seen_member_keys: set[tuple[str, str, str]] = set()
    for member in event_members:
        key = (member.event_id, member.group_id)
        identity = (*key, member.market_id)
        if identity in seen_member_keys or member.market_id in seen_member_ids:
            raise _truth_error(
                "market-truth-invalid",
                f"duplicate-member-identity:{member.market_id}",
            )
        if key not in truths_by_key:
            raise _truth_error(
                "market-truth-invalid",
                f"member-without-group-truth:{member.market_id}",
            )
        seen_member_keys.add(identity)
        seen_member_ids.add(member.market_id)
        members_by_key.setdefault(key, []).append(member)

    for key, truth in truths_by_key.items():
        members = members_by_key.get(key, [])
        member_count = len(members)
        if truth.expected_member_count != member_count:
            raise _truth_error(
                "market-truth-invalid",
                f"expected-member-count:{truth.group_id}:"
                f"{truth.expected_member_count}!={member_count}",
            )
        active_named_count = sum(
            member.member_kind == "named" and member.active for member in members
        )
        if truth.active_named_count != active_named_count:
            raise _truth_error(
                "market-truth-invalid",
                f"active-named-count:{truth.group_id}:"
                f"{truth.active_named_count}!={active_named_count}",
            )
        expected_hash = membership_hash(truth.event_id, truth.group_id, members)
        if truth.membership_hash != expected_hash:
            raise _truth_error(
                "market-truth-invalid",
                f"membership-hash:{truth.group_id}",
            )
        if truth.quality in ("complete-supported", "complete-unsupported"):
            if member_count == 0:
                raise _truth_error(
                    "market-truth-invalid",
                    f"expected_member_count-zero:{truth.group_id}",
                )


def _validate_publication_boundary(
    *,
    is_valid: bool,
    issues: list[Issue],
    source_coverage: SourceCoverage,
    group_truths: list[GroupTruth],
    publish_markets: bool,
) -> None:
    if not publish_markets:
        return
    if not source_coverage.completed:
        raise _truth_error(
            "market-truth-publication-rejected",
            f"source-incomplete:{source_coverage.failure_source}",
        )
    if not is_valid:
        raise _truth_error("market-truth-publication-rejected", "snapshot-invalid")
    if any(issue.category == Category.API_UNREACHABLE for issue in issues):
        raise _truth_error(
            "market-truth-publication-rejected",
            "blocking-source-issue:api-unreachable",
        )
    incomplete = next(
        (truth for truth in group_truths if truth.quality == "incomplete-source"),
        None,
    )
    if incomplete is not None:
        raise _truth_error(
            "market-truth-publication-rejected",
            f"incomplete-source:{incomplete.group_id}",
        )


def _validate_published_market_truth(
    event_members: list[EventMember],
    group_truths: list[GroupTruth],
    market_rows: list[dict],
) -> None:
    reason = market_truth_mismatch_reason(event_members, group_truths, market_rows)
    if reason is not None:
        raise _truth_error("market-truth-invalid", reason)


def _rollback_without_masking(con: object) -> None:
    """Best-effort rollback while preserving the exception that caused it."""
    try:
        con.execute("ROLLBACK")  # type: ignore[attr-defined]
    except sqlite3.Error as rollback_error:
        logger.warning(
            f"SQLite rollback was already unavailable after the original failure: {rollback_error}"
        )


def _row_to_tuple(row: dict, snapshot_id: int) -> tuple:
    """Project a market dict into the column order required by MARKETS_INSERT_SQL.

    Always overrides the row's snapshot_id with the new id (the orchestrator may
    pass 0 as a placeholder before the snapshot_id is known).
    """
    out: list = []
    market_id = row.get("market_id")
    for col in MARKETS_COLUMN_ORDER:
        if col == "snapshot_id":
            out.append(snapshot_id)
            continue
        v = row.get(col)
        if col in ("market_id", "event_id", "neg_risk_market_id") and v is not None:
            if type(v) is not str or not v.strip() or v != v.strip():
                field = {
                    "market_id": "market-id",
                    "event_id": "event-id",
                    "neg_risk_market_id": "group-id",
                }[col]
                raise _truth_error(
                    "market-truth-invalid",
                    f"published-market-truth-mismatch:{field}:{market_id}",
                )
        if col in _BOOL_COLUMNS and v is not None:
            if type(v) is bool:
                v = int(v)
            elif type(v) is not int or v not in (0, 1):
                field = col.replace("_", "-")
                raise _truth_error(
                    "market-truth-invalid",
                    f"published-market-truth-mismatch:{field}-invalid:{market_id}",
                )
        out.append(v)
    return tuple(out)


def _event_row_to_tuple(row: dict, snapshot_id: int) -> tuple:
    """Project an event dict into EVENTS_COLUMN_ORDER for insert."""
    out: list = []
    for col in EVENTS_COLUMN_ORDER:
        if col == "snapshot_id":
            out.append(snapshot_id)
            continue
        v = row.get(col)
        if col in _EVENT_BOOL_COLUMNS and v is not None:
            v = int(bool(v))
        out.append(v)
    return tuple(out)


def _event_tag_row_to_tuple(row: dict, snapshot_id: int) -> tuple:
    """Project an event_tag dict into EVENT_TAGS_COLUMN_ORDER for insert."""
    out: list = []
    for col in EVENT_TAGS_COLUMN_ORDER:
        if col == "snapshot_id":
            out.append(snapshot_id)
            continue
        out.append(row.get(col))
    return tuple(out)


def _source_coverage_to_tuple(
    source_coverage: SourceCoverage,
    snapshot_id: int,
) -> tuple:
    return (
        snapshot_id,
        int(source_coverage.completed),
        source_coverage.market_items,
        source_coverage.event_items,
        source_coverage.failure_source,
        source_coverage.failure_reason,
    )


def _event_member_to_tuple(member: EventMember, snapshot_id: int) -> tuple:
    return (
        snapshot_id,
        member.event_id,
        member.group_id,
        member.market_id,
        member.member_kind,
        int(member.active),
        int(member.closed),
    )


def _group_truth_to_tuple(truth: GroupTruth, snapshot_id: int) -> tuple:
    if truth.expected_member_count == 0 and truth.quality != "incomplete-source":
        raise ValueError(
            "expected_member_count=0 is valid only for quality='incomplete-source'"
        )
    return (
        snapshot_id,
        truth.event_id,
        truth.group_id,
        truth.neg_risk_type,
        truth.expected_member_count,
        truth.active_named_count,
        truth.membership_hash,
        truth.quality,
        truth.reason,
    )


def _insert_market_truth(
    con: sqlite3.Connection,
    *,
    snapshot_id: int,
    source_coverage: SourceCoverage,
    event_members: list[EventMember],
    group_truths: list[GroupTruth],
) -> None:
    con.execute(
        "INSERT INTO snapshot_source_coverage("
        "snapshot_id,completed,market_items,event_items,failure_source,failure_reason"
        ") VALUES (?,?,?,?,?,?)",
        _source_coverage_to_tuple(source_coverage, snapshot_id),
    )
    if event_members:
        con.executemany(
            "INSERT INTO event_market_memberships("
            "snapshot_id,event_id,neg_risk_market_id,market_id,member_kind,active,closed"
            ") VALUES (?,?,?,?,?,?,?)",
            [_event_member_to_tuple(member, snapshot_id) for member in event_members],
        )
    if group_truths:
        con.executemany(
            "INSERT INTO neg_risk_group_truth("
            "snapshot_id,event_id,neg_risk_market_id,neg_risk_type,"
            "expected_member_count,active_named_count,membership_hash,quality,reason"
            ") VALUES (?,?,?,?,?,?,?,?,?)",
            [_group_truth_to_tuple(truth, snapshot_id) for truth in group_truths],
        )


class SQLiteStore:
    """Single-connection SQLite writer for snapshot runs.

    Usage:
        store = SQLiteStore(Path("data/state.db"))
        store.init_schema()
        store.write_snapshot(taken_at_ms=..., finished_at_ms=..., mode="subset", ...)
    """

    def __init__(self, db_path: Path) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)

    def _connect_writer(self) -> sqlite3.Connection:
        con = sqlite3.connect(
            self._db_path,
            isolation_level=None,
            timeout=SQLITE_BUSY_TIMEOUT_S,
        )
        con.execute("PRAGMA foreign_keys=ON")
        return con

    @property
    def db_path(self) -> Path:
        return self._db_path

    def init_schema(self) -> None:
        """Create tables, indexes, set WAL mode. Idempotent — safe to re-run.

        Phase 02 Plan 02-08 (F-01): after CREATE TABLE IF NOT EXISTS runs, we
        also perform a PRAGMA-driven idempotent ALTER TABLE ADD COLUMN pass
        for the snapshots table. `CREATE TABLE IF NOT EXISTS` does NOT modify
        an existing table, so legacy DBs that were initialized before Plan 03
        added supabase_mirror_at_ms + parquet_r2_url are still missing those
        columns and any UPDATE against them raises `no such column`.

        Strategy is add-only (LEARNINGS P7): we never drop, rename, or retype.
        Each (table, column, ddl) tuple is checked against PRAGMA table_info;
        the ALTER runs only if absent. Re-running init_schema after migration
        is a no-op (no duplicate-column error).
        """
        con = self._connect_writer()
        try:
            con.executescript(DDL)
            if migrate_fault_auth_finalize(con):
                con.executescript(DDL)
            if migrate_fault_intent_status(con):
                con.executescript(DDL)
            # Phase 02 Plan 02: scheduler_state singleton table
            con.executescript(SCHEDULER_STATE_DDL)
            # Parent-observed outcomes for isolated scheduler snapshot children.
            con.executescript(SNAPSHOT_ATTEMPTS_DDL)
            con.executescript(STRUCTURE_DEFER_RECEIPTS_DDL)
            con.executescript(STRUCTURE_SCHEDULE_ADJUSTMENTS_DDL)
            con.executescript(STRUCTURE_SYNC_WINDOWS_DDL)
            con.executescript(STRUCTURE_GENERATIONS_DDL)
            con.execute(
                "CREATE VIEW IF NOT EXISTS current_structure_markets AS "
                "SELECT markets.* FROM structure_generation_markets markets "
                "JOIN current_structure_generation current "
                "ON current.snapshot_id=markets.snapshot_id WHERE current.id=1"
            )
            # Phase 03.1 Plan 01: l2_mirror_state singleton (GAP-2 + GAP-3)
            con.executescript(L2_MIRROR_STATE_DDL)

            # Phase 02 Plan 02-08 (F-01): idempotent ADD COLUMN for legacy DBs.
            # Targets include snapshot archival metadata and scheduler evidence.
            # Note: SQLite ALTER TABLE ADD COLUMN cannot add NOT NULL columns
            # without a default — both targets are nullable, which is correct
            # (NULL = "never mirrored / not yet uploaded").
            def _ensure_column(table: str, column: str, ddl: str) -> None:
                rows = con.execute(f"PRAGMA table_info({table})").fetchall()
                existing = {r[1] for r in rows}
                if column not in existing:
                    con.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
                    logger.info(
                        f"sqlite_store: idempotent migration — ALTER {table} ADD COLUMN {column}"
                    )

            _ensure_column("snapshots", "supabase_mirror_at_ms", "INTEGER")
            _ensure_column("snapshots", "parquet_r2_url", "TEXT")
            _ensure_column(
                "snapshots",
                "market_view_published",
                "INTEGER NOT NULL DEFAULT 0",
            )
            _ensure_column(
                "snapshots",
                "data_product",
                "TEXT NOT NULL DEFAULT 'legacy_combined'",
            )
            _ensure_column(
                "snapshots",
                "archive_status",
                "TEXT NOT NULL DEFAULT 'legacy'",
            )
            _ensure_column(
                "snapshots",
                "snapshot_status",
                "TEXT NOT NULL DEFAULT 'ok'",
            )
            _ensure_column("snapshot_attempts", "last_stage", "TEXT")
            _ensure_column("snapshot_attempts", "elapsed_ms", "INTEGER")
            _ensure_column("structure_publications", "write_prior_cursor", "TEXT")
            _ensure_column("structure_publications", "certification_component", "TEXT")
            _ensure_column("structure_publications", "certification_row_cursor", "TEXT")
            _ensure_column("structure_publications", "certification_hash", "TEXT")
            _ensure_column("structure_publications", "certification_counts_json", "TEXT")
            _ensure_column("current_structure_generation", "validation_hash", "TEXT")
            _ensure_column("current_structure_generation", "counts_json", "TEXT")
            _ensure_column(
                "current_structure_generation", "certification_component", "TEXT"
            )
            _ensure_column("structure_sync_event_staging", "source_ordinal", "INTEGER")
            _ensure_column("structure_sync_market_staging", "source_ordinal", "INTEGER")
            _install_structure_generation_freeze_triggers(con)
            _backfill_structure_snapshot_statuses(con)
            # H-009: quote collectors lease their collecting run.  A default
            # of zero makes any legacy collecting row immediately recoverable
            # rather than leaving the single-run gate permanently wedged.
            _ensure_column(
                "neg_risk_quote_runs",
                "lease_expires_at_ms",
                "INTEGER NOT NULL DEFAULT 0",
            )
            _ensure_column(
                "neg_risk_quote_runs",
                "universe_hash",
                "TEXT NOT NULL DEFAULT ''",
            )
            _ensure_column(
                "neg_risk_quote_runs",
                "source_truth_hash",
                "TEXT NOT NULL DEFAULT ''",
            )
            for table in ("neg_risk_quote_run_legs", "neg_risk_quotes"):
                _ensure_column(table, "event_id", "TEXT NOT NULL DEFAULT ''")
                _ensure_column(table, "membership_hash", "TEXT NOT NULL DEFAULT ''")
        finally:
            con.close()
        # The base snapshot schema also contains the opportunity owner tables.
        # Finish their canonical guard/singleton bootstrap through the same
        # migration path used by opportunity-first producers.
        from polyarb.perception.store import OpportunityPerceptionStore

        OpportunityPerceptionStore(self._db_path).init_schema()

    def init_structure_sync_schema(self) -> None:
        """Prepare only the resumable Structure staging tables.

        The daemon performs the canonical full migration before it starts the
        scheduler.  Snapshot retry children must not repeat that whole scan on
        a multi-gigabyte production database.  A standalone invocation against
        a fresh database still falls back to the full bootstrap.
        """
        con = self._connect_writer()
        try:
            has_snapshot_schema = (
                con.execute(
                    "SELECT 1 FROM sqlite_master "
                    "WHERE type='table' AND name='snapshots'"
                ).fetchone()
                is not None
            )
            if has_snapshot_schema:
                con.executescript(STRUCTURE_SYNC_WINDOWS_DDL)
                con.executescript(STRUCTURE_GENERATIONS_DDL)
                con.execute(
                    "CREATE VIEW IF NOT EXISTS current_structure_markets AS "
                    "SELECT markets.* FROM structure_generation_markets markets "
                    "JOIN current_structure_generation current "
                    "ON current.snapshot_id=markets.snapshot_id WHERE current.id=1"
                )
                existing = {
                    str(row[1])
                    for row in con.execute("PRAGMA table_info(structure_publications)")
                }
                for column, ddl in (
                    ("write_prior_cursor", "TEXT"),
                    ("certification_component", "TEXT"),
                    ("certification_row_cursor", "TEXT"),
                    ("certification_hash", "TEXT"),
                    ("certification_counts_json", "TEXT"),
                ):
                    if column not in existing:
                        con.execute(
                            f"ALTER TABLE structure_publications ADD COLUMN {column} {ddl}"
                        )
                for table in (
                    "structure_sync_event_staging",
                    "structure_sync_market_staging",
                ):
                    columns = {
                        str(row[1]) for row in con.execute(f"PRAGMA table_info({table})")
                    }
                    if "source_ordinal" not in columns:
                        con.execute(f"ALTER TABLE {table} ADD COLUMN source_ordinal INTEGER")
                    con.execute(
                        f"UPDATE {table} SET source_ordinal=rowid "
                        "WHERE source_ordinal IS NULL"
                    )
                con.execute(
                    "INSERT OR IGNORE INTO structure_sync_event_market_staging("
                    "window_id,market_id,event_id,source_ordinal) "
                    "SELECT e.window_id,json_extract(member.value,'$.id'),e.event_id,"
                    "e.source_ordinal FROM structure_sync_event_staging e JOIN "
                    "json_each(e.payload_json,'$.markets') member "
                    "WHERE json_type(member.value,'$.id')='text'"
                    )
                pointer_columns = {
                    str(row[1])
                    for row in con.execute("PRAGMA table_info(current_structure_generation)")
                }
                for column in (
                    "validation_hash",
                    "counts_json",
                    "certification_component",
                ):
                    if column not in pointer_columns:
                        con.execute(
                            f"ALTER TABLE current_structure_generation "
                            f"ADD COLUMN {column} TEXT"
                        )
                _install_structure_generation_freeze_triggers(con)
                return
        finally:
            con.close()
        self.init_schema()

    def write_snapshot(
        self,
        *,
        taken_at_ms: int,
        finished_at_ms: int,
        mode: str,
        parquet_path: str,
        is_valid: bool,
        market_rows: list[dict],
        issues: list[Issue],
        source_coverage: SourceCoverage,
        event_members: list[EventMember],
        group_truths: list[GroupTruth],
        publish_markets: bool,
        notes: str | None = None,
        event_rows: list[dict] | None = None,
        event_tag_rows: list[dict] | None = None,
        data_product: str = "legacy_combined",
        archive_status: str = "legacy",
        snapshot_status: str = "ok",
    ) -> int:
        """Persist one snapshot atomically.

        Inserts snapshot metadata, source coverage, events, memberships, group
        truth, optional current markets, and issues in one BEGIN IMMEDIATE
        transaction. ``markets`` is replaced only when ``publish_markets`` is
        true. Any exception triggers ROLLBACK and re-raises (we never swallow).

        FK ordering matters: snapshots → events → event_tags → markets. A market
        that references an event_id which is NOT in events for this snapshot is
        still inserted (markets.event_id has no FK CHECK constraint enforced —
        we want orphan-tolerance per Amendment 01 design).

        Phase 1.1 Amendment 01: events / event_tags persisted alongside markets.
        Both default to empty lists for backward compat with tests that don't
        supply them.

        Returns the new `snapshots.id`.
        """
        if mode not in _VALID_MODES:
            raise ValueError(f"mode must be one of {_VALID_MODES}, got {mode!r}")

        event_rows = event_rows or []
        event_tag_rows = event_tag_rows or []
        _validate_market_truth(event_members, group_truths)
        _validate_publication_boundary(
            is_valid=is_valid,
            issues=issues,
            source_coverage=source_coverage,
            group_truths=group_truths,
            publish_markets=publish_markets,
        )
        if publish_markets:
            _validate_published_market_truth(
                event_members,
                group_truths,
                market_rows,
            )

        con = self._connect_writer()
        # Per-connection PRAGMAs (some are persistent like journal_mode=WAL after
        # init_schema, but setting again is cheap and safe).
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=NORMAL")
        con.execute("BEGIN IMMEDIATE")
        try:
            if publish_markets:
                con.execute("DELETE FROM markets")  # full overwrite (D-C1)
            cur = con.execute(
                "INSERT INTO snapshots("
                "taken_at_ms,finished_at_ms,mode,market_count,market_view_published,"
                "data_product,archive_status,snapshot_status,is_valid,parquet_path,notes,"
                "supabase_mirror_at_ms,parquet_r2_url"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    taken_at_ms,
                    finished_at_ms,
                    mode,
                    len(market_rows),
                    int(publish_markets),
                    data_product,
                    archive_status,
                    snapshot_status,
                    int(is_valid),
                    parquet_path,
                    notes,
                    None,  # supabase_mirror_at_ms — set after successful mirror push
                    None,  # parquet_r2_url — set after successful R2 upload
                ),
            )
            snapshot_id = cur.lastrowid
            assert snapshot_id is not None  # AUTOINCREMENT guarantees this

            _insert_market_truth(
                con,
                snapshot_id=snapshot_id,
                source_coverage=source_coverage,
                event_members=event_members,
                group_truths=group_truths,
            )

            # ── Amendment 01: events first (FK target for markets.event_id) ─
            event_tuples = [_event_row_to_tuple(r, snapshot_id) for r in event_rows]
            if event_tuples:
                con.executemany(EVENTS_INSERT_SQL, event_tuples)

            # ── Amendment 01: event_tags (FK references events.id) ─────────
            event_tag_tuples = [_event_tag_row_to_tuple(r, snapshot_id) for r in event_tag_rows]
            if event_tag_tuples:
                con.executemany(EVENT_TAGS_INSERT_SQL, event_tag_tuples)

            # ── markets (references events.id via event_id, FK not enforced) ─
            market_tuples = (
                [_row_to_tuple(r, snapshot_id) for r in market_rows]
                if publish_markets
                else []
            )
            if market_tuples:
                con.executemany(MARKETS_INSERT_SQL, market_tuples)

            issue_tuples = [
                (
                    snapshot_id,
                    issue.layer,
                    issue.category.value,
                    issue.market_id,
                    issue.detail,
                    issue.raw_payload,
                )
                for issue in issues
            ]
            if issue_tuples:
                con.executemany(
                    "INSERT INTO validation_issues("
                    "snapshot_id,layer,category,market_id,detail,raw_payload"
                    ") VALUES (?,?,?,?,?,?)",
                    issue_tuples,
                )

            con.execute("COMMIT")
        except Exception:
            _rollback_without_masking(con)
            logger.exception("SQLite write_snapshot rolled back")
            raise
        finally:
            con.close()

        logger.info(
            f"SQLite snapshot id={snapshot_id} mode={mode} markets={len(market_rows)} "
            f"events={len(event_rows)} event_tags={len(event_tag_rows)} "
            f"issues={len(issues)} is_valid={is_valid}"
        )
        return snapshot_id

    # Plan 02-09 (D-23): streaming variant — accepts an iterator of market dicts
    # and inserts them in batches of `batch_size` inside a single BEGIN IMMEDIATE
    # transaction. Atomicity invariant: a crash mid-batch leaves no partial
    # snapshot visible (ROLLBACK rewinds the entire write). The legacy
    # write_snapshot above is retained for callers that pass a fully-materialized
    # list (tests, one-off scripts). TODO(02-09 follow-up): remove write_snapshot
    # once orchestrator and Supabase mirror migrate to the streaming variant.
    def write_snapshot_streaming(
        self,
        *,
        taken_at_ms: int,
        finished_at_ms: int,
        mode: str,
        parquet_path: str,
        is_valid: bool,
        market_rows: Iterable[dict],
        issues: list[Issue],
        source_coverage: SourceCoverage,
        event_members: list[EventMember],
        group_truths: list[GroupTruth],
        publish_markets: bool,
        notes: str | None = None,
        event_rows: list[dict] | None = None,
        event_tag_rows: list[dict] | None = None,
        batch_size: int = 500,
        data_product: str = "legacy_combined",
        archive_status: str = "legacy",
        snapshot_status: str = "ok",
    ) -> tuple[int, int]:
        """Streaming variant of write_snapshot.

        Identical publication semantics to write_snapshot, but `market_rows`
        can be any iterable (list, generator). Published inserts run in batches
        of `batch_size` inside ONE BEGIN IMMEDIATE → COMMIT transaction, so
        per-snapshot atomicity is preserved exactly as the legacy path.

        Because the iterator length is unknown up front, we insert the
        snapshots row with market_count=0 as a placeholder and UPDATE it to
        the real count after the iterator is consumed — still inside the
        same transaction, so the final visible row always has the correct
        count.

        Returns:
            (snapshot_id, market_row_count) — the auto-assigned snapshot id
            and how many market rows were inserted.
        """
        if mode not in _VALID_MODES:
            raise ValueError(f"mode must be one of {_VALID_MODES}, got {mode!r}")

        event_rows = event_rows or []
        event_tag_rows = event_tag_rows or []
        _validate_market_truth(event_members, group_truths)
        _validate_publication_boundary(
            is_valid=is_valid,
            issues=issues,
            source_coverage=source_coverage,
            group_truths=group_truths,
            publish_markets=publish_markets,
        )

        con = self._connect_writer()
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=NORMAL")
        con.execute("BEGIN IMMEDIATE")
        market_count = 0
        try:
            if publish_markets:
                con.execute("DELETE FROM markets")  # full overwrite (D-C1)
            cur = con.execute(
                "INSERT INTO snapshots("
                "taken_at_ms,finished_at_ms,mode,market_count,market_view_published,"
                "data_product,archive_status,snapshot_status,is_valid,parquet_path,notes,"
                "supabase_mirror_at_ms,parquet_r2_url"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    taken_at_ms,
                    finished_at_ms,
                    mode,
                    0,  # placeholder; UPDATEd after stream consumed
                    int(publish_markets),
                    data_product,
                    archive_status,
                    snapshot_status,
                    int(is_valid),
                    parquet_path,
                    notes,
                    None,
                    None,
                ),
            )
            snapshot_id = cur.lastrowid
            assert snapshot_id is not None

            _insert_market_truth(
                con,
                snapshot_id=snapshot_id,
                source_coverage=source_coverage,
                event_members=event_members,
                group_truths=group_truths,
            )

            # ── events first (FK target for markets.event_id) ──────────────
            if event_rows:
                event_tuples = [_event_row_to_tuple(r, snapshot_id) for r in event_rows]
                con.executemany(EVENTS_INSERT_SQL, event_tuples)

            # ── event_tags (FK references events.id) ───────────────────────
            if event_tag_rows:
                event_tag_tuples = [_event_tag_row_to_tuple(r, snapshot_id) for r in event_tag_rows]
                con.executemany(EVENT_TAGS_INSERT_SQL, event_tag_tuples)

            # ── markets streamed in batches ────────────────────────────────
            batch: list[tuple] = []
            for row in market_rows:
                market_count += 1
                if publish_markets:
                    batch.append(_row_to_tuple(row, snapshot_id))
                    if len(batch) >= batch_size:
                        con.executemany(MARKETS_INSERT_SQL, batch)
                        batch.clear()
            if batch:
                con.executemany(MARKETS_INSERT_SQL, batch)
                batch.clear()

            if publish_markets:
                published_market_rows = [
                    {
                        "market_id": row[0],
                        "event_id": row[1],
                        "neg_risk_market_id": row[2],
                        "neg_risk": row[3],
                        "active": row[4],
                        "closed": row[5],
                    }
                    for row in con.execute(
                        "SELECT market_id,event_id,neg_risk_market_id,"
                        "neg_risk,active,closed FROM markets WHERE snapshot_id=?",
                        (snapshot_id,),
                    ).fetchall()
                ]
                _validate_published_market_truth(
                    event_members,
                    group_truths,
                    published_market_rows,
                )

            # Patch market_count to the real value (still inside same tx)
            con.execute(
                "UPDATE snapshots SET market_count=? WHERE id=?",
                (market_count, snapshot_id),
            )

            # ── issues ─────────────────────────────────────────────────────
            if issues:
                issue_tuples = [
                    (
                        snapshot_id,
                        issue.layer,
                        issue.category.value,
                        issue.market_id,
                        issue.detail,
                        issue.raw_payload,
                    )
                    for issue in issues
                ]
                con.executemany(
                    "INSERT INTO validation_issues("
                    "snapshot_id,layer,category,market_id,detail,raw_payload"
                    ") VALUES (?,?,?,?,?,?)",
                    issue_tuples,
                )

            con.execute("COMMIT")
        except Exception:
            _rollback_without_masking(con)
            logger.exception("SQLite write_snapshot_streaming rolled back")
            raise
        finally:
            con.close()

        logger.info(
            f"SQLite snapshot id={snapshot_id} mode={mode} markets={market_count} "
            f"events={len(event_rows)} event_tags={len(event_tag_rows)} "
            f"issues={len(issues)} is_valid={is_valid} "
            f"(streaming, batch_size={batch_size})"
        )
        return snapshot_id, market_count

    def update_snapshot_mirror_fields(
        self,
        snapshot_id: int,
        *,
        supabase_mirror_at_ms: int | None = None,
        parquet_r2_url: str | None = None,
    ) -> None:
        """Update supabase_mirror_at_ms and/or parquet_r2_url for a snapshot.

        Phase 02 Plan 03 — called by orchestrator after successful mirror push / R2 upload.
        Silently no-ops if snapshot_id doesn't exist (defensive; should never happen).
        """
        if supabase_mirror_at_ms is None and parquet_r2_url is None:
            return
        sets: list[str] = []
        params: list = []
        if supabase_mirror_at_ms is not None:
            sets.append("supabase_mirror_at_ms = ?")
            params.append(supabase_mirror_at_ms)
        if parquet_r2_url is not None:
            sets.append("parquet_r2_url = ?")
            params.append(parquet_r2_url)
        params.append(snapshot_id)
        con = sqlite3.connect(self._db_path, isolation_level=None)
        try:
            con.execute(
                f"UPDATE snapshots SET {', '.join(sets)} WHERE id = ?",
                tuple(params),
            )
        finally:
            con.close()

    def get_snapshot(self, snapshot_id: int) -> dict | None:
        """Return a single snapshot row by ID, or None if not found.

        Phase 02 Plan 03 — used by reconcile to retrieve snapshot metadata.
        """
        uri = f"file:{self._db_path}?mode=ro"
        try:
            con = sqlite3.connect(uri, uri=True)
        except sqlite3.OperationalError:
            return None
        try:
            row = con.execute(
                "SELECT id, taken_at_ms, finished_at_ms, mode, is_valid, market_count, "
                "parquet_path, notes, supabase_mirror_at_ms, parquet_r2_url "
                "FROM snapshots WHERE id = ?",
                (snapshot_id,),
            ).fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "taken_at_ms": row[1],
                "finished_at_ms": row[2],
                "mode": row[3],
                "is_valid": bool(row[4]),
                "market_count": row[5],
                "parquet_path": row[6],
                "notes": row[7],
                "supabase_mirror_at_ms": row[8],
                "parquet_r2_url": row[9],
            }
        finally:
            con.close()

    def get_markets_for_snapshot(self, snapshot_id: int) -> list[dict]:
        """Return all market rows for a given snapshot_id.

        Phase 02 Plan 03 — used by reconcile to get markets to mirror.
        Returns list of dicts with selected columns for Supabase narrow mirror.
        """
        uri = f"file:{self._db_path}?mode=ro"
        try:
            con = sqlite3.connect(uri, uri=True)
        except sqlite3.OperationalError:
            return []
        try:
            rows = con.execute(
                "SELECT market_id, question, slug, event_id AS event_slug, "
                "mid_price, liquidity_usd, volume_usd, end_time_ms "
                "FROM markets WHERE snapshot_id = ?",
                (snapshot_id,),
            ).fetchall()
            return [
                {
                    "market_id": r[0],
                    "question": r[1],
                    "slug": r[2],
                    "event_slug": r[3],
                    "mid_price": r[4],
                    "liquidity_usd": r[5],
                    "volume_usd": r[6],
                    "end_time_ms": r[7],
                }
                for r in rows
            ]
        finally:
            con.close()

    def read_structure_mirror_projection(
        self,
        *,
        structure_generation_read_mode: str = "legacy",
        snapshot_id: int | None = None,
    ) -> tuple[dict[str, object], list[dict[str, object]]]:
        """Read mirror metadata and rows from one exact Structure transaction."""
        with structure_read_transaction(
            self._db_path,
            mode=structure_generation_read_mode,
            snapshot_id=snapshot_id,
            legacy_latest_snapshot=(
                structure_generation_read_mode == "legacy" and snapshot_id is not None
            ),
        ) as read:
            snapshot = read.connection.execute(
                "SELECT id,taken_at_ms,finished_at_ms,mode,is_valid,market_count,"
                "parquet_r2_url FROM snapshots WHERE id=?",
                (read.snapshot_id,),
            ).fetchone()
            if snapshot is None:
                raise StructureGenerationReadError("structure-snapshot-missing")
            rows = read.connection.execute(
                "SELECT market_id,question,slug,event_id,mid_price,liquidity_usd,"
                "volume_usd,end_time_ms,yes_token_id,no_token_id "
                f"FROM {read.table('markets')} WHERE snapshot_id=? ORDER BY market_id",
                (read.snapshot_id,),
            ).fetchall()
            return (
                {
                    "id": int(snapshot[0]),
                    "taken_at_ms": int(snapshot[1]),
                    "finished_at_ms": int(snapshot[2]),
                    "mode": str(snapshot[3]),
                    "is_valid": bool(snapshot[4]),
                    "market_count": int(snapshot[5]),
                    "parquet_r2_url": snapshot[6],
                },
                [
                    {
                        "market_id": row[0],
                        "question": row[1],
                        "slug": row[2],
                        "event_slug": row[3],
                        "mid_price": row[4],
                        "liquidity_usd": row[5],
                        "volume_usd": row[6],
                        "end_time_ms": row[7],
                        "yes_token_id": row[8],
                        "no_token_id": row[9],
                    }
                    for row in rows
                ],
            )

    def get_scheduler_state(self) -> dict | None:
        """Read the scheduler singleton row. Returns None if not yet written."""
        uri = f"file:{self._db_path}?mode=ro"
        try:
            con = sqlite3.connect(uri, uri=True)
        except sqlite3.OperationalError:
            return None
        try:
            row = con.execute(
                "SELECT state, failure_counter, updated_at_ms FROM scheduler_state WHERE id=1"
            ).fetchone()
            if not row:
                return None
            return {"state": row[0], "failure_counter": row[1], "updated_at_ms": row[2]}
        finally:
            con.close()

    def upsert_scheduler_state(self, *, state: str, failure_counter: int) -> None:
        """Write/update the scheduler singleton row."""
        import time as _time

        updated_at_ms = int(_time.time() * 1000)
        con = self._connect_writer()
        try:
            con.execute(
                "INSERT INTO scheduler_state(id, state, failure_counter, updated_at_ms) "
                "VALUES (1, ?, ?, ?) "
                "ON CONFLICT(id) DO UPDATE SET "
                "state=excluded.state, failure_counter=excluded.failure_counter, "
                "updated_at_ms=excluded.updated_at_ms",
                (state, failure_counter, updated_at_ms),
            )
        finally:
            con.close()

    @staticmethod
    def _publication_state(row: tuple[object, ...]) -> StructurePublicationState:
        return StructurePublicationState(
            publication_id=str(row[0]),
            snapshot_id=int(row[1]),
            window_id=str(row[2]),
            status=str(row[3]),
            committed_counts={
                str(key): int(value)
                for key, value in json.loads(str(row[4])).items()
            },
        )

    @staticmethod
    def _structure_component_table(component: str) -> str:
        if component not in _STRUCTURE_COMPONENTS:
            raise ValueError(f"unknown-structure-component:{component}")
        return f"structure_generation_{component}"

    @classmethod
    def _generation_counts(
        cls, con: sqlite3.Connection, snapshot_id: int
    ) -> dict[str, int]:
        return {
            component: int(
                con.execute(
                    f"SELECT COUNT(*) FROM {cls._structure_component_table(component)} "
                    "WHERE snapshot_id=?",  # noqa: S608 - allowlisted table names
                    (snapshot_id,),
                ).fetchone()[0]
            )
            for component in _STRUCTURE_COMPONENTS
        }

    @classmethod
    def _generation_hash(cls, con: sqlite3.Connection, snapshot_id: int) -> str:
        digest = hashlib.sha256()
        order_by = {
            "events": "id",
            "event_tags": "event_id,tag_id",
            "memberships": "event_id,market_id",
            "group_truth": "neg_risk_market_id",
            "markets": "market_id",
            "issues": "issue_index",
        }
        for component in _STRUCTURE_COMPONENTS:
            table = cls._structure_component_table(component)
            rows = con.execute(
                f"SELECT * FROM {table} WHERE snapshot_id=? "
                f"ORDER BY {order_by[component]}",  # noqa: S608 - internal constants
                (snapshot_id,),
            )
            digest.update(component.encode())
            digest.update(b"[")
            first = True
            for row in rows:
                if not first:
                    digest.update(b",")
                digest.update(
                    json.dumps(row, separators=(",", ":"), ensure_ascii=True).encode()
                )
                first = False
            digest.update(b"]")
        return digest.hexdigest()

    def next_structure_snapshot_id(self) -> int:
        """Reserve-by-CAS is performed by begin; this only proposes an id."""
        with sqlite3.connect(self._db_path) as con:
            return int(con.execute("SELECT COALESCE(MAX(id),0)+1 FROM snapshots").fetchone()[0])

    def get_structure_publication_progress(
        self, window_id: str
    ) -> StructurePublicationProgress | None:
        with sqlite3.connect(self._db_path) as con:
            row = con.execute(
                "SELECT publication_id,snapshot_id,window_id,status,"
                "committed_counts_json,write_component,write_row_cursor,validation_hash "
                "FROM structure_publications WHERE window_id=?",
                (window_id,),
            ).fetchone()
        if row is None:
            return None
        return StructurePublicationProgress(
            self._publication_state(row[:5]),
            None if row[5] is None else str(row[5]),
            None if row[6] is None else str(row[6]),
            None if row[7] is None else str(row[7]),
        )

    def get_latest_structure_publication(self) -> StructurePublicationState | None:
        """Return the newest unfinished generation publication, if any."""
        with sqlite3.connect(self._db_path) as con:
            row = con.execute(
                "SELECT publication_id,snapshot_id,window_id,status,"
                "committed_counts_json FROM structure_publications "
                "WHERE status IN ('writing','ready') "
                "ORDER BY checkpoint_at_ms DESC, rowid DESC LIMIT 1"
            ).fetchone()
        return None if row is None else self._publication_state(row)

    def fetch_structure_staging_chunk(
        self,
        *,
        window_id: str,
        source: str,
        after_key: str | None,
        limit: int,
    ) -> list[tuple[str, dict]]:
        """Read at most ``limit`` completed raw rows using stable keyset order."""
        if source not in {"events", "markets"} or limit < 1:
            raise ValueError("invalid-structure-staging-chunk")
        table = f"structure_sync_{source[:-1] if source == 'events' else 'market'}_staging"
        key = "event_id" if source == "events" else "market_id"
        with sqlite3.connect(self._db_path) as con:
            status = con.execute(
                "SELECT status FROM structure_sync_windows WHERE id=?", (window_id,)
            ).fetchone()
            if status is None or status[0] != "complete":
                raise ValueError("structure-sync-window-not-complete")
            rows = con.execute(
                f"SELECT {key},payload_json FROM {table} WHERE window_id=? "
                f"AND (? IS NULL OR {key}>?) ORDER BY {key} LIMIT ?",  # noqa: S608
                (window_id, after_key, after_key, limit),
            ).fetchall()
        return [(str(item[0]), json.loads(str(item[1]))) for item in rows]

    def structure_publication_taken_at_ms(self, publication_id: str) -> int:
        with sqlite3.connect(self._db_path) as con:
            row = con.execute(
                "SELECT s.taken_at_ms FROM structure_publications p JOIN snapshots s "
                "ON s.id=p.snapshot_id WHERE p.publication_id=?",
                (publication_id,),
            ).fetchone()
        if row is None:
            raise ValueError("structure-publication-not-found")
        return int(row[0])

    def structure_certification_checkpoint(
        self, publication_id: str
    ) -> tuple[str, str | None] | None:
        """Return the durable bounded-certification cursor, if sealing started."""
        with sqlite3.connect(self._db_path) as con:
            row = con.execute(
                "SELECT certification_component,certification_row_cursor FROM "
                "structure_publications WHERE publication_id=?",
                (publication_id,),
            ).fetchone()
        if row is None or row[0] is None:
            return None
        return str(row[0]), None if row[1] is None else str(row[1])

    def structure_publication_result_metadata(
        self, publication_id: str
    ) -> dict[str, object]:
        """Read the published result from durable snapshot and generation facts."""
        with sqlite3.connect(self._db_path) as con:
            row = con.execute(
                "SELECT s.id,s.market_count,s.is_valid,s.snapshot_status,s.mode,"
                "s.parquet_path,s.taken_at_ms,s.finished_at_ms FROM "
                "structure_publications p JOIN snapshots s ON s.id=p.snapshot_id "
                "WHERE p.publication_id=? AND p.status='published'",
                (publication_id,),
            ).fetchone()
            if row is None:
                raise ValueError("structure-publication-not-published")
            categories = {
                str(category): int(count)
                for category, count in con.execute(
                    "SELECT category,COUNT(*) FROM structure_generation_issues "
                    "WHERE snapshot_id=? GROUP BY category ORDER BY category",
                    (row[0],),
                ).fetchall()
            }
        return {
            "snapshot_id": int(row[0]),
            "market_count": int(row[1]),
            "is_valid": bool(row[2]),
            "status": str(row[3]),
            "mode": str(row[4]),
            "parquet_path": str(row[5] or ""),
            "taken_at_ms": int(row[6]),
            "finished_at_ms": int(row[7]),
            "issue_categories": categories,
            "issue_count": sum(categories.values()),
        }

    def structure_event_id_for_market(
        self, publication_id: str, market_id: str
    ) -> str | None:
        """Resolve one market parent from durable membership or staged JSON."""
        with sqlite3.connect(self._db_path) as con:
            row = con.execute(
                "SELECT parent.event_id FROM structure_publications p JOIN "
                "structure_sync_event_market_staging parent "
                "ON parent.window_id=p.window_id WHERE p.publication_id=? "
                "AND parent.market_id=? "
                "ORDER BY parent.source_ordinal,parent.event_id LIMIT 1",
                (publication_id, market_id),
            ).fetchone()
            if row is None:
                row = con.execute(
                    "SELECT m.event_id FROM structure_publications p JOIN "
                    "structure_generation_memberships m ON m.snapshot_id=p.snapshot_id "
                    "WHERE p.publication_id=? AND m.market_id=? ORDER BY m.event_id LIMIT 1",
                    (publication_id, market_id),
                ).fetchone()
        return None if row is None else str(row[0])

    def structure_event_has_duplicate_market(
        self, publication_id: str, event_id: str
    ) -> bool:
        with sqlite3.connect(self._db_path) as con:
            return con.execute(
                "SELECT 1 FROM structure_publications p JOIN "
                "structure_sync_event_market_staging mine ON "
                "mine.window_id=p.window_id JOIN structure_sync_event_market_staging "
                "other ON other.window_id=mine.window_id AND "
                "other.market_id=mine.market_id AND other.event_id!=mine.event_id "
                "WHERE p.publication_id=? AND mine.event_id=? LIMIT 1",
                (publication_id, event_id),
            ).fetchone() is not None

    def fetch_structure_duplicate_market_chunk(
        self, *, window_id: str, after_market_id: str | None, limit: int
    ) -> list[tuple[str, str]]:
        with sqlite3.connect(self._db_path) as con:
            rows = con.execute(
                "SELECT market_id,GROUP_CONCAT(event_id,',') FROM (SELECT "
                "market_id,event_id FROM structure_sync_event_market_staging "
                "WHERE window_id=? AND (? IS NULL OR market_id>?) "
                "ORDER BY market_id,source_ordinal,event_id) GROUP BY market_id "
                "HAVING COUNT(DISTINCT event_id)>1 ORDER BY market_id LIMIT ?",
                (window_id, after_market_id, after_market_id, limit),
            ).fetchall()
        return [(str(row[0]), str(row[1])) for row in rows]

    def seal_structure_publication_counts(
        self, publication_id: str, *, now_ms: int
    ) -> dict[str, int]:
        """Freeze deterministic normalized counts before terminal certification."""
        con = self._connect_writer()
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                "SELECT snapshot_id,status,certification_component,"
                "expected_counts_json,committed_counts_json,window_id,"
                "write_component,write_row_cursor FROM structure_publications "
                "WHERE publication_id=?",
                (publication_id,),
            ).fetchone()
            if row is None or row[1] != "writing":
                raise ValueError("structure-publication-not-writing")
            if row[2] is not None:
                con.execute("COMMIT")
                return {
                    str(key): int(value)
                    for key, value in json.loads(str(row[3])).items()
                }
            counts = {
                str(key): int(value)
                for key, value in json.loads(str(row[4])).items()
            }
            encoded = json.dumps(counts, sort_keys=True, separators=(",", ":"))
            window = con.execute(
                "SELECT status FROM structure_sync_windows WHERE id=?", (row[5],)
            ).fetchone()
            if window is None or window[0] != "complete":
                raise ValueError("source-coverage-incomplete")
            if row[6] != "issues" or row[7] != "issues|done":
                raise ValueError("normalization-incomplete")
            zero_counts = json.dumps(
                {component: 0 for component in _STRUCTURE_CERTIFICATION_COMPONENTS},
                sort_keys=True,
                separators=(",", ":"),
            )
            con.execute(
                "UPDATE structure_publications SET expected_counts_json=?,"
                "committed_counts_json=?,certification_component='events',"
                "certification_row_cursor=NULL,certification_hash=?,"
                "certification_counts_json=?,checkpoint_at_ms=? WHERE publication_id=?",
                (encoded, encoded, "0" * 64, zero_counts, now_ms, publication_id),
            )
            con.execute("COMMIT")
            return counts
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def advance_structure_certification_chunk(
        self, publication_id: str, *, max_rows: int, now_ms: int
    ) -> StructureCertificationChunk:
        """Hash and validate at most one primary-key generation chunk."""
        if max_rows < 1:
            raise ValueError("structure-certification-max-rows-must-be-positive")
        order = {
            "events": ("id",),
            "event_tags": ("event_id", "tag_id"),
            "memberships": ("event_id", "market_id"),
            "group_truth": ("neg_risk_market_id",),
            "markets": ("market_id",),
            "issues": ("issue_index",),
            "source_events": ("source_ordinal", "event_id"),
            "source_markets": ("source_ordinal", "market_id"),
        }
        with sqlite3.connect(self._db_path) as read_con:
            publication = read_con.execute(
                "SELECT snapshot_id,window_id,status,expected_counts_json,"
                "committed_counts_json,certification_component,"
                "certification_row_cursor,certification_hash,"
                "certification_counts_json,s.taken_at_ms "
                "FROM structure_publications p JOIN snapshots s ON "
                "s.id=p.snapshot_id WHERE p.publication_id=?",
                (publication_id,),
            ).fetchone()
            if publication is None or publication[2] != "writing":
                raise ValueError("structure-publication-not-writing")
            if publication[3] != publication[4]:
                raise ValueError("generation-incomplete")
            snapshot_id = int(publication[0])
            window_id = str(publication[1])
            taken_at_ms = int(publication[9])
            component = str(publication[5] or _STRUCTURE_COMPONENTS[0])
            if component not in _STRUCTURE_CERTIFICATION_COMPONENTS:
                raise ValueError("unknown-structure-certification-component")
            cursor = None if publication[6] is None else str(publication[6])
            prior_hash = str(publication[7] or ("0" * 64))
            scanned_counts = {
                str(key): int(value)
                for key, value in json.loads(str(publication[8])).items()
            }
            for name in _STRUCTURE_CERTIFICATION_COMPONENTS:
                scanned_counts.setdefault(name, 0)
            keys = order[component]
            cursor_values = None if cursor is None else json.loads(cursor)
            clause = ""
            parameters: list[object] = [
                window_id if component in _STRUCTURE_SOURCE_COMPONENTS else snapshot_id
            ]
            if cursor_values is not None:
                clause = (
                    f" AND ({','.join(keys)}) > "
                    f"({','.join('?' for _ in keys)})"
                )
                parameters.extend(cursor_values)
            parameters.append(max_rows)
            if component in _STRUCTURE_SOURCE_COMPONENTS:
                source = component.removeprefix("source_")
                singular = "event" if source == "events" else "market"
                table = f"structure_sync_{singular}_staging"
                rows = read_con.execute(
                    f"SELECT source_ordinal,{singular}_id,payload_json FROM {table} "
                    f"WHERE window_id=?{clause} ORDER BY {','.join(keys)} LIMIT ?",
                    parameters,
                ).fetchall()
                positions = {"source_ordinal": 0, f"{singular}_id": 1}
            else:
                table = self._structure_component_table(component)
                rows = read_con.execute(
                    f"SELECT * FROM {table} WHERE snapshot_id=?{clause} "
                    f"ORDER BY {','.join(keys)} LIMIT ?",  # noqa: S608
                    parameters,
                ).fetchall()
                column_names = [
                    item[0]
                    for item in read_con.execute(
                        f"SELECT * FROM {table} LIMIT 0"  # noqa: S608
                    ).description
                ]
                positions = {name: index for index, name in enumerate(column_names)}
            if component == "group_truth":
                for truth in rows:
                    event_id = str(truth[1])
                    group_id = str(truth[2])
                    member_rows = read_con.execute(
                        "SELECT market_id,member_kind,active,closed FROM "
                        "structure_generation_memberships WHERE snapshot_id=? "
                        "AND event_id=? AND neg_risk_market_id=? ORDER BY market_id",
                        (snapshot_id, event_id, group_id),
                    ).fetchall()
                    members = [
                        EventMember(
                            event_id,
                            group_id,
                            str(member[0]),
                            member[1],
                            bool(member[2]),
                            bool(member[3]),
                        )
                        for member in member_rows
                    ]
                    if (
                        truth[7] == "incomplete-source"
                        or
                        len(members) != int(truth[4])
                        or sum(
                            member.member_kind == "named" and member.active
                            for member in members
                        )
                        != int(truth[5])
                        or membership_hash(event_id, group_id, members) != truth[6]
                    ):
                        raise ValueError("membership-invalid")
            elif component == "memberships":
                for member in rows:
                    aligned = read_con.execute(
                        "SELECT 1 FROM structure_generation_events e JOIN "
                        "structure_generation_markets k ON k.snapshot_id=e.snapshot_id "
                        "WHERE e.snapshot_id=? AND e.id=? AND k.market_id=? "
                        "AND k.event_id=? AND k.neg_risk_market_id=?",
                        (snapshot_id, member[1], member[3], member[1], member[2]),
                    ).fetchone()
                    if aligned is None:
                        raise ValueError("membership-invalid")
            elif component == "markets":
                for market in rows:
                    source = read_con.execute(
                        "SELECT json_extract(raw.payload_json,'$.negRisk'),"
                        "json_extract(raw.payload_json,'$.negRiskMarketID'),"
                        "json_extract(raw.payload_json,'$.active'),"
                        "json_extract(raw.payload_json,'$.closed'),parent.event_id "
                        "FROM structure_publications p JOIN "
                        "structure_sync_market_staging raw ON raw.window_id=p.window_id "
                        "LEFT JOIN structure_sync_event_market_staging parent ON "
                        "parent.window_id=p.window_id AND parent.market_id=raw.market_id "
                        "WHERE p.publication_id=? AND raw.market_id=? ORDER BY "
                        "parent.source_ordinal,parent.event_id LIMIT 1",
                        (publication_id, market[1]),
                    ).fetchone()
                    if source is None or (
                        source[0], source[1], source[2], source[3], source[4]
                    ) != (market[17], market[18], market[15], market[16], market[22]):
                        raise ValueError("source-truth-invalid")
                    if (market[17] == 1 or market[18] is not None) and read_con.execute(
                        "SELECT 1 FROM structure_generation_memberships m JOIN "
                        "structure_generation_group_truth t ON "
                        "t.snapshot_id=m.snapshot_id AND t.event_id=m.event_id AND "
                        "t.neg_risk_market_id=m.neg_risk_market_id WHERE "
                        "m.snapshot_id=? AND m.market_id=? AND m.event_id=? AND "
                        "m.neg_risk_market_id=?",
                        (snapshot_id, market[1], market[22], market[18]),
                    ).fetchone() is None:
                        raise ValueError("source-truth-invalid")
            elif component == "issues" and rows:
                raise ValueError("generation-validation-issues")
            elif component == "source_events":
                from polyarb.snapshot.normalizer import normalize_events

                for source_event in rows:
                    raw = json.loads(str(source_event[2]))
                    events, tags, _mapping, members, truths = normalize_events([raw])
                    if len(events) != 1:
                        raise ValueError("source-truth-invalid")
                    events[0]["fetched_at_ms"] = taken_at_ms
                    actual_event = read_con.execute(
                        f"SELECT {','.join(EVENTS_COLUMN_ORDER)} FROM "
                        "structure_generation_events WHERE snapshot_id=? AND id=?",
                        (snapshot_id, source_event[1]),
                    ).fetchone()
                    if actual_event != _event_row_to_tuple(events[0], snapshot_id):
                        raise ValueError("source-truth-invalid")
                    expected_tags = sorted(
                        _event_tag_row_to_tuple(tag, snapshot_id) for tag in tags
                    )
                    actual_tags = read_con.execute(
                        f"SELECT {','.join(EVENT_TAGS_COLUMN_ORDER)} FROM "
                        "structure_generation_event_tags WHERE snapshot_id=? "
                        "AND event_id=? ORDER BY tag_id",
                        (snapshot_id, source_event[1]),
                    ).fetchall()
                    if actual_tags != expected_tags:
                        raise ValueError("source-truth-invalid")
                    for generated_component, expected_rows in (
                        ("memberships", members),
                        ("group_truth", truths),
                    ):
                        expected_values = []
                        columns: tuple[str, ...] | None = None
                        for expected_row in expected_rows:
                            if isinstance(expected_row, EventMember):
                                canonical_row: object = {
                                    "event_id": expected_row.event_id,
                                    "neg_risk_market_id": expected_row.group_id,
                                    "market_id": expected_row.market_id,
                                    "member_kind": expected_row.member_kind,
                                    "active": expected_row.active,
                                    "closed": expected_row.closed,
                                }
                            else:
                                assert isinstance(expected_row, GroupTruth)
                                canonical_row = {
                                    "event_id": expected_row.event_id,
                                    "neg_risk_market_id": expected_row.group_id,
                                    "neg_risk_type": expected_row.neg_risk_type,
                                    "expected_member_count": (
                                        expected_row.expected_member_count
                                    ),
                                    "active_named_count": expected_row.active_named_count,
                                    "membership_hash": expected_row.membership_hash,
                                    "quality": expected_row.quality,
                                    "reason": expected_row.reason,
                                }
                            columns, values = self._component_values(
                                generated_component, canonical_row, snapshot_id, 0
                            )
                            expected_values.append(values)
                        if columns is None:
                            continue
                        actual_values = read_con.execute(
                            f"SELECT {','.join(columns)} FROM "
                            f"{self._structure_component_table(generated_component)} "
                            "WHERE snapshot_id=? AND event_id=? ORDER BY "
                            + (
                                "market_id"
                                if generated_component == "memberships"
                                else "neg_risk_market_id"
                            ),
                            (snapshot_id, source_event[1]),
                        ).fetchall()
                        if actual_values != sorted(expected_values):
                            raise ValueError("source-truth-invalid")
            elif component == "source_markets":
                from polyarb.snapshot.normalizer import normalize_market

                for source_market in rows:
                    raw = json.loads(str(source_market[2]))
                    parent = read_con.execute(
                        "SELECT event_id FROM structure_sync_event_market_staging "
                        "WHERE window_id=? AND market_id=? ORDER BY "
                        "source_ordinal,event_id LIMIT 1",
                        (window_id, source_market[1]),
                    ).fetchone()
                    normalized = normalize_market(
                        raw,
                        (
                            {str(source_market[1]): str(parent[0])}
                            if parent is not None
                            else {}
                        ),
                    )
                    if normalized is None:
                        raise ValueError("source-truth-invalid")
                    normalized["fetched_at_ms"] = taken_at_ms
                    generated = read_con.execute(
                        f"SELECT {','.join(MARKETS_COLUMN_ORDER)} "
                        "FROM structure_generation_markets WHERE snapshot_id=? "
                        "AND market_id=?",
                        (snapshot_id, source_market[1]),
                    ).fetchone()
                    if generated != _row_to_tuple(normalized, snapshot_id):
                        raise ValueError("source-truth-invalid")
            digest = hashlib.sha256()
            digest.update(bytes.fromhex(prior_hash))
            if cursor is None:
                digest.update(component.encode())
            for row in rows:
                digest.update(
                    json.dumps(row, separators=(",", ":"), ensure_ascii=True).encode()
                )
            scanned_counts[component] += len(rows)
            next_hash = digest.hexdigest()
            next_cursor: str | None = cursor
            if rows:
                next_cursor = json.dumps(
                    [rows[-1][positions[key]] for key in keys], separators=(",", ":")
                )
                ready = False
                next_component = component
            else:
                committed_counts = json.loads(str(publication[4]))
                expected_count = int(
                    committed_counts[
                        component.removeprefix("source_")
                        if component in _STRUCTURE_SOURCE_COMPONENTS
                        else component
                    ]
                )
                if scanned_counts[component] != expected_count:
                    raise ValueError("generation-count-mismatch")
                index = _STRUCTURE_CERTIFICATION_COMPONENTS.index(component)
                ready = index + 1 == len(_STRUCTURE_CERTIFICATION_COMPONENTS)
                next_component = (
                    None if ready else _STRUCTURE_CERTIFICATION_COMPONENTS[index + 1]
                )
                next_cursor = None
        con = self._connect_writer()
        try:
            con.execute("BEGIN IMMEDIATE")
            if ready:
                cur = con.execute(
                    "UPDATE structure_publications SET status='ready',validation_hash=?,"
                    "certification_component='bounded-complete',"
                    "certification_row_cursor=NULL,certification_hash=?,"
                    "certification_counts_json=?,certified_at_ms=?,checkpoint_at_ms=? "
                    "WHERE publication_id=? "
                    "AND status='writing' AND certification_component IS ? "
                    "AND certification_row_cursor IS ? AND "
                    "COALESCE(certification_hash,?)=?",
                    (
                        next_hash, next_hash,
                        json.dumps(scanned_counts, sort_keys=True, separators=(",", ":")),
                        now_ms, now_ms, publication_id,
                        publication[5], publication[6], "0" * 64, prior_hash,
                    ),
                )
            else:
                cur = con.execute(
                    "UPDATE structure_publications SET certification_component=?,"
                    "certification_row_cursor=?,certification_hash=?,"
                    "certification_counts_json=?,checkpoint_at_ms=? "
                    "WHERE publication_id=? AND status='writing' "
                    "AND certification_component IS ? AND certification_row_cursor IS ? "
                    "AND COALESCE(certification_hash,?)=?",
                    (
                        next_component, next_cursor, next_hash,
                        json.dumps(scanned_counts, sort_keys=True, separators=(",", ":")),
                        now_ms, publication_id,
                        publication[5], publication[6], "0" * 64, prior_hash,
                    ),
                )
            if cur.rowcount != 1:
                raise StructurePublicationCursorError(
                    "structure-certification-cursor-mismatch"
                )
            con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()
        return StructureCertificationChunk(
            next_component, next_cursor, len(rows), ready
        )

    @staticmethod
    def _legacy_generation_hash(con: sqlite3.Connection, snapshot_id: int) -> str:
        """Hash legacy rows in the exact canonical generation column order."""
        market_columns = tuple(
            column for column in MARKETS_COLUMN_ORDER if column != "snapshot_id"
        )
        queries = {
            "events": (
                "SELECT snapshot_id,id,slug,title,ticker,active,closed,liquidity_usd,"
                "volume_usd,end_time_ms,fetched_at_ms,page_fetched_at_ms FROM events "
                "WHERE snapshot_id=? ORDER BY id"
            ),
            "event_tags": (
                "SELECT snapshot_id,event_id,tag_id,tag_label,tag_slug FROM event_tags "
                "WHERE snapshot_id=? ORDER BY event_id,tag_id"
            ),
            "memberships": (
                "SELECT snapshot_id,event_id,neg_risk_market_id,market_id,member_kind,"
                "active,closed FROM event_market_memberships WHERE snapshot_id=? "
                "ORDER BY event_id,market_id"
            ),
            "group_truth": (
                "SELECT snapshot_id,event_id,neg_risk_market_id,neg_risk_type,"
                "expected_member_count,active_named_count,membership_hash,quality,reason "
                "FROM neg_risk_group_truth WHERE snapshot_id=? ORDER BY neg_risk_market_id"
            ),
            "markets": (
                f"SELECT snapshot_id,{','.join(market_columns)} FROM markets "
                "WHERE snapshot_id=? ORDER BY market_id"
            ),
            "issues": (
                "SELECT snapshot_id,id,layer,category,market_id,detail,raw_payload "
                "FROM validation_issues WHERE snapshot_id=? ORDER BY id"
            ),
        }
        digest = hashlib.sha256()
        for component in _STRUCTURE_COMPONENTS:
            rows = con.execute(queries[component], (snapshot_id,))
            digest.update(component.encode())
            digest.update(b"[")
            first = True
            for row in rows:
                if not first:
                    digest.update(b",")
                digest.update(
                    json.dumps(row, separators=(",", ":"), ensure_ascii=True).encode()
                )
                first = False
            digest.update(b"]")
        return digest.hexdigest()

    def begin_structure_publication(
        self,
        window_id: str,
        snapshot_metadata: dict[str, object],
        now_ms: int,
    ) -> StructurePublicationState:
        """Create or resume the publication bound to one complete raw window."""
        if not window_id or now_ms < 0:
            raise ValueError("invalid-structure-publication")
        snapshot_id = snapshot_metadata.get("snapshot_id")
        taken_at_ms = snapshot_metadata.get("taken_at_ms")
        mode = snapshot_metadata.get("mode")
        data_product = snapshot_metadata.get("data_product")
        expected = snapshot_metadata.get("expected_counts")
        if (
            type(snapshot_id) is not int
            or snapshot_id <= 0
            or type(taken_at_ms) is not int
            or taken_at_ms < 0
            or mode not in _VALID_MODES
            or data_product != "structure"
            or not isinstance(expected, dict)
            or set(expected) != set(_STRUCTURE_COMPONENTS)
            or any(type(value) is not int or value < 0 for value in expected.values())
        ):
            raise ValueError("invalid-structure-publication-metadata")
        counts = {component: 0 for component in _STRUCTURE_COMPONENTS}
        expected_json = json.dumps(expected, sort_keys=True, separators=(",", ":"))
        counts_json = json.dumps(counts, sort_keys=True, separators=(",", ":"))
        con = self._connect_writer()
        try:
            con.execute("BEGIN IMMEDIATE")
            existing = con.execute(
                "SELECT publication_id,snapshot_id,window_id,status,"
                "committed_counts_json FROM structure_publications WHERE window_id=?",
                (window_id,),
            ).fetchone()
            if existing is not None:
                if int(existing[1]) != snapshot_id:
                    raise ValueError("structure-publication-window-conflict")
                con.execute("COMMIT")
                return self._publication_state(existing)
            window = con.execute(
                "SELECT status FROM structure_sync_windows WHERE id=?", (window_id,)
            ).fetchone()
            if window is None or window[0] != "complete":
                raise ValueError("structure-sync-window-not-complete")
            if con.execute(
                "SELECT 1 FROM snapshots WHERE id=?", (snapshot_id,)
            ).fetchone() is not None:
                raise ValueError("structure-publication-snapshot-conflict")
            con.execute(
                "INSERT INTO snapshots(id,taken_at_ms,finished_at_ms,mode,market_count,"
                "market_view_published,data_product,archive_status,snapshot_status,is_valid,"
                "parquet_path) VALUES (?,?,?,?,0,0,'structure','local','building',0,'')",
                (snapshot_id, taken_at_ms, now_ms, mode),
            )
            publication_id = uuid.uuid4().hex
            con.execute(
                "INSERT INTO structure_publications(publication_id,window_id,snapshot_id,"
                "status,expected_counts_json,committed_counts_json,created_at_ms,"
                "checkpoint_at_ms) VALUES (?,?,?,'writing',?,?,?,?)",
                (
                    publication_id,
                    window_id,
                    snapshot_id,
                    expected_json,
                    counts_json,
                    now_ms,
                    now_ms,
                ),
            )
            con.execute("COMMIT")
            return StructurePublicationState(
                publication_id, snapshot_id, window_id, "writing", counts
            )
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    @staticmethod
    def _component_values(
        component: str, row: object, snapshot_id: int, issue_index: int
    ) -> tuple[tuple[str, ...], tuple[object, ...]]:
        if component == "issues" and isinstance(row, Issue):
            data: dict[str, object] = {
                "layer": row.layer,
                "category": row.category.value,
                "market_id": row.market_id,
                "detail": row.detail,
                "raw_payload": row.raw_payload,
            }
        elif isinstance(row, dict):
            data = row
        else:
            raise ValueError(f"invalid-structure-component-row:{component}")
        columns_by_component = {
            "events": (
                "id", "slug", "title", "ticker", "active", "closed",
                "liquidity_usd", "volume_usd", "end_time_ms", "fetched_at_ms",
                "page_fetched_at_ms",
            ),
            "event_tags": ("event_id", "tag_id", "tag_label", "tag_slug"),
            "memberships": (
                "event_id", "neg_risk_market_id", "market_id", "member_kind",
                "active", "closed",
            ),
            "group_truth": (
                "event_id", "neg_risk_market_id", "neg_risk_type",
                "expected_member_count", "active_named_count", "membership_hash",
                "quality", "reason",
            ),
            "markets": tuple(
                column for column in MARKETS_COLUMN_ORDER if column != "snapshot_id"
            ),
            "issues": (
                "issue_index", "layer", "category", "market_id", "detail", "raw_payload"
            ),
        }
        columns = columns_by_component[component]
        values: list[object] = []
        for column in columns:
            value = issue_index if column == "issue_index" else data.get(column)
            if column in _BOOL_COLUMNS or (
                component == "events" and column in _EVENT_BOOL_COLUMNS
            ):
                if value is not None:
                    value = int(bool(value))
            values.append(value)
        return ("snapshot_id", *columns), (snapshot_id, *values)

    def append_structure_publication_chunk(
        self,
        publication_id: str,
        component: str,
        rows: Iterable[object],
        expected_prior_cursor: str | None,
        next_cursor: str | None,
        now_ms: int,
    ) -> None:
        """Write one bounded component chunk and its cursor in one transaction."""
        table = self._structure_component_table(component)
        if not publication_id or now_ms < 0:
            raise ValueError("invalid-structure-publication-chunk")
        materialized = tuple(rows)
        con = self._connect_writer()
        try:
            con.execute("BEGIN IMMEDIATE")
            publication = con.execute(
                "SELECT snapshot_id,status,write_component,write_prior_cursor,"
                "write_row_cursor,"
                "committed_counts_json FROM structure_publications "
                "WHERE publication_id=?",
                (publication_id,),
            ).fetchone()
            if publication is None or publication[1] != "writing":
                raise ValueError("structure-publication-not-writing")
            snapshot_id = int(publication[0])
            previous_component = publication[2]
            durable_prior_cursor = publication[3]
            durable_cursor = publication[4]
            counts = json.loads(str(publication[5]))
            is_replay = (
                previous_component == component
                and durable_prior_cursor == expected_prior_cursor
                and durable_cursor == next_cursor
            )
            if is_replay:
                first_issue_index = int(counts[component]) - len(materialized) + 1
                if first_issue_index < 1:
                    raise StructurePublicationCursorError(
                        "structure-publication-replay-mismatch"
                    )
                key_columns = {
                    "events": ("snapshot_id", "id"),
                    "event_tags": ("snapshot_id", "event_id", "tag_id"),
                    "memberships": (
                        "snapshot_id", "event_id", "neg_risk_market_id", "market_id"
                    ),
                    "group_truth": (
                        "snapshot_id", "event_id", "neg_risk_market_id"
                    ),
                    "markets": ("snapshot_id", "market_id"),
                    "issues": ("snapshot_id", "issue_index"),
                }[component]
                authenticated = True
                for offset, item in enumerate(materialized):
                    columns, values = self._component_values(
                        component, item, snapshot_id, first_issue_index + offset
                    )
                    by_column = dict(zip(columns, values, strict=True))
                    where = " AND ".join(f"{column}=?" for column in key_columns)
                    durable = con.execute(
                        f"SELECT {','.join(columns)} FROM {table} WHERE {where}",  # noqa: S608
                        tuple(by_column[column] for column in key_columns),
                    ).fetchone()
                    if durable != values:
                        authenticated = False
                        break
                if authenticated:
                    con.execute("COMMIT")
                    return
                raise StructurePublicationCursorError(
                    "structure-publication-replay-mismatch"
                )
            if durable_cursor != expected_prior_cursor:
                raise StructurePublicationCursorError(
                    "structure-publication-cursor-mismatch"
                )
            if next_cursor == expected_prior_cursor:
                raise StructurePublicationCursorError(
                    "structure-publication-cursor-not-advanced"
                )
            if previous_component != component and int(counts[component]) > 0:
                raise StructurePublicationCursorError(
                    "structure-publication-component-already-advanced"
                )
            start_issue_index = int(counts[component]) + 1
            for offset, item in enumerate(materialized):
                columns, values = self._component_values(
                    component, item, snapshot_id, start_issue_index + offset
                )
                placeholders = ",".join("?" for _ in columns)
                con.execute(
                    f"INSERT INTO {table}({','.join(columns)}) "
                    f"VALUES ({placeholders})",  # noqa: S608 - internal schema
                    values,
                )
            counts[component] = int(counts[component]) + len(materialized)
            con.execute(
                "UPDATE structure_publications SET write_component=?,write_prior_cursor=?,"
                "write_row_cursor=?,normalization_component=?,"
                "normalization_source_cursor=?,"
                "committed_counts_json=?,checkpoint_at_ms=? WHERE publication_id=?",
                (
                    component,
                    expected_prior_cursor,
                    next_cursor,
                    component,
                    next_cursor,
                    json.dumps(counts, sort_keys=True, separators=(",", ":")),
                    now_ms,
                    publication_id,
                ),
            )
            con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def certify_structure_generation(
        self, publication_id: str, receipt: dict[str, object]
    ) -> None:
        """Certify only facts recomputed from generation and raw-window rows."""
        coverage = receipt.get("source_coverage")
        if not isinstance(coverage, dict) or coverage.get("completed") is not True:
            raise ValueError("source-coverage-incomplete")
        supplied_hash = receipt.get("validation_hash")
        certified_at_ms = receipt.get("certified_at_ms")
        if (
            not isinstance(supplied_hash, str)
            or len(supplied_hash) != 64
            or type(certified_at_ms) is not int
            or certified_at_ms < 0
        ):
            raise ValueError("invalid-structure-certification-receipt")
        con = self._connect_writer()
        try:
            con.execute("BEGIN IMMEDIATE")
            publication = con.execute(
                "SELECT snapshot_id,window_id,status,expected_counts_json "
                "FROM structure_publications WHERE publication_id=?",
                (publication_id,),
            ).fetchone()
            if publication is None or publication[2] != "writing":
                raise ValueError("structure-publication-not-writing")
            snapshot_id, window_id = int(publication[0]), str(publication[1])
            expected = json.loads(str(publication[3]))
            actual = self._generation_counts(con, snapshot_id)
            if actual != expected:
                raise ValueError("generation-incomplete")
            source = con.execute(
                "SELECT status,(SELECT COUNT(*) FROM structure_sync_event_staging "
                "WHERE window_id=w.id),(SELECT COUNT(*) FROM structure_sync_market_staging "
                "WHERE window_id=w.id) FROM structure_sync_windows w WHERE id=?",
                (window_id,),
            ).fetchone()
            if (
                source is None
                or source[0] != "complete"
                or int(source[1]) != actual["events"]
                or int(source[2]) != actual["markets"]
                or coverage.get("event_items") != int(source[1])
                or coverage.get("market_items") != int(source[2])
            ):
                raise ValueError("source-coverage-incomplete")
            invalid_truth = con.execute(
                "SELECT 1 FROM structure_generation_group_truth t WHERE t.snapshot_id=? "
                "AND (t.quality='incomplete-source' OR t.expected_member_count != "
                "(SELECT COUNT(*) FROM structure_generation_memberships m "
                "WHERE m.snapshot_id=t.snapshot_id AND m.event_id=t.event_id "
                "AND m.neg_risk_market_id=t.neg_risk_market_id) OR "
                "t.active_named_count != (SELECT COUNT(*) FROM "
                "structure_generation_memberships m WHERE m.snapshot_id=t.snapshot_id "
                "AND m.event_id=t.event_id AND "
                "m.neg_risk_market_id=t.neg_risk_market_id AND m.member_kind='named' "
                "AND m.active=1)) LIMIT 1",
                (snapshot_id,),
            ).fetchone()
            orphan = con.execute(
                "SELECT 1 FROM structure_generation_memberships m "
                "WHERE m.snapshot_id=? AND (NOT EXISTS (SELECT 1 FROM "
                "structure_generation_group_truth t WHERE t.snapshot_id=m.snapshot_id "
                "AND t.event_id=m.event_id AND "
                "t.neg_risk_market_id=m.neg_risk_market_id) OR NOT EXISTS (SELECT 1 "
                "FROM structure_generation_events e WHERE e.snapshot_id=m.snapshot_id "
                "AND e.id=m.event_id) OR NOT EXISTS (SELECT 1 FROM "
                "structure_generation_markets k WHERE k.snapshot_id=m.snapshot_id "
                "AND k.market_id=m.market_id)) LIMIT 1",
                (snapshot_id,),
            ).fetchone()
            hash_invalid = False
            truths = con.execute(
                "SELECT event_id,neg_risk_market_id,membership_hash FROM "
                "structure_generation_group_truth WHERE snapshot_id=? "
                "ORDER BY event_id,neg_risk_market_id",
                (snapshot_id,),
            ).fetchall()
            for event_id, group_id, stored_hash in truths:
                member_rows = con.execute(
                    "SELECT market_id,member_kind,active,closed FROM "
                    "structure_generation_memberships WHERE snapshot_id=? "
                    "AND event_id=? AND neg_risk_market_id=? ORDER BY market_id",
                    (snapshot_id, event_id, group_id),
                ).fetchall()
                durable_members = [
                    EventMember(
                        str(event_id),
                        str(group_id),
                        str(market_id),
                        member_kind,
                        bool(active),
                        bool(closed),
                    )
                    for market_id, member_kind, active, closed in member_rows
                ]
                if membership_hash(str(event_id), str(group_id), durable_members) != stored_hash:
                    hash_invalid = True
                    break
            if invalid_truth is not None or orphan is not None or hash_invalid:
                raise ValueError("membership-invalid")
            validation_hash = self._generation_hash(con, snapshot_id)
            counts_json = json.dumps(actual, sort_keys=True, separators=(",", ":"))
            con.execute(
                "UPDATE structure_publications SET status='ready',"
                "committed_counts_json=?,validation_hash=?,"
                "certification_component='bounded-complete',certification_hash=?,"
                "certification_counts_json=?,certified_at_ms=?,"
                "checkpoint_at_ms=? WHERE publication_id=?",
                (
                    counts_json,
                    validation_hash,
                    validation_hash,
                    counts_json,
                    certified_at_ms,
                    certified_at_ms,
                    publication_id,
                ),
            )
            con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def publish_structure_generation(self, publication_id: str, now_ms: int) -> int:
        """Atomically publish metadata and switch the singleton generation pointer."""
        if not publication_id or now_ms < 0:
            raise ValueError("invalid-structure-publication")
        con = self._connect_writer()
        try:
            con.execute("BEGIN IMMEDIATE")
            publication = con.execute(
                "SELECT snapshot_id,window_id,status,expected_counts_json,"
                "committed_counts_json,validation_hash,certification_component,"
                "certification_hash "
                "FROM structure_publications "
                "WHERE publication_id=?",
                (publication_id,),
            ).fetchone()
            if publication is None or publication[2] != "ready":
                raise ValueError("structure-publication-not-ready")
            snapshot_id, window_id = int(publication[0]), str(publication[1])
            expected = json.loads(str(publication[3]))
            committed = json.loads(str(publication[4]))
            authenticated_frozen_receipt = (
                publication[6] == "backfill-authenticated"
                if window_id.startswith("backfill:")
                else publication[6] == "bounded-complete"
            )
            if not authenticated_frozen_receipt:
                raise ValueError("structure-publication-receipt-missing")
            actual = committed
            if actual != expected:
                raise ValueError("structure-publication-count-mismatch")
            if publication[5] != publication[7]:
                raise ValueError("structure-publication-hash-mismatch")
            counts_json = json.dumps(actual, sort_keys=True, separators=(",", ":"))
            con.execute(
                "UPDATE snapshots SET finished_at_ms=?,market_count=?,"
                "market_view_published=1,is_valid=1,snapshot_status='ok' WHERE id=?",
                (now_ms, actual["markets"], snapshot_id),
            )
            con.execute(
                "INSERT INTO current_structure_generation(id,snapshot_id,publication_id,"
                "validation_hash,counts_json,certification_component,switched_at_ms) "
                "VALUES (1,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET "
                "snapshot_id=excluded.snapshot_id,publication_id=excluded.publication_id,"
                "validation_hash=excluded.validation_hash,"
                "counts_json=excluded.counts_json,"
                "certification_component=excluded.certification_component,"
                "switched_at_ms=excluded.switched_at_ms",
                (
                    snapshot_id,
                    publication_id,
                    publication[5],
                    counts_json,
                    publication[6],
                    now_ms,
                ),
            )
            con.execute(
                "UPDATE structure_publications SET status='published',published_at_ms=?,"
                "checkpoint_at_ms=? WHERE publication_id=?",
                (now_ms, now_ms, publication_id),
            )
            window_update = con.execute(
                "UPDATE structure_sync_windows SET status='published',"
                "published_snapshot_id=?,checkpoint_at_ms=? WHERE id=? AND status='complete'",
                (snapshot_id, now_ms, window_id),
            )
            if window_update.rowcount != 1:
                raise ValueError("structure-sync-window-not-complete")
            con.execute("COMMIT")
            return snapshot_id
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def current_structure_generation(self) -> dict[str, object] | None:
        con = sqlite3.connect(self._db_path)
        try:
            con.execute("BEGIN")
            row = con.execute(
                "SELECT snapshot_id,publication_id,switched_at_ms "
                "FROM current_structure_generation WHERE id=1"
            ).fetchone()
            con.execute("COMMIT")
            if row is None:
                return None
            return {
                "snapshot_id": int(row[0]),
                "publication_id": str(row[1]),
                "switched_at_ms": int(row[2]),
            }
        finally:
            con.close()

    def current_generation_market_ids(self) -> tuple[str, ...]:
        """Resolve pointer and generation rows inside one read transaction."""
        con = sqlite3.connect(self._db_path)
        try:
            con.execute("BEGIN")
            pointer = con.execute(
                "SELECT snapshot_id FROM current_structure_generation WHERE id=1"
            ).fetchone()
            rows = [] if pointer is None else con.execute(
                "SELECT market_id FROM structure_generation_markets "
                "WHERE snapshot_id=? ORDER BY market_id",
                (int(pointer[0]),),
            ).fetchall()
            con.execute("COMMIT")
            return tuple(str(row[0]) for row in rows)
        finally:
            con.close()

    def _freeze_backfill_generation(self, publication_id: str) -> None:
        """Durably freeze a complete backfill before any receipt hash scan."""
        con = self._connect_writer()
        try:
            con.execute("BEGIN IMMEDIATE")
            cur = con.execute(
                "UPDATE structure_publications SET "
                "certification_component='backfill-frozen' "
                "WHERE publication_id=? AND status='writing' "
                "AND certification_component IS NULL "
                "AND expected_counts_json=committed_counts_json",
                (publication_id,),
            )
            if cur.rowcount != 1:
                marker = con.execute(
                    "SELECT status,certification_component FROM structure_publications "
                    "WHERE publication_id=?",
                    (publication_id,),
                ).fetchone()
                if marker != ("writing", "backfill-frozen"):
                    raise ValueError("structure-backfill-freeze-race")
            con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def _ensure_backfill_comparison_receipt(
        self,
        publication_id: str,
        snapshot_id: int,
        created_at_ms: int,
    ) -> None:
        """Build the one-time dual-read receipt outside every hot read path."""
        with sqlite3.connect(self._db_path) as read_con:
            # Pin publication metadata and both streamed universes to one
            # read snapshot before publishing the durable comparison receipt.
            read_con.execute("BEGIN")
            publication = read_con.execute(
                "SELECT committed_counts_json,validation_hash,status,"
                "certification_component FROM structure_publications "
                "WHERE publication_id=? AND snapshot_id=?",
                (publication_id, snapshot_id),
            ).fetchone()
            if (
                publication is None
                or publication[2] != "ready"
                or publication[3] != "backfill-authenticated"
                or not isinstance(publication[1], str)
            ):
                raise ValueError("structure-backfill-receipt-not-authenticated")
            counts = json.loads(str(publication[0]))
            legacy_universe_hash, legacy_source_truth_hash = _structure_universe_hash(
                read_con,
                snapshot_id=snapshot_id,
                generation=False,
            )
            generation_universe_hash, generation_source_truth_hash = (
                _structure_universe_hash(
                    read_con,
                    snapshot_id=snapshot_id,
                    generation=True,
                )
            )
            receipt = (
                snapshot_id,
                publication_id,
                snapshot_id,
                int(counts["markets"]),
                int(counts["markets"]),
                legacy_universe_hash,
                generation_universe_hash,
                legacy_source_truth_hash,
                generation_source_truth_hash,
                str(publication[1]),
                created_at_ms,
            )
        con = self._connect_writer()
        try:
            con.execute("BEGIN IMMEDIATE")
            con.execute(
                "INSERT OR IGNORE INTO structure_generation_comparison_receipts("
                "generation_snapshot_id,publication_id,legacy_snapshot_id,"
                "legacy_market_count,generation_market_count,legacy_universe_hash,"
                "generation_universe_hash,legacy_source_truth_hash,"
                "generation_source_truth_hash,generation_validation_hash,created_at_ms) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                receipt,
            )
            durable = con.execute(
                "SELECT generation_snapshot_id,publication_id,legacy_snapshot_id,"
                "legacy_market_count,generation_market_count,legacy_universe_hash,"
                "generation_universe_hash,legacy_source_truth_hash,"
                "generation_source_truth_hash,generation_validation_hash,created_at_ms "
                "FROM structure_generation_comparison_receipts "
                "WHERE generation_snapshot_id=?",
                (snapshot_id,),
            ).fetchone()
            if durable != receipt:
                raise ValueError("structure-backfill-comparison-receipt-mismatch")
            con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def backfill_current_structure_generation(
        self, max_rows: int
    ) -> BackfillCheckpoint:
        """Migrate at most ``max_rows`` legacy rows, resuming component/cursor."""
        if max_rows < 1:
            raise ValueError("max_rows must be positive")
        con = self._connect_writer()
        publish_id: str | None = None
        needs_certification = False
        copied_rows = 0
        cursor: str | None = None
        snapshot_id: int | None = None
        try:
            con.execute("BEGIN IMMEDIATE")
            pointer = con.execute(
                "SELECT snapshot_id FROM current_structure_generation WHERE id=1"
            ).fetchone()
            if pointer is not None:
                con.execute("COMMIT")
                return BackfillCheckpoint(int(pointer[0]), 0, None, True)
            publication = con.execute(
                "SELECT publication_id,snapshot_id,window_id,status,write_component,"
                "write_row_cursor,expected_counts_json,certification_component "
                "FROM structure_publications "
                "WHERE status IN ('writing','ready') AND window_id LIKE 'backfill:%' "
                "ORDER BY created_at_ms DESC LIMIT 1"
            ).fetchone()
            if publication is None:
                source = con.execute(
                    "SELECT id FROM snapshots WHERE data_product='structure' "
                    "AND market_view_published=1 AND is_valid=1 "
                    "AND snapshot_status!='failed' ORDER BY id DESC LIMIT 1"
                ).fetchone()
                if source is None:
                    con.execute("COMMIT")
                    return BackfillCheckpoint(None, 0, None, True)
                snapshot_id = int(source[0])
                window_id = f"backfill:{snapshot_id}"
                publication_id = f"backfill:{snapshot_id}"
                expected = {
                    "events": int(con.execute(
                        "SELECT COUNT(*) FROM events WHERE snapshot_id=?",
                        (snapshot_id,),
                    ).fetchone()[0]),
                    "event_tags": int(con.execute(
                        "SELECT COUNT(*) FROM event_tags WHERE snapshot_id=?",
                        (snapshot_id,),
                    ).fetchone()[0]),
                    "memberships": int(con.execute(
                        "SELECT COUNT(*) FROM event_market_memberships WHERE snapshot_id=?",
                        (snapshot_id,),
                    ).fetchone()[0]),
                    "group_truth": int(con.execute(
                        "SELECT COUNT(*) FROM neg_risk_group_truth WHERE snapshot_id=?",
                        (snapshot_id,),
                    ).fetchone()[0]),
                    "markets": int(con.execute(
                        "SELECT COUNT(*) FROM markets WHERE snapshot_id=?",
                        (snapshot_id,),
                    ).fetchone()[0]),
                    "issues": int(con.execute(
                        "SELECT COUNT(*) FROM validation_issues WHERE snapshot_id=?",
                        (snapshot_id,),
                    ).fetchone()[0]),
                }
                con.execute(
                    "INSERT INTO structure_sync_windows(id,status,started_at_ms,"
                    "checkpoint_at_ms) SELECT ?,'complete',taken_at_ms,finished_at_ms "
                    "FROM snapshots WHERE id=?",
                    (window_id, snapshot_id),
                )
                zero_counts = {component: 0 for component in _STRUCTURE_COMPONENTS}
                con.execute(
                    "INSERT INTO structure_publications(publication_id,window_id,"
                    "snapshot_id,status,write_component,expected_counts_json,"
                    "committed_counts_json,created_at_ms,checkpoint_at_ms) "
                    "SELECT ?,?,?,'writing','events',?,?,taken_at_ms,finished_at_ms "
                    "FROM snapshots WHERE id=?",
                    (
                        publication_id,
                        window_id,
                        snapshot_id,
                        json.dumps(expected, sort_keys=True, separators=(",", ":")),
                        json.dumps(zero_counts, sort_keys=True, separators=(",", ":")),
                        snapshot_id,
                    ),
                )
                publication = (
                    publication_id,
                    snapshot_id,
                    window_id,
                    "writing",
                    "events",
                    None,
                    json.dumps(expected, sort_keys=True, separators=(",", ":")),
                    None,
                )
            publication_id = str(publication[0])
            snapshot_id = int(publication[1])
            status = str(publication[3])
            component = str(publication[4])
            cursor = None if publication[5] is None else str(publication[5])
            expected = json.loads(str(publication[6]))
            certification_component = (
                None if publication[7] is None else str(publication[7])
            )
            if status == "ready":
                if certification_component not in {
                    "bounded-complete",
                    "backfill-authenticated",
                }:
                    raise ValueError("structure-backfill-receipt-missing")
                publish_id = publication_id
                con.execute("COMMIT")
            elif certification_component == "backfill-frozen":
                publish_id = publication_id
                needs_certification = True
                con.execute("COMMIT")
            else:
                market_columns = tuple(
                    column for column in MARKETS_COLUMN_ORDER if column != "snapshot_id"
                )
                specs = {
                    "events": (
                        "events",
                        (
                            "snapshot_id", "id", "slug", "title", "ticker", "active",
                            "closed", "liquidity_usd", "volume_usd", "end_time_ms",
                            "fetched_at_ms", "page_fetched_at_ms",
                        ),
                        ("id",),
                    ),
                    "event_tags": (
                        "event_tags",
                        ("snapshot_id", "event_id", "tag_id", "tag_label", "tag_slug"),
                        ("event_id", "tag_id"),
                    ),
                    "memberships": (
                        "event_market_memberships",
                        (
                            "snapshot_id", "event_id", "neg_risk_market_id", "market_id",
                            "member_kind", "active", "closed",
                        ),
                        ("event_id", "neg_risk_market_id", "market_id"),
                    ),
                    "group_truth": (
                        "neg_risk_group_truth",
                        (
                            "snapshot_id", "event_id", "neg_risk_market_id",
                            "neg_risk_type", "expected_member_count", "active_named_count",
                            "membership_hash", "quality", "reason",
                        ),
                        ("event_id", "neg_risk_market_id"),
                    ),
                    "markets": (
                        "markets",
                        ("snapshot_id", *market_columns),
                        ("market_id",),
                    ),
                    "issues": (
                        "validation_issues",
                        (
                            "snapshot_id", "id", "layer", "category", "market_id",
                            "detail", "raw_payload",
                        ),
                        ("id",),
                    ),
                }
                component_index = _STRUCTURE_COMPONENTS.index(component)
                while copied_rows == 0 and component_index < len(_STRUCTURE_COMPONENTS):
                    component = _STRUCTURE_COMPONENTS[component_index]
                    source_table, columns, key_columns = specs[component]
                    cursor_values = None if cursor is None else json.loads(cursor)
                    cursor_clause = ""
                    parameters: list[object] = [snapshot_id]
                    if cursor_values is not None:
                        tuple_columns = ",".join(key_columns)
                        placeholders = ",".join("?" for _ in key_columns)
                        cursor_clause = f" AND ({tuple_columns}) > ({placeholders})"
                        parameters.extend(cursor_values)
                    parameters.append(max_rows)
                    selected = con.execute(
                        f"SELECT {','.join(columns)} FROM {source_table} "
                        f"WHERE snapshot_id=?{cursor_clause} "
                        f"ORDER BY {','.join(key_columns)} LIMIT ?",  # noqa: S608
                        parameters,
                    ).fetchall()
                    if selected:
                        destination = self._structure_component_table(component)
                        destination_columns = (
                            (
                                "snapshot_id", "issue_index", "layer", "category",
                                "market_id", "detail", "raw_payload",
                            )
                            if component == "issues"
                            else columns
                        )
                        placeholders = ",".join("?" for _ in columns)
                        con.executemany(
                            f"INSERT OR IGNORE INTO {destination}"
                            f"({','.join(destination_columns)}) "
                            f"VALUES ({placeholders})",  # noqa: S608
                            selected,
                        )
                        copied_rows = len(selected)
                        column_indexes = {
                            name: index for index, name in enumerate(columns)
                        }
                        next_values = [
                            selected[-1][column_indexes[name]] for name in key_columns
                        ]
                        cursor = json.dumps(next_values, separators=(",", ":"))
                        more = con.execute(
                            f"SELECT 1 FROM {source_table} WHERE snapshot_id=? AND "
                            f"({','.join(key_columns)}) > "
                            f"({','.join('?' for _ in key_columns)}) LIMIT 1",  # noqa: S608
                            (snapshot_id, *next_values),
                        ).fetchone()
                        if more is None and component_index + 1 < len(_STRUCTURE_COMPONENTS):
                            component_index += 1
                            component = _STRUCTURE_COMPONENTS[component_index]
                            cursor = None
                    elif component_index + 1 < len(_STRUCTURE_COMPONENTS):
                        component_index += 1
                        component = _STRUCTURE_COMPONENTS[component_index]
                        cursor = None
                    else:
                        break

                actual = self._generation_counts(con, snapshot_id)
                counts_json = json.dumps(actual, sort_keys=True, separators=(",", ":"))
                con.execute(
                    "UPDATE structure_publications SET write_component=?,"
                    "write_prior_cursor=write_row_cursor,write_row_cursor=?,"
                    "committed_counts_json=? WHERE publication_id=?",
                    (component, cursor, counts_json, publication_id),
                )
                complete = actual == expected
                if complete:
                    publish_id = publication_id
                    needs_certification = True
                con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()
        if publish_id is not None:
            assert snapshot_id is not None
            if needs_certification:
                self._freeze_backfill_generation(publish_id)
            with sqlite3.connect(self._db_path) as read_con:
                finished_at_ms = int(
                    read_con.execute(
                        "SELECT finished_at_ms FROM snapshots WHERE id=?", (snapshot_id,)
                    ).fetchone()[0]
                )
                if needs_certification:
                    source_hash = self._legacy_generation_hash(read_con, snapshot_id)
                    destination_hash = self._generation_hash(read_con, snapshot_id)
                    if source_hash != destination_hash:
                        raise ValueError("structure-backfill-hash-mismatch")
            if needs_certification:
                certify_con = self._connect_writer()
                try:
                    certify_con.execute("BEGIN IMMEDIATE")
                    cur = certify_con.execute(
                        "UPDATE structure_publications SET status='ready',"
                        "validation_hash=?,"
                        "certification_component='backfill-authenticated',"
                        "certification_hash=?,"
                        "certified_at_ms=?,checkpoint_at_ms=? "
                        "WHERE publication_id=? AND status='writing' "
                        "AND certification_component='backfill-frozen' "
                        "AND expected_counts_json=committed_counts_json",
                        (
                            destination_hash,
                            destination_hash,
                            finished_at_ms,
                            finished_at_ms,
                            publish_id,
                        ),
                    )
                    if cur.rowcount != 1:
                        raise ValueError("structure-backfill-certification-race")
                    certify_con.execute("COMMIT")
                except BaseException:
                    if certify_con.in_transaction:
                        certify_con.execute("ROLLBACK")
                    raise
                finally:
                    certify_con.close()
            self._ensure_backfill_comparison_receipt(
                publish_id,
                snapshot_id,
                finished_at_ms,
            )
            self.publish_structure_generation(publish_id, finished_at_ms)
            return BackfillCheckpoint(snapshot_id, copied_rows, cursor, True)
        return BackfillCheckpoint(snapshot_id, copied_rows, cursor, False)

    def begin_or_resume_structure_sync(self, *, started_at_ms: int) -> dict[str, object]:
        """Return the sole resumable Structure window, creating it atomically."""
        if started_at_ms < 0:
            raise ValueError("invalid-structure-sync-start")
        con = self._connect_writer()
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                "SELECT id,status,event_cursor,market_cursor,started_at_ms,"
                "checkpoint_at_ms,event_pages,market_pages,failure_reason,published_snapshot_id "
                "FROM structure_sync_windows WHERE status IN ('open','events_complete') "
                "ORDER BY started_at_ms LIMIT 1"
            ).fetchone()
            if row is None:
                window_id = uuid.uuid4().hex
                con.execute(
                    "INSERT INTO structure_sync_windows("
                    "id,status,started_at_ms,checkpoint_at_ms) VALUES (?,'open',?,?)",
                    (window_id, started_at_ms, started_at_ms),
                )
                row = con.execute(
                    "SELECT id,status,event_cursor,market_cursor,started_at_ms,"
                    "checkpoint_at_ms,event_pages,market_pages,failure_reason,"
                    "published_snapshot_id "
                    "FROM structure_sync_windows WHERE id=?",
                    (window_id,),
                ).fetchone()
            con.execute("COMMIT")
            assert row is not None
            return self._structure_sync_window_row(row)
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def get_latest_structure_sync(self) -> dict[str, object] | None:
        """Read latest window progress without starting another collection window."""
        con = sqlite3.connect(self._db_path)
        try:
            row = con.execute(
                "SELECT id,status,event_cursor,market_cursor,started_at_ms,"
                "checkpoint_at_ms,event_pages,market_pages,failure_reason,published_snapshot_id "
                "FROM structure_sync_windows ORDER BY checkpoint_at_ms DESC LIMIT 1"
            ).fetchone()
            return None if row is None else self._structure_sync_window_row(row)
        finally:
            con.close()

    def restart_structure_sync_window(
        self,
        *,
        window_id: str,
        restarted_at_ms: int,
        failure_reason: str,
    ) -> dict[str, object]:
        """Fail one rejected-cursor window and atomically open its successor."""
        if not window_id or restarted_at_ms < 0:
            raise ValueError("invalid-structure-sync-restart")
        if not failure_reason or len(failure_reason) > 200:
            raise ValueError("invalid-structure-sync-failure-reason")
        successor_id = uuid.uuid4().hex
        con = self._connect_writer()
        try:
            con.execute("BEGIN IMMEDIATE")
            cur = con.execute(
                "UPDATE structure_sync_windows SET status='failed',"
                "failure_reason=?,checkpoint_at_ms=? "
                "WHERE id=? AND status IN ('open','events_complete')",
                (failure_reason, restarted_at_ms, window_id),
            )
            if cur.rowcount != 1:
                raise ValueError("structure-sync-window-not-restartable")
            con.execute(
                "INSERT INTO structure_sync_windows("
                "id,status,started_at_ms,checkpoint_at_ms"
                ") VALUES (?,'open',?,?)",
                (successor_id, restarted_at_ms, restarted_at_ms + 1),
            )
            row = con.execute(
                "SELECT id,status,event_cursor,market_cursor,started_at_ms,"
                "checkpoint_at_ms,event_pages,market_pages,failure_reason,"
                "published_snapshot_id FROM structure_sync_windows WHERE id=?",
                (successor_id,),
            ).fetchone()
            con.execute("COMMIT")
            return self._structure_sync_window_row(row)
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def mark_structure_sync_published(
        self, *, window_id: str, snapshot_id: int, published_at_ms: int
    ) -> None:
        """Bind a complete window to its certified snapshot exactly once."""
        con = self._connect_writer()
        try:
            cur = con.execute(
                "UPDATE structure_sync_windows SET status='published',"
                "published_snapshot_id=?,checkpoint_at_ms=? "
                "WHERE id=? AND status='complete'",
                (snapshot_id, published_at_ms, window_id),
            )
            if cur.rowcount != 1:
                raise ValueError("structure-sync-window-not-complete")
        finally:
            con.close()

    def purge_published_structure_sync_windows(
        self,
        *,
        keep_last: int = 1,
        max_windows_per_run: int = 1,
    ) -> tuple[int, list[str]]:
        """Delete a bounded batch of raw staging already bound to snapshots."""
        if keep_last < 1:
            raise ValueError("keep_last must be positive")
        if max_windows_per_run < 1:
            raise ValueError("max_windows_per_run must be positive")

        con = self._connect_writer()
        try:
            con.execute("BEGIN IMMEDIATE")
            keep_ids = [
                str(row[0])
                for row in con.execute(
                    "SELECT id FROM structure_sync_windows "
                    "WHERE status='published' "
                    "ORDER BY checkpoint_at_ms DESC,id DESC LIMIT ?",
                    (keep_last,),
                )
            ]
            placeholders = ",".join("?" for _ in keep_ids)
            to_delete = [
                str(row[0])
                for row in con.execute(
                    "SELECT id FROM structure_sync_windows "
                    "WHERE status='published' "
                    f"AND id NOT IN ({placeholders}) "
                    "ORDER BY checkpoint_at_ms,id LIMIT ?",
                    (*keep_ids, max_windows_per_run),
                )
            ]
            if to_delete:
                delete_placeholders = ",".join("?" for _ in to_delete)
                con.execute(
                    "DELETE FROM structure_sync_event_market_staging "
                    f"WHERE window_id IN ({delete_placeholders})",
                    to_delete,
                )
                con.execute(
                    "DELETE FROM structure_sync_event_staging "
                    f"WHERE window_id IN ({delete_placeholders})",
                    to_delete,
                )
                con.execute(
                    "DELETE FROM structure_sync_market_staging "
                    f"WHERE window_id IN ({delete_placeholders})",
                    to_delete,
                )
                con.execute(
                    "DELETE FROM structure_sync_windows "
                    f"WHERE id IN ({delete_placeholders}) AND status='published'",
                    to_delete,
                )
            con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

        if to_delete:
            logger.info(
                "structure staging retention deleted "
                f"{len(to_delete)} published windows ids={to_delete}"
            )
        return len(to_delete), to_delete

    def purge_failed_structure_sync_windows(
        self,
        *,
        max_windows_per_run: int = 1,
    ) -> tuple[int, list[str]]:
        """Reclaim staging from failed windows after fresh truth is certified."""
        if max_windows_per_run < 1:
            raise ValueError("max_windows_per_run must be positive")
        con = self._connect_writer()
        try:
            con.execute("BEGIN IMMEDIATE")
            to_delete = [
                str(row[0])
                for row in con.execute(
                    "SELECT id FROM structure_sync_windows WHERE status='failed' "
                    "ORDER BY checkpoint_at_ms,id LIMIT ?",
                    (max_windows_per_run,),
                )
            ]
            if to_delete:
                placeholders = ",".join("?" for _ in to_delete)
                con.execute(
                    "DELETE FROM structure_sync_event_market_staging "
                    f"WHERE window_id IN ({placeholders})",
                    to_delete,
                )
                con.execute(
                    "DELETE FROM structure_sync_event_staging "
                    f"WHERE window_id IN ({placeholders})",
                    to_delete,
                )
                con.execute(
                    "DELETE FROM structure_sync_market_staging "
                    f"WHERE window_id IN ({placeholders})",
                    to_delete,
                )
                con.execute(
                    "DELETE FROM structure_sync_windows "
                    f"WHERE id IN ({placeholders}) AND status='failed'",
                    to_delete,
                )
            con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()
        return len(to_delete), to_delete

    def read_complete_structure_sync(self, window_id: object) -> tuple[list[dict], list[dict]]:
        """Return staged facts only after both source traversals completed."""
        if not isinstance(window_id, str) or not window_id:
            raise ValueError("invalid-structure-sync-window")
        con = sqlite3.connect(self._db_path)
        try:
            row = con.execute(
                "SELECT status FROM structure_sync_windows WHERE id=?", (window_id,)
            ).fetchone()
            if row is None or row[0] != "complete":
                raise ValueError("structure-sync-window-not-complete")
            events = [
                json.loads(str(item[0]))
                for item in con.execute(
                    "SELECT payload_json FROM structure_sync_event_staging "
                    "WHERE window_id=? ORDER BY event_id", (window_id,)
                ).fetchall()
            ]
            markets = [
                json.loads(str(item[0]))
                for item in con.execute(
                    "SELECT payload_json FROM structure_sync_market_staging "
                    "WHERE window_id=? ORDER BY market_id", (window_id,)
                ).fetchall()
            ]
            return events, markets
        finally:
            con.close()

    def get_complete_structure_sync_counts(
        self, window_id: object
    ) -> tuple[int, int]:
        """Validate a completed window and return its staged row counts."""
        if not isinstance(window_id, str) or not window_id:
            raise ValueError("invalid-structure-sync-window")
        con = sqlite3.connect(self._db_path)
        try:
            row = con.execute(
                "SELECT status FROM structure_sync_windows WHERE id=?", (window_id,)
            ).fetchone()
            if row is None or row[0] != "complete":
                raise ValueError("structure-sync-window-not-complete")
            event_count = int(
                con.execute(
                    "SELECT COUNT(*) FROM structure_sync_event_staging "
                    "WHERE window_id=?",
                    (window_id,),
                ).fetchone()[0]
            )
            market_count = int(
                con.execute(
                    "SELECT COUNT(*) FROM structure_sync_market_staging "
                    "WHERE window_id=?",
                    (window_id,),
                ).fetchone()[0]
            )
            return event_count, market_count
        finally:
            con.close()

    def iter_complete_structure_events(self, window_id: str):
        """Stream completed event payloads without materializing the window."""
        yield from self._iter_complete_structure_payloads(
            window_id,
            table="structure_sync_event_staging",
            id_column="event_id",
        )

    def iter_complete_structure_markets(self, window_id: str):
        """Stream completed market payloads without materializing the window."""
        yield from self._iter_complete_structure_payloads(
            window_id,
            table="structure_sync_market_staging",
            id_column="market_id",
        )

    def _iter_complete_structure_payloads(
        self,
        window_id: object,
        *,
        table: str,
        id_column: str,
    ):
        if not isinstance(window_id, str) or not window_id:
            raise ValueError("invalid-structure-sync-window")
        con = sqlite3.connect(self._db_path)
        try:
            row = con.execute(
                "SELECT status FROM structure_sync_windows WHERE id=?", (window_id,)
            ).fetchone()
            if row is None or row[0] != "complete":
                raise ValueError("structure-sync-window-not-complete")
            cursor = con.execute(
                f"SELECT payload_json FROM {table} "  # noqa: S608 - internal constants
                f"WHERE window_id=? ORDER BY {id_column}",
                (window_id,),
            )
            for payload_row in cursor:
                yield json.loads(str(payload_row[0]))
        finally:
            con.close()

    @staticmethod
    def _structure_sync_window_row(row: tuple[object, ...]) -> dict[str, object]:
        keys = (
            "id", "status", "event_cursor", "market_cursor", "started_at_ms",
            "checkpoint_at_ms", "event_pages", "market_pages", "failure_reason",
            "published_snapshot_id",
        )
        return dict(zip(keys, row, strict=True))

    def commit_structure_event_page(
        self,
        *,
        window_id: object,
        requested_cursor: str | None,
        next_cursor: str | None,
        completed: bool,
        events: list[dict],
        finished_at_ms: int,
    ) -> None:
        """Stage one validated event page and advance its opaque cursor together."""
        if not isinstance(window_id, str) or not window_id:
            raise ValueError("invalid-structure-sync-window")
        if finished_at_ms < 0 or completed != (next_cursor is None):
            raise ValueError("invalid-structure-event-page")
        serialized: list[tuple[str, str, str | None]] = []
        for event in events:
            event_id = event.get("id") if isinstance(event, dict) else None
            if not isinstance(event_id, str) or not event_id:
                raise ValueError("invalid-structure-event")
            serialized.append((event_id, json.dumps(event, sort_keys=True), requested_cursor))
        con = self._connect_writer()
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                "SELECT status,event_cursor FROM structure_sync_windows WHERE id=?",
                (window_id,),
            ).fetchone()
            if row is None or row[0] != "open" or row[1] != requested_cursor:
                raise ValueError("structure-event-page-cursor-mismatch")
            ordinal_base = int(
                con.execute(
                    "SELECT COALESCE(MAX(source_ordinal),0) FROM "
                    "structure_sync_event_staging WHERE window_id=?",
                    (window_id,),
                ).fetchone()[0]
            )
            ordered = [
                (*item, ordinal_base + index)
                for index, item in enumerate(serialized, start=1)
            ]
            con.executemany(
                "INSERT INTO structure_sync_event_staging("
                "window_id,event_id,payload_json,source_cursor,source_ordinal) "
                "VALUES (?,?,?,?,?) ON CONFLICT(window_id,event_id) DO UPDATE SET "
                "payload_json=excluded.payload_json,source_cursor=excluded.source_cursor",
                [(window_id, *item) for item in ordered],
            )
            parent_rows: list[tuple[object, ...]] = []
            for index, event in enumerate(events, start=1):
                event_id = str(event["id"])
                members = event.get("markets")
                if not isinstance(members, list):
                    continue
                for member in members:
                    market_id = member.get("id") if isinstance(member, dict) else None
                    if isinstance(market_id, str) and market_id:
                        parent_rows.append(
                            (window_id, market_id, event_id, ordinal_base + index)
                        )
            con.executemany(
                "INSERT OR IGNORE INTO structure_sync_event_market_staging("
                "window_id,market_id,event_id,source_ordinal) VALUES (?,?,?,?)",
                parent_rows,
            )
            con.execute(
                "UPDATE structure_sync_windows SET status=?,event_cursor=?,"
                "checkpoint_at_ms=?,event_pages=event_pages+1 WHERE id=?",
                (
                    "events_complete" if completed else "open",
                    next_cursor,
                    finished_at_ms,
                    window_id,
                ),
            )
            con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def list_staged_structure_events(self, window_id: object) -> list[dict]:
        """Read staging only for finalizer tests; it is never online truth."""
        if not isinstance(window_id, str) or not window_id:
            raise ValueError("invalid-structure-sync-window")
        con = sqlite3.connect(self._db_path)
        try:
            rows = con.execute(
                "SELECT payload_json FROM structure_sync_event_staging "
                "WHERE window_id=? ORDER BY event_id",
                (window_id,),
            ).fetchall()
            return [json.loads(str(row[0])) for row in rows]
        finally:
            con.close()

    def commit_structure_market_page(
        self,
        *,
        window_id: object,
        requested_cursor: str | None,
        next_cursor: str | None,
        completed: bool,
        markets: list[dict],
        finished_at_ms: int,
    ) -> None:
        """Stage one market page only after complete event coverage is durable."""
        if not isinstance(window_id, str) or not window_id:
            raise ValueError("invalid-structure-sync-window")
        if finished_at_ms < 0 or completed != (next_cursor is None):
            raise ValueError("invalid-structure-market-page")
        serialized: list[tuple[str, str, str | None]] = []
        for market in markets:
            market_id = market.get("id") if isinstance(market, dict) else None
            if not isinstance(market_id, str) or not market_id:
                raise ValueError("invalid-structure-market")
            serialized.append((market_id, json.dumps(market, sort_keys=True), requested_cursor))
        con = self._connect_writer()
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                "SELECT status,market_cursor FROM structure_sync_windows WHERE id=?",
                (window_id,),
            ).fetchone()
            if row is None or row[0] != "events_complete" or row[1] != requested_cursor:
                raise ValueError("structure-market-page-cursor-mismatch")
            ordinal_base = int(
                con.execute(
                    "SELECT COALESCE(MAX(source_ordinal),0) FROM "
                    "structure_sync_market_staging WHERE window_id=?",
                    (window_id,),
                ).fetchone()[0]
            )
            con.executemany(
                "INSERT INTO structure_sync_market_staging("
                "window_id,market_id,payload_json,source_cursor,source_ordinal) "
                "VALUES (?,?,?,?,?) ON CONFLICT(window_id,market_id) DO UPDATE SET "
                "payload_json=excluded.payload_json,source_cursor=excluded.source_cursor",
                [
                    (window_id, *item, ordinal_base + index)
                    for index, item in enumerate(serialized, start=1)
                ],
            )
            con.execute(
                "UPDATE structure_sync_windows SET status=?,market_cursor=?,"
                "checkpoint_at_ms=?,market_pages=market_pages+1 WHERE id=?",
                (
                    "complete" if completed else "events_complete",
                    next_cursor,
                    finished_at_ms,
                    window_id,
                ),
            )
            con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def begin_snapshot_attempt(self, *, started_at_ms: int) -> int:
        """Append one running scheduler attempt before spawning its child."""
        con = self._connect_writer()
        try:
            cur = con.execute(
                "INSERT INTO snapshot_attempts(started_at_ms,outcome) "
                "VALUES (?, 'running')",
                (started_at_ms,),
            )
            assert cur.lastrowid is not None
            return int(cur.lastrowid)
        finally:
            con.close()

    def record_structure_defer(
        self,
        reason: str,
        queued_at_ms: int,
        observed_at_ms: int,
    ) -> int:
        """Persist bounded Quote-priority admission evidence across restarts."""
        if (
            not reason
            or len(reason) > 64
            or queued_at_ms < 0
            or observed_at_ms < queued_at_ms
        ):
            raise ValueError("invalid-structure-defer")
        con = self._connect_writer()
        try:
            con.execute("BEGIN IMMEDIATE")
            cur = con.execute(
                "INSERT INTO structure_defer_receipts(reason,queued_at_ms,observed_at_ms) "
                "VALUES (?,?,?)",
                (reason, queued_at_ms, observed_at_ms),
            )
            assert cur.lastrowid is not None
            receipt_id = int(cur.lastrowid)
            con.execute(
                "DELETE FROM structure_defer_receipts WHERE id <= ("
                "SELECT COALESCE(MAX(id),0)-100 FROM structure_defer_receipts)"
            )
            con.execute("COMMIT")
            return receipt_id
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def get_latest_structure_defer(self) -> dict[str, object] | None:
        """Read the latest persisted admission receipt without mutating it."""
        try:
            with sqlite3.connect(f"file:{self._db_path}?mode=ro", uri=True) as con:
                row = con.execute(
                    "SELECT id,reason,queued_at_ms,observed_at_ms "
                    "FROM structure_defer_receipts ORDER BY id DESC LIMIT 1"
                ).fetchone()
        except sqlite3.OperationalError:
            return None
        if row is None:
            return None
        return dict(
            zip(
                ("id", "reason", "queued_at_ms", "observed_at_ms"),
                row,
                strict=True,
            )
        )

    def finish_snapshot_attempt(
        self,
        *,
        attempt_id: int,
        outcome: str,
        finished_at_ms: int,
        snapshot_id: int | None,
        failure_kind: str | None,
        last_stage: str | None = None,
        elapsed_ms: int | None = None,
    ) -> None:
        """Close one running attempt exactly once with a bounded outcome."""
        if outcome not in {"succeeded", "failed", "cancelled"}:
            raise ValueError(f"invalid terminal snapshot attempt outcome: {outcome}")
        con = self._connect_writer()
        try:
            cur = con.execute(
                "UPDATE snapshot_attempts "
                "SET finished_at_ms=?, outcome=?, snapshot_id=?, failure_kind=?, "
                "last_stage=?, elapsed_ms=? "
                "WHERE id=? AND outcome='running'",
                (
                    finished_at_ms,
                    outcome,
                    snapshot_id,
                    failure_kind,
                    last_stage,
                    elapsed_ms,
                    attempt_id,
                ),
            )
            if cur.rowcount != 1:
                raise ValueError(f"snapshot attempt {attempt_id} is not running")
        finally:
            con.close()

    def get_latest_snapshot_attempt(self) -> dict[str, object] | None:
        """Read one newest scheduler attempt without mutating operational truth."""
        uri = f"file:{self._db_path}?mode=ro"
        try:
            con = sqlite3.connect(uri, uri=True)
        except sqlite3.OperationalError:
            return None
        try:
            row = con.execute(
                "SELECT id,started_at_ms,finished_at_ms,outcome,snapshot_id,failure_kind,"
                "last_stage,elapsed_ms "
                "FROM snapshot_attempts ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            return {
                "id": row[0],
                "started_at_ms": row[1],
                "finished_at_ms": row[2],
                "outcome": row[3],
                "snapshot_id": row[4],
                "failure_kind": row[5],
                "last_stage": row[6],
                "elapsed_ms": row[7],
            }
        finally:
            con.close()

    def get_snapshot_attempts(self, *, limit: int = 30) -> list[dict[str, object]]:
        """Read bounded newest scheduler attempt evidence for timing policy."""
        bounded_limit = max(1, min(int(limit), 100))
        uri = f"file:{self._db_path}?mode=ro"
        try:
            con = sqlite3.connect(uri, uri=True)
        except sqlite3.OperationalError:
            return []
        try:
            rows = con.execute(
                "SELECT id,started_at_ms,finished_at_ms,outcome,snapshot_id,"
                "failure_kind,last_stage,elapsed_ms "
                "FROM snapshot_attempts ORDER BY id DESC LIMIT ?",
                (bounded_limit,),
            ).fetchall()
            keys = (
                "id",
                "started_at_ms",
                "finished_at_ms",
                "outcome",
                "snapshot_id",
                "failure_kind",
                "last_stage",
                "elapsed_ms",
            )
            return [dict(zip(keys, row, strict=True)) for row in rows]
        finally:
            con.close()

    def append_structure_schedule_adjustment(
        self,
        *,
        source_attempt_id: int,
        decided_at_ms: int,
        success_sample_count: int,
        success_p95_s: int | None,
        previous_timeout_s: int,
        previous_cadence_s: int,
        timeout_s: int,
        cadence_s: int,
        reason: str,
    ) -> None:
        """Append one auditable effective-schedule change exactly once."""
        con = self._connect_writer()
        try:
            con.execute(
                "INSERT INTO structure_schedule_adjustments("
                "source_attempt_id,decided_at_ms,success_sample_count,success_p95_s,"
                "previous_timeout_s,previous_cadence_s,timeout_s,cadence_s,reason"
                ") VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    source_attempt_id,
                    decided_at_ms,
                    success_sample_count,
                    success_p95_s,
                    previous_timeout_s,
                    previous_cadence_s,
                    timeout_s,
                    cadence_s,
                    reason,
                ),
            )
        finally:
            con.close()

    def get_latest_structure_schedule_adjustment(
        self,
    ) -> dict[str, object] | None:
        """Read the newest persisted effective Structure schedule."""
        uri = f"file:{self._db_path}?mode=ro"
        try:
            con = sqlite3.connect(uri, uri=True)
        except sqlite3.OperationalError:
            return None
        try:
            row = con.execute(
                "SELECT source_attempt_id,success_sample_count,success_p95_s,"
                "previous_timeout_s,previous_cadence_s,timeout_s,cadence_s,reason "
                "FROM structure_schedule_adjustments ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            keys = (
                "source_attempt_id",
                "success_sample_count",
                "success_p95_s",
                "previous_timeout_s",
                "previous_cadence_s",
                "timeout_s",
                "cadence_s",
                "reason",
            )
            return dict(zip(keys, row, strict=True))
        finally:
            con.close()

    def count_structure_schedule_adjustments(self) -> int:
        """Return the append-only adjustment count for invariant checks."""
        con = sqlite3.connect(self._db_path)
        try:
            row = con.execute(
                "SELECT COUNT(*) FROM structure_schedule_adjustments"
            ).fetchone()
            return int(row[0]) if row is not None else 0
        finally:
            con.close()

    # ── Phase 03.1 Plan 01: l2_mirror_state singleton (GAP-2 + GAP-3) ──────
    #
    # Freshness anchor for /health l2_tob_age_seconds sub-check. Mirror's
    # success path writes here; /health (Plan 02 wires it) reads via the
    # getter. None = cold start ("never mirrored") → /health caller maps to
    # WARN. Sub-second /health probes MUST NOT round-trip to Supabase, so
    # this lives in the local SQLite file alongside scheduler_state.

    def get_l2_tob_last_mirror_at_s(self) -> int | None:
        """Read last successful L2 mirror push wall-clock (seconds since epoch).

        Returns None when the l2_mirror_state row is absent (cold start) —
        caller treats None as "never mirrored", typically maps to /health WARN.
        Never raises on missing file or missing table; the DDL is created in
        init_schema() but read-only callers may hit a not-yet-initialized DB.
        """
        uri = f"file:{self._db_path}?mode=ro"
        try:
            con = sqlite3.connect(uri, uri=True)
        except sqlite3.OperationalError:
            return None
        try:
            try:
                row = con.execute(
                    "SELECT last_mirror_at_s FROM l2_mirror_state WHERE id=1"
                ).fetchone()
            except sqlite3.OperationalError:
                # Legacy DB without the l2_mirror_state table (pre-03.1).
                return None
            return int(row[0]) if row else None
        finally:
            con.close()

    def upsert_l2_tob_mirror_state(self, last_mirror_at_s: int) -> None:
        """Write singleton row with the latest successful mirror wall-clock.

        Idempotent — uses ON CONFLICT(id) DO UPDATE so repeated calls overwrite
        the single row enforced by CHECK(id=1).
        """
        con = self._connect_writer()
        try:
            con.execute(
                "INSERT INTO l2_mirror_state(id, last_mirror_at_s) "
                "VALUES (1, ?) "
                "ON CONFLICT(id) DO UPDATE SET "
                "last_mirror_at_s=excluded.last_mirror_at_s",
                (int(last_mirror_at_s),),
            )
        finally:
            con.close()

    def get_latest_snapshot(self, *, data_product: str | None = None) -> dict | None:
        """Read the most-recent snapshot row for /health endpoint.

        Phase 02 Plan 02 — used by /health handler to determine pass/warn/fail.
        Uses read-only mode=ro URI (P3.8: HTTP server NEVER writes SQLite).
        Returns None if no snapshots exist (first deploy edge case).

        ``data_product`` narrows the result to one explicit product when a
        consumer has a production contract (for example health reads only
        Structure).  ``None`` retains the legacy all-snapshot behavior for
        historical reconciliation tooling.
        """
        uri = f"file:{self._db_path}?mode=ro"
        try:
            con = sqlite3.connect(uri, uri=True)
        except sqlite3.OperationalError:
            # DB file doesn't exist yet → no snapshot
            return None
        try:
            where = "WHERE data_product = ?" if data_product is not None else ""
            params = (data_product,) if data_product is not None else ()
            # Try to read the new Plan 03 columns; fall back gracefully for old DBs
            # that haven't been migrated (supabase_mirror_at_ms + parquet_r2_url).
            try:
                row = con.execute(
                    "SELECT id, taken_at_ms, finished_at_ms, mode, is_valid, market_count, notes, "
                    "snapshot_status, "
                    "supabase_mirror_at_ms, parquet_r2_url "
                    f"FROM snapshots {where} ORDER BY id DESC LIMIT 1",
                    params,
                ).fetchone()
                if not row:
                    return None
                return {
                    "id": row[0],
                    "taken_at_ms": row[1],
                    "finished_at_ms": row[2],
                    "mode": row[3],
                    "is_valid": bool(row[4]),
                    "market_count": row[5],
                    "notes": row[6],
                    "snapshot_status": row[7],
                    "supabase_mirror_at_ms": row[8],  # Phase 02 Plan 03: nullable
                    "parquet_r2_url": row[9],  # Phase 02 Plan 03: nullable
                }
            except sqlite3.OperationalError:
                # Old DB schema without Plan 03 columns — fall back to 7-column query
                row = con.execute(
                    "SELECT id, taken_at_ms, finished_at_ms, mode, is_valid, market_count, notes "
                    "FROM snapshots ORDER BY id DESC LIMIT 1"
                ).fetchone()
                if not row:
                    return None
                return {
                    "id": row[0],
                    "taken_at_ms": row[1],
                    "finished_at_ms": row[2],
                    "mode": row[3],
                    "is_valid": bool(row[4]),
                    "market_count": row[5],
                    "notes": row[6],
                    "snapshot_status": None,
                    "supabase_mirror_at_ms": None,
                    "parquet_r2_url": None,
                }
        finally:
            con.close()

    def purge_old_snapshots(
        self,
        *,
        older_than_days: int = 7,
        keep_last: int = 5,
        max_snapshots_per_run: int = 10,
        parquet_root: Path | None = None,
        dry_run: bool = False,
    ) -> tuple[int, list[int]]:
        """Delete one bounded batch older than N days, keeping M most recent.

        Deletes in FK-safe order: validation_issues → markets → event_tags →
        events → snapshots. Also deletes parquet files.

        Bounding each transaction prevents a large historical backlog from growing
        WAL for minutes and losing all progress when a deployment interrupts it.
        Returns (deleted_count, deleted_ids).
        """
        import time as _time

        if max_snapshots_per_run < 1:
            raise ValueError("max_snapshots_per_run must be positive")
        cutoff_ms = int((_time.time() - older_than_days * 86_400) * 1000)

        con = sqlite3.connect(f"file:{self._db_path}?mode=ro", uri=True)
        try:
            # Find IDs to delete: older than cutoff AND not in the last M
            keep_ids = [
                r[0]
                for r in con.execute(
                    "SELECT id FROM snapshots ORDER BY id DESC LIMIT ?",
                    (keep_last,),
                ).fetchall()
            ]
            placeholders = ",".join("?" for _ in keep_ids)
            to_delete = [
                r[0]
                for r in con.execute(
                    f"SELECT id FROM snapshots "
                    f"WHERE taken_at_ms < ? AND id NOT IN ({placeholders}) "
                    f"ORDER BY id LIMIT ?",
                    [cutoff_ms, *keep_ids, max_snapshots_per_run],
                ).fetchall()
            ]
            # Archive ownership is explicit.  A Structure snapshot carries the
            # no-archive marker in parquet_path for compatibility with the old
            # non-null column contract; that marker is not a file to unlink.
            # Never infer deletion ownership from a path-shaped string alone.
            parquet_paths = (
                [
                    r[0]
                    for r in con.execute(
                        f"SELECT parquet_path, archive_status FROM snapshots "
                        f"WHERE id IN ({','.join('?' for _ in to_delete)})",
                        to_delete,
                    ).fetchall()
                    if r[1] != "not_requested"
                ]
                if parquet_root is not None and to_delete
                else []
            )
        finally:
            con.close()

        if not to_delete:
            logger.info("purge_old_snapshots: nothing to delete")
            return (0, [])

        if dry_run:
            logger.info(
                f"purge_old_snapshots DRY-RUN: would delete {len(to_delete)} "
                f"snapshots (ids={to_delete}), {len(parquet_paths)} parquet files"
            )
            return (0, to_delete)

        # Delete in FK-safe order
        con = self._connect_writer()
        try:
            con.execute("PRAGMA journal_mode=WAL")
            con.execute("BEGIN IMMEDIATE")
            try:
                id_placeholders = ",".join("?" for _ in to_delete)
                con.execute(
                    f"DELETE FROM validation_issues WHERE snapshot_id IN ({id_placeholders})",
                    to_delete,
                )
                con.execute(
                    "DELETE FROM snapshot_source_coverage "
                    f"WHERE snapshot_id IN ({id_placeholders})",
                    to_delete,
                )
                con.execute(
                    "DELETE FROM event_market_memberships "
                    f"WHERE snapshot_id IN ({id_placeholders})",
                    to_delete,
                )
                con.execute(
                    "DELETE FROM neg_risk_group_truth "
                    f"WHERE snapshot_id IN ({id_placeholders})",
                    to_delete,
                )
                con.execute(
                    f"DELETE FROM markets WHERE snapshot_id IN ({id_placeholders})",
                    to_delete,
                )
                con.execute(
                    f"DELETE FROM event_tags WHERE snapshot_id IN ({id_placeholders})",
                    to_delete,
                )
                con.execute(
                    f"DELETE FROM events WHERE snapshot_id IN ({id_placeholders})",
                    to_delete,
                )
                con.execute(
                    f"DELETE FROM snapshots WHERE id IN ({id_placeholders})",
                    to_delete,
                )
                con.execute("COMMIT")
            except Exception:
                _rollback_without_masking(con)
                logger.exception("purge_old_snapshots rolled back")
                raise
        finally:
            con.close()

        # Delete parquet files (after SQLite commit succeeds)
        deleted_files = 0
        for pp in parquet_paths:
            try:
                p = Path(pp)
                if p.exists():
                    p.unlink()
                    deleted_files += 1
            except OSError:
                logger.warning(f"purge: could not delete parquet file {pp}")

        logger.info(
            f"purge_old_snapshots: deleted {len(to_delete)} snapshots "
            f"(ids={to_delete}), {deleted_files} parquet files"
        )
        return (len(to_delete), to_delete)
