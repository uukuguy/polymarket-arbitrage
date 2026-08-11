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
import re
import sqlite3
import time
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from loguru import logger

from polyarb.perception.market_truth import (
    EventMember,
    GroupTruth,
    SourceCoverage,
    market_truth_mismatch_reason,
    membership_hash,
)
from polyarb.perception.structure_contract import (
    STRUCTURE_CERTIFICATION_COMPONENTS,
    STRUCTURE_COMPONENTS,
    STRUCTURE_DRIFT_CLASSIFIER_V1,
    STRUCTURE_DRIFT_CLASSIFIER_V2,
    STRUCTURE_DRIFT_CLASSIFIER_V3,
    STRUCTURE_DRIFT_CLASSIFIER_V4,
    STRUCTURE_DRIFT_CLASSIFIERS_V3_COMPATIBLE,
    STRUCTURE_DRIFT_SOURCE_EVENT_MAX_MEMBER_WORK,
    STRUCTURE_DRIFT_SOURCE_EVENT_MAX_PAYLOAD_BYTES,
    STRUCTURE_DRIFT_SOURCE_EVENT_MAX_ROWS,
    STRUCTURE_EVENT_MEMBER_METADATA_CONTRACT,
    STRUCTURE_EVENT_SOURCE_CONTRACT,
    STRUCTURE_NORMALIZATION_CONTRACT_VERSION,
    STRUCTURE_POINTER_SWITCH_WRITER_LOCK_TIMEOUT_S,
    STRUCTURE_PROJECTION_EXCLUSION_REASONS,
    STRUCTURE_PUBLICATION_MAX_ROWS,
    STRUCTURE_SOURCE_COMPONENTS,
)
from polyarb.perception.structure_event_members import (
    decode_event_member_batch,
    extract_structure_event_member_row,
)
from polyarb.storage.row_chain_sha256 import (
    ROW_CHAIN_SHA256_V2,
    RowChainSHA256,
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
    PRODUCER_ARBITRATION_DDL,
    SCHEDULER_STATE_DDL,
    SNAPSHOT_ATTEMPTS_DDL,
    STRUCTURE_DEFER_RECEIPTS_DDL,
    STRUCTURE_DRIFT_ATTEMPTS_DDL,
    STRUCTURE_EVENT_MEMBER_SCHEMA_STATEMENTS,
    STRUCTURE_GENERATIONS_DDL,
    STRUCTURE_SCHEDULE_ADJUSTMENTS_DDL,
    STRUCTURE_SYNC_WINDOWS_DDL,
    migrate_fault_auth_finalize,
    migrate_fault_intent_status,
)
from polyarb.storage.serializable_sha256 import SerializableSHA256
from polyarb.validator.category import Category, Issue, SnapshotStatus
from polyarb.validator.layers import determine_snapshot_status

if TYPE_CHECKING:
    from polyarb.perception.structure_drift import (
        FreshProjectionChunk,
        FreshProjectionCommitment,
        FreshProjectionCursor,
    )

_SNAPSHOT_ATTEMPT_STDERR_MAX_BYTES = 100_000_000
_SNAPSHOT_ATTEMPT_STDERR_TAIL_RE = re.compile(
    r"(?:snapshot-stage stage="
    r"(?:gamma-events|gamma-markets|membership-recheck|validate|persist) "
    r"state=(?:start|complete) elapsed_ms=(?:0|[1-9][0-9]*)|"
    r"structure-page-boundary stage=(?:gamma-events|gamma-markets) "
    r"operation=(?:fetch|commit) state=(?:start|complete) "
    r"elapsed_ms=(?:0|[1-9][0-9]*)|"
    r"structure-publication-progress "
    r"stage=(?:normalizing|certifying|ready) "
    r"component=(?:[a-z][a-z_-]{0,31}|none) "
    r"chunks=(?:100|[1-9][0-9]?) rows=(?:0|[1-9][0-9]*)|"
    r"structure-sync-failure failure_kind=(?:membership-invalid(?: "
    r"membership_kind=(?:active-market-missing|group-truth|market-identity|"
    r"terminal-invariant) key_sha256=[0-9a-f]{64})?|generation-count-mismatch|"
    r"generation-incomplete|generation-validation-issues|pointer-switch-deadline|publication-contract-deadline|"
    r"source-truth-invalid|sqlite-busy|structure-child-error|"
    r"structure-page-deadline|"
    r"structure-publication-not-writing))"
    r"|structure-publication-superseded publication_id=[0-9a-f]{32}"
)
_STRUCTURE_DRIFT_SAFE_MARKER_RE = re.compile(
    rb"^structure-drift stage=(?:source-events|source-markets|generation-members|"
    rb"legacy-members|fresh-group-truth|sealed|stale|exact|none) "
    rb"chunks=(?:0|[1-9][0-9]*) rows=(?:0|[1-9][0-9]*)$",
    re.MULTILINE,
)
_STRUCTURE_DRIFT_ATTEMPT_RETENTION = 100


def _structure_drift_event_prefix_size(
    workloads: list[tuple[int, int, int]],
) -> int:
    """Choose a stable non-empty prefix under payload and member-work budgets."""
    selected = 0
    payload_bytes = 0
    member_work = 0
    for event_payload_bytes, embedded_count, relation_count in workloads:
        if selected == 0 and (
            event_payload_bytes > STRUCTURE_DRIFT_SOURCE_EVENT_MAX_PAYLOAD_BYTES
            or max(embedded_count, relation_count)
            > STRUCTURE_DRIFT_SOURCE_EVENT_MAX_MEMBER_WORK
        ):
            raise ValueError("structure-drift-source-event-workload-oversized")
        next_payload = payload_bytes + event_payload_bytes
        next_member_work = member_work + max(embedded_count, relation_count)
        if selected and (
            next_payload > STRUCTURE_DRIFT_SOURCE_EVENT_MAX_PAYLOAD_BYTES
            or next_member_work > STRUCTURE_DRIFT_SOURCE_EVENT_MAX_MEMBER_WORK
        ):
            break
        selected += 1
        payload_bytes = next_payload
        member_work = next_member_work
    return selected

_VALID_MODES = ("subset", "full")
# Structure publication and Quote collection share one WAL database. Their
# bounded bulk transactions can legitimately overlap for tens of seconds on
# the production volume, so SQLite's five-second default is too short.
SQLITE_BUSY_TIMEOUT_S = 120.0
STRUCTURE_EVENT_MARKET_BACKFILL_MAX_EVENTS = STRUCTURE_PUBLICATION_MAX_ROWS
STRUCTURE_EVENT_PAYLOAD_MAX_BYTES = 1_000_000
STRUCTURE_BOOTSTRAP_PAYLOAD_MAX_BYTES = 16_000_000

# Booleans that are stored as INTEGER 0/1 in SQLite — convert before insert.
_BOOL_COLUMNS = ("active", "closed", "neg_risk", "incomplete")
# events table also has bool fields stored as INTEGER 0/1.
_EVENT_BOOL_COLUMNS = ("active", "closed")

_STRUCTURE_COMPONENTS = STRUCTURE_COMPONENTS
_STRUCTURE_SOURCE_COMPONENTS = STRUCTURE_SOURCE_COMPONENTS
_STRUCTURE_CERTIFICATION_COMPONENTS = STRUCTURE_CERTIFICATION_COMPONENTS

_STRUCTURE_EVENT_MEMBER_RECEIPT_FIELDS = (
    "window_id", "source_event_count", "source_event_root", "source_identity_hash",
    "metadata_contract", "member_row_count", "member_row_root",
    "invalid_member_count", "invalid_member_root", "terminal_event_cursor",
    "terminal_member_ordinal", "terminal_member_byte_offset", "sealed_at_ms",
)


def _structure_event_member_receipt_digest(
    values: tuple[object, ...],
    *,
    event_conflict_count: int,
    event_conflict_root: str,
    event_conflict_merkle_root: str,
    source_group_truth_count: int = 0,
    source_group_truth_root: str = "0" * 64,
) -> str:
    if len(values) != len(_STRUCTURE_EVENT_MEMBER_RECEIPT_FIELDS):
        raise ValueError("invalid-structure-event-member-receipt-fields")
    return hashlib.sha256(json.dumps(
        (*values, "structure-event-global-conflict-v1", event_conflict_count,
         event_conflict_root, event_conflict_merkle_root,
         "structure-event-source-group-truth-v1", source_group_truth_count,
         source_group_truth_root),
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()).hexdigest()


def _structure_event_group_truth_checkpoint_digest(
    values: tuple[object, ...],
) -> str:
    return hashlib.sha256(json.dumps(
        ("structure-event-source-group-truth-checkpoint-v1", *values),
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()).hexdigest()


def _validated_structure_event_group_truth(
    con: sqlite3.Connection,
    window_id: str,
    *,
    expected: tuple[int, str] | None = None,
    source_receipt_digest: str | None = None,
) -> tuple[int, str]:
    progress = con.execute(
        "SELECT event_cursor,group_cursor,market_cursor,member_ordinal,"
        "membership_state,member_count,active_named_count,invalid_member_count,"
        "truth_count,truth_state,completed_at_ms,checkpoint_digest,"
        "tradable_open_named_count FROM structure_sync_event_group_truth_progress "
        "WHERE window_id=?",
        (window_id,),
    ).fetchone()
    member_progress = (
        (source_receipt_digest,)
        if source_receipt_digest is not None
        else con.execute(
            "SELECT source_receipt_digest FROM structure_sync_event_member_progress "
            "WHERE window_id=?",
            (window_id,),
        ).fetchone()
    )
    if progress is None or member_progress is None or progress[10] is None:
        raise ValueError("structure-event-group-truth-incomplete")
    membership = SerializableSHA256.from_json(str(progress[4]))
    stored = RowChainSHA256.from_json(
        str(progress[9]), expected_domain="source-event"
    )
    checkpoint = _structure_event_group_truth_checkpoint_digest((
        member_progress[0], *progress[:10], progress[12],
    ))
    if (
        stored.count != int(progress[8])
        or progress[11] != checkpoint
        or membership.to_json() != SerializableSHA256.new().to_json()
        or any(int(progress[index]) != 0 for index in (5, 6, 7, 12))
    ):
        raise ValueError("structure-event-group-truth-invalid")
    actual_pair = (stored.count, stored.hexdigest())
    if expected is not None:
        if (
            type(expected[0]) is not int
            or expected[0] < 0
            or not isinstance(expected[1], str)
            or len(expected[1]) != 64
            or expected != actual_pair
        ):
            raise ValueError("structure-event-group-truth-invalid")
    return actual_pair


def _event_conflict_leaf_hash(
    *, window_id: str, event_id: str, global_conflict: bool
) -> str:
    return hashlib.sha256(json.dumps(
        (
            "structure-event-global-conflict-leaf-v1",
            window_id,
            event_id,
            global_conflict,
        ),
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()).hexdigest()


def _event_conflict_merkle_parent(left: str, right: str) -> str:
    return hashlib.sha256(
        b"structure-event-global-conflict-merkle-v1\x00"
        + bytes.fromhex(left)
        + bytes.fromhex(right)
    ).hexdigest()


def _event_conflict_merkle_proofs(
    leaves: list[str],
) -> tuple[str, list[str]]:
    if not leaves:
        return hashlib.sha256(
            b"structure-event-global-conflict-merkle-empty-v1"
        ).hexdigest(), []
    levels = [list(leaves)]
    while len(levels[-1]) > 1:
        current = levels[-1]
        levels.append([
            _event_conflict_merkle_parent(
                current[index],
                current[index + 1] if index + 1 < len(current) else current[index],
            )
            for index in range(0, len(current), 2)
        ])
    proofs = []
    for leaf_index in range(len(leaves)):
        index = leaf_index
        proof = []
        for level in levels[:-1]:
            sibling_index = index ^ 1
            sibling = level[sibling_index] if sibling_index < len(level) else level[index]
            proof.append(("left" if sibling_index < index else "right", sibling))
            index //= 2
        proofs.append(json.dumps(proof, separators=(",", ":")))
    return levels[-1][0], proofs


def _verify_event_conflict_merkle_proof(
    *, leaf_hash: str, proof_json: str, expected_root: str
) -> bool:
    try:
        proof = json.loads(proof_json)
        if not isinstance(proof, list):
            return False
        current = leaf_hash
        for item in proof:
            if (
                not isinstance(item, list)
                or len(item) != 2
                or item[0] not in {"left", "right"}
                or not isinstance(item[1], str)
                or len(item[1]) != 64
            ):
                return False
            current = (
                _event_conflict_merkle_parent(item[1], current)
                if item[0] == "left"
                else _event_conflict_merkle_parent(current, item[1])
            )
        return current == expected_root
    except (TypeError, ValueError, json.JSONDecodeError):
        return False


def _event_member_progress_state(
    *, member_chain: RowChainSHA256, source_event_count: int,
    source_event_root: str, source_identity_hash: str,
    window_checkpoint_at_ms: int,
    phase: str = "members",
    conflict_cursor: str = "",
    event_conflict_chain: RowChainSHA256 | None = None,
    merkle_level: int = 0,
    merkle_cursor: int = -1,
    merkle_width: int = 0,
    merkle_pending_index: int = -1,
    merkle_pending_hash: str = "",
    proof_cursor: str = "",
    proof_count: int = 0,
) -> str:
    conflict_chain = (
        RowChainSHA256.new("source-event")
        if event_conflict_chain is None else event_conflict_chain
    )
    return json.dumps({
        "member_chain": member_chain.to_json(),
        "source_event_count": source_event_count,
        "source_event_root": source_event_root,
        "source_identity_hash": source_identity_hash,
        "window_checkpoint_at_ms": window_checkpoint_at_ms,
        "phase": phase,
        "conflict_cursor": conflict_cursor,
        "event_conflict_chain": conflict_chain.to_json(),
        "merkle_level": merkle_level,
        "merkle_cursor": merkle_cursor,
        "merkle_width": merkle_width,
        "merkle_pending_index": merkle_pending_index,
        "merkle_pending_hash": merkle_pending_hash,
        "proof_cursor": proof_cursor,
        "proof_count": proof_count,
    }, sort_keys=True, separators=(",", ":"))


def _read_event_member_progress_state(
    encoded: str,
) -> tuple[
    RowChainSHA256, int, str, str, int, str, str, RowChainSHA256,
    int, int, int, int, str, str, int,
]:
    try:
        state = json.loads(encoded)
        if not isinstance(state, dict) or set(state) != {
            "member_chain", "source_event_count", "source_event_root",
            "source_identity_hash", "window_checkpoint_at_ms", "phase",
            "conflict_cursor", "event_conflict_chain",
            "merkle_level", "merkle_cursor", "merkle_width",
            "merkle_pending_index", "merkle_pending_hash", "proof_cursor",
            "proof_count",
        }:
            raise ValueError
        chain = RowChainSHA256.from_json(
            str(state["member_chain"]), expected_domain="source-event"
        )
        conflict_chain = RowChainSHA256.from_json(
            str(state["event_conflict_chain"]),
            expected_domain="source-event",
        )
        count, checkpoint = state["source_event_count"], state["window_checkpoint_at_ms"]
        root, identity = state["source_event_root"], state["source_identity_hash"]
        phase, conflict_cursor = state["phase"], state["conflict_cursor"]
        merkle_level = state["merkle_level"]
        merkle_cursor = state["merkle_cursor"]
        merkle_width = state["merkle_width"]
        merkle_pending_index = state["merkle_pending_index"]
        merkle_pending_hash = state["merkle_pending_hash"]
        proof_cursor = state["proof_cursor"]
        proof_count = state["proof_count"]
        if (type(count) is not int or count < 0 or type(checkpoint) is not int
                or checkpoint < 0 or not isinstance(root, str) or len(root) != 64
                or not isinstance(identity, str) or len(identity) != 64):
            raise ValueError
        if phase not in {
            "members", "group-truth", "conflicts", "merkle", "proofs", "complete"
        }:
            raise ValueError
        if (
            not isinstance(conflict_cursor, str)
            or type(merkle_level) is not int
            or merkle_level < 0
            or type(merkle_cursor) is not int
            or merkle_cursor < -1
            or type(merkle_width) is not int
            or merkle_width < 0
            or type(merkle_pending_index) is not int
            or merkle_pending_index < -1
            or not isinstance(merkle_pending_hash, str)
            or (
                (merkle_pending_index == -1 and merkle_pending_hash != "")
                or (
                    merkle_pending_index >= 0
                    and (
                        phase != "merkle"
                        or merkle_pending_index != merkle_cursor
                        or merkle_pending_index >= merkle_width - 1
                        or merkle_pending_index % 2 != 0
                        or len(merkle_pending_hash) != 64
                    )
                )
            )
            or not isinstance(proof_cursor, str)
            or type(proof_count) is not int
            or proof_count < 0
        ):
            raise ValueError
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise ValueError("structure-event-member-progress-invalid") from error
    return (
        chain, count, root, identity, checkpoint, str(phase),
        str(conflict_cursor), conflict_chain,
        merkle_level, merkle_cursor, merkle_width,
        merkle_pending_index, str(merkle_pending_hash), str(proof_cursor),
        proof_count,
    )


def _migrate_structure_event_member_schema(
    con: sqlite3.Connection,
    *,
    fault_hook: Callable[[str], None] | None = None,
) -> None:
    """Install empty canonical event-member authority as one atomic migration."""
    con.execute("SAVEPOINT structure_event_member_schema_migration")
    try:
        for fault_point, statement in STRUCTURE_EVENT_MEMBER_SCHEMA_STATEMENTS:
            con.execute(statement)
            if fault_point is not None and fault_hook is not None:
                fault_hook(fault_point)
        progress_columns = {
            str(row[1]) for row in con.execute(
                "PRAGMA table_info(structure_sync_event_member_progress)"
            )
        }
        additions = (
            ("member_character_offset", "INTEGER NOT NULL DEFAULT 0"),
            ("source_receipt_digest", "TEXT NOT NULL DEFAULT ''"),
            ("parent_payload_hash", "TEXT NOT NULL DEFAULT ''"),
            ("checkpoint_digest", "TEXT NOT NULL DEFAULT ''"),
        )
        for column, ddl in additions:
            if column not in progress_columns:
                con.execute(
                    f"ALTER TABLE structure_sync_event_member_progress "
                    f"ADD COLUMN {column} {ddl}"
                )
                if fault_hook is not None:
                    fault_hook(f"after-progress-{column}")
        receipt_columns = {
            str(row[1]) for row in con.execute(
                "PRAGMA table_info(structure_sync_event_member_receipts)"
            )
        }
        receipt_additions = (
            ("event_conflict_count", "INTEGER NOT NULL DEFAULT 0"),
            (
                "event_conflict_root",
                "TEXT NOT NULL DEFAULT "
                "'0000000000000000000000000000000000000000000000000000000000000000'",
            ),
            (
                "event_conflict_merkle_root",
                "TEXT NOT NULL DEFAULT "
                "'0000000000000000000000000000000000000000000000000000000000000000'",
            ),
            ("source_group_truth_count", "INTEGER NOT NULL DEFAULT 0"),
            (
                "source_group_truth_root",
                "TEXT NOT NULL DEFAULT "
                "'0000000000000000000000000000000000000000000000000000000000000000'",
            ),
        )
        for column, ddl in receipt_additions:
            if column not in receipt_columns:
                con.execute(
                    f"ALTER TABLE structure_sync_event_member_receipts "
                    f"ADD COLUMN {column} {ddl}"
                )
                if fault_hook is not None:
                    fault_hook(f"after-receipt-{column}")
        group_truth_columns = {
            str(row[1]) for row in con.execute(
                "PRAGMA table_info(structure_sync_event_group_truth_staging)"
            )
        }
        if "tradable_open_named_count" not in group_truth_columns:
            con.execute(
                "ALTER TABLE structure_sync_event_group_truth_staging ADD COLUMN "
                "tradable_open_named_count INTEGER NOT NULL DEFAULT 0 "
                "CHECK(tradable_open_named_count>=0)"
            )
            if fault_hook is not None:
                fault_hook("after-group-truth-tradable_open_named_count")
        group_progress_columns = {
            str(row[1]) for row in con.execute(
                "PRAGMA table_info(structure_sync_event_group_truth_progress)"
            )
        }
        if "tradable_open_named_count" not in group_progress_columns:
            con.execute(
                "ALTER TABLE structure_sync_event_group_truth_progress ADD COLUMN "
                "tradable_open_named_count INTEGER NOT NULL DEFAULT 0 "
                "CHECK(tradable_open_named_count>=0)"
            )
            if fault_hook is not None:
                fault_hook("after-group-progress-tradable_open_named_count")
        con.execute("RELEASE SAVEPOINT structure_event_member_schema_migration")
    except BaseException:
        con.execute("ROLLBACK TO SAVEPOINT structure_event_member_schema_migration")
        con.execute("RELEASE SAVEPOINT structure_event_member_schema_migration")
        raise


def _migrate_structure_event_market_progress(con: sqlite3.Connection) -> None:
    """Add indexed subcursor authority to the brief pre-review progress schema."""
    columns = {
        str(row[1])
        for row in con.execute(
            "PRAGMA table_info(structure_sync_event_market_backfill_progress)"
        )
    }
    if not columns:
        return
    if "migration_reason" not in columns:
        con.execute(
            "ALTER TABLE structure_sync_event_market_backfill_progress "
            "ADD COLUMN migration_reason TEXT"
        )
    if "event_cursor" in columns:
        return
    additions = (
        ("window_checkpoint_at_ms", "INTEGER NOT NULL DEFAULT 0"),
        ("event_cursor", "TEXT NOT NULL DEFAULT ''"),
        ("member_offset", "INTEGER NOT NULL DEFAULT 0"),
        ("relationships_processed", "INTEGER NOT NULL DEFAULT 0"),
    )
    for column, ddl in additions:
        con.execute(
            f"ALTER TABLE structure_sync_event_market_backfill_progress ADD COLUMN {column} {ddl}"
        )
    con.execute(
        "UPDATE structure_sync_event_market_backfill_progress SET "
        "window_checkpoint_at_ms=(SELECT checkpoint_at_ms FROM structure_sync_windows "
        "WHERE id=structure_sync_event_market_backfill_progress.window_id),"
        "event_cursor='',member_offset=0,events_processed=0,"
        "relationships_processed=0,completed_at_ms=NULL,"
        "migration_reason='legacy-after-rowid-rewound'"
    )


def _migrate_structure_recovery_authority(con: sqlite3.Connection) -> None:
    """Persist bounded recovery lineage and detach append-only evidence from staging."""
    window_columns = {
        str(row[1]) for row in con.execute("PRAGMA table_info(structure_sync_windows)")
    }
    if "recovery_root_window_id" not in window_columns:
        con.execute(
            "ALTER TABLE structure_sync_windows ADD COLUMN recovery_root_window_id TEXT"
        )
    con.execute(
        "UPDATE structure_sync_windows SET recovery_root_window_id=id "
        "WHERE recovery_root_window_id IS NULL OR recovery_root_window_id=''"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_structure_sync_recovery_lineage ON "
        "structure_sync_windows(recovery_root_window_id,status,checkpoint_at_ms DESC,id DESC)"
    )

    observation_columns = {
        str(row[1])
        for row in con.execute(
            "PRAGMA table_info(structure_bootstrap_rotation_observations)"
        )
    }
    foreign_keys = con.execute(
        "PRAGMA foreign_key_list(structure_bootstrap_rotation_observations)"
    ).fetchall()
    if observation_columns and (
        "recovery_root_window_id" not in observation_columns or foreign_keys
    ):
        con.execute("DROP TRIGGER IF EXISTS trg_structure_bootstrap_rotation_update")
        con.execute("DROP TRIGGER IF EXISTS trg_structure_bootstrap_rotation_delete")
        con.execute("DROP INDEX IF EXISTS idx_structure_bootstrap_rotation_latest")
        legacy_rows = con.execute(
            "SELECT observation_id,old_window_id,event_cursor,member_offset,"
            "blocked_reason,checkpoint_at_ms,successor_window_id,rotated_at_ms "
            "FROM structure_bootstrap_rotation_observations ORDER BY observation_id"
        ).fetchall()
        con.execute(
            "ALTER TABLE structure_bootstrap_rotation_observations "
            "RENAME TO structure_bootstrap_rotation_observations_legacy"
        )
        con.execute(
            "CREATE TABLE structure_bootstrap_rotation_observations("
            "observation_id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "recovery_root_window_id TEXT NOT NULL,old_window_id TEXT NOT NULL UNIQUE,"
            "event_cursor TEXT NOT NULL,member_offset INTEGER NOT NULL CHECK(member_offset>=0),"
            "blocked_reason TEXT NOT NULL,checkpoint_at_ms INTEGER NOT NULL "
            "CHECK(checkpoint_at_ms>=0),successor_window_id TEXT NOT NULL UNIQUE,"
            "rotated_at_ms INTEGER NOT NULL CHECK(rotated_at_ms>=0),"
            "observation_digest TEXT NOT NULL CHECK(length(observation_digest)=64))"
        )
        for row in legacy_rows:
            root = con.execute(
                "SELECT recovery_root_window_id FROM structure_sync_windows WHERE id=?",
                (str(row[6]),),
            ).fetchone()
            recovery_root = str(row[6]) if root is None else str(root[0])
            digest = _bootstrap_rotation_digest(
                recovery_root_window_id=recovery_root,
                old_window_id=str(row[1]),
                event_cursor=str(row[2]),
                member_offset=int(row[3]),
                blocked_reason=str(row[4]),
                checkpoint_at_ms=int(row[5]),
                successor_window_id=str(row[6]),
                rotated_at_ms=int(row[7]),
            )
            con.execute(
                "INSERT INTO structure_bootstrap_rotation_observations VALUES "
                "(?,?,?,?,?,?,?,?,?,?)",
                (row[0], recovery_root, *row[1:8], digest),
            )
        con.execute("DROP TABLE structure_bootstrap_rotation_observations_legacy")
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_structure_bootstrap_rotation_latest ON "
        "structure_bootstrap_rotation_observations(rotated_at_ms DESC,observation_id DESC)"
    )
    con.execute(
        "CREATE TRIGGER IF NOT EXISTS trg_structure_bootstrap_rotation_update "
        "BEFORE UPDATE ON structure_bootstrap_rotation_observations "
        "BEGIN SELECT RAISE(ABORT,'bootstrap-rotation-append-only'); END"
    )
    con.execute(
        "CREATE TRIGGER IF NOT EXISTS trg_structure_bootstrap_rotation_delete "
        "BEFORE DELETE ON structure_bootstrap_rotation_observations "
        "BEGIN SELECT RAISE(ABORT,'bootstrap-rotation-append-only'); END"
    )


def _migrate_structure_cleanup_progress_binding(con: sqlite3.Connection) -> None:
    """Rebuild the pre-review progress table with one-slot composite authority."""
    columns = {
        str(row[1])
        for row in con.execute(
            "PRAGMA table_info(structure_generation_cleanup_progress)"
        )
    }
    if not columns or "slot" in columns:
        return
    for component in _STRUCTURE_COMPONENTS:
        con.execute(
            f"DROP TRIGGER IF EXISTS trg_structure_generation_{component}_frozen_delete"
        )
    con.execute(
        "ALTER TABLE structure_generation_cleanup_progress "
        "RENAME TO structure_generation_cleanup_progress_v1"
    )
    con.execute(
        "CREATE TABLE structure_generation_cleanup_progress("
        "slot INTEGER NOT NULL DEFAULT 1 UNIQUE CHECK(slot=1),"
        "generation_snapshot_id INTEGER PRIMARY KEY REFERENCES snapshots(id),"
        "publication_id TEXT NOT NULL UNIQUE,phase TEXT NOT NULL CHECK(phase IN "
        "('events','event_tags','memberships','group_truth','markets','issues')) ,"
        "rows_deleted INTEGER NOT NULL DEFAULT 0 CHECK(rows_deleted>=0),"
        "started_at_ms INTEGER NOT NULL CHECK(started_at_ms>=0),"
        "checkpoint_at_ms INTEGER NOT NULL CHECK(checkpoint_at_ms>=0),"
        "blocked_reason TEXT,authorization_digest TEXT NOT NULL CHECK("
        "length(authorization_digest)=64),FOREIGN KEY(generation_snapshot_id,"
        "publication_id) REFERENCES structure_publications(snapshot_id,publication_id))"
    )
    rows = con.execute(
        "SELECT generation_snapshot_id,publication_id,phase,rows_deleted,started_at_ms,"
        "checkpoint_at_ms,blocked_reason FROM structure_generation_cleanup_progress_v1 "
        "ORDER BY checkpoint_at_ms DESC,generation_snapshot_id DESC"
    ).fetchall()
    migrated = False
    for row in rows:
        snapshot_id = int(row[0])
        publication_id = str(row[1])
        receipt = con.execute(
            "SELECT cr.receipt_digest FROM structure_publications p JOIN "
            "structure_generation_comparison_receipts cr ON "
            "cr.generation_snapshot_id=p.snapshot_id AND "
            "cr.publication_id=p.publication_id WHERE p.snapshot_id=? AND "
            "p.publication_id=?",
            (snapshot_id, publication_id),
        ).fetchone()
        blocked_reason = None if row[6] is None else str(row[6])
        if blocked_reason is not None:
            reason = blocked_reason
        elif receipt is None:
            reason = "cleanup-progress-migration-invalid-binding"
        elif migrated:
            reason = "cleanup-progress-migration-superseded"
        else:
            con.execute(
                "INSERT INTO structure_generation_cleanup_progress("
                "generation_snapshot_id,publication_id,phase,rows_deleted,started_at_ms,"
                "checkpoint_at_ms,blocked_reason,authorization_digest) "
                "VALUES (?,?,?,?,?,?,NULL,?)",
                (*row[:6], str(receipt[0])),
            )
            migrated = True
            continue
        _append_generation_cleanup_observation(
            con,
            generation_snapshot_id=snapshot_id,
            publication_id=publication_id,
            state="blocked",
            reason=reason,
            observed_at_ms=int(row[5]),
        )
    con.execute("DROP TABLE structure_generation_cleanup_progress_v1")


def _migrate_structure_drift_hash_v2(
    con: sqlite3.Connection,
    *,
    fault_hook: Callable[[str], None] | None = None,
) -> None:
    """Rebuild the small drift authority tables with explicit hash versions."""
    progress_columns = {
        str(row[1])
        for row in con.execute(
            "PRAGMA table_info(structure_generation_drift_progress)"
        )
    }
    receipt_columns = {
        str(row[1])
        for row in con.execute(
            "PRAGMA table_info(structure_generation_drift_receipts)"
        )
    }
    if not progress_columns or not receipt_columns:
        return
    if {"hash_algorithm", "terminal_reason"} <= progress_columns and (
        "hash_algorithm" in receipt_columns
    ):
        mirror_columns = {
            "generation_projection_member_comparison_count": (
                "INTEGER CHECK(generation_projection_member_comparison_count>=0)"
            ),
            "generation_projection_member_comparison_root": (
                "TEXT CHECK(generation_projection_member_comparison_root IS NULL OR "
                "length(generation_projection_member_comparison_root)=64)"
            ),
            "generation_source_group_truth_comparison_count": (
                "INTEGER CHECK(generation_source_group_truth_comparison_count>=0)"
            ),
            "generation_source_group_truth_comparison_root": (
                "TEXT CHECK(generation_source_group_truth_comparison_root IS NULL OR "
                "length(generation_source_group_truth_comparison_root)=64)"
            ),
        }
        for column, ddl in mirror_columns.items():
            if column not in receipt_columns:
                con.execute(
                    "ALTER TABLE structure_generation_drift_receipts "
                    f"ADD COLUMN {column} {ddl}"
                )
        return
    con.execute("SAVEPOINT structure_drift_hash_v2_migration")
    try:
        con.execute("DROP INDEX IF EXISTS idx_structure_drift_progress_active")
        con.execute("DROP TRIGGER IF EXISTS trg_structure_drift_receipt_update")
        con.execute("DROP TRIGGER IF EXISTS trg_structure_drift_receipt_delete")
        con.execute(
            "ALTER TABLE structure_generation_drift_progress "
            "RENAME TO structure_generation_drift_progress_v1"
        )
        if fault_hook is not None:
            fault_hook("after-progress-rename")
        con.execute(
            "CREATE TABLE structure_generation_drift_progress("
            "comparison_id TEXT PRIMARY KEY,hash_algorithm TEXT NOT NULL "
            "DEFAULT 'serializable-sha256-v1',"
            "legacy_snapshot_id INTEGER NOT NULL REFERENCES snapshots(id),"
            "generation_snapshot_id INTEGER NOT NULL REFERENCES snapshots(id),"
            "publication_id TEXT NOT NULL REFERENCES structure_publications(publication_id),"
            "window_id TEXT NOT NULL REFERENCES structure_sync_windows(id),"
            "normalization_contract_version TEXT NOT NULL,"
            "exact_receipt_digest TEXT NOT NULL CHECK(length(exact_receipt_digest)=64),"
            "pointer_validation_hash TEXT NOT NULL CHECK(length(pointer_validation_hash)=64),"
            "generation_certification_hash TEXT NOT NULL "
            "CHECK(length(generation_certification_hash)=64),"
            "source_event_count INTEGER NOT NULL CHECK(source_event_count>=0),"
            "source_market_count INTEGER NOT NULL CHECK(source_market_count>=0),"
            "source_event_hash TEXT CHECK(source_event_hash IS NULL OR "
            "length(source_event_hash)=64),source_market_hash TEXT CHECK("
            "source_market_hash IS NULL OR length(source_market_hash)=64),"
            "source_identity_hash TEXT CHECK(source_identity_hash IS NULL OR "
            "length(source_identity_hash)=64),phase TEXT NOT NULL CHECK(phase IN "
            "('source-events','source-markets','fresh-projection-members',"
            "'generation-members','legacy-members',"
            "'fresh-group-truth','sealed','stale')),terminal_reason TEXT,"
            "row_cursor_json TEXT,digest_state_json TEXT NOT NULL,"
            "class_counts_json TEXT NOT NULL,class_digests_json TEXT NOT NULL,"
            "created_at_ms INTEGER NOT NULL CHECK(created_at_ms>=0),"
            "checkpoint_at_ms INTEGER NOT NULL CHECK(checkpoint_at_ms>=0),"
            "UNIQUE(legacy_snapshot_id,generation_snapshot_id,publication_id,window_id,"
            "normalization_contract_version,exact_receipt_digest,"
            "pointer_validation_hash,generation_certification_hash,hash_algorithm))"
        )
        con.execute(
            "INSERT INTO structure_generation_drift_progress("
            "comparison_id,hash_algorithm,legacy_snapshot_id,generation_snapshot_id,"
            "publication_id,window_id,normalization_contract_version,"
            "exact_receipt_digest,pointer_validation_hash,"
            "generation_certification_hash,source_event_count,source_market_count,"
            "source_event_hash,source_market_hash,source_identity_hash,phase,"
            "terminal_reason,row_cursor_json,digest_state_json,class_counts_json,"
            "class_digests_json,created_at_ms,checkpoint_at_ms) SELECT "
            "comparison_id,'serializable-sha256-v1',legacy_snapshot_id,"
            "generation_snapshot_id,publication_id,window_id,"
            "normalization_contract_version,exact_receipt_digest,"
            "pointer_validation_hash,generation_certification_hash,"
            "source_event_count,source_market_count,source_event_hash,"
            "source_market_hash,source_identity_hash,phase,CASE WHEN phase IN "
            "('sealed','stale') THEN 'legacy-terminal-reason-unspecified' ELSE NULL END,"
            "row_cursor_json,digest_state_json,class_counts_json,class_digests_json,"
            "created_at_ms,checkpoint_at_ms FROM "
            "structure_generation_drift_progress_v1"
        )
        if fault_hook is not None:
            fault_hook("after-progress-copy")
        con.execute("DROP TABLE structure_generation_drift_progress_v1")
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_structure_drift_progress_active ON "
            "structure_generation_drift_progress(checkpoint_at_ms DESC,comparison_id) "
            "WHERE phase NOT IN ('sealed','stale')"
        )

        con.execute(
            "ALTER TABLE structure_generation_drift_receipts "
            "RENAME TO structure_generation_drift_receipts_v1"
        )
        if fault_hook is not None:
            fault_hook("after-receipt-rename")
        con.execute(
            "CREATE TABLE structure_generation_drift_receipts("
            "comparison_id TEXT PRIMARY KEY,hash_algorithm TEXT NOT NULL "
            "DEFAULT 'serializable-sha256-v1',"
            "legacy_snapshot_id INTEGER NOT NULL REFERENCES snapshots(id),"
            "legacy_taken_at_ms INTEGER NOT NULL CHECK(legacy_taken_at_ms>=0),"
            "legacy_finished_at_ms INTEGER NOT NULL CHECK(legacy_finished_at_ms>=0),"
            "legacy_market_count INTEGER NOT NULL CHECK(legacy_market_count>=0),"
            "legacy_universe_hash TEXT NOT NULL CHECK(length(legacy_universe_hash)=64),"
            "legacy_source_truth_hash TEXT NOT NULL CHECK(length(legacy_source_truth_hash)=64),"
            "generation_snapshot_id INTEGER NOT NULL REFERENCES snapshots(id),"
            "publication_id TEXT NOT NULL REFERENCES structure_publications(publication_id),"
            "window_id TEXT NOT NULL REFERENCES structure_sync_windows(id),"
            "published_snapshot_id INTEGER NOT NULL REFERENCES snapshots(id),"
            "normalization_contract_version TEXT NOT NULL,"
            "exact_receipt_digest TEXT NOT NULL CHECK(length(exact_receipt_digest)=64),"
            "pointer_validation_hash TEXT NOT NULL CHECK(length(pointer_validation_hash)=64),"
            "generation_certification_hash TEXT NOT NULL "
            "CHECK(length(generation_certification_hash)=64),"
            "source_event_count INTEGER NOT NULL CHECK(source_event_count>=0),"
            "source_market_count INTEGER NOT NULL CHECK(source_market_count>=0),"
            "source_event_hash TEXT NOT NULL CHECK(length(source_event_hash)=64),"
            "source_market_hash TEXT NOT NULL CHECK(length(source_market_hash)=64),"
            "source_identity_hash TEXT NOT NULL CHECK(length(source_identity_hash)=64),"
            "projection_universe_hash TEXT NOT NULL "
            "CHECK(length(projection_universe_hash)=64),"
            "projection_group_truth_hash TEXT NOT NULL "
            "CHECK(length(projection_group_truth_hash)=64),"
            "generation_universe_hash TEXT NOT NULL "
            "CHECK(length(generation_universe_hash)=64),"
            "generation_group_truth_hash TEXT NOT NULL "
            "CHECK(length(generation_group_truth_hash)=64),"
            "generation_projection_member_comparison_count INTEGER CHECK("
            "generation_projection_member_comparison_count>=0),"
            "generation_projection_member_comparison_root TEXT CHECK("
            "generation_projection_member_comparison_root IS NULL OR length("
            "generation_projection_member_comparison_root)=64),"
            "generation_source_group_truth_comparison_count INTEGER CHECK("
            "generation_source_group_truth_comparison_count>=0),"
            "generation_source_group_truth_comparison_root TEXT CHECK("
            "generation_source_group_truth_comparison_root IS NULL OR length("
            "generation_source_group_truth_comparison_root)=64),"
            "class_counts_json TEXT NOT NULL,class_digests_json TEXT NOT NULL,"
            "legacy_reconstruction_root TEXT NOT NULL "
            "CHECK(length(legacy_reconstruction_root)=64),"
            "generation_reconstruction_root TEXT NOT NULL "
            "CHECK(length(generation_reconstruction_root)=64),"
            "overlap_conflict_count INTEGER NOT NULL CHECK(overlap_conflict_count=0),"
            "unclassified_count INTEGER NOT NULL CHECK(unclassified_count=0),"
            "created_at_ms INTEGER NOT NULL CHECK(created_at_ms>=0),"
            "receipt_digest TEXT NOT NULL CHECK(length(receipt_digest)=64),"
            "CHECK(published_snapshot_id=generation_snapshot_id),"
            "UNIQUE(legacy_snapshot_id,generation_snapshot_id,publication_id,window_id,"
            "normalization_contract_version,exact_receipt_digest,"
            "pointer_validation_hash,generation_certification_hash,"
            "source_identity_hash,hash_algorithm))"
        )
        receipt_copy_columns = (
            "comparison_id,legacy_snapshot_id,legacy_taken_at_ms,legacy_finished_at_ms,"
            "legacy_market_count,legacy_universe_hash,legacy_source_truth_hash,"
            "generation_snapshot_id,publication_id,window_id,published_snapshot_id,"
            "normalization_contract_version,exact_receipt_digest,"
            "pointer_validation_hash,generation_certification_hash,source_event_count,"
            "source_market_count,source_event_hash,source_market_hash,"
            "source_identity_hash,projection_universe_hash,"
            "projection_group_truth_hash,generation_universe_hash,"
            "generation_group_truth_hash,class_counts_json,class_digests_json,"
            "legacy_reconstruction_root,generation_reconstruction_root,"
            "overlap_conflict_count,unclassified_count,created_at_ms,receipt_digest"
        )
        con.execute(
            "INSERT INTO structure_generation_drift_receipts("
            "comparison_id,hash_algorithm," + receipt_copy_columns.removeprefix(
                "comparison_id,"
            ) + ") SELECT comparison_id,'serializable-sha256-v1'," +
            receipt_copy_columns.removeprefix("comparison_id,") +
            " FROM structure_generation_drift_receipts_v1"
        )
        con.execute("DROP TABLE structure_generation_drift_receipts_v1")
        con.execute(
            "CREATE TRIGGER IF NOT EXISTS trg_structure_drift_receipt_update BEFORE UPDATE ON "
            "structure_generation_drift_receipts BEGIN SELECT "
            "RAISE(ABORT,'structure-drift-receipt-sealed'); END"
        )
        con.execute(
            "CREATE TRIGGER IF NOT EXISTS trg_structure_drift_receipt_delete BEFORE DELETE ON "
            "structure_generation_drift_receipts BEGIN SELECT "
            "RAISE(ABORT,'structure-drift-receipt-sealed'); END"
        )
        if fault_hook is not None:
            fault_hook("before-release")
        con.execute("RELEASE SAVEPOINT structure_drift_hash_v2_migration")
    except BaseException:
        con.execute("ROLLBACK TO SAVEPOINT structure_drift_hash_v2_migration")
        con.execute("RELEASE SAVEPOINT structure_drift_hash_v2_migration")
        raise


def _migrate_structure_drift_classifier_v2(
    con: sqlite3.Connection,
    *,
    fault_hook: Callable[[str], None] | None = None,
) -> None:
    """Version drift authority without touching any source or serving row."""
    progress_info = con.execute(
        "PRAGMA table_info(structure_generation_drift_progress)"
    ).fetchall()
    receipt_info = con.execute(
        "PRAGMA table_info(structure_generation_drift_receipts)"
    ).fetchall()
    if not progress_info or not receipt_info:
        return
    if "classifier_contract_version" in {str(row[1]) for row in progress_info}:
        return
    progress_sql = str(
        con.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND "
            "name='structure_generation_drift_progress'"
        ).fetchone()[0]
    )
    receipt_sql = str(
        con.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND "
            "name='structure_generation_drift_receipts'"
        ).fetchone()[0]
    )
    empty_json = "{}"
    empty_samples_digest = hashlib.sha256(empty_json.encode()).hexdigest()
    diagnostic_state = RowChainSHA256.new("diagnostic/unclassified")
    empty_diagnostic_root = diagnostic_state.hexdigest()
    con.execute("SAVEPOINT structure_drift_classifier_v2_migration")
    try:
        con.execute("DROP TRIGGER IF EXISTS trg_structure_drift_terminal_receipt_update")
        con.execute("DROP TRIGGER IF EXISTS trg_structure_drift_terminal_receipt_delete")
        con.execute("DROP TRIGGER IF EXISTS trg_structure_drift_terminal_receipt_insert")
        con.execute("DROP TABLE IF EXISTS structure_generation_drift_terminal_receipts")
        con.execute("DROP INDEX IF EXISTS idx_structure_drift_progress_active")
        con.execute("DROP TRIGGER IF EXISTS trg_structure_drift_receipt_update")
        con.execute("DROP TRIGGER IF EXISTS trg_structure_drift_receipt_delete")
        con.execute(
            "ALTER TABLE structure_generation_drift_progress "
            "RENAME TO structure_generation_drift_progress_classifier_v1"
        )
        if fault_hook is not None:
            fault_hook("after-progress-rename")
        progress_create = progress_sql.replace(
            "structure_generation_drift_progress",
            "structure_generation_drift_progress",
            1,
        ).replace(
            "hash_algorithm TEXT NOT NULL DEFAULT 'serializable-sha256-v1',",
            "hash_algorithm TEXT NOT NULL DEFAULT 'serializable-sha256-v1',"
            "classifier_contract_version TEXT NOT NULL DEFAULT "
            "'structure-drift-classifier-v1',",
            1,
        ).replace(
            "class_digests_json TEXT NOT NULL,",
            "class_digests_json TEXT NOT NULL,diagnostic_counts_json TEXT NOT NULL "
            "DEFAULT '{}',diagnostic_digest_state_json TEXT NOT NULL DEFAULT '{}',"
            "diagnostic_root TEXT CHECK("
            "diagnostic_root IS NULL OR length(diagnostic_root)=64),"
            "diagnostic_samples_json TEXT NOT NULL DEFAULT '{}',"
            "diagnostic_samples_digest TEXT NOT NULL DEFAULT "
            "'44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a' "
            "CHECK(length(diagnostic_samples_digest)=64),",
            1,
        ).replace(
            "generation_certification_hash,hash_algorithm))",
            "generation_certification_hash,hash_algorithm,"
            "classifier_contract_version))",
            1,
        )
        con.execute(progress_create)
        old_progress_columns = [str(row[1]) for row in progress_info]
        con.execute(
            "INSERT INTO structure_generation_drift_progress("
            + ",".join(old_progress_columns)
            + ",classifier_contract_version,diagnostic_counts_json,"
            "diagnostic_digest_state_json,diagnostic_root,diagnostic_samples_json,"
            "diagnostic_samples_digest) SELECT "
            + ",".join(old_progress_columns)
            + ",?,?,?,?,?,? FROM structure_generation_drift_progress_classifier_v1",
            (
                STRUCTURE_DRIFT_CLASSIFIER_V1,
                empty_json,
                diagnostic_state.to_json(),
                None,
                empty_json,
                empty_samples_digest,
            ),
        )
        con.execute("DROP TABLE structure_generation_drift_progress_classifier_v1")
        con.execute(
            "CREATE INDEX idx_structure_drift_progress_active ON "
            "structure_generation_drift_progress(checkpoint_at_ms DESC,comparison_id) "
            "WHERE phase NOT IN ('sealed','stale')"
        )
        con.execute(
            "ALTER TABLE structure_generation_drift_receipts "
            "RENAME TO structure_generation_drift_receipts_classifier_v1"
        )
        if fault_hook is not None:
            fault_hook("after-authorization-receipt-rename")
        receipt_create = receipt_sql.replace(
            "hash_algorithm TEXT NOT NULL DEFAULT 'serializable-sha256-v1',",
            "hash_algorithm TEXT NOT NULL DEFAULT 'serializable-sha256-v1',"
            "classifier_contract_version TEXT NOT NULL DEFAULT "
            "'structure-drift-classifier-v1',",
            1,
        ).replace(
            "class_digests_json TEXT NOT NULL,",
            "class_digests_json TEXT NOT NULL,diagnostic_counts_json TEXT NOT NULL "
            "DEFAULT '{}',diagnostic_root TEXT NOT NULL DEFAULT "
            "'41ebbc84508b35ea8687e89d89f6b61a5640e5faf9b6ee83a50a2aa625256d7c' "
            "CHECK(length(diagnostic_root)=64),diagnostic_samples_json TEXT NOT NULL "
            "DEFAULT '{}',diagnostic_samples_digest TEXT NOT NULL DEFAULT "
            "'44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a' "
            "CHECK(length(diagnostic_samples_digest)=64),",
            1,
        ).replace(
            "source_identity_hash,hash_algorithm))",
            "source_identity_hash,hash_algorithm,classifier_contract_version))",
            1,
        )
        con.execute(receipt_create)
        old_receipt_columns = [str(row[1]) for row in receipt_info]
        con.execute(
            "INSERT INTO structure_generation_drift_receipts("
            + ",".join(old_receipt_columns)
            + ",classifier_contract_version,diagnostic_counts_json,diagnostic_root,"
            "diagnostic_samples_json,diagnostic_samples_digest) SELECT "
            + ",".join(old_receipt_columns)
            + ",?,?,?,?,? FROM structure_generation_drift_receipts_classifier_v1",
            (
                STRUCTURE_DRIFT_CLASSIFIER_V1,
                empty_json,
                empty_diagnostic_root,
                empty_json,
                empty_samples_digest,
            ),
        )
        migrated_receipt_columns = {
            str(row[1]) for row in con.execute(
                "PRAGMA table_info(structure_generation_drift_receipts)"
            )
        }
        if set(_STRUCTURE_DRIFT_RECEIPT_DIGEST_FIELDS) <= migrated_receipt_columns:
            for row in con.execute(
                "SELECT " + ",".join(_STRUCTURE_DRIFT_RECEIPT_DIGEST_FIELDS)
                + " FROM structure_generation_drift_receipts"
            ).fetchall():
                payload = dict(zip(
                    _STRUCTURE_DRIFT_RECEIPT_DIGEST_FIELDS, row, strict=True
                ))
                con.execute(
                    "UPDATE structure_generation_drift_receipts SET receipt_digest=? "
                    "WHERE comparison_id=?",
                    (
                        _structure_drift_receipt_digest(payload),
                        str(payload["comparison_id"]),
                    ),
                )
        con.execute("DROP TABLE structure_generation_drift_receipts_classifier_v1")
        con.execute(
            "CREATE TRIGGER trg_structure_drift_receipt_update BEFORE UPDATE ON "
            "structure_generation_drift_receipts BEGIN SELECT "
            "RAISE(ABORT,'structure-drift-receipt-sealed'); END"
        )
        con.execute(
            "CREATE TRIGGER trg_structure_drift_receipt_delete BEFORE DELETE ON "
            "structure_generation_drift_receipts BEGIN SELECT "
            "RAISE(ABORT,'structure-drift-receipt-sealed'); END"
        )
        con.execute(
            "CREATE TABLE structure_generation_drift_terminal_receipts("
            "comparison_id TEXT PRIMARY KEY,hash_algorithm TEXT NOT NULL,"
            "classifier_contract_version TEXT NOT NULL,legacy_snapshot_id INTEGER "
            "NOT NULL REFERENCES snapshots(id),generation_snapshot_id INTEGER NOT NULL "
            "REFERENCES snapshots(id),publication_id TEXT NOT NULL REFERENCES "
            "structure_publications(publication_id),window_id TEXT NOT NULL REFERENCES "
            "structure_sync_windows(id),normalization_contract_version TEXT NOT NULL,"
            "exact_receipt_digest TEXT NOT NULL CHECK(length(exact_receipt_digest)=64),"
            "pointer_validation_hash TEXT NOT NULL CHECK(length(pointer_validation_hash)=64),"
            "generation_certification_hash TEXT NOT NULL CHECK(length("
            "generation_certification_hash)=64),source_identity_hash TEXT NOT NULL "
            "CHECK(length(source_identity_hash)=64),terminal_reason TEXT NOT NULL,"
            "class_counts_json TEXT NOT NULL,class_digests_json TEXT NOT NULL,"
            "diagnostic_counts_json TEXT NOT NULL,diagnostic_root TEXT NOT NULL "
            "CHECK(length(diagnostic_root)=64),diagnostic_samples_json TEXT NOT NULL,"
            "diagnostic_samples_digest TEXT NOT NULL CHECK(length("
            "diagnostic_samples_digest)=64),created_at_ms INTEGER NOT NULL CHECK("
            "created_at_ms>=0),checkpoint_at_ms INTEGER NOT NULL CHECK("
            "checkpoint_at_ms>=0),receipt_digest TEXT NOT NULL CHECK(length("
            "receipt_digest)=64))"
        )
        con.execute(
            "CREATE TRIGGER IF NOT EXISTS "
            "trg_structure_drift_terminal_receipt_update BEFORE UPDATE "
            "ON structure_generation_drift_terminal_receipts BEGIN SELECT RAISE(ABORT,"
            "'structure-drift-terminal-receipt-sealed'); END"
        )
        con.execute(
            "CREATE TRIGGER IF NOT EXISTS "
            "trg_structure_drift_terminal_receipt_delete BEFORE DELETE "
            "ON structure_generation_drift_terminal_receipts BEGIN SELECT RAISE(ABORT,"
            "'structure-drift-terminal-receipt-sealed'); END"
        )
        con.execute(
            "CREATE TRIGGER IF NOT EXISTS "
            "trg_structure_drift_terminal_receipt_insert BEFORE INSERT "
            "ON structure_generation_drift_terminal_receipts WHEN EXISTS (SELECT 1 "
            "FROM structure_generation_drift_terminal_receipts WHERE "
            "comparison_id=NEW.comparison_id) BEGIN SELECT RAISE(ABORT,"
            "'structure-drift-terminal-receipt-sealed'); END"
        )
        if fault_hook is not None:
            fault_hook("after-terminal-table-create")
        con.execute("RELEASE SAVEPOINT structure_drift_classifier_v2_migration")
    except BaseException:
        con.execute("ROLLBACK TO SAVEPOINT structure_drift_classifier_v2_migration")
        con.execute("RELEASE SAVEPOINT structure_drift_classifier_v2_migration")
        raise


def _migrate_structure_drift_classifier_v3_exclusions(
    con: sqlite3.Connection,
    *,
    fault_hook: Callable[[str], None] | None = None,
) -> None:
    """Add v3 exclusion evidence without rewriting historical receipts."""
    tables = (
        "structure_generation_drift_receipts",
        "structure_generation_drift_terminal_receipts",
        "structure_generation_drift_progress",
    )
    columns = {
        table: {
            str(row[1]) for row in con.execute(f"PRAGMA table_info({table})")
        }
        for table in tables
    }
    if any(not columns[table] for table in tables):
        return
    receipt_columns = {
        "projection_candidate_count": (
            "INTEGER CHECK(projection_candidate_count IS NULL OR "
            "projection_candidate_count>=0)"
        ),
        "projection_exclusion_count": (
            "INTEGER CHECK(projection_exclusion_count IS NULL OR "
            "projection_exclusion_count>=0)"
        ),
        "projection_exclusion_counts_json": "TEXT",
        "projection_exclusion_roots_json": "TEXT",
    }
    progress_columns = {
        "projection_candidate_count": (
            "INTEGER NOT NULL DEFAULT 0 CHECK(projection_candidate_count>=0)"
        ),
        "projection_exclusion_count": (
            "INTEGER NOT NULL DEFAULT 0 CHECK(projection_exclusion_count>=0)"
        ),
        "projection_exclusion_counts_json": "TEXT NOT NULL DEFAULT '{}'",
        "projection_exclusion_roots_json": "TEXT NOT NULL DEFAULT '{}'",
        "projection_exclusion_digest_states_json": "TEXT NOT NULL DEFAULT '{}'",
    }
    if (
        set(receipt_columns) <= columns[tables[0]]
        and set(receipt_columns) <= columns[tables[1]]
        and set(progress_columns) <= columns[tables[2]]
    ):
        return
    con.execute("SAVEPOINT structure_drift_classifier_v3_exclusions_migration")
    try:
        con.execute("DROP TRIGGER IF EXISTS trg_structure_drift_receipt_update")
        con.execute("DROP TRIGGER IF EXISTS trg_structure_drift_receipt_delete")
        con.execute("DROP TRIGGER IF EXISTS trg_structure_drift_terminal_receipt_update")
        con.execute("DROP TRIGGER IF EXISTS trg_structure_drift_terminal_receipt_delete")
        con.execute("DROP TRIGGER IF EXISTS trg_structure_drift_terminal_receipt_insert")
        for column, ddl in receipt_columns.items():
            if column not in columns[tables[0]]:
                con.execute(
                    "ALTER TABLE structure_generation_drift_receipts "
                    f"ADD COLUMN {column} {ddl}"
                )
        if fault_hook is not None:
            fault_hook("after-authorization-columns")
        for column, ddl in receipt_columns.items():
            if column not in columns[tables[1]]:
                con.execute(
                    "ALTER TABLE structure_generation_drift_terminal_receipts "
                    f"ADD COLUMN {column} {ddl}"
                )
        if fault_hook is not None:
            fault_hook("after-terminal-columns")
        for column, ddl in progress_columns.items():
            if column not in columns[tables[2]]:
                con.execute(
                    "ALTER TABLE structure_generation_drift_progress "
                    f"ADD COLUMN {column} {ddl}"
                )
        if fault_hook is not None:
            fault_hook("after-progress-columns")
        con.execute(
            "CREATE TRIGGER trg_structure_drift_receipt_update BEFORE UPDATE ON "
            "structure_generation_drift_receipts BEGIN SELECT "
            "RAISE(ABORT,'structure-drift-receipt-sealed'); END"
        )
        con.execute(
            "CREATE TRIGGER trg_structure_drift_receipt_delete BEFORE DELETE ON "
            "structure_generation_drift_receipts BEGIN SELECT "
            "RAISE(ABORT,'structure-drift-receipt-sealed'); END"
        )
        con.execute(
            "CREATE TRIGGER trg_structure_drift_terminal_receipt_update BEFORE UPDATE ON "
            "structure_generation_drift_terminal_receipts BEGIN SELECT RAISE(ABORT,"
            "'structure-drift-terminal-receipt-sealed'); END"
        )
        con.execute(
            "CREATE TRIGGER trg_structure_drift_terminal_receipt_delete BEFORE DELETE ON "
            "structure_generation_drift_terminal_receipts BEGIN SELECT RAISE(ABORT,"
            "'structure-drift-terminal-receipt-sealed'); END"
        )
        con.execute(
            "CREATE TRIGGER trg_structure_drift_terminal_receipt_insert BEFORE INSERT ON "
            "structure_generation_drift_terminal_receipts WHEN EXISTS (SELECT 1 FROM "
            "structure_generation_drift_terminal_receipts WHERE comparison_id="
            "NEW.comparison_id) BEGIN SELECT RAISE(ABORT,"
            "'structure-drift-terminal-receipt-sealed'); END"
        )
        con.execute("RELEASE SAVEPOINT structure_drift_classifier_v3_exclusions_migration")
    except BaseException:
        con.execute(
            "ROLLBACK TO SAVEPOINT structure_drift_classifier_v3_exclusions_migration"
        )
        con.execute("RELEASE SAVEPOINT structure_drift_classifier_v3_exclusions_migration")
        raise


def _migrate_structure_drift_fresh_projection_phase(
    con: sqlite3.Connection,
    *,
    fault_hook: Callable[[str], None] | None = None,
) -> None:
    """Add the v2 sidecar phase to an existing authority table atomically."""
    row = con.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND "
        "name='structure_generation_drift_progress'"
    ).fetchone()
    if row is None or "fresh-projection-members" in str(row[0]):
        return
    old_sql = str(row[0])
    upgraded_sql = old_sql.replace(
        "'source-events','source-markets','generation-members'",
        "'source-events','source-markets','fresh-projection-members',"
        "'generation-members'",
        1,
    )
    if upgraded_sql == old_sql:
        raise ValueError("structure-drift-phase-schema-invalid")
    columns = [
        str(item[1])
        for item in con.execute(
            "PRAGMA table_info(structure_generation_drift_progress)"
        ).fetchall()
    ]
    con.execute("SAVEPOINT structure_drift_fresh_projection_phase_migration")
    try:
        con.execute("DROP INDEX IF EXISTS idx_structure_drift_progress_active")
        con.execute(
            "ALTER TABLE structure_generation_drift_progress RENAME TO "
            "structure_generation_drift_progress_before_projection"
        )
        if fault_hook is not None:
            fault_hook("after-fresh-projection-progress-rename")
        con.execute(upgraded_sql)
        column_sql = ",".join(columns)
        con.execute(
            "INSERT INTO structure_generation_drift_progress("
            + column_sql
            + ") SELECT "
            + column_sql
            + " FROM structure_generation_drift_progress_before_projection"
        )
        if fault_hook is not None:
            fault_hook("after-fresh-projection-progress-copy")
        con.execute(
            "DROP TABLE structure_generation_drift_progress_before_projection"
        )
        con.execute(
            "CREATE INDEX idx_structure_drift_progress_active ON "
            "structure_generation_drift_progress(checkpoint_at_ms DESC,comparison_id) "
            "WHERE phase NOT IN ('sealed','stale')"
        )
        if fault_hook is not None:
            fault_hook("after-fresh-projection-progress-index-create")
        con.execute("RELEASE SAVEPOINT structure_drift_fresh_projection_phase_migration")
    except BaseException:
        con.execute("ROLLBACK TO SAVEPOINT structure_drift_fresh_projection_phase_migration")
        con.execute("RELEASE SAVEPOINT structure_drift_fresh_projection_phase_migration")
        raise


def _migrate_structure_drift_member_receipt_binding(
    con: sqlite3.Connection,
) -> None:
    """Add the sidecar receipt cross-binding to every drift authority shape."""
    definitions = {
        "structure_generation_drift_progress": (
            "projection_member_receipt_digest TEXT CHECK("
            "projection_member_receipt_digest IS NULL OR "
            "length(projection_member_receipt_digest)=64)"
        ),
        "structure_generation_drift_receipts": (
            "projection_member_receipt_digest TEXT CHECK("
            "projection_member_receipt_digest IS NULL OR "
            "length(projection_member_receipt_digest)=64)"
        ),
        "structure_generation_drift_terminal_receipts": (
            "projection_member_receipt_digest TEXT CHECK("
            "projection_member_receipt_digest IS NULL OR "
            "length(projection_member_receipt_digest)=64)"
        ),
    }
    for table, definition in definitions.items():
        columns = {
            str(row[1]) for row in con.execute(f"PRAGMA table_info({table})")
        }
        if columns and "projection_member_receipt_digest" not in columns:
            con.execute(f"ALTER TABLE {table} ADD COLUMN {definition}")


def _install_structure_generation_freeze_triggers(con: sqlite3.Connection) -> None:
    """Reject every generation-row mutation after certification starts."""
    newer_floor = (
        "EXISTS (SELECT 1 FROM structure_publications newer WHERE "
        "newer.status='published' AND newer.published_at_ms>=0 AND "
        "newer.expected_counts_json=newer.committed_counts_json AND "
        "newer.certification_component IN "
        "('bounded-complete','backfill-authenticated') AND "
        "(newer.published_at_ms>p.published_at_ms OR "
        "(newer.published_at_ms=p.published_at_ms AND newer.snapshot_id>p.snapshot_id)) "
        "AND NOT EXISTS (SELECT 1 FROM structure_generation_cleanup_receipts reclaimed "
        "WHERE reclaimed.generation_snapshot_id=newer.snapshot_id) ORDER BY "
        "newer.published_at_ms,newer.snapshot_id LIMIT 1 OFFSET 1)"
    )
    con.execute("DROP TRIGGER IF EXISTS trg_structure_cleanup_progress_retention_floor")
    con.execute(
        "CREATE TRIGGER trg_structure_cleanup_progress_retention_floor BEFORE INSERT ON "
        "structure_generation_cleanup_progress WHEN EXISTS (SELECT 1 FROM "
        "structure_publications p WHERE p.snapshot_id=NEW.generation_snapshot_id AND "
        "p.publication_id=NEW.publication_id AND (EXISTS (SELECT 1 FROM "
        "current_structure_generation current WHERE "
        "current.snapshot_id=p.snapshot_id) OR NOT "
        f"{newer_floor})) BEGIN SELECT RAISE(ABORT,'cleanup-retention-floor'); END"
    )
    for component in _STRUCTURE_COMPONENTS:
        table = f"structure_generation_{component}"
        for operation, reference in (("insert", "NEW"), ("delete", "OLD")):
            trigger = f"trg_{table}_frozen_{operation}"
            # The v1 DELETE trigger predated evidence-aware reclamation. Replace
            # it transactionally so only an authenticated cleanup receipt can
            # authorize removal of old frozen bulk rows.
            if operation == "delete":
                con.execute(f"DROP TRIGGER IF EXISTS {trigger}")  # noqa: S608
            cleanup_guard = (
                " AND NOT EXISTS (SELECT 1 FROM "
                "structure_generation_cleanup_progress r JOIN structure_publications p "
                "ON p.snapshot_id=r.generation_snapshot_id AND "
                "p.publication_id=r.publication_id JOIN "
                "structure_generation_comparison_receipts cr ON "
                "cr.generation_snapshot_id=r.generation_snapshot_id AND "
                "cr.publication_id=r.publication_id WHERE "
                f"r.generation_snapshot_id={reference}.snapshot_id AND "
                f"r.phase='{component}' AND r.blocked_reason IS NULL AND "
                "r.authorization_digest=cr.receipt_digest AND "
                "cr.generation_validation_hash=p.validation_hash AND "
                "p.status='published' AND p.expected_counts_json=p.committed_counts_json "
                "AND p.certification_component IN "
                "('bounded-complete','backfill-authenticated') AND NOT EXISTS (SELECT 1 "
                "FROM current_structure_generation current WHERE "
                "current.snapshot_id=p.snapshot_id) AND "
                f"{newer_floor})"
                if operation == "delete"
                else ""
            )
            con.execute(
                f"CREATE TRIGGER IF NOT EXISTS {trigger} "
                f"BEFORE {operation.upper()} ON {table} WHEN EXISTS (SELECT 1 FROM "
                "structure_publications p WHERE "
                f"p.snapshot_id={reference}.snapshot_id AND "
                f"p.certification_component IS NOT NULL){cleanup_guard} "
                "BEGIN SELECT RAISE(ABORT,'structure-generation-frozen'); END"  # noqa: S608
            )
        con.execute(
            f"CREATE TRIGGER IF NOT EXISTS trg_{table}_frozen_update_v2 "
            f"BEFORE UPDATE ON {table} WHEN EXISTS (SELECT 1 FROM "
            "structure_publications p WHERE p.certification_component IS NOT NULL "
            "AND (p.snapshot_id=OLD.snapshot_id OR p.snapshot_id=NEW.snapshot_id)) "
            "BEGIN SELECT RAISE(ABORT,'structure-generation-frozen'); END"  # noqa: S608
        )
    for operation in ("update", "delete"):
        con.execute(
            f"CREATE TRIGGER IF NOT EXISTS trg_structure_cleanup_receipt_{operation} "
            f"BEFORE {operation.upper()} ON structure_generation_cleanup_receipts "
            "BEGIN SELECT RAISE(ABORT,'structure-cleanup-receipt-sealed'); END"
        )
        con.execute(
            f"CREATE TRIGGER IF NOT EXISTS trg_structure_cleanup_observation_{operation} "
            f"BEFORE {operation.upper()} ON structure_generation_cleanup_observations "
            "BEGIN SELECT RAISE(ABORT,'structure-cleanup-observation-sealed'); END"
        )


def _install_structure_comparison_receipt_triggers(
    con: sqlite3.Connection,
) -> None:
    """Make every digest-sealed comparison receipt append-only."""
    for operation in ("update", "delete"):
        con.execute(
            f"CREATE TRIGGER IF NOT EXISTS trg_structure_comparison_receipt_{operation} "
            f"BEFORE {operation.upper()} ON structure_generation_comparison_receipts "
            "WHEN OLD.receipt_digest IS NOT NULL "
            "BEGIN SELECT RAISE(ABORT,'structure-comparison-receipt-sealed'); END"
        )


def _comparison_receipt_digest(
    *,
    generation_snapshot_id: int,
    publication_id: str,
    legacy_snapshot_id: int,
    legacy_market_count: int,
    generation_market_count: int,
    legacy_universe_hash: str,
    generation_universe_hash: str,
    legacy_source_truth_hash: str,
    generation_source_truth_hash: str,
    generation_validation_hash: str,
    created_at_ms: int,
) -> str:
    """Authenticate every immutable comparison receipt field."""
    payload = (
        generation_snapshot_id,
        publication_id,
        legacy_snapshot_id,
        legacy_market_count,
        generation_market_count,
        legacy_universe_hash,
        generation_universe_hash,
        legacy_source_truth_hash,
        generation_source_truth_hash,
        generation_validation_hash,
        created_at_ms,
    )
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()


def _structure_event_source_receipt_digest(values: tuple[object, ...]) -> str:
    return hashlib.sha256(
        json.dumps(values, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()


def _structure_event_member_checkpoint_digest(values: tuple[object, ...]) -> str:
    return hashlib.sha256(
        json.dumps(values, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()


def _validated_structure_event_source_receipt(
    con: sqlite3.Connection, window_id: str,
) -> tuple[int, str, str, str] | None:
    row = con.execute(
        "SELECT window_id,event_count,event_root,terminal_event_pages,"
        "terminal_event_cursor,metadata_contract,sealed_at_ms,receipt_digest FROM "
        "structure_sync_event_source_receipts WHERE window_id=?", (window_id,),
    ).fetchone()
    if row is None:
        has_source_evidence = con.execute(
            "SELECT EXISTS(SELECT 1 FROM structure_sync_event_source_progress "
            "WHERE window_id=?) OR EXISTS(SELECT 1 FROM "
            "structure_sync_event_metadata_staging WHERE window_id=? LIMIT 1)",
            (window_id, window_id),
        ).fetchone()[0]
        if has_source_evidence:
            raise ValueError("structure-event-source-receipt-invalid")
        return None
    if row[5] != STRUCTURE_EVENT_SOURCE_CONTRACT:
        raise ValueError("structure-event-source-receipt-invalid")
    if row[7] != _structure_event_source_receipt_digest(tuple(row[:7])):
        raise ValueError("structure-event-source-receipt-invalid")
    window = con.execute(
        "SELECT status,event_cursor,event_pages FROM structure_sync_windows WHERE id=?",
        (window_id,),
    ).fetchone()
    progress = con.execute(
        "SELECT event_count,event_state FROM structure_sync_event_source_progress "
        "WHERE window_id=?", (window_id,),
    ).fetchone()
    if (
        window is None
        or window[0] not in {"events_complete", "complete", "published"}
        or progress is None
    ):
        raise ValueError("structure-event-source-receipt-invalid")
    chain = RowChainSHA256.from_json(str(progress[1]), expected_domain="source-event")
    if (
        int(progress[0]) != int(row[1])
        or chain.count != int(row[1])
        or chain.hexdigest() != row[2]
        or int(window[2]) != int(row[3])
        or str(window[1] or "") != str(row[4])
    ):
        raise ValueError("structure-event-source-receipt-invalid")
    identity = hashlib.sha256(json.dumps(
        (row[0], row[1], row[2], row[7]), separators=(",", ":")
    ).encode()).hexdigest()
    return int(row[1]), str(row[2]), identity, str(row[7])


_STRUCTURE_DRIFT_RECEIPT_DIGEST_FIELDS_V2 = (
    "comparison_id",
    "hash_algorithm",
    "classifier_contract_version",
    "legacy_snapshot_id",
    "legacy_taken_at_ms",
    "legacy_finished_at_ms",
    "legacy_market_count",
    "legacy_universe_hash",
    "legacy_source_truth_hash",
    "generation_snapshot_id",
    "publication_id",
    "window_id",
    "published_snapshot_id",
    "normalization_contract_version",
    "exact_receipt_digest",
    "pointer_validation_hash",
    "generation_certification_hash",
    "source_event_count",
    "source_market_count",
    "source_event_hash",
    "source_market_hash",
    "source_identity_hash",
    "projection_member_receipt_digest",
    "projection_universe_hash",
    "projection_group_truth_hash",
    "generation_universe_hash",
    "generation_group_truth_hash",
    "generation_projection_member_comparison_count",
    "generation_projection_member_comparison_root",
    "generation_source_group_truth_comparison_count",
    "generation_source_group_truth_comparison_root",
    "class_counts_json",
    "class_digests_json",
    "diagnostic_counts_json",
    "diagnostic_root",
    "diagnostic_samples_json",
    "diagnostic_samples_digest",
    "legacy_reconstruction_root",
    "generation_reconstruction_root",
    "overlap_conflict_count",
    "unclassified_count",
    "created_at_ms",
)
_STRUCTURE_DRIFT_RECEIPT_DIGEST_FIELDS_V3 = (
    *_STRUCTURE_DRIFT_RECEIPT_DIGEST_FIELDS_V2[:-1],
    "projection_candidate_count",
    "projection_exclusion_count",
    "projection_exclusion_counts_json",
    "projection_exclusion_roots_json",
    "created_at_ms",
)
# Compatibility for the v2-only write path replaced in the next task.
_STRUCTURE_DRIFT_RECEIPT_DIGEST_FIELDS = _STRUCTURE_DRIFT_RECEIPT_DIGEST_FIELDS_V2


def _canonical_tuple_sha256(values: tuple[object, ...]) -> str:
    return hashlib.sha256(
        json.dumps(values, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()


def _structure_drift_receipt_fields(contract: str) -> tuple[str, ...]:
    if contract in {STRUCTURE_DRIFT_CLASSIFIER_V1, STRUCTURE_DRIFT_CLASSIFIER_V2}:
        return _STRUCTURE_DRIFT_RECEIPT_DIGEST_FIELDS_V2
    if contract in STRUCTURE_DRIFT_CLASSIFIERS_V3_COMPATIBLE:
        return _STRUCTURE_DRIFT_RECEIPT_DIGEST_FIELDS_V3
    raise ValueError("invalid-structure-drift-classifier-contract")


def _structure_drift_receipt_digest(payload: Mapping[str, object]) -> str:
    """Authenticate every field in one sealed drift-safe authorization."""
    contract = str(payload.get("classifier_contract_version") or "")
    fields = _structure_drift_receipt_fields(contract)
    if set(payload) != set(fields):
        raise ValueError("invalid-structure-drift-receipt-fields")
    return _canonical_tuple_sha256(tuple(payload[field] for field in fields))


_STRUCTURE_DRIFT_TERMINAL_RECEIPT_DIGEST_FIELDS_V2 = (
    "comparison_id",
    "hash_algorithm",
    "classifier_contract_version",
    "legacy_snapshot_id",
    "generation_snapshot_id",
    "publication_id",
    "window_id",
    "normalization_contract_version",
    "exact_receipt_digest",
    "pointer_validation_hash",
    "generation_certification_hash",
    "source_identity_hash",
    "projection_member_receipt_digest",
    "terminal_reason",
    "class_counts_json",
    "class_digests_json",
    "diagnostic_counts_json",
    "diagnostic_root",
    "diagnostic_samples_json",
    "diagnostic_samples_digest",
    "created_at_ms",
    "checkpoint_at_ms",
)
_STRUCTURE_DRIFT_TERMINAL_RECEIPT_DIGEST_FIELDS_V3 = (
    *_STRUCTURE_DRIFT_TERMINAL_RECEIPT_DIGEST_FIELDS_V2[:-2],
    "projection_candidate_count",
    "projection_exclusion_count",
    "projection_exclusion_counts_json",
    "projection_exclusion_roots_json",
    "created_at_ms",
    "checkpoint_at_ms",
)
# Compatibility for the v2-only write path replaced in the next task.
_STRUCTURE_DRIFT_TERMINAL_RECEIPT_DIGEST_FIELDS = (
    _STRUCTURE_DRIFT_TERMINAL_RECEIPT_DIGEST_FIELDS_V2
)
_STRUCTURE_DRIFT_CLASS_TAGS = (
    "shared",
    "fresh-addition",
    "current-nontradable",
    "event-only-quarantine",
    "market-side-quarantine",
    "fresh-source-absent",
    "fresh-group-ineligible",
    "overlap-conflict",
    "unclassified",
)
_STRUCTURE_DRIFT_REMOVAL_CLASS_TAGS = (
    "current-nontradable",
    "event-only-quarantine",
    "market-side-quarantine",
    "fresh-source-absent",
    "fresh-group-ineligible",
)


def _validated_structure_drift_class_shape(
    class_counts_json: object,
    class_digests_json: object,
) -> tuple[dict[str, int], dict[str, str]]:
    try:
        class_counts = json.loads(str(class_counts_json))
        class_digests = json.loads(str(class_digests_json))
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        raise ValueError("structure-drift-class-evidence-invalid") from error
    valid = (
        isinstance(class_counts, dict)
        and set(class_counts) == set(_STRUCTURE_DRIFT_CLASS_TAGS)
        and all(
            type(class_counts[tag]) is int and class_counts[tag] >= 0
            for tag in _STRUCTURE_DRIFT_CLASS_TAGS
        )
        and isinstance(class_digests, dict)
        and set(class_digests)
        == {
            tag
            for tag in _STRUCTURE_DRIFT_CLASS_TAGS
            if class_counts[tag] > 0
        }
        and all(
            isinstance(root, str)
            and re.fullmatch(r"[0-9a-f]{64}", root) is not None
            for root in class_digests.values()
        )
    )
    if not valid:
        raise ValueError("structure-drift-class-evidence-invalid")
    return class_counts, class_digests


def _validated_structure_drift_class_evidence(
    class_counts_json: object,
    class_digests_json: object,
    *,
    expected_legacy_count: int,
    expected_generation_count: int,
) -> tuple[dict[str, int], dict[str, str]]:
    """Authenticate a sealed class partition against retained commitments."""
    from polyarb.perception.structure_drift import (
        reconstruction_root_from_class_commitments,
    )

    class_counts, class_digests = _validated_structure_drift_class_shape(
        class_counts_json,
        class_digests_json,
    )
    valid = (
        type(expected_legacy_count) is int
        and expected_legacy_count >= 0
        and type(expected_generation_count) is int
        and expected_generation_count >= 0
    )
    if not valid:
        raise ValueError("structure-drift-class-evidence-invalid")
    reconstruction_root_from_class_commitments(
        class_counts=class_counts,
        class_digests=class_digests,
        tags=(
            "shared",
            *_STRUCTURE_DRIFT_REMOVAL_CLASS_TAGS,
            "overlap-conflict",
            "unclassified",
        ),
        domain="legacy-reconstruction",
    )
    reconstruction_root_from_class_commitments(
        class_counts=class_counts,
        class_digests=class_digests,
        tags=(
            "shared",
            "fresh-addition",
            "overlap-conflict",
            "unclassified",
        ),
        domain="generation-reconstruction",
    )
    if class_counts["unclassified"] == 0:
        legacy_count = (
            class_counts["shared"]
            + sum(class_counts[tag] for tag in _STRUCTURE_DRIFT_REMOVAL_CLASS_TAGS)
            + class_counts["overlap-conflict"]
        )
        generation_count = (
            class_counts["shared"]
            + class_counts["fresh-addition"]
            + class_counts["overlap-conflict"]
        )
        if (
            legacy_count != expected_legacy_count
            or generation_count != expected_generation_count
        ):
            raise ValueError("structure-drift-class-evidence-invalid")
    return class_counts, class_digests


def _structure_drift_terminal_receipt_fields(contract: str) -> tuple[str, ...]:
    if contract in {STRUCTURE_DRIFT_CLASSIFIER_V1, STRUCTURE_DRIFT_CLASSIFIER_V2}:
        return _STRUCTURE_DRIFT_TERMINAL_RECEIPT_DIGEST_FIELDS_V2
    if contract in STRUCTURE_DRIFT_CLASSIFIERS_V3_COMPATIBLE:
        return _STRUCTURE_DRIFT_TERMINAL_RECEIPT_DIGEST_FIELDS_V3
    raise ValueError("invalid-structure-drift-classifier-contract")


def _structure_drift_terminal_receipt_digest(payload: Mapping[str, object]) -> str:
    """Authenticate every field in one immutable stale terminal receipt."""
    contract = str(payload.get("classifier_contract_version") or "")
    fields = _structure_drift_terminal_receipt_fields(contract)
    if set(payload) != set(fields):
        raise ValueError("invalid-structure-drift-terminal-receipt-fields")
    return _canonical_tuple_sha256(tuple(payload[field] for field in fields))


def _structure_drift_comparison_id(
    identity: tuple[object, ...], *, classifier_contract_version: str
) -> str:
    """Bind comparison identity to the classifier semantics that interpret it."""
    if classifier_contract_version not in {
        STRUCTURE_DRIFT_CLASSIFIER_V1,
        STRUCTURE_DRIFT_CLASSIFIER_V2,
        STRUCTURE_DRIFT_CLASSIFIER_V3,
        STRUCTURE_DRIFT_CLASSIFIER_V4,
    }:
        raise ValueError("invalid-structure-drift-classifier-contract")
    return hashlib.sha256(
        json.dumps(
            (*identity, classifier_contract_version),
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


def _fresh_projection_expected_candidate_count(
    con: sqlite3.Connection, *, window_id: str
) -> int:
    """Count the complete frozen market plus event-only projection source."""
    market_count = con.execute(
        "SELECT COUNT(*) FROM structure_sync_market_staging WHERE window_id=?",
        (window_id,),
    ).fetchone()[0]
    event_only_count = con.execute(
        "SELECT COUNT(*) FROM structure_sync_event_member_staging member "
        "WHERE member.window_id=? AND NOT EXISTS (SELECT 1 FROM "
        "structure_sync_market_staging market WHERE market.window_id=member.window_id "
        "AND market.market_id=member.market_id)",
        (window_id,),
    ).fetchone()[0]
    return int(market_count) + int(event_only_count)


def _bootstrap_rotation_digest(
    *,
    recovery_root_window_id: str,
    old_window_id: str,
    event_cursor: str,
    member_offset: int,
    blocked_reason: str,
    checkpoint_at_ms: int,
    successor_window_id: str,
    rotated_at_ms: int,
) -> str:
    payload = json.dumps(
        {
            "blocked_reason": blocked_reason,
            "checkpoint_at_ms": checkpoint_at_ms,
            "event_cursor": event_cursor,
            "member_offset": member_offset,
            "old_window_id": old_window_id,
            "recovery_root_window_id": recovery_root_window_id,
            "rotated_at_ms": rotated_at_ms,
            "successor_window_id": successor_window_id,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def _bootstrap_recovery_digest(
    *,
    recovery_root_window_id: str,
    successful_window_id: str,
    window_checkpoint_at_ms: int,
    completed_at_ms: int,
) -> str:
    payload = (
        recovery_root_window_id,
        successful_window_id,
        window_checkpoint_at_ms,
        completed_at_ms,
    )
    return hashlib.sha256(
        json.dumps(payload, separators=(",", ":")).encode()
    ).hexdigest()


def _generation_cleanup_digest(
    *,
    generation_snapshot_id: int,
    publication_id: str,
    component_counts_json: str,
    generation_validation_hash: str,
    reclaimed_at_ms: int,
) -> str:
    payload = (
        generation_snapshot_id,
        publication_id,
        component_counts_json,
        generation_validation_hash,
        reclaimed_at_ms,
    )
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()


def _generation_cleanup_observation_digest(
    *,
    generation_snapshot_id: int,
    publication_id: str,
    state: str,
    reason: str | None,
    observed_at_ms: int,
) -> str:
    return hashlib.sha256(
        json.dumps(
            (
                generation_snapshot_id,
                publication_id,
                state,
                reason,
                observed_at_ms,
            ),
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


def _append_generation_cleanup_observation(
    con: sqlite3.Connection,
    *,
    generation_snapshot_id: int,
    publication_id: str,
    state: str,
    reason: str | None,
    observed_at_ms: int,
) -> None:
    digest = _generation_cleanup_observation_digest(
        generation_snapshot_id=generation_snapshot_id,
        publication_id=publication_id,
        state=state,
        reason=reason,
        observed_at_ms=observed_at_ms,
    )
    con.execute(
        "INSERT INTO structure_generation_cleanup_observations("
        "generation_snapshot_id,publication_id,state,reason,observed_at_ms,"
        "observation_digest) VALUES (?,?,?,?,?,?)",
        (
            generation_snapshot_id,
            publication_id,
            state,
            reason,
            observed_at_ms,
            digest,
        ),
    )


def _active_generation_cleanup_authentication_error(
    con: sqlite3.Connection,
    *,
    snapshot_id: int,
    publication_id: str,
) -> str | None:
    """Revalidate the frozen proof skeleton before every destructive chunk."""
    publication = con.execute(
        "SELECT validation_hash,certification_hash,expected_counts_json,"
        "committed_counts_json,certification_component FROM structure_publications "
        "WHERE publication_id=? AND snapshot_id=? AND status='published'",
        (publication_id, snapshot_id),
    ).fetchone()
    receipt = con.execute(
        "SELECT legacy_snapshot_id,legacy_market_count,generation_market_count,"
        "legacy_universe_hash,generation_universe_hash,legacy_source_truth_hash,"
        "generation_source_truth_hash,generation_validation_hash,created_at_ms,"
        "receipt_digest FROM structure_generation_comparison_receipts "
        "WHERE generation_snapshot_id=? AND publication_id=?",
        (snapshot_id, publication_id),
    ).fetchone()
    if publication is None or receipt is None:
        return "generation-authentication-missing"
    progress_auth = con.execute(
        "SELECT authorization_digest FROM structure_generation_cleanup_progress "
        "WHERE generation_snapshot_id=? AND publication_id=?",
        (snapshot_id, publication_id),
    ).fetchone()
    if progress_auth is None or progress_auth[0] != receipt[9]:
        return "cleanup-authorization-digest-mismatch"
    expected_digest = _comparison_receipt_digest(
        generation_snapshot_id=snapshot_id,
        publication_id=publication_id,
        legacy_snapshot_id=int(receipt[0]),
        legacy_market_count=int(receipt[1]),
        generation_market_count=int(receipt[2]),
        legacy_universe_hash=str(receipt[3]),
        generation_universe_hash=str(receipt[4]),
        legacy_source_truth_hash=str(receipt[5]),
        generation_source_truth_hash=str(receipt[6]),
        generation_validation_hash=str(receipt[7]),
        created_at_ms=int(receipt[8]),
    )
    if receipt[9] != expected_digest:
        return "comparison-receipt-digest-mismatch"
    if publication[0] != publication[1] or receipt[7] != publication[0]:
        return "generation-validation-hash-mismatch"
    if publication[2] != publication[3] or publication[4] not in {
        "bounded-complete",
        "backfill-authenticated",
    }:
        return "generation-count-contract-mismatch"
    try:
        committed = json.loads(str(publication[3]))
    except (TypeError, ValueError):
        return "generation-count-contract-mismatch"
    if int(receipt[2]) != int(committed.get("markets", -1)):
        return "generation-count-contract-mismatch"
    return None


def _initialize_structure_comparison_progress(
    con: sqlite3.Connection,
    *,
    publication_id: str,
    snapshot_id: int,
    generation_market_count: int,
    now_ms: int,
) -> bool:
    """Pin legacy identity and create active bounded-comparison provenance."""
    legacy = con.execute(
        "SELECT s.id,s.taken_at_ms,s.finished_at_ms,s.market_count FROM snapshots s "
        "JOIN snapshot_source_coverage c ON c.snapshot_id=s.id AND c.completed=1 "
        "WHERE s.data_product='structure' AND s.market_view_published=1 "
        "AND s.is_valid=1 ORDER BY s.id DESC LIMIT 1"
    ).fetchone()
    if legacy is None:
        bootstrap = con.execute(
            "SELECT id,taken_at_ms,finished_at_ms FROM snapshots WHERE id=?",
            (snapshot_id,),
        ).fetchone()
        if bootstrap is None:
            return False
        legacy = (*bootstrap, generation_market_count)
    digest = SerializableSHA256.new()
    digest.update(b"[")
    con.execute(
        "INSERT OR IGNORE INTO structure_generation_comparison_progress("
        "publication_id,generation_snapshot_id,legacy_snapshot_id,"
        "legacy_taken_at_ms,legacy_finished_at_ms,legacy_market_count,phase,"
        "row_cursor_json,digest_state_json,phase_row_count,created_at_ms,"
        "checkpoint_at_ms) VALUES (?,?,?,?,?,?,'legacy-universe',NULL,?,0,?,?)",
        (
            publication_id,
            snapshot_id,
            int(legacy[0]),
            int(legacy[1]),
            int(legacy[2]),
            int(legacy[3]),
            digest.to_json(),
            now_ms,
            now_ms,
        ),
    )
    phase = con.execute(
        "SELECT phase FROM structure_generation_comparison_progress "
        "WHERE publication_id=? AND generation_snapshot_id=?",
        (publication_id, snapshot_id),
    ).fetchone()
    return phase is not None and phase[0] != "sealed"


def _structure_comparison_progress_is_resumable(
    progress: tuple[object, ...],
    current_legacy_identity: tuple[int, int, int, int] | None,
) -> bool:
    """Validate the bounded state needed to resume an authenticated comparison."""
    phase = str(progress[0])
    expected_cursor_size = 4 if phase.endswith("universe") else 3
    if type(progress[3]) is not int or progress[3] < 0:
        return False
    pinned_legacy = progress[8:12]
    if (
        current_legacy_identity is None
        or len(pinned_legacy) != 4
        or any(type(value) is not int for value in pinned_legacy)
        or tuple(pinned_legacy) != current_legacy_identity
    ):
        return False
    try:
        cursor = None if progress[1] is None else json.loads(str(progress[1]))
        SerializableSHA256.from_json(str(progress[2]))
    except (TypeError, ValueError, json.JSONDecodeError):
        return False
    if cursor is not None and (
        not isinstance(cursor, list)
        or len(cursor) != expected_cursor_size
        or not all(isinstance(value, str) for value in cursor)
    ):
        return False
    required_hashes = {
        "legacy-universe": 0,
        "generation-universe": 1,
        "legacy-rejections": 2,
        "generation-rejections": 3,
    }.get(phase)
    if required_hashes is None:
        return False
    return all(
        isinstance(value, str) and len(value) == 64
        for value in progress[5 : 5 + required_hashes]
    )


def _repair_current_structure_generation_authentication(
    con: sqlite3.Connection,
) -> None:
    """Repair only NULL authentication fields on a provable legacy pointer."""
    required = {
        "snapshots": {
            "id",
            "market_count",
            "data_product",
            "market_view_published",
            "is_valid",
        },
        "structure_publications": {
            "publication_id",
            "snapshot_id",
            "window_id",
            "status",
            "expected_counts_json",
            "committed_counts_json",
            "validation_hash",
            "certification_component",
            "certification_hash",
        },
        "current_structure_generation": {
            "id",
            "snapshot_id",
            "publication_id",
            "validation_hash",
            "counts_json",
            "certification_component",
            "comparison_receipt_digest",
        },
    }
    for table, columns in required.items():
        existing = {str(info[1]) for info in con.execute(f"PRAGMA table_info({table})")}
        if not columns <= existing:
            return
    row = con.execute(
        "SELECT g.snapshot_id,g.publication_id,g.validation_hash,g.counts_json,"
        "g.certification_component,g.comparison_receipt_digest,p.window_id,p.status,"
        "p.expected_counts_json,p.committed_counts_json,p.validation_hash,"
        "p.certification_component,p.certification_hash,s.market_count,"
        "s.data_product,s.market_view_published,s.is_valid "
        "FROM current_structure_generation g LEFT JOIN structure_publications p "
        "ON p.publication_id=g.publication_id AND p.snapshot_id=g.snapshot_id "
        "LEFT JOIN snapshots s ON s.id=g.snapshot_id WHERE g.id=1"
    ).fetchone()
    if row is None or any(value is None for value in row[6:17]):
        return
    try:
        expected = json.loads(str(row[8]))
        committed = json.loads(str(row[9]))
    except (TypeError, ValueError, json.JSONDecodeError):
        return
    marker = (
        "backfill-authenticated"
        if str(row[6]).startswith("backfill:")
        else "bounded-complete"
    )
    canonical_counts = json.dumps(committed, sort_keys=True, separators=(",", ":"))
    publication_valid = (
        row[7] == "published"
        and expected == committed
        and set(committed) == set(_STRUCTURE_COMPONENTS)
        and row[10] == row[12]
        and isinstance(row[10], str)
        and len(row[10]) == 64
        and row[11] == marker
        and int(row[13]) == int(committed["markets"])
        and row[14] == "structure"
        and int(row[15]) == 1
        and int(row[16]) == 1
    )
    if not publication_valid:
        return
    if not all(value is None for value in row[2:6]):
        return
    receipt = con.execute(
        "SELECT publication_id,legacy_snapshot_id,legacy_market_count,"
        "generation_market_count,legacy_universe_hash,generation_universe_hash,"
        "legacy_source_truth_hash,generation_source_truth_hash,"
        "generation_validation_hash,created_at_ms,receipt_digest "
        "FROM structure_generation_comparison_receipts WHERE generation_snapshot_id=?",
        (row[0],),
    ).fetchone()
    if receipt is None:
        finished_at_ms = con.execute(
            "SELECT finished_at_ms FROM snapshots WHERE id=?",
            (int(row[0]),),
        ).fetchone()
        if finished_at_ms is not None and _initialize_structure_comparison_progress(
            con,
            publication_id=str(row[1]),
            snapshot_id=int(row[0]),
            generation_market_count=int(committed["markets"]),
            now_ms=int(finished_at_ms[0]),
        ):
            con.execute(
                "UPDATE current_structure_generation SET validation_hash=?,"
                "counts_json=?,certification_component=? WHERE id=1 "
                "AND validation_hash IS NULL AND counts_json IS NULL "
                "AND certification_component IS NULL "
                "AND comparison_receipt_digest IS NULL",
                (row[10], canonical_counts, marker),
            )
        return
    if receipt[10] is None:
        return
    expected_digest = _comparison_receipt_digest(
        generation_snapshot_id=int(row[0]),
        publication_id=str(receipt[0]),
        legacy_snapshot_id=int(receipt[1]),
        legacy_market_count=int(receipt[2]),
        generation_market_count=int(receipt[3]),
        legacy_universe_hash=str(receipt[4]),
        generation_universe_hash=str(receipt[5]),
        legacy_source_truth_hash=str(receipt[6]),
        generation_source_truth_hash=str(receipt[7]),
        generation_validation_hash=str(receipt[8]),
        created_at_ms=int(receipt[9]),
    )
    legacy_snapshot = con.execute(
        "SELECT market_count FROM snapshots WHERE id=?",
        (int(receipt[1]),),
    ).fetchone()
    if (
        receipt[10] == expected_digest
        and receipt[0] == row[1]
        and legacy_snapshot is not None
        and int(receipt[2]) == int(legacy_snapshot[0])
        and int(receipt[3]) == int(committed["markets"])
        and receipt[8] == row[10]
    ):
        con.execute(
            "UPDATE current_structure_generation SET validation_hash=?,counts_json=?,"
            "certification_component=?,comparison_receipt_digest=? WHERE id=1 "
            "AND validation_hash IS NULL AND counts_json IS NULL "
            "AND certification_component IS NULL "
            "AND comparison_receipt_digest IS NULL",
            (row[10], canonical_counts, marker, expected_digest),
        )


class StructurePublicationCursorError(ValueError):
    """A bounded write did not continue the publication's durable cursor."""


class StructurePointerSwitchDeadlineError(ValueError):
    """Atomic generation-pointer transaction exceeded its authority budget."""


class StructurePublicationContractDeadlineError(ValueError):
    """Publication contract reconciliation exceeded its bounded writer budget."""


class StructureMembershipInvalidError(ValueError):
    """A bounded membership failure safe to expose across the child protocol."""

    ALLOWED_KINDS = frozenset(
        {
            "active-market-missing",
            "group-truth",
            "market-identity",
            "terminal-invariant",
        }
    )

    def __init__(self, membership_kind: str, identity: tuple[object, ...]) -> None:
        if membership_kind not in self.ALLOWED_KINDS:
            raise ValueError("invalid-membership-failure-kind")
        encoded = json.dumps(identity, separators=(",", ":"), ensure_ascii=True)
        self.membership_kind = membership_kind
        self.key_sha256 = hashlib.sha256(encoded.encode()).hexdigest()
        super().__init__("membership-invalid")


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
    comparison_receipt_digest: str | None = None
    pointer_bound: bool = False


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
            "g.certification_component,g.comparison_receipt_digest,"
            "cp.phase,"
            "s.taken_at_ms,s.finished_at_ms,s.market_count,"
            "p.window_id,p.expected_counts_json,p.committed_counts_json,p.validation_hash,"
            "p.certification_component,p.certification_hash FROM "
            "current_structure_generation g JOIN structure_publications p "
            "ON p.publication_id=g.publication_id AND p.snapshot_id=g.snapshot_id "
            "LEFT JOIN structure_generation_comparison_progress cp "
            "ON cp.publication_id=g.publication_id "
            "AND cp.generation_snapshot_id=g.snapshot_id "
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
        pointer_hash, pointer_counts, pointer_marker, pointer_receipt_digest = row[2:6]
        comparison_phase = row[6]
        metadata = row[7:]
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
        pointer_hash = pointer_counts = pointer_marker = pointer_receipt_digest = None
        comparison_phase = None
        metadata = row[:9]
    cleanup = con.execute(
        "SELECT CASE WHEN EXISTS (SELECT 1 FROM structure_generation_cleanup_progress "
        "WHERE generation_snapshot_id=?) THEN 'active' ELSE 'complete' END "
        "WHERE EXISTS (SELECT 1 FROM structure_generation_cleanup_progress "
        "WHERE generation_snapshot_id=?) OR EXISTS (SELECT 1 FROM "
        "structure_generation_cleanup_receipts WHERE generation_snapshot_id=?)",
        (resolved_id, resolved_id, resolved_id),
    ).fetchone()
    if cleanup is not None:
        raise StructureGenerationReadError(
            "generation-evidence-cleanup-active"
            if cleanup[0] == "active"
            else "generation-evidence-reclaimed"
        )
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
        active_repair = pointer_receipt_digest is None and comparison_phase in {
            "legacy-universe",
            "generation-universe",
            "legacy-rejections",
            "generation-rejections",
        }
        sealed_receipt = (
            isinstance(pointer_receipt_digest, str)
            and len(pointer_receipt_digest) == 64
        )
        if not (active_repair or sealed_receipt):
            raise StructureGenerationReadError("generation-identity-mismatch")
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
        pointer_receipt_digest,
        current,
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
        if generation.pointer_bound and generation.comparison_receipt_digest is None:
            reasons.append("comparison-receipt-missing")
        receipt = con.execute(
            "SELECT publication_id,legacy_snapshot_id,legacy_market_count,"
            "generation_market_count,legacy_universe_hash,generation_universe_hash,"
            "legacy_source_truth_hash,generation_source_truth_hash,"
            "generation_validation_hash,created_at_ms,receipt_digest "
            "FROM structure_generation_comparison_receipts "
            "WHERE generation_snapshot_id=?",
            (generation.snapshot_id,),
        ).fetchone()
        if reasons:
            receipt = None
        elif receipt is None:
            reasons.append("comparison-receipt-missing")
        elif receipt[10] != _comparison_receipt_digest(
            generation_snapshot_id=generation.snapshot_id,
            publication_id=str(receipt[0]),
            legacy_snapshot_id=int(receipt[1]),
            legacy_market_count=int(receipt[2]),
            generation_market_count=int(receipt[3]),
            legacy_universe_hash=str(receipt[4]),
            generation_universe_hash=str(receipt[5]),
            legacy_source_truth_hash=str(receipt[6]),
            generation_source_truth_hash=str(receipt[7]),
            generation_validation_hash=str(receipt[8]),
            created_at_ms=int(receipt[9]),
        ):
            reasons.append("comparison-receipt-digest-mismatch")
        elif (
            generation.pointer_bound
            and receipt[10] != generation.comparison_receipt_digest
        ):
            reasons.append("comparison-receipt-digest-mismatch")
        elif (
            receipt[0] != generation.publication_id
            or int(receipt[1]) != legacy.snapshot_id
        ):
            reasons.append("comparison-receipt-identity-mismatch")
        elif receipt[8] != generation.validation_hash:
            reasons.append("comparison-receipt-validation-hash-mismatch")
        elif (
            int(receipt[2]) != legacy.market_count
            or int(receipt[3]) != generation.market_count
        ):
            reasons.append("comparison-receipt-count-mismatch")
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
        int(receipt[3])
        if receipt is not None
        else (generation.market_count if generation is not None else None),
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
    *,
    trace_callback: Callable[[str], None] | None = None,
) -> StructureReadComparison:
    """Return the deterministic dual-read result consumed by strict health."""
    with structure_read_transaction(
        db_path,
        mode="compare",
        trace_callback=trace_callback,
    ) as read:
        assert read.comparison is not None
        return read.comparison


def compare_current_structure_drift(db_path: Path | str) -> dict[str, object]:
    """Read and authenticate the current exact or drift-safe authorization."""
    return SQLiteStore(Path(db_path)).structure_generation_drift_status()


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
class StructurePublicationContractReconciliation:
    publication_id: str
    compatible: bool
    superseded: bool


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
        "WHERE s.data_product='structure' AND s.snapshot_status='ok' "
        "AND (s.is_valid=0 OR EXISTS (SELECT 1 FROM validation_issues candidate "
        "WHERE candidate.snapshot_id=s.id AND candidate.layer=1)) "
        "AND NOT EXISTS (SELECT 1 FROM structure_publications p "
        "WHERE p.snapshot_id=s.id AND p.status IN ('writing','ready')) "
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
            SnapshotStatus.FAILED if not is_valid else determine_snapshot_status(issues)
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

    def __init__(
        self,
        db_path: Path,
        *,
        writer_timeout_s: float = SQLITE_BUSY_TIMEOUT_S,
    ) -> None:
        if writer_timeout_s <= 0:
            raise ValueError("writer_timeout_s must be positive")
        self._db_path = Path(db_path)
        self._writer_timeout_s = float(writer_timeout_s)

    def _connect_writer(self, *, timeout_s: float | None = None) -> sqlite3.Connection:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        con = sqlite3.connect(
            self._db_path,
            isolation_level=None,
            timeout=self._writer_timeout_s if timeout_s is None else timeout_s,
        )
        con.execute("PRAGMA foreign_keys=ON")
        return con

    def _connect_deadline_read(
        self, deadline_monotonic: float | None
    ) -> sqlite3.Connection:
        """Open one read handle which cannot outlive a cooperative slice."""
        if deadline_monotonic is None:
            return sqlite3.connect(self._db_path)
        remaining_s = deadline_monotonic - time.monotonic()
        if remaining_s <= 0:
            raise sqlite3.OperationalError("interrupted")
        con = sqlite3.connect(
            self._db_path,
            timeout=min(SQLITE_BUSY_TIMEOUT_S, max(0.001, remaining_s)),
        )
        con.set_progress_handler(
            lambda: int(time.monotonic() >= deadline_monotonic), 1_000
        )
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
            logger.info("sqlite-schema-stage stage=connected")
            # `PRAGMA journal_mode=WAL` can coordinate/checkpoint a large WAL.
            # Do it only for a fresh/non-WAL database; never embed it in the
            # ordinary startup DDL replay against the production volume.
            journal_mode = str(con.execute("PRAGMA journal_mode").fetchone()[0]).lower()
            if journal_mode != "wal":
                con.execute("PRAGMA journal_mode=WAL")
            con.execute("PRAGMA synchronous=NORMAL")
            con.executescript(
                DDL.removeprefix(
                    "\nPRAGMA journal_mode = WAL;\nPRAGMA synchronous = NORMAL;\n"
                )
            )
            logger.info("sqlite-schema-stage stage=base-ddl-complete")
            # This pre-hotfix trigger was too broad: generation publication
            # could clear an unrelated dirty legacy mutation. Legacy writers
            # now clear only at their explicit atomic COMMIT boundary.
            con.execute(
                "DROP TRIGGER IF EXISTS trg_structure_publication_clears_revision_dirty"
            )
            if migrate_fault_auth_finalize(con):
                con.executescript(DDL)
            if migrate_fault_intent_status(con):
                con.executescript(DDL)
            # Phase 02 Plan 02: scheduler_state singleton table
            con.executescript(SCHEDULER_STATE_DDL)
            # Parent-observed outcomes for isolated scheduler snapshot children.
            con.executescript(SNAPSHOT_ATTEMPTS_DDL)
            con.executescript(PRODUCER_ARBITRATION_DDL)
            con.executescript(STRUCTURE_DEFER_RECEIPTS_DDL)
            con.executescript(STRUCTURE_DRIFT_ATTEMPTS_DDL)
            con.executescript(STRUCTURE_SCHEDULE_ADJUSTMENTS_DDL)
            con.executescript(STRUCTURE_SYNC_WINDOWS_DDL)
            logger.info("sqlite-schema-stage stage=structure-sync-ddl-complete")
            for stage, migration in (
                ("recovery-authority", _migrate_structure_recovery_authority),
                ("event-market-progress", _migrate_structure_event_market_progress),
                ("event-member-schema", _migrate_structure_event_member_schema),
                ("drift-hash-v2", _migrate_structure_drift_hash_v2),
                ("drift-classifier-v2", _migrate_structure_drift_classifier_v2),
                ("drift-fresh-projection", _migrate_structure_drift_fresh_projection_phase),
                ("drift-member-receipt", _migrate_structure_drift_member_receipt_binding),
            ):
                logger.info(f"sqlite-schema-stage stage={stage}-start")
                migration(con)
                logger.info(f"sqlite-schema-stage stage={stage}-complete")
            con.executescript(STRUCTURE_GENERATIONS_DDL)
            _migrate_structure_drift_classifier_v3_exclusions(con)
            logger.info("sqlite-schema-stage stage=structure-migrations-complete")
            # ANALYZE scans the entire index and held the production daemon in
            # disk sleep for minutes on every restart.  Newly created/rebuilt
            # indexes still need planner statistics, but an existing stat row
            # proves that startup has already paid that one-time cost.
            for index_name in (
                "idx_structure_generation_memberships_drift_scan",
                "idx_event_market_memberships_drift_scan",
            ):
                stat_table_exists = con.execute(
                    "SELECT 1 FROM sqlite_master "
                    "WHERE type='table' AND name='sqlite_stat1'"
                ).fetchone()
                stat_exists = (
                    stat_table_exists is not None
                    and con.execute(
                        "SELECT 1 FROM sqlite_stat1 WHERE idx=?", (index_name,)
                    ).fetchone()
                    is not None
                )
                if not stat_exists:
                    con.execute(f"ANALYZE {index_name}")
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
            def _ensure_column(table: str, column: str, ddl: str) -> bool:
                rows = con.execute(f"PRAGMA table_info({table})").fetchall()
                existing = {r[1] for r in rows}
                if column not in existing:
                    con.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
                    logger.info(
                        f"sqlite_store: idempotent migration — ALTER {table} ADD COLUMN {column}"
                    )
                    return True
                return False

            _ensure_column("snapshots", "supabase_mirror_at_ms", "INTEGER")
            _ensure_column("snapshots", "parquet_r2_url", "TEXT")
            _ensure_column(
                "structure_sync_windows",
                "staging_reclaimed_at_ms",
                "INTEGER CHECK(staging_reclaimed_at_ms IS NULL OR "
                "staging_reclaimed_at_ms >= 0)",
            )
            _ensure_column(
                "neg_risk_quote_attempts",
                "quote_run_identity",
                "INTEGER",
            )
            _ensure_column(
                "neg_risk_quote_source_receipts",
                "leg_quote_digest",
                "TEXT NOT NULL DEFAULT ''",
            )
            # The first, unreleased receipt draft did not seal legs/quotes.
            # Never backfill it from mutable rows or fall back to a Structure
            # scan. Quarantine and remove it under the same authority used by
            # bounded purge; a fresh Quote run must independently recertify.
            con.execute(
                "INSERT OR IGNORE INTO neg_risk_quote_unsealed_receipts("
                "quote_run_id,reason) SELECT q.quote_run_id,'missing-leg-quote-digest' "
                "FROM neg_risk_quote_source_receipts q JOIN neg_risk_quote_runs r "
                "ON r.id=q.quote_run_id WHERE r.status='complete' "
                "AND q.leg_quote_digest=''"
            )
            con.execute(
                "INSERT OR IGNORE INTO neg_risk_quote_purge_authority(quote_run_id) "
                "SELECT quote_run_id FROM neg_risk_quote_unsealed_receipts"
            )
            con.execute(
                "DELETE FROM neg_risk_quote_source_receipts WHERE leg_quote_digest='' "
                "AND quote_run_id IN (SELECT quote_run_id "
                "FROM neg_risk_quote_unsealed_receipts)"
            )
            con.execute(
                "DELETE FROM neg_risk_quote_purge_authority WHERE quote_run_id IN ("
                "SELECT quote_run_id FROM neg_risk_quote_unsealed_receipts)"
            )
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
            snapshot_status_added = _ensure_column(
                "snapshots",
                "snapshot_status",
                "TEXT NOT NULL DEFAULT 'ok'",
            )
            _ensure_column("snapshot_attempts", "last_stage", "TEXT")
            _ensure_column("snapshot_attempts", "elapsed_ms", "INTEGER")
            _ensure_column("snapshot_attempts", "chunks_processed", "INTEGER")
            _ensure_column("snapshot_attempts", "stderr_bytes", "INTEGER")
            _ensure_column("snapshot_attempts", "stderr_sha256", "TEXT")
            _ensure_column("snapshot_attempts", "stderr_tail", "TEXT")
            _ensure_column(
                "structure_defer_receipts", "initialized_comparison_id", "TEXT"
            )
            _ensure_column(
                "structure_defer_receipts", "current_comparison_id", "TEXT"
            )
            _ensure_column(
                "structure_defer_receipts", "classifier_contract_version", "TEXT"
            )
            _ensure_column(
                "structure_drift_attempts",
                "identity_json",
                "TEXT NOT NULL DEFAULT '{}'",
            )
            _ensure_column(
                "structure_drift_attempts",
                "identity_digest",
                "TEXT NOT NULL DEFAULT "
                "'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'",
            )
            _ensure_column("structure_drift_attempts", "progress_id", "TEXT")
            _ensure_column("structure_drift_attempts", "finished_at_ms", "INTEGER")
            _ensure_column("structure_drift_attempts", "last_phase", "TEXT")
            _ensure_column("structure_drift_attempts", "chunks_processed", "INTEGER")
            _ensure_column("structure_drift_attempts", "rows_processed", "INTEGER")
            _ensure_column("structure_drift_attempts", "elapsed_ms", "INTEGER")
            _ensure_column("structure_drift_attempts", "failure_kind", "TEXT")
            _ensure_column("structure_drift_attempts", "stderr_bytes", "INTEGER")
            _ensure_column("structure_drift_attempts", "stderr_sha256", "TEXT")
            _ensure_column("structure_drift_attempts", "stderr_safe_marker", "TEXT")
            _ensure_column("structure_publications", "write_prior_cursor", "TEXT")
            _ensure_column(
                "structure_publications", "normalization_contract_version", "TEXT"
            )
            _ensure_column("structure_publications", "certification_component", "TEXT")
            _ensure_column("structure_publications", "certification_row_cursor", "TEXT")
            _ensure_column("structure_publications", "certification_hash", "TEXT")
            _ensure_column(
                "structure_publications", "certification_counts_json", "TEXT"
            )
            _ensure_column("current_structure_generation", "validation_hash", "TEXT")
            _ensure_column("current_structure_generation", "counts_json", "TEXT")
            _ensure_column(
                "current_structure_generation", "certification_component", "TEXT"
            )
            _ensure_column(
                "current_structure_generation", "comparison_receipt_digest", "TEXT"
            )
            _ensure_column(
                "structure_generation_comparison_receipts", "receipt_digest", "TEXT"
            )
            _ensure_column(
                "structure_generation_cleanup_progress",
                "authorization_digest",
                "TEXT",
            )
            _ensure_column("structure_sync_event_staging", "source_ordinal", "INTEGER")
            _ensure_column("structure_sync_market_staging", "source_ordinal", "INTEGER")
            _repair_current_structure_generation_authentication(con)
            _migrate_structure_cleanup_progress_binding(con)
            con.commit()
            logger.info("sqlite-schema-stage stage=additive-migrations-complete")
            _install_structure_generation_freeze_triggers(con)
            _install_structure_comparison_receipt_triggers(con)
            # This projects pre-column history only during the schema upgrade
            # that introduced ``snapshot_status``.  Re-running its joined,
            # ordered query on every daemon boot scans the whole production
            # snapshots table even when no legacy rows remain.
            if snapshot_status_added:
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

        logger.info("sqlite-schema-stage stage=opportunity-schema-start")
        OpportunityPerceptionStore(self._db_path).init_schema()
        logger.info("sqlite-schema-stage stage=complete")

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
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='snapshots'"
                ).fetchone()
                is not None
            )
            if has_snapshot_schema:
                # The ordinal access-path index belongs to the current DDL,
                # but pre-generation worker databases can still have the old
                # staging shape.  Add the dependency before executescript()
                # attempts to create that index; otherwise an old volume
                # cannot even reach its resumable migration path.
                for table in (
                    "structure_sync_event_staging",
                    "structure_sync_market_staging",
                ):
                    columns = {
                        str(row[1])
                        for row in con.execute(f"PRAGMA table_info({table})")
                    }
                    if columns and "source_ordinal" not in columns:
                        con.execute(
                            f"ALTER TABLE {table} ADD COLUMN source_ordinal INTEGER"
                        )
                con.executescript(STRUCTURE_SYNC_WINDOWS_DDL)
                window_columns = {
                    str(row[1])
                    for row in con.execute("PRAGMA table_info(structure_sync_windows)")
                }
                if "staging_reclaimed_at_ms" not in window_columns:
                    con.execute(
                        "ALTER TABLE structure_sync_windows ADD COLUMN "
                        "staging_reclaimed_at_ms INTEGER CHECK("
                        "staging_reclaimed_at_ms IS NULL OR "
                        "staging_reclaimed_at_ms >= 0)"
                    )
                _migrate_structure_recovery_authority(con)
                _migrate_structure_event_market_progress(con)
                _migrate_structure_event_member_schema(con)
                con.executescript(STRUCTURE_GENERATIONS_DDL)
                _migrate_structure_drift_classifier_v3_exclusions(con)
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
                    ("normalization_contract_version", "TEXT"),
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
                        str(row[1])
                        for row in con.execute(f"PRAGMA table_info({table})")
                    }
                    if "source_ordinal" not in columns:
                        con.execute(
                            f"ALTER TABLE {table} ADD COLUMN source_ordinal INTEGER"
                        )
                pointer_columns = {
                    str(row[1])
                    for row in con.execute(
                        "PRAGMA table_info(current_structure_generation)"
                    )
                }
                for column in (
                    "validation_hash",
                    "counts_json",
                    "certification_component",
                    "comparison_receipt_digest",
                ):
                    if column not in pointer_columns:
                        con.execute(
                            f"ALTER TABLE current_structure_generation ADD COLUMN {column} TEXT"
                        )
                receipt_columns = {
                    str(row[1])
                    for row in con.execute(
                        "PRAGMA table_info(structure_generation_comparison_receipts)"
                    )
                }
                if "receipt_digest" not in receipt_columns:
                    con.execute(
                        "ALTER TABLE structure_generation_comparison_receipts "
                        "ADD COLUMN receipt_digest TEXT"
                    )
                cleanup_columns = {
                    str(row[1])
                    for row in con.execute(
                        "PRAGMA table_info(structure_generation_cleanup_progress)"
                    )
                }
                if "authorization_digest" not in cleanup_columns:
                    con.execute(
                        "ALTER TABLE structure_generation_cleanup_progress "
                        "ADD COLUMN authorization_digest TEXT"
                    )
                _repair_current_structure_generation_authentication(con)
                _migrate_structure_cleanup_progress_binding(con)
                con.commit()
                _install_structure_generation_freeze_triggers(con)
                _install_structure_comparison_receipt_triggers(con)
                return
        finally:
            con.close()
        self.init_schema()

    def advance_structure_event_member_staging_chunk(
        self, *, window_id: str, limit: int = 500,
        inspection_callback: Callable[[int], None] | None = None,
        execution_deadline_s: float | None = None,
    ) -> dict[str, object]:
        """Derive and atomically checkpoint at most 500 raw event members."""
        if (
            not window_id
            or not 1 <= limit <= 500
            or (execution_deadline_s is not None and execution_deadline_s <= 0)
        ):
            raise ValueError("invalid-structure-event-member-advance")
        now_ms = int(time.time() * 1_000)
        deadline = (
            None
            if execution_deadline_s is None
            else time.monotonic() + execution_deadline_s
        )
        con = self._connect_writer()
        progress_sql = (
            "SELECT event_cursor,member_ordinal,rows_written,member_byte_offset,"
            "member_state,diagnostic_state,checkpoint_at_ms,completed_at_ms,"
            "failure_reason,member_character_offset,source_receipt_digest,"
            "parent_payload_hash,checkpoint_digest FROM "
            "structure_sync_event_member_progress WHERE window_id=?"
        )
        try:
            if deadline is not None:
                con.set_progress_handler(
                    lambda: int(time.monotonic() >= deadline), 1_000
                )
            con.execute("BEGIN IMMEDIATE")
            window = con.execute(
                "SELECT status,checkpoint_at_ms FROM structure_sync_windows WHERE id=?",
                (window_id,),
            ).fetchone()
            if window is None or window[0] not in {"complete", "published"}:
                raise ValueError("structure-sync-window-not-complete")
            source = _validated_structure_event_source_receipt(con, window_id)
            if source is None:
                con.execute("COMMIT")
                return {"sealed": False, "complete": False,
                        "state": "waiting-natural-window", "authenticated": True,
                        "reason": "structure-event-source-receipt-unavailable"}
            backfill = con.execute(
                "SELECT progress.window_checkpoint_at_ms,progress.completed_at_ms,"
                "progress.blocked_reason FROM "
                "structure_sync_event_market_backfill_progress progress WHERE "
                "progress.window_id=?",
                (window_id,),
            ).fetchone()
            if (
                backfill is None
                or int(backfill[0]) != int(window[1])
                or backfill[1] is None
                or backfill[2] is not None
            ):
                con.execute("COMMIT")
                return {
                    "sealed": False,
                    "complete": False,
                    "state": "waiting-event-market-backfill",
                    "authenticated": True,
                }
            progress = con.execute(progress_sql, (window_id,)).fetchone()
            if progress is None:
                con.execute("COMMIT")
                return {"sealed": False, "complete": False,
                        "reason": "structure-event-member-checkpoint-invalid"}
            con.execute("COMMIT")
            if progress[7] is not None:
                status = self.structure_event_member_status(window_id=window_id)
                return {**status, "rows_written": 0,
                        "member_ordinal": int(progress[1]), "complete": True}
            if progress[8] is not None:
                return {"sealed": False, "complete": False, "rows_written": 0,
                        "member_ordinal": max(0, int(progress[1]) - 1),
                        "failure_reason": str(progress[8])}
            cursor, ordinal, byte_offset = str(progress[0]), int(progress[1]), int(progress[3])
            character_offset = int(progress[9])
            expected_checkpoint = _structure_event_member_checkpoint_digest((
                progress[10], cursor, ordinal, character_offset, byte_offset,
                int(progress[2]), progress[11], progress[4], progress[5],
            ))
            if progress[10] != source[3] or progress[12] != expected_checkpoint:
                raise ValueError("structure-event-member-checkpoint-invalid")
            (
                chain, source_count, source_root, source_identity, checkpoint,
                phase, conflict_cursor, conflict_chain,
                merkle_level, merkle_cursor, merkle_width,
                merkle_pending_index, merkle_pending_hash, proof_cursor, proof_count,
            ) = (
                _read_event_member_progress_state(str(progress[4]))
            )
            invalid_chain = RowChainSHA256.from_json(
                str(progress[5]), expected_domain="diagnostic/unclassified"
            )
            if chain.count != int(progress[2]):
                raise ValueError("structure-event-member-progress-invalid")
            if phase == "complete":
                raise ValueError("structure-event-member-progress-invalid")
            if phase == "group-truth":
                return self._advance_structure_event_group_truth_chunk(
                    window_id=window_id,
                    limit=limit,
                    now_ms=now_ms,
                    deadline_monotonic=deadline,
                )
            if phase == "merkle":
                children = con.execute(
                    "SELECT node_index,node_hash FROM "
                    "structure_sync_event_conflict_merkle_nodes WHERE window_id=? "
                    "AND level=? AND node_index>? ORDER BY node_index LIMIT ?",
                    (window_id, merkle_level, merkle_cursor, limit),
                ).fetchall()
                if not children or int(children[0][0]) != merkle_cursor + 1:
                    raise ValueError("structure-event-conflict-summary-invalid")
                expected_indexes = list(range(
                    merkle_cursor + 1,
                    merkle_cursor + 1 + len(children),
                ))
                if [int(row[0]) for row in children] != expected_indexes:
                    raise ValueError("structure-event-conflict-summary-invalid")
                level_complete = int(children[-1][0]) == merkle_width - 1
                if not level_complete and len(children) != limit:
                    raise ValueError("structure-event-conflict-summary-invalid")
                combined_children = (
                    [(merkle_pending_index, merkle_pending_hash)] + list(children)
                    if merkle_pending_index >= 0 else list(children)
                )
                if merkle_pending_index >= 0:
                    pending_node = con.execute(
                        "SELECT node_hash FROM "
                        "structure_sync_event_conflict_merkle_nodes WHERE window_id=? "
                        "AND level=? AND node_index=?",
                        (window_id, merkle_level, merkle_pending_index),
                    ).fetchone()
                    if (
                        pending_node is None
                        or str(pending_node[0]) != merkle_pending_hash
                        or int(children[0][0]) != merkle_pending_index + 1
                    ):
                        raise ValueError("structure-event-conflict-summary-invalid")
                pairable_count = len(combined_children)
                if not level_complete and pairable_count % 2:
                    pairable_count -= 1
                parent_nodes = []
                for offset in range(0, pairable_count, 2):
                    left_index, left_hash = combined_children[offset]
                    right = (
                        combined_children[offset + 1]
                        if offset + 1 < len(combined_children)
                        else (left_index, left_hash)
                    )
                    parent_nodes.append((
                        window_id,
                        merkle_level + 1,
                        int(left_index) // 2,
                        _event_conflict_merkle_parent(
                            str(left_hash), str(right[1])
                        ),
                    ))
                pending = (
                    combined_children[-1]
                    if not level_complete and pairable_count < len(combined_children)
                    else (-1, "")
                )
                next_width = (merkle_width + 1) // 2
                next_phase = (
                    "proofs" if level_complete and next_width == 1 else "merkle"
                )
                next_level = merkle_level + 1 if level_complete else merkle_level
                next_cursor = -1 if level_complete else int(children[-1][0])
                next_state = _event_member_progress_state(
                    member_chain=chain,
                    source_event_count=source_count,
                    source_event_root=source_root,
                    source_identity_hash=source_identity,
                    window_checkpoint_at_ms=checkpoint,
                    phase=next_phase,
                    conflict_cursor=conflict_cursor,
                    event_conflict_chain=conflict_chain,
                    merkle_level=next_level,
                    merkle_cursor=next_cursor,
                    merkle_width=next_width if level_complete else merkle_width,
                    merkle_pending_index=-1 if level_complete else int(pending[0]),
                    merkle_pending_hash="" if level_complete else str(pending[1]),
                    proof_cursor=proof_cursor,
                    proof_count=proof_count,
                )
                checkpoint_digest = _structure_event_member_checkpoint_digest((
                    source[3], progress[0], int(progress[1]), int(progress[9]),
                    int(progress[3]), int(progress[2]), progress[11], next_state,
                    progress[5],
                ))
                con.execute("BEGIN IMMEDIATE")
                if con.execute(progress_sql, (window_id,)).fetchone() != progress:
                    raise ValueError("structure-event-member-cursor-race")
                con.executemany(
                    "INSERT INTO structure_sync_event_conflict_merkle_nodes VALUES "
                    "(?,?,?,?)",
                    parent_nodes,
                )
                updated = con.execute(
                    "UPDATE structure_sync_event_member_progress SET member_state=?,"
                    "checkpoint_at_ms=?,checkpoint_digest=? WHERE window_id=? AND "
                    "completed_at_ms IS NULL AND failure_reason IS NULL",
                    (next_state, now_ms, checkpoint_digest, window_id),
                )
                if updated.rowcount != 1:
                    raise ValueError("structure-event-member-cursor-race")
                con.execute("COMMIT")
                return {
                    "sealed": False,
                    "complete": False,
                    "rows_written": len(children),
                    "state": "sealing-conflict-merkle",
                }
            if phase == "proofs":
                root_row = con.execute(
                    "SELECT node_hash FROM structure_sync_event_conflict_merkle_nodes "
                    "WHERE window_id=? AND level=? AND node_index=0",
                    (window_id, merkle_level),
                ).fetchone()
                if root_row is None:
                    raise ValueError("structure-event-conflict-summary-invalid")
                merkle_root = str(root_row[0])
                proof_rows = con.execute(
                    "SELECT proof.event_id,proof.leaf_index,proof.leaf_hash,"
                    "summary.global_conflict FROM structure_sync_event_conflict_proofs "
                    "proof JOIN structure_sync_event_conflict_summaries summary ON "
                    "summary.window_id=proof.window_id AND "
                    "summary.event_id=proof.event_id WHERE proof.window_id=? AND "
                    "proof.event_id>? ORDER BY proof.event_id LIMIT ?",
                    (window_id, proof_cursor, limit),
                ).fetchall()
                proof_updates = []
                for event_id, leaf_index, leaf_hash, global_conflict in proof_rows:
                    expected_leaf = _event_conflict_leaf_hash(
                        window_id=window_id,
                        event_id=str(event_id),
                        global_conflict=bool(global_conflict),
                    )
                    if str(leaf_hash) != expected_leaf:
                        raise ValueError("structure-event-conflict-summary-invalid")
                    node_index = int(leaf_index)
                    proof = []
                    for level in range(merkle_level):
                        sibling_index = node_index ^ 1
                        sibling = con.execute(
                            "SELECT node_hash FROM "
                            "structure_sync_event_conflict_merkle_nodes WHERE "
                            "window_id=? AND level=? AND node_index=?",
                            (window_id, level, sibling_index),
                        ).fetchone()
                        if sibling is None:
                            sibling = con.execute(
                                "SELECT node_hash FROM "
                                "structure_sync_event_conflict_merkle_nodes WHERE "
                                "window_id=? AND level=? AND node_index=?",
                                (window_id, level, node_index),
                            ).fetchone()
                        if sibling is None:
                            raise ValueError("structure-event-conflict-summary-invalid")
                        proof.append((
                            "left" if sibling_index < node_index else "right",
                            str(sibling[0]),
                        ))
                        node_index //= 2
                    proof_updates.append((
                        json.dumps(proof, separators=(",", ":")),
                        window_id,
                        str(event_id),
                    ))
                next_proof_cursor = (
                    proof_cursor if not proof_rows else str(proof_rows[-1][0])
                )
                proofs_complete = con.execute(
                    "SELECT 1 FROM structure_sync_event_conflict_proofs WHERE "
                    "window_id=? AND event_id>? LIMIT 1",
                    (window_id, next_proof_cursor),
                ).fetchone() is None
                next_proof_count = proof_count + len(proof_rows)
                if proofs_complete and next_proof_count != source_count:
                    raise ValueError("structure-event-conflict-summary-invalid")
                next_state = _event_member_progress_state(
                    member_chain=chain,
                    source_event_count=source_count,
                    source_event_root=source_root,
                    source_identity_hash=source_identity,
                    window_checkpoint_at_ms=checkpoint,
                    phase="complete" if proofs_complete else "proofs",
                    conflict_cursor=conflict_cursor,
                    event_conflict_chain=conflict_chain,
                    merkle_level=merkle_level,
                    merkle_cursor=merkle_cursor,
                    merkle_width=merkle_width,
                    merkle_pending_index=merkle_pending_index,
                    merkle_pending_hash=merkle_pending_hash,
                    proof_cursor=next_proof_cursor,
                    proof_count=next_proof_count,
                )
                checkpoint_digest = _structure_event_member_checkpoint_digest((
                    source[3], progress[0], int(progress[1]), int(progress[9]),
                    int(progress[3]), int(progress[2]), progress[11], next_state,
                    progress[5],
                ))
                con.execute("BEGIN IMMEDIATE")
                if con.execute(progress_sql, (window_id,)).fetchone() != progress:
                    raise ValueError("structure-event-member-cursor-race")
                con.executemany(
                    "UPDATE structure_sync_event_conflict_proofs SET proof_json=? "
                    "WHERE window_id=? AND event_id=?",
                    proof_updates,
                )
                if proofs_complete:
                    group_count, group_root = _validated_structure_event_group_truth(
                        con, window_id
                    )
                    receipt = (
                        window_id, source_count, source_root, source_identity,
                        STRUCTURE_EVENT_MEMBER_METADATA_CONTRACT, chain.count,
                        chain.hexdigest(), invalid_chain.count,
                        invalid_chain.hexdigest(), str(progress[0]), int(progress[1]),
                        int(progress[3]), now_ms,
                    )
                    receipt_digest = _structure_event_member_receipt_digest(
                        receipt,
                        event_conflict_count=conflict_chain.count,
                        event_conflict_root=conflict_chain.hexdigest(),
                        event_conflict_merkle_root=merkle_root,
                        source_group_truth_count=group_count,
                        source_group_truth_root=group_root,
                    )
                    con.execute(
                        "INSERT INTO structure_sync_event_member_receipts VALUES ("
                        + ",".join("?" for _ in range(19)) + ")",
                        (*receipt, receipt_digest, conflict_chain.count,
                         conflict_chain.hexdigest(), merkle_root, group_count, group_root),
                    )
                updated = con.execute(
                    "UPDATE structure_sync_event_member_progress SET member_state=?,"
                    "checkpoint_at_ms=?,completed_at_ms=?,checkpoint_digest=? WHERE "
                    "window_id=? AND completed_at_ms IS NULL AND failure_reason IS NULL",
                    (next_state, now_ms, now_ms if proofs_complete else None,
                     checkpoint_digest, window_id),
                )
                if updated.rowcount != 1:
                    raise ValueError("structure-event-member-cursor-race")
                con.execute("COMMIT")
                return {
                    "sealed": proofs_complete,
                    "complete": proofs_complete,
                    "rows_written": len(proof_rows),
                    "state": "sealed" if proofs_complete else "sealing-conflict-proofs",
                }
            if phase == "conflicts":
                conflict_rows = con.execute(
                    "SELECT event_id,global_conflict FROM "
                    "structure_sync_event_conflict_summaries WHERE window_id=? "
                    "AND event_id>? ORDER BY event_id LIMIT ?",
                    (window_id, conflict_cursor, limit),
                ).fetchall()
                first_leaf_index = conflict_chain.count
                scanned_leaves = [
                    _event_conflict_leaf_hash(
                        window_id=window_id,
                        event_id=str(event_id),
                        global_conflict=bool(global_conflict),
                    )
                    for event_id, global_conflict in conflict_rows
                ]
                for event_id, global_conflict in conflict_rows:
                    conflict_chain.update((
                        "structure-event-global-conflict-v1",
                        str(event_id),
                        bool(global_conflict),
                    ))
                next_conflict_cursor = (
                    conflict_cursor
                    if not conflict_rows else str(conflict_rows[-1][0])
                )
                summaries_complete = con.execute(
                    "SELECT 1 FROM structure_sync_event_conflict_summaries "
                    "WHERE window_id=? AND event_id>? LIMIT 1",
                    (window_id, next_conflict_cursor),
                ).fetchone() is None
                if summaries_complete and conflict_chain.count != source_count:
                    raise ValueError("structure-event-conflict-summary-invalid")
                merkle_root = ""
                conflict_proofs = [
                    (
                        window_id,
                        str(row[0]),
                        first_leaf_index + index,
                        scanned_leaves[index],
                        "",
                    )
                    for index, row in enumerate(conflict_rows)
                ]
                conflict_complete = summaries_complete and source_count <= 500
                if conflict_complete:
                    all_conflicts = con.execute(
                        "SELECT event_id,global_conflict FROM "
                        "structure_sync_event_conflict_summaries WHERE window_id=? "
                        "ORDER BY event_id",
                        (window_id,),
                    ).fetchall()
                    leaves = [
                        _event_conflict_leaf_hash(
                            window_id=window_id,
                            event_id=str(event_id),
                            global_conflict=bool(global_conflict),
                        )
                        for event_id, global_conflict in all_conflicts
                    ]
                    merkle_root, proofs = _event_conflict_merkle_proofs(leaves)
                    complete_proofs = [
                        (window_id, str(row[0]), index, leaves[index], proofs[index])
                        for index, row in enumerate(all_conflicts)
                    ]
                    conflict_proofs = complete_proofs
                next_state = _event_member_progress_state(
                    member_chain=chain,
                    source_event_count=source_count,
                    source_event_root=source_root,
                    source_identity_hash=source_identity,
                    window_checkpoint_at_ms=checkpoint,
                    phase=(
                        "complete" if conflict_complete
                        else "merkle" if summaries_complete else "conflicts"
                    ),
                    conflict_cursor=next_conflict_cursor,
                    event_conflict_chain=conflict_chain,
                    merkle_width=source_count if summaries_complete else 0,
                )
                checkpoint_digest = _structure_event_member_checkpoint_digest((
                    source[3], progress[0], int(progress[1]), int(progress[9]),
                    int(progress[3]), int(progress[2]), progress[11], next_state,
                    progress[5],
                ))
                con.execute("BEGIN IMMEDIATE")
                if con.execute(progress_sql, (window_id,)).fetchone() != progress:
                    raise ValueError("structure-event-member-cursor-race")
                con.executemany(
                    "INSERT INTO structure_sync_event_conflict_proofs VALUES (?,?,?,?,?) "
                    "ON CONFLICT(window_id,event_id) DO UPDATE SET "
                    "proof_json=excluded.proof_json",
                    conflict_proofs,
                )
                con.executemany(
                    "INSERT OR IGNORE INTO structure_sync_event_conflict_merkle_nodes VALUES "
                    "(?,0,?,?)",
                    [
                        (window_id, first_leaf_index + index, leaf_hash)
                        for index, leaf_hash in enumerate(scanned_leaves)
                    ],
                )
                if conflict_complete:
                    group_count, group_root = _validated_structure_event_group_truth(
                        con, window_id
                    )
                    receipt = (
                        window_id, source_count, source_root, source_identity,
                        STRUCTURE_EVENT_MEMBER_METADATA_CONTRACT, chain.count,
                        chain.hexdigest(), invalid_chain.count,
                        invalid_chain.hexdigest(), str(progress[0]), int(progress[1]),
                        int(progress[3]), now_ms,
                    )
                    receipt_digest = _structure_event_member_receipt_digest(
                        receipt,
                        event_conflict_count=conflict_chain.count,
                        event_conflict_root=conflict_chain.hexdigest(),
                        event_conflict_merkle_root=merkle_root,
                        source_group_truth_count=group_count,
                        source_group_truth_root=group_root,
                    )
                    con.execute(
                        "INSERT INTO structure_sync_event_member_receipts VALUES ("
                        + ",".join("?" for _ in range(19)) + ")",
                        (*receipt, receipt_digest, conflict_chain.count,
                         conflict_chain.hexdigest(), merkle_root, group_count, group_root),
                    )
                updated = con.execute(
                    "UPDATE structure_sync_event_member_progress SET member_state=?,"
                    "checkpoint_at_ms=?,completed_at_ms=?,checkpoint_digest=? WHERE "
                    "window_id=? AND completed_at_ms IS NULL AND failure_reason IS NULL",
                    (next_state, now_ms, now_ms if conflict_complete else None,
                     checkpoint_digest, window_id),
                )
                if updated.rowcount != 1:
                    raise ValueError("structure-event-member-cursor-race")
                con.execute("COMMIT")
                return {
                    "sealed": conflict_complete,
                    "complete": conflict_complete,
                    "rows_written": len(conflict_rows),
                    "member_ordinal": int(progress[1]),
                    "member_byte_offset": int(progress[3]),
                    "state": (
                        "sealed" if conflict_complete
                        else "sealing-conflict-merkle" if summaries_complete
                        else "sealing-conflicts"
                    ),
                }
            rows = []
            remaining = limit
            terminal_ordinal, terminal_offset = max(0, ordinal - 1), byte_offset
            terminal_character_offset = character_offset
            parent_hash = str(progress[11])
            try:
                for _ in range(limit):
                    if not remaining:
                        break
                    sql = (
                        "SELECT event.event_id,COALESCE(event.source_ordinal,event.rowid),"
                        "event.payload_json,metadata.event_group_id,metadata.payload_hash FROM "
                        "structure_sync_event_staging event JOIN "
                        "structure_sync_event_metadata_staging metadata ON "
                        "metadata.window_id=event.window_id AND metadata.event_id=event.event_id "
                        "WHERE event.window_id=? AND event.event_id=?"
                        if ordinal else
                        "SELECT event.event_id,COALESCE(event.source_ordinal,event.rowid),"
                        "event.payload_json,metadata.event_group_id,metadata.payload_hash FROM "
                        "structure_sync_event_staging event JOIN "
                        "structure_sync_event_metadata_staging metadata ON "
                        "metadata.window_id=event.window_id AND metadata.event_id=event.event_id "
                        "WHERE event.window_id=? AND event.event_id>? "
                        "ORDER BY event.event_id LIMIT 1"
                    )
                    event = con.execute(sql, (window_id, cursor)).fetchone()
                    if event is None:
                        break
                    event_id, event_order, payload = str(event[0]), int(event[1]), str(event[2])
                    event_group_id = event[3]
                    parent_hash = str(event[4])
                    if ordinal and progress[11] != parent_hash:
                        raise ValueError("structure-event-member-parent-mismatch")
                    batch = decode_event_member_batch(
                        payload, member_ordinal=ordinal,
                        member_byte_offset=byte_offset,
                        member_character_offset=character_offset,
                        limit=remaining,
                    )
                    for item_ordinal, member, _raw in batch.members:
                        if inspection_callback is not None:
                            inspection_callback(item_ordinal)
                        row = extract_structure_event_member_row(
                            window_id=window_id, event_id=event_id,
                            event_ordinal=event_order,
                            member_ordinal=item_ordinal, member=member,
                            event_group_id=event_group_id,
                        )
                        commitment = (
                            STRUCTURE_EVENT_MEMBER_METADATA_CONTRACT, event_id,
                            event_order, item_ordinal, row.market_id, row.group_id,
                            row.member_kind, row.active, row.closed, row.payload_hash,
                        )
                        chain.update(commitment)
                        if any(value is None for value in (
                            row.market_id, row.group_id, row.member_kind,
                            row.active, row.closed,
                        )):
                            invalid_chain.update(commitment)
                        rows.append(row)
                    remaining -= len(batch.members)
                    cursor, terminal_offset = event_id, batch.next_byte_offset
                    terminal_character_offset = batch.next_character_offset
                    character_offset = batch.next_character_offset
                    if batch.members:
                        terminal_ordinal = batch.members[-1][0]
                    if batch.complete:
                        ordinal, byte_offset, character_offset = 0, 0, 0
                    else:
                        ordinal, byte_offset = batch.next_member_ordinal, batch.next_byte_offset
                        break
            except ValueError as error:
                reason = str(error)[:200]
                con.execute("BEGIN IMMEDIATE")
                if con.execute(progress_sql, (window_id,)).fetchone() != progress:
                    raise ValueError("structure-event-member-cursor-race")
                updated = con.execute(
                    "UPDATE structure_sync_event_member_progress SET "
                    "checkpoint_at_ms=?,failure_reason=? WHERE window_id=? "
                    "AND completed_at_ms IS NULL AND failure_reason IS NULL",
                    (now_ms, reason, window_id),
                )
                if updated.rowcount != 1:
                    raise ValueError("structure-event-member-cursor-race")
                con.execute("COMMIT")
                return {"sealed": False, "complete": False, "rows_written": 0,
                        "member_ordinal": max(0, int(progress[1]) - 1),
                        "failure_reason": reason}
            complete = ordinal == 0 and con.execute(
                "SELECT 1 FROM structure_sync_event_staging WHERE window_id=? "
                "AND event_id>? LIMIT 1", (window_id, cursor),
            ).fetchone() is None
            conflict_complete = False
            summaries_complete = False
            conflict_rows: list[tuple[object, ...]] = []
            merkle_root = ""
            conflict_proofs: list[tuple[object, ...]] = []
            if complete and phase == "conflicts":
                conflict_rows = con.execute(
                    "SELECT event_id,global_conflict FROM "
                    "structure_sync_event_conflict_summaries WHERE window_id=? "
                    "ORDER BY event_id LIMIT ?",
                    (window_id, limit),
                ).fetchall()
                first_leaf_index = conflict_chain.count
                scanned_leaves = [
                    _event_conflict_leaf_hash(
                        window_id=window_id,
                        event_id=str(event_id),
                        global_conflict=bool(global_conflict),
                    )
                    for event_id, global_conflict in conflict_rows
                ]
                for event_id, global_conflict in conflict_rows:
                    conflict_chain.update((
                        "structure-event-global-conflict-v1",
                        str(event_id),
                        bool(global_conflict),
                    ))
                summaries_complete = (
                    conflict_chain.count == source_count
                    and len(conflict_rows) <= limit
                )
                if conflict_chain.count > source_count:
                    raise ValueError("structure-event-conflict-summary-invalid")
                conflict_proofs = [
                    (
                        window_id,
                        str(row[0]),
                        first_leaf_index + index,
                        scanned_leaves[index],
                        "",
                    )
                    for index, row in enumerate(conflict_rows)
                ]
                conflict_complete = summaries_complete and source_count <= 500
                if conflict_complete:
                    leaves = [
                        _event_conflict_leaf_hash(
                            window_id=window_id,
                            event_id=str(event_id),
                            global_conflict=bool(global_conflict),
                        )
                        for event_id, global_conflict in conflict_rows
                    ]
                    merkle_root, proofs = _event_conflict_merkle_proofs(leaves)
                    conflict_proofs = [
                        (window_id, str(row[0]), index, leaves[index], proofs[index])
                        for index, row in enumerate(conflict_rows)
                    ]
            next_state = _event_member_progress_state(
                member_chain=chain, source_event_count=source_count,
                source_event_root=source_root, source_identity_hash=source_identity,
                window_checkpoint_at_ms=checkpoint,
                phase=(
                    "complete" if conflict_complete
                    else "merkle" if summaries_complete
                    else "group-truth" if complete else "members"
                ),
                conflict_cursor=(
                    str(conflict_rows[-1][0]) if conflict_rows else ""
                ),
                event_conflict_chain=conflict_chain,
                merkle_width=source_count if summaries_complete else 0,
            )
            con.execute("BEGIN IMMEDIATE")
            if con.execute(progress_sql, (window_id,)).fetchone() != progress:
                raise ValueError("structure-event-member-cursor-race")
            identity = con.execute(
                "SELECT status FROM structure_sync_windows WHERE id=?",
                (window_id,),
            ).fetchone()
            if identity is None or identity[0] not in {"complete", "published"}:
                raise ValueError("structure-event-member-source-identity-drift")
            con.executemany(
                "INSERT INTO structure_sync_event_member_staging VALUES "
                "(?,?,?,?,?,?,?,?,?,?,?,?)",
                [(r.window_id, r.event_id, r.event_ordinal, r.member_ordinal,
                  r.market_id, r.market_sort_key, r.group_id, r.member_kind,
                  r.active, r.closed, r.payload_json, r.payload_hash) for r in rows],
            )
            if conflict_proofs:
                con.executemany(
                    "INSERT INTO structure_sync_event_conflict_proofs VALUES (?,?,?,?,?) "
                    "ON CONFLICT(window_id,event_id) DO UPDATE SET "
                    "proof_json=excluded.proof_json",
                    conflict_proofs,
                )
                con.executemany(
                    "INSERT OR IGNORE INTO structure_sync_event_conflict_merkle_nodes "
                    "VALUES (?,0,?,?)",
                    [
                        (window_id, first_leaf_index + index, leaf_hash)
                        for index, leaf_hash in enumerate(scanned_leaves)
                    ],
                )
            stored_ordinal = terminal_ordinal if complete else ordinal
            stored_offset = terminal_offset if complete else byte_offset
            stored_character_offset = (
                terminal_character_offset if complete else character_offset
            )
            stored_parent_hash = parent_hash if cursor else ""
            checkpoint_digest = _structure_event_member_checkpoint_digest((
                source[3], cursor, stored_ordinal, stored_character_offset,
                stored_offset, chain.count, stored_parent_hash, next_state,
                invalid_chain.to_json(),
            ))
            if conflict_complete:
                group_count, group_root = _validated_structure_event_group_truth(
                    con, window_id
                )
                receipt = (
                    window_id, source_count, source_root, source_identity,
                    STRUCTURE_EVENT_MEMBER_METADATA_CONTRACT, chain.count,
                    chain.hexdigest(), invalid_chain.count,
                    invalid_chain.hexdigest(), cursor, terminal_ordinal,
                    terminal_offset, now_ms,
                )
                receipt_digest = _structure_event_member_receipt_digest(
                    receipt,
                    event_conflict_count=conflict_chain.count,
                    event_conflict_root=conflict_chain.hexdigest(),
                    event_conflict_merkle_root=merkle_root,
                    source_group_truth_count=group_count,
                    source_group_truth_root=group_root,
                )
                con.execute(
                    "INSERT INTO structure_sync_event_member_receipts VALUES ("
                    + ",".join("?" for _ in range(19)) + ")",
                    (*receipt, receipt_digest, conflict_chain.count,
                     conflict_chain.hexdigest(), merkle_root, group_count, group_root),
                )
            updated = con.execute(
                "UPDATE structure_sync_event_member_progress SET event_cursor=?,"
                "member_ordinal=?,rows_written=?,member_byte_offset=?,member_state=?,"
                "diagnostic_state=?,checkpoint_at_ms=?,completed_at_ms=?,"
                "member_character_offset=?,source_receipt_digest=?,"
                "parent_payload_hash=?,checkpoint_digest=? WHERE "
                "window_id=? AND completed_at_ms IS NULL AND failure_reason IS NULL",
                (cursor, stored_ordinal, chain.count, stored_offset, next_state,
                 invalid_chain.to_json(), now_ms,
                 now_ms if conflict_complete else None,
                 stored_character_offset, source[3], stored_parent_hash,
                 checkpoint_digest, window_id),
            )
            if updated.rowcount != 1:
                raise ValueError("structure-event-member-cursor-race")
            con.execute("COMMIT")
            return {"sealed": conflict_complete, "complete": conflict_complete,
                    "rows_written": len(rows), "member_ordinal": terminal_ordinal,
                    "member_byte_offset": stored_offset,
                    "state": (
                        "sealed" if conflict_complete
                        else "deriving-group-truth" if complete
                        else "deriving-members"
                    )}
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def _advance_structure_event_group_truth_chunk(
        self,
        *,
        window_id: str,
        limit: int,
        now_ms: int,
        deadline_monotonic: float | None = None,
    ) -> dict[str, object]:
        """Derive receipt-bound source group truth with at most ``limit`` members."""
        con = self._connect_writer()
        try:
            if deadline_monotonic is not None:
                con.set_progress_handler(
                    lambda: int(time.monotonic() >= deadline_monotonic), 1_000
                )
            progress = con.execute(
                "SELECT event_cursor,group_cursor,market_cursor,member_ordinal,"
                "membership_state,member_count,active_named_count,invalid_member_count,"
                "truth_count,truth_state,checkpoint_at_ms,completed_at_ms,"
                "checkpoint_digest,tradable_open_named_count FROM "
                "structure_sync_event_group_truth_progress "
                "WHERE window_id=?",
                (window_id,),
            ).fetchone()
            member_progress = con.execute(
                "SELECT event_cursor,member_ordinal,rows_written,member_byte_offset,"
                "member_state,diagnostic_state,member_character_offset,"
                "source_receipt_digest,parent_payload_hash,checkpoint_digest "
                "FROM structure_sync_event_member_progress WHERE window_id=?",
                (window_id,),
            ).fetchone()
            if member_progress is None:
                raise ValueError("structure-event-group-truth-progress-invalid")
            if progress is None:
                membership_state = SerializableSHA256.new().to_json()
                truth_state = RowChainSHA256.new("source-event").to_json()
                group_checkpoint = _structure_event_group_truth_checkpoint_digest((
                    member_progress[7], "", "", "", -1, membership_state,
                    0, 0, 0, 0, truth_state, 0,
                ))
                con.execute("BEGIN IMMEDIATE")
                con.execute(
                    "INSERT OR IGNORE INTO structure_sync_event_group_truth_progress "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        window_id, "", "", "", -1, membership_state, 0, 0, 0, 0,
                        truth_state, now_ms, None, group_checkpoint, 0,
                    ),
                )
                con.execute("COMMIT")
                progress = con.execute(
                    "SELECT event_cursor,group_cursor,market_cursor,member_ordinal,"
                    "membership_state,member_count,active_named_count,"
                    "invalid_member_count,truth_count,truth_state,checkpoint_at_ms,"
                    "completed_at_ms,checkpoint_digest,tradable_open_named_count FROM "
                    "structure_sync_event_group_truth_progress WHERE window_id=?",
                    (window_id,),
                ).fetchone()
            if progress is None or progress[11] is not None:
                raise ValueError("structure-event-group-truth-progress-invalid")
            (
                member_chain, source_count, source_root, source_identity, checkpoint,
                member_phase, conflict_cursor, conflict_chain, merkle_level,
                merkle_cursor, merkle_width, merkle_pending_index,
                merkle_pending_hash, proof_cursor, proof_count,
            ) = _read_event_member_progress_state(str(member_progress[4]))
            if member_phase != "group-truth":
                raise ValueError("structure-event-group-truth-phase-invalid")
            expected_checkpoint = _structure_event_group_truth_checkpoint_digest((
                member_progress[7], *progress[:10], progress[13],
            ))
            if progress[12] != expected_checkpoint:
                raise ValueError("structure-event-group-truth-checkpoint-invalid")
            membership = SerializableSHA256.from_json(str(progress[4]))
            truth_chain = RowChainSHA256.from_json(
                str(progress[9]), expected_domain="source-event"
            )
            if truth_chain.count != int(progress[8]):
                raise ValueError("structure-event-group-truth-progress-invalid")
            cursor_event, cursor_group, cursor_market, cursor_ordinal = (
                str(progress[0]), str(progress[1]), str(progress[2]), int(progress[3])
            )
            rows = con.execute(
                "SELECT member.event_id,COALESCE(member.group_id,''),"
                "member.market_sort_key,member.member_ordinal,member.market_id,"
                "member.member_kind,member.active,member.closed FROM "
                "structure_sync_event_member_staging member WHERE member.window_id=? AND "
                "(?='' OR member.event_id>? OR (member.event_id=? AND "
                "(COALESCE(member.group_id,'')>? OR (COALESCE(member.group_id,'')=? AND "
                "(member.market_sort_key>? OR (member.market_sort_key=? AND "
                "member.member_ordinal>?)))))) ORDER BY member.event_id,"
                "COALESCE(member.group_id,''),member.market_sort_key,member.member_ordinal "
                "LIMIT ?",
                (
                    window_id, cursor_event, cursor_event, cursor_event, cursor_group,
                    cursor_group, cursor_market, cursor_market, cursor_ordinal, limit + 1,
                ),
            ).fetchall()
            selected, lookahead = rows[:limit], rows[limit:]
            event_ids = sorted({
                str(row[0]) for row in selected
            } | ({cursor_event} if cursor_event else set()))
            flag_rows: dict[str, tuple[object, ...]] = {}
            if event_ids:
                placeholders = ",".join("?" for _ in event_ids)
                for row in con.execute(
                    "SELECT event_id,json_type(payload_json,'$.negRisk'),"
                    "json_extract(payload_json,'$.negRisk'),"
                    "json_type(payload_json,'$.enableNegRisk'),"
                    "json_extract(payload_json,'$.enableNegRisk'),"
                    "json_type(payload_json,'$.negRiskAugmented'),"
                    "json_extract(payload_json,'$.negRiskAugmented') FROM "
                    "structure_sync_event_staging WHERE window_id=? AND "
                    f"event_id IN ({placeholders})",
                    (window_id, *event_ids),
                ):
                    flag_rows[str(row[0])] = tuple(row[1:])

            serialized_membership = json.loads(str(progress[4]))
            current_key = (
                (cursor_event, cursor_group)
                if cursor_event
                and isinstance(serialized_membership, dict)
                and int(serialized_membership.get("byte_count", 0)) > 0
                else None
            )
            member_count = int(progress[5])
            active_named_count = int(progress[6])
            invalid_count = int(progress[7])
            tradable_open_named_count = int(progress[13])
            truth_rows: list[tuple[object, ...]] = []

            def start_group(event_id: str, group_id: str) -> None:
                nonlocal membership, member_count, active_named_count, invalid_count
                nonlocal tradable_open_named_count
                membership = SerializableSHA256.new()
                prefix = json.dumps(
                    [event_id, group_id], ensure_ascii=False, separators=(",", ":")
                )[:-1] + ",["
                membership.update(prefix.encode())
                member_count = active_named_count = invalid_count = 0
                tradable_open_named_count = 0

            def finish_group(event_id: str, group_id: str) -> None:
                nonlocal membership
                membership.update(b"]]")
                membership_root = membership.hexdigest()
                flags = flag_rows.get(event_id)
                flags_valid = (
                    flags is not None
                    and flags[0] in {"true", "false"}
                    and flags[2] in {"true", "false"}
                    and flags[4] in {"true", "false"}
                    and flags[1] == 1
                    and flags[3] == 1
                )
                augmented = flags is not None and flags[5] == 1
                if not group_id or not flags_valid or invalid_count or member_count == 0:
                    quality, reason = "incomplete-source", (
                        "event-neg-risk-flags-invalid"
                        if not flags_valid else "event-membership-member-invalid"
                    )
                elif augmented:
                    quality, reason = (
                        "complete-unsupported", "augmented-neg-risk-not-supported"
                    )
                elif tradable_open_named_count == member_count:
                    quality, reason = "complete-supported", None
                else:
                    quality, reason = (
                        "complete-unsupported",
                        "standard-neg-risk-has-non-tradable-members",
                    )
                if group_id:
                    truth = (
                        window_id, event_id, group_id,
                        "augmented" if augmented else "standard", member_count,
                        active_named_count, membership_root, quality, reason,
                        tradable_open_named_count,
                    )
                    truth_rows.append(truth)
                    truth_chain.update(("structure-event-source-group-truth-v1", *truth[1:]))

            for row in selected:
                key = (str(row[0]), str(row[1]))
                if current_key != key:
                    if current_key is not None:
                        finish_group(*current_key)
                    start_group(*key)
                    current_key = key
                market_id, member_kind, active, closed = row[4:8]
                valid = (
                    isinstance(market_id, str) and bool(market_id)
                    and member_kind in {"named", "other", "inactive-reserved"}
                    and active in {0, 1} and closed in {0, 1}
                )
                if valid:
                    if member_count:
                        membership.update(b",")
                    membership.update(json.dumps(
                        (str(market_id), str(member_kind), bool(active), bool(closed)),
                        ensure_ascii=False, separators=(",", ":"),
                    ).encode())
                    member_count += 1
                    active_named_count += int(
                        member_kind == "named" and active == 1
                    )
                    tradable_open_named_count += int(
                        member_kind == "named" and active == 1 and closed == 0
                    )
                else:
                    invalid_count += 1
                cursor_event, cursor_group = key
                cursor_market, cursor_ordinal = str(row[2]), int(row[3])
            complete = not lookahead
            if current_key is not None and (
                complete
                or (
                    lookahead
                    and (str(lookahead[0][0]), str(lookahead[0][1]))
                    != current_key
                )
            ):
                finish_group(*current_key)
                current_key = None
                membership = SerializableSHA256.new()
                member_count = active_named_count = invalid_count = 0
                tradable_open_named_count = 0
            next_truth_count = truth_chain.count
            next_checkpoint = _structure_event_group_truth_checkpoint_digest((
                member_progress[7], cursor_event, cursor_group, cursor_market,
                cursor_ordinal, membership.to_json(), member_count,
                active_named_count, invalid_count, next_truth_count,
                truth_chain.to_json(), tradable_open_named_count,
            ))
            con.execute("BEGIN IMMEDIATE")
            current = con.execute(
                "SELECT event_cursor,group_cursor,market_cursor,member_ordinal,"
                "membership_state,member_count,active_named_count,invalid_member_count,"
                "truth_count,truth_state,checkpoint_at_ms,completed_at_ms,"
                "checkpoint_digest,tradable_open_named_count FROM "
                "structure_sync_event_group_truth_progress "
                "WHERE window_id=?", (window_id,),
            ).fetchone()
            if current != progress:
                raise ValueError("structure-event-group-truth-cursor-race")
            con.executemany(
                "INSERT INTO structure_sync_event_group_truth_staging VALUES "
                "(?,?,?,?,?,?,?,?,?,?)", truth_rows,
            )
            con.execute(
                "UPDATE structure_sync_event_group_truth_progress SET event_cursor=?,"
                "group_cursor=?,market_cursor=?,member_ordinal=?,membership_state=?,"
                "member_count=?,active_named_count=?,invalid_member_count=?,truth_count=?,"
                "truth_state=?,checkpoint_at_ms=?,completed_at_ms=?,checkpoint_digest=?,"
                "tradable_open_named_count=? "
                "WHERE window_id=?",
                (
                    cursor_event, cursor_group, cursor_market, cursor_ordinal,
                    membership.to_json(), member_count, active_named_count, invalid_count,
                    next_truth_count, truth_chain.to_json(), now_ms,
                    now_ms if complete else None, next_checkpoint,
                    tradable_open_named_count, window_id,
                ),
            )
            if complete:
                next_member_state = _event_member_progress_state(
                    member_chain=member_chain, source_event_count=source_count,
                    source_event_root=source_root, source_identity_hash=source_identity,
                    window_checkpoint_at_ms=checkpoint, phase="conflicts",
                    conflict_cursor=conflict_cursor,
                    event_conflict_chain=conflict_chain,
                    merkle_level=merkle_level, merkle_cursor=merkle_cursor,
                    merkle_width=merkle_width,
                    merkle_pending_index=merkle_pending_index,
                    merkle_pending_hash=merkle_pending_hash,
                    proof_cursor=proof_cursor, proof_count=proof_count,
                )
                next_member_checkpoint = _structure_event_member_checkpoint_digest((
                    member_progress[7], member_progress[0], int(member_progress[1]),
                    int(member_progress[6]), int(member_progress[3]),
                    int(member_progress[2]), member_progress[8], next_member_state,
                    member_progress[5],
                ))
                con.execute(
                    "UPDATE structure_sync_event_member_progress SET member_state=?,"
                    "checkpoint_at_ms=?,checkpoint_digest=? WHERE window_id=?",
                    (next_member_state, now_ms, next_member_checkpoint, window_id),
                )
            con.execute("COMMIT")
            return {
                "sealed": False, "complete": False,
                "rows_written": len(selected),
                "state": "sealing-conflicts" if complete else "deriving-group-truth",
            }
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def structure_event_member_status(
        self,
        *,
        window_id: str,
        trace_callback: Callable[[str], None] | None = None,
    ) -> dict[str, object]:
        """Expose only receipt-authenticated sidecar evidence."""
        invalid = {"sealed": False, "complete": False,
                   "reason": "structure-event-member-receipt-invalid"}
        with sqlite3.connect(self._db_path) as con:
            if trace_callback is not None:
                con.set_trace_callback(trace_callback)
            receipt = con.execute(
                "SELECT * FROM structure_sync_event_member_receipts WHERE window_id=?",
                (window_id,),
            ).fetchone()
            try:
                source = _validated_structure_event_source_receipt(con, window_id)
            except (TypeError, ValueError, json.JSONDecodeError):
                return {"sealed": False, "complete": False,
                        "reason": "structure-event-source-receipt-invalid"}
            if source is None:
                if receipt is not None:
                    return invalid
                return {"sealed": False, "complete": False,
                        "state": "waiting-natural-window", "authenticated": True,
                        "reason": "structure-event-source-receipt-unavailable"}
            window = con.execute(
                "SELECT checkpoint_at_ms,staging_reclaimed_at_ms "
                "FROM structure_sync_windows WHERE id=?",
                (window_id,),
            ).fetchone()
            backfill = con.execute(
                "SELECT window_checkpoint_at_ms,completed_at_ms,blocked_reason FROM "
                "structure_sync_event_market_backfill_progress WHERE window_id=?",
                (window_id,),
            ).fetchone()
            if receipt is None and (
                window is None
                or backfill is None
                or int(backfill[0]) != int(window[0])
                or backfill[1] is None
                or backfill[2] is not None
            ):
                return {
                    "sealed": False,
                    "complete": False,
                    "state": "waiting-event-market-backfill",
                    "authenticated": True,
                }
            progress = con.execute(
                "SELECT event_cursor,member_ordinal,rows_written,member_byte_offset,"
                "member_state,diagnostic_state,checkpoint_at_ms,completed_at_ms,"
                "failure_reason,member_character_offset,source_receipt_digest,"
                "parent_payload_hash,checkpoint_digest FROM "
                "structure_sync_event_member_progress WHERE window_id=?",
                (window_id,),
            ).fetchone()
            chain = diagnostic = conflict_chain = None
            count = root = identity = checkpoint = None
            phase = conflict_cursor = None
            staging_reclaimed = window is not None and window[1] is not None
            if progress is not None:
                try:
                    expected_checkpoint = _structure_event_member_checkpoint_digest((
                        progress[10], progress[0], int(progress[1]),
                        int(progress[9]), int(progress[3]), int(progress[2]),
                        progress[11], progress[4], progress[5],
                    ))
                    (
                        chain, count, root, identity, checkpoint, phase,
                        conflict_cursor, conflict_chain,
                        _merkle_level, _merkle_cursor, _merkle_width,
                        _merkle_pending_index, _merkle_pending_hash, _proof_cursor,
                        _proof_count,
                    ) = (
                        _read_event_member_progress_state(str(progress[4]))
                    )
                    diagnostic = RowChainSHA256.from_json(
                        str(progress[5]), expected_domain="diagnostic/unclassified"
                    )
                    parent = (
                        None if staging_reclaimed or not progress[0] else con.execute(
                            "SELECT payload_hash FROM structure_sync_event_metadata_staging "
                            "WHERE window_id=? AND event_id=?",
                            (window_id, progress[0]),
                        ).fetchone()
                    )
                    if (
                        progress[10] != source[3]
                        or progress[12] != expected_checkpoint
                        or chain.count != int(progress[2])
                        or (count, root, identity) != source[:3]
                        or checkpoint is None or int(checkpoint) < 0
                        or (
                            not staging_reclaimed
                            and not progress[0]
                            and progress[11] != ""
                        )
                        or (
                            not staging_reclaimed
                            and progress[0]
                            and (parent is None or str(parent[0]) != str(progress[11]))
                        )
                    ):
                        raise ValueError("structure-event-member-checkpoint-invalid")
                except (TypeError, ValueError, json.JSONDecodeError):
                    return {"sealed": False, "complete": False,
                            "reason": "structure-event-member-checkpoint-invalid"}
            if receipt is None:
                if progress is None:
                    return {"sealed": False, "complete": False,
                            "reason": "structure-event-member-checkpoint-invalid"}
                if progress[8] is not None:
                    return {"sealed": False, "complete": False,
                            "failure_reason": str(progress[8])}
                if progress[7] is None:
                    return {"sealed": False, "complete": False,
                            "rows_written": int(progress[2]),
                            "event_cursor": str(progress[0]),
                            "member_ordinal": int(progress[1]),
                            "member_byte_offset": int(progress[3])}
                return invalid
            try:
                if (progress is None or progress[7] is None
                        or progress[8] is not None or receipt[0] != window_id
                        or receipt[4] != STRUCTURE_EVENT_MEMBER_METADATA_CONTRACT
                        or receipt[13] != _structure_event_member_receipt_digest(
                            tuple(receipt[:13]),
                            event_conflict_count=int(receipt[14]),
                            event_conflict_root=str(receipt[15]),
                            event_conflict_merkle_root=str(receipt[16]),
                            source_group_truth_count=int(receipt[17]),
                            source_group_truth_root=str(receipt[18]),
                        )
                        or progress[0] != receipt[9]
                        or int(progress[1]) != int(receipt[10])
                        or int(progress[2]) != int(receipt[5])
                        or int(progress[3]) != int(receipt[11])
                        or progress[10] != source[3]
                        or progress[12] != _structure_event_member_checkpoint_digest((
                            progress[10], progress[0], int(progress[1]),
                            int(progress[9]), int(progress[3]), int(progress[2]),
                            progress[11], progress[4], progress[5],
                        ))):
                    return invalid
                assert chain is not None and diagnostic is not None
                assert conflict_chain is not None
                if staging_reclaimed:
                    group_count, group_root = int(receipt[17]), str(receipt[18])
                else:
                    group_count, group_root = _validated_structure_event_group_truth(
                        con, window_id,
                        expected=(int(receipt[17]), str(receipt[18])),
                        source_receipt_digest=str(progress[10]),
                    )
                if ((count, root, identity) != source[:3]
                        or checkpoint < 0
                        or (count, root, identity) != tuple(receipt[1:4])
                        or chain.count != int(receipt[5])
                        or chain.hexdigest() != receipt[6]
                        or diagnostic.count != int(receipt[7])
                        or diagnostic.hexdigest() != receipt[8]
                        or phase != "complete"
                        or conflict_chain.count != int(receipt[14])
                        or conflict_chain.hexdigest() != receipt[15]
                        or conflict_chain.count != source[0]
                        or group_count != int(receipt[17])
                        or group_root != receipt[18]):
                    return invalid
            except (TypeError, ValueError, json.JSONDecodeError):
                return invalid
            return {"sealed": True, "complete": True,
                    "rows_written": int(receipt[5]),
                    "invalid_member_count": int(receipt[7]),
                    "event_cursor": str(receipt[9]),
                    "member_ordinal": int(receipt[10]),
                    "member_byte_offset": int(receipt[11]),
                    "sealed_at_ms": int(receipt[12]),
                    "receipt_digest": str(receipt[13]),
                    "event_conflict_count": int(receipt[14]),
                    "event_conflict_root": str(receipt[15]),
                    "event_conflict_merkle_root": str(receipt[16]),
                    "source_group_truth_count": int(receipt[17]),
                    "source_group_truth_root": str(receipt[18])}

    def advance_structure_event_market_backfill(
        self,
        *,
        window_id: str,
        max_events: int,
        max_relationships: int,
        now_ms: int,
        max_payload_bytes: int = STRUCTURE_BOOTSTRAP_PAYLOAD_MAX_BYTES,
        writer_timeout_s: float | None = None,
        execution_deadline_s: float | None = None,
    ) -> dict[str, object]:
        """Backfill a bounded event/relationship slice with a durable subcursor."""
        if (
            not window_id
            or not 1 <= max_events <= STRUCTURE_EVENT_MARKET_BACKFILL_MAX_EVENTS
            or not 1 <= max_relationships <= STRUCTURE_EVENT_MARKET_BACKFILL_MAX_EVENTS
            or not STRUCTURE_EVENT_PAYLOAD_MAX_BYTES
            <= max_payload_bytes
            <= STRUCTURE_BOOTSTRAP_PAYLOAD_MAX_BYTES
            or now_ms < 0
            or (writer_timeout_s is not None and writer_timeout_s <= 0)
            or (execution_deadline_s is not None and execution_deadline_s <= 0)
        ):
            raise ValueError("invalid-structure-event-market-backfill")
        con = self._connect_writer(timeout_s=writer_timeout_s)
        deadline = (
            None
            if execution_deadline_s is None
            else time.monotonic() + execution_deadline_s
        )
        try:
            if deadline is not None:
                con.set_progress_handler(
                    lambda: int(time.monotonic() >= deadline),
                    1_000,
                )
            con.execute("BEGIN IMMEDIATE")
            window = con.execute(
                "SELECT status,checkpoint_at_ms FROM structure_sync_windows WHERE id=?",
                (window_id,),
            ).fetchone()
            if window is None or window[0] != "complete":
                raise ValueError("structure-sync-window-not-complete")
            window_checkpoint_at_ms = int(window[1])
            progress = con.execute(
                "SELECT window_checkpoint_at_ms,event_cursor,member_offset,"
                "events_processed,relationships_processed,completed_at_ms,blocked_reason "
                "FROM structure_sync_event_market_backfill_progress WHERE window_id=?",
                (window_id,),
            ).fetchone()
            if progress is None:
                con.execute(
                    "INSERT INTO structure_sync_event_market_backfill_progress("
                    "window_id,window_checkpoint_at_ms,checkpoint_at_ms) VALUES (?,?,?)",
                    (window_id, window_checkpoint_at_ms, now_ms),
                )
                event_cursor = ""
                member_offset = 0
                events_total = 0
                relationships_total = 0
                completed_at_ms = None
                blocked_reason = None
            else:
                if int(progress[0]) != window_checkpoint_at_ms:
                    raise ValueError("structure-event-market-window-identity-drift")
                event_cursor = str(progress[1])
                member_offset = int(progress[2])
                events_total = int(progress[3])
                relationships_total = int(progress[4])
                completed_at_ms = progress[5]
                blocked_reason = progress[6]
            if blocked_reason is not None:
                con.execute("COMMIT")
                return {
                    "completed": False,
                    "events_processed": 0,
                    "relationships_processed": 0,
                    "event_cursor": event_cursor,
                    "member_offset": member_offset,
                    "blocked": True,
                    "blocked_reason": str(blocked_reason),
                }
            if completed_at_ms is not None:
                con.execute("COMMIT")
                return {
                    "completed": True,
                    "events_processed": 0,
                    "relationships_processed": 0,
                    "event_cursor": event_cursor,
                    "member_offset": member_offset,
                    "blocked": False,
                    "blocked_reason": None,
                }
            # The completed staging window is immutable. Read and parse the
            # bounded source slice without holding SQLite's writer lock; only
            # the cursor-authorized relationship insert below is a write txn.
            con.execute("COMMIT")
            metadata_columns = (
                "event_id,length(CAST(payload_json AS BLOB)),"
                "COALESCE(source_ordinal,rowid)"
            )

            def metadata_rows() -> Iterator[tuple[object, ...]]:
                remaining = max_events
                if member_offset:
                    current = con.execute(
                        f"SELECT {metadata_columns} FROM "
                        "structure_sync_event_staging WHERE window_id=? AND event_id=?",
                        (window_id, event_cursor),
                    ).fetchone()
                    if current is not None:
                        yield current
                        remaining -= 1
                if remaining <= 0:
                    return
                cursor = con.execute(
                    f"SELECT {metadata_columns} FROM structure_sync_event_staging "
                    "WHERE window_id=? AND event_id>? ORDER BY event_id LIMIT ?",
                    (window_id, event_cursor, remaining),
                )
                while (row := cursor.fetchone()) is not None:
                    yield row

            parent_rows: list[tuple[object, ...]] = []
            events_processed = 0
            next_event_cursor = event_cursor
            next_member_offset = member_offset
            remaining_relationships = max_relationships
            payload_bytes_processed = 0
            current_event_id = "unknown"
            try:
                for row_index, (
                    event_id,
                    payload_bytes,
                    source_ordinal,
                ) in enumerate(metadata_rows()):
                    current_event_id = str(event_id)
                    if int(payload_bytes) > STRUCTURE_EVENT_PAYLOAD_MAX_BYTES:
                        raise ValueError(
                            f"event-payload-too-large:{event_id}:{payload_bytes}"
                        )
                    if payload_bytes_processed + int(payload_bytes) > max_payload_bytes:
                        break
                    payload_row = con.execute(
                        "SELECT payload_json FROM structure_sync_event_staging "
                        "WHERE window_id=? AND event_id=?",
                        (window_id, event_id),
                    ).fetchone()
                    if payload_row is None:
                        raise ValueError(f"event-payload-missing:{event_id}")
                    payload_bytes_processed += int(payload_bytes)
                    payload_json = payload_row[0]
                    payload = json.loads(str(payload_json))
                    if not isinstance(payload, dict):
                        raise ValueError(f"invalid-event-payload:{event_id}")
                    members = payload.get("markets", [])
                    if not isinstance(members, list):
                        raise ValueError(f"invalid-event-markets:{event_id}")
                    start = member_offset if row_index == 0 else 0
                    if start > len(members):
                        raise ValueError(f"invalid-event-member-offset:{event_id}")
                    checked: list[str] = []
                    for member in members:
                        market_id = (
                            member.get("id") if isinstance(member, dict) else None
                        )
                        if not isinstance(market_id, str) or not market_id:
                            raise ValueError(f"invalid-event-market:{event_id}")
                        checked.append(market_id)
                    end = min(len(checked), start + remaining_relationships)
                    parent_rows.extend(
                        (window_id, market_id, event_id, int(source_ordinal))
                        for market_id in checked[start:end]
                    )
                    remaining_relationships -= end - start
                    next_event_cursor = str(event_id)
                    if end < len(checked):
                        next_member_offset = end
                        break
                    next_member_offset = 0
                    events_processed += 1
                    if remaining_relationships == 0:
                        break
            except (json.JSONDecodeError, ValueError) as error:
                reason = (
                    f"invalid-event-json:{current_event_id}"
                    if isinstance(error, json.JSONDecodeError)
                    else str(error)
                )
                con.execute("BEGIN IMMEDIATE")
                updated = con.execute(
                    "UPDATE structure_sync_event_market_backfill_progress SET "
                    "checkpoint_at_ms=?,blocked_reason=? WHERE window_id=? "
                    "AND window_checkpoint_at_ms=? AND event_cursor=? "
                    "AND member_offset=? AND completed_at_ms IS NULL "
                    "AND blocked_reason IS NULL",
                    (
                        now_ms,
                        reason[:200],
                        window_id,
                        window_checkpoint_at_ms,
                        event_cursor,
                        member_offset,
                    ),
                )
                if updated.rowcount != 1:
                    raise ValueError("structure-event-market-backfill-cursor-race")
                con.execute("COMMIT")
                return {
                    "completed": False,
                    "events_processed": 0,
                    "relationships_processed": 0,
                    "event_cursor": event_cursor,
                    "member_offset": member_offset,
                    "blocked": True,
                    "blocked_reason": reason[:200],
                }
            con.execute("BEGIN IMMEDIATE")
            current = con.execute(
                "SELECT window_checkpoint_at_ms,event_cursor,member_offset,"
                "completed_at_ms,blocked_reason FROM "
                "structure_sync_event_market_backfill_progress WHERE window_id=?",
                (window_id,),
            ).fetchone()
            if current != (
                window_checkpoint_at_ms,
                event_cursor,
                member_offset,
                None,
                None,
            ):
                raise ValueError("structure-event-market-backfill-cursor-race")
            identity = con.execute(
                "SELECT status,checkpoint_at_ms,recovery_root_window_id FROM "
                "structure_sync_windows WHERE id=?",
                (window_id,),
            ).fetchone()
            if (
                identity is None
                or identity[:2] != ("complete", window_checkpoint_at_ms)
                or not isinstance(identity[2], str)
                or not identity[2]
            ):
                raise ValueError("structure-event-market-window-identity-drift")
            con.executemany(
                "INSERT OR IGNORE INTO structure_sync_event_market_staging("
                "window_id,market_id,event_id,source_ordinal) VALUES (?,?,?,?)",
                parent_rows,
            )
            relationships_processed = len(parent_rows)
            completed = False
            if next_member_offset == 0:
                completed = (
                    con.execute(
                        "SELECT 1 FROM structure_sync_event_staging "
                        "WHERE window_id=? AND event_id>? LIMIT 1",
                        (window_id, next_event_cursor),
                    ).fetchone()
                    is None
                )
            con.execute(
                "UPDATE structure_sync_event_market_backfill_progress SET "
                "event_cursor=?,member_offset=?,events_processed=?,"
                "relationships_processed=?,checkpoint_at_ms=?,completed_at_ms=? "
                "WHERE window_id=?",
                (
                    next_event_cursor,
                    next_member_offset,
                    events_total + events_processed,
                    relationships_total + relationships_processed,
                    now_ms,
                    now_ms if completed else None,
                    window_id,
                ),
            )
            if completed:
                recovery_root = str(identity[2])
                has_rotation = (
                    con.execute(
                        "SELECT 1 FROM structure_bootstrap_rotation_observations "
                        "WHERE recovery_root_window_id=? LIMIT 1",
                        (recovery_root,),
                    ).fetchone()
                    is not None
                )
                if has_rotation:
                    receipt_digest = _bootstrap_recovery_digest(
                        recovery_root_window_id=recovery_root,
                        successful_window_id=window_id,
                        window_checkpoint_at_ms=window_checkpoint_at_ms,
                        completed_at_ms=now_ms,
                    )
                    con.execute(
                        "INSERT OR IGNORE INTO structure_bootstrap_recovery_receipts("
                        "recovery_root_window_id,successful_window_id,"
                        "window_checkpoint_at_ms,completed_at_ms,receipt_digest) "
                        "VALUES (?,?,?,?,?)",
                        (
                            recovery_root,
                            window_id,
                            window_checkpoint_at_ms,
                            now_ms,
                            receipt_digest,
                        ),
                    )
                    receipt = con.execute(
                        "SELECT successful_window_id,window_checkpoint_at_ms,"
                        "completed_at_ms,receipt_digest FROM "
                        "structure_bootstrap_recovery_receipts "
                        "WHERE recovery_root_window_id=?",
                        (recovery_root,),
                    ).fetchone()
                    if receipt != (
                        window_id,
                        window_checkpoint_at_ms,
                        now_ms,
                        receipt_digest,
                    ):
                        raise ValueError(
                            "structure-bootstrap-recovery-receipt-conflict"
                        )
            con.execute("COMMIT")
            return {
                "completed": completed,
                "events_processed": events_processed,
                "relationships_processed": relationships_processed,
                "event_cursor": next_event_cursor,
                "member_offset": next_member_offset,
                "blocked": False,
                "blocked_reason": None,
            }
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.set_progress_handler(None, 0)
            con.close()

    def structure_event_market_backfill_complete(self, window_id: str) -> bool:
        """Return whether one complete window owns its sealed relationship staging."""
        if not window_id:
            raise ValueError("invalid-structure-sync-window")
        with sqlite3.connect(f"file:{self._db_path}?mode=ro", uri=True) as con:
            row = con.execute(
                "SELECT window.status,progress.completed_at_ms,progress.blocked_reason "
                "FROM structure_sync_windows window LEFT JOIN "
                "structure_sync_event_market_backfill_progress progress "
                "ON progress.window_id=window.id WHERE window.id=?",
                (window_id,),
            ).fetchone()
        return bool(
            row is not None
            and row[0] == "complete"
            and row[1] is not None
            and row[2] is None
        )

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
            event_tag_tuples = [
                _event_tag_row_to_tuple(r, snapshot_id) for r in event_tag_rows
            ]
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

            # The snapshot row is inserted as published before its truth and
            # market rows are populated, all inside this transaction.  Mark
            # the coalesced legacy revision clean only at the real atomic
            # publication boundary, after every source row is durable.
            if publish_markets:
                con.execute("DELETE FROM legacy_structure_revision_dirty WHERE id=1")

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
                event_tag_tuples = [
                    _event_tag_row_to_tuple(r, snapshot_id) for r in event_tag_rows
                ]
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

            # See write_snapshot: publication becomes externally visible at
            # COMMIT, not when the snapshot metadata row is first inserted.
            if publish_markets:
                con.execute("DELETE FROM legacy_structure_revision_dirty WHERE id=1")

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
                str(key): int(value) for key, value in json.loads(str(row[4])).items()
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
                f"SELECT * FROM {table} WHERE snapshot_id=? ORDER BY {order_by[component]}",  # noqa: S608 - internal constants
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
            return int(
                con.execute("SELECT COALESCE(MAX(id),0)+1 FROM snapshots").fetchone()[0]
            )

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
        deadline_monotonic: float | None = None,
    ) -> list[tuple[str, dict]]:
        """Read at most ``limit`` completed raw rows using stable keyset order."""
        if source not in {"events", "markets"} or limit < 1:
            raise ValueError("invalid-structure-staging-chunk")
        table = (
            f"structure_sync_{source[:-1] if source == 'events' else 'market'}_staging"
        )
        key = "event_id" if source == "events" else "market_id"
        with self._connect_deadline_read(deadline_monotonic) as con:
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
        with self._connect_deadline_read(None) as con:
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

    def structure_event_ids_for_markets(
        self,
        publication_id: str,
        market_ids: list[str],
        *,
        deadline_monotonic: float | None = None,
    ) -> dict[str, str]:
        """Resolve one bounded market chunk's parents with fixed SQL/connection cost."""
        if (
            not publication_id
            or len(market_ids) > 500
            or any(
                not isinstance(market_id, str) or not market_id
                for market_id in market_ids
            )
        ):
            raise ValueError("invalid-structure-market-parent-chunk")
        if not market_ids:
            return {}
        placeholders = ",".join("?" for _ in market_ids)
        resolved: dict[str, str] = {}
        with self._connect_deadline_read(deadline_monotonic) as con:
            staged = con.execute(
                "SELECT mine.market_id,mine.event_id FROM structure_publications p JOIN "
                "structure_sync_event_market_staging mine ON mine.window_id=p.window_id "
                f"WHERE p.publication_id=? AND mine.market_id IN ({placeholders}) "
                "ORDER BY mine.market_id,mine.source_ordinal,mine.event_id",
                (publication_id, *market_ids),
            )
            for market_id, event_id in staged:
                resolved.setdefault(str(market_id), str(event_id))
            missing = [
                market_id for market_id in market_ids if market_id not in resolved
            ]
            if missing:
                missing_placeholders = ",".join("?" for _ in missing)
                memberships = con.execute(
                    "SELECT m.market_id,m.event_id FROM structure_publications p JOIN "
                    "structure_generation_memberships m ON m.snapshot_id=p.snapshot_id "
                    f"WHERE p.publication_id=? AND m.market_id IN ({missing_placeholders}) "
                    "ORDER BY m.market_id,m.event_id",
                    (publication_id, *missing),
                )
                for market_id, event_id in memberships:
                    resolved.setdefault(str(market_id), str(event_id))
        return resolved

    def structure_event_has_duplicate_market(
        self, publication_id: str, event_id: str
    ) -> bool:
        with sqlite3.connect(self._db_path) as con:
            return (
                con.execute(
                    "SELECT 1 FROM structure_publications p JOIN "
                    "structure_sync_event_market_staging mine ON "
                    "mine.window_id=p.window_id JOIN structure_sync_event_market_staging "
                    "other ON other.window_id=mine.window_id AND "
                    "other.market_id=mine.market_id AND other.event_id!=mine.event_id "
                    "WHERE p.publication_id=? AND mine.event_id=? LIMIT 1",
                    (publication_id, event_id),
                ).fetchone()
                is not None
            )

    def structure_events_with_duplicate_markets(
        self,
        publication_id: str,
        event_ids: list[str],
    ) -> set[str]:
        """Resolve duplicate-market quality for one bounded event chunk in O(1) SQL."""
        if (
            not publication_id
            or len(event_ids) > 500
            or any(
                not isinstance(event_id, str) or not event_id for event_id in event_ids
            )
        ):
            raise ValueError("invalid-structure-duplicate-event-chunk")
        if not event_ids:
            return set()
        placeholders = ",".join("?" for _ in event_ids)
        with sqlite3.connect(self._db_path) as con:
            rows = con.execute(
                "SELECT DISTINCT mine.event_id FROM structure_publications p JOIN "
                "structure_sync_event_market_staging mine ON mine.window_id=p.window_id "
                "JOIN structure_sync_event_market_staging other ON "
                "other.window_id=mine.window_id AND other.market_id=mine.market_id "
                "AND other.event_id!=mine.event_id WHERE p.publication_id=? "
                f"AND mine.event_id IN ({placeholders})",
                (publication_id, *event_ids),
            ).fetchall()
        return {str(row[0]) for row in rows}

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

    def fetch_structure_market_parent_group_chunk(
        self,
        *,
        window_id: str,
        after_market_id: str | None,
        limit: int,
    ) -> list[tuple[str, dict[str, object]]]:
        """Read one complete market keyset with every staged parent identity."""
        if not window_id or not 1 <= limit <= 500:
            raise ValueError("invalid-structure-market-parent-group-chunk")
        with sqlite3.connect(self._db_path) as con:
            rows = con.execute(
                "WITH market_keys AS (SELECT market_id,payload_json FROM "
                "structure_sync_market_staging WHERE window_id=? "
                "AND (? IS NULL OR market_id>?) GROUP BY market_id "
                "ORDER BY market_id LIMIT ?) SELECT market_keys.market_id,"
                "market_keys.payload_json,relation.event_id FROM market_keys LEFT JOIN "
                "structure_sync_event_market_staging relation ON relation.window_id=? "
                "AND relation.market_id=market_keys.market_id ORDER BY "
                "market_keys.market_id,relation.source_ordinal,relation.event_id",
                (window_id, after_market_id, after_market_id, limit, window_id),
            ).fetchall()
        grouped: dict[str, dict[str, object]] = {}
        for market_id, payload_json, event_id in rows:
            item = grouped.setdefault(
                str(market_id),
                {"raw": json.loads(str(payload_json)), "event_ids": []},
            )
            if event_id is not None:
                assert isinstance(item["event_ids"], list)
                item["event_ids"].append(str(event_id))
        return [
            (
                market_id,
                {"raw": item["raw"], "event_ids": tuple(item["event_ids"])},
            )
            for market_id, item in grouped.items()
        ]

    def fetch_structure_drift_event_source_chunk(
        self,
        *,
        publication_id: str,
        generation_snapshot_id: int,
        after_event_id: str | None,
        limit: int,
        trace_callback: Callable[[str], None] | None = None,
    ) -> list[tuple[int, str, dict[str, object], frozenset[str]]]:
        """Read one independently projected event-source chunk from a sealed window."""
        if (
            not publication_id
            or generation_snapshot_id < 1
            or not 1 <= limit <= STRUCTURE_PUBLICATION_MAX_ROWS
        ):
            raise ValueError("invalid-structure-drift-event-source-chunk")
        with sqlite3.connect(self._db_path) as con:
            if trace_callback is not None:
                con.set_trace_callback(trace_callback)
            con.execute("BEGIN")
            identity = con.execute(
                "SELECT p.window_id,p.status,p.normalization_contract_version,"
                "p.validation_hash,p.certification_hash,window.status,"
                "window.published_snapshot_id FROM structure_publications p JOIN "
                "structure_sync_windows window ON window.id=p.window_id "
                "WHERE p.publication_id=? AND p.snapshot_id=?",
                (publication_id, generation_snapshot_id),
            ).fetchone()
            if (
                identity is None
                or identity[1] != "published"
                or not isinstance(identity[2], str)
                or not identity[2]
                or not isinstance(identity[3], str)
                or len(identity[3]) != 64
                or not isinstance(identity[4], str)
                or len(identity[4]) != 64
                or identity[5] != "published"
                or identity[6] != generation_snapshot_id
            ):
                raise ValueError("structure-drift-source-identity-mismatch")
            window_id = str(identity[0])
            candidate_limit = min(limit, STRUCTURE_DRIFT_SOURCE_EVENT_MAX_ROWS)
            rows = con.execute(
                "SELECT COALESCE(source_ordinal,rowid),event_id,"
                "length(CAST(payload_json AS BLOB)) FROM "
                "structure_sync_event_staging WHERE window_id=? AND "
                "(? IS NULL OR event_id>?) ORDER BY event_id LIMIT ?",
                (window_id, after_event_id, after_event_id, candidate_limit),
            ).fetchall()
            if rows:
                payload_prefix = _structure_drift_event_prefix_size(
                    [(int(row[2]), 0, 0) for row in rows]
                )
                rows = rows[:payload_prefix]
            event_ids = [str(row[1]) for row in rows]
            raw_by_event: dict[str, dict[str, object]] = {}
            relation_counts: dict[str, int] = {}
            if event_ids:
                placeholders = ",".join("?" for _ in event_ids)
                raw_by_event = {
                    str(event_id): json.loads(str(payload_json))
                    for event_id, payload_json in con.execute(
                        "SELECT event_id,payload_json FROM "
                        "structure_sync_event_staging WHERE window_id=? "
                        f"AND event_id IN ({placeholders})",
                        (window_id, *event_ids),
                    )
                }
                relation_counts = {
                    str(event_id): int(count)
                    for event_id, count in con.execute(
                        "SELECT relation.event_id,COUNT(*) FROM "
                        "structure_sync_event_market_staging relation JOIN "
                        "structure_sync_market_staging market ON "
                        "market.window_id=relation.window_id AND "
                        "market.market_id=relation.market_id WHERE relation.window_id=? "
                        f"AND relation.event_id IN ({placeholders}) "
                        "GROUP BY relation.event_id",
                        (window_id, *event_ids),
                    )
                }
                workloads = []
                for row in rows:
                    raw = raw_by_event[str(row[1])]
                    embedded = raw.get("markets") if isinstance(raw, dict) else None
                    workloads.append(
                        (
                            int(row[2]),
                            len(embedded) if isinstance(embedded, list) else 0,
                            relation_counts.get(str(row[1]), 0),
                        )
                    )
                rows = rows[: _structure_drift_event_prefix_size(workloads)]
                event_ids = [str(row[1]) for row in rows]
            catalog_by_event: dict[str, set[str]] = {}
            if event_ids:
                placeholders = ",".join("?" for _ in event_ids)
                for event_id, market_id in con.execute(
                    "SELECT relation.event_id,relation.market_id FROM "
                    "structure_sync_event_market_staging relation JOIN "
                    "structure_sync_market_staging market ON "
                    "market.window_id=relation.window_id AND "
                    "market.market_id=relation.market_id WHERE relation.window_id=? "
                    f"AND relation.event_id IN ({placeholders}) ORDER BY "
                    "relation.event_id,relation.source_ordinal,relation.market_id",
                    (window_id, *event_ids),
                ):
                    catalog_by_event.setdefault(str(event_id), set()).add(
                        str(market_id)
                    )
            result = [
                (
                    int(row[0]),
                    str(row[1]),
                    raw_by_event[str(row[1])],
                    frozenset(catalog_by_event.get(str(row[1]), ())),
                )
                for row in rows
            ]
            con.execute("COMMIT")
            return result

    def fetch_structure_drift_market_source_chunk(
        self,
        *,
        publication_id: str,
        generation_snapshot_id: int,
        after_market_id: str | None,
        limit: int,
        trace_callback: Callable[[str], None] | None = None,
    ) -> list[tuple[str, dict[str, object], tuple[str, ...], int]]:
        """Read one independently projected market-source chunk from a sealed window."""
        if (
            not publication_id
            or generation_snapshot_id < 1
            or not 1 <= limit <= STRUCTURE_PUBLICATION_MAX_ROWS
        ):
            raise ValueError("invalid-structure-drift-market-source-chunk")
        with sqlite3.connect(self._db_path) as con:
            if trace_callback is not None:
                con.set_trace_callback(trace_callback)
            con.execute("BEGIN")
            identity = con.execute(
                "SELECT p.window_id,p.status,p.normalization_contract_version,"
                "p.validation_hash,p.certification_hash,window.status,"
                "window.published_snapshot_id,s.taken_at_ms FROM "
                "structure_publications p JOIN structure_sync_windows window ON "
                "window.id=p.window_id JOIN snapshots s ON s.id=p.snapshot_id "
                "WHERE p.publication_id=? AND p.snapshot_id=?",
                (publication_id, generation_snapshot_id),
            ).fetchone()
            if (
                identity is None
                or identity[1] != "published"
                or not isinstance(identity[2], str)
                or not identity[2]
                or not isinstance(identity[3], str)
                or len(identity[3]) != 64
                or not isinstance(identity[4], str)
                or len(identity[4]) != 64
                or identity[5] != "published"
                or identity[6] != generation_snapshot_id
            ):
                raise ValueError("structure-drift-source-identity-mismatch")
            window_id = str(identity[0])
            rows = con.execute(
                "WITH market_keys AS (SELECT market_id,payload_json FROM "
                "structure_sync_market_staging WHERE window_id=? AND "
                "(? IS NULL OR market_id>?) ORDER BY market_id LIMIT ?) "
                "SELECT market_keys.market_id,market_keys.payload_json,relation.event_id "
                "FROM market_keys LEFT JOIN structure_sync_event_market_staging relation "
                "ON relation.window_id=? AND relation.market_id=market_keys.market_id "
                "ORDER BY market_keys.market_id,relation.source_ordinal,relation.event_id",
                (window_id, after_market_id, after_market_id, limit, window_id),
            ).fetchall()
            grouped: dict[str, tuple[dict[str, object], list[str]]] = {}
            for market_id, payload_json, event_id in rows:
                key = str(market_id)
                item = grouped.get(key)
                if item is None:
                    item = (json.loads(str(payload_json)), [])
                    grouped[key] = item
                if event_id is not None:
                    item[1].append(str(event_id))
            result = [
                (market_id, raw, tuple(event_ids), int(identity[7]))
                for market_id, (raw, event_ids) in grouped.items()
            ]
            con.execute("COMMIT")
            return result

    def _validated_fresh_projection_member_authority(
        self,
        *,
        publication_id: str,
        generation_snapshot_id: int,
        trace_callback: Callable[[str], None] | None,
    ) -> str:
        """Return one fully authenticated sidecar receipt before candidate reads."""
        with sqlite3.connect(self._db_path) as con:
            if trace_callback is not None:
                con.set_trace_callback(trace_callback)
            identity = con.execute(
                "SELECT window_id FROM structure_publications WHERE publication_id=? "
                "AND snapshot_id=? AND status='published'",
                (publication_id, generation_snapshot_id),
            ).fetchone()
        if identity is None:
            raise ValueError("structure-drift-source-identity-mismatch")
        member_status = self.structure_event_member_status(
            window_id=str(identity[0]), trace_callback=trace_callback
        )
        if member_status.get("sealed") is not True:
            raise ValueError(
                str(
                    member_status.get("reason")
                    or member_status.get("failure_reason")
                    or "structure-event-member-receipt-invalid"
                )
            )
        return str(member_status["receipt_digest"])

    def fetch_structure_drift_fresh_projection_chunk(
        self,
        *,
        publication_id: str,
        generation_snapshot_id: int,
        cursor: FreshProjectionCursor | None,
        limit: int,
        classifier_contract: str = STRUCTURE_DRIFT_CLASSIFIER_V2,
        trace_callback: Callable[[str], None] | None = None,
        inspection_callback: Callable[[str, int], None] | None = None,
        sqlite_progress_callback: Callable[[], int] | None = None,
    ) -> FreshProjectionChunk:
        """Validate sealed authority, then read one bounded sidecar projection chunk."""
        if classifier_contract not in {
            STRUCTURE_DRIFT_CLASSIFIER_V2,
            STRUCTURE_DRIFT_CLASSIFIER_V3,
            STRUCTURE_DRIFT_CLASSIFIER_V4,
        }:
            raise ValueError("invalid-structure-drift-classifier-contract")
        self._validated_fresh_projection_member_authority(
            publication_id=publication_id,
            generation_snapshot_id=generation_snapshot_id,
            trace_callback=trace_callback,
        )
        return self._fetch_structure_drift_fresh_projection_chunk(
            publication_id=publication_id,
            generation_snapshot_id=generation_snapshot_id,
            cursor=cursor,
            limit=limit,
            classifier_contract=classifier_contract,
            trace_callback=trace_callback,
            inspection_callback=inspection_callback,
            sqlite_progress_callback=sqlite_progress_callback,
        )

    def _fetch_structure_drift_fresh_projection_chunk(
        self,
        *,
        publication_id: str,
        generation_snapshot_id: int,
        cursor: FreshProjectionCursor | None,
        limit: int,
        classifier_contract: str = STRUCTURE_DRIFT_CLASSIFIER_V2,
        trace_callback: Callable[[str], None] | None = None,
        inspection_callback: Callable[[str, int], None] | None = None,
        sqlite_progress_callback: Callable[[], int] | None = None,
    ) -> FreshProjectionChunk:
        """Project one bounded fresh-source union from the sealed member sidecar."""
        from polyarb.perception.structure_drift import (
            FreshGroupEvidence,
            FreshMemberEvidence,
            FreshProjectionChunk,
            FreshProjectionCursor,
            FreshProjectionExclusion,
            StructuralMemberIdentity,
            StructureDriftCandidateEnvelope,
            diagnose_unresolved_member,
            project_legacy_compatible_market,
        )
        from polyarb.perception.structure_publication import (
            EVENT_ONLY_NEG_RISK_QUARANTINE_REASON,
            event_only_member_quarantine_issue,
            market_quarantine_issue,
            structure_market_source_hash,
        )

        if classifier_contract not in {
            STRUCTURE_DRIFT_CLASSIFIER_V2,
            STRUCTURE_DRIFT_CLASSIFIER_V3,
            STRUCTURE_DRIFT_CLASSIFIER_V4,
        }:
            raise ValueError("invalid-structure-drift-classifier-contract")
        if (
            not publication_id
            or generation_snapshot_id < 1
            or not 1 <= limit <= STRUCTURE_PUBLICATION_MAX_ROWS
            or (
                cursor is not None
                and not isinstance(cursor, FreshProjectionCursor)
            )
        ):
            raise ValueError("invalid-structure-drift-fresh-projection-chunk")
        if isinstance(cursor, FreshProjectionCursor):
            if cursor.stream not in {"market", "event-only"}:
                raise ValueError("invalid-structure-drift-fresh-projection-cursor")
            if cursor.stream == "market" and (
                not cursor.market_id
                or cursor.event_id is not None
                or cursor.source_ordinal is not None
                or cursor.member_ordinal is not None
            ):
                raise ValueError("invalid-structure-drift-fresh-projection-cursor")
            if cursor.stream == "event-only" and (
                cursor.market_id is None
                or not cursor.event_id
                or cursor.source_ordinal is None
                or cursor.member_ordinal is None
            ):
                raise ValueError("invalid-structure-drift-fresh-projection-cursor")

        with sqlite3.connect(self._db_path) as con:
            if trace_callback is not None:
                con.set_trace_callback(trace_callback)
            if sqlite_progress_callback is not None:
                con.set_progress_handler(sqlite_progress_callback, 1)
            con.execute("BEGIN")
            identity = con.execute(
                "SELECT p.window_id,p.status,p.normalization_contract_version,"
                "p.validation_hash,p.certification_hash,window.status,"
                "window.published_snapshot_id,receipt.event_conflict_merkle_root "
                "FROM structure_publications p JOIN "
                "structure_sync_windows window ON window.id=p.window_id "
                "JOIN structure_sync_event_member_receipts receipt ON "
                "receipt.window_id=p.window_id "
                "WHERE p.publication_id=? AND p.snapshot_id=?",
                (publication_id, generation_snapshot_id),
            ).fetchone()
            if (
                identity is None
                or identity[1] != "published"
                or not isinstance(identity[2], str)
                or not identity[2]
                or not isinstance(identity[3], str)
                or len(identity[3]) != 64
                or not isinstance(identity[4], str)
                or len(identity[4]) != 64
                or identity[5] != "published"
                or identity[6] != generation_snapshot_id
            ):
                raise ValueError("structure-drift-source-identity-mismatch")
            window_id = str(identity[0])

            remaining = limit
            candidates: list[dict[str, object]] = []

            if cursor is None or cursor.stream == "market":
                if cursor is None:
                    market_rows = con.execute(
                        "SELECT market_id,payload_json FROM "
                        "structure_sync_market_staging WHERE window_id=? "
                        "ORDER BY market_id LIMIT ?",
                        (window_id, remaining),
                    ).fetchall()
                else:
                    market_rows = con.execute(
                        "SELECT market_id,payload_json FROM "
                        "structure_sync_market_staging WHERE window_id=? "
                        "AND market_id>? ORDER BY market_id LIMIT ?",
                        (window_id, cursor.market_id, remaining),
                    ).fetchall()
                candidates.extend(
                    {
                        "kind": "market",
                        "market_id": str(market_id),
                        "raw_market": json.loads(str(payload_json)),
                    }
                    for market_id, payload_json in market_rows
                )
                remaining -= len(market_rows)

            sidecar_cursor = (
                cursor
                if isinstance(cursor, FreshProjectionCursor)
                and cursor.stream == "event-only"
                else None
            )
            scan_cursor: FreshProjectionCursor | None = None
            projection_complete = False
            if remaining == 0 and candidates:
                last_market_id = str(candidates[-1]["market_id"])
                more_markets = con.execute(
                    "SELECT 1 FROM structure_sync_market_staging "
                    "WHERE window_id=? AND market_id>? LIMIT 1",
                    (window_id, last_market_id),
                ).fetchone()
                if more_markets is None:
                    event_only_exists = con.execute(
                        "SELECT 1 FROM structure_sync_event_member_staging member "
                        "LEFT JOIN structure_sync_market_staging market ON "
                        "market.window_id=member.window_id AND "
                        "market.market_id=member.market_id WHERE member.window_id=? "
                        "AND market.market_id IS NULL LIMIT 1",
                        (window_id,),
                    ).fetchone()
                    projection_complete = event_only_exists is None
            if remaining:
                sidecar_columns = (
                    "member.market_sort_key,member.event_id,member.event_ordinal,"
                    "member.member_ordinal,member.market_id,member.group_id,"
                    "member.member_kind,member.active,member.closed,member.payload_json,"
                    "member.payload_hash,metadata.payload_hash"
                )
                if sidecar_cursor is None:
                    sidecar_rows = con.execute(
                        "SELECT " + sidecar_columns + " FROM "
                        "structure_sync_event_member_staging member JOIN "
                        "structure_sync_event_metadata_staging metadata ON "
                        "metadata.window_id=member.window_id AND "
                        "metadata.event_id=member.event_id LEFT JOIN "
                        "structure_sync_market_staging market ON "
                        "market.window_id=member.window_id AND "
                        "market.market_id=member.market_id WHERE member.window_id=? "
                        "AND market.market_id IS NULL ORDER BY member.event_id,"
                        "member.member_ordinal,member.event_ordinal LIMIT ?",
                        (window_id, remaining),
                    ).fetchall()
                else:
                    sidecar_rows = con.execute(
                        "SELECT " + sidecar_columns + " FROM "
                        "structure_sync_event_member_staging member JOIN "
                        "structure_sync_event_metadata_staging metadata ON "
                        "metadata.window_id=member.window_id AND "
                        "metadata.event_id=member.event_id LEFT JOIN "
                        "structure_sync_market_staging market ON "
                        "market.window_id=member.window_id AND "
                        "market.market_id=member.market_id WHERE member.window_id=? "
                        "AND (member.event_id,member.member_ordinal,member.event_ordinal)"
                        ">(?,?,?) AND market.market_id IS NULL ORDER BY member.event_id,"
                        "member.member_ordinal,member.event_ordinal LIMIT ?",
                        (
                            window_id,
                            sidecar_cursor.event_id,
                            sidecar_cursor.member_ordinal,
                            sidecar_cursor.source_ordinal,
                            remaining,
                        ),
                    ).fetchall()
                for row in sidecar_rows:
                    candidates.append(
                        {
                            "kind": "event-only",
                            "market_sort_key": str(row[0]),
                            "event_id": str(row[1]),
                            "source_ordinal": int(row[2]),
                            "member_ordinal": int(row[3]),
                            "market_id": "" if row[4] is None else str(row[4]),
                            "group_id": row[5],
                            "member_kind": row[6],
                            "active": None if row[7] is None else bool(row[7]),
                            "closed": None if row[8] is None else bool(row[8]),
                            "raw_member": json.loads(str(row[9])),
                            "raw_market_hash": str(row[10]),
                            "raw_event_hash": str(row[11]),
                        }
                    )
                if sidecar_rows:
                    last = sidecar_rows[-1]
                    scan_cursor = FreshProjectionCursor(
                        stream="event-only",
                        market_id=str(last[0]),
                        event_id=str(last[1]),
                        source_ordinal=int(last[2]),
                        member_ordinal=int(last[3]),
                    )
                    more = con.execute(
                        "SELECT 1 FROM structure_sync_event_member_staging member "
                        "LEFT JOIN structure_sync_market_staging market ON "
                        "market.window_id=member.window_id AND "
                        "market.market_id=member.market_id WHERE member.window_id=? "
                        "AND (member.event_id,member.member_ordinal,member.event_ordinal)"
                        ">(?,?,?) AND market.market_id IS NULL LIMIT 1",
                        (window_id, last[1], last[3], last[2]),
                    ).fetchone()
                    projection_complete = more is None
                else:
                    projection_complete = True

            market_ids = sorted({str(item["market_id"]) for item in candidates})
            if inspection_callback is not None:
                inspection_callback("candidates", len(candidates))
            relations: dict[str, list[str]] = {}
            sidecar_rows_by_market: dict[str, list[tuple[object, ...]]] = {}
            sidecar_market_counts: dict[str, int] = {}
            sidecar_identity_counts: dict[tuple[str, str], tuple[int, int]] = {}
            group_truth_by_key: dict[tuple[str, str], tuple[object, ...]] = {}
            conflict_events: set[str] = set()
            certified_event_keys: set[tuple[str, int, int]] = set()
            certified_issue_candidate_indexes: set[int] = set()
            raw_events_by_id: dict[str, dict[str, object]] = {}
            generated_market_ids: set[str] = set()
            generated_membership_ids: set[str] = set()
            if market_ids:
                placeholders = ",".join("?" for _ in market_ids)
                if classifier_contract in STRUCTURE_DRIFT_CLASSIFIERS_V3_COMPATIBLE:
                    generated_market_ids.update(
                        str(row[0])
                        for row in con.execute(
                            "SELECT market_id FROM structure_generation_markets WHERE "
                            f"snapshot_id=? AND market_id IN ({placeholders})",
                            (generation_snapshot_id, *market_ids),
                        )
                    )
                    generated_membership_ids.update(
                        str(row[0])
                        for row in con.execute(
                            "SELECT market_id FROM structure_generation_memberships WHERE "
                            f"snapshot_id=? AND market_id IN ({placeholders})",
                            (generation_snapshot_id, *market_ids),
                        )
                    )
                    candidate_event_ids = sorted({
                        str(item["event_id"])
                        for item in candidates
                        if item["kind"] == "event-only"
                    })
                    if candidate_event_ids:
                        event_placeholders = ",".join("?" for _ in candidate_event_ids)
                        for event_id, payload_json in con.execute(
                            "SELECT event_id,payload_json FROM "
                            "structure_sync_event_staging WHERE window_id=? AND "
                            f"event_id IN ({event_placeholders})",
                            (window_id, *candidate_event_ids),
                        ):
                            payload = json.loads(str(payload_json))
                            if isinstance(payload, dict):
                                raw_events_by_id[str(event_id)] = payload
                witness_limit = 2 * len(market_ids)
                relation_probe = con.execute(
                    "SELECT market_id,event_id FROM "
                    "structure_sync_event_market_staging WHERE window_id=? AND "
                    f"market_id IN ({placeholders}) LIMIT ?",
                    (window_id, *market_ids, witness_limit + 1),
                ).fetchall()
                relation_probe_counts: dict[str, int] = {}
                for market_id, _event_id in relation_probe:
                    key = str(market_id)
                    relation_probe_counts[key] = relation_probe_counts.get(key, 0) + 1
                if (
                    len(relation_probe) > witness_limit
                    or any(count > 2 for count in relation_probe_counts.values())
                ):
                    bounded_relation_sql = " UNION ALL ".join(
                        "SELECT market_id,event_id FROM (SELECT market_id,event_id FROM "
                        "structure_sync_event_market_staging WHERE window_id=? AND "
                        "market_id=? ORDER BY event_id LIMIT 2)"
                        for _market_id in market_ids
                    )
                    relation_rows = con.execute(
                        bounded_relation_sql,
                        tuple(
                            value for market_id in market_ids
                            for value in (window_id, market_id)
                        ),
                    ).fetchall()
                else:
                    relation_rows = relation_probe
                if inspection_callback is not None:
                    inspection_callback("relations", len(relation_rows))
                for market_id, event_id in relation_rows:
                    relations.setdefault(str(market_id), []).append(str(event_id))
                staged_probe = con.execute(
                    "SELECT member.market_id,member.event_id,member.event_ordinal,"
                    "member.member_ordinal,member.group_id,member.member_kind,member.active,"
                    "member.closed,member.payload_json,member.payload_hash,"
                    "metadata.payload_hash FROM structure_sync_event_member_staging member "
                    "JOIN structure_sync_event_metadata_staging metadata ON "
                    "metadata.window_id=member.window_id AND "
                    "metadata.event_id=member.event_id WHERE member.window_id=? AND "
                    f"member.market_id IN ({placeholders}) LIMIT ?",
                    (window_id, *market_ids, witness_limit + 1),
                ).fetchall()
                staged_probe_counts: dict[str, int] = {}
                for row in staged_probe:
                    key = str(row[0])
                    staged_probe_counts[key] = staged_probe_counts.get(key, 0) + 1
                if (
                    len(staged_probe) > witness_limit
                    or any(count > 2 for count in staged_probe_counts.values())
                ):
                    bounded_sidecar_sql = " UNION ALL ".join(
                        "SELECT market_id,event_id,event_ordinal,member_ordinal,group_id,"
                        "member_kind,active,closed,payload_json,payload_hash,metadata_hash "
                        "FROM (SELECT member.market_id,member.event_id,member.event_ordinal,"
                        "member.member_ordinal,member.group_id,member.member_kind,member.active,"
                        "member.closed,member.payload_json,member.payload_hash,"
                        "metadata.payload_hash AS metadata_hash FROM "
                        "structure_sync_event_member_staging member JOIN "
                        "structure_sync_event_metadata_staging metadata ON "
                        "metadata.window_id=member.window_id AND "
                        "metadata.event_id=member.event_id WHERE member.window_id=? AND "
                        "member.market_id=? ORDER BY member.event_id,member.member_ordinal "
                        "LIMIT 2)" for _market_id in market_ids
                    )
                    staged = con.execute(
                        bounded_sidecar_sql,
                        tuple(
                            value for market_id in market_ids
                            for value in (window_id, market_id)
                        ),
                    ).fetchall()
                else:
                    staged = staged_probe
                if inspection_callback is not None:
                    inspection_callback("sidecar-witnesses", len(staged))
                staged_by_market: dict[str, list[tuple[object, ...]]] = {}
                for row in staged:
                    staged_by_market.setdefault(str(row[0]), []).append(tuple(row[1:]))
                sidecar_market_counts.update({
                    market_id: len(rows) for market_id, rows in staged_by_market.items()
                })
                sidecar_rows_by_market.update({
                    market_id: rows
                    for market_id, rows in staged_by_market.items()
                    if len(rows) == 1
                })
                group_keys = sorted({
                    (str(row[0]), str(row[3]))
                    for rows in staged_by_market.values()
                    for row in rows
                    if isinstance(row[3], str) and row[3]
                })
                if group_keys:
                    group_values = ",".join("(?,?)" for _ in group_keys)
                    group_params = tuple(value for key in group_keys for value in key)
                    for truth_row in con.execute(
                        "WITH candidate(event_id,group_id) AS (VALUES "
                        + group_values
                        + ") SELECT truth.event_id,truth.group_id,"
                        "truth.neg_risk_type,truth.quality,truth.reason,"
                        "truth.membership_hash FROM candidate JOIN "
                        "structure_sync_event_group_truth_staging truth ON "
                        "truth.window_id=? AND truth.event_id=candidate.event_id AND "
                        "truth.group_id=candidate.group_id",
                        (*group_params, window_id),
                    ):
                        group_truth_by_key[(str(truth_row[0]), str(truth_row[1]))] = (
                            *truth_row[2:],
                        )
                identity_keys = sorted({
                    (str(item["event_id"]), str(item["market_id"]))
                    for item in candidates if item["kind"] == "event-only"
                })
                identity_values = ",".join("(?,?)" for _ in identity_keys)
                identity_params = tuple(value for key in identity_keys for value in key)
                identity_rows = () if not identity_keys else con.execute(
                    "WITH candidate(event_id,market_id) AS (VALUES " + identity_values + ") "
                    "SELECT candidate.event_id,candidate.market_id,"
                    "EXISTS(SELECT 1 FROM structure_sync_event_member_staging first WHERE "
                    "first.window_id=? AND first.event_id=candidate.event_id AND "
                    "first.market_id=candidate.market_id LIMIT 1)+EXISTS(SELECT 1 FROM "
                    "structure_sync_event_member_staging second WHERE second.window_id=? "
                    "AND second.event_id=candidate.event_id AND "
                    "second.market_id=candidate.market_id LIMIT 1 OFFSET 1),"
                    "(SELECT first.member_ordinal FROM structure_sync_event_member_staging "
                    "first WHERE first.window_id=? AND first.event_id=candidate.event_id "
                    "AND first.market_id=candidate.market_id ORDER BY first.member_ordinal "
                    "LIMIT 1) FROM candidate",
                    (*identity_params, window_id, window_id, window_id),
                ).fetchall()
                if inspection_callback is not None:
                    inspection_callback("identity-cardinalities", len(identity_rows))
                for event_id, member_market_id, count, first_ordinal in identity_rows:
                    sidecar_identity_counts[(str(event_id), str(member_market_id))] = (
                        int(count), int(first_ordinal)
                    )
                event_ids = sorted({
                    str(item["event_id"])
                    for item in candidates if item["kind"] == "event-only"
                })
                if event_ids:
                    event_placeholders = ",".join("?" for _ in event_ids)
                    conflict_rows = con.execute(
                        "SELECT summary.event_id,summary.global_conflict,proof.leaf_hash,"
                        "proof.proof_json FROM structure_sync_event_conflict_summaries "
                        "summary JOIN structure_sync_event_conflict_proofs proof ON "
                        "proof.window_id=summary.window_id AND "
                        "proof.event_id=summary.event_id WHERE summary.window_id=? AND "
                        f"summary.event_id IN ({event_placeholders})",
                        (window_id, *event_ids),
                    ).fetchall()
                    if len(conflict_rows) != len(event_ids):
                        raise ValueError("structure-event-conflict-summary-invalid")
                    for event_id, global_conflict, leaf_hash, proof_json in conflict_rows:
                        expected_leaf = _event_conflict_leaf_hash(
                            window_id=window_id,
                            event_id=str(event_id),
                            global_conflict=bool(global_conflict),
                        )
                        if (
                            str(leaf_hash) != expected_leaf
                            or not _verify_event_conflict_merkle_proof(
                                leaf_hash=expected_leaf,
                                proof_json=str(proof_json),
                                expected_root=str(identity[7]),
                            )
                        ):
                            raise ValueError("structure-event-conflict-summary-invalid")
                    if inspection_callback is not None:
                        inspection_callback("conflict-events", len(conflict_rows))
                    conflict_events.update(
                        str(event_id)
                        for event_id, global_conflict, _leaf_hash, _proof_json
                        in conflict_rows
                        if bool(global_conflict)
                    )
                if classifier_contract == STRUCTURE_DRIFT_CLASSIFIER_V2:
                    issue_keys = []
                    for item in candidates:
                        if item["kind"] != "event-only":
                            continue
                        envelope = {
                            "event_id": item["event_id"],
                            "event_payload_sha256": item["raw_event_hash"],
                            "event_source_ordinal": item["source_ordinal"],
                            "group_id": item["group_id"],
                            "market_id": item["market_id"],
                            "member_ordinal": item["member_ordinal"],
                            "member_payload_sha256": item["raw_market_hash"],
                        }
                        issue_keys.append((
                            str(item["event_id"]), int(item["source_ordinal"]),
                            int(item["member_ordinal"]), str(item["market_id"]),
                            f"{EVENT_ONLY_NEG_RISK_QUARANTINE_REASON}:"
                            f"{structure_market_source_hash(envelope)}",
                        ))
                    if issue_keys:
                        issue_values = ",".join("(?,?,?,?,?)" for _ in issue_keys)
                        issue_params = tuple(value for key in issue_keys for value in key)
                        issue_rows = con.execute(
                            "WITH candidate(event_id,event_ordinal,member_ordinal,market_id,"
                            "raw_payload) AS (VALUES " + issue_values + ") SELECT "
                            "candidate.event_id,candidate.event_ordinal,candidate.member_ordinal "
                            "FROM candidate WHERE EXISTS (SELECT 1 FROM "
                            "structure_generation_issues issue WHERE issue.snapshot_id=? AND "
                            "issue.market_id=candidate.market_id AND "
                            "issue.raw_payload=candidate.raw_payload LIMIT 1)",
                            (*issue_params, generation_snapshot_id),
                        ).fetchall()
                        if inspection_callback is not None:
                            inspection_callback("certified-issues", len(issue_rows))
                        for event_id, event_ordinal, member_ordinal in issue_rows:
                            certified_event_keys.add((
                                str(event_id), int(event_ordinal), int(member_ordinal)
                            ))
                else:
                    expected_issues: list[tuple[object, ...]] = []
                    for candidate_index, item in enumerate(candidates):
                        market_id = str(item["market_id"])
                        if item["kind"] == "event-only":
                            raw_event = raw_events_by_id.get(str(item["event_id"]))
                            expected_issue = (
                                None
                                if raw_event is None
                                else event_only_member_quarantine_issue(
                                    raw_event,
                                    event_source_ordinal=int(item["source_ordinal"]),
                                    market_id=market_id,
                                )
                            )
                        else:
                            raw_market = item["raw_market"]
                            assert isinstance(raw_market, dict)
                            expected_issue = market_quarantine_issue(
                                market_id,
                                raw_market,
                                tuple(relations.get(market_id, ())),
                            )
                        if expected_issue is not None:
                            expected_issues.append((
                                candidate_index,
                                expected_issue["market_id"],
                                expected_issue["layer"],
                                expected_issue["category"],
                                expected_issue["raw_payload"],
                                expected_issue["detail"],
                            ))
                    if expected_issues:
                        issue_values = ",".join("(?,?,?,?,?,?)" for _ in expected_issues)
                        issue_params = tuple(
                            value for expected_issue in expected_issues
                            for value in expected_issue
                        )
                        issue_rows = con.execute(
                            "WITH candidate(candidate_index,market_id,layer,category,"
                            "raw_payload,detail) AS (VALUES " + issue_values + ") SELECT "
                            "candidate.candidate_index FROM candidate WHERE EXISTS "
                            "(SELECT 1 FROM structure_generation_issues issue WHERE "
                            "issue.snapshot_id=? AND "
                            "issue.market_id=candidate.market_id AND "
                            "issue.layer=candidate.layer AND "
                            "issue.category=candidate.category AND "
                            "issue.raw_payload=candidate.raw_payload AND "
                            "issue.detail=candidate.detail LIMIT 1)",
                            (*issue_params, generation_snapshot_id),
                        ).fetchall()
                        certified_issue_candidate_indexes.update(
                            int(row[0]) for row in issue_rows
                        )
                        if inspection_callback is not None:
                            inspection_callback("certified-issues", len(issue_rows))

            members: list[StructuralMemberIdentity] = []
            diagnostics = []
            exclusions: list[FreshProjectionExclusion] = []

            def add_exclusion(
                *,
                reason: str,
                stream: Literal["market", "event-only"],
                envelope: StructureDriftCandidateEnvelope,
                truth: FreshGroupEvidence | None,
            ) -> None:
                exclusions.append(
                    FreshProjectionExclusion(reason, stream, envelope, truth)
                )

            for candidate_index, candidate in enumerate(candidates):
                market_id = str(candidate["market_id"])
                if candidate["kind"] == "event-only":
                    envelope = StructureDriftCandidateEnvelope(
                        side="generation-only",
                        event_id=str(candidate["event_id"]),
                        group_id=candidate["group_id"],
                        market_id=market_id,
                        member_kind=candidate["member_kind"],
                        active=candidate["active"],
                        closed=candidate["closed"],
                        condition_id=None,
                        yes_token_id=None,
                        no_token_id=None,
                        neg_risk=None,
                        incomplete=None,
                        source_ordinal=int(candidate["source_ordinal"]),
                        member_ordinal=int(candidate["member_ordinal"]),
                        raw_event_hash=str(candidate["raw_event_hash"]),
                        raw_market_hash=None,
                    )
                    raw_event = raw_events_by_id.get(str(candidate["event_id"]))
                    identity_count, first_ordinal = sidecar_identity_counts.get(
                        (str(candidate["event_id"]), market_id), (1, 0)
                    )
                    duplicate_identity = identity_count > 1
                    if (
                        classifier_contract == STRUCTURE_DRIFT_CLASSIFIER_V2
                        and duplicate_identity
                        and int(candidate["member_ordinal"]) != first_ordinal
                    ):
                        continue
                    global_conflict = (
                        len(set(relations.get(market_id, ()))) > 1
                        or str(candidate["event_id"]) in conflict_events
                    )
                    group_evidence = (
                        FreshGroupEvidence(
                            event_id=str(candidate["event_id"]),
                            group_id=(
                                str(candidate["group_id"])
                                if isinstance(candidate["group_id"], str)
                                else ""
                            ),
                            neg_risk_type="standard",
                            quality="incomplete-source",
                            reason="conflicting-event-membership",
                            membership_hash="",
                            global_relation_conflict=True,
                        )
                        if global_conflict
                        else None
                    )
                    if (
                        classifier_contract in STRUCTURE_DRIFT_CLASSIFIERS_V3_COMPATIBLE
                        and group_evidence is None
                        and isinstance(candidate["group_id"], str)
                    ):
                        cached_event_truth = group_truth_by_key.get(
                            (str(candidate["event_id"]), str(candidate["group_id"]))
                        )
                        if cached_event_truth is not None:
                            group_evidence = FreshGroupEvidence(
                                event_id=str(candidate["event_id"]),
                                group_id=str(candidate["group_id"]),
                                neg_risk_type=str(cached_event_truth[0]),
                                quality=str(cached_event_truth[1]),
                                reason=(
                                    None
                                    if cached_event_truth[2] is None
                                    else str(cached_event_truth[2])
                                ),
                                membership_hash=str(cached_event_truth[3]),
                                global_relation_conflict=False,
                            )
                    certified = (
                        envelope.event_id is not None
                        and envelope.group_id is not None
                        and envelope.active is True
                        and envelope.closed is False
                        and isinstance(candidate["raw_member"], dict)
                        and type(candidate["raw_member"].get("negRiskOther")) is bool
                        and (
                            candidate_index in certified_issue_candidate_indexes
                            if classifier_contract in STRUCTURE_DRIFT_CLASSIFIERS_V3_COMPATIBLE
                            else (
                                str(candidate["event_id"]),
                                int(candidate["source_ordinal"]),
                                int(candidate["member_ordinal"]),
                            )
                            in certified_event_keys
                        )
                    )
                    sidecar_identity_valid = (
                        bool(market_id)
                        and market_id.strip() == market_id
                        and isinstance(candidate["group_id"], str)
                        and bool(candidate["group_id"])
                        and str(candidate["group_id"]).strip()
                        == candidate["group_id"]
                        and candidate["member_kind"]
                        in {"named", "other", "inactive-reserved"}
                        and type(candidate["active"]) is bool
                        and type(candidate["closed"]) is bool
                    )
                    if classifier_contract in STRUCTURE_DRIFT_CLASSIFIERS_V3_COMPATIBLE:
                        strict_ordinary_event = (
                            raw_event is not None
                            and raw_event.get("negRisk") is False
                            and raw_event.get("enableNegRisk") is False
                            and raw_event.get("negRiskMarketID") is None
                        )
                        nullable_ordinary_event = (
                            classifier_contract == STRUCTURE_DRIFT_CLASSIFIER_V4
                            and raw_event is not None
                            and raw_event.get("negRisk") is None
                            and raw_event.get("enableNegRisk") is False
                            and raw_event.get("negRiskMarketID") is None
                            and candidate["group_id"] is None
                            and isinstance(candidate["raw_member"], dict)
                            and candidate["raw_member"].get("negRiskOther") is False
                        )
                        ordinary_event = strict_ordinary_event or nullable_ordinary_event
                        standard_event = (
                            raw_event is not None
                            and raw_event.get("negRisk") is True
                            and raw_event.get("enableNegRisk") is True
                            and isinstance(raw_event.get("negRiskMarketID"), str)
                            and bool(raw_event["negRiskMarketID"])
                            and raw_event["negRiskMarketID"].strip()
                            == raw_event["negRiskMarketID"]
                            and raw_event.get("negRiskMarketID")
                            == candidate["group_id"]
                        )
                        ordinary_compatible_identity_valid = (
                            bool(market_id)
                            and market_id.strip() == market_id
                            and candidate["member_kind"]
                            in {"named", "other", "inactive-reserved"}
                            and type(candidate["active"]) is bool
                            and type(candidate["closed"]) is bool
                            and (
                                ordinary_event
                                or (
                                    isinstance(candidate["group_id"], str)
                                    and bool(candidate["group_id"])
                                    and str(candidate["group_id"]).strip()
                                    == candidate["group_id"]
                                )
                            )
                        )
                        expected_issue = (
                            None
                            if raw_event is None
                            else event_only_member_quarantine_issue(
                                raw_event,
                                event_source_ordinal=int(candidate["source_ordinal"]),
                                market_id=market_id,
                            )
                        )
                        exact_quarantine = (
                            expected_issue is not None
                            and candidate_index in certified_issue_candidate_indexes
                            and market_id not in generated_market_ids
                            and market_id not in generated_membership_ids
                        )
                        approved_group = (
                            group_evidence is not None
                            and not group_evidence.global_relation_conflict
                        )
                        evidence = FreshMemberEvidence(
                            source_present=True,
                            current_active=envelope.active is True,
                            current_closed=envelope.closed is True,
                            projector_matches=False,
                            generation_certified=True,
                            event_only_quarantine=exact_quarantine,
                            market_side_quarantine=False,
                            absent_from_event_catalog=False,
                            absent_from_market_catalog=True,
                            identity_revalidated=ordinary_compatible_identity_valid,
                            invalid_neg_risk_classification=not (
                                ordinary_event or standard_event
                            ),
                            invalid_event_membership=(
                                not ordinary_compatible_identity_valid
                                and not global_conflict
                            ),
                            duplicate_market_identity=(
                                duplicate_identity and not global_conflict
                            ),
                            uncertified_event_only_member=(
                                standard_event
                                and ordinary_compatible_identity_valid
                                and not global_conflict
                                and not duplicate_identity
                                and approved_group
                                and group_evidence is not None
                                and group_evidence.neg_risk_type == "standard"
                                and group_evidence.quality == "complete-supported"
                                and not exact_quarantine
                            ),
                            group_truth=group_evidence,
                            source_ordinal=envelope.source_ordinal,
                            member_ordinal=envelope.member_ordinal,
                            raw_event_hash=envelope.raw_event_hash,
                        )

                        def diagnose_event_only() -> None:
                            diagnostics.append(
                                diagnose_unresolved_member(
                                    side="generation-only",
                                    member=envelope,
                                    evidence=evidence,
                                    authorized_removal_reasons=(),
                                )
                            )

                        if not (ordinary_event or standard_event):
                            diagnose_event_only()
                        elif (
                            not ordinary_compatible_identity_valid
                            or global_conflict
                            or duplicate_identity
                        ):
                            diagnose_event_only()
                        elif ordinary_event:
                            add_exclusion(
                                reason="non-neg-risk-event-member",
                                stream="event-only",
                                envelope=envelope,
                                truth=None,
                            )
                        elif envelope.active is not True or envelope.closed is not False:
                            add_exclusion(
                                reason="current-nontradable-event-member",
                                stream="event-only",
                                envelope=envelope,
                                truth=group_evidence,
                            )
                        elif (
                            group_evidence is not None
                            and group_evidence.neg_risk_type == "augmented"
                            and group_evidence.quality == "complete-unsupported"
                            and group_evidence.reason
                            == "augmented-neg-risk-not-supported"
                        ):
                            add_exclusion(
                                reason="augmented-group",
                                stream="event-only",
                                envelope=envelope,
                                truth=group_evidence,
                            )
                        elif (
                            group_evidence is not None
                            and group_evidence.neg_risk_type == "standard"
                            and group_evidence.quality == "complete-unsupported"
                            and group_evidence.reason
                            == "standard-neg-risk-has-non-tradable-members"
                        ):
                            add_exclusion(
                                reason="fresh-group-ineligible",
                                stream="event-only",
                                envelope=envelope,
                                truth=group_evidence,
                            )
                        elif exact_quarantine:
                            add_exclusion(
                                reason="event-only-quarantine",
                                stream="event-only",
                                envelope=envelope,
                                truth=group_evidence,
                            )
                        else:
                            diagnose_event_only()
                        continue
                    if certified and not global_conflict and not duplicate_identity:
                        continue
                    evidence = FreshMemberEvidence(
                        source_present=True,
                        current_active=envelope.active is True,
                        current_closed=envelope.closed is True,
                        projector_matches=False,
                        generation_certified=True,
                        event_only_quarantine=False,
                        market_side_quarantine=False,
                        absent_from_event_catalog=False,
                        absent_from_market_catalog=True,
                        identity_revalidated=sidecar_identity_valid,
                        invalid_event_membership=not sidecar_identity_valid,
                        duplicate_market_identity=(
                            duplicate_identity and not global_conflict
                        ),
                        uncertified_event_only_member=not certified,
                        group_truth=group_evidence,
                        source_ordinal=envelope.source_ordinal,
                        member_ordinal=envelope.member_ordinal,
                        raw_event_hash=envelope.raw_event_hash,
                    )
                    diagnostics.append(
                        diagnose_unresolved_member(
                            side="generation-only",
                            member=envelope,
                            evidence=evidence,
                            authorized_removal_reasons=(),
                        )
                    )
                    continue

                raw_market = candidate["raw_market"]
                assert isinstance(raw_market, dict)
                event_ids = tuple(relations.get(market_id, ()))
                exact_rows = staged_by_market.get(market_id, [])
                source_row = exact_rows[0] if exact_rows else None
                source_identity_valid = (
                    source_row is not None
                    and len(exact_rows) == 1
                    and len(event_ids) == 1
                    and raw_market.get("id") == market_id
                    and source_row[0] == event_ids[0]
                    and isinstance(source_row[3], str) and bool(source_row[3])
                    and isinstance(source_row[4], str) and bool(source_row[4])
                    and source_row[5] in (0, 1) and source_row[6] in (0, 1)
                    and raw_market.get("negRiskMarketID") == source_row[3]
                )
                conflict = len(set(event_ids)) > 1 or any(
                    str(row[0]) in conflict_events for row in exact_rows
                )
                projected = project_legacy_compatible_market(
                    raw_market,
                    event_ids=event_ids,
                    taken_at_ms=0,
                )
                row = projected.row
                strict_condition = raw_market.get("conditionId")
                if not (
                    isinstance(strict_condition, str)
                    and strict_condition
                    and strict_condition.strip() == strict_condition
                ):
                    strict_condition = None
                raw_tokens: object = raw_market.get("clobTokenIds")
                if isinstance(raw_tokens, str):
                    try:
                        raw_tokens = json.loads(raw_tokens)
                    except json.JSONDecodeError:
                        raw_tokens = None
                strict_yes_token = (
                    raw_tokens[0]
                    if isinstance(raw_tokens, list)
                    and len(raw_tokens) > 0
                    and isinstance(raw_tokens[0], str)
                    and raw_tokens[0]
                    and raw_tokens[0].strip() == raw_tokens[0]
                    else None
                )
                strict_no_token = (
                    raw_tokens[1]
                    if isinstance(raw_tokens, list)
                    and len(raw_tokens) > 1
                    and isinstance(raw_tokens[1], str)
                    and raw_tokens[1]
                    and raw_tokens[1].strip() == raw_tokens[1]
                    else None
                )
                condition_id = (
                    str(row["condition_id"])
                    if row is not None
                    and isinstance(row.get("condition_id"), str)
                    and row["condition_id"]
                    and strict_condition == row["condition_id"]
                    else None
                )
                yes_token_id = (
                    str(row["yes_token_id"])
                    if row is not None
                    and isinstance(row.get("yes_token_id"), str)
                    and row["yes_token_id"]
                    and strict_yes_token == row["yes_token_id"]
                    else None
                )
                no_token_id = (
                    str(row["no_token_id"])
                    if row is not None
                    and isinstance(row.get("no_token_id"), str)
                    and row["no_token_id"]
                    and strict_no_token == row["no_token_id"]
                    else None
                )
                market_identity_valid = (
                    source_identity_valid
                    and row is not None
                    and condition_id is not None
                    and yes_token_id is not None
                    and no_token_id is not None
                    and row.get("market_id") == market_id
                    and row.get("neg_risk_market_id") == source_row[3]
                    and type(row.get("neg_risk")) is bool
                    and type(row.get("incomplete")) is bool
                )
                member = (
                    StructuralMemberIdentity(
                        event_id=str(source_row[0]),
                        group_id=str(source_row[3]),
                        market_id=market_id,
                        member_kind=str(source_row[4]),
                        active=bool(source_row[5]),
                        closed=bool(source_row[6]),
                        condition_id=condition_id,
                        yes_token_id=yes_token_id,
                        no_token_id=no_token_id,
                        neg_risk=bool(row["neg_risk"]),
                        incomplete=bool(row["incomplete"]),
                    )
                    if source_identity_valid and market_identity_valid
                    else None
                )
                truth_identity = (
                    (str(source_row[0]), str(source_row[3]))
                    if source_row is not None and isinstance(source_row[3], str)
                    else None
                )
                cached_truth = (
                    None
                    if truth_identity is None
                    else group_truth_by_key.get(truth_identity)
                )
                effective_truth = cached_truth or (
                    "standard",
                    "incomplete-source",
                    "event-membership-missing-or-empty",
                    "",
                )
                group_evidence = (
                    FreshGroupEvidence(
                        event_id=truth_identity[0],
                        group_id=truth_identity[1],
                        neg_risk_type=(
                            "standard" if conflict else str(effective_truth[0])
                        ),
                        quality=(
                            "incomplete-source" if conflict else str(effective_truth[1])
                        ),
                        reason=(
                            "conflicting-event-membership"
                            if conflict
                            else effective_truth[2]
                        ),
                        membership_hash=("" if conflict else str(effective_truth[3])),
                        global_relation_conflict=conflict,
                    )
                    if truth_identity is not None
                    else None
                )
                raw_source_member = (
                    json.loads(str(source_row[7])) if source_row is not None else {}
                )
                diagnostic_envelope = StructureDriftCandidateEnvelope(
                    side="generation-only",
                    event_id=(
                        str(source_row[0]) if source_row is not None else None
                    ),
                    group_id=(
                        str(source_row[3])
                        if source_row is not None and isinstance(source_row[3], str)
                        else None
                    ),
                    market_id=market_id,
                    member_kind=(
                        str(source_row[4])
                        if source_row is not None and isinstance(source_row[4], str)
                        else None
                    ),
                    active=(
                        raw_source_member.get("active")
                        if type(raw_source_member.get("active")) is bool
                        else None
                    ),
                    closed=(
                        raw_source_member.get("closed")
                        if type(raw_source_member.get("closed")) is bool
                        else None
                    ),
                    condition_id=condition_id,
                    yes_token_id=yes_token_id,
                    no_token_id=no_token_id,
                    neg_risk=(
                        row.get("neg_risk")
                        if row is not None and type(row.get("neg_risk")) is bool
                        else None
                    ),
                    incomplete=(
                        row.get("incomplete")
                        if row is not None and type(row.get("incomplete")) is bool
                        else None
                    ),
                    source_ordinal=None,
                    member_ordinal=None,
                    raw_event_hash=(
                        str(source_row[9]) if source_row is not None else None
                    ),
                    raw_market_hash=hashlib.sha256(
                        json.dumps(
                            raw_market,
                            sort_keys=True,
                            separators=(",", ":"),
                        ).encode()
                    ).hexdigest(),
                )
                expected_market_issue = market_quarantine_issue(
                    market_id, raw_market, event_ids
                )
                exact_market_quarantine = (
                    expected_market_issue is not None
                    and candidate_index in certified_issue_candidate_indexes
                )
                evidence = FreshMemberEvidence(
                    source_present=True,
                    current_active=raw_market.get("active") is True,
                    current_closed=raw_market.get("closed") is True,
                    projector_matches=member is not None,
                    generation_certified=True,
                    event_only_quarantine=False,
                    market_side_quarantine=(
                        exact_market_quarantine
                        if classifier_contract in STRUCTURE_DRIFT_CLASSIFIERS_V3_COMPATIBLE
                        else expected_market_issue is not None
                    ),
                    absent_from_event_catalog=not event_ids,
                    absent_from_market_catalog=False,
                    projected_member=member,
                    event_source_count=len(event_ids),
                    exact_source_member=member,
                    group_truth=group_evidence,
                    duplicate_market_identity=(
                        sidecar_market_counts.get(market_id, 0) > 1 and not conflict
                    ),
                    identity_revalidated=True,
                    # A globally conflicting identity is valid source evidence of
                    # that conflict, not an invalid local membership.  Preserve a
                    # bounded first witness so the stronger conflict diagnosis is
                    # not masked merely because the market has multiple rows.
                    invalid_event_membership=(
                        not source_identity_valid and not conflict
                    ),
                    invalid_neg_risk_classification=(
                        classifier_contract in STRUCTURE_DRIFT_CLASSIFIERS_V3_COMPATIBLE
                        and type(raw_market.get("negRisk")) is not bool
                    ),
                )
                if classifier_contract in STRUCTURE_DRIFT_CLASSIFIERS_V3_COMPATIBLE:
                    if type(raw_market.get("negRisk")) is not bool:
                        diagnostics.append(
                            diagnose_unresolved_member(
                                side="generation-only",
                                member=diagnostic_envelope,
                                evidence=evidence,
                                authorized_removal_reasons=(),
                            )
                        )
                    elif raw_market.get("negRisk") is False:
                        add_exclusion(
                            reason="non-neg-risk-market",
                            stream="market",
                            envelope=diagnostic_envelope,
                            truth=None,
                        )
                    elif exact_market_quarantine:
                        add_exclusion(
                            reason="market-side-quarantine",
                            stream="market",
                            envelope=diagnostic_envelope,
                            truth=None,
                        )
                    elif (
                        member is None
                        or group_evidence is None
                        or group_evidence.global_relation_conflict
                        or evidence.duplicate_market_identity
                    ):
                        diagnostics.append(
                            diagnose_unresolved_member(
                                side="generation-only",
                                member=diagnostic_envelope,
                                evidence=evidence,
                                authorized_removal_reasons=(),
                            )
                        )
                    elif (
                        group_evidence.neg_risk_type == "augmented"
                        and group_evidence.quality == "complete-unsupported"
                        and group_evidence.reason == "augmented-neg-risk-not-supported"
                    ):
                        add_exclusion(
                            reason="augmented-group",
                            stream="market",
                            envelope=diagnostic_envelope,
                            truth=group_evidence,
                        )
                    elif (
                        group_evidence.neg_risk_type == "standard"
                        and group_evidence.quality == "complete-unsupported"
                        and group_evidence.reason
                        == "standard-neg-risk-has-non-tradable-members"
                    ):
                        add_exclusion(
                            reason="fresh-group-ineligible",
                            stream="market",
                            envelope=diagnostic_envelope,
                            truth=group_evidence,
                        )
                    elif (
                        group_evidence.neg_risk_type == "standard"
                        and group_evidence.quality == "complete-supported"
                        and evidence.current_active
                        and not evidence.current_closed
                    ):
                        members.append(member)
                    else:
                        diagnostics.append(
                            diagnose_unresolved_member(
                                side="generation-only",
                                member=diagnostic_envelope,
                                evidence=evidence,
                                authorized_removal_reasons=(),
                            )
                        )
                    continue
                eligible = (
                    member is not None
                    and group_evidence is not None
                    and group_evidence.quality == "complete-supported"
                    and not group_evidence.global_relation_conflict
                    and evidence.current_active
                    and not evidence.current_closed
                    and not evidence.market_side_quarantine
                    and not evidence.duplicate_market_identity
                )
                if eligible:
                    members.append(member)
                else:
                    diagnostic_member = member or diagnostic_envelope
                    diagnostics.append(
                        diagnose_unresolved_member(
                            side="generation-only",
                            member=diagnostic_member,
                            evidence=evidence,
                            authorized_removal_reasons=(),
                        )
                    )

            next_cursor = None if projection_complete else scan_cursor
            if next_cursor is None and not projection_complete and candidates:
                last = candidates[-1]
                next_cursor = (
                    FreshProjectionCursor(
                        stream="market",
                        market_id=str(last["market_id"]),
                        event_id=None,
                        source_ordinal=None,
                        member_ordinal=None,
                    )
                    if last["kind"] == "market"
                    else FreshProjectionCursor(
                        stream="event-only",
                        market_id=None,
                        event_id=str(last["event_id"]),
                        source_ordinal=int(last["source_ordinal"]),
                        member_ordinal=int(last["member_ordinal"]),
                    )
                )
            con.execute("COMMIT")
            return FreshProjectionChunk(
                cursor=next_cursor,
                members=tuple(members),
                diagnostics=tuple(diagnostics),
                candidates_processed=len(candidates),
                exclusions=tuple(exclusions),
            )

    def advance_structure_drift_fresh_projection_commitment(
        self,
        *,
        publication_id: str,
        generation_snapshot_id: int,
        commitment: FreshProjectionCommitment | None,
        limit: int,
        trace_callback: Callable[[str], None] | None = None,
    ) -> FreshProjectionCommitment:
        """Advance one bounded complete-projection count/root commitment."""
        from polyarb.perception.structure_drift import (
            FreshProjectionCommitment,
            advance_fresh_projection_commitment,
        )

        receipt_digest = self._validated_fresh_projection_member_authority(
            publication_id=publication_id,
            generation_snapshot_id=generation_snapshot_id,
            trace_callback=trace_callback,
        )
        current = commitment or FreshProjectionCommitment.initial(
            publication_id=publication_id,
            generation_snapshot_id=generation_snapshot_id,
            member_receipt_digest=receipt_digest,
        )
        if (
            current.publication_id != publication_id
            or current.generation_snapshot_id != generation_snapshot_id
            or current.member_receipt_digest != receipt_digest
        ):
            raise ValueError("fresh-projection-commitment-identity-mismatch")
        if current.complete:
            return current
        chunk = self._fetch_structure_drift_fresh_projection_chunk(
            publication_id=publication_id,
            generation_snapshot_id=generation_snapshot_id,
            cursor=current.cursor,
            limit=limit,
            trace_callback=trace_callback,
        )
        return advance_fresh_projection_commitment(current, chunk)

    def fetch_structure_drift_member_chunk(
        self,
        *,
        snapshot_id: int,
        generation: bool,
        after_market_id: str | None,
        limit: int,
        trace_callback: Callable[[str], None] | None = None,
    ) -> list[object]:
        """Read one eligible legacy or generation member keyset for classification."""
        from polyarb.perception.structure_drift import StructuralMemberIdentity

        if (
            snapshot_id < 1
            or type(generation) is not bool
            or not 1 <= limit <= STRUCTURE_PUBLICATION_MAX_ROWS
        ):
            raise ValueError("invalid-structure-drift-member-chunk")
        prefix = "structure_generation_" if generation else ""
        truth_table = f"{prefix}group_truth" if generation else "neg_risk_group_truth"
        membership_table = (
            f"{prefix}memberships" if generation else "event_market_memberships"
        )
        market_table = f"{prefix}markets"
        scan_index = (
            "idx_structure_generation_memberships_drift_scan"
            if generation
            else "idx_event_market_memberships_drift_scan"
        )
        cursor_clause = "" if after_market_id is None else "AND m.market_id>? "
        parameters = (
            (snapshot_id, limit)
            if after_market_id is None
            else (snapshot_id, after_market_id, limit)
        )
        with sqlite3.connect(self._db_path) as con:
            if trace_callback is not None:
                con.set_trace_callback(trace_callback)
            rows = con.execute(
                "SELECT m.event_id,m.neg_risk_market_id,m.market_id,m.member_kind,"
                "m.active,m.closed,k.condition_id,k.yes_token_id,k.no_token_id,"
                f"k.neg_risk,k.incomplete FROM {membership_table} m INDEXED BY "
                f"{scan_index} CROSS JOIN {truth_table} t ON "
                "m.snapshot_id=t.snapshot_id AND "
                "m.event_id=t.event_id AND m.neg_risk_market_id=t.neg_risk_market_id "
                f"CROSS JOIN {market_table} k ON k.snapshot_id=m.snapshot_id AND "
                "k.market_id=m.market_id AND k.event_id=m.event_id AND "
                "k.neg_risk_market_id=m.neg_risk_market_id WHERE t.snapshot_id=? "
                "AND t.neg_risk_type='standard' AND t.quality='complete-supported' "
                "AND m.member_kind='named' AND m.active=1 AND m.closed=0 "
                "AND k.active=1 AND k.closed=0 AND k.incomplete=0 "
                "AND trim(k.yes_token_id)!='' "
                + cursor_clause
                + "ORDER BY m.market_id LIMIT ?",
                parameters,
            ).fetchall()
        return [
            StructuralMemberIdentity(
                event_id=str(row[0]),
                group_id=str(row[1]),
                market_id=str(row[2]),
                member_kind=str(row[3]),
                active=bool(row[4]),
                closed=bool(row[5]),
                condition_id=str(row[6] or ""),
                yes_token_id=str(row[7] or ""),
                no_token_id=str(row[8] or ""),
                neg_risk=bool(row[9]),
                incomplete=bool(row[10]),
            )
            for row in rows
        ]

    def fetch_structure_drift_members_by_id(
        self,
        *,
        snapshot_id: int,
        generation: bool,
        market_ids: list[str],
    ) -> list[object]:
        """Bulk-read eligible members for one bounded overlap classification."""
        if (
            snapshot_id < 1
            or type(generation) is not bool
            or len(market_ids) > STRUCTURE_PUBLICATION_MAX_ROWS
            or any(not market_id for market_id in market_ids)
        ):
            raise ValueError("invalid-structure-drift-member-lookup")
        if not market_ids:
            return []
        # Reuse the canonical row conversion while replacing only its keyset SQL.
        from polyarb.perception.structure_drift import StructuralMemberIdentity

        prefix = "structure_generation_" if generation else ""
        truth_table = f"{prefix}group_truth" if generation else "neg_risk_group_truth"
        membership_table = (
            f"{prefix}memberships" if generation else "event_market_memberships"
        )
        market_table = f"{prefix}markets"
        placeholders = ",".join("?" for _ in market_ids)
        with sqlite3.connect(self._db_path) as con:
            rows = con.execute(
                "SELECT m.event_id,m.neg_risk_market_id,m.market_id,m.member_kind,"
                "m.active,m.closed,k.condition_id,k.yes_token_id,k.no_token_id,"
                f"k.neg_risk,k.incomplete FROM {truth_table} t JOIN "
                f"{membership_table} m ON m.snapshot_id=t.snapshot_id AND "
                "m.event_id=t.event_id AND m.neg_risk_market_id=t.neg_risk_market_id "
                f"JOIN {market_table} k ON k.snapshot_id=m.snapshot_id AND "
                "k.market_id=m.market_id AND k.event_id=m.event_id AND "
                "k.neg_risk_market_id=m.neg_risk_market_id WHERE t.snapshot_id=? "
                "AND t.neg_risk_type='standard' AND t.quality='complete-supported' "
                "AND m.member_kind='named' AND m.active=1 AND m.closed=0 "
                "AND k.active=1 AND k.closed=0 AND k.incomplete=0 "
                "AND trim(k.yes_token_id)!='' "
                f"AND m.market_id IN ({placeholders}) ORDER BY m.market_id",
                (snapshot_id, *market_ids),
            ).fetchall()
        return [
            StructuralMemberIdentity(
                event_id=str(row[0]),
                group_id=str(row[1]),
                market_id=str(row[2]),
                member_kind=str(row[3]),
                active=bool(row[4]),
                closed=bool(row[5]),
                condition_id=str(row[6] or ""),
                yes_token_id=str(row[7] or ""),
                no_token_id=str(row[8] or ""),
                neg_risk=bool(row[9]),
                incomplete=bool(row[10]),
            )
            for row in rows
        ]

    def fetch_structure_drift_group_truth_chunk(
        self,
        *,
        publication_id: str,
        generation_snapshot_id: int,
        after_key: tuple[str, str] | None,
        limit: int,
        trace_callback: Callable[[str], None] | None = None,
    ) -> list[tuple[object, ...]]:
        """Read one authenticated generation group-truth keyset."""
        if (
            not publication_id
            or generation_snapshot_id < 1
            or not 1 <= limit <= STRUCTURE_PUBLICATION_MAX_ROWS
            or (
                after_key is not None
                and (len(after_key) != 2 or not after_key[0] or not after_key[1])
            )
        ):
            raise ValueError("invalid-structure-drift-group-truth-chunk")
        after_event_id = None if after_key is None else after_key[0]
        after_group_id = None if after_key is None else after_key[1]
        with sqlite3.connect(self._db_path) as con:
            if trace_callback is not None:
                con.set_trace_callback(trace_callback)
            identity = con.execute(
                "SELECT p.status,p.validation_hash,p.certification_hash,window.status,"
                "window.published_snapshot_id FROM structure_publications p JOIN "
                "structure_sync_windows window ON window.id=p.window_id WHERE "
                "p.publication_id=? AND p.snapshot_id=?",
                (publication_id, generation_snapshot_id),
            ).fetchone()
            if (
                identity is None
                or identity[0] != "published"
                or not isinstance(identity[1], str)
                or len(identity[1]) != 64
                or not isinstance(identity[2], str)
                or len(identity[2]) != 64
                or identity[3] != "published"
                or identity[4] != generation_snapshot_id
            ):
                raise ValueError("structure-drift-source-identity-mismatch")
            rows = con.execute(
                "SELECT event_id,neg_risk_market_id,neg_risk_type,"
                "expected_member_count,active_named_count,membership_hash,quality,"
                "reason FROM structure_generation_group_truth WHERE snapshot_id=? AND "
                "(? IS NULL OR event_id>? OR (event_id=? AND "
                "neg_risk_market_id>?)) ORDER BY event_id,neg_risk_market_id LIMIT ?",
                (
                    generation_snapshot_id,
                    after_event_id,
                    after_event_id,
                    after_event_id,
                    after_group_id,
                    limit,
                ),
            ).fetchall()
        return [tuple(row) for row in rows]

    def _advance_structure_drift_fresh_projection_chunk(
        self,
        comparison_id: str,
        *,
        max_rows: int,
        now_ms: int,
    ) -> StructureCertificationChunk:
        """Checkpoint one authenticated sidecar projection chunk atomically."""
        from polyarb.perception.structure_drift import (
            FreshProjectionChunk,
            FreshProjectionCommitment,
            FreshProjectionCursor,
            advance_fresh_projection_commitment,
            projection_missing_diagnostic,
            structure_drift_diagnostic_sample,
        )

        with sqlite3.connect(self._db_path) as read_con:
            progress = read_con.execute(
                "SELECT generation_snapshot_id,publication_id,phase,row_cursor_json,"
                "class_counts_json,class_digests_json,diagnostic_counts_json,"
                "diagnostic_digest_state_json,diagnostic_samples_json,checkpoint_at_ms,"
                "legacy_snapshot_id,classifier_contract_version,"
                "projection_candidate_count,projection_exclusion_count,"
                "projection_exclusion_counts_json,projection_exclusion_roots_json,"
                "projection_exclusion_digest_states_json "
                "FROM structure_generation_drift_progress WHERE comparison_id=?",
                (comparison_id,),
            ).fetchone()
        if (
            progress is None
            or progress[2] != "fresh-projection-members"
            or progress[11] != STRUCTURE_DRIFT_CLASSIFIER_V4
        ):
            raise ValueError("structure-drift-fresh-projection-phase-invalid")
        counts = json.loads(str(progress[4]))
        digests = json.loads(str(progress[5]))
        diagnostic_counts = json.loads(str(progress[6]))
        diagnostic_samples = json.loads(str(progress[8]))
        if not all(
            isinstance(value, dict)
            for value in (counts, digests, diagnostic_counts, diagnostic_samples)
        ):
            raise ValueError("structure-drift-progress-invalid")
        cursor_payload = (
            None if progress[3] is None else json.loads(str(progress[3]))
        )
        cursor = None
        if cursor_payload is not None:
            if not isinstance(cursor_payload, dict):
                raise ValueError("structure-drift-progress-invalid")
            try:
                cursor = FreshProjectionCursor(
                    stream=cursor_payload["stream"],
                    market_id=cursor_payload["market_id"],
                    event_id=cursor_payload["event_id"],
                    source_ordinal=cursor_payload["source_ordinal"],
                    member_ordinal=cursor_payload["member_ordinal"],
                )
            except (KeyError, TypeError) as error:
                raise ValueError("structure-drift-progress-invalid") from error
        member_state = digests.get("projection_member_state")
        if not isinstance(member_state, str):
            raise ValueError("structure-drift-progress-invalid")
        member_receipt_digest = self._validated_fresh_projection_member_authority(
            publication_id=str(progress[1]),
            generation_snapshot_id=int(progress[0]),
            trace_callback=None,
        )
        stored_member_receipt = digests.get("projection_member_receipt_digest")
        if stored_member_receipt not in {None, member_receipt_digest}:
            raise ValueError("structure-drift-fresh-projection-receipt-mismatch")
        commitment = FreshProjectionCommitment(
            publication_id=str(progress[1]),
            generation_snapshot_id=int(progress[0]),
            member_receipt_digest=member_receipt_digest,
            classifier_contract_version=STRUCTURE_DRIFT_CLASSIFIER_V4,
            cursor=cursor,
            candidates_processed=int(progress[12]),
            member_count=int(counts.get("projection_member_count", 0)),
            member_digest_state=member_state,
            exclusion_count=int(progress[13]),
            exclusion_counts_json=str(progress[14]),
            exclusion_digest_states_json=str(progress[16]),
            diagnostic_count=int(counts.get("projection_diagnostic_count", 0)),
            diagnostic_digest_state=str(progress[7]),
            complete=False,
        )
        chunk = self._fetch_structure_drift_fresh_projection_chunk(
            publication_id=str(progress[1]),
            generation_snapshot_id=int(progress[0]),
            cursor=cursor,
            limit=max_rows,
            classifier_contract=STRUCTURE_DRIFT_CLASSIFIER_V4,
        )
        unresolved_diagnostics = tuple(chunk.diagnostics)
        projected_ids = [item.market_id for item in chunk.members]
        generation_ids = {
            item.market_id
            for item in self.fetch_structure_drift_members_by_id(
                snapshot_id=int(progress[0]),
                generation=True,
                market_ids=projected_ids,
            )
        }
        unresolved_diagnostics = (
            *unresolved_diagnostics,
            *(
                projection_missing_diagnostic(item)
                for item in chunk.members
                if item.market_id not in generation_ids
            ),
        )
        committed_members = tuple(
            item for item in chunk.members if item.market_id in generation_ids
        )
        commitment_chunk = FreshProjectionChunk(
            cursor=chunk.cursor,
            members=committed_members,
            diagnostics=unresolved_diagnostics,
            candidates_processed=chunk.candidates_processed,
            exclusions=chunk.exclusions,
        )
        advanced = advance_fresh_projection_commitment(commitment, commitment_chunk)
        for diagnostic in unresolved_diagnostics:
            diagnostic_counts[diagnostic.code] = (
                int(diagnostic_counts.get(diagnostic.code, 0)) + 1
            )
            samples = diagnostic_samples.get(diagnostic.code, [])
            if not isinstance(samples, list):
                raise ValueError("structure-drift-progress-invalid")
            samples.append(structure_drift_diagnostic_sample(diagnostic))
            samples.sort(
                key=lambda item: json.dumps(
                    item, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                )
            )
            diagnostic_samples[diagnostic.code] = samples[:3]
        counts["projection_candidate_count"] = advanced.candidates_processed
        counts["projection_member_count"] = advanced.member_count
        counts["projection_diagnostic_count"] = advanced.diagnostic_count
        exclusion_roots_json = str(progress[15])
        digests["projection_member_receipt_digest"] = member_receipt_digest
        digests["projection_member_state"] = advanced.member_digest_state
        cursor_json = None
        if advanced.cursor is not None:
            cursor_json = json.dumps(
                {
                    "stream": advanced.cursor.stream,
                    "market_id": advanced.cursor.market_id,
                    "event_id": advanced.cursor.event_id,
                    "source_ordinal": advanced.cursor.source_ordinal,
                    "member_ordinal": advanced.cursor.member_ordinal,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        next_phase = "fresh-projection-members"
        next_digest = advanced.member_digest_state
        if advanced.complete:
            digests.pop("projection_member_state")
            digests["projection_member_root"] = advanced.root
            counts["projection_member_complete"] = 1
            counts["phase_row_count"] = 0
            next_phase = "generation-members"
            next_digest = RowChainSHA256.new("generation-group-truth").to_json()
            cursor_json = None
            exclusion_roots_json = json.dumps(
                advanced.exclusion_roots,
                sort_keys=True,
                separators=(",", ":"),
            )
        samples_json = json.dumps(
            diagnostic_samples, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        prior_checkpoint = int(progress[9])
        writer = self._connect_writer()
        try:
            writer.execute("BEGIN IMMEDIATE")
            changed = writer.execute(
                "UPDATE structure_generation_drift_progress SET phase=?,"
                "row_cursor_json=?,digest_state_json=?,class_counts_json=?,"
                "class_digests_json=?,diagnostic_counts_json=?,"
                "diagnostic_digest_state_json=?,diagnostic_root=?,"
                "diagnostic_samples_json=?,diagnostic_samples_digest=?,"
                "projection_member_receipt_digest=?,projection_candidate_count=?,"
                "projection_exclusion_count=?,projection_exclusion_counts_json=?,"
                "projection_exclusion_roots_json=?,"
                "projection_exclusion_digest_states_json=?,checkpoint_at_ms=? "
                "WHERE comparison_id=? AND "
                "phase='fresh-projection-members' AND checkpoint_at_ms=?",
                (
                    next_phase,
                    cursor_json,
                    next_digest,
                    json.dumps(counts, sort_keys=True, separators=(",", ":")),
                    json.dumps(digests, sort_keys=True, separators=(",", ":")),
                    json.dumps(
                        diagnostic_counts, sort_keys=True, separators=(",", ":")
                    ),
                    advanced.diagnostic_digest_state,
                    advanced.diagnostic_root if advanced.complete else None,
                    samples_json,
                    hashlib.sha256(samples_json.encode()).hexdigest(),
                    member_receipt_digest,
                    advanced.candidates_processed,
                    advanced.exclusion_count,
                    advanced.exclusion_counts_json,
                    exclusion_roots_json,
                    advanced.exclusion_digest_states_json,
                    now_ms,
                    comparison_id,
                    prior_checkpoint,
                ),
            )
            if changed.rowcount != 1:
                raise StructurePublicationCursorError(
                    "structure-drift-cursor-mismatch"
                )
            writer.execute("COMMIT")
        except BaseException:
            if writer.in_transaction:
                writer.execute("ROLLBACK")
            raise
        finally:
            writer.close()
        return StructureCertificationChunk(
            next_phase,
            cursor_json,
            chunk.candidates_processed,
            False,
        )

    def initialize_structure_drift_comparison(self, *, now_ms: int) -> str:
        """Pin one current exact-receipt identity for bounded drift comparison."""
        if now_ms < 0:
            raise ValueError("invalid-structure-drift-comparison-time")
        con = self._connect_writer()
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                "SELECT current.snapshot_id,current.publication_id,"
                "current.validation_hash,current.comparison_receipt_digest,"
                "publication.window_id,publication.normalization_contract_version,"
                "publication.validation_hash,publication.certification_hash,"
                "publication.certification_counts_json,publication.status,window.status,"
                "window.published_snapshot_id,receipt.legacy_snapshot_id,"
                "receipt.legacy_market_count,receipt.generation_market_count,"
                "receipt.legacy_universe_hash,receipt.generation_universe_hash,"
                "receipt.legacy_source_truth_hash,receipt.generation_source_truth_hash,"
                "receipt.generation_validation_hash,receipt.created_at_ms,"
                "receipt.receipt_digest FROM current_structure_generation current JOIN "
                "structure_publications publication ON "
                "publication.publication_id=current.publication_id AND "
                "publication.snapshot_id=current.snapshot_id JOIN "
                "structure_sync_windows window ON window.id=publication.window_id JOIN "
                "structure_generation_comparison_receipts receipt ON "
                "receipt.generation_snapshot_id=current.snapshot_id AND "
                "receipt.publication_id=current.publication_id WHERE current.id=1"
            ).fetchone()
            if (
                row is None
                or row[3] != row[21]
                or row[5] is None
                or not str(row[5])
                or row[6] != row[2]
                or not isinstance(row[7], str)
                or len(row[7]) != 64
                or row[9] != "published"
                or row[10] != "published"
                or row[11] != row[0]
                or row[19] != row[2]
            ):
                raise ValueError("structure-drift-current-identity-invalid")
            legacy_identity = self._comparison_legacy_identity(con)
            if legacy_identity is None or legacy_identity[0] != int(row[12]):
                raise ValueError("structure-drift-legacy-identity-invalid")
            authenticated_exact_digest = _comparison_receipt_digest(
                generation_snapshot_id=int(row[0]),
                publication_id=str(row[1]),
                legacy_snapshot_id=int(row[12]),
                legacy_market_count=int(row[13]),
                generation_market_count=int(row[14]),
                legacy_universe_hash=str(row[15]),
                generation_universe_hash=str(row[16]),
                legacy_source_truth_hash=str(row[17]),
                generation_source_truth_hash=str(row[18]),
                generation_validation_hash=str(row[19]),
                created_at_ms=int(row[20]),
            )
            if authenticated_exact_digest != row[21]:
                raise ValueError("structure-drift-exact-receipt-invalid")
            if (
                int(row[13]) == int(row[14])
                and row[15] == row[16]
                and row[17] == row[18]
            ):
                raise ValueError("structure-drift-exact-already-matches")
            try:
                certification_counts = json.loads(str(row[8]))
                source_event_count = certification_counts["source_events"]
                source_market_count = certification_counts["source_markets"]
            except (KeyError, TypeError, json.JSONDecodeError) as error:
                raise ValueError("structure-drift-source-counts-invalid") from error
            if (
                type(source_event_count) is not int
                or source_event_count < 0
                or type(source_market_count) is not int
                or source_market_count < 0
            ):
                raise ValueError("structure-drift-source-counts-invalid")
            identity = (
                *legacy_identity,
                int(row[0]),
                str(row[1]),
                str(row[4]),
                str(row[5]),
                str(row[21]),
                str(row[2]),
                str(row[7]),
                source_event_count,
                source_market_count,
                ROW_CHAIN_SHA256_V2,
            )
            comparison_id = _structure_drift_comparison_id(
                identity,
                classifier_contract_version=STRUCTURE_DRIFT_CLASSIFIER_V4,
            )
            active = con.execute(
                "SELECT comparison_id,hash_algorithm,classifier_contract_version FROM "
                "structure_generation_drift_progress "
                "WHERE phase NOT IN ('sealed','stale') LIMIT 1"
            ).fetchone()
            if active is not None and (
                active[1] == "serializable-sha256-v1"
                or active[2] != STRUCTURE_DRIFT_CLASSIFIER_V4
            ):
                superseded_reason = (
                    "drift-hash-algorithm-superseded"
                    if active[1] == "serializable-sha256-v1"
                    else "drift-classifier-contract-superseded"
                )
                superseded = con.execute(
                    "UPDATE structure_generation_drift_progress SET phase='stale',"
                    "terminal_reason=?,"
                    "checkpoint_at_ms=? WHERE comparison_id=? AND "
                    "phase NOT IN ('sealed','stale')",
                    (superseded_reason, now_ms, str(active[0])),
                )
                if superseded.rowcount != 1:
                    raise StructurePublicationCursorError(
                        "structure-drift-cursor-mismatch"
                    )
                active = None
            if active is not None and active[0] != comparison_id:
                raise ValueError("structure-drift-active-identity-mismatch")
            source_state = RowChainSHA256.new("source-event")
            group_state = RowChainSHA256.new("source-group-truth")
            exclusion_counts_json = json.dumps(
                {reason: 0 for reason in STRUCTURE_PROJECTION_EXCLUSION_REASONS},
                sort_keys=True,
                separators=(",", ":"),
            )
            exclusion_states_json = json.dumps(
                {
                    reason: RowChainSHA256.new(
                        f"projection-exclusion/{reason}"
                    ).to_json()
                    for reason in STRUCTURE_PROJECTION_EXCLUSION_REASONS
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            con.execute(
                "INSERT OR IGNORE INTO structure_generation_drift_progress("
                "comparison_id,hash_algorithm,classifier_contract_version,"
                "legacy_snapshot_id,"
                "generation_snapshot_id,publication_id,"
                "window_id,normalization_contract_version,exact_receipt_digest,"
                "pointer_validation_hash,generation_certification_hash,"
                "source_event_count,source_market_count,source_event_hash,"
                "source_market_hash,source_identity_hash,phase,row_cursor_json,"
                "digest_state_json,class_counts_json,class_digests_json,"
                "diagnostic_counts_json,diagnostic_digest_state_json,diagnostic_root,"
                "diagnostic_samples_json,diagnostic_samples_digest,created_at_ms,"
                "checkpoint_at_ms,projection_candidate_count,"
                "projection_exclusion_count,projection_exclusion_counts_json,"
                "projection_exclusion_roots_json,"
                "projection_exclusion_digest_states_json) VALUES "
                "(?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,NULL,NULL,'source-events',NULL,"
                "?,?,?,?,?,NULL,?,?,?,?,0,0,?, '{}',?)",
                (
                    comparison_id,
                    ROW_CHAIN_SHA256_V2,
                    STRUCTURE_DRIFT_CLASSIFIER_V4,
                    int(row[12]),
                    int(row[0]),
                    str(row[1]),
                    str(row[4]),
                    str(row[5]),
                    str(row[21]),
                    str(row[2]),
                    str(row[7]),
                    source_event_count,
                    source_market_count,
                    source_state.to_json(),
                    json.dumps(
                        {"phase_row_count": 0, "source_group_truth_count": 0},
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    json.dumps(
                        {
                            "source_group_truth_state": group_state.to_json(),
                            "projection_member_state": RowChainSHA256.new(
                                "projection-member"
                            ).to_json(),
                        },
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    "{}",
                    RowChainSHA256.new("diagnostic/unclassified").to_json(),
                    "{}",
                    hashlib.sha256(b"{}").hexdigest(),
                    now_ms,
                    now_ms,
                    exclusion_counts_json,
                    exclusion_states_json,
                ),
            )
            con.execute("COMMIT")
            return comparison_id
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def latest_structure_comparison_progress(self) -> dict[str, object] | None:
        """Return the one active durable comparison cursor for operator reads."""
        with sqlite3.connect(f"file:{self._db_path}?mode=ro", uri=True) as con:
            row = con.execute(
                "SELECT publication_id,generation_snapshot_id,phase,phase_row_count,"
                "row_cursor_json,checkpoint_at_ms FROM "
                "structure_generation_comparison_progress WHERE phase!='sealed' "
                "ORDER BY checkpoint_at_ms DESC,publication_id DESC LIMIT 1"
            ).fetchone()
        if row is None:
            return None
        return {
            "publication_id": str(row[0]),
            "generation_snapshot_id": int(row[1]),
            "phase": str(row[2]),
            "phase_row_count": int(row[3]),
            "row_cursor": None if row[4] is None else json.loads(str(row[4])),
            "checkpoint_at_ms": int(row[5]),
        }

    def advance_structure_drift_comparison_chunk(
        self,
        comparison_id: str,
        *,
        max_rows: int,
        now_ms: int,
    ) -> StructureCertificationChunk:
        """Advance one source projection chunk under immutable identity CAS."""
        from polyarb.perception.structure_drift import project_legacy_compatible_event

        if (
            not comparison_id
            or not 1 <= max_rows <= STRUCTURE_PUBLICATION_MAX_ROWS
            or now_ms < 0
        ):
            raise ValueError("invalid-structure-drift-comparison-chunk")
        # Recompute the complete current identity before reading a source chunk.
        if self.initialize_structure_drift_comparison(now_ms=now_ms) != comparison_id:
            raise ValueError("structure-drift-current-identity-invalid")
        with sqlite3.connect(self._db_path) as phase_con:
            phase_row = phase_con.execute(
                "SELECT phase FROM structure_generation_drift_progress WHERE comparison_id=?",
                (comparison_id,),
            ).fetchone()
        if phase_row is not None and phase_row[0] in {
            "generation-members",
            "legacy-members",
        }:
            return self._advance_structure_drift_member_chunk(
                comparison_id,
                max_rows=max_rows,
                now_ms=now_ms,
            )
        if phase_row is not None and phase_row[0] == "fresh-projection-members":
            return self._advance_structure_drift_fresh_projection_chunk(
                comparison_id,
                max_rows=max_rows,
                now_ms=now_ms,
            )
        if phase_row is not None and phase_row[0] == "fresh-group-truth":
            return self._advance_structure_drift_group_truth_chunk(
                comparison_id,
                max_rows=max_rows,
                now_ms=now_ms,
            )
        with sqlite3.connect(self._db_path) as read_con:
            progress = read_con.execute(
                "SELECT legacy_snapshot_id,generation_snapshot_id,publication_id,"
                "window_id,normalization_contract_version,exact_receipt_digest,"
                "pointer_validation_hash,generation_certification_hash,"
                "source_event_count,source_market_count,source_event_hash,"
                "source_market_hash,source_identity_hash,phase,row_cursor_json,"
                "digest_state_json,class_counts_json,class_digests_json,checkpoint_at_ms "
                "FROM structure_generation_drift_progress WHERE comparison_id=?",
                (comparison_id,),
            ).fetchone()
            if progress is None or progress[13] not in {
                "source-events",
                "source-markets",
            }:
                raise ValueError("structure-drift-source-phase-invalid")
            phase = str(progress[13])
            cursor = None if progress[14] is None else json.loads(str(progress[14]))
            digest = RowChainSHA256.from_json(
                str(progress[15]),
                expected_domain=(
                    "source-event" if phase == "source-events" else "source-market"
                ),
            )
            counts = json.loads(str(progress[16]))
            digests = json.loads(str(progress[17]))
            phase_count = counts.get("phase_row_count")
            if type(phase_count) is not int or phase_count < 0:
                raise ValueError("structure-drift-progress-invalid")
            if phase == "source-events":
                rows = self.fetch_structure_drift_event_source_chunk(
                    publication_id=str(progress[2]),
                    generation_snapshot_id=int(progress[1]),
                    after_event_id=cursor,
                    limit=min(max_rows, STRUCTURE_DRIFT_SOURCE_EVENT_MAX_ROWS),
                )
                group_state_value = digests.get("source_group_truth_state")
                group_count = counts.get("source_group_truth_count")
                if (
                    not isinstance(group_state_value, str)
                    or type(group_count) is not int
                ):
                    raise ValueError("structure-drift-progress-invalid")
                group_digest = RowChainSHA256.from_json(
                    group_state_value,
                    expected_domain="source-group-truth",
                )
                for ordinal, event_id, raw, market_ids in rows:
                    digest.update((ordinal, event_id, raw))
                    phase_count += 1
                    projection = project_legacy_compatible_event(
                        raw,
                        event_source_ordinal=ordinal,
                        complete_market_ids=market_ids,
                    )
                    for truth in projection.truths:
                        group_digest.update(
                            (
                                truth.event_id,
                                truth.group_id,
                                truth.neg_risk_type,
                                truth.expected_member_count,
                                truth.active_named_count,
                                truth.membership_hash,
                                truth.quality,
                                truth.reason,
                            )
                        )
                        group_count += 1
                counts["source_group_truth_count"] = group_count
                digests["source_group_truth_state"] = group_digest.to_json()
                next_cursor = None if not rows else rows[-1][1]
            else:
                market_rows = self.fetch_structure_drift_market_source_chunk(
                    publication_id=str(progress[2]),
                    generation_snapshot_id=int(progress[1]),
                    after_market_id=cursor,
                    limit=max_rows,
                )
                rows = market_rows
                for market_id, raw, event_ids, _taken_at_ms in market_rows:
                    digest.update((market_id, raw, event_ids))
                    phase_count += 1
                next_cursor = None if not rows else rows[-1][0]
            counts["phase_row_count"] = phase_count
            prior_checkpoint = int(progress[18])
        writer = self._connect_writer()
        try:
            writer.execute("BEGIN IMMEDIATE")
            current = writer.execute(
                "SELECT current.snapshot_id,current.publication_id,"
                "current.validation_hash,current.comparison_receipt_digest,"
                "publication.window_id,publication.normalization_contract_version,"
                "publication.certification_hash,publication.status,window.status,"
                "window.published_snapshot_id FROM current_structure_generation current "
                "JOIN structure_publications publication ON "
                "publication.publication_id=current.publication_id AND "
                "publication.snapshot_id=current.snapshot_id JOIN "
                "structure_sync_windows window ON window.id=publication.window_id "
                "WHERE current.id=1"
            ).fetchone()
            legacy = self._comparison_legacy_identity(writer)
            if (
                current is None
                or legacy is None
                or legacy[0] != int(progress[0])
                or current[0] != int(progress[1])
                or current[1] != progress[2]
                or current[2] != progress[6]
                or current[3] != progress[5]
                or current[4] != progress[3]
                or current[5] != progress[4]
                or current[6] != progress[7]
                or current[7] != "published"
                or current[8] != "published"
                or current[9] != int(progress[1])
            ):
                raise ValueError("structure-drift-current-identity-invalid")
            if rows:
                changed = writer.execute(
                    "UPDATE structure_generation_drift_progress SET row_cursor_json=?,"
                    "digest_state_json=?,class_counts_json=?,class_digests_json=?,"
                    "checkpoint_at_ms=? WHERE comparison_id=? AND phase=? AND "
                    "checkpoint_at_ms=?",
                    (
                        json.dumps(next_cursor),
                        digest.to_json(),
                        json.dumps(counts, sort_keys=True, separators=(",", ":")),
                        json.dumps(digests, sort_keys=True, separators=(",", ":")),
                        now_ms,
                        comparison_id,
                        phase,
                        prior_checkpoint,
                    ),
                )
                next_phase = phase
            else:
                expected_count = int(
                    progress[8] if phase == "source-events" else progress[9]
                )
                if phase_count != expected_count:
                    raise ValueError("structure-drift-source-count-mismatch")
                final_hash = digest.hexdigest()
                counts["phase_row_count"] = 0
                if phase == "source-events":
                    group_digest = RowChainSHA256.from_json(
                        str(digests.pop("source_group_truth_state")),
                        expected_domain="source-group-truth",
                    )
                    digests["source_group_truth_hash"] = group_digest.hexdigest()
                    next_digest = RowChainSHA256.new("source-market")
                    next_phase = "source-markets"
                    source_event_hash = final_hash
                    source_market_hash = None
                    source_identity_hash = None
                else:
                    next_digest = RowChainSHA256.new("projection-member")
                    digests["generation_source_group_truth_comparison_state"] = (
                        RowChainSHA256.new("source-group-truth").to_json()
                    )
                    next_phase = "fresh-projection-members"
                    source_event_hash = str(progress[10])
                    source_market_hash = final_hash
                    source_identity = RowChainSHA256.new("source-identity")
                    source_identity.update(
                        (
                            int(progress[8]),
                            source_event_hash,
                            int(progress[9]),
                            source_market_hash,
                        )
                    )
                    source_identity_hash = source_identity.hexdigest()
                changed = writer.execute(
                    "UPDATE structure_generation_drift_progress SET phase=?,"
                    "row_cursor_json=NULL,digest_state_json=?,class_counts_json=?,"
                    "class_digests_json=?,source_event_hash=COALESCE(?,source_event_hash),"
                    "source_market_hash=COALESCE(?,source_market_hash),"
                    "source_identity_hash=COALESCE(?,source_identity_hash),"
                    "checkpoint_at_ms=? WHERE comparison_id=? AND phase=? AND "
                    "checkpoint_at_ms=?",
                    (
                        next_phase,
                        next_digest.to_json(),
                        json.dumps(counts, sort_keys=True, separators=(",", ":")),
                        json.dumps(digests, sort_keys=True, separators=(",", ":")),
                        source_event_hash,
                        source_market_hash,
                        source_identity_hash,
                        now_ms,
                        comparison_id,
                        phase,
                        prior_checkpoint,
                    ),
                )
            if changed.rowcount != 1:
                raise StructurePublicationCursorError("structure-drift-cursor-mismatch")
            writer.execute("COMMIT")
        except BaseException:
            if writer.in_transaction:
                writer.execute("ROLLBACK")
            raise
        finally:
            writer.close()
        return StructureCertificationChunk(
            next_phase,
            None if not rows else json.dumps(next_cursor),
            len(rows),
            False,
        )

    def _advance_structure_drift_group_truth_chunk(
        self,
        comparison_id: str,
        *,
        max_rows: int,
        now_ms: int,
    ) -> StructureCertificationChunk:
        """Hash fresh generation truth and fail closed on any exact mismatch."""
        from polyarb.perception.structure_drift import (
            reconstruction_root_from_class_commitments,
        )

        with sqlite3.connect(self._db_path) as read_con:
            progress = read_con.execute(
                "SELECT legacy_snapshot_id,generation_snapshot_id,publication_id,"
                "window_id,normalization_contract_version,exact_receipt_digest,"
                "pointer_validation_hash,generation_certification_hash,phase,"
                "row_cursor_json,digest_state_json,class_counts_json,"
                "class_digests_json,checkpoint_at_ms,diagnostic_counts_json,"
                "diagnostic_digest_state_json,diagnostic_samples_json,"
                "diagnostic_samples_digest,source_identity_hash,created_at_ms,"
                "classifier_contract_version,projection_candidate_count,"
                "projection_exclusion_count,projection_exclusion_counts_json,"
                "projection_exclusion_roots_json,"
                "projection_exclusion_digest_states_json FROM "
                "structure_generation_drift_progress WHERE comparison_id=?",
                (comparison_id,),
            ).fetchone()
        if progress is None or progress[8] != "fresh-group-truth":
            raise ValueError("structure-drift-group-truth-phase-invalid")
        cursor_value = None if progress[9] is None else json.loads(str(progress[9]))
        if cursor_value is None:
            cursor = None
        elif (
            isinstance(cursor_value, list)
            and len(cursor_value) == 2
            and all(isinstance(value, str) and value for value in cursor_value)
        ):
            cursor = (cursor_value[0], cursor_value[1])
        else:
            raise ValueError("structure-drift-progress-invalid")
        counts = json.loads(str(progress[11]))
        digests = json.loads(str(progress[12]))
        if counts.get("fresh_group_truth_complete") == 1:
            raise ValueError("structure-drift-group-truth-already-complete")
        phase_count = counts.get("phase_row_count")
        if type(phase_count) is not int or phase_count < 0:
            raise ValueError("structure-drift-progress-invalid")
        digest = RowChainSHA256.from_json(
            str(progress[10]),
            expected_domain="generation-group-truth",
        )
        comparison_state = digests.get(
            "generation_source_group_truth_comparison_state"
        )
        if not isinstance(comparison_state, str):
            raise ValueError("structure-drift-progress-invalid")
        comparison_digest = RowChainSHA256.from_json(
            comparison_state,
            expected_domain="source-group-truth",
        )
        rows = self.fetch_structure_drift_group_truth_chunk(
            publication_id=str(progress[2]),
            generation_snapshot_id=int(progress[1]),
            after_key=cursor,
            limit=max_rows,
        )
        for row in rows:
            digest.update(row)
            comparison_digest.update(row)
            phase_count += 1
        counts["generation_source_group_truth_comparison_count"] = (
            comparison_digest.count
        )
        digests["generation_source_group_truth_comparison_state"] = (
            comparison_digest.to_json()
        )
        counts["phase_row_count"] = phase_count
        next_cursor = None if not rows else (str(rows[-1][0]), str(rows[-1][1]))
        prior_checkpoint = int(progress[13])

        writer = self._connect_writer()
        try:
            writer.execute("BEGIN IMMEDIATE")
            current = writer.execute(
                "SELECT current.snapshot_id,current.publication_id,"
                "current.validation_hash,current.comparison_receipt_digest,"
                "publication.window_id,publication.normalization_contract_version,"
                "publication.certification_hash,publication.status,window.status,"
                "window.published_snapshot_id FROM current_structure_generation current "
                "JOIN structure_publications publication ON "
                "publication.publication_id=current.publication_id AND "
                "publication.snapshot_id=current.snapshot_id JOIN "
                "structure_sync_windows window ON window.id=publication.window_id "
                "WHERE current.id=1"
            ).fetchone()
            legacy = self._comparison_legacy_identity(writer)
            if (
                current is None
                or legacy is None
                or legacy[0] != int(progress[0])
                or current[0] != int(progress[1])
                or current[1] != progress[2]
                or current[2] != progress[6]
                or current[3] != progress[5]
                or current[4] != progress[3]
                or current[5] != progress[4]
                or current[6] != progress[7]
                or current[7] != "published"
                or current[8] != "published"
                or current[9] != int(progress[1])
            ):
                raise ValueError("structure-drift-current-identity-invalid")
            if rows:
                changed = writer.execute(
                    "UPDATE structure_generation_drift_progress SET row_cursor_json=?,"
                    "digest_state_json=?,class_counts_json=?,class_digests_json=?,"
                    "checkpoint_at_ms=? "
                    "WHERE comparison_id=? AND phase='fresh-group-truth' AND "
                    "checkpoint_at_ms=?",
                    (
                        json.dumps(next_cursor),
                        digest.to_json(),
                        json.dumps(counts, sort_keys=True, separators=(",", ":")),
                        json.dumps(digests, sort_keys=True, separators=(",", ":")),
                        now_ms,
                        comparison_id,
                        prior_checkpoint,
                    ),
                )
                next_phase = "fresh-group-truth"
                ready = False
            else:
                generation_hash = digest.hexdigest()
                digests["generation_group_truth_hash"] = generation_hash
                counts["generation_group_truth_count"] = phase_count
                comparison_digest = RowChainSHA256.from_json(
                    str(
                        digests.pop(
                            "generation_source_group_truth_comparison_state"
                        )
                    ),
                    expected_domain="source-group-truth",
                )
                group_comparison_root = comparison_digest.hexdigest()
                digests["generation_source_group_truth_comparison_root"] = (
                    group_comparison_root
                )
                source_hash = digests.get("source_group_truth_hash")
                conflict_count = counts.get("class_count:overlap-conflict", 0)
                unclassified_count = counts.get("class_count:unclassified", 0)
                projection_count = counts.get("projection_member_count")
                generation_count = counts.get("generation_member_count")
                projection_root = digests.get("projection_member_root")
                generation_root = digests.get("generation_member_root")
                member_comparison_root = digests.get(
                    "generation_projection_member_comparison_root"
                )
                member_comparison_count = counts.get(
                    "generation_projection_member_comparison_count"
                )
                group_comparison_count = counts.get(
                    "generation_source_group_truth_comparison_count"
                )
                try:
                    diagnostic_counts = json.loads(str(progress[14]))
                except (TypeError, json.JSONDecodeError) as error:
                    raise ValueError("structure-drift-progress-invalid") from error
                if not isinstance(diagnostic_counts, dict) or any(
                    type(value) is not int or value < 0
                    for value in diagnostic_counts.values()
                ):
                    raise ValueError("structure-drift-progress-invalid")
                diagnostic_total = sum(diagnostic_counts.values())
                diagnostic_digest = RowChainSHA256.from_json(
                    str(progress[15]), expected_domain="diagnostic/unclassified"
                )
                if diagnostic_digest.count != diagnostic_total:
                    raise ValueError("structure-drift-progress-invalid")
                final_diagnostic_root = diagnostic_digest.hexdigest()
                projection_candidate_count = progress[21]
                projection_exclusion_count = progress[22]
                try:
                    projection_exclusion_counts = json.loads(str(progress[23]))
                    projection_exclusion_roots = json.loads(str(progress[24]))
                    projection_exclusion_states = json.loads(str(progress[25]))
                except (TypeError, json.JSONDecodeError) as error:
                    raise ValueError(
                        "structure-drift-exclusion-commitment-invalid"
                    ) from error
                projection_member_count = counts.get("projection_member_count")
                projection_diagnostic_count = counts.get(
                    "projection_diagnostic_count"
                )
                if (
                    type(projection_candidate_count) is not int
                    or type(projection_member_count) is not int
                    or type(projection_exclusion_count) is not int
                    or type(projection_diagnostic_count) is not int
                    or projection_candidate_count
                    != projection_member_count
                    + projection_exclusion_count
                    + projection_diagnostic_count
                ):
                    raise ValueError(
                        "structure-drift-candidate-conservation-invalid"
                    )
                if projection_candidate_count != _fresh_projection_expected_candidate_count(
                    writer, window_id=str(progress[3])
                ):
                    raise ValueError("structure-drift-candidate-source-count-invalid")
                exclusion_commitment_valid = (
                    isinstance(projection_exclusion_counts, dict)
                    and isinstance(projection_exclusion_roots, dict)
                    and isinstance(projection_exclusion_states, dict)
                    and set(projection_exclusion_counts)
                    == set(STRUCTURE_PROJECTION_EXCLUSION_REASONS)
                    and set(projection_exclusion_roots)
                    == set(STRUCTURE_PROJECTION_EXCLUSION_REASONS)
                    and set(projection_exclusion_states)
                    == set(STRUCTURE_PROJECTION_EXCLUSION_REASONS)
                    and all(
                        type(projection_exclusion_counts[reason]) is int
                        and projection_exclusion_counts[reason] >= 0
                        for reason in STRUCTURE_PROJECTION_EXCLUSION_REASONS
                    )
                    and sum(projection_exclusion_counts.values())
                    == projection_exclusion_count
                )
                if exclusion_commitment_valid:
                    try:
                        for reason in STRUCTURE_PROJECTION_EXCLUSION_REASONS:
                            exclusion_digest = RowChainSHA256.from_json(
                                str(projection_exclusion_states[reason]),
                                expected_domain=f"projection-exclusion/{reason}",
                            )
                            if (
                                exclusion_digest.count
                                != projection_exclusion_counts[reason]
                                or exclusion_digest.hexdigest()
                                != projection_exclusion_roots[reason]
                            ):
                                exclusion_commitment_valid = False
                                break
                    except (TypeError, ValueError):
                        exclusion_commitment_valid = False
                if not exclusion_commitment_valid:
                    raise ValueError("structure-drift-exclusion-commitment-invalid")
                if (
                    not isinstance(source_hash, str)
                    or len(source_hash) != 64
                    or type(conflict_count) is not int
                    or conflict_count < 0
                    or type(unclassified_count) is not int
                    or unclassified_count < 0
                    or type(projection_count) is not int
                    or projection_count < 0
                    or type(generation_count) is not int
                    or generation_count < 0
                    or not isinstance(projection_root, str)
                    or len(projection_root) != 64
                    or not isinstance(generation_root, str)
                    or len(generation_root) != 64
                    or not isinstance(member_comparison_root, str)
                    or len(member_comparison_root) != 64
                    or type(member_comparison_count) is not int
                    or member_comparison_count < 0
                    or type(group_comparison_count) is not int
                    or group_comparison_count < 0
                ):
                    raise ValueError("structure-drift-progress-invalid")
                authorized = (
                    group_comparison_root == source_hash
                    and group_comparison_count == phase_count
                    and projection_count == generation_count
                    and member_comparison_count == generation_count
                    and projection_root == member_comparison_root
                    and conflict_count == 0
                    and unclassified_count == 0
                    and diagnostic_total == 0
                )
                counts["fresh_group_truth_complete"] = 1
                counts["phase_row_count"] = 0
                next_phase = "sealed" if authorized else "stale"
                terminal_reason = None
                if authorized:
                    class_tags = (
                        "shared",
                        "fresh-addition",
                        "current-nontradable",
                        "event-only-quarantine",
                        "market-side-quarantine",
                        "fresh-source-absent",
                        "fresh-group-ineligible",
                        "overlap-conflict",
                        "unclassified",
                    )
                    final_class_counts: dict[str, int] = {}
                    final_class_digests: dict[str, str] = {}
                    for tag in class_tags:
                        class_count = counts.get(f"class_count:{tag}", 0)
                        if type(class_count) is not int or class_count < 0:
                            raise ValueError("structure-drift-progress-invalid")
                        final_class_counts[tag] = class_count
                        state_value = digests.pop(f"class_state:{tag}", None)
                        if class_count == 0:
                            if state_value is not None:
                                raise ValueError("structure-drift-progress-invalid")
                            continue
                        if not isinstance(state_value, str):
                            raise ValueError("structure-drift-progress-invalid")
                        class_digest = RowChainSHA256.from_json(
                            state_value,
                            expected_domain=f"class/{tag}",
                        )
                        final_class_digests[tag] = class_digest.hexdigest()
                    removal_tags = (
                        "current-nontradable",
                        "event-only-quarantine",
                        "market-side-quarantine",
                        "fresh-source-absent",
                        "fresh-group-ineligible",
                    )
                    legacy_root = reconstruction_root_from_class_commitments(
                        class_counts=final_class_counts,
                        class_digests=final_class_digests,
                        tags=("shared", *removal_tags),
                        domain="legacy-reconstruction",
                    )
                    generation_reconstruction_root = (
                        reconstruction_root_from_class_commitments(
                            class_counts=final_class_counts,
                            class_digests=final_class_digests,
                            tags=("shared", "fresh-addition"),
                            domain="generation-reconstruction",
                        )
                    )
                    exact = writer.execute(
                        "SELECT r.legacy_market_count,r.generation_market_count,"
                        "r.legacy_universe_hash,r.legacy_source_truth_hash,"
                        "legacy.taken_at_ms,legacy.finished_at_ms FROM "
                        "structure_generation_comparison_receipts r JOIN snapshots "
                        "legacy ON legacy.id=r.legacy_snapshot_id WHERE "
                        "r.generation_snapshot_id=? AND r.publication_id=? AND "
                        "r.legacy_snapshot_id=? AND r.receipt_digest=?",
                        (
                            int(progress[1]),
                            str(progress[2]),
                            int(progress[0]),
                            str(progress[5]),
                        ),
                    ).fetchone()
                    legacy_member_count = final_class_counts["shared"] + sum(
                        final_class_counts[tag] for tag in removal_tags
                    )
                    reconstructed_generation_count = (
                        final_class_counts["shared"]
                        + final_class_counts["fresh-addition"]
                    )
                    legacy_scan_count = counts.get("legacy_member_scan_count")
                    generation_scan_count = counts.get("generation_member_scan_count")
                    if (
                        exact is None
                        or type(legacy_scan_count) is not int
                        or legacy_scan_count != legacy_member_count
                        or type(generation_scan_count) is not int
                        or generation_scan_count != reconstructed_generation_count
                        or reconstructed_generation_count != generation_count
                    ):
                        raise ValueError(
                            "structure-drift-reconstruction-count-mismatch"
                        )
                    class_counts_json = json.dumps(
                        final_class_counts,
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                    class_digests_json = json.dumps(
                        final_class_digests,
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                    receipt_payload: dict[str, object] = {
                        "comparison_id": comparison_id,
                        "hash_algorithm": ROW_CHAIN_SHA256_V2,
                        "classifier_contract_version": STRUCTURE_DRIFT_CLASSIFIER_V4,
                        "legacy_snapshot_id": int(progress[0]),
                        "legacy_taken_at_ms": int(exact[4]),
                        "legacy_finished_at_ms": int(exact[5]),
                        "legacy_market_count": int(exact[0]),
                        "legacy_universe_hash": str(exact[2]),
                        "legacy_source_truth_hash": str(exact[3]),
                        "generation_snapshot_id": int(progress[1]),
                        "publication_id": str(progress[2]),
                        "window_id": str(progress[3]),
                        "published_snapshot_id": int(progress[1]),
                        "normalization_contract_version": str(progress[4]),
                        "exact_receipt_digest": str(progress[5]),
                        "pointer_validation_hash": str(progress[6]),
                        "generation_certification_hash": str(progress[7]),
                        "source_event_count": int(counts.get("source_event_count", 0)),
                        "source_market_count": int(
                            counts.get("source_market_count", 0)
                        ),
                        "source_event_hash": "",
                        "source_market_hash": "",
                        "source_identity_hash": "",
                        "projection_member_receipt_digest": "",
                        "projection_universe_hash": str(projection_root),
                        "projection_group_truth_hash": str(source_hash),
                        "generation_universe_hash": str(generation_root),
                        "generation_group_truth_hash": generation_hash,
                        "generation_projection_member_comparison_count": (
                            member_comparison_count
                        ),
                        "generation_projection_member_comparison_root": str(
                            member_comparison_root
                        ),
                        "generation_source_group_truth_comparison_count": (
                            group_comparison_count
                        ),
                        "generation_source_group_truth_comparison_root": str(
                            group_comparison_root
                        ),
                        "class_counts_json": class_counts_json,
                        "class_digests_json": class_digests_json,
                        "diagnostic_counts_json": str(progress[14]),
                        "diagnostic_root": RowChainSHA256.from_json(
                            str(progress[15]),
                            expected_domain="diagnostic/unclassified",
                        ).hexdigest(),
                        "diagnostic_samples_json": str(progress[16]),
                        "diagnostic_samples_digest": str(progress[17]),
                        "legacy_reconstruction_root": legacy_root,
                        "generation_reconstruction_root": (
                            generation_reconstruction_root
                        ),
                        "overlap_conflict_count": 0,
                        "unclassified_count": 0,
                        "projection_candidate_count": projection_candidate_count,
                        "projection_exclusion_count": projection_exclusion_count,
                        "projection_exclusion_counts_json": str(progress[23]),
                        "projection_exclusion_roots_json": str(progress[24]),
                        "created_at_ms": int(progress[19]),
                    }
                    stored_source = writer.execute(
                        "SELECT source_event_count,source_market_count,source_event_hash,"
                        "source_market_hash,source_identity_hash,"
                        "projection_member_receipt_digest FROM "
                        "structure_generation_drift_progress WHERE comparison_id=?",
                        (comparison_id,),
                    ).fetchone()
                    if stored_source is None or any(
                        not isinstance(value, str) or len(value) != 64
                        for value in stored_source[2:]
                    ):
                        raise ValueError("structure-drift-progress-invalid")
                    receipt_payload["source_event_count"] = int(stored_source[0])
                    receipt_payload["source_market_count"] = int(stored_source[1])
                    receipt_payload["source_event_hash"] = str(stored_source[2])
                    receipt_payload["source_market_hash"] = str(stored_source[3])
                    receipt_payload["source_identity_hash"] = str(stored_source[4])
                    receipt_payload["projection_member_receipt_digest"] = str(
                        stored_source[5]
                    )
                    receipt_digest = _structure_drift_receipt_digest(receipt_payload)
                    receipt_columns = _structure_drift_receipt_fields(
                        STRUCTURE_DRIFT_CLASSIFIER_V4
                    )
                    writer.execute(
                        "INSERT INTO structure_generation_drift_receipts("
                        + ",".join(receipt_columns)
                        + ",receipt_digest) VALUES ("
                        + ",".join("?" for _ in range(len(receipt_columns) + 1))
                        + ")",
                        (
                            *(receipt_payload[column] for column in receipt_columns),
                            receipt_digest,
                        ),
                    )
                    digests["sealed_class_digests"] = final_class_digests
                    digests["legacy_reconstruction_root"] = legacy_root
                    digests["generation_reconstruction_root"] = (
                        generation_reconstruction_root
                    )
                    digests["receipt_digest"] = receipt_digest
                else:
                    class_tags = (
                        "shared",
                        "fresh-addition",
                        "current-nontradable",
                        "event-only-quarantine",
                        "market-side-quarantine",
                        "fresh-source-absent",
                        "fresh-group-ineligible",
                        "overlap-conflict",
                        "unclassified",
                    )
                    final_class_counts = {}
                    final_class_digests = {}
                    for tag in class_tags:
                        class_count = counts.get(f"class_count:{tag}", 0)
                        if type(class_count) is not int or class_count < 0:
                            raise ValueError("structure-drift-progress-invalid")
                        final_class_counts[tag] = class_count
                        state_value = digests.get(f"class_state:{tag}")
                        if class_count == 0:
                            if state_value is not None:
                                raise ValueError("structure-drift-progress-invalid")
                            continue
                        if not isinstance(state_value, str):
                            raise ValueError("structure-drift-progress-invalid")
                        final_class_digests[tag] = RowChainSHA256.from_json(
                            state_value, expected_domain=f"class/{tag}"
                        ).hexdigest()
                    terminal_reason = (
                        "drift-overlap-conflict"
                        if conflict_count > 0
                        else "drift-unclassified"
                    )
                    class_counts_json = json.dumps(
                        final_class_counts, sort_keys=True, separators=(",", ":")
                    )
                    class_digests_json = json.dumps(
                        final_class_digests, sort_keys=True, separators=(",", ":")
                    )
                    terminal_payload: dict[str, object] = {
                        "comparison_id": comparison_id,
                        "hash_algorithm": ROW_CHAIN_SHA256_V2,
                        "classifier_contract_version": STRUCTURE_DRIFT_CLASSIFIER_V4,
                        "legacy_snapshot_id": int(progress[0]),
                        "generation_snapshot_id": int(progress[1]),
                        "publication_id": str(progress[2]),
                        "window_id": str(progress[3]),
                        "normalization_contract_version": str(progress[4]),
                        "exact_receipt_digest": str(progress[5]),
                        "pointer_validation_hash": str(progress[6]),
                        "generation_certification_hash": str(progress[7]),
                        "source_identity_hash": str(progress[18]),
                        "projection_member_receipt_digest": str(
                            digests.get("projection_member_receipt_digest", "")
                        ),
                        "terminal_reason": terminal_reason,
                        "class_counts_json": class_counts_json,
                        "class_digests_json": class_digests_json,
                        "diagnostic_counts_json": str(progress[14]),
                        "diagnostic_root": final_diagnostic_root,
                        "diagnostic_samples_json": str(progress[16]),
                        "diagnostic_samples_digest": str(progress[17]),
                        "projection_candidate_count": projection_candidate_count,
                        "projection_exclusion_count": projection_exclusion_count,
                        "projection_exclusion_counts_json": str(progress[23]),
                        "projection_exclusion_roots_json": str(progress[24]),
                        "created_at_ms": int(progress[19]),
                        "checkpoint_at_ms": now_ms,
                    }
                    terminal_digest = _structure_drift_terminal_receipt_digest(
                        terminal_payload
                    )
                    terminal_columns = _structure_drift_terminal_receipt_fields(
                        STRUCTURE_DRIFT_CLASSIFIER_V4
                    )
                    writer.execute(
                        "INSERT INTO structure_generation_drift_terminal_receipts("
                        + ",".join(terminal_columns)
                        + ",receipt_digest) VALUES ("
                        + ",".join("?" for _ in range(len(terminal_columns) + 1))
                        + ")",
                        (
                            *(terminal_payload[column] for column in terminal_columns),
                            terminal_digest,
                        ),
                    )
                    counts = final_class_counts
                    digests = final_class_digests
                changed = writer.execute(
                    "UPDATE structure_generation_drift_progress SET phase=?,"
                    "terminal_reason=?,"
                    "row_cursor_json=NULL,digest_state_json=?,class_counts_json=?,"
                    "class_digests_json=?,diagnostic_root=?,checkpoint_at_ms=? "
                    "WHERE comparison_id=? "
                    "AND phase='fresh-group-truth' AND checkpoint_at_ms=?",
                    (
                        next_phase,
                        terminal_reason,
                        digest.to_json(),
                        json.dumps(counts, sort_keys=True, separators=(",", ":")),
                        json.dumps(digests, sort_keys=True, separators=(",", ":")),
                        final_diagnostic_root,
                        now_ms,
                        comparison_id,
                        prior_checkpoint,
                    ),
                )
                ready = authorized
            if changed.rowcount != 1:
                raise StructurePublicationCursorError("structure-drift-cursor-mismatch")
            writer.execute("COMMIT")
        except BaseException:
            if writer.in_transaction:
                writer.execute("ROLLBACK")
            raise
        finally:
            writer.close()
        return StructureCertificationChunk(
            next_phase,
            None if not rows else json.dumps(next_cursor),
            len(rows),
            ready,
        )

    def _advance_structure_drift_member_chunk(
        self,
        comparison_id: str,
        *,
        max_rows: int,
        now_ms: int,
    ) -> StructureCertificationChunk:
        """Classify one bounded member keyset and CAS its digest checkpoint."""
        from polyarb.perception.structure_drift import (
            classify_structure_member_drift,
            structure_drift_diagnostic_sample,
            structure_drift_diagnostic_tuple,
        )

        with sqlite3.connect(self._db_path) as read_con:
            progress = read_con.execute(
                "SELECT legacy_snapshot_id,generation_snapshot_id,publication_id,"
                "window_id,normalization_contract_version,exact_receipt_digest,"
                "pointer_validation_hash,generation_certification_hash,phase,"
                "row_cursor_json,class_counts_json,class_digests_json,checkpoint_at_ms,"
                "diagnostic_counts_json,diagnostic_digest_state_json,"
                "diagnostic_samples_json "
                "FROM structure_generation_drift_progress WHERE comparison_id=?",
                (comparison_id,),
            ).fetchone()
        if progress is None or progress[8] not in {
            "generation-members",
            "legacy-members",
        }:
            raise ValueError("structure-drift-member-phase-invalid")
        phase = str(progress[8])
        cursor = None if progress[9] is None else json.loads(str(progress[9]))
        counts = json.loads(str(progress[10]))
        digests = json.loads(str(progress[11]))
        diagnostic_counts = json.loads(str(progress[13]))
        diagnostic_samples = json.loads(str(progress[15]))
        diagnostic_digest = RowChainSHA256.from_json(
            str(progress[14]), expected_domain="diagnostic/unclassified"
        )
        if not isinstance(diagnostic_counts, dict) or not isinstance(
            diagnostic_samples, dict
        ):
            raise ValueError("structure-drift-progress-invalid")
        phase_count = counts.get("phase_row_count")
        if type(phase_count) is not int or phase_count < 0:
            raise ValueError("structure-drift-progress-invalid")

        generation_phase = phase == "generation-members"
        snapshot_id = int(progress[1] if generation_phase else progress[0])
        rows = self.fetch_structure_drift_member_chunk(
            snapshot_id=snapshot_id,
            generation=generation_phase,
            after_market_id=cursor,
            limit=max_rows,
        )
        market_ids = [str(getattr(member, "market_id")) for member in rows]
        counterpart = self.fetch_structure_drift_members_by_id(
            snapshot_id=int(progress[0] if generation_phase else progress[1]),
            generation=not generation_phase,
            market_ids=market_ids,
        )
        if generation_phase:
            classified_rows = tuple(rows)
            evidence = self.fetch_structure_drift_fresh_evidence(
                publication_id=str(progress[2]),
                generation_snapshot_id=int(progress[1]),
                members=classified_rows,
            )
            result = classify_structure_member_drift(
                legacy=tuple(counterpart),
                generation=classified_rows,
                evidence=evidence,
                classifier_contract=STRUCTURE_DRIFT_CLASSIFIER_V4,
            )
            generation_count = counts.get("generation_member_count", 0)
            if type(generation_count) is not int or generation_count < 0:
                raise ValueError("structure-drift-progress-invalid")
            generation_state_value = digests.get("generation_member_state")
            comparison_state_value = digests.get(
                "generation_projection_member_comparison_state"
            )
            if generation_state_value is None:
                generation_digest = RowChainSHA256.new("generation-member")
            elif isinstance(generation_state_value, str):
                generation_digest = RowChainSHA256.from_json(
                    generation_state_value,
                    expected_domain="generation-member",
                )
            else:
                raise ValueError("structure-drift-progress-invalid")
            if comparison_state_value is None:
                comparison_digest = RowChainSHA256.new("projection-member")
            elif isinstance(comparison_state_value, str):
                comparison_digest = RowChainSHA256.from_json(
                    comparison_state_value,
                    expected_domain="projection-member",
                )
            else:
                raise ValueError("structure-drift-progress-invalid")
            for member in classified_rows:
                actual_tuple = (
                    member.event_id,
                    member.group_id,
                    member.market_id,
                    member.member_kind,
                    member.active,
                    member.closed,
                    member.condition_id,
                    member.yes_token_id,
                    member.no_token_id,
                    member.neg_risk,
                    member.incomplete,
                )
                generation_digest.update(actual_tuple)
                comparison_digest.update(actual_tuple)
                generation_count += 1
            counts["generation_member_count"] = generation_count
            digests["generation_member_state"] = generation_digest.to_json()
            digests["generation_projection_member_comparison_state"] = (
                comparison_digest.to_json()
            )
        else:
            generation_ids = {
                str(getattr(member, "market_id")) for member in counterpart
            }
            classified_rows = tuple(
                member
                for member in rows
                if str(getattr(member, "market_id")) not in generation_ids
            )
            evidence = self.fetch_structure_drift_fresh_evidence(
                publication_id=str(progress[2]),
                generation_snapshot_id=int(progress[1]),
                members=classified_rows,
            )
            result = classify_structure_member_drift(
                legacy=classified_rows,
                generation=(),
                evidence=evidence,
                classifier_contract=STRUCTURE_DRIFT_CLASSIFIER_V4,
            )

        for diagnostic in result.diagnostics:
            diagnostic_digest.update(structure_drift_diagnostic_tuple(diagnostic))
            diagnostic_counts[diagnostic.code] = (
                int(diagnostic_counts.get(diagnostic.code, 0)) + 1
            )
            samples = diagnostic_samples.get(diagnostic.code, [])
            if not isinstance(samples, list):
                raise ValueError("structure-drift-progress-invalid")
            samples.append(structure_drift_diagnostic_sample(diagnostic))
            samples.sort(
                key=lambda item: json.dumps(
                    item, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                )
            )
            diagnostic_samples[diagnostic.code] = samples[:3]
        diagnostic_counts_json = json.dumps(
            diagnostic_counts, sort_keys=True, separators=(",", ":")
        )
        diagnostic_samples_json = json.dumps(
            diagnostic_samples,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )

        classes = {
            "shared": result.shared,
            "fresh-addition": result.fresh_additions,
            **result.legacy_removals,
            "overlap-conflict": result.overlap_conflicts,
            "unclassified": result.unclassified,
        }
        for tag, members in classes.items():
            if not members:
                continue
            count_key = f"class_count:{tag}"
            state_key = f"class_state:{tag}"
            class_count = counts.get(count_key, 0)
            if type(class_count) is not int or class_count < 0:
                raise ValueError("structure-drift-progress-invalid")
            state_value = digests.get(state_key)
            if state_value is None:
                class_digest = RowChainSHA256.new(f"class/{tag}")
            elif isinstance(state_value, str):
                class_digest = RowChainSHA256.from_json(
                    state_value,
                    expected_domain=f"class/{tag}",
                )
            else:
                raise ValueError("structure-drift-progress-invalid")
            for member in members:
                class_digest.update(
                    (
                        tag,
                        member.event_id,
                        member.group_id,
                        member.market_id,
                        member.member_kind,
                        member.active,
                        member.closed,
                        member.condition_id,
                        member.yes_token_id,
                        member.no_token_id,
                        member.neg_risk,
                        member.incomplete,
                    )
                )
                class_count += 1
            counts[count_key] = class_count
            digests[state_key] = class_digest.to_json()
        phase_count += len(rows)
        counts["phase_row_count"] = phase_count
        next_cursor = None if not rows else market_ids[-1]
        prior_checkpoint = int(progress[12])

        writer = self._connect_writer()
        try:
            writer.execute("BEGIN IMMEDIATE")
            current = writer.execute(
                "SELECT current.snapshot_id,current.publication_id,"
                "current.validation_hash,current.comparison_receipt_digest,"
                "publication.window_id,publication.normalization_contract_version,"
                "publication.certification_hash,publication.status,window.status,"
                "window.published_snapshot_id FROM current_structure_generation current "
                "JOIN structure_publications publication ON "
                "publication.publication_id=current.publication_id AND "
                "publication.snapshot_id=current.snapshot_id JOIN "
                "structure_sync_windows window ON window.id=publication.window_id "
                "WHERE current.id=1"
            ).fetchone()
            legacy = self._comparison_legacy_identity(writer)
            if (
                current is None
                or legacy is None
                or legacy[0] != int(progress[0])
                or current[0] != int(progress[1])
                or current[1] != progress[2]
                or current[2] != progress[6]
                or current[3] != progress[5]
                or current[4] != progress[3]
                or current[5] != progress[4]
                or current[6] != progress[7]
                or current[7] != "published"
                or current[8] != "published"
                or current[9] != int(progress[1])
            ):
                raise ValueError("structure-drift-current-identity-invalid")
            if rows:
                changed = writer.execute(
                    "UPDATE structure_generation_drift_progress SET row_cursor_json=?,"
                    "class_counts_json=?,class_digests_json=?,"
                    "diagnostic_counts_json=?,diagnostic_digest_state_json=?,"
                    "diagnostic_samples_json=?,diagnostic_samples_digest=?,"
                    "checkpoint_at_ms=? "
                    "WHERE comparison_id=? AND phase=? AND checkpoint_at_ms=?",
                    (
                        json.dumps(next_cursor),
                        json.dumps(counts, sort_keys=True, separators=(",", ":")),
                        json.dumps(digests, sort_keys=True, separators=(",", ":")),
                        diagnostic_counts_json,
                        diagnostic_digest.to_json(),
                        diagnostic_samples_json,
                        hashlib.sha256(diagnostic_samples_json.encode()).hexdigest(),
                        now_ms,
                        comparison_id,
                        phase,
                        prior_checkpoint,
                    ),
                )
                next_phase = phase
            else:
                next_phase = (
                    "legacy-members" if generation_phase else "fresh-group-truth"
                )
                if generation_phase:
                    generation_digest = RowChainSHA256.from_json(
                        str(digests.pop("generation_member_state")),
                        expected_domain="generation-member",
                    )
                    comparison_digest = RowChainSHA256.from_json(
                        str(
                            digests.pop(
                                "generation_projection_member_comparison_state"
                            )
                        ),
                        expected_domain="projection-member",
                    )
                    digests["generation_member_root"] = generation_digest.hexdigest()
                    digests["generation_projection_member_comparison_root"] = (
                        comparison_digest.hexdigest()
                    )
                    counts["generation_projection_member_comparison_count"] = (
                        comparison_digest.count
                    )
                    counts["generation_member_scan_count"] = phase_count
                else:
                    counts["legacy_member_scan_count"] = phase_count
                counts["phase_row_count"] = 0
                changed = writer.execute(
                    "UPDATE structure_generation_drift_progress SET phase=?,"
                    "row_cursor_json=NULL,class_counts_json=?,class_digests_json=?,"
                    "diagnostic_counts_json=?,diagnostic_digest_state_json=?,"
                    "diagnostic_samples_json=?,diagnostic_samples_digest=?,"
                    "checkpoint_at_ms=? WHERE comparison_id=? AND phase=? AND "
                    "checkpoint_at_ms=?",
                    (
                        next_phase,
                        json.dumps(counts, sort_keys=True, separators=(",", ":")),
                        json.dumps(digests, sort_keys=True, separators=(",", ":")),
                        diagnostic_counts_json,
                        diagnostic_digest.to_json(),
                        diagnostic_samples_json,
                        hashlib.sha256(diagnostic_samples_json.encode()).hexdigest(),
                        now_ms,
                        comparison_id,
                        phase,
                        prior_checkpoint,
                    ),
                )
            if changed.rowcount != 1:
                raise StructurePublicationCursorError("structure-drift-cursor-mismatch")
            writer.execute("COMMIT")
        except BaseException:
            if writer.in_transaction:
                writer.execute("ROLLBACK")
            raise
        finally:
            writer.close()
        return StructureCertificationChunk(
            next_phase,
            None if not rows else json.dumps(next_cursor),
            len(rows),
            False,
        )

    def fetch_structure_drift_fresh_evidence(
        self,
        *,
        publication_id: str,
        generation_snapshot_id: int,
        members: tuple[object, ...],
        trace_callback: Callable[[str], None] | None = None,
    ) -> dict[str, object]:
        """Bulk-recompute fresh evidence without trusting committed issue rows."""
        from polyarb.perception.structure_drift import build_fresh_member_evidence

        market_ids = [str(getattr(member, "market_id", "")) for member in members]
        if (
            not publication_id
            or generation_snapshot_id < 1
            or len(market_ids) > STRUCTURE_PUBLICATION_MAX_ROWS
            or any(not market_id for market_id in market_ids)
            or len(set(market_ids)) != len(market_ids)
        ):
            raise ValueError("invalid-structure-drift-evidence-chunk")
        if not members:
            return {}
        placeholders = ",".join("?" for _ in market_ids)
        with sqlite3.connect(self._db_path) as con:
            if trace_callback is not None:
                con.set_trace_callback(trace_callback)
            con.execute("BEGIN")
            identity = con.execute(
                "SELECT p.window_id,p.status,p.normalization_contract_version,"
                "p.validation_hash,p.certification_hash,window.status,"
                "window.published_snapshot_id FROM structure_publications p JOIN "
                "structure_sync_windows window ON window.id=p.window_id "
                "WHERE p.publication_id=? AND p.snapshot_id=?",
                (publication_id, generation_snapshot_id),
            ).fetchone()
            if (
                identity is None
                or identity[1] != "published"
                or not isinstance(identity[2], str)
                or not identity[2]
                or not isinstance(identity[3], str)
                or len(identity[3]) != 64
                or not isinstance(identity[4], str)
                or len(identity[4]) != 64
                or identity[5] != "published"
                or identity[6] != generation_snapshot_id
            ):
                raise ValueError("structure-drift-source-identity-mismatch")
            window_id = str(identity[0])
            raw_markets = {
                str(market_id): json.loads(str(payload_json))
                for market_id, payload_json in con.execute(
                    "SELECT market_id,payload_json FROM structure_sync_market_staging "
                    f"WHERE window_id=? AND market_id IN ({placeholders})",
                    (window_id, *market_ids),
                )
            }
            event_sources: dict[str, list[tuple[str, int, dict[str, object]]]] = {}
            for market_id, event_id, source_ordinal, payload_json in con.execute(
                "SELECT relation.market_id,relation.event_id,"
                "COALESCE(event.source_ordinal,event.rowid),event.payload_json FROM "
                "structure_sync_event_market_staging relation JOIN "
                "structure_sync_event_staging event ON "
                "event.window_id=relation.window_id AND "
                "event.event_id=relation.event_id WHERE relation.window_id=? "
                f"AND relation.market_id IN ({placeholders}) ORDER BY "
                "relation.market_id,relation.source_ordinal,relation.event_id",
                (window_id, *market_ids),
            ):
                event_sources.setdefault(str(market_id), []).append(
                    (
                        str(event_id),
                        int(source_ordinal),
                        json.loads(str(payload_json)),
                    )
                )
            event_normalization_cache = {}
            result = {
                market_id: build_fresh_member_evidence(
                    member,
                    raw_market=raw_markets.get(market_id),
                    event_sources=tuple(event_sources.get(market_id, ())),
                    generation_certified=True,
                    event_normalization_cache=event_normalization_cache,
                )
                for member, market_id in zip(members, market_ids, strict=True)
            }
            con.execute("COMMIT")
            return result

    def structure_generation_drift_status(self) -> dict[str, object]:
        """Authenticate current drift evidence without advancing or mutating it."""
        if not self._db_path.exists():
            return {
                "authorization_mode": "unavailable",
                "authorized": False,
                "available": False,
                "reason": "structure-drift-database-unavailable",
            }
        with sqlite3.connect(self._db_path) as con:
            current = con.execute(
                "SELECT current.snapshot_id,current.publication_id,"
                "current.validation_hash,current.comparison_receipt_digest,"
                "publication.window_id,publication.normalization_contract_version,"
                "publication.certification_hash,publication.status,window.status,"
                "window.published_snapshot_id FROM current_structure_generation current "
                "JOIN structure_publications publication ON "
                "publication.publication_id=current.publication_id AND "
                "publication.snapshot_id=current.snapshot_id JOIN "
                "structure_sync_windows window ON window.id=publication.window_id "
                "WHERE current.id=1"
            ).fetchone()
            legacy = self._comparison_legacy_identity(con)
            if current is None or legacy is None:
                return {
                    "authorization_mode": "unavailable",
                    "authorized": False,
                    "available": False,
                    "reason": "structure-drift-current-unavailable",
                }
            identity_valid = (
                current[7] == "published"
                and current[8] == "published"
                and current[9] == current[0]
                and current[2] is not None
                and current[2] == current[6]
                and isinstance(current[5], str)
                and bool(current[5])
            )
            exact = con.execute(
                "SELECT legacy_snapshot_id,legacy_market_count,"
                "generation_market_count,legacy_universe_hash,"
                "generation_universe_hash,legacy_source_truth_hash,"
                "generation_source_truth_hash,generation_validation_hash,"
                "created_at_ms,receipt_digest FROM "
                "structure_generation_comparison_receipts WHERE "
                "generation_snapshot_id=? AND publication_id=?",
                (int(current[0]), str(current[1])),
            ).fetchone()
            exact_valid = False
            exact_matches = False
            if exact is not None:
                exact_digest = _comparison_receipt_digest(
                    generation_snapshot_id=int(current[0]),
                    publication_id=str(current[1]),
                    legacy_snapshot_id=int(exact[0]),
                    legacy_market_count=int(exact[1]),
                    generation_market_count=int(exact[2]),
                    legacy_universe_hash=str(exact[3]),
                    generation_universe_hash=str(exact[4]),
                    legacy_source_truth_hash=str(exact[5]),
                    generation_source_truth_hash=str(exact[6]),
                    generation_validation_hash=str(exact[7]),
                    created_at_ms=int(exact[8]),
                )
                exact_valid = (
                    identity_valid
                    and exact_digest == exact[9]
                    and exact[9] == current[3]
                    and int(exact[0]) == int(legacy[0])
                    and exact[7] == current[2]
                )
                exact_matches = exact_valid and (
                    int(exact[1]) == int(exact[2])
                    and exact[3] == exact[4]
                    and exact[5] == exact[6]
                )
            base: dict[str, object] = {
                "available": True,
                "generation_snapshot_id": int(current[0]),
                "legacy_snapshot_id": int(legacy[0]),
                "publication_id": str(current[1]),
                "window_id": str(current[4]),
                "normalization_contract_version": str(current[5]),
                "exact_receipt_digest": str(current[3]),
                "pointer_validation_hash": str(current[2]),
                "generation_certification_hash": str(current[6]),
            }
            if exact_matches:
                return {
                    **base,
                    "authorization_mode": "exact",
                    "authorized": True,
                    "phase": "exact",
                    "reason": None,
                }
            progress = con.execute(
                "SELECT comparison_id,phase,class_counts_json,class_digests_json,"
                "checkpoint_at_ms,hash_algorithm,source_event_count,"
                "source_market_count,source_event_hash,source_market_hash,"
                "source_identity_hash,classifier_contract_version,terminal_reason,"
                "diagnostic_counts_json,diagnostic_root,diagnostic_samples_json,"
                "diagnostic_samples_digest,projection_member_receipt_digest,"
                "projection_candidate_count,projection_exclusion_count,"
                "projection_exclusion_counts_json,projection_exclusion_roots_json,"
                "projection_exclusion_digest_states_json,"
                "diagnostic_digest_state_json "
                "FROM structure_generation_drift_progress WHERE "
                "legacy_snapshot_id=? AND generation_snapshot_id=? AND "
                "publication_id=? AND window_id=? AND "
                "normalization_contract_version=? AND exact_receipt_digest=? AND "
                "pointer_validation_hash=? AND generation_certification_hash=? AND "
                "hash_algorithm=? AND classifier_contract_version=? "
                "ORDER BY checkpoint_at_ms DESC LIMIT 1",
                (
                    int(legacy[0]),
                    int(current[0]),
                    str(current[1]),
                    str(current[4]),
                    str(current[5]),
                    str(current[3]),
                    str(current[2]),
                    str(current[6]),
                    ROW_CHAIN_SHA256_V2,
                    STRUCTURE_DRIFT_CLASSIFIER_V4,
                ),
            ).fetchone()
            if progress is None:
                return {
                    **base,
                    "authorization_mode": "none",
                    "authorized": False,
                    "phase": None,
                    "reason": (
                        "structure-drift-exact-receipt-invalid"
                        if not exact_valid
                        else "structure-drift-progress-missing"
                    ),
                }
            current_member_status = self.structure_event_member_status(
                window_id=str(current[4])
            )
            current_member_digest = current_member_status.get("receipt_digest")
            if (
                progress[1] in {"sealed", "stale"}
                and (
                    current_member_status.get("sealed") is not True
                    or not isinstance(current_member_digest, str)
                    or len(current_member_digest) != 64
                    or progress[17] != current_member_digest
                )
            ):
                return {
                    **base,
                    "authorization_mode": "none",
                    "authorized": False,
                    "progress_id": str(progress[0]),
                    "hash_algorithm": str(progress[5]),
                    "checkpoint_at_ms": int(progress[4]),
                    "phase": str(progress[1]),
                    "reason": "structure-drift-member-receipt-invalid",
                }
            try:
                progress_counts = json.loads(str(progress[2]))
                progress_digests = json.loads(str(progress[3]))
                progress_exclusion_counts = json.loads(str(progress[20]))
                progress_exclusion_roots = json.loads(str(progress[21]))
                progress_exclusion_states = json.loads(str(progress[22]))
                if not isinstance(progress_counts, dict) or not isinstance(
                    progress_digests, dict
                ) or not all(
                    isinstance(value, dict)
                    for value in (
                        progress_exclusion_counts,
                        progress_exclusion_roots,
                        progress_exclusion_states,
                    )
                ):
                    raise ValueError("structure-drift-progress-invalid")
            except (TypeError, ValueError, json.JSONDecodeError):
                return {
                    **base,
                    "authorization_mode": "none",
                    "authorized": False,
                    "progress_id": str(progress[0]),
                    "hash_algorithm": str(progress[5]),
                    "checkpoint_at_ms": int(progress[4]),
                    "phase": str(progress[1]),
                    "reason": "structure-drift-progress-invalid",
                }
            projection_candidate_count = progress[18]
            projection_exclusion_count = progress[19]
            projection_diagnostic_count = progress_counts.get(
                "projection_diagnostic_count", 0
            )
            projection_member_count = progress_counts.get(
                "projection_member_count",
                (
                    projection_candidate_count - projection_exclusion_count
                    if type(projection_candidate_count) is int
                    and type(projection_exclusion_count) is int
                    else None
                ),
            )
            terminal_source_count_valid = (
                progress[1] not in {"sealed", "stale"}
                or (
                    type(projection_candidate_count) is int
                    and projection_candidate_count
                    == _fresh_projection_expected_candidate_count(
                        con, window_id=str(current[4])
                    )
                )
            )
            progress_exclusion_evidence_valid = (
                terminal_source_count_valid
                and type(projection_candidate_count) is int
                and projection_candidate_count >= 0
                and type(projection_member_count) is int
                and projection_member_count >= 0
                and type(projection_exclusion_count) is int
                and projection_exclusion_count >= 0
                and type(projection_diagnostic_count) is int
                and projection_diagnostic_count >= 0
                and projection_candidate_count
                == projection_member_count
                + projection_exclusion_count
                + projection_diagnostic_count
                and set(progress_exclusion_counts)
                == set(STRUCTURE_PROJECTION_EXCLUSION_REASONS)
                and set(progress_exclusion_roots)
                == set(STRUCTURE_PROJECTION_EXCLUSION_REASONS)
                and set(progress_exclusion_states)
                == set(STRUCTURE_PROJECTION_EXCLUSION_REASONS)
                and all(
                    type(progress_exclusion_counts[reason]) is int
                    and progress_exclusion_counts[reason] >= 0
                    and isinstance(progress_exclusion_roots[reason], str)
                    and len(progress_exclusion_roots[reason]) == 64
                    and isinstance(progress_exclusion_states[reason], str)
                    for reason in STRUCTURE_PROJECTION_EXCLUSION_REASONS
                )
                and sum(progress_exclusion_counts.values())
                == projection_exclusion_count
            )
            if progress_exclusion_evidence_valid:
                try:
                    progress_exclusion_evidence_valid = all(
                        (
                            exclusion_digest := RowChainSHA256.from_json(
                                progress_exclusion_states[reason],
                                expected_domain=f"projection-exclusion/{reason}",
                            )
                        ).count
                        == progress_exclusion_counts[reason]
                        and exclusion_digest.hexdigest()
                        == progress_exclusion_roots[reason]
                        for reason in STRUCTURE_PROJECTION_EXCLUSION_REASONS
                    )
                except (TypeError, ValueError):
                    progress_exclusion_evidence_valid = False
            exposed_exclusion_counts = (
                {
                    reason: progress_exclusion_counts[reason]
                    for reason in sorted(STRUCTURE_PROJECTION_EXCLUSION_REASONS)
                    if progress_exclusion_counts[reason] > 0
                }
                if progress_exclusion_evidence_valid
                else {}
            )
            exposed_exclusion_roots = (
                {
                    reason: progress_exclusion_roots[reason]
                    for reason in exposed_exclusion_counts
                }
                if progress_exclusion_evidence_valid
                else {}
            )
            projection_status = {
                "classifier_contract_version": str(progress[11]),
                "projection_candidate_count": projection_candidate_count,
                "projection_member_count": projection_member_count,
                "projection_exclusion_count": projection_exclusion_count,
                "projection_diagnostic_count": projection_diagnostic_count,
                "projection_exclusion_counts": exposed_exclusion_counts,
                "projection_exclusion_roots": exposed_exclusion_roots,
            }
            receipt_row = con.execute(
                "SELECT "
                + ",".join(
                    _structure_drift_receipt_fields(STRUCTURE_DRIFT_CLASSIFIER_V4)
                )
                + ",receipt_digest FROM structure_generation_drift_receipts "
                "WHERE comparison_id=?",
                (str(progress[0]),),
            ).fetchone()
            class_tags = _STRUCTURE_DRIFT_CLASS_TAGS
            progress_class_items = {
                key: value
                for key, value in progress_counts.items()
                if key.startswith("class_count:")
            }
            progress_class_shape_valid = (
                set(progress_class_items)
                <= {f"class_count:{tag}" for tag in class_tags}
                and all(
                    type(value) is int and value >= 0
                    for value in progress_class_items.values()
                )
            )
            class_counts = {
                tag: progress_counts.get(f"class_count:{tag}", 0)
                for tag in class_tags
            }
            if progress[1] == "stale":
                terminal_row = con.execute(
                    "SELECT "
                    + ",".join(
                        _structure_drift_terminal_receipt_fields(
                            STRUCTURE_DRIFT_CLASSIFIER_V4
                        )
                    )
                    + ",receipt_digest FROM "
                    "structure_generation_drift_terminal_receipts WHERE "
                    "comparison_id=?",
                    (str(progress[0]),),
                ).fetchone()
                terminal_valid = False
                terminal_payload: dict[str, object] = {}
                diagnostic_counts: dict[str, object] = {}
                diagnostic_samples: dict[str, object] = {}
                if terminal_row is not None:
                    terminal_payload = dict(
                        zip(
                            _structure_drift_terminal_receipt_fields(
                                STRUCTURE_DRIFT_CLASSIFIER_V4
                            ),
                            terminal_row[:-1],
                            strict=True,
                        )
                    )
                    try:
                        diagnostic_counts = json.loads(
                            str(terminal_payload["diagnostic_counts_json"])
                        )
                        diagnostic_samples = json.loads(
                            str(terminal_payload["diagnostic_samples_json"])
                        )
                        diagnostic_evidence_valid = (
                            isinstance(diagnostic_counts, dict)
                            and all(
                                type(value) is int and value >= 0
                                for value in diagnostic_counts.values()
                            )
                            and isinstance(diagnostic_samples, dict)
                            and set(diagnostic_samples) <= set(diagnostic_counts)
                            and all(
                                isinstance(samples, list)
                                and len(samples) <= 3
                                and len(samples) <= diagnostic_counts[reason]
                                and all(isinstance(sample, dict) for sample in samples)
                                for reason, samples in diagnostic_samples.items()
                            )
                        )
                        diagnostic_digest = RowChainSHA256.from_json(
                            str(progress[23]),
                            expected_domain="diagnostic/unclassified",
                        )
                        diagnostic_evidence_valid = (
                            diagnostic_evidence_valid
                            and diagnostic_digest.count
                            == sum(diagnostic_counts.values())
                            and diagnostic_digest.hexdigest()
                            == terminal_payload["diagnostic_root"]
                        )
                        (
                            terminal_class_counts,
                            terminal_class_digests,
                        ) = _validated_structure_drift_class_shape(
                            terminal_payload["class_counts_json"],
                            terminal_payload["class_digests_json"],
                        )
                        expected_terminal_reason = (
                            "drift-overlap-conflict"
                            if terminal_class_counts["overlap-conflict"] > 0
                            else "drift-unclassified"
                        )
                        terminal_shape_valid = (
                            diagnostic_evidence_valid
                            and terminal_payload["terminal_reason"]
                            == expected_terminal_reason
                        )
                    except (TypeError, ValueError, json.JSONDecodeError):
                        terminal_shape_valid = False
                    try:
                        expected_terminal_digest = (
                            _structure_drift_terminal_receipt_digest(terminal_payload)
                        )
                    except ValueError:
                        expected_terminal_digest = None
                    terminal_valid = (
                        terminal_shape_valid
                        and progress_exclusion_evidence_valid
                        and expected_terminal_digest is not None
                        and terminal_row[-1] == expected_terminal_digest
                        and terminal_payload["comparison_id"] == progress[0]
                        and terminal_payload["hash_algorithm"] == progress[5]
                        and terminal_payload["classifier_contract_version"]
                        == progress[11]
                        == STRUCTURE_DRIFT_CLASSIFIER_V4
                        and terminal_payload["legacy_snapshot_id"] == legacy[0]
                        and terminal_payload["generation_snapshot_id"] == current[0]
                        and terminal_payload["publication_id"] == current[1]
                        and terminal_payload["window_id"] == current[4]
                        and terminal_payload["normalization_contract_version"]
                        == current[5]
                        and terminal_payload["exact_receipt_digest"] == current[3]
                        and terminal_payload["pointer_validation_hash"] == current[2]
                        and terminal_payload["generation_certification_hash"]
                        == current[6]
                        and terminal_payload["source_identity_hash"] == progress[10]
                        and terminal_payload["projection_member_receipt_digest"]
                        == progress[17]
                        == current_member_digest
                        and terminal_payload["terminal_reason"] == progress[12]
                        and terminal_payload["class_counts_json"] == progress[2]
                        and terminal_payload["class_digests_json"] == progress[3]
                        and terminal_payload["diagnostic_counts_json"] == progress[13]
                        and terminal_payload["diagnostic_root"] == progress[14]
                        and terminal_payload["diagnostic_samples_json"] == progress[15]
                        and terminal_payload["diagnostic_samples_digest"] == progress[16]
                        and terminal_payload["projection_candidate_count"]
                        == progress[18]
                        and terminal_payload["projection_exclusion_count"]
                        == progress[19]
                        and terminal_payload["projection_exclusion_counts_json"]
                        == progress[20]
                        and terminal_payload["projection_exclusion_roots_json"]
                        == progress[21]
                        and terminal_payload["checkpoint_at_ms"] == progress[4]
                        and terminal_payload["diagnostic_samples_digest"]
                        == hashlib.sha256(
                            str(terminal_payload["diagnostic_samples_json"]).encode()
                        ).hexdigest()
                    )
                if not terminal_valid:
                    return {
                        **base,
                        "authorization_mode": "none",
                        "authorized": False,
                        "progress_id": str(progress[0]),
                        "hash_algorithm": str(progress[5]),
                        "classifier_contract_version": str(progress[11]),
                        "checkpoint_at_ms": int(progress[4]),
                        "phase": "stale",
                        "reason": "structure-drift-terminal-receipt-invalid",
                    }
                return {
                    **base,
                    "authorization_mode": "none",
                    "authorized": False,
                    "progress_id": str(progress[0]),
                    "hash_algorithm": str(progress[5]),
                    "classifier_contract_version": str(progress[11]),
                    "diagnostic_total": diagnostic_digest.count,
                    "diagnostic_root": diagnostic_digest.hexdigest(),
                    "terminal_receipt_digest": str(terminal_row[-1]),
                    "checkpoint_at_ms": int(progress[4]),
                    "phase": "stale",
                    "reason": "structure-drift-terminal-stale",
                }
            if progress[1] != "sealed" or receipt_row is None:
                return {
                    **base,
                    "authorization_mode": "none",
                    "authorized": False,
                    "progress_id": str(progress[0]),
                    "hash_algorithm": str(progress[5]),
                    "class_counts": class_counts,
                    "checkpoint_at_ms": int(progress[4]),
                    "phase": str(progress[1]),
                    "reason": (
                        "structure-drift-stale"
                        if progress[1] == "stale"
                        else "structure-drift-incomplete"
                    ),
                }
            receipt_payload = dict(
                zip(
                    _structure_drift_receipt_fields(
                        STRUCTURE_DRIFT_CLASSIFIER_V4
                    ),
                    receipt_row[:-1],
                    strict=True,
                )
            )
            try:
                expected_receipt_digest = _structure_drift_receipt_digest(receipt_payload)
            except ValueError:
                expected_receipt_digest = None
            try:
                (
                    receipt_class_counts,
                    receipt_class_digests,
                ) = _validated_structure_drift_class_evidence(
                    receipt_payload["class_counts_json"],
                    receipt_payload["class_digests_json"],
                    expected_legacy_count=int(
                        progress_counts.get("legacy_member_scan_count", -1)
                    ),
                    expected_generation_count=int(
                        progress_counts.get("generation_member_scan_count", -1)
                    ),
                )
                sealed_class_digests = progress_digests.get(
                    "sealed_class_digests"
                )
                class_evidence_valid = (
                    progress_class_shape_valid
                    and isinstance(sealed_class_digests, dict)
                    and receipt_class_digests == sealed_class_digests
                    and receipt_class_counts == class_counts
                )
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                receipt_class_counts = {}
                class_evidence_valid = False
            member_comparison_count = receipt_payload[
                "generation_projection_member_comparison_count"
            ]
            group_comparison_count = receipt_payload[
                "generation_source_group_truth_comparison_count"
            ]
            member_counts_valid = (
                type(member_comparison_count) is int
                and member_comparison_count >= 0
                and member_comparison_count
                == progress_counts.get(
                    "generation_projection_member_comparison_count"
                )
                == progress_counts.get("projection_member_count")
                == progress_counts.get("generation_member_count")
                == progress_counts.get("generation_member_scan_count")
            )
            group_counts_valid = (
                type(group_comparison_count) is int
                and group_comparison_count >= 0
                and group_comparison_count
                == progress_counts.get(
                    "generation_source_group_truth_comparison_count"
                )
                == progress_counts.get("source_group_truth_count")
                == progress_counts.get("generation_group_truth_count")
            )
            reconstruction_counts_valid = (
                class_evidence_valid
                and progress_counts.get("legacy_member_scan_count")
                == receipt_class_counts.get("shared", -1)
                + sum(
                    receipt_class_counts.get(tag, -1)
                    for tag in (
                        "current-nontradable",
                        "event-only-quarantine",
                        "market-side-quarantine",
                        "fresh-source-absent",
                        "fresh-group-ineligible",
                    )
                )
                and progress_counts.get("generation_member_scan_count")
                == receipt_class_counts.get("shared", -1)
                + receipt_class_counts.get("fresh-addition", -1)
            )
            receipt_valid = (
                progress_exclusion_evidence_valid
                and expected_receipt_digest is not None
                and receipt_row[-1] == expected_receipt_digest
                and receipt_payload["comparison_id"] == progress[0]
                and receipt_payload["hash_algorithm"] == progress[5]
                and receipt_payload["hash_algorithm"] == ROW_CHAIN_SHA256_V2
                and receipt_payload["classifier_contract_version"] == progress[11]
                and progress[11] == STRUCTURE_DRIFT_CLASSIFIER_V4
                and receipt_payload["legacy_snapshot_id"] == legacy[0]
                and receipt_payload["generation_snapshot_id"] == current[0]
                and receipt_payload["publication_id"] == current[1]
                and receipt_payload["window_id"] == current[4]
                and receipt_payload["published_snapshot_id"] == current[0]
                and receipt_payload["normalization_contract_version"] == current[5]
                and receipt_payload["exact_receipt_digest"] == current[3]
                and receipt_payload["pointer_validation_hash"] == current[2]
                and receipt_payload["generation_certification_hash"] == current[6]
                and receipt_payload["source_event_count"] == progress[6]
                and receipt_payload["source_market_count"] == progress[7]
                and receipt_payload["source_event_hash"] == progress[8]
                and receipt_payload["source_market_hash"] == progress[9]
                and receipt_payload["source_identity_hash"] == progress[10]
                and receipt_payload["projection_member_receipt_digest"]
                == progress[17]
                == current_member_digest
                and receipt_payload["projection_universe_hash"]
                == progress_digests.get("projection_member_root")
                and receipt_payload["generation_universe_hash"]
                == progress_digests.get("generation_member_root")
                and receipt_payload["projection_group_truth_hash"]
                == progress_digests.get("source_group_truth_hash")
                and receipt_payload["generation_group_truth_hash"]
                == progress_digests.get("generation_group_truth_hash")
                and member_counts_valid
                and receipt_payload["generation_projection_member_comparison_root"]
                == progress_digests.get(
                    "generation_projection_member_comparison_root"
                )
                and group_counts_valid
                and receipt_payload[
                    "generation_source_group_truth_comparison_root"
                ]
                == progress_digests.get(
                    "generation_source_group_truth_comparison_root"
                )
                and receipt_payload["projection_universe_hash"]
                == receipt_payload["generation_projection_member_comparison_root"]
                and receipt_payload["projection_group_truth_hash"]
                == receipt_payload[
                    "generation_source_group_truth_comparison_root"
                ]
                and class_evidence_valid
                and reconstruction_counts_valid
                and receipt_payload["legacy_reconstruction_root"]
                == progress_digests.get("legacy_reconstruction_root")
                and receipt_payload["generation_reconstruction_root"]
                == progress_digests.get("generation_reconstruction_root")
                and receipt_payload["overlap_conflict_count"] == 0
                and receipt_payload["overlap_conflict_count"]
                == receipt_class_counts.get("overlap-conflict")
                and receipt_payload["unclassified_count"] == 0
                and receipt_payload["unclassified_count"]
                == receipt_class_counts.get("unclassified")
                and receipt_payload["diagnostic_counts_json"] == progress[13]
                and receipt_payload["diagnostic_root"] == progress[14]
                and receipt_payload["diagnostic_samples_json"] == progress[15]
                and receipt_payload["diagnostic_samples_digest"] == progress[16]
                and receipt_payload["projection_candidate_count"] == progress[18]
                and receipt_payload["projection_exclusion_count"] == progress[19]
                and receipt_payload["projection_exclusion_counts_json"] == progress[20]
                and receipt_payload["projection_exclusion_roots_json"] == progress[21]
                and receipt_payload["diagnostic_samples_digest"]
                == hashlib.sha256(str(progress[15]).encode()).hexdigest()
                and progress_digests.get("receipt_digest") == receipt_row[-1]
            )
            return {
                **base,
                **(projection_status if receipt_valid else {}),
                "authorization_mode": (
                    "drift-safe-sealed" if receipt_valid else "none"
                ),
                "authorized": receipt_valid,
                "progress_id": str(progress[0]),
                "hash_algorithm": str(progress[5]),
                "class_counts": class_counts if receipt_valid else {},
                "checkpoint_at_ms": int(progress[4]),
                "phase": str(progress[1]),
                "reason": None if receipt_valid else "structure-drift-receipt-invalid",
                "receipt_digest": str(receipt_row[-1]),
            }

    def advance_current_structure_drift_chunk(
        self,
        *,
        max_rows: int,
        now_ms: int,
    ) -> StructureCertificationChunk | None:
        """Advance at most one pending current-identity chunk for the scheduler."""
        status = self.structure_generation_drift_status()
        if status.get("authorized") is True or status.get("phase") == "stale":
            return None
        if status.get("reason") not in {
            "structure-drift-progress-missing",
            "structure-drift-incomplete",
        }:
            return None
        comparison_id = self.initialize_structure_drift_comparison(now_ms=now_ms)
        return self.advance_structure_drift_comparison_chunk(
            comparison_id,
            max_rows=max_rows,
            now_ms=now_ms,
        )

    def structure_event_only_market_ids(
        self,
        publication_id: str,
        event_ids: list[str],
    ) -> dict[str, frozenset[str]]:
        """Classify one bounded event chunk's unique market anti-join candidates."""
        if (
            not publication_id
            or len(event_ids) > 500
            or any(
                not isinstance(event_id, str) or not event_id for event_id in event_ids
            )
        ):
            raise ValueError("invalid-structure-event-only-chunk")
        if not event_ids:
            return {}
        placeholders = ",".join("?" for _ in event_ids)
        with sqlite3.connect(self._db_path) as con:
            rows = con.execute(
                "SELECT relation.event_id,relation.market_id FROM structure_publications p "
                "JOIN structure_sync_windows window ON window.id=p.window_id "
                "JOIN structure_sync_event_market_staging relation "
                "ON relation.window_id=p.window_id LEFT JOIN "
                "structure_sync_market_staging market ON market.window_id=p.window_id "
                "AND market.market_id=relation.market_id WHERE p.publication_id=? "
                "AND window.status='complete' AND market.market_id IS NULL "
                f"AND relation.event_id IN ({placeholders}) AND 1=(SELECT "
                "COUNT(DISTINCT other.event_id) FROM structure_sync_event_market_staging "
                "other WHERE other.window_id=p.window_id "
                "AND other.market_id=relation.market_id) ORDER BY "
                "relation.event_id,relation.market_id",
                (publication_id, *event_ids),
            ).fetchall()
        grouped: dict[str, set[str]] = {}
        for event_id, market_id in rows:
            grouped.setdefault(str(event_id), set()).add(str(market_id))
        return {key: frozenset(value) for key, value in grouped.items()}

    def fetch_structure_issue_source_chunk(
        self,
        *,
        window_id: str,
        after_market_id: str | None,
        limit: int,
        deadline_monotonic: float | None = None,
    ) -> list[tuple[str, dict[str, object]]]:
        """Read a bounded union of market-side and unique event-only candidates."""
        if not window_id or not 1 <= limit <= 500:
            raise ValueError("invalid-structure-issue-source-chunk")
        with self._connect_deadline_read(deadline_monotonic) as con:
            market_cursor_clause = ""
            event_cursor_clause = ""
            market_parameters: list[object] = [window_id]
            event_parameters: list[object] = [window_id]
            if after_market_id is not None:
                market_cursor_clause = " AND market_id>?"
                event_cursor_clause = " AND relation.market_id>?"
                market_parameters.append(after_market_id)
                event_parameters.append(after_market_id)
            keys = con.execute(
                "WITH market_candidates AS (SELECT market_id,'market' source_kind "
                "FROM structure_sync_market_staging WHERE window_id=?"
                f"{market_cursor_clause} ORDER BY market_id LIMIT ?),"
                "event_only_candidates AS (SELECT relation.market_id,"
                "'event_only' source_kind FROM "
                "structure_sync_event_market_staging relation LEFT JOIN "
                "structure_sync_market_staging market ON market.window_id=relation.window_id "
                "AND market.market_id=relation.market_id WHERE relation.window_id=? "
                f"{event_cursor_clause} AND market.market_id IS NULL GROUP BY "
                "relation.market_id HAVING COUNT(*)=1 ORDER BY relation.market_id "
                "LIMIT ?) SELECT market_id,source_kind FROM market_candidates UNION ALL "
                "SELECT market_id,source_kind FROM event_only_candidates ORDER BY "
                "market_id LIMIT ?",
                (*market_parameters, limit, *event_parameters, limit, limit),
            ).fetchall()
            result: list[tuple[str, dict[str, object]]] = []
            for market_id, source_kind in keys:
                market_id = str(market_id)
                if source_kind == "market":
                    source = con.execute(
                        "SELECT payload_json FROM structure_sync_market_staging "
                        "WHERE window_id=? AND market_id=?",
                        (window_id, market_id),
                    ).fetchone()
                    event_ids = tuple(
                        str(row[0])
                        for row in con.execute(
                            "SELECT event_id FROM structure_sync_event_market_staging "
                            "WHERE window_id=? AND market_id=? ORDER BY "
                            "source_ordinal,event_id",
                            (window_id, market_id),
                        ).fetchall()
                    )
                    assert source is not None
                    result.append(
                        (
                            market_id,
                            {
                                "source_kind": "market",
                                "raw": json.loads(str(source[0])),
                                "event_ids": event_ids,
                            },
                        )
                    )
                    continue
                source = con.execute(
                    "SELECT event.payload_json,relation.source_ordinal FROM "
                    "structure_sync_event_market_staging relation JOIN "
                    "structure_sync_event_staging event ON event.window_id=relation.window_id "
                    "AND event.event_id=relation.event_id WHERE relation.window_id=? "
                    "AND relation.market_id=? ORDER BY relation.source_ordinal,event.event_id",
                    (window_id, market_id),
                ).fetchone()
                assert source is not None
                result.append(
                    (
                        market_id,
                        {
                            "source_kind": "event_only",
                            "raw_event": json.loads(str(source[0])),
                            "event_source_ordinal": int(source[1]),
                        },
                    )
                )
        return result

    def seal_structure_publication_counts(
        self,
        publication_id: str,
        *,
        now_ms: int,
        writer_timeout_s: float | None = None,
    ) -> dict[str, int]:
        """Freeze deterministic normalized counts before terminal certification."""
        if writer_timeout_s is not None and writer_timeout_s <= 0:
            raise ValueError("invalid-structure-publication-seal")
        con = self._connect_writer(timeout_s=writer_timeout_s)
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
                str(key): int(value) for key, value in json.loads(str(row[4])).items()
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

    @staticmethod
    def _start_structure_comparison(
        con: sqlite3.Connection,
        *,
        publication_id: str,
        snapshot_id: int,
        generation_market_count: int,
        now_ms: int,
    ) -> None:
        """Pin legacy identity and initialize canonical list framing."""
        if not _initialize_structure_comparison_progress(
            con,
            publication_id=publication_id,
            snapshot_id=snapshot_id,
            generation_market_count=generation_market_count,
            now_ms=now_ms,
        ):
            raise ValueError("structure-comparison-legacy-unavailable")

    @staticmethod
    def _comparison_legacy_identity(
        con: sqlite3.Connection,
    ) -> tuple[int, int, int, int] | None:
        row = con.execute(
            "SELECT s.id,s.taken_at_ms,s.finished_at_ms,s.market_count FROM snapshots s "
            "JOIN snapshot_source_coverage c ON c.snapshot_id=s.id AND c.completed=1 "
            "WHERE s.data_product='structure' AND s.market_view_published=1 "
            "AND s.is_valid=1 ORDER BY s.id DESC LIMIT 1"
        ).fetchone()
        return None if row is None else tuple(int(value) for value in row)  # type: ignore[return-value]

    def _advance_structure_comparison_chunk(
        self,
        publication_id: str,
        *,
        max_rows: int,
        now_ms: int,
        repair_published: bool = False,
    ) -> StructureCertificationChunk:
        """Advance one canonical comparison phase by at most ``max_rows``."""
        with sqlite3.connect(self._db_path) as read_con:
            read_con.execute("BEGIN")
            publication = read_con.execute(
                "SELECT snapshot_id,window_id,status,committed_counts_json,"
                "validation_hash,certification_hash,certification_component "
                "FROM structure_publications "
                "WHERE publication_id=?",
                (publication_id,),
            ).fetchone()
            normal_writing = publication is not None and publication[2] == "writing"
            published_repair = (
                publication is not None
                and repair_published
                and publication[2] == "published"
                and publication[6]
                == (
                    "backfill-authenticated"
                    if str(publication[1]).startswith("backfill:")
                    else "bounded-complete"
                )
            )
            if (
                publication is None
                or not (normal_writing or published_repair)
                or publication[4] != publication[5]
            ):
                raise ValueError("structure-comparison-not-writing")
            progress = read_con.execute(
                "SELECT generation_snapshot_id,legacy_snapshot_id,legacy_taken_at_ms,"
                "legacy_finished_at_ms,legacy_market_count,phase,row_cursor_json,"
                "digest_state_json,phase_row_count,legacy_universe_hash,"
                "generation_universe_hash,legacy_source_truth_hash,"
                "generation_source_truth_hash,checkpoint_at_ms "
                "FROM structure_generation_comparison_progress WHERE publication_id=?",
                (publication_id,),
            ).fetchone()
            if progress is None:
                raise ValueError("structure-comparison-progress-missing")
            snapshot_id = int(publication[0])
            committed = json.loads(str(publication[3]))
            pinned = tuple(int(value) for value in progress[1:5])
            current_legacy = self._comparison_legacy_identity(read_con)
            bootstrap = int(progress[1]) == snapshot_id and current_legacy is None
            if not bootstrap and current_legacy != pinned:
                raise ValueError("structure-comparison-legacy-drift")
            phase = str(progress[5])
            cursor = None if progress[6] is None else json.loads(str(progress[6]))
            digest = SerializableSHA256.from_json(str(progress[7]))
            phase_count = int(progress[8])
            generation = phase.startswith("generation-")
            target_snapshot = snapshot_id if generation else int(progress[1])
            prefix = "structure_generation_" if generation else ""
            truth_table = (
                f"{prefix}group_truth" if generation else "neg_risk_group_truth"
            )
            if phase.endswith("universe"):
                membership_table = (
                    f"{prefix}memberships" if generation else "event_market_memberships"
                )
                market_table = f"{prefix}markets"
                clause = ""
                parameters: list[object] = [target_snapshot]
                if cursor is not None:
                    clause = (
                        " AND (t.neg_risk_market_id,t.membership_hash,"
                        "k.market_id,k.yes_token_id)>(?,?,?,?)"
                    )
                    parameters.extend(cursor)
                parameters.append(max_rows)
                rows = read_con.execute(
                    "SELECT t.neg_risk_market_id,t.membership_hash,k.market_id,"
                    f"k.yes_token_id FROM {truth_table} t JOIN {membership_table} m ON "
                    "m.snapshot_id=t.snapshot_id AND m.event_id=t.event_id AND "
                    "m.neg_risk_market_id=t.neg_risk_market_id JOIN "
                    f"{market_table} k ON k.snapshot_id=m.snapshot_id AND "
                    "k.market_id=m.market_id AND k.event_id=t.event_id AND "
                    "k.neg_risk_market_id=t.neg_risk_market_id WHERE t.snapshot_id=? "
                    "AND t.neg_risk_type='standard' AND t.quality='complete-supported' "
                    "AND m.member_kind='named' AND m.active=1 AND m.closed=0 "
                    "AND k.active=1 AND k.closed=0 AND k.incomplete=0 "
                    f"AND trim(k.yes_token_id)!=''{clause} ORDER BY "
                    "t.neg_risk_market_id,t.membership_hash,k.market_id,k.yes_token_id "
                    "LIMIT ?",
                    parameters,
                ).fetchall()
                next_cursor = list(rows[-1]) if rows else None
            else:
                clause = ""
                parameters = [target_snapshot]
                if cursor is not None:
                    clause = (
                        " AND (neg_risk_market_id,quality,COALESCE(reason,"
                        "'neg-risk-group-not-supported'))>(?,?,?)"
                    )
                    parameters.extend(cursor)
                parameters.append(max_rows)
                rows = read_con.execute(
                    "SELECT neg_risk_market_id,quality,COALESCE(reason,"
                    f"'neg-risk-group-not-supported') FROM {truth_table} "
                    "WHERE snapshot_id=? AND (neg_risk_type!='standard' OR "
                    f"quality!='complete-supported'){clause} ORDER BY "
                    "neg_risk_market_id,quality,COALESCE(reason,"
                    "'neg-risk-group-not-supported') LIMIT ?",
                    parameters,
                ).fetchall()
                next_cursor = list(rows[-1]) if rows else None
            for row in rows:
                if phase_count:
                    digest.update(b",")
                digest.update(
                    json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode()
                )
                phase_count += 1
            prior_state = str(progress[7])
            prior_cursor = progress[6]
            prior_checkpoint = int(progress[13])
        # Comparison is a low-priority, resumable producer.  Do not inherit
        # SQLite's 120-second bulk-writer timeout here: a contended checkpoint
        # must fail visibly and retry, not consume the child hard-limit while
        # the higher-priority Quote producer needs the same database.
        writer = self._connect_writer(
            timeout_s=STRUCTURE_POINTER_SWITCH_WRITER_LOCK_TIMEOUT_S
        )
        try:
            writer.execute("BEGIN IMMEDIATE")
            writer_legacy = self._comparison_legacy_identity(writer)
            writer_bootstrap = int(progress[1]) == snapshot_id and writer_legacy is None
            if not writer_bootstrap and writer_legacy != pinned:
                raise ValueError("structure-comparison-legacy-drift")
            if rows:
                next_cursor_json = json.dumps(next_cursor, separators=(",", ":"))
                changed = writer.execute(
                    "UPDATE structure_generation_comparison_progress SET "
                    "row_cursor_json=?,digest_state_json=?,phase_row_count=?,"
                    "checkpoint_at_ms=? WHERE publication_id=? AND phase=? "
                    "AND row_cursor_json IS ? AND digest_state_json=? "
                    "AND checkpoint_at_ms=?",
                    (
                        next_cursor_json,
                        digest.to_json(),
                        phase_count,
                        now_ms,
                        publication_id,
                        phase,
                        prior_cursor,
                        prior_state,
                        prior_checkpoint,
                    ),
                )
                next_phase = phase
                ready = False
            else:
                closing = b"]" if phase.endswith("universe") else b"]]"
                digest.update(closing)
                final_hash = digest.hexdigest()
                phases = (
                    "legacy-universe",
                    "generation-universe",
                    "legacy-rejections",
                    "generation-rejections",
                )
                index = phases.index(phase)
                ready = index == len(phases) - 1
                if ready:
                    hashes = (
                        str(progress[9]),
                        str(progress[10]),
                        str(progress[11]),
                        final_hash,
                    )
                    if any(len(value) != 64 for value in hashes):
                        raise ValueError("structure-comparison-hash-incomplete")
                    receipt_values = (
                        snapshot_id,
                        publication_id,
                        int(progress[1]),
                        int(progress[4]),
                        int(committed["markets"]),
                        *hashes,
                        str(publication[4]),
                        now_ms,
                    )
                    receipt_digest = _comparison_receipt_digest(
                        generation_snapshot_id=receipt_values[0],
                        publication_id=receipt_values[1],
                        legacy_snapshot_id=receipt_values[2],
                        legacy_market_count=receipt_values[3],
                        generation_market_count=receipt_values[4],
                        legacy_universe_hash=receipt_values[5],
                        generation_universe_hash=receipt_values[6],
                        legacy_source_truth_hash=receipt_values[7],
                        generation_source_truth_hash=receipt_values[8],
                        generation_validation_hash=receipt_values[9],
                        created_at_ms=receipt_values[10],
                    )
                    writer.execute(
                        "DELETE FROM structure_generation_comparison_receipts "
                        "WHERE generation_snapshot_id=? AND receipt_digest IS NULL",
                        (snapshot_id,),
                    )
                    writer.execute(
                        "INSERT INTO structure_generation_comparison_receipts("
                        "generation_snapshot_id,publication_id,legacy_snapshot_id,"
                        "legacy_market_count,generation_market_count,legacy_universe_hash,"
                        "generation_universe_hash,legacy_source_truth_hash,"
                        "generation_source_truth_hash,generation_validation_hash,"
                        "created_at_ms,receipt_digest) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                        (*receipt_values, receipt_digest),
                    )
                    progress_changed = writer.execute(
                        "UPDATE structure_generation_comparison_progress SET phase='sealed',"
                        "row_cursor_json=NULL,digest_state_json=?,phase_row_count=?,"
                        "generation_source_truth_hash=?,checkpoint_at_ms=? "
                        "WHERE publication_id=? AND phase=? AND row_cursor_json IS ? "
                        "AND digest_state_json=? AND checkpoint_at_ms=?",
                        (
                            digest.to_json(),
                            phase_count,
                            final_hash,
                            now_ms,
                            publication_id,
                            phase,
                            prior_cursor,
                            prior_state,
                            prior_checkpoint,
                        ),
                    )
                    if progress_changed.rowcount != 1:
                        raise StructurePublicationCursorError(
                            "structure-comparison-cursor-mismatch"
                        )
                    if repair_published:
                        changed = writer.execute(
                            "UPDATE current_structure_generation SET "
                            "comparison_receipt_digest=? WHERE id=1 AND snapshot_id=? "
                            "AND publication_id=? AND comparison_receipt_digest IS NULL",
                            (receipt_digest, snapshot_id, publication_id),
                        )
                    else:
                        marker = (
                            "backfill-authenticated"
                            if str(publication[1]).startswith("backfill:")
                            else "bounded-complete"
                        )
                        changed = writer.execute(
                            "UPDATE structure_publications SET status='ready',"
                            "certification_component=?,certified_at_ms=?,"
                            "checkpoint_at_ms=? WHERE publication_id=? "
                            "AND status='writing' AND "
                            "certification_component='comparison'",
                            (marker, now_ms, now_ms, publication_id),
                        )
                    next_phase = None
                else:
                    next_phase = phases[index + 1]
                    next_digest = SerializableSHA256.new()
                    if next_phase == "legacy-rejections":
                        next_digest.update(b"[")
                        next_digest.update(json.dumps(final_hash).encode())
                        next_digest.update(b",[")
                    elif next_phase == "generation-rejections":
                        next_digest.update(b"[")
                        next_digest.update(json.dumps(str(progress[10])).encode())
                        next_digest.update(b",[")
                    else:
                        next_digest.update(b"[")
                    hash_column = (
                        "legacy_universe_hash"
                        if phase == "legacy-universe"
                        else "generation_universe_hash"
                        if phase == "generation-universe"
                        else "legacy_source_truth_hash"
                    )
                    changed = writer.execute(
                        "UPDATE structure_generation_comparison_progress SET phase=?,"
                        "row_cursor_json=NULL,digest_state_json=?,phase_row_count=0,"
                        f"{hash_column}=?,checkpoint_at_ms=? WHERE publication_id=? "
                        "AND phase=? AND row_cursor_json IS ? AND digest_state_json=? "
                        "AND checkpoint_at_ms=?",
                        (
                            next_phase,
                            next_digest.to_json(),
                            final_hash,
                            now_ms,
                            publication_id,
                            phase,
                            prior_cursor,
                            prior_state,
                            prior_checkpoint,
                        ),
                    )
            if changed.rowcount != 1:
                raise StructurePublicationCursorError(
                    "structure-comparison-cursor-mismatch"
                )
            writer.execute("COMMIT")
        except BaseException:
            if writer.in_transaction:
                writer.execute("ROLLBACK")
            raise
        finally:
            writer.close()
        return StructureCertificationChunk(
            next_phase,
            None if not rows else next_cursor_json,
            len(rows),
            ready,
        )

    @staticmethod
    def _structure_quarantine_evidence_matches(
        con: sqlite3.Connection,
        *,
        publication_id: str,
        snapshot_id: int,
        market_id: str,
        layer: object,
        category: object,
        detail: object,
        raw_payload: object,
    ) -> bool:
        """Recompute one exact source quarantine receipt from the pinned window."""
        from polyarb.perception.structure_publication import (
            event_only_member_quarantine_issue,
            market_quarantine_issue,
        )

        source = con.execute(
            "SELECT raw.payload_json,p.window_id FROM structure_publications p JOIN "
            "structure_sync_market_staging raw ON raw.window_id=p.window_id "
            "WHERE p.publication_id=? AND raw.market_id=?",
            (publication_id, market_id),
        ).fetchone()
        if source is None:
            event_source = con.execute(
                "SELECT event.payload_json,relation.source_ordinal,p.window_id "
                "FROM structure_publications p JOIN structure_sync_windows window "
                "ON window.id=p.window_id JOIN "
                "structure_sync_event_market_staging relation "
                "ON relation.window_id=p.window_id JOIN "
                "structure_sync_event_staging event ON event.window_id=relation.window_id "
                "AND event.event_id=relation.event_id LEFT JOIN "
                "structure_sync_market_staging market ON market.window_id=p.window_id "
                "AND market.market_id=relation.market_id WHERE p.publication_id=? "
                "AND relation.market_id=? AND window.status='complete' "
                "AND market.market_id IS NULL AND "
                "COALESCE(event.source_ordinal,event.rowid)=relation.source_ordinal "
                "AND 1=(SELECT COUNT(DISTINCT other.event_id) FROM "
                "structure_sync_event_market_staging other WHERE "
                "other.window_id=p.window_id AND other.market_id=relation.market_id)",
                (publication_id, market_id),
            ).fetchone()
            if event_source is None:
                return False
            expected = event_only_member_quarantine_issue(
                json.loads(str(event_source[0])),
                event_source_ordinal=int(event_source[1]),
                market_id=market_id,
            )
            generated_market = con.execute(
                "SELECT 1 FROM structure_generation_markets WHERE snapshot_id=? AND market_id=?",
                (snapshot_id, market_id),
            ).fetchone()
            generated_membership = con.execute(
                "SELECT 1 FROM structure_generation_memberships WHERE snapshot_id=? "
                "AND market_id=?",
                (snapshot_id, market_id),
            ).fetchone()
            return bool(
                expected is not None
                and generated_market is None
                and generated_membership is None
                and layer == expected["layer"]
                and category == expected["category"]
                and detail == expected["detail"]
                and raw_payload == expected["raw_payload"]
            )
        event_ids = tuple(
            str(row[0])
            for row in con.execute(
                "SELECT event_id FROM structure_sync_event_market_staging "
                "WHERE window_id=? AND market_id=? ORDER BY source_ordinal,event_id",
                (str(source[1]), market_id),
            ).fetchall()
        )
        expected = market_quarantine_issue(
            market_id, json.loads(str(source[0])), event_ids
        )
        generated = con.execute(
            "SELECT 1 FROM structure_generation_markets WHERE snapshot_id=? AND market_id=?",
            (snapshot_id, market_id),
        ).fetchone()
        return bool(
            expected is not None
            and generated is None
            and layer == expected["layer"]
            and category == expected["category"]
            and detail == expected["detail"]
            and raw_payload == expected["raw_payload"]
        )

    def advance_structure_certification_chunk(
        self,
        publication_id: str,
        *,
        max_rows: int,
        now_ms: int,
        writer_timeout_s: float | None = None,
    ) -> StructureCertificationChunk:
        """Hash and validate at most one primary-key generation chunk."""
        if (
            not 1 <= max_rows <= STRUCTURE_PUBLICATION_MAX_ROWS
            or (writer_timeout_s is not None and writer_timeout_s <= 0)
        ):
            raise ValueError("structure-certification-max-rows-must-be-positive")
        order = {
            "events": ("id",),
            "event_tags": ("event_id", "tag_id"),
            "memberships": ("event_id", "market_id"),
            "group_truth": ("neg_risk_market_id",),
            "markets": ("market_id",),
            "issues": ("issue_index",),
            "source_events": ("event_id",),
            "source_markets": ("market_id",),
        }
        with sqlite3.connect(self._db_path) as read_con:
            publication = read_con.execute(
                "SELECT p.snapshot_id,p.window_id,p.status,p.expected_counts_json,"
                "committed_counts_json,certification_component,"
                "certification_row_cursor,certification_hash,"
                "certification_counts_json,s.taken_at_ms,window.status "
                "FROM structure_publications p JOIN snapshots s ON "
                "s.id=p.snapshot_id JOIN structure_sync_windows window ON "
                "window.id=p.window_id WHERE p.publication_id=?",
                (publication_id,),
            ).fetchone()
            if publication is None or publication[2] != "writing":
                raise ValueError("structure-publication-not-writing")
            if publication[3] != publication[4]:
                raise ValueError("generation-incomplete")
            snapshot_id = int(publication[0])
            window_id = str(publication[1])
            is_backfill = window_id.startswith("backfill:")
            certification_components = (
                _STRUCTURE_COMPONENTS
                if is_backfill
                else _STRUCTURE_CERTIFICATION_COMPONENTS
            )
            taken_at_ms = int(publication[9])
            component = str(publication[5] or _STRUCTURE_COMPONENTS[0])
            if component == "comparison":
                return self._advance_structure_comparison_chunk(
                    publication_id,
                    max_rows=max_rows,
                    now_ms=now_ms,
                )
            if component not in certification_components:
                raise ValueError("unknown-structure-certification-component")
            if (
                component in _STRUCTURE_SOURCE_COMPONENTS
                and publication[10] != "complete"
            ):
                raise ValueError("source-truth-invalid")
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
                clause = f" AND ({','.join(keys)}) > ({','.join('?' for _ in keys)})"
                parameters.extend(cursor_values)
            parameters.append(max_rows)
            if component in _STRUCTURE_SOURCE_COMPONENTS:
                source = component.removeprefix("source_")
                singular = "event" if source == "events" else "market"
                table = f"structure_sync_{singular}_staging"
                rows = read_con.execute(
                    f"SELECT COALESCE(source_ordinal,rowid),{singular}_id,payload_json "
                    f"FROM {table} WHERE window_id=?{clause} "
                    f"ORDER BY {','.join(keys)} LIMIT ?",
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
            if component == "group_truth" and not is_backfill:
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
                        or len(members) != int(truth[4])
                        or sum(
                            member.member_kind == "named" and member.active
                            for member in members
                        )
                        != int(truth[5])
                        or membership_hash(event_id, group_id, members) != truth[6]
                    ):
                        raise StructureMembershipInvalidError(
                            "group-truth", (event_id, group_id)
                        )
            elif component == "memberships" and not is_backfill:
                # /events is the authority for structural membership, including
                # inactive/closed reserved outcomes intentionally absent from the
                # active /markets stream.  Source-event certification later proves
                # these flags came from the durable raw payload.  Validate this
                # whole bounded chunk in one anti-join: active-open members must
                # exist, and any market that does exist must retain group identity.
                invalid = read_con.execute(
                    "WITH membership_chunk AS ("
                    "SELECT event_id,neg_risk_market_id,market_id,active,closed "
                    "FROM structure_generation_memberships WHERE snapshot_id=?"
                    f"{clause} ORDER BY {','.join(keys)} LIMIT ?) "
                    "SELECT m.event_id,m.neg_risk_market_id,m.market_id,"
                    "CASE WHEN e.id IS NULL THEN 'event-missing' "
                    "WHEN k.market_id IS NULL THEN 'active-market-missing' "
                    "ELSE 'market-identity' END FROM membership_chunk m LEFT JOIN "
                    "structure_generation_events e ON e.snapshot_id=? AND e.id=m.event_id "
                    "LEFT JOIN structure_generation_markets k ON k.snapshot_id=? "
                    "AND k.market_id=m.market_id WHERE e.id IS NULL OR "
                    "(k.market_id IS NULL AND m.active=1 AND m.closed=0) OR "
                    "(k.market_id IS NOT NULL AND (k.event_id IS NOT m.event_id OR "
                    "k.neg_risk_market_id IS NOT m.neg_risk_market_id OR "
                    "k.active IS NOT m.active OR k.closed IS NOT m.closed)) LIMIT 1",
                    (*parameters, snapshot_id, snapshot_id),
                ).fetchone()
                if invalid is not None:
                    kind = (
                        "active-market-missing"
                        if invalid[3] == "active-market-missing"
                        else "market-identity"
                    )
                    raise StructureMembershipInvalidError(
                        kind, (invalid[0], invalid[1], invalid[2])
                    )
            elif component == "markets" and not is_backfill:
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
                        source[0],
                        source[1],
                        source[2],
                        source[3],
                        source[4],
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
            elif component == "issues" and rows and not is_backfill:
                for issue in rows:
                    if not self._structure_quarantine_evidence_matches(
                        read_con,
                        publication_id=publication_id,
                        snapshot_id=snapshot_id,
                        market_id=str(issue[positions["market_id"]]),
                        layer=issue[positions["layer"]],
                        category=issue[positions["category"]],
                        detail=issue[positions["detail"]],
                        raw_payload=issue[positions["raw_payload"]],
                    ):
                        raise ValueError("generation-validation-issues")
            elif component == "source_events":
                from polyarb.perception.structure_publication import (
                    event_only_member_quarantine_issue,
                    project_event_structure,
                )
                from polyarb.snapshot.normalizer import normalize_events

                event_ids = [str(source_event[1]) for source_event in rows]
                placeholders = ",".join("?" for _ in event_ids)
                event_only_by_event: dict[str, set[str]] = {}
                event_only_evidence: dict[str, tuple[object, ...]] = {}
                actual_events: dict[str, tuple[object, ...]] = {}
                actual_tags: dict[str, list[tuple[object, ...]]] = {}
                actual_memberships: dict[str, list[tuple[object, ...]]] = {}
                actual_truths: dict[str, list[tuple[object, ...]]] = {}
                if event_ids:
                    evidence_rows = read_con.execute(
                        "SELECT relation.event_id,relation.market_id,"
                        "relation.source_ordinal,issue.layer,issue.category,issue.detail,"
                        "issue.raw_payload,generated_market.market_id,"
                        "generated_member.market_id FROM "
                        "structure_sync_event_market_staging relation JOIN "
                        "structure_sync_windows source_window ON "
                        "source_window.id=relation.window_id LEFT JOIN "
                        "structure_sync_market_staging source_market ON "
                        "source_market.window_id=relation.window_id AND "
                        "source_market.market_id=relation.market_id LEFT JOIN "
                        "structure_generation_issues issue ON issue.snapshot_id=? AND "
                        "issue.market_id=relation.market_id LEFT JOIN "
                        "structure_generation_markets generated_market ON "
                        "generated_market.snapshot_id=? AND "
                        "generated_market.market_id=relation.market_id LEFT JOIN "
                        "structure_generation_memberships generated_member ON "
                        "generated_member.snapshot_id=? AND "
                        "generated_member.market_id=relation.market_id WHERE "
                        "relation.window_id=? AND source_window.status='complete' "
                        "AND source_market.market_id IS NULL "
                        f"AND relation.event_id IN ({placeholders}) AND 1=(SELECT "
                        "COUNT(DISTINCT other.event_id) FROM "
                        "structure_sync_event_market_staging other WHERE "
                        "other.window_id=relation.window_id AND "
                        "other.market_id=relation.market_id) ORDER BY "
                        "relation.event_id,relation.market_id",
                        (snapshot_id, snapshot_id, snapshot_id, window_id, *event_ids),
                    ).fetchall()
                    for evidence in evidence_rows:
                        event_id = str(evidence[0])
                        market_id = str(evidence[1])
                        event_only_by_event.setdefault(event_id, set()).add(market_id)
                        event_only_evidence[market_id] = tuple(evidence[2:])
                    for actual in read_con.execute(
                        f"SELECT {','.join(EVENTS_COLUMN_ORDER)} FROM "
                        "structure_generation_events WHERE snapshot_id=? "
                        f"AND id IN ({placeholders}) ORDER BY id",
                        (snapshot_id, *event_ids),
                    ):
                        actual_events[str(actual[0])] = tuple(actual)
                    for actual in read_con.execute(
                        f"SELECT {','.join(EVENT_TAGS_COLUMN_ORDER)} FROM "
                        "structure_generation_event_tags WHERE snapshot_id=? "
                        f"AND event_id IN ({placeholders}) ORDER BY event_id,tag_id",
                        (snapshot_id, *event_ids),
                    ):
                        actual_tags.setdefault(str(actual[0]), []).append(tuple(actual))
                    for actual in read_con.execute(
                        "SELECT snapshot_id,event_id,neg_risk_market_id,market_id,"
                        "member_kind,active,closed FROM "
                        "structure_generation_memberships WHERE snapshot_id=? "
                        f"AND event_id IN ({placeholders}) ORDER BY event_id,market_id",
                        (snapshot_id, *event_ids),
                    ):
                        actual_memberships.setdefault(str(actual[1]), []).append(
                            tuple(actual)
                        )
                    for actual in read_con.execute(
                        "SELECT snapshot_id,event_id,neg_risk_market_id,neg_risk_type,"
                        "expected_member_count,active_named_count,membership_hash,quality,"
                        "reason FROM structure_generation_group_truth WHERE snapshot_id=? "
                        f"AND event_id IN ({placeholders}) ORDER BY "
                        "event_id,neg_risk_market_id",
                        (snapshot_id, *event_ids),
                    ):
                        actual_truths.setdefault(str(actual[1]), []).append(
                            tuple(actual)
                        )
                for source_event in rows:
                    raw = json.loads(str(source_event[2]))
                    events, tags, _mapping, members, truths = normalize_events([raw])
                    event_only_ids = frozenset(
                        event_only_by_event.get(str(source_event[1]), ())
                    )
                    members, truths = project_event_structure(raw, event_only_ids)
                    authenticated_event_only_ids = frozenset(
                        event_only_id
                        for event_only_id in event_only_ids
                        if event_only_member_quarantine_issue(
                            raw,
                            event_source_ordinal=int(source_event[0]),
                            market_id=event_only_id,
                        )
                        is not None
                    )
                    for event_only_id in authenticated_event_only_ids:
                        evidence = event_only_evidence.get(event_only_id)
                        expected_issue = event_only_member_quarantine_issue(
                            raw,
                            event_source_ordinal=int(source_event[0]),
                            market_id=event_only_id,
                        )
                        if (
                            evidence is None
                            or expected_issue is None
                            or evidence[0] != int(source_event[0])
                            or evidence[1:5]
                            != (
                                expected_issue["layer"],
                                expected_issue["category"],
                                expected_issue["detail"],
                                expected_issue["raw_payload"],
                            )
                            or evidence[5] is not None
                            or evidence[6] is not None
                        ):
                            raise ValueError("source-truth-invalid")
                    if len(events) != 1:
                        raise ValueError("source-truth-invalid")
                    events[0]["fetched_at_ms"] = taken_at_ms
                    actual_event = actual_events.get(str(source_event[1]))
                    if actual_event != _event_row_to_tuple(events[0], snapshot_id):
                        raise ValueError("source-truth-invalid")
                    expected_tags = sorted(
                        _event_tag_row_to_tuple(tag, snapshot_id) for tag in tags
                    )
                    if actual_tags.get(str(source_event[1]), []) != expected_tags:
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
                        actual_values = (
                            actual_memberships.get(str(source_event[1]), [])
                            if generated_component == "memberships"
                            else actual_truths.get(str(source_event[1]), [])
                        )
                        if columns is None:
                            if actual_values:
                                raise ValueError("source-truth-invalid")
                            continue
                        if actual_values != sorted(expected_values):
                            raise ValueError("source-truth-invalid")
            elif component == "source_markets":
                from polyarb.perception.structure_publication import (
                    market_quarantine_issue,
                )
                from polyarb.snapshot.normalizer import normalize_market

                market_ids = [str(source_market[1]) for source_market in rows]
                placeholders = ",".join("?" for _ in market_ids)
                parents: dict[str, list[str]] = {}
                generated_markets: dict[str, tuple[object, ...]] = {}
                generated_issues: dict[str, tuple[object, ...]] = {}
                if market_ids:
                    for parent in read_con.execute(
                        "SELECT market_id,event_id FROM "
                        "structure_sync_event_market_staging WHERE window_id=? "
                        f"AND market_id IN ({placeholders}) ORDER BY "
                        "market_id,source_ordinal,event_id",
                        (window_id, *market_ids),
                    ):
                        parents.setdefault(str(parent[0]), []).append(str(parent[1]))
                    for generated in read_con.execute(
                        f"SELECT {','.join(MARKETS_COLUMN_ORDER)} FROM "
                        "structure_generation_markets WHERE snapshot_id=? "
                        f"AND market_id IN ({placeholders}) ORDER BY market_id",
                        (snapshot_id, *market_ids),
                    ):
                        generated_markets[str(generated[0])] = tuple(generated)
                    for issue in read_con.execute(
                        "SELECT market_id,layer,category,detail,raw_payload FROM "
                        "structure_generation_issues WHERE snapshot_id=? "
                        f"AND market_id IN ({placeholders}) ORDER BY market_id",
                        (snapshot_id, *market_ids),
                    ):
                        generated_issues[str(issue[0])] = tuple(issue[1:])
                for source_market in rows:
                    raw = json.loads(str(source_market[2]))
                    market_id = str(source_market[1])
                    event_ids = parents.get(market_id, [])
                    normalized = normalize_market(
                        raw,
                        ({market_id: event_ids[0]} if event_ids else {}),
                    )
                    if normalized is None:
                        raise ValueError("source-truth-invalid")
                    normalized["fetched_at_ms"] = taken_at_ms
                    generated = generated_markets.get(market_id)
                    if generated is None:
                        issue = generated_issues.get(market_id)
                        expected_issue = market_quarantine_issue(
                            market_id, raw, tuple(event_ids)
                        )
                        if (
                            issue is not None
                            and expected_issue is not None
                            and issue
                            == (
                                expected_issue["layer"],
                                expected_issue["category"],
                                expected_issue["detail"],
                                expected_issue["raw_payload"],
                            )
                        ):
                            continue
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
            comparison_start = False
            if rows:
                next_cursor = json.dumps(
                    [rows[-1][positions[key]] for key in keys], separators=(",", ":")
                )
                ready = False
                next_component = component
            else:
                committed_counts = json.loads(str(publication[4]))
                if component in _STRUCTURE_SOURCE_COMPONENTS:
                    source_name = component.removeprefix("source_")
                    source_table = (
                        "structure_sync_event_staging"
                        if source_name == "events"
                        else "structure_sync_market_staging"
                    )
                    expected_count = int(
                        read_con.execute(
                            f"SELECT COUNT(*) FROM {source_table} WHERE window_id=?",  # noqa: S608
                            (window_id,),
                        ).fetchone()[0]
                    )
                else:
                    expected_count = int(committed_counts[component])
                if scanned_counts[component] != expected_count:
                    raise ValueError("generation-count-mismatch")
                index = certification_components.index(component)
                comparison_start = index + 1 == len(certification_components)
                ready = False
                next_component = (
                    "comparison"
                    if comparison_start
                    else certification_components[index + 1]
                )
                next_cursor = None
        con = self._connect_writer(timeout_s=writer_timeout_s)
        try:
            con.execute("BEGIN IMMEDIATE")
            cur = con.execute(
                "UPDATE structure_publications SET certification_component=?,"
                "certification_row_cursor=?,certification_hash=?,"
                "validation_hash=CASE WHEN ? THEN ? ELSE validation_hash END,"
                "certification_counts_json=?,checkpoint_at_ms=? "
                "WHERE publication_id=? AND status='writing' "
                "AND certification_component IS ? AND certification_row_cursor IS ? "
                "AND COALESCE(certification_hash,?)=?",
                (
                    next_component,
                    next_cursor,
                    next_hash,
                    comparison_start,
                    next_hash,
                    json.dumps(scanned_counts, sort_keys=True, separators=(",", ":")),
                    now_ms,
                    publication_id,
                    publication[5],
                    publication[6],
                    "0" * 64,
                    prior_hash,
                ),
            )
            if cur.rowcount != 1:
                raise StructurePublicationCursorError(
                    "structure-certification-cursor-mismatch"
                )
            if comparison_start:
                self._start_structure_comparison(
                    con,
                    publication_id=publication_id,
                    snapshot_id=snapshot_id,
                    generation_market_count=int(committed_counts["markets"]),
                    now_ms=now_ms,
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
        *,
        writer_timeout_s: float | None = None,
    ) -> StructurePublicationState:
        """Create or resume the publication bound to one complete raw window."""
        if (
            not window_id
            or now_ms < 0
            or (writer_timeout_s is not None and writer_timeout_s <= 0)
        ):
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
        member_status = self.structure_event_member_status(window_id=window_id)
        if (
            member_status.get("reason")
            == "structure-event-source-receipt-unavailable"
        ):
            raise ValueError("structure-event-source-receipt-unavailable")
        if not self.structure_event_market_backfill_complete(window_id):
            raise ValueError("structure-bootstrap-incomplete")
        if member_status.get("sealed") is not True:
            raise ValueError(
                str(member_status.get("reason") or member_status.get("failure_reason")
                    or "structure-event-member-sidecar-incomplete")
            )
        counts = {component: 0 for component in _STRUCTURE_COMPONENTS}
        expected_json = json.dumps(expected, sort_keys=True, separators=(",", ":"))
        counts_json = json.dumps(counts, sort_keys=True, separators=(",", ":"))
        con = self._connect_writer(timeout_s=writer_timeout_s)
        try:
            con.execute("BEGIN IMMEDIATE")
            window = con.execute(
                "SELECT status FROM structure_sync_windows WHERE id=?", (window_id,)
            ).fetchone()
            if window is None or window[0] != "complete":
                raise ValueError("structure-sync-window-not-complete")
            bootstrap = con.execute(
                "SELECT completed_at_ms,blocked_reason FROM "
                "structure_sync_event_market_backfill_progress WHERE window_id=?",
                (window_id,),
            ).fetchone()
            if bootstrap is None or bootstrap[0] is None or bootstrap[1] is not None:
                raise ValueError("structure-bootstrap-incomplete")
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
            if (
                con.execute(
                    "SELECT 1 FROM snapshots WHERE id=?", (snapshot_id,)
                ).fetchone()
                is not None
            ):
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
                "status,normalization_contract_version,expected_counts_json,"
                "committed_counts_json,created_at_ms,checkpoint_at_ms) "
                "VALUES (?,?,?,'writing',?,?,?,?,?)",
                (
                    publication_id,
                    window_id,
                    snapshot_id,
                    STRUCTURE_NORMALIZATION_CONTRACT_VERSION,
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

    def reconcile_structure_publication_contract(
        self,
        window_id: str,
        current_version: str,
        now_ms: int,
        *,
        failure_reason: str = "publication-contract-superseded",
        force_retire: bool = False,
        writer_timeout_s: float | None = None,
        transaction_deadline_s: float | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> StructurePublicationContractReconciliation:
        """Fail one incompatible unpublished generation and its source atomically."""
        if (
            not window_id
            or not current_version
            or now_ms < 0
            or failure_reason
            not in {
                "publication-contract-superseded",
                "publication-membership-invalid",
            }
            or (writer_timeout_s is not None and writer_timeout_s <= 0)
            or (transaction_deadline_s is not None and transaction_deadline_s <= 0)
        ):
            raise ValueError("invalid-structure-publication-contract")
        reason = failure_reason
        deadline = None if transaction_deadline_s is None else monotonic() + transaction_deadline_s

        def ensure_deadline() -> None:
            if deadline is not None and monotonic() >= deadline:
                raise StructurePublicationContractDeadlineError(
                    "publication-contract-deadline"
                )

        con = self._connect_writer(timeout_s=writer_timeout_s)
        try:
            if deadline is not None:
                con.set_progress_handler(lambda: int(monotonic() >= deadline), 1_000)
            ensure_deadline()
            con.execute("BEGIN IMMEDIATE")
            ensure_deadline()
            row = con.execute(
                "SELECT p.publication_id,p.snapshot_id,p.status,"
                "p.normalization_contract_version,p.failure_reason,s.data_product,"
                "s.snapshot_status,s.is_valid,s.market_view_published,w.status,"
                "w.failure_reason FROM structure_publications p "
                "JOIN snapshots s ON s.id=p.snapshot_id "
                "JOIN structure_sync_windows w ON w.id=p.window_id "
                "WHERE p.window_id=?",
                (window_id,),
            ).fetchone()
            if row is None:
                raise ValueError("structure-publication-not-found")
            publication_id = str(row[0])
            snapshot_id = int(row[1])
            status = str(row[2])
            stored_version = None if row[3] is None else str(row[3])
            if status == "published":
                con.execute("COMMIT")
                return StructurePublicationContractReconciliation(
                    publication_id, True, False
                )
            if status == "failed" and row[4] == reason:
                pointer_is_candidate = (
                    con.execute(
                        "SELECT 1 FROM current_structure_generation WHERE id=1 AND snapshot_id=?",
                        (snapshot_id,),
                    ).fetchone()
                    is not None
                )
                if (
                    (not force_retire and stored_version == current_version)
                    or row[5] != "structure"
                    or row[6] != "failed"
                    or int(row[7]) != 0
                    or int(row[8]) != 0
                    or row[9] != "failed"
                    or row[10] != reason
                    or pointer_is_candidate
                ):
                    raise ValueError("structure-publication-supersession-incomplete")
                con.execute("COMMIT")
                return StructurePublicationContractReconciliation(
                    publication_id, False, True
                )
            if status not in {"writing", "ready"}:
                raise ValueError("structure-publication-contract-not-reconcilable")
            pointer_is_candidate = (
                con.execute(
                    "SELECT 1 FROM current_structure_generation WHERE id=1 AND snapshot_id=?",
                    (snapshot_id,),
                ).fetchone()
                is not None
            )
            common_unsafe = (
                row[5] != "structure"
                or int(row[7]) != 0
                or int(row[8]) != 0
                or row[9] != "complete"
                or pointer_is_candidate
            )
            if stored_version == current_version and not force_retire:
                if common_unsafe or row[6] != "building":
                    raise ValueError("structure-publication-supersession-unsafe")
                con.execute("COMMIT")
                return StructurePublicationContractReconciliation(
                    publication_id, True, False
                )
            if common_unsafe or row[6] not in {"building", "failed"}:
                raise ValueError("structure-publication-supersession-unsafe")
            publication_change = con.execute(
                "UPDATE structure_publications SET status='failed',failure_reason=?,"
                "checkpoint_at_ms=? WHERE publication_id=? "
                "AND status IN ('writing','ready') "
                "AND normalization_contract_version IS ?",
                (reason, now_ms, publication_id, stored_version),
            )
            snapshot_change_count = 1
            if row[6] == "building":
                snapshot_change_count = con.execute(
                    "UPDATE snapshots SET snapshot_status='failed',finished_at_ms=? "
                    "WHERE id=? AND data_product='structure' "
                    "AND snapshot_status='building' AND is_valid=0 "
                    "AND market_view_published=0",
                    (now_ms, snapshot_id),
                ).rowcount
            window_change = con.execute(
                "UPDATE structure_sync_windows SET status='failed',failure_reason=?,"
                "checkpoint_at_ms=? WHERE id=? AND status='complete'",
                (reason, now_ms, window_id),
            )
            if (
                publication_change.rowcount != 1
                or snapshot_change_count != 1
                or window_change.rowcount != 1
            ):
                raise ValueError("structure-publication-supersession-race")
            ensure_deadline()
            con.execute("COMMIT")
            return StructurePublicationContractReconciliation(
                publication_id, False, True
            )
        except BaseException as error:
            con.set_progress_handler(None, 0)
            if con.in_transaction:
                con.execute("ROLLBACK")
            if (
                deadline is not None
                and isinstance(error, sqlite3.OperationalError)
                and "interrupted" in str(error).lower()
                and monotonic() >= deadline
            ):
                raise StructurePublicationContractDeadlineError(
                    "publication-contract-deadline"
                ) from error
            raise
        finally:
            con.set_progress_handler(None, 0)
            con.close()

    def retire_membership_invalid_structure_publication(
        self,
        window_id: str,
        *,
        now_ms: int,
    ) -> StructurePublicationContractReconciliation:
        """Retire one frozen source conflict without altering its evidence."""
        return self.reconcile_structure_publication_contract(
            window_id,
            STRUCTURE_NORMALIZATION_CONTRACT_VERSION,
            now_ms,
            failure_reason="publication-membership-invalid",
            force_retire=True,
        )

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
                "id",
                "slug",
                "title",
                "ticker",
                "active",
                "closed",
                "liquidity_usd",
                "volume_usd",
                "end_time_ms",
                "fetched_at_ms",
                "page_fetched_at_ms",
            ),
            "event_tags": ("event_id", "tag_id", "tag_label", "tag_slug"),
            "memberships": (
                "event_id",
                "neg_risk_market_id",
                "market_id",
                "member_kind",
                "active",
                "closed",
            ),
            "group_truth": (
                "event_id",
                "neg_risk_market_id",
                "neg_risk_type",
                "expected_member_count",
                "active_named_count",
                "membership_hash",
                "quality",
                "reason",
            ),
            "markets": tuple(
                column for column in MARKETS_COLUMN_ORDER if column != "snapshot_id"
            ),
            "issues": (
                "issue_index",
                "layer",
                "category",
                "market_id",
                "detail",
                "raw_payload",
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
        *,
        writer_timeout_s: float | None = None,
    ) -> None:
        """Write one bounded component chunk and its cursor in one transaction."""
        table = self._structure_component_table(component)
        if (
            not publication_id
            or now_ms < 0
            or (writer_timeout_s is not None and writer_timeout_s <= 0)
        ):
            raise ValueError("invalid-structure-publication-chunk")
        materialized = tuple(rows)
        con = self._connect_writer(timeout_s=writer_timeout_s)
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
                        "snapshot_id",
                        "event_id",
                        "neg_risk_market_id",
                        "market_id",
                    ),
                    "group_truth": ("snapshot_id", "event_id", "neg_risk_market_id"),
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
                    f"INSERT INTO {table}({','.join(columns)}) VALUES ({placeholders})",  # noqa: S608 - internal schema
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
                "AND e.id=m.event_id) OR ((m.active=1 AND m.closed=0) AND NOT EXISTS "
                "(SELECT 1 FROM structure_generation_markets k WHERE "
                "k.snapshot_id=m.snapshot_id AND k.market_id=m.market_id)) OR EXISTS "
                "(SELECT 1 FROM structure_generation_markets k WHERE "
                "k.snapshot_id=m.snapshot_id AND k.market_id=m.market_id AND "
                "(k.event_id IS NOT m.event_id OR k.neg_risk_market_id IS NOT "
                "m.neg_risk_market_id OR k.active IS NOT m.active OR "
                "k.closed IS NOT m.closed))) LIMIT 1",
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
                if (
                    membership_hash(str(event_id), str(group_id), durable_members)
                    != stored_hash
                ):
                    hash_invalid = True
                    break
            if invalid_truth is not None or orphan is not None or hash_invalid:
                raise StructureMembershipInvalidError(
                    "terminal-invariant", (snapshot_id,)
                )
            validation_hash = self._generation_hash(con, snapshot_id)
            counts_json = json.dumps(actual, sort_keys=True, separators=(",", ":"))
            con.execute(
                "UPDATE structure_publications SET status='writing',"
                "committed_counts_json=?,validation_hash=?,"
                "certification_component='comparison',certification_hash=?,"
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
            self._start_structure_comparison(
                con,
                publication_id=publication_id,
                snapshot_id=snapshot_id,
                generation_market_count=int(actual["markets"]),
                now_ms=certified_at_ms,
            )
            con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def publish_structure_generation(
        self,
        publication_id: str,
        now_ms: int,
        *,
        transaction_deadline_s: float = 15.0,
        writer_lock_timeout_s: float = 5.0,
        trace_callback: Callable[[str], None] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> int:
        """Atomically publish metadata and switch the singleton generation pointer."""
        if not publication_id or now_ms < 0:
            raise ValueError("invalid-structure-publication")
        if (
            transaction_deadline_s <= 0
            or writer_lock_timeout_s <= 0
            or writer_lock_timeout_s > transaction_deadline_s
        ):
            raise ValueError("invalid-pointer-switch-deadline")
        deadline = monotonic() + transaction_deadline_s
        con = self._connect_writer(timeout_s=writer_lock_timeout_s)

        def ensure_deadline() -> None:
            if monotonic() >= deadline:
                raise StructurePointerSwitchDeadlineError(
                    "pointer-switch-deadline"
                )

        try:
            if trace_callback is not None:
                con.set_trace_callback(trace_callback)
            con.set_progress_handler(lambda: int(monotonic() >= deadline), 1_000)
            ensure_deadline()
            con.execute("BEGIN IMMEDIATE")
            ensure_deadline()
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
            receipt = con.execute(
                "SELECT publication_id,legacy_snapshot_id,legacy_market_count,"
                "generation_market_count,legacy_universe_hash,generation_universe_hash,"
                "legacy_source_truth_hash,generation_source_truth_hash,"
                "generation_validation_hash,created_at_ms,receipt_digest "
                "FROM structure_generation_comparison_receipts "
                "WHERE generation_snapshot_id=?",
                (snapshot_id,),
            ).fetchone()
            if receipt is None or receipt[10] is None:
                raise ValueError("structure-publication-comparison-receipt-missing")
            receipt_digest = _comparison_receipt_digest(
                generation_snapshot_id=snapshot_id,
                publication_id=str(receipt[0]),
                legacy_snapshot_id=int(receipt[1]),
                legacy_market_count=int(receipt[2]),
                generation_market_count=int(receipt[3]),
                legacy_universe_hash=str(receipt[4]),
                generation_universe_hash=str(receipt[5]),
                legacy_source_truth_hash=str(receipt[6]),
                generation_source_truth_hash=str(receipt[7]),
                generation_validation_hash=str(receipt[8]),
                created_at_ms=int(receipt[9]),
            )
            if (
                receipt[10] != receipt_digest
                or receipt[0] != publication_id
                or int(receipt[3]) != int(actual["markets"])
                or receipt[8] != publication[5]
            ):
                raise ValueError("structure-publication-comparison-receipt-mismatch")
            ensure_deadline()
            con.execute(
                "UPDATE snapshots SET finished_at_ms=?,market_count=?,"
                "market_view_published=1,is_valid=1,snapshot_status='ok' WHERE id=?",
                (now_ms, actual["markets"], snapshot_id),
            )
            active_drift = con.execute(
                "SELECT comparison_id,generation_snapshot_id,publication_id FROM "
                "structure_generation_drift_progress WHERE phase NOT IN "
                "('sealed','stale') LIMIT 2"
            ).fetchall()
            if len(active_drift) > 1:
                raise ValueError("structure-drift-multiple-active-identities")
            if active_drift and (
                int(active_drift[0][1]) != snapshot_id
                or str(active_drift[0][2]) != publication_id
            ):
                ensure_deadline()
                superseded = con.execute(
                    "UPDATE structure_generation_drift_progress SET phase='stale',"
                    "terminal_reason='drift-current-generation-superseded',"
                    "checkpoint_at_ms=? WHERE comparison_id=? AND phase NOT IN "
                    "('sealed','stale')",
                    (now_ms, str(active_drift[0][0])),
                )
                if superseded.rowcount != 1:
                    raise StructurePublicationCursorError(
                        "structure-drift-cursor-mismatch"
                    )
            ensure_deadline()
            con.execute(
                "INSERT INTO current_structure_generation(id,snapshot_id,publication_id,"
                "validation_hash,counts_json,certification_component,"
                "comparison_receipt_digest,switched_at_ms) "
                "VALUES (1,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET "
                "snapshot_id=excluded.snapshot_id,publication_id=excluded.publication_id,"
                "validation_hash=excluded.validation_hash,"
                "counts_json=excluded.counts_json,"
                "certification_component=excluded.certification_component,"
                "comparison_receipt_digest=excluded.comparison_receipt_digest,"
                "switched_at_ms=excluded.switched_at_ms",
                (
                    snapshot_id,
                    publication_id,
                    publication[5],
                    counts_json,
                    publication[6],
                    receipt_digest,
                    now_ms,
                ),
            )
            ensure_deadline()
            con.execute(
                "UPDATE structure_publications SET status='published',published_at_ms=?,"
                "checkpoint_at_ms=? WHERE publication_id=?",
                (now_ms, now_ms, publication_id),
            )
            ensure_deadline()
            window_update = con.execute(
                "UPDATE structure_sync_windows SET status='published',"
                "published_snapshot_id=?,checkpoint_at_ms=? WHERE id=? AND status='complete'",
                (snapshot_id, now_ms, window_id),
            )
            if window_update.rowcount != 1:
                raise ValueError("structure-sync-window-not-complete")
            ensure_deadline()
            con.execute("COMMIT")
            return snapshot_id
        except BaseException as error:
            con.set_progress_handler(None, 0)
            if con.in_transaction:
                con.execute("ROLLBACK")
            if (
                isinstance(error, sqlite3.OperationalError)
                and "interrupted" in str(error).lower()
                and monotonic() >= deadline
            ):
                raise StructurePointerSwitchDeadlineError(
                    "pointer-switch-deadline"
                ) from error
            raise
        finally:
            con.set_progress_handler(None, 0)
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

    def structure_generation_status(
        self,
        *,
        retain_generations: int = 2,
        pressure_probe_limit: int = 8,
        trace_callback: Callable[[str], None] | None = None,
        sqlite_progress_callback: Callable[[], int] | None = None,
        sqlite_connection_callback: Callable[[sqlite3.Connection], None] | None = None,
    ) -> dict[str, object]:
        """Return bounded read-only rollout and evidence pressure metadata."""
        if retain_generations < 2:
            raise ValueError("retain_generations must preserve current and rollback")
        if pressure_probe_limit < retain_generations:
            raise ValueError("pressure_probe_limit must cover retention floor")
        with sqlite3.connect(
            f"file:{self._db_path}?mode=ro",
            uri=True,
            timeout=0.25 if sqlite_connection_callback is not None else 5.0,
        ) as con:
            con.execute("PRAGMA query_only=ON")
            if sqlite_connection_callback is not None:
                sqlite_connection_callback(con)
            con.execute("BEGIN")
            if trace_callback is not None:
                con.set_trace_callback(trace_callback)
            if sqlite_progress_callback is not None:
                con.set_progress_handler(sqlite_progress_callback, 1)
            pointer = con.execute(
                "SELECT g.snapshot_id,g.publication_id,g.validation_hash,g.counts_json,"
                "g.certification_component,g.comparison_receipt_digest,g.switched_at_ms,"
                "p.validation_hash,p.certification_hash,p.committed_counts_json,"
                "p.certification_component FROM current_structure_generation g "
                "LEFT JOIN structure_publications p ON p.publication_id=g.publication_id "
                "AND p.snapshot_id=g.snapshot_id WHERE g.id=1"
            ).fetchone()
            publication = con.execute(
                "SELECT publication_id,snapshot_id,status,normalization_component,"
                "normalization_source_cursor,write_component,write_row_cursor,"
                "certification_component,certification_row_cursor,checkpoint_at_ms,"
                "committed_counts_json FROM structure_publications p "
                "WHERE status IN ('normalizing','writing','ready') "
                "ORDER BY checkpoint_at_ms DESC,publication_id DESC LIMIT 1"
            ).fetchone()
            if publication is None:
                publication = con.execute(
                    "SELECT p.publication_id,p.snapshot_id,p.status,"
                    "p.normalization_component,p.normalization_source_cursor,"
                    "p.write_component,p.write_row_cursor,p.certification_component,"
                    "p.certification_row_cursor,p.checkpoint_at_ms,p.committed_counts_json "
                    "FROM "
                    "current_structure_generation g JOIN structure_publications p "
                    "ON p.snapshot_id=g.snapshot_id AND "
                    "p.publication_id=g.publication_id WHERE g.id=1"
                ).fetchone()
            comparison = con.execute(
                "SELECT cp.generation_snapshot_id,cp.phase,cp.row_cursor_json,"
                "cp.checkpoint_at_ms,NULL FROM structure_generation_comparison_progress cp "
                "WHERE cp.phase!='sealed' AND cp.checkpoint_at_ms>=0 "
                "ORDER BY cp.checkpoint_at_ms DESC,"
                "cp.publication_id DESC LIMIT 1"
            ).fetchone()
            if comparison is None:
                comparison = con.execute(
                    "SELECT g.snapshot_id,cp.phase,cp.row_cursor_json,"
                    "cp.checkpoint_at_ms,cr.receipt_digest FROM "
                    "current_structure_generation g LEFT JOIN "
                    "structure_generation_comparison_progress cp "
                    "ON cp.generation_snapshot_id=g.snapshot_id LEFT JOIN "
                    "structure_generation_comparison_receipts cr "
                    "ON cr.generation_snapshot_id=g.snapshot_id WHERE g.id=1"
                ).fetchone()
            active_cleanup = con.execute(
                "SELECT generation_snapshot_id,phase,rows_deleted,checkpoint_at_ms,"
                "blocked_reason FROM structure_generation_cleanup_progress LIMIT 1"
            ).fetchone()
            cleanup_runtime = con.execute(
                "SELECT state,consecutive_failures,last_attempt_at_ms,"
                "last_success_at_ms,next_attempt_at_ms,generation_snapshot_id,phase,"
                "rows_deleted,error_kind,checkpoint_at_ms FROM "
                "structure_generation_cleanup_runtime WHERE id=1"
            ).fetchone()
            bootstrap = con.execute(
                "SELECT progress.window_id,progress.event_cursor,"
                "progress.member_offset,progress.events_processed,"
                "progress.relationships_processed,progress.checkpoint_at_ms,"
                "progress.completed_at_ms,progress.blocked_reason FROM "
                "structure_sync_event_market_backfill_progress progress "
                "INDEXED BY idx_structure_event_market_backfill_active JOIN "
                "structure_sync_windows window ON window.id=progress.window_id "
                "WHERE progress.completed_at_ms IS NULL "
                "AND progress.checkpoint_at_ms>=0 AND window.status='complete' "
                "ORDER BY progress.checkpoint_at_ms DESC,progress.window_id DESC LIMIT 1"
            ).fetchone()
            bootstrap_rotation = con.execute(
                "SELECT observation.recovery_root_window_id,observation.old_window_id,"
                "observation.event_cursor,"
                "observation.member_offset,observation.blocked_reason,"
                "observation.checkpoint_at_ms,observation.successor_window_id,"
                "observation.rotated_at_ms,observation.observation_digest FROM "
                "structure_bootstrap_rotation_observations observation "
                "INDEXED BY idx_structure_bootstrap_rotation_latest "
                "WHERE observation.rotated_at_ms>=0 ORDER BY "
                "observation.rotated_at_ms DESC,observation.observation_id DESC LIMIT 1"
            ).fetchone()
            bootstrap_recovery = None
            if bootstrap_rotation is not None and isinstance(
                bootstrap_rotation[0], str
            ):
                bootstrap_recovery = con.execute(
                    "SELECT successful_window_id,window_checkpoint_at_ms,"
                    "completed_at_ms,receipt_digest FROM "
                    "structure_bootstrap_recovery_receipts "
                    "WHERE recovery_root_window_id=?",
                    (bootstrap_rotation[0],),
                ).fetchone()
            generation_probe = con.execute(
                "SELECT p.snapshot_id FROM structure_publications p WHERE "
                "p.status='published' AND p.published_at_ms>=0 AND NOT EXISTS (SELECT 1 FROM "
                "structure_generation_cleanup_receipts r WHERE "
                "r.generation_snapshot_id=p.snapshot_id) ORDER BY "
                "p.published_at_ms DESC,p.snapshot_id DESC LIMIT ?",
                (pressure_probe_limit + 1,),
            ).fetchall()
            cleanup_observation = con.execute(
                "SELECT generation_snapshot_id,publication_id,state,reason,"
                "observed_at_ms,observation_digest FROM "
                "structure_generation_cleanup_observations ORDER BY id DESC LIMIT 1"
            ).fetchone()
            pointer_receipt = None
            pointer_repair_progress = None
            if pointer is not None:
                pointer_receipt = con.execute(
                    "SELECT publication_id,legacy_snapshot_id,legacy_market_count,"
                    "generation_market_count,legacy_universe_hash,"
                    "generation_universe_hash,legacy_source_truth_hash,"
                    "generation_source_truth_hash,generation_validation_hash,"
                    "created_at_ms,receipt_digest FROM "
                    "structure_generation_comparison_receipts WHERE "
                    "generation_snapshot_id=?",
                    (int(pointer[0]),),
                ).fetchone()
                pointer_repair_progress = con.execute(
                    "SELECT phase,row_cursor_json,digest_state_json,phase_row_count,"
                    "checkpoint_at_ms,legacy_universe_hash,generation_universe_hash,"
                    "legacy_source_truth_hash,legacy_snapshot_id,legacy_taken_at_ms,"
                    "legacy_finished_at_ms,legacy_market_count FROM "
                    "structure_generation_comparison_progress WHERE "
                    "generation_snapshot_id=? AND publication_id=? AND phase!='sealed'",
                    (int(pointer[0]), str(pointer[1])),
                ).fetchone()
            try:
                current_legacy_identity = self._comparison_legacy_identity(con)
            except (TypeError, ValueError):
                current_legacy_identity = None
        count_agrees = hash_agrees = False
        comparison_authenticated = pointer is None
        comparison_recoverable_missing_receipt = False
        comparison_repair_checkpoint_at_ms = None
        comparison_mismatch_reasons: list[str] = []
        pointer_snapshot_id = pointer_publication_id = switched_at_ms = None
        if pointer is not None:
            pointer_snapshot_id = int(pointer[0])
            pointer_publication_id = str(pointer[1])
            switched_at_ms = int(pointer[6])
            count_agrees = pointer[3] == pointer[9]
            hash_agrees = pointer[2] == pointer[7] == pointer[8]
            count_agrees = count_agrees and pointer[4] == pointer[10]
            if pointer_receipt is None:
                comparison_mismatch_reasons.append("comparison-receipt-missing")
                comparison_recoverable_missing_receipt = bool(
                    pointer[5] is None
                    and count_agrees
                    and hash_agrees
                    and pointer_repair_progress is not None
                    and _structure_comparison_progress_is_resumable(
                        pointer_repair_progress,
                        current_legacy_identity,
                    )
                )
                if comparison_recoverable_missing_receipt:
                    comparison_repair_checkpoint_at_ms = int(pointer_repair_progress[4])
            else:
                expected_digest = _comparison_receipt_digest(
                    generation_snapshot_id=pointer_snapshot_id,
                    publication_id=str(pointer_receipt[0]),
                    legacy_snapshot_id=int(pointer_receipt[1]),
                    legacy_market_count=int(pointer_receipt[2]),
                    generation_market_count=int(pointer_receipt[3]),
                    legacy_universe_hash=str(pointer_receipt[4]),
                    generation_universe_hash=str(pointer_receipt[5]),
                    legacy_source_truth_hash=str(pointer_receipt[6]),
                    generation_source_truth_hash=str(pointer_receipt[7]),
                    generation_validation_hash=str(pointer_receipt[8]),
                    created_at_ms=int(pointer_receipt[9]),
                )
                if (
                    pointer_receipt[10] != expected_digest
                    or pointer[5] != expected_digest
                ):
                    comparison_mismatch_reasons.append(
                        "comparison-receipt-digest-mismatch"
                    )
                if (
                    pointer_receipt[0] != pointer_publication_id
                    or pointer_receipt[8] != pointer[2]
                ):
                    comparison_mismatch_reasons.append(
                        "comparison-receipt-identity-mismatch"
                    )
            comparison_authenticated = not comparison_mismatch_reasons
        probe_exact = len(generation_probe) <= pressure_probe_limit
        retained_lower_bound = min(len(generation_probe), pressure_probe_limit + 1)
        retention_floor_ids = [
            int(row[0]) for row in generation_probe[:retain_generations]
        ]
        cleanup_blocked_reason = None
        if cleanup_observation is not None:
            observation_digest = _generation_cleanup_observation_digest(
                generation_snapshot_id=int(cleanup_observation[0]),
                publication_id=str(cleanup_observation[1]),
                state=str(cleanup_observation[2]),
                reason=(
                    None
                    if cleanup_observation[3] is None
                    else str(cleanup_observation[3])
                ),
                observed_at_ms=int(cleanup_observation[4]),
            )
            if cleanup_observation[5] != observation_digest:
                cleanup_blocked_reason = "cleanup-observation-digest-mismatch"
            elif cleanup_observation[2] == "blocked":
                cleanup_blocked_reason = str(cleanup_observation[3])
        rotation_status = None
        if bootstrap_rotation is not None:
            valid_types = (
                all(
                    isinstance(bootstrap_rotation[index], str)
                    for index in (0, 1, 2, 4, 6, 8)
                )
                and all(
                    type(bootstrap_rotation[index]) is int
                    and int(bootstrap_rotation[index]) >= 0
                    for index in (3, 5, 7)
                )
                and all(str(bootstrap_rotation[index]) for index in (0, 1, 4, 6))
            )
            expected_rotation_digest = None
            if valid_types:
                expected_rotation_digest = _bootstrap_rotation_digest(
                    recovery_root_window_id=str(bootstrap_rotation[0]),
                    old_window_id=str(bootstrap_rotation[1]),
                    event_cursor=str(bootstrap_rotation[2]),
                    member_offset=int(bootstrap_rotation[3]),
                    blocked_reason=str(bootstrap_rotation[4]),
                    checkpoint_at_ms=int(bootstrap_rotation[5]),
                    successor_window_id=str(bootstrap_rotation[6]),
                    rotated_at_ms=int(bootstrap_rotation[7]),
                )
            authenticated = bool(
                expected_rotation_digest is not None
                and bootstrap_rotation[8] == expected_rotation_digest
            )
            recovery_authenticated = False
            if (
                authenticated
                and bootstrap_recovery is not None
                and isinstance(bootstrap_recovery[0], str)
                and bool(bootstrap_recovery[0])
                and type(bootstrap_recovery[1]) is int
                and int(bootstrap_recovery[1]) >= 0
                and type(bootstrap_recovery[2]) is int
                and int(bootstrap_recovery[2]) >= 0
                and isinstance(bootstrap_recovery[3], str)
            ):
                expected_recovery_digest = _bootstrap_recovery_digest(
                    recovery_root_window_id=str(bootstrap_rotation[0]),
                    successful_window_id=str(bootstrap_recovery[0]),
                    window_checkpoint_at_ms=int(bootstrap_recovery[1]),
                    completed_at_ms=int(bootstrap_recovery[2]),
                )
                recovery_authenticated = (
                    bootstrap_recovery[3] == expected_recovery_digest
                )
            recovered = authenticated and recovery_authenticated
            rotation_status = {
                "recovery_root_window_id": str(bootstrap_rotation[0]),
                "old_window_id": str(bootstrap_rotation[1]),
                "event_cursor": str(bootstrap_rotation[2]),
                "member_offset": (
                    int(bootstrap_rotation[3])
                    if type(bootstrap_rotation[3]) is int
                    else 0
                ),
                "blocked_reason": str(bootstrap_rotation[4]),
                "checkpoint_at_ms": (
                    int(bootstrap_rotation[5])
                    if type(bootstrap_rotation[5]) is int
                    else 0
                ),
                "successor_window_id": str(bootstrap_rotation[6]),
                "rotated_at_ms": (
                    int(bootstrap_rotation[7])
                    if type(bootstrap_rotation[7]) is int
                    else 0
                ),
                "observation_digest": str(bootstrap_rotation[8]),
                "authenticated": authenticated,
                "recovery_receipt_authenticated": recovery_authenticated,
                "recovered": recovered,
            }
            if not recovered:
                bootstrap = (
                    bootstrap_rotation[1],
                    bootstrap_rotation[2],
                    rotation_status["member_offset"],
                    0,
                    0,
                    rotation_status["checkpoint_at_ms"],
                    None,
                    (
                        "bootstrap-rotation-evidence-invalid"
                        if not authenticated
                        else "bootstrap-recovery-receipt-invalid"
                        if bootstrap_recovery is not None
                        else str(bootstrap_rotation[4])
                    ),
                )
        return {
            "pointer_snapshot_id": pointer_snapshot_id,
            "pointer_publication_id": pointer_publication_id,
            "pointer_switched_at_ms": switched_at_ms,
            "generation_count_agrees": count_agrees,
            "generation_hash_agrees": hash_agrees,
            "comparison_authenticated": comparison_authenticated,
            "comparison_recoverable_missing_receipt": (
                comparison_recoverable_missing_receipt
            ),
            "comparison_repair_checkpoint_at_ms": comparison_repair_checkpoint_at_ms,
            "comparison_mismatch_reasons": comparison_mismatch_reasons,
            "publication": None
            if publication is None
            else {
                "publication_id": str(publication[0]),
                "snapshot_id": int(publication[1]),
                "status": str(publication[2]),
                "normalization_component": publication[3],
                "normalization_cursor": publication[4],
                "write_component": publication[5],
                "write_cursor": publication[6],
                "certification_component": publication[7],
                "certification_cursor": publication[8],
                "checkpoint_at_ms": int(publication[9]),
                "quarantine_count": (
                    int(json.loads(str(publication[10])).get("issues", 0))
                    if str(publication[2]) in {"ready", "published"}
                    else 0
                ),
            },
            "bootstrap": None
            if bootstrap is None
            else {
                "window_id": str(bootstrap[0]),
                "event_cursor": str(bootstrap[1]),
                "member_offset": int(bootstrap[2]),
                "events_processed": int(bootstrap[3]),
                "relationships_processed": int(bootstrap[4]),
                "checkpoint_at_ms": int(bootstrap[5]),
                "completed_at_ms": bootstrap[6],
                "blocked_reason": bootstrap[7],
                **(
                    {
                        "successor_window_id": str(bootstrap_rotation[6]),
                        "recovery_state": "rotated",
                    }
                    if bootstrap_rotation is not None
                    and not rotation_status["recovered"]
                    and str(bootstrap[0]) == str(bootstrap_rotation[1])
                    else {}
                ),
            },
            "bootstrap_rotation": rotation_status,
            "comparison": None
            if comparison is None
            else {
                "generation_snapshot_id": comparison[0],
                "phase": comparison[1],
                "cursor": comparison[2],
                "checkpoint_at_ms": comparison[3],
                "receipt_present": comparison[4] is not None,
            },
            "cleanup": None
            if active_cleanup is None
            else {
                "generation_snapshot_id": int(active_cleanup[0]),
                "phase": str(active_cleanup[1]),
                "rows_deleted": int(active_cleanup[2]),
                "checkpoint_at_ms": int(active_cleanup[3]),
                "blocked_reason": active_cleanup[4],
            },
            "cleanup_blocked_reason": cleanup_blocked_reason,
            "cleanup_runtime": (
                None
                if cleanup_runtime is None
                else self._structure_generation_cleanup_runtime_from_row(
                    tuple(cleanup_runtime)
                )
            ),
            "retained_generation_count_lower_bound": retained_lower_bound,
            "retained_generation_count_is_exact": probe_exact,
            "retention_floor_generation_ids": retention_floor_ids,
            "reclaimable_generation_count_lower_bound": max(
                0, retained_lower_bound - retain_generations
            ),
            "retention_floor": retain_generations,
        }

    @staticmethod
    def _structure_generation_cleanup_runtime_from_row(
        row: tuple[object, ...],
    ) -> dict[str, object]:
        return {
            "state": str(row[0]),
            "consecutive_failures": int(row[1]),
            "last_attempt_at_ms": None if row[2] is None else int(row[2]),
            "last_success_at_ms": None if row[3] is None else int(row[3]),
            "next_attempt_at_ms": int(row[4]),
            "generation_snapshot_id": None if row[5] is None else int(row[5]),
            "phase": None if row[6] is None else str(row[6]),
            "rows_deleted": int(row[7]),
            "error_kind": None if row[8] is None else str(row[8]),
            "checkpoint_at_ms": int(row[9]),
        }

    def structure_generation_cleanup_runtime_status(self) -> dict[str, object]:
        """Read restart-persistent operational truth for resident cleanup."""
        with sqlite3.connect(f"file:{self._db_path}?mode=ro", uri=True) as con:
            row = con.execute(
                "SELECT state,consecutive_failures,last_attempt_at_ms,"
                "last_success_at_ms,next_attempt_at_ms,generation_snapshot_id,phase,"
                "rows_deleted,error_kind,checkpoint_at_ms FROM "
                "structure_generation_cleanup_runtime WHERE id=1"
            ).fetchone()
        if row is None:
            raise ValueError("structure-generation-cleanup-runtime-missing")
        return self._structure_generation_cleanup_runtime_from_row(tuple(row))

    @staticmethod
    def _capacity_controller_runtime_from_row(
        row: tuple[object, ...],
    ) -> dict[str, object]:
        return {
            "state": str(row[0]),
            "state_started_at_ms": int(row[1]),
            "free_bytes": None if row[2] is None else int(row[2]),
            "free_percent": None if row[3] is None else float(row[3]),
            "last_measurement_at_ms": int(row[4]),
            "last_action": str(row[5]),
            "consecutive_failures": int(row[6]),
            "next_attempt_at_ms": int(row[7]),
            "last_error_kind": None if row[8] is None else str(row[8]),
            "last_recovery_receipt_at_ms": (
                None if row[9] is None else int(row[9])
            ),
        }

    def capacity_controller_runtime_status(self) -> dict[str, object]:
        """Read restart-persistent capacity episode state without mutating it."""
        with sqlite3.connect(f"file:{self._db_path}?mode=ro", uri=True) as con:
            row = con.execute(
                "SELECT state,state_started_at_ms,free_bytes,free_percent,"
                "last_measurement_at_ms,last_action,consecutive_failures,"
                "next_attempt_at_ms,last_error_kind,last_recovery_receipt_at_ms "
                "FROM capacity_controller_runtime WHERE id=1"
            ).fetchone()
        if row is None:
            raise ValueError("capacity-controller-runtime-missing")
        return self._capacity_controller_runtime_from_row(tuple(row))

    def record_capacity_controller_measurement(
        self,
        *,
        state: Literal["normal", "pressure", "critical", "exhaustion-imminent"],
        free_bytes: int,
        free_percent: float,
        observed_at_ms: int,
    ) -> dict[str, object]:
        """Persist one measured watermarked state, preserving episode start time."""
        if state not in {"normal", "pressure", "critical", "exhaustion-imminent"}:
            raise ValueError("invalid-capacity-controller-state")
        if type(free_bytes) is not int or free_bytes < 0:
            raise ValueError("invalid-capacity-controller-free-bytes")
        if (
            isinstance(free_percent, bool)
            or not isinstance(free_percent, (int, float))
            or not 0.0 <= float(free_percent) <= 100.0
        ):
            raise ValueError("invalid-capacity-controller-free-percent")
        if type(observed_at_ms) is not int or observed_at_ms < 0:
            raise ValueError("invalid-capacity-controller-observed-at")
        con = self._connect_writer()
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                "SELECT state,state_started_at_ms FROM "
                "capacity_controller_runtime WHERE id=1"
            ).fetchone()
            if row is None:
                raise ValueError("capacity-controller-runtime-missing")
            state_started_at_ms = (
                observed_at_ms if str(row[0]) != state else int(row[1])
            )
            con.execute(
                "UPDATE capacity_controller_runtime SET state=?,state_started_at_ms=?,"
                "free_bytes=?,free_percent=?,last_measurement_at_ms=?,"
                "last_action='measured',last_error_kind=NULL WHERE id=1",
                (
                    state,
                    state_started_at_ms,
                    free_bytes,
                    float(free_percent),
                    observed_at_ms,
                ),
            )
            con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()
        return self.capacity_controller_runtime_status()

    def defer_capacity_controller_attempt(
        self,
        *,
        action: str,
        now_ms: int,
        next_attempt_at_ms: int,
    ) -> dict[str, object]:
        """Persist a benign, retryable deferral such as Quote priority."""
        if not 1 <= len(action) <= 64:
            raise ValueError("invalid-capacity-controller-action")
        if (
            type(now_ms) is not int
            or now_ms < 0
            or type(next_attempt_at_ms) is not int
            or next_attempt_at_ms < now_ms
        ):
            raise ValueError("invalid-capacity-controller-attempt-time")
        con = self._connect_writer()
        try:
            con.execute("BEGIN IMMEDIATE")
            con.execute(
                "UPDATE capacity_controller_runtime SET last_action=?,"
                "next_attempt_at_ms=?,last_error_kind=NULL WHERE id=1",
                (action, next_attempt_at_ms),
            )
            con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()
        return self.capacity_controller_runtime_status()

    def record_capacity_controller_reclaim(
        self,
        *,
        action: str,
        deleted_count: int,
        deleted_ids: list[int],
        completed_at_ms: int,
    ) -> dict[str, object]:
        """Append a successful bounded reclaim receipt and retain its recovery fact."""
        if not 1 <= len(action) <= 64:
            raise ValueError("invalid-capacity-controller-action")
        if type(deleted_count) is not int or deleted_count < 0:
            raise ValueError("invalid-capacity-controller-deleted-count")
        if (
            len(deleted_ids) != deleted_count
            or any(type(item) is not int or item <= 0 for item in deleted_ids)
            or len(set(deleted_ids)) != len(deleted_ids)
        ):
            raise ValueError("invalid-capacity-controller-deleted-ids")
        if type(completed_at_ms) is not int or completed_at_ms < 0:
            raise ValueError("invalid-capacity-controller-completed-at")
        con = self._connect_writer()
        try:
            con.execute("BEGIN IMMEDIATE")
            con.execute(
                "INSERT INTO capacity_reclaim_receipts("
                "action,deleted_count,deleted_ids_json,completed_at_ms"
                ") VALUES (?,?,?,?)",
                (action, deleted_count, json.dumps(deleted_ids), completed_at_ms),
            )
            con.execute(
                "UPDATE capacity_controller_runtime SET last_action=?,"
                "consecutive_failures=0,next_attempt_at_ms=0,last_error_kind=NULL,"
                "last_recovery_receipt_at_ms=CASE WHEN ?>0 THEN ? "
                "ELSE last_recovery_receipt_at_ms END WHERE id=1",
                (action, deleted_count, completed_at_ms),
            )
            con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()
        return self.capacity_controller_runtime_status()

    def record_capacity_controller_failure(
        self,
        *,
        error_kind: str,
        now_ms: int,
        next_attempt_at_ms: int,
    ) -> dict[str, object]:
        """Keep a failed reclaim diagnosable and due for bounded retry."""
        if not 1 <= len(error_kind) <= 64:
            raise ValueError("invalid-capacity-controller-error-kind")
        if (
            type(now_ms) is not int
            or now_ms < 0
            or type(next_attempt_at_ms) is not int
            or next_attempt_at_ms < now_ms
        ):
            raise ValueError("invalid-capacity-controller-attempt-time")
        con = self._connect_writer()
        try:
            con.execute("BEGIN IMMEDIATE")
            con.execute(
                "UPDATE capacity_controller_runtime SET last_action='reclaim-failed',"
                "consecutive_failures=consecutive_failures+1,next_attempt_at_ms=?,"
                "last_error_kind=? WHERE id=1",
                (next_attempt_at_ms, error_kind),
            )
            con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()
        return self.capacity_controller_runtime_status()

    def recover_structure_generation_cleanup_runtime(
        self,
        *,
        now_ms: int,
        retry_delay_ms: int,
    ) -> dict[str, object]:
        """Turn an orphaned running owner into a bounded restart retry."""
        if type(now_ms) is not int or now_ms < 0:
            raise ValueError("invalid-cleanup-runtime-time")
        if type(retry_delay_ms) is not int or retry_delay_ms < 1:
            raise ValueError("invalid-cleanup-runtime-retry-delay")
        con = self._connect_writer()
        try:
            con.execute("BEGIN IMMEDIATE")
            con.execute(
                "UPDATE structure_generation_cleanup_runtime SET state='backoff',"
                "consecutive_failures=consecutive_failures+1,next_attempt_at_ms=?,"
                "rows_deleted=0,error_kind='worker-restarted',checkpoint_at_ms=? "
                "WHERE id=1 AND state='running'",
                (now_ms + retry_delay_ms, now_ms),
            )
            con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()
        return self.structure_generation_cleanup_runtime_status()

    def begin_structure_generation_cleanup_attempt(self, *, now_ms: int) -> bool:
        """Atomically admit one due cleanup owner across daemon instances."""
        if type(now_ms) is not int or now_ms < 0:
            raise ValueError("invalid-cleanup-runtime-time")
        con = self._connect_writer()
        try:
            con.execute("BEGIN IMMEDIATE")
            changed = con.execute(
                "UPDATE structure_generation_cleanup_runtime SET state='running',"
                "last_attempt_at_ms=?,rows_deleted=0,error_kind=NULL,checkpoint_at_ms=? "
                "WHERE id=1 AND state!='running' AND next_attempt_at_ms<=?",
                (now_ms, now_ms, now_ms),
            )
            con.execute("COMMIT")
            return changed.rowcount == 1
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def defer_structure_generation_cleanup_runtime(
        self,
        *,
        now_ms: int,
        next_attempt_at_ms: int,
        error_kind: str,
    ) -> dict[str, object]:
        """Persist a non-failure admission defer without stealing an owner."""
        if (
            type(now_ms) is not int
            or now_ms < 0
            or type(next_attempt_at_ms) is not int
            or next_attempt_at_ms < now_ms
            or not 1 <= len(error_kind) <= 64
        ):
            raise ValueError("invalid-cleanup-runtime-defer")
        con = self._connect_writer()
        try:
            con.execute("BEGIN IMMEDIATE")
            con.execute(
                "UPDATE structure_generation_cleanup_runtime SET state='backoff',"
                "next_attempt_at_ms=?,rows_deleted=0,error_kind=?,checkpoint_at_ms=? "
                "WHERE id=1 AND state IN ('idle','backoff')",
                (next_attempt_at_ms, error_kind, now_ms),
            )
            con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()
        return self.structure_generation_cleanup_runtime_status()

    def finish_structure_generation_cleanup_attempt(
        self,
        *,
        state: Literal["idle", "backoff", "blocked"],
        now_ms: int,
        next_attempt_at_ms: int,
        generation_snapshot_id: int | None,
        phase: str | None,
        rows_deleted: int,
        error_kind: str | None,
        increment_failure: bool,
    ) -> dict[str, object]:
        """Terminalize the current cleanup owner with bounded runtime evidence."""
        if state not in {"idle", "backoff", "blocked"}:
            raise ValueError("invalid-cleanup-runtime-state")
        if (
            type(now_ms) is not int
            or now_ms < 0
            or type(next_attempt_at_ms) is not int
            or next_attempt_at_ms < now_ms
            or type(rows_deleted) is not int
            or rows_deleted < 0
            or type(increment_failure) is not bool
        ):
            raise ValueError("invalid-cleanup-runtime-terminal-evidence")
        if generation_snapshot_id is not None and (
            type(generation_snapshot_id) is not int or generation_snapshot_id < 1
        ):
            raise ValueError("invalid-cleanup-runtime-generation")
        allowed_phases = {
            "events",
            "event_tags",
            "memberships",
            "group_truth",
            "markets",
            "issues",
            "complete",
        }
        if phase is not None and phase not in allowed_phases:
            raise ValueError("invalid-cleanup-runtime-phase")
        if error_kind is not None and not 1 <= len(error_kind) <= 64:
            raise ValueError("invalid-cleanup-runtime-error-kind")
        con = self._connect_writer()
        try:
            con.execute("BEGIN IMMEDIATE")
            changed = con.execute(
                "UPDATE structure_generation_cleanup_runtime SET state=?,"
                "consecutive_failures=CASE WHEN ? THEN consecutive_failures+1 "
                "WHEN ?='idle' THEN 0 ELSE consecutive_failures END,"
                "last_success_at_ms=CASE WHEN ?='idle' THEN ? "
                "ELSE last_success_at_ms END,next_attempt_at_ms=?,"
                "generation_snapshot_id=?,phase=?,rows_deleted=?,error_kind=?,"
                "checkpoint_at_ms=? WHERE id=1 AND state='running'",
                (
                    state,
                    increment_failure,
                    state,
                    state,
                    now_ms,
                    next_attempt_at_ms,
                    generation_snapshot_id,
                    phase,
                    rows_deleted,
                    error_kind,
                    now_ms,
                ),
            )
            if changed.rowcount != 1:
                raise ValueError("structure-generation-cleanup-runtime-not-running")
            con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()
        return self.structure_generation_cleanup_runtime_status()

    def structure_generation_query_plans(
        self,
        *,
        retain_generations: int,
        pressure_probe_limit: int,
    ) -> dict[str, tuple[str, ...]]:
        """Expose stable SQLite planner evidence for bounded operator queries."""
        queries = {
            "active_bootstrap": (
                "SELECT progress.window_id,progress.event_cursor,"
                "progress.member_offset,progress.events_processed,"
                "progress.relationships_processed,progress.checkpoint_at_ms,"
                "progress.completed_at_ms,progress.blocked_reason FROM "
                "structure_sync_event_market_backfill_progress progress "
                "INDEXED BY idx_structure_event_market_backfill_active JOIN "
                "structure_sync_windows window ON window.id=progress.window_id "
                "WHERE progress.completed_at_ms IS NULL "
                "AND progress.checkpoint_at_ms>=0 AND window.status='complete' "
                "ORDER BY progress.checkpoint_at_ms DESC,progress.window_id DESC LIMIT 1",
                (),
            ),
            "pointer_repair": (
                "SELECT phase,row_cursor_json,digest_state_json,phase_row_count,"
                "checkpoint_at_ms,legacy_universe_hash,generation_universe_hash,"
                "legacy_source_truth_hash,legacy_snapshot_id,legacy_taken_at_ms,"
                "legacy_finished_at_ms,legacy_market_count FROM "
                "structure_generation_comparison_progress WHERE "
                "generation_snapshot_id=? AND publication_id=? AND phase!='sealed'",
                (-1, "planner-probe"),
            ),
            "active_comparison": (
                "SELECT generation_snapshot_id,phase,row_cursor_json,checkpoint_at_ms "
                "FROM structure_generation_comparison_progress WHERE phase!='sealed' "
                "AND checkpoint_at_ms>=0 "
                "ORDER BY checkpoint_at_ms DESC,publication_id DESC LIMIT 1",
                (),
            ),
            "pressure": (
                "SELECT p.snapshot_id FROM structure_publications p WHERE "
                "p.status='published' AND p.published_at_ms>=0 AND NOT EXISTS (SELECT 1 FROM "
                "structure_generation_cleanup_receipts r WHERE "
                "r.generation_snapshot_id=p.snapshot_id) ORDER BY "
                "p.published_at_ms DESC,p.snapshot_id DESC LIMIT ?",
                (pressure_probe_limit + 1,),
            ),
            "retention_floor": (
                "SELECT p.snapshot_id,p.publication_id FROM structure_publications p "
                "WHERE p.status='published' AND p.published_at_ms>=0 AND NOT EXISTS (SELECT 1 FROM "
                "structure_generation_cleanup_receipts r WHERE "
                "r.generation_snapshot_id=p.snapshot_id) ORDER BY "
                "p.published_at_ms DESC,p.snapshot_id DESC LIMIT ?",
                (retain_generations,),
            ),
            "oldest_candidate": (
                "SELECT p.snapshot_id,p.publication_id FROM structure_publications p "
                "WHERE p.status='published' AND p.published_at_ms>=0 AND NOT EXISTS (SELECT 1 FROM "
                "structure_generation_cleanup_receipts r WHERE "
                "r.generation_snapshot_id=p.snapshot_id) AND p.snapshot_id NOT IN "
                "(SELECT p2.snapshot_id FROM structure_publications p2 WHERE "
                "p2.status='published' AND p2.published_at_ms>=0 AND NOT EXISTS (SELECT 1 FROM "
                "structure_generation_cleanup_receipts r2 WHERE "
                "r2.generation_snapshot_id=p2.snapshot_id) ORDER BY "
                "p2.published_at_ms DESC,p2.snapshot_id DESC LIMIT ?) ORDER BY "
                "p.published_at_ms,p.snapshot_id LIMIT 1",
                (retain_generations,),
            ),
        }
        with sqlite3.connect(f"file:{self._db_path}?mode=ro", uri=True) as con:
            con.execute("PRAGMA query_only=ON")
            return {
                name: tuple(
                    str(row[3])
                    for row in con.execute(f"EXPLAIN QUERY PLAN {sql}", params)
                )
                for name, (sql, params) in queries.items()
            }

    def cleanup_structure_generation_evidence(
        self,
        *,
        retain_generations: int = 2,
        max_rows: int,
        now_ms: int,
    ) -> dict[str, object]:
        """Advance one durable, bounded phase of old generation reclamation."""
        if retain_generations < 2:
            raise ValueError("retain_generations must preserve current and rollback")
        if max_rows < 1:
            raise ValueError("max_rows must be positive")
        if type(now_ms) is not int or now_ms < 0:
            raise ValueError("now_ms must be a non-negative integer")
        con = self._connect_writer()
        try:
            con.execute("BEGIN IMMEDIATE")
            active = con.execute(
                "SELECT generation_snapshot_id,publication_id,phase,rows_deleted "
                "FROM structure_generation_cleanup_progress LIMIT 1"
            ).fetchone()
            retained = con.execute(
                "SELECT p.snapshot_id,p.publication_id FROM structure_publications p "
                "WHERE p.status='published' AND p.published_at_ms>=0 AND NOT EXISTS (SELECT 1 FROM "
                "structure_generation_cleanup_receipts r WHERE "
                "r.generation_snapshot_id=p.snapshot_id) "
                "ORDER BY p.published_at_ms DESC,p.snapshot_id DESC LIMIT ?",
                (retain_generations,),
            ).fetchall()
            retained_ids = [int(row[0]) for row in retained]
            if active is None:
                candidate = con.execute(
                    "SELECT p.snapshot_id,p.publication_id FROM structure_publications p "
                    "WHERE p.status='published' AND p.published_at_ms>=0 AND "
                    "NOT EXISTS (SELECT 1 FROM "
                    "structure_generation_cleanup_receipts r WHERE "
                    "r.generation_snapshot_id=p.snapshot_id) "
                    "AND p.snapshot_id NOT IN (SELECT p2.snapshot_id FROM "
                    "structure_publications p2 WHERE p2.status='published' AND "
                    "p2.published_at_ms>=0 AND "
                    "NOT EXISTS (SELECT 1 FROM structure_generation_cleanup_receipts r2 "
                    "WHERE r2.generation_snapshot_id=p2.snapshot_id) "
                    "ORDER BY p2.published_at_ms DESC,p2.snapshot_id DESC LIMIT ?) "
                    "ORDER BY p.published_at_ms,p.snapshot_id LIMIT 1",
                    (retain_generations,),
                ).fetchone()
                if candidate is None:
                    con.execute("COMMIT")
                    return {
                        "blocked": False,
                        "blocked_reason": None,
                        "generation_snapshot_id": None,
                        "phase": None,
                        "rows_deleted": 0,
                        "reclaimed_generation_ids": [],
                        "retained_generation_ids": retained_ids,
                    }
                snapshot_id, publication_id = int(candidate[0]), str(candidate[1])
                receipt = con.execute(
                    "SELECT legacy_snapshot_id,legacy_market_count,generation_market_count,"
                    "legacy_universe_hash,generation_universe_hash,"
                    "legacy_source_truth_hash,generation_source_truth_hash,"
                    "generation_validation_hash,created_at_ms,receipt_digest "
                    "FROM structure_generation_comparison_receipts "
                    "WHERE generation_snapshot_id=? AND publication_id=?",
                    (snapshot_id, publication_id),
                ).fetchone()
                publication = con.execute(
                    "SELECT window_id,expected_counts_json,committed_counts_json,"
                    "validation_hash,certification_hash,certification_component FROM "
                    "structure_publications WHERE publication_id=? AND snapshot_id=? "
                    "AND status='published'",
                    (publication_id, snapshot_id),
                ).fetchone()
                blocked_reason = None
                if receipt is None or publication is None:
                    blocked_reason = "generation-authentication-missing"
                elif receipt[9] != _comparison_receipt_digest(
                    generation_snapshot_id=snapshot_id,
                    publication_id=publication_id,
                    legacy_snapshot_id=int(receipt[0]),
                    legacy_market_count=int(receipt[1]),
                    generation_market_count=int(receipt[2]),
                    legacy_universe_hash=str(receipt[3]),
                    generation_universe_hash=str(receipt[4]),
                    legacy_source_truth_hash=str(receipt[5]),
                    generation_source_truth_hash=str(receipt[6]),
                    generation_validation_hash=str(receipt[7]),
                    created_at_ms=int(receipt[8]),
                ):
                    blocked_reason = "comparison-receipt-digest-mismatch"
                elif publication[3] != publication[4] or receipt[7] != publication[3]:
                    blocked_reason = "generation-validation-hash-mismatch"
                elif publication[1] != publication[2] or publication[5] not in {
                    "bounded-complete",
                    "backfill-authenticated",
                }:
                    blocked_reason = "generation-count-contract-mismatch"
                elif int(receipt[2]) != int(
                    json.loads(str(publication[2])).get("markets", -1)
                ):
                    blocked_reason = "generation-count-contract-mismatch"
                elif (
                    con.execute(
                        "SELECT 1 FROM current_structure_generation WHERE snapshot_id=?",
                        (snapshot_id,),
                    ).fetchone()
                    is not None
                ):
                    blocked_reason = "generation-became-current"
                if blocked_reason is not None:
                    _append_generation_cleanup_observation(
                        con,
                        generation_snapshot_id=snapshot_id,
                        publication_id=publication_id,
                        state="blocked",
                        reason=blocked_reason,
                        observed_at_ms=now_ms,
                    )
                    con.execute("COMMIT")
                    return {
                        "blocked": True,
                        "blocked_reason": blocked_reason,
                        "generation_snapshot_id": snapshot_id,
                        "phase": None,
                        "rows_deleted": 0,
                        "reclaimed_generation_ids": [],
                        "retained_generation_ids": retained_ids,
                    }
                con.execute(
                    "INSERT INTO structure_generation_cleanup_progress("
                    "generation_snapshot_id,publication_id,phase,rows_deleted,"
                    "started_at_ms,checkpoint_at_ms,authorization_digest) "
                    "VALUES (?,?,'events',0,?,?,?)",
                    (snapshot_id, publication_id, now_ms, now_ms, receipt[9]),
                )
                _append_generation_cleanup_observation(
                    con,
                    generation_snapshot_id=snapshot_id,
                    publication_id=publication_id,
                    state="authorized",
                    reason=None,
                    observed_at_ms=now_ms,
                )
                active = (snapshot_id, publication_id, "events", 0)
            snapshot_id, publication_id, phase, prior_deleted = (
                int(active[0]),
                str(active[1]),
                str(active[2]),
                int(active[3]),
            )
            active_auth_error = _active_generation_cleanup_authentication_error(
                con,
                snapshot_id=snapshot_id,
                publication_id=publication_id,
            )
            if active_auth_error is not None:
                con.execute(
                    "UPDATE structure_generation_cleanup_progress SET blocked_reason=?,"
                    "checkpoint_at_ms=? WHERE generation_snapshot_id=?",
                    (active_auth_error, now_ms, snapshot_id),
                )
                con.execute("COMMIT")
                return {
                    "blocked": True,
                    "blocked_reason": active_auth_error,
                    "generation_snapshot_id": snapshot_id,
                    "phase": phase,
                    "rows_deleted": 0,
                    "reclaimed_generation_ids": [],
                    "retained_generation_ids": retained_ids,
                }
            if (
                snapshot_id in retained_ids
                or con.execute(
                    "SELECT 1 FROM current_structure_generation WHERE snapshot_id=?",
                    (snapshot_id,),
                ).fetchone()
                is not None
            ):
                con.execute(
                    "UPDATE structure_generation_cleanup_progress SET blocked_reason=?,"
                    "checkpoint_at_ms=? WHERE generation_snapshot_id=?",
                    ("generation-entered-retention-floor", now_ms, snapshot_id),
                )
                con.execute("COMMIT")
                return {
                    "blocked": True,
                    "blocked_reason": "generation-entered-retention-floor",
                    "generation_snapshot_id": snapshot_id,
                    "phase": phase,
                    "rows_deleted": 0,
                    "reclaimed_generation_ids": [],
                    "retained_generation_ids": retained_ids,
                }
            table = self._structure_component_table(phase)
            deleted = con.execute(
                f"DELETE FROM {table} WHERE rowid IN (SELECT rowid FROM {table} "
                "WHERE snapshot_id=? ORDER BY rowid LIMIT ?)",  # noqa: S608
                (snapshot_id, max_rows),
            ).rowcount
            remaining = con.execute(
                f"SELECT 1 FROM {table} WHERE snapshot_id=? LIMIT 1",  # noqa: S608
                (snapshot_id,),
            ).fetchone()
            reclaimed: list[int] = []
            next_phase = phase
            phases = list(_STRUCTURE_COMPONENTS)
            if remaining is None:
                index = phases.index(phase)
                if index + 1 < len(phases):
                    next_phase = phases[index + 1]
                    con.execute(
                        "UPDATE structure_generation_cleanup_progress SET phase=?,"
                        "rows_deleted=?,checkpoint_at_ms=?,blocked_reason=NULL "
                        "WHERE generation_snapshot_id=?",
                        (next_phase, prior_deleted + deleted, now_ms, snapshot_id),
                    )
                else:
                    publication = con.execute(
                        "SELECT committed_counts_json,validation_hash FROM "
                        "structure_publications WHERE publication_id=?",
                        (publication_id,),
                    ).fetchone()
                    assert publication is not None
                    digest = _generation_cleanup_digest(
                        generation_snapshot_id=snapshot_id,
                        publication_id=publication_id,
                        component_counts_json=str(publication[0]),
                        generation_validation_hash=str(publication[1]),
                        reclaimed_at_ms=now_ms,
                    )
                    con.execute(
                        "INSERT INTO structure_generation_cleanup_receipts("
                        "generation_snapshot_id,publication_id,component_counts_json,"
                        "generation_validation_hash,reclaimed_at_ms,cleanup_digest) "
                        "VALUES (?,?,?,?,?,?)",
                        (
                            snapshot_id,
                            publication_id,
                            publication[0],
                            publication[1],
                            now_ms,
                            digest,
                        ),
                    )
                    con.execute(
                        "DELETE FROM structure_generation_cleanup_progress "
                        "WHERE generation_snapshot_id=?",
                        (snapshot_id,),
                    )
                    next_phase = "complete"
                    reclaimed = [snapshot_id]
            else:
                con.execute(
                    "UPDATE structure_generation_cleanup_progress SET rows_deleted=?,"
                    "checkpoint_at_ms=?,blocked_reason=NULL WHERE generation_snapshot_id=?",
                    (prior_deleted + deleted, now_ms, snapshot_id),
                )
            con.execute("COMMIT")
            return {
                "blocked": False,
                "blocked_reason": None,
                "generation_snapshot_id": snapshot_id,
                "phase": next_phase,
                "rows_deleted": int(deleted),
                "reclaimed_generation_ids": reclaimed,
                "retained_generation_ids": retained_ids,
            }
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
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
            rows = (
                []
                if pointer is None
                else con.execute(
                    "SELECT market_id FROM structure_generation_markets "
                    "WHERE snapshot_id=? ORDER BY market_id",
                    (int(pointer[0]),),
                ).fetchall()
            )
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
                if (
                    marker is None
                    or marker[0] != "writing"
                    or marker[1]
                    not in {
                        "backfill-frozen",
                        *_STRUCTURE_COMPONENTS,
                        "comparison",
                    }
                ):
                    raise ValueError("structure-backfill-freeze-race")
            con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def _start_backfill_certification(
        self,
        publication_id: str,
        *,
        now_ms: int,
    ) -> None:
        """Move a frozen backfill into the shared bounded certification chain."""
        con = self._connect_writer()
        try:
            con.execute("BEGIN IMMEDIATE")
            zero_counts = json.dumps(
                {component: 0 for component in _STRUCTURE_CERTIFICATION_COMPONENTS},
                sort_keys=True,
                separators=(",", ":"),
            )
            changed = con.execute(
                "UPDATE structure_publications SET certification_component='events',"
                "certification_row_cursor=NULL,certification_hash=?,"
                "certification_counts_json=?,checkpoint_at_ms=? "
                "WHERE publication_id=? AND status='writing' "
                "AND certification_component='backfill-frozen' "
                "AND expected_counts_json=committed_counts_json",
                ("0" * 64, zero_counts, now_ms, publication_id),
            )
            if changed.rowcount != 1:
                current = con.execute(
                    "SELECT certification_component FROM structure_publications "
                    "WHERE publication_id=? AND status='writing'",
                    (publication_id,),
                ).fetchone()
                if current is None or current[0] not in {
                    *_STRUCTURE_COMPONENTS,
                    "comparison",
                }:
                    raise ValueError("structure-backfill-certification-race")
            con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def backfill_current_structure_generation(
        self,
        max_rows: int,
        *,
        trace_callback: Callable[[str], None] | None = None,
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
            if trace_callback is not None:
                con.set_trace_callback(trace_callback)
            con.execute("BEGIN IMMEDIATE")
            pointer = con.execute(
                "SELECT snapshot_id,publication_id,comparison_receipt_digest "
                "FROM current_structure_generation WHERE id=1"
            ).fetchone()
            if pointer is not None:
                snapshot_id = int(pointer[0])
                if pointer[2] is None:
                    try:
                        generation = _resolve_generation_structure(con, None)
                    except StructureGenerationReadError:
                        con.execute("COMMIT")
                        return BackfillCheckpoint(snapshot_id, 0, None, True)
                    repair_now_ms = int(generation.finished_at_ms)
                    con.execute("COMMIT")
                    repaired = self._advance_structure_comparison_chunk(
                        str(pointer[1]),
                        max_rows=max_rows,
                        now_ms=repair_now_ms,
                        repair_published=True,
                    )
                    return BackfillCheckpoint(
                        snapshot_id,
                        0,
                        repaired.cursor,
                        repaired.ready,
                    )
                con.execute("COMMIT")
                return BackfillCheckpoint(snapshot_id, 0, None, True)
            publication = con.execute(
                "SELECT publication_id,snapshot_id,window_id,status,write_component,"
                "write_row_cursor,expected_counts_json,committed_counts_json,"
                "certification_component "
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
                expected = {component: 0 for component in _STRUCTURE_COMPONENTS}
                con.execute(
                    "INSERT INTO structure_sync_windows(id,recovery_root_window_id,status,"
                    "started_at_ms,checkpoint_at_ms) "
                    "SELECT ?,?,'complete',taken_at_ms,finished_at_ms "
                    "FROM snapshots WHERE id=?",
                    (window_id, window_id, snapshot_id),
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
                    json.dumps(zero_counts, sort_keys=True, separators=(",", ":")),
                    None,
                )
            publication_id = str(publication[0])
            snapshot_id = int(publication[1])
            status = str(publication[3])
            component = str(publication[4])
            cursor = None if publication[5] is None else str(publication[5])
            expected = json.loads(str(publication[6]))
            committed = json.loads(str(publication[7]))
            certification_component = (
                None if publication[8] is None else str(publication[8])
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
            elif certification_component is not None:
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
                            "snapshot_id",
                            "id",
                            "slug",
                            "title",
                            "ticker",
                            "active",
                            "closed",
                            "liquidity_usd",
                            "volume_usd",
                            "end_time_ms",
                            "fetched_at_ms",
                            "page_fetched_at_ms",
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
                            "snapshot_id",
                            "event_id",
                            "neg_risk_market_id",
                            "market_id",
                            "member_kind",
                            "active",
                            "closed",
                        ),
                        ("event_id", "neg_risk_market_id", "market_id"),
                    ),
                    "group_truth": (
                        "neg_risk_group_truth",
                        (
                            "snapshot_id",
                            "event_id",
                            "neg_risk_market_id",
                            "neg_risk_type",
                            "expected_member_count",
                            "active_named_count",
                            "membership_hash",
                            "quality",
                            "reason",
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
                            "snapshot_id",
                            "id",
                            "layer",
                            "category",
                            "market_id",
                            "detail",
                            "raw_payload",
                        ),
                        ("id",),
                    ),
                }
                component_index = _STRUCTURE_COMPONENTS.index(component)
                copy_complete = False
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
                                "snapshot_id",
                                "issue_index",
                                "layer",
                                "category",
                                "market_id",
                                "detail",
                                "raw_payload",
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
                        committed[component] = int(committed[component]) + copied_rows
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
                        if more is None and component_index + 1 < len(
                            _STRUCTURE_COMPONENTS
                        ):
                            expected[component] = int(committed[component])
                            component_index += 1
                            component = _STRUCTURE_COMPONENTS[component_index]
                            cursor = None
                        elif more is None:
                            expected[component] = int(committed[component])
                            copy_complete = True
                    elif component_index + 1 < len(_STRUCTURE_COMPONENTS):
                        expected[component] = int(committed[component])
                        component_index += 1
                        component = _STRUCTURE_COMPONENTS[component_index]
                        cursor = None
                    else:
                        expected[component] = int(committed[component])
                        copy_complete = True
                        break

                counts_json = json.dumps(
                    committed,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                expected_json = json.dumps(
                    expected,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                con.execute(
                    "UPDATE structure_publications SET write_component=?,"
                    "write_prior_cursor=write_row_cursor,write_row_cursor=?,"
                    "expected_counts_json=?,committed_counts_json=? "
                    "WHERE publication_id=?",
                    (component, cursor, expected_json, counts_json, publication_id),
                )
                if copy_complete:
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
                        "SELECT finished_at_ms FROM snapshots WHERE id=?",
                        (snapshot_id,),
                    ).fetchone()[0]
                )
            if needs_certification:
                self._start_backfill_certification(
                    publish_id,
                    now_ms=finished_at_ms,
                )
                remaining_rows = max_rows - copied_rows
                certification: StructureCertificationChunk | None = None
                while remaining_rows > 0:
                    certification = self.advance_structure_certification_chunk(
                        publish_id,
                        max_rows=remaining_rows,
                        now_ms=finished_at_ms,
                    )
                    remaining_rows -= certification.rows_processed
                    if certification.ready:
                        break
                if certification is None or not certification.ready:
                    return BackfillCheckpoint(
                        snapshot_id,
                        copied_rows,
                        None if certification is None else certification.cursor,
                        False,
                    )
            self.publish_structure_generation(publish_id, finished_at_ms)
            return BackfillCheckpoint(snapshot_id, copied_rows, cursor, True)
        return BackfillCheckpoint(snapshot_id, copied_rows, cursor, False)

    def begin_or_resume_structure_sync(
        self, *, started_at_ms: int
    ) -> dict[str, object]:
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
                    "id,recovery_root_window_id,status,started_at_ms,checkpoint_at_ms) "
                    "VALUES (?,?,'open',?,?)",
                    (window_id, window_id, started_at_ms, started_at_ms),
                )
                con.execute(
                    "INSERT INTO structure_sync_event_source_progress VALUES (?,?,?,?)",
                    (window_id, 0, RowChainSHA256.new("source-event").to_json(),
                     started_at_ms),
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
            lineage = con.execute(
                "SELECT recovery_root_window_id FROM structure_sync_windows "
                "WHERE id=? AND status IN ('open','events_complete')",
                (window_id,),
            ).fetchone()
            if lineage is None or not str(lineage[0]):
                raise ValueError("structure-sync-window-not-restartable")
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
                "id,recovery_root_window_id,status,started_at_ms,checkpoint_at_ms"
                ") VALUES (?,?,'open',?,?)",
                (successor_id, str(lineage[0]), restarted_at_ms, restarted_at_ms + 1),
            )
            con.execute(
                "INSERT INTO structure_sync_event_source_progress VALUES (?,?,?,?)",
                (successor_id, 0, RowChainSHA256.new("source-event").to_json(),
                 restarted_at_ms + 1),
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

    def rotate_blocked_structure_sync_window(
        self,
        *,
        window_id: str,
        rotated_at_ms: int,
    ) -> dict[str, object]:
        """Preserve a blocked complete window and atomically open a clean successor."""
        if not window_id or rotated_at_ms < 0:
            raise ValueError("invalid-structure-bootstrap-rotation")
        successor_id = uuid.uuid4().hex
        con = self._connect_writer()
        try:
            con.execute("BEGIN IMMEDIATE")
            blocked = con.execute(
                "SELECT progress.blocked_reason,progress.window_checkpoint_at_ms,"
                "window.checkpoint_at_ms,progress.event_cursor,progress.member_offset,"
                "progress.checkpoint_at_ms,window.recovery_root_window_id FROM "
                "structure_sync_event_market_backfill_progress progress JOIN "
                "structure_sync_windows window ON window.id=progress.window_id "
                "LEFT JOIN structure_publications publication "
                "ON publication.window_id=window.id WHERE progress.window_id=? "
                "AND window.status='complete' AND progress.blocked_reason IS NOT NULL "
                "AND progress.completed_at_ms IS NULL AND publication.window_id IS NULL",
                (window_id,),
            ).fetchone()
            if (
                blocked is None
                or int(blocked[1]) != int(blocked[2])
                or not str(blocked[0])
            ):
                raise ValueError("structure-bootstrap-window-not-rotatable")
            reason = str(blocked[0])[:200]
            changed = con.execute(
                "UPDATE structure_sync_windows SET status='failed',failure_reason=?,"
                "checkpoint_at_ms=? WHERE id=? AND status='complete' "
                "AND checkpoint_at_ms=?",
                (reason, rotated_at_ms, window_id, int(blocked[2])),
            )
            if changed.rowcount != 1:
                raise ValueError("structure-bootstrap-window-rotation-race")
            con.execute(
                "INSERT INTO structure_sync_windows("
                "id,recovery_root_window_id,status,started_at_ms,checkpoint_at_ms) "
                "VALUES (?,?,'open',?,?)",
                (successor_id, str(blocked[6]), rotated_at_ms, rotated_at_ms + 1),
            )
            con.execute(
                "INSERT INTO structure_sync_event_source_progress VALUES (?,?,?,?)",
                (successor_id, 0, RowChainSHA256.new("source-event").to_json(),
                 rotated_at_ms + 1),
            )
            digest = _bootstrap_rotation_digest(
                recovery_root_window_id=str(blocked[6]),
                old_window_id=window_id,
                event_cursor=str(blocked[3]),
                member_offset=int(blocked[4]),
                blocked_reason=reason,
                checkpoint_at_ms=int(blocked[5]),
                successor_window_id=successor_id,
                rotated_at_ms=rotated_at_ms,
            )
            con.execute(
                "INSERT INTO structure_bootstrap_rotation_observations("
                "recovery_root_window_id,old_window_id,event_cursor,member_offset,blocked_reason,"
                "checkpoint_at_ms,successor_window_id,rotated_at_ms,"
                "observation_digest) VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    str(blocked[6]),
                    window_id,
                    str(blocked[3]),
                    int(blocked[4]),
                    reason,
                    int(blocked[5]),
                    successor_id,
                    rotated_at_ms,
                    digest,
                ),
            )
            row = con.execute(
                "SELECT id,status,event_cursor,market_cursor,started_at_ms,"
                "checkpoint_at_ms,event_pages,market_pages,failure_reason,"
                "published_snapshot_id FROM structure_sync_windows WHERE id=?",
                (successor_id,),
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
        """Reclaim a bounded batch of staging while retaining window authority."""
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
                    "AND staging_reclaimed_at_ms IS NULL "
                    "AND NOT EXISTS (SELECT 1 FROM "
                    "structure_generation_drift_progress progress "
                    "WHERE progress.window_id=structure_sync_windows.id) "
                    "ORDER BY checkpoint_at_ms,id LIMIT ?",
                    (*keep_ids, max_windows_per_run),
                )
            ]
            if to_delete:
                delete_placeholders = ",".join("?" for _ in to_delete)
                reclaimed_at_ms = int(time.time() * 1_000)
                con.execute(
                    "UPDATE structure_sync_windows SET staging_reclaimed_at_ms=? "
                    f"WHERE id IN ({delete_placeholders}) AND status='published' "
                    "AND staging_reclaimed_at_ms IS NULL",
                    (reclaimed_at_ms, *to_delete),
                )
                for table in (
                    "structure_sync_event_conflict_proofs",
                    "structure_sync_event_conflict_merkle_nodes",
                    "structure_sync_event_member_staging",
                    "structure_sync_event_group_truth_staging",
                    "structure_sync_event_metadata_staging",
                ):
                    con.execute(
                        f"DELETE FROM {table} WHERE window_id IN ({delete_placeholders})",
                        to_delete,
                    )
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
            con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

        if to_delete:
            logger.info(
                "structure staging retention reclaimed "
                f"{len(to_delete)} published windows ids={to_delete}"
            )
        return len(to_delete), to_delete

    def purge_failed_structure_sync_windows(
        self,
        *,
        max_windows_per_run: int = 1,
    ) -> tuple[int, list[str]]:
        """Reclaim failed-window staging while retaining failure authority."""
        if max_windows_per_run < 1:
            raise ValueError("max_windows_per_run must be positive")
        con = self._connect_writer()
        try:
            con.execute("BEGIN IMMEDIATE")
            to_delete = [
                str(row[0])
                for row in con.execute(
                    "SELECT id FROM structure_sync_windows WHERE status='failed' "
                    "AND staging_reclaimed_at_ms IS NULL "
                    "ORDER BY checkpoint_at_ms,id LIMIT ?",
                    (max_windows_per_run,),
                )
            ]
            if to_delete:
                placeholders = ",".join("?" for _ in to_delete)
                reclaimed_at_ms = int(time.time() * 1_000)
                con.execute(
                    "UPDATE structure_sync_windows SET staging_reclaimed_at_ms=? "
                    f"WHERE id IN ({placeholders}) AND status='failed' "
                    "AND staging_reclaimed_at_ms IS NULL",
                    (reclaimed_at_ms, *to_delete),
                )
                for table in (
                    "structure_sync_event_conflict_proofs",
                    "structure_sync_event_conflict_merkle_nodes",
                    "structure_sync_event_member_staging",
                    "structure_sync_event_group_truth_staging",
                    "structure_sync_event_metadata_staging",
                ):
                    con.execute(
                        f"DELETE FROM {table} WHERE window_id IN ({placeholders})",
                        to_delete,
                    )
                con.execute(
                    "DELETE FROM structure_sync_event_market_staging "
                    f"WHERE window_id IN ({placeholders})",
                    to_delete,
                )
                con.execute(
                    f"DELETE FROM structure_sync_event_staging WHERE window_id IN ({placeholders})",
                    to_delete,
                )
                con.execute(
                    "DELETE FROM structure_sync_market_staging "
                    f"WHERE window_id IN ({placeholders})",
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

    def read_complete_structure_sync(
        self, window_id: object
    ) -> tuple[list[dict], list[dict]]:
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
                    "WHERE window_id=? ORDER BY event_id",
                    (window_id,),
                ).fetchall()
            ]
            markets = [
                json.loads(str(item[0]))
                for item in con.execute(
                    "SELECT payload_json FROM structure_sync_market_staging "
                    "WHERE window_id=? ORDER BY market_id",
                    (window_id,),
                ).fetchall()
            ]
            return events, markets
        finally:
            con.close()

    def get_complete_structure_sync_counts(self, window_id: object) -> tuple[int, int]:
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
                    "SELECT COUNT(*) FROM structure_sync_event_staging WHERE window_id=?",
                    (window_id,),
                ).fetchone()[0]
            )
            market_count = int(
                con.execute(
                    "SELECT COUNT(*) FROM structure_sync_market_staging WHERE window_id=?",
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
            "id",
            "status",
            "event_cursor",
            "market_cursor",
            "started_at_ms",
            "checkpoint_at_ms",
            "event_pages",
            "market_pages",
            "failure_reason",
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
        writer_timeout_s: float | None = None,
    ) -> None:
        """Stage one validated event page and advance its opaque cursor together."""
        if not isinstance(window_id, str) or not window_id:
            raise ValueError("invalid-structure-sync-window")
        if finished_at_ms < 0 or completed != (next_cursor is None):
            raise ValueError("invalid-structure-event-page")
        serialized: list[tuple[str, str, str | None, str | None, str, int]] = []
        for event in events:
            event_id = event.get("id") if isinstance(event, dict) else None
            if not isinstance(event_id, str) or not event_id:
                raise ValueError("invalid-structure-event")
            payload = json.dumps(
                event, sort_keys=True, ensure_ascii=False, allow_nan=False,
                separators=(",", ":"),
            )
            raw_group = event.get("negRiskMarketID")
            group_id = (
                raw_group if isinstance(raw_group, str) and raw_group
                and raw_group.strip() == raw_group else None
            )
            serialized.append((
                event_id, payload, requested_cursor, group_id,
                hashlib.sha256(payload.encode()).hexdigest(),
                len(payload.encode()),
            ))
        con = self._connect_writer(timeout_s=writer_timeout_s)
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                "SELECT status,event_cursor,event_pages,checkpoint_at_ms FROM "
                "structure_sync_windows WHERE id=?",
                (window_id,),
            ).fetchone()
            if row is None or row[0] != "open" or row[1] != requested_cursor:
                expected_statuses = (
                    {"events_complete", "complete", "published"}
                    if completed else {"open"}
                )
                replay_rows = con.execute(
                    "SELECT event.event_id,event.payload_json,event.source_cursor,"
                    "metadata.event_group_id,metadata.payload_hash,metadata.payload_length "
                    "FROM structure_sync_event_staging event JOIN "
                    "structure_sync_event_metadata_staging metadata ON "
                    "metadata.window_id=event.window_id AND metadata.event_id=event.event_id "
                    "WHERE event.window_id=? AND event.source_cursor IS ? "
                    "ORDER BY event.source_ordinal",
                    (window_id, requested_cursor),
                ).fetchall()
                expected_rows = [tuple(item[:6]) for item in serialized]
                sealed = (
                    con.execute(
                        "SELECT sealed_at_ms FROM structure_sync_event_source_receipts "
                        "WHERE window_id=?", (window_id,),
                    ).fetchone()
                    if completed else None
                )
                if (
                    row is not None
                    and row[0] in expected_statuses
                    and row[1] == next_cursor
                    and int(row[2]) >= 1
                    and (
                        (completed and sealed is not None
                         and int(sealed[0]) == finished_at_ms)
                        or (not completed and int(row[3]) == finished_at_ms)
                    )
                    and [tuple(item) for item in replay_rows] == expected_rows
                ):
                    if completed and _validated_structure_event_source_receipt(
                        con, window_id
                    ) is None:
                        raise ValueError("structure-event-source-replay-mismatch")
                    con.execute("COMMIT")
                    return
                if replay_rows:
                    raise ValueError("structure-event-source-replay-mismatch")
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
            source_progress = con.execute(
                "SELECT event_count,event_state FROM "
                "structure_sync_event_source_progress WHERE window_id=?", (window_id,),
            ).fetchone()
            source_chain = (
                RowChainSHA256.from_json(str(source_progress[1]), expected_domain="source-event")
                if source_progress is not None else None
            )
            source_count = 0 if source_progress is None else int(source_progress[0])
            for (
                event_id, payload, cursor, group_id, payload_hash, payload_length, ordinal,
            ) in ordered:
                existing = con.execute(
                    "SELECT event.payload_json,event.source_cursor,event.source_ordinal,"
                    "metadata.event_group_id,metadata.payload_hash,metadata.payload_length "
                    "FROM structure_sync_event_staging event LEFT JOIN "
                    "structure_sync_event_metadata_staging metadata ON "
                    "metadata.window_id=event.window_id AND metadata.event_id=event.event_id "
                    "WHERE event.window_id=? AND event.event_id=?", (window_id, event_id),
                ).fetchone()
                expected = (payload, cursor, ordinal, group_id, payload_hash, payload_length)
                if existing is not None:
                    if tuple(existing) != expected:
                        raise ValueError("structure-event-source-replay-mismatch")
                    continue
                con.execute(
                    "INSERT INTO structure_sync_event_staging VALUES (?,?,?,?,?)",
                    (window_id, event_id, payload, cursor, ordinal),
                )
                if source_chain is not None:
                    con.execute(
                        "INSERT INTO structure_sync_event_metadata_staging VALUES "
                        "(?,?,?,?,?,?,?)",
                        (window_id, event_id, ordinal, group_id, payload_hash,
                         payload_length, STRUCTURE_EVENT_SOURCE_CONTRACT),
                    )
                    source_chain.update((
                        STRUCTURE_EVENT_SOURCE_CONTRACT, event_id, ordinal, group_id,
                        payload_hash, payload_length,
                    ))
                    source_count += 1
            if source_chain is not None:
                con.execute(
                    "UPDATE structure_sync_event_source_progress SET event_count=?,"
                    "event_state=?,checkpoint_at_ms=? WHERE window_id=?",
                    (source_count, source_chain.to_json(), finished_at_ms, window_id),
                )
                if completed:
                    receipt = (
                        window_id, source_count, source_chain.hexdigest(),
                        int(con.execute("SELECT event_pages+1 FROM structure_sync_windows "
                                        "WHERE id=?", (window_id,)).fetchone()[0]),
                        "", STRUCTURE_EVENT_SOURCE_CONTRACT, finished_at_ms,
                    )
                    receipt_digest = _structure_event_source_receipt_digest(receipt)
                    con.execute(
                        "INSERT INTO structure_sync_event_source_receipts VALUES ("
                        + ",".join("?" for _ in range(8)) + ")",
                        (*receipt, receipt_digest),
                    )
                    source_identity = hashlib.sha256(json.dumps(
                        (window_id, source_count, source_chain.hexdigest(), receipt_digest),
                        separators=(",", ":"),
                    ).encode()).hexdigest()
                    member_state = _event_member_progress_state(
                        member_chain=RowChainSHA256.new("source-event"),
                        source_event_count=source_count,
                        source_event_root=source_chain.hexdigest(),
                        source_identity_hash=source_identity,
                        window_checkpoint_at_ms=finished_at_ms,
                    )
                    diagnostic_state = RowChainSHA256.new(
                        "diagnostic/unclassified"
                    ).to_json()
                    member_checkpoint = _structure_event_member_checkpoint_digest((
                        receipt_digest, "", 0, 0, 0, 0, "", member_state,
                        diagnostic_state,
                    ))
                    con.execute(
                        "INSERT INTO structure_sync_event_member_progress VALUES "
                        "(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (window_id, "", 0, 0, 0, member_state, diagnostic_state,
                         finished_at_ms, None, None, 0, receipt_digest, "",
                         member_checkpoint),
                    )
                    membership_state = SerializableSHA256.new().to_json()
                    truth_state = RowChainSHA256.new("source-event").to_json()
                    group_checkpoint = _structure_event_group_truth_checkpoint_digest((
                        receipt_digest, "", "", "", -1, membership_state,
                        0, 0, 0, 0, truth_state, 0,
                    ))
                    con.execute(
                        "INSERT INTO structure_sync_event_group_truth_progress VALUES "
                        "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            window_id, "", "", "", -1, membership_state,
                            0, 0, 0, 0, truth_state, finished_at_ms, None,
                            group_checkpoint, 0,
                        ),
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
        writer_timeout_s: float | None = None,
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
            serialized.append(
                (market_id, json.dumps(market, sort_keys=True), requested_cursor)
            )
        con = self._connect_writer(timeout_s=writer_timeout_s)
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
            if completed:
                con.execute(
                    "INSERT INTO structure_sync_event_market_backfill_progress("
                    "window_id,window_checkpoint_at_ms,event_cursor,member_offset,"
                    "events_processed,relationships_processed,checkpoint_at_ms,"
                    "completed_at_ms,blocked_reason,migration_reason) "
                    "VALUES (?,?,'',0,0,0,?,NULL,NULL,NULL) "
                    "ON CONFLICT(window_id) DO UPDATE SET "
                    "window_checkpoint_at_ms=excluded.window_checkpoint_at_ms,"
                    "event_cursor='',member_offset=0,events_processed=0,"
                    "relationships_processed=0,checkpoint_at_ms=excluded.checkpoint_at_ms,"
                    "completed_at_ms=NULL,blocked_reason=NULL,migration_reason=NULL",
                    (window_id, finished_at_ms, finished_at_ms),
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
                "INSERT INTO snapshot_attempts(started_at_ms,outcome) VALUES (?, 'running')",
                (started_at_ms,),
            )
            assert cur.lastrowid is not None
            return int(cur.lastrowid)
        finally:
            con.close()

    def begin_structure_drift_attempt(
        self,
        *,
        identity: Mapping[str, object],
        progress_id: str | None,
        started_at_ms: int,
        stale_before_ms: int | None = None,
    ) -> int:
        """Append parent ownership before spawning one drift child."""
        if started_at_ms < 0 or (progress_id is not None and not progress_id):
            raise ValueError("invalid-structure-drift-attempt")
        identity_json = json.dumps(
            dict(identity), sort_keys=True, separators=(",", ":")
        )
        identity_digest = hashlib.sha256(identity_json.encode()).hexdigest()
        con = self._connect_writer()
        try:
            con.execute("BEGIN IMMEDIATE")
            running = con.execute(
                "SELECT id,started_at_ms FROM structure_drift_attempts "
                "WHERE outcome='running' ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if running is not None:
                if stale_before_ms is None or int(running[1]) > stale_before_ms:
                    raise ValueError("structure-drift-attempt-owner-running")
                con.execute(
                    "UPDATE structure_drift_attempts SET finished_at_ms=?,outcome='failed',"
                    "elapsed_ms=MAX(0,?-started_at_ms),chunks_processed=0,rows_processed=0,"
                    "failure_kind='parent-stale-orphan',stderr_bytes=0,stderr_sha256=?,"
                    "stderr_safe_marker=NULL WHERE id=? AND outcome='running'",
                    (
                        started_at_ms,
                        started_at_ms,
                        hashlib.sha256(b"").hexdigest(),
                        int(running[0]),
                    ),
                )
            cur = con.execute(
                "INSERT INTO structure_drift_attempts("
                "identity_json,identity_digest,progress_id,started_at_ms,outcome) "
                "VALUES(?,?,?,?,'running')",
                (identity_json, identity_digest, progress_id, started_at_ms),
            )
            assert cur.lastrowid is not None
            attempt_id = int(cur.lastrowid)
            con.execute("COMMIT")
            return attempt_id
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def finish_structure_drift_attempt(
        self,
        *,
        attempt_id: int,
        outcome: str,
        finished_at_ms: int,
        last_phase: str | None,
        chunks_processed: int,
        rows_processed: int,
        elapsed_ms: int,
        failure_kind: str | None,
        stderr: bytes = b"",
        stderr_bytes: int | None = None,
        stderr_sha256: str | None = None,
        stderr_safe_marker: str | None = None,
        writer_timeout_s: float | None = None,
    ) -> None:
        """Terminalize one parent-owned drift attempt exactly once."""
        if outcome not in {
            "succeeded",
            "checkpointed",
            "deferred",
            "failed",
            "cancelled",
        }:
            raise ValueError("invalid-structure-drift-attempt-outcome")
        if min(finished_at_ms, chunks_processed, rows_processed, elapsed_ms) < 0:
            raise ValueError("invalid-structure-drift-attempt-metrics")
        if failure_kind is not None and (not failure_kind or len(failure_kind) > 64):
            raise ValueError("invalid-structure-drift-attempt-failure")
        markers = [*_STRUCTURE_DRIFT_SAFE_MARKER_RE.finditer(stderr)]
        derived_safe_marker = (
            max(markers, key=lambda marker: marker.start()).group(0).decode("ascii")
            if markers
            else None
        )
        diagnostic_bytes = len(stderr) if stderr_bytes is None else stderr_bytes
        diagnostic_digest = (
            hashlib.sha256(stderr).hexdigest()
            if stderr_sha256 is None
            else stderr_sha256
        )
        safe_marker = (
            derived_safe_marker if stderr_safe_marker is None else stderr_safe_marker
        )
        if (
            diagnostic_bytes < 0
            or re.fullmatch(r"[0-9a-f]{64}", diagnostic_digest) is None
            or (safe_marker is not None and len(safe_marker) > 256)
            or (
                safe_marker is not None
                and (
                    not safe_marker.isascii()
                    or _STRUCTURE_DRIFT_SAFE_MARKER_RE.fullmatch(
                        safe_marker.encode("ascii")
                    )
                    is None
                )
            )
        ):
            raise ValueError("invalid-structure-drift-attempt-stderr")
        con = self._connect_writer(timeout_s=writer_timeout_s)
        try:
            con.execute("BEGIN IMMEDIATE")
            cur = con.execute(
                "UPDATE structure_drift_attempts SET finished_at_ms=?,outcome=?,"
                "last_phase=?,chunks_processed=?,rows_processed=?,elapsed_ms=?,"
                "failure_kind=?,stderr_bytes=?,stderr_sha256=?,stderr_safe_marker=? "
                "WHERE id=? AND outcome='running' AND finished_at_ms IS NULL",
                (
                    finished_at_ms,
                    outcome,
                    last_phase,
                    chunks_processed,
                    rows_processed,
                    elapsed_ms,
                    failure_kind,
                    diagnostic_bytes,
                    diagnostic_digest,
                    safe_marker,
                    attempt_id,
                ),
            )
            if cur.rowcount != 1:
                raise ValueError("structure-drift-attempt-already-terminal")
            con.execute(
                "DELETE FROM structure_drift_attempts WHERE outcome!='running' "
                "AND id NOT IN (SELECT id FROM structure_drift_attempts "
                "WHERE outcome!='running' ORDER BY id DESC LIMIT ?)",
                (_STRUCTURE_DRIFT_ATTEMPT_RETENTION,),
            )
            con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def recover_orphaned_structure_drift_attempts(self, *, recovered_at_ms: int) -> int:
        """Close children whose parent disappeared before terminal evidence."""
        if recovered_at_ms < 0:
            raise ValueError("invalid-structure-drift-recovery-time")
        con = self._connect_writer()
        try:
            con.execute("BEGIN IMMEDIATE")
            cur = con.execute(
                "UPDATE structure_drift_attempts SET finished_at_ms=?,outcome='failed',"
                "elapsed_ms=MAX(0,?-started_at_ms),chunks_processed=0,rows_processed=0,"
                "failure_kind='parent-restarted-orphan',stderr_bytes=0,"
                "stderr_sha256=?,stderr_safe_marker=NULL "
                "WHERE outcome='running' AND finished_at_ms IS NULL",
                (recovered_at_ms, recovered_at_ms, hashlib.sha256(b"").hexdigest()),
            )
            recovered = int(cur.rowcount)
            con.execute(
                "DELETE FROM structure_drift_attempts WHERE outcome!='running' "
                "AND id NOT IN (SELECT id FROM structure_drift_attempts "
                "WHERE outcome!='running' ORDER BY id DESC LIMIT ?)",
                (_STRUCTURE_DRIFT_ATTEMPT_RETENTION,),
            )
            con.execute("COMMIT")
            return recovered
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def get_latest_structure_drift_attempt(self) -> dict[str, object] | None:
        """Return bounded latest parent evidence for health and operators."""
        try:
            with sqlite3.connect(f"file:{self._db_path}?mode=ro", uri=True) as con:
                row = con.execute(
                    "SELECT id,identity_json,identity_digest,progress_id,started_at_ms,"
                    "finished_at_ms,outcome,last_phase,chunks_processed,rows_processed,"
                    "elapsed_ms,failure_kind,stderr_bytes,stderr_sha256,"
                    "stderr_safe_marker FROM structure_drift_attempts "
                    "ORDER BY id DESC LIMIT 1"
                ).fetchone()
        except sqlite3.OperationalError:
            return None
        if row is None:
            return None
        keys = (
            "id",
            "identity_json",
            "identity_digest",
            "progress_id",
            "started_at_ms",
            "finished_at_ms",
            "outcome",
            "last_phase",
            "chunks_processed",
            "rows_processed",
            "elapsed_ms",
            "failure_kind",
            "stderr_bytes",
            "stderr_sha256",
            "stderr_safe_marker",
        )
        result = dict(zip(keys, row, strict=True))
        try:
            result["identity"] = json.loads(str(result.pop("identity_json")))
        except json.JSONDecodeError:
            result["identity"] = None
        return result

    def record_structure_defer(
        self,
        reason: str,
        queued_at_ms: int,
        observed_at_ms: int,
        *,
        initialized_comparison_id: str | None = None,
        current_comparison_id: str | None = None,
        classifier_contract_version: str | None = None,
    ) -> int:
        """Persist bounded Quote-priority admission evidence across restarts."""
        if (
            not reason
            or len(reason) > 64
            or queued_at_ms < 0
            or observed_at_ms < queued_at_ms
            or any(
                value is not None and (not value or len(value) > 128)
                for value in (
                    initialized_comparison_id,
                    current_comparison_id,
                    classifier_contract_version,
                )
            )
        ):
            raise ValueError("invalid-structure-defer")
        con = self._connect_writer()
        try:
            con.execute("BEGIN IMMEDIATE")
            cur = con.execute(
                "INSERT INTO structure_defer_receipts("
                "reason,queued_at_ms,observed_at_ms,initialized_comparison_id,"
                "current_comparison_id,classifier_contract_version) "
                "VALUES (?,?,?,?,?,?)",
                (
                    reason,
                    queued_at_ms,
                    observed_at_ms,
                    initialized_comparison_id,
                    current_comparison_id,
                    classifier_contract_version,
                ),
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
                    "SELECT id,reason,queued_at_ms,observed_at_ms,"
                    "initialized_comparison_id,current_comparison_id,"
                    "classifier_contract_version "
                    "FROM structure_defer_receipts ORDER BY id DESC LIMIT 1"
                ).fetchone()
        except sqlite3.OperationalError:
            return None
        if row is None:
            return None
        result = dict(
            zip(
                (
                    "id",
                    "reason",
                    "queued_at_ms",
                    "observed_at_ms",
                    "initialized_comparison_id",
                    "current_comparison_id",
                    "classifier_contract_version",
                ),
                row,
                strict=True,
            )
        )
        return {key: value for key, value in result.items() if value is not None}

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
        chunks_processed: int | None = None,
        stderr_bytes: int | None = None,
        stderr_sha256: str | None = None,
        stderr_tail: str | None = None,
    ) -> None:
        """Close one running attempt exactly once with a bounded outcome."""
        if outcome not in {"succeeded", "failed", "cancelled"}:
            raise ValueError(f"invalid terminal snapshot attempt outcome: {outcome}")
        if chunks_processed is not None and (
            isinstance(chunks_processed, bool) or not 0 <= chunks_processed <= 100
        ):
            raise ValueError("invalid snapshot attempt chunks_processed")
        if stderr_bytes is not None and (
            isinstance(stderr_bytes, bool)
            or not isinstance(stderr_bytes, int)
            or not 0 <= stderr_bytes <= _SNAPSHOT_ATTEMPT_STDERR_MAX_BYTES
        ):
            raise ValueError("invalid snapshot attempt stderr_bytes")
        if (
            stderr_sha256 is not None
            and re.fullmatch(r"[0-9a-f]{64}", stderr_sha256) is None
        ):
            raise ValueError("invalid snapshot attempt stderr_sha256")
        if (stderr_bytes is None) != (stderr_sha256 is None):
            raise ValueError("incomplete snapshot attempt stderr diagnostic")
        if stderr_tail is not None and (
            stderr_bytes is None
            or len(stderr_tail) > 256
            or _SNAPSHOT_ATTEMPT_STDERR_TAIL_RE.fullmatch(stderr_tail) is None
        ):
            raise ValueError("invalid snapshot attempt stderr_tail")
        con = self._connect_writer()
        try:
            cur = con.execute(
                "UPDATE snapshot_attempts "
                "SET finished_at_ms=?, outcome=?, snapshot_id=?, failure_kind=?, "
                "last_stage=?, elapsed_ms=?, chunks_processed=?,stderr_bytes=?,"
                "stderr_sha256=?,stderr_tail=? "
                "WHERE id=? AND outcome='running'",
                (
                    finished_at_ms,
                    outcome,
                    snapshot_id,
                    failure_kind,
                    last_stage,
                    elapsed_ms,
                    chunks_processed,
                    stderr_bytes,
                    stderr_sha256,
                    stderr_tail,
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
                "last_stage,elapsed_ms,chunks_processed,stderr_bytes,stderr_sha256,"
                "stderr_tail "
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
                "chunks_processed": row[8],
                "stderr_bytes": row[9],
                "stderr_sha256": row[10],
                "stderr_tail": row[11],
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
                "failure_kind,last_stage,elapsed_ms,chunks_processed,stderr_bytes,"
                "stderr_sha256,stderr_tail "
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
                "chunks_processed",
                "stderr_bytes",
                "stderr_sha256",
                "stderr_tail",
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

        Deletes ordinary snapshot rows in FK-safe order. Immutable Structure
        generation evidence is excluded during candidate selection and requires
        a dedicated bounded evidence-aware cleanup workflow.

        Bounding each transaction prevents a large historical backlog from growing
        WAL for minutes and losing all progress when a deployment interrupts it.
        Returns (deleted_count, deleted_ids).
        """
        import time as _time

        if max_snapshots_per_run < 1:
            raise ValueError("max_snapshots_per_run must be positive")
        cutoff_ms = int((_time.time() - older_than_days * 86_400) * 1000)

        con = self._connect_writer()
        try:
            con.execute("PRAGMA journal_mode=WAL")
            con.execute("BEGIN IMMEDIATE")
            try:
                # Keep selection and deletion under one writer lock so no new
                # publication or generation evidence can acquire a candidate.
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
                        f"SELECT s.id FROM snapshots s WHERE s.taken_at_ms < ? "
                        f"AND s.id NOT IN ({placeholders}) "
                        "AND NOT EXISTS (SELECT 1 FROM current_structure_generation g "
                        "WHERE g.snapshot_id=s.id) "
                        "AND NOT EXISTS (SELECT 1 FROM structure_publications p "
                        "WHERE p.snapshot_id=s.id) "
                        "AND NOT EXISTS (SELECT 1 FROM "
                        "structure_generation_comparison_progress cp WHERE "
                        "cp.generation_snapshot_id=s.id OR cp.legacy_snapshot_id=s.id) "
                        "AND NOT EXISTS (SELECT 1 FROM "
                        "structure_generation_comparison_receipts cr WHERE "
                        "cr.generation_snapshot_id=s.id OR cr.legacy_snapshot_id=s.id) "
                        "AND NOT EXISTS (SELECT 1 FROM structure_sync_windows sw "
                        "WHERE sw.published_snapshot_id=s.id) "
                        "AND NOT EXISTS (SELECT 1 FROM neg_risk_quote_runs qr "
                        "WHERE qr.universe_snapshot_id=s.id) "
                        "AND NOT EXISTS (SELECT 1 FROM structure_generation_events ge "
                        "WHERE ge.snapshot_id=s.id) "
                        "AND NOT EXISTS (SELECT 1 FROM structure_generation_event_tags gt "
                        "WHERE gt.snapshot_id=s.id) "
                        "AND NOT EXISTS (SELECT 1 FROM structure_generation_memberships gm "
                        "WHERE gm.snapshot_id=s.id) "
                        "AND NOT EXISTS (SELECT 1 FROM structure_generation_group_truth gg "
                        "WHERE gg.snapshot_id=s.id) "
                        "AND NOT EXISTS (SELECT 1 FROM structure_generation_markets gk "
                        "WHERE gk.snapshot_id=s.id) "
                        "AND NOT EXISTS (SELECT 1 FROM structure_generation_issues gi "
                        "WHERE gi.snapshot_id=s.id) "
                        "ORDER BY s.id LIMIT ?",
                        [cutoff_ms, *keep_ids, max_snapshots_per_run],
                    ).fetchall()
                ]
                # Archive ownership is explicit. A Structure snapshot carries the
                # no-archive marker in parquet_path for compatibility with the old
                # non-null contract; that marker is not a file to unlink.
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
                if not to_delete or dry_run:
                    con.execute("COMMIT")
                    if not to_delete:
                        logger.info("purge_old_snapshots: nothing to delete")
                        return (0, [])
                    logger.info(
                        f"purge_old_snapshots DRY-RUN: would delete {len(to_delete)} "
                        f"snapshots (ids={to_delete}), "
                        f"{len(parquet_paths)} parquet files"
                    )
                    return (0, to_delete)

                id_placeholders = ",".join("?" for _ in to_delete)
                con.execute(
                    "DELETE FROM snapshot_attempts "
                    f"WHERE snapshot_id IN ({id_placeholders})",
                    to_delete,
                )
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
                    f"DELETE FROM neg_risk_group_truth WHERE snapshot_id IN ({id_placeholders})",
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
