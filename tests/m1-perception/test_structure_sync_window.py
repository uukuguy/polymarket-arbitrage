from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from polyarb.clients.gamma_client import (
    EventPage,
    MarketPage,
    PaginationCursorRejectedError,
)
from polyarb.perception import structure_event_members as member_module
from polyarb.perception import structure_publication as structure_publication_module
from polyarb.perception import structure_sync as structure_sync_module
from polyarb.perception.structure_contract import (
    STRUCTURE_EVENT_MEMBER_METADATA_CONTRACT,
    STRUCTURE_EVENT_SOURCE_CONTRACT,
)
from polyarb.perception.structure_drift import project_legacy_compatible_event
from polyarb.perception.structure_event_members import (
    StructureEventMemberProgress,
    StructureEventMemberReceipt,
    StructureEventMemberRow,
    decode_event_member_batch,
    extract_structure_event_member_row,
)
from polyarb.perception.structure_sync import (
    StagedGammaSource,
    StructurePageDeadlineExceeded,
    StructureSyncCheckpoint,
    StructureSyncWorker,
    finalize_structure_window,
    run_structure_sync_until_published,
)
from polyarb.storage import sqlite_store as sqlite_store_module
from polyarb.storage.schemas import STRUCTURE_EVENT_MEMBER_SCHEMA_STATEMENTS
from polyarb.storage.sqlite_store import (
    SQLITE_BUSY_TIMEOUT_S,
    STRUCTURE_EVENT_PAYLOAD_MAX_BYTES,
    SQLiteStore,
)


def _schema_objects(con: sqlite3.Connection, prefix: str) -> tuple[tuple[str, str, str], ...]:
    return tuple(
        (str(kind), str(name), " ".join(str(sql).split()))
        for kind, name, sql in con.execute(
            "SELECT type,name,sql FROM sqlite_master WHERE name LIKE ? OR tbl_name LIKE ? "
            "ORDER BY type,name",
            (f"{prefix}%", f"{prefix}%"),
        )
    )


_EVENT_MEMBER_PHASE_RANK = {
    "deriving-members": 0,
    "deriving-group-truth": 1,
    "sealing-conflicts": 2,
    "sealing-conflict-merkle": 3,
    "sealing-conflict-proofs": 4,
    "sealed": 5,
}


def _advance_event_members_until_sealed(
    store: SQLiteStore,
    window_id: str,
    *,
    limit: int = 500,
    max_chunks: int = 32,
) -> list[dict[str, object]]:
    """Advance every authenticated phase with a strict progress bound."""
    results: list[dict[str, object]] = []
    last_rank = -1
    for _ in range(max_chunks):
        result = store.advance_structure_event_member_staging_chunk(
            window_id=window_id, limit=limit,
        )
        assert result.get("reason") is None
        assert result.get("failure_reason") is None
        state = str(result["state"])
        assert state in _EVENT_MEMBER_PHASE_RANK
        rank = _EVENT_MEMBER_PHASE_RANK[state]
        assert rank >= last_rank, (results, result)
        last_rank = rank
        results.append(result)
        if result.get("sealed") is True:
            assert result.get("complete") is True
            return results
    pytest.fail(
        f"event-member authority did not seal within {max_chunks} chunks: {results}"
    )


# Verbatim relevant predecessor definitions copied from schemas.py at 9b117d4.
# Tests never shell out to git and therefore remain deterministic after history pruning.
_NINE_B117D4_EVENT_MEMBER_PREDECESSOR_DDL = """
CREATE TABLE IF NOT EXISTS structure_sync_windows (
    id TEXT PRIMARY KEY,
    recovery_root_window_id TEXT,
    status TEXT NOT NULL CHECK(status IN (
        'open','events_complete','complete','published','failed'
    )),
    event_cursor TEXT,
    market_cursor TEXT,
    started_at_ms INTEGER NOT NULL CHECK(started_at_ms >= 0),
    checkpoint_at_ms INTEGER NOT NULL CHECK(checkpoint_at_ms >= 0),
    event_pages INTEGER NOT NULL DEFAULT 0 CHECK(event_pages >= 0),
    market_pages INTEGER NOT NULL DEFAULT 0 CHECK(market_pages >= 0),
    published_snapshot_id INTEGER REFERENCES snapshots(id),
    failure_reason TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_structure_sync_one_open_window
ON structure_sync_windows(status) WHERE status IN ('open','events_complete');
CREATE TABLE IF NOT EXISTS structure_sync_event_staging (
    window_id TEXT NOT NULL REFERENCES structure_sync_windows(id),
    event_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    source_cursor TEXT,
    source_ordinal INTEGER,
    PRIMARY KEY(window_id,event_id)
);
CREATE TABLE IF NOT EXISTS structure_sync_event_market_staging (
    window_id TEXT NOT NULL REFERENCES structure_sync_windows(id),
    market_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    source_ordinal INTEGER NOT NULL,
    PRIMARY KEY(window_id,event_id,market_id)
);
CREATE INDEX IF NOT EXISTS idx_structure_sync_event_market_first
ON structure_sync_event_market_staging(window_id,market_id,source_ordinal,event_id);
CREATE TABLE IF NOT EXISTS structure_sync_event_market_backfill_progress (
    window_id TEXT PRIMARY KEY REFERENCES structure_sync_windows(id) ON DELETE CASCADE,
    window_checkpoint_at_ms INTEGER NOT NULL CHECK(window_checkpoint_at_ms >= 0),
    event_cursor TEXT NOT NULL DEFAULT '',
    member_offset INTEGER NOT NULL DEFAULT 0 CHECK(member_offset >= 0),
    events_processed INTEGER NOT NULL DEFAULT 0 CHECK(events_processed >= 0),
    relationships_processed INTEGER NOT NULL DEFAULT 0
        CHECK(relationships_processed >= 0),
    checkpoint_at_ms INTEGER NOT NULL CHECK(checkpoint_at_ms >= 0),
    completed_at_ms INTEGER CHECK(completed_at_ms IS NULL OR completed_at_ms >= 0),
    blocked_reason TEXT,
    migration_reason TEXT
);
CREATE INDEX IF NOT EXISTS idx_structure_event_market_backfill_active
ON structure_sync_event_market_backfill_progress(checkpoint_at_ms DESC,window_id DESC)
WHERE completed_at_ms IS NULL;
CREATE TABLE IF NOT EXISTS structure_sync_market_staging (
    window_id TEXT NOT NULL REFERENCES structure_sync_windows(id),
    market_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    source_cursor TEXT,
    source_ordinal INTEGER,
    PRIMARY KEY(window_id,market_id)
);
CREATE TRIGGER IF NOT EXISTS trg_structure_event_staging_insert_guard
BEFORE INSERT ON structure_sync_event_staging
WHEN (SELECT status FROM structure_sync_windows WHERE id=NEW.window_id)!='open'
BEGIN SELECT RAISE(ABORT,'structure-event-staging-frozen'); END;
CREATE TRIGGER IF NOT EXISTS trg_structure_event_staging_update_guard
BEFORE UPDATE ON structure_sync_event_staging
WHEN (SELECT status FROM structure_sync_windows WHERE id=OLD.window_id)!='open'
BEGIN SELECT RAISE(ABORT,'structure-event-staging-frozen'); END;
CREATE TRIGGER IF NOT EXISTS trg_structure_event_staging_delete_guard
BEFORE DELETE ON structure_sync_event_staging
WHEN (SELECT status FROM structure_sync_windows WHERE id=OLD.window_id)='complete'
BEGIN SELECT RAISE(ABORT,'structure-event-staging-frozen'); END;
CREATE TRIGGER IF NOT EXISTS trg_structure_market_staging_insert_guard
BEFORE INSERT ON structure_sync_market_staging
WHEN (SELECT status FROM structure_sync_windows WHERE id=NEW.window_id)!='events_complete'
BEGIN SELECT RAISE(ABORT,'structure-market-staging-frozen'); END;
CREATE TRIGGER IF NOT EXISTS trg_structure_market_staging_update_guard
BEFORE UPDATE ON structure_sync_market_staging
WHEN (SELECT status FROM structure_sync_windows WHERE id=OLD.window_id)!='events_complete'
BEGIN SELECT RAISE(ABORT,'structure-market-staging-frozen'); END;
CREATE TRIGGER IF NOT EXISTS trg_structure_market_staging_delete_guard
BEFORE DELETE ON structure_sync_market_staging
WHEN (SELECT status FROM structure_sync_windows WHERE id=OLD.window_id)='complete'
BEGIN SELECT RAISE(ABORT,'structure-market-staging-frozen'); END;
CREATE TRIGGER IF NOT EXISTS trg_structure_event_market_insert_guard
BEFORE INSERT ON structure_sync_event_market_staging
WHEN (SELECT status FROM structure_sync_windows WHERE id=NEW.window_id)!='open'
AND NOT EXISTS (
    SELECT 1 FROM structure_sync_event_market_backfill_progress progress
    JOIN structure_sync_windows window ON window.id=progress.window_id
    WHERE progress.window_id=NEW.window_id AND window.status='complete'
      AND progress.completed_at_ms IS NULL AND progress.blocked_reason IS NULL
)
BEGIN SELECT RAISE(ABORT,'structure-event-market-staging-frozen'); END;
CREATE TRIGGER IF NOT EXISTS trg_structure_event_market_update_guard
BEFORE UPDATE ON structure_sync_event_market_staging
WHEN (SELECT status FROM structure_sync_windows WHERE id=OLD.window_id)!='open'
BEGIN SELECT RAISE(ABORT,'structure-event-market-staging-frozen'); END;
CREATE TRIGGER IF NOT EXISTS trg_structure_event_market_delete_guard
BEFORE DELETE ON structure_sync_event_market_staging
WHEN (SELECT status FROM structure_sync_windows WHERE id=OLD.window_id)='complete'
BEGIN SELECT RAISE(ABORT,'structure-event-market-staging-frozen'); END;
CREATE TABLE IF NOT EXISTS structure_publications (
    publication_id TEXT PRIMARY KEY,
    window_id TEXT NOT NULL UNIQUE REFERENCES structure_sync_windows(id),
    snapshot_id INTEGER NOT NULL UNIQUE REFERENCES snapshots(id),
    status TEXT NOT NULL CHECK(status IN (
        'normalizing','writing','ready','published','failed'
    )),
    normalization_contract_version TEXT,
    normalization_component TEXT,
    normalization_source_cursor TEXT,
    write_component TEXT,
    write_prior_cursor TEXT,
    write_row_cursor TEXT,
    expected_counts_json TEXT NOT NULL,
    committed_counts_json TEXT NOT NULL,
    validation_hash TEXT CHECK(
        validation_hash IS NULL OR length(validation_hash)=64
    ),
    certification_component TEXT,
    certification_row_cursor TEXT,
    certification_hash TEXT CHECK(
        certification_hash IS NULL OR length(certification_hash)=64
    ),
    certification_counts_json TEXT,
    created_at_ms INTEGER NOT NULL CHECK(created_at_ms >= 0),
    checkpoint_at_ms INTEGER NOT NULL CHECK(checkpoint_at_ms >= 0),
    certified_at_ms INTEGER,
    published_at_ms INTEGER,
    failure_reason TEXT,
    UNIQUE(snapshot_id,publication_id)
);
CREATE INDEX IF NOT EXISTS idx_structure_publications_published_history
ON structure_publications(published_at_ms,snapshot_id)
WHERE status='published';
CREATE INDEX IF NOT EXISTS idx_structure_publications_active_checkpoint
ON structure_publications(checkpoint_at_ms DESC,publication_id)
WHERE status IN ('normalizing','writing','ready');
CREATE UNIQUE INDEX IF NOT EXISTS idx_structure_publications_snapshot_publication
ON structure_publications(snapshot_id,publication_id);
CREATE TABLE IF NOT EXISTS structure_generation_memberships (
    snapshot_id INTEGER NOT NULL REFERENCES snapshots(id),
    event_id TEXT NOT NULL,
    neg_risk_market_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    member_kind TEXT NOT NULL CHECK(member_kind IN (
        'named','other','inactive-reserved'
    )),
    active INTEGER NOT NULL CHECK(active IN (0,1)),
    closed INTEGER NOT NULL CHECK(closed IN (0,1)),
    PRIMARY KEY(snapshot_id,event_id,market_id)
);
CREATE INDEX IF NOT EXISTS idx_structure_generation_memberships_quote_projection
ON structure_generation_memberships(
    snapshot_id,neg_risk_market_id,event_id,market_id
);
CREATE INDEX IF NOT EXISTS idx_structure_generation_memberships_quote_projection_v2
ON structure_generation_memberships(
    snapshot_id,neg_risk_market_id,market_id,event_id
);
CREATE INDEX IF NOT EXISTS idx_structure_generation_memberships_drift_scan
ON structure_generation_memberships(
    snapshot_id,market_id,event_id,neg_risk_market_id,member_kind,active,closed
);
CREATE TABLE IF NOT EXISTS structure_generation_markets (
    snapshot_id INTEGER NOT NULL REFERENCES snapshots(id),
    market_id TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    slug TEXT,
    question TEXT,
    yes_token_id TEXT,
    no_token_id TEXT,
    mid_price REAL,
    liquidity_usd REAL,
    volume_usd REAL,
    best_bid_price REAL,
    best_bid_size REAL,
    best_ask_price REAL,
    best_ask_size REAL,
    end_time_ms INTEGER,
    active INTEGER,
    closed INTEGER,
    neg_risk INTEGER,
    neg_risk_market_id TEXT,
    fetched_at_ms INTEGER NOT NULL,
    page_fetched_at_ms INTEGER,
    incomplete INTEGER NOT NULL DEFAULT 0,
    event_id TEXT,
    PRIMARY KEY(snapshot_id,market_id)
);
CREATE INDEX IF NOT EXISTS idx_structure_generation_markets_event
ON structure_generation_markets(snapshot_id,event_id);
CREATE INDEX IF NOT EXISTS idx_structure_generation_markets_quote_projection
ON structure_generation_markets(
    snapshot_id,neg_risk_market_id,event_id,market_id
);
CREATE INDEX IF NOT EXISTS idx_structure_generation_markets_quote_projection_v2
ON structure_generation_markets(
    snapshot_id,neg_risk_market_id,market_id,event_id
);
CREATE TABLE IF NOT EXISTS current_structure_generation (
    id INTEGER PRIMARY KEY CHECK(id=1),
    snapshot_id INTEGER NOT NULL UNIQUE REFERENCES snapshots(id),
    publication_id TEXT NOT NULL UNIQUE,
    validation_hash TEXT NOT NULL CHECK(length(validation_hash)=64),
    counts_json TEXT NOT NULL,
    certification_component TEXT NOT NULL CHECK(certification_component IN (
        'bounded-complete','backfill-authenticated'
    )),
    comparison_receipt_digest TEXT NOT NULL CHECK(length(comparison_receipt_digest)=64),
    switched_at_ms INTEGER NOT NULL CHECK(switched_at_ms >= 0),
    FOREIGN KEY(publication_id) REFERENCES structure_publications(publication_id)
);
"""


def _create_9b117d4_event_member_predecessor(path) -> None:
    with sqlite3.connect(path) as con:
        con.executescript(_NINE_B117D4_EVENT_MEMBER_PREDECESSOR_DDL)


def _seed_event_member_migration_business_rows(con: sqlite3.Connection) -> None:
    con.execute(
        "INSERT INTO structure_sync_windows(id,status,started_at_ms,checkpoint_at_ms) "
        "VALUES ('legacy-window','open',1,2)"
    )
    con.execute(
        "INSERT INTO structure_sync_event_staging VALUES "
        "('legacy-window','event-1','{\"id\":\"event-1\"}',NULL,0)"
    )
    con.execute(
        "INSERT INTO structure_sync_event_market_staging VALUES "
        "('legacy-window','market-1','event-1',0)"
    )
    con.execute(
        "UPDATE structure_sync_windows SET status='events_complete' "
        "WHERE id='legacy-window'"
    )
    con.execute(
        "INSERT INTO structure_sync_market_staging VALUES "
        "('legacy-window','market-1','{\"id\":\"market-1\"}',NULL,0)"
    )
    con.execute(
        "UPDATE structure_sync_windows SET status='complete' WHERE id='legacy-window'"
    )
    con.execute(
        "INSERT INTO structure_publications(publication_id,window_id,snapshot_id,status,"
        "expected_counts_json,committed_counts_json,created_at_ms,checkpoint_at_ms,"
        "published_at_ms) VALUES "
        "('publication-1','legacy-window',99,'published','{}','{}',2,2,3)"
    )
    con.execute(
        "INSERT INTO structure_generation_memberships VALUES "
        "(99,'event-1','group-1','market-1','named',1,0)"
    )
    con.execute(
        "INSERT INTO structure_generation_markets(snapshot_id,market_id,condition_id,"
        "active,closed,neg_risk,neg_risk_market_id,fetched_at_ms,event_id) VALUES "
        "(99,'market-1','condition-1',1,0,1,'group-1',2,'event-1')"
    )
    con.execute(
        "INSERT INTO current_structure_generation VALUES "
        "(1,99,'publication-1','" + "b" * 64 + "','{}','bounded-complete','"
        + "c" * 64
        + "',3)"
    )


def _event_member_migration_business_rows(
    con: sqlite3.Connection,
) -> tuple[tuple[str, tuple[tuple[object, ...], ...]], ...]:
    tables = (
        "structure_sync_event_staging",
        "structure_sync_event_market_staging",
        "structure_sync_market_staging",
        "structure_publications",
        "structure_generation_memberships",
        "structure_generation_markets",
        "current_structure_generation",
    )
    signatures = []
    for table in tables:
        columns = tuple(row[1] for row in con.execute(f"PRAGMA table_info({table})"))
        projection = ",".join(
            f'typeof("{column}"),hex(CAST("{column}" AS BLOB))' for column in columns
        )
        rows = tuple(
            con.execute(f"SELECT {projection} FROM {table} ORDER BY rowid")  # noqa: S608
        )
        signatures.append((table, rows))
    return tuple(signatures)


def test_event_member_contract_is_exact_and_immutable() -> None:
    assert STRUCTURE_EVENT_MEMBER_METADATA_CONTRACT == (
        "structure-event-member-staging-v1"
    )
    row = StructureEventMemberRow(
        "w", "e", 1, 2, "m", "m", "g", "named", True, False, "{}", "0" * 64
    )
    progress = StructureEventMemberProgress(
        "w", "", 0, 0, 0, "{}", "{}", 1, None, None
    )
    receipt = StructureEventMemberReceipt(
        "w", 1, "1" * 64, "2" * 64, STRUCTURE_EVENT_MEMBER_METADATA_CONTRACT,
        2, "3" * 64, 0, "4" * 64, "e", 2, 10, 3, "5" * 64,
    )
    for value in (row, progress, receipt):
        with pytest.raises(Exception, match="cannot assign"):
            value.window_id = "changed"  # type: ignore[misc]


def test_event_member_schema_has_canonical_columns_and_indexes(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    with sqlite3.connect(store.db_path) as con:
        columns = tuple(
            row[1]
            for row in con.execute("PRAGMA table_info(structure_sync_event_member_staging)")
        )
        assert columns == (
            "window_id", "event_id", "event_ordinal", "member_ordinal", "market_id",
            "market_sort_key", "group_id", "member_kind", "active", "closed",
            "payload_json", "payload_hash",
        )
        indexes = {
            row[1]: (
                bool(row[2]),
                tuple(
                    item[2]
                    for item in con.execute(f"PRAGMA index_info('{row[1]}')")
                ),
            )
            for row in con.execute(
                "PRAGMA index_list(structure_sync_event_member_staging)"
            )
        }
        primary_key = tuple(
            row[1]
            for row in sorted(
                con.execute("PRAGMA table_info(structure_sync_event_member_staging)"),
                key=lambda row: row[5],
            )
            if row[5]
        )
    assert primary_key == ("window_id", "event_id", "member_ordinal")
    assert (
        False,
        ("window_id", "market_sort_key", "event_id", "event_ordinal", "member_ordinal"),
    ) in indexes.values()
    assert (False, ("window_id", "event_id", "member_ordinal")) in indexes.values()
    assert (False, ("window_id", "market_id", "event_id", "member_ordinal")) in indexes.values()
    assert not any(
        unique and "market_id" in cols and "member_ordinal" not in cols
        for unique, cols in indexes.values()
    )


def test_event_member_immutable_preserves_duplicate_ordinals_and_replace_guard(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "INSERT INTO structure_sync_windows(id,status,started_at_ms,checkpoint_at_ms) "
            "VALUES ('w','open',1,1)"
        )
        member = (
            "w", "e", 0, None, "same", "same", None, None, 1, 0, "{}", "a" * 64
        )
        for ordinal in (4, 9):
            con.execute(
                "INSERT INTO structure_sync_event_member_staging VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                member[:3] + (ordinal,) + member[4:],
            )
        assert con.execute(
            "SELECT member_ordinal FROM structure_sync_event_member_staging ORDER BY member_ordinal"
        ).fetchall() == [(4,), (9,)]
        con.execute("UPDATE structure_sync_windows SET status='complete' WHERE id='w'")
        for sql in (
            "INSERT INTO structure_sync_event_member_staging VALUES "
            "('w','e',0,10,'same','same',NULL,NULL,1,0,'{}','"
            + "b" * 64
            + "')",
            "UPDATE structure_sync_event_member_staging SET payload_json='[]' WHERE window_id='w'",
            "DELETE FROM structure_sync_event_member_staging WHERE window_id='w'",
        ):
            with pytest.raises(
                sqlite3.IntegrityError,
                match="structure-event-member-staging-frozen",
            ):
                con.execute(sql)
        receipt = (
            "w", 1, "1" * 64, "2" * 64, STRUCTURE_EVENT_MEMBER_METADATA_CONTRACT,
            2, "3" * 64, 0, "4" * 64, "e", 2, 10, 3, "5" * 64,
            0, "0" * 64, "0" * 64,
        )
        receipt_insert = (
            "INSERT INTO structure_sync_event_member_receipts("
            "window_id,source_event_count,source_event_root,source_identity_hash,"
            "metadata_contract,member_row_count,member_row_root,invalid_member_count,"
            "invalid_member_root,terminal_event_cursor,terminal_member_ordinal,"
            "terminal_member_byte_offset,sealed_at_ms,receipt_digest,"
            "event_conflict_count,event_conflict_root,event_conflict_merkle_root) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        )
        con.execute(receipt_insert, receipt)
        original = con.execute("SELECT * FROM structure_sync_event_member_receipts").fetchone()
        for verb in ("INSERT", "INSERT OR REPLACE"):
            with pytest.raises(
                sqlite3.IntegrityError,
                match="structure-event-member-receipt-sealed",
            ):
                con.execute(receipt_insert.replace("INSERT", verb, 1), receipt)
        assert (
            con.execute("SELECT * FROM structure_sync_event_member_receipts").fetchone()
            == original
        )


def test_event_member_immutable_rejects_identity_and_target_authority_bypass(
    tmp_path,
) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    with sqlite3.connect(store.db_path) as con:
        con.executemany(
            "INSERT INTO structure_sync_windows(id,status,started_at_ms,checkpoint_at_ms) "
            "VALUES (?,?,1,1)",
            (("open-window", "open"), ("complete-window", "complete")),
        )
        con.execute(
            "INSERT INTO structure_sync_event_member_staging VALUES "
            "('open-window','event-1',0,4,'market-1','market-1',NULL,NULL,1,0,"
            "'{}','" + "a" * 64 + "')"
        )
        con.execute(
            "UPDATE structure_sync_event_member_staging SET payload_json=' { } ' "
            "WHERE window_id='open-window'"
        )
        assert con.execute(
            "SELECT payload_json FROM structure_sync_event_member_staging"
        ).fetchone() == (" { } ",)

        identity_updates = (
            "window_id='complete-window'",
            "event_id='event-2'",
            "member_ordinal=5",
        )
        for assignment in identity_updates:
            with pytest.raises(
                sqlite3.IntegrityError,
                match="structure-event-member-staging-frozen",
            ):
                con.execute(
                    "UPDATE structure_sync_event_member_staging SET "
                    f"{assignment} WHERE window_id='open-window'"
                )

        con.execute(
            "UPDATE structure_sync_windows SET status='complete' WHERE id='open-window'"
        )
        with pytest.raises(
            sqlite3.IntegrityError,
            match="structure-event-member-staging-frozen",
        ):
            con.execute(
                "UPDATE structure_sync_event_member_staging SET payload_json='[]' "
                "WHERE window_id='open-window'"
            )
        con.execute(
            "UPDATE structure_sync_windows SET status='published' WHERE id='open-window'"
        )
        with pytest.raises(
            sqlite3.IntegrityError,
            match="structure-event-member-staging-frozen",
        ):
            con.execute(
                "UPDATE structure_sync_event_member_staging SET payload_json='[1]' "
                "WHERE window_id='open-window'"
            )

        con.execute(
            "INSERT INTO structure_sync_windows(id,status,started_at_ms,checkpoint_at_ms) "
            "VALUES ('sealed-window','open',1,1)"
        )
        con.execute(
            "INSERT INTO structure_sync_event_member_staging VALUES "
            "('sealed-window','event-1',0,4,'market-1','market-1',NULL,NULL,1,0,"
            "'{}','" + "d" * 64 + "')"
        )
        con.execute(
            "INSERT INTO structure_sync_event_member_receipts("
            "window_id,source_event_count,source_event_root,source_identity_hash,"
            "metadata_contract,member_row_count,member_row_root,invalid_member_count,"
            "invalid_member_root,terminal_event_cursor,terminal_member_ordinal,"
            "terminal_member_byte_offset,sealed_at_ms,receipt_digest,"
            "event_conflict_count,event_conflict_root,event_conflict_merkle_root) VALUES "
            "('sealed-window',1,'" + "1" * 64 + "','" + "2" * 64
            + "','structure-event-member-staging-v1',1,'" + "3" * 64
            + "',0,'" + "4" * 64 + "','event-1',1,10,2,'" + "5" * 64
            + "',0,'" + "0" * 64 + "','" + "0" * 64 + "')"
        )
        with pytest.raises(
            sqlite3.IntegrityError,
            match="structure-event-member-staging-frozen",
        ):
            con.execute(
                "UPDATE structure_sync_event_member_staging SET payload_json='[2]' "
                "WHERE window_id='sealed-window'"
            )


@pytest.mark.parametrize("identity_column", ("window_id", "event_id", "member_ordinal"))
def test_event_member_immutable_rejects_null_identity_with_stable_error(
    tmp_path,
    identity_column,
) -> None:
    store = SQLiteStore(tmp_path / f"{identity_column}.db")
    store.init_schema()
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "INSERT INTO structure_sync_windows(id,status,started_at_ms,checkpoint_at_ms) "
            "VALUES ('open-window','open',1,1)"
        )
        con.execute(
            "INSERT INTO structure_sync_event_member_staging VALUES "
            "('open-window','event-1',0,4,'market-1','market-1',NULL,NULL,1,0,"
            "'{}','" + "a" * 64 + "')"
        )
        with pytest.raises(
            sqlite3.IntegrityError,
            match="^structure-event-member-staging-frozen$",
        ):
            con.execute(
                f"UPDATE structure_sync_event_member_staging SET {identity_column}=NULL "
                "WHERE window_id='open-window'"
            )


def test_event_member_rollback_labels_every_schema_statement() -> None:
    assert all(label is not None for label, _sql in STRUCTURE_EVENT_MEMBER_SCHEMA_STATEMENTS)


def test_event_member_migration_is_idempotent_and_schema_locked(tmp_path) -> None:
    fresh = SQLiteStore(tmp_path / "fresh.db")
    fresh.init_schema()
    migrated_path = tmp_path / "migrated.db"
    _create_9b117d4_event_member_predecessor(migrated_path)
    with sqlite3.connect(migrated_path) as con:
        _seed_event_member_migration_business_rows(con)
        assert _schema_objects(con, "structure_sync_event_member") == ()
        business_before = _event_member_migration_business_rows(con)
        sqlite_store_module._migrate_structure_event_member_schema(con)
        sqlite_store_module._migrate_structure_event_member_schema(con)
    with sqlite3.connect(fresh.db_path) as lhs, sqlite3.connect(
        migrated_path
    ) as rhs:
        assert _schema_objects(
            lhs, "structure_sync_event_member"
        ) == _schema_objects(rhs, "structure_sync_event_member")
        assert _schema_objects(
            lhs, "structure_sync_event_conflict"
        ) == _schema_objects(rhs, "structure_sync_event_conflict")
        assert _schema_objects(
            lhs, "structure_sync_event_group_truth"
        ) == _schema_objects(rhs, "structure_sync_event_group_truth")
        assert _event_member_migration_business_rows(rhs) == business_before


def test_event_member_migration_keeps_existing_large_staging_indexes(tmp_path) -> None:
    """Startup must not rebuild populated event-member staging indexes."""
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    statements: list[str] = []
    with sqlite3.connect(store.db_path) as con:
        con.set_trace_callback(statements.append)
        sqlite_store_module._migrate_structure_event_member_schema(con)

    assert not any(
        statement.startswith("DROP INDEX")
        and "idx_structure_event_member_" in statement
        for statement in statements
    )


def test_event_conflict_merkle_receipt_migration_fault_rolls_back(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "previous-amendment.db")
    store.init_schema()
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "ALTER TABLE structure_sync_event_member_receipts "
            "DROP COLUMN event_conflict_merkle_root"
        )
        schema_before = tuple(con.execute(
            "SELECT type,name,sql FROM sqlite_master ORDER BY type,name"
        ))

        def fail_here(point: str) -> None:
            if point == "after-receipt-event_conflict_merkle_root":
                raise RuntimeError(point)

        with pytest.raises(
            RuntimeError,
            match="after-receipt-event_conflict_merkle_root",
        ):
            sqlite_store_module._migrate_structure_event_member_schema(
                con,
                fault_hook=fail_here,
            )
        assert tuple(con.execute(
            "SELECT type,name,sql FROM sqlite_master ORDER BY type,name"
        )) == schema_before


@pytest.mark.parametrize(
    ("table", "fault_point"),
    [
        (
            "structure_sync_event_group_truth_staging",
            "after-group-truth-tradable_open_named_count",
        ),
        (
            "structure_sync_event_group_truth_progress",
            "after-group-progress-tradable_open_named_count",
        ),
    ],
)
def test_event_group_truth_column_migration_fault_rolls_back(
    tmp_path, table, fault_point,
) -> None:
    store = SQLiteStore(tmp_path / f"{table}.db")
    store.init_schema()
    with sqlite3.connect(store.db_path) as con:
        con.execute(f"ALTER TABLE {table} DROP COLUMN tradable_open_named_count")
        schema_before = tuple(
            con.execute("SELECT type,name,sql FROM sqlite_master ORDER BY type,name")
        )

        def fail_here(point: str) -> None:
            if point == fault_point:
                raise RuntimeError(point)

        with pytest.raises(RuntimeError, match=fault_point):
            sqlite_store_module._migrate_structure_event_member_schema(
                con,
                fault_hook=fail_here,
            )
        assert tuple(
            con.execute("SELECT type,name,sql FROM sqlite_master ORDER BY type,name")
        ) == schema_before


@pytest.mark.parametrize(
    "fault_point",
    tuple(label for label, _sql in STRUCTURE_EVENT_MEMBER_SCHEMA_STATEMENTS),
)
def test_event_member_rollback_restores_old_schema_and_rows(tmp_path, fault_point) -> None:
    predecessor_path = tmp_path / f"{fault_point}.db"
    _create_9b117d4_event_member_predecessor(predecessor_path)
    with sqlite3.connect(predecessor_path) as con:
        _seed_event_member_migration_business_rows(con)
        schema_before = tuple(con.execute(
            "SELECT type,name,sql FROM sqlite_master ORDER BY type,name"
        ))
        rows_before = _event_member_migration_business_rows(con)

        def fail_here(point: str) -> None:
            if point == fault_point:
                raise RuntimeError(point)

        with pytest.raises(RuntimeError, match=fault_point):
            sqlite_store_module._migrate_structure_event_member_schema(
                con, fault_hook=fail_here
            )
        assert tuple(con.execute(
            "SELECT type,name,sql FROM sqlite_master ORDER BY type,name"
        )) == schema_before
        assert _event_member_migration_business_rows(con) == rows_before


@pytest.mark.parametrize(
    "fault_point",
    tuple(
        label
        for label, _sql in STRUCTURE_EVENT_MEMBER_SCHEMA_STATEMENTS
        if label.endswith("-drop")
    ),
)
def test_event_member_rollback_restores_replaced_indexes_and_triggers(
    tmp_path,
    fault_point,
) -> None:
    predecessor_path = tmp_path / f"canonical-{fault_point}.db"
    _create_9b117d4_event_member_predecessor(predecessor_path)
    with sqlite3.connect(predecessor_path) as con:
        _seed_event_member_migration_business_rows(con)
        sqlite_store_module._migrate_structure_event_member_schema(con)
        schema_before = tuple(
            con.execute("SELECT type,name,sql FROM sqlite_master ORDER BY type,name")
        )
        rows_before = _event_member_migration_business_rows(con)

        def fail_here(point: str) -> None:
            if point == fault_point:
                raise RuntimeError(point)

        with pytest.raises(RuntimeError, match=fault_point):
            sqlite_store_module._migrate_structure_event_member_schema(
                con,
                fault_hook=fail_here,
            )
        assert tuple(
            con.execute("SELECT type,name,sql FROM sqlite_master ORDER BY type,name")
        ) == schema_before
        assert _event_member_migration_business_rows(con) == rows_before


def test_structure_window_commits_page_and_resumes_exact_successor_cursor(tmp_path) -> None:
    """A restart observes only a fully committed page and its opaque cursor."""
    db_path = tmp_path / "state.db"
    first = SQLiteStore(db_path)
    first.init_schema()

    window = first.begin_or_resume_structure_sync(started_at_ms=100)
    assert window["status"] == "open"
    assert window["event_cursor"] is None

    first.commit_structure_event_page(
        window_id=window["id"],
        requested_cursor=None,
        next_cursor="opaque-event-2",
        completed=False,
        events=[{"id": "event-1", "active": True, "closed": False}],
        finished_at_ms=200,
    )

    restarted = SQLiteStore(db_path)
    resumed = restarted.begin_or_resume_structure_sync(started_at_ms=300)

    assert resumed["id"] == window["id"]
    assert resumed["event_cursor"] == "opaque-event-2"
    assert resumed["event_pages"] == 1
    assert restarted.list_staged_structure_events(window["id"]) == [
        {"id": "event-1", "active": True, "closed": False}
    ]


def test_structure_writer_uses_production_busy_timeout(tmp_path, monkeypatch) -> None:
    """Concurrent Quote writes must not trip SQLite's five-second default."""
    db_path = tmp_path / "state.db"
    store = SQLiteStore(db_path)
    store.init_schema()
    real_connect = sqlite3.connect
    observed_timeouts: list[float | None] = []

    def recording_connect(*args, **kwargs):
        observed_timeouts.append(kwargs.get("timeout"))
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", recording_connect)
    store.begin_or_resume_structure_sync(started_at_ms=100)

    assert observed_timeouts[-1] == SQLITE_BUSY_TIMEOUT_S


def test_structure_window_stages_markets_only_after_event_coverage_completes(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    window = store.begin_or_resume_structure_sync(started_at_ms=100)
    store.commit_structure_event_page(
        window_id=window["id"],
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        events=[],
        finished_at_ms=200,
    )

    store.commit_structure_market_page(
        window_id=window["id"],
        requested_cursor=None,
        next_cursor="opaque-market-2",
        completed=False,
        markets=[{"id": "market-1", "active": True, "closed": False}],
        finished_at_ms=300,
    )

    resumed = SQLiteStore(tmp_path / "state.db").begin_or_resume_structure_sync(
        started_at_ms=400
    )
    assert resumed["market_cursor"] == "opaque-market-2"
    assert resumed["market_pages"] == 1


async def test_structure_worker_advances_one_event_then_one_market_page(tmp_path) -> None:
    class Gamma:
        async def fetch_active_event_page(self, cursor, limit):
            assert (cursor, limit) == (None, 100)
            return EventPage(({"id": "event-1"},), None, None, True, 10, 20)

        async def fetch_active_market_page(self, cursor, limit):
            assert (cursor, limit) == (None, 100)
            return MarketPage(({"id": "market-1"},), None, None, True, 30, 40)

    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    worker = StructureSyncWorker(gamma=Gamma(), store=store)

    assert (await worker.run_batch()).stage == "events"
    assert store.get_latest_structure_sync()["started_at_ms"] > 0
    assert (await worker.run_batch()).stage == "markets"
    assert store.get_latest_structure_sync()["status"] == "complete"


async def test_structure_worker_bounds_a_hung_market_page(tmp_path) -> None:
    """A stalled Gamma page must release the Structure producer before its parent kill."""

    class Gamma:
        async def fetch_active_event_page(self, cursor, limit):
            return EventPage((), cursor, None, True, 10, 20)

        async def fetch_active_market_page(self, cursor, limit):
            await asyncio.sleep(1)
            raise AssertionError("deadline must cancel the request")

    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    worker = StructureSyncWorker(gamma=Gamma(), store=store)

    assert (await worker.run_batch()).stage == "events"
    with pytest.raises(StructurePageDeadlineExceeded, match="structure-page-deadline"):
        await worker.run_batch(page_timeout_s=0.001)


async def test_structure_sync_yields_after_bounded_pages_without_losing_cursor(
    settings_for_test,
) -> None:
    """A long Structure window must release the producer slot for Quote."""
    store = SQLiteStore(settings_for_test.db_path)
    store.init_schema()
    cursors: list[str | None] = []

    class Gamma:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def fetch_active_event_page(self, cursor, limit):
            cursors.append(cursor)
            page_number = len(cursors)
            return EventPage(
                ({"id": f"event-{page_number}"},),
                cursor,
                f"event-{page_number + 1}",
                False,
                page_number * 10,
                page_number * 10 + 1,
            )

        async def fetch_active_market_page(self, cursor, limit):
            raise AssertionError("event coverage is intentionally incomplete")

    with patch(
        "polyarb.perception.structure_sync.GammaClient",
        return_value=Gamma(),
    ):
        result = await run_structure_sync_until_published(
            settings_for_test,
            max_pages=2,
        )

    assert result == StructureSyncCheckpoint(
        window_id=store.get_latest_structure_sync()["id"],
        stage="events",
        pages_processed=2,
    )
    assert cursors == [None, "event-2"]
    assert store.get_latest_structure_sync()["event_cursor"] == "event-3"


async def test_structure_sync_checkpoints_on_elapsed_wall_clock(
    settings_for_test,
) -> None:
    store = SQLiteStore(settings_for_test.db_path)
    store.init_schema()
    cursors: list[str | None] = []

    class Gamma:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def fetch_active_event_page(self, cursor, limit):
            cursors.append(cursor)
            page_number = len(cursors)
            return EventPage(
                ({"id": f"event-{page_number}"},),
                cursor,
                f"event-{page_number + 1}",
                False,
                page_number * 10,
                page_number * 10 + 1,
            )

        async def fetch_active_market_page(self, cursor, limit):
            raise AssertionError("event coverage is intentionally incomplete")

    with (
        patch(
            "polyarb.perception.structure_sync.GammaClient",
            return_value=Gamma(),
        ),
        patch(
            "polyarb.perception.structure_sync._monotonic",
            side_effect=[0.0, 10.0, 46.0],
        ),
    ):
        result = await run_structure_sync_until_published(
            settings_for_test,
            max_elapsed_s=45.0,
        )

    assert result == StructureSyncCheckpoint(
        window_id=store.get_latest_structure_sync()["id"],
        stage="events",
        pages_processed=2,
    )
    assert cursors == [None, "event-2"]


async def test_scheduler_schema_ready_contract_skips_child_schema_migration(
    settings_for_test,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = SQLiteStore(settings_for_test.db_path)
    store.init_schema()

    def unexpected_schema_migration(_self) -> None:
        raise AssertionError("scheduler child must trust the parent startup migration")

    monkeypatch.setattr(
        SQLiteStore,
        "init_structure_sync_schema",
        unexpected_schema_migration,
    )

    class Gamma:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def fetch_active_event_page(self, cursor, limit):
            return EventPage((), cursor, None, True, 10, 20)

        async def fetch_active_market_page(self, cursor, limit):
            return MarketPage((), cursor, None, True, 30, 40)

    with patch(
        "polyarb.perception.structure_sync.GammaClient",
        return_value=Gamma(),
    ):
        result = await run_structure_sync_until_published(
            settings_for_test,
            max_pages=1,
            schema_ready=True,
        )

    assert isinstance(result, StructureSyncCheckpoint)


async def test_bounded_slice_uses_remaining_time_for_publication(
    settings_for_test,
) -> None:
    """Completed page discovery enters the cooperative slice without a new child."""
    store = SQLiteStore(settings_for_test.db_path)
    store.init_schema()

    class Gamma:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def fetch_active_event_page(self, cursor, limit):
            return EventPage((), None, None, True, 10, 20)

        async def fetch_active_market_page(self, cursor, limit):
            return MarketPage((), None, None, True, 30, 40)

    finalizer = AsyncMock(side_effect=AssertionError("finalizer must use next slot"))
    checkpoint = structure_publication_module.StructurePublicationCheckpoint(
        stage="normalizing",
        component="events",
        rows_processed=1_000,
        cursor="event-1000",
        publication_id="publication-1",
        chunks_processed=2,
        elapsed_ms=2_000,
    )
    with (
        patch(
            "polyarb.perception.structure_sync.GammaClient",
            return_value=Gamma(),
        ),
        patch(
            "polyarb.perception.structure_sync.finalize_structure_window",
            new=finalizer,
        ),
            patch(
                "polyarb.perception.structure_sync._monotonic",
                side_effect=[0.0, 1.0, 2.0, 3.0, 3.0, 4.0, 4.0, 5.0],
        ),
        patch(
            "polyarb.perception.structure_publication.run_structure_publication_slice",
            return_value=checkpoint,
        ) as publication_slice,
    ):
        result = await run_structure_sync_until_published(
            settings_for_test,
            max_elapsed_s=45.0,
        )

    assert isinstance(result, StructureSyncCheckpoint)
    assert result.stage == "bootstrap"
    assert store.get_latest_structure_sync()["status"] == "complete"
    publication_slice.assert_not_called()
    finalizer.assert_not_awaited()


async def test_structure_worker_emits_scheduler_stage_before_remote_page_fetch(
    tmp_path, capsys
) -> None:
    class Gamma:
        async def fetch_active_event_page(self, cursor, limit):
            return EventPage((), cursor, None, True, 10, 20)

        async def fetch_active_market_page(self, cursor, limit):
            raise AssertionError("market page is not part of this batch")

    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()

    await StructureSyncWorker(gamma=Gamma(), store=store).run_batch()

    stderr = capsys.readouterr().err
    assert "snapshot-stage stage=gamma-events state=start elapsed_ms=0" in stderr
    assert "snapshot-stage stage=gamma-events state=complete elapsed_ms=" in stderr


async def test_staged_source_releases_raw_rows_as_stream_consumes_them() -> None:
    events = [{"id": "event-1"}, {"id": "event-2"}]
    markets = [{"id": "market-1"}, {"id": "market-2"}]
    source = StagedGammaSource(events, markets)

    assert events == []
    assert markets == []
    assert len(source._events) == 2
    assert len(source._markets) == 2

    event_stream = source.iter_active_events(SimpleNamespace(result=None))
    market_stream = source.iter_active_markets(SimpleNamespace(result=None))
    assert await anext(event_stream) == {"id": "event-1"}
    assert await anext(market_stream) == {"id": "market-1"}
    assert len(source._events) == 1
    assert len(source._markets) == 1


async def test_completed_window_can_stream_rows_directly_from_sqlite(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    window = store.begin_or_resume_structure_sync(started_at_ms=100)
    store.commit_structure_event_page(
        window_id=window["id"],
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        events=[{"id": "event-1"}, {"id": "event-2"}],
        finished_at_ms=200,
    )
    store.commit_structure_market_page(
        window_id=window["id"],
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        markets=[{"id": "market-1"}, {"id": "market-2"}],
        finished_at_ms=300,
    )
    source_type = getattr(structure_sync_module, "SQLiteStagedGammaSource", None)
    assert source_type is not None, "SQLite-backed staged source is missing"
    source = source_type(store, window["id"])
    event_coverage = SimpleNamespace(result=None)
    market_coverage = SimpleNamespace(result=None)

    events = [row async for row in source.iter_active_events(event_coverage)]
    markets = [row async for row in source.iter_active_markets(market_coverage)]

    assert events == [{"id": "event-1"}, {"id": "event-2"}]
    assert markets == [{"id": "market-1"}, {"id": "market-2"}]
    assert event_coverage.result.items_yielded == 2
    assert market_coverage.result.items_yielded == 2


def test_incomplete_structure_window_cannot_be_read_for_publication(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    window = store.begin_or_resume_structure_sync(started_at_ms=1)
    with pytest.raises(ValueError, match="not-complete"):
        store.read_complete_structure_sync(window["id"])


def test_rejected_cursor_rotates_window_and_preserves_failure_evidence(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    old = store.begin_or_resume_structure_sync(started_at_ms=100)
    store.commit_structure_event_page(
        window_id=old["id"],
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        events=[{"id": "event-old"}],
        finished_at_ms=200,
    )
    store.commit_structure_market_page(
        window_id=old["id"],
        requested_cursor=None,
        next_cursor="expired-cursor",
        completed=False,
        markets=[{"id": "market-old"}],
        finished_at_ms=300,
    )

    new = store.restart_structure_sync_window(
        window_id=str(old["id"]),
        restarted_at_ms=400,
        failure_reason="cursor-rejected:markets:403",
    )

    with sqlite3.connect(store.db_path) as con:
        failed = con.execute(
            "SELECT status,failure_reason,event_pages,market_pages "
            "FROM structure_sync_windows WHERE id=?",
            (old["id"],),
        ).fetchone()
        assert failed == ("failed", "cursor-rejected:markets:403", 1, 1)
        assert con.execute(
            "SELECT COUNT(*) FROM structure_sync_market_staging WHERE window_id=?",
            (old["id"],),
        ).fetchone()[0] == 1
    assert new["id"] != old["id"]
    assert new["status"] == "open"
    assert new["event_cursor"] is None
    assert new["market_cursor"] is None


def test_recovery_root_partial_migration_repairs_existing_null(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    window = store.begin_or_resume_structure_sync(started_at_ms=100)
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "UPDATE structure_sync_windows SET recovery_root_window_id=NULL WHERE id=?",
            (window["id"],),
        )

    store.init_structure_sync_schema()

    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT recovery_root_window_id FROM structure_sync_windows WHERE id=?",
            (window["id"],),
        ).fetchone() == (window["id"],)


def test_published_structure_retention_reclaims_payload_and_keeps_window_identity(
    tmp_path,
) -> None:
    db_path = tmp_path / "state.db"
    store = SQLiteStore(db_path)
    store.init_schema()
    window_ids: list[str] = []

    for index in range(3):
        with sqlite3.connect(db_path) as con:
            snapshot_id = con.execute(
                "INSERT INTO snapshots("
                "taken_at_ms,finished_at_ms,mode,market_count,"
                "market_view_published,data_product,archive_status,"
                "snapshot_status,is_valid,parquet_path"
                ") VALUES (?,?, 'full',0,1,'structure','not_requested',"
                "'degraded',1,'not-requested') RETURNING id",
                (index * 100 + 1, index * 100 + 2),
            ).fetchone()[0]
        window = store.begin_or_resume_structure_sync(started_at_ms=index * 100 + 10)
        window_id = str(window["id"])
        store.commit_structure_event_page(
            window_id=window_id,
            requested_cursor=None,
            next_cursor=None,
            completed=True,
            events=[
                {
                    "id": f"event-{index}",
                    "markets": [{"id": f"market-{index}"}],
                }
            ],
            finished_at_ms=index * 100 + 20,
        )
        store.commit_structure_market_page(
            window_id=window_id,
            requested_cursor=None,
            next_cursor=None,
            completed=True,
            markets=[{"id": f"market-{index}"}],
            finished_at_ms=index * 100 + 30,
        )
        assert store.advance_structure_event_market_backfill(
            window_id=window_id,
            max_events=10,
            max_relationships=10,
            now_ms=index * 100 + 35,
        )["completed"] is True
        store.mark_structure_sync_published(
            window_id=window_id,
            snapshot_id=int(snapshot_id),
            published_at_ms=index * 100 + 40,
        )
        window_ids.append(window_id)

    reclaimed, reclaimed_ids = store.purge_published_structure_sync_windows(
        keep_last=1,
        max_windows_per_run=1,
    )

    assert (reclaimed, reclaimed_ids) == (1, [window_ids[0]])
    with sqlite3.connect(db_path) as con:
        assert con.execute(
            "SELECT COUNT(*) FROM structure_sync_event_staging WHERE window_id=?",
            (window_ids[0],),
        ).fetchone()[0] == 0
        assert con.execute(
            "SELECT COUNT(*) FROM structure_sync_market_staging WHERE window_id=?",
            (window_ids[0],),
        ).fetchone()[0] == 0
        assert con.execute(
            "SELECT COUNT(*) FROM structure_sync_event_market_staging WHERE window_id=?",
            (window_ids[0],),
        ).fetchone()[0] == 0
        assert con.execute(
            "SELECT COUNT(*) FROM structure_sync_event_market_staging WHERE window_id=?",
            (window_ids[2],),
        ).fetchone()[0] == 1
        assert [
            row[0]
            for row in con.execute(
                "SELECT id FROM structure_sync_windows ORDER BY checkpoint_at_ms"
            )
        ] == window_ids
        assert con.execute(
            "SELECT staging_reclaimed_at_ms FROM structure_sync_windows WHERE id=?",
            (window_ids[0],),
        ).fetchone()[0] is not None
        assert con.execute(
            "SELECT staging_reclaimed_at_ms FROM structure_sync_windows WHERE id=?",
            (window_ids[1],),
        ).fetchone() == (None,)

    assert store.purge_published_structure_sync_windows(
        keep_last=1,
        max_windows_per_run=1,
    ) == (1, [window_ids[1]])
    assert store.get_latest_structure_sync()["id"] == window_ids[2]


def test_published_retention_preserves_sealed_member_receipt_authority(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "sealed-retention.db")
    store.init_schema()
    old_window_id = _seed_event_member_window(store, 2)
    _advance_event_members_until_sealed(store, old_window_id)
    assert store.structure_event_member_status(window_id=old_window_id)["sealed"] is True

    with sqlite3.connect(store.db_path) as con:
        old_snapshot_id = int(con.execute(
            "INSERT INTO snapshots(taken_at_ms,finished_at_ms,mode,market_count,"
            "is_valid,parquet_path) VALUES (10,11,'full',0,1,'old.parquet') "
            "RETURNING id"
        ).fetchone()[0])
    store.mark_structure_sync_published(
        window_id=old_window_id,
        snapshot_id=old_snapshot_id,
        published_at_ms=12,
    )

    latest_window_id = _seed_event_member_window(store, 0)
    with sqlite3.connect(store.db_path) as con:
        latest_snapshot_id = int(con.execute(
            "INSERT INTO snapshots(taken_at_ms,finished_at_ms,mode,market_count,"
            "is_valid,parquet_path) VALUES (20,21,'full',0,1,'latest.parquet') "
            "RETURNING id"
        ).fetchone()[0])
    store.mark_structure_sync_published(
        window_id=latest_window_id,
        snapshot_id=latest_snapshot_id,
        published_at_ms=22,
    )

    try:
        result = store.purge_published_structure_sync_windows(
            keep_last=1,
            max_windows_per_run=1,
        )
    except sqlite3.IntegrityError as error:
        pytest.fail(f"authenticated staging reclamation raised {error}")

    assert result == (1, [old_window_id])
    retained_status = store.structure_event_member_status(window_id=old_window_id)
    assert retained_status["sealed"] is True, retained_status
    with sqlite3.connect(store.db_path) as con:
        for table in (
            "structure_sync_event_staging",
            "structure_sync_event_market_staging",
            "structure_sync_event_metadata_staging",
            "structure_sync_event_member_staging",
            "structure_sync_event_group_truth_staging",
            "structure_sync_event_conflict_proofs",
            "structure_sync_event_conflict_merkle_nodes",
        ):
            assert con.execute(
                f"SELECT COUNT(*) FROM {table} WHERE window_id=?",  # noqa: S608
                (old_window_id,),
            ).fetchone() == (0,)
        assert con.execute(
            "SELECT COUNT(*) FROM structure_sync_event_member_receipts "
            "WHERE window_id=?",
            (old_window_id,),
        ).fetchone() == (1,)
        assert con.execute(
            "SELECT COUNT(*) FROM structure_sync_event_conflict_summaries "
            "WHERE window_id=?",
            (old_window_id,),
        ).fetchone() == (1,)


def test_failed_structure_retention_reclaims_staging_and_keeps_window(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    old = store.begin_or_resume_structure_sync(started_at_ms=100)
    store.commit_structure_event_page(
        window_id=old["id"],
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        events=[{"id": "event-old", "markets": [{"id": "market-old"}]}],
        finished_at_ms=200,
    )
    store.commit_structure_market_page(
        window_id=old["id"],
        requested_cursor=None,
        next_cursor="expired",
        completed=False,
        markets=[{"id": "market-old"}],
        finished_at_ms=300,
    )
    store.restart_structure_sync_window(
        window_id=str(old["id"]),
        restarted_at_ms=400,
        failure_reason="cursor-rejected:markets:403",
    )

    assert store.purge_failed_structure_sync_windows(
        max_windows_per_run=1
    ) == (1, [old["id"]])
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT COUNT(*) FROM structure_sync_windows WHERE id=?",
            (old["id"],),
        ).fetchone()[0] == 1
        assert con.execute(
            "SELECT staging_reclaimed_at_ms FROM structure_sync_windows WHERE id=?",
            (old["id"],),
        ).fetchone()[0] is not None
        assert con.execute(
            "SELECT COUNT(*) FROM structure_sync_event_staging WHERE window_id=?",
            (old["id"],),
        ).fetchone()[0] == 0
        assert con.execute(
            "SELECT COUNT(*) FROM structure_sync_market_staging WHERE window_id=?",
            (old["id"],),
        ).fetchone()[0] == 0
        assert con.execute(
            "SELECT COUNT(*) FROM structure_sync_event_market_staging WHERE window_id=?",
            (old["id"],),
        ).fetchone()[0] == 0
        assert con.execute(
            "SELECT COUNT(*) FROM structure_sync_windows WHERE status='open'"
        ).fetchone()[0] == 1


def test_structure_sync_window_schema_has_reclamation_marker(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()

    with sqlite3.connect(store.db_path) as con:
        columns = {
            row[1] for row in con.execute("PRAGMA table_info(structure_sync_windows)")
        }

    assert "staging_reclaimed_at_ms" in columns


async def test_rejected_cursor_restarts_once_then_rebuilds_from_first_page(
    settings_for_test,
) -> None:
    store = SQLiteStore(settings_for_test.db_path)
    store.init_schema()
    old = store.begin_or_resume_structure_sync(started_at_ms=100)
    store.commit_structure_event_page(
        window_id=old["id"],
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        events=[],
        finished_at_ms=200,
    )
    store.commit_structure_market_page(
        window_id=old["id"],
        requested_cursor=None,
        next_cursor="expired",
        completed=False,
        markets=[],
        finished_at_ms=300,
    )

    class Gamma:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def fetch_active_event_page(self, cursor, limit):
            assert cursor is None
            return EventPage((), None, None, True, 410, 420)

        async def fetch_active_market_page(self, cursor, limit):
            if cursor == "expired":
                raise PaginationCursorRejectedError("markets", 403)
            assert cursor is None
            return MarketPage((), None, None, True, 430, 440)

    result = SimpleNamespace(is_valid=False)
    with (
        patch(
            "polyarb.perception.structure_sync.GammaClient",
            return_value=Gamma(),
        ),
        patch(
            "polyarb.perception.structure_sync.finalize_structure_window",
            new=AsyncMock(return_value=result),
        ),
    ):
        assert await run_structure_sync_until_published(settings_for_test) is result

    with sqlite3.connect(store.db_path) as con:
        failed = con.execute(
            "SELECT status,failure_reason FROM structure_sync_windows WHERE id=?",
            (old["id"],),
        ).fetchone()
        assert failed == ("failed", "cursor-rejected:markets:403")
        assert con.execute(
            "SELECT COUNT(*) FROM structure_sync_windows WHERE status='complete'"
        ).fetchone()[0] == 1


async def test_structure_retry_skips_full_database_schema_migration(
    settings_for_test,
) -> None:
    """A scheduler retry must not rescan the whole production database."""
    store = SQLiteStore(settings_for_test.db_path)
    store.init_schema()

    class Gamma:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def fetch_active_event_page(self, cursor, limit):
            return EventPage((), None, None, True, 10, 20)

        async def fetch_active_market_page(self, cursor, limit):
            return MarketPage((), None, None, True, 30, 40)

    result = SimpleNamespace(is_valid=False)
    with (
        patch.object(
            SQLiteStore,
            "init_schema",
            side_effect=AssertionError("full schema migration on retry"),
        ),
        patch(
            "polyarb.perception.structure_sync.GammaClient",
            return_value=Gamma(),
        ),
        patch(
            "polyarb.perception.structure_sync.finalize_structure_window",
            new=AsyncMock(return_value=result),
        ),
    ):
        assert await run_structure_sync_until_published(settings_for_test) is result


async def test_structure_finalizer_reuses_daemon_initialized_schema(
    settings_for_test,
) -> None:
    store = SQLiteStore(settings_for_test.db_path)
    store.init_schema()
    window = store.begin_or_resume_structure_sync(started_at_ms=100)
    store.commit_structure_event_page(
        window_id=window["id"],
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        events=[],
        finished_at_ms=200,
    )
    store.commit_structure_market_page(
        window_id=window["id"],
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        markets=[],
        finished_at_ms=300,
    )
    result = SimpleNamespace(is_valid=False)
    run_snapshot = AsyncMock(return_value=result)

    with (
        patch.object(
            SQLiteStore,
            "init_schema",
            side_effect=AssertionError("full schema migration in finalizer"),
        ),
        patch.object(
            SQLiteStore,
            "read_complete_structure_sync",
            side_effect=AssertionError("completed window materialized in memory"),
        ),
        patch("polyarb.snapshot.orchestrator.run_snapshot", new=run_snapshot),
    ):
        assert (
            await finalize_structure_window(
                settings_for_test,
                window["id"],
                now_ms=400,
            )
            is result
        )

    assert run_snapshot.await_args.kwargs["schema_ready"] is True


def test_legacy_event_market_bootstrap_is_durable_and_bounded(tmp_path) -> None:
    """A killed bootstrap resumes after its last committed event, never from zero."""
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    window = store.begin_or_resume_structure_sync(started_at_ms=100)
    events = [
        {"id": f"event-{index:04d}", "markets": [{"id": f"market-{index:04d}"}]}
        for index in range(12)
    ]
    store.commit_structure_event_page(
        window_id=window["id"],
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        events=events,
        finished_at_ms=200,
    )
    store.commit_structure_market_page(
        window_id=window["id"],
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        markets=[{"id": f"market-{index:04d}"} for index in range(12)],
        finished_at_ms=300,
    )
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "DELETE FROM structure_sync_event_market_backfill_progress WHERE window_id=?",
            (window["id"],),
        )
        con.execute("DROP TRIGGER trg_structure_event_market_delete_guard")
        con.execute(
            "DELETE FROM structure_sync_event_market_staging WHERE window_id=?",
            (window["id"],),
        )
    store.init_structure_sync_schema()

    first = store.advance_structure_event_market_backfill(
        window_id=window["id"], max_events=5, max_relationships=5, now_ms=400
    )
    assert first == {
        "completed": False,
        "events_processed": 5,
        "event_cursor": "event-0004",
        "member_offset": 0,
        "relationships_processed": 5,
        "blocked": False,
        "blocked_reason": None,
    }
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT COUNT(*) FROM structure_sync_event_market_staging WHERE window_id=?",
            (window["id"],),
        ).fetchone()[0] == 5

    reopened = SQLiteStore(store.db_path)
    second = reopened.advance_structure_event_market_backfill(
        window_id=window["id"], max_events=5, max_relationships=5, now_ms=500
    )
    third = reopened.advance_structure_event_market_backfill(
        window_id=window["id"], max_events=5, max_relationships=5, now_ms=600
    )
    fourth = reopened.advance_structure_event_market_backfill(
        window_id=window["id"], max_events=5, max_relationships=5, now_ms=700
    )

    assert second == {
        "completed": False,
        "events_processed": 5,
        "event_cursor": "event-0009",
        "member_offset": 0,
        "relationships_processed": 5,
        "blocked": False,
        "blocked_reason": None,
    }
    assert third == {
        "completed": True,
        "events_processed": 2,
        "event_cursor": "event-0011",
        "member_offset": 0,
        "relationships_processed": 2,
        "blocked": False,
        "blocked_reason": None,
    }
    assert fourth == {
        "completed": True,
        "events_processed": 0,
        "event_cursor": "event-0011",
        "member_offset": 0,
        "relationships_processed": 0,
        "blocked": False,
        "blocked_reason": None,
    }
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT COUNT(*) FROM structure_sync_event_market_staging WHERE window_id=?",
            (window["id"],),
        ).fetchone()[0] == 12
        assert con.execute(
            "SELECT completed_at_ms FROM structure_sync_event_market_backfill_progress "
            "WHERE window_id=?",
            (window["id"],),
        ).fetchone() == (600,)


def test_event_market_bootstrap_invalid_json_blocks_without_advancing(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    window = store.begin_or_resume_structure_sync(started_at_ms=100)
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "INSERT INTO structure_sync_event_staging("
            "window_id,event_id,payload_json,source_cursor,source_ordinal) "
            "VALUES (?,'broken','{',NULL,NULL)",
            (window["id"],),
        )
    store.commit_structure_event_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True, events=[], finished_at_ms=200,
    )
    store.commit_structure_market_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True, markets=[], finished_at_ms=300,
    )
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "DELETE FROM structure_sync_event_market_backfill_progress WHERE window_id=?",
            (window["id"],),
        )

    result = store.advance_structure_event_market_backfill(
        window_id=window["id"], max_events=500, max_relationships=500, now_ms=400
    )
    assert result["blocked"] is True
    assert str(result["blocked_reason"]).startswith("invalid-event-json:")
    assert result["event_cursor"] == ""
    assert result["member_offset"] == 0
    assert store.advance_structure_event_market_backfill(
        window_id=window["id"], max_events=500, max_relationships=500, now_ms=500
    ) == result

    bootstrap = store.structure_generation_status()["bootstrap"]
    assert bootstrap == {
        "window_id": window["id"],
        "event_cursor": "",
        "member_offset": 0,
        "events_processed": 0,
        "relationships_processed": 0,
        "checkpoint_at_ms": 400,
        "completed_at_ms": None,
        "blocked_reason": result["blocked_reason"],
    }

    successor = store.rotate_blocked_structure_sync_window(
        window_id=window["id"], rotated_at_ms=600
    )
    assert successor["status"] == "open"
    assert successor["id"] != window["id"]
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT status,failure_reason FROM structure_sync_windows WHERE id=?",
            (window["id"],),
        ).fetchone() == ("failed", result["blocked_reason"])
        assert con.execute(
            "SELECT blocked_reason FROM structure_sync_event_market_backfill_progress "
            "WHERE window_id=?",
            (window["id"],),
        ).fetchone() == (result["blocked_reason"],)

    rotated_status = store.structure_generation_status()
    assert rotated_status["bootstrap"] == {
        "window_id": window["id"],
        "event_cursor": "",
        "member_offset": 0,
        "events_processed": 0,
        "relationships_processed": 0,
        "checkpoint_at_ms": 400,
        "completed_at_ms": None,
        "blocked_reason": result["blocked_reason"],
        "successor_window_id": successor["id"],
        "recovery_state": "rotated",
    }
    assert rotated_status["bootstrap_rotation"]["recovered"] is False
    with sqlite3.connect(store.db_path) as con:
        observation = con.execute(
            "SELECT old_window_id,event_cursor,member_offset,blocked_reason,"
            "checkpoint_at_ms,successor_window_id,rotated_at_ms,observation_digest "
            "FROM structure_bootstrap_rotation_observations"
        ).fetchone()
        assert observation[:7] == (
            window["id"],
            "",
            0,
            result["blocked_reason"],
            400,
            successor["id"],
            600,
        )
        assert len(observation[7]) == 64
        with pytest.raises(sqlite3.IntegrityError, match="bootstrap-rotation-append-only"):
            con.execute("UPDATE structure_bootstrap_rotation_observations SET member_offset=1")

    assert store.purge_failed_structure_sync_windows() == (1, [window["id"]])
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT old_window_id,observation_digest FROM "
            "structure_bootstrap_rotation_observations"
        ).fetchone() == (window["id"], observation[7])

    restarted = store.restart_structure_sync_window(
        window_id=successor["id"],
        restarted_at_ms=650,
        failure_reason="cursor-rejected:events:400",
    )
    store.commit_structure_event_page(
        window_id=restarted["id"], requested_cursor=None, next_cursor=None,
        completed=True, events=[{"id": "recovered", "markets": []}],
        finished_at_ms=700,
    )
    store.commit_structure_market_page(
        window_id=restarted["id"], requested_cursor=None, next_cursor=None,
        completed=True, markets=[], finished_at_ms=800,
    )
    recovered = store.advance_structure_event_market_backfill(
        window_id=restarted["id"], max_events=10, max_relationships=10, now_ms=900
    )
    assert recovered["completed"] is True
    recovered_status = store.structure_generation_status()
    assert recovered_status["bootstrap"] is None
    assert recovered_status["bootstrap_rotation"]["recovered"] is True
    with sqlite3.connect(store.db_path) as con:
        receipt = con.execute(
            "SELECT recovery_root_window_id,successful_window_id,"
            "window_checkpoint_at_ms,completed_at_ms,receipt_digest FROM "
            "structure_bootstrap_recovery_receipts"
        ).fetchone()
        assert receipt[:4] == (window["id"], restarted["id"], 800, 900)
        assert len(receipt[4]) == 64
        con.execute(
            "INSERT INTO snapshots(id,taken_at_ms,finished_at_ms,mode,market_count,"
            "is_valid,parquet_path) VALUES (1,1,2,'full',0,1,'one.parquet')"
        )
    store.mark_structure_sync_published(
        window_id=restarted["id"], snapshot_id=1, published_at_ms=1_000
    )
    newer = store.begin_or_resume_structure_sync(started_at_ms=1_100)
    store.commit_structure_event_page(
        window_id=newer["id"], requested_cursor=None, next_cursor=None,
        completed=True, events=[], finished_at_ms=1_200,
    )
    store.commit_structure_market_page(
        window_id=newer["id"], requested_cursor=None, next_cursor=None,
        completed=True, markets=[], finished_at_ms=1_300,
    )
    assert store.advance_structure_event_market_backfill(
        window_id=newer["id"], max_events=1, max_relationships=1, now_ms=1_400
    )["completed"] is True
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "INSERT INTO snapshots(id,taken_at_ms,finished_at_ms,mode,market_count,"
            "is_valid,parquet_path) VALUES (2,3,4,'full',0,1,'two.parquet')"
        )
    store.mark_structure_sync_published(
        window_id=newer["id"], snapshot_id=2, published_at_ms=1_500
    )
    assert store.purge_published_structure_sync_windows() == (1, [restarted["id"]])
    after_purge = store.structure_generation_status()
    assert after_purge["bootstrap"] is None
    assert after_purge["bootstrap_rotation"]["recovered"] is True
    with sqlite3.connect(store.db_path) as con:
        with pytest.raises(sqlite3.IntegrityError, match="bootstrap-recovery-append-only"):
            con.execute(
                "UPDATE structure_bootstrap_recovery_receipts SET completed_at_ms=901"
            )
        con.execute("DROP TRIGGER trg_structure_bootstrap_recovery_update")
        con.execute(
            "UPDATE structure_bootstrap_recovery_receipts SET receipt_digest=?",
            ("0" * 64,),
        )
    corrupt = store.structure_generation_status()
    assert corrupt["bootstrap"]["blocked_reason"] == (
        "bootstrap-recovery-receipt-invalid"
    )
    assert corrupt["bootstrap_rotation"]["recovered"] is False


@pytest.mark.parametrize("corruption", ["digest", "member-offset"])
def test_rotation_status_rejects_unauthenticated_observation(
    tmp_path, corruption: str
) -> None:
    from polyarb.storage.sqlite_store import _bootstrap_rotation_digest

    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    root = store.begin_or_resume_structure_sync(started_at_ms=100)
    member_offset: object = "bad" if corruption == "member-offset" else 0
    digest = _bootstrap_rotation_digest(
        recovery_root_window_id=root["id"],
        old_window_id="old-window",
        event_cursor="",
        member_offset=member_offset,  # type: ignore[arg-type]
        blocked_reason="invalid-event-json:broken",
        checkpoint_at_ms=200,
        successor_window_id=root["id"],
        rotated_at_ms=300,
    )
    if corruption == "digest":
        digest = "0" * 64
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "INSERT INTO structure_bootstrap_rotation_observations("
            "recovery_root_window_id,old_window_id,event_cursor,member_offset,"
            "blocked_reason,checkpoint_at_ms,successor_window_id,rotated_at_ms,"
            "observation_digest) VALUES (?,?,?,?,?,?,?,?,?)",
            (
                root["id"], "old-window", "", member_offset,
                "invalid-event-json:broken", 200, root["id"], 300, digest,
            ),
        )

    status = store.structure_generation_status()
    assert status["bootstrap"]["blocked_reason"] == (
        "bootstrap-rotation-evidence-invalid"
    )
    assert status["bootstrap_rotation"]["authenticated"] is False
    assert status["bootstrap_rotation"]["recovered"] is False


def test_fresh_window_binds_bootstrap_to_final_complete_identity(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    window = store.begin_or_resume_structure_sync(started_at_ms=100)
    store.commit_structure_event_page(
        window_id=window["id"], requested_cursor=None, next_cursor="event-page-2",
        completed=False,
        events=[{"id": "z-event", "markets": [{"id": "market-z"}]}],
        finished_at_ms=200,
    )
    store.commit_structure_event_page(
        window_id=window["id"], requested_cursor="event-page-2", next_cursor=None,
        completed=True,
        events=[{"id": "a-event", "markets": [{"id": "market-a"}]}],
        finished_at_ms=300,
    )
    store.commit_structure_market_page(
        window_id=window["id"], requested_cursor=None, next_cursor="market-page-2",
        completed=False, markets=[{"id": "market-z"}], finished_at_ms=400,
    )
    store.commit_structure_market_page(
        window_id=window["id"], requested_cursor="market-page-2", next_cursor=None,
        completed=True, markets=[{"id": "market-a"}], finished_at_ms=500,
    )

    first = store.advance_structure_event_market_backfill(
        window_id=window["id"], max_events=1, max_relationships=1, now_ms=600
    )
    second = store.advance_structure_event_market_backfill(
        window_id=window["id"], max_events=1, max_relationships=1, now_ms=700
    )

    assert first["event_cursor"] == "a-event"
    assert second["completed"] is True
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT window_checkpoint_at_ms FROM "
            "structure_sync_event_market_backfill_progress WHERE window_id=?",
            (window["id"],),
        ).fetchone() == (500,)
        assert con.execute(
            "SELECT event_id,market_id FROM structure_sync_event_market_staging "
            "WHERE window_id=? ORDER BY event_id", (window["id"],),
        ).fetchall() == [("a-event", "market-a"), ("z-event", "market-z")]


def test_fresh_window_malformed_member_blocks_bounded_bootstrap(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    window = store.begin_or_resume_structure_sync(started_at_ms=100)
    store.commit_structure_event_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True,
        events=[{"id": "broken", "markets": [{"slug": "missing-id"}]}],
        finished_at_ms=200,
    )
    store.commit_structure_market_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True, markets=[], finished_at_ms=300,
    )

    result = store.advance_structure_event_market_backfill(
        window_id=window["id"], max_events=1, max_relationships=1, now_ms=400
    )

    assert result["blocked"] is True
    assert result["blocked_reason"] == "invalid-event-market:broken"


def test_oversized_event_payload_blocks_before_json_materialization(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from polyarb.storage import sqlite_store as store_module

    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    window = store.begin_or_resume_structure_sync(started_at_ms=100)
    store.commit_structure_event_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True,
        events=[{
            "id": "oversized",
            "title": "x" * STRUCTURE_EVENT_PAYLOAD_MAX_BYTES,
            "markets": [{"id": "market-1"}],
        }],
        finished_at_ms=200,
    )
    store.commit_structure_market_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True, markets=[{"id": "market-1"}], finished_at_ms=300,
    )
    calls = 0
    real_loads = store_module.json.loads

    def guarded_loads(payload):
        nonlocal calls
        calls += 1
        return real_loads(payload)

    monkeypatch.setattr(store_module.json, "loads", guarded_loads)
    result = store.advance_structure_event_market_backfill(
        window_id=window["id"], max_events=1, max_relationships=1, now_ms=400
    )

    assert result["blocked"] is True
    assert str(result["blocked_reason"]).startswith("event-payload-too-large:oversized:")
    assert calls == 0
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT COUNT(*) FROM structure_sync_event_market_staging WHERE window_id=?",
            (window["id"],),
        ).fetchone() == (0,)


def test_bootstrap_total_payload_budget_stops_before_materializing_next_row(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from polyarb.storage import sqlite_store as store_module

    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    window = store.begin_or_resume_structure_sync(started_at_ms=100)
    payload_padding = "x" * 800_000
    store.commit_structure_event_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True,
        events=[
            {"id": f"event-{index}", "title": payload_padding, "markets": []}
            for index in range(5)
        ],
        finished_at_ms=200,
    )
    store.commit_structure_market_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True, markets=[], finished_at_ms=300,
    )
    decoded_bytes = 0
    real_loads = store_module.json.loads

    def measured_loads(payload):
        nonlocal decoded_bytes
        decoded_bytes += len(str(payload).encode())
        return real_loads(payload)

    monkeypatch.setattr(store_module.json, "loads", measured_loads)
    with pytest.raises(ValueError, match="invalid-structure-event-market-backfill"):
        store.advance_structure_event_market_backfill(
            window_id=window["id"],
                max_events=500,
                max_relationships=500,
            max_payload_bytes=STRUCTURE_EVENT_PAYLOAD_MAX_BYTES - 1,
            now_ms=350,
        )
    first = store.advance_structure_event_market_backfill(
        window_id=window["id"],
        max_events=500,
        max_relationships=500,
        max_payload_bytes=1_700_000,
        now_ms=400,
    )

    assert first["completed"] is False
    assert first["events_processed"] == 2
    assert decoded_bytes <= 1_700_000
    second = store.advance_structure_event_market_backfill(
        window_id=window["id"],
        max_events=500,
        max_relationships=500,
        max_payload_bytes=1_700_000,
        now_ms=500,
    )
    assert second["event_cursor"] > first["event_cursor"]


async def test_scheduler_path_rotates_blocked_bootstrap_window(
    settings_for_test,
) -> None:
    store = SQLiteStore(settings_for_test.db_path)
    store.init_schema()
    window = store.begin_or_resume_structure_sync(started_at_ms=100)
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "INSERT INTO structure_sync_event_staging("
            "window_id,event_id,payload_json,source_cursor,source_ordinal) "
            "VALUES (?,'broken','{',NULL,NULL)",
            (window["id"],),
        )
    store.commit_structure_event_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True, events=[], finished_at_ms=200,
    )
    store.commit_structure_market_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True, markets=[], finished_at_ms=300,
    )
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "DELETE FROM structure_sync_event_market_backfill_progress WHERE window_id=?",
            (window["id"],),
        )

    with pytest.raises(ValueError, match="structure-bootstrap-window-rotated"):
        await run_structure_sync_until_published(
            settings_for_test,
            max_elapsed_s=45,
            max_publication_rows=500,
        )

    latest = store.get_latest_structure_sync()
    assert latest is not None and latest["status"] == "open"
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT status FROM structure_sync_windows WHERE id=?", (window["id"],)
        ).fetchone() == ("failed",)


def test_event_market_bootstrap_bounds_one_huge_event_and_resumes_offset(
    tmp_path,
) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    window = store.begin_or_resume_structure_sync(started_at_ms=100)
    store.commit_structure_event_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True,
        events=[
            {
                "id": "huge-event",
                "markets": [{"id": f"market-{index}"} for index in range(7)],
            }
        ], finished_at_ms=200,
    )
    store.commit_structure_market_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True,
        markets=[{"id": f"market-{index}"} for index in range(7)],
        finished_at_ms=300,
    )
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "DELETE FROM structure_sync_event_market_backfill_progress WHERE window_id=?",
            (window["id"],),
        )
        con.execute("DROP TRIGGER trg_structure_event_market_delete_guard")
        con.execute(
            "DELETE FROM structure_sync_event_market_staging WHERE window_id=?",
            (window["id"],),
        )
    store.init_structure_sync_schema()

    offsets = []
    for now_ms in (400, 500, 600, 700):
        result = SQLiteStore(store.db_path).advance_structure_event_market_backfill(
            window_id=window["id"],
            max_events=10,
            max_relationships=2,
            now_ms=now_ms,
        )
        assert result["relationships_processed"] <= 2
        offsets.append((result["event_cursor"], result["member_offset"]))

    assert offsets == [
        ("huge-event", 2),
        ("huge-event", 4),
        ("huge-event", 6),
        ("huge-event", 0),
    ]
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT COUNT(*) FROM structure_sync_event_market_staging WHERE window_id=?",
            (window["id"],),
        ).fetchone() == (7,)


def test_bootstrap_and_source_certification_keysets_use_indexes(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    with sqlite3.connect(store.db_path) as con:
        event_plan = con.execute(
            "EXPLAIN QUERY PLAN SELECT event_id,payload_json FROM "
            "structure_sync_event_staging WHERE window_id=? AND event_id>? "
            "ORDER BY event_id LIMIT ?",
            ("window", "", 500),
        ).fetchall()
        market_plan = con.execute(
            "EXPLAIN QUERY PLAN SELECT market_id,payload_json FROM "
            "structure_sync_market_staging WHERE window_id=? AND market_id>? "
            "ORDER BY market_id LIMIT ?",
            ("window", "", 500),
        ).fetchall()

    for plan in (event_plan, market_plan):
        detail = " ".join(str(row[3]).upper() for row in plan)
        assert "SEARCH" in detail
        assert "TEMP B-TREE" not in detail


def test_complete_structure_staging_is_database_frozen(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    window = store.begin_or_resume_structure_sync(started_at_ms=100)
    store.commit_structure_event_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True,
        events=[{"id": "event-1", "markets": [{"id": "market-1"}]}],
        finished_at_ms=200,
    )
    store.commit_structure_market_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True, markets=[{"id": "market-1"}], finished_at_ms=300,
    )
    assert store.advance_structure_event_market_backfill(
        window_id=window["id"], max_events=10, max_relationships=10, now_ms=350
    )["completed"] is True

    statements = (
        (
            "UPDATE structure_sync_event_staging SET payload_json='{}' "
            "WHERE window_id=?",
            "structure-event-staging-frozen",
        ),
        (
            "DELETE FROM structure_sync_market_staging WHERE window_id=?",
            "structure-market-staging-frozen",
        ),
        (
            "UPDATE structure_sync_event_market_staging SET source_ordinal=9 "
            "WHERE window_id=?",
            "structure-event-market-staging-frozen",
        ),
    )
    with sqlite3.connect(store.db_path) as con:
        for sql, reason in statements:
            with pytest.raises(sqlite3.IntegrityError, match=reason):
                con.execute(sql, (window["id"],))


def test_bootstrap_cursor_commit_rejects_window_identity_drift(
    tmp_path, monkeypatch
) -> None:
    from polyarb.storage import sqlite_store as store_module

    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    window = store.begin_or_resume_structure_sync(started_at_ms=100)
    store.commit_structure_event_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True,
        events=[{"id": "event-1", "markets": [{"id": "market-1"}]}],
        finished_at_ms=200,
    )
    store.commit_structure_market_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True, markets=[{"id": "market-1"}], finished_at_ms=300,
    )
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "DELETE FROM structure_sync_event_market_backfill_progress WHERE window_id=?",
            (window["id"],),
        )
        con.execute("DROP TRIGGER trg_structure_event_market_delete_guard")
        con.execute(
            "DELETE FROM structure_sync_event_market_staging WHERE window_id=?",
            (window["id"],),
        )
    store.init_structure_sync_schema()
    real_loads = store_module.json.loads
    changed = False

    def mutate_identity(payload):
        nonlocal changed
        result = real_loads(payload)
        if not changed:
            changed = True
            with sqlite3.connect(store.db_path) as con:
                con.execute(
                    "UPDATE structure_sync_windows SET checkpoint_at_ms=301 WHERE id=?",
                    (window["id"],),
                )
        return result

    monkeypatch.setattr(store_module.json, "loads", mutate_identity)
    with pytest.raises(ValueError, match="window-identity-drift"):
        store.advance_structure_event_market_backfill(
            window_id=window["id"],
            max_events=5,
            max_relationships=5,
            now_ms=400,
        )
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT COUNT(*) FROM structure_sync_event_market_staging WHERE window_id=?",
            (window["id"],),
        ).fetchone() == (0,)


def test_structure_child_schema_init_never_scans_legacy_staging(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    statements: list[str] = []
    real_connect = store._connect_writer

    def traced_connect():
        con = real_connect()
        con.set_trace_callback(statements.append)
        return con

    with patch.object(store, "_connect_writer", side_effect=traced_connect):
        store.init_structure_sync_schema()

    normalized = [" ".join(statement.lower().split()) for statement in statements]
    assert not any(
        statement.startswith("update structure_sync_event_staging")
        or statement.startswith("update structure_sync_market_staging")
        or statement.startswith("insert or ignore into structure_sync_event_market_staging")
        for statement in normalized
    )


async def test_completed_legacy_window_checkpoints_bootstrap_before_publication(
    settings_for_test,
) -> None:
    """No-publication startup commits one bounded migration slice and exits cleanly."""
    store = SQLiteStore(settings_for_test.db_path)
    store.init_schema()
    window = store.begin_or_resume_structure_sync(started_at_ms=100)
    store.commit_structure_event_page(
        window_id=window["id"],
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        events=[
            {"id": f"event-{index}", "markets": [{"id": f"market-{index}"}]}
            for index in range(3)
        ],
        finished_at_ms=200,
    )
    store.commit_structure_market_page(
        window_id=window["id"],
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        markets=[{"id": f"market-{index}"} for index in range(3)],
        finished_at_ms=300,
    )
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "DELETE FROM structure_sync_event_market_backfill_progress WHERE window_id=?",
            (window["id"],),
        )
        con.execute("DROP TRIGGER trg_structure_event_market_delete_guard")
        con.execute(
            "DELETE FROM structure_sync_event_market_staging WHERE window_id=?",
            (window["id"],),
        )
    store.init_structure_sync_schema()
    store.advance_structure_event_member_staging_chunk(window_id=str(window["id"]))

    class Gamma:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

    with patch(
        "polyarb.perception.structure_sync.GammaClient",
        return_value=Gamma(),
    ):
        result = await run_structure_sync_until_published(
            settings_for_test,
            max_elapsed_s=45.0,
            max_publication_rows=2,
        )

    assert isinstance(result, StructureSyncCheckpoint)
    assert result.stage == "bootstrap"
    assert store.get_latest_structure_publication() is None
    assert store.structure_event_member_status(
        window_id=str(window["id"])
    )["sealed"] is False


def test_publication_cannot_begin_before_relationship_bootstrap_completes(
    settings_for_test,
) -> None:
    store = SQLiteStore(settings_for_test.db_path)
    store.init_schema()
    window = store.begin_or_resume_structure_sync(started_at_ms=100)
    store.commit_structure_event_page(
        window_id=window["id"],
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        events=[{"id": "event-1", "markets": [{"id": "market-1"}]}],
        finished_at_ms=200,
    )
    store.commit_structure_market_page(
        window_id=window["id"],
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        markets=[{"id": "market-1"}],
        finished_at_ms=300,
    )
    store.advance_structure_event_member_staging_chunk(window_id=str(window["id"]))

    with pytest.raises(ValueError, match="structure-bootstrap-incomplete"):
        store.begin_structure_publication(
            window_id=window["id"],
            snapshot_metadata={
                "snapshot_id": 1,
                "taken_at_ms": 400,
                "mode": "full",
                "data_product": "structure",
                "expected_counts": {
                    component: 0
                    for component in (
                        "events",
                        "event_tags",
                        "memberships",
                        "group_truth",
                        "markets",
                        "issues",
                    )
                },
            },
            now_ms=400,
        )

    assert store.advance_structure_event_market_backfill(
        window_id=window["id"],
        max_events=500,
        max_relationships=500,
        now_ms=500,
    )["completed"] is True
    assert store.structure_event_market_backfill_complete(window["id"]) is True


def test_event_member_scanner_resumes_without_committed_prefix(monkeypatch) -> None:
    payload = json.dumps({
        "note": 'escaped \\"markets\\"',
        "nested": {"markets": [{"id": "decoy"}]},
        "markets": [{"id": f"m-{i}"} for i in range(1200)],
    }, separators=(",", ":"))
    first = decode_event_member_batch(
        payload, member_ordinal=0, member_byte_offset=0, limit=500
    )
    calls = 0
    decoder = json.JSONDecoder()

    class Decoder:
        def raw_decode(self, value, offset):
            nonlocal calls
            calls += 1
            assert len(value[:offset].encode()) >= first.next_byte_offset
            return decoder.raw_decode(value, offset)

    monkeypatch.setattr(member_module, "_DECODER", Decoder())
    monkeypatch.setattr(
        member_module, "_locate_markets_array",
        lambda _payload: pytest.fail("committed prefix rescanned"),
    )
    second = decode_event_member_batch(
        payload, member_ordinal=first.next_member_ordinal,
        member_byte_offset=first.next_byte_offset, limit=500,
    )
    assert (calls, second.next_member_ordinal) == (500, 1000)


@pytest.mark.parametrize("payload", [
    "[]", '{"markets":null}', '{"markets":[{},]}',
    '{"markets":[]} trailing', '{"markets":[],"markets":[]}',
    '{"markets":[{} {}]}',
])
def test_event_member_scanner_rejects_malformed(payload) -> None:
    with pytest.raises(ValueError):
        decode_event_member_batch(
            payload, member_ordinal=0, member_byte_offset=0, limit=500
        )


def test_event_member_extraction_is_canonical_and_nullable() -> None:
    member = {"id": " padded ", "active": 1, "closed": "false"}
    row = extract_structure_event_member_row(
        window_id="w", event_id="e", event_ordinal=1,
        member_ordinal=2, member=member,
    )
    assert (row.market_id, row.market_sort_key, row.group_id,
            row.member_kind, row.active, row.closed) == (
        None, "", None, None, None, None,
    )
    assert row.payload_json == json.dumps(
        member, sort_keys=True, separators=(",", ":")
    )
    assert row.payload_hash == hashlib.sha256(row.payload_json.encode()).hexdigest()


@pytest.mark.parametrize("field,value", [
    ("id", None), ("id", ""), ("id", " padded "), ("id", 7),
    ("active", None), ("active", "true"), ("active", 1),
    ("closed", None), ("closed", "false"), ("closed", 0),
    ("negRiskOther", None), ("negRiskOther", "false"), ("negRiskOther", 0),
])
def test_event_member_extraction_rejects_null_blank_padded_and_wrong_types(
    field, value,
) -> None:
    member = {
        "id": "m", "active": True, "closed": False, "negRiskOther": False,
        field: value,
    }
    row = extract_structure_event_member_row(
        window_id="w", event_id="e", event_ordinal=1, member_ordinal=0,
        member=member, event_group_id="g",
    )
    if field == "id":
        assert (row.market_id, row.market_sort_key) == (None, "")
    elif field == "active":
        assert (row.active, row.member_kind) == (None, None)
    elif field == "closed":
        assert (row.closed, row.member_kind) == (None, None)
    else:
        assert row.member_kind is None


@pytest.mark.parametrize("group", [None, "", " padded ", 7])
def test_event_member_extraction_rejects_invalid_parent_group(group) -> None:
    row = extract_structure_event_member_row(
        window_id="w", event_id="e", event_ordinal=1, member_ordinal=0,
        member={"id": "m", "negRiskMarketID": "nested", "memberKind": "forged",
                "active": True, "closed": False, "negRiskOther": False},
        event_group_id=group,
    )
    assert row.group_id is None
    assert row.member_kind == "named"


def _seed_event_member_window(store: SQLiteStore, count: int) -> str:
    window_id = str(store.begin_or_resume_structure_sync(started_at_ms=1)["id"])
    event = {"id": "e", "negRiskMarketID": "g", "markets": [
        {"id": f"m-{i}", "negRiskMarketID": "nested-is-not-authority",
         "memberKind": "untrusted", "negRiskOther": False,
         "active": True, "closed": False}
        for i in range(count)
    ]}
    store.commit_structure_event_page(
        window_id=window_id, requested_cursor=None, next_cursor=None,
        completed=True, events=[event], finished_at_ms=2,
    )
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "UPDATE structure_sync_windows SET status='complete' WHERE id=?",
            (window_id,),
        )
        con.execute(
            "INSERT INTO structure_sync_event_market_backfill_progress("
            "window_id,window_checkpoint_at_ms,checkpoint_at_ms,completed_at_ms) "
            "VALUES (?,?,?,?)",
            (window_id, 2, 2, 2),
        )
    return window_id


@pytest.mark.parametrize("limit", [1, 17, 500])
@pytest.mark.parametrize(
    ("member", "expected_active_named", "expected_tradable", "expected_quality"),
    [
        (
            {"id": "m", "negRiskOther": False, "active": True, "closed": False},
            1, 1, "complete-supported",
        ),
        (
            {"id": "m", "negRiskOther": False, "active": True, "closed": True},
            1, 0, "complete-unsupported",
        ),
        (
            {"id": "m", "negRiskOther": False, "active": False, "closed": False},
            0, 0, "complete-unsupported",
        ),
        (
            {"id": "m", "negRiskOther": True, "active": True, "closed": False},
            0, 0, "complete-unsupported",
        ),
    ],
    ids=["named-open", "named-closed", "named-inactive", "other-open"],
)
def test_source_group_truth_matches_raw_generation_tradability_across_restarts(
    tmp_path,
    limit,
    member,
    expected_active_named,
    expected_tradable,
    expected_quality,
) -> None:
    store = SQLiteStore(tmp_path / f"truth-{limit}.db")
    store.init_schema()
    event = {
        "id": "e",
        "negRisk": True,
        "enableNegRisk": True,
        "negRiskAugmented": False,
        "negRiskMarketID": "g",
        "markets": [member],
    }
    window_id = str(store.begin_or_resume_structure_sync(started_at_ms=1)["id"])
    store.commit_structure_event_page(
        window_id=window_id,
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        events=[event],
        finished_at_ms=2,
    )
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "UPDATE structure_sync_windows SET status='complete' WHERE id=?",
            (window_id,),
        )
        con.execute(
            "INSERT INTO structure_sync_event_market_backfill_progress("
            "window_id,window_checkpoint_at_ms,checkpoint_at_ms,completed_at_ms) "
            "VALUES (?,?,?,?)",
            (window_id, 2, 2, 2),
        )
    phases = []
    for _ in range(16):
        store = SQLiteStore(store.db_path)
        result = store.advance_structure_event_member_staging_chunk(
            window_id=window_id, limit=limit,
        )
        assert result.get("reason") is None
        assert result.get("failure_reason") is None
        assert int(result["rows_written"]) <= limit
        phases.append(str(result["state"]))
        if result.get("sealed") is True:
            break
    else:
        pytest.fail(f"source group truth did not seal: {phases}")
    canonical = project_legacy_compatible_event(
        event,
        event_source_ordinal=1,
        complete_market_ids=frozenset({"m"}),
    ).truths[0]
    with sqlite3.connect(store.db_path) as con:
        source = con.execute(
            "SELECT expected_member_count,active_named_count,"
            "tradable_open_named_count,membership_hash,quality,reason FROM "
            "structure_sync_event_group_truth_staging WHERE window_id=?",
            (window_id,),
        ).fetchone()
    assert source == (
        canonical.expected_member_count,
        expected_active_named,
        expected_tradable,
        canonical.membership_hash,
        expected_quality,
        canonical.reason,
    )
    assert canonical.active_named_count == expected_active_named
    assert canonical.quality == expected_quality


def test_event_member_derivation_is_bounded_500_500_200(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "members.db")
    store.init_schema()
    window_id = _seed_event_member_window(store, 1200)
    runs, counters = [], []
    for _ in range(3):
        seen = []
        runs.append(store.advance_structure_event_member_staging_chunk(
            window_id=window_id, inspection_callback=seen.append
        ))
        counters.append(len(seen))
        store = SQLiteStore(store.db_path)
    assert [(r["rows_written"], r["member_ordinal"], r["complete"])
            for r in runs] == [(500, 499, False), (500, 999, False),
                               (200, 1199, False)]
    assert counters == [500, 500, 200]
    sealing = _advance_event_members_until_sealed(
        store, window_id, max_chunks=8,
    )
    assert [run["state"] for run in sealing] == [
        "deriving-group-truth", "deriving-group-truth",
        "sealing-conflicts", "sealed",
    ]
    assert store.structure_event_member_status(window_id=window_id)["sealed"] is True
    with sqlite3.connect(store.db_path) as con:
        assert [r[0] for r in con.execute(
            "SELECT member_ordinal FROM structure_sync_event_member_staging "
            "ORDER BY member_ordinal"
        )] == list(range(1200))


def test_event_member_1200_full_bounded_counters_and_reopen_boundaries(
    tmp_path, monkeypatch,
) -> None:
    store = SQLiteStore(tmp_path / "member-counters.db")
    store.init_schema()
    window_id = _seed_event_member_window(store, 1200)
    decoder = json.JSONDecoder()
    raw_decode_calls = 0
    whole_event_loads = 0
    candidate_queries = 0
    original_loads = sqlite_store_module.json.loads
    original_connect = SQLiteStore._connect_writer

    class CountingDecoder:
        def raw_decode(self, value, offset):
            nonlocal raw_decode_calls
            raw_decode_calls += 1
            return decoder.raw_decode(value, offset)

    def counting_loads(value, *args, **kwargs):
        nonlocal whole_event_loads
        if isinstance(value, str) and '"markets"' in value:
            whole_event_loads += 1
        return original_loads(value, *args, **kwargs)

    def traced_connect(instance):
        connection = original_connect(instance)

        def trace(statement):
            nonlocal candidate_queries
            if statement.startswith("SELECT event.event_id"):
                candidate_queries += 1

        connection.set_trace_callback(trace)
        return connection

    monkeypatch.setattr(member_module, "_DECODER", CountingDecoder())
    monkeypatch.setattr(sqlite_store_module.json, "loads", counting_loads)
    monkeypatch.setattr(SQLiteStore, "_connect_writer", traced_connect)
    decode_deltas, candidate_deltas, inspected = [], [], []
    for _ in range(3):
        store = SQLiteStore(store.db_path)  # reopen before each CAS
        before_decode, before_candidates = raw_decode_calls, candidate_queries
        seen: list[int] = []
        store.advance_structure_event_member_staging_chunk(
            window_id=window_id, limit=500, inspection_callback=seen.append,
        )
        store = SQLiteStore(store.db_path)  # reopen after each CAS
        decode_deltas.append(raw_decode_calls - before_decode)
        candidate_deltas.append(candidate_queries - before_candidates)
        inspected.append(len(seen))
    assert decode_deltas == inspected == [500, 500, 200]
    assert max(candidate_deltas) <= 500
    assert whole_event_loads == 0
    sealing = _advance_event_members_until_sealed(
        store, window_id, max_chunks=8,
    )
    assert [run["state"] for run in sealing][-1] == "sealed"
    assert store.structure_event_member_status(window_id=window_id)["sealed"] is True


@pytest.mark.parametrize("fault", ["write", "progress", "receipt"])
def test_event_member_cas_fault_is_atomic(tmp_path, fault) -> None:
    store = SQLiteStore(tmp_path / f"{fault}.db")
    store.init_schema()
    window_id = _seed_event_member_window(store, 2)
    if fault == "receipt":
        phases = []
        for _ in range(4):
            result = store.advance_structure_event_member_staging_chunk(
                window_id=window_id,
            )
            assert result.get("reason") is None
            assert result.get("failure_reason") is None
            phases.append(result["state"])
            if result["state"] == "sealing-conflicts":
                break
        assert phases == ["deriving-group-truth", "sealing-conflicts"]
    table = {"write": "structure_sync_event_member_staging",
             "progress": "structure_sync_event_member_progress",
             "receipt": "structure_sync_event_member_receipts"}[fault]
    operation = "UPDATE" if fault == "progress" else "INSERT"
    with sqlite3.connect(store.db_path) as con:
        con.execute(f"CREATE TRIGGER injected BEFORE {operation} ON {table} "
                    "BEGIN SELECT RAISE(ABORT,'injected-fault'); END")
        before_progress = con.execute(
            "SELECT * FROM structure_sync_event_member_progress WHERE window_id=?",
            (window_id,),
        ).fetchone()
        before_member_count = con.execute(
            "SELECT COUNT(*) FROM structure_sync_event_member_staging WHERE window_id=?",
            (window_id,),
        ).fetchone()[0]
    with pytest.raises(sqlite3.IntegrityError, match="injected-fault"):
        store.advance_structure_event_member_staging_chunk(window_id=window_id)
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT COUNT(*) FROM structure_sync_event_member_staging WHERE window_id=?",
            (window_id,),
        ).fetchone()[0] == before_member_count
        assert con.execute(
            "SELECT * FROM structure_sync_event_member_progress WHERE window_id=?",
            (window_id,),
        ).fetchone() == before_progress
        assert con.execute(
            "SELECT COUNT(*) FROM structure_sync_event_member_receipts"
        ).fetchone()[0] == 0


@pytest.mark.parametrize("field", [
    "window_id", "source_event_count", "source_event_root", "source_identity_hash",
    "metadata_contract", "member_row_count", "member_row_root",
    "invalid_member_count", "invalid_member_root", "terminal_event_cursor",
    "terminal_member_ordinal", "terminal_member_byte_offset", "sealed_at_ms",
    "receipt_digest", "event_conflict_count", "event_conflict_root",
    "event_conflict_merkle_root", "source_group_truth_count",
    "source_group_truth_root",
])
def test_event_member_receipt_tamper_fails_closed(tmp_path, field) -> None:
    store = SQLiteStore(tmp_path / f"receipt-{field}.db")
    store.init_schema()
    window_id = _seed_event_member_window(store, 1)
    _advance_event_members_until_sealed(store, window_id, max_chunks=4)
    with sqlite3.connect(store.db_path) as con:
        row = con.execute("SELECT * FROM structure_sync_event_member_receipts").fetchone()
        assert row[13] == sqlite_store_module._structure_event_member_receipt_digest(
            tuple(row[:13]),
            event_conflict_count=int(row[14]),
            event_conflict_root=str(row[15]),
            event_conflict_merkle_root=str(row[16]),
            source_group_truth_count=int(row[17]),
            source_group_truth_root=str(row[18]),
        )
        columns = [r[1] for r in con.execute(
            "PRAGMA table_info(structure_sync_event_member_receipts)"
        )]
        old = row[columns.index(field)]
        changed = old + 1 if type(old) is int else (
            "x" * 64 if isinstance(old, str) and len(old) == 64 else f"{old}-x"
        )
        con.execute("DROP TRIGGER trg_structure_event_member_receipt_update")
        con.execute("PRAGMA foreign_keys=OFF")
        con.execute("PRAGMA ignore_check_constraints=ON")
        con.execute(f"UPDATE structure_sync_event_member_receipts SET {field}=?",
                    (changed,))
    status = store.structure_event_member_status(
        window_id=str(changed) if field == "window_id" else window_id
    )
    assert status == {"sealed": False, "complete": False,
                      "reason": "structure-event-member-receipt-invalid"}


@pytest.mark.parametrize("operation", ["insert", "update", "delete", "replace"])
def test_event_conflict_summary_is_frozen_after_receipt(tmp_path, operation) -> None:
    store = SQLiteStore(tmp_path / f"conflict-frozen-{operation}.db")
    store.init_schema()
    window_id = _seed_event_member_window(store, 1)
    sealing = _advance_event_members_until_sealed(
        store, window_id, max_chunks=4,
    )
    assert [run["state"] for run in sealing] == [
        "deriving-group-truth", "sealing-conflicts", "sealed",
    ]
    statements = {
        "insert": (
            "INSERT INTO structure_sync_event_conflict_summaries VALUES (?,?,0)",
            (window_id, "other-event"),
        ),
        "update": (
            "UPDATE structure_sync_event_conflict_summaries SET global_conflict=1 "
            "WHERE window_id=?",
            (window_id,),
        ),
        "delete": (
            "DELETE FROM structure_sync_event_conflict_summaries WHERE window_id=?",
            (window_id,),
        ),
        "replace": (
            "INSERT OR REPLACE INTO structure_sync_event_conflict_summaries "
            "VALUES (?,?,1)",
            (window_id, "e"),
        ),
    }
    with sqlite3.connect(store.db_path) as con, pytest.raises(
        sqlite3.IntegrityError,
        match="^structure-event-conflict-summary-frozen$",
    ):
        con.execute(*statements[operation])


@pytest.mark.parametrize("operation", ["insert", "update", "delete", "replace"])
def test_event_group_truth_staging_is_frozen_after_receipt(
    tmp_path, operation,
) -> None:
    store = SQLiteStore(tmp_path / f"group-truth-frozen-{operation}.db")
    store.init_schema()
    window_id = _seed_event_member_window(store, 1)
    _advance_event_members_until_sealed(store, window_id, max_chunks=4)
    statements = {
        "insert": (
            "INSERT INTO structure_sync_event_group_truth_staging("
            "window_id,event_id,group_id,neg_risk_type,expected_member_count,"
            "active_named_count,membership_hash,quality,reason,"
            "tradable_open_named_count) VALUES (?,?,?,'standard',1,1,?,"
            "'complete-supported',NULL,1)",
            (window_id, "other-event", "other-group", "a" * 64),
        ),
        "update": (
            "UPDATE structure_sync_event_group_truth_staging SET quality="
            "'complete-unsupported' WHERE window_id=?",
            (window_id,),
        ),
        "delete": (
            "DELETE FROM structure_sync_event_group_truth_staging WHERE window_id=?",
            (window_id,),
        ),
        "replace": (
            "INSERT OR REPLACE INTO structure_sync_event_group_truth_staging("
            "window_id,event_id,group_id,neg_risk_type,expected_member_count,"
            "active_named_count,membership_hash,quality,reason,"
            "tradable_open_named_count) SELECT window_id,event_id,group_id,"
            "neg_risk_type,expected_member_count,active_named_count,membership_hash,"
            "quality,reason,tradable_open_named_count FROM "
            "structure_sync_event_group_truth_staging WHERE window_id=?",
            (window_id,),
        ),
    }
    with sqlite3.connect(store.db_path) as con, pytest.raises(
        sqlite3.IntegrityError,
        match="^structure-event-group-truth-frozen$",
    ):
        con.execute(*statements[operation])


@pytest.mark.parametrize("operation", ["insert", "update", "delete", "replace"])
def test_event_group_truth_progress_is_frozen_after_receipt(
    tmp_path, operation,
) -> None:
    store = SQLiteStore(tmp_path / f"group-progress-frozen-{operation}.db")
    store.init_schema()
    window_id = _seed_event_member_window(store, 1)
    _advance_event_members_until_sealed(store, window_id, max_chunks=4)
    statements = {
        "insert": (
            "INSERT INTO structure_sync_event_group_truth_progress SELECT * FROM "
            "structure_sync_event_group_truth_progress WHERE window_id=?",
            (window_id,),
        ),
        "update": (
            "UPDATE structure_sync_event_group_truth_progress SET truth_count="
            "truth_count+1 WHERE window_id=?",
            (window_id,),
        ),
        "delete": (
            "DELETE FROM structure_sync_event_group_truth_progress WHERE window_id=?",
            (window_id,),
        ),
        "replace": (
            "INSERT OR REPLACE INTO structure_sync_event_group_truth_progress "
            "SELECT * FROM structure_sync_event_group_truth_progress WHERE window_id=?",
            (window_id,),
        ),
    }
    with sqlite3.connect(store.db_path) as con, pytest.raises(
        sqlite3.IntegrityError,
        match="^structure-event-group-truth-progress-frozen$",
    ):
        con.execute(*statements[operation])


@pytest.mark.parametrize(
    "tamper", ["progress-checkpoint", "progress-delete", "mixed-root"],
)
def test_event_group_truth_terminal_tamper_fails_closed_without_evidence(
    tmp_path, tamper,
) -> None:
    store = SQLiteStore(tmp_path / f"group-terminal-{tamper}.db")
    store.init_schema()
    window_id = _seed_event_member_window(store, 1)
    _advance_event_members_until_sealed(store, window_id, max_chunks=4)
    with sqlite3.connect(store.db_path) as con:
        if tamper == "progress-delete":
            con.execute(
                "DROP TRIGGER trg_structure_event_group_truth_progress_delete_guard"
            )
            con.execute(
                "DELETE FROM structure_sync_event_group_truth_progress WHERE window_id=?",
                (window_id,),
            )
        else:
            con.execute(
                "DROP TRIGGER trg_structure_event_group_truth_progress_update_guard"
            )
            assignment = (
                "checkpoint_digest='" + "e" * 64 + "'"
                if tamper == "progress-checkpoint"
                else "truth_count=truth_count+1"
            )
            con.execute(
                "UPDATE structure_sync_event_group_truth_progress SET "
                + assignment
                + " WHERE window_id=?",
                (window_id,),
            )
    assert store.structure_event_member_status(window_id=window_id) == {
        "sealed": False,
        "complete": False,
        "reason": "structure-event-member-receipt-invalid",
    }


@pytest.mark.parametrize("limit", [1, 17, 500])
def test_event_conflict_summary_sealing_is_chunk_invariant_across_restarts(
    tmp_path, limit
) -> None:
    store = SQLiteStore(tmp_path / f"conflict-bounded-{limit}.db")
    store.init_schema()
    window_id = str(store.begin_or_resume_structure_sync(started_at_ms=1)["id"])
    events = [
        {"id": f"event-{index:04d}", "markets": []}
        for index in range(501)
    ]
    store.commit_structure_event_page(
        window_id=window_id,
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        events=events,
        finished_at_ms=2,
    )
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "UPDATE structure_sync_windows SET status='complete' WHERE id=?",
            (window_id,),
        )
        con.execute(
            "INSERT INTO structure_sync_event_market_backfill_progress("
            "window_id,window_checkpoint_at_ms,checkpoint_at_ms,completed_at_ms) "
            "VALUES (?,?,?,?)",
            (window_id, 2, 2, 2),
        )
    runs = []
    for _ in range(5_000):
        runs.append(store.advance_structure_event_member_staging_chunk(
            window_id=window_id, limit=limit
        ))
        store = SQLiteStore(store.db_path)
        if runs[-1]["sealed"]:
            break
    assert runs[-1]["sealed"] is True
    assert max(int(run["rows_written"]) for run in runs) <= limit
    assert {str(run["state"]) for run in runs} >= {
        "sealing-conflict-merkle",
        "sealing-conflict-proofs",
        "sealed",
    }
    assert store.structure_event_member_status(window_id=window_id)[
        "event_conflict_count"
    ] == 501
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT COUNT(*),COUNT(DISTINCT leaf_index) FROM "
            "structure_sync_event_conflict_proofs WHERE window_id=?",
            (window_id,),
        ).fetchone() == (501, 501)
        merkle_root = str(con.execute(
            "SELECT event_conflict_merkle_root FROM "
            "structure_sync_event_member_receipts WHERE window_id=?",
            (window_id,),
        ).fetchone()[0])
        proof_rows = con.execute(
            "SELECT proof.leaf_hash,proof.proof_json FROM "
            "structure_sync_event_conflict_proofs proof WHERE proof.window_id=? "
            "ORDER BY proof.leaf_index",
            (window_id,),
        ).fetchall()
        level_counts = con.execute(
            "SELECT level,COUNT(*),COUNT(DISTINCT node_index) FROM "
            "structure_sync_event_conflict_merkle_nodes WHERE window_id=? "
            "GROUP BY level ORDER BY level",
            (window_id,),
        ).fetchall()
    expected_root, _expected_proofs = sqlite_store_module._event_conflict_merkle_proofs([
        sqlite_store_module._event_conflict_leaf_hash(
            window_id=window_id,
            event_id=f"event-{index:04d}",
            global_conflict=False,
        )
        for index in range(501)
    ])
    assert merkle_root == expected_root
    expected_widths = []
    width = 501
    while True:
        expected_widths.append(width)
        if width == 1:
            break
        width = (width + 1) // 2
    assert level_counts == [
        (level, width, width) for level, width in enumerate(expected_widths)
    ]
    assert all(
        sqlite_store_module._verify_event_conflict_merkle_proof(
            leaf_hash=str(leaf_hash),
            proof_json=str(proof_json),
            expected_root=merkle_root,
        )
        for leaf_hash, proof_json in proof_rows
    )


def test_event_source_authority_seals_with_natural_event_page(tmp_path) -> None:
    assert STRUCTURE_EVENT_SOURCE_CONTRACT == "structure-event-source-v1"
    store = SQLiteStore(tmp_path / "source.db")
    store.init_schema()
    window = store.begin_or_resume_structure_sync(started_at_ms=1)
    event = {
        "id": "e", "negRiskMarketID": "group-parent",
        "markets": [{"id": "m", "active": True, "closed": False,
                     "negRiskOther": False}],
    }
    store.commit_structure_event_page(
        window_id=window["id"], requested_cursor=None, next_cursor=None,
        completed=True, events=[event], finished_at_ms=2,
    )
    with sqlite3.connect(store.db_path) as con:
        metadata = con.execute(
            "SELECT event_group_id,payload_hash,payload_length,metadata_contract "
            "FROM structure_sync_event_metadata_staging WHERE window_id=?",
            (window["id"],),
        ).fetchone()
        receipt = con.execute(
            "SELECT event_count,event_root,metadata_contract,receipt_digest "
            "FROM structure_sync_event_source_receipts WHERE window_id=?",
            (window["id"],),
        ).fetchone()
    assert metadata[0] == "group-parent"
    assert len(metadata[1]) == 64 and metadata[2] > 0
    assert metadata[3] == STRUCTURE_EVENT_SOURCE_CONTRACT
    assert receipt[0] == 1 and len(receipt[1]) == 64
    assert receipt[2] == STRUCTURE_EVENT_SOURCE_CONTRACT
    assert len(receipt[3]) == 64


@pytest.mark.parametrize("fault", ["metadata", "progress", "receipt"])
def test_event_source_authority_fault_rolls_back_page_and_cursor(tmp_path, fault) -> None:
    store = SQLiteStore(tmp_path / f"source-{fault}.db")
    store.init_schema()
    window_id = str(store.begin_or_resume_structure_sync(started_at_ms=1)["id"])
    table, operation = {
        "metadata": ("structure_sync_event_metadata_staging", "INSERT"),
        "progress": ("structure_sync_event_source_progress", "UPDATE"),
        "receipt": ("structure_sync_event_source_receipts", "INSERT"),
    }[fault]
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            f"CREATE TRIGGER injected BEFORE {operation} ON {table} "
            "BEGIN SELECT RAISE(ABORT,'source-injected-fault'); END"
        )
    with pytest.raises(sqlite3.IntegrityError, match="source-injected-fault"):
        store.commit_structure_event_page(
            window_id=window_id, requested_cursor=None, next_cursor=None,
            completed=True, events=[{"id": "e", "markets": []}],
            finished_at_ms=2,
        )
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT status,event_cursor,event_pages FROM structure_sync_windows WHERE id=?",
            (window_id,),
        ).fetchone() == ("open", None, 0)
        assert con.execute(
            "SELECT COUNT(*) FROM structure_sync_event_staging WHERE window_id=?",
            (window_id,),
        ).fetchone()[0] == 0
        assert con.execute(
            "SELECT event_count FROM structure_sync_event_source_progress WHERE window_id=?",
            (window_id,),
        ).fetchone()[0] == 0
        assert con.execute(
            "SELECT COUNT(*) FROM structure_sync_event_source_receipts WHERE window_id=?",
            (window_id,),
        ).fetchone()[0] == 0
        assert con.execute(
            "SELECT COUNT(*) FROM structure_sync_event_conflict_summaries WHERE window_id=?",
            (window_id,),
        ).fetchone()[0] == 0


def test_event_relation_conflict_summary_updates_both_parents_atomically(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "relation-conflict-atomic.db")
    store.init_schema()
    window_id = str(store.begin_or_resume_structure_sync(started_at_ms=1)["id"])
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "INSERT INTO structure_sync_event_market_staging VALUES (?,?,?,?)",
            (window_id, "market-shared", "a", 1),
        )
        con.execute(
            "CREATE TRIGGER injected_conflict_fault BEFORE INSERT ON "
            "structure_sync_event_conflict_summaries BEGIN SELECT "
            "RAISE(ABORT,'conflict-injected-fault'); END"
        )
        with pytest.raises(sqlite3.IntegrityError, match="conflict-injected-fault"):
            con.execute(
                "INSERT INTO structure_sync_event_market_staging VALUES (?,?,?,?)",
                (window_id, "market-shared", "b", 2),
            )
        assert con.execute(
            "SELECT event_id,global_conflict FROM "
            "structure_sync_event_conflict_summaries ORDER BY event_id"
        ).fetchall() == []
        assert con.execute(
            "SELECT event_id FROM structure_sync_event_market_staging "
            "WHERE market_id='market-shared' ORDER BY event_id"
        ).fetchall() == [("a",)]
        con.execute("DROP TRIGGER injected_conflict_fault")
        con.execute(
            "INSERT INTO structure_sync_event_market_staging VALUES (?,?,?,?)",
            (window_id, "market-shared", "b", 2),
        )
        assert con.execute(
            "SELECT event_id,global_conflict FROM "
            "structure_sync_event_conflict_summaries ORDER BY event_id"
        ).fetchall() == [("a", 1), ("b", 1)]
    store.commit_structure_event_page(
        window_id=window_id,
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        events=[{"id": "a", "markets": []}, {"id": "b", "markets": []}],
        finished_at_ms=2,
    )
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT event_id,global_conflict FROM "
            "structure_sync_event_conflict_summaries ORDER BY event_id"
        ).fetchall() == [("a", 1), ("b", 1)]
        assert con.execute(
            "SELECT event_count FROM structure_sync_event_source_receipts "
            "WHERE window_id=?",
            (window_id,),
        ).fetchone() == (2,)


def test_historical_window_without_source_receipt_is_unavailable(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "historical.db")
    store.init_schema()
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "INSERT INTO structure_sync_windows(id,status,started_at_ms,checkpoint_at_ms) "
            "VALUES ('w','complete',1,2)"
        )
    expected = {
        "sealed": False, "complete": False,
        "state": "waiting-natural-window", "authenticated": True,
        "reason": "structure-event-source-receipt-unavailable",
    }
    assert store.structure_event_member_status(window_id="w") == expected
    before = _event_source_state_bytes(store.db_path)
    assert store.advance_structure_event_member_staging_chunk(window_id="w") == expected
    assert _event_source_state_bytes(store.db_path) == before


def test_event_member_parent_group_and_kind_are_source_derived() -> None:
    row = extract_structure_event_member_row(
        window_id="w", event_id="e", event_ordinal=1, member_ordinal=0,
        event_group_id="parent-group",
        member={"id": "m", "negRiskMarketID": "nested-group",
                "memberKind": "other", "negRiskOther": False,
                "active": True, "closed": False},
    )
    assert (row.group_id, row.member_kind) == ("parent-group", "named")
    invalid = extract_structure_event_member_row(
        window_id="w", event_id="e", event_ordinal=1, member_ordinal=1,
        event_group_id=" padded ",
        member={"id": "m", "negRiskOther": "false",
                "active": True, "closed": False},
    )
    assert (invalid.group_id, invalid.member_kind) == (None, None)


def test_event_member_multi_event_chunk_spends_one_shared_budget(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "multi-event.db")
    store.init_schema()
    window_id = str(store.begin_or_resume_structure_sync(started_at_ms=1)["id"])
    events = [
        {"id": event_id, "negRiskMarketID": "g", "markets": [
            {"id": f"{event_id}-{index}", "negRiskOther": False,
             "active": True, "closed": False}
            for index in range(count)
        ]}
        for event_id, count in (("a", 3), ("b", 4))
    ]
    store.commit_structure_event_page(
        window_id=window_id, requested_cursor=None, next_cursor=None,
        completed=True, events=events, finished_at_ms=2,
    )
    with sqlite3.connect(store.db_path) as con:
        con.execute("UPDATE structure_sync_windows SET status='complete' WHERE id=?",
                    (window_id,))
        con.execute(
            "INSERT INTO structure_sync_event_market_backfill_progress("
            "window_id,window_checkpoint_at_ms,checkpoint_at_ms,completed_at_ms) "
            "VALUES (?,?,?,?)",
            (window_id, 2, 2, 2),
        )
    first = store.advance_structure_event_member_staging_chunk(
        window_id=window_id, limit=5,
    )
    second = store.advance_structure_event_member_staging_chunk(
        window_id=window_id, limit=5,
    )
    assert (first["rows_written"], first["complete"]) == (5, False)
    assert (second["rows_written"], second["complete"]) == (2, False)
    sealing = _advance_event_members_until_sealed(
        store, window_id, limit=5, max_chunks=4,
    )
    assert [run["state"] for run in sealing] == [
        "deriving-group-truth", "sealing-conflicts", "sealed",
    ]


def test_event_member_utf8_resume_uses_authenticated_character_offset(monkeypatch) -> None:
    payload = json.dumps({"markets": [
        {"id": f"市场-{index}", "label": "éclair"}
        for index in range(4)
    ]}, ensure_ascii=False, separators=(",", ":"))
    first = decode_event_member_batch(
        payload, member_ordinal=0, member_byte_offset=0, limit=2,
    )
    monkeypatch.setattr(
        member_module, "_character_offset",
        lambda *_args: pytest.fail("UTF-8 committed prefix decoded"),
    )
    monkeypatch.setattr(
        member_module, "_locate_markets_array",
        lambda *_args: pytest.fail("UTF-8 committed prefix rescanned"),
    )
    second = decode_event_member_batch(
        payload, member_ordinal=first.next_member_ordinal,
        member_byte_offset=first.next_byte_offset,
        member_character_offset=first.next_character_offset, limit=2,
    )
    assert second.complete is True
    assert second.next_member_ordinal == 4


@pytest.mark.parametrize("field", [
    "member_character_offset", "source_receipt_digest",
    "parent_payload_hash", "checkpoint_digest",
])
def test_event_member_authenticated_cursor_tamper_fails_closed(tmp_path, field) -> None:
    store = SQLiteStore(tmp_path / f"cursor-{field}.db")
    store.init_schema()
    window_id = _seed_event_member_window(store, 501)
    store.advance_structure_event_member_staging_chunk(window_id=window_id, limit=500)
    with sqlite3.connect(store.db_path) as con:
        value = con.execute(
            f"SELECT {field} FROM structure_sync_event_member_progress WHERE window_id=?",
            (window_id,),
        ).fetchone()[0]
        changed = value + 1 if type(value) is int else "f" * 64
        con.execute(
            f"UPDATE structure_sync_event_member_progress SET {field}=? WHERE window_id=?",
            (changed, window_id),
        )
    with pytest.raises(ValueError, match="structure-event-member-checkpoint-invalid"):
        store.advance_structure_event_member_staging_chunk(window_id=window_id, limit=500)


@pytest.mark.parametrize("field", [
    "event_cursor", "member_ordinal", "rows_written", "member_byte_offset",
    "member_state", "diagnostic_state", "member_character_offset",
    "source_receipt_digest", "parent_payload_hash", "checkpoint_digest",
])
def test_event_member_recovering_status_authenticates_every_checkpoint_field(
    tmp_path, field,
) -> None:
    store = SQLiteStore(tmp_path / f"status-{field}.db")
    store.init_schema()
    window_id = _seed_event_member_window(store, 501)
    store.advance_structure_event_member_staging_chunk(window_id=window_id, limit=500)
    with sqlite3.connect(store.db_path) as con:
        value = con.execute(
            f"SELECT {field} FROM structure_sync_event_member_progress WHERE window_id=?",
            (window_id,),
        ).fetchone()[0]
        changed = value + 1 if type(value) is int else f"{value}-tampered"
        con.execute("PRAGMA ignore_check_constraints=ON")
        con.execute(
            f"UPDATE structure_sync_event_member_progress SET {field}=? WHERE window_id=?",
            (changed, window_id),
        )
    assert store.structure_event_member_status(window_id=window_id) == {
        "sealed": False, "complete": False,
        "reason": "structure-event-member-checkpoint-invalid",
    }


@pytest.mark.parametrize("mutation", ["missing-receipt", "unknown-contract", "digest"])
def test_event_source_present_but_invalid_is_not_historical_unavailable(
    tmp_path, mutation,
) -> None:
    store = SQLiteStore(tmp_path / f"source-invalid-{mutation}.db")
    store.init_schema()
    window_id = _seed_event_member_window(store, 1)
    with sqlite3.connect(store.db_path) as con:
        con.execute("DROP TRIGGER trg_structure_event_source_receipt_delete_guard")
        con.execute("DROP TRIGGER trg_structure_event_source_receipt_update_guard")
        con.execute("PRAGMA ignore_check_constraints=ON")
        if mutation == "missing-receipt":
            con.execute(
                "DELETE FROM structure_sync_event_source_receipts WHERE window_id=?",
                (window_id,),
            )
        elif mutation == "unknown-contract":
            con.execute(
                "UPDATE structure_sync_event_source_receipts SET metadata_contract='unknown' "
                "WHERE window_id=?", (window_id,),
            )
        else:
            con.execute(
                "UPDATE structure_sync_event_source_receipts SET receipt_digest=? "
                "WHERE window_id=?", ("f" * 64, window_id),
            )
    assert store.structure_event_member_status(window_id=window_id) == {
        "sealed": False, "complete": False,
        "reason": "structure-event-source-receipt-invalid",
    }


def _event_source_state_bytes(db_path) -> bytes:
    with sqlite3.connect(db_path) as con:
        return b"\n".join(
            repr((table, rows)).encode()
            for table, rows in (
                (table, con.execute(f"SELECT * FROM {table} ORDER BY 1,2").fetchall())
                for table in (
                    "structure_sync_windows",
                    "structure_sync_event_staging",
                    "structure_sync_event_metadata_staging",
                    "structure_sync_event_source_progress",
                    "structure_sync_event_source_receipts",
                    "structure_sync_event_member_progress",
                )
            )
        )


def test_event_source_exact_completed_page_replay_is_byte_idempotent(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "source-replay.db")
    store.init_schema()
    window_id = str(store.begin_or_resume_structure_sync(started_at_ms=1)["id"])
    event = {"id": "e", "negRiskMarketID": "g", "markets": []}
    kwargs = dict(
        window_id=window_id, requested_cursor=None, next_cursor=None,
        completed=True, events=[event], finished_at_ms=2,
    )
    store.commit_structure_event_page(**kwargs)
    before = _event_source_state_bytes(store.db_path)
    store.commit_structure_event_page(**kwargs)
    assert _event_source_state_bytes(store.db_path) == before


def test_event_source_exact_replay_remains_idempotent_after_window_completion(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "source-replay-complete.db")
    store.init_schema()
    window_id = str(store.begin_or_resume_structure_sync(started_at_ms=1)["id"])
    kwargs = dict(
        window_id=window_id, requested_cursor=None, next_cursor=None,
        completed=True, events=[{"id": "e", "markets": []}], finished_at_ms=2,
    )
    store.commit_structure_event_page(**kwargs)
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "UPDATE structure_sync_windows SET status='complete',checkpoint_at_ms=3 "
            "WHERE id=?", (window_id,),
        )
    before = _event_source_state_bytes(store.db_path)
    store.commit_structure_event_page(**kwargs)
    assert _event_source_state_bytes(store.db_path) == before


@pytest.mark.parametrize("mutation", [
    "partial", "different-id", "different-payload", "different-group",
    "different-requested-cursor", "different-finished-at",
])
def test_event_source_replay_mismatch_preserves_byte_identical_state(
    tmp_path, mutation,
) -> None:
    store = SQLiteStore(tmp_path / f"source-replay-{mutation}.db")
    store.init_schema()
    window_id = str(store.begin_or_resume_structure_sync(started_at_ms=1)["id"])
    events = [
        {"id": "e-1", "negRiskMarketID": "g", "title": "one", "markets": []},
        {"id": "e-2", "negRiskMarketID": "g", "title": "two", "markets": []},
    ]
    kwargs = dict(
        window_id=window_id, requested_cursor=None, next_cursor=None,
        completed=True, events=events, finished_at_ms=2,
    )
    store.commit_structure_event_page(**kwargs)
    changed = {**kwargs, "events": [dict(event) for event in events]}
    if mutation == "partial":
        changed["events"] = changed["events"][:1]
    elif mutation == "different-id":
        changed["events"][0]["id"] = "e-x"
    elif mutation == "different-payload":
        changed["events"][0]["title"] = "changed"
    elif mutation == "different-group":
        changed["events"][0]["negRiskMarketID"] = "other"
    elif mutation == "different-requested-cursor":
        changed["requested_cursor"] = "other"
    else:
        changed["finished_at_ms"] = 3
    before = _event_source_state_bytes(store.db_path)
    with pytest.raises(ValueError):
        store.commit_structure_event_page(**changed)
    assert _event_source_state_bytes(store.db_path) == before


@pytest.mark.parametrize("field", [
    "window_id", "event_count", "event_root", "terminal_event_pages",
    "terminal_event_cursor", "metadata_contract", "sealed_at_ms", "receipt_digest",
])
def test_event_source_receipt_every_field_tamper_fails_closed(tmp_path, field) -> None:
    store = SQLiteStore(tmp_path / f"source-receipt-{field}.db")
    store.init_schema()
    window_id = _seed_event_member_window(store, 1)
    with sqlite3.connect(store.db_path) as con:
        columns = [row[1] for row in con.execute(
            "PRAGMA table_info(structure_sync_event_source_receipts)"
        )]
        receipt = con.execute(
            "SELECT * FROM structure_sync_event_source_receipts WHERE window_id=?",
            (window_id,),
        ).fetchone()
        value = receipt[columns.index(field)]
        changed = value + 1 if type(value) is int else (
            "f" * 64 if isinstance(value, str) and len(value) == 64 else f"{value}-x"
        )
        con.execute("DROP TRIGGER trg_structure_event_source_receipt_update_guard")
        con.execute("PRAGMA foreign_keys=OFF")
        con.execute("PRAGMA ignore_check_constraints=ON")
        con.execute(
            f"UPDATE structure_sync_event_source_receipts SET {field}=? WHERE window_id=?",
            (changed, window_id),
        )
    assert store.structure_event_member_status(window_id=window_id) == {
        "sealed": False, "complete": False,
        "reason": "structure-event-source-receipt-invalid",
    }


def test_event_source_receipt_replacement_is_blocked(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "source-replace.db")
    store.init_schema()
    _seed_event_member_window(store, 1)
    with sqlite3.connect(store.db_path) as con:
        receipt = con.execute(
            "SELECT * FROM structure_sync_event_source_receipts"
        ).fetchone()
        with pytest.raises(sqlite3.IntegrityError, match="source-receipt-frozen"):
            con.execute(
                "INSERT OR REPLACE INTO structure_sync_event_source_receipts VALUES "
                "(?,?,?,?,?,?,?,?)", receipt,
            )


@pytest.mark.parametrize("missing", ["progress", "receipt"])
def test_event_member_mixed_terminal_authority_fails_closed(tmp_path, missing) -> None:
    store = SQLiteStore(tmp_path / f"member-mixed-{missing}.db")
    store.init_schema()
    window_id = _seed_event_member_window(store, 1)
    _advance_event_members_until_sealed(store, window_id, max_chunks=4)
    with sqlite3.connect(store.db_path) as con:
        if missing == "progress":
            con.execute(
                "DELETE FROM structure_sync_event_member_progress WHERE window_id=?",
                (window_id,),
            )
        else:
            con.execute("DROP TRIGGER trg_structure_event_member_receipt_delete")
            con.execute(
                "DELETE FROM structure_sync_event_member_receipts WHERE window_id=?",
                (window_id,),
            )
    assert store.structure_event_member_status(window_id=window_id) == {
        "sealed": False, "complete": False,
        "reason": "structure-event-member-receipt-invalid",
    }


def test_event_member_missing_recovering_progress_fails_closed(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "member-missing-progress.db")
    store.init_schema()
    window_id = _seed_event_member_window(store, 1)
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "DELETE FROM structure_sync_event_member_progress WHERE window_id=?",
            (window_id,),
        )
    assert store.structure_event_member_status(window_id=window_id) == {
        "sealed": False, "complete": False,
        "reason": "structure-event-member-checkpoint-invalid",
    }


def test_event_member_direct_advance_never_recreates_missing_progress(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "member-direct-missing-progress.db")
    store.init_schema()
    window_id = _seed_event_member_window(store, 1)
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "DELETE FROM structure_sync_event_member_progress WHERE window_id=?",
            (window_id,),
        )
    before = _event_source_state_bytes(store.db_path)
    assert store.advance_structure_event_member_staging_chunk(window_id=window_id) == {
        "sealed": False, "complete": False,
        "reason": "structure-event-member-checkpoint-invalid",
    }
    assert _event_source_state_bytes(store.db_path) == before
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT COUNT(*) FROM structure_sync_event_member_progress WHERE window_id=?",
            (window_id,),
        ).fetchone() == (0,)
        assert con.execute(
            "SELECT COUNT(*) FROM structure_sync_event_member_staging WHERE window_id=?",
            (window_id,),
        ).fetchone() == (0,)
        assert con.execute(
            "SELECT COUNT(*) FROM structure_sync_event_member_receipts WHERE window_id=?",
            (window_id,),
        ).fetchone() == (0,)
