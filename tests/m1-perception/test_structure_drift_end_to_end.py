from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import time
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

import polyarb.storage.sqlite_store as sqlite_store_module
from polyarb.http import health as health_module
from polyarb.perception.structure_contract import (
    STRUCTURE_DRIFT_CLASSIFIER_V1,
    STRUCTURE_DRIFT_CLASSIFIER_V2,
    STRUCTURE_DRIFT_CLASSIFIER_V3,
    STRUCTURE_DRIFT_CLASSIFIER_V4,
    STRUCTURE_EVENT_SOURCE_CONTRACT,
    STRUCTURE_PROJECTION_EXCLUSION_REASONS,
)
from polyarb.perception.structure_drift import (
    FreshProjectionChunk,
    FreshProjectionCommitment,
    FreshProjectionCursor,
    _member_tuple,
    project_legacy_compatible_event,
    project_legacy_compatible_market,
)
from polyarb.storage.row_chain_sha256 import RowChainSHA256
from polyarb.storage.sqlite_store import SQLiteStore

_CLASSIFIER_PROGRESS_FIELDS = {
    "classifier_contract_version",
    "diagnostic_counts_json",
    "diagnostic_digest_state_json",
    "diagnostic_root",
    "diagnostic_samples_json",
    "diagnostic_samples_digest",
}
_TERMINAL_RECEIPT_FIELDS = (
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
_V3_RECEIPT_EXCLUSION_FIELDS = {
    "projection_candidate_count",
    "projection_exclusion_count",
    "projection_exclusion_counts_json",
    "projection_exclusion_roots_json",
}
_V3_PROGRESS_EXCLUSION_FIELDS = {
    *_V3_RECEIPT_EXCLUSION_FIELDS,
    "projection_exclusion_digest_states_json",
}


def test_classifier_schema_versions_authority_and_seals_terminal_receipts(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "classifier-schema.db")
    store.init_schema()
    with sqlite3.connect(store.db_path) as con:
        progress = {
            str(row[1])
            for row in con.execute(
                "PRAGMA table_info(structure_generation_drift_progress)"
            )
        }
        authorization = {
            str(row[1])
            for row in con.execute(
                "PRAGMA table_info(structure_generation_drift_receipts)"
            )
        }
        terminal = tuple(
            str(row[1])
            for row in con.execute(
                "PRAGMA table_info(structure_generation_drift_terminal_receipts)"
            )
        )
        terminal_triggers = {
            str(row[0]): str(row[1])
            for row in con.execute(
                "SELECT name,sql FROM sqlite_master WHERE type='trigger' AND "
                "tbl_name='structure_generation_drift_terminal_receipts'"
            )
        }
    assert _CLASSIFIER_PROGRESS_FIELDS <= progress
    assert "classifier_contract_version" in authorization
    assert (
        set(_TERMINAL_RECEIPT_FIELDS)
        | _V3_RECEIPT_EXCLUSION_FIELDS
        | {"receipt_digest"}
        == set(terminal)
    )
    assert set(terminal_triggers) == {
        "trg_structure_drift_terminal_receipt_update",
        "trg_structure_drift_terminal_receipt_delete",
        "trg_structure_drift_terminal_receipt_insert",
    }
    assert all(
        "structure-drift-terminal-receipt-sealed" in sql
        for sql in terminal_triggers.values()
    )


def test_classifier_contract_changes_comparison_identity() -> None:
    identity = tuple(range(12))
    v1_id = sqlite_store_module._structure_drift_comparison_id(
        identity, classifier_contract_version=STRUCTURE_DRIFT_CLASSIFIER_V1
    )
    v2_id = sqlite_store_module._structure_drift_comparison_id(
        identity, classifier_contract_version=STRUCTURE_DRIFT_CLASSIFIER_V2
    )
    assert v1_id != v2_id


def test_terminal_receipt_digest_has_independent_fixed_field_oracle() -> None:
    payload = {
        field: index if field.endswith("_ms") or field.endswith("_id") else field
        for index, field in enumerate(_TERMINAL_RECEIPT_FIELDS)
    }
    payload["legacy_snapshot_id"] = 1
    payload["generation_snapshot_id"] = 2
    payload["classifier_contract_version"] = STRUCTURE_DRIFT_CLASSIFIER_V2
    values = tuple(payload[field] for field in _TERMINAL_RECEIPT_FIELDS)
    expected = hashlib.sha256(
        json.dumps(values, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()
    assert sqlite_store_module._structure_drift_terminal_receipt_digest(payload) == expected
    for field in (
        "terminal_reason",
        "diagnostic_counts_json",
        "diagnostic_root",
        "diagnostic_samples_json",
        "diagnostic_samples_digest",
    ):
        changed = {**payload, field: f"changed-{field}"}
        assert (
            sqlite_store_module._structure_drift_terminal_receipt_digest(changed)
            != expected
        )
    with pytest.raises(
        ValueError, match="invalid-structure-drift-classifier-contract"
    ):
        sqlite_store_module._structure_drift_terminal_receipt_digest(
            {**payload, "classifier_contract_version": "changed-contract"}
        )


def test_terminal_receipt_schema_rejects_update_and_delete(tmp_path: Path) -> None:
    store = _drift_store(tmp_path)
    comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)
    with sqlite3.connect(store.db_path) as con:
        payload = _terminal_receipt_payload(con, comparison_id)
        terminal_fields = sqlite_store_module._structure_drift_terminal_receipt_fields(
            str(payload["classifier_contract_version"])
        )
        receipt_digest = sqlite_store_module._structure_drift_terminal_receipt_digest(
            payload
        )
        con.execute(
            "INSERT INTO structure_generation_drift_terminal_receipts("
            + ",".join(terminal_fields)
            + ",receipt_digest) VALUES ("
            + ",".join("?" for _ in range(len(payload) + 1))
            + ")",
            (*(payload[field] for field in terminal_fields), receipt_digest),
        )
        sealed_row = con.execute(
            "SELECT * FROM structure_generation_drift_terminal_receipts "
            "WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone()
        for statement in (
            "INSERT INTO structure_generation_drift_terminal_receipts("
            + ",".join(terminal_fields)
            + ",receipt_digest) VALUES ("
            + ",".join("?" for _ in range(len(payload) + 1))
            + ")",
            "INSERT OR REPLACE INTO structure_generation_drift_terminal_receipts("
            + ",".join(terminal_fields)
            + ",receipt_digest) VALUES ("
            + ",".join("?" for _ in range(len(payload) + 1))
            + ")",
        ):
            with pytest.raises(
                sqlite3.IntegrityError,
                match="structure-drift-terminal-receipt-sealed",
            ):
                con.execute(
                    statement,
                    (*(payload[field] for field in terminal_fields), "f" * 64),
                )
        assert con.execute(
            "SELECT * FROM structure_generation_drift_terminal_receipts "
            "WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone() == sealed_row
        with pytest.raises(
            sqlite3.IntegrityError,
            match="structure-drift-terminal-receipt-sealed",
        ):
            con.execute(
                "UPDATE structure_generation_drift_terminal_receipts SET "
                "checkpoint_at_ms=3003 WHERE comparison_id=?",
                (comparison_id,),
            )
        with pytest.raises(
            sqlite3.IntegrityError,
            match="structure-drift-terminal-receipt-sealed",
        ):
            con.execute(
                "DELETE FROM structure_generation_drift_terminal_receipts "
                "WHERE comparison_id=?",
                (comparison_id,),
            )


def _terminal_receipt_payload(
    con: sqlite3.Connection,
    comparison_id: str,
) -> dict[str, object]:
    window_id = str(
        con.execute(
            "SELECT window_id FROM structure_generation_drift_progress "
            "WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone()[0]
    )
    candidate_count = sqlite_store_module._fresh_projection_expected_candidate_count(
        con, window_id=window_id
    )
    exclusion_count = candidate_count - 1
    exclusion_counts = {
        reason: 0 for reason in STRUCTURE_PROJECTION_EXCLUSION_REASONS
    }
    exclusion_states = {
        reason: RowChainSHA256.new(f"projection-exclusion/{reason}").to_json()
        for reason in STRUCTURE_PROJECTION_EXCLUSION_REASONS
    }
    exclusion_chain = RowChainSHA256.from_json(
        exclusion_states["non-neg-risk-market"],
        expected_domain="projection-exclusion/non-neg-risk-market",
    )
    for index in range(exclusion_count):
        exclusion_chain.update(("fixture-exclusion", index))
    exclusion_counts["non-neg-risk-market"] = exclusion_count
    exclusion_states["non-neg-risk-market"] = exclusion_chain.to_json()
    exclusion_roots = {
        reason: RowChainSHA256.from_json(
            exclusion_states[reason],
            expected_domain=f"projection-exclusion/{reason}",
        ).hexdigest()
        for reason in STRUCTURE_PROJECTION_EXCLUSION_REASONS
    }
    diagnostic_state = RowChainSHA256.new("diagnostic/unclassified")
    diagnostic_state.update(("other-zero-removal-reason", "m1"))
    con.execute(
        "UPDATE structure_generation_drift_progress SET "
        "projection_candidate_count=?,projection_exclusion_count=?,"
        "projection_exclusion_counts_json=?,projection_exclusion_roots_json=?,"
        "projection_exclusion_digest_states_json=?,"
        "diagnostic_digest_state_json=? WHERE comparison_id=?",
        (
            candidate_count,
            exclusion_count,
            json.dumps(exclusion_counts, sort_keys=True, separators=(",", ":")),
            json.dumps(exclusion_roots, sort_keys=True, separators=(",", ":")),
            json.dumps(exclusion_states, sort_keys=True, separators=(",", ":")),
            diagnostic_state.to_json(),
            comparison_id,
        ),
    )
    row = con.execute(
        "SELECT hash_algorithm,classifier_contract_version,legacy_snapshot_id,"
        "generation_snapshot_id,publication_id,window_id,"
        "normalization_contract_version,exact_receipt_digest,"
        "pointer_validation_hash,generation_certification_hash,"
        "(SELECT receipt_digest FROM structure_sync_event_member_receipts member "
        "WHERE member.window_id=structure_generation_drift_progress.window_id),"
        "projection_candidate_count,projection_exclusion_count,"
        "projection_exclusion_counts_json,projection_exclusion_roots_json FROM "
        "structure_generation_drift_progress WHERE comparison_id=?",
        (comparison_id,),
    ).fetchone()
    diagnostic_samples_json = '{"other-zero-removal-reason":[{"market_id":"m1"}]}'
    class_counts = {
        "shared": 0,
        "fresh-addition": 0,
        "current-nontradable": 0,
        "event-only-quarantine": 0,
        "market-side-quarantine": 0,
        "fresh-source-absent": 0,
        "fresh-group-ineligible": 0,
        "overlap-conflict": 0,
        "unclassified": 1,
    }
    values: tuple[object, ...] = (
        comparison_id,
        *row[:10],
        "a" * 64,
        row[10],
        "drift-unclassified",
        json.dumps(class_counts, sort_keys=True, separators=(",", ":")),
        json.dumps({"unclassified": "c" * 64}, separators=(",", ":")),
        '{"other-zero-removal-reason":1}',
        diagnostic_state.hexdigest(),
        diagnostic_samples_json,
        hashlib.sha256(diagnostic_samples_json.encode()).hexdigest(),
        3_001,
        3_002,
    )
    payload = dict(zip(_TERMINAL_RECEIPT_FIELDS, values, strict=True))
    if row[1] in {STRUCTURE_DRIFT_CLASSIFIER_V3, STRUCTURE_DRIFT_CLASSIFIER_V4}:
        payload.update(
            {
                "projection_candidate_count": row[11],
                "projection_exclusion_count": row[12],
                "projection_exclusion_counts_json": row[13],
                "projection_exclusion_roots_json": row[14],
            }
        )
    return payload


def _insert_terminal_receipt(
    con: sqlite3.Connection,
    payload: dict[str, object],
) -> str:
    receipt_digest = sqlite_store_module._structure_drift_terminal_receipt_digest(
        payload
    )
    fields = sqlite_store_module._structure_drift_terminal_receipt_fields(
        str(payload["classifier_contract_version"])
    )
    con.execute(
        "INSERT INTO structure_generation_drift_terminal_receipts("
        + ",".join(fields)
        + ",receipt_digest) VALUES ("
        + ",".join("?" for _ in range(len(payload) + 1))
        + ")",
        (*(payload[field] for field in fields), receipt_digest),
    )
    return receipt_digest


def _stale_overlap_v3_store(tmp_path: Path) -> tuple[SQLiteStore, str]:
    store = _drift_store(tmp_path)
    with sqlite3.connect(store.db_path) as con:
        con.execute("DROP TRIGGER trg_structure_generation_markets_frozen_update_v2")
        con.execute(
            "UPDATE structure_generation_markets SET yes_token_id='divergent' "
            "WHERE snapshot_id=2 AND market_id='shared'"
        )
    comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)
    assert _run_drift_to_terminal(store, comparison_id) == "stale"
    return store, comparison_id


def _stale_unclassified_v3_store(tmp_path: Path) -> tuple[SQLiteStore, str]:
    store = _drift_store(tmp_path, omit_generation_market_id="addition")
    comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)
    assert _run_drift_to_terminal(store, comparison_id) == "stale"
    return store, comparison_id


def _rewrite_stale_terminal_class_evidence(
    store: SQLiteStore,
    comparison_id: str,
    *,
    class_counts: dict[str, object],
    class_digests: dict[str, object],
    terminal_reason: str | None = None,
) -> None:
    fields = sqlite_store_module._structure_drift_terminal_receipt_fields(
        STRUCTURE_DRIFT_CLASSIFIER_V3
    )
    counts_json = json.dumps(class_counts, sort_keys=True, separators=(",", ":"))
    digests_json = json.dumps(class_digests, sort_keys=True, separators=(",", ":"))
    with sqlite3.connect(store.db_path) as con:
        row = con.execute(
            "SELECT " + ",".join(fields) + " FROM "
            "structure_generation_drift_terminal_receipts WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone()
        assert row is not None
        payload = dict(zip(fields, row, strict=True))
        payload["class_counts_json"] = counts_json
        payload["class_digests_json"] = digests_json
        if terminal_reason is not None:
            payload["terminal_reason"] = terminal_reason
        digest = sqlite_store_module._structure_drift_terminal_receipt_digest(payload)
        con.execute("DROP TRIGGER trg_structure_drift_terminal_receipt_update")
        con.execute(
            "UPDATE structure_generation_drift_terminal_receipts SET "
            "class_counts_json=?,class_digests_json=?,terminal_reason=?,"
            "receipt_digest=? "
            "WHERE comparison_id=?",
            (
                counts_json,
                digests_json,
                payload["terminal_reason"],
                digest,
                comparison_id,
            ),
        )
        con.execute(
            "UPDATE structure_generation_drift_progress SET "
            "class_counts_json=?,class_digests_json=?,terminal_reason=? "
            "WHERE comparison_id=?",
            (counts_json, digests_json, payload["terminal_reason"], comparison_id),
        )


def _rewrite_stale_terminal_diagnostic_evidence(
    store: SQLiteStore,
    comparison_id: str,
    *,
    terminal_reason: str | None = None,
    diagnostic_counts: dict[str, object] | None = None,
    diagnostic_root: str | None = None,
    diagnostic_samples: dict[str, object] | None = None,
) -> None:
    fields = sqlite_store_module._structure_drift_terminal_receipt_fields(
        STRUCTURE_DRIFT_CLASSIFIER_V3
    )
    with sqlite3.connect(store.db_path) as con:
        row = con.execute(
            "SELECT " + ",".join(fields) + " FROM "
            "structure_generation_drift_terminal_receipts WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone()
        assert row is not None
        payload = dict(zip(fields, row, strict=True))
        if terminal_reason is not None:
            payload["terminal_reason"] = terminal_reason
        if diagnostic_counts is not None:
            payload["diagnostic_counts_json"] = json.dumps(
                diagnostic_counts, sort_keys=True, separators=(",", ":")
            )
        if diagnostic_root is not None:
            payload["diagnostic_root"] = diagnostic_root
        if diagnostic_samples is not None:
            samples_json = json.dumps(
                diagnostic_samples,
                sort_keys=True,
                separators=(",", ":"),
            )
            payload["diagnostic_samples_json"] = samples_json
            payload["diagnostic_samples_digest"] = hashlib.sha256(
                samples_json.encode()
            ).hexdigest()
        digest = sqlite_store_module._structure_drift_terminal_receipt_digest(payload)
        con.execute("DROP TRIGGER trg_structure_drift_terminal_receipt_update")
        con.execute(
            "UPDATE structure_generation_drift_terminal_receipts SET "
            "terminal_reason=?,diagnostic_counts_json=?,diagnostic_root=?,"
            "diagnostic_samples_json=?,diagnostic_samples_digest=?,receipt_digest=? "
            "WHERE comparison_id=?",
            (
                payload["terminal_reason"],
                payload["diagnostic_counts_json"],
                payload["diagnostic_root"],
                payload["diagnostic_samples_json"],
                payload["diagnostic_samples_digest"],
                digest,
                comparison_id,
            ),
        )
        con.execute(
            "UPDATE structure_generation_drift_progress SET "
            "terminal_reason=?,diagnostic_counts_json=?,diagnostic_root=?,"
            "diagnostic_samples_json=?,diagnostic_samples_digest=? "
            "WHERE comparison_id=?",
            (
                payload["terminal_reason"],
                payload["diagnostic_counts_json"],
                payload["diagnostic_root"],
                payload["diagnostic_samples_json"],
                payload["diagnostic_samples_digest"],
                comparison_id,
            ),
        )


def _stale_terminal_class_evidence(
    store: SQLiteStore,
    comparison_id: str,
) -> tuple[dict[str, object], dict[str, object]]:
    with sqlite3.connect(store.db_path) as con:
        row = con.execute(
            "SELECT class_counts_json,class_digests_json FROM "
            "structure_generation_drift_terminal_receipts WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone()
    assert row is not None
    return json.loads(str(row[0])), json.loads(str(row[1]))


def _stale_terminal_diagnostic_evidence(
    store: SQLiteStore,
    comparison_id: str,
) -> tuple[dict[str, object], str, dict[str, object]]:
    with sqlite3.connect(store.db_path) as con:
        row = con.execute(
            "SELECT diagnostic_counts_json,diagnostic_root,diagnostic_samples_json "
            "FROM structure_generation_drift_terminal_receipts "
            "WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone()
    assert row is not None
    return json.loads(str(row[0])), str(row[1]), json.loads(str(row[2]))


def _assert_stale_terminal_evidence_unavailable(status: dict[str, object]) -> None:
    assert status["authorized"] is False
    assert status["reason"] == "structure-drift-terminal-receipt-invalid"
    for field in (
        "class_counts",
        "class_digests",
        "projection_candidate_count",
        "projection_member_count",
        "projection_exclusion_count",
        "projection_diagnostic_count",
        "projection_exclusion_counts",
        "projection_exclusion_roots",
        "diagnostic_counts",
        "diagnostic_root",
        "diagnostic_samples",
        "diagnostic_samples_digest",
    ):
        assert field not in status


def _assert_stale_terminal_public_evidence_suppressed(
    status: dict[str, object],
) -> None:
    assert status["authorized"] is False
    assert status["reason"] == "structure-drift-terminal-stale"
    for field in (
        "class_counts",
        "class_digests",
        "projection_candidate_count",
        "projection_member_count",
        "projection_exclusion_count",
        "projection_diagnostic_count",
        "projection_exclusion_counts",
        "projection_exclusion_roots",
    ):
        assert field not in status


def _assert_stale_terminal_public_aggregate(
    status: dict[str, object], *, expected_total: int, expected_root: str
) -> None:
    _assert_stale_terminal_public_evidence_suppressed(
        status,
    )
    assert status["diagnostic_total"] == expected_total
    assert status["diagnostic_root"] == expected_root
    assert "diagnostic_counts" not in status
    assert "diagnostic_samples" not in status
    assert "diagnostic_samples_digest" not in status


def test_stale_terminal_relabel_cannot_change_public_semantics(tmp_path: Path) -> None:
    store, comparison_id = _stale_unclassified_v3_store(tmp_path)
    counts, root, samples = _stale_terminal_diagnostic_evidence(store, comparison_id)
    total = sum(int(value) for value in counts.values())
    sample_values = [sample for values in samples.values() for sample in values]
    _rewrite_stale_terminal_diagnostic_evidence(
        store,
        comparison_id,
        diagnostic_counts={"forged-semantic-label": total},
        diagnostic_root=root,
        diagnostic_samples={"forged-semantic-label": sample_values},
    )

    status = store.structure_generation_drift_status()
    _assert_stale_terminal_public_aggregate(
        status,
        expected_total=total,
        expected_root=root,
    )
    check = health_module._structure_drift_health_check(
        status,
        enabled=True,
        now_ms=4_000,
        publication_sla_s=100,
    )["snapshot:structure_generation_drift"][0]
    assert check["status"] == "fail"
    assert check["terminalReason"] == "structure-drift-terminal-stale"
    assert check["diagnosticTotal"] == total
    assert "diagnosticCounts" not in check
    assert "diagnosticSamples" not in check
    assert "forged-semantic-label" not in check["output"]


def test_stale_terminal_joint_overlap_forgery_cannot_change_public_reason(
    tmp_path: Path,
) -> None:
    store, comparison_id = _stale_unclassified_v3_store(tmp_path)
    counts, digests = _stale_terminal_class_evidence(store, comparison_id)
    diagnostic_counts, root, _ = _stale_terminal_diagnostic_evidence(
        store, comparison_id
    )
    total = sum(int(value) for value in diagnostic_counts.values())
    counts["overlap-conflict"] = 1
    digests["overlap-conflict"] = "f" * 64
    _rewrite_stale_terminal_class_evidence(
        store,
        comparison_id,
        class_counts=counts,
        class_digests=digests,
        terminal_reason="drift-overlap-conflict",
    )

    _assert_stale_terminal_public_aggregate(
        store.structure_generation_drift_status(),
        expected_total=total,
        expected_root=root,
    )


def test_stale_terminal_suppresses_well_formed_wrong_positive_root(
    tmp_path: Path,
) -> None:
    store, comparison_id = _stale_overlap_v3_store(tmp_path)
    counts, digests = _stale_terminal_class_evidence(store, comparison_id)
    digests["overlap-conflict"] = "f" * 64
    _rewrite_stale_terminal_class_evidence(
        store,
        comparison_id,
        class_counts=counts,
        class_digests=digests,
    )

    _assert_stale_terminal_public_evidence_suppressed(
        store.structure_generation_drift_status(),
    )


def test_stale_unclassified_suppresses_well_formed_count_and_root_injection(
    tmp_path: Path,
) -> None:
    store, comparison_id = _stale_unclassified_v3_store(tmp_path)
    counts, digests = _stale_terminal_class_evidence(store, comparison_id)
    counts["fresh-addition"] = 100
    digests["fresh-addition"] = "e" * 64
    _rewrite_stale_terminal_class_evidence(
        store,
        comparison_id,
        class_counts=counts,
        class_digests=digests,
    )

    _assert_stale_terminal_public_evidence_suppressed(
        store.structure_generation_drift_status(),
    )


def test_stale_terminal_rejects_joint_diagnostic_evidence_forgery(
    tmp_path: Path,
) -> None:
    store, comparison_id = _stale_unclassified_v3_store(tmp_path)
    _rewrite_stale_terminal_diagnostic_evidence(
        store,
        comparison_id,
        diagnostic_counts={"forged-diagnostic": 999},
        diagnostic_root="d" * 64,
        diagnostic_samples={"forged-diagnostic": [{"market_id": "forged"}]},
    )

    _assert_stale_terminal_evidence_unavailable(
        store.structure_generation_drift_status()
    )


def test_stale_terminal_rejects_joint_terminal_reason_forgery(
    tmp_path: Path,
) -> None:
    store, comparison_id = _stale_unclassified_v3_store(tmp_path)
    _rewrite_stale_terminal_diagnostic_evidence(
        store,
        comparison_id,
        terminal_reason="drift-forged-reason",
    )

    _assert_stale_terminal_evidence_unavailable(
        store.structure_generation_drift_status()
    )


def test_stale_terminal_rejects_joint_invented_class_count_forgery(
    tmp_path: Path,
) -> None:
    store, comparison_id = _stale_overlap_v3_store(tmp_path)
    _rewrite_stale_terminal_class_evidence(
        store,
        comparison_id,
        class_counts={"invented-class": 999_999},
        class_digests={"invented-class": "a" * 64},
    )

    _assert_stale_terminal_evidence_unavailable(
        store.structure_generation_drift_status()
    )


@pytest.mark.parametrize(
    "tamper",
    (
        "negative-count",
        "bool-count",
        "missing-digest",
        "extra-zero-digest",
        "malformed-root",
        "reconstruction-count",
    ),
)
def test_stale_terminal_rejects_invalid_class_commitment(
    tmp_path: Path,
    tamper: str,
) -> None:
    store, comparison_id = _stale_overlap_v3_store(tmp_path)
    counts, digests = _stale_terminal_class_evidence(store, comparison_id)
    if tamper == "negative-count":
        counts["overlap-conflict"] = -1
    elif tamper == "bool-count":
        counts["overlap-conflict"] = True
    elif tamper == "missing-digest":
        digests.pop("overlap-conflict")
    elif tamper == "extra-zero-digest":
        digests["shared"] = "a" * 64
    elif tamper == "malformed-root":
        digests["overlap-conflict"] = "not-a-root"
    else:
        counts["fresh-addition"] += 100
        digests["fresh-addition"] = "b" * 64
    _rewrite_stale_terminal_class_evidence(
        store,
        comparison_id,
        class_counts=counts,
        class_digests=digests,
    )

    status = store.structure_generation_drift_status()
    if tamper == "reconstruction-count":
        _assert_stale_terminal_public_evidence_suppressed(
            status,
        )
    else:
        _assert_stale_terminal_evidence_unavailable(status)


@pytest.mark.parametrize(
    "tamper_field",
    (
        *_TERMINAL_RECEIPT_FIELDS,
        *_V3_RECEIPT_EXCLUSION_FIELDS,
        "receipt_digest",
        "missing",
    ),
)
def test_terminal_receipt_status_tamper_fails_closed(
    tmp_path: Path,
    tamper_field: str,
) -> None:
    store = _drift_store(tmp_path)
    comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)
    with sqlite3.connect(store.db_path) as con:
        payload = _terminal_receipt_payload(con, comparison_id)
        _insert_terminal_receipt(con, payload)
        con.execute(
            "UPDATE structure_generation_drift_progress SET phase='stale',"
            "terminal_reason='drift-unclassified',class_counts_json=?,"
            "class_digests_json=?,diagnostic_counts_json=?,diagnostic_root=?,"
            "diagnostic_samples_json=?,diagnostic_samples_digest=?,"
            "source_identity_hash=?,projection_member_receipt_digest=?,"
            "checkpoint_at_ms=3002 WHERE comparison_id=?",
            (
                payload["class_counts_json"],
                payload["class_digests_json"],
                payload["diagnostic_counts_json"],
                payload["diagnostic_root"],
                payload["diagnostic_samples_json"],
                payload["diagnostic_samples_digest"],
                payload["source_identity_hash"],
                payload["projection_member_receipt_digest"],
                comparison_id,
            ),
        )
        if tamper_field == "missing":
            con.execute("DROP TRIGGER trg_structure_drift_terminal_receipt_delete")
            con.execute(
                "DELETE FROM structure_generation_drift_terminal_receipts "
                "WHERE comparison_id=?",
                (comparison_id,),
            )
        else:
            con.execute("DROP TRIGGER trg_structure_drift_terminal_receipt_update")
            replacement: object = (
                9_999
                if tamper_field.endswith("_ms")
                or tamper_field in {"legacy_snapshot_id", "generation_snapshot_id"}
                else "e" * 64
            )
            con.execute(
                f"UPDATE structure_generation_drift_terminal_receipts SET "
                f"{tamper_field}=? WHERE comparison_id=?",  # noqa: S608 - fixed test tuple
                (replacement, comparison_id),
            )
    status = store.structure_generation_drift_status()
    assert status["reason"] == "structure-drift-terminal-receipt-invalid"
    assert "class_counts" not in status
    assert "diagnostic_counts" not in status
    assert "diagnostic_samples" not in status
    check = health_module._structure_drift_health_check(
        status,
        enabled=True,
        now_ms=3_003,
        publication_sla_s=100,
    )["snapshot:structure_generation_drift"][0]
    assert check["observedValue"] == "terminal-receipt-invalid"
    assert check["output"] == "structure-drift-terminal-receipt-invalid"
    assert "diagnosticCounts" not in check
    assert "diagnosticSamples" not in check


def test_valid_terminal_receipt_status_exposes_authenticated_evidence(
    tmp_path: Path,
    daemon_settings_for_test,
) -> None:
    store = _drift_store(tmp_path)
    comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)
    with sqlite3.connect(store.db_path) as con:
        payload = _terminal_receipt_payload(con, comparison_id)
        receipt_digest = _insert_terminal_receipt(con, payload)
        con.execute(
            "UPDATE structure_generation_drift_progress SET phase='stale',"
            "terminal_reason=?,class_counts_json=?,class_digests_json=?,"
            "diagnostic_counts_json=?,diagnostic_root=?,diagnostic_samples_json=?,"
            "diagnostic_samples_digest=?,source_identity_hash=?,"
            "projection_member_receipt_digest=?,checkpoint_at_ms=? "
            "WHERE comparison_id=?",
            (
                payload["terminal_reason"],
                payload["class_counts_json"],
                payload["class_digests_json"],
                payload["diagnostic_counts_json"],
                payload["diagnostic_root"],
                payload["diagnostic_samples_json"],
                payload["diagnostic_samples_digest"],
                payload["source_identity_hash"],
                payload["projection_member_receipt_digest"],
                payload["checkpoint_at_ms"],
                comparison_id,
            ),
        )
    status = store.structure_generation_drift_status()
    assert status["terminal_receipt_digest"] == receipt_digest
    _assert_stale_terminal_public_aggregate(
        status,
        expected_total=1,
        expected_root=str(payload["diagnostic_root"]),
    )
    check = health_module._structure_drift_health_check(
        status,
        enabled=True,
        now_ms=3_003,
        publication_sla_s=100,
    )["snapshot:structure_generation_drift"][0]
    assert check["observedValue"] == "terminal-stale"
    assert check["comparisonId"] == comparison_id
    assert check["terminalReason"] == "structure-drift-terminal-stale"
    assert check["diagnosticTotal"] == 1
    assert "diagnosticCounts" not in check
    assert "diagnosticSamples" not in check
    assert check["diagnosticRoot"] == payload["diagnostic_root"]
    assert check["checkpointAtMs"] == payload["checkpoint_at_ms"]

    from unittest.mock import MagicMock

    from starlette.testclient import TestClient

    from polyarb.http.app import create_app
    from polyarb.perception.store import OpportunityPerceptionStore

    settings = daemon_settings_for_test.model_copy(update={
        "db_path": store.db_path,
        "structure_generation_drift_compare_enabled": True,
    })
    OpportunityPerceptionStore(store.db_path).init_schema()
    client = TestClient(create_app(
        scheduler=MagicMock(),
        sqlite_store=store,
        settings=settings,
    ))
    for endpoint in ("/health", "/healthz"):
        endpoint_check = client.get(endpoint).json()["checks"][
            "snapshot:structure_generation_drift"
        ][0]
        assert endpoint_check["comparisonId"] == comparison_id
        assert endpoint_check["diagnosticTotal"] == 1
        assert endpoint_check["diagnosticRoot"] == payload["diagnostic_root"]
        assert "diagnosticCounts" not in endpoint_check
        assert "diagnosticSamples" not in endpoint_check
        assert endpoint_check["checkpointAtMs"] == payload["checkpoint_at_ms"]


def test_mixed_terminal_receipt_with_valid_digest_fails_closed(tmp_path: Path) -> None:
    store = _drift_store(tmp_path)
    comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)
    with sqlite3.connect(store.db_path) as con:
        payload = _terminal_receipt_payload(con, comparison_id)
        _insert_terminal_receipt(con, payload)
        con.execute(
            "UPDATE structure_generation_drift_progress SET phase='stale',"
            "terminal_reason=?,class_counts_json=?,class_digests_json=?,"
            "diagnostic_counts_json=?,diagnostic_root=?,diagnostic_samples_json=?,"
            "diagnostic_samples_digest=?,source_identity_hash=?,"
            "projection_member_receipt_digest=?,checkpoint_at_ms=? "
            "WHERE comparison_id=?",
            (
                payload["terminal_reason"],
                payload["class_counts_json"],
                payload["class_digests_json"],
                payload["diagnostic_counts_json"],
                payload["diagnostic_root"],
                payload["diagnostic_samples_json"],
                payload["diagnostic_samples_digest"],
                payload["source_identity_hash"],
                payload["projection_member_receipt_digest"],
                payload["checkpoint_at_ms"],
                comparison_id,
            ),
        )
        mixed_payload = {**payload, "publication_id": "mixed-publication"}
        mixed_digest = sqlite_store_module._structure_drift_terminal_receipt_digest(
            mixed_payload
        )
        con.execute("DROP TRIGGER trg_structure_drift_terminal_receipt_update")
        con.execute(
            "UPDATE structure_generation_drift_terminal_receipts SET "
            "publication_id=?,receipt_digest=? WHERE comparison_id=?",
            (mixed_payload["publication_id"], mixed_digest, comparison_id),
        )
    status = store.structure_generation_drift_status()
    assert status["reason"] == "structure-drift-terminal-receipt-invalid"
    assert "class_counts" not in status
    assert "diagnostic_samples" not in status


def _raw_market(
    market_id: str, *, group_id: str, active: bool = True
) -> dict[str, object]:
    return {
        "id": market_id,
        "conditionId": f"condition-{market_id}",
        "clobTokenIds": json.dumps([f"yes-{market_id}", f"no-{market_id}"]),
        "active": active,
        "closed": False,
        "negRisk": True,
        "negRiskMarketID": group_id,
    }


def _seal_fixture_event_members(store: SQLiteStore, window_id: str) -> None:
    """Upgrade an old direct-SQL fixture to natural event/member authority."""
    with sqlite3.connect(store.db_path) as con:
        rows = con.execute(
            "SELECT event_id,payload_json,source_ordinal FROM "
            "structure_sync_event_staging WHERE window_id=? ORDER BY source_ordinal",
            (window_id,),
        ).fetchall()
        source_chain = RowChainSHA256.new("source-event")
        for event_id, payload_json, ordinal in rows:
            raw = json.loads(str(payload_json))
            group_id = raw.get("negRiskMarketID")
            if not (
                isinstance(group_id, str)
                and group_id
                and group_id.strip() == group_id
            ):
                group_id = None
            payload_hash = hashlib.sha256(str(payload_json).encode()).hexdigest()
            payload_length = len(str(payload_json).encode())
            con.execute(
                "INSERT INTO structure_sync_event_metadata_staging VALUES "
                "(?,?,?,?,?,?,?)",
                (
                    window_id,
                    event_id,
                    ordinal,
                    group_id,
                    payload_hash,
                    payload_length,
                    STRUCTURE_EVENT_SOURCE_CONTRACT,
                ),
            )
            source_chain.update(
                (
                    STRUCTURE_EVENT_SOURCE_CONTRACT,
                    event_id,
                    ordinal,
                    group_id,
                    payload_hash,
                    payload_length,
                )
            )
        con.execute(
            "INSERT INTO structure_sync_event_source_progress VALUES (?,?,?,2001)",
            (window_id, len(rows), source_chain.to_json()),
        )
        source_receipt = (
            window_id,
            len(rows),
            source_chain.hexdigest(),
            1,
            "",
            STRUCTURE_EVENT_SOURCE_CONTRACT,
            2001,
        )
        source_digest = sqlite_store_module._structure_event_source_receipt_digest(
            source_receipt
        )
        con.execute(
            "INSERT INTO structure_sync_event_source_receipts VALUES ("
            + ",".join("?" for _ in range(8))
            + ")",
            (*source_receipt, source_digest),
        )
        source_identity = hashlib.sha256(
            json.dumps(
                (window_id, len(rows), source_chain.hexdigest(), source_digest),
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        member_chain = RowChainSHA256.new("source-event")
        member_state = sqlite_store_module._event_member_progress_state(
            member_chain=member_chain,
            source_event_count=len(rows),
            source_event_root=source_chain.hexdigest(),
            source_identity_hash=source_identity,
            window_checkpoint_at_ms=2001,
        )
        diagnostic_state = RowChainSHA256.new("diagnostic/unclassified").to_json()
        checkpoint = sqlite_store_module._structure_event_member_checkpoint_digest(
            (source_digest, "", 0, 0, 0, 0, "", member_state, diagnostic_state)
        )
        con.execute(
            "INSERT INTO structure_sync_event_member_progress VALUES "
            "(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                window_id,
                "",
                0,
                0,
                0,
                member_state,
                diagnostic_state,
                2001,
                None,
                None,
                0,
                source_digest,
                "",
                checkpoint,
            ),
        )
        con.execute(
            "UPDATE structure_sync_windows SET event_pages=1,event_cursor=NULL "
            "WHERE id=?",
            (window_id,),
        )
        con.execute(
            "INSERT INTO structure_sync_event_market_backfill_progress("
            "window_id,window_checkpoint_at_ms,checkpoint_at_ms,completed_at_ms) "
            "SELECT id,checkpoint_at_ms,checkpoint_at_ms,checkpoint_at_ms FROM "
            "structure_sync_windows WHERE id=?",
            (window_id,),
        )
    for _ in range(20):
        if store.structure_event_member_status(window_id=window_id).get("sealed") is True:
            break
        result = store.advance_structure_event_member_staging_chunk(
            window_id=window_id, limit=500
        )
        assert result.get("reason") is None and result.get("failure_reason") is None
    else:
        pytest.fail("fixture event-member authority did not seal within 20 chunks")


def _drift_store(
    tmp_path: Path,
    *,
    omit_generation_market_id: str | None = None,
    sibling_recovery: bool = False,
) -> SQLiteStore:
    store = SQLiteStore(tmp_path / "drift-e2e.db")
    store.init_schema()
    main_members = (
        (("shared", True), ("current-nontradable", False))
        if sibling_recovery
        else (("shared", True), ("addition", True))
    )
    raw_main = {
        "id": "event-main",
        "slug": "event-main",
        "active": True,
        "closed": False,
        "negRisk": True,
        "enableNegRisk": True,
        "negRiskAugmented": False,
        "negRiskMarketID": "group-main",
        "markets": [
            {
                "id": market_id,
                "active": active,
                "closed": False,
                "negRiskOther": False,
            }
            for market_id, active in main_members
        ],
    }

    def single_event(
        event_id: str, group_id: str, market_id: str, *, active: bool = True
    ) -> dict[str, object]:
        return {
            "id": event_id,
            "slug": event_id,
            "active": True,
            "closed": False,
            "negRisk": True,
            "enableNegRisk": True,
            "negRiskAugmented": False,
            "negRiskMarketID": group_id,
            "markets": [
                {
                    "id": market_id,
                    "active": active,
                    "closed": False,
                    "negRiskOther": False,
                }
            ],
        }

    raw_events = (
        (
            raw_main,
            single_event("event-addition", "group-addition", "addition"),
            single_event("event-event-only", "group-event-only", "event-only"),
        )
        if sibling_recovery
        else (
            raw_main,
            single_event(
                "event-current",
                "group-current",
                "current-nontradable",
                active=False,
            ),
            single_event("event-event-only", "group-event-only", "event-only"),
        )
    )
    raw_markets = {
        "shared": _raw_market("shared", group_id="group-main"),
        "addition": _raw_market(
            "addition", group_id="group-addition" if sibling_recovery else "group-main"
        ),
        "current-nontradable": _raw_market(
            "current-nontradable",
            group_id="group-main" if sibling_recovery else "group-current",
            active=False,
        ),
        "market-side": _raw_market("market-side", group_id="group-market-a"),
    }
    complete_ids = frozenset(raw_markets)
    event_projections = tuple(
        project_legacy_compatible_event(
            raw_event,
            event_source_ordinal=ordinal,
            complete_market_ids=complete_ids,
        )
        for ordinal, raw_event in enumerate(raw_events, 1)
    )
    market_projections = {
        market_id: project_legacy_compatible_market(
            raw,
            event_ids=(
                ()
                if market_id == "market-side"
                else ("event-current",)
                if market_id == "current-nontradable"
                else ("event-addition",)
                if sibling_recovery and market_id == "addition"
                else ("event-main",)
            ),
            taken_at_ms=2_000,
        )
        for market_id, raw in raw_markets.items()
    }
    with sqlite3.connect(store.db_path) as con:
        con.executemany(
            "INSERT INTO snapshots(id,taken_at_ms,finished_at_ms,mode,market_count,"
            "market_view_published,data_product,archive_status,snapshot_status,is_valid,"
            "parquet_path) VALUES (?,?,?,'full',?,1,'structure','legacy','ok',1,'')",
            ((1, 1_000, 1_001, 5), (2, 2_000, 2_001, 3)),
        )
        con.execute(
            "INSERT INTO snapshot_source_coverage(snapshot_id,completed,market_items,"
            "event_items) VALUES (1,1,5,1)"
        )
        legacy_members = (
            (
                ("event-main", "group-main", "shared"),
                ("event-main", "group-main", "current-nontradable"),
                ("event-event-only", "group-event-only", "event-only"),
                ("event-market-a", "group-market-a", "market-side"),
                ("event-fresh", "group-fresh", "fresh-absent"),
            )
            if sibling_recovery
            else (
                ("event-main", "group-main", "shared"),
                ("event-current", "group-current", "current-nontradable"),
                ("event-event-only", "group-event-only", "event-only"),
                ("event-market-a", "group-market-a", "market-side"),
                ("event-fresh", "group-fresh", "fresh-absent"),
            )
        )
        con.executemany(
            "INSERT INTO event_market_memberships(snapshot_id,event_id,"
            "neg_risk_market_id,market_id,member_kind,active,closed) "
            "VALUES (1,?,?,?,'named',1,0)",
            legacy_members,
        )
        for event_id, group_id, market_id in legacy_members:
            legacy_hash = hashlib.sha256(
                json.dumps(
                    [(event_id, group_id, market_id, "named", True, False)],
                    separators=(",", ":"),
                ).encode()
            ).hexdigest()
            con.execute(
                "INSERT OR IGNORE INTO neg_risk_group_truth("
                "snapshot_id,event_id,neg_risk_market_id,"
                "neg_risk_type,expected_member_count,active_named_count,membership_hash,"
                "quality) VALUES (1,?,?,'standard',1,1,?,"
                "'complete-supported')",
                (event_id, group_id, legacy_hash),
            )
        con.executemany(
            "INSERT INTO markets(snapshot_id,market_id,condition_id,yes_token_id,"
            "no_token_id,active,closed,neg_risk,neg_risk_market_id,fetched_at_ms,"
            "incomplete,event_id) VALUES (1,?,?,?, ?,1,0,1,?,1000,0,?)",
            (
                (
                    market_id,
                    f"condition-{market_id}",
                    f"yes-{market_id}",
                    f"no-{market_id}",
                    group_id,
                    event_id,
                )
                for event_id, group_id, market_id in legacy_members
            ),
        )
        for projection in event_projections:
            con.executemany(
                "INSERT INTO structure_generation_memberships(snapshot_id,event_id,"
                "neg_risk_market_id,market_id,member_kind,active,closed) "
                "VALUES (2,?,?,?,?,?,?)",
                (
                    (
                        member.event_id,
                        member.group_id,
                        member.market_id,
                        member.member_kind,
                        int(member.active),
                        int(member.closed),
                    )
                    for member in projection.members
                    if member.market_id != omit_generation_market_id
                ),
            )
            con.executemany(
                "INSERT INTO structure_generation_group_truth(snapshot_id,event_id,"
                "neg_risk_market_id,neg_risk_type,expected_member_count,"
                "active_named_count,membership_hash,quality,reason) "
                "VALUES (2,?,?,?,?,?,?,?,?)",
                (
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
                    for truth in projection.truths
                ),
            )
        market_columns = (
            "market_id,condition_id,slug,question,yes_token_id,no_token_id,mid_price,"
            "liquidity_usd,volume_usd,best_bid_price,best_bid_size,best_ask_price,"
            "best_ask_size,end_time_ms,active,closed,neg_risk,neg_risk_market_id,"
            "fetched_at_ms,page_fetched_at_ms,incomplete,event_id"
        )
        for projection in market_projections.values():
            if projection.row is None:
                continue
            if projection.row["market_id"] == omit_generation_market_id:
                continue
            con.execute(
                "INSERT INTO structure_generation_markets(snapshot_id,"
                + market_columns
                + ") VALUES (2,"
                + ",".join("?" for _ in projection.row)
                + ")",
                tuple(projection.row.values()),
            )
        generation_issues = [
            issue for projection in event_projections for issue in projection.issues
        ] + [
            projection.issue
            for projection in market_projections.values()
            if projection.issue is not None
        ]
        con.executemany(
            "INSERT INTO structure_generation_issues(snapshot_id,issue_index,layer,"
            "category,market_id,detail,raw_payload) VALUES (2,?,?,?,?,?,?)",
            (
                (
                    issue_index,
                    int(issue["layer"]),
                    str(issue["category"]),
                    issue.get("market_id"),
                    issue.get("detail"),
                    issue.get("raw_payload"),
                )
                for issue_index, issue in enumerate(generation_issues)
            ),
        )
        con.execute(
            "INSERT INTO structure_sync_windows(id,status,started_at_ms,checkpoint_at_ms,"
            "published_snapshot_id) VALUES ('window-2','open',2000,2001,NULL)"
        )
        con.executemany(
            "INSERT INTO structure_sync_event_staging(window_id,event_id,payload_json,"
            "source_ordinal) VALUES ('window-2',?,?,?)",
            (
                (str(raw_event["id"]), json.dumps(raw_event), ordinal)
                for ordinal, raw_event in enumerate(raw_events, 1)
            ),
        )
        relations = [
            (str(member["id"]), str(raw_event["id"]), ordinal)
            for ordinal, raw_event in enumerate(raw_events, 1)
            for member in raw_event["markets"]
        ]
        con.executemany(
            "INSERT INTO structure_sync_event_market_staging(window_id,market_id,"
            "event_id,source_ordinal) VALUES ('window-2',?,?,?)",
            relations,
        )
        con.execute(
            "UPDATE structure_sync_windows SET status='events_complete' WHERE id='window-2'"
        )
        con.executemany(
            "INSERT INTO structure_sync_market_staging(window_id,market_id,payload_json,"
            "source_ordinal) VALUES ('window-2',?,?,?)",
            (
                (market_id, json.dumps(raw), ordinal)
                for ordinal, (market_id, raw) in enumerate(raw_markets.items(), 1)
            ),
        )
        con.execute(
            "UPDATE structure_sync_windows SET status='published',"
            "published_snapshot_id=2 WHERE id='window-2'"
        )
        cert = "a" * 64
        cert_counts = json.dumps(
            {"source_events": 3, "source_markets": 4},
            sort_keys=True,
            separators=(",", ":"),
        )
        con.execute(
            "INSERT INTO structure_publications(publication_id,window_id,snapshot_id,"
            "status,normalization_contract_version,expected_counts_json,"
            "committed_counts_json,validation_hash,certification_component,"
            "certification_hash,certification_counts_json,created_at_ms,checkpoint_at_ms) "
            "VALUES ('publication-2','window-2',2,'published','contract-v1','{}','{}',"
            "?,'bounded-complete',?,?,2000,2001)",
            (cert, cert, cert_counts),
        )
        legacy_universe, legacy_truth = sqlite_store_module._structure_universe_hash(
            con, snapshot_id=1, generation=False
        )
        generation_universe, generation_truth = (
            sqlite_store_module._structure_universe_hash(
                con, snapshot_id=2, generation=True
            )
        )
        exact_digest = sqlite_store_module._comparison_receipt_digest(
            generation_snapshot_id=2,
            publication_id="publication-2",
            legacy_snapshot_id=1,
            legacy_market_count=5,
            generation_market_count=3,
            legacy_universe_hash=legacy_universe,
            generation_universe_hash=generation_universe,
            legacy_source_truth_hash=legacy_truth,
            generation_source_truth_hash=generation_truth,
            generation_validation_hash=cert,
            created_at_ms=2_001,
        )
        con.execute(
            "INSERT INTO structure_generation_comparison_receipts("
            "generation_snapshot_id,publication_id,legacy_snapshot_id,"
            "legacy_market_count,generation_market_count,legacy_universe_hash,"
            "generation_universe_hash,legacy_source_truth_hash,"
            "generation_source_truth_hash,generation_validation_hash,created_at_ms,"
            "receipt_digest) VALUES (2,'publication-2',1,5,3,?,?,?,?,?,?,?)",
            (
                legacy_universe,
                generation_universe,
                legacy_truth,
                generation_truth,
                cert,
                2_001,
                exact_digest,
            ),
        )
        con.execute(
            "INSERT INTO current_structure_generation(id,snapshot_id,publication_id,"
            "validation_hash,counts_json,certification_component,"
            "comparison_receipt_digest,switched_at_ms) VALUES "
            "(1,2,'publication-2',?,'{}','bounded-complete',?,2001)",
            (cert, exact_digest),
        )
    _seal_fixture_event_members(store, "window-2")
    return store


def test_complete_projection_detects_generation_omission(tmp_path: Path) -> None:
    store = _drift_store(tmp_path, omit_generation_market_id="addition")
    commitment: FreshProjectionCommitment | None = None
    while commitment is None or not commitment.complete:
        commitment = store.advance_structure_drift_fresh_projection_commitment(
            publication_id="publication-2",
            generation_snapshot_id=2,
            commitment=commitment,
            limit=1,
        )

    generation = store.fetch_structure_drift_member_chunk(
        snapshot_id=2,
        generation=True,
        after_market_id=None,
        limit=500,
    )
    generation_digest = RowChainSHA256.new("projection-member")
    for member in generation:
        generation_digest.update(_member_tuple(member))

    assert {member.market_id for member in generation} == {"shared"}
    assert commitment.complete is True
    assert commitment.member_count == 2
    assert commitment.member_count != len(generation)
    assert commitment.root != generation_digest.hexdigest()
    assert commitment.matches_generation(
        count=len(generation),
        root=generation_digest.hexdigest(),
    ) is False


def test_v4_nullable_ordinary_event_member_is_excluded_not_diagnostic(
    tmp_path: Path,
) -> None:
    """The observed nullable ordinary-event shape is explicit v4 evidence."""
    store = _drift_store(tmp_path)
    nullable_event = {
        "id": "event-event-only",
        "slug": "event-event-only",
        "active": True,
        "closed": True,
        "negRisk": None,
        "enableNegRisk": False,
        "negRiskMarketID": None,
        "markets": [
            {
                "id": "event-only",
                "active": True,
                "closed": True,
                "negRiskOther": False,
            }
        ],
    }
    with sqlite3.connect(store.db_path) as con:
        for (trigger_name,) in con.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' AND "
            "tbl_name IN ('structure_sync_event_staging',"
            "'structure_sync_event_member_staging')"
        ):
            con.execute(f'DROP TRIGGER "{trigger_name}"')
        con.execute(
            "UPDATE structure_sync_event_staging SET payload_json=? "
            "WHERE window_id='window-2' AND event_id='event-event-only'",
            (json.dumps(nullable_event),),
        )
        con.execute(
            "UPDATE structure_sync_event_member_staging SET group_id=NULL,"
            "active=1,closed=1 WHERE window_id='window-2' AND "
            "event_id='event-event-only' AND market_id='event-only'"
        )

    chunk = store._fetch_structure_drift_fresh_projection_chunk(
        publication_id="publication-2",
        generation_snapshot_id=2,
        cursor=None,
        limit=500,
        classifier_contract=STRUCTURE_DRIFT_CLASSIFIER_V4,
    )

    assert not [item for item in chunk.diagnostics if item.market_id == "event-only"]
    assert [
        item.reason
        for item in chunk.exclusions
        if item.envelope.market_id == "event-only"
    ] == ["non-neg-risk-event-member"]


def _reshape_as_production_845_848(store: SQLiteStore) -> None:
    """Retain fixture semantics while matching the production identity topology."""
    with sqlite3.connect(store.db_path) as con:
        con.execute("PRAGMA foreign_keys=OFF")
        for (trigger_name,) in con.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger'"
        ).fetchall():
            con.execute(f'DROP TRIGGER "{trigger_name}"')
        con.execute("UPDATE snapshots SET id=845 WHERE id=1")
        con.execute("UPDATE snapshots SET id=848 WHERE id=2")
        con.execute("UPDATE snapshot_source_coverage SET snapshot_id=845")
        for table in (
            "event_market_memberships",
            "neg_risk_group_truth",
            "markets",
        ):
            con.execute(f"UPDATE {table} SET snapshot_id=845 WHERE snapshot_id=1")
        for table in (
            "structure_generation_memberships",
            "structure_generation_group_truth",
            "structure_generation_markets",
            "structure_generation_issues",
        ):
            con.execute(f"UPDATE {table} SET snapshot_id=848 WHERE snapshot_id=2")
        con.execute(
            "UPDATE structure_sync_windows SET id='window-97b',"
            "published_snapshot_id=848 WHERE id='window-2'"
        )
        authority_tables = (
            "structure_sync_event_staging",
            "structure_sync_event_market_staging",
            "structure_sync_market_staging",
            "structure_sync_event_metadata_staging",
            "structure_sync_event_source_progress",
            "structure_sync_event_source_receipts",
            "structure_sync_event_conflict_summaries",
            "structure_sync_event_conflict_proofs",
            "structure_sync_event_conflict_merkle_nodes",
            "structure_sync_event_member_staging",
            "structure_sync_event_member_progress",
            "structure_sync_event_member_receipts",
            "structure_sync_event_group_truth_staging",
            "structure_sync_event_group_truth_progress",
            "structure_sync_event_market_backfill_progress",
        )
        for table in authority_tables:
            con.execute(
                f"UPDATE {table} SET window_id='window-97b' WHERE window_id='window-2'"
            )
        source_receipt = list(con.execute(
            "SELECT * FROM structure_sync_event_source_receipts "
            "WHERE window_id='window-97b'"
        ).fetchone())
        source_receipt[-1] = sqlite_store_module._structure_event_source_receipt_digest(
            tuple(source_receipt[:-1])
        )
        con.execute(
            "UPDATE structure_sync_event_source_receipts SET receipt_digest=? "
            "WHERE window_id='window-97b'", (source_receipt[-1],)
        )
        source_identity = hashlib.sha256(json.dumps(
            ("window-97b", source_receipt[1], source_receipt[2], source_receipt[-1]),
            separators=(",", ":"),
        ).encode()).hexdigest()
        member = list(con.execute(
            "SELECT * FROM structure_sync_event_member_receipts "
            "WHERE window_id='window-97b'"
        ).fetchone())
        summaries = con.execute(
            "SELECT event_id,global_conflict FROM "
            "structure_sync_event_conflict_summaries WHERE window_id='window-97b' "
            "ORDER BY event_id"
        ).fetchall()
        leaves = [
            sqlite_store_module._event_conflict_leaf_hash(
                window_id="window-97b", event_id=str(event_id),
                global_conflict=bool(global_conflict),
            )
            for event_id, global_conflict in summaries
        ]
        conflict_merkle_root, conflict_proofs = (
            sqlite_store_module._event_conflict_merkle_proofs(leaves)
        )
        for index, ((event_id, _global_conflict), leaf_hash, proof_json) in enumerate(
            zip(summaries, leaves, conflict_proofs, strict=True)
        ):
            con.execute(
                "UPDATE structure_sync_event_conflict_proofs SET leaf_index=?,"
                "leaf_hash=?,proof_json=? WHERE window_id='window-97b' AND event_id=?",
                (index, leaf_hash, proof_json, event_id),
            )
        member[3] = source_identity
        member[16] = conflict_merkle_root
        member[13] = sqlite_store_module._structure_event_member_receipt_digest(
            tuple(member[:13]), event_conflict_count=int(member[14]),
            event_conflict_root=str(member[15]),
            event_conflict_merkle_root=str(member[16]),
            source_group_truth_count=int(member[17]),
            source_group_truth_root=str(member[18]),
        )
        con.execute(
            "UPDATE structure_sync_event_member_receipts SET source_identity_hash=?,"
            "receipt_digest=?,event_conflict_merkle_root=? WHERE window_id='window-97b'",
            (source_identity, member[13], conflict_merkle_root),
        )
        member_progress = list(con.execute(
            "SELECT * FROM structure_sync_event_member_progress "
            "WHERE window_id='window-97b'"
        ).fetchone())
        state = list(sqlite_store_module._read_event_member_progress_state(
            str(member_progress[5])
        ))
        state[3] = source_identity
        member_state = sqlite_store_module._event_member_progress_state(
            member_chain=state[0], source_event_count=state[1],
            source_event_root=state[2], source_identity_hash=state[3],
            window_checkpoint_at_ms=state[4], phase=state[5],
            conflict_cursor=state[6], event_conflict_chain=state[7],
            merkle_level=state[8], merkle_cursor=state[9], merkle_width=state[10],
            merkle_pending_index=state[11], merkle_pending_hash=state[12],
            proof_cursor=state[13], proof_count=state[14],
        )
        member_checkpoint = sqlite_store_module._structure_event_member_checkpoint_digest((
            source_receipt[-1], member_progress[1], int(member_progress[2]),
            int(member_progress[10]), int(member_progress[4]), int(member_progress[3]),
            member_progress[12], member_state, member_progress[6],
        ))
        con.execute(
            "UPDATE structure_sync_event_member_progress SET member_state=?,"
            "source_receipt_digest=?,checkpoint_digest=? WHERE window_id='window-97b'",
            (member_state, source_receipt[-1], member_checkpoint),
        )
        group = list(con.execute(
            "SELECT * FROM structure_sync_event_group_truth_progress "
            "WHERE window_id='window-97b'"
        ).fetchone())
        group[13] = sqlite_store_module._structure_event_group_truth_checkpoint_digest((
            source_receipt[-1], *group[1:11], group[14],
        ))
        con.execute(
            "UPDATE structure_sync_event_group_truth_progress SET checkpoint_digest=? "
            "WHERE window_id='window-97b'", (group[13],)
        )
        con.execute(
            "UPDATE structure_publications SET publication_id='publication-848',"
            "window_id='window-97b',snapshot_id=848 WHERE "
            "publication_id='publication-2'"
        )
        receipt = con.execute(
            "SELECT legacy_market_count,generation_market_count,legacy_universe_hash,"
            "generation_universe_hash,legacy_source_truth_hash,"
            "generation_source_truth_hash,generation_validation_hash,created_at_ms "
            "FROM structure_generation_comparison_receipts"
        ).fetchone()
        exact_digest = sqlite_store_module._comparison_receipt_digest(
            generation_snapshot_id=848,
            publication_id="publication-848",
            legacy_snapshot_id=845,
            legacy_market_count=int(receipt[0]),
            generation_market_count=int(receipt[1]),
            legacy_universe_hash=str(receipt[2]),
            generation_universe_hash=str(receipt[3]),
            legacy_source_truth_hash=str(receipt[4]),
            generation_source_truth_hash=str(receipt[5]),
            generation_validation_hash=str(receipt[6]),
            created_at_ms=int(receipt[7]),
        )
        con.execute(
            "UPDATE structure_generation_comparison_receipts SET "
            "generation_snapshot_id=848,publication_id='publication-848',"
            "legacy_snapshot_id=845,receipt_digest=?",
            (exact_digest,),
        )
        con.execute(
            "UPDATE current_structure_generation SET snapshot_id=848,"
            "publication_id='publication-848',comparison_receipt_digest=? WHERE id=1",
            (exact_digest,),
        )
    store.init_schema()


def test_drift_v2_schema_binds_algorithm_reason_and_member_scan_indexes(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "drift-v2-schema.db")
    store.init_schema()
    with sqlite3.connect(store.db_path) as con:
        progress_columns = {
            str(row[1]): (int(row[3]), row[4])
            for row in con.execute(
                "PRAGMA table_info(structure_generation_drift_progress)"
            )
        }
        receipt_columns = {
            str(row[1]): (int(row[3]), row[4])
            for row in con.execute(
                "PRAGMA table_info(structure_generation_drift_receipts)"
            )
        }
        indexes = {
            str(row[0])
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            )
        }

    assert {"hash_algorithm", "terminal_reason"} <= set(progress_columns)
    assert "hash_algorithm" in receipt_columns
    assert progress_columns["hash_algorithm"] == (
        1,
        "'serializable-sha256-v1'",
    )
    assert receipt_columns["hash_algorithm"] == (
        1,
        "'serializable-sha256-v1'",
    )
    assert {
        "generation_projection_member_comparison_count",
        "generation_projection_member_comparison_root",
        "generation_source_group_truth_comparison_count",
        "generation_source_group_truth_comparison_root",
    } <= set(receipt_columns)
    assert "idx_structure_generation_memberships_drift_scan" in indexes
    assert "idx_event_market_memberships_drift_scan" in indexes


def test_existing_v2_receipt_schema_adds_comparison_commitments_idempotently(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "drift-v2-comparison-columns.db")
    store.init_schema()
    mirror_columns = (
        "generation_projection_member_comparison_count",
        "generation_projection_member_comparison_root",
        "generation_source_group_truth_comparison_count",
        "generation_source_group_truth_comparison_root",
    )
    with sqlite3.connect(store.db_path) as con:
        con.execute("DROP TRIGGER trg_structure_drift_receipt_update")
        con.execute("DROP TRIGGER trg_structure_drift_receipt_delete")
        for column in mirror_columns:
            con.execute(
                "ALTER TABLE structure_generation_drift_receipts "
                f"DROP COLUMN {column}"
            )

    store.init_schema()
    store.init_schema()
    with sqlite3.connect(store.db_path) as con:
        columns = {
            str(row[1])
            for row in con.execute(
                "PRAGMA table_info(structure_generation_drift_receipts)"
            )
        }
        triggers = {
            str(row[0])
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger' AND "
                "tbl_name='structure_generation_drift_receipts'"
            )
        }

    assert set(mirror_columns) <= columns
    assert {
        "trg_structure_drift_receipt_update",
        "trg_structure_drift_receipt_delete",
    } <= triggers


@pytest.mark.parametrize("failure_point", ("index", "analyze"))
def test_drift_v2_schema_startup_failure_reinitializes_without_business_row_loss(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_point: str,
) -> None:
    store = _drift_store(tmp_path)
    business_tables = (
        "snapshots",
        "structure_publications",
        "structure_generation_memberships",
        "event_market_memberships",
        "markets",
    )
    with sqlite3.connect(store.db_path) as con:
        business_before = {
            table: con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in business_tables
        }
        if failure_point == "index":
            con.execute(
                "DROP INDEX idx_structure_generation_memberships_drift_scan"
            )

    original_connect = store._connect_writer

    def connect_with_startup_fault(
        *, timeout_s: float | None = None
    ) -> sqlite3.Connection:
        con = original_connect(timeout_s=timeout_s)

        def deny_selected_operation(
            action: int,
            arg1: str | None,
            _arg2: str | None,
            _database: str | None,
            _trigger: str | None,
        ) -> int:
            index_failure = (
                failure_point == "index"
                and action == sqlite3.SQLITE_CREATE_INDEX
                and arg1 == "idx_structure_generation_memberships_drift_scan"
            )
            analyze_failure = (
                failure_point == "analyze" and action == sqlite3.SQLITE_ANALYZE
            )
            return sqlite3.SQLITE_DENY if index_failure or analyze_failure else sqlite3.SQLITE_OK

        con.set_authorizer(deny_selected_operation)
        return con

    monkeypatch.setattr(store, "_connect_writer", connect_with_startup_fault)
    with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
        store.init_schema()
    monkeypatch.setattr(store, "_connect_writer", original_connect)

    store.init_schema()
    with sqlite3.connect(store.db_path) as con:
        business_after = {
            table: con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in business_tables
        }
        indexes = {
            str(row[0])
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            )
        }
        analyzed_indexes = {
            str(row[0])
            for row in con.execute(
                "SELECT idx FROM sqlite_stat1 WHERE idx IN (?,?)",
                (
                    "idx_structure_generation_memberships_drift_scan",
                    "idx_event_market_memberships_drift_scan",
                ),
            )
        }

    assert business_after == business_before
    assert {
        "idx_structure_generation_memberships_drift_scan",
        "idx_event_market_memberships_drift_scan",
    } <= indexes
    assert {
        "idx_structure_generation_memberships_drift_scan",
        "idx_event_market_memberships_drift_scan",
    } <= analyzed_indexes


_V1_PROGRESS_COLUMNS = (
    "comparison_id",
    "legacy_snapshot_id",
    "generation_snapshot_id",
    "publication_id",
    "window_id",
    "normalization_contract_version",
    "exact_receipt_digest",
    "pointer_validation_hash",
    "generation_certification_hash",
    "source_event_count",
    "source_market_count",
    "source_event_hash",
    "source_market_hash",
    "source_identity_hash",
    "phase",
    "row_cursor_json",
    "digest_state_json",
    "class_counts_json",
    "class_digests_json",
    "created_at_ms",
    "checkpoint_at_ms",
)
_DRIFT_RECEIPT_V2_DIGEST_FIELDS = (
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
_DRIFT_RECEIPT_V3_DIGEST_FIELDS = (
    *_DRIFT_RECEIPT_V2_DIGEST_FIELDS[:-1],
    "projection_candidate_count",
    "projection_exclusion_count",
    "projection_exclusion_counts_json",
    "projection_exclusion_roots_json",
    "created_at_ms",
)
_TERMINAL_RECEIPT_V3_FIELDS = (
    *_TERMINAL_RECEIPT_FIELDS[:-2],
    "projection_candidate_count",
    "projection_exclusion_count",
    "projection_exclusion_counts_json",
    "projection_exclusion_roots_json",
    "created_at_ms",
    "checkpoint_at_ms",
)
_V1_RECEIPT_COLUMNS = tuple(
    field
    for field in _DRIFT_RECEIPT_V2_DIGEST_FIELDS
    if field
    not in {
        "hash_algorithm",
        "generation_projection_member_comparison_count",
        "generation_projection_member_comparison_root",
        "generation_source_group_truth_comparison_count",
        "generation_source_group_truth_comparison_root",
        "classifier_contract_version",
        "diagnostic_counts_json",
        "diagnostic_root",
        "diagnostic_samples_json",
        "diagnostic_samples_digest",
    }
) + ("receipt_digest",)


def _downgrade_drift_tables_to_v1_shape(store: SQLiteStore) -> None:
    with sqlite3.connect(store.db_path) as con:
        con.execute("DROP INDEX IF EXISTS idx_structure_drift_progress_active")
        con.execute("DROP TRIGGER IF EXISTS trg_structure_drift_receipt_update")
        con.execute("DROP TRIGGER IF EXISTS trg_structure_drift_receipt_delete")
        progress_columns = ",".join(_V1_PROGRESS_COLUMNS)
        receipt_columns = ",".join(_V1_RECEIPT_COLUMNS)
        con.execute(
            "CREATE TABLE structure_generation_drift_progress_v1 AS SELECT "
            f"{progress_columns} FROM structure_generation_drift_progress"
        )
        con.execute("DROP TABLE structure_generation_drift_progress")
        con.execute(
            "ALTER TABLE structure_generation_drift_progress_v1 "
            "RENAME TO structure_generation_drift_progress"
        )
        con.execute(
            "CREATE TABLE structure_generation_drift_receipts_v1 AS SELECT "
            f"{receipt_columns} FROM structure_generation_drift_receipts"
        )
        con.execute("DROP TABLE structure_generation_drift_receipts")
        con.execute(
            "ALTER TABLE structure_generation_drift_receipts_v1 "
            "RENAME TO structure_generation_drift_receipts"
        )


def _downgrade_to_classifier_v1_shape(store: SQLiteStore) -> None:
    _downgrade_drift_tables_to_v1_shape(store)
    with sqlite3.connect(store.db_path) as con:
        sqlite_store_module._migrate_structure_drift_hash_v2(con)
        con.execute("DROP TRIGGER IF EXISTS trg_structure_drift_terminal_receipt_update")
        con.execute("DROP TRIGGER IF EXISTS trg_structure_drift_terminal_receipt_delete")
        con.execute("DROP TABLE IF EXISTS structure_generation_drift_terminal_receipts")


def _columns(con: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in con.execute(f"PRAGMA table_info({table})")}


def _downgrade_to_classifier_v2_shape(store: SQLiteStore) -> None:
    table_fields = {
        "structure_generation_drift_progress": _V3_PROGRESS_EXCLUSION_FIELDS,
        "structure_generation_drift_receipts": _V3_RECEIPT_EXCLUSION_FIELDS,
        "structure_generation_drift_terminal_receipts": (
            _V3_RECEIPT_EXCLUSION_FIELDS
        ),
    }
    with sqlite3.connect(store.db_path) as con:
        con.execute("DROP INDEX IF EXISTS idx_structure_drift_progress_active")
        con.execute("DROP TRIGGER IF EXISTS trg_structure_drift_receipt_update")
        con.execute("DROP TRIGGER IF EXISTS trg_structure_drift_receipt_delete")
        con.execute("DROP TRIGGER IF EXISTS trg_structure_drift_terminal_receipt_update")
        con.execute("DROP TRIGGER IF EXISTS trg_structure_drift_terminal_receipt_delete")
        con.execute("DROP TRIGGER IF EXISTS trg_structure_drift_terminal_receipt_insert")
        for table, omitted in table_fields.items():
            existing = _columns(con, table)
            for column in omitted:
                if column in existing:
                    con.execute(
                        f"ALTER TABLE {table} DROP COLUMN {column}"  # noqa: S608
                    )
        con.execute(
            "CREATE INDEX idx_structure_drift_progress_active ON "
            "structure_generation_drift_progress(checkpoint_at_ms DESC,comparison_id) "
            "WHERE phase NOT IN ('sealed','stale')"
        )
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


def _receipt_bytes(con: sqlite3.Connection, comparison_id: str) -> tuple[object, ...]:
    row = con.execute(
        "SELECT " + ",".join((*_DRIFT_RECEIPT_V2_DIGEST_FIELDS, "receipt_digest"))
        + " FROM structure_generation_drift_receipts WHERE comparison_id=?",
        (comparison_id,),
    ).fetchone()
    assert row is not None
    return tuple(row)


def _terminal_bytes(con: sqlite3.Connection, comparison_id: str) -> tuple[object, ...]:
    row = con.execute(
        "SELECT " + ",".join((*_TERMINAL_RECEIPT_FIELDS, "receipt_digest"))
        + " FROM structure_generation_drift_terminal_receipts WHERE comparison_id=?",
        (comparison_id,),
    ).fetchone()
    assert row is not None
    return tuple(row)


def _pre_v3_receipt_store(
    tmp_path: Path,
) -> tuple[SQLiteStore, tuple[object, ...], tuple[object, ...]]:
    store = _drift_store(tmp_path)
    comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)
    with sqlite3.connect(store.db_path) as con:
        progress = con.execute(
            "SELECT generation_snapshot_id,publication_id,window_id,"
            "normalization_contract_version,exact_receipt_digest,"
            "pointer_validation_hash,generation_certification_hash,"
            "source_event_count,source_market_count FROM "
            "structure_generation_drift_progress WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone()
        legacy_identity = store._comparison_legacy_identity(con)
        assert progress is not None and legacy_identity is not None
        v2_id = sqlite_store_module._structure_drift_comparison_id(
            (*legacy_identity, *progress, sqlite_store_module.ROW_CHAIN_SHA256_V2),
            classifier_contract_version=STRUCTURE_DRIFT_CLASSIFIER_V2,
        )
        con.execute(
            "UPDATE structure_generation_drift_progress SET comparison_id=?,"
            "classifier_contract_version=? WHERE comparison_id=?",
            (v2_id, STRUCTURE_DRIFT_CLASSIFIER_V2, comparison_id),
        )
    comparison_id = v2_id
    _install_sealed_drift_authority(store, comparison_id)
    with sqlite3.connect(store.db_path) as con:
        payload = _terminal_receipt_payload(con, comparison_id)
        _insert_terminal_receipt(con, payload)
        con.execute(
            "UPDATE structure_generation_drift_progress SET phase='stale',"
            "terminal_reason=?,checkpoint_at_ms=? WHERE comparison_id=?",
            (payload["terminal_reason"], payload["checkpoint_at_ms"], comparison_id),
        )
    _downgrade_to_classifier_v2_shape(store)
    with sqlite3.connect(store.db_path) as con:
        authorization = _receipt_bytes(con, comparison_id)
        terminal = _terminal_bytes(con, comparison_id)
    return store, authorization, terminal


def _independent_sha256(values: tuple[object, ...]) -> str:
    return hashlib.sha256(
        json.dumps(values, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()


def _changed(value: object) -> object:
    if type(value) is int:
        return value + 1
    return f"changed-{value}"


def _digest_payload(fields: tuple[str, ...], contract: str) -> dict[str, object]:
    payload = {
        field: index if field.endswith("_count") or field.endswith("_ms") else field
        for index, field in enumerate(fields)
    }
    payload["classifier_contract_version"] = contract
    return payload


def _valid_v2_authorization_payload() -> dict[str, object]:
    return _digest_payload(
        _DRIFT_RECEIPT_V2_DIGEST_FIELDS, STRUCTURE_DRIFT_CLASSIFIER_V2
    )


def _valid_v3_authorization_payload() -> dict[str, object]:
    return _digest_payload(
        _DRIFT_RECEIPT_V3_DIGEST_FIELDS, STRUCTURE_DRIFT_CLASSIFIER_V3
    )


def _valid_v2_terminal_payload() -> dict[str, object]:
    return _digest_payload(_TERMINAL_RECEIPT_FIELDS, STRUCTURE_DRIFT_CLASSIFIER_V2)


def _valid_v3_terminal_payload() -> dict[str, object]:
    return _digest_payload(_TERMINAL_RECEIPT_V3_FIELDS, STRUCTURE_DRIFT_CLASSIFIER_V3)


def _existing_v2_digest(payload: dict[str, object], fields: tuple[str, ...]) -> str:
    return _independent_sha256(tuple(payload[field] for field in fields))


def test_v3_migration_preserves_v2_receipt_bytes_and_adds_nullable_fields(
    tmp_path: Path,
) -> None:
    store, v2_authorization, v2_terminal = _pre_v3_receipt_store(tmp_path)
    comparison_id = str(v2_authorization[0])
    store.init_schema()
    with sqlite3.connect(store.db_path) as con:
        assert _V3_RECEIPT_EXCLUSION_FIELDS <= _columns(
            con, "structure_generation_drift_receipts"
        )
        assert _V3_RECEIPT_EXCLUSION_FIELDS <= _columns(
            con, "structure_generation_drift_terminal_receipts"
        )
        assert _V3_PROGRESS_EXCLUSION_FIELDS <= _columns(
            con, "structure_generation_drift_progress"
        )
        assert _receipt_bytes(con, comparison_id) == v2_authorization
        assert _terminal_bytes(con, comparison_id) == v2_terminal
        assert con.execute(
            "SELECT projection_candidate_count,projection_exclusion_count,"
            "projection_exclusion_counts_json,projection_exclusion_roots_json FROM "
            "structure_generation_drift_receipts WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone() == (None, None, None, None)
        assert con.execute(
            "SELECT projection_candidate_count,projection_exclusion_count,"
            "projection_exclusion_counts_json,projection_exclusion_roots_json FROM "
            "structure_generation_drift_terminal_receipts WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone() == (None, None, None, None)
        assert con.execute(
            "SELECT projection_candidate_count,projection_exclusion_count,"
            "projection_exclusion_counts_json,projection_exclusion_roots_json,"
            "projection_exclusion_digest_states_json FROM "
            "structure_generation_drift_progress WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone() == (0, 0, "{}", "{}", "{}")
        for statement in (
            "UPDATE structure_generation_drift_receipts SET created_at_ms=1 "
            "WHERE comparison_id=?",
            "DELETE FROM structure_generation_drift_receipts WHERE comparison_id=?",
            "UPDATE structure_generation_drift_terminal_receipts SET created_at_ms=1 "
            "WHERE comparison_id=?",
            "DELETE FROM structure_generation_drift_terminal_receipts "
            "WHERE comparison_id=?",
        ):
            with pytest.raises(sqlite3.IntegrityError, match="receipt-sealed"):
                con.execute(statement, (comparison_id,))


def test_terminal_v2_identity_starts_v4_without_mutating_v2(
    tmp_path: Path,
) -> None:
    store, _, terminal_before = _pre_v3_receipt_store(tmp_path)
    v2_id = str(terminal_before[0])
    store.init_schema()

    v4_id = store.initialize_structure_drift_comparison(now_ms=4_000)

    assert v4_id != v2_id
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT classifier_contract_version FROM "
            "structure_generation_drift_progress WHERE comparison_id=?",
            (v4_id,),
        ).fetchone() == (STRUCTURE_DRIFT_CLASSIFIER_V4,)
        assert _terminal_bytes(con, v2_id) == terminal_before
    assert store.initialize_structure_drift_comparison(now_ms=4_001) == v4_id


def test_v3_migration_lightweight_structure_sync_schema_converges(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "minimal-existing-snapshots.db"
    with sqlite3.connect(db_path) as con:
        con.execute("CREATE TABLE snapshots(id INTEGER PRIMARY KEY)")

    SQLiteStore(db_path).init_structure_sync_schema()

    with sqlite3.connect(db_path) as con:
        assert _V3_RECEIPT_EXCLUSION_FIELDS <= _columns(
            con, "structure_generation_drift_receipts"
        )
        assert _V3_RECEIPT_EXCLUSION_FIELDS <= _columns(
            con, "structure_generation_drift_terminal_receipts"
        )
        assert _V3_PROGRESS_EXCLUSION_FIELDS <= _columns(
            con, "structure_generation_drift_progress"
        )
        triggers = {
            str(name): str(sql)
            for name, sql in con.execute(
                "SELECT name,sql FROM sqlite_master WHERE type='trigger' AND "
                "tbl_name IN ('structure_generation_drift_receipts',"
                "'structure_generation_drift_terminal_receipts')"
            )
        }

    assert set(triggers) == {
        "trg_structure_drift_receipt_update",
        "trg_structure_drift_receipt_delete",
        "trg_structure_drift_terminal_receipt_update",
        "trg_structure_drift_terminal_receipt_delete",
        "trg_structure_drift_terminal_receipt_insert",
    }
    assert all("receipt-sealed" in sql for sql in triggers.values())
    full_db_path = tmp_path / "fresh-full-schema.db"
    SQLiteStore(full_db_path).init_schema()
    assert _authority_signature(db_path) == _authority_signature(full_db_path)


@pytest.mark.parametrize(
    "fault_point",
    (
        "after-authorization-columns",
        "after-terminal-columns",
        "after-progress-columns",
    ),
)
def test_v3_migration_fault_rolls_back_to_exact_v2_authority_shape(
    tmp_path: Path,
    fault_point: str,
) -> None:
    store, _, _ = _pre_v3_receipt_store(tmp_path)
    before = _authority_signature(store.db_path)
    with sqlite3.connect(store.db_path) as con:
        def inject(step: str) -> None:
            if step == fault_point:
                raise RuntimeError(f"injected-{fault_point}")

        with pytest.raises(RuntimeError, match=f"injected-{fault_point}"):
            sqlite_store_module._migrate_structure_drift_classifier_v3_exclusions(
                con, fault_hook=inject
            )
    assert _authority_signature(store.db_path) == before


def test_v3_receipt_digest_binds_every_exclusion_field() -> None:
    payload = _valid_v3_authorization_payload()
    expected = _independent_sha256(
        tuple(payload[field] for field in _DRIFT_RECEIPT_V3_DIGEST_FIELDS)
    )
    assert sqlite_store_module._structure_drift_receipt_digest(payload) == expected
    for field in _V3_RECEIPT_EXCLUSION_FIELDS:
        assert sqlite_store_module._structure_drift_receipt_digest(
            {**payload, field: _changed(payload[field])}
        ) != expected


def test_v2_receipt_digest_field_oracle_is_unchanged() -> None:
    payload = _valid_v2_authorization_payload()
    assert sqlite_store_module._structure_drift_receipt_fields(
        STRUCTURE_DRIFT_CLASSIFIER_V2
    ) == _DRIFT_RECEIPT_V2_DIGEST_FIELDS
    assert sqlite_store_module._structure_drift_receipt_digest(
        payload
    ) == _existing_v2_digest(payload, _DRIFT_RECEIPT_V2_DIGEST_FIELDS)


def test_v1_receipt_digest_field_oracle_is_unchanged() -> None:
    payload = _digest_payload(
        _DRIFT_RECEIPT_V2_DIGEST_FIELDS, STRUCTURE_DRIFT_CLASSIFIER_V1
    )
    assert sqlite_store_module._structure_drift_receipt_fields(
        STRUCTURE_DRIFT_CLASSIFIER_V1
    ) == _DRIFT_RECEIPT_V2_DIGEST_FIELDS
    expected = _independent_sha256(
        tuple(payload[field] for field in _DRIFT_RECEIPT_V2_DIGEST_FIELDS)
    )
    assert sqlite_store_module._structure_drift_receipt_digest(payload) == expected


def test_v3_receipt_digest_terminal_binds_every_exclusion_field() -> None:
    payload = _valid_v3_terminal_payload()
    expected = _independent_sha256(
        tuple(payload[field] for field in _TERMINAL_RECEIPT_V3_FIELDS)
    )
    assert sqlite_store_module._structure_drift_terminal_receipt_digest(
        payload
    ) == expected
    for field in _V3_RECEIPT_EXCLUSION_FIELDS:
        assert sqlite_store_module._structure_drift_terminal_receipt_digest(
            {**payload, field: _changed(payload[field])}
        ) != expected


def test_v2_receipt_digest_field_oracle_terminal_is_unchanged() -> None:
    payload = _valid_v2_terminal_payload()
    assert sqlite_store_module._structure_drift_terminal_receipt_fields(
        STRUCTURE_DRIFT_CLASSIFIER_V2
    ) == _TERMINAL_RECEIPT_FIELDS
    assert sqlite_store_module._structure_drift_terminal_receipt_digest(
        payload
    ) == _existing_v2_digest(payload, _TERMINAL_RECEIPT_FIELDS)


def test_v1_receipt_digest_field_oracle_terminal_is_unchanged() -> None:
    payload = _digest_payload(_TERMINAL_RECEIPT_FIELDS, STRUCTURE_DRIFT_CLASSIFIER_V1)
    assert sqlite_store_module._structure_drift_terminal_receipt_fields(
        STRUCTURE_DRIFT_CLASSIFIER_V1
    ) == _TERMINAL_RECEIPT_FIELDS
    expected = _independent_sha256(
        tuple(payload[field] for field in _TERMINAL_RECEIPT_FIELDS)
    )
    assert (
        sqlite_store_module._structure_drift_terminal_receipt_digest(payload)
        == expected
    )


def _authority_signature(path: Path) -> dict[str, tuple[tuple[object, ...], ...]]:
    names = (
        "structure_generation_drift_progress",
        "structure_generation_drift_receipts",
        "structure_generation_drift_terminal_receipts",
    )
    with sqlite3.connect(path) as con:
        signature = {
            f"columns:{name}": tuple(con.execute(f"PRAGMA table_info({name})"))
            for name in names
        }
        for kind in ("index", "trigger"):
            signature[kind] = tuple(
                (str(row[0]), str(row[1]), "".join(str(row[2]).split()))
                for row in con.execute(
                    "SELECT name,tbl_name,sql FROM sqlite_master WHERE type=? AND "
                    "tbl_name IN (?,?,?) ORDER BY name",
                    (kind, *names),
                )
            )
    return signature


def _classifier_migration_business_rows(
    path: Path,
) -> dict[str, tuple[tuple[object, ...], ...]]:
    tables = (
        "snapshots",
        "structure_publications",
        "current_structure_generation",
        "structure_sync_event_staging",
        "structure_sync_market_staging",
        "markets",
        "events",
        "structure_generation_events",
        "structure_generation_markets",
        "structure_generation_memberships",
        "structure_generation_group_truth",
    )
    with sqlite3.connect(path) as con:
        return {table: tuple(con.execute(f"SELECT * FROM {table}")) for table in tables}


def test_classifier_migration_historical_classifier_label_matches_fresh_schema(
    tmp_path: Path,
) -> None:
    migrated = _drift_store(tmp_path)
    comparison_id = migrated.initialize_structure_drift_comparison(now_ms=3_000)
    _install_sealed_drift_authority(migrated, comparison_id)
    _downgrade_to_classifier_v1_shape(migrated)
    business_before = _classifier_migration_business_rows(migrated.db_path)
    migrated.init_schema()
    migrated.init_schema()
    fresh = SQLiteStore(tmp_path / "fresh-classifier.db")
    fresh.init_schema()
    assert _authority_signature(migrated.db_path) == _authority_signature(fresh.db_path)
    assert _classifier_migration_business_rows(migrated.db_path) == business_before
    with sqlite3.connect(migrated.db_path) as con:
        assert con.execute(
            "SELECT classifier_contract_version FROM "
            "structure_generation_drift_progress WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone() == (STRUCTURE_DRIFT_CLASSIFIER_V1,)
        assert con.execute(
            "SELECT classifier_contract_version FROM "
            "structure_generation_drift_receipts WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone() == (STRUCTURE_DRIFT_CLASSIFIER_V1,)


@pytest.mark.parametrize(
    "fault_point",
    (
        "after-progress-rename",
        "after-authorization-receipt-rename",
        "after-terminal-table-create",
    ),
)
def test_classifier_migration_rollback_restores_authority_and_business_rows(
    tmp_path: Path,
    fault_point: str,
) -> None:
    store = _drift_store(tmp_path)
    store.initialize_structure_drift_comparison(now_ms=3_000)
    _downgrade_to_classifier_v1_shape(store)
    with sqlite3.connect(store.db_path) as con:
        before_signature = _authority_signature(store.db_path)
        business_tables = (
            "snapshots",
            "structure_publications",
            "current_structure_generation",
            "structure_sync_event_staging",
            "structure_sync_market_staging",
            "markets",
        )
        before_counts = {
            table: con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in business_tables
        }

        def inject(step: str) -> None:
            if step == fault_point:
                raise RuntimeError(f"injected-{fault_point}")

        with pytest.raises(RuntimeError, match=f"injected-{fault_point}"):
            sqlite_store_module._migrate_structure_drift_classifier_v2(
                con, fault_hook=inject
            )
        assert _authority_signature(store.db_path) == before_signature
        assert {
            table: con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in business_tables
        } == before_counts
    store.init_schema()
    store.init_schema()


def test_migrated_active_classifier_v1_is_superseded_before_v4_initialization(
    tmp_path: Path,
) -> None:
    store = _drift_store(tmp_path)
    store.initialize_structure_drift_comparison(now_ms=3_000)
    _downgrade_to_classifier_v1_shape(store)
    store.init_schema()
    legacy_comparison_id = "e" * 64
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "UPDATE structure_generation_drift_progress SET comparison_id=?,"
            "hash_algorithm='row-chain-sha256-v2' "
            "WHERE classifier_contract_version=?",
            (legacy_comparison_id, STRUCTURE_DRIFT_CLASSIFIER_V1),
        )

    v4_comparison_id = store.initialize_structure_drift_comparison(now_ms=3_001)

    assert v4_comparison_id != legacy_comparison_id
    with sqlite3.connect(store.db_path) as con:
        rows = con.execute(
            "SELECT comparison_id,classifier_contract_version,phase,terminal_reason "
            "FROM structure_generation_drift_progress ORDER BY "
            "classifier_contract_version"
        ).fetchall()
    assert rows == [
        (
            legacy_comparison_id,
            STRUCTURE_DRIFT_CLASSIFIER_V1,
            "stale",
            "drift-classifier-contract-superseded",
        ),
        (
                v4_comparison_id,
                STRUCTURE_DRIFT_CLASSIFIER_V4,
            "source-events",
            None,
        ),
    ]


def test_drift_v2_migration_rolls_back_injected_crash_and_reinitializes(
    tmp_path: Path,
) -> None:
    store = _drift_store(tmp_path)
    comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)
    _downgrade_drift_tables_to_v1_shape(store)
    with sqlite3.connect(store.db_path) as con:
        immutable_before = {
            table: con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in (
                "snapshots",
                "structure_publications",
                "current_structure_generation",
                "structure_sync_event_staging",
                "structure_sync_market_staging",
                "markets",
            )
        }
        migrate = getattr(
            sqlite_store_module, "_migrate_structure_drift_hash_v2"
        )

        def fail_after_progress_rename(step: str) -> None:
            if step == "after-progress-rename":
                raise RuntimeError("injected-after-progress-rename")

        with pytest.raises(RuntimeError, match="injected-after-progress-rename"):
            migrate(con, fault_hook=fail_after_progress_rename)
        assert "hash_algorithm" not in {
            str(row[1])
            for row in con.execute(
                "PRAGMA table_info(structure_generation_drift_progress)"
            )
        }

    store.init_schema()
    store.init_schema()
    with sqlite3.connect(store.db_path) as con:
        defaults = {
            table: {
                str(row[1]): (int(row[3]), row[4])
                for row in con.execute(f"PRAGMA table_info({table})")
            }["hash_algorithm"]
            for table in (
                "structure_generation_drift_progress",
                "structure_generation_drift_receipts",
            )
        }
        migrated = con.execute(
            "SELECT comparison_id,hash_algorithm,phase,terminal_reason "
            "FROM structure_generation_drift_progress"
        ).fetchall()
        immutable_after = {
            table: con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in immutable_before
        }
        old_shape_row = con.execute(
            "SELECT " + ",".join(_V1_PROGRESS_COLUMNS) + " FROM "
            "structure_generation_drift_progress"
        ).fetchone()
        con.execute("DELETE FROM structure_generation_drift_progress")
        con.execute(
            "INSERT INTO structure_generation_drift_progress("
            + ",".join(_V1_PROGRESS_COLUMNS)
            + ") VALUES ("
            + ",".join("?" for _ in _V1_PROGRESS_COLUMNS)
            + ")",
            old_shape_row,
        )
        omitted_column_algorithm = con.execute(
            "SELECT hash_algorithm FROM structure_generation_drift_progress"
        ).fetchone()
    assert migrated == [
        (comparison_id, "serializable-sha256-v1", "source-events", None)
    ]
    assert immutable_after == immutable_before
    assert set(defaults.values()) == {(1, "'serializable-sha256-v1'")}
    assert omitted_column_algorithm == ("serializable-sha256-v1",)


def test_drift_v2_migration_writer_lock_leaves_v1_schema_reinitializable(
    tmp_path: Path,
) -> None:
    store = _drift_store(tmp_path)
    store.initialize_structure_drift_comparison(now_ms=3_000)
    _downgrade_drift_tables_to_v1_shape(store)
    blocker = sqlite3.connect(store.db_path, isolation_level=None)
    blocker.execute("BEGIN IMMEDIATE")
    try:
        with pytest.raises(sqlite3.OperationalError, match="database is locked"):
            SQLiteStore(store.db_path, writer_timeout_s=0.01).init_schema()
    finally:
        blocker.execute("ROLLBACK")
        blocker.close()

    with sqlite3.connect(store.db_path) as con:
        assert "hash_algorithm" not in {
            str(row[1])
            for row in con.execute(
                "PRAGMA table_info(structure_generation_drift_progress)"
            )
        }
        assert con.execute(
            "SELECT COUNT(*) FROM structure_generation_drift_progress"
        ).fetchone() == (1,)
    store.init_schema()
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT hash_algorithm FROM structure_generation_drift_progress"
        ).fetchone() == ("serializable-sha256-v1",)


def test_active_v1_progress_is_atomically_superseded_by_cursor_zero_v2(
    tmp_path: Path,
) -> None:
    store = _drift_store(tmp_path)
    store.initialize_structure_drift_comparison(now_ms=3_000)
    v1_comparison_id = "b" * 64
    with sqlite3.connect(store.db_path) as con:
        v1_state = sqlite_store_module.SerializableSHA256.new()
        v1_state.update(b"[")
        con.execute(
            "UPDATE structure_generation_drift_progress SET comparison_id=?,"
            "hash_algorithm='serializable-sha256-v1',digest_state_json=?",
            (v1_comparison_id, v1_state.to_json()),
        )
        data_plane_before = {
            table: con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in (
                "current_structure_generation",
                "structure_publications",
                "structure_sync_event_staging",
                "structure_sync_market_staging",
                "events",
                "event_market_memberships",
                "neg_risk_group_truth",
                "markets",
            )
        }

    v2_comparison_id = store.initialize_structure_drift_comparison(now_ms=3_001)

    with sqlite3.connect(store.db_path) as con:
        progress = {
            str(row[1]): row
            for row in con.execute(
            "SELECT comparison_id,hash_algorithm,phase,terminal_reason,"
            "row_cursor_json,digest_state_json FROM "
            "structure_generation_drift_progress"
            )
        }
        data_plane_after = {
            table: con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in data_plane_before
        }
    assert v2_comparison_id != v1_comparison_id
    assert progress["serializable-sha256-v1"][:5] == (
        v1_comparison_id,
        "serializable-sha256-v1",
        "stale",
        "drift-hash-algorithm-superseded",
        None,
    )
    assert progress["row-chain-sha256-v2"][:5] == (
        v2_comparison_id,
        "row-chain-sha256-v2",
        "source-events",
        None,
        None,
    )
    v2_state = json.loads(str(progress["row-chain-sha256-v2"][5]))
    assert v2_state["algorithm"] == "row-chain-sha256-v2"
    assert v2_state["domain"] == "source-event"
    assert v2_state["count"] == 0
    assert data_plane_after == data_plane_before
    status = store.structure_generation_drift_status()
    assert status["progress_id"] == v2_comparison_id
    assert status["phase"] == "source-events"
    assert status["hash_algorithm"] == "row-chain-sha256-v2"


def test_v2_insert_failure_rolls_back_v1_supersession(tmp_path: Path) -> None:
    store = _drift_store(tmp_path)
    store.initialize_structure_drift_comparison(now_ms=3_000)
    v1_comparison_id = "c" * 64
    with sqlite3.connect(store.db_path) as con:
        v1_state = sqlite_store_module.SerializableSHA256.new()
        v1_state.update(b"[")
        con.execute(
            "UPDATE structure_generation_drift_progress SET comparison_id=?,"
            "hash_algorithm='serializable-sha256-v1',digest_state_json=?",
            (v1_comparison_id, v1_state.to_json()),
        )
        con.execute(
            "CREATE TRIGGER reject_v2_progress BEFORE INSERT ON "
            "structure_generation_drift_progress WHEN "
            "NEW.hash_algorithm='row-chain-sha256-v2' BEGIN SELECT "
            "RAISE(ABORT,'injected-v2-insert-failure'); END"
        )

    with pytest.raises(sqlite3.IntegrityError, match="injected-v2-insert-failure"):
        store.initialize_structure_drift_comparison(now_ms=3_001)

    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT comparison_id,hash_algorithm,phase,terminal_reason FROM "
            "structure_generation_drift_progress"
        ).fetchall() == [
            (
                v1_comparison_id,
                "serializable-sha256-v1",
                "source-events",
                None,
            )
        ]


def test_current_stale_v1_initializes_and_advances_v4_without_mutating_v1(
    tmp_path: Path,
) -> None:
    store = _drift_store(tmp_path)
    original_id = store.initialize_structure_drift_comparison(now_ms=3_000)
    v1_comparison_id = "d" * 64
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "UPDATE structure_generation_drift_progress SET comparison_id=?,"
            "classifier_contract_version=?,phase='stale',terminal_reason="
            "'drift-unclassified' WHERE comparison_id=?",
            (v1_comparison_id, STRUCTURE_DRIFT_CLASSIFIER_V1, original_id),
        )
        v1_before = con.execute(
            "SELECT * FROM structure_generation_drift_progress WHERE comparison_id=?",
            (v1_comparison_id,),
        ).fetchone()

    chunk = store.advance_current_structure_drift_chunk(max_rows=1, now_ms=3_001)

    assert chunk is not None
    assert chunk.rows_processed == 1
    with sqlite3.connect(store.db_path) as con:
        v1_after = con.execute(
            "SELECT * FROM structure_generation_drift_progress WHERE comparison_id=?",
            (v1_comparison_id,),
        ).fetchone()
        v4_rows = con.execute(
            "SELECT comparison_id,phase FROM structure_generation_drift_progress "
            "WHERE classifier_contract_version=?",
            (STRUCTURE_DRIFT_CLASSIFIER_V4,),
        ).fetchall()
    assert v1_after == v1_before
    assert len(v4_rows) == 1
    assert v4_rows[0][1] in {"source-events", "source-markets"}


@pytest.mark.asyncio
async def test_concurrent_scheduler_ticks_start_one_real_v4_child(
    tmp_path: Path,
) -> None:
    from polyarb.daemon.scheduler import SnapshotScheduler

    store = _drift_store(tmp_path)
    settings = SimpleNamespace(
        db_path=store.db_path,
        scheduler_interval_s=3600,
        structure_generation_drift_compare_enabled=True,
        structure_generation_drift_max_rows=500,
        structure_generation_drift_max_chunks_per_tick=100,
        structure_generation_drift_slice_s=45.0,
    )
    producer_lock = asyncio.Lock()
    scheduler = SnapshotScheduler(
        settings=settings,
        sqlite_store=store,
        producer_lock=producer_lock,
    )

    results = await asyncio.gather(
        scheduler._maybe_advance_structure_drift(queued_at_ms=1_000),
        scheduler._maybe_advance_structure_drift(queued_at_ms=1_000),
    )

    assert results.count(True) == 1
    assert results.count(None) == 1
    with sqlite3.connect(store.db_path) as con:
        progress = con.execute(
            "SELECT comparison_id FROM structure_generation_drift_progress WHERE "
            "classifier_contract_version=?",
            (STRUCTURE_DRIFT_CLASSIFIER_V4,),
        ).fetchall()
        assert len(progress) == 1
        assert con.execute(
            "SELECT COUNT(*) FROM structure_drift_attempts"
        ).fetchone() == (1,)
        assert con.execute(
            "SELECT COUNT(*) FROM structure_drift_attempts WHERE outcome='running'"
        ).fetchone() == (0,)
        assert con.execute(
            "SELECT progress_id FROM structure_drift_attempts"
        ).fetchone() == (progress[0][0],)


@pytest.mark.asyncio
async def test_same_contract_stale_does_not_spawn_real_scheduler_retry(
    tmp_path: Path,
) -> None:
    from polyarb.daemon.scheduler import SnapshotScheduler

    store = _drift_store(tmp_path, omit_generation_market_id="addition")
    comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)
    assert _run_drift_to_terminal(store, comparison_id) == "stale"
    settings = SimpleNamespace(
        db_path=store.db_path,
        scheduler_interval_s=3600,
        structure_generation_drift_compare_enabled=True,
        structure_generation_drift_max_rows=500,
        structure_generation_drift_max_chunks_per_tick=100,
        structure_generation_drift_slice_s=45.0,
    )
    scheduler = SnapshotScheduler(
        settings=settings,
        sqlite_store=store,
        producer_lock=asyncio.Lock(),
    )

    assert await scheduler._maybe_advance_structure_drift(queued_at_ms=1_000) is None
    assert await scheduler._maybe_advance_structure_drift(queued_at_ms=2_000) is None

    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT COUNT(*) FROM structure_generation_drift_progress WHERE "
            "classifier_contract_version=?",
            (STRUCTURE_DRIFT_CLASSIFIER_V4,),
        ).fetchone() == (1,)
        assert con.execute(
            "SELECT COUNT(*) FROM structure_drift_attempts"
        ).fetchone() == (0,)


@pytest.mark.asyncio
async def test_scheduler_v4_initialization_fault_rolls_back_before_child_attempt(
    tmp_path: Path,
) -> None:
    from polyarb.daemon.scheduler import SnapshotScheduler

    store = _drift_store(tmp_path)
    original_id = store.initialize_structure_drift_comparison(now_ms=3_000)
    v1_comparison_id = "e" * 64
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "UPDATE structure_generation_drift_progress SET comparison_id=?,"
            "classifier_contract_version=? WHERE comparison_id=?",
            (v1_comparison_id, STRUCTURE_DRIFT_CLASSIFIER_V1, original_id),
        )
        con.execute(
            "CREATE TRIGGER reject_scheduler_v4_progress BEFORE INSERT ON "
            "structure_generation_drift_progress WHEN "
            "NEW.classifier_contract_version='structure-drift-classifier-v4' "
            "BEGIN SELECT RAISE(ABORT,'injected-scheduler-v4-failure'); END"
        )
        v1_before = con.execute(
            "SELECT * FROM structure_generation_drift_progress WHERE comparison_id=?",
            (v1_comparison_id,),
        ).fetchone()
    scheduler = SnapshotScheduler(
        settings=SimpleNamespace(
            db_path=store.db_path,
            scheduler_interval_s=3600,
            structure_generation_drift_compare_enabled=True,
            structure_generation_drift_max_rows=500,
            structure_generation_drift_max_chunks_per_tick=100,
            structure_generation_drift_slice_s=45.0,
        ),
        sqlite_store=store,
        producer_lock=asyncio.Lock(),
    )

    assert await scheduler._maybe_advance_structure_drift(queued_at_ms=1_000) is True

    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT * FROM structure_generation_drift_progress WHERE comparison_id=?",
            (v1_comparison_id,),
        ).fetchone() == v1_before
        assert con.execute(
            "SELECT COUNT(*) FROM structure_generation_drift_progress WHERE "
            "classifier_contract_version=?",
            (STRUCTURE_DRIFT_CLASSIFIER_V4,),
        ).fetchone() == (0,)
        assert con.execute(
            "SELECT COUNT(*) FROM structure_drift_attempts"
        ).fetchone() == (0,)
    assert store.get_latest_structure_defer()["reason"] == (
        "structure-drift-status-unavailable"
    )


@pytest.mark.parametrize(
    "failure_point",
    (
        "after-fresh-projection-progress-rename",
        "after-fresh-projection-progress-copy",
        "after-fresh-projection-progress-index-create",
    ),
)
def test_fresh_projection_phase_migration_rolls_back_and_preserves_audit_rows(
    tmp_path: Path,
    failure_point: str,
) -> None:
    store = _drift_store(tmp_path)
    comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)
    with sqlite3.connect(store.db_path) as con:
        columns = [
            str(row[1])
            for row in con.execute(
                "PRAGMA table_info(structure_generation_drift_progress)"
            )
        ]
        source_row = dict(zip(
            columns,
            con.execute("SELECT * FROM structure_generation_drift_progress").fetchone(),
            strict=True,
        ))
        historical_ids = (comparison_id, "b" * 64, "c" * 64)
        for historical_id, algorithm, classifier, phase in (
            (historical_ids[1], "row-chain-sha256-v2", STRUCTURE_DRIFT_CLASSIFIER_V1, "sealed"),
            (historical_ids[2], "serializable-sha256-v1", STRUCTURE_DRIFT_CLASSIFIER_V1, "stale"),
        ):
            row = {
                **source_row,
                "comparison_id": historical_id,
                "hash_algorithm": algorithm,
                "classifier_contract_version": classifier,
                "phase": phase,
                "terminal_reason": "legacy-terminal-reason-unspecified",
            }
            con.execute(
                "INSERT INTO structure_generation_drift_progress("
                + ",".join(columns) + ") VALUES ("
                + ",".join("?" for _ in columns) + ")",
                tuple(row[column] for column in columns),
            )
        sql = con.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND "
            "name='structure_generation_drift_progress'"
        ).fetchone()[0]
        old_sql = str(sql).replace("'fresh-projection-members',", "", 1)
        column_sql = ",".join(columns)
        con.execute("DROP INDEX idx_structure_drift_progress_active")
        con.execute(
            "ALTER TABLE structure_generation_drift_progress RENAME TO "
            "structure_generation_drift_progress_new"
        )
        con.execute(old_sql)
        con.execute(
            "INSERT INTO structure_generation_drift_progress("
            + column_sql
            + ") SELECT "
            + column_sql
            + " FROM structure_generation_drift_progress_new"
        )
        con.execute("DROP TABLE structure_generation_drift_progress_new")
        con.execute(
            "CREATE INDEX idx_structure_drift_progress_active ON "
            "structure_generation_drift_progress(checkpoint_at_ms DESC,comparison_id) "
            "WHERE phase NOT IN ('sealed','stale')"
        )

        def fail(point: str) -> None:
            if point == failure_point:
                raise RuntimeError("injected-fresh-phase-migration-failure")

        with pytest.raises(
            RuntimeError, match="injected-fresh-phase-migration-failure"
        ):
            sqlite_store_module._migrate_structure_drift_fresh_projection_phase(
                con, fault_hook=fail
            )
        rolled_back_sql = con.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND "
            "name='structure_generation_drift_progress'"
        ).fetchone()[0]
        assert "fresh-projection-members" not in rolled_back_sql
        assert con.execute(
            "SELECT comparison_id FROM structure_generation_drift_progress"
            " ORDER BY comparison_id"
        ).fetchall() == [(value,) for value in sorted(historical_ids)]

        sqlite_store_module._migrate_structure_drift_fresh_projection_phase(con)
        upgraded_sql = con.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND "
            "name='structure_generation_drift_progress'"
        ).fetchone()[0]
        assert "fresh-projection-members" in upgraded_sql
        assert upgraded_sql == sql
        assert con.execute(
            "SELECT comparison_id FROM structure_generation_drift_progress"
            " ORDER BY comparison_id"
        ).fetchall() == [(value,) for value in sorted(historical_ids)]
        con.execute("PRAGMA foreign_keys=ON")
        assert con.execute("PRAGMA foreign_key_check").fetchall() == []
        signature_before = con.execute(
            "SELECT sql FROM sqlite_master WHERE name IN "
            "('structure_generation_drift_progress','idx_structure_drift_progress_active') "
            "ORDER BY type,name"
        ).fetchall()
        sqlite_store_module._migrate_structure_drift_fresh_projection_phase(con)
        assert con.execute(
            "SELECT sql FROM sqlite_master WHERE name IN "
            "('structure_generation_drift_progress','idx_structure_drift_progress_active') "
            "ORDER BY type,name"
        ).fetchall() == signature_before


def _install_sealed_drift_authority(store: SQLiteStore, comparison_id: str) -> None:
    with sqlite3.connect(store.db_path) as con:
        window_id = str(
            con.execute(
                "SELECT window_id FROM structure_generation_drift_progress "
                "WHERE comparison_id=?",
                (comparison_id,),
            ).fetchone()[0]
        )
        candidate_count = (
            sqlite_store_module._fresh_projection_expected_candidate_count(
                con, window_id=window_id
            )
        )
        exclusion_count = candidate_count - 1
        exclusion_counts = {
            reason: 0 for reason in STRUCTURE_PROJECTION_EXCLUSION_REASONS
        }
        exclusion_states = {
            reason: RowChainSHA256.new(f"projection-exclusion/{reason}").to_json()
            for reason in STRUCTURE_PROJECTION_EXCLUSION_REASONS
        }
        exclusion_chain = RowChainSHA256.from_json(
            exclusion_states["non-neg-risk-market"],
            expected_domain="projection-exclusion/non-neg-risk-market",
        )
        for index in range(exclusion_count):
            exclusion_chain.update(("fixture-exclusion", index))
        exclusion_counts["non-neg-risk-market"] = exclusion_count
        exclusion_states["non-neg-risk-market"] = exclusion_chain.to_json()
        exclusion_roots = {
            reason: RowChainSHA256.from_json(
                exclusion_states[reason],
                expected_domain=f"projection-exclusion/{reason}",
            ).hexdigest()
            for reason in STRUCTURE_PROJECTION_EXCLUSION_REASONS
        }
        con.execute(
            "UPDATE structure_generation_drift_progress SET "
            "projection_candidate_count=?,projection_exclusion_count=?,"
            "projection_exclusion_counts_json=?,"
            "projection_exclusion_roots_json=?,"
            "projection_exclusion_digest_states_json=? WHERE comparison_id=?",
            (
                candidate_count,
                exclusion_count,
                json.dumps(exclusion_counts, sort_keys=True, separators=(",", ":")),
                json.dumps(exclusion_roots, sort_keys=True, separators=(",", ":")),
                json.dumps(exclusion_states, sort_keys=True, separators=(",", ":")),
                comparison_id,
            ),
        )
        progress = con.execute(
            "SELECT legacy_snapshot_id,generation_snapshot_id,publication_id,"
            "window_id,normalization_contract_version,exact_receipt_digest,"
            "pointer_validation_hash,generation_certification_hash,"
            "classifier_contract_version,projection_candidate_count,"
            "projection_exclusion_count,projection_exclusion_counts_json,"
            "projection_exclusion_roots_json FROM "
            "structure_generation_drift_progress WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone()
        digest_fields = sqlite_store_module._structure_drift_receipt_fields(
            str(progress[8])
        )
        exact = con.execute(
            "SELECT snapshot.taken_at_ms,snapshot.finished_at_ms,"
            "receipt.legacy_market_count,receipt.legacy_universe_hash,"
            "receipt.legacy_source_truth_hash FROM "
            "structure_generation_comparison_receipts receipt JOIN snapshots snapshot "
            "ON snapshot.id=receipt.legacy_snapshot_id WHERE "
            "receipt.generation_snapshot_id=? AND receipt.publication_id=?",
            (progress[1], progress[2]),
        ).fetchone()
        source_hashes = ("1" * 64, "2" * 64, "3" * 64)
        member_receipt_digest = str(con.execute(
            "SELECT receipt_digest FROM structure_sync_event_member_receipts "
            "WHERE window_id=?", (str(progress[3]),),
        ).fetchone()[0])
        sealed_class_counts = {
            "shared": 1,
            "fresh-addition": 0,
            "current-nontradable": 0,
            "event-only-quarantine": 0,
            "market-side-quarantine": 0,
            "fresh-source-absent": 0,
            "fresh-group-ineligible": 0,
            "overlap-conflict": 0,
            "unclassified": 0,
        }
        sealed_class_digests = {"shared": "a" * 64}
        class_counts_json = json.dumps(
            sealed_class_counts,
            sort_keys=True,
            separators=(",", ":"),
        )
        payload: dict[str, object] = {
            "comparison_id": comparison_id,
            "hash_algorithm": "row-chain-sha256-v2",
            "classifier_contract_version": str(progress[8]),
            "legacy_snapshot_id": int(progress[0]),
            "legacy_taken_at_ms": int(exact[0]),
            "legacy_finished_at_ms": int(exact[1]),
            "legacy_market_count": int(exact[2]),
            "legacy_universe_hash": str(exact[3]),
            "legacy_source_truth_hash": str(exact[4]),
            "generation_snapshot_id": int(progress[1]),
            "publication_id": str(progress[2]),
            "window_id": str(progress[3]),
            "published_snapshot_id": int(progress[1]),
            "normalization_contract_version": str(progress[4]),
            "exact_receipt_digest": str(progress[5]),
            "pointer_validation_hash": str(progress[6]),
            "generation_certification_hash": str(progress[7]),
            "source_event_count": 3,
            "source_market_count": 4,
            "source_event_hash": source_hashes[0],
            "source_market_hash": source_hashes[1],
            "source_identity_hash": source_hashes[2],
            "projection_member_receipt_digest": member_receipt_digest,
            "projection_universe_hash": "4" * 64,
            "projection_group_truth_hash": "5" * 64,
            "generation_universe_hash": "6" * 64,
            "generation_group_truth_hash": "7" * 64,
            "generation_projection_member_comparison_count": 1,
            "generation_projection_member_comparison_root": "4" * 64,
            "generation_source_group_truth_comparison_count": 1,
            "generation_source_group_truth_comparison_root": "5" * 64,
            "class_counts_json": class_counts_json,
            "class_digests_json": json.dumps(
                sealed_class_digests,
                sort_keys=True,
                separators=(",", ":"),
            ),
            "diagnostic_counts_json": "{}",
            "diagnostic_root": "d" * 64,
            "diagnostic_samples_json": "{}",
            "diagnostic_samples_digest": hashlib.sha256(b"{}").hexdigest(),
            "legacy_reconstruction_root": "8" * 64,
            "generation_reconstruction_root": "9" * 64,
            "overlap_conflict_count": 0,
            "unclassified_count": 0,
            "projection_candidate_count": int(progress[9]),
            "projection_exclusion_count": int(progress[10]),
            "projection_exclusion_counts_json": str(progress[11]),
            "projection_exclusion_roots_json": str(progress[12]),
            "created_at_ms": 3_001,
        }
        receipt_digest = hashlib.sha256(
            json.dumps(
                tuple(payload[field] for field in digest_fields),
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        insert_fields = (
            digest_fields
            if "hash_algorithm" in digest_fields
            else ("hash_algorithm", *digest_fields)
        )
        con.execute(
            "INSERT INTO structure_generation_drift_receipts("
            + ",".join(insert_fields)
            + ",receipt_digest) VALUES ("
            + ",".join("?" for _ in range(len(insert_fields) + 1))
            + ")",
            (*(payload[field] for field in insert_fields), receipt_digest),
        )
        con.execute(
            "UPDATE structure_generation_drift_progress SET phase='sealed',"
            "source_event_hash=?,source_market_hash=?,source_identity_hash=?,"
            "projection_member_receipt_digest=?,class_counts_json=?,"
            "class_digests_json=?,diagnostic_root=? "
            "WHERE comparison_id=?",
            (
                *source_hashes,
                member_receipt_digest,
                json.dumps(
                    {
                        **{
                            f"class_count:{tag}": count
                            for tag, count in sealed_class_counts.items()
                        },
                        "projection_member_count": 1,
                        "generation_member_count": 1,
                        "generation_projection_member_comparison_count": 1,
                        "generation_member_scan_count": 1,
                        "legacy_member_scan_count": 1,
                        "source_group_truth_count": 1,
                        "generation_group_truth_count": 1,
                        "generation_source_group_truth_comparison_count": 1,
                    }
                ),
                json.dumps(
                    {
                        "receipt_digest": receipt_digest,
                        "projection_member_root": "4" * 64,
                        "generation_member_root": "6" * 64,
                        "source_group_truth_hash": "5" * 64,
                        "generation_group_truth_hash": "7" * 64,
                        "generation_projection_member_comparison_root": "4" * 64,
                        "generation_source_group_truth_comparison_root": "5" * 64,
                        "sealed_class_digests": sealed_class_digests,
                        "legacy_reconstruction_root": "8" * 64,
                        "generation_reconstruction_root": "9" * 64,
                    }
                ),
                "d" * 64,
                comparison_id,
            ),
        )


def _rewrite_drift_receipt(
    store: SQLiteStore,
    comparison_id: str,
    **changes: object,
) -> None:
    with sqlite3.connect(store.db_path) as con:
        contract = con.execute(
            "SELECT classifier_contract_version FROM "
            "structure_generation_drift_receipts WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone()[0]
        digest_fields = sqlite_store_module._structure_drift_receipt_fields(
            str(contract)
        )
        con.execute("DROP TRIGGER trg_structure_drift_receipt_update")
        row = con.execute(
            "SELECT " + ",".join(digest_fields) + " FROM "
            "structure_generation_drift_receipts WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone()
        assert row is not None
        payload = dict(zip(digest_fields, row, strict=True))
        payload.update({key: value for key, value in changes.items() if key in payload})
        receipt_digest = hashlib.sha256(
            json.dumps(
                tuple(payload[field] for field in digest_fields),
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        assignments = [f"{key}=?" for key in changes]
        con.execute(
            "UPDATE structure_generation_drift_receipts SET "
            + ",".join((*assignments, "receipt_digest=?"))
            + " WHERE comparison_id=?",
            (*changes.values(), receipt_digest, comparison_id),
        )
        con.execute(
            "UPDATE structure_generation_drift_progress SET class_digests_json="
            "json_set(class_digests_json,'$.receipt_digest',?) "
            "WHERE comparison_id=?",
            (receipt_digest, comparison_id),
        )


def _make_exact_receipt_authoritative(store: SQLiteStore) -> None:
    with sqlite3.connect(store.db_path) as con:
        con.execute("DROP TRIGGER trg_structure_comparison_receipt_update")
        row = con.execute(
            "SELECT generation_snapshot_id,publication_id,legacy_snapshot_id,"
            "legacy_market_count,legacy_universe_hash,legacy_source_truth_hash,"
            "generation_validation_hash,created_at_ms FROM "
            "structure_generation_comparison_receipts"
        ).fetchone()
        assert row is not None
        digest = sqlite_store_module._comparison_receipt_digest(
            generation_snapshot_id=int(row[0]),
            publication_id=str(row[1]),
            legacy_snapshot_id=int(row[2]),
            legacy_market_count=int(row[3]),
            generation_market_count=int(row[3]),
            legacy_universe_hash=str(row[4]),
            generation_universe_hash=str(row[4]),
            legacy_source_truth_hash=str(row[5]),
            generation_source_truth_hash=str(row[5]),
            generation_validation_hash=str(row[6]),
            created_at_ms=int(row[7]),
        )
        con.execute(
            "UPDATE structure_generation_comparison_receipts SET "
            "generation_market_count=legacy_market_count,"
            "generation_universe_hash=legacy_universe_hash,"
            "generation_source_truth_hash=legacy_source_truth_hash,receipt_digest=?",
            (digest,),
        )
        con.execute(
            "UPDATE current_structure_generation SET comparison_receipt_digest=? "
            "WHERE id=1",
            (digest,),
        )


def test_sealed_v1_receipt_cannot_authorize_v2_progress(tmp_path: Path) -> None:
    store = _drift_store(tmp_path)
    comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)
    _install_sealed_drift_authority(store, comparison_id)
    assert store.structure_generation_drift_status()["authorized"] is True

    _rewrite_drift_receipt(
        store,
        comparison_id,
        classifier_contract_version=STRUCTURE_DRIFT_CLASSIFIER_V1,
    )

    status = store.structure_generation_drift_status()
    assert status["authorized"] is False
    assert status["reason"] == "structure-drift-receipt-invalid"


def test_receipt_algorithm_substitution_without_digest_rewrite_fails(
    tmp_path: Path,
) -> None:
    store = _drift_store(tmp_path)
    comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)
    _install_sealed_drift_authority(store, comparison_id)
    with sqlite3.connect(store.db_path) as con:
        con.execute("DROP TRIGGER trg_structure_drift_receipt_update")
        con.execute(
            "UPDATE structure_generation_drift_receipts SET "
            "hash_algorithm='serializable-sha256-v1' WHERE comparison_id=?",
            (comparison_id,),
        )

    status = store.structure_generation_drift_status()
    assert status["authorized"] is False
    assert status["reason"] == "structure-drift-receipt-invalid"


def test_sealed_v1_progress_and_receipt_are_not_v2_authority(tmp_path: Path) -> None:
    store = _drift_store(tmp_path)
    comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)
    _install_sealed_drift_authority(store, comparison_id)
    _rewrite_drift_receipt(
        store,
        comparison_id,
        hash_algorithm="serializable-sha256-v1",
    )
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "UPDATE structure_generation_drift_progress SET "
            "hash_algorithm='serializable-sha256-v1' WHERE comparison_id=?",
            (comparison_id,),
        )

    status = store.structure_generation_drift_status()
    assert status["authorized"] is False
    assert status["authorization_mode"] == "none"
    assert status["reason"] == "structure-drift-progress-missing"


@pytest.mark.parametrize(
    ("field", "replacement"),
    (
        ("pointer_validation_hash", "b" * 64),
        ("source_identity_hash", "c" * 64),
    ),
)
def test_sealed_receipt_pointer_and_source_identity_drift_fail_closed(
    tmp_path: Path,
    field: str,
    replacement: str,
) -> None:
    store = _drift_store(tmp_path)
    comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)
    _install_sealed_drift_authority(store, comparison_id)

    _rewrite_drift_receipt(store, comparison_id, **{field: replacement})

    status = store.structure_generation_drift_status()
    assert status["authorized"] is False
    assert status["reason"] == "structure-drift-receipt-invalid"
    assert status["class_counts"] == {}


@pytest.mark.parametrize(
    ("field", "replacement"),
    (
        ("projection_universe_hash", "a" * 64),
        ("generation_universe_hash", "b" * 64),
        ("projection_group_truth_hash", "c" * 64),
        ("generation_group_truth_hash", "d" * 64),
        ("generation_projection_member_comparison_count", 2),
        ("generation_projection_member_comparison_root", "e" * 64),
        ("generation_source_group_truth_comparison_count", 2),
        ("generation_source_group_truth_comparison_root", "f" * 64),
        (
            "class_counts_json",
            json.dumps(
                {
                    "shared": 2,
                    "fresh-addition": 0,
                    "current-nontradable": 0,
                    "event-only-quarantine": 0,
                    "market-side-quarantine": 0,
                    "fresh-source-absent": 0,
                    "overlap-conflict": 0,
                    "unclassified": 0,
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
        ),
        ("class_digests_json", json.dumps({"shared": "0" * 64})),
        ("legacy_reconstruction_root", "1" * 64),
        ("generation_reconstruction_root", "2" * 64),
    ),
)
def test_sealed_receipt_audit_and_comparison_commitment_tamper_fails_closed(
    tmp_path: Path,
    field: str,
    replacement: object,
) -> None:
    store = _drift_store(tmp_path)
    comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)
    _install_sealed_drift_authority(store, comparison_id)

    _rewrite_drift_receipt(store, comparison_id, **{field: replacement})

    status = store.structure_generation_drift_status()
    assert status["authorized"] is False
    assert status["reason"] == "structure-drift-receipt-invalid"
    assert status["class_counts"] == {}


@pytest.mark.parametrize(
    "count_key",
    (
        "projection_member_count",
        "generation_member_count",
        "generation_projection_member_comparison_count",
        "source_group_truth_count",
        "generation_group_truth_count",
        "generation_source_group_truth_comparison_count",
    ),
)
def test_sealed_progress_audit_and_comparison_count_tamper_fails_closed(
    tmp_path: Path,
    count_key: str,
) -> None:
    store = _drift_store(tmp_path)
    comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)
    _install_sealed_drift_authority(store, comparison_id)
    assert store.structure_generation_drift_status()["authorized"] is True
    with sqlite3.connect(store.db_path) as con:
        counts_json = con.execute(
            "SELECT class_counts_json FROM structure_generation_drift_progress "
            "WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone()[0]
        counts = json.loads(str(counts_json))
        counts[count_key] = int(counts[count_key]) + 1_000
        con.execute(
            "UPDATE structure_generation_drift_progress SET class_counts_json=? "
            "WHERE comparison_id=?",
            (
                json.dumps(counts, sort_keys=True, separators=(",", ":")),
                comparison_id,
            ),
        )

    status = store.structure_generation_drift_status()
    assert status["authorized"] is False
    assert status["reason"] == "structure-drift-receipt-invalid"


@pytest.mark.parametrize(
    "tamper",
    (
        "class-count",
        "class-digest",
        "legacy-reconstruction",
        "generation-reconstruction",
    ),
)
def test_sealed_progress_class_and_reconstruction_tamper_fails_closed(
    tmp_path: Path,
    tamper: str,
) -> None:
    store = _drift_store(tmp_path)
    comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)
    _install_sealed_drift_authority(store, comparison_id)
    assert store.structure_generation_drift_status()["authorized"] is True
    with sqlite3.connect(store.db_path) as con:
        counts_json, digests_json = con.execute(
            "SELECT class_counts_json,class_digests_json FROM "
            "structure_generation_drift_progress WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone()
        counts = json.loads(str(counts_json))
        digests = json.loads(str(digests_json))
        if tamper == "class-count":
            counts["class_count:shared"] += 1
        elif tamper == "class-digest":
            digests["sealed_class_digests"]["shared"] = "b" * 64
        elif tamper == "legacy-reconstruction":
            digests["legacy_reconstruction_root"] = "c" * 64
        else:
            digests["generation_reconstruction_root"] = "d" * 64
        con.execute(
            "UPDATE structure_generation_drift_progress SET class_counts_json=?,"
            "class_digests_json=? WHERE comparison_id=?",
            (
                json.dumps(counts, sort_keys=True, separators=(",", ":")),
                json.dumps(digests, sort_keys=True, separators=(",", ":")),
                comparison_id,
            ),
        )

    status = store.structure_generation_drift_status()
    assert status["authorized"] is False
    assert status["reason"] == "structure-drift-receipt-invalid"
    assert status["class_counts"] == {}


def test_exact_authorization_is_independent_of_drift_receipt_algorithm(
    tmp_path: Path,
) -> None:
    store = _drift_store(tmp_path)
    comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)
    _install_sealed_drift_authority(store, comparison_id)
    _rewrite_drift_receipt(
        store,
        comparison_id,
        hash_algorithm="serializable-sha256-v1",
    )
    _make_exact_receipt_authoritative(store)

    status = store.structure_generation_drift_status()
    assert status["authorized"] is True
    assert status["authorization_mode"] == "exact"
    assert status["phase"] == "exact"


def test_nonempty_drift_state_machine_seals_all_partitions_and_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _drift_store(tmp_path)
    comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)
    observed_phases: set[str] = set()
    phase_order: list[str] = []
    for now_ms in range(3_001, 3_100):
        store = SQLiteStore(store.db_path)
        chunk = store.advance_structure_drift_comparison_chunk(
            comparison_id, max_rows=1, now_ms=now_ms
        )
        assert chunk.rows_processed <= 1
        observed_phases.add(str(chunk.component))
        if not phase_order or phase_order[-1] != chunk.component:
            phase_order.append(str(chunk.component))
        if chunk.component in {"sealed", "stale"}:
            break
    else:
        pytest.fail("drift comparison did not seal")
    with sqlite3.connect(store.db_path) as con:
        debug_row = con.execute(
            "SELECT class_counts_json,class_digests_json FROM "
            "structure_generation_drift_progress WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone()
    debug_counts = json.loads(debug_row[0])
    debug_digests = json.loads(debug_row[1])
    assert debug_counts.get("projection_member_count") == debug_counts.get(
        "generation_member_count"
    )
    assert debug_digests.get("projection_member_root") == debug_digests.get(
        "generation_projection_member_comparison_root"
    )
    assert debug_digests.get("projection_member_root") != debug_digests.get(
        "generation_member_root"
    )
    assert debug_digests.get("source_group_truth_hash") == debug_digests.get(
        "generation_source_group_truth_comparison_root"
    )
    assert debug_digests.get("source_group_truth_hash") != debug_digests.get(
        "generation_group_truth_hash"
    )
    assert debug_counts.get("class_count:overlap-conflict", 0) == 0
    assert debug_counts.get("class_count:unclassified", 0) == 0
    assert chunk.component == "sealed"
    assert tuple(phase_order) == (
        "source-events",
        "source-markets",
        "fresh-projection-members",
        "generation-members",
        "legacy-members",
        "fresh-group-truth",
        "sealed",
    )
    assert {
        "source-events",
        "source-markets",
        "fresh-projection-members",
        "generation-members",
        "legacy-members",
        "fresh-group-truth",
        "sealed",
    } <= observed_phases
    with sqlite3.connect(store.db_path) as con:
        receipt_fields = sqlite_store_module._structure_drift_receipt_fields(
            STRUCTURE_DRIFT_CLASSIFIER_V3
        )
        row = con.execute(
            "SELECT "
            + ",".join(receipt_fields)
            + ",receipt_digest,class_counts_json FROM "
            "structure_generation_drift_receipts WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone()
    assert row is not None
    field_count = len(receipt_fields)
    payload = dict(
        zip(
            receipt_fields,
            row[:field_count],
            strict=True,
        )
    )
    assert row[field_count] == sqlite_store_module._structure_drift_receipt_digest(
        payload
    )
    classes = json.loads(row[field_count + 1])
    assert classes == {
        "current-nontradable": 1,
        "event-only-quarantine": 1,
        "fresh-addition": 1,
            "fresh-source-absent": 1,
            "fresh-group-ineligible": 0,
        "market-side-quarantine": 1,
        "overlap-conflict": 0,
        "shared": 1,
        "unclassified": 0,
    }
    status = store.structure_generation_drift_status()
    assert status["authorized"] is True
    assert status["authorization_mode"] == "drift-safe-sealed"
    assert status["phase"] == "sealed"
    assert status["receipt_digest"] == row[field_count]
    from polyarb.snapshot import cli as cli_module

    monkeypatch.setattr(
        cli_module,
        "load_settings",
        lambda: SimpleNamespace(db_path=store.db_path),
    )
    cli_result = CliRunner().invoke(
        cli_module.app, ["structure-generation-drift-compare"]
    )
    assert cli_result.exit_code == 0, cli_result.stdout
    assert json.loads(cli_result.stdout)["authorization_mode"] == "drift-safe-sealed"
    substituted = dict(payload)
    substituted["projection_universe_hash"] = "f" * 64
    assert (
        sqlite_store_module._structure_drift_receipt_digest(substituted)
        != row[field_count]
    )
    with sqlite3.connect(store.db_path) as con:
        with pytest.raises(sqlite3.IntegrityError, match="receipt-sealed"):
            con.execute(
                "UPDATE structure_generation_drift_receipts SET created_at_ms=9999 "
                "WHERE comparison_id=?",
                (comparison_id,),
            )
        with pytest.raises(sqlite3.IntegrityError, match="receipt-sealed"):
            con.execute(
                "DELETE FROM structure_generation_drift_receipts WHERE comparison_id=?",
                (comparison_id,),
            )
        con.execute("DROP TRIGGER trg_structure_drift_receipt_update")
        con.execute(
            "UPDATE structure_generation_drift_receipts SET "
            "projection_universe_hash=? WHERE comparison_id=?",
            ("f" * 64, comparison_id),
        )
    tampered_status = store.structure_generation_drift_status()
    assert tampered_status["authorized"] is False
    assert tampered_status["reason"] == "structure-drift-receipt-invalid"


def test_v3_checkpoint_commitments_are_chunk_partition_independent(
    tmp_path: Path,
) -> None:
    commitment_fields = (
        "source_event_hash",
        "source_market_hash",
        "source_identity_hash",
        "projection_universe_hash",
        "generation_universe_hash",
        "projection_group_truth_hash",
        "generation_group_truth_hash",
        "generation_projection_member_comparison_count",
        "generation_projection_member_comparison_root",
        "generation_source_group_truth_comparison_count",
        "generation_source_group_truth_comparison_root",
        "class_counts_json",
        "class_digests_json",
        "legacy_reconstruction_root",
        "generation_reconstruction_root",
        "diagnostic_counts_json",
        "diagnostic_root",
        "diagnostic_samples_json",
        "diagnostic_samples_digest",
        "projection_candidate_count",
        "projection_exclusion_count",
        "projection_exclusion_counts_json",
        "projection_exclusion_roots_json",
        "receipt_digest",
    )
    observed: list[tuple[object, ...]] = []
    base = _drift_store(tmp_path / "sealed-source")
    for max_rows in (1, 17, 500):
        clone_path = tmp_path / f"rows-{max_rows}" / "drift-e2e.db"
        clone_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(base.db_path) as source, sqlite3.connect(
            clone_path
        ) as destination:
            source.backup(destination)
        store = SQLiteStore(clone_path)
        comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)
        for now_ms in range(3_001, 3_100):
            chunk = store.advance_structure_drift_comparison_chunk(
                comparison_id,
                max_rows=max_rows,
                now_ms=now_ms,
            )
            if chunk.component in {"sealed", "stale"}:
                break
        assert chunk.component == "sealed"
        assert store.structure_generation_drift_status()["authorized"] is True
        with sqlite3.connect(store.db_path) as con:
            row = con.execute(
                "SELECT " + ",".join(commitment_fields) + " FROM "
                "structure_generation_drift_receipts WHERE comparison_id=?",
                (comparison_id,),
            ).fetchone()
            terminal_reason = con.execute(
                "SELECT terminal_reason FROM structure_generation_drift_progress "
                "WHERE comparison_id=?",
                (comparison_id,),
            ).fetchone()[0]
        assert row is not None
        observed.append((*tuple(row), terminal_reason))

    assert observed[0] == observed[1] == observed[2]


def test_v3_checkpoint_conserves_one_member_and_every_exclusion_reason(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[tuple[object, ...]] = []
    base = _drift_store(tmp_path / "mixed-source")
    real_chunk = base._fetch_structure_drift_fresh_projection_chunk(
        publication_id="publication-2",
        generation_snapshot_id=2,
        cursor=None,
        limit=500,
        classifier_contract=STRUCTURE_DRIFT_CLASSIFIER_V3,
    )
    member = next(
        item
        for item in base.fetch_structure_drift_member_chunk(
            snapshot_id=2,
            generation=True,
            after_market_id=None,
            limit=500,
        )
        if item.market_id == "shared"
    )
    template = real_chunk.exclusions[0]
    outcomes = (
        FreshProjectionChunk(
            cursor=None,
            members=(member,),
            diagnostics=(),
            candidates_processed=1,
        ),
        *(
            FreshProjectionChunk(
                cursor=None,
                members=(),
                diagnostics=(),
                candidates_processed=1,
                exclusions=(
                    replace(
                        template,
                        reason=reason,
                        envelope=replace(
                            template.envelope,
                            market_id=f"synthetic-{index:02d}",
                        ),
                    ),
                ),
            )
            for index, reason in enumerate(
                STRUCTURE_PROJECTION_EXCLUSION_REASONS, start=1
            )
        ),
    )
    source_keys = ("shared",) + tuple(
        f"synthetic-{index:02d}"
        for index in range(1, len(STRUCTURE_PROJECTION_EXCLUSION_REASONS) + 1)
    )
    fetch_limits: list[int] = []

    def fetch_mixed(
        _store: SQLiteStore,
        *,
        cursor: FreshProjectionCursor | None,
        limit: int,
        **_kwargs: object,
    ) -> FreshProjectionChunk:
        fetch_limits.append(limit)
        if cursor is None:
            start = 0
        else:
            assert cursor.stream == "market"
            assert cursor.market_id is not None
            start = source_keys.index(cursor.market_id) + 1
        end = min(start + limit, len(outcomes))
        page = outcomes[start:end]
        assert 1 <= len(page) <= limit
        return FreshProjectionChunk(
            cursor=(
                None
                if end == len(outcomes)
                    else FreshProjectionCursor(
                        stream="market",
                        market_id=source_keys[end - 1],
                    event_id=None,
                    source_ordinal=None,
                    member_ordinal=None,
                )
            ),
            members=tuple(item for chunk in page for item in chunk.members),
            diagnostics=(),
            candidates_processed=len(page),
            exclusions=tuple(item for chunk in page for item in chunk.exclusions),
        )

    monkeypatch.setattr(
        SQLiteStore,
        "_fetch_structure_drift_fresh_projection_chunk",
        fetch_mixed,
    )
    real_fetch_members_by_id = SQLiteStore.fetch_structure_drift_members_by_id

    def fetch_members_by_id(
        store: SQLiteStore,
        *,
        snapshot_id: int,
        generation: bool,
        market_ids: list[str],
    ) -> list[object]:
        if generation and snapshot_id == 2 and market_ids == ["shared"]:
            return [member]
        return real_fetch_members_by_id(
            store,
            snapshot_id=snapshot_id,
            generation=generation,
            market_ids=market_ids,
        )

    monkeypatch.setattr(
        SQLiteStore,
        "fetch_structure_drift_members_by_id",
        fetch_members_by_id,
    )

    def expected_candidate_count(
        _con: sqlite3.Connection, *, window_id: str
    ) -> int:
        assert window_id == "window-2"
        return len(outcomes)

    monkeypatch.setattr(
        sqlite_store_module,
        "_fresh_projection_expected_candidate_count",
        expected_candidate_count,
    )
    for max_rows in (1, 17, 500):
        clone_path = tmp_path / f"mixed-{max_rows}" / "drift-e2e.db"
        clone_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(base.db_path) as source, sqlite3.connect(
            clone_path
        ) as destination:
            source.backup(destination)
        store = SQLiteStore(clone_path)
        comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)
        now_ms = 3_001
        while True:
            with sqlite3.connect(store.db_path) as con:
                phase = con.execute(
                    "SELECT phase FROM structure_generation_drift_progress "
                    "WHERE comparison_id=?",
                    (comparison_id,),
                ).fetchone()[0]
            if phase == "fresh-projection-members":
                break
            store.advance_structure_drift_comparison_chunk(
                comparison_id, max_rows=max_rows, now_ms=now_ms
            )
            now_ms += 1
        projection_checkpoints: list[tuple[int, int, dict[str, int]]] = []
        while True:
            store.advance_structure_drift_comparison_chunk(
                comparison_id, max_rows=max_rows, now_ms=now_ms
            )
            now_ms += 1
            with sqlite3.connect(store.db_path) as con:
                row = con.execute(
                    "SELECT phase,projection_candidate_count,"
                    "projection_exclusion_count,projection_exclusion_counts_json,"
                    "projection_exclusion_digest_states_json,class_counts_json FROM "
                    "structure_generation_drift_progress WHERE comparison_id=?",
                    (comparison_id,),
                ).fetchone()
            assert row is not None
            exclusion_counts = json.loads(str(row[3]))
            exclusion_states = json.loads(str(row[4]))
            assert {
                reason: RowChainSHA256.from_json(
                    exclusion_states[reason],
                    expected_domain=f"projection-exclusion/{reason}",
                ).count
                for reason in STRUCTURE_PROJECTION_EXCLUSION_REASONS
            } == exclusion_counts
            projection_checkpoints.append(
                (int(row[1]), int(row[2]), exclusion_counts)
            )
            store = SQLiteStore(clone_path)
            if row[0] != "fresh-projection-members":
                assert json.loads(str(row[5]))["projection_member_count"] == 1
                break
        if max_rows == 1:
            assert len(projection_checkpoints) == 8
            assert [checkpoint[0] for checkpoint in projection_checkpoints] == list(
                range(1, 9)
            )
            assert [checkpoint[1] for checkpoint in projection_checkpoints] == [
                0,
                1,
                2,
                3,
                4,
                5,
                6,
                7,
            ]
            assert {
                reason
                for _, _, counts in projection_checkpoints
                for reason, count in counts.items()
                if count
            } == set(STRUCTURE_PROJECTION_EXCLUSION_REASONS)

        while True:
            chunk = store.advance_structure_drift_comparison_chunk(
                comparison_id, max_rows=max_rows, now_ms=now_ms
            )
            now_ms += 1
            store = SQLiteStore(clone_path)
            if chunk.component in {"sealed", "stale"}:
                break
        with sqlite3.connect(store.db_path) as con:
            terminal_reason = con.execute(
                "SELECT terminal_reason FROM structure_generation_drift_progress "
                "WHERE comparison_id=?",
                (comparison_id,),
            ).fetchone()[0]
        assert chunk.component == "stale"
        assert terminal_reason == "drift-unclassified"
        status = store.structure_generation_drift_status()
        _assert_stale_terminal_public_evidence_suppressed(
            status,
        )
        with sqlite3.connect(store.db_path) as con:
            progress = con.execute(
                "SELECT projection_candidate_count,projection_exclusion_count,"
                "projection_exclusion_counts_json,projection_exclusion_roots_json "
                "FROM structure_generation_drift_progress WHERE comparison_id=?",
                (comparison_id,),
            ).fetchone()
            receipt = con.execute(
                "SELECT projection_candidate_count,projection_exclusion_count,"
                "projection_exclusion_counts_json,projection_exclusion_roots_json "
                "FROM structure_generation_drift_terminal_receipts "
                "WHERE comparison_id=?",
                (comparison_id,),
            ).fetchone()
        assert progress is not None
        assert receipt is not None
        assert tuple(progress) == tuple(receipt)
        assert int(progress[0]) - int(progress[1]) == 1
        observed.append(tuple(progress))

    assert set(fetch_limits) == {1, 17, 500}
    assert observed[0] == observed[1] == observed[2]


def _run_drift_to_terminal(
    store: SQLiteStore, comparison_id: str, *, start_ms: int = 3_001
) -> str:
    for now_ms in range(start_ms, start_ms + 100):
        chunk = store.advance_structure_drift_comparison_chunk(
            comparison_id, max_rows=17, now_ms=now_ms
        )
        if chunk.component in {"sealed", "stale"}:
            return chunk.component
    pytest.fail("drift comparison did not terminate")


def _sealed_v3_store(tmp_path: Path) -> tuple[SQLiteStore, str]:
    store = _drift_store(tmp_path)
    comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)
    assert _run_drift_to_terminal(store, comparison_id) == "sealed"
    return store, comparison_id


def _rewrite_v3_exclusion_evidence(
    store: SQLiteStore,
    comparison_id: str,
    *,
    counts: dict[str, int] | None = None,
    roots: dict[str, str] | None = None,
    null_field: str | None = None,
    corrupt_digest: bool = False,
) -> None:
    fields = sqlite_store_module._structure_drift_receipt_fields(
        STRUCTURE_DRIFT_CLASSIFIER_V3
    )
    with sqlite3.connect(store.db_path) as con:
        row = con.execute(
            "SELECT " + ",".join(fields) + " FROM "
            "structure_generation_drift_receipts WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone()
        assert row is not None
        payload = dict(zip(fields, row, strict=True))
        if counts is not None:
            encoded_counts = json.dumps(counts, sort_keys=True, separators=(",", ":"))
            payload["projection_exclusion_counts_json"] = encoded_counts
            con.execute(
                "UPDATE structure_generation_drift_progress SET "
                "projection_exclusion_counts_json=? WHERE comparison_id=?",
                (encoded_counts, comparison_id),
            )
        if roots is not None:
            encoded_roots = json.dumps(roots, sort_keys=True, separators=(",", ":"))
            payload["projection_exclusion_roots_json"] = encoded_roots
            con.execute(
                "UPDATE structure_generation_drift_progress SET "
                "projection_exclusion_roots_json=? WHERE comparison_id=?",
                (encoded_roots, comparison_id),
            )
        if null_field is not None:
            payload[null_field] = None
        digest = sqlite_store_module._structure_drift_receipt_digest(payload)
        if corrupt_digest:
            digest = "f" * 64 if digest != "f" * 64 else "e" * 64
        con.execute("DROP TRIGGER trg_structure_drift_receipt_update")
        assignments = [
            "projection_exclusion_counts_json=?",
            "projection_exclusion_roots_json=?",
            "receipt_digest=?",
        ]
        values: list[object] = [
            payload["projection_exclusion_counts_json"],
            payload["projection_exclusion_roots_json"],
            digest,
        ]
        if null_field is not None:
            assignments.append(f"{null_field}=?")
            values.append(None)
        con.execute(
            "UPDATE structure_generation_drift_receipts SET "
            + ",".join(assignments)
            + " WHERE comparison_id=?",
            (*values, comparison_id),
        )


def test_v3_sealed_status_exposes_authenticated_expected_exclusions(
    tmp_path: Path,
) -> None:
    store, _ = _sealed_v3_store(tmp_path)

    status = store.structure_generation_drift_status()

    assert status["authorized"] is True
    assert status["classifier_contract_version"] == STRUCTURE_DRIFT_CLASSIFIER_V3
    assert status["projection_candidate_count"] == (
        status["projection_member_count"]
        + status["projection_exclusion_count"]
        + status["projection_diagnostic_count"]
    )
    assert sum(status["projection_exclusion_counts"].values()) == (
        status["projection_exclusion_count"]
    )
    assert list(status["projection_exclusion_counts"]) == sorted(
        status["projection_exclusion_counts"]
    )
    assert set(status["projection_exclusion_counts"]) == set(
        status["projection_exclusion_roots"]
    )
    assert all(value > 0 for value in status["projection_exclusion_counts"].values())


def _forge_v3_terminal_candidate_and_exclusion_count(
    store: SQLiteStore,
    comparison_id: str,
    *,
    terminal: str,
) -> None:
    receipt_table = (
        "structure_generation_drift_receipts"
        if terminal == "sealed"
        else "structure_generation_drift_terminal_receipts"
    )
    receipt_fields = (
        sqlite_store_module._structure_drift_receipt_fields(
            STRUCTURE_DRIFT_CLASSIFIER_V3
        )
        if terminal == "sealed"
        else sqlite_store_module._structure_drift_terminal_receipt_fields(
            STRUCTURE_DRIFT_CLASSIFIER_V3
        )
    )
    digest_helper = (
        sqlite_store_module._structure_drift_receipt_digest
        if terminal == "sealed"
        else sqlite_store_module._structure_drift_terminal_receipt_digest
    )
    reason = "non-neg-risk-market"
    with sqlite3.connect(store.db_path) as con:
        progress = con.execute(
            "SELECT projection_exclusion_counts_json,"
            "projection_exclusion_digest_states_json FROM "
            "structure_generation_drift_progress WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone()
        receipt = con.execute(
            "SELECT " + ",".join(receipt_fields) + " FROM " + receipt_table
            + " WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone()
        assert progress is not None
        assert receipt is not None
        counts = json.loads(str(progress[0]))
        states = json.loads(str(progress[1]))
        chain = RowChainSHA256.from_json(
            states[reason], expected_domain=f"projection-exclusion/{reason}"
        )
        chain.update(("forged-source-candidate", comparison_id))
        counts[reason] += 1
        states[reason] = chain.to_json()
        roots = {
            key: RowChainSHA256.from_json(
                state, expected_domain=f"projection-exclusion/{key}"
            ).hexdigest()
            for key, state in states.items()
        }
        counts_json = json.dumps(counts, sort_keys=True, separators=(",", ":"))
        states_json = json.dumps(states, sort_keys=True, separators=(",", ":"))
        roots_json = json.dumps(roots, sort_keys=True, separators=(",", ":"))
        payload = dict(zip(receipt_fields, receipt, strict=True))
        payload["projection_candidate_count"] = (
            int(payload["projection_candidate_count"]) + 1
        )
        payload["projection_exclusion_count"] = (
            int(payload["projection_exclusion_count"]) + 1
        )
        payload["projection_exclusion_counts_json"] = counts_json
        payload["projection_exclusion_roots_json"] = roots_json
        receipt_digest = digest_helper(payload)
        trigger = (
            "trg_structure_drift_receipt_update"
            if terminal == "sealed"
            else "trg_structure_drift_terminal_receipt_update"
        )
        con.execute(f"DROP TRIGGER {trigger}")
        con.execute(
            "UPDATE structure_generation_drift_progress SET "
            "projection_candidate_count=projection_candidate_count+1,"
            "projection_exclusion_count=projection_exclusion_count+1,"
            "projection_exclusion_counts_json=?,projection_exclusion_roots_json=?,"
            "projection_exclusion_digest_states_json=? WHERE comparison_id=?",
            (counts_json, roots_json, states_json, comparison_id),
        )
        con.execute(
            "UPDATE " + receipt_table + " SET "
            "projection_candidate_count=projection_candidate_count+1,"
            "projection_exclusion_count=projection_exclusion_count+1,"
            "projection_exclusion_counts_json=?,projection_exclusion_roots_json=?,"
            "receipt_digest=? WHERE comparison_id=?",
            (counts_json, roots_json, receipt_digest, comparison_id),
        )
        if terminal == "sealed":
            con.execute(
                "UPDATE structure_generation_drift_progress SET "
                "class_digests_json=json_set(class_digests_json,'$.receipt_digest',?) "
                "WHERE comparison_id=?",
                (receipt_digest, comparison_id),
            )


@pytest.mark.parametrize("terminal", ("sealed", "stale"))
def test_v3_terminal_status_recomputes_pinned_candidate_count_before_exposure(
    tmp_path: Path,
    terminal: str,
) -> None:
    if terminal == "sealed":
        store, comparison_id = _sealed_v3_store(tmp_path)
        assert store.structure_generation_drift_status()["authorized"] is True
    else:
        store, comparison_id = _stale_unclassified_v3_store(tmp_path)
        _assert_stale_terminal_public_evidence_suppressed(
            store.structure_generation_drift_status()
        )

    _forge_v3_terminal_candidate_and_exclusion_count(
        store, comparison_id, terminal=terminal
    )

    status = store.structure_generation_drift_status()
    assert status["authorized"] is False
    assert status["reason"] == (
        "structure-drift-receipt-invalid"
        if terminal == "sealed"
        else "structure-drift-terminal-receipt-invalid"
    )
    assert "projection_candidate_count" not in status
    assert "projection_exclusion_counts" not in status


@pytest.mark.parametrize("tamper", ("unknown-reason", "missing-reason", "count-sum", "one-root"))
def test_v3_sealed_status_rejects_semantic_exclusion_tamper(
    tmp_path: Path,
    tamper: str,
) -> None:
    store, comparison_id = _sealed_v3_store(tmp_path)
    status = store.structure_generation_drift_status()
    counts = dict(status["projection_exclusion_counts"])
    roots = dict(status["projection_exclusion_roots"])
    with sqlite3.connect(store.db_path) as con:
        raw_counts = json.loads(
            con.execute(
                "SELECT projection_exclusion_counts_json FROM "
                "structure_generation_drift_receipts WHERE comparison_id=?",
                (comparison_id,),
            ).fetchone()[0]
        )
        raw_roots = json.loads(
            con.execute(
                "SELECT projection_exclusion_roots_json FROM "
                "structure_generation_drift_receipts WHERE comparison_id=?",
                (comparison_id,),
            ).fetchone()[0]
        )
    if tamper == "unknown-reason":
        raw_counts["unknown-reason"] = 1
        raw_roots["unknown-reason"] = "a" * 64
    elif tamper == "missing-reason":
        raw_counts.pop(STRUCTURE_PROJECTION_EXCLUSION_REASONS[0])
        raw_roots.pop(STRUCTURE_PROJECTION_EXCLUSION_REASONS[0])
    elif tamper == "count-sum":
        reason = next(iter(counts))
        raw_counts[reason] += 1
    else:
        reason = next(iter(roots))
        raw_roots[reason] = "f" * 64
    _rewrite_v3_exclusion_evidence(
        store,
        comparison_id,
        counts=raw_counts,
        roots=raw_roots,
    )

    tampered = store.structure_generation_drift_status()

    assert tampered["authorized"] is False
    assert tampered["reason"] == "structure-drift-receipt-invalid"
    assert "projection_exclusion_counts" not in tampered
    assert "projection_exclusion_roots" not in tampered


def test_v3_sealed_status_rejects_receipt_digest_tamper(tmp_path: Path) -> None:
    store, comparison_id = _sealed_v3_store(tmp_path)
    _rewrite_v3_exclusion_evidence(store, comparison_id, corrupt_digest=True)

    status = store.structure_generation_drift_status()

    assert status["authorized"] is False
    assert status["reason"] == "structure-drift-receipt-invalid"
    assert "projection_candidate_count" not in status


def test_invalid_v3_receipt_never_falls_back_to_valid_v2_authority(
    tmp_path: Path,
) -> None:
    store, comparison_id = _sealed_v3_store(tmp_path)
    v2_comparison_id = "2" * 64
    with sqlite3.connect(store.db_path) as con:
        progress_columns = [
            str(row[1])
            for row in con.execute(
                "PRAGMA table_info(structure_generation_drift_progress)"
            )
        ]
        progress_row = con.execute(
            "SELECT * FROM structure_generation_drift_progress WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone()
        assert progress_row is not None
        historical_progress = dict(
            zip(progress_columns, progress_row, strict=True)
        )
        historical_progress.update(
            comparison_id=v2_comparison_id,
            classifier_contract_version=STRUCTURE_DRIFT_CLASSIFIER_V2,
        )
        con.execute(
            "INSERT INTO structure_generation_drift_progress("
            + ",".join(progress_columns)
            + ") VALUES ("
            + ",".join("?" for _ in progress_columns)
            + ")",
            tuple(historical_progress[column] for column in progress_columns),
        )
        v3_fields = sqlite_store_module._structure_drift_receipt_fields(
            STRUCTURE_DRIFT_CLASSIFIER_V3
        )
        receipt_row = con.execute(
            "SELECT " + ",".join(v3_fields) + " FROM "
            "structure_generation_drift_receipts WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone()
        assert receipt_row is not None
        v3_payload = dict(zip(v3_fields, receipt_row, strict=True))
        v2_fields = sqlite_store_module._structure_drift_receipt_fields(
            STRUCTURE_DRIFT_CLASSIFIER_V2
        )
        v2_payload = {
            field: (
                v2_comparison_id
                if field == "comparison_id"
                else STRUCTURE_DRIFT_CLASSIFIER_V2
                if field == "classifier_contract_version"
                else v3_payload[field]
            )
            for field in v2_fields
        }
        v2_digest = sqlite_store_module._structure_drift_receipt_digest(v2_payload)
        con.execute(
            "INSERT INTO structure_generation_drift_receipts("
            + ",".join(v2_fields)
            + ",receipt_digest) VALUES ("
            + ",".join("?" for _ in range(len(v2_fields) + 1))
            + ")",
            (*(v2_payload[field] for field in v2_fields), v2_digest),
        )
    _rewrite_v3_exclusion_evidence(store, comparison_id, corrupt_digest=True)

    status = store.structure_generation_drift_status()

    assert status["authorized"] is False
    assert status["reason"] == "structure-drift-receipt-invalid"
    assert status["progress_id"] == comparison_id
    assert status["progress_id"] != v2_comparison_id


@pytest.mark.parametrize(
    "null_field",
    (
        "projection_candidate_count",
        "projection_exclusion_count",
        "projection_exclusion_counts_json",
        "projection_exclusion_roots_json",
    ),
)
def test_v3_sealed_status_rejects_null_v3_receipt_fields(
    tmp_path: Path,
    null_field: str,
) -> None:
    store, comparison_id = _sealed_v3_store(tmp_path)
    _rewrite_v3_exclusion_evidence(
        store,
        comparison_id,
        null_field=null_field,
    )

    status = store.structure_generation_drift_status()

    assert status["authorized"] is False
    assert status["reason"] == "structure-drift-receipt-invalid"
    assert "projection_exclusion_count" not in status


def _advance_to_v3_finalization_checkpoint(
    store: SQLiteStore, comparison_id: str
) -> int:
    now_ms = 3_001
    while True:
        with sqlite3.connect(store.db_path) as con:
            phase = con.execute(
                "SELECT phase FROM structure_generation_drift_progress "
                "WHERE comparison_id=?",
                (comparison_id,),
            ).fetchone()[0]
        if phase == "fresh-group-truth":
            break
        store.advance_structure_drift_comparison_chunk(
            comparison_id, max_rows=500, now_ms=now_ms
        )
        now_ms += 1
    chunk = store.advance_structure_drift_comparison_chunk(
        comparison_id, max_rows=500, now_ms=now_ms
    )
    assert chunk.component == "fresh-group-truth"
    return now_ms + 1


@pytest.mark.parametrize(
    ("tamper", "reason"),
    (
        (
            "candidate-conservation",
            "structure-drift-candidate-conservation-invalid",
        ),
        ("candidate-source-count", "structure-drift-candidate-source-count-invalid"),
        ("exclusion-root", "structure-drift-exclusion-commitment-invalid"),
    ),
)
def test_v3_candidate_conservation_finalizer_rejects_independent_tamper(
    tmp_path: Path,
    tamper: str,
    reason: str,
) -> None:
    store = _drift_store(tmp_path)
    comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)
    now_ms = _advance_to_v3_finalization_checkpoint(store, comparison_id)
    with sqlite3.connect(store.db_path) as con:
        if tamper == "candidate-conservation":
            con.execute(
                "UPDATE structure_generation_drift_progress SET "
                "projection_candidate_count=projection_candidate_count+1 "
                "WHERE comparison_id=?",
                (comparison_id,),
            )
        elif tamper == "candidate-source-count":
            counts = json.loads(
                con.execute(
                    "SELECT class_counts_json FROM "
                    "structure_generation_drift_progress WHERE comparison_id=?",
                    (comparison_id,),
                ).fetchone()[0]
            )
            counts["projection_member_count"] -= 1
            con.execute(
                "UPDATE structure_generation_drift_progress SET "
                "projection_candidate_count=projection_candidate_count-1,"
                "class_counts_json=? WHERE comparison_id=?",
                (
                    json.dumps(counts, sort_keys=True, separators=(",", ":")),
                    comparison_id,
                ),
            )
        else:
            roots = json.loads(
                con.execute(
                    "SELECT projection_exclusion_roots_json FROM "
                    "structure_generation_drift_progress WHERE comparison_id=?",
                    (comparison_id,),
                ).fetchone()[0]
            )
            roots["non-neg-risk-market"] = "f" * 64
            con.execute(
                "UPDATE structure_generation_drift_progress SET "
                "projection_exclusion_roots_json=? WHERE comparison_id=?",
                (
                    json.dumps(roots, sort_keys=True, separators=(",", ":")),
                    comparison_id,
                ),
            )

    with pytest.raises(ValueError, match=reason):
        store.advance_structure_drift_comparison_chunk(
            comparison_id, max_rows=500, now_ms=now_ms
        )


def test_stale_overlap_finalization_atomically_seals_terminal_receipt(
    tmp_path: Path,
) -> None:
    store = _drift_store(tmp_path)
    with sqlite3.connect(store.db_path) as con:
        con.execute("DROP TRIGGER trg_structure_generation_markets_frozen_update_v2")
        con.execute(
            "UPDATE structure_generation_markets SET yes_token_id='divergent' "
            "WHERE snapshot_id=2 AND market_id='shared'"
        )
    comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)

    assert _run_drift_to_terminal(store, comparison_id) == "stale"
    with sqlite3.connect(store.db_path) as con:
        progress = con.execute(
            "SELECT phase,terminal_reason FROM structure_generation_drift_progress "
            "WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone()
        receipts = con.execute(
            "SELECT COUNT(*) FROM structure_generation_drift_terminal_receipts "
            "WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone()[0]
    assert progress == ("stale", "drift-overlap-conflict")
    assert receipts == 1
    status = store.structure_generation_drift_status()
    diagnostic_counts, diagnostic_root, _ = _stale_terminal_diagnostic_evidence(
        store, comparison_id
    )
    _assert_stale_terminal_public_aggregate(
        status,
        expected_total=sum(int(value) for value in diagnostic_counts.values()),
        expected_root=diagnostic_root,
    )


def test_two_member_sibling_recovery_seals_v2_reconstruction(
    tmp_path: Path,
) -> None:
    store = _drift_store(tmp_path, sibling_recovery=True)
    immutable_tables = (
        "current_structure_generation",
        "structure_publications",
        "structure_generation_markets",
        "structure_generation_memberships",
        "structure_generation_group_truth",
        "structure_sync_event_metadata_staging",
        "structure_sync_event_member_staging",
        "structure_sync_event_member_receipts",
    )
    with sqlite3.connect(store.db_path) as con:
        immutable_before = {
            table: con.execute(f"SELECT * FROM {table} ORDER BY rowid").fetchall()
            for table in immutable_tables
        }
    comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)

    assert _run_drift_to_terminal(store, comparison_id) == "sealed"
    status = store.structure_generation_drift_status()
    assert status["authorized"] is True
    assert status["class_counts"]["current-nontradable"] == 1
    assert status["class_counts"]["fresh-group-ineligible"] == 1
    assert status["class_counts"]["overlap-conflict"] == 0
    assert status["class_counts"]["unclassified"] == 0
    with sqlite3.connect(store.db_path) as con:
        row = con.execute(
            "SELECT projection_universe_hash,"
            "generation_projection_member_comparison_root,"
            "legacy_reconstruction_root,generation_reconstruction_root,"
            "diagnostic_counts_json FROM structure_generation_drift_receipts "
            "WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone()
        immutable_after = {
            table: con.execute(f"SELECT * FROM {table} ORDER BY rowid").fetchall()
            for table in immutable_tables
        }
    assert row is not None
    assert row[0] == row[1]
    assert all(isinstance(value, str) and len(value) == 64 for value in row[2:4])
    assert json.loads(row[4]) == {}
    assert immutable_after == immutable_before


def test_generation_truth_tamper_cannot_steer_fresh_projection_but_blocks_final_auth(
    tmp_path: Path,
) -> None:
    stores = {
        "baseline": _drift_store(tmp_path / "baseline", sibling_recovery=True),
        "tampered": _drift_store(tmp_path / "tampered", sibling_recovery=True),
    }
    with sqlite3.connect(stores["tampered"].db_path) as con:
        for (trigger,) in con.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' AND "
            "tbl_name='structure_generation_group_truth'"
        ).fetchall():
            con.execute(f'DROP TRIGGER "{trigger}"')
        con.execute(
            "UPDATE structure_generation_group_truth SET membership_hash=? "
            "WHERE snapshot_id=2 AND event_id='event-main'",
            ("f" * 64,),
        )

    projection_evidence: dict[str, tuple[object, ...]] = {}
    comparison_ids = {}
    for label, store in stores.items():
        comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)
        comparison_ids[label] = comparison_id
        for now_ms in range(3_001, 3_050):
            store.advance_structure_drift_comparison_chunk(
                comparison_id, max_rows=1, now_ms=now_ms
            )
            with sqlite3.connect(store.db_path) as con:
                phase, counts_json, digests_json, diagnostic_counts_json = con.execute(
                    "SELECT phase,class_counts_json,class_digests_json,"
                    "diagnostic_counts_json FROM structure_generation_drift_progress "
                    "WHERE comparison_id=?", (comparison_id,),
                ).fetchone()
            if phase == "generation-members":
                counts = json.loads(counts_json)
                digests = json.loads(digests_json)
                projection_evidence[label] = (
                    counts["projection_member_count"],
                    digests["projection_member_root"],
                    diagnostic_counts_json,
                )
                break
        else:
            pytest.fail("fresh projection did not checkpoint")

    assert projection_evidence["baseline"] == projection_evidence["tampered"]
    assert _run_drift_to_terminal(
        stores["baseline"], comparison_ids["baseline"], start_ms=3_100
    ) == "sealed"
    assert _run_drift_to_terminal(
        stores["tampered"], comparison_ids["tampered"], start_ms=3_100
    ) == "stale"
    tampered_status = stores["tampered"].structure_generation_drift_status()
    assert tampered_status["authorized"] is False


@pytest.mark.parametrize("terminal", ("sealed", "stale"))
@pytest.mark.parametrize("tamper", ("missing-member-receipt", "progress-digest"))
def test_terminal_authority_revalidates_current_member_receipt_without_leaking_evidence(
    tmp_path: Path,
    terminal: str,
    tamper: str,
) -> None:
    store = _drift_store(
        tmp_path,
        omit_generation_market_id="addition" if terminal == "stale" else None,
    )
    comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)
    assert _run_drift_to_terminal(store, comparison_id) == terminal
    with sqlite3.connect(store.db_path) as con:
        if tamper == "missing-member-receipt":
            for (trigger,) in con.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger' AND "
                "tbl_name='structure_sync_event_member_receipts'"
            ).fetchall():
                con.execute(f'DROP TRIGGER "{trigger}"')
            con.execute(
                "DELETE FROM structure_sync_event_member_receipts WHERE window_id='window-2'"
            )
        else:
            con.execute(
                "UPDATE structure_generation_drift_progress SET "
                "projection_member_receipt_digest=? WHERE comparison_id=?",
                ("e" * 64, comparison_id),
            )
    status = store.structure_generation_drift_status()
    assert status["authorized"] is False
    assert status["reason"] == "structure-drift-member-receipt-invalid"
    assert "class_counts" not in status
    assert "class_digests" not in status
    assert "diagnostic_counts" not in status
    assert "diagnostic_samples" not in status


def test_generation_omission_finalizes_with_projection_missing_diagnostic(
    tmp_path: Path,
) -> None:
    store = _drift_store(tmp_path, omit_generation_market_id="addition")
    comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)

    assert _run_drift_to_terminal(store, comparison_id) == "stale"
    status = store.structure_generation_drift_status()
    _, diagnostic_root, _ = _stale_terminal_diagnostic_evidence(store, comparison_id)
    _assert_stale_terminal_public_aggregate(
        status,
        expected_total=1,
        expected_root=diagnostic_root,
    )


def test_terminal_receipt_insert_failure_rolls_back_stale_transition(
    tmp_path: Path,
) -> None:
    store = _drift_store(tmp_path)
    with sqlite3.connect(store.db_path) as con:
        con.execute("DROP TRIGGER trg_structure_generation_markets_frozen_update_v2")
        con.execute(
            "UPDATE structure_generation_markets SET yes_token_id='divergent' "
            "WHERE snapshot_id=2 AND market_id='shared'"
        )
        con.execute(
            "CREATE TRIGGER reject_terminal_receipt BEFORE INSERT ON "
            "structure_generation_drift_terminal_receipts BEGIN SELECT "
            "RAISE(ABORT,'injected-terminal-receipt-failure'); END"
        )
    comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)
    with pytest.raises(sqlite3.IntegrityError, match="injected-terminal-receipt-failure"):
        _run_drift_to_terminal(store, comparison_id)
    with sqlite3.connect(store.db_path) as con:
        phase, reason = con.execute(
            "SELECT phase,terminal_reason FROM structure_generation_drift_progress "
            "WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone()
    assert phase == "fresh-group-truth"
    assert reason is None


def test_drift_pointer_race_fails_before_next_checkpoint(tmp_path: Path) -> None:
    store = _drift_store(tmp_path)
    comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "UPDATE current_structure_generation SET validation_hash=? WHERE id=1",
            ("b" * 64,),
        )
    with pytest.raises(ValueError, match="structure-drift-current-identity-invalid"):
        store.advance_structure_drift_comparison_chunk(
            comparison_id, max_rows=1, now_ms=3_001
        )
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT phase,checkpoint_at_ms FROM structure_generation_drift_progress "
            "WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone() == ("source-events", 3_000)
    status = store.structure_generation_drift_status()
    assert status["authorized"] is False
    assert status["authorization_mode"] == "none"


@pytest.mark.asyncio
async def test_actual_drift_child_parser_resumes_committed_chunk(
    tmp_path: Path,
) -> None:
    from polyarb.daemon.scheduler import run_structure_drift_in_subprocess

    store = _drift_store(tmp_path)
    first = await run_structure_drift_in_subprocess(
        db_path=store.db_path,
        max_rows=1,
        max_chunks=1,
        max_elapsed_s=5.0,
        timeout_s=10.0,
    )
    second = await run_structure_drift_in_subprocess(
        db_path=store.db_path,
        max_rows=1,
        max_chunks=1,
        max_elapsed_s=5.0,
        timeout_s=10.0,
    )

    assert first.chunks_processed == 1
    assert second.chunks_processed == 1
    with sqlite3.connect(store.db_path) as con:
        phase, counts_json = con.execute(
            "SELECT phase,class_counts_json FROM structure_generation_drift_progress"
        ).fetchone()
    assert phase == "source-events"
    assert json.loads(counts_json)["phase_row_count"] == 2


def test_source_event_phase_adapts_global_500_row_budget(tmp_path: Path) -> None:
    dense_rows = []
    for ordinal in range(1, 101):
        event_id = f"dense-event-{ordinal:04d}"
        member_ids = [f"dense-market-{ordinal:04d}-{index:03d}" for index in range(50)]
        dense_rows.append(
            (
                ordinal,
                event_id,
                {
                    "id": event_id,
                    "active": True,
                    "closed": False,
                    "negRisk": True,
                    "enableNegRisk": True,
                    "negRiskMarketID": f"dense-group-{ordinal:04d}",
                    "markets": [
                        {"id": market_id, "active": True, "closed": False}
                        for market_id in member_ids
                    ],
                },
                frozenset(member_ids),
            )
        )

    capped = _drift_store(tmp_path / "capped")
    chunked = _drift_store(tmp_path / "chunked")
    observed_limits: list[int] = []

    def observed_fetch(**kwargs):
        observed_limits.append(int(kwargs["limit"]))
        after = kwargs["after_event_id"]
        eligible = [row for row in dense_rows if after is None or row[1] > after]
        candidates = eligible[: int(kwargs["limit"])]
        workloads = [
            (
                len(json.dumps(row[2]).encode()),
                len(row[2]["markets"]),
                len(row[3]),
            )
            for row in candidates
        ]
        prefix = sqlite_store_module._structure_drift_event_prefix_size(workloads)
        return candidates[:prefix]

    capped.fetch_structure_drift_event_source_chunk = observed_fetch  # type: ignore[method-assign]
    chunked.fetch_structure_drift_event_source_chunk = observed_fetch  # type: ignore[method-assign]
    capped_id = capped.initialize_structure_drift_comparison(now_ms=3_000)
    chunked_id = chunked.initialize_structure_drift_comparison(now_ms=3_000)
    started = time.monotonic()
    chunk = capped.advance_structure_drift_comparison_chunk(
        capped_id,
        max_rows=500,
        now_ms=3_001,
    )
    elapsed_s = time.monotonic() - started
    for index in range(5):
        chunked.advance_structure_drift_comparison_chunk(
            chunked_id,
            max_rows=2,
            now_ms=3_001 + index,
        )

    assert observed_limits[0] == 100
    assert chunk.rows_processed == 10
    assert elapsed_s < 15.0
    with sqlite3.connect(capped.db_path) as capped_con, sqlite3.connect(
        chunked.db_path
    ) as chunked_con:
        query = (
            "SELECT row_cursor_json,digest_state_json,class_counts_json,"
            "class_digests_json FROM structure_generation_drift_progress"
        )
        assert capped_con.execute(query).fetchone() == chunked_con.execute(
            query
        ).fetchone()


def test_source_event_workload_prefix_bounds_normal_and_rejects_oversized() -> None:
    select = sqlite_store_module._structure_drift_event_prefix_size

    assert select([(12_000, 23, 23)] * 100) == 21
    assert select([(300_000, 1, 1), (300_000, 1, 1)]) == 1
    with pytest.raises(
        ValueError, match="structure-drift-source-event-workload-oversized"
    ):
        select([(2_000_000, 50_000, 50_000), (1, 1, 1)])


def test_source_event_fetch_selects_metadata_before_payload_materialization(
    tmp_path: Path,
) -> None:
    store = _drift_store(tmp_path)
    statements: list[str] = []

    rows = store.fetch_structure_drift_event_source_chunk(
        publication_id="publication-2",
        generation_snapshot_id=2,
        after_event_id=None,
        limit=100,
        trace_callback=statements.append,
    )

    metadata_index = next(
        index
        for index, statement in enumerate(statements)
        if "length(CAST(payload_json AS BLOB))" in statement
    )
    payload_index = next(
        index
        for index, statement in enumerate(statements)
        if "SELECT event_id,payload_json" in statement
    )
    assert metadata_index < payload_index
    assert "SELECT COALESCE(source_ordinal,rowid),event_id,payload_json" not in statements[
        metadata_index
    ]
    assert len(rows) == 3


@pytest.mark.asyncio
async def test_actual_drift_child_defers_on_real_sqlite_writer_contention(
    tmp_path: Path,
) -> None:
    from polyarb.daemon.scheduler import run_structure_drift_in_subprocess

    store = _drift_store(tmp_path)
    blocker = sqlite3.connect(store.db_path, isolation_level=None)
    blocker.execute("BEGIN IMMEDIATE")
    try:
        checkpoint = await run_structure_drift_in_subprocess(
            db_path=store.db_path,
            max_rows=1,
            max_chunks=100,
            max_elapsed_s=45.0,
            timeout_s=10.0,
        )
    finally:
        blocker.execute("ROLLBACK")
        blocker.close()

    assert checkpoint.deferred is True
    assert checkpoint.defer_reason == "writer-busy"
    assert checkpoint.chunks_processed == 0
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT COUNT(*) FROM structure_generation_drift_progress"
        ).fetchone() == (0,)


@pytest.mark.asyncio
async def test_scheduler_records_actual_drift_child_checkpoint(tmp_path: Path) -> None:
    from polyarb.daemon.scheduler import SnapshotScheduler

    store = _drift_store(tmp_path)
    settings = SimpleNamespace(
        db_path=store.db_path,
        scheduler_interval_s=3600,
        structure_generation_drift_compare_enabled=True,
        structure_generation_drift_max_rows=1,
        structure_generation_drift_max_chunks_per_tick=1,
        structure_generation_drift_slice_s=5.0,
    )
    scheduler = SnapshotScheduler(
        settings=settings,
        sqlite_store=store,
        producer_lock=asyncio.Lock(),
    )

    assert await scheduler._maybe_advance_structure_drift(queued_at_ms=1_000) is True

    attempt = store.get_latest_structure_drift_attempt()
    assert attempt is not None
    assert attempt["outcome"] == "checkpointed"
    assert attempt["chunks_processed"] == 1
    assert attempt["rows_processed"] == 1
    assert attempt["stderr_safe_marker"].startswith("structure-drift stage=")


@pytest.mark.asyncio
async def test_scheduler_never_spawns_unledgered_child_when_attempt_db_is_busy(
    tmp_path: Path,
) -> None:
    from polyarb.daemon.scheduler import SnapshotScheduler

    seeded = _drift_store(tmp_path)
    store = SQLiteStore(seeded.db_path, writer_timeout_s=0.01)
    settings = SimpleNamespace(
        db_path=store.db_path,
        scheduler_interval_s=3600,
        structure_generation_drift_compare_enabled=True,
        structure_generation_drift_max_rows=1,
        structure_generation_drift_max_chunks_per_tick=1,
        structure_generation_drift_slice_s=5.0,
    )
    scheduler = SnapshotScheduler(
        settings=settings,
        sqlite_store=store,
        producer_lock=asyncio.Lock(),
    )
    blocker = sqlite3.connect(store.db_path, isolation_level=None)
    blocker.execute("BEGIN IMMEDIATE")
    try:
        assert (
            await scheduler._maybe_advance_structure_drift(queued_at_ms=1_000) is True
        )
    finally:
        blocker.execute("ROLLBACK")
        blocker.close()

    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT COUNT(*) FROM structure_drift_attempts"
        ).fetchone() == (0,)
        assert con.execute(
            "SELECT COUNT(*) FROM structure_generation_drift_progress"
        ).fetchone() == (0,)


@pytest.mark.asyncio
async def test_production_shaped_845_848_children_resume_to_sealed(
    tmp_path: Path,
) -> None:
    from polyarb.daemon.scheduler import run_structure_drift_in_subprocess

    store = _drift_store(tmp_path)
    _reshape_as_production_845_848(store)
    with sqlite3.connect(store.db_path) as con:
        immutable_before = con.execute(
            "SELECT snapshot_id,publication_id,validation_hash,"
            "comparison_receipt_digest FROM current_structure_generation WHERE id=1"
        ).fetchone()
    process_count = 0
    total_chunks = 0
    while process_count < 20:
        checkpoint = await run_structure_drift_in_subprocess(
            db_path=store.db_path,
            max_rows=1,
            max_chunks=3,
            max_elapsed_s=5.0,
            timeout_s=10.0,
        )
        process_count += 1
        total_chunks += checkpoint.chunks_processed
        if checkpoint.ready:
            break
    else:
        pytest.fail("production-shaped drift children did not seal")

    status = store.structure_generation_drift_status()
    assert process_count > 1
    assert total_chunks > 3
    assert status["authorized"] is True
    assert status["authorization_mode"] == "drift-safe-sealed"
    assert status["legacy_snapshot_id"] == 845
    assert status["generation_snapshot_id"] == 848
    assert status["window_id"] == "window-97b"
    with sqlite3.connect(store.db_path) as con:
        immutable_after = con.execute(
            "SELECT snapshot_id,publication_id,validation_hash,"
            "comparison_receipt_digest FROM current_structure_generation WHERE id=1"
        ).fetchone()
        receipt_count = con.execute(
            "SELECT COUNT(*) FROM structure_generation_drift_receipts"
        ).fetchone()[0]
    assert immutable_after == immutable_before
    assert receipt_count == 1
