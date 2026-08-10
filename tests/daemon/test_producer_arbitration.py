from __future__ import annotations

from dataclasses import replace

from polyarb.daemon.producer_arbitration import ProducerArbitrator
from polyarb.storage.sqlite_store import SQLiteStore


def test_cross_process_slot_is_exclusive_and_reclaimable_after_expiry(tmp_path) -> None:
    db_path = tmp_path / "state.db"
    SQLiteStore(db_path).init_schema()
    quote = ProducerArbitrator(db_path, now_ms=lambda: 1_000)
    structure = ProducerArbitrator(db_path, now_ms=lambda: 1_020)

    quote_lease = quote.acquire(owner="quote", lease_s=60)
    assert quote_lease is not None
    assert structure.acquire(owner="structure", lease_s=45) is None

    expired_structure = ProducerArbitrator(db_path, now_ms=lambda: 61_001)
    structure_lease = expired_structure.acquire(owner="structure", lease_s=45)

    assert structure_lease is not None
    assert structure_lease.owner == "structure"
    assert expired_structure.release(structure_lease) is True
    assert [item.action for item in expired_structure.receipts(limit=10)] == [
        "released",
        "acquired",
        "expired",
        "acquired",
    ]


def test_only_lease_owner_can_release_slot(tmp_path) -> None:
    db_path = tmp_path / "state.db"
    SQLiteStore(db_path).init_schema()
    arbitrator = ProducerArbitrator(db_path, now_ms=lambda: 1_000)
    lease = arbitrator.acquire(owner="quote", lease_s=60)
    assert lease is not None

    assert arbitrator.release(replace(lease, lease_id="wrong")) is False
    assert arbitrator.current() == lease
    assert arbitrator.release(lease) is True
    assert arbitrator.current() is None


def test_structure_yields_a_released_checkpoint_to_quote(tmp_path) -> None:
    db_path = tmp_path / "state.db"
    SQLiteStore(db_path).init_schema()
    now = [1_000]
    arbitrator = ProducerArbitrator(db_path, now_ms=lambda: now[0])
    structure = arbitrator.acquire(owner="structure", lease_s=45)
    assert structure is not None
    now[0] = 2_000
    assert arbitrator.release(structure) is True

    assert arbitrator.acquire(owner="structure", lease_s=45) is None
    assert arbitrator.acquire(owner="quote", lease_s=60) is not None


def test_new_supervised_quote_child_releases_only_its_orphaned_quote_lease(tmp_path) -> None:
    db_path = tmp_path / "state.db"
    SQLiteStore(db_path).init_schema()
    now = [1_000]
    arbitrator = ProducerArbitrator(db_path, now_ms=lambda: now[0])
    old_quote = arbitrator.acquire(owner="quote", lease_s=210)
    assert old_quote is not None

    now[0] = 1_001
    assert arbitrator.relinquish_orphaned_quote_lease() == old_quote
    assert arbitrator.current() is None
    assert arbitrator.acquire(owner="quote", lease_s=210) is not None

    structure = arbitrator.acquire(owner="structure", lease_s=45)
    assert structure is None  # the new Quote lease still owns the slot


def test_quote_orphan_reclaim_never_releases_a_structure_lease(tmp_path) -> None:
    db_path = tmp_path / "state.db"
    SQLiteStore(db_path).init_schema()
    arbitrator = ProducerArbitrator(db_path, now_ms=lambda: 1_000)
    structure = arbitrator.acquire(owner="structure", lease_s=45)
    assert structure is not None

    assert arbitrator.relinquish_orphaned_quote_lease() is None
    assert arbitrator.current() == structure
