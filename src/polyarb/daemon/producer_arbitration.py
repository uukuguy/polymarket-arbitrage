"""Durable mutual exclusion for Structure and supervised Quote producers.

The two producers run in different processes in production.  SQLite's
``BEGIN IMMEDIATE`` is the arbitration boundary; an expired lease is reclaimed
atomically by the next contender, so a killed child cannot permanently starve
the other producer.
"""

from __future__ import annotations

import sqlite3
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

ProducerOwner = Literal["quote", "structure"]
_RESOURCE = "market-write-producer"
_STRUCTURE_YIELD_TO_QUOTE_MS = 5_000


@dataclass(frozen=True)
class ProducerLease:
    owner: ProducerOwner
    lease_id: str
    acquired_at_ms: int
    expires_at_ms: int


@dataclass(frozen=True)
class ProducerReceipt:
    owner: ProducerOwner
    lease_id: str
    action: Literal["acquired", "released", "expired"]
    observed_at_ms: int
    expires_at_ms: int


class ProducerArbitrator:
    """Acquire and release the one production write slot.

    Callers must initialize the normal application SQLite schema before using
    this class.  The class deliberately has no process-local state.
    """

    def __init__(
        self,
        db_path: Path,
        *,
        now_ms: Callable[[], int] | None = None,
        writer_timeout_s: float = 5.0,
    ) -> None:
        self._db_path = Path(db_path)
        self._now_ms = now_ms or (lambda: int(time.time() * 1_000))
        self._writer_timeout_s = writer_timeout_s

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(
            self._db_path,
            isolation_level=None,
            timeout=self._writer_timeout_s,
        )
        con.execute("PRAGMA foreign_keys=ON")
        return con

    @staticmethod
    def _lease_from_row(row: tuple[object, ...]) -> ProducerLease:
        return ProducerLease(
            owner=row[0],  # type: ignore[arg-type]
            lease_id=str(row[1]),
            acquired_at_ms=int(row[2]),
            expires_at_ms=int(row[3]),
        )

    def acquire(self, *, owner: ProducerOwner, lease_s: float) -> ProducerLease | None:
        if owner not in ("quote", "structure") or lease_s <= 0:
            raise ValueError("invalid-producer-lease-request")
        now_ms = self._now_ms()
        expires_at_ms = now_ms + max(1, int(lease_s * 1_000))
        lease = ProducerLease(owner, uuid.uuid4().hex, now_ms, expires_at_ms)
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                "SELECT owner,lease_id,acquired_at_ms,expires_at_ms "
                "FROM producer_arbitration_leases WHERE resource=?",
                (_RESOURCE,),
            ).fetchone()
            if row is not None:
                current = self._lease_from_row(row)
                if current.expires_at_ms > now_ms:
                    con.execute("ROLLBACK")
                    return None
                con.execute(
                    "INSERT INTO producer_arbitration_receipts("
                    "owner,lease_id,action,observed_at_ms,expires_at_ms) VALUES (?,?,?,?,?)",
                    (
                        current.owner,
                        current.lease_id,
                        "expired",
                        now_ms,
                        current.expires_at_ms,
                    ),
                )
                con.execute(
                    "DELETE FROM producer_arbitration_leases WHERE resource=?",
                    (_RESOURCE,),
                )
            if owner == "structure":
                # A checkpointed Structure child can finish in milliseconds.
                # Give a pending supervised Quote two bounded retry turns
                # before Structure tries to monopolize the newly-free slot.
                last_structure_release = con.execute(
                    "SELECT observed_at_ms FROM producer_arbitration_receipts "
                    "WHERE owner='structure' AND action='released' "
                    "ORDER BY id DESC LIMIT 1"
                ).fetchone()
                if (
                    last_structure_release is not None
                    and now_ms - int(last_structure_release[0])
                    < _STRUCTURE_YIELD_TO_QUOTE_MS
                ):
                    con.execute("ROLLBACK")
                    return None
            con.execute(
                "INSERT INTO producer_arbitration_leases("
                "resource,owner,lease_id,acquired_at_ms,expires_at_ms,updated_at_ms) "
                "VALUES (?,?,?,?,?,?)",
                (_RESOURCE, owner, lease.lease_id, now_ms, expires_at_ms, now_ms),
            )
            con.execute(
                "INSERT INTO producer_arbitration_receipts("
                "owner,lease_id,action,observed_at_ms,expires_at_ms) VALUES (?,?,?,?,?)",
                (owner, lease.lease_id, "acquired", now_ms, expires_at_ms),
            )
            con.execute("COMMIT")
            return lease
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def release(self, lease: ProducerLease) -> bool:
        now_ms = self._now_ms()
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            deleted = con.execute(
                "DELETE FROM producer_arbitration_leases "
                "WHERE resource=? AND owner=? AND lease_id=?",
                (_RESOURCE, lease.owner, lease.lease_id),
            ).rowcount
            if not deleted:
                con.execute("ROLLBACK")
                return False
            con.execute(
                "INSERT INTO producer_arbitration_receipts("
                "owner,lease_id,action,observed_at_ms,expires_at_ms) VALUES (?,?,?,?,?)",
                (lease.owner, lease.lease_id, "released", now_ms, lease.expires_at_ms),
            )
            con.execute("COMMIT")
            return True
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def current(self) -> ProducerLease | None:
        con = self._connect()
        try:
            row = con.execute(
                "SELECT owner,lease_id,acquired_at_ms,expires_at_ms "
                "FROM producer_arbitration_leases WHERE resource=?",
                (_RESOURCE,),
            ).fetchone()
            return None if row is None else self._lease_from_row(row)
        finally:
            con.close()

    def receipts(self, *, limit: int = 20) -> tuple[ProducerReceipt, ...]:
        if limit < 1 or limit > 200:
            raise ValueError("invalid-producer-receipt-limit")
        con = self._connect()
        try:
            rows = con.execute(
                "SELECT owner,lease_id,action,observed_at_ms,expires_at_ms "
                "FROM producer_arbitration_receipts ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return tuple(
                ProducerReceipt(
                    owner=row[0],  # type: ignore[arg-type]
                    lease_id=str(row[1]),
                    action=row[2],  # type: ignore[arg-type]
                    observed_at_ms=int(row[3]),
                    expires_at_ms=int(row[4]),
                )
                for row in rows
            )
        finally:
            con.close()
