"""SQLite authority for certified groups and atomic all-leg quote batches."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from polyarb.perception.models import (
    GroupLeg,
    GroupQuoteBatch,
    GroupQuoteLeg,
    GroupRevision,
)
from polyarb.storage.schemas import DDL

_BUSY_TIMEOUT_MS = 5_000
_GROUP_STATUSES = {"discovered", "certified", "stale", "invalidated", "closed"}


class OpportunityPerceptionStore:
    """Transactional opportunity-first perception read model."""

    def __init__(self, db_path: Path) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)

    def init_schema(self) -> None:
        con = self._connect()
        try:
            con.executescript(DDL)
        finally:
            con.close()

    def publish_group_revision(self, revision: GroupRevision) -> GroupRevision:
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            if (
                GroupRevision.membership_digest(revision.legs)
                != revision.membership_hash
            ):
                raise ValueError("membership-hash-mismatch")
            if revision.status == "certified":
                validated = GroupRevision.certified(
                    group_id=revision.group_id,
                    event_id=revision.event_id,
                    revision=revision.revision,
                    started_at_ms=revision.started_at_ms,
                    observed_at_ms=revision.observed_at_ms,
                    source_cursor=revision.source_cursor,
                    legs=revision.legs,
                )
                if revision != validated:
                    raise ValueError("certified-group-invalid")
            current_row = self._current_group_row(con, revision.group_id)
            if current_row is not None and revision.revision <= current_row["revision"]:
                raise ValueError("group-revision-not-monotonic")

            con.execute(
                "INSERT INTO neg_risk_group_revisions("
                "group_id,event_id,revision,membership_hash,started_at_ms,"
                "observed_at_ms,source_cursor,status,legs_json"
                ") VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    revision.group_id,
                    revision.event_id,
                    revision.revision,
                    revision.membership_hash,
                    revision.started_at_ms,
                    revision.observed_at_ms,
                    revision.source_cursor,
                    revision.status,
                    self._group_legs_json(revision.legs),
                ),
            )
            if (
                current_row is not None
                and current_row["membership_hash"] != revision.membership_hash
            ):
                con.execute(
                    "UPDATE neg_risk_group_quote_batches "
                    "SET status='superseded' "
                    "WHERE group_id=? AND status='complete'",
                    (revision.group_id,),
                )
            con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()
        return revision

    def publish_quote_batch(self, batch: GroupQuoteBatch) -> GroupQuoteBatch:
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            validated = GroupQuoteBatch.complete(
                group_id=batch.group_id,
                membership_hash=batch.membership_hash,
                quote_batch_id=batch.quote_batch_id,
                started_at_ms=batch.started_at_ms,
                quoted_at_ms=batch.quoted_at_ms,
                legs=batch.legs,
            )
            if batch != validated:
                raise ValueError("quote-batch-not-complete")
            current_row = self._current_group_row(con, batch.group_id)
            if current_row is None:
                membership_owner = con.execute(
                    "SELECT group_id FROM neg_risk_group_revisions "
                    "WHERE membership_hash=? AND status='certified' "
                    "ORDER BY revision DESC LIMIT 1",
                    (batch.membership_hash,),
                ).fetchone()
                if membership_owner is not None:
                    raise ValueError("group-identity-mismatch")
                raise ValueError("certified-group-not-found")
            if current_row["status"] != "certified":
                raise ValueError("certified-group-not-found")

            current = self._validated_group_from_row(current_row)
            if current is None:
                raise ValueError("certified-group-invalid")
            if batch.group_id != current.group_id:
                raise ValueError("group-identity-mismatch")
            if batch.membership_hash != current.membership_hash:
                raise ValueError("membership-hash-mismatch")
            if tuple(leg.yes_token_id for leg in batch.legs) != tuple(
                leg.yes_token_id for leg in current.legs
            ):
                raise ValueError("quote-leg-identity-mismatch")
            if any(
                leg.membership_hash != current.membership_hash for leg in batch.legs
            ):
                raise ValueError("membership-hash-mismatch")
            con.execute(
                "INSERT INTO neg_risk_group_quote_batches("
                "id,group_id,group_revision,membership_hash,started_at_ms,"
                "quoted_at_ms,status,failure_reason,legs_json"
                ") VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    batch.quote_batch_id,
                    batch.group_id,
                    current.revision,
                    batch.membership_hash,
                    batch.started_at_ms,
                    batch.quoted_at_ms,
                    batch.status,
                    batch.failure_reason,
                    self._quote_legs_json(batch.legs),
                ),
            )
            con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()
        return batch

    def current_group(self, group_id: str) -> GroupRevision | None:
        con = self._connect()
        try:
            row = self._current_group_row(con, group_id)
            return None if row is None else self._validated_group_from_row(row)
        finally:
            con.close()

    def current_quote_batch(
        self,
        group_id: str,
        now_ms: int,
        max_age_ms: int,
    ) -> GroupQuoteBatch | None:
        con = self._connect()
        try:
            row = self._current_quote_row(con, group_id, now_ms, max_age_ms)
            if row is None:
                return None
            group = self._validated_group_from_row(row, prefix="group_")
            if group is None or group.status != "certified":
                return None
            return self._validated_quote_from_row(row, group, prefix="quote_")
        finally:
            con.close()

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(
            self._db_path,
            isolation_level=None,
            timeout=_BUSY_TIMEOUT_MS / 1_000,
        )
        con.row_factory = sqlite3.Row
        con.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}")
        return con

    @staticmethod
    def _current_group_row(
        con: sqlite3.Connection, group_id: str
    ) -> sqlite3.Row | None:
        return con.execute(
            "SELECT * FROM neg_risk_group_revisions "
            "WHERE group_id=? ORDER BY revision DESC LIMIT 1",
            (group_id,),
        ).fetchone()

    @staticmethod
    def _current_quote_row(
        con: sqlite3.Connection,
        group_id: str,
        now_ms: int,
        max_age_ms: int,
    ) -> sqlite3.Row | None:
        return con.execute(
            "WITH current_group AS ("
            "SELECT group_id,event_id,revision,membership_hash,started_at_ms,"
            "observed_at_ms,source_cursor,status,legs_json "
            "FROM neg_risk_group_revisions "
            "WHERE group_id=? ORDER BY revision DESC LIMIT 1"
            ") "
            "SELECT "
            "g.group_id AS group_group_id,"
            "g.event_id AS group_event_id,"
            "g.revision AS group_revision,"
            "g.membership_hash AS group_membership_hash,"
            "g.started_at_ms AS group_started_at_ms,"
            "g.observed_at_ms AS group_observed_at_ms,"
            "g.source_cursor AS group_source_cursor,"
            "g.status AS group_status,"
            "g.legs_json AS group_legs_json,"
            "q.id AS quote_id,"
            "q.group_id AS quote_group_id,"
            "q.membership_hash AS quote_membership_hash,"
            "q.started_at_ms AS quote_started_at_ms,"
            "q.quoted_at_ms AS quote_quoted_at_ms,"
            "q.status AS quote_status,"
            "q.failure_reason AS quote_failure_reason,"
            "q.legs_json AS quote_legs_json "
            "FROM current_group g "
            "JOIN neg_risk_group_quote_batches q "
            "ON q.group_id=g.group_id AND q.membership_hash=g.membership_hash "
            "WHERE g.status='certified' AND q.status='complete' "
            "AND q.quoted_at_ms<=? AND q.quoted_at_ms>=? "
            "ORDER BY q.quoted_at_ms DESC,q.id DESC LIMIT 1",
            (group_id, now_ms, now_ms - max_age_ms),
        ).fetchone()

    @staticmethod
    def _group_legs_json(legs: tuple[GroupLeg, ...]) -> str:
        return json.dumps(
            [
                [leg.market_id, leg.condition_id, leg.yes_token_id, leg.title]
                for leg in legs
            ],
            ensure_ascii=False,
            separators=(",", ":"),
        )

    @staticmethod
    def _quote_legs_json(legs: tuple[GroupQuoteLeg, ...]) -> str:
        return json.dumps(
            [
                [
                    leg.yes_token_id,
                    leg.membership_hash,
                    leg.best_ask_price,
                    leg.best_ask_size,
                    leg.terminal_state,
                ]
                for leg in legs
            ],
            ensure_ascii=False,
            separators=(",", ":"),
        )

    @staticmethod
    def _group_from_row(
        row: sqlite3.Row, *, prefix: str = ""
    ) -> GroupRevision:
        return GroupRevision(
            group_id=row[f"{prefix}group_id"],
            event_id=row[f"{prefix}event_id"],
            revision=row[f"{prefix}revision"],
            membership_hash=row[f"{prefix}membership_hash"],
            started_at_ms=row[f"{prefix}started_at_ms"],
            observed_at_ms=row[f"{prefix}observed_at_ms"],
            source_cursor=row[f"{prefix}source_cursor"],
            status=row[f"{prefix}status"],
            legs=tuple(
                GroupLeg(*leg) for leg in json.loads(row[f"{prefix}legs_json"])
            ),
        )

    @staticmethod
    def _quote_batch_from_row(
        row: sqlite3.Row, *, prefix: str = ""
    ) -> GroupQuoteBatch:
        return GroupQuoteBatch(
            group_id=row[f"{prefix}group_id"],
            membership_hash=row[f"{prefix}membership_hash"],
            quote_batch_id=row[f"{prefix}id"],
            started_at_ms=row[f"{prefix}started_at_ms"],
            quoted_at_ms=row[f"{prefix}quoted_at_ms"],
            status=row[f"{prefix}status"],
            failure_reason=row[f"{prefix}failure_reason"],
            legs=tuple(
                GroupQuoteLeg(*leg)
                for leg in json.loads(row[f"{prefix}legs_json"])
            ),
        )

    @classmethod
    def _validated_group_from_row(
        cls,
        row: sqlite3.Row,
        *,
        prefix: str = "",
    ) -> GroupRevision | None:
        try:
            group = cls._group_from_row(row, prefix=prefix)
            if group.status not in _GROUP_STATUSES:
                return None
            if (
                GroupRevision.membership_digest(group.legs)
                != group.membership_hash
            ):
                return None
            if group.started_at_ms > group.observed_at_ms:
                return None
            if group.status == "certified":
                validated = GroupRevision.certified(
                    group_id=group.group_id,
                    event_id=group.event_id,
                    revision=group.revision,
                    started_at_ms=group.started_at_ms,
                    observed_at_ms=group.observed_at_ms,
                    source_cursor=group.source_cursor,
                    legs=group.legs,
                )
                if group != validated:
                    return None
            return group
        except (IndexError, KeyError, TypeError, ValueError):
            return None

    @classmethod
    def _validated_quote_from_row(
        cls,
        row: sqlite3.Row,
        group: GroupRevision,
        *,
        prefix: str = "",
    ) -> GroupQuoteBatch | None:
        try:
            quote = cls._quote_batch_from_row(row, prefix=prefix)
            validated = GroupQuoteBatch.complete(
                group_id=quote.group_id,
                membership_hash=quote.membership_hash,
                quote_batch_id=quote.quote_batch_id,
                started_at_ms=quote.started_at_ms,
                quoted_at_ms=quote.quoted_at_ms,
                legs=quote.legs,
            )
            if quote != validated:
                return None
            if quote.group_id != group.group_id:
                return None
            if quote.membership_hash != group.membership_hash:
                return None
            if tuple(leg.yes_token_id for leg in quote.legs) != tuple(
                leg.yes_token_id for leg in group.legs
            ):
                return None
            return quote
        except (IndexError, KeyError, TypeError, ValueError):
            return None
