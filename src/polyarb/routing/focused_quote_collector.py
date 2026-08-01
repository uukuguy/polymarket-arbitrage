"""Read-only focused top-of-book checks for active neg-risk opportunities."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal, Protocol

from polyarb.perception.models import (
    GroupQuoteBatch,
    GroupQuoteLeg,
    GroupRevision,
)
from polyarb.routing.neg_risk_quote_collector import (
    BooksReader,
    QuoteCollectionIntegrityError,
    _build_terminal_quotes,
)
from polyarb.routing.neg_risk_quote_store import UniverseLeg
from polyarb.routing.opportunity_scanner import OpportunityLeg
from polyarb.storage.sqlite_store import (
    StructureGenerationReadError,
    structure_read_transaction,
)

FocusedStatus = Literal["observe", "no-edge", "unavailable", "invalidated"]


@dataclass(frozen=True)
class StructureLeg:
    market_id: str
    condition_id: str
    slug: str
    yes_token_id: str


@dataclass(frozen=True)
class StructureGroup:
    structure_revision: int
    event_id: str
    group_id: str
    membership_hash: str
    legs: tuple[StructureLeg, ...]

    @property
    def yes_token_ids(self) -> tuple[str, ...]:
        return tuple(leg.yes_token_id for leg in self.legs)


@dataclass(frozen=True)
class ActiveOpportunity:
    """The durable all-leg identity of an already-open observer master."""

    id: str
    event_id: str
    group_id: str
    membership_hash: str
    structure_revision: int
    quote_run_id: int
    legs: tuple[StructureLeg, ...]


@dataclass(frozen=True)
class FocusedObservation:
    opportunity_id: str
    status: FocusedStatus
    reason: str | None
    bundle_cost: float | None
    gross_edge_bps: float | None
    max_bundle_size: float | None
    legs: tuple[OpportunityLeg, ...]
    structure_revision: int
    quote_run_id: int
    observed_at_ms: int

    @classmethod
    def invalidated(
        cls,
        opportunity: ActiveOpportunity,
        *,
        reason: str,
        observed_at_ms: int,
    ) -> FocusedObservation:
        return cls(
            opportunity_id=opportunity.id,
            status="invalidated",
            reason=reason,
            bundle_cost=None,
            gross_edge_bps=None,
            max_bundle_size=None,
            legs=(),
            structure_revision=opportunity.structure_revision,
            quote_run_id=opportunity.quote_run_id,
            observed_at_ms=observed_at_ms,
        )


class MembershipReader(Protocol):
    def current_group(self, event_id: str, group_id: str) -> StructureGroup | None: ...


class SqliteStructureMembershipReader:
    """Read exactly the newest published Structure revision, never Quote history."""

    def __init__(
        self,
        db_path: Path | str,
        *,
        structure_generation_read_mode: str = "legacy",
    ) -> None:
        self._db_path = Path(db_path)
        self._structure_generation_read_mode = structure_generation_read_mode

    def current_group(self, event_id: str, group_id: str) -> StructureGroup | None:
        try:
            with structure_read_transaction(
                self._db_path,
                mode=self._structure_generation_read_mode,
            ) as read:
                snapshot_id = read.snapshot_id
                truth = read.connection.execute(
                "SELECT neg_risk_type,expected_member_count,active_named_count,"
                f"membership_hash,quality FROM {read.table('group_truth')} "
                "WHERE snapshot_id=? AND event_id=? AND neg_risk_market_id=?",
                (snapshot_id, event_id, group_id),
                ).fetchone()
                if (
                    truth is None
                    or truth[0] != "standard"
                    or truth[4] != "complete-supported"
                    or int(truth[1]) != int(truth[2])
                    or not isinstance(truth[3], str)
                    or not truth[3].strip()
                ):
                    return None
                membership_rows = read.connection.execute(
                    "SELECT market_id,member_kind,active,closed "
                    f"FROM {read.table('memberships')} WHERE snapshot_id=? "
                    "AND event_id=? AND neg_risk_market_id=? ORDER BY market_id",
                    (snapshot_id, event_id, group_id),
                ).fetchall()
                market_rows = read.connection.execute(
                "SELECT market_id,condition_id,slug,yes_token_id,active,closed,incomplete,event_id "
                f"FROM {read.table('markets')} WHERE snapshot_id=? AND event_id=? "
                "AND neg_risk_market_id=? "
                "ORDER BY market_id",
                (snapshot_id, event_id, group_id),
                ).fetchall()
        except StructureGenerationReadError:
            return None
        expected_count = int(truth[1])
        membership_ids = {str(row[0]) for row in membership_rows}
        market_ids = {str(row[0]) for row in market_rows}
        if (
            len(membership_rows) != expected_count
            or len(market_rows) != expected_count
            or membership_ids != market_ids
            or any(
                row[1] != "named" or int(row[2]) != 1 or int(row[3]) != 0
                for row in membership_rows
            )
            or any(
                row[7] != event_id
                or int(row[4]) != 1
                or int(row[5]) != 0
                or int(row[6]) != 0
                or not isinstance(row[3], str)
                or not row[3].strip()
                for row in market_rows
            )
        ):
            return None
        legs = tuple(
            StructureLeg(
                market_id=str(row[0]),
                condition_id=str(row[1]),
                slug=str(row[2]) if row[2] is not None else "",
                yes_token_id=str(row[3]),
            )
            for row in market_rows
        )
        if len({leg.yes_token_id for leg in legs}) != len(legs):
            return None
        return StructureGroup(
            structure_revision=snapshot_id,
            event_id=event_id,
            group_id=group_id,
            membership_hash=str(truth[3]),
            legs=legs,
        )


def build_complete_group_quote_batch(
    revision: GroupRevision,
    books: Sequence[Any],
    *,
    started_at_ms: int,
    quoted_at_ms: int,
    quote_batch_id: str | None = None,
) -> GroupQuoteBatch:
    """Normalize one ordered group's top books into an atomic all-leg batch."""
    universe_legs = tuple(
        UniverseLeg(
            neg_risk_market_id=revision.group_id,
            market_id=leg.market_id,
            condition_id=leg.condition_id,
            slug=leg.title,
            yes_token_id=leg.yes_token_id,
            event_id=revision.event_id,
            membership_hash=revision.membership_hash,
        )
        for leg in revision.legs
    )
    token_ids = [leg.yes_token_id for leg in revision.legs]
    _, quotes = _build_terminal_quotes(books, token_ids, universe_legs)
    if any(
        quote.terminal_state != "executable"
        or quote.best_ask_price is None
        or quote.best_ask_size is None
        for quote in quotes
    ):
        raise QuoteCollectionIntegrityError()
    return GroupQuoteBatch.complete(
        group_id=revision.group_id,
        membership_hash=revision.membership_hash,
        quote_batch_id=quote_batch_id or uuid.uuid4().hex,
        started_at_ms=started_at_ms,
        quoted_at_ms=quoted_at_ms,
        legs=tuple(
            GroupQuoteLeg(
                yes_token_id=quote.yes_token_id,
                membership_hash=revision.membership_hash,
                best_ask_price=float(quote.best_ask_price),
                best_ask_size=float(quote.best_ask_size),
                terminal_state=quote.terminal_state,
            )
            for quote in quotes
        ),
    )


async def collect_focused_observation(
    opportunity: ActiveOpportunity,
    *,
    reader: BooksReader,
    membership_reader: MembershipReader,
    now_ms: Callable[[], int],
    min_edge_bps: float = 100.0,
) -> FocusedObservation:
    """Revalidate Structure before requesting only the active master's top books."""
    current = await asyncio.to_thread(
        membership_reader.current_group,
        opportunity.event_id,
        opportunity.group_id,
    )
    observed_at_ms = now_ms()
    if (
        current is None
        or current.membership_hash != opportunity.membership_hash
        or not _same_durable_legs(opportunity.legs, current.legs)
    ):
        return FocusedObservation.invalidated(
            opportunity,
            reason="structure-membership-changed",
            observed_at_ms=observed_at_ms,
        )
    books = await reader.get_books(list(current.yes_token_ids), projection="top")
    return build_focused_observation(
        opportunity,
        current,
        books,
        observed_at_ms=observed_at_ms,
        min_edge_bps=min_edge_bps,
    )


def build_focused_observation(
    opportunity: ActiveOpportunity,
    current: StructureGroup,
    books: Sequence[Any],
    *,
    observed_at_ms: int,
    min_edge_bps: float = 100.0,
) -> FocusedObservation:
    """Apply the existing terminal top-of-book rules without creating a Quote run."""
    universe_legs = tuple(
        UniverseLeg(
            neg_risk_market_id=current.group_id,
            market_id=leg.market_id,
            condition_id=leg.condition_id,
            slug=leg.slug,
            yes_token_id=leg.yes_token_id,
            event_id=current.event_id,
            membership_hash=current.membership_hash,
        )
        for leg in current.legs
    )
    try:
        _, quotes = _build_terminal_quotes(books, list(current.yes_token_ids), universe_legs)
    except QuoteCollectionIntegrityError:
        return _unavailable(opportunity, current, observed_at_ms, "incomplete-quotes")
    if any(quote.terminal_state != "executable" for quote in quotes):
        return _unavailable(opportunity, current, observed_at_ms, "incomplete-quotes")
    legs = tuple(
        OpportunityLeg(
            market_id=quote.market_id,
            condition_id=quote.condition_id,
            slug=quote.slug or "",
            yes_token_id=quote.yes_token_id,
            ask_price=float(quote.best_ask_price),
            ask_size=float(quote.best_ask_size),
        )
        for quote in quotes
    )
    bundle_cost = sum((Decimal(str(leg.ask_price)) for leg in legs), Decimal(0))
    gross_edge_bps = (Decimal(1) - bundle_cost) * Decimal(10_000)
    return FocusedObservation(
        opportunity_id=opportunity.id,
        status="observe" if gross_edge_bps >= Decimal(str(min_edge_bps)) else "no-edge",
        reason=None,
        bundle_cost=float(bundle_cost),
        gross_edge_bps=float(gross_edge_bps),
        max_bundle_size=min(leg.ask_size for leg in legs),
        legs=legs,
        structure_revision=current.structure_revision,
        quote_run_id=opportunity.quote_run_id,
        observed_at_ms=observed_at_ms,
    )


def _unavailable(
    opportunity: ActiveOpportunity,
    current: StructureGroup,
    observed_at_ms: int,
    reason: str,
) -> FocusedObservation:
    return FocusedObservation(
        opportunity_id=opportunity.id,
        status="unavailable",
        reason=reason,
        bundle_cost=None,
        gross_edge_bps=None,
        max_bundle_size=None,
        legs=(),
        structure_revision=current.structure_revision,
        quote_run_id=opportunity.quote_run_id,
        observed_at_ms=observed_at_ms,
    )


def _same_durable_legs(
    active: tuple[StructureLeg, ...],
    current: tuple[StructureLeg, ...],
) -> bool:
    return {
        (leg.market_id, leg.condition_id, leg.yes_token_id) for leg in active
    } == {
        (leg.market_id, leg.condition_id, leg.yes_token_id) for leg in current
    }
