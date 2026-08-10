from __future__ import annotations

import asyncio
import sqlite3
import threading
from dataclasses import dataclass

import pytest

from polyarb.perception.market_truth import SourceCoverage
from polyarb.routing.neg_risk_quote_collector import (
    _QUOTE_RUN_LEASE_RENEWAL_S,
    QuoteCollectionIntegrityError,
    QuotePersistenceTimeoutError,
    QuoteRunLeaseLostError,
    QuoteUniverseUnavailableError,
    collect_neg_risk_quotes,
)
from polyarb.routing.neg_risk_quote_store import (
    QUOTE_RUN_LEASE_MS,
    NegRiskQuoteStore,
    QuoteRunBusyError,
    UniverseLeg,
)
from polyarb.storage.sqlite_store import SQLiteStore

NOW_MS = 1_700_000_000_000
EVENT_ID = "event-a"
MEMBERSHIP_HASH = "membership-hash-a"


def test_production_quote_lease_tolerates_single_vcpu_snapshot_contention() -> None:
    assert QUOTE_RUN_LEASE_MS == 180_000
    assert _QUOTE_RUN_LEASE_RENEWAL_S == 60.0


@dataclass
class FixtureBook:
    asset_id: str
    asks: list[object]


class FakeReader:
    def __init__(self, response: object) -> None:
        self.response = response
        self.requests: list[list[str]] = []
        self.projections: list[str] = []

    async def get_books(
        self,
        token_ids: list[str],
        *,
        projection: str = "full",
    ) -> object:
        self.requests.append(token_ids)
        self.projections.append(projection)
        if isinstance(self.response, BaseException):
            raise self.response
        return self.response


class BlockingReader(FakeReader):
    def __init__(self, response: object) -> None:
        super().__init__(response)
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.cancelled = False

    async def get_books(
        self,
        token_ids: list[str],
        *,
        projection: str = "full",
    ) -> object:
        self.requests.append(token_ids)
        self.projections.append(projection)
        self.started.set()
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        return self.response


class OneTickSleeper:
    def __init__(self) -> None:
        self.first_tick_started = asyncio.Event()
        self.permit_first_tick = asyncio.Event()
        self._never = asyncio.Event()
        self._calls = 0

    async def __call__(self, _: float) -> None:
        self._calls += 1
        if self._calls == 1:
            self.first_tick_started.set()
            await self.permit_first_tick.wait()
            return
        await self._never.wait()


@pytest.fixture
def quote_db(tmp_path):
    path = tmp_path / "state.db"
    SQLiteStore(path).init_schema()
    with sqlite3.connect(path) as con:
        con.execute(
            "INSERT INTO snapshots("
            "taken_at_ms, finished_at_ms, mode, market_count,market_view_published,"
            "data_product,is_valid, parquet_path"
            ") VALUES (?, ?, 'subset', 2,1,'structure',1, 'fixture.parquet')",
            (NOW_MS - 1_000, NOW_MS - 900),
        )
        snapshot_id = int(con.execute("SELECT last_insert_rowid()").fetchone()[0])
        con.executemany(
            "INSERT INTO markets("
            "market_id, condition_id, slug, yes_token_id, active, closed, "
            "neg_risk_market_id, fetched_at_ms, snapshot_id, incomplete, event_id"
            ") VALUES (?, ?, ?, ?, 1, 0, ?, ?, ?, 0, ?)",
            [
                (
                    "market-a",
                    "condition-a",
                    "alpha",
                    "token-a",
                    "group-a",
                    NOW_MS,
                    snapshot_id,
                    EVENT_ID,
                ),
                (
                    "market-b",
                    "condition-b",
                    "beta",
                    "token-b",
                    "group-a",
                    NOW_MS,
                    snapshot_id,
                    EVENT_ID,
                ),
            ],
        )
        con.execute(
            "INSERT INTO snapshot_source_coverage("
            "snapshot_id,completed,market_items,event_items"
            ") VALUES (?,1,2,1)",
            (snapshot_id,),
        )
        con.executemany(
            "INSERT INTO event_market_memberships("
            "snapshot_id,event_id,neg_risk_market_id,market_id,member_kind,active,closed"
            ") VALUES (?,?,?,?, 'named',1,0)",
            [
                (snapshot_id, EVENT_ID, "group-a", "market-a"),
                (snapshot_id, EVENT_ID, "group-a", "market-b"),
            ],
        )
        con.execute(
            "INSERT INTO neg_risk_group_truth("
            "snapshot_id,event_id,neg_risk_market_id,neg_risk_type,"
            "expected_member_count,active_named_count,membership_hash,quality,reason"
            ") VALUES (?,?,?,'standard',2,2,?,'complete-supported',NULL)",
            (snapshot_id, EVENT_ID, "group-a", MEMBERSHIP_HASH),
        )
        con.execute("DELETE FROM legacy_structure_revision_dirty")
    return path


def _now_sequence(*values: int):
    iterator = iter(values)
    return lambda: next(iterator)


def _collect(store: NegRiskQuoteStore, reader: FakeReader, *, start: int = NOW_MS):
    return asyncio.run(
        collect_neg_risk_quotes(
            quote_store=store,
            reader=reader,
            now_ms=_now_sequence(start, start + 25, start + 25, start + 25),
        )
    )


def test_collection_attempt_checkpoints_every_terminal_phase(quote_db) -> None:
    store = NegRiskQuoteStore(quote_db, now_ms=lambda: NOW_MS)
    attempt_id = store.start_collection_attempt()

    result = asyncio.run(
        collect_neg_risk_quotes(
            quote_store=store,
            reader=FakeReader(
                [
                    FixtureBook("token-a", [{"price": "0.4", "size": "10"}]),
                    FixtureBook("token-b", [{"price": "0.5", "size": "10"}]),
                ]
            ),
            now_ms=_now_sequence(NOW_MS, NOW_MS + 25, NOW_MS + 25, NOW_MS + 25),
            attempt_id=attempt_id,
        )
    )

    attempt = store.latest_collection_attempt()
    assert attempt is not None
    assert attempt["id"] == attempt_id == result.attempt_id
    assert attempt["phase"] == "certify"
    assert attempt["outcome"] == "collecting"
    assert attempt["quote_run_id"] == result.run_id
    assert attempt["target_count"] == 2
    assert len(str(attempt["structure_receipt_digest"])) == 64
    assert set(attempt["phase_timings"]) == {
        "universe_ms",
        "admission_ms",
        "fetch_ms",
        "transform_ms",
        "persist_ms",
    }


async def test_slow_universe_reconstruction_does_not_block_event_loop(quote_db) -> None:
    store = NegRiskQuoteStore(quote_db)
    original = store.latest_verified_universe
    started = threading.Event()
    release = threading.Event()

    def slow_universe():
        started.set()
        assert release.wait(timeout=1)
        return original()

    store.latest_verified_universe = slow_universe  # type: ignore[method-assign]
    task = asyncio.create_task(
        collect_neg_risk_quotes(
            quote_store=store,
            reader=FakeReader([]),
            now_ms=_now_sequence(NOW_MS, NOW_MS + 25, NOW_MS + 25, NOW_MS + 25),
        )
    )
    assert await asyncio.to_thread(started.wait, 0.2)

    ticker = asyncio.Event()
    asyncio.get_running_loop().call_soon(ticker.set)
    await asyncio.wait_for(ticker.wait(), timeout=0.05)
    assert not task.done()

    release.set()
    await task


def _legs() -> tuple[UniverseLeg, ...]:
    return (
        UniverseLeg(
            "group-a",
            "market-a",
            "condition-a",
            "alpha",
            "token-a",
            EVENT_ID,
            MEMBERSHIP_HASH,
        ),
        UniverseLeg(
            "group-a",
            "market-b",
            "condition-b",
            "beta",
            "token-b",
            EVENT_ID,
            MEMBERSHIP_HASH,
        ),
    )


def _begin(store: NegRiskQuoteStore) -> int:
    return store.begin_run(
        universe_snapshot_id=1,
        universe_taken_at_ms=NOW_MS - 1_000,
        legs=_legs(),
        quoted_at_ms=NOW_MS,
    )


def test_collects_latest_universe_once_and_persists_lowest_valid_ask(quote_db) -> None:
    store = NegRiskQuoteStore(quote_db)
    reader = FakeReader(
        [
            FixtureBook(
                "token-a",
                [
                    {"price": "0.71", "size": "4"},
                    {"price": "0.42", "size": "11"},
                ],
            ),
            {"asset_id": "token-b", "asks": [{"price": 0.51, "size": 8}]},
        ]
    )

    result = _collect(store, reader)

    assert reader.requests == [["token-a", "token-b"]]
    assert reader.projections == ["top"]
    assert result.status == "complete"
    assert result.universe_snapshot_id == 1
    assert result.requested_token_count == 2
    assert result.successful_response_count == 2
    assert result.quote_taken_at_ms == NOW_MS
    assert result.elapsed_ms == 25
    projection = store.latest_complete_projection()
    assert projection is not None
    quotes = [
        (q.yes_token_id, q.terminal_state, q.best_ask_price, q.best_ask_size)
        for q in projection.quotes
    ]
    assert quotes == [
        ("token-a", "executable", 0.42, 11.0),
        ("token-b", "executable", 0.51, 8.0),
    ]


@pytest.mark.parametrize(
    ("book", "state", "successful_response_count"),
    [
        (None, "missing-book", 1),
        (FixtureBook("token-b", []), "missing-ask", 2),
        (
            FixtureBook("token-b", [{"price": "wat", "size": "4"}]),
            "invalid-ask-price",
            2,
        ),
        (
            FixtureBook("token-b", [{"price": "0.4", "size": "0"}]),
            "invalid-ask-size",
            2,
        ),
    ],
)
def test_partial_responses_persist_visible_terminal_non_executable_reason(
    quote_db, book: FixtureBook | None, state: str, successful_response_count: int
) -> None:
    store = NegRiskQuoteStore(quote_db)
    books: list[object] = [FixtureBook("token-a", [{"price": "0.4", "size": "4"}])]
    if book is not None:
        books.append(book)

    result = _collect(store, FakeReader(books))

    assert result.status == "complete"
    assert result.successful_response_count == successful_response_count
    projection = store.latest_complete_projection()
    assert projection is not None
    assert projection.successful_response_count == successful_response_count
    sibling = next(quote for quote in projection.quotes if quote.yes_token_id == "token-b")
    assert (
        sibling.terminal_state,
        sibling.best_ask_price,
        sibling.best_ask_size,
    ) == (state, None, None)


def test_transport_failure_fails_new_run_without_displacing_prior_complete_run(quote_db) -> None:
    store = NegRiskQuoteStore(quote_db)
    first = _collect(
        store,
        FakeReader(
            [
                FixtureBook("token-a", [{"price": "0.4", "size": "4"}]),
                FixtureBook("token-b", [{"price": "0.5", "size": "4"}]),
            ]
        ),
    )

    with pytest.raises(ConnectionError, match="fixture transport failure"):
        _collect(
            store,
            FakeReader(ConnectionError("fixture transport failure")),
            start=NOW_MS + 100,
        )

    projection = store.latest_complete_projection()
    assert projection is not None
    assert projection.run_id == first.run_id
    with sqlite3.connect(quote_db) as con:
        failed = con.execute(
            "SELECT status, failure_reason FROM neg_risk_quote_runs ORDER BY id DESC LIMIT 1"
        ).fetchone()
    assert failed == ("failed", "clob-fetch-failed")


def test_persistence_failure_fails_new_run_without_displacing_prior_complete_run(
    quote_db, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = NegRiskQuoteStore(quote_db)
    first = _collect(
        store,
        FakeReader(
            [
                FixtureBook("token-a", [{"price": "0.4", "size": "4"}]),
                FixtureBook("token-b", [{"price": "0.5", "size": "4"}]),
            ]
        ),
    )

    def fail_record(*args: object, **kwargs: object) -> None:
        raise sqlite3.OperationalError("fixture write failure")

    monkeypatch.setattr(store, "record_terminal_quotes", fail_record)
    with pytest.raises(sqlite3.OperationalError, match="fixture write failure"):
        _collect(
            store,
            FakeReader(
                [
                    FixtureBook("token-a", [{"price": "0.4", "size": "4"}]),
                    FixtureBook("token-b", [{"price": "0.5", "size": "4"}]),
                ]
            ),
            start=NOW_MS + 100,
        )

    projection = store.latest_complete_projection()
    assert projection is not None
    assert projection.run_id == first.run_id
    with sqlite3.connect(quote_db) as con:
        failed = con.execute(
            "SELECT status, failure_reason FROM neg_risk_quote_runs ORDER BY id DESC LIMIT 1"
        ).fetchone()
    assert failed == ("failed", "collector-error")


def test_sqlite_writer_contention_is_typed_as_a_persist_timeout(
    quote_db, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = NegRiskQuoteStore(quote_db, writer_timeout_s=15)

    def busy_record(*args: object, **kwargs: object) -> None:
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(store, "record_terminal_quotes", busy_record)

    with pytest.raises(QuotePersistenceTimeoutError, match="quote-persist-timeout"):
        _collect(
            store,
            FakeReader(
                [
                    FixtureBook("token-a", [{"price": "0.4", "size": "4"}]),
                    FixtureBook("token-b", [{"price": "0.5", "size": "4"}]),
                ]
            ),
        )

    with sqlite3.connect(quote_db) as con:
        failed = con.execute(
            "SELECT status,failure_reason FROM neg_risk_quote_runs ORDER BY id DESC LIMIT 1"
        ).fetchone()
    assert failed == ("failed", "sqlite-persist-timeout")


@pytest.mark.parametrize(
    "response",
    [
        [FixtureBook("token-a", [{"price": "0.4", "size": "4"}]), FixtureBook("token-z", [])],
        [FixtureBook("token-a", []), FixtureBook("token-a", [])],
        object(),
    ],
)
def test_unusable_or_mismatched_clob_payload_fails_new_run(response, quote_db) -> None:
    store = NegRiskQuoteStore(quote_db)

    with pytest.raises(QuoteCollectionIntegrityError):
        _collect(store, FakeReader(response))

    assert store.latest_complete_projection() is None
    with sqlite3.connect(quote_db) as con:
        failed = con.execute(
            "SELECT status, failure_reason, successful_response_count FROM neg_risk_quote_runs"
        ).fetchone()
    assert failed == ("failed", "clob-response-integrity-failed", 0)


def test_busy_or_unavailable_universe_does_not_call_clob(quote_db) -> None:
    store = NegRiskQuoteStore(quote_db)
    busy_run = store.begin_run(
        universe_snapshot_id=1,
        universe_taken_at_ms=NOW_MS - 1_000,
        legs=_legs(),
        quoted_at_ms=NOW_MS,
    )
    busy_reader = FakeReader([])

    with pytest.raises(QuoteRunBusyError):
        _collect(store, busy_reader)
    assert busy_reader.requests == []
    store.fail_run(busy_run, failure_reason="collector-aborted")

    empty_path = quote_db.parent / "empty.db"
    SQLiteStore(empty_path).init_schema()
    unavailable_reader = FakeReader([])
    with pytest.raises(QuoteUniverseUnavailableError, match="quote-universe-unavailable"):
        _collect(NegRiskQuoteStore(empty_path), unavailable_reader)
    assert unavailable_reader.requests == []

    with sqlite3.connect(empty_path) as con:
        con.execute(
            "INSERT INTO snapshots("
            "taken_at_ms, finished_at_ms, mode, market_count,market_view_published,"
            "data_product,is_valid, parquet_path"
            ") VALUES (?, ?, 'subset', 1,1,'structure',1, 'fixture.parquet')",
            (NOW_MS, NOW_MS),
        )
        con.execute(
            "INSERT INTO markets("
            "market_id,condition_id,yes_token_id,active,closed,neg_risk_market_id,"
            "fetched_at_ms,snapshot_id,incomplete,event_id"
            ") VALUES ('augmented-market','augmented-condition','augmented-token',"
            "1,0,'augmented-group',?,1,0,'augmented-event')",
            (NOW_MS,),
        )
        con.execute(
            "INSERT INTO snapshot_source_coverage("
            "snapshot_id,completed,market_items,event_items"
            ") VALUES (1,1,1,1)"
        )
        con.execute(
            "INSERT INTO event_market_memberships("
            "snapshot_id,event_id,neg_risk_market_id,market_id,member_kind,active,closed"
            ") VALUES (1,'augmented-event','augmented-group','augmented-market','named',1,0)"
        )
        con.execute(
            "INSERT INTO neg_risk_group_truth("
            "snapshot_id,event_id,neg_risk_market_id,neg_risk_type,"
            "expected_member_count,active_named_count,membership_hash,quality,reason"
            ") VALUES (1,'augmented-event','augmented-group','augmented',1,1,"
            "'augmented-hash','complete-unsupported','augmented-neg-risk-not-supported')"
        )
        con.execute("DELETE FROM legacy_structure_revision_dirty")
    zero_eligible_reader = FakeReader([])
    result = _collect(NegRiskQuoteStore(empty_path), zero_eligible_reader)

    assert result.status == "complete"
    assert result.requested_token_count == 0
    assert result.successful_response_count == 0
    assert result.elapsed_ms == 0
    assert zero_eligible_reader.requests == []


def test_complete_published_zero_market_universe_completes_without_clob(tmp_path) -> None:
    path = tmp_path / "zero-market.db"
    snapshot_store = SQLiteStore(path)
    snapshot_store.init_schema()
    snapshot_store.write_snapshot(
        taken_at_ms=NOW_MS,
        finished_at_ms=NOW_MS + 1,
        mode="subset",
        parquet_path="zero-market.parquet",
        is_valid=True,
        market_rows=[],
        issues=[],
        source_coverage=SourceCoverage.complete(0, 0),
        event_members=[],
        group_truths=[],
        publish_markets=True,
        data_product="structure",
    )
    reader = FakeReader([])

    result = _collect(NegRiskQuoteStore(path), reader)

    assert result.status == "complete"
    assert result.requested_token_count == 0
    assert reader.requests == []


def test_zero_leg_completion_failure_fails_run_without_calling_clob(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "zero-market-failure.db"
    snapshot_store = SQLiteStore(path)
    snapshot_store.init_schema()
    snapshot_store.write_snapshot(
        taken_at_ms=NOW_MS,
        finished_at_ms=NOW_MS + 1,
        mode="subset",
        parquet_path="zero-market.parquet",
        is_valid=True,
        market_rows=[],
        issues=[],
        source_coverage=SourceCoverage.complete(0, 0),
        event_members=[],
        group_truths=[],
        publish_markets=True,
        data_product="structure",
    )
    store = NegRiskQuoteStore(path)
    reader = FakeReader([])

    def fail_complete(*_: object, **__: object):
        raise sqlite3.OperationalError("injected completion failure")

    monkeypatch.setattr(store, "complete_run", fail_complete)

    with pytest.raises(sqlite3.OperationalError, match="injected completion failure"):
        _collect(store, reader)

    assert reader.requests == []
    with sqlite3.connect(path) as con:
        assert con.execute("SELECT status,failure_reason FROM neg_risk_quote_runs").fetchall() == [
            ("failed", "collector-error")
        ]


def test_slow_collector_renews_lease_before_an_expired_run_can_be_reclaimed(
    quote_db, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        clock = {"now": NOW_MS}
        store = NegRiskQuoteStore(quote_db, now_ms=lambda: clock["now"])
        reader = BlockingReader(
            [
                FixtureBook("token-a", [{"price": "0.4", "size": "4"}]),
                FixtureBook("token-b", [{"price": "0.5", "size": "4"}]),
            ]
        )
        sleeper = OneTickSleeper()
        lease_renewed = asyncio.Event()
        actual_renew = store.renew_run_lease

        def observe_renew(run_id: int) -> None:
            actual_renew(run_id)
            lease_renewed.set()

        monkeypatch.setattr(store, "renew_run_lease", observe_renew)
        collection = asyncio.create_task(
            collect_neg_risk_quotes(
                quote_store=store,
                reader=reader,
                now_ms=lambda: clock["now"],
                lease_sleep=sleeper,
            )
        )
        await reader.started.wait()
        await sleeper.first_tick_started.wait()

        # Renew just before expiry, then advance beyond the original lease.
        # A second process must still see the renewed owner as live.
        clock["now"] = NOW_MS + QUOTE_RUN_LEASE_MS - 1
        sleeper.permit_first_tick.set()
        await lease_renewed.wait()
        clock["now"] = NOW_MS + QUOTE_RUN_LEASE_MS
        with pytest.raises(QuoteRunBusyError, match="collecting quote run"):
            store.begin_run(
                universe_snapshot_id=1,
                universe_taken_at_ms=NOW_MS - 1_000,
                legs=_legs(),
                quoted_at_ms=clock["now"],
            )

        reader.release.set()
        assert (await collection).status == "complete"

    asyncio.run(scenario())


def test_fetch_timeout_has_stable_type_and_failed_run_reason(quote_db) -> None:
    from polyarb.routing.neg_risk_quote_collector import QuoteFetchTimeoutError

    async def scenario() -> None:
        store = NegRiskQuoteStore(quote_db, now_ms=lambda: NOW_MS)
        reader = BlockingReader([])
        with pytest.raises(QuoteFetchTimeoutError, match="clob-fetch-timeout"):
            await collect_neg_risk_quotes(
                quote_store=store,
                reader=reader,
                fetch_timeout_s=0.001,
            )

        with sqlite3.connect(quote_db) as con:
            assert con.execute(
                "SELECT status,failure_reason FROM neg_risk_quote_runs"
            ).fetchone() == ("failed", "clob-fetch-timeout")

    asyncio.run(scenario())


def test_lease_renewal_failure_cancels_collection_and_fails_closed(
    quote_db, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        clock = {"now": NOW_MS}
        store = NegRiskQuoteStore(quote_db, now_ms=lambda: clock["now"])
        reader = BlockingReader([])
        sleeper = OneTickSleeper()
        renewal_attempted = asyncio.Event()

        def fail_renew(*_: object, **__: object) -> None:
            renewal_attempted.set()
            raise sqlite3.OperationalError("fixture lease write failure")

        monkeypatch.setattr(store, "renew_run_lease", fail_renew)
        collection = asyncio.create_task(
            collect_neg_risk_quotes(
                quote_store=store,
                reader=reader,
                now_ms=lambda: clock["now"],
                lease_sleep=sleeper,
            )
        )
        await reader.started.wait()
        await sleeper.first_tick_started.wait()
        sleeper.permit_first_tick.set()
        await renewal_attempted.wait()

        with pytest.raises(QuoteRunLeaseLostError, match="quote-run-lease-lost"):
            await collection
        assert reader.cancelled is True
        with sqlite3.connect(quote_db) as con:
            assert con.execute(
                "SELECT status, failure_reason FROM neg_risk_quote_runs"
            ).fetchone() == ("failed", "collector-lease-lost")

        # The aborted collector did not finish its CLOB request or leave a
        # competing live run behind; a later owner may acquire the lease.
        recovered_run = _begin(store)
        store.fail_run(recovered_run, failure_reason="fixture-cleanup")

    asyncio.run(scenario())


def test_collector_does_not_persist_after_final_renewal_lease_expires(
    quote_db, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        clock = {"now": NOW_MS}
        store = NegRiskQuoteStore(quote_db, now_ms=lambda: clock["now"])
        actual_renew = store.renew_run_lease

        def expire_after_final_renewal(run_id: int) -> None:
            actual_renew(run_id)
            clock["now"] = NOW_MS + QUOTE_RUN_LEASE_MS

        monkeypatch.setattr(store, "renew_run_lease", expire_after_final_renewal)
        with pytest.raises(QuoteRunLeaseLostError, match="quote-run-lease-lost"):
            await collect_neg_risk_quotes(
                quote_store=store,
                reader=FakeReader(
                    [
                        FixtureBook("token-a", [{"price": "0.4", "size": "4"}]),
                        FixtureBook("token-b", [{"price": "0.5", "size": "4"}]),
                    ]
                ),
                now_ms=lambda: clock["now"],
                lease_sleep=OneTickSleeper(),
            )

        with sqlite3.connect(quote_db) as con:
            assert con.execute("SELECT COUNT(*) FROM neg_risk_quotes").fetchone()[0] == 0
            assert con.execute(
                "SELECT status, failure_reason FROM neg_risk_quote_runs"
            ).fetchone() == ("failed", "collector-lease-lost")

        recovered_run = _begin(store)
        store.fail_run(recovered_run, failure_reason="fixture-cleanup")

    asyncio.run(scenario())
