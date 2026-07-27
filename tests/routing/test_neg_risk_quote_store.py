from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from collections.abc import Callable

import pytest

from polyarb.perception.market_truth import SourceCoverage
from polyarb.routing.neg_risk_quote_store import (
    QUOTE_RUN_LEASE_MS,
    NegRiskQuoteStore,
    PersistedQuote,
    QuoteProjectionIntegrityError,
    QuoteRunBusyError,
    QuoteRunStateError,
    UniverseLeg,
)
from polyarb.storage.sqlite_store import SQLiteStore

NOW_MS = 1_700_000_000_000
EVENT_ID = "event-a"
MEMBERSHIP_HASH = "membership-hash-a"


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
    return path


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


def _quote(token_id: str, *, terminal_state: str = "executable") -> PersistedQuote:
    leg = next(leg for leg in _legs() if leg.yes_token_id == token_id)
    if terminal_state == "executable":
        return PersistedQuote(
            leg.neg_risk_market_id,
            leg.market_id,
            leg.condition_id,
            leg.slug,
            leg.yes_token_id,
            terminal_state,
            0.42,
            10.0,
            leg.event_id,
            leg.membership_hash,
        )
    return PersistedQuote(
        leg.neg_risk_market_id,
        leg.market_id,
        leg.condition_id,
        leg.slug,
        leg.yes_token_id,
        terminal_state,
        None,
        None,
        leg.event_id,
        leg.membership_hash,
    )


def _begin(store: NegRiskQuoteStore) -> int:
    return store.begin_run(
        universe_snapshot_id=1,
        universe_taken_at_ms=NOW_MS - 1_000,
        legs=_legs(),
        quoted_at_ms=NOW_MS,
    )


def _complete(store: NegRiskQuoteStore) -> int:
    run_id = _begin(store)
    store.record_terminal_quotes(run_id, (_quote("token-a"), _quote("token-b")))
    store.complete_run(
        run_id,
        completed_at_ms=NOW_MS + 1,
        successful_response_count=2,
    )
    return run_id


class BeginGate:
    def __init__(self) -> None:
        self.enabled = False
        self.entered = threading.Event()
        self.release = threading.Event()


class GatedQuoteStore(NegRiskQuoteStore):
    """Test seam that pauses an owner before it serializes BEGIN IMMEDIATE."""

    def __init__(self, *args: object, gate: BeginGate, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self._gate = gate

    def _begin_immediate(self, con: sqlite3.Connection) -> None:
        if self._gate.enabled:
            self._gate.entered.set()
            assert self._gate.release.wait(timeout=1)
        super()._begin_immediate(con)


def _run_after_begin_gate(
    gate: BeginGate, clock: dict[str, int], operation: Callable[[], object]
) -> object | BaseException | None:
    outcome: dict[str, object] = {}

    def invoke() -> None:
        try:
            outcome["value"] = operation()
        except BaseException as error:
            outcome["error"] = error

    worker = threading.Thread(target=invoke)
    worker.start()
    assert gate.entered.wait(timeout=1)
    clock["now"] += QUOTE_RUN_LEASE_MS
    gate.release.set()
    worker.join(timeout=1)
    assert not worker.is_alive()
    return outcome.get("error", outcome.get("value"))


def test_started_run_is_invisible_to_latest_complete_projection(quote_db) -> None:
    store = NegRiskQuoteStore(quote_db)

    _begin(store)

    assert store.latest_complete_projection() is None
    assert store.latest_complete_run() is None


def test_run_requires_one_terminal_row_for_every_requested_token(quote_db) -> None:
    store = NegRiskQuoteStore(quote_db)
    run_id = _begin(store)
    store.record_terminal_quotes(run_id, (_quote("token-a"),))

    with pytest.raises(QuoteRunStateError, match="cannot complete quote run"):
        store.complete_run(
            run_id,
            completed_at_ms=NOW_MS + 1,
            successful_response_count=1,
        )

    with sqlite3.connect(quote_db) as con:
        status = con.execute(
            "SELECT status FROM neg_risk_quote_runs WHERE id = ?", (run_id,)
        ).fetchone()[0]
    assert status == "collecting"


@pytest.mark.parametrize("successful_response_count", (-1, 3))
def test_complete_run_rejects_response_count_outside_requested_bounds(
    quote_db, successful_response_count: int
) -> None:
    store = NegRiskQuoteStore(quote_db)
    run_id = _begin(store)
    store.record_terminal_quotes(run_id, (_quote("token-a"), _quote("token-b")))

    with pytest.raises(ValueError, match="successful_response_count"):
        store.complete_run(
            run_id,
            completed_at_ms=NOW_MS + 1,
            successful_response_count=successful_response_count,
        )


@pytest.mark.parametrize(
    ("terminal_state", "price", "size"),
    [
        ("unknown", None, None),
        ("missing-book", 0.4, 1.0),
        ("executable", 0.0, 1.0),
        ("executable", float("inf"), 1.0),
        ("executable", 0.4, float("nan")),
    ],
)
def test_terminal_quote_validation_rejects_invalid_reason_or_values(
    quote_db, terminal_state: str, price: float | None, size: float | None
) -> None:
    store = NegRiskQuoteStore(quote_db)
    run_id = _begin(store)
    leg = _legs()[0]
    invalid = PersistedQuote(
        leg.neg_risk_market_id,
        leg.market_id,
        leg.condition_id,
        leg.slug,
        leg.yes_token_id,
        terminal_state,
        price,
        size,
    )

    with pytest.raises(ValueError):
        store.record_terminal_quotes(run_id, (invalid,))


def test_quote_table_constraints_reject_invalid_terminal_row(quote_db) -> None:
    store = NegRiskQuoteStore(quote_db)
    run_id = _begin(store)

    with sqlite3.connect(quote_db) as con:
        with pytest.raises(sqlite3.IntegrityError):
            con.execute(
                "INSERT INTO neg_risk_quotes("
                "quote_run_id, neg_risk_market_id, market_id, condition_id, "
                "yes_token_id, terminal_state, best_ask_price, best_ask_size"
                ") VALUES (?, 'group-a', 'market-a', 'condition-a', 'raw-token', "
                "'missing-book', 0.4, 1.0)",
                (run_id,),
            )
        with pytest.raises(sqlite3.IntegrityError):
            con.execute(
                "INSERT INTO neg_risk_quotes("
                "quote_run_id, neg_risk_market_id, market_id, condition_id, "
                "yes_token_id, terminal_state, best_ask_price, best_ask_size"
                ") VALUES (?, 'group-a', 'market-a', 'condition-a', 'raw-token', "
                "'not-a-state', NULL, NULL)",
                (run_id,),
            )


@pytest.mark.parametrize(
    ("price", "size"),
    [(0.0, 1.0), (1.01, 1.0), (0.4, 0.0), (0.4, -1.0)],
)
def test_quote_table_constraints_reject_non_executable_values_for_executable_state(
    quote_db, price: float, size: float
) -> None:
    store = NegRiskQuoteStore(quote_db)
    run_id = _begin(store)

    with sqlite3.connect(quote_db) as con:
        with pytest.raises(sqlite3.IntegrityError):
            con.execute(
                "INSERT INTO neg_risk_quotes("
                "quote_run_id, neg_risk_market_id, market_id, condition_id, "
                "yes_token_id, terminal_state, best_ask_price, best_ask_size"
                ") VALUES (?, 'group-a', 'market-a', 'condition-a', 'raw-token', "
                "'executable', ?, ?)",
                (run_id, price, size),
            )


def test_terminal_quotes_must_belong_to_the_run_requested_set(quote_db) -> None:
    store = NegRiskQuoteStore(quote_db)
    run_id = _begin(store)
    unknown = PersistedQuote(
        "group-z",
        "market-z",
        "condition-z",
        "zeta",
        "token-z",
        "missing-book",
        None,
        None,
    )

    with pytest.raises(ValueError, match="not requested"):
        store.record_terminal_quotes(run_id, (unknown,))


@pytest.mark.parametrize(
    "forged_quote",
    [
        PersistedQuote(
            "forged-group",
            "market-a",
            "condition-a",
            "alpha",
            "token-a",
            "missing-book",
            None,
            None,
        ),
        PersistedQuote(
            "group-a",
            "forged-market",
            "condition-a",
            "alpha",
            "token-a",
            "missing-book",
            None,
            None,
        ),
    ],
)
def test_terminal_quotes_must_match_durable_requested_leg_identity(
    quote_db, forged_quote: PersistedQuote
) -> None:
    store = NegRiskQuoteStore(quote_db)
    run_id = _begin(store)

    with pytest.raises(ValueError, match="identity"):
        store.record_terminal_quotes(run_id, (forged_quote,))


def test_failed_run_does_not_displace_prior_complete_run(quote_db) -> None:
    store = NegRiskQuoteStore(quote_db)
    complete_id = _complete(store)
    failed_id = _begin(store)
    store.fail_run(failed_id, failure_reason="clob-fetch-failed")

    projection = store.latest_complete_projection()

    assert projection is not None
    assert projection.run_id == complete_id
    assert [quote.yes_token_id for quote in projection.quotes] == ["token-a", "token-b"]


def test_begin_run_lock_is_database_state_not_instance_state(quote_db) -> None:
    first = NegRiskQuoteStore(quote_db)
    second = NegRiskQuoteStore(quote_db)
    first_run = _begin(first)

    with pytest.raises(QuoteRunBusyError, match="collecting quote run"):
        _begin(second)

    first.fail_run(first_run, failure_reason="collector-aborted")
    second_run = _begin(second)
    second.fail_run(second_run, failure_reason="collector-aborted")
    third_run = _complete(first)
    after_complete_run = _begin(second)

    assert after_complete_run > third_run > second_run > first_run


def test_begin_run_recovers_only_an_expired_crashed_collecting_run(quote_db) -> None:
    clock = {"now": NOW_MS}
    first = NegRiskQuoteStore(quote_db, now_ms=lambda: clock["now"])
    second = NegRiskQuoteStore(quote_db, now_ms=lambda: clock["now"])
    crashed_run = _begin(first)

    # This is the durable shape of a collector that died after begin_run() and
    # before its best-effort failure transition could reach SQLite.
    with sqlite3.connect(quote_db) as con:
        con.execute(
            "UPDATE neg_risk_quote_runs SET lease_expires_at_ms = ? WHERE id = ?",
            (NOW_MS - 1, crashed_run),
        )

    recovered_run = _begin(second)

    with sqlite3.connect(quote_db) as con:
        assert con.execute(
            "SELECT status, failure_reason FROM neg_risk_quote_runs WHERE id = ?",
            (crashed_run,),
        ).fetchone() == ("failed", "collector-lease-expired")
        con.execute(
            "UPDATE neg_risk_quote_runs SET lease_expires_at_ms = ? WHERE id = ?",
            (NOW_MS + 1, recovered_run),
        )

    with pytest.raises(QuoteRunBusyError, match="collecting quote run"):
        _begin(first)


def test_expired_collecting_run_cannot_write_terminal_quotes(quote_db) -> None:
    clock = {"now": NOW_MS}
    store = NegRiskQuoteStore(quote_db, now_ms=lambda: clock["now"])
    run_id = _begin(store)
    clock["now"] += QUOTE_RUN_LEASE_MS

    with pytest.raises(QuoteRunStateError, match="live collection lease"):
        store.record_terminal_quotes(run_id, (_quote("token-a"), _quote("token-b")))

    with sqlite3.connect(quote_db) as con:
        assert con.execute("SELECT COUNT(*) FROM neg_risk_quotes").fetchone()[0] == 0
    recovered_run = store.begin_run(
        universe_snapshot_id=1,
        universe_taken_at_ms=NOW_MS - 1_000,
        legs=_legs(),
        quoted_at_ms=NOW_MS + QUOTE_RUN_LEASE_MS,
    )
    store.fail_run(recovered_run, failure_reason="fixture-cleanup")


def test_expired_collecting_run_cannot_complete_terminal_quotes(quote_db) -> None:
    clock = {"now": NOW_MS}
    store = NegRiskQuoteStore(quote_db, now_ms=lambda: clock["now"])
    run_id = _begin(store)
    store.record_terminal_quotes(run_id, (_quote("token-a"), _quote("token-b")))
    clock["now"] += QUOTE_RUN_LEASE_MS

    with pytest.raises(QuoteRunStateError, match="live collection lease"):
        store.complete_run(
            run_id,
            completed_at_ms=NOW_MS + QUOTE_RUN_LEASE_MS,
            successful_response_count=2,
        )

    with sqlite3.connect(quote_db) as con:
        assert con.execute(
            "SELECT status FROM neg_risk_quote_runs WHERE id = ?", (run_id,)
        ).fetchone() == ("collecting",)


@pytest.mark.parametrize("lease_value", ("default", 0))
def test_begin_recovers_legacy_default_and_zero_collecting_leases(quote_db, lease_value) -> None:
    store = NegRiskQuoteStore(quote_db)
    with sqlite3.connect(quote_db) as con:
        if lease_value == "default":
            con.execute(
                "INSERT INTO neg_risk_quote_runs("
                "universe_snapshot_id, universe_taken_at_ms, quoted_at_ms, "
                "requested_token_count, status"
                ") VALUES (1, ?, ?, 0, 'collecting')",
                (NOW_MS - 1_000, NOW_MS - 1),
            )
        else:
            con.execute(
                "INSERT INTO neg_risk_quote_runs("
                "universe_snapshot_id, universe_taken_at_ms, quoted_at_ms, "
                "requested_token_count, lease_expires_at_ms, status"
                ") VALUES (1, ?, ?, 0, ?, 'collecting')",
                (NOW_MS - 1_000, NOW_MS - 1, lease_value),
            )
        legacy_run = int(con.execute("SELECT last_insert_rowid()").fetchone()[0])

    recovered_run = _begin(store)

    with sqlite3.connect(quote_db) as con:
        assert con.execute(
            "SELECT status, failure_reason FROM neg_risk_quote_runs WHERE id = ?",
            (legacy_run,),
        ).fetchone() == ("failed", "collector-lease-expired")
    store.fail_run(recovered_run, failure_reason="fixture-cleanup")


def test_begin_recovers_legacy_null_collecting_lease(quote_db) -> None:
    store = NegRiskQuoteStore(quote_db)
    with sqlite3.connect(quote_db) as con:
        con.execute("PRAGMA foreign_keys=OFF")
        con.execute("DROP TABLE neg_risk_quote_runs")
        con.execute(
            "CREATE TABLE neg_risk_quote_runs ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, universe_snapshot_id INTEGER NOT NULL, "
            "universe_taken_at_ms INTEGER NOT NULL, universe_hash TEXT NOT NULL DEFAULT '', "
            "source_truth_hash TEXT NOT NULL DEFAULT '', "
            "quoted_at_ms INTEGER NOT NULL, "
            "requested_token_count INTEGER NOT NULL, "
            "successful_response_count INTEGER NOT NULL DEFAULT 0, "
            "lease_expires_at_ms INTEGER, status TEXT NOT NULL, failure_reason TEXT, "
            "completed_at_ms INTEGER)"
        )
        con.execute(
            "INSERT INTO neg_risk_quote_runs("
            "universe_snapshot_id, universe_taken_at_ms, quoted_at_ms, "
            "requested_token_count, lease_expires_at_ms, status"
            ") VALUES (1, ?, ?, 0, NULL, 'collecting')",
            (NOW_MS - 1_000, NOW_MS - 1),
        )
        legacy_run = int(con.execute("SELECT last_insert_rowid()").fetchone()[0])

    recovered_run = _begin(store)

    with sqlite3.connect(quote_db) as con:
        assert con.execute(
            "SELECT status, failure_reason FROM neg_risk_quote_runs WHERE id = ?",
            (legacy_run,),
        ).fetchone() == ("failed", "collector-lease-expired")
    store.fail_run(recovered_run, failure_reason="fixture-cleanup")


def test_renew_rejects_expired_and_reclaimed_original_owner(quote_db) -> None:
    clock = {"now": NOW_MS}
    store = NegRiskQuoteStore(quote_db, now_ms=lambda: clock["now"])
    original_run = _begin(store)
    clock["now"] += QUOTE_RUN_LEASE_MS

    with pytest.raises(QuoteRunStateError, match="live collection lease"):
        store.renew_run_lease(original_run)

    replacement_run = store.begin_run(
        universe_snapshot_id=1,
        universe_taken_at_ms=NOW_MS - 1_000,
        legs=_legs(),
        quoted_at_ms=clock["now"],
    )
    with pytest.raises(QuoteRunStateError, match="live collection lease"):
        store.renew_run_lease(original_run)
    store.fail_run(replacement_run, failure_reason="fixture-cleanup")


def test_renew_does_not_resurrect_after_waiting_to_begin_transaction(quote_db) -> None:
    clock = {"now": NOW_MS}
    gate = BeginGate()
    store = GatedQuoteStore(quote_db, now_ms=lambda: clock["now"], gate=gate)
    run_id = _begin(store)
    gate.enabled = True

    error = _run_after_begin_gate(gate, clock, lambda: store.renew_run_lease(run_id))

    assert isinstance(error, QuoteRunStateError)
    with sqlite3.connect(quote_db) as con:
        assert con.execute(
            "SELECT lease_expires_at_ms FROM neg_risk_quote_runs WHERE id = ?", (run_id,)
        ).fetchone() == (NOW_MS + QUOTE_RUN_LEASE_MS,)


def test_begin_recovers_after_waiting_to_begin_transaction(quote_db) -> None:
    clock = {"now": NOW_MS}
    gate = BeginGate()
    store = GatedQuoteStore(quote_db, now_ms=lambda: clock["now"], gate=gate)
    expired_owner = _begin(store)
    gate.enabled = True

    replacement = _run_after_begin_gate(
        gate,
        clock,
        lambda: store.begin_run(
            universe_snapshot_id=1,
            universe_taken_at_ms=NOW_MS - 1_000,
            legs=_legs(),
            quoted_at_ms=NOW_MS,
        ),
    )

    assert isinstance(replacement, int)
    with sqlite3.connect(quote_db) as con:
        assert con.execute(
            "SELECT status, failure_reason FROM neg_risk_quote_runs WHERE id = ?",
            (expired_owner,),
        ).fetchone() == ("failed", "collector-lease-expired")
    store.fail_run(replacement, failure_reason="fixture-cleanup")


def test_record_does_not_write_after_waiting_to_begin_transaction(quote_db) -> None:
    clock = {"now": NOW_MS}
    gate = BeginGate()
    store = GatedQuoteStore(quote_db, now_ms=lambda: clock["now"], gate=gate)
    run_id = _begin(store)
    gate.enabled = True

    error = _run_after_begin_gate(
        gate,
        clock,
        lambda: store.record_terminal_quotes(run_id, (_quote("token-a"), _quote("token-b"))),
    )

    assert isinstance(error, QuoteRunStateError)
    with sqlite3.connect(quote_db) as con:
        assert con.execute("SELECT COUNT(*) FROM neg_risk_quotes").fetchone()[0] == 0


def test_complete_does_not_finish_after_waiting_to_begin_transaction(quote_db) -> None:
    clock = {"now": NOW_MS}
    gate = BeginGate()
    store = GatedQuoteStore(quote_db, now_ms=lambda: clock["now"], gate=gate)
    run_id = _begin(store)
    store.record_terminal_quotes(run_id, (_quote("token-a"), _quote("token-b")))
    gate.enabled = True

    error = _run_after_begin_gate(
        gate,
        clock,
        lambda: store.complete_run(
            run_id,
            completed_at_ms=NOW_MS,
            successful_response_count=2,
        ),
    )

    assert isinstance(error, QuoteRunStateError)
    with sqlite3.connect(quote_db) as con:
        assert con.execute(
            "SELECT status FROM neg_risk_quote_runs WHERE id = ?", (run_id,)
        ).fetchone() == ("collecting",)


def test_init_schema_adds_quote_lease_to_legacy_quote_runs_table(tmp_path) -> None:
    path = tmp_path / "legacy.db"
    with sqlite3.connect(path) as con:
        con.execute(
            "CREATE TABLE neg_risk_quote_runs ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "universe_snapshot_id INTEGER NOT NULL, "
            "universe_taken_at_ms INTEGER NOT NULL, "
            "quoted_at_ms INTEGER NOT NULL, "
            "requested_token_count INTEGER NOT NULL, "
            "successful_response_count INTEGER NOT NULL DEFAULT 0, "
            "status TEXT NOT NULL, failure_reason TEXT, completed_at_ms INTEGER)"
        )

    SQLiteStore(path).init_schema()
    SQLiteStore(path).init_schema()

    with sqlite3.connect(path) as con:
        columns = {row[1] for row in con.execute("PRAGMA table_info(neg_risk_quote_runs)")}
    assert "lease_expires_at_ms" in columns
    assert "source_truth_hash" in columns


def test_latest_complete_projection_has_one_atomic_run_and_all_metadata(quote_db) -> None:
    store = NegRiskQuoteStore(quote_db)
    assert store.latest_complete_projection() is None

    run_id = _begin(store)
    store.record_terminal_quotes(
        run_id,
        (_quote("token-a"), _quote("token-b", terminal_state="missing-ask")),
    )
    store.complete_run(
        run_id,
        completed_at_ms=NOW_MS + 5,
        successful_response_count=2,
    )

    projection = store.latest_complete_projection()

    assert projection is not None
    assert projection.run_id == run_id
    assert projection.universe_snapshot_id == 1
    assert projection.universe_taken_at_ms == NOW_MS - 1_000
    assert projection.quoted_at_ms == NOW_MS
    assert projection.requested_token_count == 2
    assert projection.successful_response_count == 2
    assert [(quote.yes_token_id, quote.terminal_state) for quote in projection.quotes] == [
        ("token-a", "executable"),
        ("token-b", "missing-ask"),
    ]


def test_latest_universe_reads_only_latest_active_complete_membership(quote_db) -> None:
    store = NegRiskQuoteStore(quote_db)

    assert store.latest_universe() == (1, NOW_MS - 1_000, _legs())

    with sqlite3.connect(quote_db) as con:
        con.execute(
            "INSERT INTO snapshots("
            "taken_at_ms, finished_at_ms, mode, market_count, is_valid, parquet_path"
            ") VALUES (?, ?, 'subset', 3, 1, 'newer.parquet')",
            (NOW_MS, NOW_MS),
        )
        latest_id = int(con.execute("SELECT last_insert_rowid()").fetchone()[0])
        con.executemany(
            "INSERT INTO markets("
            "market_id, condition_id, yes_token_id, active, closed, "
            "neg_risk_market_id, fetched_at_ms, snapshot_id"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("closed", "closed-c", "closed-t", 1, 1, "group-b", NOW_MS, latest_id),
                ("no-group", "group-c", "group-t", 1, 0, "", NOW_MS, latest_id),
                ("no-token", "token-c", "", 1, 0, "group-b", NOW_MS, latest_id),
            ],
        )

    assert store.latest_universe() == (latest_id, NOW_MS, ())


def test_latest_verified_universe_excludes_augmented_and_reports_reason(quote_db) -> None:
    with sqlite3.connect(quote_db) as con:
        con.execute(
            "INSERT INTO markets("
            "market_id,condition_id,slug,yes_token_id,active,closed,"
            "neg_risk_market_id,fetched_at_ms,snapshot_id,incomplete,event_id"
            ") VALUES ('market-x','condition-x','x','token-x',1,0,"
            "'group-x',?,?,0,'event-x')",
            (NOW_MS, 1),
        )
        con.execute(
            "INSERT INTO event_market_memberships("
            "snapshot_id,event_id,neg_risk_market_id,market_id,member_kind,active,closed"
            ") VALUES (1,'event-x','group-x','market-x','named',1,0)"
        )
        con.execute(
            "INSERT INTO neg_risk_group_truth("
            "snapshot_id,event_id,neg_risk_market_id,neg_risk_type,"
            "expected_member_count,active_named_count,membership_hash,quality,reason"
            ") VALUES (1,'event-x','group-x','augmented',1,1,'membership-hash-x',"
            "'complete-unsupported','augmented-neg-risk-not-supported')"
        )

    universe = NegRiskQuoteStore(quote_db).latest_verified_universe()

    assert {leg.neg_risk_market_id for leg in universe.legs} == {"group-a"}
    assert {
        (leg.event_id, leg.membership_hash)
        for leg in universe.legs
    } == {(EVENT_ID, MEMBERSHIP_HASH)}
    expected_identity = sorted(
        (
            leg.neg_risk_market_id,
            leg.membership_hash,
            leg.market_id,
            leg.yes_token_id,
        )
        for leg in universe.legs
    )
    assert universe.universe_hash == hashlib.sha256(
        json.dumps(
            expected_identity,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    assert [
        (rejection.group_id, rejection.quality, rejection.reason)
        for rejection in universe.rejections
    ] == [
        (
            "group-x",
            "complete-unsupported",
            "augmented-neg-risk-not-supported",
        )
    ]


def test_latest_verified_universe_ignores_complete_but_unpublished_snapshot(quote_db) -> None:
    with sqlite3.connect(quote_db) as con:
        con.execute(
            "INSERT INTO snapshots("
            "taken_at_ms,finished_at_ms,mode,market_count,is_valid,parquet_path"
            ") VALUES (?,?,'subset',1,0,'diagnostic.parquet')",
            (NOW_MS, NOW_MS),
        )
        diagnostic_id = int(con.execute("SELECT last_insert_rowid()").fetchone()[0])
        con.execute(
            "INSERT INTO snapshot_source_coverage("
            "snapshot_id,completed,market_items,event_items"
            ") VALUES (?,1,1,1)",
            (diagnostic_id,),
        )

    universe = NegRiskQuoteStore(quote_db).latest_verified_universe()

    assert universe.snapshot_id == 1
    assert universe.taken_at_ms == NOW_MS - 1_000


def test_complete_published_zero_market_snapshot_is_valid_empty_universe(tmp_path) -> None:
    path = tmp_path / "empty-published.db"
    snapshot_store = SQLiteStore(path)
    snapshot_store.init_schema()
    snapshot_id = snapshot_store.write_snapshot(
        taken_at_ms=NOW_MS,
        finished_at_ms=NOW_MS + 1,
        mode="subset",
        parquet_path="empty.parquet",
        is_valid=True,
        market_rows=[],
        issues=[],
        source_coverage=SourceCoverage.complete(0, 0),
        event_members=[],
            group_truths=[],
            publish_markets=True,
            data_product="structure",
            archive_status="not_requested",
        )

    universe = NegRiskQuoteStore(path).latest_verified_universe()

    assert universe.snapshot_id == snapshot_id
    assert universe.legs == ()
    assert universe.rejections == ()


def test_blank_membership_hash_rejects_supported_group(quote_db) -> None:
    with sqlite3.connect(quote_db) as con:
        con.execute("UPDATE neg_risk_group_truth SET membership_hash=''")

    universe = NegRiskQuoteStore(quote_db).latest_verified_universe()

    assert universe.legs == ()
    assert universe.rejections[0].reason == "membership-market-mismatch"


@pytest.mark.parametrize(
    "unverified_kind",
    ["augmented", "incomplete", "raw", "blank-membership"],
)
def test_begin_run_rejects_unverified_market_membership(
    quote_db,
    unverified_kind: str,
) -> None:
    with sqlite3.connect(quote_db) as con:
        if unverified_kind == "augmented":
            con.execute(
                "UPDATE neg_risk_group_truth SET neg_risk_type='augmented',"
                "quality='complete-unsupported',"
                "reason='augmented-neg-risk-not-supported'"
            )
        elif unverified_kind == "incomplete":
            con.execute("UPDATE snapshot_source_coverage SET completed=0")
        elif unverified_kind == "blank-membership":
            con.execute("UPDATE neg_risk_group_truth SET membership_hash=''")
        else:
            con.execute("DELETE FROM neg_risk_group_truth")
            con.execute("DELETE FROM event_market_memberships")
            con.execute("DELETE FROM snapshot_source_coverage")

    with pytest.raises(QuoteRunStateError):
        NegRiskQuoteStore(quote_db).begin_run(
            universe_snapshot_id=1,
            universe_taken_at_ms=NOW_MS - 1_000,
            legs=_legs(),
            quoted_at_ms=NOW_MS,
        )


def test_missing_required_market_rejects_whole_standard_group(quote_db) -> None:
    with sqlite3.connect(quote_db) as con:
        con.execute("DELETE FROM markets WHERE market_id='market-b'")

    universe = NegRiskQuoteStore(quote_db).latest_verified_universe()

    assert universe.legs == ()
    assert [
        (rejection.group_id, rejection.quality, rejection.reason)
        for rejection in universe.rejections
    ] == [
        (
            "group-a",
            "complete-supported",
            "membership-market-mismatch",
        )
    ]


def test_verified_run_persists_universe_event_and_membership_identity(quote_db) -> None:
    store = NegRiskQuoteStore(quote_db)
    with sqlite3.connect(quote_db) as con:
        con.execute(
            "INSERT INTO neg_risk_group_truth("
            "snapshot_id,event_id,neg_risk_market_id,neg_risk_type,"
            "expected_member_count,active_named_count,membership_hash,quality,reason"
            ") VALUES (1,'event-z','group-z','augmented',1,1,'membership-z',"
            "'complete-unsupported','augmented-neg-risk-not-supported')"
        )
    universe = store.latest_verified_universe()

    run_id = store.begin_verified_run(universe, quoted_at_ms=NOW_MS)
    store.record_terminal_quotes(
        run_id,
        tuple(
            PersistedQuote(
                leg.neg_risk_market_id,
                leg.market_id,
                leg.condition_id,
                leg.slug,
                leg.yes_token_id,
                "missing-book",
                None,
                None,
                event_id=leg.event_id,
                membership_hash=leg.membership_hash,
            )
            for leg in universe.legs
        ),
    )
    completed = store.complete_run(
        run_id,
        completed_at_ms=NOW_MS + 1,
        successful_response_count=0,
    )

    assert completed.universe_hash == universe.universe_hash
    with sqlite3.connect(quote_db) as con:
        expected_source_truth = hashlib.sha256(
            json.dumps(
                [
                    universe.universe_hash,
                    sorted(
                        (item.group_id, item.quality, item.reason)
                        for item in universe.rejections
                    ),
                ],
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        assert con.execute(
            "SELECT universe_hash,source_truth_hash "
            "FROM neg_risk_quote_runs WHERE id=?",
            (run_id,),
        ).fetchone() == (universe.universe_hash, expected_source_truth)
        assert con.execute(
            "SELECT DISTINCT event_id,membership_hash "
            "FROM neg_risk_quote_run_legs WHERE quote_run_id=?",
            (run_id,),
        ).fetchall() == [(EVENT_ID, MEMBERSHIP_HASH)]
        assert con.execute(
            "SELECT DISTINCT event_id,membership_hash "
            "FROM neg_risk_quotes WHERE quote_run_id=?",
            (run_id,),
        ).fetchall() == [(EVENT_ID, MEMBERSHIP_HASH)]


@pytest.mark.parametrize(
    "corruption_sql",
    [
        (
            "UPDATE neg_risk_group_truth "
            "SET reason='standard-neg-risk-not-supported' "
            "WHERE neg_risk_market_id='group-z'"
        ),
        (
            "UPDATE neg_risk_group_truth "
            "SET quality='incomplete-quotes',reason='incomplete-quotes' "
            "WHERE neg_risk_market_id='group-z'"
        ),
        "DELETE FROM neg_risk_group_truth WHERE neg_risk_market_id='group-z'",
        (
            "INSERT INTO neg_risk_group_truth("
            "snapshot_id,event_id,neg_risk_market_id,neg_risk_type,"
            "expected_member_count,active_named_count,membership_hash,quality,reason"
            ") VALUES (1,'event-y','group-y','augmented',1,1,'membership-y',"
            "'complete-unsupported','augmented-neg-risk-not-supported')"
        ),
    ],
)
def test_complete_projection_rejects_source_rejection_truth_drift(
    quote_db,
    corruption_sql: str,
) -> None:
    with sqlite3.connect(quote_db) as con:
        con.execute(
            "INSERT INTO neg_risk_group_truth("
            "snapshot_id,event_id,neg_risk_market_id,neg_risk_type,"
            "expected_member_count,active_named_count,membership_hash,quality,reason"
            ") VALUES (1,'event-z','group-z','augmented',1,1,'membership-z',"
            "'complete-unsupported','augmented-neg-risk-not-supported')"
        )
    store = NegRiskQuoteStore(quote_db)
    _complete(store)
    with sqlite3.connect(quote_db) as con:
        con.execute(corruption_sql)

    with pytest.raises(QuoteProjectionIntegrityError):
        store.latest_complete_projection()


def test_complete_projection_excludes_legacy_blank_source_truth_hash(quote_db) -> None:
    store = NegRiskQuoteStore(quote_db)
    _complete(store)
    with sqlite3.connect(quote_db) as con:
        con.execute("UPDATE neg_risk_quote_runs SET source_truth_hash=''")

    assert store.latest_complete_run() is None
    assert store.latest_complete_projection() is None


def test_complete_projection_uses_one_read_connection(
    quote_db,
    monkeypatch,
) -> None:
    store = NegRiskQuoteStore(quote_db)
    _complete(store)
    real_connect = sqlite3.connect
    connections: list[sqlite3.Connection] = []

    def tracked_connect(*args, **kwargs):
        connection = real_connect(*args, **kwargs)
        connections.append(connection)
        return connection

    monkeypatch.setattr(sqlite3, "connect", tracked_connect)

    projection = store.latest_complete_projection()

    assert projection is not None
    assert len(connections) == 1


@pytest.mark.parametrize(
    "corruption_sql",
    [
        "UPDATE neg_risk_quote_runs SET universe_hash=''",
        "UPDATE neg_risk_quote_runs SET universe_hash='truncated'",
        (
            "UPDATE neg_risk_quote_run_legs SET condition_id='forged-condition' "
            "WHERE yes_token_id='token-a'"
        ),
        (
            "UPDATE neg_risk_quotes SET event_id='forged-event' "
            "WHERE yes_token_id='token-a'"
        ),
        (
            "UPDATE neg_risk_quotes SET event_id='' "
            "WHERE yes_token_id='token-a'"
        ),
        "DELETE FROM neg_risk_quotes WHERE yes_token_id='token-a'",
        (
            "INSERT INTO neg_risk_quotes("
            "quote_run_id,neg_risk_market_id,event_id,membership_hash,market_id,"
            "condition_id,slug,yes_token_id,terminal_state,best_ask_price,best_ask_size"
            ") VALUES (1,'group-a',?,?, 'extra-market','extra-condition','extra',"
            "'extra-token','missing-book',NULL,NULL)"
        ),
        (
            "UPDATE markets SET condition_id='source-condition-drift' "
            "WHERE market_id='market-a'"
        ),
    ],
)
def test_complete_projection_rejects_any_cross_chain_identity_or_count_drift(
    quote_db,
    corruption_sql: str,
) -> None:
    store = NegRiskQuoteStore(quote_db)
    _complete(store)
    with sqlite3.connect(quote_db) as con:
        if "VALUES (1,'group-a',?" in corruption_sql:
            con.execute(corruption_sql, (EVENT_ID, MEMBERSHIP_HASH))
        else:
            con.execute(corruption_sql)

    with pytest.raises(QuoteProjectionIntegrityError):
        store.latest_complete_projection()


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("best_ask_price", float("nan")),
        ("best_ask_price", float("inf")),
        ("best_ask_size", float("nan")),
        ("best_ask_size", float("inf")),
        ("best_ask_size", "not-a-number"),
    ],
)
def test_complete_projection_rejects_nonfinite_or_nonnumeric_executable_quotes(
    quote_db,
    column: str,
    value: object,
) -> None:
    store = NegRiskQuoteStore(quote_db)
    _complete(store)
    with sqlite3.connect(quote_db) as con:
        con.execute("PRAGMA ignore_check_constraints=ON")
        con.execute(
            f"UPDATE neg_risk_quotes SET {column}=? WHERE yes_token_id='token-a'",
            (value,),
        )

    with pytest.raises(QuoteProjectionIntegrityError):
        store.latest_complete_projection()


@pytest.mark.parametrize("legacy_hash", ["", "a" * 64])
def test_latest_complete_run_ignores_newer_legacy_unverified_identity(
    quote_db,
    legacy_hash: str,
) -> None:
    store = NegRiskQuoteStore(quote_db)
    verified_id = _complete(store)
    with sqlite3.connect(quote_db) as con:
        cursor = con.execute(
            "INSERT INTO neg_risk_quote_runs("
            "universe_snapshot_id,universe_taken_at_ms,universe_hash,quoted_at_ms,"
            "requested_token_count,successful_response_count,lease_expires_at_ms,"
            "status,completed_at_ms"
            ") VALUES (1,?,?,?,1,0,0,'complete',?)",
            (NOW_MS - 1_000, legacy_hash, NOW_MS + 10, NOW_MS + 11),
        )
        legacy_id = int(cursor.lastrowid)
        con.execute(
            "INSERT INTO neg_risk_quote_run_legs("
            "quote_run_id,neg_risk_market_id,event_id,membership_hash,"
            "market_id,condition_id,slug,yes_token_id"
            ") VALUES (?,'group-a','','','legacy-market','legacy-condition',"
            "'legacy','legacy-token')",
            (legacy_id,),
        )
        con.execute(
            "INSERT INTO neg_risk_quotes("
            "quote_run_id,neg_risk_market_id,event_id,membership_hash,"
            "market_id,condition_id,slug,yes_token_id,terminal_state,"
            "best_ask_price,best_ask_size"
            ") VALUES (?,'group-a','','','legacy-market','legacy-condition',"
            "'legacy','legacy-token','missing-book',NULL,NULL)",
            (legacy_id,),
        )

    latest = store.latest_complete_run()

    assert latest is not None
    assert latest.run_id == verified_id


def test_begin_rejects_duplicate_token_with_inconsistent_identity(quote_db) -> None:
    store = NegRiskQuoteStore(quote_db)
    conflicting = _legs() + (
        UniverseLeg("other-group", "other-market", "other-condition", "other", "token-a"),
    )

    with pytest.raises(ValueError, match="inconsistent identity"):
        store.begin_run(
            universe_snapshot_id=1,
            universe_taken_at_ms=NOW_MS,
            legs=conflicting,
            quoted_at_ms=NOW_MS,
        )


@pytest.mark.parametrize(
    "forged_leg",
    [
        UniverseLeg("forged-group", "market-a", "condition-a", "alpha", "token-a"),
        UniverseLeg("group-a", "forged-market", "condition-a", "alpha", "token-a"),
    ],
)
def test_begin_rejects_forged_snapshot_leg_identity(quote_db, forged_leg: UniverseLeg) -> None:
    store = NegRiskQuoteStore(quote_db)
    requested = (forged_leg, _legs()[1])

    with pytest.raises(QuoteRunStateError, match="snapshot membership"):
        store.begin_run(
            universe_snapshot_id=1,
            universe_taken_at_ms=NOW_MS - 1_000,
            legs=requested,
            quoted_at_ms=NOW_MS,
        )


def test_begin_rejects_mismatched_snapshot_taken_timestamp(quote_db) -> None:
    store = NegRiskQuoteStore(quote_db)

    with pytest.raises(QuoteRunStateError, match="taken_at_ms"):
        store.begin_run(
            universe_snapshot_id=1,
            universe_taken_at_ms=NOW_MS - 999,
            legs=_legs(),
            quoted_at_ms=NOW_MS,
        )
