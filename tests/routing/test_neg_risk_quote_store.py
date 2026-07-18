from __future__ import annotations

import sqlite3

import pytest

from polyarb.routing.neg_risk_quote_store import (
    QUOTE_RUN_LEASE_MS,
    NegRiskQuoteStore,
    PersistedQuote,
    QuoteRunBusyError,
    QuoteRunStateError,
    UniverseLeg,
)
from polyarb.storage.sqlite_store import SQLiteStore

NOW_MS = 1_700_000_000_000


@pytest.fixture
def quote_db(tmp_path):
    path = tmp_path / "state.db"
    SQLiteStore(path).init_schema()
    with sqlite3.connect(path) as con:
        con.execute(
            "INSERT INTO snapshots("
            "taken_at_ms, finished_at_ms, mode, market_count, is_valid, parquet_path"
            ") VALUES (?, ?, 'subset', 2, 1, 'fixture.parquet')",
            (NOW_MS - 1_000, NOW_MS - 900),
        )
        snapshot_id = int(con.execute("SELECT last_insert_rowid()").fetchone()[0])
        con.executemany(
            "INSERT INTO markets("
            "market_id, condition_id, slug, yes_token_id, active, closed, "
            "neg_risk_market_id, fetched_at_ms, snapshot_id"
            ") VALUES (?, ?, ?, ?, 1, 0, ?, ?, ?)",
            [
                ("market-a", "condition-a", "alpha", "token-a", "group-a", NOW_MS, snapshot_id),
                ("market-b", "condition-b", "beta", "token-b", "group-a", NOW_MS, snapshot_id),
            ],
        )
    return path


def _legs() -> tuple[UniverseLeg, ...]:
    return (
        UniverseLeg("group-a", "market-a", "condition-a", "alpha", "token-a"),
        UniverseLeg("group-a", "market-b", "condition-b", "beta", "token-b"),
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
    store.complete_run(run_id, completed_at_ms=NOW_MS + 1)
    return run_id


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
        store.complete_run(run_id, completed_at_ms=NOW_MS + 1)

    with sqlite3.connect(quote_db) as con:
        status = con.execute(
            "SELECT status FROM neg_risk_quote_runs WHERE id = ?", (run_id,)
        ).fetchone()[0]
    assert status == "collecting"


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
        store.complete_run(run_id, completed_at_ms=NOW_MS + QUOTE_RUN_LEASE_MS)

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
            "universe_taken_at_ms INTEGER NOT NULL, quoted_at_ms INTEGER NOT NULL, "
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

    with sqlite3.connect(path) as con:
        columns = {row[1] for row in con.execute("PRAGMA table_info(neg_risk_quote_runs)")}
    assert "lease_expires_at_ms" in columns


def test_latest_complete_projection_has_one_atomic_run_and_all_metadata(quote_db) -> None:
    store = NegRiskQuoteStore(quote_db)
    assert store.latest_complete_projection() is None

    run_id = _begin(store)
    store.record_terminal_quotes(
        run_id,
        (_quote("token-a"), _quote("token-b", terminal_state="missing-ask")),
    )
    store.complete_run(run_id, completed_at_ms=NOW_MS + 5)

    projection = store.latest_complete_projection()

    assert projection is not None
    assert projection.run_id == run_id
    assert projection.universe_snapshot_id == 1
    assert projection.universe_taken_at_ms == NOW_MS - 1_000
    assert projection.quoted_at_ms == NOW_MS
    assert projection.requested_token_count == 2
    assert projection.successful_response_count == 1
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
