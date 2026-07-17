"""Position tracker tests — T5 fill→close→PnL→stop-loss chain.

T5 Revision 9 (2026-06-06 SESSION 37) — Position Tracker realization.

The tracker existed pre-T5 (open_position, close_position primitives) but was
untested and had a latent bug in PositionSnapshot.roi_pct. T4 only wired
`open_position`; nothing closed positions, so `make status-arb` never showed
realized PnL or stop-loss state.

T5 locks the lifecycle:
  open → update_prices → close_with_fill → realized PnL → stop-loss check

Each test class isolates one slice. Bug-fix tests (T5.1) come first because
they unblock the rest.
"""
from __future__ import annotations

import pytest

import polyarb.routing.position_tracker as tracker_module
from polyarb.routing.config import PositionConfig
from polyarb.routing.money import Money
from polyarb.routing.position_repository import (
    SettlementReceipt,
    SQLitePositionRepository,
)
from polyarb.routing.position_tracker import (
    Fill,
    PositionSnapshot,
    PositionTracker,
    StopLossEvent,
)

# ---------------------------------------------------------------------------
# T5.1 — PositionSnapshot.roi_pct bug (pre-existing, surfaces in any snapshot)
# ---------------------------------------------------------------------------


class TestPositionSnapshotRoiPctBug:
    def test_roi_pct_does_not_raise_attribute_error(self):
        """Pre-T5 bug: roi_pct referenced self.snapshot_balance (field is `balance`).

        Any caller that builds a snapshot and reads roi_pct would crash. The
        CLI status path almost touched this — only the dict-only projection
        in cli_arbitrage.status() saved us. T5 must fix the dataclass itself.
        """
        snap = PositionSnapshot(
            balance=950.0,
            total_unrealized_pnl=10.0,
            total_realized_pnl=-5.0,
            open_positions=1,
            max_exposure=50.0,
        )
        # Must NOT raise. Pre-fix this raises AttributeError: 'snapshot_balance'.
        roi = snap.roi_pct
        assert isinstance(roi, float)

    def test_roi_pct_uses_balance_field(self):
        """roi_pct = total_pnl / balance × 100. Balance is the snapshot's own balance field."""
        snap = PositionSnapshot(
            balance=1000.0,
            total_unrealized_pnl=20.0,
            total_realized_pnl=30.0,
            open_positions=2,
            max_exposure=200.0,
        )
        # total_pnl = 50, balance = 1000 → 5%
        assert snap.roi_pct == pytest.approx(5.0)

    def test_roi_pct_zero_balance_returns_zero(self):
        snap = PositionSnapshot(
            balance=0.0,
            total_unrealized_pnl=0.0,
            total_realized_pnl=0.0,
            open_positions=0,
            max_exposure=0.0,
        )
        assert snap.roi_pct == 0.0


# ---------------------------------------------------------------------------
# T5.2 — Fill → close_position_with_fill lifecycle
# ---------------------------------------------------------------------------


class TestCloseWithFillLifecycle:
    """The production close path: a Fill event closes a position, books PnL."""

    def _open(self, tracker: PositionTracker, side: str = "BUY", entry: float = 0.50) -> str:
        market_id = "asset-1"
        ok = tracker.open_position(
            market_id=market_id,
            condition_id="cond-1",
            side=side,
            outcome="yes",
            stake=100.0,
            price=entry,
        )
        assert ok, "open_position pre-condition failed"
        return market_id

    def test_buy_close_with_profit_books_realized_pnl(self):
        tracker = PositionTracker(PositionConfig(initial_balance=1000.0))
        market_id = self._open(tracker, side="BUY", entry=0.50)
        # Higher exit → profit on BUY.
        fill = Fill(market_id=market_id, exit_price=0.60, filled_size=100.0)
        pnl = tracker.close_position_with_fill(fill)
        # BUY PnL = stake × (exit - entry) = 100 × 0.10 = 10
        assert pnl == pytest.approx(10.0)
        assert tracker.total_realized_pnl == pytest.approx(10.0)
        assert tracker.open_count == 0
        # Balance restored: initial 1000 − stake 100 (open) + stake 100 + pnl 10 = 1010
        assert tracker.balance == pytest.approx(1010.0)

    def test_buy_close_with_loss_books_negative_pnl(self):
        tracker = PositionTracker(PositionConfig(initial_balance=1000.0))
        market_id = self._open(tracker, side="BUY", entry=0.50)
        fill = Fill(market_id=market_id, exit_price=0.40, filled_size=100.0)
        pnl = tracker.close_position_with_fill(fill)
        assert pnl == pytest.approx(-10.0)
        assert tracker.balance == pytest.approx(990.0)

    def test_sell_close_with_profit_books_realized_pnl(self):
        tracker = PositionTracker(PositionConfig(initial_balance=1000.0))
        market_id = self._open(tracker, side="SELL", entry=0.60)
        # Lower exit → profit on SELL.
        fill = Fill(market_id=market_id, exit_price=0.50, filled_size=100.0)
        pnl = tracker.close_position_with_fill(fill)
        # SELL PnL = stake × (entry - exit) = 100 × 0.10 = 10
        assert pnl == pytest.approx(10.0)
        assert tracker.balance == pytest.approx(1010.0)

    def test_close_unknown_market_returns_zero_and_does_not_change_balance(self):
        tracker = PositionTracker(PositionConfig(initial_balance=1000.0))
        fill = Fill(market_id="never-opened", exit_price=0.5, filled_size=50.0)
        pnl = tracker.close_position_with_fill(fill)
        assert pnl == 0.0
        assert tracker.balance == pytest.approx(1000.0)
        assert tracker.total_realized_pnl == 0.0

    def test_close_with_fill_returns_position_and_clears_open_set(self):
        tracker = PositionTracker(PositionConfig(initial_balance=1000.0))
        market_id = self._open(tracker)
        assert market_id in [p.market_id for p in tracker.open_positions()]
        fill = Fill(market_id=market_id, exit_price=0.55, filled_size=100.0)
        tracker.close_position_with_fill(fill)
        assert market_id not in [p.market_id for p in tracker.open_positions()]

    def test_close_with_partial_fill_size_raises_until_partial_supported(self):
        """T5 explicitly does NOT support partial fills (out of scope per plan).

        If a fill arrives with filled_size != stake, we reject loudly rather
        than silently book a wrong PnL. Partial fill aggregation is T5+1.
        """
        tracker = PositionTracker(PositionConfig(initial_balance=1000.0))
        market_id = self._open(tracker)  # stake=100
        fill = Fill(market_id=market_id, exit_price=0.55, filled_size=50.0)
        with pytest.raises(ValueError, match="partial fill"):
            tracker.close_position_with_fill(fill)
        # Position must still be open after the rejected close.
        assert tracker.open_count == 1


# ---------------------------------------------------------------------------
# T5.3 — Stop-loss trigger chain (return rich event, not bare bool)
# ---------------------------------------------------------------------------


class TestStopLossTriggerChain:
    def test_stop_loss_disabled_returns_none(self):
        cfg = PositionConfig(initial_balance=1000.0, enable_pnl_stop=False, stop_loss_pct=5.0)
        tracker = PositionTracker(cfg)
        # Even a huge realized loss should not trigger when disabled.
        market_id = "asset-1"
        tracker.open_position(market_id, "cond-1", "BUY", "yes", 500.0, 0.50)
        tracker.close_position_with_fill(
            Fill(market_id=market_id, exit_price=0.10, filled_size=500.0)
        )
        assert tracker.check_stop_loss_event() is None

    def test_realized_loss_within_threshold_returns_none(self):
        cfg = PositionConfig(initial_balance=1000.0, enable_pnl_stop=True, stop_loss_pct=5.0)
        tracker = PositionTracker(cfg)
        market_id = "asset-1"
        tracker.open_position(market_id, "cond-1", "BUY", "yes", 100.0, 0.50)
        # Lose $10 → 1% of balance, below 5% threshold.
        tracker.close_position_with_fill(
            Fill(market_id=market_id, exit_price=0.40, filled_size=100.0)
        )
        event = tracker.check_stop_loss_event()
        assert event is None

    def test_realized_loss_at_threshold_returns_event(self):
        cfg = PositionConfig(initial_balance=1000.0, enable_pnl_stop=True, stop_loss_pct=5.0)
        tracker = PositionTracker(cfg)
        market_id = "asset-1"
        tracker.open_position(market_id, "cond-1", "BUY", "yes", 500.0, 0.50)
        # Lose $50 → exactly 5% of balance.
        tracker.close_position_with_fill(
            Fill(market_id=market_id, exit_price=0.40, filled_size=500.0)
        )
        event = tracker.check_stop_loss_event()
        assert isinstance(event, StopLossEvent)
        assert event.loss_pct == pytest.approx(5.0)
        assert event.realized_pnl == pytest.approx(-50.0)
        assert event.recommendation == "halt_new_signals"

    def test_profit_never_triggers_stop_loss(self):
        cfg = PositionConfig(initial_balance=1000.0, enable_pnl_stop=True, stop_loss_pct=5.0)
        tracker = PositionTracker(cfg)
        market_id = "asset-1"
        tracker.open_position(market_id, "cond-1", "BUY", "yes", 500.0, 0.50)
        tracker.close_position_with_fill(
            Fill(market_id=market_id, exit_price=0.70, filled_size=500.0)
        )
        assert tracker.check_stop_loss_event() is None

    def test_legacy_bool_form_still_works_via_bool_event(self):
        """Backward-compat: callers using `if tracker.check_stop_loss():` still work.

        The legacy method exists, returns bool, computed from check_stop_loss_event.
        """
        cfg = PositionConfig(initial_balance=1000.0, enable_pnl_stop=True, stop_loss_pct=5.0)
        tracker = PositionTracker(cfg)
        market_id = "asset-1"
        tracker.open_position(market_id, "cond-1", "BUY", "yes", 500.0, 0.50)
        tracker.close_position_with_fill(
            Fill(market_id=market_id, exit_price=0.40, filled_size=500.0)
        )
        assert tracker.check_stop_loss() is True


# ---------------------------------------------------------------------------
# T5.4 — ExecutionEngine wires close path on successful execution
# ---------------------------------------------------------------------------
# These live in tests/execution/test_engine.py to stay co-located with other
# engine tests; declared here as a placeholder reference. See:
#   tests/execution/test_engine.py::TestExecutionEngineClosePath
# ---------------------------------------------------------------------------


class TestRepositoryBackedTracker:
    def test_two_trackers_observe_the_same_open_and_close(self, tmp_path):
        path = tmp_path / "positions.db"
        first = PositionTracker(
            repository=SQLitePositionRepository(path, initial_balance=1000.0)
        )
        second = PositionTracker(
            repository=SQLitePositionRepository(path, initial_balance=1000.0)
        )

        assert first.open_position(
            "m1",
            "c1",
            "BUY",
            "YES",
            100.0,
            0.4,
            leg_id="l1",
            operation_id="open:s1:l1",
        )
        assert second.open_count == 1

        pnl = second.close_position_with_fill(
            Fill("m1", 0.5, 100.0), operation_id="close:f1"
        )

        assert pnl == pytest.approx(10.0)
        assert first.open_count == 0
        assert first.balance == pytest.approx(1010.0)
        assert first.total_realized_pnl == pytest.approx(10.0)

    def test_persisted_position_timestamp_is_timezone_aware(self, tmp_path):
        tracker = PositionTracker(
            repository=SQLitePositionRepository(
                tmp_path / "positions.db", initial_balance=1000.0
            )
        )
        tracker.open_position(
            "m1", "c1", "BUY", "YES", 100.0, 0.4, operation_id="open:m1"
        )

        assert tracker.open_positions()[0].opened_at.utcoffset() is not None

    def test_duplicate_operation_ids_do_not_double_book(self, tmp_path):
        tracker = PositionTracker(
            repository=SQLitePositionRepository(
                tmp_path / "positions.db", initial_balance=1000.0
            )
        )

        for _ in range(2):
            assert tracker.open_position(
                "m1",
                "c1",
                "BUY",
                "YES",
                100.0,
                0.4,
                operation_id="open:s1:l1",
            )
        assert tracker.balance == pytest.approx(960.0)
        assert tracker.open_count == 1

        fill = Fill("m1", 0.5, 100.0)
        assert tracker.close_position_with_fill(
            fill, operation_id="close:f1"
        ) == pytest.approx(10.0)
        assert tracker.close_position_with_fill(
            fill, operation_id="close:f1"
        ) == pytest.approx(10.0)
        assert tracker.balance == pytest.approx(1010.0)
        assert tracker.total_realized_pnl == pytest.approx(10.0)

    def test_tracker_exposes_committed_close_receipt(self, tmp_path):
        tracker = PositionTracker(
            repository=SQLitePositionRepository(
                tmp_path / "positions.db", initial_balance=1000.0
            )
        )
        tracker.open_position(
            "m1", "c1", "BUY", "YES", 100.0, 0.4, operation_id="open:m1"
        )
        tracker.close_position_with_fill(
            Fill("m1", 0.5, 100.0), operation_id="close:f1"
        )

        receipt = tracker.operation_receipt("close:f1")

        assert receipt is not None
        assert receipt.operation_id == "close:f1"
        assert receipt.operation_type == "close"
        assert receipt.target_id == "m1"
        assert receipt.result == Money.from_value("10")
        assert tracker.operation_receipt("unknown") is None

    def test_rejected_open_and_partial_fill_leave_durable_state_unchanged(
        self, tmp_path
    ):
        config = PositionConfig(initial_balance=1000.0, max_total_exposure=40.0)
        tracker = PositionTracker(
            config,
            repository=SQLitePositionRepository(
                tmp_path / "positions.db", initial_balance=1000.0
            ),
        )
        assert tracker.open_position(
            "m1", "c1", "BUY", "YES", 100.0, 0.4, operation_id="open:m1"
        )

        assert not tracker.open_position(
            "m2", "c2", "BUY", "YES", 1.0, 0.4, operation_id="open:m2"
        )
        with pytest.raises(ValueError, match="partial fill"):
            tracker.close_position_with_fill(
                Fill("m1", 0.5, 50.0), operation_id="close:partial"
            )

        assert tracker.balance == pytest.approx(960.0)
        assert [position.market_id for position in tracker.open_positions()] == ["m1"]
        assert tracker.total_realized_pnl == 0.0


class TestExactCashDomain:
    def test_repeated_decimal_closes_accumulate_exact_money(self) -> None:
        tracker = PositionTracker(PositionConfig(initial_balance=1000.0))

        for index in range(2):
            market_id = f"m{index}"
            assert tracker.open_position(
                market_id,
                f"c{index}",
                "BUY",
                "YES",
                100.0,
                0.4,
                operation_id=f"open:{index}",
            )
            assert tracker.close_position_with_fill(
                Fill(market_id, 0.5, 100.0),
                operation_id=f"close:{index}",
            ) == pytest.approx(10.0)

        state = tracker.repository.load()
        assert state.balance_money.micros == 1_020_000_000
        assert state.snapshot_balance_money.micros == 1_000_000_000
        assert state.realized_pnl_money.micros == 20_000_000
        assert tracker.balance == 1020.0
        assert tracker.total_realized_pnl == 20.0

    def test_open_quantizes_stake_once_before_balance_check(self) -> None:
        tracker = PositionTracker(PositionConfig(initial_balance=0.3))

        assert tracker.open_position("m1", "c1", "BUY", "YES", 0.3000004, 0.4)

        position = tracker.open_positions()[0]
        assert position.quantity_value.micros == 300_000
        assert position.cost_basis_money.micros == 120_000
        assert position.stake == 0.3
        assert tracker.repository.load().balance_money.micros == 180_000

    def test_exposure_limit_compares_quantized_stakes(self) -> None:
        tracker = PositionTracker(
            PositionConfig(initial_balance=1.0, max_total_exposure=0.3)
        )

        assert tracker.open_position("m1", "c1", "BUY", "YES", 0.1, 0.4)
        assert tracker.open_position("m2", "c2", "BUY", "YES", 0.2, 0.4)
        assert tracker.snapshot().max_exposure == 0.12

    def test_full_fill_equality_uses_quantized_money(self) -> None:
        tracker = PositionTracker(PositionConfig(initial_balance=1.0))
        assert tracker.open_position("m1", "c1", "BUY", "YES", 0.3, 0.4)

        pnl = tracker.close_position_with_fill(
            Fill("m1", 0.5, 0.1 + 0.2), operation_id="close:m1"
        )

        assert pnl == pytest.approx(0.03)
        assert tracker.open_count == 0
        assert tracker.repository.load().realized_pnl_money.micros == 30_000


class TestDurablePartialFillAccounting:
    def _open(self, tracker: PositionTracker) -> None:
        assert tracker.open_position(
            "m1",
            "c1",
            "BUY",
            "YES",
            price=0.4,
            quantity=100,
            operation_id="open:m1",
        )

    def test_two_partial_fills_release_cost_basis_and_close_residual(self) -> None:
        tracker = PositionTracker(PositionConfig(initial_balance=1000.0))
        self._open(tracker)

        first_pnl = tracker.close_position_with_fill(
            Fill("m1", 0.45, filled_quantity=30, fill_id="fill-1")
        )

        remaining = tracker.open_positions()[0]
        assert first_pnl == pytest.approx(1.5)
        assert remaining.quantity == 70.0
        assert remaining.cost_basis == 28.0
        assert tracker.balance == pytest.approx(973.5)
        assert tracker.total_realized_pnl == pytest.approx(1.5)

        second_pnl = tracker.close_position_with_fill(
            Fill("m1", 0.50, filled_quantity=70, fill_id="fill-2")
        )

        assert second_pnl == pytest.approx(7.0)
        assert tracker.open_count == 0
        assert tracker.balance == pytest.approx(1008.5)
        assert tracker.total_realized_pnl == pytest.approx(8.5)

    def test_fill_id_is_canonical_across_different_caller_operation_ids(
        self, tmp_path
    ) -> None:
        path = tmp_path / "positions.db"
        tracker = PositionTracker(
            repository=SQLitePositionRepository(path, initial_balance=1000.0)
        )
        self._open(tracker)
        fill = Fill("m1", 0.45, filled_quantity=30, fill_id="venue-123")

        first = tracker.close_position_with_fill(fill, operation_id="caller:a")
        replay = PositionTracker(
            repository=SQLitePositionRepository(path, initial_balance=1000.0)
        ).close_position_with_fill(fill, operation_id="caller:b")

        assert first == replay == pytest.approx(1.5)
        state = tracker.repository.load()
        assert state.open_positions["m1"].quantity == 70.0
        assert state.balance == pytest.approx(973.5)
        receipt = tracker.operation_receipt("venue-fill:venue-123")
        assert receipt is not None
        assert receipt.result == Money.from_value("1.5")

    @pytest.mark.parametrize(
        ("changed_quantity", "changed_price"),
        [(31, 0.45), (30, 0.46)],
    )
    def test_modeled_fill_id_rejects_changed_payload(
        self, tmp_path, changed_quantity: float, changed_price: float
    ) -> None:
        path = tmp_path / "positions.db"
        tracker = PositionTracker(
            repository=SQLitePositionRepository(path, initial_balance=1000.0)
        )
        self._open(tracker)
        tracker.close_position_with_fill(
            Fill("m1", 0.45, filled_quantity=30, fill_id="venue-123")
        )

        with pytest.raises(ValueError, match="operation identity conflict"):
            tracker.close_position_with_fill(
                Fill(
                    "m1",
                    changed_price,
                    filled_quantity=changed_quantity,
                    fill_id="venue-123",
                )
            )

        state = tracker.repository.load()
        assert state.open_positions["m1"].quantity == 70.0
        assert state.balance == pytest.approx(973.5)

    def test_anonymous_partial_fill_fails_without_mutation(self) -> None:
        tracker = PositionTracker(PositionConfig(initial_balance=1000.0))
        self._open(tracker)

        with pytest.raises(ValueError, match="fill_id"):
            tracker.close_position_with_fill(
                Fill("m1", 0.45, filled_quantity=30),
                operation_id="close:anonymous",
            )

        assert tracker.balance == 960.0
        assert tracker.open_positions()[0].quantity == 100.0
        assert tracker.operation_receipt("close:anonymous") is None

    @pytest.mark.parametrize("filled_quantity", [0, 101])
    def test_zero_or_overfill_fails_without_mutation(
        self, filled_quantity: float
    ) -> None:
        tracker = PositionTracker(PositionConfig(initial_balance=1000.0))
        self._open(tracker)

        with pytest.raises(ValueError, match="fill quantity"):
            tracker.close_position_with_fill(
                Fill(
                    "m1",
                    0.45,
                    filled_quantity=filled_quantity,
                    fill_id=f"bad-{filled_quantity}",
                )
            )

        assert tracker.balance == 960.0
        assert tracker.open_positions()[0].quantity == 100.0

    def test_final_micro_fill_consumes_all_cost_basis_residual(self) -> None:
        tracker = PositionTracker(PositionConfig(initial_balance=1.0))
        assert tracker.open_position(
            "m1", "c1", "BUY", "YES", price=0.333333, quantity=0.000003
        )

        for index in range(2):
            tracker.close_position_with_fill(
                Fill(
                    "m1",
                    0.333333,
                    filled_quantity=0.000001,
                    fill_id=f"micro-{index}",
                )
            )
        remaining = tracker.open_positions()[0]
        assert remaining.quantity_value.micros == 1

        tracker.close_position_with_fill(
            Fill(
                "m1",
                0.333333,
                filled_quantity=0.000001,
                fill_id="micro-final",
            )
        )

        assert tracker.open_count == 0
        assert tracker.balance == 1.0


class TestVenueTruthReconciliation:
    @staticmethod
    def _open(tracker: PositionTracker) -> None:
        assert tracker.open_position(
            "m1",
            "c1",
            "BUY",
            "YES",
            price=0.4,
            quantity=100,
            operation_id="open:m1",
        )

    @staticmethod
    def _settlement(
        *, gross: str = "13.80", fee: str = "0.30", source_ref: str = "trade-001"
    ):
        return tracker_module.VenueSettlement(
            gross_cash=Money.from_value(gross),
            fee=Money.from_value(fee),
            status="CONFIRMED",
            source_ref=source_ref,
        )

    def test_confirmed_venue_cash_supersedes_wrong_modeled_price(self) -> None:
        tracker = PositionTracker(PositionConfig(initial_balance=1000.0))
        self._open(tracker)

        result = tracker.close_position_with_fill(
            Fill(
                "m1",
                exit_price=0.90,
                filled_quantity=30,
                fill_id="venue-001",
                settlement=self._settlement(),
            )
        )

        assert result == SettlementReceipt(
            gross_cash=Money.from_value("13.80"),
            fee=Money.from_value("0.30"),
            net_cash=Money.from_value("13.50"),
            realized_pnl=Money.from_value("1.50"),
        )
        remaining = tracker.open_positions()[0]
        assert remaining.quantity == 70.0
        assert remaining.cost_basis == 28.0
        assert tracker.balance == 973.5
        assert tracker.total_realized_pnl == 1.5

    def test_identical_settlement_replays_after_restart(self, tmp_path) -> None:
        path = tmp_path / "positions.db"
        first = PositionTracker(
            repository=SQLitePositionRepository(path, initial_balance=1000.0)
        )
        self._open(first)
        fill = Fill(
            "m1",
            exit_price=0.90,
            filled_quantity=30,
            fill_id="venue-001",
            settlement=self._settlement(),
        )
        committed = first.close_position_with_fill(fill)

        restarted = PositionTracker(
            repository=SQLitePositionRepository(path, initial_balance=1000.0)
        )
        replayed = restarted.close_position_with_fill(fill)

        assert replayed == committed
        assert restarted.balance == 973.5
        assert restarted.total_realized_pnl == 1.5
        assert restarted.open_positions()[0].quantity == 70.0

    @pytest.mark.parametrize(
        "exit_price,quantity,gross,fee,source_ref",
        [
            (0.99, 30, "13.80", "0.30", "trade-001"),
            (0.90, 31, "13.80", "0.30", "trade-001"),
            (0.90, 30, "13.81", "0.30", "trade-001"),
            (0.90, 30, "13.80", "0.31", "trade-001"),
            (0.90, 30, "13.80", "0.30", "trade-other"),
        ],
    )
    def test_changed_confirmed_settlement_conflicts_with_same_fill_id(
        self, tmp_path, exit_price, quantity, gross, fee, source_ref
    ) -> None:
        path = tmp_path / "positions.db"
        tracker = PositionTracker(
            repository=SQLitePositionRepository(path, initial_balance=1000.0)
        )
        self._open(tracker)
        tracker.close_position_with_fill(
            Fill(
                "m1",
                0.90,
                filled_quantity=30,
                fill_id="venue-001",
                settlement=self._settlement(),
            )
        )

        with pytest.raises(ValueError, match="operation identity conflict"):
            tracker.close_position_with_fill(
                Fill(
                    "m1",
                    exit_price,
                    filled_quantity=quantity,
                    fill_id="venue-001",
                    settlement=self._settlement(
                        gross=gross, fee=fee, source_ref=source_ref
                    ),
                )
            )

        assert tracker.balance == 973.5
        assert tracker.open_positions()[0].quantity == 70.0

    @pytest.mark.parametrize(
        "kwargs,match",
        [
            ({"status": "MATCHED"}, "CONFIRMED"),
            ({"status": "MINED"}, "CONFIRMED"),
            ({"source_ref": ""}, "source_ref"),
            ({"gross_cash": Money.from_value("-1")}, "non-negative"),
            ({"fee": Money.from_value("-0.01")}, "non-negative"),
            (
                {
                    "gross_cash": Money.from_value("0.10"),
                    "fee": Money.from_value("0.11"),
                },
                "exceed",
            ),
        ],
    )
    def test_invalid_or_nonterminal_settlement_fails_at_boundary(
        self, kwargs, match
    ) -> None:
        values = {
            "gross_cash": Money.from_value("13.80"),
            "fee": Money.from_value("0.30"),
            "status": "CONFIRMED",
            "source_ref": "trade-001",
            **kwargs,
        }

        with pytest.raises(ValueError, match=match):
            tracker_module.VenueSettlement(**values)

    def test_venue_settlement_requires_fill_id_without_mutation(self) -> None:
        tracker = PositionTracker(PositionConfig(initial_balance=1000.0))
        self._open(tracker)

        with pytest.raises(ValueError, match="fill_id"):
            tracker.close_position_with_fill(
                Fill(
                    "m1",
                    0.90,
                    filled_quantity=30,
                    settlement=self._settlement(),
                ),
                operation_id="caller-cannot-authorize-venue-truth",
            )

        assert tracker.balance == 960.0
        assert tracker.open_positions()[0].quantity == 100.0

    def test_modeled_fill_still_returns_float(self) -> None:
        tracker = PositionTracker(PositionConfig(initial_balance=1000.0))
        self._open(tracker)

        result = tracker.close_position_with_fill(
            Fill("m1", 0.45, filled_quantity=30, fill_id="modeled-fill")
        )

        assert type(result) is float
        assert result == pytest.approx(1.5)
