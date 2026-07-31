from __future__ import annotations

import asyncio
import os
import sqlite3
import threading
from pathlib import Path
from types import MappingProxyType, SimpleNamespace
from uuid import UUID

import pytest

os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")

from polyarb.config import Settings
from polyarb.daemon.opportunity_watcher import OpportunityWatcher
from polyarb.perception.fault_adapters import (
    QualifiedTelegramTransportError,
    TelegramDeliveryFault,
)
from polyarb.perception.fault_authority import FaultAuthorityStore
from polyarb.perception.fault_control import (
    FaultAuthorization,
    FaultCallClass,
    FaultDecision,
    FaultIntentRequest,
    FaultKind,
    FaultRecoveryWriter,
    FaultRuntimeIdentity,
)
from polyarb.perception.fault_runtime import (
    FaultRecoveryOutcome,
    FaultRuntime,
)
from polyarb.perception.incidents import IncidentManager
from polyarb.perception.store import OpportunityPerceptionStore
from polyarb.routing.opportunity_ledger import OpportunityLedger
from polyarb.storage.sqlite_store import SQLiteStore


class _Runtime:
    degraded = False
    active_fault_id = "fault-telegram"
    pending_recovery_fault_id = None

    def __init__(self, decision: FaultDecision, *, receipt: object | None = None) -> None:
        self.decision = decision
        self.receipt = receipt or SimpleNamespace(
            fault_id="fault-telegram",
            call_id="call-telegram",
            occurred_at_ms=1_001,
        )
        self.calls = []
        self.events: list[str] = []

    def consume(self, call):
        self.calls.append(call)
        return self.decision

    async def sync_before_batch(self):
        return None

    async def record_injection(self, fault_id):
        self.events.append(f"injected:{fault_id}")
        return self.receipt


def _decision() -> FaultDecision:
    return FaultDecision(
        True,
        fault_id="fault-telegram",
        kind=FaultKind.TELEGRAM_FAILURE,
        parameters=MappingProxyType({}),
    )


@pytest.mark.asyncio
async def test_exact_decimal_outbox_id_receipts_before_typed_failure() -> None:
    runtime = _Runtime(_decision())
    fault = TelegramDeliveryFault(runtime=runtime)

    with pytest.raises(QualifiedTelegramTransportError) as raised:
        await fault.before_send(42)

    assert runtime.calls[0].call_class is FaultCallClass.TELEGRAM_OPPORTUNITY_CARD
    assert runtime.calls[0].target_key == "42"
    assert runtime.events == ["injected:fault-telegram"]
    assert raised.value.fault_id == "fault-telegram"
    assert raised.value.call_id == "call-telegram"
    assert str(raised.value) == "qualified-telegram-delivery-failure"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "decision",
    [
        FaultDecision(False),
        FaultDecision(
            True,
            fault_id="fault-telegram",
            kind=FaultKind.CLOB_429,
            parameters=MappingProxyType({}),
        ),
    ],
)
async def test_disabled_or_unmatched_decision_is_pass_through(
    decision: FaultDecision,
) -> None:
    runtime = _Runtime(decision)

    assert await TelegramDeliveryFault(runtime=runtime).before_send(7) is None
    assert runtime.events == []


@pytest.mark.asyncio
async def test_controller_or_receipt_failure_is_pass_through() -> None:
    class BrokenRuntime(_Runtime):
        def consume(self, call):
            raise RuntimeError("authority unavailable")

    broken = BrokenRuntime(_decision())
    assert await TelegramDeliveryFault(runtime=broken).before_send(8) is None

    no_receipt = _Runtime(_decision(), receipt=False)
    no_receipt.receipt = None
    assert await TelegramDeliveryFault(runtime=no_receipt).before_send(8) is None


def test_typed_exception_contains_no_card_or_transport_configuration() -> None:
    runtime = _Runtime(_decision())
    fault = TelegramDeliveryFault(runtime=runtime)

    assert set(vars(fault)) == {"_runtime"}
    assert "chat" not in repr(fault).lower()
    assert "token" not in repr(fault).lower()
    assert "url" not in repr(fault).lower()


def _outbox(tmp_path: Path) -> tuple[Settings, OpportunityLedger]:
    db_path = tmp_path / "state.db"
    SQLiteStore(db_path).init_schema()
    with sqlite3.connect(db_path) as con:
        con.execute(
            "INSERT INTO neg_risk_opportunities("
            "id,event_id,group_id,membership_hash,status,bundle_cost,"
            "gross_edge_bps,max_bundle_size,structure_revision,quote_run_id,"
            "opened_at_ms,updated_at_ms,closed_at_ms,transition_reason"
            ") VALUES ('opp','event','group','membership','observe',"
            "0.9,100,1,1,1,900,900,NULL,NULL)"
        )
        con.executemany(
            "INSERT INTO neg_risk_opportunity_notifications("
            "id,opportunity_id,reason,payload_json,status,attempt_count,created_at_ms"
            ") VALUES (?,'opp','opened',?,'pending',0,?)",
            [
                (1, '{"private_card":"never-persist-me"}', 900),
                (2, '{"safe":"second"}', 901),
            ],
        )
    settings = Settings(
        db_path=db_path,
        parquet_root=tmp_path / "parquet",
        cache_root=tmp_path / "cache",
    )
    return settings, OpportunityLedger(db_path)


class _ExactWatcherRuntime(_Runtime):
    def __init__(self) -> None:
        super().__init__(_decision())
        self.pending_recovery_fault_id = None

    def consume(self, call):
        self.calls.append(call)
        return _decision() if call.target_key == "1" and not self.events else FaultDecision(False)

    async def link_detection(self, fault_id, *, kind, detection_id):
        self.events.append(f"linked:{detection_id}")
        return True

    async def cleanup(self, fault_id, reason):
        self.events.append(f"cleaned:{reason}")
        self.pending_recovery_fault_id = fault_id
        return SimpleNamespace(memory_cleared=True, receipt_persisted=True)

    async def record_writer_recovery_outcome(
        self,
        writer,
        *,
        target_key,
        writer_id,
        writer_occurred_at_ms,
    ):
        self.events.append(
            f"recovered:{writer.value}:{target_key}:{writer_id}:{writer_occurred_at_ms}"
        )
        self.pending_recovery_fault_id = None
        return FaultRecoveryOutcome.RECORDED


@pytest.mark.asyncio
async def test_real_outbox_loop_fails_only_exact_id_and_sends_other_once(
    tmp_path: Path,
) -> None:
    settings, ledger = _outbox(tmp_path)
    runtime = _ExactWatcherRuntime()
    sent: list[str] = []

    async def sender(settings, card):
        sent.append(card)

    watcher = OpportunityWatcher.for_test(
        settings,
        ledger=ledger,
        send_telegram=sender,
        clock_ms=lambda: 1_010,
        fault_runtime=runtime,
    )
    await watcher.deliver_pending_notifications()

    assert [call.target_key for call in runtime.calls] == ["1", "2"]
    assert len(sent) == 1
    assert "safe=second" not in sent[0]  # card formatting remains sender-owned
    assert [attempt.outcome for attempt in ledger.notification_attempts(1)] == ["failed"]
    assert [attempt.outcome for attempt in ledger.notification_attempts(2)] == ["delivered"]
    incident = OpportunityPerceptionStore(settings.db_path).open_incidents()[0]
    assert incident.scope == "notification:1"
    history = IncidentManager(
        OpportunityPerceptionStore(settings.db_path)
    ).incident_history(incident.id)
    assert history is not None
    assert history.items[0].incident.evidence["fault_call_id"] == "call-telegram"
    assert runtime.events[:3] == [
        "injected:fault-telegram",
        f"linked:{incident.id}",
        "cleaned:notification-delivery-failed",
    ]


@pytest.mark.asyncio
async def test_cleanup_precedes_exact_delivered_attempt_recovery(
    tmp_path: Path,
) -> None:
    settings, ledger = _outbox(tmp_path)
    with sqlite3.connect(settings.db_path) as con:
        con.execute("DELETE FROM neg_risk_opportunity_notifications WHERE id=2")
    runtime = _ExactWatcherRuntime()
    sender_calls = 0

    async def sender(settings, card):
        nonlocal sender_calls
        sender_calls += 1

    watcher = OpportunityWatcher.for_test(
        settings,
        ledger=ledger,
        send_telegram=sender,
        clock_ms=lambda: 1_010,
        fault_runtime=runtime,
    )
    await watcher.deliver_pending_notifications()
    await watcher.deliver_pending_notifications()

    attempts = ledger.notification_attempts(1)
    assert [attempt.outcome for attempt in attempts] == ["failed", "delivered"]
    recovered = next(event for event in runtime.events if event.startswith("recovered:"))
    assert runtime.events.index("cleaned:notification-delivery-failed") < runtime.events.index(
        recovered
    )
    assert recovered.split(":")[2:4] == ["1", str(attempts[-1].id)]
    assert sender_calls == 1


def test_fault_tables_never_persist_card_or_transport_secrets(tmp_path: Path) -> None:
    settings, _ = _outbox(tmp_path)
    with sqlite3.connect(settings.db_path) as con:
        rows = con.execute(
            "SELECT name FROM sqlite_master WHERE name LIKE 'neg_risk_fault_%'"
        ).fetchall()
        corpus = " ".join(
            repr(con.execute(f"SELECT * FROM {row[0]}").fetchall()) for row in rows
        )
    assert "never-persist-me" not in corpus
    assert "api.telegram.org" not in corpus
    assert "bot-token" not in corpus


@pytest.mark.asyncio
async def test_real_authority_chain_recovers_only_later_exact_delivery(
    tmp_path: Path,
) -> None:
    settings, ledger = _outbox(tmp_path)
    identity = FaultRuntimeIdentity(
        component="notification",
        release_id="a" * 40,
        machine_id="machine-1",
        boot_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
    )
    authority = FaultAuthorityStore(settings.db_path)
    base_ms = 1_000
    authority.register_runtime_start(
        identity,
        supervisor_run_id="run-1",
        attempt=1,
        started_at_ms=base_ms,
    )
    assert authority.accept_intent(
        FaultIntentRequest(
            fault_id="fault-telegram-real",
            kind=FaultKind.TELEGRAM_FAILURE,
            call_class=FaultCallClass.TELEGRAM_OPPORTUNITY_CARD,
            target_key="1",
            parameters={},
            ttl_ms=30_000,
            runtime=identity,
        ),
        auth=FaultAuthorization(
            nonce_digest="b" * 64,
            authorization_digest="c" * 64,
        ),
        accepted_at_ms=base_ms + 1,
    ).accepted
    now_ms = base_ms + 10

    def clock_ms() -> int:
        nonlocal now_ms
        now_ms += 1
        return now_ms

    runtime = FaultRuntime(
        identity=identity,
        authority=authority,
        clock_ms=clock_ms,
        monotonic=lambda: 10.0,
    )
    sent: list[str] = []

    async def sender(settings, card):
        sent.append(card)

    watcher = OpportunityWatcher.for_test(
        settings,
        ledger=ledger,
        send_telegram=sender,
        clock_ms=clock_ms,
        fault_runtime=runtime,
    )
    await watcher.deliver_pending_notifications()

    cleaned = authority.validate_history("fault-telegram-real")
    assert cleaned.valid
    assert next(
        event.state.value
        for event in reversed(cleaned.events)
        if event.state is not None
    ) == "cleaned"
    assert runtime.pending_recovery_fault_id == "fault-telegram-real"
    assert [attempt.outcome for attempt in ledger.notification_attempts(2)] == [
        "delivered"
    ]

    await watcher.deliver_pending_notifications()

    recovered = authority.validate_history("fault-telegram-real")
    assert recovered.valid
    assert [event.state.value for event in recovered.events if event.state is not None] == [
        "authorized",
        "armed",
        "injected",
        "detected",
        "contained",
        "cleaned",
        "recovered",
    ]
    assert recovered.events[-1].evidence["recovery_id"].startswith(
        "telegram-delivery-"
    )
    assert runtime.pending_recovery_fault_id is None
    assert len(sent) == 2
    with sqlite3.connect(settings.db_path) as con:
        fault_rows = " ".join(
            repr(row)
            for table in ("neg_risk_fault_intents", "neg_risk_fault_events")
            for row in con.execute(f"SELECT * FROM {table}").fetchall()
        )
    assert "never-persist-me" not in fault_rows


async def _real_fault_watcher(tmp_path: Path):
    settings, ledger = _outbox(tmp_path)
    with sqlite3.connect(settings.db_path) as con:
        con.execute("DELETE FROM neg_risk_opportunity_notifications WHERE id=2")
    identity = FaultRuntimeIdentity(
        component="notification",
        release_id="a" * 40,
        machine_id="machine-1",
        boot_id=UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
    )
    authority = FaultAuthorityStore(settings.db_path)
    authority.register_runtime_start(
        identity,
        supervisor_run_id="run-1",
        attempt=1,
        started_at_ms=1_000,
    )
    assert authority.accept_intent(
        FaultIntentRequest(
            fault_id="fault-telegram-cancel",
            kind=FaultKind.TELEGRAM_FAILURE,
            call_class=FaultCallClass.TELEGRAM_OPPORTUNITY_CARD,
            target_key="1",
            parameters={},
            ttl_ms=30_000,
            runtime=identity,
        ),
        auth=FaultAuthorization(
            nonce_digest="d" * 64,
            authorization_digest="e" * 64,
        ),
        accepted_at_ms=1_001,
    ).accepted
    now_ms = 1_010

    def clock_ms():
        nonlocal now_ms
        now_ms += 1
        return now_ms

    runtime = FaultRuntime(
        identity=identity,
        authority=authority,
        clock_ms=clock_ms,
        monotonic=lambda: 10.0,
    )

    async def sender(settings, card):
        return None

    watcher = OpportunityWatcher.for_test(
        settings,
        ledger=ledger,
        send_telegram=sender,
        clock_ms=clock_ms,
        fault_runtime=runtime,
    )
    return settings, ledger, authority, runtime, watcher


@pytest.mark.asyncio
async def test_cancel_after_failed_attempt_commit_preserves_exact_evidence(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _, ledger, authority, _, watcher = await _real_fault_watcher(tmp_path)
    committed = threading.Event()
    release = threading.Event()
    original = ledger.mark_notification_failed

    def blocking_writer(*args, **kwargs):
        attempt = original(*args, **kwargs)
        committed.set()
        assert release.wait(timeout=2)
        return attempt

    monkeypatch.setattr(ledger, "mark_notification_failed", blocking_writer)
    delivery = asyncio.create_task(watcher.deliver_pending_notifications())
    assert await asyncio.to_thread(committed.wait, 2)
    delivery.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await delivery

    history = authority.validate_history("fault-telegram-cancel")
    assert history.valid
    assert next(
        event.state.value
        for event in reversed(history.events)
        if event.state is not None
    ) == "cleaned"
    assert len(ledger.notification_attempts(1)) == 1


@pytest.mark.asyncio
async def test_cancel_after_delivered_attempt_commit_preserves_recovery(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _, ledger, authority, runtime, watcher = await _real_fault_watcher(tmp_path)
    await watcher.deliver_pending_notifications()
    assert runtime.pending_recovery_fault_id == "fault-telegram-cancel"
    committed = threading.Event()
    release = threading.Event()
    original = ledger.mark_notification_delivered

    def blocking_writer(*args, **kwargs):
        attempt = original(*args, **kwargs)
        committed.set()
        assert release.wait(timeout=2)
        return attempt

    monkeypatch.setattr(ledger, "mark_notification_delivered", blocking_writer)
    delivery = asyncio.create_task(watcher.deliver_pending_notifications())
    assert await asyncio.to_thread(committed.wait, 2)
    delivery.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await delivery

    history = authority.validate_history("fault-telegram-cancel")
    assert history.valid
    assert history.events[-1].state.value == "recovered"
    assert runtime.pending_recovery_fault_id is None
    assert [attempt.outcome for attempt in ledger.notification_attempts(1)] == [
        "failed",
        "delivered",
    ]


@pytest.mark.asyncio
async def test_recovery_validator_requires_latest_attempt_overall(
    tmp_path: Path,
) -> None:
    settings, _, authority, runtime, watcher = await _real_fault_watcher(tmp_path)
    await watcher.deliver_pending_notifications()
    with sqlite3.connect(settings.db_path) as con:
        con.row_factory = sqlite3.Row
        delivered_id = con.execute(
            "INSERT INTO neg_risk_opportunity_notification_attempts("
            "notification_id,attempted_at_ms,outcome,error_kind"
            ") VALUES (1,2000,'delivered',NULL)"
        ).lastrowid
        con.execute(
            "INSERT INTO neg_risk_opportunity_notification_attempts("
            "notification_id,attempted_at_ms,outcome,error_kind"
            ") VALUES (1,2001,'failed','OSError')"
        )
        intent = authority.validate_history("fault-telegram-cancel").intent
        assert intent is not None
        stale = runtime.make_recovery_receipt(
            FaultRecoveryWriter.TELEGRAM_DELIVERY,
            writer_id=delivered_id,
            writer_occurred_at_ms=2_000,
        )
        assert stale is not None
        assert (
            authority._validated_recovery_writer_id(con, stale, intent)
            is None
        )
        latest_id = con.execute(
            "INSERT INTO neg_risk_opportunity_notification_attempts("
            "notification_id,attempted_at_ms,outcome,error_kind"
            ") VALUES (1,2002,'delivered',NULL)"
        ).lastrowid
        latest = runtime.make_recovery_receipt(
            FaultRecoveryWriter.TELEGRAM_DELIVERY,
            writer_id=latest_id,
            writer_occurred_at_ms=2_002,
        )
        assert latest is not None
        assert authority._validated_recovery_writer_id(
            con, latest, intent
        ) == f"telegram-delivery-{latest_id}"


@pytest.mark.asyncio
async def test_failed_attempt_store_unavailable_degrades_then_passes_through_once(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _, ledger, authority, runtime, watcher = await _real_fault_watcher(tmp_path)

    def unavailable_writer(*args, **kwargs):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(ledger, "mark_notification_failed", unavailable_writer)
    await watcher.deliver_pending_notifications()

    abandoned = authority.validate_history("fault-telegram-cancel")
    assert abandoned.valid
    assert abandoned.events[-1].state.value == "abandoned"
    assert runtime.degraded is True
    assert ledger.notification_attempts(1) == ()

    sent = 0

    async def sender(settings, card):
        nonlocal sent
        sent += 1

    watcher._send_telegram = sender
    monkeypatch.setattr(
        ledger,
        "mark_notification_failed",
        OpportunityLedger.mark_notification_failed.__get__(ledger),
    )
    await watcher.deliver_pending_notifications()
    assert sent == 1
    assert [attempt.outcome for attempt in ledger.notification_attempts(1)] == [
        "delivered"
    ]


@pytest.mark.asyncio
async def test_incident_evidence_store_unavailable_degrades_without_stranded_injection(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _, ledger, authority, runtime, watcher = await _real_fault_watcher(tmp_path)

    def unavailable_incident(**kwargs):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(
        watcher._notification_incidents,
        "record_qualified_failure",
        unavailable_incident,
    )
    await watcher.deliver_pending_notifications()

    history = authority.validate_history("fault-telegram-cancel")
    assert history.valid
    assert history.events[-1].state.value == "abandoned"
    assert runtime.degraded is True
    assert [attempt.outcome for attempt in ledger.notification_attempts(1)] == [
        "failed"
    ]
