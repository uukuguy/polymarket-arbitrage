"""RED tests for polyarb.clients.ws_market_client.stream_market_events.

Plan 04 Wave 0 (RED). Asserts:
- Subscribe payload shape (type/assets_ids/initial_dump)
- ping_interval=10 + ping_timeout=10 (Polymarket drops at 10s silence)
- max_size=2**22 (4 MiB cap for fat initial_dump book snapshots)
- initial_dump defaults to True
- JSONDecodeError on a non-JSON frame does NOT crash iterator
- ConnectionClosed triggers outer reconnect-iterator (2 subscribe payloads)
- CancelledError propagates (Phase 02 F-04 invariant)

Mock pattern: patch `polyarb.clients.ws_market_client.websockets.connect`
at IMPORT SITE (Phase 02 L9). FakeWs implements minimal __aiter__/send/close.
"""

from __future__ import annotations

import asyncio
import io
import json
from typing import Any

import pytest
from loguru import logger

# ─────────────────────────────────────────────────────────────────────────────
# Loguru sink fixture (Phase 02.1 L4 — caplog doesn't see loguru output)
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def loguru_sink():
    sink = io.StringIO()
    sink_id = logger.add(sink, format="{message}", level="DEBUG")
    yield sink
    logger.remove(sink_id)


# ─────────────────────────────────────────────────────────────────────────────
# FakeWs — minimal websocket-like async object
# ─────────────────────────────────────────────────────────────────────────────


class FakeWs:
    """Replays a list of frames (str or Exception) via __aiter__/__anext__."""

    def __init__(self, frames: list[Any] | None = None) -> None:
        self._frames: list[Any] = list(frames or [])
        self.sent: list[str] = []
        self.closed = False

    async def send(self, raw: str) -> None:
        self.sent.append(raw)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._frames:
            raise StopAsyncIteration
        nxt = self._frames.pop(0)
        if isinstance(nxt, BaseException):
            raise nxt
        return nxt

    async def close(self) -> None:
        self.closed = True


def _make_connect(
    fake_ws_instances: list[FakeWs],
    record_kwargs: list[dict],
) -> Any:
    """Return a callable matching `websockets.connect(url, **kw)` shape.

    Mirrors websockets 15+ reconnect-iterator form:
        async for ws in websockets.connect(...):
            ...
    Each call returns an *async iterator* that yields the next FakeWs.
    """
    pending = list(fake_ws_instances)

    def _connect(url: str, **kwargs: Any) -> Any:
        record_kwargs.append({"url": url, **kwargs})

        async def _gen():
            while pending:
                yield pending.pop(0)

        return _gen()

    return _connect


# ─────────────────────────────────────────────────────────────────────────────
# Test 1 — subscribe payload shape
# ─────────────────────────────────────────────────────────────────────────────


async def test_subscribe_payload_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    """First send() payload must be {type, assets_ids, initial_dump:True}."""
    fake = FakeWs(frames=['{"event_type":"price_change","x":1}'])
    kwargs_record: list[dict] = []

    monkeypatch.setattr(
        "polyarb.clients.ws_market_client.websockets.connect",
        _make_connect([fake], kwargs_record),
    )

    from polyarb.clients.ws_market_client import stream_market_events

    events: list[dict] = []
    async for ev in stream_market_events(["0xabc", "0xdef"], initial_dump=True):
        events.append(ev)
        break  # consume one then stop

    assert len(fake.sent) == 1, f"expected 1 send, got {len(fake.sent)}"
    payload = json.loads(fake.sent[0])
    assert payload == {
        "type": "market",
        "assets_ids": ["0xabc", "0xdef"],
        "initial_dump": True,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Test 2 — ping_interval=10 (NOT 20 default)
# ─────────────────────────────────────────────────────────────────────────────


async def test_ping_interval_10s(monkeypatch: pytest.MonkeyPatch) -> None:
    """websockets.connect must be called with ping_interval=10 + ping_timeout=10."""
    fake = FakeWs(frames=['{"event_type":"price_change"}'])
    kwargs_record: list[dict] = []
    monkeypatch.setattr(
        "polyarb.clients.ws_market_client.websockets.connect",
        _make_connect([fake], kwargs_record),
    )

    from polyarb.clients.ws_market_client import stream_market_events

    async for _ in stream_market_events(["0xabc"], initial_dump=True):
        break

    assert len(kwargs_record) >= 1
    kw = kwargs_record[0]
    assert kw.get("ping_interval") == 10, f"ping_interval={kw.get('ping_interval')}"
    assert kw.get("ping_timeout") == 10, f"ping_timeout={kw.get('ping_timeout')}"


# ─────────────────────────────────────────────────────────────────────────────
# Test 3 — max_size=2**22 (4 MiB)
# ─────────────────────────────────────────────────────────────────────────────


async def test_max_size_4_mib(monkeypatch: pytest.MonkeyPatch) -> None:
    """websockets.connect called with max_size=2**22 (4194304 bytes)."""
    fake = FakeWs(frames=['{"event_type":"book"}'])
    kwargs_record: list[dict] = []
    monkeypatch.setattr(
        "polyarb.clients.ws_market_client.websockets.connect",
        _make_connect([fake], kwargs_record),
    )

    from polyarb.clients.ws_market_client import stream_market_events

    async for _ in stream_market_events(["0xabc"], initial_dump=True):
        break

    assert kwargs_record[0].get("max_size") == 2**22, (
        f"max_size={kwargs_record[0].get('max_size')}, expected {2**22}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test 4 — initial_dump defaults to True
# ─────────────────────────────────────────────────────────────────────────────


async def test_initial_dump_default_true(monkeypatch: pytest.MonkeyPatch) -> None:
    """Calling without initial_dump=... yields payload with initial_dump=True."""
    fake = FakeWs(frames=['{"event_type":"book"}'])
    kwargs_record: list[dict] = []
    monkeypatch.setattr(
        "polyarb.clients.ws_market_client.websockets.connect",
        _make_connect([fake], kwargs_record),
    )

    from polyarb.clients.ws_market_client import stream_market_events

    async for _ in stream_market_events(["0xabc"]):
        break

    payload = json.loads(fake.sent[0])
    assert payload["initial_dump"] is True


# ─────────────────────────────────────────────────────────────────────────────
# Test 5 — initial_dump=False when passed
# ─────────────────────────────────────────────────────────────────────────────


async def test_initial_dump_false_when_passed(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = FakeWs(frames=['{"event_type":"book"}'])
    kwargs_record: list[dict] = []
    monkeypatch.setattr(
        "polyarb.clients.ws_market_client.websockets.connect",
        _make_connect([fake], kwargs_record),
    )

    from polyarb.clients.ws_market_client import stream_market_events

    async for _ in stream_market_events(["0xabc"], initial_dump=False):
        break

    payload = json.loads(fake.sent[0])
    assert payload["initial_dump"] is False


# ─────────────────────────────────────────────────────────────────────────────
# Test 6 — JSON decode error does NOT crash iterator
# ─────────────────────────────────────────────────────────────────────────────


async def test_json_decode_error_does_not_crash(
    monkeypatch: pytest.MonkeyPatch, loguru_sink
) -> None:
    """A non-JSON frame is logged + skipped; iterator continues with next frame."""
    fake = FakeWs(
        frames=[
            "not-valid-json",  # JSONDecodeError
            '{"event_type":"price_change","ok":1}',  # OK
        ]
    )
    kwargs_record: list[dict] = []
    monkeypatch.setattr(
        "polyarb.clients.ws_market_client.websockets.connect",
        _make_connect([fake], kwargs_record),
    )

    from polyarb.clients.ws_market_client import stream_market_events

    received: list[dict] = []
    async for ev in stream_market_events(["0xabc"]):
        received.append(ev)
        if len(received) >= 1:
            break

    assert len(received) == 1
    assert received[0]["ok"] == 1
    # Warning logged about the bad frame
    out = loguru_sink.getvalue()
    assert "non-JSON" in out or "JSONDecodeError" in out or "non-json" in out.lower(), (
        f"expected JSONDecodeError warning in loguru output, got: {out!r}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test 7 — ConnectionClosed triggers outer reconnect-iterator
# ─────────────────────────────────────────────────────────────────────────────


async def test_connection_closed_triggers_reconnect(monkeypatch: pytest.MonkeyPatch) -> None:
    """Two FakeWs in succession → each receives the subscribe payload (one per connect cycle)."""
    import websockets

    fake1 = FakeWs(
        frames=[
            '{"event_type":"book"}',
            websockets.ConnectionClosed(rcvd=None, sent=None),
        ]
    )
    fake2 = FakeWs(frames=['{"event_type":"price_change"}'])
    kwargs_record: list[dict] = []
    monkeypatch.setattr(
        "polyarb.clients.ws_market_client.websockets.connect",
        _make_connect([fake1, fake2], kwargs_record),
    )

    from polyarb.clients.ws_market_client import stream_market_events

    received: list[dict] = []
    async for ev in stream_market_events(["0xabc"]):
        received.append(ev)
        if len(received) >= 2:
            break

    assert len(received) == 2
    # Both connect cycles must have received the subscribe payload
    assert len(fake1.sent) == 1, f"fake1 sent: {fake1.sent}"
    assert len(fake2.sent) == 1, f"fake2 sent: {fake2.sent}"
    p1 = json.loads(fake1.sent[0])
    p2 = json.loads(fake2.sent[0])
    assert p1["type"] == "market" and p2["type"] == "market"


# ─────────────────────────────────────────────────────────────────────────────
# Test 8 — CancelledError propagates (Phase 02 F-04 invariant)
# ─────────────────────────────────────────────────────────────────────────────


async def test_cancelledError_propagates_from_stream(monkeypatch: pytest.MonkeyPatch) -> None:
    """A CancelledError raised mid-frame MUST bubble out of the iterator (F-04)."""
    fake = FakeWs(
        frames=[
            '{"event_type":"book"}',
            asyncio.CancelledError(),
        ]
    )
    kwargs_record: list[dict] = []
    monkeypatch.setattr(
        "polyarb.clients.ws_market_client.websockets.connect",
        _make_connect([fake], kwargs_record),
    )

    from polyarb.clients.ws_market_client import stream_market_events

    received: list[dict] = []

    async def _drain():
        async for ev in stream_market_events(["0xabc"]):
            received.append(ev)

    with pytest.raises(asyncio.CancelledError):
        await _drain()

    # Saw the first frame before the cancel
    assert len(received) == 1


async def test_reconnect_resolves_asset_provider_again(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import websockets

    fake1 = FakeWs(
        frames=[
            '{"event_type":"book"}',
            websockets.ConnectionClosed(rcvd=None, sent=None),
        ]
    )
    fake2 = FakeWs(frames=['{"event_type":"book"}'])
    monkeypatch.setattr(
        "polyarb.clients.ws_market_client.websockets.connect",
        _make_connect([fake1, fake2], []),
    )
    desired = ["a"]

    from polyarb.clients.ws_market_client import stream_market_events

    count = 0
    async for _ in stream_market_events(lambda: list(desired)):
        count += 1
        if count == 1:
            desired[:] = ["b", "c"]
        if count == 2:
            break

    assert json.loads(fake1.sent[0])["assets_ids"] == ["a"]
    assert json.loads(fake2.sent[0])["assets_ids"] == ["b", "c"]
