from __future__ import annotations

import asyncio
import inspect
import tomllib
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

from polyarb.config import Settings
from polyarb.http.app import create_app
from polyarb.storage.sqlite_store import SQLiteStore


def test_create_app_exposes_quote_worker_runtime(tmp_path) -> None:
    settings = Settings(db_path=tmp_path / "state.db")
    store = SQLiteStore(settings.db_path)
    store.init_schema()
    runtime = object()

    app = create_app(
        scheduler=MagicMock(),
        sqlite_store=store,
        settings=settings,
        quote_worker_runtime=runtime,
    )

    assert app.state.quote_worker_runtime is runtime


async def test_l1_start_helper_runs_quote_worker_and_task_is_cancellable() -> None:
    from polyarb.daemon.main import _start_quote_worker

    entered = asyncio.Event()

    async def run(_stop_event: asyncio.Event) -> None:
        entered.set()
        await asyncio.Event().wait()

    worker = MagicMock()
    worker.run = AsyncMock(side_effect=run)
    stop_event = asyncio.Event()

    task = _start_quote_worker(worker, stop_event)
    assert task is not None
    await asyncio.wait_for(entered.wait(), timeout=1)

    task.cancel()
    results = await asyncio.gather(task, return_exceptions=True)
    assert isinstance(results[0], asyncio.CancelledError)
    worker.run.assert_awaited_once_with(stop_event)


def test_l1_main_owns_quote_worker_shutdown() -> None:
    from polyarb.daemon import main

    source = inspect.getsource(main.main)
    assert "build_production_quote_worker(settings)" in source
    assert "_start_quote_worker(quote_worker, stop_event)" in source
    assert "quote_worker_task.cancel()" in source
    assert "quote_worker_task" in source.partition("asyncio.gather(")[2]


def test_fly_enables_worker_at_120_seconds() -> None:
    config = tomllib.loads(Path("fly.toml").read_text())
    env = config["env"]

    assert env["POLYARB_NEG_RISK_QUOTE_WORKER_ENABLED"] == "true"
    assert env["POLYARB_NEG_RISK_QUOTE_INTERVAL_S"] == "120"
