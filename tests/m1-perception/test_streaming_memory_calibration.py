"""Calibrate baseline_rss for the streaming memory budget test (T5.0).

Imports the full polyarb runtime stack and Settings, then measures RSS
AFTER imports + before run_snapshot is called. This baseline is host-
specific (developer laptop vs Fly micro-VM vs CI runner all differ);
the streaming budget test asserts a DELTA above this baseline, not an
absolute number.

Writes the measured number to ``tests/m1-perception/fixtures/baseline_rss.txt``
(gitignored — value is host-specific).
"""

from __future__ import annotations

import os

os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")

from pathlib import Path

import psutil

# Import the stack so the baseline includes pyarrow, httpx, sqlite,
# starlette/uvicorn, sentry-sdk, loguru module init.
import polyarb.clients.gamma_client  # noqa: F401
import polyarb.snapshot.orchestrator  # noqa: F401
import polyarb.storage.parquet_writer  # noqa: F401
import polyarb.storage.sqlite_store  # noqa: F401
from polyarb.config import Settings

BASELINE_FILE = Path(__file__).parent / "fixtures" / "baseline_rss.txt"


def test_record_baseline_rss(tmp_path: Path) -> None:
    """Diagnostic: measure baseline RSS and write to file. Does NOT assert
    on budget — that's T5.1's job. Sanity-asserts > 50MB only."""
    settings = Settings(
        db_path=tmp_path / "state.db",
        parquet_root=tmp_path / "parquet",
        cache_root=tmp_path / "cache",
    )
    # Touch the settings so the validator runs and any lazy init pages in.
    _ = settings.db_path
    baseline = psutil.Process(os.getpid()).memory_info().rss
    print(f"\n[calibration] baseline_rss = {baseline / 1024 / 1024:.1f}MB")
    BASELINE_FILE.parent.mkdir(parents=True, exist_ok=True)
    BASELINE_FILE.write_text(str(baseline))
    # Sanity: baseline should be at least 50MB (Python + pyarrow loaded);
    # if < 50MB the test is probably running in a stripped environment.
    assert baseline > 50 * 1024 * 1024, (
        f"baseline_rss {baseline / 1024 / 1024:.1f}MB unrealistically low — "
        "are imports actually loaded?"
    )
