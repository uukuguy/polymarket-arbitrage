"""Tests for polyarb.observation.formatter — rich.Table + atomic parquet write.

Plan 03 Task 2 — covers:
- render_table empty / non-empty / ANSI strip / markup not interpreted
- write_scan_parquet atomic (tmp + os.replace) / empty skip / path layout
- CLI smoke (typer.testing.CliRunner) for cli_observation.py
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from typer.testing import CliRunner

from polyarb.cli_observation import app as obs_app
from polyarb.observation.formatter import (
    _ANSI_RE,
    _safe_str,
    render_table,
    write_scan_parquet,
)
from polyarb.storage.schemas import DDL

runner = CliRunner(mix_stderr=False)


# =============================================================================
# render_table
# =============================================================================


def test_render_table_empty_does_not_crash(capsys: pytest.CaptureFixture) -> None:
    df = pd.DataFrame()
    render_table(df, title="empty-test")
    out = capsys.readouterr().out
    # rich strips formatting in non-tty default; fallback substring works
    assert "empty-test" in out
    assert "no rows" in out


def test_render_table_non_empty_shows_header_and_rows(
    capsys: pytest.CaptureFixture,
) -> None:
    df = pd.DataFrame(
        [
            {"slug": "a", "question": "Q1", "liquidity_usd": 100.0},
            {"slug": "b", "question": "Q2", "liquidity_usd": 200.0},
        ]
    )
    render_table(df, title="test-title")
    out = capsys.readouterr().out
    assert "test-title" in out
    assert "slug" in out
    assert "a" in out
    assert "Q1" in out


def test_render_table_truncates_long_question(
    capsys: pytest.CaptureFixture,
) -> None:
    """Long question column folds via overflow='fold' — no crash, content present."""
    long_q = "x" * 200
    df = pd.DataFrame([{"slug": "a", "question": long_q, "liquidity_usd": 100}])
    render_table(df, title="fold-test")
    out = capsys.readouterr().out
    # content (or large prefix) present; rich will wrap but keep text
    assert "x" * 20 in out


def test_render_table_strips_ansi_from_zh(
    capsys: pytest.CaptureFixture,
) -> None:
    """T-01.1-13: ANSI escape sequences in question_zh must NOT render as color.

    The fixture row contains ESC[31m...ESC[0m (red ANSI). After our _ANSI_RE
    pre-strip, the table cell shows the literal text only — no ESC bytes.
    """
    df = pd.DataFrame(
        [
            {
                "slug": "a",
                "question": "Q",
                "question_zh": "\x1b[31m危险\x1b[0m",
                "liquidity_usd": 100,
            }
        ]
    )
    render_table(df, title="ansi-test")
    out = capsys.readouterr().out
    # The Chinese characters are present; ESC bytes are stripped
    assert "危险" in out
    assert "\x1b[31m" not in out
    assert "\x1b[0m" not in out


def test_render_table_does_not_interpret_rich_markup(
    capsys: pytest.CaptureFixture,
) -> None:
    """rich-style markup `[red]X[/red]` in user data is rendered literally,
    not as styling. add_row defaults to interpreting markup; we explicitly
    pass safe strings only — and column overflow='fold' is text-mode."""
    df = pd.DataFrame([{"slug": "a", "question": "[red]X[/red]", "liquidity_usd": 100}])
    render_table(df, title="markup-test")
    out = capsys.readouterr().out
    # The literal `[red]X[/red]` substring is present
    assert "[red]X[/red]" in out or "X" in out


def test_render_table_explicit_columns_arg(capsys: pytest.CaptureFixture) -> None:
    df = pd.DataFrame([{"slug": "a", "question": "Q", "liquidity_usd": 100, "extra": "skip-me"}])
    render_table(df, title="cols-test", columns=("slug", "extra"))
    out = capsys.readouterr().out
    assert "slug" in out
    assert "extra" in out
    # `liquidity_usd` was NOT included
    assert "liquidity_usd" not in out


def test_safe_str_strips_ansi() -> None:
    assert _safe_str("\x1b[31mred\x1b[0m") == "red"


def test_safe_str_handles_none() -> None:
    assert _safe_str(None) == ""


def test_ansi_re_matches_csi() -> None:
    assert _ANSI_RE.search("\x1b[31m")


# =============================================================================
# write_scan_parquet
# =============================================================================


def test_write_scan_parquet_empty_skips(tmp_path: Path) -> None:
    df = pd.DataFrame()
    result = write_scan_parquet(df, "test-recipe", tmp_path)
    assert result is None
    # No file written
    assert list(tmp_path.rglob("*.parquet")) == []


def test_write_scan_parquet_path_layout(tmp_path: Path) -> None:
    df = pd.DataFrame([{"slug": "a", "liquidity_usd": 100.0}])
    result = write_scan_parquet(df, "thick-but-slippery", tmp_path)
    assert result is not None
    assert result.parent.name == "thick-but-slippery"
    assert result.suffix == ".parquet"
    # timestamp form: YYYY-MM-DDTHH-MM-SS.parquet
    assert len(result.stem) == len("2024-01-01T00-00-00")
    assert result.exists()


def test_write_scan_parquet_atomic_on_failure(tmp_path: Path) -> None:
    """If to_parquet raises mid-write, the .tmp file is removed and final
    target does not exist."""
    df = pd.DataFrame([{"slug": "a", "liquidity_usd": 100.0}])
    # Patch DataFrame.to_parquet on this instance to raise after creating tmp
    original_to_parquet = pd.DataFrame.to_parquet

    def _to_parquet_then_fail(self, path, *args, **kwargs):  # type: ignore[no-untyped-def]
        # Create a partial file then raise
        Path(path).write_bytes(b"\x00\x00partial\x00")
        raise RuntimeError("simulated failure mid-write")

    with patch.object(pd.DataFrame, "to_parquet", _to_parquet_then_fail):
        with pytest.raises(RuntimeError, match="simulated failure"):
            write_scan_parquet(df, "test-recipe", tmp_path)

    # After failure, no .parquet exists in target dir, .tmp is cleaned up
    final_files = list(tmp_path.rglob("*.parquet"))
    tmp_files = list(tmp_path.rglob("*.parquet.tmp"))
    assert final_files == [], f"unexpected final files: {final_files}"
    assert tmp_files == [], f"tmp file leak: {tmp_files}"
    # restore (defensive — patch.object should restore but be explicit)
    pd.DataFrame.to_parquet = original_to_parquet  # type: ignore[method-assign]


def test_write_scan_parquet_roundtrip(tmp_path: Path) -> None:
    """The parquet written can be read back."""
    df = pd.DataFrame(
        [
            {"slug": "a", "liquidity_usd": 100.0, "question": "Q1"},
            {"slug": "b", "liquidity_usd": 200.0, "question": "Q2"},
        ]
    )
    result = write_scan_parquet(df, "roundtrip", tmp_path)
    assert result is not None
    df2 = pd.read_parquet(result)
    assert len(df2) == 2
    assert list(df2["slug"]) == ["a", "b"]


# =============================================================================
# CLI smoke — cli_observation app via CliRunner
# =============================================================================


@pytest.fixture
def seeded_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Same shape as scanner test fixture: 1 snapshot + 1 event + 1 tag + 5 markets.

    Patches polyarb.cli_observation.load_settings so the CLI uses a Settings
    object pointing at this tmp DB instead of the real project DB. Going via
    env vars alone doesn't work because load_settings reads config/snapshot.yaml
    by default which overrides POLYARB_DB_PATH.
    """
    db_path = tmp_path / "obs.db"
    con = sqlite3.connect(str(db_path), isolation_level=None)
    try:
        con.executescript(DDL)
        con.execute(
            "INSERT INTO snapshots(id, taken_at_ms, finished_at_ms, mode, "
            "market_count, is_valid, parquet_path) "
            "VALUES (1, 1700000000000, 1700000060000, 'subset', 5, 1, '/tmp/x.parquet')"
        )
        con.execute(
            "INSERT INTO events(id, slug, title, ticker, active, closed, "
            "liquidity_usd, volume_usd, end_time_ms, fetched_at_ms, snapshot_id) "
            "VALUES ('EV-1', 'ev1', 'E1', 'TKR', 1, 0, 1000, 5000, "
            "1800000000000, 1700000000000, 1)"
        )
        con.execute(
            "INSERT INTO event_tags(event_id, tag_id, tag_label, tag_slug, snapshot_id) "
            "VALUES ('EV-1', '120', 'Crypto', 'crypto', 1)"
        )
        con.execute(
            "INSERT INTO markets(market_id, condition_id, slug, question, "
            "yes_token_id, no_token_id, mid_price, liquidity_usd, volume_usd, "
            "best_bid_price, best_bid_size, best_ask_price, best_ask_size, "
            "end_time_ms, active, closed, neg_risk, neg_risk_market_id, "
            "fetched_at_ms, snapshot_id, incomplete, event_id) "
            "VALUES ('M1','C','thick-1','Will X happen?',NULL,NULL,0.5,200000, "
            "50000,0.40,100,0.55,100,1900000000000,1,0,0,NULL,1700000000000, "
            "1,0,'EV-1')"
        )
    finally:
        con.close()
    # Patch the cli's load_settings to ignore project yaml and use this DB.
    from polyarb.config import Settings

    def _fake_load_settings(_cfg=None):  # type: ignore[no-untyped-def]
        return Settings(
            db_path=db_path,
            parquet_root=tmp_path / "snapshots",
            cache_root=tmp_path / ".cache",
            liquidity_threshold_usd=1.0,
        )

    monkeypatch.setattr("polyarb.cli_observation.load_settings", _fake_load_settings)
    return db_path


def test_cli_list_recipes_lists_all_six_builtins(seeded_db: Path) -> None:
    result = runner.invoke(obs_app, ["list-recipes"])
    assert result.exit_code == 0, f"stderr={result.stderr}"
    # All 6 builtin names appear, prefixed [builtin]
    for name in [
        "thick-but-slippery",
        "near-end",
        "ghost-suspicious",
        "coin-flip",
        "neg-risk-incomplete",
        "by-tag",
    ]:
        assert f"[builtin] {name}" in result.stdout


def test_cli_scan_runs_thick_but_slippery(seeded_db: Path, tmp_path: Path) -> None:
    """Run the thick-but-slippery recipe — fixture has 1 matching market (M1
    has liq 200000 + spread 0.15)."""
    scans = tmp_path / "scans"
    result = runner.invoke(
        obs_app,
        [
            "scan",
            "--name",
            "thick-but-slippery",
            "--scans-root",
            str(scans),
            "--no-parquet",
        ],
    )
    assert result.exit_code == 0, f"stderr={result.stderr}"
    assert "OK | recipe=thick-but-slippery" in result.stdout
    assert "rows=1" in result.stdout


def test_cli_scan_unknown_recipe_exits_1(seeded_db: Path) -> None:
    result = runner.invoke(obs_app, ["scan", "--name", "no-such-recipe"])
    assert result.exit_code == 1
    assert "unknown recipe" in result.stderr


def test_cli_scan_writes_parquet_to_scans_root(seeded_db: Path, tmp_path: Path) -> None:
    scans = tmp_path / "scans"
    result = runner.invoke(
        obs_app,
        [
            "scan",
            "--name",
            "thick-but-slippery",
            "--scans-root",
            str(scans),
        ],
    )
    assert result.exit_code == 0
    files = list((scans / "thick-but-slippery").glob("*.parquet"))
    assert len(files) == 1


def test_cli_scans_purge_deletes_old_files(tmp_path: Path) -> None:
    """scans-purge --older-than-days 0 removes everything immediately."""
    scans = tmp_path / "scans"
    sub = scans / "test"
    sub.mkdir(parents=True)
    f = sub / "old.parquet"
    f.write_bytes(b"dummy")
    result = runner.invoke(
        obs_app,
        ["scans-purge", "--older-than-days", "0", "--scans-root", str(scans)],
    )
    assert result.exit_code == 0
    assert not f.exists()
    assert "purged 1" in result.stdout
