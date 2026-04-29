"""Tests for snapshot.cache.ChunkCache.

Covers:
    - fresh init writes meta.json with correct fingerprints
    - try_resume returns False on first run, True on second matching run
    - settings drift invalidates cache
    - token list drift invalidates cache
    - mode drift invalidates cache
    - >30 min age invalidates cache
    - cleanup() removes the directory
    - purge_all() wipes every snapshot-* dir
    - books / prices chunk save+load roundtrip (dict + dataclass-like)
    - --no-cache style purge skips fetch entirely
"""

from __future__ import annotations

import json
import time
from pathlib import Path

from polyarb.config import Settings
from polyarb.snapshot.cache import (
    MAX_AGE_S,
    CACHE_VERSION,
    ChunkCache,
    _fingerprint_settings,
    _fingerprint_tokens,
)


# ─── helpers ───────────────────────────────────────────────────────────────────


class _FakeBook:
    """Mimic py-clob-client OrderBookSummary (has __dict__, not a dict)."""

    def __init__(self, asset_id: str, bids: list, asks: list) -> None:
        self.asset_id = asset_id
        self.bids = bids
        self.asks = asks


def _make_settings(
    tmp_cache_root: Path, threshold: float = 1000.0, batch: int = 500
) -> Settings:
    return Settings(
        cache_root=tmp_cache_root,
        liquidity_threshold_usd=threshold,
        clob_batch_size=batch,
    )


# ─── fingerprint stability ─────────────────────────────────────────────────────


def test_fingerprint_settings_stable_across_calls(tmp_cache_root: Path) -> None:
    s = _make_settings(tmp_cache_root)
    assert _fingerprint_settings(s) == _fingerprint_settings(s)


def test_fingerprint_settings_changes_with_threshold(tmp_cache_root: Path) -> None:
    a = _make_settings(tmp_cache_root, threshold=1000.0)
    b = _make_settings(tmp_cache_root, threshold=500.0)
    assert _fingerprint_settings(a) != _fingerprint_settings(b)


def test_fingerprint_tokens_order_independent() -> None:
    assert _fingerprint_tokens(["a", "b", "c"]) == _fingerprint_tokens(["c", "b", "a"])


def test_fingerprint_tokens_changes_with_membership() -> None:
    assert _fingerprint_tokens(["a", "b"]) != _fingerprint_tokens(["a", "b", "c"])


# ─── try_resume / init ─────────────────────────────────────────────────────────


def test_fresh_run_initializes_meta(tmp_cache_root: Path) -> None:
    s = _make_settings(tmp_cache_root)
    cache = ChunkCache(tmp_cache_root, 1700000000000, s, ["t1", "t2"], "subset")
    found = cache.try_resume()
    assert found is False
    assert cache.resumed is False

    meta = json.loads((cache.dir / "meta.json").read_text())
    assert meta["version"] == CACHE_VERSION
    assert meta["mode"] == "subset"
    assert meta["taken_at_ms"] == 1700000000000
    assert meta["settings_fingerprint"] == _fingerprint_settings(s)
    assert meta["token_ids_fingerprint"] == _fingerprint_tokens(["t1", "t2"])


def test_second_run_with_same_inputs_resumes(tmp_cache_root: Path) -> None:
    s = _make_settings(tmp_cache_root)
    tokens = ["t1", "t2"]

    # First run: init + write a chunk
    c1 = ChunkCache(tmp_cache_root, 1700000000000, s, tokens, "subset")
    c1.try_resume()
    c1.save_books_chunk(1, [{"asset_id": "t1", "bids": [], "asks": []}])

    # Second run with a new taken_at_ms but same tokens + settings
    c2 = ChunkCache(tmp_cache_root, 1700000999999, s, tokens, "subset")
    found = c2.try_resume()
    assert found is True
    assert c2.resumed is True
    assert c2.has_books_chunk(1)
    cached = c2.load_books_chunk(1)
    assert cached[0]["asset_id"] == "t1"


def test_settings_drift_invalidates(tmp_cache_root: Path) -> None:
    s1 = _make_settings(tmp_cache_root, threshold=1000.0)
    c1 = ChunkCache(tmp_cache_root, 1700000000000, s1, ["t1"], "subset")
    c1.try_resume()
    c1.save_books_chunk(1, [{"asset_id": "t1"}])

    s2 = _make_settings(tmp_cache_root, threshold=500.0)
    c2 = ChunkCache(tmp_cache_root, 1700000999999, s2, ["t1"], "subset")
    found = c2.try_resume()
    assert found is False
    # old cache should be wiped
    snapshots = list(tmp_cache_root.glob("snapshot-*"))
    assert len(snapshots) == 1  # only the new one


def test_tokens_drift_invalidates(tmp_cache_root: Path) -> None:
    s = _make_settings(tmp_cache_root)
    c1 = ChunkCache(tmp_cache_root, 1700000000000, s, ["t1", "t2"], "subset")
    c1.try_resume()
    c1.save_books_chunk(1, [{"asset_id": "t1"}])

    c2 = ChunkCache(tmp_cache_root, 1700000999999, s, ["t1", "t2", "t3"], "subset")
    found = c2.try_resume()
    assert found is False


def test_mode_drift_invalidates(tmp_cache_root: Path) -> None:
    s = _make_settings(tmp_cache_root)
    c1 = ChunkCache(tmp_cache_root, 1700000000000, s, ["t1"], "subset")
    c1.try_resume()
    c1.save_books_chunk(1, [{"asset_id": "t1"}])

    c2 = ChunkCache(tmp_cache_root, 1700000999999, s, ["t1"], "full")
    found = c2.try_resume()
    assert found is False


def test_expired_cache_invalidates(tmp_cache_root: Path) -> None:
    s = _make_settings(tmp_cache_root)
    c1 = ChunkCache(tmp_cache_root, 1700000000000, s, ["t1"], "subset")
    c1.try_resume()

    # Backdate created_at_ms to be older than MAX_AGE_S
    meta_path = c1.dir / "meta.json"
    meta = json.loads(meta_path.read_text())
    meta["created_at_ms"] = int((time.time() - MAX_AGE_S - 60) * 1000)
    meta_path.write_text(json.dumps(meta))

    c2 = ChunkCache(tmp_cache_root, 1700000999999, s, ["t1"], "subset")
    found = c2.try_resume()
    assert found is False


def test_corrupted_meta_invalidates(tmp_cache_root: Path) -> None:
    s = _make_settings(tmp_cache_root)
    c1 = ChunkCache(tmp_cache_root, 1700000000000, s, ["t1"], "subset")
    c1.try_resume()
    (c1.dir / "meta.json").write_text("{not json")

    c2 = ChunkCache(tmp_cache_root, 1700000999999, s, ["t1"], "subset")
    found = c2.try_resume()
    assert found is False


# ─── chunk save/load ──────────────────────────────────────────────────────────


def test_books_chunk_roundtrip_dict(tmp_cache_root: Path) -> None:
    s = _make_settings(tmp_cache_root)
    cache = ChunkCache(tmp_cache_root, 1700000000000, s, ["t1"], "subset")
    cache.try_resume()

    books = [{"asset_id": "t1", "bids": [{"price": "0.45", "size": "10"}], "asks": []}]
    cache.save_books_chunk(3, books)
    assert cache.has_books_chunk(3)
    assert cache.load_books_chunk(3) == books


def test_books_chunk_roundtrip_dataclass_like(tmp_cache_root: Path) -> None:
    """SDK OrderBookSummary has __dict__ but is not a dict — must serialize via vars()."""
    s = _make_settings(tmp_cache_root)
    cache = ChunkCache(tmp_cache_root, 1700000000000, s, ["t1"], "subset")
    cache.try_resume()

    fake = _FakeBook(asset_id="t1", bids=[], asks=[{"price": "0.50", "size": "5"}])
    cache.save_books_chunk(1, [fake])
    loaded = cache.load_books_chunk(1)
    assert loaded[0]["asset_id"] == "t1"
    assert loaded[0]["asks"][0]["price"] == "0.50"


def test_books_chunk_skips_none(tmp_cache_root: Path) -> None:
    s = _make_settings(tmp_cache_root)
    cache = ChunkCache(tmp_cache_root, 1700000000000, s, ["t1"], "subset")
    cache.try_resume()
    cache.save_books_chunk(1, [None, {"asset_id": "t1"}, None])
    loaded = cache.load_books_chunk(1)
    assert len(loaded) == 1
    assert loaded[0]["asset_id"] == "t1"


def test_prices_chunk_roundtrip_buy(tmp_cache_root: Path) -> None:
    s = _make_settings(tmp_cache_root)
    cache = ChunkCache(tmp_cache_root, 1700000000000, s, ["t1"], "subset")
    cache.try_resume()

    page = {"t1": {"BUY": "0.46"}}
    cache.save_prices_chunk("BUY", 5, page)
    assert cache.has_prices_chunk("BUY", 5)
    assert cache.load_prices_chunk("BUY", 5) == page


def test_prices_chunk_buy_and_sell_separate(tmp_cache_root: Path) -> None:
    s = _make_settings(tmp_cache_root)
    cache = ChunkCache(tmp_cache_root, 1700000000000, s, ["t1"], "subset")
    cache.try_resume()

    cache.save_prices_chunk("BUY", 1, {"t1": {"BUY": "0.46"}})
    cache.save_prices_chunk("SELL", 1, {"t1": {"SELL": "0.47"}})
    assert cache.load_prices_chunk("BUY", 1)["t1"]["BUY"] == "0.46"
    assert cache.load_prices_chunk("SELL", 1)["t1"]["SELL"] == "0.47"


def test_chunk_files_named_with_zero_padding(tmp_cache_root: Path) -> None:
    """Chunk numbering uses 3-digit zero-padding so dir listing sorts naturally."""
    s = _make_settings(tmp_cache_root)
    cache = ChunkCache(tmp_cache_root, 1700000000000, s, ["t1"], "subset")
    cache.try_resume()
    cache.save_books_chunk(7, [{"asset_id": "t1"}])
    assert (cache.dir / "books" / "chunk-007.json").exists()


# ─── cleanup / purge ──────────────────────────────────────────────────────────


def test_cleanup_removes_directory(tmp_cache_root: Path) -> None:
    s = _make_settings(tmp_cache_root)
    cache = ChunkCache(tmp_cache_root, 1700000000000, s, ["t1"], "subset")
    cache.try_resume()
    assert cache.dir.exists()
    cache.cleanup()
    assert not cache.dir.exists()


def test_purge_all_removes_only_snapshot_dirs(tmp_cache_root: Path) -> None:
    tmp_cache_root.mkdir(parents=True, exist_ok=True)
    (tmp_cache_root / "snapshot-1").mkdir()
    (tmp_cache_root / "snapshot-2").mkdir()
    (tmp_cache_root / "unrelated_dir").mkdir()
    (tmp_cache_root / "loose_file.txt").write_text("keep me")

    n = ChunkCache.purge_all(tmp_cache_root)
    assert n == 2
    assert not (tmp_cache_root / "snapshot-1").exists()
    assert not (tmp_cache_root / "snapshot-2").exists()
    assert (tmp_cache_root / "unrelated_dir").exists()
    assert (tmp_cache_root / "loose_file.txt").exists()


def test_purge_all_handles_missing_root(tmp_path: Path) -> None:
    nonexistent = tmp_path / "does-not-exist"
    assert ChunkCache.purge_all(nonexistent) == 0
