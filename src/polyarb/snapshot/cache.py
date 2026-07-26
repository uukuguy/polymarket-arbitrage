"""Snapshot CLOB chunk cache — resume long snapshot runs after interruption.

A long snapshot run (~26 min for 17k subset) can be interrupted by network
flakiness, manual Ctrl+C, or laptop sleep. Without a cache, the next run starts
from zero. This module persists each CLOB chunk to disk as it completes so a
subsequent run can skip already-fetched chunks.

Cache layout:

    {cache_root}/snapshot-{taken_at_ms}/
        meta.json                       — fingerprint + chunk counts
        books/chunk-{i:03d}.json        — list[dict] (asset_id, bids, asks, ...)
        prices/buy/chunk-{i:03d}.json   — {token_id: {"BUY": "<price>"}}
        prices/sell/chunk-{i:03d}.json  — {token_id: {"SELL": "<price>"}}

Reuse policy (all must hold or cache is discarded):
    1. settings_fingerprint matches  (sha256 of CLOB-relevant Settings fields)
    2. token_ids_fingerprint matches (sha256 of sorted target token list)
    3. cache mtime within MAX_AGE_S
    4. mode matches

A successful snapshot run calls cleanup() to delete its own cache directory.
A failed step-7 leaves the cache so the next run can reuse it.

OrderBookSummary serialization: the SDK returns dataclass-like objects with
``__dict__``. We serialize via ``vars(book)`` and rehydrate as plain dicts,
matching what _index_books_by_token consumes downstream.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import time
from pathlib import Path
from typing import Any

from loguru import logger

CACHE_VERSION = 1
MAX_AGE_S = 30 * 60  # 30 min — long enough for a single run, short enough to avoid stale data


def _fingerprint_settings(settings: Any) -> str:
    """Hash the CLOB-relevant Settings fields. Changing any invalidates cache."""
    payload = {
        "clob_url": str(getattr(settings, "clob_url", "")),
        "clob_batch_size": int(getattr(settings, "clob_batch_size", 0)),
        "liquidity_threshold_usd": float(getattr(settings, "liquidity_threshold_usd", 0.0)),
    }
    blob = json.dumps(payload, sort_keys=True).encode()
    return hashlib.sha256(blob).hexdigest()[:16]


def _fingerprint_tokens(token_ids: list[str]) -> str:
    """Hash the sorted target token list. Different tokens → different cache."""
    blob = json.dumps(sorted(token_ids)).encode()
    return hashlib.sha256(blob).hexdigest()[:16]


def _book_to_jsonable(book: Any) -> dict:
    """Convert a SDK OrderBookSummary (or already-dict) to a JSON-serializable dict."""
    if isinstance(book, dict):
        return book
    if hasattr(book, "__dict__"):
        return dict(vars(book))
    raise TypeError(f"cannot serialize book of type {type(book).__name__}")


class ChunkCache:
    """Resumable per-chunk cache for a single snapshot run.

    Construction does not touch disk. Call ``try_resume()`` to either load
    state from a prior run (reusable) or initialize a fresh cache directory.
    """

    def __init__(
        self,
        cache_root: Path,
        taken_at_ms: int,
        settings: Any,
        token_ids: list[str],
        mode: str,
    ) -> None:
        self._cache_root = Path(cache_root)
        self._taken_at_ms = taken_at_ms
        self._settings = settings
        self._token_ids = token_ids
        self._mode = mode

        self._settings_fp = _fingerprint_settings(settings)
        self._tokens_fp = _fingerprint_tokens(token_ids)

        self._dir = self._cache_root / f"snapshot-{taken_at_ms}"
        self._books_dir = self._dir / "books"
        self._prices_buy_dir = self._dir / "prices" / "buy"
        self._prices_sell_dir = self._dir / "prices" / "sell"
        self._meta_path = self._dir / "meta.json"

        self._resumed = False  # True if try_resume() found a reusable cache

    @property
    def dir(self) -> Path:
        return self._dir

    @property
    def resumed(self) -> bool:
        return self._resumed

    # ── Resume / init ────────────────────────────────────────────────────────

    def try_resume(self) -> bool:
        """Look for a reusable cache. Return True if found, False if fresh start.

        Searches ``cache_root`` for any ``snapshot-*`` directory whose meta.json
        matches the current settings + tokens fingerprint and is within MAX_AGE_S.

        On match: rebinds this cache to the existing directory (so chunks accumulate
        in the same place) and returns True.

        On miss: initializes a fresh directory at self._dir and returns False.

        Stale / mismatched directories are cleaned up as a side effect.
        """
        if not self._cache_root.exists():
            self._init_fresh()
            return False

        candidates: list[tuple[Path, dict]] = []
        now = time.time()

        for child in self._cache_root.iterdir():
            if not child.is_dir() or not child.name.startswith("snapshot-"):
                continue
            meta_path = child / "meta.json"
            if not meta_path.exists():
                self._safe_rmtree(child)
                continue
            try:
                meta = json.loads(meta_path.read_text())
            except (json.JSONDecodeError, OSError):
                logger.warning(f"cache: discarding unreadable {child}")
                self._safe_rmtree(child)
                continue

            age = now - (meta.get("created_at_ms", 0) / 1000)
            if age > MAX_AGE_S:
                logger.info(f"cache: discarding expired {child} (age {age:.0f}s)")
                self._safe_rmtree(child)
                continue

            if (
                meta.get("settings_fingerprint") != self._settings_fp
                or meta.get("token_ids_fingerprint") != self._tokens_fp
                or meta.get("mode") != self._mode
            ):
                logger.info(f"cache: discarding mismatched {child} (settings/tokens/mode changed)")
                self._safe_rmtree(child)
                continue

            candidates.append((child, meta))

        if not candidates:
            self._init_fresh()
            return False

        # Prefer the most recently created reusable cache.
        candidates.sort(key=lambda x: x[1].get("created_at_ms", 0), reverse=True)
        best_dir, best_meta = candidates[0]

        # Rebind self._dir to the existing directory (so chunk numbers line up).
        self._dir = best_dir
        self._books_dir = best_dir / "books"
        self._prices_buy_dir = best_dir / "prices" / "buy"
        self._prices_sell_dir = best_dir / "prices" / "sell"
        self._meta_path = best_dir / "meta.json"
        self._taken_at_ms = best_meta.get("taken_at_ms", self._taken_at_ms)

        n_books = self._count_chunks(self._books_dir)
        n_buy = self._count_chunks(self._prices_buy_dir)
        n_sell = self._count_chunks(self._prices_sell_dir)
        logger.info(
            f"cache: resuming {best_dir.name} (books {n_books}, prices buy {n_buy}, sell {n_sell})"
        )
        self._resumed = True
        return True

    def _init_fresh(self) -> None:
        self._books_dir.mkdir(parents=True, exist_ok=True)
        self._prices_buy_dir.mkdir(parents=True, exist_ok=True)
        self._prices_sell_dir.mkdir(parents=True, exist_ok=True)
        meta = {
            "version": CACHE_VERSION,
            "taken_at_ms": self._taken_at_ms,
            "settings_fingerprint": self._settings_fp,
            "token_ids_fingerprint": self._tokens_fp,
            "mode": self._mode,
            "created_at_ms": int(time.time() * 1000),
        }
        self._meta_path.write_text(json.dumps(meta, indent=2))
        logger.info(f"cache: initialized fresh {self._dir.name}")

    @staticmethod
    def _count_chunks(d: Path) -> int:
        if not d.exists():
            return 0
        return sum(1 for _ in d.glob("chunk-*.json"))

    @staticmethod
    def _safe_rmtree(p: Path) -> None:
        try:
            shutil.rmtree(p, ignore_errors=True)
        except OSError as e:
            logger.warning(f"cache: failed to remove {p}: {e}")

    # ── Books ────────────────────────────────────────────────────────────────

    def has_books_chunk(self, i: int) -> bool:
        return (self._books_dir / f"chunk-{i:03d}.json").exists()

    def load_books_chunk(self, i: int) -> list[dict]:
        path = self._books_dir / f"chunk-{i:03d}.json"
        return json.loads(path.read_text())

    def save_books_chunk(self, i: int, books: list[Any]) -> None:
        path = self._books_dir / f"chunk-{i:03d}.json"
        tmp = path.with_suffix(".json.tmp")
        serializable = [_book_to_jsonable(b) for b in books if b is not None]
        tmp.write_text(json.dumps(serializable, default=str))
        tmp.replace(path)

    # ── Prices ───────────────────────────────────────────────────────────────

    def _prices_dir(self, side: str) -> Path:
        if side == "BUY":
            return self._prices_buy_dir
        if side == "SELL":
            return self._prices_sell_dir
        raise ValueError(f"unknown side {side!r}")

    def has_prices_chunk(self, side: str, i: int) -> bool:
        return (self._prices_dir(side) / f"chunk-{i:03d}.json").exists()

    def load_prices_chunk(self, side: str, i: int) -> dict:
        path = self._prices_dir(side) / f"chunk-{i:03d}.json"
        return json.loads(path.read_text())

    def save_prices_chunk(self, side: str, i: int, page: dict) -> None:
        path = self._prices_dir(side) / f"chunk-{i:03d}.json"
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(page, default=str))
        tmp.replace(path)

    # ── Cleanup ──────────────────────────────────────────────────────────────

    def cleanup(self) -> None:
        """Remove this cache directory. Called after a successful snapshot."""
        self._safe_rmtree(self._dir)
        logger.info(f"cache: cleaned up {self._dir.name}")

    @classmethod
    def purge_all(cls, cache_root: Path) -> int:
        """Delete every snapshot-* directory under cache_root. Used by --no-cache.

        Returns the number of directories removed.
        """
        if not cache_root.exists():
            return 0
        n = 0
        for child in cache_root.iterdir():
            if child.is_dir() and child.name.startswith("snapshot-"):
                cls._safe_rmtree(child)
                n += 1
        return n
