"""Create and verify a consistent SQLite backup without touching the live database.

The caller chooses a new destination.  This module never overwrites an existing
artifact and deliberately has no Fly, R2, or traffic-switching integration.
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path


class SQLiteBackupError(RuntimeError):
    """A backup or verification could not be completed safely."""


class SQLiteBackupRefusal(SQLiteBackupError):
    """The requested operation would violate a non-destructive invariant."""


@dataclass(frozen=True)
class SQLiteBackupManifest:
    """Facts measured from the completed backup artifact."""

    source_path: Path
    backup_path: Path
    backup_sha256: str
    backup_size_bytes: int
    page_count: int
    page_size: int
    freelist_count: int
    integrity_check: str


def backup_sqlite(
    source_path: Path,
    backup_path: Path,
    *,
    pages_per_step: int = 256,
    progress: Callable[[int, int, int], None] | None = None,
) -> SQLiteBackupManifest:
    """Back up ``source_path`` to a new file and return verified artifact facts.

    ``sqlite3.Connection.backup`` holds a consistent SQLite snapshot while
    writers continue.  The destination is first written as a sibling partial
    file and atomically published only after integrity verification succeeds.
    """

    source = Path(source_path)
    backup = Path(backup_path)
    _validate_backup_request(source, backup, pages_per_step)
    partial = backup.with_name(f".{backup.name}.partial")
    if partial.exists():
        raise SQLiteBackupRefusal(f"partial-destination-already-exists:{partial}")

    try:
        source_uri = f"{source.resolve().as_uri()}?mode=ro"
        with sqlite3.connect(source_uri, uri=True) as source_connection:
            with sqlite3.connect(partial) as backup_connection:
                source_connection.backup(
                    backup_connection,
                    pages=pages_per_step,
                    progress=progress,
                )
        manifest = verify_sqlite(partial, source_path=source)
        os.replace(partial, backup)
        return SQLiteBackupManifest(
            source_path=source,
            backup_path=backup,
            backup_sha256=manifest.backup_sha256,
            backup_size_bytes=manifest.backup_size_bytes,
            page_count=manifest.page_count,
            page_size=manifest.page_size,
            freelist_count=manifest.freelist_count,
            integrity_check=manifest.integrity_check,
        )
    except Exception:
        partial.unlink(missing_ok=True)
        raise


def verify_sqlite(path: Path, *, source_path: Path | None = None) -> SQLiteBackupManifest:
    """Run SQLite integrity validation and calculate an artifact fingerprint."""

    artifact = Path(path)
    if not artifact.is_file():
        raise SQLiteBackupRefusal(f"backup-not-a-file:{artifact}")

    with sqlite3.connect(artifact) as connection:
        integrity_row = connection.execute("PRAGMA integrity_check").fetchone()
        integrity_check = "" if integrity_row is None else str(integrity_row[0])
        if integrity_check != "ok":
            raise SQLiteBackupError(f"integrity-check-failed:{integrity_check}")
        page_count = int(connection.execute("PRAGMA page_count").fetchone()[0])
        page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
        freelist_count = int(connection.execute("PRAGMA freelist_count").fetchone()[0])

    return SQLiteBackupManifest(
        source_path=artifact if source_path is None else Path(source_path),
        backup_path=artifact,
        backup_sha256=_sha256(artifact),
        backup_size_bytes=artifact.stat().st_size,
        page_count=page_count,
        page_size=page_size,
        freelist_count=freelist_count,
        integrity_check=integrity_check,
    )


def _validate_backup_request(source: Path, backup: Path, pages_per_step: int) -> None:
    invalid_page_count = (
        isinstance(pages_per_step, bool)
        or not isinstance(pages_per_step, int)
        or pages_per_step < 1
    )
    if invalid_page_count:
        raise SQLiteBackupRefusal("pages-per-step-must-be-positive-integer")
    if not source.is_file():
        raise SQLiteBackupRefusal(f"source-not-a-file:{source}")
    if not backup.parent.is_dir():
        raise SQLiteBackupRefusal(f"destination-parent-missing:{backup.parent}")
    if source.resolve() == backup.resolve():
        raise SQLiteBackupRefusal("source-and-destination-must-differ")
    if backup.exists():
        raise SQLiteBackupRefusal(f"destination-already-exists:{backup}")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as artifact:
        for block in iter(lambda: artifact.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()
