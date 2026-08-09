from __future__ import annotations

import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest


def _source_db(path: Path) -> None:
    with sqlite3.connect(path) as con:
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("CREATE TABLE readings(id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
        con.executemany(
            "INSERT INTO readings(value) VALUES (?)",
            ((f"reading-{index}",) for index in range(32)),
        )


def test_online_backup_is_integrity_checked_and_independently_readable(
    tmp_path: Path,
) -> None:
    from polyarb.ops.sqlite_volume_backup import backup_sqlite, verify_sqlite

    source = tmp_path / "source.db"
    backup = tmp_path / "backup.db"
    _source_db(source)

    manifest = backup_sqlite(source, backup, pages_per_step=1)

    assert manifest.source_path == source
    assert manifest.backup_path == backup
    assert manifest.integrity_check == "ok"
    assert manifest.backup_sha256 == verify_sqlite(backup).backup_sha256
    with sqlite3.connect(backup) as con:
        assert con.execute("SELECT COUNT(*) FROM readings").fetchone() == (32,)


def test_online_backup_stays_valid_when_wal_source_changes(tmp_path: Path) -> None:
    from polyarb.ops.sqlite_volume_backup import backup_sqlite

    source = tmp_path / "source.db"
    backup = tmp_path / "backup.db"
    _source_db(source)
    with sqlite3.connect(source) as con:
        con.executemany(
            "INSERT INTO readings(value) VALUES (?)",
            ((f"large-reading-{index}",) for index in range(8_000)),
        )

    changed = False

    def write_during_backup(_status: int, _remaining: int, _total: int) -> None:
        nonlocal changed
        if changed:
            return
        with sqlite3.connect(source) as con:
            con.execute("INSERT INTO readings(value) VALUES ('late-write')")
        changed = True

    manifest = backup_sqlite(source, backup, pages_per_step=1, progress=write_during_backup)

    assert changed is True
    assert manifest.integrity_check == "ok"
    with sqlite3.connect(backup) as con:
        assert con.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert con.execute("SELECT COUNT(*) FROM readings").fetchone()[0] >= 8_032


def test_backup_refuses_existing_destination(tmp_path: Path) -> None:
    from polyarb.ops.sqlite_volume_backup import SQLiteBackupRefusal, backup_sqlite

    source = tmp_path / "source.db"
    backup = tmp_path / "backup.db"
    _source_db(source)
    backup.write_bytes(b"must-not-overwrite")

    with pytest.raises(SQLiteBackupRefusal, match="destination-already-exists"):
        backup_sqlite(source, backup, pages_per_step=1)


def test_backup_refuses_missing_source(tmp_path: Path) -> None:
    from polyarb.ops.sqlite_volume_backup import SQLiteBackupRefusal, backup_sqlite

    with pytest.raises(SQLiteBackupRefusal, match="source-not-a-file"):
        backup_sqlite(tmp_path / "missing.db", tmp_path / "backup.db")


def test_upload_backup_verifies_object_digest_before_writing_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from polyarb.ops import sqlite_volume_backup as recovery

    source = tmp_path / "source.db"
    backup = tmp_path / "backup.db"
    _source_db(source)
    manifest = recovery.backup_sqlite(source, backup)
    calls: list[tuple[str, dict]] = []

    class Client:
        def upload_file(self, filename, bucket, key, ExtraArgs):
            calls.append(
                (
                    "upload",
                    {
                        "filename": filename,
                        "bucket": bucket,
                        "key": key,
                        "extra": ExtraArgs,
                    },
                )
            )

        def head_object(self, **kwargs):
            calls.append(("head", kwargs))
            return {
                "ContentLength": manifest.backup_size_bytes,
                "Metadata": {"sha256": manifest.backup_sha256},
            }

        def put_object(self, **kwargs):
            calls.append(("manifest", kwargs))

    monkeypatch.setattr(recovery, "_r2_client", lambda _settings: Client())
    settings = SimpleNamespace(r2_bucket="bucket", r2_endpoint="https://r2.example")

    object_key = recovery.upload_backup(manifest=manifest, backup=backup, settings=settings)

    assert object_key == f"volume-backups/{manifest.backup_sha256}/state.db"
    assert [kind for kind, _ in calls] == ["upload", "head", "manifest"]
    assert calls[0][1]["extra"]["Metadata"]["sha256"] == manifest.backup_sha256
