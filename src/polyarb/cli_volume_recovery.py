"""Offline SQLite backup transfer and restore verifier; never controls Fly."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from polyarb.config import load_settings
from polyarb.ops.sqlite_volume_backup import (
    SQLiteBackupManifest,
    backup_sqlite,
    restore_and_verify,
    upload_backup,
)


def _manifest_dict(manifest: SQLiteBackupManifest) -> dict[str, object]:
    return {
        "source_path": str(manifest.source_path),
        "backup_path": str(manifest.backup_path),
        "backup_sha256": manifest.backup_sha256,
        "backup_size_bytes": manifest.backup_size_bytes,
        "page_count": manifest.page_count,
        "page_size": manifest.page_size,
        "freelist_count": manifest.freelist_count,
        "integrity_check": manifest.integrity_check,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    backup = commands.add_parser("backup")
    backup.add_argument("--source", type=Path, required=True)
    backup.add_argument("--destination", type=Path, required=True)
    backup.add_argument("--pages-per-step", type=int, default=256)
    restore = commands.add_parser("restore-verify")
    restore.add_argument("--object-key", required=True)
    restore.add_argument("--destination", type=Path, required=True)
    restore.add_argument("--manifest", type=Path, required=True)
    args = parser.parse_args(argv)
    settings = load_settings()
    if args.command == "backup":
        manifest = backup_sqlite(args.source, args.destination, pages_per_step=args.pages_per_step)
        object_key = upload_backup(manifest=manifest, backup=args.destination, settings=settings)
        print(json.dumps({**_manifest_dict(manifest), "object_key": object_key}, sort_keys=True))
        return 0
    facts = json.loads(args.manifest.read_text())
    manifest = SQLiteBackupManifest(
        source_path=Path(facts["source_path"]), backup_path=Path(facts["backup_path"]),
        backup_sha256=facts["backup_sha256"], backup_size_bytes=int(facts["backup_size_bytes"]),
        page_count=int(facts["page_count"]), page_size=int(facts["page_size"]),
        freelist_count=int(facts["freelist_count"]), integrity_check=facts["integrity_check"],
    )
    restored = restore_and_verify(
        object_key=args.object_key,
        destination=args.destination,
        expected_manifest=manifest,
        settings=settings,
    )
    print(json.dumps(_manifest_dict(restored), sort_keys=True))
    return 0
