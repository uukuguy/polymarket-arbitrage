"""Descriptor-based artifact I/O resistant to links and replacement races."""

from __future__ import annotations

import os
import secrets
import stat
from pathlib import Path

_MAX_ARTIFACT_BYTES = 1_048_576


def read_stable_bytes(path: Path) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags)
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_size > _MAX_ARTIFACT_BYTES:
            raise ValueError("unsafe-artifact-input")
        chunks: list[bytes] = []
        remaining = _MAX_ARTIFACT_BYTES + 1
        while remaining:
            chunk = os.read(fd, min(65_536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        value = b"".join(chunks)
        after = os.fstat(fd)
        if (
            len(value) > _MAX_ARTIFACT_BYTES
            or (before.st_dev, before.st_ino, before.st_size)
            != (after.st_dev, after.st_ino, after.st_size)
            or len(value) != before.st_size
        ):
            raise ValueError("unstable-artifact-input")
        return value
    finally:
        os.close(fd)


def write_exclusive_bytes(path: Path, value: bytes) -> None:
    parent_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(
        os, "O_NOFOLLOW", 0
    )
    parent_fd = os.open(path.parent, parent_flags)
    try:
        parent = os.fstat(parent_fd)
        if not stat.S_ISDIR(parent.st_mode) or parent.st_uid != os.geteuid():
            raise ValueError("unsafe-artifact-parent")
        temporary = path.parent / f".{path.name}.tmp-{secrets.token_hex(16)}"
        try:
            fd = os.open(
                temporary,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL
                | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            try:
                written = 0
                while written < len(value):
                    written += os.write(fd, value[written:])
                os.fsync(fd)
                current = os.fstat(fd)
                if not stat.S_ISREG(current.st_mode) or current.st_size != len(value):
                    raise ValueError("unsafe-artifact-output")
            finally:
                os.close(fd)
            os.link(temporary, path, follow_symlinks=False)
            os.fsync(parent_fd)
        finally:
            try:
                os.unlink(temporary)
                os.fsync(parent_fd)
            except FileNotFoundError:
                pass
    finally:
        os.close(parent_fd)
