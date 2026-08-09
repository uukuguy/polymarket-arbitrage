# M1 Isolated SQLite Volume Compaction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use inline TDD task-by-task. Steps use checkbox syntax for tracking.

**Goal:** compact the retained M1 SQLite history on an isolated Fly volume and
provide a receipt-backed, reversible promotion protocol without running
`VACUUM` on the live Quote producer.

**Architecture:** a local operator tool first creates an online-consistent
SQLite backup manifest, then validates an isolated replacement volume and its
fresh Quote/health evidence. Promotion is a separately authorized Fly
operation: the old machine and volume remain the rollback source until the
replacement proves the same exact release and a fresh certified Quote run.

**Tech Stack:** Python 3.12 `sqlite3.Connection.backup`, SHA-256, boto3/R2,
Fly Machines/Volumes, pytest, Ruff, Makefile.

## Global constraints

- Never run `VACUUM`, `VACUUM INTO`, destructive delete, or volume resize on
  `/data/state.db` of the live machine.
- The promotion receipt binds source `releaseId`, machine, volume, page facts,
  SQLite integrity result, and exact source path. The artifact manifest binds
  the immutable backup SHA-256; a live source is intentionally not required to
  retain one digest while it is being backed up.
- R2 objects use content-addressed keys and exclusive manifest outputs; an
  existing object with different digest is a hard error.
- A clone/replacement runs with public service disabled until it has produced a
  fresh certified Quote and direct console/health evidence.
- Promotion and destruction are not implied by backup, restore, or validation.

## Task 1: Backup artifact primitive

**Files:** create `src/polyarb/ops/sqlite_volume_backup.py`; create
`tests/ops/test_sqlite_volume_backup.py`.

**Interfaces:**

```python
@dataclass(frozen=True)
class SQLiteBackupManifest:
    source_path: Path
    backup_path: Path
    backup_sha256: str
    page_count: int
    page_size: int
    freelist_count: int
    integrity_check: str

def backup_sqlite(source: Path, destination: Path, *, pages_per_step: int) -> SQLiteBackupManifest: ...
def verify_sqlite(path: Path) -> SQLiteBackupManifest: ...
```

- [x] Write a failing test that mutates a WAL database during a throttled backup
  and asserts the resulting backup passes `integrity_check`, has a stable
  digest, and is readable independently.
- [x] Run the focused test; it failed because the module was absent.
- [x] Implement `backup_sqlite` with `sqlite3.Connection.backup` to a caller
  supplied new path, `pages=pages_per_step`, progress callback support,
  `wal_checkpoint(PASSIVE)` only, and no source write pragma. Hash only after
  closing both connections; reject non-`ok` integrity output.
- [x] Add failing tests for existing destination and missing source, then
  implement the minimal refusal paths. A source fingerprint mismatch is not a
  valid invariant for a changing WAL database; the immutable backup digest is
  verified instead.
- [x] Run `uv run pytest tests/ops/test_sqlite_volume_backup.py -q` and Ruff.
- [ ] Commit `feat(m1): add verified sqlite backup primitive`.

## Task 2: R2 transfer and restore verifier

**Files:** modify `src/polyarb/ops/sqlite_volume_backup.py`; create
`src/polyarb/cli_volume_recovery.py`; create
`tests/ops/test_sqlite_volume_backup.py`; modify `Makefile`.

**Interfaces:**

```python
def upload_backup(*, manifest: SQLiteBackupManifest, backup: Path, settings: Settings) -> str: ...
def restore_and_verify(*, object_key: str, destination: Path, settings: Settings) -> SQLiteBackupManifest: ...
```

- [ ] Write failing Stubber tests proving upload records `sha256` and source
  identity metadata, restore refuses an object whose digest differs from its
  manifest, and R2 failures leave no success receipt.
- [ ] Run the focused tests; expect missing CLI/upload functions.
- [ ] Implement multipart-safe boto3 transfer through the existing R2 client
  configuration; stream hashes, use a deterministic
  `volume-backups/<source-digest>/state.db` key, and write the JSON manifest
  only after object-head digest/size verification.
- [ ] Add `make sqlite-volume-backup` and `make sqlite-volume-restore-verify`.
  Both require explicit new local output paths and must refuse overwrite.
- [ ] Run CLI, Stubber, Makefile contract, Ruff and docs checks.
- [ ] Commit `feat(m1): verify sqlite volume backup transfer`.

## Task 3: Isolated replacement qualification

**Files:** create `scripts/qualify_replacement_volume.py`; create
`tests/ops/test_qualify_replacement_volume.py`; modify `Makefile` and
`docs/M1-市场感知平台使用手册.md`.

**Interfaces:** the script accepts exact expected release, source manifest,
replacement health URL and output path. It emits one exclusive immutable JSON
verdict only when all are true: release matches, `integrity_check=ok`, direct
console is 200, `open_count=0`, and a newly completed Quote run has a timestamp
after replacement boot and remains inside the existing freshness SLA.

- [ ] Write failing local HTTP-fixture tests for release mismatch, old Quote
  run, console failure, open incident, and a complete success case.
- [ ] Implement bounded HTTPS/HTTP GETs with strict schemas and absolute
  deadlines. The script only creates its specified new result file; it never
  calls Fly control APIs.
- [ ] Add `make qualify-replacement-volume manifest=<path> url=<https-url>
  expected_release=<sha> output=<new-path>`.
- [ ] Document the operational sequence: snapshot/fork or clone only after a
  verified backup manifest; start the isolated machine without public service;
  restore/compact there; qualify; explicitly promote; retain old source during
  the observation period.
- [ ] Run focused tests, Makefile/docs contracts and Ruff.
- [ ] Commit `feat(m1): qualify isolated sqlite replacement`.

## Task 4: Production-only promotion runbook

**Files:** modify `docs/M1-市场感知平台使用手册.md`; create
`docs/superpowers/plans/2026-08-10-m1-isolated-sqlite-volume-compaction-SUMMARY.md` after execution.

- [ ] Record the exact current source facts: app `polyarb-l1`, volume
  `vol_40olm80dgol2xqn4`, live machine `867060ce772748`, `auto_vacuum=0`, and
  the source release.
- [ ] Require a fresh pre-promotion backup artifact, source and replacement
  manifest digest equality, replacement qualification PASS, an explicit target
  machine/volume, and a rollback command before any routing mutation.
- [ ] Use Fly volume snapshot/fork/clone only as an isolated copy mechanism;
  do not attach a second volume to the live machine or start a second public
  M1 producer against the same routing path.
- [ ] After explicit promotion, observe at least five natural Quote cycles,
  zero open incidents, direct console 200, Polywatch recovery, and stable
  free-space facts before retiring the old volume. No retirement is automatic.

## Verification

1. No test or command can compact the live mounted database.
2. Every backup/restore byte is digest and integrity checked.
3. A replacement cannot be qualified using a stale Quote or a mismatched
   release/manifest.
4. Production promotion remains an explicit, reversible operation.
