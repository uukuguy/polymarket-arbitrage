#!/usr/bin/env python3
"""Fail-closed operator CLI for immutable L3 continuous-soak evidence.

Read commands use only the least-privileged L2 runtime credential.  Retention
cleanup and migration-revision proof have deliberately separate credentials.
No command renders a DSN, password, or exception representation.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from dataclasses import dataclass, fields, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit
from uuid import NAMESPACE_URL, UUID, uuid5

import asyncpg

from polyarb.observation.l3_evidence import (
    EVIDENCE_TABLES,
    AcceptanceConfig,
    HealthStatus,
)
from polyarb.observation.l3_soak_verdict import (
    L3SoakReport,
    ManifestReport,
    SoakManifest,
    VerdictReason,
    VerdictStatus,
    build_soak_report,
    canonical_manifest_bytes,
    canonical_report_bytes,
    parse_manifest_bytes,
    parse_report_bytes,
    render_report,
)
from polyarb.storage.l3_evidence_store import L3EvidenceStore, L3RetentionOperator

EXIT_OK = 0
EXIT_NOT_CLOSED = 1
EXIT_USAGE = 2
EXIT_UNAVAILABLE = 3
RETENTION_APPROVAL = "DELETE_EXPIRED_L3_EVIDENCE"
_REF_CHARS = frozenset("abcdefghijklmnopqrstuvwxyz0123456789")
_MANIFEST_EVENT_SEQ_BASE = 8_000_000_000_000_000_000
_MANIFEST_EVENT_SEQ_SPAN = 1_000_000_000_000_000


class OperatorError(RuntimeError):
    """A safe, already-redacted operator error."""


@dataclass(frozen=True, slots=True)
class DatabaseTarget:
    host: str
    database: str
    user: str
    project_ref: str


def parse_rfc3339(value: str) -> datetime:
    """Parse an explicit UTC RFC3339 instant; offsets and naive values are refused."""
    if not isinstance(value, str) or not value:
        raise ValueError("timestamp must be non-empty RFC3339 UTC")
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError as error:
        raise ValueError("timestamp must be RFC3339 UTC") from error
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError("timestamp must use UTC")
    return parsed.astimezone(UTC)


def _utc_text(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("timestamp must use UTC")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise OperatorError(f"required credential environment variable is missing: {name}")
    return value


def validate_supabase_target(dsn: str, *, expected_ref: str) -> DatabaseTarget:
    """Allow only one explicit Supabase project target without retaining credentials."""
    if (
        not isinstance(expected_ref, str)
        or len(expected_ref) != 20
        or any(char not in _REF_CHARS for char in expected_ref)
    ):
        raise ValueError("expected project ref is invalid")
    try:
        parsed = urlsplit(dsn)
        host = (parsed.hostname or "").lower()
        database = unquote(parsed.path.lstrip("/"))
        user = unquote(parsed.username or "")
    except (TypeError, ValueError) as error:
        raise ValueError("database target is invalid") from error
    if parsed.scheme not in {"postgres", "postgresql"} or not host or not user:
        raise ValueError("database target is invalid")
    direct_host = f"db.{expected_ref}.supabase.co"
    pooler_host = host.endswith(".pooler.supabase.com")
    direct = host == direct_host
    pooler_suffix = f".{expected_ref}"
    pooled = (
        pooler_host
        and parsed.port == 5432
        and user.endswith(pooler_suffix)
        and bool(user[: -len(pooler_suffix)])
    )
    if not (direct or pooled) or database != "postgres":
        raise ValueError("database target does not match the allowlisted project")
    return DatabaseTarget(host=host, database=database, user=user, project_ref=expected_ref)


def _database_role(target: DatabaseTarget) -> str:
    """Return the database role encoded by a direct or shared-pooler login."""
    if ".pooler.supabase.com" not in target.host:
        return target.user
    suffix = f".{target.project_ref}"
    if not target.user.endswith(suffix):
        raise ValueError("pooler user does not contain the exact project suffix")
    role = target.user[: -len(suffix)]
    if not role:
        raise ValueError("pooler database role is empty")
    return role


def _read_manifest(path: Path) -> SoakManifest:
    try:
        return parse_manifest_bytes(path.read_bytes())
    except OSError as error:
        raise OperatorError("manifest is unavailable") from error


def _write_exclusive(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
    except BaseException:
        try:
            path.unlink()
        except OSError:
            pass
        raise


def write_new_manifest(
    manifest: SoakManifest, *, output: Path, now: datetime | None = None
) -> None:
    current = now or datetime.now(UTC)
    if manifest.t0 <= current:
        raise ValueError("manifest T0 must be in the future")
    _write_exclusive(output, canonical_manifest_bytes(manifest))


def _manifest_reports(
    output: Path, t0: datetime, config: AcceptanceConfig
) -> tuple[ManifestReport, ...]:
    labels = ("T+0", "T+6", "T+12", "T+18", "T+24")
    ends = (t0 + timedelta(seconds=config.sample_interval_s),) + tuple(
        t0 + timedelta(hours=hours) for hours in (6, 12, 18, 24)
    )
    return tuple(
        ManifestReport(
            checkpoint=label,
            start=t0,
            end=end,
            path=str(output.with_name(f"{output.stem}-{label.replace('+', '')}.json")),
        )
        for label, end in zip(labels, ends, strict=True)
    )


async def _create_manifest_from_runtime(
    *, dsn: str, start: datetime, end: datetime, output: Path
) -> SoakManifest:
    if end - start != timedelta(hours=24):
        raise OperatorError("manifest bounds must be exactly 24 hours")
    await _runtime_preflight(dsn)
    connection = await asyncpg.connect(dsn=dsn)
    try:
        row = await connection.fetchrow(
            """
            SELECT boot.*, promote.mapping_hash
            FROM l3_runtime_boots AS boot
            JOIN LATERAL (
                SELECT mapping_hash FROM l3_promote_runs
                WHERE boot_id=boot.boot_id AND status='success'
                ORDER BY scheduled_at DESC, run_seq DESC LIMIT 1
            ) AS promote ON TRUE
            ORDER BY boot.started_at DESC LIMIT 1
            """
        )
    finally:
        await connection.close()
    if row is None:
        raise OperatorError("no eligible runtime identity is available")
    from polyarb.config import load_settings

    settings = load_settings()
    config = AcceptanceConfig.from_settings(
        settings,
        Path("src/polyarb/scan_recipes/l3-promote.yaml"),
        row["code_version"],
    )
    if config.digest() != row["acceptance_config_hash"]:
        raise OperatorError("runtime acceptance config does not match local configuration")
    image_ref = row["image_ref"]
    marker = "@sha256:"
    if marker not in image_ref:
        raise OperatorError("runtime image ref is not digest pinned")
    image_digest = image_ref.rsplit(marker, 1)[1]
    return SoakManifest(
        schema_version=1,
        t0=start,
        t24=end,
        reports=_manifest_reports(output, start, config),
        boot_id=row["boot_id"],
        machine_id=row["machine_id"],
        machine_version=row["machine_version"],
        image_ref=image_ref,
        image_digest=image_digest,
        release_id=row["release_id"],
        code_version=row["code_version"],
        mapping_hash=row["mapping_hash"],
        acceptance_config=config,
        acceptance_config_hash=config.digest(),
    )


_BINDING_SELECT = """
SELECT event_id, boot_id, event_seq, occurred_at, recorded_at, reason_code,
       detail->>'manifest_sha256' AS manifest_sha256
FROM l3_runtime_events
WHERE kind='soak_manifest_bound' AND boot_id=$1
  AND detail->>'manifest_sha256'=$2
ORDER BY recorded_at, event_id
"""


async def _binding_rows(dsn: str, manifest: SoakManifest) -> list[Any]:
    connection = await asyncpg.connect(dsn=dsn)
    try:
        return list(
            await connection.fetch(_BINDING_SELECT, manifest.boot_id, manifest.manifest_hash)
        )
    finally:
        await connection.close()


def _validate_exact_binding(rows: list[Any], manifest: SoakManifest) -> Any:
    if len(rows) != 1:
        raise OperatorError("manifest requires exactly one database binding")
    row = rows[0]
    if (
        row["boot_id"] != manifest.boot_id
        or row["manifest_sha256"] != manifest.manifest_hash
        or row["reason_code"] != manifest.soak_hash
        or row["recorded_at"] >= manifest.t0
    ):
        raise OperatorError("manifest database binding is invalid")
    return row


def _manifest_event_seq(manifest: SoakManifest) -> int:
    """Allocate a deterministic operator-only BIGINT range unreachable by daemon counters."""
    offset = int(manifest.manifest_hash[:16], 16) % _MANIFEST_EVENT_SEQ_SPAN
    return _MANIFEST_EVENT_SEQ_BASE + offset


async def _runtime_preflight(dsn: str) -> None:
    """Exercise the real least-privileged store gate before any operator DB path."""
    await L3EvidenceStore(dsn).retention_bounds()


async def _bind_manifest(dsn: str, manifest: SoakManifest, *, now: datetime) -> Any:
    await _runtime_preflight(dsn)
    connection = await asyncpg.connect(dsn=dsn)
    bound_row: Any = None
    try:
        async with connection.transaction():
            boot = await connection.fetchrow(
                "SELECT boot_id FROM l3_runtime_boots WHERE boot_id=$1 FOR UPDATE",
                manifest.boot_id,
            )
            if boot is None:
                raise OperatorError("manifest boot identity is unavailable")
            existing = list(
                await connection.fetch(_BINDING_SELECT, manifest.boot_id, manifest.manifest_hash)
            )
            if existing:
                bound_row = _validate_exact_binding(existing, manifest)
            else:
                if now >= manifest.t0:
                    raise OperatorError("manifest binding must complete before T0")
                event_seq = _manifest_event_seq(manifest)
                collision = await connection.fetchrow(
                    "SELECT event_id FROM l3_runtime_events WHERE boot_id=$1 AND event_seq=$2",
                    manifest.boot_id,
                    event_seq,
                )
                if collision is not None:
                    raise OperatorError("reserved manifest event identity is unavailable")
                row = await connection.fetchrow(
                    """
                    WITH server_clock AS (
                        SELECT clock_timestamp() AS bound_at
                    )
                    INSERT INTO l3_runtime_events (
                        event_id, boot_id, event_seq, occurred_at, kind, severity,
                        generation, reason_code, detail, recorded_at
                    )
                    SELECT $1,$2,$3,bound_at,'soak_manifest_bound','info',NULL,
                           $4,$5::jsonb,bound_at
                    FROM server_clock WHERE bound_at < $6
                    RETURNING event_id, boot_id, event_seq, occurred_at, recorded_at,
                              reason_code, detail->>'manifest_sha256' AS manifest_sha256
                    """,
                    uuid5(
                        NAMESPACE_URL,
                        f"polyarb:l3-soak-manifest:{manifest.manifest_hash}",
                    ),
                    manifest.boot_id,
                    event_seq,
                    manifest.soak_hash,
                    json.dumps({"manifest_sha256": manifest.manifest_hash}),
                    manifest.t0,
                )
                if row is None:
                    raise OperatorError("database clock reached T0 before manifest binding")
                bound_row = _validate_exact_binding([row], manifest)
    finally:
        await connection.close()
    return bound_row


def _checkpoint_spec(manifest: SoakManifest, start: datetime, end: datetime) -> ManifestReport:
    matches = [report for report in manifest.reports if report.start == start and report.end == end]
    if len(matches) != 1:
        raise OperatorError("checkpoint bounds are not declared by the manifest")
    return matches[0]


def _require_exact_t0_sample(evidence: Any, manifest: SoakManifest) -> None:
    health = [row for row in evidence.health_samples if row.sampled_at == manifest.t0]
    if len(health) != 1 or health[0].status is not HealthStatus.PASS:
        raise OperatorError("manifest has no complete passing sample at exact T0")
    markets = [
        row
        for row in evidence.market_samples
        if row.sampled_at == manifest.t0 and row.sample_seq == health[0].sample_seq
    ]
    if (
        len(markets) != 5
        or any(row.status is not HealthStatus.PASS for row in markets)
        or len({row.market_id for row in markets}) != 5
        or len({token for row in markets for token in (row.yes_token_id, row.no_token_id)}) != 10
    ):
        raise OperatorError("manifest T0 market sample is incomplete")


def _has_exact_t0_sample(evidence: Any, manifest: SoakManifest) -> bool:
    try:
        _require_exact_t0_sample(evidence, manifest)
    except OperatorError:
        return False
    return True


def _bind_t0_verdict(report: L3SoakReport, *, exact_t0_sample: bool) -> L3SoakReport:
    if exact_t0_sample:
        return report
    reason = VerdictReason(
        "exact_t0_sample_missing",
        "manifest requires one complete passing five-market sample at exact T0",
    )
    return replace(
        report,
        status=VerdictStatus.NOT_CLOSED,
        reasons=tuple(sorted({*report.reasons, reason})),
        report_hash="",
    )


async def _build_checkpoint(
    *, dsn: str, manifest: SoakManifest, start: datetime, end: datetime, now: datetime
) -> L3SoakReport:
    spec = _checkpoint_spec(manifest, start, end)
    if now < spec.end:
        raise OperatorError(f"checkpoint is not available before {_utc_text(spec.end)}")
    _validate_exact_binding(await _binding_rows(dsn, manifest), manifest)
    store = L3EvidenceStore(dsn)
    t0_evidence = await store.fetch_window(
        manifest.t0, manifest.t0 + timedelta(seconds=manifest.acceptance_config.sample_interval_s)
    )
    exact_t0_sample = _has_exact_t0_sample(t0_evidence, manifest)
    evidence = (
        t0_evidence
        if (start, end) == (t0_evidence.start, t0_evidence.end)
        else await store.fetch_window(start, end)
    )
    return _bind_t0_verdict(
        build_soak_report(
            evidence,
            manifest,
            start,
            end,
            require_24h=spec.checkpoint == "T+24",
        ),
        exact_t0_sample=exact_t0_sample,
    )


async def _verify_reports(
    *, dsn: str, manifest: SoakManifest, start: datetime, end: datetime
) -> L3SoakReport:
    if start != manifest.t0 or end != manifest.t24 or end - start != timedelta(hours=24):
        raise OperatorError("final verification requires exact manifest T0/T24 bounds")
    _validate_exact_binding(await _binding_rows(dsn, manifest), manifest)
    store = L3EvidenceStore(dsn)
    t0_evidence = await store.fetch_window(
        manifest.t0,
        manifest.t0 + timedelta(seconds=manifest.acceptance_config.sample_interval_s),
    )
    exact_t0_sample = _has_exact_t0_sample(t0_evidence, manifest)
    for spec in manifest.reports:
        try:
            encoded = Path(spec.path).read_bytes()
        except OSError as error:
            raise OperatorError("one or more manifest report artifacts are unavailable") from error
        report = parse_report_bytes(encoded)
        if (
            report.start != spec.start
            or report.end != spec.end
            or report.manifest_hash != manifest.manifest_hash
            or report.soak_hash != manifest.soak_hash
            or report.status is not VerdictStatus.PASS
        ):
            raise OperatorError("manifest report artifact does not match its declaration")
        evidence = await store.fetch_window(spec.start, spec.end)
        rebuilt = _bind_t0_verdict(
            build_soak_report(
                evidence,
                manifest,
                spec.start,
                spec.end,
                require_24h=spec.checkpoint == "T+24",
            ),
            exact_t0_sample=exact_t0_sample,
        )
        if canonical_report_bytes(rebuilt) != encoded:
            raise OperatorError("report artifact differs from re-queried raw evidence")
    final_evidence = await store.fetch_window(start, end)
    final = build_soak_report(final_evidence, manifest, start, end, require_24h=True)
    if final.status is not VerdictStatus.PASS:
        raise OperatorError("final exact-window verdict is NOT-CLOSED")
    return final


async def _status(dsn: str) -> dict[str, object]:
    await _runtime_preflight(dsn)
    connection = await asyncpg.connect(dsn=dsn)
    try:
        row = await connection.fetchrow(
            """
            SELECT boot_id, started_at, machine_id, machine_version, image_ref,
                   release_id, code_version, acceptance_config_hash,
                   (SELECT max(recorded_at) FROM l3_promote_runs p
                    WHERE p.boot_id=b.boot_id) latest_promote_recorded_at,
                   (SELECT max(recorded_at) FROM l3_health_samples h
                    WHERE h.boot_id=b.boot_id) latest_sample_recorded_at,
                   (SELECT count(*)::bigint FROM l3_health_samples h
                    WHERE h.boot_id=b.boot_id) health_sample_count,
                   (SELECT count(*)::bigint FROM l3_market_samples m
                    WHERE m.boot_id=b.boot_id) market_sample_count,
                   latest.desired_count, latest.committed_count,
                   latest.evidenced_count, latest.ws_generation,
                   latest.mapping_hash, latest.status AS latest_sample_status,
                   latest.reason_code AS latest_sample_reason_code,
                   latest.sampled_at AS latest_sampled_at
            FROM l3_runtime_boots b
            LEFT JOIN LATERAL (
                SELECT desired_count, committed_count, evidenced_count,
                       ws_generation, mapping_hash, status, reason_code, sampled_at
                FROM l3_health_samples h WHERE h.boot_id=b.boot_id
                ORDER BY sampled_at DESC, sample_seq DESC LIMIT 1
            ) latest ON TRUE
            ORDER BY started_at DESC LIMIT 1
            """
        )
    finally:
        await connection.close()
    if row is None:
        raise OperatorError("no L3 evidence boot is available")
    return dict(row)


async def _retention_policy_anchor(dsn: str) -> dict[str, object]:
    connection = await asyncpg.connect(dsn=dsn)
    try:
        row = await connection.fetchrow(
            """
            SELECT min(started_at) AS first_started_at,
                   (array_agg(acceptance_config_hash ORDER BY started_at DESC))[1]
                       AS acceptance_config_hash,
                   (array_agg(code_version ORDER BY started_at DESC))[1] AS code_version
            FROM l3_runtime_boots
            """
        )
    finally:
        await connection.close()
    if row is None or row["first_started_at"] is None:
        raise OperatorError("retention policy has no runtime boot anchor")
    return dict(row)


async def _retention_check(dsn: str, *, now: datetime) -> dict[str, object]:
    await _runtime_preflight(dsn)
    bounds = await L3EvidenceStore(dsn).retention_bounds()
    anchor = await _retention_policy_anchor(dsn)
    from polyarb.config import load_settings

    config = AcceptanceConfig.from_settings(
        load_settings(),
        Path("src/polyarb/scan_recipes/l3-promote.yaml"),
        str(anchor["code_version"]),
    )
    if config.retention_days < 30 or config.digest() != anchor["acceptance_config_hash"]:
        raise OperatorError("retention AcceptanceConfig proof is NOT-CLOSED")
    threshold = now - timedelta(days=30)
    mature = anchor["first_started_at"] <= threshold
    if mature and any(
        bounds.row_count_by_table[table] <= 0
        or bounds.oldest_recorded_at_by_table[table] is None
        or bounds.oldest_recorded_at_by_table[table] > threshold
        for table in EVIDENCE_TABLES
    ):
        raise OperatorError("retention proof detects premature evidence deletion")
    return {
        "status": "PASS",
        "configured_retention_days": config.retention_days,
        "history_mature": mature,
        "first_started_at": anchor["first_started_at"],
        "oldest_recorded_at_by_table": bounds.oldest_recorded_at_by_table,
        "newest_recorded_at_by_table": bounds.newest_recorded_at_by_table,
        "row_count_by_table": bounds.row_count_by_table,
    }


_TARGET_ROLE_SQL = """
SELECT current_database() AS database_name, current_user AS current_user,
       inet_server_addr()::text AS server_address,
       role.rolcanlogin AS can_login, role.rolsuper AS is_superuser,
       (database.datdba=role.oid) AS is_database_owner,
       EXISTS (
           SELECT 1 FROM pg_class class
           WHERE class.relowner=role.oid
             AND class.relname = ANY(ARRAY[
                 'l3_runtime_boots','l3_promote_runs','l3_health_samples',
                 'l3_market_samples','l3_runtime_events'
             ])
       ) AS is_evidence_owner,
       pg_has_role(current_user,'service_role','MEMBER') AS service_member,
       pg_has_role(current_user,'l3_evidence_daemon','MEMBER') AS daemon_member,
       pg_has_role(current_user,'l3_retention_operator','MEMBER') AS retention_member
FROM pg_roles role JOIN pg_database database ON database.datname=current_database()
WHERE role.rolname=current_user
"""


async def _credential_check(dsn: str, *, expected_ref: str, capability: str) -> dict[str, object]:
    target = validate_supabase_target(dsn, expected_ref=expected_ref)
    connection = await asyncpg.connect(dsn=dsn)
    try:
        row = await connection.fetchrow(_TARGET_ROLE_SQL)
        if row is None:
            raise OperatorError("database identity proof returned no role")
        direct = await connection.fetch(
            """
            SELECT table_name, privilege_type,
                   has_table_privilege(current_user, table_name, privilege_type) AS allowed
            FROM unnest(ARRAY[
                'l3_runtime_boots','l3_promote_runs','l3_health_samples',
                'l3_market_samples','l3_runtime_events','markets_latest',
                'l2_book_levels','l2_top_of_book','l2_ohlc_1m','snapshots',
                'l2_event_cursor'
            ]) AS table_name
            CROSS JOIN unnest(
                ARRAY['SELECT','INSERT','UPDATE','DELETE','TRUNCATE']
            ) AS privilege_type
            ORDER BY table_name, privilege_type
            """
        )
        sequence_ok = await connection.fetchval(
            "SELECT has_sequence_privilege(current_user, 'l3_promote_runs_id_seq', 'USAGE,SELECT')"
        )
        routine = await connection.fetchval(
            "SELECT has_function_privilege(current_user, "
            "'l3_retention_cleanup(timestamptz,timestamptz,timestamptz)', "
            "'EXECUTE')"
        )
    finally:
        await connection.close()
    common_ok = (
        row["database_name"] == target.database
        and row["current_user"] == _database_role(target)
        and row["can_login"]
        and not row["is_superuser"]
        and not row["is_database_owner"]
        and not row["is_evidence_owner"]
        and not row["service_member"]
    )
    direct_grants = {
        (item["table_name"], item["privilege_type"]) for item in direct if item["allowed"]
    }
    evidence_grants = {
        (table, privilege) for table in EVIDENCE_TABLES for privilege in ("SELECT", "INSERT")
    }
    read_grants = {
        (table, "SELECT")
        for table in (
            "markets_latest",
            "l2_book_levels",
            "l2_top_of_book",
            "l2_ohlc_1m",
            "snapshots",
        )
    }
    cursor_grants = {("l2_event_cursor", privilege) for privilege in ("SELECT", "INSERT", "UPDATE")}
    required_runtime_grants = evidence_grants | read_grants | cursor_grants
    if capability == "runtime":
        ok = (
            common_ok
            and row["daemon_member"]
            and not row["retention_member"]
            and not routine
            and sequence_ok
            and direct_grants == required_runtime_grants
        )
        expected_role = "l3_evidence_daemon"
    else:
        ok = (
            common_ok
            and not row["daemon_member"]
            and row["retention_member"]
            and routine
            and not sequence_ok
            and not direct_grants
        )
        expected_role = "l3_retention_operator"
    if not ok:
        raise OperatorError(f"{capability} credential capability proof is NOT-CLOSED")
    if capability == "runtime":
        await _runtime_preflight(dsn)
    return {
        "status": "PASS",
        "project_ref": target.project_ref,
        "host": target.host,
        "database": row["database_name"],
        "current_user": row["current_user"],
        "server_address": row["server_address"],
        "capability_role": expected_role,
    }


async def _prod_revision(
    dsn: str, *, expected_ref: str, expected_revision: str
) -> dict[str, object]:
    target = validate_supabase_target(dsn, expected_ref=expected_ref)
    if ".pooler.supabase.com" in target.host:
        raise OperatorError("production revision proof requires a direct database target")
    expected_user = _database_role(target)
    if expected_user != "postgres":
        raise OperatorError("production revision proof requires the migration user")
    connection = await asyncpg.connect(dsn=dsn)
    try:
        row = await connection.fetchrow(
            """
            SELECT current_database() AS database_name, current_user AS current_user,
                   inet_server_addr()::text AS server_address,
                   (SELECT version_num FROM alembic_version) AS revision
            """
        )
    finally:
        await connection.close()
    if (
        row is None
        or row["database_name"] != target.database
        or row["current_user"] != expected_user
        or row["revision"] != expected_revision
        or not row["server_address"]
    ):
        raise OperatorError("production target/revision proof is NOT-CLOSED")
    return {
        "status": "PASS",
        "project_ref": target.project_ref,
        "host": target.host,
        "database": row["database_name"],
        "current_user": row["current_user"],
        "server_address": row["server_address"],
        "revision": row["revision"],
    }


def _json_default(value: object) -> object:
    if isinstance(value, datetime):
        return _utc_text(value)
    if isinstance(value, UUID):
        return str(value)
    if hasattr(value, "items"):
        return dict(value)  # type: ignore[arg-type]
    raise TypeError(type(value).__name__)


def _print_json(value: object) -> None:
    print(json.dumps(value, sort_keys=True, separators=(",", ":"), default=_json_default))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    create = commands.add_parser("manifest-create")
    create.add_argument("--start", required=True)
    create.add_argument("--end", required=True)
    create.add_argument("--output", type=Path, required=True)

    bind = commands.add_parser("manifest-bind")
    bind.add_argument("--manifest", type=Path, required=True)

    commands.add_parser("status")

    checkpoint = commands.add_parser("checkpoint")
    checkpoint.add_argument("--manifest", type=Path, required=True)
    checkpoint.add_argument("--start", required=True)
    checkpoint.add_argument("--end", required=True)
    checkpoint.add_argument("--output", type=Path, required=True)

    verify = commands.add_parser("verify")
    verify.add_argument("--manifest", type=Path, required=True)
    verify.add_argument("--start", required=True)
    verify.add_argument("--end", required=True)

    commands.add_parser("retention-check")

    runtime = commands.add_parser("runtime-credential-check")
    runtime.add_argument("--expected-ref", required=True)
    retention = commands.add_parser("retention-operator-check")
    retention.add_argument("--expected-ref", required=True)
    cleanup = commands.add_parser("retention-cleanup")
    cleanup.add_argument("--cutoff", required=True)
    cleanup.add_argument("--protected-start", required=True)
    cleanup.add_argument("--protected-end", required=True)
    cleanup.add_argument("--approval", required=True)
    cleanup.add_argument("--expected-ref")
    revision = commands.add_parser("prod-revision")
    revision.add_argument("--expected-ref", required=True)
    revision.add_argument("--expected-revision", required=True)
    return parser


async def _run(args: argparse.Namespace) -> int:
    now = datetime.now(UTC)
    if args.command == "manifest-create":
        dsn = _required_env("POLYARB_L2_RUNTIME_DB_DSN")
        start = parse_rfc3339(args.start)
        end = parse_rfc3339(args.end)
        if start <= now:
            raise OperatorError("manifest T0 must be in the future")
        manifest = await _create_manifest_from_runtime(
            dsn=dsn,
            start=start,
            end=end,
            output=args.output,
        )
        write_new_manifest(manifest, output=args.output, now=now)
        _print_json(
            {
                "status": "PASS",
                "manifest_hash": manifest.manifest_hash,
                "soak_hash": manifest.soak_hash,
                "t0": manifest.t0,
                "t24": manifest.t24,
                "output": str(args.output),
            }
        )
    elif args.command == "manifest-bind":
        manifest = _read_manifest(args.manifest)
        row = await _bind_manifest(_required_env("POLYARB_L2_RUNTIME_DB_DSN"), manifest, now=now)
        _print_json(
            {
                "status": "PASS",
                "manifest_hash": manifest.manifest_hash,
                "soak_hash": manifest.soak_hash,
                "recorded_at": row["recorded_at"],
            }
        )
    elif args.command == "status":
        status = await _status(_required_env("POLYARB_L2_RUNTIME_DB_DSN"))
        _print_json({"status": "PASS", **status})
    elif args.command == "checkpoint":
        manifest = _read_manifest(args.manifest)
        start = parse_rfc3339(args.start)
        end = parse_rfc3339(args.end)
        spec = _checkpoint_spec(manifest, start, end)
        if args.output != Path(spec.path):
            raise OperatorError("checkpoint output must equal the manifest-declared path")
        report = await _build_checkpoint(
            dsn=_required_env("POLYARB_L2_RUNTIME_DB_DSN"),
            manifest=manifest,
            start=start,
            end=end,
            now=now,
        )
        _write_exclusive(args.output, canonical_report_bytes(report))
        print(render_report(report), end="")
        return EXIT_OK if report.status is VerdictStatus.PASS else EXIT_NOT_CLOSED
    elif args.command == "verify":
        manifest = _read_manifest(args.manifest)
        report = await _verify_reports(
            dsn=_required_env("POLYARB_L2_RUNTIME_DB_DSN"),
            manifest=manifest,
            start=parse_rfc3339(args.start),
            end=parse_rfc3339(args.end),
        )
        print(render_report(report), end="")
    elif args.command == "retention-check":
        _print_json(await _retention_check(_required_env("POLYARB_L2_RUNTIME_DB_DSN"), now=now))
    elif args.command == "runtime-credential-check":
        _print_json(
            await _credential_check(
                _required_env("POLYARB_L2_RUNTIME_DB_DSN"),
                expected_ref=args.expected_ref,
                capability="runtime",
            )
        )
    elif args.command == "retention-operator-check":
        _print_json(
            await _credential_check(
                _required_env("L3_RETENTION_DSN"),
                expected_ref=args.expected_ref,
                capability="retention",
            )
        )
    elif args.command == "retention-cleanup":
        if args.approval != RETENTION_APPROVAL:
            raise OperatorError("retention cleanup approval token is invalid")
        dsn = _required_env("L3_RETENTION_DSN")
        expected_ref = args.expected_ref or _required_env("SUPABASE_PROJECT_REF")
        await _credential_check(dsn, expected_ref=expected_ref, capability="retention")
        result = await L3RetentionOperator(dsn).run_retention_cleanup(
            cutoff=parse_rfc3339(args.cutoff),
            protected_start=parse_rfc3339(args.protected_start),
            protected_end=parse_rfc3339(args.protected_end),
        )
        _print_json(
            {
                "status": "PASS",
                **{field.name: getattr(result, field.name) for field in fields(result)},
            }
        )
    elif args.command == "prod-revision":
        _print_json(
            await _prod_revision(
                _required_env("POLYARB_SUPABASE_DB_DSN"),
                expected_ref=args.expected_ref,
                expected_revision=args.expected_revision,
            )
        )
    return EXIT_OK


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
        return asyncio.run(_run(args))
    except (OperatorError, ValueError, FileExistsError) as error:
        print(f"NOT-CLOSED: {error}", file=sys.stderr)
        return EXIT_NOT_CLOSED
    except KeyboardInterrupt:
        return 130
    except Exception:
        print("NOT-CLOSED: evidence operation unavailable", file=sys.stderr)
        return EXIT_UNAVAILABLE


if __name__ == "__main__":
    raise SystemExit(main())
