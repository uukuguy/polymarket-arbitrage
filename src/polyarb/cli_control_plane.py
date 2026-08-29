"""Safe operator commands for the additive M1 transactional control plane."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from threading import Event, Lock, Thread
from time import monotonic
from typing import Any, Protocol, cast
from urllib.request import Request, urlopen

import psycopg

from polyarb.clients.clob_client import ClobReaderClient
from polyarb.clients.gamma_client import GammaClient
from polyarb.config import Settings
from polyarb.control_plane.alert_delivery import (
    ALERT_DELIVERY_POLICY,
    TransactionalAlertDeliveryWorker,
)
from polyarb.control_plane.blocking_bridge import (
    run_blocking_call,
    run_blocking_call_until_stopped,
)
from polyarb.control_plane.db_deadlines import CONTROL_PLANE_DB_POLICY, RECOVERY_DB_POLICY
from polyarb.control_plane.db_role_contract import (
    DatabaseRoleContractError,
    scoped_connection_factory,
    verify_daemon_database_role,
)
from polyarb.control_plane.fault_soak import verify_fault_soak
from polyarb.control_plane.faults import IntentionalStagingRetryFault
from polyarb.control_plane.models import JobLease
from polyarb.control_plane.opportunity_worker import TransactionalOpportunityCertifier
from polyarb.control_plane.postgres import PostgresControlPlane
from polyarb.control_plane.qualification import RollingQualificationPolicy
from polyarb.control_plane.qualification_identity import (
    QualificationIdentityError,
    qualification_identity_from_env,
)
from polyarb.control_plane.qualification_service import (
    PostgresQualificationFactSource,
    PostgresQualificationServiceStore,
    QualificationService,
    run_qualification_service,
)
from polyarb.control_plane.quote_admission import TransactionalQuoteAdmitter
from polyarb.control_plane.quote_worker import (
    TransactionalQuoteBatchPool,
    TransactionalQuoteBatchWorker,
    TransactionalQuoteCertifier,
)
from polyarb.control_plane.reconciler import RuntimeReconciler
from polyarb.control_plane.recovery_executor import RecoveryActionResult, RecoveryExecutor
from polyarb.control_plane.recovery_models import RecoveryActionType, RecoveryDecision
from polyarb.control_plane.recovery_records import RecoveryActionRecord, RuntimeControllerLease
from polyarb.control_plane.recovery_store import (
    RecoveryProbeLaneBusy,
    RuntimeReconcileCandidate,
    claim_controller,
    read_runtime_controller_status,
    read_runtime_reconcile_states,
    renew_controller,
    schedule_action,
)
from polyarb.control_plane.rollout import render_rollout_artifacts
from polyarb.control_plane.runtime_deadlines import runtime_policy
from polyarb.control_plane.runtime_fault_matrix import RuntimeFaultMatrixError, run_fault_matrix
from polyarb.control_plane.runtime_observe import (
    RuntimeObserveVerificationError,
    build_runtime_observe_decision_record,
    build_runtime_observe_idle_record,
    insert_runtime_observe_decisions,
    verify_runtime_observe_window,
)
from polyarb.control_plane.runtime_replay import replay_soak_observations
from polyarb.control_plane.scheduler import TransactionalControlPlaneScheduler
from polyarb.control_plane.shadow import project_shadow_sources, read_shadow_sources
from polyarb.control_plane.shadow_parity import verify_shadow_parity
from polyarb.control_plane.soak_evidence import (
    SoakEvidenceError,
    append_record,
    create_record,
    read_records,
    verify_soak,
)
from polyarb.control_plane.structure_artifact import (
    StructureBundleArtifact,
    canonical_structure_bundle_bytes,
    upload_structure_bundle_artifact,
)
from polyarb.control_plane.structure_shadow import (
    plan_structure_ranges,
    read_legacy_structure_bundle,
)
from polyarb.control_plane.structure_source import (
    TransactionalStructureSourceAdmitter,
    TransactionalStructureSourceMaterializer,
    TransactionalStructureSourcePool,
    TransactionalStructureSourceWorker,
)
from polyarb.control_plane.structure_worker import (
    TransactionalStructureCertifier,
    TransactionalStructureRangePool,
    TransactionalStructureWorker,
)
from polyarb.control_plane.watchdog import (
    CloudUsageGate,
    ProgressGate,
    RestartEventGate,
    RuntimeObservation,
    SoakEvidenceGate,
    assess_runtime,
    run_watchdog_service,
)
from polyarb.control_plane.worker_loop import TransactionalWorkerLoop
from polyarb.storage.r2_sync import _build_client, control_plane_r2_config

_R2_UPLOAD_FAULT_ACK = "staging-r2-upload-before-receipt"
_FLY_HTTP_TIMEOUT_SECONDS = 10.0
_FLY_CLI_READ_TIMEOUT_SECONDS = 30.0
_MAX_FLY_MACHINES_PER_APP = 16
_MAX_WATCHDOG_APPS = 8
_WATCHDOG_OBSERVATION_TIMEOUT_SECONDS = 2 * _FLY_HTTP_TIMEOUT_SECONDS + 1.0


class _RuntimeReconcileControlPlane(Protocol):
    """Minimal atomic surface consumed by the bounded runtime controller."""

    _connection_factory: Callable[[], psycopg.Connection[Any]]

    def _execute_recovery_action_cursor(
        self,
        cursor: Any,
        action: RecoveryActionRecord,
        *,
        now: datetime,
        heartbeat_lease_seconds: int,
    ) -> object: ...


_RETRY_FAULT_ACK = "staging-retry-before-receipt"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="control-plane")
    subcommands = parser.add_subparsers(dest="command", required=True)
    shadow = subcommands.add_parser(
        "shadow-sync",
        help="project bounded SQLite facts into Postgres without changing pointers",
    )
    shadow.add_argument("--db-path", type=Path, required=True)
    shadow.add_argument("--limit", type=int, default=100)
    shadow.add_argument("--json", action="store_true")
    status = subcommands.add_parser("status", help="read bounded durable operator state")
    status.add_argument("--limit", type=int, default=20)
    status.add_argument("--json", action="store_true")
    preflight = subcommands.add_parser(
        "preflight",
        help="read-only proof that one named database and R2 bucket are ready for shadow work",
    )
    preflight.add_argument("--expected-database", required=True)
    preflight.add_argument("--json", action="store_true")
    quote_once = subcommands.add_parser(
        "quote-once",
        help="explicitly run at most one transactional Quote batch and certification attempt",
    )
    quote_once.add_argument(
        "--enable",
        action="store_true",
        help="required acknowledgement: this command may write to the configured control plane",
    )
    quote_once.add_argument("--worker-id", default="quote-operator-once")
    quote_once.add_argument("--json", action="store_true")
    structure_once = subcommands.add_parser(
        "structure-once",
        help="explicitly run at most one transactional Structure normalization range",
    )
    structure_once.add_argument(
        "--enable",
        action="store_true",
        help="required acknowledgement: this command may write to the configured control plane",
    )
    structure_once.add_argument("--worker-id", default="structure-operator-once")
    structure_once.add_argument("--json", action="store_true")
    structure_source_once = subcommands.add_parser(
        "structure-source-once",
        help=(
            "admit one named Structure source window then fetch at most its first "
            "durable Gamma page"
        ),
    )
    structure_source_once.add_argument("--enable", action="store_true")
    structure_source_once.add_argument("--window-key", required=True)
    structure_source_once.add_argument("--worker-id", default="structure-source-operator-once")
    structure_source_once.add_argument("--json", action="store_true")
    structure_shadow_once = subcommands.add_parser(
        "structure-shadow-once",
        help="export and admit one current legacy Structure publication without pointer changes",
    )
    structure_shadow_once.add_argument("--enable", action="store_true")
    structure_shadow_once.add_argument("--db-path", type=Path, required=True)
    structure_shadow_once.add_argument("--publication-id", required=True)
    structure_shadow_once.add_argument("--range-max-rows", type=int, default=1_000)
    structure_shadow_once.add_argument("--json", action="store_true")
    structure_shadow_publish = subcommands.add_parser(
        "structure-shadow-publish",
        help="explicitly publish one certified Structure generation to the shadow pointer",
    )
    structure_shadow_publish.add_argument("--enable", action="store_true")
    structure_shadow_publish.add_argument("--generation-key", required=True)
    structure_shadow_publish.add_argument("--json", action="store_true")
    tick_once = subcommands.add_parser(
        "tick-once",
        help="explicitly run one bounded transactional control-plane scheduler tick",
    )
    tick_once.add_argument("--enable", action="store_true")
    tick_once.add_argument("--worker-id", default="control-plane-tick-once")
    tick_once.add_argument("--max-turns", type=int, default=4)
    tick_once.add_argument("--structure-materializer-turns", type=int, default=0)
    tick_once.add_argument("--structure-range-turns", type=int, default=0)
    tick_once.add_argument("--fault-crash-after-r2-upload-job-key")
    tick_once.add_argument("--fault-retry-job-key")
    tick_once.add_argument("--fault-retry-attempts", type=int)
    tick_once.add_argument("--acceptance-run-id")
    tick_once.add_argument("--fault-injection-ack")
    tick_once.add_argument("--json", action="store_true")
    serve = subcommands.add_parser(
        "serve",
        help="run bounded transactional ticks until SIGINT or SIGTERM",
    )
    serve.add_argument("--enable", action="store_true")
    serve.add_argument("--worker-id", default="control-plane-service")
    serve.add_argument(
        "--worker-role",
        choices=("all", "coordinator", "structure-range", "quote-batch"),
        default="all",
        help="run all workers, or one independently scalable fenced worker role",
    )
    serve.add_argument("--max-turns", type=int, default=4)
    serve.add_argument("--pool-turns", type=int, default=1)
    serve.add_argument("--structure-high-water", type=int, default=1)
    serve.add_argument("--quote-high-water", type=int, default=512)
    serve.add_argument("--structure-materializer-turns", type=int, default=0)
    serve.add_argument("--structure-range-turns", type=int, default=0)
    serve.add_argument("--fault-crash-after-r2-upload-job-key")
    serve.add_argument("--fault-retry-job-key")
    serve.add_argument("--fault-retry-attempts", type=int)
    serve.add_argument("--acceptance-run-id")
    serve.add_argument("--fault-injection-ack")
    serve.add_argument("--interval-seconds", type=float, default=15.0)
    serve.add_argument("--json", action="store_true")
    alert_serve = subcommands.add_parser(
        "alert-serve", help="run the isolated transactional alert-delivery worker"
    )
    alert_serve.add_argument("--enable", action="store_true")
    alert_serve.add_argument("--worker-id", default="control-plane-alert-service")
    alert_serve.add_argument("--interval-seconds", type=float, default=15.0)
    alert_serve.add_argument("--acceptance-run-id")
    alert_serve.add_argument("--json", action="store_true")
    watchdog_serve = subcommands.add_parser(
        "watchdog-serve",
        help="run the database-independent runtime watchdog and direct Telegram pager",
    )
    watchdog_serve.add_argument("--enable", action="store_true")
    watchdog_serve.add_argument("--control-api-url", required=True)
    watchdog_serve.add_argument("--fly-app", required=True)
    watchdog_serve.add_argument("--machine-id", action="append", required=True)
    watchdog_serve.add_argument(
        "--secondary-fly-app",
        help="optional independent app whose exact Machines are part of the same runtime gate",
    )
    watchdog_serve.add_argument("--secondary-machine-id", action="append")
    watchdog_serve.add_argument(
        "--secondary-target",
        action="append",
        help="additional exact Fly target in <app>/<machine-id> form; repeatable",
    )
    watchdog_serve.add_argument(
        "--watchdog-once",
        action="store_true",
        help="perform one credential-free notification-free runtime check and exit",
    )
    watchdog_serve.add_argument(
        "--soak-run-id", help="require fresh sampler evidence from this exact formal run"
    )
    watchdog_serve.add_argument("--interval-seconds", type=float, default=30.0)
    watchdog_serve.add_argument("--json", action="store_true")
    render_rollout = subcommands.add_parser(
        "render-rollout",
        help="render local-only named control-plane rollout artifacts",
    )
    render_rollout.add_argument("--enable", action="store_true")
    render_rollout.add_argument("--api-app", required=True)
    render_rollout.add_argument("--worker-app", required=True)
    render_rollout.add_argument("--alert-app", required=True)
    render_rollout.add_argument("--runtime-event-writer-app", required=True)
    render_rollout.add_argument("--runtime-controller-app")
    render_rollout.add_argument("--qualification-worker-app")
    render_rollout.add_argument("--release-id", required=True)
    render_rollout.add_argument(
        "--runtime-recovery-allowed-target",
        action="append",
        default=[],
        help="exact <app>/<machine-or-process> recovery identity; repeatable",
    )
    render_rollout.add_argument("--expected-database", required=True)
    render_rollout.add_argument("--output-dir", type=Path, required=True)
    render_rollout.add_argument("--json", action="store_true")
    verify_parity = subcommands.add_parser(
        "verify-shadow-parity",
        help="verify three local Structure/Quote shadow-run evidence records",
    )
    verify_parity.add_argument("--evidence", type=Path, required=True)
    verify_parity.add_argument("--json", action="store_true")
    verify_fault_soak_command = subcommands.add_parser(
        "verify-fault-soak",
        help="verify local cloud worker-loss and sustained-soak evidence",
    )
    verify_fault_soak_command.add_argument("--evidence", type=Path, required=True)
    verify_fault_soak_command.add_argument("--json", action="store_true")
    for command, help_text in (
        ("soak-start", "record the immutable baseline for a read-only transactional soak window"),
        ("soak-sample", "append one read-only transactional soak observation"),
    ):
        soak = subcommands.add_parser(command, help=help_text)
        soak.add_argument("--output", type=Path, required=True)
        soak.add_argument("--control-api-url", required=True)
        soak.add_argument("--machine-id", action="append", required=True)
        soak.add_argument("--fly-app", default="polyarb-control-worker-staging")
        soak.add_argument("--json", action="store_true")
    for command, help_text in (
        ("cloud-soak-start", "atomically record the cloud-resident soak baseline"),
        ("cloud-soak-sample", "append one cloud-resident transactional soak observation"),
    ):
        cloud_soak = subcommands.add_parser(command, help=help_text)
        cloud_soak.add_argument("--run-id", required=True)
        cloud_soak.add_argument("--control-api-url", required=True)
        cloud_soak.add_argument("--machine-id", action="append", required=True)
        cloud_soak.add_argument("--fly-app", required=True)
        cloud_soak.add_argument("--json", action="store_true")
    cloud_soak_verify = subcommands.add_parser(
        "cloud-soak-verify", help="fail-closed verification from cloud-resident evidence"
    )
    cloud_soak_verify.add_argument("--run-id", required=True)
    cloud_soak_verify.add_argument("--minimum-seconds", type=int, default=86_400)
    cloud_soak_verify.add_argument("--max-gap-seconds", type=int, default=900)
    cloud_soak_verify.add_argument("--json", action="store_true")
    cloud_soak_serve = subcommands.add_parser(
        "cloud-soak-serve", help="run the isolated cloud-resident soak sampler"
    )
    cloud_soak_serve.add_argument("--enable", action="store_true")
    cloud_soak_serve.add_argument("--run-id", required=True)
    cloud_soak_serve.add_argument("--control-api-url", required=True)
    cloud_soak_serve.add_argument("--machine-id", action="append", required=True)
    cloud_soak_serve.add_argument("--fly-app", required=True)
    cloud_soak_serve.add_argument("--interval-seconds", type=float, default=300.0)
    cloud_soak_serve.add_argument("--json", action="store_true")
    soak_verify = subcommands.add_parser(
        "soak-verify", help="verify a local immutable transactional soak evidence file"
    )
    soak_verify.add_argument("--evidence", type=Path, required=True)
    soak_verify.add_argument("--minimum-seconds", type=int, default=86_400)
    soak_verify.add_argument("--max-gap-seconds", type=int, default=900)
    soak_verify.add_argument("--json", action="store_true")
    runtime_replay = subcommands.add_parser(
        "runtime-policy-replay",
        help="read immutable cloud soak observations and replay live runtime policy",
    )
    runtime_replay.add_argument("--run-id", required=True)
    runtime_replay.add_argument("--max-gap-seconds", type=float, default=900.0)
    runtime_replay.add_argument("--json", action="store_true")
    runtime_fault_matrix = subcommands.add_parser(
        "runtime-fault-matrix",
        help="run the local deterministic self-healing runtime fault matrix",
    )
    runtime_fault_matrix.add_argument("--json", action="store_true")
    runtime_status = subcommands.add_parser(
        "runtime-controller-status",
        help="read the current runtime controller lease, incidents, budgets, and recovery actions",
    )
    runtime_status.add_argument("--controller-id", default="m1-runtime-reconciler")
    runtime_status.add_argument("--limit", type=int, default=20)
    runtime_status.add_argument("--json", action="store_true")
    runtime_observe_verify = subcommands.add_parser(
        "runtime-observe-verify",
        help="verify durable observe-only decisions and zero recovery mutation",
    )
    runtime_observe_verify.add_argument("--controller-id", default="m1-runtime-reconciler")
    runtime_observe_verify.add_argument("--minimum-seconds", type=int, default=1800)
    runtime_observe_verify.add_argument("--max-freshness-seconds", type=int, default=90)
    runtime_observe_verify.add_argument("--max-gap-seconds", type=int, default=90)
    runtime_observe_verify.add_argument("--limit", type=int, default=500)
    runtime_observe_verify.add_argument("--json", action="store_true")
    runtime_once = subcommands.add_parser(
        "runtime-reconcile-once",
        help="evaluate runtime facts and execute at most one fenced recovery action",
    )
    runtime_once.add_argument("--enable", action="store_true")
    runtime_once.add_argument("--controller-id", default="m1-runtime-reconciler")
    runtime_once.add_argument("--owner-id", default="runtime-reconcile-once")
    runtime_once.add_argument("--worker-id", default="runtime-recovery-executor")
    runtime_once.add_argument("--lease-seconds", type=int, default=30)
    runtime_once.add_argument("--action-lease-seconds", type=int, default=30)
    runtime_once.add_argument("--heartbeat-lease-seconds", type=int, default=30)
    runtime_once.add_argument("--limit", type=int, default=100)
    runtime_once.add_argument("--target-type", choices=("job", "circuit"))
    runtime_once.add_argument("--target-id")
    runtime_once.add_argument(
        "--expected-action",
        choices=tuple(action.value for action in RecoveryActionType),
    )
    runtime_once.add_argument("--json", action="store_true")
    runtime_serve = subcommands.add_parser(
        "runtime-reconcile-serve",
        help="run sequential fenced runtime reconciliation turns until SIGTERM or SIGINT",
    )
    runtime_serve.add_argument("--enable", action="store_true")
    runtime_serve.add_argument("--controller-id", default="m1-runtime-reconciler")
    runtime_serve.add_argument("--owner-id", default="runtime-reconcile-service")
    runtime_serve.add_argument("--worker-id", default="runtime-recovery-executor")
    runtime_serve.add_argument("--lease-seconds", type=int, default=90)
    runtime_serve.add_argument("--action-lease-seconds", type=int, default=30)
    runtime_serve.add_argument("--heartbeat-lease-seconds", type=int, default=30)
    runtime_serve.add_argument("--limit", type=int, default=100)
    runtime_serve.add_argument("--interval-seconds", type=float, default=30.0)
    runtime_serve.add_argument("--json", action="store_true")
    qualification_status = subcommands.add_parser(
        "qualification-status",
        help="read current rolling qualification progress and last breaker",
    )
    qualification_status.add_argument("--json", action="store_true")
    qualification_certificates = subcommands.add_parser(
        "qualification-certificates",
        help="read and reverify recent immutable qualification certificates",
    )
    qualification_certificates.add_argument("--limit", type=int, default=20)
    qualification_certificates.add_argument("--json", action="store_true")
    qualification_serve = subcommands.add_parser(
        "qualification-serve",
        help="run sequential rolling qualification ticks until SIGTERM or SIGINT",
    )
    qualification_serve.add_argument("--enable", action="store_true")
    qualification_serve.add_argument("--interval-seconds", type=float, default=30.0)
    qualification_serve.add_argument("--batch-size", type=int, default=100)
    qualification_serve.add_argument("--writer-id", default="qualification-service")
    qualification_serve.add_argument("--json", action="store_true")
    return parser


def _control_plane_from_env() -> PostgresControlPlane | None:
    dsn = os.environ.get("POLYARB_SUPABASE_DB_DSN", "").strip()
    if not dsn:
        return None
    return PostgresControlPlane(scoped_connection_factory(dsn))


def _qualification_connection_factory_from_env() -> Callable[[], psycopg.Connection[Any]] | None:
    dsn = os.environ.get("POLYARB_QUALIFICATION_DB_DSN", "").strip()
    if not dsn:
        return None
    return scoped_connection_factory(dsn)


def _required_expected_database_from_env() -> str:
    expected_database = os.environ.get("POLYARB_DB_EXPECTED_DATABASE", "").strip()
    if not expected_database:
        raise ValueError("POLYARB_DB_EXPECTED_DATABASE is required")
    return expected_database


def _qualification_service_from_env(
    *,
    batch_size: int,
    interval_seconds: float,
    writer_id: str,
) -> QualificationService:
    connection_factory = _qualification_connection_factory_from_env()
    if connection_factory is None:
        raise ValueError("POLYARB_QUALIFICATION_DB_DSN is required")
    identity = qualification_identity_from_env(
        interval_seconds=interval_seconds,
        batch_size=batch_size,
    )
    policy = RollingQualificationPolicy(
        release_id=identity.release_id,
        config_id=identity.config_id,
        role_identity=identity.role_identity,
    )
    return QualificationService(
        policy=policy,
        fact_source=PostgresQualificationFactSource(connection_factory),
        state_store=PostgresQualificationServiceStore(connection_factory),
        writer_id=writer_id,
        batch_size=batch_size,
    )


def _write(payload: Mapping[str, object], *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(payload, sort_keys=True))
        return
    for key, value in payload.items():
        print(f"{key}={value}")


def _read_soak_control_snapshot(url: str) -> dict[str, object]:
    """Read the independent control API without importing its database client."""
    with urlopen(url, timeout=10) as response:  # noqa: S310 -- explicit operator URL
        payload = json.loads(response.read())
    if not isinstance(payload, dict):
        raise SoakEvidenceError("control API response must be an object")
    return payload


def _read_fly_machine_states(machine_ids: Sequence[str], *, app: str) -> dict[str, str]:
    """Read exact Fly machine state using its local CLI, with no machine mutation."""
    try:
        result = subprocess.run(
            ["flyctl", "machines", "list", "--app", app, "--json"],
            check=True,
            capture_output=True,
            text=True,
            timeout=_FLY_CLI_READ_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as error:
        raise SoakEvidenceError("Fly machine state read exceeded its operator bound") from error
    payload = json.loads(result.stdout)
    if not isinstance(payload, list):
        raise SoakEvidenceError("Fly machines list must be an array")
    listed = {
        item.get("id"): item.get("state")
        for item in payload
        if isinstance(item, dict) and isinstance(item.get("id"), str)
    }
    states: dict[str, str] = {}
    for machine_id in machine_ids:
        state = listed.get(machine_id)
        if not isinstance(state, str) or not state:
            raise SoakEvidenceError("an exact Fly machine is missing or has no state")
        states[machine_id] = state
    return states


def _read_cloud_fly_machine_states(
    machine_ids: Sequence[str], *, app: str, token: str
) -> dict[str, str]:
    """Read exact Fly states over HTTPS; production images intentionally lack flyctl."""
    if not token:
        raise SoakEvidenceError("POLYARB_FLY_API_TOKEN is required for cloud soak sampling")
    if not machine_ids or len(machine_ids) > _MAX_FLY_MACHINES_PER_APP:
        raise SoakEvidenceError("cloud Fly target count is outside the bounded app envelope")
    request = Request(
        f"https://api.machines.dev/v1/apps/{app}/machines",
        headers={"Authorization": f"Bearer {token}"},
    )
    with urlopen(  # noqa: S310 -- fixed Fly API origin
        request, timeout=_FLY_HTTP_TIMEOUT_SECONDS
    ) as response:
        payload = json.loads(response.read())
    if not isinstance(payload, list):
        raise SoakEvidenceError("Fly machines response must be an array")
    listed = {
        item.get("id"): item.get("state")
        for item in payload
        if isinstance(item, dict) and isinstance(item.get("id"), str)
    }
    states = {machine_id: listed.get(machine_id) for machine_id in machine_ids}
    if any(not isinstance(state, str) or not state for state in states.values()):
        raise SoakEvidenceError("an exact cloud Fly machine is missing or has no state")
    return {machine_id: str(state) for machine_id, state in states.items()}


def _read_cloud_fly_machine_restart_counts(
    machine_ids: Sequence[str], *, app: str, token: str
) -> dict[str, int]:
    """Read exact Fly restart counters; a ``started`` Machine may still be looping."""
    if not token:
        raise SoakEvidenceError("POLYARB_FLY_API_TOKEN is required for watchdog event reads")
    if not machine_ids or len(machine_ids) > _MAX_FLY_MACHINES_PER_APP:
        raise SoakEvidenceError("cloud Fly target count is outside the bounded app envelope")
    counts: dict[str, int] = {}
    failures: list[BaseException] = []
    result_lock = Lock()

    def read_one(machine_id: str) -> None:
        try:
            request = Request(
                f"https://api.machines.dev/v1/apps/{app}/machines/{machine_id}",
                headers={"Authorization": f"Bearer {token}"},
            )
            with urlopen(  # noqa: S310 -- fixed Fly API origin
                request, timeout=_FLY_HTTP_TIMEOUT_SECONDS
            ) as response:
                payload = json.loads(response.read())
            if not isinstance(payload, dict) or payload.get("id") != machine_id:
                raise SoakEvidenceError("an exact cloud Fly machine event record is missing")
            events = payload.get("events")
            if not isinstance(events, list):
                raise SoakEvidenceError("cloud Fly machine events must be an array")
            restart_count = max(
                (
                    count
                    for event in events
                    if isinstance(event, dict)
                    and isinstance(event.get("request"), dict)
                    and isinstance((count := event["request"].get("restart_count")), int)
                ),
                default=0,
            )
            with result_lock:
                counts[machine_id] = restart_count
        except BaseException as error:
            with result_lock:
                failures.append(error)

    threads = [
        Thread(
            target=read_one,
            args=(machine_id,),
            name=f"runtime-watchdog:fly-detail:{machine_id}",
            daemon=True,
        )
        for machine_id in machine_ids
    ]
    for thread in threads:
        thread.start()
    round_deadline = monotonic() + _FLY_HTTP_TIMEOUT_SECONDS + 0.5
    for thread in threads:
        thread.join(max(0.0, round_deadline - monotonic()))
    if any(thread.is_alive() for thread in threads) or failures or len(counts) != len(machine_ids):
        raise SoakEvidenceError("cloud Fly machine event round did not complete")
    return counts


def _read_cloud_fly_machine_snapshot(
    machine_ids: Sequence[str], *, app: str, token: str
) -> tuple[dict[str, str], dict[str, int]]:
    """Read one app in a fixed list round plus one parallel detail round."""
    return (
        _read_cloud_fly_machine_states(machine_ids, app=app, token=token),
        _read_cloud_fly_machine_restart_counts(machine_ids, app=app, token=token),
    )


def _record_soak_observation(args: argparse.Namespace, *, exclusive: bool) -> dict[str, object]:
    snapshot = _read_soak_control_snapshot(args.control_api_url)
    record = create_record(
        observed_at=datetime.now(UTC).isoformat(),
        control_api_url=args.control_api_url,
        machine_states=_read_fly_machine_states(args.machine_id, app=args.fly_app),
        control_snapshot=snapshot,
    )
    append_record(args.output, record, exclusive=exclusive)
    return {
        "status": "baseline-recorded" if exclusive else "sample-recorded",
        "evidence": str(args.output),
        "observed_at": record["observed_at"],
        "machine_count": len(args.machine_id),
    }


def _record_cloud_soak_observation(
    control_plane: PostgresControlPlane,
    args: argparse.Namespace,
    *,
    baseline: bool,
    stop_requested: Callable[[], bool] | None = None,
) -> dict[str, object] | None:
    if stop_requested is not None and stop_requested():
        return None
    record = create_record(
        observed_at=datetime.now(UTC).isoformat(),
        control_api_url=args.control_api_url,
        machine_states=_read_cloud_fly_machine_states(
            args.machine_id,
            app=args.fly_app,
            token=os.environ.get("POLYARB_FLY_API_TOKEN", ""),
        ),
        control_snapshot=_read_soak_control_snapshot(args.control_api_url),
    )
    if stop_requested is not None and stop_requested():
        return None
    if baseline:
        control_plane.start_soak_run(run_id=args.run_id, baseline_record=record)
    else:
        control_plane.append_soak_observation(run_id=args.run_id, record=record)
    return {
        "status": "cloud-baseline-recorded" if baseline else "cloud-sample-recorded",
        "run_id": args.run_id,
        "observed_at": record["observed_at"],
        "machine_count": len(args.machine_id),
    }


async def _run_cloud_soak_service(
    control_plane: PostgresControlPlane,
    args: argparse.Namespace,
    *,
    stop_event: asyncio.Event | None = None,
    grace_seconds: float | None = None,
) -> dict[str, object]:
    """Sample at fixed cadence; a read/write failure exits and leaves a proof gap."""
    if args.interval_seconds <= 0:
        raise ValueError("cloud soak interval must be positive")
    stop = stop_event or asyncio.Event()
    if stop_event is None:
        loop = asyncio.get_running_loop()
        for stop_signal in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(stop_signal, stop.set)
            except (NotImplementedError, RuntimeError):
                pass
    samples = 0
    stop_grace = (
        CONTROL_PLANE_DB_POLICY.stop_grace_seconds if grace_seconds is None else grace_seconds
    )
    while not stop.is_set():
        turn_stop = Event()
        completed, outcome = await run_blocking_call_until_stopped(
            lambda: _record_cloud_soak_observation(
                control_plane,
                args,
                baseline=False,
                stop_requested=turn_stop.is_set,
            ),
            stop_event=stop,
            grace_seconds=stop_grace,
            point_of_no_return=True,
            request_stop=turn_stop.set,
            thread_name="control-plane-cloud-soak:sample",
        )
        if not completed or outcome is None:
            break
        _write(
            {"event": "cloud-soak-sample", **outcome},
            as_json=args.json,
        )
        samples += 1
        try:
            await asyncio.wait_for(stop.wait(), timeout=args.interval_seconds)
        except TimeoutError:
            continue
    return {"status": "stopped", "samples": samples}


def _read_runtime_watchdog_observation(
    args: argparse.Namespace,
    *,
    restart_gate: RestartEventGate | None = None,
    progress_gate: ProgressGate | None = None,
    soak_evidence_gate: SoakEvidenceGate | None = None,
    cloud_usage_gate: CloudUsageGate | None = None,
) -> RuntimeObservation:
    """Classify independently bounded API and Fly reads without touching Postgres."""
    control_api_payload: dict[str, object] | None = None
    control_api_error: BaseException | None = None
    machine_states: dict[str, str] = {}
    restart_counts: dict[str, int] = {}
    machine_error: BaseException | None = None
    expected_machine_ids: list[str] = []
    token = os.environ.get("POLYARB_FLY_API_TOKEN", "")
    target_apps: dict[str, tuple[str, ...]] = {}
    try:
        secondary_ids = args.secondary_machine_id or []
        if bool(args.secondary_fly_app) != bool(secondary_ids):
            raise ValueError(
                "secondary watchdog target requires both --secondary-fly-app and "
                "at least one --secondary-machine-id"
            )
        configured_apps: dict[str, list[str]] = {args.fly_app: list(args.machine_id)}
        if args.secondary_fly_app:
            configured_apps.setdefault(args.secondary_fly_app, []).extend(secondary_ids)
        for target in args.secondary_target or []:
            app, separator, machine_id = target.partition("/")
            if not separator or not app or not machine_id or "/" in machine_id:
                raise ValueError("secondary watchdog target must use <app>/<machine-id> form")
            configured_apps.setdefault(app, []).append(machine_id)
        if len(configured_apps) > _MAX_WATCHDOG_APPS:
            raise ValueError("watchdog app count exceeds the bounded observation envelope")
        target_apps = {
            app: tuple(dict.fromkeys(machine_ids)) for app, machine_ids in configured_apps.items()
        }
    except ValueError as error:
        machine_error = error

    provider_lock = Lock()
    app_results: dict[str, tuple[dict[str, str], dict[str, int]]] = {}
    app_errors: list[BaseException] = []

    def read_control_api() -> None:
        nonlocal control_api_payload, control_api_error
        try:
            payload = _read_soak_control_snapshot(args.control_api_url)
        except (OSError, SoakEvidenceError, ValueError) as error:
            with provider_lock:
                control_api_error = error
        else:
            with provider_lock:
                control_api_payload = payload

    def read_app(app: str, machine_ids: tuple[str, ...]) -> None:
        try:
            snapshot = _read_cloud_fly_machine_snapshot(machine_ids, app=app, token=token)
        except BaseException as error:
            with provider_lock:
                app_errors.append(error)
        else:
            with provider_lock:
                app_results[app] = snapshot

    threads = [
        Thread(
            target=read_control_api,
            name="runtime-watchdog:control-api-read",
            daemon=True,
        )
    ]
    if machine_error is None:
        threads.extend(
            Thread(
                target=read_app,
                args=(app, machine_ids),
                name=f"runtime-watchdog:app-read:{app}",
                daemon=True,
            )
            for app, machine_ids in target_apps.items()
        )
    for thread in threads:
        thread.start()
    operation_deadline = monotonic() + _WATCHDOG_OBSERVATION_TIMEOUT_SECONDS
    for thread in threads:
        thread.join(max(0.0, operation_deadline - monotonic()))
    if control_api_payload is None and control_api_error is None:
        control_api_error = SoakEvidenceError("control API observation round did not complete")
    if machine_error is None and (
        app_errors
        or any(thread.is_alive() for thread in threads[1:])
        or len(app_results) != len(target_apps)
    ):
        machine_error = SoakEvidenceError("Fly observation round did not complete")

    if machine_error is None:
        for app, (states, restarts) in app_results.items():
            for machine_id, state in states.items():
                qualified_id = f"{app}/{machine_id}"
                machine_states[qualified_id] = state
                restart_counts[qualified_id] = restarts[machine_id]
                expected_machine_ids.append(qualified_id)
    observation = assess_runtime(
        machine_states=machine_states,
        expected_machine_ids=expected_machine_ids if machine_error is None else (),
        control_api_payload=control_api_payload,
        control_api_error=control_api_error,
        machine_error=machine_error,
    )
    if restart_gate is None:
        restart_observation = observation
    else:
        restart_observation = restart_gate.apply(observation, restart_counts)
    now = datetime.now(UTC)
    progress_observation = (
        restart_observation
        if progress_gate is None
        else progress_gate.apply(restart_observation, control_api_payload, now=now)
    )
    evidence_observation = (
        progress_observation
        if soak_evidence_gate is None
        else soak_evidence_gate.apply(progress_observation, control_api_payload, now=now)
    )
    return (
        evidence_observation
        if cloud_usage_gate is None
        else cloud_usage_gate.apply(evidence_observation, control_api_payload, now=now)
    )


async def _send_runtime_watchdog_telegram(settings: Settings, text: str) -> None:
    """Deliver an operational page directly, with no outbox or database dependency."""
    token = settings.telegram_bot_token.get_secret_value()
    chat_id = settings.telegram_chat_id
    if not token or not chat_id:
        raise ValueError("watchdog requires Telegram credentials")

    def post() -> None:
        payload = json.dumps({"chat_id": chat_id, "text": text}).encode()
        request = Request(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urlopen(request, timeout=5) as response:  # noqa: S310 -- fixed Telegram endpoint
            response_payload = json.loads(response.read())
        if not isinstance(response_payload, dict) or response_payload.get("ok") is not True:
            raise OSError("Telegram rejected watchdog page")

    await run_blocking_call(post, thread_name="runtime-watchdog:telegram-post")


async def _persist_runtime_watchdog_transition(
    settings: Settings, payload: dict[str, object]
) -> object:
    """Submit redacted transition facts to the private ledger writer.

    The alert process retains its deliberately database-free boundary.  The
    writer endpoint is private/authenticated and owns the scoped DB role.
    """
    url = settings.runtime_event_writer_url.rstrip("/")
    token = settings.runtime_event_writer_token.get_secret_value()
    if not url or not token:
        raise OSError("runtime-event-writer-unconfigured")
    body = json.dumps(payload, sort_keys=True).encode()
    idempotency_key = sha256(body).hexdigest()

    def post() -> object:
        request = Request(
            f"{url}/runtime-events",
            data=body,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
                "Idempotency-Key": idempotency_key,
            },
            method="POST",
        )
        with urlopen(request, timeout=5) as response:  # noqa: S310 -- configured private endpoint
            if response.status != 201:
                raise OSError("runtime-event-writer-rejected")
            response_payload = json.loads(response.read())
        if not isinstance(response_payload, dict):
            raise OSError("runtime-event-writer-invalid-response")
        return response_payload

    return await run_blocking_call(post, thread_name="runtime-watchdog:event-writer-post")


async def _run_runtime_watchdog_service(
    args: argparse.Namespace, settings: Settings
) -> dict[str, object]:
    """Keep monitoring independent from the transactional data and alert workers."""
    stop_event = asyncio.Event()
    restart_gate = RestartEventGate()
    progress_gate = ProgressGate(max_stall=timedelta(minutes=5))
    soak_evidence_gate = SoakEvidenceGate(
        max_age=timedelta(minutes=15), expected_run_id=args.soak_run_id
    )
    cloud_usage_gate = CloudUsageGate(max_age=timedelta(minutes=15))
    loop = asyncio.get_running_loop()
    for stop_signal in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(stop_signal, stop_event.set)
        except (NotImplementedError, RuntimeError):
            pass

    async def write_heartbeat(observation: RuntimeObservation) -> None:
        _write(
            {
                "event": "runtime-watchdog-check",
                "healthy": observation.healthy,
                "failures": list(observation.failures),
            },
            as_json=args.json,
        )

    async def persist_transition(payload: dict[str, object]) -> object:
        return await _persist_runtime_watchdog_transition(settings, payload)

    return await run_watchdog_service(
        observe=lambda: _read_runtime_watchdog_observation(
            args,
            restart_gate=restart_gate,
            progress_gate=progress_gate,
            soak_evidence_gate=soak_evidence_gate,
            cloud_usage_gate=cloud_usage_gate,
        ),
        send=lambda text: _send_runtime_watchdog_telegram(settings, text),
        persist_transition=persist_transition,
        on_check=write_heartbeat,
        interval_seconds=args.interval_seconds,
        stop_event=stop_event,
    )


def _r2_upload_fault_callback(
    *, target_job_key: str | None, acknowledgement: str | None
) -> Callable[[JobLease], None] | None:
    """Create the explicit staging-only crash boundary for takeover acceptance."""
    if target_job_key is None:
        if acknowledgement is not None:
            raise ValueError("fault acknowledgement requires a target job key")
        return None
    if acknowledgement != _R2_UPLOAD_FAULT_ACK:
        raise ValueError("fault injection requires the exact staging acknowledgement")

    def crash_matching_lease(lease: JobLease) -> None:
        if lease.job_key == target_job_key:
            raise KeyboardInterrupt("intentional staging crash after verified R2 upload")

    return crash_matching_lease


def _retry_fault_callback(
    *, target_job_key: str | None, attempts: int | None, acknowledgement: str | None
) -> Callable[[JobLease], None] | None:
    """Create one exact, finite staging retry boundary before durable receipt."""
    if target_job_key is None and attempts is None:
        if acknowledgement == _RETRY_FAULT_ACK:
            raise ValueError("retry fault acknowledgement requires a target job key and attempts")
        return None
    if target_job_key is None or attempts is None or attempts <= 0:
        raise ValueError("retry fault target and attempts must be positive and paired")
    if acknowledgement != _RETRY_FAULT_ACK:
        raise ValueError("retry fault injection requires the exact staging acknowledgement")
    remaining = attempts

    def fail_matching_lease(lease: JobLease) -> None:
        nonlocal remaining
        if lease.job_key == target_job_key and remaining > 0:
            remaining -= 1
            raise IntentionalStagingRetryFault("intentional staging retry before receipt")

    return fail_matching_lease


def _transactional_quote_workers(
    control_plane: PostgresControlPlane,
    *,
    worker_id: str,
    crash_after_r2_upload: Callable[[JobLease], None] | None = None,
    retry_fault_before_receipt: Callable[[JobLease], None] | None = None,
    acceptance_run_id: str | None = None,
    lane_count: int = 1,
) -> tuple[
    TransactionalQuoteBatchWorker | TransactionalQuoteBatchPool,
    TransactionalQuoteCertifier,
]:
    """Build explicitly invoked workers; nothing schedules these by default."""
    if lane_count <= 0:
        raise ValueError("lane_count must be positive")
    provider_policy = runtime_policy("quote-batch", 120)
    settings = Settings().model_copy(
        update={"http_timeout_s": provider_policy.provider_timeout_seconds}
    )
    if not settings.r2_enabled:
        raise RuntimeError("transactional Quote requires configured R2 credentials")
    object_client = _build_client(
        settings.r2_endpoint,
        settings.r2_access_key_id.get_secret_value(),
        settings.r2_secret_access_key.get_secret_value(),
        config=control_plane_r2_config(provider_policy.provider_timeout_seconds),
    )
    reader = cast(Any, ClobReaderClient(settings))
    lanes = tuple(
        TransactionalQuoteBatchWorker(
            control_plane=control_plane,
            reader=reader,
            object_client=cast(Any, object_client),
            bucket=settings.r2_bucket,
            worker_id=(worker_id if lane_count == 1 else f"{worker_id}:{ordinal}"),
            now=lambda: datetime.now(UTC),
            crash_after_r2_upload=crash_after_r2_upload,
            retry_fault_before_receipt=retry_fault_before_receipt,
            acceptance_run_id=acceptance_run_id,
        )
        for ordinal in range(lane_count)
    )
    batch_worker: TransactionalQuoteBatchWorker | TransactionalQuoteBatchPool = (
        lanes[0] if lane_count == 1 else TransactionalQuoteBatchPool(lanes=lanes)
    )
    return (
        batch_worker,
        TransactionalQuoteCertifier(
            control_plane=control_plane,
            worker_id=f"{worker_id}:certifier",
            now=lambda: datetime.now(UTC),
        ),
    )


def _transactional_structure_worker(
    control_plane: PostgresControlPlane,
    *,
    worker_id: str,
    crash_after_r2_upload: Callable[[JobLease], None] | None = None,
    retry_fault_before_receipt: Callable[[JobLease], None] | None = None,
    acceptance_run_id: str | None = None,
    lane_count: int = 1,
) -> TransactionalStructureWorker | TransactionalStructureRangePool:
    """Build an explicitly invoked worker; it never exports or changes pointers."""
    if lane_count <= 0:
        raise ValueError("lane_count must be positive")
    object_client, bucket = _structure_object_client()
    lanes = tuple(
        TransactionalStructureWorker(
            control_plane=control_plane,
            object_client=object_client,
            bucket=bucket,
            worker_id=(worker_id if lane_count == 1 else f"{worker_id}:{ordinal}"),
            now=lambda: datetime.now(UTC),
            crash_after_r2_upload=crash_after_r2_upload,
            retry_fault_before_receipt=retry_fault_before_receipt,
            acceptance_run_id=acceptance_run_id,
        )
        for ordinal in range(lane_count)
    )
    return lanes[0] if lane_count == 1 else TransactionalStructureRangePool(lanes=lanes)


def _transactional_structure_source_worker(
    control_plane: PostgresControlPlane,
    *,
    worker_id: str,
    lane_count: int = 8,
) -> TransactionalStructureSourcePool:
    """Build bounded Gamma lanes; API and range workers never receive them."""
    if lane_count <= 0:
        raise ValueError("lane_count must be positive")
    object_client, bucket = _structure_object_client()
    provider_policy = runtime_policy("structure-fetch", 120)
    settings = Settings().model_copy(
        update={
            "http_timeout_s": provider_policy.provider_timeout_seconds,
            "retry_attempts": provider_policy.provider_attempts,
        }
    )
    return TransactionalStructureSourcePool(
        lanes=tuple(
            TransactionalStructureSourceWorker(
                control_plane=control_plane,
                gamma=GammaClient(settings),
                object_client=object_client,
                bucket=bucket,
                worker_id=f"{worker_id}:{ordinal}",
                now=lambda: datetime.now(UTC),
                daily_egress_budget_bytes=settings.m1_daily_egress_budget_bytes,
            )
            for ordinal in range(lane_count)
        )
    )


def _transactional_structure_source_materializer(
    control_plane: PostgresControlPlane,
    *,
    worker_id: str,
) -> TransactionalStructureSourceMaterializer:
    object_client, bucket = _structure_object_client()
    return TransactionalStructureSourceMaterializer(
        control_plane=control_plane,
        object_client=object_client,
        bucket=bucket,
        worker_id=worker_id,
        now=lambda: datetime.now(UTC),
        range_max_rows=1_000,
    )


def _transactional_structure_source_admitter(
    control_plane: PostgresControlPlane,
    *,
    structure_high_water: int = 1,
    quote_high_water: int = 512,
) -> TransactionalStructureSourceAdmitter:
    """Open cadence windows only inside the transactional worker service."""
    return TransactionalStructureSourceAdmitter(
        control_plane=control_plane,
        cadence_seconds=300,
        structure_high_water=structure_high_water,
        quote_high_water=quote_high_water,
        now=lambda: datetime.now(UTC),
    )


def _transactional_quote_admitter(
    control_plane: PostgresControlPlane,
    *,
    worker_id: str,
) -> TransactionalQuoteAdmitter:
    """Build the R2-only Structure-to-Quote bridge in the worker service."""
    object_client, bucket = _structure_object_client()
    return TransactionalQuoteAdmitter(
        control_plane=control_plane,
        object_client=object_client,
        bucket=bucket,
        worker_id=worker_id,
        now=lambda: datetime.now(UTC),
        batch_size=Settings().clob_batch_size,
    )


def _transactional_scheduler(
    control_plane: PostgresControlPlane,
    *,
    worker_id: str,
    max_turns: int,
    structure_materializer_turns: int,
    structure_range_turns: int,
    structure_high_water: int = 1,
    quote_high_water: int = 512,
    include_structure_range: bool = True,
    include_quote_batch: bool = True,
    crash_after_r2_upload: Callable[[JobLease], None] | None = None,
    retry_fault_before_receipt: Callable[[JobLease], None] | None = None,
    acceptance_run_id: str | None = None,
) -> TransactionalControlPlaneScheduler:
    quote_worker, quote_certifier = _transactional_quote_workers(
        control_plane,
        worker_id=f"{worker_id}:quote",
        crash_after_r2_upload=crash_after_r2_upload,
        retry_fault_before_receipt=retry_fault_before_receipt,
        acceptance_run_id=acceptance_run_id,
    )
    object_client, bucket = _structure_object_client()
    return TransactionalControlPlaneScheduler(
        structure_source_admitter=_transactional_structure_source_admitter(
            control_plane,
            structure_high_water=structure_high_water,
            quote_high_water=quote_high_water,
        ),
        structure_source_worker=_transactional_structure_source_worker(
            control_plane, worker_id=f"{worker_id}:structure-source"
        ),
        structure_source_materializer=_transactional_structure_source_materializer(
            control_plane, worker_id=f"{worker_id}:structure-materializer"
        ),
        structure_worker=TransactionalStructureWorker(
            control_plane=control_plane,
            object_client=object_client,
            bucket=bucket,
            worker_id=f"{worker_id}:structure",
            now=lambda: datetime.now(UTC),
            crash_after_r2_upload=crash_after_r2_upload,
            retry_fault_before_receipt=retry_fault_before_receipt,
            acceptance_run_id=acceptance_run_id,
        ),
        structure_certifier=TransactionalStructureCertifier(
            control_plane=control_plane,
            object_client=object_client,
            bucket=bucket,
            worker_id=f"{worker_id}:structure-certifier",
            now=lambda: datetime.now(UTC),
        ),
        quote_admitter=_transactional_quote_admitter(
            control_plane, worker_id=f"{worker_id}:quote-admitter"
        ),
        quote_worker=quote_worker,
        quote_certifier=quote_certifier,
        opportunity_certifier=TransactionalOpportunityCertifier(
            control_plane=control_plane,
            object_client=object_client,
            bucket=bucket,
            worker_id=f"{worker_id}:opportunity-certifier",
            now=lambda: datetime.now(UTC),
        ),
        max_turns=max_turns,
        structure_materializer_turns=structure_materializer_turns,
        structure_range_turns=structure_range_turns,
        include_structure_range=include_structure_range,
        include_quote_batch=include_quote_batch,
    )


def _structure_object_client() -> tuple[Any, str]:
    settings = Settings()
    if not settings.r2_enabled:
        raise RuntimeError("transactional Structure requires configured R2 credentials")
    provider_policy = runtime_policy("structure-certify", 30)
    return (
        _build_client(
            settings.r2_endpoint,
            settings.r2_access_key_id.get_secret_value(),
            settings.r2_secret_access_key.get_secret_value(),
            config=control_plane_r2_config(provider_policy.provider_timeout_seconds),
        ),
        settings.r2_bucket,
    )


async def _run_scheduler_service(
    scheduler: TransactionalControlPlaneScheduler | TransactionalWorkerLoop,
    *,
    interval_seconds: float,
    as_json: bool,
) -> dict[str, object]:
    """Own signal delivery while the scheduler owns only bounded worker turns."""
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def request_stop() -> None:
        stop_event.set()

    for stop_signal in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(stop_signal, request_stop)
        except (NotImplementedError, RuntimeError):
            # The service is still safely stoppable by its hosting runtime on
            # platforms that cannot install asyncio signal handlers.
            pass

    async def emit_tick(outcome: dict[str, object]) -> None:
        _write({"event": "tick", **outcome}, as_json=as_json)

    try:
        return await scheduler.run_until_stopped(
            stop_event=stop_event,
            interval_seconds=interval_seconds,
            on_tick=emit_tick,
        )
    finally:
        await scheduler.aclose()


async def _run_alert_service(
    worker: TransactionalAlertDeliveryWorker,
    *,
    interval_seconds: float,
    as_json: bool,
    stop_event: asyncio.Event | None = None,
) -> dict[str, object]:
    """Run alert delivery separately from all data-plane process groups."""
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    stop = stop_event or asyncio.Event()
    if stop_event is None:
        loop = asyncio.get_running_loop()
        for stop_signal in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(stop_signal, stop.set)
            except (NotImplementedError, RuntimeError):
                pass
    turns = 0
    while not stop.is_set():
        completed, result = await run_blocking_call_until_stopped(
            lambda: asyncio.run(worker.run_once()),
            stop_event=stop,
            grace_seconds=ALERT_DELIVERY_POLICY.stop_grace_seconds,
            point_of_no_return=True,
            request_stop=getattr(worker, "request_stop", None),
            thread_name="control-plane-alert:delivery-turn",
        )
        if not completed:
            break
        if result is None or not hasattr(result, "outbox_id") or not hasattr(result, "outcome"):
            raise TypeError("alert delivery turn returned an invalid result")
        turns += 1
        _write(
            {"event": "alert-delivery", "outbox_id": result.outbox_id, "outcome": result.outcome},
            as_json=as_json,
        )
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_seconds)
        except TimeoutError:
            continue
    return {"status": "stopped", "turns": turns}


async def _run_one_structure_source_window(
    control_plane: PostgresControlPlane,
    *,
    window_key: str,
    worker_id: str,
) -> dict[str, object]:
    """Admit one named window and close its Gamma transport in the same loop."""
    control_plane.admit_structure_source_window(window_key=window_key, now=datetime.now(UTC))
    worker = _transactional_structure_source_worker(control_plane, worker_id=worker_id)
    try:
        result = await worker.run_once()
    finally:
        await worker.aclose()
    return {
        "status": "ok",
        "window_key": window_key,
        "page": {"job_key": result.job_key, "outcome": result.outcome},
        "pointer_mutations": 0,
    }


def _runtime_safe_text(value: object, *, limit: int = 256) -> str:
    text = str(value).replace("\x00", "")
    text = "".join(character if character.isprintable() else " " for character in text)
    if any(
        marker in text.casefold()
        for marker in ("authorization", "api_key", "apikey", "password", "secret", "token=")
    ):
        return "<redacted>"
    return text[:limit]


def _runtime_controller_payload(controller: RuntimeControllerLease) -> dict[str, object]:
    return {
        "controller_id": _runtime_safe_text(controller.controller_id),
        "owner_id": _runtime_safe_text(controller.owner_id),
        "lease_epoch": controller.lease_epoch,
        "lease_expires_at": controller.lease_expires_at.astimezone(UTC).isoformat(),
    }


def _runtime_attempt_payload(candidate: RuntimeReconcileCandidate) -> dict[str, object]:
    state = candidate.runtime_state
    return {
        "attempt_id": _runtime_safe_text(state.attempt_id),
        "lease_epoch": state.lease_epoch,
        "worker_id": _runtime_safe_text(candidate.worker_id),
        "started_at": state.attempt_started_at.astimezone(UTC).isoformat(),
        "last_heartbeat_at": state.last_heartbeat_at.astimezone(UTC).isoformat(),
        "last_progress_at": state.last_progress_at.astimezone(UTC).isoformat(),
        "lease_expires_at": state.lease_expires_at.astimezone(UTC).isoformat(),
    }


def _runtime_action_lease_payload(
    action: RecoveryActionRecord | None,
) -> dict[str, object]:
    if action is None:
        return {"worker_id": None, "worker_epoch": None, "expires_at": None}
    return {
        "worker_id": None if action.worker_id is None else _runtime_safe_text(action.worker_id),
        "worker_epoch": action.worker_epoch,
        "expires_at": (
            None
            if action.worker_lease_expires_at is None
            else action.worker_lease_expires_at.astimezone(UTC).isoformat()
        ),
    }


def _runtime_result_payload(result: RecoveryActionResult | None) -> dict[str, object] | None:
    if result is None:
        return None
    return {
        "action_id": None if result.action_id is None else _runtime_safe_text(result.action_id),
        "action_type": (
            None if result.action_type is None else _runtime_safe_text(result.action_type)
        ),
        "target_id": None if result.target_id is None else _runtime_safe_text(result.target_id),
        "outcome": _runtime_safe_text(result.outcome),
        "detail": {
            key: value
            for key, value in result.detail.items()
            if key in {"postcondition", "reason_code", "component", "action_type"}
            and (type(value) in {str, int, bool} or value is None)
        },
    }


def _runtime_reconcile_once(
    control_plane: _RuntimeReconcileControlPlane,
    args: argparse.Namespace,
    *,
    controller: RuntimeControllerLease | None = None,
    recovery_mode: str = "observe-only",
    stop_requested: Callable[[], bool] | None = None,
) -> dict[str, object]:
    """Run one bounded evaluate turn, with mutation fenced by recovery mode.

    Observe-only evaluates and records every bounded candidate, then returns
    before action scheduling or executor construction. Execute mode preserves
    the single-action turn. Store/fencing exceptions are not swallowed; the
    top-level CLI turns them into a non-zero operator result.
    """
    if recovery_mode not in {"observe-only", "execute"}:
        raise ValueError("runtime recovery mode must be observe-only or execute")
    if args.lease_seconds <= 0 or args.action_lease_seconds <= 0:
        raise ValueError("lease seconds must be positive")
    if args.heartbeat_lease_seconds <= 0:
        raise ValueError("heartbeat lease seconds must be positive")
    if args.limit <= 0 or args.limit > 100:
        raise ValueError("limit must be in 1..100")
    target_type = getattr(args, "target_type", None)
    target_id = getattr(args, "target_id", None)
    expected_action = getattr(args, "expected_action", None)
    selector_values = (target_type, target_id, expected_action)
    if any(value is not None for value in selector_values) and not all(
        isinstance(value, str) and value.strip() for value in selector_values
    ):
        raise ValueError("target-type, target-id, and expected-action must be provided together")
    if target_id is not None and recovery_mode != "execute":
        raise ValueError("an exact recovery target is valid only in execute mode")
    _raise_if_runtime_reconcile_stopped(stop_requested)
    now = datetime.now(UTC)
    connection_factory = control_plane._connection_factory
    selected_controller = controller or claim_controller(
        connection_factory,
        controller_id=args.controller_id,
        owner_id=args.owner_id,
        lease_seconds=args.lease_seconds,
        now=now,
    )
    candidates = read_runtime_reconcile_states(
        connection_factory,
        controller_id=selected_controller.controller_id,
        now=now,
        sample_limit=args.limit,
        target_id=target_id,
    )
    if target_id is not None:
        candidates = tuple(
            candidate
            for candidate in candidates
            if candidate.target_type == target_type and candidate.target_id == target_id
        )
        if len(candidates) != 1:
            raise RuntimeError("requested recovery target is not uniquely active")
    _raise_if_runtime_reconcile_stopped(stop_requested)
    reconciler = RuntimeReconciler()
    evaluated: list[tuple[RuntimeReconcileCandidate, RecoveryDecision]] = []
    selected: tuple[RuntimeReconcileCandidate, RecoveryDecision] | None = None
    # Rows are already deterministically ordered by the read projection.  The
    # first actionable fact wins, keeping one turn bounded even with a large
    # incident storm.
    for candidate in candidates:
        decision = reconciler.evaluate(candidate.runtime_state, now=now)
        evaluated.append((candidate, decision))
        if selected is None:
            selected = (candidate, decision)
            if decision.action is not None and recovery_mode == "execute":
                break
            continue
        if decision.action is not None and selected[1].action is None:
            selected = (candidate, decision)
            if recovery_mode == "execute":
                break

    candidate: RuntimeReconcileCandidate | None = None
    decision = None
    scheduled: RecoveryActionRecord | None = None
    result: RecoveryActionResult | None = None
    deferred_by: dict[str, str] | None = None
    observed_decision_count = 0
    if selected is not None:
        candidate, decision = selected
    if expected_action is not None:
        actual_action = (
            None if decision is None or decision.action is None else decision.action.value
        )
        if actual_action != expected_action:
            raise RuntimeError("requested recovery action does not match current facts")
    if recovery_mode == "observe-only":
        if evaluated:
            observe_records = tuple(
                build_runtime_observe_decision_record(
                    controller_id=selected_controller.controller_id,
                    controller_owner_id=selected_controller.owner_id,
                    controller_epoch=selected_controller.lease_epoch,
                    observed_at=now,
                    candidate=observed_candidate,
                    decision=observed_decision,
                    observed_by=selected_controller.owner_id,
                )
                for observed_candidate, observed_decision in evaluated
            )
            insert_runtime_observe_decisions(
                connection_factory,
                observe_records,
                stop_requested=stop_requested,
            )
            observed_decision_count = len(observe_records)
        else:
            cadence_seconds = max(1.0, float(getattr(args, "interval_seconds", 30.0)))
            insert_runtime_observe_decisions(
                connection_factory,
                (
                    build_runtime_observe_idle_record(
                        controller_id=selected_controller.controller_id,
                        controller_owner_id=selected_controller.owner_id,
                        controller_epoch=selected_controller.lease_epoch,
                        observed_at=now,
                        next_check_at=now + timedelta(seconds=cadence_seconds),
                        observed_by=selected_controller.owner_id,
                    ),
                ),
                stop_requested=stop_requested,
            )
            observed_decision_count = 1
    else:
        if candidate is not None and decision is not None:
            _raise_if_runtime_reconcile_stopped(stop_requested)
            action_type = getattr(decision, "action", None)
            if isinstance(action_type, RecoveryActionType):
                try:
                    scheduled = schedule_action(
                        connection_factory,
                        controller=selected_controller,
                        decision=decision,
                        incident_key=candidate.incident_key,
                        component=candidate.component,
                        target_type=candidate.target_type,
                        target_id=candidate.target_id,
                        expected_attempt_id=candidate.runtime_state.attempt_id,
                        expected_lease_epoch=candidate.runtime_state.lease_epoch,
                        recovery_budget_remaining=(
                            candidate.runtime_state.recovery_budget.remaining_actions
                        ),
                        recovery_episode_key=candidate.runtime_state.recovery_episode_key,
                        cooldown_seconds=max(0, candidate.cooldown_seconds),
                        channels=candidate.channels,
                        now=now,
                        detail={
                            "job_type": candidate.job_type,
                            "job_state": candidate.job_state,
                        },
                    )
                except RecoveryProbeLaneBusy as error:
                    deferred_by = {
                        "blocking_kind": _runtime_safe_text(error.blocking_kind),
                        "blocking_target_id": _runtime_safe_text(error.blocking_target_id),
                        "worker_id": _runtime_safe_text(error.worker_id),
                    }
        _raise_if_runtime_reconcile_stopped(stop_requested)
        if deferred_by is None:
            executor = RecoveryExecutor(
                control_plane=control_plane,
                controller=selected_controller,
                worker_id=args.worker_id,
                connection_factory=connection_factory,
                action_lease_seconds=args.action_lease_seconds,
                heartbeat_lease_seconds=args.heartbeat_lease_seconds,
            )
            if expected_action is not None and scheduled is not None:
                result = executor.run_once(now=now, expected_action_id=scheduled.action_id)
            else:
                result = executor.run_once(now=now)
    decision_action = None if decision is None else getattr(decision, "action", None)
    reason = "runtime.no-active-attempts" if decision is None else decision.reason_code
    if deferred_by is not None:
        reason = "circuit.probe-lane-busy"
    if recovery_mode == "observe-only":
        state = "observe-only"
        outcome = "no-mutation"
    elif deferred_by is not None:
        state = "deferred"
        outcome = "worker-lane-busy"
    elif result is not None:
        state = "recovery-executed"
        outcome = result.outcome
    elif scheduled is not None and scheduled.result_code is not None:
        state = "stale-noop" if scheduled.result_code == "stale-noop" else "no-action"
        outcome = scheduled.result_code
    elif decision_action is not None:
        state = "action-scheduled"
        outcome = "pending"
    elif decision is None or decision.reason_code == "job.healthy":
        state = "healthy" if candidate is not None else "idle"
        outcome = "no-action"
    elif decision.reason_code == "recovery.stale-fence":
        state = "stale-noop"
        outcome = "stale-noop"
    else:
        state = "no-action"
        outcome = "no-action"
    action_for_view = scheduled
    if action_for_view is None and result is not None:
        action_for_view = None
    action_name: str | None = None
    if scheduled is not None:
        action_name = scheduled.action_type
    elif result is not None:
        action_name = result.action_type
    elif recovery_mode == "observe-only" and isinstance(decision_action, RecoveryActionType):
        action_name = decision_action.value
    elif deferred_by is not None and isinstance(decision_action, RecoveryActionType):
        action_name = decision_action.value
    budget_remaining = None
    if candidate is not None:
        budget_remaining = candidate.runtime_state.recovery_budget.remaining_actions
        if scheduled is not None and scheduled.state in {"pending", "running"}:
            budget_remaining = max(0, budget_remaining - 1)
    return {
        "status": "ok",
        "recovery_mode": recovery_mode,
        "state": state,
        "reason": reason,
        "action": action_name,
        "outcome": outcome,
        "attempt": None if candidate is None else _runtime_attempt_payload(candidate),
        "job": (
            None
            if candidate is None
            else {
                "job_key": _runtime_safe_text(candidate.target_id),
                "job_type": _runtime_safe_text(candidate.job_type),
                "state": _runtime_safe_text(candidate.job_state),
                "target_type": _runtime_safe_text(candidate.target_type),
            }
        ),
        "controller": _runtime_controller_payload(selected_controller),
        "action_lease": _runtime_action_lease_payload(action_for_view),
        "budget": (
            None
            if candidate is None
            else {
                "remaining_actions": budget_remaining,
                "cooldown_seconds": candidate.cooldown_seconds,
            }
        ),
        "next_check_at": (
            None if decision is None else decision.next_check_at.astimezone(UTC).isoformat()
        ),
        "timestamps": {
            "evaluated_at": now.isoformat(),
            "requested_at": None if scheduled is None else scheduled.requested_at.isoformat(),
            "started_at": None
            if scheduled is None
            else (None if scheduled.started_at is None else scheduled.started_at.isoformat()),
            "finished_at": None
            if scheduled is None
            else (None if scheduled.finished_at is None else scheduled.finished_at.isoformat()),
        },
        "executor": _runtime_result_payload(result),
        "deferred_by": deferred_by,
        "observed_decision_count": observed_decision_count,
        "pointer_mutations": 0,
    }


def _raise_if_runtime_reconcile_stopped(
    stop_requested: Callable[[], bool] | None,
) -> None:
    if stop_requested is not None and stop_requested():
        raise RuntimeError("runtime reconcile stop requested")


async def _run_runtime_reconcile_service(
    control_plane: _RuntimeReconcileControlPlane,
    args: argparse.Namespace,
    *,
    recovery_mode: str = "observe-only",
    stop_event: asyncio.Event | None = None,
) -> dict[str, object]:
    """Run sequential reconciliation turns and fail immediately on store errors."""
    if args.interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    if args.lease_seconds <= 0:
        raise ValueError("lease_seconds must be positive")
    stop = stop_event or asyncio.Event()
    if stop_event is None:
        loop = asyncio.get_running_loop()
        for stop_signal in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(stop_signal, stop.set)
            except (NotImplementedError, RuntimeError):
                pass
    controller: RuntimeControllerLease | None = None
    turns = 0
    while not stop.is_set():
        turn_stop = Event()

        def run_turn() -> dict[str, object]:
            nonlocal controller
            _raise_if_runtime_reconcile_stopped(turn_stop.is_set)
            now = datetime.now(UTC)
            if controller is None:
                controller = claim_controller(
                    control_plane._connection_factory,
                    controller_id=args.controller_id,
                    owner_id=args.owner_id,
                    lease_seconds=args.lease_seconds,
                    now=now,
                )
            else:
                controller = renew_controller(
                    control_plane._connection_factory,
                    controller=controller,
                    lease_seconds=args.lease_seconds,
                    now=now,
                )
            _raise_if_runtime_reconcile_stopped(turn_stop.is_set)
            return _runtime_reconcile_once(
                control_plane,
                args,
                controller=controller,
                recovery_mode=recovery_mode,
                stop_requested=turn_stop.is_set,
            )

        completed, payload = await run_blocking_call_until_stopped(
            run_turn,
            stop_event=stop,
            grace_seconds=RECOVERY_DB_POLICY.stop_grace_seconds,
            point_of_no_return=True,
            request_stop=turn_stop.set,
            thread_name="runtime-controller:reconcile-turn",
        )
        if not completed:
            break
        if not isinstance(payload, dict):
            raise TypeError("runtime reconcile turn returned an invalid result")
        _write({"event": "runtime-reconcile-turn", **payload}, as_json=args.json)
        turns += 1
        try:
            await asyncio.wait_for(stop.wait(), timeout=args.interval_seconds)
        except TimeoutError:
            continue
    return {"status": "stopped", "turns": turns}


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    requires_enable = {
        "quote-once",
        "structure-once",
        "structure-source-once",
        "structure-shadow-once",
        "structure-shadow-publish",
        "tick-once",
        "serve",
        "alert-serve",
        "cloud-soak-serve",
        "watchdog-serve",
        "render-rollout",
        "runtime-reconcile-once",
        "runtime-reconcile-serve",
        "qualification-serve",
    }
    if args.command in requires_enable and not args.enable:
        print(f"--enable is required for {args.command}", file=sys.stderr)
        return 2
    if args.command == "render-rollout":
        try:
            artifacts = render_rollout_artifacts(
                api_app=args.api_app,
                worker_app=args.worker_app,
                alert_app=args.alert_app,
                runtime_event_writer_app=args.runtime_event_writer_app,
                release_id=args.release_id,
                runtime_controller_app=args.runtime_controller_app,
                qualification_worker_app=args.qualification_worker_app,
                runtime_recovery_allowed_targets=tuple(args.runtime_recovery_allowed_target),
                expected_database=args.expected_database,
                output_dir=args.output_dir,
            )
        except (OSError, ValueError) as error:
            print(
                f"rollout artifact rendering unavailable: {type(error).__name__}",
                file=sys.stderr,
            )
            return 1
        _write({"status": "rendered-local-only", **artifacts}, as_json=args.json)
        return 0
    if args.command == "verify-shadow-parity":
        try:
            evidence = json.loads(args.evidence.read_text())
            if not isinstance(evidence, dict):
                raise ValueError("shadow parity evidence must be an object")
            _write(verify_shadow_parity(evidence), as_json=args.json)
        except (OSError, ValueError) as error:
            print(f"shadow parity unavailable: {type(error).__name__}", file=sys.stderr)
            return 1
        return 0
    if args.command == "verify-fault-soak":
        try:
            evidence = json.loads(args.evidence.read_text())
            if not isinstance(evidence, dict):
                raise ValueError("fault/soak evidence must be an object")
            _write(verify_fault_soak(evidence), as_json=args.json)
        except (OSError, ValueError) as error:
            print(f"fault/soak evidence unavailable: {type(error).__name__}", file=sys.stderr)
            return 1
        return 0
    if args.command in {"soak-start", "soak-sample"}:
        try:
            _write(
                _record_soak_observation(args, exclusive=args.command == "soak-start"),
                as_json=args.json,
            )
        except (OSError, SoakEvidenceError, subprocess.SubprocessError, ValueError) as error:
            print(f"soak evidence unavailable: {type(error).__name__}", file=sys.stderr)
            return 1
        return 0
    if args.command == "soak-verify":
        try:
            _write(
                verify_soak(
                    read_records(args.evidence),
                    minimum_seconds=args.minimum_seconds,
                    max_gap_seconds=args.max_gap_seconds,
                ),
                as_json=args.json,
            )
        except (OSError, SoakEvidenceError, ValueError) as error:
            detail = str(error) if isinstance(error, SoakEvidenceError) else type(error).__name__
            print(f"soak evidence unavailable: {detail}", file=sys.stderr)
            return 1
        return 0
    if args.command == "runtime-policy-replay":
        control_plane = _control_plane_from_env()
        if control_plane is None:
            print("POLYARB_SUPABASE_DB_DSN is required", file=sys.stderr)
            return 2
        try:
            replay = replay_soak_observations(
                control_plane.read_soak_observations(args.run_id),
                max_gap_seconds=args.max_gap_seconds,
            )
        except (OSError, RuntimeError, ValueError, SoakEvidenceError, psycopg.Error) as error:
            detail = str(error) if isinstance(error, SoakEvidenceError) else type(error).__name__
            print(f"runtime policy replay unavailable: {detail}", file=sys.stderr)
            return 1
        _write(
            {
                "status": replay.status,
                "first_breaking_at": (
                    None
                    if replay.first_breaking_at is None
                    else replay.first_breaking_at.astimezone(UTC).isoformat()
                ),
                "reason_codes": list(replay.reason_codes),
                "sample_count": replay.sample_count,
                "max_gap_seconds": replay.max_gap_seconds,
            },
            as_json=args.json,
        )
        return 0
    if args.command == "runtime-fault-matrix":
        try:
            _write(run_fault_matrix(), as_json=args.json)
        except RuntimeFaultMatrixError as error:
            print(str(error), file=sys.stderr)
            return 2
        except (OSError, RuntimeError, ValueError, psycopg.Error) as error:
            print(f"runtime fault matrix unavailable: {type(error).__name__}", file=sys.stderr)
            return 1
        return 0
    if args.command == "watchdog-serve":
        try:
            if not os.environ.get("POLYARB_FLY_API_TOKEN", "").strip():
                raise ValueError("watchdog requires POLYARB_FLY_API_TOKEN")
            if args.watchdog_once:
                observation = _read_runtime_watchdog_observation(
                    args,
                    restart_gate=RestartEventGate(),
                    progress_gate=ProgressGate(max_stall=timedelta(minutes=5)),
                    soak_evidence_gate=SoakEvidenceGate(
                        max_age=timedelta(minutes=15), expected_run_id=args.soak_run_id
                    ),
                )
                _write(
                    {
                        "status": "healthy" if observation.healthy else "unhealthy",
                        "failures": list(observation.failures),
                    },
                    as_json=args.json,
                )
                return 0 if observation.healthy else 1
            settings = Settings()
            result = asyncio.run(_run_runtime_watchdog_service(args, settings))
            _write(result, as_json=args.json)
            return 0
        except (OSError, RuntimeError, ValueError) as error:
            print(f"runtime watchdog unavailable: {type(error).__name__}", file=sys.stderr)
            return 1
    if args.command in {
        "qualification-status",
        "qualification-certificates",
        "qualification-serve",
    }:
        try:
            connection_factory = _qualification_connection_factory_from_env()
        except DatabaseRoleContractError as error:
            print(f"qualification service unavailable: {error}", file=sys.stderr)
            return 1
        if connection_factory is None:
            print("POLYARB_QUALIFICATION_DB_DSN is required", file=sys.stderr)
            return 2
        failure_reason = "qualification-service.read-failed"
        try:
            if args.command == "qualification-status":
                store = PostgresQualificationServiceStore(connection_factory)
                _write(
                    {"status": "available", **store.status(now=datetime.now(UTC))},
                    as_json=args.json,
                )
                return 0
            if args.command == "qualification-certificates":
                store = PostgresQualificationServiceStore(connection_factory)
                _write(
                    {
                        "status": "available",
                        "certificates": store.certificates(limit=args.limit),
                    },
                    as_json=args.json,
                )
                return 0
            if args.interval_seconds <= 0 or args.batch_size <= 0:
                print("--interval-seconds and --batch-size must be positive", file=sys.stderr)
                return 2
            verify_daemon_database_role(
                connection_factory,
                "qualification-worker",
                expected_database=_required_expected_database_from_env(),
            )
            failure_reason = "qualification-service.startup-failed"
            service = _qualification_service_from_env(
                batch_size=args.batch_size,
                interval_seconds=args.interval_seconds,
                writer_id=args.writer_id,
            )
            failure_reason = "qualification-service.tick-failed"
            result = asyncio.run(
                run_qualification_service(
                    service,
                    interval_seconds=args.interval_seconds,
                    emit=lambda payload: _write(
                        {"event": "qualification-tick", **payload},
                        as_json=args.json,
                    ),
                )
            )
            _write(result, as_json=args.json)
            return 0
        except DatabaseRoleContractError as error:
            print(f"qualification service unavailable: {error}", file=sys.stderr)
            return 1
        except QualificationIdentityError as error:
            print(f"qualification service unavailable: {error}", file=sys.stderr)
            return 1
        except (OSError, RuntimeError, ValueError, psycopg.Error):
            print(f"qualification service unavailable: {failure_reason}", file=sys.stderr)
            return 1
    try:
        control_plane = _control_plane_from_env()
    except DatabaseRoleContractError as error:
        if args.command in {"runtime-reconcile-once", "runtime-reconcile-serve"}:
            print(f"runtime reconciliation unavailable: {error}", file=sys.stderr)
        else:
            print(f"control-plane command unavailable: {error}", file=sys.stderr)
        return 1
    if control_plane is None:
        print("POLYARB_SUPABASE_DB_DSN is required", file=sys.stderr)
        return 2
    try:
        if args.command in {"cloud-soak-start", "cloud-soak-sample"}:
            try:
                observation = _record_cloud_soak_observation(
                    control_plane,
                    args,
                    baseline=args.command == "cloud-soak-start",
                )
                if observation is None:
                    raise RuntimeError("cloud soak observation stopped before completion")
                _write(
                    observation,
                    as_json=args.json,
                )
            except (OSError, SoakEvidenceError, ValueError) as error:
                print(f"cloud soak evidence unavailable: {type(error).__name__}", file=sys.stderr)
                return 1
            return 0
        if args.command == "cloud-soak-verify":
            try:
                _write(
                    verify_soak(
                        control_plane.read_soak_observations(args.run_id),
                        minimum_seconds=args.minimum_seconds,
                        max_gap_seconds=args.max_gap_seconds,
                    ),
                    as_json=args.json,
                )
            except (SoakEvidenceError, ValueError) as error:
                detail = (
                    str(error) if isinstance(error, SoakEvidenceError) else type(error).__name__
                )
                print(f"cloud soak evidence unavailable: {detail}", file=sys.stderr)
                return 1
            return 0
        if args.command == "cloud-soak-serve":
            try:
                _write(
                    asyncio.run(_run_cloud_soak_service(control_plane, args)),
                    as_json=args.json,
                )
            except (OSError, SoakEvidenceError, ValueError) as error:
                print(f"cloud soak service unavailable: {type(error).__name__}", file=sys.stderr)
                return 1
            return 0
        if args.command == "runtime-controller-status":
            status = read_runtime_controller_status(
                control_plane._connection_factory,
                controller_id=args.controller_id,
                now=datetime.now(UTC),
                sample_limit=args.limit,
            )
            _write({"status": "available", **status}, as_json=args.json)
            return 0
        if args.command == "runtime-observe-verify":
            now = datetime.now(UTC)
            status = read_runtime_controller_status(
                control_plane._connection_factory,
                controller_id=args.controller_id,
                now=now,
                sample_limit=1,
            )
            controller_status = status.get("controller")
            if not isinstance(controller_status, Mapping):
                raise RuntimeError("runtime observe controller identity is unavailable")
            owner_id = controller_status.get("owner_id")
            lease_epoch = controller_status.get("lease_epoch")
            if (
                not isinstance(owner_id, str)
                or not owner_id
                or type(lease_epoch) is not int
                or lease_epoch <= 0
                or controller_status.get("lease_active") is not True
            ):
                raise RuntimeError("runtime observe controller identity is inactive")
            verification = verify_runtime_observe_window(
                control_plane._connection_factory,
                controller_id=args.controller_id,
                controller_owner_id=owner_id,
                controller_epoch=lease_epoch,
                now=now,
                minimum_seconds=args.minimum_seconds,
                max_freshness_seconds=args.max_freshness_seconds,
                max_gap_seconds=args.max_gap_seconds,
                sample_limit=args.limit,
            )
            _write(
                {
                    "status": verification.status,
                    "controller_id": verification.controller_id,
                    "controller_owner_id": verification.controller_owner_id,
                    "controller_epoch": verification.controller_epoch,
                    "started_at": verification.started_at.astimezone(UTC).isoformat(),
                    "latest_observed_at": verification.latest_observed_at.astimezone(
                        UTC
                    ).isoformat(),
                    "duration_seconds": verification.duration_seconds,
                    "decision_count": verification.decision_count,
                    "idle_count": verification.idle_count,
                    "recovery_action_count": verification.recovery_action_count,
                    "current_candidate_count": verification.current_candidate_count,
                    "max_gap_seconds": verification.max_gap_seconds,
                    "latest_decision_digest": verification.latest_decision_digest,
                },
                as_json=args.json,
            )
            return 0
        if args.command == "runtime-reconcile-once":
            verify_daemon_database_role(
                control_plane._connection_factory,
                "runtime-controller",
                expected_database=_required_expected_database_from_env(),
            )
            result = _runtime_reconcile_once(
                control_plane,
                args,
                recovery_mode=Settings().runtime_recovery_mode,
            )
            _write(result, as_json=args.json)
            return 0
        if args.command == "runtime-reconcile-serve":
            verify_daemon_database_role(
                control_plane._connection_factory,
                "runtime-controller",
                expected_database=_required_expected_database_from_env(),
            )
            try:
                result = asyncio.run(
                    _run_runtime_reconcile_service(
                        control_plane,
                        args,
                        recovery_mode=Settings().runtime_recovery_mode,
                    )
                )
            except KeyboardInterrupt:
                _write(
                    {"status": "stopped", "reason": "keyboard-interrupt"},
                    as_json=args.json,
                )
                return 130
            _write(result, as_json=args.json)
            return 0
        if args.command == "preflight":
            database = control_plane.deployment_preflight(expected_database=args.expected_database)
            object_client, bucket = _structure_object_client()
            object_client.head_bucket(Bucket=bucket)
            _write(
                {
                    "status": "ready-for-shadow-only",
                    "control_plane": database,
                    "r2": {"bucket": bucket, "reachable": True},
                },
                as_json=args.json,
            )
            return 0
        if args.command == "shadow-sync":
            sources = read_shadow_sources(args.db_path, limit=args.limit)
            projected = project_shadow_sources(
                sources,
                control_plane=control_plane,
                now=datetime.now(UTC),
            )
            _write(
                {
                    "status": "ok",
                    "projected_sources": projected,
                    "pointer_mutations": 0,
                },
                as_json=args.json,
            )
            return 0
        if args.command == "quote-once":
            batch_worker, certifier = _transactional_quote_workers(
                control_plane, worker_id=args.worker_id
            )
            batch_result = asyncio.run(batch_worker.run_once())
            certifier_result = certifier.run_once()
            _write(
                {
                    "status": "ok",
                    "batch": {
                        "job_key": batch_result.job_key,
                        "outcome": batch_result.outcome,
                    },
                    "certifier": {
                        "job_key": certifier_result.job_key,
                        "outcome": certifier_result.outcome,
                    },
                },
                as_json=args.json,
            )
            return 0
        if args.command == "structure-once":
            worker = _transactional_structure_worker(control_plane, worker_id=args.worker_id)
            result = asyncio.run(worker.run_once())
            _write(
                {
                    "status": "ok",
                    "range": {"job_key": result.job_key, "outcome": result.outcome},
                    "pointer_mutations": 0,
                },
                as_json=args.json,
            )
            return 0
        if args.command == "structure-source-once":
            _write(
                asyncio.run(
                    _run_one_structure_source_window(
                        control_plane,
                        window_key=args.window_key,
                        worker_id=args.worker_id,
                    )
                ),
                as_json=args.json,
            )
            return 0
        if args.command == "structure-shadow-once":
            identity, components = read_legacy_structure_bundle(
                args.db_path, publication_id=args.publication_id
            )
            artifact = StructureBundleArtifact.from_bytes(
                canonical_structure_bundle_bytes(identity=identity, components=components)
            )
            object_client, bucket = _structure_object_client()
            upload_structure_bundle_artifact(object_client, bucket=bucket, artifact=artifact)
            admitted = control_plane.enqueue_structure_generation(
                identity=identity,
                bundle=artifact,
                ranges=plan_structure_ranges(components, max_rows=args.range_max_rows),
                now=datetime.now(UTC),
            )
            _write(
                {
                    "status": "ok",
                    "source_identity": identity.header(),
                    "bundle_digest": artifact.sha256,
                    "admitted_job_count": len(admitted),
                    "pointer_mutations": 0,
                },
                as_json=args.json,
            )
            return 0
        if args.command == "structure-shadow-publish":
            if not args.generation_key.startswith("structure:"):
                print("--generation-key must name a Structure generation", file=sys.stderr)
                return 2
            before = control_plane.structure_shadow_pointer()
            current = control_plane.publish_structure_shadow(
                generation_key=args.generation_key,
                now=datetime.now(UTC),
            )
            _write(
                {
                    "status": "ok",
                    "previous_generation_key": (
                        None if before is None else before["generation_key"]
                    ),
                    "current_generation_key": current,
                    "legacy_pointer_mutations": 0,
                },
                as_json=args.json,
            )
            return 0
        if args.command == "tick-once":
            if (
                args.max_turns <= 0
                or args.structure_materializer_turns < 0
                or args.structure_range_turns < 0
            ):
                print(
                    "--max-turns must be positive and optional turn budgets non-negative",
                    file=sys.stderr,
                )
                return 2
            crash_after_r2_upload = _r2_upload_fault_callback(
                target_job_key=args.fault_crash_after_r2_upload_job_key,
                acknowledgement=(
                    args.fault_injection_ack
                    if args.fault_crash_after_r2_upload_job_key is not None
                    else None
                ),
            )
            retry_fault_before_receipt = _retry_fault_callback(
                target_job_key=args.fault_retry_job_key,
                attempts=args.fault_retry_attempts,
                acknowledgement=args.fault_injection_ack,
            )
            scheduler = _transactional_scheduler(
                control_plane,
                worker_id=args.worker_id,
                max_turns=args.max_turns,
                structure_materializer_turns=args.structure_materializer_turns,
                structure_range_turns=args.structure_range_turns,
                crash_after_r2_upload=crash_after_r2_upload,
                retry_fault_before_receipt=retry_fault_before_receipt,
                acceptance_run_id=args.acceptance_run_id,
            )
            _write(asyncio.run(scheduler.run_tick()), as_json=args.json)
            return 0
        if args.command == "serve":
            if (
                args.max_turns <= 0
                or args.pool_turns <= 0
                or args.structure_high_water <= 0
                or args.quote_high_water <= 0
                or args.structure_materializer_turns < 0
                or args.structure_range_turns < 0
                or args.interval_seconds <= 0
            ):
                print(
                    "--max-turns and --interval-seconds must be positive; "
                    "--pool-turns and high-water bounds must be positive; "
                    "optional turn budgets must be non-negative",
                    file=sys.stderr,
                )
                return 2
            crash_after_r2_upload = _r2_upload_fault_callback(
                target_job_key=args.fault_crash_after_r2_upload_job_key,
                acknowledgement=(
                    args.fault_injection_ack
                    if args.fault_crash_after_r2_upload_job_key is not None
                    else None
                ),
            )
            retry_fault_before_receipt = _retry_fault_callback(
                target_job_key=args.fault_retry_job_key,
                attempts=args.fault_retry_attempts,
                acknowledgement=args.fault_injection_ack,
            )
            if args.worker_role in {"all", "coordinator"}:
                scheduler: TransactionalControlPlaneScheduler | TransactionalWorkerLoop
                scheduler = _transactional_scheduler(
                    control_plane,
                    worker_id=args.worker_id,
                    max_turns=args.max_turns,
                    structure_materializer_turns=args.structure_materializer_turns,
                    structure_range_turns=args.structure_range_turns,
                    structure_high_water=args.structure_high_water,
                    quote_high_water=args.quote_high_water,
                    include_structure_range=args.worker_role == "all",
                    include_quote_batch=args.worker_role == "all",
                    crash_after_r2_upload=crash_after_r2_upload,
                    retry_fault_before_receipt=retry_fault_before_receipt,
                    acceptance_run_id=args.acceptance_run_id,
                )
            elif args.worker_role == "structure-range":
                if args.structure_high_water != 1 or args.quote_high_water != 512:
                    print(
                        "pool roles cannot configure source admission high-water bounds",
                        file=sys.stderr,
                    )
                    return 2
                scheduler = TransactionalWorkerLoop(
                    worker_name="structure-range",
                    worker=_transactional_structure_worker(
                        control_plane,
                        worker_id=f"{args.worker_id}:structure-range",
                        crash_after_r2_upload=crash_after_r2_upload,
                        retry_fault_before_receipt=retry_fault_before_receipt,
                        acceptance_run_id=args.acceptance_run_id,
                        lane_count=Settings().structure_range_max_concurrency,
                    ),
                    turns_per_tick=args.pool_turns,
                )
            else:
                if args.structure_high_water != 1 or args.quote_high_water != 512:
                    print(
                        "pool roles cannot configure source admission high-water bounds",
                        file=sys.stderr,
                    )
                    return 2
                quote_worker, _quote_certifier = _transactional_quote_workers(
                    control_plane,
                    worker_id=f"{args.worker_id}:quote-batch",
                    crash_after_r2_upload=crash_after_r2_upload,
                    retry_fault_before_receipt=retry_fault_before_receipt,
                    acceptance_run_id=args.acceptance_run_id,
                    lane_count=Settings().clob_batch_max_concurrency,
                )
                scheduler = TransactionalWorkerLoop(
                    worker_name="quote-batch",
                    worker=quote_worker,
                    turns_per_tick=args.pool_turns,
                )
            result = asyncio.run(
                _run_scheduler_service(
                    scheduler,
                    interval_seconds=args.interval_seconds,
                    as_json=args.json,
                )
            )
            _write(result, as_json=args.json)
            return 0
        if args.command == "alert-serve":
            result = asyncio.run(
                _run_alert_service(
                    TransactionalAlertDeliveryWorker(
                        control_plane=control_plane,
                        worker_id=args.worker_id,
                        now=lambda: datetime.now(UTC),
                        acceptance_run_id=args.acceptance_run_id,
                    ),
                    interval_seconds=args.interval_seconds,
                    as_json=args.json,
                )
            )
            _write(result, as_json=args.json)
            return 0
        snapshot = control_plane.operational_snapshot(sample_limit=args.limit)
        _write({"status": "ok", **snapshot}, as_json=args.json)
        return 0
    except RuntimeObserveVerificationError as error:
        print(f"runtime observe verification failed: {error}", file=sys.stderr)
        return 1
    except DatabaseRoleContractError as error:
        if args.command in {"runtime-reconcile-once", "runtime-reconcile-serve"}:
            print(f"runtime reconciliation unavailable: {error}", file=sys.stderr)
        else:
            print(f"control-plane command unavailable: {error}", file=sys.stderr)
        return 1
    except (OSError, RuntimeError, ValueError, psycopg.Error) as error:
        if args.command in {"runtime-reconcile-once", "runtime-reconcile-serve"}:
            detail = _runtime_safe_text(error)
            print(f"runtime reconciliation unavailable: {detail}", file=sys.stderr)
        else:
            print(f"control-plane command unavailable: {type(error).__name__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
