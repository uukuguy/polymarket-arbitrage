import os
import signal
from pathlib import Path

import pytest

from polyarb.perception.chaos_primitive import (
    PrimitiveRefusedError,
    locate_worker,
    terminate_worker,
)


def _process(
    proc_root: Path,
    pid: int,
    *,
    ppid: int,
    argv: tuple[str, ...],
) -> None:
    process = proc_root / str(pid)
    process.mkdir(parents=True)
    (process / "cmdline").write_bytes(b"\0".join(item.encode() for item in argv) + b"\0")
    (process / "stat").write_text(f"{pid} (python worker) S {ppid} 0 0 0\n")


def _valid_proc(proc_root: Path) -> None:
    _process(
        proc_root,
        1,
        ppid=0,
        argv=("python", "-m", "polyarb.daemon.main"),
    )
    _process(
        proc_root,
        41,
        ppid=1,
        argv=("python", "-m", "polyarb.perception.worker_cli", "candidate"),
    )


def test_locate_worker_requires_one_exact_direct_daemon_child(tmp_path: Path) -> None:
    _valid_proc(tmp_path)

    worker = locate_worker(tmp_path, "candidate")

    assert worker.pid == 41
    assert worker.ppid == 1
    assert worker.component == "candidate"


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        ("second", "worker-count"),
        ("wrong-parent", "worker-parent"),
        ("wrong-daemon", "daemon-command"),
        ("extra-arg", "worker-count"),
    ],
)
def test_locate_worker_fails_closed_on_ambiguous_or_drifted_proc(
    tmp_path: Path,
    mutation: str,
    reason: str,
) -> None:
    _valid_proc(tmp_path)
    if mutation == "second":
        _process(
            tmp_path,
            42,
            ppid=1,
            argv=("python", "-m", "polyarb.perception.worker_cli", "candidate"),
        )
    elif mutation == "wrong-parent":
        (tmp_path / "41/stat").write_text("41 (python worker) S 7 0 0 0\n")
    elif mutation == "wrong-daemon":
        (tmp_path / "1/cmdline").write_bytes(b"python\0-m\0other.main\0")
    else:
        (tmp_path / "41/cmdline").write_bytes(
            b"python\0-m\0polyarb.perception.worker_cli\0candidate\0--other\0"
        )

    with pytest.raises(PrimitiveRefusedError, match=reason):
        locate_worker(tmp_path, "candidate")


def test_terminate_worker_binds_release_fault_and_exact_pid(tmp_path: Path) -> None:
    _valid_proc(tmp_path)
    killed: list[tuple[int, signal.Signals]] = []
    release = "a" * 40

    worker = terminate_worker(
        tmp_path,
        component="candidate",
        expected_pid=41,
        expected_release=release,
        authorization=f"fault:candidate-exit:{release}:41",
        environ={"POLYARB_RELEASE_ID": release},
        kill=lambda pid, sig: killed.append((pid, sig)),
    )

    assert worker.pid == 41
    assert killed == [(41, signal.SIGTERM)]


@pytest.mark.parametrize(
    ("expected_pid", "expected_release", "authorization", "runtime_release", "reason"),
    [
        (42, "a" * 40, f"fault:candidate-exit:{'a' * 40}:42", "a" * 40, "pid-drift"),
        (41, "b" * 40, f"fault:candidate-exit:{'b' * 40}:41", "a" * 40, "release-drift"),
        (41, "a" * 40, "wrong", "a" * 40, "authorization"),
        (41, "short", "wrong", "short", "release"),
    ],
)
def test_terminate_worker_refuses_before_signal(
    tmp_path: Path,
    expected_pid: int,
    expected_release: str,
    authorization: str,
    runtime_release: str,
    reason: str,
) -> None:
    _valid_proc(tmp_path)
    killed: list[tuple[int, signal.Signals]] = []

    with pytest.raises(PrimitiveRefusedError, match=reason):
        terminate_worker(
            tmp_path,
            component="candidate",
            expected_pid=expected_pid,
            expected_release=expected_release,
            authorization=authorization,
            environ={"POLYARB_RELEASE_ID": runtime_release},
            kill=lambda pid, sig: killed.append((pid, sig)),
        )

    assert killed == []


def test_terminate_worker_rejects_unsupported_component_before_signal(
    tmp_path: Path,
) -> None:
    _valid_proc(tmp_path)
    killed: list[tuple[int, signal.Signals]] = []

    with pytest.raises(PrimitiveRefusedError, match="component"):
        terminate_worker(
            tmp_path,
            component="http",
            expected_pid=1,
            expected_release="a" * 40,
            authorization=f"fault:http-exit:{'a' * 40}:1",
            environ=os.environ,
            kill=lambda pid, sig: killed.append((pid, sig)),
        )

    assert killed == []
