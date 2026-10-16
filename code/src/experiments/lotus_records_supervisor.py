"""Bound one dedicated records CLI and its observed owned processes, without a model import."""

import math
import os
from pathlib import Path
import signal
import subprocess
import time

import psutil

from src.baselines.common.private_artifacts import (
    new_private_directory, open_private_text, require_outside_git, write_private_json,
)
from src.baselines.common.redact import redact_argument_list, redact_json_values, redact_text


def _seconds(value, name):
    if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
        raise ValueError(name + " must be a finite positive number; bool is not accepted")


def _live(owned):
    live = []
    for pid, created in owned:
        try:
            process = psutil.Process(pid)
            if process.create_time() == created and process.status() not in (
                psutil.STATUS_ZOMBIE, psutil.STATUS_DEAD,
            ):
                live.append(process)
        except psutil.NoSuchProcess:
            pass
    return live


def _observe(owned, pgid, created):
    # A reused leader PID can create a different group with the same integer ID.
    try:
        if psutil.Process(pgid).create_time() != created:
            raise RuntimeError("owned process-group identity was reused")
    except psutil.NoSuchProcess:
        pass
    for process in list(_live(owned)):
        try:
            children = process.children(recursive=True)
        except psutil.NoSuchProcess:
            # It can exit after _live's identity/status read. Continue scanning
            # the owned group so a lost parent does not hide surviving members.
            continue
        for child in children:
            try:
                owned.add((child.pid, child.create_time()))
            except psutil.NoSuchProcess:
                pass
    # The leader can exit between observations. Its surviving group still belongs
    # to the session created by this Popen; never inspect a different PGID as owned.
    for process in psutil.process_iter(["pid", "create_time"]):
        try:
            born = process.info["create_time"]
            if (type(born) in (int, float) and born >= created
                    and os.getpgid(process.pid) == pgid):
                owned.add((process.pid, born))
        except (psutil.NoSuchProcess, psutil.AccessDenied, ProcessLookupError, PermissionError):
            pass


def _signal(owned, pgid, created, value):
    live = _live(owned)
    try:
        leader = psutil.Process(pgid)
        group_valid = leader.create_time() == created
    except psutil.NoSuchProcess:
        group_valid = True
    grouped = []
    for process in live:
        try:
            if group_valid and pgid != os.getpgrp() and os.getpgid(process.pid) == pgid:
                grouped.append(process.pid)
            else:
                process.send_signal(value)
        except (psutil.NoSuchProcess, ProcessLookupError):
            pass
    if grouped:
        try:
            os.killpg(pgid, value)
        except ProcessLookupError:
            pass
    if not group_valid:
        raise RuntimeError("process group was reused; only matched observed processes were signalled")


def _wait(owned, pgid, created, deadline):
    while _live(owned) and time.monotonic() < deadline:
        _observe(owned, pgid, created)
        time.sleep(min(0.02, max(0.0, deadline - time.monotonic())))


def supervise(command: list[str], output_dir: Path, *, timeout_s: float,
              grace_s: float = 5) -> dict:
    """Run a worker within one wall budget, reserving a bounded cleanup window.

    The report confirms only observed OS process exits. It cannot establish GPU
    idleness, a clean model shutdown, or the exit of unobserved detached children.
    """
    _seconds(timeout_s, "timeout_s")
    _seconds(grace_s, "grace_s")
    if (type(command) is not list or not command or not command[0]
            or any(type(value) is not str or "\0" in value for value in command)):
        raise ValueError("command must be a nonempty argument list, without NUL bytes")
    output_dir = Path(output_dir)
    require_outside_git(output_dir)
    if not output_dir.exists():
        new_private_directory(output_dir)
    if not output_dir.is_dir():
        raise ValueError("output_dir must be a private artifact directory")
    started = time.monotonic()
    deadline = started + timeout_s
    reserved = min(grace_s, timeout_s / 3)
    worker_deadline = deadline - reserved
    owned, process, created, failure = set(), None, None, None
    report = dict(
        schema="semloom.lotus_records.supervisor.v1", status="failed",
        command=redact_argument_list(command), timeout_s=timeout_s, grace_s=grace_s,
        cleanup_reserved_s=reserved, started_monotonic=started,
        overall_deadline_monotonic=deadline, worker_deadline_monotonic=worker_deadline,
        physical_gpu_exit_checked=False, gpu_exit_confirmed=None,
        process_exit_scope="observed_owned_processes", unobserved_descendants_confirmed=False,
        signals=[], cleanup_errors=[], remaining_owned_pids=[], worker_exit=None,
    )

    def error_record(error):
        return redact_text(type(error).__name__ + ": " + str(error))

    def cleanup_attempt(action, default=None):
        try:
            return action()
        except BaseException as error:
            report["cleanup_errors"].append(error_record(error))
            return default

    try:
        with open_private_text(output_dir / "worker.log") as log:
            try:
                process = subprocess.Popen(
                    command, stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                    start_new_session=True, shell=False,
                )
                created = psutil.Process(process.pid).create_time()
                owned.add((process.pid, created))
                report.update(owned_pid=process.pid, owned_create_time=created, owned_pgid=process.pid)
                while True:
                    _observe(owned, process.pid, created)
                    if time.monotonic() >= worker_deadline:
                        raise TimeoutError("records worker exceeded its overall work window")
                    if process.poll() is not None:
                        if process.returncode:
                            raise RuntimeError("records worker failed; see private worker.log")
                        break
                    time.sleep(min(0.02, max(0.0, worker_deadline - time.monotonic())))
            except BaseException as error:
                failure = error
            finally:
                if process is not None:
                    cleanup_deadline = min(deadline, time.monotonic() + reserved)
                    try:
                        if created is None:
                            raise RuntimeError("spawned worker creation identity is unavailable")
                        cleanup_attempt(lambda: _observe(owned, process.pid, created))
                        if cleanup_attempt(lambda: _live(owned), default=True):
                            if failure is None:
                                failure = RuntimeError("records worker left live owned processes")
                            for value, until in (
                                (signal.SIGTERM, (time.monotonic() + cleanup_deadline) / 2),
                                (signal.SIGKILL, cleanup_deadline),
                            ):
                                if not cleanup_attempt(lambda: _live(owned), default=True):
                                    break
                                report["signals"].append(signal.Signals(value).name)
                                cleanup_attempt(lambda: _signal(owned, process.pid, created, value))
                                cleanup_attempt(lambda: _wait(owned, process.pid, created, until))
                        cleanup_attempt(lambda: _observe(owned, process.pid, created))
                    except BaseException as error:
                        report["cleanup_errors"].append(error_record(error))
                    finally:
                        # Reap the exact Popen child even when observation itself failed.
                        if process.poll() is None:
                            try:
                                process.kill()
                                process.wait(timeout=max(0.0, deadline - time.monotonic()))
                            except BaseException as error:
                                report["cleanup_errors"].append(error_record(error))
                        report["worker_exit"] = process.poll()
    except BaseException as error:
        if failure is None:
            failure = error
        else:
            report["cleanup_errors"].append(error_record(error))
    try:
        report["remaining_owned_pids"] = [item.pid for item in _live(owned)]
    except BaseException as error:
        report["cleanup_errors"].append(error_record(error))
    report["observed_owned_exit_confirmed"] = not (
        report["remaining_owned_pids"] or report["cleanup_errors"]
    )
    report["status"] = ("unknown" if not report["observed_owned_exit_confirmed"]
                        else "failed" if failure else "passed")
    report.update(ended_monotonic=time.monotonic(),
                  observed_owned=[dict(pid=pid, create_time=born) for pid, born in sorted(owned)])
    report["elapsed_s"] = report["ended_monotonic"] - started
    if failure is not None:
        report["error"] = error_record(failure)
    if report["status"] == "unknown" and failure is None:
        failure = RuntimeError("owned worker cleanup outcome is unknown")
    report = redact_json_values(report)
    try:
        write_private_json(output_dir / "supervisor.json", report)
    except BaseException as error:
        if failure is None:
            failure = error
        report.update(status="unknown", report_write_error=error_record(error))
    if failure is not None:
        failure.report = report
        raise failure
    return report
