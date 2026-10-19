"""Bound one dedicated records CLI and its observed owned processes, without a model import."""

import hashlib
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


_MAX_LOG_LINE_BYTES = 16 * 1024


class _RedactedOutput:
    """Bound pipe reads and whole-line buffering before any output reaches disk."""

    def __init__(self, pipe, log):
        self.pipe, self.log = pipe, log
        self.pending = bytearray()
        self.discarding = False
        self.eof = False
        self.bytes_read = self.omitted_lines = self.omitted_bytes = self.max_buffer = 0
        os.set_blocking(pipe.fileno(), False)

    def _consume(self, data):
        pieces = data.split(b"\n")
        for index, piece in enumerate(pieces):
            newline = index < len(pieces) - 1
            if self.discarding:
                self.omitted_bytes += len(piece) + int(newline)
            elif len(self.pending) + len(piece) > _MAX_LOG_LINE_BYTES:
                self.omitted_lines += 1
                self.omitted_bytes += len(self.pending) + len(piece) + int(newline)
                self.pending.clear()
                self.discarding = True
                self.log.write("[worker output line exceeding 16384 bytes omitted]\n")
            else:
                self.pending.extend(piece)
                self.max_buffer = max(self.max_buffer, len(self.pending))
                if newline:
                    self.log.write(redact_text(self.pending.decode("utf-8", errors="replace")) + "\n")
                    self.pending.clear()
            if newline:
                self.discarding = False

    def pump(self, deadline):
        # Never drain until quiet: a writer may run forever or an unknown child
        # may keep the FD open. Each observation gets at most 64 KiB of reads.
        for _ in range(8):
            if self.eof or time.monotonic() >= deadline:
                break
            try:
                data = os.read(self.pipe.fileno(), 8192)
            except BlockingIOError:
                break
            if not data:
                self.eof = True
                break
            self.bytes_read += len(data)
            self._consume(data)

    def finish(self):
        try:
            if self.pending:
                self.log.write(redact_text(self.pending.decode("utf-8", errors="replace")))
                self.pending.clear()
        finally:
            self.pipe.close()

    def report(self):
        return dict(
            redacted_before_write=True, max_line_bytes=_MAX_LOG_LINE_BYTES,
            max_buffer_bytes=self.max_buffer, bytes_read=self.bytes_read,
            omitted_long_lines=self.omitted_lines, omitted_bytes=self.omitted_bytes,
            eof_observed=self.eof, pipe_closed=self.pipe.closed,
            complete=self.eof and self.omitted_lines == 0,
        )


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
    errors, events = [], []
    try:
        if psutil.Process(pgid).create_time() != created:
            errors.append(RuntimeError("process group was reused; only matched observed processes were signalled"))
    except psutil.NoSuchProcess:
        pass
    except BaseException as error:
        errors.append(error)
    for process in live:
        event = dict(pid=process.pid, create_time=None, signal=signal.Signals(value).name,
                     requested_monotonic=time.monotonic(), requested_epoch_s=time.time())
        try:
            event.update(_exit_wait_identity(process))
            # Optional diagnostics never gate a literal birth-matched signal.
            # No group signal can reach members absent from the owned snapshot.
            process.send_signal(value)
            event["outcome"] = "sent"
        except (psutil.NoSuchProcess, ProcessLookupError):
            event["outcome"] = "exited_before_signal"
        except BaseException as error:
            event.update(outcome="unconfirmed", error=redact_text(type(error).__name__ + ": " + str(error)))
            errors.append(error)
        events.append(event)
    if errors:
        errors[0].signal_events = events
        raise errors[0]
    return events


def _exit_wait_identity(process):
    identity = dict(pid=process.pid, create_time=process.create_time())
    try:
        command = process.cmdline()
        identity.update(command=redact_argument_list(command), name=process.name(),
            cmdline_sha256=hashlib.sha256(" ".join(command).encode()).hexdigest())
    except (psutil.NoSuchProcess, ProcessLookupError):
        identity["command_unavailable"] = "exited before wait metadata capture"
    except Exception as error:
        identity["metadata_error"] = redact_text(type(error).__name__ + ": " + str(error))
    return identity


def _wait(owned, pgid, created, deadline, output=None, *, exit_events=None,
          phase=None, watch=()):
    watched = set(watch)
    recorded = set()
    while True:
        live = _live(owned)
        identities = {(process.pid, process.create_time()) for process in live}
        watched.update(identities)
        if exit_events is not None:
            for pid, born in sorted(watched - identities - recorded):
                exit_events.append(dict(pid=pid, create_time=born, phase=phase,
                    observed_monotonic=time.monotonic(), observed_epoch_s=time.time(),
                    time_scope="first observed not alive in this wait"))
                recorded.add((pid, born))
        if not live or time.monotonic() >= deadline:
            break
        if output is not None:
            output.pump(deadline)
        _observe(owned, pgid, created)
        time.sleep(min(0.02, max(0.0, deadline - time.monotonic())))


def supervise(command: list[str], output_dir: Path, *, timeout_s: float,
              grace_s: float = 5, natural_exit_grace_s: float = 0) -> dict:
    """Run a worker within one wall budget, reserving a bounded cleanup window.

    The report confirms only observed OS process exits. It cannot establish GPU
    idleness, a clean model shutdown, or the exit of unobserved detached children.
    """
    _seconds(timeout_s, "timeout_s")
    _seconds(grace_s, "grace_s")
    if (type(natural_exit_grace_s) not in (int, float)
            or not math.isfinite(natural_exit_grace_s) or natural_exit_grace_s < 0):
        raise ValueError("natural_exit_grace_s must be finite and nonnegative; bool is not accepted")
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
    owned, process, created, failure, output = set(), None, None, None, None
    report = dict(
        schema="semloom.lotus_records.supervisor.v1", status="failed",
        command=redact_argument_list(command), timeout_s=timeout_s, grace_s=grace_s,
        cleanup_reserved_s=reserved, started_monotonic=started,
        overall_deadline_monotonic=deadline, worker_deadline_monotonic=worker_deadline,
        physical_gpu_exit_checked=False, gpu_exit_confirmed=None,
        process_exit_scope="observed_owned_processes", unobserved_descendants_confirmed=False,
        signals=[], signal_events=[], exit_observations=[], natural_exit_wait=None,
        natural_exit_grace_requested_s=natural_exit_grace_s,
        cleanup_errors=[], remaining_owned_pids=[], worker_exit=None,
        output_capture=None,
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
                    command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    stdin=subprocess.DEVNULL, start_new_session=True, shell=False, bufsize=0,
                )
                created = psutil.Process(process.pid).create_time()
                owned.add((process.pid, created))
                report.update(owned_pid=process.pid, owned_create_time=created, owned_pgid=process.pid)
                output = _RedactedOutput(process.stdout, log)
                while True:
                    output.pump(deadline)
                    _observe(owned, process.pid, created)
                    if time.monotonic() >= worker_deadline:
                        raise TimeoutError("records worker exceeded its overall work window")
                    if process.poll() is not None:
                        report.update(worker_exit_observed_monotonic=time.monotonic(),
                                      worker_exit_observed_epoch_s=time.time(),
                                      worker_exit_observation_scope="worker-loop poll")
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
                        # Opt-in natural teardown uses at most half of the
                        # already-reserved cleanup window; total deadlines stay
                        # unchanged and forced cleanup still means failure.
                        if failure is None and process.poll() == 0 and natural_exit_grace_s:
                            before = cleanup_attempt(lambda: _live(owned), default=None)
                            if before is not None:
                                watch = {(p.pid, p.create_time()) for p in before}
                                start = time.monotonic()
                                until = min(cleanup_deadline, start + min(
                                    natural_exit_grace_s, reserved / 2))
                                report["natural_exit_wait"] = dict(
                                    started_monotonic=start, started_epoch_s=time.time(),
                                    deadline_monotonic=until, initial_live=[
                                        _exit_wait_identity(p) for p in before])
                                cleanup_attempt(lambda: _wait(owned, process.pid, created, until, output,
                                    exit_events=report["exit_observations"], phase="natural_exit", watch=watch))
                                after = cleanup_attempt(lambda: _live(owned), default=None)
                                report["natural_exit_wait"].update(
                                    ended_monotonic=time.monotonic(), ended_epoch_s=time.time(),
                                    remaining=None if after is None else [dict(
                                        pid=p.pid, create_time=p.create_time()) for p in after])
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
                                def send():
                                    try:
                                        events = _signal(owned, process.pid, created, value)
                                    except BaseException as error:
                                        report["signal_events"].extend(getattr(error, "signal_events", []))
                                        raise
                                    report["signal_events"].extend(events or [])
                                cleanup_attempt(send)
                                watch = {(event["pid"], event["create_time"]) for event in
                                         report["signal_events"] if event["signal"] == signal.Signals(value).name
                                         and type(event["create_time"]) in (int, float)}
                                cleanup_attempt(lambda: _wait(owned, process.pid, created, until, output,
                                    exit_events=report["exit_observations"], phase=signal.Signals(value).name,
                                    watch=watch))
                        cleanup_attempt(lambda: _observe(owned, process.pid, created))
                    except BaseException as error:
                        report["cleanup_errors"].append(error_record(error))
                    finally:
                        # Reap the exact Popen child even when observation itself failed.
                        if process.poll() is None:
                            event = dict(pid=process.pid, create_time=created, signal="SIGKILL",
                                command=redact_argument_list(command),
                                cmdline_sha256=hashlib.sha256(" ".join(command).encode()).hexdigest(),
                                identity_scope="unreaped Popen child", requested_monotonic=time.monotonic(),
                                requested_epoch_s=time.time(), outcome="unconfirmed")
                            report["signals"].append("SIGKILL")
                            try:
                                process.kill()
                                event["outcome"] = "sent"
                                process.wait(timeout=max(0.0, deadline - time.monotonic()))
                            except BaseException as error:
                                report["cleanup_errors"].append(error_record(error))
                            finally:
                                report["signal_events"].append(event)
                        report["worker_exit"] = process.poll()
                        if report["worker_exit"] is not None and "worker_exit_observed_monotonic" not in report:
                            report.update(worker_exit_observed_monotonic=time.monotonic(),
                                worker_exit_observed_epoch_s=time.time(),
                                worker_exit_observation_scope="poll after cleanup")
                if output is not None:
                    cleanup_attempt(lambda: output.pump(deadline))
                    cleanup_attempt(output.finish)
                elif process is not None and getattr(process, "stdout", None) is not None:
                    cleanup_attempt(process.stdout.close)
    except BaseException as error:
        if failure is None:
            failure = error
        else:
            report["cleanup_errors"].append(error_record(error))
    finally:
        if output is not None:
            if not output.pipe.closed:
                cleanup_attempt(output.finish)
            report["output_capture"] = output.report()
        elif process is not None and getattr(process, "stdout", None) is not None:
            cleanup_attempt(process.stdout.close)
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
