"""Birth-matched owned-process observation, signals and bounded exit waits.

These helpers do not launch workers, choose a query policy or confirm GPU exit.
The records supervisor retains its existing helper imports for current callers;
new process transports can import this module without experiment orchestration.
"""

import hashlib
import math
import os
import signal
import time

import psutil

from src.infrastructure.redact import redact_argument_list, redact_text


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
