"""Sema HTTP occurrences executed through the author's caller-owned IMLane service."""
from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path
import re
import selectors
import signal
import socket
import subprocess
import threading
import time
import uuid

from src.baselines.common.private_artifacts import (
    new_private_directory, open_private_text, require_outside_git, write_private_json,
)
from src.baselines.common.redact import redact_text
from src.baselines.text.frameworks.native_calls import (
    NativeCall, _call_wire, _outcome_from_wire, outcome_kind,
)
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.baselines.text.frameworks.imlane_limits import IMLaneBatchLimits
from .full_response import encode_full_response
from .sema_native import SemaNativeService


AUTHOR_COMMIT = '072f8257db80569e42c9f5727d839587df529652'
_RESULT = b'SEMA_IMLANE_RESULT '
_META = b'SEMA_IMLANE_META '
_LIMITS = b'SEMA_IMLANE_LIMITS '
_STATS = b'SEMA_IMLANE_STATS '
_ERROR = b'SEMA_IMLANE_ERROR '
_ENV_NAMES = {'LD_LIBRARY_PATH', 'PATH', 'PYTHONPATH', 'TMPDIR',
              'OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS'}


def _digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


class SemaIMLaneExecutor:
    """A finite DBEnd child; the existing Coordinator owns Lane and backend scheduling.

    Configuration and request evidence remain outside Git. Stopping this child
    does not establish that a model request already sent by IMLane was cancelled.
    """

    backend = 'imlane'

    def __init__(self, config_path: Path, options: NativeGraphOptions | None = None, *, limits=None):
        self.options = options or NativeGraphOptions()
        self.limits = limits or IMLaneBatchLimits()
        if type(self.limits) is not IMLaneBatchLimits:
            raise TypeError('IMLane limits requires IMLaneBatchLimits')
        self.last_preflight = None
        if type(self.options) is not NativeGraphOptions:
            raise ValueError('Sema IMLane requires explicit native execution options')
        self.config_path = Path(config_path).resolve()
        require_outside_git(self.config_path)
        value = json.loads(self.config_path.read_text())
        required = {'author_commit', 'bridge_binary', 'bridge_sha256', 'gateway_port',
                    'udf_path', 'udf_sha256', 'work_root', 'namespace', 'native_task_threads'}
        if type(value) is not dict or not required <= value.keys() or set(value) - required - {'env'}:
            raise ValueError('Sema IMLane configuration requires explicit build and service fields')
        if value['author_commit'] != AUTHOR_COMMIT:
            raise ValueError('Sema IMLane author commit differs from its supported source')
        if (type(value['native_task_threads']) is not int
                or not 1 <= value['native_task_threads'] <= min(128, self.options.concurrency)):
            raise ValueError('Sema IMLane native task threads exceed the declared request allowance')
        if (type(value['gateway_port']) is not int or not 1 <= value['gateway_port'] <= 65535
                or type(value['namespace']) is not str
                or re.fullmatch(r'[A-Za-z0-9_-]{1,64}', value['namespace']) is None):
            raise ValueError('Sema IMLane requires an explicit loopback port and owned namespace')
        for name in ('bridge_binary', 'udf_path', 'work_root'):
            if type(value[name]) is not str or not Path(value[name]).is_absolute():
                raise ValueError('Sema IMLane paths must be absolute')
        for name in ('bridge_sha256', 'udf_sha256'):
            if type(value[name]) is not str or re.fullmatch(r'[0-9a-f]{64}', value[name]) is None:
                raise ValueError('Sema IMLane requires recorded binary and workload digests')
        env = value.get('env', {})
        if (type(env) is not dict or set(env) - _ENV_NAMES
                or any(type(v) is not str or '\0' in v for v in env.values())):
            raise ValueError('Sema IMLane environment contains unsupported entries')
        self.config = value
        self.work_root = Path(value['work_root']).resolve()
        require_outside_git(self.work_root)
        self.last_close_report = None
        self._lock = threading.Lock()
        self._process = None
        self._started = self._closed = False
        self.poisoned = False

    def start(self):
        if self._closed or self.poisoned:
            raise RuntimeError('Sema IMLane executor has closed or retained uncertain work')
        for name, digest in (('bridge_binary', 'bridge_sha256'), ('udf_path', 'udf_sha256')):
            path = Path(self.config[name])
            if not path.is_file() or _digest(path) != self.config[digest]:
                raise ValueError('Sema IMLane built file differs from its recorded identity')
        if not os.access(self.config['bridge_binary'], os.X_OK):
            raise ValueError('Sema IMLane bridge is not executable')
        with socket.create_connection(('127.0.0.1', self.config['gateway_port']), timeout=1):
            pass
        self._started = True
        return self

    def __enter__(self):
        return self.start()

    def __exit__(self, kind, error, traceback):
        try:
            self.close()
        except BaseException as cleanup:
            if error is None:
                raise
            error.add_note('Sema IMLane close error: ' + type(cleanup).__name__)

    @staticmethod
    def _stop(process):
        forced = False
        if process is not None and process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                forced = True
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait(timeout=2)
        return forced

    def close(self):
        with self._lock:
            self._closed = True
            process = self._process
        self._stop(process)

    def __call__(self, calls, cancelled=None):
        return self.iter_batch(calls, cancelled=cancelled)

    def iter_batch(self, calls, *, cancelled=None, timeout_s=120):
        if type(timeout_s) not in (int, float) or not math.isfinite(timeout_s) or not 0 < timeout_s <= 120:
            raise ValueError('Sema IMLane batch requires a finite deadline at most 120 seconds')
        batch_started_ns = time.monotonic_ns()
        deadline = time.monotonic() + timeout_s
        calls = tuple(calls)
        self.last_close_report = None
        self.last_preflight = self.limits.inspect_calls(calls)
        if self.poisoned:
            raise RuntimeError('Sema IMLane executor retained an uncertain previous batch')
        if self._closed or cancelled and cancelled():
            raise InterruptedError('Sema IMLane batch cancelled before submission')
        if not self._started:
            self.start()
        if not calls:
            self.last_close_report = dict(graph_consumer_stopped=True, clean=True,
                                         bridge_started=False, bridge_stopped=True,
                                         prepared=0, received=0, remote_http_stopped='unavailable')
            return
        expected = {call.call_id: call for call in calls}
        received = set()
        total_bytes = 0
        primary = process = None
        clean = forced = False
        native_settings = None
        bridge_limits = bridge_stats = None
        bridge_started_ns = None
        self.last_close_report = None
        out = self.work_root / ('batch-' + uuid.uuid4().hex)
        new_private_directory(out)
        with open_private_text(out / 'input.jsonl') as stream:
            for call in calls:
                stream.write(_call_wire(call) + '\n')
        env = dict(os.environ, **self.config.get('env', {}),
                   IMLANE_NAMESPACE=self.config['namespace'], CUDA_VISIBLE_DEVICES='')
        try:
            with (out / 'input.jsonl').open('rb') as source, open_private_text(out / 'bridge.log') as log:
                with self._lock:
                    if self._process is not None or self._closed:
                        raise RuntimeError('Sema IMLane requires one live snapshot owner')
                    if time.monotonic() >= deadline:
                        raise TimeoutError('Sema IMLane preparation used the batch deadline')
                    command = [self.config['bridge_binary'],
                        str(self.config['gateway_port']), self.config['udf_path'],
                        str(self.config['native_task_threads'])]
                    if not self.limits.legacy:
                        command += self.limits.cpp_arguments()
                    process = subprocess.Popen(command,
                        stdin=source, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                        env=env, cwd=out, start_new_session=True)
                    bridge_started_ns = time.monotonic_ns()
                    self._process = process
                os.set_blocking(process.stdout.fileno(), False)
                with selectors.DefaultSelector() as selector:
                    selector.register(process.stdout, selectors.EVENT_READ)
                    buffer = bytearray()
                    eof = False
                    while not eof:
                        if self._closed or cancelled and cancelled():
                            raise InterruptedError('Sema IMLane batch cancelled')
                        if time.monotonic() >= deadline:
                            raise TimeoutError('Sema IMLane batch deadline exceeded')
                        if not selector.select(min(.02, max(0, deadline - time.monotonic()))):
                            continue
                        chunk = os.read(process.stdout.fileno(), 65536)
                        if not chunk:
                            eof = True
                        buffer.extend(chunk)
                        if len(buffer) > 12 * 1024**2:
                            raise ValueError('Sema IMLane response line exceeds its allowance')
                        while b'\n' in buffer:
                            line, _, rest = buffer.partition(b'\n')
                            buffer = bytearray(rest)
                            if line.startswith(_RESULT):
                                if native_settings is None:
                                    raise ValueError('Sema IMLane result preceded native settings')
                                if not self.limits.legacy and bridge_limits is None:
                                    raise ValueError('IMLane configured bridge did not confirm its capacity')
                                outcome = _outcome_from_wire(line[len(_RESULT):])
                                call = expected.get(outcome.call_id)
                                if call is None or call.row_id != outcome.row_id or outcome.call_id in received:
                                    raise ValueError('Sema IMLane result changed occurrence identity')
                                outcome_kind(outcome)
                                size = (len(encode_full_response(outcome.response)) if outcome.response is not None
                                        else len((outcome.transport_error or outcome.execution_error).encode()))
                                total_bytes += size
                                if size > call.max_response_bytes or total_bytes > self.limits.max_batch_response_bytes:
                                    raise ValueError('Sema IMLane results exceed their byte allowance')
                                received.add(outcome.call_id)
                                yield outcome
                            elif line.startswith(_META):
                                native_settings = json.loads(line[len(_META):])
                                if (type(native_settings) is not dict or set(native_settings) !=
                                        {'native_executors', 'native_task_threads'}
                                        or any(type(v) is not int or not 1 <= v <= 1024
                                               for v in native_settings.values())
                                        or native_settings['native_task_threads'] != self.config['native_task_threads']):
                                    raise ValueError('Sema IMLane native settings are invalid')
                            elif line.startswith(_LIMITS):
                                bridge_limits = json.loads(line[len(_LIMITS):])
                                from dataclasses import asdict
                                if bridge_limits != asdict(self.limits):
                                    raise ValueError('IMLane C++ capacity differs from the selected Python profile')
                            elif line.startswith(_STATS):
                                bridge_stats = json.loads(line[len(_STATS):])
                            elif line.startswith(_ERROR):
                                failure = json.loads(line[len(_ERROR):])
                                if failure.get('error_type') != 'ValueError' or type(failure.get('message')) is not str:
                                    raise ValueError('IMLane bridge error record is invalid')
                                raise ValueError(redact_text(failure['message'])[:4096])
                            else:
                                log.write(redact_text(line.decode(errors='replace')) + '\n')
                        if eof and buffer:
                            raise ValueError('Sema IMLane bridge returned an incomplete output line')
                while process.poll() is None:
                    if self._closed or cancelled and cancelled():
                        raise InterruptedError('Sema IMLane batch cancelled while bridge was closing')
                    if time.monotonic() >= deadline:
                        raise TimeoutError('Sema IMLane bridge exit deadline exceeded')
                    time.sleep(.01)
                if process.returncode != 0 or len(received) != len(calls):
                    raise RuntimeError('Sema IMLane bridge did not complete every submitted occurrence')
                if native_settings is None:
                    raise ValueError('Sema IMLane bridge did not report actual native settings')
                if not self.limits.legacy and (bridge_stats is None
                        or bridge_stats.get('submitted') != len(calls) or bridge_stats.get('delivered') != len(calls)
                        or type(bridge_stats.get('pending_peak')) is not int
                        or not 0 <= bridge_stats['pending_peak'] <= self.limits.max_pending_calls):
                    raise ValueError('IMLane C++ did not confirm complete finite-window consumption')
                clean = True
        except BaseException as error:
            primary = error
            raise
        finally:
            stop_requested_ns = time.monotonic_ns()
            try:
                forced = self._stop(process)
                if process is not None:
                    process.stdout.close()
            except BaseException as cleanup:
                clean = False
                if primary is None:
                    raise
                primary.add_note('Sema IMLane child cleanup error: ' + type(cleanup).__name__)
            finally:
                stopped = process is None or process.poll() is not None
                with self._lock:
                    if self._process is process and stopped:
                        self._process = None
                self.last_close_report = dict(graph_consumer_stopped=stopped, bridge_stopped=stopped,
                    bridge_started=process is not None, author_submission_possible=process is not None,
                    clean=clean, forced=forced, prepared=len(calls), received=len(received),
                    returncode=None if process is None else process.returncode,
                    remote_http_stopped='unavailable', coordinator_owner='caller',
                    scheduler_owner='IMLane author implementation',
                    batch_started_monotonic_ns=batch_started_ns,
                    bridge_started_monotonic_ns=bridge_started_ns,
                    bridge_stop_requested_monotonic_ns=stop_requested_ns,
                    bridge_stopped_monotonic_ns=time.monotonic_ns() if process is not None and stopped else None,
                    native_settings_are_physical_executor_count=False)
                self.last_close_report['native_settings'] = native_settings
                self.last_close_report.update(snapshot=self.last_preflight, bridge_limits=bridge_limits,
                                              bridge_stats=bridge_stats)
                self.last_close_report['concurrency_allowance'] = self.options.concurrency
                self.poisoned = self.poisoned or (process is not None and not clean)
                self.last_close_report['executor_poisoned'] = self.poisoned
                try:
                    write_private_json(out / 'close.json', self.last_close_report)
                except BaseException as cleanup:
                    if primary is None:
                        raise
                    primary.add_note('Sema IMLane close record error: ' + type(cleanup).__name__)


class SemaIMLaneService(SemaNativeService):
    """The shared Sema source/sink with explicit IMLane capacity reporting."""

    def __init__(self, *, executor, **service_options):
        if (getattr(executor, 'backend', None) != 'imlane'
                or not callable(getattr(executor, 'iter_batch', None))):
            raise ValueError('Sema IMLane service requires an explicit complete-call IMLane executor')
        super().__init__(backend='imlane', executor=executor, **service_options)

    @property
    def summary(self):
        value = super().summary
        settings = [r['native_settings'] for r in value['native_close_reports']
                    if type(r.get('native_settings')) is dict]
        value['upstream_capacity_owner'] = 'IMLane author TaskThreadPool and Lane settings'
        value['upstream_concurrency'] = None if not settings else settings[-1]['native_task_threads']
        value['native_settings_observed'] = bool(settings)
        value['concurrency_allowance'] = self.executor.options.concurrency
        modes = {r.get('runtime') for r in value['native_close_reports']
                 if r.get('runtime') in ('cpython', 'ray')}
        value['imlane_runtime'] = next(iter(modes)) if len(modes) == 1 else None
        value['imlane_runtime_verified'] = value['imlane_runtime'] is not None and any(
            r.get('runtime') == value['imlane_runtime'] and r.get('author_commit') == AUTHOR_COMMIT
            for r in value['native_close_reports'])
        value['native_settings_are_physical_executor_count'] = False
        executors = [r['actual_coordinator_executors'] for r in value['native_close_reports']
                     if type(r.get('actual_coordinator_executors')) is int]
        value['actual_coordinator_executors'] = executors[-1] if executors else None
        return value


def open_sema_imlane_service(upstream_url, *, executor, **service_options):
    return SemaIMLaneService(upstream_url=upstream_url, executor=executor, **service_options)
