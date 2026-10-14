"""Complete original-method calls through the fixed author IMLane DBEnd API."""
from __future__ import annotations

from dataclasses import dataclass, field, replace
import hashlib
import json
import math
import os
from pathlib import Path
import time

from src.execution_provider.adapters.full_response import encode_full_response
from .native_calls import NativeCall, outcome_kind, raise_execution_error
from .prepared_map import NativeGraphOptions
from .imlane_limits import IMLaneBatchLimits
from src.execution_provider.adapters.sema_imlane import SemaIMLaneExecutor
from src.baselines.common.private_artifacts import write_private_json
from src.experiments.execution_timing import get_timing, timing_span

AUTHOR_COMMIT = "072f8257db80569e42c9f5727d839587df529652"


def _digest(path):
    value = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            value.update(block)
    return value.hexdigest()


def process_start_ticks(pid):
    return int(Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()[19])

@dataclass(frozen=True)
class IMLaneCallConfig:
    runtime: str
    bridge_config_path: str
    coordinator_pid: int
    coordinator_start_ticks: int
    author_build_identity: str
    n_executors: int = 2
    max_batch_rows: int = 128
    max_input_payload_bytes: int = 32 * 1024**2
    max_input_wire_bytes: int = 48 * 1024**2
    max_item_payload_bytes: int = 8 * 1024**2
    max_pending_calls: int = 128
    max_batch_response_bytes: int = 32 * 1024**2
    reuse_bridge: bool = False

    def __post_init__(self):
        self.batch_limits
        if type(self.reuse_bridge) is not bool:
            raise TypeError('IMLane reuse_bridge requires an explicit boolean')
        if self.runtime not in ('cpython', 'ray'):
            raise ValueError('IMLane requires explicit author cpython or ray mode')
        if any(type(v) is not int or v < 1 for v in
               (self.coordinator_pid, self.coordinator_start_ticks, self.n_executors)):
            raise ValueError('explicit author process identity and executor count required')
        for value in (self.bridge_config_path, self.author_build_identity):
            if type(value) is not str or not Path(value).is_absolute():
                raise ValueError('IMLane configuration paths must be absolute')

    @property
    def batch_limits(self):
        return IMLaneBatchLimits(max_batch_rows=self.max_batch_rows,
            max_input_payload_bytes=self.max_input_payload_bytes,
            max_input_wire_bytes=self.max_input_wire_bytes,
            max_item_payload_bytes=self.max_item_payload_bytes,
            max_pending_calls=self.max_pending_calls,
            max_batch_response_bytes=self.max_batch_response_bytes)


def verify_coordinator(config, bridge):
    if process_start_ticks(config.coordinator_pid) != config.coordinator_start_ticks:
        raise ValueError('author Coordinator PID was reused')
    root = Path(f'/proc/{config.coordinator_pid}')
    args = [a.decode() for a in root.joinpath('cmdline').read_bytes().split(b'\0') if a]
    def argument(name):
        if name in args:
            return args[args.index(name) + 1]
        values = [a.split('=', 1)[1] for a in args if a.startswith(name + '=')]
        return values[0] if len(values) == 1 else None
    if (argument('--runtime') != config.runtime
            or argument('--gateway_port') != str(bridge['gateway_port'])
            or argument('--n_executors') != str(config.n_executors)):
        raise ValueError('author mode, gateway port or actual executor count differs')
    env = dict(item.split(b'=', 1) for item in root.joinpath('environ').read_bytes().split(b'\0')
               if b'=' in item)
    if (env.get(b'IMLANE_NAMESPACE', b'').decode() != bridge['namespace']
            or env.get(b'IMLANE_GATEWAY_HOST', b'').decode() != '127.0.0.1'):
        raise ValueError('author namespace or gateway host differs')
    identity = json.loads(Path(config.author_build_identity).read_text())
    if identity.get('upstream_commit') != AUTHOR_COMMIT or identity.get('mode') != 'patched':
        raise ValueError('requires the checked fixed-author patched build')
    sha = identity['binaries_sha256']['bin/imlane']
    if _digest(root/'exe') != sha:
        raise ValueError('running author Coordinator differs from build evidence')
    return dict(runtime=config.runtime, coordinator_pid=config.coordinator_pid,
                coordinator_start_ticks=config.coordinator_start_ticks,
                actual_coordinator_executors=config.n_executors,
                author_commit=AUTHOR_COMMIT, author_binary_sha256=sha)


class IMLaneCallExecutor:
    """Reuse the delivered Sema DBEnd bridge as a complete original-method executor."""
    backend = 'imlane'

    def __init__(self, config, options=None, *, max_batch_rows=None, max_batch_bytes=None,
                 max_pending_calls=None, max_input_wire_bytes=None, max_item_payload_bytes=None,
                 max_batch_response_bytes=None):
        if type(config) is not IMLaneCallConfig:
            raise TypeError('explicit IMLaneCallConfig required')
        self.config = config
        self.options = options or NativeGraphOptions(concurrency=8)
        overrides = dict(max_batch_rows=max_batch_rows, max_input_payload_bytes=max_batch_bytes,
            max_pending_calls=max_pending_calls, max_input_wire_bytes=max_input_wire_bytes,
            max_item_payload_bytes=max_item_payload_bytes, max_batch_response_bytes=max_batch_response_bytes)
        self.limits = replace(config.batch_limits, **{name: value for name, value in overrides.items()
                                                     if value is not None})
        self.max_batch_rows = self.limits.max_batch_rows
        self.max_batch_bytes = self.limits.max_input_payload_bytes
        with timing_span('imlane.delegate_create', backend='imlane', kind=config.runtime):
            self.delegate = SemaIMLaneExecutor(Path(config.bridge_config_path), self.options, limits=self.limits,
                                              reuse_bridge=config.reuse_bridge)
        self.last_close_report = None
        self.last_preflight = None
        self._facts = None
        bridge = self.delegate.config
        self._uncertain = self.delegate.work_root / (
            f'.uncertain-{bridge["namespace"]}-{config.coordinator_pid}-{config.coordinator_start_ticks}.json')

    def start(self):
        with timing_span('imlane.start', backend='imlane', kind=self.config.runtime,
                         scope='verify caller-owned ready Coordinator and bridge prerequisites'):
            if self._uncertain.exists():
                raise RuntimeError('author work is unsettled; explicit owner cleanup required')
            self._facts = verify_coordinator(self.config, self.delegate.config)
            self.delegate.start()
        return self

    def __enter__(self):
        return self.start()

    def __exit__(self, kind, error, traceback):
        try:
            self.close()
        except BaseException as cleanup:
            if error is None:
                raise
            error.add_note('IMLane close error: ' + type(cleanup).__name__)

    def close(self):
        with timing_span('imlane.close', backend='imlane', kind=self.config.runtime,
                         scope='caller bridge close; remote HTTP stop unavailable'):
            self._close()

    def _close(self):
        primary = None
        pending, process = self.delegate.begin_close()
        if pending:
            if self.last_close_report is None:
                self.last_close_report = {}
            self.last_close_report.update(clean=False,
                graph_consumer_stopped=False, author_submission_possible=True,
                author_dispatch_started=True, author_batches_drained=False,
                bridge_started=True, bridge_stopped=False,
                external_coordinator_requires_cleanup=True, remote_http_stopped='unavailable')
            try:
                self._record_uncertain(self.last_close_report)
            except BaseException as error:
                primary = error
        try:
            self.delegate.close()
        except BaseException as cleanup:
            if primary is None:
                raise
            primary.add_note('IMLane child close also failed: ' + type(cleanup).__name__)
        finally:
            if self.last_close_report is not None and self.last_close_report.get('external_coordinator_requires_cleanup'):
                self.last_close_report['bridge_stopped'] = process is None or process.poll() is not None
        if primary is not None:
            raise primary

    def _record_uncertain(self, report):
        # Preserve the first record; a suspended iterator may later attempt the same close.
        if not self._uncertain.exists():
            write_private_json(self._uncertain, report)

    def __call__(self, calls, cancelled=None):
        return self.iter_batch(calls, cancelled=cancelled)

    def iter_batch(self, calls, *, cancelled=None, timeout_s=120):
        with timing_span('imlane.batch', backend='imlane', kind=self.config.runtime,
                         scope='caller bridge batch through result yields; includes consumer yield wait'):
            primary = None
            try:
                yield from self._iter_batch(calls, cancelled=cancelled, timeout_s=timeout_s)
            except BaseException as error:
                primary = error
                raise
            finally:
                try:
                    timing = get_timing()
                    if timing is not None:
                        report = self.last_close_report or {}
                        for stage, elapsed in (report.get('timing_ns') or {}).items():
                            timing.mark('imlane.caller_stage', backend='imlane', kind=self.config.runtime,
                                source_event=stage, elapsed_ns=elapsed,
                                scope='retained caller duration; overlaps batch and consumer waits')
                except BaseException as observation:
                    if primary is None:
                        raise
                    primary.add_note('Detailed IMLane timing also failed: ' + type(observation).__name__)

    def _iter_batch(self, calls, *, cancelled=None, timeout_s=120):
        self.last_close_report = self.last_preflight = None
        self.delegate.last_close_report = None
        if (timeout_s is None and self.config.reuse_bridge or timeout_s is not None and (
                type(timeout_s) not in (int, float) or not math.isfinite(timeout_s) or not 0 < timeout_s <= 120)):
            raise ValueError('IMLane requires a finite session deadline or supervised per-batch completion')
        deadline = None if timeout_s is None else time.monotonic() + timeout_s
        calls = tuple(calls)
        self.last_preflight = self.limits.inspect_calls(calls, include_wire=False)
        report = dict(clean=False, graph_consumer_stopped=True, prepared=len(calls), received=0,
                      runtime=self.config.runtime, author_dispatch_started=False,
                      author_batches_drained=False, remote_http_stopped='unavailable',
                      external_coordinator_stopped=False, external_coordinator_requires_cleanup=False,
                      bridge_started=False, author_submission_possible=False,
                      effective_max_batch_rows=self.max_batch_rows, effective_max_batch_bytes=self.max_batch_bytes,
                      snapshot=self.last_preflight)
        self.last_close_report = report
        if cancelled and cancelled():
            raise InterruptedError('IMLane cancelled before author dispatch')
        if not calls:
            report.update(clean=True, author_batches_drained=True)
            return
        if self._uncertain.exists():
            raise RuntimeError('author work is unsettled; explicit owner cleanup required')
        self._facts = verify_coordinator(self.config, self.delegate.config)
        report.update(self._facts)
        remaining = None if deadline is None else deadline - time.monotonic()
        if remaining is not None and remaining <= 0:
            raise TimeoutError('IMLane preparation used the batch deadline')
        iterator = self.delegate.iter_batch(calls, cancelled=cancelled, timeout_s=remaining)
        primary = None
        received = 0
        total_bytes = 0
        transport_uncertain = False
        try:
            for outcome in iterator:
                outcome_kind(outcome)
                raise_execution_error(outcome)
                transport_uncertain = transport_uncertain or bool(outcome.transport_error)
                size = (len(encode_full_response(outcome.response)) if outcome.response is not None
                        else len(outcome.transport_error.encode()))
                total_bytes += size
                if total_bytes > self.limits.max_batch_response_bytes:
                    raise ValueError('IMLane complete results exceed the requested byte allowance')
                received += 1
                report['received'] = received
                yield outcome
        except BaseException as exc:
            primary = exc
            raise
        finally:
            try:
                iterator.close()
            except BaseException as exc:
                if primary is None:
                    raise
                if hasattr(primary, 'add_note'):
                    primary.add_note('IMLane iterator close also failed: ' + type(exc).__name__)
            finally:
                previous = self.delegate.last_close_report
                if previous is not None:
                    report.update(previous)
                    dispatched = previous.get('author_submission_possible', previous.get('bridge_started')) is True
                    clean = previous.get('clean') is True
                    returned = previous.get('batch_callbacks_returned', clean)
                    report.update(runtime=self.config.runtime, author_dispatch_started=dispatched,
                        clean=clean and not transport_uncertain,
                        transport_completion_uncertain=transport_uncertain,
                        author_submission_possible=dispatched,
                        author_batches_drained=returned,
                        external_coordinator_stopped=False,
                        external_coordinator_requires_cleanup=dispatched and (not clean or transport_uncertain),
                        author_lane_release=('DBEnd returned all batches and sends RestoreLane'
                                             if returned else 'unconfirmed; caller owns cleanup'
                                             if dispatched else 'no bridge process created; no author submission'),
                        native_settings_are_physical_executor_count=False,
                        author_query_job_id='empty in existing bridge; caller occurrence IDs retained',
                        resident_output_limit='existing bridge has no aggregate 32MiB resident-output check')
                    if dispatched and (not clean or transport_uncertain):
                        try:
                            self._record_uncertain(report)
                        except BaseException as exc:
                            if primary is None:
                                raise
                            if hasattr(primary, 'add_note'):
                                primary.add_note('IMLane uncertain-work record failed: ' + type(exc).__name__)
