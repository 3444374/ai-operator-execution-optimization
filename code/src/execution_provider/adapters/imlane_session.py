"""Opt-in serial batches on one fixed-function DBEnd child, with explicit end records."""
from dataclasses import asdict
import json
import math
import os
import select
import selectors
import subprocess
import threading
import time
import uuid

from src.baselines.common.private_artifacts import (
    new_private_directory, open_private_text, write_private_json,
)
from src.baselines.common.redact import redact_text
from src.baselines.text.frameworks.native_calls import _call_wire, _outcome_from_wire, outcome_kind
from .full_response import encode_full_response


_PREFIX = b'SEMA_IMLANE_'
_LINE_MAX = 12 * 1024**2
_LOG_MAX = 1024**2
_BATCH_MAX = 4096
_BRIDGE_TIMINGS = {'input_read_parse_ns', 'input_arrow_ns', 'context_setup_ns', 'function_register_ns',
                   'author_execute_inclusive_ns', 'output_copy_encode_ns', 'output_copy_decode_ns',
                   'output_wire_ns', 'stdout_write_ns', 'batch_wall_ns'}


class IMLaneBridgeSession:
    """Own only the caller bridge; author concurrency and remote cleanup remain external."""

    def __init__(self, owner):
        self.owner = owner
        self.process = None
        self.root = None
        self.settings = None
        self.buffer = bytearray()
        self.batches = 0
        self.log_bytes = 0
        self.command_active = False
        self._busy = threading.Lock()

    def _check(self, deadline, cancelled=None, *, closing=False):
        if not closing and (self.owner._closed or cancelled and cancelled()):
            raise InterruptedError('IMLane session batch cancelled')
        if time.monotonic() >= deadline:
            raise TimeoutError('IMLane session deadline exceeded')

    def _send(self, value, deadline, cancelled=None, *, closing=False):
        wire = (json.dumps(value, separators=(',', ':')) + '\n').encode()
        if len(wire) > 1024:
            raise ValueError('IMLane session command exceeds its byte allowance')
        offset = 0
        while offset < len(wire):
            self._check(deadline, cancelled, closing=closing)
            try:
                offset += os.write(self.process.stdin.fileno(), wire[offset:])
            except BlockingIOError:
                select.select([], [self.process.stdin], [], min(.02, deadline - time.monotonic()))

    def _line(self, deadline, cancelled=None, *, closing=False):
        with selectors.DefaultSelector() as selector:
            selector.register(self.process.stdout, selectors.EVENT_READ)
            while True:
                self._check(deadline, cancelled, closing=closing)
                if b'\n' in self.buffer:
                    line, _, rest = self.buffer.partition(b'\n')
                    self.buffer = bytearray(rest)
                    return bytes(line)
                if not selector.select(min(.02, max(0, deadline - time.monotonic()))):
                    continue
                chunk = os.read(self.process.stdout.fileno(), 65536)
                if not chunk:
                    raise RuntimeError('IMLane session bridge closed before its end record')
                self.buffer.extend(chunk)
                if len(self.buffer) > _LINE_MAX:
                    raise ValueError('IMLane session output line exceeds its byte allowance')

    def _log(self, line, log):
        if self.log_bytes < _LOG_MAX:
            text = redact_text(line.decode(errors='replace')) + '\n'
            text = text.encode()[:_LOG_MAX - self.log_bytes].decode(errors='ignore')
            log.write(text)
            self.log_bytes += len(text.encode())

    def _record(self, deadline, log, cancelled=None, *, closing=False):
        while True:
            line = self._line(deadline, cancelled, closing=closing)
            if line.startswith(_PREFIX):
                tag, separator, wire = line[len(_PREFIX):].partition(b' ')
                if not separator:
                    raise ValueError('IMLane session record has no body')
                value = json.loads(wire)
                if type(value) is not dict:
                    raise ValueError('IMLane session record is not an object')
                if tag == b'ERROR':
                    if value.get('error_type') != 'ValueError' or type(value.get('message')) is not str:
                        raise ValueError('IMLane session error record is invalid')
                    raise ValueError(redact_text(value['message'])[:4096])
                return tag, value
            self._log(line, log)

    def _idle(self, deadline, log, cancelled=None, *, closing=False):
        # Any output after the last end record is unclaimed. Never attribute it to a new batch.
        if self.process.poll() is not None:
            raise RuntimeError('IMLane session is no longer idle after its end record')
        while self.buffer or select.select([self.process.stdout], [], [], 0)[0]:
            line = self._line(deadline, cancelled, closing=closing)
            if line.startswith(_PREFIX):
                raise ValueError('IMLane session has an unclaimed record after its end record')
            self._log(line, log)

    def _start(self, deadline, log, cancelled, timings):
        if self.process is not None:
            self._idle(deadline, log, cancelled)
            return
        config = self.owner.config
        env = dict(os.environ, **config.get('env', {}),
                   IMLANE_NAMESPACE=config['namespace'], CUDA_VISIBLE_DEVICES='')
        command = [config['bridge_binary'], str(config['gateway_port']), config['udf_path'],
                   str(config['native_task_threads']), *self.owner.limits.cpp_arguments(), '--session']
        started = time.monotonic_ns()
        with self.owner._lock:
            if self.owner._closed:
                raise InterruptedError('IMLane session closed before bridge creation')
            self.process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, env=env, cwd=self.root, start_new_session=True)
            self.owner._process = self.process
        timings['popen_ns'] = time.monotonic_ns() - started
        os.set_blocking(self.process.stdin.fileno(), False)
        os.set_blocking(self.process.stdout.fileno(), False)
        seen = set()
        while True:
            tag, value = self._record(deadline, log, cancelled)
            if tag in seen:
                raise ValueError('IMLane session repeated startup metadata')
            seen.add(tag)
            if tag == b'META':
                if (set(value) != {'native_executors', 'native_task_threads'}
                        or any(type(v) is not int or not 1 <= v <= 1024 for v in value.values())
                        or value['native_task_threads'] != config['native_task_threads']):
                    raise ValueError('IMLane session native settings differ from configuration')
                self.settings = value
            elif tag == b'LIMITS':
                if value != asdict(self.owner.limits) or any(type(v) is not int for v in value.values()):
                    raise ValueError('IMLane session capacity differs from configuration')
            elif tag == b'SESSION':
                if (value != {'protocol': 1} or type(value['protocol']) is not int
                        or seen != {b'META', b'LIMITS', b'SESSION'}):
                    raise ValueError('IMLane session startup protocol is invalid')
                timings['process_to_ready_ns'] = time.monotonic_ns() - started
                return
            else:
                raise ValueError('IMLane session returned a batch record before startup')

    def _stop(self):
        process = self.process
        forced = self.owner._stop(process)
        if process is not None:
            for stream in (process.stdin, process.stdout):
                if stream is not None:
                    stream.close()
        if process is None or process.poll() is not None:
            self.owner._process = None
        return forced

    def close(self):
        process = self.process
        if process is None or process.poll() is not None:
            self._stop()
            return
        primary = None
        try:
            if self._busy.locked() or self.owner.poisoned:
                self.owner.poisoned = True
                return
            deadline = time.monotonic() + 3
            with open_private_text(self.root / 'session-close.log') as log:
                self._idle(deadline, log, closing=True)
                self._send({'kind': 'close'}, deadline, closing=True)
                tag, value = self._record(deadline, log, closing=True)
                if (tag != b'SESSION_END' or value != {'protocol': 1}
                        or type(value['protocol']) is not int or self.buffer):
                    raise ValueError('IMLane session close record is invalid')
            process.wait(timeout=max(.001, deadline - time.monotonic()))
            if process.returncode != 0:
                raise RuntimeError('IMLane session bridge did not exit cleanly')
        except BaseException as error:
            primary = error
            self.owner.poisoned = True
            raise
        finally:
            try:
                self._stop()
            except BaseException as error:
                if primary is None:
                    raise
                primary.add_note('IMLane session stop also failed: ' + type(error).__name__)

    def iter_batch(self, calls, *, cancelled=None, timeout_s=120):
        if not self._busy.acquire(blocking=False):
            raise RuntimeError('IMLane session already owns an active batch')
        try:
            yield from self._batch(calls, cancelled, timeout_s)
        finally:
            self._busy.release()

    def _batch(self, calls, cancelled, timeout_s):
        owner = self.owner
        owner.last_close_report = None
        if type(timeout_s) not in (int, float) or not math.isfinite(timeout_s) or not 0 < timeout_s <= 120:
            raise ValueError('IMLane session requires a finite deadline at most 120 seconds')
        wall_started = time.monotonic_ns()
        deadline = time.monotonic() + timeout_s
        calls = tuple(calls)
        owner.last_preflight = owner.limits.inspect_calls(calls)
        timings = dict(input_preflight_ns=time.monotonic_ns() - wall_started,
                       call_encoding_ns=0, input_file_write_ns=0, outcome_decode_ns=0,
                       consumer_wait_ns=0, popen_ns=0)
        if owner.poisoned:
            raise RuntimeError('IMLane session retained an uncertain previous batch')
        self._check(deadline, cancelled)
        if not calls:
            owner.last_close_report = dict(clean=True, graph_consumer_stopped=True,
                bridge_started=False, author_submission_possible=False, prepared=0, received=0,
                bridge_lifecycle='session', remote_http_stopped='unavailable', batch_callbacks_returned=True)
            return
        if self.batches >= _BATCH_MAX:
            raise ValueError('IMLane session reached its finite batch allowance')
        # Recheck fixed files, including before reusing a live context; never silently reload an alias.
        if self.root is None:
            self.root = owner.work_root / ('session-' + uuid.uuid4().hex)
            new_private_directory(self.root)
        batch_id = uuid.uuid4().hex
        out = self.root / ('batch-' + batch_id)
        new_private_directory(out)
        expected = {call.call_id: call for call in calls}
        received = set()
        total_bytes = 0
        primary = None
        clean = callbacks_returned = sent = uncertain = forced = False
        stats = None
        try:
            owner.start()
            started = time.monotonic_ns()
            with open_private_text(out / 'input.jsonl') as stream:
                for call in calls:
                    encoded = time.monotonic_ns()
                    wire = _call_wire(call) + '\n'
                    timings['call_encoding_ns'] += time.monotonic_ns() - encoded
                    stream.write(wire)
            timings['input_file_write_ns'] = time.monotonic_ns() - started - timings['call_encoding_ns']
            with open_private_text(out / 'bridge.log') as log:
                self._start(deadline, log, cancelled, timings)
                self._idle(deadline, log, cancelled)
                with owner._lock:
                    self._check(deadline, cancelled)
                    sent = True  # A partial command send is also uncertain; do not reuse it.
                    self.command_active = True
                self._send(dict(kind='batch', batch_id=batch_id,
                                input_path=f'batch-{batch_id}/input.jsonl'), deadline, cancelled)
                self.batches += 1
                while True:
                    tag, value = self._record(deadline, log, cancelled)
                    if value.get('batch_id') != batch_id:
                        raise ValueError('IMLane session result belongs to a different batch')
                    if tag == b'RESULT':
                        if set(value) != {'batch_id', 'outcome'}:
                            raise ValueError('IMLane session result envelope is invalid')
                        started = time.monotonic_ns()
                        outcome = _outcome_from_wire(json.dumps(value['outcome']))
                        timings['outcome_decode_ns'] += time.monotonic_ns() - started
                        call = expected.get(outcome.call_id)
                        if (call is None or outcome.call_id in received
                                or outcome.row_id != call.row_id):
                            raise ValueError('IMLane session result changed occurrence identity')
                        kind = outcome_kind(outcome)
                        uncertain = uncertain or kind != 'response'
                        size = (len(encode_full_response(outcome.response)) if outcome.response is not None
                                else len((outcome.transport_error or outcome.execution_error).encode()))
                        total_bytes += size
                        if size > call.max_response_bytes or total_bytes > owner.limits.max_batch_response_bytes:
                            raise ValueError('IMLane session responses exceed their byte allowance')
                        received.add(outcome.call_id)
                        started = time.monotonic_ns()
                        try:
                            yield outcome
                        finally:
                            timings['consumer_wait_ns'] += time.monotonic_ns() - started
                    elif tag == b'BATCH_END':
                        stats = value.get('stats')
                        expected_counts = dict(submitted=len(calls), delivered=len(calls),
                            input_payload_bytes=owner.last_preflight['payload_bytes'],
                            stdin_wire_bytes=owner.last_preflight['stdin_wire_bytes'],
                            delivered_response_bytes=total_bytes, session_batches=self.batches)
                        if (set(value) != {'batch_id', 'stats'} or type(stats) is not dict
                                or len(received) != len(calls)
                                or any(type(stats.get(k)) is not int or stats[k] != v
                                       for k, v in expected_counts.items())
                                or type(stats.get('pending_peak')) is not int
                                or not 1 <= stats['pending_peak'] <= min(len(calls), owner.limits.max_pending_calls)
                                or type(stats.get('timing_ns')) is not dict
                                or set(stats['timing_ns']) != _BRIDGE_TIMINGS
                                or any(type(v) is not int or v < 0 for v in stats['timing_ns'].values())
                                or stats.get('lifecycle') != dict(context_creations=int(self.batches == 1),
                                    registrations=int(self.batches == 1), author_calls=len(calls), output_copies=len(calls))
                                or any(type(v) is not int for v in stats['lifecycle'].values())):
                            raise ValueError('IMLane session end record did not confirm this complete batch')
                        callbacks_returned = True
                        self._idle(deadline, log, cancelled)
                        with owner._lock:
                            self._check(deadline, cancelled)
                            clean = not uncertain and not owner.poisoned
                            if clean:
                                self.command_active = False
                        break
                    else:
                        raise ValueError('IMLane session returned an unexpected batch record')
        except BaseException as error:
            primary = error
            raise
        finally:
            cleanup_started = time.monotonic_ns()
            if not clean:
                owner.poisoned = owner.poisoned or self.process is not None
                try:
                    forced = self._stop()
                except BaseException as cleanup:
                    if primary is None:
                        primary = cleanup
                    else:
                        primary.add_note('IMLane session cleanup error: ' + type(cleanup).__name__)
            stopped = self.process is None or self.process.poll() is not None
            timings.update(cleanup_ns=time.monotonic_ns() - cleanup_started,
                           batch_wall_ns=time.monotonic_ns() - wall_started)
            owner.last_close_report = dict(clean=clean, graph_consumer_stopped=True,
                bridge_started=self.process is not None, bridge_stopped=stopped,
                author_submission_possible=sent, bridge_lifecycle='session', batch_id=batch_id,
                bridge_pid=None if self.process is None else self.process.pid,
                batch_callbacks_returned=callbacks_returned, batch_command_sent=sent,
                prepared=len(calls), received=len(received), forced=forced,
                returncode=None if self.process is None else self.process.returncode,
                remote_http_stopped='unavailable', coordinator_owner='caller',
                author_lane_restore_ack='unavailable; author does not acknowledge RestoreLane success',
                scheduler_owner='IMLane author implementation', native_settings=self.settings,
                bridge_limits=asdict(owner.limits), bridge_stats=stats, snapshot=owner.last_preflight,
                executor_poisoned=owner.poisoned, concurrency_allowance=owner.options.concurrency,
                timing_ns=timings, timing_scope='caller stages; do not sum with overlapping bridge stages')
            try:
                write_private_json(out / 'close.json', owner.last_close_report)
            except BaseException as cleanup:
                owner.poisoned = True
                owner.last_close_report.update(clean=False, executor_poisoned=True)
                if primary is None:
                    primary = cleanup
                else:
                    primary.add_note('IMLane session record error: ' + type(cleanup).__name__)
                try:
                    self._stop()
                    owner.last_close_report['bridge_stopped'] = self.process.poll() is not None
                except BaseException as stopped_error:
                    primary.add_note('IMLane session stop error: ' + type(stopped_error).__name__)
            if primary is not None and not isinstance(primary, GeneratorExit):
                # An exception already propagating retains its original traceback and notes.
                raise primary
