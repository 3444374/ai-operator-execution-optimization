"""Optional adapter diagnostics using the existing bounded event writer.

Set SEMLOOM_DETAILED_TIMING_DIR to an outside-Git directory to enable. Timestamps
describe observable adapter calls, never inferred vendor queue or GPU time.
"""
import atexit
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
import hashlib
import inspect
import os
from pathlib import Path
import sys
import threading
import time
import uuid

from .buffered_events import BufferedEvents
from src.baselines.common.private_artifacts import require_outside_git, write_private_json


_identity = ContextVar('execution_timing_identity', default={})
_parent = ContextVar('execution_timing_parent', default=None)
_shared = None
_shared_lock = threading.Lock()
_FIELDS = frozenset(('query_id', 'request_id', 'actor_id', 'arm', 'phase', 'slot',
    'kind', 'call_id', 'row_id', 'stage_id', 'source_position', 'sequence',
    'owner_id', 'executor_id', 'worker_id', 'job_id', 'endpoint_id', 'gateway_request_id',
    'elapsed_ns',
    'status', 'error_type', 'reason', 'backend', 'scope', 'clock_verified',
    'context_exit_includes_failed_commit_rollback', 'source_event', 'worker_pid'))


def is_timing_error(error):
    return bool(getattr(error, '_semloom_timing_failure', False))


def _timing_error(error):
    error._semloom_timing_failure = True
    return error


class Timing:
    """One process clock and bounded writer; no per-event flush or fsync."""
    def __init__(self, directory, *, run_id=None, writer_factory=BufferedEvents):
        directory = Path(directory)
        require_outside_git(directory)
        directory.mkdir(parents=True, exist_ok=True)
        self.directory = directory.resolve()
        self.pid, self.process_instance = os.getpid(), uuid.uuid4().hex
        from src.execution_provider.adapters.ray_map_transport import _clock_domain
        domain = _clock_domain()
        self.clock_domain = domain or 'process-' + self.process_instance
        self.clock_verified = domain is not None
        self.run_id = run_id or hashlib.sha256(str(directory.resolve()).encode()).hexdigest()[:24]
        self.path = directory / ('timing-' + self.process_instance + '.jsonl')
        self.writer = writer_factory(self.path)
        self.closed = False

    def _record(self, event, operation, monotonic_ns, **fields):
        value = dict(_identity.get())
        value.update(fields)
        value = {key: item for key, item in value.items() if key in _FIELDS and item is not None}
        if any(not isinstance(item, (str, int, bool)) or isinstance(item, str) and len(item) > 512
               for item in value.values()):
            raise ValueError('timing identities must be finite scalar values')
        try:
            self.writer.record(dict(value, schema='semloom.execution_timing.v1', event=event,
                operation=operation, monotonic_ns=monotonic_ns, run_id=self.run_id,
                pid=self.pid, process_instance=self.process_instance, clock_domain=self.clock_domain,
                clock_verified=self.clock_verified, **self._span_fields(fields)))
        except BaseException as error:
            _timing_error(error)
            raise

    @staticmethod
    def _span_fields(fields):
        return {key: fields[key] for key in ('span_id', 'parent_span_id') if fields.get(key) is not None}

    def mark(self, operation, *, monotonic_ns=None, **fields):
        self._record('mark', operation, time.monotonic_ns() if monotonic_ns is None else monotonic_ns,
                     **dict(fields, parent_span_id=_parent.get()))

    def record_interval(self, operation, started_ns, ended_ns, **fields):
        """Publish already captured integers after a transaction has released its lock."""
        if (type(started_ns) is not int or started_ns <= 0 or ended_ns is not None and
                (type(ended_ns) is not int or ended_ns < started_ns)):
            raise ValueError('timing interval needs ordered local monotonic timestamps')
        span_id = uuid.uuid4().hex
        common = dict(fields, span_id=span_id, parent_span_id=_parent.get())
        self._record('span_started', operation, started_ns, **common)
        if ended_ns is not None:
            self._record('span_ended', operation, ended_ns, **common)

    @contextmanager
    def bind(self, **fields):
        token = _identity.set(dict(_identity.get(), **fields))
        try:
            yield self
        finally:
            _identity.reset(token)

    @contextmanager
    def span(self, operation, **fields):
        span_id, parent = uuid.uuid4().hex, _parent.get()
        surrounding = sys.exception()
        observation = None
        try:
            self._record('span_started', operation, time.monotonic_ns(),
                         **dict(fields, span_id=span_id, parent_span_id=parent))
        except BaseException as failure:
            observation = _timing_error(failure)
        token = _parent.set(span_id)
        try:
            yield span_id
        except BaseException as error:
            try:
                self._record('span_ended', operation, time.monotonic_ns(), **dict(fields,
                    span_id=span_id, parent_span_id=parent,
                    status=('cancelled' if type(error).__name__ in
                        ('CancelledError', 'GeneratorExit', 'KeyboardInterrupt') else 'error'),
                    error_type=type(error).__name__))
            except BaseException as failure:
                observation = observation or _timing_error(failure)
            if observation is not None:
                error.add_note('Detailed timing also failed: ' + type(observation).__name__)
            raise
        else:
            try:
                self._record('span_ended', operation, time.monotonic_ns(), **dict(fields,
                    span_id=span_id, parent_span_id=parent, status=fields.get('status', 'completed')))
            except BaseException as failure:
                observation = observation or _timing_error(failure)
            if observation is not None:
                if surrounding is None:
                    raise observation
                surrounding.add_note('Detailed timing also failed: ' + type(observation).__name__)
        finally:
            _parent.reset(token)

    def close(self):
        if self.closed:
            return
        try:
            self.writer.close()
            snapshot = getattr(self.writer, 'snapshot', lambda: None)()
            temporary = self.path.with_suffix('.closing.json')
            write_private_json(temporary, dict(schema='semloom.execution_timing_close.v1',
                run_id=self.run_id, pid=self.pid, process_instance=self.process_instance,
                clock_domain=self.clock_domain, status='closed', writer=snapshot))
            temporary.replace(self.path.with_suffix('.closed.json'))
        finally:
            self.closed = True


def get_timing():
    """Disabled paths create no writer and read no additional clock."""
    directory = os.environ.get('SEMLOOM_DETAILED_TIMING_DIR')
    if not directory:
        return None
    global _shared
    pid = os.getpid()
    with _shared_lock:
        if _shared is not None and _shared.pid == pid and not _shared.closed and _shared.directory != Path(directory).resolve():
            raise ValueError('close detailed timing before changing its output directory')
        if _shared is None or _shared.pid != pid or _shared.closed:
            _shared = Timing(directory, run_id=os.environ.get('SEMLOOM_TIMING_RUN_ID'))
        return _shared


def close_timing(*, error=None):
    if _shared is not None and _shared.pid == os.getpid() and not _shared.closed:
        try:
            _shared.close()
        except BaseException as observation:
            _timing_error(observation)
            if error is None:
                raise
            error.add_note('Detailed timing close also failed: ' + type(observation).__name__)


atexit.register(close_timing)


@contextmanager
def timing_span(operation, **fields):
    surrounding = sys.exception()
    try:
        timing = get_timing()
    except BaseException as observation:
        _timing_error(observation)
        # Even a failed recorder setup must not skip a real resource close.
        try:
            yield None
        except BaseException as error:
            error.add_note('Detailed timing setup also failed: ' + type(observation).__name__)
            raise
        if surrounding is None:
            raise observation
        surrounding.add_note('Detailed timing setup also failed: ' + type(observation).__name__)
        return
    if timing is None:
        yield None
    else:
        with timing.span(operation, **fields) as span:
            yield span


def timed_call(operation, *, query=False, finalize=False):
    """Wrap an actual adapter invocation; generators need explicit iteration spans."""
    def decorate(function):
        signature = inspect.signature(function)

        @wraps(function)
        def call(*args, **kwargs):
            if not os.environ.get('SEMLOOM_DETAILED_TIMING_DIR'):
                return function(*args, **kwargs)
            arguments = signature.bind_partial(*args, **kwargs).arguments
            fields = {name: arguments[name] for name in ('arm', 'phase') if name in arguments}
            if 'unit_id' in arguments:
                fields['query_id'] = arguments['unit_id']
            owner = arguments.get('self')
            if owner is not None:
                for attribute, field in (('arm', 'arm'), ('identity', 'owner_id'), ('backend', 'backend')):
                    value = getattr(owner, attribute, None)
                    if isinstance(value, (str, int)):
                        fields.setdefault(field, value)
            if owner is not None and hasattr(owner, 'args') and hasattr(owner.args, 'arm'):
                fields['arm'] = owner.args.arm
            top_level = _parent.get() is None
            surrounding = arguments.get('error') if isinstance(arguments.get('error'), BaseException) else sys.exception()
            primary = None
            completed = False
            token = _identity.set(dict(_identity.get(), **fields))
            try:
                with timing_span(operation):
                    result = function(*args, **kwargs)
                    completed = True
                    if query and isinstance(result, dict):
                        timing = get_timing()
                        if timing is not None:
                            record_query_timing(timing, result)
                    return result
            except BaseException as error:
                if completed and surrounding is not None:
                    surrounding.add_note('Detailed timing also failed: ' + type(error).__name__)
                    return result
                primary = error
                raise
            finally:
                _identity.reset(token)
                if finalize and top_level and arguments.get('owner') is None:
                    close_timing(error=primary or surrounding)
        return call
    return decorate


def timing_thread_target(function):
    """Carry only diagnostic identity into an existing adapter consumer thread."""
    if not os.environ.get('SEMLOOM_DETAILED_TIMING_DIR'):
        return function
    identity, parent = dict(_identity.get()), _parent.get()

    @wraps(function)
    def run():
        identity_token, parent_token = _identity.set(identity), _parent.set(parent)
        try:
            return function()
        finally:
            _parent.reset(parent_token)
            _identity.reset(identity_token)
    return run


def record_query_timing(timing, result):
    """Reuse the base recorder's timestamps without changing its totals or schema."""
    execution = result.get('execution') or result
    first = result.get('query_preparation_started_ns', execution.get('t_release_ns'))
    terminal = execution.get('t_query_terminal_ns')
    for name, begin, end in (
            ('query.total_to_terminal', first, terminal),
            ('query.preparation', first, execution.get('t_backend_ready_ns')),
            ('query.submitted_to_terminal', execution.get('t_submit_ns'), terminal),
            ('query.stream_cleanup', terminal, execution.get('t_stream_cleanup_ns'))):
        if type(begin) is int and type(end) is int:
            timing.record_interval(name, begin, end, status=execution.get('query_status', 'unknown'))
    if type(terminal) is int:
        timing.mark('query.consumer_terminal', monotonic_ns=terminal,
                    status=execution.get('query_status', 'unknown'))
