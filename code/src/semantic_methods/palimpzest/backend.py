"""Observe/dispatch original Generator completions without replacing the PZ DAG.

This private, version-checked seam is synchronous. A supplied completion callable
owns model-call execution only; the original PZ strategy still owns operators,
dependency queues, LIMIT behavior and thread lifetime.
"""

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from functools import wraps
from importlib import import_module
import threading

from .source import validate_source


_BRIDGE_LOCK = threading.Lock()
_ATTEMPT = ContextVar("palimpzest_generation_attempt", default=None)


class PalimpzestAdapterError(RuntimeError):
    """A bridge contract failure, distinct from a model/HTTP/parser failure."""


@dataclass
class _Attempt:
    call_id: str
    row_id: tuple[str, ...]
    adapter_error: BaseException | None = None
    observed: bool = False


class BridgeState:
    """Small lifecycle summary; response/usage/parse evidence stays in the observer."""

    def __init__(self, observer):
        self.observer = observer
        self._lock = threading.RLock()
        self._next = self._active = self._completed = 0
        self._admitted = 0
        self._closing = self._restored = False
        self._fatal = None
        self._restore = None
        self.failures = []

    def snapshot(self):
        with self._lock:
            return dict(generators_started=self._next, generators_completed=self._completed,
                        active_generators=self._active, closing=self._closing,
                        patches_restored=self._restored,
                        completions_admitted=self._admitted,
                        fatal_error_type=None if self._fatal is None else type(self._fatal).__name__,
                        failures=[dict(stage=stage, error_type=kind) for stage, kind in self.failures],
                        remote_work_stopped="unavailable")

    def _failed(self, stage, error):
        with self._lock:
            if self._fatal is None:
                self._fatal = error
            # A failing observer must not create unbounded secondary diagnostics.
            if len(self.failures) < 16:
                self.failures.append((stage, type(error).__name__))

    def _check_submission(self, attempt=None):
        with self._lock:
            if self._fatal is not None:
                error = PalimpzestAdapterError("PZ bridge stopped after a prior adapter contract failure")
                if attempt is not None:
                    attempt.adapter_error = error
                raise error from self._fatal
            if self._closing:
                error = PalimpzestAdapterError("PZ completion bridge is closing; new submission rejected")
                if attempt is not None:
                    attempt.adapter_error = error
                raise error

    def _admit(self, attempt, kwargs):
        # Serialize only the fail-closed check/accounting, never model execution.
        with self._lock:
            self._check_submission(attempt)
            self._observe("started", attempt, kwargs, row_id=attempt.row_id)
            if attempt.adapter_error is not None:
                raise attempt.adapter_error
            attempt.observed = True
            self._admitted += 1

    def _observe(self, method, attempt, *args, **kwargs):
        if self.observer is None:
            return
        try:
            getattr(self.observer, method)(attempt.call_id, *args, **kwargs)
        except BaseException as error:
            self._failed(method, error)
            if attempt.adapter_error is None:
                attempt.adapter_error = error

    def _begin(self, row_id):
        with self._lock:
            self._check_submission()
            self._next += 1
            self._active += 1
            return _Attempt(f"pz-generation-{self._next}", row_id)

    def _end(self):
        with self._lock:
            self._active -= 1
            self._completed += 1
            if self._closing and self._active == 0:
                self._restore()

    def _close(self):
        with self._lock:
            self._closing = True
            if self._active == 0:
                self._restore()
            return self._active


class _LiteLLMProxy:
    def __init__(self, original, completion):
        self._original = original
        self.completion = completion

    def __getattr__(self, name):
        return getattr(self._original, name)


def _source_indices(candidate, right_candidate):
    records = candidate if isinstance(candidate, list) else [candidate]
    if right_candidate is not None:
        records = [*records, right_candidate]
    return tuple(index for record in records for index in record.source_indices)


@contextmanager
def completion_bridge(*, completion, observer=None):
    """Keep original prompt/parser/stats and use one explicitly supplied completion.

    The callable may provide validate(kwargs) to reject configuration before submission.
    Configuration/observer failures fail the query outside Generator's exception-to-default
    handler. Actual completion/parser exceptions keep the original False/None behavior.
    The caller must join all PZ work before leaving this context. If it does not, the
    proxy remains installed until those generators exit, preventing a late fallback.
    """
    validate_source()
    if not callable(completion):
        raise TypeError("PZ completion bridge requires an explicit callable")
    if not _BRIDGE_LOCK.acquire(blocking=False):
        raise RuntimeError("PZ completion bridge already owns the private Generator seam")
    state = BridgeState(observer)
    try:
        module = import_module("palimpzest.query.generators.generators")
        generator = module.Generator
        original_litellm = module.litellm
        original_call, original_parse = generator.__call__, generator._parse_answer
    except BaseException:
        _BRIDGE_LOCK.release()
        raise

    def complete(**kwargs):
        attempt = _ATTEMPT.get()
        if attempt is None:
            raise RuntimeError("PZ completion lacks its original Generator occurrence")
        state._check_submission(attempt)
        try:
            validator = getattr(completion, "validate", None)
            if validator is not None:
                validator(kwargs)
        except BaseException as error:
            attempt.adapter_error = error
            state._failed("validation", error)
            raise
        state._admit(attempt, kwargs)
        try:
            response = completion(**kwargs)
        except BaseException as error:
            if isinstance(error, PalimpzestAdapterError):
                attempt.adapter_error = error
                state._failed("execution_contract", error)
            state._observe("finished", attempt, error=error)
            raise
        state._observe("finished", attempt, response=response)
        return response

    @wraps(original_parse)
    def parse(self, *args, **kwargs):
        attempt = _ATTEMPT.get()
        try:
            result = original_parse(self, *args, **kwargs)
        except Exception as error:
            if attempt is not None:
                state._observe("parsed", attempt, None, error=error, parser_default=True)
            raise
        if attempt is not None:
            state._observe("parsed", attempt, result)
        return result

    @wraps(original_call)
    def generate(self, candidate, fields, right_candidate=None, *args, **kwargs):
        attempt = state._begin(_source_indices(candidate, right_candidate))
        token = _ATTEMPT.set(attempt)
        result = primary = None
        try:
            result = original_call(self, candidate, fields, right_candidate, *args, **kwargs)
            if attempt.adapter_error is not None:
                raise attempt.adapter_error
            return result
        except BaseException as error:
            primary = error
            raise
        finally:
            try:
                if attempt.observed:
                    state._observe("generator_finished", attempt, result=result, error=primary,
                                   row_id=attempt.row_id)
                if attempt.adapter_error is not None:
                    if primary is None:
                        raise attempt.adapter_error
                    if primary is not attempt.adapter_error:
                        primary.add_note("PZ observer also failed: " + type(attempt.adapter_error).__name__)
            finally:
                _ATTEMPT.reset(token)
                state._end()

    proxy = _LiteLLMProxy(original_litellm, complete)

    def restore():
        try:
            restored = True
            for owner, name, expected, original in (
                (module, "litellm", proxy, original_litellm),
                (generator, "__call__", generate, original_call),
                (generator, "_parse_answer", parse, original_parse),
            ):
                if getattr(owner, name) is expected:
                    setattr(owner, name, original)
                else:
                    restored = False
                    state._failed("restore", RuntimeError("PZ private seam changed owner"))
            state._restored = restored
        finally:
            _BRIDGE_LOCK.release()

    state._restore = restore
    module.litellm = proxy
    generator.__call__, generator._parse_answer = generate, parse
    primary = None
    try:
        yield state
    except BaseException as error:
        primary = error
        raise
    finally:
        active = state._close()
        if active:
            error = RuntimeError("PZ bridge still has live generators; patches retained until they exit")
            if primary is None:
                raise error
            primary.add_note(str(error))
        elif not state._restored:
            error = RuntimeError("PZ private seam ownership changed before bridge restoration")
            if primary is None:
                raise error
            primary.add_note(str(error))
