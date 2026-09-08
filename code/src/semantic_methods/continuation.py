"""One row's method state: produce work, consume its completion, return one result."""

from dataclasses import dataclass
from typing import Protocol

from ..scheduling.core.session_contract import OfferedTask, TaskKey


@dataclass(frozen=True)
class Request:
    profile_name: str
    payload: bytes
    estimated_work: int
    max_result_bytes: int

    def offered(self, sequence: int) -> OfferedTask:
        return OfferedTask(
            sequence,
            self.payload,
            self.estimated_work,
            self.max_result_bytes,
            profile_name=self.profile_name,
        )


@dataclass(frozen=True)
class Continue:
    request: Request
    state: bytes = b""


@dataclass(frozen=True)
class Final:
    value: bytes


class Method(Protocol):
    """Callbacks only transform bounded data; they must not submit work or block on I/O.

    Immutable continuation state belongs to the row, not to a shared method instance.
    The caller supplies an already authorized semantic method and model profiles.
    """

    def start(self, value: bytes) -> Continue | Final: ...
    def resume(self, state: bytes, result: bytes) -> Continue | Final: ...


@dataclass(frozen=True)
class MethodLimits:
    input_bytes: int
    state_bytes: int
    result_bytes: int
    stages: int

    def __post_init__(self):
        if any(type(v) is not int or v <= 0 for v in vars(self).values()):
            raise ValueError("method limits must be positive integers")


def _bounded(value: bytes, limit: int) -> None:
    if type(value) is not bytes or len(value) > limit:
        raise ValueError("invalid or oversized method data")


class MethodRun:
    """A bounded sequential continuation; scheduling and delivery leases stay outside.

    The driver owns row identity, aggregate memory, cancellation, and session sealing.
    One accepted stage at a time; parallel stages require a separate explicit contract.
    """

    def __init__(self, method: Method, value: bytes, limits: MethodLimits):
        _bounded(value, limits.input_bytes)
        self._method = method
        self._limits = limits
        self._stages = 0
        self._key: TaskKey | None = None
        self.failed = False
        self._step = self._checked(method.start(value))

    def _checked(self, step: Continue | Final) -> Continue | Final:
        if type(step) is Final:
            _bounded(step.value, self._limits.result_bytes)
            return step
        if type(step) is not Continue or type(step.request) is not Request:
            raise ValueError("method must return Continue or Final")
        request = step.request
        _bounded(step.state, self._limits.state_bytes)
        _bounded(request.payload, self._limits.input_bytes)
        if (
            type(request.profile_name) is not str
            or not request.profile_name
            or len(request.profile_name.encode()) > 256
            or type(request.estimated_work) is not int
            or request.estimated_work <= 0
            or type(request.max_result_bytes) is not int
            or not 0 < request.max_result_bytes <= self._limits.result_bytes
        ):
            raise ValueError("invalid method request")
        if self._stages >= self._limits.stages:
            raise ValueError("method stage limit exceeded")
        return step

    @property
    def pending(self) -> Request | None:
        if not self.failed and self._key is None and type(self._step) is Continue:
            return self._step.request
        return None

    @property
    def final(self) -> Final | None:
        return self._step if not self.failed and type(self._step) is Final else None

    def accepted(self, key: TaskKey) -> None:
        """Bind only after offer accepts the request; backpressure does not advance state."""
        if self.pending is None or type(key) is not TaskKey:
            raise ValueError("no pending stage or invalid task key")
        if any(type(v) is not int or not 0 <= v < 2**64 for v in vars(key).values()):
            raise ValueError("invalid task key")
        self._key = key
        self._stages += 1

    def complete(self, key: TaskKey, result: bytes) -> None:
        """Consume one successful completion. Driver releases its lease even on failure."""
        if self.failed or self._key is None or key != self._key:
            raise ValueError("completion does not belong to the active stage")
        try:
            _bounded(result, self._step.request.max_result_bytes)
            step = self._checked(self._method.resume(self._step.state, result))
        except Exception:
            self.failed = True
            self._key = None
            self._step = Final(b"")
            raise
        self._step = step
        self._key = None
