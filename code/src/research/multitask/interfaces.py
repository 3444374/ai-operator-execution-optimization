"""Read-only inputs supplied by the caller, independent of an execution engine."""

from typing import Protocol


class CapacityChoice(Protocol):
    @property
    def request_limit(self) -> int: ...

    @property
    def work_limit(self) -> int: ...


class ReadyRequest(Protocol):
    @property
    def request_id(self) -> str: ...

    @property
    def job_id(self) -> str: ...

    @property
    def estimated_work_units(self) -> int: ...

    @property
    def preferred_endpoint_id(self) -> str | None: ...


class WorkEnvelope(Protocol):
    @property
    def request(self) -> ReadyRequest: ...


class StageBacklog(Protocol):
    @property
    def queued_work(self) -> int: ...


class RuntimeObservation(Protocol):
    @property
    def observed_at_s(self) -> float: ...

    @property
    def calibration_signature(self) -> str: ...

    def is_fresh(self, *, now_s: float, max_age_s: float) -> bool: ...

    def for_stage(self, stage: str, /) -> StageBacklog | None: ...
