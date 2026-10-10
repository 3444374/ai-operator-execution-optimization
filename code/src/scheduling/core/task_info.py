"""Validate bounded immutable organization metadata before ownership transfer."""

import json
from dataclasses import asdict, dataclass, fields
from operator import attrgetter

from ...planning.work import StageWork, WorkDescriptor
from .session_contract import OfferedTask, SessionSpec, TaskInfo

MAX_WORK_STAGES = 16
MAX_IDENTITY_BYTES = 256
UINT64_MAX = (1 << 64) - 1

_INFO_VALUES = attrgetter(*(f.name for f in fields(TaskInfo) if f.name != 'work'))
_WORK_VALUES = attrgetter(*(f.name for f in fields(WorkDescriptor) if f.name != 'stages'))
_STAGE_VALUES = attrgetter(*(f.name for f in fields(StageWork)))
_SCALAR_TYPES = (str, int, float, bool, type(None))


def _values_key(values):
    kinds = tuple(map(type, values))
    if any(kind not in _SCALAR_TYPES for kind in kinds):
        return None
    # Separate type/value vectors retain 1/True and 1/1.0 distinctions without per-field pairs.
    if float in kinds:
        values = tuple(value.hex() if kind is float else value for kind, value in zip(kinds, values))
    return kinds, values


def _info_key(info):
    if type(info) is not TaskInfo or type(info.work) is not WorkDescriptor:
        return None
    work = info.work
    if type(work.stages) is not tuple or not 0 < len(work.stages) <= MAX_WORK_STAGES:
        return None
    values = _INFO_VALUES(info) + _WORK_VALUES(work)
    for stage in work.stages:
        if type(stage) is not StageWork:
            return None
        values += _STAGE_VALUES(stage)
    key = _values_key(values)
    return (len(work.stages), key) if key is not None else None


@dataclass(frozen=True)
class _InfoCheck:
    key: tuple
    scope: tuple
    encoded_bytes: int


class _TaskInfoChecks:
    """One session's latest unaccepted metadata; no task, payload or response references."""

    def __init__(self):
        self._retained = {}
        self._checking = {}

    @property
    def retained_count(self):
        return len(self._retained)

    def begin(self):
        self._checking = {}

    def find(self, sequence, key, scope):
        previous = self._retained.get(sequence)
        return previous if previous is not None and previous.key == key and previous.scope == scope else None

    def remember(self, sequence, check):
        self._checking[sequence] = check

    def retain(self, next_sequence):
        self._retained = {sequence: check for sequence, check in self._checking.items()
                          if sequence >= next_sequence}
        self._checking = {}

    def clear(self):
        self._retained.clear()
        self._checking.clear()


def validate_task_info(task: OfferedTask, spec: SessionSpec, metadata_bytes: int,
                       _checks: _TaskInfoChecks | None = None) -> None:
    info = task.info
    if info is None:
        return
    key = _info_key(info) if _checks is not None else None
    scope = (_values_key((
        spec.job_id, spec.flow_id, spec.capability, spec.operator, spec.work_unit, metadata_bytes))
        if key is not None else None)
    previous = _checks.find(task.sequence, key, scope) if key is not None else None
    if previous is not None:
        work = info.work
        if work.primary.unit != spec.work_unit or work.primary.units != task.estimated_work:
            raise ValueError("work metadata and admission accounting disagree")
        if type(task.metadata) is not bytes or previous.encoded_bytes + len(task.metadata) > metadata_bytes:
            raise ValueError("task metadata exceeds its bound")
        _checks.remember(task.sequence, previous)
        return
    if type(info) is not TaskInfo or type(info.work) is not WorkDescriptor:
        raise ValueError("invalid task information")
    work = info.work
    if type(work.stages) is not tuple or not 0 < len(work.stages) <= MAX_WORK_STAGES:
        raise ValueError("work stages must be a bounded immutable tuple")
    if any(type(stage) is not StageWork for stage in work.stages):
        raise ValueError("invalid work stage")
    strings = (info.call_id, info.stage_id, work.primary_stage, work.calibration_signature)
    strings += tuple(value for stage in work.stages for value in (stage.stage, stage.unit))
    if any(
        type(value) is not str or not value or len(value.encode()) > MAX_IDENTITY_BYTES
        for value in strings
    ):
        raise ValueError("invalid task or work identity")
    if type(work.locality_key) is not str or len(work.locality_key.encode()) > MAX_IDENTITY_BYTES:
        raise ValueError("invalid locality key")
    integers = (info.row_sequence, *(stage.units for stage in work.stages))
    integers += tuple(
        v for v in (work.lower_primary_units, work.upper_primary_units) if v is not None
    )
    if any(type(value) is not int or not 0 <= value <= UINT64_MAX for value in integers):
        raise ValueError("invalid row sequence or work estimate")
    if work.primary.unit != spec.work_unit or work.primary.units != task.estimated_work:
        raise ValueError("work metadata and admission accounting disagree")
    # The metadata limit covers both opaque bytes and the typed descriptor, per task.
    encoded = json.dumps(asdict(info), ensure_ascii=False, allow_nan=False).encode()
    if type(task.metadata) is not bytes or len(encoded) + len(task.metadata) > metadata_bytes:
        raise ValueError("task metadata exceeds its bound")
    if key is not None:
        _checks.remember(task.sequence, _InfoCheck(key, scope, len(encoded)))
