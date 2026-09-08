"""Validate bounded immutable organization metadata before ownership transfer."""

import json
from dataclasses import asdict

from ...planning.work import StageWork, WorkDescriptor
from .session_contract import OfferedTask, SessionSpec, TaskInfo

MAX_WORK_STAGES = 16
MAX_IDENTITY_BYTES = 256
UINT64_MAX = (1 << 64) - 1


def validate_task_info(task: OfferedTask, spec: SessionSpec, metadata_bytes: int) -> None:
    info = task.info
    if info is None:
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
