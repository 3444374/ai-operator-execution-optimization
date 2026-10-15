"""Small opt-in worker ownership records, independent of reuse/timing probes."""
from __future__ import annotations

import json
from contextlib import contextmanager
import os
from pathlib import Path
import re
import uuid


_GPU_UUID = re.compile(r"(?:GPU-)?([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})")


def _canonical_gpu_uuid(value, *, nvml=False):
    if not isinstance(value, str) or (nvml and not value.startswith("GPU-")):
        raise ValueError("CLIP worker requires a complete GPU UUID")
    match = _GPU_UUID.fullmatch(value)
    if match is None:
        raise ValueError("CLIP worker GPU UUID format is unavailable or unsupported")
    return "GPU-" + match.group(1).lower()


@contextmanager
def resource_record_directory(root):
    from src.baselines.common.private_artifacts import require_outside_git
    require_outside_git(root)
    previous = os.environ.get("SEMLOOM_IMAGE_RESOURCE_DIR")
    os.environ["SEMLOOM_IMAGE_RESOURCE_DIR"] = str(root)
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("SEMLOOM_IMAGE_RESOURCE_DIR", None)
        else:
            os.environ["SEMLOOM_IMAGE_RESOURCE_DIR"] = previous


def worker_identity(role, **fields):
    record = dict(worker_uid=uuid.uuid4().hex, role=role, pid=os.getpid(), **fields)
    stat = Path("/proc/self/stat")
    if stat.is_file():
        raw = stat.read_text()
        record["process_start_time_ticks"] = raw[raw.rfind(")") + 2:].split()[19]
    return record


def record_worker_identity(record):
    directory = os.environ.get("SEMLOOM_IMAGE_RESOURCE_DIR")
    if directory is None:
        return
    from src.baselines.common.private_artifacts import require_outside_git
    root = Path(directory)
    require_outside_git(root)
    root.mkdir(parents=True, exist_ok=True)
    path = root / (record["worker_uid"] + ".json")
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(record, sort_keys=True) + "\n")
    os.replace(temporary, path)


def assigned_gpu_uuid(torch, device, allowed_gpu_uuids):
    """Check actual CUDA assignment before loading weights on a worker."""
    if not isinstance(allowed_gpu_uuids, (tuple, list)) or not allowed_gpu_uuids:
        raise ValueError("scoped CLIP worker needs an explicit GPU UUID allocation")
    if torch.cuda.device_count() != 1:
        raise ValueError("scoped full-GPU CLIP workers must see exactly one backend-assigned CUDA device")
    allowed = {}
    for token in allowed_gpu_uuids:
        canonical = _canonical_gpu_uuid(token, nvml=True)
        if canonical in allowed:
            raise ValueError("CLIP worker allocation contains duplicate GPU UUID identities")
        allowed[canonical] = token
    # Torch exposes a _CUuuid object whose string omits NVML's GPU- prefix.
    uid = _canonical_gpu_uuid(str(getattr(torch.cuda.get_device_properties(device), "uuid", None)))
    if uid not in allowed:
        raise ValueError("CLIP worker GPU UUID is outside its deployment allocation")
    return allowed[uid]


def validate_preprocessing_identity(identity):
    from .staged import image_transform_signature
    if (not isinstance(identity, dict)
            or not image_transform_signature(**identity).startswith("image-transform-v2:")
            or identity.get("input_size") != 224 or identity.get("layout") != "NCHW"
            or identity.get("dtype") != "float32"
            or identity.get("representation") != "prepared_fp32_nchw"):
        raise ValueError("Slow CLIP worker requires its actual preprocessing identity")
    return identity
