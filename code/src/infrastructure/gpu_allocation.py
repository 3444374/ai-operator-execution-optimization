"""Resolve an explicit local deployment allocation, never the whole host.

This is launch configuration, not a scheduler or a performance tuner. CUDA
visibility is applied by an owner before starting its runtime/model children.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, dataclass
import os
import re
import subprocess


def _tokens(value, name):
    if isinstance(value, str):
        value = value.split(",")
    if not isinstance(value, (list, tuple)) or not 1 <= len(value) <= 256:
        raise ValueError(name + " must be a nonempty explicit GPU list")
    result = tuple(value)
    if (any(type(x) is not str or not re.fullmatch(r"(?:[0-9]+|GPU-[A-Za-z0-9-]+)", x) for x in result)
            or len(set(result)) != len(result)):
        raise ValueError(name + " requires unique NVIDIA indices or full GPU UUIDs; MIG/all are unsupported")
    return result


def _probe_uuid(token):
    # -i is deliberate: auto never enumerates unallocated host devices.
    result = subprocess.run(["nvidia-smi", "-i", token, "--query-gpu=uuid",
                             "--format=csv,noheader,nounits"],
                            capture_output=True, text=True, timeout=10, check=True)
    values = result.stdout.strip().splitlines()
    if len(values) != 1 or not re.fullmatch(r"GPU-[A-Za-z0-9-]+", values[0]):
        raise ValueError("allocated GPU UUID is unavailable or ambiguous")
    return values[0]


@dataclass(frozen=True)
class GpuAllocation:
    requested_count: int | str
    requested_devices: tuple[str, ...]
    allocation_source: str
    allowed_gpu_uuids: tuple[str, ...]
    selected_gpu_uuids: tuple[str, ...]

    @property
    def count(self):
        return len(self.selected_gpu_uuids)

    def report(self):
        return dict(asdict(self), resolved_count=self.count,
                    adaptation="deployment capacity only; no performance tuning")

    @contextmanager
    def visibility(self):
        """Only use before CUDA/Ray/Daft initialization in an owned process."""
        previous = os.environ.get("CUDA_VISIBLE_DEVICES")
        os.environ["CUDA_VISIBLE_DEVICES"] = ",".join(self.selected_gpu_uuids)
        try:
            yield
        finally:
            if previous is None:
                os.environ.pop("CUDA_VISIBLE_DEVICES", None)
            else:
                os.environ["CUDA_VISIBLE_DEVICES"] = previous


def resolve_gpu_allocation(count="auto", allowed_gpus=None, *, environ=None, probe=None):
    """Intersect declared allocation with inherited masks, then freeze UUIDs.

    GPU_IDS/allowed_gpus use nvidia-smi's physical namespace. Inherited CUDA
    masks must use UUIDs: numeric CUDA ordinals can differ from NVML indices,
    so guessing their mapping could widen an allocation. Legacy callers which
    do not opt into this resolver retain their existing single-device defaults.
    """
    if count != "auto" and (type(count) is not int or not 1 <= count <= 256):
        raise ValueError("GPU count must be auto or an integer from 1 to 256")
    env, query = (os.environ if environ is None else environ), (probe or _probe_uuid)
    if allowed_gpus is not None:
        source, tokens = "allowed_gpus", _tokens(allowed_gpus, "allowed_gpus")
    elif "GPU_IDS" in env:
        source, tokens = "GPU_IDS", _tokens(env["GPU_IDS"], "GPU_IDS")
    elif "CUDA_VISIBLE_DEVICES" in env:
        source, tokens = "CUDA_VISIBLE_DEVICES", _tokens(env["CUDA_VISIBLE_DEVICES"], "CUDA_VISIBLE_DEVICES")
    else:
        raise ValueError("auto/scoped resources require allowed_gpus, GPU_IDS or a UUID CUDA_VISIBLE_DEVICES allocation")
    masks = []
    for name in ("CUDA_VISIBLE_DEVICES", "NVIDIA_VISIBLE_DEVICES"):
        if name not in env:
            continue
        if name == "NVIDIA_VISIBLE_DEVICES" and env[name] == "all":
            continue  # An explicit narrower allocation is still required above.
        mask = _tokens(env[name], name)
        if any(not token.startswith("GPU-") for token in mask):
            raise ValueError(name + " must use full GPU UUIDs for scoped auto; numeric CUDA/container ordinals are ambiguous")
        masks.append(mask)
    if masks and any(not token.startswith("GPU-") for token in tokens):
        raise ValueError("an inherited UUID mask requires UUID allowed_gpus/GPU_IDS; physical indices cannot be resolved outside that mask")
    # Filter UUID declarations before probing. Numeric physical IDs are only
    # queried when there is no inherited mask requiring a prior UUID mapping.
    candidates = [token for token in tokens if not token.startswith("GPU-")
                  or all(token in mask for mask in masks)]
    uuids = tuple(query(token) for token in candidates)
    if (any(type(x) is not str or not re.fullmatch(r"GPU-[A-Za-z0-9-]+", x) for x in uuids)
            or len(set(uuids)) != len(uuids)):
        raise ValueError("allocated GPU identities are invalid or duplicate physical devices")
    permitted = tuple(uid for uid in uuids if all(uid in mask for mask in masks))
    wanted = len(permitted) if count == "auto" else count
    if not permitted or wanted > len(permitted):
        raise ValueError("requested GPU count exceeds the deployment allocation after visibility intersection")
    return GpuAllocation(count, tokens, source, permitted, permitted[:wanted])
