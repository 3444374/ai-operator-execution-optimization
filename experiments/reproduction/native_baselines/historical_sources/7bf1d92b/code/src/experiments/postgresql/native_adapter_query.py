"""Shared runtime and finite-source checks for LOTUS query comparisons."""

from dataclasses import replace
import json
import os
import uuid

from src.execution_provider.adapters.ray_map_transport import _clock_domain
from src.experiments.execution_timing import timed_call
from .query_config import NativeRayRuntimeConfig
from .query_execution import native_ray_runtime

_PROCESS_CLOCK = "process-clock-" + uuid.uuid4().hex


def resolve_adapter_limits(options, max_held_tasks=None):
    held = options.concurrency if max_held_tasks is None else max_held_tasks
    if type(held) is not int or not 1 <= held <= 256:
        raise ValueError("adapter held tasks must be from 1 to 256")
    if held < options.concurrency:
        raise ValueError("held tasks must cover the active request capacity")
    return held


def call_clock_domain():
    return _clock_domain() or _PROCESS_CLOCK


def load_bounded_rows(load_source, *, max_rows=4096, max_source_bytes=64 * 1024 * 1024):
    if type(max_rows) is not int or not 1 <= max_rows <= 4096:
        raise ValueError("external source row allowance must be bounded")
    if type(max_source_bytes) is not int or not 1 <= max_source_bytes <= 64 * 1024 * 1024:
        raise ValueError("external source byte allowance must be bounded")
    values, seen = [], set()
    size = 0
    for row in load_source():
        if len(values) == max_rows:
            raise ValueError("external producer exceeded its declared row allowance")
        if (
            not isinstance(row, dict)
            or set(row) != {"row_id", "text"}
            or not isinstance(row["row_id"], str)
            or not 1 <= len(row["row_id"].encode()) <= 128
            or row["row_id"] in seen
            or "\x00" in row["row_id"]
            or not isinstance(row["text"], str)
            or "\x00" in row["text"]
            or len(row["text"].encode()) > 65536
        ):
            raise ValueError("external source requires unique bounded row identities and raw text")
        size += len(json.dumps(row, ensure_ascii=False).encode())
        if size > max_source_bytes:
            raise ValueError("external producer exceeded its declared byte allowance")
        seen.add(row["row_id"])
        values.append(row)
    if not values:
        raise ValueError("this comparison requires nonempty external input")
    return values


@timed_call("runtime.prepare_call")
def _runtime(stack, arm, options, physical, ray_temp_root, ray_address):
    if arm not in ("fixed-map-native-daft", "fixed-map-native-ray", "fixed-map-semloom"):
        raise ValueError("unsupported LOTUS runtime role")
    if arm == "fixed-map-native-daft":
        from src.baselines.text.frameworks.daft_pg_http import prepare_runtime

        prepare_runtime(options.num_threads)
        return None, dict(
            owner="Daft Native",
            version="0.7.21",
            num_threads=options.num_threads,
            source="external finite prepared calls",
        )
    if arm == "fixed-map-semloom" and physical is not None and physical.payload_backend == "daft":
        from src.baselines.text.frameworks.daft_pg_http import prepare_runtime

        prepare_runtime(options.num_threads)
    if arm == "fixed-map-native-ray":
        options.validate_backend("ray-data")
    config = NativeRayRuntimeConfig(
        ray_num_cpus=options.num_threads,
        ray_async_batches_per_actor=options.ray_async_batches_per_actor,
        ray_address=ray_address,
    )
    os.environ["RAY_DATA_DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY"] = str(
        options.ray_async_batches_per_actor
    )
    import ray

    runtime = stack.enter_context(native_ray_runtime(ray, config, ray_temp_root))
    if arm == "fixed-map-semloom":
        physical = replace(
            physical, address=ray.get_runtime_context().gcs_address, response_mode="full"
        )
    owner = "Ray Data"
    if arm == "fixed-map-semloom":
        owner = "SemLoom Daft/Ray" if physical.payload_backend == "daft" else "SemLoom Arrow/Ray"
    # The context updates this same report when its driver disconnects on exit.
    runtime.update(owner=owner, version=ray.__version__, source="external finite prepared calls")
    return physical, runtime
