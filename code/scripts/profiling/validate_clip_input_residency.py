#!/usr/bin/env python3
"""Qualify actual CLIP input reuse, or observe the unchanged image CLI.

``qualify`` materializes exactly 64 real JPEGs on the driver for bounded
correctness/lifecycle checks. It is not a performance measurement. ``wrap``
accepts only the SemLoom project_ray arm, replaces its input actor in this
experiment process, and writes actor observations outside the original CLI's
measurement window. All original outputs, batching and timing remain owned by
the supplied CLI. CUDA events are an optional, separately labelled diagnostic.
Neither --help nor --preview imports Ray, Torch or model libraries.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import importlib.util
import json
import math
import os
import subprocess
import sys
import time
from dataclasses import asdict
from pathlib import Path, PurePosixPath


CODE_ROOT = next(parent for parent in Path(__file__).resolve().parents
                 if (parent / "src").is_dir())
QUALIFICATION_ROWS = 64
QUALIFICATION_CASES = ("first_miss", "new_rows_hit", "content_change_miss", "original_reload")


def _option(argv, name):
    values = []
    for index, token in enumerate(argv):
        if token == name:
            if index + 1 == len(argv) or argv[index + 1].startswith("--"):
                raise ValueError(f"{name} requires a value")
            values.append(argv[index + 1])
        elif token.startswith(name + "="):
            values.append(token.split("=", 1)[1])
        elif token.startswith("--") and token != "--" and name.startswith(token):
            raise ValueError(f"spell {name} in full")
    if len(values) > 1:
        raise ValueError(f"{name} must not be repeated")
    return values[0] if values else None


def validate_base_argv(argv):
    if _option(argv, "--arm") != "project_ray":
        raise ValueError("wrap requires exactly one explicit --arm project_ray")
    workers = _option(argv, "--gpu-workers")
    if workers is not None and workers != "1":
        raise ValueError("this validation scope supports one GPU worker")
    if any(item.split("=", 1)[0] == "--detailed-stage-timing" for item in argv):
        raise ValueError("use wrapper --cuda-events for separate diagnostics")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    modes = parser.add_subparsers(dest="mode", required=True)
    qualify = modes.add_parser("qualify", allow_abbrev=False)
    qualify.add_argument("--model", required=True, type=Path)
    qualify.add_argument("--processor", type=Path)
    qualify.add_argument("--image-root", required=True, type=Path)
    qualify.add_argument("--image-list", required=True, type=Path,
                         help="COCO file manifest, or JSON array of relative JPEG paths")
    qualify.add_argument("--dtype", choices=("float16", "float32", "bfloat16"), default="float16")
    qualify.add_argument("--cpu-budget", type=int, default=16)
    qualify.add_argument("--ray-address")
    qualify.add_argument("--ray-temp-dir", type=Path)
    qualify.add_argument("--rtol", type=float, default=1e-5)
    qualify.add_argument("--atol", type=float, default=1e-5)
    wrap = modes.add_parser("wrap", allow_abbrev=False)
    wrap.add_argument("--base-cli", required=True, type=Path)
    wrap.add_argument("--cache-bytes", required=True, type=int)
    wrap.add_argument("--cache-entries", type=int, default=8)
    wrap.add_argument("--cuda-events", action="store_true")
    topology = modes.add_parser("topology", allow_abbrev=False,
                                help="inspect hardware peer-access declarations; no model or copy")
    topology.add_argument("--output", required=True, dest="observations", type=Path)
    for mode in (qualify, wrap):
        mode.add_argument("--observations", required=True, type=Path)
    for mode in (qualify, wrap, topology):
        mode.add_argument("--timeout-s", type=float, default=120.0)
        mode.add_argument("--preview", action="store_true")
    wrap.add_argument("base_argv", nargs=argparse.REMAINDER, help="-- followed by original CLI arguments")
    args = parser.parse_args(argv)
    if not math.isfinite(args.timeout_s) or args.timeout_s <= 0:
        parser.error("--timeout-s must be finite and positive")
    if args.mode == "wrap":
        if args.base_argv and args.base_argv[0] == "--":
            args.base_argv = args.base_argv[1:]
        if args.cache_bytes < 0 or args.cache_entries < 1:
            parser.error("cache bytes must be non-negative and entries positive")
        try:
            validate_base_argv(args.base_argv)
        except ValueError as error:
            parser.error(str(error))
    elif args.mode == "qualify" and (args.cpu_budget < 1 or any(
            not math.isfinite(value) or value < 0 for value in (args.atol, args.rtol))):
        parser.error("CPU budget must be positive and tolerances finite and non-negative")
    return args


@contextlib.contextmanager
def _offline():
    names = ("HF_HUB_OFFLINE", "TRANSFORMERS_OFFLINE")
    previous = {name: os.environ.get(name) for name in names}
    os.environ.update({name: "1" for name in names})
    try:
        yield
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def _activate_code(path):
    code_root = next(parent for parent in path.resolve(strict=True).parents
                     if (parent / "src").is_dir())
    sys.path.insert(0, str(code_root))
    loaded = sys.modules.get("src")
    if loaded is not None and Path(loaded.__file__).resolve().parent != code_root / "src":
        raise RuntimeError("launch this wrapper in a fresh process using the base CLI's code root")
    return code_root


def _source_manifest(base_cli=None):
    sources = {"validation_script": Path(__file__).resolve()}
    if base_cli is not None:
        sources["base_cli"] = base_cli.resolve()
    sources.update({name: Path(module.__file__).resolve()
                    for name, module in list(sys.modules.items())
                    if name.startswith("src.") and getattr(module, "__file__", "").endswith(".py")})
    return {name: {"file": path.name, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
            for name, path in sorted(sources.items())}


def _nvml_device_memory():
    """Device totals via nvidia-smi's NVML query; no cache attribution."""
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=index,uuid,memory.used,memory.total",
             "--format=csv,noheader,nounits"], text=True, capture_output=True,
            timeout=10, check=True)
        devices = []
        for row in result.stdout.splitlines():
            index, uuid, used, total = (part.strip() for part in row.split(","))
            devices.append({"index": int(index), "uuid": uuid,
                            "memory_used_mib": float(used), "memory_total_mib": float(total)})
        return {"source": "nvidia-smi_NVML_device_totals", "devices": devices}
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        return {"source": "nvidia-smi_NVML_device_totals", "unavailable": type(error).__name__}


def make_measured_actor(cache_bytes, cache_entries=8, cuda_events=False):
    """Build one experiment-only actor class; never patch a native baseline."""
    import numpy as np

    from src.modalities.image.reusable_clip import ReusableClipTensorActor
    from src.modalities.image.clip import extract_clip_image_features, l2_normalize_embeddings

    class TorchObserver:
        def __init__(self, torch, owner):
            self.torch, self.owner = torch, owner

        def __getattr__(self, name):
            return getattr(self.torch, name)

        def as_tensor(self, *args, **kwargs):
            if self.owner._trace is not None:
                # Observe the owned host buffer actually passed by _device_input.
                # A read-only source alone does not imply a copy on a cache hit.
                copied = not np.shares_memory(self.owner._host_input_source, args[0])
                self.owner._trace["host_ownership_copy_bytes"] = int(args[0].nbytes) if copied else 0
            return self.owner._observe_stage("as_tensor", self.torch.as_tensor, *args, **kwargs)

    class ModelObserver:
        def __init__(self, model, owner):
            self.model, self.owner = model, owner

        def __getattr__(self, name):
            return getattr(self.model, name)

        def get_image_features(self, *args, **kwargs):
            self.owner._forward_calls += 1
            return self.owner._observe_stage("clip_forward", self.model.get_image_features, *args, **kwargs)

    class MeasuredReusableClipActor(ReusableClipTensorActor):
        def __init__(self, *args, **kwargs):
            if kwargs.get("detailed_stage_timing", False):
                raise ValueError("global stage synchronization is excluded from this wrapper")
            kwargs.update(input_cache_bytes=cache_bytes, input_cache_entries=cache_entries,
                          local_files_only=True)
            super().__init__(*args, **kwargs)
            self._records, self._forward_calls = [], 0
            self._trace, self._qualification_fault = None, None
            self._cancel_flag_observed = False
            self._initial_memory = self._memory()
            self._torch.cuda.reset_peak_memory_stats(self._device)
            self._torch = TorchObserver(self._torch, self)
            self._model = ModelObserver(self._model, self)

        def _memory(self):
            cuda = self._torch.cuda
            return {"allocated_bytes": cuda.memory_allocated(self._device),
                    "reserved_bytes": cuda.memory_reserved(self._device),
                    "max_allocated_bytes": cuda.max_memory_allocated(self._device),
                    "max_reserved_bytes": cuda.max_memory_reserved(self._device)}

        def ready(self):
            import ray
            import transformers
            context = ray.get_runtime_context()
            return {**super().ready(), "ray_actor_id": str(context.get_actor_id()),
                    "ray_node_id": str(context.get_node_id()),
                    "accelerator_ids": context.get_accelerator_ids(),
                    "cuda_visible_devices": os.environ.get("CUDA_VISIBLE_DEVICES"),
                    "torch_device": self._device,
                    "gpu_name": self._torch.cuda.get_device_name(self._device),
                    "versions": {"torch": self._torch.__version__, "ray": ray.__version__,
                                 "transformers": transformers.__version__},
                    "semantics": asdict(self.semantics)}

        def _observe_stage(self, stage, function, *args, **kwargs):
            events = None
            if self._trace is not None and cuda_events:
                start, end = (self._torch.cuda.Event(enable_timing=True) for _ in range(2))
                start.record(self._torch.cuda.current_stream(self._device))
                events = (start, end)
            started = time.perf_counter()
            try:
                return function(*args, **kwargs)
            finally:
                if events is not None:
                    events[1].record(self._torch.cuda.current_stream(self._device))
                if self._trace is not None:
                    self._trace[stage + "_host_call_s"] = time.perf_counter() - started
                    if events is not None:
                        self._trace[stage + "_events"] = events

        def _device_input(self, payload):
            self._host_input_source = payload
            try:
                result = super()._device_input(payload)
                if self._trace is not None:
                    self._trace["host_copy_s"] = result[1]
                    observation = self.input_reuse_observation()
                    if self._cache_enabled and observation is not None:
                        self._trace["host_ownership_copy_bytes"] = observation.host_ownership_copy_bytes
                return result
            finally:
                self._host_input_source = None

        def embed(self, batch):
            self._trace = {"row_count": len(batch.doc_ids),
                           "row_ids_sha256": hashlib.sha256(json.dumps(batch.doc_ids).encode()).hexdigest(),
                           "input_host_bytes": int(batch.payload.nbytes),
                           "host_ownership_copy_bytes": 0}
            started = time.perf_counter()
            try:
                result = super().embed(batch)
                self._trace["service_s"] = result.service_s
                self._trace["output_bytes"] = result.telemetry.output_bytes
                return result
            except BaseException as error:
                self._trace["error_type"] = type(error).__name__
                raise
            finally:
                self._trace["embed_wall_s"] = time.perf_counter() - started
                observation = self.input_reuse_observation()
                self._trace["input_reuse_observation"] = asdict(observation) if observation else None
                self._records.append(self._trace)
                self._trace = None

        def residency_measurement_snapshot(self):
            records = []
            for trace in self._records:
                record = {key: value for key, value in trace.items() if not key.endswith("_events")}
                for key, value in trace.items():
                    if key.endswith("_events"):
                        start, end = value
                        done = end.query()
                        record[key[:-7] + "_event_done"] = done
                        record[key[:-7] + "_cuda_s"] = start.elapsed_time(end) / 1000.0 if done else None
                records.append(record)
            return {"ready": self.ready(), "cache": asdict(self.input_residency_snapshot()),
                    "torch_initial_after_model_load": self._initial_memory,
                    "torch_memory": self._memory(), "peak_scope": "since_actor_model_loaded",
                    "forward_calls": self._forward_calls, "calls": records,
                    "cancel_flag_observed": self._cancel_flag_observed,
                    "input_completion_event_retained": self._input_completion_event is not None,
                    "cuda_events_enabled": cuda_events,
                    "stage_semantics": "host_call_s_is_launch_wall; cuda_event_s_is_device_stream_elapsed"}

        def qualification_fault(self, batch, kind, marker=None, wait_s=30.0):
            self._qualification_fault = (kind, marker, wait_s)
            try:
                return self.embed(batch)
            finally:
                self._qualification_fault = None

        def _confirm_input_completion(self):
            if self._qualification_fault is not None:
                kind, marker, wait_s = self._qualification_fault
                event = self._torch.cuda.Event()
                self._input_completion_event = event
                event.record(self._torch.cuda.current_stream(self._device))
                if kind == "acknowledgement":
                    raise RuntimeError("controlled acknowledgement failure after real event.record; not a hardware error")
                if kind == "cooperative_cancel":
                    import ray
                    context = ray.get_runtime_context()
                    Path(marker).write_text("awaiting_real_ray_cancel\n", encoding="utf-8")
                    deadline = time.monotonic() + wait_s
                    while time.monotonic() < deadline:
                        if context.is_canceled():
                            self._cancel_flag_observed = True
                            raise asyncio.CancelledError("observed real Ray task cancellation")
                        time.sleep(0.02)
                    raise TimeoutError("real Ray cancellation flag was not observed")
                raise ValueError("unknown qualification fault")
            return super()._confirm_input_completion()

        def qualification_async_forward(self, batch):
            # Keep the normal whole-batch input lease through the actual CLIP
            # forward. Omit only D2H here so query() can observe its true state.
            pixels, _, _ = self._device_input(batch.payload)
            key = self.input_reuse_observation().key
            self._duplicate_lease = self._inputs.acquire(
                key, logical_bytes=pixels.numel() * pixels.element_size(),
                load=lambda: (_ for _ in ()).throw(AssertionError("duplicate reference must hit")))
            with self._torch.inference_mode():
                self._async_features = l2_normalize_embeddings(
                    extract_clip_image_features(self._model.get_image_features(pixel_values=pixels)).float())
            self._async_ids = batch.doc_ids
            self._async_event = self._torch.cuda.Event()
            self._async_event.record(self._torch.cuda.current_stream(self._device))
            return {"event_done_at_query": self._async_event.query(),
                    "cache": asdict(self.input_residency_snapshot()),
                    "torch_memory": self._memory()}

        def qualification_complete_async(self):
            self._async_event.synchronize()
            self._finish_input(succeeded=True)
            self._inputs.release(self._duplicate_lease)
            embeddings = self._async_features.cpu().numpy()
            self._duplicate_lease = self._async_features = self._async_event = None
            return {"doc_ids": self._async_ids, "embeddings": embeddings,
                    "cache": asdict(self.input_residency_snapshot())}

    return MeasuredReusableClipActor


def _read_qualification_images(args):
    raw = args.image_list.read_bytes()
    manifest = json.loads(raw)
    if isinstance(manifest, dict) and (manifest.get("source_kind") != "coco_jpeg_files"
                                      or type(manifest.get("schema_version")) is not int
                                      or manifest["schema_version"] != 1):
        raise ValueError("qualification requires a version-one COCO file manifest")
    entries = manifest.get("entries") if isinstance(manifest, dict) else manifest
    if not isinstance(entries, list) or len(entries) < QUALIFICATION_ROWS:
        raise ValueError("qualification requires at least 64 real JPEG manifest entries")
    root = args.image_root.resolve(strict=True)
    if not root.is_dir():
        raise ValueError("image root must be a directory")
    ids, encoded, identities = [], [], []
    for item in entries[:QUALIFICATION_ROWS]:
        relative = item["path"] if isinstance(item, dict) else item
        if not isinstance(relative, str):
            raise ValueError("relative JPEG paths must be strings")
        relative_path = PurePosixPath(relative)
        if (relative_path.is_absolute() or ".." in relative_path.parts or "\\" in relative
                or relative_path.suffix.lower() not in (".jpg", ".jpeg")):
            raise ValueError("qualification requires relative JPEGs below image root")
        path = root
        for component in relative_path.parts:
            path /= component
            if path.is_symlink():
                raise ValueError("qualification source must not contain symlinks")
        path = path.resolve(strict=True)
        if not path.is_relative_to(root) or not path.is_file():
            raise ValueError("qualification image escaped its root or is not a regular file")
        before = path.stat()
        content = path.read_bytes()
        after = path.stat()
        digest = hashlib.sha256(content).hexdigest()
        if content[:3] != b"\xff\xd8\xff":
            raise ValueError("qualification requires actual JPEG encoded input")
        if isinstance(item, dict) and (item.get("image_bytes") != len(content) or item.get("sha256") != digest):
            raise ValueError("qualification content differs from fixed file manifest")
        identity = (before.st_size, before.st_mtime_ns, before.st_ctime_ns)
        if identity != (after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            raise ValueError("qualification image changed during read")
        if isinstance(item, dict) and identity != (item.get("image_bytes"), item.get("mtime_ns"), item.get("ctime_ns")):
            raise ValueError("qualification fixed file manifest metadata changed")
        doc_id = str(item["doc_id"]) if isinstance(item, dict) else relative_path.stem
        ids.append(doc_id)
        encoded.append(content)
        identities.append({"doc_id": doc_id, "path": relative, "sha256": digest, "bytes": len(content)})
    if len(set(ids)) != QUALIFICATION_ROWS:
        raise ValueError("qualification requires 64 unique source row IDs")
    return tuple(ids), encoded, {"manifest_sha256": hashlib.sha256(raw).hexdigest(),
                                 "selection": "first_64_entries", "images": identities}


def validate_output(result, ids):
    import numpy as np
    matrix = np.asarray(result.embeddings)
    if (tuple(result.doc_ids) != tuple(ids) or matrix.shape != (QUALIFICATION_ROWS, 512)
            or matrix.dtype != np.float32 or not np.isfinite(matrix).all()
            or not result.semantics.projected or not result.semantics.normalized
            or result.semantics.dtype != "float32"):
        raise AssertionError("full current-row-ID normalized float32 512-D contract changed")
    np.testing.assert_allclose(np.linalg.norm(matrix, axis=1), 1.0, rtol=1e-5, atol=1e-5)
    return {"row_count": len(ids), "shape": list(matrix.shape), "dtype": str(matrix.dtype),
            "row_ids_sha256": hashlib.sha256(json.dumps(tuple(ids)).encode()).hexdigest(),
            "norm_error_max": float(np.max(np.abs(np.linalg.norm(matrix, axis=1) - 1.0)))}


def _assert_held(snapshot, logical_bytes, references, unknown):
    cache = snapshot["cache"]
    if (cache["resident_bytes"], cache["held_references"], cache["unknown_references"]) != (
            logical_bytes, references, unknown):
        raise AssertionError("completion-owned input references or bytes were released early")


def run_qualification(args, evidence):
    sys.path.insert(0, str(CODE_ROOT))
    import numpy as np
    import ray
    from src.modalities.image.clip import FastClipImagePreprocessor
    from src.modalities.image.contracts import ImageEmbeddingBatch
    from src.infrastructure.runtime_env import ray_runtime_env
    for path in (args.model, args.processor or args.model):
        if not path.is_dir():
            raise ValueError("qualification requires a supplied local offline model/processor directory")
    ids, encoded, source = _read_qualification_images(args)
    started = time.perf_counter()
    preprocessor = FastClipImagePreprocessor(str(args.processor or args.model),
                                            torch_intraop_threads=1, torch_interop_threads=1)
    pixels = preprocessor.preprocess(encoded)
    if pixels.shape != (QUALIFICATION_ROWS, 3, 224, 224) or pixels.dtype != np.float32:
        raise AssertionError("qualification prepared-input contract changed")
    evidence.update(source=source, driver_preprocess_s=time.perf_counter() - started,
                    input_tensor_bytes=int(pixels.nbytes), qualification_only=True,
                    cpu_budget=args.cpu_budget, phases=[], checks=[])
    changed = pixels.copy()
    changed[0, 0, 0, 0] += np.float32(0.25)
    batches = tuple(ImageEmbeddingBatch(
        tuple(f"{row}#pass={index + 1}" for row in ids) if index else ids,
        changed if index == 2 else pixels, "preprocessed_tensor", QUALIFICATION_ROWS * 224 * 224,
        "pixels") for index in range(4))
    logical_bytes = int(pixels.size * {"float16": 2, "float32": 4, "bfloat16": 2}[args.dtype])
    evidence["one_batch_logical_device_bytes"] = logical_bytes
    init = {"address": args.ray_address} if args.ray_address else {
        "num_cpus": args.cpu_budget, "num_gpus": 1,
        **({"_temp_dir": str(args.ray_temp_dir)} if args.ray_temp_dir else {})}
    runtime_env = ray_runtime_env(CODE_ROOT)
    runtime_env["env_vars"].update(HF_HUB_OFFLINE="1", TRANSFORMERS_OFFLINE="1")
    ray.init(include_dashboard=False, runtime_env=runtime_env, **init)
    outputs, actor = {}, None
    get = lambda ref: ray.get(ref, timeout=args.timeout_s)
    try:
        references = {}
        for mode, budget in (("off", 0), ("on", logical_bytes)):
            actor_type = make_measured_actor(budget, cache_entries=1, cuda_events=True)
            remote = ray.remote(num_cpus=1, num_gpus=1, max_restarts=0, max_task_retries=0)(actor_type)
            actor = remote.remote(str(args.model), processor_revision=str(args.processor or args.model),
                                  dtype=args.dtype, normalize=True,
                                  torch_intraop_threads=1, torch_interop_threads=1)
            get(actor.ready.remote())
            evidence["physical_device_validation"] = True
            for index, batch in enumerate(batches):
                result = get(actor.embed.remote(ray.put(batch)))
                check = {"case": QUALIFICATION_CASES[index], "mode": mode,
                         **validate_output(result, batch.doc_ids)}
                outputs[mode + "_" + QUALIFICATION_CASES[index]] = result.embeddings
                if mode == "off":
                    references[index] = result
                else:
                    if result.semantics != references[index].semantics:
                        raise AssertionError("off/on model/processor/output semantics changed")
                    np.testing.assert_allclose(result.embeddings, references[index].embeddings,
                                               rtol=args.rtol, atol=args.atol)
                    check["off_on_allclose"] = True
                evidence["checks"].append(check)
            snapshot = get(actor.residency_measurement_snapshot.remote())
            if snapshot["forward_calls"] != 4:
                raise AssertionError("input reuse shortened or duplicated full CLIP forward")
            if mode == "on":
                cache = snapshot["cache"]
                if (cache["hits"], cache["misses"], cache["evictions"], cache["resident_bytes"]) != (
                        1, 3, 2, logical_bytes):
                    raise AssertionError("actual miss/hit/change/eviction sequence was not observed")
                if [call["input_reuse_observation"]["transfer"]["reuse_available"]
                    for call in snapshot["calls"]] != [False, True, False, False]:
                    raise AssertionError("actual transfer observations disagree with the qualification cases")
                _run_lifecycle_qualification(args, evidence, actor, batches[0], logical_bytes, get, outputs, references[0])
            evidence["phases"].append({"mode": mode, "snapshot": get(actor.residency_measurement_snapshot.remote()),
                                        "nvml_device_memory": _nvml_device_memory()})
            ray.kill(actor, no_restart=True)
            actor = None
        evidence["passed"] = True
    finally:
        if actor is not None:
            ray.kill(actor, no_restart=True)
        ray.shutdown()
        output_path = args.observations.with_suffix(".embeddings.npz")
        with output_path.open("xb") as stream:
            np.savez(stream, **outputs)
        evidence["embedding_artifact"] = {"file": output_path.name,
                                           "sha256": hashlib.sha256(output_path.read_bytes()).hexdigest()}


def _run_lifecycle_qualification(args, evidence, actor, batch, logical_bytes, get, outputs, reference):
    import numpy as np
    import ray
    from types import SimpleNamespace
    evidence["lifecycle"] = []
    for kind in ("acknowledgement", "cooperative_cancel"):
        marker = args.observations.with_suffix(".cancel-marker")
        ref = actor.qualification_fault.remote(ray.put(batch), kind, str(marker), min(30.0, args.timeout_s))
        if kind == "cooperative_cancel":
            deadline = time.monotonic() + min(30.0, args.timeout_s)
            while not marker.exists():
                if time.monotonic() > deadline:
                    raise TimeoutError("qualification actor did not reach cancellation marker")
                time.sleep(0.02)
            ray.cancel(ref, force=False, recursive=False)
        try:
            get(ref)
        except Exception as error:
            error_type = type(error).__name__
        else:
            raise AssertionError("qualification completion interruption did not occur")
        held = get(actor.residency_measurement_snapshot.remote())
        if kind == "cooperative_cancel" and (held["cancel_flag_observed"] is not True
                                              or held["calls"][-1].get("error_type") != "CancelledError"):
            raise AssertionError("actual actor did not observe real Ray cancellation")
        if not held["input_completion_event_retained"]:
            raise AssertionError("interrupted acknowledgement dropped its real CUDA event")
        _assert_held(held, logical_bytes, 1, 1)
        get(actor.clear_input_residency.remote())
        cleared = get(actor.residency_measurement_snapshot.remote())
        _assert_held(cleared, logical_bytes, 1, 1)
        get(actor.synchronize.remote())
        settled = get(actor.residency_measurement_snapshot.remote())
        _assert_held(settled, 0, 0, 0)
        evidence["lifecycle"].append({"case": kind, "driver_error_type": error_type,
                                     "actor_error_type": held["calls"][-1].get("error_type"),
                                     "real_cancel_flag_observed": held["cancel_flag_observed"] if kind == "cooperative_cancel" else None,
                                     "real_cuda_event_retained": held["input_completion_event_retained"],
                                     "held": held["cache"], "after_clear": cleared["cache"],
                                     "after_actual_owner_synchronize": settled["cache"],
                                     "hardware_fault_injected": False})
    pending = get(actor.qualification_async_forward.remote(ray.put(batch)))
    _assert_held(pending, logical_bytes, 2, 0)
    get(actor.clear_input_residency.remote())
    cleared = get(actor.residency_measurement_snapshot.remote())
    _assert_held(cleared, logical_bytes, 2, 0)
    completed = get(actor.qualification_complete_async.remote())
    _assert_held(completed, 0, 0, 0)
    validate_output(SimpleNamespace(**completed, semantics=reference.semantics), batch.doc_ids)
    np.testing.assert_allclose(completed["embeddings"], reference.embeddings,
                               rtol=args.rtol, atol=args.atol)
    outputs["async_full_forward"] = completed["embeddings"]
    evidence["lifecycle"].append({"case": "async_full_forward_duplicate_reference",
                                 "event_done_at_first_query": pending["event_done_at_query"],
                                 "held": pending["cache"], "after_clear": cleared["cache"],
                                 "after_event_confirmation": completed["cache"],
                                 "off_on_allclose": True, "artificial_cuda_delay": False})


def run_wrapper(args, evidence):
    _activate_code(args.base_cli)
    from src.modalities.image import clip
    validate_base_argv(args.base_argv)
    spec = importlib.util.spec_from_file_location("clip_residency_base_cli", args.base_cli)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    original_actor, original_stop, original_parse = clip.ClipTensorActor, module.stop_semloom_ray_worker_pool, module.parse_args
    clip.ClipTensorActor = make_measured_actor(args.cache_bytes, args.cache_entries, args.cuda_events)
    pools = []

    def checked_parse(*values, **kwargs):
        parsed = original_parse(*values, **kwargs)
        if parsed.arm != "project_ray" or parsed.gpu_workers != 1 or parsed.detailed_stage_timing:
            raise ValueError("wrapper only supports one-GPU project_ray without intrusive stage timing")
        for value in (parsed.model, parsed.processor or parsed.model):
            if not Path(value).is_dir():
                raise ValueError("wrapper requires supplied local offline model/processor directories")
        return parsed

    def observed_stop(pool):
        import ray
        observation = {"pool_index": len(pools), "phase": "warmup" if not pools else "formal",
                       "outside_original_measurement_window": True}
        try:
            observation["actors"] = ray.get(
                [actor.residency_measurement_snapshot.remote() for actor in pool.gpu_actors],
                timeout=args.timeout_s)
            observation["nvml_device_memory"] = _nvml_device_memory()
        except Exception as error:
            observation["snapshot_error_type"] = type(error).__name__
        finally:
            pools.append(observation)
            original_stop(pool)

    previous_argv = sys.argv
    module.parse_args, module.stop_semloom_ray_worker_pool = checked_parse, observed_stop
    sys.argv = [str(args.base_cli), *args.base_argv]
    evidence.update(pools=pools, cache_bytes=args.cache_bytes, cache_entries=args.cache_entries,
                    cuda_events_diagnostic=args.cuda_events,
                    original_cli_owns="output_contract_capture_batching_resources_timing",
                    physical_memory_semantics="torch_allocator_and_NVML_device_totals_are_not_logical_cache_bytes")
    try:
        started = time.perf_counter()
        module.main()
        evidence["original_cli_main_wall_s"] = time.perf_counter() - started
        if not pools or any("snapshot_error_type" in pool for pool in pools):
            raise RuntimeError("original CLI completed but actor observations are incomplete")
        evidence["physical_device_validation"] = True
        evidence["passed"] = True
    finally:
        evidence["source_manifest"] = _source_manifest(args.base_cli)
        clip.ClipTensorActor = original_actor
        module.parse_args, module.stop_semloom_ray_worker_pool = original_parse, original_stop
        sys.argv = previous_argv
        ray = sys.modules.get("ray")
        if ray is not None and ray.is_initialized():
            evidence["cleanup_completion"] = "device_state_unknown_on_original_cli_failure"
            ray.shutdown()


def run_topology(evidence):
    import torch
    count = torch.cuda.device_count()
    evidence.update(visible_cuda_devices=count, physical_device_validation=count > 0,
                    semloom_peer_copy_implemented=False,
                    fallback="host_reload_on_destination",
                    hardware_peer_access_semantics="driver_declaration_not_transport_implementation",
                    two_devices_required=count < 2, status="pending" if count < 2 else "hardware_inspected")
    evidence["directional_peer_access"] = [
        {"source": source, "destination": destination,
         "can_access_peer": torch.cuda.can_device_access_peer(source, destination)}
        for source in range(count) for destination in range(count) if source != destination]
    try:
        result = subprocess.run(["nvidia-smi", "topo", "-m"], text=True, capture_output=True,
                                timeout=10, check=True)
        evidence["nvidia_smi_topology"] = result.stdout
    except (OSError, subprocess.SubprocessError) as error:
        evidence["nvidia_smi_topology_unavailable"] = type(error).__name__
    evidence["passed"] = True


def main(argv=None):
    args = parse_args(argv)
    invoked = time.perf_counter()
    if args.preview:
        print(json.dumps({"mode": args.mode, "preview_only": True, "models_started": 0,
                          "qualification_rows": QUALIFICATION_ROWS if args.mode == "qualify" else None,
                          "cache_bytes": getattr(args, "cache_bytes", None),
                          "cuda_events_diagnostic": getattr(args, "cuda_events", False)}, sort_keys=True))
        return
    args.observations.parent.mkdir(parents=True, exist_ok=True)
    if args.observations.exists() or (args.mode == "qualify" and any(
            args.observations.with_suffix(suffix).exists() for suffix in (".embeddings.npz", ".cancel-marker"))):
        raise FileExistsError("refuse to overwrite existing validation evidence")
    if args.mode == "wrap":
        _activate_code(args.base_cli)
    else:
        sys.path.insert(0, str(CODE_ROOT))
    from src.baselines.common.redact import redact_argument_list, redact_json_values, redact_text
    evidence = {"schema_version": 1, "mode": args.mode, "passed": False,
                "physical_device_validation": False, "started_epoch_s": time.time(),
                "argv": [redact_text(item) for item in redact_argument_list(list(argv or sys.argv[1:]))]}
    try:
        with _offline():
            if args.mode == "qualify":
                run_qualification(args, evidence)
                evidence["source_manifest"] = _source_manifest()
            elif args.mode == "wrap":
                run_wrapper(args, evidence)
            else:
                run_topology(evidence)
    except BaseException as error:
        evidence["error"] = {"type": type(error).__name__, "message": redact_text(str(error))}
        raise
    finally:
        evidence["ended_epoch_s"] = time.time()
        evidence["wrapper_invocation_wall_s"] = time.perf_counter() - invoked
        evidence["invocation_wall_semantics"] = (
            "after_argument_parse_through_imports_model_setup_warmup_formal_original_outputs_"
            "actor_snapshots_worker_stop_Ray_shutdown; includes_diagnostic_overhead; "
            "excludes_Python_process_start_and_final_validation_sidecar_write; not_formal_JCT")
        evidence.setdefault("source_manifest", _source_manifest(args.base_cli if args.mode == "wrap" else None))
        with args.observations.open("x", encoding="utf-8") as stream:
            json.dump(redact_json_values(evidence), stream, indent=2, sort_keys=True)
            stream.write("\n")


if __name__ == "__main__":
    main()
