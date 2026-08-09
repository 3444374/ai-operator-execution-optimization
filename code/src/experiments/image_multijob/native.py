"""Coordinate native image applications on one externally owned Ray cluster.

The harness only aligns immutable PostgreSQL ranges and formal start times.  It
does not inject project admission, routing, batching, or credit into Daft
``embed_image`` or Ray Data ``map_batches`` graphs.
"""

from __future__ import annotations

import json
import math
import random
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Sequence

from src.baselines.image.provenance import image_arm_provenance
from src.infrastructure.config_env import expand_structure
from src.infrastructure.runner_lease import acquire_host_runner_lease
from src.modalities.image.resource_sampling import (
    NvidiaSmiSampler,
    RayClusterResourceSampler,
    SystemCpuSampler,
)
from .manifest import ImageJobManifest, load_image_job_manifest


NATIVE_IMAGE_ARMS = frozenset({"daft_builtin_embed", "ray_data_staged"})
_BANNED = (
    "project_ray",
    "max-active-batches",
    "shared-credit",
    "shared_work",
    "static_partition",
)
_OWNED_FLAGS = frozenset(
    {
        "--arm",
        "--phase",
        "--repeat-index",
        "--workload-name",
        "--limit",
        "--offset",
        "--ray-address",
        "--formal-ready-file",
        "--formal-start-barrier-file",
        "--formal-start-offset-s",
        "--out-csv",
        "--out-manifest",
    }
)


@dataclass(frozen=True)
class ImageJob:
    job_id: str
    workload_name: str
    limit: int
    offset: int
    start_offset_s: float


@dataclass(frozen=True)
class NativeImageArm:
    arm_id: str
    adapter: str
    args: tuple[str, ...]
    jobs: tuple[ImageJob, ...]


@dataclass(frozen=True)
class NativeImageMultiJobConfig:
    experiment_id: str
    output_root: Path
    python_executable: str
    image_runner: Path
    ray_address: str
    job_manifest: ImageJobManifest
    common_args: tuple[str, ...]
    launch_lead_s: float
    ready_timeout_s: float
    process_timeout_s: float
    gpu_sample_interval_s: float
    cpu_sample_interval_s: float
    warmup_repeats: int
    formal_repeats: int
    schedule_seed: int
    minimum_measurement_seconds: float
    arms: tuple[NativeImageArm, ...]


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or "REPLACE_ME" in value:
        raise ValueError(f"{field} must be a resolved non-empty string")
    return value.strip()


def _integer(value: object, field: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{field} must be an integer >= {minimum}")
    return value


def _number(value: object, field: str, *, allow_zero: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field} must be numeric")
    parsed = float(value)
    if not math.isfinite(parsed) or parsed < 0 or (parsed == 0 and not allow_zero):
        raise ValueError(f"{field} must be finite and {'non-negative' if allow_zero else 'positive'}")
    return parsed


def _arguments(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(
        isinstance(item, bool) or not isinstance(item, (str, int, float))
        for item in value
    ):
        raise ValueError(f"{field} must be a list of string/number argv scalars")
    result = tuple(str(item) for item in value)
    conflicts = sorted(set(result) & _OWNED_FLAGS)
    if conflicts:
        raise ValueError(f"{field} contains runner-owned flags: {conflicts}")
    joined = " ".join(result).lower()
    banned = [item for item in _BANNED if item in joined]
    if banned:
        raise ValueError(f"native image command contains project controls: {banned}")
    return result


def _flag_value(arguments: tuple[str, ...], flag: str) -> str:
    if arguments.count(flag) != 1:
        raise ValueError(f"native image common_args requires exactly one {flag}")
    index = arguments.index(flag)
    if index + 1 >= len(arguments) or arguments[index + 1].startswith("--"):
        raise ValueError(f"native image common_args has no value for {flag}")
    return arguments[index + 1]


def _arm(raw: object, manifest: ImageJobManifest) -> NativeImageArm:
    if not isinstance(raw, dict) or set(raw) != {"id", "adapter", "args", "jobs"}:
        raise ValueError("native image arm fields are invalid")
    arm_id = _text(raw["id"], "arm id")
    adapter = _text(raw["adapter"], f"arm {arm_id} adapter")
    if adapter not in NATIVE_IMAGE_ARMS:
        raise ValueError(f"arm {arm_id} must use one of {sorted(NATIVE_IMAGE_ARMS)}")
    job_ids = raw["jobs"]
    if (
        not isinstance(job_ids, list)
        or len(job_ids) not in {1, 4}
        or any(not isinstance(item, str) for item in job_ids)
    ):
        raise ValueError(f"arm {arm_id} must contain exactly one or four jobs")
    selected = manifest.select(tuple(job_ids))
    jobs = tuple(
        ImageJob(
            item.job_id,
            item.workload_name,
            item.limit,
            item.offset,
            item.multi_job_start_offset_s,
        )
        for item in selected
    )
    return NativeImageArm(
        arm_id=arm_id,
        adapter=adapter,
        args=_arguments(raw["args"], f"arm {arm_id} args"),
        jobs=jobs,
    )


def load_native_image_multijob_config(path: str | Path) -> NativeImageMultiJobConfig:
    payload = expand_structure(
        json.loads(Path(path).read_text(encoding="utf-8")),
        "native_image_multijob_config",
    )
    required = {
        "schema_version", "formal", "experiment_id", "output_root",
        "python_executable", "image_runner", "ray_address", "job_manifest", "common_args",
        "launch_lead_s", "ready_timeout_s", "process_timeout_s",
        "gpu_sample_interval_s", "cpu_sample_interval_s", "warmup_repeats",
        "formal_repeats", "schedule_seed", "minimum_measurement_seconds", "arms",
    }
    if not isinstance(payload, dict) or set(payload) != required:
        raise ValueError("native image multi-job config fields are invalid")
    if payload["schema_version"] != 1 or payload["formal"] is not True:
        raise ValueError("native image multi-job config requires schema_version=1/formal=true")
    manifest = load_image_job_manifest(_text(payload["job_manifest"], "job_manifest"))
    arms_raw = payload["arms"]
    if not isinstance(arms_raw, list) or not arms_raw:
        raise ValueError("arms must be non-empty")
    arms = tuple(_arm(item, manifest) for item in arms_raw)
    if len({item.arm_id for item in arms}) != len(arms):
        raise ValueError("arm ids must be unique")
    expected_arm_ids = {
        f"{prefix}_{suffix}"
        for prefix in ("daft_builtin", "ray_data")
        for suffix in (
            "single_short", "single_long1", "single_long2", "single_long3", "fourjob"
        )
    }
    if {item.arm_id for item in arms} != expected_arm_ids:
        raise ValueError("native image matrix must keep four single controls and one four-job arm per system")
    for arm in arms:
        expected_adapter = "daft_builtin_embed" if arm.arm_id.startswith("daft_builtin_") else "ray_data_staged"
        suffix = arm.arm_id.removeprefix("daft_builtin_").removeprefix("ray_data_")
        expected_jobs = (
            {"short", "long1", "long2", "long3"}
            if suffix == "fourjob"
            else {suffix.removeprefix("single_")}
        )
        if arm.adapter != expected_adapter or {job.job_id for job in arm.jobs} != expected_jobs:
            raise ValueError(f"native image arm identity changed: {arm.arm_id}")
    output_root = Path(_text(payload["output_root"], "output_root"))
    if output_root.exists():
        raise FileExistsError(f"output_root already exists: {output_root}")
    warmups = _integer(payload["warmup_repeats"], "warmup_repeats", minimum=1)
    if warmups != 1:
        raise ValueError("warmup_repeats is frozen at one")
    formal_repeats = _integer(payload["formal_repeats"], "formal_repeats", minimum=1)
    if formal_repeats != 3:
        raise ValueError("formal_repeats is frozen at three")
    common_args = _arguments(payload["common_args"], "common_args")
    frozen_common = {
        "--batch-size": "64",
        "--gpu-workers": "2",
        "--source-shards": "4",
        "--dtype": "float16",
        "--embedding-output-contract": "l2_normalized",
    }
    mismatches = {
        flag: (_flag_value(common_args, flag), expected)
        for flag, expected in frozen_common.items()
        if _flag_value(common_args, flag) != expected
    }
    for required_flag in (
        "--model", "--processor", "--model-flops-per-image", "--gpu-peak-flops-per-s"
    ):
        _flag_value(common_args, required_flag)
    if mismatches:
        raise ValueError(f"native image frozen comparison contract changed: {mismatches}")
    return NativeImageMultiJobConfig(
        experiment_id=_text(payload["experiment_id"], "experiment_id"),
        output_root=output_root,
        python_executable=_text(payload["python_executable"], "python_executable"),
        image_runner=Path(_text(payload["image_runner"], "image_runner")),
        ray_address=_text(payload["ray_address"], "ray_address"),
        job_manifest=manifest,
        common_args=common_args,
        launch_lead_s=_number(payload["launch_lead_s"], "launch_lead_s", allow_zero=True),
        ready_timeout_s=_number(payload["ready_timeout_s"], "ready_timeout_s"),
        process_timeout_s=_number(payload["process_timeout_s"], "process_timeout_s"),
        gpu_sample_interval_s=_number(payload["gpu_sample_interval_s"], "gpu_sample_interval_s"),
        cpu_sample_interval_s=_number(payload["cpu_sample_interval_s"], "cpu_sample_interval_s"),
        warmup_repeats=warmups,
        formal_repeats=formal_repeats,
        schedule_seed=_integer(payload["schedule_seed"], "schedule_seed", minimum=1),
        minimum_measurement_seconds=_number(
            payload["minimum_measurement_seconds"],
            "minimum_measurement_seconds",
            allow_zero=True,
        ),
        arms=arms,
    )


def _atomic_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def _schedule(config: NativeImageMultiJobConfig, phase: str, repeat: int) -> tuple[NativeImageArm, ...]:
    arms = list(config.arms)
    random.Random(f"{config.schedule_seed}:{phase}").shuffle(arms)
    shift = (repeat - 1) % len(arms)
    return tuple(arms[shift:] + arms[:shift])


def build_job_command(
    config: NativeImageMultiJobConfig,
    arm: NativeImageArm,
    job: ImageJob,
    *,
    phase: str,
    repeat: int,
    root: Path,
) -> list[str]:
    command = [
        config.python_executable,
        str(config.image_runner),
        *config.common_args,
        *arm.args,
        "--arm", arm.adapter,
        "--workload-name", job.workload_name,
        "--limit", str(job.limit),
        "--offset", str(job.offset),
        "--ray-address", config.ray_address,
        "--formal-ready-file", str(root / job.job_id / "ready.json"),
        "--formal-start-barrier-file", str(root / "start_barrier.json"),
        "--formal-start-offset-s", str(job.start_offset_s),
        "--formal-barrier-timeout-s", str(config.ready_timeout_s),
        "--phase", phase,
        "--repeat-index", str(repeat),
        "--out-csv", str(root / job.job_id / "run.csv"),
        "--out-manifest", str(root / job.job_id / "run.json"),
    ]
    joined = " ".join(command).lower()
    if any(item in joined for item in _BANNED):
        raise ValueError("native image job command contains project scheduling controls")
    return command


def _wait_ready(paths: Sequence[Path], processes: Sequence[subprocess.Popen[object]], timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    while not all(path.is_file() for path in paths):
        failed = [process.poll() for process in processes if process.poll() is not None]
        if failed:
            raise RuntimeError(f"native image job exited before readiness: {failed}")
        if time.monotonic() >= deadline:
            raise TimeoutError("native image jobs did not reach the formal barrier")
        time.sleep(0.1)


def _wait_processes(processes: Sequence[subprocess.Popen[object]], timeout_s: float) -> list[int]:
    deadline = time.monotonic() + timeout_s
    codes: list[int] = []
    try:
        for process in processes:
            codes.append(int(process.wait(timeout=max(0.01, deadline - time.monotonic()))))
    except subprocess.TimeoutExpired:
        for process in processes:
            if process.poll() is None:
                process.terminate()
        for process in processes:
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
        raise TimeoutError("native image multi-job process timeout")
    return codes


def _read_row(path: Path, arm: NativeImageArm, job: ImageJob, minimum_s: float) -> dict[str, object]:
    manifest = json.loads(path.read_text(encoding="utf-8"))
    row = manifest.get("row")
    if not isinstance(row, dict):
        raise ValueError(f"missing image row: {path}")
    provenance = image_arm_provenance(arm.adapter)
    expected = {
        "baseline_role": provenance.role,
        "scheduler_owner": provenance.scheduler_owner,
        "custom_scheduling_code": provenance.custom_scheduling_code,
        "formal_baseline_eligible": provenance.formal_baseline_eligible,
    }
    mismatches = {key: (value, row.get(key)) for key, value in expected.items() if row.get(key) != value}
    if mismatches:
        raise ValueError(f"native image provenance mismatch: {mismatches}")
    if row.get("ray_address_mode") != "external_shared":
        raise ValueError("native image job did not use the shared external Ray cluster")
    if row.get("exactly_once") is not True or row.get("output_rows") != job.limit:
        raise ValueError("native image job failed exactly-once")
    if float(row["operator_e2e_s"]) < minimum_s:
        raise ValueError("native image job is below the preregistered measurement duration")
    return row


def _run_cell(
    config: NativeImageMultiJobConfig,
    arm: NativeImageArm,
    *,
    phase: str,
    repeat: int,
    root: Path,
    popen_factory: Callable[..., subprocess.Popen[object]],
) -> dict[str, object]:
    root.mkdir(parents=True)
    commands = [build_job_command(config, arm, job, phase=phase, repeat=repeat, root=root) for job in arm.jobs]
    logs = []
    processes = []
    for job, command in zip(arm.jobs, commands, strict=True):
        job_root = root / job.job_id
        job_root.mkdir()
        log = (job_root / "process.log").open("w", encoding="utf-8")
        logs.append(log)
        processes.append(popen_factory(command, stdout=log, stderr=subprocess.STDOUT, text=True))
    try:
        _wait_ready([root / job.job_id / "ready.json" for job in arm.jobs], processes, config.ready_timeout_s)
        start_epoch_s = time.time() + config.launch_lead_s
        gpu_sampler = NvidiaSmiSampler(config.gpu_sample_interval_s, active_device_count=2)
        cpu_sampler = SystemCpuSampler(config.cpu_sample_interval_s)
        ray_sampler = RayClusterResourceSampler(config.gpu_sample_interval_s)
        gpu_sampler.start()
        cpu_sampler.start()
        ray_sampler.start()
        _atomic_json(root / "start_barrier.json", {"start_epoch_s": start_epoch_s})
        try:
            codes = _wait_processes(processes, config.process_timeout_s)
        finally:
            gpu = gpu_sampler.stop()
            cpu = cpu_sampler.stop()
            ray_resources = ray_sampler.stop()
            ray_sampler.write_csv(root / "ray_resource_trace.csv")
    finally:
        for log in logs:
            log.close()
    if codes != [0] * len(processes):
        raise RuntimeError(f"native image job exits were nonzero: {codes}")
    rows = [
        _read_row(root / job.job_id / "run.json", arm, job, config.minimum_measurement_seconds if phase == "formal" else 0.0)
        for job in arm.jobs
    ]
    summary = {
        "status": "passed",
        "arm_id": arm.arm_id,
        "adapter": arm.adapter,
        "phase": phase,
        "repeat": repeat,
        "jobs": [
            {
                "job_id": job.job_id,
                "limit": job.limit,
                "offset": job.offset,
                "start_offset_s": job.start_offset_s,
                "operator_e2e_s": row["operator_e2e_s"],
                "first_output_s": row["first_output_s"],
                "images_per_s": row["images_per_s"],
                "formal_start_lateness_s": row["formal_start_lateness_s"],
                "formal_start_epoch_s_actual": row["formal_start_epoch_s_actual"],
                "formal_end_epoch_s_actual": (
                    float(row["formal_start_epoch_s_actual"])
                    + float(row["operator_e2e_s"])
                ),
            }
            for job, row in zip(arm.jobs, rows, strict=True)
        ],
        "group_start_epoch_s": start_epoch_s,
        "group_end_epoch_s": time.time(),
        "gpu_summary": gpu,
        "cpu_summary": cpu,
        "ray_resource_summary": ray_resources,
        "commands": commands,
    }
    _atomic_json(root / "group_summary.json", summary)
    return summary


def _repository_commit() -> str:
    return subprocess.run(
        ["git", "rev-parse", "HEAD"], check=True, capture_output=True, text=True,
        cwd=Path(__file__).resolve().parents[5],
    ).stdout.strip()


def run_native_image_multijob(
    config: NativeImageMultiJobConfig,
    *,
    popen_factory: Callable[..., subprocess.Popen[object]] = subprocess.Popen,
) -> int:
    """Run the preregistered native matrix; callers may inject fake processes in tests."""

    import ray

    ray.init(address=config.ray_address)
    config.output_root.mkdir(parents=True)
    index = {
        "schema_version": 1,
        "experiment_id": config.experiment_id,
        "repository_commit": _repository_commit(),
        "job_manifest": str(config.job_manifest.path),
        "job_manifest_sha256": config.job_manifest.sha256,
        "status": "running",
        "runs": [],
    }
    index_path = config.output_root / "matrix_index.json"
    _atomic_json(index_path, index)
    try:
        with acquire_host_runner_lease(config.output_root.parent, repository_commit=index["repository_commit"]):
            try:
                for phase, count in (
                    ("warmup", config.warmup_repeats),
                    ("formal", config.formal_repeats),
                ):
                    for repeat in range(1, count + 1):
                        for arm in _schedule(config, phase, repeat):
                            run_id = f"{phase}_{repeat}_{arm.arm_id}"
                            summary = _run_cell(
                                config,
                                arm,
                                phase=phase,
                                repeat=repeat,
                                root=config.output_root / "runs" / run_id,
                                popen_factory=popen_factory,
                            )
                            index["runs"].append(
                                {"run_id": run_id, "status": summary["status"]}
                            )
                            _atomic_json(index_path, index)
            except Exception as exc:
                index["status"] = "failed"
                index["error"] = f"{type(exc).__name__}:{exc}"
                _atomic_json(index_path, index)
                return 1
    finally:
        ray.shutdown()
    index["status"] = "passed"
    _atomic_json(index_path, index)
    return 0
