"""Repeat frozen native-text baseline cells without reimplementing their gate.

The module turns one immutable manifest and explicitly frozen framework cells
into isolated one-cell validity-gate runs.  Its output index is the only place
where repeat order and formal-duration rankability are decided; execution,
request accounting, and two-endpoint validity remain owned by ``gate_runner``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from src.baselines.common.manifests import read_manifest
from src.baselines.text.orchestration.gate_runner import (
    BLOCKED_ADAPTER_REASONS,
    run_core_gate,
)
from src.infrastructure.config_env import expand_structure


_GATE_HARD_GATES = frozenset(
    {
        "provenance_fields_present",
        "native_arms_have_no_project_scheduler",
        "exactly_once",
        "failed_rows",
        "worker_failures",
        "vllm_running_final",
        "vllm_waiting_final",
        "both_endpoints_used",
        "service_counter_consistency",
        "same_model",
        "same_protocol",
        "same_service_config",
        "endpoint_predicted_work_skew_max",
    }
)
_REQUIRED_ARM_FIELDS = frozenset(
    {
        "id",
        "adapter",
        "concurrency_per_endpoint",
        "batch_size",
        "ray_address",
        "python_executable",
        "calibration",
    }
)
_RAY_ADAPTERS = frozenset({"daft_ray", "ray_data_http"})
_FRAMEWORK_TRACK_ADAPTERS = frozenset(
    {"vllm_bench", "bounded_http", "daft_native", "daft_ray", "ray_data_http"}
)

CoreGateInvoker = Callable[..., dict[str, object]]


@dataclass(frozen=True)
class NativeMatrixArm:
    """One fully frozen framework cell and its calibration provenance."""

    cell_id: str
    adapter: str
    concurrency_per_endpoint: int
    batch_size: int
    ray_address: str | None
    python_executable: str
    calibration: dict[str, str]


@dataclass(frozen=True)
class NativeMatrixConfig:
    """Formal repeat contract for native text framework comparisons."""

    experiment_id: str
    rows_total: int
    endpoint_urls: tuple[str, ...]
    completion_protocol: str
    model: str
    tokenizer: str | None
    service: dict[str, object]
    manifest: Path
    manifest_sha256: str
    output_root: Path
    warmup_repeats: int
    formal_repeats: int
    schedule_seed: int
    minimum_measurement_seconds: float
    hard_gates: dict[str, object]
    partition_policy: str | None
    arms: tuple[NativeMatrixArm, ...]


def _atomic_json(path: Path, payload: object) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _nonempty_string(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or "REPLACE_ME" in value:
        raise ValueError(f"{field} must be a resolved non-empty string")
    return value


def _positive_int(value: object, field: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a positive integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a positive integer") from exc
    if parsed <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return parsed


def _calibration(raw: object, arm_id: str) -> dict[str, str]:
    if not isinstance(raw, dict):
        raise ValueError(f"arm {arm_id} calibration must be an object")
    unknown = set(raw) - {"selection", "fingerprint"}
    if unknown:
        raise ValueError(f"arm {arm_id} calibration has unknown keys: {sorted(unknown)}")
    return {
        "selection": _nonempty_string(raw.get("selection"), f"arm {arm_id} calibration.selection"),
        "fingerprint": _nonempty_string(raw.get("fingerprint"), f"arm {arm_id} calibration.fingerprint"),
    }


def _parse_arm(raw: object, seen_ids: set[str]) -> NativeMatrixArm:
    if not isinstance(raw, dict):
        raise ValueError("each matrix arm must be an object")
    missing = _REQUIRED_ARM_FIELDS - set(raw)
    unknown = set(raw) - _REQUIRED_ARM_FIELDS
    if missing or unknown:
        message = []
        if missing:
            message.append(f"missing {sorted(missing)}")
        if unknown:
            message.append(f"unknown {sorted(unknown)}")
        raise ValueError("matrix arm " + "; ".join(message))
    cell_id = _nonempty_string(raw["id"], "arm id")
    if cell_id in seen_ids:
        raise ValueError(f"duplicate arm id: {cell_id}")
    seen_ids.add(cell_id)
    adapter = _nonempty_string(raw["adapter"], f"arm {cell_id} adapter")
    if adapter in BLOCKED_ADAPTER_REASONS:
        raise ValueError(f"arm {cell_id} adapter is blocked: {adapter}")
    if adapter not in _FRAMEWORK_TRACK_ADAPTERS:
        raise ValueError(f"arm {cell_id} has unsupported adapter: {adapter}")
    raw_ray = raw["ray_address"]
    if adapter in _RAY_ADAPTERS:
        ray_address = _nonempty_string(raw_ray, f"arm {cell_id} ray_address")
    elif raw_ray is not None:
        raise ValueError(f"arm {cell_id} ray_address must be null for {adapter}")
    else:
        ray_address = None
    return NativeMatrixArm(
        cell_id=cell_id,
        adapter=adapter,
        concurrency_per_endpoint=_positive_int(
            raw["concurrency_per_endpoint"], f"arm {cell_id} concurrency_per_endpoint"
        ),
        batch_size=_positive_int(raw["batch_size"], f"arm {cell_id} batch_size"),
        ray_address=ray_address,
        python_executable=_nonempty_string(
            raw["python_executable"], f"arm {cell_id} python_executable"
        ),
        calibration=_calibration(raw["calibration"], cell_id),
    )


def load_native_matrix_config(path: str | Path) -> NativeMatrixConfig:
    """Load a formal-only matrix contract and reject implicit tuning fields."""

    payload = expand_structure(
        json.loads(Path(path).read_text(encoding="utf-8")), "native_text_matrix_config"
    )
    if not isinstance(payload, dict):
        raise ValueError("native text matrix config must be an object")
    if payload.get("formal") is not True:
        raise ValueError("native text matrix runner requires formal=true")
    output_root = Path(_nonempty_string(payload.get("output_root"), "output_root"))
    if output_root.exists():
        raise FileExistsError(f"output_root already exists: {output_root}")
    manifest = Path(_nonempty_string(payload.get("manifest"), "manifest"))
    if not manifest.is_file():
        raise FileNotFoundError(f"manifest does not exist: {manifest}")
    rows_total = _positive_int(payload.get("rows_total"), "rows_total")
    requests = read_manifest(manifest)
    if len(requests) != rows_total:
        raise ValueError(f"manifest row count does not match rows_total: {len(requests)} != {rows_total}")
    if {request.endpoint_index for request in requests} != {0, 1}:
        raise ValueError("manifest must use endpoint indexes 0 and 1")
    endpoints = payload.get("endpoint_urls")
    if not isinstance(endpoints, list) or len(endpoints) < 2:
        raise ValueError("endpoint_urls must contain at least two endpoints")
    service = payload.get("service")
    if not isinstance(service, dict):
        raise ValueError("service must be an object")
    hard_gates = payload.get("hard_gates")
    if not isinstance(hard_gates, dict):
        raise ValueError("hard_gates must be an object")
    unknown_gates = set(hard_gates) - _GATE_HARD_GATES
    if unknown_gates:
        raise ValueError(f"hard_gates unsupported by core gate: {sorted(unknown_gates)}")
    arms_raw = payload.get("arms")
    if not isinstance(arms_raw, list) or len(arms_raw) < 2:
        raise ValueError("formal matrix requires at least two arms")
    seen_ids: set[str] = set()
    arms = tuple(_parse_arm(raw, seen_ids) for raw in arms_raw)
    minimum = payload.get("minimum_measurement_seconds")
    if isinstance(minimum, bool) or not isinstance(minimum, (int, float)):
        raise ValueError("minimum_measurement_seconds must be a positive number")
    if not math.isfinite(float(minimum)) or float(minimum) <= 0:
        raise ValueError("minimum_measurement_seconds must be a positive number")
    warmups = _positive_int(payload.get("warmup_repeats"), "warmup_repeats")
    if warmups != 1:
        raise ValueError("native text matrix freezes warmup_repeats=1")
    partition_policy = payload.get("partition_policy")
    if partition_policy is not None and not isinstance(partition_policy, str):
        raise ValueError("partition_policy must be a string or null")
    tokenizer = payload.get("tokenizer")
    if tokenizer is not None and not isinstance(tokenizer, str):
        raise ValueError("tokenizer must be a string or null")
    return NativeMatrixConfig(
        experiment_id=_nonempty_string(payload.get("experiment_id"), "experiment_id"),
        rows_total=rows_total,
        endpoint_urls=tuple(
            _nonempty_string(value, "endpoint_urls entry") for value in endpoints
        ),
        completion_protocol=_nonempty_string(
            payload.get("completion_protocol"), "completion_protocol"
        ),
        model=_nonempty_string(payload.get("model"), "model"),
        tokenizer=tokenizer,
        service=dict(service),
        manifest=manifest,
        manifest_sha256=hashlib.sha256(manifest.read_bytes()).hexdigest(),
        output_root=output_root,
        warmup_repeats=warmups,
        formal_repeats=_positive_int(payload.get("formal_repeats"), "formal_repeats"),
        schedule_seed=_positive_int(payload.get("schedule_seed"), "schedule_seed"),
        minimum_measurement_seconds=float(minimum),
        hard_gates=dict(hard_gates),
        partition_policy=partition_policy,
        arms=arms,
    )


def balanced_arm_order(
    config: NativeMatrixConfig,
    phase: str,
    repeat: int,
) -> tuple[NativeMatrixArm, ...]:
    """Return a seeded cyclic order so formal arms rotate across positions."""

    if phase not in {"warmup", "formal"} or repeat <= 0:
        raise ValueError("phase must be warmup/formal and repeat must be positive")
    ordered = list(config.arms)
    random.Random(f"{config.schedule_seed}:{phase}").shuffle(ordered)
    offset = (repeat - 1) % len(ordered)
    return tuple(ordered[offset:] + ordered[:offset])


def _derived_gate_payload(
    config: NativeMatrixConfig,
    arm: NativeMatrixArm,
    run_root: Path,
    run_id: str,
) -> dict[str, object]:
    cell: dict[str, object] = {
        "id": arm.cell_id,
        "adapter": arm.adapter,
        "concurrency_per_endpoint": arm.concurrency_per_endpoint,
        "batch_size": arm.batch_size,
        "ray_address": arm.ray_address,
        "python_executable": arm.python_executable,
    }
    return {
        "schema_version": 1,
        "experiment_id": f"{config.experiment_id}:{run_id}",
        "formal": False,
        "rows_total": config.rows_total,
        "endpoint_urls": list(config.endpoint_urls),
        "completion_protocol": config.completion_protocol,
        "model": config.model,
        "tokenizer": config.tokenizer,
        "service": config.service,
        "manifest": str(config.manifest),
        "output_root": str(run_root),
        "cells": [cell],
        "hard_gates": config.hard_gates,
        "partition_policy": config.partition_policy,
    }


def _duration_rankability(
    run_root: Path,
    arm: NativeMatrixArm,
    minimum_seconds: float,
) -> dict[str, object]:
    gate_path = run_root / arm.cell_id / "gate.json"
    if not gate_path.is_file():
        return {
            "comparison_eligible": False,
            "duration_status": "unavailable_not_rankable",
            "group_service_wall_s": None,
            "reason": "core gate did not preserve cell gate.json",
        }
    payload = json.loads(gate_path.read_text(encoding="utf-8"))
    metrics = payload.get("metrics")
    duration = metrics.get("group_service_wall_s") if isinstance(metrics, dict) else None
    if isinstance(duration, bool) or not isinstance(duration, (int, float)):
        return {
            "comparison_eligible": False,
            "duration_status": "unavailable_not_rankable",
            "group_service_wall_s": None,
            "reason": "core gate did not expose group_service_wall_s",
        }
    if not math.isfinite(float(duration)) or float(duration) <= 0:
        return {
            "comparison_eligible": False,
            "duration_status": "invalid_not_rankable",
            "group_service_wall_s": float(duration),
            "reason": "group_service_wall_s must be finite and positive",
        }
    eligible = float(duration) >= minimum_seconds
    return {
        "comparison_eligible": eligible,
        "duration_status": "passed" if eligible else "below_minimum_not_rankable",
        "group_service_wall_s": float(duration),
        "reason": None if eligible else f"{duration:.6g}s < {minimum_seconds:.6g}s",
    }


def _assert_manifest_unchanged(config: NativeMatrixConfig) -> None:
    observed = hashlib.sha256(config.manifest.read_bytes()).hexdigest()
    if observed != config.manifest_sha256:
        raise RuntimeError("immutable manifest changed during matrix execution")


def _initial_index(config: NativeMatrixConfig) -> dict[str, object]:
    return {
        "schema_version": 1,
        "experiment_id": config.experiment_id,
        "status": "running",
        "comparison_admission": "pending",
        "manifest": str(config.manifest),
        "manifest_sha256": config.manifest_sha256,
        "minimum_measurement_seconds": config.minimum_measurement_seconds,
        "warmup_repeats": config.warmup_repeats,
        "formal_repeats": config.formal_repeats,
        "schedule_seed": config.schedule_seed,
        "arms": [
            {
                "id": arm.cell_id,
                "adapter": arm.adapter,
                "concurrency_per_endpoint": arm.concurrency_per_endpoint,
                "batch_size": arm.batch_size,
                "ray_address": arm.ray_address,
                "python_executable": arm.python_executable,
                "calibration": arm.calibration,
            }
            for arm in config.arms
        ],
        "runs": [],
    }


def run_native_text_matrix(
    config_path: str | Path,
    *,
    driver_python: str,
    vllm_python: str,
    core_gate_invoker: CoreGateInvoker = run_core_gate,
) -> dict[str, object]:
    """Execute one warm-up and N formal repeats with isolated derived gates."""

    config = load_native_matrix_config(config_path)
    config.output_root.mkdir(parents=True)
    derived_root = config.output_root / "derived_gate_configs"
    derived_root.mkdir()
    runs_root = config.output_root / "runs"
    runs_root.mkdir()
    index_path = config.output_root / "matrix_index.json"
    index = _initial_index(config)
    _atomic_json(index_path, index)
    ordinal = 0
    try:
        for phase, repeats in (("warmup", config.warmup_repeats), ("formal", config.formal_repeats)):
            for repeat in range(1, repeats + 1):
                for position, arm in enumerate(balanced_arm_order(config, phase, repeat), start=1):
                    ordinal += 1
                    run_id = f"{ordinal:03d}_{phase}_{repeat:02d}_{arm.cell_id}"
                    run_root = runs_root / run_id
                    derived_path = derived_root / f"{run_id}.json"
                    _assert_manifest_unchanged(config)
                    _atomic_json(derived_path, _derived_gate_payload(config, arm, run_root, run_id))
                    record: dict[str, object] = {
                        "run_id": run_id,
                        "phase": phase,
                        "repeat": repeat,
                        "interleaved_position": position,
                        "arm_id": arm.cell_id,
                        "adapter": arm.adapter,
                        "output_root": str(run_root),
                        "derived_gate_config": str(derived_path),
                        "status": "running",
                    }
                    index["runs"].append(record)  # type: ignore[index]
                    _atomic_json(index_path, index)
                    try:
                        result = core_gate_invoker(
                            derived_path,
                            driver_python=driver_python,
                            vllm_python=vllm_python,
                        )
                    except Exception as exc:
                        record.update(
                            {
                                "status": "failed",
                                "error": f"{type(exc).__name__}: {exc}",
                            }
                        )
                        index.update(
                            {
                                "status": "failed",
                                "comparison_admission": "not_rankable",
                                "failed_run": run_id,
                            }
                        )
                        _atomic_json(index_path, index)
                        raise
                    record["core_gate_result"] = result
                    record["status"] = "passed"
                    if phase == "formal":
                        record.update(_duration_rankability(run_root, arm, config.minimum_measurement_seconds))
                    else:
                        record["comparison_eligible"] = False
                        record["duration_status"] = "warmup_not_ranked"
                    _atomic_json(index_path, index)
    except Exception:
        raise

    formal_runs = [
        item for item in index["runs"]
        if isinstance(item, dict) and item.get("phase") == "formal"
    ]
    admissible = bool(formal_runs) and all(
        item.get("comparison_eligible") is True for item in formal_runs
    )
    index.update(
        {
            "status": "passed" if admissible else "not_rankable",
            "comparison_admission": "admissible" if admissible else "not_rankable",
            "formal_runs_total": len(formal_runs),
            "formal_runs_rankable": sum(
                item.get("comparison_eligible") is True for item in formal_runs
            ),
        }
    )
    _atomic_json(index_path, index)
    return index


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run frozen text native baseline repeats through the core gate."
    )
    parser.add_argument("--config", required=True)
    parser.add_argument("--driver-python", required=True)
    parser.add_argument("--vllm-python", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = run_native_text_matrix(
            args.config,
            driver_python=args.driver_python,
            vllm_python=args.vllm_python,
        )
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": f"{type(exc).__name__}: {exc}"}))
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["comparison_admission"] == "admissible" else 2
