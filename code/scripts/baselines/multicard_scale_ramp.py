#!/usr/bin/env python3
"""Multicard scale-ramp driver: 2048 -> 4096 -> 8192 -> 10570 at fixed c=32/K=32.

WHY
---
The 2048-row screening (operator wall ~5s, 16-22 GPU samples) is too short to
show steady-state. This driver ramps the WORKLOAD scale at the already-calibrated
saturation operating point (c=32/K=32 per endpoint), holding concurrency fixed so
scale is the only moving variable (per the calibration rule: never move two
dimensions at once). It observes whether tokens/s / rows/s plateau, whether GPU
util / running stay stable, whether TTFT P95/P99 degrade with scale, and whether
the three-arm throughput ordering stays stable across scales.

ARMS
----
- bounded_http / duckdb_ai (2-endpoint, sharded gate cells): run via run_core_gate
  (single cell per invocation), WRAPPED in cell_instrumentation.instrumented_cell
  so each gate cell gets a TTFT histogram delta (vllm_metric_delta_stats, before
  the cell idle -> after idle) + a per-cell gpu_resource.csv recording gpu0 AND
  gpu1. The shared gate_runner itself is left untouched.
- project_static (2-endpoint, via endpoint_urls): run via run_project_static; the
  profiler already emits TTFT + a resource CSV natively, so no wrapper needed.

OUTPUT LAYOUT (per scale, per arm, per repeat)
----------------------------------------------
<output_root>/scale_<S>/<arm>_c<K>_rep<R>/
    gate_output/...                  # run_core_gate cell evidence (gate arms)
    ttft_metrics.json                # gate arms only (from instrumented_cell)
    gpu_resource.csv                 # gate arms only (from instrumented_cell)
    project.csv + project_evidence.csv + project_resource.csv + project_trace.csv  # project

The aggregator is then run once per scale_<S>/ to produce a per-scale report; the
ramp README combines the per-scale summaries into the plateau/ordering narrative.

This driver does NOT add project scheduling logic; it only orchestrates the
existing, separately-tested runners + the instrumented_cell wrapper. It is
config-driven (deploy/autodl/multicard_scale_ramp.example.json) so the run is
reproducible, not an ad-hoc server script.
"""

from __future__ import annotations

import argparse
import json
import sys
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

CODE_ROOT = next(
    parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.baselines.common.cell_instrumentation import (  # noqa: E402
    instrumented_cell,
)
from src.baselines.common.manifests import read_manifest  # noqa: E402
from src.baselines.text.orchestration.gate_runner import (  # noqa: E402
    CoreGateCell,
    CoreGateConfig,
    run_core_gate,
)
from src.infrastructure.config_env import expand_structure  # noqa: E402

GATE_ARMS = ("bounded_http", "duckdb_ai")


@dataclass(frozen=True)
class RampArm:
    arm: str
    concurrency: int  # c for gate arms, K for project
    reps: int


@dataclass(frozen=True)
class RampScale:
    rows: int
    manifest: Path


@dataclass(frozen=True)
class RampConfig:
    experiment_id: str
    endpoint_urls: tuple[str, ...]
    completion_protocol: str
    model: str
    tokenizer: str | None
    service_prefix_caching: str
    service_max_num_seqs: int
    service_max_num_batched_tokens: int
    scales: tuple[RampScale, ...]
    arms: tuple[RampArm, ...]
    output_root: Path
    driver_python: str
    vllm_python: str
    database_url: str  # project_static only
    workload_name: str  # project_static only
    max_tokens: int  # project_static only (cap)
    project_token_budget: int
    project_active_work: int
    project_actor_workers: int
    project_ray_concurrency: int
    idle_timeout_s: float = 120.0


def _load_ramp_config(path: str | Path, *, driver_python: str, vllm_python: str) -> RampConfig:
    payload = expand_structure(json.loads(Path(path).read_text(encoding="utf-8")), "ramp_config")
    if not isinstance(payload, dict):
        raise ValueError("ramp config must be an object")
    scales = tuple(
        RampScale(rows=int(s["rows"]), manifest=Path(s["manifest"]))
        for s in payload["scales"]
    )
    for s in scales:
        if not s.manifest.is_file():
            raise ValueError(f"scale {s.rows} manifest missing: {s.manifest}")
        manifest_rows = len(read_manifest(s.manifest))
        if manifest_rows != s.rows:
            raise ValueError(
                f"scale {s.rows} manifest has {manifest_rows} rows (mismatch)"
            )
    arms = tuple(
        RampArm(arm=str(a["arm"]), concurrency=int(a["concurrency"]), reps=int(a.get("reps", 1)))
        for a in payload["arms"]
    )
    for a in arms:
        if a.arm not in GATE_ARMS and a.arm != "project_static":
            raise ValueError(f"unsupported arm {a.arm!r}; expected {GATE_ARMS} or project_static")
        if a.concurrency <= 0 or a.reps <= 0:
            raise ValueError(f"arm {a.arm} concurrency/reps must be positive")
    return RampConfig(
        experiment_id=str(payload.get("experiment_id", "multicard_scale_ramp")),
        endpoint_urls=tuple(payload["endpoint_urls"]),
        completion_protocol=str(payload.get("completion_protocol", "chat_completions")),
        model=str(payload["model"]),
        tokenizer=payload.get("tokenizer"),
        service_prefix_caching=str(payload.get("service_prefix_caching", "enabled")),
        service_max_num_seqs=int(payload.get("service_max_num_seqs", -1)),
        service_max_num_batched_tokens=int(payload.get("service_max_num_batched_tokens", -1)),
        scales=scales,
        arms=arms,
        output_root=Path(payload["output_root"]),
        driver_python=driver_python,
        vllm_python=vllm_python,
        database_url=str(payload["database_url"]),
        workload_name=str(payload["workload_name"]),
        max_tokens=int(payload["max_tokens"]),
        project_token_budget=int(payload["project_token_budget"]),
        project_active_work=int(payload["project_active_work"]),
        project_actor_workers=int(payload["project_actor_workers"]),
        project_ray_concurrency=int(payload["project_ray_concurrency"]),
        idle_timeout_s=float(payload.get("idle_timeout_s", 120.0)),
    )


def _gate_config_for_cell(
    ramp: RampConfig, scale: RampScale, arm: RampArm, cell_output: Path
) -> tuple[Path, CoreGateConfig]:
    """Build an in-memory single-cell gate config + its resolved CoreGateConfig."""
    cell_id = f"{arm.arm}_c{arm.concurrency}"
    cfg_dict = {
        "schema_version": 1,
        "experiment_id": f"{ramp.experiment_id}_{arm.arm}_{scale.rows}r",
        "formal": False,
        "rows_total": scale.rows,
        "endpoint_urls": list(ramp.endpoint_urls),
        "model": ramp.model,
        "tokenizer": ramp.tokenizer,
        "completion_protocol": ramp.completion_protocol,
        "temperature": 0.0,
        "service": {
            "prefix_caching": ramp.service_prefix_caching,
            "max_num_seqs": ramp.service_max_num_seqs,
            "max_num_batched_tokens": ramp.service_max_num_batched_tokens,
        },
        "manifest": str(scale.manifest),
        "output_root": str(cell_output / "gate_output"),
        "cells": [
            {"id": cell_id, "adapter": arm.arm, "concurrency_per_endpoint": arm.concurrency}
        ],
        "hard_gates": {
            "provenance_fields_present": True,
            "native_arms_have_no_project_scheduler": True,
            "exactly_once": True,
            "failed_rows": 0,
            "worker_failures": 0,
            "both_endpoints_used": True,
            "service_counter_consistency": True,
            "endpoint_predicted_work_skew_max": 0.02,
            "vllm_running_final": 0,
            "vllm_waiting_final": 0,
            "same_model": True,
            "same_protocol": True,
            "same_service_config": True,
        },
    }
    cfg_path = cell_output / "gate_config.json"
    cfg_path.write_text(json.dumps(cfg_dict, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    # Reuse the gate runner's own loader to get a validated CoreGateConfig
    from src.baselines.text.orchestration.gate_runner import load_core_gate_config
    core = load_core_gate_config(
        cfg_path,
        output_root_override=str(cell_output / "gate_output"),
    )
    return cfg_path, core


def _metrics_urls(endpoint_urls: Sequence[str]) -> tuple[str, ...]:
    return tuple(u.split("/v1/", maxsplit=1)[0] + "/metrics" for u in endpoint_urls)


def _run_gate_cell(ramp: RampConfig, scale: RampScale, arm: RampArm, rep: int) -> dict:
    cell_output = ramp.output_root / f"scale_{scale.rows}" / f"{arm.arm}_c{arm.concurrency}_rep{rep}"
    if cell_output.exists():
        raise FileExistsError(f"cell output already exists: {cell_output}")
    cell_output.mkdir(parents=True)
    cfg_path, _core = _gate_config_for_cell(ramp, scale, arm, cell_output)
    metrics_urls = _metrics_urls(ramp.endpoint_urls)
    record = {"arm": arm.arm, "scale": scale.rows, "concurrency": arm.concurrency, "rep": rep,
              "cell": str(cell_output), "kind": "gate"}
    try:
        with instrumented_cell(
            metrics_urls,
            cell_output / "gpu_resource.csv",
            interval_s=0.3,
        ) as instr:
            run_core_gate(
                cfg_path,
                driver_python=ramp.driver_python,
                vllm_python=ramp.vllm_python,
                output_root_override=str(cell_output / "gate_output"),
                idle_timeout_s=ramp.idle_timeout_s,
            )
    except Exception as exc:
        record["status"] = "failed"
        record["error"] = f"{type(exc).__name__}: {exc}"
        record["traceback"] = traceback.format_exc()
        (cell_output / "run_error.json").write_text(
            json.dumps(record, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        return record
    ttft_path = cell_output / "ttft_metrics.json"
    if instr.ttft_deltas is not None:
        ttft_path.write_text(
            json.dumps(
                {str(ep): deltas for ep, deltas in instr.ttft_deltas.items()},
                indent=2, ensure_ascii=False, sort_keys=True,
            ),
            encoding="utf-8",
        )
    record["status"] = "passed"
    record["gpu_summary"] = instr.gpu_summary
    record["ttft_metrics"] = str(ttft_path) if ttft_path.is_file() else None
    return record


def _run_project_cell(ramp: RampConfig, scale: RampScale, arm: RampArm, rep: int) -> dict:
    from src.baselines.text.products.project_static import (
        ProjectStaticConfig,
        run_project_static,
    )
    cell_output = ramp.output_root / f"scale_{scale.rows}" / f"{arm.arm}_K{arm.concurrency}_rep{rep}"
    if cell_output.exists():
        raise FileExistsError(f"cell output already exists: {cell_output}")
    cell_output.mkdir(parents=True)
    cfg = ProjectStaticConfig(
        database_url=ramp.database_url,
        workload_name=ramp.workload_name,
        endpoint_url=ramp.endpoint_urls[0],
        endpoint_urls=tuple(ramp.endpoint_urls),
        model=ramp.model,
        max_tokens=ramp.max_tokens,
        token_budget=ramp.project_token_budget,
        max_inflight=arm.concurrency,
        max_active_work_per_endpoint=ramp.project_active_work,
        actor_workers_per_endpoint=ramp.project_actor_workers,
        ray_actor_max_concurrency=ramp.project_ray_concurrency,
        total_rows=scale.rows,
        python_executable=ramp.driver_python,
    )
    record = {"arm": "project_static", "scale": scale.rows, "concurrency": arm.concurrency,
              "rep": rep, "cell": str(cell_output), "kind": "project"}
    try:
        run_result = run_project_static(cfg, cell_output)
        record["status"] = "passed" if run_result.exit_code == 0 else "failed"
        record["exit_code"] = run_result.exit_code
        record["effective_k"] = run_result.effective_k
    except Exception as exc:
        record["status"] = "failed"
        record["error"] = f"{type(exc).__name__}: {exc}"
        record["traceback"] = traceback.format_exc()
    return record


def _ensure_ray_head() -> None:
    """Start a Ray head if project_static is an arm and none is running.

    project_static uses --executor ray_actor, so ray.init() must connect to a
    live head. The runtime env sets RAY_ADDRESS=127.0.0.1:6380; if no head runs
    there, ray.init() hangs ~forever retrying the dead GCS (deploy/autodl/README.md
    §10.5 stale-cluster failure). The gate arms (bounded_http/duckdb_ai) don't use
    Ray, so they're unaffected -- this is why an earlier run saw project hang while
    bounded/duckdb passed. Idempotent: ``ray start --head`` reconnects if a head
    already exists.
    """
    import shutil
    import subprocess
    ray_cli = shutil.which("ray") or "/root/miniconda3/bin/ray"
    try:
        subprocess.run(
            [ray_cli, "start", "--head", "--node-ip-address=127.0.0.1",
             "--port=6380", "--disable-usage-stats"],
            capture_output=True, text=True, timeout=60,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        print(f"[ramp] WARNING: could not ensure Ray head: {exc}", flush=True)


def run_ramp(ramp: RampConfig) -> dict:
    ramp.output_root.mkdir(parents=True, exist_ok=True)
    if any(arm.arm == "project_static" for arm in ramp.arms):
        print("[ramp] project_static arm present -- ensuring Ray head", flush=True)
        _ensure_ray_head()
    records: list[dict] = []
    for scale in ramp.scales:
        for arm in ramp.arms:
            for rep in range(1, arm.reps + 1):
                print(f"[ramp] scale={scale.rows} arm={arm.arm} "
                      f"{'c' if arm.arm in GATE_ARMS else 'K'}={arm.concurrency} rep={rep}", flush=True)
                if arm.arm in GATE_ARMS:
                    record = _run_gate_cell(ramp, scale, arm, rep)
                else:
                    record = _run_project_cell(ramp, scale, arm, rep)
                records.append(record)
                print(f"  -> {record['status']}", flush=True)
    summary = {"experiment_id": ramp.experiment_id, "records": records,
               "n_passed": sum(1 for r in records if r.get("status") == "passed"),
               "n_failed": sum(1 for r in records if r.get("status") != "passed")}
    (ramp.output_root / "ramp_run.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return summary


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--config", required=True)
    p.add_argument("--driver-python", required=True)
    p.add_argument("--vllm-python", required=True)
    args = p.parse_args()
    ramp = _load_ramp_config(args.config, driver_python=args.driver_python, vllm_python=args.vllm_python)
    summary = run_ramp(ramp)
    print(json.dumps({k: v for k, v in summary.items() if k != "records"},
                     indent=2, ensure_ascii=False))
    return 0 if summary["n_failed"] == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
