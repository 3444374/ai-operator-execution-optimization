#!/usr/bin/env python3
"""Audit an opening-project feeding calibration and freeze the smallest saturated K.

The primary input must contain the bounded-HTTP control and project-static
candidates for the same immutable request manifest. Failed incidents remain in
that evidence; optional repair roots may contribute only the exact same-config
successful replacement cells needed to reach three valid repeats. Selection is
deliberately repeat-based: the smallest K
whose median service throughput reaches both the bounded-control floor and the
tested project-peak floor is selected.  A failed audit never yields a K.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from pathlib import Path
from typing import Any


GPU_PEAK_TFLOPS_PER_4090_BF16 = 165.0


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _first_csv_row(path: Path) -> dict[str, str]:
    with path.open(newline="", encoding="utf-8") as handle:
        return next(csv.DictReader(handle))


def _int(value: Any, default: int = -1) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _float(value: Any, default: float = math.nan) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def _zero_list(value: Any) -> bool:
    parts = str(value).split(";")
    return bool(parts) and all(_int(part) == 0 for part in parts)


def _cv(values: list[float]) -> float:
    mean = statistics.fmean(values)
    return statistics.stdev(values) / mean if len(values) > 1 and mean else 0.0


def _json_ready(value: Any) -> Any:
    """Replace non-finite diagnostic values so failed audits still write JSON."""
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {key: _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    return value


def _portable_cell(root: Path, rows: int, record: dict[str, Any]) -> Path:
    original = Path(record["cell"])
    local = root / f"scale_{rows}" / original.name
    return local if local.exists() else original


def _audit_direct_cell(
    cell: Path, rows: int
) -> tuple[float, str, dict[str, Any], list[str]]:
    errors: list[str] = []
    gate_dir = cell / "gate_output" / "bounded_http_c32"
    gate = _read_json(gate_dir / "gate.json")
    if gate.get("status") != "passed" or gate.get("passed") is not True:
        errors.append("gate status is not passed")

    shards = sorted(gate_dir.glob("shard_*/summary.json"))
    if len(shards) != 2:
        errors.append(f"expected 2 shard summaries, found {len(shards)}")
    total_tokens = 0
    max_jct = 0.0
    completed = 0
    manifest_shas: set[str] = set()
    for path in shards:
        shard = _read_json(path)
        if shard.get("status") != "completed":
            errors.append(f"{path.parent.name}: status is not completed")
        if shard.get("exactly_once") is not True:
            errors.append(f"{path.parent.name}: exactly_once is not true")
        for key in ("failed_count", "worker_failures", "vllm_num_requests_running_final", "vllm_num_requests_waiting_final"):
            if _int(shard.get(key)) != 0:
                errors.append(f"{path.parent.name}: {key} is not zero")
        total_tokens += _int(shard.get("service_total_tokens_delta"), 0)
        max_jct = max(max_jct, _float(shard.get("jct_s"), 0.0))
        completed += _int(shard.get("completed_count"), 0)
        metadata = _read_json(path.parent / "manifest_metadata.json")
        manifest_shas.add(str(metadata.get("sha256", "")))
    if completed != rows:
        errors.append(f"completed rows {completed} != expected {rows}")
    if len(manifest_shas) != 1 or "" in manifest_shas:
        errors.append(f"direct manifest SHA mismatch: {sorted(manifest_shas)}")
    gate_metrics = gate.get("metrics", {})
    group_wall_s = _float(gate_metrics.get("group_service_wall_s"))
    gate_total_tokens = _int(gate_metrics.get("group_service_total_tokens"), 0)
    if gate_total_tokens != total_tokens:
        errors.append(
            f"shard service tokens {total_tokens} != gate total {gate_total_tokens}"
        )
    if not math.isfinite(group_wall_s) or group_wall_s <= 0:
        errors.append("group_service_wall_s is not positive and finite")
    elif group_wall_s < max_jct:
        errors.append(
            f"group wall {group_wall_s} is shorter than max shard JCT {max_jct}"
        )
    elif max_jct > 0 and (group_wall_s - max_jct) / group_wall_s > 0.02:
        errors.append(
            f"group/max-shard wall gap exceeds 2%: {group_wall_s} vs {max_jct}"
        )
    rate = total_tokens / group_wall_s if group_wall_s > 0 else math.nan
    gate_rate = _float(gate_metrics.get("group_service_total_tokens_per_s"))
    if not math.isfinite(rate) or not math.isclose(rate, gate_rate, rel_tol=1e-6):
        errors.append(f"recomputed direct rate {rate} != gate rate {gate_rate}")
    ttft_path = cell / "ttft_metrics.json"
    endpoint_metrics = _read_json(ttft_path) if ttft_path.is_file() else {}
    if len(endpoint_metrics) != 2:
        errors.append(f"expected 2 direct endpoint metric deltas, found {len(endpoint_metrics)}")
    estimated_flops = sum(
        _float(endpoint.get("vllm_estimated_flops_per_gpu_delta"), 0.0)
        for endpoint in endpoint_metrics.values()
    )
    mfu = (
        estimated_flops
        / (group_wall_s * len(endpoint_metrics) * GPU_PEAK_TFLOPS_PER_4090_BF16 * 1e12)
        if group_wall_s > 0 and len(endpoint_metrics) == 2 and estimated_flops > 0
        else math.nan
    )
    if not math.isfinite(mfu) or mfu <= 0:
        errors.append("direct recovered MFU is not positive and finite")
    observations = {
        "evidence_cell": str(cell),
        "group_service_wall_s": group_wall_s,
        "max_shard_jct_s": max_jct,
        "vllm_estimated_flops_all_endpoints_delta": estimated_flops,
        "mfu_recovered_fraction": mfu,
    }
    return rate, next(iter(manifest_shas), ""), observations, errors


def _audit_project_cell(cell: Path, rows: int) -> tuple[float, str, dict[str, Any], list[str]]:
    errors: list[str] = []
    row = _first_csv_row(cell / "project_static_summary.csv")
    exact = {
        "status": row.get("status"),
        "request_manifest_validation_status": row.get("request_manifest_validation_status"),
        "resource_metrics_status": row.get("resource_metrics_status"),
        "total_rows": _int(row.get("total_rows")),
        "written_rows": _int(row.get("written_rows")),
        "object_count": _int(row.get("object_count")),
        "request_manifest_rows": _int(row.get("request_manifest_rows")),
        "request_manifest_validated_rows": _int(row.get("request_manifest_validated_rows")),
        "vllm_request_success_delta": _int(row.get("vllm_request_success_delta")),
        "endpoint_count": _int(row.get("endpoint_count")),
        "vllm_num_requests_running_after": _int(row.get("vllm_num_requests_running_after")),
        "vllm_num_requests_waiting_after": _int(row.get("vllm_num_requests_waiting_after")),
    }
    if exact["status"] != "ok":
        errors.append("project status is not ok")
    if exact["request_manifest_validation_status"] != "ok":
        errors.append("project manifest validation status is not ok")
    if exact["resource_metrics_status"] != "ok":
        errors.append("project resource metrics status is not ok")
    for key in ("total_rows", "written_rows", "object_count", "request_manifest_rows", "request_manifest_validated_rows", "vllm_request_success_delta"):
        if exact[key] != rows:
            errors.append(f"{key} {exact[key]} != expected {rows}")
    if exact["endpoint_count"] != 2:
        errors.append(f"endpoint_count {exact['endpoint_count']} != 2")
    for key in ("vllm_num_requests_running_after", "vllm_num_requests_waiting_after"):
        if exact[key] != 0:
            errors.append(f"{key} is not zero")
    if not _zero_list(row.get("actor_worker_failures")):
        errors.append("actor_worker_failures contains a nonzero value")
    rate = _float(row.get("model_request_tokens_per_s"))
    if not math.isfinite(rate) or rate <= 0:
        errors.append("model_request_tokens_per_s is not positive and finite")
    observations = {
        "evidence_cell": str(cell),
        "model_name": row.get("model_name", ""),
        "completion_protocol": row.get("completion_protocol", ""),
        "service_prefix_caching": row.get("service_prefix_caching", ""),
        "token_budget": _int(row.get("token_budget")),
        "gpu_utilization_pct_mean": _float(row.get("gpu_utilization_pct_mean")),
        "vllm_running_mean": _float(row.get("vllm_running_mean")),
        "vllm_running_p95": _float(row.get("vllm_running_p95")),
        "vllm_running_max": _float(row.get("vllm_running_max")),
        "vllm_waiting_mean": _float(row.get("vllm_waiting_mean")),
        "vllm_waiting_max": _float(row.get("vllm_waiting_max")),
        "vllm_kv_cache_usage_mean": _float(row.get("vllm_kv_cache_usage_mean")),
        "vllm_kv_cache_usage_max": _float(row.get("vllm_kv_cache_usage_max")),
        "max_active_work_per_endpoint": _int(row.get("max_active_work_per_endpoint")),
        "max_active_work_per_endpoint_seen": _int(row.get("max_active_work_per_endpoint_seen")),
        "actor_workers_per_endpoint": _int(row.get("actor_workers_per_endpoint")),
        "ray_actor_max_concurrency": _int(row.get("ray_actor_max_concurrency")),
        "per_endpoint_inflight_limit": _int(row.get("per_endpoint_inflight_limit")),
        "operator_wall_s": _float(row.get("operator_wall_s")),
        "vllm_estimated_flops_per_gpu_delta": _float(
            row.get("vllm_estimated_flops_per_gpu_delta")
        ),
    }
    operator_wall_s = float(observations["operator_wall_s"])
    estimated_flops_per_gpu = float(
        observations["vllm_estimated_flops_per_gpu_delta"]
    )
    recovered_mfu = (
        estimated_flops_per_gpu
        / (operator_wall_s * GPU_PEAK_TFLOPS_PER_4090_BF16 * 1e12)
        if operator_wall_s > 0 and estimated_flops_per_gpu > 0
        else math.nan
    )
    observations["mfu_recovered_fraction"] = recovered_mfu
    if not math.isfinite(recovered_mfu) or recovered_mfu <= 0:
        errors.append("project recovered MFU is not positive and finite")
    return rate, str(row.get("request_manifest_sha256", "")), observations, errors


def summarize(
    root: Path,
    *,
    repair_roots: tuple[Path, ...] = (),
    rows: int,
    expected_repeats: int = 3,
    direct_concurrency: int = 32,
    candidate_ks: tuple[int, ...] = (32, 64, 128, 256),
    feeding_floor: float = 0.95,
    project_peak_floor: float = 0.97,
) -> dict[str, Any]:
    evidence_roots = (root,) + tuple(repair_roots)
    runs: list[dict[str, Any]] = []
    records: list[dict[str, Any]] = []
    for evidence_root in evidence_roots:
        run = _read_json(evidence_root / "ramp_run.json")
        runs.append(run)
        for source in run.get("records", []):
            record = dict(source)
            record["_evidence_root"] = str(evidence_root)
            records.append(record)
    errors: list[str] = []
    expected_count = expected_repeats * (1 + len(candidate_ks))
    failed_records = [record for record in records if record.get("status") != "passed"]
    passed_records = [record for record in records if record.get("status") == "passed"]
    if len(passed_records) != expected_count:
        errors.append(
            f"successful record count {len(passed_records)} != expected {expected_count}"
        )
    reported_failed = sum(_int(run.get("n_failed"), 0) for run in runs)
    if reported_failed != len(failed_records):
        errors.append(
            f"reported failed count {reported_failed} != observed {len(failed_records)}"
        )
    incidents = [
        {
            "evidence_root": record["_evidence_root"],
            "arm": record.get("arm"),
            "concurrency": record.get("concurrency"),
            "repeat": record.get("rep"),
            "status": record.get("status"),
            "cell": record.get("cell"),
            "error": record.get("error", ""),
            "exit_code": record.get("exit_code"),
        }
        for record in failed_records
    ]

    direct_records = [r for r in passed_records if r.get("arm") == "bounded_http" and _int(r.get("concurrency")) == direct_concurrency]
    if len(direct_records) != expected_repeats:
        errors.append(f"direct repeat count {len(direct_records)} != expected {expected_repeats}")
    direct_rates: list[float] = []
    direct_observations: list[dict[str, Any]] = []
    manifest_shas: set[str] = set()
    for record in direct_records:
        rate, manifest_sha, observation, cell_errors = _audit_direct_cell(
            _portable_cell(Path(record["_evidence_root"]), rows, record), rows
        )
        direct_rates.append(rate)
        direct_observations.append(observation)
        manifest_shas.add(manifest_sha)
        errors.extend(f"direct rep {record.get('rep')}: {error}" for error in cell_errors)

    project_values: dict[int, list[float]] = {}
    project_observations: dict[int, list[dict[str, Any]]] = {}
    for k in sorted(set(candidate_ks)):
        candidates = [r for r in passed_records if r.get("arm") == "project_static" and _int(r.get("concurrency")) == k]
        if len(candidates) != expected_repeats:
            errors.append(f"project K{k} repeat count {len(candidates)} != expected {expected_repeats}")
        values: list[float] = []
        observations: list[dict[str, Any]] = []
        for record in candidates:
            rate, manifest_sha, obs, cell_errors = _audit_project_cell(
                _portable_cell(Path(record["_evidence_root"]), rows, record), rows
            )
            values.append(rate)
            observations.append(obs)
            manifest_shas.add(manifest_sha)
            errors.extend(f"project K{k} rep {record.get('rep')}: {error}" for error in cell_errors)
        project_values[k] = values
        project_observations[k] = observations

    if len(manifest_shas) != 1 or "" in manifest_shas:
        errors.append(f"cross-arm manifest SHA mismatch: {sorted(manifest_shas)}")

    valid_direct = len(direct_rates) == expected_repeats and all(math.isfinite(v) for v in direct_rates)
    direct_median = statistics.median(direct_rates) if valid_direct else math.nan
    project_medians = {
        k: statistics.median(values)
        for k, values in project_values.items()
        if len(values) == expected_repeats and all(math.isfinite(v) for v in values)
    }
    project_peak = max(project_medians.values(), default=math.nan)
    candidates_passing = [
        k for k, median in project_medians.items()
        if not errors
        and median / direct_median >= feeding_floor
        and median / project_peak >= project_peak_floor
    ] if valid_direct and math.isfinite(project_peak) else []
    selected_k = min(candidates_passing) if candidates_passing else None

    project_summary: dict[str, Any] = {}
    for k, values in sorted(project_values.items()):
        median = project_medians.get(k, math.nan)
        project_summary[str(k)] = {
            "values_tokens_per_s": values,
            "median_tokens_per_s": median,
            "cv": _cv(values) if len(values) == expected_repeats and all(math.isfinite(v) for v in values) else math.nan,
            "feeding_ratio_to_direct_median": median / direct_median if valid_direct and math.isfinite(median) else math.nan,
            "ratio_to_tested_project_peak": median / project_peak if math.isfinite(project_peak) and math.isfinite(median) else math.nan,
            "passes_feeding_floor": bool(valid_direct and math.isfinite(median) and median / direct_median >= feeding_floor),
            "passes_project_peak_floor": bool(math.isfinite(project_peak) and math.isfinite(median) and median / project_peak >= project_peak_floor),
            "mfu_recovered_values_fraction": [
                observation.get("mfu_recovered_fraction", math.nan)
                for observation in project_observations.get(k, [])
            ],
            "mfu_recovered_median_fraction": statistics.median(
                observation.get("mfu_recovered_fraction", math.nan)
                for observation in project_observations.get(k, [])
            ) if project_observations.get(k) else math.nan,
            "observations": project_observations.get(k, []),
        }

    status = "selected" if selected_k is not None else ("audit_failed" if errors else "active_work_scan_required")
    return {
        "schema_version": 1,
        "evidence_root": str(root.resolve()),
        "evidence_roots": [str(path.resolve()) for path in evidence_roots],
        "experiment_id": runs[0].get("experiment_id"),
        "experiment_ids": [run.get("experiment_id") for run in runs],
        "rows": rows,
        "manifest_sha256": next(iter(manifest_shas), "") if len(manifest_shas) == 1 else None,
        "thresholds": {
            "expected_repeats": expected_repeats,
            "feeding_ratio_to_direct_median_min": feeding_floor,
            "ratio_to_tested_project_peak_min": project_peak_floor,
            "selection_rule": "smallest tested K meeting both median-throughput floors after all audit gates pass",
        },
        "mfu_contract": {
            "gpu": "NVIDIA GeForce RTX 4090",
            "precision": "BF16",
            "assumed_peak_tflops_per_gpu": GPU_PEAK_TFLOPS_PER_4090_BF16,
            "direct_formula": "sum(endpoint estimated_flops_delta) / (group_wall_s * 2 * 165e12)",
            "project_formula": "estimated_flops_per_gpu_delta / (operator_wall_s * 165e12)",
            "role": "resource-utilization evidence; does not replace the service-token feeding gate",
        },
        "direct_control": {
            "concurrency_per_endpoint": direct_concurrency,
            "values_tokens_per_s": direct_rates,
            "median_tokens_per_s": direct_median,
            "cv": _cv(direct_rates) if valid_direct else math.nan,
            "mfu_recovered_values_fraction": [
                observation.get("mfu_recovered_fraction", math.nan)
                for observation in direct_observations
            ],
            "mfu_recovered_median_fraction": statistics.median(
                observation.get("mfu_recovered_fraction", math.nan)
                for observation in direct_observations
            ) if direct_observations else math.nan,
            "observations": direct_observations,
        },
        "project_candidates": project_summary,
        "project_peak_median_tokens_per_s": project_peak,
        "selected_k_per_endpoint": selected_k,
        "status": status,
        "audit": {
            "passed": not errors,
            "errors": errors,
            "failed_incident_count": len(incidents),
            "failed_incidents_preserved": incidents,
            "replacement_policy": (
                "failed cells remain incidents; same-config repair roots may contribute "
                "only enough successful cells to reach exactly the expected repeat count"
            ),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--repair-root", action="append", type=Path, default=[])
    parser.add_argument("--rows", required=True, type=int)
    parser.add_argument("--expected-repeats", type=int, default=3)
    parser.add_argument("--direct-concurrency", type=int, default=32)
    parser.add_argument("--candidate-k", type=int, action="append", dest="candidate_ks")
    parser.add_argument("--feeding-floor", type=float, default=0.95)
    parser.add_argument("--project-peak-floor", type=float, default=0.97)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--require-pass", action="store_true")
    args = parser.parse_args()
    result = summarize(
        args.root,
        repair_roots=tuple(args.repair_root),
        rows=args.rows,
        expected_repeats=args.expected_repeats,
        direct_concurrency=args.direct_concurrency,
        candidate_ks=tuple(args.candidate_ks or (32, 64, 128, 256)),
        feeding_floor=args.feeding_floor,
        project_peak_floor=args.project_peak_floor,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(_json_ready(result), indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"status": result["status"], "selected_k_per_endpoint": result["selected_k_per_endpoint"], "output": str(args.output)}, indent=2))
    return 2 if args.require_pass and result["status"] != "selected" else 0


if __name__ == "__main__":
    raise SystemExit(main())
