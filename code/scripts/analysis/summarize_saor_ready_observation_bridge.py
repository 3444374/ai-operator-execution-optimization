#!/usr/bin/env python3
"""Validate and summarize the three-arm SAOR ready-observation bridge.

The bridge separates shared-capacity access from bounded-ready observation while
holding the FIFO selector fixed.  It reports raw effects and never authorizes a
formal claim or labels a project arm as a native-system baseline.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path


EXPECTED = {
    "active_set_project_frozen_static": (
        "static_partition",
        "project_frozen_static_reference",
        "single_head",
    ),
    "active_set_project_single_head_shared_fifo": (
        "shared_fifo",
        "project_policy",
        "single_head",
    ),
    "active_set_project_bounded_ready_fifo": (
        "shared_fifo",
        "project_internal_selector_ablation",
        "bounded_concrete_pre_registration",
    ),
}


def _args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix-root", action="append", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser.parse_args()


def _read(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _write(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _truth(value: object) -> bool:
    return str(value).strip().lower() in {"1", "true"}


def _pair(row: dict[str, str], key: str) -> list[float]:
    values = json.loads(row[key])
    if not isinstance(values, list) or len(values) != 2:
        raise ValueError(f"{key} must contain exactly two values")
    resolved = [float(value) for value in values]
    if any(not math.isfinite(value) for value in resolved):
        raise ValueError(f"{key} contains a non-finite value")
    return resolved


def _cell_metrics(
    round_index: int,
    row: dict[str, str],
    expected: tuple[str, str, str],
) -> tuple[dict[str, object], bool]:
    arrived = _pair(row, "job_arrived_rows")
    completed = _pair(row, "job_completed_rows")
    failed = _pair(row, "job_failed_rows")
    jct = _pair(row, "job_jct_s")
    p99 = _pair(row, "job_p99_s")
    slo = _pair(row, "job_slo_violation_ratio")
    bounded = expected[2] == "bounded_concrete_pre_registration"
    correctness = bool(
        row.get("execution_mode") == "rehearsal"
        and row.get("phase") == "warmup"
        and int(row.get("incidents", "-1")) == 0
        and int(row.get("actor_worker_failures", "-1")) == 0
        and row.get("metrics_status") == "ok"
        and row.get("resource_metrics_status") == "ok"
        and _truth(row.get("active_set_lifecycle_passed"))
        and arrived == completed
        and failed == [0.0, 0.0]
    )
    observation = bool(
        not bounded
        or (
            row.get("bounded_ready_event_status") == "ok:actor_event_join"
            and _truth(row.get("bounded_ready_lifecycle_complete"))
            and int(row.get("bounded_ready_jobs_with_intervals", "0")) == 2
            and int(row.get("bounded_ready_intervals", "0")) >= 2
            and int(row.get("bounded_ready_max_ready_requests_seen", "0")) >= 2
            and int(row.get("bounded_ready_max_ready_work_seen", "0")) > 0
            and int(row.get("bounded_ready_max_ready_payload_bytes_seen", "0")) > 0
        )
    )
    metrics = {
        "round": round_index,
        "scenario_id": row["scenario_id"],
        "policy": expected[0],
        "experiment_identity": expected[1],
        "ready_observation_contract": expected[2],
        "correctness_passed": correctness,
        "observation_passed": observation,
        "cell_evidence_passed": correctness and observation,
        "tokens_per_s": float(row["tokens_per_s"]),
        "group_jct_s": float(row["duration_s"]),
        "bulk_jct_s": jct[0],
        "foreground_jct_s": jct[1],
        "bulk_p99_s": p99[0],
        "foreground_p99_s": p99[1],
        "bulk_slo_violation": slo[0],
        "foreground_slo_violation": slo[1],
        "jain_fairness": float(row.get("jain_fairness", "nan")),
        "mfu_fraction": float(row["mfu_estimate"]),
        "gpu_utilization_pct_mean": row.get("gpu_utilization_pct_mean", ""),
        "host_cpu_busy_cores_p95": row.get("host_cpu_busy_cores_p95", ""),
        "host_memory_used_pct_p95": row.get("host_memory_used_pct_p95", ""),
        "ready_requests_p95": row.get(
            "bounded_ready_requests_transition_p95_max", ""
        ),
        "ready_work_p95": row.get("bounded_ready_work_transition_p95_max", ""),
        "ready_payload_bytes_p95": row.get(
            "bounded_ready_payload_bytes_transition_p95_max", ""
        ),
        "completion_fairness_status": row.get(
            "completion_fairness_status", "unavailable"
        ),
        "completion_service_lag_p95_work": row.get(
            "completion_service_lag_p95_work", ""
        ),
        "longest_no_service_s": row.get("longest_no_service_s", ""),
    }
    return metrics, correctness and observation


def _relative(new: float, old: float) -> float:
    return (new / old - 1.0) * 100.0 if old else math.nan


def _effect(
    round_index: int,
    effect: str,
    before: dict[str, object],
    after: dict[str, object],
) -> dict[str, object]:
    return {
        "round": round_index,
        "effect": effect,
        "before_scenario": before["scenario_id"],
        "after_scenario": after["scenario_id"],
        "tokens_per_s_delta_pct": _relative(
            float(after["tokens_per_s"]), float(before["tokens_per_s"])
        ),
        "group_jct_delta_pct": _relative(
            float(after["group_jct_s"]), float(before["group_jct_s"])
        ),
        "bulk_jct_delta_pct": _relative(
            float(after["bulk_jct_s"]), float(before["bulk_jct_s"])
        ),
        "foreground_p99_delta_pct": _relative(
            float(after["foreground_p99_s"]), float(before["foreground_p99_s"])
        ),
        "foreground_slo_violation_delta_pp": 100.0
        * (
            float(after["foreground_slo_violation"])
            - float(before["foreground_slo_violation"])
        ),
        "mfu_delta_pct": _relative(
            float(after["mfu_fraction"]), float(before["mfu_fraction"])
        ),
    }


def summarize(roots: tuple[Path, ...], output: Path) -> dict[str, object]:
    errors: list[str] = []
    metrics: list[dict[str, object]] = []
    effects: list[dict[str, object]] = []
    signatures: list[tuple[str, str, str]] = []
    if len(roots) not in {1, 2}:
        errors.append("ready-observation bridge requires one or two rehearsal roots")

    for round_index, root in enumerate(roots, start=1):
        try:
            manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
            rows = _read(root / "group_runs.csv")
        except (OSError, json.JSONDecodeError) as exc:
            errors.append(f"round {round_index} evidence is unreadable: {exc}")
            continue
        if (
            manifest.get("status") != "completed"
            or manifest.get("execution_mode") != "rehearsal"
            or manifest.get("incidents")
        ):
            errors.append(f"round {round_index} is not a clean rehearsal")
        signatures.append(
            (
                str(manifest.get("config_fingerprint", "")),
                str(manifest.get("repository_commit", "")),
                json.dumps(
                    manifest.get("redacted_config", {}).get("service_metadata", {}),
                    sort_keys=True,
                ),
            )
        )
        observed = {
            row.get("scenario_id", ""): (
                row.get("policy", ""),
                row.get("experiment_identity", ""),
                row.get("ready_observation_contract", ""),
            )
            for row in rows
        }
        if observed != EXPECTED or len(rows) != len(EXPECTED):
            errors.append(f"round {round_index} does not match the three-arm bridge")
        round_metrics: dict[str, dict[str, object]] = {}
        for scenario_id, expected in EXPECTED.items():
            selected = [row for row in rows if row.get("scenario_id") == scenario_id]
            if len(selected) != 1:
                continue
            cell, passed = _cell_metrics(round_index, selected[0], expected)
            metrics.append(cell)
            round_metrics[scenario_id] = cell
            if not passed:
                errors.append(f"round {round_index} {scenario_id} failed evidence")
        if len(round_metrics) == len(EXPECTED):
            static = round_metrics["active_set_project_frozen_static"]
            single = round_metrics["active_set_project_single_head_shared_fifo"]
            bounded = round_metrics["active_set_project_bounded_ready_fifo"]
            effects.extend(
                (
                    _effect(round_index, "shared_capacity", static, single),
                    _effect(round_index, "bounded_ready_observation", single, bounded),
                )
            )

    if signatures and (
        any(not all(signature) for signature in signatures)
        or len(set(signatures)) != 1
    ):
        errors.append("roots do not share config fingerprint, commit, and service signature")
    output.mkdir(parents=True, exist_ok=True)
    if metrics:
        _write(output / "bridge_metrics.csv", metrics)
    if effects:
        _write(output / "bridge_effects.csv", effects)
    payload: dict[str, object] = {
        "schema_version": 1,
        "status": "passed" if not errors else "failed",
        "conclusion": "bridge_effects_observed" if not errors else "diagnostic_only",
        "experiment_layer": "project_observation_bridge",
        "evaluation_scope": "single_tenant_multi_job",
        "fairness_mode": "differentiated_service",
        "native_baseline_count": 0,
        "round_count": len(roots),
        "shared_capacity_effect_decided": False,
        "ready_observation_effect_decided": False,
        "formal_authorized": False,
        "matrix_roots": [str(root) for root in roots],
        "errors": errors,
    }
    (output / "validation.json").write_text(
        json.dumps(payload, indent=2) + "\n", encoding="utf-8"
    )
    if errors:
        raise ValueError("; ".join(errors))
    return payload


def main() -> int:
    args = _args()
    summarize(
        tuple(root.resolve() for root in args.matrix_root),
        args.output_dir.resolve(),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
