#!/usr/bin/env python3
"""Fail-closed evidence summary for Project matched-ready selector ablations.

This tool validates experimental identity and evidence completeness.  It does
not call any arm a native baseline and does not decide that the proposed
selector wins; that decision still requires the pre-registered effect and
non-inferiority contract described in the experiment plan.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path

from src.experiments.shared_vllm.metrics import (
    completion_accounted_service_fairness,
)


EXPECTED = {
    "active_set_project_frozen_static": (
        "static_partition",
        "project_frozen_static_reference",
        "single_head",
    ),
    "active_set_project_bounded_ready_fifo": (
        "shared_fifo",
        "project_internal_selector_ablation",
        "bounded_concrete_pre_registration",
    ),
    "active_set_project_bounded_ready_drr": (
        "shared_drr",
        "project_internal_selector_ablation",
        "bounded_concrete_pre_registration",
    ),
    "active_set_project_bounded_ready_vtc_style": (
        "external_vtc",
        "project_internal_selector_ablation",
        "bounded_concrete_pre_registration",
    ),
    "active_set_project_bounded_ready_strict_priority": (
        "foreground_strict_priority",
        "project_internal_selector_ablation",
        "bounded_concrete_pre_registration",
    ),
    "active_set_project_bounded_ready_guarded_debt_0125we": (
        "saor_bounded_ready",
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


def _array(row: dict[str, str], key: str) -> list[float]:
    values = json.loads(row[key])
    if not isinstance(values, list) or len(values) != 2:
        raise ValueError(f"{key} must contain exactly two values")
    resolved = [float(value) for value in values]
    if any(not math.isfinite(value) for value in resolved):
        raise ValueError(f"{key} contains a non-finite value")
    return resolved


def _completion_fairness_from_raw(
    root: Path,
    row: dict[str, str],
) -> dict[str, float | int | str]:
    """Replay registered-ready service fairness from per-request raw traces."""

    if not (root / "jobs").is_dir():
        return completion_accounted_service_fairness(
            [
                {
                    "ready_lifecycle_complete": False,
                    "ready_lifecycle_rows": [],
                }
                for _index in range(2)
            ],
            (1, 1),
        )
    evidence = []
    order = int(row["order_index"])
    phase = row["phase"]
    repeat = int(row["repeat_index"])
    scenario = row["scenario_id"]
    for job_index in range(2):
        stem = (
            f"{order:03d}_{phase}_{repeat}_{scenario}_job{job_index}"
        )
        request_path = root / "jobs" / f"{stem}.requests.csv"
        submission_path = root / "jobs" / f"{stem}.submissions.csv"
        if not request_path.is_file() or not submission_path.is_file():
            return completion_accounted_service_fairness(
                [
                    {
                        "ready_lifecycle_complete": False,
                        "ready_lifecycle_rows": [],
                    }
                    for _index in range(2)
                ],
                (1, 1),
            )
        requests = _read(request_path)
        submissions = _read(submission_path)
        service_by_id = {}
        for request in requests:
            submission_id = str(request.get("submission_id", "") or "")
            actual_output = request.get("actual_output_tokens", "")
            output_work = int(
                actual_output
                if actual_output not in (None, "")
                else request.get("client_estimated_output_tokens", "")
                or request["estimated_output_tokens"]
            )
            service_by_id[submission_id] = (
                float(request["completion_epoch_s"]),
                int(request["prompt_tokens"]) + output_work,
            )
        lifecycle = []
        for submission in submissions:
            ready = submission.get("ready_epoch_s", "")
            registered = submission.get("credit_registered_epoch_s", "")
            granted = submission.get("credit_granted_epoch_s", "")
            if not ready and not registered and not granted:
                continue
            submission_id = str(submission.get("submission_id", "") or "")
            if (
                not ready
                or not registered
                or not granted
                or submission_id not in service_by_id
            ):
                raise ValueError(
                    f"{stem} has an incomplete registered-ready service join"
                )
            completion, work = service_by_id[submission_id]
            lifecycle.append(
                {
                    "registered_epoch_s": float(registered),
                    "completion_epoch_s": completion,
                    "actual_work": work,
                }
            )
        evidence.append(
            {
                "ready_lifecycle_complete": (
                    bool(requests) and len(lifecycle) == len(requests)
                ),
                "ready_lifecycle_rows": lifecycle,
            }
        )
    return completion_accounted_service_fairness(evidence, (1, 1))


def summarize(
    roots: tuple[Path, ...],
    output: Path,
) -> dict[str, object]:
    errors: list[str] = []
    metrics: list[dict[str, object]] = []
    identities: list[tuple[str, str, str]] = []
    if len(roots) not in {1, 2}:
        errors.append("matched-ready attribution requires one or two rehearsal roots")

    for round_index, root in enumerate(roots, start=1):
        try:
            manifest = json.loads(
                (root / "manifest.json").read_text(encoding="utf-8")
            )
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
        identities.append(
            (
                str(manifest.get("config_fingerprint", "")),
                str(manifest.get("repository_commit", "")),
                json.dumps(
                    manifest.get("redacted_config", {}).get(
                        "service_metadata", {}
                    ),
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
            errors.append(
                f"round {round_index} does not match the six-arm Project "
                "internal ablation identity"
            )
        for scenario_id, expected_identity in EXPECTED.items():
            selected = [row for row in rows if row.get("scenario_id") == scenario_id]
            if len(selected) != 1:
                continue
            row = selected[0]
            arrived = _array(row, "job_arrived_rows")
            completed = _array(row, "job_completed_rows")
            failed = _array(row, "job_failed_rows")
            p99 = _array(row, "job_p99_s")
            slo = _array(row, "job_slo_violation_ratio")
            jct = _array(row, "job_jct_s")
            bounded = expected_identity[2] == "bounded_concrete_pre_registration"
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
                    row.get("bounded_ready_event_status")
                    == "ok:actor_event_join"
                    and _truth(row.get("bounded_ready_lifecycle_complete"))
                    and int(row.get("bounded_ready_jobs_with_intervals", "0"))
                    == 2
                    and int(row.get("bounded_ready_intervals", "0")) >= 2
                    and int(
                        row.get("bounded_ready_max_ready_requests_seen", "0")
                    )
                    >= 2
                    and int(row.get("bounded_ready_max_ready_work_seen", "0"))
                    > 0
                    and int(
                        row.get(
                            "bounded_ready_max_ready_payload_bytes_seen", "0"
                        )
                    )
                    > 0
                )
            )
            proposed = expected_identity[0] == "saor_bounded_ready"
            completion_fairness = _completion_fairness_from_raw(root, row)
            fairness_evidence = bool(
                completion_fairness["completion_service_lag_status"]
                == "ok:registered_backlog_completion_accounted_empirical"
            )
            mechanism = bool(
                not proposed
                or (
                    row.get("bounded_saor_event_status")
                    == "ok:lossless_ledger"
                    and _truth(row.get("bounded_saor_event_sequence_complete"))
                    and int(row.get("bounded_saor_slo_priority_grants", "0"))
                    >= 1
                    and int(row.get("bounded_saor_debt_recovery_grants", "0"))
                    >= 1
                    and int(row.get("bounded_saor_avoidable_idle_events", "-1"))
                    == 0
                    and int(
                        row.get(
                            "bounded_saor_foreign_grant_over_debt_critical_events",
                            "-1",
                        )
                    )
                    == 0
                    and int(row.get("bounded_saor_recovery_inflight_max", "2"))
                    <= 1
                    and int(row.get("bounded_ready_foreign_fallback_events", "-1"))
                    == 0
                )
            )
            cell_passed = (
                correctness
                and observation
                and mechanism
                and fairness_evidence
            )
            if not cell_passed:
                errors.append(
                    f"round {round_index} {scenario_id} failed correctness, "
                    "observation, mechanism, or completion-fairness evidence"
                )
            metrics.append(
                {
                    "round": round_index,
                    "scenario_id": scenario_id,
                    "policy": expected_identity[0],
                    "experiment_identity": expected_identity[1],
                    "ready_observation_contract": expected_identity[2],
                    "correctness_passed": correctness,
                    "observation_passed": observation,
                    "mechanism_passed": mechanism,
                    "fairness_evidence_passed": fairness_evidence,
                    "cell_evidence_passed": cell_passed,
                    "evaluation_scope": "single_tenant_multi_job",
                    "fairness_mode": "differentiated_service",
                    "tokens_per_s": float(row["tokens_per_s"]),
                    "group_jct_s": float(row["duration_s"]),
                    "mfu_fraction": float(row["mfu_estimate"]),
                    "bulk_jct_s": jct[0],
                    "foreground_jct_s": jct[1],
                    "bulk_p99_s": p99[0],
                    "foreground_p99_s": p99[1],
                    "bulk_slo_violation": slo[0],
                    "foreground_slo_violation": slo[1],
                    "jain_fairness": float(row.get("jain_fairness", "nan")),
                    "max_overlap_service_disparity_work": float(
                        row.get(
                            "max_overlap_normalized_service_disparity",
                            "0",
                        )
                    ),
                    **completion_fairness,
                    "ready_requests_p95": row.get(
                        "bounded_ready_requests_transition_p95_max", ""
                    ),
                    "ready_work_p95": row.get(
                        "bounded_ready_work_transition_p95_max", ""
                    ),
                    "ready_payload_bytes_p95": row.get(
                        "bounded_ready_payload_bytes_transition_p95_max", ""
                    ),
                    "host_cpu_busy_cores_p95": row.get(
                        "host_cpu_busy_cores_p95", ""
                    ),
                    "host_memory_used_pct_p95": row.get(
                        "host_memory_used_pct_p95", ""
                    ),
                }
            )
    if identities and (
        any(not all(identity) for identity in identities)
        or len(set(identities)) != 1
    ):
        errors.append(
            "rehearsal roots do not share config fingerprint, commit, and "
            "service signature"
        )

    output.mkdir(parents=True, exist_ok=True)
    if metrics:
        _write(output / "ablation_metrics.csv", metrics)
    payload: dict[str, object] = {
        "schema_version": 1,
        "status": "passed" if not errors else "failed",
        "conclusion": (
            "ready_for_preregistered_pareto_review"
            if not errors
            else "diagnostic_only"
        ),
        "experiment_layer": "project_internal_selector_ablation",
        "evaluation_scope": "single_tenant_multi_job",
        "fairness_mode": "differentiated_service",
        "native_baseline_count": 0,
        "matrix_roots": [str(root) for root in roots],
        "round_count": len(roots),
        "selector_victory_decided": False,
        "formal_authorized": False,
        "errors": errors,
    }
    (output / "validation.json").write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )
    if errors:
        raise ValueError("; ".join(errors))
    return payload


def main() -> int:
    args = _args()
    summarize(
        tuple(path.resolve() for path in args.matrix_root),
        args.output_dir.resolve(),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
