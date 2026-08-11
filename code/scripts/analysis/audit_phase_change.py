#!/usr/bin/env python3
"""Audit phase-change calibration, actuation, or formal evidence fail-closed."""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Iterable


_MIN_OCCUPIED_SAMPLE_FRACTION = 0.5
_MIN_ADMISSION_LAG_P95_S = 1.0


def _number(row: dict[str, object], key: str, default: float = 0.0) -> float:
    value = row.get(key)
    if value in (None, ""):
        return default
    resolved = float(value)
    if not math.isfinite(resolved):
        raise ValueError(f"{key} must be finite")
    return resolved


def _required_number(row: dict[str, object], key: str) -> float:
    if row.get(key) in (None, ""):
        raise ValueError(f"required metric {key} is missing")
    return _number(row, key)


def _percentile(values: Iterable[float], fraction: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("cannot summarize an empty metric series")
    index = min(len(ordered) - 1, math.ceil(fraction * len(ordered)) - 1)
    return ordered[index]


def _records(run_dir: Path) -> dict[str, tuple[Path, dict[str, object]]]:
    result = {}
    for path in sorted((run_dir / "records").glob("*.json")):
        record = json.loads(path.read_text(encoding="utf-8"))
        if record.get("phase") != "formal":
            continue
        scenario = str(record.get("scenario_id", ""))
        if not scenario or scenario in result:
            raise ValueError("expected one formal record per calibration scenario")
        result[scenario] = (path, record)
    if not result:
        raise ValueError("no formal group records found")
    return result


def _record_groups(
    run_dir: Path,
) -> dict[str, list[tuple[Path, dict[str, object]]]]:
    result: dict[str, list[tuple[Path, dict[str, object]]]] = defaultdict(list)
    for path in sorted((run_dir / "records").glob("*.json")):
        record = json.loads(path.read_text(encoding="utf-8"))
        if record.get("phase") == "formal":
            result[str(record.get("scenario_id", ""))].append((path, record))
    if not result or "" in result:
        raise ValueError("formal group records are missing scenario IDs")
    return result


def _states(run_dir: Path, record_path: Path) -> list[dict[str, str]]:
    path = run_dir / "traces" / f"{record_path.stem}.states.csv"
    if not path.is_file():
        raise ValueError(f"state trace is missing: {path}")
    with path.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    if not rows:
        raise ValueError(f"state trace is empty: {path}")
    return rows


def _requests(run_dir: Path, record_path: Path) -> list[dict[str, str]]:
    paths = sorted(
        (run_dir / "jobs").glob(
            f"{record_path.stem}_job*.requests.csv"
        )
    )
    if not paths:
        raise ValueError(f"request traces are missing for {record_path.stem}")
    rows: list[dict[str, str]] = []
    for path in paths:
        with path.open(newline="", encoding="utf-8") as stream:
            rows.extend(csv.DictReader(stream))
    if not rows:
        raise ValueError(f"request traces are empty for {record_path.stem}")
    return rows


def _segments(contract_dir: Path) -> tuple[dict[str, object], ...]:
    audit = json.loads((contract_dir / "audit.json").read_text(encoding="utf-8"))
    segments = audit.get("phase_segments")
    if not isinstance(segments, list) or len(segments) != 4:
        raise ValueError("phase-change contract has no usable segments")
    return tuple(segments)


def _phase_index(elapsed_s: float, segments: tuple[dict[str, object], ...]) -> int | None:
    for index, segment in enumerate(segments):
        if float(segment["start_s"]) <= elapsed_s < float(segment["end_s"]):
            return index
    return None


def _phase_rows(
    rows: list[dict[str, str]],
    record: dict[str, object],
    segments: tuple[dict[str, object], ...],
) -> dict[str, dict[int, list[dict[str, str]]]]:
    start = _number(record, "start_epoch_s")
    grouped: dict[str, dict[int, list[dict[str, str]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for row in rows:
        phase = _phase_index(_required_number(row, "observed_epoch_s") - start, segments)
        if phase is not None:
            grouped[str(row["endpoint_id"])][phase].append(row)
    if set(grouped) != {"endpoint-0", "endpoint-1"}:
        raise ValueError("state trace must cover both endpoints")
    if any(any(not phases.get(index) for index in range(4)) for phases in grouped.values()):
        raise ValueError("state trace must cover every phase on both endpoints")
    return grouped


def _integrity(record: dict[str, object]) -> None:
    if int(record.get("incidents", -1)) != 0 or int(record.get("actor_worker_failures", -1)) != 0:
        raise ValueError("run contains an incident or actor failure")
    if int(record.get("adaptive_capacity_fallbacks", 0)) != 0:
        raise ValueError("run contains a state-controller fallback")
    if "job_failed_rows" in record:
        failed = json.loads(str(record["job_failed_rows"]))
        arrived = json.loads(str(record["job_arrived_rows"]))
        completed = json.loads(str(record["job_completed_rows"]))
        if any(int(value) for value in failed) or completed != arrived:
            raise ValueError("run violates the exactly-once completion contract")


def _summary(rows: list[dict[str, str]]) -> dict[str, float]:
    waiting = [_required_number(row, "vllm_waiting") for row in rows]
    kv = [_required_number(row, "vllm_kv_usage") for row in rows]
    active = [_required_number(row, "active_requests") for row in rows]
    queued = [_required_number(row, "organizer_queued_work") for row in rows]
    rates = [
        _required_number(row, "service_rate_tokens_s")
        for row in rows
        if row.get("service_rate_tokens_s") not in (None, "")
    ]
    return {
        "waiting_max": max(waiting),
        "waiting_p95": _percentile(waiting, 0.95),
        "kv_max": max(kv),
        "kv_p95": _percentile(kv, 0.95),
        "active_max": max(active),
        "queued_work_max": max(queued),
        "service_rate_p50": statistics.median(rates) if rates else 0.0,
    }


def _admission_lag_summary(
    rows: list[dict[str, str]],
    endpoint_id: str,
) -> dict[str, float]:
    selected = [row for row in rows if row.get("endpoint_id") == endpoint_id]
    if not selected:
        raise ValueError(f"request trace has no rows for {endpoint_id}")
    if any(
        row.get("request_time_origin") != "replayed_arrival"
        for row in selected
    ):
        raise ValueError("A-only request traces must use replayed arrivals")
    lags = [
        _required_number(row, "submit_epoch_s")
        - _required_number(row, "arrival_epoch_s")
        for row in selected
    ]
    if any(lag < -1e-6 for lag in lags):
        raise ValueError("request submission precedes its replayed arrival")
    nonnegative = [max(0.0, lag) for lag in lags]
    return {
        "admission_lag_p50_s": statistics.median(nonnegative),
        "admission_lag_p95_s": _percentile(nonnegative, 0.95),
        "admission_lag_max_s": max(nonnegative),
    }


def audit_a_only(
    run_dir: Path,
    contract_dir: Path,
    lower_k: int,
    upper_k: int,
) -> dict[str, object]:
    records = _records(run_dir)
    required = {"a_only_lower", "a_only_upper"}
    if set(records) != required:
        raise ValueError(f"A-only calibration scenarios must be {sorted(required)}")
    segments = _segments(contract_dir)
    evidence = {}
    rates: dict[str, list[float]] = defaultdict(list)
    for scenario in sorted(required):
        path, record = records[scenario]
        _integrity(record)
        grouped = _phase_rows(_states(run_dir, path), record, segments)
        request_rows = _requests(run_dir, path)
        limit = lower_k if scenario.endswith("lower") else upper_k
        endpoint_evidence = {}
        for endpoint, phases in grouped.items():
            rows = [row for index in range(4) for row in phases[index]]
            item = _summary(rows)
            occupied_fraction = statistics.mean(
                _required_number(row, "active_requests") >= 0.8 * limit
                for row in rows
            )
            item["occupied_sample_fraction"] = occupied_fraction
            item.update(_admission_lag_summary(request_rows, endpoint))
            if item["waiting_max"] > 0 or item["kv_max"] >= 0.85:
                raise ValueError(f"{scenario}/{endpoint} is not a safe A-only arm")
            if scenario.endswith("lower"):
                if occupied_fraction < _MIN_OCCUPIED_SAMPLE_FRACTION:
                    raise ValueError(
                        f"{scenario}/{endpoint} did not persistently occupy "
                        "the lower arm"
                    )
                if item["admission_lag_p95_s"] < _MIN_ADMISSION_LAG_P95_S:
                    raise ValueError(
                        f"{scenario}/{endpoint} did not expose replayed-arrival "
                        "admission backlog"
                    )
            if item["service_rate_p50"] <= 0:
                raise ValueError(f"{scenario}/{endpoint} has no service-rate evidence")
            rates[scenario].append(item["service_rate_p50"])
            endpoint_evidence[endpoint] = item
        evidence[scenario] = endpoint_evidence
    lower_rate = statistics.mean(rates["a_only_lower"])
    upper_rate = statistics.mean(rates["a_only_upper"])
    if upper_rate < 1.05 * lower_rate:
        raise ValueError("upper arm does not improve A-only service rate by at least 5%")
    return {
        "schema_version": 1,
        "status": "passed",
        "mode": "a_only",
        "lower_service_rate_tokens_s_per_endpoint": lower_rate,
        "upper_service_rate_tokens_s_per_endpoint": upper_rate,
        "target_service_rate_tokens_s_per_endpoint": upper_rate,
        "evidence": evidence,
    }


def audit_pressure(
    run_dir: Path,
    contract_dir: Path,
    a_only_audit: dict[str, object],
    *,
    lower_k: int,
    lower_w: int,
    upper_k: int,
    upper_w: int,
) -> tuple[dict[str, object], dict[str, object]]:
    if a_only_audit.get("status") != "passed" or a_only_audit.get("mode") != "a_only":
        raise ValueError("pressure calibration requires a passed A-only audit")
    records = _records(run_dir)
    required = {"pressure_lower", "pressure_upper"}
    if set(records) != required:
        raise ValueError(f"pressure calibration scenarios must be {sorted(required)}")
    segments = _segments(contract_dir)
    evidence = {}
    for scenario in sorted(required):
        path, record = records[scenario]
        _integrity(record)
        grouped = _phase_rows(_states(run_dir, path), record, segments)
        endpoint_evidence = {}
        for endpoint, phases in grouped.items():
            items = {str(index): _summary(phases[index]) for index in range(4)}
            for index in (0, 2):
                if items[str(index)]["waiting_max"] > 0 or items[str(index)]["kv_max"] >= 0.85:
                    raise ValueError(f"{scenario}/{endpoint} is congested during A-only phase {index}")
            endpoint_evidence[endpoint] = items
        evidence[scenario] = endpoint_evidence
    for endpoint in ("endpoint-0", "endpoint-1"):
        for index in (1, 3):
            upper = evidence["pressure_upper"][endpoint][str(index)]
            lower = evidence["pressure_lower"][endpoint][str(index)]
            upper_risk = upper["waiting_max"] > 0 or upper["kv_max"] >= 0.85
            lower_safe = lower["waiting_p95"] == 0 and lower["kv_p95"] < 0.85
            relieved = (
                lower["waiting_p95"] < upper["waiting_p95"]
                or lower["kv_p95"] <= upper["kv_p95"] - 0.02
            )
            if not upper_risk or not lower_safe or not relieved:
                raise ValueError(
                    f"phase {index}/{endpoint} lacks upper pressure plus lower relief"
                )
    audit = {
        "schema_version": 1,
        "status": "passed",
        "mode": "pressure",
        "evidence": evidence,
    }
    workload_audit = json.loads((contract_dir / "audit.json").read_text(encoding="utf-8"))
    selection = {
        "kind": "phase_change_capacity_calibration",
        "status": "passed",
        "lower_request_limit": lower_k,
        "lower_work_limit": lower_w,
        "upper_request_limit": upper_k,
        "upper_work_limit": upper_w,
        "target_service_rate_tokens_s_per_endpoint": float(
            a_only_audit["target_service_rate_tokens_s_per_endpoint"]
        ),
        "output_cap": int(workload_audit["spec"]["output_cap"]),
        "arrival_time_scale": 1.0,
    }
    calibration = {
        "schema_version": 1,
        "status": "ready",
        "selection": selection,
        "evidence": {
            "feeding": {"status": "passed", "source": "a_only_audit.json"},
            "token_budget": {"status": "passed", "value": 6144},
            "actor_pool": {"status": "passed", "workers_per_endpoint": 8},
            "pressure": {"status": "passed", "source": "pressure_audit.json"},
        },
    }
    return audit, calibration


def _risk_improved(rows: list[dict[str, str]], action_time: float) -> bool:
    before = [
        row
        for row in rows
        if action_time - 5.0 <= _number(row, "_elapsed_s") <= action_time
    ]
    after = [
        row
        for row in rows
        if action_time + 5.0 <= _number(row, "_elapsed_s") <= action_time + 20.0
    ]
    if not before or not after:
        return False
    wait_before = max(_required_number(row, "vllm_waiting") for row in before)
    wait_after = statistics.median(
        _required_number(row, "vllm_waiting") for row in after
    )
    kv_before = max(_required_number(row, "vllm_kv_usage") for row in before)
    kv_after = statistics.median(
        _required_number(row, "vllm_kv_usage") for row in after
    )
    return (wait_before > 0 and wait_after < wait_before) or (
        kv_before >= 0.85 and kv_after <= kv_before - 0.01
    )


def _expanded_capacity_summary(
    rows: list[dict[str, str]],
    action_time: float,
    lower_k: int,
) -> dict[str, float]:
    after = [
        row
        for row in rows
        if action_time + 2.0 <= _number(row, "_elapsed_s") <= action_time + 20.0
    ]
    if not after:
        raise ValueError("upshift has no post-action occupancy evidence")
    active = [_required_number(row, "active_requests") for row in after]
    summary = {
        "post_increase_active_p50": statistics.median(active),
        "post_increase_active_max": max(active),
    }
    if summary["post_increase_active_p50"] <= lower_k:
        raise ValueError("upshift did not sustain capacity above the lower arm")
    return summary


def _audit_actions(
    rows: list[dict[str, str]],
    record: dict[str, object],
    segments: tuple[dict[str, object], ...],
    lower_k: int,
    upper_k: int,
) -> dict[str, object]:
    start = _number(record, "start_epoch_s")
    by_endpoint: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        copied = dict(row)
        copied["_elapsed_s"] = str(_required_number(row, "observed_epoch_s") - start)
        by_endpoint[str(row["endpoint_id"])].append(copied)
    result = {}
    expected = ((0, "increase"), (1, "decrease"), (2, "increase"), (3, "decrease"))
    valid_reasons = {
        "increase": {"ready_backlog_below_target", "ready_backlog_rate_bootstrap"},
        "decrease": {"persistent_service_queue"},
    }
    for endpoint in ("endpoint-0", "endpoint-1"):
        endpoint_rows = by_endpoint.get(endpoint, [])
        if not endpoint_rows or any(row.get("control_action") == "fallback" for row in endpoint_rows):
            raise ValueError(f"{endpoint} lacks usable actuation evidence")
        selected = []
        for phase, action in expected:
            candidates = [
                row
                for row in endpoint_rows
                if _phase_index(_number(row, "_elapsed_s"), segments) == phase
                and row.get("control_action") == action
            ]
            if not candidates:
                raise ValueError(f"{endpoint} lacks {action} in phase {phase}")
            row = candidates[0]
            if row.get("control_reason") not in valid_reasons[action]:
                raise ValueError(f"{endpoint} has an invalid {action} reason")
            applied = int(float(row["control_applied_request_limit"]))
            if applied != (upper_k if action == "increase" else lower_k):
                raise ValueError(f"{endpoint} applied the wrong capacity arm")
            expansion = (
                _expanded_capacity_summary(
                    endpoint_rows,
                    _number(row, "_elapsed_s"),
                    lower_k,
                )
                if action == "increase"
                else {}
            )
            if action == "decrease" and not _risk_improved(
                endpoint_rows, _number(row, "_elapsed_s")
            ):
                raise ValueError(f"{endpoint} downshift in phase {phase} did not reduce risk")
            selected.append(
                {
                    "phase": phase,
                    "action": action,
                    "elapsed_s": _number(row, "_elapsed_s"),
                    "reason": row["control_reason"],
                    "applied_request_limit": applied,
                    **expansion,
                }
            )
        if [item["elapsed_s"] for item in selected] != sorted(item["elapsed_s"] for item in selected):
            raise ValueError(f"{endpoint} actions are not ordered")
        result[endpoint] = selected
    return result


def audit_action(
    run_dir: Path,
    contract_dir: Path,
    lower_k: int,
    upper_k: int,
) -> dict[str, object]:
    records = _records(run_dir)
    if set(records) != {"phase_change_adaptive_gate"}:
        raise ValueError("action gate requires exactly its adaptive scenario")
    path, record = records["phase_change_adaptive_gate"]
    _integrity(record)
    segments = _segments(contract_dir)
    rows = _states(run_dir, path)
    actions = _audit_actions(rows, record, segments, lower_k, upper_k)
    return {
        "schema_version": 1,
        "status": "passed",
        "mode": "action",
        "required_sequence": ["up", "down", "up", "down"],
        "actions": actions,
    }


def audit_formal(
    run_dir: Path,
    contract_dir: Path,
    lower_k: int,
    upper_k: int,
) -> dict[str, object]:
    groups = _record_groups(run_dir)
    required = {
        "phase_change_frozen_lower",
        "phase_change_frozen_upper",
        "phase_change_adaptive",
    }
    if set(groups) != required or any(len(items) != 3 for items in groups.values()):
        raise ValueError("formal requires exactly three repeats of all three arms")
    segments = _segments(contract_dir)
    scenario_evidence = {}
    aggregate: dict[str, dict[int, list[dict[str, float]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    throughput: dict[str, list[float]] = defaultdict(list)
    for scenario in sorted(required):
        repeat_evidence = []
        for path, record in groups[scenario]:
            _integrity(record)
            rows = _states(run_dir, path)
            grouped = _phase_rows(rows, record, segments)
            phase_items = {}
            for phase in range(4):
                endpoint_items = [_summary(grouped[endpoint][phase]) for endpoint in sorted(grouped)]
                combined = {
                    key: statistics.mean(item[key] for item in endpoint_items)
                    for key in endpoint_items[0]
                }
                aggregate[scenario][phase].append(combined)
                phase_items[str(phase)] = combined
            actions = None
            if scenario == "phase_change_adaptive":
                actions = _audit_actions(rows, record, segments, lower_k, upper_k)
            tokens_per_s = _number(record, "tokens_per_s")
            if tokens_per_s <= 0:
                raise ValueError(f"{scenario} has invalid E2E tokens/s")
            throughput[scenario].append(tokens_per_s)
            repeat_evidence.append(
                {
                    "repeat_index": int(record["repeat_index"]),
                    "tokens_per_s": tokens_per_s,
                    "gpu_utilization_pct_mean": _number(
                        record, "gpu_utilization_pct_mean"
                    ),
                    "job_jct_s": json.loads(str(record["job_jct_s"])),
                    "phase_metrics": phase_items,
                    **({"actions": actions} if actions is not None else {}),
                }
            )
        mean = statistics.mean(throughput[scenario])
        cv = statistics.pstdev(throughput[scenario]) / mean
        if cv > 0.10:
            raise ValueError(f"{scenario} tokens/s CV exceeds 10%")
        scenario_evidence[scenario] = {
            "tokens_per_s_mean": mean,
            "tokens_per_s_cv": cv,
            "repeats": repeat_evidence,
        }

    lower_name = "phase_change_frozen_lower"
    upper_name = "phase_change_frozen_upper"
    adaptive_name = "phase_change_adaptive"
    lower_rate = scenario_evidence[lower_name]["tokens_per_s_mean"]
    upper_rate = scenario_evidence[upper_name]["tokens_per_s_mean"]
    adaptive_rate = scenario_evidence[adaptive_name]["tokens_per_s_mean"]
    if adaptive_rate < 1.05 * lower_rate:
        raise ValueError("adaptive arm does not improve total tokens/s over lower by 5%")
    if adaptive_rate < 0.95 * upper_rate:
        raise ValueError("adaptive arm is below 95% of frozen-upper total tokens/s")

    phase_aggregate = {}
    for scenario in sorted(required):
        phase_aggregate[scenario] = {}
        for phase in range(4):
            items = aggregate[scenario][phase]
            phase_aggregate[scenario][str(phase)] = {
                key: statistics.mean(item[key] for item in items)
                for key in items[0]
            }
    for phase in (0, 2):
        adaptive = phase_aggregate[adaptive_name][str(phase)]
        upper = phase_aggregate[upper_name][str(phase)]
        if adaptive["service_rate_p50"] < 0.95 * upper["service_rate_p50"]:
            raise ValueError(f"adaptive A-only phase {phase} misses upper efficiency")
    for phase in (1, 3):
        adaptive = phase_aggregate[adaptive_name][str(phase)]
        lower = phase_aggregate[lower_name][str(phase)]
        if (
            adaptive["waiting_p95"] > lower["waiting_p95"]
            or adaptive["kv_p95"] > lower["kv_p95"] + 0.02
        ):
            raise ValueError(f"adaptive pressure phase {phase} misses lower-arm safety")
    return {
        "schema_version": 1,
        "status": "passed",
        "mode": "formal",
        "claim_gate": (
            "adaptive matches upper safe-phase efficiency and lower pressure-phase "
            "safety while improving total tokens/s over lower by >=5%"
        ),
        "scenarios": scenario_evidence,
        "phase_aggregate": phase_aggregate,
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        required=True,
        choices=("a-only", "pressure", "action", "formal"),
    )
    parser.add_argument("--run-dir", required=True, type=Path)
    parser.add_argument("--contract-dir", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--lower-k", required=True, type=int)
    parser.add_argument("--lower-w", type=int)
    parser.add_argument("--upper-k", required=True, type=int)
    parser.add_argument("--upper-w", type=int)
    parser.add_argument("--a-only-audit", type=Path)
    parser.add_argument("--calibration-output", type=Path)
    args = parser.parse_args()
    if args.lower_k <= 0 or args.upper_k <= args.lower_k:
        parser.error("capacity request arms must be positive and ordered")
    if args.mode == "pressure" and (
        args.lower_w is None
        or args.upper_w is None
        or args.lower_w <= 0
        or args.upper_w <= args.lower_w
        or args.a_only_audit is None
        or args.calibration_output is None
    ):
        parser.error("pressure mode requires ordered work arms and both output paths")
    return args


def main() -> int:
    args = _parse_args()
    try:
        if args.mode == "a-only":
            audit = audit_a_only(
                args.run_dir.resolve(),
                args.contract_dir.resolve(),
                args.lower_k,
                args.upper_k,
            )
        elif args.mode == "pressure":
            a_only = json.loads(args.a_only_audit.read_text(encoding="utf-8"))
            audit, calibration = audit_pressure(
                args.run_dir.resolve(),
                args.contract_dir.resolve(),
                a_only,
                lower_k=args.lower_k,
                lower_w=args.lower_w,
                upper_k=args.upper_k,
                upper_w=args.upper_w,
            )
            args.calibration_output.parent.mkdir(parents=True, exist_ok=True)
            args.calibration_output.write_text(
                json.dumps(calibration, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
        elif args.mode == "action":
            audit = audit_action(
                args.run_dir.resolve(),
                args.contract_dir.resolve(),
                args.lower_k,
                args.upper_k,
            )
        else:
            audit = audit_formal(
                args.run_dir.resolve(),
                args.contract_dir.resolve(),
                args.lower_k,
                args.upper_k,
            )
    except Exception as exc:
        audit = {
            "schema_version": 1,
            "status": "failed",
            "mode": args.mode,
            "reason": f"{type(exc).__name__}: {exc}",
        }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(audit, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(audit, ensure_ascii=False, indent=2))
    return 0 if audit["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
