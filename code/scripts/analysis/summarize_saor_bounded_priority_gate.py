#!/usr/bin/env python3
"""Fail-closed two-round development gate for bounded-priority SAOR."""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from pathlib import Path


EXPECTED = {
    "active_set_static_partition": "static_partition",
    "active_set_saor_release": "saor_release",
    "active_set_saor_bounded_priority_0125k": "saor_bounded_priority",
    "active_set_saor_bounded_priority_025k": "saor_bounded_priority",
}
P99_LIMIT_S = 30.7
FOREGROUND_SLO_LIMIT = 0.01
BULK_SLO_LIMIT = 0.723
THROUGHPUT_FLOOR = 9984.0


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


def _array(row: dict[str, str], key: str) -> list[float]:
    values = json.loads(row[key])
    if not isinstance(values, list) or len(values) != 2:
        raise ValueError(f"{key} must contain two values")
    resolved = [float(value) for value in values]
    if any(not math.isfinite(value) for value in resolved):
        raise ValueError(f"{key} contains non-finite values")
    return resolved


def _truth(value: object) -> bool:
    return str(value).strip().lower() in {"1", "true"}


def _event_summary(events: list[dict[str, str]]) -> dict[str, int | bool]:
    sequences: dict[str, list[int]] = {}
    recovery_max = 0
    for event in events:
        sequences.setdefault(event["endpoint_id"], []).append(
            int(event["event_seq"])
        )
        recovery_max = max(
            recovery_max,
            len(json.loads(event.get("recovery_inflight_by_job", "[]"))),
        )
    complete = bool(events) and all(
        sequence == list(range(1, len(sequence) + 1))
        for sequence in sequences.values()
    )
    return {
        "event_sequence_complete": complete,
        "event_count": len(events),
        "slo_priority_grants": sum(
            event.get("action") == "grant"
            and event.get("tier") == "slo_priority"
            for event in events
        ),
        "debt_recovery_grants": sum(
            event.get("action") == "grant"
            and event.get("tier") == "debt_recovery"
            for event in events
        ),
        "constraint_conflicts": sum(
            _truth(event.get("constraint_conflict")) for event in events
        ),
        "recovery_inflight_max": recovery_max,
        "avoidable_idle_events": sum(
            _truth(event.get("avoidable_idle")) for event in events
        ),
        "foreign_grant_events": sum(
            _truth(event.get("foreign_grant_over_debt_critical"))
            for event in events
        ),
    }


def summarize(roots: tuple[Path, ...], output: Path) -> dict[str, object]:
    errors: list[str] = []
    gate_rows: list[dict[str, object]] = []
    mechanism_rows: list[dict[str, object]] = []
    identities: list[tuple[str, str, str]] = []
    if len(roots) != 2:
        errors.append("bounded gate requires exactly two matrix roots")
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
        service_signature = json.dumps(
            manifest.get("redacted_config", {}).get("service_metadata", {}),
            sort_keys=True,
        )
        identities.append(
            (
                str(manifest.get("config_fingerprint", "")),
                str(manifest.get("repository_commit", "")),
                service_signature,
            )
        )
        observed = {row.get("scenario_id", ""): row.get("policy", "") for row in rows}
        if observed != EXPECTED or len(rows) != 4:
            errors.append(f"round {round_index} does not contain the frozen four arms")
        for scenario_id, policy in EXPECTED.items():
            selected = [row for row in rows if row.get("scenario_id") == scenario_id]
            if len(selected) != 1:
                continue
            row = selected[0]
            correctness = bool(
                row.get("execution_mode") == "rehearsal"
                and row.get("phase") == "warmup"
                and int(row.get("incidents", "-1")) == 0
                and row.get("metrics_status") == "ok"
                and row.get("resource_metrics_status") == "ok"
                and int(row.get("actor_worker_failures", "-1")) == 0
                and _truth(row.get("active_set_lifecycle_passed"))
                and _array(row, "job_arrived_rows")
                == _array(row, "job_completed_rows")
                and _array(row, "job_failed_rows") == [0.0, 0.0]
            )
            p99 = _array(row, "job_p99_s")
            slo = _array(row, "job_slo_violation_ratio")
            throughput = float(row["tokens_per_s"])
            gate = {
                "round": round_index,
                "scenario_id": scenario_id,
                "policy": policy,
                "correctness_passed": correctness,
                "tokens_per_s": throughput,
                "foreground_p99_s": p99[1],
                "foreground_slo_violation": slo[1],
                "bulk_slo_violation": slo[0],
                "foreground_passed": p99[1] <= P99_LIMIT_S and slo[1] <= FOREGROUND_SLO_LIMIT,
                "efficiency_passed": throughput >= THROUGHPUT_FLOOR,
                "bulk_protection_passed": slo[0] <= BULK_SLO_LIMIT,
            }
            if policy == "saor_bounded_priority":
                event_path = root / row.get("release_event_trace_path", "")
                if not event_path.is_file():
                    errors.append(
                        f"round {round_index} {scenario_id} event ledger is missing"
                    )
                    mechanism = _event_summary([])
                else:
                    mechanism = _event_summary(_read(event_path))
                    if not mechanism["event_sequence_complete"]:
                        errors.append(
                            f"round {round_index} {scenario_id} event ledger "
                            "has a gap, duplicate, or empty sequence"
                        )
                mechanism_passed = bool(
                    mechanism["event_sequence_complete"]
                    and mechanism["slo_priority_grants"] >= 1
                    and mechanism["debt_recovery_grants"] >= 1
                    and mechanism["avoidable_idle_events"] == 0
                    and mechanism["foreign_grant_events"] == 0
                    and mechanism["recovery_inflight_max"] <= 1
                )
                mechanism_rows.append(
                    {"round": round_index, "scenario_id": scenario_id, **mechanism}
                )
            else:
                mechanism_passed = True
            gate["mechanism_passed"] = mechanism_passed
            gate["all_gates_passed"] = bool(
                correctness
                and gate["foreground_passed"]
                and gate["efficiency_passed"]
                and gate["bulk_protection_passed"]
                and mechanism_passed
            )
            gate_rows.append(gate)
    if identities and (any(not all(identity) for identity in identities) or len(set(identities)) != 1):
        errors.append("rounds do not share config fingerprint, commit, and service signature")
    caps = tuple(EXPECTED)[-2:]
    cap_pass = {
        cap: len([row for row in gate_rows if row["scenario_id"] == cap]) == 2
        and all(row["all_gates_passed"] for row in gate_rows if row["scenario_id"] == cap)
        for cap in caps
    }
    if any(cap_pass.values()):
        conclusion = "formal_registration_candidate"
    elif all(
        row["foreground_passed"]
        for row in gate_rows
        if row["scenario_id"] in caps
    ) and any(row["scenario_id"] in caps for row in gate_rows):
        conclusion = "constraint_conflict_stop"
    else:
        conclusion = "diagnostic_only"
    status = "passed" if not errors else "failed"
    output.mkdir(parents=True, exist_ok=True)
    if gate_rows:
        _write(output / "gate_summary.csv", gate_rows)
    if mechanism_rows:
        _write(output / "mechanism_summary.csv", mechanism_rows)
    payload: dict[str, object] = {
        "schema_version": 1,
        "status": status,
        "conclusion": conclusion if not errors else "diagnostic_only",
        "matrix_roots": [str(root) for root in roots],
        "cap_passed_both_rounds": cap_pass,
        "thresholds": {
            "foreground_p99_s": P99_LIMIT_S,
            "foreground_slo_violation": FOREGROUND_SLO_LIMIT,
            "bulk_slo_violation": BULK_SLO_LIMIT,
            "tokens_per_s": THROUGHPUT_FLOOR,
        },
        "slowdown_is_diagnostic_only": True,
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
    summarize(tuple(path.resolve() for path in args.matrix_root), args.output_dir.resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
