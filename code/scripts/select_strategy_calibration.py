#!/usr/bin/env python3
"""Select and freeze downstream strategy parameters from completed calibration."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shlex
import statistics
import subprocess
from collections import defaultdict
from pathlib import Path


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_formal_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = [
            row
            for row in csv.DictReader(handle)
            if row.get("phase") == "formal"
        ]
    if not rows:
        raise ValueError(f"no formal rows in {path}")
    return rows


def _require_successful_repeats(
    rows: list[dict[str, str]],
    *,
    label: str,
    minimum: int,
) -> None:
    repeats = {int(row["repeat_index"]) for row in rows}
    if len(repeats) < minimum:
        raise ValueError(
            f"{label} has {len(repeats)} formal repeats; need {minimum}"
        )
    for row in rows:
        if row.get("status") != "ok":
            raise ValueError(f"{label} contains non-ok formal rows")
        failures = [
            int(value)
            for value in row.get("actor_worker_failures", "0").split(";")
            if value
        ]
        if any(failures):
            raise ValueError(f"{label} contains actor worker failures")


def _unique_int(rows: list[dict[str, str]], key: str) -> int:
    values = {int(row[key]) for row in rows}
    if len(values) != 1:
        raise ValueError(f"{key} is not constant: {sorted(values)}")
    return values.pop()


def _median(rows: list[dict[str, str]], key: str) -> float:
    return statistics.median(float(row[key]) for row in rows)


def _direct_baseline_metrics(root: Path, cell_id: str) -> tuple[float, int]:
    cell_root = root / cell_id
    gate = json.loads((cell_root / "gate.json").read_text(encoding="utf-8"))
    if gate.get("passed") is not True:
        raise ValueError(f"direct baseline gate did not pass: {cell_id}")
    summaries = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted(cell_root.glob("shard_*/summary.json"))
    ]
    if len(summaries) != 2:
        raise ValueError(f"direct baseline must contain two shards: {cell_id}")
    for summary in summaries:
        if (
            summary.get("status") != "completed"
            or summary.get("failed_count") != 0
            or summary.get("worker_failures") != 0
            or summary.get("exactly_once") is not True
        ):
            raise ValueError(f"direct baseline shard failed: {cell_id}")
    jct_s = max(float(item["jct_s"]) for item in summaries)
    total_tokens = sum(int(item["total_tokens"]) for item in summaries)
    return total_tokens / jct_s, total_tokens


def _select_token_budget(
    rows: list[dict[str, str]],
    *,
    minimum_repeats: int,
    ceiling_ratio: float,
    next_gain_limit: float,
) -> tuple[int, dict[int, float]]:
    grouped: dict[int, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[int(row["token_budget"])].append(row)
    medians = {}
    for budget, items in grouped.items():
        _require_successful_repeats(
            items,
            label=f"token_budget={budget}",
            minimum=minimum_repeats,
        )
        medians[budget] = _median(items, "model_request_tokens_per_s")

    safe_max = max(medians.values())
    ordered = sorted(medians)
    for index, budget in enumerate(ordered):
        throughput = medians[budget]
        if throughput < ceiling_ratio * safe_max:
            continue
        if index + 1 == len(ordered):
            return budget, medians
        next_throughput = medians[ordered[index + 1]]
        next_gain = (next_throughput - throughput) / throughput
        if next_gain < next_gain_limit:
            return budget, medians
    raise ValueError("no token budget satisfies the frozen selection rule")


def build_selection(
    *,
    feeding_runs: Path,
    feeding_scenario: str,
    direct_baseline_root: Path,
    direct_cell: str,
    token_budget_runs: Path,
    minimum_repeats: int,
    minimum_feeding_ratio: float,
) -> dict[str, object]:
    feeding_all = _read_formal_rows(feeding_runs)
    feeding = [
        row for row in feeding_all if row["scenario_id"] == feeding_scenario
    ]
    _require_successful_repeats(
        feeding,
        label=f"feeding scenario {feeding_scenario}",
        minimum=minimum_repeats,
    )
    project_tps = _median(feeding, "model_request_tokens_per_s")
    direct_tps, direct_tokens = _direct_baseline_metrics(
        direct_baseline_root,
        direct_cell,
    )
    feeding_ratio = project_tps / direct_tps
    if feeding_ratio < minimum_feeding_ratio:
        raise ValueError(
            f"feeding ratio {feeding_ratio:.6f} is below "
            f"{minimum_feeding_ratio:.6f}"
        )

    budget_rows = _read_formal_rows(token_budget_runs)
    best_budget, budget_medians = _select_token_budget(
        budget_rows,
        minimum_repeats=minimum_repeats,
        ceiling_ratio=0.97,
        next_gain_limit=0.03,
    )
    selected = {
        "best_token_budget": best_budget,
        "project_static_k_per_endpoint": _unique_int(
            budget_rows,
            "per_endpoint_inflight_limit",
        ),
        "project_active_work_per_endpoint": _unique_int(
            budget_rows,
            "max_active_work_per_endpoint",
        ),
        "project_actor_workers_per_endpoint": _unique_int(
            budget_rows,
            "actor_workers_per_endpoint",
        ),
        "project_ray_actor_max_concurrency": _unique_int(
            budget_rows,
            "ray_actor_max_concurrency",
        ),
    }
    repository_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return {
        "schema_version": 1,
        "status": "ready",
        "repository_commit": repository_commit,
        "selection": selected,
        "evidence": {
            "feeding": {
                "status": "passed",
                "runs_path": str(feeding_runs.resolve()),
                "runs_sha256": _sha256(feeding_runs),
                "scenario_id": feeding_scenario,
                "formal_repeats": len(
                    {int(row["repeat_index"]) for row in feeding}
                ),
                "project_model_request_tokens_per_s_median": project_tps,
                "direct_cell": direct_cell,
                "direct_total_tokens": direct_tokens,
                "direct_tokens_per_s": direct_tps,
                "project_to_direct_ratio": feeding_ratio,
                "minimum_ratio": minimum_feeding_ratio,
            },
            "token_budget": {
                "status": "passed",
                "runs_path": str(token_budget_runs.resolve()),
                "runs_sha256": _sha256(token_budget_runs),
                "formal_repeats_per_budget": minimum_repeats,
                "selection_rule": {
                    "ceiling_ratio": 0.97,
                    "next_gain_limit": 0.03,
                },
                "model_request_tokens_per_s_median": {
                    str(key): value
                    for key, value in sorted(budget_medians.items())
                },
            },
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--feeding-runs", required=True, type=Path)
    parser.add_argument("--feeding-scenario", default="fixed16_c16")
    parser.add_argument("--direct-baseline-root", required=True, type=Path)
    parser.add_argument("--direct-cell", default="bounded_fixed16_c16")
    parser.add_argument("--token-budget-runs", required=True, type=Path)
    parser.add_argument("--minimum-repeats", type=int, default=3)
    parser.add_argument("--minimum-feeding-ratio", type=float, default=0.95)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--env-output", type=Path)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    selection = build_selection(
        feeding_runs=args.feeding_runs,
        feeding_scenario=args.feeding_scenario,
        direct_baseline_root=args.direct_baseline_root,
        direct_cell=args.direct_cell,
        token_budget_runs=args.token_budget_runs,
        minimum_repeats=args.minimum_repeats,
        minimum_feeding_ratio=args.minimum_feeding_ratio,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_name(f"{args.output.name}.tmp")
    temporary.write_text(
        json.dumps(selection, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(args.output)
    if args.env_output is not None:
        values = selection["selection"]
        assert isinstance(values, dict)
        env_values = {
            "STRATEGY_CALIBRATION_SELECTION": str(args.output.resolve()),
            "BEST_TOKEN_BUDGET": values["best_token_budget"],
            "PROJECT_STATIC_K_PER_ENDPOINT": (
                values["project_static_k_per_endpoint"]
            ),
            "PROJECT_ACTIVE_WORK_PER_ENDPOINT": (
                values["project_active_work_per_endpoint"]
            ),
            "PROJECT_ACTOR_WORKERS_PER_ENDPOINT": (
                values["project_actor_workers_per_endpoint"]
            ),
            "PROJECT_RAY_ACTOR_MAX_CONCURRENCY": (
                values["project_ray_actor_max_concurrency"]
            ),
        }
        args.env_output.parent.mkdir(parents=True, exist_ok=True)
        env_temporary = args.env_output.with_name(
            f"{args.env_output.name}.tmp"
        )
        env_temporary.write_text(
            "".join(
                f"{key}={shlex.quote(str(value))}\n"
                for key, value in env_values.items()
            ),
            encoding="utf-8",
        )
        env_temporary.replace(args.env_output)
    print(json.dumps(selection["selection"], sort_keys=True))


if __name__ == "__main__":
    main()
