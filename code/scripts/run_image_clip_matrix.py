#!/usr/bin/env python3
"""Run an interleaved image execution matrix with fail-closed result gates.

The runner turns a JSON scenario matrix into isolated ``run_image_clip_e2e.py``
calls, records the seeded order, and stops on correctness or steady-duration
violations. Raw CSV, per-run manifests, logs, and the orchestration manifest all
remain under one output directory.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path


CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.experiment_scenarios import (  # noqa: E402
    ScheduledScenarioRun,
    build_scenario_schedule,
)
from src.runner_lease import acquire_runner_lease  # noqa: E402


_SCENARIO_ID = re.compile(r"^[A-Za-z0-9_.-]+$")
_OWNED_FLAGS = {"--out-csv", "--out-manifest", "--phase", "--repeat-index"}


@dataclass(frozen=True)
class Scenario:
    scenario_id: str
    args: tuple[str, ...]


@dataclass(frozen=True)
class MatrixConfig:
    experiment_id: str
    seed: int
    warmup_runs_per_scenario: int
    formal_repeats: int
    minimum_unique_rows: int
    minimum_steady_state_s: float
    common_args: tuple[str, ...]
    scenarios: tuple[Scenario, ...]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--image-runner", required=True, type=Path)
    parser.add_argument("--python-executable", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--recover-stale-lease", action="store_true")
    args = parser.parse_args(argv)
    if args.recover_stale_lease and not args.resume:
        parser.error("--recover-stale-lease requires --resume")
    return args


def load_config(path: Path) -> MatrixConfig:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or raw.get("schema_version") != 1:
        raise ValueError("image matrix config schema_version must be 1")
    scenarios_raw = raw.get("scenarios")
    if not isinstance(scenarios_raw, list) or not scenarios_raw:
        raise ValueError("scenarios must be a non-empty list")
    scenarios = tuple(
        Scenario(
            scenario_id=_required_text(item, "scenario_id"),
            args=_string_tuple(item.get("args"), "scenario args"),
        )
        for item in scenarios_raw
        if isinstance(item, dict)
    )
    if len(scenarios) != len(scenarios_raw):
        raise ValueError("every scenario must be an object")
    ids = [item.scenario_id for item in scenarios]
    if any(not _SCENARIO_ID.fullmatch(item) for item in ids):
        raise ValueError("scenario_id contains unsupported characters")
    if len(set(ids)) != len(ids):
        raise ValueError("scenario_id values must be unique")

    config = MatrixConfig(
        experiment_id=_required_text(raw, "experiment_id"),
        seed=_integer(raw, "seed", minimum=0),
        warmup_runs_per_scenario=_integer(
            raw, "warmup_runs_per_scenario", minimum=0
        ),
        formal_repeats=_integer(raw, "formal_repeats", minimum=1),
        minimum_unique_rows=_integer(raw, "minimum_unique_rows", minimum=1),
        minimum_steady_state_s=_finite_number(
            raw, "minimum_steady_state_s", minimum=0.0
        ),
        common_args=_string_tuple(raw.get("common_args"), "common_args"),
        scenarios=scenarios,
    )
    _reject_owned_flags(config.common_args)
    for scenario in config.scenarios:
        _reject_owned_flags(scenario.args)
    return config


def run_matrix(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    schedule = build_scenario_schedule(
        [item.scenario_id for item in config.scenarios],
        config.warmup_runs_per_scenario,
        config.formal_repeats,
        config.seed,
    )
    fingerprint = _fingerprint(config, schedule)
    with acquire_runner_lease(
        args.output_dir,
        config_fingerprint=fingerprint,
        repository_commit=_git_commit(),
        recover_stale=args.recover_stale_lease,
    ) as lease:
        return _run_locked(args, config, schedule, fingerprint, lease.recovered_owner)


def _run_locked(
    args: argparse.Namespace,
    config: MatrixConfig,
    schedule: tuple[ScheduledScenarioRun, ...],
    fingerprint: str,
    recovered_owner: dict[str, object] | None,
) -> int:
    args.output_dir.mkdir(parents=True, exist_ok=True)
    outer_path = args.output_dir / "matrix_manifest.json"
    expected = {
        "schema_version": 1,
        "experiment_id": config.experiment_id,
        "config_fingerprint": fingerprint,
        "repository_commit": _git_commit(),
        "config_path": str(args.config),
        "schedule": [asdict(item) for item in schedule],
        "completed_runs": [],
        "incidents": [],
        "status": "running",
    }
    if args.resume:
        manifest = json.loads(outer_path.read_text(encoding="utf-8"))
        if manifest.get("config_fingerprint") != fingerprint:
            raise RuntimeError("resume config fingerprint does not match")
        manifest["status"] = "running"
    else:
        if outer_path.exists():
            raise RuntimeError("output directory already contains matrix_manifest.json")
        manifest = expected
    if recovered_owner is not None:
        manifest["incidents"].append(
            {"reason": "stale_runner_lease_recovered", "owner": recovered_owner}
        )
    _write_json_atomic(outer_path, manifest)

    definitions = {item.scenario_id: item for item in config.scenarios}
    completed = {
        (item["scenario_id"], item["phase"], item["repeat_index"])
        for item in manifest["completed_runs"]
    }
    for scheduled in schedule:
        key = (scheduled.scenario_id, scheduled.phase, scheduled.repeat_index)
        if key in completed:
            continue
        stem = (
            f"{scheduled.order_index:03d}_{scheduled.phase}_"
            f"{scheduled.repeat_index}_{scheduled.scenario_id}"
        )
        per_run_manifest = args.output_dir / f"{stem}.json"
        command = [
            str(args.python_executable),
            str(args.image_runner),
            *config.common_args,
            *definitions[scheduled.scenario_id].args,
            "--phase",
            scheduled.phase,
            "--repeat-index",
            str(scheduled.repeat_index),
            "--out-csv",
            str(args.output_dir / "runs.csv"),
            "--out-manifest",
            str(per_run_manifest),
        ]
        completed_process = subprocess.run(
            command,
            text=True,
            capture_output=True,
            check=False,
        )
        (args.output_dir / f"{stem}.stdout.log").write_text(
            completed_process.stdout, encoding="utf-8"
        )
        (args.output_dir / f"{stem}.stderr.log").write_text(
            completed_process.stderr, encoding="utf-8"
        )
        try:
            if completed_process.returncode != 0:
                raise RuntimeError(f"subprocess_exit_{completed_process.returncode}")
            row = _validated_row(per_run_manifest, config, scheduled.phase)
        except Exception as exc:  # noqa: BLE001
            manifest["incidents"].append(
                {
                    **asdict(scheduled),
                    "reason": f"{type(exc).__name__}:{exc}",
                    "command": command,
                }
            )
            manifest["status"] = "failed"
            _write_json_atomic(outer_path, manifest)
            return 1
        manifest["completed_runs"].append(
            {
                **asdict(scheduled),
                "arm": row["arm"],
                "rows": row["rows"],
                "operator_e2e_s": row["operator_e2e_s"],
                "steady_state_proxy_s": _steady_state_proxy(row),
                "manifest": per_run_manifest.name,
            }
        )
        _write_json_atomic(outer_path, manifest)
    manifest["status"] = "complete"
    _write_json_atomic(outer_path, manifest)
    return 0


def _validated_row(
    path: Path,
    config: MatrixConfig,
    phase: str,
) -> dict[str, object]:
    decoded = json.loads(path.read_text(encoding="utf-8"))
    row = decoded.get("row")
    if not isinstance(row, dict):
        raise ValueError("per-run manifest has no row object")
    rows = row.get("rows")
    if not isinstance(rows, int) or rows < config.minimum_unique_rows:
        raise ValueError("run does not satisfy minimum_unique_rows")
    if row.get("output_rows") != rows or row.get("exactly_once") is not True:
        raise ValueError("row-count or exactly-once gate failed")
    steady_s = _steady_state_proxy(row)
    if phase == "formal" and steady_s < config.minimum_steady_state_s:
        raise ValueError(
            f"steady-state proxy {steady_s:.3f}s is below "
            f"{config.minimum_steady_state_s:.3f}s"
        )
    return row


def _steady_state_proxy(row: dict[str, object]) -> float:
    operator_s = float(row["operator_e2e_s"])
    setup = row.get("worker_setup_s")
    setup_s = float(setup) if isinstance(setup, (int, float)) else 0.0
    return operator_s - setup_s


def _required_text(mapping: dict[str, object], key: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{key} must be a non-empty string")
    return value.strip()


def _integer(mapping: dict[str, object], key: str, *, minimum: int) -> int:
    value = mapping.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{key} must be an integer >= {minimum}")
    return value


def _finite_number(mapping: dict[str, object], key: str, *, minimum: float) -> float:
    value = mapping.get(key)
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < minimum
    ):
        raise ValueError(f"{key} must be finite and >= {minimum}")
    return float(value)


def _string_tuple(value: object, label: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ValueError(f"{label} must be a list of strings")
    return tuple(value)


def _reject_owned_flags(arguments: tuple[str, ...]) -> None:
    conflicts = sorted(set(arguments) & _OWNED_FLAGS)
    if conflicts:
        raise ValueError("runner-owned flags are not allowed: " + ", ".join(conflicts))


def _fingerprint(
    config: MatrixConfig,
    schedule: tuple[ScheduledScenarioRun, ...],
) -> str:
    payload = {
        "config": {
            **asdict(config),
            "scenarios": [asdict(item) for item in config.scenarios],
        },
        "schedule": [asdict(item) for item in schedule],
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _git_commit() -> str:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"], text=True, capture_output=True, check=True
    )
    return completed.stdout.strip()


def _write_json_atomic(path: Path, payload: dict[str, object]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    temporary.replace(path)


def main() -> None:
    raise SystemExit(run_matrix(parse_args()))


if __name__ == "__main__":
    main()
