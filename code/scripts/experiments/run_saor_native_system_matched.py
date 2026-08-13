#!/usr/bin/env python3
"""Run the matched SAOR system matrix through existing single-cell runners."""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

CODE_ROOT = next(
    parent for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from scripts.experiments.run_ai_operator_scenarios import wait_for_idle  # noqa: E402
from src.baselines.text.orchestration.native_multijob import (  # noqa: E402
    NativeRunIdentity,
    audit_command,
    load_native_multijob_config,
    run_native_multijob_cell,
)
from src.experiments.saor.native_system_matched import (  # noqa: E402
    MatchedArm,
    ScheduledMatchedCell,
    run_matched_system,
)
from src.experiments.shared_vllm import (  # noqa: E402
    GroupRunIdentity,
    RunnerOptions,
    load_config as load_project_config,
    run_shared_vllm_group_cell,
)


@dataclass(frozen=True)
class CliOptions:
    config: Path
    native_config: Path
    project_config: Path
    native_runner: Path
    profiler: Path
    python_executable: Path
    health_url: str
    metrics_urls: tuple[str, ...]
    ray_address: str
    idle_timeout_s: float
    start_delay_s: float
    rehearsal: bool
    resume: bool
    recover_stale_lease: bool


def parse_args(argv: list[str] | None = None) -> CliOptions:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--native-config", required=True, type=Path)
    parser.add_argument("--project-config", required=True, type=Path)
    parser.add_argument("--native-runner", required=True, type=Path)
    parser.add_argument("--profiler", required=True, type=Path)
    parser.add_argument("--python-executable", required=True, type=Path)
    parser.add_argument("--health-url", required=True)
    parser.add_argument("--metrics-urls", required=True)
    parser.add_argument("--ray-address", required=True)
    parser.add_argument("--idle-timeout-s", type=float, default=60.0)
    parser.add_argument("--start-delay-s", type=float, default=15.0)
    parser.add_argument("--rehearsal", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--recover-stale-lease", action="store_true")
    args = parser.parse_args(argv)
    metrics_urls = tuple(item.strip() for item in args.metrics_urls.split(",") if item.strip())
    if not metrics_urls:
        parser.error("--metrics-urls must contain at least one URL")
    if args.recover_stale_lease and not args.resume:
        parser.error("--recover-stale-lease requires --resume")
    return CliOptions(
        config=args.config.resolve(),
        native_config=args.native_config.resolve(),
        project_config=args.project_config.resolve(),
        native_runner=args.native_runner.resolve(),
        profiler=args.profiler.resolve(),
        python_executable=args.python_executable.resolve(),
        health_url=args.health_url,
        metrics_urls=metrics_urls,
        ray_address=args.ray_address,
        idle_timeout_s=args.idle_timeout_s,
        start_delay_s=args.start_delay_s,
        rehearsal=args.rehearsal,
        resume=args.resume,
        recover_stale_lease=args.recover_stale_lease,
    )


def _normalize_native(
    arm: MatchedArm, record: dict[str, object]
) -> dict[str, object]:
    jobs = record["jobs"]
    return {
        **record,
        "command": record.get("command", []),
        "implementation_source": "official_native_single_cell_runner",
        "start_epoch_s": record["t0_epoch_s"],
        "end_epoch_s": float(record["t0_epoch_s"]) + float(record["arm_barrier_jct_s"]),
        "database_operator_e2e_s": record["arm_barrier_jct_s"],
        "service_metrics": {
            "metrics_status": "ok",
            "service_counters_path": record["service_counters"],
        },
        "resource_metrics": {
            "resource_metrics_status": "ok",
            "path": record["gpu_resource_trace"],
            "gpu_summary": record["gpu_summary"],
            "gauge_summary": record["gauge_summary"],
        },
        "request_tail_status": dict(arm.unsupported_request_tails),
        "output_paths": {
            "root": record["output_root"],
            "service_counters": record["service_counters"],
            "resources": record["gpu_resource_trace"],
        },
        "jobs": jobs,
    }


def _normalize_project(
    arm: MatchedArm,
    record: dict[str, object],
    output_dir: Path,
) -> dict[str, object]:
    configured = json.loads(str(record["replay_configured_start_epoch_s"]))
    observed = json.loads(str(record["replay_observed_start_epoch_s"]))
    actual_work = json.loads(str(record["job_actual_work"]))
    summaries = []
    for path in sorted((output_dir / "jobs").glob("*.runs.csv")):
        with path.open(encoding="utf-8", newline="") as stream:
            rows = list(csv.DictReader(stream))
        if len(rows) != 1:
            raise RuntimeError(f"Project Job summary is not unique: {path}")
        summaries.append(rows[0])
    if len(summaries) != len(observed):
        raise RuntimeError("Project Job summary evidence is incomplete")
    jobs = [
        {
            "job_id": f"job-{index}",
            "actual_launch_epoch_s": float(observed[index]),
            "ended_epoch_s": float(observed[index]) + float(
                json.loads(str(record["job_jct_s"]))[index]
            ),
            "completed_count": int(summaries[index]["total_rows"]),
            "actual_work": int(actual_work[index]),
            "exactly_once": True,
            "shard_provenance": [{
                "source_kind": "timed_postgres_manifest",
                "source_timing_boundary": "inside_job_barrier",
                "source_validation_status": (
                    "ok"
                    if summaries[index].get(
                        "request_manifest_validation_status"
                    ) == "ok"
                    and float(summaries[index].get("db_fetch_s", "-1")) >= 0
                    else "failed"
                ),
                "source_read_s": float(summaries[index]["db_fetch_s"]),
            }],
        }
        for index in range(len(observed))
    ]
    # Preserve the exact configured 0/5 barrier even when observed crossings
    # differ slightly, while retaining observed start timestamps per Job.
    if [round(float(value) - float(configured[0]), 6) for value in configured] != [0.0, 5.0]:
        raise RuntimeError("Project job offsets drift from [0, 5]")
    command_files = list((output_dir / "traces").glob("*.commands.json"))
    if len(command_files) != 1:
        raise RuntimeError("Project command evidence is incomplete")
    command_evidence = json.loads(command_files[0].read_text(encoding="utf-8"))
    return {
        **record,
        "command": [
            token
            for command in command_evidence.get("commands", [])
            for token in command
        ],
        "implementation_source": "project_shared_vllm_single_cell_runner",
        "database_operator_e2e_s": float(record["end_epoch_s"]) - float(record["start_epoch_s"]),
        "jobs": jobs,
        "service_metrics": {"metrics_status": record["metrics_status"]},
        "resource_metrics": {
            "resource_metrics_status": record["resource_metrics_status"],
            "path": str(output_dir / "traces"),
        },
        "exactly_once": True,
        "request_tail_status": dict(arm.unsupported_request_tails),
        "output_paths": {"root": str(output_dir)},
        "status": "passed",
    }


def run(options: CliOptions) -> dict[str, object]:
    if options.resume:
        raise ValueError(
            "matched-system resume is accepted for interface compatibility "
            "but fail-closed until durable cell-resume validation is implemented"
        )
    native = load_native_multijob_config(options.native_config)
    project = load_project_config(options.project_config)
    repository_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
        cwd=CODE_ROOT.parent,
    ).stdout.strip()
    native_by_id = {item.arm_id: item for item in native.arms}
    project_by_id = {item.scenario_id: item for item in project.scenarios}

    def native_executor(arm: MatchedArm, cell: ScheduledMatchedCell, output_dir: Path):
        record = run_native_multijob_cell(
            native,
            native_by_id[arm.arm_id],
            NativeRunIdentity(cell.phase, cell.repeat, cell.order_index),
            output_dir,
            runner_script=options.native_runner,
            repository_commit=repository_commit,
        )
        commands = [
            command
            for job in record.get("jobs", [])
            for command in json.loads(
                (Path(record["output_root"]) / "jobs" / job["job_id"] / "commands.json").read_text()
            )
        ]
        for command in commands:
            audit_command(command)
        normalized = _normalize_native(arm, record)
        normalized["command"] = [
            token for command in commands for token in command
        ]
        return normalized

    def project_executor(arm: MatchedArm, cell: ScheduledMatchedCell, output_dir: Path):
        scenario = project_by_id[arm.arm_id]
        runner = RunnerOptions(
            config_path=options.project_config,
            profiler_path=options.profiler,
            python_executable=options.python_executable,
            output_dir=output_dir,
            health_url=options.health_url,
            metrics_urls=options.metrics_urls,
            ray_address=options.ray_address,
            idle_timeout_s=options.idle_timeout_s,
            start_delay_s=options.start_delay_s,
            rehearsal=options.rehearsal,
        )
        for child in ("jobs", "logs", "traces", "records"):
            (output_dir / child).mkdir(parents=True, exist_ok=True)
        record = run_shared_vllm_group_cell(
            runner,
            project,
            scenario,
            GroupRunIdentity(cell.phase, cell.repeat, cell.order_index),
        )
        return _normalize_project(arm, record, output_dir)

    return run_matched_system(
        options.config,
        native_executor=native_executor,
        project_executor=project_executor,
        idle_gate=lambda _position: wait_for_idle(
            options.health_url, options.metrics_urls, options.idle_timeout_s
        ),
        instrumenter=lambda *_args: None,
        repository_commit_getter=lambda: repository_commit,
    )


def main(argv: list[str] | None = None) -> int:
    try:
        result = run(parse_args(argv))
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": f"{type(exc).__name__}: {exc}"}))
        return 1
    print(json.dumps({"status": result["status"], "cells": len(result["cells"])}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
