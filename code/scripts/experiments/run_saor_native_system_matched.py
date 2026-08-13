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
from src.baselines.common.manifests import read_manifest  # noqa: E402
from src.experiments.saor.native_system_matched import (  # noqa: E402
    MatchedArm,
    ScheduledMatchedCell,
    load_matched_system_config,
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


def _argument_value(arguments: tuple[str, ...], flag: str) -> str:
    try:
        index = arguments.index(flag)
    except ValueError as exc:
        raise ValueError(f"Project config is missing {flag}") from exc
    if index + 1 >= len(arguments):
        raise ValueError(f"Project config {flag} has no value")
    return arguments[index + 1]


def _canonical_config_path(
    value: str | Path | None,
    config_path: str | Path | None = None,
) -> str | None:
    if value is None:
        return None
    path = Path(value)
    if not path.is_absolute() and config_path is not None:
        path = Path(config_path).parent / path
    return str(path.resolve())


def _validate_combined_manifest(
    matched_path: str,
    job_paths: tuple[str | Path, ...],
) -> None:
    if len(job_paths) != 2:
        raise ValueError("executor must define exactly two Job manifests")
    matched = read_manifest(matched_path)
    combined = tuple(
        request
        for path in job_paths
        for request in read_manifest(path)
    )
    if matched != combined:
        raise ValueError(
            "executor Job manifests do not equal the authoritative matched "
            "combined manifest in Job order"
        )


def _validate_executor_bindings(
    matched,
    native,
    project,
    *,
    matched_config_path: str | Path | None = None,
    project_config_path: str | Path | None = None,
) -> None:
    """Fail before dispatch when actual executor contracts drift."""

    native_by_id = {item.arm_id: item for item in native.arms}
    project_by_id = {item.scenario_id: item for item in project.scenarios}
    project_protocol = _argument_value(project.common_args, "--completion-protocol")
    project_output_cap = int(
        _argument_value(project.common_args, "--completion-max-tokens")
    )
    project_organizer = _argument_value(project.common_args, "--organizer")
    project_database_url = _argument_value(project.common_args, "--database-url")
    project_workload = _argument_value(
        project.common_args, "--source-workload-name"
    )
    actor_topology = {
        "workers": int(
            _argument_value(project.common_args, "--actor-workers-per-endpoint")
        ),
        "concurrency": int(
            _argument_value(project.common_args, "--ray-actor-max-concurrency")
        ),
    }
    for arm in matched.arms:
        expected_source = dict(arm.source)
        if arm.kind == "native":
            actual = native_by_id.get(arm.arm_id)
            if actual is None:
                raise ValueError(f"native config is missing arm {arm.arm_id}")
            if native.source is None:
                raise ValueError("native matrix config is missing explicit source")
            comparisons = {
                "endpoint_ids": (native.endpoint_ids, arm.endpoint_ids),
                "service_signature": (
                    native.service_signature, arm.service_signature
                ),
                "protocol": (native.protocol, arm.protocol),
                "output_cap": (native.output_cap, arm.output_cap),
                "arrival_offsets_s": (
                    tuple(job.offset_s for job in actual.jobs),
                    arm.arrival_offsets_s,
                ),
                "job_internal_arrival_contract": (
                    native.job_internal_arrival_contract,
                    arm.job_internal_arrival_contract,
                ),
                "organizer": (native.organizer, arm.organizer),
                "source.database_url": (
                    native.source.database_url,
                    expected_source["database_url"],
                ),
                "source.workload_name": (
                    native.source.workload_name,
                    expected_source["workload_name"],
                ),
            }
            _validate_combined_manifest(
                arm.manifest_path,
                tuple(job.manifest for job in actual.jobs),
            )
        else:
            actual = project_by_id.get(arm.arm_id)
            if actual is None:
                raise ValueError(f"Project config is missing scenario {arm.arm_id}")
            request_limit, work_limit = actual.endpoint_limits(
                project.request_limit_per_endpoint,
                project.work_limit_per_endpoint,
            )
            comparisons = {
                "endpoint_ids": (project.endpoint_ids, arm.endpoint_ids),
                "service_signature": (
                    project.service_signature, arm.service_signature
                ),
                "protocol": (project_protocol, arm.protocol),
                "output_cap": (project_output_cap, arm.output_cap),
                "arrival_offsets_s": (
                    actual.arrival_offsets_s, arm.arrival_offsets_s
                ),
                "job_internal_arrival_contract": (
                    project.job_internal_arrival_contract,
                    arm.job_internal_arrival_contract,
                ),
                "organizer": (project_organizer, arm.organizer),
                "source.database_url": (
                    project_database_url, expected_source["database_url"]
                ),
                "source.workload_name": (
                    project_workload, expected_source["workload_name"]
                ),
                "source_row_offsets": (
                    actual.source_row_offsets,
                    tuple(
                        expected_source.get(
                            "source_row_offsets",
                            (0,) * len(actual.request_manifests),
                        )
                    ),
                ),
                "k_per_endpoint": (
                    request_limit, arm.project_value("k_per_endpoint")
                ),
                "work_limit_per_endpoint": (
                    work_limit, arm.project_value("work_limit_per_endpoint")
                ),
                "ready_bytes": (
                    project.ready_payload_bytes_limit_per_job,
                    arm.project_value("ready_bytes"),
                ),
                "actor_topology": (
                    tuple(sorted(actor_topology.items())),
                    arm.project_value("actor_topology"),
                ),
                "policy": (actual.policy, arm.project_value("policy")),
                "ready_observation": (
                    actual.ready_observation_contract,
                    arm.project_value("ready_observation") or "single_head",
                ),
                "debt_caps": (
                    actual.debt_cap_fractions,
                    arm.project_value("debt_caps") or (),
                ),
                "calibration_path": (
                    _canonical_config_path(
                        project.calibration_contract.path,
                        project_config_path,
                    )
                    if project.calibration_contract is not None else None,
                    _canonical_config_path(
                        arm.calibration_path,
                        matched_config_path,
                    ),
                ),
            }
            _validate_combined_manifest(
                arm.manifest_path,
                tuple(actual.request_manifests),
            )
        drift = [name for name, values in comparisons.items() if values[0] != values[1]]
        if drift:
            raise ValueError(
                f"{arm.arm_id} executor contract drift: {', '.join(drift)}"
            )


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
    args = parser.parse_args(argv)
    metrics_urls = tuple(item.strip() for item in args.metrics_urls.split(",") if item.strip())
    if not metrics_urls:
        parser.error("--metrics-urls must contain at least one URL")
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
        resume=False,
        recover_stale_lease=False,
    )


def _normalize_native(
    arm: MatchedArm, record: dict[str, object]
) -> dict[str, object]:
    jobs = record["jobs"]
    service_path = Path(str(record["service_counters"]))
    service_payload = json.loads(service_path.read_text(encoding="utf-8"))
    deltas = service_payload.get("delta", {})
    prompt_delta = sum(
        int(row.get("prompt_tokens", 0)) for row in deltas.values()
    )
    generation_delta = sum(
        int(row.get("generation_tokens", 0)) for row in deltas.values()
    )
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
            "prompt_tokens_delta": prompt_delta,
            "generation_tokens_delta": generation_delta,
        },
        "resource_metrics": {
            "resource_metrics_status": "ok",
            "path": record["gpu_resource_trace"],
            "gpu_summary": record["gpu_summary"],
            "gauge_summary": record["gauge_summary"],
        },
        "request_tail_status": dict(arm.unsupported_request_tails),
        "output_paths": {
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
    observed = json.loads(str(record["job_arrival_start_epoch_s"]))
    completed = json.loads(str(record["job_completion_end_epoch_s"]))
    actual_work = json.loads(str(record["job_actual_work"]))
    expected_counts = json.loads(str(record["job_expected_count"]))
    completed_counts = json.loads(str(record["job_completed_count"]))
    exactly_once = json.loads(str(record["job_exactly_once"]))
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
            "ended_epoch_s": float(completed[index]),
            "completed_count": int(completed_counts[index]),
            "expected_count": int(expected_counts[index]),
            "actual_work": int(actual_work[index]),
            "exactly_once": bool(exactly_once[index]),
            "shard_provenance": [{
                "source_kind": dict(arm.source)["kind"],
                "source_timing_boundary": dict(arm.source)["timing_boundary"],
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
        "service_metrics": {
            "metrics_status": record["metrics_status"],
            "prompt_tokens_delta": record["prompt_tokens_delta"],
            "generation_tokens_delta": record["generation_tokens_delta"],
        },
        "resource_metrics": {
            "resource_metrics_status": record["resource_metrics_status"],
            "path": str(
                next((output_dir / "traces").glob("*.resources.csv"))
            ),
        },
        "exactly_once": all(bool(value) for value in exactly_once),
        "request_tail_status": dict(arm.unsupported_request_tails),
        "output_paths": {
            "commands": str(command_files[0]),
            "resources": str(
                next((output_dir / "traces").glob("*.resources.csv"))
            ),
        },
        "status": "passed",
    }


def run(options: CliOptions) -> dict[str, object]:
    native = load_native_multijob_config(options.native_config)
    project = load_project_config(options.project_config)
    matched = load_matched_system_config(options.config)
    _validate_executor_bindings(
        matched,
        native,
        project,
        matched_config_path=options.config,
        project_config_path=options.project_config,
    )
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
        rehearsal=options.rehearsal,
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
