#!/usr/bin/env python3
"""Run seeded, interleaved AI-operator scenarios as isolated profiler calls."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable
from urllib import error, request

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.experiments.calibration.contracts import (  # noqa: E402
    CalibrationContract,
    JsonScalar,
    load_calibration_contract,
)
from src.experiments.scenarios.core import (  # noqa: E402
    ScheduledScenarioRun,
    build_scenario_schedule,
    validate_service_metadata,
)
from src.observability.metrics import parse_prometheus_metrics  # noqa: E402
from src.baselines.common.redact import (  # noqa: E402
    redact_argument_list as _redact_argument_list,
    redact_database_url as _redact_database_url,
)
from src.infrastructure.config_env import ENV_REFERENCE, expand_text  # noqa: E402
from src.infrastructure.runner_lease import (  # noqa: E402
    acquire_host_runner_lease,
    acquire_runner_lease,
)
from src.serving.probes.vllm import probe_live_prefix_caching  # noqa: E402


_SCENARIO_ID_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")
_RUNNER_OWNED_FLAGS = {
    "--control-trace-output",
    "--experiment-id",
    "--flush-trace-output",
    "--output",
    "--random-seed",
    "--repeats",
    "--request-trace-output",
    "--resource-trace-output",
    "--run-phase",
    "--run-repeat-index",
    "--scenario-id",
    "--submission-trace-output",
    "--warmup-runs",
}


@dataclass(frozen=True)
class RunnerOptions:
    config_path: Path
    profiler_path: Path
    python_executable: Path
    output_dir: Path
    health_url: str
    metrics_urls: tuple[str, ...]
    idle_timeout_s: float
    resume: bool = False
    skip_failed_scenarios: bool = False
    recover_stale_lease: bool = False


@dataclass(frozen=True)
class ScenarioDefinition:
    scenario_id: str
    args: tuple[str, ...]


@dataclass(frozen=True)
class ScenarioExperimentConfig:
    experiment_id: str
    seed: int
    service_metadata: tuple[tuple[str, object], ...]
    calibration_contract: CalibrationContract | None
    warmup_runs_per_scenario: int
    formal_repeats: int
    common_args: tuple[str, ...]
    scenarios: tuple[ScenarioDefinition, ...]


def parse_args(argv: list[str] | None = None) -> RunnerOptions:
    parser = argparse.ArgumentParser(
        description="Run seeded interleaved AI-operator profiler scenarios."
    )
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--profiler", required=True, type=Path)
    parser.add_argument("--python-executable", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--health-url", required=True)
    parser.add_argument(
        "--metrics-urls",
        required=True,
        help="Comma-separated metrics URLs for all vLLM instances.",
    )
    parser.add_argument("--idle-timeout-s", type=float, default=60.0)
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume a matching manifest and skip verified completed runs.",
    )
    parser.add_argument(
        "--skip-failed-scenarios",
        action="store_true",
        help=(
            "With --resume, prune every remaining run belonging to an "
            "unrecovered failed scenario."
        ),
    )
    parser.add_argument(
        "--recover-stale-lease",
        action="store_true",
        help=(
            "With --resume, replace an inspected stale output-directory "
            "runner lease."
        ),
    )
    args = parser.parse_args(argv)
    if not math.isfinite(args.idle_timeout_s) or args.idle_timeout_s <= 0:
        parser.error("--idle-timeout-s must be finite and positive")
    metrics_urls = tuple(
        url.strip() for url in args.metrics_urls.split(",") if url.strip()
    )
    if not metrics_urls:
        parser.error("--metrics-urls must contain at least one non-empty URL")
    if args.recover_stale_lease and not args.resume:
        parser.error("--recover-stale-lease requires --resume")
    return RunnerOptions(
        config_path=args.config,
        profiler_path=args.profiler,
        python_executable=args.python_executable,
        output_dir=args.output_dir,
        health_url=args.health_url,
        metrics_urls=metrics_urls,
        idle_timeout_s=args.idle_timeout_s,
        resume=args.resume,
        skip_failed_scenarios=args.skip_failed_scenarios,
        recover_stale_lease=args.recover_stale_lease,
    )


def _verify_prefix_caching_matches_live(config) -> None:
    """Fail-closed when declared ``prefix_caching`` contradicts the live vLLM.

    Probing is best-effort: when the live flag cannot be determined (non-Linux
    host, no co-located vLLM ``api_server`` process, or disagreeing processes),
    warn on stderr and rely on the declared value instead of failing. This
    catches the class of bug where a scenario config declares
    ``prefix_caching: false`` while vLLM is actually started with
    ``--enable-prefix-caching`` (or vice versa), which would otherwise silently
    record wrong ``service_metadata`` in the manifest.
    """
    declared = dict(config.service_metadata).get("prefix_caching")
    if not isinstance(declared, bool):
        return
    live = probe_live_prefix_caching()
    if live is None:
        sys.stderr.write(
            "[runner] warning: could not live-verify vLLM prefix_caching "
            "(no local vllm api_server process found, or ps unavailable); "
            "relying on declared service_metadata.prefix_caching="
            + str(declared)
            + "\n"
        )
        return
    if live != declared:
        raise ValueError(
            "service_metadata.prefix_caching="
            + str(declared)
            + " conflicts with the live vLLM process flags (detected "
            "prefix_caching=" + str(live) + "). Fix the scenario config or "
            "the vLLM startup flags before running."
        )


def _verify_service_flags_match_live(config) -> None:
    """Fail-closed when declared ``max_num_seqs`` / ``max_num_batched_tokens`` disagree with the
    live vLLM cmdline (audit F8: prefix_caching was already live-verified, but the two capacity
    flags were declared-only, so a stale vLLM process on vLLM defaults would silently record the
    declared value as effective). Best-effort: if no co-located vLLM process is probeable (non-Linux
    host, dry-run) or COMPLETION_ENDPOINT_URLS is unset, warn + rely on declared, matching
    ``_verify_prefix_caching_matches_live``. When vLLM IS up, any per-endpoint missing/mismatched
    flag is fail-closed (strict)."""
    from src.infrastructure.vllm_preflight import _read_live_cmdlines, verify_endpoint_cmdlines

    declared_meta = dict(config.service_metadata)
    seqs = declared_meta.get("max_num_seqs")
    batched = declared_meta.get("max_num_batched_tokens")
    if seqs is None and batched is None:
        return  # nothing declared to verify
    endpoint_urls_csv = os.environ.get("COMPLETION_ENDPOINT_URLS", "")
    endpoint_urls = [u.strip() for u in endpoint_urls_csv.split(",") if u.strip()]
    if not endpoint_urls:
        sys.stderr.write(
            "[runner] warning: COMPLETION_ENDPOINT_URLS unset; cannot live-verify "
            "max_num_seqs/max_num_batched_tokens\n"
        )
        return
    try:
        cmdlines = _read_live_cmdlines()
    except Exception:
        cmdlines = {}
    if not cmdlines:
        sys.stderr.write(
            "[runner] warning: no live vllm.entrypoints process probeable; cannot verify "
            "max_num_seqs/max_num_batched_tokens, relying on declared service_metadata\n"
        )
        return
    declared: dict[str, str] = {}
    if seqs is not None:
        declared["--max-num-seqs"] = str(seqs)
    if batched is not None:
        declared["--max-num-batched-tokens"] = str(batched)
    verify_endpoint_cmdlines(
        list(cmdlines.values()), endpoint_urls, declared, strict=True, tag="cost-profile"
    )


def run_experiment(
    options: RunnerOptions,
    *,
    idle_gate: Callable[[str, tuple[str, ...], float], None] | None = None,
) -> int:
    if not options.metrics_urls:
        raise ValueError("at least one model metrics URL is required")
    config = _load_config(options.config_path)
    if options.skip_failed_scenarios and not options.resume:
        raise ValueError("--skip-failed-scenarios requires --resume")
    if options.recover_stale_lease and not options.resume:
        raise ValueError("--recover-stale-lease requires --resume")
    schedule = build_scenario_schedule(
        [item.scenario_id for item in config.scenarios],
        config.warmup_runs_per_scenario,
        config.formal_repeats,
        config.seed,
    )
    fingerprint = _config_fingerprint(config, schedule)
    repository_commit = _repository_commit()
    with acquire_host_runner_lease(
        options.output_dir.parent,
        repository_commit=repository_commit,
    ) as host_lease:
        with acquire_runner_lease(
            options.output_dir,
            config_fingerprint=fingerprint,
            repository_commit=repository_commit,
            recover_stale=options.recover_stale_lease,
        ) as output_lease:
            recovered_owner = (
                output_lease.recovered_owner or host_lease.recovered_owner
            )
            return _run_experiment_locked(
                options,
                config=config,
                schedule=schedule,
                recovered_owner=recovered_owner,
                idle_gate=idle_gate,
            )


def _run_experiment_locked(
    options: RunnerOptions,
    *,
    config: ScenarioExperimentConfig,
    schedule: tuple[ScheduledScenarioRun, ...],
    recovered_owner: dict[str, object] | None,
    idle_gate: Callable[[str, tuple[str, ...], float], None] | None,
) -> int:
    definitions = {
        item.scenario_id: item for item in config.scenarios
    }
    manifest_path = options.output_dir / "manifest.json"
    runs_path = options.output_dir / "runs.csv"
    expected_manifest = {
        "schema_version": 1,
        "experiment_id": config.experiment_id,
        "seed": config.seed,
        "config_path": str(options.config_path),
        "redacted_config": _redacted_config(config),
        "schedule": [asdict(item) for item in schedule],
        "completed_runs": [],
        "skipped_runs": [],
        "incidents": [],
        "status": "running",
    }
    if options.resume:
        manifest = _load_resume_manifest(
            manifest_path,
            runs_path,
            expected_manifest,
        )
        manifest["status"] = "running"
    else:
        manifest = expected_manifest
    if recovered_owner is not None:
        manifest["incidents"].append(
            {
                "scenario_id": "_runner",
                "phase": "control",
                "repeat_index": -1,
                "exit_code": None,
                "reason": "stale_runner_lease_recovered",
                "recovered_owner": recovered_owner,
                "recovered": True,
            }
        )
    _write_json_atomic(manifest_path, manifest)
    resolved_idle_gate = idle_gate or wait_for_idle
    completed_keys = {
        _run_key_from_mapping(item)
        for item in manifest["completed_runs"]
    }
    skipped_keys = {
        _run_key_from_mapping(item)
        for item in manifest["skipped_runs"]
    }
    pruned_scenario_ids = set()
    if options.skip_failed_scenarios:
        pruned_scenario_ids = {
            str(item["scenario_id"])
            for item in manifest["incidents"]
            if not item.get("recovered", False)
        }
        for incident in manifest["incidents"]:
            if str(incident.get("scenario_id")) in pruned_scenario_ids:
                incident["pruned"] = True

    for scheduled in schedule:
        run_key = _scheduled_run_key(scheduled)
        if run_key in completed_keys:
            continue
        if scheduled.scenario_id in pruned_scenario_ids:
            if run_key not in skipped_keys:
                manifest["skipped_runs"].append(
                    {
                        **asdict(scheduled),
                        "reason": "scenario_pruned_after_failure",
                    }
                )
                skipped_keys.add(run_key)
                _write_json_atomic(manifest_path, manifest)
            continue
        definition = definitions[scheduled.scenario_id]
        run_stem = (
            f"{scheduled.order_index:03d}_"
            f"{scheduled.phase}_{scheduled.repeat_index}_"
            f"{scheduled.scenario_id}"
        )
        command = _build_profiler_command(
            options,
            config,
            definition,
            scheduled,
            runs_path,
            run_stem,
        )
        try:
            resolved_idle_gate(
                options.health_url,
                options.metrics_urls,
                options.idle_timeout_s,
            )
        except Exception as exc:
            manifest["incidents"].append(
                {
                    **asdict(scheduled),
                    "exit_code": None,
                    "reason": f"idle_gate:{type(exc).__name__}:{exc}",
                    "command": _redact_argument_list(command),
                    "recovered": False,
                }
            )
            manifest["status"] = "failed"
            _write_json_atomic(manifest_path, manifest)
            return 1

        before_rows = _read_csv_rows(runs_path)
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
        )
        prior_attempts = sum(
            1
            for incident in manifest["incidents"]
            if _run_key_from_mapping(incident) == run_key
        )
        attempt_suffix = (
            "" if prior_attempts == 0 else f".attempt-{prior_attempts + 1}"
        )
        stdout_path = (
            options.output_dir / f"{run_stem}{attempt_suffix}.stdout.log"
        )
        stderr_path = (
            options.output_dir / f"{run_stem}{attempt_suffix}.stderr.log"
        )
        stdout_path.write_text(completed.stdout, encoding="utf-8")
        stderr_path.write_text(completed.stderr, encoding="utf-8")

        failure_reason = ""
        if completed.returncode != 0:
            failure_reason = "subprocess_nonzero"
        elif not _has_expected_new_row(
            before_rows,
            _read_csv_rows(runs_path),
            config,
            scheduled,
        ):
            failure_reason = "missing_expected_csv_row"

        run_record = {
            **asdict(scheduled),
            "exit_code": completed.returncode,
            "command": _redact_argument_list(command),
            "stdout_path": str(stdout_path),
            "stderr_path": str(stderr_path),
        }
        if failure_reason:
            manifest["incidents"].append(
                {
                    **run_record,
                    "reason": failure_reason,
                    "recovered": False,
                }
            )
            manifest["status"] = "failed"
            _write_json_atomic(manifest_path, manifest)
            return 1

        manifest["completed_runs"].append(run_record)
        completed_keys.add(run_key)
        for incident in manifest["incidents"]:
            if _run_key_from_mapping(incident) == run_key:
                incident["recovered"] = True
        _write_json_atomic(manifest_path, manifest)

    manifest["status"] = (
        "completed_with_pruned_scenarios"
        if manifest["skipped_runs"]
        else "completed"
    )
    _write_json_atomic(manifest_path, manifest)
    return 0


def _load_resume_manifest(
    manifest_path: Path,
    runs_path: Path,
    expected: dict,
) -> dict:
    if not manifest_path.exists():
        raise ValueError("--resume requires an existing manifest.json")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for field in (
        "schema_version",
        "experiment_id",
        "seed",
        "redacted_config",
        "schedule",
    ):
        if manifest.get(field) != expected[field]:
            raise ValueError(
                f"resume manifest does not match current {field}"
            )
    completed = manifest.get("completed_runs")
    incidents = manifest.get("incidents")
    if not isinstance(completed, list) or not isinstance(incidents, list):
        raise ValueError("resume manifest has invalid run records")
    manifest.setdefault("skipped_runs", [])
    if not isinstance(manifest["skipped_runs"], list):
        raise ValueError("resume manifest has invalid skipped run records")
    completed_keys = {_run_key_from_mapping(item) for item in completed}
    csv_keys = {
        (
            row.get("scenario_id"),
            row.get("phase"),
            int(row.get("repeat_index", "-1")),
        )
        for row in _read_csv_rows(runs_path)
        if row.get("experiment_id") == expected["experiment_id"]
    }
    if not completed_keys.issubset(csv_keys):
        raise ValueError(
            "resume manifest completed runs are missing from runs.csv"
        )
    return manifest


def _config_fingerprint(
    config: ScenarioExperimentConfig,
    schedule: tuple[ScheduledScenarioRun, ...],
) -> str:
    payload = {
        "experiment_id": config.experiment_id,
        "seed": config.seed,
        "redacted_config": _redacted_config(config),
        "schedule": [asdict(item) for item in schedule],
    }
    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _repository_commit() -> str:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=CODE_ROOT.parent,
        check=True,
        capture_output=True,
        text=True,
    )
    commit = completed.stdout.strip()
    if not commit:
        raise RuntimeError("git rev-parse HEAD returned an empty commit")
    return commit


def _scheduled_run_key(
    scheduled: ScheduledScenarioRun,
) -> tuple[str, str, int]:
    return (
        scheduled.scenario_id,
        scheduled.phase,
        scheduled.repeat_index,
    )


def _run_key_from_mapping(item: dict) -> tuple[str, str, int]:
    try:
        return (
            str(item["scenario_id"]),
            str(item["phase"]),
            int(item["repeat_index"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("manifest contains an invalid run identity") from exc


def wait_for_idle(
    health_url: str,
    metrics_urls: tuple[str, ...],
    timeout_s: float,
) -> None:
    deadline_s = time.monotonic() + timeout_s
    last_reason = "not checked"
    while time.monotonic() < deadline_s:
        try:
            with request.urlopen(health_url, timeout=2.0) as response:
                healthy = response.status == 200
        except (OSError, error.URLError) as exc:
            healthy = False
            last_reason = f"health:{type(exc).__name__}"
        if healthy:
            all_idle = True
            for metrics_url in metrics_urls:
                try:
                    with request.urlopen(metrics_url, timeout=2.0) as response:
                        metrics = parse_prometheus_metrics(
                            response.read().decode("utf-8", errors="replace")
                        )
                    running = metrics.get("vllm:num_requests_running")
                    waiting = metrics.get("vllm:num_requests_waiting")
                    if running is None or waiting is None:
                        all_idle = False
                        last_reason = f"missing_idle_metrics_at_{metrics_url}"
                        break
                    if running != 0 or waiting != 0:
                        all_idle = False
                        last_reason = (
                            f"busy_at_{metrics_url}:"
                            f"running={running},waiting={waiting}"
                        )
                        break
                except (OSError, error.URLError) as exc:
                    all_idle = False
                    last_reason = (
                        f"metrics_at_{metrics_url}:{type(exc).__name__}"
                    )
                    break
            if all_idle:
                return
        time.sleep(0.25)
    raise TimeoutError(f"model service did not become idle: {last_reason}")


def _load_config(path: Path) -> ScenarioExperimentConfig:
    decoded = json.loads(path.read_text(encoding="utf-8"))
    if decoded.get("schema_version") != 1:
        raise ValueError("scenario config schema_version must be 1")
    experiment_id = decoded.get("experiment_id")
    if not isinstance(experiment_id, str) or not experiment_id.strip():
        raise ValueError("experiment_id must be non-empty")
    seed = decoded.get("seed")
    service_metadata = _normalize_service_metadata(
        decoded.get("service_metadata", {})
    )
    if decoded.get("require_complete_service_metadata") is True:
        validate_service_metadata(dict(service_metadata))
    calibration_contract = _load_calibration_contract(
        decoded.get("calibration_contract")
    )
    warmups = decoded.get("warmup_runs_per_scenario")
    repeats = decoded.get("formal_repeats")
    if not isinstance(seed, int) or isinstance(seed, bool):
        raise ValueError("seed must be an integer")
    if (
        not isinstance(warmups, int)
        or isinstance(warmups, bool)
        or not isinstance(repeats, int)
        or isinstance(repeats, bool)
    ):
        raise ValueError("scenario run counts must be integers")
    common_args = _validate_argument_list(
        decoded.get("common_args", []),
        "common_args",
    )
    raw_scenarios = decoded.get("scenarios")
    if not isinstance(raw_scenarios, list) or not raw_scenarios:
        raise ValueError("scenarios must be a non-empty list")
    scenarios = []
    for raw in raw_scenarios:
        if not isinstance(raw, dict):
            raise ValueError("each scenario must be an object")
        scenario_id = raw.get("scenario_id")
        if (
            not isinstance(scenario_id, str)
            or not _SCENARIO_ID_PATTERN.fullmatch(scenario_id)
        ):
            raise ValueError(
                "scenario_id must contain only letters, digits, dot, dash, or underscore"
            )
        scenarios.append(
            ScenarioDefinition(
                scenario_id,
                _validate_argument_list(
                    raw.get("args", []),
                    f"scenario {scenario_id} args",
                ),
            )
        )
    _validate_runtime_arguments(
        common_args,
        tuple(scenarios),
        service_metadata=service_metadata,
    )
    return ScenarioExperimentConfig(
        experiment_id=experiment_id,
        seed=seed,
        service_metadata=service_metadata,
        calibration_contract=calibration_contract,
        warmup_runs_per_scenario=warmups,
        formal_repeats=repeats,
        common_args=common_args,
        scenarios=tuple(scenarios),
    )


def _load_calibration_contract(
    value: object,
) -> CalibrationContract | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError("calibration_contract must be an object")
    raw_path = value.get("path")
    if not isinstance(raw_path, str) or not raw_path.strip():
        raise ValueError("calibration_contract.path must be non-empty")
    path = Path(
        _expand_environment_references(
            raw_path,
            "calibration_contract.path",
        )
    )
    raw_expected = value.get("expected")
    if not isinstance(raw_expected, dict) or not raw_expected:
        raise ValueError(
            "calibration_contract.expected must be a non-empty object"
        )
    expected: dict[str, JsonScalar] = {}
    for key, item in raw_expected.items():
        if not isinstance(key, str) or not key.strip():
            raise ValueError(
                "calibration_contract.expected keys must be non-empty"
            )
        if isinstance(item, str):
            expanded = _expand_environment_references(
                item,
                f"calibration_contract.expected.{key}",
            )
            if ENV_REFERENCE.fullmatch(item):
                try:
                    item = json.loads(expanded)
                except json.JSONDecodeError:
                    item = expanded
            else:
                item = expanded
        if not isinstance(item, (str, int, float, bool)) and item is not None:
            raise ValueError(
                "calibration_contract.expected values must be JSON scalars"
            )
        expected[key] = item
    return load_calibration_contract(path, expected)


def _validate_argument_list(values, label: str) -> tuple[str, ...]:
    if not isinstance(values, list) or any(
        not isinstance(value, str) for value in values
    ):
        raise ValueError(f"{label} must be a list of strings")
    expanded = tuple(
        _expand_environment_references(value, label) for value in values
    )
    for value in expanded:
        flag = value.split("=", 1)[0]
        if flag in _RUNNER_OWNED_FLAGS:
            raise ValueError(f"{label} contains runner-owned flag {flag}")
    return expanded


def _validate_runtime_arguments(
    common_args: tuple[str, ...],
    scenarios: tuple[ScenarioDefinition, ...],
    *,
    service_metadata: tuple[tuple[str, object], ...],
) -> None:
    """Fail before execution when required runtime endpoints expand to empty."""

    nonempty_if_present = (
        "--completion-endpoint-urls",
        "--completion-model",
        "--database-url",
        "--model-metrics-urls",
        "--ray-address",
        "--service-prefix-caching",
    )
    for scenario in scenarios:
        arguments = (*common_args, *scenario.args)
        values = {
            flag: _last_option_value(arguments, flag)
            for flag in nonempty_if_present
        }
        empty = [
            flag
            for flag, value in values.items()
            if value is not None and not value.strip()
        ]
        if empty:
            raise ValueError(
                f"scenario {scenario.scenario_id} has empty required option(s): "
                + ", ".join(empty)
            )
        declared_prefix_caching = dict(service_metadata).get("prefix_caching")
        recorded_prefix_caching = values["--service-prefix-caching"]
        if recorded_prefix_caching is not None and isinstance(
            declared_prefix_caching, bool
        ):
            expected = "enabled" if declared_prefix_caching else "disabled"
            if recorded_prefix_caching != expected:
                raise ValueError(
                    f"scenario {scenario.scenario_id} records "
                    f"--service-prefix-caching={recorded_prefix_caching} "
                    f"but service_metadata.prefix_caching={declared_prefix_caching}"
                )


def _last_option_value(arguments: tuple[str, ...], flag: str) -> str | None:
    value: str | None = None
    for index, argument in enumerate(arguments):
        if argument.startswith(flag + "="):
            value = argument.split("=", 1)[1]
        elif argument == flag:
            if index + 1 >= len(arguments) or arguments[index + 1].startswith(
                "--"
            ):
                raise ValueError(f"option {flag} requires a value")
            value = arguments[index + 1]
    return value


def _expand_environment_references(value: str, label: str) -> str:
    return expand_text(value, label)


def _normalize_service_metadata(
    value: object,
) -> tuple[tuple[str, object], ...]:
    if not isinstance(value, dict):
        raise ValueError("service_metadata must be an object")
    validated = []
    for key, item in value.items():
        if not isinstance(key, str) or not key.strip():
            raise ValueError("service_metadata keys must be non-empty strings")
        if isinstance(item, str):
            expanded = _expand_environment_references(
                item,
                f"service_metadata.{key}",
            )
            if ENV_REFERENCE.fullmatch(item):
                try:
                    item = json.loads(expanded)
                except json.JSONDecodeError:
                    item = expanded
            else:
                item = expanded
        if not isinstance(item, (str, int, float, bool)) and item is not None:
            raise ValueError("service_metadata values must be JSON scalars")
        if (
            isinstance(item, float)
            and not math.isfinite(item)
        ):
            raise ValueError("service_metadata numbers must be finite")
        validated.append((key, item))
    return tuple(sorted(validated))


def _build_profiler_command(
    options: RunnerOptions,
    config: ScenarioExperimentConfig,
    scenario: ScenarioDefinition,
    scheduled: ScheduledScenarioRun,
    runs_path: Path,
    run_stem: str,
) -> list[str]:
    return [
        str(options.python_executable),
        str(options.profiler_path),
        *config.common_args,
        *scenario.args,
        "--experiment-id",
        config.experiment_id,
        "--scenario-id",
        scheduled.scenario_id,
        "--random-seed",
        str(config.seed),
        "--run-phase",
        scheduled.phase,
        "--run-repeat-index",
        str(scheduled.repeat_index),
        "--output",
        str(runs_path),
        "--request-trace-output",
        str(options.output_dir / f"{run_stem}.requests.csv"),
        "--submission-trace-output",
        str(options.output_dir / f"{run_stem}.submissions.csv"),
        "--flush-trace-output",
        str(options.output_dir / f"{run_stem}.flush.csv"),
        "--control-trace-output",
        str(options.output_dir / f"{run_stem}.control.csv"),
        "--resource-trace-output",
        str(options.output_dir / f"{run_stem}.resources.csv"),
    ]


def _has_expected_new_row(
    before: list[dict[str, str]],
    after: list[dict[str, str]],
    config: ScenarioExperimentConfig,
    scheduled: ScheduledScenarioRun,
) -> bool:
    if len(after) != len(before) + 1:
        return False
    row = after[-1]
    return (
        row.get("status") in {"ok", "dry_run"}
        and row.get("experiment_id") == config.experiment_id
        and row.get("phase") == scheduled.phase
        and row.get("repeat_index") == str(scheduled.repeat_index)
        and row.get("scenario_id") == scheduled.scenario_id
        and row.get("random_seed") == str(config.seed)
    )


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _redacted_config(config: ScenarioExperimentConfig) -> dict:
    return {
        "experiment_id": config.experiment_id,
        "seed": config.seed,
        "service_metadata": _redact_service_metadata(
            config.service_metadata
        ),
        "calibration_contract": (
            {
                "path": config.calibration_contract.path,
                "sha256": config.calibration_contract.sha256,
                "selection": dict(config.calibration_contract.selection),
            }
            if config.calibration_contract is not None
            else None
        ),
        "warmup_runs_per_scenario": config.warmup_runs_per_scenario,
        "formal_repeats": config.formal_repeats,
        "common_args": _redact_argument_list(list(config.common_args)),
        "scenarios": [
            {
                "scenario_id": item.scenario_id,
                "args": _redact_argument_list(list(item.args)),
            }
            for item in config.scenarios
        ],
    }


def _redact_service_metadata(
    values: tuple[tuple[str, object], ...],
) -> dict[str, object]:
    sensitive_markers = ("api_key", "api-key", "auth", "secret", "password")
    return {
        key: (
            "***"
            if any(marker in key.lower() for marker in sensitive_markers)
            else value
        )
        for key, value in values
    }


def _write_json_atomic(path: Path, value: dict) -> None:
    temporary = path.with_name(f"{path.name}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temporary.replace(path)


def main() -> None:
    options = parse_args()
    # Pre-flight: fail-closed (or warn) if the scenario config's declared
    # ``service_metadata.prefix_caching`` contradicts the live vLLM process
    # flags, before acquiring the lease or touching the GPU. Done here rather
    # than in ``run_experiment`` so unit tests that drive ``run_experiment``
    # directly stay hermetic (no dependence on host vLLM state).
    _verify_prefix_caching_matches_live(_load_config(options.config_path))
    _verify_service_flags_match_live(_load_config(options.config_path))
    raise SystemExit(run_experiment(options))


if __name__ == "__main__":
    main()
