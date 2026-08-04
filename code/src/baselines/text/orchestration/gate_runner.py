"""Two-endpoint validity gate for text ceilings, controls, and native baselines."""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import subprocess
import time
import urllib.request
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Mapping, Sequence

from src.baselines.common.contracts import BaselineRequestResult
from src.baselines.common.gate import validate_gate
from src.baselines.common.manifests import read_manifest
from src.baselines.common.provenance import adapter_provenance
from src.baselines.common.results import summarize_group_service_counters
from src.infrastructure.config_env import expand_structure


CORE_ADAPTERS = (
    "vllm_bench",
    "bounded_http",
    "bounded_completions",
    "daft_native",
    "daft_ray",
    "ray_data_http",
)
BLOCKED_ADAPTER_REASONS = {
    "project_profiler": "requires_existing_project_profiler",
}

PairRunner = Callable[
    [list[list[str]], list[Path]],
    tuple[int, ...],
]
IdleWaiter = Callable[
    [tuple[str, ...], float],
    Mapping[int, Mapping[str, int]],
]
CounterSampler = Callable[
    [tuple[str, ...]],
    Mapping[int, Mapping[str, int]],
]


@dataclass(frozen=True)
class CoreGateCell:
    cell_id: str
    adapter: str
    concurrency: int
    batch_size: int
    ray_address: str | None


@dataclass(frozen=True)
class CoreGateConfig:
    experiment_id: str
    rows_total: int
    endpoint_urls: tuple[str, ...]
    completion_protocol: str
    model: str
    tokenizer: str | None
    manifest: Path
    output_root: Path
    cells: tuple[CoreGateCell, ...]
    blocked_cells: tuple[dict[str, str], ...]
    max_endpoint_work_skew: float


_BOOLEAN_HARD_GATES = {
    "provenance_fields_present",
    "native_arms_have_no_project_scheduler",
    "exactly_once",
    "both_endpoints_used",
    "service_counter_consistency",
    "same_model",
    "same_protocol",
    "same_service_config",
}
_ZERO_HARD_GATES = {
    "failed_rows",
    "worker_failures",
    "vllm_running_final",
    "vllm_waiting_final",
}
_NUMERIC_HARD_GATES = {"endpoint_predicted_work_skew_max"}


def _load_hard_gates(payload: Mapping[str, object]) -> float:
    """Validate the documented gate contract and return its skew threshold."""

    raw = payload.get("hard_gates", {})
    if not isinstance(raw, dict):
        raise ValueError("hard_gates must be an object")
    unknown = (
        set(raw)
        - _BOOLEAN_HARD_GATES
        - _ZERO_HARD_GATES
        - _NUMERIC_HARD_GATES
    )
    if unknown:
        raise ValueError(f"unsupported hard gate(s): {sorted(unknown)}")
    disabled = [name for name in _BOOLEAN_HARD_GATES if raw.get(name, True) is not True]
    if disabled:
        raise ValueError(
            "mandatory hard gates cannot be disabled: " + ", ".join(sorted(disabled))
        )
    invalid_zero = [
        name
        for name in _ZERO_HARD_GATES
        if name in raw
        and (
            isinstance(raw[name], bool)
            or not isinstance(raw[name], (int, float))
            or raw[name] != 0
        )
    ]
    if invalid_zero:
        raise ValueError(
            "zero-tolerance hard gates must remain 0: "
            + ", ".join(sorted(invalid_zero))
        )
    threshold_value = raw.get("endpoint_predicted_work_skew_max", 0.02)
    if isinstance(threshold_value, bool) or not isinstance(
        threshold_value, (int, float)
    ):
        raise ValueError(
            "endpoint_predicted_work_skew_max must be a JSON number"
        )
    threshold = float(threshold_value)
    if not math.isfinite(threshold) or threshold < 0 or threshold >= 1:
        raise ValueError(
            "endpoint_predicted_work_skew_max must be finite in [0, 1)"
        )
    return threshold


def _atomic_json(path: Path, payload: object) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _resolved_path(value: object, field: str) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty path")
    if "REPLACE_ME" in value:
        raise ValueError(f"{field} still contains REPLACE_ME")
    return Path(value)


def load_core_gate_config(
    path: str | Path,
    *,
    manifest_override: str | Path | None = None,
    output_root_override: str | Path | None = None,
    rows_total_override: int | None = None,
    include_cell_ids: Sequence[str] | None = None,
    concurrency_overrides: Mapping[str, int] | None = None,
) -> CoreGateConfig:
    """Load and fail closed on an unresolved or formal gate config."""

    payload = expand_structure(
        json.loads(Path(path).read_text(encoding="utf-8")),
        "core_gate_config",
    )
    if not isinstance(payload, dict):
        raise ValueError("core gate config must be an object")
    if payload.get("formal") is not False:
        raise ValueError("core gate runner only accepts formal=false")
    max_endpoint_work_skew = _load_hard_gates(payload)
    rows_total = (
        int(rows_total_override)
        if rows_total_override is not None
        else int(payload.get("rows_total", 0))
    )
    if rows_total <= 0:
        raise ValueError("rows_total must be positive")
    endpoint_urls = tuple(payload.get("endpoint_urls", ()))
    completion_protocol = str(
        payload.get("completion_protocol", "chat_completions")
    )
    if completion_protocol not in {"chat_completions", "completions"}:
        raise ValueError("unsupported completion_protocol")
    endpoint_suffix = (
        "/v1/completions"
        if completion_protocol == "completions"
        else "/v1/chat/completions"
    )
    if len(endpoint_urls) < 2 or any(
        not isinstance(url, str) or not url.endswith(endpoint_suffix)
        for url in endpoint_urls
    ):
        raise ValueError(
            "core gate requires at least two "
            f"{completion_protocol} endpoints"
        )
    model = payload.get("model")
    if not isinstance(model, str) or not model.strip():
        raise ValueError("model must be non-empty")
    manifest = (
        Path(manifest_override)
        if manifest_override is not None
        else _resolved_path(payload.get("manifest"), "manifest")
    )
    output_root = (
        Path(output_root_override)
        if output_root_override is not None
        else _resolved_path(payload.get("output_root"), "output_root")
    )
    if not manifest.is_file():
        raise ValueError(f"manifest does not exist: {manifest}")
    if output_root.exists():
        raise FileExistsError(f"output root already exists: {output_root}")

    requested_cell_ids = tuple(include_cell_ids or ())
    if len(set(requested_cell_ids)) != len(requested_cell_ids):
        raise ValueError("include_cell_ids contains duplicates")
    selected_cell_ids = set(requested_cell_ids) if requested_cell_ids else None
    normalized_overrides = dict(concurrency_overrides or {})
    if any(value <= 0 for value in normalized_overrides.values()):
        raise ValueError("concurrency overrides must be positive")

    raw_cells = payload.get("cells", ())
    if not isinstance(raw_cells, list):
        raise ValueError("cells must be a list")
    raw_cell_ids: list[str] = []
    for raw_cell in raw_cells:
        if not isinstance(raw_cell, dict):
            raise ValueError("each gate cell must be an object")
        cell_id = str(raw_cell.get("id", "")).strip()
        if not cell_id or cell_id in raw_cell_ids:
            raise ValueError(f"invalid or duplicate cell id: {cell_id!r}")
        raw_cell_ids.append(cell_id)
    available_cell_ids = set(raw_cell_ids)
    unknown_selected = set(requested_cell_ids) - available_cell_ids
    if unknown_selected:
        raise ValueError(
            f"unknown included gate cells: {sorted(unknown_selected)}"
        )
    unknown_overrides = set(normalized_overrides) - available_cell_ids
    if unknown_overrides:
        raise ValueError(
            f"unknown concurrency override cells: {sorted(unknown_overrides)}"
        )
    if selected_cell_ids is not None:
        excluded_overrides = set(normalized_overrides) - selected_cell_ids
        if excluded_overrides:
            raise ValueError(
                "concurrency overrides target excluded cells: "
                f"{sorted(excluded_overrides)}"
            )

    cells: list[CoreGateCell] = []
    blocked_cells: list[dict[str, str]] = []
    for raw_cell in raw_cells:
        cell_id = str(raw_cell.get("id", "")).strip()
        adapter = str(raw_cell.get("adapter", "")).strip()
        if selected_cell_ids is not None and cell_id not in selected_cell_ids:
            continue
        if adapter in BLOCKED_ADAPTER_REASONS:
            blocked_cells.append(
                {
                    "id": cell_id,
                    "adapter": adapter,
                    "reason": BLOCKED_ADAPTER_REASONS[adapter],
                }
            )
            continue
        if adapter not in CORE_ADAPTERS:
            raise ValueError(f"unsupported core gate adapter: {adapter!r}")
        adapter_provenance(adapter)
        if (
            completion_protocol == "completions"
            and adapter != "bounded_completions"
        ):
            raise ValueError(
                "Completions core gate currently supports "
                "bounded_completions cells only"
            )
        if (
            completion_protocol == "chat_completions"
            and adapter == "bounded_completions"
        ):
            raise ValueError(
                "bounded_completions requires completion_protocol=completions"
            )
        concurrency = int(
            normalized_overrides.get(
                cell_id,
                raw_cell.get("concurrency_per_endpoint", 1),
            )
        )
        batch_size = int(raw_cell.get("batch_size", 1))
        if concurrency <= 0 or batch_size <= 0:
            raise ValueError(f"cell {cell_id} concurrency/batch_size must be positive")
        ray_address = raw_cell.get("ray_address")
        if adapter in {"daft_ray", "ray_data_http"} and not ray_address:
            raise ValueError(f"cell {cell_id} requires an explicit ray_address")
        cells.append(
            CoreGateCell(
                cell_id=cell_id,
                adapter=adapter,
                concurrency=concurrency,
                batch_size=batch_size,
                ray_address=(str(ray_address) if ray_address is not None else None),
            )
        )
    if not cells:
        raise ValueError("config contains no runnable core gate cells")
    tokenizer = payload.get("tokenizer")
    if any(cell.adapter == "vllm_bench" for cell in cells):
        if not isinstance(tokenizer, str) or not tokenizer.strip():
            raise ValueError("vllm_bench cell requires an explicit tokenizer local directory")
        if not Path(tokenizer).is_dir():
            raise ValueError("vllm_bench tokenizer must be an existing local directory")
    return CoreGateConfig(
        experiment_id=str(payload.get("experiment_id", "core_gate")),
        rows_total=rows_total,
        endpoint_urls=endpoint_urls,
        completion_protocol=completion_protocol,
        model=model,
        tokenizer=(str(tokenizer) if tokenizer is not None else None),
        manifest=manifest,
        output_root=output_root,
        cells=tuple(cells),
        blocked_cells=tuple(blocked_cells),
        max_endpoint_work_skew=max_endpoint_work_skew,
    )


_QUEUE_PATTERNS = {
    "running": re.compile(r"^vllm:num_requests_running(?:\{[^}]*\})?\s+([0-9.eE+-]+)$"),
    "waiting": re.compile(r"^vllm:num_requests_waiting(?:\{[^}]*\})?\s+([0-9.eE+-]+)$"),
}
_TOKEN_COUNTER_PATTERNS = {
    "prompt_tokens": re.compile(
        r"^vllm:prompt_tokens_total(?:\{[^}]*\})?\s+([0-9.eE+-]+)$"
    ),
    "generation_tokens": re.compile(
        r"^vllm:generation_tokens_total(?:\{[^}]*\})?\s+([0-9.eE+-]+)$"
    ),
}


def parse_vllm_queue_metrics(text: str) -> dict[str, int]:
    """Parse and sum every model-labelled vLLM queue gauge."""

    parsed: dict[str, int] = {}
    lines = text.splitlines()
    for name, pattern in _QUEUE_PATTERNS.items():
        values = [float(match.group(1)) for line in lines if (match := pattern.match(line.strip()))]
        if not values:
            raise ValueError(f"vLLM metrics missing {name} queue gauge")
        total = sum(values)
        if not total.is_integer():
            raise ValueError(f"vLLM {name} queue gauge is not integral")
        parsed[name] = int(total)
    return parsed


def parse_vllm_token_counters(text: str) -> dict[str, int]:
    """Parse and sum cumulative prompt/generation token counters."""

    parsed: dict[str, int] = {}
    lines = text.splitlines()
    for name, pattern in _TOKEN_COUNTER_PATTERNS.items():
        values = [
            float(match.group(1))
            for line in lines
            if (match := pattern.match(line.strip()))
        ]
        if not values:
            raise ValueError(f"vLLM metrics missing {name} counter")
        total = sum(values)
        if not total.is_integer():
            raise ValueError(f"vLLM {name} counter is not integral")
        parsed[name] = int(total)
    return parsed


def _fetch_metrics(url: str) -> str:
    with urllib.request.urlopen(url, timeout=5.0) as response:
        return response.read().decode("utf-8")


def wait_for_idle(
    metrics_urls: tuple[str, ...],
    timeout_s: float,
) -> dict[int, dict[str, int]]:
    """Poll both endpoints until their running and waiting gauges are zero."""

    deadline = time.monotonic() + timeout_s
    latest: dict[int, dict[str, int]] = {}
    while True:
        latest = {
            index: parse_vllm_queue_metrics(_fetch_metrics(url))
            for index, url in enumerate(metrics_urls)
        }
        if all(row["running"] == 0 and row["waiting"] == 0 for row in latest.values()):
            return latest
        if time.monotonic() >= deadline:
            raise TimeoutError(f"vLLM queues did not drain before timeout: {latest}")
        time.sleep(0.5)


def sample_vllm_token_counters(
    metrics_urls: tuple[str, ...],
) -> dict[int, dict[str, int]]:
    """Snapshot cumulative token counters for every endpoint."""

    return {
        index: parse_vllm_token_counters(_fetch_metrics(url))
        for index, url in enumerate(metrics_urls)
    }


def run_command_pair(
    commands: list[list[str]],
    log_paths: list[Path],
) -> tuple[int, ...]:
    """Start every endpoint process before waiting for either one."""

    if len(commands) != len(log_paths):
        raise ValueError("commands and log_paths must have equal length")
    with ExitStack() as stack:
        logs = [
            stack.enter_context(path.open("x", encoding="utf-8", newline="\n"))
            for path in log_paths
        ]
        processes = [
            subprocess.Popen(
                command,
                stdout=log,
                stderr=subprocess.STDOUT,
                text=True,
            )
            for command, log in zip(commands, logs)
        ]
        return tuple(process.wait() for process in processes)


def _metrics_url(endpoint_url: str) -> str:
    return endpoint_url.split("/v1/", maxsplit=1)[0] + "/metrics"


def _shard_command(
    *,
    config: CoreGateConfig,
    cell: CoreGateCell,
    endpoint_index: int,
    output_dir: Path,
    driver_python: str,
    vllm_python: str,
) -> list[str]:
    script = Path(__file__).resolve().parents[4] / "scripts" / "run_official_baseline.py"
    command = [
        driver_python,
        str(script),
        "run-shard",
        "--adapter",
        cell.adapter,
        "--manifest",
        str(config.manifest),
        "--endpoint-index",
        str(endpoint_index),
        "--endpoint-url",
        config.endpoint_urls[endpoint_index],
        "--model",
        config.model,
        "--concurrency",
        str(cell.concurrency),
        "--batch-size",
        str(cell.batch_size),
        "--output-dir",
        str(output_dir),
        "--disable-arrival-replay",
    ]
    if cell.ray_address:
        command.extend(["--ray-address", cell.ray_address])
    if cell.adapter == "vllm_bench":
        if config.tokenizer is None:
            raise ValueError("vllm_bench tokenizer is missing")
        command.extend(
            [
                "--python-executable",
                vllm_python,
                "--tokenizer",
                config.tokenizer,
            ]
        )
    return command


def _normalization_command(
    *,
    config: CoreGateConfig,
    endpoint_index: int,
    raw_output_dir: Path,
    output_dir: Path,
    queue: Mapping[str, int],
    driver_python: str,
) -> list[str]:
    script = Path(__file__).resolve().parents[4] / "scripts" / "run_official_baseline.py"
    return [
        driver_python,
        str(script),
        "normalize-vllm-bench",
        "--manifest",
        str(config.manifest),
        "--endpoint-index",
        str(endpoint_index),
        "--endpoint-url",
        config.endpoint_urls[endpoint_index],
        "--model",
        config.model,
        "--input",
        str(raw_output_dir / "raw" / "vllm_bench.json"),
        "--output-dir",
        str(output_dir),
        "--vllm-running-final",
        str(queue["running"]),
        "--vllm-waiting-final",
        str(queue["waiting"]),
    ]


def _read_results(path: Path) -> tuple[BaselineRequestResult, ...]:
    with path.open(encoding="utf-8", newline="") as stream:
        rows = tuple(csv.DictReader(stream))
    return tuple(
        BaselineRequestResult(
            doc_id=int(row["doc_id"]),
            endpoint_index=int(row["endpoint_index"]),
            status=row["status"],
            error=row["error"] or None,
            submitted_at_s=float(row["submitted_at_s"]),
            started_at_s=float(row["started_at_s"]),
            completed_at_s=float(row["completed_at_s"]),
            input_tokens=int(row["input_tokens"]),
            output_tokens=int(row["output_tokens"]),
            output_text=row["output_text"] or None,
            finish_reason=row["finish_reason"] or None,
        )
        for row in rows
    )


def _stamp_final_queues(
    output_dirs: tuple[Path, ...],
    queues: Mapping[int, Mapping[str, int]],
) -> None:
    for endpoint_index, output_dir in enumerate(output_dirs):
        summary_path = output_dir / "summary.json"
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        summary["vllm_num_requests_running_final"] = queues[endpoint_index]["running"]
        summary["vllm_num_requests_waiting_final"] = queues[endpoint_index]["waiting"]
        _atomic_json(summary_path, summary)


def _service_counter_evidence(
    before: Mapping[int, Mapping[str, int]],
    after: Mapping[int, Mapping[str, int]],
) -> dict[str, object]:
    if set(before) != set(after):
        raise ValueError("service counter endpoint set changed during cell")
    delta: dict[int, dict[str, int]] = {}
    for endpoint_index in sorted(before):
        if set(before[endpoint_index]) != set(after[endpoint_index]):
            raise ValueError(
                f"service counter keys changed for endpoint {endpoint_index}"
            )
        endpoint_delta = {
            name: after[endpoint_index][name]
            - before[endpoint_index][name]
            for name in before[endpoint_index]
        }
        if any(value < 0 for value in endpoint_delta.values()):
            raise ValueError(
                f"service counter decreased for endpoint {endpoint_index}"
            )
        delta[endpoint_index] = endpoint_delta
    return {
        "before": before,
        "after": after,
        "delta": delta,
    }


def _stamp_service_counters(
    output_dirs: tuple[Path, ...],
    delta: Mapping[int, Mapping[str, int]],
) -> None:
    for endpoint_index, output_dir in enumerate(output_dirs):
        endpoint_delta = delta[endpoint_index]
        prompt_tokens = endpoint_delta["prompt_tokens"]
        generation_tokens = endpoint_delta["generation_tokens"]
        summary_path = output_dir / "summary.json"
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        summary.update(
            {
                "service_counter_status": "ok",
                "service_prompt_tokens_delta": prompt_tokens,
                "service_generation_tokens_delta": generation_tokens,
                "service_total_tokens_delta": (
                    prompt_tokens + generation_tokens
                ),
                "service_prompt_tokens_per_s": (
                    prompt_tokens / float(summary["jct_s"])
                    if float(summary.get("jct_s", 0.0)) > 0
                    else 0.0
                ),
                "service_generation_tokens_per_s": (
                    generation_tokens / float(summary["jct_s"])
                    if float(summary.get("jct_s", 0.0)) > 0
                    else 0.0
                ),
                "service_total_tokens_per_s": (
                    (prompt_tokens + generation_tokens)
                    / float(summary["jct_s"])
                    if float(summary.get("jct_s", 0.0)) > 0
                    else 0.0
                ),
            }
        )
        _atomic_json(summary_path, summary)


def validate_service_counter_summary(
    summary: Mapping[str, object],
    endpoint_index: int,
) -> tuple[str, ...]:
    """Validate service-side token deltas and compatible client counters."""

    incidents: list[str] = []
    if summary.get("service_counter_status") != "ok":
        return (f"endpoint {endpoint_index} missing service token counters",)

    prompt_delta = int(summary.get("service_prompt_tokens_delta", 0))
    generation_delta = int(summary.get("service_generation_tokens_delta", 0))
    if prompt_delta <= 0:
        incidents.append(
            f"endpoint {endpoint_index} missing positive service prompt delta"
        )
    if generation_delta <= 0:
        incidents.append(
            f"endpoint {endpoint_index} missing positive service generation delta"
        )

    accounting = summary.get("token_accounting")
    if accounting == "server_usage":
        if int(summary.get("input_tokens", -1)) != prompt_delta:
            incidents.append(
                f"endpoint {endpoint_index} client/service prompt mismatch"
            )
        if int(summary.get("output_tokens", -1)) != generation_delta:
            incidents.append(
                f"endpoint {endpoint_index} client/service generation mismatch"
            )
    elif accounting == "official_benchmark":
        if int(summary.get("output_tokens", -1)) != generation_delta:
            incidents.append(
                f"endpoint {endpoint_index} benchmark/service generation mismatch"
            )
    elif accounting not in {
        "manifest_prompt_only",
        "service_counter",
        "unavailable",
    }:
        incidents.append(
            f"endpoint {endpoint_index} unknown token accounting {accounting!r}"
        )

    return tuple(incidents)


def validate_configured_service_identity(
    summaries: Sequence[Mapping[str, object]],
    *,
    model: str,
    completion_protocol: str,
) -> tuple[str, ...]:
    """Reject summaries that agree with each other but not with the config."""

    incidents: list[str] = []
    if any(summary.get("model_name") != model for summary in summaries):
        incidents.append("configured_model_mismatch")
    if any(
        summary.get("completion_protocol") != completion_protocol
        for summary in summaries
    ):
        incidents.append("configured_protocol_mismatch")
    return tuple(incidents)


def _validate_cell(
    config: CoreGateConfig,
    output_dirs: tuple[Path, ...],
    gate_path: Path,
) -> dict[str, object]:
    summaries = tuple(
        json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
        for output_dir in output_dirs
    )
    results = tuple(
        result
        for output_dir in output_dirs
        for result in _read_results(output_dir / "requests.csv")
    )
    counter_incidents = [
        incident
        for endpoint_index, summary in enumerate(summaries)
        for incident in validate_service_counter_summary(
            summary,
            endpoint_index,
        )
    ]
    identity_incidents = validate_configured_service_identity(
        summaries,
        model=config.model,
        completion_protocol=config.completion_protocol,
    )
    report = validate_gate(
        manifest=read_manifest(config.manifest),
        summaries=summaries,
        request_results=results,
        max_endpoint_work_skew=config.max_endpoint_work_skew,
    )
    payload = {
        "status": (
            "passed"
            if report.passed and not counter_incidents and not identity_incidents
            else "failed"
        ),
        "passed": (
            report.passed and not counter_incidents and not identity_incidents
        ),
        "incidents": (
            list(report.incidents)
            + counter_incidents
            + list(identity_incidents)
        ),
        "metrics": {
            **report.metrics,
            **summarize_group_service_counters(summaries, results),
        },
    }
    _atomic_json(gate_path, payload)
    return payload


def run_core_gate(
    config_path: str | Path,
    *,
    driver_python: str,
    vllm_python: str,
    manifest_override: str | Path | None = None,
    output_root_override: str | Path | None = None,
    rows_total_override: int | None = None,
    include_cell_ids: Sequence[str] | None = None,
    concurrency_overrides: Mapping[str, int] | None = None,
    idle_timeout_s: float = 120.0,
    pair_runner: PairRunner = run_command_pair,
    idle_waiter: IdleWaiter = wait_for_idle,
    counter_sampler: CounterSampler = sample_vllm_token_counters,
) -> dict[str, object]:
    """Run each core adapter once, fail closed, and preserve all evidence."""

    config = load_core_gate_config(
        config_path,
        manifest_override=manifest_override,
        output_root_override=output_root_override,
        rows_total_override=rows_total_override,
        include_cell_ids=include_cell_ids,
        concurrency_overrides=concurrency_overrides,
    )
    manifest = read_manifest(config.manifest)
    if len(manifest) != config.rows_total:
        raise ValueError(
            f"manifest row count does not match rows_total: {len(manifest)} != {config.rows_total}"
        )
    endpoints = {request.endpoint_index for request in manifest}
    if endpoints != {0, 1}:
        raise ValueError(f"manifest must use endpoint indexes 0 and 1: {endpoints}")

    config.output_root.mkdir(parents=True)
    resolved = {
        "experiment_id": config.experiment_id,
        "formal": False,
        "rows_total": config.rows_total,
        "endpoint_urls": list(config.endpoint_urls),
        "completion_protocol": config.completion_protocol,
        "model": config.model,
        "tokenizer": config.tokenizer,
        "max_endpoint_work_skew": config.max_endpoint_work_skew,
        "manifest": str(config.manifest),
        "output_root": str(config.output_root),
        "driver_python": driver_python,
        "vllm_python": vllm_python,
        "cells": [
            {
                "id": cell.cell_id,
                "adapter": cell.adapter,
                "concurrency_per_endpoint": cell.concurrency,
                "batch_size": cell.batch_size,
                "ray_address": cell.ray_address,
                **adapter_provenance(cell.adapter).summary_fields(),
            }
            for cell in config.cells
        ],
        "blocked_cells": list(config.blocked_cells),
    }
    _atomic_json(config.output_root / "resolved_config.json", resolved)
    metrics_urls = tuple(_metrics_url(url) for url in config.endpoint_urls)
    completed_cells: list[str] = []

    try:
        for cell in config.cells:
            cell_root = config.output_root / cell.cell_id
            cell_root.mkdir()
            raw_dirs = tuple(
                cell_root
                / (
                    f"raw_shard_{endpoint_index}"
                    if cell.adapter == "vllm_bench"
                    else f"shard_{endpoint_index}"
                )
                for endpoint_index in (0, 1)
            )
            commands = [
                _shard_command(
                    config=config,
                    cell=cell,
                    endpoint_index=endpoint_index,
                    output_dir=raw_dirs[endpoint_index],
                    driver_python=driver_python,
                    vllm_python=vllm_python,
                )
                for endpoint_index in (0, 1)
            ]
            _atomic_json(cell_root / "commands.json", commands)
            counters_before = counter_sampler(metrics_urls)
            return_codes = pair_runner(
                commands,
                [
                    cell_root / "shard_0.log",
                    cell_root / "shard_1.log",
                ],
            )
            if return_codes != (0, 0):
                raise RuntimeError(f"cell {cell.cell_id} shard exits: {return_codes}")
            queues = idle_waiter(metrics_urls, idle_timeout_s)
            counter_evidence = _service_counter_evidence(
                counters_before,
                counter_sampler(metrics_urls),
            )
            _atomic_json(
                cell_root / "service_counters.json",
                counter_evidence,
            )

            output_dirs = raw_dirs
            if cell.adapter == "vllm_bench":
                output_dirs = tuple(
                    cell_root / f"shard_{endpoint_index}" for endpoint_index in (0, 1)
                )
                normalize_commands = [
                    _normalization_command(
                        config=config,
                        endpoint_index=endpoint_index,
                        raw_output_dir=raw_dirs[endpoint_index],
                        output_dir=output_dirs[endpoint_index],
                        queue=queues[endpoint_index],
                        driver_python=driver_python,
                    )
                    for endpoint_index in (0, 1)
                ]
                _atomic_json(
                    cell_root / "normalize_commands.json",
                    normalize_commands,
                )
                normalize_codes = pair_runner(
                    normalize_commands,
                    [
                        cell_root / "normalize_0.log",
                        cell_root / "normalize_1.log",
                    ],
                )
                if normalize_codes != (0, 0):
                    raise RuntimeError(
                        f"cell {cell.cell_id} normalization exits: {normalize_codes}"
                    )
            _stamp_final_queues(output_dirs, queues)
            _stamp_service_counters(
                output_dirs,
                counter_evidence["delta"],
            )

            gate = _validate_cell(
                config,
                output_dirs,
                cell_root / "gate.json",
            )
            if gate["status"] != "passed":
                raise RuntimeError(f"cell {cell.cell_id} gate failed: {gate['incidents']}")
            completed_cells.append(cell.cell_id)
    except Exception as exc:
        failed = {
            "status": "failed",
            "completed_cells": completed_cells,
            "blocked_cells": list(config.blocked_cells),
            "error": f"{type(exc).__name__}: {exc}",
        }
        _atomic_json(config.output_root / "run_status.json", failed)
        raise

    passed = {
        "status": "passed",
        "scope": "text_comparison_validity_gate",
        "completed_cells": completed_cells,
        "blocked_cells": list(config.blocked_cells),
    }
    _atomic_json(config.output_root / "run_status.json", passed)
    return passed


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run the reproducible two-endpoint text comparison validity gate."
        )
    )
    parser.add_argument("--config", required=True)
    parser.add_argument("--driver-python", required=True)
    parser.add_argument("--vllm-python", required=True)
    parser.add_argument("--manifest")
    parser.add_argument("--output-root")
    parser.add_argument("--rows-total", type=int)
    parser.add_argument(
        "--include-cell",
        action="append",
        default=[],
        help="Run only this configured cell; repeat for multiple cells.",
    )
    parser.add_argument(
        "--concurrency-override",
        action="append",
        default=[],
        metavar="CELL_ID=N",
        help="Override per-endpoint concurrency for one cell; repeat as needed.",
    )
    parser.add_argument("--idle-timeout-s", type=float, default=120.0)
    return parser


def parse_concurrency_overrides(values: Sequence[str]) -> dict[str, int]:
    """Parse repeatable CELL_ID=N overrides and reject ambiguous input."""

    overrides: dict[str, int] = {}
    for value in values:
        cell_id, separator, raw_concurrency = value.partition("=")
        cell_id = cell_id.strip()
        if not separator or not cell_id or not raw_concurrency.strip():
            raise ValueError(
                f"invalid concurrency override {value!r}; expected CELL_ID=N"
            )
        if cell_id in overrides:
            raise ValueError(
                f"duplicate concurrency override for cell {cell_id!r}"
            )
        try:
            concurrency = int(raw_concurrency)
        except ValueError as exc:
            raise ValueError(
                f"invalid concurrency for cell {cell_id!r}: {raw_concurrency!r}"
            ) from exc
        if concurrency <= 0:
            raise ValueError(
                f"concurrency for cell {cell_id!r} must be positive"
            )
        overrides[cell_id] = concurrency
    return overrides


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        concurrency_overrides = parse_concurrency_overrides(
            args.concurrency_override
        )
        result = run_core_gate(
            args.config,
            driver_python=args.driver_python,
            vllm_python=args.vllm_python,
            manifest_override=args.manifest,
            output_root_override=args.output_root,
            rows_total_override=args.rows_total,
            include_cell_ids=args.include_cell,
            concurrency_overrides=concurrency_overrides,
            idle_timeout_s=args.idle_timeout_s,
        )
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "failed",
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
        )
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0
