"""Reproducible two-endpoint orchestration for the official baseline gate."""

from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
import time
import urllib.request
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Mapping, Sequence

from .contracts import BaselineRequestResult
from .gate import validate_gate
from .manifests import read_manifest


CORE_ADAPTERS = (
    "vllm_bench",
    "bounded_http",
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
    model: str
    tokenizer: str | None
    manifest: Path
    output_root: Path
    cells: tuple[CoreGateCell, ...]
    blocked_cells: tuple[dict[str, str], ...]


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
) -> CoreGateConfig:
    """Load and fail closed on an unresolved or formal gate config."""

    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if payload.get("formal") is not False:
        raise ValueError("core gate runner only accepts formal=false")
    rows_total = int(payload.get("rows_total", 0))
    if rows_total <= 0:
        raise ValueError("rows_total must be positive")
    endpoint_urls = tuple(payload.get("endpoint_urls", ()))
    if len(endpoint_urls) != 2 or any(
        not isinstance(url, str) or not url.endswith("/v1/chat/completions")
        for url in endpoint_urls
    ):
        raise ValueError("core gate requires exactly two Chat Completions endpoints")
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

    cells: list[CoreGateCell] = []
    blocked_cells: list[dict[str, str]] = []
    seen_ids: set[str] = set()
    for raw_cell in payload.get("cells", ()):
        if not isinstance(raw_cell, dict):
            raise ValueError("each gate cell must be an object")
        cell_id = str(raw_cell.get("id", "")).strip()
        adapter = str(raw_cell.get("adapter", "")).strip()
        if not cell_id or cell_id in seen_ids:
            raise ValueError(f"invalid or duplicate cell id: {cell_id!r}")
        seen_ids.add(cell_id)
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
        concurrency = int(raw_cell.get("concurrency_per_endpoint", 1))
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
        model=model,
        tokenizer=(str(tokenizer) if tokenizer is not None else None),
        manifest=manifest,
        output_root=output_root,
        cells=tuple(cells),
        blocked_cells=tuple(blocked_cells),
    )


_QUEUE_PATTERNS = {
    "running": re.compile(r"^vllm:num_requests_running(?:\{[^}]*\})?\s+([0-9.eE+-]+)$"),
    "waiting": re.compile(r"^vllm:num_requests_waiting(?:\{[^}]*\})?\s+([0-9.eE+-]+)$"),
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
    script = Path(__file__).resolve().parents[2] / "scripts" / "run_official_baseline.py"
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
    script = Path(__file__).resolve().parents[2] / "scripts" / "run_official_baseline.py"
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
    report = validate_gate(
        manifest=read_manifest(config.manifest),
        summaries=summaries,
        request_results=results,
    )
    payload = {
        "status": "passed" if report.passed else "failed",
        "passed": report.passed,
        "incidents": list(report.incidents),
        "metrics": report.metrics,
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
    idle_timeout_s: float = 120.0,
    pair_runner: PairRunner = run_command_pair,
    idle_waiter: IdleWaiter = wait_for_idle,
) -> dict[str, object]:
    """Run each core adapter once, fail closed, and preserve all evidence."""

    config = load_core_gate_config(
        config_path,
        manifest_override=manifest_override,
        output_root_override=output_root_override,
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
        "model": config.model,
        "tokenizer": config.tokenizer,
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
            else:
                _stamp_final_queues(output_dirs, queues)

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
        "scope": "core_official_adapters",
        "completed_cells": completed_cells,
        "blocked_cells": list(config.blocked_cells),
    }
    _atomic_json(config.output_root / "run_status.json", passed)
    return passed


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the reproducible two-endpoint official baseline gate."
    )
    parser.add_argument("--config", required=True)
    parser.add_argument("--driver-python", required=True)
    parser.add_argument("--vllm-python", required=True)
    parser.add_argument("--manifest")
    parser.add_argument("--output-root")
    parser.add_argument("--idle-timeout-s", type=float, default=120.0)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = run_core_gate(
            args.config,
            driver_python=args.driver_python,
            vllm_python=args.vllm_python,
            manifest_override=args.manifest,
            output_root_override=args.output_root,
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
