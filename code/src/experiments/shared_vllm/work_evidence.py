"""Fail-closed joins for request estimates and endpoint-observed token work."""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, Mapping

from .config import CompletionWorkCostConfig


@dataclass(frozen=True)
class JoinedRequestWork:
    scenario_id: str
    job_id: str
    endpoint_id: str
    request_id: str
    submission_id: str
    doc_id: str
    completion_epoch_s: float
    raw_prompt_tokens: int
    endpoint_prompt_tokens: int
    endpoint_output_tokens: int
    actual_work: int
    estimated_work: int
    prompt_overhead_tokens: int
    token_count_source: str
    input_token_count_source: str
    output_token_count_source: str


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def model_artifact_calibration_identity(model_root: Path) -> dict[str, object]:
    """Hash the exact tokenizer/template files used by the completion service."""

    root = model_root.resolve()
    required = {
        "model_config_sha256": root / "config.json",
        "tokenizer_config_sha256": root / "tokenizer_config.json",
        "tokenizer_json_sha256": root / "tokenizer.json",
    }
    missing = [str(path) for path in required.values() if not path.is_file()]
    if missing:
        raise ValueError(
            "work-cost calibration model files are missing: "
            + ", ".join(missing)
        )
    tokenizer_config = json.loads(
        required["tokenizer_config_sha256"].read_text(encoding="utf-8")
    )
    chat_template = tokenizer_config.get("chat_template")
    if not isinstance(chat_template, str) or not chat_template:
        raise ValueError("tokenizer_config.json lacks a chat_template")
    tree_paths = sorted(
        (root / ".cache" / "huggingface" / "trees").glob("*.json")
    )
    if len(tree_paths) != 1 or len(tree_paths[0].stem) != 40:
        raise ValueError("model artifact lacks one unambiguous Hub revision")
    revision = tree_paths[0].stem
    return {
        "model_revision": revision,
        "tokenizer_revision": revision,
        **{key: sha256_file(path) for key, path in required.items()},
        "chat_template_sha256": hashlib.sha256(
            chat_template.encode("utf-8")
        ).hexdigest(),
    }


def _required_text(row: Mapping[str, str], key: str, context: str) -> str:
    value = str(row.get(key, "") or "").strip()
    if not value:
        raise ValueError(f"{context}: missing {key}")
    return value


def _nonnegative_int(row: Mapping[str, str], key: str, context: str) -> int:
    value = _required_text(row, key, context)
    try:
        resolved = int(value)
    except ValueError as exc:
        raise ValueError(f"{context}: {key} is not an integer") from exc
    if resolved < 0:
        raise ValueError(f"{context}: {key} must be non-negative")
    return resolved


def _unique_by(
    rows: Iterable[Mapping[str, str]],
    key: str,
    context: str,
) -> dict[str, Mapping[str, str]]:
    indexed: dict[str, Mapping[str, str]] = {}
    for row in rows:
        value = _required_text(row, key, context)
        if value in indexed:
            raise ValueError(f"{context}: duplicate {key} {value!r}")
        indexed[value] = row
    return indexed


def join_request_submission_work(
    request_rows: list[dict[str, str]],
    submission_rows: list[dict[str, str]],
    *,
    work_cost: CompletionWorkCostConfig,
    context: str,
    require_endpoint_usage: bool,
    require_estimate_upper_bound: bool,
) -> list[JoinedRequestWork]:
    """Join one Job's traces and independently recompute actual/estimated work."""

    if not request_rows or not submission_rows:
        raise ValueError(f"{context}: request/submission evidence is empty")
    requests = _unique_by(request_rows, "submission_id", context)
    submissions = _unique_by(submission_rows, "submission_id", context)
    if set(requests) != set(submissions):
        missing_submissions = len(set(requests) - set(submissions))
        missing_requests = len(set(submissions) - set(requests))
        raise ValueError(
            f"{context}: request/submission ID sets differ "
            f"({missing_submissions} missing submissions, "
            f"{missing_requests} missing requests)"
        )
    request_ids = [
        _required_text(row, "request_id", context) for row in request_rows
    ]
    doc_ids = [_required_text(row, "doc_id", context) for row in request_rows]
    if len(set(request_ids)) != len(request_ids):
        raise ValueError(f"{context}: request_id values are not unique")
    if len(set(doc_ids)) != len(doc_ids):
        raise ValueError(f"{context}: doc_id values are not unique")

    joined: list[JoinedRequestWork] = []
    for submission_id in sorted(requests):
        request = requests[submission_id]
        submission = submissions[submission_id]
        item_context = f"{context}/{submission_id}"
        if request.get("status") != "completed" or request.get(
            "error_type", ""
        ).strip():
            raise ValueError(f"{item_context}: request did not complete cleanly")
        if submission.get("status") != "completed" or submission.get(
            "error", ""
        ).strip():
            raise ValueError(
                f"{item_context}: submission did not complete cleanly"
            )
        if _nonnegative_int(submission, "rows", item_context) != 1:
            raise ValueError(f"{item_context}: submission must contain one row")

        for key in ("experiment_id", "phase", "repeat_index", "job_id"):
            if _required_text(request, key, item_context) != _required_text(
                submission,
                key,
                item_context,
            ):
                raise ValueError(f"{item_context}: {key} identity mismatch")
        endpoint_id = _required_text(request, "endpoint_id", item_context)
        if endpoint_id != _required_text(
            submission,
            "endpoint_id",
            item_context,
        ):
            raise ValueError(f"{item_context}: endpoint_id identity mismatch")
        doc_id = _required_text(request, "doc_id", item_context)
        if _required_text(submission, "doc_ids", item_context) != doc_id:
            raise ValueError(f"{item_context}: doc_id identity mismatch")

        raw_prompt = _nonnegative_int(request, "prompt_tokens", item_context)
        actual_output = _nonnegative_int(
            request,
            "actual_output_tokens",
            item_context,
        )
        # ``client_estimated_output_tokens`` is a post-hoc client-side
        # retokenization of the returned text. It is diagnostic evidence, not
        # the estimate charged by admission. The admission estimate is stored
        # separately and equals the output cap for fixed-output-cap runs.
        admission_estimated_output = _nonnegative_int(
            request,
            "estimated_output_tokens",
            item_context,
        )
        actual_work = _nonnegative_int(
            submission,
            "token_count",
            item_context,
        )
        endpoint_prompt = _nonnegative_int(
            submission,
            "input_token_count",
            item_context,
        )
        endpoint_output = _nonnegative_int(
            submission,
            "output_token_count",
            item_context,
        )
        token_source = str(submission.get("token_count_source", "") or "")
        input_source = str(
            submission.get("input_token_count_source", "") or ""
        )
        output_source = str(
            submission.get("output_token_count_source", "") or ""
        )
        if require_endpoint_usage:
            if token_source != "endpoint_usage_total_tokens":
                raise ValueError(
                    f"{item_context}: total work is not endpoint usage"
                )
            if input_source != "endpoint_usage_prompt_tokens":
                raise ValueError(
                    f"{item_context}: input work is not endpoint usage"
                )
            if output_source not in {
                "endpoint_usage_completion_tokens",
                "endpoint_token_ids",
            }:
                raise ValueError(
                    f"{item_context}: output work is not endpoint/token-ID data"
                )
            if request.get("output_token_source") != "endpoint_request":
                raise ValueError(
                    f"{item_context}: request output work lacks endpoint source"
                )
            if actual_work != endpoint_prompt + endpoint_output:
                raise ValueError(
                    f"{item_context}: endpoint usage components do not sum"
                )
            if actual_output != endpoint_output:
                raise ValueError(
                    f"{item_context}: request/submission output work differs"
                )
            request_total = _nonnegative_int(
                request,
                "total_tokens",
                item_context,
            )
            if request_total != raw_prompt + actual_output:
                raise ValueError(
                    f"{item_context}: raw request token identity is invalid"
                )

        overhead = endpoint_prompt - raw_prompt
        if overhead != work_cost.prompt_token_overhead_per_request:
            raise ValueError(
                f"{item_context}: prompt overhead {overhead} != "
                f"{work_cost.prompt_token_overhead_per_request}"
            )
        estimated_work = work_cost.estimated_work(
            raw_prompt,
            admission_estimated_output,
        )
        if require_estimate_upper_bound and actual_work > estimated_work:
            raise ValueError(
                f"{item_context}: actual work {actual_work} exceeds estimate "
                f"{estimated_work}"
            )
        joined.append(
            JoinedRequestWork(
                scenario_id=_required_text(
                    request,
                    "scenario_id",
                    item_context,
                ),
                job_id=_required_text(request, "job_id", item_context),
                endpoint_id=endpoint_id,
                request_id=_required_text(request, "request_id", item_context),
                submission_id=submission_id,
                doc_id=doc_id,
                completion_epoch_s=float(
                    _required_text(request, "completion_epoch_s", item_context)
                ),
                raw_prompt_tokens=raw_prompt,
                endpoint_prompt_tokens=endpoint_prompt,
                endpoint_output_tokens=endpoint_output,
                actual_work=actual_work,
                estimated_work=estimated_work,
                prompt_overhead_tokens=overhead,
                token_count_source=token_source,
                input_token_count_source=input_source,
                output_token_count_source=output_source,
            )
        )
    return joined


def cell_trace_paths(
    root: Path,
    row: Mapping[str, str],
    *,
    job_count: int,
) -> list[tuple[Path, Path]]:
    order = int(_required_text(row, "order_index", "group row"))
    phase = _required_text(row, "phase", "group row")
    repeat = int(_required_text(row, "repeat_index", "group row"))
    scenario = _required_text(row, "scenario_id", "group row")
    paths = []
    for job_index in range(job_count):
        stem = f"{order:03d}_{phase}_{repeat}_{scenario}_job{job_index}"
        paths.append(
            (
                root / "jobs" / f"{stem}.requests.csv",
                root / "jobs" / f"{stem}.submissions.csv",
            )
        )
    return paths


def joined_cell_work(
    root: Path,
    row: Mapping[str, str],
    *,
    work_cost: CompletionWorkCostConfig,
    job_count: int = 2,
    require_estimate_upper_bound: bool = True,
) -> tuple[list[list[JoinedRequestWork]], list[Path]]:
    """Load and strictly validate all per-Job work evidence for one cell."""

    by_job: list[list[JoinedRequestWork]] = []
    input_paths: list[Path] = []
    for job_index, (request_path, submission_path) in enumerate(
        cell_trace_paths(root, row, job_count=job_count)
    ):
        if not request_path.is_file() or not submission_path.is_file():
            raise ValueError(
                f"cell {row.get('scenario_id', '')} job {job_index}: "
                "request/submission trace is missing"
            )
        input_paths.extend((request_path, submission_path))
        by_job.append(
            join_request_submission_work(
                read_csv(request_path),
                read_csv(submission_path),
                work_cost=work_cost,
                context=(
                    f"{row.get('scenario_id', '')}/job-{job_index}"
                ),
                require_endpoint_usage=True,
                require_estimate_upper_bound=require_estimate_upper_bound,
            )
        )
    return by_job, input_paths


def audit_work_cost_matrix(
    root: Path,
    *,
    work_cost: CompletionWorkCostConfig,
    expected_scenarios: Iterable[str],
    expected_phase: str,
    expected_repeat_indexes: Iterable[int],
    expected_requests_per_cell: int,
) -> dict[str, object]:
    """Audit source, identity, coverage, overhead, and upper bounds for a matrix."""

    resolved_root = root.resolve()
    errors: list[str] = []
    group_path = resolved_root / "group_runs.csv"
    try:
        group_rows = read_csv(group_path)
    except OSError as exc:
        group_rows = []
        errors.append(f"cannot read group_runs.csv: {exc}")
    expected_cells = {
        (scenario, expected_phase, repeat)
        for scenario in expected_scenarios
        for repeat in expected_repeat_indexes
    }
    rows_by_cell: dict[tuple[str, str, int], dict[str, str]] = {}
    for row in group_rows:
        try:
            identity = (
                _required_text(row, "scenario_id", "group row"),
                _required_text(row, "phase", "group row"),
                int(_required_text(row, "repeat_index", "group row")),
            )
        except (ValueError, TypeError) as exc:
            errors.append(str(exc))
            continue
        if identity not in expected_cells:
            continue
        if identity in rows_by_cell:
            errors.append(f"duplicate group cell {identity}")
        rows_by_cell[identity] = row
    if set(rows_by_cell) != expected_cells:
        errors.append(
            "group matrix cells differ from the frozen scenario/phase/repeat set"
        )

    all_joined: list[JoinedRequestWork] = []
    input_paths: list[Path] = [group_path] if group_path.is_file() else []
    cell_results: dict[str, dict[str, object]] = {}
    for identity in sorted(expected_cells):
        row = rows_by_cell.get(identity)
        cell_key = f"{identity[0]}:{identity[1]}:{identity[2]}"
        if row is None:
            cell_results[cell_key] = {"status": "failed", "requests": 0}
            continue
        try:
            by_job, paths = joined_cell_work(
                resolved_root,
                row,
                work_cost=work_cost,
                require_estimate_upper_bound=False,
            )
            joined = [item for job in by_job for item in job]
            input_paths.extend(paths)
            if len(joined) != expected_requests_per_cell:
                raise ValueError(
                    f"{cell_key}: {len(joined)} requests != "
                    f"{expected_requests_per_cell}"
                )
            all_joined.extend(joined)
            estimate_overruns = [
                item.actual_work - item.estimated_work
                for item in joined
                if item.actual_work > item.estimated_work
            ]
            if estimate_overruns:
                errors.append(
                    f"{cell_key}: {len(estimate_overruns)} actual-work "
                    "estimate overruns"
                )
            cell_results[cell_key] = {
                "status": "failed" if estimate_overruns else "passed",
                "requests": len(joined),
                "actual_work": sum(item.actual_work for item in joined),
                "estimated_work": sum(item.estimated_work for item in joined),
                "estimate_overrun_events": len(estimate_overruns),
                "estimate_overrun_max_work": max(
                    estimate_overruns,
                    default=0,
                ),
            }
        except (OSError, TypeError, ValueError) as exc:
            errors.append(str(exc))
            cell_results[cell_key] = {"status": "failed", "requests": 0}

    for key, values in (
        ("submission_id", [item.submission_id for item in all_joined]),
        ("request_id", [item.request_id for item in all_joined]),
        (
            "logical_request",
            [
                (item.scenario_id, item.job_id, item.doc_id)
                for item in all_joined
            ],
        ),
    ):
        if len(values) != len(set(values)):
            errors.append(f"matrix {key} values are not globally unique")

    overheads = Counter(item.prompt_overhead_tokens for item in all_joined)
    token_sources = Counter(item.token_count_source for item in all_joined)
    input_sources = Counter(item.input_token_count_source for item in all_joined)
    output_sources = Counter(item.output_token_count_source for item in all_joined)
    unique_paths = sorted(set(input_paths))
    input_files = [
        {
            "path": str(path.relative_to(resolved_root)),
            "sha256": sha256_file(path),
        }
        for path in unique_paths
    ]
    input_files_manifest_sha256 = hashlib.sha256(
        json.dumps(
            input_files,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    expected_requests = expected_requests_per_cell * len(expected_cells)
    if len(all_joined) != expected_requests:
        errors.append(
            f"audited request count {len(all_joined)} != {expected_requests}"
        )
    return {
        "schema_version": 2,
        "status": "passed" if all_joined and not errors else "failed",
        "matrix_root": str(resolved_root),
        "completion_protocol": work_cost.protocol,
        "message_shape": "single_user_message_no_system",
        "prompt_token_overhead_per_request": (
            work_cost.prompt_token_overhead_per_request
        ),
        "expected_cells": len(expected_cells),
        "observed_cells": len(rows_by_cell),
        "expected_requests": expected_requests,
        "audited_requests": len(all_joined),
        "overhead_distribution": {
            str(value): count for value, count in sorted(overheads.items())
        },
        "token_count_sources": dict(sorted(token_sources.items())),
        "input_token_count_sources": dict(sorted(input_sources.items())),
        "output_token_count_sources": dict(sorted(output_sources.items())),
        "cell_results": cell_results,
        "input_files_manifest_sha256": input_files_manifest_sha256,
        "input_files": input_files,
        "joined_work_schema": list(asdict(all_joined[0])) if all_joined else [],
        "errors": errors,
    }
