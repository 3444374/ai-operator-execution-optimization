"""Fail-closed validity checks shared by official baseline gate cells."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Iterable, Mapping

from .contracts import BaselineRequestResult, ChatRequest


@dataclass(frozen=True)
class GateReport:
    passed: bool
    incidents: tuple[str, ...]
    metrics: dict[str, object]


def validate_gate(
    *,
    manifest: Iterable[ChatRequest],
    summaries: Iterable[Mapping[str, object]],
    request_results: Iterable[BaselineRequestResult],
    max_endpoint_work_skew: float = 0.02,
) -> GateReport:
    """Report every hard incident without deleting or retrying evidence."""

    requests = tuple(manifest)
    summary_rows = tuple(summaries)
    results = tuple(request_results)
    incidents: list[str] = []

    endpoint_work: dict[int, int] = defaultdict(int)
    for request in requests:
        endpoint_work[request.endpoint_index] += request.estimated_work
    work_values = list(endpoint_work.values())
    work_skew = (
        (max(work_values) - min(work_values)) / max(work_values)
        if work_values and max(work_values) > 0
        else 0.0
    )
    if work_skew > max_endpoint_work_skew:
        incidents.append("endpoint_work_skew")

    expected_ids = [request.doc_id for request in requests]
    observed_ids = [result.doc_id for result in results]
    if (
        len(observed_ids) != len(expected_ids)
        or len(set(observed_ids)) != len(observed_ids)
        or set(observed_ids) != set(expected_ids)
    ):
        incidents.append("exactly_once")
    if any(
        result.status != "completed" or result.error
        for result in results
    ):
        incidents.append("failed_requests")

    expected_endpoints = set(endpoint_work)
    completed_endpoints = {
        result.endpoint_index
        for result in results
        if result.status == "completed" and not result.error
    }
    if completed_endpoints != expected_endpoints:
        incidents.append("unused_endpoint")

    metadata_fields = (
        "model_name",
        "completion_protocol",
        "service_config_sha256",
    )
    if not summary_rows or any(
        len({row.get(field) for row in summary_rows}) != 1
        for field in metadata_fields
    ):
        incidents.append("metadata_mismatch")

    provenance_fields = (
        "comparison_role",
        "implementation_provenance",
        "scheduler_owner",
        "custom_scheduling_code",
        "formal_baseline_eligible",
        "upstream_source",
        "qualification_gate",
    )
    if not summary_rows or any(
        field not in row
        for row in summary_rows
        for field in provenance_fields
    ):
        incidents.append("provenance_missing")
    elif any(
        bool(row["formal_baseline_eligible"])
        and bool(row["custom_scheduling_code"])
        for row in summary_rows
    ):
        incidents.append("invalid_native_provenance")
    elif any(
        len({row[field] for row in summary_rows}) != 1
        for field in provenance_fields
    ):
        incidents.append("provenance_mismatch")

    summary_by_endpoint = {
        int(row["endpoint_index"]): row
        for row in summary_rows
        if "endpoint_index" in row
    }
    if set(summary_by_endpoint) != expected_endpoints or any(
        int(summary_by_endpoint[endpoint].get("predicted_work", -1))
        != predicted_work
        for endpoint, predicted_work in endpoint_work.items()
        if endpoint in summary_by_endpoint
    ):
        incidents.append("predicted_work_mismatch")

    if any(
        int(row.get("vllm_num_requests_running_final", -1)) != 0
        or int(row.get("vllm_num_requests_waiting_final", -1)) != 0
        for row in summary_rows
    ):
        incidents.append("nonempty_final_queue")
    if any(
        int(row.get("worker_failures", -1)) != 0
        for row in summary_rows
    ):
        incidents.append("worker_failure")

    unique_incidents = tuple(dict.fromkeys(incidents))
    return GateReport(
        passed=not unique_incidents,
        incidents=unique_incidents,
        metrics={
            "manifest_rows": len(requests),
            "result_rows": len(results),
            "endpoint_work": dict(sorted(endpoint_work.items())),
            "endpoint_work_skew": work_skew,
            "result_endpoint_counts": dict(
                sorted(
                    Counter(
                        result.endpoint_index for result in results
                    ).items()
                )
            ),
        },
    )
