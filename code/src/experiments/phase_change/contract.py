"""Fail-closed validation for a prepared phase-change workload contract."""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

from src.baselines.common.manifests import read_manifest


DERIVED_NOTE = (
    "project-derived phase-change workload; not official VTC reproduction"
)


def _read_json(path: Path) -> dict[str, object]:
    decoded = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(decoded, dict):
        raise ValueError(f"contract file must contain an object: {path}")
    return decoded


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_contract(contract_dir: Path) -> dict[str, object]:
    """Load an imported immutable contract and validate every runner input."""
    root = contract_dir.resolve()
    audit_path = root / "audit.json"
    receipt_path = root / "database_import_receipt.json"
    if not audit_path.is_file() or not receipt_path.is_file():
        raise ValueError("phase-change run requires audit and import receipt")
    audit = _read_json(audit_path)
    receipt = _read_json(receipt_path)
    if audit.get("status") != "prepared" or audit.get("label") != DERIVED_NOTE:
        raise ValueError("phase-change audit identity/status is invalid")
    if audit.get("arrival_time_scale") != 1.0:
        raise ValueError("phase-change contract must retain arrival_time_scale=1")
    if int(audit.get("endpoint_count", 0)) != 2:
        raise ValueError("phase-change contract requires exactly two endpoints")
    target_workload = audit.get("target_workload")
    if not isinstance(target_workload, str) or not target_workload:
        raise ValueError("phase-change target workload is missing")
    counts = audit.get("job_row_counts")
    offsets = audit.get("job_first_arrival_s")
    spec = audit.get("spec")
    segments = audit.get("phase_segments")
    manifests = audit.get("manifests")
    if (
        not isinstance(counts, list)
        or len(counts) != 2
        or any(int(value) <= 0 for value in counts)
        or not isinstance(offsets, list)
        or len(offsets) != 2
        or any(
            not math.isfinite(float(value)) or float(value) < 0
            for value in offsets
        )
        or not isinstance(spec, dict)
        or not isinstance(segments, list)
        or len(segments) != 4
        or not isinstance(manifests, list)
        or len(manifests) != 2
    ):
        raise ValueError("phase-change audit shape is invalid")
    if (
        float(spec.get("duration_s", -1)) != 240.0
        or float(spec.get("period_s", -1)) != 60.0
    ):
        raise ValueError("phase-change runner requires the frozen 240s/60s clock")
    expected_start = 0.0
    expected_active = False
    for segment in segments:
        if not isinstance(segment, dict):
            raise ValueError("phase-change segment must be an object")
        start = float(segment.get("start_s", -1))
        end = float(segment.get("end_s", -1))
        active = segment.get("job_b_active")
        if (
            not math.isfinite(start)
            or not math.isfinite(end)
            or abs(start - expected_start) > 1e-9
            or end <= start
            or active is not expected_active
        ):
            raise ValueError("phase-change segments must be contiguous OFF-first")
        expected_start = end
        expected_active = not expected_active
    if abs(expected_start - float(spec.get("duration_s", -1))) > 1e-9:
        raise ValueError("phase-change segments do not cover the duration")
    if int(spec.get("output_cap", 0)) <= 0:
        raise ValueError("phase-change output cap is invalid")
    if (
        receipt.get("status") != "imported"
        or receipt.get("target_workload") != target_workload
        or int(receipt.get("inserted_rows", -1)) != sum(int(value) for value in counts)
        or int(receipt.get("distinct_doc_ids", -1)) != sum(int(value) for value in counts)
        or receipt.get("doc_id_range") != audit.get("doc_id_range")
        or int(receipt.get("output_cap", -1)) != int(spec["output_cap"])
    ):
        raise ValueError("phase-change import receipt does not match audit")

    for index, (expected_count, metadata) in enumerate(zip(counts, manifests)):
        path = root / f"client_{index}.jsonl"
        if not path.is_file() or not isinstance(metadata, dict):
            raise ValueError(f"phase-change manifest is missing: {path}")
        requests = read_manifest(path)
        if (
            len(requests) != int(expected_count)
            or int(metadata.get("row_count", -1)) != len(requests)
            or metadata.get("sha256") != _file_sha256(path)
            or any(
                request.max_output_tokens != int(spec["output_cap"])
                or request.estimated_output_tokens != int(spec["output_cap"])
                or request.endpoint_index not in {0, 1}
                for request in requests
            )
        ):
            raise ValueError(f"phase-change manifest {index} failed validation")
        first_arrival_s = min(request.arrival_time_s for request in requests)
        if abs(first_arrival_s - float(offsets[index])) > 1e-9:
            raise ValueError(
                f"phase-change manifest {index} first-arrival offset is invalid"
            )
        if index == 1:
            active_windows = [
                (float(item["start_s"]), float(item["end_s"]))
                for item in segments
                if item["job_b_active"] is True
            ]
            if any(
                not any(start < request.arrival_time_s < end for start, end in active_windows)
                for request in requests
            ):
                raise ValueError("Job B contains arrivals outside active phases")
    return audit


def runner_environment(audit: dict[str, object], contract_dir: Path) -> dict[str, str]:
    """Resolve config variables without changing the workload's phase clock."""
    counts = audit["job_row_counts"]
    offsets = audit["job_first_arrival_s"]
    spec = audit["spec"]
    assert (
        isinstance(counts, list)
        and isinstance(offsets, list)
        and isinstance(spec, dict)
    )
    return {
        "PHASE_CHANGE_WORKLOAD": str(audit["target_workload"]),
        "PHASE_CHANGE_CLIENT0_ROWS": str(int(counts[0])),
        "PHASE_CHANGE_CLIENT1_ROWS": str(int(counts[1])),
        # Each profiler process normalizes replay to its manifest's first
        # arrival. Restore the shared phase clock at the group runner.
        "PHASE_CHANGE_CLIENT0_OFFSET_S": str(float(offsets[0])),
        "PHASE_CHANGE_CLIENT1_OFFSET_S": str(float(offsets[1])),
        "PHASE_CHANGE_CLIENT0_MANIFEST": str(
            (contract_dir / "client_0.jsonl").resolve()
        ),
        "PHASE_CHANGE_CLIENT1_MANIFEST": str(
            (contract_dir / "client_1.jsonl").resolve()
        ),
        "PHASE_CHANGE_OUTPUT_CAP": str(int(spec["output_cap"])),
    }
