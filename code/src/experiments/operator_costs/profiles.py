"""Historical profile CSV input, identity and pre-execution feature conversion."""
from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

import numpy as np


FEATURE_NAMES = (
    "total_rows",
    "prompt_token_count",
    "completion_max_tokens",
    "token_budget",
    "packing_batch_count",
    "batch_estimated_cost_p50",
    "batch_estimated_cost_p95",
    "batch_estimated_cost_max",
    "max_inflight_limit",
    "flush_timeout_ms",
    "flush_max_wait_ms",
    "arrival_time_scale",
    "arrival_replay_enabled",
    "flush_is_adaptive",
    "flush_is_immediate",
    "max_active_work_per_endpoint",
    "per_endpoint_inflight_limit",
    "actor_workers_per_endpoint",
    "ray_actor_max_concurrency",
    "endpoint_count",
    "service_quantum_tokens",
    "gpu_peak_tflops_per_gpu",
    "gpu_memory_total_mib_per_gpu",
)


DECISION_CONTEXT_FIELDS = (
    "model_name",
    "cost_model_id",
    "source_workload_name",
    "total_rows",
    "completion_max_tokens",
    "arrival_replay",
    "arrival_time_scale",
    "server_version",
    "pgvector_version",
    "model_backend",
    "completion_protocol",
    "completion_http_transport",
    "service_prefix_caching",
)

CANDIDATE_FIELDS = (
    "batching_policy",
    "output_cost_mode",
    "token_budget",
    "max_inflight_limit",
    "max_active_work_per_endpoint",
    "actor_workers_per_endpoint",
    "ray_actor_max_concurrency",
    "endpoint_count",
    "admission_scope",
    "per_endpoint_inflight_limit",
    "service_quantum_tokens",
    "submission_granularity",
    "flush_policy",
    "flush_timeout_ms",
    "flush_max_wait_ms",
)


def feature_vector(row: dict[str, str]) -> list[float]:
    flush_policy = row.get("flush_policy", "")
    return [
        _number(row, "total_rows"),
        _number(row, "token_count"),
        _number(row, "completion_max_tokens"),
        _number(row, "token_budget"),
        _number(row, "packing_batch_count"),
        _number(row, "batch_estimated_cost_units_p50"),
        _number(row, "batch_estimated_cost_units_p95"),
        _number(row, "batch_estimated_cost_units_max"),
        _number(row, "max_inflight_limit"),
        _number(row, "flush_timeout_ms"),
        _number(row, "flush_max_wait_ms"),
        _number(row, "arrival_time_scale"),
        float(_boolean(row.get("arrival_replay", ""))),
        float(flush_policy == "queue_adaptive"),
        float(flush_policy == "immediate"),
        _number_or_default(row, "max_active_work_per_endpoint"),
        _number_or_default(row, "per_endpoint_inflight_limit"),
        _number_or_default(row, "actor_workers_per_endpoint"),
        _number_or_default(row, "ray_actor_max_concurrency"),
        float(_endpoint_count(row)),
        _number_or_default(row, "service_quantum_tokens"),
        _number_or_default(row, "gpu_peak_tflops"),
        _per_endpoint_number(row, "gpu_memory_total_mib"),
    ]


def scenario_group(row: dict[str, str]) -> str:
    signature = json.dumps(
        {
            "context": decision_context_payload(row),
            "candidate": candidate_payload(row),
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    return hashlib.sha256(signature.encode("utf-8")).hexdigest()[:16]


def _payload_signature(payload: dict[str, str]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:16]


def decision_context_payload(row: dict[str, str]) -> dict[str, str]:
    """Identity for one comparable plan-selection context.

    GPU model/memory and serving protocol are part of the context so profiles from
    different physical environments cannot be merged into one plan-selection fold.
    Endpoint count remains a candidate field: choosing one or more endpoints can be
    an optimizer decision on a fixed host.
    """

    payload = {field: row.get(field, "") for field in DECISION_CONTEXT_FIELDS}
    payload["gpu_model"] = _normalized_gpu_model(row.get("gpu_name", ""))
    payload["gpu_memory_total_mib_per_gpu"] = str(
        _per_endpoint_number(row, "gpu_memory_total_mib")
    )
    return payload


def candidate_payload(row: dict[str, str]) -> dict[str, str]:
    payload = {field: row.get(field, "") for field in CANDIDATE_FIELDS}
    payload["endpoint_count"] = str(_endpoint_count(row))
    return payload


def is_formal_profile_row(row: dict[str, str]) -> bool:
    """Accept only measured formal repeats for estimator fitting/evaluation.

    Warm-up runs exercise caches and compilation and are intentionally excluded.
    Missing phase metadata fails closed instead of being silently treated as formal.
    """

    return row.get("phase", "").strip().lower() == "formal"


def _number(row: dict[str, str], field: str) -> float:
    value = row.get(field, "")
    if value in ("", None):
        raise ValueError(f"missing numeric field {field}")
    parsed = float(value)
    if not np.isfinite(parsed):
        raise ValueError(f"non-finite numeric field {field}")
    return parsed


def _number_or_default(
    row: dict[str, str],
    field: str,
    default: float = 0.0,
) -> float:
    value = row.get(field, "")
    if value in ("", None):
        return default
    parsed = float(value)
    if not np.isfinite(parsed):
        raise ValueError(f"non-finite numeric field {field}")
    return parsed


def _endpoint_count(row: dict[str, str]) -> int:
    explicit = _number_or_default(row, "endpoint_count")
    if explicit > 0 and explicit.is_integer():
        return int(explicit)
    identifiers = str(row.get("endpoint_gpu_ids", "")).replace(",", ";")
    values = [value.strip() for value in identifiers.split(";") if value.strip()]
    return len(values) if values else 1


def _per_endpoint_number(row: dict[str, str], field: str) -> float:
    raw = str(row.get(field, "") or "").strip()
    if not raw:
        return 0.0
    parts = [value.strip() for value in raw.split(";") if value.strip()]
    values = [float(value) for value in parts]
    if not values or not all(np.isfinite(value) for value in values):
        raise ValueError(f"invalid per-endpoint numeric field {field}")
    if len(values) > 1:
        return sum(values) / len(values)
    return values[0] / _endpoint_count(row)


def _normalized_gpu_model(value: str) -> str:
    models = [item.strip() for item in str(value).split(";") if item.strip()]
    return ";".join(sorted(set(models)))


def _boolean(value: str) -> bool:
    normalized = str(value).strip().lower()
    if normalized in {"true", "1", "yes"}:
        return True
    if normalized in {"false", "0", "no", ""}:
        return False
    raise ValueError(f"invalid boolean value: {value}")


def reference_csv_paths(reference: Path, repository: Path) -> list[Path]:
    payload = json.loads(reference.read_text(encoding="utf-8"))
    return [repository / path.replace("\\", "/") for path in payload["source_csvs"]]


def load_profile_rows(paths: list[Path], target: str) -> tuple[list[dict[str, str]], int]:
    rows = []
    excluded = 0
    for path in paths:
        with path.open(encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                if not is_formal_profile_row(row) or row.get("status") != "ok":
                    excluded += 1
                    continue
                try:
                    feature_vector(row)
                    target_value = _number(row, target)
                except (KeyError, TypeError, ValueError):
                    excluded += 1
                    continue
                if target_value < 0:
                    excluded += 1
                    continue
                rows.append(row)
    return rows, excluded


def build_profile_matrix(rows: list[dict[str, str]], target: str):
    features = np.asarray([feature_vector(r) for r in rows], dtype=float)
    targets = np.asarray([_number(r, target) for r in rows], dtype=float)
    groups = [scenario_group(r) for r in rows]
    contexts = [
        _payload_signature(decision_context_payload(r)) for r in rows
    ]
    candidates = [
        _payload_signature(candidate_payload(r)) for r in rows
    ]
    return features, targets, groups, contexts, candidates
