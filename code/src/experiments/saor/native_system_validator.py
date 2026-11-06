"""Cross-file fail-closed validators for matched-system evidence."""

from __future__ import annotations

import json
import math
from typing import Iterable, Mapping


ROOT_IDENTITY_FIELDS = (
    "repository_commit",
    "matrix_instance_id",
    "config_sha256",
    "config_fingerprint",
    "authorization_sha256",
    "manifest_sha256",
    "service_signature",
)


def validate_uniform_cell_identity(
    cells: Iterable[Mapping[str, object]],
    expected_scheduler_owners: Mapping[str, str],
) -> None:
    """Reject mixed roots, manifests, service stacks, owners, or fingerprints."""

    rows = list(cells)
    if not rows:
        raise ValueError("matrix contains no cell evidence")
    for field in ROOT_IDENTITY_FIELDS:
        values = {_freeze(row.get(field)) for row in rows}
        if len(values) != 1 or None in values:
            raise ValueError(f"matrix cell {field} identity is mixed or missing")
    for row in rows:
        arm_id = str(row.get("arm_id", ""))
        expected = expected_scheduler_owners.get(arm_id)
        if expected is None or row.get("scheduler_owner") != expected:
            raise ValueError("matrix cell scheduler-owner identity drifted")


def _freeze(value: object) -> object:
    if isinstance(value, dict):
        return tuple(sorted((str(key), _freeze(item)) for key, item in value.items()))
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    return value


def _decode_json_container(value: object, run_id: str, field: str) -> object:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError as error:
            raise ValueError(f"{run_id} {field} is not valid JSON") from error
    return value


def _finite_number(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be numeric")
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError(f"{name} must be finite")
    return parsed


def validate_native_final_queue(
    value: object, run_id: str
) -> dict[str, dict[str, object]]:
    """Canonicalize and require an empty final queue from a native system."""

    decoded = _decode_json_container(value, run_id, "queue_final")
    if not isinstance(decoded, dict) or not decoded:
        raise ValueError(f"{run_id} native queue_final has an invalid schema")
    output: dict[str, dict[str, object]] = {}
    for endpoint, state in decoded.items():
        if not str(endpoint) or not isinstance(state, dict):
            raise ValueError(f"{run_id} native queue_final has an invalid schema")
        if not {"running", "waiting"}.issubset(state):
            raise ValueError(f"{run_id} native queue_final lacks live fields")
        if any(
            _finite_number(state[name], f"{run_id} queue_final {name}") != 0
            for name in ("running", "waiting")
        ):
            raise ValueError(f"{run_id} native final queue is not empty")
        output[str(endpoint)] = dict(state)
    return output


def validate_project_final_credit(
    value: object, run_id: str, *, frozen_static: bool
) -> list[dict[str, object]]:
    """Canonicalize Project credit snapshots and reject every live remainder."""

    decoded = _decode_json_container(value, run_id, "shared_credit_final")
    if frozen_static:
        if decoded != []:
            raise ValueError(f"{run_id} frozen-static shared_credit_final must be []")
        return []
    if not isinstance(decoded, list) or not decoded:
        raise ValueError(f"{run_id} shared_credit_final has an invalid schema")
    scalar_live = (
        "active_requests", "active_work", "waiting_requests", "waiting_work",
    )
    mapping_live = (
        "active_by_job", "active_work_by_job", "waiting_by_job",
        "waiting_work_by_job", "waiting_head_work_by_job",
    )
    required = {
        "endpoint_id", "request_limit", "work_limit", *scalar_live, *mapping_live,
    }
    output: list[dict[str, object]] = []
    endpoint_ids: set[str] = set()
    for raw_snapshot in decoded:
        if not isinstance(raw_snapshot, dict) or not required.issubset(raw_snapshot):
            raise ValueError(f"{run_id} shared_credit_final has an invalid schema")
        snapshot = dict(raw_snapshot)
        endpoint_id = str(snapshot["endpoint_id"])
        if not endpoint_id or endpoint_id in endpoint_ids:
            raise ValueError(f"{run_id} shared_credit_final has invalid endpoint IDs")
        endpoint_ids.add(endpoint_id)
        if any(
            _finite_number(snapshot[name], f"{run_id} shared credit {name}") <= 0
            for name in ("request_limit", "work_limit")
        ):
            raise ValueError(f"{run_id} shared credit limits must be positive")
        for name in mapping_live:
            child = _decode_json_container(
                snapshot[name], run_id, f"shared credit {name}"
            )
            if not isinstance(child, (list, dict)):
                raise ValueError(
                    f"{run_id} shared credit {name} must encode a container"
                )
            snapshot[name] = child
        if any(
            _finite_number(snapshot[name], f"{run_id} shared credit {name}") != 0
            for name in scalar_live
        ) or any(snapshot[name] not in ([], {}) for name in mapping_live):
            raise ValueError(f"{run_id} final shared credit is not empty")
        output.append(snapshot)
    return output
