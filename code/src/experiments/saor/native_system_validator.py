"""Cross-file fail-closed validators for matched-system evidence."""

from __future__ import annotations

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
