"""Compatibility artifact API and the existing workload-specific public summary.

Generic file/JSON operations live in infrastructure. Keep the two workload
schema decisions here and reuse the same generic function objects.
"""

from src.infrastructure.private_artifacts import (
    Path, content_digest, contextmanager, file_identity, hashlib, json, json_metric,
    math, new_private_directory, open_private_text, os, publish_private_json,
    require_outside_git, uuid, write_private_json,
)

def workload_summary(manifest: dict) -> dict:
    """Export only counts and hashes, never arbitrary workload fields or IDs."""
    if manifest.get("schema") == "squad_v11_pg_map_v1":
        counts = {key: len(manifest["splits"][key]) for key in ("tuning", "evaluation")}
    elif manifest.get("schema") == "semloom.sharegpt.first_human.v1":
        counts = {"selected": len(manifest["rows"])}
    else:
        raise ValueError("unsupported private workload schema")
    payload = {key: value for key, value in manifest.items() if key != "sha256"}
    if manifest.get("sha256") != content_digest(payload):
        raise ValueError("workload identity mismatch")
    return {
        "schema": "semloom.workload.public_summary.v1",
        "private_manifest_sha256": content_digest(payload),
        "row_counts": counts,
    }
