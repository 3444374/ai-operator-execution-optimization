"""Deep-validate completed SAOR matrix roots and their portable tar mirrors.

The validators consume files produced by the matrix runner, re-hash every
referenced artifact, and reject declarative ``status=passed`` records that are
not backed by a complete five-arm root.
"""

from __future__ import annotations

import hashlib
import json
import tarfile
from pathlib import Path, PurePosixPath
from typing import Mapping

from src.experiments.saor.native_system_contract import MatchedSystemConfig


def _sha256(path: Path) -> str:
    """Hash one artifact in bounded-memory chunks."""

    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path, label: str) -> dict[str, object]:
    """Load one required JSON object and give failures a stable label."""

    try:
        decoded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"{label} is unreadable") from exc
    if not isinstance(decoded, dict):
        raise RuntimeError(f"{label} must be a JSON object")
    return decoded


def _root_file(root: Path, relative: object, label: str) -> Path:
    """Resolve a portable relative path without allowing root escape."""

    if not isinstance(relative, str) or not relative:
        raise RuntimeError(f"{label} path is missing")
    pure = PurePosixPath(relative)
    if pure.is_absolute() or ".." in pure.parts:
        raise RuntimeError(f"{label} path escapes the matrix root")
    candidate = (root / Path(*pure.parts)).resolve()
    if root != candidate and root not in candidate.parents:
        raise RuntimeError(f"{label} path escapes the matrix root")
    if not candidate.is_file():
        raise RuntimeError(f"{label} artifact is missing")
    return candidate


def _validate_contract_snapshot(
    root: Path,
    index: Mapping[str, object],
    expected_resolved_sha256: str,
) -> None:
    """Bind the index to the exact runner snapshot and resolved config."""

    snapshot_path = root / "matrix_contract_snapshot.json"
    if _sha256(snapshot_path) != index.get("contract_snapshot_sha256"):
        raise RuntimeError("matrix contract snapshot SHA drifted")
    snapshot = _load_json(snapshot_path, "matrix contract snapshot")
    if snapshot.get("schema_version") != 1 or set(snapshot) != {
        "schema_version", "runtime_identity", "resolved_config"
    }:
        raise RuntimeError("matrix contract snapshot schema is invalid")
    encoded = json.dumps(
        snapshot["resolved_config"], ensure_ascii=False, sort_keys=True,
        separators=(",", ":")
    ).encode("utf-8")
    if hashlib.sha256(encoded).hexdigest() != expected_resolved_sha256:
        raise RuntimeError("matrix contract snapshot resolved config drifted")
    runtime = snapshot.get("runtime_identity")
    if not isinstance(runtime, dict):
        raise RuntimeError("matrix contract snapshot runtime identity is missing")
    for field in (
        "repository_commit", "config_sha256", "resolved_config_sha256",
        "matrix_instance_id", "execution_mode", "authorization_sha256",
        "service_identity_preflight",
    ):
        index_field = "config_fingerprint" if field == "resolved_config_sha256" else field
        if runtime.get(field) != index.get(index_field):
            raise RuntimeError(f"matrix contract snapshot {field} drifted")


def _validate_sealed_manifests(
    root: Path,
    index: Mapping[str, object],
    config: MatchedSystemConfig,
) -> None:
    """Re-hash the combined and per-Job manifests copied into the root."""

    manifest = _root_file(
        root, index.get("manifest_evidence_path"), "sealed combined manifest"
    )
    if _sha256(manifest) != config.arms[0].manifest_sha256:
        raise RuntimeError("sealed combined manifest SHA drifted")
    observed_jobs = index.get("job_manifest_evidence")
    expected_jobs = config.arms[0].job_manifests
    if not isinstance(observed_jobs, list) or len(observed_jobs) != len(expected_jobs):
        raise RuntimeError("sealed Job manifest evidence is incomplete")
    for observed, expected in zip(observed_jobs, expected_jobs, strict=True):
        if not isinstance(observed, dict) or any(
            observed.get(field) != value
            for field, value in {
                "job_id": expected.job_id,
                "rows": expected.rows,
                "sha256": expected.sha256,
            }.items()
        ):
            raise RuntimeError("sealed Job manifest identity drifted")
        path = _root_file(
            root, observed.get("evidence_path"), f"sealed {expected.job_id} manifest"
        )
        if _sha256(path) != expected.sha256:
            raise RuntimeError(f"sealed {expected.job_id} manifest SHA drifted")


def _validate_cell_artifacts(root: Path, cell: Mapping[str, object]) -> None:
    """Require every persisted cell locator to carry a matching SHA-256."""

    identities = cell.get("artifact_identities")
    output_paths = cell.get("output_paths")
    if not isinstance(identities, dict) or not identities:
        raise RuntimeError("matrix cell artifact identities are missing")
    if not isinstance(output_paths, dict) or set(identities) != set(output_paths):
        raise RuntimeError("matrix cell artifact identities are incomplete")
    for name, identity in identities.items():
        if not isinstance(identity, dict) or set(identity) != {"path", "sha256"}:
            raise RuntimeError("matrix cell artifact identity schema is invalid")
        if identity.get("path") != output_paths.get(name):
            raise RuntimeError("matrix cell artifact path drifted")
        path = _root_file(root, identity["path"], f"matrix cell {name}")
        if _sha256(path) != identity.get("sha256"):
            raise RuntimeError("matrix cell artifact SHA drifted")
    cell_root_value = cell.get("cell_artifact_root")
    cell_root_marker = _root_file(
        root,
        next(iter(identities.values()))["path"],
        "matrix cell root marker",
    )
    if not isinstance(cell_root_value, str) or not cell_root_value:
        raise RuntimeError("matrix cell raw artifact root is missing")
    cell_root = (root / cell_root_value).resolve()
    if root != cell_root and root not in cell_root.parents:
        raise RuntimeError("matrix cell raw artifact root escapes the matrix root")
    if cell_root not in cell_root_marker.parents:
        raise RuntimeError("matrix cell output artifact is outside its cell root")
    raw_manifest = cell.get("raw_artifact_manifest")
    if not isinstance(raw_manifest, dict) or not raw_manifest:
        raise RuntimeError("matrix cell raw artifact manifest is missing")
    raw_paths = list(cell_root.rglob("*"))
    if any(path.is_symlink() for path in raw_paths):
        raise RuntimeError("matrix cell raw artifact tree contains a symlink")
    actual = {
        path.relative_to(root).as_posix(): _sha256(path)
        for path in raw_paths if path.is_file()
    }
    if actual != raw_manifest:
        raise RuntimeError("matrix cell raw artifact manifest drifted")


def _validate_cell(
    root: Path,
    cell: Mapping[str, object],
    arm_by_id: Mapping[str, object],
    expected: Mapping[str, object],
    expected_native_provenance: Mapping[str, Mapping[str, object]] | None,
    expected_endpoint_urls: tuple[str, ...],
) -> None:
    """Re-check completion, provenance, and immutable cell identity."""

    arm_id = cell.get("arm_id")
    arm = arm_by_id.get(str(arm_id))
    if arm is None:
        raise RuntimeError("matrix cell arm identity is unknown")
    for field, value in {
        "status": "passed",
        "phase": "warmup",
        "repeat": 1,
        "repository_commit": expected["repository_commit"],
        "config_sha256": expected["config_sha256"],
        "config_fingerprint": expected["resolved_config_sha256"],
        "exactly_once": True,
    }.items():
        if cell.get(field) != value:
            raise RuntimeError(f"matrix cell {field} correctness gate failed")
    for field in ("native_config_sha256", "project_config_sha256"):
        if field in expected and cell.get(field) != expected[field]:
            raise RuntimeError(f"matrix cell {field} correctness gate failed")
    jobs = cell.get("jobs")
    if not isinstance(jobs, list) or len(jobs) != len(arm.job_manifests):
        raise RuntimeError("matrix cell Job evidence is incomplete")
    for observed, job in zip(jobs, arm.job_manifests, strict=True):
        if not isinstance(observed, dict) or any(
            observed.get(field) != value
            for field, value in {
                "job_id": job.job_id,
                "manifest_sha256": job.sha256,
                "expected_count": job.rows,
                "completed_count": job.rows,
                "exactly_once": True,
            }.items()
        ):
            raise RuntimeError("matrix cell Job identity or row count drifted")
        timeline = [
            observed.get(field)
            for field in (
                "t0_job_release_epoch_s", "t1_first_batch_epoch_s",
                "t2_first_request_epoch_s", "t3_last_request_completion_epoch_s",
                "t4_result_visible_epoch_s",
            )
        ]
        if any(
            isinstance(value, bool) or not isinstance(value, (int, float))
            for value in timeline
        ) or timeline != sorted(timeline):
            raise RuntimeError("matrix cell T0-T4 Job timeline drifted")
    system_observation = cell.get("system_observation")
    gateway = cell.get("observation_gateway")
    if (
        not isinstance(system_observation, dict)
        or system_observation.get("status") != "passed"
        or system_observation.get("timed_boundary")
        != "job_release_before_postgres_to_validated_result_visibility"
        or not isinstance(gateway, dict)
        or gateway.get("status") != "passed"
        or gateway.get("mode") != "pass_through_no_queue_no_retry"
    ):
        raise RuntimeError("matrix cell observation contract failed")
    expected_routes = {
        (job.job_id, endpoint_id, expected_endpoint_urls[index])
        for job in arm.job_manifests
        for index, endpoint_id in enumerate(arm.endpoint_ids)
    }
    routes = gateway.get("routes")
    observed_routes = {
        (
            str(route.get("job_id", "")),
            str(route.get("endpoint_id", "")),
            str(route.get("upstream_url", "")),
        )
        for route in routes
        if isinstance(route, dict)
    } if isinstance(routes, list) else set()
    if observed_routes != expected_routes:
        raise RuntimeError("matrix cell observation gateway route drifted")
    completion = cell.get("completion_evidence")
    expected_rows = sum(job.rows for job in arm.job_manifests)
    if (
        not isinstance(completion, dict)
        or completion.get("status") != "passed"
        or completion.get("mode") != "completion_trace_digest"
        or completion.get("exactly_once") is not True
        or completion.get("expected_rows") != expected_rows
        or completion.get("observed_rows") != expected_rows
        or not completion.get("expected_doc_id_digest")
        or completion.get("observed_doc_id_digest")
        != completion.get("expected_doc_id_digest")
        or not completion.get("output_digest")
    ):
        raise RuntimeError("matrix cell completion evidence failed")
    if arm.kind == "native":
        provenance = cell.get("native_implementation_provenance")
        if not isinstance(provenance, dict) or set(provenance) != {
            "upstream_url", "upstream_version", "upstream_commit",
            "adapter_path", "adapter_sha256", "upstream_source_modified",
            "adapter_diff_status",
        } or provenance.get("upstream_source_modified") is not False or (
            provenance.get("adapter_diff_status")
            != "thin_adapter_only_no_upstream_patch"
        ):
            raise RuntimeError("matrix cell native provenance is incomplete")
        if (
            expected_native_provenance is not None
            and provenance != dict(expected_native_provenance.get(str(arm_id), {}))
        ):
            raise RuntimeError("matrix cell native provenance drifted from frozen config")
    _validate_cell_artifacts(root, cell)
    identities = cell["artifact_identities"]
    gateway_identity = identities.get("observation_gateway_trace")
    if (
        not isinstance(gateway_identity, dict)
        or gateway.get("trace_path") != gateway_identity.get("path")
        or gateway.get("trace_sha256") != gateway_identity.get("sha256")
    ):
        raise RuntimeError("matrix cell observation gateway trace identity drifted")


def validate_completed_matrix_root(
    root: Path,
    config: MatchedSystemConfig,
    expected: Mapping[str, object],
    *,
    execution_mode: str,
    expected_readiness_binding: Mapping[str, object] | None = None,
    expected_system_preflight_sha256: str | None = None,
    expected_native_provenance: Mapping[str, Mapping[str, object]] | None = None,
) -> dict[str, object]:
    """Validate one actual five-arm root and return its parsed matrix index."""

    root = root.resolve()
    index = _load_json(root / "matrix_index.json", "matrix index")
    for field, value in {
        "schema_version": 1,
        "status": "completed",
        "execution_mode": execution_mode,
        "repository_commit": expected["repository_commit"],
        "config_sha256": expected["config_sha256"],
        "config_fingerprint": expected["resolved_config_sha256"],
        "manifest_sha256": expected["manifest_sha256"],
    }.items():
        if index.get(field) != value:
            raise RuntimeError(f"matrix index {field} drifted")
    if execution_mode != "formal" and index.get("authorization_sha256") not in (None, ""):
        raise RuntimeError("non-formal matrix carries formal authorization")
    readiness = index.get("service_identity_preflight")
    if not isinstance(readiness, dict):
        raise RuntimeError("matrix readiness evidence is missing")
    readiness_binding = readiness.get("binding")
    if not isinstance(readiness_binding, dict) or any(
        readiness_binding.get(field) != expected[field]
        for field in (
            "repository_commit", "config_sha256", "resolved_config_sha256"
        )
    ):
        raise RuntimeError("matrix readiness core binding drifted")
    for field in ("native_config_sha256", "project_config_sha256"):
        if field in expected:
            if index.get(field) != expected[field]:
                raise RuntimeError(f"matrix index {field} drifted")
            if readiness_binding.get(field) != expected[field]:
                raise RuntimeError(f"matrix readiness {field} drifted")
    if expected_native_provenance is not None and (
        index.get("native_implementation_provenance")
        != {key: dict(value) for key, value in expected_native_provenance.items()}
    ):
        raise RuntimeError("matrix native provenance index drifted")
    if expected_readiness_binding is not None and (
        readiness_binding != dict(expected_readiness_binding)
    ):
        raise RuntimeError("matrix readiness binding drifted")
    if expected_system_preflight_sha256 is not None:
        system = readiness.get("system_preflight")
        if not isinstance(system, dict) or (
            system.get("evidence_sha256") != expected_system_preflight_sha256
        ):
            raise RuntimeError("matrix system preflight binding drifted")
    if execution_mode == "rehearsal" and readiness.get("rehearsal_ready") is not True:
        raise RuntimeError("rehearsal root was not admitted by all readiness stages")
    stages = readiness.get("stages")
    expected_stages = {
        "static_config": "passed",
        "service_identity": "passed",
        "system_preflight": "passed",
        "correctness_smoke": (
            "not_checked" if execution_mode == "correctness_smoke" else "passed"
        ),
    }
    expected_status = (
        "system_preflight_passed"
        if execution_mode == "correctness_smoke" else "rehearsal_ready"
    )
    if execution_mode in {"correctness_smoke", "rehearsal"} and (
        readiness.get("status") != expected_status
        or stages != expected_stages
    ):
        raise RuntimeError("matrix readiness stage evidence is incomplete")
    if execution_mode == "rehearsal":
        service = readiness.get("service_identity")
        if (
            not isinstance(service, dict)
            or not isinstance(service.get("installed_source"), dict)
            or service["installed_source"].get("status") != "passed"
            or not isinstance(service.get("live_service"), dict)
            or service["live_service"].get("status") != "passed"
            or not isinstance(readiness.get("system_preflight"), dict)
            or readiness["system_preflight"].get("status") != "passed"
            or not isinstance(readiness.get("correctness_smoke"), dict)
            or readiness["correctness_smoke"].get("status") != "passed"
        ):
            raise RuntimeError("rehearsal root lacks deep readiness evidence")
    _validate_contract_snapshot(root, index, str(expected["resolved_config_sha256"]))
    _validate_sealed_manifests(root, index, config)
    schedule = index.get("schedule")
    cells = index.get("cells")
    expected_arms = {arm.arm_id for arm in config.arms}
    if (
        not isinstance(schedule, list)
        or len(schedule) != len(expected_arms)
        or {item.get("arm_id") for item in schedule if isinstance(item, dict)}
        != expected_arms
        or any(
            not isinstance(item, dict)
            or item.get("phase") != "warmup"
            or item.get("repeat") != 1
            for item in schedule
        )
    ):
        raise RuntimeError("matrix schedule is not one complete five-arm pass")
    if (
        not isinstance(cells, list)
        or len(cells) != len(expected_arms)
        or {item.get("arm_id") for item in cells if isinstance(item, dict)}
        != expected_arms
    ):
        raise RuntimeError("matrix cells are not one complete five-arm set")
    arm_by_id = {arm.arm_id: arm for arm in config.arms}
    for cell in cells:
        if not isinstance(cell, dict):
            raise RuntimeError("matrix cell is not an object")
        _validate_cell(
            root, cell, arm_by_id, expected, expected_native_provenance,
            config.endpoint_urls,
        )
    database_ids = {
        (cell.get("server_version"), cell.get("pgvector_version"))
        for cell in cells if isinstance(cell, dict)
    }
    if len(database_ids) != 1 or any(not value for value in next(iter(database_ids))):
        raise RuntimeError("matrix cells do not share one PostgreSQL identity")
    system = readiness.get("system_preflight")
    checks = system.get("checks") if isinstance(system, dict) else None
    postgresql = checks.get("postgresql") if isinstance(checks, dict) else None
    if isinstance(postgresql, dict) and next(iter(database_ids)) != (
        postgresql.get("server_version"), postgresql.get("pgvector_version")
    ):
        raise RuntimeError("matrix PostgreSQL identity drifted from system preflight")
    return index


def validate_archive_mirror(root: Path, archive_path: Path) -> None:
    """Require a tar archive to contain every root file with identical bytes."""

    root = root.resolve()
    expected = {
        path.relative_to(root).as_posix(): _sha256(path)
        for path in root.rglob("*") if path.is_file()
    }
    if not expected:
        raise RuntimeError("matrix root contains no files")
    observed: dict[str, str] = {}
    try:
        with tarfile.open(archive_path, mode="r:*") as archive:
            for member in archive.getmembers():
                if member.issym() or member.islnk():
                    raise RuntimeError("matrix archive contains a link")
                if not member.isfile():
                    continue
                pure = PurePosixPath(member.name)
                if pure.is_absolute() or ".." in pure.parts or len(pure.parts) < 2:
                    raise RuntimeError("matrix archive contains an unsafe path")
                relative = PurePosixPath(*pure.parts[1:]).as_posix()
                if relative in observed:
                    raise RuntimeError("matrix archive contains a duplicate file")
                stream = archive.extractfile(member)
                if stream is None:
                    raise RuntimeError("matrix archive file is unreadable")
                observed[relative] = hashlib.sha256(stream.read()).hexdigest()
    except (OSError, tarfile.TarError) as exc:
        raise RuntimeError("matrix archive is not a readable tar") from exc
    if observed != expected:
        raise RuntimeError("matrix archive is not an exact mirror of the root")
