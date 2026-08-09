"""Immutable PostgreSQL image-job manifest shared by every experiment arm."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass, replace
from pathlib import Path

from src.modalities.image.source import ImageSourceConfig, read_image_source_metadata


@dataclass(frozen=True)
class ImageJobManifestEntry:
    job_id: str
    workload_name: str
    limit: int
    offset: int
    multi_job_start_offset_s: float
    doc_ids_sha256: str
    input_encoded_bytes: int
    avg_encoded_bytes: float


@dataclass(frozen=True)
class ImageJobManifest:
    path: Path
    sha256: str
    jobs: tuple[ImageJobManifestEntry, ...]

    def select(self, job_ids: tuple[str, ...]) -> tuple[ImageJobManifestEntry, ...]:
        mapping = {item.job_id: item for item in self.jobs}
        if len(set(job_ids)) != len(job_ids) or any(item not in mapping for item in job_ids):
            raise ValueError("job_ids must be unique members of the immutable manifest")
        selected = tuple(mapping[item] for item in job_ids)
        if len(selected) == 1:
            return (replace(selected[0], multi_job_start_offset_s=0.0),)
        return selected


def doc_ids_sha256(doc_ids: frozenset[str]) -> str:
    payload = "\n".join(sorted(doc_ids)).encode("utf-8") + b"\n"
    return hashlib.sha256(payload).hexdigest()


def load_image_job_manifest(path: str | Path) -> ImageJobManifest:
    resolved = Path(path)
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or set(payload) != {
        "schema_version", "status", "selection", "jobs"
    }:
        raise ValueError("image job manifest fields are invalid")
    if payload["schema_version"] != 1 or payload["status"] != "ready":
        raise ValueError("image job manifest must be schema_version=1/status=ready")
    jobs_raw = payload["jobs"]
    if not isinstance(jobs_raw, list) or len(jobs_raw) != 4:
        raise ValueError("image job manifest must contain exactly four jobs")
    jobs = []
    intervals: dict[str, list[tuple[int, int]]] = {}
    for raw in jobs_raw:
        required = set(ImageJobManifestEntry.__dataclass_fields__)
        if not isinstance(raw, dict) or set(raw) != required:
            raise ValueError("image job manifest entry fields are invalid")
        entry = ImageJobManifestEntry(**raw)
        if (
            not entry.job_id
            or not entry.workload_name
            or entry.limit <= 0
            or entry.offset < 0
            or not math.isfinite(entry.multi_job_start_offset_s)
            or entry.multi_job_start_offset_s < 0
            or len(entry.doc_ids_sha256) != 64
            or entry.input_encoded_bytes <= 0
            or entry.avg_encoded_bytes <= 0
        ):
            raise ValueError("image job manifest entry values are invalid")
        interval = (entry.offset, entry.offset + entry.limit)
        existing = intervals.setdefault(entry.workload_name, [])
        if any(max(interval[0], start) < min(interval[1], end) for start, end in existing):
            raise ValueError("image job manifest source ranges overlap")
        existing.append(interval)
        jobs.append(entry)
    if {item.job_id for item in jobs} != {"short", "long1", "long2", "long3"}:
        raise ValueError("image job manifest requires short/long1/long2/long3")
    start_offsets = [item.multi_job_start_offset_s for item in jobs]
    if start_offsets.count(0.0) != 1 or len({item for item in start_offsets if item > 0}) != 1:
        raise ValueError("image job manifest requires one foreground and three matched late jobs")
    return ImageJobManifest(
        path=resolved,
        sha256=hashlib.sha256(resolved.read_bytes()).hexdigest(),
        jobs=tuple(jobs),
    )


def validate_image_job_source(
    database_url: str,
    entry: ImageJobManifestEntry,
) -> tuple[frozenset[str], dict[str, object]]:
    """Fail closed if PostgreSQL no longer matches an immutable job entry."""

    doc_ids, metadata = read_image_source_metadata(
        database_url,
        ImageSourceConfig(entry.workload_name, entry.limit, entry.offset),
    )
    observed = doc_ids_sha256(doc_ids)
    if observed != entry.doc_ids_sha256:
        raise ValueError(f"image job {entry.job_id} doc-id digest changed")
    if int(metadata["input_encoded_bytes"]) != entry.input_encoded_bytes:
        raise ValueError(f"image job {entry.job_id} encoded-byte total changed")
    return doc_ids, metadata


def build_image_job_manifest(
    *,
    database_url: str,
    workload_name: str,
    short_rows: int,
    long_rows: int,
    late_offset_s: float,
    output_path: Path,
) -> ImageJobManifest:
    """Freeze one short and three disjoint equal-size long image jobs."""

    if min(short_rows, long_rows) <= 0 or late_offset_s <= 0:
        raise ValueError("row counts and late_offset_s must be positive")
    offsets = (0, short_rows, short_rows + long_rows, short_rows + 2 * long_rows)
    entries = []
    for index, (job_id, rows, offset) in enumerate(
        zip(("short", "long1", "long2", "long3"), (short_rows, long_rows, long_rows, long_rows), offsets, strict=True)
    ):
        doc_ids, metadata = read_image_source_metadata(
            database_url,
            ImageSourceConfig(workload_name, rows, offset),
        )
        entries.append(
            ImageJobManifestEntry(
                job_id=job_id,
                workload_name=workload_name,
                limit=rows,
                offset=offset,
                multi_job_start_offset_s=0.0 if index == 0 else late_offset_s,
                doc_ids_sha256=doc_ids_sha256(doc_ids),
                input_encoded_bytes=int(metadata["input_encoded_bytes"]),
                avg_encoded_bytes=float(metadata["avg_encoded_bytes"]),
            )
        )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "status": "ready",
        "selection": {
            "kind": "contiguous_disjoint_postgresql_ranges",
            "short_rows": short_rows,
            "long_rows_each": long_rows,
            "late_offset_s": late_offset_s,
        },
        "jobs": [asdict(item) for item in entries],
    }
    temporary = output_path.with_suffix(output_path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(output_path)
    return load_image_job_manifest(output_path)
