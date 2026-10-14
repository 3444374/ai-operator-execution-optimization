"""Pinned COCO file metadata and lazy encoded-JPEG sources.

Manifest preparation reads and hashes content once, without decoding images.
Runs pin size/mtime/ctime metadata instead of repeating that content scan. Keep
the source directory unchanged during a run; this is not a filesystem snapshot.
"""

from __future__ import annotations

import hashlib
import json
import math
import stat
import time
from dataclasses import asdict, dataclass
from pathlib import Path, PurePosixPath

from ...data.coco import coco_doc_id, list_images
from .source import ImageSourceConfig


@dataclass(frozen=True)
class ImageFileEntry:
    doc_id: str
    path: str
    image_bytes: int
    mtime_ns: int
    ctime_ns: int
    sha256: str


def checked_image_path(root: Path, relative: str) -> Path:
    if not isinstance(relative, str):
        raise ValueError("image path must be a string")
    path = PurePosixPath(relative)
    if (not relative or path.is_absolute() or relative != path.as_posix()
            or ".." in path.parts or "\\" in relative
            or path.suffix.lower() not in (".jpg", ".jpeg")):
        raise ValueError("image path must be a relative JPEG path below the root")
    candidate = root
    for part in path.parts:
        candidate = candidate / part
        if candidate.is_symlink():
            raise ValueError("image source must not contain symlinks")
    if not candidate.resolve(strict=True).is_relative_to(root):
        raise ValueError("image path escapes the root")
    if not stat.S_ISREG(candidate.stat().st_mode):
        raise ValueError("image path must name a regular file")
    return candidate


@dataclass(frozen=True)
class ImageFileManifest:
    path: Path
    root: Path
    sha256: str
    workload_name: str
    entries: tuple[ImageFileEntry, ...]
    preparation: dict[str, float | int]

    def select(self, config: ImageSourceConfig) -> tuple[ImageFileEntry, ...]:
        selected = self.entries[config.offset : config.offset + config.limit]
        if config.workload_name != self.workload_name or len(selected) != config.limit:
            raise ValueError("file source workload or selected row count does not match manifest")
        return selected

    def metadata(self, config: ImageSourceConfig) -> tuple[frozenset[str], dict[str, object]]:
        entries = self.select(config)
        ids = frozenset(
            file_doc_id(entry.doc_id, index, config.dataset_passes)
            for index in range(1, config.dataset_passes + 1) for entry in entries
        )
        size = sum(entry.image_bytes for entry in entries)
        return ids, {
            "server_version": "not_applicable_file_source",
            "pgvector_version": "not_applicable_file_source",
            "input_encoded_bytes": size * config.dataset_passes,
            "avg_encoded_bytes": size / len(entries),
            "max_encoded_bytes": max(entry.image_bytes for entry in entries),
        }


def file_doc_id(doc_id: str, pass_index: int, passes: int) -> str:
    return doc_id if passes == 1 else f"{doc_id}#pass={pass_index}"


def load_image_file_manifest(path: str | Path, root: str | Path) -> ImageFileManifest:
    root = Path(root).resolve(strict=True)
    if not root.is_dir():
        raise ValueError("image root must be a directory")
    path = Path(path)
    raw = path.read_bytes()
    payload = json.loads(raw)
    if (not isinstance(payload, dict)
            or set(payload) != {"schema_version", "source_kind", "workload_name", "entries", "preparation"}
            or type(payload["schema_version"]) is not int or payload["schema_version"] != 1
            or payload["source_kind"] != "coco_jpeg_files"
            or not isinstance(payload["workload_name"], str) or not payload["workload_name"]
            or not isinstance(payload["entries"], list) or not payload["entries"]):
        raise ValueError("invalid or empty image file manifest")
    preparation = payload["preparation"]
    if (not isinstance(preparation, dict)
            or set(preparation) != {"metadata_s", "content_hash_s", "content_hash_bytes"}
            or any(type(x) not in (int, float) or not math.isfinite(x) or x < 0
                   for x in preparation.values())
            or type(preparation["content_hash_bytes"]) is not int):
        raise ValueError("invalid manifest preparation accounting")
    entries = []
    ids, paths = set(), set()
    for value in payload["entries"]:
        if not isinstance(value, dict) or set(value) != set(ImageFileEntry.__dataclass_fields__):
            raise ValueError("invalid image file entry fields")
        entry = ImageFileEntry(**value)
        p = checked_image_path(root, entry.path)
        source_id = coco_doc_id(p)
        if (source_id < 0 or entry.doc_id != str(source_id)
                or entry.doc_id in ids or entry.path in paths):
            raise ValueError("duplicate or mismatched COCO row identity")
        if (any(type(x) is not int for x in (entry.image_bytes, entry.mtime_ns, entry.ctime_ns))
                or entry.image_bytes <= 0
                or not isinstance(entry.sha256, str) or len(entry.sha256) != 64
                or any(x not in "0123456789abcdef" for x in entry.sha256)):
            raise ValueError("invalid image file metadata or digest")
        s = p.stat()
        if (s.st_size, s.st_mtime_ns, s.st_ctime_ns) != (entry.image_bytes, entry.mtime_ns, entry.ctime_ns):
            raise ValueError("image file metadata changed; prepare a new manifest")
        ids.add(entry.doc_id)
        paths.add(entry.path)
        entries.append(entry)
    if sum(entry.image_bytes for entry in entries) != preparation["content_hash_bytes"]:
        raise ValueError("manifest preparation byte count does not match its entries")
    return ImageFileManifest(path, root, hashlib.sha256(raw).hexdigest(), payload["workload_name"],
                             tuple(entries), preparation)


def build_image_file_manifest(*, root: str | Path, workload_name: str,
                              output: str | Path, limit: int = 0, offset: int = 0) -> ImageFileManifest:
    if not workload_name or min(limit, offset) < 0:
        raise ValueError("workload is required and selection counts must be non-negative")
    output = Path(output)
    if output.exists():
        raise FileExistsError("refuse to overwrite an existing image file manifest")
    root = Path(root).resolve(strict=True)
    if not root.is_dir():
        raise ValueError("image root must be a directory")
    started = time.perf_counter()
    files = list_images(root, "*.jpg", limit, offset)
    if not files:
        raise ValueError("image source selection is empty")
    selected = []
    ids = set()
    for p in files:
        relative = p.relative_to(root).as_posix()
        p = checked_image_path(root, relative)
        before = p.stat()
        doc_id = coco_doc_id(p)
        if doc_id < 0 or doc_id in ids:
            raise ValueError("duplicate or invalid COCO row identity")
        if before.st_size == 0:
            raise ValueError("image file is empty")
        ids.add(doc_id)
        selected.append((p, relative, before, str(doc_id)))
    metadata_s = time.perf_counter() - started
    hash_started = time.perf_counter()
    entries = []
    for p, relative, before, doc_id in selected:
        digest = hashlib.sha256()
        with p.open("rb") as f:
            header = f.read(3)
            if header != b"\xff\xd8\xff":
                raise ValueError("image file does not have a JPEG header")
            digest.update(header)
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                digest.update(chunk)
        after = p.stat()
        if (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
                after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            raise ValueError("image changed during manifest preparation")
        entries.append(ImageFileEntry(doc_id, relative, after.st_size,
                                      after.st_mtime_ns, after.st_ctime_ns, digest.hexdigest()))
    payload = {
        "schema_version": 1, "source_kind": "coco_jpeg_files", "workload_name": workload_name,
        "entries": [asdict(entry) for entry in entries],
        "preparation": {"metadata_s": metadata_s, "content_hash_s": time.perf_counter() - hash_started,
                        "content_hash_bytes": sum(entry.image_bytes for entry in entries)},
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
        f.write("\n")
    return load_image_file_manifest(output, root)


def build_daft_image_file_source(
    manifest: ImageFileManifest, config: ImageSourceConfig, *,
    batch_size: int = 64, max_connections: int = 1,
):
    """Build metadata only, then use Daft's lazy batched binary download.

    ``max_connections`` is an upstream request parameter, documented per thread
    in Daft 0.7.21. Runtime prefetch and total concurrency remain Daft-owned.
    """
    if min(batch_size, max_connections) <= 0:
        raise ValueError("file reader batch size and connection count must be positive")
    import daft
    from daft.functions.url import download

    entries = manifest.select(config)
    rows = [(entry, index) for index in range(1, config.dataset_passes + 1) for entry in entries]
    frame = daft.from_pydict({
        "doc_id": [file_doc_id(entry.doc_id, index, config.dataset_passes) for entry, index in rows],
        "workload_name": [manifest.workload_name for _ in rows],
        "path": [(manifest.root / entry.path).as_uri() for entry, _ in rows],
        "image_bytes": [entry.image_bytes for entry, _ in rows],
    })
    return frame.into_batches(batch_size).with_column(
        "image", download(daft.col("path"), max_connections=max_connections, on_error="raise")
    )
