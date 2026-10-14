"""Stable COCO filename selection shared by imports and file experiments."""

from pathlib import Path, PurePath, PurePosixPath
from zipfile import ZipFile


def list_images(directory, pattern, limit, offset=0):
    """Return the importer's deterministic selection without reading payloads."""
    paths = sorted(Path(directory).glob(pattern))
    return paths[offset : offset + limit] if limit > 0 else paths[offset:]


def list_zip_images(archive: ZipFile, pattern: str, limit: int, offset: int = 0) -> list[PurePosixPath]:
    paths = sorted(
        PurePosixPath(info.filename)
        for info in archive.infolist()
        if not info.is_dir() and PurePosixPath(info.filename).match(pattern)
    )
    return paths[offset : offset + limit] if limit > 0 else paths[offset:]


def coco_doc_id(path: PurePath) -> int:
    """Preserve the numeric COCO source ID encoded in the filename."""
    try:
        return int(path.stem)
    except ValueError as exc:
        raise ValueError(f"COCO image filename must have a numeric stem: {path}") from exc
