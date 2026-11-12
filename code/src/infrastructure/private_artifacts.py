"""Write new private data verbatim; keep public summaries a separate operation."""

from contextlib import contextmanager
import hashlib
import json
import math
import os
from pathlib import Path
import stat
import uuid


def content_digest(value: object) -> str:
    raw = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def file_identity(path):
    """Describe file bytes without loading the complete file into memory."""
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda:stream.read(1048576),b''):
            digest.update(block)
    return dict(sha256=digest.hexdigest(),bytes=Path(path).stat().st_size)


def json_metric(value):
    """Keep upstream infinite-error meaning while writing strict JSON."""
    if isinstance(value,float) and not math.isfinite(value):
        return 'Infinity' if value>0 else '-Infinity' if value<0 else 'NaN'
    if isinstance(value,dict):
        return {k:json_metric(v) for k,v in value.items()}
    if isinstance(value,(list,tuple)):
        return [json_metric(v) for v in value]
    return value


def require_outside_git(path: Path) -> None:
    resolved = path.resolve()
    if any((parent / ".git").exists() for parent in (resolved, *resolved.parents)):
        raise ValueError("private artifacts must be outside a Git checkout")


def new_private_directory(path: Path) -> None:
    require_outside_git(path)
    path.mkdir(parents=True, mode=0o700, exist_ok=False)


def copy_private_tree(source, destination):
    """Copy regular evidence bytes; record links/special nodes without following them.

    The caller must persist the returned inventory beside the copy and quiesce its
    source first. This is not a filesystem snapshot or a resource-cleanup verdict.
    Any read, write or verification error propagates, retaining the partial copy.
    """
    source, destination = Path(source), Path(destination)
    if not stat.S_ISDIR(source.lstat().st_mode):
        raise ValueError('evidence source must be a directory, not a symlink')
    if destination.resolve().is_relative_to(source.resolve()):
        raise ValueError('evidence destination must not be inside its source')
    new_private_directory(destination)
    entries = []

    def copy_node(path, target):
        info = path.lstat()
        node = dict(path=path.relative_to(source).as_posix(), mode=info.st_mode,
                    device=info.st_dev, inode=info.st_ino, uid=info.st_uid,
                    gid=info.st_gid, size=info.st_size, mtime_ns=info.st_mtime_ns)
        if stat.S_ISDIR(info.st_mode):
            node.update(type='directory', storage='directory')
            if path != source:
                target.mkdir(mode=0o700)
            entries.append(node)
            for child in sorted(path.iterdir()):
                copy_node(child, target / child.name)
        elif stat.S_ISREG(info.st_mode):
            digest, size = hashlib.sha256(), 0
            descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            with os.fdopen(descriptor, 'rb') as reader:
                opened = os.fstat(reader.fileno())
                if (not stat.S_ISREG(opened.st_mode) or
                        (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns) != (
                            info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)):
                    raise OSError('evidence file changed before copying: ' + str(path))
                output = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                with os.fdopen(output, 'wb') as writer:
                    for block in iter(lambda: reader.read(1048576), b''):
                        writer.write(block)
                        digest.update(block)
                        size += len(block)
                    writer.flush()
                    os.fsync(writer.fileno())
                after = os.fstat(reader.fileno())
            expected = dict(sha256=digest.hexdigest(), bytes=size)
            if (size != info.st_size or (after.st_size, after.st_mtime_ns) != (
                    opened.st_size, opened.st_mtime_ns) or file_identity(target) != expected):
                raise OSError('evidence file copy verification failed: ' + str(path))
            node.update(type='file', storage='copied_and_verified', **expected)
            entries.append(node)
        else:
            kind = next((name for name, check in (
                ('symlink', stat.S_ISLNK), ('socket', stat.S_ISSOCK), ('fifo', stat.S_ISFIFO),
                ('character_device', stat.S_ISCHR), ('block_device', stat.S_ISBLK))
                if check(info.st_mode)), 'other')
            node.update(type=kind, storage='metadata_only', rdev=info.st_rdev)
            if kind == 'symlink':
                node['link_target'] = os.readlink(path)
            entries.append(node)

    copy_node(source, destination)
    return dict(schema='semloom.private_tree_copy.v1', source=str(source),
                destination=str(destination), entries=entries,
                regular_files_verified=sum(item['type'] == 'file' for item in entries),
                scope='archive only; no query acceptance or resource cleanup inference')


@contextmanager
def open_private_text(path: Path, *, newline=None):
    """Refuse overwrite/symlinks and flush private bytes without redaction."""
    require_outside_git(path)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8", newline=newline) as stream:
        try:
            yield stream
        finally:
            stream.flush()
            os.fsync(stream.fileno())


def write_private_json(path: Path, value: object) -> None:
    payload = json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
    with open_private_text(path) as stream:
        stream.write(payload)


def publish_private_json(path: Path, value: object) -> None:
    """Expose a complete fresh record atomically to a concurrent observer."""
    path = Path(path)
    pending = path.with_name('.pending-' + uuid.uuid4().hex)
    write_private_json(pending, value)
    os.link(pending, path)  # Exclusive publication: never replace old evidence.
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
        pending.unlink()
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def read_json(path, *, max_bytes=8 * 1024**2):
    path = Path(path)
    if path.is_symlink() or not path.is_file() or path.stat().st_size > max_bytes:
        raise ValueError("a bounded regular JSON file is required")
    def reject(value):
        raise ValueError("nonfinite JSON: " + value)
    return json.loads(path.read_text(), parse_constant=reject)


def file_sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()
