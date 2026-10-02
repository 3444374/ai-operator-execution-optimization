"""Restore retained experiment bytes without running queries or experiment scripts."""

import hashlib
import json
from pathlib import Path, PurePosixPath
import tarfile

from src.baselines.common.private_artifacts import new_private_directory


SCHEMA = 'semloom.evidence_storage.v2'
RESULT_ROOTS = ('experiments/results/', 'motivation/results/', 'feasibility/results/')


def _relative(value):
    if not isinstance(value, str) or not value or '\\' in value:
        raise ValueError('invalid evidence path')
    path = PurePosixPath(value)
    if not path.parts or path.is_absolute() or '..' in path.parts or path.as_posix() != value:
        raise ValueError('invalid evidence path')
    return path


def _source(repository, value):
    relative = _relative(value)
    if not value.startswith(RESULT_ROOTS):
        raise ValueError('source is outside result directories')
    path = (repository / relative).resolve()
    if not path.is_relative_to(repository):
        raise ValueError('source escapes repository')
    return path


def _digest(data):
    return hashlib.sha256(data).hexdigest()


def _check_bytes(data, record):
    if len(data) != record['bytes'] or _digest(data) != record['sha256']:
        raise ValueError('evidence bytes or digest differ')


def load_manifest(manifest):
    with Path(manifest).open() as stream:
        records = [json.loads(line) for line in stream if line.strip()]
    if not records or records[0].get('schema') != SCHEMA:
        raise ValueError('unsupported evidence manifest')
    header, entries = records[0], records[1:]
    paths = set()
    for entry in entries:
        path = _relative(entry['path']).as_posix()
        if path in paths:
            raise ValueError('duplicate evidence path')
        paths.add(path)
        if type(entry['bytes']) is not int or entry['bytes'] < 0:
            raise ValueError('invalid evidence size')
        digest = entry['sha256']
        if not isinstance(digest, str) or len(digest) != 64 or any(c not in '0123456789abcdef' for c in digest):
            raise ValueError('invalid evidence digest')
        mode = entry.get('mode', 0o644)
        if type(mode) is not int or not 0 <= mode <= 0o777:
            raise ValueError('invalid evidence permissions')
        storage = entry['storage']
        if storage.get('kind') not in ('file', 'archive'):
            raise ValueError('unsupported evidence storage')
    if any(parent.as_posix() in paths for path in paths for parent in PurePosixPath(path).parents
           if parent.parts):
        raise ValueError('conflicting evidence paths')
    if type(header['files']) is not int or type(header['original_bytes']) is not int:
        raise ValueError('invalid manifest totals')
    if len(entries) != header['files'] or sum(e['bytes'] for e in entries) != header['original_bytes']:
        raise ValueError('manifest totals differ')
    return header, entries


def restore_result(manifest, output, *, repository):
    """Validate every source, then materialize original paths in a fresh private directory."""
    repository = Path(repository).resolve()
    manifest = Path(manifest).resolve()
    header, entries = load_manifest(manifest)
    result_root = _source(repository, header['result'] + '/README.md').parent
    if not manifest.is_relative_to(result_root):
        raise ValueError('manifest does not belong to declared result')
    output = Path(output)
    if output.exists() or output.is_symlink():
        raise FileExistsError('restore output already exists')

    archive_records = {}
    for record in header['archives']:
        if record['path'] in archive_records:
            raise ValueError('duplicate archive declaration')
        archive_records[record['path']] = record
    archive_objects = {}
    prepared = []
    for entry in entries:
        storage = entry['storage']
        if storage['kind'] == 'file':
            data = _source(repository, storage['path']).read_bytes()
        else:
            archive_name = storage['archive']
            if archive_name not in archive_records:
                raise ValueError('undeclared evidence archive')
            if archive_name not in archive_objects:
                archive_path = _source(repository, archive_name)
                _check_bytes(archive_path.read_bytes(), archive_records[archive_name])
                objects = {}
                with tarfile.open(archive_path, 'r:gz') as archive:
                    for member in archive:
                        name = _relative(member.name).as_posix()
                        if not member.isfile() or name in objects:
                            raise ValueError('archive contains non-file or duplicate member')
                        objects[name] = archive.extractfile(member).read()
                archive_objects[archive_name] = objects
            member = _relative(storage['member']).as_posix()
            try:
                data = archive_objects[archive_name][member]
            except KeyError as failure:
                raise ValueError('missing evidence archive member') from failure
        _check_bytes(data, entry)
        prepared.append((entry, data))

    new_private_directory(output)
    for entry, data in prepared:
        path = output / _relative(entry['path'])
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with path.open('xb') as stream:
            stream.write(data)
        path.chmod(entry.get('mode', 0o644))
    return {'schema': SCHEMA, 'result': header['result'], 'files_verified': len(prepared),
            'bytes_verified': sum(len(data) for _, data in prepared), 'model_requests': 0}
