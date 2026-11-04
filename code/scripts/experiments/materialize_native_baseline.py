"""Prepare a named, hash-verified historical code tree; never run a model.

The maintained implementation stays in code/. Only files that differ are stored
in the reproduction directory. Selection is explicit and runs in a separate tree.
"""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import shutil
import zlib


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def member(root, name):
    relative = Path(name)
    if relative.is_absolute() or '..' in relative.parts:
        raise ValueError('relative reproduction member required')
    path = root / relative
    if path.is_symlink() or not path.resolve().is_relative_to(root.resolve()):
        raise ValueError('reproduction member escapes its declared root')
    return path


def read_source(repository, manifest_dir, spec, name):
    """Read one exact source, including declared, length-checked gzip archives."""
    source = (member(manifest_dir, spec['overrides'][name])
              if name in spec['overrides'] else member(repository, name))
    if not source.is_file():
        raise ValueError('reproduction source differs: ' + name)
    formats = spec.get('source_formats', {})
    if type(formats) is not dict:
        raise ValueError('reproduction source formats must be an object')
    encoding = formats.get(name)
    if encoding is None:
        data = source.read_bytes()
    else:
        if (name not in spec['overrides'] or type(encoding) is not dict
                or set(encoding) != {'encoding', 'decoded_bytes', 'archive_sha256'}
                or encoding['encoding'] != 'gzip' or type(encoding['decoded_bytes']) is not int
                or encoding['decoded_bytes'] < 1 or digest(source) != encoding['archive_sha256']):
            raise ValueError('invalid reproduction source archive: '+name)
        try:
            with gzip.open(source, 'rb') as stream:
                data = stream.read(encoding['decoded_bytes']+1)
        except (OSError, EOFError, zlib.error) as error:
            raise ValueError('invalid reproduction source archive: '+name) from error
        if len(data) != encoding['decoded_bytes']:
            raise ValueError('reproduction source archive length differs: '+name)
    if hashlib.sha256(data).hexdigest() != spec['code_files_sha256'][name]:
        raise ValueError('reproduction source differs: '+name)
    return data


def materialize(repository, manifest_dir, variant, output):
    if output.exists():
        raise FileExistsError(output)
    manifest = json.loads((manifest_dir / 'MANIFEST.json').read_text())
    if variant not in manifest['variants']:
        raise ValueError('select a declared variant explicitly')
    spec = manifest['variants'][variant]
    selected = []
    for name, expected in spec['code_files_sha256'].items():
        destination = member(output, name)
        data = read_source(repository, manifest_dir, spec, name)
        source = (member(manifest_dir, spec['overrides'][name])
                  if name in spec['overrides'] else member(repository, name))
        selected.append((data if name in spec.get('source_formats', {}) else None,
                         source, destination, name, expected))
    output.mkdir(parents=True, exist_ok=False)
    for data, source, destination, _, _ in selected:
        destination.parent.mkdir(parents=True, exist_ok=True)
        if data is None:
            shutil.copy2(source, destination)
        else:
            destination.write_bytes(data)
    receipt = dict(schema='semloom.native_baseline.code_projection.v1', variant=variant,
        original_source_revision=spec['original_source_revision'],
        identity_scope='exact declared code projection; content ID is not a Git commit',
        code_projection_sha256=spec['code_projection_sha256'],
        files=len(selected), model_calls=0, models_started=0,
        packet_assets_and_environment_bound=False,
        entrypoint='code/scripts/experiments/run_movie_records_matrix.py',
        requirements='fresh explicit packet, source/input/token/environment bindings and owner qualification')
    (output / 'REPRODUCTION.json').write_text(json.dumps(receipt, indent=2) + '\n')
    return receipt


def main(argv=None):
    repository = Path(__file__).resolve().parents[3]
    default = repository / 'experiments/reproduction/native_baselines'
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('list', 'prepare'))
    parser.add_argument('--variant')
    parser.add_argument('--output-dir', type=Path)
    args = parser.parse_args(argv)
    if args.action == 'list':
        print(json.dumps(list(json.loads((default / 'MANIFEST.json').read_text())['variants'])))
        return 0
    if not args.variant or args.output_dir is None:
        parser.error('prepare requires explicit --variant and --output-dir')
    print(json.dumps(materialize(repository, default, args.variant, args.output_dir)))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
