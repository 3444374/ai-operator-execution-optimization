#!/usr/bin/env python3
"""Build pinned external IMLane and the owned Arrow fixture without database forks."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

OWNED = Path(__file__).resolve().parent
sys.path.insert(0, str(OWNED.parents[1]))
from src.baselines.common.redact import redact_text
from prepare import run


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify_source(source: Path, *, patched: bool) -> dict:
    identity = json.loads((OWNED / 'source_identity.json').read_text())
    if subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=source, text=True).strip() != identity['upstream_commit']:
        raise ValueError('IMLane source does not match its pinned commit')
    changed = set(subprocess.check_output(['git', 'diff', '--name-only', 'HEAD'],
                                         cwd=source, text=True).splitlines())
    allowed = set(identity['patched_sha256']) if patched else set()
    if not changed <= allowed:
        raise ValueError('IMLane source has unreviewed tracked file edits')
    expected = dict(identity['original_sha256'])
    if patched:
        expected.update(identity['patched_sha256'])
    hashes = {name: digest(source / name) for name in expected}
    if hashes != expected:
        raise ValueError('IMLane source files differ from the selected source identity')
    for name, item in identity['git_dependencies'].items():
        head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=source / 'third_party' / name, text=True).strip()
        if head != item['commit']:
            raise ValueError(f'{name} dependency differs from its pinned commit')
        if subprocess.run(['git', 'diff', '--quiet', 'HEAD', '--'],
                          cwd=source / 'third_party' / name).returncode:
            raise ValueError(f'{name} dependency has unreviewed tracked file edits')
    return identity


def arrow_config(directory: Path, python: Path) -> dict:
    data = json.loads(subprocess.check_output([str(python), '-c',
        'import json,pyarrow,sys,sysconfig; pyarrow.create_library_symlinks(); '
        'print(json.dumps(dict(python=sys.version,arrow=pyarrow.__version__,include=pyarrow.get_include(),'
        'lib=pyarrow.get_library_dirs()[0],site=sysconfig.get_path("purelib"))))'], text=True))
    if data['arrow'] != '16.1.0':
        raise ValueError('the fixture requires matching Arrow C++ and Python version 16.1.0')
    directory.mkdir(parents=True, exist_ok=True)
    (directory / 'ArrowConfig.cmake').write_text(
        'set(Arrow_FOUND TRUE)\nset(ARROW_VERSION "16.1.0")\n'
        'if(NOT TARGET Arrow::arrow_shared)\n'
        'add_library(Arrow::arrow_shared SHARED IMPORTED GLOBAL)\n'
        'set_target_properties(Arrow::arrow_shared PROPERTIES\n'
        f'IMPORTED_LOCATION "{data["lib"]}/libarrow.so"\n'
        f'INTERFACE_INCLUDE_DIRECTORIES "{data["include"]}")\nendif()\n')
    return data


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--mode', choices=('pristine', 'patched'), required=True)
    parser.add_argument('--jobs', type=int, default=8)
    args = parser.parse_args(argv)
    if not 1 <= args.jobs <= 8: parser.error('--jobs must be between 1 and 8')
    root = args.root.resolve()
    if root.is_relative_to(OWNED.parents[2]): parser.error('--root must be outside the repository')
    source = root / 'source'
    logs = root / 'artifacts' / ('build-' + args.mode)
    logs.mkdir(parents=True, exist_ok=True)
    if args.mode == 'patched':
        expected = json.loads((OWNED / 'source_identity.json').read_text())
        if all(digest(source / k) == v for k, v in expected['original_sha256'].items()):
            verify_source(source, patched=False)
            run(['git', 'apply', '--unidiff-zero', '--check', str(OWNED / 'imlane_fixture.patch')], source, logs)
            run(['git', 'apply', '--unidiff-zero', str(OWNED / 'imlane_fixture.patch')], source, logs)
    identity = verify_source(source, patched=args.mode == 'patched')
    python = root / 'venv/bin/python'
    data = arrow_config(root / 'arrow-cmake', python)
    os.environ['PATH'] = str(python.parent) + os.pathsep + os.environ['PATH']
    os.environ['TMPDIR'] = str(root / 'tmp')
    prefix, build = root / ('prefix-' + args.mode), root / ('build-' + args.mode)
    prior = logs / 'build-identity.json'
    if prior.exists():
        shutil.copyfile(prior, logs / ('build-identity-before-' + str(time.time_ns()) + '.json'))
    started = time.monotonic()
    commands = [
        ['cmake', '-S', str(OWNED), '-B', str(build), '-G', 'Ninja', '-DCMAKE_BUILD_TYPE=Release',
         '-DCMAKE_INSTALL_PREFIX=' + str(prefix), '-DIMLANE_SOURCE=' + str(source),
         '-DPYTHON_EXECUTABLE=' + str(python), '-DPython_EXECUTABLE=' + str(python),
         '-DPYARROW_LIBRARY_DIR=' + data['lib'], '-DArrow_DIR=' + str(root / 'arrow-cmake'),
         '-DCMAKE_INSTALL_LIBDIR=lib', '-DCMAKE_INSTALL_INCLUDEDIR=include'],
        ['cmake', '--build', str(build), '--parallel', str(args.jobs)],
        ['cmake', '--install', str(build)]
    ]
    for command in commands: run(command, root, logs)
    binaries = {str(p.relative_to(prefix)): digest(p) for p in
                (prefix / 'bin/imlane', prefix / 'bin/backend/py_exec_instance', prefix / 'bin/imlane_fixture_cpp')}
    manifest = {**identity, 'mode': args.mode, 'python_arrow': data, 'binaries_sha256': binaries,
                'jobs': args.jobs, 'build_seconds': time.monotonic() - started,
                'owned_sha256': {p.name: digest(p) for p in OWNED.iterdir() if p.is_file()},
                'real_model_requests': 0}
    (logs / 'build-identity.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print(json.dumps({'built': True, 'mode': args.mode, 'binaries_sha256': binaries, 'real_model_requests': 0}))
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except Exception as error:
        print(redact_text(f'{type(error).__name__}: {error}'), file=sys.stderr)
        raise SystemExit(1)
