#!/usr/bin/env python3
"""Prepare pinned public IMLane dependencies in an explicit external directory."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import time

OWNED = Path(__file__).resolve().parent
sys.path.insert(0, str(OWNED.parents[1]))
from src.baselines.common.redact import redact_text

IDENTITY = json.loads((OWNED / 'source_identity.json').read_text())


def run(command: list[str], cwd: Path, logs: Path, *, turbo: bool = False) -> str:
    logs.mkdir(parents=True, exist_ok=True)
    log = logs / f'{time.time_ns()}-{Path(command[0]).name}.log'
    env = dict(os.environ)
    for key in ('http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'all_proxy'):
        env.pop(key, None)
    if turbo and Path('/etc/network_turbo').exists():
        command = ['bash', '-c', 'source /etc/network_turbo >/dev/null 2>&1; exec "$@"', 'imlane-network', *command]
    with (logs / 'actions.jsonl').open('a') as stream:
        stream.write(json.dumps({'command': [redact_text(item) for item in command],
                                'cwd': str(cwd), 'log': log.name}) + '\n')
    try:
        result = subprocess.run(command, cwd=cwd, env=env, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, timeout=3600)
    except subprocess.TimeoutExpired as error:
        partial = error.stdout or b''
        if isinstance(partial, bytes): partial = partial.decode(errors='replace')
        log.write_text(redact_text(partial) + '\nAction exceeded 3600 seconds.\n')
        raise RuntimeError(f'action exceeded 3600 seconds; retained log: {log.name}') from None
    output = redact_text(result.stdout)
    log.write_text(output)
    if result.returncode:
        raise RuntimeError(f'action failed ({result.returncode}); retained log: {log.name}\n{output[-2000:]}')
    return output


def clone(url: str, commit: str, target: Path, logs: Path) -> None:
    if not target.exists():
        target.mkdir(parents=True)
        run(['git', 'init', '-q'], target, logs)
        run(['git', 'remote', 'add', 'origin', url], target, logs)
    head = run(['git', 'rev-parse', '--verify', 'HEAD'], target, logs) if (target / '.git/HEAD').exists() and (target / '.git/objects').exists() and (target / '.git/index').exists() else ''
    if head.strip() != commit:
        run(['git', 'fetch', '--depth=1', 'origin', commit], target, logs, turbo=True)
        run(['git', 'checkout', '--detach', commit], target, logs)
    if run(['git', 'rev-parse', 'HEAD'], target, logs).strip() != commit:
        raise ValueError('dependency commit differs from its source identity')


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--python', type=Path, required=True)
    parser.add_argument('--jobs', type=int, default=8)
    args = parser.parse_args(argv)
    root = args.root.resolve()
    if root.is_relative_to(OWNED.parents[2]):
        parser.error('--root must be outside the repository')
    if not 1 <= args.jobs <= 8:
        parser.error('--jobs must be between 1 and 8')
    root.mkdir(parents=True, exist_ok=True)
    logs, source = root / 'artifacts/prepare', root / 'source'
    env_python = root / 'venv/bin/python'
    os.environ['PIP_CACHE_DIR'] = str(root / 'cache/pip')
    os.environ['TMPDIR'] = str(root / 'tmp')
    Path(os.environ['TMPDIR']).mkdir(parents=True, exist_ok=True)
    if not env_python.exists():
        run([str(args.python.resolve()), '-m', 'venv', str(root / 'venv')], root, logs)
    run([str(env_python), '-m', 'pip', 'install', 'numpy==1.26.4', 'pyarrow==16.1.0',
         'ray[default]==2.56.1', 'psutil==7.0.0', 'setuptools==80.9.0', 'wheel==0.45.1'], root, logs)
    clone(IDENTITY['upstream_url'], IDENTITY['upstream_commit'], source, logs)
    for name, item in IDENTITY['git_dependencies'].items():
        clone(item['url'], item['commit'], source / 'third_party' / name, logs)
    boost = source / 'third_party/boost'
    archive = root / 'cache/boost_1_84_0.tar.gz'
    archive.parent.mkdir(parents=True, exist_ok=True)
    if not archive.exists():
        run(['curl', '--fail', '--location', '--retry', '3', '--output', str(archive),
             IDENTITY['boost']['url']], root, logs)
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    if digest != IDENTITY['boost']['sha256']:
        raise ValueError('Boost archive does not match its official SHA-256')
    if not boost.exists():
        with tarfile.open(archive) as tar:
            tar.extractall(source / 'third_party', filter='data')
        (source / 'third_party/boost_1_84_0').rename(boost)
    if not (boost / 'dist/lib/libboost_program_options.so').exists():
        run(['bash', 'bootstrap.sh', '--with-libraries=program_options,system',
             '--prefix=' + str(boost / 'dist')], boost, logs)
        run(['./b2', '-j' + str(args.jobs), 'variant=release', 'link=shared',
             'threading=multi', 'cxxflags=-std=c++17', 'install'], boost, logs)
    fmt = source / 'third_party/fmt'
    run(['cmake', '-S', str(fmt), '-B', str(root / 'build-fmt'), '-G', 'Ninja',
         '-DCMAKE_BUILD_TYPE=Release', '-DBUILD_SHARED_LIBS=ON', '-DFMT_TEST=OFF',
         '-DCMAKE_POSITION_INDEPENDENT_CODE=ON', '-DCMAKE_INSTALL_PREFIX=' + str(fmt / 'dist')], root, logs)
    run(['cmake', '--build', str(root / 'build-fmt'), '--parallel', str(args.jobs)], root, logs)
    run(['cmake', '--install', str(root / 'build-fmt')], root, logs)
    run([str(env_python), '-m', 'pip', 'install', '--no-build-isolation', '--no-deps',
         str(source / 'contrib/pythonpkg')], root, logs)
    freeze = run([str(env_python), '-m', 'pip', 'freeze', '--all'], root, logs)
    (root / 'artifacts/python-lock.txt').write_text(freeze)
    print(json.dumps({'prepared': True, 'source_commit': IDENTITY['upstream_commit'],
                      'real_model_requests': 0, 'jobs': args.jobs}))
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except Exception as error:
        print(redact_text(f'{type(error).__name__}: {error}'), file=sys.stderr)
        raise SystemExit(1)
