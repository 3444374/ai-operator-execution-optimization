#!/usr/bin/env python3
"""Build the owned small patch against pinned external source checkouts."""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

OWNED = Path(__file__).resolve().parent
CODE = OWNED.parents[1]
sys.path.insert(0, str(CODE))
from src.baselines.common.redact import redact_text


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(command: list[str], *, cwd: Path, log: Path | None = None) -> str:
    try:
        result = subprocess.run(command, cwd=cwd, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, timeout=600)
    except subprocess.TimeoutExpired as error:
        partial = error.stdout or b''
        output = redact_text(partial.decode(errors='replace') if isinstance(partial, bytes) else partial)
        if log is not None:
            log.write_text(output + '\nBuild action exceeded 600 seconds.\n')
        raise RuntimeError('build action exceeded 600 seconds') from None
    output = redact_text(result.stdout)
    if log is not None:
        log.write_text(output)
    if result.returncode:
        raise RuntimeError(f"build action failed ({result.returncode}): {output[-6000:]}")
    return output


def prepare(source: Path) -> dict:
    identity = json.loads((OWNED / 'source_identity.json').read_text())
    head = run(['git', 'rev-parse', 'HEAD'], cwd=source).strip()
    if head != identity['upstream_commit']:
        raise ValueError('duckdb-ai checkout does not match the pinned commit')
    hashes = {name: digest(source / name) for name in identity['original_sha256']}
    if hashes == identity['original_sha256']:
        run(['git', 'apply', '--unidiff-zero', '--check', str(OWNED / 'duckdb_ai_semloom.patch')], cwd=source)
        run(['git', 'apply', '--unidiff-zero', str(OWNED / 'duckdb_ai_semloom.patch')], cwd=source)
    elif hashes != identity['patched_sha256']:
        raise ValueError('duckdb-ai source differs from both the pinned source and this patch')
    for name in ('semloom_batch.h', 'semloom_batch.hpp'):
        shutil.copyfile(OWNED / name, source / 'src/include' / name)
    shutil.copyfile(OWNED / 'semloom_batch.cpp', source / 'src/semloom_batch.cpp')
    return identity


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--duckdb', type=Path, required=True)
    parser.add_argument('--build', type=Path, required=True)
    parser.add_argument('--jobs', type=int, default=2)
    args = parser.parse_args(argv)
    if not 1 <= args.jobs <= 8:
        parser.error('--jobs must be between 1 and 8')
    source, duckdb, build = (p.resolve() for p in (args.source, args.duckdb, args.build))
    identity = prepare(source)
    if run(['git', 'rev-parse', 'HEAD'], cwd=duckdb).strip() != identity['duckdb_commit']:
        raise ValueError('DuckDB headers do not match the pinned v1.5.4 ABI')
    build.mkdir(parents=True, exist_ok=True)
    extension_config = build / 'semloom-extension.cmake'
    extension_config.write_text('duckdb_extension_load(ai SOURCE_DIR "' + source.as_posix()
                                + '" EXTENSION_VERSION 0.4.14-semloom1)\n')
    # Preserve DuckDB's own metadata appender and dependent yyjson/re2 targets.
    run(['cmake', '-S', str(duckdb), '-B', str(build), '-G', 'Ninja',
         '-DCMAKE_BUILD_TYPE=Release', '-DCMAKE_CXX_STANDARD=17', '-DBUILD_UNITTESTS=OFF', '-DBUILD_SHELL=OFF',
         '-DEXTENSION_STATIC_BUILD=OFF',
         '-DENABLE_SANITIZER=FALSE', '-DENABLE_UBSAN=FALSE',
         '-DDUCKDB_EXTENSION_CONFIGS=' + str(extension_config),
         '-DGIT_COMMIT_HASH=' + identity['duckdb_commit'][:10],
         '-DOVERRIDE_GIT_DESCRIBE=v1.5.4'], cwd=source, log=build / 'configure.log')
    run(['cmake', '--build', str(build), '--target', 'ai_loadable_extension',
         '--parallel', str(args.jobs)], cwd=source, log=build / 'compile.log')
    binary = build / 'extension/ai/ai.duckdb_extension'
    manifest = {**identity, 'binary': str(binary), 'binary_sha256': digest(binary),
                'patch_sha256': digest(OWNED / 'duckdb_ai_semloom.patch'),
                'adapter_sha256': {name: digest(OWNED / name)
                                  for name in ('semloom_batch.h', 'semloom_batch.hpp', 'semloom_batch.cpp')},
                'jobs': args.jobs, 'real_model_requests': 0}
    (build / 'build-identity.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print(json.dumps({'binary': str(binary), 'binary_sha256': manifest['binary_sha256']}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
