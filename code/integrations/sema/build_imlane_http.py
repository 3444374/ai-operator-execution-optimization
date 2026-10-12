#!/usr/bin/env python3
"""Link the Sema complete-call client against an already built public IMLane prefix."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys

OWNED = Path(__file__).resolve().parent
sys.path.insert(0, str(OWNED.parents[1]))
from src.baselines.common.private_artifacts import require_outside_git, write_private_json
from src.baselines.common.redact import redact_text


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    root, output = args.root.resolve(), args.output.resolve()
    require_outside_git(root); require_outside_git(output)
    identity = json.loads((root / 'artifacts/build-patched/build-identity.json').read_text())
    if identity['upstream_commit'] != '072f8257db80569e42c9f5727d839587df529652' or identity['mode'] != 'patched':
        raise ValueError('IMLane complete-call client requires the checked public build')
    source, prefix = root / 'source', root / 'prefix-patched'
    expected = dict(identity['original_sha256'], **identity['patched_sha256'])
    for name, digest in expected.items():
        if hashlib.sha256((source / name).read_bytes()).hexdigest() != digest:
            raise ValueError('IMLane checked source changed after its public build')
    for name, digest in identity['binaries_sha256'].items():
        if hashlib.sha256((prefix / name).read_bytes()).hexdigest() != digest:
            raise ValueError('IMLane public build binary changed')
    arrow = identity['python_arrow']
    command = ['g++', '-std=c++17', '-O2', str(OWNED / 'imlane_http.cpp'), '-o', str(output),
        '-I' + str(source / 'include'), '-I' + arrow['include'],
        '-I' + str(source / 'third_party/msgpack/include'),
        '-I' + str(source / 'third_party/concurrentqueue/include'),
        '-I' + str(source / 'third_party/libuv/include'),
        '-I' + str(source / 'third_party/json/single_include'),
        '-I' + str(source / 'third_party/spdlog'),
        '-I' + str(source / 'third_party/fmt/dist/include'),
        '-I' + str(source / 'third_party/boost/dist/include'),
        '-L' + str(prefix / 'lib'), '-L' + arrow['lib'],
        '-L' + str(source / 'third_party/fmt/dist/lib'),
        '-L' + str(source / 'third_party/boost/dist/lib'),
        '-Wl,-rpath,' + ':'.join([str(prefix / 'lib'), arrow['lib'],
            str(source / 'third_party/fmt/dist/lib'), str(source / 'third_party/boost/dist/lib')]),
        '-ldbendrt_cxx', '-larrow', '-lfmt', '-lboost_system', '-pthread']
    if output.exists():
        raise ValueError('IMLane complete-call build refuses to overwrite an existing binary')
    output.parent.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(command, capture_output=True, text=True)
    record = {'upstream_commit': identity['upstream_commit'], 'returncode': result.returncode,
        'compiler_output': redact_text(result.stdout + result.stderr),
        'source_sha256': hashlib.sha256((OWNED / 'imlane_http.cpp').read_bytes()).hexdigest(),
        'binary_sha256': hashlib.sha256(output.read_bytes()).hexdigest() if output.is_file() else None,
        'real_model_requests': 0}
    write_private_json(output.with_suffix('.build.json'), record)
    if result.returncode:
        print(record['compiler_output'], file=sys.stderr)
        return 1
    print(json.dumps({'built': True, 'binary_sha256': record['binary_sha256']}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
