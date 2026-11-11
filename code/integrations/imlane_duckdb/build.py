"""Prepare the pinned author's DuckDB copy, or link the thin Movie Q3 driver.

``prepare`` writes a private source copy and explicit configure/build commands;
it never runs CMake, installs packages, or changes the canonical author prefix.
``driver`` compiles q3.cpp against a separately built shared DuckDB library.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys

OWNED = Path(__file__).resolve().parent
sys.path.insert(0, str(OWNED.parents[1]))
from src.infrastructure.private_artifacts import (
    file_identity, new_private_directory, require_outside_git, write_private_json,
)
from src.infrastructure.redact import redact_text

AUTHOR = '072f8257db80569e42c9f5727d839587df529652'
FMT_ORIGINAL = 'set(IMLANE_FMT_PATH "/home/IMLane_main/third_party/fmt/dist")'
FMT_PORTABLE = 'set(IMLANE_FMT_PATH "" CACHE PATH "IMLane fmt prefix")'
CONVERTER_HEADER = Path('src/include/prediction/datachunk_converter.hpp')
SCHEMA_ROOT_CALL = 'ConverArrowAarrayToVector(tbl->chunks[0].children[0], &schema, out)'
SCHEMA_CHILD_CALL = 'ConverArrowAarrayToVector(tbl->chunks[0].children[0], schema.children[0], out)'
ORIGINAL_CONVERTER_SHA256 = '3baaa780a905e7a1c26c4b83aeaa6d48bc6a990d38aceb9e81ae4935aab30666'
SCHEMA_CHILD_SHA256 = 'b361ce33e9ff89e44def48c039a9da2c614dc08b032e3f2eb6d1ec6ea6aafa54'


def prepare_converter(copied_source, *, schema_child):
    """Apply the explicit engineering variant only to a fresh pinned export."""
    header = copied_source / CONVERTER_HEADER
    original = header.read_bytes()
    if hashlib.sha256(original).hexdigest() != ORIGINAL_CONVERTER_SHA256:
        raise ValueError('converter differs from the pinned original author bytes')
    if schema_child:
        header.write_bytes(original.replace(SCHEMA_ROOT_CALL.encode(), SCHEMA_CHILD_CALL.encode()))
    return dict(variant='schema-child-engineering' if schema_child else 'original-author',
                path=str(CONVERTER_HEADER), original_sha256=ORIGINAL_CONVERTER_SHA256,
                prepared=file_identity(header), native_DSO_qualified=False)


def _capture(command):
    return subprocess.run(command, capture_output=True, text=True, check=False)


def _version(command):
    try:
        result = _capture(command)
    except FileNotFoundError:
        return None
    return dict(returncode=result.returncode,
                output=redact_text((result.stdout + result.stderr).strip()))


def _author(root):
    root = root.resolve()
    require_outside_git(root)
    source, prefix = root / 'source', root / 'prefix-patched'
    head = _capture(['git', '-C', str(source), 'rev-parse', 'HEAD'])
    if head.returncode or head.stdout.strip() != AUTHOR:
        raise ValueError('native DuckDB Q3 requires the declared 072f author source')
    identity_path = root / 'artifacts/build-patched/build-identity.json'
    identity = json.loads(identity_path.read_text())
    if identity['upstream_commit'] != AUTHOR or identity['mode'] != 'patched':
        raise ValueError('use the existing qualified patched author prefix')
    return source, prefix, dict(
        upstream_commit=AUTHOR, author_build_identity=file_identity(identity_path),
        author_build_mode=identity['mode'], python_arrow=identity['python_arrow'],
        dbend_inline_header=file_identity(source / 'include/dbend/dbend_context.hpp'),
        dbend_c_library=file_identity(prefix / 'lib/libdbendrt_c.so'),
        dbend_c_static_library=file_identity(prefix / 'lib/libdbendrt_c_a.a'))


def version_options(source):
    """Preserve actual author Git metadata without inventing a DuckDB release."""
    result = _capture(['git', '-C', str(source), 'describe', '--tags', '--long'])
    description = result.stdout.strip() if result.returncode == 0 else None
    options = ['-DGIT_COMMIT_HASH=' + AUTHOR[:10]]
    if description and re.fullmatch(r'v\d+\.\d+\.\d+-\d+-g[0-9a-f]+', description):
        options.append('-DOVERRIDE_GIT_DESCRIBE=' + description)
        declared = 'author-root-git-describe'
    else:
        options.append('-DDUCKDB_EXPLICIT_VERSION=unknown')
        declared = 'unknown; no supported author-root version tag'
    return options, dict(git_describe=description, git_describe_returncode=result.returncode,
                         git_commit=AUTHOR, version_source=declared,
                         paper_duckdb_version_qualified=False)


def configure_commands(source, prefix, copied_source, build, jobs, version_flags):
    fmt = source / 'third_party/fmt/dist'
    configure = [
        'cmake', '-S', str(copied_source), '-B', str(build), '-G', 'Ninja',
        '-DCMAKE_BUILD_TYPE=Release', '-DCMAKE_CXX_STANDARD=17',
        '-DCMAKE_PREFIX_PATH=' + str(prefix) + ';' + str(fmt),
        '-DIMLane_DIR=' + str(prefix / 'lib/cmake'),
        '-DPython3_EXECUTABLE=' + str(source.parent / 'venv/bin/python'),
        '-DIMLANE_FMT_PATH=' + str(fmt), '-DBUILD_PYTHON=OFF',
        '-DBUILD_UNITTESTS=OFF', '-DBUILD_SHELL=OFF', *version_flags,
    ]
    # The author's installed CMake target omits these transitive header paths.
    # Supply them for this build only; keep DuckDB's explicit -I order intact.
    includes = [source / relative for relative in (
        'third_party/boost/dist/include', 'third_party/fmt/dist/include',
        'third_party/nanoarrow/dist/include', 'third_party/msgpack/include',
        'third_party/concurrentqueue/include', 'third_party/libuv/include',
        'third_party/json/single_include', 'third_party/spdlog')]
    return configure, ['env', 'CPLUS_INCLUDE_PATH=' + os.pathsep.join(map(str, includes)),
        'cmake', '--build', str(build), '--target', 'duckdb', '--parallel', str(jobs)]


def restore_missing_sources(copied_source, upstream_source):
    """Restore the three proven upstream files omitted by the author's build* rule."""
    manifest = json.loads((OWNED / 'missing_sources.json').read_text())
    restored = {}
    for name, expected in manifest['files'].items():
        data = (upstream_source / name).read_bytes()
        identity = dict(bytes=len(data), sha256=hashlib.sha256(data).hexdigest(),
                        git_blob=hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest())
        if identity != expected:
            raise ValueError('upstream restoration bytes differ: ' + name)
        output = copied_source / name
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open('xb') as stream:
            stream.write(data)
        restored[name] = identity
    return dict(upstream_commit=manifest['upstream_commit'], restored=restored)


def prepare(args):
    source, prefix, author = _author(args.root)
    workspace = args.workspace.resolve()
    if workspace.is_relative_to(prefix):
        raise ValueError('preparation must not write inside the canonical author prefix')
    new_private_directory(workspace)
    copied_source, build = workspace / 'source', workspace / 'build'
    copied_source.mkdir(mode=0o700)
    archive = workspace / 'author-duckdb.tar'
    # Export the fixed query-engine tree, not mutable canonical working bytes.
    with archive.open('xb') as stream:
        subprocess.run(['git', '-C', str(source), 'archive', '--format=tar', AUTHOR,
                        'database/duckdb'], stdout=stream, check=True)
    subprocess.run(['tar', '-xf', str(archive), '--strip-components=2', '-C', str(copied_source)],
                   check=True)
    cmake = copied_source / 'CMakeLists.txt'
    original = cmake.read_text()
    if original.count(FMT_ORIGINAL) != 1:
        raise ValueError('pinned DuckDB fmt declaration was not found exactly once')
    modified = original.replace(FMT_ORIGINAL, FMT_PORTABLE)
    cmake.write_text(modified)
    converter = prepare_converter(copied_source, schema_child=args.schema_child)
    restoration = restore_missing_sources(copied_source, args.upstream_source)
    version_flags, versions = version_options(source)
    configure, compile_library = configure_commands(
        source, prefix, copied_source, build, args.jobs, version_flags)
    report = dict(
        schema='semloom.imlane-duckdb-prepare.v1', author=author, version=versions,
        source_archive=file_identity(archive), source_subtree='database/duckdb',
        author_prediction_sources_modified=args.schema_child, converter=converter,
        missing_source_restoration=restoration,
        portability_patch=dict(path='CMakeLists.txt', old=FMT_ORIGINAL, new=FMT_PORTABLE,
            original_sha256=hashlib.sha256(original.encode()).hexdigest(),
            modified_sha256=hashlib.sha256(modified.encode()).hexdigest()),
        tools={name: _version([name, '--version']) for name in ('cmake', 'g++', 'ninja', 'tar')},
        commands=dict(configure=configure, build=compile_library),
        real_model_requests=0, library_built=False)
    write_private_json(workspace / 'prepare.json', report)
    print(shlex.join(configure))
    print(shlex.join(compile_library))
    return 0


def driver_command(source, prefix, duckdb_source, duckdb_build, output, compiler, *, driver_source=None):
    includes = [duckdb_source / 'src/include', prefix / 'include', source / 'include',
        source / 'third_party/nanoarrow/dist/include', source / 'third_party/msgpack/include',
        source / 'third_party/concurrentqueue/include', source / 'third_party/libuv/include',
        source / 'third_party/json/single_include', source / 'third_party/spdlog',
        source / 'third_party/fmt/dist/include', source / 'third_party/boost/dist/include']
    libraries = [duckdb_build / 'src', prefix / 'lib', source / 'third_party/nanoarrow/dist/lib',
                 source / 'third_party/fmt/dist/lib', source / 'third_party/boost/dist/lib']
    return [compiler, '-std=c++17', '-O2', str(driver_source or OWNED / 'q3.cpp'), '-o', str(output),
            *['-I' + str(path) for path in includes], *['-L' + str(path) for path in libraries],
            '-Wl,-rpath,' + ':'.join(map(str, libraries)),
            '-lduckdb', '-ldbendrt_c', '-lfmt', '-lboost_system', '-pthread']


def driver(args):
    source, prefix, author = _author(args.root)
    duckdb_source, duckdb_build, output = (
        path.resolve() for path in (args.duckdb_source, args.duckdb_build, args.output))
    for path in (duckdb_source, duckdb_build, output):
        require_outside_git(path)
    if duckdb_source.is_relative_to(source) or duckdb_build.is_relative_to(source):
        raise ValueError('native DuckDB build/source must use a separate private workspace')
    if output.is_relative_to(prefix) or output.is_relative_to(source):
        raise ValueError('driver output must not write inside the canonical author source/prefix')
    record_path = output.with_suffix('.build.json')
    if output.exists() or record_path.exists():
        raise ValueError('driver build refuses to overwrite a prior result')
    library = duckdb_build / 'src/libduckdb.so'
    library_identity = file_identity(library)
    output.parent.mkdir(parents=True, exist_ok=True)
    command = driver_command(source, prefix, duckdb_source, duckdb_build, output, args.compiler)
    record = dict(schema='semloom.imlane-duckdb-driver-build.v1', author=author,
        command=command, compiler=_version([args.compiler, '--version']),
        duckdb_library=library_identity, driver_source=file_identity(OWNED / 'q3.cpp'),
        duckdb_cmake=file_identity(duckdb_source / 'CMakeLists.txt'),
        cmake_cache=file_identity(duckdb_build / 'CMakeCache.txt'),
        real_model_requests=0, returncode=None, binary=None)
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=args.timeout_seconds)
        record.update(returncode=result.returncode,
            compiler_output=redact_text(result.stdout + result.stderr))
        if result.returncode == 0:
            record['binary'] = file_identity(output)
    except (OSError, subprocess.TimeoutExpired) as error:
        record['error'] = redact_text(str(error))
        try:
            write_private_json(record_path, record)
        except Exception as artifact_error:
            error.add_note('driver build record also failed: ' + redact_text(str(artifact_error)))
        raise
    write_private_json(record_path, record)
    if record['returncode']:
        print(record['compiler_output'], file=sys.stderr)
        return 1
    print(json.dumps(dict(built=True, binary=record['binary'])))
    return 0


def _positive(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError('a positive integer is required')
    return number


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='action', required=True)
    preparation = commands.add_parser('prepare')
    preparation.add_argument('--root', type=Path, required=True)
    preparation.add_argument('--workspace', type=Path, required=True)
    preparation.add_argument('--upstream-source', type=Path, required=True,
                             help='local cache of the three exact files in missing_sources.json')
    preparation.add_argument('--jobs', type=_positive, required=True)
    preparation.add_argument('--schema-child', action='store_true',
        help='explicit engineering variant: pair the output column with its Arrow child schema')
    compilation = commands.add_parser('driver')
    compilation.add_argument('--root', type=Path, required=True)
    compilation.add_argument('--duckdb-source', type=Path, required=True)
    compilation.add_argument('--duckdb-build', type=Path, required=True)
    compilation.add_argument('--output', type=Path, required=True)
    compilation.add_argument('--compiler', default='g++')
    compilation.add_argument('--timeout-seconds', type=_positive, default=120)
    args = parser.parse_args(argv)
    return prepare(args) if args.action == 'prepare' else driver(args)


if __name__ == '__main__':
    raise SystemExit(main())
