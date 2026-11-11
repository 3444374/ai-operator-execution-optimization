"""Build or run one zero-model native VARCHAR qualification on Linux.

The existing outer host lease/resource supervisor remains responsible for the
whole worker. Each SQL invocation creates its own coordinator namespace. A
source variant declaration alone does not prove which header built a DSO;
retain the isolated library build log alongside this entry's SHA256 records.
"""

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading

from integrations.imlane_duckdb import build
from src.infrastructure.private_artifacts import (
    file_identity, new_private_directory, read_json, require_outside_git, write_private_json,
)
from src.infrastructure.redact import redact_text

CASES = ('strings', 'empty', 'null', 'offset', 'ownership')
VARIANTS = {'original-author': build.ORIGINAL_CONVERTER_SHA256,
            'schema-child-engineering': build.SCHEMA_CHILD_SHA256}


def require_linux():
    if sys.platform != 'linux':
        raise RuntimeError('native IMLane DSO/SQL qualification requires Linux; no package installation is attempted')


def compile_driver(args):
    require_linux()
    source, prefix, author = build._author(args.author_root)
    duckdb_source, duckdb_build = args.duckdb_source.resolve(), args.duckdb_build.resolve()
    output = args.output.resolve()
    for path in (duckdb_source, duckdb_build, output):
        require_outside_git(path)
    if any(path.is_relative_to(source) or path.is_relative_to(prefix)
           for path in (duckdb_source, duckdb_build, output)):
        raise ValueError('qualification source, build and output must be outside the canonical author tree/prefix')
    header = file_identity(duckdb_source / build.CONVERTER_HEADER)
    if header['sha256'] != VARIANTS[args.variant]:
        raise ValueError('declared source variant does not match the converter header bytes')
    new_private_directory(output)
    report = dict(schema='semloom.imlane.varchar.build.v1', status='failed', author=author,
        declared_source_variant=args.variant, source_header=header, real_model_requests=0,
        library_build_provenance='retain the separate isolated CMake configure/build log')
    try:
        probe = subprocess.check_output([str(args.method_python.absolute()), '-I', '-c',
            'import json,pyarrow as p;print(json.dumps(dict(version=p.__version__,include=p.get_include(),lib=p.get_library_dirs()[0])))'],
            text=True, timeout=15)
        arrow = json.loads(probe)
        if arrow['version'] != '16.1.0':
            raise ValueError('the native author qualification requires its Arrow 16.1.0 ABI')
        library = duckdb_build / 'src/libduckdb.so'
        arrow_library = Path(arrow['lib']) / 'libarrow.so.1601'
        binary = output / 'varchar_qualification'
        command = build.driver_command(source, prefix, duckdb_source, duckdb_build, binary,
            args.compiler, driver_source=build.OWNED / 'varchar_qualification.cpp')
        command.extend(['-I' + arrow['include'], str(arrow_library), '-ldl', '-Wl,-rpath,' + arrow['lib']])
        report.update(command=command, binary_path=str(binary), duckdb_library_path=str(library),
            duckdb_library=file_identity(library), arrow_library=file_identity(arrow_library),
            source=file_identity(build.OWNED / 'varchar_qualification.cpp'),
            fixture_path=str(build.OWNED / 'varchar_fixture.py'),
            fixture=file_identity(build.OWNED / 'varchar_fixture.py'),
            cmake_cache=file_identity(duckdb_build / 'CMakeCache.txt'))
        result = subprocess.run(command, capture_output=True, text=True, timeout=args.timeout_seconds)
        report.update(returncode=result.returncode, compiler_output=redact_text(result.stdout + result.stderr))
        if result.returncode:
            raise RuntimeError('VARCHAR qualification driver compilation failed')
        symbols = subprocess.check_output(['nm', '-C', str(binary)], text=True, timeout=15)
        proof = [line for line in symbols.splitlines() if 'ConverArrowAarrayToVector(' in line]
        if len(proof) != 1 or ' U ' not in proof[0]:
            raise RuntimeError('the converter must be an undefined driver symbol resolved from the shared library')
        report.update(status='built', binary=file_identity(binary), converter_link_proof=proof)
    except BaseException as error:
        report.update(error_type=type(error).__name__, error=redact_text(str(error)))
        raise
    finally:
        write_private_json(output / 'build.json', report)
    return report


def read_build(path):
    evidence = read_json(path)
    if evidence['status'] != 'built':
        raise ValueError('a completed native qualification driver build is required')
    for name in ('binary', 'duckdb_library', 'fixture'):
        if file_identity(Path(evidence[name + '_path'])) != evidence[name]:
            raise ValueError('qualification build artifact changed: ' + name)
    return evidence


def verify_result(path, evidence, *, mode, case, returncode):
    result = read_json(path)
    if (result['schema'] != 'semloom.imlane.varchar.cpu.v1' or result['mode'] != mode
            or result['case'] != case or result['declared_variant'] != evidence['declared_source_variant']):
        raise ValueError('native result identity differs from this qualification')
    resolved = Path(result['converter_resolved_DSO']).resolve()
    if resolved != Path(evidence['duckdb_library_path']).resolve():
        raise ValueError('converter resolved from a different DSO')
    if file_identity(resolved) != evidence['duckdb_library']:
        raise ValueError('resolved DSO bytes changed')
    return dict(native_result=result, native_process_exit_verified=returncode == 0,
                value_contract_passed=result['passed'] is True,
                passed=returncode == 0 and result['passed'] is True)


def run_case(args):
    require_linux()
    evidence = read_build(args.build_record)
    args.output = args.output.resolve()
    new_private_directory(args.output)
    result_path = args.output / 'native.json'
    command = [evidence['binary_path'], args.mode, args.case, str(result_path), evidence['declared_source_variant']]
    report = dict(schema='semloom.imlane.varchar.run.v1', status='failed', mode=args.mode,
        case=args.case, declared_source_variant=evidence['declared_source_variant'], cleanup=[],
        build_record=file_identity(args.build_record), models=0, GPU=False, performance_claim=False)
    try:
        if args.mode == 'converter':
            result = subprocess.run(command, capture_output=True, text=True,
                env=dict(os.environ, CUDA_VISIBLE_DEVICES=''), timeout=args.timeout_seconds)
            report.update(driver_returncode=result.returncode, driver_log=redact_text(result.stdout + result.stderr))
        else:
            from integrations.imlane_duckdb.runtime import DuckDBScope
            from integrations.imlane_records import owner

            args.author_root = args.author_root.resolve()
            args.method_python = args.method_python.absolute()
            args.scopes, args.workload_environment = [], {}
            args.stop_budget = owner.StopBudget(threading.Event())
            previous = {number: signal.signal(number, args.stop_budget.signal)
                        for number in (signal.SIGINT, signal.SIGTERM)}
            scope = None
            try:
                kernel = build.OWNED / 'varchar_fixture.py'
                with DuckDBScope(args, report, kernel=kernel) as scope:
                    command.extend([args.kind, str(kernel)])
                    process, writer = owner.spawn(command, scope.env, args.output / 'driver.log')
                    scope.children.append((process, writer))
                    scope.track_root(process)
                    report['driver_returncode'] = process.wait(timeout=args.timeout_seconds)
                    writer.join(1)
                    scope.check()
            finally:
                for number, handler in previous.items():
                    signal.signal(number, handler)
                if scope is not None:
                    report['process_and_namespace_cleanup'] = owner.resource_state(scope)
        report.update(verify_result(result_path, evidence, mode=args.mode, case=args.case,
                                    returncode=report['driver_returncode']))
        report['status'] = 'passed' if report['passed'] else 'failed'
    except BaseException as error:
        report.update(error_type=type(error).__name__, error=redact_text(str(error)))
        if result_path.exists():
            report['native_result'] = read_json(result_path)
    finally:
        report['command'] = command
        write_private_json(args.output / 'qualification.json', report)
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_subparsers(dest='action', required=True)
    compile_parser = actions.add_parser('build')
    for name in ('author-root', 'method-python', 'duckdb-source', 'duckdb-build', 'output'):
        compile_parser.add_argument('--' + name, type=Path, required=True)
    compile_parser.add_argument('--variant', choices=tuple(VARIANTS), required=True)
    compile_parser.add_argument('--compiler', default='g++')
    compile_parser.add_argument('--timeout-seconds', type=build._positive, default=120)
    run = actions.add_parser('run')
    for name in ('build-record', 'output'):
        run.add_argument('--' + name, type=Path, required=True)
    run.add_argument('--mode', choices=('converter', 'sql'), required=True)
    run.add_argument('--case', choices=CASES, required=True)
    run.add_argument('--kind', choices=('process', 'thread'))
    run.add_argument('--author-root', type=Path)
    run.add_argument('--method-python', type=Path)
    run.add_argument('--timeout-seconds', type=build._positive, default=60)
    args = parser.parse_args(argv)
    if args.action == 'run' and args.mode == 'sql' and any(
            getattr(args, name) is None for name in ('kind', 'author_root', 'method_python')):
        parser.error('SQL qualification requires --kind, --author-root and --method-python')
    result = compile_driver(args) if args.action == 'build' else run_case(args)
    print(json.dumps(dict(status=result['status'], models=0, GPU=False)))
    return 0 if result['status'] in ('built', 'passed') else 1


if __name__ == '__main__':
    raise SystemExit(main())
