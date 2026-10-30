"""Git-synced capture tools; private bindings and original evidence stay outside Git."""
from copy import deepcopy
from pathlib import Path
import argparse
import json
import os
import subprocess
import sys

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
sys.path.insert(0, str(REPO / 'code'))
sys.path.insert(0, str(HERE))

from src.infrastructure.private_artifacts import (
    content_digest, file_identity, new_private_directory, require_outside_git,
    write_private_json,
)
from src.infrastructure.redact import redact_text
import control_resource_C2 as controller

TESTS = ('tests.experiments.test_record_resource_capture',
         'tests.experiments.test_record_resource_handoff',
         'tests.experiments.test_git_record_observation')


def read_private(path):
    path = Path(path).resolve()
    require_outside_git(path)
    return json.loads(path.read_text())


def git_identity(root, expected):
    # Same head/tree/clean-code contract as the existing Git query CLI.
    identity = controller.source_identity(dict(source_root=str(root), expected_source=expected))
    if subprocess.check_output(['git', 'status', '--porcelain', '--untracked-files=all',
                                '--', 'code'], cwd=root, text=True).strip():
        raise ValueError('exact clean Git tools required; no private overlay')
    return identity


def tool_identity(expected):
    tree = subprocess.check_output(['git', 'rev-parse', expected + '^{tree}'],
                                   cwd=REPO, text=True).strip()
    return git_identity(REPO, dict(head=expected, tree=tree))


def runtime_hashes():
    return {name: file_identity(HERE / name)['sha256']
            for name in sorted(controller.RUNTIME_FILES)}


def validate_base(config):
    from src.experiments.attempt_ledger import AttemptBudget
    require_outside_git(Path(config['artifact_root']))
    AttemptBudget(config['run_id'], config['workload']['expected_calls'])
    units = config['units']
    for key, count in units.items():
        AttemptBudget(key, count)
    if not units or sum(units.values()) != config['workload']['expected_calls']:
        raise ValueError('valid units must exactly cover declared calls')
    target = config.get('movie_plan') or config
    if target['engine']['max_model_calls'] != config['workload']['expected_calls']:
        raise ValueError('native call cap must match the declared complete workload')
    for key in ('host_helpers', 'host_config'):
        require_outside_git(Path(config[key]['path']))
        if file_identity(config[key]['path'])['sha256'] != config[key]['sha256']:
            raise ValueError('private host binding changed')
    if config['workload']['kind'] == 'record_chain':
        binding = config['workload']['input']
        require_outside_git(Path(binding['csv']))
        if file_identity(binding['csv'])['sha256'] != binding['sha256']:
            raise ValueError('private input changed')
    elif config['workload']['kind'] != 'movie_session':
        raise ValueError('use an existing Movie session or finite record chain')
    return git_identity(config['source_root'], config['expected_source'])


def qualify(config_path, output_root, expected_tool_commit):
    config = read_private(config_path)
    base_identity = file_identity(config_path)
    tools = tool_identity(expected_tool_commit)
    source = validate_base(config)
    output_root = Path(output_root).resolve()
    new_private_directory(output_root)
    env = {k: v for k, v in os.environ.items() if not k.startswith('SEM_RECORD_')}
    env.update(PYTHONPATH=str(REPO / 'code'), PYTHONDONTWRITEBYTECODE='1',
               CUDA_VISIBLE_DEVICES='', HF_HUB_OFFLINE='1', TRANSFORMERS_OFFLINE='1',
               LITELLM_LOCAL_MODEL_COST_MAP='True')
    commands = [('unit', [sys.executable, '-B', '-m', 'unittest', '-v', *TESTS])]
    probe = REPO / 'code/tests/experiments/probe_record_resource_handoff.py'
    for mode in ('off', 'on'):
        commands.append((mode, [sys.executable, '-B', str(probe), config['source_root'],
                               str(output_root / mode), mode]))
    for label, command in commands:
        result = subprocess.run(command, cwd=REPO / 'code', env=env,
                                text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        write_private_json(output_root / (label + '-run.json'),
            dict(exit_code=result.returncode, output=redact_text(result.stdout)))
        if result.returncode:
            raise RuntimeError('CPU qualification failed; original output retained')
    if (file_identity(config_path) != base_identity
            or tool_identity(expected_tool_commit) != tools
            or validate_base(read_private(config_path)) != source):
        raise ValueError('source or configuration changed during CPU qualification')
    receipt = dict(status='passed_CPU_fixtures', tool_source=tools, public_source=source,
        base_config_sha256=base_identity['sha256'],
        observer_sha256=runtime_hashes(), real_Ray_RPC=False, models=0, GPU=False)
    write_private_json(output_root / 'CPU-READY.json', receipt)
    return receipt


def prepare(base_path, ready_path, output_root, cell_id, mode, profile):
    if cell_id in ('.', '..'):
        raise ValueError('cell identity must name a new artifact')
    base = read_private(base_path)
    ready = read_private(ready_path)
    tools = tool_identity(ready['tool_source']['head'])
    if (ready['status'] != 'passed_CPU_fixtures' or ready['tool_source'] != tools
            or ready['base_config_sha256'] != file_identity(base_path)['sha256']
            or ready['observer_sha256'] != runtime_hashes()
            or ready['public_source'] != validate_base(base)):
        raise ValueError('qualified source, bindings or tools changed')
    config = deepcopy(base)
    config.update(cell_id=cell_id, run_id=cell_id, trace_name=cell_id,
        artifact_root=str(Path(output_root).resolve()), execution_enabled=False,
        tool_source=tools, observer_sha256=runtime_hashes(),
        base_config=dict(path=str(Path(base_path).resolve()),
                         sha256=file_identity(base_path)['sha256']),
        cpu_check=dict(path=str(Path(ready_path).resolve()),
                       sha256=file_identity(ready_path)['sha256']))
    config['observation'] = dict(config['observation'], mode=mode, profile=profile)
    validate_base(config)
    controller.check_material(config)
    new_private_directory(Path(config['artifact_root']))
    write_private_json(Path(config['artifact_root']) / 'PREPARED-CONFIG.json', config)
    return summary(config)


def summary(config):
    return dict(status='configuration_checked_not_runtime_qualified',
        tool_source=config['tool_source'], source=config['expected_source'],
        mode=config['observation']['mode'], profile=config['observation'].get('profile', 'full'),
        declared_calls=config['workload']['expected_calls'], models=0)


def check(config_path):
    config = read_private(config_path)
    if tool_identity(config['tool_source']['head']) != config['tool_source']:
        raise ValueError('Git tools changed')
    validate_base(config)
    controller.check_material(config)
    ready = controller.checked_json(config['cpu_check'])
    base = read_private(config['base_config']['path'])
    if (file_identity(config['base_config']['path'])['sha256'] != config['base_config']['sha256']
            or config['base_config']['sha256'] != ready['base_config_sha256']):
        raise ValueError('qualified base changed')
    def business(value):
        value = deepcopy(value)
        for key in ('cell_id','run_id','trace_name','artifact_root','execution_enabled',
                    'tool_source','observer_sha256','cpu_check','base_config'):
            value.pop(key, None)
        for key in ('mode','profile'):
            value['observation'].pop(key, None)
        return value
    if (ready['tool_source'] != config['tool_source']
            or ready['public_source'] != config['expected_source']
            or business(base) != business(config)):
        raise ValueError('CPU source binding changed')
    return config


def run(config_path):
    config_path = Path(config_path).resolve()
    config = check(config_path)
    root = Path(config['artifact_root']).resolve()
    if config_path != root / 'PREPARED-CONFIG.json':
        raise ValueError('execute only the frozen cell configuration')
    controller.main(str(root))


def original_files(root, excluded=None):
    root = Path(root).resolve()
    require_outside_git(root)
    paths = []
    for path in root.rglob('*'):
        if path.is_symlink():
            raise ValueError('original evidence cannot contain symlinks')
        if path.is_file() and path.resolve() != excluded:
            paths.append(path.relative_to(root).as_posix())
    return sorted(paths)


def originals_manifest(root, output):
    root, output = Path(root).resolve(), Path(output).resolve()
    require_outside_git(output)
    if output.exists():
        raise FileExistsError('never overwrite an original manifest')
    names = original_files(root, output)
    entries = []
    for name in names:
        path = root / name
        before = path.stat()
        identity = file_identity(path)
        after = path.stat()
        if (before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns):
            raise ValueError('original changed while hashing; wait for final receipts')
        entries.append(dict(path=name, **identity))
    if original_files(root, output) != names:
        raise ValueError('original file set changed while hashing')
    value = dict(schema='semloom.record_observation.originals.v1', files=entries,
                 scope='exact listed file bytes; not capture, model or physical-exit qualification')
    value['manifest_sha256'] = content_digest(value)
    write_private_json(output, value)
    return dict(status='original_file_bytes_manifested', files=len(entries),
                bytes=sum(e['bytes'] for e in entries), manifest_sha256=value['manifest_sha256'])


def verify_originals(root, manifest_path):
    root, manifest_path = Path(root).resolve(), Path(manifest_path).resolve()
    value = read_private(manifest_path)
    payload = {k: v for k, v in value.items() if k != 'manifest_sha256'}
    if (value['schema'] != 'semloom.record_observation.originals.v1'
            or content_digest(payload) != value['manifest_sha256']):
        raise ValueError('original manifest changed')
    names = []
    for entry in value['files']:
        name = entry['path']
        path = Path(name)
        if path.is_absolute() or '..' in path.parts or path.as_posix() != name or name in names:
            raise ValueError('invalid or repeated original path')
        names.append(name)
    if sorted(names) != original_files(root, manifest_path):
        raise ValueError('missing or extra original files')
    for entry in value['files']:
        if file_identity(root / entry['path']) != {k: entry[k] for k in ('sha256', 'bytes')}:
            raise ValueError('original bytes changed')
    return dict(status='original_file_bytes_verified', files=len(names),
                bytes=sum(e['bytes'] for e in value['files']), capture_complete=None)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    command = commands.add_parser('qualify', help='CPU fixtures and synthetic public-class probes only')
    command.add_argument('--config', required=True)
    command.add_argument('--output-root', required=True)
    command.add_argument('--tool-commit', required=True)
    command = commands.add_parser('prepare', help='clone existing private bindings without changing work')
    command.add_argument('--config', required=True)
    command.add_argument('--ready', required=True)
    command.add_argument('--output-root', required=True)
    command.add_argument('--cell-id', required=True)
    command.add_argument('--mode', choices=('off', 'on'), required=True)
    command.add_argument('--profile', choices=('handoff', 'full'), required=True)
    for name in ('check', 'run'):
        command = commands.add_parser(name)
        command.add_argument('--config', required=True)
        if name == 'run':
            command.add_argument('--execute-model', action='store_true', required=True)
    command = commands.add_parser('manifest')
    command.add_argument('--root', required=True)
    command.add_argument('--output', required=True)
    command = commands.add_parser('verify')
    command.add_argument('--root', required=True)
    command.add_argument('--manifest', required=True)
    args = parser.parse_args(argv)
    if args.command == 'qualify':
        report = qualify(args.config, args.output_root, args.tool_commit)
    elif args.command == 'prepare':
        report = prepare(args.config, args.ready, args.output_root, args.cell_id, args.mode, args.profile)
    elif args.command == 'check':
        report = summary(check(args.config))
    elif args.command == 'run':
        run(args.config)
        return
    elif args.command == 'manifest':
        report = originals_manifest(args.root, args.output)
    else:
        report = verify_originals(args.root, args.manifest)
    print(json.dumps(report, ensure_ascii=False))


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(json.dumps(dict(status='failed', error_type=type(error).__name__)))
        raise SystemExit(1)
