"""Verify or run original Movie Q1/Q2/Q3 on the delivered LOTUS executors."""
import argparse
from dataclasses import asdict, fields
import importlib.metadata
import json
import os
from pathlib import Path
import time

from src.baselines.common.private_artifacts import content_digest, new_private_directory, require_outside_git, write_private_json
from src.baselines.common.redact import redact_json_values
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.execution_provider.adapters.model_config import load_fixed_model_config
from src.execution_provider.adapters.ray_map_transport import RayMapConfig
from src.execution_provider.semantic_map import SemanticMapPlan
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.execution_timing import timed_call
from .imlane_query import IMLANE_METHOD_MODES, imlane_config_for_arm, load_imlane_call_configs
from .persistent_native_adapter_query import PersistentAdapterGroup
from .sembench_queries import LOTUS_MOVIE_ARMS, MovieDataset, MovieQuery, MOVIE_MANIFEST_DIGEST
from .supplier_adapter_query import validate_lotus_observation_mode


def _env_path(name, *, private=True):
    if type(name) is not str or not name or not os.environ.get(name):
        raise ValueError('a required SemBench path environment variable is missing')
    path = Path(os.environ[name]).resolve()
    if private:
        require_outside_git(path)
    return path


def load_config(path):
    path = Path(path)
    if path.stat().st_size > 32768:
        raise ValueError('SemBench query configuration exceeds its byte allowance')
    config = json.loads(path.read_text())
    required = {'schema', 'dataset', 'arms', 'queries', 'warmups', 'repeats', 'max_attempts',
                'model_config_env', 'output_env', 'native_options', 'query_timeout_s', 'owner_timeout_s'}
    optional = {'ray_temp_parent_env', 'physical', 'max_held_tasks', 'imlane_config_envs', 'tokenizer_env',
                'arm_native_options', 'lotus_observation_mode'}
    if (type(config) is not dict or not required <= set(config) or set(config)-required-optional
            or config['schema'] != 'semloom.sembench_lotus_queries.v1'):
        raise ValueError('SemBench query configuration has missing or unsupported fields')
    validate_lotus_observation_mode(config.get('lotus_observation_mode', 'qualification'))
    data = config['dataset']
    if (type(data) is not dict or set(data) != {'checkout_env', 'manifest_env', 'csv_env', 'manifest_content_digest'}
            or data['manifest_content_digest'] != MOVIE_MANIFEST_DIGEST):
        raise ValueError('this runner requires the single selected original Movie preparation')
    arms = config['arms']
    if (type(arms) is not list or not 1 <= len(arms) <= 6 or len(set(arms)) != len(arms)
            or any(a not in LOTUS_MOVIE_ARMS for a in arms)
            or type(config['queries']) is not list or not config['queries']
            or len(set(config['queries'])) != len(config['queries'])
            or any(type(q) is not int or q not in (1, 2, 3) for q in config['queries'])):
        raise ValueError('original Movie runs require explicit Q1/Q2/Q3 selections and supported LOTUS arms')
    if (type(config['warmups']) is not int or not 0 <= config['warmups'] <= 10
            or type(config['repeats']) is not int or not 1 <= config['repeats'] <= 200
            or type(config['max_attempts']) is not int or config['max_attempts'] < 1):
        raise ValueError('query repeats and shared attempt budget must be explicitly bounded')
    return config


def arm_options(config, arm):
    """Retain explicit per-executor controls; a low native setting is not raised."""
    overrides = config.get('arm_native_options', {})
    if (type(overrides) is not dict or set(overrides)-set(config['arms'])
            or any(type(value) is not dict for value in overrides.values())):
        raise ValueError('native options may override only explicitly selected arms')
    values = dict(config['native_options'])
    values.update(overrides.get(arm, {}))
    return NativeGraphOptions(**values)


def _imlane_configs(config, *, allow_missing=False):
    configured = config.get('imlane_config_envs', {})
    if type(configured) is not dict or set(configured)-{'cpython', 'ray'}:
        raise ValueError('IMLane configuration must name its two caller-owned modes')
    paths = {}
    for mode, name in configured.items():
        if type(name) is not str or not name:
            raise ValueError('a required SemBench path environment variable is missing')
        if allow_missing and not os.environ.get(name):
            continue
        paths[mode] = _env_path(name)
    return load_imlane_call_configs(paths)


def preflight(config):
    data = config['dataset']
    dataset = MovieDataset(_env_path(data['checkout_env'], private=False), _env_path(data['manifest_env']),
                           _env_path(data['csv_env']), data['manifest_content_digest'])
    evidence = dataset.preflight()
    rows = dataset.load_rows()
    imlane = _imlane_configs(config)
    for arm in config['arms']:
        selected = imlane_config_for_arm(arm, imlane)
        for q in config['queries']:
            MovieQuery(dataset, q).check_executor_shape(arm, rows, imlane_config=selected)
    options = NativeGraphOptions(**config['native_options'])
    per_arm = {arm: arm_options(config, arm) for arm in config['arms']}
    model = load_fixed_model_config(_env_path(config['model_config_env']))
    if not model.endpoint_url.endswith('/chat/completions'):
        raise ValueError('original LOTUS requires an explicit OpenAI chat endpoint')
    if not 0 < config['query_timeout_s'] <= 120 or not config['query_timeout_s']+120 < config['owner_timeout_s'] <= 1800:
        raise ValueError('owner duration must include the existing 120-second cleanup reserve')
    expected = sum(len(MovieQuery(dataset, q).model_rows(rows)) for q in config['queries'])
    expected *= len(config['arms'])*(config['warmups']+config['repeats'])
    if config['max_attempts'] != expected:
        raise ValueError('declared attempt budget must match every scheduled original query')
    physical = None
    if 'lotus-method-semloom' in config['arms']:
        values = config.get('physical')
        if type(values) is not dict or set(values)-{f.name for f in fields(RayMapConfig)}:
            raise ValueError('SemLoom requires explicit existing physical transport options')
        physical = RayMapConfig(**values)
    elif config.get('physical') is not None:
        raise ValueError('SemLoom physical options require its selected execution arm')
    needs_ray = any(a in ('lotus-method-semloom', 'lotus-method-native-ray-data') for a in config['arms'])
    ray_parent = _env_path(config['ray_temp_parent_env']) if needs_ray else None
    if ray_parent is not None and len(str(ray_parent).encode()) > 24:
        raise ValueError('private Ray parent path must leave room for short unique session paths')
    versions = dict(evidence['versions'])
    for name, expected_version in (('daft', '0.7.21'), ('ray', '2.56.1')):
        if name == 'ray' and not needs_ray or name == 'daft' and not any(a in (
                'lotus-method-native-daft', 'lotus-method-semloom') for a in config['arms']):
            continue
        actual = importlib.metadata.version(name)
        if actual != expected_version:
            raise ValueError('native executor library differs from its selected version')
        versions[name] = actual
    author = {}
    for arm in config['arms']:
        call_config = imlane_config_for_arm(arm, imlane)
        if call_config is not None:
            from src.baselines.text.frameworks.imlane_calls import IMLaneCallExecutor, verify_coordinator
            executor = IMLaneCallExecutor(call_config, per_arm[arm])
            try:
                author[arm] = verify_coordinator(call_config, executor.delegate.config)
                author[arm].update(configured_max_batch_rows=call_config.max_batch_rows,
                                  effective_max_batch_rows=executor.max_batch_rows,
                                  bridge_sha256=executor.delegate.config['bridge_sha256'])
            finally:
                executor.close()
    output = _env_path(config['output_env'])
    if output.exists():
        raise ValueError('Movie execution requires a new private output directory')
    tokenizer = _env_path(config['tokenizer_env']) if config.get('tokenizer_env') else None
    evidence.update(versions=versions, arms=config['arms'], queries=config['queries'],
                    lotus_observation_mode=config.get('lotus_observation_mode', 'qualification'),
                    max_attempts=expected, author_runtimes=author,
                    native_options_by_arm={arm: asdict(value) for arm, value in per_arm.items()},
                    config_fingerprint=content_digest(config), model_id=model.model_id,
                    output_exists=output.exists(), preflight_model_requests=0,
                    execution_started=False, model_or_quality_evidence=False)
    return dataset, model, options, physical, ray_parent, imlane, output, tokenizer, evidence


@timed_call('sembench.run', finalize=True)
def run(config):
    dataset, model, options, physical, ray_parent, imlane, output, tokenizer, evidence = preflight(config)
    new_private_directory(output)
    write_private_json(output/'preflight.json', redact_json_values(evidence))
    write_private_json(output/'config.json', config)
    duration = config['owner_timeout_s']*len(config['arms'])+120
    ledger = CellBudgetLedger.create(output/'budget.sqlite', AttemptBudget('sembench-movie', config['max_attempts']),
                                    deadline_utc=time.time()+duration)
    result = dict(schema='semloom.sembench_lotus_matrix.v1', status='failed', cells=[],
                  model_execution='caller-declared external service; inspect separate model evidence',
                  source_identity=evidence, performance_qualified=False)
    try:
        for arm_index, arm in enumerate(config['arms']):
            ray_root = (ray_parent/('s'+str(arm_index)+'-'+str(os.getpid())) if ray_parent is not None
                        and arm in ('lotus-method-semloom', 'lotus-method-native-ray-data') else None)
            with PersistentAdapterGroup((arm,), plan=SemanticMapPlan('original SemBench Movie Filter', model.model_id, 8),
                    model=model, ledger=ledger, root=output/('owner-'+str(arm_index)), options=arm_options(config, arm),
                    physical=physical if arm == 'lotus-method-semloom' else None,
                    ray_temp_root=ray_root, query_timeout_s=config['query_timeout_s'],
                    owner_timeout_s=config['owner_timeout_s'], max_held_tasks=config.get('max_held_tasks'),
                    imlane_call_configs=imlane, tokenizer_path=tokenizer, sembench_dataset=dataset,
                    lotus_observation_mode=config.get('lotus_observation_mode', 'qualification')) as group:
                for phase, repeats in (('warmup', config['warmups']), ('measurement', config['repeats'])):
                    for repeat in range(repeats):
                        for q in config['queries']:
                            identity = f'a{arm_index}-{phase}-{repeat}-q{q}'
                            value = group.run(arm, unit_id=identity, root=output/identity, phase=phase,
                                              sembench_query=MovieQuery(dataset, q))
                            result['cells'].append(dict(unit_id=identity, arm=arm, query_id=q, phase=phase,
                                lotus_observation_mode=value.get('lotus_observation_mode'),
                                repeat=repeat, status=value['status'], actual_posts=value['actual_posts'],
                                full_query_seconds=value['full_query_seconds'],
                                ready_query_seconds=value['execution']['ready_query_seconds'],
                                quality=value['quality']))
                            write_private_json(output/('matrix-progress-'+str(len(result['cells']))+'.json'),
                                               redact_json_values(result))
        result['status'] = 'passed'
    finally:
        result.update(ended_ns=time.monotonic_ns(), budget=ledger.snapshot())
        write_private_json(output/'matrix.json', redact_json_values(result))
    return result


@timed_call('sembench.cli', finalize=True)
def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    actions = parser.add_mutually_exclusive_group(required=True)
    actions.add_argument('--preflight', action='store_true')
    actions.add_argument('--data-preflight', action='store_true')
    actions.add_argument('--execute', action='store_true')
    args = parser.parse_args(argv)
    config = load_config(args.config)
    if args.data_preflight:
        data = config['dataset']
        dataset = MovieDataset(_env_path(data['checkout_env'], private=False), _env_path(data['manifest_env']),
                               _env_path(data['csv_env']), data['manifest_content_digest'])
        value = dataset.preflight()
        rows, coverage = dataset.load_rows(), []
        imlane = _imlane_configs(config, allow_missing=True)
        for arm in config['arms']:
            selected = imlane.get(IMLANE_METHOD_MODES.get(arm))
            for q in config['queries']:
                cell = dict(arm=arm, query_id=q, model_rows=len(MovieQuery(dataset, q).model_rows(rows)))
                if arm in IMLANE_METHOD_MODES:
                    cell.update(configured_max_batch_rows=128 if selected is None else selected.max_batch_rows,
                                capacity_source='legacy default' if selected is None else 'caller-owned configuration')
                try:
                    MovieQuery(dataset, q).check_executor_shape(arm, rows, imlane_config=selected)
                    cell['input_shape'] = 'within selected row allowance; bytes and runtime need full preflight'
                except ValueError as error:
                    cell.update(input_shape='blocked', reason=str(error))
                coverage.append(cell)
        value.update(execution_started=False, model_requests=0, input_shape_coverage=coverage,
                     complete_matrix_ready=not any(c['input_shape']=='blocked' for c in coverage),
                     performance_qualified=False)
    else:
        value = preflight(config)[-1] if args.preflight else run(config)
    print(json.dumps(redact_json_values(value), indent=2, allow_nan=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
