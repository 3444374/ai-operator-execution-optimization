"""Execute one declared comparison stage using existing supervised query workers.

Services, immutable source installation and a fresh finite ledger belong to the
caller. This module never starts the next stage, retries, or replenishes quota.
"""
from dataclasses import asdict, replace
import json
import math
import os
from pathlib import Path
import re
import sys
import time

from src.baselines.common.private_artifacts import content_digest, new_private_directory, write_private_json
from src.baselines.common.redact import redact_text
from src.execution_provider.adapters.model_config import load_fixed_model_config
from src.execution_provider.adapters.ray_map_transport import RayMapConfig
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from .query_config import QueryConfig
from .query_supervisor import supervise
from .query_workloads import file_identity, load_manifest, read_prepared
from .text_map_comparison import SCHEMA as COMPARISON_SCHEMA, ROLES, comparison_roles, checked_json, configuration_identity, read_run, summarize

SCHEMA = 'semloom.text_map_campaign.v1'


def preflight(spec):
    """Validate local identities, complete schedules and exact POST totals only."""
    if spec['schema'] != SCHEMA or spec['stage'] not in ('qualification', 'tuning', 'evaluation'):
        raise ValueError('unsupported comparison stage')
    if type(spec['max_seconds']) not in (int, float) or not math.isfinite(spec['max_seconds']) or not 30 <= spec['max_seconds'] <= 3600:
        raise ValueError('stage time must be between thirty seconds and one hour')
    manifest_path, _ = checked_json(spec['manifest'])
    manifest = load_manifest(manifest_path)
    if manifest['kind'] != 'movie':
        raise ValueError('first comparison requires Movie-derived Map inputs')
    for name in ('raw.jsonl', 'references.jsonl'):
        # Verify the complete file identity before any model request.
        list(read_prepared(manifest_path, name))
    model_path, _ = checked_json(spec['model'])
    model = load_fixed_model_config(model_path)
    _, installation = checked_json(spec['installation'])
    if (installation['manifest_sha256'] != manifest['sha256'] or installation['rows'] != manifest['rows']
            or installation['source_sha256'] != manifest['files']['raw.jsonl']['sha256']
            or installation['immutable'] is not True):
        raise ValueError('source installation receipt does not match the prepared input')
    _, environment = checked_json(spec['environment'])
    if environment['status'] != 'ok':
        raise ValueError('environment preflight did not pass')
    if not re.fullmatch('[0-9a-f]{64}', spec['service_signature']):
        raise ValueError('identified fixed service configuration required')
    required_roles = comparison_roles(spec)
    configs, roles, transports, ray_settings = {}, {}, [], set()
    for candidate in spec['candidates']:
        key, role = candidate['id'], candidate['role']
        if not isinstance(key, str) or not re.fullmatch('[A-Za-z0-9_-]{1,24}', key) or key in configs or role not in required_roles:
            raise ValueError('unique short candidate identities and supported roles required')
        cfg = QueryConfig(**candidate['config'])
        actual_role = ('pg-daft-ray' if cfg.map_transport_config else 'pg-http') if cfg.arm == 'pg' else cfg.arm
        if (actual_role != role or cfg.task != 'map' or cfg.movie_id is not None
                or cfg.organization_config is not None or cfg.event_content != 'full'
                or cfg.table != installation['table'] or cfg.max_posts != manifest['rows']):
            raise ValueError('candidate does not describe the matched complete Map task')
        if cfg.arm == 'pg' and cfg.concurrency > cfg.window:
            raise ValueError('declared PG request capacity exceeds its row window')
        if cfg.map_transport_config:
            path = Path(cfg.map_transport_config)
            if file_identity(path)['sha256'] != cfg.map_transport_sha256:
                raise ValueError('transport file changed before the stage')
            physical = RayMapConfig.load(path)
            if physical.workers > cfg.concurrency or physical.batch_rows > cfg.concurrency:
                raise ValueError('Ray Map worker/batch settings exceed the declared request capacity')
            transports.append(physical.address)
        if role == 'ray-data':
            if cfg.ray_address is None:
                raise ValueError('matched native Ray requires an explicit caller-owned runtime')
            if spec.get('profile')=='main' and (cfg.ray_batch_rows!=1
                    or cfg.ray_actors*cfg.ray_async_batches_per_actor!=cfg.concurrency
                    or cfg.ray_read_blocks<cfg.ray_actors):
                raise ValueError('main Ray candidates require explicit async capacity and sufficient source blocks')
            ray_settings.add((cfg.ray_address, cfg.ray_num_cpus, cfg.ray_object_store_bytes))
        configs[key], roles[key] = cfg, role
    if set(roles.values()) != set(required_roles) or len(ray_settings) != (1 if 'ray-data' in required_roles else 0):
        raise ValueError('declared profile roles and Ray runtime are required')
    address = next(iter(ray_settings))[0] if ray_settings else None
    if address is not None and any(value != address for value in transports):
        raise ValueError('both Ray paths must use the same declared cluster')
    if spec.get('profile')=='main':
        cpus=next(iter(ray_settings))[1]
        if any(cfg.daft_num_threads!=cpus for key,cfg in configs.items() if roles[key]=='daft-native'):
            raise ValueError('main native CPU declarations differ')
        if spec['stage']=='tuning':
            capacities=[sorted(cfg.concurrency for key,cfg in configs.items() if roles[key]==role)
                        for role in required_roles]
            if any(values!=capacities[0] for values in capacities[1:]):
                raise ValueError('main tuning requires equal finite request-capacity opportunities')
    orders = spec['orders']
    expected_rounds = 1 if spec['stage'] == 'qualification' else spec['repeats']+1
    if (type(spec['repeats']) is not int or not 3 <= spec['repeats'] <= 30
            or len(orders) != expected_rounds
            or any(len(order) != len(configs) or set(order) != set(configs) for order in orders)):
        raise ValueError('declare one qualification round or a warmup plus complete paired repeats')
    if spec['stage'] in ('qualification', 'evaluation') and len(configs) != len(required_roles):
        raise ValueError('qualification/evaluation use exactly one candidate per declared role')
    total = manifest['rows']*len(configs)*len(orders)
    if type(spec['max_posts']) is not int or spec['max_posts'] != total:
        raise ValueError('POST allowance must equal all declared rows, candidates and rounds')
    if spec['stage'] == 'evaluation':
        _, selected = checked_json(spec['tuning_result'])
        if (selected['schema'] != COMPARISON_SCHEMA or selected['stage'] != 'tuning'
                or selected['identities'][0] == manifest['sha256']
                or selected['identities'][2] != spec['model']['sha256']
                or selected.get('service_signature') != spec['service_signature']
                or comparison_roles(selected) != required_roles
                or any(selected['selected'][roles[key]] != configuration_identity(asdict(cfg)) for key, cfg in configs.items())):
            raise ValueError('evaluation must use identified tuning choices and a different input')
    return dict(schema=SCHEMA, stage=spec['stage'], specification_sha256=content_digest(spec),
        rows=manifest['rows'], manifest_sha256=manifest['sha256'], model_id=model.model_id,
        model_config_sha256=spec['model']['sha256'], service_signature=spec['service_signature'],
        queries=len(configs)*len(orders), max_posts=total,
        candidates=[dict(id=key, role=roles[key], config_sha256=configuration_identity(asdict(cfg))) for key,cfg in configs.items()],
        input_independence='requires historical-use and overlap audit; differing manifests are insufficient',
        database_contacted=False, model_requests=0)


def run_stage(path):
    spec = json.loads(Path(path).read_text())
    checked = preflight(spec)
    if not os.environ.get(spec['dsn_env']):
        raise ValueError('source database environment variable is unset')
    ledger = CellBudgetLedger(Path(spec['budget_path']), AttemptBudget(spec['budget_id'], spec['max_posts']))
    before = ledger.snapshot()
    if (before['allocated_requests'] != 0 or before['units']
            or not 0 < before['deadline_utc']-time.time() <= spec['max_seconds']):
        raise ValueError('stage requires a fresh existing ledger within its declared duration')
    root = Path(spec['output_root'])
    new_private_directory(root)
    write_private_json(root/'preflight.json', checked)
    write_private_json(root/'specification.json', spec)
    deadline = min(time.monotonic()+spec['max_seconds'], time.monotonic()+before['deadline_utc']-time.time())
    candidates = {v['id']: v for v in spec['candidates']}
    collected = {key: dict(id=key, role=v['role'], warmup=[], measured=[]) for key,v in candidates.items()}
    completed = []
    try:
        for repeat, order in enumerate(spec['orders']):
            for key in order:
                remaining = deadline-time.monotonic()-15  # Reserve three five-second cleanup waits.
                if remaining <= 0:
                    raise TimeoutError('stage has no remaining query and cleanup time')
                # Files remain private; changes after preflight stop the next query.
                checked_json(spec['model'])
                unit = f'{key}-r{repeat}'
                config = replace(QueryConfig(**candidates[key]['config']), unit_id=unit)
                config_path = root/(unit+'.json')
                write_private_json(config_path, asdict(config))
                output = root/unit
                new_private_directory(output)
                command = [sys.executable, '-m', 'src.experiments.postgresql.query_cli', 'run', '--worker',
                    '--config', str(config_path), '--manifest', spec['manifest']['path'], '--model', spec['model']['path'],
                    '--budget', spec['budget_path'], '--budget-id', spec['budget_id'], '--max-attempts', str(spec['max_posts']),
                    '--dsn-env', spec['dsn_env'], '--pg-log', spec['pg_log'], '--output', str(output)]
                supervise(command, output, lambda: ledger.close_shared_unit(unit), query_timeout_s=config.query_timeout_s,
                          max_duration_s=remaining, grace_s=5)
                summary_path = output/'unit/summary.json'
                ref = dict(path=str(summary_path), sha256=file_identity(summary_path)['sha256'])
                record = read_run(ref, candidates[key]['role'])
                if (record['identities'][0] != checked['manifest_sha256'] or record['rows'] != checked['rows']
                        or record['identities'][2] != spec['model']['sha256']):
                    raise ValueError('executed query differs from declared stage identities')
                completed.append(dict(unit=unit, role=candidates[key]['role'], summary=ref, actual_posts=record['actual_posts']))
                collected[key]['warmup' if repeat == 0 else 'measured'].append(ref)
                write_private_json(root/f'completed-{len(completed):03}.json', completed[-1])
        comparison = dict(schema=COMPARISON_SCHEMA, stage=spec['stage'], repeats=spec['repeats'],
                          profile=spec.get('profile','legacy-four-paths'),
                          candidates=list(collected.values()))
        if spec['stage'] == 'evaluation':
            comparison['tuning_result'] = spec['tuning_result']
        result = dict(status='completed', performance_qualified=False)
        if spec['stage'] != 'qualification':
            write_private_json(root/'comparison-input.json', comparison)
            result.update(summarize(comparison))
        result.update(campaign_sha256=checked['specification_sha256'], service_signature=spec['service_signature'],
                      completed=completed, budget=ledger.snapshot(), next_stage_started=False)
        write_private_json(root/'result.json', result)
        return result
    except BaseException as error:
        write_private_json(root/'failure.json', dict(status='failed', error_type=type(error).__name__,
            error=redact_text(str(error)), completed=completed, budget=ledger.snapshot(), next_stage_started=False))
        raise


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('specification', type=Path)
    parser.add_argument('--preflight', action='store_true')
    args = parser.parse_args()
    if args.preflight:
        print(json.dumps(preflight(json.loads(args.specification.read_text())), indent=2))
    else:
        run_stage(args.specification)
