"""Offline finite-choice comparison of complete Movie Map query recordings.

This reads evidence only. It neither runs queries nor changes M1 platform rules.
Hardware/service provenance and independent input selection remain separate audits.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
from statistics import median

from src.baselines.common.private_artifacts import content_digest, write_private_json
from .query_config import QueryConfig

SCHEMA = 'semloom.text_map_comparison.v1'
ROLES = ('pg-http', 'pg-daft-ray', 'pg-source-direct', 'ray-data')
MAIN_ROLES = ('pg-daft-ray', 'pg-source-direct', 'ray-data', 'daft-native')
PROFILES = {'legacy-four-paths': ROLES, 'main': MAIN_ROLES,
            'local-ablation': ('pg-http', 'pg-daft-ray')}


def comparison_roles(specification):
    profile = specification.get('profile', 'legacy-four-paths')
    if profile not in PROFILES:
        raise ValueError('unknown comparison profile')
    return PROFILES[profile]


def checked_json(reference):
    path = Path(reference['path'])
    data = path.read_bytes()
    if hashlib.sha256(data).hexdigest() != reference['sha256']:
        raise ValueError('comparison evidence SHA differs')
    return path, json.loads(data)


def configuration_identity(config):
    # Source relation and row allowance change between tuning and evaluation.
    return content_digest({key: value for key, value in config.items()
        if key not in ('unit_id', 'table', 'max_posts', 'map_transport_config')})


def read_run(reference, expected_role):
    path, summary = checked_json(reference)
    config = QueryConfig(**summary['config'])
    role = ('pg-daft-ray' if config.map_transport_config else 'pg-http') if config.arm == 'pg' else config.arm
    if (role != expected_role or config.task != 'map' or config.movie_id is not None
            or config.organization_config is not None or config.event_content != 'full'):
        raise ValueError('comparison requires the declared full-scan Map execution path')
    if (summary['status'] != 'passed' or summary.get('errors')
            or summary.get('executor_lifecycle') != 'per-query'):
        raise ValueError('failed or differently scoped queries cannot enter comparison')
    if role == 'ray-data':
        runtime = summary['resources'].get('ray_runtime', {})
        if (config.ray_address is None or runtime.get('owner') != 'caller'
                or runtime.get('startup') != 'external' or runtime.get('driver_disconnected') is not True):
            raise ValueError('native Ray comparison requires an external shared runtime and driver cleanup')
    execution = json.loads((path.parent/'q0/execution.json').read_text())
    if (execution != summary['execution'] or execution['status'] != 'completed'
            or execution.get('query_status') != 'completed'):
        raise ValueError('query execution is incomplete or differs from its recording')
    result_path = path.parent/'q0/results.jsonl'
    digest = hashlib.sha256()
    count = size = 0
    with result_path.open('rb') as stream:
        for line in stream:
            digest.update(line)
            count += 1
            size += len(line)
    if (count != execution['recorded_rows'] or count != execution['received_rows']
            or size != execution['recorded_bytes'] or digest.hexdigest() != execution['results_sha256']):
        raise ValueError('query result bytes or counts differ from the completed recording')
    evaluation = summary['evaluation']
    saved_evaluation = json.loads((path.parent/'q0/evaluation.json').read_text())
    if (saved_evaluation.get('status') != 'completed' or saved_evaluation.get('consumed_rows') != count
            or saved_evaluation.get('result') != evaluation):
        raise ValueError('query evaluation differs from its saved recording')
    quality = evaluation['quality']
    fields = ('true_positive', 'false_positive', 'true_negative', 'false_negative', 'invalid')
    if (count < 1 or any(type(quality.get(key)) is not int or quality[key] < 0 for key in fields)
            or sum(quality[key] for key in fields) != count
            or quality['evaluated_rows'] != count or quality['missing_rows'] != 0
            or quality['invalid'] != 0 or evaluation['actual_posts'] != count):
        raise ValueError('comparison requires complete Movie labels and exactly one request per row')
    start, end = summary['query_preparation_started_ns'], execution['t_query_terminal_ns']
    if type(start) is not int or type(end) is not int or not 0 < start < end:
        raise ValueError('complete invocation-to-EOF timing is absent')
    identities = tuple(summary.get(key) for key in
        ('manifest_sha256', 'semantic_reference_sha256', 'model_config_sha256'))
    if any(not isinstance(value, str) or not re.fullmatch('[0-9a-f]{64}', value) for value in identities):
        raise ValueError('source, semantic and model configuration identities are required')
    return dict(unit_id=config.unit_id, summary_sha256=reference['sha256'],
        config_sha256=configuration_identity(summary['config']), identities=identities,
        rows=count, jct_seconds=(end-start)/1e9, quality=quality,
        actual_posts=evaluation['actual_posts'],
        model_usage=evaluation.get('model_usage', dict(status='unavailable', reason='older query report lacks usage summary')),
        http=evaluation.get('http', dict(status='unavailable', reason='query report lacks HTTP occupancy')),
        resources=summary['resources'])


def summarize(specification):
    if specification['schema'] != SCHEMA or specification['stage'] not in ('tuning', 'evaluation'):
        raise ValueError('unsupported text Map comparison specification')
    roles = comparison_roles(specification)
    repeats = specification['repeats']
    if type(repeats) is not int or not 3 <= repeats <= 30:
        raise ValueError('declare between three and thirty measured queries per candidate')
    groups, seen_paths, seen_units, identities, rows = [], set(), set(), set(), set()
    for candidate in specification['candidates']:
        if (candidate['role'] not in roles or not candidate['warmup']
                or len(candidate['measured']) != repeats):
            raise ValueError('each candidate needs declared warmup and complete measured repeats')
        recorded = []
        for reference in candidate['warmup'] + candidate['measured']:
            path = Path(reference['path']).resolve()
            if path in seen_paths:
                raise ValueError('one recording cannot count as independent queries')
            seen_paths.add(path)
            value = read_run(reference, candidate['role'])
            if value['unit_id'] in seen_units:
                raise ValueError('query unit identity was reused')
            seen_units.add(value['unit_id'])
            identities.add(value.pop('identities'))
            rows.add(value['rows'])
            recorded.append(value)
        configs = {value['config_sha256'] for value in recorded}
        if len(configs) != 1:
            raise ValueError('candidate configuration changed between queries')
        measured = recorded[len(candidate['warmup']):]
        groups.append(dict(id=candidate['id'], role=candidate['role'], config_sha256=configs.pop(),
            warmup=recorded[:len(candidate['warmup'])], measured=measured,
            median_jct_seconds=median(value['jct_seconds'] for value in measured),
            min_jct_seconds=min(value['jct_seconds'] for value in measured),
            max_jct_seconds=max(value['jct_seconds'] for value in measured)))
    if (set(g['role'] for g in groups) != set(roles) or len(identities) != 1 or len(rows) != 1
            or len({g['id'] for g in groups}) != len(groups)):
        raise ValueError('declared roles, unique candidates and matched task identities are required')
    selected = None
    if specification['stage'] == 'tuning':
        selected = {role: min((g for g in groups if g['role'] == role),
            key=lambda g: g['median_jct_seconds'])['config_sha256'] for role in roles}
    else:
        _, tuning = checked_json(specification['tuning_result'])
        if (tuning['schema'] != SCHEMA or tuning['stage'] != 'tuning' or len(groups) != len(roles)
                or comparison_roles(tuning) != roles
                or tuning['identities'][0] == next(iter(identities))[0]
                or tuple(tuning['identities'][1:]) != next(iter(identities))[1:]
                or any(tuning['selected'][g['role']] != g['config_sha256'] for g in groups)):
            raise ValueError('evaluation requires identified tuning choices and a different input manifest')
    return dict(schema=SCHEMA, stage=specification['stage'], profile=specification.get('profile','legacy-four-paths'), specification_sha256=content_digest(specification),
        identities=next(iter(identities)), groups=groups, selected=selected,
        selection_scope='lowest measured median per role; ties follow declared candidate order',
        timing='query preparation through observed EOF; shared services outside; query executors inside',
        saturation_proven=False, quality_equivalence_proven=False, performance_qualified=False,
        remaining_audits=['hardware and service provenance', 'data overlap and historical use',
                          'shared service startup, process cleanup and total resource accounting',
                          'predeclared candidate set, run order and request budget'])


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('specification', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    write_private_json(args.output, summarize(json.loads(args.specification.read_text())))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
