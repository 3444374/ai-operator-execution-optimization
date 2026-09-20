"""Explicit M1 screening or evaluation, reusing existing query execution owners.

A stage never starts another stage, initializes a budget, or starts a service.
Legacy fixed-matrix configurations are rejected before connecting to PostgreSQL.
"""
from contextlib import ExitStack
from dataclasses import asdict
import hashlib
import json
import math
import os
from pathlib import Path
import time

from src.baselines.common.private_artifacts import new_private_directory, write_private_json
from src.baselines.common.redact import redact_text
from src.baselines.text.sembench_movie import MOVIE_MAP_INSTRUCTION
from src.execution_provider.semantic_map import SemanticMapPlan, MAX_OUTPUT_BYTES, MAX_INPUT_BYTES
from src.execution_provider.adapters.map_organization import MapOrganizationConfig
from src.execution_provider.adapters.model_config import MAX_MODEL_RESPONSE_BYTES, load_fixed_model_config
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.process_sampling import ProcessSampler
from .persistent_gateway import PersistentMapGateway
from .query_config import QueryConfig
from .query_inputs import QueryInputs
from .query_tables import install_input_table
from .query_workloads import load_manifest, read_prepared
from .query_runner import run_query
from .query_evaluation import read_events
from .waiting_positions import analyze_waiting_positions
from .m1_selection import select_capacity, paired_comparison, supply_observation, validate_policy

SCHEMA = 'semloom.m1_campaign.v2'
RESOURCE_FIELDS = {'window', 'input_bytes', 'result_bytes', 'pg_window_bytes', 'pg_staging_bytes'}


def configuration(c, group, table, *, organization_path=None):
    options = {}
    if organization_path is not None:
        options = dict(organization_config=str(organization_path),
                       organization_sha256=hashlib.sha256(organization_path.read_bytes()).hexdigest())
    return QueryConfig(group['id'], group['arm'], 'map', table,
        concurrency=group['capacity'], pg_total_budget=group['arm'] == 'pg',
        query_timeout_s=c['query_timeout_s'], event_content=group['event_content'],
        **c['resources'], **options)


def reachability(config, manifest, plan):
    """Conservative byte proof for the fixed five-column Movie full scan.

The PG estimate follows semloom_window_row_allocation_bound: twice materialized
bytes plus a 64 KiB context allowance. 4 KiB covers slot/attribute metadata for
this fixed shape. Actual EXPLAIN/window usage still needs runtime verification.
"""
    empty = len(json.dumps(dict(model=plan.model_id, messages=[{'role':'system','content':plan.instruction},
        {'role':'user','content':''}], **plan.generation_constraints()), ensure_ascii=False).encode())
    request_bytes = empty + 6 * manifest['max_input_bytes']
    pg_row_bytes = 2 * (manifest['max_source_bytes'] + 4096 + manifest['max_input_bytes']
                      + request_bytes + 2 * 257 + 4 + MAX_OUTPUT_BYTES) + 65536
    pg_staging = 2 * (manifest['max_source_bytes'] + 4096) + 65536
    common = dict(client_http=config.concurrency, source_rows=manifest['rows'])
    if config.arm == 'pg':
        common.update(core_held=config.window, core_results=config.result_bytes // MAX_MODEL_RESPONSE_BYTES,
                      core_input=config.input_bytes // request_bytes, pg_window=config.window,
                      pg_retained=config.pg_window_bytes // pg_row_bytes)
    possible = min(common.values())
    blockers = [key for key, value in common.items() if value < config.concurrency]
    if manifest['max_input_bytes'] > MAX_INPUT_BYTES or request_bytes > 1048576:
        blockers.append('single_input_or_conversion_limit')
    if config.arm == 'pg' and config.pg_staging_bytes < pg_staging:
        blockers.append('pg_staging')
    return dict(status='allowed_by_static_estimate' if not blockers else 'unreachable',
        configured_capacity=config.concurrency, guaranteed_items=possible, limits=common, blockers=blockers,
        request_bytes_upper=request_bytes, pg_row_bytes_upper=pg_row_bytes,
        pg_staging_bytes_upper=pg_staging, result_bytes_per_task=MAX_MODEL_RESPONSE_BYTES,
        runtime_supply_proven=False, backend_client='existing transport sized to query capacity',
        service_running_limit='external service signature; not equated with client in-flight count')


def preflight(c):
    """Read local identities only; no PG, model, tokenizer, or budget operations."""
    if c.get('schema') != SCHEMA:
        raise ValueError('legacy M1 matrix retired; explicit v2 stage and schedule required')
    if c['stage'] not in ('screening', 'strategy-tuning', 'evaluation', 'observation'):
        raise ValueError('unknown M1 stage')
    validate_policy(c['selection_policy'])
    if set(c['resources']) != RESOURCE_FIELDS:
        raise ValueError('one shared resource configuration required for all groups')
    if c['split'] != ('evaluation' if c['stage'] in ('evaluation', 'observation') else 'tuning'):
        raise ValueError('tuning and independent evaluation inputs must stay separate')
    for key in ('max_seconds', 'query_timeout_s'):
        if type(c[key]) not in (int, float) or not math.isfinite(c[key]) or c[key] <= 0:
            raise ValueError('finite positive stage and query deadlines required')
    if c['query_timeout_s'] >= c['max_seconds']:
        raise ValueError('stage must leave time beyond one query')
    if not isinstance(c['service_signature'], str) or not c['service_signature']:
        raise ValueError('explicit service identity required')
    manifests = {key: load_manifest(Path(c['inputs_root']) / key / 'manifest.json') for key in ('tuning', 'evaluation')}
    for key, manifest in manifests.items():
        if manifest['kind'] != 'movie' or manifest['sha256'] != c['manifest_sha256'][key]:
            raise ValueError('Movie input identity differs')
    if manifests['tuning']['sha256'] == manifests['evaluation']['sha256']:
        raise ValueError('evaluation must use a different prepared partition')
    manifest = manifests[c['split']]
    plan = SemanticMapPlan(MOVIE_MAP_INSTRUCTION, c['model_id'], 128)
    if not 1 <= len(c['groups']) <= 16 or not 1 <= len(c['orders']) <= 32:
        raise ValueError('stage exceeds finite group or repeat count')
    groups = {g['id']: g for g in c['groups']}
    if len(groups) != len(c['groups']):
        raise ValueError('duplicate group identity')
    for order in c['orders']:
        if len(order) != len(groups) or set(order) != set(groups):
            raise ValueError('each predeclared round must contain every group exactly once')
    if len(c['orders']) - 1 < c['selection_policy']['min_repeats']:
        raise ValueError('warmup plus sufficient paired measurement rounds required')
    checks = {}
    work = None
    if any(g['control'] == 'token' for g in groups.values()):
        path = Path(c['inputs_root']) / c['split'] / 'work.json'
        if hashlib.sha256(path.read_bytes()).hexdigest() != c['work_sha256'][c['split']]:
            raise ValueError('precomputed work identity differs')
        work = json.loads(path.read_text())['work']
        if len(work) != manifest['rows'] or any(type(w) is not int or not 0 < w <= c['context_tokens'] for w in work):
            raise ValueError('complete request work differs from fixed context or row count')
    for key, group in groups.items():
        cfg = configuration(c, group, 'm1_preflight')
        if group['arm'] not in ('pg', 'pg-source-direct') or group['control'] not in ('request', 'token'):
            raise ValueError('M1 only supports FIFO request and PG token controls')
        if c['stage'] == 'screening' and group['control'] != 'request':
            raise ValueError('capacity screening does not tune work limits')
        if group['control'] == 'token':
            if group['arm'] != 'pg':
                raise ValueError('direct control must retain its own execution')
            organization(c, group, work)
        elif 'work_limit' in group:
            raise ValueError('request-count control must not carry a work limit')
        checks[key] = reachability(cfg, manifest, plan)
        if group['control'] == 'token':
            maximum = sum(sorted(work, reverse=True)[:cfg.concurrency])
            checks[key]['work'] = dict(limit=group['work_limit'], maximum_at_configured_capacity=maximum,
                nonbinding_for_described_input=group['work_limit'] >= maximum,
                context_tokens=c['context_tokens'])
    if c['stage'] == 'screening':
        paths = {arm: sorted(g['capacity'] for g in groups.values() if g['arm'] == arm)
                 for arm in ('pg', 'pg-source-direct')}
        if paths['pg'] != paths['pg-source-direct'] or len(paths['pg']) < 3 or len(set(paths['pg'])) != len(paths['pg']):
            raise ValueError('screening requires the same three or more distinct capacities on both paths')
    if c['stage'] != 'screening' and c.get('reference_group') not in groups:
        raise ValueError('comparison requires a declared reference group')
    pg_modes = {g['event_content'] for g in groups.values() if g['arm'] == 'pg'}
    if c['stage'] != 'observation' and len(pg_modes) > 1:
        raise ValueError('log modes may vary only in a separate observation stage')
    if c['stage'] == 'strategy-tuning' and any(g['control'] == 'token' and g['event_content'] != 'full' for g in groups.values()):
        raise ValueError('work tuning requires actual work-block observations')
    if c['stage'] == 'strategy-tuning':
        if any(g['arm'] != 'pg' for g in groups.values()) or groups[c['reference_group']]['control'] != 'request':
            raise ValueError('work tuning must face a request-count reference')
        guards = {g['capacity'] for g in groups.values() if g['control'] == 'token'}
        if len(guards) != 1:
            raise ValueError('work tuning varies W at one fixed high C')
        source = Path(c['screening_source'])
        if hashlib.sha256(source.read_bytes()).hexdigest() != c['screening_sha256']:
            raise ValueError('capacity screening identity differs')
        screening = json.loads(source.read_text())
        if (screening.get('status') != 'completed' or screening.get('stage') != 'screening'
                or screening.get('service_signature') != c['service_signature']
                or screening.get('manifest_sha256') != c['manifest_sha256']
                or screening.get('resources') != c['resources']
                or screening.get('selection_policy') != c['selection_policy']):
            raise ValueError('work tuning requires matched completed screening')
        selection = screening['selection']['pg']
        guard = next(iter(guards))
        if (selection['status'] != 'platform_candidate'
                or groups[c['reference_group']]['capacity'] != selection['selected']
                or guard < selection['selected'] or str(guard) not in selection['median_rates']
                or not any(g['control'] == 'request' and g['capacity'] == guard for g in groups.values())):
            raise ValueError('work tuning requires the selected request reference and a measured high-C control')
    total = manifest['rows'] * len(groups) * len(c['orders'])
    if type(c['max_posts']) is not int or c['max_posts'] != total:
        raise ValueError(f'POST allocation must equal {total} for this explicit stage, including warmups')
    if c['stage'] in ('evaluation', 'observation'):
        # Evaluation cannot silently inherit the old matrix or choose using holdout outcomes.
        source = Path(c['selection_source'])
        if hashlib.sha256(source.read_bytes()).hexdigest() != c['selection_sha256']:
            raise ValueError('independent tuning decision identity differs')
        decision = json.loads(source.read_text())
        expected = dict(groups=c['groups'], resources=c['resources'], selection_policy=c['selection_policy'],
                        service_signature=c['service_signature'], manifest_sha256=c['manifest_sha256'])
        if decision.get('status') != 'ready_for_independent_evaluation' or decision.get('evaluation_design') != expected:
            raise ValueError('evaluation design must match the recorded tuning decision')
    return dict(schema=SCHEMA, stage=c['stage'], split=c['split'], expected_posts=total,
        queries=len(groups) * len(c['orders']), checks=checks,
        status='ready' if all(v['status'] == 'allowed_by_static_estimate' for v in checks.values()) else 'unreachable',
        manifests=manifests, work=work, service_signature=c['service_signature'],
        real_execution_authorized=False)


def organization(c, group, work):
    wide = sum(sorted(work, reverse=True)[:c['resources']['window']])
    return MapOrganizationConfig('rows', c['resources']['window'], c['resources']['window'], wide,
        group['work_limit'], c['model_id'], c['model_revision'], c['service_signature'],
        c['tokenizer_path'], c['tokenizer_fingerprint'], c['context_tokens'])


def query_record(summary, output, group, *, repeat, expected_rows, policy, window):
    execution, evaluation = summary['execution'], summary['evaluation']
    started, ended = summary['query_preparation_started_ns'], execution['t_query_terminal_ns']
    record = dict(group=group['id'], arm=group['arm'], control=group['control'], capacity=group['capacity'],
        repeat=repeat, warmup=repeat == 0, status=summary['status'], expected_rows=expected_rows,
        completed_rows=execution['recorded_rows'], actual_posts=evaluation['actual_posts'],
        manifest_sha256=summary['manifest_sha256'], quality=evaluation['quality'],
        jct_seconds=(ended-started)/1e9, sql_or_stream_seconds=execution['query_jct_seconds'],
        results_sha256=execution['results_sha256'], evaluation=evaluation,
        resources=summary['resources'], event_content=group['event_content'])
    events = read_events(output/'events.jsonl')
    from .m1_measurement import retained_resource_areas, output_identity, token_usage
    record['resource_areas'] = retained_resource_areas(events, output/'query-rss.jsonl')
    record['output_values'] = output_identity(output/'q0/results.jsonl')
    record['token_usage'] = token_usage(events, expected_rows)
    record['supply'] = supply_observation(events, start_ns=started, end_ns=ended,
        capacity=group['capacity'], policy=policy)
    record['throughput_rows_per_second'] = record['completed_rows']/record['jct_seconds']
    record['waiting'] = dict(status='unavailable', reason='no PG stage trace on this path/observation mode')
    if group['arm'] == 'pg' and group['event_content'] == 'full':
        write_private_json(output/'waiting-contract.json', dict(eligibility='sealed-immutable-full-scan-at-invocation',
            manifest_sha256=record['manifest_sha256'], role='warmup' if repeat == 0 else 'measurement'))
        record['waiting'] = analyze_waiting_positions(output, expected_rows=expected_rows, window=window)
    record['service_metrics'] = dict(status='unavailable', reason='attach independently sampled deployed-service metrics; HTTP active is not vLLM running')
    org = evaluation.get('organization')
    if org and org['submitted_sequences'] != list(range(expected_rows)):
        raise ValueError('FIFO actual submission order changed')
    if (record['status'] != 'passed' or record['completed_rows'] != expected_rows
            or record['actual_posts'] != expected_rows or record['quality']['invalid'] != 0):
        raise ValueError('invalid or incomplete query; preserve evidence and stop stage')
    return record


def summarize(c, rows):
    measured = {g['id']: [r for r in rows if r['group'] == g['id'] and not r['warmup']] for g in c['groups']}
    result = dict(status='completed', stage=c['stage'],
                  service_signature=c.get('service_signature'), manifest_sha256=c.get('manifest_sha256'),
                  resources=c.get('resources'), selection_policy=c['selection_policy'], rows=rows, actual_posts=sum(r['actual_posts'] for r in rows),
                  next_stage_started=False, performance_qualified=False)
    if c['stage'] == 'screening':
        result['selection'] = {}
        for arm in ('pg', 'pg-source-direct'):
            samples = {g['capacity']: measured[g['id']] for g in c['groups'] if g['arm'] == arm}
            result['selection'][arm] = select_capacity(samples, capacities=sorted(samples), policy=c['selection_policy'])
        result['direct_scope'] = 'matched PG-source bounded direct; includes SQL reading and per-query client setup, not a pure service ceiling'
    else:
        result['paired_comparisons'] = {g['id']: paired_comparison(measured[g['id']], measured[c['reference_group']],
            policy=c['selection_policy']) for g in c['groups'] if g['id'] != c['reference_group']}
        if c['stage'] == 'strategy-tuning':
            eligible = []
            for group in c['groups']:
                if group['control'] != 'token' or group['id'] == c['reference_group']:
                    continue
                comparison = result['paired_comparisons'][group['id']]
                triggered = all(r['evaluation'].get('organization', {}).get('compute_lifecycle', {}).get(
                    'work_only_block_count', 0) > 0 for r in measured[group['id']])
                if triggered and comparison['status'] == 'within_declared_loss_in_all_pairs':
                    eligible.append(group)
            result['work_candidate'] = min(eligible, key=lambda g: g['work_limit'])['id'] if eligible else None
            result['work_candidate_scope'] = 'smallest measured W with observed blocking and no declared paired throughput loss; independent evaluation required'
        result['interpretation'] = 'paired throughput and quality/resource records; latency or configured limits alone do not establish a benefit'
    return result


def run_campaign(config_path):
    """Run one explicitly authorized stage using an already active finite ledger."""
    c = json.loads(Path(config_path).read_text())
    check = preflight(c)
    root = Path(c['output_root'])
    new_private_directory(root)
    write_private_json(root/'preflight.json', check)
    write_private_json(root/'resolved-config.json', c)
    if check['status'] != 'ready':
        raise ValueError('unreachable supply configuration; no PG or model operations performed')
    model_path = Path(c['model_config'])
    if load_fixed_model_config(model_path).model_id != c['model_id']:
        raise ValueError('model identity differs')
    budget = AttemptBudget(c['budget_id'], c['max_posts'])
    ledger = CellBudgetLedger(c['budget_path'], budget)
    snapshot = ledger.snapshot()
    if snapshot['allocated_requests'] or not 0 < snapshot['deadline_utc']-time.time() <= c['max_seconds']:
        raise ValueError('stage requires an unused existing allocation with the declared bounded deadline')
    deadline = time.monotonic() + min(c['max_seconds'], snapshot['deadline_utc']-time.time())
    split = c['split']; manifest = check['manifests'][split]
    inputs_root = Path(c['inputs_root'])/split
    table = 'm1_'+hashlib.sha256(str(root).encode()).hexdigest()[:16]
    rows = []
    def time_remaining():
        if time.monotonic() + c['query_timeout_s'] >= deadline:
            raise TimeoutError('insufficient remaining stage time for the next query')
    try:
        time_remaining()
        import psycopg
        with psycopg.connect(os.environ['SEMLOOM_TEST_PG_DSN'], autocommit=True) as connection:
            install_input_table(connection, QueryInputs('movie', table, manifest['rows'],
                manifest['max_source_bytes'], manifest['max_input_bytes']), read_prepared(inputs_root/'manifest.json', 'raw.jsonl'))
        plan = SemanticMapPlan(MOVIE_MAP_INSTRUCTION, c['model_id'], 128)
        with ExitStack() as stack:
            groups, configs = {}, {}
            for group in c['groups']:
                time_remaining()
                path = None
                if group['control'] == 'token':
                    path = root/(group['id']+'-organization.json')
                    write_private_json(path, asdict(organization(c, group, check['work'])))
                cfg = configuration(c, group, table, organization_path=path)
                configs[group['id']] = cfg
                if group['arm'] == 'pg':
                    groups[group['id']] = stack.enter_context(PersistentMapGateway(cfg, plan=plan,
                        manifest_path=inputs_root/'manifest.json', model_path=model_path, ledger=ledger,
                        root=root/group['id'], query_count=len(c['orders'])))
            pids = {key: value.gateway.pid for key, value in groups.items()}
            stage_sampler = stack.enter_context(ProcessSampler(root/'stage-rss.jsonl', dict(pids, driver=os.getpid())))
            definitions = {g['id']: g for g in c['groups']}
            for repeat, order in enumerate(c['orders']):
                for key in order:
                    time_remaining()
                    group = definitions[key]; cfg = configs[key]
                    unit = f'q-{repeat}'; output = root/key/unit
                    if key in groups:
                        summary = groups[key].run_query(unit, dsn=os.environ['SEMLOOM_TEST_PG_DSN'],
                            pg_log=Path(c['pg_log']), trace_flow=group['event_content'] == 'full', peer_pids=pids)
                    else:
                        from dataclasses import replace
                        (root/key).mkdir(exist_ok=True)
                        summary = run_query(replace(cfg, unit_id=key+'-'+unit), manifest_path=inputs_root/'manifest.json',
                            model_path=model_path, budget_path=c['budget_path'], budget=budget, root=output,
                            dsn=os.environ['SEMLOOM_TEST_PG_DSN'])
                    record = query_record(summary, output, group, repeat=repeat, expected_rows=manifest['rows'],
                        policy=c['selection_policy'], window=c['resources']['window'])
                    work_check = check['checks'][key].get('work')
                    if work_check is not None:
                        record['work_preflight'] = work_check
                        blocking = record['evaluation'].get('organization', {}).get('compute_lifecycle', {}).get('work_only_block_count')
                        if work_check['nonbinding_for_described_input'] and blocking:
                            raise ValueError('declared nonbinding W blocked work; stop rather than retune')
                    rows.append(record)
                    write_private_json(root/f'completed-{len(rows):03}.json', record)
        result = summarize(c, rows)
        result['budget'] = ledger.snapshot()
        result['resident_processes'] = stage_sampler.summary()
        result['resident_process_scope'] = 'all resident gateways and driver sampled throughout every arm; query samplers separately include the active PG reader'
        write_private_json(root/'comparison.json', result)
    except BaseException as error:
        write_private_json(root/'failure.json', dict(status='failed', error_type=type(error).__name__,
            error=redact_text(str(error)), completed_queries=len(rows),
            completed_query_posts=sum(r['actual_posts'] for r in rows),
            actual_posts='see all per-query/group request evidence, including the failed query',
            next_stage_started=False))
        raise


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('config')
    parser.add_argument('--preflight', action='store_true', help='local identity and static resource checks only')
    args = parser.parse_args()
    if args.preflight:
        print(json.dumps(preflight(json.loads(Path(args.config).read_text())), indent=2))
    else:
        run_campaign(args.config)
