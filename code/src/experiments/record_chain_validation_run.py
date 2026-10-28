"""Git-owned finite-chain validation entry; site config and receipts stay outside Git.

This is one bounded four-case check, not a matrix/host controller. The existing
external owner keeps its lease, budgets, CPU/GPU placement and physical checks.
"""
from dataclasses import asdict
from pathlib import Path
import argparse
import csv
import importlib.metadata as metadata
import hashlib
import itertools
import json
import os
import subprocess
import sys
import time
import traceback

from src.infrastructure.private_artifacts import (
    file_identity, require_outside_git, write_private_json,
)
from src.infrastructure.redact import redact_text
from src.semantic_methods.lotus.record_chain import RecordOccurrence
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
from src.experiments.record_chain_validation import (
    make_chain, make_budget, run_case, replay_native_answers, compare,
)

CASES = (('fifo-count', False, False), ('fifo-token', True, False),
         ('successor-count', False, True), ('successor-token', True, True))
REPO = Path(__file__).resolve().parents[3]
ENTRY = REPO / 'code/scripts/experiments/validate_record_chain_options.py'


def read(path):
    return json.loads(Path(path).read_text())


def make_resources(config):
    resources = RecordsResources(**config['resources'])
    if resources.max_active_batches != 4:
        raise ValueError('this four-case check requires explicit max_active_batches=4')
    return resources


def checkout_identity(expected_commit, *, repo=REPO):
    def git(*args):
        return subprocess.check_output(['git', '-C', str(repo), *args], text=True).strip()
    commit = git('rev-parse', 'HEAD')
    if commit != expected_commit or git('status', '--porcelain', '--untracked-files=all', '--', 'code'):
        raise ValueError('exact clean Git source required; no private source overlay')
    return dict(commit=commit, tree=git('rev-parse', 'HEAD^{tree}'))


def configuration(path):
    path = Path(path).resolve()
    require_outside_git(path)
    config = read(path)
    engine = EngineSpec.from_dict(config['engine'])
    resources = make_resources(config)
    generation = GenerationSpec.from_dict(config['generation'])
    if (engine.max_model_calls != 48 or engine.max_num_seqs != 4 or
            resources.prepare_batch_rows != 1 or resources.model_batch_rows != 1 or
            resources.cpu_workers != 1 or resources.gpu_workers != 1):
        raise ValueError('this check requires one model/CPU, B1, C4 and a48-call native cap')
    for name in ('artifact_root',):
        require_outside_git(Path(config[name]))
    require_outside_git(Path(config['input']['existing_csv']))
    if set(config['ray_roots']) != {name for name, _, _ in CASES}:
        raise ValueError('explicit private Ray roots required for all four cases')
    for root in config['ray_roots'].values():
        require_outside_git(Path(root))
    config['cases'] = [dict(case_id=name, token=token, successor=priority)
                       for name, token, priority in CASES]
    return config, engine, resources, generation


def inputs(config):
    path = Path(config['input']['existing_csv'])
    if file_identity(path)['sha256'] != config['input']['sha256']:
        raise ValueError('bound CSV changed')
    with path.open(newline='') as stream:
        rows = list(itertools.islice(csv.DictReader(stream), 16))
    if len(rows) != 16:
        raise ValueError('this bounded check needs the first16 records')
    return [RecordOccurrence('movie:' + str(i), i, row.get('reviewId', str(i)),
                             {'text': row['reviewText']}) for i, row in enumerate(rows)]


def check(path):
    config, engine, resources, generation = configuration(path)
    source = checkout_identity(config['public_git_commit'])
    records = inputs(config)
    make_chain(config['model_name'], generation)
    return dict(status='configuration_checked_not_tokenizer_or_runtime_qualified', source=source,
                resources=asdict(resources), rows=len(records), cases=4,
                max_requests=192, models=0, claims=0)


def objects(config, engine, generation):
    from transformers import AutoTokenizer
    tokenizer = AutoTokenizer.from_pretrained(engine.tokenizer_path, local_files_only=True, use_fast=True)
    records = inputs(config)
    chain = make_chain(config['model_name'], generation)
    budget, W = make_budget(tokenizer, engine, chain, records)
    return tokenizer, records, chain, budget, W


def qualify(path, output):
    config, engine, resources, generation = configuration(path)
    source = checkout_identity(config['public_git_commit'])
    _, _, _, budget, W = objects(config, engine, generation)
    versions = {p: metadata.version(p) for p in ('ray', 'lotus-ai', 'transformers', 'tokenizers', 'pyarrow')}
    if versions['ray'] != '2.56.1':
        raise ValueError('published owner requires Ray2.56.1')
    report = dict(status='passed_CPU_only_host_lease_pending', models=0, claims=0,
                  config_sha256=file_identity(path)['sha256'], source=source,
                  resources=asdict(resources), versions=versions, W=W,
                  token_estimator_signature=budget.signature)
    write_private_json(output, report)
    return report


def qualified(path, ready_path):
    config, engine, resources, generation = configuration(path)
    require_outside_git(Path(ready_path))
    ready = read(ready_path)
    if (ready['status'] != 'passed_CPU_only_host_lease_pending' or
            ready['config_sha256'] != file_identity(path)['sha256'] or
            ready['resources'] != asdict(resources) or
            ready['source'] != checkout_identity(config['public_git_commit'])):
        raise ValueError('qualified configuration/source/resources changed')
    return config, engine, resources, generation, ready


def execute_worker(path, ready_path, case_id, *, process_started_ns=None):
    from integrations.imlane_records.chain_runtime import check_external_lease
    from src.experiments.attempt_ledger import AttemptBudget
    from src.experiments.cell_budget import CellBudgetLedger
    from src.observability.records_telemetry import RecordsTelemetry
    from src.experiments.process_sampling import ProcessSampler
    import psutil
    import ray

    started = time.monotonic_ns() if process_started_ns is None else process_started_ns
    config, engine, resources, generation, ready = qualified(path, ready_path)
    case = next(c for c in config['cases'] if c['case_id'] == case_id)
    if config['execution_enabled'] is not True or ray.__version__ != '2.56.1' or ray.is_initialized():
        raise ValueError('explicit enabled config and fresh Ray2.56.1 required')
    check_external_lease(config['external_owner'])
    root = Path(config['artifact_root'])
    out = root / 'cases' / case_id
    out.mkdir(parents=True, exist_ok=False, mode=0o700)
    claim = CellBudgetLedger(root / 'budget.sqlite', AttemptBudget(config['run_id'], 192)).claim_unit(case_id)
    claim.bind_execution()
    report = dict(status='failed', case_id=case_id, actual_calls=None,
                  public_git_commit=ready['source']['commit'], process_start_ns=started,
                  pid=os.getpid(), pid_birth=psutil.Process().create_time(),
                  scope='n1 functional check; not performance or default-switch acceptance')
    primary = None
    created_ray = False
    sampler = ProcessSampler(out / 'rss.jsonl', {'driver': os.getpid()}, interval=.2,
                             max_samples=2000, include_children=True)
    telemetry = RecordsTelemetry(out / 'telemetry', gpu_ids=[config['physical_gpu_UUID']],
                                 interval_s=.2, max_samples=2000).start()
    try:
        sampler.__enter__()
        tokenizer, records, chain, budget, W = objects(config, engine, generation)
        if W != ready['W'] or budget.signature != ready['token_estimator_signature']:
            raise ValueError('qualified tokenizer/W changed')
        env = dict(HF_HUB_OFFLINE='1', TRANSFORMERS_OFFLINE='1',
                   LITELLM_LOCAL_MODEL_COST_MAP='True', VLLM_WORKER_MULTIPROC_METHOD='spawn')
        os.environ.update(env)
        # Same caller bootstrap as the normal Movie session, not another model owner.
        created_ray = True
        ray.init(address='local', num_cpus=3, num_gpus=1, include_dashboard=False,
                 _temp_dir=config['ray_roots'][case_id], runtime_env={'env_vars': env})
        result = run_case(records, chain, engine, resources, control_dir=out / 'control',
                          token_budget=budget if case['token'] else None, priority=case['successor'])
        report.update({k:v for k,v in result.items() if k != 'records'})
        report['actual_calls'] = len(result['calls'])
        replay_started = time.monotonic_ns()
        oracle = replay_native_answers(records, chain, result, tokenizer)
        outputs = []
        for record in result.pop('records'):
            value = record.describe()
            text = value['values'].pop('text')
            value['source_text_sha256'] = hashlib.sha256(text.encode()).hexdigest()
            outputs.append(value)
        report.update(status='completed', **result, outputs=outputs, replay=oracle, W=W,
            replay_wall_s=(time.monotonic_ns()-replay_started)/1e9,
            engine=asdict(engine), generation=asdict(generation), resources=asdict(resources),
            W_exercised=None if not case['token'] else result['W_block_states'] > 0 and result['W_block_states_later_submitted'] > 0,
            successor_contention_exercised=None if not case['successor'] else result['contended_successor_choices'] > 0)
    except BaseException as error:
        primary = error
        report.update(observed_calls=getattr(error, 'chain_calls', []),
                      observed_close=getattr(error, 'chain_close', None), error_type=type(error).__name__,
                      error=redact_text(str(error)), traceback=redact_text(traceback.format_exc()))
    finally:
        if created_ray:
            try:
                ray.shutdown()
                report['ray_shutdown_returned'] = True
            except BaseException as error:
                report['ray_shutdown_error'] = type(error).__name__
        try:
            sampler.__exit__(None, None, None)
            report['rss'] = sampler.summary()
        except BaseException as error:
            report['rss_error'] = type(error).__name__
        report['telemetry'] = telemetry.close()
        report['worker_wall_before_final_write_s'] = (time.monotonic_ns()-started)/1e9
        report['physical_exit_and_GPU_empty'] = 'requires independent external owner receipt'
        write_private_json(out / 'case.json', report)
    if primary is not None:
        raise primary
    return report


def supervised_case(path, ready, case_id):
    from integrations.imlane_records.chain_runtime import check_external_lease
    from src.experiments.lotus_records_supervisor import supervise
    config, engine, _, _, _ = qualified(path, ready)
    if config['execution_enabled'] is not True:
        raise ValueError('explicit enabled config required before worker launch')
    check_external_lease(config['external_owner'])
    command = [engine.driver_python, '-B', str(ENTRY), 'worker', '--config', str(path),
               '--ready', str(ready), '--case', case_id, '--execute-model']
    return supervise(command, Path(config['artifact_root']) / 'supervision' / case_id,
                     timeout_s=360, grace_s=90, natural_exit_grace_s=1)


def main(argv=None, *, process_started_ns=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('check', 'qualify', 'case', 'worker', 'compare'))
    parser.add_argument('--config', required=True, type=Path)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--ready', type=Path)
    parser.add_argument('--case', choices=[name for name, _, _ in CASES])
    parser.add_argument('--execute-model', action='store_true')
    args = parser.parse_args(argv)
    if args.action == 'check':
        result = check(args.config)
    elif args.action == 'qualify':
        if args.output is None: parser.error('qualify requires a fresh private --output')
        result = qualify(args.config, args.output)
    elif args.action == 'compare':
        if args.output is None: parser.error('compare requires a fresh private --output')
        config, _, _, _ = configuration(args.config)
        result = compare(Path(config['artifact_root']), config)
        write_private_json(args.output, result)
    else:
        if not args.execute_model or args.case is None or args.ready is None:
            parser.error('case/worker require --case, --ready and --execute-model under the existing external owner')
        if args.action == 'worker':
            result = execute_worker(args.config, args.ready, args.case, process_started_ns=process_started_ns)
        else:
            result = supervised_case(args.config, args.ready, args.case)
    print(json.dumps({k:result[k] for k in ('status', 'models', 'claims', 'max_requests') if k in result}))
    return 0
