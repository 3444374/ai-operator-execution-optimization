"""Run original SemBench Movie Q3 with PZ-owned parallel execution."""
import argparse
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from enum import Enum
import importlib.metadata
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from src.baselines.common.private_artifacts import (
    content_digest, new_private_directory, require_outside_git, write_private_json,
)
from src.baselines.common.redact import redact_json_values, redact_text
from src.baselines.text.sembench_movie import classification_audit, evaluate_original
from src.execution_provider.adapters.model_config import load_fixed_model_config
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.buffered_events import BufferedEvents
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.native_http_observer import observe_native_httpx
from src.experiments.postgresql.sembench_queries import MovieDataset, MOVIE_MANIFEST_DIGEST
from src.infrastructure.runner_lease import HOST_SCOPE_FINGERPRINT, acquire_runner_lease
from .ledger import PalimpzestLedger
from .model import LocalModelBinding, PZ_VLLM_ENUM, bind_local_completion, fetch_model_metadata
from .source import build_movie_query, load_palimpzest, source_identity


PZ_RUNTIME_VERSIONS = {'palimpzest': '0.8.2', 'litellm': '1.95.0', 'openai': '2.50.0',
                       'pandas': '2.3.3', 'numpy': '2.0.2', 'pyarrow': '20.0.0'}


def verify_libraries():
    versions = {name: importlib.metadata.version(name) for name in (*PZ_RUNTIME_VERSIONS, 'pydantic', 'httpx')}
    for name, expected in PZ_RUNTIME_VERSIONS.items():
        if versions[name] != expected:
            raise ValueError(name+' version differs from the qualified PZ runtime profile')
    return versions


@dataclass(frozen=True)
class NativeOptions:
    max_workers: int = 20
    join_parallelism: int = 20
    batch_size: int | None = None

    def __post_init__(self):
        if any(type(v) is not int or not 1 <= v <= 256
               for v in (self.max_workers, self.join_parallelism)):
            raise ValueError('native workers/join parallelism require explicit bounds from 1 to 256')
        if self.batch_size is not None and (type(self.batch_size) is not int or self.batch_size < 1):
            raise ValueError('native batch size must be positive when specified')


def query_config(pz, binding, options):
    """Explicit QPC is necessary: this old Dataset.run does not forward kwargs."""
    return pz.QueryProcessorConfig(policy=pz.MaxQuality(), execution_strategy='parallel',
        sentinel_execution_strategy=None, available_models=[getattr(pz.Model, PZ_VLLM_ENUM)],
        max_workers=options.max_workers, join_parallelism=options.join_parallelism,
        batch_size=options.batch_size, api_base=binding.api_base, use_vertex=False,
        verbose=False, progress=False)


def config_identity(config):
    values = config.to_dict()
    policy = values['policy']
    values['policy'] = dict(type=type(policy).__name__, weights=policy.get_dict())
    values['available_models'] = [model.value for model in values['available_models']]
    for name in ('execution_strategy', 'sentinel_execution_strategy', 'optimizer_strategy'):
        value = values[name]
        if isinstance(value, Enum):
            values[name] = value.name
        elif value is not None and not isinstance(value, str):
            values[name] = dict(type=type(value).__name__,
                settings={key: getattr(value, key) for key in ('max_workers', 'batch_size', 'scan_start_idx',
                                                               'num_samples', 'verbose', 'progress')})
    return values


@contextmanager
def _private_working_directory(path):
    """Original PZ parse-error files must remain with this run's private evidence."""
    require_outside_git(Path(path))
    previous = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


@contextmanager
def _closed_events(events):
    try:
        yield events
    finally:
        primary = sys.exception()
        try:
            events.close()
        except BaseException as error:
            if primary is None:
                raise
            primary.add_note('PZ event cleanup also failed: '+type(error).__name__)


def _env_path(name, *, private=True):
    if type(name) is not str or not name or not os.environ.get(name):
        raise ValueError('a required PZ path environment variable is missing')
    path = Path(os.environ[name]).resolve()
    if private:
        require_outside_git(path)
    return path


def load_config(path):
    if Path(path).stat().st_size > 32768:
        raise ValueError('PZ experiment configuration exceeds its byte allowance')
    config = json.loads(Path(path).read_text())
    required = {'schema', 'dataset', 'palimpzest_checkout_env', 'model_config_env', 'output_env',
                'host_scope_env', 'query_id', 'max_attempts', 'query_timeout_s', 'native_options'}
    if (type(config) is not dict or set(config) != required
            or config['schema'] != 'semloom.sembench_palimpzest.v1'):
        raise ValueError('PZ configuration has missing or unsupported fields')
    data = config['dataset']
    if (type(data) is not dict or set(data) != {'checkout_env', 'manifest_env', 'csv_env', 'manifest_content_digest'}
            or data['manifest_content_digest'] != MOVIE_MANIFEST_DIGEST):
        raise ValueError('PZ CLI requires the selected original Movie input')
    if (type(config['query_id']) is not int or config['query_id'] not in (2, 3)
            or type(config['max_attempts']) is not int or config['max_attempts'] < 1
            or type(config['query_timeout_s']) not in (int, float)
            or not 0 < config['query_timeout_s'] <= 3600
            or type(config['native_options']) is not dict):
        raise ValueError('PZ query, attempt allowance and request deadline must be explicitly bounded')
    NativeOptions(**config['native_options'])
    return config


def preflight(config, *, libraries=True):
    data = config['dataset']
    dataset = MovieDataset(_env_path(data['checkout_env'], private=False), _env_path(data['manifest_env']),
                           _env_path(data['csv_env']), data['manifest_content_digest'])
    evidence = dataset.preflight(require_libraries=False)
    for key in ('upstream_lotus_version', 'selected_lotus_version', 'versions', 'source_files',
                'max_tokens', 'stop', 'retries'):
        evidence.pop(key, None)
    pz_root = _env_path(config['palimpzest_checkout_env'], private=False)
    evidence.update(source_identity(pz_root, dataset.checkout, config['query_id']))
    rows = dataset.load_rows()
    eligible = [row for row in rows if row['movie_id'] == 'taken_3']
    if not eligible or config['max_attempts'] < len(rows):
        raise ValueError('attempt allowance must cover all source rows if the native optimizer reorders filters')
    output, host_scope = _env_path(config['output_env']), _env_path(config['host_scope_env'])
    if output.exists():
        raise ValueError('PZ runner requires a new private output directory')
    binding = LocalModelBinding(load_fixed_model_config(_env_path(config['model_config_env'])))
    if binding.model.timeout_ms / 1000 > config['query_timeout_s']:
        raise ValueError('local model timeout exceeds the declared query request deadline')
    versions = {}
    if libraries:
        versions = verify_libraries()
        load_palimpzest(pz_root)
    evidence.update(schema='semloom.sembench_palimpzest_preflight.v1', versions=versions,
        libraries_verified=libraries, config_fingerprint=content_digest(config),
        native_options=asdict(NativeOptions(**config['native_options'])), eligible_rows=len(eligible),
        source_row_call_allowance=len(rows), max_attempts=config['max_attempts'],
        preflight_model_requests=0, execution_started=False,
        cache='no Generator cache in the selected source; external service cache separately configured',
        query_optimization='original run(): no validator or sample execution; final optimizer selection retained',
        generation='original Generator temperature=0; no client max_tokens or stop override',
        limit_calls='Q2 LIMIT5 does not imply five submitted/completed model requests',
        model_or_quality_evidence=False)
    return dataset, rows, pz_root, binding, output, host_scope, evidence


def movie_quality(dataset, rows, query_id, result, ledger):
    import pandas as pd
    from src.experiments.postgresql.query_evaluation import json_metric
    labels = dataset.references(rows)
    source_map = {'reviews-'+str(index): row['row_id'] for index, row in enumerate(rows)}
    selected = {row['row_id']: labels[row['row_id']] for row in rows if row['movie_id'] == 'taken_3'}
    predictions = ledger.predictions(source_map)
    task_predictions = [(key, label) for key, label in predictions if key in selected]
    audit = classification_audit(selected, task_predictions, require_complete=query_id == 3)
    all_evaluated = classification_audit(labels, predictions, require_complete=False)
    frame = result.to_df()
    if query_id == 3:
        if list(frame.columns) != ['count'] or len(frame) != 1:
            raise ValueError('original PZ Q3 result is not its one-row count schema')
        kept = sum(call['native_passed_operator'] is True for call in ledger.calls()
                   if source_map[call['source_indices'][0]] in selected)
        if int(frame.iloc[0, 0]) != kept:
            raise ValueError('original PZ COUNT differs from all native Filter outcomes')
    elif list(frame.columns) != ['reviewId'] or len(frame) > 5:
        raise ValueError('original PZ Q2 result differs from its projection/LIMIT contract')
    labeled = pd.DataFrame([(row['review_id'], row['movie_id'], row['text'], labels[row['row_id']])
                            for row in rows], columns=['reviewId', 'id', 'reviewText', 'scoreSentiment'])
    metric = evaluate_original(dataset.checkout, query_id, frame, labeled)
    metric.update(classification_audit=audit, all_evaluated_classification_audit=all_evaluated,
                  extra_movie_filter_calls=sum(key not in selected for key, _ in predictions),
                  dataset_role=dataset.role,
                  audit_scope='every submitted Filter occurrence; failures/defaults are invalid predictions',
                  complete_filter_audit=query_id == 3, quality_equivalence_proven=False)
    return json_metric(metric)


def execute_native(dataset, rows, pz_root, binding, verified, options, *, query_id, budget,
                   events, timeout_s, ledger=None, execution=None):
    """One isolated process, original Generator and native parallel DAG owner."""
    from src.semantic_methods.palimpzest import completion_bridge
    versions = verify_libraries()
    pz = load_palimpzest(pz_root)
    from palimpzest.query.generators import generators
    ledger = ledger if ledger is not None else PalimpzestLedger(max_calls=budget.remaining, record=events.record)
    evidence = execution if execution is not None else {}
    config = query_config(pz, binding, options)
    selected_config = config_identity(config)
    completion = bind_local_completion(generators.litellm.completion, binding, verified)
    started_ns = time.monotonic_ns()
    bridge = None
    try:
        with observe_native_httpx(budget, events.record, binding.model.endpoint_url, timeout_s=timeout_s):
            with completion_bridge(completion=completion, observer=ledger) as bridge:
                query = build_movie_query(dataset.checkout, pz, query_id, rows, config)
                result = query.run(config)
    finally:
        ended_ns = time.monotonic_ns()
        evidence.update(full_query_seconds=(ended_ns-started_ns)/1e9,
                        query_started_ns=started_ns, query_returned_ns=ended_ns,
                        selected_config=selected_config, effective_config=config_identity(config),
                        versions=versions,
                        bridge=bridge.snapshot() if bridge is not None else None, ledger=ledger.snapshot(),
                        actual_posts=budget.attempts, scheduler_owner='Palimpzest ParallelExecutionStrategy',
                        residual_work='native call exited; remote device work not independently proved',
                        upstream_execution_cost_is_actual_price=False,
                        source_load_scope='preverified finite in-memory relation; source preparation reported separately')
    state = evidence['bridge']
    if (not state['patches_restored'] or state['active_generators'] or state['failures']):
        raise RuntimeError('PZ completion bridge did not finish and restore cleanly')
    return result, ledger, evidence


def run(config):
    prepared_ns = time.monotonic_ns()
    dataset, rows, pz_root, binding, output, host_scope, evidence = preflight(config)
    preparation_seconds = (time.monotonic_ns()-prepared_ns)/1e9
    repository = Path(__file__).resolve().parents[4]
    commit = subprocess.check_output(['git', '-C', str(repository), 'rev-parse', 'HEAD'], text=True).strip()
    with acquire_runner_lease(host_scope, config_fingerprint=HOST_SCOPE_FINGERPRINT,
                              repository_commit=commit, recover_stale=False):
        new_private_directory(output)
        write_private_json(output/'preflight.json', redact_json_values(evidence))
        write_private_json(output/'config.json', redact_json_values(config))
        cell = CellBudgetLedger.create(output/'budget.sqlite', AttemptBudget('palimpzest-movie', config['max_attempts']),
                                       deadline_utc=time.time()+config['query_timeout_s'])
        cell.reserve_unit('native', config['max_attempts'])
        budget = cell.claim_unit('native')
        summary = dict(schema='semloom.sembench_palimpzest_native.v1', status='failed',
                       query_id=config['query_id'], repository_commit=commit,
                       input_preparation_seconds=preparation_seconds,
                       model=dict(binding_fingerprint=binding.fingerprint, metadata_status='unverified'),
                       real_model_evidence='external caller service; verify separate model/host evidence',
                       performance_qualified=False)
        events = None
        ledger = PalimpzestLedger(max_calls=config['max_attempts'])
        execution = {}
        write_private_json(output/'started.json', redact_json_values(summary))
        write_private_json(output/'initial-generator-ledger.json', ledger.snapshot())
        try:
            verified = fetch_model_metadata(binding)
            summary['model'] = verified.summary()
            events = BufferedEvents(output/'events.jsonl')
            ledger.record = events.record
            with _closed_events(events):
                with _private_working_directory(output):
                    result, ledger, execution = execute_native(dataset, rows, pz_root, binding, verified,
                        NativeOptions(**config['native_options']), query_id=config['query_id'], budget=budget,
                        events=events, timeout_s=config['query_timeout_s'], ledger=ledger, execution=execution)
                summary.update(execution=execution, quality=movie_quality(dataset, rows, config['query_id'], result, ledger),
                               status='passed')
                write_private_json(output/'original-result.json', json.loads(result.to_df().to_json(orient='split')))
                if (execution['ledger']['completion_failed'] or execution['ledger']['parser_default_retained']
                        or execution['ledger']['generator_failed']):
                    summary['status'] = 'completed_with_invalid_predictions'
        except BaseException as error:
            summary.update(status='failed', error=dict(type=type(error).__name__, message=redact_text(str(error))))
            raise
        finally:
            primary = sys.exception()
            try:
                summary.update(actual_posts=budget.attempts, budget=cell.snapshot(), execution=execution,
                               completion_ledger=ledger.snapshot(),
                               events=events.snapshot() if events is not None else None)
            except BaseException as error:
                if primary is None:
                    raise
                primary.add_note('PZ final accounting also failed: '+type(error).__name__)
            for filename, value in (('generator-calls.json', ledger.calls()), ('summary.json', summary)):
                try:
                    write_private_json(output/filename, redact_json_values(value))
                except BaseException as error:
                    if primary is None:
                        raise
                    primary.add_note('PZ evidence write also failed: '+type(error).__name__)
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument('--data-preflight', action='store_true')
    action.add_argument('--preflight', action='store_true')
    action.add_argument('--execute', action='store_true')
    args = parser.parse_args(argv)
    config = load_config(args.config)
    value = run(config) if args.execute else preflight(config, libraries=args.preflight)[-1]
    print(json.dumps(redact_json_values(value), ensure_ascii=False, allow_nan=False))
    return 0 if not args.execute or value['status'] == 'passed' else 2
