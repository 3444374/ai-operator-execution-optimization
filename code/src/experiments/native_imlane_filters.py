"""One original Movie Q1–Q4, MMQA Q3/Q6 or Medical Q4 Filter and full sink on author IMLane.

The native TINYINT SQL, owner, request budget, result ACK and supervisor retain
their existing implementations. One invocation selects one complete query;
old joint-batch measurements are not converted into per-query timings.
"""

import argparse
import csv
from dataclasses import replace
import json
import os
from pathlib import Path
import signal
import sys
import threading
import time

from integrations.imlane_duckdb import model_provider, mmqa_filter_udf
from integrations.imlane_duckdb.model_service import ModelService
from integrations.imlane_records import owner
from src.baselines.text.sembench_prepared import finish_relation, audit_relation, result_value
from src.baselines.text.sembench_text import SemBenchTextQuery
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.native_imlane_model_query import supervised_run, _positive
from src.experiments.native_imlane_relation import run_relation_query, finish_mmqa_filters, persist_relation_observation
from src.experiments.native_imlane_sembench import _ResultWindow, audit_ipc
from src.infrastructure.private_artifacts import (
    file_identity, json_metric, new_private_directory, open_private_text,
    read_json, write_private_json,
)
from src.infrastructure.redact import redact_text
from src.serving.backends.vllm_protocol import GenerationSpec
from src.semantic_methods.lotus.record_method import LotusRecordMethod

SCHEMA = 'semloom.imlane.mmqa_filter.v1'
MEDICAL_SCHEMA = 'semloom.imlane.medical_filter.v1'
MOVIE_SCHEMA = 'semloom.imlane.movie_filter.v1'
QUERIES = ('3a', '3b', '3c', '3d', '3e', '3f', '3g', '6a', '6b', '6c')
DSO_SHA256 = '52bf5763aab6f35b76746a9fc15b75e27bac1534315d1a20926eb4a78c2badaf'
CODE = Path(__file__).resolve().parents[2]


def configuration(value):
    fields = {'schema', 'query_id', 'execution_id', 'sembench_checkout', 'data_dir', 'input_sha256',
              'model_name', 'tokenizer', 'generation', 'prompt_columns', 'model_service', 'max_model_calls',
              'query_timeout_s', 'cleanup_s', 'stop_grace_s', 'request_timeout_s'}
    medical = type(value) is dict and value.get('schema') == MEDICAL_SCHEMA
    movie = type(value) is dict and value.get('schema') == MOVIE_SCHEMA
    if medical:
        fields.add('scale_factor')
    queries = ('4',) if medical else ('1', '2', '3', '4') if movie else QUERIES
    limit = 1200 if medical else (2000 if value.get('query_id') == '1' else 120) if movie else 25
    if (type(value) is not dict or set(value) != fields
            or value['schema'] not in (SCHEMA, MEDICAL_SCHEMA, MOVIE_SCHEMA)
            or value['query_id'] not in queries
            or value['max_model_calls'] != limit
            or medical and value['scale_factor'] != 11112):
        raise ValueError('select Movie Q1 with 2000 calls, Q2–Q4 with 120, MMQA Q3/Q6 with 25, or Medical Q4 with 1200 and scale 11112')
    generation = GenerationSpec.from_dict(value['generation'])
    if generation.response_format is not None or generation.max_tokens != 8192:
        raise ValueError('native Filter requires the original unstructured 8192-token generation')
    for name in ('query_timeout_s', 'cleanup_s', 'stop_grace_s', 'request_timeout_s'):
        _positive(value[name])
    AttemptBudget(value['execution_id'], value['max_model_calls'])
    return dict(value), generation


def prepare_query(config):
    import lotus
    from transformers import AutoTokenizer
    config, generation = configuration(config)
    medical = config['schema'] == MEDICAL_SCHEMA
    scenario = {SCHEMA: 'mmqa', MEDICAL_SCHEMA: 'medical', MOVIE_SCHEMA: 'movie'}[config['schema']]
    query = SemBenchTextQuery(config['sembench_checkout'], config['data_dir'], scenario,
        config['query_id'], execution_id=config['execution_id'], model_name=config['model_name'],
        generation=generation, **(dict(scale_factor=config['scale_factor']) if medical else {}))
    if {name: item['sha256'] for name, item in query.files.items()} != config['input_sha256']:
        raise ValueError('original source files changed')
    stage = query.operation.stage
    columns = config['prompt_columns']
    if (type(columns) is not list or len(set(columns)) != len(columns)
            or set(columns) != set(stage.method.resolved_input_columns)):
        raise ValueError('freeze exactly the original prompt columns in their observed order')
    # LOTUS resolves placeholder names through a set. Freeze the observed
    # original column order through its existing bound-method interface, so a
    # different Python hash seed cannot silently reorder the model context.
    stage = replace(stage, method=replace(stage.method, resolved_input_columns=tuple(columns)))
    with lotus.settings.context(enable_cache=False):
        method = LotusRecordMethod(stage)
        prepared = method.prepare_rows(query.operation.records, query_id=query.query_id)
    requests = [dict(call_id=row['call_id'], messages=json.loads(row['messages_json'])) for row in prepared]
    if stage.kind != 'filter' or len(prepared) != config['max_model_calls']:
        raise ValueError('single-query Filter binds every original occurrence')
    tokenizer = AutoTokenizer.from_pretrained(config['tokenizer'], local_files_only=True, trust_remote_code=False)
    tokens = [tokenizer.apply_chat_template(row['messages'], tokenize=True, add_generation_prompt=True)
              for row in requests]
    if scenario == 'movie':
        label, text = 'reviewId', 'reviewText'
    elif medical:
        label, text = 'patient_id', 'symptoms'
    else:
        label, text = ('Airlines', 'Destinations') if query.query.startswith('6') else ('title', 'text')
    rows = []
    for record, request in zip(query.operation.records, requests, strict=True):
        source = query.operation.frame.iloc[record.source_position]
        rows.append(dict(occurrence_id=request['call_id'], source_position=record.source_position,
            title=str(source[label]), text=str(source[text]), messages=request['messages']))
    return query, method, prepared, requests, tokens, rows


def check(config):
    query, _method, prepared, requests, tokens, rows = prepare_query(config)
    return dict(schema=config['schema'], status='passed_zero_model_source_check', variant='original-author',
        source=query.describe(), effective_prompt_columns=list(_method.columns),
        prepared=prepared, requests=requests, token_ids=tokens, rows=rows,
        maximum_model_calls=config['max_model_calls'], retries=0, extra_warmup=0, models=0,
        target_native_driver_SQL_model_qualified=False)


def verified_build(path):
    record = read_json(path)
    if (record['schema'] != 'semloom.imlane.filter_driver_build.v1' or record['status'] != 'built'
            or record['variant'] != 'original-author' or record['duckdb_library']['sha256'] != DSO_SHA256):
        raise ValueError('the original 072f typed DSO and single-query Filter driver are required')
    for name in ('binary', 'duckdb_library', 'kernel'):
        if file_identity(Path(record[name + '_path'])) != record[name]:
            raise ValueError('native Filter build artifact changed: ' + name)
    if file_identity(CODE / 'integrations/imlane_duckdb/mmqa_filter_udf.py') != record['kernel']:
        raise ValueError('Filter build must bind the current kernel')
    return record


class FilterRelation:
    def __init__(self, query, method, prepared, requests, tokens, rows):
        self.query, self.method, self.prepared = query, method, prepared
        self.requests, self.tokens, self.rows = requests, tokens, rows

    def sink(self, native):
        if native['variant'] != 'original-author' or native['expected_DSO_sha256'] != DSO_SHA256:
            raise ValueError('native Filter must retain the original author DSO identity')
        return finish_mmqa_filters(native, {self.query.query: (self.query, self.rows)})[self.query.query]

    def audit(self, native, result, directory, sampling):
        from pandas.testing import assert_frame_equal
        predictions = []
        for path in Path(directory).glob('*.terminal.json'):
            batch = read_json(path, max_bytes=path.stat().st_size)
            for call in batch['calls']:
                predictions.append(dict(call_id=call['occurrence_id'],
                    generated_text=call['response']['generated_text']))
        ipc = audit_ipc(directory, self.requests, self.tokens, predictions, sampling, by_occurrence=True)
        reconstructed, relation = finish_relation(self.query, self.method, self.prepared, predictions)
        positives = {row['occurrence_id'] for row, positive in
                     zip(self.rows, relation['parsed_outputs'], strict=True) if positive}
        if positives != {row['occurrence_id'] for row in native['accepted_occurrences']}:
            raise ValueError('native Filter membership differs from original parser decisions')
        assert_frame_equal(result, reconstructed)
        return dict(ipc=ipc, semantic=audit_relation(self.prepared, relation),
                    quality=json_metric(self.query.evaluate(result)))


def run_worker(args, config, *, service_factory=ModelService):
    entered = time.monotonic_ns()
    config, generation = configuration(config)
    build = verified_build(args.build_record)
    args.output = Path(args.output).resolve()
    new_private_directory(args.output)
    window = _ResultWindow(args.output, config['query_timeout_s'], config['cleanup_s'],
        config['stop_grace_s'], cleanup_path=args.output / 'cleanup-start.json')
    args.scopes, args.stop_budget = [], owner.StopBudget(threading.Event())
    args.stop_grace_seconds = config['stop_grace_s']
    args.duckdb_library = Path(build['duckdb_library_path']).resolve()
    previous = {n: signal.signal(n, args.stop_budget.signal) for n in (signal.SIGTERM, signal.SIGINT)}
    report = dict(schema=config['schema'], status='failed', variant='original-author', query_id=config['query_id'],
        worker_entry_monotonic_ns=entered, retries=0, extra_warmup=0,
        outer_host_lease_required=True, physical_gpu_exit_confirmed=None,
        build_record=file_identity(Path(args.build_record)))
    primary = service = ledger = unit = result = None
    try:
        for name in ('model-calls', 'model-service'):
            (args.output / name).mkdir(mode=0o700)
        limit = config['max_model_calls']
        budget = AttemptBudget(config['execution_id'], limit)
        ledger = CellBudgetLedger.create(args.output / 'physical-budget.sqlite', budget,
            deadline_utc=time.time() + config['query_timeout_s'])
        ledger.reserve_unit('query', limit)
        unit = ledger.claim_shared_unit('query')
        descriptor = dict(path=str(ledger.path), budget_id=budget.budget_id, limit=limit, unit_id='query')
        service = service_factory(dict(config['model_service'], request_budget=descriptor),
            args.output / 'model-service', environment=dict(os.environ), deadline=window.query_deadline,
            cleanup_deadline=window.begin_cleanup, stop_budget=args.stop_budget, stop_grace_s=config['stop_grace_s'])
        sampling = generation.kwargs()
        provider = model_provider.configuration(dict(schema=model_provider.CONFIG_SCHEMA,
            provider_mode='model', socket_path=str(service.socket_path), model=service.served_model_name,
            sampling_params=sampling, request_timeout_s=config['request_timeout_s'],
            deadline_monotonic=window.query_deadline, receipt_dir=str(args.output / 'model-calls')))
        write_private_json(args.output / 'model-provider.json', provider)
        args.workload_environment = {model_provider.CONFIG_ENV: str(args.output / 'model-provider.json'),
            mmqa_filter_udf.CONFIG_ENV: str(args.output / 'messages.json')}
        with service:
            report.update(model_ready_observed_ns=time.monotonic_ns(), model_owner=service.ready_info)
            report['model_startup_seconds'] = report['model_ready_observed_ns'] / 1e9 - service.report['started_monotonic']
            started = time.monotonic_ns()
            report['query_started_monotonic_ns'] = started
            query, method, prepared, requests, tokens, rows = prepare_query(config)
            report['source'] = query.describe()
            report['effective_prompt_columns'] = list(method.columns)
            write_private_json(args.output / 'messages.json', dict(rows=rows))
            with open_private_text(args.output / 'input.csv') as stream:
                writer = csv.writer(stream); writer.writerow(('occurrence_id', 'title', 'text', 'source_position'))
                writer.writerows((r['occurrence_id'], r['title'], r['text'], r['source_position']) for r in rows)
            if max(map(len, tokens)) + 8192 > service.ready_info['max_model_len']:
                raise ValueError('full Filter prompt and output exceed the actual model context')
            relation = FilterRelation(query, method, prepared, requests, tokens, rows)
            args.startup_timeout_s = window.query_deadline - time.monotonic()
            command = [build['binary_path'], '--input', str(args.output / 'input.csv'),
                '--module', build['kernel_path'], '--function', 'infer_mmqa', '--kind', args.kind,
                '--output', str(args.output / 'query.json'), '--sink-release', str(args.output / 'sink-release.ack'),
                '--expected-rows', str(limit)]
            if args.threads is not None: command += ['--threads', str(args.threads)]
            def audit(native, value):
                if Path(native['converter_resolved_DSO']).resolve() != args.duckdb_library:
                    raise ValueError('native Filter used a different DSO')
                return relation.audit(native, value, args.output / 'model-calls', sampling)
            try:
                result, audited = run_relation_query(args, report, kernel=Path(build['kernel_path']),
                    command=command, window=window, query_started_ns=started, sink=relation.sink,
                    audit=audit, check=service.check)
                report.update(quality=audited['quality'], ipc=audited['ipc'])
                write_private_json(args.output / 'semantic-audit.json', audited['semantic'])
            finally:
                service.begin_cleanup(window.begin_cleanup())
        report.update(status='completed', result=result_value(result))
    except BaseException as error:
        primary, result = error, getattr(error, 'query_result', result)
        report.update(status='failed', error_type=type(error).__name__, error=redact_text(str(error)))
    finally:
        window.begin_cleanup()
        if service is not None:
            try:
                with args.stop_budget.cleanup(): service.close(deadline=window.cleanup_deadline)
                report['service_close'] = service.report
            except BaseException as error:
                if primary is None: primary = error
                else: primary.add_note('model close also failed: ' + type(error).__name__)
        if ledger is not None:
            try:
                ledger.close_shared_unit('query'); report['budget'] = ledger.snapshot()
                report['budget']['consumed_requests'] = None if unit is None else unit.attempts
            except BaseException as error:
                if primary is None: primary = error
                else: primary.add_note('budget close also failed: ' + type(error).__name__)
        for n, handler in previous.items(): signal.signal(n, handler)
        if primary is not None: report['status'] = 'failed'
        if report.get('full_sink_observed_monotonic_ns') is not None:
            report['cold_worker_entry_to_sink_seconds'] = (report['full_sink_observed_monotonic_ns'] - entered) / 1e9
            report['ready_source_to_sink_seconds'] = report['source_to_sink_seconds']
        report['deadline'] = window.report()
        report['timing_scope'] = ('ready_source_to_sink: fresh single-query CSV/prompt preparation after owner ready '
            'through full original memory sink, including required native transport; ACK, audit, evaluation and '
            'close follow. cold_worker_entry_to_sink additionally includes worker preflight and model startup; '
            'host imports/preflight/lease before this worker remain excluded, not silently reconstructed.')
        try:
            persist_relation_observation(args.output, report, result, primary=primary)
        except BaseException as error:
            if primary is None: primary = error
            else: primary.add_note('relation persistence also failed: ' + type(error).__name__)
            report.update(status='failed', error_type=type(primary).__name__)
        try:
            write_private_json(args.output / 'worker.json', report)
        except BaseException as error:
            if primary is None: primary = error
            else: primary.add_note('worker report also failed: ' + type(error).__name__)
    if primary is not None: raise primary
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('check', 'run'))
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    for name in ('build-record', 'author-root', 'method-python'):
        parser.add_argument('--' + name, type=Path)
    parser.add_argument('--kind', choices=('thread', 'process'), default='thread')
    parser.add_argument('--threads', type=int)
    parser.add_argument('--executor-count', type=int)
    parser.add_argument('--execute-model', action='store_true')
    parser.add_argument('--worker', action='store_true', help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    config, _ = configuration(read_json(args.config))
    if args.action == 'check':
        new_private_directory(args.output); write_private_json(args.output / 'CHECK.json', check(config))
        return 0
    if not args.execute_model or any(getattr(args, n) is None for n in ('build_record', 'author_root', 'method_python')):
        parser.error('run requires --execute-model and explicit build/author/Python bindings')
    if args.worker:
        run_worker(args, config)
    else:
        output = args.output.resolve(); new_private_directory(output)
        command = [sys.executable, '-B', '-m', __spec__.name, 'run', '--config', str(args.config.resolve()),
            '--output', str(output / 'worker'), '--build-record', str(args.build_record.resolve()),
            '--author-root', str(args.author_root.resolve()), '--method-python', str(args.method_python.absolute()),
            '--kind', args.kind, '--execute-model', '--worker']
        for name in ('threads', 'executor_count'):
            if getattr(args, name) is not None: command += ['--' + name.replace('_', '-'), str(getattr(args, name))]
        supervision = supervised_run(command, output / 'worker', output / 'supervision',
            query_seconds=config['query_timeout_s'], cleanup_seconds=config['cleanup_s'], stop_grace_seconds=config['stop_grace_s'])
        terminal = read_json(output / 'worker/worker.json')
        write_private_json(output / 'timing.json', dict(
            cold_supervised_runner_to_sink_seconds=terminal['result_ready_monotonic_ns'] / 1e9 - supervision['started_monotonic'],
            cold_worker_entry_to_sink_seconds=terminal['cold_worker_entry_to_sink_seconds'],
            ready_source_to_sink_seconds=terminal['ready_source_to_sink_seconds'],
            model_startup_seconds=terminal['model_startup_seconds'], strict_host_controller_entry_seconds=None,
            cold_boundary='existing supervise started_monotonic before log/Popen; excludes host preflight and lease',
            query_boundary='one fresh complete original source after actual ready through full memory sink'))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
