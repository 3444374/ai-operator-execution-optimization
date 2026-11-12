"""Prepared SemBench Boolean work and its complete original sink on author IMLane.

The native TINYINT SQL, owner, request budget, result ACK and supervisor retain
their existing implementations. One invocation selects one complete query;
old joint-batch measurements are not converted into per-query timings.
"""

import argparse
import csv
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
from src.baselines.text.sembench_prepared import prepare, finish_relation, audit_relation, result_value
from src.baselines.text.sembench_text import SemBenchTextQuery
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.native_imlane_model_query import supervised_run, _positive
from src.experiments.native_imlane_relation import (
    run_relation_query, finish_mmqa_filters, native_filter_membership, persist_relation_observation,
)
from src.experiments.native_imlane_sembench import _ResultWindow, audit_ipc
from src.infrastructure.private_artifacts import (
    file_identity, json_metric, new_private_directory, open_private_text,
    read_json, write_private_json,
)
from src.infrastructure.redact import redact_text
from src.serving.backends.vllm_protocol import GenerationSpec

SCHEMA = 'semloom.imlane.mmqa_filter.v1'
MEDICAL_SCHEMA = 'semloom.imlane.medical_filter.v1'
MOVIE_SCHEMA = 'semloom.imlane.movie_filter.v1'
CARS_SCHEMA = 'semloom.imlane.cars_filter.v1'
ECOMM_SCHEMA = 'semloom.imlane.ecomm_filter.v1'
JOIN_SCHEMA = 'semloom.imlane.text_join.v1'
DSO_SHA256 = '52bf5763aab6f35b76746a9fc15b75e27bac1534315d1a20926eb4a78c2badaf'
CODE = Path(__file__).resolve().parents[2]


def configuration(value, *, frozen=True):
    fields = {'schema', 'query_id', 'execution_id', 'sembench_checkout', 'data_dir', 'input_sha256',
              'model_name', 'tokenizer', 'generation', 'prompt_columns', 'model_service', 'max_model_calls',
              'query_timeout_s', 'cleanup_s', 'stop_grace_s', 'request_timeout_s'}
    if not frozen:
        fields -= {'input_sha256', 'prompt_columns', 'max_model_calls'}
    schema = value.get('schema') if type(value) is dict else None
    if schema == JOIN_SCHEMA:
        fields.discard('prompt_columns')
        fields.add('scenario')
        if 'scale_factor' in value:
            fields.add('scale_factor')
    if schema in (MEDICAL_SCHEMA, CARS_SCHEMA):
        fields.add('scale_factor')
    if (type(value) is not dict or set(value) != fields
            or schema not in (SCHEMA, MEDICAL_SCHEMA, MOVIE_SCHEMA, CARS_SCHEMA, ECOMM_SCHEMA, JOIN_SCHEMA)):
        raise ValueError('use a SemBench Filter or explicit text Join configuration')
    if schema == MEDICAL_SCHEMA:
        from src.baselines.text.sembench_medical import text_files
        text_files(value['scale_factor'])
    elif schema == CARS_SCHEMA:
        from src.baselines.text.sembench_cars import text_files
        text_files(value['scale_factor'])
    generation = GenerationSpec.from_dict(value['generation'])
    if generation.response_format is not None:
        raise ValueError('native Filter requires unstructured Boolean answers')
    for name in ('query_timeout_s', 'cleanup_s', 'stop_grace_s', 'request_timeout_s'):
        _positive(value[name])
    if frozen:
        AttemptBudget(value['execution_id'], value['max_model_calls'])
    return dict(value), generation


def _source_query(config, generation, *, frozen=True):
    scenario = (config['scenario'] if config['schema'] == JOIN_SCHEMA else
        {SCHEMA: 'mmqa', MEDICAL_SCHEMA: 'medical', MOVIE_SCHEMA: 'movie',
         CARS_SCHEMA: 'cars', ECOMM_SCHEMA: 'ecomm'}[config['schema']])
    query = SemBenchTextQuery(config['sembench_checkout'], config['data_dir'], scenario,
        config['query_id'], execution_id=config['execution_id'], model_name=config['model_name'],
        generation=generation, scale_factor=config.get('scale_factor'))
    kind = 'join' if config['schema'] == JOIN_SCHEMA else 'filter'
    if query.operation is None or query.operation.describe()['kind'] != kind:
        raise ValueError('this native entry requires one original ' + kind.title() + ' operation')
    if not query.operation.records:
        raise ValueError('the native Filter driver requires a nonempty original input')
    if frozen:
        if {name: item['sha256'] for name, item in query.files.items()} != config['input_sha256']:
            raise ValueError('original source files changed')
        if len(query.operation.records) != config['max_model_calls']:
            raise ValueError('single-query Filter binds every original occurrence')
    return query


def freeze_config(value):
    """Observe the original input once; produce a config, not permission to run."""
    config, generation = configuration(value, frozen=False)
    query = _source_query(config, generation, frozen=False)
    method, prepared, _requests = prepare(query)
    config.update(input_sha256={name: item['sha256'] for name, item in query.files.items()},
                  max_model_calls=len(prepared))
    if config['schema'] != JOIN_SCHEMA:
        config['prompt_columns'] = list(method.columns)
    return configuration(config)[0]


def prepare_query(config):
    """Validate source and build audit token IDs before starting the native owner."""
    from transformers import AutoTokenizer
    config, generation = configuration(config)
    query = _source_query(config, generation)
    method, prepared, requests, rows = _prepare_requests(query, config.get('prompt_columns'))
    tokenizer = AutoTokenizer.from_pretrained(config['tokenizer'], local_files_only=True, trust_remote_code=False)
    tokens = [tokenizer.apply_chat_template(row['messages'], tokenize=True, add_generation_prompt=True)
              for row in requests]
    return query, method, prepared, requests, tokens, rows


def _prepare_requests(query, prompt_columns):
    """Build the actual native inputs without reloading source code or audit tokens."""
    method, prepared, requests = prepare(query, prompt_columns=prompt_columns)
    rows = []
    for record, request in zip(query.operation.records, requests, strict=True):
        # Keep the existing three-string native ABI. These two fields carry
        # source identity/cells, not a guessed task title and text column. The
        # original formatter's complete messages remain the model input.
        rows.append(dict(occurrence_id=request['call_id'], source_position=record.source_position,
            title=str(record.source_index),
            text=json.dumps(record.values, ensure_ascii=False, allow_nan=False, sort_keys=True),
            messages=request['messages']))
    return method, prepared, requests, rows


def check(config):
    query, _method, prepared, requests, tokens, rows = prepare_query(config)
    return dict(schema=config['schema'], status='passed_zero_model_source_check', variant='original-author',
        source=query.describe(), **_prompt_description(config, query, _method),
        prepared=prepared, requests=requests, token_ids=tokens, rows=rows,
        maximum_model_calls=config['max_model_calls'], retries=0, extra_warmup=0, models=0,
        target_native_driver_SQL_model_qualified=False)


def _prompt_description(config, query, method):
    if config['schema'] == JOIN_SCHEMA:
        return dict(declared_join_columns=list(query.operation.columns),
                    join_semantics='original LOTUS exact Join over native IMLane TINYINT prediction')
    return dict(effective_prompt_columns=list(method.columns))


def verified_build(path):
    record = read_json(path)
    if (record['schema'] != 'semloom.imlane.filter_driver_build.v1' or record['status'] != 'built'
            or record['variant'] != 'original-author' or record['duckdb_library']['sha256'] != DSO_SHA256):
        raise ValueError('the original 072f typed DSO and single-query Filter driver are required')
    if file_identity(CODE / 'integrations/imlane_duckdb/mmqa_filter.cpp') != record.get('driver_source'):
        raise ValueError('Filter build must bind the current native driver source; relink the thin driver')
    if file_identity(CODE / 'integrations/imlane_duckdb/relation_handoff.hpp') != record.get('relation_handoff'):
        raise ValueError('Filter build must bind current relation_handoff.hpp; relink the thin driver')
    for name in ('binary', 'duckdb_library', 'kernel'):
        if file_identity(Path(record[name + '_path'])) != record[name]:
            raise ValueError('native Filter build artifact changed: ' + name)
    if file_identity(CODE / 'integrations/imlane_duckdb/mmqa_filter_udf.py') != record['kernel']:
        raise ValueError('Filter build must bind the current kernel')
    return record


class FilterRelation:
    def __init__(self, query, method, prepared, requests, tokens, rows, execute_query):
        self.query, self.method, self.prepared = query, method, prepared
        self.requests, self.tokens, self.rows = requests, tokens, rows
        self.execute_query = execute_query

    def sink(self, native):
        if native['variant'] != 'original-author' or native['expected_DSO_sha256'] != DSO_SHA256:
            raise ValueError('native Filter must retain the original author DSO identity')
        if self.query.operation.describe()['kind'] == 'join':
            selected = native_filter_membership(native, {self.query.query: (self.query, self.rows)})[self.query.query]
            positives = {record.source_position for record in selected}
            # These are the author's actual TINYINT decisions, encoded for the
            # native Join relation constructor. Raw model replies are audited
            # independently after the full sink; no model request is repeated.
            predictions = [dict(call_id=row['call_id'], generated_text=(
                'True' if row['source_position'] in positives else 'False')) for row in self.prepared]
            result, _relation = finish_relation(self.query, self.method, self.prepared, predictions,
                                                execute_query=self.execute_query)
            return result
        return finish_mmqa_filters(native, {self.query.query: (self.query, self.rows)},
            execute_queries={self.query.query: self.execute_query})[self.query.query]

    def audit(self, native, result, directory, sampling):
        from pandas import Series
        from pandas.testing import assert_frame_equal, assert_series_equal
        predictions = []
        for path in Path(directory).glob('*.terminal.json'):
            batch = read_json(path, max_bytes=path.stat().st_size)
            for call in batch['calls']:
                predictions.append(dict(call_id=call['occurrence_id'],
                    generated_text=call['response']['generated_text']))
        ipc = audit_ipc(directory, self.requests, self.tokens, predictions, sampling, by_occurrence=True)
        reconstructed, relation = finish_relation(self.query, self.method, self.prepared, predictions)
        decisions = relation['parsed_outputs']
        if self.query.operation.describe()['kind'] == 'join':
            from lotus.sem_ops.postprocessors import filter_postprocess
            decisions = filter_postprocess(relation['texts'], self.method, True).outputs
        positives = {row['occurrence_id'] for row, positive in
                     zip(self.rows, decisions, strict=True) if positive}
        if positives != {row['occurrence_id'] for row in native['accepted_occurrences']}:
            raise ValueError('native Filter membership differs from original parser decisions')
        if isinstance(reconstructed, Series):
            assert_series_equal(result, reconstructed)
        else:
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
        query_clock_version='sembench.source_to_sink.v3', source_started_ns=None,
        outer_host_lease_required=True, physical_gpu_exit_confirmed=None,
        build_record=file_identity(Path(args.build_record)))
    primary = service = ledger = unit = result = None
    try:
        # Source binding and token-reference work belong to preflight. The
        # measured query regenerates its execution messages, and the original
        # sink still performs the real data scan as in the prepared Ray/Daft paths.
        checked = prepare_query(config)
        query, tokens = checked[0], checked[4]
        report.update(source=query.describe(), **_prompt_description(config, query, checked[1]),
                      source_preflight_finished_ns=time.monotonic_ns())
        del checked
        execute_query = query.bind_execution()
        window.check()
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
            if max(map(len, tokens)) + generation.max_tokens > service.ready_info['max_model_len']:
                raise ValueError('full Filter prompt and output exceed the actual model context')
            report['preflight_finished_ns'] = time.monotonic_ns()
            report['model_ready_before_source'] = True
            report['model_ready_before_source_observed_ns'] = report['model_ready_observed_ns']
            started = time.monotonic_ns()
            report['query_started_monotonic_ns'] = started
            report['source_started_ns'] = started
            method, prepared, requests, rows = _prepare_requests(query, config.get('prompt_columns'))
            write_private_json(args.output / 'messages.json', dict(rows=rows))
            with open_private_text(args.output / 'input.csv') as stream:
                writer = csv.writer(stream); writer.writerow(('occurrence_id', 'title', 'text', 'source_position'))
                writer.writerows((r['occurrence_id'], r['title'], r['text'], r['source_position']) for r in rows)
            relation = FilterRelation(query, method, prepared, requests, tokens, rows, execute_query)
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
        report['timing_scope'] = ('ready_source_to_sink v3: actual message/native-input preparation, native SQL, '
            'original CSV scan and complete memory sink after owner ready; source binding/configuration, '
            'tokenizer/audit-token preparation, context and budget checks precede the start. ACK, audit, '
            'evaluation and close follow. cold_worker_entry_to_sink additionally includes preflight and model startup; '
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
    parser.add_argument('action', choices=('freeze-config', 'check', 'run'))
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
    if args.action == 'freeze-config':
        config = freeze_config(read_json(args.config))
        new_private_directory(args.output)
        write_private_json(args.output / 'CONFIG.json', config)
        return 0
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
