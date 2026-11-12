"""MMQA Q1 on the separately qualified IMLane VARCHAR engineering variant.

Reuse the existing author/model owners, relation ACK and outer supervisor.
The author owns prediction batches; original LOTUS/SemBench owns all semantics.
This entry does not qualify the unmodified author or implement image support.
"""

import argparse
import csv
from dataclasses import asdict
import json
import os
from pathlib import Path
import signal
import sys
import threading
import time

from integrations.imlane_duckdb import model_provider
from integrations.imlane_duckdb.model_service import ModelService
from integrations.imlane_records import owner
from src.baselines.text.sembench_prepared import prepare, finish_relation, audit_relation, result_value
from src.baselines.text.sembench_text import SemBenchTextQuery
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.native_imlane_model_query import QueryWindow, READY_SCHEMA, supervised_run, _positive
from src.experiments.native_imlane_relation import run_relation_query, persist_relation_observation
from src.infrastructure.private_artifacts import (
    file_identity, json_metric, new_private_directory, open_private_text,
    publish_private_json, read_json, write_private_json,
)
from src.infrastructure.redact import redact_text
from src.serving.backends.vllm_protocol import GenerationSpec

SCHEMA = 'semloom.imlane.mmqa_q1.v1'
VARIANT = 'varchar-owned-engineering'
DSO_SHA256 = '5a172f77e9c46fae28f96251d20a022fda472749c1e858b2b2c25262eafa5fdd'
CODE = Path(__file__).resolve().parents[2]


def configuration(value):
    fields = {'schema', 'query_id', 'sembench_checkout', 'data_dir', 'input_sha256', 'model_name',
              'tokenizer', 'generation', 'model_service', 'max_model_calls', 'query_timeout_s',
              'cleanup_s', 'stop_grace_s', 'request_timeout_s'}
    if type(value) is not dict or set(value) != fields or value['schema'] != SCHEMA:
        raise ValueError('explicit native IMLane MMQA Q1 configuration required')
    generation = GenerationSpec.from_dict(value['generation'])
    if (generation.response_format != {'type': 'json_object'} or generation.max_tokens != 8192
            or value['max_model_calls'] != 25):
        raise ValueError('Q1 requires original JSON Extract, max_tokens 8192 and a 25-call cap')
    for name in ('query_timeout_s', 'cleanup_s', 'stop_grace_s', 'request_timeout_s'):
        _positive(value[name])
    AttemptBudget(value['query_id'], value['max_model_calls'])
    return dict(value), generation


def prepare_query(config):
    from transformers import AutoTokenizer
    config, generation = configuration(config)
    query = SemBenchTextQuery(config['sembench_checkout'], config['data_dir'], 'mmqa', '1',
        execution_id=config['query_id'], model_name=config['model_name'], generation=generation)
    if {name: value['sha256'] for name, value in query.files.items()} != config['input_sha256']:
        raise ValueError('Q1 input files differ from their frozen identities')
    method, prepared, requests = prepare(query)
    if len(prepared) != 25 or query.operation.stage.kind != 'extract':
        raise ValueError('Q1 binds all 25 source occurrences to the original Extract')
    tokenizer = AutoTokenizer.from_pretrained(config['tokenizer'], local_files_only=True,
                                               trust_remote_code=False)
    token_ids = [tokenizer.apply_chat_template(row['messages'], tokenize=True,
                  add_generation_prompt=True) for row in requests]
    return query, method, prepared, requests, token_ids


def check(config):
    query, method, prepared, requests, tokens = prepare_query(config)
    return dict(schema=SCHEMA, status='passed_zero_model_source_check', variant=VARIANT,
        source=query.describe(), generation=asdict(query.operation.generation),
        prepared=prepared, requests=requests, token_ids=tokens,
        maximum_model_calls=25, extra_warmup=0, retries=0, model_calls=0,
        native_DSO_SQL_model_binding_qualified=False)


def verified_build(path):
    record = read_json(path)
    if (record['schema'] != 'semloom.imlane.prepared_driver_build.v1'
            or record['status'] != 'built' or record['variant'] != VARIANT
            or record['duckdb_library']['sha256'] != DSO_SHA256):
        raise ValueError('the separately qualified VARCHAR-owned DSO and prepared driver are required')
    for label, filename in (('driver_source', 'sembench.cpp'), ('relation_handoff', 'relation_handoff.hpp')):
        if file_identity(CODE / 'integrations/imlane_duckdb' / filename) != record.get(label):
            raise ValueError('prepared build must bind current ' + filename + '; relink the thin driver')
    for name in ('binary', 'duckdb_library', 'kernel'):
        if file_identity(Path(record[name + '_path'])) != record[name]:
            raise ValueError('native build artifact changed: ' + name)
    if file_identity(CODE / 'integrations/imlane_duckdb/sembench_udf.py') != record['kernel']:
        raise ValueError('driver build must bind the current prepared kernel')
    return record


class Q1Relation:
    def __init__(self, query, method, prepared):
        self.query, self.method, self.prepared = query, method, prepared
        self.relation = None
        self.predictions = None

    def sink(self, native):
        if (native['return_type'] != 'VARCHAR' or native['full_projection_materialized'] is not True
                or native['source_rows'] != len(self.prepared) or native['variant'] != VARIANT):
            raise ValueError('complete native VARCHAR prediction relation required')
        predictions = native['mapped_occurrences']
        if any(type(row['generated_text']) is not str for row in predictions):
            raise ValueError('raw model text must survive the native VARCHAR result unchanged')
        result, self.relation = finish_relation(self.query, self.method, self.prepared, predictions)
        self.predictions = predictions
        return result

    def audit(self):
        return audit_relation(self.prepared, self.relation)


def audit_ipc(directory, requests, tokens, predictions, sampling):
    """Correlate native batch positions and occurrence IDs, including equal messages."""
    expected = {row['call_id']:
                (row['call_id'], ids, row['messages'])
                for row, ids in zip(requests, tokens, strict=True)}
    outputs = {row['call_id']: row['generated_text'] for row in predictions}
    if (len(expected) != len(requests) or len(outputs) != len(predictions)
            or set(outputs) != {row['call_id'] for row in requests}):
        raise ValueError('IPC audit requires every unique source occurrence and its original output')
    seen, attempts, request_ids, calls = set(), set(), set(), []
    starts = {p.stem.removesuffix('.started') for p in Path(directory).glob('*.started.json')}
    ends = list(Path(directory).glob('*.terminal.json'))
    if starts != {p.stem.removesuffix('.terminal') for p in ends}:
        raise ValueError('every model batch requires its original started and terminal receipt')
    for path in ends:
        batch = read_json(path, max_bytes=path.stat().st_size)
        if (batch['status'] != 'succeeded' or batch['client_closed'] is not True
                or batch['request']['sampling_params'] != sampling):
            raise ValueError('model IPC failed or changed original generation parameters')
        for position, call in enumerate(batch['calls']):
            key = call.get('occurrence_id')
            if key not in expected or key in seen:
                raise ValueError('duplicate or foreign model occurrence')
            call_id, ids, messages = expected[key]
            response = call['response']
            if (call['position'] != position or response['position'] != position
                    or call['requested_payload']['messages'] != messages
                    or batch['request']['messages_batch'][position] != messages):
                raise ValueError('native batch position or original occurrence messages changed')
            if (call['status'] != 'succeeded' or call['attempted'] is not True
                    or response['generated_text'] != outputs[call_id]
                    or response['rendered_prompt_token_ids'] != ids
                    or response['requested_max_tokens'] != sampling['max_tokens']
                    or response['resolved_max_tokens'] != sampling['max_tokens']
                    or response['budget_attempt'] in attempts or response['request_id'] in request_ids):
                raise ValueError('native text, tokens or charged request identity changed')
            seen.add(key); attempts.add(response['budget_attempt']); request_ids.add(response['request_id'])
            calls.append(dict(call_id=call_id, response=response))
    if seen != set(expected) or attempts != set(range(1, len(expected) + 1)):
        raise ValueError('every source occurrence requires one identified charged native terminal result')
    return dict(calls=calls, native_generate_terminal_count=len(calls), retries=0,
                first_engine_accept=None, kernel_count=None)


class _ResultWindow(QueryWindow):
    def __init__(self, output, *args, **kwargs):
        self.output = output
        super().__init__(*args, **kwargs)

    def accept_ready(self, ready, *, clock):
        super().accept_ready(ready, clock=clock)
        publish_private_json(self.output / 'result-ready.json',
            dict(schema=READY_SCHEMA, result_ready_monotonic_ns=ready, clock=clock))


def run_worker(args, config, *, service_factory=ModelService):
    worker_entered = time.monotonic_ns()
    config, generation = configuration(config)
    build = verified_build(args.build_record)
    args.output = Path(args.output).resolve()
    new_private_directory(args.output)
    window = _ResultWindow(args.output, config['query_timeout_s'], config['cleanup_s'],
        config['stop_grace_s'], cleanup_path=args.output / 'cleanup-start.json')
    report = dict(schema=SCHEMA, variant=VARIANT, status='failed', worker_entry_monotonic_ns=worker_entered,
        strict_controller_entry_ns=None, strict_controller_entry_reason='host preflight and lease are external',
        build_record=file_identity(Path(args.build_record)), extra_warmup=0, retries=0,
        cache='LOTUS result cache off; native engine cache settings and readiness are separate',
        outer_host_lease_required=True, physical_gpu_exit_confirmed=None)
    args.scopes = []
    args.stop_budget = owner.StopBudget(threading.Event())
    args.stop_grace_seconds = config['stop_grace_s']
    args.duckdb_library = Path(build['duckdb_library_path']).resolve()
    previous = {number: signal.signal(number, args.stop_budget.signal) for number in (signal.SIGTERM, signal.SIGINT)}
    primary = service = ledger = unit = result = None
    try:
        for name in ('model-calls', 'model-service'):
            (args.output / name).mkdir(mode=0o700)
        budget = AttemptBudget(config['query_id'], 25)
        ledger = CellBudgetLedger.create(args.output / 'physical-budget.sqlite', budget,
            deadline_utc=time.time() + config['query_timeout_s'])
        ledger.reserve_unit('query', 25)
        unit = ledger.claim_shared_unit('query')
        descriptor = dict(path=str(ledger.path), budget_id=budget.budget_id, limit=25, unit_id='query')
        service = service_factory(dict(config['model_service'], request_budget=descriptor),
            args.output / 'model-service', environment=dict(os.environ), deadline=window.query_deadline,
            cleanup_deadline=window.begin_cleanup, stop_budget=args.stop_budget,
            stop_grace_s=config['stop_grace_s'])
        sampling = generation.kwargs()
        provider = model_provider.configuration(dict(schema=model_provider.CONFIG_SCHEMA,
            provider_mode='model', socket_path=str(service.socket_path), model=service.served_model_name,
            sampling_params=sampling, request_timeout_s=config['request_timeout_s'],
            deadline_monotonic=window.query_deadline, receipt_dir=str(args.output / 'model-calls')))
        write_private_json(args.output / 'model-provider.json', provider)
        args.workload_environment = {model_provider.CONFIG_ENV: str(args.output / 'model-provider.json')}
        def audit(native, value):
            if Path(native['converter_resolved_DSO']).resolve() != args.duckdb_library:
                raise ValueError('native driver used a different DSO')
            semantic = relation.audit()
            ipc = audit_ipc(args.output / 'model-calls', requests, tokens, relation.predictions, sampling)
            return dict(semantic=semantic, ipc=ipc, quality=json_metric(query.evaluate(value)))
        with service:
            report['model_ready_observed_ns'] = time.monotonic_ns()
            report['model_owner'] = service.ready_info
            report['model_startup_seconds'] = (
                report['model_ready_observed_ns'] / 1e9 - service.report['started_monotonic'])
            # The common primary clock starts with a fresh source after model
            # readiness. Keep startup and worker-entry totals in separate fields.
            started = time.monotonic_ns()
            report['query_started_monotonic_ns'] = started
            query, method, prepared, requests, tokens = prepare_query(config)
            report['source'] = query.describe()
            with open_private_text(args.output / 'input.csv') as stream:
                writer = csv.writer(stream); writer.writerow(('call_id', 'messages_json'))
                writer.writerows((row['call_id'], row['messages_json']) for row in prepared)
            relation = Q1Relation(query, method, prepared)
            if max(map(len, tokens)) + 8192 > service.ready_info['max_model_len']:
                raise ValueError('original full prompt plus output exceeds actual model context')
            args.startup_timeout_s = window.query_deadline - time.monotonic()
            command = [build['binary_path'], '--input', str(args.output / 'input.csv'),
                '--module', build['kernel_path'], '--function', 'infer_prepared', '--kind', args.kind,
                '--output', str(args.output / 'query.json'), '--sink-release', str(args.output / 'sink-release.ack')]
            if args.threads is not None:
                command += ['--threads', str(args.threads)]
            try:
                result, audited = run_relation_query(args, report, kernel=Path(build['kernel_path']),
                    command=command, window=window, query_started_ns=started,
                    sink=relation.sink, audit=audit, check=service.check)
                report.update(quality=audited['quality'], ipc=audited['ipc'])
                write_private_json(args.output / 'semantic-audit.json', audited['semantic'])
            finally:
                service.begin_cleanup(window.begin_cleanup())
        report.update(status='completed', service_close=service.report, result=result_value(result))
    except BaseException as error:
        primary = error
        result = getattr(error, 'query_result', result)
        report.update(status='failed', error_type=type(error).__name__, error=redact_text(str(error)))
    finally:
        window.begin_cleanup()
        if service is not None:
            try:
                with args.stop_budget.cleanup(): service.close(deadline=window.cleanup_deadline)
                report['service_close'] = service.report
            except BaseException as error:
                if primary is None: primary = error
                else: primary.add_note('model service cleanup also failed: ' + type(error).__name__)
        if ledger is not None:
            try:
                ledger.close_shared_unit('query'); report['budget'] = ledger.snapshot()
                report['budget']['consumed_requests'] = None if unit is None else unit.attempts
            except BaseException as error:
                if primary is None: primary = error
                else: primary.add_note('budget close also failed: ' + type(error).__name__)
        for number, handler in previous.items(): signal.signal(number, handler)
        if primary is not None: report['status'] = 'failed'
        if report.get('full_sink_observed_monotonic_ns') is not None:
            report['cold_worker_entry_to_sink_seconds'] = (
                report['full_sink_observed_monotonic_ns'] - worker_entered) / 1e9
            report['ready_source_to_sink_seconds'] = report.get('source_to_sink_seconds')
        report['timing_scope'] = ('source_to_sink_seconds/ready_source_to_sink_seconds: model-ready then fresh '
            'CSV/source, original prompt preparation, '
            'required native transport/projection, original parser and complete memory sink; '
            'cold_worker_entry_to_sink_seconds also includes worker preflight and model startup; '
            'neither clock includes post-sink audit, evaluation, persistence or final close. '
            'Worker entry excludes host preflight/lease and Python import/CLI before run_worker.')
        report['deadline'] = window.report()
        try:
            persist_relation_observation(args.output, report, result, primary=primary)
        except BaseException as error:
            primary = error
            report.update(status='failed', error_type=type(error).__name__)
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
        new_private_directory(args.output)
        write_private_json(args.output / 'CHECK.json', check(config))
        return 0
    if not args.execute_model or any(getattr(args, name) is None for name in ('build_record', 'author_root', 'method_python')):
        parser.error('run requires --execute-model and the build/author/Python bindings')
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
            query_seconds=config['query_timeout_s'], cleanup_seconds=config['cleanup_s'],
            stop_grace_seconds=config['stop_grace_s'])
        terminal = read_json(output / 'worker/worker.json')
        write_private_json(output / 'timing.json', dict(
            cold_supervised_runner_to_sink_seconds=(terminal['result_ready_monotonic_ns'] / 1e9
                - supervision['started_monotonic']),
            cold_worker_entry_to_sink_seconds=terminal['cold_worker_entry_to_sink_seconds'],
            ready_source_to_sink_seconds=terminal['ready_source_to_sink_seconds'],
            model_startup_seconds=terminal['model_startup_seconds'],
            strict_host_controller_entry_seconds=None,
            cold_boundary='existing supervisor started_monotonic before log open/Popen; '
                          'excludes host imports/preflight/lease before supervise',
            ready_query_boundary='fresh source/prepare after actual owner ready through original full memory sink',
            comparison='select the same observed start boundary on every compared system; '
                       'do not equate ready-query and startup-inclusive cold totals'))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
