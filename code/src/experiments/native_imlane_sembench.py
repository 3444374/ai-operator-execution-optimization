"""Original SemBench queries on the qualified IMLane VARCHAR engineering variant.

Reuse the existing author/model owners, relation ACK and outer supervisor.
The author owns prediction batches; original LOTUS/SemBench owns all semantics.
The complete-query profile preserves original image messages; target VLM qualification is separate.
"""

import argparse
from contextlib import nullcontext
from copy import copy
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
from src.baselines.text.sembench_prepared import (
    prepare, finish_relation, audit_relation, result_value, make_query_lm, audit_query_batches,
)
from src.baselines.text.sembench_text import SemBenchTextQuery
from src.experiments.attempt_ledger import AttemptBudget, BudgetExhausted
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.native_imlane_model_query import _ResultWindow, supervised_run, _positive
from src.experiments.native_imlane_relation import run_relation_query, persist_relation_observation
from src.infrastructure.private_artifacts import (
    file_identity, json_metric, new_private_directory, open_private_text,
    read_json, write_private_json, content_digest,
)
from src.infrastructure.redact import redact_text
from src.serving.backends.vllm_protocol import GenerationSpec

TEXT_SCHEMA = 'semloom.imlane.prepared_text.v1'
TOPK_SCHEMA = 'semloom.imlane.lotus_topk.v1'
QUERY_SCHEMA = 'semloom.imlane.lotus_query.v1'
VARIANT = 'varchar-owned-engineering'
DSO_SHA256 = '5a172f77e9c46fae28f96251d20a022fda472749c1e858b2b2c25262eafa5fdd'
CODE = Path(__file__).resolve().parents[2]


def _aggregation(value):
    task = value.get('task', {})
    return (value.get('schema') in (TEXT_SCHEMA, QUERY_SCHEMA) and isinstance(task, dict)
            and task.get('scenario') == 'mmqa' and str(task.get('query')).lower().removeprefix('q') == '5')


def configuration(value):
    fields = {'schema', 'query_id', 'sembench_checkout', 'data_dir', 'input_sha256', 'model_name',
              'tokenizer', 'generation', 'model_service', 'max_model_calls', 'query_timeout_s',
              'cleanup_s', 'stop_grace_s', 'request_timeout_s', 'task'}
    aggregate = isinstance(value, dict) and _aggregation(value)
    if (type(value) is not dict or set(value) - {'lifecycle'} != fields | ({'token_counter'} if aggregate else set())
            or value['schema'] not in (TEXT_SCHEMA, TOPK_SCHEMA, QUERY_SCHEMA)):
        raise ValueError('explicit native IMLane query configuration required')
    if (value.get('lifecycle', 'isolated') not in ('isolated', 'model-session')
            or 'lifecycle' in value and value['schema'] != QUERY_SCHEMA):
        raise ValueError('only the complete original-query profile supports lifecycle=model-session')
    task_options = {'scenario', 'query', 'scale_factor'}
    if value['schema'] == QUERY_SCHEMA:
        task_options |= {'ranking', 'visual_query', 'image_join', 'manifest', 'manifest_digest'}
    if (type(value['task']) is not dict or not {'scenario', 'query'} <= value['task'].keys()
            or value['task'].keys() - task_options):
        raise ValueError('declare an original scenario/query task')
    if value['schema'] == QUERY_SCHEMA and any(name in value['task'] and type(value['task'][name]) is not bool
            for name in ('visual_query','image_join')):
        raise ValueError('visual_query and image_join must be explicit booleans')
    generation = GenerationSpec.from_dict(value['generation'])
    if value['schema'] == QUERY_SCHEMA and (generation.response_format is not None
            or generation.logprobs is not None or generation.repetition_penalty != 1):
        raise ValueError('complete original queries require plain base generation; each native operator owns its overrides')
    if aggregate and (generation.response_format is not None or generation.logprobs is not None
            or generation.repetition_penalty != 1):
        raise ValueError('native aggregation uses standard generation and the original unstructured parser')
    for name in ('query_timeout_s', 'cleanup_s', 'stop_grace_s', 'request_timeout_s'):
        _positive(value[name])
    AttemptBudget(value['query_id'], value['max_model_calls'])
    return dict(value), generation


def _aggregation_lm_options(config, generation):
    from src.baselines.text.sembench_mmqa_aggregate import load_token_counter

    counter = config['token_counter']
    fields = {'kind', 'path', 'sha256', 'tokenizer_id', 'tokenizer_revision'}
    if (type(counter) is not dict or set(counter) != fields
            or counter['kind'] != 'huggingface_tokenizer_file'
            or any(type(counter[name]) is not str or not counter[name].strip() for name in fields)):
        raise ValueError('bind the original LOTUS counter to the declared local model tokenizer JSON')
    model = config['model_service']
    native = model['engine_options']
    tokenizer = Path(config['tokenizer']).resolve()
    if (Path(counter['path']).resolve() != tokenizer / 'tokenizer.json'
            or tokenizer != Path(native.get('tokenizer', model['model'])).resolve()):
        raise ValueError('the LOTUS counter and audit tokenizer must match the declared model tokenizer')
    context = native.get('max_model_len')
    if type(context) is not int or context <= generation.max_tokens:
        raise ValueError('native aggregation needs an explicit model context with input space after max_tokens')
    revision = native.get('tokenizer_revision', native.get('revision'))
    if revision is not None and revision != counter['tokenizer_revision']:
        raise ValueError('the LOTUS counter revision differs from the declared model tokenizer')
    return dict(max_ctx_len=context, tokenizer=load_token_counter(counter))


def prepare_query(config, *, execution_id=None):
    from transformers import AutoTokenizer
    config, generation = configuration(config)
    task = config['task']
    options = _aggregation_lm_options(config, generation) if _aggregation(config) else {}
    query = SemBenchTextQuery(config['sembench_checkout'], config['data_dir'], task['scenario'], task['query'],
        execution_id=config['query_id'] if execution_id is None else execution_id,
        model_name=config['model_name'], generation=generation,
        scale_factor=task.get('scale_factor'), **(dict(lm_options=options) if options else {}),
        **({name: task[name] for name in ('ranking','visual_query','image_join','manifest','manifest_digest')
            if name in task} if config['schema'] == QUERY_SCHEMA else {}))
    if {name: value['sha256'] for name, value in query.files.items()} != config['input_sha256']:
        raise ValueError('original input files differ from their frozen identities')
    if _aggregation(config) or config['schema'] == QUERY_SCHEMA:
        description = query.describe()
        minimum = description.get('minimum_model_calls')
        if minimum is None:
            minimum = len(query.operation.records) if query.operation is not None else description['expected_model_calls']
        if minimum is not None and config['max_model_calls'] < minimum * (2 if config.get('lifecycle') == 'model-session' else 1):
            raise ValueError('the finite allowance must cover the original query minimum for every declared execution')
        return query, None, None, None, None
    if query.operation is not None and query.operation.describe()['kind'] not in ('map', 'extract'):
        raise ValueError('prepared VARCHAR binds ordinary text Map/Extract; use the existing Filter/Join/TopK profiles')
    method, prepared, requests = prepare(query)
    if config['max_model_calls'] < len(prepared):
        raise ValueError('declare every original Map/Extract occurrence in the finite query allowance')
    tokenizer = AutoTokenizer.from_pretrained(config['tokenizer'], local_files_only=True,
                                               trust_remote_code=False)
    token_ids = [tokenizer.apply_chat_template(row['messages'], tokenize=True,
                  add_generation_prompt=True) for row in requests]
    return query, method, prepared, requests, token_ids


def check(config):
    config, generation = configuration(config)
    if config['schema'] == TOPK_SCHEMA:
        from .native_imlane_topk import prepare_query as prepare_topk
        query = prepare_topk(config, generation)
        return dict(schema=TOPK_SCHEMA, status='passed_zero_model_source_check', variant=VARIANT,
            source=query.describe(), generation=asdict(generation),
            maximum_model_calls=query.describe()['expected_model_calls'], model_calls=0,
            native_DSO_SQL_model_binding_qualified=False)
    query, method, prepared, requests, tokens = prepare_query(config)
    if _aggregation(config) or config['schema'] == QUERY_SCHEMA:
        return dict(schema=config['schema'], status='passed_zero_model_source_check', variant=VARIANT,
            source=query.describe(), generation=asdict(query.generation), model_calls=0,
            **(dict(token_counter=config['token_counter']) if _aggregation(config) else {}),
            native_DSO_SQL_model_binding_qualified=False, target_VLM_qualified=False,
            lifecycle=config.get('lifecycle', 'isolated'),
            query_executions=2 if config.get('lifecycle') == 'model-session' else 1)
    return dict(schema=config['schema'], status='passed_zero_model_source_check', variant=VARIANT,
        source=query.describe(), generation=asdict(query.generation if query.operation is None else query.operation.generation),
        prepared=prepared, requests=requests, token_ids=tokens,
        maximum_model_calls=len(requests), extra_warmup=0, retries=0, model_calls=0,
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


def prediction_command(args, build):
    command = [build['binary_path'], '--input', str(args.output / 'input.csv'),
        '--module', build['kernel_path'], '--function', 'infer_prepared', '--kind', args.kind,
        '--output', str(args.output / 'query.json'), '--sink-release', str(args.output / 'sink-release.ack')]
    if args.threads is not None:
        command += ['--threads', str(args.threads)]
    return command


class PreparedRelation:
    def __init__(self, query, method, prepared, *, execute_query):
        self.query, self.method, self.prepared = query, method, prepared
        self.execute_query = execute_query
        self.relation = None
        self.predictions = None

    def sink(self, native):
        if (native['return_type'] != 'VARCHAR' or native['full_projection_materialized'] is not True
                or native['source_rows'] != len(self.prepared) or native['variant'] != VARIANT):
            raise ValueError('complete native VARCHAR prediction relation required')
        predictions = native['mapped_occurrences']
        if any(type(row['generated_text']) is not str for row in predictions):
            raise ValueError('raw model text must survive the native VARCHAR result unchanged')
        result, self.relation = finish_relation(self.query, self.method, self.prepared, predictions,
                                               execute_query=self.execute_query)
        self.predictions = predictions
        return result

    def audit(self):
        return audit_relation(self.prepared, self.relation)


def audit_ipc(directory, requests, tokens, predictions, sampling, *, attempt_offset=0, native_input_only=False):
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
                    or call['messages_sha256'] != content_digest(messages)
                    or batch['request']['messages_batch'][position] != messages):
                raise ValueError('native batch position or original occurrence messages changed')
            native_ids = response['rendered_prompt_token_ids']
            if ids is None:
                if (not native_input_only or not _image_message(messages)
                        or response.get('rendered_prompt_type') != 'multimodal'
                        or not {'prompt_token_ids','mm_kwargs','mm_hashes','mm_placeholders'} <= set(response.get('rendered_prompt_fields', []))
                        or not isinstance(native_ids, list) or any(type(i) is not int or i < 0 for i in native_ids)
                        or response.get('rendered_prompt_tokens') != len(native_ids)
                        or response.get('usage', {}).get('prompt_tokens') != len(native_ids)):
                    raise ValueError('image occurrence lacks native expanded multimodal input evidence')
            elif native_ids != ids:
                raise ValueError('native prompt tokens differ from the independent text reference')
            if (call['status'] != 'succeeded' or call['attempted'] is not True
                    or response['generated_text'] != outputs[call_id]
                    or response['requested_max_tokens'] != sampling['max_tokens']
                    or response['resolved_max_tokens'] != sampling['max_tokens']
                    or response['budget_attempt'] in attempts or response['request_id'] in request_ids):
                raise ValueError('native text, tokens or charged request identity changed')
            seen.add(key); attempts.add(response['budget_attempt']); request_ids.add(response['request_id'])
            calls.append(dict(call_id=call_id, response=response))
    if seen != set(expected) or attempts != set(range(attempt_offset + 1, attempt_offset + len(expected) + 1)):
        raise ValueError('every source occurrence requires one identified charged native terminal result')
    return dict(calls=calls, native_generate_terminal_count=len(calls), retries=0,
                first_engine_accept=None, kernel_count=None)


def _image_message(messages):
    return any(isinstance(message.get('content'), list) and any(
        part.get('type') == 'image_url' for part in message['content']) for message in messages)


def _native_sampling(sampling):
    native = dict(sampling)
    response_format = native.pop('response_format', None)
    if response_format is not None:
        if response_format != {'type': 'json_object'} or native.get('structured_outputs', {'json_object': True}) != {'json_object': True}:
            raise ValueError('only the original JSON-object response format maps to native structured outputs')
        native['structured_outputs'] = {'json_object': True}
    return native


def _run_original_query(query, config, args, build, service, window, report, unit, tokenizer, *, on_sink=None):
    """Complete original query; serialized SQL carrier, one model/budget owner."""
    from .native_imlane_topk import SQLPredictions, _token_reference
    from .native_service_query import native_query_settings
    from src.baselines.text.frameworks.lotus_native_join import observe_native_join_inputs, describe_native_join_inputs

    predict_sql = SQLPredictions(args, build, service, window, config)
    carrier_lock = threading.Lock()
    providers, join_inputs = [], []
    attempt_offset = unit.attempts

    def predict(requests, *, sampling_params):
        if config['schema'] != QUERY_SCHEMA and sampling_params != query.generation.kwargs():
            raise ValueError('this native query requires its declared standard generation in every batch')
        effective = _native_sampling(sampling_params)
        # The author uses a global registration endpoint. Protect only its SQL
        # scope and bound provider configuration, without changing SDK batching.
        with carrier_lock:
            if len(requests) > unit.remaining:
                raise BudgetExhausted('the next original LM batch exceeds the remaining permanent query budget')
            if not requests:
                return dict(predictions=[])
            if service is None:
                raise ValueError('a nonempty original LM batch requires its declared model owner')
            index = len(providers)
            directory = args.output/'model-calls'/('batch-'+str(index))
            new_private_directory(directory)
            provider = model_provider.configuration(dict(schema=model_provider.CONFIG_SCHEMA,
                provider_mode='model', socket_path=str(service.socket_path), model=service.served_model_name,
                sampling_params=effective, request_timeout_s=config['request_timeout_s'],
                deadline_monotonic=window.query_deadline, receipt_dir=str(directory)))
            path = directory/'provider.json'
            write_private_json(path, provider)
            entry = dict(index=index, directory=str(directory), original_sampling=dict(sampling_params),
                         native_sampling=effective, attempt_offset=unit.attempts, requests=requests)
            providers.append(entry)
            args.workload_environment = {model_provider.CONFIG_ENV: str(path)}
            outputs = predict_sql(requests)
            predictions = [dict(call_id=row['call_id'], generated_text=outputs[row['call_id']]) for row in requests]
            entry['predictions'] = predictions
            return dict(predictions=predictions)

    lm, batches = make_query_lm(query, predict, **query.lm_options)
    execute = query.bind_original_execution()
    report.update(prediction_batches=predict_sql.reports, native_LM_execution_accounting_used=False,
        semantic_owner='original LOTUS queries, accessors, token counting, hierarchy and parsers',
        baseline_identity='lotus-over-imlane-varchar-owned',
        author_lifecycle='fresh native SQL/author scope per original LM batch; one external model service',
        carrier_concurrency='one SQL/configuration lifecycle at a time; global author registration endpoint',
        scheduler_owner='original LOTUS algorithm; native IMLane prediction batches and CPython execution',
        query_clock_version='lotus_imlane.source_to_sink.v1')
    if _aggregation(config):
        report['token_counter'] = dict(config['token_counter'], max_ctx_len=lm.max_ctx_len,
                                       model_tokenizer_verified=False)
    primary = result = None
    try:
        with native_query_settings(lm), observe_native_join_inputs(join_inputs):
            report['query_started_monotonic_ns'] = time.monotonic_ns()
            result = execute()
            ready = time.monotonic_ns()
        report.update(full_sink_observed_monotonic_ns=ready, result_ready_monotonic_ns=ready,
            source_to_sink_seconds=(ready-report['query_started_monotonic_ns'])/1e9,
            native_alive_after_sink=False,
            native_alive_scope=('every prediction SQL scope already closed; external model owner remains live'
                                if service is not None else 'no model or prediction SQL scope allocated'),
            source=query.describe())
        if config['schema'] == QUERY_SCHEMA:
            report.update(model_ready_before_source=True if service is not None else None,
                model_ready_before_source_observed_ns=report.get('model_ready_observed_ns'))
        window.accept_ready(ready, clock='CLOCK_MONOTONIC')
        if on_sink is not None:
            on_sink(ready)
        query.verify_source()
        if query.aggregate_task is not None:
            query.aggregate_task.validate_batch_sizes([len(batch['requests']) for batch in batches], result)
        if query.visual_task is not None:
            query.visual_task.validate_batch_sizes([len(batch['requests']) for batch in batches], result)
        requests = [row for batch in batches for row in batch['requests']]
        if unit.attempts - attempt_offset != len(requests):
            raise ValueError('native permanent attempts differ from the complete original query')
        calls = []
        image_calls = 0
        for entry in providers:
            rows = entry['requests']
            if tokenizer is None and any(not _image_message(row['messages']) for row in rows):
                from transformers import AutoTokenizer
                tokenizer = AutoTokenizer.from_pretrained(config['tokenizer'], local_files_only=True, trust_remote_code=False)
            tokens = [None if _image_message(row['messages']) else _token_reference(tokenizer, row['messages']) for row in rows]
            image_calls += sum(ids is None for ids in tokens)
            ipc = audit_ipc(Path(entry['directory']), rows, tokens, entry['predictions'], entry['native_sampling'],
                attempt_offset=entry['attempt_offset'], native_input_only=True)
            calls.extend(ipc['calls'])
        if len({call['response']['request_id'] for call in calls}) != len(calls):
            raise ValueError('native request identity was reused across original LM batches')
        semantic = audit_query_batches(batches)
        semantic['native_join_inputs'] = describe_native_join_inputs(join_inputs)
        report.update(ipc=dict(calls=calls, native_generate_terminal_count=len(calls), retries=0,
            independent_image_processor_reference_verified=False if image_calls else None,
            image_occurrences=image_calls, text_token_reference='independent tokenizer; unchanged original text audit',
            image_token_evidence='native renderer expanded arrays and EngineInput type; independent processor unverified'),
            semantic=semantic, model_calls=len(requests), quality=json_metric(query.evaluate(result)),
            native_API_usage={key: sum(call['response']['usage'][key] for call in calls)
                              for key in ('prompt_tokens','completion_tokens','total_tokens')})
        return result
    except BaseException as error:
        primary = error
        error.query_result = result
        raise
    finally:
        report['provider_batches'] = [{key:value for key,value in entry.items() if key not in ('requests','predictions')}
                                      for entry in providers]
        report['native_lm_batches'] = [dict(index=index, status=batch['status'],
            occurrences=len(batch['requests']), sampling_params=batch['sampling_params'])
            for index,batch in enumerate(batches)]
        try:
            write_private_json(args.output/'original-lm-batches.json',
                [{key:value for key,value in batch.items() if key != 'native'} for batch in batches])
        except BaseException as error:
            if primary is None:
                error.query_result = result
                raise
            primary.add_note('original LM batch persistence also failed: '+type(error).__name__)


def _run_original_session(query, config, args, build, service, window, report, unit, tokenizer):
    """Two fresh original queries; SQL scopes end per batch, the model ends last."""
    report.update(lifecycle='model-session', queries=[], model_owner_observations=[],
        model_owner_reused=None, warm_qualified=False, result_cache_enabled=False,
        warm_qualification_scope='same birth-matched IPC worker with one AsyncLLM in the fixed worker source; not weights or cache-hit evidence')
    identity = None

    def observe():
        nonlocal identity
        if service is None:
            return None
        actual = service.observe_owner()
        report['model_owner_observations'].append(actual)
        current = {key: value for key, value in actual.items() if key != 'observed_monotonic_ns'}
        if identity is not None and current != identity:
            report['model_owner_reused'] = False
            raise RuntimeError('the original query session model owner changed')
        identity = current
        return actual

    def final_sink(ready):
        window.accept_ready(ready, clock='CLOCK_MONOTONIC')
        if service is not None:
            service.begin_cleanup(window.begin_cleanup())

    observe()
    last_result = None
    try:
        for number in (1, 2):
            window.check()
            if number == 2:
                query, *_ = prepare_query(config, execution_id=config['query_id']+'-round-2')
                window.check()
            round_args = copy(args)
            round_args.output = args.output/('round-'+str(number))
            new_private_directory(round_args.output)
            (round_args.output/'model-calls').mkdir(mode=0o700)
            round_window = _ResultWindow(round_args.output, window.query_deadline-time.monotonic(),
                config['cleanup_s'], config['stop_grace_s'])
            round_window.query_deadline = window.query_deadline
            value = dict(round=number, phase='ready-first' if number == 1 else 'same-owner-repeat',
                status='failed', model_ready_observed_ns=report.get('model_ready_observed_ns'),
                model_owner_before_source=observe(), cost=None, cost_available=False)
            report['queries'].append(value)
            primary = result = None
            try:
                result = _run_original_query(query, config, round_args, build, service,
                    round_window, value, unit, tokenizer, on_sink=final_sink if number == 2 else None)
                value['model_owner_after_sink'] = observe()
                round_window.check()
                value['status'] = 'completed'
            except BaseException as error:
                primary = error
                result = getattr(error, 'query_result', result)
                value.update(error_type=type(error).__name__, error=redact_text(str(error)))
                raise
            finally:
                value.update(result=result_value(result), deadline=round_window.report())
                if value.get('full_sink_observed_monotonic_ns') is not None:
                    last_result = result
                    report.update({key: value[key] for key in ('full_sink_observed_monotonic_ns',
                        'result_ready_monotonic_ns', 'native_alive_after_sink')})
                    report['last_observed_sink_round'] = number
                if primary is not None:
                    primary.query_result = last_result
        report['model_owner_reused'] = True if service is not None else None
        report['warm_qualified'] = service is not None and all(value['model_calls'] > 0 for value in report['queries'])
        report['model_calls'] = sum(value['model_calls'] for value in report['queries'])
        report['query_clock_version'] = 'lotus_imlane.source_to_sink.v1'
        return last_result
    except BaseException as error:
        error.query_result = last_result
        raise

def run_worker(args, config, *, service_factory=ModelService):
    worker_entered = time.monotonic_ns()
    config, generation = configuration(config)
    topk = config['schema'] == TOPK_SCHEMA
    aggregate = _aggregation(config)
    full_query = config['schema'] == QUERY_SCHEMA
    session = config.get('lifecycle') == 'model-session'
    query = tokenizer = None
    if topk:
        from transformers import AutoTokenizer
        from .native_imlane_topk import prepare_query as prepare_topk, run_query as run_topk
        query = prepare_topk(config, generation)
        tokenizer = AutoTokenizer.from_pretrained(config['tokenizer'], local_files_only=True, trust_remote_code=False)
    else:
        query, method, prepared, requests, tokens = prepare_query(config,
            **(dict(execution_id=config['query_id']+'-round-1') if session else {}))
        # Extract's JSON format belongs to the original operator, not a config guess.
        generation = query.generation if query.operation is None else query.operation.generation
        if aggregate:
            from transformers import AutoTokenizer
            tokenizer = AutoTokenizer.from_pretrained(config['tokenizer'], local_files_only=True, trust_remote_code=False)
    build = verified_build(args.build_record)
    args.output = Path(args.output).resolve()
    new_private_directory(args.output)
    window = _ResultWindow(args.output, config['query_timeout_s'], config['cleanup_s'],
        config['stop_grace_s'], cleanup_path=args.output / 'cleanup-start.json')
    report = dict(schema=config['schema'], variant=VARIANT, status='failed', worker_entry_monotonic_ns=worker_entered,
        strict_controller_entry_ns=None, strict_controller_entry_reason='host preflight and lease are external',
        build_record=file_identity(Path(args.build_record)), extra_warmup=0, retries=0,
        cache='LOTUS result cache off; native engine cache settings and readiness are separate',
        outer_host_lease_required=True, physical_gpu_exit_confirmed=None, generation=asdict(generation),
        cost=None, cost_available=False)
    args.scopes = []
    args.stop_budget = owner.StopBudget(threading.Event())
    args.stop_grace_seconds = config['stop_grace_s']
    args.duckdb_library = Path(build['duckdb_library_path']).resolve()
    previous = {number: signal.signal(number, args.stop_budget.signal) for number in (signal.SIGTERM, signal.SIGINT)}
    primary = service = ledger = unit = result = None
    try:
        for name in ('model-calls', 'model-service'):
            (args.output / name).mkdir(mode=0o700)
        allowance = config['max_model_calls']
        budget = AttemptBudget(config['query_id'], allowance)
        ledger = CellBudgetLedger.create(args.output / 'physical-budget.sqlite', budget,
            deadline_utc=time.time() + config['query_timeout_s'])
        ledger.reserve_unit('query', allowance)
        unit = ledger.claim_shared_unit('query')
        descriptor = dict(path=str(ledger.path), budget_id=budget.budget_id, limit=allowance, unit_id='query')
        sampling = generation.kwargs()
        needs_model = aggregate or query.describe()['expected_model_calls'] is None or bool(query.describe()['expected_model_calls'])
        if needs_model:
            service = service_factory(dict(config['model_service'], request_budget=descriptor),
                args.output / 'model-service', environment=dict(os.environ), deadline=window.query_deadline,
                cleanup_deadline=window.begin_cleanup, stop_budget=args.stop_budget,
                stop_grace_s=config['stop_grace_s'])
            if not (aggregate or full_query):
                provider = model_provider.configuration(dict(schema=model_provider.CONFIG_SCHEMA,
                    provider_mode='model', socket_path=str(service.socket_path), model=service.served_model_name,
                    sampling_params=sampling, request_timeout_s=config['request_timeout_s'],
                    deadline_monotonic=window.query_deadline, receipt_dir=str(args.output / 'model-calls')))
                write_private_json(args.output / 'model-provider.json', provider)
                args.workload_environment = {model_provider.CONFIG_ENV: str(args.output / 'model-provider.json')}
        with service if service is not None else nullcontext():
            if service is not None:
                report['model_ready_observed_ns'] = time.monotonic_ns()
                report['model_owner'] = service.ready_info
                report['model_startup_seconds'] = (
                    report['model_ready_observed_ns'] / 1e9 - service.report['started_monotonic'])
            else:
                report['model_owner'] = dict(allocated=False)
                report['model_startup_seconds'] = None
            if aggregate or full_query:
                if aggregate and service.ready_info['max_model_len'] != query.lm_options['max_ctx_len']:
                    raise ValueError('the actual native model context differs from the LOTUS aggregation context')
                try:
                    execute = _run_original_session if session else _run_original_query
                    result = execute(query, config, args, build, service, window, report, unit, tokenizer)
                finally:
                    if service is not None:
                        service.begin_cleanup(window.begin_cleanup())
            elif topk:
                try:
                    result = run_topk(query, config, args, build, service, window, report, unit, tokenizer)
                finally:
                    if service is not None:
                        service.begin_cleanup(window.begin_cleanup())
            else:
                def audit(native, value):
                    if Path(native['converter_resolved_DSO']).resolve() != args.duckdb_library:
                        raise ValueError('native driver used a different DSO')
                    semantic = relation.audit()
                    ipc = audit_ipc(args.output / 'model-calls', requests, tokens, relation.predictions, sampling)
                    query.verify_source()
                    return dict(semantic=semantic, ipc=ipc, quality=json_metric(query.evaluate(value)))
                execute_query = query.bind_execution()
                if service is not None and max(map(len, tokens), default=0) + generation.max_tokens > service.ready_info['max_model_len']:
                    raise ValueError('original full prompt plus output exceeds actual model context')
                report['source'] = query.describe()
                # The common primary clock starts with a fresh source after model
                # readiness. Keep startup and worker-entry totals in separate fields.
                started = time.monotonic_ns()
                report['query_started_monotonic_ns'] = started
                method, prepared, requests = prepare(query)
                with open_private_text(args.output / 'input.csv') as stream:
                    writer = csv.writer(stream); writer.writerow(('call_id', 'messages_json'))
                    writer.writerows((row['call_id'], row['messages_json']) for row in prepared)
                relation = PreparedRelation(query, method, prepared, execute_query=execute_query)
                args.startup_timeout_s = window.query_deadline - time.monotonic()
                try:
                    result, audited = run_relation_query(args, report, kernel=Path(build['kernel_path']),
                        command=prediction_command(args, build), window=window, query_started_ns=started,
                        sink=relation.sink, audit=audit, check=(lambda: None) if service is None else service.check)
                    report.update(quality=audited['quality'], ipc=audited['ipc'])
                    write_private_json(args.output / 'semantic-audit.json', audited['semantic'])
                finally:
                    if service is not None:
                        service.begin_cleanup(window.begin_cleanup())
        report.update(status='completed', service_close=(dict(status='closed', scope='no model requests')
            if service is None else service.report), result=result_value(result))
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
        if primary is not None:
            report['status'] = 'failed'
            if session:
                report['warm_qualified'] = False
        if report.get('full_sink_observed_monotonic_ns') is not None:
            report['cold_worker_entry_to_sink_seconds'] = (
                report['full_sink_observed_monotonic_ns'] - worker_entered) / 1e9
            report['ready_source_to_sink_seconds'] = (None if full_query and service is None
                                                     else report.get('source_to_sink_seconds'))
        report['timing_scope'] = ('source_to_sink_seconds/ready_source_to_sink_seconds: model-ready then fresh '
            'CSV/source, original prompt preparation, '
            'required native transport/projection, original parser and complete memory sink; '
            'cold_worker_entry_to_sink_seconds also includes worker preflight and model startup; '
            'neither clock includes post-sink audit, evaluation, persistence or final model close; '
            'TopK includes each required prediction SQL scope close before the next original comparison. '
            'Worker entry excludes host preflight/lease and Python import/CLI before run_worker.')
        if aggregate or full_query:
            report['timing_scope'] = ('independent lotus_imlane.source_to_sink.v1: fresh original source after model-ready when a model is needed; '
                'zero/no-op has no READY prerequisite; '
                'original LOTUS operators/algorithms and full result; each batch includes necessary CSV/SQL, relation publication, '
                'mapping, ACK and native scope close before the next original stage; synchronous provider fingerprints, '
                'permanent attempt reservation and receipt I/O remain included. Input/LM/tokenizer preflight, '
                'post-sink audits, evaluation, final persistence and model close are excluded. '
                'Not the shared v3 comparison scope; cold_worker_entry_to_sink_seconds also includes startup.')
        if session:
            report['timing_scope'] += (' Each query has its own direct source-to-sink clock in queries; '
                'the root cold worker clock ends at the last observed sink and includes the earlier post-sink work. '
                'query_timeout_s is the whole startup/two-query work allowance, not a renewed per-query limit.')
        report['deadline'] = window.report()
        try:
            persist_relation_observation(args.output, report, result, primary=primary)
        except BaseException as error:
            primary = error
            report.update(status='failed', error_type=type(error).__name__)
            if session:
                report['warm_qualified'] = False
        try:
            write_private_json(args.output / 'worker.json', report)
        except BaseException as error:
            report['status'] = 'failed'
            if session:
                report['warm_qualified'] = False
            if primary is None:
                primary = error
                error.query_report, error.query_result = report, result
            else:
                primary.add_note('worker report also failed: ' + type(error).__name__)
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
            query_timings=[{key: value[key] for key in ('round', 'phase', 'status', 'source_to_sink_seconds')}
                           for value in terminal.get('queries', [])],
            strict_host_controller_entry_seconds=None,
            cold_boundary='existing supervisor started_monotonic before log open/Popen; '
                          'excludes host imports/preflight/lease before supervise',
            ready_query_boundary='fresh source/prepare after actual owner ready through original full memory sink',
            comparison='select the same observed start boundary on every compared system; '
                       'do not equate ready-query and startup-inclusive cold totals'))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
