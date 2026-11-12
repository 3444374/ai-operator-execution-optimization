"""Original LOTUS quick ranking over the existing native VARCHAR prediction API.

One caller-owned model service spans the query. Each original LM batch uses a
fresh author SQL scope; its required shutdown is part of the measured query.
This is the VARCHAR engineering variant, not an author TopK algorithm.
"""
from copy import copy, deepcopy
import csv
import json
from pathlib import Path
import time

from lotus.models import LM
from lotus.types import LMOutput
import numpy as np

from src.baselines.text.frameworks.lotus_native_topk import comparisons, make_lm, audit as audit_topk
from src.baselines.text.sembench_prepared import result_value
from src.baselines.text.sembench_text import SemBenchTextQuery
from src.experiments.native_imlane_model_query import QueryWindow
from src.experiments.native_imlane_relation import run_relation_query
from src.experiments.native_service_query import native_query_settings
from src.infrastructure.private_artifacts import content_digest, json_metric, new_private_directory, open_private_text


def prepare_query(config, generation):
    task = config['task']
    query = SemBenchTextQuery(config['sembench_checkout'], config['data_dir'], task['scenario'], task['query'],
        execution_id=config['query_id'], model_name=config['model_name'], generation=generation,
        scale_factor=task.get('scale_factor'), ranking='topk')
    if {name: value['sha256'] for name, value in query.files.items()} != config['input_sha256']:
        raise ValueError('TopK input files differ from their frozen identities')
    if query.operation is not None and query.operation.describe()['kind'] != 'topk':
        raise ValueError('this native prediction profile requires the original full quick ranking')
    if config['max_model_calls'] != max(1, query.describe()['expected_model_calls']):
        raise ValueError('declare the original full-ranking comparison upper bound')
    return query


class SQLPredictions:
    """Native per-batch SQL, retaining one external model owner and its budget."""
    def __init__(self, args, build, service, window, config):
        self.args, self.build, self.service, self.window, self.config = args, build, service, window, config
        self.reports = []

    def __call__(self, requests):
        from src.experiments.native_imlane_sembench import prediction_command

        self.window.check()
        args = copy(self.args)
        args.output = Path(self.args.output) / ('prediction-' + str(len(self.reports)))
        new_private_directory(args.output)
        args.scopes = []
        args.startup_timeout_s = self.window.query_deadline - time.monotonic()
        stage_window = QueryWindow(args.startup_timeout_s, self.config['cleanup_s'],
                                   self.config['stop_grace_s'])
        report = dict(scope='one original LM batch; not the complete SemBench sink', input_rows=len(requests))
        self.reports.append(report)
        started = time.monotonic_ns()
        with open_private_text(args.output / 'input.csv') as stream:
            writer = csv.writer(stream)
            writer.writerow(('call_id', 'messages_json'))
            writer.writerows((row['call_id'], json.dumps(row['messages'], ensure_ascii=False)) for row in requests)
        expected = {row['call_id'] for row in requests}

        def sink(native):
            if (native['return_type'] != 'VARCHAR' or not native['full_projection_materialized']
                    or native['variant'] != 'varchar-owned-engineering' or native['source_rows'] != len(requests)
                    or Path(native['converter_resolved_DSO']).resolve() != Path(args.duckdb_library).resolve()):
                raise ValueError('complete prediction relation from the declared native VARCHAR DSO required')
            rows = native['mapped_occurrences']
            predictions = {row['call_id']: row['generated_text'] for row in rows}
            if (len(predictions) != len(rows) or set(predictions) != expected
                    or any(type(text) is not str for text in predictions.values())):
                raise ValueError('native prediction lost, duplicated or changed an original occurrence')
            return predictions

        def check():
            self.window.check()
            self.service.check()

        try:
            predictions, _ = run_relation_query(args, report, kernel=Path(self.build['kernel_path']),
                command=prediction_command(args, self.build), window=stage_window, query_started_ns=started,
                sink=sink, audit=lambda _native, _value: None, check=check)
        except BaseException as error:
            # A prediction batch is not a partially completed frontend query.
            error.native_prediction_result = getattr(error, 'query_result', None)
            error.query_result = None
            raise
        self.window.check()
        return predictions


class PredictionLM(LM):
    """The public LM output interface; prediction batching belongs to IMLane.

    The original accessor owns formatting, token estimates, parsing and ranking.
    Base LM execution/cache/usage counters are not used; report IPC usage instead.
    """
    def __call__(self, messages, show_progress_bar=True, progress_bar_desc='', **kwargs):
        if kwargs or len(messages) != len(self.source_pairs):
            raise ValueError('original plain quick comparison batch required')
        requests = [dict(call_id=self.execution_id + '-partition-' + str(self.partition) +
            '-pair-' + str(left) + '-' + str(right), messages=message)
            for (left, right), message in zip(self.source_pairs, messages, strict=True)]
        self.requests.extend(requests)
        self.pairs.extend(dict(call_id=row['call_id'], left=left, right=right, messages=row['messages'])
            for row, (left, right) in zip(requests, self.source_pairs, strict=True))
        self.partition += 1
        outputs = self.predict(requests)
        self.predictions.extend(dict(call_id=row['call_id'], generated_text=outputs[row['call_id']]) for row in requests)
        return LMOutput(outputs=[outputs[row['call_id']] for row in requests])


def _token_reference(tokenizer, messages):
    # Audit only: vLLM0.25.1 auto string-content normalization of text parts.
    # The native prediction receives the unchanged original messages.
    conversation = deepcopy(messages)
    for message in conversation:
        if isinstance(message['content'], list):
            if any(part['type'] != 'text' for part in message['content']):
                raise ValueError('this profile qualifies text comparisons only')
            message['content'] = '\n'.join(part['text'] for part in message['content'] if part['text'])
    return tokenizer.apply_chat_template(conversation, tokenize=True, add_generation_prompt=True)


def run_query(query, config, args, build, service, window, report, budget, tokenizer):
    from src.experiments.native_imlane_sembench import audit_ipc

    operation = query.operation
    predict = SQLPredictions(args, build, service, window, config)
    lm = None if operation is None else make_lm(operation, 64, lm_type=PredictionLM)
    if lm is not None:
        lm.predict, lm.execution_id = predict, query.query_id
        lm.requests, lm.predictions, lm.pairs = [], [], []
        lm.partition = 0
    relation = operator = None

    def execute(actual, invoke):
        nonlocal relation, operator
        state = np.random.get_state()
        operator = dict(rng_state=[state[0], state[1].tolist(), int(state[2]), int(state[3]), float(state[4])])

        def batch(pairs, compare):
            lm.source_pairs = pairs
            return compare()

        with comparisons(actual, lm, batch):
            frame, stats = invoke()
        relation = frame.copy(deep=True)
        operator.update(frame=frame, native_stats=stats)
        return operator

    execute_query = query.bind_execution()
    report.update(prediction_batches=predict.reports, native_LM_execution_accounting_used=False,
        author_lifecycle='fresh native SQL/author scope per original LM batch; one external model service',
        scheduler_owner='original LOTUS quick; native IMLane prediction batches and CPython execution',
        cost=None, cost_available=False)
    result = None
    try:
        report['query_started_monotonic_ns'] = time.monotonic_ns()
        with native_query_settings(lm):
            result, _ = execute_query(execute)
        ready = time.monotonic_ns()
        report.update(full_sink_observed_monotonic_ns=ready, result_ready_monotonic_ns=ready,
            source_to_sink_seconds=(ready-report['query_started_monotonic_ns']) / 1e9,
            query_clock_version='sembench.source_to_sink.v3', native_alive_after_sink=False,
            native_alive_scope='per-batch native SQL already closed; external model owner remains live',
            source=query.describe())
        window.accept_ready(ready, clock='CLOCK_MONOTONIC')
        query.verify_source()
        requests = [] if lm is None else lm.requests
        predictions = [] if lm is None else lm.predictions
        if budget.attempts != len(requests):
            raise ValueError('native permanent attempts differ from original comparisons')
        tokens = [_token_reference(tokenizer, row['messages']) for row in requests]
        ipc = audit_ipc(Path(args.output) / 'model-calls', requests, tokens, predictions, query.generation.kwargs())
        if operator is not None:
            operator.update(pairs=lm.pairs, calls=[dict(call_id=row['call_id'], text=row['generated_text'],
                status='succeeded', messages_sha256=content_digest(request['messages']))
                for row, request in zip(predictions, requests, strict=True)])
            if operator['native_stats']['total_llm_calls'] != len(requests):
                raise ValueError('original quick statistics differ from completed native predictions')
            audit_topk(query, operator, result, relation)
            report.update(operator_relation=result_value(relation), native_stats=operator['native_stats'])
        report.update(ipc=ipc, model_calls=len(requests), quality=json_metric(query.evaluate(result)),
            native_API_usage={key: sum(call['response']['usage'][key] for call in ipc['calls'])
                              for key in ('prompt_tokens', 'completion_tokens', 'total_tokens')})
        return result
    except BaseException as error:
        error.query_result = result
        raise
    finally:
        if operator is not None:
            report['topk'] = dict(rng_state=operator['rng_state'],
                comparison_occurrences=[{key: pair[key] for key in ('call_id', 'left', 'right')}
                                        for pair in lm.pairs])
