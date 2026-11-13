"""Original query LM and typed prepared-operator prediction boundaries.

This is source/prompt/parser/sink wiring, not a model executor. The native
framework receives complete messages or serialized prompts and owns scheduling.
"""

from copy import deepcopy
from dataclasses import replace
import json
from threading import Lock

from src.infrastructure.private_artifacts import content_digest, json_metric
from src.semantic_methods.lotus.record_method import LotusRecordMethod


def make_query_lm(query, predict, *, max_ctx_len=None, tokenizer=None):
    """Run complete original LOTUS queries over a backend's native predictions.

    LOTUS owns the accessors, actual later inputs, prompts, parsers, algorithms
    and final relation. The backend receives each real LM batch, owns execution
    and closes at query scope. It does not use LOTUS LM execution/cache/usage;
    backend receipts are the accounting source, including unknown usage.
    Explicit context/tokenizer parameters retain native aggregation's token
    estimates; callers that omit them keep the established LM defaults.
    """
    from lotus.models import LM
    from lotus.types import LMOutput

    batches = []
    batch_lock = Lock()

    class QueryLM(LM):
        def __call__(self, messages, show_progress_bar=True, progress_bar_desc='', **kwargs):
            requests = [dict(messages=deepcopy(message)) for message in messages]
            sampling = dict(query.generation.kwargs(), **kwargs)
            if sampling.get('logprobs') is False:
                sampling['logprobs'] = None
            if (sampling.get('structured_outputs') == {'json_object': True}
                    and sampling.get('response_format') == {'type': 'json_object'}):
                sampling.pop('response_format')
            batch = dict(requests=requests, operator_kwargs=deepcopy(kwargs),
                sampling_params=deepcopy(sampling), status='submitted')
            # Grouped native operators may call this LM from several threads.
            # Register identities together; prediction remains concurrent.
            with batch_lock:
                index = len(batches)
                for position, request in enumerate(requests):
                    request['call_id'] = f'{query.query_id}-batch-{index}-row-{position}'
                batches.append(batch)
            try:
                native = predict(requests, sampling_params=sampling)
                batch['native'] = native
                predictions = native['predictions']
                by_id = {row['call_id']: row['generated_text'] for row in predictions}
                if (len(by_id) != len(predictions)
                        or set(by_id) != {row['call_id'] for row in requests}):
                    raise ValueError('native query predictions require every unique batch occurrence')
                batch.update(status='completed', predictions=predictions)
                return LMOutput(outputs=[by_id[row['call_id']] for row in requests])
            except BaseException as error:
                batch.update(status='failed', error_type=type(error).__name__)
                if 'native' not in batch and hasattr(error, 'native_query_report'):
                    batch['native'] = error.native_query_report
                raise

    # The native algorithms still use LM token estimation and generation
    # metadata. max_batch_size belongs to LOTUS execution, which this seam does
    # not run; no value is used to split a backend's real batch.
    options = {}
    if max_ctx_len is not None:
        options["max_ctx_len"] = max_ctx_len
    if tokenizer is not None:
        options["tokenizer"] = tokenizer
    return QueryLM(query.model_name, **options, **query.generation.kwargs()), batches


def audit_query_batches(batches):
    """Associate raw replies with actual SDK batches after the complete sink."""
    calls = []
    for index, batch in enumerate(batches):
        calls.extend(dict(call_id=row['call_id'], batch=index, source_position=position,
            messages_sha256=content_digest(row['messages']),
            status='succeeded') for position, row in enumerate(batch['requests']))
    return dict(calls=calls, batches=len(batches), native_lm_execution_used=False,
                semantic_owner='original LOTUS accessors/algorithms', usage_owner='native backend receipts')


def prepare(query, tokenizer=None, *, prompt_columns=None):
    if query.operation is None:
        return None, [], []
    kind = query.operation.describe()['kind']
    if kind == 'join':
        if prompt_columns is not None:
            raise ValueError('the native Join accessor owns its prompt column order')
        from .sembench_prepared_join import prepare as prepare_join
        method, prepared = prepare_join(query)
    elif kind in ('filter', 'map', 'extract'):
        method, prepared = _prepare_rows(query, prompt_columns)
    else:
        raise ValueError('prepared text requests support Filter/Map/Extract/Join, not ' + kind)
    # Ray owns chat templating; Daft consumes the same serialized messages.
    requests = []
    for row in prepared:
        messages = json.loads(row['messages_json'])
        request = dict(call_id=row['call_id'])
        if tokenizer is None:
            request['messages'] = messages
        else:
            request['prompt'] = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
        requests.append(request)
    return method, prepared, requests


def _prepare_rows(query, prompt_columns):
    import lotus

    stage = query.operation.stage
    # Reuse the first observed raw order across processes, not display headers
    # or a fresh iteration of LOTUS placeholder sets. Default callers are unchanged.
    if prompt_columns is not None:
        original = (stage.method.input_columns if stage.kind == 'extract'
                    else stage.method.resolved_input_columns)
        if (type(prompt_columns) is not list or len(set(prompt_columns)) != len(prompt_columns)
                or set(prompt_columns) != set(original)):
            raise ValueError('freeze exactly the original prompt columns in their observed order')
        field = 'input_columns' if stage.kind == 'extract' else 'resolved_input_columns'
        stage = replace(stage, method=replace(stage.method, **{field: tuple(prompt_columns)}))
    with lotus.settings.context(enable_cache=False):
        method = LotusRecordMethod(stage)
        prepared = method.prepare_rows(query.operation.records, query_id=query.query_id)
    return method, prepared


def finish_relation(query, method, prepared, predictions, *, execute_query=None):
    """Run the original parser and full query without post-result audit work."""
    from lotus.sem_ops.postprocessors import extract_postprocess, map_postprocess
    from src.semantic_methods.lotus.filter_answers import parse_filter_answers

    by_id = {row['call_id']: row['generated_text'] for row in predictions}
    if len(by_id) != len(predictions) or set(by_id) != {row['call_id'] for row in prepared}:
        raise ValueError('native predictions require every unique original occurrence')
    texts = [by_id[row['call_id']] for row in prepared]
    if query.operation is not None and query.operation.describe()['kind'] == 'join':
        from .sembench_prepared_join import finish_relation as finish_join
        return finish_join(query, method, texts, execute_query)
    if method is None:
        parsed = None
    elif method.stage.kind == 'filter':
        parsed = parse_filter_answers(texts, method.lm, method.method.default,
            output_tokens=method.method.output_tokens, answer_parser=method.method.answer_parser)
    elif method.stage.kind == 'map':
        parsed = map_postprocess(texts, method.lm, False)
    else:
        parsed = extract_postprocess(texts, method.lm, False)

    def original_operator(operation, invoke):
        records = deepcopy(operation.records)
        output_records = []
        for record, value in zip(records, parsed.outputs, strict=True):
            if operation.stage.kind == 'filter':
                if value:
                    output_records.append(record)
                continue
            if operation.stage.kind == 'map':
                record.values[operation.stage.method.output_column] = value
            else:
                record.values.update(value)
                record.index_updates.update(value)
            output_records.append(record)
        return dict(frame=operation.result_frame(output_records), parsed_outputs=parsed.outputs)

    result, operator = (query.execute if execute_query is None else execute_query)(original_operator)
    return result, dict(texts=texts, parsed_outputs=None if parsed is None else parsed.outputs,
                        operator=operator)


def audit_relation(prepared, relation):
    """Describe a completed relation after its caller records the memory sink."""
    # Message digests and relation serialization are audit work, not part of
    # the original parser/query. Keep failures visible to the caller.
    calls = [dict(row, text=text, status='succeeded',
                  messages_sha256=content_digest(json.loads(row['messages_json'])))
             for row, text in zip(prepared, relation['texts'], strict=True)]
    operator = relation['operator']
    report = dict(calls=calls, parsed_outputs=relation['parsed_outputs'],
                  operator_relation=None if operator is None else result_value(operator['frame']))
    if 'native_join_inputs' in relation:
        from .sembench_prepared_join import audit
        report['native_join_inputs'] = audit(prepared, relation)
    return report


def finish(query, method, prepared, predictions, *, on_result_ready=None, execute_query=None):
    """Run original semantics, expose the full sink, then build audit artifacts."""
    result, relation = finish_relation(query, method, prepared, predictions, execute_query=execute_query)
    if on_result_ready is not None:
        on_result_ready(result)
    if execute_query is not None:
        query.verify_source()
    return result, audit_relation(prepared, relation)


def result_value(result):
    import pandas as pd

    if isinstance(result, pd.Series):
        result = result.to_frame()
    if isinstance(result, pd.DataFrame):
        result = result.to_dict(orient='split')
    return json_metric(result)
