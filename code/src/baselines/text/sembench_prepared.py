"""Original single-operator semantics around identity-keyed native predictions.

This is source/prompt/parser/sink wiring, not a model executor. The native
framework receives complete messages or serialized prompts and owns scheduling.
"""

from copy import deepcopy
from dataclasses import replace
import json

from src.infrastructure.private_artifacts import content_digest, json_metric
from src.semantic_methods.lotus.record_method import LotusRecordMethod


def prepare(query, tokenizer=None, *, prompt_columns=None):
    import lotus

    if query.operation is None:
        return None, [], []
    kind = query.operation.describe()['kind']
    if kind not in ('filter', 'map', 'extract'):
        raise ValueError('prepared text requests support Filter/Map/Extract, not ' + kind)
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
    # Ray's public processor owns chat templating/tokenization. Daft consumes
    # serialized prompts. Both retain the same original occurrence and messages.
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


def finish_relation(query, method, prepared, predictions, *, execute_query=None):
    """Run the original parser and full query without post-result audit work."""
    from lotus.sem_ops.postprocessors import extract_postprocess, map_postprocess
    from src.semantic_methods.lotus.filter_answers import parse_filter_answers

    by_id = {row['call_id']: row['generated_text'] for row in predictions}
    if len(by_id) != len(predictions) or set(by_id) != {row['call_id'] for row in prepared}:
        raise ValueError('native predictions require every unique original occurrence')
    texts = [by_id[row['call_id']] for row in prepared]
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
    return dict(calls=calls, parsed_outputs=relation['parsed_outputs'],
                operator_relation=None if operator is None else result_value(operator['frame']))


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
