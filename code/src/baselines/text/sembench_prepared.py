"""Original single-operator semantics around identity-keyed native predictions.

This is source/prompt/parser/sink wiring, not a model executor. The native
framework receives complete chat-serialized prompts and owns their scheduling.
"""

from copy import deepcopy
import json

from src.infrastructure.private_artifacts import content_digest, json_metric
from src.semantic_methods.lotus.record_method import LotusRecordMethod


def prepare(query, tokenizer):
    import lotus

    if query.operation is None:
        return None, [], []
    with lotus.settings.context(enable_cache=False):
        method = LotusRecordMethod(query.operation.stage)
        prepared = method.prepare_rows(query.operation.records, query_id=query.query_id)
    requests = [dict(call_id=row['call_id'], prompt=tokenizer.apply_chat_template(
        json.loads(row['messages_json']), tokenize=False, add_generation_prompt=True)) for row in prepared]
    return method, prepared, requests


def finish(query, method, prepared, predictions):
    """Use source identities, native postprocessors and the unchanged full query."""
    from lotus.sem_ops.postprocessors import extract_postprocess, map_postprocess
    from src.semantic_methods.lotus.filter_answers import parse_filter_answers

    by_id = {row['call_id']: row['generated_text'] for row in predictions}
    if len(by_id) != len(predictions) or set(by_id) != {row['call_id'] for row in prepared}:
        raise ValueError('native predictions require every unique original occurrence')
    texts = [by_id[row['call_id']] for row in prepared]
    calls = [dict(row, text=text, status='succeeded',
                  messages_sha256=content_digest(json.loads(row['messages_json'])))
             for row, text in zip(prepared, texts, strict=True)]
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
        return dict(frame=operation.result_frame(output_records), calls=calls,
                    parsed_outputs=parsed.outputs)

    result, operator = query.execute(original_operator)
    return result, dict(calls=calls,
        parsed_outputs=None if parsed is None else parsed.outputs,
        operator_relation=None if operator is None else result_value(operator['frame']))


def result_value(result):
    import pandas as pd

    if isinstance(result, pd.Series):
        result = result.to_frame()
    if isinstance(result, pd.DataFrame):
        result = result.to_dict(orient='split')
    return json_metric(result)
