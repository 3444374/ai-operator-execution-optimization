"""Original MMQA source, LOTUS format/parser and author sink only."""
import json
from src.infrastructure.private_artifacts import content_digest,json_metric

def query_for(config):
    from src.baselines.text.sembench_text import SemBenchTextQuery
    from src.serving.backends.vllm_protocol import GenerationSpec
    sampling = dict(config['sampling_params'])
    assert sampling.pop('n') == 1
    return SemBenchTextQuery(config['sembench_checkout'], config['data_dir'], 'mmqa', '3a',
        execution_id=config['query_id'], model_name=config['model_name'],
        generation=GenerationSpec.from_dict(sampling))

def prepare(config, tokenizer):
    import lotus
    from src.semantic_methods.lotus.record_method import LotusRecordMethod
    query = query_for(config)
    operation = query.operation
    assert operation.stage.kind == 'filter' and len(operation.records) == 25
    with lotus.settings.context(enable_cache=False):
        method = LotusRecordMethod(operation.stage)
        prepared = method.prepare_rows(operation.records, query_id=config['query_id'])
    requests = [dict(call_id=row['call_id'], prompt=tokenizer.apply_chat_template(
        json.loads(row['messages_json']), tokenize=False, add_generation_prompt=True)) for row in prepared]
    return query, method, prepared, requests

def finish(query, method, prepared, predictions):
    from lotus.sem_ops.postprocessors import filter_postprocess
    by_id = {row['call_id']: row['generated_text'] for row in predictions}
    if len(by_id) != len(predictions) or set(by_id) != {r['call_id'] for r in prepared}:
        raise ValueError('MMQA Q3a requires every unique original occurrence')
    texts = [by_id[row['call_id']] for row in prepared]
    parsed = filter_postprocess(texts, method.lm, method.method.default,
                               output_tokens=method.method.output_tokens)
    assert len(parsed.outputs) == len(prepared) == 25
    decisions = [dict(row_id=row['row_id'], source_position=row['source_position'], decision=value)
                 for row, value in zip(prepared, parsed.outputs, strict=True)]
    calls = [dict(row, status='succeeded', text=text,
                  messages_sha256=content_digest(json.loads(row['messages_json'])))
             for row, text in zip(prepared, texts, strict=True)]

    def original_operator(operation, invoke):
        assert operation.stage == query.operation.stage
        assert len(operation.records) == len(decisions)
        selected = [record for record, decision in zip(operation.records, decisions, strict=True)
                    if decision['decision']]
        return dict(frame=operation.result_frame(selected), calls=calls)

    result, operator = query.execute(original_operator)
    return result, dict(calls=calls, decisions=decisions), texts

def frame_value(result):
    return json_metric(result.to_dict(orient='split'))
