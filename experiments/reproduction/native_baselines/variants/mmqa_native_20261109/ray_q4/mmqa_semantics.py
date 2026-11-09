"""Original SemBench MMQA Q4 driver semantics; no native engine imports."""
import json
from src.infrastructure.private_artifacts import content_digest,json_metric

def query_for(config):
    from src.baselines.text.sembench_text import SemBenchTextQuery
    from src.serving.backends.vllm_protocol import GenerationSpec
    sampling = dict(config['sampling_params'])
    assert sampling.pop('n') == 1
    return SemBenchTextQuery(config['sembench_checkout'], config['data_dir'], 'mmqa', '4',
        execution_id=config['query_id'], model_name=config['model_name'],
        generation=GenerationSpec.from_dict(sampling))


def prepare(config, tokenizer):
    import lotus
    from src.semantic_methods.lotus.record_method import LotusRecordMethod
    query = query_for(config)
    operation = query.operation
    assert operation.stage.kind == 'extract' and len(operation.records) == 24
    with lotus.settings.context(enable_cache=False):
        method = LotusRecordMethod(operation.stage)
        prepared = method.prepare_rows(operation.records, query_id=config['query_id'])
    requests = [dict(call_id=row['call_id'], prompt=tokenizer.apply_chat_template(
        json.loads(row['messages_json']), tokenize=False, add_generation_prompt=True)) for row in prepared]
    return query, method, prepared, requests


def finish(query, method, prepared, predictions):
    from copy import deepcopy
    from lotus.sem_ops.postprocessors import extract_postprocess
    by_id = {row['call_id']: row['generated_text'] for row in predictions}
    if len(by_id) != len(predictions) or set(by_id) != {r['call_id'] for r in prepared}:
        raise ValueError('MMQA Q4 requires every unique original occurrence')
    texts = [by_id[row['call_id']] for row in prepared]
    parsed = extract_postprocess(texts, method.lm, False)
    assert len(parsed.outputs) == len(prepared) == 24
    calls = [dict(row, status='succeeded', text=text,
                  messages_sha256=content_digest(json.loads(row['messages_json'])))
             for row, text in zip(prepared, texts, strict=True)]
    def original_operator(operation, invoke):
        assert operation.stage == query.operation.stage and len(operation.records) == 24
        records = deepcopy(operation.records)
        for record, output in zip(records, parsed.outputs, strict=True):
            record.values.update(output)
            record.index_updates.update(output)
        return dict(frame=operation.result_frame(records), calls=calls)
    result, _ = query.execute(original_operator)
    return result, dict(calls=calls, parsed_outputs=parsed.outputs), texts


def frame_value(result):
    return json_metric(result.to_dict(orient='split'))
