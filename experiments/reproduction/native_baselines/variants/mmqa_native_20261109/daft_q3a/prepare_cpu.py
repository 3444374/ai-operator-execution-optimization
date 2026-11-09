"""Replay the 25 saved original answers; no model or URL/image fetch."""
import json
from pathlib import Path

from src.infrastructure.private_artifacts import content_digest, json_metric, write_private_json
from native_daft_mmqa_q3a import prepare
import mmqa_semantics

ROOT = Path(__file__).resolve().parent
config = json.loads((ROOT/'CONFIG.json').read_text())
REFERENCE = Path(config['reference_query_json'])
from transformers import AutoTokenizer
tokenizer = AutoTokenizer.from_pretrained(config['model'], local_files_only=True)
query, method, prepared, requests = prepare(config, tokenizer)
reference = json.loads(REFERENCE.read_text())
prior = reference['operator']['calls']['records']
assert len(prepared) == len(prior) == 25
assert query.operation.stage.method.instruction == '{title} is a comedy movie given their description: {text}'
assert query.operation.describe()['model_columns'] == ['title', 'text']
from transformers import AutoTokenizer
tokenizer = AutoTokenizer.from_pretrained(config['model'], local_files_only=True)
ids = []
for row, request, saved in zip(prepared, requests, prior, strict=True):
    assert row['source_position'] == saved['source_position']
    assert content_digest(json.loads(row['messages_json'])) == saved['messages_sha256']
    expected = json.loads(saved['prompt_token_ids_json'])
    assert tokenizer.encode(request['prompt'], add_special_tokens=False) == expected
    ids.append(expected)
predictions = list(reversed([dict(call_id=row['call_id'], generated_text=saved['text'])
    for row, saved in zip(prepared, prior, strict=True)]))
result, semantic, texts = mmqa_semantics.finish(query, method, prepared, predictions)
quality = json_metric(query.evaluate(result))
assert mmqa_semantics.frame_value(result) == reference['result']
assert quality == reference['quality']
assert list(result.columns) == ['title']
for bad in (predictions[:-1], predictions[:-1] + [predictions[0]]):
    try:
        mmqa_semantics.finish(query, method, prepared, bad)
    except ValueError:
        pass
    else:
        raise AssertionError('Missing/duplicate original occurrence accepted')
write_private_json(ROOT/'CPU-TOKEN-REFERENCES.json', dict(ids=ids, prompts=[r['prompt'] for r in requests]))
write_private_json(ROOT/'CPU-DRIVER.json', dict(status='passed', source_rows=25, messages=25,
    original_messages_and_prompt_tokens_matched=25, source_order_and_title_DataFrame_preserved=True,
    saved_answer_original_sink_and_evaluator_equal=True, quality=quality, result=mmqa_semantics.frame_value(result),
    reverse_output_association=True, missing_duplicate_rejected=True, real_model_calls=0))
write_private_json(ROOT/'cpu-requests-PRIVATE.json',dict(schema='semloom.native_daft_prompts.v1',requests=requests))
write_private_json(ROOT/'expected-prompt-token-ids-PRIVATE.json',dict(ids=ids))
write_private_json(ROOT/'recorded-outputs-PRIVATE.json',dict(outputs=[r['text'] for r in prior]))
write_private_json(ROOT/'CPU-REFERENCE.json',dict(result=mmqa_semantics.frame_value(result),quality=quality))
from src.infrastructure.private_artifacts import file_sha256
worker={k:config[k] for k in ('model','sampling_params','engine_kwargs','provider_options','query_timeout_s','cleanup_s','stop_grace_s')}
worker.update(schema='semloom.native_daft_map.v1',requests_path=str(ROOT/'cpu-requests-PRIVATE.json'),requests_sha256=file_sha256(ROOT/'cpu-requests-PRIVATE.json'))
write_private_json(ROOT/'cpu-worker-config-PRIVATE.json',worker)
print((ROOT/'CPU-DRIVER.json').read_text())
