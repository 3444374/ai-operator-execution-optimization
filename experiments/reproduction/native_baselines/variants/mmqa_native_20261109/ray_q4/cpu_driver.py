"""Replay the 24 saved original answers; no model or URL/image fetch."""
import json
from pathlib import Path

from src.infrastructure.private_artifacts import content_digest, json_metric, write_private_json
from prepare import prepare_case
import mmqa_semantics

ROOT = Path(__file__).resolve().parent
REFERENCE = Path(json.loads((ROOT/'CONFIG.json').read_text())['reference_query_json'])
query, method, prepared, requests = prepare_case(ROOT)
reference = json.loads(REFERENCE.read_text())
prior = reference['operator']['calls']['records']
assert len(prepared) == len(prior) == 24
assert query.operation.stage.kind == 'extract'
assert query.operation.stage.method.output_columns == {'genres':'The genres of the movie, separated by commas'}
assert query.operation.describe()['model_columns'] == ['text']
from transformers import AutoTokenizer
tokenizer = AutoTokenizer.from_pretrained(json.loads((ROOT/'CONFIG.json').read_text())['model'], local_files_only=True)
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
assert list(result.columns) == ['genre','movies_in_genre']
for bad in (predictions[:-1], predictions[:-1] + [predictions[0]]):
    try:
        mmqa_semantics.finish(query, method, prepared, bad)
    except ValueError:
        pass
    else:
        raise AssertionError('Missing/duplicate original occurrence accepted')
write_private_json(ROOT/'CPU-TOKEN-REFERENCES.json', dict(ids=ids, prompts=[r['prompt'] for r in requests]))
write_private_json(ROOT/'CPU-DRIVER.json', dict(status='passed', source_rows=25, messages=24,
    original_messages_and_prompt_tokens_matched=24, source_order_original_indices_and_genre_DataFrame_preserved=True,
    saved_answer_original_sink_and_evaluator_equal=True, quality=quality, result=mmqa_semantics.frame_value(result),
    reverse_output_association=True, missing_duplicate_rejected=True, real_model_calls=0))
print((ROOT/'CPU-DRIVER.json').read_text())
