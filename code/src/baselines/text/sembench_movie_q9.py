"""Original Q9 Map source, LOTUS formatter, and complete SemBench sink.

The selected source determines occurrence and business-ID counts. Historical
256-row inputs are frozen experiment assets, not limits of this task binding.
"""
from dataclasses import replace
import csv
import json
from pathlib import Path

from src.infrastructure.private_artifacts import content_digest, file_sha256, json_metric, write_private_json


def prepare(config, directory, query_id):
    from src.baselines.text.sembench_text import SemBenchTextQuery
    from src.semantic_methods.lotus.record_method import LotusRecordMethod
    from src.serving.backends.vllm_protocol import GenerationSpec

    directory = Path(directory)
    directory.mkdir(mode=0o700)
    sampling = dict(config['sampling_params'])
    assert sampling.pop('n') == 1
    query = SemBenchTextQuery(config['sembench_checkout'], config['data_dir'], 'movie', '9',
        execution_id=query_id, model_name=config['model_name'], generation=GenerationSpec.from_dict(sampling))
    if query.operation is None or not query.operation.records:
        raise ValueError('native Q9 Map requires a nonempty original selection')
    captured = []

    def capture(operation, original):
        assert operation.stage.kind == 'map' and not captured
        captured.append(operation)
        dummy = [replace(row, values={**row.values, operation.stage.method.output_column: '3'})
                 for row in operation.records]
        return dict(frame=operation.result_frame(dummy), calls=[])

    query.execute(capture)  # Source/selection only; the placeholder frame is discarded, never evaluated.
    operation, = captured
    method = LotusRecordMethod(operation.stage)
    calls = method.prepare_rows(operation.records, query_id=query_id)
    rows = []
    messages = {}
    for index, (record, call) in enumerate(zip(operation.records, calls, strict=True)):
        values = operation.frame.iloc[index]
        row = dict(occurrence_id=call['call_id'], id=str(values['id']),
                   reviewId=str(values['reviewId']), reviewText=str(values['reviewText']),
                   source_position=int(record.source_position))
        rows.append(row)
        messages[row['occurrence_id']] = dict(row, messages=json.loads(call['messages_json']))
    assert len(rows) == len(messages) == len(operation.records)
    assert all(r['id'] == 'ant_man_and_the_wasp_quantumania' for r in rows)
    assert [r['source_position'] for r in rows] == list(range(len(rows)))
    with (directory / 'input.csv').open('w', newline='') as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    write_private_json(directory / 'messages.json', dict(rows=messages))
    write_private_json(directory / 'prepared.json', dict(query_id=query_id, rows=rows, calls=calls,
        csv_sha256=file_sha256(Path(config['data_dir']) / 'Reviews.csv'),
        messages_sha256=[content_digest(messages[r['occurrence_id']]['messages']) for r in rows],
        gold_fields_in_model_messages=False, placeholder_frame_discarded=True))
    return query, operation, calls, rows


def finish(query, operation, calls, rows, native, udf_directory):
    from lotus.sem_ops.postprocessors import map_postprocess
    from src.semantic_methods.lotus.record_method import LotusRecordMethod

    predictions = {}
    for path in sorted(Path(udf_directory).glob('q9-batch-*.json')):
        receipt = json.loads(path.read_text())
        assert receipt['status'] == 'succeeded'
        for item in receipt['predictions']:
            assert item['occurrence_id'] not in predictions
            predictions[item['occurrence_id']] = item
    mapped = native['mapped_occurrences']
    by_id = {row['occurrence_id']: row for row in mapped}
    expected = {row['occurrence_id'] for row in rows}
    assert len(mapped) == len(by_id) == len(expected) == len(rows) and by_id.keys() == predictions.keys() == expected
    texts = []
    for source in rows:
        item, value = predictions[source['occurrence_id']], by_id[source['occurrence_id']]
        assert all(value[key] == source[key] for key in ('id', 'reviewId', 'source_position'))
        assert item['score'] == value['score']
        assert item['messages_sha256'] == content_digest(json.loads(calls[source['source_position']]['messages_json']))
        texts.append(item['generated_text'])
    method = LotusRecordMethod(operation.stage)
    parsed = map_postprocess(texts, method.lm, False).outputs
    assert parsed == texts  # Installed original LOTUS non-CoT Map is identity.
    completed = [replace(row, values={**row.values, operation.stage.method.output_column: text})
                 for row, text in zip(operation.records, parsed, strict=True)]

    def original_sink(current, original):
        assert [r.source_index for r in current.records] == [r.source_index for r in operation.records]
        return dict(frame=current.result_frame(completed), calls=[])

    frame, _ = query.execute(original_sink)
    assert [float(x) for x in frame['reviewScore']] == [by_id[row['occurrence_id']]['score'] for row in rows]
    assert [str(x) for x in frame['reviewId']] == [row['reviewId'] for row in rows]
    return frame, dict(frame=json_metric(frame.to_dict(orient='split')), calls=[
        dict(call, text=text, status='succeeded', score=by_id[row['occurrence_id']]['score'])
        for row, call, text in zip(rows, calls, texts, strict=True)],
        unique_occurrences=len(expected), unique_reviewIds=len({row['reviewId'] for row in rows}),
        native_projection='DOUBLE score; raw model texts retained in matching UDF/IPC receipts',
        original_lotus_map_postprocess_identity=True, original_numeric_sink_matches_projection=True)
