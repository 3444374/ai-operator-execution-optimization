"""Replay original Movie saved replies through the maintained Filter binding.

No model or author DSO runs. Arrow, Unix IPC, exact occurrence budgeting, full
original relation, parser, sink, evaluator and ACK timing seams are real CPU paths.
"""

import argparse
import asyncio
import json
from pathlib import Path

from src.baselines.text.sembench_movie_native import q3_decision
from src.infrastructure.private_artifacts import content_digest, file_identity, new_private_directory, read_json, write_private_json
from tests.baselines.text.replay_imlane_medical_filter import replay_saved_filter


def reference(root, query_id):
    root = Path(root)
    if query_id == '1':
        rows = list(read_json(root / 'input/messages.json')['rows'].values())
        rows.sort(key=lambda row: row['source_position'])
        audit = read_json(root / 'audit.json')
        predictions = audit['predictions']
        messages = [row['messages'] for row in rows]
        native = read_json(root / 'query.json')
        # Native SQL returns VARCHAR IDs; original source/sink retains integers.
        frame = dict(index=list(range(len(native['reviewIds']))), columns=['reviewId'],
            data=[[int(value)] for value in native['reviewIds']])
        names = ('input/messages.json', 'audit.json', 'query.json', 'result-ready.json')
    else:
        prepared = read_json(root / 'input/prepared.json')
        rows = prepared['rows']
        messages = [json.loads(call['messages_json']) for call in prepared['calls']]
        saved = read_json(root / 'result-ready.json')['result']
        predictions, frame = saved['predictions'], saved['frame']
        audit = read_json(root / 'AUDIT.json')
        names = ('input/prepared.json', 'result-ready.json', 'AUDIT.json')
    by_id = {row['occurrence_id']: row for row in predictions}
    assert len(by_id) == len(predictions) == len(rows) == (2000 if query_id == '1' else 120)
    texts = []
    for row, messages_row in zip(rows, messages, strict=True):
        occurrence = 'movie-row-' + str(row['source_position'])
        prediction = by_id[occurrence]
        assert prediction['messages_sha256'] == content_digest(messages_row)
        assert prediction['decision'] == q3_decision(prediction['generated_text'])
        assert str(prediction['reviewId']) == str(row['reviewId'])
        assert prediction['id'] == row['id']
        texts.append(prediction['generated_text'])
    quality = {key: value for key, value in audit['quality'].items() if key != 'classification_audit'}
    return dict(messages=messages, texts=texts, result=frame, quality=quality,
        source_columns=['reviewId', 'reviewText'],
        labels_and_texts=[(str(row['reviewId']), row['reviewText']) for row in rows],
        evidence={name: file_identity(root / name) for name in names})


def bridge_q3_reference(path, config):
    # This is the historical 168bc LOTUS-compatible project resident bridge,
    # not an original-author native SQL qualification or new model execution.
    from src.experiments.native_imlane_filters import prepare_query
    query, _method, _prepared, requests, tokens, rows = prepare_query(config)
    report = read_json(path)['backend_report']
    calls = sorted(report['calls']['records'], key=lambda row: row['source_position'])
    decisions = {row['row_id']: row for row in report['decisions']}
    assert len(calls) == len(decisions) == len(rows) == 120
    assert len({row['call_id'] for row in calls}) == 120
    for row, call, request, token_ids in zip(rows, calls, requests, tokens, strict=True):
        position = int(query.operation.frame.index[row['source_position']])
        assert call['source_position'] == position
        assert call['row_id'] == 'movie-row-' + str(position)
        assert call['status'] == 'succeeded' and call['call_index'] == 0
        assert call['messages_sha256'] == content_digest(request['messages'])
        assert json.loads(call['prompt_token_ids_json']) == token_ids
        assert decisions[call['row_id']]['decision'] == q3_decision(call['text'])
    quality = {key: value for key, value in report['quality'].items() if key != 'classification_audit'}
    return dict(messages=[row['messages'] for row in requests], texts=[call['text'] for call in calls],
        source_columns=['reviewId', 'reviewText'],
        labels_and_texts=[(str(row.reviewId), str(row.reviewText)) for row in query.operation.frame.itertuples()],
        quality=quality,
        result=dict(index=[0], columns=['positive_review_cnt'], data=[[report['result']['positive_review_cnt']]]),
        evidence=dict(source_identity='168bc LOTUS-compatible IMLane project resident bridge, not native author defaults',
            saved_query=file_identity(path), complete_saved_prompt_token_arrays_equal=True))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('config', 'original-run', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args(argv)
    config = read_json(args.config)
    output = args.output.resolve(); new_private_directory(output)
    report = dict(status='failed', real_model_calls=0)
    try:
        frozen = (bridge_q3_reference(args.original_run, config) if config['query_id'] == '3'
            else reference(args.original_run, config['query_id']))
        report.update(asyncio.run(replay_saved_filter(config, frozen, 'historical-saved-replies', output)))
    except BaseException as error:
        report.update(error_type=type(error).__name__, error=str(error)); raise
    finally:
        write_private_json(output / 'CPU.json', report)
    print(f"Movie Q{config['query_id']}: passed {report['saved_answers']} saved answers; real models 0", flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
