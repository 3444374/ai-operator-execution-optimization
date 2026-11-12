"""Original full-query semantics through the Ray API software fixture, no model.

Saved replies are keyed by occurrence, including identical messages with distinct
answers. This does not exercise the target Ray SDK or native tokenization.
"""

import argparse
from collections import Counter
import json
from pathlib import Path
import socket
from unittest.mock import patch

from src.baselines.text.sembench_prepared import prepare
from src.experiments.sembench_text_query import configuration, query_from_config, check, run_worker
from src.experiments.attempt_ledger import AttemptBudget, BudgetExhausted
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.shared_request_budget import SharedClaimedUnit
from src.infrastructure.private_artifacts import new_private_directory, read_json, write_private_json, content_digest
from tests.baselines.text.ray_native_fixtures import recorded_processor


def replay(config_path, checkout, data_dir, tokenizer_path, reference, output):
    from transformers import AutoTokenizer

    output = Path(output)
    new_private_directory(output)
    config, engine, generation, resources = configuration(config_path)
    query = query_from_config(config, checkout, data_dir, generation)
    preflight = check(config, query, engine)
    write_private_json(output/'preflight.json', preflight)
    _, calls, requests = prepare(query)
    messages = [row['messages'] for row in requests]
    assert messages == [json.loads(row['messages_json']) for row in reference['calls']]
    assert [row['source_position'] for row in calls] == [row['source_position'] for row in reference['calls']]
    tokenizer = AutoTokenizer.from_pretrained(tokenizer_path, local_files_only=True, trust_remote_code=False)
    tokens = [tokenizer.apply_chat_template(message, tokenize=True, add_generation_prompt=True) for message in messages]
    assert tokens == reference['token_ids']
    by_id = {row['call_id']: text for row, text in zip(requests, reference['texts'], strict=True)}
    assert len(by_id) == len(requests)
    budget = dict(path=str(output/'budget.sqlite'), id=query.query_id, limit=len(requests), unit_id='query')
    config['budget'] = budget
    ledger = CellBudgetLedger.create(Path(budget['path']), AttemptBudget(budget['id'], budget['limit']), deadline_utc=None)
    ledger.reserve_unit('query', len(requests))
    with recorded_processor(by_id) as sdk:
        report = run_worker(config, query, engine, resources, output)
    assert [row['messages'] for row in sdk.requests] == messages
    assert all(row['sampling_params'] == query.operation.stage.generation.kwargs() for row in sdk.requests)
    assert report['result'] == reference['result']
    assert report['quality'] == reference['quality']
    semantic = report['semantic']
    assert semantic['operator_relation'] == reference['relation']
    assert [row['text'] for row in semantic['calls']] == reference['texts']
    assert [row['source_position'] for row in semantic['calls']] == [row['source_position'] for row in calls]
    assert report['sink_ready_ns'] <= report['semantic_audit_started_ns'] <= report['semantic_audit_finished_ns']
    assert report['sink_ready_ns'] < report['native']['events']['close_started_ns']
    assert report['sink_ready_ns'] < report['evaluation_started_ns'] < report['native_persistence_started_ns']
    assert not sdk.initialized
    unit = SharedClaimedUnit(Path(budget['path']), AttemptBudget(budget['id'], budget['limit']), 'query')
    assert unit.attempts == len(requests) and unit.remaining == 0
    try:
        unit.reserve('f'*64)
    except BudgetExhausted:
        pass
    else:
        raise AssertionError('extra request accepted after complete query')
    hashes = Counter(content_digest(message) for message in messages)
    receipt = dict(status='passed', model_calls=0, software_requests=len(requests),
        unique_messages=len(hashes), duplicate_message_occurrences=len(requests)-len(hashes),
        complete_source_order_messages_tokens_equal=True, saved_answer_occurrences_equal=True,
        original_operator_relation_sink_evaluator_equal=True, sink_before_audit_close_evaluation_persistence=True,
        physical_budget_software_attempts=unit.attempts, next_attempt_rejected=True,
        data=query.files, token_arrays_sha256=content_digest(tokens),
        limitations=['Ray Dataset/processor and AsyncLLM are software fixtures, not target SDK qualification',
            'token arrays use local frozen tokenizer, not Ray actor tokenization',
            'saved replies are historical fixtures, not new Ray/model quality or performance',
            'ray.shutdown double is not physical process/SHM/GPU cleanup proof'])
    write_private_json(output/'CPU.json', receipt)
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('config', 'sembench-checkout', 'data-dir', 'tokenizer', 'reference', 'output-dir'):
        parser.add_argument('--'+name, required=True, type=Path)
    args = parser.parse_args()
    with patch.object(socket.socket, 'connect', side_effect=RuntimeError('CPU replay forbids network/model access')):
        receipt = replay(args.config, args.sembench_checkout, args.data_dir, args.tokenizer,
            read_json(args.reference, max_bytes=32*1024**2), args.output_dir)
    print(json.dumps({key: value for key, value in receipt.items() if key != 'data'}))


if __name__ == '__main__':
    main()
