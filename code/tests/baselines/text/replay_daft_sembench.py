"""CPU-only full-query qualification using identity-bound saved model replies.

Uses the installed Daft SDK/executor, native LOTUS accessor as the reference,
and the maintained worker/driver. Model and supervisor process are software
fixtures here; physical supervision is tested separately.
"""

import argparse
from collections import Counter
from importlib.metadata import version
import json
from pathlib import Path
import socket
import time
from unittest.mock import patch

from src.infrastructure.private_artifacts import (
    content_digest, read_json, write_private_json, publish_private_json,
)


def saved_reply_bindings(calls, frozen):
    """Bind each occurrence; message lookup is only for the native reference.

    LOTUS's message-only fixture cannot distinguish equal messages with
    different saved answers. Reject that ambiguity instead of assigning by
    arrival order. Daft always receives the separate call-ID bindings.
    """
    by_call, by_message = {}, {}
    for call, saved, text in zip(calls, frozen['calls'], frozen['texts'], strict=True):
        message = json.loads(call['messages_json'])
        if (call['source_position'] != saved['source_position']
                or message != json.loads(saved['messages_json'])):
            raise ValueError('saved reply source occurrence or message differs')
        call_id = call['call_id']
        if call_id in by_call:
            raise ValueError('duplicate replay call ID')
        by_call[call_id] = text
        digest = content_digest(message)
        if digest in by_message and by_message[digest] != text:
            raise ValueError('equal messages have different saved replies; native reference needs occurrence identity')
        by_message[digest] = text
    return by_call, by_message


def replay(config, frozen, output):
    import lotus
    from litellm.types.utils import ModelResponse
    from transformers import AutoTokenizer
    from src.baselines.text.sembench_text import SemBenchTextQuery
    from src.baselines.text.sembench_prepared import prepare, result_value
    from src.experiments.native_daft_sembench import run_query
    from src.experiments.native_daft_query import run_worker, QueryDeadline
    from src.experiments.attempt_ledger import AttemptBudget, BudgetExhausted
    from src.experiments.shared_request_budget import SharedClaimedUnit
    from src.serving.backends.vllm_protocol import GenerationSpec
    from tests.baselines.text.native_sdk_fixtures import recorded_map_provider

    sampling = dict(config['sampling_params'])
    assert sampling.pop('n') == 1
    tokenizer = AutoTokenizer.from_pretrained(config['model'], local_files_only=True, trust_remote_code=False)
    query = SemBenchTextQuery(config['sembench_checkout'], config['data_dir'], config['scenario'],
        config['query'], execution_id=config['query_id'], model_name=config['model_name'],
        generation=GenerationSpec.from_dict(sampling), scale_factor=config.get('scale_factor'))
    method, calls, requests = prepare(query, tokenizer)
    lm = method if query.operation.describe()['kind'] == 'join' else method.lm
    messages = [json.loads(row['messages_json']) for row in calls]
    by_call, by_message = saved_reply_bindings(calls, frozen)
    tokens = [tokenizer.encode(row['prompt'], add_special_tokens=False) for row in requests]
    assert tokens == frozen['token_ids']
    assert [tokenizer.encode(row['prompt']) for row in requests] == tokens
    native_reference_messages = []

    def saved_native(data, options, *args):
        native_reference_messages.extend(content_digest(message) for message, _ in data)
        return [ModelResponse(model=lm.model, choices=[dict(index=0, finish_reason='stop',
            message=dict(role='assistant', content=by_message[content_digest(message)]))],
            usage=dict(prompt_tokens=0, completion_tokens=0, total_tokens=0)) for message, _ in data]
    lm._process_uncached_messages = saved_native
    relations = []
    def accessor(operation, invoke):
        relation = invoke()
        relations.append(result_value(relation))
        return dict(frame=relation)
    with lotus.settings.context(lm=lm, enable_cache=False):
        original, _ = query.execute(accessor)
    assert Counter(native_reference_messages) == Counter(content_digest(message) for message in messages)
    assert result_value(original) == frozen['result']
    if 'relation' in frozen:
        assert relations[0] == frozen['relation']

    fixture_receipts = []
    def supervised(config_path, directory, *, model_python, monitor, on_result_ready):
        worker = read_json(config_path)
        actual_requests = read_json(worker['requests_path'])['requests']
        assert [row['prompt'] for row in actual_requests] == [row['prompt'] for row in requests]
        assert [row['call_id'] for row in actual_requests] == [
            f'{query.query_id}-batch-0-row-{index}' for index in range(len(requests))]
        outputs = [by_call[row['call_id']] for row in calls]
        deadline = QueryDeadline(Path(directory) / 'result-ready.json',
            query_s=worker['query_timeout_s'], cleanup_s=worker['cleanup_s'],
            stop_grace_s=worker['stop_grace_s'], ready_schema='semloom.native_daft_map.result_ready.v1',
            on_result_ready=on_result_ready, wait_for_result_release=worker['wait_for_result_release'])
        def publish_then_observe(path, value):
            publish_private_json(path, value)
            deadline(())
        with recorded_map_provider(actual_requests, outputs) as sdk, \
                patch('src.experiments.native_daft_query.publish_private_json', publish_then_observe):
            terminal = run_worker(worker, directory)
            assert all(engine.closed for engine in sdk.engines)
        assert all(not executor.loop_thread.is_alive() and executor.loop.is_closed() for executor in sdk.executors)
        native = terminal['native_api']
        assert [native[key] for key in ('generate_calls', 'final_events', 'unresolved')] == [len(requests)] * 2 + [0]
        assert not native['errors']
        budget = worker['request_budget']
        unit = SharedClaimedUnit(Path(budget['path']), AttemptBudget(budget['budget_id'], budget['limit']), budget['unit_id'])
        assert unit.attempts == len(requests) and unit.remaining == 0
        try:
            unit.reserve('f' * 64)
        except BudgetExhausted:
            pass
        else:
            raise AssertionError('exhausted physical allowance accepted another request')
        fixture_receipts.append(dict(native=native, model_close=terminal['model_close_receipt'],
            budget_attempts=unit.attempts, next_attempt_rejected=True, executor_threads_closed=True))
        return dict(status='CPU fixture', scope='same-process observer; no OS process or GPU claim')

    report = run_query(config, output, model_python='CPU-fixture', tokenizer=tokenizer, supervise=supervised)
    actual = read_json(Path(output) / 'result-ready.json')
    assert actual['result'] == frozen['result']
    batch_path = Path(output) / 'native-lm-batches.json'
    batches = read_json(batch_path, max_bytes=batch_path.stat().st_size)
    assert len(batches) == 1
    assert [row['messages'] for row in batches[0]['requests']] == messages
    texts = {row['call_id']: row['generated_text'] for row in batches[0]['predictions']}
    assert [texts[row['call_id']] for row in batches[0]['requests']] == frozen['texts']
    assert [(row['call_id'], row['source_position']) for row in actual['semantic']['calls']] == [
        (f'{query.query_id}-batch-0-row-{index}', index) for index in range(len(calls))]
    assert report['quality'] == frozen['quality']
    assert report['sink_ready_ns'] < report['model_close_receipt']['close_started_monotonic_ns']
    assert (report['sink_ready_ns'] <= report['semantic_audit_started_ns']
            <= report['semantic_audit_finished_ns'] <= report['evaluation_started_ns'])
    receipt = dict(status='passed', model_calls=0, requests=len(requests),
        unique_call_ids=len(by_call), unique_messages=len(by_message),
        native_reference_occurrences=len(native_reference_messages),
        duplicate_message_replies_consistent=True,
        source_indices=[row.source_index for row in query.operation.records],
        original_accessor_relation_sink_evaluator_equal=True, full_messages_and_tokens_equal=True,
        source_identity_saved_replies_equal=True, native_null_and_filter_defaults_preserved=True,
        sink_before_close_and_audit=True, SDK_fixture=fixture_receipts,
        versions={name: version(name) for name in ('daft', 'lotus-ai', 'transformers', 'pandas')},
        limitations=['recorded model replies, not vLLM/GPU correctness or quality',
                    'supervisor process is a fixture; target model/physical cleanup still pending',
                    'one cold query; no same-owner warm qualification'])
    write_private_json(Path(output) / 'CPU.json', receipt)
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--reference', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    args = parser.parse_args()
    connect = socket.socket.connect
    def cpu_connect(sock, address):
        if sock.family in (socket.AF_INET, socket.AF_INET6):
            raise RuntimeError('CPU replay forbids model endpoints and network access')
        return connect(sock, address)
    with patch.object(socket.socket, 'connect', cpu_connect):
        # Q10's complete saved calls, token arrays and relation exceed 8 MiB.
        result = replay(read_json(args.config), read_json(args.reference, max_bytes=16 * 1024**2), args.output_dir)
    print(json.dumps({key: value for key, value in result.items() if key != 'SDK_fixture'}))


if __name__ == '__main__':
    main()
