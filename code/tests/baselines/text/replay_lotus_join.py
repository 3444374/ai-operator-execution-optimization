"""Zero-model exact Join replay over original LOTUS/LM/LiteLLM and real IPC.

All answers are explicitly synthetic, keyed by complete original messages.
Neither the saved fixture quality nor these CPU timings are model results.
"""
import argparse
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path
import socket
import sys
from unittest.mock import patch

from src.infrastructure.private_artifacts import (
    content_digest, file_sha256, json_metric, new_private_directory, write_private_json,
)
from src.experiments.attempt_ledger import AttemptBudget, BudgetExhausted
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.shared_request_budget import SharedClaimedUnit
from src.serving.backends.vllm_protocol import EngineSpec
from .replay_lotus_resident import RecordedProcess


def original_fixture(query, tokenizer):
    import lotus
    import pandas as pd
    from lotus.types import LMOutput
    from src.baselines.text.frameworks.lotus_native_join import make_lm
    from src.baselines.text.sembench_movie import run_original_lotus_query
    from src.baselines.text.sembench_prepared import result_value

    messages, texts, relations = [], [], []
    with lotus.settings.context(enable_cache=False):
        lm = make_lm(query.operation, 1)
    join_type = type(pd.DataFrame().sem_join)
    original_join = join_type.__call__

    def answer(current, prompts, **kwargs):
        assert current is lm and not messages
        messages.extend(deepcopy(prompts))
        # Same conversations deliberately share the same synthetic answer; all
        # occurrences still consume their own native provider call and budget.
        texts.extend('True' if int(content_digest(prompt)[-1], 16) % 2 else 'False' for prompt in prompts)
        return LMOutput(outputs=texts)

    def joined(accessor, *args, **kwargs):
        frame = original_join(accessor, *args, **kwargs)
        relations.append(frame.copy(deep=True))
        return frame

    with lotus.settings.context(lm=lm, enable_cache=False), \
            patch.object(type(lm), '__call__', answer), patch.object(join_type, '__call__', joined):
        result = run_original_lotus_query(query.checkout, int(query.query), lambda: query._load('Reviews.csv'))
    assert len(messages) == len(query.operation.records) == 64 and len(relations) == 1
    pairs = []
    for record, message, text in zip(query.operation.records, messages, texts, strict=True):
        assert all(isinstance(value['content'], str) for value in message)
        ids = tokenizer.apply_chat_template(message, tokenize=True, add_generation_prompt=True)
        left, right = divmod(record.source_position, len(query.operation.frame))
        pairs.append(dict(left=left, right=right, source_position=record.source_position,
            source_index=record.source_index, messages=message, messages_sha256=content_digest(message),
            expected_token_ids=ids, normalized_conversation=message, text=text))
    return dict(schema='semloom.lotus_join_software_pairs.v1', pairs=pairs,
        result=result_value(result), relation=result_value(relations[0]),
        quality=json_metric(query.evaluate(result)), quality_claim=False,
        semantics='synthetic complete-message-hash parity Boolean; never a sentiment gold/model reference')


def replay(config_path, checkout, data_dir, output):
    from transformers import AutoTokenizer
    from src.experiments.sembench_text_query import configuration, query_from_config
    from src.experiments.sembench_text_resident import verify_inputs, run
    from src.baselines.text.frameworks import lotus_native_local as native

    config, engine, generation, resources = configuration(config_path)
    assert config['resident']['repetitions'] == 1
    query = query_from_config(config, checkout, data_dir, generation)
    assert query.scenario == 'movie' and query.query in ('5', '6', '7')
    assert len(query.operation.frame) == 8 and query.describe()['expected_model_calls'] == 64
    verify_inputs(config, query)
    tokenizer = AutoTokenizer.from_pretrained(engine.tokenizer_path or engine.model_path, local_files_only=True)
    frozen = original_fixture(query, tokenizer)
    output = Path(output)
    new_private_directory(output)
    write_private_json(output / 'pairs.json', frozen)
    responses = {}
    for pair in frozen['pairs']:
        value = dict(text=pair['text'], token_ids=pair['expected_token_ids'],
                     source_position=pair['source_position'], source_index=pair['source_index'])
        responses.setdefault(pair['messages_sha256'], value)
        assert responses[pair['messages_sha256']]['text'] == pair['text']
    fixture = output / 'responses.json'
    write_private_json(fixture, responses)
    value = config['budget']
    ledger = CellBudgetLedger.create(Path(value['path']), AttemptBudget(value['id'], value['limit']), deadline_utc=None)
    ledger.reserve_unit(value['unit_ids'][0], 64)
    cpu_engine = replace(engine, model_path=str(fixture), model_python=sys.executable,
        driver_python=sys.executable, max_model_len=32768)
    connect = socket.socket.connect

    def no_network(sock, address):
        if sock.family in (socket.AF_INET, socket.AF_INET6):
            raise RuntimeError('Join CPU fixture forbids network/model endpoints')
        return connect(sock, address)

    with patch.object(socket.socket, 'connect', no_network), \
            patch.object(native, 'AsyncEngineProcess', RecordedProcess), \
            patch.object(EngineSpec, 'validate_driver_environment', return_value={'CPU_fixture': True}):
        session = run(config, checkout, data_dir, cpu_engine, generation, resources, output / 'session')
    assert session['status'] == 'completed' and session['owner_start_count'] == 1
    assert session['owners'][0]['close']['status'] == 'closed'
    round_ = session['queries'][0]
    actual = round_['backend_report']
    assert actual['result'] == frozen['result'] and actual['quality'] == frozen['quality']
    assert actual['operator_relation'] == frozen['relation']
    calls = sorted(actual['operator']['calls']['records'], key=lambda item: item['source_position'])
    assert len(calls) == len({call['call_id'] for call in calls}) == 64
    assert len({call['engine_request_id'] for call in calls}) == 64
    for pair, call in zip(frozen['pairs'], calls, strict=True):
        assert call['source_position'] == pair['source_position']
        assert call['row_id'].endswith(f"-pair-{pair['left']}-{pair['right']}")
        assert json.loads(call['fixture_messages']) == pair['messages']
        assert call['text'] == pair['text']
        assert json.loads(call['prompt_token_ids_json']) == pair['expected_token_ids']
        assert call['fixture_generation'] == generation.kwargs()
    times = [actual[key] for key in ('source_start_ns', 'prepare_finished_ns', 'model_ready_observed_ns',
        'sink_ready_ns', 'snapshot_finished_ns', 'audit_finished_ns', 'evaluation_started_ns', 'evaluation_finished_ns')]
    assert times == sorted(times)
    assert times[-1] <= round_['persistence_started_ns'] <= round_['persistence_finished_ns']
    unit = SharedClaimedUnit(Path(value['path']), AttemptBudget(value['id'], value['limit']), value['unit_ids'][0])
    assert unit.attempts == 64 and unit.remaining == 0
    try:
        unit.reserve('f' * 64)
    except BudgetExhausted:
        pass
    else:
        raise AssertionError('65th request accepted')
    events = [json.loads(line) for line in Path(str(fixture) + '.events').read_text().splitlines()]
    assert sum(e['event'] == 'construct' for e in events) == sum(e['event'] == 'close' for e in events) == 1
    assert sum(e['event'] == 'start' for e in events) == sum(e['event'] == 'end' for e in events) == 64
    source = Path(__file__).resolve().parents[4]
    receipt = dict(status='passed_software_only', query=query.describe(), model_calls=0,
        source_occurrences=8, ordered_pairs=64, unique_messages=len(responses), software_IPC_requests=64,
        complete_messages_and_token_arrays_equal=True, original_full_join_sink_evaluator_equal=True,
        attempt_65_rejected=True, sink_before_snapshot_audit_evaluation_persistence_close=True,
        fixture_constructs=1, fixture_closes=1, quality_claim=False,
        code_sha256={str(p.relative_to(source)): file_sha256(p) for p in [
            source / 'code/src/baselines/text/sembench_text.py',
            source / 'code/src/baselines/text/frameworks/lotus_native_join.py',
            source / 'code/src/experiments/sembench_text_query.py',
            source / 'code/src/experiments/sembench_text_resident.py', Path(__file__)]},
        limitations=['synthetic responses and software engine: no model quality, performance or GPU qualification',
            'target native renderer/generate and physical host cleanup remain unverified',
            'direct existing resident lifecycle replay; outer host supervisor not run',
            'full256 Cartesian input memory/deadline and 65536 budget are not released'])
    write_private_json(output / 'CPU.json', receipt)
    return receipt


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('config', 'sembench-checkout', 'data-dir', 'output-dir'):
        parser.add_argument('--' + name, required=True)
    args = parser.parse_args()
    print(json.dumps(replay(args.config, args.sembench_checkout, args.data_dir, args.output_dir)))
