"""Zero-model Q9 TopK qualification through the formal resident entry.

Responses are explicit Document-1 software fixtures, never Map outputs or quality
evidence. The optional renderer check runs only tokenizer/config APIs on target.
"""
import argparse
from copy import deepcopy
from dataclasses import replace
import importlib
import json
from pathlib import Path
import socket
import sys
from unittest.mock import patch

from src.infrastructure.private_artifacts import (
    content_digest, file_sha256, new_private_directory, read_json, write_private_json,
)
from src.experiments.attempt_ledger import AttemptBudget, BudgetExhausted
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.shared_request_budget import SharedClaimedUnit
from src.serving.backends.vllm_protocol import EngineSpec
from .replay_lotus_resident import RecordedProcess


def pair_fixture(query, tokenizer):
    import lotus
    from lotus.templates import task_instructions
    from src.baselines.text.frameworks.lotus_native_topk import make_lm

    op = query.operation
    values = []
    fixture = dict(schema='semloom.lotus_topk_software_pairs.v1', pairs=values,
        requested_output_tokens=0 if op is None else op.generation.max_tokens,
        semantics='constant Document 1 drives worst-case partitions; no sentiment/quality claim',
        tokenizer_scope='source-derived text normalization; target renderer equivalence pending',
        renderer_source='https://github.com/vllm-project/vllm/blob/v0.25.1/vllm/entrypoints/chat_utils.py')
    if op is None:
        return fixture  # The original empty query emits no semantic operator.
    docs = task_instructions.df2multimodal_info(op.frame, list(op.columns))
    instruction = lotus.nl_expression.nle2str(op.instruction, list(op.columns))
    module = importlib.import_module('lotus.sem_ops.sem_topk')
    lm = make_lm(op, 1)
    for left in range(len(docs)):
        for right in range(len(docs)):
            if left == right:
                continue
            messages = module.get_match_prompt_binary(docs[left], docs[right], instruction, model=lm)
            # Source-derived expectation only: vLLM 0.25.1 chat_utils string
            # content joins text parts with a newline. Actual renderer is a
            # separate target CPU gate; production messages remain untouched.
            conversation = deepcopy(messages)
            for message in conversation:
                if isinstance(message['content'], list):
                    assert all(set(p) == {'type','text'} and p['type'] == 'text' for p in message['content'])
                    message['content'] = '\n'.join(p['text'] for p in message['content'] if p['text'])
            tokens = tokenizer.apply_chat_template(conversation, tokenize=True, add_generation_prompt=True)
            values.append(dict(left=left, right=right, messages=messages,
                messages_sha256=content_digest(messages), normalized_conversation=conversation,
                expected_token_ids=tokens, text='Document 1'))
    return fixture


def renderer_check(fixture, model, output):
    """Use the real native renderer with no LLM/engine/weight load."""
    from vllm.config import DeviceConfig, ModelConfig, VllmConfig
    from vllm.renderers import ChatParams, renderer_from_config
    import torch
    import vllm

    assert vllm.__version__ == '0.25.1'
    assert not torch.cuda.is_initialized()
    model_config = ModelConfig(model=model, tokenizer=model, trust_remote_code=False, max_model_len=32768)
    config = VllmConfig(model_config=model_config, device_config=DeviceConfig(device='cpu'))
    renderer = renderer_from_config(config)
    params = ChatParams(chat_template=None, chat_template_content_format='auto',
        chat_template_kwargs=dict(add_generation_prompt=True, continue_final_message=False,
            tools=None, tokenize=False))
    fixture = Path(fixture)
    frozen = read_json(fixture, max_bytes=fixture.stat().st_size)
    try:
        conversations, prompts = renderer.render_chat([p['messages'] for p in frozen['pairs']], params)
        for pair, conversation, prompt in zip(frozen['pairs'], conversations, prompts, strict=True):
            assert conversation == pair['normalized_conversation']
            assert prompt['prompt_token_ids'] == pair['expected_token_ids']
            assert len(prompt['prompt_token_ids']) + frozen['requested_output_tokens'] <= model_config.max_model_len
        assert not torch.cuda.is_initialized()
        write_private_json(Path(output), dict(status='passed', model_calls=0, native_renderer='vllm '+vllm.__version__,
            pairs=len(prompts), complete_messages_and_token_arrays_equal=True,
            context_limit=model_config.max_model_len, requested_output_tokens=frozen['requested_output_tokens'],
            all_pairs_fit_context=True,
            prompt_tokens=[len(p['prompt_token_ids']) for p in prompts],
            CUDA_initialized=False, scope='CPU renderer/config/tokenizer only; no engine/weights/physical lifecycle'))
    finally:
        renderer._executor.shutdown(wait=True)


def replay(config_path, checkout, data_dir, output):
    from transformers import AutoTokenizer
    from src.experiments.sembench_text_query import configuration, query_from_config
    from src.experiments.sembench_text_resident import verify_inputs, run
    from src.baselines.text.frameworks import lotus_native_local as native

    config, engine, generation, resources = configuration(config_path)
    assert config['ranking'] == 'topk' and config['resident']['repetitions'] == 1
    query = query_from_config(config, checkout, data_dir, generation)
    verify_inputs(config, query)
    count = 0 if query.operation is None else len(query.operation.records)
    bound = query.describe()['expected_model_calls']
    allowance = max(1, bound)  # The existing permanent ledger requires a positive reservation.
    assert config['budget']['limit'] == engine.max_model_calls == allowance
    tokenizer = AutoTokenizer.from_pretrained(engine.tokenizer_path or engine.model_path, local_files_only=True)
    frozen = pair_fixture(query, tokenizer)
    output = Path(output)
    new_private_directory(output)
    write_private_json(output/'pairs.json', frozen)
    responses, source_pairs = {}, {}
    for pair in frozen['pairs']:
        source_pairs[pair['left'], pair['right']] = pair
        # Equal messages deliberately share the constant software response. Pair
        # identity comes from the native document occurrences, never this digest.
        responses.setdefault(pair['messages_sha256'], dict(text=pair['text'],
            token_ids=pair['expected_token_ids'], source_position=pair['left'], source_index=pair['right']))
    fixture = output/'responses.json'
    write_private_json(fixture, responses)
    value = config['budget']
    ledger = CellBudgetLedger.create(Path(value['path']), AttemptBudget(value['id'], value['limit']), deadline_utc=None)
    ledger.reserve_unit(value['unit_ids'][0], allowance)
    cpu_engine = replace(engine, model_path=str(fixture), model_python=sys.executable,
        driver_python=sys.executable, max_model_len=32768)
    connect = socket.socket.connect

    def no_network(sock, address):
        if sock.family in (socket.AF_INET, socket.AF_INET6):
            raise RuntimeError('TopK CPU fixture forbids network/model endpoints')
        return connect(sock, address)

    with patch.object(socket.socket, 'connect', no_network), \
            patch.object(native, 'AsyncEngineProcess', RecordedProcess), \
            patch.object(EngineSpec, 'validate_driver_environment', return_value={'CPU_fixture':True}):
        session = run(config, checkout, data_dir, cpu_engine, generation, resources, output/'session')
    assert session['status'] == 'completed' and session['owner_start_count'] == 1
    assert session['owners'][0]['close']['status'] == 'closed'
    round_ = session['queries'][0]
    actual = round_['backend_report']
    calls, pairs = actual['operator'].get('calls', []), actual['operator'].get('pairs', [])
    by_id = {call['call_id']: call for call in calls}
    assert len(by_id) == len(pairs) == bound
    assert len({tuple(sorted((p['left'], p['right']))) for p in pairs}) == bound
    for pair in pairs:
        call = by_id[pair['call_id']]
        expected = source_pairs[pair['left'], pair['right']]
        assert pair['messages'] == expected['messages']
        assert json.loads(call['fixture_messages']) == pair['messages']
        saved = responses[content_digest(pair['messages'])]
        assert call['text'] == 'Document 1'
        assert json.loads(call['prompt_token_ids_json']) == saved['token_ids']
        assert call['fixture_generation'] == generation.kwargs()
    times = [actual[k] for k in ('source_start_ns','prepare_finished_ns',
        'sink_ready_ns','snapshot_finished_ns','audit_finished_ns','evaluation_started_ns','evaluation_finished_ns')]
    if bound:
        times.insert(2, actual['model_ready_observed_ns'])
    assert times == sorted(times)
    assert times[-1] <= round_['persistence_started_ns'] <= round_['persistence_finished_ns']
    unit = SharedClaimedUnit(Path(value['path']), AttemptBudget(value['id'], value['limit']), value['unit_ids'][0])
    assert unit.attempts == bound and unit.remaining == allowance - bound
    try:
        unit.reserve('f'*64)
    except BudgetExhausted:
        pass
    else:
        raise AssertionError('closed query accepted another request')
    event_file = Path(str(fixture)+'.events')
    events = [json.loads(line) for line in event_file.read_text().splitlines()] if bound else []
    constructs = int(bound > 0)
    assert sum(e['event']=='construct' for e in events) == sum(e['event']=='close' for e in events) == constructs
    assert sum(e['event']=='start' for e in events) == sum(e['event']=='end' for e in events) == bound
    source = Path(__file__).resolve().parents[4]
    receipt = dict(status='passed_software_only', model_calls=0, source_occurrences=count,
        software_IPC_requests=bound, worst_case_bound_exercised=True, closed_unit_extra_attempt_rejected=True,
        comparison_upper_bound=bound, directed_renderer_pairs=len(frozen['pairs']),
        complete_native_relation_sink_and_original_evaluator_replayed=True,
        native_source_pair_occurrences_complete=True, messages_and_fixture_tokens_equal=True,
        sink_before_snapshot_audit_evaluation_persistence_close=True,
        model_fixture_constructs=constructs, model_fixture_closes=constructs, quality_claim=False,
        code_sha256={str(p.relative_to(source)):file_sha256(p) for p in [
            source/'code/src/baselines/text/sembench_text.py',
            source/'code/src/baselines/text/frameworks/lotus_native_topk.py',
            source/'code/src/experiments/sembench_text_query.py',
            source/'code/src/experiments/sembench_text_resident.py', Path(__file__)]},
        limitations=['constant software responses; not actual model quality or performance',
            'full token expectation uses source-derived text normalization; native target renderer pending',
            'target SDK, engine acceptance/generate count and physical GPU/host cleanup unverified',
            'direct CPU lifecycle run; host supervisor not executed'])
    write_private_json(output/'CPU.json', receipt)
    return receipt


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action', required=True)
    cpu = sub.add_parser('replay')
    for name in ('config','sembench-checkout','data-dir','output-dir'):
        cpu.add_argument('--'+name, required=True)
    target = sub.add_parser('renderer')
    for name in ('fixture','model','output'):
        target.add_argument('--'+name, required=True)
    args = parser.parse_args()
    if args.action == 'renderer':
        renderer_check(args.fixture, args.model, args.output)
    else:
        print(json.dumps(replay(args.config, args.sembench_checkout, args.data_dir, args.output_dir)))
