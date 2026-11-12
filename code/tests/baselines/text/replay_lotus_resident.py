"""Original LOTUS/LM/LiteLLM/IPC resident replay, with identity-bound saved replies."""
import argparse
from dataclasses import replace
import json
from pathlib import Path
import socket
import sys
from unittest.mock import patch

from src.infrastructure.private_artifacts import content_digest, new_private_directory, read_json, write_private_json
from src.experiments.attempt_ledger import AttemptBudget, BudgetExhausted
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.shared_request_budget import SharedClaimedUnit
from src.serving.backends.async_engine_process import AsyncEngineProcess
from src.serving.backends.vllm_protocol import EngineSpec


class RecordedProcess(AsyncEngineProcess):
    @staticmethod
    def _command(spec, descriptors):
        return [sys.executable, '-B', str(Path(__file__).with_name('lotus_native_ipc_fixture.py')),
                '--recorded', *map(str, descriptors)]


def replay(config_path, checkout, data_dir, reference, output):
    from transformers import AutoTokenizer
    from src.experiments.sembench_text_query import configuration, query_from_config
    from src.experiments.sembench_text_resident import verify_binding, run
    from src.semantic_methods.lotus.record_method import LotusRecordMethod
    from src.baselines.text.frameworks import lotus_native_local as native

    config, engine, generation, resources = configuration(config_path)
    assert config['lifecycle'] == 'resident'
    query = query_from_config(config, checkout, data_dir, generation)
    binding = verify_binding(config, query, engine)
    saved = read_json(reference)
    prepared = LotusRecordMethod(query.operation.stage).prepare_rows(query.operation.records, query_id=query.query_id)
    messages = [json.loads(row['messages_json']) for row in prepared]
    assert messages == [json.loads(row['messages_json']) for row in saved['calls']]
    tokenizer = AutoTokenizer.from_pretrained(engine.tokenizer_path or engine.model_path, local_files_only=True)
    tokens = [tokenizer.apply_chat_template(m, tokenize=True, add_generation_prompt=True) for m in messages]
    assert tokens == saved['token_ids']
    output = Path(output)
    new_private_directory(output)
    responses = {content_digest(message):dict(text=text, token_ids=ids, source_position=record.source_position,
        source_index=record.source_index) for message,text,ids,record in
        zip(messages,saved['texts'],tokens,query.operation.records,strict=True)}
    assert len(responses) == len(prepared)
    fixture = output/'responses.json'
    write_private_json(fixture,responses)
    value = config['budget']
    ledger = CellBudgetLedger.create(Path(value['path']),AttemptBudget(value['id'],value['limit']),deadline_utc=None)
    for unit in value['unit_ids']:
        ledger.reserve_unit(unit,max(1,len(prepared)))
    cpu_engine = replace(engine,model_path=str(fixture),model_python=sys.executable,
        driver_python=sys.executable,max_model_len=32768)
    connect = socket.socket.connect
    def no_network(sock,address):
        if sock.family in (socket.AF_INET,socket.AF_INET6):
            raise RuntimeError('resident CPU fixture forbids model endpoints/network')
        return connect(sock,address)
    with patch.object(socket.socket,'connect',no_network), \
            patch.object(native,'AsyncEngineProcess',RecordedProcess), \
            patch.object(EngineSpec,'validate_driver_environment',return_value={'CPU_fixture':True}):
        session = run(config,checkout,data_dir,cpu_engine,generation,resources,output/'session')
    assert session['status'] == 'completed' and session['owner_start_count'] == 1
    assert session['owners'][0]['close']['status'] == 'closed'
    previous = None
    all_ids = set()
    for index,round_ in enumerate(session['queries']):
        actual = round_['backend_report']
        assert actual['result'] == saved['result'] and actual['quality'] == saved['quality']
        if 'relation' in saved:
            assert actual['operator_relation'] == saved['relation']
        calls = sorted(actual['operator']['calls']['records'],key=lambda c:c['source_position'])
        assert len(calls) == len(prepared)
        for i,call in enumerate(calls):
            assert call['source_position'] == call['fixture_source_position'] == i
            assert call['fixture_source_index'] == query.operation.records[i].source_index
            assert json.loads(call['fixture_messages']) == messages[i]
            assert call['text'] == saved['texts'][i]
            assert json.loads(call['prompt_token_ids_json']) == tokens[i]
            assert call['fixture_generation'] == query.operation.stage.generation.kwargs()
        state = actual['snapshot']
        if previous is not None:
            for name in ('engine_uid','model_API_pid','process_created','owner_id','provider_name'):
                assert state[name] == previous[name]
            assert actual['startup_this_round_seconds'] == 0
        else:
            assert actual['startup_this_round_seconds'] > 0
        previous = state
        ids = {c['engine_request_id'] for c in calls}
        assert not all_ids & ids
        all_ids |= ids
        times = [actual[k] for k in ('source_start_ns','prepare_finished_ns','model_ready_observed_ns',
            'sink_ready_ns','snapshot_finished_ns','audit_finished_ns','evaluation_started_ns','evaluation_finished_ns')]
        assert times == sorted(times)
        assert times[-1] <= round_['persistence_started_ns'] <= round_['persistence_finished_ns']
        unit = SharedClaimedUnit(Path(value['path']),AttemptBudget(value['id'],value['limit']),value['unit_ids'][index])
        assert unit.attempts == len(prepared) and unit.remaining == 0
        try:
            unit.reserve('f'*64)
        except BudgetExhausted:
            pass
        else:
            raise AssertionError('completed native query accepted another attempt')
    events = [json.loads(line) for line in Path(str(fixture)+'.events').read_text().splitlines()]
    assert sum(e['event']=='construct' for e in events) == sum(e['event']=='close' for e in events) == 1
    assert sum(e['event']=='start' for e in events) == sum(e['event']=='end' for e in events) == value['limit']
    receipt = dict(status='passed',model_calls=0,source_binding=binding,software_IPC_requests=value['limit'],
        complete_messages_tokens_native_relation_sink_evaluator_equal=True,source_identity_bound=True,
        same_actual_owner_model_PID_and_birth=True,constructs=1,closes=1,
        fresh_query_source_and_post_sink_clock_order=True,extra_attempt_rejected=True,
        limitations=['saved responses, not GPU output/quality/performance','existing supervisor not run by this direct CPU replay',
                    'native model/version/physical GPU and host cleanup need target qualification'])
    write_private_json(output/'CPU.json',receipt)
    return receipt


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('config','sembench-checkout','data-dir','reference','output-dir'):
        parser.add_argument('--'+name,required=True)
    args = parser.parse_args()
    print(json.dumps(replay(args.config,args.sembench_checkout,args.data_dir,args.reference,args.output_dir)))
