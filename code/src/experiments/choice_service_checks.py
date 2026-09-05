"""Audit a bounded old/choice SemFilter smoke through SQL and actual HTTP records."""
import argparse
import hashlib
import json
import os
from pathlib import Path
try:
    import pwd
except ImportError:  # Windows checkouts import this module for unit tests only.
    pwd = None
import sys
import time

from src.baselines.common.redact import redact_text
from src.experiments.attempt_ledger import AttemptLedger
from src.experiments.choice_gateway_observer import CHOICE_BUDGET


MODEL = 'Qwen2.5-1.5B-Instruct'
INSTRUCTION = 'The input asks for writing, explaining, or debugging computer code.'
INPUTS = ('Write a Python function that adds two integers.',
          'Give me a recipe for tomato soup.', 'Can you explain this?')


def verify_choice_pair(old, choice):
    """Require identical JSON values and types except the ordered choice field."""
    if 'structured_outputs' in old or choice.get('structured_outputs') != {
            'choice': ['TRUE', 'FALSE', 'UNKNOWN']}:
        raise ValueError('unexpected choice request profile')
    remainder = {key: value for key, value in choice.items() if key != 'structured_outputs'}
    if json.dumps(old, sort_keys=True) != json.dumps(remainder, sort_keys=True):
        raise ValueError('request difference is not limited to choice')


def verify_completion(completion, plan, sqlstate, model):
    """Check PG rows and usage, including strict rejection of invalid old output."""
    if completion['response_model_id'] != model or completion['finish_reason'] != 'stop':
        raise ValueError('unexpected completion identity or finish reason')
    valid = completion['raw_output'] in ('TRUE', 'FALSE', 'UNKNOWN')
    if not valid:
        if sqlstate != '22000' or plan is not None:
            raise ValueError('invalid output was not rejected by PostgreSQL')
        return False
    rows = int(completion['raw_output'] == 'TRUE')
    expected = {'Model Calls': 1, 'Actual Rows': rows, 'Emitted Rows': rows,
                'Prompt Tokens': completion['prompt_tokens'], 'Output Tokens': completion['output_tokens']}
    if sqlstate is not None or plan is None or any(plan.get(key) != value for key, value in expected.items()):
        raise ValueError('PostgreSQL rows or usage differ from raw completion')
    return True


def verify_prompt_usage(tokenizer, request, completion):
    """Compare reported prompt usage with the tokenizer's rendered messages."""
    tokens = tokenizer.apply_chat_template(
        request['messages'], tokenize=True, add_generation_prompt=True, return_dict=False)
    if completion['prompt_tokens'] != len(tokens):
        raise ValueError('reported prompt usage differs from chat template tokens')


def save(path, value):
    with path.open('x', encoding='utf-8') as handle:
        handle.write(redact_text(json.dumps(value, ensure_ascii=False, indent=2)) + '\n')


def file_sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def service_snapshot(args):
    if args.fixture_only:
        return {'kind': 'fixture'}
    identity = json.loads(args.identity.read_text())
    process = Path('/proc')/str(identity['pid'])
    assert identity['package_version'] == '0.25.1'
    assert process.joinpath('stat').read_text().rsplit(')', 1)[1].split()[19] == identity['process_start_time_ticks']
    argv = process.joinpath('cmdline').read_bytes().decode().split('\0')[:-1]
    expected = {'--model': str(args.model_root), '--served-model-name': MODEL,
                '--dtype': 'bfloat16', '--max-model-len': '4096', '--gpu-memory-utilization': '0.25',
                '--scheduling-policy': 'fcfs', '--max-num-seqs': '1', '--max-num-batched-tokens': '4096',
                '--tensor-parallel-size': '1', '--host': '127.0.0.1', '--port': str(identity['port'])}
    for flag, value in expected.items():
        assert argv.count(flag) == 1 and argv[argv.index(flag)+1] == value, flag
    assert '--enforce-eager' in argv and '--no-enable-prefix-caching' in argv
    assert b'CUDA_VISIBLE_DEVICES=0' in process.joinpath('environ').read_bytes().split(b'\0')
    config = json.loads(args.config.read_text())
    assert config['endpoint_url'] == f"http://127.0.0.1:{identity['port']}/v1/chat/completions"
    assert config['model_id'] == MODEL and config['choice_format'] == 'vllm_structured_outputs'
    expected_files = json.loads(args.model_manifest.read_text())
    observed = {name: file_sha(args.model_root/name) for name in expected_files}
    assert observed == expected_files
    generation = json.loads((args.model_root/'generation_config.json').read_text())
    assert generation['repetition_penalty'] == 1.1
    return dict(kind='real', identity=identity, argv=argv, model_files=observed,
                inherited_generation=generation, config_sha256=file_sha(args.config))


def query(connection, choice, index):
    import psycopg
    options = dict(model=MODEL, temperature=0, max_tokens=8)
    if choice:
        options['generation_profile'] = 'semloom.generation.choice.tristate.v1'
    statement = psycopg.sql.SQL(
        'EXPLAIN (ANALYZE, FORMAT JSON) SELECT id FROM choice_inputs '
        'WHERE id=%s AND ai_semantic.filter(payload,{},{}::jsonb)'
    ).format(psycopg.sql.Literal(INSTRUCTION), psycopg.sql.Literal(json.dumps(options)))
    try:
        return connection.execute(statement, (index,)).fetchone()[0][0]['Plan'], None
    except psycopg.Error as error:
        return None, error.sqlstate


def run_queries(args, connection, ledger, tokenizer):
    events_path = args.root/'events.jsonl'
    schedule = [(False, 0, -1), (True, 0, -1)] + [
        (choice, index, repeat) for repeat in range(2) for index in range(3) for choice in (False, True)]
    save(args.root/'schedule.json', dict(instruction=INSTRUCTION, inputs=INPUTS, schedule=schedule))
    results = []
    for choice in (False, True):
        before = ledger.attempts
        plan, state = query(connection, choice, 3)
        save(args.root/f'null-{choice}.json', dict(plan=plan, sqlstate=state))
        assert state is None and plan['Model Calls'] == 0 and plan['Actual Rows'] == 0
        assert ledger.attempts == before and not events_path.read_text()
    old_request = None
    consumed_events = 0
    for number, (choice, index, repeat) in enumerate(schedule):
        before = ledger.attempts
        started = time.monotonic()
        plan, state = query(connection, choice, index)
        events = [json.loads(line) for line in events_path.read_text().splitlines()]
        current = events[consumed_events:]
        consumed_events = len(events)
        record = dict(choice=choice, input_index=index, repeat=repeat, plan=plan, sqlstate=state,
                      elapsed_seconds=time.monotonic()-started, events=current,
                      attempts_before=before, attempts_after=ledger.attempts)
        save(args.root/f'query-{number:02}.json', record)
        assert ledger.attempts == before + 1
        assert [event['event'] for event in current] == ['request', 'completion']
        request, completion = current[0]['body'], current[1]
        assert current[0]['attempt'] == before + 1
        valid = verify_completion(completion, plan, state, MODEL)
        if choice:
            verify_choice_pair(old_request, request)
            if not valid:
                raise ValueError('choice output failed the PostgreSQL parser')
        else:
            old_request = request
        if tokenizer is not None:
            verify_prompt_usage(tokenizer, request, completion)
        results.append(dict(choice=choice, input_index=index, repeat=repeat,
                            format_valid=valid, sqlstate=state, raw_output=completion['raw_output']))
    return results


def run(args):
    from src.experiments.choice_resource_checks import child, cluster, wait_file
    user = pwd.getpwnam('postgres')
    args.root.mkdir()
    os.chown(args.root, user.pw_uid, user.pw_gid)
    ledger = AttemptLedger(args.ledger, CHOICE_BUDGET)
    initial_attempts = ledger.attempts
    assert initial_attempts + 14 <= 100
    before = service_snapshot(args)
    save(args.root/'service-before.json', before)
    tokenizer = None
    if not args.fixture_only:
        from transformers import AutoTokenizer
        tokenizer = AutoTokenizer.from_pretrained(args.model_root, local_files_only=True)
    status = 'failed'
    try:
        with cluster(args.prefix, args.root, user) as connection:
            connection.execute('CREATE TABLE choice_inputs(id integer, payload text)')
            with connection.cursor() as cursor:
                cursor.executemany('INSERT INTO choice_inputs VALUES (%s,%s)', enumerate((*INPUTS, None)))
            socket_path = args.root/'socket/provider.sock'
            env = dict(os.environ, PYTHONPATH=str(args.repo/'code'), PYTHONDONTWRITEBYTECODE='1')
            command = [sys.executable, '-m', 'src.experiments.choice_gateway_observer',
                       '--events', str(args.root/'events.jsonl'), '--ledger', str(args.ledger),
                       '--', '--socket', str(socket_path), '--fixed-model-config', str(args.config)]
            with child(command, args.root, 'gateway', env, user) as gateway:
                wait_file(socket_path, gateway)
                connection.execute("SET semloom_pg.provider_execution_profile='openai-compatible-fixed'")
                connection.execute("SET statement_timeout='70s'")
                connection.execute("SELECT set_config('semloom_pg.gateway_socket',%s,false)", (str(socket_path),))
                results = run_queries(args, connection, ledger, tokenizer)
                save(args.root/'results.json', results)
                gateway.terminate()
                assert gateway.wait(timeout=5) == 0 and not socket_path.exists()
        after = service_snapshot(args)
        save(args.root/'service-after.json', after)
        assert before == after and ledger.attempts == initial_attempts + 14
        status = 'passed'
    finally:
        save(args.root/'summary.json', dict(status=status, kind='fixture' if args.fixture_only else 'real',
             initial_attempts=initial_attempts, final_attempts=ledger.attempts,
             attempted_requests=ledger.attempts-initial_attempts, quality_evaluated=False))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('repo', 'root', 'prefix', 'config', 'ledger'):
        parser.add_argument('--'+name, type=Path, required=True)
    parser.add_argument('--fixture-only', action='store_true')
    for name in ('identity', 'model-root', 'model-manifest'):
        parser.add_argument('--'+name, type=Path)
    args = parser.parse_args()
    if not args.fixture_only and not all((args.identity, args.model_root, args.model_manifest)):
        parser.error('real checks require service identity, model root and model file manifest')
    run(args)


if __name__ == '__main__':
    main()
