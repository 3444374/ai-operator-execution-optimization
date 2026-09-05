"""Execute the seven-attempt follow-up against the original 32-attempt ledger.

Only experiment effects live here. PostgreSQL, gateway, HTTP adapter and resource
assessment come from the fixed repository. Full request/output evidence stays in
the private run root; the final public audit is a separate allowlisted operation.
"""
import argparse
from dataclasses import asdict
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import pwd
import subprocess
import sys
import threading
import time
import urllib.request

import psycopg
from psycopg import sql

from src.baselines.common.redact import redact_text
from src.experiments.postgresql.resource_lifecycle import RunSpec
from src.experiments.postgresql.resource_phase import execute_phase, hashes, save_json
from src.experiments.postgresql.runtime_helpers import isolated_pg18_cluster, owned_child_process, wait_for_path
from src.experiments.postgresql.semmap_resource_runner import ObservationProbe, query_after_observation
from src.observability.process_resources.model import PgFileClassificationContext
from src.observability.process_resources.recorder import ProcfsTickSampler

LEGACY_HASHES = {
    'legacy_checks': 'f0feb20978e0c8d4389cbfae322f38515c74c1b26f908dace511ae2ea11ef217',
    'legacy_observer': '476424cd689390f3e86d0a7dec4c6186dbb2b7eb48bab8774541b05fa45880d4',
}


def load_events(path):
    return [] if not path.exists() else [json.loads(line) for line in path.read_text().splitlines()]


def attempts(path):
    rows = load_events(path)
    assert rows[0] == {'schema_version': 1, 'budget_id': 'semloom.semmap.4d.real.v1', 'limit': 32}
    assert len(rows) <= 33
    assert all(row['attempt'] == n for n, row in enumerate(rows[1:], 1))
    return len(rows) - 1


def service_idle():
    import re
    with urllib.request.urlopen('http://127.0.0.1:18150/metrics', timeout=5) as response:
        text = response.read().decode()
    values = {}
    for line in text.splitlines():
        match = re.fullmatch(r'vllm:num_requests_(running|waiting)(?:\{.*\})? ([0-9.e+-]+)', line)
        if match:
            values.setdefault(match[1], []).append(float(match[2]))
    assert set(values) == {'running', 'waiting'}, 'missing_service_queue_metrics'
    return {key: sum(value) for key, value in values.items()}


def wait_idle():
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        value = service_idle()
        if value == {'running': 0, 'waiting': 0}:
            return value
        time.sleep(.1)
    raise RuntimeError('service_queue_did_not_drain')


def run(args):
    args.root.mkdir()
    summary = {'status': 'incomplete', 'runtime_commit': args.commit, 'initial_attempts': None,
               'request_limit': 32, 'quality_evaluated': False, 'performance_evaluated': False, 'phases': {}}
    try:
        summary['initial_attempts'] = attempts(args.ledger)
        for role, expected in LEGACY_HASHES.items():
            assert hashlib.sha256(getattr(args, role).read_bytes()).hexdigest() == expected, 'legacy_helper_identity'
        save_json(args.root/'manifest.json', {**summary, 'experiment_files_sha256': {
            role: hashlib.sha256(path.read_bytes()).hexdigest()
            for role, path in {'runner': Path(__file__), 'gateway': args.gateway,
                               'legacy_checks': args.legacy_checks, 'legacy_observer': args.legacy_observer}.items()}})
        assert summary['initial_attempts'] == 25, 'unexpected_existing_attempt_count'
        assert subprocess.check_output(['git', '-C', str(args.repo), 'rev-parse', 'HEAD'], text=True).strip() == args.commit
        assert not subprocess.check_output(['git', '-C', str(args.repo), 'status', '--porcelain'], text=True).strip()
        service = json.loads(args.service_verification.read_text())
        assert service['verified'] and service['model_revision'] == 'a09a35458c702b33eeacc393d103063234e8bc28'
        assert service['config_sha256'] == hashlib.sha256(args.config.read_bytes()).hexdigest(), 'service_config_identity'
        def verify_service_identity():
            process = Path('/proc')/str(service['pid'])
            stat = (process/'stat').read_text()
            assert int(stat[stat.rfind(')')+2:].split()[19]) == service['start_time_ticks'], 'service_process_identity'
            assert hashlib.sha256((process/'cmdline').read_bytes()).hexdigest() == service['cmdline_sha256'], 'service_command_identity'
        verify_service_identity()
        save_json(args.root/'service-verification.json', service)
        spec = importlib.util.spec_from_file_location('prior_real_contract', args.legacy_checks)
        prior = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(prior)
        from transformers import AutoTokenizer
        tokenizer = AutoTokenizer.from_pretrained(str(args.model_root), local_files_only=True)
        cancel_instruction = 'Write exactly 128 numbered one-word items and do not stop early.'
        token_cases = [('unicode', '数据库与人工智能', prior.INSTRUCTION), ('empty', '', prior.INSTRUCTION),
                       ('ascii', 'Hello, SemLoom.', prior.INSTRUCTION),
                       ('cancel', 'cancel this generation', cancel_instruction),
                       ('cancel_recovery', 'after cancel', prior.INSTRUCTION),
                       ('reject', 'token ' * 18000, prior.INSTRUCTION),
                       ('reject_recovery', 'after model error', prior.INSTRUCTION)]
        tokens = {}
        for name, text, instruction in token_cases:
            messages = [{'role': 'system', 'content': instruction}, {'role': 'user', 'content': text}]
            count = len(tokenizer.apply_chat_template(messages, tokenize=True, add_generation_prompt=True, return_dict=False))
            tokens[name] = {'prompt_tokens': count, 'max_tokens': 128, 'input_bytes': len(text.encode())}
            assert (count + 128 > 4096) if name == 'reject' else (count + 128 <= 4096)
        save_json(args.root/'token-preflight.json', tokens)
        save_json(args.root/'service-initial-queue.json', wait_idle())
        user = pwd.getpwuid(os.getuid())
        with isolated_pg18_cluster(args.prefix, args.root, user) as connection:
            for table in ('resource_rows', 'insert_rows', 'map_sink'):
                connection.execute(sql.SQL('CREATE TABLE {} (id integer, payload text)').format(sql.Identifier(table)))
            with connection.cursor() as cursor:
                cursor.executemany('INSERT INTO resource_rows VALUES (%s,%s)', [(1, '数据库与人工智能'), (2, ''), (3, None)])
                cursor.executemany('INSERT INTO insert_rows VALUES (%s,%s)', [(4, 'Hello, SemLoom.'), (5, None)])
            connection.execute('INSERT INTO map_sink VALUES (-1,NULL)')
            connection.execute('DELETE FROM map_sink')
            for table in ('resource_rows', 'insert_rows', 'map_sink'):
                connection.execute(sql.SQL('SELECT * FROM {}').format(sql.Identifier(table))).fetchall()
            filenodes = connection.execute("SELECT relfilenode FROM pg_class WHERE relnamespace='public'::regnamespace AND relfilenode<>0").fetchall()
            context = PgFileClassificationContext(str(args.root/'data'), frozenset(row[0] for row in filenodes), frozenset())
            sessions, http_events = args.root/'sessions.jsonl', args.root/'http-events.jsonl'
            releases = args.root/'releases'; releases.mkdir()
            socket_path = args.root/'socket/provider.sock'
            command = [sys.executable, str(args.gateway), '--legacy-observer', str(args.legacy_observer),
                       '--session-events', str(sessions), '--releases', str(releases), '--events', str(http_events),
                       '--ledger', str(args.ledger), '--', '--socket', str(socket_path), '--fixed-model-config', str(args.config)]
            with owned_child_process(command, args.root, 'gateway', dict(os.environ, PYTHONPATH=str(args.repo/'code')), user) as gateway:
                wait_for_path(socket_path, gateway)
                connection.execute("SET semloom_pg.provider_execution_profile='openai-compatible-fixed'")
                connection.execute("SET statement_timeout='120s'")
                connection.execute("SELECT set_config('semloom_pg.gateway_socket',%s,false)", (str(socket_path),))
                expr = prior.map_expression(sql.Identifier('payload'))
                # Zero-work carriers and ordinary file access warmup consume no model request.
                connection.execute(sql.SQL('EXPLAIN SELECT {} FROM resource_rows').format(expr)).fetchall()
                assert connection.execute(sql.SQL('SELECT {} FROM resource_rows LIMIT 0').format(expr)).fetchall() == []
                assert connection.execute(sql.SQL('SELECT {} FROM resource_rows WHERE id=3').format(expr)).fetchall() == [(None,)]
                assert attempts(args.ledger) == 25 and load_events(http_events) == []
                save_json(args.root/'zero-calls.json', {'explain_limit0_null_only': True, 'attempts': 25})
                accumulated = 25

                def phase(name, query, inputs, expected_state=None, instruction=None):
                    nonlocal accumulated
                    before = len(load_events(http_events))
                    session_number = sum(e['event'] == 'session_start' for e in load_events(sessions)) + 1
                    probe = ObservationProbe(ProcfsTickSampler({'backend': connection.info.backend_pid, 'gateway': gateway.pid}, str(socket_path), context))
                    actual = {}
                    def invoke():
                        done = threading.Event()
                        canceler = None
                        if name == 'cancel':
                            def cancel_after_request():
                                deadline = time.monotonic() + 10
                                while not done.is_set() and time.monotonic() < deadline:
                                    if len(load_events(http_events)) > before:
                                        if not done.wait(.1):
                                            connection.cancel()
                                        return
                                    done.wait(.005)
                            canceler = threading.Thread(target=cancel_after_request)
                            canceler.start()
                        try:
                            actual['rows'] = query()
                            save_json(args.root/name/'sql-result.json', actual['rows'])
                            return {'sql_completed': True, 'rows': len(actual['rows'])}
                        except psycopg.Error as error:
                            actual['error_message'] = error.diag.message_primary
                            raise
                        finally:
                            done.set()
                            if canceler is not None:
                                canceler.join(timeout=15)
                                assert not canceler.is_alive(), 'canceler_survived'
                    def verify():
                        events = load_events(http_events)[before:]
                        save_json(args.root/name/'http-evidence.json', events)
                        requests = [e for e in events if e['event'] == 'request']
                        assert len(requests) == len(inputs), 'request_count'
                        for event, text in zip(requests, inputs):
                            prior.verify_request(event, text, instruction or prior.INSTRUCTION)
                        assert attempts(args.ledger) == accumulated + len(inputs), 'ledger_count'
                        if expected_state is None:
                            rows = actual['rows']
                            expected_ids = [1, 2, 3] if name == 'select' else ([4, 5] if name == 'insert' else [1])
                            assert all(len(row) == 3 for row in rows) and sorted(row[0] for row in rows) == expected_ids, 'row_shape_ids'
                            nonnull = [row for row in rows if row[1] is not None]
                            assert [row[1] for row in nonnull] == inputs, 'row_input_order'
                            prior.verify_success_pairs(events, inputs, [row[2] for row in nonnull], tokenizer)
                            assert all(row[2] is None for row in rows if row[1] is None), 'null_output'
                            if name == 'insert':
                                node = prior.find_map_plan(json.loads((args.root/name/'plan.json').read_text()))
                                completions = [event for event in events if event['event'] == 'completion']
                                assert node['Prompt Tokens'] == sum(event['prompt_tokens'] for event in completions), 'plan_prompt_usage'
                                assert node['Output Tokens'] == sum(event['output_tokens'] for event in completions), 'plan_output_usage'
                        elif name == 'reject':
                            assert len(events) == 2 and events[1]['event'] == 'error' and events[1]['code'] == 'MODEL_REQUEST_REJECTED', 'model_rejection'
                            assert actual['error_message'] == 'SemLoom model request was rejected', 'model_error_message'
                        elif name == 'cancel':
                            assert len(events) == 2 and events[1]['event'] in ('completion', 'error'), 'cancel_attempt_unfinished'
                        save_json(args.root/name/'service-ended-queue.json', wait_idle())
                        return []
                    result = execute_phase(root=args.root/name, phase=name, spec=RunSpec('diagnostic'), sampler=probe,
                        operation=lambda: query_after_observation(invoke, probe, releases/f'session-{session_number}', connection.cancel),
                        events=lambda: load_events(sessions), expected_tasks=len(inputs), expected_sessions=1,
                        expected_sqlstate=expected_state, extra_checks=verify)
                    summary['phases'][name] = asdict(result)
                    save_json(args.root/'progress.json', summary)
                    assert result.assessment == ('valid', 'passed'), 'phase_did_not_pass'
                    accumulated += len(inputs)

                phase('select', lambda: connection.execute(sql.SQL('SELECT id,payload,{} FROM resource_rows').format(expr)).fetchall(), ['数据库与人工智能', ''])
                def insert():
                    plan = connection.execute(sql.SQL('EXPLAIN (ANALYZE, FORMAT JSON) INSERT INTO map_sink SELECT id,{} FROM insert_rows').format(expr)).fetchone()[0][0]
                    save_json(args.root/'insert/plan.json', plan)
                    node = prior.find_map_plan(plan)
                    assert node['Model Calls'] == node['Accepted Rows'] == node['Emitted Rows'] == 1
                    return connection.execute('SELECT i.id,i.payload,s.payload FROM insert_rows i JOIN map_sink s USING(id) ORDER BY i.id').fetchall()
                phase('insert', insert, ['Hello, SemLoom.'])
                def one(text, instruction=None):
                    expression = prior.map_expression(sql.Literal(text), instruction or prior.INSTRUCTION)
                    output = connection.execute(sql.SQL('SELECT {} FROM ONLY resource_rows WHERE id=1').format(expression)).fetchone()[0]
                    return [(1, text, output)]
                phase('cancel', lambda: one('cancel this generation', cancel_instruction), ['cancel this generation'], '57014', cancel_instruction)
                phase('cancel_recovery', lambda: one('after cancel'), ['after cancel'])
                phase('reject', lambda: one('token ' * 18000), ['token ' * 18000], '38000')
                phase('reject_recovery', lambda: one('after model error'), ['after model error'])
                assert attempts(args.ledger) == 32
                verify_service_identity()
                summary['status'] = 'passed'
    except BaseException as error:
        summary['error_type'] = type(error).__name__
        summary['error_code'] = str(error) if isinstance(error, AssertionError) and str(error).replace('_','').isalnum() else 'real_followup_failed'
        raise
    finally:
        try:
            summary['final_attempts'] = attempts(args.ledger)
        except Exception as error:
            summary['final_attempts'] = None
            summary['ledger_read_error'] = type(error).__name__
            summary['status'] = 'incomplete'
        save_json(args.root/'summary.json', summary)
        save_json(args.root/'SHA256SUMS.json', hashes(args.root))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('repo', 'root', 'prefix', 'ledger', 'config', 'model-root', 'gateway', 'legacy-checks', 'legacy-observer', 'service-verification'):
        parser.add_argument('--'+name, type=Path, required=True)
    parser.add_argument('--commit', required=True)
    run(parser.parse_args())
