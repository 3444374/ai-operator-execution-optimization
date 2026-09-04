"""Measure fixed, fixture-only choice resource cells on isolated PostgreSQL 18.3.

The runner owns its cluster and child processes. It records observations before
checking predeclared limits; no model service is started or contacted here.
"""
import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import statistics

try:
    import pwd
except ImportError:  # Windows checkouts import this module for unit tests only.
    pwd = None
import subprocess
import sys
import threading
import time

import psycopg
import psutil


SAMPLE_SECONDS = 0.02
MIB = 1024 * 1024
MODEL = 'choice-resource-fixture-v1'
INSTRUCTION = 'Classify input.'


def save(path, value):
    with path.open('x', encoding='utf-8') as handle:
        json.dump(value, handle, indent=2)
        handle.write('\n')


def sample(process):
    return dict(rss_bytes=process.memory_info().rss, fd=process.num_fds(), threads=process.num_threads())


def settled(processes):
    time.sleep(0.5)
    snapshots = []
    for _ in range(5):
        snapshots.append({key: sample(value) for key, value in processes.items()})
        time.sleep(SAMPLE_SECONDS)
    result = {key: {field: int(statistics.median(item[key][field] for item in snapshots))
                    for field in ('rss_bytes', 'fd', 'threads')} for key in processes}
    for key, process in processes.items():
        # Container root may lack ptrace rights over another UID's proc links.
        result[key]['fd_targets'] = json.loads(subprocess.check_output(
            [sys.executable, '-c',
             'import json,os,pathlib,sys; '
             'print(json.dumps({p.name:os.readlink(p) for p in pathlib.Path(sys.argv[1]).iterdir()}))',
             f'/proc/{process.pid}/fd'], text=True, user=process.uids().real,
            group=process.gids().real, extra_groups=[]))
    return result


def observe(processes, operation):
    samples = []
    stop = threading.Event()
    failures = []

    def collect():
        while not stop.is_set():
            try:
                samples.append(dict(monotonic=time.monotonic(),
                                    processes={key: sample(value) for key, value in processes.items()}))
            except (psutil.Error, OSError) as error:
                failures.append(type(error).__name__)
                break
            stop.wait(SAMPLE_SECONDS)

    worker = threading.Thread(target=collect)
    worker.start()
    try:
        result = operation()
    finally:
        stop.set()
        worker.join()
    assert not failures, failures
    assert samples
    return result, samples


def check_resources(baseline, ending, samples, *, blocked_dns=False):
    for key in baseline:
        assert ending[key]['rss_bytes'] - baseline[key]['rss_bytes'] <= 4 * MIB, (key, 'settled RSS')
        assert ending[key]['fd'] == baseline[key]['fd'], (key, 'settled FD')
        allowance = 1 if blocked_dns and key == 'gateway' else 0
        assert ending[key]['threads'] == baseline[key]['threads'] + allowance, (key, 'settled threads')
        for point in samples:
            value = point['processes'][key]
            assert value['rss_bytes'] - baseline[key]['rss_bytes'] <= 16 * MIB, (key, 'peak RSS')
            assert value['fd'] <= baseline[key]['fd'] + 3, (key, 'peak FD')
            assert value['threads'] <= baseline[key]['threads'] + 2, (key, 'peak threads')


def wait_file(path, process):
    for _ in range(500):
        if path.exists():
            return
        assert process.poll() is None, ('child exited before ready', process.returncode)
        time.sleep(0.02)
    raise RuntimeError('test process did not become ready')


@contextmanager
def child(command, root, name, env, user):
    with (root / (name + '.log')).open('x') as log:
        process = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT,
                                   user=user.pw_uid, group=user.pw_gid, extra_groups=[])
        try:
            yield process
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                    raise RuntimeError('owned test process required forced termination')


@contextmanager
def cluster(prefix, root, user):
    data = root / 'data'
    socket_dir = root / 'socket'
    socket_dir.mkdir()
    os.chown(socket_dir, user.pw_uid, user.pw_gid)
    # runuser is root-only; a runner already executing as the cluster user
    # (required on hosts where /proc fd inspection needs target-user identity)
    # must invoke the PostgreSQL tools directly.
    pg = [] if os.getuid() == user.pw_uid else ['runuser', '-u', user.pw_name, '--']
    with (root / 'cluster.log').open('x') as log:
        subprocess.run(pg + [str(prefix/'bin/initdb'), '-D', str(data), '--no-locale', '-E', 'UTF8'],
                       check=True, stdout=log, stderr=subprocess.STDOUT)
        ctl = pg + [str(prefix/'bin/pg_ctl'), '-D', str(data)]
        subprocess.run(ctl + ['-l', str(root/'postgres.log'), '-o',
            f"-c listen_addresses='' -c shared_preload_libraries=semloom_pg -k {socket_dir} -p 55446",
            '-w', 'start'], check=True, stdout=log, stderr=subprocess.STDOUT)
        try:
            with psycopg.connect(host=str(socket_dir), port=55446, user='postgres', dbname='postgres',
                                  autocommit=True) as connection:
                assert connection.execute('SHOW server_version').fetchone()[0] == '18.3'
                connection.execute('CREATE EXTENSION semloom_pg')
                yield connection
        finally:
            subprocess.run(ctl + ['-m', 'fast', '-w', 'stop'], check=True, stdout=log, stderr=subprocess.STDOUT)


@contextmanager
def fixture_gateway(args, user, label, *, choice, requests, delay_ms=0, timeout_ms=1000, dns=False):
    root = args.root / label
    root.mkdir()
    os.chown(root, user.pw_uid, user.pw_gid)
    port_file = root / 'http.port'
    fixture = args.repo / 'code/postgres/semloom_pg/t/fixtures/openai_compatible_server.py'
    env = dict(os.environ, PYTHONPATH=str(args.repo/'code'), PYTHONDONTWRITEBYTECODE='1')
    command = [sys.executable, str(fixture), '--port-file', str(port_file), '--model-id', MODEL,
               '--max-requests', str(requests), '--delay-ms', str(delay_ms)]
    if choice:
        command += ['--require-choice']
    if dns:
        command += ['--request-log', str(root/'http.jsonl')]
    with child(command, root, 'http', env, user) as http:
        wait_file(port_file, http)
        config = root/'fixed.json'
        save(config, dict(endpoint_url=f'http://127.0.0.1:{int(port_file.read_text())}/v1/chat/completions',
                          model_id=MODEL, timeout_ms=timeout_ms, choice_format='vllm_structured_outputs'))
        # Short UDS paths remain outside the data directory but are owned here.
        socket_path = args.root/'socket'/f'{label}.sock'
        command = [sys.executable, '-m', 'src.experiments.choice_gateway_observer',
                   '--events', str(root/'events.jsonl'), '--fixture-only']
        if dns:
            command += ['--dns-release-file', str(root/'dns.release')]
        command += ['--', '--socket', str(socket_path), '--fixed-model-config', str(config)]
        with child(command, root, 'gateway', env, user) as gateway:
            wait_file(socket_path, gateway)
            yield root, socket_path, gateway
            gateway.terminate()
            assert gateway.wait(timeout=3) == 0
            assert not socket_path.exists()
            assert http.wait(timeout=3) == 0
            assert not port_file.exists()


def explain(connection, *, choice, rows):
    options = dict(model=MODEL, temperature=0, max_tokens=8)
    if choice:
        options['generation_profile'] = 'semloom.generation.choice.tristate.v1'
    statement = psycopg.sql.SQL(
        'EXPLAIN (ANALYZE, FORMAT JSON) SELECT id FROM resource_rows '
        'WHERE id<=%s AND ai_semantic.filter(payload,{},{}::jsonb)'
    ).format(psycopg.sql.Literal(INSTRUCTION), psycopg.sql.Literal(json.dumps(options)))
    plan = connection.execute(statement, (rows,)).fetchone()[0][0]['Plan']
    assert plan['Custom Plan Provider'] == 'SemLoom SemFilter'
    assert plan['Model Calls'] == rows and plan['Emitted Rows'] == rows and plan['Actual Rows'] == rows
    assert plan['Prompt Tokens'] == 17 * rows and plan['Output Tokens'] == rows
    return {key: plan[key] for key in ('Model Calls', 'Emitted Rows', 'Actual Rows', 'Prompt Tokens', 'Output Tokens')}


def normal_checks(args, connection, user, choice):
    label = 'v4' if choice else 'v3'
    with fixture_gateway(args, user, label, choice=choice, requests=64+100+1000+4000) as (root, socket_path, gateway):
        connection.execute("SELECT set_config('semloom_pg.gateway_socket',%s,false)", (str(socket_path),))
        processes = dict(backend=psutil.Process(connection.info.backend_pid), gateway=psutil.Process(gateway.pid))
        explain(connection, choice=choice, rows=64)
        baseline = settled(processes)
        save(root/'baseline.json', baseline)
        for rows in (100, 1000, 4000):
            start = settled(processes)
            result, samples = observe(processes, lambda: explain(connection, choice=choice, rows=rows))
            ending = settled(processes)
            save(root/f'rows-{rows}.json', dict(rows=rows, input_bytes=rows*65536, start=start,
                                             ending=ending, result=result, samples=samples))
            check_resources(baseline, ending, samples)


def cancel_checks(args, connection, user):
    with fixture_gateway(args, user, 'cancel', choice=True, requests=12, delay_ms=300) as (root, socket_path, gateway):
        connection.execute("SELECT set_config('semloom_pg.gateway_socket',%s,false)", (str(socket_path),))
        processes = dict(backend=psutil.Process(connection.info.backend_pid), gateway=psutil.Process(gateway.pid))
        explain(connection, choice=True, rows=1)
        baseline = settled(processes)
        save(root/'baseline.json', baseline)
        for index in range(10):
            connection.execute("SET statement_timeout='50ms'")
            started = time.monotonic()
            def cancelled():
                try:
                    explain(connection, choice=True, rows=1)
                except psycopg.Error as error:
                    assert error.sqlstate == '57014'
                    return error.sqlstate
                raise AssertionError('expected cancellation')
            state, samples = observe(processes, cancelled)
            elapsed = time.monotonic() - started
            ending = settled(processes)
            save(root/f'cancel-{index}.json', dict(sqlstate=state, elapsed_seconds=elapsed, ending=ending, samples=samples))
            assert elapsed < 2
            check_resources(baseline, ending, samples)
        connection.execute("SET statement_timeout='5s'")
        explain(connection, choice=True, rows=1)
        ending = settled(processes)
        save(root/'recovery.json', ending)
        check_resources(baseline, ending, [])


def dns_checks(args, connection, user):
    with fixture_gateway(args, user, 'dns', choice=True, requests=1, timeout_ms=100, dns=True) as (root, socket_path, gateway):
        connection.execute("SELECT set_config('semloom_pg.gateway_socket',%s,false)", (str(socket_path),))
        processes = dict(backend=psutil.Process(connection.info.backend_pid), gateway=psutil.Process(gateway.pid))
        baseline = settled(processes)
        save(root/'baseline.json', baseline)
        for index in range(10):
            def timed_out():
                try:
                    explain(connection, choice=True, rows=1)
                except psycopg.Error as error:
                    assert error.sqlstate == '08006'
                    return error.sqlstate
                raise AssertionError('expected resolver timeout')
            state, samples = observe(processes, timed_out)
            ending = settled(processes)
            save(root/f'timeout-{index}.json', dict(sqlstate=state, ending=ending, samples=samples))
            check_resources(baseline, ending, samples, blocked_dns=True)
            assert not (root/'http.jsonl').exists()
        (root/'dns.release').touch(exist_ok=False)
        explain(connection, choice=True, rows=1)
        ending = settled(processes)
        save(root/'recovery.json', ending)
        check_resources(baseline, ending, [])
        events = [json.loads(line) for line in (root/'events.jsonl').read_text().splitlines()]
        assert sum(event['event']=='dns-enter' for event in events) == 1
        assert sum(event['event']=='dns-exit' for event in events) == 1
        assert len((root/'http.jsonl').read_text().splitlines()) == 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for field in ('repo', 'root', 'prefix'):
        parser.add_argument('--'+field, type=Path, required=True)
    args = parser.parse_args()
    assert subprocess.check_output([str(args.prefix/'bin/pg_config'), '--version'], text=True).strip() == 'PostgreSQL 18.3'
    args.root.mkdir()
    user = pwd.getpwnam('postgres')
    os.chown(args.root, user.pw_uid, user.pw_gid)
    with cluster(args.prefix, args.root, user) as connection:
        connection.execute('CREATE TABLE resource_rows(id int, payload text) '
                           'WITH (autovacuum_enabled=false, toast.autovacuum_enabled=false)')
        connection.execute("INSERT INTO resource_rows SELECT n, repeat('x',65536) FROM generate_series(1,4000) n")
        connection.execute('VACUUM ANALYZE resource_rows')
        connection.execute("SET semloom_pg.provider_execution_profile='openai-compatible-fixed'")
        connection.execute("SET statement_timeout='120s'")
        for choice in (False, True):
            normal_checks(args, connection, user, choice)
            print(('v4' if choice else 'v3') + ' normal resources passed', flush=True)
        cancel_checks(args, connection, user)
        print('cancel/recovery passed', flush=True)
        dns_checks(args, connection, user)
        print('blocked DNS/recovery passed', flush=True)
    save(args.root/'summary.json', dict(status='passed', real_model_requests=0,
         normal_rows_per_profile=5164, input_bytes_per_row=65536, cancellation_cycles=10,
         blocked_dns_cycles=10, sample_interval_seconds=SAMPLE_SECONDS))


if __name__ == '__main__':
    main()
