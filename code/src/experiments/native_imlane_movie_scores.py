"""One original Q9 DOUBLE Map query under an externally supervised host lease."""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import threading
import time
import traceback
from types import SimpleNamespace

import psutil

from integrations.imlane_duckdb.model_provider import configuration as provider_configuration
from integrations.imlane_duckdb.model_service import ModelService
from integrations.imlane_records import owner
from src.experiments.native_imlane_model_query import QueryWindow, READY_SCHEMA
from src.infrastructure.redact import redact_text
from src.infrastructure.private_artifacts import content_digest, file_identity, file_sha256, json_metric, new_private_directory, write_private_json

from src.experiments.native_imlane_relation import run_relation_query, finish_movie_scores, persist_relation_observation

from src.baselines.text import sembench_movie_q9 as q9_binding

CODE = Path(__file__).resolve().parents[2]
KERNEL = CODE / 'integrations/imlane_duckdb/q9_udf.py'
ORIGINAL_DSO_SHA256 = '52bf5763aab6f35b76746a9fc15b75e27bac1534315d1a20926eb4a78c2badaf'


def read(path):
    return json.loads(Path(path).read_text())


def identity(service, mode):
    result = dict(api_pid=service.process.pid, api_process_created=service.created,
        ready_sha256=file_sha256(service.ready_path), ready=read(service.ready_path),
        native_engine_instances=service.ready_info['engine_instances'])
    if mode == 'model':
        lines = subprocess.check_output(['nvidia-smi', '--query-compute-apps=pid,gpu_uuid,used_memory',
            '--format=csv,noheader,nounits'], text=True, timeout=10).strip().splitlines()
        gpu = []
        for line in lines:
            pid, gpu_uuid, memory = [field.strip() for field in line.split(',')]
            process = psutil.Process(int(pid))
            assert service.process.pid in [parent.pid for parent in process.parents()]
            gpu.append(dict(pid=int(pid), create_time=process.create_time(), gpu_uuid=gpu_uuid, memory_mib=memory))
        assert len(gpu) == 1
        result['gpu_model_identity'] = [{k: item[k] for k in ('pid', 'create_time', 'gpu_uuid')} for item in gpu]
        result['gpu_sample'] = gpu
    else:
        result['gpu_model_identity'] = []
    return result


def audit_ipc(directory, rows):
    directory = Path(directory)
    provider = read(directory / 'model-provider.json')
    starts = {p.name.removesuffix('.started.json') for p in (directory / 'model-calls').glob('*.started.json')}
    ends = {p.name.removesuffix('.terminal.json') for p in (directory / 'model-calls').glob('*.terminal.json')}
    assert starts == ends and starts
    batches, calls, ids = {}, [], []
    for name in sorted(ends):
        batch = read(directory / 'model-calls' / (name + '.terminal.json'))
        assert batch['status'] == 'succeeded' and batch['client_closed'] is True
        request, response = batch['request'], batch['response']
        assert content_digest(request) == batch['request_sha256'] and content_digest(response) == batch['response_sha256']
        assert request['batch_id'] == response['batch_id'] == batch['batch_id']
        assert request['sampling_params'] == dict(provider['sampling_params'], n=1)
        assert len(batch['calls']) == batch['engine_generate_invocations'] == len(response['calls'])
        for position, item in enumerate(batch['calls']):
            assert item['position'] == position and item['attempted'] and item['status'] == 'succeeded'
            assert item['response'] == response['calls'][position]
            assert item['response']['finish_reason'] in ('stop', 'length')
            assert item['response']['requested_max_tokens'] == item['response']['resolved_max_tokens'] == 8192
            assert item['messages_sha256'] == content_digest(request['messages_batch'][position])
            assert item['response_id'] == item['response']['request_id']
            ids.append(item['response_id'])
            calls.append(item)
        batches[name] = batch
    for path in (directory / 'udf-receipts').glob('q9-batch-*.json'):
        udf = read(path)
        assert udf['status'] == 'succeeded'
        invocation, = udf['provider_calls']
        matches = [name for name, batch in batches.items() if batch['worker_pid'] == udf['worker_pid']
            and invocation['started_monotonic_ns'] <= batch['started_monotonic_ns'] <= batch['finished_monotonic_ns'] <= invocation['finished_monotonic_ns']]
        assert len(matches) == 1
        batch = batches.pop(matches[0])
        for source, prediction, item in zip(udf['sources'], udf['predictions'], batch['calls'], strict=True):
            assert source['messages_sha256'] == prediction['messages_sha256'] == item['messages_sha256']
            assert prediction['generated_text'] == item['response']['generated_text']
    assert not batches and len(calls) == len(ids) == len(set(ids)) == len(rows) == 256
    return dict(calls=calls, batch_rpc_invocations=len(ends), generate_API_calls=len(calls),
        unique_request_ids=len(set(ids)), unknown=0, retries=0,
        count_scope='Public AsyncLLM.generate invocation receipts; native add_request/kernel counts unobserved')


def verified_build(path):
    record = read(path)
    if (record['schema'] != 'semloom.imlane.movie_scores_build.v1'
            or record['status'] != 'built'
            or record['duckdb_library']['sha256'] != ORIGINAL_DSO_SHA256):
        raise ValueError('Q9 requires a successful build against the original qualified DOUBLE DSO')
    for label, filename in (('driver_source', 'q9.cpp'), ('kernel', 'q9_udf.py'),
                            ('relation_handoff', 'relation_handoff.hpp')):
        if file_identity(KERNEL.parent / filename) != record[label]:
            raise ValueError('Q9 build source changed: ' + filename)
    for label in ('binary', 'duckdb_library'):
        if file_identity(Path(record[label + '_path'])) != record[label]:
            raise ValueError('Q9 build artifact changed: ' + label)
    return record


def run(config_path, output, *, service_factory=ModelService):
    config, output = read(config_path), Path(output)
    identity_lock = read(config['input_identity'])
    build = verified_build(Path(config['build_record']))
    config['driver'] = build['binary_path']
    assert file_sha256(Path(config['data_dir']) / 'Reviews.csv') == identity_lock['Reviews_csv_sha256']
    descriptor = dict(config['request_budget'])
    if descriptor['limit'] != 256:
        raise ValueError('Q9 requires exactly one permanently bounded 256-call unit')
    from integrations.imlane_duckdb.model_ipc_worker import request_budget
    if request_budget(descriptor).remaining != 256:
        raise ValueError('Q9 requires a fresh claimed unit; continuation/retry forbidden')
    config['model_service'] = dict(config['model_service'], request_budget=descriptor)
    new_private_directory(output)
    window = QueryWindow(config['query_seconds'], config['cleanup_seconds'], 8,
        cleanup_path=output / 'cleanup-start.json')
    stop = owner.StopBudget(threading.Event())
    previous = {number: signal.signal(number, stop.signal) for number in (signal.SIGTERM, signal.SIGINT)}
    report = dict(status='failed', baseline_name='Author DuckDB + IMLane native',
        semantic_operator='Movie Q9 Map / numeric prediction projection',
        execution_backend='author DBEnd/RegisterFunction/THREAD_SCHEDULE_PREDICTION/CPython',
        started_monotonic_ns=time.monotonic_ns(), rounds=[], warmup=0, retries=0,
        result_cache_enabled=False, query_state_lifecycle='one cold author coordinator, DuckDB query and native model service')
    service = None
    primary = None
    try:
        phase = 'cold'
        directory = output / phase
        directory.mkdir(mode=0o700)
        started = time.monotonic_ns()
        query, operation, calls, rows = q9_binding.prepare(config, directory / 'input', config['run_id'] + '-' + phase)
        assert [content_digest(json.loads(call['messages_json'])) for call in calls] == identity_lock['messages_sha256']
        service = service_factory(config['model_service'], output / 'model-service', environment=dict(os.environ),
                deadline=window.query_deadline, cleanup_deadline=window.begin_cleanup, stop_budget=stop, stop_grace_s=8)
        service.__enter__()
        service.check()
        before_identity = identity(service, config['provider_mode'])
        for name in ('udf-receipts', 'model-calls'):
            (directory / name).mkdir(mode=0o700)
        provider = provider_configuration(dict(schema='semloom.imlane.duckdb.q3.model_provider.v2',
            provider_mode=config['provider_mode'], socket_path=str(service.socket_path), model=service.served_model_name,
            sampling_params=config['sampling_params'], request_timeout_s=config['request_timeout_s'],
            deadline_monotonic=window.query_deadline, receipt_dir=str(directory / 'model-calls')))
        write_private_json(directory / 'model-provider.json', provider)
        write_private_json(directory / 'udf.json', dict(provider_path=str(CODE / 'integrations/imlane_duckdb/model_provider.py'),
            provider_function='infer_batch', provider_mode=config['provider_mode'], receipt_dir=str(directory / 'udf-receipts'),
            messages_path=str(directory / 'input/messages.json'), messages_sha256=file_sha256(directory / 'input/messages.json')))
        args = SimpleNamespace(output=directory, author_root=config['author_root'], method_python=config['method_python'],
            executor_count=None, scopes=[], stop_budget=stop, startup_timeout_s=window.query_deadline-time.monotonic(),
            workload_environment=dict(SEMLOOM_IMLANE_Q9_CONFIG=str(directory / 'udf.json'),
                SEMLOOM_IMLANE_Q3_MODEL_CONFIG=str(directory / 'model-provider.json')))
        native_report = dict(cleanup=[])
        command = [str(Path(config['driver'])), '--input', str(directory / 'input/input.csv'), '--module', str(KERNEL),
            '--function', 'infer_q9', '--kind', 'thread', '--output', str(directory / 'query.json'),
            '--sink-release', str(directory / 'sink-release.ack')]
        frame, failure = None, None
        try:
            frame, audited = run_relation_query(args, native_report, kernel=KERNEL, command=command,
                window=window, query_started_ns=started,
                sink=lambda native: finish_movie_scores(native, query, rows),
                audit=lambda native, frame: q9_binding.finish(query, operation, calls, rows, native, directory / 'udf-receipts'),
                check=service.check)
        except BaseException as error:
            failure = error
            frame = getattr(error, 'query_result', frame)
            raise
        finally:
            persist_relation_observation(directory, native_report, frame, primary=failure)
        audited_frame, result = audited
        assert frame.equals(audited_frame)
        native = read(directory / 'query.json')
        ready = native_report['result_ready_monotonic_ns']
        write_private_json(directory / 'result-ready.json', dict(result=result, result_ready_monotonic_ns=ready))
        evaluation_start = time.monotonic_ns()
        quality = json_metric(query.evaluate(frame))
        evaluation_end = time.monotonic_ns()
        ipc = audit_ipc(directory, rows)
        after_identity = identity(service, config['provider_mode'])
        assert all(after_identity[key] == before_identity[key] for key in
            ('api_pid', 'api_process_created', 'ready_sha256', 'gpu_model_identity'))
        current = dict(phase=phase, started_ns=started, result_ready_ns=ready,
            complete_query_seconds=(ready-started)/1e9, native_projection_seconds=native['native_projection_materialized_seconds'],
            native_result_ready_ns=native['result_ready_monotonic_ns'],
            evaluation_seconds=(evaluation_end-evaluation_start)/1e9, quality=quality,
            native=native, ipc=ipc, model_identity=after_identity,
            result_sha256=file_sha256(directory / 'result-ready.json'),
            messages_sha256=read(directory / 'input/prepared.json')['messages_sha256'],
            barrier='memory sink before ACK; driver exit, receipt audit and author close required for success')
        current.update(service_start_to_result_seconds=ready/1e9-service.report['started_monotonic'],
                model_ready_to_result_seconds=ready/1e9-service.report['ready_monotonic'],
                service_startup_seconds=service.report['ready_monotonic']-service.report['started_monotonic'])
        report['rounds'].append(current)
        write_private_json(directory / 'AUDIT.json', current)
        all_ids = [item['response_id'] for current in report['rounds'] for item in current['ipc']['calls']]
        assert len(all_ids) == len(set(all_ids)) == 256
        last_ready = report['rounds'][-1]['result_ready_ns']
        write_private_json(output / 'result-ready.json', dict(schema=READY_SCHEMA,
            result_ready_monotonic_ns=last_ready, clock='CLOCK_MONOTONIC'))
        window.accept_ready(last_ready, clock='CLOCK_MONOTONIC')
        report.update(status='completed', model_calls=256, unknown=0, same_model=True)
    except BaseException as error:
        primary = error
        report.update(error_type=type(error).__name__, error=redact_text(str(error)), traceback=redact_text(traceback.format_exc()))
        raise
    finally:
        close_start = time.monotonic_ns()
        window.begin_cleanup()
        if service is not None:
            try:
                with stop.cleanup():
                    service.close(deadline=window.cleanup_deadline)
            except BaseException as error:
                report.update(status='failed_cleanup', cleanup_error=dict(type=type(error).__name__, message=redact_text(str(error))))
                if primary is None:
                    primary = error
            report['service_close'] = service.report
        report.update(model_close_seconds=(time.monotonic_ns()-close_start)/1e9,
            ended_monotonic_ns=time.monotonic_ns(), deadline=window.report())
        for number, handler in previous.items():
            signal.signal(number, handler)
        write_private_json(output / 'worker.json', report)
        if primary is not None:
            raise primary
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    run(args.config, args.output)


if __name__ == '__main__':
    main()
