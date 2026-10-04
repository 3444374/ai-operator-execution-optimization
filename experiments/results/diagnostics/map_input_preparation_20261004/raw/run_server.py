"""Actual Ray/Daft/Arrow and finite loopback HTTP; no PG or model execution."""

import argparse
from dataclasses import asdict
import gzip
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.metadata
import json
import os
from pathlib import Path
import statistics
import threading
import time

from src.baselines.common.private_artifacts import new_private_directory
from src.baselines.common.redact import redact_json_values
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.completion_response import decode_backend_completion
from src.execution_provider.adapters.ray_map_transport import RayMapConfig, ray_map_factory
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.choice_gateway_observer import _guard_remote_request
from src.planning.work import StageWork, WorkDescriptor
from src.scheduling.core.session_contract import OfferedTask, SessionSpec, TaskInfo
from src.scheduling.runtime.stage_broker import StageBrokerLimits


CAPACITY, MAX_CALLS = 4, 2112
WORK = WorkDescriptor((StageWork('model', 1, 'work_units'),), 'model', 'server-echo-request-count')


def output_value(label, sequence, fixture):
    return f'{label}:{sequence}:{fixture}'


class Capture:
    def __init__(self):
        self.lock, self.events = threading.Lock(), []

    def __call__(self, value):
        with self.lock:
            if len(self.events) >= 8192:
                raise RuntimeError('finite observation capacity exceeded')
            self.events.append(dict(value, monotonic_ns=time.monotonic_ns()))

    def record(self, event, **fields):
        self(dict(event=event, **fields))


class Endpoint:
    def __init__(self):
        self.lock = threading.Lock()
        self.calls, self.active, self.peak, self.failures = [], 0, 0, []
        self.capture = None
        endpoint = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def log_message(self, *args):
                pass

            def do_POST(self):
                counted = False
                try:
                    if self.path != '/fixture' or not 0 < int(self.headers['Content-Length']) <= 1048576:
                        raise AssertionError('fixture route or body size differs')
                    body = self.rfile.read(int(self.headers['Content-Length']))
                    request = json.loads(body)
                    label, sequence = request['messages'][-1]['content'].split(':')
                    sequence = int(sequence)
                    if request['model'] != 'model' or label != 'row' or not 0 <= sequence < 128:
                        raise AssertionError('fixture semantic input differs')
                    with endpoint.lock:
                        if len(endpoint.calls) >= MAX_CALLS or endpoint.active >= CAPACITY:
                            raise AssertionError('finite fixture request capacity exceeded')
                        endpoint.active += 1
                        endpoint.peak = max(endpoint.peak, endpoint.active)
                        counted = True
                        capture = endpoint.capture
                        fields = dict(label=label, sequence=sequence,
                                      request_sha256=hashlib.sha256(body).hexdigest())
                        endpoint.calls.append(fields)
                        capture.record('loopback_http_started', active=endpoint.active, **fields)
                    time.sleep(.010 + sequence % 3 * .001)
                    with endpoint.lock:
                        endpoint.active -= 1
                        counted = False
                    capture.record('loopback_service_ended', **fields)
                    response = json.dumps(dict(model='model', choices=[dict(message=dict(
                        content=output_value(label, sequence, 'platform')), finish_reason='stop')],
                        usage=dict(prompt_tokens=2, completion_tokens=1))).encode()
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.send_header('Content-Length', str(len(response)))
                    self.end_headers()
                    self.wfile.write(response)
                    self.wfile.flush()
                    capture.record('loopback_http_finished', **fields)
                except BaseException as error:
                    endpoint.failures.append(type(error).__name__)
                    self.close_connection = True
                finally:
                    if counted:
                        with endpoint.lock:
                            endpoint.active -= 1

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, name='loopback-fixture')
        self.thread.start()
        self.config = FixedModelConfig(f'http://127.0.0.1:{self.server.server_port}/fixture', 'model', 5000)

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(3)
        if self.thread.is_alive() or self.active:
            raise AssertionError('loopback fixture did not stop')

def offered(sequence):
    body = json.dumps(dict(model='model', messages=[dict(role='system', content='fixture'),
        dict(role='user', content=f'row:{sequence}')], temperature=0, top_p=1, max_tokens=8,
        n=1, stream=False, stop=None), separators=(',', ':')).encode()
    return OfferedTask(sequence, body, 1, 1048576, info=TaskInfo('fixture', sequence, 'model', WORK))


def run_case(output, endpoint, address, backend, staged, rows):
    new_private_directory(output)
    capture = Capture()
    endpoint.capture = capture
    calls_before = len(endpoint.calls)
    ledger = CellBudgetLedger.create(output/'ledger.sqlite', AttemptBudget('fixture-http', rows),
                                    deadline_utc=time.time()+60)
    ledger.reserve_unit('case', rows)
    unit = ledger.claim_shared_unit('case')
    expected = {hashlib.sha256(offered(i).payload).hexdigest():i for i in range(rows)}
    counted = {}
    def observe(attempt, body):
        digest = hashlib.sha256(body).hexdigest()
        sequence = expected[digest]
        if sequence in counted or attempt != len(counted)+1:
            raise AssertionError('prepared request content or counting differs')
        counted[sequence] = attempt
        capture.record('fixture_counted', sequence=sequence, attempt=attempt, request_sha256=digest)
    physical = RayMapConfig(address, 1, 4, 2**21, 2**22, payload_backend=backend,
        preparation=StageBrokerLimits(2**21, 2**22, 128, 1, 128) if staged else None)
    case_started = time.monotonic_ns()
    execution = session = job = None
    report = dict(status='running', backend=backend, staged=staged, rows=rows,
                  model_requests=0, pg_queries=0, physical=asdict(physical))
    error = None
    try:
        execution = ray_map_factory(physical, lambda task:
            _guard_remote_request(task, unit, observe, capture))(
                endpoint.config, observer=capture, max_tasks=128, max_active_requests=4,
                input_bytes=128*1048576, result_bytes=128*1048576)
        job, session, _ = execution.open_job('fixture', SessionSpec('fixture', 'map', 'fixture'))
        prepared = tuple(offered(i) for i in range(rows))
        start = time.monotonic_ns()
        result = session.offer(prepared)
        assert result.accepted_prefix_count == rows
        session.seal()
        outputs, first = {}, None
        while len(outputs) < rows:
            if time.monotonic_ns()-case_started > 60*10**9:
                raise TimeoutError('finite case deadline reached')
            progress = execution.engine.advance()
            result = session.advance(128)
            assert not result.error and not progress.error
            usage = execution.engine.capacity.usage()
            assert usage.active_requests <= 4 and usage.active_work <= 4 and usage.held_tasks <= 128
            for delivery in result.deliveries:
                sequence = delivery.key.sequence
                assert sequence not in outputs
                completion=decode_backend_completion(delivery.result)
                assert completion.raw_output == output_value('row',sequence,'platform')
                assert completion.response_model_id == 'model' and completion.finish_reason == 'stop'
                outputs[sequence] = hashlib.sha256(delivery.result).hexdigest()
                if first is None:first = time.monotonic_ns()-start
            if result.deliveries:
                session.release([d.lease_id for d in result.deliveries])
            elif not progress.has_immediate_work:
                execution.engine.wake.wait(execution.engine.wake.generation,.01)
        end = time.monotonic_ns()
        http = endpoint.calls[calls_before:]
        assert len(http) == rows and len(counted) == rows and unit.attempts == rows
        assert {c['sequence'] for c in http} == set(range(rows))
        assert all(c['request_sha256'] in expected for c in http)
        assert not endpoint.failures and endpoint.active == 0 and endpoint.peak <= 4
        records = capture.events
        requests = [e for e in records if e['event']=='ray_http_completed']
        assert len(requests)==rows and {e['key']['sequence'] for e in requests}==set(range(rows))
        assert execution.engine.capacity.usage().held_tasks == 0
        prep = [e for e in records if e['event']=='ray_preparation_completed']
        if staged:
            assert sum(e['rows'] for e in prep)==rows and not any(e['failed'] for e in prep)
            assert all(e['preparation']['stages']['ready_held_bytes'] <= 2**22 for e in prep)
            assert all(e['preparation']['stages']['ready_held_work'] <= 128 for e in prep)
        report.update(status='passed', query_seconds=(end-start)/1e9, first_result_seconds=first/1e9,
            setup_seconds=(start-case_started)/1e9, http_requests=len(http), accounted_requests=unit.attempts,
            request_sha256=sorted(expected), result_sha256=outputs, exactly_once=True,
            object_batches=sum(e['event']=='ray_block_put' for e in records),
            object_peak_bytes=max(e.get('object_bytes',0) for e in records),
            preparation_payload_seconds=sum(e['payload_ns'] for e in prep)/1e9 if prep else None,
            preparation_put_seconds=sum(e['put_ns'] for e in prep)/1e9 if prep else None)
    except BaseException as failure:
        error = failure
        report.update(status='failed', error_type=type(failure).__name__,
                      actual_http_requests=len(endpoint.calls)-calls_before)
    finally:
        cleanup = []
        if execution is not None:
            try:
                if session is not None:session.close_consumer()
                if job is not None:execution.engine.close_job(job)
                deadline=time.monotonic()+10
                while True:
                    execution.engine.reap(8)
                    if execution.engine.capacity.usage().held_tasks==0 and execution.close():
                        break
                    if time.monotonic()>deadline:raise TimeoutError('fixture cleanup deadline reached')
                    time.sleep(.01)
                assert execution.engine.capacity.usage().held_tasks==0
            except BaseException as failure:
                cleanup.append(type(failure).__name__)
                error=error or failure
        try:
            ledger.close_shared_unit('case')
        except BaseException as failure:
            cleanup.append(type(failure).__name__);error=error or failure
        report.update(case_seconds=(time.monotonic_ns()-case_started)/1e9, cleanup_errors=cleanup)
        if error:report['status']='failed'
        (output/'result.json').write_text(json.dumps(redact_json_values(report),indent=2)+'\n')
        with gzip.open(output/'events.jsonl.gz','xt') as stream:
            for event in capture.events:stream.write(json.dumps(redact_json_values(event))+'\n')
    if error:raise RuntimeError('server fixture failed; evidence retained') from None
    return report


def main(output, ray_temp):
    import ray
    new_private_directory(output)
    cluster_started=time.monotonic()
    ray.init(num_cpus=4,num_gpus=0,object_store_memory=128*1024*1024,_temp_dir=str(ray_temp),
        include_dashboard=False,runtime_env={'env_vars':{'PYTHONPATH':os.environ['PYTHONPATH']}})
    address=ray.get_runtime_context().gcs_address
    cluster_seconds=time.monotonic()-cluster_started
    endpoint=Endpoint()
    cases=[]; started=time.monotonic(); error=None
    try:
        for phase, repeat, rows in [('check',0,16),('warmup',0,128),
                                    ('measurement',1,128),('measurement',2,128),('measurement',3,128)]:
            arms=[('daft',False),('daft',True),('arrow',False),('arrow',True)]
            if repeat % 2 == 0:arms.reverse()
            for backend,staged in arms:
                assert time.monotonic()-started < 600
                name=f'{phase}-{repeat}-{backend}-'+('staged' if staged else 'immediate')
                result=run_case(output/name,endpoint,address,backend,staged,rows)
                cases.append(dict(phase=phase,repeat=repeat,arm=f'{backend}-{staged}',case=name,result=result))
                print(json.dumps(dict(case=name,status=result['status'],http_requests=result['http_requests'],
                                      query_seconds=result['query_seconds'])),flush=True)
        assert len(endpoint.calls)==MAX_CALLS
    except BaseException as failure:error=failure
    finally:
        endpoint.close()
        ray.shutdown()
        summary=dict(status='failed' if error else 'passed',http_requests=len(endpoint.calls),model_requests=0,
            pg_queries=0,ray_startup_seconds=cluster_seconds,elapsed_seconds=time.monotonic()-started,cases=cases,
            versions={name:importlib.metadata.version(name) for name in ('ray','daft','pyarrow','httpx')},metrics={})
        if not error:
            for backend in ('daft','arrow'):
                for staged in (False,True):
                    arm=f'{backend}-{staged}'
                    summary['metrics'][arm]={}
                    for metric in ('query_seconds','first_result_seconds','case_seconds','object_batches'):
                        values=[c['result'][metric] for c in cases if c['phase']=='measurement' and c['arm']==arm]
                        summary['metrics'][arm][metric]=dict(values=values,median=statistics.median(values))
        summary['cleanup']=dict(http_thread_stopped=not endpoint.thread.is_alive(),http_active=endpoint.active,
                                ray_disconnected=not ray.is_initialized())
        (output/'summary.json').write_text(json.dumps(redact_json_values(summary),indent=2)+'\n')
    if error:raise RuntimeError('server suite stopped; evidence retained') from None


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--ray-temp',type=Path,required=True)
    args=parser.parse_args()
    main(args.output,args.ray_temp)
