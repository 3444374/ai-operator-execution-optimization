#!/usr/bin/env python3
"""Four actual Ray graphs for a fixed adapter repair; no model or shared Ray."""
from __future__ import annotations

import argparse
import hashlib
import inspect
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
from unittest.mock import patch


COMMIT = '1d6453d31b9e59123148d22c85f57086c8f7ccad'
NATIVE_SHA = 'ef7f7294a15e5463a8a8c6f89a9465e39f7f2c14cf80a7ba9952ed31955fa936'
CASES = ('off-eof', 'on-eof', 'off-early', 'on-early')
REPLY = b'{"fixture":true}'
CONTROLLER_TOKEN_ENV = 'SEMLOOM_RAY_CLEANUP_CONTROLLER_TOKEN'


def plan():
    return dict(source_commit=COMMIT, native_sha256=NATIVE_SHA, cases=list(CASES),
        prepared_rows_per_graph=2, maximum_fixture_posts=8, model_requests=0, gpu_requests=0,
        ray_cpus=4, object_store_bytes=512 * 1024 * 1024, no_progress_seconds=60,
        total_wall_deadline=None, existing_adapter_timeout_seconds=120,
        faults='after real iterator close and executor shutdown; ON also fails one writer record',
        early_stop='caller InterruptedError after first result; unknown responsibility remains',
        qualification='expected injected query failures, separately verified owner exit',
        post_eof_probe='same closed callbacks with ValueError primary; no new graph or POST',
        lease='existing host scope; wait without stale recovery or stopping another owner')


def identity(pid):
    try:
        raw = Path('/proc', str(pid), 'stat').read_text()
        fields = raw[raw.rfind(')') + 1:].split()
        return dict(pid=pid, start_ticks=fields[19], state=fields[0], ppid=int(fields[1]))
    except FileNotFoundError:
        return None
    except (OSError, IndexError, ValueError) as error:
        return dict(pid=pid, start_ticks=None, state='unknown', reason=type(error).__name__)


def matches(record):
    if record['start_ticks'] is None:
        return None
    current = identity(record['pid'])
    if current is not None and current['start_ticks'] is None:
        return None
    return bool(current and current['start_ticks'] == record['start_ticks'] and current['state'] != 'Z')


def record_error(error):
    return dict(type=type(error).__name__, notes=list(getattr(error, '__notes__', ())))


def require_case_controller(args):
    """An internal case must still belong to the live shared-lease controller."""
    token = os.environ.get(CONTROLLER_TOKEN_ENV)
    if args.artifact_root is None or not token:
        raise RuntimeError('internal case requires its shared-lease controller')
    lease_path = args.artifact_root / '.host-runner-scope' / '.runner-lease.json'
    try:
        lease = json.loads(lease_path.read_text())
    except (OSError, ValueError) as error:
        raise RuntimeError('internal case shared lease is unavailable') from error
    if not isinstance(lease, dict):
        raise RuntimeError('internal case shared lease is malformed')
    parent = identity(os.getppid())
    if (parent is None or parent['start_ticks'] is None or parent['state'] in ('Z', 'unknown')
            or lease.get('pid') != parent['pid']
            or lease.get('process_start_id') != parent['start_ticks']
            or lease.get('owner_token') != token
            or lease.get('repository_commit') != COMMIT
            or lease.get('config_fingerprint') != 'host-exclusive-experiment-runner-v1'):
        raise RuntimeError('internal case does not match its live shared-lease controller')


def case(args, write):
    require_case_controller(args)
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from src.baselines.text.frameworks import native_calls as native
    from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
    from src.experiments import execution_timing as timing_module
    from src.infrastructure.environment import write_json_atomic
    import ray

    root = args.output
    root.mkdir(mode=0o700)
    enabled, early = args.case.startswith('on-'), args.case.endswith('early')
    if enabled:
        os.environ['SEMLOOM_DETAILED_TIMING_DIR'] = str(root / 'timings')
    else:
        os.environ.pop('SEMLOOM_DETAILED_TIMING_DIR', None)
    state = dict(case=args.case, source_commit=COMMIT, owner=identity(os.getpid()),
        expected_injections=[], actions=[], receives=[], handler_threads=[], handler_errors=[], status='started')
    lock = threading.Lock()
    progress_lock = threading.Lock()

    def progress(event):
        with progress_lock:
            write_json_atomic(root / 'progress.json', dict(event=event, monotonic_ns=time.monotonic_ns()))

    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def log_message(self, *_):
            pass

        def do_POST(self):
            with lock:
                state['handler_threads'].append(threading.current_thread())
                size = int(self.headers.get('Content-Length', '0'))
                body = self.rfile.read(size) if 0 < size <= 1024 else b''
                row = json.loads(body) if body else {}
                valid = (self.path == '/fixture' and len(state['receives']) < 2 and
                         row.get('case') == args.case and row.get('row') in (0, 1) and
                         row['row'] not in [item['row'] for item in state['receives']])
                state['receives'].append(dict(row=row.get('row'), accepted=valid,
                    body_sha256=hashlib.sha256(body).hexdigest()))
            progress('fixture received')
            response = REPLY if valid else b'fixture rejected'
            self.send_response(200 if valid else 500)
            self.send_header('Content-Length', str(len(response)))
            self.send_header('Connection', 'close')
            self.end_headers()
            self.wfile.write(response)
            self.wfile.flush()

    class FixtureServer(ThreadingHTTPServer):
        def handle_error(self, request, client_address):
            with lock:
                state['handler_errors'].append(record_error(sys.exception()))
            super().handle_error(request, client_address)

    server = FixtureServer(('127.0.0.1', 0), Handler)
    server.daemon_threads = False
    http_thread = threading.Thread(target=server.serve_forever, name='cleanup-fixture-http')
    http_thread.start()
    original_cleanup = native._observed_graph_cleanup
    closed_callbacks = []
    close_error, shutdown_error = OSError('injected after iterator close'), RuntimeError('injected after shutdown')
    business = InterruptedError('injected caller early stop')
    timing = timing_module.get_timing() if enabled else None
    writer_failed = False
    writer_original = timing.writer.record if timing else None
    executor = None
    primary = None

    def writer(event):
        nonlocal writer_failed
        operation = 'native_call.graph_cleanup' if early else 'native_call.consume'
        event_name = 'span_started' if early else 'span_ended'
        if not writer_failed and event['operation'] == operation and event['event'] == event_name:
            writer_failed = True
            state['expected_injections'].append('writer')
            raise OSError('injected diagnostic writer record')
        return writer_original(event)

    def wrapped(stage, action, fault, *, probe=False):
        def invoke():
            item = dict(stage=stage, probe=probe, started_ns=time.monotonic_ns(), native_call_succeeded=False)
            state['actions'].append(item)
            progress(stage + ' start')
            try:
                action()
                item['native_call_succeeded'] = True
            except BaseException as error:
                item['native_error'] = record_error(error)
                raise
            finally:
                item['ended_ns'] = time.monotonic_ns()
                progress(stage + ' end')
            item['injected_error'] = type(fault).__name__
            raise fault
        return invoke

    def observed_cleanup(backend, actions):
        caller = inspect.currentframe().f_back
        actual_iterator, result = caller.f_locals.get('iterator'), caller.f_locals.get('result')
        public = result.iter_rows()  # Inert Iterable inspection; never iterated or submitted.
        state['iteration_types'] = dict(actual=type(actual_iterator).__name__,
            actual_module=type(actual_iterator).__module__, actual_is_generator=inspect.isgenerator(actual_iterator),
            actual_state_before_close=inspect.getgeneratorstate(actual_iterator),
            public=type(public).__name__, public_module=type(public).__module__,
            public_has_next=hasattr(public, '__next__'), public_has_close=hasattr(public, 'close'),
            public_probe_not_consumed=True)
        closed_callbacks.extend(actions)
        state['expected_injections'].extend(['iterator_close', 'executor_shutdown'])
        faults = {'iterator_close': close_error, 'executor_shutdown': shutdown_error}
        return original_cleanup(backend, [(stage, wrapped(stage, action, faults[stage])) for stage, action in actions])

    try:
        progress('ray init start')
        if ray.__version__ != '2.56.1' or ray.is_initialized():
            raise RuntimeError('requires a fresh existing Ray 2.56.1 driver')
        ray.init(address='local', num_cpus=4, num_gpus=0, include_dashboard=False,
            log_to_driver=False, _node_ip_address='127.0.0.1',
            _temp_dir=str(args.ray_temp_parent / args.case), object_store_memory=512 * 1024 * 1024)
        resources = ray.cluster_resources()
        state['runtime_resources'] = dict(cpu=resources.get('CPU', 0), gpu=resources.get('GPU', 0),
            affinity_slots=len(os.sched_getaffinity(0)))
        if state['runtime_resources'] != dict(cpu=4, gpu=0, affinity_slots=4):
            raise RuntimeError('actual isolated Ray resources differ from the approved plan')
        progress('ray init complete')
        options = NativeGraphOptions(concurrency=8, ray_actors=2, ray_async_batches_per_actor=2,
            ray_batch_rows=1, source_blocks=2)
        executor = native.NativeCallExecutor('ray-data', options)
        calls = tuple(native.NativeCall(args.case + ':' + str(i), i,
            json.dumps(dict(case=args.case, row=i)).encode(),
            'http://127.0.0.1:' + str(server.server_port) + '/fixture', (), 10) for i in range(2))
        seen = []
        with patch.object(native, '_observed_graph_cleanup', observed_cleanup):
            writer_context = patch.object(timing.writer, 'record', writer) if timing else patch.dict(os.environ, {})
            with writer_context:
                iterator = executor.iter_batch(calls, timeout_s=120)
                try:
                    for outcome in iterator:
                        if (outcome.transport_error or outcome.execution_error or outcome.response is None or
                                outcome.response.status_code != 200 or outcome.response.body != REPLY):
                            raise RuntimeError('actual fixture response failed validation')
                        seen.append((outcome.call_id, outcome.row_id))
                        progress('native result received')
                        if early:
                            iterator.throw(business)
                except BaseException as error:
                    primary = error
                finally:
                    iterator.close()
        state['query_error'] = record_error(primary) if primary else None
        expected = business if early else close_error
        if primary is not expected:
            raise RuntimeError('expected injected first error was not preserved') from primary
        required_notes = {'OSError', 'RuntimeError'} if early else {'RuntimeError'}
        if not required_notes <= set(note.rsplit(': ', 1)[-1] for note in primary.__notes__):
            raise RuntimeError('cleanup secondary notes were lost')
        if enabled and (not writer_failed or not any('Detailed timing' in note for note in primary.__notes__)):
            raise RuntimeError('writer fault was not retained with the real first error')
        if len(seen) != (1 if early else 2) or len(set(seen)) != len(seen):
            raise RuntimeError('unexpected result identity or EOF/early-stop consumption')
        if any(call_id != args.case + ':' + str(row) for call_id, row in seen):
            raise RuntimeError('result identity changed')
        if [stage for stage, _ in closed_callbacks] != ['iterator_close', 'executor_shutdown']:
            raise RuntimeError('both actual cleanup callbacks were not attempted')
        if not all(item['native_call_succeeded'] for item in state['actions']):
            raise RuntimeError('real cleanup failure cannot count as an expected injection')
        types = state['iteration_types']
        if not types['actual_is_generator'] or types['public_has_next'] or types['public_has_close']:
            raise RuntimeError('actual iterator versus public Iterable differs from pinned Ray')
        report = executor.last_close_report
        if (report['clean'] or report['reuse_allowed'] or not report['unconfirmed_work'] or
                not report['graph_consumer_stopped'] or report['remote_http_stopped'] != 'unavailable'):
            raise RuntimeError('injected query failure incorrectly confirms unknown responsibility')
        value_error = ValueError('injected business-first probe on closed actual callbacks')
        try:
            raise value_error
        except ValueError:
            original_cleanup('ray-data', [(stage, wrapped(stage, action,
                OSError('injected probe close') if stage == 'iterator_close' else RuntimeError('injected probe shutdown'),
                probe=True)) for stage, action in closed_callbacks])
        if not any('RuntimeError' in note for note in value_error.__notes__):
            raise RuntimeError('business-first probe lost actual shutdown secondary notes')
        if not all(item['native_call_succeeded'] for item in state['actions']):
            raise RuntimeError('real callback replay failure is not an expected injection')
        state.update(status='expected_fault_verified', results=seen, close_report=report,
            business_first_probe=record_error(value_error), options=vars(options))
    except BaseException as error:
        state.update(status='failed', unexpected_error=record_error(error))
        raise
    finally:
        cleanup_errors = []
        for stage, action in (('fixture_shutdown', server.shutdown), ('fixture_close', server.server_close),
                ('http_thread_join', lambda: http_thread.join(5)), ('ray_shutdown', ray.shutdown),
                ('timing_close', lambda: timing_module.close_timing())):
            progress(stage)
            try:
                action()
            except BaseException as error:
                cleanup_errors.append(dict(stage=stage, **record_error(error)))
        handlers = state.pop('handler_threads')
        state.update(fixture_posts=len(state['receives']), http_thread_alive=http_thread.is_alive(),
            live_handler_threads=sum(thread.is_alive() for thread in handlers),
            live_native_consumers=[thread.name for thread in threading.enumerate()
                                  if thread.name == 'native-call-consumer'],
            ray_disconnected=not ray.is_initialized(), cleanup_errors=cleanup_errors)
        state['owner_cleanup_confirmed'] = (not cleanup_errors and not state['http_thread_alive'] and
            not state['live_handler_threads'] and not state['live_native_consumers'] and state['ray_disconnected'])
        valid_receives = (1 <= state['fixture_posts'] <= 2 and all(item['accepted'] for item in state['receives'])
                          and not state['handler_errors'])
        if not state['owner_cleanup_confirmed'] or not valid_receives:
            state['status'] = 'failed'
        write(root / 'result.json', state)
        progress('case finished')
        if state['status'] != 'expected_fault_verified':
            raise RuntimeError('actual software case failed; evidence retained')


def execute(args, write):
    import psutil
    from src.infrastructure.runner_lease import acquire_runner_lease, HOST_SCOPE_DIR_NAME, HOST_SCOPE_FINGERPRINT
    from src.infrastructure.environment import write_json_atomic
    root = args.output
    root.mkdir(mode=0o700)
    owner = identity(os.getpid())
    if owner is None or owner['start_ticks'] is None:
        write(root / 'controller-identity-failure.json', dict(status='failed', owner=owner))
        raise RuntimeError('controller birth identity is unconfirmed; no lease or child started')
    write(root / 'controller.json', dict(owner=owner, source_commit=COMMIT, plan=plan()))
    lease_path = args.artifact_root / HOST_SCOPE_DIR_NAME / '.runner-lease.json'
    while True:
        if lease_path.exists():
            write_json_atomic(root / 'queue.json', dict(status='waiting for shared lease', epoch_s=time.time()))
            time.sleep(15)
            continue
        try:
            lease = acquire_runner_lease(lease_path.parent, config_fingerprint=HOST_SCOPE_FINGERPRINT,
                                        repository_commit=COMMIT, recover_stale=False)
            break
        except RuntimeError:
            if not lease_path.exists():
                raise
    write(root / 'lease-acquired.json', dict(owner=owner, acquired=True, recovered=False))
    cases, owned, forced, process_cleanup_errors = [], {}, [], []
    failure = None

    def collect(process):
        if matches(process.birth_identity) is not True:
            return
        try:
            parent = psutil.Process(process.pid)
            if matches(process.birth_identity) is not True:
                return
            children = [parent] + parent.children(recursive=True)
        except psutil.NoSuchProcess:
            children = []
        if matches(process.birth_identity) is not True:
            return
        for child in children:
            if child.pid == process.pid:
                continue  # The original root birth is already registered; never replace it.
            item = identity(child.pid)
            if item and child.is_running():
                owned[(child.pid, item['start_ticks'])] = item

    def stop_owned():
        for sig in (signal.SIGTERM, signal.SIGKILL):
            for item in list(owned.values()):
                if matches(item):
                    try:
                        descriptor = os.pidfd_open(item['pid'])
                    except ProcessLookupError:
                        continue
                    except OSError as error:
                        process_cleanup_errors.append(dict(stage='pidfd_open', owner=item, **record_error(error)))
                        continue
                    try:
                        if matches(item):
                            try:
                                signal.pidfd_send_signal(descriptor, sig)
                                forced.append(dict(**item, signal=int(sig)))
                            except ProcessLookupError:
                                pass  # This birth disappeared; the fd cannot target a reused PID.
                            except OSError as error:
                                process_cleanup_errors.append(dict(stage='pidfd_signal', owner=item, **record_error(error)))
                    finally:
                        os.close(descriptor)
            time.sleep(2)

    try:
        for name in CASES:
            case_root = root / name
            command = [sys.executable, '-u', '-B', str(Path(__file__).resolve()), '--case', name,
                '--source-root', str(args.source_root), '--source-manifest', str(args.source_manifest),
                '--output', str(case_root), '--ray-temp-parent', str(args.ray_temp_parent),
                '--artifact-root', str(args.artifact_root)]
            with (root / (name + '.log')).open('xb') as log:
                process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                    env={**{key: os.environ[key] for key in ('PATH', 'LANG', 'LC_ALL') if key in os.environ},
                         'PYTHONPATH': str(args.source_root / 'code'), 'CUDA_VISIBLE_DEVICES': '',
                         'HF_HUB_OFFLINE': '1', 'TRANSFORMERS_OFFLINE': '1',
                         'RAY_USAGE_STATS_ENABLED': '0', 'PYTHONDONTWRITEBYTECODE': '1',
                         CONTROLLER_TOKEN_ENV: lease.owner.owner_token,
                         'RAY_DATA_DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY': '2'}, start_new_session=True)
                process.birth_identity = identity(process.pid)
                if process.birth_identity is None:
                    owned[(process.pid, None)] = dict(pid=process.pid, start_ticks=None, state='unknown',
                        reason='spawn succeeded but original birth identity was not captured')
                    raise RuntimeError('owned child birth identity was not captured')
                owned[(process.pid, process.birth_identity['start_ticks'])] = process.birth_identity
                if process.birth_identity['start_ticks'] is None:
                    raise RuntimeError('owned child birth identity is unreadable')
                started = time.monotonic_ns()
                while process.poll() is None:
                    collect(process)
                    progress = case_root / 'progress.json'
                    last_progress = json.loads(progress.read_text())['monotonic_ns'] if progress.exists() else started
                    idle = (time.monotonic_ns() - last_progress) / 1e9
                    if idle < 0:
                        raise RuntimeError('same-host monotonic progress clock differs')
                    if idle > 60:
                        stop_owned()
                        process.wait(timeout=10)
                        raise TimeoutError('owned case made no progress for 60 seconds')
                    time.sleep(.2)
                collect(process)
                result_path = case_root / 'result.json'
                result = json.loads(result_path.read_text()) if result_path.exists() else {}
                cases.append(dict(case=name, exit_code=process.returncode, result=result))
                if process.returncode != 0 or result.get('status') != 'expected_fault_verified':
                    raise RuntimeError('actual Ray case failed: ' + name)
                if sum(item['result']['fixture_posts'] for item in cases) > 8:
                    raise RuntimeError('protocol fixture request limit exceeded')
        deadline = time.monotonic() + 10
        while any(matches(item) is not False for item in owned.values()) and time.monotonic() < deadline:
            time.sleep(.2)
        if any(matches(item) is not False for item in owned.values()):
            stop_owned()
            raise RuntimeError('owned non-zombie runtime survived; force cleanup is not a pass')
    except BaseException as error:
        failure = record_error(error)
        stop_owned()
    finally:
        live = [item for item in owned.values() if matches(item) is not False]
        lease_released = False
        if not live:
            try:
                lease.release()
                lease_released = True
            except BaseException as error:
                process_cleanup_errors.append(dict(stage='lease_release', **record_error(error)))
        summary = dict(status='passed' if not failure and not live and not forced and not process_cleanup_errors else 'failed',
            source_commit=COMMIT, plan=plan(), controller=owner, cases=cases,
            fixture_posts=sum(item.get('result', {}).get('fixture_posts', 0) for item in cases),
            model_requests=0, gpu_requests=0, owned_birth_identities=list(owned.values()),
            live_owned_non_zombies=live, forced=forced, failure=failure,
            process_cleanup_errors=process_cleanup_errors, lease_released=lease_released,
            controller_exit_requires_independent_check=True)
        write(root / 'summary.json', summary)
        if summary['status'] != 'passed':
            raise RuntimeError('software protocol failed; retained evidence identifies the action')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument('--execute', action='store_true')
    modes.add_argument('--case', choices=CASES)
    parser.add_argument('--source-root', type=Path)
    parser.add_argument('--source-manifest', type=Path)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--artifact-root', type=Path)
    parser.add_argument('--ray-temp-parent', type=Path)
    args = parser.parse_args()
    if not args.execute and not args.case:
        print(json.dumps(plan(), indent=2))
        return
    if args.case:
        try:
            require_case_controller(args)
        except RuntimeError as error:
            parser.error(str(error))
    if sys.platform != 'linux' or not all((args.source_root, args.source_manifest, args.output, args.ray_temp_parent)):
        parser.error('execution needs Linux and explicit isolated source/output/temp paths')
    if args.execute and args.artifact_root is None:
        parser.error('--execute requires the existing shared artifact root')
    def stop_signal(_number, _frame):
        raise InterruptedError('owned software probe stop signal')
    signal.signal(signal.SIGTERM, stop_signal)
    signal.signal(signal.SIGINT, stop_signal)
    sys.path.insert(0, str(args.source_root / 'code'))
    from src.baselines.common.private_artifacts import write_private_json, require_outside_git
    require_outside_git(args.output)
    manifest = json.loads(args.source_manifest.read_text())
    if manifest['source_commit'] != COMMIT:
        raise RuntimeError('source commit differs from the fixed qualification')
    for rel, expected in manifest['files'].items():
        path = args.source_root / rel
        if not path.resolve().is_relative_to(args.source_root.resolve()) or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise RuntimeError('source bundle differs: ' + rel)
    if hashlib.sha256((args.source_root / 'code/src/baselines/text/frameworks/native_calls.py').read_bytes()).hexdigest() != NATIVE_SHA:
        raise RuntimeError('adapter source differs from reviewed fixed commit')
    os.sched_setaffinity(0, sorted(os.sched_getaffinity(0))[:4])
    if len(os.sched_getaffinity(0)) != 4:
        raise RuntimeError('exactly four local CPU affinity slots required')
    from importlib.metadata import version
    if version('ray') != '2.56.1' or version('pyarrow') != '24.0.0':
        raise RuntimeError('reuse the existing pinned Ray/Arrow environment')
    if args.case:
        case(args, write_private_json)
    else:
        execute(args, write_private_json)


if __name__ == '__main__':
    main()
