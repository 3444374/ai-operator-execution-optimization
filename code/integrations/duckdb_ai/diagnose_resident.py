#!/usr/bin/env python3
"""CPU-only real-library diagnosis of the existing DuckDB supplier path."""
import argparse
from collections import Counter, defaultdict
from contextlib import ExitStack, contextmanager
import ctypes
import hashlib
import json
import os
from pathlib import Path
import sys
import threading
import time
from types import SimpleNamespace
from unittest.mock import patch

CODE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(CODE))
from src.baselines.common.redact import redact_text
from src.execution_provider.adapters import native_tasks
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.ray_map_transport import RayMapConfig, owned_map_worker_pool
from src.experiments.buffered_events import BufferedEvents
from src.experiments.postgresql.supplier_adapter_query import (
    MethodObservations, _duckdb_rows, prepare_duckdb_connection, replace_duckdb_inputs,
)
from src.semantic_methods import duckdb_ai
from tests.semantic_methods.test_duckdb_ai import _FixtureServer, _FixtureHandler, response_body


class ResidentHandler(_FixtureHandler):
    def do_POST(self):
        raw = self.rfile.read(int(self.headers['Content-Length']))
        value = json.loads(raw)
        self.server.request_events.append((time.monotonic_ns(), 1))
        with self.server.lock:
            self.server.records.append((raw, self.headers.get('Authorization')))
            self.server.active += 1
            self.server.peak = max(self.server.peak, self.server.active)
        try:
            time.sleep(0.04)
            body = response_body(value['messages'][-1]['content'])
            # End service work before the short response write; client receipt is separate.
            self.server.request_events.append((time.monotonic_ns(), -1))
            self.send_response(200)
            self.send_header('Content-Length', str(len(body)))
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(body)
        finally:
            with self.server.lock:
                self.server.active -= 1


@contextmanager
def instrument(stats):
    def timed(name, function):
        def call(*args, **kwargs):
            started, cpu = time.perf_counter_ns(), time.thread_time_ns()
            try:
                return function(*args, **kwargs)
            finally:
                stats['counts'][name] += 1
                stats['wall_ns'][name] += time.perf_counter_ns() - started
                stats['cpu_ns'][name] += time.thread_time_ns() - cpu
                stats['threads'][name].add(threading.get_native_id())
        return call

    def dispatch(self, batch_id, calls, count, responses, cancelled, consume, context):
        stats['native_callback_calls'] += count
        stats['cpp_prepare_span_ns'] += max(0, calls[count-1].ready_ns - calls[0].ready_ns)
        measured = duckdb_ai._Consume(timed('native_consume', consume))
        return original_dispatch(self, batch_id, calls, count, responses, cancelled, measured, context)

    def offer(self, tasks):
        stats['offered_items'] += len(tasks)
        result = original_offer(self, tasks)
        stats['accepted_items'] += result.accepted_prefix_count
        stats['offer_zero'] += int(result.accepted_prefix_count == 0)
        stats['core_request_peak'] = max(stats['core_request_peak'], self.execution.engine.capacity.usage().active_requests)
        return result

    def release(self, leases):
        stats['release_group_sizes'].append(len(leases))
        return original_release(self, leases)

    original_dispatch = duckdb_ai.DuckDBSemLoomBridge._dispatch
    original_offer = native_tasks.NativeTaskSession.offer
    original_release = native_tasks.NativeTaskSession.release
    with ExitStack() as stack:
        stack.enter_context(patch.object(native_tasks, 'prepare_native_task',
            timed('prepare_native_task', native_tasks.prepare_native_task)))
        stack.enter_context(patch.object(duckdb_ai.DuckDBSemLoomBridge, '_dispatch', dispatch))
        stack.enter_context(patch.object(native_tasks.NativeTaskSession, 'offer', timed('offer', offer)))
        stack.enter_context(patch.object(native_tasks.NativeTaskSession, 'release', timed('release', release)))
        for name in ('advance', 'wait'):
            function = getattr(native_tasks.NativeTaskSession, name)
            stack.enter_context(patch.object(native_tasks.NativeTaskSession, name, timed(name, function)))
        for name in ('ready', 'received'):
            function = getattr(MethodObservations, name)
            stack.enter_context(patch.object(MethodObservations, name, timed('observation_' + name, function)))
        from src.execution_provider.adapters import full_response
        stack.enter_context(patch.object(full_response, 'decode_full_response',
            timed('decode_full_response', full_response.decode_full_response)))
        yield


def occupancy(events, start, end, count):
    active = completed = peak = 0
    cursor = start
    area = zero = partial = 0
    for when, delta in sorted(events):
        when = min(end, max(start, when))
        duration = max(0, when - cursor)
        area += active * duration
        if completed < count:
            zero += duration if active == 0 else 0
            partial += duration if 0 < active < 4 else 0
        active += delta
        completed += int(delta < 0)
        peak = max(peak, active)
        cursor = when
    duration = end - cursor
    area += active * duration
    if completed < count:
        zero += duration if active == 0 else 0
    return dict(peak=peak, mean=area / (end - start), necessary_zero_seconds=zero / 1e9,
                necessary_partial_seconds=partial / 1e9)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--extension', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--ray-temp', type=Path, required=True)
    parser.add_argument('--arms', default='native,local,ray')
    parser.add_argument('--assert-single-preparation', action='store_true')
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    os.environ['CUDA_VISIBLE_DEVICES'] = ''
    os.sched_setaffinity(0, set(range(48, 56)))
    import ray
    context = ray.init(num_cpus=8, num_gpus=0, include_dashboard=False,
                       _temp_dir=str(args.ray_temp), object_store_memory=256 * 1024 * 1024)
    http = _FixtureServer()
    http.RequestHandlerClass = ResidentHandler
    http.request_events = []
    # Instrument the actual deterministic server without replacing native SQL or transport.
    events = []
    events = http.request_events
    server_thread = threading.Thread(target=http.serve_forever, daemon=True)
    server_thread.start()
    config = FixedModelConfig(f'http://127.0.0.1:{http.server_port}/v1/chat/completions',
                             'fixture', 5000, bearer_token='EMPTY')
    plan = SimpleNamespace(max_tokens=7, instruction='Preserve the supplied text exactly.')
    options = SimpleNamespace(concurrency=4)
    summary = dict(base='3a5f7bc9', gpu_count=0, cpus=sorted(os.sched_getaffinity(0)),
                       concurrency=4, fixture_shape=[8, 8, 128], queries=[], model_requests=0)
    summary['occupancy_scope'] = 'fixture request body read through response body ready; response write/client receipt excluded'
    try:
        for arm in args.arms.split(','):
            with ExitStack() as owner:
                connection = prepare_duckdb_connection(owner, plan, config, options, args.extension)
                execution = None
                if arm != 'native':
                    physical = None
                    if arm == 'ray':
                        physical = RayMapConfig(context.address_info['address'], workers=1, batch_rows=4,
                            window_bytes=2*1024*1024, object_bytes=4*1024*1024,
                            worker_pool='duckdb-diagnosis', response_mode='full')
                        owner.enter_context(owned_map_worker_pool(ray, config, 4, physical))
                    execution = native_tasks.build_native_execution(config, physical=physical,
                        max_tasks=4, max_active_requests=4)
                    owner.callback(lambda: execution.close() or (_ for _ in ()).throw(RuntimeError('unsettled execution')))
                connection_id = id(connection)
                for ordinal, count in enumerate((8, 8, 128)):
                    stats = dict(counts=Counter(), wall_ns=Counter(), cpu_ns=Counter(), threads=defaultdict(set),
                        native_callback_calls=0, cpp_prepare_span_ns=0, offered_items=0, accepted_items=0,
                        offer_zero=0, release_group_sizes=[], core_request_peak=0)
                    values = [dict(row_id=f'r{i:04}', text=f'q{ordinal}/row-{i}') for i in range(count)]
                    replace_duckdb_inputs(connection, values, plan)
                    input_digest = hashlib.sha256(json.dumps(values, sort_keys=True).encode()).hexdigest()
                    record_offset, event_offset = len(http.records), len(events)
                    unit = args.output / f'{arm}-{ordinal}'
                    unit.mkdir()
                    with ExitStack() as query:
                        writer = query.enter_context(BufferedEvents(unit/'events.jsonl'))
                        raw = query.enter_context((unit/'raw.jsonl').open('x'))
                        observations = MethodObservations(writer, raw, f'{arm}-{ordinal}')
                        query.enter_context(instrument(stats))
                        role = 'duckdb-adapted-native' if arm == 'native' else 'duckdb-method-semloom'
                        execute, identity = query.enter_context(_duckdb_rows(query, role, values, plan, config,
                            execution, observations, options, args.extension, connection=connection))
                        before_threads = len(os.listdir('/proc/self/task'))
                        start, cpu = time.monotonic_ns(), time.process_time_ns()
                        rows = list(execute())
                        end, process_cpu = time.monotonic_ns(), time.process_time_ns()-cpu
                    requests = http.records[record_offset:]
                    assert len(rows) == len(requests) == count
                    assert all('q' + str(ordinal) + '/row-' in text for _, text in rows)
                    assert len(set(row for row, _ in rows)) == count
                    assert execution is None or (not execution.engine.capacity.records and not execution.engine.jobs.jobs)
                    entry = dict(arm=arm, ordinal=ordinal, rows=count, connection_id=connection_id,
                        input_sha256=input_digest, wall_seconds=(end-start)/1e9, process_cpu_seconds=process_cpu/1e9,
                        threads_before=before_threads, threads_after=len(os.listdir('/proc/self/task')),
                        service=occupancy(events[event_offset:], start, end, count),
                        core_request_peak=stats['core_request_peak'],
                        native_callback_calls=stats['native_callback_calls'],
                        cpp_prepare_span_seconds=stats['cpp_prepare_span_ns']/1e9,
                        counts=dict(stats['counts']), wall_seconds_by_call={k:v/1e9 for k,v in stats['wall_ns'].items()},
                        thread_cpu_seconds_by_call={k:v/1e9 for k,v in stats['cpu_ns'].items()},
                        native_thread_ids={k:sorted(v) for k,v in stats['threads'].items()},
                        offered_items=stats['offered_items'], accepted_items=stats['accepted_items'],
                        offer_zero=stats['offer_zero'], release_group_sizes=stats['release_group_sizes'],
                        request_values_sha256=sorted(hashlib.sha256(raw).hexdigest() for raw, _ in requests))
                    assert entry['core_request_peak'] <= 4
                    summary['queries'].append(entry)
                    (args.output/'summary.json').write_text(json.dumps(summary, indent=2)+'\n')
        for ordinal in range(3):
            fingerprints=[q['request_values_sha256'] for q in summary['queries'] if q['ordinal']==ordinal]
            assert all(f == fingerprints[0] for f in fingerprints)
        print(json.dumps([dict(arm=q['arm'],rows=q['rows'],wall=q['wall_seconds'],
            preparations=q['counts'].get('prepare_native_task',0), service=q['service']) for q in summary['queries']], indent=2))
        if args.assert_single_preparation:
            assert all(q['counts'].get('prepare_native_task',0) == q['rows'] for q in summary['queries'] if q['arm']!='native'), 'repeated complete-task preparation under backpressure'
        return 0
    finally:
        http.shutdown(); http.server_close(); server_thread.join(3)
        ray.shutdown()


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except Exception as error:
        print(redact_text(f'{type(error).__name__}: {error}'), file=sys.stderr)
        raise
