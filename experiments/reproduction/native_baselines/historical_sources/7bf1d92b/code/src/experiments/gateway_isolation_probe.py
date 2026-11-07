"""Local UDS/Core interference diagnosis; vendor tables, workers and peers are fixtures."""

import argparse
import asyncio
from dataclasses import asdict, replace
from datetime import datetime, timezone
from functools import partial
import gzip
import hashlib
import json
import os
from pathlib import Path
import platform
import resource
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from unittest.mock import patch

from src.baselines.common.private_artifacts import new_private_directory, open_private_text, write_private_json
from src.baselines.common.redact import redact_json_values, redact_text
from src.execution_provider.adapters.incremental_execution import build_fixed_model_execution
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.ray_map_transport import RayMapConfig, RayMapTransport, _RemoteResult
from src.execution_provider.multiplexed_gateway import MultiSessionMapGateway
from src.execution_provider.semantic_map import SemanticMapPlan
from src.execution_provider.session_dispatch import run_session
from src.execution_provider.wire import v6
from src.execution_provider.wire.framing import encode_frame, read_frame
from src.scheduling.core.session_jobs import shared_compute_job_budget
from .map_observation_probe import SyntheticRay, SyntheticTable, CLOCK


SCENARIOS = ('owner_prepare', 'payload_prepare', 'slow_consumer', 'observer_block')
CAPACITY, CASE_SECONDS, SUITE_SECONDS = 4, 20, 300


def output_value(label, sequence, scenario):
    text = f'fixture:{label}:{sequence}'
    return text + '\n' * 60000 if scenario == 'slow_consumer' and label == 'A' else text


def _label(body):
    return json.loads(body)['messages'][-1]['content'].split(':')[0]


class Capture:
    def __init__(self):
        self.events, self.lock, self.injector = [], threading.Lock(), None
        self.session_jobs = {}

    def record(self, event, **fields):
        self._record(dict(event=event, **fields))

    def _record(self, fields):
        entry = dict(fields, monotonic_ns=time.monotonic_ns(), thread=threading.current_thread().name)
        if 'raw_output' in entry:
            raw = entry.pop('raw_output').encode()
            entry.update(output_bytes=len(raw), output_sha256=hashlib.sha256(raw).hexdigest())
        with self.lock:
            if len(self.events) >= 20000:
                raise RuntimeError('finite diagnostic event capacity exhausted')
            if entry['event'] == 'query_flow_joined':
                self.session_jobs[entry['engine_session_id']] = entry['job_id']
            self.events.append(entry)

    def __call__(self, fields):
        self._record(fields)
        if self.injector and fields['event'] == 'ray_first_submit':
            self.injector.delay('observer_block')


class Intervention:
    def __init__(self, scenario, paused, duration, capture):
        self.scenario, self.paused, self.duration, self.capture = scenario, paused, duration, capture
        self.started, self.reader, self.lock = threading.Event(), threading.Event(), threading.Lock()
        self.began_ns, self.ended_ns, self.release_at = None, None, None
        if scenario != 'slow_consumer' or not paused:
            self.reader.set()

    def begin(self, stage):
        with self.lock:
            if stage != self.scenario or self.began_ns is not None:
                return False
            self.began_ns = time.monotonic_ns()
            self.release_at = time.monotonic() + (self.duration if self.paused else 0)
            self.capture.record('fixture_interference_started', stage=stage, paused=self.paused)
            self.started.set()
            return True

    def finish(self):
        with self.lock:
            if self.began_ns is not None and self.ended_ns is None:
                self.ended_ns = time.monotonic_ns()
                self.capture.record('fixture_interference_ended', stage=self.scenario)
            self.reader.set()

    def delay(self, stage):
        if self.begin(stage):
            if self.paused:
                time.sleep(self.duration)
            self.finish()


class Worker(SyntheticRay):
    """Use the existing independent fixture loop; retain complete task keys."""

    def __init__(self, capture, scenario, rows):
        self.capture, self.scenario, self.rows = capture, scenario, rows
        super().__init__({})

    def _submit(self, table, index, template):
        key = template.key
        identity = (key.session_id, key.sequence)
        label = _label(table.rows[index][2])
        if label not in ('A', 'B') or key.sequence >= self.rows or identity in self.issued:
            raise AssertionError('fixture call identity, cancellation or multiplicity differs')
        if len(self.issued) >= 2 * self.rows:
            raise AssertionError('finite synthetic call capacity exhausted')
        self.issued.append(identity)
        self.capture.record('fixture_worker_submitted', key=asdict(key), label=label)
        future = asyncio.run_coroutine_threadsafe(self._execute(table, index, template), self.loop)
        return asyncio.wrap_future(future)

    async def _execute(self, table, index, template):
        began, key = time.monotonic_ns(), template.key
        session, sequence, body = table.rows[index]
        if (session, sequence) != (key.session_id, key.sequence):
            raise AssertionError('fixture table and row identity differ')
        label = _label(body)
        if json.loads(body)['messages'][-1]['content'] != f'{label}:{sequence}':
            raise AssertionError('fixture input content differs')
        self.active += 1
        self.peak = max(self.peak, self.active)
        self.capture.record('fixture_worker_started', key=asdict(key), label=label, active=self.active)
        try:
            await asyncio.sleep(.010 + (sequence % 3) * .001)
            result = json.dumps(dict(model='model', choices=[dict(message=dict(
                content=output_value(label, sequence, self.scenario)), finish_reason='stop')],
                usage=dict(prompt_tokens=2, completion_tokens=1))).encode()
            ended = time.monotonic_ns()
            self.completed.append((key.session_id, key.sequence))
            self.capture.record('fixture_worker_completed', key=asdict(key), label=label)
            return _RemoteResult(key, result, began, ended, CLOCK)
        finally:
            self.active -= 1


class Client:
    def __init__(self, path, label, capture):
        self.label, self.capture, self.digests, self.results = label, capture, {}, []
        self.control = socket.socket(socket.AF_UNIX)
        self.control.settimeout(3)
        self.control.connect(path)
        self.control.sendall(encode_frame(dict(type='query_open', binding_version=1, flow_count=1)))
        registered = read_frame(self.control)
        if registered is None or registered.get('type') != 'query_opened':
            raise AssertionError('fixture query registration failed')
        self.stream = socket.socket(socket.AF_UNIX)
        self.stream.settimeout(3)
        self.stream.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        self.stream.connect(path)
        self.stream.sendall(encode_frame(dict(type='stream_join', binding_version=1,
                                              token=registered['token'], flow=0)))
        if read_frame(self.stream) != dict(type='stream_joined', binding_version=1):
            raise AssertionError('fixture stream membership failed')
        self.plan = SemanticMapPlan('Echo.', 'model', 8)
        opened = v6.build_open_message(self.plan)
        self.context = v6.validate_open(opened)
        self.stream.sendall(encode_frame(opened))
        if read_frame(self.stream).get('type') != 'opened':
            raise AssertionError('fixture semantic opening failed')

    def offer(self, sequence):
        message = v6.build_task_message(self.plan, sequence=sequence, input_value=f'{self.label}:{sequence}')
        self.capture.record('fixture_offer_started', label=self.label, sequence=sequence)
        self.stream.sendall(encode_frame(message))
        accepted = read_frame(self.stream)
        if (accepted is None or accepted.get('type') != 'accepted'
                or accepted.get('protocol_version') != 6 or accepted.get('sequence') != str(sequence)
                or accepted.get('accepted_prefix_count') != 1):
            raise AssertionError('fixture input not accepted exactly once')
        self.digests[sequence] = message['semantic_payload_digest']
        self.capture.record('fixture_offer_accepted', label=self.label, sequence=sequence)

    def consume(self, rows, scenario, reader):
        for _ in range(rows):
            self.stream.sendall(encode_frame(dict(type='poll', protocol_version=6)))
            if self.label == 'A' and not reader.wait(CASE_SECONDS):
                raise TimeoutError('finite fixture reader wait expired')
            message = read_frame(self.stream)
            if message is None or message.get('type') != 'completion':
                raise AssertionError('fixture completion missing')
            sequence = int(message['sequence'])
            if sequence in self.results or sequence not in self.digests:
                raise AssertionError('fixture result association or multiplicity differs')
            completion = v6.validate_completion(message, expected_sequence=sequence,
                payload_digest=self.digests[sequence], open_context=self.context)
            if completion.raw_output != output_value(self.label, sequence, scenario):
                raise AssertionError('fixture output content differs')
            self.results.append(sequence)
            self.capture.record('fixture_result_validated', label=self.label, sequence=sequence,
                                output_sha256=hashlib.sha256(completion.raw_output.encode()).hexdigest())
        if sorted(self.results) != list(range(rows)):
            raise AssertionError('fixture ordered reconstruction differs')
        self.capture.record('fixture_consumer_completed', label=self.label)

    def close(self):
        for name in ('stream', 'control'):
            connection = getattr(self, name, None)
            if connection is not None:
                connection.close()


def analyze(events, rows, scenario):
    def one(kind, **fields):
        matches = [e for e in events if e['event'] == kind and all(e.get(k) == v for k, v in fields.items())]
        if len(matches) != 1:
            raise AssertionError('diagnostic event population differs')
        return matches[0]

    offers, receives, sessions, jobs = {}, {}, {}, []
    for event in events:
        if event['event'] == 'job_opened':
            jobs.append(event['job_id'])
        if event['event'] == 'query_flow_joined':
            sessions[event['job_id']] = event['engine_session_id']
    if len(jobs) != 3 or len(sessions) != 3:
        raise AssertionError('fixture query/flow population differs')
    labels = dict(zip(jobs, ('A', 'B', 'C')))
    expected_calls = {(sessions[job], sequence) for job in jobs[:2] for sequence in range(rows)}
    expected_offers = expected_calls | {(sessions[jobs[2]], 0)}
    offered = [e for e in events if e['event'] == 'offer']
    offered_keys = [(e['key']['session_id'], e['key']['sequence']) for e in offered]
    if (len(offered_keys) != 2 * rows + 1 or set(offered_keys) != expected_offers
            or any(e['accepted_prefix_count'] != 1 for e in offered)):
        raise AssertionError('fixture accepted-prefix history differs')
    for kind in ('fixture_worker_submitted', 'fixture_worker_completed', 'terminal'):
        keys = [(e['key']['session_id'], e['key']['sequence']) for e in events if e['event'] == kind]
        if len(keys) != 2 * rows or set(keys) != expected_calls:
            raise AssertionError('fixture call/terminal keys differ')
    for label in ('A', 'B'):
        offers[label] = one('fixture_offer_started', label=label, sequence=0)['monotonic_ns']
        receives[label] = one('fixture_consumer_completed', label=label)['monotonic_ns']
        observed = [e for e in events if e['event'] == 'fixture_result_validated' and e['label'] == label]
        if sorted(e['sequence'] for e in observed) != list(range(rows)):
            raise AssertionError('fixture wire result population differs')
        if any(e['output_sha256'] != hashlib.sha256(output_value(label, e['sequence'], scenario).encode()).hexdigest()
               for e in observed):
            raise AssertionError('fixture result hash differs')
    started = one('fixture_interference_started')
    finished = one('fixture_interference_ended')
    cancelled = one('fixture_cancel_requested')['monotonic_ns']
    drained = one('job_drained', job_id=jobs[2])['monotonic_ns']
    b_results = [e['monotonic_ns'] for e in events if e['event'] == 'fixture_result_validated' and e['label'] == 'B']
    usages = [e['usage'] for e in events if 'usage' in e]
    peak = {name: max(u[name] for u in usages) for name in usages[0]}
    windows = [e for e in events if e['event'] == 'fixture_window']
    sent = [e for e in events if e['event'] == 'fixture_send_completed']
    return dict(
        b_first_accept_ms=(one('fixture_offer_accepted', label='B', sequence=0)['monotonic_ns'] - offers['B']) / 1e6,
        b_first_result_ms=(min(b_results) - offers['B']) / 1e6,
        b_consume_ms=(receives['B'] - offers['B']) / 1e6,
        a_consume_ms=(receives['A'] - offers['A']) / 1e6,
        queued_cancel_drain_ms=(drained - cancelled) / 1e6,
        interference_ms=(finished['monotonic_ns'] - started['monotonic_ns']) / 1e6,
        interference_thread=started['thread'],
        core_observed_peak=peak,
        object_observed_peak_bytes=max(e.get('object_bytes', 0) for e in events),
        window_rows=[e['rows'] for e in windows],
        window_labels=[labels[e['job_id']] for e in windows],
        a_send_max_ms=max((e['ended_ns'] - e['began_ns']) / 1e6 for e in sent) if sent else None,
        worker_active_peak=max(e['active'] for e in events if e['event'] == 'fixture_worker_started'),
        synthetic_calls=2 * rows, exactly_once=True, queued_cancel_calls=0,
    )


def _fd_count():
    for directory in ('/proc/self/fd', '/dev/fd'):
        if Path(directory).is_dir():
            return len(os.listdir(directory))
    return None


def run_case(output, scenario, paused, *, rows=16, stall_s=.25):
    if scenario not in SCENARIOS or type(paused) is not bool or type(rows) is not int or not 1 <= rows <= 16:
        raise ValueError('invalid finite local diagnosis configuration')
    if not 0 <= stall_s <= 1:
        raise ValueError('fixture stall must be finite and at most one second')
    output = Path(output)
    new_private_directory(output)
    config = dict(scenario=scenario, paused=paused, rows_per_producing_job=rows, stall_seconds=stall_s,
        jobs=3, capacity=CAPACITY, held_tasks=3 * max(CAPACITY, rows), batch_rows=CAPACITY,
        frame_timeout_ms=3000, case_timeout_seconds=CASE_SECONDS, http_requests=0, model_requests=0, pg_queries=0,
        kernel_peer_credentials='fixture', vendor_api='SyntheticRay', table='SyntheticTable')
    write_private_json(output / 'config.json', config)
    capture = Capture()
    injector = capture.injector = Intervention(scenario, paused, stall_s, capture)
    before_fds = _fd_count()
    before_threads = set(threading.enumerate())
    worker = Worker(capture, scenario, rows)
    stop, ready = threading.Event(), threading.Event()
    gateways, transports, errors, clients, threads = [], [], [], [], []
    began, failure = time.monotonic_ns(), None
    report = dict(config, schema='semloom.gateway-isolation.case.v1', status='failed', errors=errors,
                  started_utc=datetime.now(timezone.utc).isoformat())

    def batches(data, limits, *, batch_rows, backend):
        if backend != 'arrow' or len(data) > limits.rows or sum(len(r[2]) + 24 for r in data) > limits.bytes:
            raise AssertionError('fixture preparation exceeds declared capacity')
        if len({capture.session_jobs[r[0]] for r in data}) != 1:
            raise AssertionError('fixture payload window contains different Jobs')
        if _label(data[0][2]) == 'A':
            injector.delay('payload_prepare')
        for offset in range(0, len(data), batch_rows):
            group = data[offset:offset + batch_rows]
            capture.record('fixture_window', rows=len(group),
                           job_id=capture.session_jobs[group[0][0]])
            time.sleep(.0005)
            yield SyntheticTable(group)

    def transport_factory(config, maximum, observer):
        transport = RayMapTransport(config, maximum, observer, ray_api=worker,
            physical=RayMapConfig('fixture-cluster', 1, CAPACITY, 2097152, 4194304, payload_backend='arrow'))
        transports.append(transport)
        return transport

    def execution_factory(config, **kwargs):
        kwargs.pop('execute')
        execution = build_fixed_model_execution(config, **kwargs, transport_factory=transport_factory,
                                                allocate_job=shared_compute_job_budget)
        prepare = execution.prepare_task

        def prepare_task(request, sequence):
            if request.canonical_messages[-1]['content'].startswith('A:'):
                injector.delay('owner_prepare')
            return prepare(request, sequence)

        return replace(execution, prepare_task=prepare_task)

    def serve(path):
        gateway = None
        with socket.socket(socket.AF_UNIX) as listener:
            try:
                gateway = MultiSessionMapGateway(FixedModelConfig('http://localhost/unused-fixture', 'model', 5000),
                    max_jobs=3, max_connections=8, max_tasks=config['held_tasks'], max_active_requests=CAPACITY,
                    frame_timeout_ms=3000, observer=capture, execution_factory=execution_factory)
                gateways.append(gateway)
                listener.bind(path)
                listener.listen(8)
                ready.set()
                handler = partial(run_session, completion_adapter=gateway, response_delay_ms=0,
                    tamper_evidence_digest=False, disconnect_on_task=False, completion_fixture=None)
                gateway.serve(listener, stop, handler=handler)
            except BaseException as error:
                errors.append(dict(where='service', exception_type=type(error).__name__))
            finally:
                ready.set()
                if gateway is not None:
                    try:
                        if not gateway.close():
                            raise RuntimeError('fixture transport did not close')
                    except BaseException as error:
                        errors.append(dict(where='service_cleanup', exception_type=type(error).__name__))

    def produce(client):
        try:
            for sequence in range(rows):
                client.offer(sequence)
            client.consume(rows, scenario, injector.reader)
        except BaseException as error:
            errors.append(dict(where=client.label, exception_type=type(error).__name__))

    real_connect, real_connect_ex, real_send = socket.socket.connect, socket.socket.connect_ex, socket.socket.sendall
    with tempfile.TemporaryDirectory(prefix='sgw-', dir='/tmp') as directory:
        path = str(Path(directory) / 's')

        def connect(connection, address):
            if connection.family != socket.AF_UNIX or address != path:
                raise AssertionError('only the declared local fixture UDS is allowed')
            return real_connect(connection, address)

        def connect_ex(connection, address):
            if connection.family != socket.AF_UNIX or address != path:
                raise AssertionError('only the declared local fixture UDS is allowed')
            return real_connect_ex(connection, address)

        def send(connection, frame, *args, **kwargs):
            targeted = scenario == 'slow_consumer' and b'"raw_output":"fixture:A:' in frame
            if not targeted:
                return real_send(connection, frame, *args, **kwargs)
            connection.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
            start = time.monotonic_ns()
            injector.begin('slow_consumer')
            if not paused:
                injector.finish()
            result = real_send(connection, frame, *args, **kwargs)
            capture.record('fixture_send_completed', began_ns=start, ended_ns=time.monotonic_ns(), bytes=len(frame))
            return result

        service_thread = threading.Thread(target=serve, args=(path,), name='fixture-gateway-owner')
        with patch('src.execution_provider.multiplexed_gateway.trusted_peer', return_value=(1, 42)), \
             patch('src.execution_provider.adapters.ray_map_transport.iter_payload_batches', batches), \
             patch('src.execution_provider.adapters.ray_map_transport._clock_domain', return_value=CLOCK), \
             patch.object(socket.socket, 'connect', connect), patch.object(socket.socket, 'connect_ex', connect_ex), \
             patch.object(socket.socket, 'sendall', send):
            try:
                service_thread.start()
                if not ready.wait(3) or not gateways:
                    raise RuntimeError('fixture service did not become ready')
                for label in ('A', 'B', 'C'):
                    clients.append(Client(path, label, capture))
                clients[2].offer(0)  # Accepted, with dispatch still disabled.
                first = threading.Thread(target=produce, args=(clients[0],), name='fixture-producer-A')
                threads.append(first)
                first.start()
                if not injector.started.wait(3):
                    raise RuntimeError('fixture intervention did not start')
                capture.record('fixture_cancel_requested', label='C')
                clients[2].control.close()
                second = threading.Thread(target=produce, args=(clients[1],), name='fixture-producer-B')
                threads.append(second)
                second.start()
                deadline = time.monotonic() + CASE_SECONDS
                while any(thread.is_alive() for thread in threads):
                    if errors or time.monotonic() >= deadline:
                        raise RuntimeError('fixture driver failed or exceeded its deadline')
                    if scenario == 'slow_consumer' and time.monotonic() >= injector.release_at:
                        injector.finish()
                    for thread in threads:
                        thread.join(.002)
                if errors:
                    raise RuntimeError('fixture client failed')
            except BaseException as error:
                failure = error
                errors.append(dict(where='driver', exception_type=type(error).__name__))
            finally:
                injector.finish()
                for client in clients:
                    client.close()
                stop.set()
                if gateways:
                    gateways[0].request_stop()
                for thread in threads:
                    thread.join(4)
                service_thread.join(7)
                if service_thread.is_alive() or any(t.is_alive() for t in threads):
                    errors.append(dict(where='threads', exception_type='UnconfirmedThreadExit'))
                try:
                    worker.close()
                except BaseException as error:
                    errors.append(dict(where='worker_cleanup', exception_type=type(error).__name__))
    ended = time.monotonic_ns()
    report.update(case_seconds=(ended - began) / 1e9, synthetic_calls=len(worker.issued),
        settled_synthetic_calls=len(worker.completed), actor_disposed=worker.killed,
        worker_stopped=not worker.thread.is_alive(),
        remaining_threads=[t.name for t in threading.enumerate() if t not in before_threads],
        fd_count_before=before_fds, fd_count_after=_fd_count(),
        core_final_usage=asdict(gateways[0].engine.capacity.usage()) if gateways else None,
        transport_drained=bool(transports) and not any(t.rows or t.blocks or t.unknown or t.used_bytes for t in transports),
        process_ru_maxrss_raw=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        process_ru_maxrss_unit='unavailable; raw OS unit was not normalized by this probe',
        completed_utc=datetime.now(timezone.utc).isoformat(),
        rss_scope='process since startup; raw OS unit, not a Core buffer measurement')
    try:
        if not errors and failure is None:
            report.update(analyze(capture.events, rows, scenario))
            if (any(report['core_final_usage'].values()) or report['remaining_threads']
                    or not report['transport_drained'] or not report['worker_stopped'] or not report['actor_disposed']
                    or (report['fd_count_after'] is not None and report['fd_count_before'] is not None
                        and report['fd_count_after'] > report['fd_count_before'])
                    or report['core_observed_peak']['active_requests'] > CAPACITY
                    or report['worker_active_peak'] > CAPACITY or report['object_observed_peak_bytes'] > 4194304):
                raise AssertionError('fixture capacity or final resources differ')
            report['status'] = 'passed'
    except BaseException as error:
        failure = error
        errors.append(dict(where='analysis', exception_type=type(error).__name__))
    with gzip.open(output / 'events.jsonl.gz', 'xt', encoding='utf-8') as stream:
        for event in capture.events:
            stream.write(json.dumps(redact_json_values(event), separators=(',', ':')) + '\n')
    write_private_json(output / 'result.json', redact_json_values(report))
    if report['status'] != 'passed':
        raise RuntimeError('local gateway diagnosis failed; evidence retained') from failure
    return report


def aggregate(cases):
    metrics = ('b_first_accept_ms', 'b_first_result_ms', 'b_consume_ms', 'queued_cancel_drain_ms')
    result = {}
    for scenario in SCENARIOS:
        result[scenario] = {}
        for paused in (False, True):
            selected = [r for r in cases if r['scenario'] == scenario and r['paused'] == paused
                        and r['phase'] == 'measurement']
            if len(selected) != 3:
                raise AssertionError('three retained measurement repetitions are required')
            result[scenario]['paused' if paused else 'control'] = {
                key: dict(values=[r[key] for r in selected], median=statistics.median(r[key] for r in selected))
                for key in metrics}
    return result


def replay_suite(source, output):
    """Recompute timing, keys and capacities from the retained event populations."""
    source, output = Path(source), Path(output)
    new_private_directory(output)
    original = json.loads((source / 'summary.json').read_text())
    cases = []
    for saved in original['cases']:
        with gzip.open(source / saved['case_id'] / 'events.jsonl.gz', 'rt') as stream:
            events = [json.loads(line) for line in stream]
        computed = analyze(events, saved['rows_per_producing_job'], saved['scenario'])
        if any(saved[key] != value for key, value in computed.items()):
            raise AssertionError('retained case metrics differ from raw events')
        cases.append(dict(saved, **computed))
    recomputed = aggregate(cases)
    if recomputed != original['analysis']:
        raise AssertionError('retained suite aggregation differs')
    result = dict(status='passed', cases=len(cases), synthetic_calls=sum(r['synthetic_calls'] for r in cases),
                  analysis=recomputed, scope='event identities and derived metrics; runtime cleanup uses saved snapshots')
    write_private_json(output / 'replay.json', result)
    return result


def run_suite(output):
    output = Path(output)
    new_private_directory(output)
    repo = Path(__file__).resolve().parents[3]
    sources = ('code/src/experiments/gateway_isolation_probe.py', 'code/src/experiments/map_observation_probe.py',
               'code/src/execution_provider/multiplexed_gateway.py',
               'code/src/execution_provider/adapters/ray_map_transport.py', 'code/src/scheduling/core/session.py')
    metadata = dict(schema='semloom.gateway-isolation.suite.v1', reference_commit=subprocess.check_output(
        ['git', '-C', str(repo), 'rev-parse', 'HEAD'], text=True).strip(),
        source_sha256={name: hashlib.sha256((repo / name).read_bytes()).hexdigest() for name in sources},
        python=sys.version, platform=platform.system(), architecture=platform.machine(),
        simulation_capacity=1088, http_requests=0, model_requests=0, pg_queries=0,
        comparison_role='project_scheduled_method', formal_baseline_eligible=False,
        scheduler_owner='SemLoom SessionEngine', kernel_peer_credentials='fixture',
        started_utc=datetime.now(timezone.utc).isoformat(), run_id=output.name, status='running', cases=[])
    write_private_json(output / 'metadata.json', metadata)
    started = time.monotonic()
    for phase, repeat in (('check', -1), ('warmup', 0), ('measurement', 1), ('measurement', 2), ('measurement', 3)):
        for scenario in SCENARIOS:
            for paused in ((False, True) if repeat % 2 else (True, False)):
                identity = f'{phase}-{repeat}-{scenario}-{int(paused)}'
                if time.monotonic() - started >= SUITE_SECONDS:
                    raise TimeoutError('finite gateway diagnostic suite deadline')
                command = [sys.executable, '-m', 'src.experiments.gateway_isolation_probe', '--case',
                           '--output', str(output / identity), '--scenario', scenario, '--rows',
                           '4' if phase == 'check' else '16']
                if paused:
                    command.append('--paused')
                try:
                    run = subprocess.run(command, capture_output=True, text=True,
                                         timeout=min(35, max(1, SUITE_SECONDS - (time.monotonic() - started))))
                except subprocess.TimeoutExpired as error:
                    metadata.update(status='failed', failed_case=identity, actual_case_calls='unavailable',
                                    failure='case supervisor timeout')
                    write_private_json(output / 'failure.json', metadata)
                    with open_private_text(output / f'{identity}.log') as stream:
                        stream.write(redact_text((error.stdout or b'').decode(errors='replace')
                                                 + (error.stderr or b'').decode(errors='replace')))
                    raise RuntimeError('gateway diagnostic child timed out; evidence retained') from None
                with open_private_text(output / f'{identity}.log') as stream:
                    stream.write(redact_text(run.stdout + run.stderr))
                result_file = output / identity / 'result.json'
                row = json.loads(result_file.read_text()) if result_file.exists() else dict(status='failed')
                row.update(case_id=identity, phase=phase, repeat=repeat, returncode=run.returncode)
                metadata['cases'].append(row)
                if run.returncode or row['status'] != 'passed':
                    metadata['status'] = 'failed'
                write_private_json(output / 'progress-next.json', redact_json_values(metadata))
                os.replace(output / 'progress-next.json', output / 'progress.json')
                if run.returncode or row['status'] != 'passed':
                    raise RuntimeError('gateway diagnostic suite stopped on first error; evidence retained')
    metadata.update(status='passed', analysis=aggregate(metadata['cases']),
                    synthetic_calls=sum(r['synthetic_calls'] for r in metadata['cases']),
                    wall_seconds=time.monotonic() - started, completed_utc=datetime.now(timezone.utc).isoformat())
    if metadata['synthetic_calls'] != 1088:
        raise AssertionError('finite suite call accounting differs')
    write_private_json(output / 'summary.json', redact_json_values(metadata))
    return metadata


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--case', action='store_true')
    parser.add_argument('--replay', type=Path)
    parser.add_argument('--scenario', choices=SCENARIOS)
    parser.add_argument('--paused', action='store_true')
    parser.add_argument('--rows', type=int, default=16)
    args = parser.parse_args(argv)
    if args.replay is not None:
        result = replay_suite(args.replay, args.output)
    else:
        result = run_case(args.output, args.scenario, args.paused, rows=args.rows) if args.case else run_suite(args.output)
    print(json.dumps(dict(status=result['status'], synthetic_calls=result['synthetic_calls'])))


if __name__ == '__main__':
    main()
