"""Opt-in real-Ray snapshot/Future/footer composition; no model or GPU work."""
from concurrent.futures import Future
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest

from src.scheduling.runtime.observations import ServiceMetricsSnapshot
from src.scheduling.submission_control.feedback_options import ResourceRequestConfig
from src.semantic_methods.lotus.prepared_batching import PreparedBatching
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_async_engine import AsyncEngineLimits
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
from tests.execution_provider.resource_request_sampler_fixture import ready, frame_age, request_sampler

UID, OWNER = 'a'*32, 'b'*32


class CPUFeedbackEngine:
    """Native-shaped idle engine fixture; never starts a model process."""
    ready_report = ready(UID)

    def __init__(self):
        self.sequence, self.closed = 0, False

    def snapshot(self):
        self.sequence += 1
        return dict(owner_uid=OWNER, issued_submissions=0,
            native_feedback=dict(engine_uid=UID, sequence=self.sequence,
                observed_at_s=time.monotonic(), running=0, waiting=0, kv_usage=0.,
                generation_tokens_total=0, preemptions_total=0),
            held_requests=0, active_requests=0, input_bytes=0, reserved_result_bytes=0,
            ready_results=0, queued_commands=0, queued_controls=0, unknown_requests=0,
            broken=False, closing=self.closed, child_alive=not self.closed)

    def poll(self, maximum):
        return ()

    def close(self, timeout):
        self.closed = True
        return dict(status='closed', producer_quiescence=dict(
            schema='semloom.async_producer_quiescence.v1', confirmed=True,
            owner_uid=OWNER, engine_uid=UID, issued_submissions=0))


class CountedRef:
    def __init__(self, raw):
        self.raw, self.future_reads = raw, 0
        self.acquired_future = None

    def future(self):
        self.future_reads += 1
        self.acquired_future = self.raw.future()
        return self.acquired_future


def collect_resource_snapshot(feedback, refs, *, timeout=10):
    requests, references = feedback.requests, len(refs)
    snapshot = feedback.sample()
    assert feedback.requests == requests+1 and len(refs) == references+1
    ref = refs[-1]
    # sample() may already have consumed and cleared its Future. Retain the
    # original subscription, never ask the ObjectRef for a second one.
    future = ref.acquired_future
    assert ref.future_reads == 1 and future is not None
    packet = future.result(timeout)
    if feedback.ref is not None:
        assert feedback.ref is ref and feedback.future is future
        snapshot = feedback.sample()
    assert feedback.requests == requests+1 and len(refs) == references+1, 'collection dispatched another RPC'
    assert feedback.ref is None and feedback.future is None
    assert snapshot is not None, 'valid native response was not collected'
    native = packet['ipc']['native_feedback']
    assert (snapshot.source_uid, snapshot.source_session_id, snapshot.source_sequence) == (
        native['engine_uid'], packet['session_id'], native['sequence'])
    return packet, snapshot


class SnapshotCollectionTimingTests(unittest.TestCase):
    def collect_case(self, *, immediate, cached):
        engine = CPUFeedbackEngine()
        engine.snapshot()  # Cached source sequence precedes the next returned frame.
        packet = frame_age(dict(session_id=7, ipc=engine.snapshot(), resources=dict(
            engine_uid=UID, session_id=7, source_elapsed_s=0, gpu_uuids=[],
            status='CPU-fixture', sample=dict(values=dict(host_cpu_per_core_max_pct=20),
                sources=dict(host=dict(cpu_percent_interval_s=.25)), missing={}))))

        class CompleteWhenWaited(Future):
            """Keep initial polling pending; release only at the witness wait."""
            def result(self, timeout=None):
                if timeout is not None and not self.done():
                    self.set_result(packet)
                return super().result(timeout)

        future = Future() if immediate else CompleteWhenWaited()
        if immediate:
            future.set_result(packet)
        refs, calls, future_reads = [], [], []

        def raw_future():
            future_reads.append(future)
            return future

        def remote(**kwargs):
            calls.append(kwargs)
            ref = CountedRef(SimpleNamespace(future=raw_future))
            refs.append(ref)
            return ref

        spec = EngineSpec('/tmp/zero-model-resource-fixture', sys.executable, sys.executable)
        owner = SimpleNamespace(resources=RecordsResources(max_active_batches=128), engine=spec,
            prepared_batching=PreparedBatching('token_budget', None, 4096),
            _actors=SimpleNamespace(model_actor=SimpleNamespace(snapshot=SimpleNamespace(remote=remote)),
                model_ready=ready(UID), limits=AsyncEngineLimits(max_inflight=128),
                ray=SimpleNamespace(cancel=lambda *_a, **_kw: self.fail('completed RPC must not be cancelled'))))
        feedback = request_sampler(owner, 7)
        previous = ServiceMetricsSnapshot(0, 0, 0., source_uid=UID, source_session_id=7,
            source_sequence=1, source_observed_at_s=time.monotonic()-.01)
        if cached:
            feedback.last_native = previous
        native_sample, observed_returns = feedback.sample, []

        def sample():
            value = native_sample()
            observed_returns.append(value)
            return value

        feedback.sample = sample
        try:
            raw, snapshot = collect_resource_snapshot(feedback, refs, timeout=1)
            self.assertIs(raw, packet)
            self.assertEqual((snapshot.source_uid, snapshot.source_session_id, snapshot.source_sequence), (UID, 7, 2))
            self.assertEqual(len(observed_returns), 1 if immediate else 2)
            if not immediate:
                self.assertIs(observed_returns[0], previous if cached else None)
            self.assertIsNone(feedback.future)
            self.assertIsNone(feedback.ref)
            self.assertEqual(calls, [dict(observe_resources=True)])
            self.assertEqual(future_reads, [future])
            self.assertEqual(len(refs), 1)
            self.assertEqual(refs[0].future_reads, 1)
            self.assertEqual(feedback.errors, 0)
        finally:
            feedback.close()
        report = feedback.report()
        self.assertEqual(report['snapshot_requests'], 1)
        self.assertTrue(report['snapshot_rpc_terminal_evidence_complete'])
        self.assertEqual(report['snapshot_rpc_cleanup_status'], 'closed')
        self.assertIsNone(report['snapshot_rpc_pending'])
        self.assertEqual(len(report['snapshot_rpc_receipts']), 1)
        self.assertEqual(report['snapshot_rpc_receipts'][0]['rpc_terminal_status'], 'returned')
        self.assertEqual(report['snapshot_rpc_receipts'][0]['frame_disposition'], 'eligible_for_provider')

    def test_immediate_completion_uses_one_RPC_and_Future_subscription(self):
        for cached in (False, True):
            with self.subTest(cached=cached):
                self.collect_case(immediate=True, cached=cached)

    def test_delayed_completion_uses_the_same_pending_Future_with_or_without_cached_native(self):
        for cached in (False, True):
            with self.subTest(cached=cached):
                self.collect_case(immediate=False, cached=cached)


@unittest.skipUnless(os.environ.get('SEMLOOM_REAL_RAY_CPU') == '1', 'explicit real Ray CPU driver required')
class RealRayHostCPUResourceTests(unittest.TestCase):
    def test_ten_snapshots_share_host_baseline_across_queries_and_flush_light_sink(self):
        import ray
        if ray.is_initialized() or ray.__version__ != '2.56.1':
            raise RuntimeError('a fresh isolated Ray 2.56.1 driver is required')
        code = Path(__file__).resolve().parents[2]
        observer_code = code/'scripts/profiling/record_observation'
        limits, options = AsyncEngineLimits(max_inflight=128), ResourceRequestConfig(poll_interval_s=.1, window_s=.25, max_sample_age_s=.25)
        spec = EngineSpec('/tmp/zero-model-resource-fixture', sys.executable, sys.executable)
        refs, attempts, packets, phases = [], [], [], []
        concurrency = limits.max_inflight+2
        report = dict(schema='semloom.zero_model_host_CPU_Ray.v1', snapshot_RPC_limit=10,
            model_calls=0, engine='CPUFeedbackEngine fixture; no model process',
            real_host_cpu=True, GPU='unassigned/unknown', actor_max_concurrency=concurrency,
            poll_interval_s=options.poll_interval_s, qualification_window_s=options.window_s)
        with tempfile.TemporaryDirectory(prefix='rcpu-', dir=os.environ.get('SEMLOOM_REAL_RAY_TMPDIR', '/tmp')) as temp:
            root = Path(temp)
            observer = root/'observer'
            observer.mkdir()
            (observer/'config.json').write_text(json.dumps(dict(observation=dict(profile='handoff',
                max_events_per_process=200, max_event_bytes=16384))))
            actor = None
            try:
                ray.init(num_cpus=2, num_gpus=0, include_dashboard=False, log_to_driver=False,
                    _temp_dir=str(root/'ray'), object_store_memory=80*1024**2)

                @ray.remote(num_cpus=0, num_gpus=0, max_restarts=0, max_task_retries=0, max_concurrency=concurrency)
                class Snapshots:
                    def __init__(self, spec, limits):
                        from record_ipc_observer import ObservedAsyncCoreModelActor
                        from src.observability.records_telemetry import MetricSources
                        self.actor = ObservedAsyncCoreModelActor(spec, asdict(GenerationSpec()), 1, limits)
                        self.actor.engine = CPUFeedbackEngine()
                        self.actor._resource_sources = MetricSources((), gpu_binding_error='unassigned:CPU_fixture')

                    def configure(self, session):
                        with self.actor._condition:
                            self.actor._session = session

                    def snapshot(self, **kwargs):
                        return self.actor.snapshot(**kwargs)

                    def close(self):
                        from record_observer import close_events
                        value = self.actor.close(3)
                        return dict(close=value, sink=close_events(),
                                    sink_finalized=getattr(self.actor, '_record_sink_finalized', False))

                actor = Snapshots.options(runtime_env=dict(env_vars=dict(
                    PYTHONPATH=os.pathsep.join((str(code), str(observer_code))),
                    SEM_RECORD_OBSERVATION='on', SEM_RECORD_OBSERVER_ROOT=str(observer),
                    SEM_RECORD_RUN_ID='zero-model-CPU', SEM_RECORD_SESSION_ID='CPU-Ray'))).remote(asdict(spec), asdict(limits))

                def remote(**kwargs):
                    if len(attempts) >= 10:
                        raise RuntimeError('zero-model snapshot RPC allowance exhausted')
                    attempt = dict(index=len(attempts)+1, kwargs=kwargs, dispatch='attempted')
                    attempts.append(attempt)
                    try:
                        ref = CountedRef(actor.snapshot.remote(**kwargs))
                        attempt['dispatch'] = 'returned_ObjectRef'
                    except BaseException as error:
                        attempt.update(dispatch='unknown', error_type=type(error).__name__)
                        raise
                    refs.append(ref)
                    return ref

                for session in (7, 8):
                    ray.get(actor.configure.remote(session), timeout=10)
                    if session == 7:
                        self.assertNotIn('resources', remote().future().result(10))
                    owner = SimpleNamespace(resources=RecordsResources(max_active_batches=128), engine=spec,
                        prepared_batching=PreparedBatching('token_budget', None, 4096),
                        _actors=SimpleNamespace(model_actor=SimpleNamespace(snapshot=SimpleNamespace(remote=remote)),
                            model_ready=ready(UID), limits=limits,
                            ray=SimpleNamespace(cancel=lambda ref, **kw: ray.cancel(ref.raw, **kw))))
                    feedback = request_sampler(owner, session, options=options, started_at_s=time.monotonic())
                    try:
                        for _ in range(4):
                            packet, snapshot = collect_resource_snapshot(feedback, refs)
                            packets.append(packet)
                            self.assertIsNotNone(snapshot)
                            time.sleep(options.poll_interval_s)  # Normal sampling cadence; no workload delay.
                    finally:
                        feedback.close()
                        phases.append(feedback.report())
                    self.assertTrue(phases[-1]['snapshot_rpc_terminal_evidence_complete'])
                    self.assertEqual(phases[-1]['snapshot_rpc_cleanup_status'], 'closed')
                    recent = phases[-1]['resource_observation']['recent_samples']
                    self.assertGreaterEqual(sum(row['host_cpu_reject_reason'] is None for row in recent), 2, recent)
                self.assertNotIn('resources', remote().future().result(10))
                report['actor_close'] = ray.get(actor.close.remote(), timeout=10)
                self.assertTrue(report['actor_close']['sink_finalized'])
                self.assertEqual(report['actor_close']['close']['status'], 'closed')
                self.assertTrue(report['actor_close']['sink']['flush_confirmed'])
                self.assertTrue(report['actor_close']['sink']['feedback_buffer']['drain_complete'])
                host = [row['resources']['sample']['sources']['host'] for row in packets]
                self.assertEqual([row['cpu_counter_sample_sequence'] for row in host], list(range(1, 9)))
                self.assertTrue(all(row['cpu_baseline_scope'] == 'sampler_instance' for row in host))
                self.assertEqual(host[0]['cpu_metrics_status'], 'cpu_percent_baseline_priming')
                self.assertNotEqual(host[4]['cpu_metrics_status'], 'cpu_percent_baseline_priming')
                self.assertEqual(len(refs), 10)
                self.assertTrue(all(ref.future_reads == 1 for ref in refs))
                report['status'] = 'passed'
            except BaseException as error:
                report.update(status='failed', error_type=type(error).__name__)
                raise
            finally:
                report['cleanup_errors'] = []
                if actor is not None:
                    try:
                        ray.kill(actor, no_restart=True)
                    except Exception as error:
                        report['cleanup_errors'].append(type(error).__name__)
                ray.shutdown()
                report.update(snapshot_RPCs=len(attempts), dispatch_attempts=attempts, phases=phases, packets=packets,
                              Future_reads=[ref.future_reads for ref in refs], Ray_shutdown=True)
                report['sink_files'] = [dict(name=p.name, sha256=hashlib.sha256(p.read_bytes()).hexdigest(),
                    content=p.read_text()) for p in sorted((observer/'events').glob('*'))]
                if os.environ.get('SEMLOOM_REAL_RAY_RESOURCE_RECEIPT'):
                    with open(os.environ['SEMLOOM_REAL_RAY_RESOURCE_RECEIPT'], 'x') as out:
                        json.dump(report, out, allow_nan=False, indent=2)
                        out.write('\n')
                self.assertEqual(report['cleanup_errors'], [])


if __name__ == '__main__':
    unittest.main()
