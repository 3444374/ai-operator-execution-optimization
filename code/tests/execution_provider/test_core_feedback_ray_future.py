"""Real Ray ObjectRef/Future sampling, in a private CPU-only driver.

The proxy counts calls but returns Ray's actual new concurrent Future each time.
A constant-Future stub would miss the production notification loss.
Metrics, ready and age packets are CPU fixtures; the ObjectRef/Future is real.
"""
import os
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.execution_provider.adapters import records_request_feedback as wiring
from src.execution_provider.adapters.records_native_source import native_sample_age
from tests.scheduling.test_incremental_session import setup


class IdleProvider:
    """Tests drive the sampler directly; this provider starts no thread."""
    is_running = False
    def __init__(self, sampler, *, poll_interval_s, stale_after_s, close_timeout_s,
                 ready_event=None, collect_ready=None):
        self.sampler = sampler
        self.poll_interval_s = poll_interval_s
        self.stale_after_s = stale_after_s
        self.close_timeout_s = close_timeout_s
    def pause(self): pass
    def resume(self): pass
    def close(self): pass


def ready(uid):
    return dict(engine_uid=uid, batch_mode='independent_final_results', closed=False,
        resource_owner=dict(schema='semloom.model_resource_owner.v1', engine_uid=uid,
            request_memory_owner='vllm', native_feedback_clock='private_child_and_model_actor.monotonic'))


def native_frame(*, session=7, uid='real-Ray-CPU-fixture', sequence=1,
                 source_at_s=None, captured_at_s=None):
    captured = time.monotonic() if captured_at_s is None else captured_at_s
    native = dict(engine_uid=uid, sequence=sequence,
        observed_at_s=captured if source_at_s is None else source_at_s,
        running=1, waiting=0, kv_usage=.1, preemptions_total=0, generation_tokens_total=4)
    return dict(session_id=session, ipc=dict(native_feedback=native),
        native_sample_age=native_sample_age(native, ready(uid), captured_at_s=captured))


def bind_feedback(feedback, session):
    feedback.begin(100, 1)
    feedback.stream = SimpleNamespace(session=SimpleNamespace(session_id=session))


def make_feedback(owner, *, session=7, poll_interval_s=.25, max_sample_age_s=.75,
                  provider_type=IdleProvider):
    owner.core, _, _, _ = setup(held_tasks=4, input_bytes=4096, result_bytes=4096,
        item_input_bytes=1024, item_result_bytes=1024, active_requests=4, active_work=4)
    owner._actors.model_ready = ready(owner._actors.model_ready['engine_uid'])
    owner._actors.limits = SimpleNamespace(max_inflight=4, result_slots=4,
        result_bytes=4096, item_result_bytes=1024)
    feedback = wiring.CoreResourceRequestFeedback(owner, dict(kind='resource_feedback',
        poll_interval_s=poll_interval_s, max_sample_age_s=max_sample_age_s, allow_exploration=False))
    with patch.object(wiring, 'NonBlockingMetricsObservationProvider', provider_type):
        bind_feedback(feedback, session)
    return feedback


def eligible_samples(feedback):
    return sum(row['frame_disposition'] == 'eligible_for_provider'
               for row in feedback.snapshot_rpc_receipts)


class CountedRef:
    def __init__(self, ref):
        self.raw = ref
        self.futures = []
    def future(self):
        value = self.raw.future()
        self.futures.append(value)
        return value


class ProviderShapeTests(unittest.TestCase):
    def feedback(self):
        owner = SimpleNamespace(resources=SimpleNamespace(max_active_batches=4),
            _actors=SimpleNamespace(model_ready=dict(engine_uid='CPU-provider-shape')))
        value = make_feedback(owner)
        self.addCleanup(value.stop)
        return value

    def test_idle_fixture_keeps_the_provider_source_duration_separate_from_work_window(self):
        feedback = self.feedback()
        self.assertEqual(feedback.provider.stale_after_s, .75)
        self.assertEqual(feedback.provider.poll_interval_s, .25)
        self.assertEqual(feedback.options.max_sample_age_s, feedback.provider.stale_after_s)
        self.assertNotEqual(feedback.options.window_s, feedback.provider.stale_after_s)

    def test_next_query_keeps_the_same_provider_and_its_actual_declared_duration(self):
        feedback = self.feedback()
        provider = feedback.provider
        self.assertEqual(provider.stale_after_s, .75)
        feedback.pause()
        bind_feedback(feedback, 8)
        self.assertIs(feedback.provider, provider)
        self.assertEqual(feedback.provider.stale_after_s, .75)
        self.assertEqual(feedback.controller.current_window, 1)
        self.assertEqual(feedback.native_source_clock.session_id, 8)

    def test_finish_reports_the_duration_from_the_existing_provider(self):
        feedback = self.feedback()
        stream = feedback.stream
        report = feedback.finish(stream)
        self.assertEqual(report['source_stale_after_s'], feedback.provider.stale_after_s)
        self.assertTrue(report['sampling_paused'])
        self.assertTrue(report['sampler_stopped'])


@unittest.skipUnless(os.environ.get('SEMLOOM_REAL_RAY_CPU') == '1', 'explicit real Ray CPU driver required')
class RealRayFeedbackFutureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import ray
        if ray.is_initialized():
            raise RuntimeError('real Future regression requires its own fresh driver')
        if ray.__version__ != '2.56.1':
            raise RuntimeError('real Future regression requires Ray 2.56.1')
        cls.ray = ray
        cls.directory = tempfile.TemporaryDirectory(prefix='rff-',
            dir=os.environ.get('SEMLOOM_REAL_RAY_TMPDIR', '/tmp'))
        ray.init(num_cpus=2, num_gpus=0, include_dashboard=False, log_to_driver=False,
                 _temp_dir=cls.directory.name, object_store_memory=80*1024**2)
        @ray.remote(num_cpus=0, max_restarts=0, max_concurrency=8)
        class Snapshots:
            def __init__(self):
                self.calls = 0
                self.events = {}
                self.session = 7
                self.fail = False
            def configure(self, session, fail=False):
                self.session, self.fail = session, fail
            async def snapshot(self):
                import asyncio
                index = self.calls
                self.calls += 1
                event = self.events[index] = asyncio.Event()
                value = native_frame(session=self.session, sequence=index+1)
                fail = self.fail
                await event.wait()
                if fail:
                    raise RuntimeError('intentional CPU snapshot failure')
                return value
            def release(self, index): self.events[index].set()
            def count(self): return self.calls
        cls.actor_type = Snapshots

    @classmethod
    def tearDownClass(cls):
        cls.ray.shutdown()
        cls.directory.cleanup()

    def setUp(self):
        self.actor = self.actor_type.remote()
        self.ray.get(self.actor.configure.remote(7), timeout=5)
        self.refs, self.cancels = [], []
        self.cancel_error = False
        def remote():
            value = CountedRef(self.actor.snapshot.remote())
            self.refs.append(value)
            return value
        def cancel(ref, **options):
            self.cancels.append(ref)
            if self.cancel_error:
                raise RuntimeError('intentional cancellation transport failure')
            self.ray.cancel(ref.raw, **options)
        owner = SimpleNamespace(resources=SimpleNamespace(max_active_batches=4),
            _actors=SimpleNamespace(model_actor=SimpleNamespace(snapshot=SimpleNamespace(remote=remote)),
                ray=SimpleNamespace(cancel=cancel), model_ready=dict(engine_uid='real-Ray-CPU-fixture')))
        self.feedback = make_feedback(owner)

    def tearDown(self):
        self.feedback.stop()
        self.ray.kill(self.actor, no_restart=True)

    def entered(self, count):
        deadline = time.monotonic() + 5
        while self.ray.get(self.actor.count.remote(), timeout=5) < count:
            if time.monotonic() >= deadline:
                self.fail('controlled Ray snapshot did not enter')
            time.sleep(.01)  # test coordination only; never used by production sampler

    def release(self, index, *, failed=False):
        self.entered(index+1)
        self.ray.get(self.actor.release.remote(index), timeout=5)
        # Coordinate the very Future retained by production, not an independent
        # notification for the same object. sample() itself still never waits.
        witness = self.refs[index].futures[0]
        if failed:
            with self.assertRaises(Exception): witness.result(timeout=5)
        else:
            witness.result(timeout=5)

    def test_pending_then_done_keeps_one_real_future_and_yields_feedback_without_wait(self):
        f = self.feedback
        self.assertIsNone(f.sample())
        self.entered(1)
        started = time.monotonic()
        for _ in range(20): self.assertIsNone(f.sample())
        self.assertLess(time.monotonic()-started, 1.)
        self.assertEqual(len(self.refs[0].futures), 1)
        self.release(0)
        self.assertIsNotNone(f.sample())
        self.assertEqual(eligible_samples(f), 1)
        self.assertIsNone(f.ref)
        self.assertIsNone(f.future)
        self.assertEqual(len(self.refs), 1)

    def test_real_ray_exception_releases_pair_and_allows_one_new_optional_read(self):
        self.ray.get(self.actor.configure.remote(7, True), timeout=5)
        f = self.feedback
        self.assertIsNone(f.sample())
        self.release(0, failed=True)
        self.assertIsNone(f.sample())
        self.assertEqual(f.errors, 1)
        self.assertIsNone(f.ref)
        self.assertIsNone(f.future)
        self.ray.get(self.actor.configure.remote(7), timeout=5)
        self.assertIsNone(f.sample())
        self.release(1)
        self.assertIsNotNone(f.sample())
        self.assertEqual(len(self.refs), 2)
        self.assertTrue(all(len(ref.futures) == 1 for ref in self.refs))

    def test_expired_real_ray_request_is_cancelled_once_then_drained(self):
        f = self.feedback
        f.sample(); self.entered(1)
        f.expiry = time.monotonic()-1
        self.assertIsNone(f.sample())
        witness = self.refs[0].futures[0]
        with self.assertRaises(Exception): witness.result(timeout=5)
        self.assertIsNone(f.sample())
        self.assertEqual(self.cancels, [self.refs[0]])
        self.assertIsNone(f.ref)
        self.assertIsNone(f.future)
        self.assertEqual(eligible_samples(f), 0)

    def test_cancel_failure_retains_pair_across_query_until_old_read_settles(self):
        f = self.feedback
        f.sample(); self.entered(1)
        old = self.refs[0]
        self.cancel_error = True
        f.expiry = time.monotonic()-1
        self.assertIsNone(f.sample())
        self.assertEqual(f.errors, 1)
        f.pause(); bind_feedback(f, 8)
        self.ray.get(self.actor.configure.remote(8), timeout=5)
        for _ in range(5): self.assertIsNone(f.sample())
        self.assertEqual(self.refs, [old]); self.assertEqual(self.cancels, [old])
        self.assertEqual(len(old.futures), 1)
        self.release(0)
        self.assertIsNone(f.sample())
        self.assertIsNone(f.ref); self.assertIsNone(f.future)
        self.assertIsNone(f.sample())
        self.release(1)
        self.assertIsNotNone(f.sample())
        self.assertEqual(eligible_samples(f), 1)
        self.assertEqual(len(self.refs), 2)

    def test_wrong_query_snapshot_releases_pair_without_delivering_then_new_query_succeeds(self):
        f = self.feedback
        self.ray.get(self.actor.configure.remote(900), timeout=5)
        f.sample(); self.release(0)
        self.assertIsNone(f.sample())
        self.assertEqual(eligible_samples(f), 0)
        self.assertIsNone(f.ref); self.assertIsNone(f.future)
        self.ray.get(self.actor.configure.remote(7), timeout=5)
        f.sample(); self.release(1)
        self.assertIsNotNone(f.sample())
        self.assertEqual(eligible_samples(f), 1)
        self.assertTrue(all(len(ref.futures) == 1 for ref in self.refs))
