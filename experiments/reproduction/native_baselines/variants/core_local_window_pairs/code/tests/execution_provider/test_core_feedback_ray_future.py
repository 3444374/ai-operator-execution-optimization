"""Real Ray ObjectRef/Future sampling, in a private CPU-only driver.

The proxy counts calls but returns Ray's actual new concurrent Future each time.
A constant-Future stub would miss the production notification loss.
"""
import os
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.execution_provider.adapters import lotus_records_core as core


class IdleProvider:
    is_running = False
    def __init__(self, *args, **kwargs): pass
    def pause(self): pass
    def resume(self): pass
    def close(self): pass


class CountedRef:
    def __init__(self, ref):
        self.raw = ref
        self.futures = []
    def future(self):
        value = self.raw.future()
        self.futures.append(value)
        return value


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
        cls.directory = tempfile.TemporaryDirectory(prefix='rff-', dir='/tmp')
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
                value = dict(session_id=self.session, ipc=dict(native_feedback=dict(
                    engine_uid='real-Ray-CPU-fixture', sequence=index+1, observed_at_s=time.monotonic(),
                    running=1, waiting=0, kv_usage=.1, preemptions_total=0, generation_tokens_total=4)))
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
        with patch.object(core, 'NonBlockingMetricsObservationProvider', IdleProvider):
            self.feedback = core._CoreFeedback(owner, dict(poll_interval_s=.25))
            self.feedback.begin(100, 2)
        self.feedback.stream = self.stream(7)

    def tearDown(self):
        self.feedback.stop()
        self.ray.kill(self.actor, no_restart=True)

    @staticmethod
    def stream(session):
        return SimpleNamespace(session=SimpleNamespace(session_id=session))

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
        self.assertEqual(f.samples, 1)
        self.assertIsNone(f.ref)
        self.assertIsNone(f.future)
        self.assertEqual(len(self.refs), 1)

    def test_real_ray_exception_releases_pair_and_allows_one_new_optional_read(self):
        self.ray.get(self.actor.configure.remote(7, True), timeout=5)
        f = self.feedback
        self.assertIsNone(f.sample())
        self.release(0, failed=True)
        self.assertIsNone(f.sample())
        self.assertEqual(f.sample_errors, 1)
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
        f.ref_deadline = time.monotonic()-1
        self.assertIsNone(f.sample())
        witness = self.refs[0].futures[0]
        with self.assertRaises(Exception): witness.result(timeout=5)
        self.assertIsNone(f.sample())
        self.assertEqual(self.cancels, [self.refs[0]])
        self.assertIsNone(f.ref)
        self.assertIsNone(f.future)
        self.assertEqual(f.samples, 0)

    def test_cancel_failure_retains_pair_across_query_until_old_read_settles(self):
        f = self.feedback
        f.sample(); self.entered(1)
        old = self.refs[0]
        self.cancel_error = True
        f.ref_deadline = time.monotonic()-1
        self.assertIsNone(f.sample())
        self.assertEqual(f.sample_errors, 1)
        f.pause(); f.begin(100, 3); f.stream = self.stream(8)
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
        self.assertEqual(f.samples, 1)
        self.assertEqual(len(self.refs), 2)

    def test_wrong_query_snapshot_releases_pair_without_delivering_then_new_query_succeeds(self):
        f = self.feedback
        self.ray.get(self.actor.configure.remote(900), timeout=5)
        f.sample(); self.release(0)
        self.assertIsNone(f.sample())
        self.assertEqual(f.samples, 0)
        self.assertIsNone(f.ref); self.assertIsNone(f.future)
        self.ray.get(self.actor.configure.remote(7), timeout=5)
        f.sample(); self.release(1)
        self.assertIsNotNone(f.sample())
        self.assertEqual(f.samples, 1)
        self.assertTrue(all(len(ref.futures) == 1 for ref in self.refs))
