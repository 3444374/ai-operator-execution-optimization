"""Deterministic sampler epoch/RPC regressions; no actor or model allocation."""
from concurrent.futures import Future,ThreadPoolExecutor
import threading
import time
from types import SimpleNamespace
import unittest
from src.scheduling.runtime.observations import ServiceMetricsSnapshot,NonBlockingMetricsObservationProvider
from tests.execution_provider.test_core_feedback_ray_future import (
    bind_feedback, eligible_samples, make_feedback, native_frame,
)


class Ref:
    def __init__(self,value=None):
        self.value=Future()
        if value is not None:self.value.set_result(value)
    def future(self):return self.value


class FeedbackLifetimeTests(unittest.TestCase):
    def feedback(self):
        self.refs=[];self.cancels=[];self.sequence=0
        def remote():
            ref=Ref();self.refs.append(ref);return ref
        owner=SimpleNamespace(resources=SimpleNamespace(max_active_batches=4),
            _actors=SimpleNamespace(model_ready=dict(engine_uid='same-model'),
                model_actor=SimpleNamespace(snapshot=SimpleNamespace(remote=remote)),
                ray=SimpleNamespace(cancel=lambda ref,**kw:self.cancels.append(ref))))
        fb=make_feedback(owner,session=1,poll_interval_s=.01,max_sample_age_s=.5)
        self.addCleanup(fb.stop)
        return fb

    def snapshot(self,session=1,stamp=None):
        self.sequence+=1
        return native_frame(session=session,uid='same-model',sequence=self.sequence,source_at_s=stamp)

    def test_expiring_rpc_is_cancelled_once_and_retained_until_it_settles(self):
        for cancel_fails in (False,True):
            with self.subTest(cancel_fails=cancel_fails):
                fb=self.feedback();self.assertIsNone(fb.sample());ref=fb.ref
                if cancel_fails:
                    def cancel(value,**kw):
                        self.cancels.append(value)
                        raise RuntimeError('CPU cancellation transport failure')
                    fb.owner._actors.ray.cancel=cancel
                fb.expiry=time.monotonic()-1
                for _ in range(4):self.assertIsNone(fb.sample())
                self.assertEqual(self.cancels,[ref]);self.assertEqual(self.refs,[ref])
                self.assertEqual(fb.errors,int(cancel_fails))
                fb.pause();bind_feedback(fb,2)
                self.assertIsNone(fb.sample());self.assertEqual(self.refs,[ref])
                ref.value.set_result(self.snapshot())
                self.assertIsNone(fb.sample());self.assertIsNone(fb.ref)
                self.assertIsNone(fb.sample());self.assertEqual(len(self.refs),2)

    def test_wrong_session_or_pre_query_source_never_reaches_controller(self):
        fb=self.feedback()
        for value in (self.snapshot(session=17),self.snapshot(stamp=fb.started-1)):
            fb.sample();fb.ref.value.set_result(value)
            self.assertIsNone(fb.sample());self.assertEqual(eligible_samples(fb),0)
        fb.sample();fb.ref.value.set_result(self.snapshot())
        self.assertIsNotNone(fb.sample());self.assertEqual(eligible_samples(fb),1)

    def test_late_optional_audit_cannot_overwrite_next_query_rpc(self):
        for fail in (False,True):
            fb=self.feedback();fb.sample();fb.ref.value.set_result(self.snapshot())
            entered,release=threading.Event(),threading.Event()
            def audit(*a,**kw):
                entered.set();release.wait(2)
                if fail:raise RuntimeError('CPU optional audit failure')
            fb.provider.observe_native=audit
            with ThreadPoolExecutor(max_workers=1) as pool:
                old=pool.submit(fb.sample);self.assertTrue(entered.wait(1))
                old_stream=fb.stream;fb.finish(old_stream);bind_feedback(fb,2)
                self.assertIsNone(fb.sample());new_ref=fb.ref
                release.set();self.assertIsNone(old.result(1))
                self.assertIs(fb.ref,new_ref);self.assertEqual(fb.errors,0)
                self.assertIsNone(fb.last_native)
            fb.stop()

    def test_pause_resume_discards_inflight_sample_and_reuses_one_thread(self):
        entered,release,second=threading.Event(),threading.Event(),threading.Event()
        calls=[]
        def sample():
            calls.append(1)
            if len(calls)==1:
                entered.set();release.wait(1)
                return ServiceMetricsSnapshot(8,4,.99,source_sequence=1,source_observed_at_s=time.monotonic())
            second.wait(1)
            return ServiceMetricsSnapshot(1,0,.1,source_sequence=2,source_observed_at_s=time.monotonic())
        provider=NonBlockingMetricsObservationProvider(sample,poll_interval_s=.01,close_timeout_s=1)
        thread=provider._thread
        try:
            self.assertTrue(entered.wait(1));provider.pause();provider.resume();release.set()
            provider.wait_until_sampled(1);self.assertFalse(provider.latest(1).fresh)
            self.assertIs(provider._thread,thread)
        finally:second.set();provider.close()
        self.assertFalse(provider.is_running)
