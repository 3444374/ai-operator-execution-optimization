"""Deterministic sampler epoch/RPC regressions; no actor or model allocation."""
from concurrent.futures import Future,ThreadPoolExecutor
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from src.execution_provider.adapters import lotus_records_core as core
from src.scheduling.runtime.observations import ServiceMetricsSnapshot,NonBlockingMetricsObservationProvider


class Ref:
    def __init__(self,value=None):
        self.value=Future()
        if value is not None:self.value.set_result(value)
    def future(self):return self.value


class Provider:
    is_running=False
    def __init__(self,*a,**kw):self.stale_after_s=kw['stale_after_s']
    def pause(self):pass
    def resume(self):pass
    def close(self):pass


class FeedbackLifetimeTests(unittest.TestCase):
    def feedback(self,gpu=False):
        self.refs=[];self.cancels=[]
        def remote():
            ref=Ref();self.refs.append(ref);return ref
        owner=SimpleNamespace(resources=SimpleNamespace(max_active_batches=4),
            _actors=SimpleNamespace(model_ready=dict(engine_uid='same-model'),
                model_actor=SimpleNamespace(snapshot=SimpleNamespace(remote=remote)),
                ray=SimpleNamespace(cancel=lambda ref,**kw:self.cancels.append(ref))))
        options=dict(kind='resource_feedback',poll_interval_s=.01)
        if gpu:options['gpu_ids']=['GPU-CPU-fixture']
        fb=core._CoreFeedback(owner,options)
        patcher=patch.object(core,'NonBlockingMetricsObservationProvider',Provider)
        patcher.start();self.addCleanup(patcher.stop)
        fb.begin(100,2);fb.stream=self.stream(1)
        return fb

    def stream(self,session):return SimpleNamespace(session=SimpleNamespace(session_id=session))

    def snapshot(self,session=1,stamp=None):
        return dict(session_id=session,ipc=dict(native_feedback=dict(engine_uid='same-model',
            sequence=1,observed_at_s=time.monotonic() if stamp is None else stamp,
            running=1,waiting=0,kv_usage=.1,preemptions_total=0,generation_tokens_total=2)))

    def test_expiring_rpc_is_cancelled_once_and_retained_until_it_settles(self):
        for cancel_fails in (False,True):
            with self.subTest(cancel_fails=cancel_fails):
                fb=self.feedback();self.assertIsNone(fb.sample());ref=fb.ref
                if cancel_fails:
                    def cancel(value,**kw):
                        self.cancels.append(value)
                        raise RuntimeError('CPU cancellation transport failure')
                    fb.owner._actors.ray.cancel=cancel
                fb.ref_deadline=time.monotonic()-1
                for _ in range(4):self.assertIsNone(fb.sample())
                self.assertEqual(self.cancels,[ref]);self.assertEqual(self.refs,[ref])
                self.assertEqual(fb.sample_errors,int(cancel_fails))
                fb.pause();fb.begin(100,2);fb.stream=self.stream(2)
                self.assertIsNone(fb.sample());self.assertEqual(self.refs,[ref])
                ref.value.set_result(self.snapshot())
                self.assertIsNone(fb.sample());self.assertIsNone(fb.ref)
                self.assertIsNone(fb.sample());self.assertEqual(len(self.refs),2)

    def test_wrong_session_or_pre_query_source_never_reaches_controller(self):
        fb=self.feedback()
        for value in (self.snapshot(session=17),self.snapshot(stamp=fb.started-1)):
            fb.sample();fb.ref.value.set_result(value)
            self.assertIsNone(fb.sample());self.assertEqual(fb.samples,0)
        fb.sample();fb.ref.value.set_result(self.snapshot())
        self.assertIsNotNone(fb.sample());self.assertEqual(fb.samples,1)

    def test_late_reader_success_and_failure_cannot_overwrite_next_query_rpc(self):
        for fail in (False,True):
            fb=self.feedback(gpu=True);fb.sample();fb.ref.value.set_result(self.snapshot())
            entered,release=threading.Event(),threading.Event()
            def read(*a,**kw):
                entered.set();release.wait(2)
                if fail:raise RuntimeError('CPU optional reader failure')
                return dict(gpu_memory_used_mib='1',gpu_memory_total_mib='10')
            with patch('src.observability.metrics.resources.gpu_metadata',side_effect=read),ThreadPoolExecutor(max_workers=1) as pool:
                old=pool.submit(fb.sample);self.assertTrue(entered.wait(1))
                old_stream=fb.stream;fb.finish(old_stream);fb.begin(100,3);fb.stream=self.stream(2)
                self.assertIsNone(fb.sample());new_ref=fb.ref
                release.set();self.assertIsNone(old.result(1))
                self.assertIs(fb.ref,new_ref);self.assertEqual(fb.sample_errors,0);self.assertEqual(fb.samples,0)
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
