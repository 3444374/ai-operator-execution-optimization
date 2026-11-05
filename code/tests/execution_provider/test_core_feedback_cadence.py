"""Current sampler cadence and shared batch/profiling feedback behavior; CPU only."""
from concurrent.futures import Future
from types import SimpleNamespace
import unittest

from src.scheduling.core.models import AdmissionObservation
from src.scheduling.submission_control.adaptive import ResourceFeedbackConfig,ResourceFeedbackController
from src.scheduling.submission_control.admission import DynamicAdmissionGate
from src.scheduling.runtime.observations import NonBlockingMetricsObservationProvider,ServiceMetricsSnapshot
from tests.execution_provider.test_core_feedback_ray_future import make_feedback,native_frame


class Provider:
    def __init__(self,*,poll_interval_s):
        self.calls=0;self.now=0.;self.kv=.1;self.poll_interval_s=poll_interval_s
    def latest(self,inflight,**kw):
        self.calls+=1
        return AdmissionObservation(self.now,True,inflight,2,0,self.kv,
            completed_items_total=20,pending_items=100)


class CadenceTests(unittest.TestCase):
    def test_sample_response_keeps_pressure_polling_independent_of_performance_window(self):
        def remote():
            result=Future()
            result.set_result(native_frame(uid='CPU-cadence'))
            return SimpleNamespace(future=lambda:result)
        owner=SimpleNamespace(resources=SimpleNamespace(max_active_batches=4),_actors=SimpleNamespace(
            model_actor=SimpleNamespace(snapshot=SimpleNamespace(remote=remote)),
            model_ready=dict(engine_uid='CPU-cadence'),ray=SimpleNamespace(cancel=lambda *a,**kw:None)))
        feedback=make_feedback(owner,poll_interval_s=.1)
        self.addCleanup(feedback.stop)
        self.assertIsNotNone(feedback.sample())
        self.assertEqual(feedback.provider.poll_interval_s,.1)
        self.assertEqual(feedback.options.window_s,1.)
        self.assertEqual(feedback.requests,1)

    def test_default_still_shrinks_under_pressure_without_exploring(self):
        options=ResourceFeedbackConfig(window_s=1)
        self.assertFalse(options.allow_exploration)
        now=[0.]
        controller=ResourceFeedbackController(8,4,options,clock=lambda:now[0]);controller.reset(4,100)
        for n in range(1,4):
            now[0]=n
            controller.update(AdmissionObservation(n,True,4,4,0,.99,completed_items_total=n*8,pending_items=90))
        self.assertLess(controller.current_window,4)
        self.assertTrue(all(e['reason']=='resource_pressure' for e in controller.history))

    def test_generic_gate_uses_each_provider_observation_by_default(self):
        provider=Provider(poll_interval_s=.1)
        controller=ResourceFeedbackController(8,2,ResourceFeedbackConfig())
        gate=DynamicAdmissionGate(controller,provider)
        for _ in range(10):gate.decide(0)
        self.assertEqual(provider.calls,10)

    def test_delayed_pressure_publication_survives_gate_phase_difference(self):
        now=[0.]
        provider=NonBlockingMetricsObservationProvider(lambda:None,poll_interval_s=100,
            stale_after_s=.75,clock=lambda:now[0],close_timeout_s=1)
        controller=ResourceFeedbackController(8,8,ResourceFeedbackConfig(window_s=1),
            source_stale_after_s=.75,clock=lambda:now[0])
        gate=DynamicAdmissionGate(controller,provider,clock=lambda:now[0])
        try:
            self.assertTrue(provider.wait_until_sampled(1));provider.pause()
            controller.reset(8,100)
            deliveries=[.50+1.25*i for i in range(8)];next_delivery=0
            for i in range(40):
                now[0]=.20+.25*i
                while next_delivery<len(deliveries) and deliveries[next_delivery]<=now[0]:
                    observed=deliveries[next_delivery]-.49
                    with provider._lock:
                        provider._snapshot=ServiceMetricsSnapshot(8,0,.99,
                            source_observed_at_s=observed,source_sequence=next_delivery+1,
                            completed_items_total=(next_delivery+1)*8,pending_items=90,
                            preemptions_total=0)
                        provider._sampled_at_s=observed
                        provider._sample_generation+=1
                    next_delivery+=1
                gate.decide(8)
            self.assertEqual(next_delivery,8)
            self.assertEqual(controller.current_window,1)
            self.assertTrue(all(e['reason']=='resource_pressure' for e in controller.history))
        finally:provider.close()

    def test_pressure_age_allowance_still_expires_and_consumes_each_frame_once(self):
        now=[0.]
        provider=NonBlockingMetricsObservationProvider(lambda:None,poll_interval_s=100,
            stale_after_s=.75,clock=lambda:now[0],close_timeout_s=1)
        try:
            self.assertTrue(provider.wait_until_sampled(1));provider.pause()
            with provider._lock:
                provider._snapshot=ServiceMetricsSnapshot(8,0,.99,source_sequence=1,source_observed_at_s=0.)
                provider._sampled_at_s=0.;provider._sample_generation+=1
            now[0]=provider.stale_after_s-.01
            self.assertTrue(provider.latest(8).fresh)
            self.assertFalse(provider.latest(8).fresh)
            with provider._lock:provider._sample_generation+=1
            now[0]=provider.stale_after_s+.01
            stale=provider.latest(8)
            self.assertFalse(stale.fresh)
            self.assertGreater(stale.sample_age_s,provider.stale_after_s)
        finally:provider.close()
