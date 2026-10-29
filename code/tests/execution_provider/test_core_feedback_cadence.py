"""Admission cadence and pressure-only reuse, with no real model or hardware."""
from concurrent.futures import Future
from types import SimpleNamespace
import time
import unittest
from unittest.mock import patch
from src.execution_provider.adapters import lotus_records_core as core
from src.scheduling.core.models import AdmissionObservation
from src.scheduling.submission_control.adaptive import ResourceFeedbackConfig,ResourceFeedbackController
from src.scheduling.submission_control.admission import DynamicAdmissionGate
from src.scheduling.runtime.observations import NonBlockingMetricsObservationProvider,ServiceMetricsSnapshot


class Provider:
    is_running=False
    def __init__(self,*args,**kw):
        self.calls=0;self.now=0.;self.kv=.1;self.poll_interval_s=kw['poll_interval_s']
        self.stale_after_s=kw.get('stale_after_s',.5)
    def latest(self,inflight,**kw):
        self.calls+=1
        return AdmissionObservation(self.now,True,inflight,2,0,self.kv,
            completed_items_total=20,pending_items=100)
    def pause(self):pass
    def resume(self):pass
    def close(self):pass


class CadenceTests(unittest.TestCase):
    def make(self,total=100,initial=2,poll_interval_s=.1,**options):
        self.refs=[]
        def remote():
            result=Future();self.refs.append(result)
            result.set_result(dict(session_id=7,ipc=dict(native_feedback=dict(
                engine_uid='CPU-cadence',sequence=len(self.refs),observed_at_s=time.monotonic(),
                running=1,waiting=0,kv_usage=.1,preemptions_total=0,generation_tokens_total=4))))
            return SimpleNamespace(future=lambda:result)
        owner=SimpleNamespace(resources=SimpleNamespace(max_active_batches=8),_actors=SimpleNamespace(
            model_actor=SimpleNamespace(snapshot=SimpleNamespace(remote=remote)),
            model_ready=dict(engine_uid='CPU-cadence'),ray=SimpleNamespace(cancel=lambda *a,**kw:None)))
        with patch.object(core,'NonBlockingMetricsObservationProvider',Provider):
            f=core._CoreFeedback(owner,dict(poll_interval_s=poll_interval_s,window_s=1,**options));f.begin(total,initial)
        f.stream=SimpleNamespace(session=SimpleNamespace(session_id=7))
        return f

    def test_many_admission_checks_reuse_window_but_recheck_current_inflight(self):
        f=self.make();f.gate.clock=lambda:f.provider.now
        allowed=[]
        for i in range(100):allowed.append(f.gate.decide(i%3).allowed)
        self.assertEqual(f.provider.calls,1)
        self.assertEqual(f.controller.report()['decision_calls'],1)
        self.assertEqual(allowed,[i%3<2 for i in range(100)])
        f.provider.now=.11
        f.gate.decide(1)
        self.assertEqual(f.provider.calls,2)

    def test_query_restart_has_no_cadence_or_window_left_from_previous_query(self):
        f=self.make();f.gate.clock=lambda:0.
        f.gate.decide(0);before=f.provider.calls
        f.pause();f.begin(100,4);f.stream=SimpleNamespace(session=SimpleNamespace(session_id=8))
        self.assertTrue(f.gate.decide(3).allowed)
        self.assertEqual(f.provider.calls,before+1)
        self.assertFalse(f.gate.decide(4).allowed)
        self.assertEqual(f.provider.calls,before+1)

    def test_explicit_legacy_tail_capacity_and_fixed_use_pressure_polling_after_response(self):
        for total,pending,initial,extra in [(3,3,2,{}),(100,2,2,dict(allow_exploration=True,min_remaining=4)),
                (100,90,8,dict(allow_exploration=True)),(100,90,2,{})]:
            with self.subTest(total=total,pending=pending,initial=initial,extra=extra):
                f=self.make(total,initial,**extra);f.progress=(0,pending,0,False)
                self.assertIsNotNone(f.sample())
                self.assertEqual(f.provider.poll_interval_s,1.)
                self.assertEqual(len(self.refs),1)

    def test_unbudgeted_feedback_does_not_treat_small_pending_count_as_a_tail_cutoff(self):
        f=self.make(100,2,allow_exploration=True);f.progress=(0,2,0,False)
        self.assertIsNotNone(f.sample())
        self.assertEqual(f.provider.poll_interval_s,.1)

    def test_explicit_growth_keeps_faster_polling_and_pending_rpc_is_never_slowed(self):
        f=self.make(100,2,allow_exploration=True);f.progress=(0,90,0,False)
        self.assertIsNotNone(f.sample());self.assertEqual(f.provider.poll_interval_s,.1)
        f.provider.poll_interval_s=1.
        result=Future()
        f.owner._actors.model_actor.snapshot.remote=lambda:SimpleNamespace(future=lambda:result)
        for _ in range(10):self.assertIsNone(f.sample())
        self.assertEqual(f.provider.poll_interval_s,.1)
        self.assertFalse(result.done())

    def test_default_still_shrinks_under_pressure_without_exploring(self):
        options=ResourceFeedbackConfig(window_s=1)
        self.assertFalse(options.allow_exploration)
        now=[0.]
        c=ResourceFeedbackController(8,4,options,clock=lambda:now[0]);c.reset(4,100)
        for n in range(1,4):
            now[0]=n
            c.update(AdmissionObservation(n,True,4,4,0,.99,completed_items_total=n*8,pending_items=90))
        self.assertLess(c.current_window,4)
        self.assertTrue(all(e['reason']=='resource_pressure' for e in c.history))

    def test_protection_poll_loop_issues_one_snapshot_per_window(self):
        f=self.make();f.progress=(0,90,0,False)
        elapsed=0.
        while elapsed<1.:
            f.sample()
            elapsed+=f.provider.poll_interval_s
        self.assertEqual(len(self.refs),1)
        self.assertEqual(f.snapshot_requests,1)

    def test_generic_gate_keeps_legacy_unthrottled_behavior_without_core_option(self):
        provider=Provider(poll_interval_s=.1);c=ResourceFeedbackController(8,2,ResourceFeedbackConfig())
        gate=DynamicAdmissionGate(c,provider)
        for _ in range(10):gate.decide(0)
        self.assertEqual(provider.calls,10)

    def test_delayed_pressure_publication_survives_gate_phase_difference(self):
        f=self.make(initial=8,poll_interval_s=.25);now=[0.]
        provider=NonBlockingMetricsObservationProvider(lambda:None,poll_interval_s=100,
            stale_after_s=f.provider.stale_after_s,clock=lambda:now[0],close_timeout_s=1)
        try:
            self.assertTrue(provider.wait_until_sampled(1));provider.pause()
            f.gate.observation_provider=provider;f.gate.clock=lambda:now[0]
            f.gate.trace_sink=None  # No Core tasks exist in this publication fixture.
            f.controller.clock=lambda:now[0];f.controller.reset(8,100)
            deliveries=[.50+1.25*i for i in range(8)];next_delivery=0
            for i in range(40):
                now[0]=.20+.25*i
                while next_delivery<len(deliveries) and deliveries[next_delivery]<=now[0]:
                    observed=deliveries[next_delivery]-.49
                    # Publish a received source frame; actual provider freshness,
                    # consume-once generation and gate/controller are exercised.
                    with provider._lock:
                        provider._snapshot=ServiceMetricsSnapshot(8,0,.99,
                            source_observed_at_s=observed,source_sequence=next_delivery+1,
                            completed_items_total=(next_delivery+1)*8,pending_items=90,
                            preemptions_total=0)
                        provider._sampled_at_s=observed
                        provider._sample_generation+=1
                    next_delivery+=1
                f.gate.decide(8)
            self.assertEqual(next_delivery,8)
            self.assertEqual(f.controller.current_window,1)
            self.assertTrue(all(e['reason']=='resource_pressure' for e in f.controller.history))
        finally:provider.close()

    def test_pressure_age_allowance_still_expires_and_consumes_each_frame_once(self):
        f=self.make(poll_interval_s=.25);now=[0.]
        provider=NonBlockingMetricsObservationProvider(lambda:None,poll_interval_s=100,
            stale_after_s=f.provider.stale_after_s,clock=lambda:now[0],close_timeout_s=1)
        try:
            self.assertTrue(provider.wait_until_sampled(1));provider.pause()
            with provider._lock:
                provider._snapshot=ServiceMetricsSnapshot(8,0,.99,source_sequence=1,source_observed_at_s=0.)
                provider._sampled_at_s=0.;provider._sample_generation+=1
            self.assertLessEqual(provider.stale_after_s,.75)
            now[0]=provider.stale_after_s-.01
            self.assertTrue(provider.latest(8).fresh)
            self.assertFalse(provider.latest(8).fresh)
            with provider._lock:provider._sample_generation+=1
            now[0]=provider.stale_after_s+.01
            stale=provider.latest(8)
            self.assertFalse(stale.fresh)
            self.assertGreater(stale.sample_age_s,provider.stale_after_s)
        finally:provider.close()


class CachedProtectionTests(unittest.TestCase):
    def test_exact_cache_hit_applies_capacity_without_exploring_or_reprocessing(self):
        from pathlib import Path
        import tempfile
        from src.scheduling.runtime import calibration as runtime
        from src.semantic_methods.lotus.records import FilterMethod,RecordsResources
        from src.serving.backends.vllm_engine import EngineSpec,GenerationSpec
        from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor
        from tests.execution_provider.test_lotus_records_async_core import ThreadedRuntime
        from tests.execution_provider.test_lotus_records_core import Source
        from tests.serving.test_native_resource_feedback import CPUFeedbackProcess
        rows=[dict(row_id='r'+str(i),source_position=i,reviewId='review-'+str(i),id='taken_3',reviewText='fast') for i in range(48)]
        profile=dict(mode='async',max_inflight=1,admission=dict(kind='resource_feedback',
            poll_interval_s=.003,window_s=.01,min_samples=2,min_remaining=4,exploration_s=4))
        identity=dict(hardware=dict(fixture=True),model=dict(revision='CPU-only'),generation=dict(fixed=True),
            execution=dict(policy='pressure-default-cadence-v2',capacity=4),
            workload=dict(rows=48),measurement=dict(work_unit='rows',objective='synthetic-complete-query'))
        candidates=[dict(max_inflight=1),dict(max_inflight=2)]
        with tempfile.TemporaryDirectory() as tmp,ThreadedRuntime(rows) as fake, \
                patch.object(AsyncCoreModelActor,'_process_type',return_value=CPUFeedbackProcess):
            owner=core.create_records_core(FilterMethod('fixture', 'Evaluate {reviewText}'),
                EngineSpec('/tmp/CPU-fixture','/tmp/model-python','/tmp/driver-python',max_num_seqs=8),
                GenerationSpec(),RecordsResources(cpu_workers=2,prepare_batch_rows=1,model_batch_rows=1,
                    max_active_batches=4,startup_timeout_s=5,query_timeout_s=10),profile=profile)
            self.assertFalse(owner.execution_profile['admission']['allow_exploration'])
            reports=[];values=[];measurements=[]
            def execute(configuration):
                stream=owner.evaluate(Source(rows),query_id='cache-'+str(len(reports)),max_inflight=configuration['max_inflight'])
                result=list(stream);self.assertEqual(len(result),48)
                self.assertEqual(len({r['call_id'] for r in result}),48)
                reports.append(stream.last_close_report['admission_feedback']);values.append(result)
                return result
            def measure(configuration,**kw):
                measurements.append(configuration)
                return runtime.Measurement(10/configuration['max_inflight'],'ok',True,48)
            common=dict(default=candidates[0],cache_dir=Path(tmp),repetitions=1,work_per_sample=48)
            try:
                miss=runtime.run_query(identity,candidates,execute,measure=measure,**common)
                self.assertEqual(miss['configuration_source'],'explicit_default');self.assertEqual(measurements,[])
                runtime.run_query(identity,candidates,execute,mode='offline',measure=measure,
                    budget=runtime.ScanBudget(5,96),**common)
                hit=runtime.run_query(identity,candidates,execute,measure=measure,**common)
                self.assertEqual(hit['configuration_source'],'calibration_cache')
                self.assertEqual(len(measurements),2)
                self.assertEqual([r['window'] for r in reports],[1,2])
                self.assertTrue(all(r['changes']==0 and not r['exploration_enabled'] for r in reports))
                self.assertEqual(fake.async_actors[0].instance.engine._sequence,96)
                self.assertEqual(len(fake.async_actors),1)
                self.assertEqual(owner.core.capacity.usage().held_tasks,0)
                self.assertEqual(owner.transport.retained_refs(),0)
            finally:self.assertEqual(owner.close(timeout=3)['status'],'closed')
