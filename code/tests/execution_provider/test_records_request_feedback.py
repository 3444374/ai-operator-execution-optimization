"""Public policy selection and actual Core/CPU IPC resource ownership."""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import json
from pathlib import Path
import sys
import time
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.lotus_records_core import create_records_core
from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor
from src.execution_provider.adapters.records_profile import resolve_core_profile
from src.execution_provider.adapters.records_request_feedback import (
    CoreResourceRequestFeedback, ResourceRequestSupplyFeedback, applied_request_limit,
)
from src.scheduling.submission_control.adaptive import ResourceRequestConfig
from src.serving.backends.async_engine_process import AsyncEngineProcess
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
from src.scheduling.runtime.observations import NonBlockingMetricsObservationProvider, ServiceMetricsSnapshot
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources
from tests.execution_provider import test_lotus_async_token_batches as base
from tests.execution_provider.test_lotus_records_core import Source
from tests.execution_provider.test_lotus_records_async_core import ThreadedRuntime
from tests.execution_provider import test_lotus_token_rolling_refill as rolling


class CPUResourceProcess(AsyncEngineProcess):
    @staticmethod
    def _command(spec, descriptors):
        return [sys.executable, str(Path(__file__).with_name('resource_request_child_fixture.py')),
                *map(str, descriptors)]


class RequestPolicyWiringTests(unittest.TestCase):
    setUp = base.AsyncTokenBatchTests.setUp
    tick_until = rolling.RollingRefillTests.tick_until

    def test_required_kernel_memory_source_reaches_rolling_public_owner(self):
        from src.observability.metrics import memory_pressure
        from tests.observability.memory_pressure_fixture import KernelMemoryFixture
        plan, rows = base.fixture.chain_for('M'), base.fixture.source_rows(size=4)
        gate = Path(str(self.helper.config)+'.memory-release')
        config = self.helper.config_for(plan, rows)
        config['responses']['step_0|occ-0']['wait_file'] = str(gate)
        self.helper.config.write_text(json.dumps(config))
        options = ResourceRequestConfig(require_memory_feedback=True,allow_exploration=False,
            poll_interval_s=.01,window_s=.04,max_sample_age_s=.3,min_samples=1,measurement_windows=2)
        with KernelMemoryFixture() as kernel, ThreadedRuntime([]), \
                patch.object(AsyncCoreModelActor,'_process_type',return_value=CPUResourceProcess), \
                patch.object(memory_pressure,'CgroupMemoryPressureSource',return_value=kernel.source):
            kernel.source.clock=time.monotonic
            owner = base.AsyncTokenBatchTests.owner(self,plan,R=4,C=None,rolling_refill=True,request_feedback=options)
            stream = owner.evaluate(iter(rows),plan,expected=rows,query_id=plan.query_id)
            try:
                self.tick_until(stream,lambda:owner._supply_feedback is not None
                    and getattr(owner._supply_feedback,'memory_pressure_binding',None) is not None
                    and owner._supply_feedback.memory_pressure_binding.report()['sample'] is not None
                    and owner._supply_feedback.memory_pressure_binding.report()['sample']['memory_pressure'] is False)
                guard=owner._supply_feedback.memory_pressure_binding.report()
                self.assertEqual(guard['engine_uid'],owner._actors.model_ready['engine_uid'])
                self.assertEqual(guard['session_id'],stream.session.session_id)
                gate.touch()
                self.assertEqual(len(list(stream)),4)
                report=owner.last_close_report
                self.assertTrue(report['request_supply_feedback']['memory_feedback']['enabled'])
                self.assertEqual(report['core_usage']['held_tasks'],0)
            finally:
                gate.touch();owner.close(timeout=3)

    def test_required_kernel_memory_source_rebinds_ordinary_resident_query(self):
        from src.observability.metrics import memory_pressure
        from tests.observability.memory_pressure_fixture import KernelMemoryFixture
        raw = [dict(row_id='row-'+str(i),source_position=i,reviewId=str(i),id='taken_3',reviewText='fast') for i in range(3)]
        gate=Path(str(self.helper.config)+'.ordinary-memory-release')
        self.helper.config.write_text(json.dumps(dict(responses={
            'filter|'+r['row_id']:dict(text='True',wait_file=str(gate)) for r in raw})))
        engine=EngineSpec(str(self.helper.config),sys.executable,sys.executable,max_model_len=4096)
        resources=RecordsResources(prepare_batch_rows=1,model_batch_rows=1,max_active_batches=3,
            query_timeout_s=10,startup_timeout_s=8)
        profile=dict(mode='async',admission=dict(kind='resource_feedback',require_memory_feedback=True,
            allow_exploration=False,poll_interval_s=.01,window_s=.04,max_sample_age_s=.3,
            min_samples=1,measurement_windows=2))
        with KernelMemoryFixture() as kernel,ThreadedRuntime(raw) as runtime, \
                patch.object(AsyncCoreModelActor,'_process_type',return_value=CPUResourceProcess), \
                patch.object(memory_pressure,'CgroupMemoryPressureSource',return_value=kernel.source):
            kernel.source.clock=time.monotonic
            owner=create_records_core(FilterMethod('fixture',instruction='Keep {reviewText}'),engine,
                GenerationSpec(),resources,profile=profile)
            guards=[]
            try:
                for query in ('memory-one','memory-two'):
                    if gate.exists():gate.unlink()
                    stream=owner.evaluate(Source(raw),query_id=query)
                    def release_after_memory():
                        try:
                            deadline=time.monotonic()+5
                            while True:
                                current=getattr(owner._feedback,'memory_pressure_binding',None)
                                guard=None if current is None else current.report()
                                if (guard is not None and guard['sample'] is not None
                                        and guard['sample']['memory_pressure'] is False
                                        and guard['session_id'] not in {g['session_id'] for g in guards}):
                                    return guard
                                self.assertLess(time.monotonic(),deadline,guard)
                                time.sleep(.001)
                        finally:gate.touch()
                    with ThreadPoolExecutor(max_workers=1) as observer:
                        proof=observer.submit(release_after_memory)
                        self.assertEqual(len(list(stream)),3)
                        guards.append(proof.result(timeout=1))
                    self.assertEqual(guards[-1]['session_id'],stream.session.session_id)
                    self.assertTrue(stream.last_close_report['admission_feedback']['memory_feedback']['enabled'])
                    self.assertEqual(owner.core.capacity.usage().held_tasks,0)
                self.assertNotEqual(guards[0]['session_id'],guards[1]['session_id'])
                self.assertEqual(guards[0]['engine_uid'],guards[1]['engine_uid'])
                self.assertEqual(len(runtime.async_actors),1)
            finally:
                gate.touch();owner.close(timeout=3)

    def test_observation_and_permission_keep_the_same_source_during_new_publication(self):
        first = ServiceMetricsSnapshot(1, 0, .96, source_sequence=1, source_uid='same-owner',
                                      source_session_id=7, source_observed_at_s=time.monotonic())
        second = replace(first, running=2, source_sequence=2)
        provider = NonBlockingMetricsObservationProvider(lambda:first, poll_interval_s=10, close_timeout_s=1)
        self.addCleanup(provider.close)
        self.assertTrue(provider.wait_until_sampled(1))
        def publish(event):
            if event['stage'] == 'consumer':
                provider._sample_once(lambda:second)
        provider.observer = publish
        observation, source = provider.latest_with_source(1)
        self.assertEqual((observation.running, source.source_sequence), (1, 1))
        self.assertEqual(provider._snapshot.source_sequence, 2)

    def test_applied_result_bytes_limit_the_trial_without_relabelling_R_as_capacity(self):
        plan, rows = base.fixture.chain_for('M'), base.fixture.source_rows(size=3)
        self.helper.config_for(plan, rows)
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, '_process_type', return_value=CPUResourceProcess):
            owner = base.AsyncTokenBatchTests.owner(self, plan, R=4, C=None, rolling_refill=True,
                                                    request_feedback=ResourceRequestConfig())
            owner._actors.limits = replace(owner._actors.limits, item_result_bytes=65536, result_bytes=65536)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                self.assertEqual(len(list(stream)), 3)
                report = owner.last_close_report
                self.assertEqual(report['async_token_batches']['request_limit'], 4)
                self.assertEqual(report['request_supply_feedback']['max_requests'], 1)
                self.assertLessEqual(report['async_token_batches']['peak_requests'], 1)
                self.assertTrue(all(p['maximum']==1 and p['physical_capacity_known'] is False
                                    for p in report['request_supply_feedback']['phases']))
            finally:owner.close(timeout=3)

    def test_ordinary_empty_query_has_no_model_or_synthetic_resource_identity(self):
        self.helper.config.write_text(json.dumps(dict(responses={})))
        engine = EngineSpec(str(self.helper.config), sys.executable, sys.executable, max_model_len=4096)
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1, max_active_batches=3)
        with ThreadedRuntime([]) as runtime:
            owner = create_records_core(FilterMethod('fixture', instruction='Keep {reviewText}'),
                engine, GenerationSpec(), resources,
                profile=dict(mode='async', admission=dict(kind='resource_feedback')))
            try:
                stream = owner.evaluate(Source([]), query_id='empty')
                self.assertEqual(list(stream), [])
                self.assertFalse(runtime.async_actors)
                report = stream.last_close_report['admission_feedback']
                self.assertTrue(report['empty_query'])
                self.assertEqual(report['snapshot_requests'], 0)
                self.assertIsNone(owner._feedback.session_id)
            finally:owner.close(timeout=3)

    def test_public_defaults_select_the_reviewed_policy_and_reject_legacy_thresholds(self):
        engine = EngineSpec('/tmp/model', sys.executable, sys.executable)
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1, max_active_batches=4)
        for rolling in (False, True):
            selected = resolve_core_profile(engine, resources,
                dict(mode='async', admission=dict(kind='resource_feedback')), rolling_refill=rolling)
            options = selected['admission']
            self.assertEqual(selected['max_inflight'], 1)
            self.assertEqual(selected['owner_max_inflight'], 4)
            self.assertNotIn('pressure_kv_usage', options)
            self.assertNotIn('search_policy', options)
            self.assertEqual(options['measurement_windows'], 3)
            for forbidden in (dict(high_kv_usage=.85), dict(physical_probe_policy='idle_majority'),
                              dict(search_policy='ascending')):
                with self.subTest(rolling=rolling, forbidden=forbidden), self.assertRaises(ValueError):
                    resolve_core_profile(engine, resources,
                        dict(mode='async', admission=dict(kind='resource_feedback', **forbidden)),
                        rolling_refill=rolling)
        legacy = resolve_core_profile(engine, replace(resources, max_active_batches=8), 'computed-work-local-pairs')
        self.assertEqual(legacy['admission']['kind'], 'legacy_resource_feedback')
        self.assertEqual(legacy['admission']['work_basis'], 'computed')
        self.assertEqual(legacy['admission']['max_changes'], 8)

    def test_rolling_uses_ready_source_and_grows_past_old_KV_cutoff_with_existing_credit(self):
        plan, rows = base.fixture.chain_for('MM'), base.fixture.source_rows(size=4)
        gate = Path(str(self.helper.config)+'.release')
        config = self.helper.config_for(plan, rows)
        config['responses']['step_0|occ-0']['wait_file'] = str(gate)
        self.helper.config.write_text(json.dumps(config))
        options = ResourceRequestConfig(poll_interval_s=.01, window_s=.04, max_sample_age_s=.3,
                                        min_samples=1, measurement_windows=2)
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, '_process_type', return_value=CPUResourceProcess):
            owner = base.AsyncTokenBatchTests.owner(self, plan, R=4, C=None, rolling_refill=True,
                                                    request_feedback=options)
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                self.tick_until(stream, lambda: owner._supply_feedback.controller is not None
                    and owner._token_organizer.request_window > 1)
                feedback = owner._supply_feedback
                self.assertIsInstance(feedback, ResourceRequestSupplyFeedback)
                self.assertEqual(feedback.controller.strategy, 'resource_request_v1')
                self.assertEqual(applied_request_limit(owner), 4)
                self.assertTrue(feedback.controller.step_decisions)
                self.assertGreater(feedback.last_native.kv_usage, .9)
                self.assertIsNone(feedback.controller.export_hint())
                self.tick_until(stream, lambda:any(c['algorithm_stage']=='step_1' and c['row_id']=='occ-1'
                                                       for c in owner.calls))
                self.assertFalse(any(e['event']=='end' and e.get('token')=='step_0|occ-0' for e in self.helper.events()))
                gate.touch()
                self.assertEqual(len(list(stream)), 4)
                report = owner.last_close_report
                self.assertEqual(report['core_usage']['held_tasks'], 0)
                phases = report['request_supply_feedback']['phases']
                self.assertTrue(all(p['strategy']=='resource_request_v1' for p in phases))
                self.assertFalse(any(p['cross_query_hints_supported'] for p in phases))
            finally:
                gate.touch(); owner.close(timeout=3)

    def test_ordinary_public_factory_runs_repeated_queries_with_new_session_and_one_model_owner(self):
        raw = [dict(row_id='row-'+str(i), source_position=i, reviewId=str(i), id='taken_3', reviewText='fast')
               for i in range(3)]
        self.helper.config.write_text(json.dumps(dict(responses={
            'filter|'+r['row_id']:dict(text=str(r['source_position'] != 1)) for r in raw})))
        engine = EngineSpec(str(self.helper.config), sys.executable, sys.executable, max_model_len=4096)
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1, max_active_batches=3,
                                     query_timeout_s=10, startup_timeout_s=8)
        profile = dict(mode='async', admission=dict(kind='resource_feedback', poll_interval_s=.01,
                       window_s=.04, max_sample_age_s=.3, min_samples=1, measurement_windows=2))
        with ThreadedRuntime(raw) as runtime, patch.object(AsyncCoreModelActor, '_process_type', return_value=CPUResourceProcess):
            owner = create_records_core(FilterMethod('fixture', instruction='Keep {reviewText}'),
                                        engine, GenerationSpec(), resources, profile=profile)
            try:
                self.assertIsInstance(owner._feedback, CoreResourceRequestFeedback)
                seen=[]
                for query in ('one', 'two'):
                    stream = owner.evaluate(Source(raw), query_id=query)
                    seen.append(stream.session.session_id)
                    results = list(stream)
                    self.assertEqual({v['row_id'] for v in results}, {r['row_id'] for r in raw})
                    self.assertEqual(stream.last_close_report['admission_feedback']['strategy'], 'resource_request_v1')
                    self.assertEqual(owner.core.capacity.usage().held_tasks, 0)
                self.assertEqual(len(set(seen)), 2)
                self.assertEqual(len(runtime.async_actors), 1)
                with self.assertRaises(ValueError):
                    owner.evaluate(Source(raw), query_id='bad-window', max_inflight=2)
                with self.assertRaises(ValueError):
                    owner.evaluate(Source(raw), query_id='bad-hint', search_hint={})
            finally:
                report=owner.close(timeout=3)
            self.assertEqual(report['status'], 'closed')
            self.assertTrue(report['feedback_sampler_stopped'])

    def test_missing_ready_capability_has_no_legacy_fallback(self):
        plan, rows = base.fixture.chain_for('M'), base.fixture.source_rows(size=2)
        self.helper.config_for(plan, rows)
        with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, '_process_type', return_value=base.query_fixture.Process):
            owner = base.AsyncTokenBatchTests.owner(self, plan, R=2, C=None, rolling_refill=True,
                                                    request_feedback=ResourceRequestConfig())
            stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
            try:
                with self.assertRaisesRegex(ValueError, 'resource owner ready'):
                    list(stream)
                self.assertFalse(any(e['event']=='start' for e in self.helper.events()))
            finally:owner.close(timeout=3)


if __name__ == '__main__':
    unittest.main()
