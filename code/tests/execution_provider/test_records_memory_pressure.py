"""Bind real kernel-shaped CPU measurements without new RPCs or ownership changes."""
from concurrent.futures import Future
from copy import deepcopy
from dataclasses import asdict, replace
import sys
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.records_async_model import AsyncCoreModelActor
from src.execution_provider.adapters.records_memory_pressure import MemoryPressureBinding
from src.execution_provider.adapters.records_profile import resolve_core_profile
from src.observability.records_telemetry import MetricSources
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec
from src.serving.backends.vllm_async_engine import AsyncEngineLimits
from tests.execution_provider import test_records_request_feedback_safety as safety
from tests.observability.memory_pressure_fixture import KernelMemoryFixture, resource_packet


def binding():
    return MemoryPressureBinding('CPU-engine',7,started_at_s=9.,max_age_s=1.)


class MemoryPressureBindingTests(unittest.TestCase):
    def test_actual_kernel_interval_maps_clock_offset_and_does_not_retime_cached_reads(self):
        with KernelMemoryFixture() as kernel:
            kernel.source();frame=kernel.source();packet=resource_packet(frame)
            source=binding();source.observe(packet,requested_at_s=10.,received_at_s=10.1)
            first=source.permit_fields('CPU-engine',7)
            self.assertIs(first['memory_pressure'],False)
            self.assertEqual(first['safety_observed_at_s'],9.96875)
            packet['captured_at_s']+=.5
            source.observe(packet,requested_at_s=10.5,received_at_s=10.6)
            self.assertEqual(source.permit_fields('CPU-engine',7),first)
            self.assertEqual(source.permit_fields('CPU-engine',8),{})

    def test_foreign_actor_query_pid_or_clock_cannot_authorize(self):
        for name,wrong in (('engine_uid','foreign'),('session_id',8),('session_id',True),
                ('actor_pid',-1),('actor_pid',True),('clock_domain','driver')):
            with self.subTest(name=name,wrong=wrong),KernelMemoryFixture() as kernel:
                kernel.source();packet=resource_packet(kernel.source());packet[name]=wrong
                source=binding();source.observe(packet,requested_at_s=10.,received_at_s=10.1)
                self.assertEqual(source.permit_fields('CPU-engine',7),{})

    def test_first_read_and_missing_fields_remain_unknown(self):
        with KernelMemoryFixture() as kernel:
            source=binding();source.observe(resource_packet(kernel.source()),requested_at_s=10.,received_at_s=10.1)
            self.assertIsNone(source.permit_fields('CPU-engine',7)['memory_pressure'])
            for packet in ({},None,dict(engine_uid='CPU-engine',session_id=7,clock_domain='private_child_and_model_actor.monotonic')):
                source.observe(packet,requested_at_s=10.,received_at_s=10.1)
                self.assertEqual(source.permit_fields('CPU-engine',7),{})

    def test_invalid_interval_numbers_and_boolean_pressure_never_get_coerced(self):
        changes=[('source_sequence',True),('source_sequence',0),('pressure',0),('pressure',1),
            ('observed_at_s',True),('observed_at_s',float('nan')),('interval_s',False),('interval_s',2.),
            ('interval_s',float('inf')),('interval_started_at_s',True),('clock_domain','driver')]
        for name,wrong in changes:
            with self.subTest(name=name,wrong=wrong),KernelMemoryFixture() as kernel:
                kernel.source();frame=kernel.source();frame[name]=wrong
                source=binding();source.observe(resource_packet(frame),requested_at_s=10.,received_at_s=10.1)
                self.assertEqual(source.permit_fields('CPU-engine',7),{})

    def test_changed_source_epoch_or_hierarchy_cannot_clear_pressure_in_same_query(self):
        for changed in ('source_uid','hierarchy_uid'):
            with self.subTest(changed=changed),KernelMemoryFixture() as kernel:
                kernel.source();kernel.write(kernel.leaf,high=1);frame=kernel.source()
                source=binding();source.observe(resource_packet(frame),requested_at_s=10.,received_at_s=10.1)
                self.assertIs(source.permit_fields('CPU-engine',7)['memory_pressure'],True)
                frame=kernel.source();frame[changed]='replacement'
                source.observe(resource_packet(frame),requested_at_s=10.25,received_at_s=10.3)
                self.assertEqual(source.permit_fields('CPU-engine',7),{})
                source.observe(resource_packet(kernel.source()),requested_at_s=10.5,received_at_s=10.6)
                self.assertEqual(source.permit_fields('CPU-engine',7),{})

    def test_conflicting_cached_content_is_rejected_instead_of_clearing_pressure(self):
        with KernelMemoryFixture() as kernel:
            kernel.source();kernel.write(kernel.leaf,high=1);frame=kernel.source()
            source=binding();packet=resource_packet(frame)
            source.observe(packet,requested_at_s=10.,received_at_s=10.1)
            altered=deepcopy(packet);altered['sample']['sources']['host']['memory_pressure']['pressure']=False
            source.observe(altered,requested_at_s=10.1,received_at_s=10.2)
            self.assertEqual(source.permit_fields('CPU-engine',7),{})

    def test_source_before_query_and_future_source_or_reversed_rpc_times_are_unknown(self):
        with KernelMemoryFixture() as kernel:
            kernel.source();frame=kernel.source();packet=resource_packet(frame)
            for issued,received in ((8.,8.1),(10.1,10.),(False,10.),(float('nan'),10.)):
                with self.subTest(issued=issued):
                    source=binding();source.observe(packet,requested_at_s=issued,received_at_s=received)
                    self.assertEqual(source.permit_fields('CPU-engine',7),{})
            packet['captured_at_s']=frame['observed_at_s']-1
            source=binding();source.observe(packet,requested_at_s=10.,received_at_s=10.1)
            self.assertEqual(source.permit_fields('CPU-engine',7),{})

    def test_advancing_sequence_cannot_retime_an_old_source_timestamp(self):
        with KernelMemoryFixture() as kernel:
            kernel.source();frame=kernel.source();source=binding()
            source.observe(resource_packet(frame),requested_at_s=10.,received_at_s=10.1)
            frame['source_sequence']+=1
            source.observe(resource_packet(frame),requested_at_s=10.5,received_at_s=10.6)
            self.assertEqual(source.permit_fields('CPU-engine',7),{})


class MemoryPressureIntegrationTests(unittest.TestCase):
    def test_missing_or_old_memory_after_pressure_cannot_grow_with_healthy_native_or_release_results(self):
        for fault in ('missing','old_session'):
            with self.subTest(fault=fault),KernelMemoryFixture() as kernel:
                fixture=safety.OrdinarySafetyFixture('succeeded')
                feedback=fixture.feedback
                feedback.options=replace(feedback.options,require_memory_feedback=True)
                fixture.controller.options=replace(fixture.controller.options,require_memory_feedback=True)
                feedback.memory_pressure_binding=MemoryPressureBinding('CPU-engine',7,
                    started_at_s=fixture.now-1,max_age_s=feedback.options.max_sample_age_s)
                kernel.source();kernel.write(kernel.leaf,high=1)
                packet=resource_packet(kernel.source())
                feedback._read_resource(packet,request_index=1,requested_at_s=fixture.now,received_at_s=fixture.now+.001)
                _,_,first=fixture.observe(observation=dict(fresh=False,sample_sequence=None,kv_usage=None,
                    preemptions_total=None),source=dict(kv_usage=0.,preemptions_total=0))
                self.assertEqual(feedback.gate.limit,8)
                original=fixture.ownership()
                packet=resource_packet(kernel.source())
                if fault=='missing':packet['sample']['sources']['host'].pop('memory_pressure')
                else:packet['session_id']=6
                feedback._read_resource(packet,request_index=2,requested_at_s=fixture.now,received_at_s=fixture.now+.001)
                _,_,report=fixture.observe(observation=dict(kv_usage=0.,preemptions_total=0),
                    source=dict(kv_usage=0.,preemptions_total=0))
                self.assertLessEqual(feedback.gate.limit,8)
                self.assertTrue(any('safety' in reason for reason in report['reasons']))
                self.assertEqual(fixture.ownership(),original)

    def test_existing_option_is_explicit_for_ordinary_and_rolling_without_changing_defaults(self):
        engine=EngineSpec('/tmp/CPU-only-model',sys.executable,sys.executable)
        resources=RecordsResources(max_active_batches=4)
        for rolling in (False,True):
            with self.subTest(rolling=rolling):
                profile=resolve_core_profile(engine,resources,
                    dict(mode='async',admission=dict(kind='resource_feedback',require_memory_feedback=True)),
                    rolling_refill=rolling)
                self.assertTrue(profile['admission']['require_memory_feedback'])
                default=resolve_core_profile(engine,resources,
                    dict(mode='async',admission=dict(kind='resource_feedback')),rolling_refill=rolling)
                self.assertFalse(default['admission']['require_memory_feedback'])

    def test_actual_actor_snapshot_protects_unknown_work_without_native_progress_or_extra_rpc(self):
        with KernelMemoryFixture() as kernel:
            fixture=safety.OrdinarySafetyFixture('unknown')
            feedback=fixture.feedback
            feedback.options=replace(feedback.options,require_memory_feedback=True,allow_exploration=False)
            fixture.controller.options=replace(fixture.controller.options,require_memory_feedback=True)
            feedback.started=fixture.now-1
            feedback.memory_pressure_binding=MemoryPressureBinding('CPU-engine',7,
                started_at_s=feedback.started,max_age_s=feedback.options.max_sample_age_s)
            model=kernel.root/'model.json';model.write_text('{}')
            actor=AsyncCoreModelActor(asdict(EngineSpec(str(model),sys.executable,sys.executable)),
                asdict(GenerationSpec()),1,asdict(AsyncEngineLimits(max_inflight=8)))
            actor._session=7
            actor.engine=SimpleNamespace(ready_report=fixture.feedback.owner._actors.model_ready,
                snapshot=lambda:dict(owner_uid='CPU-producer',native_feedback=None))
            actor._resource_sources=MetricSources((),gpu_binding_error='CPU_fixture')
            kernel.source.clock=time.monotonic
            actor._resource_sources._memory_source=kernel.source
            calls=[]
            def remote(**kwargs):
                calls.append(kwargs)
                value=Future();value.set_result(actor.snapshot(**kwargs))
                return SimpleNamespace(future=lambda:value)
            feedback.owner._actors.model_actor=SimpleNamespace(snapshot=SimpleNamespace(remote=remote))
            with patch.object(feedback,'_snapshot_clock',return_value=(fixture.now,None)), \
                    patch('src.observability.records_telemetry.host_metadata',return_value={}), \
                    patch('src.infrastructure.cgroup.memory_observation',return_value={}):
                self.assertIsNone(feedback.sample())
                self.assertIsNone(feedback.memory_pressure_binding.permit_fields('CPU-engine',7)['memory_pressure'])
                kernel.write(kernel.leaf,high=1)
                self.assertIsNone(feedback.sample())
            guard=feedback.memory_pressure_binding.permit_fields('CPU-engine',7)
            self.assertIs(guard['memory_pressure'],True)
            self.assertEqual(calls,[dict(observe_resources=True)]*2)
            rpc=feedback.snapshot_rpc_report()
            self.assertFalse(rpc['snapshot_rpc_terminal_evidence_complete'])
            self.assertEqual(len(rpc['snapshot_rpc_receipts']),2)
            self.assertEqual({r['frame_disposition'] for r in rpc['snapshot_rpc_receipts']},{'missing_native'})
            original=fixture.ownership()
            _,applied,report=fixture.observe(observation=dict(fresh=False,sample_sequence=None,kv_usage=None,
                preemptions_total=None),source=dict(kv_usage=0.,preemptions_total=0))
            self.assertFalse(applied.fresh)
            self.assertEqual(feedback.gate.limit,8)
            self.assertEqual(report['reasons']['resource_pressure'],1)
            self.assertEqual(fixture.ownership(),original)
            feedback.provider=None  # The fixture's latest() adapter owns no sampling thread.
            feedback._close_sampling()
            self.assertTrue(feedback.snapshot_rpc_report()['snapshot_rpc_terminal_evidence_complete'])
