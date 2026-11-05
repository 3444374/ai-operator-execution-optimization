"""Actual supply builder and Core with a CPU backend and explicit policy seam.

This checks the shared builder, dispatch and settlement. It does not validate
the public policy factory or a real native/resource producer.
"""
from dataclasses import replace
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.execution_provider.adapters import lotus_chain_token_batches as batching
from src.planning.work import StageWork, WorkDescriptor
from src.scheduling.core.models import AdmissionObservation
from src.scheduling.core.session_contract import TaskInfo
from src.scheduling.submission_control.adaptive import ResourceRequestConfig
from src.scheduling.submission_control.admission import StaticAdmissionController
from src.scheduling.submission_control.request_scan import ResourceRequestController
from src.scheduling.submission_control.resource_probe import ResourceProbeView, ResourceRequestPermit
from tests.execution_provider.resource_request_sampler_fixture import ready, request_sampler
from src.serving.backends.vllm_async_engine import AsyncEngineLimits
from src.serving.backends.vllm_engine import EngineSpec
import sys
from tests.scheduling.test_incremental_session import setup, task


class ResourceRequestSupplyBridgeTests(unittest.TestCase):
    def test_real_request_saturation_remains_measurable_and_all_original_tasks_settle_once(self):
        maximum, rows = 4, 96
        engine, session, backend, clock = setup(held_tasks=rows, input_bytes=rows*4,
            result_bytes=rows*4, active_requests=maximum, active_work=maximum,
            offer_tasks=rows, step_actions=64, metadata_bytes=2048, wait_timeout_s=100.)
        clock.now = 10.
        controller = ResourceRequestController(maximum,
            ResourceRequestConfig(min_samples=1, measurement_windows=2), clock=clock)
        controller.reset(1, rows, resource_identity=('CPU-builder', session.session_id))

        class SelectedWindow:
            def decide(self, inflight, **kwargs):
                return StaticAdmissionController(controller.current_window).decide(inflight, **kwargs)

        engine.policies = replace(engine.policies, admission=SelectedWindow())
        work = WorkDescriptor((StageWork('model', 1, 'work_units'),), 'model', 'CPU-fixture')
        offered = tuple(task(index, info=TaskInfo('call-'+str(index), index, 'model', work))
                        for index in range(rows))
        self.assertEqual(session.offer(offered).accepted_prefix_count, rows)
        session.seal()
        session.advance(16)
        organizer = SimpleNamespace(request_window=1, usage=lambda: (
            engine.capacity.usage().active_requests, engine.capacity.usage().active_requests))
        owner = SimpleNamespace(core=engine, resources=SimpleNamespace(max_active_batches=maximum),
            _token_organizer=organizer, _receipt_lock=threading.Lock(), _raw_receipts={}, _expected_count=rows)
        owner._actors = SimpleNamespace(limits=AsyncEngineLimits(max_inflight=maximum, result_slots=maximum),
                                        model_ready=ready('CPU-builder'))
        owner.engine = EngineSpec('CPU-builder', sys.executable, sys.executable)
        owner.prepared_batching = SimpleNamespace(token_budget=256)
        feedback = request_sampler(owner, session.session_id, started_at_s=10.)
        feedback.controller = controller
        feedback.prepared = rows
        driver = SimpleNamespace(ready_observer_error=None, active_rows=rows, _rows={})
        stream = SimpleNamespace(driver=driver, _phase_eof=True, offered=set(range(rows)))
        sequence = 0

        def latest(inflight):
            nonlocal sequence
            sequence += 1
            return AdmissionObservation(clock.now, True, inflight, inflight, 0, .99,
                sample_age_s=0., sample_sequence=sequence, preemptions_total=0)

        feedback.provider = SimpleNamespace(latest_with_source=lambda inflight: (latest(inflight), None))
        observed = []

        def decide(observation, supply, *_args):
            permit = ResourceRequestPermit('CPU-builder', session.session_id, sequence, sequence,
                clock.now, maximum, local_limits_enforced=True, backend_owns_memory=True)
            pair = tuple(ResourceProbeView('CPU-builder', session.session_id, sequence*2+index,
                clock.now-.20+index*.05, clock.now-.10+index*.05, .01,
                ('GPU-cpu-fixture',), 0., 0.) for index in (0, 1))
            decision = controller.update_supply(observation, supply,
                resource_permit=permit, physical_samples=pair)
            observed.append((supply.capacity_blocked, supply.active_requests, decision.reason))
            return decision

        # Preserve the actual builder and its member_R value. Only the policy
        # seam supplies fixture-qualified permissions and physical samples.
        feedback._decide = decide
        deliveries = []
        with patch.object(batching, 'time', SimpleNamespace(monotonic=clock)):
            feedback.update(stream)
            for _ in range(100):
                for key in tuple(backend.pending):
                    self.assertNotIn(key, owner._raw_receipts)
                    backend.complete(key)
                    owner._raw_receipts[key] = dict(status='succeeded', prompt_tokens=100,
                        completion_tokens=4, prompt_computed_tokens=80, prompt_cached_tokens=20)
                value = session.advance(16)
                self.assertIsNone(value.error)
                deliveries.extend(value.deliveries)
                session.release(tuple(item.lease_id for item in value.deliveries))
                driver.active_rows = engine.capacity.usage().held_tasks
                feedback.submitted = len(owner._raw_receipts)+engine.capacity.usage().active_requests
                clock.now += 1.
                feedback.update(stream)
                self.assertLessEqual(engine.capacity.usage().active_requests, maximum)
                if engine.capacity.usage().held_tasks == 0:
                    break
            else:
                self.fail('finite original fixture work did not drain')
        report = controller.report()
        self.assertTrue(any(block == 'member_R' and active == maximum for block, active, _ in observed))
        self.assertTrue(any(row['window'] == maximum for row in report['measurements']))
        self.assertTrue(any(row['window'] < row['before'] for row in report['history']))
        self.assertEqual(report['verified_performance_window'], maximum)
        self.assertEqual(len(deliveries), rows)
        self.assertEqual({item.key.sequence for item in deliveries}, set(range(rows)))
        self.assertEqual(len(owner._raw_receipts), rows)
        self.assertEqual(len(backend.routes), rows)
        self.assertEqual(backend.cancelled, [])
        usage = engine.capacity.usage()
        self.assertEqual((usage.held_tasks, usage.input_bytes, usage.result_bytes, usage.active_requests), (0, 0, 0, 0))
        session.close()


if __name__ == '__main__':
    unittest.main()
