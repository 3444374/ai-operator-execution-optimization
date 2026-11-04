"""Service grants around the actual Core and CPU socket child, with Ray fixtures."""
from copy import deepcopy
import threading
import time
import unittest
from unittest.mock import patch

from src.execution_provider.adapters import lotus_records_core as core
from src.execution_provider.adapters import lotus_records_async_actor as actors
from src.execution_provider.adapters.records_capacity import CoreRecordsCapacity
from src.execution_provider.adapters.records_service_budget import RecordsPayloadPool
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources
from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec
from tests.execution_provider import test_lotus_records_async_core as fixtures
from tests.execution_provider import test_lotus_records_core as source_fixture


class ServiceOwnerTests(unittest.TestCase):
    def setUp(self):
        self.pool = RecordsPayloadPool(8 * 1024**2)
        self.shape = CoreRecordsCapacity(item_input_bytes=8192, item_result_bytes=8192)
        self.method = FilterMethod('fixture', instruction='Keep {reviewText}')
        self.engine = EngineSpec('/tmp/service-fixture-model', '/tmp/model-python', '/tmp/driver-python')

    def owner(self, **options):
        return core.create_records_core(self.method, self.engine, GenerationSpec(),
            capacity=self.shape, service_budget=self.pool, **options)

    def rows(self, text='fast'):
        return [dict(row_id='row-0', source_position=0, reviewId='repeat-review',
                     id='taken_3', reviewText=text)]

    def until(self, predicate):
        deadline = time.monotonic() + 4
        while not predicate():
            self.assertLess(time.monotonic(), deadline, 'CPU service owner did not progress')
            time.sleep(.002)

    def test_factory_uses_the_actual_service_grant_without_R_or_eight_result_defaults(self):
        planned = self.pool.preview(self.shape)
        owner = self.owner()
        try:
            self.assertEqual(owner.capacity, planned.capacity)
            self.assertEqual(owner._actors.limits, planned.ipc_limits)
            self.assertGreater(owner.resources.max_active_batches, 8)
            self.assertIsNone(owner._actors.model_actor)
            self.assertEqual(owner.execution_profile['declared_legacy_request_budget'], None)
            self.assertEqual(owner.execution_profile['service_payload_plan'], planned.describe())
            self.assertNotIn(owner._service_allocation.allocation_id, str(owner.execution_profile))
            self.assertEqual(self.pool.snapshot()['allocated_bytes'], planned.bytes)
        finally:
            report = owner.close(timeout=1)
        self.assertEqual(report['service_allocation']['status'], 'released')
        self.assertEqual(self.pool.snapshot()['allocated_bytes'], 0)
        self.assertEqual(owner.close(timeout=1), report)

    def test_two_model_owners_cannot_spend_the_same_bytes(self):
        first = self.owner(service_allocation_bytes=self.pool.total_bytes // 2)
        second = self.owner(service_allocation_bytes=self.pool.total_bytes // 2)
        try:
            self.assertNotEqual(first._service_allocation.allocation_id, second._service_allocation.allocation_id)
            with patch.object(actors, 'AsyncRecordsActorOwner') as allocate:
                with self.assertRaisesRegex(ValueError, 'payload'):
                    self.owner()
                allocate.assert_not_called()
            before = second._service_allocation.describe()
            first.close(timeout=1)
            self.assertEqual(second._service_allocation.describe(), before)
            self.assertEqual(self.pool.snapshot()['allocations'], 1)
        finally:
            first.close(timeout=1)
            second.close(timeout=1)
        self.assertEqual(self.pool.snapshot()['allocations'], 0)

    def test_conflicting_old_controls_are_rejected_before_actors_or_allocation(self):
        for options in (dict(resources=RecordsResources(prepare_batch_rows=1, model_batch_rows=1)),
                        dict(profile=dict(mode='sync')), dict(profile='computed-work-local-pairs'),
                        dict(async_limits={}), dict(rolling_refill=True), dict(batch_concurrency=1),
                        dict(prepared_batching=object())):
            with self.subTest(options=options), patch.object(actors, 'AsyncRecordsActorOwner') as allocate:
                with self.assertRaises((ValueError, TypeError)):
                    self.owner(**options)
                allocate.assert_not_called()
                self.assertEqual(self.pool.snapshot()['allocated_bytes'], 0)

    def test_constructor_failure_returns_an_unbound_allocation(self):
        with patch.object(core, 'SemLoomRecordsCore', side_effect=ValueError('bad constructor')):
            with self.assertRaisesRegex(ValueError, 'bad constructor'):
                self.owner()
        self.assertEqual(self.pool.snapshot()['allocated_bytes'], 0)

    def test_modern_feedback_still_starts_at_one_and_cannot_guess_memory(self):
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1, max_active_batches=None)
        owner = self.owner(resources=resources, profile=dict(mode='async', admission=dict(kind='resource_feedback')))
        try:
            self.assertEqual(owner.execution_profile['max_inflight'], 1)
            self.assertGreater(owner.execution_profile['owner_max_inflight'], 8)
            self.assertFalse(owner.execution_profile['admission']['require_memory_feedback'])
        finally:
            owner.close(timeout=1)
        self.assertEqual(self.pool.snapshot()['allocations'], 0)

    def test_one_actual_model_reuses_its_grant_across_queries_until_owner_closes(self):
        rows = self.rows()
        with fixtures.ThreadedRuntime(rows) as runtime:
            owner = self.owner()
            try:
                allocation = owner._service_allocation
                first = list(owner.evaluate(source_fixture.Source(rows), query_id='one'))
                identity = allocation.describe()['model_identity']
                second = list(owner.evaluate(source_fixture.Source(rows), query_id='two'))
                self.assertEqual(allocation.describe()['model_identity'], identity)
                self.assertEqual(len(runtime.async_actors), 1)
                self.assertEqual((first[0]['total_tokens'], second[0]['total_tokens']), (12, 12))
                self.assertEqual(self.pool.snapshot()['allocations'], 1)
                with self.assertRaisesRegex(RuntimeError, 'cleanup'):
                    allocation.cancel()
            finally:
                report = owner.close(timeout=3)
            self.assertEqual(report['status'], 'closed', report)
            self.assertEqual(report['service_allocation']['status'], 'released')
            self.assertEqual(self.pool.snapshot()['allocations'], 0)
            self.assertEqual(owner.take_unconsumed_results(), [])

    def test_ready_cannot_replace_the_bound_model_or_producer(self):
        with fixtures.ThreadedRuntime([]):
            owner = self.owner()
            try:
                ready = owner.start()
                self.assertEqual(owner._service_allocation.describe()['model_identity']['engine_uid'], ready['engine_uid'])
                changed = dict(ready, engine_uid='another-actual-model')
                with patch.object(owner._actors, 'start', return_value=changed), \
                        patch.object(owner._actors, 'model_ready', changed):
                    with self.assertRaisesRegex(ValueError, 'identity'):
                        owner.start()
                self.assertEqual(self.pool.snapshot()['allocations'], 1)
            finally:
                report = owner.close(timeout=3)
            self.assertEqual(report['service_allocation']['status'], 'released')

    def test_ready_without_an_actual_producer_identity_keeps_the_grant_until_cleanup(self):
        with fixtures.ThreadedRuntime([]):
            owner = self.owner()
            try:
                ready = owner.start()
                with patch.object(owner._actors, 'model_ready', dict(ready, producer_owner_uid=None)):
                    with self.assertRaisesRegex(ValueError, 'identities'):
                        owner.start()
                self.assertEqual(self.pool.snapshot()['allocations'], 1)
            finally:
                owner.close(timeout=3)
            self.assertEqual(self.pool.snapshot()['allocations'], 0)

    def test_close_for_another_producer_cannot_return_the_original_allocation(self):
        with fixtures.ThreadedRuntime([]):
            owner = self.owner()
            try:
                owner.start()
                original = owner._actors.close
                def wrong_proof():
                    report = deepcopy(original())
                    report['model_ready']['producer_owner_uid'] = 'another-producer'
                    return report
                with patch.object(owner._actors, 'close', side_effect=wrong_proof):
                    report = owner.close(timeout=3)
                self.assertEqual(report['status'], 'closed')
                self.assertEqual(report['service_allocation']['status'], 'bound')
                self.assertEqual(self.pool.snapshot()['allocations'], 1)
            finally:
                owner.close(timeout=3)
            self.assertEqual(self.pool.snapshot()['allocations'], 0)

    def test_unconsumed_actual_receipts_keep_bytes_until_the_caller_takes_them_once(self):
        rows = self.rows('hold')
        with fixtures.ThreadedRuntime(rows) as runtime:
            owner = self.owner()
            stream = owner.evaluate(source_fixture.Source(rows), query_id='cancel')
            try:
                stream._fill()
                def admitted():
                    stream.session.advance(1)
                    return runtime.async_actors[0].instance.engine.snapshot()['accepted_requests'] == 1
                self.until(admitted)
                stream.close()
                self.until(lambda: self._reap(owner))
                stream.collect_receipts()
                self.assertEqual(len(stream.unconsumed_call_results), 1)
                with self.assertRaisesRegex(RuntimeError, 'unconsumed'):
                    owner.evaluate(source_fixture.Source(rows), query_id='would-lose-receipt')
                first = owner.close(timeout=3)
                saved = deepcopy(first)
                self.assertEqual(first['status'], 'closed', first)
                self.assertEqual(first['service_allocation']['status'], 'bound')
                self.assertEqual(self.pool.snapshot()['allocations'], 1)
                taken = owner.take_unconsumed_results()
                self.assertEqual(len(taken), 1)
                self.assertEqual(taken[0]['prompt_tokens'], 11)
                self.assertEqual(first, saved)
                self.assertEqual(owner.last_close_report['service_allocation']['status'], 'released')
                self.assertEqual(owner.last_close_report['last_query_report']['unconsumed_call_results'], [])
                self.assertEqual(owner.take_unconsumed_results(), [])
                self.assertEqual(self.pool.snapshot()['allocations'], 0)
                self.assertEqual(owner.close(timeout=3)['last_query_report']['unconsumed_call_results'], [])
            finally:
                owner.close(timeout=3)

    def _reap(self, owner):
        owner.core.reap(owner.capacity.step_actions)
        return owner.core.capacity.usage().held_tasks == 0

    def test_failed_ready_keeps_the_allocation_when_cleanup_has_no_proof(self):
        owner = self.owner()
        original = owner._actors.close
        try:
            with patch.object(owner._actors, 'start', side_effect=TimeoutError('ready not confirmed')):
                with self.assertRaises(TimeoutError):
                    owner.start()
            with patch.object(owner._actors, 'close', return_value=dict(status='unknown',
                    gpu_exit_confirmed=None, cleanup_errors=['CPU unknown close'],
                    data_rpc_pending=1, data_rpc_unconfirmed=1)):
                self.assertEqual(owner.close(timeout=.02)['service_allocation']['status'], 'bound')
            self.assertEqual(self.pool.snapshot()['allocations'], 1)
        finally:
            with patch.object(owner._actors, 'close', original):
                owner.close(timeout=1)
        self.assertEqual(self.pool.snapshot()['allocations'], 0)

    def test_cancelled_query_can_transfer_receipts_and_reuse_the_same_live_model(self):
        rows = self.rows('hold')
        with fixtures.ThreadedRuntime(rows) as runtime:
            owner = self.owner()
            try:
                stream = owner.evaluate(source_fixture.Source(rows), query_id='cancel-then-reuse')
                stream._fill()
                def admitted():
                    stream.session.advance(1)
                    return runtime.async_actors[0].instance.engine.snapshot()['accepted_requests'] == 1
                self.until(admitted)
                stream.close()
                self.until(lambda: self._reap(owner))
                taken = owner.take_unconsumed_results()
                self.assertEqual(len(taken), 1)
                self.assertEqual(taken[0]['prompt_tokens'], 11)
                self.assertEqual(self.pool.snapshot()['allocations'], 1)
                self.assertIsNone(owner.last_close_report)
                runtime.rows = self.rows()
                later = list(owner.evaluate(source_fixture.Source(runtime.rows), query_id='next'))
                self.assertEqual(later[0]['status'], 'succeeded')
                self.assertEqual(len(runtime.async_actors), 1)
                self.assertEqual(owner.take_unconsumed_results(), [])
            finally:
                owner.close(timeout=3)
            self.assertEqual(self.pool.snapshot()['allocations'], 0)

    def test_empty_query_does_not_invent_model_identity_or_release_a_live_owner_early(self):
        with fixtures.ThreadedRuntime([]) as runtime:
            owner = self.owner()
            try:
                self.assertEqual(list(owner.evaluate(source_fixture.Source([]), query_id='empty')), [])
                self.assertEqual(len(runtime.async_actors), 0)
                self.assertIsNone(owner._service_allocation.describe()['model_identity'])
                self.assertEqual(self.pool.close()['status'], 'pending')
                self.assertEqual(self.pool.snapshot()['allocations'], 1)
            finally:
                owner.close(timeout=3)
            self.assertEqual(self.pool.close()['status'], 'closed')

    def _computing_close(self, text, expected_status):
        rows = self.rows(text)
        close_entered, release = threading.Event(), threading.Event()
        original_close, original_abort = actors.AsyncCoreModelActor.close, actors.AsyncCoreModelActor.abort_core
        def held_close(actor, timeout):
            close_entered.set()
            if not release.wait(5):
                raise TimeoutError('test did not release close')
            return original_close(actor, timeout)
        def held_abort(actor, key):
            if not release.wait(5):
                raise TimeoutError('test did not release abort')
            return original_abort(actor, key)
        with fixtures.ThreadedRuntime(rows) as runtime, \
                patch.object(actors.AsyncCoreModelActor, 'close', held_close), \
                patch.object(actors.AsyncCoreModelActor, 'abort_core', held_abort):
            owner = self.owner()
            stream = owner.evaluate(source_fixture.Source(rows), query_id='computing')
            try:
                stream._fill()
                def admitted():
                    stream.session.advance(1)
                    return runtime.async_actors[0].instance.engine.snapshot()['accepted_requests'] == 1
                self.until(admitted)
                held = self.pool.snapshot()['allocated_bytes']
                report = owner.close(timeout=.02)
                self.assertEqual(report['status'], 'unknown')
                self.assertEqual(report['service_allocation']['status'], 'bound')
                self.assertEqual(self.pool.snapshot()['allocated_bytes'], held)
                self.assertEqual(owner.take_unconsumed_results(), [])
                self.assertEqual(self.pool.snapshot()['allocated_bytes'], held)
                release.set()
                report = owner.close(timeout=3)
                self.assertTrue(close_entered.is_set())
                self.assertEqual(report['status'], 'closed', report)
                self.assertEqual(self.pool.snapshot()['allocated_bytes'], held)
                taken = owner.take_unconsumed_results()
                self.assertEqual(len(taken), 1)
                self.assertEqual(taken[0]['prompt_tokens'], 11)
                self.assertEqual(taken[0]['status'], expected_status)
                self.assertEqual(self.pool.snapshot()['allocated_bytes'], 0)
                self.assertEqual(owner.take_unconsumed_results(), [])
            finally:
                release.set()
                owner.close(timeout=3)

    def test_computing_close_keeps_bytes_through_unknown_control_then_receipt_transfer(self):
        self._computing_close('hold', 'succeeded')

    def test_producer_retirement_does_not_upgrade_an_actual_unknown_receipt(self):
        self._computing_close('hold-unknown', 'unknown')


if __name__ == '__main__':
    unittest.main()
