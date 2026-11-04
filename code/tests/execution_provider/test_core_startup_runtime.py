"""Startup and cleanup keep the same observed runtime; CPU child and Ray fixtures."""
from contextlib import ExitStack
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from tests.execution_provider import test_records_service_owner as service
from tests.execution_provider import test_records_service_recovery as recovery


class CoreStartupRuntimeTests(unittest.TestCase):
    setUp = service.ServiceOwnerTests.setUp
    owner = service.ServiceOwnerTests.owner
    rows = service.ServiceOwnerTests.rows

    def _fail_after_actor_creation(self, entry, change, *, fail_ready=True):
        self.pool = type(self.pool)(self.pool.total_bytes)
        rows = self.rows()
        with recovery.RecoveryRuntime(rows) as runtime, ExitStack() as cleanup:
            owner = self.owner()
            cleanup.callback(owner.close, timeout=3)
            allocation = owner._service_allocation.allocation_id
            original_identity = dict(runtime.identity)
            original_initialized = runtime.ray.is_initialized
            original_module = sys.modules['ray']
            original_error = LookupError('CPU original startup failure')
            before_error = []
            blocked = []
            original_wait = owner._actors.wait_ready
            with ExitStack() as failures:
                def lose_runtime():
                    if change == 'stopped':
                        runtime.ray.is_initialized = lambda: False
                    elif change == 'module':
                        sys.modules['ray'] = object()
                    else:
                        runtime.identity = dict(original_identity, **{change: 'replacement-' + change})
                    for name in ('init', 'remote', 'wait', 'get', 'kill', 'cancel'):
                        blocked.append(failures.enter_context(patch.object(runtime.ray, name,
                            side_effect=AssertionError('startup cleanup used Ray.' + name), create=True)))
                    before_error.extend(runtime.events)

                def fail_wait(ref, deadline, cancelled=None):
                    value = original_wait(ref, deadline, cancelled)
                    self.assertIsNotNone(owner._actors.model_actor)
                    self.assertEqual(len(runtime.async_actors), 1)
                    self.assertIsNotNone(runtime.async_actors[0].instance.engine)
                    lose_runtime()
                    raise original_error

                original_start = owner._actors.start
                def fail_start(*args, **kwargs):
                    value = original_start(*args, **kwargs)
                    lose_runtime()
                    raise original_error

                patched = failures.enter_context(patch.object(owner._actors,
                    'wait_ready' if fail_ready else 'start', side_effect=fail_wait if fail_ready else fail_start))
                try:
                    with self.assertRaises(LookupError) as caught:
                        (owner.start() if entry == 'start' else
                         owner.evaluate(service.source_fixture.Source(rows), query_id='startup-failure'))
                    self.assertIs(caught.exception, original_error)
                    self.assertEqual(runtime.events, before_error)
                    for operation in blocked:
                        operation.assert_not_called()
                    self.assertEqual(self.pool.snapshot()['allocations'], 1)
                    self.assertEqual(owner._service_allocation.describe()['status'], 'bound')
                    with self.assertRaisesRegex(RuntimeError, 'runtime'):
                        owner.close(timeout=.02)
                    self.assertEqual(runtime.events, before_error)
                    with self.assertRaisesRegex(RuntimeError, 'runtime'):
                        self.pool.retry_cleanup(allocation, timeout=.02)
                    self.assertEqual(runtime.events, before_error)
                    self.assertEqual(owner._model_runtime[1], original_identity)
                    if entry == 'evaluate':
                        self.assertTrue(any('cleanup' in note and 'runtime' in note
                            for note in getattr(original_error, '__notes__', [])))
                finally:
                    runtime.identity = original_identity
                    runtime.ray.is_initialized = original_initialized
                    sys.modules['ray'] = original_module
            try:
                closed = self.pool.retry_cleanup(allocation, timeout=3)
                self.assertEqual(closed['status'], 'closed', closed)
                self.assertEqual(closed['service_allocation']['status'], 'released')
                self.assertEqual(self.pool.snapshot()['allocated_bytes'], 0)
            finally:
                owner.close(timeout=3)

    def test_failed_ready_keeps_original_error_and_never_closes_through_a_lost_runtime(self):
        for entry in ('start', 'evaluate'):
            for change in ('stopped', 'job_id', 'node_id', 'module'):
                with self.subTest(entry=entry, change=change):
                    self._fail_after_actor_creation(entry, change)

    def test_failed_actor_start_uses_the_same_protection_before_ready_is_adopted(self):
        for change in ('stopped', 'job_id'):
            with self.subTest(change=change):
                self._fail_after_actor_creation('evaluate', change, fail_ready=False)

    def test_startup_cleanup_failure_is_only_a_note_on_the_original_error(self):
        with recovery.RecoveryRuntime(self.rows()):
            owner = self.owner()
            original_start = owner._actors.start
            failure = LookupError('CPU startup error')
            def start_then_fail(*args, **kwargs):
                original_start(*args, **kwargs)
                raise failure
            try:
                with patch.object(owner._actors, 'start', side_effect=start_then_fail), \
                        patch.object(owner, 'close', side_effect=ValueError('CPU close failure')):
                    with self.assertRaises(LookupError) as caught:
                        owner.evaluate(service.source_fixture.Source(self.rows()), query_id='cleanup-error')
                self.assertIs(caught.exception, failure)
                self.assertTrue(any('ValueError' in note for note in failure.__notes__))
                self.assertEqual(self.pool.snapshot()['allocations'], 1)
                closed = self.pool.retry_cleanup(owner._service_allocation.allocation_id, timeout=3)
                self.assertEqual(closed['service_allocation']['status'], 'released')
            finally:
                owner.close(timeout=3)

    def test_next_start_or_query_cannot_adopt_another_runtime_after_ready(self):
        rows = self.rows()
        with recovery.RecoveryRuntime(rows) as runtime:
            owner = self.owner()
            owner.start()
            identity = deepcopy(owner._model_runtime[1])
            before = list(runtime.events)
            try:
                runtime.identity = dict(identity, job_id='replacement-job')
                with patch.object(owner._actors, 'start', wraps=owner._actors.start) as start:
                    with self.assertRaisesRegex(ValueError, 'runtime'):
                        owner.start()
                    with self.assertRaisesRegex(ValueError, 'runtime'):
                        owner.evaluate(service.source_fixture.Source(rows), query_id='another-runtime')
                    start.assert_not_called()
                remote = lambda events: [event for event in events
                    if event[0] in ('actor', 'submit', 'driver_get', 'kill', 'cancel')]
                self.assertEqual(remote(runtime.events), remote(before))
                self.assertEqual(owner._model_runtime[1], identity)
                self.assertEqual(self.pool.snapshot()['allocations'], 1)
            finally:
                runtime.identity = identity
                owner.close(timeout=3)

    def test_context_change_during_successful_start_does_not_publish_ready(self):
        with recovery.RecoveryRuntime([]) as runtime:
            owner = self.owner()
            original = owner._actors.start
            identity = dict(runtime.identity)
            def replace_after_start(*args, **kwargs):
                ready = original(*args, **kwargs)
                runtime.identity = dict(identity, node_id='replacement-node')
                return ready
            try:
                with patch.object(owner._actors, 'start', side_effect=replace_after_start):
                    with self.assertRaisesRegex(RuntimeError, 'runtime'):
                        owner.start()
                self.assertIsNone(owner._service_allocation.describe()['model_identity'])
                self.assertEqual(owner._model_runtime[1], identity)
                self.assertEqual(self.pool.snapshot()['allocations'], 1)
            finally:
                runtime.identity = identity
                owner.close(timeout=3)

    def test_first_unobserved_fixture_cannot_gain_identity_from_a_later_connection(self):
        rows = self.rows()
        with service.fixtures.ThreadedRuntime(rows) as runtime:
            owner = self.owner()
            try:
                ready = owner.start()
                self.assertTrue(owner._model_runtime_capture_attempted)
                self.assertIsNone(owner._model_runtime)
                context = SimpleNamespace(get_job_id=lambda: 'later-job', get_node_id=lambda: 'later-node')
                with patch.object(runtime.ray, 'get_runtime_context', return_value=context, create=True):
                    self.assertEqual(owner.start(), ready)
                    self.assertIsNone(owner._model_runtime)
                    self.assertIsNone(owner._service_allocation._runtime_identity)
                    with self.assertRaisesRegex(RuntimeError, 'runtime'):
                        self.pool.retry_cleanup(owner._service_allocation.allocation_id, timeout=1)
                result = list(owner.evaluate(service.source_fixture.Source(rows), query_id='legacy-fixture'))
                self.assertEqual(result[0]['prompt_tokens'], 11)
            finally:
                self.assertEqual(owner.close(timeout=3)['status'], 'closed')

    def test_unstarted_owner_cleanup_does_not_observe_or_start_a_runtime(self):
        owner = self.owner()
        unavailable = SimpleNamespace(is_initialized=Mock(side_effect=AssertionError('runtime read')),
                                      init=Mock(side_effect=AssertionError('runtime start')))
        with patch.dict(sys.modules, {'ray': unavailable}):
            closed = owner.close(timeout=1)
        unavailable.is_initialized.assert_not_called()
        unavailable.init.assert_not_called()
        self.assertEqual(closed['service_allocation']['status'], 'released')

    def test_entry_binding_precedes_first_start_and_still_allows_unstarted_local_cleanup(self):
        with recovery.RecoveryRuntime([]) as runtime:
            owner = self.owner()
            from src.execution_provider.adapters.records_service_budget import RecordsPayloadPool
            observed = RecordsPayloadPool._ray_runtime_observation(runtime.ray)
            owner.bind_model_runtime(observed)
            original = dict(owner._model_runtime[1])
            observed[1]['job_id'] = 'mutated-input-copy'
            self.assertEqual(owner._model_runtime[1], original)
            runtime.identity = dict(original, job_id='replacement-before-first-start')
            try:
                with patch.object(owner._actors, 'start', wraps=owner._actors.start) as start:
                    with self.assertRaisesRegex(RuntimeError, 'runtime'):
                        owner.start()
                    start.assert_not_called()
                self.assertEqual(len(runtime.async_actors), 0)
                self.assertEqual(self.pool.snapshot()['allocations'], 1)
            finally:
                closed = owner.close(timeout=1)
            self.assertEqual(closed['service_allocation']['status'], 'released')

    def test_explicit_binding_is_rechecked_and_cannot_replace_a_failed_first_observation(self):
        with service.fixtures.ThreadedRuntime([]) as runtime:
            owner = self.owner()
            owner.bind_model_runtime()
            self.assertIsNone(owner._model_runtime)
            context = SimpleNamespace(get_job_id=lambda: 'later-job', get_node_id=lambda: 'later-node')
            try:
                with patch.object(runtime.ray, 'get_runtime_context', return_value=context, create=True):
                    with self.assertRaisesRegex(RuntimeError, 'rebound'):
                        owner.bind_model_runtime((runtime.ray, dict(job_id='later-job', node_id='later-node')))
                self.assertIsNone(owner._model_runtime)
            finally:
                owner.close(timeout=1)
        with recovery.RecoveryRuntime([]) as runtime:
            owner = self.owner()
            observed = (runtime.ray, dict(runtime.identity))
            try:
                runtime.identity = dict(runtime.identity, node_id='changed-before-binding')
                with self.assertRaisesRegex(RuntimeError, 'runtime'):
                    owner.bind_model_runtime(observed)
                self.assertFalse(owner._model_runtime_capture_attempted)
                self.assertIsNone(owner._model_runtime)
            finally:
                owner.close(timeout=1)

    def test_matching_entry_binding_keeps_original_requests_and_ready_identity(self):
        rows = self.rows()
        with recovery.RecoveryRuntime(rows) as runtime:
            owner = self.owner()
            observed = (runtime.ray, dict(runtime.identity))
            try:
                owner.bind_model_runtime(observed)
                ready = owner.start()
                owner.bind_model_runtime(observed)
                results = list(owner.evaluate(service.source_fixture.Source(rows), query_id='bound-entry'))
                self.assertEqual(results[0]['engine_uid'], ready['engine_uid'])
                self.assertEqual(results[0]['prompt_tokens'], 11)
                self.assertEqual(len(runtime.async_actors), 1)
                self.assertEqual(owner._model_runtime[1], observed[1])
                self.assertIsNot(owner._model_runtime[1], observed[1])
            finally:
                owner.close(timeout=3)

    def test_confirmed_service_close_repeats_after_shutdown_without_another_actor_action(self):
        with recovery.RecoveryRuntime([]) as runtime:
            owner = self.owner()
            owner.start()
            closed = owner.close(timeout=3)
            self.assertEqual(closed['status'], 'closed')
            before = list(runtime.events)
            with patch.object(runtime.ray, 'is_initialized', return_value=False), \
                    patch.object(owner._actors, 'close', side_effect=AssertionError('no repeated actor close')) as close:
                self.assertEqual(owner.close(timeout=3), closed)
            close.assert_not_called()
            self.assertEqual(runtime.events, before)
            self.assertEqual(self.pool.snapshot()['allocations'], 0)

    def test_confirmed_plain_core_close_repeats_after_runtime_replacement(self):
        from src.execution_provider.adapters.lotus_records_core import create_records_core
        from src.semantic_methods.lotus.records import RecordsResources
        from src.serving.backends.vllm_protocol import GenerationSpec
        with recovery.RecoveryRuntime([]) as runtime:
            owner = create_records_core(self.method, self.engine, GenerationSpec(),
                RecordsResources(prepare_batch_rows=1, model_batch_rows=1, max_active_batches=2),
                profile=dict(mode='async'))
            owner.start()
            closed = owner.close(timeout=3)
            self.assertEqual(closed['status'], 'closed')
            before = list(runtime.events)
            original_identity = dict(runtime.identity)
            try:
                runtime.identity['job_id'] = 'replacement-job-after-confirmed-close'
                with patch.object(owner._actors, 'close', side_effect=AssertionError('no repeated actor close')) as close:
                    self.assertEqual(owner.close(timeout=3), closed)
                close.assert_not_called()
                self.assertEqual(runtime.events, before)
            finally:
                runtime.identity = original_identity

    def test_repeated_closed_proof_keeps_results_until_explicit_local_acceptance(self):
        for text, status in (('hold', 'succeeded'), ('hold-unknown', 'unknown')):
            with self.subTest(result=status):
                self.pool = type(self.pool)(self.pool.total_bytes)
                rows = self.rows(text)
                with recovery.RecoveryRuntime(rows) as runtime:
                    owner = self.owner()
                    stream = owner.evaluate(service.source_fixture.Source(rows), query_id='closed-local-results')
                    stream._fill()
                    def admitted():
                        stream.session.advance(1)
                        return runtime.async_actors[0].instance.engine.snapshot()['accepted_requests'] == 1
                    service.ServiceOwnerTests.until(self, admitted)
                    stream.close()
                    closed = owner.close(timeout=3)
                    self.assertEqual(closed['status'], 'closed', closed)
                    self.assertEqual(self.pool.snapshot()['allocations'], 1)
                    allocation = owner._service_allocation.allocation_id
                    with ExitStack() as blocked:
                        operations = [blocked.enter_context(patch.object(runtime.ray, name,
                            side_effect=AssertionError('local repeated close called Ray.'+name), create=True))
                            for name in ('init','wait','get','remote','kill','cancel','get_runtime_context','is_initialized')]
                        self.assertEqual(owner.close(timeout=3), closed)
                        self.assertEqual(self.pool.retry_cleanup(allocation, timeout=3), closed)
                        self.assertEqual(self.pool.snapshot()['allocations'], 1)
                        taken = self.pool.take_unconsumed_results(allocation)
                        self.assertEqual([(row['status'],row['prompt_tokens']) for row in taken], [(status,11)])
                        self.assertEqual(self.pool.snapshot()['allocations'], 0)
                        for operation in operations:
                            operation.assert_not_called()
                    self.assertEqual(closed['service_allocation']['status'], 'bound')
                    self.assertEqual(owner.last_close_report['service_allocation']['status'], 'released')

    def test_repeated_closed_proof_still_requires_the_original_control_thread(self):
        with recovery.RecoveryRuntime([]) as runtime:
            owner = self.owner()
            owner.start()
            closed = owner.close(timeout=3)
            before = list(runtime.events)
            with ThreadPoolExecutor(max_workers=1) as worker:
                with self.assertRaisesRegex(RuntimeError, 'control thread'):
                    worker.submit(owner.close, timeout=3).result()
            self.assertEqual(owner.last_close_report, closed)
            self.assertEqual(runtime.events, before)

    def test_invalid_timeout_is_rejected_before_reusing_a_closed_proof(self):
        with recovery.RecoveryRuntime([]):
            owner = self.owner()
            owner.start()
            owner.close(timeout=3)
            with patch.object(owner._actors, 'close', side_effect=AssertionError('invalid timeout reached actor')):
                for value in (True,0,-1,float('nan'),float('inf'),'1'):
                    with self.subTest(timeout=value), self.assertRaises(ValueError):
                        owner.close(timeout=value)


if __name__ == '__main__':
    unittest.main()
