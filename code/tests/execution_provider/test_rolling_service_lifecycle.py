"""Rolling service ownership through the existing chain and actual CPU child."""
from copy import deepcopy
from dataclasses import replace
import threading
import time
import weakref
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.baselines.text.movie_records import MovieRecordSource
from src.execution_provider.adapters.records_capacity import CoreRecordsCapacity, resolve_records_async_limits
from src.execution_provider.adapters.records_service_delivery import declare_ordinary_delivery, declare_ordinary_session
from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor, AsyncRecordsActorOwner
from src.semantic_methods.budget import row_reservation
from tests.experiments.movie_token_resident_fixture import MovieTokenResidentFixture


class Grant:
    """Only the capability seam is substituted; producer and IPC are actual."""

    def __init__(self, owner, delivery):
        self.owner, self.producer, self.released = owner, None, False
        self.events = []
        self.retirement = self.retired_producer_ref = None
        self._identity = self._runtime_identity = None
        self.plan = SimpleNamespace(capacity=owner.capacity, ipc_limits=owner._limits, delivery=delivery)
        owner._service_allocation = self

    def adopt_producer(self, owner, producer):
        assert owner is self.owner and producer is owner.owner
        assert self.producer in (None, producer)
        assert producer.capacity == self.plan.capacity
        assert producer.resources == owner.resources
        assert producer._actors.limits == self.plan.ipc_limits
        assert producer._actors.model_actor is None
        self.producer = producer
        self.events.append('adopt')

    def observe_ready(self, owner):
        assert owner is self.owner and owner.owner is self.producer
        ready = owner.model_ready
        assert ready is not None
        actual = {name: ready[name] for name in ('engine_uid', 'producer_owner_uid')}
        if self._identity is not None and actual != self._identity:
            raise ValueError('actual producer identity changed')
        self._identity = actual
        self.events.append('ready')

    def settle(self, owner):
        assert owner is self.owner and self.producer is owner.owner
        self.events.append('settle')
        producer = None if self.retired_producer_ref is None else self.retired_producer_ref()
        self.released = (owner.cleanup_confirmed() and not owner.has_unconsumed_results()
            and (producer is None or producer._service_payloads_retired_confirmed()))
        return self.released

    def retire_producer(self, owner, producer):
        if self.retirement is not None:
            assert self.retired_producer_ref() is producer
            return self.retirement
        assert owner is self.owner and producer is self.producer
        assert owner.cleanup_confirmed() and not owner.has_unconsumed_results()
        certificate = dict(model_identity=self._identity, terminal=True)
        self.retirement, self.retired_producer_ref = certificate, weakref.ref(producer)
        self.producer = None
        self.events.append('retire')
        return certificate

    def describe(self):
        return dict(status='released' if self.released else 'bound')


class RollingServiceLifecycleTests(unittest.TestCase):
    def setup_owner(self, queries=('service-first',), *, resident=True, mode='RF'):
        f = MovieTokenResidentFixture(self)
        owner = f.owner(mode, resident=resident)
        runtime_identity = dict(job_id='fixture-original-job', node_id='fixture-original-node')
        f.runtime.ray.get_runtime_context = lambda: SimpleNamespace(
            get_job_id=lambda: runtime_identity['job_id'], get_node_id=lambda: runtime_identity['node_id'])
        source = MovieRecordSource.from_manifest(f.manifests['movie-q1'])
        shape = CoreRecordsCapacity(item_input_bytes=65536, item_result_bytes=65536)
        expected = [(query, source.expected(query)) for query in queries]
        declaration = (declare_ordinary_delivery(*expected[0], item_result_bytes=shape.item_result_bytes)
                       if len(expected) == 1 else
                       declare_ordinary_session(expected, item_result_bytes=shape.item_result_bytes))
        maximum = max(map(lambda pair: len(pair[1]), expected))
        owner.capacity = shape.with_storage_bytes(input_bytes=2*shape.item_input_bytes,
            result_bytes=2*shape.item_result_bytes,
            method_bytes=max(1, maximum)*row_reservation(shape.method_limits(1)))
        owner._limits = resolve_records_async_limits(owner.resources, owner.capacity)
        grant = Grant(owner, declaration)
        return f, owner, source, grant, runtime_identity

    def test_actual_producer_is_adopted_before_ready_and_full_results_keep_original_meaning(self):
        for resident in (True, False):
            with self.subTest(resident=resident):
                f, owner, source, grant, _ = self.setup_owner(resident=resident)
                try:
                    values = list(owner.evaluate(source, query_id='service-first'))
                    self.assertEqual([value['decision'] for value in values], [True, False])
                    self.assertEqual([value['total_tokens'] for value in values], [14, 14])
                    self.assertEqual(grant.events[:2], ['adopt', 'ready'])
                    self.assertIs(owner.owner, grant.producer)
                    self.assertFalse(owner.has_unconsumed_results())
                    self.assertFalse(grant.released)
                    if resident:
                        self.assertTrue(owner.last_close_report['model_retained'])
                finally:
                    report = owner.close(timeout=3)
                self.assertEqual(report['status'], 'closed', report)
                self.assertTrue(owner.cleanup_confirmed())
                self.assertTrue(grant.released)
                self.assertEqual(sum(e['event'] == 'construct' for e in f.events()), 1)

    def test_changed_source_identity_is_rejected_before_any_producer_or_model(self):
        f, owner, source, grant, _ = self.setup_owner()
        expected = source.expected
        with patch.object(source, 'expected', side_effect=lambda query: expected('different-query')):
            with self.assertRaisesRegex(ValueError, 'delivery|declar'):
                owner.evaluate(source, query_id='service-first')
        self.assertIsNone(owner.owner)
        self.assertEqual(f.events(), [])
        self.assertEqual(grant.events, [])

    def test_incomplete_consumer_cannot_replace_the_current_query(self):
        _, owner, source, grant, _ = self.setup_owner(queries=('service-first', 'service-second'))
        try:
            stream = owner.evaluate(source, query_id='service-first')
            first = next(stream)
            self.assertTrue(owner.has_unconsumed_results())
            with self.assertRaisesRegex((ValueError, RuntimeError), 'active|unconsumed'):
                owner.evaluate(source, query_id='service-second')
            report = owner.close(timeout=3)
            saved = deepcopy(report)
            self.assertFalse(grant.released)
            self.assertFalse(stream.finished)
            retained = report['unconsumed_call_results']
            with self.assertRaisesRegex(ValueError, 'changed|acceptance'):
                owner.take_unconsumed_results(expected=[])
            actual = owner.take_unconsumed_results(expected=retained)
            self.assertEqual(actual, retained)
            self.assertNotEqual(actual[0]['call_id'], first['call_id'])
            self.assertEqual(report, saved)
            self.assertEqual(owner.take_unconsumed_results(), [])
            self.assertTrue(grant.released)
        finally:
            owner.close(timeout=3)

    def test_resident_two_queries_keep_one_actual_model_and_old_reports(self):
        for mode in ('RF', 'RD'):
            with self.subTest(mode=mode):
                f, owner, source, grant, _ = self.setup_owner(
                    queries=('service-first', 'service-second'), mode=mode)
                try:
                    first = owner.evaluate(source, query_id='service-first')
                    first_values = list(first)
                    saved = first.last_close_report
                    snapshot = deepcopy(saved)
                    second = list(owner.evaluate(source, query_id='service-second'))
                    self.assertIs(owner.owner, grant.producer)
                    self.assertEqual(first.last_close_report, snapshot)
                    self.assertEqual(saved, snapshot)
                    self.assertEqual({v['engine_uid'] for v in first_values + second},
                                     {owner.model_ready['engine_uid']})
                    self.assertEqual(grant.events.count('adopt'), 1)
                    self.assertNotIn('settle', grant.events)
                    self.assertFalse(grant.released)
                finally:
                    report = owner.close(timeout=3)
                self.assertEqual(report['status'], 'closed', report)
                self.assertTrue(grant.released)
                self.assertEqual(sum(e['event'] == 'construct' for e in f.events()), 1)

    def test_runtime_change_inside_failed_ready_blocks_cleanup_until_original_returns(self):
        for entry in ('start', 'evaluate'):
            with self.subTest(entry=entry):
                f, owner, source, grant, runtime = self.setup_owner(resident=entry == 'start')
                identity = dict(runtime)
                original = AsyncRecordsActorOwner.wait_ready
                failure, blocked = LookupError('original CPU ready failure'), []
                def fail_after_ready(actors, *args, **kwargs):
                    value = original(actors, *args, **kwargs)
                    runtime['job_id'] = 'replacement-job'
                    blocked.extend(f.runtime.events)
                    raise failure
                try:
                    with patch.object(AsyncRecordsActorOwner, 'wait_ready', fail_after_ready):
                        with self.assertRaises(LookupError) as caught:
                            (owner.start() if entry == 'start' else
                             owner.evaluate(source, query_id='service-first'))
                    self.assertIs(caught.exception, failure)
                    self.assertEqual(f.runtime.events, blocked)
                    self.assertFalse(grant.released)
                    self.assertIs(owner.owner, grant.producer)
                    self.assertEqual(owner._model_runtime[1], identity)
                    with self.assertRaisesRegex(RuntimeError, 'runtime'):
                        owner.close(timeout=.02)
                    self.assertEqual(f.runtime.events, blocked)
                finally:
                    runtime.update(identity)
                    report = owner.close(timeout=3)
                self.assertEqual(report['status'], 'closed', report)
                self.assertTrue(grant.released)

    def test_missing_initial_identity_cannot_be_filled_by_a_later_runtime(self):
        f, owner, source, grant, _ = self.setup_owner()
        with patch.object(f.runtime.ray, 'get_runtime_context', return_value=None):
            owner.bind_model_runtime()
        self.assertTrue(owner._model_runtime_capture_attempted)
        self.assertIsNone(owner._model_runtime)
        with self.assertRaisesRegex(RuntimeError, 'runtime'):
            owner.bind_model_runtime((f.runtime.ray, dict(job_id='fixture-original-job',
                                                        node_id='fixture-original-node')))
        with self.assertRaisesRegex(RuntimeError, 'runtime'):
            owner.evaluate(source, query_id='service-first')
        self.assertEqual(f.events(), [])
        self.assertIsNone(owner._model_runtime)
        self.assertTrue(grant.released)

    def test_terminal_cleanup_still_requires_actual_data_RPC_and_audit_handoff(self):
        _, owner, source, grant, _ = self.setup_owner()
        try:
            list(owner.evaluate(source, query_id='service-first'))
            child = owner.owner
            owner.close(timeout=3)
            self.assertTrue(owner.cleanup_confirmed())
            self.assertIsNone(owner.owner)
            from src.execution_provider.adapters.records_runtime_identity import prepared_cleanup_confirmed
            def actual_proof():
                return prepared_cleanup_confirmed(child._actors, child.transport, child.core,
                    child.last_close_report, closing=True, feedback=child._supply_feedback,
                    expected_identity=grant._identity)
            progress = child._actors._data_rpc_progress()
            for field in ('pending', 'inflight', 'batch_submits_pending', 'unconfirmed'):
                with self.subTest(field=field), patch.object(child._actors, '_data_rpc_progress',
                        return_value=replace(progress, **{field: 1})):
                    self.assertFalse(actual_proof())
            handoff = child.transport.audit_handoff_report()
            with patch.object(child.transport, 'audit_handoff_report',
                              return_value=dict(handoff, pending_receipts=1)):
                self.assertFalse(actual_proof())
            self.assertTrue(grant.released)
        finally:
            owner.close(timeout=3)

    def test_unknown_close_keeps_late_receipts_and_requires_explicit_transfer(self):
        f, owner, source, grant, _ = self.setup_owner()
        gate = f.root / 'service-late-unknown'
        held = next(key for key, value in f.replies.items() if value['text'] == 'False')
        f.replies[held].update(status='unknown', wait_file=str(gate))
        f.write_replies()
        release = threading.Event()
        original_close, original_abort = AsyncCoreModelActor.close, AsyncCoreModelActor.abort_core
        def hold(method, actor, *args):
            if not release.wait(5):
                raise TimeoutError('CPU service test did not release control')
            return method(actor, *args)
        with patch.object(AsyncCoreModelActor, 'close', lambda actor, timeout: hold(original_close, actor, timeout)), \
                patch.object(AsyncCoreModelActor, 'abort_core', lambda actor, key: hold(original_abort, actor, key)):
            try:
                stream = owner.evaluate(source, query_id='service-first')
                actual = stream.stream
                deadline = time.monotonic() + 4
                while len(owner.owner.calls) != 1 or not any(e['event'] == 'start' and e['token'] == held for e in f.events()):
                    actual._tick()
                    self.assertLess(time.monotonic(), deadline)
                early = owner.close(timeout=.02)
                saved = deepcopy(early)
                self.assertEqual(early['status'], 'unknown')
                self.assertFalse(grant.released)
                self.assertFalse(owner.cleanup_confirmed())
                gate.touch()
                deadline = time.monotonic() + 4
                while not any(e['event'] == 'end' and e['token'] == held for e in f.events()):
                    self.assertLess(time.monotonic(), deadline)
                    time.sleep(.002)
                release.set()
                final = owner.close(timeout=3)
                self.assertEqual(final['status'], 'closed', final)
                self.assertTrue(owner.cleanup_confirmed())
                self.assertFalse(grant.released)
                self.assertEqual(sorted(v['status'] for v in final['calls']), ['succeeded', 'unknown'])
                self.assertEqual(stream.last_close_report['calls'], final['calls'])
                self.assertEqual(early, saved)
                taken = owner.take_unconsumed_results(expected=final['unconsumed_call_results'])
                self.assertEqual(taken, final['calls'])
                self.assertTrue(grant.released)
                self.assertEqual(stream.last_close_report['unconsumed_call_results'], [])
            finally:
                gate.touch()
                release.set()
                owner.close(timeout=3)

    def test_unstarted_close_returns_its_grant_without_reading_runtime_or_allocating_model(self):
        f, owner, _, grant, _ = self.setup_owner()
        with patch.object(f.runtime.ray, 'get_runtime_context', side_effect=AssertionError('runtime read')):
            report = owner.close(timeout=3)
        self.assertEqual(report['status'], 'closed')
        self.assertEqual(report['service_allocation']['status'], 'released')
        self.assertIsNone(owner.owner)
        self.assertEqual(f.events(), [])
        self.assertTrue(grant.released)

    def test_empty_finite_source_has_a_valid_delivery_and_no_model(self):
        f, owner, source, grant, _ = self.setup_owner(resident=False)
        empty = SimpleNamespace(preflight=source.preflight, scan_spec=source.scan_spec,
                                expected=lambda query: [], rows=lambda: iter([]))
        grant.plan.delivery = declare_ordinary_delivery('service-first', [],
            item_result_bytes=owner.capacity.item_result_bytes)
        try:
            self.assertEqual(list(owner.evaluate(empty, query_id='service-first')), [])
            self.assertEqual(f.events(), [])
            self.assertEqual(grant.events, ['adopt'])
        finally:
            report = owner.close(timeout=3)
        self.assertEqual(report['status'], 'closed', report)
        self.assertTrue(grant.released)

    def test_internal_chain_start_and_close_cannot_bypass_original_runtime(self):
        f, owner, _, grant, runtime = self.setup_owner()
        try:
            owner.start()
            original = dict(runtime)
            events = list(f.runtime.events)
            runtime['node_id'] = 'another-node'
            for operation in (owner.owner.start, owner.owner.close):
                with self.assertRaisesRegex(RuntimeError, 'runtime'):
                    operation()
            self.assertEqual(f.runtime.events, events)
            self.assertFalse(grant.released)
        finally:
            runtime.update(original)
            owner.close(timeout=3)
        self.assertTrue(grant.released)

    def test_proved_terminal_repeat_close_uses_no_replacement_runtime(self):
        f, owner, source, grant, _ = self.setup_owner()
        list(owner.evaluate(source, query_id='service-first'))
        report = owner.close(timeout=3)
        events = list(f.runtime.events)
        with patch.object(f.runtime.ray, 'is_initialized', return_value=False), \
                patch.object(f.runtime.ray, 'get_runtime_context', side_effect=AssertionError('runtime read')):
            repeated = owner.close(timeout=3)
        self.assertEqual(repeated['status'], report['status'])
        self.assertEqual(repeated['calls_count'], len(report['calls']))
        self.assertTrue(repeated['payload_transferred'])
        self.assertNotIn('calls', repeated)
        self.assertEqual(f.runtime.events, events)
        self.assertTrue(grant.released)

    def test_rejected_second_query_keeps_the_previous_actual_receipt_identity(self):
        _, owner, source, grant, runtime = self.setup_owner(queries=('service-first', 'service-second'))
        first = list(owner.evaluate(source, query_id='service-first'))
        old = owner.last_close_report
        original = dict(runtime)
        runtime['job_id'] = 'replacement-before-second-query'
        try:
            with self.assertRaisesRegex(RuntimeError, 'runtime'):
                owner.evaluate(source, query_id='service-second')
            self.assertEqual(owner.last_close_report['calls'], old['calls'])
            self.assertEqual({value['query_id'] for value in owner.last_close_report['calls']}, {'service-first'})
        finally:
            runtime.update(original)
            report = owner.close(timeout=3)
        self.assertEqual(report['status'], 'closed', report)
        self.assertEqual({value['call_id'] for value in report['calls']}, {value['call_id'] for value in first})
        self.assertTrue(grant.released)

    def test_snapshot_scalar_types_cannot_acknowledge_retained_receipts(self):
        _, owner, source, grant, _ = self.setup_owner()
        try:
            next(owner.evaluate(source, query_id='service-first'))
            report = owner.close(timeout=3)
            expected = report['unconsumed_call_results']
            for field, changed in (('call_index', False), ('submission_confirmed', 1),
                                   ('completion_tokens', True)):
                with self.subTest(field=field):
                    altered = deepcopy(expected)
                    altered[0][field] = changed
                    with self.assertRaisesRegex(ValueError, 'changed|acceptance'):
                        owner.take_unconsumed_results(expected=altered)
                    self.assertTrue(owner.has_unconsumed_results())
                    self.assertFalse(grant.released)
                    self.assertEqual(owner.last_close_report['unconsumed_call_results'], expected)
            self.assertEqual(owner.take_unconsumed_results(expected=expected), expected)
            self.assertTrue(grant.released)
        finally:
            owner.close(timeout=3)

    def test_terminal_service_retirement_keeps_only_metadata_and_independent_caller_report(self):
        _, owner, source, grant, _ = self.setup_owner()
        stream = owner.evaluate(source, query_id='service-first')
        rows = list(stream)
        before = deepcopy(stream.last_close_report)
        report = owner.close(timeout=3)
        snapshot = deepcopy(report)
        self.assertTrue(grant.released)
        self.assertIsNone(owner.owner)
        self.assertIsNone(owner._active)
        self.assertIsNone(stream.stream)
        self.assertIsNone(stream.results)
        self.assertEqual(report['calls'], before['calls'])
        self.assertEqual(stream.last_close_report['calls'], before['calls'])
        self.assertEqual({v['call_id'] for v in rows}, {v['call_id'] for v in report['calls']})
        retired = owner.last_close_report
        self.assertEqual(retired['calls_count'], 2)
        self.assertTrue(retired['payload_transferred'])
        def inspect(value):
            if type(value) is dict:
                self.assertFalse({'calls', 'results', 'unconsumed_core_results', 'text'} & set(value))
                for child in value.values():
                    inspect(child)
            elif type(value) is list:
                self.assertTrue(all(type(child) is str for child in value))
        inspect(retired)
        owner.close(timeout=3)
        self.assertEqual(report, snapshot)

    def test_retirement_metadata_selects_proof_fields_without_visiting_payload_objects(self):
        from src.execution_provider.adapters.lotus_records_batches import _retired_metadata
        class Payload:
            def __deepcopy__(self, memo):
                raise AssertionError('retirement visited an unselected payload')
        payload = Payload()
        report = dict(status='closed', source_closed=True, raw_result=dict(body=payload),
            body=payload, text=payload, prompt=payload, raw_response=payload,
            model_owner=dict(status='closed', engine_close_report=dict(status='closed',
                unconsumed_call_results=[payload], raw_result=dict(body=payload),
                engine_api_shutdown_confirmed=True)))
        self.assertEqual(_retired_metadata(report), dict(status='closed', source_closed=True,
            model_owner=dict(status='closed', engine_close_report=dict(status='closed',
                engine_api_shutdown_confirmed=True)), cleanup_errors=[]))

    def test_failed_retirement_keeps_the_original_owner_for_retry(self):
        _, owner, source, grant, _ = self.setup_owner()
        list(owner.evaluate(source, query_id='service-first'))
        producer = owner.owner
        with patch.object(grant, 'retire_producer', side_effect=RuntimeError('original retire refusal')):
            with self.assertRaisesRegex(RuntimeError, 'retire refusal'):
                owner.close(timeout=3)
        self.assertIs(owner.owner, producer)
        self.assertIs(grant.producer, producer)
        self.assertIsNone(owner._retired_report)
        self.assertFalse(grant.released)
        self.assertEqual(owner.close(timeout=3)['status'], 'closed')
        self.assertTrue(grant.released)


if __name__ == '__main__':
    unittest.main()
