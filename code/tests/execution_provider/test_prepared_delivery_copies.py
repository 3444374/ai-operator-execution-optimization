"""Prepared result lifetimes and copies through the existing actual CPU child."""
from copy import deepcopy
from dataclasses import replace
import threading
import time
import unittest
from unittest.mock import patch

from src.baselines.text.movie_records import MovieRecordSource
from src.execution_provider.adapters import records_service_delivery as delivery
from src.execution_provider.adapters.records_async_model import AsyncCoreModelActor
from src.execution_provider.adapters.records_model_adapter import _encode
from src.semantic_methods.lotus.record_chain import RecordOccurrence, stage_identity
from src.semantic_methods.lotus.records import identity
from tests.experiments.movie_token_resident_fixture import MovieTokenResidentFixture


class _DiscardedCalls(list):
    def __deepcopy__(self, memo):
        raise AssertionError('the chain calls field is discarded by ordinary projection')


class PreparedDeliveryCopyTests(unittest.TestCase):
    def source(self, fixture):
        return MovieRecordSource.from_manifest(fixture.manifests['movie-q1'])

    def test_report_does_not_copy_the_chain_calls_it_immediately_replaces(self):
        f = MovieTokenResidentFixture(self)
        owner = f.owner('RF', resident=True)
        try:
            self.assertEqual(len(list(owner.evaluate(self.source(f), query_id='copy-report'))), 2)
            expected = owner.last_close_report
            chain_report = owner.owner.last_close_report
            chain_report['calls'] = _DiscardedCalls(chain_report['calls'])
            chain_report['copy_probe'] = dict(values=[1])
            projected = owner.last_close_report
            self.assertEqual(projected['calls'], expected['calls'])
            self.assertEqual(projected['unconsumed_call_results'], [])
            projected['copy_probe']['values'].append(2)
            self.assertEqual(chain_report['copy_probe'], dict(values=[1]))
            self.assertEqual(owner.last_query_report['calls'], expected['calls'])
        finally:
            if owner.owner is not None:
                report = owner.owner.last_close_report
                if report is not None and type(report.get('calls')) is _DiscardedCalls:
                    report['calls'] = list(report['calls'])
            owner.close(timeout=3)

    def test_permanent_close_clears_iteration_references_and_keeps_unconsumed_results(self):
        for close_from in ('owner', 'iterator'):
            with self.subTest(close_from=close_from):
                f = MovieTokenResidentFixture(self)
                owner = f.owner('RF', resident=True)
                stream = owner.evaluate(self.source(f), query_id='close-' + close_from)
                try:
                    first = next(stream)
                    self.assertIsNotNone(stream.results)
                    self.assertIsNotNone(stream.stream)
                    before = deepcopy(owner.last_close_report)
                    report = owner.close(timeout=3) if close_from == 'owner' else stream.close()
                    self.assertIsNone(stream.results)
                    self.assertIsNone(stream.stream)
                    self.assertEqual(report['status'], 'closed')
                    self.assertEqual(len(report['calls']), 2)
                    retained = report['unconsumed_call_results']
                    self.assertEqual(len(retained), 1)
                    self.assertNotEqual(retained[0]['call_id'], first['call_id'])
                    self.assertEqual(retained[0]['total_tokens'], 14)
                    self.assertEqual(retained[0]['status'], 'succeeded')
                    snapshot = deepcopy(report)
                    again = owner.close(timeout=3)
                    self.assertEqual(again['unconsumed_call_results'], retained)
                    self.assertEqual(report, snapshot)
                    self.assertEqual(before['unconsumed_call_results'], retained)
                    self.assertEqual(list(stream), [])
                finally:
                    owner.close(timeout=3)

    def test_completed_drained_query_reuses_its_existing_close_calls_for_iteration(self):
        f = MovieTokenResidentFixture(self)
        owner = f.owner('RF', resident=True)
        try:
            stream = owner.evaluate(self.source(f), query_id='reuse-close-calls')
            inner = owner.owner
            original = type(inner).calls
            late_reads = []
            def observed_calls(actual):
                if actual is inner and getattr(actual._active, 'completed', False):
                    late_reads.append(True)
                return original.fget(actual)
            with patch.object(type(inner), 'calls', property(observed_calls)):
                first = next(stream)
            self.assertEqual(late_reads, [])
            self.assertEqual(first['status'], 'succeeded')
            public = owner.last_close_report
            public['calls'][1]['text'] = 'caller changed its own report'
            second = next(stream)
            self.assertNotEqual(second['text'], public['calls'][1]['text'])
            self.assertEqual(len({first['call_id'], second['call_id']}), 2)
            self.assertEqual(list(stream), [])
        finally:
            owner.close(timeout=3)

    def test_missing_actual_RPC_drain_keeps_the_original_call_read(self):
        f = MovieTokenResidentFixture(self)
        owner = f.owner('RF', resident=True)
        try:
            stream = owner.evaluate(self.source(f), query_id='keep-live-calls')
            inner, actual_stream = owner.owner, stream.stream
            original_calls = type(inner).calls
            original_progress = inner._actors._data_rpc_progress
            late_reads = []
            def observed_calls(actual):
                if actual is inner and getattr(actual._active, 'completed', False):
                    late_reads.append(True)
                return original_calls.fget(actual)
            def pending_after_query():
                value = original_progress()
                return replace(value, pending=1) if actual_stream.completed else value
            with patch.object(type(inner), 'calls', property(observed_calls)), \
                    patch.object(inner._actors, '_data_rpc_progress', pending_after_query):
                first = next(stream)
            self.assertTrue(late_reads)
            self.assertEqual(first['status'], 'succeeded')
            self.assertEqual(len(list(stream)), 1)
        finally:
            owner.close(timeout=3)

    def test_resident_queries_keep_original_calls_and_previous_iterator_reports(self):
        for mode in ('RF', 'RD'):
            with self.subTest(mode=mode):
                f = MovieTokenResidentFixture(self)
                owner = f.owner(mode, resident=True)
                try:
                    ready = owner.start()
                    source = self.source(f)
                    first = owner.evaluate(source, query_id='resident-copy-first')
                    first_values = list(first)
                    saved = first.last_close_report
                    original = deepcopy(saved)
                    self.assertIsNone(first.results)
                    self.assertIsNone(first.stream)
                    self.assertTrue(saved['model_retained'])
                    second_values = list(owner.evaluate(source, query_id='resident-copy-second'))
                    self.assertEqual(first.last_close_report, original)
                    self.assertEqual(saved, original)
                    self.assertEqual({v['engine_uid'] for v in first_values + second_values}, {ready['engine_uid']})
                    self.assertEqual([v['fixture_messages'] for v in first_values],
                                     [v['fixture_messages'] for v in second_values])
                    self.assertEqual([v['total_tokens'] for v in first_values], [14, 14])
                    self.assertEqual([v['total_tokens'] for v in second_values], [14, 14])
                    self.assertEqual(sum(e['event'] == 'construct' for e in f.events()), 1)
                finally:
                    report = owner.close(timeout=3)
                self.assertEqual(report['status'], 'closed')
                self.assertEqual(first.last_close_report, original)

    def test_unknown_close_keeps_late_actual_calls_visible_and_old_reports_unchanged(self):
        f = MovieTokenResidentFixture(self)
        gate = f.root / 'allow-late-unknown'
        held = next(key for key, value in f.replies.items() if value['text'] == 'False')
        f.replies[held].update(status='unknown', wait_file=str(gate))
        f.write_replies()
        release, close_entered = threading.Event(), threading.Event()
        original_close, original_abort = AsyncCoreModelActor.close, AsyncCoreModelActor.abort_core
        def held_close(actor, timeout):
            close_entered.set()
            if not release.wait(5):
                raise TimeoutError('CPU test did not release close')
            return original_close(actor, timeout)
        def held_abort(actor, key):
            if not release.wait(5):
                raise TimeoutError('CPU test did not release abort')
            return original_abort(actor, key)
        owner = f.owner('RF', resident=True)
        with patch.object(AsyncCoreModelActor, 'close', held_close), \
                patch.object(AsyncCoreModelActor, 'abort_core', held_abort):
            try:
                stream = owner.evaluate(self.source(f), query_id='late-copy')
                actual = stream.stream
                deadline = time.monotonic() + 4
                while len(owner.owner.calls) != 1 or not any(e['event'] == 'start' and e['token'] == held for e in f.events()):
                    actual._tick()
                    self.assertLess(time.monotonic(), deadline)
                early = owner.close(timeout=.02)
                early_snapshot = deepcopy(early)
                self.assertEqual(early['status'], 'unknown')
                self.assertIsNone(stream.results)
                self.assertIsNone(stream.stream)
                gate.touch()
                deadline = time.monotonic() + 4
                while not any(e['event'] == 'end' and e['token'] == held for e in f.events()):
                    self.assertLess(time.monotonic(), deadline)
                    time.sleep(.002)
                release.set()
                final = owner.close(timeout=3)
                self.assertTrue(close_entered.is_set())
                self.assertEqual(final['status'], 'closed', final)
                self.assertEqual(sorted((v['status'], v['total_tokens']) for v in final['calls']),
                                 [('succeeded', 14), ('unknown', 14)])
                self.assertEqual(final['unconsumed_call_results'], final['calls'])
                self.assertEqual(early, early_snapshot)
                self.assertEqual(owner.last_query_report['calls'], final['calls'])
                self.assertEqual(owner.close(timeout=3)['calls'], final['calls'])
            finally:
                gate.touch()
                release.set()
                owner.close(timeout=3)


class DeliveryArrayCopyTests(unittest.TestCase):
    def test_array_check_uses_items_without_encoding_the_full_result_array(self):
        expected = [identity('q', 'row-' + str(i), i) for i in range(3)]
        declaration = delivery.declare_ordinary_delivery('q', expected, item_result_bytes=4096)
        receipts = []
        for row in expected:
            chain = stage_identity('q', RecordOccurrence(row['row_id'], row['source_position'], 0, {}), 'filter')
            receipts.append(dict(chain, chain_call_id=chain['call_id'], parent_call_ids=[],
                status='succeeded', submission_confirmed=True, prompt_tokens=3,
                completion_tokens=1, total_tokens=4, text='中文\\"', **row))
        actual = len(_encode(receipts, declaration.array_bytes))
        original = delivery._encode
        def count_one(value, maximum):
            if type(value) in (list, tuple) and value and type(value[0]) is dict:
                raise AssertionError('the complete delivery array is already measured per item')
            return original(value, maximum)
        with patch.object(delivery, '_encode', count_one):
            self.assertEqual(delivery.check_ordinary_receipts(declaration, receipts, complete=True), actual)
            self.assertEqual(delivery.check_ordinary_receipts(declaration, []), len(_encode([], 4096)))
        self.assertEqual(receipts[0]['text'], '中文\\"')


if __name__ == '__main__':
    unittest.main()
