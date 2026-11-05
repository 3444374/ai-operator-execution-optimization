"""Complete ordinary result projection accounting, with no runtime allocation."""
from copy import deepcopy
from dataclasses import FrozenInstanceError, asdict, replace
import threading
from types import SimpleNamespace
import unittest

from src.execution_provider.adapters.records_model_adapter import _encode
from src.scheduling.core.session import UINT64_MAX
from src.scheduling.core.session_contract import BatchKey, BatchMember, BackendTask, OfferedTask, SessionSpec, TaskKey
from src.semantic_methods.lotus.record_chain import RecordOccurrence, stage_identity
from src.semantic_methods.lotus.records import identity


class DeliveryDeclarationTests(unittest.TestCase):
    def module(self):
        from src.execution_provider.adapters import records_service_delivery
        return records_service_delivery

    def expected(self, query='q', count=3):
        return [identity(query, 'row-' + str(i), i) for i in range(count)]

    def declare(self, query='q', expected=None, maximum=4096):
        return self.module().declare_ordinary_delivery(query,
            self.expected(query) if expected is None else expected, item_result_bytes=maximum)

    def receipt(self, expected, *, count=3, status='succeeded', decision=True,
                maximum_key=UINT64_MAX, index=None):
        record = RecordOccurrence(expected['row_id'], expected['source_position'],
                                  expected['source_position'], {})
        chain = stage_identity(expected['query_id'], record, 'filter')
        value = dict(chain, status=status, submission_confirmed=True, text='fixed',
            prompt_tokens=3, completion_tokens=1, total_tokens=4,
            parent_call_ids=[], core_task_key=asdict(TaskKey(maximum_key, maximum_key)),
            transport_batch=asdict(BatchMember(BatchKey(maximum_key, maximum_key),
                count - 1 if index is None else index, count)))
        value = dict(value, chain_call_id=chain['call_id'], **expected)
        if decision is not None:
            value['decision'] = decision
        return value

    def test_declaration_covers_complete_projection_and_array_framing(self):
        expected = self.expected()
        declaration = self.declare(expected=expected)
        receipts = [self.receipt(item) for item in expected]
        self.assertEqual(declaration.query_id, 'q')
        self.assertEqual(declaration.count, 3)
        self.assertEqual(len(declaration.identity_sha256), 64)
        self.assertTrue(all(n > declaration.item_result_bytes for n in declaration.item_upper_bounds))
        self.assertEqual(declaration.array_bytes, sum(declaration.item_upper_bounds) +
            len(_encode([], declaration.array_bytes)) + len(receipts) - 1)
        self.assertEqual(self.module().check_ordinary_receipts(declaration, receipts, complete=True),
                         len(_encode(receipts, declaration.array_bytes)))

    def test_unicode_quotes_and_long_legal_identity_are_counted_as_UTF8_JSON(self):
        for query, row in [('查询\\"\t', '行\\"\n'), ('"' * 256, '\\' * 256), ('中' * 85, '行' * 85)]:
            with self.subTest(query_bytes=len(query.encode()), row_bytes=len(row.encode())):
                expected = [identity(query, row, UINT64_MAX)]
                declaration = self.declare(query, expected, maximum=16384)
                receipt = self.receipt(expected[0], count=1)
                self.assertEqual(self.module().check_ordinary_receipt(declaration, receipt),
                    len(_encode(receipt, declaration.item_upper_bounds[0])))
                self.assertLessEqual(len(_encode([receipt], declaration.array_bytes)), declaration.array_bytes)

    def test_full_raw_limit_plus_all_projection_fields_fit_the_declared_item(self):
        expected = self.expected(count=1)[0]
        declaration = self.declare(expected=[expected])
        chain = stage_identity('q', RecordOccurrence(expected['row_id'], 0, 0, {}), 'filter')
        raw = dict(chain, status='succeeded', submission_confirmed=True, text='',
                   prompt_tokens=3, completion_tokens=1, total_tokens=4)
        raw['text'] = 'x' * (declaration.item_result_bytes - len(_encode(raw, declaration.item_result_bytes)))
        self.assertEqual(len(_encode(raw, declaration.item_result_bytes)), declaration.item_result_bytes)
        projected = dict(raw, parent_call_ids=[], decision=False,
            core_task_key=asdict(TaskKey(UINT64_MAX, UINT64_MAX)),
            transport_batch=asdict(BatchMember(BatchKey(UINT64_MAX, UINT64_MAX), 0, 1)),
            chain_call_id=chain['call_id'], **expected)
        self.assertGreater(len(_encode(projected, declaration.item_upper_bounds[0])), declaration.item_result_bytes)
        self.assertLessEqual(self.module().check_ordinary_receipt(declaration, projected),
                             declaration.item_upper_bounds[0])

    def test_boolean_decisions_and_failed_unknown_without_decision_are_preserved(self):
        expected = self.expected(count=1)[0]
        declaration = self.declare(expected=[expected])
        for status, decision in [('succeeded', False), ('succeeded', True), ('failed', None), ('unknown', None)]:
            with self.subTest(status=status, decision=decision):
                receipt = self.receipt(expected, count=1, status=status, decision=decision)
                if status == 'unknown':
                    receipt.update(prompt_tokens=None, completion_tokens=None, total_tokens=None)
                original = deepcopy(receipt)
                self.module().check_ordinary_receipts(declaration, [receipt], complete=True)
                self.assertEqual(receipt, original)
                self.assertEqual(receipt['status'], status)
                self.assertEqual('decision' in receipt, decision is not None)

    def test_batch_count_digit_transitions_remain_covered_without_a_fixed_count_ceiling(self):
        for count in (1, 9, 10, 99, 100):
            with self.subTest(count=count):
                expected = self.expected(count=count)
                declaration = self.declare(expected=expected)
                receipt = self.receipt(expected[-1], count=count, index=count - 1)
                self.assertLessEqual(self.module().check_ordinary_receipt(declaration, receipt),
                                     max(declaration.item_upper_bounds))
                self.module().check_ordinary_receipts(declaration, [receipt])

    def test_existing_receipt_and_ordinary_projection_fit_after_a_full_model_result(self):
        from src.execution_provider.adapters.lotus_chain_actor_backend import LotusChainActorBackend
        from src.execution_provider.adapters.lotus_record_chain_core import SemLoomRecordChainCore
        from src.execution_provider.adapters.lotus_records_batches import PreparedRecordsCore
        from src.serving.backends.vllm_protocol import GenerationSpec

        identities = [('q', 'row'), ('查询\\"\t', '行\\"\n'), ('"' * 256, '\\' * 256)]
        for query, row in identities:
            expected = identity(query, row, UINT64_MAX)
            declaration = self.declare(query, [expected], maximum=16384)
            record = RecordOccurrence(row, UINT64_MAX, UINT64_MAX, {})
            chain = stage_identity(query, record, 'filter')
            generation = GenerationSpec()
            key = TaskKey(UINT64_MAX, UINT64_MAX)
            member = BatchMember(BatchKey(UINT64_MAX, UINT64_MAX), 0, 1)
            value = dict(row=dict(chain, messages_json='[]'), generation=asdict(generation), parent_call_ids=[])
            task = BackendTask(key, SessionSpec(query, 'chain-section-0', 'lotus-record-prepared-v1'),
                OfferedTask(UINT64_MAX, _encode(value, 65536), 1, 16384, profile_name='filter'), member)
            for status, decision in [('succeeded', False), ('succeeded', True), ('failed', None), ('unknown', None)]:
                with self.subTest(query=query, status=status, decision=decision):
                    owner = SimpleNamespace(_receipt_lock=threading.Lock(), _contexts={}, _raw_receipts={},
                        _stage_bindings={'filter': SimpleNamespace(stage_id='filter', model_role='main', generation=generation)},
                        _expected_bindings={row: record}, query_chain=SimpleNamespace(query_id=query))
                    SemLoomRecordChainCore._register_local_task(owner, task, value)
                    def transfer(key, receipt):
                        SemLoomRecordChainCore._record_local_receipt(owner, key, receipt)
                    backend = SimpleNamespace(on_result=transfer, _bulk={})
                    raw = dict(chain, status=status, submission_confirmed=True, text='',
                        prompt_tokens=3, completion_tokens=1, total_tokens=4)
                    raw['text'] = 'x' * (declaration.item_result_bytes - len(_encode(raw, declaration.item_result_bytes)))
                    self.assertEqual(len(_encode(raw, declaration.item_result_bytes)), declaration.item_result_bytes)
                    LotusChainActorBackend._result_transferred(backend, task, raw)
                    receipt = owner._raw_receipts[key]
                    if decision is not None:
                        receipt = dict(receipt, decision=decision)
                    wrapper = object.__new__(PreparedRecordsCore)
                    wrapper._expected = {row: expected}
                    projected = wrapper._ordinary(receipt)
                    preserved = deepcopy(projected)
                    size = self.module().check_ordinary_receipts(declaration, [projected], complete=True)
                    self.assertEqual(size, len(_encode([projected], declaration.array_bytes)))
                    self.assertEqual(projected, preserved)

    def test_empty_query_has_an_exact_empty_array_and_accepts_no_receipt(self):
        declaration = self.declare(expected=[])
        self.assertEqual((declaration.count, declaration.item_upper_bounds), (0, ()))
        self.assertEqual(declaration.array_bytes, len(_encode([], 4096)))
        self.assertEqual(self.module().check_ordinary_receipts(declaration, [], complete=True), 2)
        with self.assertRaises(ValueError):
            self.module().check_ordinary_receipt(declaration, self.receipt(self.expected()[0]))

    def test_declaration_is_deeply_immutable_and_does_not_alias_caller_data(self):
        expected = self.expected()
        declaration = self.declare(expected=expected)
        signature = declaration.identity_sha256
        expected[0]['row_id'] = 'changed-after-declaration'
        self.assertEqual(declaration.identity_sha256, signature)
        with self.assertRaises(FrozenInstanceError):
            declaration.array_bytes = 1
        with self.assertRaises(TypeError):
            declaration.occurrences[0][0] = 'changed'
        description = declaration.describe()
        description['item_upper_bounds'][0] = 0
        self.assertGreater(declaration.item_upper_bounds[0], 0)
        self.assertEqual(replace(declaration).identity_sha256, signature)

    def test_sessions_use_real_query_identity_and_choose_bytes_rather_than_max_count(self):
        short = ('short', self.expected('short', 3))
        long_query = '"' * 256
        long = (long_query, [identity(long_query, '\\' * 255 + str(i), i) for i in range(2)])
        session = self.module().declare_ordinary_session([short, long], item_result_bytes=4096)
        self.assertLess(session.for_query(long_query).count, session.for_query('short').count)
        self.assertGreater(session.for_query(long_query).array_bytes, session.for_query('short').array_bytes)
        self.assertEqual(session.array_bytes, session.for_query(long_query).array_bytes)
        changed = self.module().declare_ordinary_session([short, ('other', self.expected('other', 2))],
            item_result_bytes=4096)
        self.assertNotEqual(session.identity_sha256, changed.identity_sha256)
        self.assertEqual(session.bytes_for_copies(2), 2 * session.array_bytes)
        with self.assertRaises(ValueError):
            session.for_query('not-declared')
        with self.assertRaises(ValueError):
            session.for_query(None)

    def test_query_fingerprint_changes_for_actual_occurrence_and_is_order_independent(self):
        expected = self.expected()
        declaration = self.declare(expected=expected)
        self.assertEqual(declaration, self.declare(expected=expected[::-1]))
        immutable = tuple(tuple(v[n] for n in self.module().IDENTITY_FIELDS) for v in expected[::-1])
        self.assertEqual(declaration, self.module().OrdinaryDeliveryDeclaration('q', immutable, 4096))
        changed = expected[:-1] + [identity('q', 'different-row', 2)]
        self.assertNotEqual(declaration.identity_sha256, self.declare(expected=changed).identity_sha256)

    def test_duplicate_or_nonordinary_declarations_are_rejected(self):
        first = self.expected()[0]
        invalid = [[first, first], [first, identity('q', first['row_id'], 1)],
            [identity('q', 'map', 0, algorithm_stage='map')],
            [identity('q', 'role', 0, model_role='helper')],
            [identity('q', 'repeat', 0, call_index=1)], [dict(first, extra=True)],
            [dict(first, source_position=True)], [identity('other-query', 'row-0', 0)],
            [None], [tuple(first.values())], [{n: v for n, v in first.items() if n != 'call_id'}]]
        for expected in invalid:
            with self.subTest(expected=expected), self.assertRaises(ValueError):
                self.declare(expected=expected)
        for expected in (None, {}, iter([first])):
            with self.subTest(expected_type=type(expected).__name__), self.assertRaises(ValueError):
                self.module().declare_ordinary_delivery('q', expected, item_result_bytes=4096)

    def test_missing_invalid_storage_or_unrepresentable_identity_is_rejected(self):
        for maximum in (None, True, 0, -1, 2.5, float('inf')):
            with self.subTest(maximum=maximum), self.assertRaises(ValueError):
                self.declare(maximum=maximum)
        for query, expected in [('', []), (' ' , []), ('q' * 257, []),
                                ('q', [identity('q', 'r' * 257, 0)]),
                                ('q', [identity('q', 'row', UINT64_MAX + 1)])]:
            with self.subTest(query=query), self.assertRaises(ValueError):
                self.declare(query, expected)
        with self.assertRaises(ValueError):
            self.declare(maximum=1)

    def test_quantity_identity_duplicate_and_completion_checks_do_not_mutate_input(self):
        declaration = self.declare()
        receipt = self.receipt(self.expected()[0])
        snapshot = deepcopy(receipt)
        self.module().check_ordinary_receipts(declaration, [receipt])
        for receipts, complete in [([receipt], True), ([receipt, receipt], False),
                                    ([receipt] * 4, False), ([dict(receipt, row_id='foreign')], False),
                                    ([dict(receipt, chain_call_id='foreign-chain')], False)]:
            with self.subTest(complete=complete, count=len(receipts)), self.assertRaises(ValueError):
                self.module().check_ordinary_receipts(declaration, receipts, complete=complete)
        self.assertEqual(receipt, snapshot)
        with self.assertRaises(ValueError):
            self.module().check_ordinary_receipts(declaration, [receipt], complete=None)

    def test_actual_core_and_batch_fields_must_obey_the_counted_protocol(self):
        declaration = self.declare()
        original = self.receipt(self.expected()[0])
        variants = [dict(original, parent_call_ids=['foreign-parent']), dict(original, decision='False'),
            dict(original, core_task_key=dict(session_id=UINT64_MAX + 1, sequence=0)),
            dict(original, core_task_key=dict(session_id=True, sequence=0)),
            dict(original, core_task_key=dict(session_id=0, sequence=0, extra=1))]
        for index, size in [(-1, 3), (3, 3), (0, 4), (True, 3), (0, True)]:
            member = asdict(BatchMember(BatchKey(UINT64_MAX, UINT64_MAX), index, size))
            variants.append(dict(original, transport_batch=member))
        variants.append(dict(original, transport_batch=asdict(BatchMember(BatchKey(0, 0), 0, 3))))
        for receipt in variants:
            with self.subTest(receipt=receipt), self.assertRaises(ValueError):
                self.module().check_ordinary_receipt(declaration, receipt)

    def test_receipt_too_large_or_not_finite_JSON_is_rejected(self):
        declaration = self.declare()
        original = self.receipt(self.expected()[0])
        for receipt in [dict(original, text='x' * (max(declaration.item_upper_bounds) + 1)),
                        dict(original, text=float('nan')), dict(original, extra=object())]:
            with self.subTest(value=type(receipt['text']).__name__), self.assertRaises(ValueError):
                self.module().check_ordinary_receipt(declaration, receipt)

    def test_sessions_require_unique_queries_one_item_limit_and_explicit_copies(self):
        module = self.module()
        with self.assertRaises(ValueError):
            module.declare_ordinary_session([], item_result_bytes=4096)
        with self.assertRaises(ValueError):
            module.declare_ordinary_session([('q', self.expected()), ('q', self.expected())], item_result_bytes=4096)
        with self.assertRaises(ValueError):
            module.OrdinaryDeliverySession((self.declare(), self.declare('other', maximum=8192)))
        declaration = self.declare()
        for copies in (None, True, 0, -1, 1.5):
            with self.subTest(copies=copies), self.assertRaises(ValueError):
                declaration.bytes_for_copies(copies)
        self.assertEqual(declaration.bytes_for_copies(2), 2 * declaration.array_bytes)


if __name__ == '__main__':
    unittest.main()
