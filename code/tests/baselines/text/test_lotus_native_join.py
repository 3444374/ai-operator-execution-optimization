"""Original exact Join bounds, duplicate occurrences and failure ownership."""
from contextlib import contextmanager
from copy import deepcopy
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import lotus
from lotus.types import LMOutput
import pandas as pd

from src.baselines.text.sembench_text import SemBenchTextQuery, _operation
from src.baselines.text.frameworks.lotus_native_join import audit, execute_native_join, finish_accounting, make_lm
from src.serving.backends.vllm_protocol import GenerationSpec
from src.infrastructure.private_artifacts import content_digest


INSTRUCTION = 'Same sentiment: {reviewText:left} and {reviewText:right}'


def operation(frame, right=None, instruction=INSTRUCTION):
    with lotus.settings.context(enable_cache=False):
        return _operation('join', frame, (frame if right is None else right,), dict(join_instruction=instruction),
            model_name='openai/local-qwen2.5-7b', generation=GenerationSpec(max_tokens=8192, stop=()),
            query_id='join-cpu')


class NativeJoinTests(unittest.TestCase):
    def setUp(self):
        self.frame = pd.DataFrame(dict(reviewId=[1, 1, 2, 2, 3, 3, 4, 4],
            id=['movie'] * 8, reviewText=['duplicate text'] * 8), index=range(8, 16))

    def test_multi_column_join_binds_native_positional_and_keyword_arguments(self):
        left = pd.DataFrame(dict(title=['same', 'same'], text=[{'description': 'one'}, {'description': 'two'}]),
                            index=[7, 7])
        right = pd.DataFrame(dict(title=['right', 'right', 'right'], text=['a', 'b', 'c']), index=[9, 9, 10])
        instruction = 'Same product: {title:left} {text:left}, {title:right} {text:right}'
        for args, kwargs in (((right, instruction), {}), ((right,), dict(join_instruction=instruction)),
                             ((), dict(other=right, join_instruction=instruction))):
            with self.subTest(args=len(args)), lotus.settings.context(enable_cache=False):
                op = _operation('join', left, args, kwargs, model_name='openai/local-fixture',
                                generation=GenerationSpec(), query_id='multi-column')
                self.assertEqual(len(op.records), 6)
                self.assertEqual(op.describe()['expected_model_calls'], 6)
                self.assertEqual([r.values['text:right'] for r in op.records], ['a', 'b', 'c'] * 2)
                self.assertEqual([r.values['text:left'] for r in op.records],
                    [str({'description': 'one'})] * 3 + [str({'description': 'two'})] * 3)
                self.assertEqual(len({r.row_id for r in op.records}), 6)

    def test_original_join_compares_all64_ordered_occurrences_before_id_exclusion(self):
        op = operation(self.frame)
        seen = []
        with lotus.settings.context(enable_cache=False):
            lm = make_lm(op, 4)
        def answer(current, messages, **kwargs):
            seen.extend(messages)
            return LMOutput(outputs=['True'] * len(messages))
        with lotus.settings.context(lm=lm, enable_cache=False), patch.object(type(lm), '__call__', answer):
            joined = self.frame.sem_join(self.frame, join_instruction=INSTRUCTION)
        self.assertEqual(len(joined), 64)
        self.assertEqual(len(seen), 64)
        self.assertEqual(len(set(str(message) for message in seen)), 1)
        self.assertEqual(op.describe()['expected_model_calls'], 64)
        self.assertEqual(len({r.row_id for r in op.records}), 64)
        self.assertEqual([r.source_position for r in op.records], list(range(64)))
        self.assertEqual([op.records[i].source_index for i in (0, 7, 8, 63)],
                         ['8:8', '8:15', '9:8', '15:15'])
        self.assertEqual(len(joined[joined['reviewId:left'] != joined['reviewId:right']]), 48)

    def test_original_invalid_boolean_defaults_true_without_another_model_request(self):
        with lotus.settings.context(enable_cache=False):
            lm = make_lm(operation(self.frame), 4)
        with lotus.settings.context(lm=lm, enable_cache=False), \
                patch.object(type(lm), '__call__', return_value=LMOutput(outputs=['unparseable'] * 64)) as call:
            joined = self.frame.sem_join(self.frame, join_instruction=INSTRUCTION)
        self.assertEqual(len(joined), 64)
        self.assertEqual(call.call_count, 1)

    def test_changed_join_policy_or_missing_sided_columns_is_not_this_binding(self):
        for args, kwargs in (((self.frame,), dict(join_instruction=INSTRUCTION, cascade_args='approximate')),
                ((self.frame,), dict(join_instruction='Same {reviewText:left} and {missing:right}')),
                ((self.frame,), dict(join_instruction='Same {reviewText}')),
                ((None,), dict(join_instruction=INSTRUCTION))):
            reason = 'unsupported semantic operator argument: cascade_args' if 'cascade_args' in kwargs else 'ordinary exact Join'
            with self.subTest(kwargs=kwargs), self.assertRaisesRegex(ValueError, reason):
                _operation('join', self.frame, args, kwargs, model_name='openai/local',
                           generation=GenerationSpec(), query_id='cpu')

    def test_rectangular_join_preserves_sided_columns_occurrences_and_original_sink(self):
        left = pd.DataFrame(dict(note=['equal', 'equal'], label=[7, 7]), index=['left-a', 'left-b'])
        right = pd.DataFrame(dict(summary=['one', 'two', 'three'], label=[8, 8, 9]),
                             index=['right-a', 'right-b', 'right-c'])
        instruction = 'Does {note:left} agree with {summary:right}?'
        op = operation(left, right, instruction)
        self.assertEqual(op.describe()['expected_model_calls'], 6)
        self.assertEqual(op.describe()['model_columns'], ['note:left', 'summary:right'])
        self.assertEqual([record.source_index for record in op.records],
                         [f'{l}:{r}' for l in left.index for r in right.index])
        self.assertEqual([record.values for record in op.records],
                         [dict(zip(('note:left', 'summary:right'), (l, r)))
                          for l in left.note for r in right.summary])
        self.assertEqual(len({record.row_id for record in op.records}), 6)
        texts = ['True', 'False', 'unparseable', 'False', 'True', 'True']
        reference_messages, captured = [], []
        lm = make_lm(op, 2)

        def reference_answer(current, messages, **kwargs):
            reference_messages.extend(deepcopy(messages))
            return LMOutput(outputs=texts)

        with lotus.settings.context(lm=lm, enable_cache=False), patch.object(type(lm), '__call__', reference_answer):
            reference = left.sem_join(right, join_instruction=instruction)

        @contextmanager
        def scope(lm, expected, budget, **kwargs):
            def report():
                return dict(calls=dict(counts=dict(succeeded=6, missing=0), records=[
                    dict(row, status='succeeded', text=text, messages_sha256=content_digest(prompt['messages']))
                    for row, text, prompt in zip(expected, texts, captured, strict=True)]))
            yield SimpleNamespace(capture_prompts=lambda prompts: captured.extend(deepcopy(prompts)), report=report)

        owner = SimpleNamespace(generation=op.generation, concurrency=2, query=scope)
        query = object.__new__(SemBenchTextQuery)
        query.files, query.operation = {}, op
        query.generation, query.model_name, query.query_id = op.generation, op.model_name, 'join-cpu'
        query._original = lambda: left.sem_join(right, join_instruction=instruction).head(3)
        with patch.object(type(lm), '__call__', return_value=LMOutput(outputs=texts)):
            result, report = query.execute(lambda actual, invoke: execute_native_join(actual, invoke=invoke,
                shared_owner=owner, query_id=query.query_id, budget=SimpleNamespace(remaining=6)))
        self.assertEqual(report['status'], 'native_returned')
        pd.testing.assert_frame_equal(report['frame'], reference)
        pd.testing.assert_frame_equal(result, reference.head(3))
        self.assertEqual([prompt['messages'] for prompt in captured], reference_messages)
        finish_accounting(report)
        audit(query, report, result, report['frame'])
        self.assertEqual(report['status'], 'completed')
        with self.assertRaisesRegex(ValueError, 'complete ordered-pair allowance'):
            execute_native_join(op, invoke=lambda: self.fail('accessor ran'),
                shared_owner=SimpleNamespace(generation=op.generation),
                query_id='short-budget', budget=SimpleNamespace(remaining=5))

    def test_changed_right_input_is_rejected_before_native_execution(self):
        right = self.frame.iloc[:2].copy()
        op = operation(self.frame, right)
        query = object.__new__(SemBenchTextQuery)
        query.files, query.operation = {}, op
        query.generation, query.model_name, query.query_id = op.generation, op.model_name, 'join-cpu'
        query._original = lambda: self.frame.sem_join(right, join_instruction=INSTRUCTION)
        right.loc[8, 'reviewText'] = 'changed after preflight'
        with self.assertRaisesRegex(ValueError, 'changed after preflight'):
            query.execute(lambda *args: self.fail('native operator ran with changed right input'))

    def test_captured_join_requires_explicit_lotus_resident_for_each_backend(self):
        from src.experiments import sembench_text_query as entry
        query = SimpleNamespace(operation=operation(self.frame))
        for backend, lifecycle in (('lotus-native', 'isolated'), ('ray-data', 'resident'),
                                   ('daft', 'resident'), ('imlane', 'resident')):
            with self.subTest(backend=backend), self.assertRaisesRegex(ValueError, 'explicit native LOTUS resident'):
                entry.check(dict(backend=backend, lifecycle=lifecycle), query, None)

    def test_incomplete_pair_allowance_stops_before_native_owner(self):
        op = operation(self.frame)
        owner = SimpleNamespace(generation=op.generation)
        with self.assertRaisesRegex(ValueError, 'complete ordered-pair allowance'):
            execute_native_join(op, invoke=lambda: self.fail('accessor ran'), shared_owner=owner,
                                query_id='cpu', budget=SimpleNamespace(remaining=63))

    def test_primary_native_error_retains_identity_when_accounting_also_fails(self):
        op = operation(self.frame)
        binding = SimpleNamespace(capture_prompts=lambda value: None,
            report=lambda: (_ for _ in ()).throw(RuntimeError('accounting failed')))
        @contextmanager
        def scope(*args, **kwargs):
            yield binding
        owner = SimpleNamespace(generation=op.generation, concurrency=4, query=scope)
        with lotus.settings.context(enable_cache=False), \
                patch('lotus.models.LM.__call__', side_effect=ValueError('primary model failure')):
            with self.assertRaisesRegex(ValueError, 'primary model failure') as caught:
                execute_native_join(op, invoke=lambda: self.frame.sem_join(self.frame, join_instruction=INSTRUCTION),
                    shared_owner=owner, query_id='cpu', budget=SimpleNamespace(remaining=64))
        report = caught.exception.operator_report
        self.assertEqual(len(report['prompts']), 64)
        self.assertEqual(report['accounting_error_type'], 'RuntimeError')

    def resident_clock(self, *, accounting_failure=False):
        from src.experiments import sembench_text_resident as resident
        from src.experiments import sembench_text_query as entry

        op = operation(self.frame)
        clock, events, prompts = [100_000_000_000], [], []
        unit = SimpleNamespace(attempts=0, remaining=64)
        owner = SimpleNamespace(generation=op.generation, concurrency=4, transport=None, ready_report={})

        def accounting():
            events.append(('receipt-accounting', clock[0]))
            clock[0] += 11_000_000_000
            if accounting_failure:
                raise ValueError('post-sink accounting failed')
            calls = [dict(row, status='succeeded', text='True', engine_request_id=str(i),
                          messages_sha256=content_digest(prompt['messages']))
                     for i, (row, prompt) in enumerate(zip(owner.expected, prompts, strict=True))]
            return dict(calls=dict(records=calls, counts=dict(succeeded=64, missing=0)))

        @contextmanager
        def scope(lm, expected, budget, **kwargs):
            owner.expected = expected
            yield SimpleNamespace(capture_prompts=lambda values: prompts.extend(deepcopy(values)), report=accounting)
            unit.attempts, unit.remaining = 64, 0
        owner.query = scope

        def snapshot():
            events.append(('snapshot', clock[0]))
            clock[0] += 7_000_000_000
            return dict(active_query=False, tainted=False, transport=None, ready=dict(startup_seconds=0))
        owner.snapshot = snapshot
        owner.close = lambda **kwargs: events.append(('close', clock[0])) or dict(status='closed')

        def execute(operator):
            value = operator(op, lambda: self.frame.sem_join(self.frame, join_instruction=INSTRUCTION))
            # The original complete query's post-Join relation work.
            clock[0] += 3_000_000_000
            return value['frame'].head(1), value
        def evaluate(result):
            events.append(('evaluation', clock[0]))
            clock[0] += 13_000_000_000
            return dict(software_fixture=True)
        query = SimpleNamespace(operation=op, query_id='clock-query', execute=execute,
            bind_execution=lambda: execute, verify_source=lambda: None,
            describe=lambda: dict(expected_model_calls=64), evaluate=evaluate)
        ledger = SimpleNamespace(claim_shared_unit=lambda value: unit,
                                 close_shared_unit=lambda value: events.append(('unit-close', clock[0])))
        config = dict(profile=dict(native_lm_max_batch_size=4), resident=dict(cleanup_s=1))
        driver = resident.ResidentDriver(config, None, None, None, op.generation, None, ledger)
        driver.owner = owner
        def emit(event):
            events.append((event, clock[0]))
            return clock[0]
        with patch.object(entry, 'query_from_config', return_value=query), \
                patch.object(resident, 'verify_inputs'), \
                patch.object(resident, 'native_request_boundaries', return_value={}), \
                patch.object(resident.time, 'monotonic_ns', side_effect=lambda: clock[0]), \
                patch('lotus.models.LM.__call__', side_effect=lambda current, messages, **kwargs:
                      LMOutput(outputs=['True'] * len(messages)), autospec=True):
            try:
                result = driver.query(dict(query_id='clock-query', temperature='cold', unit_id='cold'), emit)
            except BaseException as error:
                result = error
            finally:
                driver.close()
        return result, events

    def test_complete_sink_precedes_receipt_summary_snapshot_and_evaluation_costs(self):
        result, events = self.resident_clock()
        if isinstance(result, BaseException):
            raise result
        self.assertEqual(result['source_to_sink_seconds'], 3)
        self.assertEqual(result['sink_ready_ns'], 103_000_000_000)
        self.assertEqual(events[:3], [('result_ready', 103_000_000_000),
                                     ('receipt-accounting', 103_000_000_000), ('snapshot', 114_000_000_000)])
        self.assertGreater(result['evaluation_finished_ns'], result['sink_ready_ns'] + 30_000_000_000)
        self.assertEqual(events[-1][0], 'close')

    def test_receipt_failure_after_sink_preserves_observed_result_and_closes_unit(self):
        result, events = self.resident_clock(accounting_failure=True)
        self.assertIsInstance(result, ValueError)
        self.assertEqual(str(result), 'post-sink accounting failed')
        self.assertEqual(result.query_report['source_to_sink_seconds'], 3)
        self.assertEqual(result.query_report['sink_ready_ns'], 103_000_000_000)
        self.assertNotEqual(result.query_report['status'], 'completed')
        self.assertEqual([event[0] for event in events], ['result_ready', 'receipt-accounting', 'unit-close', 'close'])


if __name__ == '__main__':
    unittest.main()
