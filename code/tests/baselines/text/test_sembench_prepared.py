"""Source-keyed native parsing and full relational sink, without a model."""

from types import SimpleNamespace
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import lotus
import pandas as pd

from src.baselines.text.sembench_text import _operation
from src.baselines.text.sembench_prepared import prepare, finish
from src.serving.backends.vllm_protocol import GenerationSpec


class PreparedSemanticsTests(unittest.TestCase):
    def test_named_arguments_and_output_columns_match_native_accessors(self):
        from copy import deepcopy
        from src.semantic_methods.lotus.engine import engine_response
        from src.semantic_methods.lotus.record_method import LotusRecordMethod
        from tests.experiments.test_sembench_text_original import _reply

        for column in ('text', 'productDescriptors'):
            frame = pd.DataFrame({column: [{'description': 'first'}, {'description': 'second'}],
                                  'category': ['old-first', 'old-second']}, index=[7, 7])
            instruction = 'Describe {' + column + '}'
            cases = [
                ('filter', (), dict(user_instruction=instruction), ['True', 'False']),
                ('map', (instruction,), dict(suffix='category'), ['first category', 'second category']),
                ('map', (), dict(user_instruction=instruction, suffix='category'), ['first category', 'second category']),
                ('extract', (), dict(input_cols=[column], output_cols={'category': 'category'}),
                 ['{"category":"first"}', '{"category":"second"}']),
                ('extract', ([column],), dict(output_cols={'category': 'category'}),
                 ['{"category":"first"}', '{"category":"second"}']),
            ]
            for kind, args, kwargs, texts in cases:
                with self.subTest(column=column, kind=kind, args=args, kwargs=kwargs):
                    operation = _operation(kind, frame, args, kwargs, model_name='openai/CPU-only',
                        generation=GenerationSpec(), query_id='named-arguments')
                    reference = LotusRecordMethod(operation.stage)
                    observed = []
                    def respond(data, options, *unused):
                        observed.extend(deepcopy(message) for message, _ in data)
                        return [engine_response(_reply(dict(call_id=str(i)), text), reference.lm)
                                for i, text in enumerate(texts)]
                    reference.lm._process_uncached_messages = respond
                    with lotus.settings.context(lm=reference.lm, enable_cache=False):
                        expected = getattr(frame.copy(deep=True), 'sem_' + kind)(*args, **kwargs)
                    def execute(handler):
                        report = handler(operation, None)
                        return report['frame'], report
                    query = SimpleNamespace(operation=operation, query_id='named-arguments', execute=execute)
                    method, prepared, requests = prepare(query)
                    self.assertEqual([row['messages'] for row in requests], observed)
                    predictions = [dict(call_id=row['call_id'], generated_text=text)
                                   for row, text in zip(prepared, texts, strict=True)]
                    actual, _ = finish(query, method, prepared, predictions[::-1])
                    pd.testing.assert_frame_equal(actual, expected)

    def test_duplicate_or_unsupported_accessor_arguments_are_not_silently_dropped(self):
        frame = pd.DataFrame({'text': ['input']})
        for kind, args, kwargs in (
                ('map', ('Label {text}',), dict(user_instruction='other')),
                ('map', ('Label {text}',), dict(return_explanations=True)),
                ('extract', (['text'],), dict(input_cols=['text'], output_cols={'label': None})),
                ('extract', (), dict(input_cols=['text'])),
                ('extract', (), dict(input_cols=['text'], output_cols={'label': None}, extract_quotes=True))):
            with self.subTest(kind=kind, kwargs=kwargs), self.assertRaises(ValueError):
                _operation(kind, frame, args, kwargs, model_name='openai/CPU-only',
                    generation=GenerationSpec(), query_id='invalid-arguments')

    def test_non_row_operator_is_rejected_before_stage_or_model_access(self):
        for kind in ('join', 'topk', 'aggregate'):
            query = SimpleNamespace(operation=SimpleNamespace(describe=lambda: dict(kind=kind)))
            with self.subTest(kind=kind), self.assertRaisesRegex(ValueError, 'Filter/Map/Extract, not ' + kind):
                prepare(query)

    def query(self, kind):
        frame = pd.DataFrame({'text': ['first', 'second'], 'title': ['b', 'a']}, index=[8, 3])
        args = (['text'], {'genre': None}) if kind == 'extract' else ('Describe {text}',)
        with lotus.settings.context(enable_cache=False):
            operation = _operation(kind, frame, args, {}, model_name='openai/CPU-only',
                generation=GenerationSpec(), query_id='fixture')
        def execute(handler):
            report = handler(operation, lambda: self.fail('model accessor invoked'))
            result = report['frame'].sort_index()
            return result, report
        query = SimpleNamespace(operation=operation, query_id='fixture', execute=execute,
                                verify_source=lambda: None)
        query.bind_execution = lambda: query.execute
        return query

    def prepared(self, kind):
        query = self.query(kind)
        method, calls, requests = prepare(query, SimpleNamespace(apply_chat_template=lambda m, **kw: str(m)))
        return query, method, calls

    def test_explicit_raw_columns_reuse_default_filter_map_extract_messages(self):
        for kind in ('filter', 'map', 'extract'):
            with self.subTest(kind=kind):
                query = self.query(kind)
                method, _, original = prepare(query)
                bound, _, requests = prepare(query, prompt_columns=list(method.columns))
                self.assertEqual(requests, original)
                self.assertEqual(bound.columns, method.columns)
                for invalid in (['Text'], ['text', 'text'], []):
                    with self.assertRaisesRegex(ValueError, 'original prompt columns'):
                        prepare(query, prompt_columns=invalid)

    def test_filter_map_extract_reordered_predictions_preserve_source_rows_and_original_parser(self):
        cases = {'filter': ['True then False', 'unrecognized'],
                 'map': ['first output', 'second output'],
                 'extract': ['{"genre":"comedy"}', '{"genre":null}']}
        for kind, texts in cases.items():
            with self.subTest(kind=kind):
                query, method, calls = self.prepared(kind)
                predictions = [dict(call_id=row['call_id'], generated_text=text)
                               for row, text in zip(calls, texts)]
                result, semantic = finish(query, method, calls, predictions[::-1])
                self.assertEqual(result.index.tolist(), [3, 8])
                self.assertEqual(result.loc[8, 'text'], 'first')
                if kind == 'filter':
                    # Original LOTUS True-before-False and unrecognized default=True.
                    self.assertEqual(semantic['parsed_outputs'], [True, True])
                elif kind == 'map':
                    self.assertEqual(result.loc[8, '_map'], 'first output')
                else:
                    self.assertEqual(result.loc[8, 'genre'], 'comedy')
                    # Native LOTUS stringifies JSON null; do not repair its semantics.
                    self.assertEqual(result.loc[3, 'genre'], 'None')

    def test_missing_duplicate_and_unrelated_predictions_fail_before_original_sink(self):
        query, method, calls = self.prepared('map')
        good = [dict(call_id=row['call_id'], generated_text='answer') for row in calls]
        for predictions in (good[:1], [good[0], good[0]],
                            [good[0], dict(call_id='unrelated', generated_text='answer')]):
            ready = Mock()
            with self.subTest(predictions=predictions), self.assertRaises(ValueError):
                finish(query, method, calls, predictions, on_result_ready=ready)
            ready.assert_not_called()

    def run_driver(self, root, now, *, preflight_seconds=0):
        from src.experiments import native_daft_sembench as driver
        from src.infrastructure.private_artifacts import publish_private_json

        query = self.query('map')
        execute = query.execute
        def original_sink(handler):
            value = execute(handler)
            now[0] += 3_000_000_000
            return value
        query.execute = original_sink
        query.describe = lambda: {'expected_model_calls': 2}
        query.evaluate = lambda result: {'rows': len(result)}
        method, calls, requests = prepare(query, SimpleNamespace(apply_chat_template=lambda m, **kw: str(m)))
        predictions = [dict(call_id=row['call_id'], generated_text='answer') for row in calls]
        config = dict(schema='semloom.native_daft_sembench.v1',sembench_checkout='unused',data_dir='unused',
            scenario='movie',query='8',query_id='clock-fixture',model_name='openai/CPU-only',model='CPU-only',
            sampling_params=dict(n=1),engine_kwargs={},provider_options={},query_timeout_s=1200,
            cleanup_s=120,stop_grace_s=8,max_model_calls=2)
        def supervised(config_path, directory, **options):
            directory.mkdir()
            raw = dict(result_ready_monotonic_ns=now[0],result=dict(predictions=predictions))
            publish_private_json(directory/'result-ready.json',raw)
            options['on_result_ready'](raw)
            publish_private_json(directory/'worker.json',dict(status='completed',native_api={},
                model_close_receipt={'status':'closed'}))
            return {'status':'CPU fixture'}
        def preflight(*args, **kwargs):
            now[0] += preflight_seconds * 1_000_000_000
            return query
        with patch.object(driver,'SemBenchTextQuery',side_effect=preflight), \
                patch.object(driver,'prepare',return_value=(method,calls,requests)), \
                patch.object(driver.time,'monotonic_ns',side_effect=lambda:now[0]), \
                patch.object(driver.time,'monotonic',side_effect=lambda:now[0]/1e9):
            return driver.run_query(config,root,model_python='CPU-only',tokenizer=object(),supervise=supervised)

    def test_source_preflight_is_excluded_and_original_relational_work_is_timed(self):
        with tempfile.TemporaryDirectory() as directory:
            report = self.run_driver(Path(directory)/'run', [1_000_000_000], preflight_seconds=17)
        self.assertEqual(report['source_to_sink_seconds'], 3)
        self.assertEqual(report['preflight_finished_ns'] - report['worker_entry_ns'], 17_000_000_000)
        self.assertEqual(report['query_clock_version'], 'sembench.source_to_sink.v3')

    def test_message_digests_and_relation_serialization_advance_only_post_sink_clock(self):
        from src.baselines.text import sembench_prepared as semantics
        from src.experiments import native_daft_sembench as driver
        now = [1_000_000_000]
        digest, serialize = semantics.content_digest, semantics.result_value
        observed = []
        def audit_digest(value):
            observed.append(('digest',now[0]));now[0] += 7_000_000_000
            return digest(value)
        def audit_relation(value):
            observed.append(('relation',now[0]));now[0] += 11_000_000_000
            return serialize(value)
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(semantics,'content_digest',side_effect=audit_digest), \
                patch.object(semantics,'result_value',side_effect=audit_relation), \
                patch.object(driver,'result_value',side_effect=audit_relation):
            report = self.run_driver(Path(directory)/'run',now)
        self.assertEqual(report['status'],'completed')
        self.assertEqual(report['source_to_sink_seconds'],3)
        self.assertEqual([kind for kind,_ in observed],['digest','digest','relation','relation','relation'])
        self.assertTrue(all(at >= report['sink_ready_ns'] for _,at in observed))
        self.assertEqual(report['semantic_audit_finished_ns']-report['sink_ready_ns'],25_000_000_000)
        self.assertLessEqual(report['semantic_audit_finished_ns'],report['sink_persistence_started_ns'])

    def test_post_sink_audit_failure_keeps_native_output_and_failed_driver_receipt(self):
        from src.baselines.text import sembench_prepared as semantics
        from src.infrastructure.private_artifacts import read_json
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)/'run'
            with patch.object(semantics,'content_digest',side_effect=ValueError('CPU audit failure')), \
                    self.assertRaisesRegex(ValueError,'CPU audit failure'):
                self.run_driver(root,[1_000_000_000])
            report = read_json(root/'driver.json')
            self.assertEqual(report['status'],'failed')
            self.assertEqual(report['error_type'],'ValueError')
            self.assertEqual(report['source_to_sink_seconds'],3)
            self.assertNotIn('semantic_audit_finished_ns',report)
            self.assertNotIn('quality',report)
            self.assertEqual(len(read_json(root/'native-worker/result-ready.json')['result']['predictions']),2)
            self.assertFalse((root/'result-ready.json').exists())


if __name__ == '__main__':
    unittest.main()
