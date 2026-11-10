"""Source-keyed native parsing and full relational sink, without a model."""

from types import SimpleNamespace
import unittest

import lotus
import pandas as pd

from src.baselines.text.sembench_text import _operation
from src.baselines.text.sembench_prepared import prepare, finish
from src.serving.backends.vllm_protocol import GenerationSpec


class PreparedSemanticsTests(unittest.TestCase):
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
        return SimpleNamespace(operation=operation, query_id='fixture', execute=execute)

    def prepared(self, kind):
        query = self.query(kind)
        method, calls, requests = prepare(query, SimpleNamespace(apply_chat_template=lambda m, **kw: str(m)))
        return query, method, calls

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
            with self.subTest(predictions=predictions), self.assertRaises(ValueError):
                finish(query, method, calls, predictions)


if __name__ == '__main__':
    unittest.main()
