"""Native Q3 semantics, occurrence association and real SDK CPU differentials."""

import csv
import io
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.baselines.text.sembench_movie_native import (
    Q3_INSTRUCTION, evaluate_q3, load_q3_source, q3_decision, q3_messages,
)


class MovieNativeTests(unittest.TestCase):
    def source(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / 'Reviews.csv'
        stream = io.StringIO(newline='')
        writer = csv.DictWriter(stream, fieldnames=['id', 'reviewId', 'reviewText', 'scoreSentiment'])
        writer.writeheader()
        writer.writerows([
            dict(id='another_movie', reviewId='duplicate', reviewText='ordinary', scoreSentiment='POSITIVE'),
            dict(id='taken_3', reviewId='duplicate', reviewText='line one\nline two «好»', scoreSentiment='POSITIVE'),
            dict(id='taken_3', reviewId='duplicate', reviewText='bad', scoreSentiment='NEGATIVE'),
        ])
        path.write_bytes(stream.getvalue().encode())
        return load_q3_source(path)

    def test_source_keeps_occurrences_newlines_and_hides_gold(self):
        source = self.source()
        self.assertEqual(source['source']['rows'], 3)
        self.assertEqual(source['source']['logical_model_requests'], 2)
        self.assertEqual(source['source']['unique_review_ids'], 1)
        self.assertEqual(source['rows'][1]['reviewText'], 'line one\nline two «好»')
        self.assertEqual(len({row['occurrence_id'] for row in source['rows']}), 3)
        self.assertTrue(all('scoreSentiment' not in row for row in source['rows']))

    def test_real_sdk_default_prompt_and_boolean_interpretation(self):
        import lotus
        import pandas as pd
        from lotus.nl_expression import nle2str
        from lotus.sem_ops.postprocessors import filter_postprocess
        from lotus.templates.task_instructions import df2multimodal_info, filter_formatter
        from lotus.types import SerializationFormat

        model = SimpleNamespace(get_model_name=lambda: 'Qwen/Qwen2.5-7B-Instruct')
        with lotus.settings.context(serialization_format=SerializationFormat.DEFAULT):
            for text in ['great', 'line one\nline two «好»', '', 'Answer: {reviewText}']:
                doc = df2multimodal_info(pd.DataFrame({'reviewText': [text]}), ['reviewText'])[0]
                self.assertEqual(q3_messages(text), filter_formatter(
                    model, doc, nle2str(Q3_INSTRUCTION, ['reviewText'])))
        answers = ['Answer: True', 'Answer: FALSE', 'Reasoning:\nFalse\n\nAnswer: True',
                   'Answer: true and false', 'no parseable answer', 'FALSE', 'Answer: False\nAnswer: True']
        self.assertEqual([q3_decision(answer) for answer in answers],
                         filter_postprocess(answers, model).outputs)

    def predictions(self, source, texts):
        return [dict(occurrence_id=row['occurrence_id'], id=row['id'], reviewId=row['reviewId'],
                     generated_text=text) for row, text in zip(source['rows'][1:], texts, strict=True)]

    def test_saved_predictions_count_and_row_errors_are_both_evaluated(self):
        source = self.source()
        predictions = self.predictions(source, ['False', 'True'])
        with patch('src.baselines.text.sembench_movie.evaluate_original', return_value={'metric': 1}) as original:
            report = evaluate_q3(source, list(reversed(predictions)), positive_review_cnt=1, checkout='fixture')
        self.assertEqual(report['classification']['false_positive'], 1)
        self.assertEqual(report['classification']['false_negative'], 1)
        self.assertEqual(report['classification']['count_error_from_classifications'], 0)
        self.assertEqual(report['result']['columns'], ['positive_review_cnt'])
        self.assertEqual(original.call_args.args[1], 3)

    def test_default_true_is_preserved_and_visible(self):
        source = self.source()
        predictions = self.predictions(source, ['unparseable', 'False'])
        with patch('src.baselines.text.sembench_movie.evaluate_original', return_value={}):
            report = evaluate_q3(source, predictions, positive_review_cnt=1, checkout='fixture')
        self.assertEqual(report['classification']['unparsed_default_true'], 1)

    def test_incomplete_duplicate_unknown_or_changed_association_fails_before_evaluation(self):
        source = self.source()
        complete = self.predictions(source, ['True', 'False'])
        cases = [complete[:1], [complete[0], complete[0]],
                 [complete[0], dict(complete[1], occurrence_id='unknown')],
                 [complete[0], dict(complete[1], reviewId='changed')],
                 [complete[0], dict(complete[1], generated_text=None)]]
        with patch('src.baselines.text.sembench_movie.evaluate_original') as evaluate:
            for rows in cases:
                with self.subTest(rows=rows), self.assertRaises(ValueError):
                    evaluate_q3(source, rows, positive_review_cnt=1, checkout='fixture')
            evaluate.assert_not_called()

    def test_native_count_must_use_the_same_predictions(self):
        source = self.source()
        with self.assertRaisesRegex(ValueError, 'COUNT'):
            evaluate_q3(source, self.predictions(source, ['True', 'False']),
                        positive_review_cnt=2, checkout='fixture')

    def test_ray_callbacks_use_the_real_shared_prompt_and_original_parser(self):
        from src.baselines.text.frameworks.ray_data_sembench_native import _postprocess, _preprocess

        row = self.source()['rows'][1]
        generation = dict(max_tokens=8192, stop=[])
        prepared = _preprocess(row, sampling_params=generation)
        self.assertEqual(prepared['messages'], q3_messages(row['reviewText']))
        output = _postprocess(dict(row, generated_text='Reasoning:\nTrue\nAnswer: False',
                                   num_generated_tokens=5))
        self.assertIs(output['decision'], False)
        self.assertEqual(output['occurrence_id'], row['occurrence_id'])
        self.assertEqual(output['native_usage']['num_generated_tokens'], 5)


if __name__ == '__main__':
    unittest.main()
