"""Original extra Movie tasks keep complete input and independent gold data."""

import csv
import hashlib
import importlib.util
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.baselines.text.movie_tasks import MovieTask, RATIO_INSTRUCTION
from src.baselines.text.movie_records import MovieRecordSource, prepare_movie_source
from src.baselines.text import sembench_movie


PANDAS = importlib.util.find_spec('pandas') is not None


@unittest.skipUnless(PANDAS, 'requires pandas; no model or external service')
class MovieExtraQueryTests(unittest.TestCase):
    def test_ratio_counts_all_eligible_occurrences_and_retains_its_original_prompt(self):
        task = MovieTask.from_config('movie-q4')
        rows = [dict(row_id='a', id='taken_3'), dict(row_id='b', id='other'),
                dict(row_id='c', id='taken_3'), dict(row_id='d', id='taken_3')]
        decisions = [dict(row_id='d', decision=True), dict(row_id='a', decision=True),
                     dict(row_id='c', decision=False)]
        result = task.encode_result(task.result_frame(rows, decisions))
        self.assertEqual(result, dict(positivity_ratio=2/3))
        self.assertEqual(task.method('fixture').instruction, RATIO_INSTRUCTION)
        task.validate_result(result, rows, decisions)
        with self.assertRaises(ValueError):
            task.result_frame(rows, decisions[:2])

    def test_empty_ratio_matches_original_zero_without_a_model_call(self):
        task = MovieTask(4)
        self.assertEqual(task.encode_result(task.result_frame([], [])), dict(positivity_ratio=0.0))

    def test_q4_source_preparation_keeps_labels_out_of_scan_and_counts_eligible_rows(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            path = root/'Reviews.csv'
            with path.open('w', newline='') as stream:
                writer=csv.DictWriter(stream, fieldnames=['reviewId','id','reviewText','scoreSentiment'])
                writer.writeheader()
                writer.writerows([dict(reviewId=str(i), id=movie, reviewText='review '+str(i),
                    scoreSentiment='POSITIVE') for i,movie in enumerate(('taken_3','other','taken_3'))])
            manifest=prepare_movie_source(path,root/'prepared',
                expected_csv_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                max_rows=3, task='movie-q4')
            source=MovieRecordSource.from_manifest(manifest)
            check=source.preflight(source.task.method('fixture'))
            self.assertEqual(check['eligible_rows'],2)
            self.assertTrue(all('scoreSentiment' not in row for row in source.rows()))
            self.assertEqual(source.scan_spec(source.task.method('fixture')).payload_columns,('reviewText',))

    def test_map_queries_remain_outside_the_filter_only_task(self):
        for query in (8,10):
            with self.subTest(query=query),self.assertRaises(ValueError):
                MovieTask(query)

    def test_extra_original_queries_dispatch_without_constructor_or_warmup(self):
        import pandas as pd
        frame=pd.DataFrame({'reviewText':['review']})
        marker=object()
        for query in (4,8,10):
            calls=[]
            method=lambda owner: calls.append(owner.load_data('Reviews.csv')) or marker
            runner=type('LotusRunner',(),{'__init__':lambda *_:self.fail('constructor called'),
                                        '_execute_q'+str(query):method})
            with self.subTest(query=query),patch.object(sembench_movie,'_load',return_value=SimpleNamespace(LotusRunner=runner)):
                self.assertIs(sembench_movie.run_original_lotus_query('checkout',query,lambda:frame),marker)
                self.assertEqual(len(calls),1)
                self.assertIs(calls[0],frame)

    def test_q10_never_substitutes_review_sentiment_for_movie_audience_score(self):
        import pandas as pd
        with self.assertRaisesRegex(ValueError,'Movies.csv audienceScore'):
            sembench_movie.evaluate_original('checkout',10,pd.DataFrame(),pd.DataFrame())


if __name__ == '__main__':
    unittest.main()
