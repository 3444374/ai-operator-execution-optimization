"""Original extra Movie tasks keep complete input and independent gold data."""

import csv
import hashlib
import importlib.util
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.baselines.text.movie_tasks import MovieTask, RATIO_INSTRUCTION
from src.baselines.text.movie_records import MovieRecordSource, prepare_movie_source
from src.baselines.text import sembench_movie


PANDAS = importlib.util.find_spec('pandas') is not None
ORIGINAL_CHECKOUT = os.environ.get('SEMLOOM_NATIVE_TEST_CHECKOUT')
ORIGINAL_SDK = bool(ORIGINAL_CHECKOUT and Path(ORIGINAL_CHECKOUT).is_dir()
                    and importlib.util.find_spec('lotus') is not None)
ORIGINAL_METRICS = ORIGINAL_SDK and all(importlib.util.find_spec(name) is not None
    for name in ('duckdb','scipy','cdlib','overrides'))


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
        for query in (8,9,10):
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

    def test_q9_requires_an_explicit_native_ranking_algorithm_before_loading_source(self):
        with patch.object(sembench_movie, '_load') as load:
            for ranking in (None, 'quick', 'approximate', ''):
                with self.subTest(ranking=ranking), self.assertRaisesRegex(ValueError, 'explicit ranking'):
                    sembench_movie.run_original_lotus_query('checkout',9,lambda:None,ranking=ranking)
            with self.assertRaisesRegex(ValueError, 'only.*Q9'):
                sembench_movie.run_original_lotus_query('checkout',8,lambda:None,ranking='map')
        load.assert_not_called()

    def test_q9_passes_both_original_algorithm_choices_without_constructor_or_fallback(self):
        observed=[]
        class Runner:
            def __init__(self):
                raise AssertionError('benchmark setup must not run')
            def _execute_q9(self):
                observed.append((self.ranking,self.policy,self.load_data('Reviews.csv')))
                return self.ranking
        marker=object()
        with patch.object(sembench_movie,'_load',return_value=SimpleNamespace(LotusRunner=Runner)):
            for ranking in ('map','topk'):
                self.assertEqual(sembench_movie.run_original_lotus_query('checkout',9,
                    lambda:marker,ranking=ranking),ranking)
        self.assertEqual(observed,[('map','exact',marker),('topk','exact',marker)])

    def test_join_entry_retains_original_class_helpers_without_constructing_an_executor(self):
        observed=[]
        class Runner:
            def __init__(self):
                raise AssertionError('benchmark setup must not run')
            def _configure_lotus_for_join_type(self,kind):
                observed.append((kind,self.policy))
            def _get_empty_results_dataframe(self,query):
                return ('original empty',query)
        for query in (5,6,7):
            def implementation(owner,query=query):
                owner._configure_lotus_for_join_type('text')
                return owner._get_empty_results_dataframe(query)
            setattr(Runner,'_execute_q'+str(query),implementation)
        with patch.object(sembench_movie,'_load',return_value=SimpleNamespace(LotusRunner=Runner)):
            for query in (5,6,7):
                self.assertEqual(sembench_movie.run_original_lotus_query('checkout',query,lambda:None),
                                 ('original empty',query))
        self.assertEqual(observed,[('text','exact')]*3)

    @unittest.skipUnless(ORIGINAL_SDK, 'requires SDK and pinned public checkout')
    def test_original_empty_join_queries_keep_their_upstream_schema(self):
        import pandas as pd
        from src.baselines.text.sembench_source import load_module
        frame=pd.DataFrame(columns=['reviewId','id','reviewText'])
        module=load_module(ORIGINAL_CHECKOUT,'sembench_movie_empty_test',
                           'src/scenario/movie/runner/lotus_runner/lotus_runner.py')
        original=object.__new__(module.LotusRunner)
        for query,empty_query in ((5,5),(6,6),(7,6)):
            with self.subTest(query=query):
                actual=sembench_movie.run_original_lotus_query(ORIGINAL_CHECKOUT,query,lambda:frame)
                expected=original._get_empty_results_dataframe(empty_query)
                pd.testing.assert_frame_equal(actual,expected)

    @unittest.skipUnless(ORIGINAL_SDK, 'requires SDK and pinned public checkout')
    def test_exact_join_keeps_all_pairs_before_id_rejection_and_limit(self):
        import lotus
        from lotus.models import LM
        from litellm.types.utils import ModelResponse
        import pandas as pd
        frame=pd.DataFrame(dict(reviewId=['a','a','b','c','d'],
            id=['ant_man_and_the_wasp_quantumania']*5,
            reviewText=['software review '+str(index) for index in range(5)]))
        for query,returned in ((5,10),(6,10),(7,18)):
            batches=[]
            lm=LM('openai/local-join-fixture',max_tokens=8192)
            def respond(data,options,*args):
                batches.append(len(data))
                return [ModelResponse(model=lm.model,id='software-'+str(index),
                    choices=[dict(index=0,finish_reason='stop',message=dict(role='assistant',content='True'))],
                    usage=dict(prompt_tokens=0,completion_tokens=0,total_tokens=0))
                    for index in range(len(data))]
            lm._process_uncached_messages=respond
            with self.subTest(query=query),lotus.settings.context(lm=lm,enable_cache=False):
                result=sembench_movie.run_original_lotus_query(ORIGINAL_CHECKOUT,query,lambda:frame)
                self.assertEqual(batches,[25])
                self.assertEqual(len(result),returned)
                self.assertEqual(list(result.columns),['id','reviewId','reviewId2'])
                self.assertTrue(all(result.reviewId!=result.reviewId2))

    @unittest.skipUnless(ORIGINAL_METRICS, 'requires pinned checkout and original evaluator dependencies')
    def test_q9_original_rank_metric_uses_original_score_not_sentiment_labels(self):
        import pandas as pd
        reviews=pd.DataFrame(dict(reviewId=['a','b','c'],
            id=['ant_man_and_the_wasp_quantumania']*3,
            originalScore=['1/5','3/5','5/5'],scoreSentiment=['POSITIVE']*3))
        for predictions,expected in (([1.0,3.0,5.0],1.0),([5.0,3.0,1.0],-1.0)):
            with self.subTest(predictions=predictions):
                result=pd.DataFrame(dict(reviewId=['a','b','c'],reviewScore=predictions))
                metric=sembench_movie.evaluate_original(ORIGINAL_CHECKOUT,9,result,reviews)['original_metric']
                self.assertAlmostEqual(metric['spearman_correlation'],expected)
                self.assertAlmostEqual(metric['kendall_tau'],expected)


if __name__ == '__main__':
    unittest.main()
