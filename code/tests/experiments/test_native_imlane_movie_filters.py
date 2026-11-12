"""Movie Filter task selection and complete original relation corner cases."""
from copy import deepcopy
from pathlib import Path
import tempfile
import unittest

import pandas as pd

from src.experiments import native_imlane_filters as binding
from src.experiments.native_imlane_relation import finish_mmqa_filters
from tests.experiments.test_native_imlane_filters import config
from tests.experiments.test_native_imlane_relation import CHECKOUT


class MovieConfigurationTests(unittest.TestCase):
    def test_positive_source_budget_and_no_other_movie_operator(self):
        value = config()
        value.update(schema=binding.MOVIE_SCHEMA, prompt_columns=['reviewText'])
        for query_id, count in [('1',2000), ('2',120), ('3',120), ('4',120)]:
            value.update(query_id=query_id, max_model_calls=count)
            self.assertEqual(binding.configuration(value)[0], value)
            for wrong in (count - 1, count + 1, 25):
                with self.subTest(query=query_id, calls=wrong):
                    self.assertEqual(binding.configuration(dict(value, max_model_calls=wrong))[0]['max_model_calls'], wrong)
        for changes in (dict(query_id='8'), dict(query_id='9'), dict(query_id='10'),
                        dict(query_id='3a'), dict(scale_factor=2000)):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                binding.configuration(dict(value, **changes))


@unittest.skipUnless(CHECKOUT, 'set SEMLOOM_NATIVE_TEST_CHECKOUT to pinned source')
class MovieRelationTests(unittest.TestCase):
    def test_original_head_count_ratio_with_duplicates_empty_and_reordered_results(self):
        from src.baselines.text.sembench_text import SemBenchTextQuery
        from src.serving.backends.vllm_protocol import GenerationSpec
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            frame = pd.DataFrame(dict(reviewId=[90,1,1,3,4,5,6,7,8],
                id=['other']+['taken_3']*8, reviewText=['same']*9))
            frame.to_csv(root/'Reviews.csv', index=False)
            for query_id in ('1','2','3','4'):
                query = SemBenchTextQuery(CHECKOUT, root, 'movie', query_id,
                    execution_id='movie-cpu', model_name='no-model', generation=GenerationSpec(max_tokens=8192))
                original = frame if query_id == '1' else frame.iloc[1:]
                pd.testing.assert_frame_equal(query.operation.frame, original)
                rows = [dict(occurrence_id='r'+str(i), source_position=i, title=str(row.reviewId))
                        for i, row in enumerate(original.itertuples())]
                for chosen in (list(reversed(range(len(rows)))), [2,0,1], [], [6]):
                    native = dict(return_type='TINYINT', full_filter_materialized_before_title_sink=True,
                        source_rows=len(rows), eligible_rows=len(rows), accepted_occurrences=[rows[i] for i in chosen])
                    result = finish_mmqa_filters(native, {query_id:(query, rows)})[query_id]
                    selected = original.iloc[sorted(chosen)]
                    if query_id in ('1','2'):
                        expected = (pd.DataFrame({'reviewId':selected.reviewId.head(5).tolist()}) if chosen
                            else pd.DataFrame(columns=['reviewId', 'movieId', 'reviewText']))
                    elif query_id == '3':
                        expected = pd.DataFrame({'positive_review_cnt':[len(chosen)]})
                    else:
                        expected = pd.DataFrame({'positivity_ratio':[len(chosen)/len(rows)]})
                    pd.testing.assert_frame_equal(result, expected, check_dtype=False)
                for change in ('duplicate','foreign','position','label','cardinality'):
                    native['accepted_occurrences'] = [deepcopy(rows[0])]
                    native['source_rows'] = len(rows)
                    if change == 'duplicate': native['accepted_occurrences'].append(rows[0])
                    elif change == 'foreign': native['accepted_occurrences'][0]['occurrence_id'] = 'missing'
                    elif change == 'position': native['accepted_occurrences'][0]['source_position'] = 8
                    elif change == 'label': native['accepted_occurrences'][0]['title'] = 'wrong'
                    else: native['source_rows'] += 1
                    with self.subTest(query=query_id, change=change), self.assertRaises(ValueError):
                        finish_mmqa_filters(native, {query_id:(query, rows)})


if __name__ == '__main__': unittest.main()
