"""Caller-owned ordinary Filter instructions; all examples run without inference."""
from dataclasses import asdict
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.semantic_methods.lotus.records import FilterMethod, identity


class InstructionContractTests(unittest.TestCase):
    def test_instruction_is_required_for_direct_construction(self):
        with self.assertRaises(TypeError):
            FilterMethod('fixture-model')

    def test_serialized_method_requires_explicit_instruction(self):
        with self.assertRaisesRegex(ValueError, 'instruction'):
            FilterMethod.from_dict(dict(model_name='fixture-model'))

    def test_empty_or_invalid_instruction_is_rejected(self):
        for value in ('', ' \n\t', None, 7, ['Keep {body}']):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, 'instruction'):
                FilterMethod.from_dict(dict(model_name='fixture-model', instruction=value))

    def test_nonmovie_method_round_trips_with_caller_owned_columns(self):
        method=FilterMethod('fixture-model', 'Keep {description} about {category}',
                            resolved_input_columns=('category', 'description'))
        self.assertEqual(FilterMethod.from_dict(asdict(method)), method)
        self.assertEqual(method.resolved_input_columns, ('category', 'description'))


class CallerFieldFormattingTests(unittest.TestCase):
    def prepare(self, instruction, values, columns=None):
        from src.semantic_methods.lotus.engine import bind_filter_method, LotusFilterMethod
        method=bind_filter_method(FilterMethod('fixture-model', instruction,
                                              resolved_input_columns=columns))
        occurrence=identity('generic-filter', 'row-a', 0)
        result=LotusFilterMethod(method).prepare_rows([dict(occurrence, **values)])[0]
        return method, occurrence, result

    def test_nonmovie_field_is_used_without_review_or_label_columns(self):
        method, occurrence, result=self.prepare('Keep plant descriptions: {description}',
                                                dict(description='an orchid with white petals'))
        self.assertEqual(method.resolved_input_columns, ('description',))
        self.assertIn('an orchid with white petals', result['messages_json'])
        self.assertEqual({name: result[name] for name in occurrence}, occurrence)

    def test_two_explicit_columns_preserve_order_and_call_identity(self):
        method, occurrence, result=self.prepare('Keep {body} related to {topic}',
            dict(body='white orchid petals', topic='botany'), ('topic', 'body'))
        self.assertEqual(method.resolved_input_columns, ('topic', 'body'))
        self.assertIn('white orchid petals', result['messages_json'])
        self.assertIn('botany', result['messages_json'])
        self.assertEqual({name: result[name] for name in occurrence}, occurrence)

    def test_movie_shaped_row_does_not_satisfy_a_different_caller_column(self):
        with self.assertRaisesRegex(TypeError, 'input columns'):
            self.prepare('Keep {description}', dict(reviewText='an unrelated field'))


class MovieEntrypointInstructionTests(unittest.TestCase):
    def test_movie_task_supplies_its_unchanged_q3_instruction(self):
        from src.baselines.text.movie_tasks import MovieTask
        self.assertEqual(MovieTask(3).method('fixture-model').instruction,
            'Determine if the following review is clearly positive. Review: {reviewText}')

    def test_movie_cascade_preflight_receives_explicit_task_method(self):
        from src.baselines.text.movie_tasks import MovieTask
        from src.experiments.lotus_cascade_movie import run_movie_cascade_query
        seen=[]
        class Source:
            def preflight(self, method):
                seen.append(method)
                raise RuntimeError('fixture stops before any model or evaluator')
        with self.assertRaisesRegex(RuntimeError, 'fixture stops'):
            run_movie_cascade_query(Source(), '/tmp/fixture-checkout',
                SimpleNamespace(model='fixture-main'), None, None, None,
                query_id='fixture', max_calls=1)
        self.assertEqual(seen, [MovieTask(3).method('fixture-main')])

    def check_invalid_config(self, method):
        from src.experiments.lotus_records_movie import read_config
        with tempfile.TemporaryDirectory() as root:
            file=Path(root)/'config.json'
            file.write_text(json.dumps(dict(schema='semloom.movie_records_run.v1',
                backend='ray-data-local-model', task='movie-q3', query_id='fixture',
                method=method, engine={})))
            with patch('src.serving.backends.vllm_engine.EngineSpec.from_dict') as engine:
                with self.assertRaisesRegex(ValueError, 'instruction'):
                    read_config(file)
                engine.assert_not_called()

    def test_movie_config_missing_instruction_fails_before_engine_binding(self):
        self.check_invalid_config(dict(model_name='fixture-model'))

    def test_movie_config_empty_instruction_fails_before_engine_binding(self):
        self.check_invalid_config(dict(model_name='fixture-model', instruction=' '))

    def test_cascade_helper_keeps_explicit_task_instruction(self):
        from src.baselines.text.movie_tasks import MovieTask
        from src.experiments.lotus_records_movie import _cascade_lms
        from src.serving.backends.vllm_engine import GenerationSpec
        method=MovieTask(3).method('fixture-main');seen=[]
        def lm(binding, generation):
            seen.append(binding)
            return SimpleNamespace(kwargs={}, encode_text=lambda label: [1 if label=='True' else 2])
        tokenizer=SimpleNamespace(encode=lambda label, **_: SimpleNamespace(ids=[1 if label=='True' else 2]))
        engines={role: SimpleNamespace(tokenizer_path='/tmp/fixture', model_path='/tmp/fixture')
                 for role in ('main', 'helper')}
        with patch('src.semantic_methods.lotus.engine.make_lm', side_effect=lm), \
                patch('tokenizers.Tokenizer.from_file', return_value=tokenizer):
            _cascade_lms(dict(helper_model_name='fixture-helper'), method,
                dict(main=GenerationSpec(), helper=GenerationSpec(logprobs=1)), engines)
        self.assertEqual([binding.model_name for binding in seen], ['fixture-main', 'fixture-helper'])
        self.assertEqual([binding.instruction for binding in seen], [method.instruction]*2)
