"""Independent MMQA Extract contracts and fixed original-query CPU references."""

import ast
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest
from unittest.mock import patch

from src.baselines.text.sembench_native_mmqa import MMQATextExtractTask
from src.baselines.text.sembench_native_text import task_from_config


class NativeMMQAExtractTests(unittest.TestCase):
    def inputs(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        self.write(root/'ben_piazza.csv', ['Title', 'Role'], [
            ('Ignored', 'Other Role'), ('Target', 'Bob Whitewood'), ('Target', 'Bob Whitewood'),
            ('Missing', 'Bob Whitewood'), ('NullTarget', 'Bob Whitewood'),
        ])
        self.write(root/'ben_piazza_text_data.csv', ['title', 'text'], [
            ('Target', 'director evidence «好»\nline two'), ('Target', 'director evidence «好»\nline two'),
            ('Unused', 'must still be extracted'), ('NullTarget', 'no director stated'),
        ])
        self.write(root/'lizzy_caplan_text_data.csv', ['title', 'text'], [
            ('Excluded', 'not in original title predicate'), ('Mean Girls', 'genres one'),
            ('Orange County', 'genres two'), ('Mean Girls', 'duplicate genre row'), ('Cobweb', 'unknown genres'),
        ])
        return root

    @staticmethod
    def write(path, columns, rows):
        stream = io.StringIO(newline='')
        writer = csv.writer(stream)
        writer.writerow(columns)
        writer.writerows(rows)
        path.write_bytes(stream.getvalue().encode())

    @staticmethod
    def replies(task, source, values):
        return [dict(call_id=row['call_id'], generated_text=json.dumps({task.field: value}))
                for row, value in zip(task.requests(source), values, strict=True)]

    def official_checkout(self):
        checkout = os.environ.get('SEMLOOM_NATIVE_TEST_CHECKOUT')
        if not checkout:
            self.skipTest('bind the fixed public SemBench checkout for original scorer/reference checks')
        return checkout

    def test_factory_uses_only_mmqa_data_dir_and_never_reads_during_construction(self):
        with patch.object(Path, 'read_bytes', side_effect=AssertionError('constructor I/O')):
            for query in ['Q1', 'Q4']:
                config = dict(scenario='mmqa', data_dir='/not-read', query=query, query_id='mmqa-'+query,
                              model='runtime-field-ignored')
                task = task_from_config(config)
                self.assertIs(type(task), MMQATextExtractTask)
                self.assertEqual(task.execution_id, config['query_id'])
                self.assertEqual(task.query, query[1:])
                self.assertIsNone(task.prepare_evaluation())
                for foreign in ['reviews_csv', 'movies_csv', 'ranking']:
                    with self.subTest(foreign=foreign), self.assertRaises(TypeError):
                        task_from_config(dict(config, **{foreign: None}))
            with self.assertRaises(ValueError):
                MMQATextExtractTask('/not-read', 'Q3a')

    def test_q1_extracts_all_text_before_merge_and_keeps_series_index_duplicates_and_nan(self):
        import pandas as pd

        task = MMQATextExtractTask(self.inputs(), 'Q1', 'directors')
        source = task.read_source()
        self.assertEqual(task.model_request_count(source), 4)
        predictions = self.replies(task, source, ['Michael Ritchie', 'Second Director', 'Unused Director', None])
        result = task.finish(source, reversed(predictions))
        expected = pd.Series(['Michael Ritchie', 'Second Director', 'Michael Ritchie', 'Second Director',
                              float('nan'), 'None'], index=[1, 2, 3, 4, 5, 6], name='director', dtype=object)
        pd.testing.assert_series_equal(result, expected)
        encoded = task.encode_result(result)
        self.assertEqual(encoded, dict(kind='series', name='director', index=[1, 2, 3, 4, 5, 6],
            data=['Michael Ritchie', 'Second Director', 'Michael Ritchie', 'Second Director', 'NaN', 'None'], dtype='object'))
        json.dumps(encoded, allow_nan=False)

    def test_q4_preserves_comma_split_strip_expansion_duplicates_null_and_group_sort(self):
        import pandas as pd

        task = MMQATextExtractTask(self.inputs(), 'Q4')
        source = task.read_source()
        self.assertEqual(source.positions, (1, 2, 3, 4))
        self.assertEqual(task.model_request_count(source), 4)
        result = task.finish(source, reversed(self.replies(task, source,
            ['Comedy, Drama', 'Comedy, ,Comedy', '', None])))
        expected = pd.DataFrame(dict(genre=['', 'Comedy', 'Drama', 'None'], movies_in_genre=[
            'Orange County, Mean Girls', 'Mean Girls, Orange County, Orange County', 'Mean Girls', 'Cobweb']))
        pd.testing.assert_frame_equal(result, expected)
        self.assertEqual(task.encode_result(result)['columns'], ['genre', 'movies_in_genre'])

    def test_messages_are_text_only_and_same_text_keeps_separate_occurrences(self):
        for query in ['Q1', 'Q4']:
            task = MMQATextExtractTask(self.inputs(), query, 'occurrences')
            source = task.read_source()
            requests = task.requests(source)
            self.assertEqual(len({row['call_id'] for row in requests}), len(requests))
            for request, position in zip(requests, source.positions, strict=True):
                payload = json.loads(request['messages'][1]['content'])
                self.assertEqual(payload, dict(text=source.frames[task.text_file].iloc[position]['text']))
                self.assertEqual([item['role'] for item in request['messages']], ['system', 'user'])
            if query == 'Q1':
                self.assertEqual(requests[0]['messages'], requests[1]['messages'])
                self.assertNotEqual(requests[0]['call_id'], requests[1]['call_id'])
            serialized = json.dumps([row['messages'] for row in requests])
            for hidden in ['Bob Whitewood', 'Mean Girls', 'Orange County', 'NullTarget', 'Title', 'Role', 'Excluded']:
                self.assertNotIn(hidden, serialized)
            self.assertEqual(task.response_format, {'type': 'json_object'})

    def test_strict_string_or_null_json_and_complete_occurrence_coverage(self):
        for query in ['Q1', 'Q4']:
            task = MMQATextExtractTask(self.inputs(), query)
            source = task.read_source()
            complete = self.replies(task, source, ['value']*task.model_request_count(source))
            cases = [complete[:-1], complete+[complete[0]],
                     complete[:-1]+[dict(complete[-1], call_id='foreign')]]
            for rows in cases:
                with self.subTest(query=query, rows=rows), self.assertRaises(ValueError):
                    task.finish(source, rows)
            field = task.field
            for response in [None, 'not JSON', '[]', '{"wrong":"value"}',
                             '{"'+field+'":1}', '{"'+field+'":true}', '{"'+field+'":[]}',
                             '{"'+field+'":"a","extra":null}', '{"'+field+'":"a","'+field+'":"b"}']:
                with self.subTest(query=query, response=response), self.assertRaises(ValueError):
                    task.finish(source, [dict(complete[0], generated_text=response)]+complete[1:])

    def test_missing_files_columns_and_changed_inputs_are_distinct_failures(self):
        root = self.inputs()
        task = MMQATextExtractTask(root, 'Q1')
        source = task.read_source()
        metadata = task.describe(source)
        self.assertEqual(metadata['source']['ben_piazza.csv']['sha256'],
                         hashlib.sha256(source.raw_files['ben_piazza.csv']).hexdigest())
        self.assertIn("null becomes the string 'None'", metadata['value_materialization'])
        with patch.object(Path, 'read_bytes', side_effect=AssertionError('metadata/size I/O')):
            self.assertEqual(task.model_request_count(source), 4)
            self.assertEqual(task.describe(source)['expected_model_calls'], 4)
        table = root/'ben_piazza.csv'
        table.write_bytes(table.read_bytes()+b'\n')
        with self.assertRaisesRegex(ValueError, 'changed'):
            task.verify_source(source)
        task.verify_source(task.read_source())
        self.write(table, ['Title'], [('Target',)])
        with self.assertRaisesRegex(ValueError, 'Title/Role'):
            task.read_source()
        (root/'lizzy_caplan_text_data.csv').unlink()
        with self.assertRaises(FileNotFoundError):
            MMQATextExtractTask(root, 'Q4').read_source()

    def test_original_empty_frontier_errors_are_not_replaced_with_successful_empty_results(self):
        root = self.inputs()
        self.write(root/'ben_piazza_text_data.csv', ['title', 'text'], [])
        self.write(root/'lizzy_caplan_text_data.csv', ['title', 'text'], [('Excluded', 'unused')])
        for query, missing in [('Q1', 'director'), ('Q4', 'genre')]:
            task = MMQATextExtractTask(root, query)
            source = task.read_source()
            self.assertEqual(task.model_request_count(source), 0)
            self.assertEqual(task.requests(source), [])
            with self.assertRaisesRegex(KeyError, missing):
                task.finish(source, [])

    def test_original_scorer_receives_series_at_post_query_bridge_and_retains_its_behavior(self):
        from src.baselines.text.sembench_mmqa import evaluate_original

        root = self.inputs()
        self.write(root/'ben_piazza.csv', ['Title', 'Role'], [('Target', 'Bob Whitewood')])
        self.write(root/'ben_piazza_text_data.csv', ['title', 'text'], [('Target', 'one'), ('Target', 'two')])
        task = MMQATextExtractTask(root, 'Q1')
        source = task.read_source()
        result = task.finish(source, self.replies(task, source, ['Michael Ritchie', 'Michael Ritchie']))
        with patch('src.baselines.text.sembench_mmqa.evaluate_original', return_value={}) as score:
            task.evaluate(source, result, checkout=self.official_checkout())
            self.assertIs(score.call_args.args[2], result)
        quality = task.evaluate(source, result, checkout=self.official_checkout())
        self.assertEqual(quality, evaluate_original(self.official_checkout(), '1', result))
        self.assertAlmostEqual(quality['original_metric']['f1_score'], 4/3)
        unmatched_task = MMQATextExtractTask(self.inputs(), 'Q1')
        unmatched = unmatched_task.read_source()
        failed_quality_result = unmatched_task.finish(unmatched, self.replies(unmatched_task, unmatched, ['x']*4))
        with self.assertRaises(AttributeError):
            unmatched_task.evaluate(unmatched, failed_quality_result, checkout=self.official_checkout())
        self.assertEqual(unmatched_task.encode_result(failed_quality_result)['data'][-2], 'NaN')

    def test_complete_original_functions_and_real_sdk_software_answers_match_relations_and_errors(self):
        import importlib.util
        from contextlib import ExitStack, redirect_stdout
        import socket

        checkout = self.official_checkout()
        if any(importlib.util.find_spec(name) is None for name in ('lotus', 'litellm')):
            self.skipTest('real LOTUS SDK required only for the software original-query reference')
        with ExitStack() as scope:
            scope.enter_context(patch.dict(os.environ, dict(HF_HUB_OFFLINE='1', TRANSFORMERS_OFFLINE='1',
                HF_DATASETS_OFFLINE='1', LITELLM_LOCAL_MODEL_COST_MAP='True', CUDA_VISIBLE_DEVICES='')))
            scope.enter_context(patch.object(socket.socket, 'connect', side_effect=AssertionError('network forbidden')))
            import lotus
            import pandas as pd
            from tests.experiments.test_sembench_text_original import OriginalSemBenchTextTests
            from src.serving.backends.vllm_protocol import GenerationSpec
            from src.serving.backends.vllm_engine import VllmEngine
            from src.serving.backends.vllm_async_engine import VllmAsyncEngine

            for engine in (VllmEngine, VllmAsyncEngine):
                scope.enter_context(patch.object(engine, '__init__', side_effect=AssertionError('model forbidden')))
            scope.enter_context(lotus.settings.context(enable_cache=False))
            original = OriginalSemBenchTextTests()
            original.generation = GenerationSpec(max_tokens=32, temperature=0, seed=0)
            q4_source = Path(checkout)/'src/scenario/mmqa/runner/lotus_runner/lotus_runner.py'
            module = ast.parse(q4_source.read_text())
            method = next(node for owner in module.body if isinstance(owner, ast.ClassDef) for node in owner.body
                          if isinstance(node, ast.FunctionDef) and node.name == '_execute_q4')
            titles = next(ast.literal_eval(node.value) for node in method.body if isinstance(node, ast.Assign)
                          and any(isinstance(target, ast.Name) and target.id == 'target_values' for target in node.targets))
            cases = [('q1-duplicate-null-unmatched', 'Q1'), ('q1-empty-table', 'Q1'), ('q1-empty-text', 'Q1'),
                     ('q4-all-original-titles-null-duplicates', 'Q4'), ('q4-empty-frontier', 'Q4')]
            for case, query in cases:
                with self.subTest(case=case), redirect_stdout(io.StringIO()):
                    root = self.inputs()
                    if case == 'q1-empty-table':
                        self.write(root/'ben_piazza.csv', ['Title', 'Role'], [])
                    if case == 'q1-empty-text':
                        self.write(root/'ben_piazza_text_data.csv', ['title', 'text'], [])
                    if query == 'Q4':
                        selected_titles = titles+[titles[1], titles[1]] if 'all-original' in case else []
                        self.write(root/'lizzy_caplan_text_data.csv', ['title', 'text'],
                            [('Excluded', 'unused')]+[(title, 'text '+str(i)) for i, title in enumerate(selected_titles)])
                    task = MMQATextExtractTask(root, query)
                    source = task.read_source()
                    count = task.model_request_count(source)
                    values = (['Michael Ritchie', 'Second Director', 'Unused Director', None] if query == 'Q1' and count
                              else [None if i % 4 == 0 else ['', 'Comedy, Drama', ' comedy, ,Comedy'][i % 3]
                                    for i in range(count)])
                    answers = [json.dumps({task.field: value}) for value in values]
                    original.data = root
                    reference = original._query('mmqa', task.query)
                    self.assertEqual(reference.operation.describe()['expected_model_calls'], count)
                    expected_error = actual_error = None
                    try:
                        expected, batches = original._reference(reference, answers)
                    except (KeyError, AttributeError) as error:
                        expected_error = error
                    try:
                        actual = task.finish(source, reversed(self.replies(task, source, values)))
                    except (KeyError, AttributeError) as error:
                        actual_error = error
                    self.assertEqual(type(actual_error), type(expected_error))
                    if expected_error is not None:
                        self.assertEqual(str(actual_error), str(expected_error))
                    else:
                        self.assertEqual(sum(len(messages) for messages, _ in batches), count)
                        if query == 'Q1':
                            pd.testing.assert_series_equal(actual, expected)
                        else:
                            pd.testing.assert_frame_equal(actual, expected)

    def test_fresh_process_factory_extract_and_official_evaluation_need_no_lotus(self):
        root = self.inputs()
        self.write(root/'ben_piazza.csv', ['Title', 'Role'], [('Target', 'Bob Whitewood')])
        self.write(root/'ben_piazza_text_data.csv', ['title', 'text'], [('Target', 'Michael Ritchie directed it')])
        self.write(root/'lizzy_caplan_text_data.csv', ['title', 'text'], [('Mean Girls', 'a comedy')])
        script = textwrap.dedent('''
            import importlib.abc,json,socket,sys
            class NoLotus(importlib.abc.MetaPathFinder):
                def find_spec(self,fullname,path=None,target=None):
                    blocked=('lotus','semantic_methods.lotus','src.semantic_methods.lotus',
                        'src.baselines.text.sembench_text','src.baselines.text.sembench_prepared',
                        'src.experiments','tests')
                    if any(fullname==name or fullname.startswith(name+'.') for name in blocked):
                        raise AssertionError('forbidden runtime dependency: '+fullname)
            sys.meta_path.insert(0,NoLotus())
            def denied(*args,**kwargs):raise AssertionError('network forbidden')
            socket.socket.connect=denied
            socket.create_connection=denied
            from src.baselines.text.sembench_native_text import task_from_config
            from src.baselines.text.sembench_source import verify_checkout
            verify_checkout(sys.argv[2])
            for query,value in [('Q1','Michael Ritchie'),('Q4','comedy')]:
                task=task_from_config(dict(scenario='mmqa',data_dir=sys.argv[1],query=query,query_id='fresh-'+query))
                evaluation=task.prepare_evaluation()
                source=task.read_source()
                requests=task.requests(source)
                assert len(requests)==task.model_request_count(source)==1
                result=task.finish(source,[dict(call_id=row['call_id'],generated_text=json.dumps({task.field:value}))
                                           for row in requests])
                encoded=task.encode_result(result)
                if query=='Q1':assert encoded['kind']=='series' and encoded['name']=='director'
                assert task.evaluate(source,result,checkout=sys.argv[2],evaluation=evaluation)['sembench_commit'].startswith('c814e380')
                task.verify_source(source)
            print('MMQA full Extract and original scoring; LOTUS imports and network denied')
        ''')
        result = subprocess.run([sys.executable, '-c', script, str(root), self.official_checkout()],
            text=True, capture_output=True, timeout=90, env=dict(os.environ))
        self.assertEqual(result.returncode, 0, result.stdout+result.stderr)
        self.assertIn('LOTUS imports and network denied', result.stdout)


if __name__ == '__main__':
    unittest.main()
