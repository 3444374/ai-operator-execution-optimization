"""HTTP processor contracts using CPU doubles; no Ray, server or model starts."""

import builtins
from copy import deepcopy
import os
from pathlib import Path
import pickle
import socket
import subprocess
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from src.baselines.text.frameworks import ray_data_http_sembench_native as native
from src.baselines.text.sembench_movie_native import q3_messages


class CPUDataSet:
    """Only the public Dataset methods consumed by this binding."""

    def __init__(self, read, events):
        self.read, self.events = read, events

    def filter(self, predicate):
        self.events.append(("filter", predicate.__name__))
        return CPUDataSet(lambda: [row for row in self.read() if predicate(row)], self.events)

    def materialize(self):
        self.events.append(("materialize",))
        rows = self.read()
        return CPUDataSet(lambda: rows, self.events)

    def take_all(self):
        self.events.append(("take_all",))
        return list(self.read())

    def count(self):
        self.events.append(("count",))
        return len(self.read())



class NativeRayHTTPQ3Tests(unittest.TestCase):
    def setUp(self):
        self.events, self.requests, self.config_calls = [], [], []
        self.initialized = False
        self.run_error = self.build_error = self.cleanup_error = None
        self.response_transform = lambda response: response
        self.output_transform = lambda rows: rows
        self.mutate_payload = False
        self.ray = ModuleType('ray')
        self.ray.__path__ = []
        self.ray.__version__ = '2.56.1-CPU-double'
        self.ray.is_initialized = lambda: self.initialized
        self.ray.init = Mock(side_effect=AssertionError('explicit Ray start forbidden'))
        self.ray.shutdown = Mock(side_effect=self.shutdown)
        self.data = ModuleType('ray.data')
        self.data.__path__ = []
        self.data.from_items = Mock(side_effect=self.from_items)
        self.context = SimpleNamespace(actor_init_retry_on_errors=False, actor_init_max_retries=3,
            actor_task_retry_on_errors=False, retried_map_errors=False, max_map_retries=3,
            max_tasks_in_flight_per_actor=None)
        self.data.DataContext = SimpleNamespace(get_current=lambda: self.context)
        self.ray.data = self.data
        self.llm = ModuleType('ray.data.llm')
        self.llm.HttpRequestProcessorConfig = self.config
        self.llm.build_processor = Mock(side_effect=self.build)
        self.data.llm = self.llm
        modules = {'ray': self.ray, 'ray.data': self.data, 'ray.data.llm': self.llm}
        original_import = builtins.__import__

        def guarded_import(name, *args, **kwargs):
            if name not in modules and any(name == root or name.startswith(root + '.') for root in (
                'ray', 'daft', 'vllm', 'torch', 'lotus', 'src.semantic_methods', 'src.serving',
                'src.execution_provider', 'src.scheduling', 'src.research', 'integrations',
            )):
                self.fail('forbidden real framework/model/Core/owner import: ' + name)
            return original_import(name, *args, **kwargs)

        for scope in (
            patch.dict(sys.modules, modules), patch.object(builtins, '__import__', guarded_import),
            patch.object(socket.socket, 'connect', side_effect=AssertionError('network forbidden')),
            patch.object(socket.socket, 'connect_ex', side_effect=AssertionError('network forbidden')),
            patch.object(subprocess, 'Popen', side_effect=AssertionError('process launch forbidden')),
        ):
            scope.start()
            self.addCleanup(scope.stop)
        self.rows = [
            dict(occurrence_id='outside', id='another_movie', reviewId='4', reviewText='excluded'),
            dict(occurrence_id='a', id='taken_3', reviewId='7', reviewText='same review\n中文'),
            dict(occurrence_id='b', id='taken_3', reviewId='7', reviewText='same review\n中文'),
            dict(occurrence_id='c', id='taken_3', reviewId='8', reviewText='bad review'),
        ]
        self.replies = dict(a='Answer: True', b='invalid native reply', c='Answer: False')
        self.sampling = dict(temperature=0, max_tokens=61, stop=['fixture-stop'],
                             repetition_penalty=1.05, n=1)

    def shutdown(self):
        if self.cleanup_error is not None:
            raise self.cleanup_error
        self.initialized = False

    def from_items(self, rows):
        self.initialized = True
        self.events.append(('from_items', [row['occurrence_id'] for row in rows]))
        return CPUDataSet(lambda: rows, self.events)

    def config(self, **kwargs):
        self.config_calls.append(deepcopy(kwargs))
        values = dict(batch_size=64, concurrency=1, qps=None, max_retries=0,
                      base_retry_wait_time_in_s=1, http_request_stage=True)
        values.update(kwargs)
        return SimpleNamespace(values=values, model_dump=lambda **unused: deepcopy(values))

    def build(self, config, *, preprocess, postprocess):
        if self.build_error is not None:
            raise self.build_error
        owner = self

        class CPUHTTPProcessor:
            def list_stage_names(self):
                return ['HttpRequestStage']

            def __call__(self, selected):
                owner.events.append(('processor',))

                def read():
                    if owner.run_error is not None:
                        raise owner.run_error
                    output = []
                    for row in selected.read():
                        request = dict(row, **preprocess(row))
                        owner.requests.append(deepcopy(request))
                        if owner.mutate_payload:
                            request['payload']['structured_outputs'].pop('regex')
                        response = dict(id='cpu-' + row['occurrence_id'], model='CPU-model',
                            choices=[dict(message=dict(content=owner.replies[row['occurrence_id']]))],
                            usage=dict(prompt_tokens=17, completion_tokens=2))
                        output.append(postprocess(dict(request,
                            http_response=owner.response_transform(response))))
                    return owner.output_transform(output)

                return CPUDataSet(read, owner.events)

        return CPUHTTPProcessor()

    def execute(self, rows=None, **kwargs):
        values = dict(url='http://127.0.0.1:9000/v1/chat/completions', model='CPU-model',
                      sampling_params=self.sampling)
        values.update(kwargs)
        return native.execute_q3(self.rows if rows is None else rows, **values)

    def test_full_source_filter_and_count_use_one_materialized_http_graph(self):
        result = self.execute()
        self.assertEqual(self.events, [
            ('from_items', ['outside', 'a', 'b', 'c']), ('filter', '_eligible'),
            ('processor',), ('materialize',), ('take_all',), ('filter', '_positive'), ('count',),
        ])
        self.assertEqual(len(self.requests), 3)
        self.assertEqual(result['positive_review_cnt'], 2)
        self.assertEqual([row['reviewId'] for row in result['predictions']], ['7', '7', '8'])
        self.assertTrue(result['predictions'][1]['decision'])
        self.assertEqual(result['predictions'][0]['native_usage']['usage']['prompt_tokens'], 17)
        self.assertNotIn('reviewText', result['predictions'][0])
        self.ray.init.assert_not_called()
        self.ray.shutdown.assert_called_once_with()

    def test_constructor_preserves_http_defaults_and_ray_retries(self):
        before = deepcopy(vars(self.context))
        result = self.execute()
        self.assertEqual(self.config_calls, [dict(url='http://127.0.0.1:9000/v1/chat/completions')])
        metadata = result['metadata']
        self.assertEqual(metadata['profile'], 'ray-data-http-external-vllm')
        config = metadata['processor_config']
        self.assertEqual((config['batch_size'], config['concurrency'], config['qps'], config['max_retries']),
                         (64, 1, None, 0))
        self.assertEqual(metadata['stages'], ['HttpRequestStage'])
        self.assertEqual(vars(self.context), before)
        self.assertEqual(metadata['retry_policy']['adapter_overrides'], {})
        self.assertIsNone(metadata['retry_policy']['physical_attempts'])
        self.assertEqual(metadata['native_capacity']['status'], 'unobserved')
        self.assertFalse(metadata['lifecycle']['external_service_lifecycle_managed'])
        self.assertIsNone(metadata['lifecycle']['model_process_exit_confirmed'])

    def test_original_messages_and_generation_are_forwarded_without_mutating_caller(self):
        self.sampling['structured_outputs'] = dict(regex='True|False')
        original = deepcopy(self.sampling)
        self.mutate_payload = True
        self.execute()
        for row, request in zip(self.rows[1:], self.requests, strict=True):
            self.assertEqual(request['payload'], dict(original, model='CPU-model', stream=False, n=1,
                                                      messages=q3_messages(row['reviewText'])))
        self.assertEqual(self.sampling, original)

    def test_n_is_explicit_one_even_when_native_sampling_omits_it(self):
        del self.sampling['n']
        self.execute()
        self.assertTrue(all(request['payload']['n'] == 1 for request in self.requests))
        self.assertNotIn('n', self.sampling)

    def test_reserved_sampling_and_multiple_completions_fail_before_runtime_allocation(self):
        invalid = [dict(model='override'), dict(messages=[]), dict(stream=False),
                   dict(n=2), dict(n=True), dict(temperature=float('nan'))]
        for sampling in invalid:
            with self.subTest(sampling=sampling), self.assertRaises(ValueError):
                self.execute(sampling_params=sampling)
        self.data.from_items.assert_not_called()

    def test_nonlocal_credential_and_malformed_urls_fail_before_runtime_allocation(self):
        invalid = ['https://localhost/v1/chat/completions', 'http://example.com/v1/chat/completions',
            'http://user:password@localhost/v1/chat/completions',
            'http://localhost/v1/chat/completions?token=placeholder',
            'http://localhost:invalid/v1/chat/completions', 'http://localhost/v1/chat/completions#fragment']
        for url in invalid:
            with self.subTest(url=url), self.assertRaises(ValueError):
                self.execute(url=url)
        self.data.from_items.assert_not_called()

    def test_duplicate_source_occurrences_fail_before_runtime_allocation(self):
        with self.assertRaisesRegex(ValueError, 'unique eligible'):
            self.execute([self.rows[1], dict(self.rows[1], reviewId='different')])
        self.data.from_items.assert_not_called()

    def test_error_json_multiple_choices_and_nontext_content_never_reach_parser_fallback(self):
        bad = [None, [], {}, dict(error=dict(type='fixture', message='HTTP fixture failure')),
            dict(choices=[]), dict(choices=[dict(message=dict(content='Answer: True'))] * 2),
            dict(choices=[dict(message=dict(content=None))]),
            dict(choices=[dict(message=dict(content='Answer: True'))], usage=dict(tokens=float('inf')))]
        for response in bad:
            with self.subTest(response=response):
                self.response_transform = lambda original, value=response: value
                self.requests.clear()
                with self.assertRaises(ValueError) as caught:
                    self.execute()
                self.assertEqual(len(self.requests), 1)
                self.assertIsNone(caught.exception.native_q3_report['positive_review_cnt'])
                self.assertTrue(caught.exception.native_q3_report['metadata']['lifecycle']['ray_driver_disconnected'])
                if response is not None:
                    self.assertEqual(caught.exception.http_response, response)

    def test_error_payload_survives_exception_serialization(self):
        response = dict(error=dict(type='fixture', message='retained private JSON'))
        with self.assertRaises(ValueError) as caught:
            native._postprocess(dict(http_response=response))
        restored = pickle.loads(pickle.dumps(caught.exception))
        self.assertEqual(restored.http_response, response)

    def test_empty_string_is_valid_text_and_uses_original_default_true_policy(self):
        self.replies['a'] = ''
        result = self.execute()
        self.assertEqual(result['positive_review_cnt'], 2)
        self.assertEqual(result['predictions'][0]['generated_text'], '')

    def test_output_order_may_change_without_losing_occurrence_association(self):
        self.output_transform = lambda rows: list(reversed(rows))
        result = self.execute()
        self.assertEqual([row['occurrence_id'] for row in result['predictions']], ['c', 'b', 'a'])
        self.assertEqual(result['positive_review_cnt'], 2)

    def test_missing_duplicate_or_changed_predictions_fail_before_count(self):
        for transform in (lambda rows: rows[:-1], lambda rows: [rows[0]] * len(rows),
            lambda rows: [dict(rows[0], occurrence_id='unknown'), *rows[1:]],
            lambda rows: [dict(rows[0], reviewId='changed'), *rows[1:]]):
            with self.subTest(transform=transform):
                self.events.clear()
                self.output_transform = transform
                with self.assertRaisesRegex(ValueError, 'missing, duplicate or unknown') as caught:
                    self.execute()
                self.assertNotIn(('count',), self.events)
                self.assertTrue(caught.exception.native_q3_report['predictions'])

    def test_empty_source_and_empty_selection_do_not_build_http_processor(self):
        empty = self.execute([])
        self.assertEqual(empty['positive_review_cnt'], 0)
        self.assertEqual(empty['metadata']['lifecycle']['runtime_owner'], 'none')
        self.data.from_items.assert_not_called()
        selected = self.execute(self.rows[:1])
        self.assertEqual(selected['positive_review_cnt'], 0)
        self.llm.build_processor.assert_not_called()
        self.assertEqual(self.requests, [])

    def test_caller_ray_runtime_is_retained_without_service_reuse_or_exit_claim(self):
        self.initialized = True
        result = self.execute()
        self.ray.shutdown.assert_not_called()
        self.assertEqual(result['metadata']['lifecycle']['runtime_owner'], 'caller')
        self.assertIsNone(result['metadata']['lifecycle']['ray_driver_disconnected'])
        self.assertIsNone(result['metadata']['lifecycle']['model_process_exit_confirmed'])

    def test_build_and_transport_errors_preserve_original_and_do_not_retry_in_adapter(self):
        for name in ('build_error', 'run_error'):
            with self.subTest(name=name):
                failure = TimeoutError('CPU native ' + name)
                setattr(self, name, failure)
                with self.assertRaises(TimeoutError) as caught:
                    self.execute()
                self.assertIs(caught.exception, failure)
                self.assertTrue(failure.native_q3_report['metadata']['lifecycle']['ray_driver_disconnected'])
                self.assertIsNone(failure.native_q3_report['positive_review_cnt'])
                setattr(self, name, None)

    def test_cleanup_errors_preserve_primary_or_returned_predictions(self):
        self.cleanup_error = RuntimeError('CPU Ray shutdown fixture failed')
        self.run_error = ValueError('CPU HTTP primary failure')
        with self.assertRaises(ValueError) as caught:
            self.execute()
        self.assertIs(caught.exception, self.run_error)
        self.assertEqual(caught.exception.__notes__, ['native Ray runtime cleanup failed: RuntimeError'])
        self.run_error = None
        self.initialized = False  # Independent owned runtime for the successful-query case.
        with self.assertRaises(RuntimeError) as caught:
            self.execute()
        self.assertIs(caught.exception, self.cleanup_error)
        self.assertEqual(len(caught.exception.native_q3_report['predictions']), 3)
        self.assertEqual(caught.exception.native_q3_report['positive_review_cnt'], 2)

    def test_full_2000_row_source_has_120_logical_requests_and_count_14(self):
        rows = [dict(occurrence_id=f'cohort-{i}', id='taken_3' if i < 120 else 'outside',
            reviewId=str(i), reviewText=f'CPU cohort review {i}') for i in range(2000)]
        self.replies = {row['occurrence_id']: 'Answer: True' if i < 14 else 'Answer: False'
                        for i, row in enumerate(rows[:120])}
        result = self.execute(rows)
        self.assertEqual(result['metadata']['input_rows'], 2000)
        self.assertEqual(result['metadata']['logical_inference_rows'], 120)
        self.assertEqual(len(self.requests), 120)
        self.assertEqual(result['positive_review_cnt'], 14)
        self.assertEqual(self.events.count(('materialize',)), 1)
        self.assertIsNone(result['metadata']['retry_policy']['physical_attempts'])


class HTTPImportBoundaryTests(unittest.TestCase):
    def test_module_and_empty_source_import_without_frameworks_or_service_owner(self):
        code = Path(__file__).resolve().parents[3]
        program = """
import importlib.abc, socket, sys
sys.path.insert(0, sys.argv[1])
class Guard(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if any(fullname == p or fullname.startswith(p + '.') for p in
               ('ray', 'daft', 'vllm', 'torch', 'lotus', 'integrations', 'src.semantic_methods',
                'src.serving', 'src.execution_provider', 'src.scheduling', 'src.research')):
            raise AssertionError('forbidden dependency: ' + fullname)
sys.meta_path.insert(0, Guard())
def no_network(*args, **kwargs):
    raise AssertionError('network forbidden')
socket.socket.connect = socket.socket.connect_ex = no_network
from src.baselines.text.frameworks.ray_data_http_sembench_native import execute_q3
result = execute_q3([], url='http://localhost:9000/v1/chat/completions', model='CPU-model', sampling_params={})
assert result['predictions'] == [] and result['positive_review_cnt'] == 0
"""
        result = subprocess.run([sys.executable, '-B', '-c', program, str(code)],
            capture_output=True, text=True, timeout=20, env=dict(os.environ, CUDA_VISIBLE_DEVICES=''))
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == '__main__':
    unittest.main()
