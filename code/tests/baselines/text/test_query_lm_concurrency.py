"""Controlled grouped-query interleavings; no LOTUS/Ray SDK or model execution."""

from concurrent.futures import ThreadPoolExecutor, TimeoutError
from copy import deepcopy
from threading import Barrier, Event
from types import ModuleType, SimpleNamespace
import sys
import unittest
from unittest.mock import patch

from src.baselines.text import sembench_prepared as semantic
from src.baselines.text.frameworks import ray_data_sembench_native as native
from src.serving.backends.vllm_protocol import GenerationSpec
from tests.baselines.text.native_sdk_fixtures import module_entries
from tests.baselines.text import test_ray_data_sembench_native as fixture


class QueryLMConcurrencyTests(unittest.TestCase):
    def model(self, predict):
        models, types = ModuleType('lotus.models'), ModuleType('lotus.types')
        class LM:
            def __init__(self, model, **kwargs):
                self.model, self.kwargs = model, kwargs
        models.LM = LM
        types.LMOutput = SimpleNamespace
        with module_entries({'lotus.models': models, 'lotus.types': types}):
            query = SimpleNamespace(query_id='grouped', model_name='CPU-no-model',
                                    generation=GenerationSpec(max_tokens=128, stop=()))
            return semantic.make_query_lm(query, predict)

    def test_interleaved_groups_have_unique_ids_and_preserve_parallel_native_parameters(self):
        copying, executing = Barrier(2), Barrier(2)
        calls = []
        def copy(value):
            if isinstance(value, list) and value and isinstance(value[0], dict) and 'role' in value[0]:
                copying.wait(5)
            return deepcopy(value)
        def predict(requests, *, sampling_params):
            calls.append((deepcopy(requests), deepcopy(sampling_params)))
            executing.wait(5)
            return dict(predictions=[dict(call_id=row['call_id'], generated_text=row['messages'][0]['content'])
                                     for row in reversed(requests)])
        lm, batches = self.model(predict)
        with patch.object(semantic, 'deepcopy', side_effect=copy), ThreadPoolExecutor(2) as threads:
            left = threads.submit(lm, [[dict(role='user', content='left')]], logprobs=False)
            right = threads.submit(lm, [[dict(role='user', content='right')]],
                                   response_format={'type': 'json_object'})
            self.assertEqual(left.result(8).outputs, ['left'])
            self.assertEqual(right.result(8).outputs, ['right'])
        ids = [request['call_id'] for batch in batches for request in batch['requests']]
        self.assertEqual(len(set(ids)), 2)
        for index, batch in enumerate(batches):
            self.assertEqual(batch['requests'][0]['call_id'], f'grouped-batch-{index}-row-0')
            self.assertEqual(batch['status'], 'completed')
        by_content = {requests[0]['messages'][0]['content']: sampling for requests, sampling in calls}
        self.assertIsNone(by_content['left']['logprobs'])
        self.assertEqual(by_content['right']['response_format'], {'type': 'json_object'})
        self.assertTrue(all(options['max_tokens'] == 128 and options['stop'] == [] for options in by_content.values()))
        self.assertTrue(any(batch['operator_kwargs'].get('logprobs') is False for batch in batches))

    def test_one_failed_group_keeps_its_native_report_and_does_not_fail_its_sibling(self):
        executing = Barrier(2)
        failure = ValueError('CPU group failed')
        failed_native = {'predictions': [], 'fixture': 'failed group'}
        failure.native_query_report = failed_native
        def predict(requests, *, sampling_params):
            executing.wait(5)
            if requests[0]['messages'][0]['content'] == 'bad':
                raise failure
            return dict(predictions=[dict(call_id=row['call_id'], generated_text='good') for row in requests])
        lm, batches = self.model(predict)
        with ThreadPoolExecutor(2) as threads:
            good = threads.submit(lm, [[dict(role='user', content='good')]])
            bad = threads.submit(lm, [[dict(role='user', content='bad')]])
            self.assertEqual(good.result(8).outputs, ['good'])
            with self.assertRaises(ValueError) as caught:
                bad.result(8)
        self.assertIs(caught.exception, failure)
        by_content = {batch['requests'][0]['messages'][0]['content']: batch for batch in batches}
        self.assertEqual(by_content['good']['status'], 'completed')
        self.assertEqual(by_content['bad']['status'], 'failed')
        self.assertIs(by_content['bad']['native'], failed_native)


class RayQueryConcurrencyTests(unittest.TestCase):
    def interleave_start(self, owner, sdk, *, error=None):
        started, release, second = Event(), Event(), Event()
        original_start = owner._start
        def init(**kwargs):
            sdk.events.append('init')
            sdk.initialized = True
            started.set()
            if not release.wait(5):
                raise AssertionError('fixture initialization was not released')
            if error is not None:
                raise error
        def observe(events):
            if started.is_set():
                second.set()
            return original_start(events)
        self.addCleanup(release.set)
        return started, release, second, init, observe

    def run_interleaved_start(self, *, startup_error=None):
        with fixture.NativeRayPreparedAPITests().processor({'a': 'A', 'b': 'B'}) as (sdk, config):
            with native.query_scope(**config) as owner:
                started, release, second, init, observe = self.interleave_start(owner, sdk, error=startup_error)
                with patch.object(sys.modules['ray'], 'init', side_effect=init), \
                        patch.object(owner, '_start', side_effect=observe), ThreadPoolExecutor(2) as threads:
                    first = threads.submit(owner.predict, [dict(call_id='a', messages=[])], sampling_params={})
                    self.assertTrue(started.wait(5))
                    waiter = threads.submit(owner.predict, [dict(call_id='b', messages=[])], sampling_params={})
                    self.assertTrue(second.wait(5))
                    try:
                        try:
                            waiter.result(.1)
                        except TimeoutError:
                            blocked = True
                        except BaseException:
                            blocked = False
                        else:
                            blocked = False
                    finally:
                        release.set()
                    outcomes = []
                    for future in (first, waiter):
                        try:
                            outcomes.append(future.result(8))
                        except BaseException as error:
                            outcomes.append(error)
                self.assertTrue(blocked, 'a caller escaped while native runtime initialization was incomplete')
            return sdk, owner.report, outcomes

    def test_parallel_first_batches_wait_for_one_runtime_initialization(self):
        sdk, report, outcomes = self.run_interleaved_start()
        self.assertTrue(all(isinstance(value, dict) for value in outcomes), outcomes)
        self.assertEqual([value['predictions'][0]['generated_text'] for value in outcomes], ['A', 'B'])
        self.assertEqual(sdk.events.count('init'), 1)
        self.assertEqual(sdk.events.count('close'), 1)
        self.assertTrue(report['ray_driver_disconnected'])
        self.assertEqual({batch['events']['runtime_start_ns'] for batch in report['batches']},
                         {report['events']['runtime_start_ns']})

    def test_failed_initialization_is_not_retried_or_misreported_by_a_waiter(self):
        failure = RuntimeError('CPU native init failed')
        sdk, report, outcomes = self.run_interleaved_start(startup_error=failure)
        self.assertTrue(all(value is failure for value in outcomes), outcomes)
        self.assertEqual(sdk.events, ['init', 'close'])
        self.assertEqual([batch['input_call_ids'] for batch in report['batches']], [['a'], ['b']])
        self.assertTrue(report['ray_driver_disconnected'])

    def test_native_batches_materialize_concurrently_and_failed_report_keeps_its_input_ids(self):
        executing = Barrier(2)
        raw_output = native._prepared_output
        failure = ValueError('CPU native b failed')
        def output(row):
            executing.wait(5)
            if row['call_id'] == 'b':
                raise failure
            return raw_output(row)
        with fixture.NativeRayPreparedAPITests().processor({'a': 'A', 'b': 'B'}) as (sdk, config):
            with self.assertRaises(ValueError) as caught:
                with native.query_scope(**config) as owner, patch.object(native, '_prepared_output', side_effect=output):
                    with ThreadPoolExecutor(2) as threads:
                        good = threads.submit(owner.predict, [dict(call_id='a', messages=[])], sampling_params={})
                        bad = threads.submit(owner.predict, [dict(call_id='b', messages=[])], sampling_params={})
                        self.assertEqual(good.result(8)['predictions'][0]['generated_text'], 'A')
                        bad.result(8)
            self.assertIs(caught.exception, failure)
            by_ids = {tuple(batch['input_call_ids']): batch for batch in caught.exception.native_query_report['batches']}
            self.assertEqual(by_ids[('a',)]['predictions'][0]['generated_text'], 'A')
            self.assertEqual(by_ids[('b',)]['predictions'], [])
            self.assertEqual(sdk.events.count('init'), 1)
            self.assertEqual(sdk.events.count('close'), 1)
            self.assertTrue(caught.exception.native_query_report['ray_driver_disconnected'])


if __name__ == '__main__':
    unittest.main()
