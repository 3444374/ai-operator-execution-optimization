"""Prepared public Ray API contracts without importing Ray or starting models."""

from copy import deepcopy
from contextlib import contextmanager
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks import ray_data_sembench_native as native
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from tests.baselines.text.ray_native_fixtures import recorded_processor


class NativeRayPreparedAPITests(unittest.TestCase):
    @contextmanager
    def processor(self, replies, **kwargs):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            limit = max(1, len(replies))
            ledger = CellBudgetLedger.create(root/'budget.sqlite', AttemptBudget('fixture', limit), deadline_utc=None)
            ledger.reserve_unit('query', limit)
            ledger.claim_shared_unit('query')
            spec = dict(path=str(root/'budget.sqlite'), id='fixture', limit=limit,
                unit_id='query', output=str(root))
            with recorded_processor(replies, **kwargs) as sdk:
                yield sdk, dict(model='unused', engine_kwargs={}, processor_options={},
                    runtime_options={}, engine_runtime_env={'env_vars': {
                        'SEMLOOM_RAY_NATIVE_OBSERVER': json.dumps(spec)}})
            ledger.close_shared_unit('query')

    def test_sampling_and_messages_are_independent_for_every_occurrence(self):
        messages=[dict(role='user',content='same text')]
        sampling=dict(max_tokens=8192,structured_outputs=dict(regex='True|False'))
        expected=deepcopy(sampling)
        first=native._prepared_input(dict(call_id='a',messages=messages),sampling_params=sampling)
        second=native._prepared_input(dict(call_id='b',messages=messages),sampling_params=sampling)
        first['sampling_params']['structured_outputs'].pop('regex')
        first['messages'][0]['content']='native mutation'
        self.assertEqual(second,dict(messages=messages,sampling_params=expected))
        self.assertEqual(sampling,expected)

    def test_raw_answers_and_observed_usage_keep_occurrence_identity(self):
        output=native._prepared_output(dict(call_id='b',generated_text='Answer: False',
            request_id='native-1',num_generated_tokens=5,unused_label='not an output'))
        self.assertEqual(output,dict(call_id='b',generated_text='Answer: False',
            native_usage=dict(request_id='native-1',num_generated_tokens=5)))
        with self.assertRaises(KeyError):native._prepared_output(dict(call_id='b'))

    def test_unknown_or_ignored_stage_options_fail_before_native_allocation(self):
        for options in (dict(runner_threads=1),dict(max_active_batches=3),
                        dict(tokenize_stage=dict(batch_size=8)),
                        dict(chat_template_stage=SimpleNamespace(batch_size=8))):
            with self.subTest(options=options),self.assertRaises(ValueError):
                native.execute_prepared([],model='unused',sampling_params={},engine_kwargs={},
                    processor_options=options,runtime_options={},engine_runtime_env={},on_result_ready=lambda _:None)

    def test_empty_source_runs_sink_without_framework_model_or_core_imports(self):
        program='''
import importlib.abc,sys
sys.path.insert(0,sys.argv[1])
class Guard(importlib.abc.MetaPathFinder):
    def find_spec(self,fullname,*unused):
        if fullname.split('.')[0] in ('ray','daft','vllm','torch','lotus') or fullname.startswith(('src.execution_provider','src.scheduling')):
            raise AssertionError('forbidden dependency: '+fullname)
sys.meta_path.insert(0,Guard())
from src.baselines.text.frameworks.ray_data_sembench_native import execute_prepared,query_scope
ready=[]
result=execute_prepared([],model='unused',sampling_params={},engine_kwargs={},processor_options={},
    runtime_options={},engine_runtime_env={},on_result_ready=lambda value:ready.append(value))
assert result['predictions']==[] and ready==[result]
assert result['events']['predictions_materialized_ns']>0
with query_scope(model='unused',engine_kwargs={},processor_options={},runtime_options={},engine_runtime_env={}) as owner:
    empty=owner.predict([],sampling_params={})
    assert empty['predictions']==[] and not owner.report['events']
assert owner.report['ray_driver_disconnected'] is None
'''
        result=subprocess.run([sys.executable,'-B','-c',program,str(Path(__file__).resolve().parents[3])],
            capture_output=True,text=True,timeout=20)
        self.assertEqual(result.returncode,0,result.stderr)

    def test_dependent_batches_share_runtime_and_close_after_full_sink(self):
        messages = [dict(role='user', content='same input remains distinct')]
        sampling = dict(max_tokens=8192, response_format={'type': 'json_object'})
        with self.processor({'first': 'True', 'next-a': '{"name":"A"}', 'next-b': '{"name":"B"}'}) as (sdk, config):
            with native.query_scope(**config) as owner:
                self.assertEqual(sdk.events, [])
                first = owner.predict([dict(call_id='first', messages=messages)], sampling_params={'max_tokens': 8192})
                self.assertTrue(sdk.initialized)
                empty = owner.predict([], sampling_params=sampling)
                second = owner.predict([dict(call_id=key, messages=messages)
                    for key in ('next-a', 'next-b')], sampling_params=sampling)
                self.assertEqual({row['call_id']: row['generated_text'] for row in second['predictions']},
                    {'next-a': '{"name":"A"}', 'next-b': '{"name":"B"}'})
                sdk.events.append('complete_original_sink')
                self.assertNotIn('close', sdk.events)
            self.assertEqual(sdk.events.count('init'), 1)
            self.assertEqual(sdk.events.count('materialize'), 2)
            self.assertEqual(sdk.events[-2:], ['complete_original_sink', 'close'])
            self.assertEqual(len(sdk.configs), 2)
            self.assertEqual(first['events']['runtime_start_ns'], second['events']['runtime_start_ns'])
            self.assertEqual(len(owner.report['batches']), 3)
            self.assertEqual(empty['predictions'], [])
            self.assertTrue(owner.report['ray_driver_disconnected'])
            self.assertIsNone(owner.report['model_process_exit_confirmed'])
            self.assertEqual(second['requested_config']['sampling_params'], sampling)
            self.assertEqual(second['native_sampling_params'], dict(max_tokens=8192, structured_outputs={'json_object': True}))
            self.assertEqual(sdk.requests[-1]['sampling_params'], second['native_sampling_params'])
            self.assertEqual(sampling, dict(max_tokens=8192, response_format={'type': 'json_object'}))
            for batch in owner.report['batches']:
                self.assertEqual(batch['events']['close_started_ns'], owner.report['events']['close_started_ns'])
            with self.assertRaisesRegex(RuntimeError, 'owner is closed'):
                owner.predict([], sampling_params={})

    def test_existing_runtime_is_rejected_on_scope_entry_without_shutdown(self):
        with self.processor({}) as (sdk, config):
            sdk.initialized = True
            with self.assertRaisesRegex(RuntimeError, 'isolated Ray worker'):
                with native.query_scope(**config):
                    self.fail('borrowed runtime was accepted')
            self.assertTrue(sdk.initialized)
            self.assertEqual(sdk.events, [])
            ready = []
            empty = native.execute_prepared([], **config, sampling_params={}, on_result_ready=ready.append)
            self.assertEqual(ready, [empty])
            self.assertTrue(sdk.initialized)
            self.assertEqual(sdk.events, [])

    def test_later_batch_identity_failure_preserves_first_and_closes_once(self):
        def corrupt_later(rows):
            return rows if rows[0]['call_id'] == 'first' else [rows[0], rows[0]]
        with self.processor({'first': 'True', 'later': 'False'}, transform=corrupt_later) as (sdk, config):
            with self.assertRaisesRegex(ValueError, 'missing, duplicate') as caught:
                with native.query_scope(**config) as owner:
                    owner.predict([dict(call_id='first', messages=[])], sampling_params={})
                    owner.predict([dict(call_id='later', messages=[])], sampling_params={})
            report = caught.exception.native_query_report
            self.assertEqual(len(report['batches']), 2)
            self.assertEqual(report['batches'][0]['predictions'][0]['generated_text'], 'True')
            self.assertEqual(len(report['batches'][1]['predictions']), 2)
            self.assertTrue(report['ray_driver_disconnected'])
            self.assertEqual(sdk.events.count('close'), 1)

    def test_query_error_remains_primary_if_final_shutdown_fails(self):
        with self.processor({'first': 'True'}, close_error=RuntimeError('shutdown failed')) as (sdk, config):
            with self.assertRaisesRegex(ValueError, 'original parser failed') as caught:
                with native.query_scope(**config) as owner:
                    owner.predict([dict(call_id='first', messages=[])], sampling_params={})
                    raise ValueError('original parser failed')
            report = caught.exception.native_query_report
            self.assertEqual(report['close_error_type'], 'RuntimeError')
            self.assertEqual(report['batches'][0]['close_error_type'], 'RuntimeError')
            self.assertEqual(report['batches'][0]['predictions'][0]['generated_text'], 'True')
            self.assertIn('native Ray close failed: RuntimeError', caught.exception.__notes__)
            self.assertEqual(sdk.events.count('close'), 1)

    def test_partial_startup_failure_still_closes_the_owned_runtime(self):
        with self.processor({'first': 'True'}) as (sdk, config):
            def fail_after_start(**kwargs):
                sdk.initialized = True
                sdk.events.append('partial_init')
                raise RuntimeError('native startup failed')
            with patch.object(sys.modules['ray'], 'init', side_effect=fail_after_start), \
                    self.assertRaisesRegex(RuntimeError, 'native startup failed') as caught:
                with native.query_scope(**config) as owner:
                    owner.predict([dict(call_id='first', messages=[])], sampling_params={})
            self.assertEqual(sdk.events, ['partial_init', 'close'])
            self.assertTrue(caught.exception.native_query_report['ray_driver_disconnected'])
            self.assertEqual(caught.exception.native_query_report['batches'][0]['predictions'], [])
            self.assertEqual(caught.exception.native_query_report['batches'][0]['events']['runtime_start_ns'],
                caught.exception.native_query_report['events']['runtime_start_ns'])

    def test_conflicting_or_unimplemented_response_formats_fail_before_start(self):
        with self.processor({'first': 'True'}) as (sdk, config):
            for sampling in ({'response_format': {'type': 'json_schema'}},
                             {'response_format': {'type': 'json_object'}, 'structured_outputs': {'regex': '.*'}}):
                with self.subTest(sampling=sampling), self.assertRaisesRegex(ValueError, 'unambiguous JSON object'):
                    with native.query_scope(**config) as owner:
                        owner.predict([dict(call_id='first', messages=[])], sampling_params=sampling)
                self.assertEqual(sdk.events, [])


if __name__=='__main__':unittest.main()
