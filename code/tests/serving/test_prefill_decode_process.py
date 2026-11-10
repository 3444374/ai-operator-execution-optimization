"""CPU behavior checks for the native two-process text handoff."""
from dataclasses import replace
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, patch

from src.serving.backends import prefill_decode_process as stages
from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec, _prepared
from tests.serving.test_sync_native_rendering import kernel, output, rows
from tests.serving.test_sync_model_context import native_api


TRANSFER = dict(do_remote_decode=False, do_remote_prefill=True,
                remote_engine_id='CPU-P', remote_request_id='CPU-request',
                remote_block_ids=[1], remote_host='127.0.0.1', remote_port=5600)


class IPCFixtureEngine:
    def __init__(self, engine, generation):
        self.spec = engine

    def ready(self):
        return dict(engine_uid='CPU-pipe', device=os.environ['CUDA_VISIBLE_DEVICES'])

    def run_prepared(self, records, *, generation, deadline_monotonic, kv_transfer_params):
        return [dict(_prepared(records)[0][0], status='succeeded',
                     kv_transfer_params=kv_transfer_params[0], max_tokens=generation.max_tokens)]

    def close(self, timeout=10):
        return dict(status='closed', engine_api_shutdown_confirmed=True)


class CPUStageProcess(stages.ChainEngineProcess):
    @staticmethod
    def _command(engine, fd):
        return [sys.executable, '-B', '-m', __name__, '--cpu-serve', str(fd)]


class FixtureProcess:
    def __init__(self, engine, generation, control, deadline, startup_timeout, **options):
        self.generation, self.options = generation, options
        self.control = Path(control)
        self.ready_report = dict(engine_uid=options['worker_log'].stem)
        self.calls = []
        self.close = Mock(return_value=dict(status='closed', process_exit_confirmed=True))
        self.after_result = lambda: None

    def run_prepared(self, records, *, generation, kv_transfer_params):
        self.calls.append((records, generation, kv_transfer_params))
        value = dict(_prepared(records)[0][0], engine_uid=self.ready_report['engine_uid'],
            engine_request_id='CPU-call', status='succeeded', text='full response',
            prompt_token_ids_json='[1, 2]', output_token_ids_json='[3]', logprobs_json='null',
            prompt_tokens=2, completion_tokens=1, total_tokens=3,
            finish_reason='stop', submission_confirmed=True, kv_transfer_params=dict(TRANSFER))
        self.after_result()
        return [value]


class HandoffTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.patch = patch.object(stages, 'ChainEngineProcess', FixtureProcess)
        self.patch.start()
        self.addCleanup(self.patch.stop)
        self.spec = EngineSpec('/tmp/CPU-model', sys.executable, sys.executable)
        self.generation = GenerationSpec(max_tokens=11, stop=('END',), seed=9)

    def owner(self):
        owner = stages.PrefillDecodeProcess(self.spec, self.generation, self.temp.name,
            None, 10, prefill=stages.NixlPlacement('0', 5600),
            decode=stages.NixlPlacement('1', 5601))
        self.addCleanup(owner.close)
        return owner

    def test_original_request_and_sampling_reach_decode_with_two_physical_receipts(self):
        owner = self.owner()
        records = rows(2)
        results = owner.run_prepared(records)
        self.assertEqual([r['status'] for r in results], ['succeeded'] * 2)
        for i, row in enumerate(records):
            self.assertEqual(owner.prefill.calls[i][0], [row])
            self.assertEqual(owner.prefill.calls[i][1], replace(self.generation, max_tokens=1))
            self.assertEqual(owner.decode.calls[i], ([row], self.generation, [TRANSFER]))
            self.assertEqual(set(results[i]['stage_receipts']), {'prefill', 'decode'})
            self.assertEqual(results[i]['total_tokens'], 3)
        self.assertEqual(owner.prefill.options['worker_env']['CUDA_VISIBLE_DEVICES'], '0')
        self.assertEqual(owner.decode.options['worker_env']['CUDA_VISIBLE_DEVICES'], '1')

    def test_missing_transfer_is_not_silently_recomputed_on_decode(self):
        owner = self.owner()
        original = owner.prefill.run_prepared
        def missing(*args, **kwargs):
            result = original(*args, **kwargs)
            result[0]['kv_transfer_params'] = None
            return result
        owner.prefill.run_prepared = missing
        result = owner.run_prepared(rows(2))
        self.assertEqual(result[0]['status'], 'unknown')
        self.assertEqual(result[1]['status'], 'failed')
        self.assertEqual(owner.decode.calls, [])
        self.assertEqual(len(owner.prefill.calls), 1)

    def test_stop_after_prefill_retains_usage_without_starting_decode(self):
        owner = self.owner()
        owner.prefill.after_result = owner.cancel
        result = owner.run_prepared(rows())[0]
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['stage_receipts']['prefill'][0]['total_tokens'], 3)
        self.assertEqual(owner.decode.calls, [])

    def test_wrong_identity_cannot_cross_stage_handoff(self):
        owner = self.owner()
        original = owner.prefill.run_prepared
        def wrong(*args, **kwargs):
            result = original(*args, **kwargs)
            result[0]['source_position'] = 100
            return result
        owner.prefill.run_prepared = wrong
        result = owner.run_prepared(rows())[0]
        self.assertEqual(result['status'], 'unknown')
        self.assertEqual(owner.decode.calls, [])
        self.assertEqual(result['stage_receipts']['prefill'][0]['source_position'], 100)

    def test_prompt_mismatch_preserves_both_receipts_and_rejects_output(self):
        owner = self.owner()
        original = owner.decode.run_prepared
        def wrong(*args, **kwargs):
            result = original(*args, **kwargs)
            result[0].update(prompt_token_ids_json='[7, 8]')
            return result
        owner.decode.run_prepared = wrong
        result = owner.run_prepared(rows())[0]
        self.assertEqual(result['status'], 'unknown')
        self.assertIsNone(result['text'])
        self.assertEqual(len(result['stage_receipts']), 2)

    def test_second_start_failure_closes_first_process(self):
        first = FixtureProcess(self.spec, self.generation, self.temp.name, None, 10,
                               worker_log=Path('prefill.log'))
        with patch.object(stages, 'ChainEngineProcess', side_effect=[first, RuntimeError('start failed')]):
            with self.assertRaisesRegex(RuntimeError, 'start failed'):
                self.owner()
        first.close.assert_called_once()

    def test_close_reports_each_stage_and_never_claims_gpu_exit(self):
        owner = self.owner()
        owner.decode.close.return_value = dict(status='unknown', process_exit_confirmed=False)
        result = owner.close(timeout=2)
        self.assertEqual(result['status'], 'unknown')
        self.assertIsNone(result['gpu_exit_confirmed'])
        owner.prefill.close.assert_called_once()
        owner.decode.close.assert_called_once()

    def test_native_sampling_and_output_forward_opaque_transfer(self):
        with kernel([[1, 2]]) as native:
            native.spec = replace(native.spec, kv_transfer_config=dict(kv_connector='NixlConnector'))
            response = output(0, [1, 2])
            response.kv_transfer_params = dict(TRANSFER)
            native._llm.wait_for_completion.return_value = [response]
            result = native.run_prepared(rows(), kv_transfer_params=[dict(TRANSFER)])[0]
            params = native._llm._render_and_add_requests.call_args.kwargs['params']
            self.assertEqual(params[0]['extra_args']['kv_transfer_params'], TRANSFER)
            self.assertEqual(result['kv_transfer_params'], TRANSFER)

    def test_transfer_cardinality_fails_before_submission(self):
        with kernel([[1, 2]]) as native:
            native.spec = replace(native.spec, kv_transfer_config=dict(kv_connector='NixlConnector'))
            with self.assertRaises(ValueError):
                native.run_prepared(rows(), kv_transfer_params=[])
            native._llm._render_and_add_requests.assert_not_called()

    def test_native_connector_config_is_materialized_in_model_constructor(self):
        module = ModuleType('vllm.config')
        module.KVTransferConfig = lambda **kw: SimpleNamespace(**kw)
        configured = replace(self.spec, enforce_eager=True,
                             kv_transfer_config=dict(kv_connector='NixlConnector', kv_role='kv_both'))
        from src.serving.backends.vllm_engine import VllmEngine
        with native_api() as (calls, _), patch.dict(sys.modules, {'vllm.config': module}):
            owner = VllmEngine(configured, self.generation)
            try:
                self.assertEqual(calls[0]['kv_transfer_config'].kv_connector, 'NixlConnector')
                self.assertTrue(calls[0]['enforce_eager'])
            finally:
                owner.close()

    def test_real_stage_pipe_preserves_transfer_and_child_only_environment(self):
        before = os.environ.get('CUDA_VISIBLE_DEVICES')
        owner = CPUStageProcess(self.spec, self.generation, self.temp.name, None, 5,
            worker_env={'CUDA_VISIBLE_DEVICES': 'CPU-test-device'},
            worker_log=Path(self.temp.name) / 'cpu.log')
        try:
            self.assertEqual(owner.ready_report['device'], 'CPU-test-device')
            result = owner.run_prepared(rows(), generation=self.generation,
                                       kv_transfer_params=[dict(TRANSFER)])[0]
            self.assertEqual(result['kv_transfer_params'], TRANSFER)
            self.assertEqual(result['max_tokens'], 11)
            self.assertEqual(os.environ.get('CUDA_VISIBLE_DEVICES'), before)
        finally:
            self.assertEqual(owner.close()['status'], 'closed')


if __name__ == '__main__':
    if sys.argv[1:2] == ['--cpu-serve']:
        from src.serving.backends.staged_generation_process import serve
        serve(int(sys.argv[2]), engine_factory=IPCFixtureEngine)
    else:
        unittest.main()
