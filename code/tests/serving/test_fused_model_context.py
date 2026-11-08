"""Bind fused method calls to a CPU process's actual ready observation."""
from copy import deepcopy
from dataclasses import asdict, replace
import json
from pathlib import Path
import socket
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.execution_provider.method_workers import lotus_chain_batch as worker
from src.serving.backends import lotus_chain_worker as native_worker
from src.serving.backends.model_context import model_context_record
from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec
from tests.serving.test_shared_chain_migration import FixtureChain, FixtureProcess


class FusedModelContextTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='fused-context-cpu-')
        self.addCleanup(temporary.cleanup)
        self.control = Path(temporary.name)
        (self.control / 'owner').write_text('CPU-owner')
        self.spec = EngineSpec('/tmp/CPU-context-model', sys.executable, sys.executable)
        self.record = native_worker.RecordOccurrence('row-0', 0, 'input', {'text': 'CPU input'})
        decoder = patch.object(native_worker.LotusRecordChain, 'from_dict', side_effect=lambda value: value)
        decoder.start()
        self.addCleanup(decoder.stop)

    def chain(self, stage='first', output=8):
        chain = FixtureChain(stage)
        chain.stages[0].generation = GenerationSpec(max_tokens=output)
        chain.stages[0].model_role = 'main'
        return chain

    def owner(self, chain=None, spec=None, *, cls=worker.FusedChainBatch, **options):
        owner = cls(chain or self.chain(), asdict(spec or self.spec),
            [native_worker._source_binding(self.record)], self.control, time.monotonic() + 30, 2,
            socket.gethostname(), 'CPU-owner', **options)
        owner._engine_type = FixtureProcess
        return owner

    def run_rows(self, owner):
        return owner.run_rows([dict(record_json=json.dumps(self.record.describe()))])

    def test_omission_retains_requested_identity_and_actual_large_context(self):
        owner = self.owner(self.chain(output=8192))
        result = self.run_rows(owner)
        self.assertIsNone(result['error_type'])
        self.assertIsNone(owner.engine_spec.max_model_len)
        observed = json.loads(owner._audit_path.read_text())['model_context']
        self.assertEqual(observed, owner.engine.ready_report['model_context'])
        self.assertIsNone(observed['requested_max_model_len'])
        self.assertEqual(observed['max_model_len'], 32768)
        self.assertEqual(len(owner.engine.calls), 1)
        self.assertEqual(owner.engine.closes, 1)

    def test_explicit_context_is_checked_against_the_same_process(self):
        owner = self.owner(spec=replace(self.spec, max_model_len=8192))
        result = self.run_rows(owner)
        self.assertIsNone(result['error_type'])
        self.assertEqual(owner._model_context['max_model_len'], 8192)
        self.assertEqual(owner._model_context['requested_max_model_len'], 8192)

    def test_missing_failed_closed_or_foreign_ready_never_submits(self):
        valid = dict(engine_uid='CPU-worker', model_context=model_context_record(
            self.spec, 'CPU-worker', 32768))
        foreign = replace(self.spec, model_path='/tmp/CPU-other-model')
        reports = [None, {}, dict(engine_uid='CPU-worker'),
            dict(valid, kind='error'), dict(valid, closed=True),
            dict(valid, engine_uid='different-owner'),
            dict(valid, model_context=dict(valid['model_context'], max_model_len=4096)),
            dict(engine_uid='CPU-worker', model_context=model_context_record(foreign, 'CPU-worker', 32768))]
        for report in reports:
            with self.subTest(report=report):
                class Process(FixtureProcess):
                    def __init__(inner, *args):
                        super().__init__(*args)
                        inner.ready_report = deepcopy(report)
                owner = self.owner()
                owner._engine_type = Process
                result = self.run_rows(owner)
                self.assertIsNotNone(result['error_type'])
                self.assertEqual(owner.engine.calls, [])
                self.assertEqual(owner.engine.closes, 1)
                self.assertEqual([c['status'] for c in result['calls']], ['failed'])
                self.assertEqual(result['calls'][0]['error_type'], 'ModelContextUnavailable')
                self.assertFalse(result['calls'][0]['submission_confirmed'])

    def test_later_stage_output_is_checked_before_the_first_model_call(self):
        chain = self.chain()
        chain.stages += self.chain('later', output=8192).stages
        class SmallProcess(FixtureProcess):
            def __init__(inner, *args):
                super().__init__(*args)
                inner.ready_report['model_context'] = model_context_record(args[0], 'CPU-worker', 4096)
        owner = self.owner(chain)
        owner._engine_type = SmallProcess
        result = self.run_rows(owner)
        self.assertIsNotNone(result['error_type'])
        self.assertEqual(owner.engine.calls, [])
        self.assertEqual(owner.engine.closes, 1)

    def test_context_failure_keeps_allocation_cleanup_unknown(self):
        class UncertainProcess(FixtureProcess):
            def __init__(inner, *args):
                super().__init__(*args)
                inner.ready_report = None
            def close(inner, timeout=None):
                inner.closes += 1
                return dict(status='unknown', process_exit_confirmed=None,
                    engine_api_shutdown_confirmed=False, gpu_exit_confirmed=None)
        owner = self.owner()
        owner._engine_type = UncertainProcess
        result = self.run_rows(owner)
        self.assertEqual(result['calls'][0]['status'], 'failed')
        self.assertEqual(result['engine_close']['status'], 'unknown')
        self.assertEqual(owner.close()['status'], 'unknown')
        self.assertEqual(owner.engine.calls, [])

    def test_relation_phase_reuse_rechecks_context_without_mutating_the_phase(self):
        owner = self.owner(close_on_source_complete=False)
        self.assertIsNone(self.run_rows(owner)['error_type'])
        first_context = deepcopy(owner._model_context)
        previous = owner.chain
        owner.engine.ready_report['model_context'] = model_context_record(self.spec, 'CPU-worker', 65536)
        with self.assertRaisesRegex(RuntimeError, 'changed during'):
            owner.begin_relation_phase(self.chain('next'), [native_worker._source_binding(self.record)],
                initial_parents={(self.record.row_id, 0): ('first-call',)}, columns=['text'],
                allow_nulls=False, source_exhausted=True)
        self.assertIs(owner.chain, previous)
        self.assertEqual(owner._model_context, first_context)
        self.assertEqual(len(owner.engine.calls), 1)
        owner.close()

    def test_separate_owners_do_not_reuse_another_context_observation(self):
        first = self.owner()
        self.assertIsNone(self.run_rows(first)['error_type'])
        class LargerProcess(FixtureProcess):
            def __init__(inner, *args):
                super().__init__(*args)
                inner.ready_report = dict(engine_uid='CPU-other-owner', model_context=model_context_record(
                    args[0], 'CPU-other-owner', 65536))
        second = self.owner(self.chain(output=16384))
        second._engine_type = LargerProcess
        self.assertIsNone(self.run_rows(second)['error_type'])
        self.assertNotEqual(first._model_context['observation_signature'], second._model_context['observation_signature'])
        self.assertEqual(first._model_context['max_model_len'], 32768)
        self.assertEqual(second._model_context['max_model_len'], 65536)

    def test_batch_estimates_are_reported_with_actual_owner_and_context_identity(self):
        from transformers import AutoTokenizer
        owner = self.owner(prepared_batching=dict(policy='token_budget', token_budget=100))
        tokenizer = SimpleNamespace(apply_chat_template=lambda *args, **kwargs: [1, 2, 3])
        with patch.object(AutoTokenizer, 'from_pretrained', return_value=tokenizer):
            result = self.run_rows(owner)
        self.assertIsNone(result['error_type'])
        receipt, = result['request_batches']
        self.assertEqual(receipt['model_context_signature'], owner._model_context['observation_signature'])
        self.assertEqual(receipt['engine_uid'], owner.engine.ready_report['engine_uid'])
        self.assertIsNone(owner.engine_spec.max_model_len)

    def test_native_reference_specialization_keeps_context_unspecified_until_ready(self):
        from src.baselines.text.frameworks.reference_chain_batch import ReferenceFusedChainBatch
        owner = self.owner(cls=ReferenceFusedChainBatch)
        self.assertIsNone(owner.engine_spec.max_model_len)
        self.assertIsNone(self.run_rows(owner)['error_type'])
        self.assertIsNone(owner._model_context['requested_max_model_len'])
        self.assertEqual(owner._model_context['max_model_len'], 32768)
        with self.assertRaisesRegex(ValueError, 'chain max_tokens exceeds'):
            self.owner(self.chain(output=8192), spec=replace(self.spec,max_model_len=4096), cls=ReferenceFusedChainBatch)

    def test_start_report_failure_does_not_become_a_model_construction_failure(self):
        from transformers import AutoTokenizer
        from src.serving.backends import vllm_sync_process as transport

        owner = self.owner(prepared_batching=dict(policy='token_budget', token_budget=100))
        original = transport._publish

        def publish(path, value):
            if Path(path) == owner.worker_report and value.get('engine_allocation_started') is True:
                raise OSError('CPU worker report write failed')
            return original(path, value)

        tokenizer = SimpleNamespace(apply_chat_template=lambda *args, **kwargs: [1, 2, 3])
        with patch.object(AutoTokenizer, 'from_pretrained', return_value=tokenizer), \
                patch.object(transport, '_publish', side_effect=publish), \
                patch.object(owner, '_engine_type') as construct:
            result = self.run_rows(owner)
        construct.assert_not_called()
        self.assertEqual(result['error_type'], 'OSError')
        self.assertFalse(owner._engine_start_attempted)
        self.assertIsNone(owner.engine)
        self.assertEqual([c['error_type'] for c in owner._audit_calls], ['NativeTaskUnconfirmed'])


if __name__ == '__main__':
    unittest.main()
