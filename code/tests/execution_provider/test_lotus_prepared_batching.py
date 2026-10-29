"""Actual public Core query, original LOTUS methods and private CPU batch IPC."""
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.lotus_record_chain_core import SemLoomRecordChainCore
from src.execution_provider.method_workers.lotus_chain_batch import FusedChainBatch
from src.modalities.text.tokenization import chat_token_count
from src.semantic_methods.lotus.prepared_batching import PreparedBatching, PreparedRequestBatcher
from src.semantic_methods.lotus.record_chain import LotusRecordChain, StageSpec, MapMethod
from src.semantic_methods.lotus.record_results import ChainExecutionError
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources
from src.serving.backends.staged_generation_process import ChainEngineProcess
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec, _prepared
from tests.execution_provider.chain_tokenizer_fixture import tokenizer
from tests.semantic_methods import test_lotus_chain_composition as fixture


class Process(ChainEngineProcess):
    @staticmethod
    def _command(engine, result_fd):
        return [engine.model_python, str(Path(__file__).with_name('prepared_batch_cpu_engine.py')), str(result_fd)]


class Batch(FusedChainBatch):
    _engine_type = Process


class Owner(SemLoomRecordChainCore):
    _batch_type = Batch


class PreparedBatchingTests(unittest.TestCase):
    def setUp(self):
        import lotus
        scope = lotus.settings.context(enable_cache=False); scope.__enter__()
        self.addCleanup(scope.__exit__, None, None, None)
        temp = tempfile.TemporaryDirectory(prefix='prepared-batch-'); self.addCleanup(temp.cleanup)
        self.root = Path(temp.name); self.tokenizer = tokenizer()
        self.tokenizer.save_pretrained(self.root / 'tokenizer')
        self.rows = fixture.source_rows(column='text', size=4)
        for row, text in zip(self.rows, ('short', 'short', 'long ' * 400, 'short')):
            row.values['text'] = text
        self.chain = LotusRecordChain((
            StageSpec('label', MapMethod('openai/cpu-fixture', 'Label {text}', 'label'), GenerationSpec(max_tokens=64)),
            StageSpec('keep', FilterMethod('openai/cpu-fixture', 'Is {label} positive?'), GenerationSpec(max_tokens=8)),
        ), query_id='same-prepared-chain')
        run = self.chain.start_batch(self.rows); self.assertTrue(run.prepare())
        _, conversations = _prepared(run.prepared)
        self.B = sum(chat_token_count(self.tokenizer, c) + 64 for c in conversations[:2])

    def execute(self, policy, *, fail_batch=None, cancel_after=None, corrupt_batch=None,
                invalid_success=None, allow_failure=False):
        model = Path(tempfile.mkdtemp(prefix=policy+'-', dir=self.root)) / 'model.json'
        spec = EngineSpec(str(model), sys.executable, sys.executable,
            tokenizer_path=str(self.root / 'tokenizer'), max_model_len=4096, max_num_seqs=4)
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=4,
            max_active_batches=1, startup_timeout_s=12, query_timeout_s=20)
        config = PreparedBatching(policy, max_rows=4, token_budget=self.B if policy == 'token_budget' else 0)
        owner = Owner(spec, resources, control_dir=self.root, prepared_batching=config)
        error, records = None, []
        with patch.object(EngineSpec, 'validate_driver_environment', return_value={}):
            stream = owner.evaluate(iter(self.rows), self.chain, expected=self.rows, query_id=self.chain.query_id)
            model.write_text(json.dumps(dict(fail_batch=fail_batch, cancel_after=cancel_after,
                                           corrupt_batch=corrupt_batch,
                                           invalid_success_batch=None if invalid_success is None else 0,
                                           invalid_success=invalid_success,
                                           stop_file=str(owner.control / 'stop'))))
            try:
                records = list(stream)
            except Exception as exc:
                if not allow_failure:
                    raise
                error = exc
            finally:
                report = owner.close(timeout=3)
        batches = [json.loads(s) for s in Path(str(model) + '.batches').read_text().splitlines()]
        return owner, records, report, batches, error

    def test_real_native_facing_batch_boundaries_change_without_changing_semantics(self):
        outputs = []
        for policy in ('fixed_rows', 'token_budget'):
            owner, records, report, native, error = self.execute(policy)
            self.assertIsNone(error)
            self.assertEqual(report['status'], 'closed'); self.assertTrue(report['query_completed'])
            self.assertEqual(len(owner.calls), 8)
            self.assertEqual(len({c['call_id'] for c in owner.calls}), 8)
            self.assertEqual(len({c['engine_uid'] for c in owner.calls}), 1)
            declared = owner.request_batches
            self.assertEqual([b['call_ids'] for b in declared], [[c['call_id'] for c in b['calls']] for b in native])
            if policy == 'fixed_rows': self.assertEqual([len(b['calls']) for b in native], [4, 4])
            else:
                self.assertEqual([len(b['calls']) for b in native if b['calls'][0]['algorithm_stage'] == 'label'], [2, 1, 1])
                self.assertTrue(any(b['oversize'] for b in declared))
            for receipt, actual in zip(declared, native):
                input_tokens = sum(chat_token_count(self.tokenizer, conversation) for conversation in actual['messages'])
                self.assertEqual(receipt['input_tokens'], input_tokens)
                self.assertEqual(receipt['estimated_tokens'], input_tokens + len(actual['calls']) * actual['generation']['max_tokens'])
                self.assertLessEqual(len(actual['calls']), 4)
                if policy == 'token_budget' and not receipt['oversize']:
                    self.assertLessEqual(receipt['estimated_tokens'], self.B)
            successor = [b for b in native if b['calls'][0]['algorithm_stage'] == 'keep']
            self.assertTrue(all('generated' in json.dumps(b['messages']) for b in successor))
            outputs.append(fixture.normalized(dict(records=records, calls=owner.calls, columns=list(records[0].values))))
        self.assertEqual(outputs[0], outputs[1])

    def test_failed_native_batch_keeps_prefix_unknown_and_never_submits_suffix(self):
        owner, records, report, native, error = self.execute('token_budget', fail_batch=1, allow_failure=True)
        self.assertIsNotNone(error); self.assertFalse(records)
        self.assertFalse(report['query_completed'])
        self.assertEqual(len(native), 2)
        self.assertEqual(len(owner.request_batches), 2)
        self.assertEqual([call['status'] for call in owner.calls], ['succeeded', 'succeeded', 'unknown', 'failed'])
        self.assertEqual([call['submission_confirmed'] for call in owner.calls], [True, True, False, False])
        self.assertEqual(owner.calls[-1]['error_type'], 'BatchStoppedBeforeSubmission')
        self.assertEqual(owner.calls[0]['total_tokens'], 2)
        self.assertEqual(report['status'], 'unknown')
        self.assertGreater(report['core_usage']['held_tasks'], 0)
        owner.close(timeout=1)
        self.assertEqual(len(Path(owner.engine.model_path + '.batches').read_text().splitlines()), 2)

    def test_cancel_between_batches_preserves_returned_prefix_without_later_model_calls(self):
        owner, records, report, native, error = self.execute('token_budget', cancel_after=0, allow_failure=True)
        self.assertIsNotNone(error); self.assertFalse(records)
        self.assertFalse(report['query_completed']); self.assertEqual(len(native), 1)
        self.assertEqual([call['status'] for call in owner.calls], ['succeeded', 'succeeded', 'failed', 'failed'])
        self.assertTrue(all(call['submission_confirmed'] is False for call in owner.calls[2:]))
        self.assertEqual(len(owner.request_batches), 1)

    def test_malformed_batch_does_not_erase_known_prefix_or_trigger_more_batches(self):
        owner, records, report, native, error = self.execute('token_budget', corrupt_batch=1, allow_failure=True)
        self.assertIsNotNone(error); self.assertFalse(records)
        self.assertFalse(report['query_completed']); self.assertEqual(len(native), 2)
        self.assertEqual([call['status'] for call in owner.calls[:2]], ['succeeded', 'succeeded'])
        self.assertEqual([call['total_tokens'] for call in owner.calls[:2]], [2, 2])
        self.assertEqual(owner.calls[2]['status'], 'unknown')
        self.assertEqual(owner.calls[-1]['error_type'], 'BatchStoppedBeforeSubmission')
        self.assertGreater(len(owner.results.unassociated_results), 0)

    def test_invalid_success_stops_following_native_groups_before_stage_parsing(self):
        faults = ['converter_missing_usage', {'total_tokens':3}, {'text':None},
                  {'finish_reason':'abort'}, {'completion_tokens':True}]
        for fault in faults:
            with self.subTest(fault=fault):
                owner, records, report, native, error = self.execute('token_budget',
                    invalid_success=fault, allow_failure=True)
                self.assertIsNotNone(error); self.assertFalse(records)
                self.assertIsInstance(error, ChainExecutionError)
                self.assertEqual(error.remote_error_type, 'RuntimeError')
                self.assertFalse(report['query_completed'])
                self.assertEqual(len(native), 1)
                self.assertEqual(len(owner.request_batches), 1)
                self.assertEqual(owner.calls[0]['status'], 'succeeded')
                self.assertIs(owner.calls[0]['submission_confirmed'], True)
                self.assertEqual(owner.calls[1]['total_tokens'], 2)
                self.assertTrue(all(call['status'] == 'failed' and call['submission_confirmed'] is False
                    and call['error_type'] == 'BatchStoppedBeforeSubmission' for call in owner.calls[2:]))
                if fault == 'converter_missing_usage':
                    self.assertIsNone(owner.calls[0]['prompt_tokens'])
                    self.assertEqual(owner.calls[0]['completion_tokens'], 1)

    def test_constructor_failure_marks_no_call_attempted_but_keeps_model_cleanup_unknown(self):
        owner = Owner(EngineSpec('CPU-fixture', sys.executable, sys.executable,
            tokenizer_path=str(self.root/'tokenizer')),
            RecordsResources(model_batch_rows=4, max_active_batches=1), control_dir=self.root,
            prepared_batching=PreparedBatching('fixed_rows', 1))
        with patch.object(EngineSpec, 'validate_driver_environment', return_value={}), \
                patch.object(Process, '__init__', side_effect=RuntimeError('CPU allocation failed')), \
                patch.object(Process, 'run_prepared') as submit:
            stream = owner.evaluate(iter(self.rows), self.chain, expected=self.rows, query_id=self.chain.query_id)
            try:
                with self.assertRaises(ChainExecutionError) as caught: list(stream)
                self.assertEqual(caught.exception.remote_error_type, 'RuntimeError')
            finally:
                report = owner.close(timeout=1)
            submit.assert_not_called()
        self.assertEqual(owner.request_batches, [])
        self.assertEqual(len(owner.calls), 4)
        self.assertTrue(all(call['status'] == 'failed' and call['submission_confirmed'] is False
            and call['error_type'] == 'BatchStoppedBeforeSubmission' for call in owner.calls))
        self.assertFalse(report['query_completed']); self.assertEqual(report['status'], 'unknown')
        self.assertEqual(report['model_owner']['status'], 'unknown')
        self.assertIs(report['model_owner']['engine_api_shutdown_confirmed'], False)

    def test_configuration_rejects_invalid_or_unsupported_scope_before_model(self):
        for args in [('typo', 4, 0), ('token_budget', 4, 0), ('fixed_rows', True, 0),
                     ('fixed_rows', 4, 10), ('token_budget', 1, True)]:
            with self.subTest(args=args), self.assertRaises(ValueError): PreparedBatching(*args)
        spec = EngineSpec('CPU-fixture', sys.executable, sys.executable)
        with self.assertRaisesRegex(ValueError, 'fused'):
            Owner(spec, RecordsResources(), execution='record_local_sections', prepared_batching=PreparedBatching('fixed_rows', 1))
        with self.assertRaisesRegex(ValueError, 'model_batch_rows'):
            Owner(spec, RecordsResources(), prepared_batching=PreparedBatching('fixed_rows', 10000))
        custom = EngineSpec('CPU-fixture', sys.executable, sys.executable, chat_template='override')
        with self.assertRaisesRegex(ValueError, 'tokenizer-owned chat template'):
            Owner(custom, RecordsResources(), prepared_batching=PreparedBatching('fixed_rows', 1))

    def test_extract_or_role_changes_fail_before_source_consumption_and_engine_preflight(self):
        role_change = LotusRecordChain((self.chain.stages[0],
            StageSpec('keep', self.chain.stages[1].method, GenerationSpec(max_tokens=8), model_role='another')),
            query_id=self.chain.query_id)
        for chain in (fixture.chain_for('ME'), role_change):
            owner = Owner(EngineSpec('CPU-fixture', sys.executable, sys.executable),
                RecordsResources(max_active_batches=1), prepared_batching=PreparedBatching('fixed_rows', 1))
            def source():
                raise AssertionError('unsupported query consumed source')
                yield
            with self.subTest(chain=chain.describe()), patch.object(EngineSpec, 'validate_driver_environment') as preflight:
                with self.assertRaisesRegex(ValueError, 'Filter/Map with one model role'):
                    owner.evaluate(source(), chain, expected=self.rows, query_id=chain.query_id)
                preflight.assert_not_called()

    def test_local_tokenizer_failure_is_presubmit_and_never_starts_model(self):
        owner = Owner(EngineSpec('CPU-fixture', sys.executable, sys.executable),
            RecordsResources(model_batch_rows=4, max_active_batches=1), control_dir=self.root,
            prepared_batching=PreparedBatching('token_budget', 4, self.B))
        with patch.object(EngineSpec, 'validate_driver_environment', return_value={}), \
                patch('transformers.AutoTokenizer.from_pretrained', side_effect=ValueError('CPU template failure')), \
                patch.object(Process, '__init__', side_effect=AssertionError('model started')) as start:
            stream = owner.evaluate(iter(self.rows), self.chain, expected=self.rows, query_id=self.chain.query_id)
            try:
                with self.assertRaises(Exception): list(stream)
            finally:
                report = owner.close(timeout=3)
            start.assert_not_called()
        self.assertEqual(len(owner.calls), 4)
        self.assertTrue(all(call['status'] == 'failed' and call['submission_confirmed'] is False
                            and call['error_type'] == 'PreparedBatchingEstimateError' for call in owner.calls))
        self.assertEqual(owner.request_batches, [])
        self.assertEqual(report['status'], 'closed'); self.assertFalse(report['query_completed'])

    def test_builder_does_not_merge_incompatible_roles(self):
        run = self.chain.start_batch(self.rows[:2]); self.assertTrue(run.prepare())
        rows = [dict(run.prepared[0]), dict(run.prepared[1], model_role='different-role')]
        groups = PreparedRequestBatcher(PreparedBatching('fixed_rows', 4), self.tokenizer).batches(rows, run.method.stage.generation)
        self.assertEqual([len(group.rows) for group in groups], [1, 1])
        self.assertEqual(groups[0].reason, 'compatibility')
