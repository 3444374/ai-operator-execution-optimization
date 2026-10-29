"""Actual public Core query, original LOTUS methods and private CPU batch IPC."""
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.lotus_record_chain_core import CoreChainCapacity, SemLoomRecordChainCore
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
        ), query_id='same-prepared-chain')
        run = self.chain.start_batch(self.rows); self.assertTrue(run.prepare())
        _, conversations = _prepared(run.prepared)
        self.B = sum(chat_token_count(self.tokenizer, c) + 64 for c in conversations[:2])

    def execute(self, policy, *, fail_batch=None, cancel_after=None, corrupt_batch=None,
                invalid_success=None, allow_failure=False, read_rows=4, max_rows=4,
                rows=None, chain=None, budget=None, capacity=None, false_positions=None):
        rows = self.rows if rows is None else rows
        chain = self.chain if chain is None else chain
        model = Path(tempfile.mkdtemp(prefix=str(policy)+'-', dir=self.root)) / 'model.json'
        spec = EngineSpec(str(model), sys.executable, sys.executable,
            tokenizer_path=str(self.root / 'tokenizer'), max_model_len=4096, max_num_seqs=4)
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=read_rows,
            max_active_batches=1, startup_timeout_s=12, query_timeout_s=20)
        config = (None if policy is None else PreparedBatching(policy, max_rows=max_rows,
                  token_budget=(self.B if budget is None else budget) if policy == 'token_budget' else 0))
        owner = Owner(spec, resources, control_dir=self.root, prepared_batching=config, capacity=capacity)
        error, records = None, []
        with patch.object(EngineSpec, 'validate_driver_environment', return_value={}):
            stream = owner.evaluate(iter(rows), chain, expected=rows, query_id=chain.query_id)
            model.write_text(json.dumps(dict(fail_batch=fail_batch, cancel_after=cancel_after,
                                           corrupt_batch=corrupt_batch,
                                           invalid_success_batch=None if invalid_success is None else 0,
                                           invalid_success=invalid_success,
                                           false_positions=false_positions,
                                           stop_file=str(owner.control / 'stop'))))
            try:
                records = list(stream)
            except Exception as exc:
                if not allow_failure:
                    raise
                error = exc
            finally:
                report = owner.close(timeout=3)
        log = Path(str(model) + '.batches')
        batches = [] if not log.exists() else [json.loads(s) for s in log.read_text().splitlines()]
        return owner, records, report, batches, error

    def test_real_native_facing_batch_boundaries_change_without_changing_semantics(self):
        outputs = []
        for policy in ('fixed_rows', 'token_budget'):
            owner, records, report, native, error = self.execute(policy)
            self.assertIsNone(error)
            self.assertEqual(report['status'], 'closed'); self.assertTrue(report['query_completed'])
            self.assertEqual(len(owner.calls), 4)
            self.assertEqual(len({c['call_id'] for c in owner.calls}), 4)
            self.assertEqual(len({c['engine_uid'] for c in owner.calls}), 1)
            declared = owner.request_batches
            self.assertEqual([b['call_ids'] for b in declared], [[c['call_id'] for c in b['calls']] for b in native])
            if policy == 'fixed_rows': self.assertEqual([len(b['calls']) for b in native], [4])
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
            outputs.append(fixture.normalized(dict(records=records, calls=owner.calls, columns=list(records[0].values))))
        self.assertEqual(outputs[0], outputs[1])

    def test_failed_native_batch_keeps_prefix_unknown_and_never_submits_suffix(self):
        owner, records, report, native, error = self.execute('token_budget', fail_batch=1, allow_failure=True)
        self.assertIsNotNone(error); self.assertFalse(records)
        self.assertFalse(report['query_completed'])
        self.assertEqual(len(native), 2)
        self.assertEqual(len(owner.request_batches), 2)
        self.assertEqual([call['status'] for call in owner.calls], ['succeeded', 'succeeded', 'unknown'])
        self.assertEqual([call['submission_confirmed'] for call in owner.calls], [True, True, False])
        self.assertEqual(owner.calls[0]['total_tokens'], 2)
        self.assertEqual(report['status'], 'unknown')
        self.assertGreater(report['core_usage']['held_tasks'], 0)
        owner.close(timeout=1)
        self.assertEqual(len(Path(owner.engine.model_path + '.batches').read_text().splitlines()), 2)

    def test_cancel_between_batches_preserves_returned_prefix_without_later_model_calls(self):
        owner, records, report, native, error = self.execute('token_budget', cancel_after=0, allow_failure=True)
        self.assertIsNotNone(error); self.assertFalse(records)
        self.assertFalse(report['query_completed']); self.assertEqual(len(native), 1)
        self.assertEqual([call['status'] for call in owner.calls], ['succeeded', 'succeeded'])
        self.assertEqual(len(owner.request_batches), 1)

    def test_malformed_batch_does_not_erase_known_prefix_or_trigger_more_batches(self):
        owner, records, report, native, error = self.execute('token_budget', corrupt_batch=1, allow_failure=True)
        self.assertIsNotNone(error); self.assertFalse(records)
        self.assertFalse(report['query_completed']); self.assertEqual(len(native), 2)
        self.assertEqual([call['status'] for call in owner.calls[:2]], ['succeeded', 'succeeded'])
        self.assertEqual([call['total_tokens'] for call in owner.calls[:2]], [2, 2])
        self.assertEqual(owner.calls[2]['status'], 'unknown')
        self.assertEqual(len(owner.calls), 3)
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
                self.assertEqual(len(owner.calls), 2)
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
            StageSpec('keep', FilterMethod('openai/cpu-fixture', 'Is {label} positive?'),
                      GenerationSpec(max_tokens=8), model_role='another')),
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
        self.assertEqual(owner.calls, [])
        self.assertEqual(owner.request_batches, [])
        self.assertEqual(report['status'], 'closed'); self.assertFalse(report['query_completed'])

    def test_builder_does_not_merge_incompatible_roles(self):
        run = self.chain.start_batch(self.rows[:2]); self.assertTrue(run.prepare())
        rows = [dict(run.prepared[0]), dict(run.prepared[1], model_role='different-role')]
        groups = PreparedRequestBatcher(PreparedBatching('fixed_rows', 4), self.tokenizer).batches(rows, run.method.stage.generation)
        self.assertEqual([len(group.rows) for group in groups], [1, 1])
        self.assertEqual(groups[0].reason, 'compatibility')

    def test_read_chunk_sizes_do_not_change_short_long_or_mixed_model_groups(self):
        for texts in (['short'] * 9, ['long ' * 400] * 9,
                      ['short', 'short', 'long ' * 400, 'short'] * 3):
            rows = fixture.source_rows(column='text', size=len(texts))
            for row, text in zip(rows, texts): row.values['text'] = text
            reference = None
            for read_rows in (1, 2, 7, 64):
                with self.subTest(lengths=[len(t) for t in texts], read_rows=read_rows):
                    owner, records, report, native, error = self.execute('token_budget',
                        rows=rows, read_rows=read_rows, max_rows=128)
                    self.assertIsNone(error); self.assertTrue(report['query_completed'])
                    actual = [[r['call_id'] for r in b['calls']] for b in native]
                    if reference is None: reference = actual
                    self.assertEqual(actual, reference)
                    self.assertEqual(len(records), len(rows))
                    self.assertEqual(report['batch_concurrency'], 1)
                    self.assertEqual(report['source_pull_batch_rows'], read_rows)
                    self.assertEqual(report['model_batch_row_guard'], 128)
                    for receipt in owner.request_batches:
                        if receipt['oversize']:
                            self.assertEqual(len(receipt['call_ids']), 1)
                        else: self.assertLessEqual(receipt['estimated_tokens'], self.B)

    def test_token_batch_can_exceed_64_reads_and_engine_sequence_capacity(self):
        rows = fixture.source_rows(column='text', size=80)
        for row in rows: row.values['text'] = 'short'
        owner, records, report, native, error = self.execute('token_budget',
            rows=rows, read_rows=1, max_rows=128, budget=1_000_000)
        self.assertIsNone(error); self.assertEqual(len(records), 80)
        self.assertEqual([len(b['calls']) for b in native], [80])
        self.assertEqual(owner.engine.max_num_seqs, 4)
        self.assertEqual(owner.request_batches[0]['flush_reason'], 'source_exhausted')
        self.assertEqual(report['peak_core_usage']['held_tasks'], 1)

    def test_no_row_cap_at_B17506_is_not_replaced_by_128_or_read_chunk_size(self):
        rows = fixture.source_rows(column='text', size=400)
        for row in rows: row.values['text'] = 'short'
        chain = LotusRecordChain((StageSpec('label', self.chain.stages[0].method,
            GenerationSpec(max_tokens=8)),), query_id=self.chain.query_id)
        run = chain.start_batch(rows[:1]); run.prepare()
        cost = chat_token_count(self.tokenizer, _prepared(run.prepared)[1][0]) + 8
        full, tail = divmod(len(rows), 17506 // cost)
        sizes = [17506 // cost] * full + ([tail] if tail else [])
        self.assertGreater(sizes[0], 128)
        for read_rows in (1, 64):
            owner, records, report, native, error = self.execute('token_budget', rows=rows,
                chain=chain, read_rows=read_rows, max_rows=None, budget=17506)
            self.assertIsNone(error); self.assertEqual(len(records), len(rows))
            self.assertEqual([len(b['calls']) for b in native], sizes)
            self.assertIsNone(report['model_batch_row_guard'])
            self.assertEqual(owner._fused_limits(1).active_work, len(rows))
            for batch in owner.request_batches:
                self.assertIsNone(batch['max_rows'])
                self.assertNotEqual(batch['flush_reason'], 'max_rows')
                self.assertLessEqual(batch['estimated_tokens'], 17506)

    def test_no_row_cap_keeps_oversize_EOF_cancel_and_unknown_semantics(self):
        for fault, sizes, status in (({}, [2, 1, 1], 'closed'),
                ({'cancel_after':0}, [2], None), ({'fail_batch':1}, [2, 1], 'unknown')):
            owner, records, report, native, error = self.execute('token_budget',
                read_rows=1, max_rows=None, allow_failure=True, **fault)
            self.assertEqual([len(b['calls']) for b in native], sizes)
            if status is None:
                # Cancellation preserves separately uncertain cleanup; successful
                # request receipts do not turn that report into a closed owner.
                self.assertIn(report['status'], ('closed', 'unknown'))
            else:
                self.assertEqual(report['status'], status, fault)
            self.assertTrue(all(b['max_rows'] is None and b['flush_reason'] != 'max_rows'
                                for b in owner.request_batches))
            if not fault:
                self.assertIsNone(error); self.assertEqual(len(records), 4)
                self.assertTrue(owner.request_batches[1]['oversize'])
                self.assertEqual(owner.request_batches[-1]['flush_reason'], 'source_exhausted')
            else:
                self.assertIsNotNone(error)
                self.assertEqual([c['status'] for c in owner.calls[:2]], ['succeeded', 'succeeded'])
                if status == 'unknown': self.assertEqual(report['core_usage']['held_tasks'], 1)
        owner, records, report, native, error = self.execute('token_budget', max_rows=None, rows=[])
        self.assertIsNone(error); self.assertFalse(native); self.assertEqual(records, [])
        self.assertTrue(report['query_completed'])

    def test_no_row_cap_retains_the_separate_byte_guard(self):
        rows = fixture.source_rows(column='text', size=7)
        for row in rows: row.values['text'] = 'short'
        owner, records, report, native, error = self.execute('token_budget',
            rows=rows, read_rows=1, max_rows=None, budget=1_000_000,
            capacity=CoreChainCapacity(item_input_bytes=2048), allow_failure=True)
        self.assertIsNone(error); self.assertEqual(len(records), 7)
        self.assertTrue(any(b['flush_reason'] == 'max_bytes' for b in owner.request_batches))
        self.assertTrue(all(b['flush_reason'] != 'max_rows' for b in owner.request_batches))
        owner, records, report, native, error = self.execute('token_budget', max_rows=None,
            capacity=CoreChainCapacity(item_input_bytes=32), allow_failure=True)
        self.assertIsNotNone(error); self.assertFalse(native); self.assertEqual(owner.calls, [])
        self.assertEqual(report['status'], 'closed')

    def test_token_config_can_omit_rows_but_fixed_rows_still_requires_them(self):
        config = PreparedBatching('token_budget', token_budget=17506)
        self.assertIsNone(config.max_rows)
        self.assertIsNone(PreparedBatching(**json.loads(json.dumps(
            dict(policy='token_budget', max_rows=None, token_budget=17506)))).max_rows)
        with self.assertRaisesRegex(ValueError, 'max_rows'):
            PreparedBatching('fixed_rows')

    def test_tail_merges_across_reads_and_larger_B_changes_groups_at_constant_C(self):
        rows = fixture.source_rows(column='text', size=7)
        for row in rows: row.values['text'] = 'short'
        run = self.chain.start_batch(rows[:1]); run.prepare()
        cost = chat_token_count(self.tokenizer, _prepared(run.prepared)[1][0]) + 64
        for multiple, sizes in ((2, [2, 2, 2, 1]), (3, [3, 3, 1]), (5, [5, 2])):
            owner, records, report, native, error = self.execute('token_budget',
                rows=rows, read_rows=2, max_rows=128, budget=multiple * cost)
            self.assertIsNone(error); self.assertEqual(len(records), 7)
            self.assertEqual([len(b['calls']) for b in native], sizes)
            self.assertEqual(owner.request_batches[-1]['flush_reason'], 'source_exhausted')
            self.assertEqual(report['batch_concurrency'], 1)
            self.assertEqual(report['peak_core_usage']['held_tasks'], 1)

    def test_row_and_byte_guards_are_independent_and_report_why_they_flush(self):
        from src.semantic_methods.lotus.record_results import record_row
        rows = fixture.source_rows(column='text', size=7)
        for row in rows: row.values['text'] = 'short'
        run = self.chain.start_batch(rows); run.prepare()
        charge = lambda record, row: len(json.dumps([
            dict(record_row(record), prepared=row)], ensure_ascii=False, separators=(',', ':')).encode())
        allowance = sum(sorted((charge(r, p) for r, p in zip(rows, run.prepared)), reverse=True)[:2])
        for guard, capacity, sizes, reason in (
                (5, None, [5, 2], 'max_rows'),
                (128, CoreChainCapacity(item_input_bytes=allowance), [2, 2, 2, 1], 'max_bytes')):
            owner, records, report, native, error = self.execute('token_budget', rows=rows,
                read_rows=1, max_rows=guard, budget=1_000_000, capacity=capacity)
            self.assertIsNone(error); self.assertEqual(len(records), 7)
            self.assertEqual([len(b['calls']) for b in native], sizes)
            self.assertEqual(owner.request_batches[0]['flush_reason'], reason)
            self.assertLessEqual(report['peak_core_usage']['input_bytes'], owner.capacity.item_input_bytes)
            self.assertLessEqual(report['peak_core_usage']['active_work'], guard)

    def test_byte_oversize_is_presubmit_and_does_not_create_unknown_model_work(self):
        owner, records, report, native, error = self.execute('token_budget',
            capacity=CoreChainCapacity(item_input_bytes=32), allow_failure=True)
        self.assertIsNotNone(error); self.assertFalse(records)
        self.assertEqual(native, []); self.assertEqual(owner.calls, [])
        self.assertEqual(report['core_usage']['held_tasks'], 0)
        self.assertEqual(report['status'], 'closed'); self.assertFalse(report['query_completed'])

    def test_later_buffer_failure_keeps_the_completed_prefix_and_stops_submission(self):
        rows = fixture.source_rows(column='text', size=2)
        rows[0].values['text'], rows[1].values['text'] = 'short', 'long ' * 10000
        run = self.chain.start_batch(rows[:1]); run.prepare()
        cost = chat_token_count(self.tokenizer, _prepared(run.prepared)[1][0]) + 64
        owner, records, report, native, error = self.execute('token_budget', rows=rows,
            read_rows=1, max_rows=128, budget=cost,
            capacity=CoreChainCapacity(item_input_bytes=2048), allow_failure=True)
        self.assertIsNotNone(error); self.assertEqual(len(native), 1)
        self.assertEqual(len(owner.calls), 1); self.assertEqual(owner.calls[0]['status'], 'succeeded')
        self.assertIs(owner.calls[0]['submission_confirmed'], True)
        self.assertEqual(report['core_usage']['held_tasks'], 0)
        self.assertEqual(report['status'], 'closed', report); self.assertFalse(report['query_completed'])

    def test_idle_reaper_first_after_buffer_error_confirms_API_and_preserves_prefix(self):
        from src.execution_provider.adapters.lotus_record_chain_core import _ChainTransport

        original, reaped = _ChainTransport.close_worker, []

        def reap_first(transport, timeout):
            process = transport.worker.engine
            self.assertTrue((transport.owner.control / 'stop').exists())
            process._reaper.join(timeout=3)
            self.assertFalse(process._reaper.is_alive())
            reaped.append(process)
            return original(transport, timeout)

        rows = fixture.source_rows(column='text', size=2)
        rows[0].values['text'], rows[1].values['text'] = 'short', 'long ' * 10000
        run = self.chain.start_batch(rows[:1]); run.prepare()
        cost = chat_token_count(self.tokenizer, _prepared(run.prepared)[1][0]) + 64
        with patch.object(_ChainTransport, 'close_worker', reap_first):
            owner, records, report, native, error = self.execute('token_budget', rows=rows,
                read_rows=1, max_rows=None, budget=cost,
                capacity=CoreChainCapacity(item_input_bytes=2048), allow_failure=True)
        self.assertIsNotNone(error); self.assertEqual(len(native), 1)
        self.assertEqual(len(owner.calls), 1); self.assertEqual(owner.calls[0]['status'], 'succeeded')
        self.assertIs(owner.calls[0]['submission_confirmed'], True)
        self.assertTrue(all(value == 0 for value in report['core_usage'].values()))
        self.assertEqual(report['status'], 'closed', report)
        self.assertTrue(report['model_owner']['engine_api']['engine_api_shutdown_confirmed'])
        self.assertFalse(report['query_completed'])
        self.assertTrue(reaped)
        self.assertTrue(all(process._broken and process._closed for process in reaped))

    def test_empty_source_and_all_filtered_rows_complete_without_phantom_calls(self):
        owner, records, report, native, error = self.execute('token_budget', rows=[], read_rows=2)
        self.assertIsNone(error); self.assertEqual(records, []); self.assertEqual(native, [])
        self.assertEqual(owner.calls, []); self.assertTrue(report['query_completed'])
        chain = LotusRecordChain((StageSpec('keep', FilterMethod('openai/cpu-fixture',
            'Keep {text}?'), GenerationSpec(max_tokens=8)),), query_id=self.chain.query_id)
        owner, records, report, native, error = self.execute('token_budget', chain=chain,
            read_rows=1, max_rows=128, budget=1_000_000, false_positions=list(range(4)))
        self.assertIsNone(error); self.assertEqual(records, [])
        self.assertEqual(len(owner.calls), 4); self.assertTrue(report['query_completed'])

    def test_multi_stage_token_mode_rejects_before_source_and_model(self):
        chain = LotusRecordChain((*self.chain.stages, StageSpec('keep',
            FilterMethod('openai/cpu-fixture', 'Keep {label}?'), GenerationSpec(max_tokens=8))),
            query_id=self.chain.query_id)
        owner = Owner(EngineSpec('CPU-fixture', sys.executable, sys.executable),
            RecordsResources(max_active_batches=1), prepared_batching=PreparedBatching('token_budget', 128, 8192))
        def source():
            raise AssertionError('unsupported chain consumed source')
            yield
        with patch.object(EngineSpec, 'validate_driver_environment') as preflight:
            with self.assertRaisesRegex(ValueError, 'single Filter or Map'):
                owner.evaluate(source(), chain, expected=self.rows, query_id=chain.query_id)
            preflight.assert_not_called()
        # Existing row-batch chain behavior still prepares successors from real outputs.
        owner, records, report, native, error = self.execute('fixed_rows', chain=chain)
        self.assertIsNone(error); self.assertTrue(report['query_completed'])
        self.assertEqual([len(b['calls']) for b in native], [4, 4])
        self.assertIn('generated', json.dumps(native[1]['messages']))

    def test_disabled_batching_retains_original_read_batches_and_needs_no_tokenizer(self):
        with patch('transformers.AutoTokenizer.from_pretrained', side_effect=AssertionError('extra tokenizer')):
            owner, records, report, native, error = self.execute(None, read_rows=2)
        self.assertIsNone(error); self.assertTrue(report['query_completed'])
        self.assertEqual([len(b['calls']) for b in native], [2, 2])
        self.assertEqual(owner.request_batches, [])

    def test_single_stage_relation_flag_keeps_the_token_row_guard_in_core(self):
        chain = LotusRecordChain(self.chain.stages, query_id=self.chain.query_id, relation_barriers=True)
        owner, records, report, native, error = self.execute('token_budget', chain=chain,
            read_rows=1, max_rows=128, budget=1_000_000)
        self.assertIsNone(error); self.assertEqual(len(records), 4)
        self.assertEqual([len(b['calls']) for b in native], [4])
        self.assertTrue(report['query_completed'])
        self.assertEqual(report['model_batch_mode'], 'single_stage_cross_read_token_batches')
