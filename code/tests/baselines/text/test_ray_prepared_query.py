"""Complete original sink ordering, occurrence identity and failures, CPU only."""

import asyncio
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.baselines.text import sembench_prepared as semantics
from src.experiments import native_ray_sembench as driver
from src.experiments.attempt_ledger import AttemptBudget, BudgetExhausted
from src.experiments.cell_budget import CellBudgetLedger
from src.serving.backends.vllm_protocol import EngineSpec
from tests.baselines.text import test_sembench_prepared
from tests.baselines.text.ray_native_fixtures import recorded_processor


class RayPreparedTests(unittest.TestCase):
    @unittest.skipUnless(os.environ.get('SEMLOOM_NATIVE_TEST_CHECKOUT'), 'requires pinned public query source')
    def test_saved_single_operator_replay_migrates_ids_and_checks_original_relation_without_new_native_work(self):
        import pandas as pd
        from src.baselines.text.sembench_text import SemBenchTextQuery
        from src.infrastructure.private_artifacts import json_metric
        from src.serving.backends.vllm_protocol import GenerationSpec
        from tests.baselines.text.replay_ray_sembench import replay

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = root/'data'; data.mkdir()
            texts = ['1', '2', '4', '5']
            pd.DataFrame(dict(id=['ant_man_and_the_wasp_quantumania'] * 4,
                reviewId=list(range(4)), reviewText=['same review'] * 4,
                originalScore=[text+'/5' for text in texts])).to_csv(data/'Reviews.csv', index=False)
            config = dict(schema='semloom.sembench_text_run.v1', scenario='movie', query='9',
                ranking='map', query_id='replay-migration', backend='ray-data',
                model_name='openai/local-qwen2.5-7b', generation=dict(max_tokens=8192, stop=[]),
                engine=dict(model_path='CPU-no-model', model_python='unused', driver_python='unused', max_model_calls=4),
                resources=dict(query_timeout_s=30), profile={},
                ray_native=dict(engine_kwargs={}, processor_options={}))
            path = root/'config.json'; path.write_text(json.dumps(config))
            query = SemBenchTextQuery(os.environ['SEMLOOM_NATIVE_TEST_CHECKOUT'], data, 'movie', '9',
                execution_id=config['query_id'], model_name=config['model_name'],
                generation=GenerationSpec(max_tokens=8192, stop=()), ranking='map')
            method, calls, requests = semantics.prepare(query)
            expected, semantic = semantics.finish(query, method, calls,
                [dict(call_id=row['call_id'], generated_text=text) for row, text in zip(requests, texts, strict=True)])
            # An explicit software tokenizer tests replay plumbing only.
            tokenize = lambda messages, **kw: list(json.dumps(messages).encode())
            reference = dict(calls=calls, token_ids=[tokenize(row['messages']) for row in requests], texts=texts,
                result=semantics.result_value(expected), relation=semantic['operator_relation'],
                quality=json_metric(query.evaluate(expected)))
            with patch('transformers.AutoTokenizer.from_pretrained', return_value=SimpleNamespace(apply_chat_template=tokenize)):
                receipt = replay(path, os.environ['SEMLOOM_NATIVE_TEST_CHECKOUT'], data, 'software-tokenizer',
                                 reference, root/'replay')
            self.assertEqual(receipt['software_requests'], 4)
            self.assertEqual(receipt['model_calls'], 0)
            self.assertEqual(receipt['physical_budget_software_attempts'], 4)

    @unittest.skipUnless(os.environ.get('SEMLOOM_NATIVE_TEST_CHECKOUT'), 'requires pinned public query source')
    def test_original_movie_quick_uses_multiple_native_batches_and_one_query_owner(self):
        import lotus
        import numpy as np
        import pandas as pd
        from copy import deepcopy
        from lotus.models import LM
        from litellm.types.utils import ModelResponse
        from src.baselines.text.sembench_prepared import result_value
        from src.baselines.text.sembench_text import SemBenchTextQuery
        from src.serving.backends.vllm_protocol import GenerationSpec

        state = np.random.get_state()
        self.addCleanup(np.random.set_state, state)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pd.DataFrame(dict(id=['ant_man_and_the_wasp_quantumania'] * 8,
                reviewId=list(range(8)), reviewText=['same review'] * 8,
                originalScore=[str(i % 5 + 1) + '/5' for i in range(8)])).to_csv(root/'Reviews.csv', index=False)
            query = SemBenchTextQuery(os.environ['SEMLOOM_NATIVE_TEST_CHECKOUT'], root, 'movie', '9',
                execution_id='native-quick', model_name='openai/local-qwen2.5-7b',
                generation=GenerationSpec(max_tokens=8192, stop=()), ranking='topk')
            reference_lm = LM(query.model_name, max_tokens=8192)
            reference_batches = []
            def answer(data, options, *unused):
                reference_batches.append([deepcopy(messages) for messages, _ in data])
                return [ModelResponse(model=query.model_name, choices=[dict(index=0, finish_reason='stop',
                    message=dict(role='assistant', content='Document 1'))],
                    usage=dict(prompt_tokens=0, completion_tokens=0, total_tokens=0)) for _ in data]
            reference_lm._process_uncached_messages = answer
            with lotus.settings.context(lm=reference_lm, enable_cache=False):
                expected = query.bind_original_execution()()
            np.random.set_state(state)
            budget = dict(path=str(root/'budget.sqlite'), id='quick', limit=28, unit_id='query')
            ledger = CellBudgetLedger.create(Path(budget['path']), AttemptBudget('quick', 28), deadline_utc=None)
            ledger.reserve_unit('query', 28)
            config = dict(profile={}, ranking='topk', budget=budget,
                ray_native=dict(engine_kwargs={}, processor_options={}))
            engine = EngineSpec(model_path='CPU-no-model', model_python='unused', driver_python='unused')
            with recorded_processor(lambda row, request: 'Document 1') as sdk:
                report = driver.run_query(config, query, engine, None, root)
            self.assertEqual(report['result'], result_value(expected))
            self.assertEqual(report['physical_attempts_reserved'], 28)
            self.assertEqual([len(batch) for batch in reference_batches], [7, 6, 5, 4, 3, 2, 1])
            self.assertEqual(len(report['native']['batches']), 7)
            self.assertEqual(sdk.events.count('init'), 1)
            self.assertEqual(sdk.events.count('close'), 1)
            self.assertEqual([row['messages'] for row in sdk.requests],
                             [message for batch in reference_batches for message in batch])
            self.assertEqual(len({row['call_id'] for row in sdk.requests}), 28)
            self.assertLessEqual(report['sink_ready_ns'], report['native']['events']['close_started_ns'])

    def run_query(self, root, now, *, kind='map', transform=lambda rows: rows, close_error=None,
                  empty=False, duplicate=False, preflight_seconds=0, parser_error=False):
        query = test_sembench_prepared.PreparedSemanticsTests().query(kind)
        if duplicate:
            query.operation.frame['text'] = 'same'
            for record in query.operation.records:
                record.values['text'] = 'same'
        if empty:
            query.operation = None
            query.execute = lambda handler: (0, None)
        query.checkout = query.data_dir = root
        query.files = {}
        query.scenario, query.query, query.model_name = 'movie', '10', 'CPU-only'
        query.visual_inputs = None
        query.aggregate_task = None
        query.lm_options = {}
        from src.serving.backends.vllm_protocol import GenerationSpec
        query.generation = GenerationSpec()
        query.describe = lambda: {'expected_model_calls': 0 if empty else 2}
        query.evaluate = lambda result: {'fixture': 'quality'}
        def execute():
            def fail_parser(*args, **kwargs):
                raise ValueError('parser failed')
            value = (0 if empty else query.operation.frame.sem_map('Describe {text}',
                **(dict(postprocessor=fail_parser) if parser_error else {})).sort_index())
            now[0] += 3_000_000_000
            return value
        query.bind_original_execution = lambda: execute
        replies = lambda row, request: str(int(row['call_id'].rsplit('-', 1)[1]) + 1) if duplicate else 'answer'
        budget = dict(path=str(root/'budget.sqlite'), id='fixture', limit=2, unit_id='query')
        ledger = CellBudgetLedger.create(Path(budget['path']), AttemptBudget('fixture', 2), deadline_utc=None)
        ledger.reserve_unit('query', 2)
        config = dict(profile={}, ray_native={'engine_kwargs': {}, 'processor_options': {}}, budget=budget)
        engine = EngineSpec(model_path='unused', model_python='unused', driver_python='unused')
        def advance(phase):
            now[0] += (5 if phase == 'close' else 2)*1_000_000_000
        def preflight(*args, **kwargs):
            now[0] += preflight_seconds * 1_000_000_000
            return query
        with recorded_processor(replies, transform=transform, close_error=close_error, advance=advance) as sdk, \
                patch.object(driver, 'SemBenchTextQuery', side_effect=preflight), \
                patch.object(driver.time, 'monotonic_ns', side_effect=lambda: now[0]):
            report = driver.run_query(config, query, engine, None, root)
        return report, sdk

    def test_source_preflight_is_excluded_without_subtracting_from_old_clock(self):
        with tempfile.TemporaryDirectory() as directory:
            report, _ = self.run_query(Path(directory), [1_000_000_000], preflight_seconds=17)
        self.assertEqual(report['source_to_sink_seconds'], 5)
        self.assertEqual(report['schema'], 'semloom.sembench_text_result.v2')
        self.assertEqual(report['preflight_finished_ns'] - report['worker_entry_ns'], 17_000_000_000)
        self.assertEqual(report['query_clock_version'], 'lotus_ray.source_to_sink.v1')
        self.assertGreater(report['query_seconds'], report['source_to_sink_seconds'] + 17)

    def test_sink_excludes_audit_serialization_and_close(self):
        now = [1_000_000_000]
        digest, serialize = semantics.content_digest, semantics.result_value
        def audit(value):
            now[0] += 7_000_000_000
            return digest(value)
        def convert(value):
            now[0] += 11_000_000_000
            return serialize(value)
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(semantics, 'content_digest', side_effect=audit), \
                patch.object(semantics, 'result_value', side_effect=convert), \
                patch.object(driver, 'result_value', side_effect=convert):
            report, sdk = self.run_query(Path(directory), now)
        self.assertEqual(report['source_to_sink_seconds'], 5)
        self.assertEqual(report['physical_attempts_reserved'], 2)
        self.assertEqual(report['semantic_audit_finished_ns']-report['semantic_audit_started_ns'], 14_000_000_000)
        self.assertLessEqual(report['sink_ready_ns'], report['native']['events']['close_started_ns'])
        self.assertLessEqual(report['native']['events']['close_finished_ns'], report['evaluation_started_ns'])
        self.assertLessEqual(report['sink_ready_ns'], report['result_serialization_started_ns'])
        self.assertFalse(sdk.initialized)
        self.assertEqual(sdk.stage.map_batches_kwargs['max_task_retries'], 0)
        self.assertEqual(sdk.stage.map_batches_kwargs['max_restarts'], 0)
        self.assertEqual([row['source_position'] for row in report['semantic']['calls']], [0, 1])
        self.assertTrue(all('predictions' not in batch for batch in report['native']['batches']))
        self.assertTrue(all('text' not in call for call in report['semantic']['calls']))

    def assert_failure(self, *, patcher=None, quality=False, **kwargs):
        from contextlib import nullcontext
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with (patcher or nullcontext()), self.assertRaises(Exception):
                self.run_query(root, [1_000_000_000], **kwargs)
            report = json.loads((root/'query.json').read_text())
            native = json.loads((root/'native-predictions.json').read_text())
            self.assertEqual(report['status'], 'failed')
            self.assertEqual('quality' in report, quality)
            self.assertEqual(sum(len(batch['predictions']) for batch in native['batches']), 2)
            return report

    def test_parser_failure_has_no_sink_and_still_closes(self):
        report = self.assert_failure(parser_error=True)
        self.assertIsNone(report['source_to_sink_seconds'])
        self.assertTrue(report['native']['ray_driver_disconnected'])

    def test_post_sink_audit_failure_keeps_sink_and_raw_answers(self):
        report = self.assert_failure(patcher=patch.object(semantics, 'content_digest',
            side_effect=ValueError('audit failed')))
        self.assertEqual(report['source_to_sink_seconds'], 5)
        self.assertNotIn('semantic_audit_finished_ns', report)
        self.assertTrue(report['native']['ray_driver_disconnected'])

    def test_close_failure_does_not_erase_sink_or_claim_success(self):
        report = self.assert_failure(close_error=RuntimeError('close failed'))
        self.assertEqual(report['source_to_sink_seconds'], 5)
        self.assertEqual(report['native']['close_error_type'], 'RuntimeError')

    def test_duplicate_occurrence_rejected_before_sink(self):
        report = self.assert_failure(transform=lambda rows: [rows[0], rows[0]])
        self.assertIsNone(report['source_to_sink_seconds'])

    def test_identical_messages_keep_different_occurrence_answers_despite_reordering(self):
        with tempfile.TemporaryDirectory() as directory:
            report, sdk = self.run_query(Path(directory), [1_000_000_000], duplicate=True)
            native = json.loads((Path(directory)/'native-predictions.json').read_text())
        self.assertEqual(sdk.requests[0]['messages'], sdk.requests[1]['messages'])
        texts = {row['call_id']: row['generated_text'] for batch in native['batches'] for row in batch['predictions']}
        self.assertEqual([texts[row['call_id']] for row in report['semantic']['calls']], ['1', '2'])

    def test_missing_budget_hook_cannot_publish_success(self):
        report = self.assert_failure(patcher=patch(
            'src.experiments.shared_request_budget.SharedClaimedUnit.reserve', return_value=1))
        self.assertEqual(report['physical_attempts_reserved'], 0)
        self.assertEqual(report['source_to_sink_seconds'], 5)

    def test_parser_error_is_primary_when_close_also_fails(self):
        report = self.assert_failure(close_error=RuntimeError('close failed'), parser_error=True)
        self.assertEqual(report['error_type'], 'ValueError')
        self.assertEqual(report['native']['close_error_type'], 'RuntimeError')

    def test_budget_close_failure_still_preserves_raw_predictions(self):
        report = self.assert_failure(quality=True, patcher=patch.object(CellBudgetLedger, 'close_shared_unit',
            side_effect=RuntimeError('budget close failed')))
        self.assertEqual(report['budget_close_error_type'], 'RuntimeError')
        self.assertEqual(report['source_to_sink_seconds'], 5)

    def test_empty_input_runs_original_sink_without_ray_or_budget_consumption(self):
        with tempfile.TemporaryDirectory() as directory:
            report, sdk = self.run_query(Path(directory), [1_000_000_000], empty=True)
        self.assertEqual(report['result'], 0)
        self.assertEqual(report['source_to_sink_seconds'], 3)
        self.assertEqual(report['physical_attempts_reserved'], 0)
        self.assertEqual(sdk.events, [])

    def test_generate_budget_precedes_original_native_call_and_preserves_terminal(self):
        from src.baselines.text.frameworks.ray_native_observer import install
        events, invoked = [], []
        class Engine:
            async def generate(self, prompt, sampling_params, request_id):
                invoked.append(request_id)
                yield SimpleNamespace(finished=True, prompt_token_ids=[1, 2], outputs=[])
        async def run():
            engine = Engine()
            return [value async for value in engine.generate({}, SimpleNamespace(), 'same')]
        with tempfile.TemporaryDirectory() as directory:
            ledger = CellBudgetLedger.create(Path(directory)/'ledger', AttemptBudget('one', 1), deadline_utc=None)
            ledger.reserve_unit('query', 1)
            budget = ledger.claim_shared_unit('query')
            install(Engine, budget, lambda event, **data: events.append(event), lambda *args: None)
            self.assertEqual(len(asyncio.run(run())), 1)
            with self.assertRaises(BudgetExhausted):
                asyncio.run(run())
            self.assertEqual(invoked, ['same'])
            self.assertIn('native_generate_terminal', events)


if __name__ == '__main__':
    unittest.main()
