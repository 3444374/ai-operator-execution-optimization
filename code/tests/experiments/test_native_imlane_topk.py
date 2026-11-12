"""Original quick/query plus Arrow/provider receipts; SQL and model are fixtures."""
import csv
import json
import os
from pathlib import Path
import socket
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import pyarrow as pa
from transformers import AutoTokenizer

from integrations.imlane_duckdb import model_provider, sembench_udf
from src.experiments import native_imlane_sembench as entry, native_imlane_topk as topk
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.shared_request_budget import SharedClaimedUnit
from src.infrastructure.private_artifacts import content_digest, file_identity
from tests.baselines.text.test_imlane_duckdb_model_provider import SoftwareSocket


CHECKOUT = os.environ.get('SEMLOOM_NATIVE_TEST_CHECKOUT')


@unittest.skipUnless(CHECKOUT and Path(CHECKOUT).is_dir(), 'requires pinned original SemBench checkout')
class NativePredictionTopKTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='imlane-topk-', dir='/tmp')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        state = np.random.get_state()
        self.addCleanup(np.random.set_state, state)
        np.random.seed(2)  # Fixture only; production retains the original RNG.
        model_provider._configuration.cache_clear()
        self.addCleanup(model_provider._configuration.cache_clear)
        connect = socket.socket.connect

        def local_only(sock, address):
            if sock.family in (socket.AF_INET, socket.AF_INET6):
                self.fail('software validation must not access an external service')
            return connect(sock, address)

        scope = patch.object(socket.socket, 'connect', local_only)
        scope.start()
        self.addCleanup(scope.stop)

    def config(self, count):
        # Equal messages and duplicate business IDs must retain distinct occurrences.
        pd.DataFrame(dict(reviewId=['duplicate'] * count,
            id=['ant_man_and_the_wasp_quantumania'] * count, reviewText=['same review'] * count,
            originalScore=[str(i + 1) + '/5' for i in range(count)])).to_csv(self.root / 'Reviews.csv', index=False)
        value = json.loads((entry.CODE / 'configs/sembench_imlane_topk.example.json').read_text())
        value.update(schema=entry.TOPK_SCHEMA, query_id='software-topk', sembench_checkout=CHECKOUT,
            data_dir=str(self.root), input_sha256={'Reviews.csv': file_identity(self.root / 'Reviews.csv')['sha256']},
            task=dict(scenario='movie', query='9'), max_model_calls=max(1, count * (count - 1) // 2))
        return value

    def run_fixture(self, count=5, *, audit_failure=False, sql_failure=False, close_failure=False):
        config = self.config(count)
        args = SimpleNamespace(output=self.root / 'worker', build_record=self.root / 'build.json',
                               kind='thread', threads=None, executor_count=None)
        args.build_record.write_text('{}')
        build = dict(binary_path='/unused/driver', kernel_path='/unused/kernel',
                     duckdb_library_path='/unused/libduckdb.so')
        events, services, raw_outputs = [], [], []
        clock = [time.monotonic_ns()]
        test = self

        class Service:
            def __init__(self, cfg, directory, **kwargs):
                services.append(self)
                self.unit = SharedClaimedUnit(Path(cfg['request_budget']['path']),
                    AttemptBudget(cfg['request_budget']['budget_id'], cfg['request_budget']['limit']), 'query')
                self.socket_path, self.served_model_name = test.root / 'fake.sock', 'fixture'
                self.ready_info, self.report = dict(max_model_len=30000), dict(status='starting')
                self.closed = False
            def __enter__(self):
                self.report['started_monotonic'] = clock[0] / 1e9
                events.append('model-start')
                clock[0] += 90_000_000_000
                events.append('model-ready')
                return self
            def check(self): pass
            def begin_cleanup(self, _deadline): pass
            def __exit__(self, *_args): self.close()
            def close(self, **kwargs):
                if self.closed: return
                self.closed = True
                events.append('model-close')
                clock[0] += 5_000_000_000
                if close_failure: raise RuntimeError('original model close failed')
                self.report['status'] = 'closed'

        def reply(request):
            calls = []
            for position, message in enumerate(request['messages_batch']):
                attempt = services[0].unit.reserve(content_digest(message))
                text = ('Neither', 'Document 2', 'Document 1')[attempt % 3]
                raw_outputs.append(text)
                calls.append(dict(position=position, attempted=True, status='succeeded',
                    generated_text=text, request_id='fixture-' + str(attempt), budget_attempt=attempt,
                    requested_max_tokens=request['sampling_params']['max_tokens'],
                    resolved_max_tokens=request['sampling_params']['max_tokens'],
                    rendered_prompt_token_ids=[7, 8], finish_reason='stop',
                    usage=dict(prompt_tokens=2, completion_tokens=1, total_tokens=3)))
            return dict(schema=request['schema'], batch_id=request['batch_id'],
                        model=request['model'], status='succeeded', calls=calls)

        def sql(stage_args, stage_report, **kwargs):
            events.append('sql-open')
            test.assertEqual(len(services), 1)
            test.assertFalse(services[0].closed)
            test.assertFalse((args.output / 'result-ready.json').exists())
            with (stage_args.output / 'input.csv').open() as stream:
                records = list(csv.DictReader(stream))
            table = pa.table({'c0': [row['call_id'] for row in records],
                              'c1': [row['messages_json'] for row in records]})
            with patch.dict(os.environ, stage_args.workload_environment), \
                    patch.object(model_provider.socket, 'socket', side_effect=lambda *a: SoftwareSocket(reply)):
                predictions = sembench_udf.infer_prepared(table).to_pylist()
            predictions = [dict(call_id=row['call_id'], **prediction)
                for row, prediction in zip(records, predictions, strict=True)]
            # Native SQL relation order is not a request association key.
            native = dict(return_type='VARCHAR', full_projection_materialized=True,
                variant=entry.VARIANT, source_rows=len(records),
                converter_resolved_DSO=build['duckdb_library_path'], mapped_occurrences=list(reversed(predictions)))
            clock[0] += 5_000_000_000
            result = kwargs['sink'](native)
            if sql_failure:
                error = RuntimeError('prediction SQL close failed')
                error.query_result = result
                raise error
            events.append('sql-close')
            clock[0] += 5_000_000_000
            stage_report['status'] = 'completed'
            return result, None

        tokenizer = Mock()
        def tokenize(*_args, **kwargs):
            events.append('token-audit')
            test.assertTrue((args.output / 'result-ready.json').exists())
            clock[0] += 2_000_000_000
            return [7, 8]
        tokenizer.apply_chat_template.side_effect = tokenize
        original_audit = topk.audit_topk
        def audit(*values):
            events.append('ranking-audit')
            self.assertTrue((args.output / 'result-ready.json').exists())
            if audit_failure: raise ValueError('original post-sink audit failed')
            return original_audit(*values)
        native_window = entry._ResultWindow
        fake_time = SimpleNamespace(monotonic_ns=lambda: clock[0], monotonic=lambda: clock[0] / 1e9,
                                    time=time.time)
        with patch.object(entry, 'verified_build', return_value=build), \
                patch.object(AutoTokenizer, 'from_pretrained', return_value=tokenizer), \
                patch.object(entry, 'time', fake_time), patch.object(topk, 'time', fake_time), \
                patch.object(entry, '_ResultWindow', side_effect=lambda *a, **k:
                    native_window(*a, clock=fake_time.monotonic, **k)), \
                patch.object(topk, 'run_relation_query', side_effect=sql), \
                patch.object(topk, 'audit_topk', side_effect=audit):
            error = None
            try: report = entry.run_worker(args, config, service_factory=Service)
            except BaseException as caught:
                error = caught
                report = json.loads((args.output / 'author.json').read_text())
        return report, error, events, services, raw_outputs, args.output

    def test_original_full_query_keeps_occurrences_replies_and_batch_close_in_its_clock(self):
        report, error, events, services, outputs, output = self.run_fixture()
        self.assertIsNone(error)
        self.assertEqual(len(services), 1)
        self.assertEqual(events.count('model-close'), 1)
        self.assertEqual(report['model_startup_seconds'], 90)
        self.assertEqual(report['ready_source_to_sink_seconds'], 10 * events.count('sql-close'))
        self.assertEqual(report['cold_worker_entry_to_sink_seconds'], 90 + report['ready_source_to_sink_seconds'])
        self.assertLess(events.index('sql-close'), events.index('token-audit'))
        self.assertFalse(report['native_LM_execution_accounting_used'])
        self.assertIsNone(report['cost'])
        self.assertFalse(report['cost_available'])
        self.assertIn('Neither', outputs)  # Original parser fallback is replayed unchanged.
        self.assertEqual(report['model_calls'], len(outputs))
        self.assertEqual(report['native_stats']['total_llm_calls'], len(outputs))
        self.assertEqual(len(report['topk']['comparison_occurrences']), len(outputs))
        self.assertEqual(len({row['call_id'] for row in report['topk']['comparison_occurrences']}), len(outputs))
        self.assertEqual(report['topk']['rng_state'][0], 'MT19937')
        self.assertEqual(report['budget']['consumed_requests'], len(outputs))
        self.assertEqual(report['native_API_usage']['total_tokens'], 3 * len(outputs))
        self.assertEqual(len(report['operator_relation']['data']), 5)
        saved = json.loads((output / 'full-sink-observed.json').read_text())
        self.assertTrue(saved['frames'])

    def test_zero_or_one_row_has_no_model_or_prediction_sql(self):
        for count in (0, 1):
            with self.subTest(count=count):
                report, error, events, services, outputs, _ = self.run_fixture(count)
                self.assertIsNone(error)
                self.assertEqual(services, [])
                self.assertNotIn('sql-open', events)
                self.assertEqual(outputs, [])
                self.assertEqual(report['budget']['consumed_requests'], 0)
                self.assertIsNone(report['model_startup_seconds'])
                # Use a new output directory for each independent worker.
                (self.root / 'worker').rename(self.root / ('worker-' + str(count)))

    def test_post_sink_audit_failure_retains_full_result_without_promoting_success(self):
        report, error, _, services, _, output = self.run_fixture(audit_failure=True)
        self.assertIsInstance(error, ValueError)
        self.assertEqual(report['status'], 'failed')
        self.assertTrue((output / 'full-sink-observed.json').exists())
        self.assertTrue(services[0].closed)

    def test_prediction_batch_failure_is_not_a_complete_frontend_result(self):
        report, error, events, services, _, output = self.run_fixture(sql_failure=True)
        self.assertIsInstance(error, RuntimeError)
        self.assertEqual(str(error), 'prediction SQL close failed')
        self.assertIsNotNone(error.native_prediction_result)
        self.assertIsNone(error.query_result)
        self.assertFalse((output / 'full-sink-observed.json').exists())
        self.assertNotIn('ranking-audit', events)
        self.assertTrue(services[0].closed)

    def test_model_close_failure_preserves_full_sink_and_failed_status(self):
        report, error, _, _, _, output = self.run_fixture(close_failure=True)
        self.assertIsInstance(error, RuntimeError)
        self.assertEqual(str(error), 'original model close failed')
        self.assertEqual(report['status'], 'failed')
        self.assertTrue((output / 'full-sink-observed.json').exists())

    def test_bound_and_original_task_are_checked_before_any_service(self):
        value = self.config(5)
        config, generation = entry.configuration(value)
        for limit in (5, 9, 11):
            with self.subTest(limit=limit), self.assertRaisesRegex(ValueError, 'comparison upper bound'):
                topk.prepare_query(dict(config, max_model_calls=limit), generation)
        with self.assertRaisesRegex(ValueError, 'ranking=map/topk'):
            topk.prepare_query(dict(config, task=dict(scenario='movie', query='8')), generation)
        with self.assertRaisesRegex(ValueError, 'frozen identities'):
            topk.prepare_query(dict(config, input_sha256={'Reviews.csv': '0' * 64}), generation)


if __name__ == '__main__':
    unittest.main()
