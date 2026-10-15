"""Native configuration and optional pinned SDK/localhost software qualification."""
from contextlib import contextmanager, nullcontext
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from io import StringIO
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock, patch

from src.baselines.common.private_artifacts import write_private_json
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.buffered_events import BufferedEvents
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.palimpzest.ledger import PalimpzestLedger
from src.experiments.palimpzest.model import LocalModelBinding, fetch_model_metadata
from src.experiments.palimpzest.native import (
    NativeOptions, PZ_RUNTIME_VERSIONS, _closed_events, _private_working_directory,
    execute_native, main, movie_quality, run, verify_libraries,
)
from src.experiments.palimpzest.source import PALIMPZEST_COMMIT, source_identity, verify_palimpzest_checkout
from src.experiments.postgresql.query_workloads import prepare
from src.experiments.postgresql.sembench_queries import MovieDataset
from src.infrastructure.runner_lease import HOST_SCOPE_FINGERPRINT, acquire_runner_lease
from src.semantic_methods.palimpzest.backend import PalimpzestAdapterError


class PZNativeTests(unittest.TestCase):
    def test_private_adapter_rejects_a_different_runtime_before_execution(self):
        versions = dict(PZ_RUNTIME_VERSIONS, pydantic='recorded', httpx='recorded')
        with patch('src.experiments.palimpzest.native.importlib.metadata.version', side_effect=versions.__getitem__):
            self.assertEqual(verify_libraries(), versions)
        versions['openai'] = 'unknown'
        with patch('src.experiments.palimpzest.native.importlib.metadata.version', side_effect=versions.__getitem__):
            with self.assertRaisesRegex(ValueError, 'openai version differs'):
                verify_libraries()

    def test_native_parallel_controls_are_explicit_and_not_silently_strengthened(self):
        selected = NativeOptions(max_workers=1, join_parallelism=2, batch_size=7)
        self.assertEqual((selected.max_workers, selected.join_parallelism, selected.batch_size), (1, 2, 7))
        for values in ({'max_workers': 0}, {'max_workers': True}, {'join_parallelism': 257}, {'batch_size': 0}):
            with self.assertRaises(ValueError):
                NativeOptions(**values)

    def test_different_or_modified_palimpzest_checkout_is_refused_before_import(self):
        for outputs in ((PALIMPZEST_COMMIT+'\n', ' M src/palimpzest/constants.py'), ('0'*40, ''),
                        (PALIMPZEST_COMMIT, '?? src/palimpzest/hidden_override.py')):
            with patch('src.experiments.palimpzest.source.subprocess.check_output', side_effect=outputs):
                with self.assertRaisesRegex(ValueError, 'pinned source'):
                    verify_palimpzest_checkout('/tmp/pz-source-fixture')

    def test_cleanup_failure_preserves_original_query_error_and_fails_a_successful_run(self):
        first, closing = ValueError('primary query fixture'), RuntimeError('writer fixture')
        events = Mock()
        events.close.side_effect = closing
        with self.assertRaises(ValueError) as caught:
            with _closed_events(events):
                raise first
        self.assertIs(caught.exception, first)
        self.assertIn('PZ event cleanup also failed: RuntimeError', first.__notes__)
        with self.assertRaises(RuntimeError) as caught:
            with _closed_events(events):
                pass
        self.assertIs(caught.exception, closing)

    def test_invalid_prediction_cli_status_is_not_a_successful_qualification_exit(self):
        with patch('src.experiments.palimpzest.native.load_config', return_value={}), \
             patch('src.experiments.palimpzest.native.run', return_value={'status': 'completed_with_invalid_predictions'}), \
             patch('sys.stdout', StringIO()):
            self.assertEqual(main(['--config', 'fixture.json', '--execute']), 2)

    def test_metadata_failure_retains_private_evidence_zero_posts_and_releases_lease(self):
        for failure in (ValueError('metadata model differs'), TimeoutError('metadata GET unavailable')):
            with self.subTest(failure=type(failure).__name__), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                output, host_scope = root/'output', root/'.host-runner-scope'
                binding = LocalModelBinding(FixedModelConfig('http://127.0.0.1:8000/v1/chat/completions',
                                                            'fixture', 1000))
                config = dict(query_id=3, max_attempts=2, query_timeout_s=10, native_options={})
                prepared = (None, [], root/'pz', binding, output, host_scope, {'status': 'passed'})
                def metadata(_binding):
                    for name in ('config.json', 'preflight.json', 'started.json', 'initial-generator-ledger.json'):
                        self.assertTrue((output/name).is_file(), name)
                    self.assertTrue((host_scope/'.runner-lease.json').exists())
                    raise failure
                with patch('src.experiments.palimpzest.native.preflight', return_value=prepared), \
                     patch('src.experiments.palimpzest.native.subprocess.check_output', return_value='f'*40), \
                     patch('src.experiments.palimpzest.native.fetch_model_metadata', side_effect=metadata), \
                     patch('src.experiments.palimpzest.native.execute_native') as execute, \
                     patch('src.experiments.palimpzest.native.acquire_runner_lease', wraps=acquire_runner_lease) as lease:
                    with self.assertRaises(type(failure)) as caught:
                        run(config)
                self.assertIs(caught.exception, failure)
                execute.assert_not_called()
                self.assertFalse(lease.call_args.kwargs['recover_stale'])
                self.assertFalse((host_scope/'.runner-lease.json').exists())
                summary = json.loads((output/'summary.json').read_text())
                self.assertEqual(summary['status'], 'failed')
                self.assertEqual(summary['actual_posts'], 0)
                self.assertEqual(summary['completion_ledger']['completion_submitted'], 0)
                self.assertEqual(json.loads((output/'generator-calls.json').read_text()), [])
                self.assertEqual(json.loads((output/'config.json').read_text()), config)

    def test_busy_lease_does_not_create_output_or_fetch_metadata(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            output, host_scope = root/'output', root/'.host-runner-scope'
            prepared = (None, [], None, None, output, host_scope, {})
            with acquire_runner_lease(host_scope, config_fingerprint=HOST_SCOPE_FINGERPRINT,
                                      repository_commit='f'*40, recover_stale=False):
                with patch('src.experiments.palimpzest.native.preflight', return_value=prepared), \
                     patch('src.experiments.palimpzest.native.subprocess.check_output', return_value='f'*40), \
                     patch('src.experiments.palimpzest.native.fetch_model_metadata') as metadata:
                    with self.assertRaisesRegex(RuntimeError, 'active runner'):
                        run({})
                metadata.assert_not_called()
                self.assertFalse(output.exists())


@contextmanager
def local_movie_server(*, response_model='fixture', status=200):
    """Explicit deterministic response fixture; it performs no inference."""
    requests, lock = [], threading.Lock()
    class Handler(BaseHTTPRequestHandler):
        def reply(self, value, response_status=200):
            body = json.dumps(value).encode()
            self.send_response(response_status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        def do_GET(self):
            if self.path != '/v1/models':
                self.send_error(404)
                return
            self.reply(dict(object='list', data=[dict(id='fixture', object='model', owned_by='vllm')]))
        def do_POST(self):
            value = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            with lock:
                requests.append(value)
            if status != 200:
                self.reply(dict(error=dict(message='ordinary provider fixture unavailable', type='server_error')), status)
                return
            text = 'True' if 'fixture-positive' in str(value['messages']) else 'False'
            self.reply(dict(id='fixture', object='chat.completion', created=1, model=response_model,
                choices=[dict(index=0, message=dict(role='assistant', content=text), finish_reason='stop')],
                usage=dict(prompt_tokens=2, completion_tokens=1, total_tokens=3)))
        def log_message(self, *_args):
            pass
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        yield 'http://127.0.0.1:'+str(server.server_port)+'/v1/chat/completions', requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join(5)
        if thread.is_alive():
            raise RuntimeError('local Movie fixture did not stop')


@unittest.skipUnless(os.environ.get('SEMLOOM_PZ_LIBRARY_TEST') == '1',
                     'requires explicit pinned PZ/SDK environment; local HTTP fixture only')
class PZNativeLibraryTests(unittest.TestCase):
    def test_native_response_identity_failure_stops_queued_posts_but_503_keeps_original_defaults(self):
        pz_root = Path(os.environ['SEMLOOM_TEST_PALIMPZEST_ROOT'])
        bench = Path(os.environ['SEMLOOM_TEST_SEMBENCH_ROOT'])
        retained = os.environ.get('SEMLOOM_PZ_FIXTURE_OUTPUT')
        retained = retained+'-identity-guard' if retained else None
        scope = nullcontext(retained) if retained else tempfile.TemporaryDirectory()
        with scope as temporary:
            root = Path(temporary)
            if retained:
                root.mkdir(parents=True, exist_ok=False)
            manifest = prepare(root/'data', 'movie', [
                ('movie-row-'+str(i), 'taken_3', 'fixture-positive-'+str(i), 'POSITIVE', 'review-'+str(i))
                for i in range(2)], {'source': 'synthetic SemBench software fixture'}, max_rows=2)
            dataset = MovieDataset(bench, root/'data/manifest.json', expected_manifest_digest=manifest['sha256'],
                                   role='software_fixture')
            rows = dataset.load_rows()
            for name, response_model, status in (('wrong-model', 'unknown-response-model', 200),
                                                  ('provider-503', 'fixture', 503)):
                output = root/name
                output.mkdir()
                cell = CellBudgetLedger.create(output/'budget.sqlite', AttemptBudget('identity-fixture', 2),
                                               deadline_utc=time.time()+60)
                cell.reserve_unit('query', 2)
                budget = cell.claim_unit('query')
                ledger, execution = PalimpzestLedger(max_calls=2), {}
                with local_movie_server(response_model=response_model, status=status) as (endpoint, requests), \
                        BufferedEvents(output/'events.jsonl') as events:
                    binding = LocalModelBinding(FixedModelConfig(endpoint, 'fixture', 10000))
                    verified = fetch_model_metadata(binding)
                    ledger.record = events.record
                    with _private_working_directory(output), patch('sys.stdout', StringIO()):
                        if name == 'wrong-model':
                            with self.assertRaisesRegex(PalimpzestAdapterError, 'response model differs'):
                                execute_native(dataset, rows, pz_root, binding, verified, NativeOptions(1, 1),
                                    query_id=3, budget=budget, events=events, timeout_s=30,
                                    ledger=ledger, execution=execution)
                        else:
                            execute_native(dataset, rows, pz_root, binding, verified, NativeOptions(1, 1),
                                query_id=3, budget=budget, events=events, timeout_s=30,
                                ledger=ledger, execution=execution)
                    write_private_json(output/'evidence.json', dict(source=source_identity(pz_root, bench, 3),
                        fixture_posts=len(requests), model_inference_requests=0, execution=execution,
                        calls=ledger.calls()))
                    self.assertEqual(budget.attempts, len(requests))
                    self.assertTrue(execution['bridge']['patches_restored'])
                    self.assertEqual(execution['bridge']['active_generators'], 0)
                    if name == 'wrong-model':
                        self.assertEqual(len(requests), 1)
                        self.assertEqual(execution['bridge']['fatal_error_type'], 'PalimpzestAdapterError')
                    else:
                        self.assertEqual(len(requests), 2)
                        self.assertIsNone(execution['bridge']['fatal_error_type'])
                        self.assertEqual(ledger.snapshot()['completion_failed'], 2)
                        self.assertTrue(all(call['native_passed_operator'] is False for call in ledger.calls()))

    def test_original_q3_full_filter_count_and_q2_native_limit_keep_occurrences(self):
        from tests.experiments.test_sembench_queries import fixture_dataset
        pz_root = Path(os.environ['SEMLOOM_TEST_PALIMPZEST_ROOT'])
        bench = Path(os.environ['SEMLOOM_TEST_SEMBENCH_ROOT'])
        retained = os.environ.get('SEMLOOM_PZ_FIXTURE_OUTPUT')
        scope = nullcontext(retained) if retained else tempfile.TemporaryDirectory()
        with scope as temporary, patch.dict(os.environ, {'LITELLM_LOCAL_MODEL_COST_MAP': 'True',
                                                        'HF_HUB_OFFLINE': '1', 'TRANSFORMERS_OFFLINE': '1'}):
            root = Path(temporary)
            if retained:
                root.mkdir(parents=True, exist_ok=False)
            dataset = fixture_dataset(root/'data', bench)
            rows = dataset.load_rows()
            for query_id in (3, 2):
                output = root/str(query_id)
                output.mkdir()
                cell = CellBudgetLedger.create(output/'budget.sqlite', AttemptBudget('pz-fixture', 8),
                                               deadline_utc=time.time()+60)
                cell.reserve_unit('query', 8)
                budget = cell.claim_unit('query')
                with local_movie_server() as (endpoint, requests), BufferedEvents(output/'events.jsonl') as events:
                    binding = LocalModelBinding(FixedModelConfig(endpoint, 'fixture', 10000))
                    verified = fetch_model_metadata(binding)
                    with _private_working_directory(output):
                        result, ledger, evidence = execute_native(dataset, rows, pz_root, binding, verified,
                            NativeOptions(4, 4), query_id=query_id, budget=budget, events=events, timeout_s=30)
                    write_private_json(output/'execution.json', evidence)
                    write_private_json(output/'generator-calls.json', ledger.calls())
                    write_private_json(output/'result.json', json.loads(result.to_df().to_json(orient='split')))
                    metric = movie_quality(dataset, rows, query_id, result, ledger)
                    write_private_json(output/'quality.json', metric)
                    self.assertTrue(all(request['model'] == 'fixture' for request in requests))
                    self.assertTrue(all('scoreSentiment' not in str(request['messages']) for request in requests))
                    self.assertEqual(budget.attempts, len(requests))
                    self.assertEqual(evidence['ledger']['completion_failed'], 0)
                    self.assertTrue(evidence['bridge']['patches_restored'])
                    self.assertEqual(evidence['effective_config']['execution_strategy']['type'], 'ParallelExecutionStrategy')
                    if query_id == 3:
                        self.assertGreaterEqual(len(requests), 6)
                        self.assertLessEqual(len(requests), 8)
                        self.assertEqual(int(result.to_df().iloc[0, 0]), 5)
                        self.assertEqual(metric['classification_audit']['false_positive'], 1)
                        self.assertEqual(metric['classification_audit']['false_negative'], 1)
                    else:
                        self.assertGreaterEqual(len(requests), 1)
                        self.assertLessEqual(len(requests), 8)
                        self.assertEqual(evidence['effective_config']['batch_size'], 5)
                        self.assertLessEqual(len(result.to_df()), 5)
