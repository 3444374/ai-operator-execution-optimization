"""Exercise sampling and the real cell lifecycle with bounded, zero-network HTTP."""
import asyncio
import json
import os
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.baselines.text.squad_capacity import prepare_capacity_samples
from src.baselines.text.squad_map import validate_manifest
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.postgresql.map_capacity_runner import CellConfig, run_cell


class Tokenizer:
    def apply_chat_template(self, messages, **_):
        return list(range(len(messages[-1]['content'])))


def examples():
    return [SimpleNamespace(source_example_id=str(i), context=('x' * (i//3+1)), question=str(i),
                            reference_answers=['correct']) for i in range(90)]


class CapacityTests(unittest.TestCase):
    def test_gateway_launch_supplies_source_path_without_caller_pythonpath(self):
        from unittest.mock import Mock
        from src.experiments.postgresql.cell_evidence import CellErrors
        from src.experiments.postgresql.map_capacity_runner import _run_pg
        module = 'src.experiments.postgresql.map_capacity_runner.'
        config = CellConfig('cell', 'pg', 'tuning', 4, 1, 2, 4, 4*1048576, 4*1048576, 8388608)
        with tempfile.TemporaryDirectory(dir='/tmp') as directory, patch.dict(os.environ):
            os.environ.pop('PYTHONPATH', None)
            root = Path(directory)
            with patch(module + '_prepare_pg', return_value='SELECT 1'), \
                 patch(module + 'owned_child_process', side_effect=RuntimeError('captured launch')) as launch:
                _run_pg(config, [], None, SimpleNamespace(execute=Mock()), root / 'pg.log', root / 'model',
                        root / 'budget', AttemptBudget('fixture', 4), root, CellErrors())
            environment = launch.call_args.args[3]
            self.assertEqual(environment['PYTHONPATH'].split(os.pathsep)[0], str(Path(__file__).resolve().parents[2]))

    def test_partial_summary_does_not_hide_independent_cell_evidence(self):
        from src.experiments.postgresql.cell_evidence import collect_cell_evidence
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'q0').mkdir()
            (root / 'q0/execution.json').write_text('{partial')
            (root / 'observer.json').write_text('{"observed_attempts": 3}')
            result = collect_cell_evidence(root, SimpleNamespace(queries=1))
        self.assertIsNone(result['queries'][0]['execution'])
        self.assertEqual(result['observer']['observed_attempts'], 3)
        self.assertEqual(result['resource_state'], 'unknown')
        self.assertIn('q0/execution.json', result['read_errors'])

    def test_sql_error_precedes_producer_capture_and_table_cleanup_failures(self):
        from contextlib import nullcontext
        from unittest.mock import Mock
        from src.experiments.postgresql.cell_evidence import CellErrors
        from src.experiments.postgresql.map_capacity_runner import _run_pg

        query_error = ValueError("original SQL error")
        connection = SimpleNamespace(info=SimpleNamespace(backend_pid=1), execute=Mock())
        connection.execute.side_effect = [None, RuntimeError("table cleanup")]
        config = CellConfig('cell', 'pg', 'tuning', 12, 1, 3, 3, 3*1048576, 3*1048576, 8388608)
        errors = CellErrors()
        module = 'src.experiments.postgresql.map_capacity_runner.'
        with tempfile.TemporaryDirectory(dir='/tmp') as directory:
            root = Path(directory)
            log = root / 'pg.log'
            log.write_text('')
            with patch(module + '_prepare_pg', return_value='SELECT fixture'), \
                 patch(module + 'owned_child_process', return_value=nullcontext(SimpleNamespace(pid=1))), \
                 patch(module + 'wait_for_path'), \
                 patch(module + 'ProcessSampler', return_value=nullcontext(SimpleNamespace(phase=None))), \
                 patch(module + 'record_pg_query', side_effect=query_error), \
                 patch(module + '_capture_producer', side_effect=OSError('producer capture')):
                _run_pg(config, [], None, connection, log, root / 'model', root / 'budget',
                        AttemptBudget('fixture', 12), root, errors)
        self.assertIs(errors.first, query_error)
        self.assertEqual(errors.details['query']['type'], 'ValueError')
        self.assertEqual(errors.details['producer_capture']['type'], 'OSError')
        self.assertEqual(errors.details['table_cleanup']['type'], 'RuntimeError')

    def prepared(self):
        return prepare_capacity_samples(examples(), 'a'*64, Tokenizer(), rows_per_split=12,
                                        seed=33, context_limit=4096, tokenizer_identity={'fixture': True})

    def test_sampling_is_deterministic_context_isolated_and_answer_independent(self):
        first = self.prepared()
        self.assertEqual(first, self.prepared())
        for manifest in first['manifests'].values():
            validate_manifest(manifest)
        changed = examples()
        for row in changed:
            row.reference_answers = ['a different answer']
        second = prepare_capacity_samples(changed, 'a'*64, Tokenizer(), rows_per_split=12,
                                           seed=33, context_limit=4096, tokenizer_identity={'fixture': True})
        for name in ('natural', 'stratified'):
            for split in ('tuning', 'evaluation'):
                ids = lambda value: [r['source_example_id'] for r in value['manifests'][name]['splits'][split]]
                self.assertEqual(ids(first), ids(second))
        self.assertEqual(len(first['profile']), 90)

    def test_exclusions_are_explicit_without_truncation(self):
        result = prepare_capacity_samples(examples(), 'b'*64, Tokenizer(), rows_per_split=3,
                    seed=1, context_limit=110, tokenizer_identity={})
        self.assertGreater(result['excluded_context_rows'], 0)
        self.assertEqual(result['excluded_context_rows'], sum(not r['fits_context'] for r in result['profile']))

    def test_pg_prepare_and_cleanup_errors_preserve_cell_evidence(self):
        connection = SimpleNamespace(autocommit=True, closed=False,
                                     info=SimpleNamespace(transaction_status=0))
        def execute(sql):
            if sql.startswith('DROP'):
                raise RuntimeError('cleanup failed')
        connection.execute = execute
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            model = root / 'model.json'
            model.write_text(json.dumps(dict(endpoint_url='http://localhost/fixture', model_id='fixture', timeout_ms=1000)))
            budget = AttemptBudget('fixture', 12)
            ledger = CellBudgetLedger.create(root / 'budget.sqlite', budget, deadline_utc=time.time()+60)
            config = CellConfig('cell', 'pg', 'tuning', 12, 1, 3, 3, 3*1048576, 3*1048576, 8388608)
            with patch('src.experiments.postgresql.map_capacity_runner._prepare_pg', side_effect=ValueError('prepare failed')):
                with self.assertRaisesRegex(ValueError, 'prepare failed'):
                    run_cell(config, manifest=self.prepared()['manifests']['natural'], fixed_model_file=model,
                             budget_file=ledger.path, budget=budget, root=root / 'cell', connection=connection,
                             pg_log=root / 'pg.log')
            result = json.loads((root / 'cell' / 'summary.json').read_text())
            self.assertEqual(result['status'], 'failed')
            self.assertEqual(result['errors']['pg']['type'], 'ValueError')
            self.assertEqual(result['errors']['table_cleanup']['type'], 'RuntimeError')
            self.assertEqual(result['evidence']['resource_state'], 'unknown')
            self.assertEqual(ledger.snapshot()['allocated_requests'], 12)

    def test_capacity_api_rejects_callers_transaction_before_mutation(self):
        from unittest.mock import Mock
        connection = SimpleNamespace(autocommit=False, closed=False, execute=Mock())
        config = CellConfig('cell', 'pg', 'tuning', 12, 1, 3, 3, 3*1048576, 3*1048576, 8388608)
        with self.assertRaisesRegex(ValueError, 'autocommit'):
            run_cell(config, manifest=self.prepared()['manifests']['natural'], fixed_model_file=None,
                     budget_file=None, budget=None, root=None, connection=connection, pg_log=Path('unused'))
        connection.execute.assert_not_called()

    def test_direct_cell_reuses_client_bounds_intake_and_preserves_wrong_answers(self):
        import httpx
        original = httpx.AsyncClient
        class ResponseStream(httpx.AsyncByteStream):
            def __init__(self, value):
                self.payload = json.dumps(value).encode()
            async def __aiter__(self):
                yield self.payload
        manifest = self.prepared()['manifests']['natural']
        active = peak = calls = clients = 0
        async def respond(request):
            nonlocal active, peak, calls
            active += 1
            peak = max(peak, active)
            calls += 1
            await asyncio.sleep(.002)
            body = json.loads(request.content)
            active -= 1
            return httpx.Response(200, stream=ResponseStream(dict(model='fixture', choices=[dict(
                message={'content': 'wrong but valid'}, finish_reason='stop')],
                usage=dict(prompt_tokens=len(body['messages'][-1]['content']), completion_tokens=3))))
        class Client(original):
            def __init__(self, **kw):
                nonlocal clients
                clients += 1
                super().__init__(transport=httpx.MockTransport(respond), **kw)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            model = root / 'model.json'
            model.write_text(json.dumps(dict(endpoint_url='http://localhost/fixture', model_id='fixture', timeout_ms=1000)))
            budget = AttemptBudget('fixture', 24)
            ledger = CellBudgetLedger.create(root / 'budget.sqlite', budget, deadline_utc=time.time()+60)
            config = CellConfig('cell', 'direct', 'tuning', 12, 2, 3, 3, 3*1048576, 3*1048576, 8388608)
            with patch('httpx.AsyncClient', Client):
                report = run_cell(config, manifest=manifest, fixed_model_file=model,
                    budget_file=ledger.path, budget=budget, root=root / 'cell')
            self.assertEqual((calls, clients, peak, active), (24, 1, 3, 0))
            self.assertEqual(report['status'], 'passed')
            self.assertEqual(report['queries'][0]['evaluation']['result']['exact_match_percent'], 0)
            self.assertEqual(report['resource_accounting']['http_peak'], 3)
            self.assertEqual(ledger.snapshot()['allocated_requests'], 24)


if __name__ == '__main__':
    unittest.main()
