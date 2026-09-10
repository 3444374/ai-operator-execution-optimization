"""Exercise sampling and the real cell lifecycle with bounded, zero-network HTTP."""
import asyncio
import json
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
