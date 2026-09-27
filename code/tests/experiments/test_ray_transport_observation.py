"""Remote dispatch spends the same durable quota and reports actual object ownership."""

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
import json
import tempfile
import time
import unittest
from unittest.mock import patch

from src.experiments.attempt_ledger import AttemptBudget, BudgetExhausted
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.choice_gateway_observer import main
from src.experiments.postgresql.query_config import QueryConfig
from src.experiments.postgresql.query_evaluation import ray_transport_accounting


class RayObservationTests(unittest.TestCase):
    def test_remote_guard_cannot_bypass_budget_using_cli_equals_spelling(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            budget = AttemptBudget('ray-fixture', 2)
            owner = CellBudgetLedger.create(root/'budget.sqlite', budget, deadline_utc=time.time()+60)
            owner.reserve_unit('query', 2)
            request = SimpleNamespace(task=SimpleNamespace(payload=b'{"model":"fixture"}'))
            def serve(argv, **options):
                options['remote_request_guard'](request)
                owner.close_shared_unit('query')
                with self.assertRaises(BudgetExhausted):
                    options['remote_request_guard'](request)
                return 0
            with patch('src.experiments.choice_gateway_observer.server.main', side_effect=serve):
                code = main(['--events', str(root/'events.jsonl'), '--cell-budget', str(owner.path),
                    '--shared-unit-budget', '--unit-id', 'query', '--budget-id', budget.budget_id,
                    '--max-attempts', '2', '--observer-summary', str(root/'observer.json'), '--',
                    '--incremental-map', '--map-transport-config=fixture.json'])
            self.assertEqual(code, 0)
            self.assertEqual(json.loads((root/'observer.json').read_text())['observed_attempts'], 1)
            self.assertEqual(len((root/'events.jsonl').read_text().splitlines()), 1)
            self.assertEqual(owner.snapshot()['allocated_requests'], 2)

    def test_transport_identity_is_required_and_native_arms_remain_native(self):
        base = QueryConfig('test', 'pg', 'map', 'inputs')
        selected = replace(base, map_transport_config='fixture.json', map_transport_sha256='a'*64)
        for changes in ({'arm': 'ray-data'}, {'event_content': 'compact'},
                        {'map_transport_sha256': None}, {'map_transport_sha256': 'bad'}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                replace(selected, **changes)

    def test_put_in_progress_remains_charged_when_previous_block_releases(self):
        def event(kind, block, used):
            return dict(event='core_ray_block_'+kind, block_id=block, bytes=40, rows=1,
                        object_bytes=used, object_limit_bytes=80)
        events = [event('reserved', 0, 40), event('put', 0, 40), event('reserved', 1, 80),
                  event('released', 0, 40), event('put', 1, 40), event('released', 1, 0)]
        events.extend(dict(event='core_ray_http_completed', key=dict(session_id=0, sequence=i))
                      for i in range(2))
        self.assertEqual(ray_transport_accounting(events, 2)['peak_arrow_buffer_bytes'], 80)
        for corrupted in (events[:-1], events + [events[-1]], [*events[:2], *events[3:]]):
            with self.assertRaises(ValueError):
                ray_transport_accounting(corrupted, 2)
