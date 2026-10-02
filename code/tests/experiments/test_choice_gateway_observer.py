"""Exercise independent event content and writer modes through the observer CLI."""

import json
from pathlib import Path
import tempfile
import asyncio
import time
import unittest
from unittest.mock import Mock, patch

from src.experiments.choice_gateway_observer import main, _guard_remote_request
from src.experiments.buffered_events import compact_event
from tests.execution_provider.test_ray_map_transport import task
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.postgresql.query_config import QueryConfig


class ChoiceObserverTests(unittest.TestCase):
    def test_threaded_cli_accounts_with_real_ledger_and_reports_selected_mode(self):
        self.check_accounting_cli("threaded")

    def test_batched_cli_accounts_with_real_ledger_and_reports_selected_mode(self):
        self.check_accounting_cli("batched")

    def check_accounting_cli(self, mode):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            budget = AttemptBudget('fixture.threaded.cli.v1', 2)
            ledger = CellBudgetLedger.create(root/'budget.sqlite', budget, deadline_utc=time.time()+20)
            ledger.reserve_unit('unit', 2)
            def gateway(args, **options):
                async def run():
                    await asyncio.gather(*(options['remote_request_guard'](
                        task(i, b'{"model":"fixture-model","messages":[]}')) for i in range(2)))
                asyncio.run(run())
                return 0
            with patch('src.experiments.choice_gateway_observer.server.main', side_effect=gateway):
                self.assertEqual(main(['--events', str(root/'events'), '--observer-summary', str(root/'summary'),
                    '--event-content', 'compact', '--event-write-mode', 'buffered',
                    '--cell-budget', str(ledger.path), '--shared-unit-budget', '--unit-id', 'unit',
                    '--budget-id', budget.budget_id, '--max-attempts', '2', '--remote-budget-mode', mode,
                    '--', '--socket', str(root/'socket'), '--fixed-model-config', str(root/'model'),
                    '--incremental-map', '--map-transport-config', str(root/'transport')]), 0)
            summary = json.loads((root/'summary').read_text())
            self.assertEqual(summary['remote_budget_mode'], mode)
            self.assertEqual(summary['observed_attempts'], 2)
            events = [json.loads(line) for line in (root/'events').read_text().splitlines()]
            self.assertEqual(sum(e['event']=='request' for e in events), 2)
            guards = [e for e in events if e['event']=='remote_request_guard']
            self.assertEqual([e['attempt'] for e in guards], [1, 2])
            self.assertTrue(all(e['status']=='completed' for e in guards))

    def test_threaded_config_requires_the_remote_map_path(self):
        with self.assertRaisesRegex(ValueError, 'Ray Map'):
            QueryConfig('unit', 'pg-source-direct', 'map', 'inputs', remote_budget_mode='threaded')
        old = QueryConfig('unit', 'pg', 'map', 'inputs')
        self.assertEqual(old.remote_budget_mode, 'synchronous')

    def test_remote_guard_separates_durable_reserve_from_request_recording(self):
        events = []
        ledger = Mock()
        ledger.reserve.return_value = 3
        observe = Mock()
        with patch('src.experiments.choice_gateway_observer.time.monotonic_ns',
                   side_effect=[100, 110, 160, 200, 205]):
            _guard_remote_request(task(7, b'private input'), ledger, observe, events.append)
        ledger.reserve.assert_called_once()
        self.assertEqual(len(ledger.reserve.call_args.args[0]), 64)
        observe.assert_called_once_with(3, b'private input')
        event = events[0]
        self.assertEqual((event['hash_ns'], event['reserve_ns'], event['request_observe_ns']), (10, 50, 40))
        self.assertEqual(event['elapsed_ns'], 105)
        self.assertEqual(event['key'], {'session_id': 0, 'sequence': 7})
        self.assertEqual(compact_event(event)['reserve_ns'], 50)
        self.assertNotIn('private', json.dumps(event))

    def test_remote_guard_failure_does_not_observe_or_refund_request(self):
        events = []
        ledger = Mock()
        ledger.reserve.side_effect = ValueError('fixture budget exhausted')
        observe = Mock()
        with self.assertRaisesRegex(ValueError, 'fixture budget exhausted'):
            _guard_remote_request(task(0), ledger, observe, events.append)
        observe.assert_not_called()
        self.assertEqual(events[0]['status'], 'failed')
        self.assertIsNone(events[0]['reserve_ns'])
        self.assertIsNone(events[0]['request_observe_ns'])

    def record(self, root, flags, full=False):
        events, summary = root / 'events.jsonl', root / 'summary.json'
        private = root / 'private.jsonl'
        def gateway(args, **kwargs):
            kwargs['incremental_observer']({'event': 'map_completion', 'raw_output': 'private answer',
                                           'sequence': 1, 'payload_digest': 'a' * 64})
            return 0
        args = ['--fixture-only', '--events', str(events), '--observer-summary', str(summary), *flags]
        if full:
            args += ['--private-events', str(private)]
        with patch('src.experiments.choice_gateway_observer.server.main', side_effect=gateway):
            self.assertEqual(main([*args, '--', '--incremental-map']), 0)
        return json.loads(events.read_text()), json.loads(summary.read_text()), private

    def test_content_and_write_modes_are_independent(self):
        for content in ('compact', 'full'):
            for mode in ('synchronous', 'buffered'):
                with self.subTest(content=content, mode=mode), tempfile.TemporaryDirectory() as directory:
                    event, summary, private = self.record(Path(directory),
                        ['--event-content', content, '--event-write-mode', mode], full=content == 'full')
                    self.assertEqual(summary['event_content'], content)
                    self.assertEqual(summary['event_write_mode'], mode)
                    self.assertGreaterEqual(summary['startup']['module_import_seconds'],0)
                    self.assertGreaterEqual(summary['startup']['observer_setup_seconds'],0)
                    if content == 'compact':
                        self.assertNotIn('raw_output', event)
                        self.assertIn('raw_output_sha256', event)
                        self.assertFalse(private.exists())
                    else:
                        self.assertEqual(json.loads(private.read_text())['raw_output'], 'private answer')
                        self.assertNotIn('raw_output', event)
                    if mode == 'buffered':
                        self.assertEqual(summary['events']['pending_bytes'], 0)
                        self.assertEqual(summary['events']['written_events'], 1)

    def test_legacy_flags_report_actual_content_and_write_mode(self):
        for flags, full, content, mode in (
            ([], False, 'redacted', 'synchronous'),
            ([], True, 'full', 'synchronous'),
            (['--event-mode', 'compact-buffered'], False, 'compact', 'buffered'),
        ):
            with self.subTest(flags=flags, full=full), tempfile.TemporaryDirectory() as directory:
                _, summary, _ = self.record(Path(directory), flags, full)
                self.assertEqual((summary['event_content'], summary['event_write_mode']), (content, mode))

    def test_full_without_private_destination_is_rejected_before_writing(self):
        with tempfile.TemporaryDirectory() as directory:
            events = Path(directory) / 'events'
            with patch('src.experiments.choice_gateway_observer.server.main') as gateway, \
                 self.assertRaises(SystemExit):
                main(['--fixture-only', '--events', str(events), '--event-content', 'full'])
            gateway.assert_not_called()
            self.assertFalse(events.exists())
