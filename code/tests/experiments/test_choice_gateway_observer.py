"""Exercise independent event content and writer modes through the observer CLI."""

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from src.experiments.choice_gateway_observer import main, _guard_remote_request
from src.experiments.buffered_events import compact_event
from tests.execution_provider.test_ray_map_transport import task


class ChoiceObserverTests(unittest.TestCase):
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
