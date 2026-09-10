"""Exercise independent event content and writer modes through the observer CLI."""

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from src.experiments.choice_gateway_observer import main


class ChoiceObserverTests(unittest.TestCase):
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
