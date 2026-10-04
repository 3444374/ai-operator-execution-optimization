"""Check diagnostic identity, actual socket backpressure and retained failures."""

import gzip
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from src.experiments.gateway_isolation_probe import SCENARIOS, Client, analyze, run_case


class GatewayIsolationProbeTests(unittest.TestCase):
    def test_all_locations_preserve_query_identity_and_drain_resources(self):
        with tempfile.TemporaryDirectory() as root:
            for scenario in SCENARIOS:
                for paused in (False, True):
                    with self.subTest(scenario=scenario, paused=paused):
                        output = Path(root) / f'{scenario}-{paused}'
                        result = run_case(output, scenario, paused, rows=4, stall_s=.04)
                        self.assertEqual(result['status'], 'passed')
                        self.assertEqual((result['synthetic_calls'], result['settled_synthetic_calls']), (8, 8))
                        self.assertEqual((result['http_requests'], result['model_requests'], result['pg_queries']), (0, 0, 0))
                        self.assertEqual(result['queued_cancel_calls'], 0)
                        self.assertFalse(result['remaining_threads'] or result['errors'])
                        self.assertFalse(any(result['core_final_usage'].values()))
                        self.assertTrue(result['worker_stopped'] and result['transport_drained'] and result['actor_disposed'])
                        self.assertLessEqual(result['core_observed_peak']['active_requests'], 4)
                        if paused:
                            self.assertGreaterEqual(result['interference_ms'], 40)
                        with gzip.open(output / 'events.jsonl.gz', 'rt') as stream:
                            events = [json.loads(line) for line in stream]
                        recalculated = analyze(events, 4, scenario)
                        self.assertEqual(result['b_consume_ms'], recalculated['b_consume_ms'])
                        self.assertNotIn('token', {key for e in events for key in e})

    def test_existing_directory_is_refused_without_overwrite(self):
        with tempfile.TemporaryDirectory() as root:
            marker = Path(root) / 'kept'
            marker.write_text('original')
            with self.assertRaises(FileExistsError):
                run_case(root, 'owner_prepare', False, rows=4)
            self.assertEqual(marker.read_text(), 'original')

    def test_failed_consumer_preserves_observations_and_cleanup(self):
        consume = Client.consume

        def fail_second(client, *args):
            if client.label == 'B':
                raise ValueError('fixture consumer rejected')
            return consume(client, *args)

        with tempfile.TemporaryDirectory() as root:
            output = Path(root) / 'failed'
            with patch.object(Client, 'consume', fail_second):
                with self.assertRaisesRegex(RuntimeError, 'evidence retained'):
                    run_case(output, 'payload_prepare', False, rows=4)
            result = json.loads((output / 'result.json').read_text())
            self.assertEqual(result['status'], 'failed')
            self.assertTrue(result['errors'])
            self.assertTrue(result['worker_stopped'] and result['transport_drained'])
            self.assertFalse(result['remaining_threads'])
            with gzip.open(output / 'events.jsonl.gz', 'rt') as stream:
                events = [json.loads(line) for line in stream]
            self.assertEqual(result['synthetic_calls'], sum(e['event'] == 'fixture_worker_submitted' for e in events))
            self.assertEqual(result['settled_synthetic_calls'], sum(e['event'] == 'fixture_worker_completed' for e in events))


if __name__ == '__main__':
    unittest.main()
