"""Verify write-once diagnostic evidence and retention of partial failures."""
import importlib.util
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch


path = Path(__file__).resolve().parents[2]/'scripts/experiments/pg_result_buffer_probe.py'
spec = importlib.util.spec_from_file_location('pg_result_buffer_probe', path)
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


class BufferProbeEvidenceTests(unittest.TestCase):
    def invoke(self, root, case):
        connection = MagicMock()
        connection.info.server_version = 180003
        connection.__enter__.return_value = connection
        connection.execute.return_value.fetchone.return_value = [[]]
        psycopg = SimpleNamespace(connect=lambda *args, **kwargs: connection, __version__='fixture')
        with patch.dict('sys.modules', psycopg=psycopg), \
             patch.dict('os.environ', SEMLOOM_TEST_PG_DSN='fixture'), \
             patch('sys.argv', ['probe', '--output', str(root)]), \
             patch.object(probe, 'run_case', side_effect=case), patch('builtins.print'):
            return probe.main()

    def test_four_cases_write_distinct_checkpoints_and_one_final_summary(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)/'probe'
            def case(connection, output, index, size):
                return dict(index=index, payload_bytes=size)
            self.assertEqual(self.invoke(root, case), 0)
            summary = json.loads((root/'summary.json').read_text())
            self.assertEqual(summary['status'], 'completed_diagnostic')
            self.assertEqual([c['payload_bytes'] for c in summary['cases']], [8, 128, 128, 8])
            for i, result in enumerate(summary['cases']):
                self.assertEqual(json.loads((root/f'case-{i}-summary.json').read_text()), result)

    def test_later_failure_preserves_first_checkpoint_and_original_error(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)/'probe'
            def case(connection, output, index, size):
                if index == 1:
                    raise RuntimeError('fixture second query failed')
                return dict(index=index, payload_bytes=size)
            self.assertEqual(self.invoke(root, case), 1)
            summary = json.loads((root/'summary.json').read_text())
            self.assertEqual(summary['status'], 'failed')
            self.assertEqual(summary['error']['detail'], 'fixture second query failed')
            self.assertEqual(len(summary['cases']), 1)
            self.assertTrue((root/'case-0-summary.json').is_file())
            self.assertFalse((root/'case-1-summary.json').exists())

    def test_interrupted_stream_retains_received_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            connection = MagicMock()
            cursor = connection.cursor.return_value.__enter__.return_value
            def interrupted(*args, **kwargs):
                yield ('0', 'xxxxxxxx', '100.005000')
                raise RuntimeError('fixture interrupted stream')
            cursor.stream.side_effect = interrupted
            with self.assertRaisesRegex(RuntimeError, 'interrupted stream'):
                probe.run_case(connection, root, 0, 8)
            partial = json.loads((root/'case-0-rows.json').read_text())
            self.assertEqual(len(partial['records']), 1)
            self.assertEqual(partial['records'][0]['row'][0], '0')
