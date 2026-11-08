"""Full-cohort export and saved-result audit without an author/model runtime."""

import csv
from contextlib import chdir
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, Mock, patch

from src.experiments.native_imlane_query import audit_result, prepare_fixture, run_fixture, software_reply
from src.baselines.text.sembench_movie_native import load_q3_source, q3_messages
from src.infrastructure.private_artifacts import content_digest, read_json, write_private_json


class NativeIMLanePacketTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.source = self.root / 'Reviews.csv'
        with self.source.open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=('id', 'reviewId', 'reviewText', 'scoreSentiment'))
            writer.writeheader()
            writer.writerows([
                dict(id='other_movie', reviewId='duplicate', reviewText='', scoreSentiment='NEGATIVE'),
                dict(id='taken_3', reviewId='duplicate', reviewText='雪,"hello"\nnext', scoreSentiment='POSITIVE'),
                dict(id='taken_3', reviewId='duplicate', reviewText='other', scoreSentiment='NEGATIVE')])
        self.packet = self.root / 'packet'
        self.report = prepare_fixture(self.source, self.packet)

    def test_export_keeps_all_rows_values_and_occurrences_without_gold(self):
        with (self.packet / 'input.csv').open(newline='') as stream:
            rows = list(csv.DictReader(stream))
        self.assertEqual(rows, load_q3_source(self.source)['rows'])
        self.assertEqual(len(rows), 3)
        self.assertTrue(all('scoreSentiment' not in row for row in rows))
        self.assertEqual(self.report['model_requests'], 0)
        with patch.dict(os.environ, {'SEMLOOM_IMLANE_Q3_REPLIES': str(self.packet / 'replies.json')}):
            self.assertEqual(software_reply([q3_messages(row['reviewText']) for row in rows[1:]]),
                             ['Answer: True', 'Answer: False'])

    def receipts(self):
        source = load_q3_source(self.source)
        rows = source['rows'][1:]
        receipt_dir = self.root / 'receipts'
        receipt_dir.mkdir()
        predictions = [dict(occurrence_id=row['occurrence_id'], id=row['id'], reviewId=row['reviewId'],
            messages_sha256=content_digest(q3_messages(row['reviewText'])), generated_text=reply,
            decision=decision) for row, reply, decision in zip(rows, ['Answer: True', 'Answer: False'], [1, 0])]
        write_private_json(receipt_dir / 'q3-batch-one.json', dict(
            schema='semloom.imlane.duckdb.q3.batch.v1', provider_mode='software_fixture',
            status='succeeded', logical_rows=2, predictions=list(reversed(predictions)),
            sources=[{key: value for key, value in row.items() if key not in ('generated_text', 'decision')}
                     for row in reversed(predictions)],
            provider_calls=[{'logical_rows': 2, 'status': 'succeeded', 'returned': True}]))
        result = self.root / 'query.json'
        write_private_json(result, dict(status='query_returned', native_ai_query_returns=1,
            source_rows=3, eligible_rows=2, positive_review_cnt=1))
        return result, receipt_dir

    def test_audit_uses_saved_predictions_and_native_count_once(self):
        result, receipts = self.receipts()
        with patch('src.baselines.text.sembench_movie.evaluate_original', return_value={'CPU': True}) as original:
            report = audit_result(self.packet, result, receipts, 'software-checkout')
        self.assertEqual(report['quality']['classification']['false_positive'], 0)
        self.assertEqual(report['quality']['classification']['false_negative'], 0)
        original.assert_called_once()
        self.assertEqual(report['model_requests'], 0)
        self.assertFalse(report['performance_claim'])

    def test_changed_source_is_rejected_before_evaluation(self):
        result, receipts = self.receipts()
        self.source.write_text(self.source.read_text() + '\n')
        with self.assertRaisesRegex(ValueError, 'source changed'):
            audit_result(self.packet, result, receipts, 'software-checkout')

    def test_changed_messages_and_failed_receipts_are_not_accepted(self):
        result, receipts = self.receipts()
        path = receipts / 'q3-batch-one.json'
        original = read_json(path)
        for status, digest in [('failed', 'ignored'), ('succeeded', 'wrong')]:
            data = dict(original, status=status, predictions=[dict(row, messages_sha256=digest)
                                                           for row in original['predictions']])
            path.unlink()
            write_private_json(path, data)
            with self.assertRaises(ValueError):
                audit_result(self.packet, result, receipts, 'software-checkout')

    def test_conflicting_gold_for_identical_model_messages_is_not_fabricated(self):
        with self.source.open('a', newline='') as stream:
            csv.writer(stream).writerow(['taken_3', 'other_id', 'other', 'POSITIVE'])
        with self.assertRaisesRegex(ValueError, 'conflicting gold'):
            prepare_fixture(self.source, self.root / 'conflict')

    def test_unknown_or_missing_batch_calls_cannot_be_audited_as_complete(self):
        result, receipts = self.receipts()
        path = receipts / 'q3-batch-one.json'
        original = read_json(path)
        for calls in ([], [{'logical_rows': 2, 'status': 'unknown', 'returned': True}],
                      [{'logical_rows': 1, 'status': 'succeeded', 'returned': True}]):
            path.unlink()
            write_private_json(path, dict(original, provider_calls=calls))
            with self.assertRaisesRegex(ValueError, 'incomplete or unknown'):
                audit_result(self.packet, result, receipts, 'software-checkout')

    def run_args(self):
        return SimpleNamespace(packet=Path('packet'), output=Path('run'), author_root=Path('author'),
            method_python=Path('method-venv/bin/python'), driver=Path('bin/q3'), kind='process',
            threads=None, executor_count=None, query_seconds=120)

    def fake_scope(self, args, report, *, kernel):
        self.assertTrue(args.author_root.is_absolute())
        self.assertTrue(args.method_python.is_absolute())
        self.assertEqual(args.method_python, self.root / 'method-venv/bin/python')
        self.assertTrue(kernel.is_absolute())
        scope = SimpleNamespace(env=dict(args.workload_environment), children=[], track_root=Mock(), check=Mock())
        manager = MagicMock()
        manager.__enter__.return_value = scope
        manager.__exit__.return_value = False
        return manager

    def test_relative_cli_paths_are_absolute_before_spawn_changes_directory(self):
        process, writer = Mock(), Mock()
        process.wait.return_value = 0
        def spawn(command, env, log):
            for flag in ('--input', '--module', '--output'):
                self.assertTrue(Path(command[command.index(flag) + 1]).is_absolute())
            self.assertEqual(command[0], str(self.root / 'bin/q3'))
            self.assertTrue(Path(env['SEMLOOM_IMLANE_Q3_CONFIG']).is_absolute())
            self.assertTrue(log.is_absolute())
            return process, writer
        with chdir(self.root), patch('integrations.imlane_duckdb.runtime.DuckDBScope', side_effect=self.fake_scope), \
                patch('integrations.imlane_records.owner.spawn', side_effect=spawn), \
                patch('integrations.imlane_records.owner.resource_state', return_value={'resources_confirmed': True}):
            result = run_fixture(self.run_args())
        self.assertEqual(result['status'], 'passed')
        self.assertEqual(result['model_requests'], 0)
        self.assertTrue((self.root / 'run/worker.json').exists())

    def test_driver_error_preserves_primary_and_writes_failed_worker(self):
        failure = RuntimeError('CPU driver failed before its result')
        process, writer = Mock(), Mock()
        process.wait.side_effect = failure
        with chdir(self.root), patch('integrations.imlane_duckdb.runtime.DuckDBScope', side_effect=self.fake_scope), \
                patch('integrations.imlane_records.owner.spawn', return_value=(process, writer)):
            with self.assertRaises(RuntimeError) as caught:
                run_fixture(self.run_args())
        self.assertIs(caught.exception, failure)
        self.assertEqual(read_json(self.root / 'run/worker.json')['status'], 'failed')


if __name__ == '__main__':
    unittest.main()
