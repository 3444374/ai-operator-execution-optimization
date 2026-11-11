"""Native relation timing and original sinks, with no author library or model."""

import os
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import pandas as pd

from src.experiments import native_imlane_relation as relation
from src.infrastructure.private_artifacts import publish_private_json


class PublishedRelationTimingTests(unittest.TestCase):
    def run_fixture(self, *, exit_code=0, audit_failure=False, sink_failure=False, early_exit=False,
                    close_failure=False, exit_during_sink=False):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        events, clock = [], [100_000_000_000]
        scope = SimpleNamespace(env={}, children=[], track_root=Mock(), check=Mock())
        class Scope:
            def __enter__(self):
                events.append('author-open')
                return scope
            def __exit__(self, *_args):
                events.append('author-close')
                clock[0] = 400_000_000_000
                if close_failure:
                    raise RuntimeError('author close failed')
        process = SimpleNamespace(returncode=None)
        def poll():
            if process.returncode is None and (early_exit or (root / 'sink-release.ack').exists()):
                process.returncode = exit_code
                clock[0] = 150_000_000_000
            return process.returncode
        process.poll = poll
        process.wait = lambda **_kwargs: poll()
        def spawn(*_args):
            publish_private_json(root / 'query.json', dict(status='query_returned',
                result_clock='CLOCK_MONOTONIC', result_ready_monotonic_ns=110_000_000_000,
                sink_handoff=dict(protocol=relation.ACK_PROTOCOL, release_path=str(root / 'sink-release.ack')),
                accepted_occurrences=[dict(occurrence_id='r1', title='actual native row')]))
            clock[0] = 120_000_000_000
            return process, Mock()
        def sink(native):
            self.assertIsNone(process.returncode)
            self.assertNotIn('author-close', events)
            events.append('sink')
            clock[0] = 130_000_000_000
            if sink_failure:
                raise ValueError('original sink failed')
            if exit_during_sink:
                process.returncode = 7
            return [row['title'] for row in native['accepted_occurrences']]
        def audit(_native, result):
            self.assertEqual(result, ['actual native row'])
            self.assertNotIn('author-close', events)
            events.append('audit')
            clock[0] = 210_000_000_000
            if audit_failure:
                raise ValueError('receipt audit failed')
            return dict(checked=True)
        report = {}
        args = SimpleNamespace(output=root)
        window = SimpleNamespace(check=Mock(), accept_ready=Mock(), begin_cleanup=Mock(return_value=300.))
        with patch.object(relation, 'DuckDBScope', return_value=Scope()), \
                patch.object(relation.owner, 'spawn', side_effect=spawn), \
                patch.object(relation.owner, 'resource_state', return_value={'resources_confirmed': True}), \
                patch.object(relation.time, 'monotonic_ns', side_effect=lambda: clock[0]):
            error = None
            try:
                result = relation.run_relation_query(args, report, kernel=root / 'fixture.py',
                    command=['fixture'], window=window, query_started_ns=100_000_000_000,
                    sink=sink, audit=audit, check=lambda: None)
            except (RuntimeError, ValueError) as caught:
                error = caught
                result = None
        return report, events, result, error

    def test_full_sink_is_before_audit_and_close_and_uses_the_native_relation(self):
        report, events, result, error = self.run_fixture()
        self.assertIsNone(error)
        self.assertEqual(events, ['author-open', 'sink', 'audit', 'author-close'])
        self.assertEqual(report['source_to_sink_seconds'], 30)
        self.assertEqual(report['result_ready_monotonic_ns'], 130_000_000_000)
        self.assertEqual(report['audit_started_monotonic_ns'], 150_000_000_000)
        self.assertEqual(report['status'], 'completed')
        self.assertEqual(result, (['actual native row'], {'checked': True}))

    def test_post_result_process_failure_cannot_be_promoted_to_success(self):
        report, events, _result, error = self.run_fixture(exit_code=1)
        self.assertIsInstance(error, RuntimeError)
        self.assertNotIn('audit', events)
        self.assertEqual(events[-1], 'author-close')
        self.assertEqual(report['result_ready_monotonic_ns'], 130_000_000_000)
        self.assertEqual(report['status'], 'failed')

    def test_published_relation_does_not_prove_the_native_driver_is_still_owned(self):
        report, events, _result, error = self.run_fixture(early_exit=True)
        self.assertIsInstance(error, RuntimeError)
        self.assertNotIn('sink', events)
        self.assertNotIn('result_ready_monotonic_ns', report)
        self.assertEqual(report['status'], 'failed')

    def test_receipt_audit_failure_preserves_clock_but_fails_the_query(self):
        report, events, _result, error = self.run_fixture(audit_failure=True)
        self.assertIsInstance(error, ValueError)
        self.assertEqual(events[-2:], ['audit', 'author-close'])
        self.assertEqual(report['source_to_sink_seconds'], 30)
        self.assertEqual(report['status'], 'failed')

    def test_audit_and_close_failures_carry_the_completed_sink_and_report(self):
        for failure in ('audit_failure', 'close_failure'):
            with self.subTest(failure=failure):
                report, _events, _result, error = self.run_fixture(**{failure: True})
                self.assertIsNotNone(error)
                self.assertIs(error.query_report, report)
                self.assertEqual(error.query_result, ['actual native row'])
                self.assertEqual(report['full_sink_observed_monotonic_ns'], 130_000_000_000)
                self.assertTrue(report['native_alive_after_sink'])
                self.assertEqual(report['status'], 'failed')

    def test_exit_during_sink_keeps_observation_without_qualified_readiness(self):
        report, events, _result, error = self.run_fixture(exit_during_sink=True)
        self.assertIsInstance(error, RuntimeError)
        self.assertIs(error.query_report, report)
        self.assertEqual(error.query_result, ['actual native row'])
        self.assertEqual(report['full_sink_observed_monotonic_ns'], 130_000_000_000)
        self.assertFalse(report['native_alive_after_sink'])
        self.assertNotIn('result_ready_monotonic_ns', report)
        self.assertNotIn('audit', events)
        self.assertEqual(report['status'], 'failed')

    def test_sink_failure_publishes_abort_before_reaping_and_author_close(self):
        report, events, _result, error = self.run_fixture(sink_failure=True)
        self.assertIsInstance(error, ValueError)
        self.assertEqual(report['sink_release']['outcome'], 'abort')
        self.assertNotIn('result_ready_monotonic_ns', report)
        self.assertEqual(events[-1], 'author-close')
        self.assertEqual(report['status'], 'failed')


class RelationObservationPersistenceTests(unittest.TestCase):
    def test_failed_query_saves_complete_frame_and_failed_report(self):
        report = dict(status='failed', full_sink_observed_monotonic_ns=130,
            result_ready_monotonic_ns=130, native_alive_after_sink=True)
        frame = pd.DataFrame({'title': ['second', 'first', 'second']}, index=[2, 0, 4])
        with tempfile.TemporaryDirectory() as directory:
            relation.persist_relation_observation(directory, report, {'3b': frame}, primary=ValueError('audit'))
            saved = json.loads((Path(directory) / 'full-sink-observed.json').read_text())
            pd.testing.assert_frame_equal(pd.DataFrame.from_dict(saved['frames']['3b'], orient='tight'), frame)
            self.assertEqual(saved['status'], 'failed')
            self.assertEqual(json.loads((Path(directory) / 'author.json').read_text()), report)

    def test_secondary_write_failure_does_not_replace_primary_and_still_saves_report(self):
        report = dict(status='failed', full_sink_observed_monotonic_ns=130,
            native_alive_after_sink=False)
        primary = RuntimeError('native exited')
        with patch.object(relation, 'write_private_json', side_effect=[OSError('disk'), None]) as write:
            relation.persist_relation_observation('/unused', report, pd.DataFrame({'score': [3.]}), primary=primary)
        self.assertEqual(write.call_count, 2)
        self.assertEqual(write.call_args.args, (Path('/unused/author.json'), report))
        self.assertEqual(primary.__notes__, ['full-sink-observed.json persistence also failed: OSError'])


@unittest.skipUnless(shutil.which('c++'), 'requires the existing local C++ compiler')
class NativeHandoffTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temporary.cleanup)
        root = Path(cls.temporary.name)
        source = root / 'publisher.cpp'
        source.write_text('''#include "relation_handoff.hpp"
#include <iostream>
int main(int argc, char **argv) {
    try {
        semloom_imlane::RelationHandoff handoff(argv[1], 123);
        std::cout << "published" << std::endl;
        handoff.Wait(std::chrono::milliseconds(std::stoi(argv[2])));
        std::cout << "released" << std::endl;
        return 0;
    } catch (const std::exception &error) {
        std::cerr << error.what() << std::endl;
        return 1;
    }
}
''')
        cls.binary = root / 'publisher'
        include = Path(__file__).resolve().parents[2] / 'integrations/imlane_duckdb'
        compiled = subprocess.run(['c++', '-std=c++17', str(source), '-I' + str(include),
            '-pthread', '-o', str(cls.binary)], capture_output=True, text=True, timeout=30)
        if compiled.returncode:
            raise AssertionError(compiled.stdout + compiled.stderr)

    def test_publisher_cannot_exit_until_the_sink_ack_and_rejects_stale_ack(self):
        with tempfile.TemporaryDirectory() as temporary:
            ack = Path(temporary) / 'sink-release.ack'
            process = subprocess.Popen([str(self.binary), str(ack), '3000'],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            self.addCleanup(lambda: process.kill() if process.poll() is None else None)
            self.assertEqual(process.stdout.readline().strip(), 'published')
            with self.assertRaises(subprocess.TimeoutExpired):
                process.wait(timeout=.1)
            relation.release_relation(ack, 123, 456, outcome='complete')
            stdout, stderr = process.communicate(timeout=3)
            self.assertEqual(process.returncode, 0, stderr)
            self.assertEqual(stdout.strip(), 'released')
            stale = subprocess.run([str(self.binary), str(ack), '3000'],
                capture_output=True, text=True, timeout=3)
            self.assertEqual(stale.returncode, 1)
            self.assertIn('fresh ACK path', stale.stderr)
            self.assertEqual(stale.stdout, '')

    def test_abort_releases_publisher_as_failure_and_missing_ack_times_out(self):
        with tempfile.TemporaryDirectory() as temporary:
            ack = Path(temporary) / 'abort.ack'
            process = subprocess.Popen([str(self.binary), str(ack), '3000'],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            self.addCleanup(lambda: process.kill() if process.poll() is None else None)
            self.assertEqual(process.stdout.readline().strip(), 'published')
            relation.release_relation(ack, 123, 0, outcome='abort')
            _stdout, stderr = process.communicate(timeout=3)
            self.assertEqual(process.returncode, 1)
            self.assertIn('sink aborted', stderr)
            timeout = subprocess.run([str(self.binary), str(Path(temporary) / 'missing.ack'), '100'],
                capture_output=True, text=True, timeout=3)
            self.assertEqual(timeout.returncode, 1)
            self.assertIn('deadline exceeded', timeout.stderr)


CHECKOUT = os.environ.get('SEMLOOM_NATIVE_TEST_CHECKOUT')


@unittest.skipUnless(CHECKOUT, 'set SEMLOOM_NATIVE_TEST_CHECKOUT to the pinned source')
class OriginalRelationSinkTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        pd.DataFrame(dict(reviewId=['duplicate', 'duplicate', 'third'], id=['movie-a', 'movie-a', 'movie-b'],
            reviewText=['first', 'second', 'third'])).to_csv(self.root / 'Reviews.csv', index=False)
        pd.DataFrame(dict(id=['movie-a', 'movie-b'], audienceScore=[0, 0])).to_csv(self.root / 'Movies.csv', index=False)
        pd.DataFrame(dict(row_id=[0, 1, 2], title=['Mean Girls', 'Orange County', 'Mean Girls'],
            text=['first', 'second', 'third'])).to_csv(self.root / 'lizzy_caplan_text_data.csv', index=False)

    def query(self, scenario, code):
        from src.baselines.text.sembench_text import SemBenchTextQuery
        from src.serving.backends.vllm_protocol import GenerationSpec
        return SemBenchTextQuery(CHECKOUT, self.root, scenario, code,
            execution_id='cpu-' + code, model_name='cpu-no-model', generation=GenerationSpec(max_tokens=8))

    def test_original_movie_group_sink_consumes_actual_double_scores_and_duplicate_occurrences(self):
        query = self.query('movie', '10')
        sources = [dict(occurrence_id='r' + str(i), id=row['id'], reviewId=row['reviewId'], source_position=i)
                   for i, row in query.operation.frame.iterrows()]
        native = dict(return_type='DOUBLE', full_projection_materialized=True, source_rows=3,
            mapped_occurrences=[dict(source, score=score) for source, score in zip(sources, [1., 5., 2.], strict=True)])
        native['mapped_occurrences'].reverse()
        frame = relation.finish_movie_scores(native, query, sources)
        self.assertEqual(frame.to_dict(orient='records'),
                         [dict(movieId='movie-a', movieScore=3.), dict(movieId='movie-b', movieScore=2.)])
        native['mapped_occurrences'].pop()
        with self.assertRaisesRegex(ValueError, 'occurrence coverage'):
            relation.finish_movie_scores(native, query, sources)

    def test_joint_original_mmqa_title_sinks_use_native_membership_and_source_order(self):
        bindings, accepted = {}, []
        for group, code in enumerate(('3b', '3c')):
            query = self.query('mmqa', code)
            sources = [dict(occurrence_id=code + '-r' + str(i), source_position=group * 3 + i)
                       for i in range(3)]
            bindings[code] = (query, sources)
            for index in (1, 0) if group == 0 else (2, 0):
                accepted.append(dict(sources[index], title=query.operation.frame.iloc[index]['title']))
        native = dict(return_type='TINYINT', full_filter_materialized_before_title_sink=True,
            source_rows=6, eligible_rows=6, accepted_occurrences=accepted)
        results = relation.finish_mmqa_filters(native, bindings)
        self.assertEqual(results['3b'].iloc[:, 0].tolist(), ['Mean Girls', 'Orange County'])
        self.assertEqual(results['3c'].iloc[:, 0].tolist(), ['Mean Girls', 'Mean Girls'])
        native['accepted_occurrences'].append(accepted[0])
        with self.assertRaisesRegex(ValueError, 'duplicate or unknown'):
            relation.finish_mmqa_filters(native, bindings)


if __name__ == '__main__':
    unittest.main()
