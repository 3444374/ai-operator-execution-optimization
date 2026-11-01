"""Movie callback qualification over real CPU IPC and a logical native frontier.

No model/GPU is used. Useful requests and the actual controller decide quotas;
the fixture never emits a window change or makes a query last longer for a scan.
"""
from copy import deepcopy
from dataclasses import replace
import json
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.lotus_records_batches import PreparedRecordsCore
from tests.experiments.movie_token_resident_fixture import (
    MovieTokenResidentFixture, query_calls, query_close,
)
from tests.experiments.movie_feedback_frontier import logical_request_frontier


class MovieAscendingFeedbackTests(unittest.TestCase):
    def fixture(self, *, count=96):
        fixture = MovieTokenResidentFixture(self)
        rows = [[str(i+1), 'taken_3', 'good' if i % 2 == 0 else 'bad',
                 'POSITIVE' if i % 2 == 0 else 'NEGATIVE'] for i in range(count)]
        fixture.manifests['movie-q1'] = fixture.source_manifest('ascending-many', rows, task='movie-q1')
        return fixture

    def plan(self, fixture, tasks=('movie-q1', 'movie-q1')):
        plan = fixture.plan('RD', tasks=tasks)
        plan['resources']['max_active_batches'] = 8
        plan['profile']['admission'].update(poll_interval_s=.002, min_samples=1)
        return plan

    def test_normal_movie_entry_qualifies_a_selection_reuses_it_and_preserves_it_through_empty_query(self):
        fixture = self.fixture()
        fixture.manifests['movie-q2'] = fixture.source_manifest('ascending-empty', [], task='movie-q2')
        fixture.manifests['movie-q3'] = fixture.source_manifest('ascending-other-method', [
            ['1', 'taken_3', 'good', 'POSITIVE'], ['2', 'taken_3', 'bad', 'NEGATIVE'],
            ['3', 'taken_3', 'good', 'POSITIVE']], task='movie-q3')
        snapshots = []
        staged_before_callback_return = []
        from src.experiments.movie_matrix_support import record_core_selection

        def observe_selection(query, context):
            if query['query_id'] == 'resident-query-0':
                chain = fixture.owners[0].owner
                staged_before_callback_return.append(deepcopy(chain._request_scan_pending))
                self.assertTrue(staged_before_callback_return[-1])
                self.assertFalse(chain._request_scan_hints, 'model EOF cannot commit before caller acknowledgement')
            return record_core_selection(query, context)

        def checkpoint(info):
            owner, = fixture.owners
            chain = owner.owner
            snapshots.append(deepcopy(chain._request_scan_hints))
            self.assertFalse(chain._request_scan_pending)

        with logical_request_frontier(), \
                patch('src.experiments.movie_matrix_support.record_core_selection', observe_selection):
            report = fixture.run(self.plan(fixture,
                tasks=('movie-q1', 'movie-q2', 'movie-q1', 'movie-q3')), checkpoint=checkpoint)
        self.assertEqual(report['status'], 'completed')
        self.assertEqual(len(staged_before_callback_return), 1)
        self.assertEqual(report['owner_start_count'], 1)
        self.assertEqual(len(fixture.runtime.async_actors), 1)
        self.assertEqual([len(query_calls(q)) for q in report['queries']], [96, 0, 96, 3])
        self.assertEqual(len({call['engine_uid'] for query in report['queries']
                              for call in query_calls(query)}), 1)
        first, empty, reused, changed = [query_close(q) for q in report['queries']]
        phase = first['request_supply_feedback']['phases'][0]
        self.assertEqual(phase['strategy'], 'ascending_request_work_v1')
        self.assertGreater(phase['accepted_steps'], 0, 'the original CPU work must actually qualify a gain')
        self.assertGreater(phase['recommended_window'], 1)
        self.assertEqual(snapshots[0], snapshots[1], 'an empty query must not replace a healthy hint')
        self.assertTrue(snapshots[0])
        self.assertTrue(report['queries'][0]['backend_report']['request_scan_reuse']['committed'])
        self.assertFalse(report['queries'][1]['backend_report']['request_scan_reuse']['committed'])
        self.assertGreater(reused['async_token_batches']['recent_batches'][0]['C_request'], 1)
        self.assertGreater(reused['request_supply_feedback']['initial_requests'], 1)
        self.assertTrue(reused['request_supply_feedback']['phases'][0]['state_reused'])
        self.assertFalse(changed['request_supply_feedback']['phases'][0]['state_reused'])
        self.assertEqual(changed['async_token_batches']['recent_batches'][0]['C_request'], 1)
        self.assertLess(max(c['core_task_key']['session_id'] for c in query_calls(report['queries'][0])),
                        min(c['core_task_key']['session_id'] for c in query_calls(report['queries'][2])))
        for query, close in zip(report['queries'], (first, empty, reused, changed)):
            self.assertEqual(query['status'], 'completed')
            self.assertTrue(close['query_completed'])
            self.assertTrue(close['request_supply_feedback']['snapshot_rpc_terminal_evidence_complete'])
            self.assertEqual(close['core_usage']['held_tasks'], 0)
            self.assertEqual(close['unconsumed_call_results'], [])
        committed = deepcopy(fixture.owners[0].owner._request_scan_hints)
        external = report['queries'][0]['backend_report']
        external['request_scan_reuse']['qualified_phase_hints'] = -1
        external['backend_close']['request_supply_feedback']['phases'][0]['search_state']['work_context']['input_min'] = -1
        self.assertEqual(fixture.owners[0].owner._request_scan_hints, committed)

    def test_downstream_interpretation_or_selection_failure_discards_staged_evidence(self):
        for boundary in ('interpret', 'selection'):
            with self.subTest(boundary=boundary):
                fixture = self.fixture()
                captured = []
                acknowledgements = []
                confirm = PreparedRecordsCore.confirm_query_feedback

                def acknowledge(owner, *, completed):
                    acknowledgements.append(completed)
                    return confirm(owner, completed=completed)

                def reject(*args, **kwargs):
                    owner, = fixture.owners
                    captured.append(deepcopy(owner.owner._request_scan_pending))
                    self.assertFalse(owner.owner._request_scan_hints)
                    raise ValueError('CPU fixture downstream '+boundary+' rejection')

                target = ('src.semantic_methods.lotus.engine.interpret_results' if boundary == 'interpret' else
                          'src.experiments.movie_matrix_support.record_core_selection')
                with logical_request_frontier(), patch(target, side_effect=reject), \
                        patch.object(PreparedRecordsCore, 'confirm_query_feedback', acknowledge), \
                        self.assertRaisesRegex(ValueError, 'CPU fixture downstream '+boundary+' rejection'):
                    fixture.run(self.plan(fixture))
                self.assertEqual(len(captured), 1)
                self.assertEqual(acknowledgements, [False])
                owner, = fixture.owners
                self.assertFalse(owner.owner._request_scan_hints)
                self.assertFalse(owner.owner._request_scan_pending)
                self.assertEqual(owner.last_close_report['core_usage']['held_tasks'], 0)
                self.assertEqual(len(owner.last_close_report['calls']), 96)

    def test_changed_controller_configuration_cannot_load_the_previous_query_hint(self):
        fixture = self.fixture()
        plan = self.plan(fixture)
        pending = []

        def checkpoint(info):
            owner, = fixture.owners
            if info['query_index'] == 0:
                query = json.loads((fixture.outputs[-1]/'query-0.json').read_text())
                close = query_close(query)
                phase = close['request_supply_feedback']['phases'][0]
                self.assertTrue(owner.owner._request_scan_hints, str(dict(
                    reuse=query['backend_report'].get('request_scan_reuse'),
                    phase={key:phase.get(key) for key in ('window', 'recommended_window',
                        'accepted_steps', 'unconfirmed_trial', 'feedback_gaps', 'stop_reason',
                        'work_epoch', 'search_state')},
                    events=[{key:event.get(key) for key in ('kind', 'outcome', 'work_epoch')}
                            for event in phase['evaluations'][-4:]])))
                pending.append(deepcopy(owner.owner._request_scan_hints))
                # Deliberately vary only the controller on the retained owner:
                # exact identity must protect reuse even with unchanged weights.
                owner.owner.request_feedback = replace(owner.owner.request_feedback, min_gain=.05)

        with logical_request_frontier():
            report = fixture.run(plan, checkpoint=checkpoint)
        self.assertEqual(report['status'], 'completed')
        first, second = [query_close(q) for q in report['queries']]
        self.assertGreater(first['request_supply_feedback']['phases'][0]['accepted_steps'], 0)
        self.assertFalse(second['request_supply_feedback']['phases'][0]['state_reused'])
        self.assertEqual(second['async_token_batches']['recent_batches'][0]['C_request'], 1)
        identity = fixture.owners[0].owner._supply_feedback._hint_identity
        self.assertNotIn(identity, pending[0])


if __name__ == '__main__':
    unittest.main()
