"""Synthetic actor-resource packet -> existing RPC -> controller -> Core permits.

This is a CPU wiring test with explicit fake physical observations. Neither the
observations nor the logical-clock query durations are hardware measurements.
"""
from copy import deepcopy
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor
from tests.experiments.movie_token_resident_fixture import MovieTokenResidentFixture, query_calls, query_close
from tests.experiments.movie_feedback_frontier import logical_request_frontier


GPU = 'GPU-00000000-0000-0000-0000-000000000001'


class MovieResourceProbeTests(unittest.TestCase):
    def test_actor_packet_on_same_snapshot_rpc_selects_a_headroom_sized_probe_and_actuates_Core(self):
        results, first_windows = [], []
        for physical in (False, True):
            with self.subTest(synthetic_physical_observation=physical):
                fixture = MovieTokenResidentFixture(self)
                fixture.manifests['movie-q1'] = fixture.source_manifest('resource-wiring', [
                    [str(i+1), 'taken_3', 'good' if i % 2 == 0 else 'bad',
                     'POSITIVE' if i % 2 == 0 else 'NEGATIVE'] for i in range(96)], task='movie-q1')
                plan = fixture.plan('RD', tasks=('movie-q1',))
                plan['resources']['max_active_batches'] = 8
                # The CPU service advances each completed/ready cohort by one
                # tick. Two samples therefore require a two-tick window;
                # this changes only the declared CPU fixture qualification.
                plan['profile']['admission'].update(window_s=2, min_samples=1, poll_interval_s=.002)
                packets = []

                def resources(actor, session_id):
                    packet = dict(engine_uid=actor.engine.ready_report['engine_uid'],
                        session_id=session_id, source_elapsed_s=0,
                        gpu_uuids=[GPU] if physical else [],
                        status='observed_or_partial' if physical else 'unavailable',
                        sample=dict(values=(dict(gpu_utilization_pct=10, host_cpu_per_core_max_pct=20)
                                            if physical else {}),
                            sources=dict(host=dict(cpu_percent_interval_s=.1)),
                            missing={} if physical else {'physical': 'explicit CPU fixture observation off'}))
                    packets.append(deepcopy(packet))
                    return packet

                with logical_request_frontier(), patch.object(AsyncCoreModelActor, '_resource_snapshot', resources):
                    report = fixture.run(plan)
                self.assertEqual(report['status'], 'completed')
                query, = report['queries']
                close = query_close(query)
                feedback = close['request_supply_feedback']
                phase, = feedback['phases']
                self.assertTrue(packets, 'the actor resource method must actually be called over snapshot RPC')
                first = next(row for row in phase['history'] if row['reason'] == 'performance_probe')
                first_windows.append(first['window'])
                self.assertEqual((first['before'], first['window']), (1, 6 if physical else 2),
                                 str(phase['step_decisions']))
                decision = phase['step_decisions'][0]
                self.assertEqual(decision['source'], 'resource_feedback_probe')
                self.assertFalse(decision['resource_target_accepted'], 'no exact KV cost model was supplied')
                self.assertIsNone(decision['evidence']['cost_model'])
                if physical:
                    self.assertEqual(decision['requested_probe_target'], 6)
                    self.assertTrue(decision['probe_target_accepted'])
                    self.assertEqual(decision['resource_observation']['physical_qualification'],
                                     'two_fresh_idle_majority_samples')
                    self.assertGreaterEqual(close['async_token_batches']['peak_requests'], 6)
                    self.assertTrue(any(batch['C_request'] == 6 and batch['members'] > 1
                                        for batch in close['async_token_batches']['recent_batches']))
                self.assertEqual(query['backend_report']['result'], {'reviewId': ['1', '3', '5', '7', '9']})
                classification = query['backend_report']['quality']['classification_audit']
                self.assertEqual((classification['evaluated_rows'], classification['true_positive'],
                                  classification['true_negative'], classification['false_positive'],
                                  classification['false_negative'], classification['missing_rows']),
                                 (96, 48, 48, 0, 0, 0))
                calls = query_calls(query)
                self.assertEqual(len(calls), 96)
                self.assertTrue(all(call['status'] == 'succeeded' for call in calls))
                self.assertEqual(len({call['call_id'] for call in calls}), 96)
                self.assertEqual({call['engine_uid'] for call in calls}, {packet['engine_uid'] for packet in packets})
                # A snapshot dispatched before the first call can honestly
                # report the actor's unbound session. Only accepted packets
                # may seed the current phase's resource decisions.
                accepted = feedback['resource_observation']['recent_samples']
                self.assertTrue(accepted)
                self.assertTrue(all(row['view']['session_id'] == phase['session_id'] for row in accepted))
                self.assertEqual(close['async_token_batches']['request_limit'], 8)
                self.assertLessEqual(close['async_token_batches']['peak_requests'], 8)
                limits = fixture.owners[0].owner._actors.limits
                self.assertLessEqual(close['async_token_batches']['peak_data_rpc_bytes'], limits.input_bytes)
                self.assertLessEqual(close['async_token_batches']['peak_result_rpc_bytes'], limits.result_bytes)
                self.assertEqual(close['unconsumed_call_results'], [])
                self.assertEqual(close['core_usage']['held_tasks'], 0)
                self.assertTrue(feedback['sampler_stopped'])
                self.assertTrue(feedback['snapshot_rpc_terminal_evidence_complete'])
                self.assertFalse(feedback['snapshot_ref_retained'])
                self.assertEqual(feedback['snapshot_requests'], len(feedback['snapshot_rpc_receipts']))
                self.assertTrue(feedback['resource_observation']['enabled'])
                results.append(dict(result=query['backend_report']['result'],
                    # Original decisions retain owner UID, native request ID
                    # and clocks. Separate owners must keep those identities;
                    # compare complete semantic decisions, not random handles.
                    decisions=[(row['source_position'], row['decision'], row['explanation'], row['raw_output'])
                               for row in query['backend_report']['decisions']],
                    generation=query['backend_report']['generation'],
                    token_work=[(call['source_position'], call['text'], call['prompt_tokens'],
                                 call['completion_tokens'], call['prompt_computed_tokens'],
                                 call['prompt_cached_tokens']) for call in calls]))
        self.assertEqual(first_windows, [2, 6])
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0], results[1])


if __name__ == '__main__':
    unittest.main()
