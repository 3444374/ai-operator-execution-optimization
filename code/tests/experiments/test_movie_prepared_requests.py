"""Movie NoB and explicit-B preparation through existing public entry points."""
import unittest
from src.experiments import lotus_records_movie as movie, movie_records_session as session
from tests.experiments.movie_token_resident_fixture import MovieTokenResidentFixture, query_close, query_calls


class PreparedRequestEntryTests(unittest.TestCase):
    def test_no_B_preparation_and_dynamic_feedback_use_normal_and_resident_entries(self):
        for mode in ('interleaved', 'event_driven'):
            with self.subTest(mode=mode):
                fixture = MovieTokenResidentFixture(self)
                config = fixture.helper.rolling_config()
                config.pop('prepared_batching')
                config['preparation_mode'] = mode
                config['core_profile'] = dict(mode='async', max_inflight='auto', admission=dict(kind='resource_feedback'))
                for field in ('max_model_len', 'max_num_seqs', 'max_num_batched_tokens'):
                    config['engine'].pop(field, None)
                parsed = movie.read_config(fixture.helper.save(config))[0]
                self.assertIsNone(movie._record_batching_options(parsed)['prepared_batching'])
                plan = fixture.plan('RD')
                plan.pop('prepared_batching')
                plan['profile']['max_inflight'] = 'auto'
                plan['engine'] = config['engine']
                plan['preparation_mode'] = mode
                checked = session.check_plan(plan)
                self.assertIsNone(checked['record_batching']['B'])
                self.assertEqual(checked['record_batching']['request_feedback_kind'], 'resource_feedback')
                self.assertEqual(len(fixture.runtime.async_actors), 0)
                report = fixture.run(plan)
                self.assertEqual(report['status'], 'completed')
                self.assertEqual(sum(e['event']=='construct' for e in fixture.events()), 1)
                self.assertEqual(sum(e['event']=='close' for e in fixture.events()), 1)
                for query in report['queries']:
                    close = query_close(query)
                    self.assertTrue(close['query_completed'])
                    self.assertIsNone(close['async_token_batches']['policy'])
                    self.assertEqual(close['core_usage']['held_tasks'], 0)
                    self.assertEqual(query['backend_report']['result'], {'reviewId': ['1']})
                    self.assertEqual(len(query_calls(query)), 2)

    def test_same_public_resident_factory_runs_two_queries_and_keeps_payloads(self):
        results = {}
        for no_B in (False, True):
            with self.subTest(no_B=no_B):
                fixture = MovieTokenResidentFixture(self)
                plan = fixture.plan('RF')
                if no_B:
                    plan.pop('prepared_batching')
                checked = session.check_plan(plan)
                self.assertEqual(checked['expected_calls'], 4)
                self.assertEqual(checked['record_batching']['B'] is None, no_B)
                report = fixture.run(plan)
                self.assertEqual(report['status'], 'completed')
                self.assertEqual(sum(e['event']=='construct' for e in fixture.events()), 1)
                self.assertEqual(sum(e['event']=='close' for e in fixture.events()), 1)
                normalized = []
                for q in report['queries']:
                    close = query_close(q)
                    self.assertTrue(close['query_completed'])
                    self.assertEqual(close['core_usage']['held_tasks'], 0)
                    self.assertEqual(close['async_token_batches']['policy'] is None, no_B)
                    normalized.append(dict(result=q['backend_report']['result'],
                        calls=[{k:c[k] for k in ('call_id', 'row_id', 'fixture_messages', 'fixture_generation', 'status', 'text')}
                               for c in query_calls(q)]))
                results[no_B] = normalized
        self.assertEqual(results[False], results[True])


if __name__ == '__main__':
    unittest.main()
