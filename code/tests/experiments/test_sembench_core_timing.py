"""Complete relation timestamps precede final Core and Ray resource cleanup."""
from dataclasses import replace
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.experiments import sembench_core_operator as adapter
from src.experiments import sembench_text_query as entry
from src.execution_provider.adapters.records_async_model import AsyncCoreModelActor
from tests.experiments import test_sembench_core_operator as fixtures


class CoreQueryTimingTests(unittest.TestCase):
    def setUp(self):
        fixture = fixtures.CoreOperatorTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.fixture = fixture
        self.operation = fixture.operation()
        self.clock = 0
        self.events = []
        self.close_error = False
        self.sink_error = False

    def query(self):
        test = self
        operation = self.operation
        class Query:
            query_id = "timing-query"
            operation = test.operation
            def describe(self):
                return {'expected_model_calls': len(operation.records)}
            def execute(self, execute):
                test.events.append('source')
                test.clock += 2
                report = execute(operation, None)
                test.assertNotIn('close', test.events)
                test.events.append('relation')
                test.clock += 3
                if test.sink_error:
                    raise ValueError('sink failed')
                # Final relation work belongs to the query, not the Map output.
                return report['frame'].iloc[::-1].head(2).copy(), report
            def evaluate(self, frame):
                test.events.append('quality')
                test.clock += 13
                return {'rows': len(frame)}
        return Query()

    def owner(self):
        parent, seen = fixtures.cpu_owner(['zero', 'one', 'two'])
        test = self
        class Owner(parent):
            def close(self):
                test.events.append('close')
                test.clock += 7
                report = super().close()
                if test.close_error:
                    raise RuntimeError('close failed')
                return report
        return Owner, seen

    def run_query(self):
        fixture = self.fixture
        owner, _ = self.owner()
        clock = SimpleNamespace(monotonic=lambda: self.clock,
                                monotonic_ns=lambda: self.clock * 1_000_000_000)
        with patch.object(adapter, 'SemLoomRecordChainCore', owner), \
                patch.object(entry, 'time', clock), \
                patch.object(type(fixture.engine), 'validate_driver_environment', return_value={}):
            return entry.run_query({'backend': 'semloom-core', 'profile': {'execution': 'fused'}},
                self.query(), fixture.engine, fixture.resources, fixture.root)

    def test_complete_relation_has_independent_clock_before_close_and_quality(self):
        report = self.run_query()
        self.assertEqual(self.events, ['source', 'relation', 'close', 'quality'])
        timing = report['memory_sink_timing']
        self.assertEqual(timing['source_start_ns'], 0)
        self.assertEqual(timing['result_ready_ns'], 5_000_000_000)
        self.assertEqual(timing['seconds'], 5)
        self.assertEqual(report['query_seconds'], 12)
        self.assertEqual(report['evaluation_seconds'], 13)
        self.assertEqual(report['result']['index'], [11, 7])
        self.assertEqual(report['operator']['backend_close']['status'], 'closed')
        self.assertEqual(report['model_calls'], 3)

    def test_cleanup_failure_preserves_sink_without_success(self):
        self.close_error = True
        with self.assertRaisesRegex(RuntimeError, 'close failed') as caught:
            self.run_query()
        report = caught.exception.query_report
        self.assertEqual(report['status'], 'failed')
        self.assertEqual(report['memory_sink_timing']['seconds'], 5)
        self.assertEqual(report['memory_sink_timing']['result_ready_ns'], 5_000_000_000)
        self.assertEqual(report['operator']['backend_close']['status'], 'unknown')
        self.assertNotIn('quality', self.events)

    def test_relation_failure_has_no_sink_but_still_closes(self):
        self.sink_error = True
        with self.assertRaisesRegex(ValueError, 'sink failed') as caught:
            self.run_query()
        report = caught.exception.query_report
        self.assertEqual(report['status'], 'failed')
        self.assertIsNone(report['memory_sink_timing']['result_ready_ns'])
        self.assertIsNone(report['memory_sink_timing']['seconds'])
        self.assertEqual(self.events, ['source', 'relation', 'close'])

    def test_scope_keeps_real_core_alive_until_caller_materializes_sink(self):
        fixture = self.fixture
        owner, seen = self.owner()
        with patch.object(adapter, 'SemLoomRecordChainCore', owner), \
                patch.object(type(fixture.engine), 'validate_driver_environment', return_value={}):
            with adapter.query_scope(self.operation, engine=fixture.engine,
                    resources=fixture.resources, query_id='scope',
                    profile={'execution': 'fused'}, control_dir=fixture.root) as report:
                self.assertNotIn('close', self.events)
                self.assertEqual(report['frame']['_map'].tolist(), ['zero', 'one', 'two'])
            self.assertEqual(self.events, ['close'])
            self.assertEqual(len(seen), 3)
            self.assertEqual(report['backend_close']['status'], 'closed')
            self.assertEqual(set(report['backend_close']['core_usage'].values()), {0})


    def test_record_local_scope_closes_child_and_ray_only_after_full_sink(self):
        fixture = self.fixture
        operation = self.operation
        path = fixture.root / 'responses.json'
        path.write_text(json.dumps({'responses': {
            operation.stage.stage_id + '|' + row.row_id: dict(text=str(row.source_position))
            for row in operation.records}}))
        engine = replace(fixture.engine, model_path=str(path))
        resources = replace(fixture.resources, prepare_batch_rows=1, model_batch_rows=1,
                            max_active_batches=2)
        with fixtures.cold_threaded_runtime() as runtime, \
                patch.object(AsyncCoreModelActor, '_process_type', return_value=fixtures.CPUResourceProcess):
            with adapter.query_scope(operation, engine=engine, resources=resources,
                    query_id='deferred-records', profile={'execution': 'record_local_sections'},
                    control_dir=fixture.root) as report:
                self.assertTrue(runtime.ray.is_initialized())
                self.assertEqual(report['frame']['_map'].tolist(), ['0', '1', '2'])
                events = [json.loads(line) for line in
                          Path(str(path) + '.events').read_text().splitlines()]
                self.assertNotIn('close', [event['event'] for event in events])
            self.assertFalse(runtime.ray.is_initialized())
            self.assertEqual(report['backend_close']['status'], 'closed')
            self.assertTrue(report['backend_close']['query_completed'])
            self.assertEqual(set(report['backend_close']['core_usage'].values()), {0})
        events = [json.loads(line) for line in Path(str(path) + '.events').read_text().splitlines()]
        self.assertEqual(sum(event['event'] == 'close' for event in events), 1)


if __name__ == '__main__':
    unittest.main()
