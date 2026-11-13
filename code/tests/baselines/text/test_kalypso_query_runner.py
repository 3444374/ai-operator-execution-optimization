"""CPU-only lifecycle checks; original SDK/operator parity is tested separately."""
import ast
from contextlib import contextmanager
import json
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch


SOURCE = Path(__file__).parents[3] / 'src/baselines/text/frameworks/kalypso_native/query_runner.py'


class QueryLifecycleTests(unittest.TestCase):
    def runner(self, *, fail=False):
        events = []
        receipts = []
        states = []

        @contextmanager
        def phase(timing, name):
            events.append(name)
            yield

        class Owner:
            def __init__(self, output):
                self.output = output
                self.unit = {'queries': [{'query_id': 'first'}, {'query_id': 'second'}]}
                self.next_query = 0
                self.cfg = {}
                self.model_ready_observed_ns = 7

            @contextmanager
            def query_evidence(self, *args, **kwargs):
                receipt = ({}, {}, {})
                receipts.append(receipt)
                yield receipt
                events.append('persist')

        class Operation:
            kind = 'filter'
            source_metadata = {'native_policy': 'unchanged'}

            def __init__(self, *args):
                states.append(self)

            def check_query(self, query):
                events.append('check')

            def execute(self, operation, invoke):
                events.append('native')
                if fail:
                    raise RuntimeError('native failure')
                return ['native result']

            def audit(self):
                events.append('audit')

            def quality_metadata(self):
                return {}

            def report_metadata(self, source):
                return {'physical_native_calls': 1}

        class Query:
            def bind_execution(self):
                events.append('bind')

                def execute(callback):
                    events.append('source')
                    rows = callback(None, None)
                    events.append('original_relation')
                    return rows + ['relational result'], None
                return execute

            def verify_source(self):
                events.append('verify')

            def describe(self):
                return {'query': 'original'}

            def evaluate(self, frame):
                events.append('evaluate')
                return {'rows': len(frame)}

        # Compile the real lifecycle class without importing the optional SDK.
        tree = ast.parse(SOURCE.read_text())
        cls = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == 'KalypsoRunner')
        namespace = dict(time=time, native=SimpleNamespace(NativeQueryDriver=Owner, query_phase=phase),
                         make_query=lambda *args, **kwargs: Query(), result_value=lambda frame: frame,
                         operator_handler=lambda spec: Operation)
        exec(compile(ast.Module(body=[cls], type_ignores=[]), str(SOURCE), 'exec'), namespace)
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        return namespace['KalypsoRunner'](Path(self.temporary.name)), events, receipts, states

    def test_complete_relation_precedes_sink_and_evaluation_persistence_follow(self):
        runner, events, receipts, states = self.runner()
        report = runner.query(runner.unit['queries'][0], events.append)
        self.assertEqual(report['result'], ['native result', 'relational result'])
        self.assertEqual(report['quality'], {'rows': 2})
        self.assertLess(events.index('original_relation'), events.index('memory_sink_ready'))
        self.assertLess(events.index('memory_sink_ready'), events.index('audit'))
        self.assertLess(events.index('memory_sink_ready'), events.index('evaluate'))
        self.assertLess(events.index('evaluate'), events.index('persist'))
        timing = receipts[0][2]
        self.assertLessEqual(timing['source_started_ns'], timing['memory_sink_ready_ns'])
        self.assertEqual(timing['model_ready_before_source_observed_ns'], 7)

    def test_one_owner_can_run_more_than_one_query_with_fresh_operator_state(self):
        runner, events, receipts, states = self.runner()
        for spec in runner.unit['queries']:
            runner.query(spec, events.append)
        self.assertEqual(runner.next_query, 2)
        self.assertEqual(len(receipts), 2)
        self.assertIsNot(states[0], states[1])

    def test_failed_native_query_has_no_success_sink_or_quality(self):
        runner, events, receipts, states = self.runner(fail=True)
        with self.assertRaisesRegex(RuntimeError, 'native failure'):
            runner.query(runner.unit['queries'][0], events.append)
        self.assertNotIn('memory_sink_ready', events)
        self.assertNotIn('evaluate', events)
        self.assertNotIn('status', receipts[0][0])


class PostSinkEvidenceTests(unittest.TestCase):
    def assert_complete_result_survives(self, failed_phase):
        import pandas as pd
        from src.baselines.text.frameworks.kalypso_native import adapter, query_runner
        from src.infrastructure.private_artifacts import write_private_json

        failure = (RuntimeError('trace audit failed') if failed_phase == 'trace'
                   else ValueError('source snapshot changed'))
        evaluated = []

        class Query:
            def bind_execution(self):
                def execute(callback):
                    report = callback(None, None)
                    final = pd.concat([report['frame'], pd.DataFrame({'value': ['relational result']})],
                                      ignore_index=True)
                    return final, report
                return execute

            def verify_source(self):
                if failed_phase == 'source':
                    raise failure

            def describe(self):
                return {'query': 'original'}

            def evaluate(self, frame):
                evaluated.append(frame)
                return {'should_not_be_published': True}

        class Operation:
            kind = 'map'
            source_metadata = {}

            def __init__(self, *args):
                pass

            def check_query(self, query):
                pass

            def execute(self, *args):
                return {'frame': pd.DataFrame({'value': ['native result']})}

            def audit(self):
                if failed_phase == 'trace':
                    raise failure

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)/'run'
            spec = {'query_id': 'query'}
            with patch.object(adapter, 'load_dependencies', return_value=write_private_json), \
                    patch.object(query_runner, 'make_query', return_value=Query()), \
                    patch.object(query_runner, 'operator_handler', return_value=Operation), \
                    patch('socket.socket.connect', side_effect=AssertionError('fixture attempted network')):
                driver = query_runner.KalypsoRunner({}, {'queries': [spec]}, output,
                    transport=Mock(side_effect=AssertionError('fixture attempted native HTTP')))
                driver.close = Mock(side_effect=AssertionError('query closed externally owned model'))
                events = []
                with self.assertRaises(type(failure)) as caught:
                    driver.query(spec, events.append)
            self.assertIs(caught.exception, failure)
            report = caught.exception.query_report
            self.assertEqual(report['status'], 'failed')
            self.assertEqual(report['failure_type'], type(failure).__name__)
            self.assertEqual(json.loads((output/'query/result.json').read_text()), {
                'index': [0, 1], 'columns': ['value'], 'data': [['native result'], ['relational result']]})
            self.assertFalse((output/'query/quality.json').exists())
            self.assertEqual(evaluated, [])
            self.assertIn('memory_sink_ready', events)
            self.assertNotIn('result_ready', events)
            timing = json.loads((output/'query/memory-sink-timing.json').read_text())
            self.assertLessEqual(timing['memory_sink_ready_ns'], timing['phases']['result_encoding']['started_ns'])
            self.assertLessEqual(timing['phases']['result_encoding']['finished_ns'],
                                 timing['phases']['trace_audit']['started_ns'])
            self.assertEqual(timing['trace_audit_status'], 'failed' if failed_phase == 'trace' else 'passed')
            driver.close.assert_not_called()
            driver.transport.assert_not_called()

    def test_trace_failure_preserves_complete_result_without_success_quality(self):
        self.assert_complete_result_survives('trace')

    def test_source_failure_preserves_complete_result_without_success_quality(self):
        self.assert_complete_result_survives('source')


if __name__ == '__main__':
    unittest.main()
