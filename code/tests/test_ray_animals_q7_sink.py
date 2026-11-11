"""Real handoff/files/threads with software Ray and host receipts; no GPU claim."""
from contextlib import ExitStack, nullcontext
import gzip
import json
import os
from pathlib import Path
import tempfile
import time
from types import ModuleType
import unittest
from unittest.mock import patch

import pandas as pd

from src.baselines.image.ray_animals_q7 import binding, runner


class Q7SinkTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='ray-q7-sink-cpu-')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.bound = dict(manifest='unused-fixture', checkout='unused-fixture', manifest_digest='fixture',
            native_python='not-executed', num_cpus=1, num_gpus=1, object_store_memory=1, model='not-loaded',
            host_adapter=dict(stage_timeout_s=2))
        self.actions, self.active, self.stage = [], set(), None
        self.native_failure = self.close_failure = self.after_sink_failure = self.parse_failure = None
        self.invalid_receipt = None
        self.result = pd.DataFrame({'City': ['fixture-city']})
        config = self.root / 'config.json'
        config.write_text(json.dumps(dict(runtime_profile='official-ray-0.22')))
        fake_ray = ModuleType('ray')

        def initialize(**kwargs):
            with gzip.open(self.root / f'input-{self.stage}.json.gz', 'rt') as stream:
                self.rows = json.load(stream)
            self.assertFalse(self.active, 'stage 1 cannot start before stage 0 was released')
            self.active.add(self.stage)
            self.actions.append(f'start-{self.stage}')

        def shutdown():
            self.actions.append(f'close-{self.stage}')
            self.active.discard(self.stage)
            if self.stage == 1 and self.close_failure is not None:
                raise self.close_failure

        def execute(rows, model):
            if self.stage == 1 and self.native_failure is not None:
                raise self.native_failure
            self.assertEqual(model, 'not-loaded')
            self.assertEqual(len(rows), 64)
            return [dict(call_id=row['call_id'], generated_text='True') for row in rows[::-1]]

        fake_ray.init, fake_ray.shutdown, fake_ray.is_initialized = initialize, shutdown, lambda: bool(self.active)
        native = ModuleType('src.baselines.image.ray_animals_q7.native_query')
        native.execute = execute
        stack = ExitStack()
        self.addCleanup(stack.close)
        for scope in (
            patch.dict('sys.modules', {'ray': fake_ray, native.__name__: native}),
            patch.dict(os.environ, Q7_CONFIG=str(config), RAY_TMPDIR=str(self.root)),
            patch('importlib.metadata.version', side_effect=lambda name: {'ray': '2.56.1', 'vllm': '0.22.0'}.get(name, 'fixture')),
            patch.object(runner, 'observed_builder', nullcontext),
            patch('src.baselines.image.sembench_animals.AnimalsDataset', side_effect=lambda *args: object()),
            patch.object(binding, 'run', self.original_query_fixture),
        ):
            stack.enter_context(scope)

    def original_query_fixture(self, dataset, execute, *, on_result_ready):
        two = binding.TwoPassBinding(execute)
        two(['fixture-image'] * 64)
        self.assertEqual(self.active, set())
        two(['fixture-image'] * 64)
        self.assertEqual(self.active, {1})
        if self.parse_failure is not None:
            raise self.parse_failure
        self.actions.append('original-full-sink')
        sink = time.monotonic_ns()
        on_result_ready(self.result, sink)
        self.assertFalse(self.active)
        self.actions.append('binding-audit')
        two.require_complete()
        if self.after_sink_failure is not None:
            raise self.after_sink_failure
        self.actions.append('evaluation')
        return self.result, {'fixture_only': True}, two.passes, dict(sink_ready_monotonic_ns=sink)

    def supervise(self, command, stage, root):
        self.stage = stage
        runner.native_stage(root, stage, self.bound)
        self.actions.append(f'host-cleanup-{stage}')
        return dict(worker_exit_zero=True, physical_idle=self.invalid_receipt != stage,
            resource_passed=True, owned_processes_exited=True, private_sockets_clear=True)

    def test_last_owner_stays_alive_until_full_sink_and_first_release_remains(self):
        report = runner.run_query(self.root, self.bound, self.supervise)
        self.assertEqual(self.actions, ['start-0', 'close-0', 'host-cleanup-0', 'start-1',
            'original-full-sink', 'close-1', 'host-cleanup-1', 'binding-audit', 'evaluation'])
        clock = report['timing']
        self.assertLessEqual(clock['query_entered_monotonic_ns'], clock['sink_ready_monotonic_ns'])
        self.assertLessEqual(clock['sink_ready_monotonic_ns'], clock['final_release_observed_monotonic_ns'])
        self.assertLessEqual(clock['final_release_observed_monotonic_ns'], clock['final_close_started_monotonic_ns'])
        self.assertLessEqual(clock['final_close_finished_monotonic_ns'], clock['serialization_started_monotonic_ns'])
        self.assertEqual(report['result'], self.result.to_dict(orient='split'))
        self.assertEqual(len(report['stage_lifecycle']), 2)
        self.assertEqual(report['logical_occurrences'], 128)
        self.assertEqual(report['retries'], 0)
        self.assertTrue((self.root / 'QUERY.json').is_file())

    def failed(self, expected):
        with self.assertRaises(type(expected)) as caught:
            runner.run_query(self.root, self.bound, self.supervise)
        self.assertIs(caught.exception, expected)
        self.assertFalse(self.active)
        self.assertFalse((self.root / 'QUERY.json').exists())
        report = json.loads((self.root / 'QUERY-failure.json').read_text())
        self.assertEqual(report['status'], 'failed')
        return report

    def test_native_failure_before_publication_stays_primary(self):
        self.native_failure = ValueError('native fixture failed')
        report = self.failed(self.native_failure)
        self.assertIsNone(report['timing']['sink_ready_monotonic_ns'])
        self.assertNotIn('original-full-sink', self.actions)

    def test_parser_failure_aborts_and_releases_final_owner(self):
        self.parse_failure = ValueError('original parser failed')
        report = self.failed(self.parse_failure)
        self.assertIsNone(report['timing']['sink_ready_monotonic_ns'])
        self.assertEqual(json.loads((self.root / runner.FINAL_RELEASE).read_text())['action'], 'abort')
        self.assertTrue((self.root / 'predictions-1.json').exists())
        self.assertTrue(any('supervision' in note for note in self.parse_failure.__notes__))

    def test_after_sink_audit_failure_keeps_sink_and_result(self):
        self.after_sink_failure = ValueError('post-sink binding audit failed')
        report = self.failed(self.after_sink_failure)
        self.assertIsNotNone(report['timing']['source_to_sink_seconds'])
        self.assertEqual(report['result'], self.result.to_dict(orient='split'))

    def test_close_failure_keeps_complete_relation_and_observed_sink(self):
        self.close_failure = RuntimeError('native Ray shutdown fixture failure')
        report = self.failed(self.close_failure)
        self.assertIsNotNone(report['timing']['sink_ready_monotonic_ns'])
        self.assertEqual(report['result'], self.result.to_dict(orient='split'))

    def test_unconfirmed_first_exit_stops_second_dispatch(self):
        self.invalid_receipt = 0
        with self.assertRaisesRegex(RuntimeError, 'physical exit unconfirmed'):
            runner.run_query(self.root, self.bound, self.supervise)
        self.assertNotIn('start-1', self.actions)

    def test_unconfirmed_final_exit_keeps_sink_but_fails_query(self):
        self.invalid_receipt = 1
        with self.assertRaisesRegex(RuntimeError, 'physical exit unconfirmed') as caught:
            runner.run_query(self.root, self.bound, self.supervise)
        self.assertIsNotNone(caught.exception.query_report['timing']['sink_ready_monotonic_ns'])
        self.assertFalse((self.root / 'QUERY.json').exists())
        self.assertFalse(self.active)

    def test_close_event_failure_cannot_skip_shutdown(self):
        original = runner.event

        def event(root, kind, **fields):
            if kind == 'public_ray_close_started' and fields.get('stage') == 1:
                raise OSError('close event fixture failure')
            return original(root, kind, **fields)

        with patch.object(runner, 'event', event):
            with self.assertRaisesRegex(OSError, 'close event fixture failure'):
                runner.run_query(self.root, self.bound, self.supervise)
        self.assertFalse(self.active)
        self.assertIn('close-1', self.actions)
        self.assertFalse((self.root / 'QUERY.json').exists())

    def test_empty_original_relation_still_has_sink(self):
        self.result = pd.DataFrame({'City': []})
        report = runner.run_query(self.root, self.bound, self.supervise)
        self.assertEqual(report['result']['data'], [])
        self.assertIsNotNone(report['timing']['source_to_sink_seconds'])

    def test_stale_release_rejected_before_dispatch(self):
        (self.root / runner.FINAL_RELEASE).write_text('{}')
        with self.assertRaisesRegex(ValueError, 'fresh query paths'):
            runner.run_query(self.root, self.bound, self.supervise)
        self.assertEqual(self.actions, [])

    def test_handoff_wait_has_a_finite_timeout(self):
        with self.assertRaisesRegex(TimeoutError, 'stage budget'):
            runner.wait_for_final_release(self.root, .01)

    def test_existing_supervisor_holds_a_real_CPU_child_until_sink(self):
        import sys
        import psutil
        from src.experiments.lotus_records_supervisor import supervise
        script = '''
import gzip,json,os,sys,time
from pathlib import Path
from src.baselines.image.ray_animals_q7.runner import wait_for_final_release
from src.infrastructure.private_artifacts import publish_private_json
root,stage=Path(sys.argv[1]),int(sys.argv[2])
with gzip.open(root/f'input-{stage}.json.gz','rt') as stream: rows=json.load(stream)
publish_private_json(root/f'pid-{stage}.json',{'pid':os.getpid()})
publish_private_json(root/f'predictions-{stage}.json',[dict(call_id=r['call_id'],generated_text='True') for r in rows])
if stage==1: wait_for_final_release(root,5)
begun=time.monotonic_ns()
publish_private_json(root/f'native-close-{stage}.json',dict(started_monotonic_ns=begun,finished_monotonic_ns=time.monotonic_ns(),disconnected=True))
'''
        processes = []

        def supervised(command, stage, root):
            report = supervise([sys.executable, '-B', '-c', script, str(root), str(stage)],
                root / f'CPU-supervision-{stage}', timeout_s=5, grace_s=1)
            processes.append(report)
            self.assertTrue(report['observed_owned_exit_confirmed'])
            # Physical GPU/Ray/resource predicates are explicit software fixtures.
            return dict(worker_exit_zero=report['worker_exit'] == 0, physical_idle=True,
                resource_passed=True, owned_processes_exited=True, private_sockets_clear=True,
                GPU_and_Ray_fixture_only=True)

        def query(dataset, execute, *, on_result_ready):
            two = binding.TwoPassBinding(execute)
            two(['CPU-image'] * 64)
            self.assertEqual(len(processes), 1)
            two(['CPU-image'] * 64)
            pid = json.loads((self.root / 'pid-1.json').read_text())['pid']
            self.assertTrue(psutil.pid_exists(pid))
            sink = time.monotonic_ns()
            on_result_ready(self.result, sink)
            self.assertFalse(psutil.pid_exists(pid))
            two.require_complete()
            return self.result, {'CPU_fixture_only': True}, two.passes, dict(sink_ready_monotonic_ns=sink)

        with patch.object(binding, 'run', query):
            report = runner.run_query(self.root, self.bound, supervised)
        self.assertEqual(report['status'], 'completed')
        self.assertEqual(len(processes), 2)


class OriginalBindingBoundaryTests(unittest.TestCase):
    def test_original_query_return_precedes_message_audit_and_evaluation(self):
        import lotus
        from types import SimpleNamespace
        from src.baselines.image import sembench_animals
        actions = []
        frame = pd.DataFrame({'city': []})
        dataset = SimpleNamespace(checkout='software-fixture', preflight=lambda query: None,
                                  frame=lambda **kwargs: None)
        digest = binding.message_digest

        def query(*args):
            for stage in range(2):
                lotus.settings.lm([f'fixture-message-{stage}-{i}' for i in range(64)])
            actions.append('original-return')
            return frame

        def summarize(message):
            actions.append('message-summary')
            return digest(message)

        def evaluate(*args):
            actions.append('evaluate')
            return {'software_fixture': True}

        def accept(result, timestamp):
            self.assertIs(result, frame)
            self.assertEqual(actions, ['original-return'])
            actions.append('sink')

        with patch.object(sembench_animals, 'run_original_query', query), \
             patch.object(sembench_animals, 'evaluate_original', evaluate), \
             patch.object(binding, 'message_digest', summarize):
            result, quality, passes, timing = binding.run(dataset,
                lambda rows: [dict(call_id=row['call_id'], generated_text='False') for row in rows[::-1]],
                on_result_ready=accept)
        self.assertEqual(actions, ['original-return', 'sink'] + ['message-summary'] * 128 + ['evaluate'])
        self.assertEqual([len(p['message_sha256']) for p in passes], [64, 64])
        self.assertLessEqual(timing['sink_ready_monotonic_ns'], timing['binding_audit_started_monotonic_ns'])
        self.assertLessEqual(timing['binding_audit_finished_monotonic_ns'], timing['evaluation_started_monotonic_ns'])


if __name__ == '__main__':
    unittest.main()
