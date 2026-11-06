"""Untimed query supervision still observes exit, cancellation and finite cleanup."""
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from src.experiments import lotus_records_supervisor as supervisor


class QuerySupervisionTests(unittest.TestCase):
    def test_omitted_duration_observes_exit_and_cancellation_while_explicit_duration_expires(self):
        for duration, cancel, expected in ((None, False, None), (60, False, TimeoutError),
                                           (None, True, InterruptedError)):
            clock = SimpleNamespace(now=100.)
            process = SimpleNamespace(pid=12345, returncode=None, stdout=Mock())
            def poll():
                if clock.now > 160 and not cancel:
                    process.returncode = 0
                return process.returncode
            process.poll = poll
            process.kill = Mock(side_effect=lambda: setattr(process, 'returncode', -9))
            process.wait = Mock(return_value=-9)
            output = Mock(pipe=process.stdout)
            output.report.return_value = dict(complete=True)
            monitor = Mock(side_effect=InterruptedError('caller cancelled') if cancel else None)
            with self.subTest(duration=duration, cancel=cancel), tempfile.TemporaryDirectory() as directory, \
                    patch.object(supervisor.subprocess, 'Popen', return_value=process), \
                    patch.object(supervisor.psutil, 'Process', return_value=SimpleNamespace(create_time=lambda: 1.)), \
                    patch.object(supervisor, '_RedactedOutput', return_value=output), \
                    patch.object(supervisor, '_observe'), patch.object(supervisor, '_live', return_value=[]), \
                    patch('time.monotonic', side_effect=lambda: clock.now), \
                    patch.object(supervisor.time, 'sleep', side_effect=lambda _: setattr(clock, 'now', 161.)):
                if expected is None:
                    result = supervisor.supervise(['cpu-fixture'], Path(directory), timeout_s=duration, monitor=monitor)
                    self.assertEqual(result['status'], 'passed')
                    self.assertIsNone(result['worker_deadline_monotonic'])
                    self.assertEqual(result['elapsed_s'], 61.)
                else:
                    with self.assertRaises(expected) as caught:
                        supervisor.supervise(['cpu-fixture'], Path(directory), timeout_s=duration, monitor=monitor)
                    result = caught.exception.report
                    self.assertEqual(result['status'], 'failed')
                    if cancel:
                        process.kill.assert_called_once_with()
                        process.wait.assert_called_once_with(timeout=5.)
                self.assertEqual(result['cleanup_reserved_s'], 5.)
                self.assertTrue(result['observed_owned_exit_confirmed'])

    def test_existing_reader_and_check_keep_null_duration_in_resolved_configuration(self):
        from src.experiments.movie_records_config import records_resources
        from src.experiments.movie_records_session import check_plan
        from tests.experiments.test_movie_records_session import PlanTests
        case = PlanTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        case.plan['queries'] = [dict(case.plan['queries'][0], phase='measured')]
        case.plan['resources'].pop('query_timeout_s', None)
        checked = check_plan(case.plan)
        self.assertEqual(checked['status'], 'checked_cpu_only')
        self.assertIsNone(records_resources(case.plan).query_timeout_s)
        explicit = dict(case.plan, resources=dict(case.plan['resources'], query_timeout_s=300))
        self.assertEqual(check_plan(explicit)['status'], 'checked_cpu_only')
        self.assertEqual(records_resources(explicit).query_timeout_s, 300)
        from dataclasses import asdict
        from src.experiments.calibration.core import build_core_identity
        case.plan['resources'] = asdict(records_resources(case.plan))
        case.plan['profile'] = dict(mode='async')
        case.plan['engine']['max_model_len'] = 8192
        identity = build_core_identity(case.plan, check_plan(case.plan),
            hardware=dict(machine_id='cpu-fixture', gpu_ids=['fixture'], gpu_driver='fixture',
                          cpu_limit=1, resource_limits=dict(cpu=1)),
            model=dict(model_revision='fixture', tokenizer_revision='fixture'),
            software=dict(source_revision='fixture', versions=dict(python='fixture')))
        self.assertIsNone(identity['execution']['resources']['query_timeout_s'])


if __name__ == '__main__':
    unittest.main()
