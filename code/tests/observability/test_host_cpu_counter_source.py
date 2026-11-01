"""Host CPU counter intervals owned by one collector, without hardware/model work."""
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.observability.metrics.resources import HostCPUCounterSource, host_metadata
from src.observability.records_telemetry import MetricSources


Times = namedtuple('Times', 'user nice system idle iowait irq softirq steal guest guest_nice')


def counters(user=10., idle=90., **fields):
    values = dict.fromkeys(Times._fields, 0.)
    values.update(user=user, idle=idle, **fields)
    return Times(**values)


class CrossThreadMetricSourcesTests(unittest.TestCase):
    def test_distinct_live_snapshot_threads_share_the_same_counter_baseline(self):
        first_done, second_done = threading.Event(), threading.Event()
        clock, samples, outputs, errors = [1_000_000_000], [], {}, []

        def read(*, percpu):
            self.assertTrue(percpu)
            samples.append(threading.get_ident())
            clock[0] = 1_000_000_000 + (len(samples)-1)*500_000_000
            return [counters()] if len(samples) == 1 else [counters(user=15., idle=95.)]

        def first():
            try:
                outputs['first'] = source()
                first_done.set()
                if not second_done.wait(3):
                    raise AssertionError('second snapshot did not finish')
            except BaseException as error:
                errors.append(error)
                first_done.set()

        def second():
            try:
                if not first_done.wait(3):
                    raise AssertionError('first snapshot did not finish')
                outputs['second'] = source()
            except BaseException as error:
                errors.append(error)
            finally:
                second_done.set()

        with patch('src.observability.records_telemetry.gpu_metadata', return_value={}), \
                patch('psutil.cpu_percent', return_value=[20.]), \
                patch('psutil.cpu_times', side_effect=read), \
                patch('psutil.virtual_memory', return_value=SimpleNamespace(percent=40, available=1024**3)), \
                patch('time.monotonic_ns', side_effect=lambda: clock[0]):
            source = MetricSources(('GPU-fixture',))
            a, b = threading.Thread(target=first), threading.Thread(target=second)
            a.start(); b.start()
            a.join(4); b.join(4)
        self.assertFalse(a.is_alive() or b.is_alive())
        self.assertEqual(errors, [])
        self.assertIsNone(outputs['first']['values']['host_cpu_per_core_max_pct'])
        self.assertEqual(outputs['second']['values']['host_cpu_per_core_max_pct'], 50., outputs)
        self.assertEqual(len(set(samples)), 2)
        self.assertEqual(outputs['second']['sources']['host']['cpu_percent_interval_s'], .5)


class HostCPUCounterSourceTests(unittest.TestCase):
    def observe(self, readings, times=None):
        times = times if times is not None else [1_000_000_000+i*500_000_000 for i in range(len(readings))]
        with patch('psutil.cpu_times', side_effect=readings) as read, \
                patch('time.monotonic_ns', side_effect=[t for t in times for _ in range(2)]):
            source = HostCPUCounterSource()
            values = [source() for _ in readings]
        self.assertEqual(read.call_args.kwargs, {'percpu': True})
        return values

    def assert_unknown(self, value, reason):
        self.assertIsNone(value['host_cpu_busy_cores'])
        self.assertIsNone(value['host_cpu_per_core_max_pct'])
        self.assertEqual(value['cpu_metrics_status'], reason)

    def test_priming_and_counter_deltas_exclude_duplicate_guest_and_idle_iowait(self):
        before = counters(user=100, nice=10, system=20, idle=600, iowait=30, guest=20, guest_nice=1)
        after = counters(user=110, nice=12, system=24, idle=650, iowait=40, guest=25, guest_nice=2)
        first, second = self.observe([[before, counters()], [after, counters(idle=100)]])
        self.assert_unknown(first, 'cpu_percent_baseline_priming')
        self.assertIsNone(first['cpu_percent_interval_s'])
        self.assertEqual(second['cpu_metrics_status'], 'observed')
        self.assertEqual(second['host_cpu_per_core_max_pct'], 21.1)
        self.assertAlmostEqual(second['host_cpu_busy_cores'], .211)
        self.assertEqual(second['cpu_percent_interval_s'], .5)
        self.assertEqual(second['cpu_baseline_scope'], 'sampler_instance')
        self.assertEqual(second['cpu_counter_sample_sequence'], 2)

    def test_legacy_reader_is_unchanged_but_stateful_reader_never_calls_cpu_percent(self):
        with patch('psutil.cpu_percent', return_value=[10]) as legacy, \
                patch('psutil.cpu_times', return_value=[counters()]), \
                patch('psutil.virtual_memory', return_value=SimpleNamespace(percent=40, available=1024**3)):
            self.assertEqual(host_metadata()['host_cpu_busy_cores'], .1)
            a, b = HostCPUCounterSource(), HostCPUCounterSource()
            for source in (a, b):
                value = host_metadata(cpu_source=source, include_status=True)
                self.assert_unknown(value, 'cpu_percent_baseline_priming')
                self.assertEqual(value['host_memory_used_pct'], 40)
                self.assertEqual(value['cpu_counter_sample_sequence'], 1)
        self.assertEqual(legacy.call_count, 1)

    def test_zero_cpu_counter_progress_is_unknown_but_real_idle_is_measured_zero(self):
        _, stalled, idle = self.observe([[counters()], [counters()], [counters(idle=100)]])
        self.assert_unknown(stalled, 'cpu_counter_delta_invalid')
        self.assertEqual(idle['host_cpu_busy_cores'], 0)
        self.assertEqual(idle['host_cpu_per_core_max_pct'], 0)
        self.assertEqual(idle['cpu_metrics_status'], 'observed')

    def test_counter_regression_reanchors_without_negative_or_clipped_idle_result(self):
        _, reset, recovered = self.observe([[counters()], [counters(user=5)], [counters(user=10, idle=95)]])
        self.assert_unknown(reset, 'cpu_counter_regressed')
        self.assertEqual(reset['cpu_percent_interval_s'], .5)
        self.assertEqual(recovered['host_cpu_per_core_max_pct'], 50)

    def test_invalid_counters_or_read_failure_clear_the_baseline(self):
        for bad in ([], [counters(user=-1)], [counters(user=float('nan'))],
                    [counters(user=float('inf'))], [counters(user=True)], PermissionError('fixture')):
            with self.subTest(bad=bad):
                _, invalid, priming, valid = self.observe([
                    [counters()], bad, [counters()], [counters(user=15, idle=95)]])
                self.assertIsNone(invalid['host_cpu_per_core_max_pct'])
                self.assertTrue(invalid['cpu_metrics_status'].startswith('cpu_counter_read_failed:'))
                self.assert_unknown(priming, 'cpu_percent_baseline_priming')
                self.assertEqual(valid['host_cpu_per_core_max_pct'], 50)

    def test_cpu_count_or_counter_schema_change_reanchors(self):
        for change in ([counters(), counters()], [SimpleNamespace(_asdict=lambda: {'user': 10, 'idle': 90})]):
            with self.subTest(change=change):
                _, changed, valid = self.observe([[counters()], change, [
                    SimpleNamespace(_asdict=lambda row=row: {name: value+5 for name, value in row._asdict().items()})
                    for row in change]])
                self.assert_unknown(changed, 'cpu_counter_shape_changed')
                self.assertEqual(valid['cpu_metrics_status'], 'observed')

    def test_nonpositive_interval_remains_raw_unknown_then_recovers(self):
        for end in (1_000_000_000, 500_000_000):
            with self.subTest(end=end):
                _, invalid, valid = self.observe([
                    [counters()], [counters(user=15, idle=95)], [counters(user=20, idle=100)]],
                    [1_000_000_000, end, end+500_000_000])
                self.assert_unknown(invalid, 'cpu_percent_nonpositive_interval')
                self.assertEqual(invalid['cpu_percent_interval_s'], (end-1_000_000_000)/1e9)
                self.assertEqual(valid['cpu_percent_interval_s'], .5)
                self.assertEqual(valid['host_cpu_per_core_max_pct'], 50)

    def test_clock_reversal_during_read_clears_the_baseline(self):
        with patch('psutil.cpu_times', return_value=[counters()]), \
                patch('time.monotonic_ns', side_effect=[10, 9, 11, 12]):
            source = HostCPUCounterSource()
            self.assert_unknown(source(), 'cpu_counter_clock_invalid')
            self.assert_unknown(source(), 'cpu_percent_baseline_priming')

    def test_long_interval_is_retained_at_source_then_next_fresh_interval_recovers(self):
        _, old, fresh = self.observe([[counters()], [counters(user=15, idle=95)],
                                     [counters(user=20, idle=100)]], [0, 2_000_000_000, 2_500_000_000])
        self.assertEqual(old['cpu_percent_interval_s'], 2)
        self.assertEqual(old['cpu_metrics_status'], 'observed')
        self.assertEqual(fresh['cpu_percent_interval_s'], .5)
        self.assertEqual(fresh['host_cpu_per_core_max_pct'], 50)

    def test_concurrent_reads_serialize_counter_timestamp_and_baseline_transaction(self):
        source = HostCPUCounterSource()
        first_read, second_attempt, release = threading.Event(), threading.Event(), threading.Event()
        attempts, clock, calls = [0], [1_000_000_000], []
        lock = source._lock

        class WitnessLock:
            def __enter__(self):
                attempts[0] += 1
                if attempts[0] == 2:
                    second_attempt.set()
                lock.acquire()
            def __exit__(self, *_):
                lock.release()

        source._lock = WitnessLock()

        def read(*, percpu):
            calls.append(threading.get_ident())
            if len(calls) == 1:
                first_read.set()
                if not release.wait(3):
                    raise AssertionError('counter read was not released')
                return [counters()]
            clock[0] += 500_000_000
            return [counters(user=15, idle=95)]

        with patch('psutil.cpu_times', side_effect=read), \
                patch('time.monotonic_ns', side_effect=lambda: clock[0]), ThreadPoolExecutor(2) as pool:
            first = pool.submit(source)
            try:
                self.assertTrue(first_read.wait(3))
                second = pool.submit(source)
                self.assertTrue(second_attempt.wait(3))
                self.assertEqual(len(calls), 1, 'the second CPU read must wait outside the transaction')
            finally:
                release.set()
            a, b = first.result(3), second.result(3)
        self.assert_unknown(a, 'cpu_percent_baseline_priming')
        self.assertEqual((b['cpu_counter_sample_sequence'], b['cpu_percent_interval_s']), (2, .5))
        self.assertEqual(b['host_cpu_per_core_max_pct'], 50)
        self.assertEqual(len(set(calls)), 2)


if __name__ == '__main__':
    unittest.main()
