"""Allocation-scoped sampling and consumption, using CPU counters as fixtures."""

from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.infrastructure.cpu_allocation import CpuCapacity
from src.observability.metrics.resources import AllocatedCPUCounterSource, HostCPUCounterSource
from src.observability.records_telemetry import MetricSources


class AllocatedResourceTests(unittest.TestCase):
    def observe(self, usages, capacities=None, times=None):
        capacities = capacities or [CpuCapacity(64, 2.5, 2.5, "observed_cgroup_quota")] * len(usages)
        times = times or [1_000_000_000 + i * 1_000_000_000 for i in range(len(usages))]
        with ExitStack() as stack:
            stack.enter_context(patch("src.infrastructure.cpu_allocation.observe_cpu_capacity", side_effect=capacities))
            stack.enter_context(patch("src.infrastructure.cgroup.controller_hierarchy", return_value=(2, (Path("/fixture"),))))
            stack.enter_context(patch("src.infrastructure.cgroup.cpu_usage_seconds", side_effect=usages))
            stack.enter_context(patch("time.monotonic_ns", side_effect=[t for t in times for _ in range(2)]))
            stack.enter_context(patch("psutil.cpu_times", side_effect=AssertionError("host counters cannot replace cgroup usage")))
            source = AllocatedCPUCounterSource()
            return [source() for _ in usages]

    def test_fractional_cpu_usage_is_normalized_to_quota_not_host_cpu_count(self):
        first, second = self.observe([10, 12])
        self.assertEqual(first["cpu_metrics_status"], "cpu_percent_baseline_priming")
        self.assertIsNone(first["cpu_allocation_utilization_pct"])
        self.assertEqual(second["cpu_allocation_utilization_pct"], 80)
        self.assertEqual(second["cpu_allocation_busy_cores"], 2)
        self.assertIsNone(second["host_cpu_per_core_max_pct"])
        self.assertEqual(second["cpu_percent_interval_s"], 1)

    def test_real_idle_is_zero_but_missing_is_never_zero(self):
        _, idle = self.observe([10, 10])
        self.assertEqual(idle["cpu_allocation_utilization_pct"], 0)
        missing, priming = self.observe([PermissionError("fixture"), 10])
        self.assertIsNone(missing["cpu_allocation_utilization_pct"])
        self.assertTrue(missing["cpu_metrics_status"].startswith("cpu_allocation_read_failed"))
        self.assertEqual(priming["cpu_metrics_status"], "cpu_percent_baseline_priming")

    def test_reset_clock_or_allocation_change_needs_new_evidence(self):
        _, reset, valid = self.observe([10, 1, 2])
        self.assertEqual(reset["cpu_metrics_status"], "cpu_counter_regressed")
        self.assertEqual(valid["cpu_allocation_utilization_pct"], 40)
        a = CpuCapacity(64, 2.5, 2.5, "observed_cgroup_quota")
        b = CpuCapacity(64, 1, 1, "observed_cgroup_quota")
        _, changed, valid = self.observe([10, 11, 12], [a, b, b])
        self.assertEqual(changed["cpu_metrics_status"], "cpu_allocation_changed")
        self.assertEqual(valid["cpu_allocation_utilization_pct"], 100)
        _, clock = self.observe([10, 11], times=[1_000_000_000, 1_000_000_000])
        self.assertIsNone(clock["cpu_allocation_utilization_pct"])
        self.assertEqual(clock["cpu_metrics_status"], "cpu_percent_nonpositive_interval")

    def test_unknown_quota_cannot_fall_back_to_low_host_cpu(self):
        value = self.observe([10], [CpuCapacity(64, None, None, "unavailable:cgroup_CPU_quota")])[0]
        self.assertIsNone(value["cpu_allocation_utilization_pct"])
        self.assertIsNone(value["host_cpu_per_core_max_pct"])

    def test_shared_parent_CPU_quota_includes_sibling_usage(self):
        with patch("src.infrastructure.cpu_allocation.observe_cpu_capacity", return_value=CpuCapacity(64, 2.5, 2.5, "observed")), \
                patch("src.infrastructure.cgroup.controller_hierarchy", return_value=(2, (Path("/fixture/job"), Path("/fixture")))), \
                patch("src.infrastructure.cgroup.cpu_quota", return_value=2.5), \
                patch("src.infrastructure.cgroup.cpu_usage_seconds", side_effect=[0, 0, .5, 2.4]), \
                patch("time.monotonic_ns", side_effect=[0, 0, 1_000_000_000, 1_000_000_000]):
            source = AllocatedCPUCounterSource()
            source()
            value = source()
        self.assertEqual(value["cpu_allocation_busy_cores"], .5)
        self.assertEqual(value["cpu_allocation_utilization_pct"], 96)
        self.assertEqual(value["cpu_quota_ancestor_peak_pct"], 96)

    def test_affinity_scoped_host_context_ignores_busy_unallocated_cores(self):
        def row(user=0, idle=0):
            return SimpleNamespace(_asdict=lambda: dict(user=user, idle=idle))
        with patch("psutil.cpu_times", side_effect=[
                [row(), row()], [row(user=10), row(idle=10)]]), \
                patch("time.monotonic_ns", side_effect=[0, 0, 1_000_000_000, 1_000_000_000]):
            source = HostCPUCounterSource(cpu_ids=(1,))
            source()
            value = source()
        self.assertEqual(value["host_cpu_per_core_max_pct"], 0)
        self.assertEqual(value["host_cpu_busy_cores"], 0)

    def test_telemetry_keeps_cgroup_cpu_and_memory_separate_from_host_context(self):
        raw = dict(cpu_allocation_busy_cores=2, cpu_allocation_utilization_pct=80,
            cpu_observation_scope="current_cgroup_per_effective_CPU", cpu_effective_slots=2.5,
            cpu_metrics_status="observed", host_memory_available_mib=100000)
        memory = dict(memory_allocation_status="observed", memory_allocation_headroom_bytes=1024)
        with patch("src.observability.records_telemetry.host_metadata", return_value=raw), \
                patch("src.observability.records_telemetry.gpu_metadata", return_value={}), \
                patch("src.infrastructure.cgroup.memory_observation", return_value=memory):
            sample = MetricSources(("GPU-fixture",))()
        self.assertEqual(sample["values"]["cpu_allocation_utilization_pct"], 80)
        self.assertEqual(sample["sources"]["host"]["memory_allocation_headroom_bytes"], 1024)
        self.assertEqual(sample["values"]["host_memory_available_mib"], 100000)

    def test_feedback_uses_scoped_cpu_and_does_not_fall_back_when_it_is_missing(self):
        from src.execution_provider.adapters.lotus_chain_token_batches import BatchSupplyFeedback
        for scoped in (80, None):
            packet = dict(sample=dict(values=dict(host_cpu_per_core_max_pct=5), sources=dict(host=dict(
                cpu_observation_scope="current_cgroup_per_effective_CPU", cpu_percent_interval_s=.1,
                cpu_allocation_utilization_pct=scoped))))
            view = BatchSupplyFeedback._resource_view(packet, request_index=1, requested_at_s=1, received_at_s=2)
            self.assertEqual(view.host_peak_cpu_pct, scoped)
