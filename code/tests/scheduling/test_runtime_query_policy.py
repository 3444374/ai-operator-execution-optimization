"""Public ordinary-query policy; exact cache, explicit exploration, real targets."""
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from src.scheduling.runtime import calibration as runtime
from tests.scheduling.test_runtime_calibration import Clock


class QueryPolicyTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(); self.addCleanup(temp.cleanup)
        self.root = Path(temp.name); self.clock = Clock()
        self.identity = dict(hardware=dict(cores=8), model=dict(revision="fixture-model"),
            generation=dict(fixed=True), execution=dict(max_num_seqs=64),
            workload=dict(inputs=[1, 2, 3], manifest="exact-one", instruction="square each input"),
            measurement=dict(objective="full_query_seconds", work_unit="rows"))
        self.candidates = [dict(workers=1), dict(workers=2)]
        self.measured, self.targets, self.delivered = [], [], 0

    def measure(self, configuration, *, repeat_index, deadline_monotonic, work_limit):
        self.measured.append((configuration["workers"], repeat_index, deadline_monotonic, work_limit))
        # Model-only duration would favor C1; complete-query duration favors C2.
        full_query = {1: 15, 2: 10}[configuration["workers"]]
        self.clock.advance(full_query + 2)  # Real tuning wall also includes extra callback work.
        return runtime.Measurement(full_query, "ok", True, 3)

    def execute(self, configuration, **limits):
        self.targets.append((deepcopy(configuration), limits))
        with ThreadPoolExecutor(max_workers=configuration["workers"]) as pool:
            result = list(pool.map(lambda value: value * value, self.identity["workload"]["inputs"]))
        self.delivered += len(result); self.clock.advance(7)
        return result

    def run_policy(self, **overrides):
        options = dict(default=dict(workers=1), cache_dir=self.root, clock=self.clock)
        options.update(overrides)
        return runtime.run_query(self.identity, self.candidates, self.execute, **options)

    def offline(self):
        return self.run_policy(mode="offline", measure=self.measure,
            budget=runtime.ScanBudget(1000, 18), work_per_sample=3)

    def test_default_miss_executes_declared_configuration_without_scanning_even_with_callback(self):
        value = self.run_policy(measure=lambda *a, **k: self.fail("implicit scan"),
            budget=runtime.ScanBudget(1000, 1000), default=dict(workers=3))
        self.assertEqual(value["result"], [1, 4, 9])
        self.assertEqual(value["configuration"], dict(workers=3))
        self.assertEqual(value["configuration_source"], "explicit_default")
        self.assertFalse(value["performance_tuned"])
        self.assertEqual(self.measured, []); self.assertEqual(len(self.targets), 1)
        self.assertEqual(list(self.root.iterdir()), [])
        self.assertEqual(value["target_wall_s"], 7)
        self.assertIsNone(value["request_wait_s"])

    def test_offline_only_populates_selection_and_matching_queries_execute_each_time(self):
        offline = self.offline()
        self.assertEqual(offline["status"], "calibrated")
        self.assertEqual(offline["configuration"], dict(workers=2))
        self.assertEqual(self.targets, [])
        for _ in range(2):
            value = self.run_policy(measure=lambda *a, **k: self.fail("cache hit scanned"))
            self.assertEqual(value["configuration_source"], "calibration_cache")
            self.assertEqual(value["result"], [1, 4, 9])
            self.assertTrue(value["performance_tuned"])
        self.assertEqual(len(self.measured), 6)
        self.assertEqual(self.delivered, 6)  # Parameter cache, never answer cache.

    def test_each_exact_identity_group_change_falls_back_without_relaxing_workload_match(self):
        self.offline(); original = deepcopy(self.identity)
        artifacts = {p.name: p.read_bytes() for p in self.root.iterdir()}
        for group in original:
            with self.subTest(group=group):
                self.identity = deepcopy(original)
                if group == "workload":
                    self.identity[group]["instruction"] = "different instruction, same inputs"
                else:
                    self.identity[group]["changed"] = True
                value = self.run_policy()
                self.assertEqual(value["configuration_source"], "explicit_default")
        self.assertEqual(len(self.measured), 6)
        self.assertEqual({p.name: p.read_bytes() for p in self.root.iterdir()}, artifacts)

    def test_search_or_model_capacity_change_misses_and_default_stays_within_declared_bound(self):
        search = runtime.IncreasingSearch("workers", dict(model_capacity=8))
        trace = {1: 20, 2: 10, 4: 5, 8: 2}
        def measure(c, **kw): return runtime.Measurement(trace[c["workers"]], "ok", True, 3)
        runtime.run_query(self.identity, [dict(workers=1)], mode="offline", default=dict(workers=1),
            cache_dir=self.root, measure=measure, budget=runtime.ScanBudget(100, 36),
            work_per_sample=3, search=search, clock=self.clock)
        for identity, changed_search in [(self.identity, runtime.IncreasingSearch("workers", dict(model_capacity=16))),
                (dict(self.identity, execution=dict(max_num_seqs=128)), search)]:
            value = runtime.run_query(identity, [dict(workers=1)], self.execute, default=dict(workers=2),
                cache_dir=self.root, search=changed_search, clock=self.clock)
            self.assertEqual(value["configuration_source"], "explicit_default")
        with self.assertRaises(ValueError):
            runtime.run_query(self.identity, [dict(workers=1)], self.execute, default=dict(workers=9),
                cache_dir=self.root, search=search, clock=self.clock)

    def test_unreadable_or_invalid_cache_is_optional_and_never_authorizes_bad_settings(self):
        saved = self.offline()["calibration"]["selection_path"]
        Path(saved).write_text("{invalid json")
        value = self.run_policy()
        self.assertEqual(value["configuration_source"], "explicit_default")
        self.assertEqual(value["calibration"]["reason"], "unusable_selection")
        with patch.object(runtime, "resolve_calibration", side_effect=PermissionError("fixture cache")):
            value = self.run_policy()
        self.assertEqual(value["configuration_source"], "explicit_default")
        self.assertEqual(len(self.measured), 6)

    def test_invalid_mode_identity_or_default_rejects_before_execution(self):
        for options in [dict(mode="automatic-tuned"), dict(default=dict(workers=0))]:
            with self.subTest(options=options), self.assertRaises(ValueError): self.run_policy(**options)
        self.identity.pop("hardware")
        with self.assertRaises(ValueError): self.run_policy()
        self.assertEqual(self.targets, [])

    def test_online_counts_real_tuning_overhead_and_same_clock_outside_start_in_total_wait(self):
        request_start = self.clock(); self.clock.advance(7)  # Caller-owned startup/warmup.
        value = self.run_policy(mode="online", measure=self.measure,
            budget=runtime.ScanBudget(1000, 21), work_per_sample=3,
            request_started_monotonic=request_start)
        self.assertEqual(value["configuration"], dict(workers=2))
        self.assertEqual(value["configuration_source"], "measured_online")
        self.assertEqual(value["calibration"]["scan_cost"]["reserved_work"], 18)
        self.assertEqual(value["scan_work_budget"], 18)
        self.assertEqual(value["target_work_limit"], 3)
        self.assertEqual(self.targets[0][1], dict(deadline_monotonic=1000, work_limit=3))
        self.assertAlmostEqual(value["calibration"]["scan_cost"]["wall_s"], 87)
        self.assertAlmostEqual(value["total_wall_s"], 94)
        self.assertAlmostEqual(value["request_wait_s"], 101)

    def test_online_cache_hit_can_use_target_only_budget_and_still_runs_full_query(self):
        self.offline()
        value = self.run_policy(mode="online", measure=lambda *a, **k: self.fail("hit scanned"),
            budget=runtime.ScanBudget(20, 3), work_per_sample=3)
        self.assertEqual(value["configuration_source"], "calibration_cache")
        self.assertEqual(value["calibration"]["scan_cost"]["reserved_work"], 0)
        self.assertEqual(self.delivered, 3)
        self.assertEqual(self.targets[-1][1]["work_limit"], 3)
        self.assertIsNone(value["request_wait_s"])  # No invented outside start timestamp.

    def test_online_target_only_budget_miss_does_not_scan_or_fall_back_implicitly(self):
        value = self.run_policy(mode="online", measure=self.measure,
            budget=runtime.ScanBudget(20, 3), work_per_sample=3)
        self.assertEqual(value["status"], "needs_scan")
        self.assertEqual(value["stop_reason"], "work_budget")
        self.assertEqual(self.measured, []); self.assertEqual(self.targets, [])

    def test_online_work_budget_keeps_completed_scan_cost_and_never_executes_a_second_default(self):
        search = runtime.IncreasingSearch("workers", dict(model_capacity=8))
        with self.assertRaises(runtime.CalibrationScanError) as caught:
            runtime.run_query(self.identity, [dict(workers=1)], self.execute,
                default=dict(workers=1), cache_dir=self.root, mode="online", search=search,
                measure=self.measure, budget=runtime.ScanBudget(1000, 15), work_per_sample=3, clock=self.clock)
        failure = caught.exception.query_policy_report["calibration_failure"]
        self.assertEqual(failure["search"]["stop_reason"], "work_budget")
        self.assertEqual(failure["scan_cost"]["reported_work"], 9)
        self.assertEqual(self.targets, [])

    def test_online_uncertain_measurement_preserves_unknown_and_never_retries_or_falls_back(self):
        def broken(*a, **kw): raise RuntimeError("fixture unknown ownership")
        with self.assertRaises(runtime.CalibrationScanError) as caught:
            self.run_policy(mode="online", measure=broken,
                budget=runtime.ScanBudget(1000, 21), work_per_sample=3)
        failure = caught.exception.query_policy_report["calibration_failure"]
        self.assertEqual(failure["scan_cost"]["unreported_reserved_work"], 3)
        self.assertEqual(self.targets, [])
        self.assertIsInstance(caught.exception.__cause__, RuntimeError)

    def test_original_target_error_keeps_identity_and_known_scan_cost(self):
        failure = ValueError("fixture actual target failure")
        def broken(c, **kw): raise failure
        with self.assertRaises(ValueError) as caught:
            runtime.run_query(self.identity, self.candidates, broken, default=dict(workers=1),
                cache_dir=self.root, mode="online", measure=self.measure,
                budget=runtime.ScanBudget(1000, 21), work_per_sample=3, clock=self.clock)
        self.assertIs(caught.exception, failure)
        report = failure.query_policy_report
        self.assertEqual(report["configuration"], dict(workers=2))
        self.assertEqual(report["calibration_failure"]["calibration"]["scan_cost"]["reported_work"], 18)

    def test_online_returned_failure_or_unknown_stops_fixed_grid_and_preserves_known_work(self):
        for state, resources, unknown in [("failed", True, 0), ("unknown", False, 3)]:
            with self.subTest(state=state):
                probes=[]
                def failed(c, **kw):
                    probes.append(c)
                    return runtime.Measurement(1, state, resources, 3, unknown_work=unknown)
                with self.assertRaises(runtime.CalibrationScanError) as caught:
                    self.run_policy(mode="online", measure=failed,
                        budget=runtime.ScanBudget(100, 21), work_per_sample=3)
                failure=caught.exception.query_policy_report["calibration_failure"]
                self.assertEqual(len(probes), 1)
                self.assertEqual(failure["scan_cost"]["reported_work"], 3)
                self.assertEqual(failure["scan_cost"]["unknown_work"], unknown)
        self.assertEqual(self.targets, [])

    def test_online_deadline_overrun_keeps_complete_result_and_preexisting_time_is_not_free(self):
        self.offline()
        with self.assertRaises(TimeoutError) as caught:
            self.run_policy(mode="online", measure=self.measure,
                budget=runtime.ScanBudget(2, 3), work_per_sample=3)
        self.assertEqual(caught.exception.completed_result, [1, 4, 9])
        self.assertEqual(caught.exception.query_policy_report["completed_result"], [1, 4, 9])
        start = self.clock(); self.clock.advance(5); before = len(self.targets)
        with self.assertRaises(TimeoutError) as caught:
            self.run_policy(mode="online", measure=self.measure, budget=runtime.ScanBudget(4, 21),
                work_per_sample=3, request_started_monotonic=start)
        self.assertEqual(len(self.targets), before)
        self.assertEqual(caught.exception.query_policy_report["request_wait_s"], 5)

    def test_online_requires_explicit_budget_and_target_reservation_before_any_measurement(self):
        for options in [dict(budget=None), dict(budget=runtime.ScanBudget(10, 2))]:
            with self.subTest(options=options), self.assertRaises(ValueError):
                self.run_policy(mode="online", measure=self.measure, work_per_sample=3, **options)
        self.assertEqual(self.measured, []); self.assertEqual(self.targets, [])

    def test_default_execution_failure_does_not_get_masked_by_an_unavailable_optional_cache(self):
        failure = RuntimeError("fixture original business failure")
        def broken(c): raise failure
        with patch.object(runtime, "resolve_calibration", side_effect=OSError("fixture cache")), \
                self.assertRaises(RuntimeError) as caught:
            runtime.run_query(self.identity, self.candidates, broken,
                default=dict(workers=1), cache_dir=self.root, clock=self.clock)
        self.assertIs(caught.exception, failure)
        self.assertEqual(failure.query_policy_report["configuration_source"], "explicit_default")


if __name__ == "__main__": unittest.main()
