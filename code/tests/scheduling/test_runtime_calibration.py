"""Ordinary CPU workloads exercise the public API without Movie/model services."""

from copy import deepcopy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from src.scheduling.runtime import calibration as runtime


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class RuntimeCalibrationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.clock = Clock()
        self.identity = dict(hardware=dict(host="CPU-fixture", cores=8), model=dict(revision="integer-transform-v1"),
            generation=dict(operation="square"), execution=dict(implementation="CPU-fixture-v1"),
            workload=dict(inputs=[1, 2, 3], order="source"),
            measurement=dict(objective="complete transform", warmup="one explicit fixture pass", work_unit="rows"))
        self.candidates = [dict(workers=value, chunk_rows=1) for value in (1, 2, 4)]
        self.measured, self.executed = [], []

    def measure(self, candidate, *, repeat_index, deadline_monotonic, work_limit):
        self.measured.append((deepcopy(candidate), repeat_index, deadline_monotonic, work_limit))
        elapsed = {1: 20., 2: 10.2, 4: 10.}[candidate["workers"]]
        # Callback wall includes setup and warmup; only elapsed follows the objective.
        self.clock.advance(elapsed + 2)
        return runtime.Measurement(elapsed, "ok", True, 3, warmup_work=1)

    def execute(self, candidate):
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=candidate["workers"]) as executor:
            self.executed.append((deepcopy(candidate), executor._max_workers))
            result = list(executor.map(lambda value: value * value, self.identity["workload"]["inputs"]))
        self.clock.advance(7)
        return result

    def options(self, **overrides):
        return dict(cache_dir=self.directory, measure=self.measure, budget=runtime.ScanBudget(1000, 27),
                    work_per_sample=3, clock=self.clock, **overrides)

    def test_nonmovie_scan_select_execute_then_cache_hit_applies_same_configuration(self):
        first = runtime.run_calibrated(self.identity, self.candidates, self.execute, **self.options())
        self.assertEqual(first["status"], "executed")
        self.assertEqual(first["result"], [1, 4, 9])
        self.assertEqual(first["calibration"]["candidate"], dict(workers=2, chunk_rows=1))
        self.assertEqual(self.executed[-1], (dict(workers=2, chunk_rows=1), 2))
        self.assertEqual(len(self.measured), 9)
        self.assertTrue(all(limit == 3 and deadline == 1000 for _, _, deadline, limit in self.measured))
        self.assertEqual(first["calibration"]["scan_cost"]["reserved_work"], 27)
        self.assertEqual(first["calibration"]["scan_cost"]["warmup_work"], 9)
        self.assertAlmostEqual(first["calibration"]["scan_cost"]["wall_s"], 138.6)
        self.assertEqual(first["target_wall_s"], 7)
        self.assertAlmostEqual(first["total_wall_s"], 145.6)
        self.assertFalse(first["calibration"]["cache_hit"])
        self.assertFalse(first["calibration"]["scan_cost"]["target_measurement_included"])
        with patch.object(self, "measure", side_effect=AssertionError("hit scanned")):
            second = runtime.run_calibrated(self.identity, self.candidates, self.execute, **self.options())
        self.assertTrue(second["calibration"]["cache_hit"])
        self.assertEqual(second["calibration"]["scan_cost"]["reserved_work"], 0)
        self.assertEqual(second["calibration"]["scan_cost"]["wall_s"], 0)
        self.assertEqual(second["calibration"]["calibration_cost"]["reserved_work"], 27)
        self.assertEqual(second["target_wall_s"], second["total_wall_s"])
        self.assertEqual(len(self.measured), 9)
        self.assertEqual(self.executed[-1], (dict(workers=2, chunk_rows=1), 2))

    def test_cache_miss_without_measure_callback_does_not_execute_or_claim_tuned(self):
        result = runtime.run_calibrated(self.identity, self.candidates, self.execute,
                                        cache_dir=self.directory, clock=self.clock)
        self.assertEqual(result["status"], "needs_scan")
        self.assertFalse(result["calibration"]["performance_tuned"])
        self.assertEqual(self.executed, [])
        self.assertEqual(list(self.directory.iterdir()), [])

    def test_each_identity_group_requires_new_scan_and_keeps_old_artifact(self):
        first = runtime.calibrate(self.identity, self.candidates, **self.options())
        for group in self.identity:
            changed = deepcopy(self.identity)
            changed[group]["revision_change"] = group
            result = runtime.calibrate(changed, self.candidates, cache_dir=self.directory, clock=self.clock)
            self.assertEqual(result["status"], "needs_scan")
            self.assertFalse(result["performance_tuned"])
        self.assertTrue(Path(first["selection_path"]).exists())

    def test_changed_candidate_set_repeats_or_selection_rule_needs_scan(self):
        runtime.calibrate(self.identity, self.candidates, **self.options())
        for candidates, kwargs in ((self.candidates[:2], {}), (self.candidates, dict(repetitions=4)),
                                    (self.candidates, dict(near_peak_ratio=.999))):
            result = runtime.calibrate(self.identity, candidates, cache_dir=self.directory, clock=self.clock, **kwargs)
            self.assertEqual(result["status"], "needs_scan")
            self.assertFalse(result["performance_tuned"])

    def test_mismatch_can_run_a_new_bounded_scan(self):
        runtime.calibrate(self.identity, self.candidates, **self.options())
        changed = deepcopy(self.identity)
        changed["model"]["revision"] = "integer-transform-v2"
        result = runtime.calibrate(changed, self.candidates, **self.options())
        self.assertFalse(result["cache_hit"])
        self.assertEqual(len(self.measured), 18)
        self.assertEqual(len(list(self.directory.glob('*.json'))), 2)

    def test_budget_is_required_and_predictable_work_overrun_stops_before_callback(self):
        for budget in (None, runtime.ScanBudget(100, 26)):
            with self.subTest(budget=budget), self.assertRaises(ValueError):
                runtime.calibrate(self.identity, self.candidates, cache_dir=self.directory,
                    measure=self.measure, budget=budget, work_per_sample=3, clock=self.clock)
        self.assertEqual(self.measured, [])
        self.assertEqual(list(self.directory.iterdir()), [])

    def test_deadline_is_passed_to_callback_and_overrun_retains_cost_without_execution(self):
        with self.assertRaises(runtime.CalibrationScanError) as caught:
            runtime.run_calibrated(self.identity, self.candidates, self.execute,
                cache_dir=self.directory, measure=self.measure, budget=runtime.ScanBudget(1, 27),
                work_per_sample=3, clock=self.clock)
        report = caught.exception.report
        self.assertEqual(report["scan_cost"]["samples"], 1)
        self.assertEqual(report["scan_cost"]["reserved_work"], 3)
        self.assertEqual(report["scan_cost"]["reported_work"], 3)
        self.assertFalse(report["performance_tuned"])
        self.assertEqual(self.executed, [])
        self.assertEqual(list(self.directory.iterdir()), [])

    def test_callback_exception_stops_peers_and_preserves_uncertain_reserved_work(self):
        def broken(candidate, **kwargs):
            self.clock.advance(2)
            raise RuntimeError("CPU fixture callback could not confirm completion")

        with self.assertRaises(runtime.CalibrationScanError) as caught:
            runtime.run_calibrated(self.identity, self.candidates, self.execute,
                cache_dir=self.directory, measure=broken, budget=runtime.ScanBudget(100, 27),
                work_per_sample=3, clock=self.clock)
        self.assertIsInstance(caught.exception.__cause__, RuntimeError)
        self.assertEqual(caught.exception.report["scan_cost"]["samples"], 1)
        self.assertEqual(caught.exception.report["scan_cost"]["unreported_reserved_work"], 3)
        self.assertEqual(self.executed, [])
        self.assertEqual(list(self.directory.iterdir()), [])

    def test_interrupt_propagates_with_cost_and_no_following_work(self):
        def interrupted(candidate, **kwargs):
            raise KeyboardInterrupt()

        with self.assertRaises(KeyboardInterrupt) as caught:
            runtime.calibrate(self.identity, self.candidates, cache_dir=self.directory,
                measure=interrupted, budget=runtime.ScanBudget(100, 27), work_per_sample=3, clock=self.clock)
        self.assertEqual(caught.exception.calibration_report["scan_cost"]["reserved_work"], 3)
        self.assertEqual(list(self.directory.iterdir()), [])

    def test_callback_work_limit_overrun_keeps_reported_cost_and_aborts(self):
        def too_much(candidate, **kwargs):
            return runtime.Measurement(1, "ok", True, 4)

        with self.assertRaises(runtime.CalibrationScanError) as caught:
            runtime.calibrate(self.identity, self.candidates, cache_dir=self.directory,
                measure=too_much, budget=runtime.ScanBudget(100, 27), work_per_sample=3, clock=self.clock)
        self.assertEqual(caught.exception.report["scan_cost"]["reported_work"], 4)
        self.assertEqual(caught.exception.report["scan_cost"]["reserved_work"], 3)
        self.assertEqual(len(caught.exception.report["samples"]), 1)

    def test_cache_write_failure_preserves_completed_scan_cost(self):
        with patch.object(runtime, "_save", side_effect=OSError("CPU fixture cache not writable")), \
                self.assertRaises(runtime.CalibrationScanError) as caught:
            runtime.run_calibrated(self.identity, self.candidates, self.execute, **self.options())
        self.assertEqual(caught.exception.report["scan_cost"]["reserved_work"], 27)
        self.assertEqual(caught.exception.report["scan_cost"]["reported_work"], 27)
        self.assertEqual(self.executed, [])

    def test_target_failure_preserves_original_exception_and_visible_calibration_cost(self):
        failure = ValueError("CPU fixture target failure")

        def broken_target(candidate):
            self.clock.advance(3)
            raise failure

        with self.assertRaises(ValueError) as caught:
            runtime.run_calibrated(self.identity, self.candidates, broken_target, **self.options())
        self.assertIs(caught.exception, failure)
        self.assertEqual(failure.calibration_report["calibration"]["scan_cost"]["reserved_work"], 27)
        self.assertEqual(failure.calibration_report["target_wall_s"], 3)
        self.assertEqual(len(self.measured), 9)

    def test_candidate_and_receipt_mutations_do_not_change_the_execution_configuration(self):
        candidates = deepcopy(self.candidates)

        def measure(candidate, **kwargs):
            measured = self.measure(candidate, **kwargs)
            candidate["workers"] = 999
            return measured

        result = runtime.run_calibrated(self.identity, candidates, self.execute,
            cache_dir=self.directory, measure=measure, budget=runtime.ScanBudget(1000, 27),
            work_per_sample=3, clock=self.clock)
        self.assertEqual(self.candidates, candidates)
        self.assertEqual(result["calibration"]["candidate"]["workers"], 2)
        self.assertEqual(self.executed[-1][1], 2)

    def records(self, elapsed=None):
        elapsed = elapsed or {1: [20., 20., 20.], 2: [10.2, 10.2, 100.], 4: [10., 10., 10.]}
        return [runtime.measurement_record(self.identity, candidate, repeat,
            runtime.Measurement(seconds, "ok", True, 3, warmup_work=1), reserved_work=3)
            for candidate in self.candidates for repeat, seconds in enumerate(elapsed[candidate["workers"]], 1)]

    def test_precollected_selector_is_the_same_median_near_peak_rule(self):
        selected = runtime.select_calibration(self.identity, self.candidates, self.records(), scan_wall_s=100)
        self.assertEqual(selected["selection"], dict(workers=2, chunk_rows=1))
        self.assertEqual([cell["elapsed_s_median"] for cell in selected["evidence"]["candidates"]], [20, 10.2, 10])
        strict = runtime.select_calibration(self.identity, self.candidates, self.records(), near_peak_ratio=.999, scan_wall_s=100)
        self.assertEqual(strict["selection"]["workers"], 4)

    def test_failed_unknown_or_resource_failed_repeat_excludes_entire_candidate(self):
        for measurement in (runtime.Measurement(1, "failed", True, 3),
                            runtime.Measurement(1, "ok", False, 3),
                            runtime.Measurement(1, "unknown", True, 3, unknown_work=1)):
            with self.subTest(measurement=measurement):
                records = self.records()
                records[-1] = runtime.measurement_record(self.identity, self.candidates[-1], 3, measurement, reserved_work=3)
                result = runtime.select_calibration(self.identity, self.candidates, records, scan_wall_s=100)
                self.assertEqual(result["selection"]["workers"], 2)
                self.assertFalse(result["evidence"]["candidates"][-1]["eligible"])
                self.assertEqual(result["scan_cost"]["reserved_work"], 27)

    def test_all_ineligible_scan_has_no_cache_or_target_execution(self):
        def bad(candidate, **kwargs):
            return runtime.Measurement(None, "unknown", True, None)

        with self.assertRaises(runtime.CalibrationScanError) as caught:
            runtime.run_calibrated(self.identity, self.candidates, self.execute,
                cache_dir=self.directory, measure=bad, budget=runtime.ScanBudget(100, 27),
                work_per_sample=3, clock=self.clock)
        self.assertEqual(caught.exception.report["scan_cost"]["unreported_reserved_work"], 27)
        self.assertEqual(self.executed, [])
        self.assertEqual(list(self.directory.iterdir()), [])

    def test_duplicate_repeat_mixed_identity_and_unmeasured_candidate_are_rejected(self):
        records = self.records()
        for changed in (records + records[:1],
                        [dict(records[0], identity_sha256="1" * 64)] + records[1:],
                        [dict(records[0], candidate=dict(workers=3, chunk_rows=1))] + records[1:]):
            with self.subTest(changed=changed[0]), self.assertRaises(ValueError):
                runtime.select_calibration(self.identity, self.candidates, changed, scan_wall_s=100)

    def test_corrupt_or_fabricated_selection_never_loads_as_tuned(self):
        artifact = runtime.select_calibration(self.identity, self.candidates, self.records(), scan_wall_s=100)
        path = self.directory / "selection.json"
        for change in ("identity", "unmeasured", "inferior", "auto", "tuned", "repeats"):
            with self.subTest(change=change):
                value = deepcopy(artifact)
                if change == "identity":
                    value["identity"]["workload"]["new_input"] = True
                elif change == "unmeasured":
                    value["selection"]["workers"] = 3
                elif change == "inferior":
                    value["selection"]["workers"] = 1
                elif change == "auto":
                    value["selection"]["workers"] = "auto"
                elif change == "tuned":
                    value["performance_tuned"] = False
                else:
                    value["evidence"]["candidates"][1]["repeats"] = 1
                path.write_text(json.dumps(value))
                with self.assertRaises(ValueError):
                    runtime.resolve_calibration(path, self.identity)

    def test_public_module_import_and_nonmovie_execution_need_no_experiment_or_ml_dependencies(self):
        import src

        module = Path(runtime.__file__).resolve()
        script = '''
import importlib, importlib.abc, pathlib, sys, tempfile
class Guard(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.startswith(("src.experiments", "src.baselines", "src.semantic_methods", "ray", "daft", "vllm", "torch", "pyarrow")):
            raise AssertionError("forbidden runtime dependency: " + fullname)
sys.meta_path.insert(0, Guard())
sys.path.insert(0, sys.argv[2])
import src.scheduling.runtime as package
package.__path__.insert(0, str(pathlib.Path(sys.argv[1]).parent))
api = importlib.import_module("src.scheduling.runtime.calibration")
identity = {key: {"revision": "CPU-fixture"} for key in ("hardware", "model", "generation", "execution", "workload", "measurement")}
identity["measurement"]["work_unit"] = "items"
with tempfile.TemporaryDirectory() as cache:
    output = api.run_calibrated(identity, [{"workers": 3}], lambda config: list(range(config["workers"])),
        cache_dir=cache, measure=lambda config, **kw: api.Measurement(1, "ok", True, 1),
        repetitions=1, budget=api.ScanBudget(10, 1))
    assert output["result"] == [0, 1, 2]
    assert output["calibration"]["performance_tuned"] is True
print("PASS public stdlib-only CPU callback scan/select/cache/execute")
'''
        result = subprocess.run([sys.executable, "-B", "-c", script, str(module), str(Path(src.__file__).parent.parent)], capture_output=True, text=True,
                                env=dict(os.environ, PYTHONDONTWRITEBYTECODE="1"), timeout=20)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("PASS public stdlib-only", result.stdout)


if __name__ == "__main__":
    unittest.main()
