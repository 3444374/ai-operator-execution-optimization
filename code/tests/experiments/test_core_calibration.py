"""CPU-only calibration behavior. All timings, hardware and model IDs are fixtures."""

from contextlib import redirect_stderr, redirect_stdout
from copy import deepcopy
from io import StringIO
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts.analysis import select_strategy_calibration as cli
from src.experiments.calibration import core
from src.experiments.calibration.contracts import core_identity_signature, load_calibration_contract


class CoreCalibrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.plan = dict(schema="semloom.movie_session_plan.v1", backend="semloom-core", lifecycle="resident",
            model_name="cpu-fixture-model", generation=dict(max_tokens=8, temperature=0, top_p=1),
            engine=dict(model_path="/tmp/cpu-fixture-model", model_python="/tmp/cpu-fixture-model-python",
                driver_python="/tmp/cpu-fixture-driver-python", max_model_len=4096, max_num_seqs=8,
                max_num_batched_tokens=4096, dtype="bfloat16", max_model_calls=4),
            resources=dict(cpu_workers=2, gpu_workers=1, prepare_batch_rows=1, model_batch_rows=1,
                max_active_batches=8, query_timeout_s=60, startup_timeout_s=120),
            profile=dict(mode="async", max_inflight=4, stage_timing=True),
            queries=[dict(query_id="warm", task=dict(name="fixture-q3"), manifest="/tmp/fixture-manifest", phase="warmup"),
                     dict(query_id="measure", task=dict(name="fixture-q3"), manifest="/tmp/fixture-manifest", phase="measured")])
        self.checked = self.checked_plan(self.plan)
        self.hardware = dict(machine_id="cpu-fixture-host", gpu_ids=["cpu-fixture-device"],
                             gpu_driver="cpu-fixture-driver", cpu_limit=8,
                             resource_limits=dict(rss_bytes=20 * 1024**3, shm_bytes=512 * 1024**2))
        self.model = dict(model_revision="cpu-fixture-weights-digest", tokenizer_revision="cpu-fixture-tokenizer-digest")
        self.software = dict(source_revision="cpu-fixture-source", versions=dict(ray="fixture", vllm="fixture"))
        self.identity = core.build_core_identity(self.plan, self.checked,
            hardware=self.hardware, model=self.model, software=self.software)

    @staticmethod
    def checked_plan(plan):
        return dict(queries=[dict(query, expected_calls=2, manifest_sha256="0" * 64) for query in plan["queries"]])

    def session(self, candidate, repeat, seconds, *, resources=True, status="completed", unknown=0):
        plan = core.core_candidate_plan(self.plan, self.checked, candidate, identity=self.identity,
                                        sample_id=f"c{candidate['max_inflight']}-p{candidate['cpu_workers']}-r{repeat}")
        checked = self.checked_plan(plan)
        report = dict(backend="semloom-core", lifecycle="resident", status=status,
                      application_e2e_s=100 + 40 + seconds + 17 + 29, queries=[])
        for declaration in checked["queries"]:
            counts = dict(expected=2, recorded=2, succeeded=2, failed=0, unknown=0,
                          missing=0, submission_confirmed=2, submission_unconfirmed=0)
            if declaration["phase"] == "measured" and unknown:
                counts.update(succeeded=2 - unknown, unknown=unknown)
            report["queries"].append(dict(query_id=declaration["query_id"], task=declaration["task"],
                phase=declaration["phase"], status="completed", complete_result_observed=True,
                full_query_seconds=40 if declaration["phase"] == "warmup" else seconds,
                post_result_seconds=17,
                backend_report=dict(status="completed", calls=dict(counts=counts),
                    core_execution=dict(mode="async", max_inflight=candidate["max_inflight"]),
                    quality=dict(gold_label="ignored", score=0), model_request_tokens_per_s=1)))
        return plan, checked, report

    def sample(self, candidate, repeat, seconds, **options):
        resources = options.pop("resources", True)
        plan, checked, report = self.session(candidate, repeat, seconds, **options)
        return core.core_measurement(plan, checked, report, identity=self.identity,
                                      repeat_index=repeat, resources_passed=resources)

    def scan(self, times=None):
        times = times or {(1, 1): [20, 21, 22], (2, 1): [10.2, 10.2, 30], (4, 1): [10, 10, 10]}
        candidates = [dict(max_inflight=c, cpu_workers=p) for c, p in times]
        samples = [self.sample(candidate, index, value) for candidate in candidates
                   for index, value in enumerate(times[(candidate["max_inflight"], candidate["cpu_workers"])], 1)]
        return dict(identity=self.identity, candidates=candidates, measurements=samples, scan_wall_s=2345)

    def save_selection(self, selection):
        path = self.directory / "selection.json"
        path.write_text(json.dumps(selection))
        return path

    def test_median_and_near_peak_choose_smaller_measured_configuration(self):
        selected = core.build_core_selection(**self.scan())
        self.assertEqual(selected["selection"]["max_inflight"], 2)
        cells = selected["evidence"]["candidates"]
        self.assertEqual([cell["elapsed_s_median"] for cell in cells], [21, 10.2, 10])
        self.assertEqual(selected["scope"], "measured_candidates_only")
        self.assertEqual(selected["scan_cost"]["reserved_work"], 36)
        self.assertEqual(selected["scan_cost"]["warmup_work"], 18)
        self.assertEqual(selected["scan_cost"]["wall_s"], 2345)
        self.assertFalse(selected["scan_cost"]["target_measurement_included"])

    def test_candidate_values_and_near_peak_ratio_are_configurable(self):
        scan = self.scan({(1, 1): [10.2] * 3, (3, 1): [10] * 3})
        self.assertEqual(core.build_core_selection(**scan)["selection"]["max_inflight"], 1)
        self.assertEqual(core.build_core_selection(**scan, near_peak_ratio=0.999)["selection"]["max_inflight"], 3)

    def test_equal_capacity_prefers_fewer_prepare_workers(self):
        selected = core.build_core_selection(**self.scan({(3, 2): [10] * 3, (3, 1): [10.1] * 3}))
        self.assertEqual((selected["selection"]["max_inflight"], selected["selection"]["cpu_workers"]), (3, 1))

    def test_any_failed_repeat_excludes_candidate_and_keeps_its_cost(self):
        scan = self.scan()
        fast = dict(max_inflight=8, cpu_workers=1)
        scan["candidates"].append(fast)
        scan["measurements"].extend(self.sample(fast, repeat, 1, unknown=1 if repeat == 3 else 0) for repeat in range(1, 4))
        result = core.build_core_selection(**scan)
        self.assertEqual(result["selection"]["max_inflight"], 2)
        self.assertFalse(result["evidence"]["candidates"][-1]["eligible"])
        self.assertEqual(result["scan_cost"]["reported_work"], 48)
        self.assertEqual(result["scan_cost"]["unknown_work"], 1)

    def test_resource_or_cleanup_failure_excludes_faster_candidate(self):
        scan = self.scan({(1, 1): [10] * 3, (2, 1): [1] * 3})
        scan["measurements"][-1] = self.sample(scan["candidates"][-1], 3, 1, resources=False)
        self.assertEqual(core.build_core_selection(**scan)["selection"]["max_inflight"], 1)

    def test_unmeasured_or_insufficient_candidate_cannot_win(self):
        scan = self.scan({(1, 1): [10] * 3, (2, 1): [1] * 2})
        scan["candidates"].append(dict(max_inflight=8, cpu_workers=2))
        result = core.build_core_selection(**scan)
        self.assertEqual(result["selection"]["max_inflight"], 1)
        self.assertEqual([cell["reason"] for cell in result["evidence"]["candidates"]],
                         [None, "insufficient_repeats", "insufficient_repeats"])

    def test_no_eligible_candidate_has_no_tuned_result(self):
        scan = self.scan({(3, 1): [1, 1]})
        with self.assertRaisesRegex(ValueError, "no eligible"):
            core.build_core_selection(**scan)

    def test_duplicate_repeat_or_undeclared_candidate_is_rejected(self):
        scan = self.scan()
        with self.assertRaisesRegex(ValueError, "duplicate"):
            core.build_core_selection(**dict(scan, measurements=scan["measurements"] + scan["measurements"][:1]))
        scan["measurements"].append(self.sample(dict(max_inflight=7, cpu_workers=1), 1, 1))
        with self.assertRaisesRegex(ValueError, "not declared"):
            core.build_core_selection(**scan)

    def test_nonfinite_time_and_negative_call_cost_cannot_enter_selection(self):
        for change in ("time", "cost", "wall"):
            with self.subTest(change=change):
                scan = self.scan()
                if change == "time":
                    scan["measurements"][0]["full_query_s"] = float("nan")
                elif change == "cost":
                    scan["measurements"][0]["recorded_model_calls"] = -1
                else:
                    scan["scan_wall_s"] = float("inf")
                with self.assertRaises(ValueError):
                    core.build_core_selection(**scan)

    def test_mixed_hardware_or_workload_sample_is_rejected(self):
        scan = self.scan()
        scan["measurements"][0]["identity_sha256"] = "1" * 64
        with self.assertRaisesRegex(ValueError, "different Core identity"):
            core.build_core_selection(**scan)

    def test_missing_failed_or_unknown_results_remain_invalid_samples(self):
        candidate = dict(max_inflight=2, cpu_workers=1)
        plan, checked, report = self.session(candidate, 1, 10, status="failed", unknown=1)
        report["queries"].pop()
        sample = core.core_measurement(plan, checked, report, identity=self.identity, repeat_index=1, resources_passed=True)
        self.assertEqual(sample["status"], "failed")
        self.assertIn("incomplete_query_schedule", sample["reasons"])
        self.assertEqual(sample["declared_model_calls"], 4)
        self.assertEqual(sample["recorded_model_calls"], 2)
        self.assertIsNone(sample["full_query_s"])

    def test_only_complete_measured_query_time_enters_the_objective(self):
        plan, checked, report = self.session(dict(max_inflight=3, cpu_workers=1), 1, 10)
        sample = core.core_measurement(plan, checked, report, identity=self.identity, repeat_index=1, resources_passed=True)
        self.assertEqual((sample["full_query_s"], sample["warmup_query_s"], sample["session_wall_s"]), (10, [40], 196))
        report["queries"][1]["backend_report"].update(quality=dict(gold_label="different", score=1), model_request_tokens_per_s=1000000)
        self.assertEqual(sample, core.core_measurement(plan, checked, report, identity=self.identity,
                                                     repeat_index=1, resources_passed=True))

    def test_movie_receipt_directly_adapts_to_the_public_measure_callback(self):
        from src.scheduling.runtime.calibration import Measurement

        candidate = dict(max_inflight=3, cpu_workers=1)
        valid = core.core_runtime_measurement(self.sample(candidate, 1, 10))
        self.assertEqual(valid, Measurement(10, "ok", True, 4, warmup_work=2))
        unknown = core.core_runtime_measurement(self.sample(candidate, 1, 10, unknown=1))
        self.assertEqual((unknown.status, unknown.unknown_work, unknown.work), ("unknown", 1, 4))

    def test_measured_query_sequence_is_summed_without_overlapping_stage_totals(self):
        plan = deepcopy(self.plan)
        plan["queries"].append(dict(plan["queries"][1], query_id="last-q3"))
        checked = self.checked_plan(plan)
        identity = core.build_core_identity(plan, checked, hardware=self.hardware, model=self.model, software=self.software)
        _, _, report = self.session(dict(max_inflight=4, cpu_workers=2), 1, 10)
        report["queries"] = [dict(report["queries"][0], query_id="warm"),
                             dict(report["queries"][1], query_id="measure"),
                             dict(report["queries"][1], query_id="last-q3", full_query_seconds=7)]
        sample = core.core_measurement(plan, checked, report, identity=identity, repeat_index=1, resources_passed=True)
        self.assertEqual(sample["full_query_s"], 17)

    def test_existing_session_receipt_round_trips_through_measurement_and_selected_plan(self):
        from src.experiments.movie_records_session import run_sequence

        candidate = dict(max_inflight=3, cpu_workers=1)
        plan, checked, template = self.session(candidate, 1, 10)
        clock = [0]

        class Driver:
            def start(self):
                clock[0] += 100 * 10**9
                return dict(engine_uid="cpu-fixture-engine", physical_model=False)

            def query(self, query, emit):
                index = 0 if query["phase"] == "warmup" else 1
                clock[0] += (40 if index == 0 else 10) * 10**9
                emit("result_ready")
                clock[0] += 17 * 10**9
                return template["queries"][index]["backend_report"]

            def close(self):
                clock[0] += 29 * 10**9
                return dict(status="closed")

        receipt = run_sequence(plan["queries"], lambda *_: Driver(), self.directory / "existing-session",
            backend="semloom-core", lifecycle="resident", clock=lambda: clock[0])
        sample = core.core_measurement(plan, checked, receipt, identity=self.identity,
                                       repeat_index=1, resources_passed=True)
        self.assertEqual((sample["status"], sample["full_query_s"], sample["warmup_query_s"]), ("ok", 10, [40]))
        self.assertEqual(sample["session_wall_s"], 213)
        result = core.build_core_selection(self.identity, [candidate], [sample], scan_wall_s=220, minimum_repeats=1)
        resolution = core.resolve_core_selection(self.save_selection(result), self.identity)
        selected = core.core_candidate_plan(self.plan, self.checked, resolution["candidate"], identity=self.identity)
        self.assertEqual((selected["profile"]["max_inflight"], selected["resources"]["cpu_workers"]), (3, 1))

    def test_actual_profile_result_and_order_must_match(self):
        for change in ("profile", "complete", "order", "nan"):
            with self.subTest(change=change):
                plan, checked, report = self.session(dict(max_inflight=2, cpu_workers=1), 1, 10)
                if change == "profile":
                    report["queries"][-1]["backend_report"]["core_execution"]["max_inflight"] = 4
                elif change == "complete":
                    report["queries"][-1]["complete_result_observed"] = False
                elif change == "order":
                    report["queries"].reverse()
                else:
                    report["queries"][-1]["full_query_seconds"] = float("nan")
                if change in {"order", "nan"}:
                    with self.assertRaises(ValueError):
                        core.core_measurement(plan, checked, report, identity=self.identity, repeat_index=1, resources_passed=True)
                else:
                    self.assertEqual(core.core_measurement(plan, checked, report, identity=self.identity,
                        repeat_index=1, resources_passed=True)["status"], "failed")

    def test_model_generation_hardware_software_inputs_and_warmup_each_change_signature(self):
        original = core_identity_signature(self.identity)
        for group, key, value in (("hardware", "gpu_ids", ["other-device"]), ("model", "model_revision", "other-model"),
                                  ("generation", "max_tokens", 16), ("execution", "lifecycle", "isolated"),
                                  ("measurement", "warmup", "other-policy")):
            changed = deepcopy(self.identity)
            changed[group][key] = value
            self.assertNotEqual(core_identity_signature(changed), original)
        for change in ("source", "input", "phase"):
            changed = deepcopy(self.identity)
            if change == "source":
                changed["execution"]["software"]["source_revision"] = "other-source"
            elif change == "input":
                changed["workload"]["queries"][1]["manifest_sha256"] = "1" * 64
            else:
                changed["workload"]["queries"][0]["phase"] = "measured"
            self.assertNotEqual(core_identity_signature(changed), original)

    def test_actual_model_sequence_capacity_changes_identity_and_allows_a_larger_declared_probe(self):
        plan = deepcopy(self.plan)
        plan["engine"]["max_num_seqs"] = 64
        plan["resources"]["max_active_batches"] = 64
        checked = self.checked_plan(plan)
        identity = core.build_core_identity(plan, checked,
            hardware=self.hardware, model=self.model, software=self.software)
        self.assertNotEqual(core_identity_signature(identity), core_identity_signature(self.identity))
        chosen = core.core_candidate_plan(plan, checked, dict(max_inflight=16, cpu_workers=1), identity=identity)
        self.assertEqual(chosen["profile"]["max_inflight"], 16)
        self.assertEqual(chosen["engine"]["max_num_seqs"], 64)
        self.assertEqual(self.plan["engine"]["max_num_seqs"], 8)

    def test_scanned_knobs_budget_and_unique_sample_ids_do_not_change_fixed_identity(self):
        original = deepcopy(self.plan)
        candidate = dict(max_inflight=3, cpu_workers=1)
        plan = core.core_candidate_plan(self.plan, self.checked, candidate, identity=self.identity, sample_id="scan-unique")
        plan["engine"]["max_model_calls"] = 100
        checked = self.checked_plan(plan)
        self.assertEqual(self.plan, original)
        self.assertEqual([q["query_id"] for q in plan["queries"]], ["scan-unique:warm", "scan-unique:measure"])
        self.assertEqual(self.identity, core.build_core_identity(plan, checked,
            hardware=self.hardware, model=self.model, software=self.software))

    def test_candidate_caps_and_batch_one_are_preserved(self):
        for candidate in (dict(max_inflight=9, cpu_workers=1), dict(max_inflight=1, cpu_workers=7),
                          dict(max_inflight="auto", cpu_workers=1), dict(max_inflight=True, cpu_workers=1)):
            with self.subTest(candidate=candidate), self.assertRaises(ValueError):
                core.core_candidate_plan(self.plan, self.checked, candidate, identity=self.identity)
        plan = deepcopy(self.plan)
        plan["resources"]["model_batch_rows"] = 4
        with self.assertRaisesRegex(ValueError, "batch rows"):
            core.build_core_identity(plan, self.checked_plan(plan), hardware=self.hardware, model=self.model, software=self.software)

    def test_incomplete_or_stale_identity_is_rejected_before_creating_a_plan(self):
        for remove in ("hardware", "workload"):
            changed = deepcopy(self.identity)
            changed.pop(remove)
            with self.assertRaisesRegex(ValueError, "calibration identity"):
                core_identity_signature(changed)
        with self.assertRaisesRegex(ValueError, "missing"):
            core.build_core_identity(self.plan, self.checked, hardware={}, model=self.model, software=self.software)
        checked = deepcopy(self.checked)
        checked["queries"][0]["manifest"] = "/tmp/other-manifest"
        with self.assertRaisesRegex(ValueError, "checked inputs differ"):
            core.build_core_identity(self.plan, checked, hardware=self.hardware, model=self.model, software=self.software)
        plan = deepcopy(self.plan)
        plan["generation"]["max_tokens"] = 16
        with self.assertRaisesRegex(ValueError, "plan differs"):
            core.core_candidate_plan(plan, self.checked, dict(max_inflight=1, cpu_workers=1), identity=self.identity)

    def test_cache_hit_only_reads_and_materializes_the_measured_candidate(self):
        path = self.save_selection(core.build_core_selection(**self.scan()))
        with patch.object(core, "core_measurement", side_effect=AssertionError("cache hit scanned")), \
                patch.object(core, "build_core_selection", side_effect=AssertionError("cache hit selected again")), \
                patch("subprocess.run", side_effect=AssertionError("cache hit started a process")):
            resolution = core.resolve_core_selection(path, self.identity)
        self.assertEqual(resolution["status"], "ready")
        plan = core.core_candidate_plan(self.plan, self.checked, resolution["candidate"], identity=self.identity)
        self.assertEqual(plan["profile"]["max_inflight"], 2)
        self.assertEqual(plan["resources"]["cpu_workers"], 1)
        self.assertTrue(resolution["performance_tuned"])
        self.assertEqual(len(resolution["selection_sha256"]), 64)

    def test_cache_missing_or_different_identity_explicitly_requires_scan(self):
        path = self.directory / "absent.json"
        missing = core.resolve_core_selection(path, self.identity)
        self.assertEqual((missing["status"], missing["reason"], missing["performance_tuned"]),
                         ("needs_scan", "missing_selection", False))
        path = self.save_selection(core.build_core_selection(**self.scan()))
        changed = deepcopy(self.identity)
        changed["generation"]["max_tokens"] = 16
        mismatch = core.resolve_core_selection(path, changed)
        self.assertEqual((mismatch["status"], mismatch["reason"], mismatch["performance_tuned"]),
                         ("needs_scan", "identity_or_policy_mismatch", False))

    def test_materialized_selection_reaches_the_existing_async_owner_factory(self):
        from src.execution_provider.adapters import lotus_records_core as records_core
        from src.execution_provider.adapters import lotus_records_async_actor as async_actor
        from src.semantic_methods.lotus.records import FilterMethod, RecordsResources
        from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec

        path = self.save_selection(core.build_core_selection(**self.scan({(3, 1): [10] * 3})))
        resolution = core.resolve_core_selection(path, self.identity)
        selected = core.core_candidate_plan(self.plan, self.checked, resolution["candidate"], identity=self.identity)
        resources = RecordsResources(**selected["resources"])
        with patch.object(records_core, "SemLoomRecordsCore") as owner, \
                patch.object(async_actor, "AsyncRecordsActorOwner") as actor:
            records_core.create_records_core(FilterMethod("cpu-fixture-model", instruction="Keep {reviewText}"), EngineSpec.from_dict(selected["engine"]),
                GenerationSpec.from_dict(selected["generation"]), resources, profile=selected["profile"])
        effective = actor.call_args.args[3]
        self.assertEqual((effective.max_active_batches, effective.cpu_workers), (3, 1))
        self.assertEqual(owner.call_args.args[3], effective)

    def test_corrupt_or_unmeasured_artifact_cannot_be_reported_as_tuned(self):
        selection = core.build_core_selection(**self.scan())
        for change in ("identity", "evidence", "unmeasured", "auto", "kind", "repeats"):
            with self.subTest(change=change):
                value = deepcopy(selection)
                if change == "identity":
                    value["identity"]["model"]["model_revision"] = "other-model"
                elif change == "evidence":
                    value["evidence"]["candidates"][1]["eligible"] = False
                elif change == "unmeasured":
                    value["selection"]["max_inflight"] = 7
                elif change == "auto":
                    value["selection"]["max_inflight"] = "auto"
                elif change == "kind":
                    value["schema"] = "other-schema"
                else:
                    value["policy"]["repetitions"] = 10
                with self.assertRaises(ValueError):
                    core.resolve_core_selection(self.save_selection(value), self.identity)

    def test_core_artifact_does_not_weaken_the_legacy_evidence_requirements(self):
        path = self.save_selection(core.build_core_selection(**self.scan()))
        core.resolve_core_selection(path, self.identity)
        with self.assertRaisesRegex(ValueError, "schema_version"):
            load_calibration_contract(path, {"max_inflight": 2})

    def test_public_signature_reuses_the_existing_canonical_digest_bytes(self):
        from src.baselines.common.private_artifacts import content_digest

        identity = deepcopy(self.identity)
        identity["workload"]["unicode_identity"] = "输入数据"
        expected = content_digest(dict(schema="semloom.runtime_calibration_identity.v1", **identity))
        self.assertEqual(core_identity_signature(identity), expected)

    def test_cli_core_branch_never_reads_a_baseline_or_starts_a_model(self):
        scan_path = self.directory / "scan.json"
        scan_path.write_text(json.dumps(self.scan()))
        path = self.directory / "selection.json"
        with patch.object(cli, "_direct_baseline_metrics", side_effect=AssertionError("baseline used")), \
                patch.object(cli, "build_selection", side_effect=AssertionError("legacy selection used")), \
                patch("subprocess.run", side_effect=AssertionError("a process was started")), redirect_stdout(StringIO()):
            cli.main(["--core-scan", str(scan_path), "--output", str(path)])
        result = json.loads(path.read_text())
        self.assertEqual(result["selection"]["max_inflight"], 2)
        self.assertEqual(result["evidence"]["scan_path"], str(scan_path.resolve()))
        self.assertEqual(core.resolve_core_selection(path, self.identity)["status"], "ready")

    def test_cli_rejects_mixed_inputs_and_keeps_required_legacy_arguments(self):
        for argv in (["--core-scan", "scan.json", "--feeding-runs", "baseline.csv", "--output", "out.json"],
                     ["--core-scan", "scan.json", "--env-output", "old.env", "--output", "out.json"],
                     ["--output", "out.json"]):
            with self.subTest(argv=argv), redirect_stderr(StringIO()), self.assertRaises(SystemExit):
                cli.parse_args(argv)


if __name__ == "__main__":
    unittest.main()
