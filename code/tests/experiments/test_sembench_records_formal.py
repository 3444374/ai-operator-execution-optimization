"""Finite budget, observer boundaries and no-model default for the Q3 controller."""

from pathlib import Path
from dataclasses import asdict
import csv
import hashlib
import os
import json
import sys
import shutil
import tempfile
import time
import unittest
from unittest.mock import patch
from types import SimpleNamespace

from src.experiments.sembench_records_formal import QueryClock, main, parser_audit, select_profiles, validate_result, validate_runtime_qualification
from src.experiments.attempt_ledger import AttemptBudget, BudgetError, BudgetExhausted
from src.experiments.cell_budget import CellBudgetLedger
from src.semantic_methods.lotus.records import CallLedger, FilterMethod, identity


class CampaignContractTests(unittest.TestCase):
    def test_no_execute_flag_checks_only_and_never_dispatches(self):
        with patch("src.experiments.sembench_records_formal.check", return_value={"blockers": []}), \
                patch("src.experiments.sembench_records_formal.run_campaign") as execute:
            code = main(["run", "--package", "/unused", "--authorization", "/unused-auth", "--output-dir", "/unused-output"])
        self.assertEqual(code, 0)
        execute.assert_not_called()

    def test_clock_starts_before_result_and_requires_one_terminal_count(self):
        started = time.monotonic_ns()
        clock = QueryClock()
        clock("query_source_start")
        clock("result_ready")
        clock("evaluation_start")
        clock("evaluation_end")
        clock("owner_closed")
        value = clock.result(started, time.monotonic_ns())
        self.assertLessEqual(value["query_result_seconds"], value["application_e2e_seconds"])
        clock("result_ready")
        with self.assertRaisesRegex(ValueError, "one complete"):
            clock.result(started, time.monotonic_ns())
        with self.assertRaisesRegex(ValueError, "one complete"):
            QueryClock().result(started, time.monotonic_ns())

    def test_calibration_requires_all_native_points_and_stable_tie(self):
        rows = [dict(backend=b, profile=p, cold_complete_seconds=1) for b in
            ("lotus-native", "semloom-core", "ray-data", "daft", "imlane") for p in ("p2", "p1")]
        self.assertTrue(all(p == "p1" for p in select_profiles(rows).values()))
        with self.assertRaises(ValueError):
            select_profiles(rows[:-1])
        rows[-1]["profile"] = "p2"
        with self.assertRaises(ValueError):
            select_profiles(rows)

    def test_failed_query_keeps_whole_reservation_and_cannot_reclaim(self):
        with tempfile.TemporaryDirectory() as name:
            ledger = CellBudgetLedger.create(Path(name) / "budget.sqlite", AttemptBudget("formal-fixture", 240),
                deadline_utc=time.time() + 60)
            ledger.reserve_unit("query-one", 120)
            ledger.claim_unit("query-one")
            self.assertEqual(ledger.snapshot()["allocated_requests"], 120)
            with self.assertRaises(BudgetError):
                ledger.claim_unit("query-one")
            ledger.reserve_unit("query-two", 120)
            with self.assertRaises(BudgetExhausted):
                ledger.reserve_unit("repair-not-authorized", 1)
            self.assertEqual(ledger.snapshot()["allocated_requests"], 240)

    def test_unknown_or_missing_call_cannot_be_a_quality_pass(self):
        expected = [identity("q", "r", 0)]
        ledger = CallLedger(expected, 1)
        ledger.record(dict(expected[0], status="unknown", prompt_tokens=None, completion_tokens=None))
        report = dict(status="completed", calls=ledger.report())
        with self.assertRaisesRegex(ValueError, "failed/unknown/missing"):
            validate_result(report, expected)

    def test_complete_result_binds_decision_count_messages_and_usage(self):
        expected = [identity("q", "r", 0)]
        ledger = CallLedger(expected, 1)
        ledger.record(dict(expected[0], status="succeeded", prompt_tokens=2, completion_tokens=1,
            total_tokens=3, messages_sha256="a" * 64, text="False", finish_reason="stop"))
        report = dict(status="completed", calls=ledger.report(), decisions=[dict(row_id="r", decision=False)],
            result=dict(positive_review_cnt=0), quality=dict(original_metric={"fixture": True}))
        self.assertEqual(validate_result(report, expected)["result"], {"positive_review_cnt": 0})
        report["result"]["positive_review_cnt"] = 1
        with self.assertRaisesRegex(ValueError, "COUNT"):
            validate_result(report, expected)

    def test_cpu_fixture_is_not_model_qualification(self):
        with self.assertRaisesRegex(ValueError, "cannot qualify"):
            validate_result(dict(status="completed", inference_calls=0), [identity("q", "r", 0)])

    def test_old_or_cpu_preflight_does_not_qualify_new_source(self):
        with tempfile.TemporaryDirectory() as name:
            path = Path(name) / "preflight.json"
            for value in (dict(status="ok"), dict(status="ok", qualification_kind="cpu", actual_model_calls=0)):
                path.write_text(json.dumps(value))
                with self.assertRaisesRegex(ValueError, "CPU/old"):
                    validate_runtime_qualification(path)

    def test_default_parser_use_is_audited_without_changing_original_policy(self):
        from src.serving.backends.vllm_engine import GenerationSpec
        rows = [dict(call_id=str(i), text=value) for i, value in enumerate(("True", "False", "unknown", "True False"))]
        result = parser_audit(dict(calls=dict(records=rows)), FilterMethod("openai/qwen2.5-7b"), GenerationSpec())
        self.assertEqual(result["default_call_ids"], ["2"])
        self.assertEqual(result["both_tokens_call_ids"], ["3"])
        self.assertTrue(result["original_parser_default"])
        self.assertFalse(result["altered_decisions"])

    def test_supervisor_monitor_failure_stops_only_its_cpu_child(self):
        from src.experiments.lotus_records_supervisor import supervise
        observed = []
        def monitor(owned):
            observed.extend(owned)
            raise RuntimeError("fixture resource stop")
        with tempfile.TemporaryDirectory() as name:
            # This declared CPU child creates no descendants. macOS sandbox
            # global sysctl inventory is unavailable; the exact Popen PID and
            # birth identity remain real, as do callback stop and cleanup.
            with patch("src.experiments.lotus_records_supervisor._observe"), \
                    self.assertRaisesRegex(RuntimeError, "resource stop") as caught:
                supervise([sys.executable, "-I", "-c", "import time; time.sleep(10)"], Path(name),
                    timeout_s=2, grace_s=1, monitor=monitor)
            report = caught.exception.report
            self.assertTrue(report["observed_owned_exit_confirmed"])
            self.assertEqual(report["remaining_owned_pids"], [])
            self.assertTrue(observed)

    def test_full_campaign_fixture_freezes_profiles_and_permanent_units(self):
        self._campaign_fixture(fail=False)

    def test_campaign_fixture_stops_first_failure_without_refund_or_next_owner(self):
        self._campaign_fixture(fail=True)

    def test_global_deadline_stops_inside_supervision_and_reserves_physical_cleanup(self):
        self._campaign_fixture(fail=True, deadline=True)

    def test_final_gpu_observation_failure_cannot_return_completed(self):
        self._campaign_fixture(fail=False, final_failure="gpu")

    def test_final_cleanup_wall_overrun_cannot_return_completed(self):
        self._campaign_fixture(fail=False, final_failure="deadline")

    def test_lease_release_wall_overrun_cannot_return_completed(self):
        self._campaign_fixture(fail=False, final_failure="lease_deadline")

    def test_final_gpu_failure_preserves_existing_query_error(self):
        self._campaign_fixture(fail=True, final_failure="gpu")

    def _campaign_fixture(self, *, fail, deadline=False, final_failure=None):
        """Declared synthetic transport/hardware; tests orchestration, never a model."""
        from src.baselines.text.movie_records import MovieRecordSource, prepare_movie_source
        from src.experiments.sembench_records_formal import run_campaign
        from src.baselines.common.private_artifacts import write_private_json
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            package = root / "package"
            package.mkdir()
            raw = root / "reviews.csv"
            raw.write_text("reviewId,id,reviewText,scoreSentiment\na,taken_3,synthetic,NEGATIVE\n")
            manifest = prepare_movie_source(raw, package / "input", expected_csv_sha256=hashlib.sha256(raw.read_bytes()).hexdigest(), max_rows=1)
            source = MovieRecordSource.from_manifest(manifest)
            write_private_json(package / "package-identity.json", {"sembench_checkout": "unused-cpu-fixture"})
            authorization = root / "authorization.json"
            authorization.write_text("{}")
            lease_file = root / "lease"
            lease_file.write_text("declared CPU fixture lease")
            released = []

            def release():
                released.append(True)
                if final_failure == "lease_deadline":
                    clock_time[0] = 30251.0

            lease = SimpleNamespace(path=lease_file, release=release)
            backends = ["lotus-native", "semloom-core", "ray-data", "daft", "imlane"]
            units = []
            for phase, count in (("calibration", 2), ("measured", 5)):
                for block in range(count):
                    for backend in backends:
                        query = f"cpu-{phase}-{block}-{backend}"
                        configs = []
                        for point in ("p1", "p2"):
                            file = query + "-" + point + ".json"
                            write_private_json(package / file, dict(
                                method=dict(model_name="openai/qwen2.5-7b"), generation={}))
                            configs.append(dict(profile=point, path=file, sha256=hashlib.sha256((package / file).read_bytes()).hexdigest()))
                        unit = dict(query_id=query, backend=backend, phase=phase, block=block,
                            model_call_upper_bound=1, selected_profile="p" + str(block + 1) if phase == "calibration" else None,
                            configurations=configs)
                        file = query + ".json"
                        write_private_json(package / file, unit)
                        units.append(dict(query_id=query, manifest=file))
            schedule = dict(units=units, serial_unit_ceiling_s=5)
            runtime = dict(driver_python=sys.executable, cuda_visible_devices="0", imlane_artifact_root=str(root / "host"))
            hardware = dict(uuid="declared-cpu-fixture", memory_mib=0, compute_processes=[])
            monitor = SimpleNamespace(finish=lambda: {"passed": True, "scope": "synthetic CPU fixture"})
            invoked = []
            clock_time = [100.0]

            def snapshot(_device):
                last = units[0 if fail else -1]["query_id"]
                if (root / "campaign" / last / "physical.json").exists():
                    if final_failure == "gpu":
                        raise OSError("fixture final GPU observation failed")
                    if final_failure == "deadline":
                        clock_time[0] = 30251.0
                return hardware

            def transport(argv, output, **kwargs):
                invoked.append(argv)
                if deadline:
                    clock_time[0] = 124.0
                    kwargs["monitor"](())
                if fail:
                    raise RuntimeError("declared fixture transport failure")
                context = json.loads((output / "context.json").read_text())
                budget = context["budget"]
                unit_id = budget["unit_id"]
                ledger = CellBudgetLedger(Path(budget["path"]), AttemptBudget(budget["id"], budget["limit"]))
                ledger.claim_unit(unit_id)
                expected = source.expected(unit_id)
                calls = CallLedger(expected, 1)
                calls.record(dict(expected[0], status="succeeded", messages_sha256="a" * 64,
                    prompt_tokens=1, completion_tokens=1, total_tokens=2, text="False", finish_reason="stop",
                    usage_source="declared CPU fixture, zero inference"))
                write_private_json(output / "query.json", dict(status="completed", calls=calls.report(),
                    decisions=[dict(row_id=expected[0]["row_id"], decision=False)],
                    result=dict(positive_review_cnt=0), quality=dict(original_metric={"synthetic": True})))
                clock = QueryClock()
                clock("result_ready")
                clock("owner_closed")
                write_private_json(output / "clock.json", clock.events)
                return {"observed_owned_exit_confirmed": True}
            with patch("src.experiments.sembench_records_formal.validate_authorization", return_value=dict(
                    budget_id="cpu-campaign-fixture", limit_calls=35, max_wall_s=53 if deadline else 30150)), \
                    patch("src.experiments.sembench_records_formal.time.monotonic", side_effect=lambda: clock_time[0]), \
                    patch("src.experiments.sembench_records_formal.check_package", return_value=(schedule, runtime)), \
                    patch.object(os, "sched_getaffinity", return_value=set(range(8)), create=True), \
                    patch("integrations.imlane_records.resource_limits.gpu_snapshot", side_effect=snapshot), \
                    patch("src.infrastructure.runner_lease.acquire_runner_lease", return_value=lease), \
                    patch("src.experiments.sembench_records_formal_resources.ResourceMonitor", return_value=monitor), \
                    patch("src.experiments.lotus_records_supervisor.supervise", side_effect=transport):
                if final_failure and not fail:
                    expected_error = OSError if final_failure == "gpu" else TimeoutError
                    with self.assertRaises(expected_error):
                        run_campaign(package, authorization, root / "campaign")
                    report = json.loads((root / "campaign/campaign.json").read_text())
                    self.assertEqual(report["status"], "failed")
                    self.assertEqual(len(invoked), 35)
                    self.assertEqual(report["permanent_budget"]["allocated_requests"], 35)
                    self.assertEqual(released, [] if final_failure == "gpu" else [True])
                    self.assertEqual(report["physical_exit_confirmed"], final_failure != "gpu")
                    if final_failure in ("deadline", "lease_deadline"):
                        self.assertEqual(report["deadline_overrun_seconds"], 1)
                elif fail:
                    with self.assertRaisesRegex(TimeoutError if deadline else RuntimeError,
                            "physical-cleanup window" if deadline else "fixture transport failure"):
                        run_campaign(package, authorization, root / "campaign")
                    report = json.loads((root / "campaign/campaign.json").read_text())
                    self.assertEqual(len(invoked), 1)
                    self.assertEqual(report["permanent_budget"]["allocated_requests"], 1)
                    self.assertEqual(released, [])
                    self.assertEqual(report["lease_status"], "held_unresolved")
                    self.assertEqual(report["status"], "failed")
                else:
                    report = run_campaign(package, authorization, root / "campaign")
                    self.assertEqual(len(invoked), 35)
                    self.assertEqual(report["status"], "completed")
                    self.assertEqual(report["permanent_budget"]["allocated_requests"], 35)
                    self.assertTrue(all(u["claimed"] == 1 for u in report["permanent_budget"]["units"]))
                    self.assertEqual(len(report["selected_profiles"]), 5)
                    self.assertEqual(len(report["queries"]), 35)
                    self.assertEqual(released, [True])


@unittest.skipUnless(os.environ.get("SEMBENCH_CHECKOUT"), "requires fixed SemBench checkout and CPU LOTUS")
class MovieClockTests(unittest.TestCase):
    def test_original_parser_and_count_observer_order_on_nonempty_cpu_fixture(self):
        from src.baselines.text.movie_records import MovieRecordSource, prepare_movie_source
        from src.experiments.lotus_records_movie import run_movie_query
        from src.serving.backends.vllm_engine import GenerationSpec
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            raw = root / "reviews.csv"
            with raw.open("w", newline="") as stream:
                writer = csv.writer(stream)
                writer.writerow(["reviewId", "id", "reviewText", "scoreSentiment"])
                writer.writerows([["a", "taken_3", "good", "POSITIVE"], ["b", "taken_3", "bad", "NEGATIVE"]])
            manifest = prepare_movie_source(raw, root / "input", expected_csv_sha256=hashlib.sha256(raw.read_bytes()).hexdigest(), max_rows=2)
            source = MovieRecordSource.from_manifest(manifest)
            expected = source.expected("cpu-timing")
            rows = [dict(row, status="succeeded", text="True" if i == 0 else "False", prompt_tokens=1,
                completion_tokens=1, total_tokens=2, engine_request_id=str(i), finish_reason="stop") for i, row in enumerate(expected)]
            clock = QueryClock()
            report = run_movie_query(source, os.environ["SEMBENCH_CHECKOUT"], FilterMethod("openai/local-fixture"),
                GenerationSpec(), lambda: iter(rows), query_id="cpu-timing", observer=clock)
            events = [r["event"] for r in clock.events]
            self.assertEqual(report["status"], "completed")
            self.assertEqual(report["result"]["positive_review_cnt"], 1)
            self.assertEqual(events, ["query_source_start", "query_source_ready", "result_ready",
                "evaluation_start", "evaluation_end", "query_cleanup_end"])

    def test_cleanup_observer_failure_retains_real_known_fixture_ledger(self):
        from src.baselines.text.movie_records import MovieRecordSource, prepare_movie_source
        from src.experiments.lotus_records_movie import run_movie_query
        from src.serving.backends.vllm_engine import GenerationSpec
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            raw = root / "reviews.csv"
            raw.write_text("reviewId,id,reviewText,scoreSentiment\na,taken_3,good,POSITIVE\n")
            manifest = prepare_movie_source(raw, root / "input", expected_csv_sha256=hashlib.sha256(raw.read_bytes()).hexdigest(), max_rows=1)
            source = MovieRecordSource.from_manifest(manifest)
            row = dict(source.expected("cpu-observer-error")[0], status="succeeded", text="True",
                prompt_tokens=1, completion_tokens=1, total_tokens=2, engine_request_id="one", finish_reason="stop")
            def observe(event):
                if event == "query_cleanup_end":
                    raise RuntimeError("fixture observation failure")
            with self.assertRaisesRegex(RuntimeError, "observation failure") as caught:
                run_movie_query(source, os.environ["SEMBENCH_CHECKOUT"], FilterMethod("openai/local-fixture"),
                    GenerationSpec(), lambda: iter([row]), query_id="cpu-observer-error", observer=observe)
            report = caught.exception.query_report
            self.assertEqual(report["status"], "failed")
            self.assertEqual(report["calls"]["counts"]["succeeded"], 1)
            self.assertEqual(report["calls"]["usage"]["total_tokens"], 2)
            self.assertEqual(report["result"]["positive_review_cnt"], 1)


@unittest.skipUnless(os.environ.get("SEMBENCH_CHECKOUT"), "requires pinned CPU libraries and SemBench")
class PublicWorkerAdapterTests(unittest.TestCase):
    def test_public_worker_runs_actual_native_adapter_with_cpu_http_fixture(self):
        from tests.baselines.text.test_lotus_native_local_model import NativeLocalTests
        fixture = NativeLocalTests("test_original_accessor_lm_thread_pool_decisions_usage_and_quality")
        self.addCleanup(fixture.doCleanups)
        fixture.setUp()
        report = self._worker(fixture, "lotus-native", fixture.profile)
        self.assertTrue(report["native_scheduling_retained"])
        self.assertEqual(len(fixture.wire.requests), 4)

    def test_public_worker_runs_actual_imlane_adapter_with_cpu_author_carrier(self):
        from tests.experiments.test_imlane_movie_formal import IMLaneMovieFormalTests
        fixture = IMLaneMovieFormalTests("test_original_q3_parser_count_evaluator_usage_and_actual_messages")
        self.addCleanup(fixture.doCleanups)
        fixture.setUp()
        report = self._worker(fixture, "imlane", asdict(fixture.resources))
        self.assertEqual(report["backend_close"]["status"], "closed")
        self.assertEqual(len(fixture.carriers), 1)

    def _worker(self, fixture, backend, profile):
        """Real public worker, claim, adapter and Q3; only hardware/transport are fixtures."""
        from src.experiments.sembench_records_formal import worker
        from src.baselines.common.private_artifacts import write_private_json
        package = fixture.root / "package"
        package.mkdir()
        shutil.copytree(fixture.source.manifest_path.parent, package / "input")
        for directory in ("units", "configs"):
            (package / directory).mkdir()
        config = dict(engine=asdict(fixture.engine), method=asdict(fixture.method),
            generation=asdict(fixture.generation), resources=asdict(fixture.resources))
        if backend == "lotus-native":
            config["native_options"] = profile
        config_path = package / "configs/q.json"
        write_private_json(config_path, config)
        count = len(fixture.source.expected("q"))
        unit = dict(query_id="q", backend=backend, model_call_upper_bound=count,
            configurations=[dict(path="configs/q.json", sha256=hashlib.sha256(config_path.read_bytes()).hexdigest())])
        unit_path = package / "units/q.json"
        write_private_json(unit_path, unit)
        write_private_json(package / "runtime-binding.json", fixture.context["runtime_binding"])
        ledger = CellBudgetLedger.create(fixture.root / "public-worker-budget.sqlite",
            AttemptBudget("cpu-public-worker", count), deadline_utc=None)
        ledger.reserve_unit("q", count)
        authorization = fixture.root / "public-worker-authorization.json"
        authorization.write_text("{}")
        context = dict(owner_context=fixture.context["owner_context"],
            budget=dict(path=str(ledger.path), id="cpu-public-worker", limit=count, unit_id="q"),
            authorization_path=str(authorization), authorization_sha256=hashlib.sha256(authorization.read_bytes()).hexdigest(),
            assigned_gpu_uuid="GPU-00000000-0000-0000-0000-000000000000",
            sembench_checkout=os.environ["SEMBENCH_CHECKOUT"])
        context_path = fixture.output / "context.json"
        write_private_json(context_path, context)
        with patch("src.experiments.sembench_records_formal.check_package"), \
                patch.dict(os.environ), \
                patch("src.experiments.sembench_records_formal.validate_authorization", return_value=dict(
                    budget_id="cpu-public-worker", limit_calls=count)), \
                patch("integrations.imlane_records.chain_runtime.check_external_lease"), \
                patch("integrations.imlane_records.resource_limits.gpu_snapshot", return_value=dict(
                    uuid=context["assigned_gpu_uuid"], compute_processes=[], memory_mib=0)):
            worker(package, unit_path, config_path, context_path, fixture.output)
        report = json.loads((fixture.output / "query.json").read_text())
        self.assertEqual(report["status"], "completed")
        self.assertEqual(report["calls"]["counts"]["succeeded"], count)
        self.assertEqual([e["event"] for e in json.loads((fixture.output / "clock.json").read_text())].count("result_ready"), 1)
        self.assertEqual(ledger.snapshot()["allocated_requests"], count)
        self.assertEqual(ledger.snapshot()["units"][0]["claimed"], 1)
        with self.assertRaises(BudgetError):
            ledger.claim_unit("q")
        return report


class ResourceMonitorTests(unittest.TestCase):
    def test_sampled_rss_shm_or_affinity_violation_stops(self):
        from src.experiments.sembench_records_formal_resources import ResourceMonitor
        for rss, shm, affinity in ((11, 0, [0]), (1, 11, [0]), (1, 0, [1])):
            with self.subTest(rss=rss, shm=shm, affinity=affinity), tempfile.TemporaryDirectory() as name:
                monitor = ResourceMonitor(dict(cpu_affinity_slots=[0], max_sampled_owned_rss_bytes=10,
                    max_owned_shm_bytes=10), shm_root=Path(name), sample_seconds=0)
                process = SimpleNamespace(create_time=lambda: 1.0, status=lambda: "running",
                    memory_info=lambda: SimpleNamespace(rss=rss), cpu_affinity=lambda: affinity)
                with patch("src.experiments.sembench_records_formal_resources.psutil.Process", return_value=process), \
                        patch.object(monitor, "_shm_used", return_value=monitor.initial_shm + shm), \
                        self.assertRaisesRegex(RuntimeError, "ceiling"):
                    monitor([(123, 1.0)])
                self.assertFalse(monitor.finish()["passed"])

    def test_reused_pid_not_counted_and_remaining_shm_blocks_release(self):
        from src.experiments.sembench_records_formal_resources import ResourceMonitor
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            monitor = ResourceMonitor(dict(cpu_affinity_slots=[0], max_sampled_owned_rss_bytes=10,
                max_owned_shm_bytes=10), shm_root=root, sample_seconds=0)
            process = SimpleNamespace(create_time=lambda: 2.0)
            with patch("src.experiments.sembench_records_formal_resources.psutil.Process", return_value=process):
                monitor([(123, 1.0)])
            self.assertEqual(monitor.samples[0]["observed_rss_sum_bytes"], 0)
            (root / "unresolved-owned-fixture").write_text("fixture")
            self.assertFalse(monitor.finish()["passed"])


if __name__ == "__main__":
    unittest.main()
