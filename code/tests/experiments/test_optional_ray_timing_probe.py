"""Zero-start checks for the bounded fixture plan, source reads and new ledgers."""
import contextlib
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, call, patch


SCRIPT = Path(__file__).resolve().parents[2] / "scripts/benchmarks/probe_optional_ray_timing.py"
SPEC = importlib.util.spec_from_file_location("optional_ray_timing_probe", SCRIPT)
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)


class OptionalRayTimingProbe(unittest.TestCase):
    def test_default_is_plan_only_with_four_interleaved_cases_and_eight_calls(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output), patch.object(probe, "execute") as execute:
            self.assertEqual(probe.main([]), 0)
        execute.assert_not_called()
        plan = json.loads(output.getvalue())
        self.assertEqual([case["prefix_rows"] for case in plan["cases"]], [0, 2520, 2520, 0])
        self.assertEqual(sum(case["complete_http_calls"] for case in plan["cases"]), 8)
        self.assertEqual((plan["model_requests"], plan["gpu_slots"]), (0, 0))
        self.assertEqual(plan["pyarrow_version"], "24.0.0")
        self.assertEqual(plan["owner_lifecycle"], "independent new owner per graph")
        output = io.StringIO()
        with contextlib.redirect_stdout(output), patch.object(probe, "execute") as execute:
            probe.main(["--expected-pyarrow-version", "16.1.0"])
        execute.assert_not_called()
        self.assertEqual(json.loads(output.getvalue())["pyarrow_version"], "16.1.0")

    def test_execute_requires_explicit_new_output_before_any_launch(self):
        with contextlib.redirect_stderr(io.StringIO()), patch.object(probe, "execute") as execute:
            with self.assertRaises(SystemExit):
                probe.main(["--execute"])
        execute.assert_not_called()
        with contextlib.redirect_stderr(io.StringIO()), patch.object(probe, "execute") as execute:
            with self.assertRaises(SystemExit):
                probe.main(["--handoff", "unused.json", "--source-mapping-json", "unused.json"])
        execute.assert_not_called()

    def portable_mapping(self):
        sources = {"pre_send_ledger.py": probe.CODE / "scripts/benchmarks/sembench_pre_send_ledger.py",
                   "send_counts.py": probe.CODE / "src/experiments/send_counts.py",
                   "native_calls.py": probe.CODE / "src/baselines/text/frameworks/native_calls.py"}
        mapping = {name: {"working_sha256": probe.digest(path)} for name, path in sources.items()}
        calls = probe.graph_calls(sources["native_calls.py"].read_text())
        mapping["native_calls.py"].update(working_vendor_graph_calls=calls, retained_vendor_graph_calls=calls)
        return mapping

    def test_portable_mapping_rejects_source_sha_change_before_graph_check(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "mapping.json"
            mapping = self.portable_mapping()
            mapping["send_counts.py"]["working_sha256"] = "0" * 64
            path.write_text(json.dumps({"source_mapping": mapping}))
            with patch.object(probe, "verify_graph_mapping") as graph_check:
                with self.assertRaisesRegex(RuntimeError, "source changed after mapping: send_counts.py"):
                    probe.verified_source_mapping_json(path)
            graph_check.assert_not_called()

    def test_portable_mapping_rechecks_graph_signature_and_records_local_provenance(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "mapping.json"
            mapping = self.portable_mapping()
            path.write_text(json.dumps({"source_mapping": mapping}))
            with patch.object(probe, "source_mapping", side_effect=AssertionError("private helpers must not be read")):
                verified = probe.verified_source_mapping_json(path)
            self.assertEqual(verified["source_mapping_json_sha256"], probe.digest(path))
            self.assertEqual(verified["mapping_source"], "local_verified_handoff")
            self.assertFalse(verified["private_helpers_rechecked_here"])
            mapping["native_calls.py"]["retained_vendor_graph_calls"] = ["different graph"]
            path.write_text(json.dumps(mapping))
            with self.assertRaisesRegex(RuntimeError, "do not map"):
                probe.verified_source_mapping_json(path)

    def test_graph_mapping_retains_vendor_arguments_through_timing_only_wrappers(self):
        original = """def _ray_rows(calls, options, control):
 dataset = ray.data.from_items(calls, override_num_blocks=2)
 processor = build_processor(HttpRequestProcessorConfig(batch_size=1,concurrency=(2,2)))
 result = processor(dataset)
 iterator = result.iter_rows()
 executor.shutdown(force=True)
"""
        instrumented = """def _ray_rows(calls, options, control):
 with timing_span('native.graph_build'):
  dataset = ray.data.from_items(calls, override_num_blocks=2)
  processor = build_processor(HttpRequestProcessorConfig(batch_size=1,concurrency=(2,2)))
  result = processor(dataset)
 with timing_span('native.iterator_open'):
  iterator = result.iter_rows()
 with timing_span('native.cleanup'):
  executor.shutdown(force=True)
"""
        reference = probe.graph_calls(original)
        self.assertEqual(reference, probe.graph_calls(instrumented))
        mapping = {"native_calls.py": {"working_vendor_graph_calls": probe.graph_calls(instrumented),
                                        "retained_vendor_graph_calls": reference}}
        probe.verify_graph_mapping(mapping)
        mapping["native_calls.py"]["working_vendor_graph_calls"] = probe.graph_calls(
            instrumented.replace("concurrency=(2,2)", "concurrency=(1,1)"))
        with self.assertRaisesRegex(RuntimeError, "do not map"):
            probe.verify_graph_mapping(mapping)

        deferred = instrumented.replace(" with timing_span('native.cleanup'):\n  executor.shutdown(force=True)",
            " if detailed:\n  actions = []\n  actions.append(('executor_shutdown', lambda: executor.shutdown(force=True)))\n"
            "  _observed_graph_cleanup('ray-data', actions)\n else:\n  executor.shutdown(force=True)")
        self.assertEqual(reference, probe.graph_calls(deferred))

    def test_staged_site_hook_is_syntactically_valid_without_starting_it(self):
        compile(probe.SITE, "staged-sitecustomize", "exec")

    def test_prefix_is_readonly_and_excludes_request_values(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "closed.sqlite"
            with sqlite3.connect(source) as db:
                db.execute("CREATE TABLE sends(sequence INTEGER PRIMARY KEY,phase TEXT,kind TEXT,url TEXT,body TEXT)")
                db.executemany("INSERT INTO sends VALUES(?,'software','fixture','http://localhost/private','not copied')",
                               [(index,) for index in range(1, 2522)])
            original = source.read_bytes()
            rows, provenance = probe.prefix_rows(source)
            self.assertEqual(len(rows), 2520)
            self.assertEqual(rows[-1], (2520, "software", "fixture"))
            self.assertEqual(source.read_bytes(), original)
            self.assertEqual(provenance["sha256"], hashlib.sha256(original).hexdigest())
            self.assertNotIn("not copied", json.dumps(rows))
            self.assertNotIn("http://", json.dumps(rows))
            Path(str(source) + "-wal").touch()
            with self.assertRaisesRegex(ValueError, "closed ledger"):
                probe.prefix_rows(source)

    def test_short_or_gapped_prefix_is_rejected_without_writing_source(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "short.sqlite"
            with sqlite3.connect(source) as db:
                db.execute("CREATE TABLE sends(sequence INTEGER PRIMARY KEY,phase TEXT,kind TEXT)")
                db.executemany("INSERT INTO sends VALUES(?,'software','fixture')", [(1,), (3,)])
            before = source.read_bytes()
            with self.assertRaisesRegex(ValueError, "contiguous"):
                probe.prefix_rows(source)
            self.assertEqual(source.read_bytes(), before)

    def test_seed_uses_new_full_schema_and_new_inode_identity_with_exact_counters(self):
        for count in (0, 2520):
            with self.subTest(rows=count), tempfile.TemporaryDirectory() as directory:
                root = Path(directory) / "case"
                rows, _ = probe.prefix_rows()
                with patch.object(probe.subprocess, "Popen", side_effect=AssertionError("no process start")):
                    config = probe.stage_case(root, "source-test", rows[:count], "run-fixture")
                self.assertEqual(config["seed"]["snapshot"]["rows"], count)
                self.assertTrue(config["seed"]["snapshot"]["history_matches_counters"])
                marker = json.loads((root / "send-ledger-identity.json").read_text())
                self.assertEqual(marker["inode"], (root / "sends.sqlite").stat().st_ino)
                with sqlite3.connect(root / "sends.sqlite") as db:
                    self.assertEqual(db.execute("SELECT count(*) FROM send_totals").fetchone(), (5,))
                    self.assertEqual(db.execute("SELECT count(*) FROM fixture_received").fetchone(), (0,))
                    self.assertEqual(db.execute("SELECT count(*) FROM sends WHERE url IS NOT NULL").fetchone(), (0,))
                    self.assertEqual(db.execute("SELECT count(*) FROM sends WHERE body_sha256 IS NOT NULL").fetchone(), (0,))
                self.assertEqual(json.loads((root / "state.json").read_text())[
                    "minimal_sync_public_cli_software"]["max_posts"], count + 2)

    def test_retained_mapping_checks_source_sha_instead_of_trusting_path(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            retained = root / "retained.py"
            retained.write_text("def initialize():\n return 1\n")
            record = {"path": str(retained), "sha256": probe.digest(retained)}
            handoff = root / "handoff.json"
            handoff.write_text(json.dumps({"replay": {"sample_count": 192},
                "actual_ray_helpers": {"pre_send_ledger.py": record, "send_counts.py": record},
                "retained_ray_source": {"code/src/baselines/text/frameworks/native_calls.py": record}}))
            mapping = probe.source_mapping(handoff)
            self.assertEqual(mapping["existing_initialization_samples"], 192)
            self.assertEqual(mapping["send_counts.py"]["retained_sha256"], probe.digest(retained))
            retained.write_text("def initialize():\n return 2\n")
            with self.assertRaisesRegex(RuntimeError, "retained source SHA differs"):
                probe.source_mapping(handoff)

    def test_unverified_worker_clock_is_not_subtracted_from_driver_clock(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "timings").mkdir()
            base = dict(operation="probe.python_startup_hook", span_id="s", pid=12,
                        process_instance="process", clock_domain="another-domain", clock_verified=True)
            (root / "timings/timing-test.jsonl").write_text("\n".join(json.dumps(dict(base, **value)) for value in (
                dict(event="span_started", monotonic_ns=5), dict(event="span_ended", monotonic_ns=15))))
            row = probe.summarize_bootstrap(root, 10, 20, "driver-domain")[0]
            self.assertIn("cross-process comparison unavailable", row["status"])
            self.assertNotIn("inside_query_hook_seconds", row)
            self.assertIn("not an actor callback", row["actor_id"])

    def test_exit_without_writer_close_marker_is_never_reported_complete(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "timings").mkdir()
            path = root / "timings/timing-owner.jsonl"
            path.write_text(json.dumps(dict(pid=123, process_instance="owner")) + "\n")
            self.assertFalse(probe.writer_coverage(root, 123)["driver_verified"])
            marker = path.with_suffix(".closed.json")
            closed = dict(status="closed", pid=123, process_instance="owner", writer=dict(
                closed=True, error_type=None, pending_bytes=0, accepted_events=1, written_events=1))
            marker.write_text(json.dumps(closed))
            self.assertTrue(probe.writer_coverage(root, 123)["driver_verified"])
            closed["writer"]["written_events"] = 0
            marker.write_text(json.dumps(closed))
            self.assertFalse(probe.writer_coverage(root, 123)["driver_verified"])

    def test_timing_failures_still_close_and_save_original_attempts_and_error(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            original = ValueError("original execution failure")
            record = dict(status="failed", error_type="ValueError", error=str(original),
                          http_posts=1, fixture_receives=[{"sequence": 1}], query_start_ns=10)
            timing = Mock(clock_domain="clock")
            timing.record_interval.side_effect = OSError("recording failed")
            close = Mock(side_effect=OSError("close failed"))
            write = probe.write_private_json
            with patch.object(probe, "summarize_bootstrap", side_effect=LookupError("summary failed")) as summary, \
                 patch.object(probe, "writer_coverage", return_value={"driver_verified": False}) as coverage, \
                 patch.object(probe, "write_private_json", wraps=write) as save:
                with self.assertRaises(ValueError) as raised:
                    try:
                        raise original
                    finally:
                        probe.finish_case_evidence(root, record, timing, close, ray=None,
                            cleanup_started=20, cleanup_ended=30, primary=original)
            self.assertIs(raised.exception, original)
            close.assert_called_once_with()
            summary.assert_called_once()
            coverage.assert_called_once()
            save.assert_called_once()
            saved = json.loads((root / "result.json").read_text())
            self.assertEqual((saved["http_posts"], saved["error_type"], saved["status"]), (1, "ValueError", "failed"))
            self.assertNotIn("query_eof_ns", saved)
            self.assertFalse(saved["cleanup_confirmed"])
            self.assertTrue(saved["resource_cleanup_confirmed"])
            self.assertEqual({row["stage"] for row in saved["evidence_errors"]},
                             {"cleanup_timing", "timing_close", "bootstrap_summary", "writer_coverage"})

    def test_evidence_save_failure_does_not_hide_first_cleanup_error(self):
        first = OSError("first timing failure")
        timing = Mock(clock_domain="clock")
        timing.record_interval.side_effect = first
        close = Mock()
        record = dict(status="completed", http_posts=2)
        with patch.object(probe, "writer_coverage", return_value={"driver_verified": True}), \
             patch.object(probe, "write_private_json", side_effect=PermissionError("save failed")) as save:
            with self.assertRaises(OSError) as raised:
                probe.finish_case_evidence(Path("unused-private-root"), record, timing, close, ray=None,
                    cleanup_started=20, cleanup_ended=30)
        self.assertIs(raised.exception, first)
        close.assert_called_once_with()
        save.assert_called_once()
        self.assertEqual(record["status"], "failed")
        self.assertFalse(record["cleanup_confirmed"])
        self.assertTrue(any("result recording also failed: PermissionError" in note for note in first.__notes__))

    def test_nonzero_owner_exit_cleans_only_its_recorded_process_group(self):
        process = Mock(pid=54321, returncode=7)
        reader = Mock()
        reader.is_alive.return_value = False
        def kill(pgid, sig):
            self.assertEqual(pgid, process.pid)
            if sig == 0:
                raise ProcessLookupError()
        with patch.object(probe.os, "killpg", side_effect=kill) as killpg:
            error, cleanup = probe.supervise_owner(process, reader, [], timeout=1, case_id="fixture")
        self.assertIsInstance(error, RuntimeError)
        self.assertIn("fixture owner failed", str(error))
        self.assertEqual(killpg.call_args_list, [call(54321, probe.signal.SIGTERM), call(54321, 0)])
        self.assertTrue(cleanup["group_exit_confirmed"])
        self.assertTrue(cleanup["log_stream_closed"])

    def test_unclosed_reader_escalates_owned_group_and_retains_unknown_exit(self):
        process = Mock(pid=54322, returncode=0)
        reader = Mock()
        reader.is_alive.return_value = True
        with patch.object(probe.os, "killpg") as killpg:
            error, cleanup = probe.supervise_owner(process, reader, [], timeout=1, case_id="fixture")
        self.assertIn("log stream did not close", str(error))
        self.assertEqual(killpg.call_args_list, [call(54322, probe.signal.SIGTERM), call(54322, 0),
                                               call(54322, probe.signal.SIGKILL), call(54322, 0)])
        self.assertFalse(cleanup["group_exit_confirmed"])
        self.assertFalse(cleanup["log_stream_closed"])
        self.assertTrue(any("cleanup also failed" in note for note in error.__notes__))

    def test_owned_kill_failure_and_reader_error_preserve_original_wait_timeout(self):
        original = probe.subprocess.TimeoutExpired("owned-child", 1)
        process = Mock(pid=54323, returncode=9)
        process.wait.side_effect = [original, 9]
        reader = Mock()
        reader.is_alive.return_value = False
        with patch.object(probe.os, "killpg", side_effect=PermissionError("mock kill denied")) as killpg:
            error, cleanup = probe.supervise_owner(process, reader, [OSError("mock log error")],
                                                   timeout=1, case_id="fixture")
        self.assertIs(error, original)
        self.assertFalse(cleanup["group_exit_confirmed"])
        self.assertTrue(all(item.args[0] == 54323 for item in killpg.call_args_list))
        self.assertTrue(any("Child log observation also failed" in note for note in original.__notes__))

    def test_damaged_owner_result_retains_supervision_error_and_unknown_counts(self):
        original = probe.subprocess.TimeoutExpired("owned-child", 1)
        with patch.object(probe.Path, "read_text", return_value='{"status":'):
            result, primary = probe.read_owner_result(Path("unused-owned-case"), "fixture", original)
        self.assertIs(primary, original)
        self.assertEqual(result["read_error"]["error_type"], "JSONDecodeError")
        self.assertIsNone(result["http_posts"])
        self.assertFalse(result["cleanup_confirmed"])
        self.assertEqual(result["status"], "failed")
        self.assertNotIn("query_eof_ns", result)
        self.assertTrue(any("Owner result read also failed: JSONDecodeError" in note for note in original.__notes__))

    def test_missing_owner_result_becomes_primary_and_never_claims_zero_requests(self):
        missing = FileNotFoundError("mock missing final owner JSON")
        with patch.object(probe.Path, "read_text", side_effect=missing):
            result, primary = probe.read_owner_result(Path("unused-owned-case"), "fixture")
        self.assertIs(primary, missing)
        self.assertEqual(result["read_error"]["error_type"], "FileNotFoundError")
        self.assertIsNone(result["http_posts"])
        self.assertEqual(result["supervisor_cleanup_status"], "unknown")
        self.assertFalse(result["cleanup_confirmed"])

    def test_arrow_version_mismatch_stops_before_prefix_read_or_owner_start(self):
        packages = {"ray": "2.56.1", "pyarrow": "16.1.0", "aiohttp": "3.14.3"}
        with patch.object(probe.sys, "platform", "linux"), patch.object(probe.Path, "exists", return_value=True), \
             patch.dict(probe.os.environ, {}, clear=True), patch.object(probe, "version", side_effect=packages.__getitem__), \
             patch.object(probe, "prefix_rows") as prefix, patch.object(probe.subprocess, "Popen") as launch:
            with self.assertRaisesRegex(RuntimeError, "requires existing Ray 2.56.1 / Arrow 24.0.0"):
                probe.execute(SimpleNamespace(expected_pyarrow_version="24.0.0"))
        prefix.assert_not_called()
        launch.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
