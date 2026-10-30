"""CPU-only driver/probe fixtures; no GPU SDK initialization or network."""

from email.message import Message
import io
import json
import os
from pathlib import Path
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from urllib import error, request, response

from src.observability.metrics.resources import gpu_metadata, host_metadata
from src.observability.records_telemetry import (
    FIELDS, MetricSources, RecordsTelemetry, TelemetryEvents, phase_windows,
)


def fixture_sample():
    return dict(values={field: float(index) for index, field in enumerate(FIELDS)}, missing={}, sources={})


class RecordsTelemetryTests(unittest.TestCase):
    def test_cuda_single_device_binding_fixes_the_ordinal_reader_mismatch(self):
        nvml = SimpleNamespace(NVMLError=RuntimeError,nvmlInit=Mock(),nvmlShutdown=Mock(),
            nvmlDeviceGetCount=lambda:1,nvmlDeviceGetHandleByIndex=lambda i:('index',i),
            nvmlDeviceGetMigMode=lambda handle:(0,0),
            nvmlDeviceGetUUID=lambda handle:b'GPU-fixture',
            nvmlDeviceGetHandleByUUID=Mock(return_value='UUID-handle'),
            nvmlDeviceGetUtilizationRates=lambda handle:SimpleNamespace(gpu=42),
            nvmlDeviceGetMemoryInfo=lambda handle:SimpleNamespace(used=12*1024**2,total=24*1024**2),
            nvmlDeviceGetPowerUsage=lambda handle:150000)
        with patch.dict('sys.modules',pynvml=nvml),patch.dict(os.environ,CUDA_VISIBLE_DEVICES='0'), \
                patch('subprocess.run',side_effect=AssertionError('no probe subprocess')), \
                patch('src.observability.records_telemetry.host_metadata',return_value={}):
            old=MetricSources(('0',))()
            source=MetricSources.from_cuda_visibility();result=source()
        self.assertIsNone(old['values']['gpu_utilization_pct'])
        self.assertEqual(source.gpu_ids,('GPU-fixture',))
        self.assertEqual(result['values']['gpu_utilization_pct'],42)
        self.assertEqual(result['values']['gpu_memory_used_mib'],12)
        self.assertEqual(result['sources']['gpu']['physical_gpu_uuids'],['GPU-fixture'])
        nvml.nvmlDeviceGetHandleByUUID.assert_called_once_with('GPU-fixture')
        self.assertEqual(nvml.nvmlInit.call_count,nvml.nvmlShutdown.call_count)

    def test_uuid_cuda_mask_preserves_selection_order_and_still_verifies_reader_identity(self):
        with patch.dict(os.environ,CUDA_VISIBLE_DEVICES='GPU-b,GPU-a'), \
                patch('src.observability.records_telemetry.gpu_metadata',return_value={})as read, \
                patch('src.observability.records_telemetry.host_metadata',return_value={}):
            source=MetricSources.from_cuda_visibility();source()
        self.assertEqual(source.gpu_ids,('GPU-b','GPU-a'))
        read.assert_called_once_with(('GPU-b','GPU-a'),use_nvml=True)

    def test_ambiguous_ordinals_and_unavailable_NVML_remain_unknown_without_host_device_guessing(self):
        for visibility,count in [('0',2),('0',0),('0,1',2),('',1),('GPU-a,0',2)]:
            with self.subTest(visibility=visibility,count=count):
                nvml=SimpleNamespace(nvmlInit=Mock(),nvmlShutdown=Mock(),nvmlDeviceGetCount=lambda:count,
                    nvmlDeviceGetHandleByIndex=Mock(side_effect=AssertionError('must not select ambiguous GPU')))
                with patch.dict('sys.modules',pynvml=nvml), \
                        patch('src.observability.records_telemetry.gpu_metadata')as read, \
                        patch('src.observability.records_telemetry.host_metadata',return_value={'host_memory_used_pct':40}):
                    source=MetricSources.from_cuda_visibility(visibility);result=source()
                read.assert_not_called();nvml.nvmlDeviceGetHandleByIndex.assert_not_called()
                self.assertEqual(source.gpu_ids,())
                self.assertIsNone(result['values']['gpu_utilization_pct'])
                self.assertIn('CUDA_UUID_binding',result['missing']['gpu_utilization_pct'])
                self.assertEqual(result['values']['host_memory_used_pct'],40)
        with patch.dict('sys.modules',pynvml=None):
            source=MetricSources.from_cuda_visibility('0')
        self.assertIsNotNone(source.gpu_binding_error)

    def test_single_device_identity_drift_is_unknown_and_cannot_publish_another_devices_values(self):
        nvml=SimpleNamespace(NVMLError=RuntimeError,nvmlInit=Mock(),nvmlShutdown=Mock(),
            nvmlDeviceGetCount=lambda:1,nvmlDeviceGetHandleByIndex=lambda i:i,
            nvmlDeviceGetMigMode=lambda handle:(0,0),
            nvmlDeviceGetUUID=Mock(side_effect=[b'GPU-before',b'GPU-after']),
            nvmlDeviceGetHandleByUUID=lambda uid:uid,
            nvmlDeviceGetUtilizationRates=Mock(side_effect=AssertionError('wrong UUID must stop before values')))
        with patch.dict('sys.modules',pynvml=nvml), \
                patch('src.observability.records_telemetry.host_metadata',return_value={}):
            source=MetricSources.from_cuda_visibility('0');value=source()
        self.assertEqual(source.gpu_ids,('GPU-before',))
        self.assertIsNone(value['values']['gpu_utilization_pct'])
        self.assertIn('unavailable:nvml:ValueError',value['missing']['gpu_utilization_pct'])
        nvml.nvmlDeviceGetUtilizationRates.assert_not_called()

    def test_UUID_mapping_preserves_inherited_mask_and_MIG_ambiguity(self):
        with patch.dict(os.environ,NVIDIA_VISIBLE_DEVICES='GPU-other'):
            self.assertIsNotNone(MetricSources.from_cuda_visibility('GPU-fixture').gpu_binding_error)
        nvml=SimpleNamespace(nvmlInit=lambda:None,nvmlShutdown=Mock(),nvmlDeviceGetCount=lambda:1,
            nvmlDeviceGetHandleByIndex=lambda i:i,nvmlDeviceGetMigMode=lambda handle:(1,1),
            nvmlDeviceGetUUID=Mock(side_effect=AssertionError('MIG ordinal must remain unknown')))
        with patch.dict('sys.modules',pynvml=nvml):
            self.assertIsNotNone(MetricSources.from_cuda_visibility('0').gpu_binding_error)
        nvml.nvmlDeviceGetUUID.assert_not_called();nvml.nvmlShutdown.assert_called_once()

    def test_native_metrics_errors_preserve_unavailable_reason(self):
        with patch("src.observability.records_telemetry.gpu_metadata", return_value={}), \
                patch("src.observability.records_telemetry.host_metadata", return_value={}), \
                patch("src.observability.records_telemetry.scrape_prometheus_metrics",
                      side_effect=error.URLError("fixture connection refused")) as http:
            result = MetricSources(("GPU-fixture",), "http://127.0.0.1:1234/metrics")()
        http.assert_called_once_with("http://127.0.0.1:1234/metrics", timeout_s=1.0,
                                     require_direct_response=True)
        self.assertIn("unavailable", result["sources"]["native"]["status"])
        self.assertIn("URLError", result["missing"]["vllm_num_requests_running"])

    def test_native_response_without_required_fields_is_not_observed(self):
        with patch("src.observability.records_telemetry.gpu_metadata", return_value={}), \
                patch("src.observability.records_telemetry.host_metadata", return_value={}), \
                patch("src.observability.records_telemetry.scrape_prometheus_metrics", return_value={}):
            result = MetricSources(("GPU-fixture",), "http://127.0.0.1:1234/metrics")()
        self.assertEqual(result["sources"]["native"]["status"],
                         "unavailable:required_fields_not_exposed")
        self.assertIn("required_fields_not_exposed", result["missing"]["vllm_num_requests_running"])

    def test_direct_native_metrics_reject_redirect_without_remote_read_and_keep_legacy_error_default(self):
        from src.observability.metrics.vllm import scrape_prometheus_metrics

        observed = []
        class FixtureHTTP(request.HTTPHandler):
            def http_open(self, req):
                observed.append(req.full_url)
                headers = Message()
                if req.full_url == "http://localhost/metrics":
                    headers["Location"] = "http://remote.invalid/metrics"
                    result = response.addinfourl(io.BytesIO(b""), headers, req.full_url, 302)
                    result.msg = "Found"
                else:
                    result = response.addinfourl(io.BytesIO(b"vllm:num_requests_running 7\n"),
                                                headers, req.full_url, 200)
                    result.msg = "OK"
                return result

        build = request.build_opener
        legacy = build(request.ProxyHandler({}), FixtureHTTP())
        with patch("src.observability.metrics.vllm.request.build_opener",
                   side_effect=lambda *handlers: build(*handlers, FixtureHTTP())), \
                patch("src.observability.metrics.vllm.request.urlopen", side_effect=legacy.open), \
                patch("src.observability.records_telemetry.gpu_metadata", return_value={}), \
                patch("src.observability.records_telemetry.host_metadata", return_value={}), \
                patch("socket.create_connection", side_effect=AssertionError("network forbidden")):
            result = MetricSources(("GPU-fixture",), "http://localhost/metrics")()
        self.assertEqual(observed, ["http://localhost/metrics"])
        self.assertIsNone(result["values"]["vllm_num_requests_running"])
        self.assertIn("HTTPError", result["sources"]["native"]["status"])
        with patch("src.observability.metrics.vllm.request.urlopen", side_effect=error.URLError("fixture")):
            self.assertEqual(scrape_prometheus_metrics("http://localhost/metrics"), {})

    def test_event_fsync_failure_is_not_successful_flush_confirmation(self):
        with tempfile.TemporaryDirectory() as name:
            path = Path(name) / "events.jsonl"
            sink = TelemetryEvents(path)
            sink.mark("owner_start")
            with patch("src.baselines.common.private_artifacts.os.fsync",
                       side_effect=OSError("fixture fsync failure")):
                sink.close()
            self.assertTrue(sink._stream.closed)
            status = json.loads(Path(str(path) + ".status.json").read_text())
            self.assertFalse(status["flush_confirmed"])
            self.assertTrue(status["errors"])

    def test_sampler_fsync_failure_is_not_successful_flush_confirmation(self):
        ready = threading.Event()
        def sample():
            ready.set()
            return fixture_sample()
        with tempfile.TemporaryDirectory() as name:
            telemetry = RecordsTelemetry(Path(name) / "perf", gpu_ids=("GPU-fixture",), sample_fn=sample).start()
            self.assertTrue(ready.wait(2))
            with patch("src.baselines.common.private_artifacts.os.fsync",
                       side_effect=OSError("fixture fsync failure")):
                report = telemetry.close()
            self.assertTrue(report["collector_thread_stopped"])
            self.assertTrue(telemetry._stream.closed)
            self.assertFalse(report["flush_confirmed"])
            self.assertFalse(report["raw_samples_complete"])

    def test_nonregular_event_and_status_files_are_rejected_before_read(self):
        original_read = Path.read_text
        for target in ("events", "status"):
            with self.subTest(target=target), tempfile.TemporaryDirectory() as name:
                root = Path(name)
                event = root / "events.jsonl"
                status = Path(str(event) + ".status.json")
                fifo = event if target == "events" else status
                if target == "status":
                    event.write_text("")
                os.mkfifo(fifo)
                def guarded_read(path, *args, **kwargs):
                    if path == fifo:
                        raise AssertionError("fixture forbidden FIFO read")
                    return original_read(path, *args, **kwargs)
                with patch.object(Path, "read_text", guarded_read):
                    with RecordsTelemetry(root / "perf", gpu_ids=("GPU-fixture",),
                                          event_paths=(event,), sample_fn=fixture_sample) as telemetry:
                        result = 7
                self.assertEqual(result, 7)
                self.assertNotIn("fixture forbidden FIFO read", " ".join(telemetry.report["errors"]))
                self.assertIn("bounded regular telemetry", " ".join(telemetry.report["errors"]))



    def test_events_permission_or_existing_output_never_overwrites_or_masks_task_error(self):
        with tempfile.TemporaryDirectory() as name:
            path = Path(name) / "events.jsonl"
            path.write_text("existing evidence")
            with self.assertRaisesRegex(RuntimeError, "original query"):
                with TelemetryEvents(path) as sink:
                    sink.mark("owner_start")
                    raise RuntimeError("original query")
            self.assertEqual(path.read_text(), "existing evidence")
            self.assertTrue(sink.errors)

    def test_phase_inherits_worker_query_id(self):
        with tempfile.TemporaryDirectory() as name:
            path = Path(name) / "events.jsonl"
            with TelemetryEvents(path, query_id="q1") as sink:
                with sink.phase("session_startup"):
                    pass
            self.assertEqual([json.loads(x)["query_id"] for x in path.read_text().splitlines()], ["q1", "q1"])

    def test_probes_are_independent_missing_is_not_zero_and_cpu_first_tick_is_priming(self):
        host = {field: 10 for field in FIELDS if field.startswith("host_")}
        gpu = dict(gpu_metrics_status="snapshot:nvml", gpu_utilization_pct="0", gpu_memory_used_mib="12",
                   gpu_memory_total_mib="24", gpu_power_w="")
        with patch("src.observability.records_telemetry.gpu_metadata", return_value=gpu) as probe, \
                patch("src.observability.records_telemetry.host_metadata", return_value=host), \
                patch("src.observability.records_telemetry.scrape_prometheus_metrics") as http:
            source = MetricSources(("GPU-fixture",))
            first, second = source(), source()
        probe.assert_called_with(("GPU-fixture",), use_nvml=True)
        http.assert_not_called()
        self.assertEqual(first["values"]["gpu_utilization_pct"], 0)
        self.assertIsNone(first["values"]["gpu_power_w"])
        self.assertIn("unsupported", first["missing"]["gpu_power_w"])
        self.assertIsNone(first["values"]["host_cpu_busy_cores"])
        self.assertEqual(second["values"]["host_cpu_busy_cores"], 10)
        self.assertIn("no_HTTP_required", first["missing"]["vllm_num_requests_running"])

    def test_GPU_probe_permission_failure_does_not_discard_native_or_host_fields(self):
        with patch("src.observability.records_telemetry.gpu_metadata", side_effect=PermissionError("fixture")), \
                patch("src.observability.records_telemetry.host_metadata", return_value={"host_memory_used_pct": 40}), \
                patch("src.observability.records_telemetry.scrape_prometheus_metrics", return_value={
                    "vllm:num_requests_running": 2, "vllm:num_requests_waiting": 0}) as http:
            result = MetricSources(("GPU-fixture",), "http://127.0.0.1:1234/metrics")()
        self.assertIn("PermissionError", result["missing"]["gpu_utilization_pct"])
        self.assertEqual(result["values"]["host_memory_used_pct"], 40)
        self.assertEqual(result["values"]["vllm_num_requests_waiting"], 0)
        self.assertIsNone(result["values"]["vllm_kv_cache_usage_perc"])
        http.assert_called_once_with("http://127.0.0.1:1234/metrics", timeout_s=1.0,
                                     require_direct_response=True)

    def test_CPU_baseline_only_advances_after_first_valid_probe(self):
        host = {field: 10 for field in FIELDS if field.startswith("host_")}
        with patch("src.observability.records_telemetry.gpu_metadata", return_value={}), \
                patch("src.observability.records_telemetry.host_metadata", side_effect=[PermissionError("fixture"), host, host]):
            source = MetricSources(("GPU-fixture",))
            first, second, third = source(), source(), source()
        self.assertIsNone(first["values"]["host_cpu_busy_cores"])
        self.assertEqual(second["missing"]["host_cpu_busy_cores"], "cpu_percent_baseline_priming")
        self.assertEqual(third["values"]["host_cpu_busy_cores"], 10)

    def test_no_implicit_GPU_selection_or_remote_metrics_endpoint(self):
        with self.assertRaises(ValueError):
            MetricSources(())
        for url in ("http://remote.invalid/metrics", "http://user:pass@localhost/metrics", "https://localhost/metrics"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                MetricSources(("GPU-fixture",), url)

    def test_NVML_partial_fields_and_matched_init_shutdown_without_subprocess(self):
        class NVMLError(Exception):
            pass
        nvml = SimpleNamespace(NVMLError=NVMLError, nvmlInit=Mock(), nvmlShutdown=Mock(),
            nvmlDeviceGetHandleByUUID=Mock(return_value="handle"),
            nvmlDeviceGetUUID=Mock(return_value=b"GPU-fixture"),
            nvmlDeviceGetUtilizationRates=Mock(return_value=SimpleNamespace(gpu=70)),
            nvmlDeviceGetPowerUsage=Mock(side_effect=NVMLError("unsupported")),
            nvmlDeviceGetMemoryInfo=Mock(return_value=SimpleNamespace(used=1024**2, total=24*1024**2)))
        with patch.dict("sys.modules", pynvml=nvml), patch("subprocess.run") as command:
            result = gpu_metadata(("GPU-fixture",), use_nvml=True)
        command.assert_not_called()
        nvml.nvmlInit.assert_called_once()
        nvml.nvmlShutdown.assert_called_once()
        self.assertEqual(result["gpu_utilization_pct"], "70.0")
        self.assertEqual(result["gpu_memory_used_mib"], "1.0")
        self.assertEqual(result["gpu_power_w"], "")
        self.assertIn("NVMLError", result["gpu_metrics_status"])

    def test_NVML_absent_does_not_spawn_automatic_fallback(self):
        with patch.dict("sys.modules", pynvml=None), patch("subprocess.run") as command:
            result = gpu_metadata(("GPU-fixture",), use_nvml=True)
        command.assert_not_called()
        self.assertIn("unavailable:nvml", result["gpu_metrics_status"])

    def test_NVML_failed_init_cannot_shutdown_an_unowned_reference(self):
        nvml = SimpleNamespace(nvmlInit=Mock(side_effect=PermissionError("fixture")), nvmlShutdown=Mock())
        with patch.dict("sys.modules", pynvml=nvml):
            result = gpu_metadata(("GPU-fixture",), use_nvml=True)
        nvml.nvmlShutdown.assert_not_called()
        self.assertIn("PermissionError", result["gpu_metrics_status"])

    def test_host_reader_keeps_existing_shared_runner_fields(self):
        with patch("psutil.cpu_percent", return_value=[25, 75]) as cpu, \
                patch("psutil.virtual_memory", return_value=SimpleNamespace(percent=40, available=2*1024**2)):
            self.assertEqual(host_metadata(), dict(host_cpu_busy_cores=1, host_cpu_per_core_max_pct=75,
                                                  host_memory_used_pct=40, host_memory_available_mib=2))
        cpu.assert_called_once_with(interval=None, percpu=True)

    def test_host_permission_reason_is_available_without_changing_legacy_schema(self):
        with patch("psutil.cpu_percent", side_effect=PermissionError("fixture")):
            old, observed = host_metadata(), host_metadata(include_status=True)
        self.assertNotIn("host_metrics_status", old)
        self.assertEqual(observed["host_metrics_status"], "unavailable:PermissionError")

    def test_existing_shared_runner_reuses_host_reader_without_model_probe(self):
        from src.experiments.shared_vllm.runtime import _resource_sample
        host = dict(host_cpu_busy_cores=1, host_cpu_per_core_max_pct=25,
                    host_memory_used_pct=40, host_memory_available_mib=200)
        gpu = dict(gpu_metrics_status="snapshot", gpu_utilization_pct="50", gpu_memory_used_mib="12", gpu_power_w="90")
        with patch("src.experiments.shared_vllm.runtime.host_metadata", return_value=host) as reader, \
                patch("src.experiments.shared_vllm.runtime.gpu_metadata", return_value=gpu), \
                patch("src.experiments.shared_vllm.runtime.scrape_prometheus_metrics", return_value={}):
            sample = _resource_sample(("http://127.0.0.1/metrics",), time.time())[0]
        reader.assert_called_once_with()
        self.assertEqual({field: sample[field] for field in host}, host)

    def test_session_start_query_and_close_literal_events_form_distinct_windows(self):
        events = [dict(event=event, query_id=query, monotonic_ns=i*10) for i, (event, query) in enumerate([
            ("session_start", None), ("session_ready", None), ("query_start", "q1"),
            ("query_end", "q1"), ("session_close_start", None), ("session_closed", None)])]
        windows = phase_windows(events, 100)
        self.assertEqual([r["phase"] for r in windows], ["session_startup", "query_execution", "session_close"])
        self.assertTrue(all(r["status"] == "complete" for r in windows))

    def test_phase_windows_preserve_query_identity_and_incomplete_exit(self):
        events = [dict(event="phase_start", boundary="start", phase="query", query_id="q1", monotonic_ns=10),
                  dict(event="phase_end", boundary="end", phase="query", query_id="q1", monotonic_ns=30),
                  dict(event="owner_start", query_id="q2", monotonic_ns=40)]
        windows = phase_windows(events, 70)
        self.assertEqual(windows[0], dict(query_id="q1", phase="query", start_ns=10, end_ns=30, status="complete"))
        self.assertIn("incomplete", windows[1]["status"])
        self.assertEqual(windows[1]["query_id"], "q2")

    def test_actual_intervals_phase_split_final_flush_and_model_result_unchanged(self):
        reached, count = threading.Event(), [0]
        def sample():
            time.sleep(.003)
            count[0] += 1
            if count[0] >= 4:
                reached.set()
            return fixture_sample()
        with tempfile.TemporaryDirectory() as name:
            root = Path(name) / "performance"
            original = {"status": "completed", "result": {"positive_review_cnt": 7}}
            with RecordsTelemetry(root, gpu_ids=("GPU-fixture",), interval_s=.002, sample_fn=sample) as telemetry:
                with telemetry.events.phase("query", query_id="q1"):
                    self.assertTrue(reached.wait(2))
                    result = original
            report = telemetry.report
            self.assertIs(result, original)
            self.assertTrue(report["collector_thread_stopped"])
            self.assertTrue(report["flush_confirmed"])
            raw = [json.loads(line) for line in (root / "samples.jsonl").read_text().splitlines()]
            self.assertEqual(len(raw), report["summary"]["samples"])
            self.assertGreater(raw[1]["actual_interval_s"], .002)
            self.assertEqual(raw[1]["actual_interval_s"],
                             (raw[1]["sample_start_monotonic_ns"] - raw[0]["sample_start_monotonic_ns"]) / 1e9)
            self.assertGreater(report["phase_windows"][0]["summary"]["samples"], 0)
            self.assertEqual(report["phase_windows"][0]["query_id"], "q1")
            self.assertEqual(report["mfu"]["status"], "unavailable")
            self.assertFalse(report["mfu"]["estimated"])
            self.assertIs(telemetry.close(), report)

    def test_collection_errors_do_not_mask_original_task_exception(self):
        sampled = threading.Event()
        def broken():
            sampled.set()
            raise PermissionError("fixture inaccessible performance counter")
        with tempfile.TemporaryDirectory() as name:
            with self.assertRaisesRegex(RuntimeError, "original system error"):
                with RecordsTelemetry(Path(name) / "perf", gpu_ids=("GPU-fixture",), sample_fn=broken) as telemetry:
                    self.assertTrue(sampled.wait(2))
                    raise RuntimeError("original system error")
            self.assertTrue(telemetry.report["flush_confirmed"])
            field = telemetry.report["summary"]["field_coverage"]["gpu_utilization_pct"]
            self.assertEqual(field["valid_samples"], 0)
            self.assertIn("PermissionError", field["missing_reasons"][0])

    def test_existing_output_and_summary_IO_error_are_noncritical(self):
        with tempfile.TemporaryDirectory() as name:
            path = Path(name) / "existing"
            path.mkdir()
            prior = path / "prior.json"
            prior.write_text("existing evidence")
            with RecordsTelemetry(path, gpu_ids=("GPU-fixture",), sample_fn=fixture_sample) as telemetry:
                result = 7
            self.assertEqual(result, 7)
            self.assertEqual(prior.read_text(), "existing evidence")
            self.assertEqual(sorted(p.name for p in path.iterdir()), ["prior.json"])
            self.assertTrue(telemetry.report["errors"])
        with tempfile.TemporaryDirectory() as name:
            with patch("src.observability.records_telemetry.write_private_json", side_effect=PermissionError("fixture")):
                with RecordsTelemetry(Path(name) / "perf", gpu_ids=("GPU-fixture",), sample_fn=fixture_sample) as telemetry:
                    pass
            self.assertIn("PermissionError", telemetry.report["errors"][-1])
            self.assertTrue(telemetry.report["flush_confirmed"])

    def test_no_samples_is_not_observed(self):
        with tempfile.TemporaryDirectory() as name:
            with patch.object(RecordsTelemetry, "_sample_loop"):
                with RecordsTelemetry(Path(name) / "perf", gpu_ids=("GPU-fixture",), sample_fn=fixture_sample) as telemetry:
                    pass
            self.assertEqual(telemetry.report["status"], "unavailable:no_samples")

    def test_energy_uses_monotonic_when_UTC_moves_backwards(self):
        from src.observability.records_telemetry import _summary
        rows = [dict(values=fixture_sample()["values"], missing={},
                     sample_start_monotonic_ns=ns, sample_start_epoch_ns=epoch,
                     collection_wall_s=.01, collection_thread_cpu_s=.001)
                for ns, epoch in [(1000000000, 2000000000), (2000000000, 1000000000)]]
        for row in rows:
            row["sample_end_monotonic_ns"] = row["sample_start_monotonic_ns"] + 1
        result = _summary(rows, 1, 1)
        self.assertEqual(result["gpu_energy_j"], 3.0)
        self.assertIn("monotonic", result["energy_time_basis"])

    def test_start_is_idempotent_and_cannot_reset_time_or_launch_second_thread(self):
        with tempfile.TemporaryDirectory() as name:
            telemetry = RecordsTelemetry(Path(name) / "perf", gpu_ids=("GPU-fixture",), sample_fn=fixture_sample).start()
            started, thread = telemetry.started_ns, telemetry._thread
            self.assertIs(telemetry.start(), telemetry)
            self.assertEqual(telemetry.started_ns, started)
            self.assertIs(telemetry._thread, thread)
            telemetry.close()

    def test_close_before_start_cannot_later_launch_an_untracked_collector(self):
        with tempfile.TemporaryDirectory() as name:
            telemetry = RecordsTelemetry(Path(name) / "perf", gpu_ids=("GPU-fixture",), sample_fn=fixture_sample)
            report = telemetry.close()
            self.assertIs(telemetry.start(), telemetry)
            self.assertIsNone(telemetry._thread)
            self.assertIs(telemetry.close(), report)
            self.assertFalse(telemetry.output_dir.exists())

    def test_thread_start_failure_flushes_only_owned_streams_and_does_not_mask_result(self):
        with tempfile.TemporaryDirectory() as name:
            with patch("threading.Thread.start", side_effect=RuntimeError("fixture thread resource exhausted")):
                with RecordsTelemetry(Path(name) / "perf", gpu_ids=("GPU-fixture",), sample_fn=fixture_sample) as telemetry:
                    result = 7
            self.assertEqual(result, 7)
            self.assertTrue(telemetry.report["flush_confirmed"])
            self.assertTrue(telemetry.events._stream.closed)
            self.assertIn("unavailable", telemetry.report["status"])

    def test_loop_framework_failure_is_explicit_and_flushes(self):
        with tempfile.TemporaryDirectory() as name:
            with patch.object(RecordsTelemetry, "_sample_loop", side_effect=RuntimeError("fixture loop fault")):
                with RecordsTelemetry(Path(name) / "perf", gpu_ids=("GPU-fixture",), sample_fn=fixture_sample) as telemetry:
                    pass
            self.assertTrue(any("collector_loop_failed" in error for error in telemetry.report["errors"]))
            self.assertTrue(telemetry.report["flush_confirmed"])

    def test_worker_event_write_failure_is_visible_to_parent_summary(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            worker = root / "worker-events.jsonl"
            with TelemetryEvents(worker, query_id="q1") as sink:
                sink.mark("owner_start")
                with patch.object(sink._stream, "write", side_effect=PermissionError("fixture")):
                    sink.mark("owner_ready")
            status = json.loads(Path(str(worker) + ".status.json").read_text())
            self.assertEqual(status["events_written"], 1)
            self.assertTrue(status["errors"])
            with RecordsTelemetry(root / "perf", gpu_ids=("GPU-fixture",), event_paths=(worker,),
                                  sample_fn=fixture_sample) as telemetry:
                pass
            self.assertTrue(any("event_writer_incomplete" in error for error in telemetry.report["errors"]))
            self.assertIn("incomplete", telemetry.report["phase_windows"][0]["status"])

    def test_sampler_only_stops_its_own_thread_and_does_not_close_service(self):
        foreign_stop = threading.Event()
        foreign = threading.Thread(target=lambda: foreign_stop.wait(3), name="records-performance-telemetry", daemon=True)
        foreign.start()
        service = Mock()
        service.read.side_effect = fixture_sample
        try:
            with tempfile.TemporaryDirectory() as name:
                with RecordsTelemetry(Path(name) / "perf", gpu_ids=("GPU-fixture",), sample_fn=service.read) as telemetry:
                    pass
                self.assertTrue(telemetry.report["collector_thread_stopped"])
                self.assertTrue(foreign.is_alive())
                service.close.assert_not_called()
        finally:
            foreign_stop.set()
            foreign.join(1)

    def test_finite_sampling_cap_reports_remaining_window_unobserved(self):
        with tempfile.TemporaryDirectory() as name:
            with RecordsTelemetry(Path(name) / "perf", gpu_ids=("GPU-fixture",), sample_fn=fixture_sample,
                                  interval_s=.001, max_samples=2) as telemetry:
                telemetry._thread.join(2)
            self.assertEqual(telemetry.report["summary"]["samples"], 2)
            self.assertIn("sample_limit_reached", telemetry.report["errors"][0])
            self.assertTrue(telemetry.report["flush_confirmed"])


if __name__ == "__main__":
    unittest.main()
