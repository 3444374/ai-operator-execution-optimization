"""Optional records/local-model telemetry owned by the outer driver.

No model/framework imports, scheduling, signals, lease or budget operations.
The sampling thread uses in-process NVML/optional bounded metrics HTTP reads. Native
embedded engines need not expose HTTP: absent native metrics stay missing.
"""

from contextlib import contextmanager
import json
import math
import os
from pathlib import Path
import stat
import statistics
import threading
import time
from urllib.parse import urlsplit

from src.baselines.common.private_artifacts import (
    new_private_directory, open_private_text, write_private_json,
)
from src.baselines.common.redact import redact_json_values, redact_text
from src.observability.metrics.resources import gpu_metadata, host_metadata, resource_sample_stats
from src.observability.metrics.vllm import scrape_prometheus_metrics


GPU_FIELDS = ("gpu_utilization_pct", "gpu_memory_used_mib", "gpu_memory_total_mib", "gpu_power_w")
HOST_FIELDS = ("host_cpu_busy_cores", "host_cpu_per_core_max_pct",
               "host_memory_used_pct", "host_memory_available_mib")
NATIVE_FIELDS = {
    "vllm_num_requests_running": "vllm:num_requests_running",
    "vllm_num_requests_waiting": "vllm:num_requests_waiting",
    "vllm_kv_cache_usage_perc": "vllm:kv_cache_usage_perc",
}
FIELDS = GPU_FIELDS + HOST_FIELDS + tuple(NATIVE_FIELDS)


def _error(error):
    return redact_text(type(error).__name__ + ": " + str(error))


def _number(value):
    if isinstance(value, bool):
        return None
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) and value >= 0 else None


def _read_regular_text(path, maximum):
    """Validate and bound the same descriptor; nonblocking open also rejects FIFOs."""
    descriptor = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "rb") as stream:
        observed = os.fstat(stream.fileno())
        if not stat.S_ISREG(observed.st_mode) or observed.st_size > maximum:
            raise ValueError("bounded regular telemetry file required")
        raw = stream.read(maximum + 1)
        if len(raw) > maximum:
            raise ValueError("bounded regular telemetry file required")
    return raw.decode("utf-8")


class TelemetryEvents:
    """Fresh private JSONL sink for QueryClock; failures are diagnostic only."""

    def __init__(self, path, *, query_id=None):
        self.path, self.query_id = Path(path), query_id
        self.errors, self.closed = [], False
        self.flush_confirmed = False
        self.events_written, self.last_monotonic_ns = 0, None
        self._context = self._stream = None
        self._lock = threading.Lock()
        try:
            self._context = open_private_text(self.path)
            self._stream = self._context.__enter__()
        except Exception as error:
            self.errors.append(_error(error))

    def __call__(self, entry):
        with self._lock:
            try:
                if self.closed or self._stream is None:
                    return
                value = dict(query_id=self.query_id)
                value.update(entry)
                value["schema"] = "semloom.records_performance_event.v1"
                self._stream.write(json.dumps(redact_json_values(value), allow_nan=False) + "\n")
                self._stream.flush()
                self.events_written += 1
                self.last_monotonic_ns = value["monotonic_ns"]
            except Exception as error:
                self.errors.append(_error(error))

    def mark(self, event, **fields):
        self(dict(event=event, monotonic_ns=time.monotonic_ns(), epoch_ns=time.time_ns(), **fields))

    @contextmanager
    def phase(self, phase, *, query_id=None):
        query_id = self.query_id if query_id is None else query_id
        self.mark("phase_start", phase=phase, boundary="start", query_id=query_id)
        try:
            yield
        finally:
            self.mark("phase_end", phase=phase, boundary="end", query_id=query_id)

    def close(self):
        with self._lock:
            if not self.closed:
                self.closed = True
                if self._stream is not None:
                    try:
                        self._context.__exit__(None, None, None)
                        self.flush_confirmed = self._stream.closed
                    except Exception as error:
                        self.errors.append(_error(error))
                    try:
                        write_private_json(Path(str(self.path) + ".status.json"), dict(
                            schema="semloom.records_performance_event_status.v1", closed=True,
                            flush_confirmed=self.flush_confirmed, events_written=self.events_written,
                            last_monotonic_ns=self.last_monotonic_ns, errors=self.errors))
                    except Exception as error:
                        self.errors.append(_error(error))

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        self.close()


class MetricSources:
    """Reuse the existing GPU/host/Prometheus readers without a model endpoint."""

    def __init__(self, gpu_ids, native_metrics_url=None):
        self.gpu_ids, self.native_metrics_url = tuple(gpu_ids), native_metrics_url
        self._first_cpu = True
        if not self.gpu_ids:
            raise ValueError("explicit assigned physical GPU IDs are required")
        if native_metrics_url is not None:
            parsed = urlsplit(native_metrics_url)
            if (parsed.scheme != "http" or parsed.hostname not in ("127.0.0.1", "localhost", "::1")
                    or parsed.username is not None or parsed.password is not None):
                raise ValueError("native metrics URL must be an explicitly supplied credential-free loopback HTTP URL")

    def __call__(self):
        values, missing, sources = {}, {}, {}

        def collect(name, fields, reader):
            started = time.monotonic_ns()
            reason = None
            try:
                raw = reader()
            except Exception as error:
                raw, reason = {}, "unavailable:" + _error(error)
            source_status = raw.get(name + "_metrics_status", "observed")
            sources[name] = dict(start_monotonic_ns=started, end_monotonic_ns=time.monotonic_ns(),
                                 status=reason or source_status)
            for field in fields:
                values[field] = _number(raw.get(field))
                if values[field] is None:
                    missing[field] = reason or name + ":field_not_exposed_or_unsupported:" + source_status

        collect("gpu", GPU_FIELDS, lambda: gpu_metadata(self.gpu_ids, use_nvml=True))
        collect("host", HOST_FIELDS, lambda: host_metadata(include_status=True))
        if self._first_cpu and all(values[field] is not None for field in HOST_FIELDS[:2]):
            for field in HOST_FIELDS[:2]:
                values[field], missing[field] = None, "cpu_percent_baseline_priming"
            self._first_cpu = False

        def native():
            if self.native_metrics_url is None:
                return dict(native_metrics_status="unavailable:not_configured")
            raw = scrape_prometheus_metrics(self.native_metrics_url, timeout_s=1.0,
                                            require_direct_response=True)
            values = {field: raw.get(metric) for field, metric in NATIVE_FIELDS.items()}
            values["native_metrics_status"] = ("observed" if any(_number(value) is not None for value in values.values())
                                                else "unavailable:required_fields_not_exposed")
            return values

        collect("native", tuple(NATIVE_FIELDS), native)
        if self.native_metrics_url is None:
            missing.update({field: "native_metrics_not_exposed_no_HTTP_required" for field in NATIVE_FIELDS})
        return dict(values=values, missing=missing, sources=sources)


def _summary(samples, interval_s, duration_s):
    coverage, metrics = {}, {}
    for field in FIELDS:
        known = [r["values"][field] for r in samples if r["values"][field] is not None]
        coverage[field] = dict(valid_samples=len(known), attempted_samples=len(samples),
            sample_coverage_ratio=len(known) / len(samples) if samples else None,
            missing_reasons=sorted({r["missing"].get(field, "missing") for r in samples
                                    if r["values"][field] is None}))
        metrics[field] = dict(mean=statistics.mean(known) if known else None,
                             max=max(known) if known else None)
    gaps = [b["sample_start_monotonic_ns"] / 1e9 - a["sample_start_monotonic_ns"] / 1e9
            for a, b in zip(samples, samples[1:])]
    resource_rows = [dict(r["values"], sample_monotonic_s=r["sample_start_monotonic_ns"] / 1e9) for r in samples]
    reused = resource_sample_stats(resource_rows, observed_tokens=0)
    energy_complete = (len(samples) > 1 and coverage["gpu_power_w"]["sample_coverage_ratio"] == 1
                       and max(gaps, default=0) <= 4 * interval_s)
    return dict(samples=len(samples), duration_s=duration_s, field_coverage=coverage, metrics=metrics,
        first_sample_start_monotonic_ns=samples[0]["sample_start_monotonic_ns"] if samples else None,
        last_sample_end_monotonic_ns=samples[-1]["sample_end_monotonic_ns"] if samples else None,
        observed_span_s=(samples[-1]["sample_end_monotonic_ns"] - samples[0]["sample_start_monotonic_ns"]) / 1e9 if samples else 0,
        mean_scope="unweighted observed samples; missing values are excluded, never zero-filled",
        interval_s=dict(requested=interval_s, median=statistics.median(gaps) if gaps else None,
                        max=max(gaps) if gaps else None),
        gpu_energy_j=reused["gpu_energy_j"] if energy_complete else None,
        energy_time_basis="monotonic_s; paired UTC is display/provenance only",
        energy_window="first to last selected sample start; no extrapolation to phase endpoints",
        energy_status="estimated_trapezoid_observed_window" if energy_complete else "unavailable:missing_power_or_sampling_gap",
        collection_wall_s=sum(r["collection_wall_s"] for r in samples),
        collection_thread_cpu_s=sum(r["collection_thread_cpu_s"] for r in samples),
        sampling_coverage_scope="sample availability, not continuous-time or GPU-kernel coverage")


def phase_windows(events, ended_ns):
    """Explicit phase pairs plus literal existing observer boundaries; no inferred readiness."""
    starts, windows = {}, []
    pairs = {
        "worker_entry": ("worker_application", ("worker_finished",)),
        "owner_start": ("owner_startup", ("owner_ready",)),
        "query_source_start": ("query_source_through_result", ("result_ready",)),
        "source_read_start": ("source_read_through_result", ("result_ready",)),
        "evaluation_start": ("evaluation", ("evaluation_end",)),
        "evaluation_end": ("post_evaluation_close", ("owner_closed", "runtime_closed", "query_cleanup_end", "query_end")),
        "session_start": ("session_startup", ("session_ready",)),
        "query_start": ("query_execution", ("result_ready", "query_end")),
        "session_close_start": ("session_close", ("session_closed",)),
        "session_owner_start": ("owner_startup", ("session_owner_ready",)),
        "session_owner_close_start": ("owner_close", ("session_owner_closed",)),
        "query_checkpoint_start": ("owner_checkpoint", ("query_checkpoint_end",)),
    }
    def identity(row):
        query = row.get("query_id")
        global_session = row["event"] in {"session_start", "session_ready", "session_close_start", "session_closed"}
        return row.get("session_id"), query, row.get("owner_id") if query is None and not global_session else None

    def window(start, end_ns, status, end=None):
        row, phase = start
        value = dict(query_id=row.get("query_id"), phase=phase,
                     start_ns=row["monotonic_ns"], end_ns=end_ns, status=status)
        for name in ("session_id", "owner_id"):
            value_at_boundary = row.get(name) or (end or {}).get(name)
            if value_at_boundary is not None:
                value[name] = value_at_boundary
        return value

    for row in sorted(events, key=lambda r: r["monotonic_ns"]):
        event, scope = row["event"], identity(row)
        if row.get("boundary") == "start":
            starts[(scope, row["phase"], ("phase_end",))] = row, row["phase"]
        elif row.get("boundary") == "end":
            key = (scope, row["phase"], ("phase_end",))
            if key in starts:
                windows.append(window(starts.pop(key), row["monotonic_ns"], "complete", row))
        for key in list(starts):
            if key[0] == scope and event in key[2] and event != "phase_end":
                windows.append(window(starts.pop(key), row["monotonic_ns"], "complete", row))
        if event in pairs:
            name, end = pairs[event]
            starts[(scope, name, end)] = row, name
        if event == "result_ready" and row.get("session_id") is not None:
            starts[(scope, "query_post_result", ("query_end",))] = row, "query_post_result"
    for start in starts.values():
        windows.append(window(start, ended_ns, "incomplete_observation_only_no_terminal_inferred"))
    return windows


class RecordsTelemetry:
    """Opt-in outer-driver lifetime, independent of model/safety success criteria.

    sample_fn is a bounded read-only reader seam for CPU fixtures. Production
    defaults use in-process NVML and a 1s optional HTTP timeout.
    close never signals a process or closes any supplied native service.
    """

    def __init__(self, output_dir, *, gpu_ids, interval_s=.25, event_paths=(),
                 native_metrics_url=None, sample_fn=None, max_samples=100000):
        if isinstance(interval_s, bool) or not math.isfinite(interval_s) or interval_s <= 0:
            raise ValueError("positive finite sampling interval required")
        if type(max_samples) is not int or max_samples < 1:
            raise ValueError("positive finite sample count required")
        self.output_dir, self.interval_s = Path(output_dir), interval_s
        self.gpu_ids, self.event_paths = tuple(gpu_ids), tuple(Path(p) for p in event_paths)
        self._sample_fn = sample_fn
        self.native_metrics_url = native_metrics_url
        self.max_samples = max_samples
        self.samples, self.errors = [], []
        self._stop, self._thread, self._stream, self._context = threading.Event(), None, None, None
        self.events = self.report = None
        self.started_ns = self.ended_ns = None
        self.stream_write_wall_s = 0.0
        self.final_flush_wall_s = 0.0
        self._owns_output = False
        self._thread_started = False
        self._samples_flush_confirmed = False
        self.persisted_samples = 0

    def start(self):
        if self.started_ns is not None or self.report is not None:
            return self
        self.started_ns = time.monotonic_ns()
        try:
            new_private_directory(self.output_dir)
            self._owns_output = True
            self.events = TelemetryEvents(self.output_dir / "driver-events.jsonl")
            self._context = open_private_text(self.output_dir / "samples.jsonl")
            self._stream = self._context.__enter__()
            self._sample_fn = self._sample_fn or MetricSources(self.gpu_ids, self.native_metrics_url)
            self._thread = threading.Thread(target=self._run, name="records-performance-telemetry", daemon=True)
            self._thread.start()
            self._thread_started = True
        except Exception as error:
            self.errors.append(_error(error))
        return self

    def _run(self):
        try:
            self._sample_loop()
        except Exception as error:
            self.errors.append("collector_loop_failed:" + _error(error))
        finally:
            flushing = time.monotonic()
            try:
                self._context.__exit__(None, None, None)
                self._samples_flush_confirmed = self._stream.closed
            except Exception as error:
                self.errors.append(_error(error))
            finally:
                self.final_flush_wall_s += time.monotonic() - flushing

    def _sample_loop(self):
        previous = None
        while not self._stop.is_set():
            if len(self.samples) >= self.max_samples:
                self.errors.append("sample_limit_reached:remaining_window_unobserved")
                return
            start, epoch, cpu = time.monotonic_ns(), time.time_ns(), time.thread_time()
            try:
                result = self._sample_fn()
                values = {field: _number(result.get("values", {}).get(field)) for field in FIELDS}
                missing = {field: result.get("missing", {}).get(field, "field_not_exposed")
                           for field in FIELDS if values[field] is None}
                sources = result.get("sources", {})
            except Exception as error:
                values, missing = dict.fromkeys(FIELDS), dict.fromkeys(FIELDS, _error(error))
                sources = {"collector": dict(status=_error(error))}
            end = time.monotonic_ns()
            row = dict(schema="semloom.records_performance_sample.v1", sample_index=len(self.samples),
                sample_start_monotonic_ns=start, sample_end_monotonic_ns=end,
                sample_start_epoch_ns=epoch, sample_end_epoch_ns=time.time_ns(),
                actual_interval_s=(start - previous) / 1e9 if previous is not None else None,
                collection_wall_s=(end - start) / 1e9, collection_thread_cpu_s=time.thread_time() - cpu,
                values=values, missing=missing, sources=sources)
            previous = start
            self.samples.append(row)
            writing = time.monotonic()
            try:
                self._stream.write(json.dumps(redact_json_values(row), allow_nan=False) + "\n")
                self._stream.flush()
                self.persisted_samples += 1
            except Exception as error:
                self.errors.append(_error(error))
                break
            finally:
                self.stream_write_wall_s += time.monotonic() - writing
            self._stop.wait(self.interval_s)

    def close(self):
        if self.report is not None:
            return self.report
        try:
            return self._close()
        except Exception as error:
            self.errors.append(_error(error))
            self._stop.set()
            self.report = dict(schema="semloom.records_performance.v1", status="unavailable",
                errors=self.errors, model_or_safety_gate=False,
                collector_thread_stopped=self._thread is None or not self._thread.is_alive(),
                mfu=dict(status="unavailable", estimated=False, reasons=["telemetry_finalization_failed"]))
            return self.report

    def _close(self):
        self.ended_ns = time.monotonic_ns()
        self._stop.set()
        stopping = time.monotonic()
        if self._thread_started:
            self._thread.join(timeout=8)
        shutdown_wait_s = time.monotonic() - stopping
        alive = self._thread is not None and self._thread.is_alive()
        if alive:
            self.errors.append("collector_thread_exit_unconfirmed:no_process_signals_sent")
        events = []
        if self.events is not None:
            self.events.close()
            self.errors.extend(self.events.errors)
        if not self._thread_started and self._stream is not None:
            try:
                self._context.__exit__(None, None, None)
                self._samples_flush_confirmed = self._stream.closed
            except Exception as error:
                self.errors.append(_error(error))
        owned_paths = (self.output_dir / "driver-events.jsonl",) if self._owns_output else ()
        for path in (*self.event_paths, *owned_paths):
            try:
                for line in _read_regular_text(path, 8 * 1024**2).splitlines():
                    row = json.loads(line)
                    if type(row.get("monotonic_ns")) is not int or type(row.get("event")) is not str:
                        raise ValueError("telemetry event identity malformed")
                    events.append(row)
            except Exception as error:
                self.errors.append(_error(error))
            try:
                status_path = Path(str(path) + ".status.json")
                status = json.loads(_read_regular_text(status_path, 1024**2))
                if not status.get("closed") or not status.get("flush_confirmed") or status.get("errors"):
                    self.errors.append("event_writer_incomplete:" + json.dumps(status.get("errors", [])))
            except Exception as error:
                self.errors.append("event_writer_status_missing_or_invalid:" + _error(error))
        snapshots = list(self.samples)
        observed_end_ns = max([self.ended_ns] + [r["sample_end_monotonic_ns"] for r in snapshots])
        windows = phase_windows(events, self.ended_ns)
        for window in windows:
            selected = [r for r in snapshots if window["start_ns"] <= r["sample_start_monotonic_ns"]
                        and r["sample_end_monotonic_ns"] <= window["end_ns"]]
            window["summary"] = _summary(selected, self.interval_s, (window["end_ns"] - window["start_ns"]) / 1e9)
            window["boundary_overlap_samples"] = sum(
                r["sample_start_monotonic_ns"] < window["end_ns"] and r["sample_end_monotonic_ns"] > window["start_ns"]
                and r not in selected for r in snapshots)
        summary = _summary(snapshots, self.interval_s, (observed_end_ns - self.started_ns) / 1e9)
        status = "unavailable:no_samples" if not snapshots else "partial" if self.errors or any(
            v["valid_samples"] != len(snapshots) for v in summary["field_coverage"].values()) else "observed"
        self.report = dict(schema="semloom.records_performance.v1", status=status,
            clock_domain="same-host monotonic_ns; paired epoch_ns retained; sequential non-atomic probes",
            scope="outer-driver collector lifetime; explicit phase windows may overlap and must not be summed",
            gpu_ids=self.gpu_ids, gpu_aggregation="mean busy percent; sum device memory/power across explicit IDs",
            host_scope="host-wide psutil; includes collector and other host processes, not owned-process CPU attribution",
            gpu_probe="in_process_NVML; no probe child or automatic subprocess fallback",
            gpu_utilization_scope="NVML-reported device busy percent over its internal sampling period; not MFU or SM occupancy",
            start_monotonic_ns=self.started_ns, requested_stop_monotonic_ns=self.ended_ns,
            observed_end_monotonic_ns=observed_end_ns, max_samples=self.max_samples,
            persistence_write_wall_s=self.stream_write_wall_s, shutdown_wait_s=shutdown_wait_s,
            final_flush_wall_s=self.final_flush_wall_s,
            persisted_samples=self.persisted_samples,
            raw_samples_complete=not alive and self._samples_flush_confirmed and self.persisted_samples == len(snapshots) and not self.errors,
            summary=summary, phase_windows=windows, errors=self.errors, collector_thread_stopped=not alive,
            flush_confirmed=not alive and self._samples_flush_confirmed,
            model_or_safety_gate=False, service_closed_or_process_signals=False,
            mfu=dict(status="unavailable", estimated=False, reasons=[
                "no_trustworthy_model_FLOPs_numerator_bound_to_these_phase_windows",
                "no_validated_hardware_precision_peak_and_GPU_count_binding"],
                gpu_busy_percent_is_not_MFU=True))
        if self._owns_output:
            try:
                write_private_json(self.output_dir / "summary.json", redact_json_values(self.report))
            except Exception as error:
                self.errors.append(_error(error))
                self.report["status"] = "partial"
        return self.report

    def __enter__(self):
        return self.start()

    def __exit__(self, *_exc):
        self.close()
