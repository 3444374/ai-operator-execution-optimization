from __future__ import annotations

import csv
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow as pa

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from scripts import postgres_ai_operator_profile as profile  # noqa: E402
from src.scheduling.adaptive_admission import AimdAdmissionController  # noqa: E402
from src.scheduling.admission import DynamicAdmissionGate  # noqa: E402
from src.scheduling.models import SubmissionCompletion  # noqa: E402
from src.scheduling.observations import (  # noqa: E402
    AdmissionTraceEvent,
    CachedMetricsObservationProvider,
    ServiceMetricsSnapshot,
)
from src.scheduling.batching import (  # noqa: E402
    FlushTraceEvent,
    ReplayServiceObservation,
)
from src.scheduling.routing import (  # noqa: E402
    LeastQueuedEndpointRouter,
    RequestPoolRouter,
)
from src.scheduling.scheduler import SchedulerResult  # noqa: E402


class SchedulingProfileHelperTests(unittest.TestCase):
    def test_batch_envelopes_preserve_arrow_payload_and_compute_cost(self) -> None:
        batch = pa.table(
            {
                "doc_id": [1, 2],
                "prompt_tokens": [10, 20],
                "prefix_key": ["shared", "shared"],
                "arrival_time_s": [1.5, 2.0],
            }
        )

        envelopes = profile._batch_envelopes(
            [batch],
            job_id="job-1",
            operator="ai_complete",
            completion_max_tokens=8,
        )

        self.assertEqual(len(envelopes), 1)
        envelope = envelopes[0]
        self.assertIs(envelope.payload, batch)
        self.assertEqual(envelope.request.request_id, "job-1:batch:0")
        self.assertEqual(envelope.request.payload_id, "job-1:batch:0")
        self.assertEqual(envelope.request.row_count, 2)
        self.assertEqual(envelope.request.prompt_tokens, 30)
        self.assertEqual(envelope.request.estimated_output_tokens, 16)
        self.assertEqual(envelope.request.prefix_key, "shared")
        self.assertEqual(envelope.request.first_arrival_s, 1.5)
        self.assertEqual(envelope.request.oldest_arrival_s, 1.5)

    def test_endpoint_topology_pairs_ids_and_urls_in_default_pool(self) -> None:
        topology = profile._endpoint_topology(
            endpoint_ids=["endpoint-0", "endpoint-1"],
            endpoint_urls=["http://one", "http://two"],
        )

        self.assertEqual(
            [(item.endpoint_id, item.url) for item in topology.endpoints],
            [("endpoint-0", "http://one"), ("endpoint-1", "http://two")],
        )
        self.assertTrue(all(item.pool_id == "default" for item in topology.endpoints))
        self.assertTrue(all(item.gpu_id == "0" for item in topology.endpoints))
        self.assertTrue(all(item.healthy for item in topology.endpoints))

    def test_endpoint_topology_rejects_mismatched_inputs(self) -> None:
        with self.assertRaisesRegex(ValueError, "same length"):
            profile._endpoint_topology(["endpoint-0"], [])

    def test_endpoint_topology_preserves_pool_and_gpu_assignments(self) -> None:
        topology = profile._endpoint_topology(
            ["endpoint-0", "endpoint-1"],
            ["http://one", "http://two"],
            pool_ids=["short", "long"],
            gpu_ids=["0", "1"],
        )

        self.assertEqual(
            [(item.pool_id, item.gpu_id) for item in topology.endpoints],
            [("short", "0"), ("long", "1")],
        )

    def test_scheduler_metrics_preserve_existing_profiler_schema(self) -> None:
        result = SchedulerResult(
            completions=(SubmissionCompletion("request-1", "completed", result={"ok": True}),),
            operator_invocations=1,
            max_inflight_seen=1,
            applied_limit=4,
            bounded_wait_s=0.2,
            avg_bounded_wait_s=0.2,
            fanin_s=0.1,
            submit_s=0.05,
        )

        metrics = profile._scheduler_metrics(result)

        self.assertEqual(
            set(metrics),
            {
                "operator_invocations",
                "max_inflight",
                "bounded_wait_s",
                "avg_bounded_wait_s",
                "fanin_s",
                "submit_s",
                "adaptive_downshifts",
                "adaptive_upshifts",
                "adaptive_limit_mean",
            },
        )
        self.assertEqual(metrics["operator_invocations"], 1)
        self.assertEqual(metrics["max_inflight"], 1)
        self.assertEqual(metrics["adaptive_downshifts"], 0)
        self.assertEqual(metrics["adaptive_upshifts"], 0)
        self.assertEqual(metrics["adaptive_limit_mean"], 4)

    def test_service_metrics_snapshot_maps_available_vllm_gauges(self) -> None:
        with patch.object(
            profile,
            "scrape_prometheus_metrics",
            return_value={
                "vllm:num_requests_running": 12.0,
                "vllm:num_requests_waiting": 3.0,
                "vllm:kv_cache_usage_perc": 0.7,
            },
        ):
            snapshot = profile._service_metrics_snapshot("http://metrics")

        self.assertEqual(snapshot.running, 12)
        self.assertEqual(snapshot.waiting, 3)
        self.assertEqual(snapshot.kv_usage, 0.7)

    def test_service_metrics_snapshot_returns_none_on_missing_scrape(self) -> None:
        with patch.object(profile, "scrape_prometheus_metrics", return_value={}):
            self.assertIsNone(
                profile._service_metrics_snapshot("http://metrics")
            )

    def test_build_adaptive_config_preserves_controller_across_submissions(self) -> None:
        traces = []
        config = profile._build_adaptive_config(
            scheduling_policy="aimd",
            metrics_url="http://metrics",
            trace_events=traces,
            min_window=4,
            max_window=16,
            initial_window=4,
            sample_interval_s=0.25,
            ewma_alpha=0.3,
            pid_proportional_gain=0.5,
            pid_integral_gain=0.1,
            pid_derivative_gain=0.05,
        )

        self.assertIs(config["trace_events"], traces)
        self.assertEqual(config["admission_gate"].limit, 4)
        self.assertEqual(config["controller_name"], "aimd")
        self.assertEqual(config["min_window"], 4)
        self.assertEqual(config["max_window"], 16)

    def test_build_adaptive_config_requires_metrics_url(self) -> None:
        with self.assertRaisesRegex(ValueError, "metrics URL"):
            profile._build_adaptive_config(
                scheduling_policy="pid",
                metrics_url=None,
                trace_events=[],
                min_window=2,
                max_window=16,
                initial_window=2,
                sample_interval_s=0.25,
                ewma_alpha=0.3,
                pid_proportional_gain=0.5,
                pid_integral_gain=0.1,
                pid_derivative_gain=0.05,
            )

    def test_write_control_trace_emits_plot_ready_rows(self) -> None:
        events = [
            AdmissionTraceEvent(
                observed_at_s=10.0,
                fresh=True,
                inflight=3,
                window=6,
                running=10,
                waiting=0,
                kv_usage=0.2,
                controller_action="increase",
                reason="low_load",
                allowed=True,
            ),
            AdmissionTraceEvent(
                observed_at_s=10.5,
                fresh=True,
                inflight=6,
                window=4,
                running=12,
                waiting=2,
                kv_usage=0.7,
                controller_action="decrease",
                reason="queue_congestion",
                allowed=False,
            ),
        ]
        output = Path("control_trace.csv")
        captured_rows = []
        with patch.object(
            profile,
            "append_metrics",
            side_effect=lambda path, row: captured_rows.append((path, row)),
        ):
            profile._write_control_trace(
                output,
                experiment_id="experiment",
                phase="formal",
                repeat_index=2,
                job_id=9,
                controller_name="aimd",
                trace_events=events,
            )

        self.assertEqual(len(captured_rows), 2)
        self.assertTrue(all(path == output for path, _ in captured_rows))
        rows = [row for _, row in captured_rows]
        self.assertEqual(rows[0]["elapsed_s"], 0.0)
        self.assertEqual(rows[1]["elapsed_s"], 0.5)
        self.assertEqual(rows[1]["k_max"], 4)
        self.assertEqual(rows[1]["controller_action"], "decrease")

    def test_build_routing_config_resolves_endpoint_assignments(self) -> None:
        config = profile._build_routing_config(
            endpoint_count=3,
            endpoint_routing="least_queued",
            pool_routing="request_cost",
            pool_ids_text="short,long,prefix",
            gpu_ids_text="0,0,1",
            long_request_tokens=1024,
        )

        self.assertIsInstance(
            config["endpoint_router"], LeastQueuedEndpointRouter
        )
        self.assertIsInstance(config["pool_router"], RequestPoolRouter)
        self.assertEqual(config["pool_ids"], ["short", "long", "prefix"])
        self.assertEqual(config["gpu_ids"], ["0", "0", "1"])

    def test_build_routing_config_rejects_assignment_count_mismatch(self) -> None:
        with self.assertRaisesRegex(ValueError, "pool IDs"):
            profile._build_routing_config(
                endpoint_count=2,
                endpoint_routing="round_robin",
                pool_routing="none",
                pool_ids_text="short",
                gpu_ids_text=None,
                long_request_tokens=1024,
            )

    def test_row_arrivals_preserve_complete_arrow_rows_and_metadata(self) -> None:
        table = pa.table(
            {
                "doc_id": pa.array([11, 12], type=pa.int64()),
                "tenant_id": pa.array([3, 4], type=pa.int32()),
                "text": ["alpha", "beta"],
                "prompt_tokens": pa.array([7, 9], type=pa.int32()),
                "prefix_key": ["shared", "other"],
                "arrival_time_s": pa.array([2.5, 2.75], type=pa.float64()),
            }
        )

        arrivals = profile._row_arrivals(table, completion_max_tokens=5)

        self.assertEqual(
            [
                (
                    item.row_id,
                    item.arrival_s,
                    item.prompt_tokens,
                    item.estimated_output_tokens,
                    item.prefix_key,
                )
                for item in arrivals
            ],
            [
                ("11", 2.5, 7, 5, "shared"),
                ("12", 2.75, 9, 5, "other"),
            ],
        )
        for index, arrival in enumerate(arrivals):
            self.assertIsInstance(arrival.payload_ref, pa.Table)
            self.assertEqual(arrival.payload_ref.schema, table.schema)
            self.assertEqual(
                arrival.payload_ref.to_pylist(),
                table.slice(index, 1).to_pylist(),
            )
            self.assertEqual(
                arrival.payload_ref.column("doc_id").chunk(0).buffers()[1].address,
                table.column("doc_id").chunk(0).buffers()[1].address,
            )

    def test_arrow_envelope_reconstructs_schema_order_and_values_exactly_once(
        self,
    ) -> None:
        table = pa.table(
            {
                "doc_id": pa.array([11, 12], type=pa.int64()),
                "text": ["alpha", "beta"],
                "prompt_tokens": pa.array([7, 9], type=pa.int32()),
                "prefix_key": ["shared", "shared"],
                "arrival_time_s": pa.array([2.5, 2.75], type=pa.float64()),
            }
        )
        arrivals = profile._row_arrivals(table, completion_max_tokens=5)
        builder = profile.PendingBatchBuilder(max_rows=2, token_budget=0)
        for arrival in arrivals:
            builder.add(arrival)

        envelope = profile._arrow_envelope(
            builder.close(),
            batch_index=4,
            job_id="job-7",
            operator="ai_complete",
        )

        self.assertEqual(envelope.payload.schema, table.schema)
        self.assertEqual(envelope.payload.column_names, table.column_names)
        self.assertEqual(envelope.payload.to_pylist(), table.to_pylist())
        self.assertEqual(envelope.payload.num_rows, table.num_rows)
        self.assertEqual(envelope.request.request_id, "job-7:batch:4")
        self.assertEqual(envelope.request.row_count, 2)
        self.assertEqual(envelope.request.prompt_tokens, 16)
        self.assertEqual(envelope.request.estimated_output_tokens, 10)
        self.assertEqual(envelope.request.prefix_key, "shared")
        self.assertEqual(envelope.request.first_arrival_s, 2.5)
        self.assertEqual(envelope.request.oldest_arrival_s, 2.5)

    def test_row_arrivals_reject_invalid_and_decreasing_arrival_values(self) -> None:
        invalid_columns = [
            pa.array([None], type=pa.float64()),
            pa.array([-0.1], type=pa.float64()),
            pa.array([True], type=pa.bool_()),
            pa.array([float("nan")], type=pa.float64()),
            pa.array([float("inf")], type=pa.float64()),
        ]
        for arrival_column in invalid_columns:
            with self.subTest(arrival=arrival_column.to_pylist()):
                table = pa.table(
                    {
                        "doc_id": [1],
                        "prompt_tokens": [1],
                        "arrival_time_s": arrival_column,
                    }
                )
                with self.assertRaisesRegex(ValueError, "arrival_time_s"):
                    profile._row_arrivals(table, completion_max_tokens=0)

        decreasing = pa.table(
            {
                "doc_id": [1, 2],
                "prompt_tokens": [1, 1],
                "arrival_time_s": [2.0, 1.0],
            }
        )
        with self.assertRaisesRegex(ValueError, "non-decreasing"):
            profile._row_arrivals(decreasing, completion_max_tokens=0)

    def test_multiple_arrow_chunks_share_one_arrival_replay_origin(self) -> None:
        clock = _DeterministicReplayClock(now_s=100.0)
        args = SimpleNamespace(
            ray_batch_rows=8,
            batching_policy="fixed_rows",
            token_budget=0,
            flush_policy="fixed_timeout",
            flush_timeout_ms=1000.0,
            flush_max_wait_ms=2000.0,
            _replay_clock=clock,
        )
        tables = [
            pa.table(
                {
                    "doc_id": [1],
                    "prompt_tokens": [2],
                    "arrival_time_s": [10.0],
                }
            ),
            pa.table(
                {
                    "doc_id": [2],
                    "prompt_tokens": [3],
                    "arrival_time_s": [10.25],
                }
            ),
        ]
        trace_events = []

        envelopes = list(
            profile._arrival_replay_envelopes(
                tables,
                args,
                job_id="job",
                operator="ai_embed",
                service_observation=lambda: ReplayServiceObservation(
                    fresh=True,
                    running=0,
                    waiting=0,
                    kv_usage=0.0,
                ),
                trace_sink=trace_events,
            )
        )

        self.assertEqual(clock.waited_until, [100.25])
        self.assertEqual(len(envelopes), 1)
        self.assertEqual(
            envelopes[0].payload.column("doc_id").to_pylist(),
            [1, 2],
        )
        self.assertTrue(trace_events)

    def test_token_budget_membership_survives_arrow_assembly(self) -> None:
        args = SimpleNamespace(
            ray_batch_rows=8,
            batching_policy="token_budget",
            token_budget=10,
            flush_policy="fixed_timeout",
            flush_timeout_ms=1000.0,
            flush_max_wait_ms=2000.0,
            _replay_clock=_DeterministicReplayClock(),
        )
        table = pa.table(
            {
                "doc_id": [1, 2, 3],
                "text": ["one", "two", "oversized"],
                "prompt_tokens": [6, 6, 12],
                "arrival_time_s": [0.0, 0.0, 0.0],
            }
        )

        envelopes = list(
            profile._arrival_replay_envelopes(
                [table],
                args,
                job_id="job",
                operator="ai_embed",
                service_observation=lambda: ReplayServiceObservation(
                    fresh=True,
                    running=0,
                    waiting=0,
                    kv_usage=0.0,
                ),
                trace_sink=[],
            )
        )

        self.assertEqual(
            [envelope.payload.column("doc_id").to_pylist() for envelope in envelopes],
            [[1], [2], [3]],
        )
        self.assertEqual(
            [envelope.request.prompt_tokens for envelope in envelopes],
            [6, 6, 12],
        )

    def test_nonadaptive_flush_never_reads_service_metrics(self) -> None:
        class BlockingMetricsMustNotRun:
            def latest(self, inflight):
                raise AssertionError("non-adaptive flush must not read metrics")

        for flush_policy in ("immediate", "fixed_timeout"):
            with self.subTest(flush_policy=flush_policy):
                args = SimpleNamespace(
                    ray_batch_rows=2,
                    batching_policy="fixed_rows",
                    token_budget=0,
                    flush_policy=flush_policy,
                    flush_timeout_ms=25.0,
                    flush_max_wait_ms=50.0,
                    _replay_clock=_DeterministicReplayClock(),
                )
                table = pa.table(
                    {
                        "doc_id": [1],
                        "prompt_tokens": [1],
                        "arrival_time_s": [0.0],
                    }
                )

                envelopes = list(
                    profile._arrival_replay_envelopes(
                        [table],
                        args,
                        job_id="job",
                        operator="ai_embed",
                        service_observation=BlockingMetricsMustNotRun(),
                        trace_sink=[],
                    )
                )

                self.assertEqual(len(envelopes), 1)

    def test_dry_run_records_default_and_explicit_replay_configuration(self) -> None:
        default_args = profile.parse_args(["--dry-run"])
        default_row = profile.run_once(default_args, "formal", 1)

        self.assertFalse(default_row["arrival_replay"])
        self.assertEqual(default_row["arrival_time_scale"], 1.0)
        self.assertEqual(default_row["flush_policy"], "immediate")
        self.assertEqual(default_row["flush_timeout_ms"], 25.0)
        self.assertEqual(default_row["flush_max_wait_ms"], 50.0)
        self.assertEqual(default_row["flush_trace_output"], "")
        self.assertEqual(default_row["flush_trace_path"], "")

        implicit_trace_args = profile.parse_args(
            [
                "--dry-run",
                "--executor",
                "ray_task",
                "--data-source",
                "daft_postgres",
                "--source-order",
                "arrival_time",
                "--arrival-replay",
            ]
        )
        implicit_trace_row = profile.run_once(implicit_trace_args, "formal", 1)

        self.assertEqual(implicit_trace_row["flush_trace_output"], "")
        self.assertEqual(
            implicit_trace_row["flush_trace_path"],
            str(
                Path("feasibility/results/postgres_ai_operator_profile_flush_trace.csv")
            ),
        )

        replay_args = profile.parse_args(
            [
                "--dry-run",
                "--executor",
                "ray_task",
                "--data-source",
                "daft_postgres",
                "--source-order",
                "arrival_time",
                "--arrival-replay",
                "--arrival-time-scale",
                "0.0005",
                "--flush-policy",
                "fixed_timeout",
                "--flush-timeout-ms",
                "12.5",
                "--flush-max-wait-ms",
                "30",
                "--flush-trace-output",
                "trace.csv",
            ]
        )
        replay_row = profile.run_once(replay_args, "formal", 1)

        self.assertTrue(replay_row["arrival_replay"])
        self.assertEqual(replay_row["arrival_time_scale"], 0.0005)
        self.assertEqual(replay_row["flush_policy"], "fixed_timeout")
        self.assertEqual(replay_row["flush_timeout_ms"], 12.5)
        self.assertEqual(replay_row["flush_max_wait_ms"], 30.0)
        self.assertEqual(replay_row["flush_trace_output"], "trace.csv")
        self.assertEqual(replay_row["flush_trace_path"], "trace.csv")
        self.assertEqual(
            replay_row["arrival_replay_preload"],
            "bounded_requested_workload",
        )

    def test_replay_validation_rejects_invalid_formal_paths(self) -> None:
        invalid_cases = [
            (
                [
                    "--dry-run",
                    "--arrival-replay",
                    "--data-source",
                    "daft_postgres",
                    "--source-order",
                    "doc_id",
                ],
                "source-order arrival_time",
            ),
            (
                [
                    "--dry-run",
                    "--arrival-replay",
                    "--data-source",
                    "arrow_postgres",
                    "--source-order",
                    "arrival_time",
                ],
                "data-source daft_postgres",
            ),
            (
                [
                    "--dry-run",
                    "--arrival-replay",
                    "--data-source",
                    "daft_postgres",
                    "--source-order",
                    "arrival_time",
                    "--executor",
                    "python",
                ],
                "Ray executor",
            ),
            (
                [
                    "--dry-run",
                    "--arrival-replay",
                    "--data-source",
                    "daft_postgres",
                    "--source-order",
                    "arrival_time",
                    "--flush-timeout-ms",
                    "-1",
                ],
                "flush-timeout-ms",
            ),
            (
                [
                    "--dry-run",
                    "--arrival-replay",
                    "--data-source",
                    "daft_postgres",
                    "--source-order",
                    "arrival_time",
                    "--flush-max-wait-ms",
                    "0",
                ],
                "flush-max-wait-ms",
            ),
            (
                [
                    "--dry-run",
                    "--arrival-replay",
                    "--data-source",
                    "daft_postgres",
                    "--source-order",
                    "arrival_time",
                    "--batching-policy",
                    "length_align_fixed_rows",
                ],
                "offline reordering",
            ),
            (
                [
                    "--dry-run",
                    "--arrival-replay",
                    "--data-source",
                    "daft_postgres",
                    "--source-order",
                    "arrival_time",
                    "--arrival-time-scale",
                    "0",
                ],
                "arrival-time-scale",
            ),
            (
                [
                    "--dry-run",
                    "--arrival-replay",
                    "--data-source",
                    "daft_postgres",
                    "--source-order",
                    "arrival_time",
                    "--arrival-time-scale",
                    "nan",
                ],
                "arrival-time-scale",
            ),
        ]
        for argv, message in invalid_cases:
            with self.subTest(argv=argv):
                with self.assertRaisesRegex(SystemExit, message):
                    profile.run_once(profile.parse_args(argv), "formal", 1)

    def test_replay_disabled_retains_batch_envelope_behavior(self) -> None:
        args = profile.parse_args(["--dry-run"])
        batch = pa.table(
            {
                "doc_id": [1],
                "prompt_tokens": [3],
                "arrival_time_s": [1.0],
            }
        )

        envelopes = profile._batch_envelopes(
            [batch],
            job_id="job",
            operator="ai_embed",
            completion_max_tokens=0,
        )

        self.assertFalse(args.arrival_replay)
        self.assertEqual(len(envelopes), 1)
        self.assertIs(envelopes[0].payload, batch)
        self.assertEqual(envelopes[0].request.request_id, "job:batch:0")

    def test_run_scheduler_consumes_a_single_pass_lazy_iterable(self) -> None:
        batch = pa.table({"doc_id": [1], "prompt_tokens": [3]})
        envelope = profile._batch_envelopes(
            [batch],
            job_id="job",
            operator="ai_embed",
            completion_max_tokens=0,
        )[0]
        consumed = []

        def envelopes():
            consumed.append("started")
            yield envelope

        remote = _RecordingRemote()
        topology = profile._endpoint_topology(
            ["endpoint-0"],
            ["ray://task/0"],
        )

        results, metrics = profile._run_scheduler(
            _ImmediateRay,
            envelopes(),
            topology,
            {"endpoint-0": lambda payload: remote.remote(payload)},
            profile.StaticAdmissionController(1),
        )

        self.assertEqual(consumed, ["started"])
        self.assertEqual(results, [{"call_index": 0}])
        self.assertEqual(metrics["operator_invocations"], 1)

    def test_flush_trace_writer_emits_all_fields_and_propagates_errors(self) -> None:
        events = [
            FlushTraceEvent(
                elapsed_s=0.125,
                pending_rows=2,
                pending_tokens=17,
                oldest_age_s=0.025,
                action="flush",
                reason="fixed_timeout",
            )
        ]
        test_tmp_root = CODE_ROOT.parent / "tmp"
        test_tmp_root.mkdir(exist_ok=True)
        output = test_tmp_root / "task3_flush_trace_test.csv"
        output.unlink(missing_ok=True)
        try:
            profile._write_flush_trace(
                output,
                experiment_id="experiment",
                phase="formal",
                repeat_index=2,
                job_id=9,
                flush_policy="fixed_timeout",
                flush_timeout_ms=25.0,
                flush_max_wait_ms=50.0,
                arrival_time_scale=0.0005,
                trace_events=events,
            )
            with output.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))

            self.assertEqual(len(rows), 1)
            self.assertEqual(
                set(rows[0]),
                {
                    "schema_version",
                    "experiment_id",
                    "phase",
                    "repeat_index",
                    "job_id",
                    "flush_policy",
                    "flush_timeout_ms",
                    "flush_max_wait_ms",
                    "arrival_time_scale",
                    "trace_index",
                    "elapsed_s",
                    "pending_rows",
                    "pending_tokens",
                    "oldest_age_s",
                    "action",
                    "reason",
                },
            )
            self.assertEqual(rows[0]["pending_rows"], "2")
            self.assertEqual(rows[0]["reason"], "fixed_timeout")
            self.assertEqual(rows[0]["arrival_time_scale"], "0.0005")

            with self.assertRaises(OSError):
                profile._write_flush_trace(
                    test_tmp_root,
                    experiment_id="experiment",
                    phase="formal",
                    repeat_index=2,
                    job_id=9,
                    flush_policy="fixed_timeout",
                    flush_timeout_ms=25.0,
                    flush_max_wait_ms=50.0,
                    arrival_time_scale=0.0005,
                    trace_events=events,
                )
        finally:
            output.unlink(missing_ok=True)


class _DeterministicReplayClock:
    def __init__(self, now_s: float = 100.0) -> None:
        self.current_s = now_s
        self.waited_until = []

    def now(self) -> float:
        return self.current_s

    def wait_until(self, deadline_s: float) -> None:
        self.waited_until.append(deadline_s)
        self.current_s = deadline_s


class _ImmediateRef:
    def __init__(self, result: object):
        self.result = result


class _ImmediateRay:
    @staticmethod
    def wait(handles, num_returns):
        return handles[:num_returns], handles[num_returns:]

    @staticmethod
    def get(handle):
        if isinstance(handle, list):
            return [item.result for item in handle]
        return handle.result


class _RecordingRemote:
    def __init__(self):
        self.calls = []

    def remote(self, *args):
        self.calls.append(args)
        return _ImmediateRef({"call_index": len(self.calls) - 1})


class StaticTaskSchedulingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.batches = [
            pa.table({"doc_id": [1], "prompt_tokens": [10]}),
            pa.table({"doc_id": [2], "prompt_tokens": [20]}),
        ]

    def _submit(self, remote, **overrides):
        arguments = {
            "ray_module": _ImmediateRay,
            "remote_embed": remote,
            "batches": self.batches,
            "max_inflight": 2,
            "operator": "ai_embed",
            "embedding_dim": 16,
            "model_backend": "fake",
            "endpoint_urls": [],
            "model_name": "model",
            "api_key": None,
            "timeout_s": 5.0,
            "completion_max_tokens": 8,
            "adaptive_config": None,
        }
        arguments.update(overrides)
        return profile.submit_ray_tasks(**arguments)

    def test_static_task_path_delegates_to_shared_scheduler(self) -> None:
        remote = _RecordingRemote()
        expected = ([{"ok": True}], {"operator_invocations": 1})

        with patch.object(profile, "_run_static_scheduler", return_value=expected) as run:
            actual = self._submit(remote)

        self.assertEqual(actual, expected)
        run.assert_called_once()

    def test_fake_task_submitter_preserves_operator_arguments(self) -> None:
        remote = _RecordingRemote()

        results, metrics = self._submit(remote)

        self.assertEqual(results, [{"call_index": 0}, {"call_index": 1}])
        self.assertEqual(
            [(call[0], call[1]) for call in remote.calls],
            [(self.batches[0], 16), (self.batches[1], 16)],
        )
        self.assertEqual(metrics["operator_invocations"], 2)
        self.assertEqual(metrics["max_inflight"], 2)
        self.assertEqual(metrics["adaptive_downshifts"], 0)

    def test_http_task_submitters_route_across_endpoint_urls(self) -> None:
        remote = _RecordingRemote()

        self._submit(
            remote,
            model_backend="compatible_http",
            endpoint_urls=["http://one", "http://two"],
        )

        self.assertEqual(
            [call[1] for call in remote.calls],
            ["http://one", "http://two"],
        )
        self.assertTrue(all(call[2:] == ("model", None, 5.0) for call in remote.calls))

    def test_adaptive_task_path_remains_isolated_from_static_scheduler(self) -> None:
        remote = _RecordingRemote()
        adaptive_config = {}

        with patch.object(profile, "_run_static_scheduler") as run:
            self._submit(remote, adaptive_config=adaptive_config)

        run.assert_not_called()

    def test_typed_adaptive_task_path_uses_dynamic_gate_without_legacy_loop(self) -> None:
        remote = _RecordingRemote()
        traces = []
        gate = DynamicAdmissionGate(
            AimdAdmissionController(initial_window=4),
            CachedMetricsObservationProvider(
                lambda: ServiceMetricsSnapshot(10, 0, 0.2),
                min_sample_interval_s=0.0,
            ),
            trace_sink=traces.append,
        )

        results, metrics = self._submit(
            remote,
            adaptive_config={
                "admission_gate": gate,
                "trace_events": traces,
            },
        )

        self.assertEqual(len(results), 2)
        self.assertGreater(metrics["adaptive_upshifts"], 0)
        self.assertEqual(metrics["adaptive_downshifts"], 0)
        self.assertGreaterEqual(metrics["adaptive_limit_mean"], 4)

    def test_prebuilt_replay_envelopes_feed_existing_scheduler_lazily(self) -> None:
        remote = _RecordingRemote()
        consumed = []
        envelope = profile._batch_envelopes(
            [self.batches[0]],
            job_id="replay",
            operator="ai_embed",
            completion_max_tokens=0,
        )[0]

        def replay_envelopes():
            consumed.append("started")
            yield envelope

        results, metrics = profile.submit_ray_tasks(
            ray_module=_ImmediateRay,
            remote_embed=remote,
            batches=[],
            max_inflight=2,
            operator="ai_embed",
            embedding_dim=16,
            model_backend="fake",
            endpoint_urls=[],
            model_name="model",
            api_key=None,
            timeout_s=5.0,
            completion_max_tokens=8,
            replay_envelopes=replay_envelopes(),
        )

        self.assertEqual(consumed, ["started"])
        self.assertEqual(results, [{"call_index": 0}])
        self.assertEqual(metrics["operator_invocations"], 1)
        self.assertIs(remote.calls[0][0], envelope.payload)

    def test_replay_envelopes_cover_typed_and_legacy_task_paths(self) -> None:
        envelope = profile._batch_envelopes(
            [self.batches[0]],
            job_id="replay",
            operator="ai_embed",
            completion_max_tokens=0,
        )[0]
        for policy in ("typed", "legacy"):
            with self.subTest(policy=policy):
                remote = _RecordingRemote()
                if policy == "typed":
                    traces = []
                    adaptive_config = {
                        "admission_gate": DynamicAdmissionGate(
                            AimdAdmissionController(initial_window=4),
                            CachedMetricsObservationProvider(
                                lambda: ServiceMetricsSnapshot(10, 0, 0.2),
                                min_sample_interval_s=0.0,
                            ),
                            trace_sink=traces.append,
                        ),
                        "trace_events": traces,
                    }
                else:
                    adaptive_config = {}

                results, metrics = self._submit(
                    remote,
                    adaptive_config=adaptive_config,
                    model_backend="compatible_http",
                    endpoint_urls=["http://local-test-endpoint"],
                    replay_envelopes=iter([envelope]),
                )

                self.assertEqual(len(results), 1)
                self.assertEqual(metrics["operator_invocations"], 1)
                self.assertIs(remote.calls[0][0], envelope.payload)


class _RecordingActor:
    def __init__(self):
        self.execute_batch = _RecordingRemote()


class StaticActorSchedulingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.batches = [
            pa.table({"doc_id": [1], "prompt_tokens": [10]}),
            pa.table({"doc_id": [2], "prompt_tokens": [20]}),
            pa.table({"doc_id": [3], "prompt_tokens": [30]}),
        ]

    def _submit(
        self,
        actors,
        adaptive_config=None,
        routing_config=None,
        replay_envelopes=None,
    ):
        return profile.submit_with_backpressure(
            ray_module=_ImmediateRay,
            actors=actors,
            batches=self.batches,
            max_inflight=2,
            method_name="execute_batch",
            adaptive_config=adaptive_config,
            routing_config=routing_config,
            replay_envelopes=replay_envelopes,
        )

    def test_static_actor_path_delegates_to_shared_scheduler(self) -> None:
        actors = [_RecordingActor()]
        expected = ([{"ok": True}], {"operator_invocations": 1})

        with patch.object(profile, "_run_static_scheduler", return_value=expected) as run:
            actual = self._submit(actors)

        self.assertEqual(actual, expected)
        run.assert_called_once()

    def test_actor_submitters_round_robin_across_actor_pool(self) -> None:
        actors = [_RecordingActor(), _RecordingActor()]

        results, metrics = self._submit(actors)

        self.assertEqual(
            [call[0] for call in actors[0].execute_batch.calls],
            [self.batches[0], self.batches[2]],
        )
        self.assertEqual(
            [call[0] for call in actors[1].execute_batch.calls],
            [self.batches[1]],
        )
        self.assertEqual(len(results), 3)
        self.assertEqual(metrics["operator_invocations"], 3)
        self.assertEqual(metrics["max_inflight"], 2)

    def test_static_actor_path_requires_at_least_one_actor(self) -> None:
        with self.assertRaisesRegex(ValueError, "actors must not be empty"):
            self._submit([])

    def test_adaptive_actor_path_remains_isolated_from_static_scheduler(self) -> None:
        actors = [_RecordingActor()]

        with patch.object(profile, "_run_static_scheduler") as run:
            self._submit(actors, adaptive_config={})

        run.assert_not_called()

    def test_typed_adaptive_actor_path_uses_dynamic_gate(self) -> None:
        actors = [_RecordingActor()]
        traces = []
        gate = DynamicAdmissionGate(
            AimdAdmissionController(initial_window=8),
            CachedMetricsObservationProvider(
                lambda: ServiceMetricsSnapshot(10, 2, 0.2),
                min_sample_interval_s=0.0,
            ),
            trace_sink=traces.append,
        )

        results, metrics = self._submit(
            actors,
            adaptive_config={
                "admission_gate": gate,
                "trace_events": traces,
            },
        )

        self.assertEqual(len(results), 3)
        self.assertGreater(metrics["adaptive_downshifts"], 0)
        self.assertGreaterEqual(len(traces), 3)

    def test_replay_envelopes_cover_static_typed_and_legacy_actor_paths(self) -> None:
        envelope = profile._batch_envelopes(
            [self.batches[0]],
            job_id="replay",
            operator="ai_embed",
            completion_max_tokens=0,
        )[0]
        for policy in ("static", "typed", "legacy"):
            with self.subTest(policy=policy):
                actors = [_RecordingActor()]
                if policy == "typed":
                    traces = []
                    adaptive_config = {
                        "admission_gate": DynamicAdmissionGate(
                            AimdAdmissionController(initial_window=4),
                            CachedMetricsObservationProvider(
                                lambda: ServiceMetricsSnapshot(10, 0, 0.2),
                                min_sample_interval_s=0.0,
                            ),
                            trace_sink=traces.append,
                        ),
                        "trace_events": traces,
                    }
                elif policy == "legacy":
                    adaptive_config = {}
                else:
                    adaptive_config = None

                results, metrics = self._submit(
                    actors,
                    adaptive_config=adaptive_config,
                    replay_envelopes=iter([envelope]),
                )

                self.assertEqual(len(results), 1)
                self.assertEqual(metrics["operator_invocations"], 1)
                self.assertIs(
                    actors[0].execute_batch.calls[0][0],
                    envelope.payload,
                )

    def test_actor_pool_routes_short_and_long_requests_to_partitioned_actors(self) -> None:
        actors = [_RecordingActor(), _RecordingActor()]

        self._submit(
            actors,
            routing_config={
                "pool_ids": ["short", "long"],
                "gpu_ids": ["0", "0"],
                "endpoint_router": LeastQueuedEndpointRouter(),
                "pool_router": RequestPoolRouter(long_request_tokens=25),
            },
        )

        self.assertEqual(
            [call[0] for call in actors[0].execute_batch.calls],
            [self.batches[0], self.batches[1]],
        )
        self.assertEqual(
            [call[0] for call in actors[1].execute_batch.calls],
            [self.batches[2]],
        )


if __name__ == "__main__":
    unittest.main()
