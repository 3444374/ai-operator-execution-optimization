from __future__ import annotations

import csv
import json
import subprocess
import sys
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace


CODE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir())
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.baselines.common.contracts import ChatRequest
from src.baselines.common.manifests import write_manifest
from src.baselines.common.provenance import adapter_provenance
from src.baselines.text.orchestration.native_multijob import (
    NativeRunIdentity,
    audit_command,
    balanced_arm_order,
    build_shard_command,
    load_native_multijob_config,
    redact_command,
    run_native_multijob_cell,
    run_native_multijob,
)


class _FakeProcess:
    next_pid = 1000

    def __init__(self, command: list[str], **_kwargs: object) -> None:
        self.command = command
        self.args = command
        self.pid = _FakeProcess.next_pid
        self.returncode: int | None = None
        _FakeProcess.next_pid += 1
        output = Path(command[command.index("--output-dir") + 1])
        manifest = Path(command[command.index("--manifest") + 1])
        endpoint = int(command[command.index("--endpoint-index") + 1])
        requests = [
            json.loads(line)
            for line in manifest.read_text(encoding="utf-8").splitlines()
            if json.loads(line)["endpoint_index"] == endpoint
        ]
        output.mkdir(parents=True)
        fields = [
            "doc_id", "endpoint_index", "status", "error", "submitted_at_s",
            "started_at_s", "completed_at_s", "input_tokens", "output_tokens",
            "output_text", "finish_reason",
        ]
        with (output / "requests.csv").open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=fields)
            writer.writeheader()
            for position, row in enumerate(requests):
                writer.writerow({
                    "doc_id": row["doc_id"], "endpoint_index": endpoint, "status": "completed",
                    "error": "", "submitted_at_s": position, "started_at_s": position,
                    "completed_at_s": position + 1, "input_tokens": row["prompt_tokens"],
                    "output_tokens": 2, "output_text": "ok", "finish_reason": "stop",
                })
        adapter = command[command.index("--adapter") + 1]
        (output / "summary.json").write_text(
            json.dumps(
                {
                    "status": "completed", "adapter": adapter,
                    "source_kind": "timed_postgres_manifest",
                    "source_timing_boundary": "inside_job_barrier",
                    "source_read_s": 0.01,
                    "source_validation_status": "ok",
                    "server_version": "18.4",
                    "pgvector_version": "0.8.5",
                    **adapter_provenance(adapter).summary_fields(),
                }
            ),
            encoding="utf-8",
        )

    def wait(self, timeout: float | None = None) -> int:
        del timeout
        self.returncode = 0
        return self.returncode

    def poll(self) -> int | None:
        return self.returncode

    def terminate(self) -> None:
        self.returncode = -15

    def kill(self) -> None:
        self.returncode = -9


class _FailingProcess(_FakeProcess):
    def wait(self, timeout: float | None = None) -> int:
        del timeout
        return 1 if "shard_0" in self.command[self.command.index("--output-dir") + 1] else 0


class _HangingProcess(_FakeProcess):
    def wait(self, timeout: float | None = None) -> int:
        if self.returncode is not None:
            return self.returncode
        raise subprocess.TimeoutExpired(self.args, timeout)


class NativeMultiJobTests(unittest.TestCase):
    @staticmethod
    def _ray_nofile(_address: str) -> tuple[int, int]:
        return 65_536, 1_048_576

    def _manifest(self, root: Path, name: str, ids: tuple[int, int]) -> Path:
        path = root / name
        write_manifest(
            path,
            tuple(
                ChatRequest(
                    doc_id=doc_id,
                    prompt=f"p-{doc_id}",
                    arrival_time_s=0.0,
                    prompt_tokens=10,
                    max_output_tokens=8,
                    estimated_output_tokens=8,
                    source_row_hash=f"row-{doc_id}",
                    endpoint_index=position,
                )
                for position, doc_id in enumerate(ids)
            ),
        )
        return path

    def _config(self, root: Path, *, offset_s: float = 0.001) -> Path:
        first = self._manifest(root, "short.jsonl", (1, 2))
        second = self._manifest(root, "long.jsonl", (3, 4))
        arms = []
        for arm_id, adapter, ray_address in (
            ("daft_native", "daft_native", None),
            ("ray_data_http", "ray_data_http", "ray://127.0.0.1:10001"),
        ):
            arms.append({
                "id": arm_id, "adapter": adapter, "python_executable": sys.executable,
                "concurrency_per_endpoint": 2, "batch_size": 8, "timeout_s": 10.0,
                "process_timeout_s": 1.0,
                "ray_address": ray_address,
                "jobs": [
                    {"id": "short", "manifest": str(first), "offset_s": 0.0},
                    {"id": "long", "manifest": str(second), "offset_s": offset_s},
                ],
            })
        payload = {
            "schema_version": 1, "experiment_id": "native-multijob-test", "formal": True,
            "output_root": str(root / "out"),
            "endpoint_urls": [
                "http://127.0.0.1:8000/v1/chat/completions",
                "http://127.0.0.1:8001/v1/chat/completions",
            ],
            "model": "qwen", "api_key_env": None,
            "service_signature": {"model": "qwen", "service": "vllm-test"},
            "endpoint_ids": ["endpoint-0", "endpoint-1"],
            "protocol": "chat_completions", "output_cap": 8,
            "organizer": "daft",
            "source": {
                "kind": "timed_postgres_manifest",
                "database_url": "postgresql://postgres:postgres@localhost:5432/ai_operator",
                "workload_name": "sharegpt",
            },
            "job_internal_arrival_contract": "eager",
            "service": {"prefix_caching": "enabled", "max_num_seqs": 64, "max_num_batched_tokens": 4096},
            "idle_timeout_s": 1.0, "launch_lead_s": 0.0, "warmup_repeats": 1,
            "formal_repeats": 1, "schedule_seed": 9, "endpoint_work_skew_max": 0.02,
            "minimum_measurement_seconds": 0.000001,
            "arms": arms,
        }
        path = root / "config.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    def test_generic_config_keeps_matrix_binding_fields_optional(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = self._config(Path(directory))
            payload = json.loads(path.read_text(encoding="utf-8"))
            for field in (
                "endpoint_ids", "service_signature", "protocol",
                "output_cap", "organizer",
            ):
                payload.pop(field)
            path.write_text(json.dumps(payload), encoding="utf-8")

            config = load_native_multijob_config(path)

        self.assertEqual(config.endpoint_ids, ())
        self.assertEqual(config.service_signature, ())
        self.assertIsNone(config.protocol)
        self.assertIsNone(config.output_cap)
        self.assertIsNone(config.organizer)

    @staticmethod
    def _queues(_urls: tuple[str, ...], _timeout: float) -> dict[int, dict[str, int]]:
        return {0: {"running": 0, "waiting": 0}, 1: {"running": 0, "waiting": 0}}

    @staticmethod
    def _counters(_urls: tuple[str, ...]) -> dict[int, dict[str, int]]:
        _FakeProcess.next_pid += 1
        value = _FakeProcess.next_pid
        return {
            0: {"prompt_tokens": value, "generation_tokens": value},
            1: {"prompt_tokens": value, "generation_tokens": value},
        }

    @staticmethod
    @contextmanager
    def _instrumentation(_urls: tuple[str, ...], path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("sample_index,gpu_index,gpu_utilization_pct\n0,0,90\n", encoding="utf-8")
        yield SimpleNamespace(
            gpu_summary={"gpu0_util_mean": 90.0, "n_samples": 1.0},
            gauge_summary={"vllm_running_mean": 2.0, "n_gauge_samples": 1.0},
            ttft_deltas={0: {"status": "ok"}, 1: {"status": "ok"}},
        )

    def test_requires_disjoint_manifests_and_balanced_endpoints(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = self._config(root)
            payload = json.loads(path.read_text())
            payload["arms"][0]["jobs"][1]["manifest"] = payload["arms"][0]["jobs"][0]["manifest"]
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "overlapping doc_ids"):
                load_native_multijob_config(path)

    def test_rejects_bounded_control_as_native_multijob_arm(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = self._config(Path(directory))
            payload = json.loads(path.read_text())
            payload["arms"][0]["adapter"] = "bounded_http"
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "adapter must be one of"):
                load_native_multijob_config(path)

    def test_accepts_duckdb_native_jobs_and_freezes_extension_concurrency(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = self._config(root)
            payload = json.loads(path.read_text())
            arm = payload["arms"][0]
            arm["adapter"] = "duckdb_ai"
            arm["ray_address"] = None
            path.write_text(json.dumps(payload), encoding="utf-8")

            parsed = load_native_multijob_config(path).arms[0]
            command = build_shard_command(
                runner_script="run_official_baseline.py",
                arm=parsed,
                job=parsed.jobs[0],
                endpoint_index=0,
                endpoint_url="http://127.0.0.1:8000/v1/chat/completions",
                output_dir=root / "duckdb-shard",
                model="qwen",
                service_prefix_caching="enabled",
                service_max_num_seqs=64,
                service_max_num_batched_tokens=4096,
                api_key=None,
            )

            self.assertEqual(
                command[command.index("--duckdb-max-concurrent-requests") + 1],
                str(parsed.concurrency_per_endpoint),
            )

    def test_schedule_is_deterministic_and_rotates_formal_positions(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = load_native_multijob_config(self._config(Path(directory)))
            first = balanced_arm_order(config, "formal", 1)
            second = balanced_arm_order(config, "formal", 2)
            self.assertEqual(first, balanced_arm_order(config, "formal", 1))
            self.assertEqual(first[0].arm_id, second[-1].arm_id)

    def test_accepts_one_short_plus_three_matched_late_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = self._config(root)
            payload = json.loads(path.read_text())
            long2 = self._manifest(root, "long2.jsonl", (5, 6))
            long3 = self._manifest(root, "long3.jsonl", (7, 8))
            for arm in payload["arms"]:
                arm["jobs"].extend(
                    [
                        {"id": "long2", "manifest": str(long2), "offset_s": 0.001},
                        {"id": "long3", "manifest": str(long3), "offset_s": 0.001},
                    ]
                )
            path.write_text(json.dumps(payload), encoding="utf-8")
            config = load_native_multijob_config(path)
            self.assertTrue(all(len(arm.jobs) == 4 for arm in config.arms))

    def test_accepts_single_job_control_at_zero_offset(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = self._config(root)
            payload = json.loads(path.read_text())
            for arm in payload["arms"]:
                arm["jobs"] = arm["jobs"][:1]
            path.write_text(json.dumps(payload), encoding="utf-8")
            config = load_native_multijob_config(path)
            self.assertTrue(all(len(arm.jobs) == 1 for arm in config.arms))

    def test_rejects_multiple_late_arrival_offsets(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = self._config(root)
            payload = json.loads(path.read_text())
            long2 = self._manifest(root, "long2.jsonl", (5, 6))
            long3 = self._manifest(root, "long3.jsonl", (7, 8))
            payload["arms"][0]["jobs"].extend(
                [
                    {"id": "long2", "manifest": str(long2), "offset_s": 0.002},
                    {"id": "long3", "manifest": str(long3), "offset_s": 0.003},
                ]
            )
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "one shared positive arrival offset"):
                load_native_multijob_config(path)

    def test_command_audit_redacts_secret_and_rejects_project_controls(self) -> None:
        self.assertEqual(redact_command(["--api-key", "secret"]), ["--api-key", "***"])
        self.assertEqual(
            redact_command(
                ["--database-url", "postgresql://user:password@db.example/test"]
            ),
            ["--database-url", "postgresql://user:***@db.example/test"],
        )
        with self.assertRaisesRegex(ValueError, "prohibited"):
            audit_command(["runner", "--max-active-work", "65536"])

    def test_command_audit_rejects_coordinator_and_bounded_ready_spellings(self) -> None:
        for flag in (
            "--shared-credit-coordinator-name",
            "--shared_credit_coordinator_name",
            "--shared-ready-observation-contract",
            "--shared_ready_observation_contract",
        ):
            with self.subTest(flag=flag), self.assertRaisesRegex(
                ValueError, "prohibited"
            ):
                audit_command(["runner", flag, "project-control"])

    def test_timed_postgres_source_is_required_for_rankable_native_shards(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = load_native_multijob_config(self._config(root))
            arm = config.arms[0]
            command = build_shard_command(
                runner_script="run_official_baseline.py", arm=arm, job=arm.jobs[0],
                endpoint_index=0,
                endpoint_url="http://127.0.0.1:8000/v1/chat/completions",
                output_dir=root / "shard", model="qwen",
                service_prefix_caching="enabled", service_max_num_seqs=64,
                service_max_num_batched_tokens=4096, api_key=None, source=config.source,
            )
            self.assertEqual(
                command[command.index("--database-url") + 1],
                "postgresql://postgres:postgres@localhost:5432/ai_operator",
            )
            self.assertEqual(
                command[command.index("--source-workload-name") + 1], "sharegpt"
            )
            self.assertIn("--timed-postgres-source", command)

            payload = json.loads((root / "config.json").read_text())
            del payload["source"]
            path = root / "untimed.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            legacy = load_native_multijob_config(path)
            self.assertIsNone(legacy.source)
            untimed_command = build_shard_command(
                runner_script="run_official_baseline.py", arm=legacy.arms[0],
                job=legacy.arms[0].jobs[0], endpoint_index=0,
                endpoint_url="http://127.0.0.1:8000/v1/chat/completions",
                output_dir=root / "untimed-shard", model="qwen",
                service_prefix_caching="enabled", service_max_num_seqs=64,
                service_max_num_batched_tokens=4096, api_key=None,
                source=legacy.source,
            )
            self.assertNotIn("--timed-postgres-source", untimed_command)

    def test_native_config_requires_matching_explicit_service_signature(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = self._config(Path(directory))
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["service_signature"]["model"] = "different-model"
            path.write_text(json.dumps(payload), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "service_signature.model"):
                load_native_multijob_config(path)

    def test_runs_four_native_shards_per_arm_and_preserves_job_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = run_native_multijob(
                self._config(root), runner_script=root / "run_official_baseline.py",
                popen_factory=_FakeProcess, queue_waiter=self._queues, counter_sampler=self._counters,
                cell_instrumenter=self._instrumentation,
                ray_nofile_probe=self._ray_nofile,
            )
            self.assertEqual(result["comparison_admission"], "admissible")
            self.assertEqual(len(result["runs"]), 4)  # 2 arms * (warmup + formal)
            formal = [run for run in result["runs"] if run["phase"] == "formal"]
            self.assertTrue(all(len(run["jobs"]) == 2 for run in formal))
            job = formal[0]["jobs"][0]
            self.assertTrue(job["exactly_once"])
            self.assertEqual(len(job["pids"]), 2)
            self.assertTrue(Path(job["shards"][0]["log"]).is_file())
            self.assertTrue(Path(job["shards"][0]["requests"]).is_file())
            self.assertTrue(result["repository_commit"])
            self.assertEqual(job["shard_provenance"][0]["adapter"], formal[0]["adapter"])
            self.assertTrue(Path(formal[0]["gpu_resource_trace"]).is_file())
            self.assertEqual(formal[0]["gpu_summary"]["gpu0_util_mean"], 90.0)
            self.assertEqual(formal[0]["gauge_summary"]["vllm_running_mean"], 2.0)
            self.assertEqual(
                result["ray_worker_nofile"]["ray://127.0.0.1:10001"]["soft"],
                65_536,
            )

    def test_single_cell_retains_native_evidence_without_matrix_schedule(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = load_native_multijob_config(self._config(root))
            output_dir = root / "single-cell"
            clock = iter(100.0 + index * 0.001 for index in range(1000))

            record = run_native_multijob_cell(
                config,
                config.arms[0],
                NativeRunIdentity("formal", 2, 3),
                output_dir,
                runner_script="runner.py",
                popen_factory=_FakeProcess,
                queue_waiter=self._queues,
                counter_sampler=self._counters,
                now=lambda: next(clock),
                repository_commit="abc123",
                cell_instrumenter=self._instrumentation,
            )

            self.assertEqual(record["phase"], "formal")
            self.assertEqual(record["repeat"], 2)
            self.assertEqual(record["order_index"], 3)
            self.assertEqual(record["repository_commit"], "abc123")
            self.assertTrue(record["exactly_once"])
            self.assertEqual(len(record["jobs"]), 2)
            self.assertTrue(all(job["exactly_once"] for job in record["jobs"]))
            self.assertTrue(all(
                shard["source_validation_status"] == "ok"
                for job in record["jobs"]
                for shard in job["shard_provenance"]
            ))
            self.assertIn("service_counters", record)
            self.assertIn("gpu_summary", record)
            self.assertIn("gauge_summary", record)
            self.assertFalse((output_dir / "matrix_index.json").exists())

    def test_single_cell_persists_redacted_database_url_but_executes_raw_url(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config_path = self._config(root)
            payload = json.loads(config_path.read_text(encoding="utf-8"))
            raw_url = "postgresql://runner:sensitive@db.example:5432/ai_operator"
            payload["source"]["database_url"] = raw_url
            config_path.write_text(json.dumps(payload), encoding="utf-8")
            config = load_native_multijob_config(config_path)
            output_dir = root / "single-cell"
            clock = iter(100.0 + index * 0.001 for index in range(1000))
            executed: list[list[str]] = []

            def capture(command: list[str], **kwargs: object) -> _FakeProcess:
                executed.append(command)
                return _FakeProcess(command, **kwargs)

            run_native_multijob_cell(
                config,
                config.arms[0],
                NativeRunIdentity("formal", 1, 0),
                output_dir,
                runner_script="runner.py",
                popen_factory=capture,
                queue_waiter=self._queues,
                counter_sampler=self._counters,
                now=lambda: next(clock),
                repository_commit="abc123",
                cell_instrumenter=self._instrumentation,
            )

            persisted = "\n".join(
                path.read_text(encoding="utf-8")
                for path in (output_dir / "jobs").glob("*/commands.json")
            )
            self.assertTrue(any(raw_url in command for command in executed))
            self.assertNotIn("sensitive", persisted)
            self.assertIn("postgresql://runner:***@db.example:5432/ai_operator", persisted)

    def test_gate_only_runs_each_four_job_arm_once_and_is_not_rankable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = self._config(root)
            payload = json.loads(path.read_text())
            long2 = self._manifest(root, "long2.jsonl", (5, 6))
            long3 = self._manifest(root, "long3.jsonl", (7, 8))
            for arm in payload["arms"]:
                arm["jobs"].extend(
                    [
                        {"id": "long2", "manifest": str(long2), "offset_s": 0.001},
                        {"id": "long3", "manifest": str(long3), "offset_s": 0.001},
                    ]
                )
            path.write_text(json.dumps(payload), encoding="utf-8")

            result = run_native_multijob(
                path,
                runner_script=root / "run_official_baseline.py",
                popen_factory=_FakeProcess,
                queue_waiter=self._queues,
                counter_sampler=self._counters,
                cell_instrumenter=self._instrumentation,
                ray_nofile_probe=self._ray_nofile,
                gate_only=True,
            )

            self.assertEqual(result["status"], "passed")
            self.assertEqual(result["execution_mode"], "gate")
            self.assertEqual(result["comparison_admission"], "not_rankable")
            self.assertEqual(result["gate_runs_total"], 2)
            self.assertTrue(all(run["phase"] == "gate" for run in result["runs"]))
            self.assertTrue(all(len(run["jobs"]) == 4 for run in result["runs"]))
            self.assertTrue(all(run["duration_status"] == "gate_not_ranked" for run in result["runs"]))
            self.assertTrue(all(run["comparison_eligible"] is False for run in result["runs"]))

    def test_summary_provenance_mismatch_fails_closed(self) -> None:
        class WrongProvenanceProcess(_FakeProcess):
            def __init__(self, command: list[str], **kwargs: object) -> None:
                super().__init__(command, **kwargs)
                output = Path(command[command.index("--output-dir") + 1])
                summary = json.loads((output / "summary.json").read_text())
                summary["scheduler_owner"] = "project_credit_router"
                summary["custom_scheduling_code"] = True
                (output / "summary.json").write_text(json.dumps(summary), encoding="utf-8")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(RuntimeError, "native job shards failed"):
                run_native_multijob(
                    self._config(root), runner_script=root / "run_official_baseline.py",
                    popen_factory=WrongProvenanceProcess, queue_waiter=self._queues,
                    counter_sampler=self._counters,
                    cell_instrumenter=self._instrumentation,
                    ray_nofile_probe=self._ray_nofile,
                )
            index = json.loads((root / "out" / "matrix_index.json").read_text())
            self.assertEqual(index["status"], "failed")

    def test_summary_missing_database_version_fails_closed(self) -> None:
        class MissingVersionProcess(_FakeProcess):
            def __init__(self, command: list[str], **kwargs: object) -> None:
                super().__init__(command, **kwargs)
                output = Path(command[command.index("--output-dir") + 1])
                summary = json.loads((output / "summary.json").read_text())
                summary["pgvector_version"] = "not_applicable"
                (output / "summary.json").write_text(
                    json.dumps(summary), encoding="utf-8"
                )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(RuntimeError, "native job shards failed"):
                run_native_multijob(
                    self._config(root),
                    runner_script=root / "run_official_baseline.py",
                    popen_factory=MissingVersionProcess,
                    queue_waiter=self._queues,
                    counter_sampler=self._counters,
                    cell_instrumenter=self._instrumentation,
                    ray_nofile_probe=self._ray_nofile,
                )

    def test_records_commit_and_releases_host_scope_lease(self) -> None:
        class Lease:
            released = False

            def release(self) -> None:
                self.released = True

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            lease = Lease()
            acquired: list[tuple[Path, str]] = []

            def acquire(path: Path, *, repository_commit: str) -> Lease:
                acquired.append((path, repository_commit))
                return lease

            result = run_native_multijob(
                self._config(root), runner_script=root / "run_official_baseline.py",
                popen_factory=_FakeProcess, queue_waiter=self._queues, counter_sampler=self._counters,
                repository_commit_getter=lambda: "test-commit", host_lease_acquirer=acquire,
                cell_instrumenter=self._instrumentation,
                ray_nofile_probe=self._ray_nofile,
            )
            self.assertEqual(result["repository_commit"], "test-commit")
            self.assertEqual(acquired, [(root, "test-commit")])
            self.assertTrue(lease.released)

    def test_failure_is_retained_in_index_and_job_summary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(RuntimeError, "native job shards failed"):
                run_native_multijob(
                    self._config(root), runner_script=root / "run_official_baseline.py",
                    popen_factory=_FailingProcess, queue_waiter=self._queues, counter_sampler=self._counters,
                    cell_instrumenter=self._instrumentation,
                    ray_nofile_probe=self._ray_nofile,
                )
            index = json.loads((root / "out" / "matrix_index.json").read_text())
            self.assertEqual(index["status"], "failed")
            failed_jobs = list((root / "out" / "runs").glob("*/jobs/short/job_summary.json"))
            self.assertEqual(len(failed_jobs), 1)
            self.assertTrue(failed_jobs[0].is_file())

    def test_hung_shards_time_out_and_preserve_job_summary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(RuntimeError, "native job shards failed"):
                run_native_multijob(
                    self._config(root), runner_script=root / "run_official_baseline.py",
                    popen_factory=_HangingProcess, queue_waiter=self._queues,
                    counter_sampler=self._counters,
                    cell_instrumenter=self._instrumentation,
                    ray_nofile_probe=self._ray_nofile,
                )
            summaries = list(
                (root / "out" / "runs").glob("*/jobs/short/job_summary.json")
            )
            self.assertEqual(len(summaries), 1)
            summary = json.loads(summaries[0].read_text())
            self.assertTrue(summary["process_timed_out"])
            self.assertEqual(summary["failure_reason"], "shard_process_timeout")


if __name__ == "__main__":
    unittest.main()
