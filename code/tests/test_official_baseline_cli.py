from __future__ import annotations

import csv
import json
import sys
import unittest
from dataclasses import asdict
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.baselines.cli import run_cli
from src.baselines.contracts import BaselineRequestResult, ChatRequest
from src.baselines.manifests import write_manifest


class OfficialBaselineCliTests(unittest.TestCase):
    @staticmethod
    def _write_balanced_manifest(path: Path) -> tuple[ChatRequest, ...]:
        requests = tuple(
            ChatRequest(
                doc_id=index,
                prompt=f"question-{index}",
                arrival_time_s=0.0,
                prompt_tokens=4,
                max_output_tokens=8,
                estimated_output_tokens=8,
                source_row_hash=f"row-{index}",
                endpoint_index=index % 2,
            )
            for index in range(4)
        )
        write_manifest(path, requests)
        return requests

    def test_run_shard_dry_run_is_side_effect_free(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            manifest = root / "manifest.jsonl"
            output_dir = root / "output"
            write_manifest(
                manifest,
                tuple(
                    ChatRequest(
                        doc_id=index,
                        prompt=f"question-{index}",
                        arrival_time_s=0.0,
                        prompt_tokens=4,
                        max_output_tokens=8,
                        estimated_output_tokens=8,
                        source_row_hash=f"row-{index}",
                        endpoint_index=0,
                    )
                    for index in range(32)
                ),
            )

            result = run_cli(
                [
                    "run-shard",
                    "--adapter",
                    "bounded_http",
                    "--manifest",
                    str(manifest),
                    "--endpoint-index",
                    "0",
                    "--endpoint-url",
                    "http://127.0.0.1:8000/v1/chat/completions",
                    "--model",
                    "qwen",
                    "--concurrency",
                    "32",
                    "--output-dir",
                    str(output_dir),
                    "--dry-run",
                ]
            )

            self.assertEqual(result["status"], "dry_run")
            self.assertEqual(result["request_count"], 32)
            self.assertEqual(
                result["completion_protocol"],
                "chat_completions",
            )
            self.assertFalse(output_dir.exists())

    def test_ray_runtime_dry_run_requires_explicit_cluster_address(
        self,
    ) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            manifest = root / "manifest.jsonl"
            self._write_balanced_manifest(manifest)

            with self.assertRaisesRegex(
                ValueError,
                "explicit --ray-address",
            ):
                run_cli(
                    [
                        "run-shard",
                        "--adapter",
                        "ray_data_http",
                        "--manifest",
                        str(manifest),
                        "--endpoint-index",
                        "0",
                        "--endpoint-url",
                        ("http://127.0.0.1:8000/v1/chat/completions"),
                        "--model",
                        "qwen",
                        "--output-dir",
                        str(root / "output"),
                        "--dry-run",
                    ]
                )

    def test_vllm_bench_dry_run_requires_explicit_tokenizer(
        self,
    ) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            manifest = root / "manifest.jsonl"
            self._write_balanced_manifest(manifest)

            with self.assertRaisesRegex(
                ValueError,
                "explicit --tokenizer",
            ):
                run_cli(
                    [
                        "run-shard",
                        "--adapter",
                        "vllm_bench",
                        "--manifest",
                        str(manifest),
                        "--endpoint-index",
                        "0",
                        "--endpoint-url",
                        ("http://127.0.0.1:8000/v1/chat/completions"),
                        "--model",
                        "qwen",
                        "--output-dir",
                        str(root / "output"),
                        "--dry-run",
                    ]
                )

    def test_export_postgres_manifest_freezes_and_assigns_rows(
        self,
    ) -> None:
        class FakeCursor:
            def execute(self, _sql, params):
                self.params = params

            def fetchall(self):
                return [
                    (1, "a", 0.0, 12, 8),
                    (2, "b", 0.1, 11, 9),
                    (3, "c", 0.2, 10, 10),
                    (4, "d", 0.3, 9, 11),
                ]

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

        class FakeConnection:
            def __init__(self):
                self.cursor_instance = FakeCursor()

            def cursor(self):
                return self.cursor_instance

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            output = root / "manifest.jsonl"
            connection = FakeConnection()

            with patch(
                "src.baselines.cli._connect_postgres",
                create=True,
                return_value=connection,
            ):
                result = run_cli(
                    [
                        "export-postgres-manifest",
                        "--database-url",
                        "postgresql://example",
                        "--workload-name",
                        "sharegpt_burstgpt",
                        "--row-count",
                        "4",
                        "--row-offset",
                        "32",
                        "--max-output-tokens",
                        "256",
                        "--estimated-output-mode",
                        "trace_target",
                        "--endpoint-count",
                        "2",
                        "--output",
                        str(output),
                    ]
                )

            self.assertEqual(
                connection.cursor_instance.params,
                ("sharegpt_burstgpt", 4, 32),
            )
            self.assertEqual(result["row_count"], 4)
            self.assertEqual(result["workload_name"], "sharegpt_burstgpt")
            frozen = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(
                {row["endpoint_index"] for row in frozen},
                {0, 1},
            )

    def test_service_fingerprint_ignores_equivalent_endpoint_address(
        self,
    ) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            manifest = root / "manifest.jsonl"
            self._write_balanced_manifest(manifest)

            fingerprints = []
            for endpoint_index, port in ((0, 8000), (1, 8001)):
                result = run_cli(
                    [
                        "run-shard",
                        "--adapter",
                        "bounded_http",
                        "--manifest",
                        str(manifest),
                        "--endpoint-index",
                        str(endpoint_index),
                        "--endpoint-url",
                        (f"http://127.0.0.1:{port}/v1/chat/completions"),
                        "--model",
                        "qwen",
                        "--output-dir",
                        str(root / f"output-{endpoint_index}"),
                        "--dry-run",
                    ]
                )
                fingerprints.append(result["service_config_sha256"])

            self.assertEqual(fingerprints[0], fingerprints[1])

    def test_failed_request_rows_are_preserved_before_gate_failure(
        self,
    ) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            manifest = root / "manifest.jsonl"
            requests = self._write_balanced_manifest(manifest)
            output_dir = root / "failed-output"
            failed = BaselineRequestResult(
                doc_id=requests[0].doc_id,
                endpoint_index=0,
                status="failed",
                error="http 500",
                submitted_at_s=0.0,
                started_at_s=0.0,
                completed_at_s=0.1,
                input_tokens=0,
                output_tokens=0,
                output_text=None,
                finish_reason=None,
            )

            with patch(
                "src.baselines.cli._run_adapter",
                return_value=(failed,),
            ):
                with self.assertRaisesRegex(
                    ValueError,
                    "exactly-once validation failed",
                ):
                    run_cli(
                        [
                            "run-shard",
                            "--adapter",
                            "bounded_http",
                            "--manifest",
                            str(manifest),
                            "--endpoint-index",
                            "0",
                            "--endpoint-url",
                            ("http://127.0.0.1:8000/v1/chat/completions"),
                            "--model",
                            "qwen",
                            "--output-dir",
                            str(output_dir),
                        ]
                    )

            self.assertTrue((output_dir / "requests.csv").exists())
            summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["status"], "failed")
            self.assertIn(
                "exactly-once validation failed",
                summary["error"],
            )

    def test_normalize_vllm_bench_writes_shared_schema(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            manifest = root / "manifest.jsonl"
            self._write_balanced_manifest(manifest)
            raw_result = root / "raw.json"
            raw_result.write_text(
                json.dumps(
                    {
                        "input_lens": [4, 4],
                        "output_lens": [2, 3],
                        "request_latencies": [0.2, 0.3],
                    }
                ),
                encoding="utf-8",
            )
            output_dir = root / "normalized"

            normalized = run_cli(
                [
                    "normalize-vllm-bench",
                    "--manifest",
                    str(manifest),
                    "--endpoint-index",
                    "0",
                    "--endpoint-url",
                    "http://127.0.0.1:8000/v1/chat/completions",
                    "--model",
                    "qwen",
                    "--input",
                    str(raw_result),
                    "--output-dir",
                    str(output_dir),
                    "--vllm-running-final",
                    "0",
                    "--vllm-waiting-final",
                    "0",
                ]
            )

            self.assertEqual(normalized["status"], "completed")
            self.assertEqual(normalized["completed_count"], 2)
            self.assertEqual(normalized["total_tokens"], 13)
            with (output_dir / "requests.csv").open(
                encoding="utf-8",
                newline="",
            ) as stream:
                rows = list(csv.DictReader(stream))
            self.assertEqual([row["doc_id"] for row in rows], ["0", "2"])

    def test_validate_gate_cli_accepts_two_complete_shards(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            manifest = root / "manifest.jsonl"
            requests = self._write_balanced_manifest(manifest)
            summaries: list[Path] = []
            result_paths: list[Path] = []
            for endpoint_index in (0, 1):
                endpoint_requests = tuple(
                    request for request in requests if request.endpoint_index == endpoint_index
                )
                summary_path = root / f"summary-{endpoint_index}.json"
                summary_path.write_text(
                    json.dumps(
                        {
                            "endpoint_index": endpoint_index,
                            "predicted_work": 24,
                            "model_name": "qwen",
                            "completion_protocol": "chat_completions",
                            "service_config_sha256": "same-service",
                            "vllm_num_requests_running_final": 0,
                            "vllm_num_requests_waiting_final": 0,
                            "worker_failures": 0,
                        }
                    ),
                    encoding="utf-8",
                )
                summaries.append(summary_path)
                result_path = root / f"requests-{endpoint_index}.csv"
                with result_path.open(
                    "w",
                    encoding="utf-8",
                    newline="",
                ) as stream:
                    rows = [
                        BaselineRequestResult(
                            doc_id=request.doc_id,
                            endpoint_index=endpoint_index,
                            status="completed",
                            error=None,
                            submitted_at_s=0.0,
                            started_at_s=0.0,
                            completed_at_s=0.1,
                            input_tokens=4,
                            output_tokens=2,
                            output_text="ok",
                            finish_reason="stop",
                        )
                        for request in endpoint_requests
                    ]
                    writer = csv.DictWriter(
                        stream,
                        fieldnames=list(asdict(rows[0])),
                    )
                    writer.writeheader()
                    writer.writerows(asdict(row) for row in rows)
                result_paths.append(result_path)
            output = root / "gate.json"

            report = run_cli(
                [
                    "validate-gate",
                    "--manifest",
                    str(manifest),
                    "--summary",
                    str(summaries[0]),
                    "--summary",
                    str(summaries[1]),
                    "--request-results",
                    str(result_paths[0]),
                    "--request-results",
                    str(result_paths[1]),
                    "--output",
                    str(output),
                ]
            )

            self.assertTrue(report["passed"])
            self.assertEqual(report["incidents"], [])
            self.assertEqual(
                json.loads(output.read_text(encoding="utf-8")),
                json.loads(json.dumps(report)),
            )


if __name__ == "__main__":
    unittest.main()
