from __future__ import annotations

import csv
import json
import sys
import unittest
from dataclasses import asdict
from pathlib import Path
from tempfile import TemporaryDirectory


CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.baselines.contracts import BaselineRequestResult, ChatRequest
from src.baselines.gate_runner import (
    load_core_gate_config,
    parse_concurrency_overrides,
    parse_vllm_queue_metrics,
    parse_vllm_token_counters,
    run_core_gate,
    validate_service_counter_summary,
)
from src.baselines.manifests import read_manifest, write_manifest


class OfficialBaselineGateRunnerTests(unittest.TestCase):
    def test_parse_vllm_queue_metrics_requires_both_gauges(self) -> None:
        metrics = """
# HELP vllm:num_requests_running running
vllm:num_requests_running{model_name="qwen"} 2.0
vllm:num_requests_waiting{model_name="qwen"} 3.0
"""

        self.assertEqual(
            parse_vllm_queue_metrics(metrics),
            {"running": 2, "waiting": 3},
        )
        with self.assertRaisesRegex(ValueError, "waiting"):
            parse_vllm_queue_metrics('vllm:num_requests_running{model_name="qwen"} 0.0')

    def test_parse_vllm_token_counters_sums_labelled_series(self) -> None:
        metrics = """
vllm:prompt_tokens_total{engine="0",model_name="qwen"} 1.2e+02
vllm:generation_tokens_total{engine="0",model_name="qwen"} 80
"""

        self.assertEqual(
            parse_vllm_token_counters(metrics),
            {"prompt_tokens": 120, "generation_tokens": 80},
        )
        with self.assertRaisesRegex(ValueError, "generation_tokens"):
            parse_vllm_token_counters(
                'vllm:prompt_tokens_total{model_name="qwen"} 1'
            )

    def test_service_usage_must_match_endpoint_counter_delta(self) -> None:
        summary = {
            "service_counter_status": "ok",
            "service_prompt_tokens_delta": 8,
            "service_generation_tokens_delta": 4,
            "token_accounting": "server_usage",
            "input_tokens": 8,
            "output_tokens": 4,
        }
        self.assertEqual(
            validate_service_counter_summary(summary, 0),
            (),
        )
        self.assertIn(
            "generation",
            " ".join(
                validate_service_counter_summary(
                    {**summary, "output_tokens": 3},
                    0,
                )
            ),
        )
        self.assertEqual(
            validate_service_counter_summary(
                {
                    **summary,
                    "token_accounting": "service_counter",
                    "input_tokens": 0,
                    "output_tokens": 0,
                },
                0,
            ),
            (),
        )

    def test_completions_gate_accepts_only_batched_completions(
        self,
    ) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            manifest = root / "manifest.jsonl"
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
                        endpoint_index=index % 2,
                    )
                    for index in range(4)
                ),
            )
            payload = {
                "formal": False,
                "rows_total": 4,
                "completion_protocol": "completions",
                "endpoint_urls": [
                    "http://127.0.0.1:8000/v1/completions",
                    "http://127.0.0.1:8001/v1/completions",
                ],
                "model": "qwen",
                "manifest": str(manifest),
                "output_root": str(root / "output"),
                "cells": [
                    {
                        "id": "batched",
                        "adapter": "bounded_completions",
                        "batch_size": 4,
                        "concurrency_per_endpoint": 2,
                    }
                ],
            }
            config_path = root / "gate.json"
            config_path.write_text(
                json.dumps(payload),
                encoding="utf-8",
            )

            config = load_core_gate_config(config_path)

            self.assertEqual(config.completion_protocol, "completions")
            self.assertEqual(config.cells[0].adapter, "bounded_completions")
            payload["cells"][0]["adapter"] = "bounded_http"
            config_path.write_text(
                json.dumps(payload),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(
                ValueError,
                "bounded_completions cells only",
            ):
                load_core_gate_config(config_path)

    def test_config_rejects_placeholder_and_unsupported_core_cell(
        self,
    ) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config_path = root / "gate.json"
            config_path.write_text(
                json.dumps(
                    {
                        "formal": False,
                        "rows_total": 4,
                        "endpoint_urls": [
                            "http://127.0.0.1:8000/v1/chat/completions",
                            "http://127.0.0.1:8001/v1/chat/completions",
                        ],
                        "model": "qwen",
                        "manifest": "REPLACE_ME",
                        "output_root": "REPLACE_ME",
                        "cells": [
                            {
                                "id": "bad",
                                "adapter": "unknown_adapter",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "manifest"):
                load_core_gate_config(config_path)

    def test_vllm_cell_requires_explicit_tokenizer(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            manifest = root / "manifest.jsonl"
            write_manifest(
                manifest,
                (
                    ChatRequest(
                        doc_id=0,
                        prompt="question",
                        arrival_time_s=0.0,
                        prompt_tokens=4,
                        max_output_tokens=8,
                        estimated_output_tokens=8,
                        source_row_hash="row-0",
                        endpoint_index=0,
                    ),
                ),
            )
            config_path = root / "gate.json"
            config_path.write_text(
                json.dumps(
                    {
                        "formal": False,
                        "rows_total": 1,
                        "endpoint_urls": [
                            "http://127.0.0.1:8000/v1/chat/completions",
                            "http://127.0.0.1:8001/v1/chat/completions",
                        ],
                        "model": "qwen",
                        "manifest": str(manifest),
                        "output_root": str(root / "output"),
                        "cells": [
                            {
                                "id": "vllm",
                                "adapter": "vllm_bench",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "tokenizer"):
                load_core_gate_config(config_path)

    def test_rows_total_override_supports_scale_gate(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            manifest = root / "manifest.jsonl"
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
                        endpoint_index=index % 2,
                    )
                    for index in range(4)
                ),
            )
            config_path = root / "gate.json"
            config_path.write_text(
                json.dumps(
                    {
                        "formal": False,
                        "rows_total": 2,
                        "endpoint_urls": [
                            "http://127.0.0.1:8000/v1/chat/completions",
                            "http://127.0.0.1:8001/v1/chat/completions",
                        ],
                        "model": "qwen",
                        "manifest": str(manifest),
                        "output_root": str(root / "output"),
                        "cells": [
                            {
                                "id": "bounded",
                                "adapter": "bounded_http",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            config = load_core_gate_config(
                config_path,
                rows_total_override=4,
            )

            self.assertEqual(config.rows_total, 4)

    def test_cell_selection_and_concurrency_override_scope_calibration(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            manifest = root / "manifest.jsonl"
            write_manifest(
                manifest,
                (
                    ChatRequest(
                        doc_id=0,
                        prompt="question",
                        arrival_time_s=0.0,
                        prompt_tokens=4,
                        max_output_tokens=8,
                        estimated_output_tokens=8,
                        source_row_hash="row-0",
                        endpoint_index=0,
                    ),
                ),
            )
            config_path = root / "gate.json"
            config_path.write_text(
                json.dumps(
                    {
                        "formal": False,
                        "rows_total": 1,
                        "endpoint_urls": [
                            "http://127.0.0.1:8000/v1/chat/completions",
                            "http://127.0.0.1:8001/v1/chat/completions",
                        ],
                        "model": "qwen",
                        "manifest": str(manifest),
                        "output_root": str(root / "output"),
                        "cells": [
                            {
                                "id": "bounded_http",
                                "adapter": "bounded_http",
                                "concurrency_per_endpoint": 32,
                            },
                            {
                                "id": "ray_data_http",
                                "adapter": "ray_data_http",
                                "ray_address": "127.0.0.1:6380",
                                "concurrency_per_endpoint": 4,
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )

            config = load_core_gate_config(
                config_path,
                include_cell_ids=("bounded_http",),
                concurrency_overrides={"bounded_http": 64},
            )

            self.assertEqual(
                [
                    (cell.cell_id, cell.concurrency)
                    for cell in config.cells
                ],
                [("bounded_http", 64)],
            )
            with self.assertRaisesRegex(ValueError, "unknown included"):
                load_core_gate_config(
                    config_path,
                    include_cell_ids=("missing",),
                )
            with self.assertRaisesRegex(ValueError, "excluded cells"):
                load_core_gate_config(
                    config_path,
                    include_cell_ids=("bounded_http",),
                    concurrency_overrides={"ray_data_http": 8},
                )

    def test_concurrency_override_parser_rejects_ambiguous_values(self) -> None:
        self.assertEqual(
            parse_concurrency_overrides(
                ("vllm_bench=64", "bounded_http=128")
            ),
            {"vllm_bench": 64, "bounded_http": 128},
        )
        with self.assertRaisesRegex(ValueError, "duplicate"):
            parse_concurrency_overrides(
                ("vllm_bench=64", "vllm_bench=128")
            )
        with self.assertRaisesRegex(ValueError, "positive"):
            parse_concurrency_overrides(("vllm_bench=0",))
        with self.assertRaisesRegex(ValueError, "CELL_ID=N"):
            parse_concurrency_overrides(("vllm_bench",))

    def test_core_gate_runs_two_shards_and_writes_pass_report(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            manifest_path = root / "manifest.jsonl"
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
            write_manifest(manifest_path, requests)
            output_root = root / "gate-output"
            config_path = root / "gate.json"
            config_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "experiment_id": "test",
                        "formal": False,
                        "rows_total": 4,
                        "endpoint_urls": [
                            "http://127.0.0.1:8000/v1/chat/completions",
                            "http://127.0.0.1:8001/v1/chat/completions",
                        ],
                        "model": "qwen",
                        "manifest": str(manifest_path),
                        "output_root": str(output_root),
                        "cells": [
                            {
                                "id": "bounded_http",
                                "adapter": "bounded_http",
                                "concurrency_per_endpoint": 2,
                            },
                            {
                                "id": "project_static",
                                "adapter": "project_profiler",
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )
            seen_commands: list[list[list[str]]] = []

            def fake_pair_runner(commands, _log_paths):
                seen_commands.append(commands)
                for command in commands:
                    output_dir = Path(command[command.index("--output-dir") + 1])
                    endpoint_index = int(command[command.index("--endpoint-index") + 1])
                    endpoint_requests = tuple(
                        request
                        for request in read_manifest(manifest_path)
                        if request.endpoint_index == endpoint_index
                    )
                    output_dir.mkdir(parents=True)
                    results = [
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
                    with (output_dir / "requests.csv").open(
                        "w",
                        encoding="utf-8",
                        newline="",
                    ) as stream:
                        writer = csv.DictWriter(
                            stream,
                            fieldnames=list(asdict(results[0])),
                        )
                        writer.writeheader()
                        writer.writerows(asdict(row) for row in results)
                    predicted_work = sum(request.estimated_work for request in endpoint_requests)
                    (output_dir / "summary.json").write_text(
                        json.dumps(
                            {
                                "endpoint_index": endpoint_index,
                                "predicted_work": predicted_work,
                                "model_name": "qwen",
                                "completion_protocol": "chat_completions",
                                "service_config_sha256": "same",
                                "worker_failures": 0,
                                "token_accounting": "server_usage",
                                "input_tokens": 8,
                                "output_tokens": 4,
                            }
                        ),
                        encoding="utf-8",
                    )
                return (0, 0)

            counter_snapshots = iter(
                (
                    {
                        0: {
                            "prompt_tokens": 100,
                            "generation_tokens": 200,
                        },
                        1: {
                            "prompt_tokens": 300,
                            "generation_tokens": 400,
                        },
                    },
                    {
                        0: {
                            "prompt_tokens": 108,
                            "generation_tokens": 204,
                        },
                        1: {
                            "prompt_tokens": 308,
                            "generation_tokens": 404,
                        },
                    },
                )
            )
            report = run_core_gate(
                config_path,
                driver_python="driver-python",
                vllm_python="vllm-python",
                pair_runner=fake_pair_runner,
                idle_waiter=lambda _urls, _timeout: {
                    0: {"running": 0, "waiting": 0},
                    1: {"running": 0, "waiting": 0},
                },
                counter_sampler=lambda _urls: next(
                    counter_snapshots
                ),
            )

            self.assertEqual(report["status"], "passed")
            self.assertEqual(report["completed_cells"], ["bounded_http"])
            self.assertEqual(
                report["blocked_cells"],
                [
                    {
                        "id": "project_static",
                        "adapter": "project_profiler",
                        "reason": "requires_existing_project_profiler",
                    }
                ],
            )
            self.assertEqual(len(seen_commands), 1)
            self.assertEqual(len(seen_commands[0]), 2)
            self.assertIn("--disable-arrival-replay", seen_commands[0][0])
            self.assertTrue((output_root / "bounded_http" / "gate.json").exists())
            service_counters = json.loads(
                (
                    output_root
                    / "bounded_http"
                    / "service_counters.json"
                ).read_text(encoding="utf-8")
            )
            self.assertEqual(
                service_counters["delta"]["0"],
                {"prompt_tokens": 8, "generation_tokens": 4},
            )
            summary = json.loads(
                (
                    output_root
                    / "bounded_http"
                    / "shard_0"
                    / "summary.json"
                ).read_text(encoding="utf-8")
            )
            self.assertEqual(summary["service_prompt_tokens_delta"], 8)
            self.assertEqual(
                summary["service_generation_tokens_delta"],
                4,
            )


if __name__ == "__main__":
    unittest.main()
