"""Direct tests for the shared vLLM preflight module (audit F8).

The deep 8-case coverage of the pure cmdline-matching helpers lives in
``test_multicard_scale_ramp_helpers.py::StrictPreflightTests`` (which imports the ramp's aliases,
which now delegate to this module). These tests anchor the shared module's PUBLIC API directly so
``run_ai_operator_scenarios`` and ``multicard_scale_ramp`` have one canonical, independently-tested
implementation (code/AGENTS.md §4 低耦合).
"""

from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path

from src.infrastructure.vllm_preflight import (
    cmdline_for_port,
    flag_value_present,
    prefix_cache_flag_enabled,
    scheduler_cls_value,
    verify_endpoint_service_identity,
    verify_endpoint_cmdlines,
    verify_endpoint_scheduler_cls,
    verify_model_artifact_identity,
)


class VllmPreflightPureTests(unittest.TestCase):
    def _cmd(self, port, *, seqs="256", batched="8192", prefix="on"):
        flags = f"--port {port} --max-num-seqs {seqs} --max-num-batched-tokens {batched}"
        if prefix == "on":
            return f"python -m vllm.entrypoints {flags} --enable-prefix-caching"
        if prefix == "off":
            return f"python -m vllm.entrypoints {flags} --enable-prefix-caching=false"
        return f"python -m vllm.entrypoints {flags}"

    def test_cmdline_for_port_matches_both_forms(self):
        pool = [self._cmd(8000), "python -m vllm.entrypoints --port=8001"]
        self.assertIsNotNone(cmdline_for_port(pool, "8000"))
        self.assertIsNotNone(cmdline_for_port(pool, "8001"))
        self.assertIsNone(cmdline_for_port(pool, "9000"))

    def test_prefix_cache_flag_enabled_token_based_not_substring(self):
        self.assertTrue(prefix_cache_flag_enabled(self._cmd(8000, prefix="on")))
        self.assertFalse(prefix_cache_flag_enabled(self._cmd(8000, prefix="off")))
        self.assertIsNone(prefix_cache_flag_enabled(self._cmd(8000, prefix="absent")))

    def test_verify_endpoint_cmdlines_strict_raises_on_missing_flag(self):
        # port 8001's cmdline omits --max-num-seqs 256 -> strict must fail-closed.
        pool = [self._cmd(8000), "python -m vllm.entrypoints --port=8001 --max-num-batched-tokens 8192 --enable-prefix-caching"]
        with self.assertRaises(RuntimeError):
            verify_endpoint_cmdlines(
                pool,
                ["http://127.0.0.1:8000/v1/completions", "http://127.0.0.1:8001/v1/completions"],
                {"--max-num-seqs": "256", "--max-num-batched-tokens": "8192"},
                strict=True,
                tag="cost-profile",
            )

    def test_verify_endpoint_cmdlines_passes_when_all_declared_present(self):
        pool = [self._cmd(8000), self._cmd(8001)]
        verify_endpoint_cmdlines(  # no raise
            pool,
            ["http://127.0.0.1:8000/v1/completions", "http://127.0.0.1:8001/v1/completions"],
            {"--max-num-seqs": "256", "--max-num-batched-tokens": "8192"},
            strict=True,
            tag="cost-profile",
        )

    def test_flag_value_present_space_and_equals(self):
        c = "--max-num-seqs 256 --max-num-batched-tokens=8192"
        self.assertTrue(flag_value_present(c, "--max-num-seqs", "256"))
        self.assertTrue(flag_value_present(c, "--max-num-batched-tokens", "8192"))
        self.assertFalse(flag_value_present(c, "--max-num-seqs", "512"))

    def test_scheduler_cls_identity_distinguishes_native_and_custom(self):
        native = self._cmd(8000)
        custom = native + " --scheduler-cls=module.DRRScheduler"
        self.assertIsNone(scheduler_cls_value(native))
        self.assertEqual(scheduler_cls_value(custom), "module.DRRScheduler")
        verify_endpoint_scheduler_cls(
            [native], ["http://127.0.0.1:8000/v1/completions"], None
        )
        verify_endpoint_scheduler_cls(
            [custom],
            ["http://127.0.0.1:8000/v1/completions"],
            "module.DRRScheduler",
        )

    def test_native_fcfs_gate_rejects_custom_or_malformed_scheduler_cls(self):
        url = ["http://127.0.0.1:8000/v1/completions"]
        with self.assertRaisesRegex(RuntimeError, "scheduler class drift"):
            verify_endpoint_scheduler_cls(
                [self._cmd(8000) + " --scheduler-cls module.VTCScheduler"],
                url,
                None,
            )
        with self.assertRaisesRegex(RuntimeError, "bare --scheduler-cls"):
            scheduler_cls_value(self._cmd(8000) + " --scheduler-cls")

    @staticmethod
    def _service_identity(model_path: Path) -> dict[str, object]:
        hashes = {
            name: hashlib.sha256((model_path / name).read_bytes()).hexdigest()
            for name in (
                "config.json",
                "tokenizer_config.json",
                "tokenizer.json",
                "model.safetensors.index.json",
                "generation_config.json",
                "model-00001-of-00004.safetensors",
                "model-00002-of-00004.safetensors",
                "model-00003-of-00004.safetensors",
                "model-00004-of-00004.safetensors",
            )
        }
        return {
            "model": "qwen2.5-7b",
            "model_path": str(model_path),
            "model_revision": "a" * 40,
            "model_config_sha256": hashes["config.json"],
            "tokenizer_config_sha256": hashes["tokenizer_config.json"],
            "tokenizer_json_sha256": hashes["tokenizer.json"],
            "model_safetensors_index_sha256": hashes[
                "model.safetensors.index.json"
            ],
            "generation_config_sha256": hashes["generation_config.json"],
            "model_weight_00001_sha256": hashes[
                "model-00001-of-00004.safetensors"
            ],
            "model_weight_00002_sha256": hashes[
                "model-00002-of-00004.safetensors"
            ],
            "model_weight_00003_sha256": hashes[
                "model-00003-of-00004.safetensors"
            ],
            "model_weight_00004_sha256": hashes[
                "model-00004-of-00004.safetensors"
            ],
            "dtype": "bfloat16",
            "service": "0.25.1",
            "vllm_metadata_sha256": "1" * 64,
            "vllm_wheel_sha256": "2" * 64,
            "vllm_record_sha256": "3" * 64,
            "vllm_source_config_scheduler_sha256": "4" * 64,
            "vllm_source_scheduler_sha256": "5" * 64,
            "vllm_source_async_scheduler_sha256": "6" * 64,
            "vllm_source_request_queue_sha256": "7" * 64,
            "vllm_source_request_sha256": "8" * 64,
            "scheduler": "vllm_native_fcfs",
            "max_model_len": 8192,
            "max_num_seqs": 256,
            "max_num_batched_tokens": 8192,
            "chunked_prefill": True,
            "prefix_caching": True,
            "mfu_metrics": True,
            "enforce_eager": False,
            "compilation_mode": "vllm_compile",
            "gpu_memory_utilization": 0.9,
        }

    @staticmethod
    def _complete_cmdline(port: int, model_path: Path) -> str:
        return (
            "python -m vllm.entrypoints.openai.api_server "
            f"--model {model_path} --served-model-name qwen2.5-7b "
            "--dtype bfloat16 --max-model-len 8192 "
            "--gpu-memory-utilization 0.9 --max-num-seqs 256 "
            "--max-num-batched-tokens 8192 --enable-prefix-caching "
            "--enable-chunked-prefill --enable-mfu-metrics "
            f"--port {port}"
        )

    def test_complete_service_identity_accepts_exact_native_fcfs_cmdlines(self):
        with tempfile.TemporaryDirectory() as directory:
            model = Path(directory)
            for name in (
                "config.json",
                "tokenizer_config.json",
                "tokenizer.json",
                "model.safetensors.index.json",
                "generation_config.json",
                "model-00001-of-00004.safetensors",
                "model-00002-of-00004.safetensors",
                "model-00003-of-00004.safetensors",
                "model-00004-of-00004.safetensors",
            ):
                (model / name).write_text(name, encoding="utf-8")
            identity = self._service_identity(model)
            endpoints = [
                "http://127.0.0.1:8000/v1/chat/completions",
                "http://127.0.0.1:8001/v1/chat/completions",
            ]
            observed = verify_endpoint_service_identity(
                [self._complete_cmdline(8000, model), self._complete_cmdline(8001, model)],
                endpoints,
                identity,
            )
            self.assertEqual(set(observed), set(endpoints))
            verify_model_artifact_identity(identity)

    def test_complete_service_identity_rejects_every_runtime_drift(self):
        with tempfile.TemporaryDirectory() as directory:
            model = Path(directory)
            for name in (
                "config.json",
                "tokenizer_config.json",
                "tokenizer.json",
                "model.safetensors.index.json",
                "generation_config.json",
                "model-00001-of-00004.safetensors",
                "model-00002-of-00004.safetensors",
                "model-00003-of-00004.safetensors",
                "model-00004-of-00004.safetensors",
            ):
                (model / name).write_text(name, encoding="utf-8")
            identity = self._service_identity(model)
            endpoint = ["http://127.0.0.1:8000/v1/chat/completions"]
            exact = self._complete_cmdline(8000, model)
            drifts = {
                "model": exact.replace("qwen2.5-7b", "other-model"),
                "dtype": exact.replace("bfloat16", "float16"),
                "max_num_seqs": exact.replace("--max-num-seqs 256", "--max-num-seqs 128"),
                "max_num_batched_tokens": exact.replace(
                    "--max-num-batched-tokens 8192", "--max-num-batched-tokens 4096"
                ),
                "chunked_prefill": exact.replace("--enable-chunked-prefill", ""),
                "prefix_caching": exact.replace("--enable-prefix-caching", ""),
                "compile_mode": exact + " --enforce-eager",
                "gpu_memory_utilization": exact.replace(
                    "--gpu-memory-utilization 0.9", "--gpu-memory-utilization 0.8"
                ),
                "scheduler": exact + " --scheduler-cls custom.Scheduler",
            }
            for field, cmdline in drifts.items():
                with self.subTest(field=field), self.assertRaises(RuntimeError):
                    verify_endpoint_service_identity(
                        [cmdline], endpoint, identity
                    )

    def test_model_revision_binding_rejects_artifact_hash_drift(self):
        with tempfile.TemporaryDirectory() as directory:
            model = Path(directory)
            for name in (
                "config.json",
                "tokenizer_config.json",
                "tokenizer.json",
                "model.safetensors.index.json",
                "generation_config.json",
                "model-00001-of-00004.safetensors",
                "model-00002-of-00004.safetensors",
                "model-00003-of-00004.safetensors",
                "model-00004-of-00004.safetensors",
            ):
                (model / name).write_text(name, encoding="utf-8")
            identity = self._service_identity(model)
            (model / "config.json").write_text("drift", encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "model artifact.*drift"):
                verify_model_artifact_identity(identity)


if __name__ == "__main__":
    unittest.main()
