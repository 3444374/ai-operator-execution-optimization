"""Standard-library CPU checks; all vLLM allocations and results are explicit fakes."""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import types
import unittest
from contextlib import ExitStack
from dataclasses import asdict
from pathlib import Path
from unittest.mock import patch

CODE_ROOT = next(parent for parent in Path(__file__).resolve().parents
                 if (parent / "src").is_dir())
MODULE_PATH = CODE_ROOT / "src/serving/backends/vllm_engine.py"
# Isolate this CPU-only test from the existing backend package's numpy/Arrow imports.
# The normal public package import has its own explicitly dependency-gated check below.
MODULE_NAME = "_vllm_engine_cpu_test_kernel"
SPEC = importlib.util.spec_from_file_location(MODULE_NAME, MODULE_PATH)
kernel = importlib.util.module_from_spec(SPEC)
sys.modules[MODULE_NAME] = kernel
SPEC.loader.exec_module(kernel)
EngineSpec, GenerationSpec, VllmEngine = (
    kernel.EngineSpec, kernel.GenerationSpec, kernel.VllmEngine,
)


def prepared(index=0):
    return dict(
        query_id="movie-q3", model_role="main", algorithm_stage="filter",
        row_id=f"row-{index}", source_position=index, call_index=0, call_id=f"call-{index}",
        messages_json=json.dumps([
            dict(role="system", content="Return True or False."),
            dict(role="user", content="Review: 非常好\n{{literal}}"),
        ], ensure_ascii=False),
    )


def output(request_id="request-0", **changes):
    choice = types.SimpleNamespace(
        index=0, text="True", token_ids=(5, 6), finish_reason="stop", logprobs=None,
    )
    value = types.SimpleNamespace(
        request_id=request_id, prompt_token_ids=[1, 2, 3], finished=True, outputs=[choice],
    )
    for name, item in changes.items():
        setattr(value, name, item)
    return value


class FakeCore:
    def __init__(self):
        self.calls = []
        self.errors = []

    def shutdown(self, timeout=None):
        self.calls.append(timeout)
        if self.errors:
            raise self.errors.pop(0)


class FakeLLM:
    def __init__(self):
        self.core = FakeCore()
        self.llm_engine = types.SimpleNamespace(engine_core=self.core)
        self.calls = []
        self.outputs = [output()]
        self.error = None

    def chat(self, messages, **kwargs):
        self.calls.append((messages, kwargs))
        if self.error:
            raise self.error
        return self.outputs


class KernelTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.model_dir = Path(self.temp.name) / "model"
        self.model_dir.mkdir()
        self.spec = EngineSpec(str(self.model_dir), sys.executable, sys.executable)
        self.model_identity = dict(
            executable=sys.executable, prefix=sys.prefix, base_prefix=sys.base_prefix,
            vllm_version="0.25.1", lotus_available=False, python_version=[3, 12, 6],
        )
        self.driver_identity = {**self.model_identity, "prefix": self.temp.name + "/driver"}
        self.events = []
        self.fake_llm = FakeLLM()
        self.fake_module = types.ModuleType("vllm")
        self.fake_module.__version__ = "0.25.1"
        self.fake_module.LLM = self.allocate
        self.fake_module.SamplingParams = self.sample

    def allocate(self, **kwargs):
        self.events.append(("allocate", kwargs))
        return self.fake_llm

    def sample(self, **kwargs):
        self.events.append(("sample", kwargs))
        return types.SimpleNamespace(**kwargs)

    def environment(self):
        stack = ExitStack()
        stack.enter_context(patch.object(
            kernel, "_probe_python", side_effect=[self.model_identity, self.driver_identity],
        ))
        stack.enter_context(patch.dict(sys.modules, {"vllm": self.fake_module}))
        stack.enter_context(patch.object(kernel.importlib.metadata, "version", return_value="0.25.1"))
        stack.enter_context(patch.dict(os.environ))
        return stack

    def build(self):
        with self.environment():
            return VllmEngine(self.spec, GenerationSpec())

    def test_sampling_validation_precedes_allocation_and_forces_local_defaults(self):
        engine = self.build()
        self.assertEqual([event[0] for event in self.events], ["sample", "allocate"])
        sample, allocation = self.events[0][1], self.events[1][1]
        self.assertEqual(sample, GenerationSpec().kwargs())
        for name, expected in dict(
            tensor_parallel_size=1, pipeline_parallel_size=1,
            distributed_executor_backend="uni", trust_remote_code=False,
            generation_config="vllm", model=str(self.model_dir), tokenizer=str(self.model_dir),
        ).items():
            self.assertEqual(allocation[name], expected)
        self.assertEqual(engine.ready()["batch_mode"], "finite_synchronous_chat")
        json.dumps(engine.ready())

    def test_vllm_sampling_constructor_failure_allocates_nothing(self):
        def reject(**kwargs):
            self.events.append(("sample", kwargs))
            raise ValueError("fake SamplingParams validation error")
        self.fake_module.SamplingParams = reject
        with self.environment(), self.assertRaises(ValueError):
            VllmEngine(self.spec, GenerationSpec())
        self.assertEqual([item[0] for item in self.events], ["sample"])

    def test_mutated_invalid_generation_is_rechecked_before_allocation(self):
        generation = GenerationSpec()
        object.__setattr__(generation, "presence_penalty", 3.0)
        with self.environment(), self.assertRaises(ValueError):
            VllmEngine(self.spec, generation)
        self.assertEqual(self.events, [])

    def test_invalid_explicit_template_is_rejected_before_model_allocation(self):
        chat_utils = types.ModuleType("vllm.entrypoints.chat_utils")

        def load_chat_template(value):
            self.events.append(("template", value))
            raise ValueError("fake missing chat template")

        chat_utils.load_chat_template = load_chat_template
        options = asdict(self.spec)
        options["chat_template"] = "/missing/template.jinja"
        with self.environment(), patch.dict(sys.modules, {
            "vllm.entrypoints.chat_utils": chat_utils,
        }), self.assertRaises(ValueError):
            VllmEngine(EngineSpec(**options), GenerationSpec())
        self.assertEqual([event[0] for event in self.events], ["sample", "template"])

    def test_public_constructor_rejects_driver_runtime_before_sampling(self):
        self.model_identity["prefix"] = self.temp.name + "/other-model-env"
        with self.environment(), self.assertRaisesRegex(RuntimeError, "model_python"):
            VllmEngine(self.spec, GenerationSpec())
        self.assertEqual(self.events, [])

    def test_public_constructor_checks_current_executable_directory(self):
        self.model_identity["executable"] = self.temp.name + "/model-env/bin/python"
        with self.environment(), self.assertRaisesRegex(RuntimeError, "current Python"):
            VllmEngine(self.spec, GenerationSpec())
        self.assertEqual(self.events, [])

    def test_imported_lotus_is_rejected_without_model_allocation(self):
        with self.environment(), patch.dict(sys.modules, {"lotus.models": types.ModuleType("lotus.models")}):
            with self.assertRaisesRegex(RuntimeError, "LOTUS"):
                VllmEngine(self.spec, GenerationSpec())
        self.assertEqual(self.events, [])

    def test_wrong_imported_vllm_version_is_rejected_without_allocation(self):
        self.fake_module.__version__ = "0.25.0"
        with self.environment(), self.assertRaisesRegex(RuntimeError, "0.25.1"):
            VllmEngine(self.spec, GenerationSpec())
        self.assertEqual(self.events, [])

    def test_generation_exceeding_context_allocates_nothing(self):
        with self.environment(), self.assertRaisesRegex(ValueError, "max_model_len"):
            VllmEngine(self.spec, GenerationSpec(max_tokens=5000))
        self.assertEqual(self.events, [])

    def test_chat_preserves_untemplated_messages_and_complete_call_identity(self):
        engine = self.build()
        self.fake_llm.outputs = [output("request-A"), output("request-B")]
        rows = [prepared(0), prepared(1)]
        results = engine.run_prepared(rows)
        conversations, kwargs = self.fake_llm.calls[0]
        self.assertEqual(conversations, [json.loads(row["messages_json"]) for row in rows])
        self.assertEqual(len(self.fake_llm.calls), 1)
        self.assertFalse(kwargs["use_tqdm"])
        for row, result in zip(rows, results):
            self.assertEqual({key: result[key] for key in kernel.IDENTITY_FIELDS},
                             {key: row[key] for key in kernel.IDENTITY_FIELDS})
            self.assertEqual(result["status"], "succeeded")
            self.assertIs(result["submission_confirmed"], True)
            self.assertEqual((result["prompt_tokens"], result["completion_tokens"],
                              result["total_tokens"]), (3, 2, 5))
            self.assertEqual(result["text"], "True")
            self.assertEqual(json.loads(result["output_token_ids_json"]), [5, 6])
            self.assertEqual(result["logprobs_json"], "null")
            self.assertGreaterEqual(result["model_elapsed_s"], 0)
            json.dumps(result, allow_nan=False)

    def test_missing_usage_is_none_not_zero_or_estimated(self):
        engine = self.build()
        value = output(prompt_token_ids=None)
        value.outputs[0].token_ids = None
        self.fake_llm.outputs = [value]
        result = engine.run_prepared([prepared()])[0]
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual([result[key] for key in (
            "prompt_tokens", "completion_tokens", "total_tokens",
        )], [None, None, None])
        self.assertEqual(result["output_token_ids_json"], "null")

    def test_invalid_finished_result_is_failed_without_legal_empty_text(self):
        engine = self.build()
        for change in (dict(finished=False), dict(outputs=[])):
            with self.subTest(change=change):
                self.fake_llm.outputs = [output(**change)]
                result = engine.run_prepared([prepared()])[0]
                self.assertEqual(result["status"], "failed")
                self.assertIsNone(result["text"])
                self.assertEqual(result["prompt_tokens"], 3)
        for name, item in (("finish_reason", "abort"), ("index", 1), ("text", None),
                           ("token_ids", [True])):
            with self.subTest(field=name):
                value = output()
                setattr(value.outputs[0], name, item)
                self.fake_llm.outputs = [value]
                result = engine.run_prepared([prepared()])[0]
                self.assertEqual(result["status"], "failed")
                self.assertIsNone(result["text"])

    def test_output_count_or_request_identity_failure_is_unknown(self):
        engine = self.build()
        for outputs in ([], None, [output(), output()], [output("")]):
            with self.subTest(outputs=outputs):
                self.fake_llm.outputs = outputs
                result = engine.run_prepared([prepared()])[0]
                self.assertEqual(result["status"], "unknown")
                self.assertIsNone(result["text"])
                self.assertIsNone(result["total_tokens"])
        self.fake_llm.outputs = [output("duplicate"), output("duplicate")]
        self.assertTrue(all(result["status"] == "unknown" for result in
                            engine.run_prepared([prepared(0), prepared(1)])))

    def test_chat_error_has_unconfirmed_submission_and_unknown_usage(self):
        engine = self.build()
        self.fake_llm.error = RuntimeError("fake engine failed after possible acceptance")
        result = engine.run_prepared([prepared()])[0]
        self.assertEqual(result["status"], "unknown")
        self.assertIsNone(result["text"])
        self.assertIsNone(result["total_tokens"])
        self.assertIs(result["submission_confirmed"], False)
        self.assertIs(result["chat_invoked"], True)

    def test_bad_prepared_input_is_rejected_before_chat(self):
        engine = self.build()
        row = prepared()
        cases = [dict(row, call_index=True), dict(row, messages_json="{}"),
                 dict(row, messages_json=json.dumps([dict(role="user", content=[
                     dict(type="image_url", image_url="https://example.invalid/image"),
                 ])])), {key: value for key, value in row.items() if key != "query_id"}]
        for case in cases:
            with self.subTest(case=case), self.assertRaises(ValueError):
                engine.run_prepared([case])
        with self.assertRaises(ValueError):
            engine.run_prepared([row, row])
        self.assertEqual(self.fake_llm.calls, [])

    def test_expired_deadline_does_not_submit(self):
        engine = self.build()
        with patch.object(kernel.time, "monotonic", return_value=5.0):
            result = engine.run_prepared([prepared()], deadline_monotonic=4.0)[0]
        self.assertEqual(result["status"], "failed")
        self.assertIs(result["deadline_expired"], True)
        self.assertIs(result["submission_confirmed"], False)
        self.assertEqual(self.fake_llm.calls, [])

    def test_deadline_after_completion_keeps_usage_and_rejects_semantic_text(self):
        engine = self.build()
        with patch.object(kernel.time, "monotonic", side_effect=[2.0, 7.0]):
            result = engine.run_prepared([prepared()], deadline_monotonic=5.0)[0]
        self.assertEqual(result["status"], "failed")
        self.assertIs(result["deadline_expired"], True)
        self.assertIs(result["submission_confirmed"], True)
        self.assertEqual(result["total_tokens"], 5)
        self.assertIsNone(result["text"])

    def test_bad_deadlines_and_timeouts_never_call_chat_or_shutdown(self):
        engine = self.build()
        for value in (True, float("nan"), float("inf"), 0, -1):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    engine.run_prepared([prepared()], deadline_monotonic=value)
                with self.assertRaises(ValueError):
                    engine.close(timeout=value)
        self.assertEqual(self.fake_llm.calls, [])
        self.assertEqual(self.fake_llm.core.calls, [])

    def test_close_failure_is_unknown_and_retry_retains_handle(self):
        engine = self.build()
        self.fake_llm.core.errors = [RuntimeError("fake shutdown failure")]
        first = engine.close(timeout=1.5)
        self.assertEqual(first["status"], "unknown")
        self.assertFalse(first["engine_api_shutdown_confirmed"])
        self.assertIsNone(first["gpu_exit_confirmed"])
        self.assertFalse(engine.ready()["closed"])
        with self.assertRaisesRegex(RuntimeError, "cleanup is uncertain"):
            engine.run_prepared([prepared()])
        second = engine.close(timeout=2.5)
        self.assertEqual(second["status"], "closed")
        self.assertEqual(second["cleanup_scope"], "engine_core_api")
        self.assertTrue(second["engine_api_shutdown_confirmed"])
        self.assertEqual(self.fake_llm.core.calls, [1.5, 2.5])
        self.assertEqual(engine.close(), second)
        with self.assertRaisesRegex(RuntimeError, "closed"):
            engine.run_prepared([prepared()])

    def test_detached_vllm_finalizer_cannot_make_failed_cleanup_confirmed(self):
        engine = self.build()
        self.fake_llm.core.errors = [RuntimeError("fake detach-then-fail")]
        self.assertEqual(engine.close()["status"], "unknown")
        self.fake_llm.core._finalizer = types.SimpleNamespace(alive=False)
        second = engine.close()
        self.assertEqual(second["status"], "unknown")
        self.assertEqual(second["error_type"], "ShutdownNotConfirmed")
        self.assertFalse(engine.ready()["closed"])


class SpecificationTests(unittest.TestCase):
    def test_generation_roundtrip_preserves_effective_parameters(self):
        generation = GenerationSpec.from_dict(json.loads(json.dumps(asdict(GenerationSpec()))))
        self.assertEqual(generation, GenerationSpec())
        self.assertEqual(generation.kwargs()["n"], 1)

    def test_invalid_generation_values_are_rejected(self):
        for name, value in (
            ("max_tokens", True), ("max_tokens", 0), ("max_tokens", 1.5),
            ("temperature", float("nan")), ("temperature", float("inf")),
            ("temperature", 2.1), ("temperature", -0.1), ("temperature", True),
            ("top_p", 0), ("top_p", 1.01), ("top_p", "1"),
            ("presence_penalty", 3), ("frequency_penalty", -3),
            ("presence_penalty", float("nan")), ("frequency_penalty", True),
            ("repetition_penalty", 0), ("repetition_penalty", float("inf")),
            ("repetition_penalty", True), ("seed", True), ("seed", -1),
            ("stop", "\n"), ("stop", [""]), ("stop", [1]),
            ("logprobs", 0), ("logprobs", True),
        ):
            with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                GenerationSpec(**{name: value})

    def test_invalid_engine_capacities_and_numbers_are_rejected(self):
        for name, value in (
            ("max_num_seqs", True), ("max_num_batched_tokens", False),
            ("max_model_len", 0), ("gpu_memory_utilization", float("nan")),
            ("gpu_memory_utilization", float("inf")), ("gpu_memory_utilization", True),
            ("gpu_memory_utilization", 0), ("gpu_memory_utilization", 1.1),
            ("seed", True), ("dtype", "int8"),
        ):
            with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                EngineSpec("/model", "/model-python", "/driver-python", **{name: value})

    def test_probe_reports_real_python_identity_without_importing_ml_packages(self):
        identity = kernel._probe_python(sys.executable)
        self.assertEqual(Path(identity["prefix"]).resolve(), Path(sys.prefix).resolve())
        self.assertEqual(Path(identity["executable"]).parent.resolve(),
                         Path(sys.executable).parent.resolve())
        self.assertIs(type(identity["lotus_available"]), bool)

    def test_local_validation_rejects_missing_artifacts_before_probe(self):
        spec = EngineSpec("/not-a-local-model-fixture", sys.executable, sys.executable)
        with patch.object(kernel, "_probe_python") as probe, self.assertRaises(ValueError):
            spec.validate_local()
        probe.assert_not_called()

    def test_local_validation_uses_prefixes_instead_of_resolved_venv_binary(self):
        with tempfile.TemporaryDirectory() as folder:
            model_dir = Path(folder) / "model"
            model_dir.mkdir()
            paths = []
            for name in ("model-env", "driver-env"):
                path = Path(folder) / name / "bin/python"
                path.parent.mkdir(parents=True)
                path.symlink_to(sys.executable)
                paths.append(path)
            self.assertEqual(paths[0].resolve(), paths[1].resolve())
            identities = [dict(executable=str(path), prefix=str(path.parent.parent),
                               base_prefix=sys.base_prefix, vllm_version="0.25.1",
                               lotus_available=False) for path in paths]
            spec = EngineSpec(str(model_dir), *(str(path) for path in paths))
            with patch.object(kernel, "_probe_python", side_effect=identities):
                self.assertEqual(spec.validate_local()["model"], identities[0])
            with patch.object(kernel, "_probe_python", return_value=identities[0]):
                with self.assertRaisesRegex(ValueError, "different Python"):
                    spec.validate_local()

    def test_model_environment_requires_fixed_version_and_no_lotus_installation(self):
        with tempfile.TemporaryDirectory() as folder:
            spec = EngineSpec(folder, sys.executable, sys.executable)
            model = dict(executable=sys.executable, prefix="/model-fixture",
                         base_prefix=sys.base_prefix, vllm_version="0.25.1", lotus_available=False)
            driver = dict(model, prefix="/driver-fixture")
            for change in (dict(vllm_version="0.25.0"), dict(lotus_available=True)):
                with self.subTest(change=change):
                    with patch.object(kernel, "_probe_python", side_effect=[dict(model, **change), driver]):
                        with self.assertRaises(ValueError):
                            spec.validate_local()

    @unittest.skipUnless(all(importlib.util.find_spec(name) for name in ("pyarrow", "numpy")),
                         "normal backend package import needs existing pyarrow and numpy")
    def test_normal_package_import_without_lotus_or_vllm_import(self):
        result = subprocess.run([
            sys.executable, "-c", "import sys; from src.serving.backends.vllm_engine import "
            "EngineSpec; assert 'lotus' not in sys.modules; assert 'vllm' not in sys.modules",
        ], cwd=CODE_ROOT, capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
