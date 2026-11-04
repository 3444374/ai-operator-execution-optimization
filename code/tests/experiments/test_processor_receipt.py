"""CPU-only service stubs reproduce the old cap and preserve native UUID behavior."""

from contextlib import contextmanager, ExitStack
import hashlib
import importlib.util
import json
import os
from pathlib import Path
from types import ModuleType, SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch


TOOL = Path(__file__).resolve().parents[2] / "scripts/profiling/record_observation/processor_receipt.py"
BEFORE_SHA256 = "3b6468274bebe2ce5b8e9605c9ed3fd0d14a6c764eda3082a53e1c81148174e3"


def load_receipt(path=TOOL):
    spec = importlib.util.spec_from_file_location("fixture_processor_receipt", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@contextmanager
def service_stubs():
    class ImageProcessor:
        def __init__(self, size):
            self.size = size

        def to_dict(self):
            return dict(min_pixels=self.size, max_pixels=12845056, do_normalize=True)

    class Qwen2VLProcessor:
        @staticmethod
        def from_pretrained(_path, *, revision, use_fast, size=1):
            return SimpleNamespace(image_processor=ImageProcessor(size))

    class BaseRenderer:
        def _process_mm_uuids(self, mm_data, mm_data_items, mm_uuid_items, mm_req_id):
            return mm_uuid_items

    class OpenAIServingModels:
        def __init__(self):
            self.model_config = SimpleNamespace(model="fixture-model", tokenizer="fixture-tokenizer",
                dtype="bfloat16", max_model_len=32768,
                hf_config=SimpleNamespace(architectures=["Qwen2VLForConditionalGeneration"]),
                generation_config="auto", get_diff_sampling_param=lambda: dict(temperature=0),
                multimodal_config=SimpleNamespace(mm_processor_cache_gb=0))
            self.renderer = SimpleNamespace(config=SimpleNamespace(
                cache_config=SimpleNamespace(enable_prefix_caching=False)))

    modules = {n: ModuleType(n) for n in ("transformers", "vllm", "vllm.renderers",
        "vllm.renderers.base", "vllm.entrypoints", "vllm.entrypoints.openai",
        "vllm.entrypoints.openai.models", "vllm.entrypoints.openai.models.serving")}
    modules["transformers"].Qwen2VLProcessor = Qwen2VLProcessor
    modules["vllm.renderers.base"].BaseRenderer = BaseRenderer
    modules["vllm.entrypoints.openai.models.serving"].OpenAIServingModels = OpenAIServingModels
    with ExitStack() as stack:
        # Restore even the old observer's UUID patch after exceptions in a test.
        for owner, name in ((Qwen2VLProcessor, "from_pretrained"),
                (OpenAIServingModels, "__init__"), (BaseRenderer, "_process_mm_uuids")):
            stack.enter_context(patch.object(owner, name, owner.__dict__[name]))
        yield SimpleNamespace(processor=Qwen2VLProcessor, model=OpenAIServingModels,
                              renderer=BaseRenderer, modules=modules)


def install_receipt(receipt, stubs):
    with patch.dict("sys.modules", stubs.modules):
        receipt.install()


def fixed_profiles(stubs):
    processors = [stubs.processor.from_pretrained("fixture-model", revision="fixture-revision",
        use_fast=False, size=size) for size in (1, 2, 3)]
    model = stubs.model()
    return processors, model


class ProcessorReceiptTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.output = Path(self.temporary.name) / "profiles.jsonl"
        self.environment = patch.dict(os.environ, Q7_PROCESSOR_RECEIPT=str(self.output))
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.receipt = load_receipt()

    def rows(self):
        return [json.loads(s) for s in self.output.read_text().splitlines()]

    def test_651_distinct_native_uuids_do_not_consume_fixed_profiles_or_change_results(self):
        with service_stubs() as stubs:
            original = stubs.renderer._process_mm_uuids
            install_receipt(self.receipt, stubs)
            processors, model = fixed_profiles(stubs)
            self.assertEqual(stubs.renderer._process_mm_uuids, original)
            renderer = stubs.renderer()
            for request in range(651):
                identifiers = dict(image=[f"fixture-{request}"])
                self.assertIs(renderer._process_mm_uuids({}, {}, identifiers, str(request)), identifiers)
            fixed_profiles(stubs)
            self.assertEqual(len(self.rows()), 4)
            self.assertEqual(len(self.receipt._seen), 4)
            self.assertEqual(len(processors), 3)
            self.assertEqual(model.model_config.dtype, "bfloat16")
        self.assertEqual(stubs.renderer._process_mm_uuids, original)
        rows = self.rows()
        self.assertEqual([r["kind"] for r in rows], ["processor"] * 3 + ["loaded_service_model_config"])
        self.assertEqual([r["parameters"]["min_pixels"] for r in rows[:3]], [1, 2, 3])
        self.assertTrue(all(r["revision"] == "fixture-revision" for r in rows[:3]))
        self.assertEqual(rows[3]["sampling_defaults"], dict(temperature=0))
        self.assertEqual(self.output.stat().st_mode & 0o777, 0o600)

    @unittest.skipUnless(os.environ.get("SEMLOOM_TEST_PROCESSOR_RECEIPT_BEFORE"),
                         "requires retained exact pre-fix observer source")
    def test_actual_old_uuid_binding_fails_after_252_successful_requests(self):
        path = Path(os.environ["SEMLOOM_TEST_PROCESSOR_RECEIPT_BEFORE"])
        self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), BEFORE_SHA256)
        old = load_receipt(path)
        successes, failures = 0, 0
        with service_stubs() as stubs:
            original = stubs.renderer._process_mm_uuids
            install_receipt(old, stubs)
            fixed_profiles(stubs)
            for request in range(651):
                try:
                    stubs.renderer()._process_mm_uuids({}, {}, {"image": [str(request)]}, str(request))
                    successes += 1
                except RuntimeError as error:
                    self.assertEqual(str(error), "runtime observer distinct-profile cap reached")
                    failures += 1
        self.assertEqual((successes, failures, len(self.rows())), (252, 399, 256))
        self.assertEqual(stubs.renderer._process_mm_uuids, original)

    def test_identity_sink_failure_keeps_native_return_but_has_no_ready_receipt(self):
        with service_stubs() as stubs:
            install_receipt(self.receipt, stubs)
            with patch.object(self.receipt.os, "write", side_effect=OSError("fixture disk error")):
                processors, model = fixed_profiles(stubs)
            self.assertEqual(len(processors), 3)
            self.assertEqual(model.model_config.model, "fixture-model")
            self.assertEqual(self.receipt._seen, set())
            self.assertEqual(self.output.read_bytes(), b"")
            # The controller requires actual processor/model rows before any POST.
            fixed_profiles(stubs)
        self.assertEqual(len(self.rows()), 4)

    def test_request_exception_and_result_identity_are_owned_by_native_renderer(self):
        marker = ValueError("fixture native failure")
        with service_stubs() as stubs:
            def raising(*_):
                raise marker
            stubs.renderer._process_mm_uuids = raising
            install_receipt(self.receipt, stubs)
            fixed_profiles(stubs)
            with self.assertRaises(ValueError) as caught:
                stubs.renderer()._process_mm_uuids({}, {}, {}, "request")
            self.assertIs(caught.exception, marker)
        self.assertEqual(len(self.rows()), 4)
