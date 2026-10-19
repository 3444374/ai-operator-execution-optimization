"""Unsupported native relation plans fail before runtime or model allocation."""

from unittest import TestCase
from unittest.mock import Mock, patch

from src.baselines.text.frameworks import daft_lotus_chain, ray_data_lotus_chain
from src.semantic_methods.lotus.record_chain import ExtractMethod, LotusRecordChain, MapMethod, StageSpec
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec


class NativeRelationGuardTests(TestCase):
    def check_native_guard(self, native):
        import lotus

        with lotus.settings.context(enable_cache=False):
            stages = (
                StageSpec("extract", ExtractMethod("fixture", ("text",), {"field": None}),
                          GenerationSpec(response_format={"type": "json_object"})),
                StageSpec("map", MapMethod("fixture", "Summarize {field}", "summary"), GenerationSpec()),
            )
            engine = EngineSpec("/tmp/unused-model", "/tmp/unused-model-python", "/tmp/unused-driver-python")
            # Even a terminal-only plan with the explicit profile must not silently
            # enter a native adapter that has no relation completion boundary.
            for selected in (stages[:1], stages):
                with self.subTest(stages=len(selected)):
                    chain = LotusRecordChain(selected, query_id="q", relation_barriers=True)
                    source = Mock(side_effect=AssertionError("source consumption forbidden"))
                    with patch.object(EngineSpec, "validate_driver_environment") as validate, \
                            patch.object(native, "_graph") as graph, \
                            patch.object(native.tempfile, "mkdtemp") as directory, \
                            patch("subprocess.Popen") as process:
                        with self.assertRaisesRegex(ValueError, "explicit Core profile"):
                            native.evaluate(source, chain, engine, RecordsResources(), expected=(), query_id="q")
                        for action in (source, validate, graph, directory, process):
                            action.assert_not_called()

    def test_ray_rejects_before_runtime_allocation(self):
        self.check_native_guard(ray_data_lotus_chain)

    def test_daft_rejects_before_runtime_allocation(self):
        self.check_native_guard(daft_lotus_chain)
