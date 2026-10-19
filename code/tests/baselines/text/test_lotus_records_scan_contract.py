"""Source scan validation must finish before runtime allocation in each backend."""

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks import daft_lotus_records, ray_data_lotus_records
from src.execution_provider.adapters.lotus_records_core import SemLoomRecordsCore
from src.execution_provider.adapters.lotus_records_static import SemLoomRecordsStaticReference
from src.semantic_methods.lotus.records import FilterMethod, RecordScan, RecordsResources
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
from tests.baselines.text.generic_records_fixture import CsvFilterFixture


class ScanContractTests(unittest.TestCase):
    def test_invalid_scan_is_rejected_before_driver_or_graph_allocation(self):
        method = FilterMethod("software-fixture", instruction="Does {body} describe urgent work?")
        engine = EngineSpec("/tmp/fixture-model", "/tmp/model-python", "/tmp/driver-python")
        generation = GenerationSpec()
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1)

        class Source:
            def preflight(self, method):
                method.describe()

            def scan_spec(self, method):
                return {"csv_schema": "not a RecordScan"}

            def expected(self, query_id):
                raise AssertionError("invalid scan reached occurrence enumeration")

        for name in ("ray", "daft", "static", "core"):
            with self.subTest(backend=name), patch.object(
                EngineSpec, "validate_driver_environment", side_effect=AssertionError("allocation preflight"),
            ) as driver:
                with self.assertRaisesRegex(TypeError, "RecordScan"):
                    if name in ("ray", "daft"):
                        module = ray_data_lotus_records if name == "ray" else daft_lotus_records
                        module.evaluate(Source(), method, engine, generation, resources, query_id="scan")
                    else:
                        cls = SemLoomRecordsStaticReference if name == "static" else SemLoomRecordsCore
                        cls(method, engine, generation, resources).evaluate(Source(), query_id="scan")
                driver.assert_not_called()

    def test_fixture_binding_rejects_method_or_input_changes(self):
        method = FilterMethod("software-fixture", instruction="Does {body} describe urgent work?")
        scan = RecordScan((("row_id", "string"), ("source_position", "int64"), ("body", "string")),
                          ("body",))
        with TemporaryDirectory() as directory:
            source = CsvFilterFixture(directory, method, scan,
                                      [dict(row_id="r0", source_position=0, body="001")])
            self.assertEqual(source.preflight(method)["status"], "passed")
            different = FilterMethod("software-fixture", instruction="Is {body} routine work?")
            with self.assertRaisesRegex(ValueError, "method binding"):
                source.preflight(different)
            Path(source.csv_path).write_text("changed", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "hash"):
                source.preflight(method)

    def test_missing_method_input_is_rejected_before_driver_or_graph_allocation(self):
        method = FilterMethod("software-fixture", instruction="Does {heading} describe urgent work?")
        engine = EngineSpec("/tmp/fixture-model", "/tmp/model-python", "/tmp/driver-python")
        generation = GenerationSpec()
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1)

        class Source:
            def preflight(self, method):
                method.describe()

            def scan_spec(self, method):
                return RecordScan((("row_id", "string"), ("source_position", "int64"),
                                   ("body", "string")), ("body",))

            def expected(self, query_id):
                raise AssertionError("missing method column reached occurrence enumeration")

        for name in ("ray", "daft", "static", "core"):
            with self.subTest(backend=name), patch.object(
                EngineSpec, "validate_driver_environment", side_effect=AssertionError("allocation preflight"),
            ) as driver:
                with self.assertRaisesRegex(ValueError, "omits Filter input"):
                    if name in ("ray", "daft"):
                        module = ray_data_lotus_records if name == "ray" else daft_lotus_records
                        module.evaluate(Source(), method, engine, generation, resources, query_id="scan")
                    else:
                        cls = SemLoomRecordsStaticReference if name == "static" else SemLoomRecordsCore
                        cls(method, engine, generation, resources).evaluate(Source(), query_id="scan")
                driver.assert_not_called()


if __name__ == "__main__":
    unittest.main()
