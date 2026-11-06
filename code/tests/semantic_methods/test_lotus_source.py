"""Source provenance is observable data, separate from execution contracts."""

import hashlib
from importlib.metadata import PackageNotFoundError
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.semantic_methods.lotus import source


class SourceReportTests(unittest.TestCase):
    def test_changed_source_and_package_versions_are_reported_without_rejection(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "lotus").mkdir()
            body = b"# An installed revision with different source bytes.\n"
            (root / "lotus" / "method.py").write_bytes(body)
            installed = SimpleNamespace(version="fixture-new-version", locate_file=lambda p: root / p)
            with patch.object(source, "distribution", return_value=installed):
                report = source.source_report("fixture-profile", ("method.py", "absent.py"))
        self.assertEqual(report["versions"], dict.fromkeys(("lotus-ai", "litellm", "openai"),
                                                          "fixture-new-version"))
        self.assertEqual(report["source_hashes"], {"method.py": hashlib.sha256(body).hexdigest()})
        self.assertEqual(report["unavailable_sources"], ["absent.py"])
        self.assertIsNone(report["lotus_commit"])
        self.assertEqual(report["reference_lotus_commit"], source.LOTUS_COMMIT)
        self.assertEqual(report["source_policy"], "record_only")

    def test_missing_metadata_is_reported_without_fabricating_source_identity(self):
        with patch.object(source, "distribution", side_effect=PackageNotFoundError):
            report = source.source_report("fixture-profile", ("method.py",))
        self.assertEqual(report["versions"], dict.fromkeys(("lotus-ai", "litellm", "openai")))
        self.assertEqual(report["source_hashes"], {})
        self.assertEqual(report["unavailable_sources"], ["lotus-ai", "litellm", "openai"])


class ExecutionSourceIndependenceTests(unittest.TestCase):
    def test_filter_and_finite_chain_do_not_consult_source_metadata(self):
        import lotus
        from src.semantic_methods.lotus.engine import bind_filter_method, LotusFilterMethod
        from src.semantic_methods.lotus.record_chain import LotusRecordChain, MapMethod, StageSpec
        from src.semantic_methods.lotus.records import FilterMethod, identity
        from src.serving.backends.vllm_protocol import GenerationSpec
        from tests.semantic_methods.test_lotus_record_chain import engine_result, records

        with lotus.settings.context(enable_cache=False), \
                patch.object(source, "distribution", side_effect=AssertionError("runtime source read")):
            method = bind_filter_method(FilterMethod("openai/fixture", "Keep {description}"))
            prepared = LotusFilterMethod(method).prepare_rows([
                dict(identity("fixture", "row", 0), description="an orchid")])
            self.assertIn("an orchid", prepared[0]["messages_json"])
            chain = LotusRecordChain((StageSpec("map", MapMethod("openai/fixture",
                "Rewrite {reviewText}"), GenerationSpec()),), query_id="fixture")
            result = chain.run_batch(records(), lambda batch, **_: [
                engine_result(row, "rewritten") for row in batch])
            self.assertEqual([r.values["summary"] for r in result["records"]], ["rewritten"] * 3)


if __name__ == "__main__":
    unittest.main()
