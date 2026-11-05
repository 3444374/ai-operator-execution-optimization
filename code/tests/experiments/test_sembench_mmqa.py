"""MMQA input preparation and original-query/evaluator dispatch without models."""

import csv
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import shutil
import stat
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import pandas as pd

from src.baselines.text import sembench_mmqa as mmqa


@dataclass
class RetrievalMetric:
    precision: float
    recall: float
    f1_score: float


class MMQATextInputTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="mmqa-text-inputs-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.source = self.root / "author assets; literal paths"
        self.source.mkdir()
        self.output = self.root / "text output"
        self.calls = []
        calls = self.calls

        class Generator:
            def __init__(self, *_args, **_kwargs):
                raise AssertionError("the constructor owns setup outside this adapter")

            def _copy_fixed_files(self):
                raise AssertionError("do not call the author's unquoted shell copy")

            def _download_source_data(self):
                raise AssertionError("text preparation must not download")

            def _generate_images_per_scale_factor(self):
                raise AssertionError("text preparation must not generate images")

            def write(self, input_name, output_name):
                calls.append((input_name, self.source_data_dir, self.output_data_dir, self.scale_factor))
                shutil.copyfile(Path(self.source_data_dir) / input_name,
                                Path(self.output_data_dir) / output_name)

            def _generate_ben_piazza_text_data_per_scale_factor(self):
                self.write("base_ben_piazza_text_data.csv", "ben_piazza_text_data.csv")

            def _generate_lizzy_caplan_text_data_per_scale_factor(self):
                self.write("base_lizzy_caplan_text_data.csv", "lizzy_caplan_text_data.csv")

            def _generate_airport_text_data_per_scale_factor(self):
                self.write("base_tampa_international_airport.csv", "tampa_international_airport.csv")

        self.module = SimpleNamespace(
            MMQADataGenerator=Generator, FIXED_FILES=["ben_piazza.csv", "ap_warrior.csv"],
            BASE_BEN_PIAZZA_TEXT_DATA_FILENAME="base_ben_piazza_text_data.csv",
            BASE_LIZZY_CAPLAN_TEXT_DATA_FILENAME="base_lizzy_caplan_text_data.csv",
            BASE_TAMPA_AIRPORT_FILENAME="base_tampa_international_airport.csv",
            ADDITIONAL_MOVIES_FILENAME="additional_movies.csv",
            ADDITIONAL_AIRPORTS_FILENAME="additional_airports.csv",
        )
        for name in ("ben_piazza.csv", "ap_warrior.csv", "base_ben_piazza_text_data.csv",
                     "base_lizzy_caplan_text_data.csv", "base_tampa_international_airport.csv",
                     "additional_movies.csv", "additional_airports.csv"):
            with (self.source / name).open("w", encoding="utf-8", newline="") as stream:
                writer = csv.writer(stream)
                writer.writerow(("title", "text"))
                writer.writerow(("A, movie", 'A quoted "description"\nwith another line'))
                writer.writerow(("Another movie", "第二个描述"))

    def test_delegates_text_generation_and_keeps_author_assets_unchanged(self):
        before = {f.name: f.read_bytes() for f in self.source.iterdir()}
        with patch.object(mmqa, "load_module", return_value=self.module) as load:
            report = mmqa.prepare_text_inputs("checkout", self.source, self.output, scale_factor=37)
        load.assert_called_once_with("checkout", "sembench_mmqa_data",
                                     "src/scenario/mmqa/preparation/generate_data.py")
        self.assertEqual([call[0] for call in self.calls], [
            "base_ben_piazza_text_data.csv", "base_lizzy_caplan_text_data.csv",
            "base_tampa_international_airport.csv"])
        self.assertTrue(all(call[1:] == (str(self.source), str(self.output), 37) for call in self.calls))
        self.assertEqual(before, {f.name: f.read_bytes() for f in self.source.iterdir()})
        self.assertEqual(report["model_calls"], 0)
        self.assertFalse(report["downloaded"])
        self.assertFalse(report["images_generated"])
        self.assertFalse((self.output / "images").exists())
        self.assertEqual(set(report["files"]), {
            "ben_piazza.csv", "ap_warrior.csv", "ben_piazza_text_data.csv",
            "lizzy_caplan_text_data.csv", "tampa_international_airport.csv"})
        for name, identity in report["files"].items():
            payload = (self.output / name).read_bytes()
            self.assertEqual(identity["sha256"], hashlib.sha256(payload).hexdigest())
            self.assertEqual(identity["bytes"], len(payload))
            self.assertEqual(identity["rows"], 2)
            self.assertEqual(identity["columns"], ["title", "text"])
            self.assertEqual(stat.S_IMODE((self.output / name).stat().st_mode), 0o600)
        self.assertEqual((self.output / "ben_piazza.csv").read_bytes(), before["ben_piazza.csv"])
        self.assertEqual(json.loads((self.output / "manifest.json").read_text()), report)
        self.assertEqual(stat.S_IMODE(self.output.stat().st_mode), 0o700)

    def test_fresh_output_never_overwrites_previous_evidence(self):
        self.output.mkdir()
        sentinel = self.output / "sentinel"
        sentinel.write_text("keep existing input and evidence")
        with patch.object(mmqa, "load_module", return_value=self.module):
            with self.assertRaises(FileExistsError):
                mmqa.prepare_text_inputs("checkout", self.source, self.output)
        self.assertEqual(sentinel.read_text(), "keep existing input and evidence")
        self.assertEqual(self.calls, [])

    def test_missing_author_asset_fails_before_creating_output(self):
        (self.source / "additional_movies.csv").unlink()
        with patch.object(mmqa, "load_module", return_value=self.module):
            with self.assertRaises(FileNotFoundError):
                mmqa.prepare_text_inputs("checkout", self.source, self.output)
        self.assertFalse(self.output.exists())
        self.assertEqual(self.calls, [])

    def test_upstream_scale_contract_is_checked_before_setup(self):
        with patch.object(mmqa, "load_module") as load:
            for invalid in (24, 1001, True, "25", 25.0):
                with self.subTest(scale=invalid), self.assertRaises(ValueError):
                    mmqa.prepare_text_inputs("checkout", self.source, self.output, invalid)
        load.assert_not_called()
        self.assertFalse(self.output.exists())


class MMQAOriginalQueryTests(unittest.TestCase):
    def test_dispatch_preserves_upstream_loader_kwargs_and_return_value(self):
        queries = ("1", "3a", "3b", "3c", "3d", "3e", "3f", "3g", "4", "6a", "6b", "6c")
        self.assertEqual(mmqa.TEXT_QUERY_IDS, queries)

        class Runner:
            def __init__(self):
                raise AssertionError("benchmark setup must not run")

        def implementation(key):
            def original(owner):
                return owner.load_data("original-" + key + ".csv", sep=",", quotechar='"')
            return original

        for key in queries:
            setattr(Runner, "_execute_q" + key, implementation(key))
        module = SimpleNamespace(LotusRunner=Runner)
        with patch.object(mmqa, "load_module", return_value=module):
            for key in queries:
                with self.subTest(query=key):
                    marker = object()
                    loader = Mock(return_value=marker)
                    self.assertIs(mmqa.run_original_query("checkout", "Q" + key, loader), marker)
                    loader.assert_called_once_with("original-" + key + ".csv", sep=",", quotechar='"')

    def test_unsupported_operators_and_variants_do_not_load_or_run_benchmark(self):
        with patch.object(mmqa, "load_module") as load:
            for key in ("2a", "2b", "3", "5", "6", "7", "", True, 3.0):
                with self.subTest(query=key), self.assertRaises(ValueError):
                    mmqa.run_original_query("checkout", key, Mock())
        load.assert_not_called()


class MMQAOriginalEvaluationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="mmqa-original-evaluation-")
        self.addCleanup(temporary.cleanup)
        self.checkout = Path(temporary.name).resolve()
        self.truth = self.checkout / "files/mmqa/query/natural_language"
        self.truth.mkdir(parents=True)
        self.calls = []
        calls = self.calls

        class Evaluator:
            def __init__(self):
                raise AssertionError("evaluator setup must not write benchmark artifacts")

            def _evaluate_single_query(self, query, frame, path):
                calls.append((query, frame, path))
                truth = json.loads(Path(path).read_text())["ground_truth"]
                return RetrievalMetric(float(len(frame)), float(len(truth)), 0.5)

        self.module = SimpleNamespace(MMQAEvaluator=Evaluator)

    def test_q1_series_matches_official_csv_shape_without_changing_values(self):
        (self.truth / "q1.json").write_text(json.dumps({"ground_truth": ["Michael Ritchie"]}))
        series = pd.Series(["Michael Ritchie"], index=pd.Index([9], name="source"), name="director")
        original = series.copy(deep=True)
        with patch.object(mmqa, "load_module", return_value=self.module):
            report = mmqa.evaluate_original(self.checkout, 1, series)
        query, frame, path = self.calls[0]
        self.assertEqual(query, "1")
        self.assertEqual(path, str(self.truth / "q1.json"))
        pd.testing.assert_frame_equal(frame, original.to_frame())
        pd.testing.assert_series_equal(series, original)
        self.assertEqual(report["original_metric"], {"precision": 1.0, "recall": 1.0, "f1_score": 0.5})
        self.assertEqual(report["sembench_commit"], mmqa.SEMBENCH_COMMIT)

    def test_each_variant_keeps_its_own_ground_truth_and_frame(self):
        frame = pd.DataFrame({"title": ["A movie"]}, index=[7])
        for query, truth in (("3a", ["Comedy"]), ("3b", ["Sci-fi", "Other"])):
            (self.truth / ("q" + query + ".json")).write_text(json.dumps({"ground_truth": truth}))
            with patch.object(mmqa, "load_module", return_value=self.module):
                report = mmqa.evaluate_original(self.checkout, query, frame)
            key, actual, path = self.calls[-1]
            self.assertEqual(key, query)
            self.assertIs(actual, frame)
            self.assertEqual(Path(path).name, "q" + query + ".json")
            self.assertEqual(report["original_metric"]["recall"], float(len(truth)))

    def test_original_evaluator_errors_are_not_hidden(self):
        with patch.object(mmqa, "load_module", return_value=self.module):
            with self.assertRaises(FileNotFoundError):
                mmqa.evaluate_original(self.checkout, "4", pd.DataFrame())


if __name__ == "__main__":
    unittest.main()
