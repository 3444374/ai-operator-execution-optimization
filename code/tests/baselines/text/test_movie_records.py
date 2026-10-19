"""A bounded Movie prefix preserves occurrences and excludes evaluation labels."""

import csv
import hashlib
import json
from pathlib import Path
import tempfile
from types import ModuleType
import unittest
from unittest.mock import patch

from src.baselines.text.movie_records import (
    MAX_CSV_BYTES, MAX_MANIFEST_BYTES, MAX_REVIEW_BYTES, MAX_ROWS,
    MovieRecordSource, MOVIE_CSV_DIGEST, prepare_movie_source,
)
from src.semantic_methods.lotus.records import FilterMethod, MOVIE_Q3_INSTRUCTION, RecordScan, identity


class MovieRecordSourceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.raw = self.root / "raw.csv"
        self.examples = [
            ("other", "r-other", "An ordinary review", "NEGATIVE"),
            ("taken_3", "duplicate", "Excellent, with Unicode 文本", "POSITIVE"),
            ("taken_3", "duplicate", "Not good\nsecond line", "NEGATIVE"),
            ("taken_3", "tail", "Excluded prefix tail", "POSITIVE"),
        ]
        self.write_raw(self.examples)

    def write_raw(self, rows):
        with self.raw.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(["id", "reviewId", "reviewText", "scoreSentiment"])
            writer.writerows(rows)
        self.raw_digest = hashlib.sha256(self.raw.read_bytes()).hexdigest()

    def prepare(self, **options):
        return prepare_movie_source(
            self.raw, self.root / "prepared",
            **dict(dict(expected_csv_sha256=self.raw_digest, max_rows=3), **options),
        )

    def mutate_manifest(self, path, **changes):
        value = json.loads(path.read_text())
        value.update(changes)
        path.write_text(json.dumps(value))

    def test_prefix_retains_duplicate_review_ids_as_independent_occurrences(self):
        original = self.raw.read_bytes()
        manifest_path = self.prepare()
        source = MovieRecordSource.from_manifest(manifest_path)
        self.assertEqual(self.raw.read_bytes(), original)
        rows = source.rows()
        self.assertEqual([row["row_id"] for row in rows], ["movie-row-0", "movie-row-1", "movie-row-2"])
        self.assertEqual([row["source_position"] for row in rows], [0, 1, 2])
        self.assertEqual([row["reviewId"] for row in rows], ["r-other", "duplicate", "duplicate"])
        self.assertEqual([row["reviewText"] for row in rows], [item[2] for item in self.examples[:3]])
        self.assertTrue(all(set(row) == {"row_id", "source_position", "reviewId", "id", "reviewText"}
                            for row in rows))
        self.assertEqual(source.expected("q"), [identity("q", "movie-row-1", 1),
                                                identity("q", "movie-row-2", 2)])
        self.assertNotEqual(source.expected("q")[0]["call_id"], source.expected("q")[1]["call_id"])
        report = source.preflight(FilterMethod("fixture"))
        self.assertEqual(report["status"], "passed")
        self.assertEqual(report["rows"], 3)
        self.assertEqual(report["eligible_rows"], 2)
        self.assertEqual(source.csv_path, manifest_path.parent / "records.csv")
        self.assertNotIn("scoreSentiment", source.csv_path.read_text())

    def test_labels_are_only_exposed_through_explicit_evaluation_frame(self):
        source = MovieRecordSource.from_manifest(self.prepare())
        pandas = ModuleType("pandas")
        calls = []
        pandas.DataFrame = lambda rows, columns: calls.append((rows, columns)) or rows
        with patch.dict("sys.modules", {"pandas": pandas}):
            frame = source.labeled_frame()
        self.assertEqual(calls[0][1], ["reviewId", "id", "reviewText", "scoreSentiment"])
        self.assertEqual([row["scoreSentiment"] for row in frame], [item[3] for item in self.examples[:3]])
        self.assertEqual([row["reviewText"] for row in frame], [item[2] for item in self.examples[:3]])

    def test_wrong_original_sha_is_rejected_before_output_creation(self):
        with self.assertRaisesRegex(ValueError, "CSV SHA256"):
            self.prepare(expected_csv_sha256="0" * 64)
        self.assertFalse((self.root / "prepared").exists())

    def test_source_change_after_reading_is_rejected_without_a_manifest(self):
        from src.baselines.text import movie_records
        hash_file = movie_records._sha256
        calls = []
        def changed(path):
            calls.append(path)
            if len(calls) == 2:
                self.raw.write_text(self.raw.read_text() + "\n")
            return hash_file(path)
        with patch.object(movie_records, "_sha256", side_effect=changed):
            with self.assertRaisesRegex(ValueError, "changed during"):
                self.prepare()
        self.assertFalse((self.root / "prepared" / "manifest.json").exists())

    def test_output_directory_is_exclusive(self):
        self.prepare()
        with self.assertRaises(FileExistsError):
            self.prepare()

    def test_fixed_benchmark_role_rejects_fixture_provenance(self):
        with self.assertRaisesRegex(ValueError, "fixed original"):
            self.prepare(role="sembench_movie_subset")
        with self.assertRaises(ValueError):
            self.prepare(role="original_movie_sf2000")
        from src.experiments.postgresql.sembench_queries import MOVIE_CSV_DIGEST as original
        self.assertEqual(MOVIE_CSV_DIGEST, original)

    def test_rejects_bool_non_integer_and_nonpositive_row_capacity(self):
        for value in (True, False, 0, -1, 1.0, float("nan"), float("inf"), MAX_ROWS + 1):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.prepare(max_rows=value)

    def test_task_predicate_schema_instruction_and_columns_are_bound(self):
        manifest_path = self.prepare()
        original = manifest_path.read_text()
        cases = dict(schema="other", task="movie-q2", predicate={"column": "id", "operator": "eq", "value": "other"},
                     instruction="Other {reviewText}", columns=["Image"])
        for name, value in cases.items():
            manifest_path.write_text(original)
            self.mutate_manifest(manifest_path, **{name: value})
            with self.subTest(field=name), self.assertRaisesRegex(ValueError, "task/predicate/schema"):
                MovieRecordSource.from_manifest(manifest_path)

    def test_source_preflight_rejects_another_method_or_mutated_method(self):
        source = MovieRecordSource.from_manifest(self.prepare())
        with self.assertRaisesRegex(ValueError, "FilterMethod"):
            source.preflight({"model_name": "fixture"})
        method = FilterMethod("fixture")
        object.__setattr__(method, "instruction", "Changed {reviewText}")
        with self.assertRaisesRegex(ValueError, "instruction"):
            source.preflight(method)

    def test_generic_filter_policy_is_still_rejected_by_the_movie_task_binding(self):
        source = MovieRecordSource.from_manifest(self.prepare())
        for changes in (dict(instruction="Accept {paragraph}"),
                        dict(instruction="Other {reviewText}"), dict(default=False),
                        dict(system_prompt="Custom")):
            method = FilterMethod("fixture", **changes)
            for action in (source.preflight, source.scan_spec):
                with self.subTest(changes=changes, action=action.__name__), self.assertRaisesRegex(ValueError, "Movie Q3"):
                    action(method)
        method = FilterMethod("fixture")
        object.__setattr__(method, "output_tokens", ("Yes", "No"))
        with self.assertRaises(ValueError):
            source.preflight(method)

    def test_movie_scan_spec_keeps_the_original_schema_projection_and_equality(self):
        source = MovieRecordSource.from_manifest(self.prepare())
        spec = source.scan_spec(FilterMethod("fixture"))
        self.assertIs(type(spec), RecordScan)
        self.assertEqual(dict(spec.csv_schema), dict(row_id="string", source_position="int64",
                                                   reviewId="string", id="string", reviewText="string"))
        self.assertEqual(spec.payload_columns, ("reviewText",))
        self.assertEqual(spec.predicate, ("id", "taken_3"))
        self.assertEqual(source.preflight(FilterMethod("fixture"))["method"]["instruction"], MOVIE_Q3_INSTRUCTION)
        source.csv_path.write_bytes(source.csv_path.read_bytes() + b"\n")
        with self.assertRaisesRegex(ValueError, "SHA256"):
            source.scan_spec(FilterMethod("fixture"))

    def test_records_and_labels_sha_are_rechecked_after_source_construction(self):
        manifest_path = self.prepare()
        for filename in ("records.csv", "references.csv"):
            source = MovieRecordSource.from_manifest(manifest_path)
            path = manifest_path.parent / filename
            original = path.read_bytes()
            path.write_bytes(original + b"\n")
            with self.subTest(filename=filename), self.assertRaisesRegex(ValueError, "SHA256"):
                source.preflight(FilterMethod("fixture"))
            path.write_bytes(original)

    def test_manifest_identity_is_pinned_when_source_is_selected(self):
        manifest_path = self.prepare()
        source = MovieRecordSource.from_manifest(manifest_path)
        self.mutate_manifest(manifest_path, source_rows=100)
        with self.assertRaisesRegex(ValueError, "manifest changed"):
            source.preflight(FilterMethod("fixture"))

    def test_original_csv_is_rechecked_after_source_construction(self):
        source = MovieRecordSource.from_manifest(self.prepare())
        self.write_raw([("taken_3", "changed", "Changed original", "POSITIVE")])
        with self.assertRaisesRegex(ValueError, "input CSV SHA256"):
            source.preflight(FilterMethod("fixture"))

    def test_resigned_text_and_labels_must_still_equal_the_original_prefix(self):
        manifest_path = self.prepare()
        original_manifest = manifest_path.read_text()
        for filename, changed in (("records.csv", ("Excellent", "Different")),
                                  ("references.csv", ("POSITIVE", "NEGATIVE"))):
            manifest_path.write_text(original_manifest)
            path = manifest_path.parent / filename
            original = path.read_bytes()
            path.write_bytes(original.replace(changed[0].encode(), changed[1].encode()))
            name = "records" if filename == "records.csv" else "references"
            self.mutate_manifest(manifest_path, **{name: dict(
                path=filename, sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
            )})
            with self.subTest(filename=filename), self.assertRaisesRegex(ValueError, "original CSV prefix"):
                MovieRecordSource.from_manifest(manifest_path)
            path.write_bytes(original)

    def test_file_byte_bounds_are_checked_before_csv_or_json_parsing(self):
        manifest_path = self.prepare()
        for path, limit in ((self.raw, MAX_CSV_BYTES),
                            (manifest_path.parent / "records.csv", MAX_CSV_BYTES),
                            (manifest_path.parent / "references.csv", MAX_CSV_BYTES),
                            (manifest_path, MAX_MANIFEST_BYTES)):
            original = path.read_bytes()
            with path.open("wb") as stream:
                stream.truncate(limit + 1)
            with self.subTest(path=path.name), self.assertRaisesRegex(ValueError, "file byte limit"):
                MovieRecordSource.from_manifest(manifest_path)
            path.write_bytes(original)

    def test_review_limit_uses_utf8_bytes_without_truncating_text(self):
        exact = "x" * MAX_REVIEW_BYTES
        self.write_raw([("taken_3", "large", exact, "POSITIVE")])
        source = MovieRecordSource.from_manifest(self.prepare(max_rows=1))
        self.assertEqual(source.rows()[0]["reviewText"], exact)
        self.write_raw([("taken_3", "large", "文" * (MAX_REVIEW_BYTES // 3 + 1), "POSITIVE")])
        with self.assertRaisesRegex(ValueError, "UTF-8 byte limit"):
            prepare_movie_source(self.raw, self.root / "oversize", expected_csv_sha256=self.raw_digest,
                                 max_rows=1)
        self.assertFalse((self.root / "oversize").exists())

    def test_unrelated_string_image_column_is_never_expanded(self):
        with self.raw.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(["id", "reviewId", "reviewText", "scoreSentiment", "Image"])
            writer.writerow(["taken_3", "plain", "Good film", "POSITIVE", "/does/not/exist.png"])
        self.raw_digest = hashlib.sha256(self.raw.read_bytes()).hexdigest()
        source = MovieRecordSource.from_manifest(self.prepare(max_rows=1))
        self.assertEqual(source.rows()[0]["reviewText"], "Good film")
        self.assertNotIn("Image", source.rows()[0])

    def test_artifacts_reject_traversal_absolute_paths_and_symlinks(self):
        manifest_path = self.prepare()
        original = manifest_path.read_text()
        manifest = json.loads(original)
        for name in ("../raw.csv", str(self.raw), "sub/records.csv"):
            manifest_path.write_text(original)
            self.mutate_manifest(manifest_path, records=dict(manifest["records"], path=name))
            with self.subTest(path=name), self.assertRaisesRegex(ValueError, "same-directory"):
                MovieRecordSource.from_manifest(manifest_path)
        manifest_path.write_text(original)
        records = manifest_path.parent / "records.csv"
        external = self.root / "external.csv"
        records.rename(external)
        records.symlink_to(external)
        with self.assertRaisesRegex(ValueError, "regular file"):
            MovieRecordSource.from_manifest(manifest_path)

    def test_reference_occurrence_mismatch_is_rejected_even_with_updated_digest(self):
        manifest_path = self.prepare()
        references = manifest_path.parent / "references.csv"
        text = references.read_text().replace("movie-row-1", "movie-row-0")
        references.write_text(text)
        self.mutate_manifest(manifest_path, references=dict(
            path="references.csv", sha256=hashlib.sha256(references.read_bytes()).hexdigest(),
        ))
        with self.assertRaisesRegex(ValueError, "label identity"):
            MovieRecordSource.from_manifest(manifest_path)

    def test_eligible_occurrences_are_rederived_and_bool_positions_rejected(self):
        manifest_path = self.prepare()
        original = manifest_path.read_text()
        for changes in (
            dict(eligible_rows=1), dict(eligible_occurrences=[]),
            dict(eligible_occurrences=[dict(row_id="movie-row-1", source_position=True),
                                       dict(row_id="movie-row-2", source_position=2)]),
        ):
            manifest_path.write_text(original)
            self.mutate_manifest(manifest_path, **changes)
            with self.subTest(changes=changes), self.assertRaisesRegex(ValueError, "eligible occurrences"):
                MovieRecordSource.from_manifest(manifest_path)

    def test_empty_and_no_eligible_prefixes_do_not_create_calls(self):
        self.write_raw(self.examples[:1])
        source = MovieRecordSource.from_manifest(self.prepare())
        self.assertEqual(source.expected("q"), [])
        with self.assertRaisesRegex(ValueError, "query_id"):
            source.expected("")
        self.assertEqual(source.preflight(FilterMethod("fixture"))["eligible_rows"], 0)
        self.write_raw([])
        manifest_path = prepare_movie_source(self.raw, self.root / "empty",
            expected_csv_sha256=self.raw_digest, max_rows=3)
        empty = MovieRecordSource.from_manifest(manifest_path)
        self.assertEqual(empty.rows(), [])
        self.assertEqual(empty.expected("q"), [])


if __name__ == "__main__":
    unittest.main()
