"""NoPopen checks for a finite task-neutral cascade CSV projection."""

import csv
from dataclasses import replace
import os
from pathlib import Path
import stat
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from src.baselines.text.cascade_records_source import (
    MAX_CSV_BYTES, MAX_FIELD_CHARS, MAX_ROWS, MaterializedCascadeSource,
)
from src.semantic_methods.lotus.record_chain import RecordOccurrence
from src.semantic_methods.lotus.records import FilterMethod, identity


class MaterializedCascadeSourceTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="cascade-source-cpu-", dir="/tmp")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.method = FilterMethod("fixture/main", "Compare {body} with {title}",
                                   resolved_input_columns=("body", "title"))
        self.columns = ("body", "title", "synopsis")
        self.records = (
            RecordOccurrence('occ,0"文', 7, 17,
                dict(title='题目 "quoted"', body="line1\nline2,末尾\r\n", synopsis="helper 文本", oracle=12)),
            RecordOccurrence("occ-1", 21, 17,
                dict(title="another", body="", synopsis="different helper", oracle=None)),
        )
        self.popen = patch.object(subprocess, "Popen", side_effect=AssertionError("NoPopen source test"))
        self.popen.start()
        self.addCleanup(self.popen.stop)

    def source(self, **changes):
        options = dict(directory=self.root, records=self.records, method=self.method,
                       payload_columns=self.columns)
        options.update(changes)
        return MaterializedCascadeSource(**options)

    def test_utf8_newlines_quotes_and_two_role_projection_preserve_original_order(self):
        source = self.source()
        with source.csv_path.open(newline="", encoding="utf-8") as stream:
            reader = csv.DictReader(stream)
            self.assertEqual(reader.fieldnames, ["row_id", "source_position", *self.columns])
            rows = list(reader)
        self.assertEqual(rows, [dict(row_id=r.row_id, source_position=str(r.source_position),
                                    **{c:r.values[c] for c in self.columns}) for r in self.records])
        scan = source.scan_spec(self.method)
        self.assertEqual(scan.payload_columns, self.columns)
        self.assertEqual(scan.csv_schema, (("row_id", "string"), ("source_position", "int64"),
                                          *((c,"string") for c in self.columns)))
        self.assertIsNone(scan.predicate)
        self.assertNotIn("oracle", source.csv_path.read_text())
        report = source.preflight(self.method)
        self.assertEqual(report["rows"], 2)
        self.assertEqual(report["csv_bytes"], len(source.csv_path.read_bytes()))
        self.assertEqual(stat.S_IMODE(source.csv_path.stat().st_mode), 0o600)

    def test_expected_preserves_each_original_occurrence_and_freezes_caller_values(self):
        source = self.source()
        expected = [identity("query-A", r.row_id, r.source_position) for r in self.records]
        self.records[0].values["body"] = "caller changed after materialization"
        self.assertEqual(source.expected("query-A"), expected)
        self.assertEqual(source.expected("query-B"),
            [identity("query-B", r.row_id, r.source_position) for r in self.records])
        self.assertEqual(source.preflight(self.method)["status"], "passed")
        with self.assertRaises(ValueError):
            source.expected("")

    def test_empty_relation_keeps_header_and_validates_empty_query_identity(self):
        source = self.source(records=())
        self.assertEqual(source.expected("empty-query"), [])
        self.assertEqual(source.preflight(self.method)["rows"], 0)
        with source.csv_path.open(newline="", encoding="utf-8") as stream:
            self.assertEqual(list(csv.reader(stream)), [["row_id", "source_position", *self.columns]])
        with self.assertRaises(ValueError):
            source.expected(None)

    def test_method_binding_is_explicit_and_drift_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "bound FilterMethod"):
            self.source(method=FilterMethod("fixture/main", "Compare {body} with {title}"))
        self.assertFalse((self.root/"cascade-source.csv").exists())
        source = self.source()
        for method in (replace(self.method, instruction="New {body}"),
                       replace(self.method, model_name="fixture/other"),
                       replace(self.method, resolved_input_columns=("title", "body"))):
            with self.subTest(method=method), self.assertRaisesRegex(ValueError, "bound method changed"):
                source.preflight(method)

    def test_bad_projection_and_missing_or_nontext_values_leave_no_csv(self):
        for columns in (("body", "body", "title"), ("body", "title", "call_id"),
                        ("row_id", "title"), ("source_position", "title"),
                        ("body", "title", " "), ("body",), "body"):
            with self.subTest(columns=columns), self.assertRaises(ValueError):
                self.source(payload_columns=columns)
        for value in (None, 1, ["text"]):
            row = RecordOccurrence("x", 0, 0, dict(body=value, title="title", synopsis="helper"))
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "to be text"):
                self.source(records=(row,))
        row = RecordOccurrence("x", 0, 0, dict(body="text", title="title"))
        with self.assertRaisesRegex(ValueError, "to be text"):
            self.source(records=(row,))
        self.assertFalse((self.root/"cascade-source.csv").exists())

    def test_occurrence_type_duplicate_ids_positions_and_pending_updates_are_rejected(self):
        for records in (list(self.records), (object(),),
                        (self.records[0], replace(self.records[1], row_id=self.records[0].row_id)),
                        (self.records[0], replace(self.records[1], source_position=7)),
                        (replace(self.records[0], source_position=2**63),),
                        (replace(self.records[0], index_updates={"title":"pending"}),)):
            with self.subTest(records=type(records)), self.assertRaises((ValueError, TypeError)):
                self.source(records=records)
        self.assertFalse((self.root/"cascade-source.csv").exists())

    def test_row_cap_is_4096_and_rejects_the_next_occurrence_before_writing(self):
        self.assertEqual(MAX_ROWS, 4096)
        rows = tuple(RecordOccurrence("r"+str(i), i, 0, dict(body="b", title="t", synopsis="s"))
                     for i in range(MAX_ROWS+1))
        with self.assertRaisesRegex(ValueError, "at most 4096"):
            self.source(records=rows)
        self.assertFalse((self.root/"cascade-source.csv").exists())

    def test_byte_cap_counts_utf8_and_csv_quoting_before_file_creation(self):
        self.assertEqual(MAX_CSV_BYTES, 16*1024*1024)
        record = RecordOccurrence("r", 0, 0, dict(body="界"*8, title="t", synopsis="s"))
        with patch("src.baselines.text.cascade_records_source.MAX_CSV_BYTES", 64):
            with self.assertRaisesRegex(ValueError, "UTF-8 byte limit"):
                self.source(records=(record,))
        self.assertFalse((self.root/"cascade-source.csv").exists())

    def test_same_size_payload_or_identity_drift_and_oversize_files_fail_preflight(self):
        source = self.source()
        original = source.csv_path.read_bytes()
        for old, new in ((b"another", b"changed"), (b"occ-1", b"occ-9")):
            source.csv_path.write_bytes(original.replace(old, new))
            for action in (lambda:source.preflight(self.method), lambda:source.scan_spec(self.method),
                           lambda:source.expected("query")):
                with self.assertRaisesRegex(ValueError, "SHA256 changed"):
                    action()
        source.csv_path.write_bytes(original)
        with patch("src.baselines.text.cascade_records_source.MAX_CSV_BYTES", len(original)-1):
            with self.assertRaisesRegex(ValueError, "private bounded regular file"):
                source.preflight(self.method)

    def test_no_overwrite_even_when_existing_target_is_a_dangling_symlink(self):
        path = self.root / "cascade-source.csv"
        path.symlink_to(self.root/"absent")
        with self.assertRaises(FileExistsError):
            self.source()
        self.assertTrue(path.is_symlink())
        path.unlink()
        source = self.source()
        original = source.csv_path.read_bytes()
        with self.assertRaises(FileExistsError):
            self.source()
        self.assertEqual(source.csv_path.read_bytes(), original)

    def test_git_missing_and_symlink_directories_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "existing nonsymlink"):
            self.source(directory=self.root/"missing")
        linked = self.root/"linked"
        linked.symlink_to(self.root, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "existing nonsymlink"):
            self.source(directory=linked)
        (self.root/".git").mkdir()
        with self.assertRaisesRegex(ValueError, "outside a Git checkout"):
            self.source()
        self.assertFalse((self.root/"cascade-source.csv").exists())

    def test_csv_symlink_or_permission_drift_never_passes_preflight(self):
        source = self.source()
        source.csv_path.chmod(0o644)
        with self.assertRaisesRegex(ValueError, "private bounded regular file"):
            source.preflight(self.method)
        source.csv_path.chmod(0o600)
        saved = self.root/"saved.csv"
        source.csv_path.rename(saved)
        source.csv_path.symlink_to(saved)
        with self.assertRaisesRegex(ValueError, "nonsymlink regular file"):
            source.preflight(self.method)

    def test_nonregular_csv_replacement_is_rejected_without_waiting_for_a_writer(self):
        source = self.source()
        source.csv_path.unlink()
        os.mkfifo(source.csv_path, 0o600)
        with self.assertRaisesRegex(ValueError, "private bounded regular file"):
            source.preflight(self.method)

    def test_field_profile_accepts_boundary_and_rejects_larger_role_text(self):
        self.assertEqual(MAX_FIELD_CHARS, 128*1024)
        exact = self.root/"exact"
        exact.mkdir(mode=0o700)
        record = replace(self.records[0], values=dict(self.records[0].values, body="界"*MAX_FIELD_CHARS))
        source = self.source(directory=exact, records=(record,))
        with source.csv_path.open(newline="", encoding="utf-8") as stream:
            self.assertEqual(next(csv.DictReader(stream))["body"], record.values["body"])
        self.assertEqual(source.preflight(self.method)["logical_limits"]["field_unicode_chars"], MAX_FIELD_CHARS)
        too_long = replace(record, values=dict(record.values, body="界"*(MAX_FIELD_CHARS+1)))
        with self.assertRaisesRegex(ValueError, "Unicode characters"):
            self.source(records=(too_long,))
        self.assertFalse((self.root/"cascade-source.csv").exists())

    def test_field_limit_covers_column_names_and_occurrence_ids_before_writing(self):
        name = "x"*(MAX_FIELD_CHARS+1)
        record = replace(self.records[0], values={**self.records[0].values, name:"extra"})
        with self.assertRaisesRegex(ValueError, "Unicode characters"):
            self.source(records=(record,), payload_columns=(*self.columns,name))
        with self.assertRaisesRegex(ValueError, "Unicode characters"):
            self.source(records=(replace(self.records[0], row_id=name),))
        self.assertFalse((self.root/"cascade-source.csv").exists())

    def test_lower_reader_limit_is_rejected_without_changing_global_setting(self):
        previous = csv.field_size_limit()
        source = self.source()
        with patch("src.baselines.text.cascade_records_source.csv.field_size_limit",
                   return_value=MAX_FIELD_CHARS-1) as reader_limit:
            for action in (lambda:source.preflight(self.method), lambda:source.scan_spec(self.method),
                           lambda:source.expected("query")):
                with self.assertRaisesRegex(ValueError, "reader field limit"):
                    action()
            fresh = self.root/"fresh"
            fresh.mkdir(mode=0o700)
            with self.assertRaisesRegex(ValueError, "reader field limit"):
                self.source(directory=fresh)
            self.assertFalse((fresh/"cascade-source.csv").exists())
            self.assertTrue(all(not call.args and not call.kwargs for call in reader_limit.call_args_list))
        self.assertEqual(csv.field_size_limit(), previous)


if __name__ == "__main__":
    unittest.main()
