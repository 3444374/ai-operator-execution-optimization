from __future__ import annotations

import asyncio
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

OWNED = Path(__file__).resolve().parents[2] / 'integrations/imlane'
sys.path.insert(0, str(OWNED))
spec = importlib.util.spec_from_file_location('imlane_fixture_build', OWNED / 'build.py')
imlane_build = importlib.util.module_from_spec(spec)
spec.loader.exec_module(imlane_build)
try:
    import pyarrow as pa
except ModuleNotFoundError as error:
    if error.name != 'pyarrow': raise
    pa = None
if pa is not None:
    import fixture_udf


class SourceIdentityTests(unittest.TestCase):
    def check_source(self, *, actual_commit='pinned', contents=b'original', patched=False, changed=''):
        with tempfile.TemporaryDirectory() as path:
            root = Path(path)
            original = hashlib.sha256(b'original').hexdigest()
            modified = hashlib.sha256(b'patched').hexdigest()
            (root / 'file.cpp').write_bytes(contents)
            (root / 'source_identity.json').write_text(json.dumps({
                'upstream_commit': 'pinned', 'original_sha256': {'file.cpp': original},
                'patched_sha256': {'file.cpp': modified}, 'git_dependencies': {}}))
            with patch.object(imlane_build, 'OWNED', root), patch.object(
                    imlane_build.subprocess, 'check_output', side_effect=[actual_commit, changed]):
                return imlane_build.verify_source(root, patched=patched)

    def test_original_source_is_accepted(self):
        self.assertEqual(self.check_source()['upstream_commit'], 'pinned')

    def test_other_commit_is_rejected_even_with_matching_file(self):
        with self.assertRaisesRegex(ValueError, 'pinned commit'):
            self.check_source(actual_commit='other')

    def test_unreviewed_source_edits_are_rejected(self):
        with self.assertRaisesRegex(ValueError, 'source identity'):
            self.check_source(contents=b'unreviewed')

    def test_unlisted_tracked_file_edits_are_rejected(self):
        with self.assertRaisesRegex(ValueError, 'unreviewed tracked'):
            self.check_source(changed='other.cpp')

    def test_patch_is_only_accepted_in_its_explicit_mode(self):
        self.check_source(contents=b'patched', patched=True)
        with self.assertRaisesRegex(ValueError, 'source identity'):
            self.check_source(contents=b'patched')


@unittest.skipIf(pa is None, 'PyArrow is an optional IMLane fixture dependency')
class ArrowWorkloadTests(unittest.TestCase):
    def table(self):
        return pa.table({'id': pa.array([17, 17, 18], type=pa.int64()),
                         'position': pa.array([0, 1, 2], type=pa.int64()),
                         'value': pa.array([-2, None, 3], type=pa.int64()),
                         'text': pa.array(['repeat', None, 'a\x00b'])})

    def test_duplicate_ids_nulls_strings_and_shape_are_preserved(self):
        source = self.table()
        result = fixture_udf.transform(source)
        self.assertTrue(result.select(source.column_names).equals(source))
        self.assertEqual(result['doubled'].to_pylist(), [-4, None, 6])
        self.assertEqual(result.num_columns, 6)
        self.assertTrue(all(pid > 0 for pid in result['worker_pid'].to_pylist()))

    def test_async_function_returns_the_same_arrow_columns(self):
        source = self.table()
        result = asyncio.run(fixture_udf.async_transform(source))
        self.assertTrue(result.equals(fixture_udf.transform(source)))

    def test_empty_table_retains_the_same_types(self):
        result = fixture_udf.transform(self.table().slice(0, 0))
        self.assertEqual(result.num_rows, 0)
        self.assertEqual(result['doubled'].type, pa.int64())
        self.assertEqual(result['worker_pid'].type, pa.int64())

    def test_wrong_schema_is_rejected_before_transform(self):
        with self.assertRaisesRegex(ValueError, 'schema'):
            fixture_udf.transform(pa.table({'value': [1]}))
