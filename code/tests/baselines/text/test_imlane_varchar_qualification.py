"""CPU checks of preparation and evidence boundaries; no native DSO is loaded."""

import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa

from integrations.imlane_duckdb import build, qualify_varchar, varchar_fixture
from src.infrastructure.private_artifacts import file_identity, write_private_json


class ConverterPreparationTests(unittest.TestCase):
    def test_original_is_preserved_and_candidate_changes_only_the_schema_argument(self):
        original = ('// fixture\n' + build.SCHEMA_ROOT_CALL + ';\n').encode()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            header = root / build.CONVERTER_HEADER
            header.parent.mkdir(parents=True)
            header.write_bytes(original)
            digest = hashlib.sha256(original).hexdigest()
            with patch.object(build, 'ORIGINAL_CONVERTER_SHA256', digest):
                unchanged = build.prepare_converter(root, schema_child=False)
                self.assertEqual(header.read_bytes(), original)
                self.assertEqual(unchanged['variant'], 'original-author')
                candidate = build.prepare_converter(root, schema_child=True)
                self.assertEqual(header.read_bytes(), ('// fixture\n' + build.SCHEMA_CHILD_CALL + ';\n').encode())
                self.assertEqual(candidate['variant'], 'schema-child-engineering')
                self.assertFalse(candidate['native_DSO_qualified'])
                with self.assertRaisesRegex(ValueError, 'pinned original'):
                    build.prepare_converter(root, schema_child=True)

    def test_unrecognized_header_is_never_modified(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            header = root / build.CONVERTER_HEADER
            header.parent.mkdir(parents=True)
            header.write_bytes(b'not the author header')
            with self.assertRaisesRegex(ValueError, 'pinned original'):
                build.prepare_converter(root, schema_child=True)
            self.assertEqual(header.read_bytes(), b'not the author header')


class VarcharFixtureTests(unittest.TestCase):
    def test_echo_preserves_strings_nulls_and_empty_input(self):
        for values in ([], ['', 'quote\"\\\n中文', None, 'L' * 8192]):
            table = pa.table({'c0': pa.array([str(i) for i in range(len(values))], type=pa.string()),
                              'c1': pa.array(values, type=pa.string())})
            for function in (varchar_fixture.echo_strings, varchar_fixture.echo_sliced):
                result = function(table)
                self.assertEqual(result['value'].to_pylist(), values)
                self.assertEqual(result['value'].num_chunks, 1)
                if function is varchar_fixture.echo_sliced:
                    self.assertEqual(result['value'].chunk(0).offset, 1)

    def test_null_case_is_an_actual_arrow_null(self):
        table = pa.table({'c0': ['0', '1', '2'], 'c1': ['before', 'null sentinel', 'after']})
        result = varchar_fixture.return_null(table)
        self.assertEqual(result['value'].to_pylist(), ['before', None, 'after'])
        self.assertEqual(result['value'].null_count, 1)


class QualificationBoundaryTests(unittest.TestCase):
    def test_mac_cannot_silently_claim_native_execution(self):
        with patch.object(qualify_varchar.sys, 'platform', 'darwin'):
            with self.assertRaisesRegex(RuntimeError, 'requires Linux'):
                qualify_varchar.require_linux()

    def test_changed_python_kernel_invalidates_the_build_record(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            evidence = dict(status='built')
            for name in ('binary', 'duckdb_library', 'fixture'):
                path = root / name
                path.write_text(name)
                evidence[name + '_path'] = str(path)
                evidence[name] = file_identity(path)
            record = root / 'build.json'
            write_private_json(record, evidence)
            self.assertEqual(qualify_varchar.read_build(record), evidence)
            (root / 'fixture').write_text('changed kernel')
            with self.assertRaisesRegex(ValueError, 'artifact changed: fixture'):
                qualify_varchar.read_build(record)

    def test_result_requires_matching_dso_and_successful_process_exit(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            library = root / 'libduckdb.so'
            library.write_bytes(b'evidence fixture, not a loaded DSO')
            evidence = dict(declared_source_variant='original-author',
                            duckdb_library_path=str(library), duckdb_library=file_identity(library))
            result = dict(schema='semloom.imlane.varchar.cpu.v1', mode='converter', case='null',
                declared_variant='original-author', converter_resolved_DSO=str(library), passed=True)
            path = root / 'result.json'
            write_private_json(path, result)
            failed = qualify_varchar.verify_result(path, evidence, mode='converter', case='null', returncode=1)
            self.assertTrue(failed['value_contract_passed'])
            self.assertFalse(failed['native_process_exit_verified'])
            self.assertFalse(failed['passed'])
            library.write_bytes(b'changed')
            with self.assertRaisesRegex(ValueError, 'DSO bytes changed'):
                qualify_varchar.verify_result(path, evidence, mode='converter', case='null', returncode=0)


if __name__ == '__main__':
    unittest.main()
