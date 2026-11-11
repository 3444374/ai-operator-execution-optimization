"""Real Arrow C Data, exact patched converter body, explicit storage test double.

Set SEMLOOM_IMLANE_AUTHOR_CHECKOUT to a local repository containing the pinned
author commit. No author DSO, SQL engine, model, network or installation is used.
"""
import ctypes as c
import gc
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

import pyarrow as pa

from integrations.imlane_duckdb import build, qualify_varchar


class ArrowArray(c.Structure):
    pass


class ArrowSchema(c.Structure):
    pass


ArrowArray._fields_ = [(name, c.c_int64) for name in ('length', 'null_count', 'offset', 'n_buffers', 'n_children')] + [
    ('buffers', c.POINTER(c.c_void_p)), ('children', c.POINTER(c.POINTER(ArrowArray))),
    ('dictionary', c.POINTER(ArrowArray)), ('release', c.CFUNCTYPE(None, c.POINTER(ArrowArray))), ('private_data', c.c_void_p)]
ArrowSchema._fields_ = [(name, c.c_char_p) for name in ('format', 'name', 'metadata')] + [
    ('flags', c.c_int64), ('n_children', c.c_int64), ('children', c.POINTER(c.POINTER(ArrowSchema))),
    ('dictionary', c.POINTER(ArrowSchema)), ('release', c.CFUNCTYPE(None, c.POINTER(ArrowSchema))), ('private_data', c.c_void_p)]

CHECKOUT = os.environ.get('SEMLOOM_IMLANE_AUTHOR_CHECKOUT')


@unittest.skipUnless(CHECKOUT and shutil.which('c++'), 'requires the existing pinned source and local C++ compiler')
class VarcharOwnedConverterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temporary.cleanup)
        cls.root = Path(cls.temporary.name)
        relative = 'database/duckdb/' + str(build.CONVERTER_HEADER)
        original = subprocess.check_output(['git', '-C', CHECKOUT, 'show', build.AUTHOR + ':' + relative])
        header = cls.root / build.CONVERTER_HEADER
        header.parent.mkdir(parents=True)
        header.write_bytes(original)
        cls.prepared = build.prepare_converter(cls.root, varchar_owned=True)
        candidate = header.read_text()
        cls.original, cls.candidate = original.decode(), candidate
        body = candidate[candidate.index('namespace duckdb {'):candidate.index('\nnamespace IMLane {')]
        (cls.root / 'converter_body.inc').write_text(body)
        syntax = cls.root / 'syntax.cpp'
        syntax.write_text('#include "duckdb/common/types/data_chunk.hpp"\n'
            '#include "duckdb/common/arrow/arrow.hpp"\n'
            '#include "duckdb/common/exception/conversion_exception.hpp"\n'
            '#include "duckdb/common/types/arrow_string_view_type.hpp"\n' + body)
        include = Path(CHECKOUT) / 'database/duckdb/src/include'
        subprocess.run(['c++', '-std=c++17', '-fsyntax-only', '-I' + str(include), str(syntax)],
            capture_output=True, text=True, check=True, timeout=30)
        fixture = Path(__file__).with_name('imlane_converter_storage_fixture.cpp')
        library = cls.root / 'converter_storage_fixture.so'
        subprocess.run(['c++', '-std=c++17', '-shared', '-fPIC', '-I' + str(cls.root),
            '-I' + pa.get_include(), str(fixture), '-o', str(library)],
            capture_output=True, text=True, check=True, timeout=30)
        cls.native = c.CDLL(str(library))
        for name, arguments, result in (
            ('new_vector', [], c.c_void_p), ('delete_vector', [c.c_void_p], None),
            ('convert', [c.POINTER(ArrowArray), c.POINTER(ArrowSchema), c.c_void_p], c.c_int),
            ('is_null', [c.c_void_p, c.c_uint64], c.c_bool),
            ('string_size', [c.c_void_p, c.c_uint64], c.c_uint32),
            ('string_data', [c.c_void_p, c.c_uint64], c.c_void_p)):
            function = getattr(cls.native, name)
            function.argtypes, function.restype = arguments, result

    def check_values(self, value, expected, *, vector=None):
        ranges = [(buf.address, buf.address + buf.size) for buf in value.buffers() if buf is not None]
        array, schema = ArrowArray(), ArrowSchema()
        value._export_to_c(c.addressof(array), c.addressof(schema))
        del value
        gc.collect()
        owns_vector = vector is None
        if owns_vector:
            vector = self.native.new_vector()
        try:
            self.assertEqual(self.native.convert(c.byref(array), c.byref(schema), vector), 0)
            for row, text in enumerate(expected):
                self.assertEqual(self.native.is_null(vector, row), text is None)
                if text is not None:
                    address = self.native.string_data(vector, row)
                    self.assertFalse(any(start <= address < end for start, end in ranges), 'borrowed Arrow buffer')
            array.release(c.byref(array))
            schema.release(c.byref(schema))
            self.assertFalse(array.release)
            self.assertFalse(schema.release)
            gc.collect()
            for row, text in enumerate(expected):
                if text is not None:
                    actual = c.string_at(self.native.string_data(vector, row), self.native.string_size(vector, row))
                    self.assertEqual(actual, text.encode())
        finally:
            if array.release:
                array.release(c.byref(array))
            if schema.release:
                schema.release(c.byref(schema))
            if owns_vector:
                self.native.delete_vector(vector)

    def test_hash_bound_variant_preserves_fixed_width_branch(self):
        self.assertEqual(self.prepared['variant'], 'varchar-owned-engineering')
        self.assertEqual(hashlib.sha256(self.candidate.encode()).hexdigest(), build.VARCHAR_OWNED_SHA256)
        self.assertEqual(qualify_varchar.VARIANTS['varchar-owned-engineering'], build.VARCHAR_OWNED_SHA256)
        original = self.original.split('            case LogicalTypeId::VARCHAR: {', 1)[0]
        self.assertEqual(self.candidate.split('            case LogicalTypeId::VARCHAR: {', 1)[0], original)
        with self.assertRaisesRegex(ValueError, 'one explicit'):
            build.prepare_converter(self.root, schema_child=True, varchar_owned=True)

    def test_real_c_data_null_slice_offset_and_long_strings_after_release(self):
        expected = [None, '中文', '', 'L' * 8192, 'after', None]
        self.check_values(pa.array(['sentinel'] * 9 + expected + ['trailer'], type=pa.string()).slice(9, 6), expected)

    def test_real_c_data_empty_strings_zero_byte_value_buffer_and_no_rows(self):
        for values in ([], [''], ['', '']):
            with self.subTest(values=values):
                value = pa.array(values, type=pa.string())
                self.assertEqual(value.buffers()[2].size, 0)
                self.check_values(value, values)

    def test_reused_output_validity_is_reset(self):
        vector = self.native.new_vector()
        try:
            self.check_values(pa.array([None] * 3, type=pa.string()), [None] * 3, vector=vector)
            self.check_values(pa.array(['first', '', 'last'], type=pa.string()), ['first', '', 'last'], vector=vector)
        finally:
            self.native.delete_vector(vector)


if __name__ == '__main__':
    unittest.main()
