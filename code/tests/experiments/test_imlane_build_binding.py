"""Build provenance checked by the real worker CLI before any owner starts."""

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from src.experiments import native_imlane_filters, native_imlane_sembench


class BuildBindingTests(unittest.TestCase):
    def test_current_build_and_stale_inputs_at_cli_boundary(self):
        for entry, stem, schema, variant, config_name in (
            (native_imlane_sembench, 'sembench', 'prepared', 'varchar-owned-engineering', 'mmqa_q1'),
            (native_imlane_filters, 'mmqa_filter', 'filter', 'original-author', 'mmqa_filter'),
        ):
            with self.subTest(entry=entry.__name__), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                native = entry.CODE / 'integrations/imlane_duckdb'
                binary, library = root / 'driver', root / 'libduckdb.so'
                binary.write_bytes(b'identity fixture; never executed')
                library.write_bytes(b'DSO identity fixture; never loaded')
                record = dict(schema='semloom.imlane.' + schema + '_driver_build.v1',
                    status='built', variant=variant,
                    driver_source=entry.file_identity(native / (stem + '.cpp')),
                    relation_handoff=entry.file_identity(native / 'relation_handoff.hpp'))
                for name, path in (('binary', binary), ('duckdb_library', library),
                                   ('kernel', native / (stem + '_udf.py'))):
                    record[name + '_path'] = str(path)
                    record[name] = entry.file_identity(path)
                build_path, config_path = root / 'build.json', root / 'config.json'
                config_path.write_bytes((entry.CODE / 'configs' /
                    ('sembench_imlane_' + config_name + '.example.json')).read_bytes())
                args = ['run', '--config', str(config_path), '--output', str(root / 'output'),
                    '--build-record', str(build_path), '--author-root', str(root / 'author'),
                    '--method-python', '/unused/python', '--kind', 'process',
                    '--threads', '3', '--executor-count', '2', '--execute-model', '--worker']
                with patch.object(entry, 'DSO_SHA256', record['duckdb_library']['sha256']):
                    build_path.write_text(json.dumps(record))
                    self.assertEqual(entry.verified_build(build_path), record)
                    for field in ('driver_source', 'relation_handoff', 'binary', 'duckdb_library', 'kernel'):
                        changed = json.loads(json.dumps(record))
                        changed[field]['sha256'] = '0' * 64
                        build_path.write_text(json.dumps(changed))
                        with self.subTest(field=field), self.assertRaises(ValueError):
                            entry.main(args)
                        self.assertFalse((root / 'output').exists())
                    # Legacy records remain historical evidence, but cannot claim
                    # current compiled ACK provenance without relinking the driver.
                    del record['relation_handoff']
                    build_path.write_text(json.dumps(record))
                    with self.assertRaisesRegex(ValueError, 'relation_handoff.hpp; relink'):
                        entry.main(args)
                    self.assertFalse((root / 'output').exists())
