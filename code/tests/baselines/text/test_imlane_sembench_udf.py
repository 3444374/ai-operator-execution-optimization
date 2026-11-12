"""Prepared Arrow and build-record boundaries; no author DSO/model is loaded."""

import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import pyarrow as pa

from integrations.imlane_duckdb import build, sembench_udf as udf


class PreparedVarcharTests(unittest.TestCase):
    def table(self, ids=('occ-0', 'occ-1')):
        messages = json.dumps([{'role': 'user', 'content': 'same original text'}])
        return pa.table({'c0': pa.array(ids, type=pa.string()),
                         'c1': pa.array([messages] * len(ids), type=pa.string())})

    def test_duplicate_messages_keep_occurrences_and_raw_model_texts(self):
        table = pa.concat_tables([self.table(('occ-0',)), self.table(('occ-1',))])
        before = table.to_pylist()
        texts = ['{"director":"雪\\nquoted"}', 'not JSON\n']
        with patch.object(udf.model_provider, 'infer_batch', return_value=texts) as provider:
            output = udf.infer_prepared(table)
        self.assertEqual(provider.call_count, 1)
        self.assertEqual(provider.call_args.args[0], [json.loads(row['c1']) for row in before])
        self.assertEqual(output.schema, udf.OUTPUT)
        self.assertEqual(output['generated_text'].to_pylist(), texts)
        self.assertEqual(output['generated_text'].num_chunks, 1)
        self.assertEqual(table.to_pylist(), before)

    def test_empty_input_does_not_call_provider(self):
        with patch.object(udf.model_provider, 'infer_batch') as provider:
            self.assertEqual(udf.infer_prepared(self.table(())).to_pylist(), [])
        provider.assert_not_called()

    def test_bad_source_contract_fails_before_provider(self):
        values = [self.table(('same', 'same')), self.table(('', 'occ-1')),
                  pa.table({'c0': [None], 'c1': ['[]']}),
                  pa.table({'c0': [1], 'c1': ['[]']}),
                  pa.table({'wrong': ['id'], 'c1': ['[]']})]
        for value in values:
            with self.subTest(schema=str(value.schema)), patch.object(udf.model_provider, 'infer_batch') as provider:
                with self.assertRaises(ValueError):
                    udf.infer_prepared(value)
                provider.assert_not_called()

    def test_bad_message_json_fails_before_provider(self):
        with patch.object(udf.model_provider, 'infer_batch') as provider:
            with self.assertRaises(json.JSONDecodeError):
                udf.infer_prepared(pa.table({'c0': ['occ'], 'c1': ['not JSON']}))
        provider.assert_not_called()

    def test_provider_cardinality_or_type_failure_has_no_replacement(self):
        for result in ([], ['only one'], [None, '{}'], ['{}', 3], ('{}', '{}')):
            with self.subTest(result=result), patch.object(udf.model_provider, 'infer_batch', return_value=result) as provider:
                with self.assertRaisesRegex(ValueError, 'cardinality or text type'):
                    udf.infer_prepared(self.table())
                self.assertEqual(provider.call_count, 1)

    def test_provider_failure_is_not_retried(self):
        failure = RuntimeError('native provider failed')
        with patch.object(udf.model_provider, 'infer_batch', side_effect=failure) as provider:
            with self.assertRaises(RuntimeError) as error:
                udf.infer_prepared(self.table())
        self.assertIs(error.exception, failure)
        self.assertEqual(provider.call_count, 1)


class PreparedDriverBuildTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.source, self.prefix = self.root / 'author', self.root / 'prefix'
        self.duckdb_source, self.duckdb_build = self.root / 'copy', self.root / 'build'
        for directory in (self.source, self.prefix, self.duckdb_source, self.duckdb_build / 'src'):
            directory.mkdir(parents=True)
        header = self.duckdb_source / build.CONVERTER_HEADER
        header.parent.mkdir(parents=True)
        header.write_bytes(b'explicit source-header fixture')
        (self.duckdb_source / 'CMakeLists.txt').write_text('fixture, never configured')
        (self.duckdb_build / 'CMakeCache.txt').write_text('existing build record fixture')
        self.library = self.duckdb_build / 'src/libduckdb.so'
        self.library.write_bytes(b'library identity fixture, not a loaded DSO')
        self.args = SimpleNamespace(root=self.root, duckdb_source=self.duckdb_source,
            duckdb_build=self.duckdb_build, output=self.root / 'new/sembench-driver', compiler='g++',
            timeout_seconds=120, duckdb_sha256=hashlib.sha256(self.library.read_bytes()).hexdigest())
        for guard in (patch.object(build.sys, 'platform', 'linux'),
                      patch.object(build, '_author', return_value=(self.source, self.prefix, {'fixture': True})),
                      patch.object(build, '_version', return_value={'fixture': True}),
                      patch.object(build, 'VARCHAR_OWNED_SHA256', hashlib.sha256(header.read_bytes()).hexdigest())):
            guard.start()
            self.addCleanup(guard.stop)

    def compiler_fixture(self, command, **kwargs):
        if command[0] == 'g++':
            self.args.output.write_bytes(b'compiler fixture, not executable')
        else:
            self.assertEqual(command[1], '--probe-dso')
            Path(command[2]).write_text(json.dumps({'converter_resolved_DSO': str(self.library),
                'expected_DSO_sha256': self.args.duckdb_sha256, 'variant': 'varchar-owned-engineering'}))
        return subprocess.CompletedProcess(command, 0, '', '')

    def test_new_entry_only_links_existing_verified_library_and_probes_its_identity(self):
        with patch.object(build.subprocess, 'run', side_effect=self.compiler_fixture) as run:
            self.assertEqual(build.driver(self.args, sembench=True), 0)
        command = run.call_args_list[0].args[0]
        self.assertIn(str(build.OWNED / 'sembench.cpp'), command)
        self.assertIn('-lduckdb', command)
        self.assertIn('-ldl', command)
        self.assertEqual(len(run.call_args_list), 2)
        record = json.loads(self.args.output.with_suffix('.build.json').read_text())
        self.assertEqual(record['status'], 'built')
        self.assertEqual(record['variant'], 'varchar-owned-engineering')
        self.assertEqual(record['driver_source'], build.file_identity(build.OWNED / 'sembench.cpp'))
        self.assertEqual(record['relation_handoff'], build.file_identity(build.OWNED / 'relation_handoff.hpp'))
        self.assertFalse(record['DSO_rebuilt'])
        self.assertEqual(record['duckdb_library_path'], str(self.library))
        self.assertEqual(record['binary_path'], str(self.args.output))
        self.assertEqual(record['kernel_path'], str(build.OWNED / 'sembench_udf.py'))

    def test_wrong_library_hash_fails_before_compilation(self):
        self.args.duckdb_sha256 = '0' * 64
        with patch.object(build.subprocess, 'run') as run:
            with self.assertRaisesRegex(ValueError, 'already-qualified'):
                build.driver(self.args, sembench=True)
        run.assert_not_called()

    def test_probe_mismatch_preserves_failed_build_and_does_not_recompile(self):
        other = self.root / 'other.so'
        other.write_bytes(self.library.read_bytes())
        def compiler(command, **kwargs):
            result = self.compiler_fixture(command, **kwargs)
            if command[0] != 'g++':
                Path(command[2]).write_text(json.dumps({'converter_resolved_DSO': str(other)}))
            return result
        with patch.object(build.subprocess, 'run', side_effect=compiler) as run:
            with self.assertRaisesRegex(ValueError, 'different or modified'):
                build.driver(self.args, sembench=True)
        record = json.loads(self.args.output.with_suffix('.build.json').read_text())
        self.assertEqual(record['status'], 'failed')
        self.assertEqual(record['error'].split(':')[0], 'prepared driver resolved a different or modified DuckDB DSO')
        self.assertEqual(len(run.call_args_list), 2)


if __name__ == '__main__':
    unittest.main()
