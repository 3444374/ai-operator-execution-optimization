"""Recompute published supplemental conditions and restore sanitized CPU copies."""
from copy import deepcopy
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
import unittest
from unittest.mock import patch

from tests.serving.sync_native_source_fixture import check_native_pipeline


ROOT = Path(__file__).resolve().parents[3]
RAW = ROOT / 'experiments/results/diagnostics/records_runtime_refactor_20261104/raw'


class SupplementalEvidenceTests(unittest.TestCase):
    def test_controlled_ack_reproduces_both_original_failures_and_current_success(self):
        from tests.execution_provider.ack_order_fixture import check_ack_order
        report = check_ack_order()
        self.assertEqual(len(report['outcomes'][0]['failures']), 2)
        self.assertTrue(report['outcomes'][1]['successful'])
        self.assertEqual(report['derived_fixture_changed_call_sites'], 1)

    def test_changed_upstream_bytes_reject_before_any_engine_construction(self):
        with tempfile.TemporaryDirectory() as directory:
            file = Path(directory) / 'vllm/entrypoints/offline_utils.py'
            file.parent.mkdir(parents=True)
            file.write_text('# source that is not the pinned version\n')
            with patch('src.serving.backends.vllm_engine.VllmEngine.__init__') as construct:
                with self.assertRaisesRegex(ValueError, 'fixed vLLM source differs'):
                    check_native_pipeline(Path(directory))
            construct.assert_not_called()

    def test_public_CPU_copies_restore_without_personal_home_prefixes(self):
        home = re.compile(r'/(?:Users|home)/[^/\s"\'<>]+')
        for name in ('sync_default_methods.json.gz', 'sync_generation_methods.json.gz'):
            with self.subTest(archive=name):
                content = gzip.decompress((RAW / name).read_bytes())
                self.assertEqual(len(home.findall(content.decode())), 0, 'personal home prefixes remain')
                archive = json.loads(content)
                self.assertEqual(archive['schema'], 'semloom.cpu_methods.public_source_delta.v2')
                basis_info = archive['source_basis']
                basis_bytes = gzip.decompress((RAW.parent / basis_info['path']).read_bytes())
                self.assertEqual(hashlib.sha256(basis_bytes).hexdigest(), basis_info['decoded_sha256'])
                basis = json.loads(basis_bytes)['source_sha256']
                for row in archive['runs']:
                    result = deepcopy(row['run'])
                    delta = result['source_sha256']
                    values = {key:value for key,value in basis.items() if key not in delta['removed']}
                    values.update(delta['changed'])
                    result['source_sha256'] = {key:values[key] for key in sorted(values)}
                    data = (json.dumps(result, ensure_ascii=False, indent=2)+'\n').encode()
                    self.assertEqual(len(data), row['public_json_bytes'])
                    self.assertEqual(hashlib.sha256(data).hexdigest(), row['public_json_sha256'])
                    self.assertEqual(len(row['original_json_sha256']), 64)


class NativePipelineSourceTests(unittest.TestCase):
    def test_pinned_upstream_function_bodies_match_the_current_native_inputs(self):
        source = os.environ.get('VLLM_0251_SOURCE_ROOT')
        if not source:
            self.skipTest('set VLLM_0251_SOURCE_ROOT to the pinned source; no source download is performed')
        actual = check_native_pipeline(Path(source))
        expected = json.loads((RAW / 'sync_native_pipeline_source_check.json').read_text())
        self.assertEqual(json.loads(json.dumps(actual, ensure_ascii=False)), expected)


if __name__ == '__main__':
    unittest.main()
