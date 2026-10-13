"""Actual Daft runtime use across original-method and SemLoom preparation."""
import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import unittest


@unittest.skipUnless(importlib.util.find_spec('daft') and importlib.util.find_spec('pyarrow'),
                     'actual pinned Daft and Arrow are required')
class DaftRuntimeCompositionTests(unittest.TestCase):
    def test_native_http_then_sealed_payload_keeps_complete_rows_in_one_driver(self):
        code = '''
from src.baselines.text.frameworks.daft_pg_http import prepare_runtime
from src.data.materializers.payloads import PayloadBatchLimits, iter_payload_batches
prepare_runtime(8)
rows = ((0,0,b'first complete request'),(0,1,b'duplicate request'),(0,2,b'duplicate request'))
tables = list(iter_payload_batches(rows,PayloadBatchLimits(3,1024),batch_rows=2))
returned = tuple((r['session_id'],r['sequence'],r['payload']) for t in tables for r in t.to_pylist())
assert returned == rows, (returned,rows)
print('NATIVE_HTTP_THEN_PAYLOAD_PASS')
'''
        self._check_driver(code, 'NATIVE_HTTP_THEN_PAYLOAD_PASS')

    def test_repeat_runtime_keeps_threads_and_rejects_a_different_configuration(self):
        code = '''
from src.baselines.text.frameworks.daft_pg_http import prepare_runtime
from src.infrastructure.daft_runtime import configure_daft_runner
from src.data.materializers.payloads import PayloadBatchLimits, iter_payload_batches
prepare_runtime(8)
rows = ((0,0,b'complete request'),)
for _ in range(2):
    tables = list(iter_payload_batches(rows,PayloadBatchLimits(1,128),batch_rows=1))
    assert tables[0].to_pylist()[0]['payload'] == rows[0][2]
    configure_daft_runner('native')
    prepare_runtime(8)
try:
    prepare_runtime(4)
except RuntimeError as error:
    assert 'thread count' in str(error)
else:
    raise AssertionError('incompatible native threads were silently accepted')
print('NATIVE_RUNTIME_REPEAT_PASS')
'''
        self._check_driver(code,'NATIVE_RUNTIME_REPEAT_PASS')

    def _check_driver(self, code, marker):
        source = Path(__file__).resolve().parents[3]
        result = subprocess.run([sys.executable,'-c',code],cwd=source,
                                env=dict(os.environ,PYTHONPATH=str(source/'code'),CUDA_VISIBLE_DEVICES=''),
                                text=True,capture_output=True,timeout=40)
        self.assertEqual(result.returncode,0,result.stdout+'\n'+result.stderr)
        self.assertIn(marker,result.stdout)


if __name__ == '__main__':
    unittest.main()
