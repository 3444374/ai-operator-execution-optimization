"""Native graphs omit unspecified capacity without importing execution policy."""
from dataclasses import asdict
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

from src.baselines.text.frameworks.sync_reference_engine import (
    ReferenceChainEngineProcess, ReferenceEngineProcess,
)
from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec


CODE = Path(__file__).resolve().parents[2]


def cpu_bootstrap(process_type):
    program = '''import runpy,sys
from tests.serving.test_sync_model_context import native_api
module,flag,descriptor=sys.argv[1:]
sys.argv=[module,flag,descriptor]
with native_api(actual=32768):
    runpy.run_module(module,run_name='__main__')
'''
    class Process(process_type):
        @staticmethod
        def _command(engine, result_fd):
            original = process_type._command(engine, result_fd)
            return [engine.model_python, '-B', '-c', program, *original[2:]]
    return Process


class SyncReferenceRouteTests(unittest.TestCase):
    def test_record_adapters_select_native_reference_owner(self):
        from src.baselines.text.frameworks.ray_data_lotus_records import InferBatch
        from src.baselines.text.frameworks.sync_reference_engine import ReferenceVllmEngine
        from tests.serving.test_sync_model_context import native_api
        spec = EngineSpec('/tmp/CPU-ray-reference', sys.executable, sys.executable)
        with native_api(actual=32768) as (calls, closes):
            batch = InferBatch(asdict(spec), asdict(GenerationSpec()), 1, time.monotonic()+10, 3)
            try:
                self.assertIsInstance(batch.engine, ReferenceVllmEngine)
                for name in ('max_model_len', 'max_num_seqs', 'max_num_batched_tokens'):
                    self.assertNotIn(name, calls[0])
                self.assertEqual(batch.engine.ready()['model_context']['max_model_len'], 32768)
            finally:
                batch.engine.close(timeout=1)

    def test_author_environment_keeps_native_reference_processes(self):
        from integrations.imlane_records.model_process import IMLaneEngineProcess, IMLaneChainEngineProcess
        self.assertTrue(issubclass(IMLaneEngineProcess, ReferenceEngineProcess))
        self.assertTrue(issubclass(IMLaneChainEngineProcess, ReferenceChainEngineProcess))

    def test_model_side_reference_entry_does_not_import_driver_or_policy(self):
        program = '''import importlib.abc,sys
sys.path.insert(0,sys.argv[1])
class Guard(importlib.abc.MetaPathFinder):
    def find_spec(self,fullname,*args):
        forbidden=('lotus','ray','daft','vllm','torch','src.scheduling','src.semantic_methods',
                   'src.experiments','src.execution_provider','tests','scripts')
        if any(fullname==name or fullname.startswith(name+'.') for name in forbidden):
            raise AssertionError('unexpected model-side import: '+fullname)
sys.meta_path.insert(0,Guard())
from src.baselines.text.frameworks.sync_reference_engine import ReferenceVllmEngine
assert ReferenceVllmEngine.constructor_policy=='native_optional_capacity'
'''
        result = subprocess.run([sys.executable, '-I', '-B', '-c', program, str(CODE)],
            capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_both_reference_child_entries_transmit_their_actual_ready_context(self):
        for process_type in (ReferenceEngineProcess, ReferenceChainEngineProcess):
            with self.subTest(process=process_type.__name__), tempfile.TemporaryDirectory() as folder:
                spec = EngineSpec('/tmp/CPU-reference-model', sys.executable, sys.executable)
                process = cpu_bootstrap(process_type)(spec, GenerationSpec(), Path(folder),
                    time.monotonic() + 15, 8)
                try:
                    ready = process.ready_report
                    self.assertEqual(ready['kind'], 'ready')
                    self.assertEqual(ready['constructor_policy'], 'native_optional_capacity')
                    self.assertEqual(ready['constructor_parameters'], {})
                    self.assertEqual(ready['model_context']['max_model_len'], 32768)
                    self.assertIsNone(ready['model_context']['requested_max_model_len'])
                    report = process.close(3)
                    self.assertTrue(report['process_exit_confirmed'], report)
                    self.assertTrue(report['engine_api']['engine_api_shutdown_confirmed'], report)
                    self.assertIsNone(report['gpu_exit_confirmed'])
                finally:
                    process.close(3)


if __name__ == '__main__':
    unittest.main()
