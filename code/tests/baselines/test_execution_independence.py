"""Native baseline paths stay usable when the SemLoom execution core is absent."""

import os
from pathlib import Path
import subprocess
import sys
import unittest


CODE = Path(__file__).resolve().parents[2]
GUARD = """
import importlib, importlib.abc, json, pathlib, socket, sys, tempfile, time
sys.path.insert(0, sys.argv[1])
class RejectCore(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if any(fullname == p or fullname.startswith(p + '.') for p in
               ('src.execution_provider', 'src.scheduling', 'src.research')):
            raise AssertionError('native baseline loaded SemLoom execution: ' + fullname)
sys.meta_path.insert(0, RejectCore())
"""


class NativeExecutionIndependenceTests(unittest.TestCase):
    def probe(self, program):
        result = subprocess.run([sys.executable, "-B", "-c", GUARD + program, str(CODE)],
                                capture_output=True, text=True, timeout=30,
                                env=dict(os.environ, LITELLM_LOCAL_MODEL_COST_MAP="True"))
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_native_paths_import_without_core_execution_or_scheduling(self):
        self.probe("""
for name in (
    'src.baselines.image.sembench_animals',
    'src.baselines.image.sembench_mmqa',
    'src.experiments.sembench_image_query',
    'src.experiments.sembench_lotus_query',
    'src.baselines.text.sembench_source',
    'src.baselines.text.sembench_mmqa',
    'src.baselines.text.sembench_text',
    'src.baselines.text.frameworks.lotus_native_stage',
    'src.baselines.text.frameworks.sembench_text',
    'src.experiments.sembench_text_query',
    'src.baselines.text.frameworks.lotus_native_local',
    'src.baselines.text.frameworks.ray_data_lotus_records',
    'src.baselines.text.frameworks.daft_lotus_records',
    'src.baselines.text.frameworks.daft_lotus_session',
    'src.baselines.text.frameworks.ray_data_lotus_chain',
    'src.baselines.text.frameworks.daft_lotus_chain',
    'src.baselines.text.frameworks.ray_data_lotus_cascade',
    'src.baselines.text.frameworks.daft_lotus_cascade',
    'integrations.imlane_records.filter_udf',
    'integrations.imlane_records.cascade_udf',
    'integrations.imlane_records.chain_udf',
    'src.baselines.text.frameworks.kalypso_native.adapter',
    'src.baselines.text.frameworks.kalypso_native.native_server',
    'src.semantic_methods.lotus.image_backend',
    'src.baselines.image.frameworks.daft_clip',
):
    importlib.import_module(name)
""")

    def test_enabled_timing_does_not_load_core_transport(self):
        self.probe("""
from src.observability.execution_timing import Timing
with tempfile.TemporaryDirectory() as directory:
    timing = Timing(directory)
    timing.mark('CPU native observation')
    timing.close()
    assert timing.path.is_file()
""")

    def test_native_chain_executes_whole_method_batch_and_closes_without_core(self):
        self.probe("""
from src.baselines.text.frameworks.reference_chain_batch import ReferenceFusedChainBatch
from src.semantic_methods.lotus.record_chain import LotusRecordChain
from src.semantic_methods.lotus.record_types import MapMethod, RecordOccurrence, StageSpec
from src.semantic_methods.lotus.record_results import _source_binding
from src.serving.backends.model_context import model_context_record
from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec, IDENTITY_FIELDS
import lotus

class CPUProcess:
    def __init__(self, spec, *args):
        self.ready_report = dict(engine_uid='CPU-native',
            model_context=model_context_record(spec, 'CPU-native', 32768 if spec.max_model_len is None else spec.max_model_len))
        self.batches, self.closes = [], 0
    def run_prepared(self, rows, *, generation):
        self.batches.append([row['row_id'] for row in rows])
        return [dict({k: row[k] for k in IDENTITY_FIELDS}, status='succeeded',
            submission_confirmed=True, text='CPU answer', prompt_tokens=7,
            completion_tokens=1, total_tokens=8, finish_reason='stop',
            engine_request_id='CPU-' + row['call_id']) for row in rows]
    def close(self):
        self.closes += 1
        return dict(status='closed', process_exit_confirmed=True, gpu_exit_confirmed=None)

class NativeWorker(ReferenceFusedChainBatch):
    _engine_type = CPUProcess

with tempfile.TemporaryDirectory() as directory, lotus.settings.context(enable_cache=False):
    control = pathlib.Path(directory)
    (control / 'owner').write_text('CPU-token')
    records = [RecordOccurrence('r' + str(i), i, i, {'body': 'CPU source'}) for i in range(3)]
    chain = LotusRecordChain((StageSpec('rewrite', MapMethod('openai/fixture', 'Rewrite {body}'),
        GenerationSpec(max_tokens=8)),), query_id='native-without-core')
    worker = NativeWorker(chain.describe(),
        dict(model_path='/tmp/CPU-fixture', driver_python=sys.executable, model_python=sys.executable),
        [_source_binding(r) for r in records], control, time.monotonic() + 10, 3,
        socket.gethostname(), 'CPU-token')
    result = worker.run_rows([dict(record_json=json.dumps(r.describe())) for r in records])
    assert result['error_type'] is None, result
    assert worker.engine.batches == [['r0', 'r1', 'r2']]
    assert worker.engine.closes == 1
    assert [r['values']['summary'] for r in result['records']] == ['CPU answer'] * 3
    assert len(result['calls']) == 3
    assert 'request_batches' not in result
""")


if __name__ == "__main__":
    unittest.main()
