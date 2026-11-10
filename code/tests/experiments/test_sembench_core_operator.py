"""Original F/M/E methods through real Core; model results are CPU fixtures."""

from contextlib import contextmanager
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path
import sys
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch

import lotus
import pandas as pd

from src.baselines.text.sembench_text import TextOperation
from src.experiments import sembench_core_operator as adapter
from src.execution_provider.adapters.records_async_model import AsyncCoreModelActor
from src.execution_provider.method_workers.lotus_chain_batch import FusedChainBatch
from src.semantic_methods.lotus.record_types import ExtractMethod, MapMethod, RecordOccurrence, StageSpec
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources
from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec
from tests.execution_provider.test_lotus_records_async_core import ThreadedRuntime
from tests.execution_provider.test_records_request_feedback import CPUResourceProcess


def cpu_owner(answers):
    """Replace only model execution; retain formatter, parser, transport and Core."""
    seen = []

    class Engine:
        def __init__(self, spec, *_args):
            from src.serving.backends.model_context import model_context_record
            self.ready_report = dict(engine_uid='cpu-sembench-core', model_context=model_context_record(
                spec, 'cpu-sembench-core', spec.max_model_len or 32768))

        def run_prepared(self, rows, *, generation):
            seen.extend((deepcopy(row), deepcopy(generation)) for row in rows)
            return [dict(row, status='succeeded', text=answers[row['source_position']],
                prompt_tokens=11, completion_tokens=2, total_tokens=13, finish_reason='stop',
                engine_request_id='cpu-' + row['call_id'], submission_confirmed=True,
                logprobs_json='null') for row in rows]

        def close(self, timeout=None):
            return dict(status='closed', cleanup_errors=[], process_exit_confirmed=True,
                        engine_api_shutdown_confirmed=True, gpu_exit_confirmed=None)

    class Batch(FusedChainBatch):
        _engine_type = Engine

    class Owner(adapter.SemLoomRecordChainCore):
        _batch_type = Batch

    return Owner, seen


@contextmanager
def cold_threaded_runtime():
    with ThreadedRuntime([]) as runtime:
        state = SimpleNamespace(initialized=False)
        runtime.ray.is_initialized = lambda: state.initialized
        runtime.ray.init = lambda **_kwargs: setattr(state, 'initialized', True)
        runtime.ray.shutdown = lambda: setattr(state, 'initialized', False)
        yield runtime


class CoreOperatorTests(unittest.TestCase):
    def setUp(self):
        scope = lotus.settings.context(enable_cache=False)
        scope.__enter__()
        self.addCleanup(scope.__exit__, None, None, None)
        temporary = tempfile.TemporaryDirectory(prefix='sembench-core-cpu-')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.engine = EngineSpec('/tmp/cpu-sembench', sys.executable, '/tmp/driver', max_model_calls=3)
        self.resources = RecordsResources(prepare_batch_rows=2, model_batch_rows=2,
                                         max_active_batches=1, query_timeout_s=5, startup_timeout_s=5)

    def operation(self, kind='map'):
        frame = pd.DataFrame(dict(text=['same', 'same', 'different'], business=['A', 'B', 'C']),
                             index=pd.Index([7, 7, 11], name='source_label'))
        method = (FilterMethod('openai/fixture', 'Keep {text}') if kind == 'filter' else
                  MapMethod('openai/fixture', 'Label {text}', output_column='_map') if kind == 'map' else
                  ExtractMethod('openai/fixture', ('text',), {'label': 'requested label'}))
        stage = StageSpec(kind, method, GenerationSpec(max_tokens=32,
            response_format={'type': 'json_object'} if kind == 'extract' else None)).bind()
        rows = tuple(RecordOccurrence('row-' + str(i), i, int(label), dict(text=text))
                     for i, (label, text) in enumerate(zip(frame.index, frame.text, strict=True)))
        return TextOperation(frame, stage, rows)

    def test_fused_core_uses_original_methods_restores_labels_and_drains_real_capacity(self):
        cases = {'filter': ['True', 'False', 'True'], 'map': ['first', 'second', 'third'],
                 'extract': ['{"label":"early","business":"changed"}',
                             '{"label":"late","extra":"kept"}', '{}']}
        for kind, answers in cases.items():
            with self.subTest(kind=kind):
                op = self.operation(kind)
                owner, seen = cpu_owner(answers)
                with patch.object(adapter, 'SemLoomRecordChainCore', owner), \
                        patch.object(EngineSpec, 'validate_driver_environment', return_value={}):
                    report = adapter.execute(op, engine=self.engine, resources=self.resources,
                        query_id='cpu-' + kind, profile={'execution': 'fused'}, control_dir=self.root)
                self.assertEqual(report['status'], 'completed')
                self.assertTrue(report['production_core_used'])
                self.assertEqual(len(seen), 3)
                self.assertEqual(len({r['call_id'] for r in report['calls']}), 3)
                close = report['backend_close']
                self.assertTrue(close['query_completed'])
                self.assertEqual(set(close['core_usage'].values()), {0})
                if kind == 'filter':
                    pd.testing.assert_frame_equal(report['frame'], op.frame.iloc[[0, 2]])
                elif kind == 'map':
                    self.assertEqual(report['frame']['_map'].tolist(), answers)
                    self.assertEqual(report['frame'].index.tolist(), [7, 7, 11])
                else:
                    self.assertEqual(report['frame']['label'].tolist(), ['late', 'late', None])
                    self.assertEqual(report['frame']['business'].tolist(), ['changed', 'changed', 'C'])
                    self.assertEqual(report['frame']['extra'].tolist(), ['kept', 'kept', None])

    def test_record_local_core_reorders_actual_cpu_completions_and_closes_actor_and_child(self):
        op = self.operation()
        path = self.root / 'responses.json'
        path.write_text(json.dumps({'responses': {op.stage.stage_id + '|' + row.row_id:
            dict(text=str(row.source_position), delay_s=.08 if row.source_position == 0 else 0)
            for row in op.records}}))
        engine = replace(self.engine, model_path=str(path))
        resources = replace(self.resources, prepare_batch_rows=1, model_batch_rows=1, max_active_batches=2)
        with cold_threaded_runtime(), patch.object(AsyncCoreModelActor, '_process_type', return_value=CPUResourceProcess):
            report = adapter.execute(op, engine=engine, resources=resources, query_id='cpu-async',
                profile={'execution': 'record_local_sections'}, control_dir=self.root)
        self.assertEqual(report['frame']['_map'].tolist(), ['0', '1', '2'])
        self.assertEqual(report['frame'].index.tolist(), [7, 7, 11])
        self.assertEqual(len(report['calls']), 3)
        self.assertTrue(report['ray_driver_disconnected'])
        self.assertEqual(set(report['backend_close']['core_usage'].values()), {0})
        events = [json.loads(line) for line in Path(str(path) + '.events').read_text().splitlines()]
        ends = [e['token'] for e in events if e['event'] == 'end']
        self.assertNotEqual(ends[0], op.stage.stage_id + '|row-0')
        self.assertEqual(sum(e['event'] == 'close' for e in events), 1)

    def test_empty_operation_does_not_allocate_core_or_ray(self):
        op = self.operation()
        op = replace(op, frame=op.frame.iloc[:0], records=())
        with patch.object(adapter, 'SemLoomRecordChainCore', side_effect=AssertionError('allocated')):
            report = adapter.execute(op, engine=self.engine, resources=self.resources, query_id='empty',
                                     profile={'execution': 'fused'}, control_dir=self.root)
        self.assertFalse(report['production_core_used'])
        self.assertEqual(report['calls'], [])
        self.assertIn('_map', report['frame'])

    def test_existing_ray_runtime_is_not_closed_or_reused(self):
        ray = ModuleType('ray')
        ray.is_initialized = lambda: True
        ray.shutdown = lambda: self.fail('closed an existing Ray runtime')
        resources = replace(self.resources, prepare_batch_rows=1, model_batch_rows=1)
        with patch.dict(sys.modules, {'ray': ray}), self.assertRaisesRegex(ValueError, 'isolated Ray') as failed:
            adapter.execute(self.operation(), engine=self.engine, resources=resources, query_id='existing',
                            profile={'execution': 'record_local_sections'}, control_dir=self.root)
        self.assertFalse(failed.exception.operator_report['production_core_used'])

    def test_primary_failure_keeps_calls_when_owner_close_also_fails(self):
        calls = [dict(call_id='unknown-call', status='unknown')]
        class Owner:
            last_close_report = None
            def __init__(self, *_args, **_kwargs):
                self.calls = calls
            def evaluate(self, *_args, **_kwargs):
                raise InterruptedError('CPU cancelled')
            def close(self):
                raise RuntimeError('CPU close failure')
        with patch.object(adapter, 'SemLoomRecordChainCore', Owner), \
                self.assertRaisesRegex(InterruptedError, 'CPU cancelled') as failed:
            adapter.execute(self.operation(), engine=self.engine, resources=self.resources, query_id='failure',
                            profile={'execution': 'fused'}, control_dir=self.root)
        report = failed.exception.operator_report
        self.assertEqual(report['calls'], calls)
        self.assertEqual(report['backend_close']['status'], 'unknown')
        self.assertEqual(report['status'], 'failed')

    def test_profile_rejects_unapplied_or_unsupported_settings_before_execution(self):
        for profile in ({}, {'execution': 'fused', 'ray_init': {}},
                        {'execution': 'fused', 'ready_order': 'prefix'},
                        {'execution': 'record_local_sections'}, {'execution': 'unknown'}):
            with self.subTest(profile=profile), self.assertRaises(ValueError):
                adapter.configuration(profile, self.resources)


if __name__ == '__main__':
    unittest.main()
