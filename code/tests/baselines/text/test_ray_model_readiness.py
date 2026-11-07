"""Actual Ray worker readiness crosses the public iterator without a model."""

import builtins
from copy import deepcopy
from dataclasses import asdict
import json
import socket
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import pyarrow as pa

from src.baselines.text.frameworks import ray_data_lotus_records as native
from src.baselines.text.frameworks import sync_reference_engine
from src.semantic_methods.lotus.records import identity
from src.serving.backends.vllm_engine import VllmEngine
from src.serving.backends.vllm_async_engine import VllmAsyncEngine
from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec


class RayModelReadinessTests(unittest.TestCase):
    def setUp(self):
        original_import = builtins.__import__

        def guarded_import(name, *args, **kwargs):
            if name.split('.')[0] in ('ray', 'daft', 'vllm', 'torch'):
                self.fail('readiness fixture attempted a real framework import')
            return original_import(name, *args, **kwargs)

        for scope in (patch.object(builtins, '__import__', guarded_import),
                patch.object(socket.socket, 'connect', side_effect=AssertionError('network forbidden')),
                patch.object(socket.socket, 'connect_ex', side_effect=AssertionError('network forbidden')),
                patch.object(VllmEngine, '__init__', side_effect=AssertionError('real engine forbidden')),
                patch.object(VllmAsyncEngine, '__init__', side_effect=AssertionError('real engine forbidden'))):
            scope.start()
            self.addCleanup(scope.stop)
        self.spec = EngineSpec('/tmp/CPU-readiness-fixture', '/tmp/model-python', '/tmp/driver-python')
        self.generation = GenerationSpec()
        self.expected = [identity('ready-query', 'r' + str(i), i) for i in range(3)]
        self.rows = [dict(row, messages_json='[]') for row in self.expected]
        self.ready = dict(engine_uid='observed-engine', constructor_policy='native_optional_capacity',
            constructor_parameters={}, model_config=asdict(self.spec),
            model_context=dict(engine_uid='observed-engine', max_model_len=32768),
            native_capacity=dict(status='available', max_num_seqs=37, max_num_batched_tokens=8192,
                source='CPU stand-in for constructed native configuration'))
        self.instances = []

    def kernel_type(self, *, ready_error=None):
        owner = self

        class Kernel:
            def __init__(self, spec, generation):
                owner.assertEqual(spec, owner.spec)
                owner.assertEqual(generation, owner.generation)
                self.ready_calls = self.model_calls = self.closes = 0
                owner.instances.append(self)

            def ready(self):
                self.ready_calls += 1
                if ready_error is not None:
                    raise ready_error
                return deepcopy(owner.ready)

            def run_prepared(self, rows, *, deadline_monotonic):
                self.model_calls += len(rows)
                return [dict(row, engine_uid=owner.ready['engine_uid'], status='succeeded', text='True')
                        for row in rows]

            def close(self):
                self.closes += 1
                return dict(status='closed', engine_api_shutdown_confirmed=True,
                            engine_uid=owner.ready['engine_uid'], gpu_exit_confirmed=None)

        return Kernel

    def worker(self, **kwargs):
        with patch.object(sync_reference_engine, 'ReferenceVllmEngine', self.kernel_type(**kwargs)):
            return native.InferBatch(asdict(self.spec), asdict(self.generation), 3,
                                     time.monotonic() + 5, 3)

    def iterator(self, rows, *, failure=None):
        def iter_rows():
            yield from rows
            if failure is not None:
                raise failure

        self.executor = SimpleNamespace(shutdown=Mock())
        result = SimpleNamespace(iter_rows=iter_rows, _current_executor=self.executor)
        return native._RayRows(result, self.expected, time.monotonic() + 5, None)

    def test_observed_capacity_is_sent_once_and_survives_public_iterator_close(self):
        worker = self.worker()
        first = worker(pa.Table.from_pylist(self.rows[:1])).to_pylist()
        second = worker(pa.Table.from_pylist(self.rows[1:])).to_pylist()
        self.assertEqual(json.loads(first[0][native._READY_COLUMN]), self.ready)
        self.assertTrue(all(row[native._READY_COLUMN] == '' for row in second))
        iterator = self.iterator(first + second)
        output = list(iterator)
        self.assertEqual([row['call_id'] for row in output], [row['call_id'] for row in self.expected])
        self.assertTrue(all(native._READY_COLUMN not in row and native._CLOSE_COLUMN not in row for row in output))
        receipt = iterator.last_close_report['engine_ready']
        self.assertEqual(receipt, self.ready)
        self.assertIsNone(receipt['model_config']['max_num_seqs'])
        self.assertEqual(receipt['native_capacity']['max_num_seqs'], 37)
        self.assertEqual(iterator.last_close_report['status'], 'closed')
        self.assertEqual((self.instances[0].ready_calls, self.instances[0].model_calls,
                          self.instances[0].closes), (1, 3, 1))

    def test_later_native_failure_preserves_earlier_readiness_and_original_error(self):
        worker = self.worker()
        first = worker(pa.Table.from_pylist(self.rows[:1])).to_pylist()
        failure = RuntimeError('original native failure')
        iterator = self.iterator(first, failure=failure)
        self.assertEqual(next(iterator)['call_id'], self.expected[0]['call_id'])
        with self.assertRaises(RuntimeError) as caught:
            next(iterator)
        self.assertIs(caught.exception, failure)
        self.assertEqual(iterator.last_close_report['engine_ready'], self.ready)
        self.assertEqual(iterator.last_close_report['status'], 'unknown')
        self.assertIsNone(iterator.last_close_report['engine_close_report'])
        self.assertEqual(self.instances[0].model_calls, 1)

    def test_unavailable_native_capacity_stays_unknown_instead_of_using_requested_values(self):
        self.ready['native_capacity'] = dict(status='unavailable', max_num_seqs=None)
        worker = self.worker()
        iterator = self.iterator(worker(pa.Table.from_pylist(self.rows)).to_pylist())
        list(iterator)
        self.assertEqual(iterator.last_close_report['engine_ready']['native_capacity'],
                         dict(status='unavailable', max_num_seqs=None))

    def test_readiness_failure_closes_allocated_engine_and_keeps_original_exception(self):
        failure = ValueError('original readiness failure')
        with self.assertRaises(ValueError) as caught:
            self.worker(ready_error=failure)
        self.assertIs(caught.exception, failure)
        self.assertEqual((self.instances[0].ready_calls, self.instances[0].model_calls,
                          self.instances[0].closes), (1, 0, 1))

    def test_empty_relation_has_no_readiness_or_engine_allocation(self):
        with patch.object(sync_reference_engine, 'ReferenceVllmEngine') as engine:
            iterator = native._RayRows(None, [], time.monotonic() + 5, None)
            self.assertEqual(list(iterator), [])
        engine.assert_not_called()
        self.assertIsNone(iterator.last_close_report['engine_ready'])
        self.assertEqual(iterator.last_close_report['status'], 'closed')


if __name__ == '__main__':
    unittest.main()
