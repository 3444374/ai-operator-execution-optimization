"""Source-qualified native ages across different host clock offsets; CPU only."""
from copy import deepcopy
from types import SimpleNamespace
import sys
import threading
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.records_native_source import (
    NativeSourceClock, backend_manages_request_memory, native_sample_age,
)
from src.serving.backends.model_context import model_context_record
from src.serving.backends.vllm_async_engine import AsyncEngineLimits, VllmAsyncEngine
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec


def ready():
    # Call the actual ready packaging on a CPU object; no native engine is loaded.
    value = object.__new__(VllmAsyncEngine)
    value.spec = EngineSpec('/tmp/native-source-model', sys.executable, sys.executable, max_model_len=4096)
    value.generation = GenerationSpec()
    value.engine_uid = 'source-engine'
    value._context_observed = model_context_record(value.spec, value.engine_uid, 4096)
    value._llm = SimpleNamespace(vllm_config=SimpleNamespace())
    value._closed, value._reserved_model_calls = False, 0
    return value.ready()


def frame(sequence=1, source_at=10000., *, captured_at=10000.5):
    native = dict(engine_uid='source-engine', sequence=sequence, observed_at_s=source_at)
    packet = dict(session_id=7, native_sample_age=native_sample_age(native, ready(), captured_at_s=captured_at))
    return native, packet


class NativeSourceTests(unittest.TestCase):
    def test_actual_ready_packaging_attests_its_own_memory_owner(self):
        result = ready()
        self.assertTrue(backend_manages_request_memory(result))
        self.assertEqual(result['resource_owner']['engine_uid'], result['engine_uid'])
        self.assertNotIn('physical_request_capacity', result['resource_owner'])

    def test_missing_closed_and_foreign_owner_remain_unavailable(self):
        for change in ('missing', 'closed', 'foreign', 'unmanaged'):
            result = ready()
            if change == 'missing':result.pop('resource_owner')
            elif change == 'closed':result['closed'] = True
            elif change == 'foreign':result['resource_owner']['engine_uid'] = 'other'
            else:result['resource_owner']['request_memory_owner'] = None
            with self.subTest(change=change):
                self.assertFalse(backend_manages_request_memory(result))

    def test_source_offset_never_enters_driver_timestamp(self):
        native, packet = frame()
        mapper = NativeSourceClock('source-engine', 7)
        mapped = mapper.observed_at(native, packet, issued_at_s=20., received_at_s=20.2)
        self.assertEqual(mapped, 19.5)
        self.assertAlmostEqual(20.2-mapped, .7)
        native, packet = frame(captured_at=10003.5)
        self.assertEqual(mapper.observed_at(native, packet, issued_at_s=23., received_at_s=23.2), mapped)

    def test_invalid_age_identity_cannot_be_retimed_into_acceptance(self):
        native, packet = frame()
        mapper = NativeSourceClock('source-engine', 7)
        invalid = dict(packet, native_sample_age=None)
        with self.assertRaises(ValueError):
            mapper.observed_at(native, invalid, issued_at_s=20., received_at_s=20.2)
        with self.assertRaises(ValueError):
            mapper.observed_at(native, packet, issued_at_s=21., received_at_s=21.2)
        native, packet = frame(2, 10001., captured_at=10001.2)
        self.assertAlmostEqual(mapper.observed_at(native, packet, issued_at_s=21.3, received_at_s=21.5), 21.1)

    def test_conflicting_timestamp_and_backwards_source_are_rejected(self):
        mapper = NativeSourceClock('source-engine', 7)
        native, packet = frame(2)
        mapper.observed_at(native, packet, issued_at_s=20., received_at_s=20.2)
        changed, changed_packet = frame(2, 10000.1)
        with self.assertRaises(ValueError):
            mapper.observed_at(changed, changed_packet, issued_at_s=21., received_at_s=21.2)
        with self.assertRaises(ValueError):
            mapper.observed_at(native, packet, issued_at_s=21., received_at_s=21.2)
        native, packet = frame(1)
        with self.assertRaises(ValueError):
            mapper.observed_at(native, packet, issued_at_s=21., received_at_s=21.2)

    def test_future_nonfinite_wrong_engine_or_query_cannot_qualify(self):
        native, packet = frame()
        for kind in ('future_driver', 'nan', 'engine', 'query', 'age'):
            mapper = NativeSourceClock('source-engine', 7)
            value, source = deepcopy(packet), deepcopy(native)
            issued, received = 20., 20.2
            if kind == 'future_driver':issued = 21.
            elif kind == 'nan':received = float('nan')
            elif kind == 'engine':source['engine_uid'] = 'other'
            elif kind == 'query':value['session_id'] = 8
            else:value['native_sample_age']['age_s'] = -1.
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                mapper.observed_at(source, value, issued_at_s=issued, received_at_s=received)
        self.assertIsNone(native_sample_age(native, ready(), captured_at_s=9999.))

    def test_actual_actor_snapshot_captures_age_on_its_own_host(self):
        from src.execution_provider.adapters import records_async_model as actors
        actor = object.__new__(actors.AsyncCoreModelActor)
        actor._condition = threading.Condition()
        actor._held, actor._early_cancels = {}, {}
        actor._session, actor._readers = 7, 0
        actor._closing = actor._unhealthy = False
        actor.limits = AsyncEngineLimits(max_inflight=8)
        native, _ = frame()
        actor.engine = SimpleNamespace(ready_report=ready(), snapshot=lambda:dict(native_feedback=native))
        with patch.object(actors, 'time', SimpleNamespace(monotonic=lambda:10000.5)):
            packet = actor.snapshot()
        self.assertEqual(packet['native_sample_age']['age_s'], .5)
        self.assertEqual(NativeSourceClock('source-engine', 7).observed_at(
            native, packet, issued_at_s=20., received_at_s=20.2), 19.5)


if __name__ == '__main__':
    unittest.main()
