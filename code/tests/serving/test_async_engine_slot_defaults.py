"""IPC slot defaults follow a declared request count; no model or process startup."""
from dataclasses import asdict
import json
import unittest

from src.execution_provider.adapters.lotus_records_core import create_records_core
from src.execution_provider.adapters.records_capacity import CoreRecordsCapacity
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources
from src.serving.backends.vllm_async_engine import AsyncEngineLimits
from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec


class AsyncEngineSlotDefaultsTests(unittest.TestCase):
    def test_omitted_and_null_slots_follow_the_declared_request_count(self):
        for maximum in (8, 16, 32):
            for overrides in ({}, {'result_slots': None}, {'command_slots': None},
                              {'result_slots': None, 'command_slots': None}):
                with self.subTest(maximum=maximum, overrides=overrides):
                    limits = AsyncEngineLimits(max_inflight=maximum, **overrides)
                    self.assertEqual((limits.result_slots, limits.command_slots), (maximum, maximum))
        bare = AsyncEngineLimits()
        self.assertEqual((bare.max_inflight, bare.result_slots, bare.command_slots), (8, 8, 8))
        self.assertEqual((bare.input_bytes, bare.result_bytes), (16*1024**2, 16*1024**2))
        self.assertEqual((bare.control_slots, bare.control_frame_bytes), (16, 64*1024))

    def test_explicit_slots_and_JSON_round_trip_keep_independent_values(self):
        for result_slots, command_slots in ((1, 64), (64, 2)):
            with self.subTest(result_slots=result_slots, command_slots=command_slots):
                limits = AsyncEngineLimits(max_inflight=16, result_slots=result_slots,
                    command_slots=command_slots)
                self.assertEqual((limits.result_slots, limits.command_slots),
                                 (result_slots, command_slots))
                self.assertEqual(AsyncEngineLimits(**json.loads(json.dumps(asdict(limits)))), limits)
        raw = json.loads('{"max_inflight":32,"result_slots":null,"command_slots":null}')
        limits = AsyncEngineLimits(**raw)
        self.assertEqual((asdict(limits)['result_slots'], asdict(limits)['command_slots']), (32, 32))
        self.assertEqual(AsyncEngineLimits(**asdict(limits)), limits)

    def test_invalid_request_and_explicit_slot_counts_still_reject(self):
        for name in ('max_inflight', 'result_slots', 'command_slots'):
            for invalid in (False, 0, -1, '16'):
                with self.subTest(name=name, invalid=invalid), self.assertRaisesRegex(ValueError, name):
                    AsyncEngineLimits(**{name: invalid})
        with self.assertRaisesRegex(ValueError, 'max_inflight'):
            AsyncEngineLimits(max_inflight=None)

    def test_public_factory_keeps_slots_and_explicit_bytes_without_starting_an_actor(self):
        item = 8192
        engine = EngineSpec('/tmp/slot-model', '/tmp/slot-model-python', '/tmp/slot-driver-python')
        for maximum in (16, 32):
            limits = AsyncEngineLimits(max_inflight=maximum,
                input_bytes=maximum*item, result_bytes=maximum*item,
                item_input_bytes=item, item_result_bytes=item)
            for grant in (limits, asdict(limits)):
                with self.subTest(maximum=maximum, representation=type(grant).__name__):
                    owner = create_records_core(FilterMethod('fixture', 'Keep {reviewText}'),
                        engine, GenerationSpec(),
                        RecordsResources(prepare_batch_rows=1, model_batch_rows=1,
                                         max_active_batches=maximum),
                        profile=dict(mode='async', max_inflight='auto'), async_limits=grant,
                        capacity=CoreRecordsCapacity(item_input_bytes=item, item_result_bytes=item))
                    try:
                        self.assertEqual(owner._actors.limits, limits)
                        self.assertEqual((owner._actors.limits.result_slots,
                                          owner._actors.limits.command_slots), (maximum, maximum))
                        self.assertEqual((owner.capacity.budget.input_bytes,
                                          owner.capacity.budget.result_bytes),
                                         (maximum*item, maximum*item))
                        self.assertIsNone(owner._actors.model_actor)
                    finally:
                        owner.close(timeout=1)
