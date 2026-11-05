"""IPC slot defaults follow a declared request count; no model or process startup."""
from dataclasses import asdict
from collections import deque
import json
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.lotus_records_core import create_records_core
from src.execution_provider.adapters.records_capacity import CoreRecordsCapacity
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources
from src.serving.backends.async_engine_process import AsyncEngineProcess
from src.serving.backends.vllm_async_engine import AsyncEngineLimits, VllmAsyncEngine
from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec
from tests.serving.test_async_engine_process import row


class AsyncEngineSlotDefaultsTests(unittest.TestCase):
    def test_transport_requires_a_declared_request_count_before_any_startup(self):
        with self.assertRaises(TypeError):
            AsyncEngineLimits()
        spec = EngineSpec('/tmp/capacity-fixture', '/tmp/model-python', '/tmp/driver-python')
        with patch.object(EngineSpec, 'validate_driver_environment', side_effect=AssertionError('no startup')), \
                patch.object(EngineSpec, 'validate_local', side_effect=AssertionError('no startup')):
            for owner in (AsyncEngineProcess, VllmAsyncEngine):
                with self.subTest(owner=owner.__name__), self.assertRaises(TypeError):
                    owner(spec, GenerationSpec())

    def test_omitted_totals_cover_the_declared_slots_and_complete_items(self):
        for results in (3, 16, 32):
            for totals in ({}, dict(input_bytes=None, result_bytes=None)):
                with self.subTest(results=results, totals=totals):
                    limits = AsyncEngineLimits(max_inflight=16, result_slots=results, **totals)
                    self.assertEqual(limits.input_bytes, 16*limits.item_input_bytes)
                    self.assertEqual(limits.result_bytes, results*limits.item_result_bytes)
                    self.assertEqual(AsyncEngineLimits(**json.loads(json.dumps(asdict(limits)))), limits)

    def test_real_submit_uses_all_declared_reservations_and_keeps_unconsumed_results(self):
        for explicit_total, accepted in ((None, 16), (8*2*1024**2, 8)):
            with self.subTest(explicit_total=explicit_total):
                process = object.__new__(AsyncEngineProcess)
                process.spec = EngineSpec('/tmp/capacity-fixture', '/tmp/model-python',
                    '/tmp/driver-python', max_model_len=4096)
                process.generation = GenerationSpec()
                process.limits = AsyncEngineLimits(max_inflight=16,
                    **({} if explicit_total is None else dict(result_bytes=explicit_total)))
                process._condition, process._io_done = threading.Condition(), threading.Event()
                process.process = SimpleNamespace(poll=lambda: None)
                process._broken, process._closing, process._query_id = None, False, None
                process._sequence, process._owner_uid, process._slots = 0, 'x'*32, {}
                process._data_commands, process._controls, process._ready = deque(), deque(), deque()
                deadline = time.monotonic()+60
                statuses = [process.submit(row(i), deadline_monotonic=deadline).status
                            for i in range(accepted+1)]
                self.assertEqual(statuses, ['queued']*accepted+['backpressure'])
                for slot in process._slots.values():
                    slot.producer_stopped = True
                    slot.terminal = dict(confirmed=True)
                self.assertEqual(process._usage()['active_requests'], 0)
                self.assertEqual(process.submit(row(accepted+1), deadline_monotonic=deadline).status,
                                 'backpressure')

    def test_omitted_and_null_slots_follow_the_declared_request_count(self):
        for maximum in (8, 16, 32):
            for overrides in ({}, {'result_slots': None}, {'command_slots': None},
                              {'result_slots': None, 'command_slots': None}):
                with self.subTest(maximum=maximum, overrides=overrides):
                    limits = AsyncEngineLimits(max_inflight=maximum, **overrides)
                    self.assertEqual((limits.result_slots, limits.command_slots), (maximum, maximum))
        declared = AsyncEngineLimits(max_inflight=8)
        self.assertEqual((declared.max_inflight, declared.result_slots, declared.command_slots), (8, 8, 8))
        self.assertEqual((declared.input_bytes, declared.result_bytes), (16*1024**2, 16*1024**2))
        self.assertEqual((declared.control_slots, declared.control_frame_bytes), (16, 64*1024))

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
                    AsyncEngineLimits(**(dict(max_inflight=16) | {name: invalid}))
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
