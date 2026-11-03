"""Configuration-only checks; no actor, model, or hardware is created."""
from dataclasses import replace
import sys
import unittest

from src.execution_provider.adapters.lotus_record_chain_core import SemLoomRecordChainCore
from src.execution_provider.adapters.lotus_records_core import resolve_core_profile
from src.scheduling.submission_control.stage_work import default_request_feedback_options
from src.semantic_methods.lotus.prepared_batching import PreparedBatching
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_engine import EngineSpec


class RollingFeedbackSignalTests(unittest.TestCase):
    def setUp(self):
        self.engine = EngineSpec('fixture', sys.executable, sys.executable)
        self.resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1,
                                          max_active_batches=8)

    def test_profile_rejects_memory_floor_without_a_rolling_memory_source(self):
        profile = dict(mode='async', admission=dict(kind='resource_feedback', memory_floor_bytes=1024))
        with self.assertRaisesRegex(ValueError, 'memory.*source'):
            resolve_core_profile(self.engine, self.resources, profile, rolling_refill=True)

    def test_direct_owner_rejects_the_same_missing_memory_source(self):
        options = replace(default_request_feedback_options(), memory_floor_bytes=1024)
        with self.assertRaisesRegex(ValueError, 'memory.*source'):
            SemLoomRecordChainCore(self.engine, self.resources,
                execution='record_local_sections',
                prepared_batching=PreparedBatching('token_budget', None, 1024),
                rolling_refill=True, request_feedback=options)

    def test_default_rolling_profile_keeps_memory_signal_unknown(self):
        profile = dict(mode='async', admission=dict(kind='resource_feedback'))
        resolved = resolve_core_profile(self.engine, self.resources, profile, rolling_refill=True)
        self.assertEqual(resolved['admission']['memory_floor_bytes'], 0)

    def test_named_historical_profiles_retain_the_window_pressure_clock(self):
        for name in ('delivered-work-scan', 'computed-work-local-pairs'):
            with self.subTest(name=name):
                resources = replace(self.resources, max_active_batches=64)
                resolved = resolve_core_profile(self.engine, resources, name)
                self.assertEqual(resolved['admission']['pressure_response'], 'window')
