import atexit
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from src.serving.backends.vllm_async_engine import NativeStats


class QueueCaptureTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory(); self.root = Path(self.folder.name)
        (self.root/'config.json').write_text(json.dumps(dict(observation=dict(max_events_per_process=100, max_event_bytes=65536))))
        self.env = patch.dict(os.environ, SEM_RECORD_OBSERVATION='on', SEM_RECORD_OBSERVER_ROOT=str(self.root),
                              SEM_RECORD_RUN_ID='queue-test', SEM_RECORD_SESSION_ID='session')
        self.env.start()
        path = Path(__file__).resolve().parents[2]/'scripts/profiling/record_observation/record_observer.py'
        spec = importlib.util.spec_from_file_location('queue_capture_observer', path)
        self.observer = importlib.util.module_from_spec(spec); spec.loader.exec_module(self.observer)
        self.original = NativeStats.record

    def tearDown(self):
        NativeStats.record = self.original
        self.observer.close_events(); atexit.unregister(self.observer.close_events)
        self.env.stop(); self.folder.cleanup()

    def test_native_logger_counters_are_observed_without_changing_snapshot(self):
        baseline = NativeStats('a' * 32, clock=lambda: 10.)
        stats = SimpleNamespace(num_running_reqs=2, num_waiting_reqs=3, kv_cache_usage=.2)
        baseline.record(stats, None)
        self.observer.install_native_queue_hook()
        observed = NativeStats('a' * 32, clock=lambda: 10.)
        with patch.object(self.observer.os, 'write', side_effect=AssertionError('no producer I/O')):
            observed.record(stats, None)
        self.assertEqual(observed.snapshot(), baseline.snapshot())
        self.observer.close_events()
        rows = [json.loads(line) for p in (self.root/'events').glob('*.jsonl') for line in p.read_text().splitlines()]
        self.assertEqual(len(rows), 1)
        self.assertEqual((rows[0]['running'], rows[0]['waiting'], rows[0]['source_sequence']), (2, 3, 1))
        self.assertIn('not GPU idle', rows[0]['scope'])

    def test_observer_exception_cannot_mask_native_result_or_counter_update(self):
        self.observer.install_native_queue_hook()
        observed = NativeStats('a' * 32, clock=lambda: 10.)
        stats = SimpleNamespace(num_running_reqs=0, num_waiting_reqs=0, kv_cache_usage=0.)
        with patch.object(self.observer, 'emit_buffered', side_effect=OSError('observer failure')):
            self.assertIsNone(observed.record(stats, None))
        self.assertEqual(observed.snapshot()['sequence'], 1)

    def test_core_identity_uses_actual_transport_model_binding_and_stays_stable(self):
        transport = SimpleNamespace(actors=SimpleNamespace(model_ready=dict(engine_uid='b' * 32)))
        core = SimpleNamespace(backend=transport)
        first = self.observer._core_identity(core)
        self.assertEqual(first[1], 'b' * 32)
        self.assertEqual(self.observer._core_identity(core), first)
        other = self.observer._core_identity(SimpleNamespace(backend=transport))
        self.assertNotEqual(other[0], first[0])

    def test_representation_events_round_trip_through_the_bounded_private_sink(self):
        from src.modalities.image.shared_media import SharedMediaRepresentation, media_key
        from src.experiments.representation_diagnostics import analyze_directory
        raw = b'NEVER_LOG_RAW_MEDIA'; digest = hashlib.sha256(raw).hexdigest()
        profile = dict(media_recipe='fixture', model='unused', generation='fixed', service_preprocess='none')
        key = media_key(digest, profile)
        def record(event):
            values = dict(event); name = values.pop('event')
            self.observer.emit_buffered(name, **values)
        self.observer.settings()
        owner = SharedMediaRepresentation({key: ('one', 'two')}, capacity_bytes=100, max_entries=1,
            max_item_bytes=100, max_consumers=2, observer=record)
        for consumer in ('one', 'two'):
            owner.borrow(raw, digest, profile, consumer, lambda source: 'data:image/png;base64,AAAA')
            owner.finish(digest, profile, consumer)
        owner.close(); self.observer.close_events()
        report = analyze_directory(self.root/'events')
        self.assertEqual(report['capture_status'], 'closed_without_recorded_loss')
        self.assertEqual(report['owners'][0]['observed_hits'], 1)
        self.assertEqual(report['owners'][0]['successful_preparations'], 1)
        content = next((self.root/'events').glob('*.jsonl')).read_text()
        self.assertNotIn('NEVER_LOG_RAW_MEDIA', content)


if __name__ == '__main__':
    unittest.main()
