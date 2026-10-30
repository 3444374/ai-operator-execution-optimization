"""Deterministic CPU checks for optional feedback audit; no model or network."""
import atexit
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock,patch

from src.scheduling.runtime.observations import NonBlockingMetricsObservationProvider,ServiceMetricsSnapshot


class FeedbackIdentityTests(unittest.TestCase):
    def provider(self,observer):
        self.now=10.
        self.value=ServiceMetricsSnapshot(2,0,.1,source_sequence=1,source_observed_at_s=10.,
            source_uid='a'*32,source_session_id=7,received_at_s=10.)
        with patch('src.scheduling.runtime.observations.threading.Thread'):
            p=NonBlockingMetricsObservationProvider(lambda:self.value,clock=lambda:self.now,
                stale_after_s=1.,observer=observer)
        self.addCleanup(p.close)
        return p

    def tick(self,p):
        with patch.object(p._stop,'wait',return_value=True):p._sample_loop()

    def test_identity_duplicate_future_clock_and_staleness_are_recorded_without_refreshed_timestamps(self):
        events=[];p=self.provider(events.append);self.tick(p)
        self.assertTrue(p.latest(2).fresh)
        self.assertFalse(p.latest(2).fresh)
        self.assertEqual(events[-1]['reason'],'already_delivered_generation')
        self.now=10.5;self.tick(p)
        self.assertEqual(events[-1]['reason'],'duplicate_or_older_source_sequence')
        self.assertEqual(p.latest(2).sample_age_s,.5)
        self.now=11.1
        self.assertFalse(p.latest(2).fresh)
        self.assertEqual(events[-1]['reason'],'stale_snapshot')
        self.value=ServiceMetricsSnapshot(2,0,.1,source_sequence=2,source_observed_at_s=20.,
            source_uid='a'*32,source_session_id=7,received_at_s=self.now)
        self.tick(p)
        self.assertEqual(events[-1]['reason'],'source_clock_in_future')
        self.assertFalse(events[-1]['accepted'])
        self.assertEqual(events[-1]['source_sequence'],2)
        self.assertEqual(events[-1]['engine_uid'],'a'*32)
        self.assertEqual(events[-1]['session_id'],7)
        self.assertEqual(events[-1]['source_observed_at_s'],20.)
        self.assertIsNone(p.latest(2).running)

    def test_absent_audit_and_failing_audit_have_identical_observation_results(self):
        good=self.provider(None);bad=self.provider(Mock(side_effect=OSError('CPU sink failure')))
        self.tick(good);self.tick(bad)
        self.assertEqual(good.latest(2),bad.latest(2))
        self.assertEqual(good.latest(2),bad.latest(2))

    def test_installed_tool_hook_records_RPC_and_actual_consumer_decisions_without_hot_path_IO(self):
        path=Path(__file__).resolve().parents[2]/'scripts/profiling/record_observation/record_observer.py'
        with tempfile.TemporaryDirectory()as folder,patch.dict(sys.modules), \
                patch.dict(os.environ,SEM_RECORD_OBSERVATION='on',SEM_RECORD_OBSERVER_ROOT=folder):
            root=Path(folder);(root/'config.json').write_text(json.dumps(dict(observation=dict(
                mode='on',profile='handoff',max_events_per_process=100,max_event_bytes=4096))))
            spec=importlib.util.spec_from_file_location('record_observer',path)
            observer=importlib.util.module_from_spec(spec);sys.modules['record_observer']=observer;spec.loader.exec_module(observer)
            original=NonBlockingMetricsObservationProvider.__init__
            try:
                observer.settings();observer.install_feedback_observer();p=self.provider(None)
                with patch.object(observer.os,'write',side_effect=AssertionError('hot-path disk I/O')):
                    p.observe_native(dict(engine_uid='b'*32,sequence=2,observed_at_s=10.),
                        session_id=8,received_at_s=10.,accepted=False,reason='session_mismatch')
                    self.tick(p);self.assertTrue(p.latest(2).fresh)
                self.assertFalse((root/'events').exists())
                observer.close_events()
                rows=[json.loads(x)for x in next((root/'events').glob('*.jsonl')).read_text().splitlines()]
                self.assertEqual([e['stage']for e in rows],['RPC_sample','sample','consumer'])
                self.assertEqual(rows[0]['reason'],'session_mismatch');self.assertFalse(rows[0]['accepted'])
                self.assertEqual(rows[-1]['engine_uid'],'a'*32);self.assertEqual(rows[-1]['session_id'],7)
                self.assertEqual(rows[-1]['source_sequence'],1);self.assertEqual(rows[-1]['reason'],'fresh_for_consumer')
            finally:
                NonBlockingMetricsObservationProvider.__init__=original
                observer.close_events();atexit.unregister(observer.close_events)


if __name__=='__main__':unittest.main()
