import hashlib
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from src.experiments.representation_diagnostics import diagnose_representations
from src.modalities.image.shared_media import SharedMediaRepresentation, media_key
from src.scheduling.organization.representation_retention import RetentionCandidate, consumer_value_victims


PROFILE = dict(media_recipe='CPU-fixture', model='not-invoked', generation='fixed', service_preprocess='none')
VALUE = 'data:image/png;base64,AAAA'


def identity(raw):
    digest = hashlib.sha256(raw).hexdigest()
    return digest, media_key(digest, PROFILE)


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.events = []
        self.time = 1000
        self.clock = patch('src.modalities.image.shared_media.time.monotonic_ns', lambda: self.time)
        self.clock.start(); self.addCleanup(self.clock.stop)

    def observer(self, event):
        self.events.append(dict(event, run_id='fixture', observer_session_id='session', pid=os.getpid()))

    def create(self, plan, policy=None, observer=True):
        return SharedMediaRepresentation(plan, capacity_bytes=len(VALUE), max_entries=2, max_item_bytes=100,
            max_consumers=20, retention_policy=policy, observer=self.observer if observer else None)

    def produce(self, cost):
        def prepare(raw):
            self.time += cost
            return VALUE
        return prepare

    def test_consumers_and_actual_release_are_complete_and_not_physical_RSS(self):
        digest, key = identity(b'A')
        owner = self.create({key: ('first', 'second', 'unused')})
        first = owner.borrow(b'A', digest, PROFILE, 'first', self.produce(10))
        owner.finish(digest, PROFILE, 'first')
        self.time += 25
        second = owner.borrow(b'A', digest, PROFILE, 'second', self.produce(50))
        self.assertIs(first, second)
        owner.abandon_unstarted(digest, PROFILE, 'unused')
        self.assertEqual(owner.snapshot()['retained_bytes'], len(VALUE))
        owner.finish(digest, PROFILE, 'second'); owner.close()
        report = diagnose_representations(self.events, capture_issues=[])
        self.assertEqual(report['capture_status'], 'closed_without_recorded_loss')
        observed = report['owners'][0]
        self.assertEqual((observed['successful_preparations'], observed['observed_hits']), (1, 1))
        self.assertEqual(observed['retained_payload_peak_bytes'], len(VALUE))
        self.assertEqual(observed['preparations'][0]['rebuild_ns'], 10)
        self.assertEqual(len(observed['consumer_lifetimes']), 2)
        self.assertEqual(observed['unresolved_borrows'], 0)
        self.assertIn('not process RSS', report['scope'])
        self.assertNotIn('unused', str(self.events))

    def test_observer_failures_preserve_values_and_original_producer_error(self):
        digest, key = identity(b'A')
        owner = self.create({key: ('first', 'second')})
        owner._observer = Mock(side_effect=OSError('private-data'))
        failure = ValueError('original-failure')
        with self.assertRaises(ValueError) as caught:
            owner.borrow(b'A', digest, PROFILE, 'first', Mock(side_effect=failure))
        self.assertIs(caught.exception, failure)
        self.assertEqual(owner.snapshot()['active_borrows'], 0)
        self.assertEqual(owner.borrow(b'A', digest, PROFILE, 'first', self.produce(10)), VALUE)
        owner.finish(digest, PROFILE, 'first'); owner.abandon_unstarted(digest, PROFILE, 'second'); owner.close()
        self.assertGreater(owner.snapshot()['observation_errors'], 0)
        self.assertEqual(owner.snapshot()['retained_bytes'], 0)

    def test_consumer_value_reclaims_only_idle_lower_value_representation(self):
        da, ka = identity(b'A'); db, kb = identity(b'B')
        owner = self.create({ka: ('a1', 'a2'), kb: ('b1', 'b2', 'b3')}, 'consumer_rebuild_value')
        owner.borrow(b'A', da, PROFILE, 'a1', self.produce(1)); owner.finish(da, PROFILE, 'a1')
        owner.borrow(b'B', db, PROFILE, 'b1', self.produce(100)); owner.finish(db, PROFILE, 'b1')
        self.assertNotIn(ka, owner.values); self.assertIn(kb, owner.values)
        owner.borrow(b'B', db, PROFILE, 'b2', Mock(side_effect=AssertionError('unexpected rebuild')))
        owner.finish(db, PROFILE, 'b2')
        owner.borrow(b'B', db, PROFILE, 'b3', self.produce(100)); owner.finish(db, PROFILE, 'b3')
        owner.borrow(b'A', da, PROFILE, 'a2', self.produce(1)); owner.finish(da, PROFILE, 'a2'); owner.close()
        report = diagnose_representations(self.events, capture_issues=[])
        self.assertEqual(report['capture_status'], 'closed_without_recorded_loss')
        self.assertEqual(report['owners'][0]['retention_evictions'], 1)
        self.assertEqual(owner.snapshot()['peak_retained_bytes'], len(VALUE))

    def test_active_borrow_cannot_be_evicted_even_for_larger_predicted_value(self):
        da, ka = identity(b'A'); db, kb = identity(b'B')
        owner = self.create({ka: ('a1', 'a2'), kb: ('b1', 'b2')}, 'consumer_rebuild_value')
        a = owner.borrow(b'A', da, PROFILE, 'a1', self.produce(1))
        owner.borrow(b'B', db, PROFILE, 'b1', self.produce(100))
        self.assertIs(owner.values[ka], a)
        self.assertNotIn(kb, owner.values)
        owner.finish(db, PROFILE, 'b1'); owner.abandon_unstarted(db, PROFILE, 'b2')
        owner.finish(da, PROFILE, 'a1'); owner.abandon_unstarted(da, PROFILE, 'a2'); owner.close()
        self.assertEqual(owner.snapshot()['retention_evictions'], 0)

    def test_LRU_uses_actual_borrow_recency_and_preserves_consumer_accounting(self):
        da, ka = identity(b'A'); db, kb = identity(b'B'); dc, kc = identity(b'C')
        owner = SharedMediaRepresentation({ka: ('a1', 'a2', 'a3'), kb: ('b1', 'b2'), kc: ('c1', 'c2')},
            capacity_bytes=2 * len(VALUE), max_entries=3, max_item_bytes=100, max_consumers=7,
            retention_policy='lru', observer=self.observer)
        for raw, digest, consumer in ((b'A', da, 'a1'), (b'B', db, 'b1'), (b'A', da, 'a2'), (b'C', dc, 'c1')):
            owner.borrow(raw, digest, PROFILE, consumer, self.produce(1)); owner.finish(digest, PROFILE, consumer)
        self.assertEqual(set(owner.values), {ka, kc})
        for digest, consumer in ((da, 'a3'), (db, 'b2'), (dc, 'c2')):
            owner.abandon_unstarted(digest, PROFILE, consumer)
        owner.close()
        self.assertEqual(diagnose_representations(self.events, capture_issues=[])['capture_status'],
                         'closed_without_recorded_loss')

    def test_lost_or_reordered_owner_events_do_not_produce_complete_lifetimes(self):
        digest, key = identity(b'A'); owner = self.create({key: ('first', 'second')})
        owner.borrow(b'A', digest, PROFILE, 'first', self.produce(10)); owner.finish(digest, PROFILE, 'first')
        owner.abandon_unstarted(digest, PROFILE, 'second'); owner.close()
        rows = [event for event in self.events if event['action'] != 'consumer_terminal']
        report = diagnose_representations(rows, capture_issues=[])
        self.assertEqual(report['capture_status'], 'incomplete')
        self.assertEqual(report['findings'], [])

    def test_already_active_consumers_do_not_create_future_retention_value(self):
        da, ka = identity(b'A'); db, kb = identity(b'B')
        owner = self.create({ka: ('a1', 'a2'), kb: ('b1', 'b2')}, 'consumer_rebuild_value')
        owner.borrow(b'B', db, PROFILE, 'b1', self.produce(10))
        owner.borrow(b'A', da, PROFILE, 'a1', self.produce(10))
        owner.finish(db, PROFILE, 'b1'); owner.abandon_unstarted(db, PROFILE, 'b2')
        owner.borrow(b'A', da, PROFILE, 'a2', self.produce(10))
        self.assertEqual(owner.snapshot()['retained_bytes'], 0)
        owner.finish(da, PROFILE, 'a1'); owner.finish(da, PROFILE, 'a2'); owner.close()


class RetentionChoiceTests(unittest.TestCase):
    def test_two_victims_require_more_incoming_value_than_their_combined_value(self):
        retained = (RetentionCandidate('a', 5, 1, 10), RetentionCandidate('b', 5, 1, 10))
        low = RetentionCandidate('new', 10, 1, 15)
        high = RetentionCandidate('new', 10, 1, 21)
        self.assertEqual(consumer_value_victims(low, retained, capacity_bytes=10, max_entries=2), ())
        self.assertEqual(consumer_value_victims(high, retained, capacity_bytes=10, max_entries=2), ('a', 'b'))

    def test_single_use_and_zero_cost_never_displace(self):
        retained = (RetentionCandidate('a', 5, 3, 10),)
        for incoming in (RetentionCandidate('b', 5, 0, 100), RetentionCandidate('b', 5, 2, 0)):
            self.assertEqual(consumer_value_victims(incoming, retained, capacity_bytes=5, max_entries=1), ())


class OriginalProducerTests(unittest.TestCase):
    @unittest.skipUnless(os.environ.get('SEMLOOM_REAL_LOTUS_MEDIA_CPU') == '1', 'explicit original LOTUS CPU preparation')
    def test_original_LOTUS_producer_four_arms_preserve_every_representation(self):
        from PIL import Image
        from src.experiments.media_retention_study import run_study
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); paths = []
            for i, size in enumerate((16, 32, 64)):
                path = root / f'image-{i}.png'; Image.new('RGB', (size, size), (i * 70, 20, 90)).save(path)
                paths.append(str(path))
            result = run_study(dict(schema='semloom.media_retention_study.v1', images=paths, passes=2,
                repeats=1, capacity_bytes=1000, input_kind='fixture'), root/'results')
            self.assertTrue(result['matched_outputs']); self.assertEqual(result['model_calls'], 0)
            self.assertEqual(len(result['arms']), 4)
            self.assertTrue(all(arm['diagnostic']['capture_status'] == 'closed_without_recorded_loss'
                                for arm in result['arms']))
            self.assertEqual([arm['snapshot']['cache']['capacity_bytes'] for arm in result['arms']], [1000] * 4)
            self.assertEqual(result['arms'][0]['snapshot']['encodings'], 6)


if __name__ == '__main__':
    unittest.main()
