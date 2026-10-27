"""Finite image representation identity, retention and terminal-consumer contracts."""

import hashlib
import unittest

from src.modalities.image.shared_media import SharedMediaRepresentation, media_key


class SharedImageRepresentationTests(unittest.TestCase):
    def setUp(self):
        self.raw = b'source'
        self.digest = hashlib.sha256(self.raw).hexdigest()
        self.profile = dict(media_recipe='recipe-A', model='model-A',
                            generation={'temperature': 0}, service_preprocess='processor-A')
        self.key = media_key(self.digest, self.profile)
        self.value = 'data:image/png;base64,AAAA'

    def owner(self):
        return SharedMediaRepresentation({self.key: ('a', 'b')}, capacity_bytes=100,
            max_entries=2, max_item_bytes=100, max_consumers=4)

    def test_hit_validates_source_and_releases_after_last_consumer(self):
        owner = self.owner()
        calls = []
        first = owner.borrow(self.raw, self.digest, self.profile, 'a',
                             lambda raw: calls.append(raw) or self.value)
        with self.assertRaises(ValueError):
            owner.borrow(self.raw, self.digest, self.profile, 'a', lambda _: self.value)
        owner.finish(self.digest, self.profile, 'a')
        self.assertEqual(owner.snapshot()['retained_bytes'], len(self.value))
        with self.assertRaises(ValueError):
            owner.borrow(self.raw + b'changed', self.digest, self.profile, 'b', lambda _: self.value)
        self.assertEqual(owner.snapshot()['active_borrows'], 0)
        second = owner.borrow(self.raw, self.digest, self.profile, 'b',
                              lambda _: self.fail('a retained representation must not reencode'))
        self.assertIs(first, second)
        self.assertEqual(calls, [self.raw])
        owner.finish(self.digest, self.profile, 'b')
        self.assertEqual(owner.snapshot()['retained_bytes'], 0)
        self.assertEqual(owner.snapshot()['retained_entries'], 0)
        with self.assertRaises(ValueError):
            owner.finish(self.digest, self.profile, 'b')
        owner.close()

    def test_close_and_unstarted_cancel_preserve_active_and_future_consumers(self):
        owner = self.owner()
        with self.assertRaises(RuntimeError):
            owner.close()
        owner.borrow(self.raw, self.digest, self.profile, 'a', lambda _: self.value)
        with self.assertRaises(ValueError):
            owner.abandon_unstarted(self.digest, self.profile, 'a')
        owner.abandon_unstarted(self.digest, self.profile, 'b')
        self.assertEqual(owner.snapshot()['retained_bytes'], len(self.value))
        with self.assertRaises(RuntimeError):
            owner.close()
        owner.finish(self.digest, self.profile, 'a')
        owner.close()
        self.assertEqual(owner.snapshot()['retained_bytes'], 0)

    def test_invalid_identity_and_representation_do_not_acquire_consumers(self):
        owner = self.owner()
        changed_generation = dict(self.profile, generation={'temperature': 1})
        changed_preprocess = dict(self.profile, service_preprocess='processor-B')
        cases = [
            (self.raw + b'changed', self.profile, self.value),
            (self.raw, changed_generation, self.value),
            (self.raw, changed_preprocess, self.value),
            (self.raw, self.profile, bytearray(b'mutable')),
            (self.raw, self.profile, self.value * 10),
        ]
        for raw, profile, value in cases:
            with self.subTest(raw_changed=raw != self.raw, profile=profile, value_type=type(value).__name__):
                with self.assertRaises(ValueError):
                    owner.borrow(raw, self.digest, profile, 'a', lambda _: value)
                self.assertEqual(owner.snapshot()['active_borrows'], 0)
        owner.abandon_unstarted(self.digest, self.profile, 'a')
        owner.abandon_unstarted(self.digest, self.profile, 'b')
        owner.close()

    def test_distinct_planned_profiles_do_not_share_representation(self):
        changed = dict(self.profile, service_preprocess='processor-B')
        changed_key = media_key(self.digest, changed)
        self.assertNotEqual(self.key, changed_key)
        owner = SharedMediaRepresentation({self.key: ('a', 'b'), changed_key: ('c', 'd')},
            capacity_bytes=100, max_entries=2, max_item_bytes=100, max_consumers=4)
        calls = []
        for profile, consumers in ((self.profile, ('a', 'b')), (changed, ('c', 'd'))):
            for consumer in consumers:
                owner.borrow(self.raw, self.digest, profile, consumer, lambda _: calls.append(consumer) or self.value)
                owner.finish(self.digest, profile, consumer)
        self.assertEqual(len(calls), 2)
        self.assertEqual(owner.snapshot()['cache_hits'], 2)
        owner.close()
        self.profile['generation']['temperature'] = 9
        self.assertNotEqual(media_key(self.digest, self.profile), self.key)

    def test_nonretention_reasons_are_distinct(self):
        raw=b'unit-source'; digest=hashlib.sha256(raw).hexdigest()
        profile=dict(media_recipe='unit',model='unit',generation='unit',service_preprocess='unit')
        key=media_key(digest,profile); value='data:image/png;base64,AAAA'
        for edges,capacity,enabled,expected in [
            (('a',),1024,True,(0,1,0)),
            (('a','b'),len(value)-1,True,(1,1,0)),
            (('a','b'),1024,False,(0,1,1)),
            (('a','b'),1024,True,(0,0,0)),
        ]:
            with self.subTest(edges=edges,capacity=capacity,enabled=enabled):
                owner=SharedMediaRepresentation({key:edges},capacity_bytes=capacity,
                    max_entries=1,max_item_bytes=1024,max_consumers=2,retention_enabled=enabled)
                for edge in edges:
                    owner.borrow(raw,digest,profile,edge,lambda _:value)
                    owner.finish(digest,profile,edge)
                snapshot=owner.snapshot()
                self.assertEqual((snapshot['capacity_bypasses'],snapshot['no_future_consumer_bypasses'],
                                  snapshot['retention_disabled_bypasses']),expected)
                owner.close()


if __name__ == '__main__':
    unittest.main()
