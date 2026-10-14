from __future__ import annotations

import dataclasses
import unittest

import numpy as np

from src.data.materializers.prepared_assets import prepared_asset_key
from src.modalities.image.input_residency import (
    DeviceInputCache, InputLease, estimate, prepared_tensor_key,
)


class InputResidencyTests(unittest.TestCase):
    def cache(self, budget=8, entries=8):
        return DeviceInputCache(budget_bytes=budget, location="actor-visible:0", max_entries=entries)

    def key(self, content=b"a"):
        return prepared_asset_key(content, "prepared-v1")

    def acquire(self, cache, content=b"a", size=4):
        return cache.acquire(self.key(content), logical_bytes=size, load=object)

    def test_two_references_share_input_but_release_independently(self):
        cache = self.cache()
        first = self.acquire(cache)
        second = cache.acquire(self.key(), logical_bytes=4,
                               load=lambda: self.fail("must reuse input"))
        self.assertIs(first.payload, second.payload)
        self.assertFalse(first.cache_hit)
        self.assertTrue(second.cache_hit)
        cache.release(second)
        self.assertEqual(cache.snapshot().held_references, 1)
        cache.clear()
        self.assertEqual(cache.snapshot().resident_bytes, 4)
        cache.release(first)
        self.assertEqual(cache.snapshot().resident_bytes, 0)

    def test_content_processor_shape_and_dtype_versions_separate_keys(self):
        pixels = np.arange(24, dtype=np.float32).reshape(2, 3, 2, 2)
        def key(array=pixels, processor="processor-v1", dtype="float16"):
            return prepared_tensor_key(array, processor_revision=processor, device_dtype=dtype)
        changed = pixels.copy()
        changed[0, 0, 0, 0] += 1
        for other in (key(changed), key(processor="processor-v2"), key(dtype="float32"),
                      key(pixels.reshape(1, 3, 2, 4)), key(pixels.astype(np.float64))):
            self.assertNotEqual(key(), other)
        self.assertEqual(key(), key(pixels.copy()))
        self.assertEqual(key(pixels[:, :, :, ::-1]), key(pixels[:, :, :, ::-1].copy()))

    def test_idle_lru_eviction_and_changed_content_reload(self):
        cache = self.cache()
        a = self.acquire(cache, b"a")
        b = self.acquire(cache, b"b")
        cache.release(a)
        cache.release(b)
        hit = self.acquire(cache, b"a")
        cache.release(hit)
        c = self.acquire(cache, b"c")
        cache.release(c)
        self.assertEqual([e.key for e in cache.snapshot().entries], [self.key(b"a"), self.key(b"c")])
        reloaded = self.acquire(cache, b"b")
        self.assertFalse(reloaded.cache_hit)
        cache.release(reloaded)
        self.assertEqual(cache.snapshot().logical_bytes_peak, 8)

    def test_entry_limit_applies_even_when_byte_budget_has_space(self):
        cache = self.cache(64, entries=1)
        first = self.acquire(cache)
        self.assertIsNone(cache.acquire(self.key(b"b"), logical_bytes=4,
                                       load=lambda: self.fail("active entry must stay held")))
        cache.release(first)
        second = self.acquire(cache, b"b")
        self.assertEqual(len(cache.snapshot().entries), 1)
        self.assertEqual(cache.snapshot().evictions, 1)
        cache.release(second)

    def test_oversized_disabled_and_active_pressure_do_not_load(self):
        for cache, size in ((self.cache(0), 1), (self.cache(4), 5)):
            self.assertIsNone(cache.acquire(self.key(), logical_bytes=size,
                                           load=lambda: self.fail("uncacheable input must bypass")))
            self.assertEqual(cache.snapshot().resident_bytes, 0)
        cache = self.cache(4)
        first = self.acquire(cache)
        self.assertIsNone(cache.acquire(self.key(b"b"), logical_bytes=4,
                                       load=lambda: self.fail("active input cannot be evicted")))
        self.assertEqual(cache.snapshot().held_references, 1)
        cache.release(first)

    def test_failed_loader_keeps_reservation_until_its_owner_fences(self):
        cache = self.cache()
        def fail():
            self.assertEqual(cache.snapshot().reserved_bytes, 4)
            raise RuntimeError("conversion submitted before error")
        with self.assertRaisesRegex(RuntimeError, "conversion"):
            cache.acquire(self.key(), logical_bytes=4, load=fail)
        self.assertEqual(cache.snapshot().reserved_bytes, 4)
        cache.clear()
        with self.assertRaisesRegex(RuntimeError, "unknown"):
            self.acquire(cache)
        cache.confirm_synchronized()
        self.assertEqual(cache.snapshot().reserved_bytes, 0)
        lease = self.acquire(cache)
        cache.release(lease)

    def test_cancellation_clear_cannot_release_unknown_or_another_reference(self):
        cache = self.cache(4)
        first, second = self.acquire(cache), self.acquire(cache)
        cache.mark_unknown(first)
        cache.clear()
        cache.release(second)
        self.assertEqual((cache.snapshot().unknown_references, cache.snapshot().resident_bytes), (1, 4))
        with self.assertRaisesRegex(RuntimeError, "synchronization"):
            cache.release(first)
        unrelated = self.cache()
        unrelated.confirm_synchronized()
        self.assertEqual(cache.snapshot().resident_bytes, 4)
        cache.confirm_synchronized()
        self.assertEqual((cache.snapshot().held_references, cache.snapshot().resident_bytes), (0, 0))

    def test_foreign_forged_and_repeated_release_are_rejected(self):
        cache, other = self.cache(), self.cache()
        lease, foreign = self.acquire(cache), self.acquire(other)
        forged = InputLease(lease.key, lease.payload, lease.logical_bytes, lease.cache_hit)
        for invalid in (foreign, forged, None):
            with self.assertRaisesRegex(ValueError, "not held"):
                cache.release(invalid)
        cache.release(lease)
        with self.assertRaisesRegex(ValueError, "not held"):
            cache.release(lease)
        other.release(foreign)

    def test_cost_and_snapshot_are_read_only_and_do_not_promote_lru(self):
        cache = self.cache()
        first, second = self.acquire(cache), self.acquire(cache, b"b")
        cache.release(first)
        cache.release(second)
        before = cache.snapshot()
        cost = cache.transfer_cost(self.key(), host_bytes=8, device_bytes=4)
        self.assertTrue(cost.reuse_available)
        self.assertEqual((cost.host_reload_bytes, cost.device_copy_bytes, cost.transfer_s), (0, 0, None))
        self.assertFalse(cost.p2p_supported)
        miss = cache.transfer_cost(self.key(b"new"), host_bytes=8, device_bytes=4)
        self.assertEqual((miss.host_reload_bytes, miss.device_copy_bytes), (8, 4))
        self.assertEqual(cache.snapshot(), before)
        with self.assertRaises(dataclasses.FrozenInstanceError):
            before.resident_bytes = 0
        third = self.acquire(cache, b"c")
        self.assertEqual([e.key for e in cache.snapshot().entries], [self.key(b"b"), self.key(b"c")])
        cache.release(third)
        self.assertNotEqual(cache.snapshot().cache_id, self.cache().snapshot().cache_id)

    def test_loader_reentry_cannot_drop_its_reservation(self):
        cache = self.cache()
        def load():
            for action in (cache.clear, cache.confirm_synchronized, lambda: self.acquire(cache, b"b")):
                with self.assertRaisesRegex(RuntimeError, "loader"):
                    action()
            return object()
        lease = cache.acquire(self.key(), logical_bytes=4, load=load)
        cache.release(lease)

    def test_existing_descriptor_locality_returns_unknown_without_inspection(self):
        class MustNotRead:
            def __getattribute__(self, _):
                raise AssertionError("selection must not read tensor or descriptor contents")
        self.assertIsNone(estimate(MustNotRead(), MustNotRead(), 0.0))


if __name__ == "__main__":
    unittest.main()
