from __future__ import annotations

import unittest

from src.data.materializers.prepared_assets import (
    AssetUse,
    PreparedAssetLease,
    PreparedAssetWindow,
    prepared_asset_key,
)


class PreparedAssetsTest(unittest.TestCase):
    def window(self, *sources, host_bytes=16):
        uses = tuple(AssetUse(str(i), prepared_asset_key(source, "fixed-transform"))
                     for i, source in enumerate(sources))
        return PreparedAssetWindow(uses, max_consumers=8, host_bytes=host_bytes)

    def acquire(self, window, consumer, payload=b"ready", *, bound=5, scratch=0):
        return window.acquire(consumer, prepare=lambda: payload,
                              prepared_bytes_upper_bound=bound,
                              scratch_bytes_upper_bound=scratch)

    def test_two_logical_consumers_share_only_preparation_bytes(self):
        window = self.window(b"image", b"image")
        first = self.acquire(window, "0")
        second = self.acquire(window, "1", payload=b"must-not-prepare")
        self.assertIs(first.payload, second.payload)
        self.assertNotEqual(first.consumer_id, second.consumer_id)
        snapshot = window.snapshot()
        self.assertEqual((snapshot.held_consumers, snapshot.prepare_calls,
                          snapshot.reuse_hits, snapshot.prepared_bytes), (2, 1, 1, 5))
        window.release(second)
        self.assertEqual(window.snapshot().prepared_bytes, 5)
        window.release(first)
        self.assertEqual(window.snapshot().prepared_bytes, 0)

    def test_changed_content_or_transform_does_not_reuse(self):
        key = prepared_asset_key(b"image", "rgb-png-v1")
        self.assertNotEqual(key, prepared_asset_key(b"changed", "rgb-png-v1"))
        self.assertNotEqual(key, prepared_asset_key(b"image", "rgb-png-v2"))
        uses = (AssetUse("first", key), AssetUse("second", prepared_asset_key(b"image", "v2")))
        window = PreparedAssetWindow(uses, max_consumers=2, host_bytes=16)
        self.acquire(window, "first")
        self.acquire(window, "second")
        self.assertEqual(window.snapshot().prepare_calls, 2)

    def test_active_buffer_pressure_does_not_start_another_prepare(self):
        window = self.window(b"a", b"b", host_bytes=5)
        first = self.acquire(window, "0")
        with self.assertRaisesRegex(ValueError, "acquired"):
            self.acquire(window, "0")
        self.assertIsNone(window.acquire("1", prepare=lambda: self.fail("prepare under pressure"),
                                         prepared_bytes_upper_bound=5))
        self.assertEqual(window.snapshot().prepare_calls, 1)
        window.release(first)
        second = self.acquire(window, "1")
        window.release(second)
        self.assertEqual(window.snapshot().prepared_bytes, 0)

    def test_output_and_scratch_are_reserved_before_callback(self):
        window = self.window(b"a", host_bytes=12)
        def prepare():
            self.assertEqual(window.snapshot().preparation_reserved_bytes, 12)
            self.assertEqual(window.snapshot().host_reservation_peak_bytes, 12)
            return b"ready"
        lease = window.acquire("0", prepare=prepare, prepared_bytes_upper_bound=5,
                               scratch_bytes_upper_bound=7)
        self.assertEqual(window.snapshot().preparation_reserved_bytes, 0)
        window.release(lease)

    def test_scratch_can_block_even_when_final_outputs_would_fit(self):
        window = self.window(b"a", b"b", host_bytes=10)
        first = self.acquire(window, "0", payload=b"aaaa", bound=4)
        self.assertIsNone(window.acquire("1", prepare=lambda: self.fail("scratch not reserved"),
                                         prepared_bytes_upper_bound=2,
                                         scratch_bytes_upper_bound=6))
        window.release(first)
        self.assertIsNotNone(self.acquire(window, "1", payload=b"bb", bound=2, scratch=6))

    def test_prepare_error_leaves_consumer_pending_and_releases_reservation(self):
        window = self.window(b"a", host_bytes=12)
        def failure():
            raise RuntimeError("controlled decoder failure")
        with self.assertRaisesRegex(RuntimeError, "decoder"):
            window.acquire("0", prepare=failure, prepared_bytes_upper_bound=5,
                           scratch_bytes_upper_bound=7)
        self.assertEqual(window.snapshot().pending_consumers, 1)
        self.assertEqual(window.snapshot().preparation_reserved_bytes, 0)
        self.assertEqual(window.snapshot().prepared_bytes, 0)
        lease = self.acquire(window, "0")
        window.release(lease)

    def test_oversized_or_mutable_output_cannot_enter_cache(self):
        for output in (b"oversized", bytearray(b"ready")):
            with self.subTest(output=type(output)):
                window = self.window(b"a")
                with self.assertRaises((BufferError, TypeError)):
                    self.acquire(window, "0", output, bound=5)
                self.assertEqual(window.snapshot().prepared_bytes, 0)
                self.assertEqual(window.snapshot().pending_consumers, 1)
                self.assertEqual(window.snapshot().preparation_reserved_bytes, 0)

    def test_idle_eviction_allows_later_reprepare_without_merging_consumers(self):
        window = self.window(b"a", b"b", b"a", host_bytes=5)
        first = self.acquire(window, "0")
        window.release(first)
        self.assertEqual(window.snapshot().prepared_bytes, 5)
        second = self.acquire(window, "1")
        self.assertEqual(window.snapshot().evictions, 1)
        window.release(second)
        last = self.acquire(window, "2")
        self.assertEqual(window.snapshot().prepare_calls, 3)
        self.assertEqual(last.consumer_id, "2")
        window.release(last)
        self.assertEqual(window.snapshot().host_reservation_peak_bytes, 5)

    def test_cancel_pending_demand_does_not_release_unknown_active_consumer(self):
        window = self.window(b"a", b"a")
        active = self.acquire(window, "0")
        window.cancel_pending("1")
        self.assertEqual(window.snapshot().prepared_bytes, 5)
        with self.assertRaisesRegex(ValueError, "unacquired"):
            window.cancel_pending("0")
        window.release(active)
        self.assertEqual(window.snapshot().prepared_bytes, 0)

    def test_job_local_window_rejects_foreign_forged_and_duplicate_releases(self):
        first = self.window(b"a")
        other = self.window(b"a")
        lease = self.acquire(first, "0")
        other_lease = self.acquire(other, "0")
        for wrong in (other_lease, PreparedAssetLease(lease.consumer_id, lease.key, lease.payload)):
            with self.assertRaisesRegex(ValueError, "not held"):
                first.release(wrong)
        first.release(lease)
        with self.assertRaisesRegex(ValueError, "not held"):
            first.release(lease)
        other.release(other_lease)

    def test_callback_reentry_cannot_bypass_host_reservation(self):
        window = self.window(b"a", b"b")
        def nested():
            return self.acquire(window, "1").payload
        with self.assertRaisesRegex(RuntimeError, "callbacks cannot mutate"):
            window.acquire("0", prepare=nested, prepared_bytes_upper_bound=5)
        self.assertEqual(window.snapshot().pending_consumers, 2)
        self.assertEqual(window.snapshot().preparation_reserved_bytes, 0)

    def test_unique_assets_have_no_reuse_and_keep_each_logical_output(self):
        window = self.window(b"a", b"b", b"c")
        outputs = {}
        for i in range(3):
            lease = self.acquire(window, str(i), payload=str(i).encode(), bound=1)
            outputs[lease.consumer_id] = lease.payload
            window.release(lease)
        self.assertEqual(outputs, {"0": b"0", "1": b"1", "2": b"2"})
        self.assertEqual(window.snapshot().reuse_hits, 0)
        self.assertEqual(window.snapshot().prepare_calls, 3)


if __name__ == "__main__":
    unittest.main()
