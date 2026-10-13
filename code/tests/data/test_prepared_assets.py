from __future__ import annotations

import unittest

from src.data.materializers.prepared_assets import (
    AssetUse,
    PreparedAssetLease,
    PreparedAssetWindow,
    PreparedSourceMetadata,
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

    def test_source_manifest_metadata_is_optional_and_must_match_version(self):
        from src.planning.work import StageWork, WorkDescriptor
        key = prepared_asset_key(b"image", "fixed-transform")
        work = WorkDescriptor((StageWork("source", 5, "encoded_bytes"),
                               StageWork("prepare", 48, "rgb_bytes"),
                               StageWork("model", 1, "complete_requests")),
                              "model", "header+request-count-only", locality_key="source-object",
                              lower_primary_units=1, upper_primary_units=1)
        source = PreparedSourceMetadata("coco:one.jpg", key.content_digest, work,
                                        "immutable-manifest+JPEG-header")
        self.assertEqual(AssetUse("one", key, source=source).source, source)
        self.assertIs(AssetUse("one", key, source=source).source.work, work)
        self.assertEqual((source.work.locality_key,source.work.lower_primary_units,
                          source.work.upper_primary_units), ("source-object",1,1))
        self.assertIsNone(AssetUse("plain", key).source)
        with self.assertRaisesRegex(ValueError, "version"):
            AssetUse("changed", prepared_asset_key(b"changed", "fixed-transform"), source=source)
        for changed in ({"work": None}, {"provenance": ""},
                        {"content_version": "unversioned"}):
            with self.subTest(changed=changed), self.assertRaises((TypeError,ValueError)):
                PreparedSourceMetadata(**{**source.__dict__, **changed})

    def test_actual_stage_output_gates_and_prunes_future_demand(self):
        key = prepared_asset_key(b"image", "fixed-transform")
        uses = (AssetUse("filter", key), AssetUse("map", key, ready_after="filter-return"))
        for needed in (True, False):
            with self.subTest(needed=needed):
                window = PreparedAssetWindow(uses, max_consumers=2, host_bytes=5)
                with self.assertRaisesRegex(ValueError, "not ready"):
                    window.acquire("map", prepare=lambda: self.fail("speculative map prepare"),
                                   prepared_bytes_upper_bound=5)
                lease = self.acquire(window, "filter")
                window.release(lease)
                self.assertEqual(window.snapshot().prepared_bytes, 5)
                window.resolve_stage("filter-return", ("map",) if needed else ())
                if needed:
                    lease = self.acquire(window, "map", payload=b"not-reprepared")
                    window.release(lease)
                    self.assertEqual(window.snapshot().reuse_hits, 1)
                else:
                    self.assertEqual(window.snapshot().pending_consumers, 0)
                self.assertEqual(window.snapshot().prepared_bytes, 0)
                self.assertEqual(window.snapshot().prepare_calls, 1)
                with self.assertRaisesRegex(ValueError, "resolved"):
                    window.resolve_stage("filter-return", ())

    def test_existing_database_image_work_is_referenced_without_recalibration(self):
        from src.execution_provider.semantic_image import SemanticImagePlan
        from src.modalities.image.contracts import build_image_work_descriptor
        plan = SemanticImagePlan("openai/clip-vit-base-patch32", "a" * 40,
                                 "openai/clip-vit-base-patch32", "a" * 40,
                                 "float32", 512, 224)
        work = build_image_work_descriptor(row_count=1, encoded_bytes=5,
            model_revision=plan.model_revision, processor_revision=plan.processor_revision,
            dtype=plan.dtype, input_size=plan.input_size, embedding_dimension=plan.dimension)
        key = prepared_asset_key(b"image", plan.processor_revision + ":rgb-fp32-224")
        source = PreparedSourceMetadata("selected-pg-image", key.content_digest, work,
                                        "producer-fixture; no database wire change")
        use = AssetUse("image-prepare", key, source=source)
        self.assertIs(use.source.work, work)
        self.assertEqual(use.source.work.primary.unit, "pixels")
        self.assertEqual(use.source.work.for_stage("source").units, 5)
        self.assertEqual(use.source.work.calibration_signature, work.calibration_signature)
        self.assertEqual(plan.digest, SemanticImagePlan.from_record(plan.to_record()).digest)

    def test_invalid_stage_receipt_is_atomic_and_does_not_enable_work(self):
        key = prepared_asset_key(b"image", "fixed-transform")
        window = PreparedAssetWindow((AssetUse("a", key, ready_after="first"),
                                      AssetUse("b", key, ready_after="second")),
                                     max_consumers=2, host_bytes=5)
        for required in (["a"], ("a", "a"), ("b",), (None,)):
            with self.subTest(required=required), self.assertRaises(ValueError):
                window.resolve_stage("first", required)
            self.assertEqual(window.snapshot().pending_consumers, 2)
            with self.assertRaisesRegex(ValueError, "not ready"):
                self.acquire(window, "a")
        window.resolve_stage("first", ())
        window.resolve_stage("second", ())
        self.assertEqual(window.snapshot().pending_consumers, 0)

    def test_unresolved_or_pruned_demand_never_releases_an_active_reader(self):
        key = prepared_asset_key(b"image", "fixed-transform")
        window = PreparedAssetWindow((AssetUse("active", key),
                                      AssetUse("conditional", key, ready_after="predicate")),
                                     max_consumers=2, host_bytes=5)
        lease = self.acquire(window, "active")
        window.resolve_stage("predicate", ())
        self.assertEqual(window.snapshot().prepared_bytes, 5)
        self.assertEqual(window.snapshot().held_consumers, 1)
        window.release(lease)
        self.assertEqual(window.snapshot().prepared_bytes, 0)

    def test_input_copy_read_end_does_not_change_native_call_or_result_lifetime(self):
        from src.baselines.text.frameworks.native_calls import NativeCall, NativeCallOutcome, _call_wire
        from src.execution_provider.adapters.full_response import FullModelResponse
        window = self.window(b"image")
        lease = self.acquire(window, "0")
        call = NativeCall("same-call", 0, lease.payload.decode().encode(), "http://localhost/v1",
                          (), 5)
        wire = _call_wire(call)
        # A separate immutable copy was exported. Ending this read conveys no
        # model receipt; the complete request and later result still have owners.
        window.release(lease)
        self.assertEqual(window.snapshot().prepared_bytes, 0)
        self.assertEqual(_call_wire(call), wire)
        outcome = NativeCallOutcome(call.call_id, call.row_id, FullModelResponse(200, (), b"answer"))
        self.assertEqual(outcome.response.body, b"answer")
        self.assertEqual(_call_wire(call), wire)

    def test_resident_byte_seconds_counts_one_shared_buffer_and_real_read_end(self):
        now = [0.0]
        key = prepared_asset_key(b"image", "fixed-transform")
        window = PreparedAssetWindow((AssetUse("a", key), AssetUse("b", key)),
                                     max_consumers=2, host_bytes=5, clock=lambda: now[0])
        first = self.acquire(window, "a")
        second = self.acquire(window, "b")
        now[0] = 2.0
        window.release(first)
        self.assertEqual(window.snapshot().resident_byte_seconds, 10.0)
        now[0] = 3.0
        window.release(second)
        now[0] = 9.0
        self.assertEqual(window.snapshot().resident_byte_seconds, 15.0)
        self.assertIsNone(self.window(b"image").snapshot().resident_byte_seconds)

    def test_residency_clock_does_not_assign_preparation_time_to_new_output(self):
        now = [0.0]
        use = AssetUse("a", prepared_asset_key(b"image", "fixed-transform"))
        window = PreparedAssetWindow((use,), max_consumers=1, host_bytes=5, clock=lambda: now[0])
        def prepare():
            now[0] = 10.0
            return b"ready"
        lease = window.acquire("a", prepare=prepare, prepared_bytes_upper_bound=5)
        now[0] = 12.0
        window.release(lease)
        self.assertEqual(window.snapshot().resident_byte_seconds, 10.0)
        now[0] = 11.0
        with self.assertRaisesRegex(ValueError, "monotonic"):
            window.snapshot()


if __name__ == "__main__":
    unittest.main()
