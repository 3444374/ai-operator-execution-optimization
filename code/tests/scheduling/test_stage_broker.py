from __future__ import annotations

import unittest
from dataclasses import replace

from src.planning.blocks import StageBlockDescriptor
from src.planning.work import StageWork, WorkDescriptor
from src.scheduling.runtime.stage_broker import BoundedStageBroker, StageBrokerLimits


def _descriptor(
    sequence: int,
    *,
    encoded_bytes: int = 10,
    ready_bytes: int = 40,
    job_id: str = "job-a",
) -> StageBlockDescriptor:
    work = WorkDescriptor(
        stages=(
            StageWork("prepare", 12, "tensor_values"),
            StageWork("model", 4, "pixels"),
        ),
        primary_stage="model",
        calibration_signature="sig",
    )
    return StageBlockDescriptor(
        block_id=f"block-{sequence}",
        job_id=job_id,
        ordered_sequence=sequence,
        row_ids=(f"row-{sequence}",),
        representation="encoded",
        shape=(1,),
        layout="variable_binary",
        dtype="uint8_bytes",
        logical_bytes=encoded_bytes,
        physical_bytes=encoded_bytes,
        ready_bytes_estimate=ready_bytes,
        content_digest=f"digest-{sequence}",
        transform_signature="transform",
        model_signature="model",
        work=work,
        created_at_s=float(sequence),
    )


class BoundedStageBrokerTest(unittest.TestCase):
    def test_extended_tail_is_charged_once_and_leased_with_all_rows(self):
        original = _descriptor(0)
        extended = replace(original, row_ids=('row-0', 'row-new'), shape=(2,),
                           logical_bytes=20, physical_bytes=20, ready_bytes_estimate=80,
                           content_digest='extended', work=replace(original.work,
                               stages=(StageWork('prepare', 24, 'tensor_values'),
                                       StageWork('model', 8, 'pixels'))))
        self.broker.enqueue_encoded(original)
        self.broker.extend_encoded(extended)
        self.assertEqual(self.broker.snapshot().encoded_held_bytes, 20)
        self.assertEqual(self.broker.snapshot().encoded_queued, 1)
        lease = self.broker.lease_prepare(now_s=1)
        self.assertEqual(lease.descriptor, extended)
        self.assertEqual(self.broker.snapshot().ready_held_work, 8)
        self.broker.complete_prepare(lease.lease_id, replace(extended, representation='ready'), now_s=2)
        model = self.broker.lease_model(now_s=3)
        self.broker.complete_model(model.lease_id, output_row_ids=extended.row_ids)
        self.broker.release_terminal(extended.block_id)
        self.assertTrue(self.broker.is_drained())
        self.assertEqual(self.broker.snapshot().ready_held_bytes, 0)

    def test_invalid_extension_preserves_queue_bytes_and_original_lease(self):
        original = _descriptor(0)
        extended = replace(original, row_ids=('row-0', 'row-new'), shape=(2,),
                           logical_bytes=20, physical_bytes=20, content_digest='extended')
        invalid = (
            replace(extended, row_ids=('different', 'row-new')),
            replace(extended, job_id='other'),
            replace(extended, model_signature='other'),
            replace(extended, physical_bytes=31),
            replace(extended, ready_bytes_estimate=81),
            replace(extended, work=replace(original.work,
                stages=(StageWork('prepare', 12, 'tensor_values'), StageWork('model', 9, 'pixels')))),
            replace(extended, logical_bytes=9),
        )
        for descriptor in invalid:
            with self.subTest(descriptor=descriptor):
                broker = BoundedStageBroker(self.broker.limits)
                broker.enqueue_encoded(original)
                before = broker.snapshot()
                with self.assertRaises((ValueError, BufferError)):
                    broker.extend_encoded(descriptor)
                self.assertEqual(broker.snapshot(), before)
                self.assertEqual(broker.lease_prepare(now_s=1).descriptor, original)

    def test_extension_cannot_change_a_leased_or_non_tail_block(self):
        original = _descriptor(0)
        extended = replace(original, row_ids=('row-0', 'row-new'), shape=(2,),
                           logical_bytes=20, physical_bytes=20, content_digest='extended')
        self.broker.enqueue_encoded(original)
        self.broker.enqueue_encoded(_descriptor(1))
        before = self.broker.snapshot()
        with self.assertRaisesRegex(ValueError, 'tail'):
            self.broker.extend_encoded(extended)
        self.assertEqual(self.broker.snapshot(), before)
        lease = self.broker.lease_prepare(now_s=2)
        before = self.broker.snapshot()
        with self.assertRaisesRegex(ValueError, 'unleased'):
            self.broker.extend_encoded(extended)
        self.assertEqual(self.broker.snapshot(), before)
        self.assertEqual(lease.descriptor, original)

    def test_extension_rejects_rows_owned_by_another_block(self):
        self.broker.enqueue_encoded(_descriptor(1))
        original = _descriptor(0)
        self.broker.enqueue_encoded(original)
        extended = replace(original, row_ids=('row-0', 'row-1'), shape=(2,),
                           logical_bytes=20, physical_bytes=20, content_digest='extended')
        before = self.broker.snapshot()
        with self.assertRaisesRegex(ValueError, 'already admitted'):
            self.broker.extend_encoded(extended)
        self.assertEqual(self.broker.snapshot(), before)

    def test_explicit_ready_block_preserves_fifo_for_the_remaining_blocks(self):
        for i in range(2):
            descriptor = _descriptor(i)
            self.broker.enqueue_encoded(descriptor)
            lease = self.broker.lease_prepare(now_s=2)
            self.broker.complete_prepare(lease.lease_id,
                replace(descriptor, representation='ready'), now_s=2)
        model = self.broker.lease_model(now_s=3, block_id='block-1')
        self.assertEqual(model.descriptor.block_id, 'block-1')
        self.broker.complete_model(model.lease_id, output_row_ids=('row-1',))
        self.assertEqual(self.broker.lease_model(now_s=3).descriptor.block_id, 'block-0')

    def test_explicit_model_selection_rejects_unprepared_or_other_job_block(self):
        descriptor = _descriptor(0)
        self.broker.enqueue_encoded(descriptor)
        with self.assertRaisesRegex(ValueError, 'not ready'):
            self.broker.lease_model(now_s=1, block_id=descriptor.block_id)
        prepare = self.broker.lease_prepare(now_s=1)
        self.broker.complete_prepare(prepare.lease_id,
            replace(descriptor, representation='ready'), now_s=1)
        with self.assertRaisesRegex(ValueError, 'this Job'):
            self.broker.lease_model(now_s=1, block_id=descriptor.block_id, preferred_job_id='other')
        self.assertEqual(self.broker.snapshot().ready_queued, 1)

    def setUp(self) -> None:
        self.broker = BoundedStageBroker(
            StageBrokerLimits(
                encoded_bytes=30,
                ready_bytes=80,
                ready_work=8,
                prepare_inflight=2,
                model_inflight=1,
            )
        )

    def test_real_ready_lifecycle_preserves_identity_and_releases_capacity(self) -> None:
        first = _descriptor(0)
        self.broker.enqueue_encoded(first)

        prepare = self.broker.lease_prepare(now_s=1.0)
        self.assertIsNotNone(prepare)
        self.assertEqual(self.broker.state_of(first.block_id), "preparing")
        self.assertIsNone(self.broker.lease_model(now_s=1.1))
        reserved = self.broker.snapshot()
        self.assertEqual(reserved.ready_held_bytes, 40)
        self.assertEqual(reserved.ready_queued, 0)

        prepared = replace(
            first,
            representation="prepared_fp32_nchw",
            shape=(1, 3, 2, 2),
            layout="NCHW",
            dtype="float32",
            logical_bytes=36,
            physical_bytes=36,
            ready_at_s=1.2,
        )
        self.broker.complete_prepare(prepare.lease_id, prepared, now_s=1.2)
        ready = self.broker.snapshot()
        self.assertEqual(ready.ready_queued, 1)
        self.assertEqual(ready.ready_held_bytes, 36)
        self.assertEqual(ready.encoded_held_bytes, 0)

        model = self.broker.lease_model(now_s=1.3)
        self.assertIsNotNone(model)
        self.broker.complete_model(model.lease_id, output_row_ids=first.row_ids)
        drained = self.broker.snapshot()
        self.assertEqual(drained.completed, 1)
        self.assertEqual(drained.ready_held_bytes, 0)
        self.assertTrue(self.broker.is_drained())

    def test_prepare_reserves_ready_bytes_before_work_completes(self) -> None:
        self.broker.enqueue_encoded(_descriptor(0, ready_bytes=50))
        self.broker.enqueue_encoded(_descriptor(1, ready_bytes=50))

        self.assertIsNotNone(self.broker.lease_prepare(now_s=2.0))
        self.assertIsNone(self.broker.lease_prepare(now_s=2.0))
        snapshot = self.broker.snapshot()
        self.assertEqual(snapshot.prepare_inflight, 1)
        self.assertLessEqual(snapshot.ready_held_bytes, snapshot.ready_bytes_limit)

    def test_invalid_prepare_completion_does_not_destroy_lease(self) -> None:
        descriptor = _descriptor(0)
        self.broker.enqueue_encoded(descriptor)
        lease = self.broker.lease_prepare(now_s=1.0)
        invalid = replace(
            descriptor,
            row_ids=("different",),
            representation="prepared_fp32_nchw",
        )

        with self.assertRaisesRegex(ValueError, "identity"):
            self.broker.complete_prepare(lease.lease_id, invalid, now_s=1.1)

        self.broker.fail_prepare(lease.lease_id, requeue=False)
        self.assertEqual(self.broker.snapshot().failed, 1)

    def test_model_completion_is_exactly_once_and_order_checked(self) -> None:
        descriptor = _descriptor(0)
        self.broker.enqueue_encoded(descriptor)
        prepare = self.broker.lease_prepare(now_s=1.0)
        prepared = replace(
            descriptor,
            representation="prepared_fp32_nchw",
            shape=(1, 3, 2, 2),
            layout="NCHW",
            dtype="float32",
            physical_bytes=36,
            ready_at_s=1.1,
        )
        self.broker.complete_prepare(prepare.lease_id, prepared, now_s=1.1)
        model = self.broker.lease_model(now_s=1.2)

        with self.assertRaisesRegex(ValueError, "row order"):
            self.broker.complete_model(model.lease_id, output_row_ids=("wrong",))

        self.broker.complete_model(model.lease_id, output_row_ids=descriptor.row_ids)
        with self.assertRaisesRegex(KeyError, "unknown model lease"):
            self.broker.complete_model(model.lease_id, output_row_ids=descriptor.row_ids)

    def test_runtime_snapshot_uses_true_stage_queues(self) -> None:
        self.broker.enqueue_encoded(_descriptor(0))
        self.broker.enqueue_encoded(_descriptor(1))
        lease = self.broker.lease_prepare(now_s=2.0)
        snapshot = self.broker.runtime_snapshot(observed_at_s=3.0)

        self.assertEqual(snapshot.for_stage("prepare").active_work, 12)
        self.assertEqual(snapshot.for_stage("prepare").queued_work, 12)
        self.assertEqual(snapshot.for_stage("model").queued_work, 0)
        self.broker.fail_prepare(lease.lease_id, requeue=False)

    def test_rejects_duplicate_rows_across_completed_blocks(self) -> None:
        self.broker.enqueue_encoded(_descriptor(0))
        duplicate = replace(
            _descriptor(1),
            row_ids=("row-0",),
        )
        with self.assertRaisesRegex(ValueError, "already admitted"):
            self.broker.enqueue_encoded(duplicate)


if __name__ == "__main__":
    unittest.main()
