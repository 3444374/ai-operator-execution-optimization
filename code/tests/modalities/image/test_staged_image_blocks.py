from __future__ import annotations

import unittest

import numpy as np

from src.modalities.image.staged import (
    build_encoded_image_block_descriptor,
    build_prepared_image_block_descriptor,
)


class StagedImageBlockTest(unittest.TestCase):
    def test_reserves_exact_contiguous_fp32_ready_bytes(self) -> None:
        descriptor = build_encoded_image_block_descriptor(
            job_id="job-a",
            ordered_sequence=0,
            row_ids=("a", "b"),
            encoded_images=[b"jpeg-a", b"jpeg-b"],
            model_revision="model",
            processor_revision="processor",
            model_dtype="float16",
            created_at_s=1.0,
            input_size=2,
            embedding_dimension=4,
        )

        self.assertEqual(descriptor.physical_bytes, 12)
        self.assertEqual(descriptor.ready_bytes_estimate, 2 * 3 * 2 * 2 * 4)
        self.assertEqual(descriptor.work.for_stage("model").units, 8)

        payload = np.zeros((2, 3, 2, 2), dtype=np.float32)
        prepared = build_prepared_image_block_descriptor(
            descriptor,
            payload,
            ready_at_s=2.0,
        )
        self.assertEqual(prepared.physical_bytes, payload.nbytes)
        self.assertEqual(prepared.shape, payload.shape)
        self.assertEqual(prepared.block_id, descriptor.block_id)

    def test_digest_and_block_id_depend_on_content(self) -> None:
        common = dict(
            job_id="job-a",
            ordered_sequence=0,
            row_ids=("a",),
            model_revision="model",
            processor_revision="processor",
            model_dtype="float16",
            created_at_s=1.0,
        )
        first = build_encoded_image_block_descriptor(encoded_images=[b"a"], **common)
        second = build_encoded_image_block_descriptor(encoded_images=[b"b"], **common)

        self.assertNotEqual(first.content_digest, second.content_digest)
        self.assertNotEqual(first.block_id, second.block_id)

    def test_rejects_non_contiguous_prepared_payload(self) -> None:
        descriptor = build_encoded_image_block_descriptor(
            job_id="job-a",
            ordered_sequence=0,
            row_ids=("a",),
            encoded_images=[b"jpeg"],
            model_revision="model",
            processor_revision="processor",
            model_dtype="float16",
            created_at_s=1.0,
            input_size=2,
        )
        payload = np.zeros((1, 3, 2, 2), dtype=np.float32)[:, :, :, ::-1]

        with self.assertRaisesRegex(ValueError, "C-contiguous"):
            build_prepared_image_block_descriptor(
                descriptor,
                payload,
                ready_at_s=2.0,
            )

    def test_rejects_shape_that_disagrees_with_calibrated_input(self) -> None:
        descriptor = build_encoded_image_block_descriptor(
            job_id="job-a",
            ordered_sequence=0,
            row_ids=("a",),
            encoded_images=[b"jpeg"],
            model_revision="model",
            processor_revision="processor",
            model_dtype="float16",
            created_at_s=1.0,
            input_size=2,
        )

        with self.assertRaisesRegex(ValueError, "calibrated model work"):
            build_prepared_image_block_descriptor(
                descriptor,
                np.zeros((1, 3, 1, 1), dtype=np.float32),
                ready_at_s=2.0,
            )


if __name__ == "__main__":
    unittest.main()
