import math
import struct
import unittest
from dataclasses import replace

from src.execution_provider.semantic_image import MAX_IMAGE_BYTES, SemanticImagePlan


class SemanticImageTest(unittest.TestCase):
    def setUp(self):
        self.plan = SemanticImagePlan("clip", "a" * 40, "processor", "b" * 40,
                                      "float32", 2, 224)

    def test_all_semantic_choices_change_identity(self):
        changes = dict(model_id="other", model_revision="c" * 40, processor_id="other",
                       processor_revision="d" * 40, dtype="float16", dimension=3, input_size=32)
        for name, value in changes.items():
            self.assertNotEqual(self.plan.digest, replace(self.plan, **{name: value}).digest)
        self.assertEqual(SemanticImagePlan.from_record(self.plan.to_record()), self.plan)

    def test_input_is_binary_and_strictly_bounded(self):
        self.plan.validate_input(b"\0\xff")
        for value in (None, b"", "abc", bytearray(b"a"), b"a" * (MAX_IMAGE_BYTES + 1)):
            with self.assertRaises(ValueError):
                self.plan.validate_input(value)
        self.assertNotEqual(self.plan.payload_digest(b"a"), self.plan.payload_digest(b"b"))

    def test_float4_roundtrip_and_invalid_vectors(self):
        self.assertEqual(self.plan.encode_result([0.1, -2]), struct.pack("!ff", 0.1, -2))
        for values in ([0], [0, 1, 2], [None, 1], [True, 0], [math.inf, 0], [math.nan, 0],
                       [1e100, 0], ["1", 0], [[1], [0]]):
            with self.assertRaises(ValueError):
                self.plan.encode_result(values)
        with self.assertRaises(ValueError):
            self.plan.validate_result(struct.pack("!ff", math.nan, 0))

    def test_revisions_and_types_are_not_coerced(self):
        for name, value in (("model_revision", "main"), ("dimension", True),
                            ("input_size", 1025), ("dtype", "fp16"), ("model_id", "x\0y")):
            with self.assertRaises(ValueError):
                replace(self.plan, **{name: value})
        with self.assertRaises(ValueError):
            SemanticImagePlan.from_record({**self.plan.to_record(), "normalize": False})
