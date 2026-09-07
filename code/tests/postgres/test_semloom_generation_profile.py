"""Choice-profile value contracts; SQL/schema-3/wire-v4 integration is separate."""

from __future__ import annotations

import unittest
from dataclasses import FrozenInstanceError
import ctypes
import hashlib
from pathlib import Path
import subprocess
import tempfile

from src.execution_provider.generation_profile import GenerationProfile


# Worked C.2 encoding: NUL-terminated domain, uint32-BE byte lengths/version/count.
# Independently hashed with `xxd -r -p | openssl dgst -sha256`, before implementation.
CANONICAL = bytes.fromhex(
    "73656d6c6f6f6d2d67656e65726174696f6e2d70726f66696c652d763100"
    "00000022"
    "73656d6c6f6f6d2e67656e65726174696f6e2e63686f6963652e7472697374617465"
    "00000001"
    "0000000643484f494345"
    "00000003"
    "0000000454525545"
    "0000000546414c5345"
    "00000007554e4b4e4f574e"
)
PROFILE_DIGEST = "941327729217db0ad438a8d0c945750485c6047834229aa40912b254d90a24f7"
PROFILE_VALUES = {
    "profile_id": "semloom.generation.choice.tristate",
    "profile_version": 1,
    "constraint_kind": "CHOICE",
    "choices": ("TRUE", "FALSE", "UNKNOWN"),
}


class GenerationProfileTests(unittest.TestCase):
    def test_tristate_profile_matches_independent_canonical_vector(self) -> None:
        profile = GenerationProfile(
            profile_id="semloom.generation.choice.tristate",
            profile_version=1,
            constraint_kind="CHOICE",
            choices=("TRUE", "FALSE", "UNKNOWN"),
        )

        self.assertEqual(profile.canonical_bytes(), CANONICAL)
        self.assertEqual(profile.digest, PROFILE_DIGEST)

    def test_only_the_exact_typed_ordered_profile_is_supported(self) -> None:
        changes = {
            "profile_id": (None, 1, "", "other", "semloom.generation.choice.tristate\0"),
            "profile_version": (None, True, 1.0, "1", 0, 2, 2**32),
            "constraint_kind": (None, "choice", "JSON", 1),
            "choices": (
                None, ["TRUE", "FALSE", "UNKNOWN"], (), ("TRUE", "FALSE"),
                ("FALSE", "TRUE", "UNKNOWN"), ("TRUE", "FALSE", "UNKNOWN", "OTHER"),
                ("TRUE", "TRUE", "UNKNOWN"), ("TRUE", "FALSE", None),
                ("TRUE", "FALSE", "UNKNOWN\0"), ("TRUE", "FALSE", "未知"),
                ("TRUE", "FALSE", "\ud800"), ("TRUE", "FALSE", b"UNKNOWN"),
            ),
        }
        for field, values in changes.items():
            for value in values:
                with self.subTest(field=field, value=value):
                    with self.assertRaisesRegex(ValueError, "^invalid generation profile$"):
                        GenerationProfile(**{**PROFILE_VALUES, field: value})

    def test_profile_cannot_change_after_identity_is_computed(self) -> None:
        profile = GenerationProfile(**PROFILE_VALUES)
        with self.assertRaises(FrozenInstanceError):
            profile.profile_version = 2
        self.assertEqual(profile.digest, PROFILE_DIGEST)

    def test_record_is_self_contained_and_rejects_unbound_or_changed_fields(self) -> None:
        expected = {**PROFILE_VALUES, "choices": ["TRUE", "FALSE", "UNKNOWN"],
                    "profile_digest": PROFILE_DIGEST}
        profile = GenerationProfile(**PROFILE_VALUES)
        record = profile.to_record()
        self.assertEqual(record, expected)
        restored = GenerationProfile.from_record(record)
        self.assertEqual(restored, profile)
        record["choices"][0] = "FALSE"
        self.assertEqual(restored.choices, ("TRUE", "FALSE", "UNKNOWN"))
        self.assertEqual(profile.to_record(), expected)

        malformed = [None, [], record, {**expected, "extra": 1},
                     {**expected, "profile_digest": "0" * 64},
                     {**expected, "profile_digest": PROFILE_DIGEST.upper()},
                     {**expected, "profile_digest": None},
                     {**expected, "choices": tuple(expected["choices"])}]
        malformed.extend({key: value for key, value in expected.items() if key != missing}
                         for missing in expected)
        for value in malformed:
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "^invalid generation profile$"):
                    GenerationProfile.from_record(value)


class ByteSlice(ctypes.Structure):
    _fields_ = [("data", ctypes.c_void_p), ("length", ctypes.c_uint32)]


class CGenerationProfile(ctypes.Structure):
    _fields_ = [
        ("profile_id", ByteSlice), ("profile_version", ctypes.c_uint32),
        ("constraint_kind", ctypes.c_uint32), ("choice_count", ctypes.c_uint32),
        ("choices", ByteSlice * 3),
    ]


class CGenerationProfileTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        source = Path(__file__).resolve().parents[2] / "postgres/semloom_pg/src"
        directory = tempfile.TemporaryDirectory(prefix="semloom-profile-test-")
        cls.addClassCleanup(directory.cleanup)
        library_path = Path(directory.name) / "profile.so"
        subprocess.run([
            "cc", "-std=c11", "-Wall", "-Wextra", "-Werror", "-pedantic",
            "-shared", "-fPIC", "-I", str(source), str(source / "semantics/generation_profile.c"),
            "-o", str(library_path),
        ], check=True, capture_output=True, text=True)
        cls.library = ctypes.CDLL(str(library_path))
        cls.library.semloom_generation_profile_tristate.restype = ctypes.POINTER(CGenerationProfile)
        cls.library.semloom_generation_profile_encode.argtypes = [
            ctypes.POINTER(CGenerationProfile), ctypes.POINTER(ctypes.c_uint8),
            ctypes.c_uint32, ctypes.POINTER(ctypes.c_uint32),
        ]
        cls.library.semloom_generation_profile_encode.restype = ctypes.c_bool

    def test_c_canonical_bytes_and_identity_match_python_and_independent_vector(self) -> None:
        profile = self.library.semloom_generation_profile_tristate()
        output = (ctypes.c_uint8 * len(CANONICAL))()
        written = ctypes.c_uint32()
        self.assertTrue(self.library.semloom_generation_profile_encode(
            profile, output, len(output), ctypes.byref(written)))
        self.assertEqual(written.value, len(CANONICAL))
        self.assertEqual(bytes(output), CANONICAL)
        self.assertEqual(bytes(output), GenerationProfile(**PROFILE_VALUES).canonical_bytes())
        self.assertEqual(hashlib.sha256(bytes(output)).hexdigest(), PROFILE_DIGEST)

    def assert_rejected_without_writing(self, profile: CGenerationProfile | None) -> None:
        output = (ctypes.c_uint8 * len(CANONICAL))(*([0xA5] * len(CANONICAL)))
        written = ctypes.c_uint32(123)
        pointer = ctypes.byref(profile) if profile is not None else None
        self.assertFalse(self.library.semloom_generation_profile_encode(
            pointer, output, len(output), ctypes.byref(written)))
        self.assertEqual(written.value, 0)
        self.assertEqual(bytes(output), b"\xa5" * len(CANONICAL))

    def profile_copy(self) -> CGenerationProfile:
        return CGenerationProfile.from_buffer_copy(
            self.library.semloom_generation_profile_tristate().contents)

    def test_c_rejects_unknown_identity_kind_count_and_malformed_slices(self) -> None:
        self.assert_rejected_without_writing(None)
        for field, values in {
            "profile_version": (0, 2, 2**32 - 1),
            "constraint_kind": (0, 2, 2**32 - 1),
            "choice_count": (0, 2, 4, 2**32 - 1),
        }.items():
            for value in values:
                with self.subTest(field=field, value=value):
                    profile = self.profile_copy()
                    setattr(profile, field, value)
                    self.assert_rejected_without_writing(profile)
        for index in range(-1, 3):
            for length in (0, 1, 2**32 - 1):
                profile = self.profile_copy()
                value = profile.profile_id if index == -1 else profile.choices[index]
                value.length = length
                self.assert_rejected_without_writing(profile)
            profile = self.profile_copy()
            value = profile.profile_id if index == -1 else profile.choices[index]
            value.data = None
            self.assert_rejected_without_writing(profile)
        profile = self.profile_copy()
        profile.choices[0] = profile.choices[1]
        self.assert_rejected_without_writing(profile)

    def test_c_binds_every_profile_byte_and_never_requires_a_terminator(self) -> None:
        values = [b"semloom.generation.choice.tristate", b"TRUE", b"FALSE", b"UNKNOWN"]
        profile = self.profile_copy()
        buffers = [ctypes.create_string_buffer(value, len(value)) for value in values]
        for index, buffer in enumerate(buffers):
            target = profile.profile_id if index == 0 else profile.choices[index-1]
            target.data = ctypes.addressof(buffer)
        output = (ctypes.c_uint8 * len(CANONICAL))()
        written = ctypes.c_uint32()
        self.assertTrue(self.library.semloom_generation_profile_encode(
            ctypes.byref(profile), output, len(output), ctypes.byref(written)))
        self.assertEqual(bytes(output), CANONICAL)

        for index, original in enumerate(values):
            for offset in range(len(original)):
                with self.subTest(field=index, byte=offset):
                    changed = bytearray(original)
                    changed[offset] ^= 0x80
                    replacement = ctypes.create_string_buffer(bytes(changed), len(changed))
                    bad = CGenerationProfile.from_buffer_copy(profile)
                    target = bad.profile_id if index == 0 else bad.choices[index-1]
                    target.data = ctypes.addressof(replacement)
                    self.assert_rejected_without_writing(bad)

    def test_c_encoder_is_bounded_and_failure_never_publishes_partial_bytes(self) -> None:
        profile = self.library.semloom_generation_profile_tristate()
        output = (ctypes.c_uint8 * (len(CANONICAL)+2))(*([0xA5] * (len(CANONICAL)+2)))
        target = ctypes.cast(ctypes.byref(output, 1), ctypes.POINTER(ctypes.c_uint8))
        written = ctypes.c_uint32(123)
        for capacity in range(len(CANONICAL)):
            self.assertFalse(self.library.semloom_generation_profile_encode(
                profile, target, capacity, ctypes.byref(written)))
            self.assertEqual(written.value, 0)
            self.assertEqual(bytes(output), b"\xa5" * len(output))
        self.assertFalse(self.library.semloom_generation_profile_encode(
            profile, None, len(CANONICAL), ctypes.byref(written)))
        self.assertFalse(self.library.semloom_generation_profile_encode(
            profile, target, len(CANONICAL), None))
        self.assertEqual(bytes(output), b"\xa5" * len(output))
        self.assertTrue(self.library.semloom_generation_profile_encode(
            profile, target, len(CANONICAL), ctypes.byref(written)))
        self.assertEqual(bytes(output), b"\xa5" + CANONICAL + b"\xa5")
        self.assertEqual(written.value, len(CANONICAL))


if __name__ == "__main__":
    unittest.main()
