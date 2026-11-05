"""Choice-profile value contracts; SQL/schema-3/wire-v4 integration is separate."""

from __future__ import annotations

import unittest
from dataclasses import FrozenInstanceError

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


if __name__ == "__main__":
    unittest.main()
