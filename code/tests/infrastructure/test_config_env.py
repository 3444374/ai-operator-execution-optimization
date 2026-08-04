from __future__ import annotations

import unittest

from src.infrastructure.config_env import expand_scalar, expand_structure, expand_text


class ConfigEnvironmentTests(unittest.TestCase):
    def test_expands_multiple_references(self) -> None:
        self.assertEqual(
            expand_text(
                "${ROOT}/models/${NAME}",
                "model path",
                environment={"ROOT": "/srv", "NAME": "clip"},
            ),
            "/srv/models/clip",
        )

    def test_full_scalar_reference_preserves_json_type(self) -> None:
        self.assertEqual(
            expand_scalar("${GPU_COUNT}", "gpu count", environment={"GPU_COUNT": "2"}),
            2,
        )

    def test_missing_reference_fails_closed(self) -> None:
        with self.assertRaisesRegex(ValueError, "MODEL_ROOT"):
            expand_text("${MODEL_ROOT}/qwen", "model path", environment={})

    def test_expands_nested_json_structure(self) -> None:
        self.assertEqual(
            expand_structure(
                {"urls": ["${HOST}:8000"], "count": "${COUNT}"},
                "config",
                environment={"HOST": "http://localhost", "COUNT": "2"},
            ),
            {"urls": ["http://localhost:8000"], "count": 2},
        )


if __name__ == "__main__":
    unittest.main()
