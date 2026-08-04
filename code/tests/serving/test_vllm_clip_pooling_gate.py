from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import numpy as np


CODE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir())
SCRIPT = CODE_ROOT / "scripts" / "profiling" / "gate_vllm_clip_pooling.py"
SPEC = importlib.util.spec_from_file_location("gate_vllm_clip_pooling", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(MODULE)


class VllmClipPoolingGateTests(unittest.TestCase):
    def test_extracts_vllm_025_pooling_shape(self) -> None:
        results = [SimpleNamespace(outputs=SimpleNamespace(embedding=[1.0, 2.0, 3.0]))]

        embedding = MODULE.extract_embedding(results)

        np.testing.assert_allclose(embedding, np.asarray([1.0, 2.0, 3.0], dtype=np.float32))

    def test_extracts_older_sequence_wrapped_pooling_shape(self) -> None:
        results = [SimpleNamespace(outputs=[SimpleNamespace(embedding=[4.0, 5.0])])]

        embedding = MODULE.extract_embedding(results)

        np.testing.assert_allclose(embedding, np.asarray([4.0, 5.0], dtype=np.float32))

    def test_rejects_empty_result_list(self) -> None:
        with self.assertRaisesRegex(ValueError, "expected one embedding result"):
            MODULE.extract_embedding([])

    def test_resolve_image_fails_closed_when_glob_is_empty(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            pattern = str(Path(directory) / "*.jpg")
            with self.assertRaisesRegex(FileNotFoundError, "matched no images"):
                MODULE.resolve_image_path("", pattern)


if __name__ == "__main__":
    unittest.main()
