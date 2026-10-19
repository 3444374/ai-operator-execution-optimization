"""Portable artifact semantics used by native queries and historical consumers."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from src.baselines.common.private_artifacts import file_identity, json_metric, write_private_json


class ArtifactValueTests(unittest.TestCase):
    def test_upstream_infinite_errors_survive_nested_strict_json(self):
        values = {"errors": (float("inf"), float("-inf"), float("nan")),
                  "rows": [{"finite": 1.5, "missing": None, "valid": True}],
                  "text": "Infinity"}
        normalized = json_metric(values)
        self.assertEqual(json.dumps(normalized, allow_nan=False),
                         '{"errors": ["Infinity", "-Infinity", "NaN"], "rows": '
                         '[{"finite": 1.5, "missing": null, "valid": true}], "text": "Infinity"}')
        self.assertIsInstance(values["errors"], tuple)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "quality.json"
            write_private_json(path, normalized)
            self.assertEqual(json.loads(path.read_text()), normalized)
            with self.assertRaises(ValueError):
                write_private_json(Path(directory) / "raw.json", values)

    def test_file_identity_preserves_empty_binary_and_chunk_crossing_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.bin"
            for payload in (b"", bytes(range(256)), b"x" * (1048576 + 7) + b"\x00\xff"):
                with self.subTest(size=len(payload)):
                    path.write_bytes(payload)
                    expected = {"sha256": hashlib.sha256(payload).hexdigest(), "bytes": len(payload)}
                    self.assertEqual(file_identity(path), expected)
                    self.assertEqual(file_identity(str(path)), expected)
            with self.assertRaises(FileNotFoundError):
                file_identity(Path(directory) / "missing")

    def test_legacy_imports_are_the_same_function_without_wrappers(self):
        from src.experiments.postgresql.query_evaluation import json_metric as old_json_metric
        from src.experiments.postgresql.query_workloads import file_identity as old_file_identity
        self.assertIs(old_json_metric, json_metric)
        self.assertIs(old_file_identity, file_identity)

    def test_neutral_import_does_not_load_pg_or_execution_libraries(self):
        code_root = Path(__file__).resolve().parents[3]
        script = (
            "import sys; from src.baselines.common.private_artifacts import file_identity, json_metric; "
            "assert not any(name.startswith(('src.experiments.postgresql', 'psycopg', 'ray', 'daft')) "
            "for name in sys.modules)"
        )
        result = subprocess.run([sys.executable, "-B", "-c", script],
                                env={**os.environ, "PYTHONPATH": str(code_root)},
                                text=True, capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
