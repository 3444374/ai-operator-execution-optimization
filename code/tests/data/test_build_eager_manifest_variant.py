import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


SCRIPT = (
    Path(__file__).resolve().parents[2]
    / "scripts"
    / "data"
    / "build_eager_manifest_variant.py"
)
SPEC = importlib.util.spec_from_file_location("build_eager_manifest_variant", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class BuildEagerManifestVariantTest(unittest.TestCase):
    def test_only_arrival_and_order_change(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.jsonl"
            output = root / "eager.jsonl"
            audit_path = root / "audit.json"
            rows = [
                {
                    "doc_id": 2,
                    "endpoint_index": 1,
                    "prompt_tokens": 20,
                    "source_row_hash": "b",
                    "arrival_time_s": 8.0,
                },
                {
                    "doc_id": 1,
                    "endpoint_index": 0,
                    "prompt_tokens": 10,
                    "source_row_hash": "a",
                    "arrival_time_s": 1.0,
                },
            ]
            source.write_text(
                "\n".join(json.dumps(row) for row in rows) + "\n",
                encoding="utf-8",
            )

            audit = MODULE.build_eager_variant(source, output, audit_path)
            observed = [json.loads(line) for line in output.read_text().splitlines()]

            self.assertEqual([row["doc_id"] for row in observed], [1, 2])
            self.assertEqual([row["arrival_time_s"] for row in observed], [0.0, 0.0])
            self.assertEqual(audit["prompt_tokens_total"], 30)
            self.assertEqual(audit["endpoint_counts"], {"0": 1, "1": 1})
            self.assertEqual(json.loads(audit_path.read_text()), audit)


if __name__ == "__main__":
    unittest.main()
