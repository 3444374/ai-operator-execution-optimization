"""Named historical trees must survive changes to maintained shared modules."""

import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from scripts.experiments.materialize_native_baseline import materialize


ROOT = Path(__file__).resolve().parents[3]
MANIFEST_DIR = ROOT / "experiments/reproduction/native_baselines"
PROJECTIONS = {
    "native_isolated_and_imlane": "8a30b42aeecaa56403ec684877450e6375900a476d09d206b71cb533671dd4f1",
    "core_original_dynamic": "9819f3ee6f2103bf407576447dc5c52be8f30ebbd787a702fd61511f97ee4e71",
    "core_local_window_pairs": "b461f3e632db13ac81453826658fa6fc1254a608fed8e24e25145844d08b61d6",
    "ray_runtime_reuse": "0fdf47e12d4d9c08cb289b1a357fb07b409a764ac99a78083177d794f8deb464",
    "daft_single_thread_session": "a32a7c4354d7c2512eb3d8870fce11f94b87cf7fafaeb76edf9b3a8d43ba3349",
}


class HistoricalProjectionTests(unittest.TestCase):
    def test_all_named_projections_materialize_with_original_ids_and_exact_bytes(self):
        manifest = json.loads((MANIFEST_DIR / "MANIFEST.json").read_text())
        self.assertEqual(set(manifest["variants"]), set(PROJECTIONS))
        with tempfile.TemporaryDirectory() as directory:
            for variant, identity in PROJECTIONS.items():
                with self.subTest(variant=variant):
                    output = Path(directory) / variant
                    report = materialize(ROOT, MANIFEST_DIR, variant, output)
                    spec = manifest["variants"][variant]
                    self.assertEqual(spec["code_projection_sha256"], identity)
                    self.assertEqual(report["code_projection_sha256"], identity)
                    self.assertEqual(report["model_calls"], 0)
                    self.assertEqual(report["files"], len(spec["code_files_sha256"]))
                    actual = {p.relative_to(output).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
                              for p in output.rglob("*") if p.is_file() and p.name != "REPRODUCTION.json"}
                    self.assertEqual(actual, spec["code_files_sha256"])

    def test_existing_output_is_rejected_without_overwriting_its_contents(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "existing"
            output.mkdir()
            marker = output / "keep.txt"
            marker.write_bytes(b"preserve")
            with self.assertRaises(FileExistsError):
                materialize(ROOT, MANIFEST_DIR, next(iter(PROJECTIONS)), output)
            self.assertEqual(list(output.iterdir()), [marker])
            self.assertEqual(marker.read_bytes(), b"preserve")

    def test_stale_source_fails_before_creating_any_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            repository, manifest_dir, output = root / "repo", root / "manifest", root / "out"
            repository.mkdir()
            manifest_dir.mkdir()
            (repository / "source.py").write_bytes(b"changed")
            spec = dict(code_files_sha256={"source.py": hashlib.sha256(b"original").hexdigest()}, overrides={})
            (manifest_dir / "MANIFEST.json").write_text(json.dumps({"variants": {"fixture": spec}}))
            with self.assertRaisesRegex(ValueError, "reproduction source differs"):
                materialize(repository, manifest_dir, "fixture", output)
            self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
