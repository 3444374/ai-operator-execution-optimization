"""Bounded JPEG source/content identities for the native LOTUS entry."""
import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from PIL import Image
from src.modalities.image.file_source import build_image_file_manifest, load_image_file_manifest
from src.modalities.image.source import ImageSourceConfig


class ImageFileSourceTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "images"
        self.root.mkdir()
        for source_id in (139, 285, 632):
            Image.new("RGB", (3, 2), (source_id % 255, 80, 160)).save(
                self.root / f"{source_id:012d}.jpg", format="JPEG")
        self.path = Path(self.temp.name) / "manifest.json"
        self.manifest = build_image_file_manifest(root=self.root, workload_name="coco_val2017", output=self.path)

    def rewrite(self, mutate):
        value = json.loads(self.path.read_text())
        mutate(value)
        self.path.write_text(json.dumps(value))

    def test_real_jpeg_manifest_records_content_and_pass_identities(self):
        selected = self.manifest.entries
        self.assertEqual([entry.doc_id for entry in selected], ["139", "285", "632"])
        for entry in selected:
            self.assertEqual(entry.sha256, hashlib.sha256((self.root / entry.path).read_bytes()).hexdigest())
        ids, metadata = self.manifest.metadata(ImageSourceConfig("coco_val2017", 2, 1, 2))
        self.assertEqual(ids, {"285#pass=1", "632#pass=1", "285#pass=2", "632#pass=2"})
        self.assertEqual(metadata["input_encoded_bytes"], 2 * sum(e.image_bytes for e in selected[1:]))
        self.assertEqual(self.manifest.preparation["content_hash_bytes"], sum(e.image_bytes for e in selected))
        with self.assertRaises(FileExistsError):
            build_image_file_manifest(root=self.root, workload_name="coco_val2017", output=self.path)

    def test_changed_ctime_is_rejected_even_when_size_and_mtime_match(self):
        # Do not assume filesystem ctime resolution advances between two fast
        # writes. The manifest loader checks metadata; the CLI hashes bytes.
        self.rewrite(lambda value: value["entries"][0].update(
            ctime_ns=value["entries"][0]["ctime_ns"] - 1))
        with self.assertRaisesRegex(ValueError, "metadata changed"):
            load_image_file_manifest(self.path, self.root)

    def test_empty_duplicate_wrong_identity_and_invalid_manifest_are_rejected(self):
        original = self.path.read_text()
        cases = [
            lambda p: p.update(entries=[]),
            lambda p: p["entries"].append(p["entries"][0]),
            lambda p: p["entries"][0].update(doc_id="999"),
            lambda p: p["entries"][0].update(sha256="bad"),
            lambda p: p["entries"][0].update(image_bytes=True),
            lambda p: p["preparation"].update(content_hash_s=float("nan")),
        ]
        for mutate in cases:
            with self.subTest(mutate=mutate):
                self.path.write_text(original)
                self.rewrite(mutate)
                with self.assertRaises(ValueError):
                    load_image_file_manifest(self.path, self.root)
        self.path.write_text("[]")
        with self.assertRaises(ValueError):
            load_image_file_manifest(self.path, self.root)

    def test_paths_outside_root_symlinks_and_invalid_suffixes_are_rejected(self):
        original = self.path.read_text()
        for path in ("../outside.jpg", "/absolute.jpg", "./000000000139.jpg", "image.png", "a\\b.jpg"):
            self.path.write_text(original)
            self.rewrite(lambda p: p["entries"][0].update(path=path))
            with self.assertRaises(ValueError):
                load_image_file_manifest(self.path, self.root)
        self.path.write_text(original)
        target = self.root / self.manifest.entries[0].path
        target.unlink()
        target.symlink_to(self.root / self.manifest.entries[1].path)
        with self.assertRaisesRegex(ValueError, "symlink"):
            load_image_file_manifest(self.path, self.root)

    def test_build_rejects_empty_directory_duplicate_ids_and_non_jpeg_payload(self):
        empty = Path(self.temp.name) / "empty"
        empty.mkdir()
        with self.assertRaisesRegex(ValueError, "empty"):
            build_image_file_manifest(root=empty, workload_name="test", output=empty / "new.json")
        (empty / "1.jpg").write_bytes((self.root / self.manifest.entries[0].path).read_bytes())
        (empty / "000000000001.jpg").write_bytes((empty / "1.jpg").read_bytes())
        with self.assertRaisesRegex(ValueError, "duplicate"):
            build_image_file_manifest(root=empty, workload_name="test", output=empty / "new.json")
        (empty / "000000000001.jpg").unlink()
        (empty / "1.jpg").write_bytes(b"not a jpeg")
        with self.assertRaisesRegex(ValueError, "JPEG header"):
            build_image_file_manifest(root=empty, workload_name="test", output=empty / "new.json")

    def test_wrong_workload_missing_rows_and_missing_files_fail(self):
        for config in (ImageSourceConfig("other", 1), ImageSourceConfig("coco_val2017", 4)):
            with self.assertRaises(ValueError):
                self.manifest.metadata(config)
        (self.root / self.manifest.entries[0].path).unlink()
        with self.assertRaises(FileNotFoundError):
            load_image_file_manifest(self.path, self.root)
