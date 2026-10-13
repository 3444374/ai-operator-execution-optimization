"""Pillow-only regressions for immutable, bounded stage A source reads."""

import hashlib
from io import BytesIO
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from PIL import Image

from tests.modalities.image.stage_asset_snapshot import read_image_snapshot
from tests.modalities.image import test_lotus_stage_demand as demand


class StageAssetSnapshotTests(unittest.TestCase):
    def test_verified_bytes_are_used_after_file_replacement(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "image.png"
            Image.new("RGBA", (8, 4), (30, 60, 90, 128)).save(path)
            original = path.read_bytes()
            digest = hashlib.sha256(original).hexdigest()
            snapshot = read_image_snapshot(path, digest, len(original))
            Image.new("RGB", (4, 8), "red").save(path)

            def pixels(image, image_type):
                self.assertEqual(image_type, "base64")
                return image.size, image.convert("RGB").getpixel((0, 0))

            with Image.open(BytesIO(original)) as expected:
                expected_pixels = expected.size, expected.convert("RGB").getpixel((0, 0))
            self.assertEqual(snapshot.fetch(pixels), expected_pixels)
            self.assertEqual(snapshot.encoded, original)
            self.assertEqual(snapshot.content_digest, digest)
            self.assertEqual(snapshot.rgb_bytes, 8 * 4 * 3)
            with self.assertRaisesRegex(ValueError, "source changed"):
                read_image_snapshot(path, digest, max(len(original), path.stat().st_size))

    def test_oversized_source_is_rejected_before_image_decode(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "too-large.bin"
            path.write_bytes(b"x" * 64)
            with patch("tests.modalities.image.stage_asset_snapshot.Image.open",
                       side_effect=AssertionError("decoder started")):
                with self.assertRaisesRegex(BufferError, "snapshot limit"):
                    read_image_snapshot(path, hashlib.sha256(path.read_bytes()).hexdigest(), 32)

    def test_multiframe_source_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "animation.gif"
            Image.new("RGB", (4, 4), "red").save(path, save_all=True,
                append_images=[Image.new("RGB", (4, 4), "blue")])
            encoded = path.read_bytes()
            with self.assertRaisesRegex(ValueError, "not single-frame"):
                read_image_snapshot(path, hashlib.sha256(encoded).hexdigest(), len(encoded))

    def test_finite_manifest_keeps_identity_work_and_source_residency(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            rows = []
            for index in range(12):
                path = root / (str(index) + ".png")
                Image.new("RGB", (8, 4), (index, index * 2, index * 3)).save(path)
                encoded = path.read_bytes()
                rows.append(dict(file=path.name, bytes=len(encoded),
                                 sha256=hashlib.sha256(encoded).hexdigest()))
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps(dict(images=dict(selected=rows))))
            with patch.object(demand, "MANIFEST", manifest), \
                 patch.dict(os.environ, SEMLOOM_COCO_IMAGE_ROOT=str(root)):
                paths, sources, keys, snapshots, _ = demand.stage_assets()
                self.assertEqual(paths, [str(root / row["file"]) for row in rows])
                self.assertEqual(sum(len(s.encoded) for s in snapshots.values()), sum(r["bytes"] for r in rows))
                for path in paths:
                    snapshot, source = snapshots[path], sources[keys[path]]
                    self.assertEqual(keys[path].content_digest, snapshot.content_digest)
                    self.assertEqual(source.work.for_stage("source").units, len(snapshot.encoded))
                    self.assertEqual(source.work.for_stage("prepare").units, snapshot.rgb_bytes)
                    Path(path).unlink()
                    self.assertEqual(snapshot.fetch(lambda image, _: image.size), (8, 4))
                manifest.write_text(json.dumps(dict(images=dict(selected=rows + rows))))
                with self.assertRaisesRegex(ValueError, "twelve unique"):
                    demand.stage_assets()


if __name__ == "__main__":
    unittest.main()
