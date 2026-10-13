"""Optional source-function fixture, without importing LOTUS or a model SDK.

Set SEMLOOM_LOTUS_SOURCE_ROOT to the lotus/ directory at the pinned commit.
Only the reviewed image-loading and user-message functions are compiled. This
does not run a DataFrame accessor, LM executor, provider SDK or model request.
"""

from __future__ import annotations

import ast
import base64
import hashlib
from io import BytesIO
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

import numpy as np
from PIL import Image, __version__ as pillow_version

from src.data.materializers.prepared_assets import AssetUse, PreparedAssetWindow, prepared_asset_key
from src.semantic_methods.lotus.sdk import _validate_messages


SOURCE_HASHES = {
    "utils.py": "2d8d38af8230f447a354f460e25a8f523f1dc1aa5edebf977a8297ed04458ebe",
    "templates/task_instructions.py": "059909938902f3fa44ae1fc9666ce91452266f1e4e8d57c5c81134c24e51a8a3",
}


def source_functions(root, relative, names):
    payload = (root / relative).read_bytes()
    if hashlib.sha256(payload).hexdigest() != SOURCE_HASHES[relative]:
        raise ValueError("LOTUS source fixture differs: " + relative)
    tree = ast.parse(payload)
    selected = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    if {n.name for n in selected} != set(names):
        raise ValueError("LOTUS source fixture is missing reviewed functions")
    module = ast.Module(body=[ast.ImportFrom(module="__future__", level=0,
                         names=[ast.alias(name="annotations")]), *selected], type_ignores=[])
    def reject_network(*args, **kwargs):
        raise AssertionError("the source fixture must never access remote images")
    scope = {"np": np, "Image": Image, "BytesIO": BytesIO, "base64": base64,
             "requests": SimpleNamespace(get=reject_network)}
    exec(compile(ast.fix_missing_locations(module), str(root / relative), "exec"), scope)
    return scope


@unittest.skipUnless(os.environ.get("SEMLOOM_LOTUS_SOURCE_ROOT"), "pinned LOTUS source not selected")
class LotusImagePreparationSourceTest(unittest.TestCase):
    def test_small_encoded_image_does_not_imply_small_decoder_scratch(self):
        buffer = BytesIO()
        Image.new("RGB", (1024, 1024), "black").save(buffer, format="PNG")
        encoded = buffer.getvalue()
        with Image.open(BytesIO(encoded)) as header:
            width, height = header.size
        decoded_rgb_bytes = width * height * 3
        self.assertLess(len(encoded), 16384)
        self.assertGreater(decoded_rgb_bytes, 1024 * 1024)
        use = AssetUse("one-frame", prepared_asset_key(encoded, "fixed-rgb-png-transform"))
        window = PreparedAssetWindow((use,), max_consumers=1, host_bytes=16384)
        with self.assertRaisesRegex(BufferError, "cannot fit"):
            window.acquire("one-frame", prepare=lambda: self.fail("decode started without scratch"),
                           prepared_bytes_upper_bound=8192,
                           scratch_bytes_upper_bound=2 * decoded_rgb_bytes)
        self.assertEqual(window.snapshot().prepare_calls, 0)
        self.assertEqual(window.snapshot().prepared_bytes, 0)

    def test_same_four_image_text_messages_with_two_prepares_and_static_cache_counterexample(self):
        root = Path(os.environ["SEMLOOM_LOTUS_SOURCE_ROOT"])
        fetch = source_functions(root, "utils.py", {"fetch_image"})["fetch_image"]
        formatter = source_functions(root, "templates/task_instructions.py",
                                     {"context_formatter", "user_message_formatter"})["user_message_formatter"]
        with tempfile.TemporaryDirectory(prefix="semloom-stage-source-") as temporary:
            paths = []
            for name, color in (("a", "red"), ("b", "blue")):
                path = Path(temporary) / (name + ".png")
                Image.new("RGBA", (8, 8), color).save(path)
                paths.append(path)
            # Relational aliases of the same image keep their own listing text.
            rows = [("listing-0", paths[0], "red cup"), ("listing-1", paths[0], "red plate"),
                    ("listing-2", paths[1], "blue cup"), ("listing-3", paths[1], "blue plate")]
            instruction = "Instruction: Does the image match the listing description?"
            def message(uri, text):
                return formatter({"text": "[Listing_text]: «" + text + "»\n",
                                  "image": {"Image": uri}}, instruction)
            native = [message(fetch(str(path), "base64"), text) for _, path, text in rows]
            _validate_messages([{"role": "user", "content": "ordinary text remains supported"}])
            with self.assertRaisesRegex(ValueError, "supports text messages"):
                _validate_messages([native[0]])
            transform = "lotus-rgb-png-base64:" + SOURCE_HASHES["utils.py"] + ":pillow=" + pillow_version
            uses = tuple(AssetUse(row, prepared_asset_key(path.read_bytes(), transform))
                         for row, path, _ in rows)
            window = PreparedAssetWindow(uses, max_consumers=4, host_bytes=16384)
            leases, shared, static, static_cache = [], [], [], {}
            for (row, path, text), use in zip(rows, uses, strict=True):
                lease = window.acquire(row, prepare=lambda path=path: fetch(str(path), "base64").encode(),
                                       prepared_bytes_upper_bound=4096, scratch_bytes_upper_bound=4096)
                self.assertIsNotNone(lease)
                leases.append(lease)
                shared.append(message(lease.payload.decode(), text))
                if use.key not in static_cache:
                    static_cache[use.key] = fetch(str(path), "base64")
                static.append(message(static_cache[use.key], text))
            canonical = lambda messages: json.dumps(messages, ensure_ascii=False, separators=(",", ":")).encode()
            self.assertEqual(canonical(native), canonical(shared))
            self.assertEqual(canonical(native), canonical(static))
            self.assertEqual((len(shared), window.snapshot().prepare_calls, len(static_cache)), (4, 2, 2))
            self.assertEqual(len({canonical([m]) for m in shared}), 4)
            # Output completion order does not merge rows or release another use.
            for i in (1, 3, 0, 2):
                window.release(leases[i])
            self.assertEqual(window.snapshot().prepared_bytes, 0)
            self.assertEqual(window.snapshot().held_consumers, 0)
            self.assertLessEqual(window.snapshot().host_reservation_peak_bytes, 16384)


if __name__ == "__main__":
    unittest.main()
