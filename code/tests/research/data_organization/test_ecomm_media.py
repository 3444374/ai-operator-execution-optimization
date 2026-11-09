"""Request identity and representation lifetime checks, with no SDK or model."""

import io
import json
from copy import deepcopy
from types import SimpleNamespace
from pathlib import Path
import tempfile
import unittest

from src.research.data_organization.ecomm_media import RequestAudit, TileImages, asset_tiles
from src.research.data_organization.ecomm_media_profile import MediaObserver, record_profile


class EcommMediaTests(unittest.TestCase):
    def test_tiles_preserve_every_occurrence_and_first_seen_asset_order(self):
        assets = ["a", "b", "a", "c", "b", "d"]
        self.assertEqual(asset_tiles(assets, 2), [(0, 2, 1, 4), (3, 5)])
        self.assertEqual(sorted(i for tile in asset_tiles(assets, 1) for i in tile), list(range(6)))
        self.assertEqual(asset_tiles([], 1), [])

    def test_tile_reuses_native_string_without_merging_requests_or_mutable_images(self):
        calls = []

        def fetch(image, image_type):
            calls.append((image, image_type))
            return "data:image/png;base64," + image if image_type == "base64" else object()

        cache = TileImages(fetch)
        first = cache("a", "base64")
        self.assertIs(cache("a", "base64"), first)
        self.assertIsNot(cache("a"), cache("a"))
        self.assertEqual(len(calls), 3)
        cache.clear()
        cache("a", "base64")
        self.assertEqual(len(calls), 4)

    def test_reordered_requests_keep_duplicate_occurrences_and_complete_media(self):
        reference_stream, replay_stream = io.StringIO(), io.StringIO()
        reference = RequestAudit(reference_stream, max_requests=8, pattern="mixed")
        replay = RequestAudit(replay_stream, max_requests=8)
        messages = [[dict(role="user", content=[dict(type="image_url",
                      image_url=dict(url="data:image/png;base64,YQ=="))])]]
        for key in ("filter_0:0", "filter_0:1"):
            with reference.operation("filter_0", [(key, [])]):
                reference.reply(messages, model="model/revision", params=dict(temperature=0, max_tokens=8))
        replay.fixed = reference.responses
        for key in reversed(reference.requests):
            with replay.operation("filter_0", [(key, [])]):
                replay.reply(messages, model="model/revision", params=dict(max_tokens=8, temperature=0))
        reference.assert_same_requests(replay)
        self.assertEqual(len(replay.requests), 2, "identical payloads still have two physical occurrences")
        self.assertNotEqual(list(reference.requests), list(replay.requests))
        written = json.loads(replay_stream.getvalue().splitlines()[0])
        self.assertEqual(written["request"]["messages"], messages[0])

    def test_duplicate_missing_and_excess_occurrences_fail(self):
        audit = RequestAudit(io.StringIO(), max_requests=2)
        with audit.operation("filter_0", [("row:0", [])]):
            audit.reply([[dict(role="user", content="same")]], model="fixture", params={})
        with self.assertRaisesRegex(ValueError, "twice"):
            with audit.operation("filter_0", [("row:0", [])]):
                audit.reply([[]], model="fixture", params={})
        with self.assertRaisesRegex(ValueError, "every occurrence"):
            with audit.operation("filter_0", [("row:1", [])]):
                pass
        with self.assertRaisesRegex(ValueError, "extra model"):
            with audit.operation("filter_0", []):
                audit.reply([[]], model="fixture", params={})
        limited = RequestAudit(io.StringIO(), max_requests=1)
        with self.assertRaisesRegex(ValueError, "allowance"):
            with limited.operation("filter_0", [("row:0", []), ("row:1", [])]):
                limited.reply([[], []], model="fixture", params={})
        self.assertEqual(len(limited.requests), 1, "partial evidence survives a stopped probe")

    def test_replay_never_invents_an_output_for_an_unseen_request(self):
        audit = RequestAudit(io.StringIO(), fixed={}, max_requests=1)
        with self.assertRaises(KeyError):
            with audit.operation("filter_0", [("unseen", [])]):
                audit.reply([[]], model="fixture", params={})

    def test_same_request_coverage_rejects_changed_media_model_params_or_dependency(self):
        def record(message, model="fixture", params=None, deps=None):
            audit = RequestAudit(io.StringIO(), max_requests=1)
            with audit.operation("filter_0", [("filter_0:0", deps or [])]):
                audit.reply([message], model=model, params=params or {"temperature": 0})
            return audit

        message = [dict(role="user", content=[dict(type="image_url", image_url=dict(url="data:image/png;base64,YQ=="))])]
        reference = record(message)
        changed_media = deepcopy(message)
        changed_media[0]["content"][0]["image_url"]["url"] = "data:image/png;base64,Yg=="
        for changed in (record(changed_media), record(message, model="different"),
                        record(message, params={"temperature": 1}), record(message, deps=["parent:1"])):
            with self.assertRaisesRegex(ValueError, "per-occurrence"):
                reference.assert_same_requests(changed)

    def test_observer_retains_native_index_cache_and_releases_only_tile_arrays(self):
        calls = []

        def fetch(value, image_type):
            calls.append(value)
            return "data:image/png;base64," + value

        module = SimpleNamespace(fetch_image=fetch)

        class ImageArray:
            def __init__(self, values):
                self.values, self._cached_images = values, {}

            def get_image(self, index, image_type="Image"):
                key = index, image_type
                if key not in self._cached_images:
                    self._cached_images[key] = module.fetch_image(self.values[index], image_type)
                return self._cached_images[key]

        module.ImageArray = ImageArray
        templates = SimpleNamespace(df2multimodal_info=lambda *args: None, filter_formatter=lambda *args: None)
        observer = MediaObserver(module, templates)
        with observer.observe():
            outside = ImageArray(["outside"])
            outside.get_image(0, "base64")
            with observer.tile(shared=False):
                native_tile = ImageArray(["native-only"])
                native_tile.get_image(0, "base64")
            self.assertFalse(native_tile._cached_images)
            with self.assertRaisesRegex(RuntimeError, "cancel tile"):
                with observer.tile(shared=True):
                    same = ImageArray(["a"])
                    for _ in range(3):
                        same.get_image(0, "base64")
                    sliced = ImageArray(["a"])
                    self.assertIs(same.get_image(0, "base64"), sliced.get_image(0, "base64"))
                    raise RuntimeError("cancel tile")
            self.assertFalse(same._cached_images)
            self.assertFalse(sliced._cached_images)
            self.assertTrue(outside._cached_images)
        counts = observer.metrics["initial_filters"]
        self.assertEqual(calls, ["outside", "native-only", "a"])
        self.assertEqual(counts["native_index_cache_hits"], 3)
        self.assertEqual(counts["tile_shared_hits"], 1)
        self.assertGreater(counts["peak_observed_cached_string_bytes"], 0)
        self.assertEqual(observer.samples[-1][2], len(outside._cached_images[(0, "base64")]))
        self.assertIs(module.fetch_image, fetch, "the isolated probe must restore native functions")

    def test_failed_profile_keeps_completed_observations_in_one_private_record(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            with self.assertRaisesRegex(RuntimeError, "fixture failed"):
                with record_profile(output) as report:
                    report["runs"] = [dict(case="branch_native", logical_requests=3)]
                    raise RuntimeError("fixture failed")
            saved = json.loads((output / "profile.json").read_text())
            self.assertEqual(saved["status"], "failed")
            self.assertEqual(saved["runs"], report["runs"])
            self.assertEqual(list(output.iterdir()), [output / "profile.json"])


if __name__ == "__main__":
    unittest.main()
