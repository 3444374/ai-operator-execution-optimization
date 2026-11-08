"""Original Animals image queries/evaluators with native SDK and software replies.

Only the LiteLLM batch transport returns fixed responses. Original ImageArray
formatting, LM parsing, complete queries and gold evaluation remain unchanged.
This does not supply or qualify a visual model provider.
"""

import base64
from collections import deque
from copy import deepcopy
import importlib.util
from io import BytesIO
import json
import os
from pathlib import Path
import socket
import tempfile
import unittest
from unittest.mock import patch

from src.baselines.image import sembench_animals as animals
from src.infrastructure.private_artifacts import content_digest
from src.semantic_methods.lotus.records import identity
from src.serving.backends.vllm_protocol import _prepared


CHECKOUT = os.environ.get("SEMLOOM_NATIVE_TEST_CHECKOUT")
HAS_SDK = all(importlib.util.find_spec(name) is not None
              for name in ("lotus", "litellm", "pandas", "PIL", "duckdb", "cdlib", "overrides", "scipy"))


@unittest.skipUnless(CHECKOUT and HAS_SDK,
    "select the pinned SemBench checkout and install original SDK/evaluator dependencies")
class OriginalAnimalsImageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.checkout = Path(CHECKOUT)
        animals.verify_animals_checkout(cls.checkout)

    def setUp(self):
        import lotus
        from PIL import Image
        from src.serving.backends.vllm_async_engine import VllmAsyncEngine
        from src.serving.backends.vllm_engine import VllmEngine

        self.lotus, self.Image = lotus, Image
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root, self.dataset_number = Path(self.directory.name), 0
        self.guard_attempts = dict(network=0, real_engine=0)
        connect = socket.socket.connect

        def forbid_network(sock, address):
            if sock.family in (socket.AF_INET, socket.AF_INET6):
                self.guard_attempts["network"] += 1
                self.fail("Animals CPU qualification attempted a network connection")
            return connect(sock, address)

        def forbid_engine(*args, **kwargs):
            self.guard_attempts["real_engine"] += 1
            self.fail("Animals CPU qualification constructed a real model engine")

        for scope in (patch.object(socket.socket, "connect", forbid_network),
                patch.object(VllmAsyncEngine, "__init__", forbid_engine),
                patch.object(VllmEngine, "__init__", forbid_engine)):
            scope.start()
            self.addCleanup(scope.stop)
        self.addCleanup(lambda: self.assertEqual(self.guard_attempts, dict(network=0, real_engine=0)))
        self.dataset = self.make_dataset()

    def make_dataset(self, *, count=4, no_zebras=False):
        self.dataset_number += 1
        directory = self.root / ("dataset-" + str(self.dataset_number))
        directory.mkdir()
        paths = []
        for index, color in enumerate(("red", "green", "blue", "yellow")):
            path = directory / ("image-" + str(index) + ".png")
            self.Image.new("RGB", (3, 2), color).save(path)
            paths.append(path)
        rows = []
        for position in range(count):
            index = position % 4
            species = "IMPALA" if index >= 2 else "ELEPHANT" if no_zebras else "ZEBRA"
            rows.append(dict(row_id="animals-row-" + str(position), ImagePath=str(paths[index]),
                Species="REFERENCE_ONLY_" + species,
                City="CityA" if index % 2 == 0 else "CityB",
                StationID="StationA" if index % 2 == 0 else "StationB",
                image=animals.image_identity(paths[index])))
        value = dict(schema=animals.MANIFEST_SCHEMA, role="software_fixture",
            sembench_commit=animals.SEMBENCH_COMMIT, rows=rows)
        value["sha256"] = content_digest(value)
        manifest = directory / "manifest.json"
        manifest.write_text(json.dumps(value) + "\n")
        return animals.AnimalsDataset(self.checkout, manifest, value["sha256"])

    def run_query(self, query_id, responses, *, dataset=None):
        from litellm.types.utils import ModelResponse
        from lotus.models import LM
        import lotus.models.lm as lm_module

        dataset = self.dataset if dataset is None else dataset
        replies = deque(responses)
        batches = []
        lm = LM("openai/CPU-vision-fixture", max_tokens=8, temperature=0)
        lm.max_batch_size = 64

        def batch(*args, **kwargs):
            messages = args[1] if len(args) > 1 else kwargs["messages"]
            batches.append(deepcopy(messages))
            result = []
            for _ in messages:
                text = replies.popleft()
                result.append(ModelResponse(model=lm.model, object="chat.completion",
                    choices=[dict(index=0, finish_reason="stop",
                        message=dict(role="assistant", content=text))],
                    usage=dict(prompt_tokens=11, completion_tokens=2, total_tokens=13)))
            return result

        with self.lotus.settings.context(lm=lm, enable_cache=False), \
                patch.object(lm_module, "batch_completion", batch):
            result = animals.run_original_query(self.checkout, dataset.frame, query_id)
        self.assertFalse(replies, "the original query must consume its complete software response work")
        quality = animals.evaluate_original(self.checkout, result,
            dataset.frame(references=True), query_id)
        self.assertEqual(lm.stats.physical_usage.total_tokens,
            13 * sum(len(messages) for messages in batches))
        return result, quality, batches

    @staticmethod
    def image_uris(messages):
        return [part["image_url"]["url"] for conversation in messages
            for message in conversation if isinstance(message["content"], list)
            for part in message["content"] if part["type"] == "image_url"]

    def test_preflight_declares_complete_64_or_128_occurrences_without_vision_provider(self):
        dataset = self.make_dataset(count=64)
        for query_id in animals.IMAGE_QUERY_IDS:
            with self.subTest(query_id=query_id):
                report = dataset.preflight(query_id=query_id)
                self.assertEqual(report["rows"], 64)
                self.assertEqual(report["expected_model_calls"], 128 if query_id == 7 else 64)
                self.assertEqual(report["native_filter_passes"], 2 if query_id == 7 else 1)
                self.assertEqual(report["query_id"], query_id)
                self.assertEqual(report["query"], "Q" + str(query_id))
                self.assertEqual(report["model_calls"], 0)
                self.assertEqual(report["local_vision_provider_status"], "unwired")
                self.assertIn(f"files/animals/query/gold_sql/Q{query_id}.sql", report["source_sha256"])
        self.assertEqual(dataset.preflight()["query_id"], 1)

    def test_original_image_queries_and_original_gold_evaluators_keep_their_result_schemas(self):
        for query_id in animals.IMAGE_QUERY_IDS:
            with self.subTest(query_id=query_id):
                responses = ["True", "True", "False", "False"]
                if query_id == 7:
                    responses += ["False", "False", "True", "True"]
                result, quality, _ = self.run_query(query_id, responses)
                self.assertEqual(set(quality), {"original_metric", "ground_truth", "predicted", "sembench_commit"})
                self.assertEqual(quality["sembench_commit"], animals.SEMBENCH_COMMIT)
                self.assertEqual(list(result.columns), ["count(*)"] if query_id == 1 else
                    ["city", "stationID"] if query_id == 10 else ["city"])
                if query_id == 1:
                    self.assertEqual(quality["ground_truth"], 2)
                    self.assertEqual(quality["predicted"], 2)
                    self.assertEqual(quality["original_metric"]["absolute_error"], 0)
                else:
                    self.assertEqual(quality["original_metric"]["f1_score"], 1)
                if query_id == 7:
                    self.assertEqual(quality["ground_truth"], ["CityA", "CityB"])
                    self.assertEqual(quality["predicted"], ["CityA", "CityB"])

    def test_native_imagearray_formatter_embeds_exact_png_pixels_without_reference_labels(self):
        _, _, batches = self.run_query(1, ["True", "True", "False", "False"])
        messages = [conversation for batch in batches for conversation in batch]
        uris = self.image_uris(messages)
        self.assertEqual(len(uris), len(self.dataset.read()["rows"]))
        for row, uri in zip(self.dataset.read()["rows"], uris, strict=True):
            self.assertTrue(uri.startswith("data:image/png;base64,"))
            raw = base64.b64decode(uri.partition(",")[2], validate=True)
            with self.Image.open(BytesIO(raw)) as actual, self.Image.open(row["ImagePath"]) as source:
                self.assertEqual(actual.format, "PNG")
                self.assertEqual(actual.size, source.size)
                self.assertEqual(actual.convert("RGB").tobytes(), source.convert("RGB").tobytes())
        self.assertNotIn("Species", self.dataset.frame().columns)
        self.assertNotIn("REFERENCE_ONLY_", json.dumps(messages))

    def test_q7_second_filter_receives_the_whole_original_image_relation(self):
        result, _, batches = self.run_query(7,
            ["True", "True", "False", "False", "False", "False", "True", "True"])
        self.assertEqual(result.city.tolist(), ["CityA", "CityB"])
        self.assertEqual([len(batch) for batch in batches], [4, 4])
        self.assertEqual(self.image_uris(batches[0]), self.image_uris(batches[1]))
        self.assertIn("zebra", json.dumps(batches[0]))
        self.assertIn("impala", json.dumps(batches[1]))

    def test_no_zebras_preserves_original_empty_relations_and_count_evaluation(self):
        dataset = self.make_dataset(no_zebras=True)
        for query_id in animals.IMAGE_QUERY_IDS:
            with self.subTest(query_id=query_id):
                responses = ["False"] * (8 if query_id == 7 else 4)
                result, quality, _ = self.run_query(query_id, responses, dataset=dataset)
                if query_id == 1:
                    self.assertEqual(quality["ground_truth"], 0)
                    self.assertEqual(quality["predicted"], 0)
                    self.assertEqual(quality["original_metric"]["absolute_error"], 0)
                else:
                    self.assertTrue(result.empty)
                    self.assertEqual(quality["ground_truth"], [])
                    self.assertEqual(quality["predicted"], [])
                    if query_id in (3, 10):
                        # Keep the original evaluator's empty-case metric policy.
                        self.assertEqual(quality["original_metric"]["precision"], 1)

    def test_q3_evaluator_accepts_either_valid_maximum_and_rejects_multiple_rows(self):
        import pandas as pd
        labeled = self.dataset.frame(references=True)
        for city in ("CityA", "CityB"):
            with self.subTest(city=city):
                report = animals.evaluate_original(self.checkout, pd.DataFrame(dict(city=[city])), labeled, 3)
                self.assertEqual(report["original_metric"]["f1_score"], 1)
        report = animals.evaluate_original(self.checkout,
            pd.DataFrame(dict(city=["CityA", "CityB"])), labeled, 3)
        self.assertEqual(report["original_metric"]["f1_score"], 0)

    def test_q10_evaluator_checks_the_valid_city_station_pair_without_fixing_tie_order(self):
        import pandas as pd
        labeled = self.dataset.frame(references=True)
        for city, station in (("CityA", "StationA"), ("CityB", "StationB")):
            with self.subTest(city=city):
                report = animals.evaluate_original(self.checkout,
                    pd.DataFrame(dict(city=[city], stationID=[station])), labeled, 10)
                self.assertEqual(report["original_metric"]["f1_score"], 1)
        report = animals.evaluate_original(self.checkout,
            pd.DataFrame(dict(city=["CityA"], stationID=["StationB"])), labeled, 10)
        self.assertEqual(report["original_metric"]["f1_score"], 0)

    def test_reference_species_is_rejected_before_the_native_model_boundary(self):
        import lotus.models.lm as lm_module
        with patch.object(lm_module, "batch_completion",
                side_effect=AssertionError("reference labels reached the model boundary")), \
                self.assertRaisesRegex(ValueError, "Species"):
            animals.run_original_query(self.checkout, lambda: self.dataset.frame(references=True), 1)

    def test_native_image_messages_from_software_query_are_rejected_by_text_ipc(self):
        _, _, batches = self.run_query(1, ["True", "True", "False", "False"])
        conversation = batches[0][0]
        row = dict(identity("CPU-image", "row", 0), messages_json=json.dumps(conversation))
        with self.assertRaisesRegex(ValueError, "text content only"):
            _prepared([row])

    def test_audio_queries_remain_unsupported_and_frozen_pixels_are_still_verified(self):
        for query_id in (2, 4, 5, 6, 8, 9):
            with self.subTest(query_id=query_id), self.assertRaisesRegex(ValueError, "Q1/Q3/Q7/Q10"):
                self.dataset.preflight(query_id)
        path = Path(self.dataset.read()["rows"][0]["ImagePath"])
        self.Image.new("RGB", (3, 2), "black").save(path)
        with self.assertRaisesRegex(ValueError, "bytes or pixels"):
            self.dataset.preflight(1)


if __name__ == "__main__":
    unittest.main()
