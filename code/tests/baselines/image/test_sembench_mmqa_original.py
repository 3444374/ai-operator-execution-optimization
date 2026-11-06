"""Original visual Join/Extract with real LOTUS and fixed CPU answers only."""

import base64
from copy import deepcopy
import importlib.util
import io
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
import unittest
from unittest.mock import patch

from src.baselines.image.sembench_mmqa import describe_inputs, evaluate_original, run_original_query


CHECKOUT = os.environ.get("SEMLOOM_NATIVE_TEST_CHECKOUT")
READY = bool(CHECKOUT and Path(CHECKOUT).is_dir() and importlib.util.find_spec("lotus"))
GOLD_IMAGE = "117d500aaa630023c4038b8268b309c0.png"


@unittest.skipUnless(READY, "requires LOTUS and the pinned SEMLOOM_NATIVE_TEST_CHECKOUT")
class OriginalMMQAImageTests(unittest.TestCase):
    def setUp(self):
        import pandas as pd
        from PIL import Image

        temporary = tempfile.TemporaryDirectory(prefix="sembench-mmqa-image-fixture-")
        self.addCleanup(temporary.cleanup)
        self.data = Path(temporary.name)
        (self.data / "images").mkdir()
        Image.new("RGB", (5, 3), (0, 0, 255)).save(self.data / "images" / GOLD_IMAGE)
        Image.new("RGB", (5, 3), (255, 0, 0)).save(self.data / "images" / "other.jpg")
        (self.data / "images" / "ignored.PNG").write_text("not an original selected extension")
        pd.DataFrame({"ID": [0, 5, 6, 9, 10], "Track": ["software racetrack"] * 5}).to_csv(
            self.data / "ap_warrior.csv", index=False)
        connect = socket.socket.connect

        def offline(sock, address):
            if sock.family in (socket.AF_INET, socket.AF_INET6):
                self.fail("visual CPU fixture attempted a network connection")
            return connect(sock, address)

        guard = patch.object(socket.socket, "connect", offline)
        guard.start()
        self.addCleanup(guard.stop)
        aliases = ("src.runner", "src.runner.generic_lotus_runner",
                   "src.runner.generic_lotus_runner.generic_lotus_runner")
        before = {name: sys.modules.get(name) for name in aliases}
        self.addCleanup(lambda: self.assertEqual({name: sys.modules.get(name) for name in aliases}, before))

    def _run(self, query, *, none_match=False, extract_text='{"color":"blue"}'):
        import lotus
        from lotus.models import LM
        from litellm.types.utils import ModelResponse
        from PIL import Image

        lm = LM("openai/local-image-fixture", max_tokens=8192, max_batch_size=64)
        batches = []

        def respond(data, options, *_args):
            messages = [deepcopy(message) for message, _ in data]
            batches.append((messages, deepcopy(options)))
            replies = []
            for index, message in enumerate(messages):
                parts = [part for item in message if isinstance(item["content"], list)
                         for part in item["content"] if part["type"] == "image_url"]
                self.assertEqual(len(parts), 1)
                uri = parts[0]["image_url"]["url"]
                self.assertTrue(uri.startswith("data:image/png;base64,"))
                with Image.open(io.BytesIO(base64.b64decode(uri.split(",", 1)[1]))) as image:
                    self.assertEqual(image.size, (5, 3))
                    blue = image.convert("RGB").getpixel((0, 0)) == (0, 0, 255)
                if options.get("response_format") == {"type": "json_object"}:
                    text = extract_text
                else:
                    text = "Reasoning:\nsoftware answer\nAnswer: " + str(blue and not none_match)
                replies.append(ModelResponse(model=lm.model, id="software-" + str(index),
                    choices=[dict(index=0, finish_reason="stop", message=dict(role="assistant", content=text))],
                    usage=dict(prompt_tokens=0, completion_tokens=0, total_tokens=0)))
            return replies

        lm._process_uncached_messages = respond
        with lotus.settings.context(lm=lm, enable_cache=False):
            result = run_original_query(CHECKOUT, self.data, query)
        return result, batches

    def test_q2a_calls_every_pair_and_uses_original_gold(self):
        result, batches = self._run("Q2a")
        self.assertEqual([len(messages) for messages, _ in batches], [10])
        self.assertEqual(list(result.columns), ["ID", "image_id"])
        self.assertEqual(set(result["ID"]), {0, 5, 6, 9, 10})
        self.assertEqual(set(result["image_id"]), {GOLD_IMAGE})
        self.assertEqual(evaluate_original(CHECKOUT, "2a", result)["original_metric"]["f1_score"], 1)

    def test_all_visual_joins_request_native_zero_shot_cot(self):
        import pandas as pd
        from lotus.templates.task_instructions import cot_prompt_formatter

        (self.data / "images" / "other.jpg").unlink()
        pd.DataFrame({"ID": [0], "Track": ["software racetrack"]}).to_csv(
            self.data / "ap_warrior.csv", index=False)
        pd.DataFrame({"Airlines": ["software airline"], "Destinations": ["London"]}).to_csv(
            self.data / "tampa_international_airport.csv", index=False)
        expected = cot_prompt_formatter(answer_instructions="The answer should be either True or False")
        for query in ("2a", "2b", "7"):
            with self.subTest(query=query):
                _, batches = self._run(query)
                for message in batches[0][0]:
                    self.assertIn(expected, message[0]["content"])

    def test_native_join_receives_enum_and_restores_its_accessor_on_success_and_failure(self):
        from lotus.sem_ops.sem_extract import SemExtractDataFrame
        from lotus.sem_ops.sem_join import SemJoinDataframe
        from lotus.types import ReasoningStrategy

        original, seen = SemJoinDataframe.__call__, []
        original_extract, extract_rows = SemExtractDataFrame.__call__, []

        def observe(accessor, *args, **kwargs):
            seen.append(kwargs["strategy"])
            return original(accessor, *args, **kwargs)

        def observe_extract(accessor, *args, **kwargs):
            extract_rows.append(len(accessor._obj))
            return original_extract(accessor, *args, **kwargs)

        with (patch.object(SemJoinDataframe, "__call__", observe),
              patch.object(SemExtractDataFrame, "__call__", observe_extract)):
            self._run("2a")
            self.assertIs(SemJoinDataframe.__call__, observe)
            self._run("2b", none_match=True)
            self.assertIs(SemExtractDataFrame.__call__, observe_extract)
            with self.assertRaises(KeyError):
                self._run("2b", extract_text="{}")
            self.assertIs(SemJoinDataframe.__call__, observe)
            self.assertIs(SemExtractDataFrame.__call__, observe_extract)
        self.assertIs(SemJoinDataframe.__call__, original)
        self.assertIs(SemExtractDataFrame.__call__, original_extract)
        self.assertEqual(seen, [ReasoningStrategy.ZS_COT] * 3)
        self.assertEqual(extract_rows, [0, 5])

    def test_q2b_runs_join_then_extract_for_every_actual_match(self):
        result, batches = self._run("2b")
        self.assertEqual([len(messages) for messages, _ in batches], [10, 5])
        self.assertNotIn("response_format", batches[0][1])
        self.assertEqual(batches[1][1]["response_format"], {"type": "json_object"})
        self.assertEqual(list(result.columns), ["ID", "image_id", "color"])
        self.assertEqual(result["color"].tolist(), ["blue"] * 5)
        self.assertEqual(evaluate_original(CHECKOUT, "2b", result)["original_metric"]["f1_score"], 1)
        # The repeated image remains five Extract occurrences, not one cached answer.
        self.assertEqual(len({json.dumps(message, sort_keys=True) for message in batches[1][0]}), 1)

    def test_no_q2a_matches_returns_original_empty_schema(self):
        result, batches = self._run("2a", none_match=True)
        self.assertEqual([len(messages) for messages, _ in batches], [10])
        self.assertTrue(result.empty)
        self.assertEqual(list(result.columns), ["ID", "image_id"])

    def test_q2b_missing_color_is_not_repaired_after_native_parse(self):
        with self.assertRaises(KeyError):
            self._run("2b", extract_text="{}")

    def test_q2b_no_matches_returns_empty_declared_schema_without_extract_requests(self):
        import pandas as pd

        result, batches = self._run("2b", none_match=True)
        self.assertEqual([len(messages) for messages, _ in batches], [10, 0])
        self.assertEqual(batches[1][1]["response_format"], {"type": "json_object"})
        self.assertTrue(result.empty)
        self.assertEqual(list(result.columns), ["ID", "image_id", "color"])
        saved = self.data / "empty-q2b.json"
        saved.write_text(result.to_json(orient="split"))
        restored = pd.read_json(saved, orient="split")
        self.assertEqual(list(restored.columns), list(result.columns))
        metric = evaluate_original(CHECKOUT, "2b", restored)["original_metric"]
        self.assertEqual(metric["f1_score"], 0)

    def test_q7_preserves_original_all_airline_join_and_separate_gold(self):
        import pandas as pd
        from PIL import Image

        (self.data / "images" / GOLD_IMAGE).unlink()
        airport_image = "cwncgabxti09zmf36t4phvaz1xzv10wu.png"
        Image.new("RGB", (5, 3), (0, 0, 255)).save(self.data / "images" / airport_image)
        pd.DataFrame({"Airlines": ["British Airways", "software domestic airline"],
                      "Destinations": ["London", "Chicago"]}).to_csv(
                          self.data / "tampa_international_airport.csv", index=False)
        result, batches = self._run("7")
        self.assertEqual([len(messages) for messages, _ in batches], [4])
        self.assertEqual(list(result.columns), ["Airlines", "image_id"])
        # The pinned query does not apply the natural question's Europe filter.
        self.assertEqual(set(result["Airlines"]), {"British Airways", "software domestic airline"})
        metric = evaluate_original(CHECKOUT, "7", result)["original_metric"]
        self.assertEqual(metric["precision"], 0.5)
        self.assertEqual(metric["recall"], 0.2)
        plan = describe_inputs(CHECKOUT, self.data, "7")
        self.assertEqual(plan["exact_join_candidates"], 4)
        self.assertEqual(plan["maximum_model_calls"], 4)
        self.assertIn("Europe", plan["original_source_difference"])

    def test_plan_uses_original_directory_selection_and_full_join_budget(self):
        names = ["other.jpg", GOLD_IMAGE, "ignored.PNG"]
        with patch("os.listdir", return_value=names):
            report = describe_inputs(CHECKOUT, self.data, "Q2b")
        self.assertEqual([Path(image["path"]).name for image in report["images"]], names[:2])
        self.assertEqual(report["exact_join_candidates"], 10)
        self.assertEqual(report["maximum_model_calls"], 20)
        self.assertIsNone(report["actual_followup_extracts"])
        self.assertEqual(report["model_calls"], 0)
        for image in report["images"]:
            self.assertEqual((image["width"], image["height"]), (5, 3))
            self.assertEqual(len(image["rgb_sha256"]), 64)

    def test_check_reports_pending_visual_providers_without_claiming_execution(self):
        from src.experiments.sembench_image_query import check

        report = check(CHECKOUT, "mmqa", "2a", data_dir=self.data)
        self.assertFalse(report["execution_enabled"])
        self.assertEqual(report["requests_claimed"], 0)
        self.assertEqual(report["inputs"]["maximum_model_calls"], 10)
        self.assertTrue(all(value is None for value in report["results"].values()))

    def test_check_cli_writes_private_report_and_keeps_source_bytes(self):
        from src.experiments.sembench_image_query import main

        before = {str(path): path.read_bytes() for path in self.data.rglob("*") if path.is_file()}
        output = self.data / "check-artifact"
        with patch("sys.stdout", new=io.StringIO()):
            status = main(["--sembench-checkout", CHECKOUT, "--scenario", "mmqa", "--query", "2b",
                           "--data-dir", str(self.data), "--output-dir", str(output)])
        self.assertEqual(status, 0)
        report = json.loads((output / "check.json").read_text())
        self.assertEqual(report["inputs"]["maximum_model_calls"], 20)
        self.assertEqual({name: Path(name).read_bytes() for name in before}, before)

    def test_unsupported_visual_query_is_rejected_before_data_access(self):
        with self.assertRaisesRegex(ValueError, "Q2a, Q2b and Q7"):
            describe_inputs(CHECKOUT, self.data / "does-not-exist", "q8")
