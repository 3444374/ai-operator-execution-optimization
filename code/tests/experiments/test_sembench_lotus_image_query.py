"""Original LOTUS visual queries through native SDK HTTP, with no service/model.

Only HTTPTransport.handle_request is replaced by HTTPX MockTransport. Original
LM batching/parsing and the real POST budget/event observer remain in use.
"""

import base64
from collections import Counter
from contextlib import contextmanager, redirect_stdout
import csv
import hashlib
from io import BytesIO, StringIO
import importlib.util
import itertools
import json
import os
from pathlib import Path
import socket
import tempfile
import threading
import unittest
from unittest.mock import patch

from src.baselines.image import sembench_animals as animals
from src.infrastructure.private_artifacts import content_digest, write_private_json


CHECKOUT = os.environ.get("SEMLOOM_NATIVE_TEST_CHECKOUT")
HAS_SDK = all(importlib.util.find_spec(name) is not None for name in
    ("lotus", "litellm", "httpx", "pandas", "PIL", "duckdb", "cdlib", "overrides", "scipy"))
KEY_ENV = "SEMLOOM_IMAGE_FIXTURE_KEY"
KEY_VALUE = "test-key"


class NativeHTTPReplies:
    def __init__(self, case, scenario, *, mode="success", failure=None, response_model=None):
        self.case, self.scenario = case, scenario
        self.mode, self.failure, self.response_model = mode, failure, response_model
        self.requests, self.lock = [], threading.Lock()

    @staticmethod
    def image_uris(body):
        return [part["image_url"]["url"] for message in body["messages"]
            if isinstance(message["content"], list) for part in message["content"]
            if part["type"] == "image_url"]

    def respond(self, request):
        import httpx
        from PIL import Image

        self.case.assertEqual(request.method, "POST")
        self.case.assertEqual(str(request.url), "http://localhost:18473/v1/chat/completions")
        self.case.assertEqual(request.headers["authorization"], "Bearer " + KEY_VALUE)
        body = json.loads(request.content)
        self.case.assertEqual(body["model"], "local-image-fixture")
        uris = self.image_uris(body)
        self.case.assertTrue(uris, "original visual requests must contain their actual image")
        self.case.assertTrue(all(uri.startswith("data:image/png;base64,") for uri in uris))
        self.case.assertNotIn("REFERENCE_ONLY_", json.dumps(body))
        self.case.assertEqual(body.get("max_tokens", body.get("max_completion_tokens")), 32)
        if self.scenario == "mmqa" and body.get("response_format") != {"type": "json_object"}:
            from lotus.templates.task_instructions import cot_prompt_formatter
            expected = cot_prompt_formatter(answer_instructions="The answer should be either True or False")
            self.case.assertIn(expected, body["messages"][0]["content"])
        self.case.assertEqual(body.get("stop", []), [])
        pixels = []
        for uri in uris:
            with Image.open(BytesIO(base64.b64decode(uri.partition(",")[2]))) as image:
                self.case.assertEqual(image.format, "PNG")
                self.case.assertEqual(image.size, (3, 2))
                pixels.append(image.convert("RGB").getpixel((0, 0)))
        with self.lock:
            request_id = "chatcmpl-fixture-" + str(len(self.requests))
            self.requests.append(dict(body=body, payload=request.content))
        if self.failure == "timeout":
            raise httpx.ReadTimeout("software fixture transport timeout", request=request)
        if self.failure == "api":
            return httpx.Response(503, request=request,
                json={"error": {"message": "software fixture API failure", "type": "server_error"}})
        if body.get("response_format") == {"type": "json_object"}:
            text = "{}" if self.mode == "empty_json" else '{"color":"red"}'
        elif self.scenario == "animals":
            pixel = pixels[0]
            instruction = json.dumps(body["messages"]).lower()
            keep = pixel in ((0, 0, 255), (255, 255, 0)) if "impala" in instruction else \
                pixel in ((255, 0, 0), (0, 128, 0))
            text = "True" if keep else "False"
        else:
            text = ("Reasoning:\nThe word True is in this software reasoning, not the decision.\n\nAnswer: False"
                if self.mode == "distracting_reasoning" else
                "Reasoning: deterministic CPU fixture.\nAnswer: " + ("False" if self.mode == "no_matches" else "True"))
        return httpx.Response(200, request=request, json=dict(
            id=request_id, object="chat.completion", created=1,
            model=self.response_model or "local-image-fixture",
            choices=[dict(index=0, finish_reason="stop", message=dict(role="assistant", content=text))],
            usage=dict(prompt_tokens=11, completion_tokens=2, total_tokens=13)))

    @contextmanager
    def install(self):
        import httpx
        transport = httpx.MockTransport(self.respond)

        def handle(_native_transport, request):
            return transport.handle_request(request)

        try:
            with patch.object(httpx.HTTPTransport, "handle_request", handle):
                yield self
        finally:
            transport.close()


@unittest.skipUnless(CHECKOUT and HAS_SDK, "requires the pinned SemBench checkout and real native SDK")
class NativeLotusImageQueryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from src.experiments import sembench_lotus_image_query
        cls.entry = sembench_lotus_image_query
        cls.checkout = Path(CHECKOUT)
        animals.verify_animals_checkout(cls.checkout)

    def setUp(self):
        from PIL import Image
        from src.serving.backends.vllm_async_engine import VllmAsyncEngine
        from src.serving.backends.vllm_engine import VllmEngine

        self.Image, self.serial = Image, itertools.count()
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        scope = patch.dict(os.environ, {KEY_ENV: KEY_VALUE})
        scope.start()
        self.addCleanup(scope.stop)
        self.guard_attempts = dict(network=0, real_engine=0)
        connect = socket.socket.connect

        def forbid_network(sock, address):
            if sock.family in (socket.AF_INET, socket.AF_INET6):
                self.guard_attempts["network"] += 1
                self.fail("visual CPU integration attempted an actual IP connection")
            return connect(sock, address)

        def forbid_engine(*args, **kwargs):
            self.guard_attempts["real_engine"] += 1
            self.fail("visual CPU integration constructed an actual model engine")

        for scope in (patch.object(socket.socket, "connect", forbid_network),
                patch.object(VllmAsyncEngine, "__init__", forbid_engine),
                patch.object(VllmEngine, "__init__", forbid_engine)):
            scope.start()
            self.addCleanup(scope.stop)
        self.addCleanup(lambda: self.assertEqual(self.guard_attempts, dict(network=0, real_engine=0)))

    def new_directory(self, label):
        path = self.root / (label + "-" + str(next(self.serial)))
        path.mkdir(mode=0o700)
        return path

    def animals_inputs(self, *, count=4):
        directory = self.new_directory("animals")
        rows = []
        for index, color in enumerate(("red", "green", "blue", "yellow")[:count]):
            path = directory / (str(index) + ".png")
            self.Image.new("RGB", (3, 2), color).save(path)
            rows.append(dict(row_id="animals-row-" + str(index), ImagePath=str(path),
                City="CityA" if index % 2 == 0 else "CityB",
                StationID="StationA" if index % 2 == 0 else "StationB",
                Species="REFERENCE_ONLY_ZEBRA" if index < 2 else "REFERENCE_ONLY_IMPALA",
                image=animals.image_identity(path)))
        value = dict(schema=animals.MANIFEST_SCHEMA, role="software_fixture",
            sembench_commit=animals.SEMBENCH_COMMIT, rows=rows)
        value["sha256"] = content_digest(value)
        path = directory / "manifest.json"
        write_private_json(path, value)
        return dict(manifest=path, manifest_digest=value["sha256"])

    def mmqa_inputs(self):
        directory = self.new_directory("mmqa")
        (directory / "images").mkdir()
        # Different image occurrences intentionally carry identical PNG pixels.
        for name in ("first.png", "duplicate.png"):
            self.Image.new("RGB", (3, 2), "red").save(directory / "images" / name)
        for filename, fields, rows in (
            ("ap_warrior.csv", ("ID", "Track"), ((1, "TrackA"), (2, "TrackB"))),
            ("tampa_international_airport.csv", ("Airlines", "Destinations"),
             (("DomesticOnly", "Chicago"), ("EuropeanLine", "Frankfurt"))),
        ):
            with (directory / filename).open("w", newline="") as stream:
                writer = csv.writer(stream)
                writer.writerow(fields)
                writer.writerows(rows)
        return dict(data_dir=directory)

    def config(self, scenario="animals", query="Q1", **changes):
        value = dict(schema="semloom.sembench_lotus_image_run.v1", scenario=scenario,
            query=query, query_id="cpu-image-query", model_name="openai/local-image-fixture",
            api_base="http://localhost:18473/v1", api_key_env=KEY_ENV,
            generation=dict(max_tokens=32, temperature=0, top_p=1, seed=0),
            native_lm_max_batch_size=2, query_timeout_s=60,
            service_profile=dict(role="software HTTP fixture", model_revision="fixture",
                processor_revision="fixture", dtype="fixture"))
        value.update(changes)
        path = self.root / ("config-" + str(next(self.serial)) + ".json")
        write_private_json(path, value)
        config, generation = self.entry.configuration(path)
        self.assertEqual(generation.max_tokens, value["generation"]["max_tokens"])
        return config, path

    def ledger(self, config, requests):
        from src.experiments.attempt_ledger import AttemptBudget
        from src.experiments.cell_budget import CellBudgetLedger
        path = self.root / ("budget-" + str(next(self.serial)) + ".sqlite")
        ledger = CellBudgetLedger.create(path, AttemptBudget("cpu-image-budget", requests), deadline_utc=None)
        ledger.reserve_unit(config["query_id"], requests)
        value = dict(path=str(path), id="cpu-image-budget", limit=requests, unit_id=config["query_id"])
        return ledger, value

    def invoke(self, config, inputs, replies, *, allowance=None, worker=False):
        checked = self.entry.check(config, self.checkout, **inputs)
        maximum = checked["inputs"].get("expected_model_calls", checked["inputs"].get("maximum_model_calls"))
        directory = self.new_directory("query")
        if worker:
            claim = None
        else:
            ledger, _ = self.ledger(config, maximum if allowance is None else allowance)
            claim = ledger.claim_unit(config["query_id"])
            claim.bind_execution()
        self.last = dict(directory=directory, claim=claim, replies=replies)
        with replies.install():
            report = (self.entry.run_worker(config, self.checkout, directory, **inputs) if worker else
                self.entry.run_query(config, self.checkout, directory, budget=claim, **inputs))
        return report, directory, claim

    @staticmethod
    def result_rows(report):
        value = report["result"]
        return [dict(zip(value["columns"], row, strict=True)) for row in value["data"]]

    def assert_audit(self, report, directory, replies):
        events = [json.loads(line) for line in (directory / "native-http.jsonl").read_text().splitlines()]
        requests = [event for event in events if event["event"] == "request"]
        self.assertEqual(Counter(event["request_values_sha256"] for event in requests),
            Counter(content_digest(request["body"]) for request in replies.requests))
        self.assertEqual(Counter(event["request_bytes_sha256"] for event in requests),
            Counter(hashlib.sha256(request["payload"]).hexdigest() for request in replies.requests))
        self.assertEqual(report["model_calls"], len(replies.requests))
        self.assertTrue(report["event_writer"]["closed"])
        evidence = (directory / "native-http.jsonl").read_text() + json.dumps(report)
        self.assertNotIn("data:image/png;base64,", evidence)
        self.assertNotIn(KEY_VALUE, evidence)
        if requests:
            self.assertIn("url_sha256", evidence)

    def test_check_and_run_without_execute_model_do_not_claim_or_send(self):
        config, _ = self.config()
        inputs = self.animals_inputs()
        ledger, budget = self.ledger(config, 4)
        config, path = self.config(budget=budget)
        output = self.root / "must-not-be-created"
        with patch.object(self.entry, "_make_lm", side_effect=AssertionError("check constructed an LM")):
            checked = self.entry.check(config, self.checkout, **inputs)
            self.assertTrue(checked["execution_available"])
            self.assertFalse(checked["execution_enabled"])
            self.assertFalse(checked["service_startup_checked"])
            for action in ("check", "run"):
                arguments = [action, "--config", str(path), "--sembench-checkout", str(self.checkout),
                    "--manifest", str(inputs["manifest"]), "--manifest-digest", inputs["manifest_digest"]]
                if action == "run":
                    arguments += ["--output-dir", str(output)]
                with redirect_stdout(StringIO()):
                    self.assertEqual(self.entry.main(arguments), 0)
        self.assertFalse(output.exists())
        self.assertEqual(ledger.snapshot()["units"][0]["claimed"], 0)

    def test_animals_original_queries_use_native_http_and_complete_uncached_inputs(self):
        inputs = self.animals_inputs()
        for query in ("Q1", "Q3", "Q7", "Q10"):
            with self.subTest(query=query):
                config, _ = self.config(query=query)
                replies = NativeHTTPReplies(self, "animals")
                report, directory, claim = self.invoke(config, inputs, replies)
                self.assertEqual(report["status"], "completed")
                self.assertEqual([stage["input_occurrences"] for stage in report["native_lm_calls"]],
                    [4, 4] if query == "Q7" else [4])
                self.assertEqual(report["model_calls"], 8 if query == "Q7" else 4)
                self.assertEqual(claim.attempts, report["model_calls"])
                self.assertEqual(report["quality"]["sembench_commit"], animals.SEMBENCH_COMMIT)
                self.assertEqual(report["quality"]["original_metric"][
                    "absolute_error" if query == "Q1" else "f1_score"], 0 if query == "Q1" else 1)
                self.assertFalse(report["model_process_owned"])
                self.assertIsNone(report["physical_gpu_exit_confirmed"])
                self.assertFalse(report["performance_claim"])
                self.assertEqual(report["retries"], 0)
                self.assert_audit(report, directory, replies)

    def test_mmqa_native_join_and_extract_keep_duplicate_images_and_json_format(self):
        inputs = self.mmqa_inputs()
        for query in ("Q2a", "Q2b", "Q7"):
            with self.subTest(query=query):
                config, _ = self.config("mmqa", query)
                replies = NativeHTTPReplies(self, "mmqa")
                report, directory, claim = self.invoke(config, inputs, replies)
                self.assertEqual(report["status"], "completed")
                self.assertEqual([stage["input_occurrences"] for stage in report["native_lm_calls"]],
                    [4, 4] if query == "Q2b" else [4])
                self.assertEqual(claim.attempts, 8 if query == "Q2b" else 4)
                self.assertEqual(report["quality"]["sembench_commit"], animals.SEMBENCH_COMMIT)
                self.assertEqual(report["inputs"]["join_strategy"], "zs-cot")
                self.assertEqual(report["inputs"]["native_join_strategy"], "ReasoningStrategy.ZS_COT")
                if query == "Q2b":
                    self.assertEqual([stage["kind"] for stage in report["native_lm_calls"]],
                        ["filter_or_join", "extract"])
                    extracts = [row["body"] for row in replies.requests
                        if row["body"].get("response_format") == {"type": "json_object"}]
                    self.assertEqual(len(extracts), 4)
                    self.assertEqual(len({NativeHTTPReplies.image_uris(body)[0] for body in extracts}), 1)
                    self.assertTrue(all(row["color"] == "red" for row in self.result_rows(report)))
                elif query == "Q7":
                    self.assertEqual({row["Airlines"] for row in self.result_rows(report)},
                        {"DomesticOnly", "EuropeanLine"})
                    self.assertTrue(report["inputs"]["original_source_difference"])
                self.assert_audit(report, directory, replies)

    def test_mmqa_native_parser_uses_final_answer_without_rewriting_reasoning(self):
        inputs = self.mmqa_inputs()
        for query in ("Q2a", "Q7"):
            with self.subTest(query=query):
                config, _ = self.config("mmqa", query)
                replies = NativeHTTPReplies(self, "mmqa", mode="distracting_reasoning")
                report, directory, claim = self.invoke(config, inputs, replies)
                self.assertEqual(report["status"], "completed")
                self.assertEqual(report["result"]["data"], [])
                self.assertEqual(claim.attempts, 4)
                self.assert_audit(report, directory, replies)

    def test_api_or_transport_failure_keeps_first_attempt_without_sdk_retries(self):
        inputs = self.animals_inputs(count=1)
        for failure in ("api", "timeout"):
            with self.subTest(failure=failure):
                config, _ = self.config(native_lm_max_batch_size=1)
                replies = NativeHTTPReplies(self, "animals", failure=failure)
                # Spare allowance prevents a retry from being hidden by budget exhaustion.
                with self.assertRaises(Exception) as caught:
                    self.invoke(config, inputs, replies, allowance=3)
                report = caught.exception.query_report
                self.assertEqual(report["status"], "failed")
                self.assertEqual(report["model_calls"], 1)
                self.assertEqual(self.last["claim"].attempts, 1)
                self.assertEqual(self.last["claim"].remaining, 2)
                self.assertEqual(len(replies.requests), 1)
                self.assertEqual(report["calls"]["failed"], 1)
                self.assertNotIn("quality", report)
                self.assert_audit(report, self.last["directory"], replies)

    def test_mismatched_response_model_does_not_qualify_a_query(self):
        config, _ = self.config(native_lm_max_batch_size=1)
        replies = NativeHTTPReplies(self, "animals", response_model="different-model")
        with self.assertRaisesRegex(RuntimeError, "response/model evidence") as caught:
            self.invoke(config, self.animals_inputs(count=1), replies)
        self.assertEqual(caught.exception.query_report["status"], "failed")
        self.assertEqual(caught.exception.query_report["calls"]["failed"], 1)

    def test_original_q2b_empty_join_or_empty_json_remains_a_recorded_keyerror(self):
        inputs = self.mmqa_inputs()
        for mode in ("no_matches", "empty_json"):
            with self.subTest(mode=mode):
                config, _ = self.config("mmqa", "Q2b")
                replies = NativeHTTPReplies(self, "mmqa", mode=mode)
                with self.assertRaises(KeyError) as caught:
                    self.invoke(config, inputs, replies)
                report = caught.exception.query_report
                self.assertEqual(report["status"], "failed")
                self.assertEqual(report["error_type"], "KeyError")
                self.assertEqual(report["model_calls"], 4 if mode == "no_matches" else 8)
                self.assertNotIn("result", report)
                self.assertNotIn("quality", report)
                self.assert_audit(report, self.last["directory"], replies)

    def test_worker_insufficient_full_query_allowance_stays_claimed_without_a_post(self):
        config, _ = self.config(query="Q7")
        inputs = self.animals_inputs()
        ledger, value = self.ledger(config, 7)
        config, _ = self.config(query="Q7", budget=value)
        replies = NativeHTTPReplies(self, "animals")
        with self.assertRaisesRegex(ValueError, "complete visual query"):
            self.invoke(config, inputs, replies, worker=True)
        self.assertFalse(replies.requests)
        snapshot = ledger.snapshot()
        self.assertEqual(snapshot["allocated_requests"], 7)
        self.assertEqual(snapshot["units"][0]["claimed"], 1)
        persisted = json.loads((self.last["directory"] / "query.json").read_text())
        self.assertEqual(persisted["status"], "failed")
        self.assertEqual(persisted["model_calls"], 0)
        self.assertEqual(persisted["permanent_budget"], snapshot)

    def test_worker_uses_the_existing_reserved_unit_and_writes_private_final_evidence(self):
        config, _ = self.config()
        inputs = self.animals_inputs()
        ledger, value = self.ledger(config, 4)
        config, _ = self.config(budget=value)
        replies = NativeHTTPReplies(self, "animals")
        report, directory, _ = self.invoke(config, inputs, replies, worker=True)
        self.assertEqual(report["status"], "completed")
        self.assertEqual(ledger.snapshot()["allocated_requests"], 4)
        self.assertEqual(ledger.snapshot()["units"][0]["claimed"], 1)
        self.assertEqual(json.loads((directory / "query.json").read_text())["status"], "completed")
        self.assertEqual((directory / "query.json").stat().st_mode & 0o777, 0o600)
        self.assert_audit(report, directory, replies)

    def test_missing_api_key_does_not_claim_the_already_reserved_worker_unit(self):
        config, _ = self.config()
        inputs = self.animals_inputs()
        ledger, value = self.ledger(config, 4)
        config, _ = self.config(budget=value)
        replies = NativeHTTPReplies(self, "animals")
        with patch.dict(os.environ, {}), \
                patch.object(self.entry, "_make_lm", side_effect=AssertionError("missing key allocated LM")):
            os.environ.pop(KEY_ENV, None)
            with self.assertRaises(ValueError):
                self.invoke(config, inputs, replies, worker=True)
        self.assertFalse(replies.requests)
        self.assertEqual(ledger.snapshot()["allocated_requests"], 4)
        self.assertEqual(ledger.snapshot()["units"][0]["claimed"], 0)

    def test_global_json_generation_override_is_rejected_before_any_model_call(self):
        with self.assertRaisesRegex(ValueError, "original Extract"):
            self.config(generation=dict(max_tokens=32, response_format={"type": "json_object"}))

    def test_native_generation_defaults_and_explicit_stop_are_normalized_before_runtime(self):
        base, _ = self.config()
        for supplied, maximum, stop in (({}, 8192, []), ({"max_tokens": 19}, 19, []),
                ({"stop": ["END"]}, 8192, ["END"]), ({"max_tokens": 32, "stop": []}, 32, [])):
            with self.subTest(generation=supplied):
                path = self.root / ("generation-" + str(next(self.serial)) + ".json")
                write_private_json(path, dict(base, generation=supplied))
                config, generation = self.entry.configuration(path)
                self.assertEqual(generation.max_tokens, maximum)
                self.assertEqual(list(generation.stop), stop)
                self.assertEqual(config["generation"]["max_tokens"], maximum)
                self.assertEqual(config["generation"]["stop"], stop)
                lm = self.entry._make_lm(config)
                self.assertEqual(lm.max_tokens, maximum)
                self.assertEqual(lm.kwargs["max_completion_tokens"], maximum)
                self.assertEqual(lm.kwargs["stop"], stop)

    def test_audit_close_failure_cannot_leave_a_completed_report_or_replace_its_error_fields(self):
        from src.observability.buffered_events import BufferedEvents
        config, _ = self.config()
        inputs = self.animals_inputs()
        replies = NativeHTTPReplies(self, "animals")
        original = BufferedEvents.close

        def fail_after_drain(writer):
            original(writer)
            raise OSError("software fixture audit close failure")

        with patch.object(BufferedEvents, "close", fail_after_drain), self.assertRaises(OSError) as caught:
            self.invoke(config, inputs, replies)
        report = caught.exception.query_report
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["error_type"], "OSError")
        self.assertIn("audit close failure", report["error"])
        self.assertEqual(report["calls"]["succeeded"], 4)
        self.assertEqual(report["model_calls"], self.last["claim"].attempts)


if __name__ == "__main__":
    unittest.main()
