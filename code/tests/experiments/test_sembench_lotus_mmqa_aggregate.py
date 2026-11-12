"""CPU integration of unmodified Q5/sem_agg/LM with software HTTP responses."""

from collections import Counter
from contextlib import contextmanager, redirect_stdout
import csv
import hashlib
import importlib.util
from importlib.metadata import PackageNotFoundError
from io import StringIO
import itertools
import json
import os
from pathlib import Path
import socket
import tempfile
import threading
import unittest
from unittest.mock import patch

from src.infrastructure.private_artifacts import content_digest, write_private_json


CHECKOUT = os.environ.get("SEMLOOM_NATIVE_TEST_CHECKOUT")
REAL_DATA = os.environ.get("SEMLOOM_MMQA_SF25_DATA")
HAS_SDK = all(importlib.util.find_spec(name) is not None for name in
    ("lotus", "litellm", "httpx", "pandas", "tokenizers", "duckdb", "cdlib", "overrides"))
KEY_ENV, KEY_VALUE = "SEMLOOM_AGGREGATE_FIXTURE_KEY", "aggregate-test-key"


class NativeAggregateReplies:
    def __init__(self, case, *, mode="compact", failure=None, response_model="local-aggregate-fixture"):
        self.case, self.mode, self.failure, self.response_model = case, mode, failure, response_model
        self.requests, self.lock = [], threading.Lock()

    def respond(self, request):
        import httpx

        body = json.loads(request.content)
        self.case.assertEqual(request.method, "POST")
        self.case.assertEqual(str(request.url), "http://localhost:18474/v1/chat/completions")
        self.case.assertEqual(request.headers["authorization"], "Bearer " + KEY_VALUE)
        self.case.assertEqual(body["model"], "local-aggregate-fixture")
        self.case.assertNotIn("response_format", body)
        self.case.assertEqual(body.get("stop", []), [])
        self.case.assertNotIn("EXCLUDED_SOURCE_MARKER", json.dumps(body))
        self.case.assertIn("Simply give the name of the actor.", body["messages"][0]["content"])
        with self.lock:
            request_id = "chatcmpl-aggregate-fixture-" + str(len(self.requests))
            self.requests.append(dict(body=body, payload=request.content))
        if self.failure == "timeout":
            raise httpx.ReadTimeout("software aggregation transport timeout", request=request)
        if self.failure == "api":
            return httpx.Response(503, request=request,
                json={"error": {"message": "software aggregation API failure", "type": "server_error"}})
        prompt = body["messages"][0]["content"]
        answer = "Lizzy Caplan"
        if self.mode == "long_summaries" and ("\n\tDocument " in prompt or prompt.count("\n\tSource ") > 2):
            answer += " " + "detail " * 90
        if self.mode == "wrong_actor":
            answer = "Another Actor"
        output_tokens = len(answer.split())  # Exact for the declared software WhitespaceSplit tokenizer.
        self.case.assertLessEqual(output_tokens, body.get("max_tokens", body.get("max_completion_tokens")))
        return httpx.Response(200, request=request, json=dict(id=request_id, object="chat.completion", created=1,
            model=self.response_model, choices=[dict(index=0, finish_reason="stop",
                message=dict(role="assistant", content=answer))],
            usage=dict(prompt_tokens=11, completion_tokens=output_tokens, total_tokens=11 + output_tokens)))

    @contextmanager
    def install(self):
        import httpx

        transport = httpx.MockTransport(self.respond)
        try:
            with patch.object(httpx.HTTPTransport, "handle_request", lambda _transport, request: transport.handle_request(request)):
                yield self
        finally:
            transport.close()


@unittest.skipUnless(CHECKOUT and HAS_SDK, "requires pinned SemBench source and the real native LOTUS SDK")
class NativeMMQAAggregateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from src.baselines.text.sembench_source import verify_checkout
        from src.experiments import sembench_lotus_mmqa_aggregate

        cls.entry, cls.checkout = sembench_lotus_mmqa_aggregate, verify_checkout(CHECKOUT)

    def setUp(self):
        from tokenizers import Tokenizer, models, pre_tokenizers
        from src.serving.backends.vllm_async_engine import VllmAsyncEngine
        from src.serving.backends.vllm_engine import VllmEngine

        self.temporary = tempfile.TemporaryDirectory(prefix="mmqa-aggregate-cpu-")
        self.addCleanup(self.temporary.cleanup)
        self.root, self.serial = Path(self.temporary.name), itertools.count()
        self.tokenizer_path = self.root / "software-tokenizer.json"
        tokenizer = Tokenizer(models.WordLevel(vocab={"[UNK]": 0, "detail": 1, "Lizzy": 2, "Caplan": 3}, unk_token="[UNK]"))
        tokenizer.pre_tokenizer = pre_tokenizers.WhitespaceSplit()
        tokenizer.save(str(self.tokenizer_path))
        self.guard_attempts = dict(network=0, real_engine=0)
        original_connect = socket.socket.connect

        def forbid_network(sock, address):
            if sock.family in (socket.AF_INET, socket.AF_INET6):
                self.guard_attempts["network"] += 1
                self.fail("CPU aggregation attempted an actual IP connection")
            return original_connect(sock, address)

        def forbid_engine(*_args, **_kwargs):
            self.guard_attempts["real_engine"] += 1
            self.fail("CPU aggregation constructed an actual model engine")

        for scope in (patch.dict(os.environ, {KEY_ENV: KEY_VALUE}),
                patch.object(socket.socket, "connect", forbid_network),
                patch.object(VllmAsyncEngine, "__init__", forbid_engine),
                patch.object(VllmEngine, "__init__", forbid_engine)):
            scope.start()
            self.addCleanup(scope.stop)
        self.addCleanup(lambda: self.assertEqual(self.guard_attempts, dict(network=0, real_engine=0)))

    def directory(self, label):
        directory = self.root / (label + "-" + str(next(self.serial)))
        directory.mkdir(mode=0o700)
        return directory

    def source(self, *, selected=16, words=80):
        directory = self.directory("input")
        with (directory / "lizzy_caplan_text_data.csv").open("w", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(("title", "text"))
            writer.writerow(("Unrelated Feature", "EXCLUDED_SOURCE_MARKER"))
            for index in range(selected):
                writer.writerow(("Crashing" if index % 2 else "Cobweb", "Lizzy Caplan appears. " + "description " * words))
        return directory

    def config(self, **changes):
        value = dict(schema=self.entry.SCHEMA, query_id="cpu-mmqa-q5", model_name="openai/local-aggregate-fixture",
            api_base="http://localhost:18474/v1", api_key_env=KEY_ENV,
            generation=dict(max_tokens=32, temperature=0, top_p=1, seed=0),
            native_lm_max_batch_size=1, native_lm_max_ctx_len=32768, query_timeout_s=60,
            token_counter=dict(kind="huggingface_tokenizer_file", path=str(self.tokenizer_path),
                sha256=hashlib.sha256(self.tokenizer_path.read_bytes()).hexdigest(),
                tokenizer_id="software-word-counter", tokenizer_revision="fixture-v1"),
            service_profile=dict(role="software HTTP fixture", max_model_len=32768,
                context_source="explicit deterministic CPU service profile", tokenizer_id="software-word-counter",
                tokenizer_revision="fixture-v1", model_revision="fixture", dtype="fixture"))
        value.update(changes)
        path = self.root / ("config-" + str(next(self.serial)) + ".json")
        write_private_json(path, value)
        config, _generation = self.entry.configuration(path)
        return config, path

    def multi_config(self):
        config, _path = self.config()
        return self.config(native_lm_max_ctx_len=600, generation=dict(config["generation"], max_tokens=128),
            service_profile=dict(config["service_profile"], max_model_len=600))

    def ledger(self, config, requests):
        from src.experiments.attempt_ledger import AttemptBudget
        from src.experiments.cell_budget import CellBudgetLedger

        path = self.root / ("budget-" + str(next(self.serial)) + ".sqlite")
        ledger = CellBudgetLedger.create(path, AttemptBudget("cpu-mmqa-aggregate", requests), deadline_utc=None)
        ledger.reserve_unit(config["query_id"], requests)
        return ledger, dict(path=str(path), id="cpu-mmqa-aggregate", limit=requests, unit_id=config["query_id"])

    def invoke(self, config, data_dir, replies, *, allowance=1, worker=False):
        directory = self.directory("query")
        claim = None
        if not worker:
            ledger, _value = self.ledger(config, allowance)
            claim = ledger.claim_unit(config["query_id"])
            claim.bind_execution()
        self.last = dict(directory=directory, claim=claim)
        with replies.install():
            report = (self.entry.run_worker(config, self.checkout, directory, data_dir=data_dir) if worker else
                self.entry.run_query(config, self.checkout, directory, budget=claim, data_dir=data_dir))
        return report, directory, claim

    def assert_audit(self, report, directory, replies):
        self.assertIsInstance(report["source_started_ns"], int)
        if report["sink_ready_ns"] is None:
            self.assertIsNone(report["source_to_sink_seconds"])
        else:
            self.assertEqual(report["source_to_sink_seconds"],
                (report["sink_ready_ns"] - report["source_started_ns"]) / 1e9)
            self.assertLessEqual(report["sink_ready_ns"], report["result_serialization_started_ns"])
        events = [json.loads(line) for line in (directory / "native-http.jsonl").read_text().splitlines()]
        requests = [event for event in events if event["event"] == "request"]
        self.assertEqual(Counter(event["request_values_sha256"] for event in requests),
            Counter(content_digest(request["body"]) for request in replies.requests))
        self.assertEqual(Counter(event["request_bytes_sha256"] for event in requests),
            Counter(hashlib.sha256(request["payload"]).hexdigest() for request in replies.requests))
        self.assertEqual(report["model_calls"], len(requests))
        self.assertTrue(report["event_writer"]["closed"])
        self.assertNotIn(KEY_VALUE, json.dumps(events) + json.dumps(report))

    def test_check_reaches_native_full_leaf_prompt_without_http_key_or_claim(self):
        from lotus.models import LM

        config, path = self.config()
        data = self.source()
        ledger, value = self.ledger(config, 1)
        config, path = self.config(budget=value)
        output = self.root / "no-execution-output"
        with patch.dict(os.environ, {}), patch.object(LM, "_process_uncached_messages", side_effect=AssertionError("preflight inferred")):
            os.environ.pop(KEY_ENV, None)
            checked = self.entry.check(config, self.checkout, data_dir=data)
            self.assertEqual(checked["native_plan"]["source_rows"], 17)
            self.assertEqual(checked["native_plan"]["selection"]["rows"], 16)
            self.assertEqual(checked["native_plan"]["selection"]["operator_args"], [])
            self.assertEqual(checked["native_plan"]["selection"]["operator_kwargs"], {})
            self.assertEqual(checked["work"]["expected_model_calls"], 1)
            self.assertEqual(checked["work"]["maximum_model_calls"], 1)
            self.assertEqual(checked["model_calls"], 0)
            self.assertFalse(checked["token_counter"]["model_tokenizer_verified"])
            self.assertTrue(checked["native_plan"]["token_count_calls"])
            prompt = checked["native_plan"]["leaf_messages"][0][0]["content"]
            self.assertIn("multiple documents", prompt)
            self.assertIn("\n\tDocument 16:", prompt)
            self.assertIn("[Title]:", prompt)
            self.assertIn("[Text]:", prompt)
            self.assertNotIn("EXCLUDED_SOURCE_MARKER", prompt)
            for action in ("check", "run"):
                arguments = [action, "--config", str(path), "--sembench-checkout", str(self.checkout), "--data-dir", str(data)]
                if action == "run":
                    arguments += ["--output-dir", str(output)]
                with redirect_stdout(StringIO()):
                    self.assertEqual(self.entry.main(arguments), 0)
        self.assertFalse(output.exists())
        self.assertEqual(ledger.snapshot()["units"][0]["claimed"], 0)

    def test_single_leaf_uses_original_projection_parser_and_author_json_evaluator(self):
        config, _path = self.config()
        for mode, expected_f1 in (("compact", 1.0), ("wrong_actor", 0.0)):
            with self.subTest(mode=mode):
                replies = NativeAggregateReplies(self, mode=mode)
                report, directory, claim = self.invoke(config, self.source(), replies)
                self.assertEqual(report["status"], "completed")
                self.assertEqual(report["result"]["columns"], ["_output"])
                self.assertEqual(report["result"]["data"], [["Lizzy Caplan" if mode == "compact" else "Another Actor"]])
                self.assertEqual(report["quality"]["original_metric"]["f1_score"], expected_f1)
                self.assertEqual(report["native_lm_calls"], [dict(input_occurrences=1, kind="aggregate", post_attempts=1)])
                self.assertEqual(claim.attempts, 1)
                self.assertEqual(replies.requests[0]["body"]["messages"], report["preflight"]["native_plan"]["leaf_messages"][0])
                self.assert_audit(report, directory, replies)

    def test_multilevel_uses_native_summary_prompts_and_output_dependent_request_count(self):
        config, _path = self.multi_config()
        data = self.source()
        checked = self.entry.check(config, self.checkout, data_dir=data)
        self.assertGreater(checked["work"]["leaf_requests"], 1)
        self.assertIsNone(checked["work"]["expected_model_calls"])
        self.assertIsNone(checked["work"]["maximum_model_calls"])
        compact = NativeAggregateReplies(self)
        short_report, short_directory, _claim = self.invoke(config, data, compact, allowance=40)
        replies = NativeAggregateReplies(self, mode="long_summaries")
        report, directory, claim = self.invoke(config, data, replies, allowance=40)
        self.assertEqual(report["status"], "completed")
        self.assertGreaterEqual(len(report["native_lm_calls"]), 3)
        self.assertGreater(report["model_calls"], short_report["model_calls"])
        self.assertEqual(report["native_lm_calls"][-1]["input_occurrences"], 1)
        nodes = [request["body"]["messages"][0]["content"] for request in replies.requests
                 if "\n\tSource " in request["body"]["messages"][0]["content"]]
        self.assertTrue(nodes)
        self.assertTrue(all("multiple sources" in prompt for prompt in nodes))
        self.assertTrue(any("detail" in prompt for prompt in nodes))
        # The original parser retains the final native output verbatim, and
        # the original evaluator rejects extra answer text rather than fixing it.
        self.assertEqual(report["result"]["data"], [["Lizzy Caplan " + "detail " * 90]])
        self.assertEqual(report["quality"]["original_metric"]["f1_score"], 0.0)
        self.assertEqual(report["model_calls"], claim.attempts)
        self.assert_audit(short_report, short_directory, compact)
        self.assert_audit(report, directory, replies)

    def test_finite_budget_stops_reduction_without_refund_or_hidden_request(self):
        config, _path = self.multi_config()
        data = self.source()
        minimum = self.entry.check(config, self.checkout, data_dir=data)["work"]["minimum_model_calls"]
        replies = NativeAggregateReplies(self, mode="long_summaries")
        with self.assertRaises(Exception) as caught:
            self.invoke(config, data, replies, allowance=minimum)
        report = caught.exception.query_report
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["model_calls"], minimum)
        self.assertEqual(self.last["claim"].remaining, 0)
        self.assertEqual(len(replies.requests), minimum)
        self.assertNotIn("quality", report)
        self.assert_audit(report, self.last["directory"], replies)

    def test_transport_and_api_failure_preserve_one_attempt_without_retry(self):
        config, _path = self.config()
        for failure in ("timeout", "api"):
            with self.subTest(failure=failure):
                replies = NativeAggregateReplies(self, failure=failure)
                with self.assertRaises(Exception) as caught:
                    self.invoke(config, self.source(selected=1), replies, allowance=3)
                report = caught.exception.query_report
                self.assertEqual(report["model_calls"], 1)
                self.assertEqual(report["calls"]["failed"], 1)
                self.assertEqual(self.last["claim"].attempts, 1)
                self.assertEqual(self.last["claim"].remaining, 2)
                self.assertNotIn("quality", report)
                self.assert_audit(report, self.last["directory"], replies)

    def test_response_model_mismatch_does_not_qualify_query(self):
        config, _path = self.config()
        replies = NativeAggregateReplies(self, response_model="another-model")
        with self.assertRaisesRegex(RuntimeError, "response/model evidence") as caught:
            self.invoke(config, self.source(), replies)
        self.assertEqual(caught.exception.query_report["calls"]["failed"], 1)

    def test_worker_cli_uses_one_fresh_native_preflight_and_lm(self):
        config, _path = self.config()
        ledger, value = self.ledger(config, 1)
        config, path = self.config(budget=value)
        data, directory = self.source(selected=1), self.directory("worker-cli")
        replies = NativeAggregateReplies(self)
        with replies.install(), patch.object(self.entry, "check", wraps=self.entry.check) as check, \
                patch.object(self.entry, "_make_lm", wraps=self.entry._make_lm) as make_lm:
            self.assertEqual(self.entry.main(["run", "--config", str(path), "--sembench-checkout", str(self.checkout),
                "--data-dir", str(data), "--output-dir", str(directory), "--execute-model", "--worker"]), 0)
        self.assertEqual(check.call_count, 1)
        self.assertEqual(make_lm.call_count, 1)
        self.assertEqual(ledger.snapshot()["units"][0]["claimed"], 1)
        report = json.loads((directory / "query.json").read_text())
        self.assertEqual(report["status"], "completed")
        self.assertEqual(report["model_calls"], 1)
        self.assert_audit(report, directory, replies)

    def test_worker_claim_is_permanent_and_cannot_be_replayed(self):
        config, _path = self.config()
        ledger, value = self.ledger(config, 1)
        config, _path = self.config(budget=value)
        replies = NativeAggregateReplies(self)
        report, directory, _claim = self.invoke(config, self.source(), replies, worker=True)
        self.assertEqual(report["status"], "completed")
        self.assertEqual(report["permanent_budget"], ledger.snapshot())
        self.assertEqual(ledger.snapshot()["allocated_requests"], 1)
        self.assertEqual(ledger.snapshot()["units"][0]["claimed"], 1)
        self.assertEqual(json.loads((directory / "query.json").read_text())["status"], "completed")
        self.assertEqual((directory / "query.json").stat().st_mode & 0o777, 0o600)
        with self.assertRaises(Exception):
            self.invoke(config, self.source(), replies, worker=True)
        self.assertEqual(len(replies.requests), 1)

    def test_insufficient_minimum_stays_claimed_with_no_post(self):
        config, _path = self.multi_config()
        ledger, value = self.ledger(config, 1)
        config, _path = self.multi_config()
        config["budget"] = value
        replies = NativeAggregateReplies(self)
        with self.assertRaisesRegex(ValueError, "native minimum request count"):
            self.invoke(config, self.source(), replies, worker=True)
        self.assertFalse(replies.requests)
        self.assertEqual(ledger.snapshot()["allocated_requests"], 1)
        self.assertEqual(ledger.snapshot()["units"][0]["claimed"], 1)
        persisted = json.loads((self.last["directory"] / "query.json").read_text())
        self.assertEqual(persisted["model_calls"], 0)

    def test_missing_api_key_cannot_claim_reserved_unit(self):
        config, _path = self.config()
        ledger, value = self.ledger(config, 1)
        config, _path = self.config(budget=value)
        with patch.dict(os.environ, {}):
            os.environ.pop(KEY_ENV, None)
            with self.assertRaises(ValueError):
                self.invoke(config, self.source(), NativeAggregateReplies(self), worker=True)
        self.assertEqual(ledger.snapshot()["units"][0]["claimed"], 0)

    def test_generation_defaults_preserve_original_long_answer_and_no_stop(self):
        config, _path = self.config(generation={})
        self.assertEqual(config["generation"]["max_tokens"], 8192)
        self.assertEqual(config["generation"]["stop"], [])
        lm = self.entry._make_lm(config)
        self.assertEqual(lm.max_tokens, 8192)
        self.assertEqual(lm.max_ctx_len, 32768)
        self.assertEqual(lm.kwargs["stop"], [])
        self.assertEqual(lm.kwargs["num_retries"], 0)
        self.assertEqual(lm.kwargs["max_retries"], 0)
        with self.assertRaisesRegex(ValueError, "unstructured aggregation parser"):
            self.config(generation=dict(max_tokens=32, response_format={"type": "json_object"}))

    def test_original_default_chunking_precedes_leaf_batching(self):
        config, _path = self.multi_config()
        checked = self.entry.check(config, self.checkout, data_dir=self.source(selected=1, words=1000))
        plan = checked["native_plan"]
        self.assertEqual(plan["selection"]["rows"], 1)
        self.assertGreater(plan["native_documents"], 1)
        self.assertEqual(plan["long_context_strategy"], "chunk")
        self.assertGreater(checked["work"]["leaf_requests"], 1)

    def test_missing_distribution_metadata_and_optional_context_provenance_do_not_gate_the_native_api(self):
        config, _path = self.config()
        profile = dict(config["service_profile"])
        profile.pop("context_source")
        config, _path = self.config(service_profile=profile)
        with patch("src.semantic_methods.lotus.source.distribution", side_effect=PackageNotFoundError):
            checked = self.entry.check(config, self.checkout, data_dir=self.source())
        self.assertIsNone(checked["lotus_version"])
        self.assertIn("lotus-ai", checked["native_sources"]["unavailable_sources"])
        self.assertEqual(checked["native_sources"]["source_policy"], "record_only")
        self.assertEqual(checked["work"]["expected_model_calls"], 1)

    def test_counter_and_context_binding_cannot_fall_back_to_generic_defaults(self):
        config, _path = self.config()
        with self.assertRaisesRegex(ValueError, "service_profile.max_model_len"):
            self.config(native_lm_max_ctx_len=4096)
        with self.assertRaisesRegex(ValueError, "service tokenizer"):
            self.config(token_counter=dict(config["token_counter"], tokenizer_revision="unbound"))
        config, _path = self.config(token_counter=dict(config["token_counter"], sha256="0" * 64))
        with self.assertRaisesRegex(ValueError, "SHA256"):
            self.entry.check(config, self.checkout, data_dir=self.source())

    def test_audit_close_error_fails_query_and_remains_in_report(self):
        from src.observability.buffered_events import BufferedEvents

        config, _path = self.config()
        close = BufferedEvents.close

        def fail_after_drain(writer):
            close(writer)
            raise OSError("software aggregation audit close failure")

        replies = NativeAggregateReplies(self)
        with patch.object(BufferedEvents, "close", fail_after_drain), self.assertRaises(OSError) as caught:
            self.invoke(config, self.source(), replies)
        report = caught.exception.query_report
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["error_type"], "OSError")
        self.assertEqual(report["audit_cleanup_error"]["error_type"], "OSError")
        self.assertEqual(report["model_calls"], 1)

    def test_primary_transport_failure_and_audit_cleanup_error_both_survive(self):
        from src.observability.buffered_events import BufferedEvents

        config, _path = self.config()
        close = BufferedEvents.close

        def fail_after_drain(writer):
            close(writer)
            raise OSError("secondary software audit failure")

        replies = NativeAggregateReplies(self, failure="timeout")
        with patch.object(BufferedEvents, "close", fail_after_drain), self.assertRaises(Exception) as caught:
            self.invoke(config, self.source(selected=1), replies, allowance=3)
        report = caught.exception.query_report
        self.assertNotEqual(report["error_type"], "OSError")
        self.assertEqual(report["audit_cleanup_error"]["error_type"], "OSError")
        self.assertEqual(report["calls"]["failed"], 1)
        self.assertEqual(report["model_calls"], 1)
        self.assertTrue(any("audit close failed" in note for note in caught.exception.__notes__))

    @unittest.skipUnless(REAL_DATA, "requires the supplied real original sf25 text relations")
    def test_real_original_sf25_selects_q5_sixteen_and_evaluates_original_output(self):
        config, _path = self.config(generation={})
        replies = NativeAggregateReplies(self)
        report, directory, claim = self.invoke(config, REAL_DATA, replies)
        selected = report["preflight"]["native_plan"]["selection"]
        self.assertEqual(report["preflight"]["native_plan"]["source_rows"], 25)
        self.assertEqual(selected["rows"], 16)
        self.assertEqual(selected["text_utf8_bytes"], 12403)
        self.assertEqual(selected["text_characters"], 12395)
        self.assertEqual(selected["title_utf8_bytes"], 235)
        self.assertEqual(report["model_calls"], claim.attempts)
        self.assertEqual(report["model_calls"], 1)
        self.assertEqual(report["quality"]["original_metric"]["f1_score"], 1.0)
        self.assert_audit(report, directory, replies)


if __name__ == "__main__":
    unittest.main()
