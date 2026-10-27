"""CPU-only native SDK / mock HTTP transport checks; no inference or service."""

import csv
from copy import deepcopy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

os.environ.setdefault("LITELLM_LOCAL_MODEL_COST_MAP", "true")
os.environ.setdefault("HF_HUB_OFFLINE", "1")
os.environ.setdefault("TRANSFORMERS_OFFLINE", "1")

from src.baselines.text.frameworks import lotus_native_local_model as native
from src.baselines.text.movie_records import MovieRecordSource, prepare_movie_source
from src.experiments.cell_budget import ClaimedUnit, Reservation
from src.baselines.text.movie_tasks import MovieTask
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec, _prepared

LIBRARIES = all(importlib.util.find_spec(name) is not None for name in ("lotus", "duckdb", "pandas", "cdlib"))
CHECKOUT = os.environ.get("SEMBENCH_CHECKOUT")
REAL_LOCAL_SERVICE = native.LocalService


def source_fixture(directory):
    path = directory / "Reviews.csv"
    with path.open("w", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["reviewId", "id", "reviewText", "scoreSentiment"])
        writer.writerows([["duplicate", "taken_3", "good", "POSITIVE"],
            ["duplicate", "other", "excluded", "NEGATIVE"],
            ["duplicate", "taken_3", "bad", "NEGATIVE"],
            ["duplicate", "taken_3", "good", "POSITIVE"],
            ["empty", "taken_3", "", "NEGATIVE"]])
    manifest = prepare_movie_source(path, directory / "source",
        expected_csv_sha256=hashlib.sha256(path.read_bytes()).hexdigest(), max_rows=5)
    return MovieRecordSource.from_manifest(manifest)


class FixtureService:
    """No socket, process, GPU library or model construction occurs."""
    instances = []
    close_confirmed = True
    def __init__(self, *args):
        self.endpoint = "http://127.0.0.1:39281/v1/chat/completions"
        self.engine_uid = "software-fixture-" + str(len(self.instances))
        self.cuda_device = args[-1]
        self.closed = False
        self.instances.append(self)
    def start(self):
        return dict(engine_uid=self.engine_uid, no_inference=True, software_fixture=True, pid=None,
                    template_application="fixture receives the untemplated native conversation")
    def verify_listener(self):
        pass
    def close(self):
        self.closed = True
        return dict(status="closed" if self.close_confirmed else "unknown",
                    engine_uid=self.engine_uid,
                    process_exit_confirmed=self.close_confirmed, gpu_exit_confirmed=None,
                    software_fixture=True)


class WireFixture:
    def __init__(self, failure=None, malformed=False):
        self.requests, self.responses = [], []
        self.failure, self.malformed = failure, malformed
        self.token_fault = None
        self.active = self.peak = 0
        self.lock = threading.Lock()
    def __call__(self, request):
        import httpx
        body = json.loads(request.content)
        with self.lock:
            ordinal = len(self.requests)
            self.requests.append(body)
            self.active += 1
            self.peak = max(self.peak, self.active)
        time.sleep(0.005)  # overlap proves the actual native thread pool is used
        content = json.dumps(body["messages"], ensure_ascii=False)
        positive = "good" in content
        response = dict(id="fixture-" + str(ordinal), model=body["model"], created=1,
            object="chat.completion", choices=[dict(index=0, finish_reason="stop",
                message=dict(role="assistant", content="Answer: True" if positive else "Answer: False"))],
            usage=dict(prompt_tokens=13 if positive else 11, completion_tokens=2,
                       total_tokens=15 if positive else 13))
        # Synthetic transport IDs deliberately differ from encoding the text.
        response["prompt_token_ids"] = [71] * response["usage"]["prompt_tokens"]
        response["choices"][0]["token_ids"] = [91, 93]
        if self.token_fault == "missing":
            response.pop("prompt_token_ids")
            response["choices"][0].pop("token_ids")
        elif self.token_fault == "bool":
            response["choices"][0]["token_ids"] = [True, 93]
        elif self.token_fault == "length":
            response["choices"][0]["token_ids"] = [91]
        status = 200
        if self.failure == ordinal:
            status = 500
            response = dict(response, error=dict(message="fixture failure", type="server_error"))
        if self.malformed:
            response["usage"]["total_tokens"] += 1
        with self.lock:
            self.active -= 1
            self.responses.append(deepcopy(response))
        return httpx.Response(status, json=response, request=request)


@unittest.skipUnless(LIBRARIES and CHECKOUT, "requires pinned CPU SDKs and SEMBENCH_CHECKOUT")
class NativeLocalTests(unittest.TestCase):
    def setUp(self):
        import httpx
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = source_fixture(self.root)
        self.method = MovieTask(3).method("openai/local-fixture")
        self.generation = GenerationSpec(seed=0)
        self.engine = EngineSpec(model_path="/fixture/model", model_python="/fixture/model/bin/python",
            driver_python="/fixture/driver/bin/python", max_model_calls=4)
        self.resources = RecordsResources(query_timeout_s=5)
        self.profile = dict(native_lm_max_batch_size=4)
        self.output = self.root / "output"
        self.output.mkdir()
        self.lease = self.root / "lease.json"
        self.lease.write_text('{"owner":"cpu-fixture"}')
        self.budget = ClaimedUnit(Reservation("q", 1, 4), None)
        self.context = dict(output_dir=self.output, claimed_budget=self.budget,
            runtime_binding=dict(driver_python=self.engine.driver_python, model_python=self.engine.model_python,
                model_path=self.engine.model_path, cuda_visible_devices="0"),
            owner_context=dict(lease_path=str(self.lease), lease_sha256=hashlib.sha256(self.lease.read_bytes()).hexdigest()))
        FixtureService.instances = []
        FixtureService.close_confirmed = True
        self.wire = WireFixture()
        self.addCleanup(patch.stopall)
        popen = subprocess.Popen
        def readonly_git(command, *args, **kwargs):
            # numpy's import-time capability probe also runs the read-only
            # lscpu command (and handles its absence on macOS).
            allowed_inventory = (["fc-list", "--help"], ["fc-list", "--format=%{file}\\n"],
                                 ["system_profiler", "-xml", "SPFontsDataType"])
            if not (type(command) is list and command[0] == "git" or command == "lscpu"
                    or command in allowed_inventory):
                raise AssertionError("CPU fixture forbids model/service subprocesses")
            return popen(command, *args, **kwargs)
        patch.object(native, "LocalService", FixtureService).start()
        patch.object(EngineSpec, "validate_driver_environment", return_value={"software_fixture": True}).start()
        patch.object(httpx, "HTTPTransport", side_effect=lambda **kw: httpx.MockTransport(self.wire)).start()
        patch("socket.create_connection", side_effect=AssertionError("CPU fixture forbids sockets")).start()
        patch("socket.socket.connect", side_effect=AssertionError("CPU fixture forbids network connections")).start()
        patch("subprocess.Popen", side_effect=readonly_git).start()

    def run_query(self, observer=None):
        return native.run(self.source, CHECKOUT, self.method, self.generation, self.engine,
                          self.resources, query_id="q", profile=self.profile, observer=observer, context=self.context)

    def test_original_accessor_lm_thread_pool_decisions_usage_and_quality(self):
        import lotus
        import pandas as pd
        from src.baselines.text.frameworks.lotus_pg import observe_filter_rows
        from src.baselines.text.sembench_movie import run_original_lotus_query, evaluate_original
        from src.semantic_methods.lotus.engine import lm_report
        from lotus.models import LM
        import lotus.models.lm as lm_module
        uncached = LM._process_uncached_messages
        batch = lm_module.batch_completion
        events = []
        report = self.run_query(events.append)
        self.assertIs(LM._process_uncached_messages, uncached)
        self.assertIs(lm_module.batch_completion, batch)
        self.assertEqual(report["status"], "completed")
        self.assertEqual(report["result"], {"positive_review_cnt": 2})
        self.assertEqual(report["calls"]["counts"]["succeeded"], 4)
        self.assertEqual(self.budget.attempts, 4)
        self.assertEqual(len(self.wire.requests), 4)
        self.assertGreater(self.wire.peak, 1)
        self.assertLessEqual(self.wire.peak, 4)
        self.assertEqual(report["native_batch_invocations"], 1)
        self.assertTrue(report["owner_ready"]["no_inference"])
        self.assertEqual(events.count("result_ready"), 1)
        self.assertLess(events.index("result_ready"), events.index("evaluation_start"))
        self.assertLess(events.index("evaluation_end"), events.index("owner_closed"))
        self.assertTrue(FixtureService.instances[0].closed)
        self.assertTrue(report["backend_close"]["process_exit_confirmed"])
        self.assertEqual({r["engine_uid"] for r in report["calls"]["records"]}, {report["owner_ready"]["engine_uid"]})
        self.assertEqual(report["owner_ready"]["engine_uid"], report["backend_close"]["engine_uid"])
        self.assertFalse(lotus.settings.enable_cache)
        reference_decisions, reference_prompts = [], []
        client, lm = native._client_and_lm(self.method, self.generation, self.resources, self.profile,
                                         FixtureService.instances[0].endpoint)
        try:
            def load():
                return pd.DataFrame([dict(reviewId=r["reviewId"], id=r["id"], reviewText=r["reviewText"],
                    _execution_row_id=r["row_id"]) for r in self.source.rows()])
            with lotus.settings.context(lm=lm, enable_cache=False):
                with observe_filter_rows(reference_decisions.extend, reference_prompts.extend):
                    reference = run_original_lotus_query(CHECKOUT, 3, load)
        finally:
            client.close()
        self.assertEqual(int(reference.iloc[0, 0]), report["result"]["positive_review_cnt"])
        self.assertEqual(reference_decisions, [(r["row_id"], r["decision"]) for r in report["decisions"]])
        prepared, _ = _prepared([dict(identity, messages_json=json.dumps(prompt["messages"]))
            for identity, prompt in zip(self.source.expected("q"), reference_prompts, strict=True)])
        self.assertEqual([r["messages_sha256"] for r in prepared],
                         [r["messages_sha256"] for r in report["calls"]["records"]])
        self.assertEqual(lm_report(lm), report["lm"])
        self.assertEqual(evaluate_original(CHECKOUT, 3, reference, self.source.labeled_frame())["original_metric"],
                         report["quality"]["original_metric"])
        self.assertEqual(report["calls"]["usage"]["total_tokens"], 56)

    def test_http_failure_keeps_every_native_receipt_and_never_retries(self):
        self.wire.failure = 1
        with self.assertRaises(Exception) as caught:
            self.run_query()
        report = caught.exception.query_report
        self.assertEqual(report["status"], "failed")
        self.assertEqual(len(self.wire.requests), 4)
        self.assertEqual(self.budget.attempts, 4)
        self.assertEqual(report["calls"]["counts"]["recorded"], 4)
        self.assertEqual(report["calls"]["counts"]["succeeded"], 3)
        self.assertEqual(report["calls"]["counts"]["failed"], 1)
        self.assertEqual(report["calls"]["usage"]["known_completion_tokens"], 8)
        for row in report["calls"]["records"]:
            if row["status"] != "succeeded":
                self.assertNotIn("token_ids_source", row)
                self.assertNotIn("prompt_token_ids", row)
                self.assertNotIn("output_token_ids", row)
        self.assertNotIn("quality", report)
        self.assertTrue(FixtureService.instances[0].closed)

    def test_requests_observation_ids_and_preserves_authoritative_wire_arrays(self):
        report = self.run_query()
        self.assertTrue(all(body.get("return_token_ids") is True for body in self.wire.requests))
        by_id = {response["id"]: response for response in self.wire.responses}
        for row in report["calls"]["records"]:
            response = by_id[row["engine_request_id"]]
            self.assertEqual(row["prompt_token_ids"], response["prompt_token_ids"])
            self.assertEqual(row["output_token_ids"], response["choices"][0]["token_ids"])
            self.assertEqual(row["token_ids_source"], "vllm_http_response")
        self.assertEqual(report["result"], {"positive_review_cnt": 2})
        self.assertEqual(report["native_batch_invocations"], 1)
        self.assertEqual(self.budget.attempts, 4)
        self.assertGreater(self.wire.peak, 1)

    def _reject_token_receipt(self, fault):
        self.wire.token_fault = fault
        with self.assertRaises(Exception) as caught:
            self.run_query()
        report = caught.exception.query_report
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["calls"]["counts"]["failed"], 4)
        usage = report["calls"]["usage"]
        self.assertEqual(usage["known_prompt_tokens"] + usage["known_completion_tokens"], 56)
        self.assertEqual(len(report["http_receipts"]), 4)
        self.assertEqual(self.budget.attempts, 4)
        self.assertTrue(FixtureService.instances[0].closed)

    def test_missing_actual_ids_fails_without_retokenizing_or_losing_usage(self):
        self._reject_token_receipt("missing")

    def test_boolean_token_id_is_a_protocol_error(self):
        self._reject_token_receipt("bool")

    def test_actual_id_counts_must_agree_with_usage(self):
        self._reject_token_receipt("length")

    def test_controller_physical_gpu_uuid_is_forwarded_without_ordinal_remapping(self):
        device = "GPU-12345678-1234-5678-9abc-123456789abc"
        self.context["runtime_binding"]["cuda_visible_devices"] = device
        report = self.run_query()
        self.assertEqual(report["status"], "completed")
        self.assertEqual(FixtureService.instances[0].cuda_device, device)

    def test_transport_failure_keeps_successes_and_unknown_usage(self):
        original = self.wire.__call__
        import httpx
        def interrupted(request):
            if "bad" in request.content.decode():
                raise httpx.ReadTimeout("fixture outcome unknown", request=request)
            return original(request)
        with patch.object(httpx, "HTTPTransport", side_effect=lambda **kw: httpx.MockTransport(interrupted)):
            with self.assertRaises(Exception) as caught:
                self.run_query()
        report = caught.exception.query_report
        self.assertEqual(report["calls"]["counts"]["succeeded"], 3)
        self.assertEqual(report["calls"]["counts"]["unknown"], 1)
        self.assertIsNone(report["calls"]["usage"]["total_tokens"])
        self.assertEqual(report["calls"]["usage"]["known_completion_tokens"], 6)
        self.assertEqual(self.budget.attempts, 4)

    def test_bad_authoritative_usage_fails_even_if_native_parser_accepts_text(self):
        self.wire.malformed = True
        with self.assertRaises(ValueError) as caught:
            self.run_query()
        report = caught.exception.query_report
        self.assertEqual(report["calls"]["counts"]["failed"], 4)
        self.assertEqual(report["calls"]["usage"]["known_prompt_tokens"], 48)
        self.assertTrue(report["call_protocol_errors"] or report["status"] == "failed")

    def test_unconfirmed_cleanup_fails_with_completed_known_result_retained(self):
        FixtureService.close_confirmed = False
        with self.assertRaisesRegex(RuntimeError, "exit remains unconfirmed") as caught:
            self.run_query()
        report = caught.exception.query_report
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["result"], {"positive_review_cnt": 2})
        self.assertEqual(report["calls"]["counts"]["succeeded"], 4)
        self.assertIn("original_metric", report["quality"])

    def test_invalid_profile_budget_or_lease_precedes_service_construction(self):
        for failure in ("profile", "budget", "lease"):
            profile = dict(self.profile)
            if failure == "profile":
                profile["max_active_batches"] = 1
            elif failure == "budget":
                self.budget.reserve("0" * 64)
            else:
                self.context["owner_context"]["lease_sha256"] = "0" * 64
            with self.subTest(failure=failure), self.assertRaises(ValueError) as caught:
                native.run(self.source, CHECKOUT, self.method, self.generation, self.engine,
                    self.resources, query_id="q", profile=profile, context=self.context)
            self.assertEqual(caught.exception.query_report["status"], "failed")
            self.assertEqual(FixtureService.instances, [])
            self.budget = self.context["claimed_budget"] = ClaimedUnit(Reservation("q", 1, 4), None)

    def test_evaluator_failure_retains_count_usage_and_restores_observers(self):
        from lotus.models import LM
        import lotus.models.lm as lm_module
        original = LM.__call__, lm_module.batch_completion
        events = []
        with patch.object(native, "evaluate_original", side_effect=RuntimeError("fixture evaluation failed")):
            with self.assertRaisesRegex(RuntimeError, "evaluation failed") as caught:
                self.run_query(events.append)
        self.assertEqual(caught.exception.query_report["result"], {"positive_review_cnt": 2})
        self.assertEqual(caught.exception.query_report["calls"]["counts"]["succeeded"], 4)
        self.assertIs(LM.__call__, original[0])
        self.assertIs(lm_module.batch_completion, original[1])
        self.assertIn("owner_closed", events)
        self.assertIn("evaluation_end", events)

    def test_result_observer_failure_still_retains_complete_count_and_usage(self):
        def observer(event):
            if event == "result_ready":
                raise RuntimeError("fixture observer failed")
        with self.assertRaisesRegex(RuntimeError, "observer failed") as caught:
            self.run_query(observer)
        self.assertEqual(caught.exception.query_report["result"], {"positive_review_cnt": 2})
        self.assertEqual(caught.exception.query_report["calls"]["counts"]["succeeded"], 4)

    def test_final_accounting_failure_keeps_primary_and_validated_receipts(self):
        original = native._Calls.ledger
        invocations = []
        def broken(calls, failure):
            invocations.append(failure)
            if len(invocations) == 2:
                raise ValueError("fixture final accounting failed")
            return original(calls, failure)
        with patch.object(native._Calls, "ledger", broken), \
             patch.object(native, "evaluate_original", side_effect=RuntimeError("fixture original evaluation failed")):
            with self.assertRaisesRegex(RuntimeError, "original evaluation failed") as caught:
                self.run_query()
        report = caught.exception.query_report
        self.assertEqual(report["calls"]["counts"]["succeeded"], 4)
        self.assertEqual(report["calls"]["usage"]["total_tokens"], 56)
        self.assertEqual(report["call_accounting_error"], "ValueError")
        self.assertTrue(report["backend_close"]["process_exit_confirmed"])

    def test_evaluation_and_end_observer_failure_preserves_original_error(self):
        def observer(event):
            if event == "evaluation_end":
                raise ValueError("fixture evaluation end observer failed")
        with patch.object(native, "evaluate_original", side_effect=RuntimeError("fixture original evaluation failed")):
            with self.assertRaisesRegex(RuntimeError, "original evaluation failed") as caught:
                self.run_query(observer)
        self.assertEqual(caught.exception.query_report["calls"]["counts"]["succeeded"], 4)
        self.assertTrue(any("evaluation_end observer also failed" in n for n in caught.exception.__notes__))

    def test_empty_original_query_returns_zero_without_service_or_inference(self):
        directory = self.root / "empty"
        directory.mkdir()
        path = directory / "Reviews.csv"
        path.write_text("reviewId,id,reviewText,scoreSentiment\n")
        manifest = prepare_movie_source(path, directory / "prepared",
            expected_csv_sha256=hashlib.sha256(path.read_bytes()).hexdigest(), max_rows=1)
        self.source = MovieRecordSource.from_manifest(manifest)
        self.context["claimed_budget"] = ClaimedUnit(Reservation("q", 1, 0), None)
        report = self.run_query()
        self.assertEqual(report["result"], {"positive_review_cnt": 0})
        self.assertEqual(report["calls"]["usage"]["total_tokens"], 0)
        self.assertEqual(self.wire.requests, [])
        self.assertEqual(FixtureService.instances, [])

    @unittest.skipUnless(os.environ.get("SEMBENCH_Q3_MANIFEST"), "requires the fixed 2000-row Q3 source manifest")
    def test_full_120_occurrence_native_http_profiles_match_original_q3(self):
        import httpx
        import lotus
        import pandas as pd
        from src.baselines.text.frameworks.lotus_pg import observe_filter_rows
        from src.baselines.text.sembench_movie import run_original_lotus_query, evaluate_original
        from src.semantic_methods.lotus.engine import lm_report
        self.source = MovieRecordSource.from_manifest(os.environ["SEMBENCH_Q3_MANIFEST"])
        self.method = MovieTask(3).method("openai/qwen2.5-7b")
        self.assertEqual(len(self.source.rows()), 2000)
        self.assertEqual(len(self.source.expected("q")), 120)
        self.engine = EngineSpec(**dict(self.engine.__dict__, max_model_calls=120))
        received = []
        lock = threading.Lock()
        def fixture(request):
            with lock:
                ordinal = len(received)
                received.append(json.loads(request.content))
            return httpx.Response(200, json=dict(id="fixture-full-" + str(ordinal), model=json.loads(request.content)["model"],
                created=1, object="chat.completion", prompt_token_ids=[71], choices=[dict(index=0, finish_reason="stop", token_ids=[91],
                    message=dict(role="assistant", content="False"))],
                usage=dict(prompt_tokens=1, completion_tokens=1, total_tokens=2)), request=request)
        with patch.object(httpx, "HTTPTransport", side_effect=lambda **kw: httpx.MockTransport(fixture)):
            for concurrency in (4, 8):
                self.profile = dict(native_lm_max_batch_size=concurrency)
                self.context["claimed_budget"] = ClaimedUnit(Reservation("q", 1, 120), None)
                self.context["output_dir"] = self.root / ("full-output-" + str(concurrency))
                self.context["output_dir"].mkdir()
                report = self.run_query()
                prompts, decisions = [], []
                client, lm = native._client_and_lm(self.method, self.generation, self.resources,
                    self.profile, FixtureService.instances[-1].endpoint)
                try:
                    frame = pd.DataFrame(self.source.rows()).rename(columns={"row_id": "_execution_row_id"})
                    with lotus.settings.context(lm=lm, enable_cache=False):
                        with observe_filter_rows(decisions.extend, prompts.extend):
                            reference = run_original_lotus_query(CHECKOUT, 3, lambda: frame)
                finally:
                    client.close()
                identities, _ = _prepared([dict(identity, messages_json=json.dumps(prompt["messages"]))
                    for identity, prompt in zip(self.source.expected("q"), prompts, strict=True)])
                self.assertEqual([r["messages_sha256"] for r in identities],
                                 [r["messages_sha256"] for r in report["calls"]["records"]])
                self.assertEqual(decisions, [(r["row_id"], r["decision"]) for r in report["decisions"]])
                self.assertEqual(report["result"], {"positive_review_cnt": int(reference.iloc[0, 0])})
                self.assertEqual(report["lm"], lm_report(lm))
                self.assertEqual(report["calls"]["usage"]["total_tokens"], 240)
                self.assertEqual(report["quality"]["original_metric"],
                    evaluate_original(CHECKOUT, 3, reference, self.source.labeled_frame())["original_metric"])
                self.assertEqual(self.context["claimed_budget"].attempts, 120)
                evidence = os.environ.get("NATIVE_CPU_EVIDENCE_DIR")
                if evidence:
                    from src.baselines.common.private_artifacts import write_private_json
                    from src.baselines.common.redact import redact_json_values
                    write_private_json(Path(evidence) / ("full-native-p" + str(concurrency) + ".json"),
                        redact_json_values(dict(report, execution_kind="CPU MockTransport; no inference",
                                                inference_calls=0, gpu_launches=0, server_launches=0)))
                    write_private_json(Path(evidence) / ("full-reference-p" + str(concurrency) + ".json"),
                        dict(result=report["result"], decisions=decisions,
                             messages_sha256=[r["messages_sha256"] for r in identities], lm=lm_report(lm),
                             quality=evaluate_original(CHECKOUT, 3, reference, self.source.labeled_frame()),
                             inference_calls=0, native_uncached_hook_replaced=False))


    def _use_actual_listener_check(self):
        import psutil
        owner = REAL_LOCAL_SERVICE(self.engine, "local-fixture", self.resources, self.root, "0")
        owner.port, owner.created = 39281, 10.0
        owner.process = Mock(pid=1200)
        owner.process.poll.return_value = None
        owner.owned = {(1200, 10.0)}
        process = Mock(create_time=Mock(return_value=10.0),
                       cmdline=Mock(return_value=owner.command()))
        listener = SimpleNamespace(status=psutil.CONN_LISTEN,
            laddr=SimpleNamespace(ip="127.0.0.1", port=39281), pid=1200)
        for change in (patch.object(psutil, "Process", return_value=process),
                       patch.object(psutil, "net_connections", return_value=[listener]),
                       patch.object(FixtureService, "verify_listener", side_effect=owner.verify_listener)):
            change.start()
        observe = patch.object(owner, "_observe").start()
        return process, observe

    def test_actual_native_sdk_posts_use_fresh_checks_without_repeated_discovery(self):
        process, observe = self._use_actual_listener_check()
        report = self.run_query()
        self.assertEqual(len(self.wire.requests), 4)
        self.assertEqual(self.budget.attempts, 4)
        self.assertEqual(report["result"], {"positive_review_cnt": 2})
        self.assertGreater(self.wire.peak, 1)
        self.assertEqual(process.create_time.call_count, 8)
        self.assertEqual(process.cmdline.call_count, 4)
        observe.assert_not_called()

    def test_actual_native_sdk_birth_change_blocks_later_sends_and_keeps_first_receipt(self):
        import httpx
        process, observe = self._use_actual_listener_check()
        self.profile = dict(native_lm_max_batch_size=1)
        def receive(request):
            response = self.wire(request)
            process.create_time.return_value = 11.0
            return response
        with patch.object(httpx, "HTTPTransport", side_effect=lambda **kw: httpx.MockTransport(receive)):
            with self.assertRaises(Exception) as caught:
                self.run_query()
        report = caught.exception.query_report
        self.assertEqual(len(self.wire.requests), 1)
        self.assertEqual(self.budget.attempts, 1)
        self.assertEqual(report["calls"]["counts"]["recorded"], 4)
        self.assertEqual(report["calls"]["counts"]["succeeded"], 1)
        self.assertEqual(report["calls"]["usage"]["known_prompt_tokens"], 13)
        self.assertEqual(report["calls"]["usage"]["known_completion_tokens"], 2)
        self.assertEqual(report["calls"]["counts"]["unknown"], 3)
        self.assertIsNone(report["calls"]["usage"]["total_tokens"])
        observe.assert_not_called()


class OwnershipTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.engine = EngineSpec(model_path="/fixture/model", model_python="/fixture/model/bin/python",
                                 driver_python="/fixture/driver/bin/python", chat_template="{{ messages }}")
        self.owner = native.LocalService(self.engine, "local-fixture", RecordsResources(), self.temp.name, "0")
        self.owner.port = 39281

    def test_command_binds_engine_and_single_server_template_application(self):
        command = self.owner.command()
        for flag, value in (("--host", "127.0.0.1"), ("--model", self.engine.model_path),
                            ("--generation-config", "vllm"), ("--tensor-parallel-size", "1"),
                            ("--pipeline-parallel-size", "1"), ("--chat-template", "{{ messages }}")):
            self.assertEqual(command[command.index(flag) + 1], value)
        self.assertIn("--no-enable-log-requests", command)

    def test_foreign_listener_is_rejected_without_http_contact_or_foreign_signal(self):
        import psutil
        self.owner.process = Mock(pid=1200)
        self.owner.created = 10.0
        self.owner.owned = {(1200, 10.0)}
        foreign = SimpleNamespace(status=psutil.CONN_LISTEN,
            laddr=SimpleNamespace(ip="127.0.0.1", port=39281), pid=1300)
        processes = {1200: Mock(create_time=Mock(return_value=10.0)),
                     1300: Mock(create_time=Mock(return_value=11.0))}
        with patch.object(self.owner, "_observe"), patch.object(psutil, "net_connections", return_value=[foreign]), \
             patch.object(psutil, "Process", side_effect=processes.__getitem__), \
             patch("httpx.Client.get") as contact:
            with self.assertRaisesRegex(RuntimeError, "foreign process"):
                self.owner._listen_identity()
            contact.assert_not_called()
        self.assertEqual(self.owner.owned, {(1200, 10.0)})

    def test_listener_and_exact_command_identity_are_both_required(self):
        import psutil
        self.owner.process = Mock(pid=1200)
        self.owner.created = 10.0
        self.owner.owned = {(1200, 10.0)}
        listener = SimpleNamespace(status=psutil.CONN_LISTEN,
            laddr=SimpleNamespace(ip="127.0.0.1", port=39281), pid=1200)
        process = Mock(create_time=Mock(return_value=10.0), cmdline=Mock(return_value=self.owner.command()))
        with patch.object(self.owner, "_observe"), patch.object(psutil, "net_connections", return_value=[listener]), \
             patch.object(psutil, "Process", return_value=process):
            self.assertEqual(self.owner._listen_identity(), [dict(pid=1200, create_time=10.0)])
            process.cmdline.return_value = self.owner.command() + ["--chat-template", "foreign-template"]
            with self.assertRaisesRegex(RuntimeError, "command/model/template/capacity changed"):
                self.owner._listen_identity()

    def listener_fixture(self):
        import psutil
        self.owner.process = Mock(pid=1200)
        self.owner.process.poll.return_value = None
        self.owner.created = 10.0
        self.owner.owned = {(1200, 10.0)}
        process = Mock(create_time=Mock(return_value=10.0),
                       cmdline=Mock(return_value=self.owner.command()))
        listener = SimpleNamespace(status=psutil.CONN_LISTEN,
            laddr=SimpleNamespace(ip="127.0.0.1", port=39281), pid=1200)
        processes = {1200: process}
        listeners = [listener]
        for change in (patch.object(psutil, "Process", side_effect=processes.__getitem__),
                       patch.object(psutil, "net_connections", side_effect=lambda **kw: list(listeners))):
            change.start()
            self.addCleanup(change.stop)
        observe = patch.object(self.owner, "_observe")
        observed = observe.start()
        self.addCleanup(observe.stop)
        return SimpleNamespace(process=process, listener=listener, processes=processes,
                               listeners=listeners, observe=observed)

    def test_known_listener_posts_skip_discovery_but_read_current_identity_and_command(self):
        state = self.listener_fixture()
        for _ in range(8):
            self.owner.verify_listener()
        state.observe.assert_not_called()
        self.assertEqual(state.process.cmdline.call_count, 8)
        self.assertEqual(state.process.create_time.call_count, 16)

    def test_new_owned_listener_is_discovered_once_then_keeps_fresh_checks(self):
        state = self.listener_fixture()
        state.listener.pid = 1201
        child = state.processes[1201] = Mock(create_time=Mock(return_value=10.1))
        state.observe.side_effect = lambda: self.owner.owned.add((1201, 10.1))
        for _ in range(4):
            self.owner.verify_listener()
        state.observe.assert_called_once_with()
        self.assertEqual(child.create_time.call_count, 4)
        self.assertEqual(state.process.cmdline.call_count, 4)

    def test_concurrent_first_posts_discover_a_new_listener_once(self):
        from concurrent.futures import ThreadPoolExecutor
        state = self.listener_fixture()
        state.listener.pid = 1201
        state.processes[1201] = Mock(create_time=Mock(return_value=10.1))
        together = threading.Barrier(4)
        def discover():
            with self.owner._owned_lock:
                time.sleep(0.005)
                self.owner.owned.add((1201, 10.1))
        state.observe.side_effect = discover
        def verify(_):
            together.wait(timeout=1)
            self.owner.verify_listener()
        with ThreadPoolExecutor(max_workers=4) as workers:
            list(workers.map(verify, range(4)))
        state.observe.assert_called_once_with()
        self.assertEqual(state.process.cmdline.call_count, 4)

    def test_cached_owned_pid_with_new_birth_is_rejected(self):
        state = self.listener_fixture()
        self.owner.verify_listener()
        state.process.create_time.return_value = 11.0
        with self.assertRaisesRegex(RuntimeError, "process identity changed"):
            self.owner.verify_listener()

    def test_replaced_child_listener_cannot_reuse_old_owned_pid(self):
        state = self.listener_fixture()
        state.listener.pid = 1201
        self.owner.owned.add((1201, 10.1))
        child = state.processes[1201] = Mock(create_time=Mock(return_value=10.1))
        self.owner.verify_listener()
        child.create_time.return_value = 11.1
        with self.assertRaisesRegex(RuntimeError, "foreign process"):
            self.owner.verify_listener()
        state.observe.assert_called_once_with()

    def test_command_change_on_same_pid_and_listener_still_rejects(self):
        state = self.listener_fixture()
        self.owner.verify_listener()
        state.process.cmdline.return_value = self.owner.command() + ["--served-model-name", "other"]
        with self.assertRaisesRegex(RuntimeError, "command/model/template/capacity changed"):
            self.owner.verify_listener()
        state.observe.assert_not_called()

    def test_missing_wildcard_unattributed_and_foreign_listeners_reject(self):
        import psutil
        state = self.listener_fixture()
        foreign = SimpleNamespace(status=psutil.CONN_LISTEN,
            laddr=SimpleNamespace(ip="127.0.0.1", port=39281), pid=1300)
        state.processes[1300] = Mock(create_time=Mock(return_value=11.0))
        cases = [[], [SimpleNamespace(status=psutil.CONN_LISTEN,
                    laddr=SimpleNamespace(ip="0.0.0.0", port=39281), pid=1200)],
                 [SimpleNamespace(status=psutil.CONN_LISTEN,
                    laddr=SimpleNamespace(ip="127.0.0.1", port=39281), pid=None)],
                 [foreign], [state.listener, foreign]]
        for listeners in cases:
            with self.subTest(listeners=listeners):
                state.listeners[:] = listeners
                with self.assertRaises(RuntimeError):
                    self.owner.verify_listener()

    def test_service_exit_and_monitor_error_reject_before_connection_inventory(self):
        state = self.listener_fixture()
        self.owner.process.poll.return_value = 1
        with self.assertRaises(RuntimeError):
            self.owner.verify_listener()
        state.process.create_time.assert_not_called()
        self.owner.process.poll.return_value = None
        self.owner._monitor_errors.append("fixture observation failure")
        with self.assertRaises(RuntimeError):
            self.owner.verify_listener()
        state.process.create_time.assert_not_called()

    def test_post_error_and_changed_listener_are_checked_before_next_native_send(self):
        import httpx
        from src.experiments.native_http_observer import _observe_native_httpx_client
        from src.semantic_methods.lotus.records import identity
        state = self.listener_fixture()
        calls = native._Calls([identity("q", str(i), i) for i in range(3)],
            Mock(reserve=Mock(side_effect=[1, 2, 3])), GenerationSpec(), "local-fixture", 3)
        calls.before_post = self.owner.verify_listener
        sent, events = [], []
        original = httpx.ConnectError("CPU fixture first connection failure")
        endpoint = "http://127.0.0.1:39281/v1/chat/completions"
        def receive(request):
            sent.append(request.content)
            if len(sent) == 1:
                raise original
            return httpx.Response(200, json={"choices": []})
        with httpx.Client(transport=httpx.MockTransport(receive), trust_env=False) as client:
            with _observe_native_httpx_client(calls, events.append, endpoint):
                with self.assertRaises(httpx.ConnectError) as caught:
                    client.post(endpoint, content=b'{"messages":["unchanged"]}')
                self.assertIs(caught.exception, original)
                client.post(endpoint, content=b'{"messages":["unchanged"]}')
                state.listener.pid = 1300
                state.processes[1300] = Mock(create_time=Mock(return_value=11.0))
                with self.assertRaisesRegex(RuntimeError, "foreign process"):
                    client.post(endpoint, content=b'{"messages":["unchanged"]}')
        self.assertEqual(sent, [b'{"messages":["unchanged"]}'] * 2)
        self.assertEqual(calls.budget.reserve.call_count, 2)
        self.assertEqual(state.process.cmdline.call_count, 2)
        state.observe.assert_called_once_with()

    def test_close_only_signals_observed_owned_births_and_is_idempotent(self):
        self.owner.process = Mock(pid=1200, returncode=-15)
        self.owner.process.poll.return_value = -15
        self.owner.created = 10.0
        self.owner.owned = {(1200, 10.0), (1201, 10.1)}
        with patch.object(self.owner, "_observe"), \
             patch("src.infrastructure.owned_processes._live", side_effect=[[Mock()], [], []]), \
             patch("src.infrastructure.owned_processes._signal", return_value=[]) as send, \
             patch("src.infrastructure.owned_processes._wait"):
            report = self.owner.close()
            self.assertTrue(report["process_exit_confirmed"])
            self.assertEqual(send.call_count, 1)
            self.assertEqual(send.call_args.args[0], {(1200, 10.0), (1201, 10.1)})
            self.assertIs(self.owner.close(), report)
        self.owner.process.kill.assert_not_called()

    def test_crashed_ready_service_is_failed_even_after_confirmed_os_exit(self):
        self.owner.process = Mock(pid=1200, returncode=1)
        self.owner.process.poll.return_value = 1
        self.owner.created = 10.0
        self.owner.owned = {(1200, 10.0)}
        self.owner.ready_report = dict(software_fixture=True)
        with patch.object(self.owner, "_observe"), \
             patch("src.infrastructure.owned_processes._live", return_value=[]):
            report = self.owner.close()
        self.assertTrue(report["process_exit_confirmed"])
        self.assertEqual(report["status"], "failed")
        self.assertTrue(report["unexpected_exit_before_close"])


class ReceiptTests(unittest.TestCase):
    def calls(self, duplicate=False):
        from src.semantic_methods.lotus.records import identity
        expected = [identity("q", "a", 0)]
        if duplicate:
            expected.append(identity("q", "b", 1))
        calls = native._Calls(expected, ClaimedUnit(Reservation("q", 1, len(expected)), None),
                              GenerationSpec(), "local-fixture", len(expected))
        messages = [dict(role="user", content="same conversation")]
        calls.prompts([dict(row_id=r["row_id"], messages=messages) for r in expected])
        return calls, messages

    def receipt(self, calls, messages, ordinal, response):
        calls.events[ordinal] = dict(request=dict(body=dict(messages=messages)), http_started={},
            http_response=dict(status=200, response=response))

    def response(self, identifier, text="Answer: True"):
        return dict(id=identifier, model="local-fixture", prompt_token_ids=[71] * 11,
                    choices=[dict(index=0, finish_reason="stop", message=dict(content=text), token_ids=[91, 93])],
                    usage=dict(prompt_tokens=11, completion_tokens=2, total_tokens=13))

    def test_duplicate_messages_use_native_return_order_and_response_id(self):
        calls, messages = self.calls(duplicate=True)
        first, second = self.response("first"), self.response("second", "Answer: False")
        self.receipt(calls, messages, 1, first)
        self.receipt(calls, messages, 2, second)
        def value(response):
            choice = response["choices"][0]
            return SimpleNamespace(id=response["id"], usage=SimpleNamespace(**response["usage"]),
                choices=[SimpleNamespace(message=SimpleNamespace(**choice["message"]), finish_reason=choice["finish_reason"])])
        calls.returned = [value(second), value(first)]
        report, _, errors = calls.ledger(None)
        self.assertEqual(errors, [])
        self.assertEqual([(r["row_id"], r["engine_request_id"]) for r in report["records"]],
                         [("a", "second"), ("b", "first")])

    def test_malformed_response_fields_retain_known_usage_without_overwriting_failure(self):
        for changes, known in ((dict(usage=None), 0), (dict(choices=[None]), 11),
                               (dict(choices=[dict(message=None)]), 11), (dict(id=["invalid"]), 11)):
            calls, messages = self.calls()
            self.receipt(calls, messages, 1, dict(self.response("receipt"), **changes))
            report, receipts, errors = calls.ledger(RuntimeError("original failure"))
            self.assertEqual(report["counts"]["failed"], 1)
            self.assertEqual(report["usage"]["known_prompt_tokens"], known)
            self.assertEqual(len(receipts), 1)
            self.assertTrue(errors)


if __name__ == "__main__":
    unittest.main()
