"""Native-stage lifetime contracts and optional real-SDK CPU transport checks.

The always-available fixtures exercise the adapter boundary, not LOTUS parsing.
The SDK cases invoke original accessors with an in-memory model transport;
neither group loads a model or opens a network connection.
"""

from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import importlib.util
import json
import socket
import sys
import threading
import time
from types import ModuleType
import unittest
from unittest.mock import patch

import pandas as pd

from src.baselines.text.frameworks import lotus_native_stage as native_stage
from src.infrastructure.request_budget import ClaimedUnit, Reservation
from src.semantic_methods.lotus.record_types import (
    ExtractMethod, MapMethod, RecordOccurrence, StageSpec, stage_identity,
)
from src.semantic_methods.lotus.records import CallLedger, FilterMethod, RecordsResources
from src.serving.backends.async_engine_process import SubmitResult, TerminalEvent
from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec, _prepared, result_record


HAS_SDK = all(importlib.util.find_spec(name) is not None for name in ("lotus", "litellm"))


class FixtureSettings:
    def __init__(self):
        self.enable_cache, self.lm = True, "original LM"

    @contextmanager
    def context(self, **values):
        previous = dict(vars(self))
        vars(self).update(values)
        try:
            yield self
        finally:
            vars(self).clear()
            vars(self).update(previous)


@dataclass
class FixtureStats:
    total_tokens: int = 0


class FixtureLM:
    def __init__(self, model):
        self.model, self.stats, self.actual_calls = model, FixtureStats(), 0

    def __call__(self, messages, **kwargs):
        self.actual_calls += 1
        return self.complete(messages)


class FixtureMethod:
    def __init__(self, stage):
        self.stage = stage
        self.lm = FixtureLM(stage.method.model_name)

    def prepare_rows(self, records, *, query_id):
        raise AssertionError("native execution must not perform a formatter prepass")


class FixtureBinding:
    def __init__(self, expected, budget, lm):
        self.expected, self.budget, self.lm = expected, budget, lm
        self.prompts, self.receipts, self.released = [], [], False
        self.complete_count = None

    def capture_prompts(self, prompts):
        self.prompts.extend(prompts)

    def complete(self, messages):
        count = len(self.expected) if self.complete_count is None else self.complete_count
        for index, target in enumerate(self.expected[:count]):
            self.budget.reserve(hashlib.sha256(
                json.dumps(self.prompts[index]["messages"]).encode()).hexdigest())
            row = result_record(target, engine_uid="fixture", start=0, end=1,
                status="succeeded", submitted=True)
            row.update(text="fixture response", finish_reason="stop", prompt_tokens=3,
                completion_tokens=1, total_tokens=4, engine_request_id="fixture-" + str(index))
            self.receipts.append(row)
            self.lm.stats.total_tokens += 4

    def report(self):
        if self.released:
            raise RuntimeError("accounting already transferred")
        ledger = CallLedger(self.expected, max(1, len(self.expected)))
        for receipt in self.receipts:
            ledger.record(receipt)
        ledger.mark_unconfirmed("NativeQueryDidNotReturnEveryOccurrence")
        return dict(calls=ledger.report(), native_transport_receipts=list(self.receipts),
            late_native_receipts=[], source_binding="CPU boundary fixture")


class NativeStageFixtureTests(unittest.TestCase):
    def setUp(self):
        self.settings = FixtureSettings()
        self.original_lm_call = FixtureLM.__call__
        self.lotus = ModuleType("lotus")
        self.lotus.settings = self.settings
        self.owners, self.close_status, self.close_error, self.startup_error = [], "closed", None, None
        case = self

        class Owner:
            def __init__(self, engine, generation, resources, concurrency):
                self.engine, self.generation = engine, generation
                self.resources, self.concurrency = resources, concurrency
                self.ready_report = self.last_close_report = self.binding = None
                self.failed, self.closed = False, False
                case.owners.append(self)

            @contextmanager
            def query(self, lm, expected, budget, *, query_id):
                if case.startup_error is not None:
                    raise case.startup_error
                self.binding = FixtureBinding(expected, budget, lm)
                lm.complete = self.binding.complete
                self.ready_report = dict(engine_uid="fixture", inference_calls=0) if expected else None
                try:
                    yield self.binding
                except BaseException:
                    self.failed = True
                    raise

            def close(self, timeout):
                self.closed = True
                if case.close_error is not None:
                    raise case.close_error
                result = dict(status=case.close_status, process_exit_confirmed=case.close_status == "closed")
                if self.failed and self.binding is not None:
                    result["native_final_accounting"] = self.binding.report()
                    self.binding.released = case.close_status == "closed"
                self.last_close_report = result
                return result

        module = ModuleType("src.baselines.text.frameworks.lotus_native_local")
        module.LocalNativeModel = Owner
        scope = patch.dict(sys.modules, {"lotus": self.lotus, module.__name__: module})
        scope.start()
        self.addCleanup(scope.stop)
        scope = patch.object(native_stage, "LotusRecordMethod", FixtureMethod)
        scope.start()
        self.addCleanup(scope.stop)
        self.engine = EngineSpec("/tmp/native-stage-CPU-fixture", sys.executable, "/tmp/driver",
            max_model_calls=20)
        self.resources = RecordsResources(query_timeout_s=1, startup_timeout_s=1)
        self.generation = GenerationSpec(seed=0)

    def inputs(self, texts=("same", "same"), kind="filter"):
        method = (FilterMethod("openai/local-fixture", "Keep {reviewText}",
            resolved_input_columns=("reviewText",)) if kind == "filter" else
            MapMethod("openai/local-fixture", "Classify {reviewText}",
                resolved_input_columns=("reviewText",)) if kind == "map" else
            ExtractMethod("openai/local-fixture", ("reviewText",), {"sentiment": "sentiment"}))
        generation = GenerationSpec(max_tokens=32, response_format={"type": "json_object"}) if kind == "extract" else self.generation
        stage = StageSpec("original-accessor", method, generation, model_role="subject")
        frame = pd.DataFrame(dict(reviewText=list(texts)), index=[7] * len(texts))
        records = tuple(RecordOccurrence(str(i), i, 7, dict(reviewText=text))
            for i, text in enumerate(texts))
        budget = ClaimedUnit(Reservation("q", 1, max(1, len(records))), None)
        return frame, stage, records, budget

    def execute(self, *, kind="filter", texts=("same", "same"), invoke=None, profile=None, budget=None):
        frame, stage, records, allowance = self.inputs(texts, kind)
        self.budget = allowance if budget is None else budget
        self.messages = [[dict(role="user", content=row.values["reviewText"])] for row in records]
        def complete():
            self.assertIs(self.settings.lm, self.owners[-1].binding.lm)
            self.assertFalse(self.settings.enable_cache)
            self.settings.lm(self.messages)
            return frame
        self.result_frame = frame
        return native_stage.execute_native(frame, stage, records, invoke=complete if invoke is None else invoke,
            engine=self.engine, resources=self.resources, query_id="q",
            profile=dict(native_lm_max_batch_size=2) if profile is None else profile, budget=self.budget)

    def test_original_frame_duplicate_occurrences_and_settings_survive_all_three_stage_kinds(self):
        for kind in ("filter", "map", "extract"):
            with self.subTest(kind=kind):
                report = self.execute(kind=kind)
                owner = self.owners[-1]
                self.assertIs(report["frame"], self.result_frame)
                self.assertEqual(report["calls"]["counts"]["succeeded"], 2)
                self.assertEqual([row["row_id"] for row in owner.binding.prompts], ["0", "1"])
                self.assertEqual(owner.binding.prompts[0]["messages"], owner.binding.prompts[1]["messages"])
                self.assertTrue(all(row["algorithm_stage"] == "original-accessor" and
                    row["model_role"] == "subject" for row in report["calls"]["records"]))
                self.assertTrue(all(row["call_id"].startswith("lotus-filter-v1:")
                    for row in report["calls"]["records"]))
                self.assertEqual(set(report["stage_call_ids"].values()),
                    {row["call_id"] for row in report["stage_calls"]})
                self.assertEqual(self.budget.attempts, 2)
                self.assertEqual(owner.binding.lm.max_batch_size, 2)
                self.assertEqual(owner.binding.lm.actual_calls, 1)
                self.assertIsNone(owner.engine.max_model_len)
                self.assertIsNone(owner.engine.max_num_seqs)
                self.assertEqual(owner.generation.response_format,
                    {"type": "json_object"} if kind == "extract" else None)
                self.assertTrue(owner.closed)
                self.assertEqual(report["backend_close"]["status"], "closed")
                self.assertTrue(self.settings.enable_cache)
                self.assertEqual(self.settings.lm, "original LM")
                self.assertIs(FixtureLM.__call__, self.original_lm_call)

    def test_callback_failure_transfers_known_calls_without_promoting_unreturned_rows(self):
        error = ValueError("native accessor failed after one return")
        def fail():
            self.owners[-1].binding.complete_count = 1
            self.settings.lm(self.messages)
            raise error
        with self.assertRaises(ValueError) as caught:
            self.execute(invoke=fail)
        self.assertIs(caught.exception, error)
        report = error.operator_report
        self.assertEqual(report["calls"]["counts"]["succeeded"], 1)
        self.assertEqual(report["calls"]["counts"]["unknown"], 1)
        self.assertEqual(report["calls"]["usage"]["known_prompt_tokens"], 3)
        self.assertEqual(self.budget.attempts, 1)
        self.assertTrue(self.owners[-1].binding.released)
        self.assertNotIn("frame", report)
        json.dumps(report, allow_nan=False)
        self.assertIs(FixtureLM.__call__, self.original_lm_call)

    def test_incomplete_native_return_fails_even_when_accessor_returns_a_frame(self):
        with self.assertRaisesRegex(RuntimeError, "bound LM") as caught:
            self.execute(invoke=lambda: pd.DataFrame())
        self.assertEqual(caught.exception.operator_report["calls"]["counts"]["unknown"], 2)
        self.assertTrue(self.owners[-1].closed)
        self.assertIs(FixtureLM.__call__, self.original_lm_call)

    def test_message_count_mismatch_fails_before_any_model_request(self):
        with self.assertRaisesRegex(ValueError, "message count") as caught:
            self.execute(invoke=lambda: self.settings.lm(self.messages[:1]))
        self.assertEqual(caught.exception.operator_report["calls"]["counts"]["unknown"], 2)
        self.assertEqual(self.budget.attempts, 0)
        self.assertEqual(self.owners[-1].binding.lm.actual_calls, 0)
        self.assertIs(FixtureLM.__call__, self.original_lm_call)

    def test_repeated_lm_call_does_not_spend_another_allowance(self):
        def repeat():
            self.settings.lm(self.messages)
            self.settings.lm(self.messages)
        with self.assertRaisesRegex(RuntimeError, "more than once") as caught:
            self.execute(invoke=repeat)
        self.assertEqual(caught.exception.operator_report["calls"]["counts"]["succeeded"], 2)
        self.assertEqual(self.budget.attempts, 2)
        self.assertEqual(self.owners[-1].binding.lm.actual_calls, 1)
        self.assertIs(FixtureLM.__call__, self.original_lm_call)

    def test_another_lm_is_not_associated_with_this_stage(self):
        other = FixtureLM("another model")
        with self.assertRaisesRegex(ValueError, "another LM"):
            self.execute(invoke=lambda: other(self.messages))
        self.assertEqual(self.budget.attempts, 0)
        self.assertEqual(other.actual_calls, 0)
        self.assertEqual(self.owners[-1].binding.lm.actual_calls, 0)
        self.assertIs(FixtureLM.__call__, self.original_lm_call)

    def test_unknown_or_raising_close_does_not_report_completion(self):
        for close_error in (None, OSError("fixture close failed")):
            with self.subTest(error=close_error):
                self.close_status, self.close_error = "unknown", close_error
                with self.assertRaises((RuntimeError, OSError)) as caught:
                    self.execute()
                report = caught.exception.operator_report
                self.assertEqual(report["status"], "failed")
                self.assertEqual(report["backend_close"]["status"], "unknown")
                self.assertEqual(report["calls"]["counts"]["succeeded"], 2)

    def test_close_error_preserves_the_original_accessor_failure(self):
        self.close_error = OSError("fixture close failed")
        error = ValueError("original accessor failure")
        def fail():
            raise error
        with self.assertRaises(ValueError) as caught:
            self.execute(invoke=fail)
        self.assertIs(caught.exception, error)
        self.assertEqual(error.operator_report["backend_close"]["status"], "unknown")
        self.assertIn("OSError", error.__notes__[0])

    def test_startup_failure_still_closes_the_allocated_owner(self):
        self.startup_error = OSError("fixture startup failed")
        with self.assertRaises(OSError) as caught:
            self.execute()
        self.assertTrue(self.owners[-1].closed)
        self.assertEqual(caught.exception.operator_report["calls"]["counts"]["unknown"], 2)
        self.assertEqual(self.budget.attempts, 0)

    def test_missing_sdk_retains_unattempted_calls_without_allocating_an_owner(self):
        with patch.dict(sys.modules, {"lotus": None}), self.assertRaises(ImportError) as caught:
            self.execute()
        self.assertEqual(caught.exception.operator_report["calls"]["counts"]["unknown"], 2)
        self.assertEqual(self.budget.attempts, 0)
        self.assertFalse(self.owners)

    def test_empty_accessor_runs_without_model_start_or_allowance_spend(self):
        report = self.execute(texts=())
        self.assertNotIn("owner_ready", report)
        self.assertEqual(report["calls"]["counts"]["expected"], 0)
        self.assertEqual(self.budget.attempts, 0)
        self.assertEqual(report["backend_close"]["status"], "closed")

    def test_invalid_profile_or_insufficient_allowance_fails_before_owner_allocation(self):
        for profile in ({}, {"native_lm_max_batch_size": True},
                {"native_lm_max_batch_size": 1, "max_num_seqs": 8}):
            with self.subTest(profile=profile), self.assertRaises(ValueError):
                self.execute(profile=profile)
        with self.assertRaisesRegex(ValueError, "complete declared"):
            self.execute(budget=ClaimedUnit(Reservation("q", 1, 1), None))
        self.assertFalse(self.owners)

    def test_original_call_ledger_rejects_finite_stage_ids_without_contract_widening(self):
        _, stage, records, _ = self.inputs()
        finite = [stage_identity("q", row, stage.stage_id, model_role=stage.model_role)
            for row in records]
        with self.assertRaisesRegex(ValueError, "LOTUS call contract"):
            CallLedger(finite, max_calls=len(finite))


@unittest.skipUnless(HAS_SDK, "LOTUS and LiteLLM are required for original-accessor CPU transport tests")
class NativeStageSdkTests(unittest.TestCase):
    def run_stage(self, kind, texts=("duplicate", "good", "duplicate")):
        import lotus
        from lotus.models import LM
        from src.baselines.text.frameworks import lotus_native_local as native

        transport_requests = []

        class Transport:
            def __init__(self, *args, **kwargs):
                self.ready_report = dict(engine_uid="CPU-stage-fixture", inference_calls=0)
                self.events, self.lock = [], threading.Lock()

            def submit(self, row, *, generation, deadline_monotonic):
                with self.lock:
                    request_id = "request-" + str(len(transport_requests))
                    transport_requests.append((dict(row), generation))
                    result = result_record(_prepared([row])[0][0], engine_uid="CPU-stage-fixture",
                        start=time.monotonic(), end=time.monotonic(), status="succeeded", submitted=True)
                    result.update(engine_request_id=request_id, text=(
                        '{"sentiment":"POSITIVE"}' if generation.response_format is not None else
                        "True" if kind == "filter" else "POSITIVE"),
                        finish_reason="stop", prompt_tokens=11, completion_tokens=2, total_tokens=13)
                    self.events.append(TerminalEvent(request_id, _prepared([row])[0][0], result, True))
                    return SubmitResult("queued", request_id)

            def poll(self, maximum):
                with self.lock:
                    result, self.events = self.events[:maximum], self.events[maximum:]
                    return result

            def wait(self, timeout):
                return None

            def abort(self, request_id):
                return None

            def close(self, timeout):
                return dict(status="closed", process_exit_confirmed=True, cleanup_errors=[])

        method = (FilterMethod("openai/local-fixture", "Keep {reviewText}") if kind == "filter" else
            MapMethod("openai/local-fixture", "Classify {title}: {reviewText}", output_column="_sentiment")
            if kind == "map" else ExtractMethod("openai/local-fixture", ("reviewText", "title"),
                {"sentiment": "The review sentiment"}))
        generation = GenerationSpec(max_tokens=32,
            response_format={"type": "json_object"} if kind == "extract" else None)
        stage = StageSpec("original-" + kind, method, generation, model_role="subject").bind()
        frame = pd.DataFrame(dict(reviewText=list(texts), title=["review"] * len(texts)),
            index=[7] * len(texts))
        records = tuple(RecordOccurrence(str(i), i, 7, dict(reviewText=text, title="review"))
            for i, text in enumerate(texts))
        # This independent reference preparation is test setup, outside execution.
        with lotus.settings.context(enable_cache=False):
            reference = native_stage.LotusRecordMethod(stage).prepare_rows(records, query_id="q")
        expected_messages = [json.loads(row["messages_json"]) for row in reference]
        original_call, actual_calls = LM.__call__, []

        def counted(current, messages, *args, **kwargs):
            actual_calls.append(json.loads(json.dumps(messages)))
            return original_call(current, messages, *args, **kwargs)

        def invoke():
            if kind == "filter":
                return frame.sem_filter(method.instruction)
            if kind == "map":
                return frame.sem_map(method.instruction, suffix="_sentiment")
            return frame.sem_extract(list(method.input_columns), method.output_columns,
                extract_quotes=False, return_raw_outputs=False)
        budget = ClaimedUnit(Reservation("q", 1, len(records)), None)
        connect = socket.socket.connect
        def forbid_network(sock, address):
            if sock.family in (socket.AF_INET, socket.AF_INET6):
                self.fail("CPU native-stage test attempted a network connection")
            return connect(sock, address)
        with patch.object(native, "AsyncEngineProcess", Transport), \
                patch.object(socket.socket, "connect", forbid_network), \
                patch.object(LM, "__call__", counted), \
                patch.object(native_stage.LotusRecordMethod, "prepare_rows",
                    side_effect=AssertionError("execution performed a second formatting pass")), \
                lotus.settings.context(enable_cache=False):
            report = native_stage.execute_native(frame, stage, records, invoke=invoke,
                engine=EngineSpec("/tmp/CPU-stage-fixture", sys.executable, "/tmp/driver",
                    max_model_calls=20), resources=RecordsResources(query_timeout_s=2),
                query_id="q", profile=dict(native_lm_max_batch_size=2), budget=budget)
            self.assertIs(LM.__call__, counted)
        self.assertIs(LM.__call__, original_call)
        self.assertEqual(actual_calls, [expected_messages])
        self.assertEqual(report["calls"]["counts"]["succeeded"], len(records))
        self.assertEqual(report["calls"]["usage"]["total_tokens"], 13 * len(records))
        self.assertEqual(budget.attempts, len(records))
        self.assertEqual(report["backend_close"]["status"], "closed")
        self.assertTrue(all(row["model_role"] == "subject" and
            row["algorithm_stage"] == stage.stage_id for row in report["calls"]["records"]))
        self.assertEqual(len({row["engine_request_id"] for row in report["calls"]["records"]}), len(records))
        self.assertTrue(all(value.response_format == generation.response_format for _, value in transport_requests))
        by_row = {row["row_id"]: row for row in report["calls"]["records"]}
        for record, messages in zip(records, expected_messages, strict=True):
            digest = hashlib.sha256(json.dumps(messages, ensure_ascii=False, sort_keys=True,
                separators=(",", ":"), allow_nan=False).encode()).hexdigest()
            self.assertEqual(by_row[record.row_id]["messages_sha256"], digest)
        return report

    def test_real_original_filter_uses_ordered_duplicate_prompt_binding(self):
        report = self.run_stage("filter")
        self.assertEqual(len(report["frame"]), 3)
        self.assertEqual(report["calls"]["records"][0]["messages_sha256"],
            report["calls"]["records"][2]["messages_sha256"])

    def test_real_original_map_retains_the_accessor_frame(self):
        report = self.run_stage("map")
        self.assertEqual(list(report["frame"]["_sentiment"]), ["POSITIVE"] * 3)

    def test_real_original_extract_retains_json_response_format(self):
        report = self.run_stage("extract")
        self.assertEqual(list(report["frame"]["sentiment"]), ["POSITIVE"] * 3)


if __name__ == "__main__":
    unittest.main()
