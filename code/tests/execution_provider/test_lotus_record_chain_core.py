"""Real Core/async backend and LOTUS; model, environment probe and OS are fixtures."""

from copy import deepcopy
from dataclasses import asdict, replace
import hashlib
from importlib.util import find_spec
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

os.environ.setdefault("LITELLM_LOCAL_MODEL_COST_MAP", "True")
CODE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir())
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.execution_provider.adapters import lotus_record_chain_core as core
from src.baselines.text.frameworks.lotus_chain_engine_process import ChainEngineProcess
from src.scheduling.core.session import SessionEngine
from src.scheduling.runtime.async_backend import BoundedAsyncBackend
from src.semantic_methods.lotus.record_chain import (
    ExtractMethod, LotusRecordChain, MapMethod, RecordOccurrence, StageSpec, records_to_frame,
)
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec, IDENTITY_FIELDS


def source_records():
    # Repeated index labels are legal; business keys are data, not transport IDs.
    return [RecordOccurrence("occ-" + str(i), i, 17,
                dict(reviewText="NEG review" if i == 1 else "POS review",
                     row_id="business-row", call_id="business-call"),
                source_refs={"source": "same-label"}) for i in range(3)]


def method_chain(kinds="FME", query_id="cpu-core-chain"):
    stages, previous = [], "reviewText"
    for index, kind in enumerate(kinds):
        name = kind.lower() + str(index)
        if kind == "F":
            method = FilterMethod("openai/local-fixture", "Is {" + previous + "} positive?")
            generation = GenerationSpec(max_tokens=8 + index)
        elif kind == "M":
            column = "summary" + str(index)
            method = MapMethod("openai/local-fixture", "Summarize {" + previous + "}.", column)
            previous = column
            generation = GenerationSpec(max_tokens=31 + index, stop=())
        else:
            method = ExtractMethod("openai/local-fixture", (previous,),
                                   {"aspect": "aspect", "row_id": "business value", "call_id": None})
            generation = GenerationSpec(max_tokens=51 + index, stop=(),
                                        response_format={"type": "json_object"})
        stages.append(StageSpec(name, method, generation, model_role="main"))
    return LotusRecordChain(tuple(stages), query_id=query_id)


class ModelFixture:
    def __init__(self, chain, *, unknown_stage=None, invalid_extract=False, all_false=False,
                 corrupt_stage=None, blocked_stage=None, close_unknown=False,
                 unknown_position=None, close_raises=False):
        self.chain = chain
        self.unknown_stage, self.corrupt_stage = unknown_stage, corrupt_stage
        self.invalid_extract, self.all_false = invalid_extract, all_false
        self.blocked_stage, self.close_unknown = blocked_stage, close_unknown
        self.unknown_position, self.close_raises = unknown_position, close_raises
        self.engines, self.prepared, self.generations, self.worker_threads = [], [], [], []
        self.started, self.gate = threading.Event(), threading.Event()

    def kernel_type(self):
        fixture = self

        class Kernel:
            def __init__(self, engine, generation, control, deadline, startup_timeout_s):
                self.control, self.closed, self.active = Path(control), False, False
                fixture.engines.append(self)

            def run_prepared(self, rows, *, generation):
                if self.closed:
                    raise AssertionError("an already closed model was reused")
                self.active = True
                fixture.prepared.extend(deepcopy(rows))
                fixture.generations.append(asdict(generation))
                fixture.worker_threads.append(threading.get_ident())
                stage_id = rows[0]["algorithm_stage"]
                stage = next(item for item in fixture.chain.stages if item.stage_id == stage_id)
                if stage_id == fixture.blocked_stage:
                    fixture.started.set()
                    if not fixture.gate.wait(4):
                        raise AssertionError("CPU fixture gate was not released")
                values = []
                for row in rows:
                    message = json.loads(row["messages_json"])
                    negative = "NEG" in json.dumps(message, ensure_ascii=False)
                    if stage.kind == "filter":
                        text = "False" if fixture.all_false or negative else "True"
                    elif stage.kind == "map":
                        text = "NEG summary" if negative else "POS summary"
                    else:
                        text = ("[]" if fixture.invalid_extract else json.dumps({
                            "aspect": "camera", "row_id": "generated-business-row",
                            "call_id": "generated-business-call"}))
                    unknown = (stage_id == fixture.unknown_stage
                               and fixture.unknown_position in (None, row["source_position"]))
                    value = dict({key: row[key] for key in IDENTITY_FIELDS},
                        status="unknown" if unknown else "succeeded", text=None if unknown else text,
                        submission_confirmed=not unknown,
                        prompt_tokens=None if unknown else 11, completion_tokens=None if unknown else 2,
                        total_tokens=None if unknown else 13, finish_reason=None if unknown else "stop",
                        messages_sha256=hashlib.sha256(row["messages_json"].encode()).hexdigest(),
                        engine_uid="cpu-model-fixture", engine_request_id="fixture-" + row["call_id"],
                        error_type="FixtureUnknown" if unknown else None)
                    if stage_id == fixture.corrupt_stage and not values:
                        value["call_id"] = "foreign-call"
                    values.append(value)
                self.active = False
                return values

            def close(self, timeout=None):
                if fixture.close_raises:
                    error_type = fixture.close_raises if isinstance(fixture.close_raises, type) else RuntimeError
                    raise error_type("fixture-secondary-close")
                confirmed = not self.active and not fixture.close_unknown
                self.closed = confirmed
                report = dict(status="closed" if confirmed else "unknown",
                              engine_api_shutdown_confirmed=confirmed,
                              process_exit_confirmed=confirmed, gpu_exit_confirmed=None,
                              cleanup_errors=[], fixture=True)
                (self.control / "engine-fixture.json").write_text(json.dumps(report))
                return report

        return Kernel


class CpuProcess(ChainEngineProcess):
    @staticmethod
    def _command(engine, result_fd):
        return [sys.executable, str(Path(__file__).resolve()), "--engine-fixture", str(result_fd)]


class CapacityTests(unittest.TestCase):
    def test_capacity_is_finite_logical_bytes_and_fused_call_upper_bound(self):
        for kwargs in ({"item_input_bytes": True}, {"item_result_bytes": 0},
                       {"poll_interval_s": float("nan")}, {"step_actions": -1}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                core.CoreChainCapacity(**kwargs)
        resources = RecordsResources(cpu_workers=1, max_active_batches=1, model_batch_rows=2)
        limits = core.CoreChainCapacity().limits(resources, 3)
        self.assertEqual((limits.held_tasks, limits.active_requests, limits.active_work), (1, 1, 6))

    def test_profile_rejects_multiple_workers_or_active_batches(self):
        spec = EngineSpec("/tmp/model-fixture", sys.executable, sys.executable)
        for kwargs in ({"cpu_workers": 2, "max_active_batches": 1}, {"max_active_batches": 2}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                core.SemLoomRecordChainCore(spec, RecordsResources(**kwargs))


@unittest.skipUnless(find_spec("lotus"), "requires pinned original LOTUS in the nonmodel environment")
class CoreChainTests(unittest.TestCase):
    def setUp(self):
        from src.baselines.text.frameworks.lotus_chain_batch import FusedChainBatch

        self.batch_type = FusedChainBatch
        self.root = tempfile.TemporaryDirectory(prefix="core-chain-cpu-")
        self.addCleanup(self.root.cleanup)
        self.rows = source_records()
        self.spec = EngineSpec(self.root.name, sys.executable, sys.executable)
        self.owners, self.fixtures = [], []
        probe = patch.object(EngineSpec, "validate_driver_environment",
                             return_value={"fixture": "model environment is not installed"})
        self.environment_probe = probe.start()
        self.addCleanup(probe.stop)

    def tearDown(self):
        for fixture in self.fixtures:
            fixture.gate.set()
        for owner in self.owners:
            if owner.core is not None:
                until = time.monotonic() + 2
                while owner.core.capacity.usage().active_requests and time.monotonic() < until:
                    owner.core.reap(owner.capacity.step_actions)
                    time.sleep(0.005)
            owner.close(timeout=1)

    def owner(self, chain, *, capacity=None, batch_rows=2, fixture=None):
        fixture = ModelFixture(chain) if fixture is None else fixture
        self.fixtures.append(fixture)
        owner = core.SemLoomRecordChainCore(
            self.spec, RecordsResources(cpu_workers=1, max_active_batches=1,
                prepare_batch_rows=1, model_batch_rows=batch_rows, query_timeout_s=20),
            capacity=capacity, control_dir=self.root.name)
        owner._batch_type = type("CpuFusedBatch", (self.batch_type,), {"_engine_type": fixture.kernel_type()})
        self.owners.append(owner)
        return owner, fixture

    def run_chain(self, kinds="FME", *, rows=None, fixture=None):
        chain = method_chain(kinds)
        owner, fixture = self.owner(chain, fixture=fixture)
        rows = self.rows if rows is None else rows
        stream = owner.evaluate(iter(rows), chain, expected=rows, query_id=chain.query_id)
        return owner, stream, fixture

    def test_fme_and_legal_mf_mmf_ffm_share_the_only_method_loop(self):
        for kinds, expected_calls in [("FME", 7), ("MF", 6), ("MMF", 9), ("FFM", 7)]:
            with self.subTest(kinds=kinds):
                owner, stream, fixture = self.run_chain(kinds)
                with patch.object(stream.session, "release", wraps=stream.session.release) as release:
                    output = list(stream)
                self.assertIsInstance(owner.core, SessionEngine)
                self.assertIsInstance(owner.transport.backend, BoundedAsyncBackend)
                self.assertTrue(stream.production_core_connected)
                self.assertTrue(stream.completed)
                self.assertEqual([row.row_id for row in output], ["occ-0", "occ-2"])
                self.assertEqual(len(stream.calls), expected_calls)
                self.assertEqual(sum(call["prompt_tokens"] for call in stream.calls), expected_calls * 11)
                self.assertEqual(sum(call["total_tokens"] for call in stream.calls), expected_calls * 13)
                self.assertEqual(len({call["call_id"] for call in stream.calls}), expected_calls)
                self.assertEqual(release.call_count, 2)
                self.assertEqual(len(fixture.engines), 1)
                self.assertTrue(all(value != threading.get_ident() for value in fixture.worker_threads))
                self.assertEqual(owner.last_close_report["status"], "closed")
                self.assertIsNone(owner.last_close_report["gpu_exit_confirmed"])
                self.assertFalse(owner.last_close_report["physical_gpu_exit_checked"])
                self.assertEqual(owner.core.capacity.usage().held_tasks, 0)
                self.assertLessEqual(stream.peak_core_usage["held_tasks"], 1)
                self.assertEqual(owner.last_close_report["work_unit"], "stage_call_upper_bound")
                self.assertEqual(owner.last_close_report["physical_transport"],
                                 "bounded_async_host_fused_worker_private_pipe")
                self.assertFalse(owner.last_close_report["ray_used_by_adapter"])
                self.assertFalse(owner.last_close_report["daft_used_by_adapter"])
                self.assertFalse(owner.last_close_report["cross_query_model_reuse"])
                self.assertFalse(owner.last_close_report["overlapped_cross_batch_supply"])
                self.assertEqual(owner.last_close_report["source_pull_batch_rows"], 2)
                self.assertEqual(owner.last_close_report["declared_model_evaluations"], expected_calls)
                self.assertEqual(list(stream), [])
                with self.assertRaisesRegex(RuntimeError, "already used"):
                    owner.evaluate(self.rows, method_chain(kinds, "other"),
                                   expected=self.rows, query_id="other")

    def test_matches_direct_original_method_loop_and_preserves_generation(self):
        chain = method_chain("FME")
        fixture = ModelFixture(chain)
        owner, fixture = self.owner(chain, fixture=fixture)
        stream = owner.evaluate(self.rows, chain, expected=self.rows, query_id=chain.query_id)
        output = list(stream)
        direct_fixture = ModelFixture(chain)
        direct = direct_fixture.kernel_type()(self.spec, None, self.root.name, 0, 0)
        expected = LotusRecordChain.from_dict(chain.describe()).run_batch(self.rows, direct.run_prepared)
        self.assertEqual([row.describe() for row in output], [row.describe() for row in expected["records"]])
        comparable = lambda calls: sorted(
            [{key:value for key,value in call.items() if key != "engine_uid"} for call in calls],
            key=lambda call: call["call_id"])
        self.assertEqual(comparable(stream.calls), comparable(expected["calls"]))
        self.assertEqual({value["max_tokens"] for value in fixture.generations}, {8, 32, 53})
        self.assertIn({"type": "json_object"}, [value["response_format"] for value in fixture.generations])

    def test_real_private_process_wire_and_observed_exit_with_only_model_kernel_substituted(self):
        chain = method_chain()
        owner, _ = self.owner(chain)
        owner._batch_type = type("CpuProcessBatch", (self.batch_type,), {"_engine_type": CpuProcess})
        stream = owner.evaluate(self.rows, chain, expected=self.rows, query_id=chain.query_id)
        self.assertEqual([row.row_id for row in stream], ["occ-0", "occ-2"])
        self.assertEqual(len(stream.calls), 7)
        self.assertEqual(len({call["model_pid"] for call in stream.calls}), 1)
        self.assertTrue(all(call["model_pid"] != os.getpid() for call in stream.calls))
        self.assertTrue(all(call["lotus_imported"] is False for call in stream.calls))
        report = owner.last_close_report
        self.assertEqual(report["status"], "closed")
        self.assertTrue(report["model_owner"]["process_exit_confirmed"])
        self.assertTrue(report["model_owner"]["engine_api"]["engine_api_shutdown_confirmed"])
        self.assertGreaterEqual(report["model_owner"]["observed_processes"], 1)
        self.assertIsNone(report["gpu_exit_confirmed"])

    def test_repeated_index_and_business_row_call_columns_keep_transport_identity(self):
        owner, stream, _ = self.run_chain()
        output = list(stream)
        frame = records_to_frame(output, columns=stream.columns)
        self.assertEqual(frame.index.tolist(), [17, 17])
        self.assertEqual(frame["row_id"].tolist(), ["generated-business-row"] * 2)
        self.assertEqual(frame["call_id"].tolist(), ["generated-business-call"] * 2)
        self.assertEqual({row.row_id for row in output}, {"occ-0", "occ-2"})
        self.assertTrue(all(call["row_id"].startswith("occ-") for call in stream.calls))
        for row in output:
            stages = [call for call in stream.calls if call["row_id"] == row.row_id]
            self.assertEqual(stages[0]["parent_call_ids"], [])
            self.assertEqual(stages[1]["parent_call_ids"], [stages[0]["call_id"]])
            self.assertEqual(stages[2]["parent_call_ids"], [stages[1]["call_id"]])

    def test_empty_source_and_all_false_preserve_schema_and_call_usage(self):
        owner, stream, fixture = self.run_chain(rows=[])
        self.assertEqual(list(stream), [])
        self.assertTrue(stream.completed)
        self.assertEqual(fixture.engines, [])
        self.assertEqual(stream.calls, [])
        self.assertIn("summary1", stream.columns)
        chain = method_chain()
        fixture = ModelFixture(chain, all_false=True)
        owner, fixture = self.owner(chain, fixture=fixture)
        stream = owner.evaluate(self.rows, chain, expected=self.rows, query_id=chain.query_id)
        self.assertEqual(list(stream), [])
        self.assertEqual(len(stream.calls), 3)
        self.assertTrue(all(call["algorithm_stage"] == "f0" for call in stream.calls))
        self.assertTrue(stream.completed)
        self.assertIn("summary1", stream.columns)

    def test_extract_nonterminal_query_identity_and_source_metadata_precede_core(self):
        with self.assertRaisesRegex(ValueError, "Extract must be terminal"):
            method_chain("EM")
        chain = method_chain()
        for rows in ([self.rows[0], self.rows[0]],
                     [self.rows[0], replace(self.rows[1], source_position=0)]):
            owner, fixture = self.owner(chain)
            with self.assertRaisesRegex(ValueError, "globally unique"):
                owner.evaluate(rows, chain, expected=rows, query_id=chain.query_id)
            self.assertFalse(owner.production_core_connected)
            self.assertEqual(fixture.engines, [])
        owner, fixture = self.owner(chain)
        with self.assertRaisesRegex(ValueError, "same query"):
            owner.evaluate(self.rows, chain, expected=self.rows, query_id="wrong-query")
        self.assertFalse(owner.production_core_connected)

    def test_model_environment_validation_failure_precedes_core_and_worker(self):
        chain = method_chain()
        owner, fixture = self.owner(chain)
        with patch.object(EngineSpec, "validate_driver_environment",
                          side_effect=RuntimeError("fixture-environment-rejected")):
            with self.assertRaisesRegex(RuntimeError, "fixture-environment-rejected"):
                owner.evaluate(self.rows, chain, expected=self.rows, query_id=chain.query_id)
        self.assertFalse(owner.production_core_connected)
        self.assertEqual(fixture.engines, [])

    def test_source_binding_and_raw_capacity_reject_before_model(self):
        for capacity, rows, expected in [
            (core.CoreChainCapacity(item_input_bytes=32), self.rows, self.rows),
            (None, [replace(self.rows[0], values={"reviewText":"replaced"})], self.rows[:1]),
            (None, self.rows[:1], []),
        ]:
            chain = method_chain()
            owner, fixture = self.owner(chain, capacity=capacity)
            stream = owner.evaluate(iter(rows), chain, expected=expected, query_id=chain.query_id)
            with self.assertRaises(ValueError):
                list(stream)
            self.assertEqual(fixture.engines, [])
            self.assertEqual(owner.core.capacity.usage().held_tasks, 0)

    def test_slow_consumer_keeps_one_fused_batch_and_releases_lease_once(self):
        owner, fixture = self.owner(method_chain("MF"))
        pulled = []

        def source():
            for row in self.rows:
                pulled.append(row.row_id)
                yield row

        chain = fixture.chain
        stream = owner.evaluate(source(), chain, expected=self.rows, query_id=chain.query_id)
        first = next(stream)
        self.assertEqual(first.row_id, "occ-0")
        self.assertEqual(len(pulled), 2)
        self.assertEqual(owner.core.capacity.usage().held_tasks, 0)
        self.assertEqual([row.row_id for row in stream], ["occ-2"])
        self.assertEqual(stream.session._next_lease, 2)

    def test_parser_error_keeps_successful_usage_and_does_not_publish_failed_batch(self):
        chain = method_chain()
        owner, fixture = self.owner(chain, fixture=ModelFixture(chain, invalid_extract=True))
        stream = owner.evaluate(self.rows, chain, expected=self.rows, query_id=chain.query_id)
        with self.assertRaises(Exception) as caught:
            list(stream)
        self.assertEqual(getattr(caught.exception, "remote_error_type", None), "AttributeError")
        self.assertFalse(stream.completed)
        self.assertEqual(len(stream.calls), 4)
        self.assertTrue(all(call["status"] == "succeeded" for call in stream.calls))
        self.assertEqual(sum(call["total_tokens"] for call in stream.calls), 52)
        self.assertEqual(owner.core.capacity.usage().held_tasks, 0)

    def test_unknown_and_invalid_association_keep_credit_references_and_known_prefix(self):
        for options in ({"unknown_stage": "m1"}, {"corrupt_stage": "m1"}):
            chain = method_chain()
            owner, fixture = self.owner(chain, fixture=ModelFixture(chain, **options))
            stream = owner.evaluate(self.rows, chain, expected=self.rows, query_id=chain.query_id)
            with self.assertRaises(Exception):
                list(stream)
            self.assertFalse(stream.completed)
            self.assertEqual(owner.last_close_report["status"], "unknown")
            self.assertEqual(owner.core.capacity.usage().held_tasks, 1)
            self.assertEqual(owner.last_close_report["retained_remote_inputs"], 1)
            self.assertEqual(len(fixture.engines), 1)
            self.assertTrue(any(call["status"] == "succeeded" for call in stream.calls))
            unknown = [call for call in stream.calls if call["status"] == "unknown"]
            self.assertTrue(unknown)
            self.assertTrue(all(call["prompt_tokens"] is None for call in unknown))

    def test_later_batch_failure_preserves_previously_delivered_relation_and_call_prefix(self):
        chain = method_chain()
        owner, fixture = self.owner(chain, fixture=ModelFixture(
            chain, unknown_stage="m1", unknown_position=2))
        stream = owner.evaluate(self.rows, chain, expected=self.rows, query_id=chain.query_id)
        prefix = []
        with self.assertRaises(Exception) as caught:
            prefix.extend(stream)
        self.assertEqual([row.row_id for row in prefix], ["occ-0"])
        self.assertEqual(len(stream.calls), 6)
        self.assertEqual(sum(call["status"] == "succeeded" for call in stream.calls), 5)
        self.assertEqual(sum(call["prompt_tokens"] for call in stream.calls
                             if call["prompt_tokens"] is not None), 55)
        self.assertIsNone(stream.calls[-1]["prompt_tokens"])
        self.assertEqual(caught.exception.chain_calls, stream.calls)
        self.assertFalse(stream.completed)

    def test_model_close_exception_is_secondary_to_original_parser_failure(self):
        for error_type in (RuntimeError, TimeoutError):
            with self.subTest(error_type=error_type.__name__):
                chain = method_chain()
                owner, fixture = self.owner(chain, fixture=ModelFixture(
                    chain, invalid_extract=True, close_raises=error_type))
                stream = owner.evaluate(self.rows, chain, expected=self.rows, query_id=chain.query_id)
                with self.assertRaises(Exception) as caught:
                    list(stream)
                self.assertEqual(getattr(caught.exception, "remote_error_type", None), "AttributeError")
                self.assertEqual(len(stream.calls), 4)
                self.assertTrue(all(call["status"] == "succeeded" for call in stream.calls))
                self.assertIn("private_engine_close: " + error_type.__name__,
                              owner.last_close_report["cleanup_errors"])
                self.assertFalse(stream.completed)

    def test_decode_error_releases_delivery_without_overwriting_primary(self):
        chain = method_chain("MF")
        owner, fixture = self.owner(chain)
        stream = owner.evaluate(self.rows, chain, expected=self.rows, query_id=chain.query_id)
        with patch.object(owner.results, "accept", side_effect=ValueError("primary-decode")):
            with self.assertRaisesRegex(ValueError, "primary-decode"):
                list(stream)
        self.assertEqual(stream.session._next_lease, 1)
        self.assertEqual(owner.core.capacity.usage().held_tasks, 0)
        self.assertTrue(stream.calls)

    def test_result_overflow_recovers_known_audit_without_fake_core_terminal(self):
        chain = method_chain("MF")
        owner, fixture = self.owner(chain, capacity=core.CoreChainCapacity(item_result_bytes=256))
        stream = owner.evaluate(self.rows, chain, expected=self.rows, query_id=chain.query_id)
        with self.assertRaises(RuntimeError):
            list(stream)
        self.assertEqual(owner.core.capacity.usage().held_tasks, 1)
        self.assertEqual(owner.last_close_report["status"], "unknown")
        self.assertEqual(len(stream.calls), 4)
        self.assertEqual(sum(call["total_tokens"] for call in stream.calls), 52)

    def test_unconfirmed_model_close_remains_failure_without_erasing_known_usage(self):
        chain = method_chain("MF")
        owner, fixture = self.owner(chain, fixture=ModelFixture(chain, close_unknown=True))
        stream = owner.evaluate(self.rows, chain, expected=self.rows, query_id=chain.query_id)
        with self.assertRaisesRegex(RuntimeError, "cleanup remains unconfirmed"):
            list(stream)
        self.assertFalse(stream.completed)
        self.assertEqual(len(stream.calls), 6)
        self.assertEqual(owner.last_close_report["status"], "unknown")

    def test_cancel_and_deadline_retain_running_borrow_then_drain_without_retry(self):
        for kind in ("cancel", "deadline"):
            chain = method_chain("M")
            fixture = ModelFixture(chain, blocked_stage="m0")
            owner, fixture = self.owner(chain, fixture=fixture)
            cancelled = threading.Event()
            stream = owner.evaluate(self.rows, chain, expected=self.rows, query_id=chain.query_id,
                                    cancelled=cancelled.is_set)
            stream._fill()
            start = time.monotonic()
            stream.session.advance(1)
            self.assertLess(time.monotonic() - start, 0.1)
            self.assertTrue(fixture.started.wait(2))
            if kind == "cancel":
                cancelled.set()
            else:
                owner.deadline = time.monotonic() - 1
            with self.assertRaises(InterruptedError if kind == "cancel" else TimeoutError):
                next(stream)
            self.assertEqual(owner.core.capacity.usage().held_tasks, 1)
            self.assertFalse(stream.completed)
            fixture.gate.set()
            until = time.monotonic() + 2
            while owner.core.capacity.usage().held_tasks and time.monotonic() < until:
                owner.core.reap(owner.capacity.step_actions)
                time.sleep(0.005)
            report = owner.close(timeout=1)
            self.assertEqual(report["status"], "closed")
            self.assertEqual(owner.core.capacity.usage().held_tasks, 0)
            self.assertEqual(len(fixture.engines), 1)
            self.assertTrue(stream.calls)
            self.assertFalse(stream.completed)

    def test_cancel_before_dependent_stage_keeps_unconfirmed_declaration_and_no_extra_model_call(self):
        chain = method_chain("MF")
        owner, fixture = self.owner(chain, fixture=ModelFixture(chain, blocked_stage="m0"))
        cancelled = threading.Event()
        stream = owner.evaluate(self.rows, chain, expected=self.rows, query_id=chain.query_id,
                                cancelled=cancelled.is_set)
        stream._fill()
        stream.session.advance(1)
        self.assertTrue(fixture.started.wait(2))
        cancelled.set()
        with self.assertRaises(InterruptedError):
            next(stream)
        fixture.gate.set()
        until = time.monotonic() + 0.5
        while owner.core.capacity.usage().held_tasks and time.monotonic() < until:
            owner.core.reap(owner.capacity.step_actions)
            time.sleep(0.005)
        report = owner.close(timeout=1)
        self.assertEqual(report["status"], "unknown")
        self.assertEqual(owner.core.capacity.usage().held_tasks, 1)
        self.assertEqual({row["algorithm_stage"] for row in fixture.prepared}, {"m0"})
        self.assertTrue(any(call["status"] == "succeeded" for call in stream.calls))
        self.assertTrue(any(call["status"] == "unknown" and not call["submission_confirmed"]
                            and call["prompt_tokens"] is None for call in stream.calls))
        self.assertFalse(stream.completed)

    def test_owner_and_iterator_reject_foreign_thread_before_source_access(self):
        owner, fixture = self.owner(method_chain())
        pulled = []
        stream = owner.evaluate(iter(self.rows), fixture.chain, expected=self.rows,
                                query_id=fixture.chain.query_id)

        def foreign():
            try:
                next(stream)
            except BaseException as error:
                pulled.append(type(error).__name__)

        thread = threading.Thread(target=foreign)
        thread.start()
        thread.join(timeout=1)
        self.assertEqual(pulled, ["RuntimeError"])
        self.assertEqual(fixture.engines, [])

    def test_close_during_worker_construction_is_bounded_and_cannot_start_a_model_later(self):
        chain = method_chain("M")
        owner, fixture = self.owner(chain)
        entered, gate = threading.Event(), threading.Event()
        batch = owner._batch_type

        class SlowConstruction(batch):
            def __init__(self, *args):
                entered.set()
                if not gate.wait(2):
                    raise AssertionError("CPU constructor gate was not released")
                super().__init__(*args)

        owner._batch_type = SlowConstruction
        stream = owner.evaluate(self.rows, chain, expected=self.rows, query_id=chain.query_id)
        stream._fill()
        stream.session.advance(1)
        self.assertTrue(entered.wait(1))
        try:
            started = time.monotonic()
            report = owner.close(timeout=0.03)
            self.assertLess(time.monotonic() - started, 0.3)
            self.assertEqual(report["status"], "unknown")
        finally:
            gate.set()
        self.assertEqual(fixture.engines, [])

    def test_blocked_private_close_respects_timeout_and_retains_same_cleanup_owner(self):
        chain = method_chain("M")
        owner, fixture = self.owner(chain)
        stream = owner.evaluate(self.rows, chain, expected=self.rows, query_id=chain.query_id)
        next(stream)
        worker = owner.transport.worker
        original_close = worker.close
        entered, gate = threading.Event(), threading.Event()

        def blocked_close(timeout=None):
            entered.set()
            if not gate.wait(2):
                raise AssertionError("CPU close gate was not released")
            return original_close(timeout=timeout)

        with patch.object(worker, "close", side_effect=blocked_close) as close:
            started = time.monotonic()
            report = owner.close(timeout=0.03)
            self.assertLess(time.monotonic() - started, 0.3)
            self.assertTrue(entered.is_set())
            self.assertEqual(report["status"], "unknown")
            self.assertIs(owner.transport.worker, worker)
            gate.set()
            owner.transport._close_thread.join(timeout=1)
            report = owner.close(timeout=1)
        self.assertEqual(report["status"], "closed")
        self.assertTrue(report["model_owner"]["cleanup_thread_exit_confirmed"])
        self.assertEqual(len(fixture.engines), 1)
        self.assertEqual(close.call_count, 1)


def serve_cpu_model(result_fd):
    """The actual private IPC process substitutes VllmEngine, never LOTUS/Core."""
    from src.baselines.text.frameworks import lotus_chain_engine_process as transport
    from src.serving.backends import vllm_engine

    class CpuModel:
        def __init__(self, *_):
            pass

        def ready(self):
            return dict(engine_uid="cpu-process-" + str(os.getpid()), pid=os.getpid())

        def run_prepared(self, rows, *, generation, deadline_monotonic):
            outputs = []
            for row in rows:
                negative = "NEG" in row["messages_json"]
                kind = row["algorithm_stage"][0]
                if kind == "f":
                    text = "False" if negative else "True"
                elif kind == "m":
                    text = "NEG summary" if negative else "POS summary"
                else:
                    text = json.dumps(dict(aspect="camera", row_id="generated-business-row",
                                           call_id="generated-business-call"))
                outputs.append(dict({key: row[key] for key in IDENTITY_FIELDS},
                    status="succeeded", text=text, prompt_tokens=11, completion_tokens=2,
                    total_tokens=13, finish_reason="stop", submission_confirmed=True,
                    engine_request_id="cpu-" + row["call_id"], engine_uid="cpu-process-" + str(os.getpid()),
                    messages_sha256=hashlib.sha256(row["messages_json"].encode()).hexdigest(),
                    model_pid=os.getpid(), lotus_imported="lotus" in sys.modules))
            return outputs

        def close(self):
            return dict(status="closed", engine_api_shutdown_confirmed=True)

    vllm_engine.VllmEngine = CpuModel
    transport.serve(result_fd)


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--engine-fixture":
        serve_cpu_model(int(sys.argv[2]))
    else:
        unittest.main()
