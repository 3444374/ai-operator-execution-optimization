"""Core Extract barriers with real LOTUS/Core and explicit CPU model fixtures."""

from copy import deepcopy
from dataclasses import asdict
import hashlib
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

from src.baselines.text.frameworks.lotus_chain_batch import FusedChainBatch
from src.execution_provider.adapters import lotus_record_chain_core as core
from src.semantic_methods.lotus.record_chain import (
    ExtractMethod, LotusRecordChain, LotusRecordMethod, MapMethod, RecordOccurrence,
    StageSpec, records_to_frame,
)
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec, IDENTITY_FIELDS
from tests.execution_provider.test_lotus_record_chain_core import CpuProcess


DOCUMENTS = ("source first review", "source separate review", "source later review", "source empty review")
EXTRACTED = (
    dict(shared_value="drop earlier", retained="first only", row_id="data first", call_id="first business"),
    dict(shared_value="drop separate"),
    dict(shared_value="keep broadcast", row_id="data last", call_id="last business"),
    {},
)


def source_records(count=4):
    return [RecordOccurrence("occ-" + str(i), i, (17, 29, 17, 53)[i],
        dict(reviewText=DOCUMENTS[i], row_id="business row", call_id="business call", optional=None),
        source_refs={"source": [i, None]}) for i in range(count)]


def relation_chain(kinds="EF", query_id="core-extract-relations"):
    stages, previous = [], "reviewText"
    for index, kind in enumerate(kinds):
        if kind == "E":
            method = ExtractMethod("openai/local-fixture", (previous,), {"shared_value": "signal"})
            generation = GenerationSpec(max_tokens=64 + index, stop=(), response_format={"type": "json_object"})
            previous = "shared_value"
        elif kind == "M":
            method = MapMethod("openai/local-fixture", "Summarize {" + previous + "}.", "mapped" + str(index))
            generation = GenerationSpec(max_tokens=32 + index, stop=())
            previous = method.output_column
        else:
            method = FilterMethod("openai/local-fixture", "Is {" + previous + "} positive?")
            generation = GenerationSpec(max_tokens=8 + index)
        stages.append(StageSpec(kind.lower() + str(index), method, generation,
                                model_role="role-" + str(index)))
    return LotusRecordChain(tuple(stages), query_id=query_id, relation_barriers=True)


def response(stage, message):
    text = json.dumps(message, ensure_ascii=False)
    found = next((i for i, document in enumerate(DOCUMENTS) if document in text), None)
    if stage.kind == "extract":
        return json.dumps(EXTRACTED[found] if found is not None else
                          {"shared_value": "keep broadcast" if "keep broadcast" in text else "drop"})
    if stage.kind == "filter":
        return "True" if "keep broadcast" in text else "False"
    return "mapped " + (DOCUMENTS[found] if found is not None else
                        "keep broadcast" if "keep broadcast" in text else "drop")


def terminal(row, text):
    return dict({key: row[key] for key in IDENTITY_FIELDS}, status="succeeded", text=text,
                submission_confirmed=True, prompt_tokens=11, completion_tokens=3, total_tokens=14,
                finish_reason="stop", engine_uid="core-relations-cpu",
                engine_request_id="cpu-" + row["call_id"],
                messages_sha256=hashlib.sha256(row["messages_json"].encode()).hexdigest())


class ModelFixture:
    def __init__(self, chain, *, unknown_position=None, invalid_extract_position=None,
                 oversize_stage=None, close_raises=False):
        self.chain = chain
        self.unknown_position, self.invalid_extract_position = unknown_position, invalid_extract_position
        self.oversize_stage, self.close_raises = oversize_stage, close_raises
        self.engines, self.prepared, self.generations, self.begin_threads = [], [], [], []
        self.close_calls, self.source_eof = 0, False
        self.submitted_ids = set()

    def kernel_type(self):
        fixture = self

        class Model:
            def __init__(self, spec, *args):
                self.control, self.closed = Path(args[1]), False
                self.limit = spec.max_model_calls
                fixture.engines.append(self)

            def run_prepared(self, rows, *, generation):
                if self.closed:
                    raise AssertionError("the model closed before its final consumer")
                stage = next(stage for stage in fixture.chain.stages
                             if stage.stage_id == rows[0]["algorithm_stage"])
                if stage.stage_id in {item.stage_id for segment in fixture.chain.relation_segments()[1:]
                                     for item in segment.stages} and not fixture.source_eof:
                    raise AssertionError("a successor started before the original source EOF")
                fixture.prepared.extend(deepcopy(rows))
                fixture.generations.append(asdict(generation))
                values = []
                for row in rows:
                    if row["call_id"] in fixture.submitted_ids:
                        raise AssertionError("model occurrence replayed")
                    fixture.submitted_ids.add(row["call_id"])
                    if self.limit is not None and len(fixture.submitted_ids) > self.limit:
                        raise AssertionError("permanent query call budget reset")
                    text = response(stage, json.loads(row["messages_json"]))
                    if stage.kind == "extract" and row["source_position"] == fixture.invalid_extract_position:
                        text = "[]"
                    item = terminal(row, text)
                    if stage.kind == "extract" and row["source_position"] == fixture.unknown_position:
                        item.update(status="unknown", submission_confirmed=False, text=None,
                                    prompt_tokens=None, completion_tokens=None, total_tokens=None,
                                    error_type="CPUUnknown", finish_reason=None)
                    if stage.stage_id == fixture.oversize_stage:
                        item["fixture_result_padding"] = "x" * 20000
                    values.append(item)
                return values

            def close(self, timeout=None):
                fixture.close_calls += 1
                if fixture.close_raises:
                    raise PermissionError("CPU secondary close failure")
                self.closed = True
                value = dict(status="closed", process_exit_confirmed=True,
                             engine_api_shutdown_confirmed=True, gpu_exit_confirmed=None, cleanup_errors=[])
                (self.control / "engine-fixture.json").write_text(json.dumps(value))
                return value

        return Model


class CoreRelationTests(unittest.TestCase):
    def setUp(self):
        import lotus

        self.scope = lotus.settings.context(enable_cache=False)
        self.scope.__enter__()
        self.addCleanup(self.scope.__exit__, None, None, None)
        self.temp = tempfile.TemporaryDirectory(prefix="core-relations-cpu-")
        self.addCleanup(self.temp.cleanup)
        self.owners = []
        probe = patch.object(EngineSpec, "validate_driver_environment", return_value={"fixture": True})
        probe.start()
        self.addCleanup(probe.stop)

    def tearDown(self):
        for owner in self.owners:
            if owner.core is not None:
                owner.core.reap(owner.capacity.step_actions)
            owner.close(timeout=2)

    def execute(self, kinds="EF", *, records=None, fixture=None, capacity=None,
                cancelled=None, columns=None):
        chain = relation_chain(kinds)
        fixture = fixture or ModelFixture(chain)
        records = source_records() if records is None else records
        spec = EngineSpec(self.temp.name, sys.executable, sys.executable,
                          max_model_calls=max(1, len(records) * len(chain.stages)))
        owner = core.SemLoomRecordChainCore(spec, RecordsResources(cpu_workers=1,
            max_active_batches=1, model_batch_rows=2, query_timeout_s=20),
            capacity=capacity, control_dir=self.temp.name)
        fixture_type = fixture.kernel_type()

        class Batch(FusedChainBatch):
            _engine_type = fixture_type

            def begin_relation_phase(self, *args, **kwargs):
                fixture.begin_threads.append(threading.get_ident())
                return super().begin_relation_phase(*args, **kwargs)

        owner._batch_type = Batch
        self.owners.append(owner)

        def source():
            yield from records
            fixture.source_eof = True

        stream = owner.evaluate(source(), chain, expected=records, query_id=chain.query_id,
                                cancelled=cancelled, columns=columns)
        return owner, stream, fixture

    def original(self, records, chain):
        import lotus
        import pandas as pd
        from src.semantic_methods.lotus.engine import engine_response

        frame = pd.DataFrame([record.values for record in records], index=[record.source_index for record in records])
        observations, methods = [], []
        for stage in chain.stages:
            method = LotusRecordMethod(stage)
            methods.append(method)

            def answer(data, options, *_args, method=method):
                messages = [message for message, _ in data]
                observations.extend((method.stage.stage_id, message) for message in deepcopy(messages))
                return [engine_response(terminal(dict(call_id=str(i), messages_json=json.dumps(message),
                    **{key: "reference" for key in IDENTITY_FIELDS if key != "call_id"}),
                    response(method.stage, message)), method.lm) for i, message in enumerate(messages)]

            method.lm._process_uncached_messages = answer
            with lotus.settings.context(lm=method.lm):
                if stage.kind == "extract":
                    frame = frame.sem_extract(list(stage.method.input_columns), stage.method.output_columns)
                elif stage.kind == "filter":
                    frame = frame.sem_filter(stage.method.instruction, default=stage.method.default)
                else:
                    frame = frame.sem_map(stage.method.instruction, suffix=stage.method.output_column)
        return frame, observations, methods

    def test_cross_batch_broadcast_parents_messages_and_stats_match_original(self):
        import pandas as pd

        owner, stream, fixture = self.execute()
        output = list(stream)
        expected, observations, methods = self.original(source_records(), fixture.chain)
        pd.testing.assert_frame_equal(records_to_frame(output, columns=stream.columns), expected)
        self.assertEqual([row.row_id for row in output], ["occ-0", "occ-2"])
        self.assertEqual([row.values["row_id"] for row in output], ["data last"] * 2)
        self.assertEqual([row.values["call_id"] for row in output], ["last business"] * 2)
        self.assertEqual([(row["algorithm_stage"], json.loads(row["messages_json"]))
                          for row in fixture.prepared], observations)
        extract = {call["source_position"]: call["call_id"] for call in stream.calls
                   if call["algorithm_stage"] == "e0"}
        for call in stream.calls:
            if call["algorithm_stage"] == "f1":
                self.assertEqual(call["parent_call_ids"], [extract[0], extract[2]]
                                 if call["source_position"] in (0, 2) else [extract[call["source_position"]]])
        self.assertEqual(len(stream.calls), 8)
        self.assertEqual(sum(call["prompt_tokens"] for call in stream.calls), 88)
        self.assertEqual(sum(method.lm.stats.physical_usage.prompt_tokens for method in methods), 88)
        latest = {}
        for snapshot in stream.batch_stage_stats:
            latest.update(snapshot)
        for method in methods:
            self.assertEqual(latest[method.stage.stage_id]["stats"], asdict(method.lm.stats))
        self.assertEqual(len(fixture.engines), 1)
        self.assertEqual(fixture.close_calls, 1)
        self.assertTrue(all(thread != threading.get_ident() for thread in fixture.begin_threads))
        self.assertEqual(len(owner._accountants), 2)
        self.assertEqual(owner.last_close_report["status"], "closed")
        self.assertFalse(owner.last_close_report["whole_query_rss_bounded"])

    def test_default_profile_keeps_its_existing_live_accounting_views(self):
        chain = relation_chain("F").relation_segments()[0]
        owner = core.SemLoomRecordChainCore(
            EngineSpec(self.temp.name, sys.executable, sys.executable),
            RecordsResources(cpu_workers=1, max_active_batches=1), control_dir=self.temp.name)
        self.owners.append(owner)
        stream = owner.evaluate(iter(()), chain, expected=[], query_id=chain.query_id)
        self.assertFalse(owner.relation_barriers)
        self.assertIs(stream.calls, owner.results.calls)
        self.assertIs(stream.batch_stage_stats, owner.results.batch_stage_stats)
        self.assertIs(stream.raw_result_batches, owner.results.raw_result_batches)
        self.assertIs(stream.unassociated_results, owner.results.unassociated_results)

    def test_legal_segments_share_engine_and_preserve_independent_generation(self):
        for kinds in ("MEF", "EMF", "EEF"):
            with self.subTest(kinds=kinds):
                owner, stream, fixture = self.execute(kinds)
                output = list(stream)
                self.assertTrue(stream.completed)
                self.assertEqual([row.row_id for row in output], ["occ-0", "occ-2"])
                self.assertEqual(len(fixture.engines), 1)
                self.assertEqual(fixture.close_calls, 1)
                self.assertEqual({call["model_role"] for call in stream.calls},
                                 {stage.model_role for stage in fixture.chain.stages})
                self.assertEqual({value["max_tokens"] for value in fixture.generations},
                                 {stage.generation.max_tokens for stage in fixture.chain.stages})
                self.assertTrue(all(not any(item["core_usage"].values()) for item in owner.phase_history))

    def test_empty_source_and_empty_successor_preserve_schema_without_extra_model(self):
        owner, stream, fixture = self.execute("EMF", records=[], columns=("reviewText", "row_id", "call_id"))
        self.assertEqual(list(stream), [])
        self.assertEqual(fixture.engines, [])
        self.assertEqual(stream.columns, ["reviewText", "row_id", "call_id", "mapped1"])
        self.assertTrue(stream.completed)
        rows = source_records(2)
        owner, stream, fixture = self.execute("EFEF", records=rows)
        self.assertEqual(list(stream), [])
        self.assertEqual({call["algorithm_stage"] for call in stream.calls}, {"e0", "f1"})
        self.assertEqual(len(fixture.engines), 1)
        self.assertEqual(fixture.close_calls, 1)
        self.assertIn("shared_value", stream.columns)
        self.assertTrue(stream.completed)

    def test_offered_work_is_raw_batch_times_current_segment_and_phase_leases_zero(self):
        owner, stream, fixture = self.execute("MEF")
        calls = []
        original = owner.transport._run

        def observe(task):
            rows = json.loads(task.task.payload)
            calls.append((len(rows), len(owner.chain.stages), task.task.estimated_work,
                          set(rows[0]), owner.phase_stage_ids))
            return original(task)

        with patch.object(owner.transport, "_run", side_effect=observe):
            list(stream)
        self.assertTrue(calls)
        self.assertTrue(all(work == rows * stages and fields == {"record_json"}
                            for rows, stages, work, fields, _ in calls))
        self.assertEqual({phase for *_, phase in calls}, {("m0", "e1"), ("f2",)})
        self.assertEqual(len(owner._source_expected), 4)
        self.assertTrue(all(not any(item["core_usage"].values()) for item in owner.phase_history))
        self.assertEqual(owner.core.capacity.usage().held_tasks, 0)
        self.assertLessEqual(stream.peak_core_usage["held_tasks"], 1)

    def test_prefix_parser_failure_with_succeeded_calls_never_enters_successor(self):
        chain = relation_chain()
        fixture = ModelFixture(chain, invalid_extract_position=2)
        owner, stream, fixture = self.execute(fixture=fixture)
        with self.assertRaises(Exception):
            list(stream)
        self.assertTrue(stream.calls)
        self.assertTrue(all(call["status"] == "succeeded" for call in stream.calls))
        self.assertEqual({row["algorithm_stage"] for row in fixture.prepared}, {"e0"})
        self.assertEqual(owner.phase_history, [])
        with self.assertRaises(Exception):
            owner.results.verify_success(source_exhausted=True)

    def test_unknown_prefix_keeps_credit_source_and_known_calls_without_replay(self):
        chain = relation_chain()
        fixture = ModelFixture(chain, unknown_position=2)
        owner, stream, fixture = self.execute(fixture=fixture)
        with self.assertRaises(Exception):
            list(stream)
        self.assertEqual(owner.core.capacity.usage().held_tasks, 1)
        self.assertEqual(owner.last_close_report["retained_remote_inputs"], 1)
        self.assertEqual({row["algorithm_stage"] for row in fixture.prepared}, {"e0"})
        self.assertEqual(len(fixture.engines), 1)
        self.assertEqual(sum(call["prompt_tokens"] or 0 for call in stream.calls), 33)
        self.assertIsNone(next(call for call in stream.calls if call["status"] == "unknown")["prompt_tokens"])

    def test_late_old_phase_audits_recover_on_early_close_without_double_counting(self):
        owner, stream, fixture = self.execute()
        first = next(stream)
        self.assertEqual(first.row_id, "occ-0")
        report = owner.close(timeout=2)
        self.assertEqual(report["status"], "closed")
        self.assertEqual(report["cleanup_errors"], [])
        self.assertEqual(len(stream.calls), 6)
        self.assertEqual(len({call["call_id"] for call in stream.calls}), 6)
        self.assertFalse(stream.completed)
        self.assertEqual(fixture.close_calls, 1)

    def test_later_result_overflow_keeps_prior_phase_and_late_current_usage(self):
        chain = relation_chain()
        fixture = ModelFixture(chain, oversize_stage="f1")
        owner, stream, fixture = self.execute(fixture=fixture,
                                             capacity=core.CoreChainCapacity(item_result_bytes=16000))
        with self.assertRaises(RuntimeError):
            list(stream)
        self.assertEqual(len(stream.calls), 6)
        self.assertEqual(sum(call["prompt_tokens"] for call in stream.calls), 66)
        self.assertEqual(owner.core.capacity.usage().held_tasks, 1)
        self.assertEqual(owner.last_close_report["status"], "unknown")
        self.assertFalse(any("call_audit" in error for error in owner.last_close_report["cleanup_errors"]))
        self.assertEqual(len(fixture.engines), 1)

    def test_cancel_after_complete_barrier_starts_no_successor_model_call(self):
        cancelled = threading.Event()
        owner, stream, fixture = self.execute(cancelled=cancelled.is_set)
        original = owner._advance_relation_phase

        def stop_after_phase(value):
            original(value)
            cancelled.set()

        with patch.object(owner, "_advance_relation_phase", side_effect=stop_after_phase):
            with self.assertRaises(InterruptedError):
                next(stream)
        self.assertEqual({row["algorithm_stage"] for row in fixture.prepared}, {"e0"})
        self.assertEqual(owner.core.capacity.usage().held_tasks, 0)
        self.assertEqual(len(fixture.engines), 1)
        self.assertFalse(stream.completed)

    def test_source_error_after_all_expected_rows_cannot_masquerade_as_eof(self):
        owner, stream, fixture = self.execute()
        records = source_records()

        def source():
            yield from records
            raise PermissionError("CPU source error after declared rows")

        stream.source = iter(source())
        with self.assertRaisesRegex(PermissionError, "source error"):
            list(stream)
        self.assertEqual({row["algorithm_stage"] for row in fixture.prepared}, {"e0"})
        self.assertEqual(owner.phase_history, [])
        self.assertFalse(stream.completed)

    def test_source_close_error_after_eof_remains_the_primary_and_stops_successor(self):
        owner, stream, fixture = self.execute()
        primary = PermissionError("CPU original source close failure")

        class Source:
            def __init__(self):
                self.rows = iter(source_records())

            def __iter__(self):
                return self

            def __next__(self):
                return next(self.rows)

            def close(self):
                raise primary

        stream.source = Source()
        with self.assertRaises(PermissionError) as caught:
            list(stream)
        self.assertIs(caught.exception, primary)
        self.assertEqual({row["algorithm_stage"] for row in fixture.prepared}, {"e0"})
        self.assertEqual(len(stream.calls), 4)
        self.assertEqual(owner.phase_history, [])
        self.assertFalse(stream.completed)

    def test_real_cpu_private_process_is_the_same_across_relation_segments(self):
        from tests.execution_provider.test_lotus_record_chain_core import method_chain

        # The established private process fixture emits an Extract `aspect`.
        legacy = method_chain("E", query_id="core-extract-relations")
        suffix = StageSpec("m1", MapMethod("openai/local-fixture", "Summarize {aspect}.", "summary"),
                           GenerationSpec(max_tokens=32, stop=()))
        chain = LotusRecordChain((legacy.stages[0], suffix), query_id=legacy.query_id,
                                 relation_barriers=True)
        spec = EngineSpec(self.temp.name, sys.executable, sys.executable, max_model_calls=8)
        owner = core.SemLoomRecordChainCore(spec, RecordsResources(cpu_workers=1,
            max_active_batches=1, model_batch_rows=2, query_timeout_s=30), control_dir=self.temp.name)
        owner._batch_type = type("CpuPipeBatch", (FusedChainBatch,), {"_engine_type": CpuProcess})
        self.owners.append(owner)
        records = source_records()
        stream = owner.evaluate(iter(records), chain, expected=records, query_id=chain.query_id)
        self.assertEqual(len(list(stream)), 4)
        self.assertEqual(len({call["model_pid"] for call in stream.calls}), 1)
        self.assertTrue(all(call["model_pid"] != os.getpid() and not call["lotus_imported"]
                            for call in stream.calls))
        self.assertEqual(owner.last_close_report["status"], "closed")
        self.assertTrue(owner.last_close_report["model_owner"]["process_exit_confirmed"])
        self.assertIsNone(owner.last_close_report["gpu_exit_confirmed"])


if __name__ == "__main__":
    unittest.main()
