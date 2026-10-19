"""Native EOF/receipt boundaries over the shared Extract method, without models.

The fake native iterators isolate the source tail from model completion. Actual
Ray/Daft graph tests are added only against the separately delivered adapters.
"""

from copy import deepcopy
from dataclasses import asdict
import json
from pathlib import Path
import socket
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid

from src.baselines.text.frameworks import daft_lotus_chain, ray_data_lotus_chain
from src.baselines.text.frameworks.lotus_chain_batch import FusedChainBatch
from src.baselines.text.frameworks.lotus_chain_results import (
    ChainBatchResults, _source_binding, record_row,
)
from src.baselines.text.frameworks.lotus_chain_native_relation import NativeExtractRelationRows
from src.semantic_methods.lotus.record_chain import LotusRecordMethod, records_to_frame
from src.semantic_methods.lotus.record_relation import complete_extract_relation
from src.serving.backends.vllm_engine import EngineSpec
from tests.execution_provider.test_lotus_record_chain_core_relations import (
    ModelFixture, relation_chain, response, source_records,
)


class BoundaryFixture(ModelFixture):
    """Use the established responses, with distinct synthetic engine lifetimes."""

    def kernel_type(self):
        base = super().kernel_type()

        class Model(base):
            def __init__(self, *args):
                super().__init__(*args)
                self.uid = "native-boundary-cpu-" + uuid.uuid4().hex

            def run_prepared(self, *args, **kwargs):
                return [dict(row, engine_uid=self.uid)
                        for row in super().run_prepared(*args, **kwargs)]

        return Model


class NativeExtractRelationContractTests(unittest.TestCase):
    def setUp(self):
        import lotus

        self.scope = lotus.settings.context(enable_cache=False)
        self.scope.__enter__()
        self.addCleanup(self.scope.__exit__, None, None, None)
        self.no_process = patch("subprocess.Popen", side_effect=AssertionError("CPU fixture forbids Popen"))
        self.process = self.no_process.start()
        self.addCleanup(self.no_process.stop)
        self.temp = tempfile.TemporaryDirectory(prefix="native-extract-boundary-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.records = source_records()
        self.chain = relation_chain("EMF", query_id="native-extract-boundary")
        self.prefix, self.suffix = self.chain.relation_segments()
        self.streams = []

    def tearDown(self):
        for stream in self.streams:
            stream.close()
        self.process.assert_not_called()

    def kernel(self, fixture, chain, records, name, **options):
        control = self.root / name
        control.mkdir()
        (control / "owner").write_text("cpu-owner")

        class Batch(FusedChainBatch):
            _engine_type = fixture.kernel_type()

        worker = Batch(chain.describe(), asdict(EngineSpec(self.temp.name, sys.executable,
            sys.executable, max_model_calls=12)), [_source_binding(row) for row in records],
            str(control), time.monotonic() + 20, 5, socket.gethostname(), "cpu-owner", **options)
        return worker, control

    def prefix_stream(self, backend, *, tail_error=False, unknown_position=None,
                      close_raises=False, cancelled=None):
        fixture = BoundaryFixture(self.chain, unknown_position=unknown_position, close_raises=close_raises)
        worker, control = self.kernel(fixture, self.prefix, self.records, backend)
        # The duplicate label at positions 0 and 2 deliberately crosses batches.
        envelopes = [worker.run_rows([record_row(row) for row in chunk])
                     for chunk in (self.records[:2], self.records[2:])]
        eof = []

        def rows():
            for envelope in envelopes:
                yield {"batch_json": json.dumps(envelope)}
            if tail_error:
                raise OSError("CPU source tail failed after its last complete batch")
            eof.append(True)

        native = SimpleNamespace(iter_rows=rows)
        if backend == "ray":
            native._current_executor = SimpleNamespace(shutdown=lambda **_kwargs: None)
            stream = ray_data_lotus_chain._RayChainRows(native, self.records, self.prefix,
                time.monotonic() + 20, cancelled, control)
        else:
            stream = daft_lotus_chain._DaftChainRows(native, self.records, self.prefix,
                time.monotonic() + 20, cancelled, control)
        self.streams.append(stream)
        return stream, fixture, eof

    def original(self, records, chain):
        import lotus
        import pandas as pd
        from src.semantic_methods.lotus.engine import engine_response

        frame = pd.DataFrame([row.values for row in records], index=[row.source_index for row in records])
        for stage in chain.stages:
            method = LotusRecordMethod(stage)

            def answer(data, options, *_args, method=method):
                return [engine_response(dict(status="succeeded", text=response(method.stage, message),
                    submission_confirmed=True, prompt_tokens=11, completion_tokens=3,
                    total_tokens=14, finish_reason="stop", logprobs_json="null",
                    engine_request_id="original-cpu-replay"), method.lm)
                    for message, _media in data]

            method.lm._process_uncached_messages = answer
            with lotus.settings.context(lm=method.lm):
                if stage.kind == "extract":
                    frame = frame.sem_extract(list(stage.method.input_columns), stage.method.output_columns)
                elif stage.kind == "map":
                    frame = frame.sem_map(stage.method.instruction, suffix=stage.method.output_column)
                else:
                    frame = frame.sem_filter(stage.method.instruction, default=stage.method.default)
        return frame

    def relation_stream(self, backend, *, tail_error=False, unknown_position=None, close_raises=False,
                        columns=None, cancel_at_eof=False):
        fixture = BoundaryFixture(self.chain, unknown_position=unknown_position, close_raises=close_raises)
        control = self.root / (backend + "-owner")
        control.mkdir()
        opened = []
        cancelled = lambda: cancel_at_eof and fixture.source_eof

        def open_phase(index, segment, records, **options):
            opened.append([stage.stage_id for stage in segment.stages])
            worker, phase_control = self.kernel(fixture, segment, records,
                backend + "-owned-phase-" + str(index), **options)
            envelopes = [worker.run_rows([record_row(row) for row in chunk])
                         for chunk in (records[:2], records[2:]) if chunk]

            def rows():
                for envelope in envelopes:
                    yield {"batch_json": json.dumps(envelope)}
                if index == 0:
                    if tail_error:
                        raise OSError("owned native source tail failed")
                    fixture.source_eof = True

            native = SimpleNamespace(iter_rows=rows)
            if backend.startswith("ray"):
                native._current_executor = SimpleNamespace(shutdown=lambda **_kwargs: None)
                kind = ray_data_lotus_chain._RayChainRows
            else:
                kind = daft_lotus_chain._DaftChainRows
            return kind(native, records, segment, time.monotonic() + 20, cancelled, phase_control,
                initial_parents=options["initial_parents"], columns=options["columns"])

        stream = NativeExtractRelationRows(self.chain, self.records, open_phase, control,
            time.monotonic() + 20, cancelled, scheduler_owner=backend, columns=columns)
        self.streams.append(stream)
        return stream, fixture, opened

    def test_complete_native_eof_then_global_relation_changes_earlier_batch_and_group_parents(self):
        import pandas as pd

        for backend in ("ray", "daft"):
            with self.subTest(backend=backend):
                stream, fixture, eof = self.prefix_stream(backend)
                prefix_rows = list(stream)
                self.assertEqual(eof, [True])
                self.assertTrue(stream.completed)
                stream.verify_success(source_exhausted=True)
                stream.finalize_columns()
                # A local Extract record still has its own value, before the
                # original whole-relation .loc writes have been replayed.
                self.assertEqual(prefix_rows[0].values["shared_value"], "drop earlier")
                completed = complete_extract_relation(prefix_rows, columns=stream.columns,
                    extract_stage=self.prefix.stages[-1], query_id=self.chain.query_id,
                    calls=stream.calls, source_exhausted=True)
                self.assertEqual(completed.records[0].values["shared_value"], "keep broadcast")
                self.assertIsNone(completed.records[3].values["shared_value"])
                self.assertEqual(completed.records[0].values["row_id"], "data last")
                self.assertEqual(completed.records[0].values["call_id"], "last business")
                parents = completed.parent_call_ids
                self.assertEqual(parents[("occ-0", 0)], parents[("occ-2", 2)])
                self.assertEqual(len(parents[("occ-0", 0)]), 2)
                fixture.source_eof = True
                suffix_worker, _ = self.kernel(fixture, self.suffix, completed.records,
                    backend + "-suffix", initial_parents=parents, columns=completed.columns, allow_nulls=True)
                results = ChainBatchResults(completed.records, self.suffix,
                    initial_parents=parents, columns=completed.columns)
                for chunk in (completed.records[:2], completed.records[2:]):
                    results.accept(suffix_worker.run_rows([record_row(row) for row in chunk]))
                results.verify_success(source_exhausted=True)
                results.finalize_columns()
                final = list(results.pending)
                pd.testing.assert_frame_equal(records_to_frame(final, columns=results.columns),
                                               self.original(self.records, self.chain))
                self.assertEqual([row.row_id for row in final], ["occ-0", "occ-2"])
                calls = stream.calls + results.calls
                self.assertEqual(len({call["call_id"] for call in calls}), 12)
                self.assertEqual(sum(call["total_tokens"] for call in calls), 168)
                self.assertEqual(sum(call.get("decision") is False for call in calls), 2)
                self.assertEqual(len(fixture.engines), 2)  # Explicit reinitialization, not one physical engine.
                self.assertEqual(len({call["engine_uid"] for call in calls}), 2)
                for call in results.calls:
                    if call["algorithm_stage"] == self.suffix.stages[0].stage_id:
                        self.assertEqual(call["parent_call_ids"], list(parents[(call["row_id"], call["source_position"])]))
                before = deepcopy(calls)
                self.assertEqual(list(stream), [])
                self.assertIs(stream.close(), stream.last_close_report)
                self.assertEqual(stream.calls + results.calls, before)
                self.assertEqual(suffix_worker.close()["status"], "closed")

    def test_full_received_coverage_is_not_source_eof(self):
        for backend in ("ray", "daft"):
            with self.subTest(backend=backend):
                stream, fixture, eof = self.prefix_stream(backend, tail_error=True)
                with self.assertRaisesRegex(OSError, "source tail failed") as caught:
                    list(stream)
                self.assertEqual(stream.received, set(stream.expected))
                self.assertFalse(stream.completed)
                self.assertEqual(eof, [])
                self.assertEqual(len(caught.exception.chain_calls), 4)
                self.assertEqual(sum(call["total_tokens"] for call in stream.calls), 56)
                with self.assertRaises(ValueError):
                    stream.verify_success(source_exhausted=False)
                self.assertEqual(len(fixture.engines), 1)
                self.assertTrue(all(row["algorithm_stage"] == "e0" for row in fixture.prepared))

    def test_cancellation_after_last_result_preserves_usage_without_confirming_eof(self):
        for backend in ("ray", "daft"):
            with self.subTest(backend=backend):
                stop = []
                stream, fixture, eof = self.prefix_stream(backend, cancelled=lambda: bool(stop))
                self.assertEqual(len([next(stream) for _ in self.records]), 4)
                self.assertFalse(stream.completed)
                stop.append(True)
                with self.assertRaises(InterruptedError):
                    next(stream)
                self.assertFalse(stream.completed)
                self.assertEqual(sum(call["total_tokens"] for call in stream.calls), 56)
                self.assertEqual(len(fixture.engines), 1)
                self.assertEqual(list(stream), [])

    def test_unknown_prefix_keeps_known_usage_and_never_exposes_a_successful_prefix(self):
        for backend in ("ray", "daft"):
            with self.subTest(backend=backend):
                stream, fixture, eof = self.prefix_stream(backend, unknown_position=2)
                with self.assertRaises(RuntimeError):
                    list(stream)
                self.assertFalse(stream.completed)
                self.assertEqual(eof, [])
                self.assertEqual(len(stream.calls), 4)
                self.assertEqual(sum(call["total_tokens"] for call in stream.calls
                                     if call["total_tokens"] is not None), 42)
                self.assertEqual(sum(call["status"] == "unknown" for call in stream.calls), 1)
                with self.assertRaises(ValueError):
                    stream.verify_success(source_exhausted=True)
                self.assertEqual(len(fixture.engines), 1)

    def test_cleanup_failure_retains_completed_calls_but_does_not_confirm_prefix(self):
        for backend in ("ray", "daft"):
            with self.subTest(backend=backend):
                stream, fixture, eof = self.prefix_stream(backend, close_raises=True)
                with self.assertRaises(RuntimeError):
                    list(stream)
                self.assertFalse(stream.completed)
                self.assertEqual(len(stream.calls), 4)
                self.assertEqual(sum(call["total_tokens"] for call in stream.calls), 56)
                self.assertEqual(stream.last_close_report["status"], "unknown")
                self.assertIsNone(stream.last_close_report["gpu_exit_confirmed"])
                self.assertEqual(stream.close()["status"], "unknown")
                self.assertEqual(len(fixture.engines), 1)

    def test_explicit_owner_gates_suffix_and_retains_each_phase_once(self):
        import pandas as pd

        for backend in ("ray", "daft"):
            with self.subTest(backend=backend):
                stream, fixture, opened = self.relation_stream(backend)
                final = list(stream)
                pd.testing.assert_frame_equal(records_to_frame(final, columns=stream.columns),
                                               self.original(self.records, self.chain))
                self.assertEqual(opened, [["e0"], ["m1", "f2"]])
                self.assertEqual(len(stream.calls), 12)
                self.assertEqual(sum(call["total_tokens"] for call in stream.calls), 168)
                self.assertEqual(len(stream.phase_history), 2)
                self.assertTrue(all(row["verified_success"] for row in stream.phase_history))
                self.assertFalse(stream.last_close_report["engine_reuse_across_segments"])
                self.assertEqual(list(stream), [])
                self.assertEqual(len(fixture.submitted_ids), 12)
                self.assertEqual(stream.close()["status"], "closed")

    def test_owner_never_builds_suffix_for_source_unknown_or_cleanup_failure(self):
        for backend in ("ray", "daft"):
            for fault in ("tail", "unknown", "close"):
                with self.subTest(backend=backend, fault=fault):
                    # Each fault gets its own exact owned directories.
                    label = backend + "-" + fault
                    stream, fixture, opened = self.relation_stream(label,
                        tail_error=fault == "tail", unknown_position=2 if fault == "unknown" else None,
                        close_raises=fault == "close")
                    with self.assertRaises((OSError, RuntimeError)) as caught:
                        list(stream)
                    self.assertEqual(opened, [["e0"]])
                    self.assertFalse(stream.completed)
                    self.assertEqual(len(caught.exception.chain_calls), 4)
                    self.assertEqual(len(fixture.engines), 1)

    def test_failed_native_phase_construction_remains_unknown_on_repeated_close(self):
        control = self.root / "failed-phase-build"
        control.mkdir()

        def fail(*args, **kwargs):
            raise OSError("CPU graph constructor failed without a returned owner")

        stream = NativeExtractRelationRows(self.chain, self.records, fail, control,
            time.monotonic() + 20, None, scheduler_owner="fixture")
        self.streams.append(stream)
        with self.assertRaises(OSError):
            next(stream)
        self.assertEqual(stream.close()["status"], "unknown")
        self.assertIn("native_phase_construction_unconfirmed", stream.last_close_report["cleanup_errors"])
        self.assertIsNone(stream.last_close_report["gpu_exit_confirmed"])
        self.assertTrue(control.exists())

    def test_empty_source_preserves_declared_schema_without_model_allocation(self):
        self.records = []
        columns = ["reviewText", "row_id", "call_id", "optional"]
        for backend in ("ray", "daft"):
            with self.subTest(backend=backend):
                stream, fixture, opened = self.relation_stream(backend, columns=columns)
                self.assertEqual(list(stream), [])
                self.assertTrue(stream.completed)
                self.assertEqual(stream.columns, columns + ["mapped1"])
                self.assertEqual(stream.calls, [])
                self.assertEqual(fixture.engines, [])
                self.assertEqual(opened, [["e0"], ["m1", "f2"]])
                self.assertTrue(stream.last_close_report["gpu_exit_confirmed"])

    def test_all_false_prefix_keeps_usage_and_empty_suffix_schema_without_another_engine(self):
        self.chain = relation_chain("FEMF", query_id="native-all-false")
        for backend in ("ray", "daft"):
            with self.subTest(backend=backend):
                stream, fixture, opened = self.relation_stream(backend)
                self.assertEqual(list(stream), [])
                self.assertTrue(stream.completed)
                self.assertEqual(opened, [["f0", "e1"], ["m2", "f3"]])
                self.assertEqual(len(fixture.engines), 1)
                self.assertEqual(len(stream.calls), 4)
                self.assertTrue(all(call["decision"] is False for call in stream.calls))
                self.assertEqual(sum(call["total_tokens"] for call in stream.calls), 56)
                self.assertIn("mapped2", stream.columns)
                self.assertEqual(stream.phase_history[-1]["input_occurrences"], 0)

    def test_owner_cancellation_at_source_eof_never_opens_suffix(self):
        for backend in ("ray", "daft"):
            with self.subTest(backend=backend):
                stream, fixture, opened = self.relation_stream(backend, cancel_at_eof=True)
                with self.assertRaises(InterruptedError) as caught:
                    list(stream)
                self.assertEqual(opened, [["e0"]])
                self.assertFalse(stream.completed)
                self.assertEqual(len(caught.exception.chain_calls), 4)
                self.assertEqual(sum(call["total_tokens"] for call in stream.calls), 56)
                self.assertEqual(len(fixture.engines), 1)


if __name__ == "__main__":
    unittest.main()
