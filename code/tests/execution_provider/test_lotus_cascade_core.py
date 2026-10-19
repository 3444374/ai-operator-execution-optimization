"""Core cascade identity/generation/credit checks with explicit CPU fixtures."""

from contextlib import contextmanager
from copy import deepcopy
import csv
from dataclasses import replace
import json
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from src.execution_provider.adapters import lotus_cascade_core as cascade_core
from src.scheduling.core.session import SessionEngine
from src.semantic_methods.lotus.cascade import CascadeStage, cascade_identity
from src.semantic_methods.lotus import engine as lotus_engine
from src.semantic_methods.lotus.records import FilterMethod, RecordScan, RecordsResources, identity
from src.serving.backends.vllm_engine import EngineSpec
from tests.execution_provider.test_lotus_records_core import Runtime


def method(column="body", model="fixture-main"):
    return FilterMethod(model, "Check {" + column + "}",
                        resolved_input_columns=(column,))


def stage(rows, *, name="helper_score", instruction=None, columns=("body",), role=None):
    role = role or ("helper" if name == "helper_score" else "main")
    options = dict(max_completion_tokens=8, temperature=0.0, logprobs=role == "helper")
    if role == "helper":
        options["top_logprobs"] = 10
    records = [dict(cascade_identity("cascade-query", row["row_id"], row["source_position"],
                                    index, model_role=role, stage=name),
                    **{column: row[column] for column in columns})
               for index, row in enumerate(rows)]
    return CascadeStage(name, role, "fixture-" + role,
                        instruction or "Check {" + columns[0] + "}", True, None,
                        ("True", "False"), options, tuple(records), columns)


class Source:
    def __init__(self, directory, rows, main, *, payload=("body",)):
        self.csv_path = Path(directory) / "input.csv"
        self.method, self.payload = main, payload
        self.rows = deepcopy(rows)
        self.scan = RecordScan(
            (("row_id", "string"), ("source_position", "int64"),
             *((column, "string") for column in payload), ("selected", "string")),
            payload, ("selected", "yes"),
        )
        with self.csv_path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=[name for name, _ in self.scan.csv_schema])
            writer.writeheader()
            writer.writerows(dict(row, selected=row.get("selected", "yes")) for row in rows)

    def preflight(self, value):
        if value.describe() != self.method.describe():
            raise ValueError("original source accepts only its main method")
        return dict(eligible_rows=sum(row.get("selected", "yes") == "yes" for row in self.rows))

    def scan_spec(self, value):
        self.preflight(value)
        return self.scan

    def expected(self, query_id):
        return [identity(query_id, row["row_id"], row["source_position"])
                for row in self.rows if row.get("selected", "yes") == "yes"]


@contextmanager
def core_runtime(rows, *, change=None):
    """Actual Core/transport, simulated actor APIs and explicit process receipts."""
    engines = []

    class Frame:
        def __init__(self, records):
            self.records = records

        def select(self, *names):
            return Frame([{name: row[name] for name in names} for row in self.records])

        def iter_rows(self, **kwargs):
            return iter(self.records)

    def read_csv(path, **kwargs):
        with Path(path).open(newline="", encoding="utf-8") as stream:
            records = list(csv.DictReader(stream))
        for row in records:
            row["source_position"] = int(row["source_position"])
        return Frame(records)

    class Formatter:
        def __init__(self, config):
            self.method = config

        def prepare_rows(self, records):
            return [dict({key: row[key] for key in cascade_core.IDENTITY_FIELDS},
                         messages_json=json.dumps([{"role": "user", "content": {
                             "instruction": self.method.instruction,
                             "inputs": {name: row[name] for name in self.method.resolved_input_columns},
                         }}])) for row in records]

    class Process:
        def __init__(self, spec, generation, control, deadline, startup_timeout_s):
            self.generation, self.control, self.calls = generation, control, []
            self.ready_report = dict(engine_uid="explicit-unit-fixture", physical_gpu_work=False)
            engines.append(self)

        def run_prepared(self, records):
            self.calls.extend(deepcopy(records))
            result = []
            for row in records:
                assert row["call_id"].startswith("lotus-cascade-v1:")
                assert (self.generation.logprobs == 10) == (row["model_role"] == "helper")
                value = dict({key: row[key] for key in cascade_core.IDENTITY_FIELDS},
                             status="succeeded", text="True", prompt_tokens=11,
                             completion_tokens=1, total_tokens=12, submission_confirmed=True,
                             engine_uid="explicit-unit-fixture", engine_request_id=row["call_id"],
                             finish_reason="stop", usage_source="explicit simulated unit fixture")
                result.append(value if change is None else change(value))
            return result

        def close(self, timeout):
            report = dict(status="closed", process_exit_confirmed=True,
                          engine_api=dict(engine_api_shutdown_confirmed=True), cleanup_errors=[],
                          gpu_exit_confirmed=None, model_pid=12345678, process_created=1.0,
                          evidence_scope="simulated process receipt; no OS claim")
            (self.control / "engine-unit.json").write_text(json.dumps(report))
            return report

    with Runtime(rows) as runtime:
        runtime.ray.nodes = lambda: [dict(Alive=True, NodeID="fixture-local")]
        runtime.ray.get_runtime_context = lambda: SimpleNamespace(get_node_id=lambda: "fixture-local")
        runtime.stack.enter_context(patch.object(runtime.daft, "read_csv", side_effect=read_csv))
        runtime.stack.enter_context(patch.object(lotus_engine, "LotusFilterMethod", Formatter))
        runtime.stack.enter_context(patch.object(lotus_engine, "bind_filter_method", side_effect=lambda value: value))
        runtime.stack.enter_context(patch.object(cascade_core._CascadeCoreModelActor,
                                                "_process_type", return_value=Process))
        fake_lotus = SimpleNamespace(nl_expression=SimpleNamespace(nle2str=lambda value, columns: value))
        runtime.stack.enter_context(patch.dict("sys.modules", {"lotus": fake_lotus}))
        yield runtime, engines


class CoreCascadeTests(unittest.TestCase):
    def setUp(self):
        self.main, self.helper = method(), method(model="fixture-helper")
        self.raw = [dict(row_id="duplicate-value-a", source_position=7, body="same input"),
                    dict(row_id="duplicate-value-b", source_position=9, body="same input")]
        self.directory = tempfile.TemporaryDirectory(prefix="cascade-core-unit-")
        self.addCleanup(self.directory.cleanup)
        self.source = Source(self.directory.name, self.raw, self.main)

    def backend(self, source=None, **changes):
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1,
                                     query_timeout_s=20, max_active_batches=2)
        backend = cascade_core.CoreCascadeBackend(source or self.source, engines={
            "main": EngineSpec("/tmp/main-fixture", "/tmp/model-python", "/tmp/driver-python"),
            "helper": EngineSpec("/tmp/helper-fixture", "/tmp/model-python", "/tmp/driver-python"),
        }, resources=resources, **changes)
        backend.scan, backend.methods = self.source.scan, dict(main=self.main, helper=self.helper)
        backend._checked, backend.max_possible_calls = True, 20
        backend.effective = dict(main=stage([], name="fallback").effective_kwargs,
                                 helper=stage([]).effective_kwargs)
        return backend

    def test_private_process_gets_full_close_window_through_existing_actor_owner(self):
        from src.baselines.text.frameworks.daft_engine_process import DEFAULT_CLOSE_TIMEOUT_S

        with core_runtime(self.raw) as (runtime, engines):
            rows = self.backend()(stage(self.raw))
            list(rows)
            with patch.object(engines[0], "close", wraps=engines[0].close) as close:
                rows.close()
            close.assert_called_once_with(timeout=DEFAULT_CLOSE_TIMEOUT_S)

    def test_core_cleanup_window_includes_private_rpc_and_teardown(self):
        from src.baselines.text.frameworks.daft_engine_process import DEFAULT_CLOSE_TIMEOUT_S

        owner = SimpleNamespace(close=Mock(return_value={}), _actors=None)
        cleanup = SimpleNamespace(stop=Mock(return_value={}))
        cascade_core._close_stage_owner(owner, cleanup, time.monotonic())
        owner.close.assert_called_once_with(timeout=DEFAULT_CLOSE_TIMEOUT_S + 5)

    def test_stage_source_round_trip_preserves_repeated_draws_and_empty_text(self):
        raw = [dict(self.raw[0], body=""), dict(self.raw[0], body="")]
        value = stage(raw, name="threshold_sample")
        with tempfile.TemporaryDirectory(prefix="cascade-alias-") as directory:
            source = cascade_core._StageSource(value, self.main, Path(directory), 4096)
            source.preflight(self.main)
            restored = [source.restore(dict(provider, status="succeeded"))
                        for provider in source.expected(source.query_id)]
            self.assertEqual([row["call_id"] for row in restored], [row["call_id"] for row in value.records])
            self.assertEqual(len({row["provider_call_id"] for row in restored}), 2)
            with source.csv_path.open(newline="") as stream:
                self.assertEqual([row["body"] for row in csv.DictReader(stream)], ["", ""])
            with source.csv_path.open("a") as stream:
                stream.write("changed")
            with self.assertRaisesRegex(ValueError, "changed"):
                source.preflight(self.main)

    def test_core_owns_credits_and_canonical_calls_reach_actual_stage_generation(self):
        repeated = [self.raw[0], self.raw[0], self.raw[1]]
        with core_runtime(self.raw) as (runtime, engines):
            backend = self.backend()
            self.assertFalse(backend.production_core_connected)
            first = backend(stage(self.raw))
            helper_values = list(first)
            self.assertIsInstance(first.owner.core, SessionEngine)
            self.assertTrue(backend.production_core_connected)
            with self.assertRaisesRegex(RuntimeError, "cleanup is unconfirmed"):
                backend(stage(repeated, name="threshold_sample"))
            first.close()
            second = backend(stage(repeated, name="threshold_sample"))
            sample_values = list(second)
            report = second.close()
        self.assertEqual([engine.generation.logprobs for engine in engines], [10, None])
        self.assertEqual([row["source_position"] for row in sample_values], [7, 7, 9])
        self.assertEqual(len({row["call_id"] for row in sample_values}), 3)
        self.assertEqual(len({row["provider_call_id"] for row in sample_values}), 3)
        self.assertEqual(sum(row["prompt_tokens"] for row in helper_values + sample_values), 55)
        self.assertTrue(report["stage_process_exit_confirmed"])
        self.assertIsNone(report["gpu_exit_confirmed"])
        self.assertEqual(report["retained_remote_inputs"], 0)
        self.assertEqual(report["core_usage"]["held_tasks"], 0)
        self.assertEqual(report["stage_csv"]["rows"], 3)
        self.assertFalse(any(event[0] == "driver_get" and event[1].endswith(".prepare")
                             for event in runtime.events))

    def test_distinct_helper_column_and_instruction_use_the_same_core(self):
        raw = [dict(row, heading="headline " + str(index)) for index, row in enumerate(self.raw)]
        source = Source(self.directory.name, raw, self.main, payload=("body", "heading"))
        self.source, self.helper = source, method("heading", "fixture-helper")
        with core_runtime(raw) as (_, engines):
            backend = self.backend(source)
            rows = backend(stage(raw, columns=("heading",)))
            list(rows)
            rows.close()
            messages = json.loads(engines[0].calls[0]["messages_json"])
        self.assertEqual(messages[0]["content"]["instruction"], "Check {heading}")
        self.assertEqual(messages[0]["content"]["inputs"], {"heading": "headline 0"})

    def test_source_mismatch_and_predicate_bypass_fail_before_model_allocation(self):
        for raw in ([dict(self.raw[0], body="changed")],
                    [dict(self.raw[0], row_id="absent")]):
            with self.subTest(raw=raw), core_runtime(self.raw) as (_, engines):
                backend = self.backend()
                with self.assertRaisesRegex(ValueError, "source"):
                    backend(stage(raw))
                self.assertFalse(engines)
                self.assertFalse(backend.production_core_connected)

    def test_stage_identity_generation_and_global_budget_are_checked_before_send(self):
        for mutation in (lambda value: replace(value, effective_kwargs=dict(value.effective_kwargs, logprobs=False)),
                         lambda value: replace(value, records=(dict(value.records[0], call_id="forged"),)),
                         lambda value: replace(value, records=(value.records[0], value.records[0]))):
            with self.subTest(mutation=mutation), core_runtime(self.raw) as (_, engines):
                backend = self.backend()
                with self.assertRaises(ValueError):
                    backend(mutation(stage(self.raw)))
                self.assertFalse(engines)
        with core_runtime(self.raw) as (_, engines):
            backend = self.backend()
            backend.max_possible_calls = 1
            with self.assertRaisesRegex(ValueError, "global"):
                backend(stage(self.raw))
            self.assertFalse(engines)

    def test_engine_lifetime_budget_and_query_call_bound_have_distinct_scopes(self):
        with core_runtime(self.raw):
            backend = self.backend()
            backend.engines["main"] = replace(backend.engines["main"], max_model_calls=2)
            rows = backend(stage(self.raw, name="threshold_sample"))
            list(rows)
            rows.close()
            final = backend(stage([self.raw[0]], name="fallback"))
            list(final)
            final.close()
            self.assertEqual(backend._reserved["main"], 3)

    def test_failed_receipt_keeps_known_usage_and_unknown_work_retains_core_credit(self):
        for status in ("failed", "unknown"):
            def change(value):
                return dict(value, status=status, error_type="ExplicitFixtureFailure")
            with self.subTest(status=status), core_runtime(self.raw, change=change):
                backend = self.backend()
                rows = backend(stage([self.raw[0]]))
                first = next(rows)
                self.assertEqual(first["status"], status)
                self.assertEqual(first["prompt_tokens"], 11)
                with self.assertRaises(RuntimeError):
                    next(rows)
                if status == "failed":
                    self.assertTrue(rows.close()["stage_process_exit_confirmed"])
                else:
                    with self.assertRaisesRegex(RuntimeError, "unconfirmed"):
                        rows.close()
                    self.assertGreater(rows.owner.core.capacity.usage().held_tasks, 0)
                    with self.assertRaisesRegex(RuntimeError, "previous"):
                        backend(stage(self.raw, name="fallback"))

    def test_unconfirmed_process_exit_blocks_next_stage_and_preserves_delivered_usage(self):
        with core_runtime(self.raw):
            backend = self.backend()
            rows = backend(stage(self.raw))
            values = list(rows)
            with patch.object(rows.cleanup, "stop", return_value=dict(
                    reports=[], errors=["explicit unconfirmed process fixture"], process_exit_confirmed=False)):
                with self.assertRaisesRegex(RuntimeError, "unconfirmed"):
                    rows.close()
                self.assertEqual(sum(value["total_tokens"] for value in values), 24)
                with self.assertRaisesRegex(RuntimeError, "previous"):
                    backend(stage(self.raw, name="fallback"))
            self.assertTrue(rows.close()["stage_process_exit_confirmed"])

    def test_cancel_and_expired_global_deadline_allocate_no_core_or_model(self):
        for options in (dict(cancelled=lambda: True), {}):
            with self.subTest(options=options), core_runtime(self.raw) as (_, engines):
                backend = self.backend(**options)
                if not options:
                    backend.deadline = time.monotonic() - 1
                with self.assertRaises((InterruptedError, TimeoutError)):
                    backend(stage(self.raw))
                self.assertFalse(engines)
                self.assertFalse(backend.production_core_connected)

    def test_owner_close_exception_still_cleans_registered_processes(self):
        owner = SimpleNamespace(close=Mock(side_effect=ValueError("explicit fixture close failure")))
        cleanup = SimpleNamespace(stop=Mock(return_value=dict(
            reports=[], errors=[], process_exit_confirmed=True)))
        report, registered = cascade_core._close_stage_owner(owner, cleanup, True)
        cleanup.stop.assert_called_once_with(started=True)
        self.assertEqual(report["status"], "unknown")
        self.assertEqual(report["owner_close_error"], "ValueError")
        self.assertTrue(registered["process_exit_confirmed"])

    def test_owner_close_error_preserves_late_known_receipt_for_controller_recovery(self):
        value = stage([self.raw[0]])
        with tempfile.TemporaryDirectory(prefix="cascade-late-receipt-") as directory:
            source = cascade_core._StageSource(value, self.helper, Path(directory), 4096)
            source.materialization_seconds = 0.0
            receipt = dict(source.expected_rows[0], status="succeeded", prompt_tokens=11,
                           completion_tokens=1, total_tokens=12, submission_confirmed=True)
            active = SimpleNamespace(collect_receipts=Mock(), unconsumed_call_results=[receipt])
            owner = SimpleNamespace(close=Mock(side_effect=ValueError("late cleanup")),
                                    _active=active, last_query_report={"unconsumed_call_results": []},
                                    production_core_connected=True)
            cleanup = SimpleNamespace(control=Path(directory), stop=Mock(return_value=dict(
                reports=[], errors=[], process_exit_confirmed=True)))
            rows = cascade_core._CoreStageRows(SimpleNamespace(_reports={}), value, source,
                                               owner, iter(()), cleanup, time.monotonic())
            with self.assertRaisesRegex(RuntimeError, "unconfirmed"):
                rows.close()
            actual = rows.last_close_report["unconsumed_call_results"]
            self.assertEqual(actual[0]["prompt_tokens"], 11)
            self.assertEqual(actual[0]["call_id"], value.records[0]["call_id"])
            self.assertEqual(actual[0]["provider_call_id"], receipt["call_id"])
            self.assertFalse(rows.last_close_report["stage_process_exit_confirmed"])
            active.collect_receipts.assert_called_once()
            cleanup.stop.assert_called_once_with(started=True)

    def test_startup_error_is_not_replaced_by_owner_close_error(self):
        with core_runtime(self.raw):
            backend = self.backend()
            cleanup = Mock()
            cleanup.stop.return_value = dict(reports=[], errors=[], process_exit_confirmed=True)
            with patch.object(cascade_core, "StageEngineCleanup", return_value=cleanup), \
                    patch.object(cascade_core.SemLoomRecordsCore, "evaluate", side_effect=LookupError("startup")), \
                    patch.object(cascade_core.SemLoomRecordsCore, "close", side_effect=ValueError("close")):
                with self.assertRaisesRegex(LookupError, "startup"):
                    backend(stage(self.raw))
            cleanup.stop.assert_called_once_with(started=True)
            self.assertEqual(backend.close_reports[0]["owner_close_error"], "ValueError")
            self.assertFalse(backend.close_reports[0]["stage_process_exit_confirmed"])
            with self.assertRaisesRegex(RuntimeError, "previous"):
                backend(stage(self.raw, name="fallback"))

    def test_empty_stage_creates_no_actor_or_model(self):
        with core_runtime(self.raw) as (runtime, engines):
            backend = self.backend()
            rows = backend(stage([]))
            self.assertEqual(list(rows), [])
            report = rows.close()
        self.assertFalse(engines)
        self.assertFalse(any(event[0] == "actor" for event in runtime.events))
        self.assertTrue(report["stage_process_exit_confirmed"])
        self.assertEqual(report["stage_csv"]["rows"], 0)

    def test_oversized_engine_result_keeps_known_usage_in_bounded_failed_receipt(self):
        capacity = cascade_core.CoreRecordsCapacity(item_result_bytes=1000)
        with core_runtime(self.raw, change=lambda value: dict(value, text="x" * 2000)):
            backend = self.backend(capacity=capacity)
            rows = backend(stage([self.raw[0]]))
            receipt = next(rows)
            self.assertEqual(receipt["status"], "failed")
            self.assertEqual(receipt["error_type"], "ResultCapacityExceeded")
            self.assertEqual(receipt["prompt_tokens"], 11)
            self.assertTrue(receipt["result_fields_omitted"])
            self.assertTrue(receipt["call_id"].startswith("lotus-cascade-v1:"))
            with self.assertRaises(RuntimeError):
                next(rows)
            self.assertTrue(rows.close()["stage_process_exit_confirmed"])

    def test_failed_private_engine_construction_releases_wrapper_only_after_registered_exit(self):
        attempts = []

        class FailedProcess:
            def __init__(self, *args):
                attempts.append(args)
                raise LookupError("explicit private engine construction failure")

        with core_runtime(self.raw) as (runtime, _):
            backend = self.backend()
            registered = dict(reports=[], errors=["registration still unconfirmed"],
                              process_exit_confirmed=False)
            with patch.object(cascade_core._CascadeCoreModelActor, "_process_type", return_value=FailedProcess), \
                    patch.object(cascade_core.StageEngineCleanup, "stop", side_effect=lambda **kwargs: registered):
                with self.assertRaisesRegex(LookupError, "construction failure"):
                    backend(stage(self.raw))
                owner = backend._startup_failure[0]
                self.assertTrue(owner._actors._start_failed)
                self.assertIsNotNone(owner._actors.model_actor)
                model_name = type(owner._actors.model_actor.instance).__name__
                self.assertFalse(any(event[0] == "kill" and event[1] == model_name for event in runtime.events))
                waiting = backend.close()
                self.assertFalse(waiting["wrapper_actor_after_registered_exit"]["wrapper_actor_kill_request_confirmed"])
                registered = dict(reports=[], errors=[], process_exit_confirmed=True)
                settled = backend.close()
                self.assertTrue(settled["wrapper_actor_after_registered_exit"]["wrapper_actor_kill_request_confirmed"])
                self.assertEqual(sum(event[0] == "kill" and event[1] == model_name for event in runtime.events), 1)
                self.assertEqual(settled["status"], "unknown")
                self.assertIsNone(settled["gpu_exit_confirmed"])
                self.assertFalse(settled["stage_process_exit_confirmed"])
                backend.close()
                self.assertEqual(sum(event[0] == "kill" and event[1] == model_name for event in runtime.events), 1)
                self.assertEqual(len(attempts), 1, "cleanup must never retry a model constructor")

    def test_private_engine_receipt_recovers_known_usage_without_upgrading_delivered_unknown(self):
        value = stage([self.raw[0]])
        with tempfile.TemporaryDirectory(prefix="cascade-private-receipt-") as directory:
            source = cascade_core._StageSource(value, self.helper, Path(directory), 4096)
            source.materialization_seconds = 0.0
            receipt = dict({key: value.records[0][key] for key in cascade_core.IDENTITY_FIELDS},
                           status="succeeded", prompt_tokens=11, completion_tokens=1, total_tokens=12,
                           submission_confirmed=True, engine_uid="known-engine",
                           engine_protocol_error="damaged result envelope")
            owner = SimpleNamespace(close=lambda **kwargs: dict(
                status="unknown", transport_closed=False, retained_remote_inputs=1,
                cleanup_errors=[], last_query_report=None))
            cleanup = SimpleNamespace(control=Path(directory), stop=lambda **kwargs: dict(
                reports=[dict(result_failure=dict(verified_call_results=[receipt]),
                              engine_ready=dict(engine_uid="known-engine"))], errors=[],
                process_exit_confirmed=True))
            rows = cascade_core._CoreStageRows(SimpleNamespace(_reports={}), value, source,
                                               owner, iter(()), cleanup, time.monotonic())
            with self.assertRaisesRegex(RuntimeError, "unconfirmed"):
                rows.close()
            actual = rows.last_close_report["unconsumed_call_results"]
            self.assertEqual(actual[0]["prompt_tokens"], 11)
            self.assertEqual(actual[0]["provider_call_id"], source.expected_rows[0]["call_id"])
            self.assertIn("engine_protocol_error", actual[0])
            rows.audit.record(dict(receipt, status="unknown", prompt_tokens=None,
                                   completion_tokens=None, submission_confirmed=False))
            with self.assertRaisesRegex(RuntimeError, "unconfirmed"):
                rows.close()
            self.assertEqual(rows.last_close_report["unconsumed_call_results"], [])

    def test_private_engine_receipt_with_wrong_engine_identity_is_rejected(self):
        value = stage([self.raw[0]])
        source = SimpleNamespace(bindings={"provider": {
            key: value.records[0][key] for key in cascade_core.IDENTITY_FIELDS}})
        rows = cascade_core._CoreStageRows(SimpleNamespace(), value, source,
                                           None, iter(()), None, time.monotonic())
        receipt = dict(source.bindings["provider"], status="succeeded", submission_confirmed=True,
                       prompt_tokens=11, completion_tokens=1, engine_uid="wrong-engine",
                       engine_protocol_error="damaged result envelope")
        actual, errors = rows._recover_result_receipts([dict(
            result_failure=dict(verified_call_results=[receipt]),
            engine_ready=dict(engine_uid="known-engine"))], [])
        self.assertEqual(actual, [])
        self.assertEqual(errors, ["result_receipt_recovery: ValueError"])


if __name__ == "__main__":
    unittest.main()
