"""Real SessionEngine/async transport; Ray, Daft, LOTUS and model are CPU fixtures."""

from concurrent.futures import Future, ThreadPoolExecutor, wait, FIRST_COMPLETED
import asyncio
from contextlib import ExitStack
from dataclasses import replace
import json
import os
from pathlib import Path
import sys
import threading
import time
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch

CODE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir())
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.execution_provider.adapters import lotus_records_core as core
from src.execution_provider.adapters import lotus_records_static as static
from src.execution_provider.adapters.lotus_records_actor_owner import RecordsActorOwner
from src.infrastructure import daft_runtime
from src.scheduling.core.session import SessionEngine
from src.scheduling.core.session_contract import State
from src.semantic_methods.lotus import engine as lotus_engine
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources, identity
from src.serving.backends import vllm_engine
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec


class Source:
    csv_path = Path("/tmp/core-records-fixture.csv")

    def __init__(self, rows):
        self.raw = rows

    def preflight(self, method):
        method.describe()

    def expected(self, query_id):
        return [identity(query_id, row["row_id"], row["source_position"])
                for row in self.raw if row["id"] == "taken_3"]


class Runtime:
    def __init__(self, rows, *, result_change=None, block=None, slow_submit=None,
                 fail_close=False, source_close_error=False, prepare_error=False):
        self.rows, self.events, self.engines, self.pools = rows, [], [], []
        self.result_change, self.block, self.slow_submit = result_change, block, slow_submit
        self.fail_close = fail_close
        runtime = self

        class Ref:
            def __init__(self, future, tag):
                self.value, self.tag = future, tag

            def future(self):
                runtime.events.append(("future", self.tag, threading.get_ident()))
                return self.value

        class Method:
            def __init__(self, actor, name):
                self.actor, self.name = actor, name

            def remote(self, *args):
                tag = type(self.actor.instance).__name__ + "." + self.name
                runtime.events.append(("submit", tag, threading.get_ident(), args))
                if runtime.slow_submit is not None and self.name == "run_core":
                    runtime.slow_submit.wait(2)

                def invoke():
                    resolved = [arg.value.result() if isinstance(arg, Ref) else arg for arg in args]
                    return getattr(self.actor.instance, self.name)(*resolved)
                return Ref(self.actor.pool.submit(invoke), tag)

        class Actor:
            def __init__(self, instance):
                self.instance = instance
                self.pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="fixture-actor")
                runtime.pools.append(self.pool)

            def __getattr__(self, name):
                return Method(self, name)

        class ActorClass:
            def __init__(self, cls):
                self.cls, self.config = cls, {}

            def options(self, **kwargs):
                self.config = kwargs
                return self

            def remote(self, *args):
                runtime.events.append(("actor", self.cls.__name__, dict(self.config)))
                return Actor(self.cls(*args))

        ray = self.ray = ModuleType("ray")
        ray.__version__, ray.is_initialized = "2.56.1", lambda: True
        ray.remote = ActorClass

        def get(ref):
            runtime.events.append(("driver_get", ref.tag))
            return ref.value.result()

        def ray_wait(refs, **kwargs):
            if refs:
                wait([ref.value for ref in refs], timeout=kwargs.get("timeout", 0),
                     return_when=FIRST_COMPLETED)
            ready = [ref for ref in refs if ref.value.done()][:kwargs.get("num_returns", 1)]
            return ready, [ref for ref in refs if ref not in ready]

        ray.get, ray.wait = get, ray_wait
        ray.cancel = lambda ref, **kw: runtime.events.append(("cancel", ref.tag))
        ray.kill = lambda actor, **kw: runtime.events.append(("kill", type(actor.instance).__name__))

        class Formatter:
            def __init__(self, method):
                runtime.events.append(("formatter",))

            def prepare_rows(self, rows):
                if prepare_error:
                    raise ValueError("fixture preparation failure")
                runtime.events.append(("prepare", tuple(row["source_position"] for row in rows)))
                return [{**{k: v for k, v in row.items() if k != "reviewText"},
                         "messages_json": "prepared message stays off driver"} for row in rows]

        class Engine:
            def __init__(self, spec, generation):
                self.calls, self.closes = [], 0
                runtime.engines.append(self)

            def ready(self):
                return dict(engine_uid="fixture-resident-engine", pid=1, physical_gpu=False)

            def run_prepared(self, rows, *, deadline_monotonic):
                self.calls.extend(row["call_id"] for row in rows)
                if runtime.block is not None:
                    runtime.block.wait(3)
                values = []
                for row in rows:
                    value = {**{k: v for k, v in row.items() if k != "messages_json"},
                             "status": "succeeded", "text": "True", "prompt_tokens": 11,
                             "completion_tokens": 1, "total_tokens": 12,
                             "prompt_token_ids_json": "[1,2]", "output_token_ids_json": "[3]",
                             "logprobs_json": "null", "finish_reason": "stop",
                             "engine_uid": "fixture-resident-engine",
                             "engine_request_id": row["call_id"], "submission_confirmed": True}
                    if runtime.result_change is not None:
                        value = runtime.result_change(value)
                    values.append(value)
                return values

            def close(self, timeout):
                self.closes += 1
                confirmed = not runtime.fail_close or self.closes > 1
                return dict(status="closed" if confirmed else "unknown", attempt=self.closes,
                            engine_api_shutdown_confirmed=confirmed, gpu_exit_confirmed=None)

        class Expr:
            def __init__(self, name):
                self.name = name

            def __eq__(self, value):
                return self.name, value

        class Frame:
            def __init__(self, rows):
                self.rows = rows

            def filter(self, expression):
                runtime.events.append(("filter", expression))
                return Frame([row for row in self.rows if row[expression[0]] == expression[1]])

            def select(self, *columns):
                runtime.events.append(("select", columns))
                return Frame([{k: row[k] for k in columns} for row in self.rows])

            def iter_rows(self, **kwargs):
                def generate():
                    try:
                        for row in self.rows:
                            runtime.events.append(("source_row", row["source_position"]))
                            yield row
                    finally:
                        runtime.events.append(("source_close",))
                        if source_close_error:
                            raise ValueError("fixture source close failure")
                return generate()

        daft = self.daft = ModuleType("daft")
        daft.__version__ = "0.7.21"
        daft.col = Expr
        daft.DataType = SimpleNamespace(string=lambda: "string", int64=lambda: "int64")
        daft.read_csv = lambda *a, **kw: Frame(runtime.rows)
        runners = ModuleType("daft.runners")
        self.runner = [None]
        runners._get_runner = lambda: self.runner[0]
        runners.get_or_create_runner = lambda: self.runner[0]
        daft.set_runner_native = lambda: self.runner.__setitem__(0, SimpleNamespace(name="native"))
        self.modules = {"ray": ray, "daft": daft, "daft.runners": runners}
        self.stack = ExitStack()
        self.stack.enter_context(patch.dict(sys.modules, self.modules))
        self.stack.enter_context(patch.object(daft_runtime, "_configured", None))
        self.stack.enter_context(patch.dict(os.environ))
        os.environ.pop("DAFT_RUNNER", None)
        self.stack.enter_context(patch.object(EngineSpec, "validate_driver_environment", return_value={}))
        self.stack.enter_context(patch.object(lotus_engine, "validate_filter_source", return_value={}))
        self.stack.enter_context(patch.object(lotus_engine, "LotusFilterMethod", Formatter))
        self.stack.enter_context(patch.object(vllm_engine, "VllmEngine", Engine))

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self.block is not None:
            self.block.set()
        if self.slow_submit is not None:
            self.slow_submit.set()
        for pool in self.pools:
            pool.shutdown(wait=True)
        self.stack.close()


class CoreRecordsTests(unittest.TestCase):
    def setUp(self):
        self.rows = [dict(row_id="row-" + str(i), source_position=i,
                          reviewId="repeated-review", id="taken_3", reviewText="review-" + str(i))
                     for i in range(5)]
        self.rows.insert(2, dict(row_id="excluded", source_position=99,
                                reviewId="other", id="other", reviewText="excluded"))
        self.spec = EngineSpec("/tmp/fixture-model", "/tmp/model-python", "/tmp/driver-python")

    def owner(self, **changes):
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1, **changes)
        return core.SemLoomRecordsCore(FilterMethod("fixture"), self.spec, GenerationSpec(), resources)

    def drain(self, owner, predicate=lambda: False):
        until = time.monotonic() + 2
        while time.monotonic() < until:
            owner.core.reap(owner.capacity.step_actions)
            if not owner.core.capacity.usage().held_tasks or predicate():
                return
            owner.core.wake.wait(owner.core.wake.generation, 0.005)
        self.fail("fixture Core did not settle within two seconds")

    def test_real_core_path_leases_identity_and_no_prepared_driver_get(self):
        with Runtime(self.rows) as runtime:
            owner = self.owner()
            stream = owner.evaluate(Source(self.rows), query_id="q")
            self.assertIsInstance(owner.core, SessionEngine)
            release = stream.session.release
            with patch.object(stream.session, "release", wraps=release) as releases:
                results = list(stream)
            report = owner.close()
        self.assertEqual(releases.call_count, 5)
        self.assertEqual({v["source_position"] for v in results}, set(range(5)))
        self.assertEqual(len({v["call_id"] for v in results}), 5)
        self.assertEqual(sum(v["prompt_tokens"] for v in results), 55)
        self.assertEqual(stream.last_close_report["status"], "closed")
        self.assertEqual(owner.core.capacity.usage().held_tasks, 0)
        self.assertTrue(report["production_core_connected"])
        self.assertEqual(report["status"], "unknown")
        self.assertIsNone(report["gpu_exit_confirmed"])
        self.assertTrue(report["transport_closed"])
        self.assertFalse(any(e[0] == "driver_get" and e[1].endswith(".prepare") for e in runtime.events))
        submissions = [e for e in runtime.events if e[0] == "submit" and e[1].endswith("run_core")]
        self.assertTrue(all(e[2] != threading.get_ident() for e in submissions))
        self.assertTrue(all(e[3][0].tag.endswith(".prepare") for e in submissions))
        actors = [e for e in runtime.events if e[0] == "actor"]
        self.assertEqual(actors[-1][2]["num_gpus"], 1)
        self.assertEqual(actors[-1][2]["num_cpus"], 1)
        self.assertTrue(all(e[2]["max_task_retries"] == 0 for e in actors))

    def test_matching_single_row_static_reference_has_same_result_and_usage(self):
        results = []
        for owner_type in (static.SemLoomRecordsStaticReference, core.SemLoomRecordsCore):
            with Runtime(self.rows) as runtime:
                owner = owner_type(FilterMethod("fixture"), self.spec, GenerationSpec(),
                                   RecordsResources(prepare_batch_rows=1, model_batch_rows=1))
                rows = list(owner.evaluate(Source(self.rows), query_id="same"))
                results.append(sorted(rows, key=lambda v: v["source_position"]))
                owner.close()
        self.assertEqual(results[0], results[1])

    def test_two_queries_share_one_model_but_keep_call_and_session_identity(self):
        with Runtime(self.rows) as runtime:
            owner = self.owner()
            first = list(owner.evaluate(Source(self.rows), query_id="first"))
            first_id = owner._active.session.session_id
            second = list(owner.evaluate(Source(self.rows), query_id="second"))
            second_id = owner._active.session.session_id
            owner.close()
        self.assertEqual(len(runtime.engines), 1)
        self.assertEqual(len(runtime.engines[0].calls), 10)
        self.assertNotEqual(first_id, second_id)
        self.assertTrue({v["call_id"] for v in first}.isdisjoint(v["call_id"] for v in second))

    def test_shared_owner_keeps_constructor_failure_unconfirmed(self):
        with Runtime(self.rows) as runtime, patch.object(
            vllm_engine, "VllmEngine", side_effect=RuntimeError("constructor failed after allocation"),
        ) as constructor:
            owner = self.owner()
            self.assertIsInstance(owner._actors, RecordsActorOwner)
            self.assertNotIsInstance(owner._actors, static.SemLoomRecordsStaticReference)
            with self.assertRaisesRegex(RuntimeError, "constructor failed after allocation"):
                owner.evaluate(Source(self.rows), query_id="q")
            report = owner.close(timeout=1)
            self.assertEqual(report["status"], "unknown")
            self.assertFalse(report["model_owner"]["engine_close_report"]["engine_api_shutdown_confirmed"])
            self.assertIsNone(report["gpu_exit_confirmed"])
            self.assertIsNotNone(owner._actors.model_actor)
            self.assertFalse(any(event == ("kill", "_CoreModelActor") for event in runtime.events))
            self.assertEqual(constructor.call_count, 1)

    def test_shared_owner_wait_keeps_the_core_cleanup_deadline_clamp(self):
        owner = self.owner()
        ref = object()
        with patch.object(RecordsActorOwner, "wait_ready", return_value={}) as wait:
            owner._actors.close_deadline = 100.0
            owner._actors.wait_ready(ref, 200.0)
            wait.assert_called_once_with(ref, 100.0, None)
            wait.reset_mock()
            owner._actors.wait_ready(ref, 50.0)
            wait.assert_called_once_with(ref, 50.0, None)
        owner.close()

    def test_slow_consumer_limits_source_pull_and_core_reservations(self):
        with Runtime(self.rows) as runtime:
            owner = self.owner(max_active_batches=2)
            stream = owner.evaluate(Source(self.rows), query_id="q")
            next(stream)
            self.assertLessEqual(sum(e[0] == "source_row" for e in runtime.events), 2)
            self.assertLessEqual(stream.peak_usage["held_tasks"], 2)
            self.assertLessEqual(stream.peak_usage["active_requests"], 2)
            list(stream)
            owner.close()

    def test_reverse_completion_keeps_original_occurrence_and_each_lease_once(self):
        rows = self.rows[:2]
        execute = core._RecordTransport._execute

        async def reordered(transport, task, endpoint):
            value = await execute(transport, task, endpoint)
            if task.key.sequence == 0:
                await asyncio.sleep(0.03)
            return value

        with Runtime(rows), patch.object(core._RecordTransport, "_execute", reordered):
            owner = self.owner(max_active_batches=2)
            stream = owner.evaluate(Source(rows), query_id="reverse")
            results = list(stream)
            owner.close()
        self.assertEqual([value["source_position"] for value in results], [1, 0])
        self.assertEqual([value["call_id"] for value in results],
                         [identity("reverse", row["row_id"], row["source_position"])["call_id"]
                          for row in reversed(rows)])
        self.assertEqual(stream.session._next_lease, 2)

    def test_control_advance_does_not_wait_for_slow_ray_submit(self):
        gate = threading.Event()
        with Runtime(self.rows, slow_submit=gate):
            owner = self.owner()
            stream = owner.evaluate(Source(self.rows), query_id="q")
            stream._fill()
            start = time.monotonic()
            stream.session.advance(1)
            self.assertLess(time.monotonic() - start, 0.1)
            gate.set()
            list(stream)
            owner.close()

    def test_early_close_retains_compute_until_real_terminal_and_no_new_input(self):
        gate = threading.Event()
        with Runtime(self.rows, block=gate) as runtime:
            owner = self.owner(max_active_batches=1)
            stream = owner.evaluate(Source(self.rows), query_id="q")
            stream._fill()
            stream.session.advance(1)
            report = stream.close()
            self.assertEqual(report["status"], "unknown")
            self.assertEqual(owner.core.capacity.usage().active_requests, 1)
            with self.assertRaisesRegex(RuntimeError, "unconfirmed"):
                owner.evaluate(Source(self.rows), query_id="next")
            gate.set()
            self.drain(owner)
            self.assertEqual(owner.core.capacity.usage().active_requests, 0)
            self.assertEqual(sum(e[0] == "source_row" for e in runtime.events), 1)
            owner.close()

    def test_unknown_model_response_retains_reservations_and_usage_receipt(self):
        def unknown(value):
            value.update(status="unknown", text=None, submission_confirmed=True)
            return value
        with Runtime(self.rows, result_change=unknown):
            owner = self.owner(max_active_batches=1)
            received = []
            stream = owner.evaluate(Source(self.rows), query_id="q")
            with self.assertRaisesRegex(RuntimeError, "execution failed"):
                received.extend(stream)
            report = owner.close()
            self.assertEqual(owner.core.capacity.usage().active_requests, 1)
            self.assertEqual(owner.core.capacity.usage().held_tasks, 1)
            self.assertEqual(report["retained_remote_inputs"], 1)
            self.assertEqual(report["status"], "unknown")
            self.assertEqual(received[0]["status"], "unknown")
            self.assertTrue(received[0]["submission_confirmed"])
            self.assertEqual(received[0]["prompt_tokens"], 11)

    def test_model_failure_preserves_success_prefix_and_stops_new_calls(self):
        def failure(value):
            if value["source_position"] == 1:
                value.update(status="failed", text=None, error_type="fixture")
            return value
        with Runtime(self.rows, result_change=failure) as runtime:
            owner = self.owner(max_active_batches=1)
            received = []
            with self.assertRaisesRegex(RuntimeError, "execution failed"):
                received.extend(owner.evaluate(Source(self.rows), query_id="q"))
            report = owner.close()
        self.assertEqual([v["status"] for v in received], ["succeeded", "failed"])
        self.assertEqual(sum(v["prompt_tokens"] for v in received), 22)
        self.assertEqual(len(runtime.engines[0].calls), 2)
        self.assertEqual(report["core_usage"]["held_tasks"], 0)

    def test_preparation_exception_is_unconfirmed_and_never_calls_model(self):
        with Runtime(self.rows, prepare_error=True) as runtime:
            owner = self.owner(max_active_batches=1)
            with self.assertRaisesRegex(RuntimeError, "execution failed"):
                list(owner.evaluate(Source(self.rows), query_id="q"))
            report = owner.close()
        self.assertEqual(runtime.engines[0].calls, [])
        self.assertEqual(report["core_usage"]["active_requests"], 1)
        self.assertEqual(report["status"], "unknown")

    def test_duplicate_or_changed_result_identity_is_not_delivered_or_retried(self):
        def corrupt(value):
            value["call_id"] = "wrong-query-call"
            return value
        with Runtime(self.rows, result_change=corrupt) as runtime:
            owner = self.owner(max_active_batches=1)
            with self.assertRaisesRegex(RuntimeError, "execution failed"):
                list(owner.evaluate(Source(self.rows), query_id="q"))
            report = owner.close()
        self.assertEqual(len(runtime.engines[0].calls), 1)
        self.assertEqual(report["core_usage"]["active_requests"], 1)

    def test_too_small_result_reservation_is_rejected_before_model_allocation(self):
        with Runtime(self.rows) as runtime:
            owner = self.owner(max_active_batches=1)
            owner.capacity = replace(owner.capacity, item_result_bytes=16)
            with self.assertRaisesRegex(ValueError, "reserved item capacity"):
                list(owner.evaluate(Source(self.rows), query_id="q"))
            report = owner.close()
        self.assertEqual(runtime.engines, [])
        self.assertFalse(report["production_core_connected"])

    def test_oversized_result_preserves_known_usage_in_bounded_failure_receipt(self):
        def large(value):
            value["text"] = "x" * 8192
            return value
        with Runtime(self.rows, result_change=large) as runtime:
            owner = self.owner(max_active_batches=1)
            owner.capacity = replace(owner.capacity, item_result_bytes=1024)
            received = []
            with self.assertRaisesRegex(RuntimeError, "execution failed"):
                received.extend(owner.evaluate(Source(self.rows), query_id="q"))
            report = owner.close()
        self.assertEqual(len(runtime.engines[0].calls), 1)
        self.assertEqual(received[0]["prompt_tokens"], 11)
        self.assertEqual(received[0]["completion_tokens"], 1)
        self.assertEqual(received[0]["total_tokens"], 12)
        self.assertEqual(received[0]["status"], "failed")
        self.assertEqual(received[0]["error_type"], "ResultCapacityExceeded")
        self.assertTrue(received[0]["submission_confirmed"])
        self.assertNotIn("x" * 1024, json.dumps(received[0]))
        self.assertEqual(report["core_usage"]["held_tasks"], 0)

    def test_oversized_unknown_keeps_both_known_usage_and_unknown_reservations(self):
        def unknown(value):
            value.update(status="unknown", text="x" * 8192)
            return value
        with Runtime(self.rows, result_change=unknown):
            owner = self.owner(max_active_batches=1)
            owner.capacity = replace(owner.capacity, item_result_bytes=1024)
            received = []
            with self.assertRaisesRegex(RuntimeError, "execution failed"):
                received.extend(owner.evaluate(Source(self.rows), query_id="q"))
            report = owner.close()
        self.assertEqual(received[0]["status"], "unknown")
        self.assertEqual(received[0]["prompt_tokens"], 11)
        self.assertEqual(report["core_usage"]["active_requests"], 1)

    def test_bad_suffix_does_not_erase_already_delivered_prefix(self):
        with Runtime(self.rows):
            owner = self.owner(max_active_batches=1)
            owner.capacity = replace(owner.capacity, item_input_bytes=512)
            stream = owner.evaluate(Source(self.rows), query_id="q")
            first = next(stream)
            self.rows[1]["reviewText"] = "x" * 1024
            # The fixture frame projected values before this mutation; change the
            # iterator explicitly to reproduce a malformed late producer value.
            stream.source_rows = iter([self.rows[1]])
            with self.assertRaisesRegex(ValueError, "reserved item capacity"):
                next(stream)
            owner.close()
        self.assertEqual(first["status"], "succeeded")
        self.assertEqual(first["prompt_tokens"], 11)

    def test_rejected_oversize_raw_input_never_enters_core_or_model(self):
        with Runtime(self.rows) as runtime:
            owner = self.owner()
            owner.capacity = replace(owner.capacity, item_input_bytes=1)
            with self.assertRaisesRegex(ValueError, "reserved item capacity"):
                list(owner.evaluate(Source(self.rows), query_id="q"))
            report = owner.close()
        self.assertEqual(runtime.engines[0].calls, [])
        self.assertEqual(report["core_usage"]["held_tasks"], 0)

    def test_preflight_runner_and_identity_checks_precede_actor_creation(self):
        for kind in ("source", "generation", "runner", "query"):
            with self.subTest(kind=kind), Runtime(self.rows) as runtime:
                owner = self.owner()
                source = Source(self.rows)
                if kind == "source":
                    source.preflight = lambda _: (_ for _ in ()).throw(ValueError("source binding"))
                elif kind == "generation":
                    owner.generation = GenerationSpec(repetition_penalty=1.1)
                elif kind == "runner":
                    runtime.runner[0] = SimpleNamespace(name="ray")
                with self.assertRaises((ValueError, RuntimeError)):
                    owner.evaluate(source, query_id="x" * 257 if kind == "query" else "q")
                self.assertEqual(runtime.engines, [])
                self.assertFalse(any(e[0] == "actor" for e in runtime.events))

    def test_empty_input_creates_no_model_and_closes_transport(self):
        with Runtime([]) as runtime:
            owner = self.owner()
            self.assertEqual(list(owner.evaluate(Source([]), query_id="empty")), [])
            report = owner.close()
        self.assertEqual(runtime.engines, [])
        self.assertTrue(report["transport_closed"])
        self.assertEqual(report["status"], "closed")

    def test_unconfirmed_model_close_retries_original_actor_and_preserves_scope(self):
        with Runtime(self.rows, fail_close=True) as runtime:
            owner = self.owner()
            list(owner.evaluate(Source(self.rows), query_id="q"))
            first = owner.close()
            self.assertFalse(first["model_owner"]["engine_close_report"]["engine_api_shutdown_confirmed"])
            self.assertFalse(any(e == ("kill", "_CoreModelActor") for e in runtime.events))
            second = owner.close(timeout=1)
        self.assertEqual(runtime.engines[0].closes, 2)
        self.assertTrue(second["model_owner"]["engine_close_report"]["engine_api_shutdown_confirmed"])
        self.assertEqual(second["status"], "unknown")
        self.assertIsNone(second["gpu_exit_confirmed"])

    def test_source_close_failure_remains_visible_and_does_not_leak_leases(self):
        with Runtime(self.rows, source_close_error=True):
            owner = self.owner()
            stream = owner.evaluate(Source(self.rows), query_id="q")
            next(stream)
            first = stream.close()
            self.assertTrue(first["cleanup_errors"])
            self.drain(owner)
            report = owner.close()
        self.assertEqual(report["core_usage"]["held_tasks"], 0)

    def test_cancel_signal_and_absolute_deadline_stop_input_without_reset(self):
        for kind in ("cancel", "deadline"):
            with self.subTest(kind=kind), Runtime(self.rows) as runtime:
                owner = self.owner(max_active_batches=1)
                cancelled = threading.Event()
                stream = owner.evaluate(Source(self.rows), query_id="q", cancelled=cancelled.is_set)
                next(stream)
                original = stream.deadline
                if kind == "cancel":
                    cancelled.set()
                else:
                    stream.deadline = time.monotonic() - 1
                with self.assertRaises(InterruptedError if kind == "cancel" else TimeoutError):
                    next(stream)
                self.assertEqual(len(runtime.engines[0].calls), 1)
                if kind == "cancel":
                    self.assertEqual(stream.deadline, original)
                owner.close()

    def test_owner_rejects_cross_thread_operation_before_source_access(self):
        with Runtime(self.rows) as runtime:
            owner = self.owner()
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(owner.evaluate, Source(self.rows), query_id="q")
                with self.assertRaisesRegex(RuntimeError, "single control thread"):
                    future.result()
            self.assertEqual(runtime.events, [])
            owner.close()

    def test_cancellation_observed_at_source_eof_prevents_dispatch(self):
        with Runtime(self.rows[:1]) as runtime:
            owner = self.owner(max_active_batches=2)
            cancelled = threading.Event()
            stream = owner.evaluate(Source(self.rows[:1]), query_id="q", cancelled=cancelled.is_set)

            def source():
                yield self.rows[0]
                cancelled.set()

            stream.source_rows = source()
            with self.assertRaises(InterruptedError):
                next(stream)
            report = owner.close()
        self.assertEqual(runtime.engines[0].calls, [])
        self.assertEqual(report["core_usage"]["held_tasks"], 0)

    def test_iterator_rejects_foreign_thread_before_pulling_records(self):
        with Runtime(self.rows) as runtime:
            owner = self.owner()
            stream = owner.evaluate(Source(self.rows), query_id="q")
            with ThreadPoolExecutor(max_workers=1) as executor:
                with self.assertRaisesRegex(RuntimeError, "single control thread"):
                    executor.submit(next, stream).result()
            self.assertFalse(any(e[0] == "source_row" for e in runtime.events))
            stream.close()
            owner.close()

    def test_late_cancelled_receipt_is_retained_in_old_report_and_not_next_query(self):
        gate = threading.Event()
        rows = self.rows[:1]
        with Runtime(rows, block=gate):
            owner = self.owner(max_active_batches=1)
            stream = owner.evaluate(Source(rows), query_id="old")
            stream._fill()
            stream.session.advance(1)
            stream.close()
            gate.set()
            self.drain(owner)
            second = owner.evaluate(Source(rows), query_id="new")
            old_report = owner.last_query_report
            results = list(second)
            owner.close()
        self.assertEqual(old_report["unconsumed_call_results"][0]["query_id"], "old")
        self.assertEqual(results[0]["query_id"], "new")
        self.assertEqual(owner.transport.failure_receipts(), ())

    def test_non_single_row_configuration_is_explicitly_rejected(self):
        for field in ("prepare_batch_rows", "model_batch_rows"):
            resources = RecordsResources(**{field: 2})
            with self.assertRaisesRegex(ValueError, "prepare_batch_rows=model_batch_rows=1"):
                core.SemLoomRecordsCore(FilterMethod("fixture"), self.spec, GenerationSpec(), resources)


if __name__ == "__main__":
    unittest.main()
