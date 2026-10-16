"""CPU substitutes for static reference ownership; no vendor/model qualification."""

from __future__ import annotations

import sys
import time
import unittest
from contextlib import ExitStack
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import patch


CODE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir())
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.execution_provider.adapters import lotus_records_static as static
from src.semantic_methods.lotus import engine as lotus_engine
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources, identity
from src.serving.backends import vllm_engine
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec


class Source:
    csv_path = Path("/tmp/software-fixture-records.csv")

    def __init__(self, rows):
        self.raw = rows

    def preflight(self, method):
        method.describe()

    def expected(self, query_id):
        return [identity(query_id, row["row_id"], row["source_position"])
                for row in self.raw if row["id"] == "taken_3"]

    def rows(self):
        raise AssertionError("static reference must use the native Daft scanner")


class StaticRecordsTests(unittest.TestCase):
    def setUp(self):
        self.rows = [dict(row_id="movie-row-" + str(i), source_position=i,
                          reviewId="repeated", id="taken_3", reviewText="text-" + str(i))
                     for i in range(5)]
        self.rows.insert(2, dict(row_id="excluded", source_position=99,
                                 reviewId="other", id="other", reviewText="excluded"))
        self.spec = EngineSpec("/tmp/fixture-model", "/tmp/model-python", "/tmp/driver-python")

    def _runtime(self, *, fail_first_close=False, fail_on_batch=None):
        events = []
        engines = []
        formatters = []
        outer = self

        class Ref:
            def __init__(self, tag, fn):
                self.tag, self.fn = tag, fn
                self.done = False
                self.value = None

            def resolve(self):
                if not self.done:
                    self.value = self.fn()
                    self.done = True
                return self.value

        class RemoteMethod:
            def __init__(self, actor, name):
                self.actor, self.name = actor, name

            def remote(self, *args):
                tag = type(self.actor).__name__ + "." + self.name
                events.append(("submit", tag, args))
                return Ref(tag, lambda: getattr(self.actor, self.name)(
                    *[arg.resolve() if isinstance(arg, Ref) else arg for arg in args]
                ))

        class Actor:
            def __init__(self, value):
                self.value = value

            def __getattr__(self, name):
                return RemoteMethod(self.value, name)

        class ActorClass:
            def __init__(self, cls):
                self.cls, self.options_value = cls, {}

            def options(self, **kwargs):
                self.options_value = kwargs
                return self

            def remote(self, *args):
                events.append(("actor", self.cls.__name__, dict(self.options_value)))
                return Actor(self.cls(*args))

        class Formatter:
            def __init__(self, method):
                self.calls = 0
                formatters.append(self)

            def prepare_rows(self, rows):
                self.calls += 1
                outer.assertTrue(all(set(row) == set(identity("q", row["row_id"], row["source_position"])) | {"reviewText"} for row in rows))
                return [{**{key: value for key, value in row.items() if key != "reviewText"},
                         "messages_json": "prepared text stays in object store"} for row in rows]

        class Engine:
            def __init__(self, engine, generation):
                self.calls, self.close_calls = [], 0
                engines.append(self)

            def ready(self):
                return dict(engine_uid="software-fixture-engine", pid=123, physical_gpu=False)

            def run_prepared(self, rows, *, deadline_monotonic):
                self.calls.append(len(rows))
                if len(self.calls) == fail_on_batch:
                    raise RuntimeError("fixture later sub-batch failure")
                return [{**{key: value for key, value in row.items() if key != "messages_json"},
                         "status": "succeeded", "text": "True", "prompt_tokens": 11,
                         "completion_tokens": 1, "total_tokens": 12} for row in rows]

            def close(self, timeout):
                self.close_calls += 1
                closed = not fail_first_close or self.close_calls > 1
                return dict(status="closed" if closed else "unknown",
                            engine_api_shutdown_confirmed=closed, gpu_exit_confirmed=None,
                            attempt=self.close_calls)

        ray = ModuleType("ray")
        ray.__version__ = "2.56.1"
        ray.is_initialized = lambda: True
        ray.remote = ActorClass

        def get(ref):
            events.append(("driver_get", ref.tag))
            return ref.resolve()

        ray.get = get
        # Reverse completion order independently of source and dispatch order.
        ray.wait = lambda refs, **kwargs: ([refs[-1]], refs[:-1])
        ray.cancel = lambda ref, **kwargs: events.append(("cancel", ref.tag, kwargs))
        ray.kill = lambda actor, **kwargs: events.append(("kill", type(actor.value).__name__, kwargs))

        class Expr:
            def __init__(self, name):
                self.name = name

            def __eq__(self, value):
                return (self.name, value)

        class Frame:
            def __init__(self, rows):
                self.rows = rows

            def filter(self, expr):
                events.append(("filter", expr))
                return Frame([row for row in self.rows if row[expr[0]] == expr[1]])

            def select(self, *columns):
                events.append(("select", columns))
                return Frame([{key: row[key] for key in columns} for row in self.rows])

            def iter_rows(self, **kwargs):
                events.append(("iter_rows", kwargs))
                def rows():
                    try:
                        yield from self.rows
                    finally:
                        events.append(("source_close",))
                return rows()

        daft = ModuleType("daft")
        daft.__version__ = "0.7.21"
        daft.col = Expr
        daft.DataType = SimpleNamespace(string=lambda: "string", int64=lambda: "int64")

        def read_csv(path, **kwargs):
            events.append(("read_csv", path, kwargs))
            return Frame(outer.rows)

        daft.read_csv = read_csv
        runners = ModuleType("daft.runners")
        runners.get_or_create_runner = lambda: SimpleNamespace(name="native")
        stack = ExitStack()
        stack.enter_context(patch.dict(sys.modules, {"ray": ray, "daft": daft, "daft.runners": runners}))
        stack.enter_context(patch.object(EngineSpec, "validate_local", return_value={}))
        stack.enter_context(patch.object(lotus_engine, "validate_filter_source", return_value={}))
        stack.enter_context(patch.object(lotus_engine, "LotusFilterMethod", Formatter))
        stack.enter_context(patch.object(vllm_engine, "VllmEngine", Engine))
        return stack, events, engines, formatters

    def _owner(self, **resources):
        return static.SemLoomRecordsStaticReference(
            FilterMethod("software-fixture"), self.spec, GenerationSpec(),
            RecordsResources(**resources),
        )

    def test_prepare_objectrefs_go_directly_to_model_and_results_keep_occurrences(self):
        stack, events, engines, formatters = self._runtime()
        with stack:
            owner = self._owner(prepare_batch_rows=2, model_batch_rows=1, max_active_batches=2)
            stream = owner.evaluate(Source(self.rows), query_id="q")
            results = list(stream)
            report = owner.close()
        self.assertEqual(sorted(row["source_position"] for row in results), list(range(5)))
        self.assertEqual(len({row["call_id"] for row in results}), 5)
        self.assertEqual(len(engines), 1)
        self.assertEqual(sum(engines[0].calls), 5)
        self.assertEqual(len(formatters), 1)
        self.assertEqual(formatters[0].calls, 3)
        self.assertEqual(sum(row["prompt_tokens"] for row in results), 55)
        gets = [event[1] for event in events if event[0] == "driver_get"]
        self.assertTrue(gets)
        self.assertFalse(any(tag.endswith(".prepare") for tag in gets))
        submitted = [event for event in events if event[:2] == ("submit", "_ModelActor.run")]
        self.assertTrue(all(event[2][0].tag == "_PrepareActor.prepare" for event in submitted))
        self.assertEqual(stream.last_close_report["status"], "closed")
        self.assertFalse(report["production_core_connected"])
        self.assertEqual(report["status"], "unknown")
        self.assertIsNone(report["gpu_exit_confirmed"])
        actors = [event for event in events if event[0] == "actor"]
        self.assertEqual(actors[-1][2]["num_gpus"], 1)
        self.assertEqual(actors[-1][2]["runtime_env"]["py_executable"], "/tmp/model-python")
        self.assertTrue(all(event[2]["max_task_retries"] == 0 for event in actors))

    def test_preflight_and_method_validation_precede_all_actors(self):
        stack, events, engines, _ = self._runtime()
        with stack:
            owner = self._owner()
            source = Source(self.rows)
            with patch.object(source, "preflight", side_effect=ValueError("manifest binding")):
                with self.assertRaisesRegex(ValueError, "manifest binding"):
                    owner.evaluate(source, query_id="q")
            owner.generation = GenerationSpec(repetition_penalty=1.1)
            with self.assertRaisesRegex(ValueError, "repetition"):
                owner.evaluate(source, query_id="q")
        self.assertEqual(events, [])
        self.assertEqual(engines, [])

    def test_failed_engine_close_retains_actor_for_retry_and_unknown_report(self):
        stack, events, engines, _ = self._runtime(fail_first_close=True)
        with stack:
            owner = self._owner()
            list(owner.evaluate(Source(self.rows), query_id="q"))
            first = owner.close()
            self.assertEqual(first["engine_close_report"]["attempt"], 1)
            self.assertFalse(first["engine_close_report"]["engine_api_shutdown_confirmed"])
            self.assertFalse(any(event[:2] == ("kill", "_ModelActor") for event in events))
            second = owner.close()
        self.assertEqual(engines[0].close_calls, 2)
        self.assertTrue(second["engine_close_report"]["engine_api_shutdown_confirmed"])
        self.assertTrue(any(event[:2] == ("kill", "_ModelActor") for event in events))
        self.assertEqual(second["status"], "unknown")

    def test_early_close_preserves_pending_ownership_and_blocks_reuse(self):
        stack, events, _, _ = self._runtime()
        with stack:
            owner = self._owner(prepare_batch_rows=1, max_active_batches=2)
            stream = owner.evaluate(Source(self.rows), query_id="q")
            next(stream)
            report = stream.close()
            self.assertGreater(report["pending_batches"], 0)
            self.assertEqual(report["status"], "unknown")
            self.assertTrue(any(event[0] == "cancel" for event in events))
            with self.assertRaisesRegex(RuntimeError, "unconfirmed"):
                owner.evaluate(Source(self.rows), query_id="another")
            owner.close()

    def test_total_deadline_is_reused_across_waits_and_batches(self):
        stack, _, _, _ = self._runtime()
        clock = [100.0]
        with stack, patch.object(static.time, "monotonic", side_effect=lambda: clock[0]):
            owner = self._owner(prepare_batch_rows=1, max_active_batches=1, query_timeout_s=2)
            stream = owner.evaluate(Source(self.rows), query_id="q")
            first_deadline = stream.deadline
            next(stream)
            clock[0] = 102.1
            with self.assertRaises(TimeoutError):
                next(stream)
            self.assertEqual(stream.deadline, first_deadline)
            owner.close()

    def test_later_model_sub_batch_failure_keeps_earlier_usage(self):
        stack, _, _, _ = self._runtime(fail_on_batch=2)
        received = []
        with stack:
            owner = self._owner(prepare_batch_rows=3, model_batch_rows=1, max_active_batches=1)
            stream = owner.evaluate(Source(self.rows), query_id="q")
            with self.assertRaisesRegex(RuntimeError, "model batch failed"):
                for row in stream:
                    received.append(row)
            report = owner.close()
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0]["prompt_tokens"], 11)
        self.assertEqual(report["status"], "unknown")


if __name__ == "__main__":
    unittest.main()
