"""CPU-only graph and lifecycle checks; these do not qualify installed Ray."""

from __future__ import annotations

import json
import sys
import threading
import time
import unittest
from contextlib import ExitStack
from dataclasses import replace
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import patch


CODE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir())
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.baselines.text.frameworks import ray_data_lotus_records as native
from src.semantic_methods.lotus import engine as lotus_engine
from src.semantic_methods.lotus.records import FilterMethod, RecordScan, RecordsResources, identity
from src.serving.backends import vllm_engine
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec


class Table:
    def __init__(self, rows):
        self.rows = [dict(row) for row in rows]

    def to_pylist(self):
        return [dict(row) for row in self.rows]

    @classmethod
    def from_pylist(cls, rows):
        return cls(rows)


class Source:
    csv_path = Path("/tmp/software-fixture-records.csv")

    def __init__(self, rows):
        self.raw = rows
        self.preflight_calls = 0

    def preflight(self, method):
        self.preflight_calls += 1
        method.describe()

    def expected(self, query_id):
        return [identity(query_id, row["row_id"], row["source_position"])
                for row in self.raw if row["id"] == "taken_3"]

    def scan_spec(self, method):
        return RecordScan((('row_id', 'string'), ('source_position', 'int64'),
                           ('reviewId', 'string'), ('id', 'string'), ('reviewText', 'string')),
                          ('reviewText',), ('id', 'taken_3'))

    def rows(self):
        raise AssertionError("native graph must scan CSV rather than collect source.rows")


class NativeRecordsTests(unittest.TestCase):
    def setUp(self):
        self.rows = [
            dict(row_id="movie-row-0", source_position=0, reviewId="same", id="taken_3", reviewText="yes"),
            dict(row_id="movie-row-1", source_position=1, reviewId="same", id="taken_3", reviewText="no"),
            dict(row_id="movie-row-2", source_position=2, reviewId="other", id="other", reviewText="excluded"),
            dict(row_id="movie-row-3", source_position=3, reviewId="same", id="taken_3", reviewText="yes again"),
        ]
        self.method = FilterMethod("software-fixture")
        self.spec = EngineSpec("/tmp/fixture-model", "/tmp/model-python", "/tmp/driver-python")

    def _runtime(self, *, close_gpu=None, close_raises=False):
        events = []
        prepare_instances = []
        engines = []

        class Formatter:
            def __init__(self, method):
                assert method.resolved_input_columns == ("reviewText",)
                self.method = method
                self.calls = 0
                prepare_instances.append(self)

            def prepare_rows(self, rows):
                self.calls += 1
                for row in rows:
                    if set(row) != set(identity("q", row["row_id"], row["source_position"])) | {"reviewText"}:
                        raise AssertionError("labels or unused source columns reached the formatter")
                return [{**{key: value for key, value in row.items() if key != "reviewText"},
                         "messages_json": json.dumps([{"role": "user", "content": row["reviewText"]}])}
                        for row in rows]

        class Engine:
            def __init__(self, engine, generation):
                self.calls = 0
                self.close_calls = 0
                engines.append(self)

            def run_prepared(self, rows, *, deadline_monotonic):
                self.calls += 1
                return [{**{key: value for key, value in row.items() if key != "messages_json"},
                         "status": "succeeded", "text": "True", "prompt_tokens": 7,
                         "completion_tokens": 1, "total_tokens": 8}
                        for row in rows]

            def close(self):
                self.close_calls += 1
                if close_raises:
                    raise RuntimeError("fixture engine shutdown failure")
                return dict(status="closed", engine_api_shutdown_confirmed=True, gpu_exit_confirmed=close_gpu)

        class Dataset:
            def __init__(self, rows, steps=()):
                self.rows, self.steps = rows, steps
                self._current_executor = SimpleNamespace(shutdown=lambda **kwargs: events.append(("shutdown", kwargs)))

            def _step(self, name, args, kwargs):
                events.append((name, args, kwargs))
                return Dataset(self.rows, self.steps + ((name, args, kwargs),))

            def filter(self, fn, **kwargs):
                return self._step("filter", (fn,), kwargs)

            def select_columns(self, columns):
                return self._step("select_columns", (columns,), {})

            def map(self, fn, **kwargs):
                return self._step("map", (fn,), kwargs)

            def map_batches(self, cls, **kwargs):
                return self._step("map_batches", (cls,), kwargs)

            def iter_rows(self):
                def iterate():
                    rows = [dict(row) for row in self.rows]
                    for name, args, kwargs in self.steps:
                        if name == "filter":
                            rows = [row for row in rows if args[0](row)]
                        elif name == "select_columns":
                            rows = [{key: row[key] for key in args[0]} for row in rows]
                        elif name == "map":
                            rows = [args[0](row, **kwargs["fn_kwargs"]) for row in rows]
                        else:
                            worker = args[0](**kwargs["fn_constructor_kwargs"])
                            batches = [rows[i:i + kwargs["batch_size"]] for i in range(0, len(rows), kwargs["batch_size"])]
                            rows = [row for batch in batches for row in worker(Table(batch)).to_pylist()]
                    try:
                        yield from rows
                    finally:
                        events.append(("iterator_close",))
                return iterate()

        ray = ModuleType("ray")
        ray.__version__ = "2.56.1"
        ray.is_initialized = lambda: True
        data = ModuleType("ray.data")
        data.ActorPoolStrategy = lambda **kwargs: kwargs

        def read_csv(path, **kwargs):
            events.append(("read_csv", path, kwargs))
            return Dataset(self.rows)

        data.read_csv = read_csv
        ray.data = data
        stack = ExitStack()
        stack.enter_context(patch.dict(sys.modules, {
            "ray": ray, "ray.data": data,
            "pyarrow": SimpleNamespace(
                Table=Table, string=lambda: "string", int64=lambda: "int64",
                csv=SimpleNamespace(ConvertOptions=lambda **kwargs: kwargs),
            ),
        }))
        stack.enter_context(patch.object(EngineSpec, "validate_driver_environment", return_value={}))
        stack.enter_context(patch.object(lotus_engine, "validate_filter_source", return_value={}))

        def fixture_binding(method):
            # These graph tests substitute the formatter; real binding has its own library checks.
            lotus_engine.validate_filter_source()
            return replace(method, resolved_input_columns=("reviewText",))

        stack.enter_context(patch.object(lotus_engine, "bind_filter_method", side_effect=fixture_binding))
        stack.enter_context(patch.object(lotus_engine, "LotusFilterMethod", Formatter))
        stack.enter_context(patch.object(vllm_engine, "VllmEngine", Engine))
        return stack, events, prepare_instances, engines

    def test_native_csv_graph_keeps_occurrences_and_reuses_worker_models(self):
        stack, events, formatters, engines = self._runtime()
        with stack:
            stream = native.evaluate(
                Source(self.rows), self.method, self.spec, GenerationSpec(),
                RecordsResources(prepare_batch_rows=2, model_batch_rows=1), query_id="q",
            )
            results = list(stream)
        self.assertEqual([row["source_position"] for row in results], [0, 1, 3])
        self.assertEqual(len({row["call_id"] for row in results}), 3)
        self.assertEqual(len(formatters), 1)
        self.assertEqual(formatters[0].calls, 2)
        self.assertEqual(len(engines), 1)
        self.assertEqual((engines[0].calls, engines[0].close_calls), (3, 1))
        self.assertEqual(sum(row["prompt_tokens"] for row in results), 21)
        self.assertTrue(all("messages_json" not in row for row in results))
        stages = [event for event in events if event[0] == "map_batches"]
        self.assertEqual([stage[1][0] for stage in stages], [native.PrepareBatch, native.InferBatch])
        self.assertEqual(stages[1][2]["num_gpus"], 1)
        self.assertEqual(stages[1][2]["runtime_env"]["py_executable"], "/tmp/model-python")
        self.assertEqual(stages[0][2]["runtime_env"]["py_executable"], "/tmp/driver-python")
        for stage in stages:
            self.assertEqual((stage[2]["max_restarts"], stage[2]["max_task_retries"]), (0, 0))
        self.assertEqual(stream.last_close_report["status"], "unknown")
        self.assertIsNone(stream.last_close_report["gpu_exit_confirmed"])
        self.assertEqual(len([event for event in events if event[0] == "shutdown"]), 1)

    def test_bad_source_and_unsupported_generation_fail_before_graph(self):
        stack, events, _, engines = self._runtime()
        source = Source(self.rows)
        with stack:
            with patch.object(source, "preflight", side_effect=ValueError("source binding")):
                with self.assertRaisesRegex(ValueError, "source binding"):
                    native.evaluate(source, self.method, self.spec, GenerationSpec(), RecordsResources(), query_id="q")
            with self.assertRaisesRegex(ValueError, "repetition"):
                native.evaluate(source, self.method, self.spec, GenerationSpec(repetition_penalty=1.2), RecordsResources(), query_id="q")
            with self.assertRaises(InterruptedError):
                native.evaluate(source, self.method, self.spec, GenerationSpec(), RecordsResources(), query_id="q", cancelled=lambda: True)
        self.assertEqual(events, [])
        self.assertEqual(engines, [])

    def test_cleanup_attempts_both_actions_and_retries_without_hiding_primary(self):
        class BrokenIterator:
            attempts = 0

            def __iter__(self):
                return self

            def __next__(self):
                raise ValueError("primary row failure")

            def close(self):
                self.attempts += 1
                if self.attempts == 1:
                    raise RuntimeError("iterator cleanup")

        iterator = BrokenIterator()
        calls = []

        def shutdown(**kwargs):
            calls.append(kwargs)
            if len(calls) == 1:
                raise RuntimeError("graph cleanup")

        result = SimpleNamespace(iter_rows=lambda: iterator, _current_executor=SimpleNamespace(shutdown=shutdown))
        stream = native._RayRows(result, [identity("q", "row", 0)], time.monotonic() + 3, None)
        with self.assertRaisesRegex(ValueError, "primary row failure") as caught:
            next(stream)
        self.assertEqual(len(calls), 1)
        self.assertIn("iterator_close: RuntimeError", stream.last_close_report["cleanup_errors"])
        self.assertIn("executor_shutdown: RuntimeError", stream.last_close_report["cleanup_errors"])
        self.assertTrue(caught.exception.__notes__)
        report = stream.close()
        self.assertEqual((iterator.attempts, len(calls)), (2, 2))
        self.assertEqual(report["cleanup_errors"], ["model_engine_shutdown_unconfirmed"])
        self.assertEqual(report["status"], "unknown")

    def test_deadline_interrupts_blocked_vendor_consumption_without_reset(self):
        released = threading.Event()
        shutdowns = []

        def shutdown(**kwargs):
            shutdowns.append(kwargs)
            released.set()

        def rows():
            released.wait(timeout=1)
            if False:
                yield {}

        result = SimpleNamespace(iter_rows=rows, _current_executor=SimpleNamespace(shutdown=shutdown))
        deadline = time.monotonic() + 0.04
        stream = native._RayRows(result, [identity("q", "row", 0)], deadline, None)
        with self.assertRaises(TimeoutError):
            next(stream)
        self.assertEqual(stream.deadline, deadline)
        self.assertEqual(len(shutdowns), 1)
        self.assertTrue(stream.last_close_report["watchdog_stopped"])
        self.assertEqual(stream.last_close_report["status"], "unknown")

    def test_duplicate_and_missing_results_fail_with_full_identity(self):
        expected = identity("q", "row", 0)
        for rows, message in (([expected, expected], "duplicate"), ([], "omitted")):
            with self.subTest(message=message):
                result = SimpleNamespace(iter_rows=lambda: iter(rows), _current_executor=None)
                stream = native._RayRows(result, [expected], time.monotonic() + 3, None)
                with self.assertRaisesRegex(ValueError, message):
                    list(stream)

    def test_stop_does_not_discard_already_returned_batch_usage(self):
        expected = [identity("q", "row-" + str(i), i) for i in range(2)]
        results = [dict(row, status="failed", prompt_tokens=9, completion_tokens=1)
                   for row in expected]
        result = SimpleNamespace(iter_rows=lambda: iter(results), _current_executor=None)
        stream = native._RayRows(result, expected, time.monotonic() + 3, None)
        first = next(stream)
        stream._failure = TimeoutError("query deadline")
        second = next(stream)
        with self.assertRaises(TimeoutError):
            next(stream)
        self.assertEqual(first["prompt_tokens"] + second["prompt_tokens"], 18)
        self.assertEqual(stream.last_close_report["received_occurrences"], 2)

    def test_engine_close_error_does_not_discard_completed_batch_usage(self):
        stack, _, _, _ = self._runtime(close_raises=True)
        results = []
        with stack:
            stream = native.evaluate(
                Source(self.rows), self.method, self.spec, GenerationSpec(),
                RecordsResources(model_batch_rows=3), query_id="q",
            )
            with self.assertRaisesRegex(RuntimeError, "cleanup"):
                for row in stream:
                    results.append(row)
        self.assertEqual(len(results), 3)
        self.assertEqual(sum(row["prompt_tokens"] for row in results), 21)
        self.assertEqual(stream.last_close_report["engine_close_report"]["error_type"], "RuntimeError")


if __name__ == "__main__":
    unittest.main()
