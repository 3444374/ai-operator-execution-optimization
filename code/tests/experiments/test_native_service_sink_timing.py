"""Controlled source/sink boundaries for visual and aggregate service wrappers."""

from contextlib import ExitStack, contextmanager, nullcontext
from concurrent.futures import ThreadPoolExecutor
import importlib.util
from pathlib import Path
from types import SimpleNamespace
import threading
import unittest
from unittest.mock import patch

from src.baselines.image import sembench_animals, sembench_mmqa
from src.baselines.text.frameworks import lotus_native_join
from src.experiments import sembench_lotus_image_query as image
from src.experiments import sembench_lotus_mmqa_aggregate as aggregate
from src.experiments import native_http_observer
from src.experiments.native_service_query import NativeServiceCalls, native_query_settings
from src.observability import buffered_events
from src.semantic_methods.lotus import engine


class ServiceFixture:
    def __init__(self, kind, *, empty=False, errors=None):
        self.kind, self.errors = kind, errors or {}
        self.now, self.events = 10, []
        self.rows = [] if empty else [["original result"]]
        self.serialization_calls = 0
        fixture = self

        class Result:
            def __len__(self):
                return len(fixture.rows)

            def to_dict(self, *, orient):
                if orient != "split":
                    raise AssertionError(orient)
                fixture.serialization_calls += 1
                fixture.step("serialization", 29)
                return dict(columns=["original"], index=list(range(len(fixture.rows))), data=fixture.rows)

        self.result = Result()
        counts = [1] if kind == "aggregate" else [2, len(self.rows)] if kind == "mmqa" else [len(self.rows)]
        self.stages = [dict(input_occurrences=count, post_attempts=count) for count in counts]
        self.requests = set(range(sum(counts)))
        self.config = dict(query_id="fixture", scenario="mmqa" if kind == "mmqa" else "animals",
            query="Q2b" if kind == "mmqa" else "Q1", model_name="openai/fixture",
            generation={}, service_profile={}, query_timeout_s=60)

    def step(self, name, seconds):
        self.events.append((name, self.now))
        self.now += seconds
        if name in self.errors:
            raise self.errors[name]

    def check(self, *args, **kwargs):
        self.step("source", 2)
        if self.kind == "aggregate":
            return dict(source="frozen", endpoint="unused", work=dict(minimum_model_calls=1, leaf_requests=1))
        inputs = (dict(query="Q2b", exact_join_candidates=2, maximum_model_calls=len(self.requests))
            if self.kind == "mmqa" else dict(query_id=1, rows=len(self.rows), native_filter_passes=1,
                                             expected_model_calls=len(self.requests)))
        return dict(inputs=inputs, endpoint="unused")

    def make_lm(self, config):
        self.step("LM setup", 5)
        return object()

    def query(self, *args, **kwargs):
        self.step("original query", 3)
        return self.result

    def bind(self, *args, **kwargs):
        self.step("query binding", 11)
        return self.query

    def identity(self, *args):
        self.step("identity audit", 7)
        return "frozen"

    def snapshot(self):
        self.step("snapshot", 13)
        return dict(succeeded=len(self.requests), failed=0, unknown=0, unfinished=0)

    def evaluate(self, *args):
        self.step("evaluation", 31)
        return dict(original_score=1)

    def close(self):
        self.step("flush", 17)

    def writer_snapshot(self):
        self.step("writer snapshot", 19)
        return dict(closed=True)

    def lm_report(self, lm):
        self.step("LM report", 23)
        return dict(role="software")

    @contextmanager
    def observe(self, *args, **kwargs):
        try:
            yield
        finally:
            self.step("observer teardown", 7)

    def run(self):
        entry = aggregate if self.kind == "aggregate" else image
        writer = SimpleNamespace(close=self.close, snapshot=self.writer_snapshot)
        audit = SimpleNamespace(lm_calls=self.stages, requests=self.requests, snapshot=self.snapshot,
            observe_lm=lambda *a, **k: nullcontext(), record=lambda event: None)
        clock = SimpleNamespace(monotonic=lambda: self.now, monotonic_ns=lambda: self.now * 1_000_000_000)
        settings = SimpleNamespace(lm=None, enable_cache=False, context=lambda **kwargs: nullcontext())
        settings.configure = lambda **values: vars(settings).update(values)
        fake_lotus = SimpleNamespace(settings=settings)
        with ExitStack() as stack:
            for scope in (
                patch.dict("sys.modules", {"lotus": fake_lotus}),
                patch.object(entry, "time", clock),
                patch.object(entry, "check", self.check),
                patch.object(entry, "_make_lm", self.make_lm),
                patch.object(entry, "NativeServiceCalls" if self.kind == "aggregate" else "_NativeCalls", return_value=audit),
                patch.object(buffered_events, "BufferedEvents", return_value=writer),
                patch.object(native_http_observer, "observe_native_httpx", self.observe),
                patch.object(engine, "lm_report", self.lm_report),
                patch.object(lotus_native_join, 'observe_native_join_inputs', lambda events: nullcontext()),
                patch.object(lotus_native_join, 'describe_native_join_inputs', lambda events: []),
            ):
                stack.enter_context(scope)
            if self.kind == "aggregate":
                module = aggregate.mmqa
                stack.enter_context(patch.object(module, "input_identity", self.identity))
            elif self.kind == "mmqa":
                module = sembench_mmqa
            else:
                module = sembench_animals
                stack.enter_context(patch.object(module, "AnimalsDataset",
                    return_value=SimpleNamespace(frame=lambda **kwargs: None,
                        bind_frame=lambda: lambda: None, read=self.identity)))
            stack.enter_context(patch.object(module, "bind_original_query", self.bind))
            stack.enter_context(patch.object(module, "evaluate_original", self.evaluate))
            inputs = dict(data_dir="unused") if self.kind != "animals" else dict(manifest="unused", manifest_digest="unused")
            return entry.run_query(self.config, "unused", Path("unused"),
                budget=SimpleNamespace(remaining=len(self.requests)), **inputs)


class NativeServiceSinkTimingTests(unittest.TestCase):
    def test_complete_relation_precedes_observer_audit_serialization_evaluation_and_flush(self):
        for kind in ("animals", "mmqa", "aggregate"):
            with self.subTest(kind=kind):
                fixture = ServiceFixture(kind)
                report = fixture.run()
                self.assertEqual(report["source_to_sink_seconds"], 3)
                self.assertGreater(report["query_seconds"], report["source_to_sink_seconds"])
                lm_started = next(at for name, at in fixture.events if name == "LM setup")
                self.assertGreaterEqual(report["source_started_ns"] / 1e9, lm_started + 5)
                bound_at = next(at for name, at in fixture.events if name == "query binding")
                self.assertGreaterEqual(report["source_started_ns"] / 1e9, bound_at + 11)
                if kind == "aggregate":
                    self.assertEqual(report["driver_initialization_seconds"], 5)
                sink = report["sink_ready_ns"] / 1e9
                for name, at in fixture.events:
                    if name in {"observer teardown", "identity audit", "snapshot", "serialization", "evaluation", "flush"}:
                        self.assertGreaterEqual(at, sink, name)
                for phase in ("post_sink_audit", "result_serialization", "evaluation", "audit_finalization", "audit_flush"):
                    self.assertLessEqual(report[phase + "_started_ns"], report[phase + "_finished_ns"])
                    self.assertGreaterEqual(report[phase + "_started_ns"], report["sink_ready_ns"])
                self.assertEqual(report["evaluation_seconds"], 31)
                self.assertEqual(fixture.serialization_calls, 1)

    def test_empty_original_relation_still_has_a_complete_sink(self):
        for kind in ("animals", "mmqa", "aggregate"):
            with self.subTest(kind=kind):
                fixture = ServiceFixture(kind, empty=True)
                report = fixture.run()
                self.assertEqual(report["status"], "completed")
                self.assertEqual(report["result"]["data"], [])
                self.assertIsNotNone(report["sink_ready_ns"])
                self.assertEqual(report["model_calls"], len(fixture.requests))

    def test_post_sink_failures_keep_the_original_result_error_and_timestamp(self):
        for kind in ("animals", "mmqa", "aggregate"):
            for phase in ("snapshot", "serialization", "evaluation", "LM report", "flush", "writer snapshot"):
                with self.subTest(kind=kind, phase=phase):
                    failure = RuntimeError("original " + phase + " failure")
                    fixture = ServiceFixture(kind, errors={phase: failure})
                    with self.assertRaises(RuntimeError) as caught:
                        fixture.run()
                    self.assertIs(caught.exception, failure)
                    self.assertIs(failure.query_result, fixture.result)
                    report = failure.query_report
                    self.assertEqual(report["status"], "failed")
                    self.assertEqual(report["source_to_sink_seconds"], 3)
                    self.assertEqual(fixture.serialization_calls, 1)
                    if phase != "serialization":
                        self.assertEqual(report["result"]["data"], fixture.rows)
                    self.assertIn("flush", [name for name, at in fixture.events])

    def test_original_query_failure_is_not_replaced_by_snapshot_or_close_failure(self):
        for kind in ("animals", "mmqa", "aggregate"):
            with self.subTest(kind=kind):
                primary = LookupError("original query failure")
                fixture = ServiceFixture(kind, errors={"original query": primary,
                    "snapshot": ValueError("later snapshot failure"), "flush": OSError("later flush failure")})
                with self.assertRaises(LookupError) as caught:
                    fixture.run()
                self.assertIs(caught.exception, primary)
                self.assertIsNone(primary.query_report["sink_ready_ns"])
                self.assertIsNone(primary.query_report["source_to_sink_seconds"])
                self.assertNotIn("result", primary.query_report)
                self.assertEqual(fixture.serialization_calls, 0)
                self.assertTrue(any("snapshot" in note for note in primary.__notes__))
                self.assertTrue(any("audit close" in note for note in primary.__notes__))

    def test_aggregate_identity_audit_failure_keeps_completed_hierarchy_output(self):
        failure = ValueError("input changed after result")
        fixture = ServiceFixture("aggregate", errors={"identity audit": failure})
        with self.assertRaises(ValueError) as caught:
            fixture.run()
        self.assertIs(caught.exception, failure)
        self.assertEqual(failure.query_report["source_to_sink_seconds"], 3)
        self.assertEqual(failure.query_report["result"]["data"], fixture.rows)
        self.assertNotIn("quality", failure.query_report)


class NativeQueryObservationTests(unittest.TestCase):
    def test_overlapping_native_calls_keep_query_totals_without_false_stage_counts(self):
        barrier = threading.Barrier(2, timeout=5)
        events = []
        calls = NativeServiceCalls(SimpleNamespace(record=events.append), 'openai/software-model')

        class LM:
            def __call__(self, messages):
                barrier.wait()
                for attempt in messages:
                    calls.record(dict(event='request', attempt=attempt))
                    calls.record(dict(event='http_response', attempt=attempt, status=200,
                        response=dict(model='software-model', choices=[{}])))
                    calls.record(dict(event='http_finished', attempt=attempt))
                barrier.wait()
                return messages

        lm = LM()
        with calls.observe_lm(lm), ThreadPoolExecutor(max_workers=2) as pool:
            self.assertEqual(list(pool.map(lm, ([1], [2]))), [[1],[2]])
        self.assertEqual(calls.snapshot(), dict(attempts=2,succeeded=2,failed=0,unknown=0,unfinished=0))
        self.assertTrue(all(stage['post_attempts'] is None for stage in calls.lm_calls))
        self.assertTrue(all('overlapping' in stage['post_attempts_unavailable_reason'] for stage in calls.lm_calls))

    @unittest.skipUnless(importlib.util.find_spec('lotus'), 'requires the native LOTUS settings API')
    def test_native_worker_threads_receive_the_query_model_and_globals_are_restored(self):
        import lotus

        before = {name: object.__getattribute__(lotus.settings,name) for name in ('lm','enable_cache')}
        outer, current = object(), object()
        with lotus.settings.context(lm=outer,enable_cache=True):
            with native_query_settings(current), ThreadPoolExecutor(max_workers=1) as pool:
                self.assertIs(lotus.settings.lm,current)
                self.assertIs(pool.submit(lambda: lotus.settings.lm).result(),current)
                self.assertFalse(pool.submit(lambda: lotus.settings.enable_cache).result())
            self.assertIs(lotus.settings.lm,outer)
            self.assertTrue(lotus.settings.enable_cache)
        self.assertEqual({name: object.__getattribute__(lotus.settings,name) for name in before},before)
        with self.assertRaisesRegex(RuntimeError,'native query failure'):
            with native_query_settings(current):
                raise RuntimeError('native query failure')
        self.assertEqual({name: object.__getattribute__(lotus.settings,name) for name in before},before)


if __name__ == "__main__":
    unittest.main()
