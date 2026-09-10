"""Preserve independent execution evidence under query, consumer, and evaluator faults."""

from contextlib import contextmanager, asynccontextmanager
import asyncio
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from src.experiments.postgresql.map_query_recording import (
    public_execution_summary, record_execution, record_pg_query, verify_map_completions,
    record_async_execution, evaluate_recording,
)
from src.execution_provider.semantic_map import SemanticMapPlan, canonical_messages
from src.execution_provider.wire.map_codec import semantic_payload_digest


class QueryRecordingTests(unittest.TestCase):
    def test_evaluation_failure_is_not_overwritten_by_report_write_failure(self):
        self.execute(self.source([('a', 'answer')]))
        failure = ValueError('evaluation failed')
        def evaluate(rows):
            raise failure
        with patch('src.experiments.postgresql.map_query_recording.write_private_json',
                   side_effect=OSError('disk unavailable')):
            with self.assertRaises(ValueError) as raised:
                evaluate_recording(self.root, evaluate)
        self.assertIs(raised.exception, failure)
        self.assertEqual(public_execution_summary(self.root)['execution_status'], 'completed')

    def test_external_async_cancellation_precedes_slow_cleanup(self):
        now = [0]
        @asynccontextmanager
        async def source():
            async def rows():
                now[0] = 2_000_000_000
                raise asyncio.CancelledError('external cancel')
                yield
            try:
                yield rows()
            finally:
                now[0] = 12_000_000_000
        with self.assertRaises(asyncio.CancelledError):
            asyncio.run(record_async_execution(self.root, source, max_rows=1, max_result_bytes=4096,
                                                clock=lambda: now[0]))
        record = json.loads((self.root / 'execution.json').read_text())
        self.assertEqual(record['t_cancel_triggered_ns'], 2_000_000_000)
        self.assertEqual(record['query_jct_seconds'], 2)
        self.assertEqual(record['t_stream_cleanup_ns'], 12_000_000_000)

    def test_invalid_deadlines_fail_before_creating_query_artifacts(self):
        for value in (float('nan'), float('inf'), 0, -1, True):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    record_execution(self.root, self.source(), max_rows=1, max_result_bytes=4096,
                                     query_timeout_s=value, cancel_query=lambda: None)
                with self.assertRaises(ValueError):
                    asyncio.run(record_async_execution(self.root, None, max_rows=1, max_result_bytes=4096,
                                                        query_timeout_s=value))
                self.assertFalse(self.root.exists())

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / "run"
        self.closed = False

    def source(self, values=(), failure=None):
        @contextmanager
        def open_rows():
            def rows():
                yield from values
                if failure:
                    raise failure
            try:
                yield rows()
            finally:
                self.closed = True
        return open_rows

    def execute(self, source, evaluator=None, **options):
        return record_execution(self.root, source, max_rows=options.get("max_rows", 10),
            max_result_bytes=options.get("max_result_bytes", 4096), evaluator=evaluator)

    def test_evaluator_sees_saved_terminal_and_raw_results(self):
        def evaluate(rows):
            self.assertTrue(self.closed)
            self.assertEqual(json.loads((self.root / "execution.json").read_text())["status"], "completed")
            self.assertEqual(rows, [["id", "  raw\n"]])
            return {"rows": len(rows)}
        self.execute(self.source([("id", "  raw\n")]), evaluate)
        self.assertEqual(public_execution_summary(self.root)["recorded_rows"], 1)

    def test_partial_rows_survive_query_failure(self):
        with self.assertRaisesRegex(RuntimeError, "query_failed"):
            self.execute(self.source([("a", "partial")], RuntimeError("query_failed")))
        execution = json.loads((self.root / "execution.json").read_text())
        self.assertEqual(execution["status"], "failed")
        self.assertTrue(execution["partial_results_are_provisional"])
        self.assertEqual(execution["recorded_rows"], 1)
        self.assertGreaterEqual(execution["ended_ns"], execution["started_ns"])
        self.assertTrue(self.closed)
        self.assertFalse((self.root / "evaluation.json").exists())

    def test_failure_before_first_row_still_has_timing(self):
        @contextmanager
        def source():
            self.assertTrue((self.root / "started.json").exists())
            raise RuntimeError("before_execute")
            yield
        with self.assertRaises(RuntimeError):
            self.execute(source)
        record = json.loads((self.root / "execution.json").read_text())
        self.assertEqual(record["recorded_rows"], 0)
        self.assertEqual(record["status"], "failed")

    def test_evaluator_failure_keeps_completed_execution(self):
        def fail(_rows):
            raise ValueError('password="fixture-only"')
        with self.assertRaises(ValueError):
            self.execute(self.source([("a", "answer")]), fail)
        self.assertEqual(json.loads((self.root / "execution.json").read_text())["status"], "completed")
        self.assertEqual(json.loads((self.root / "evaluation.json").read_text())["status"], "failed")
        self.assertNotIn("fixture-only", (self.root / "evaluation.json").read_text())
        summary = public_execution_summary(self.root)
        self.assertEqual(summary["execution_status"], "completed")
        self.assertEqual(summary["evaluation_status"], "failed")
        self.assertFalse(summary["performance_qualified"])

    def test_source_failure_precedes_slow_cleanup_and_preserves_sqlstate(self):
        class QueryFailure(Exception):
            sqlstate = "54000"
        for cleanup_fails in (False, True):
            with self.subTest(cleanup_fails=cleanup_fails):
                tick = [0]
                @contextmanager
                def source():
                    def rows():
                        tick[0] = 2_000_000_000
                        raise QueryFailure("source failed")
                        yield
                    try:
                        yield rows()
                    finally:
                        tick[0] = 12_000_000_000
                        if cleanup_fails:
                            raise RuntimeError("cleanup failed")
                directory = Path(self.tmp.name) / str(cleanup_fails)
                with self.assertRaises(QueryFailure):
                    record_execution(directory, source, max_rows=2, max_result_bytes=1000,
                                     clock=lambda: tick[0])
                result = json.loads((directory / "execution.json").read_text())
                self.assertEqual(result["query_jct_seconds"], 2)
                self.assertEqual(result["t_stream_cleanup_ns"], 12_000_000_000)
                self.assertEqual(result["query_error"]["sqlstate"], "54000")
                self.assertEqual(result["error"]["type"], "QueryFailure")
                self.assertEqual(result["cleanup_error"] is not None, cleanup_fails)

    def test_async_source_failure_is_recorded_before_cleanup(self):
        from contextlib import asynccontextmanager
        tick = [0]
        @asynccontextmanager
        async def source():
            async def rows():
                tick[0] = 2_000_000_000
                raise ValueError("async source failed")
                yield
            try:
                yield rows()
            finally:
                tick[0] = 12_000_000_000
                raise RuntimeError("async cleanup failed")
        with self.assertRaisesRegex(ValueError, "async source failed"):
            asyncio.run(record_async_execution(self.root, source, max_rows=2,
                        max_result_bytes=1000, clock=lambda: tick[0]))
        result = json.loads((self.root / "execution.json").read_text())
        self.assertEqual(result["query_jct_seconds"], 2)
        self.assertEqual(result["cleanup_error"]["type"], "RuntimeError")

    def test_flush_failure_does_not_replace_source_error(self):
        from src.baselines.common.private_artifacts import open_private_text
        @contextmanager
        def fail_close(path):
            with open_private_text(path) as stream:
                yield stream
            raise OSError("synthetic flush failed")
        with patch('src.experiments.postgresql.map_query_recording.open_private_text', fail_close):
            with self.assertRaisesRegex(ValueError, "source failed"):
                self.execute(self.source([('a', 'partial')], ValueError('source failed')))
        result = json.loads((self.root / 'execution.json').read_text())
        self.assertEqual(result['query_error']['type'], 'ValueError')
        self.assertEqual(result['recording_error']['type'], 'OSError')
        self.assertIsNone(result['results_sha256'])

    def test_summary_failure_keeps_primary_error_and_writes_fallback(self):
        from src.baselines.common.private_artifacts import write_private_json
        def write(path, value):
            if path.name == 'execution.json':
                raise OSError('summary failed')
            write_private_json(path, value)
        with patch('src.experiments.postgresql.map_query_recording.write_private_json', write):
            with self.assertRaisesRegex(ValueError, 'source failed'):
                self.execute(self.source(failure=ValueError('source failed')))
        result = json.loads((self.root / 'recording-failure.json').read_text())
        self.assertEqual(result['error']['type'], 'ValueError')
        self.assertEqual(result['recording_error']['type'], 'OSError')

    def test_async_query_deadline_stops_before_slow_cleanup(self):
        @asynccontextmanager
        async def source():
            async def rows():
                await asyncio.sleep(10)
                yield ('a', 'late')
            try:
                yield rows()
            finally:
                await asyncio.sleep(.03)
        with self.assertRaises(TimeoutError):
            asyncio.run(record_async_execution(self.root, source, max_rows=2,
                        max_result_bytes=1000, query_timeout_s=.01))
        result = json.loads((self.root / 'execution.json').read_text())
        self.assertEqual(result['query_error']['type'], 'TimeoutError')
        self.assertIsNotNone(result['t_cancel_triggered_ns'])
        self.assertGreater(result['t_stream_cleanup_ns'] - result['t_query_terminal_ns'], 20_000_000)

    def test_sync_deadline_cancels_and_joins_watchdog(self):
        import threading
        cancelled = threading.Event()
        @contextmanager
        def source():
            def rows():
                self.assertTrue(cancelled.wait(2))
                yield ('a', 'late')
            yield rows()
        with self.assertRaises(TimeoutError):
            record_execution(self.root, source, max_rows=2, max_result_bytes=1000,
                             query_timeout_s=.01, cancel_query=cancelled.set)
        result = json.loads((self.root / 'execution.json').read_text())
        self.assertEqual(result['received_rows'], 0)
        self.assertIsNotNone(result['t_cancel_triggered_ns'])

    def test_empty_result_records_completion_without_quality_claim(self):
        self.execute(self.source())
        summary = public_execution_summary(self.root)
        self.assertEqual(summary["recorded_rows"], 0)
        self.assertEqual(summary["evaluation_status"], "not_run")
        self.assertFalse(summary["performance_qualified"])

    def test_interruption_and_result_limit_close_stream(self):
        for failure, options in ((KeyboardInterrupt(), {}), (None, {"max_rows": 1})):
            with self.subTest(failure=failure):
                self.root = Path(self.tmp.name) / ("interrupt" if failure else "limit")
                with self.assertRaises(KeyboardInterrupt if failure else ValueError):
                    self.execute(self.source([("a", "first"), ("b", "second")], failure), **options)
                self.assertTrue(self.closed)
                self.assertEqual(json.loads((self.root / "execution.json").read_text())["status"], "failed")

    def test_existing_root_prevents_execution(self):
        self.root.mkdir()
        @contextmanager
        def source():
            self.fail("must not start SQL")
            yield
        with self.assertRaises(FileExistsError):
            self.execute(source)

    def test_pg_wrapper_closes_generator_and_cursor(self):
        state = {"generator": False, "cursor": False}
        class Cursor:
            def __enter__(self):
                return self
            def __exit__(self, *_):
                state["cursor"] = True
            def stream(self, query):
                self.query = query
                try:
                    yield ("a", "one")
                    yield ("b", "two")
                finally:
                    state["generator"] = True
        class Connection:
            def cursor(self):
                return Cursor()
        with self.assertRaises(ValueError):
            record_pg_query(Connection(), "explicit query", self.root, max_rows=1, max_result_bytes=4096)
        self.assertEqual(state, {"generator": True, "cursor": True})

    def test_query_terminal_is_after_last_row_and_not_final_fsync(self):
        clock = iter(range(1000)).__next__
        record = record_execution(self.root, self.source([('a', 'first'), ('b', 'last')]),
                                  max_rows=2, max_result_bytes=4096, clock=clock)
        ordered = [record[key] for key in ('started_ns', 't_release_ns', 't_first_row_ns',
                   't_last_row_ns', 't_query_terminal_ns', 't_stream_cleanup_ns',
                   't_results_durable_ns', 'ended_ns')]
        self.assertEqual(ordered, sorted(ordered))
        self.assertLess(record['t_last_row_ns'], record['t_query_terminal_ns'])
        self.assertLess(record['query_jct_seconds'], record['elapsed_seconds'])
        self.assertEqual(record['received_rows'], 2)

    def test_empty_and_partial_error_have_terminals_without_inventing_rows(self):
        self.execute(self.source())
        empty = json.loads((self.root / 'execution.json').read_text())
        self.assertIsNone(empty['t_first_row_ns'])
        self.assertIsNone(empty['t_last_row_ns'])
        self.assertIsNotNone(empty['t_query_terminal_ns'])
        self.root = Path(self.tmp.name) / 'partial'
        with self.assertRaises(RuntimeError):
            self.execute(self.source([('a', 'partial')], RuntimeError('SQL error')))
        failed = json.loads((self.root / 'execution.json').read_text())
        self.assertEqual(failed['query_status'], 'failed')
        self.assertGreaterEqual(failed['t_query_terminal_ns'], failed['t_last_row_ns'])

    def test_result_limit_distinguishes_received_from_recorded(self):
        with self.assertRaises(ValueError):
            self.execute(self.source([('a', 'first'), ('b', 'discarded')]), max_rows=1)
        record = json.loads((self.root / 'execution.json').read_text())
        self.assertEqual((record['received_rows'], record['recorded_rows']), (2, 1))
        self.assertEqual(record['query_status'], 'consumer_aborted')
        self.assertIsNone(record['t_query_terminal_ns'])
        self.assertIsNone(record['query_jct_seconds'])

    def test_stream_evaluation_checks_all_rows_and_separates_its_time(self):
        def evaluate(rows):
            self.assertNotIsInstance(rows, list)
            return {'rows': sum(1 for _ in rows)}
        record_execution(self.root, self.source([('a', 'first'), ('b', 'last')]),
                         max_rows=2, max_result_bytes=4096, evaluator=evaluate,
                         evaluation_mode='stream', flush_rows=2)
        evaluation = json.loads((self.root / 'evaluation.json').read_text())
        execution = json.loads((self.root / 'execution.json').read_text())
        self.assertEqual((evaluation['mode'], evaluation['consumed_rows']), ('stream', 2))
        self.assertGreaterEqual(evaluation['started_ns'], execution['ended_ns'])

    def test_incomplete_stream_evaluation_and_changed_results_fail_separately(self):
        for variant in ('partial', 'changed'):
            with self.subTest(variant=variant):
                self.root = Path(self.tmp.name) / variant
                self.execute(self.source([('a', 'first'), ('b', 'last')]))
                if variant == 'changed':
                    path = self.root / 'results.jsonl'
                    path.write_text(path.read_text().replace('first', 'other'))
                def evaluate(rows):
                    return next(rows) if variant == 'partial' else list(rows)
                with self.assertRaises(ValueError):
                    evaluate_recording(self.root, evaluate, mode='stream')
                self.assertEqual(public_execution_summary(self.root)['execution_status'], 'completed')
                self.assertEqual(public_execution_summary(self.root)['evaluation_status'], 'failed')

    def test_non_json_evaluation_result_records_failure(self):
        with self.assertRaises(TypeError):
            self.execute(self.source([('a', 'answer')]), lambda _: {object()})
        self.assertEqual(public_execution_summary(self.root)['evaluation_status'], 'failed')

    def test_async_source_uses_same_contract_and_closes_on_failure(self):
        @asynccontextmanager
        async def source():
            async def rows():
                yield ('a', 'one')
                raise RuntimeError('async failure')
            try:
                yield rows()
            finally:
                self.closed = True
        with self.assertRaisesRegex(RuntimeError, 'async failure'):
            asyncio.run(record_async_execution(self.root, source, max_rows=2, max_result_bytes=4096))
        record = json.loads((self.root / 'execution.json').read_text())
        self.assertEqual((record['recorded_rows'], record['query_status']), (1, 'failed'))
        self.assertTrue(self.closed)


class MapAssociationTests(unittest.TestCase):
    def setUp(self):
        self.plan = SemanticMapPlan("Instruction", "model", 64)
        self.inputs = [("a", "same input"), ("b", "same input")]
        self.predictions = [("b", "second"), ("a", "first")]
        digest = semantic_payload_digest(semantic_spec_sha256=self.plan.digest, input_value="same input",
            canonical_messages_utf8=canonical_messages("Instruction", "same input"))
        self.completions = [dict(sequence=sequence, payload_digest=digest, raw_output=output,
            response_model_id="model", finish_reason="stop") for sequence, output in ((1, "second"), (0, "first"))]

    def check(self, completions):
        return verify_map_completions(self.inputs, self.predictions, completions, plan=self.plan, sequence_ids=["a", "b"])

    def test_out_of_order_identical_inputs_keep_distinct_sequences(self):
        self.assertEqual(self.check(self.completions)["matched_rows"], 2)

    def test_duplicates_missing_payload_output_and_length_rejected(self):
        invalid = [self.completions[:1], [self.completions[0]]*2]
        for key, value in (("payload_digest", "0"*64), ("raw_output", "wrong"), ("finish_reason", "length")):
            invalid.append([dict(self.completions[0], **{key: value}), self.completions[1]])
        for records in invalid:
            with self.subTest(records=records), self.assertRaises(ValueError):
                self.check(records)


if __name__ == "__main__":
    unittest.main()

class EntryFailureTests(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root=Path(tmp.name)/'run'

    def test_explicit_entry_failure_precedes_cleanup_sync_and_async(self):
        from src.experiments.postgresql.map_query_recording import query_failure_scope
        class Primary(Exception):
            sqlstate='54000'
        for asynchronous in (False,True):
            with self.subTest(asynchronous=asynchronous):
                now=[0]
                @contextmanager
                def source():
                    try:
                        with query_failure_scope():
                            now[0]=2_000_000_000
                            raise Primary('query entry')
                        yield
                    finally:
                        now[0]=12_000_000_000
                        raise RuntimeError('entry cleanup')
                @asynccontextmanager
                async def async_source():
                    with source() as rows:
                        yield rows
                path=self.root/str(asynchronous)
                with self.assertRaises(Primary):
                    if asynchronous:
                        asyncio.run(record_async_execution(path,async_source,max_rows=1,
                            max_result_bytes=4096,clock=lambda:now[0]))
                    else:
                        record_execution(path,source,max_rows=1,max_result_bytes=4096,clock=lambda:now[0])
                record=json.loads((path/'execution.json').read_text())
                self.assertEqual(record['query_jct_seconds'],2)
                self.assertEqual(record['query_error']['sqlstate'],'54000')
                self.assertEqual(record['cleanup_error']['type'],'RuntimeError')

    def test_adapter_timeout_observation_is_not_overwritten_after_cleanup(self):
        from src.experiments.postgresql.map_query_recording import query_failure_scope
        now=[0]
        @asynccontextmanager
        async def source():
            async def rows():
                try:
                    with query_failure_scope():
                        try:
                            await asyncio.Future()
                        except asyncio.CancelledError:
                            now[0]=2_000_000_000
                            raise
                finally:
                    now[0]=12_000_000_000
                yield
            yield rows()
        with self.assertRaises(TimeoutError):
            asyncio.run(record_async_execution(self.root,source,max_rows=1,max_result_bytes=4096,
                clock=lambda:now[0],query_timeout_s=.01))
        record=json.loads((self.root/'execution.json').read_text())
        self.assertEqual(record['t_cancel_triggered_ns'],2_000_000_000)
        self.assertEqual(record['query_jct_seconds'],2)
        self.assertIsNone(record['cleanup_error'])
