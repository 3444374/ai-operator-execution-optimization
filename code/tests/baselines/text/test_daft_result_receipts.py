"""Daft receipt accounting with synthetic CPU responses; no model or GPU work."""

from copy import deepcopy
import csv
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest

from src.baselines.text.frameworks import daft_lotus_records as native
from src.baselines.text.frameworks.daft_engine_process import EngineProcess, DEFAULT_CLOSE_TIMEOUT_S
from src.semantic_methods.lotus.records import CallLedger, FilterMethod, RecordsResources, identity
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec, IDENTITY_FIELDS


MODULE = "tests.baselines.text.test_daft_result_receipts"


def terminal(item, *, status="succeeded", marker=True, uid="receipt-cpu"):
    messages = json.loads(item.get("messages_json", '[{"role":"user","content":"CPU fixture"}]'))
    stamp = time.monotonic()
    return dict({key: item[key] for key in IDENTITY_FIELDS}, status=status,
        submission_confirmed=status != "unknown", text="Answer: True" if status == "succeeded" else None,
        prompt_tokens=None if status == "unknown" else 5,
        completion_tokens=None if status == "unknown" else 2,
        total_tokens=None if status == "unknown" else 7,
        prompt_token_ids_json="null" if status == "unknown" else "[1,2,3,4,5]",
        output_token_ids_json="null" if status == "unknown" else "[6,7]", logprobs_json="null",
        messages_sha256=hashlib.sha256(json.dumps(messages, ensure_ascii=False, sort_keys=True,
                                                separators=(",", ":")).encode()).hexdigest(),
        engine_uid=uid, engine_request_id="cpu-request:" + item["call_id"],
        finish_reason="stop" if status == "succeeded" else None,
        error_type=None if status == "succeeded" else "CPUFixtureFailure",
        deadline_expired=False, model_start_monotonic=stamp, model_end_monotonic=stamp,
        model_elapsed_s=0.0, **({"engine_protocol_error": "ValueError"} if marker else {}))


def failure_report(rows, *, cleanup_errors=()):
    return dict(status="unknown" if cleanup_errors else "closed", process_exit_confirmed=True,
        gpu_exit_confirmed=None, cleanup_errors=list(cleanup_errors), engine_ready={"engine_uid": "receipt-cpu"},
        result_failure=dict(error_type="ValueError", verified_call_results=deepcopy(rows),
            raw_response_json=json.dumps(dict(kind="results", rows=rows), ensure_ascii=False),
            unassociated_result_indexes=[]))


class Frame:
    def __init__(self, rows, error=None):
        self.rows, self.error = rows, error

    def iter_rows(self):
        for row in self.rows:
            yield {"engine_result_json": json.dumps(row)}
        if self.error is not None:
            raise self.error


class ReceiptAccountingTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.TemporaryDirectory(prefix="daft-receipts-contract-")
        self.addCleanup(self.root.cleanup)
        self.expected = [identity("q", "row-" + str(index), index) for index in range(3)]
        self.results = [terminal(row) for row in self.expected]
        self.control = Path(self.root.name)

    def rows(self, values=(), *, report=None, error=None):
        if report is not None:
            (self.control / "engine-fixture.json").write_text(json.dumps(report))
        return native._DaftRows(Frame(values, error), self.expected, time.monotonic() + 30, None, self.control)

    def test_close_recovers_only_unreceived_receipts_without_comparing_daft_metadata(self):
        enhanced = dict(self.results[0], prepare_worker_pid=777, infer_worker_pid=778,
                        native_infer_batch_rows=3, native_infer_message_bytes=30)
        rows = self.rows([enhanced], report=failure_report(self.results))
        self.assertEqual(next(rows), enhanced)
        report = rows.close()
        self.assertEqual(report["status"], "closed")
        late = report["unconsumed_call_results"]
        self.assertEqual([row["call_id"] for row in late], [row["call_id"] for row in self.expected[1:]])
        self.assertEqual(sum(row["prompt_tokens"] for row in late), 10)
        self.assertIs(rows.close(), report)

    def test_received_unknown_cannot_be_upgraded_by_a_close_receipt(self):
        unknown = terminal(self.expected[0], status="unknown")
        rows = self.rows([unknown], report=failure_report(self.results))
        observed = next(rows)
        ledger = CallLedger(self.expected, max_calls=3)
        ledger.record(observed)
        for row in rows.close()["unconsumed_call_results"]:
            ledger.record(row)
        self.assertEqual(ledger.report()["counts"]["unknown"], 1)
        self.assertEqual(ledger.report()["usage"]["known_prompt_tokens"], 10)
        self.assertIsNone(ledger.report()["usage"]["prompt_tokens"])

    def test_bad_foreign_or_ambiguous_receipt_does_not_erase_a_valid_suffix(self):
        for fault in ("query", "duplicate", "marker", "usage", "engine"):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as directory:
                values = deepcopy(self.results)
                if fault == "query": values[0]["query_id"] = "foreign-query"
                if fault == "duplicate": values.append(deepcopy(values[0]))
                if fault == "marker": values[0].pop("engine_protocol_error")
                if fault == "usage": values[0]["prompt_tokens"] = -1
                if fault == "engine": values[0]["engine_uid"] = "foreign-engine"
                (Path(directory) / "engine-fixture.json").write_text(json.dumps(failure_report(values)))
                rows = native._DaftRows(None, self.expected, time.monotonic() + 30, None, directory)
                report = rows.close()
                self.assertEqual(report["status"], "unknown")
                self.assertEqual([row["call_id"] for row in report["unconsumed_call_results"]],
                                 [row["call_id"] for row in self.expected[1:]])

    def test_retry_cannot_upgrade_a_recovered_unknown(self):
        unknown = terminal(self.expected[0], status="unknown")
        rows = self.rows(report=failure_report([unknown], cleanup_errors=["CPU cleanup unconfirmed"]))
        self.assertEqual(rows.close()["unconsumed_call_results"][0]["status"], "unknown")
        (self.control / "engine-fixture.json").write_text(json.dumps(failure_report([self.results[0]])))
        report = rows.close()
        self.assertEqual(report["status"], "unknown")
        self.assertEqual(report["unconsumed_call_results"][0]["status"], "unknown")
        self.assertIsNone(report["unconsumed_call_results"][0]["prompt_tokens"])

    def test_failed_or_unknown_partial_usage_is_not_discarded_when_total_is_none(self):
        for status in ("failed", "unknown"):
            for missing in ("prompt", "completion"):
                with self.subTest(status=status, missing=missing), tempfile.TemporaryDirectory() as directory:
                    values = deepcopy(self.results)
                    values[0].update(status=status, text=None, error_type="CPU partial usage",
                                     submission_confirmed=status == "failed", total_tokens=None)
                    values[0][missing + "_tokens"] = None
                    values[0]["prompt_token_ids_json" if missing == "prompt" else "output_token_ids_json"] = "null"
                    (Path(directory) / "engine-fixture.json").write_text(json.dumps(failure_report(values)))
                    rows = native._DaftRows(None, self.expected, time.monotonic() + 30, None, directory)
                    receipt = rows.close()["unconsumed_call_results"][0]
                    self.assertEqual(receipt["status"], status)
                    self.assertIsNone(receipt["total_tokens"])
                    known = "completion" if missing == "prompt" else "prompt"
                    self.assertEqual(receipt[known + "_tokens"], 2 if known == "completion" else 5)
                    self.assertIsNone(receipt[missing + "_tokens"])

    def test_native_error_and_known_prefix_survive_close_errors_and_raw_is_preserved(self):
        engine = failure_report(self.results, cleanup_errors=["CPU close error"])
        rows = self.rows(self.results[:1], report=engine, error=PermissionError("native read failed"))
        observed = next(rows)
        with self.assertRaisesRegex(PermissionError, "native read failed") as caught:
            next(rows)
        self.assertTrue(caught.exception.__notes__)
        report = rows.last_close_report
        self.assertEqual(report["engine_reports"][0]["result_failure"], engine["result_failure"])
        self.assertEqual(observed["prompt_tokens"], 5)
        self.assertEqual(sum(row["prompt_tokens"] for row in report["unconsumed_call_results"]), 10)

    def test_healthy_close_keeps_existing_result_and_report_shape(self):
        engine = {key: value for key, value in failure_report([]).items() if key != "result_failure"}
        values = [{key: value for key, value in row.items() if key != "engine_protocol_error"} for row in self.results]
        rows = self.rows(values, report=engine)
        self.assertEqual(list(rows), values)
        self.assertNotIn("unconsumed_call_results", rows.last_close_report)
        self.assertEqual(rows.last_close_report["status"], "closed")


class ReceiptProcess(EngineProcess):
    mode = "normal"

    def _command(self, engine, result_fd):
        return [sys.executable, "-B", "-m", MODULE, "--receipt-worker", str(result_fd), self.mode]


class ShortProcess(ReceiptProcess):
    mode = "short"


class DuplicateProcess(ReceiptProcess):
    mode = "duplicate"


class ReorderedProcess(ReceiptProcess):
    mode = "reordered"


class FailedPrefixProcess(ReceiptProcess):
    mode = "failed-prefix"


class RaisingCloseProcess(ShortProcess):
    def close(self, timeout=DEFAULT_CLOSE_TIMEOUT_S):
        super().close(timeout)
        raise PermissionError("CPU fixture close observation failed")


@unittest.skipUnless(os.environ.get("RUN_LOTUS_RECORDS_CPU_LIBRARIES") == "1",
                     "opt-in pinned Daft process/CPU transport checks")
class DaftTransportTests(unittest.TestCase):
    def setUp(self):
        from src.baselines.text.movie_records import MovieRecordSource, prepare_movie_source

        self.temp = tempfile.TemporaryDirectory(prefix="daft-receipts-library-")
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        csv_path = root / "input.csv"
        # The malformed reply spans one native batch. Using only eligible rows
        # avoids treating Daft's batch_size upper bound as a fill guarantee.
        with csv_path.open("w", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(["reviewId", "id", "reviewText", "scoreSentiment"])
            writer.writerows([
                ["1", "taken_3", "excellent", "POSITIVE"],
                ["2", "taken_3", "bad", "NEGATIVE"],
                ["3", "taken_3", "very bad", "NEGATIVE"],
            ])
        manifest = prepare_movie_source(csv_path, root / "prepared", max_rows=3,
            expected_csv_sha256=hashlib.sha256(csv_path.read_bytes()).hexdigest())
        self.source = MovieRecordSource.from_manifest(manifest)
        self.query_id = "daft-receipts-cpu"
        self.expected = self.source.expected(self.query_id)
        self.engine = EngineSpec(self.temp.name, sys.executable, sys.executable, max_model_calls=3)
        self.resources = RecordsResources(prepare_batch_rows=3, model_batch_rows=3,
                                         query_timeout_s=60, startup_timeout_s=20)

    def stream(self, process):
        control = Path(self.temp.name) / "control"
        control.mkdir()
        deadline = time.monotonic() + 60
        frame = native._graph(self.source, FilterMethod("openai/local-fixture"), self.engine,
            GenerationSpec(), self.resources, self.query_id, self.expected, deadline, control,
            engine_type=process, model_gpus=0)
        return native._DaftRows(frame, self.expected, deadline, None, control)

    def evidence(self, rows, observed, error=None):
        destination = os.environ.get("DAFT_RECEIPT_EVIDENCE")
        if destination:
            from src.baselines.common.private_artifacts import write_private_json
            from src.baselines.common.redact import redact_json_values
            write_private_json(Path(destination) / (self._testMethodName + ".json"), redact_json_values(dict(
                observed=observed, close=rows.last_close_report,
                error_type=None if error is None else type(error).__name__,
                actual_model_requests=0, logical_gpus=0, token_usage_source="synthetic CPU fixture")))

    def assert_one_engine_without_replay(self, rows):
        apis = [report["engine_api"] for report in rows.last_close_report["engine_reports"]
                if report.get("engine_api") is not None]
        witnesses = list(apis)
        for report in rows.last_close_report["engine_reports"]:
            if report.get("result_failure"):
                witnesses.extend(json.loads(report["result_failure"]["raw_response_json"])["rows"])
                self.assertIsNone(report.get("engine_api"))
        self.assertTrue(witnesses)
        self.assertTrue(all(row["cpu_fixture_run_calls"] == 1 for row in witnesses))
        self.assertTrue(all(row["cpu_fixture_request_rows"] == 3 for row in witnesses))
        self.assertTrue(any(report["process_exit_confirmed"] for report in rows.last_close_report["engine_reports"]))

    def consume(self, process):
        rows, observed, error = self.stream(process), [], None
        try:
            for row in rows:
                observed.append(row)
        except Exception as failure:
            error = failure
        self.evidence(rows, observed, error)
        return rows, observed, error

    def test_actual_native_graph_healthy_and_failed_prefix_keep_all_usage(self):
        rows, values, error = self.consume(ReceiptProcess)
        self.assertIsNone(error)
        self.assertEqual(len(values), 3)
        self.assert_one_engine_without_replay(rows)
        self.assertEqual(len({row["engine_uid"] for row in values}), 1)
        self.assertTrue(all("engine_protocol_error" not in row for row in values))
        self.assertEqual(rows.last_close_report["status"], "closed")

    def test_actual_native_graph_failed_prefix_retains_successful_suffix(self):
        rows, values, error = self.consume(FailedPrefixProcess)
        self.assertIsNone(error)
        self.assert_one_engine_without_replay(rows)
        self.assertEqual([row["status"] for row in values], ["succeeded", "failed", "succeeded"])
        self.assertEqual(sum(row["prompt_tokens"] for row in values), 15)

    def test_actual_private_short_return_keeps_known_prefix_and_all_unknown_occurrences(self):
        rows, values, _error = self.consume(ShortProcess)
        self.assert_one_engine_without_replay(rows)
        self.assertEqual([row["status"] for row in values], ["succeeded", "unknown", "unknown"])
        self.assertEqual(sum(row["prompt_tokens"] or 0 for row in values), 5)
        self.assertTrue(all(row["engine_protocol_error"] for row in values))
        self.assertEqual(rows.last_close_report["unconsumed_call_results"], [])
        self.assertEqual(len(json.loads(rows.last_close_report["engine_reports"][0]["result_failure"]
                                        ["raw_response_json"])["rows"]), 1)

    def test_actual_early_close_recovers_suffix_and_never_upgrades_received_unknown(self):
        rows = self.stream(DuplicateProcess)
        first = next(rows)
        report = rows.close()
        self.assert_one_engine_without_replay(rows)
        self.evidence(rows, [first])
        self.assertEqual(first["status"], "unknown")
        ledger = CallLedger(self.expected, max_calls=3)
        ledger.record(first)
        for row in report["unconsumed_call_results"]:
            ledger.record(row)
        self.assertEqual(ledger.report()["counts"]["unknown"], 1)
        self.assertEqual(ledger.report()["usage"]["known_prompt_tokens"], 10)
        self.assertIsNone(ledger.report()["usage"]["prompt_tokens"])
        failure = report["engine_reports"][0]["result_failure"]
        self.assertEqual(failure["unassociated_result_indexes"], [0, 3])

    def test_actual_reordered_close_receipts_do_not_conflict_with_delivered_metadata(self):
        rows = self.stream(ReorderedProcess)
        first = next(rows)
        report = rows.close()
        self.assert_one_engine_without_replay(rows)
        self.evidence(rows, [first])
        self.assertIn("infer_worker_pid", first)
        self.assertEqual(len(report["unconsumed_call_results"]), 2)
        self.assertNotIn("result_receipt_recovery: ValueError", report["cleanup_errors"])

    def test_actual_close_exception_cannot_erase_a_malformed_batchs_known_usage(self):
        rows, values, error = self.consume(RaisingCloseProcess)
        self.assert_one_engine_without_replay(rows)
        self.assertEqual(len(values), 3)
        self.assertEqual(values[0]["prompt_tokens"], 5)
        self.assertTrue(all(row["engine_protocol_error"] for row in values))
        self.assertIsInstance(error, RuntimeError)
        self.assertEqual(rows.last_close_report["status"], "unknown")
        self.assertTrue(any(report.get("result_failure") for report in rows.last_close_report["engine_reports"]))

    @unittest.skipUnless(os.environ.get("SEMBENCH_CHECKOUT"), "requires the fixed original SemBench checkout")
    def test_actual_runner_late_receipts_and_native_interpretation_diagnostic_are_separate(self):
        from unittest.mock import patch
        from src.experiments.lotus_records_movie import run_movie_query
        from src.semantic_methods.lotus.engine import interpret_results, lm_report, make_lm

        stream = self.stream(DuplicateProcess)

        class InterruptedConsumer:
            delivered = False

            def __iter__(self):
                return self

            def __next__(self):
                if self.delivered:
                    raise PermissionError("CPU fixture consumer read failed")
                self.delivered = True
                return next(stream)

            def close(self):
                return stream.close()

            @property
            def last_close_report(self):
                return stream.last_close_report

        method, generation = FilterMethod("openai/local-fixture"), GenerationSpec()
        with self.assertRaisesRegex(PermissionError, "consumer read failed") as caught:
            run_movie_query(self.source, os.environ["SEMBENCH_CHECKOUT"], method, generation,
                            lambda: InterruptedConsumer(), query_id=self.query_id)
        report = caught.exception.query_report
        self.assert_one_engine_without_replay(stream)
        self.assertEqual(report["calls"]["counts"]["unknown"], 1)
        self.assertEqual(report["calls"]["counts"]["succeeded"], 2)
        self.assertEqual(report["calls"]["usage"]["known_prompt_tokens"], 10)
        self.assertIsNone(report["calls"]["usage"]["prompt_tokens"])
        self.assertEqual(report["lm"]["stats"]["physical_usage"]["prompt_tokens"], 0)
        before = deepcopy(report["lm"])
        # This is an independent post-run accounting diagnostic, not a second
        # interpretation inside the failed query or a second model request.
        diagnostic = make_lm(method, generation)
        with patch("lotus.sem_ops.postprocessors.filter_postprocess") as parser:
            with self.assertRaises(RuntimeError):
                interpret_results(report["calls"]["records"], diagnostic, method)
        parser.assert_not_called()
        self.assertEqual(diagnostic.stats.physical_usage.prompt_tokens, 10)
        self.assertEqual(diagnostic.stats.physical_usage.completion_tokens, 4)
        self.assertEqual(diagnostic.stats.virtual_usage.prompt_tokens, 10)
        self.assertEqual(report["lm"], before)
        self.evidence(stream, report["calls"]["records"], caught.exception)
        destination = os.environ.get("DAFT_RECEIPT_EVIDENCE")
        if destination:
            from src.baselines.common.private_artifacts import write_private_json
            from src.baselines.common.redact import redact_json_values
            write_private_json(Path(destination) / "runner-and-separate-interpretation.json", redact_json_values(dict(
                query=report, independent_accounting_diagnostic=lm_report(diagnostic),
                parser_called=False, model_requests=0, diagnostic_included_in_query=False)))


def serve_cpu(result_fd, mode):
    """Exercise the actual private transport with a fixed, model-free kernel."""
    from src.baselines.text.frameworks.daft_engine_process import serve
    from src.serving.backends import vllm_engine

    class CPU:
        def __init__(self, spec, *_):
            self.uid, self.runs, self.requests = "receipt-cpu-" + str(os.getpid()), 0, 0
            self.limit = spec.max_model_calls

        def ready(self):
            assert "vllm" not in sys.modules and "lotus" not in sys.modules
            return dict(engine_uid=self.uid, pid=os.getpid(), fixture=True)

        def run_prepared(self, inputs, **_):
            self.runs += 1
            self.requests += len(inputs)
            if self.limit is None or self.requests > self.limit:
                raise RuntimeError("CPU fixture permanent request allowance exceeded")
            results = [dict(terminal(item, marker=False, uid=self.uid), model_fixture_pid=os.getpid(),
                            cpu_fixture_run_calls=self.runs, cpu_fixture_request_rows=self.requests)
                       for item in inputs]
            if mode == "short": return results[:1]
            if mode == "duplicate": return results + [deepcopy(results[0])]
            if mode == "reordered": return list(reversed(results))
            if mode == "failed-prefix":
                results[1].update(status="failed", text=None, error_type="CPUFixtureFailure", finish_reason="abort")
            return results

        def close(self):
            return dict(status="closed", engine_api_shutdown_confirmed=True,
                        cpu_fixture_run_calls=self.runs, cpu_fixture_request_rows=self.requests)

    vllm_engine.VllmEngine = CPU
    serve(result_fd)


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "--receipt-worker":
        serve_cpu(int(sys.argv[2]), sys.argv[3])
    else:
        unittest.main()
