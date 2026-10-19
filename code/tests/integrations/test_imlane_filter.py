"""Ordinary Filter bridge contracts with original LOTUS and explicit CPU engines."""
from dataclasses import asdict
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from integrations.imlane_records.filter_host import FilterResults
from integrations.imlane_records.filter_workload import Occurrences, PROTOCOL, input_record
from src.semantic_methods.lotus.records import CallLedger, FilterMethod, RecordsResources, identity
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec, _prepared


def source(query="q", count=3):
    return [dict(identity(query, "重复-来源", index // 2, index % 2), reviewText="same\n中文\0{}",
                 scoreSentiment="do not transmit") for index in range(count)]


def terminal(prepared, status="succeeded"):
    item = _prepared([prepared])[0][0]
    return dict(item, status=status, submission_confirmed=True, text="Answer: True" if status == "succeeded" else None,
                prompt_tokens=5, completion_tokens=2, total_tokens=7, prompt_token_ids_json="[1,2,3,4,5]",
                output_token_ids_json="[6,7]", finish_reason="stop" if status == "succeeded" else "abort",
                error_type=None if status == "succeeded" else "FixtureError", engine_uid="fixed",
                engine_request_id=item["call_id"])


class FixedEngine:
    def __init__(self, *_):
        self.ready_report = {"engine_uid": "fixed"}
        self.calls, self.closes = [], 0

    def run_prepared(self, rows):
        self.calls.extend(rows)
        return [terminal(row) for row in rows]

    def close(self):
        self.closes += 1
        return dict(status="closed", process_exit_confirmed=True, cleanup_errors=[], gpu_exit_confirmed=None)


class PartialEngine(FixedEngine):
    def run_prepared(self, rows):
        return [terminal(row, "failed" if row["call_index"] == 1 else "succeeded") for row in rows]


class Prepare:
    def __init__(self, _method):
        pass

    def prepare_rows(self, rows):
        return [dict({key: value for key, value in row.items() if key != "reviewText"},
                     messages_json=json.dumps([dict(role="user", content=row["reviewText"])]),
                     prepare_worker_pid=os.getpid(), prepare_started_ns=1, prepare_finished_ns=2) for row in rows]


class LocalAuthor:
    """Deterministic author fixture, with completion order independent of submit."""
    def __init__(self, worker):
        self.worker, self.pending = worker, {}
        self.meta, self._terminal = {"protocol": PROTOCOL}, False
        self.submissions, self.closes, self.aborts = [], 0, 0

    def begin(self, query, **_):
        self.query = query

    def submit(self, block, rows, **_):
        self.submissions.append(block)
        if json.loads(rows[0]["record_json"]) == {"operation": "close"}:
            values = [self.worker.envelope(rows[0], close=self.worker.close())]
        else:
            values = [self.worker.envelope(row, result=result, attempted=attempted)
                      for row, (result, attempted) in zip(rows, self.worker.run(rows), strict=True)]
        self.pending[block] = [dict(call_id=row["call_id"], row_id=row["row_id"], outcome_json=json.dumps(value))
                               for row, value in zip(rows, values, strict=True)]

    def wait_ready(self, **_):
        return list(reversed(self.pending))

    def collect(self, block, **_):
        return self.pending.pop(block)

    def end(self, **_):
        pass

    def close(self):
        self.closes += 1

    def abort(self):
        self._terminal = True
        self.aborts += 1


class FilterTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.TemporaryDirectory(prefix="imlane-filter-cpu-")
        self.addCleanup(self.root.cleanup)
        self.config = dict(method=asdict(FilterMethod("openai/local-fixture")),
                           engine=asdict(EngineSpec(model_path=self.root.name, model_python=sys.executable,
                                                   driver_python=sys.executable, max_model_calls=8)),
                           generation=asdict(GenerationSpec()), resources=asdict(RecordsResources()),
                           query_id="q", control_dir=self.root.name, deadline_monotonic=time.monotonic() + 30,
                           model_environment={})

    def worker(self, engine=FixedEngine, prepare=Prepare):
        from integrations.imlane_records.filter_udf import FilterWorker
        return FilterWorker(self.config, prepare_type=prepare, engine_type=engine)

    def iterator(self, worker, rows, **kwargs):
        client = LocalAuthor(worker)
        iterator = FilterResults(rows, client, query_id="q", max_calls=8, control_dir=self.root.name,
                                 deadline=time.monotonic() + 20,
                                 close_owner=lambda: {"resources_confirmed": True}, **kwargs)
        self.addCleanup(iterator.close)
        return iterator, client

    def test_reversible_identity_preserves_repeated_source_and_never_transmits_labels(self):
        records = source()
        mapping = Occurrences("q", max_calls=3)
        encoded = mapping.encode(records)
        self.assertEqual([row["row_id"] for row in encoded], [0, 1, 2])
        self.assertEqual([input_record(row)["call_id"] for row in encoded], [row["call_id"] for row in records])
        self.assertEqual([input_record(row)["row_id"] for row in encoded], ["重复-来源"] * 3)
        self.assertNotIn("scoreSentiment", json.dumps(encoded))
        self.assertEqual(input_record(encoded[0])["source_position"], input_record(encoded[1])["source_position"])
        self.assertNotEqual(encoded[0]["call_id"], encoded[1]["call_id"])

    def test_rejected_suffix_does_not_register_unsubmitted_prefix(self):
        mapping = Occurrences("q", max_calls=3)
        bad = source()
        bad[-1]["call_id"] = "wrong"
        with self.assertRaises(ValueError):
            mapping.encode(bad)
        self.assertEqual(mapping.inputs, {})
        self.assertEqual(mapping.encode(source())[0]["row_id"], 0)

    def test_mixed_block_keeps_failed_usage_and_later_success_before_native_parser_error(self):
        from src.semantic_methods.lotus.engine import interpret_results, make_lm
        worker = self.worker(PartialEngine)
        values, client = self.iterator(worker, source(), block_rows=3)
        results = list(values)
        ledger = CallLedger([{k: v for k, v in row.items() if k not in ("reviewText", "scoreSentiment")} for row in source()], max_calls=3)
        for row in results:
            ledger.record(row)
        self.assertEqual([r["status"] for r in results], ["succeeded", "failed", "succeeded"])
        self.assertEqual(ledger.report()["usage"]["known_prompt_tokens"], 15)
        self.assertEqual(results[1]["output_token_ids_json"], "[6,7]")
        lm = make_lm(FilterMethod("openai/local-fixture"), GenerationSpec())
        with self.assertRaises(RuntimeError):
            interpret_results(results, lm, FilterMethod("openai/local-fixture"))
        self.assertEqual(lm.stats.physical_usage.prompt_tokens, 10)
        values.close()
        self.assertEqual(client.submissions[-1], "filter-close")
        self.assertTrue(values.closed)

    def test_bounded_ready_order_and_two_accepted_blocks_keep_all_observed_results(self):
        values, client = self.iterator(self.worker(), source(count=6), block_rows=2)
        results = list(values)
        self.assertEqual(len(results), 6)
        self.assertEqual(len({row["call_id"] for row in results}), 6)
        self.assertEqual([row["call_index"] for row in results[:2]], [0, 1])
        self.assertEqual([row["source_position"] for row in results[:2]], [1, 1])
        self.assertEqual(len(client.submissions), 3)

    def test_bad_later_block_preserves_accepted_prefix_then_raises(self):
        records = source(count=4)
        records[2]["call_id"] = "bad suffix"
        values, client = self.iterator(self.worker(), records, block_rows=2)
        seen = []
        with self.assertRaises(ValueError):
            for row in values:
                seen.append(row)
        self.assertEqual(len(seen), 2)
        self.assertEqual(client.submissions, ["filter-0"])

    def test_model_failure_stops_new_blocks_but_delivers_already_accepted_terminals(self):
        values, client = self.iterator(self.worker(PartialEngine), source(count=8), block_rows=2)
        results = list(values)
        self.assertEqual(len(results), 4)
        self.assertEqual(client.submissions, ["filter-0", "filter-1"])
        self.assertTrue(values.failed)
        self.assertEqual(sum(row["prompt_tokens"] or 0 for row in results), 10)

    def test_output_reorder_missing_duplicate_and_cross_query_are_rejected(self):
        for fault in ("reorder", "missing", "duplicate", "query"):
            with self.subTest(fault=fault):
                mapping = Occurrences("q", max_calls=8)
                encoded = mapping.encode(source())
                client = LocalAuthor(self.worker())
                client.submit("b", encoded)
                rows = client.collect("b")
                if fault == "reorder": rows.reverse()
                if fault == "missing": rows.pop()
                if fault == "duplicate": rows[1] = rows[0]
                if fault == "query":
                    value = json.loads(rows[0]["outcome_json"])
                    value["query_id"] = "different"
                    rows[0]["outcome_json"] = json.dumps(value)
                with self.assertRaises(ValueError):
                    mapping.decode(rows, encoded)
                self.assertEqual(mapping.collected, set())

    def test_close_failure_is_retryable_without_inference_reuse(self):
        class RetryClose(FixedEngine):
            def close(self):
                self.closes += 1
                if self.closes == 1:
                    return dict(status="unknown", process_exit_confirmed=False, cleanup_errors=["fixture"])
                return super().close()
        worker = self.worker(RetryClose)
        encoded = Occurrences("q", max_calls=8).encode(source(count=1))
        worker.run(encoded)
        self.assertEqual(worker.close()["status"], "unknown")
        self.assertFalse(worker.closed)
        self.assertEqual(worker.run(encoded)[0][0]["status"], "failed")
        self.assertEqual(worker.close()["status"], "closed")
        self.assertTrue(worker.closed)

    def test_cancel_before_model_preserves_definite_non_submission_and_closes_without_process(self):
        worker = self.worker()
        Path(self.root.name, "stop").touch()
        encoded = Occurrences("q", max_calls=8).encode(source())
        results = [row for row, _ in worker.run(encoded)]
        self.assertEqual({row["status"] for row in results}, {"failed"})
        self.assertEqual({row["submission_confirmed"] for row in results}, {False})
        self.assertIsNone(worker.kernel)
        self.assertTrue(worker.close()["no_model_process_started"])

    def test_failed_model_construction_does_not_claim_no_process_ever_started(self):
        class FailedConstructor:
            def __init__(self, *_):
                raise RuntimeError("fixture constructor lost a possible child handle")
        worker = self.worker(FailedConstructor)
        encoded = Occurrences("q", max_calls=8).encode(source())
        worker.run(encoded)
        report = worker.close()
        self.assertFalse(report["no_model_process_started"])
        self.assertFalse(report["process_exit_confirmed"])
        self.assertFalse(worker.closed)

    def test_model_launch_clears_author_abi_without_mutating_parent_environment(self):
        from integrations.imlane_records.model_process import IMLaneEngineProcess
        from src.baselines.text.frameworks.daft_engine_process import EngineProcess
        with patch.dict(os.environ, {"CUDA_VISIBLE_DEVICES": "1", "LD_LIBRARY_PATH": "/author/arrow16"}), \
             patch.object(EngineProcess, "__init__", return_value=None):
            process = IMLaneEngineProcess(model_environment={"CUDA_VISIBLE_DEVICES": "1"})
            command = process._command(EngineSpec(**self.config["engine"]), 5)
            self.assertEqual(command[:3], ["/usr/bin/env", "-u", "LD_LIBRARY_PATH"])
            self.assertIn("CUDA_VISIBLE_DEVICES=1", command)
            self.assertEqual(os.environ["LD_LIBRARY_PATH"], "/author/arrow16")
            with self.assertRaises(ValueError):
                IMLaneEngineProcess(model_environment={"CUDA_VISIBLE_DEVICES": "0"})

    def test_unknown_after_possible_acceptance_keeps_digest_and_never_invents_usage(self):
        class Unknown(FixedEngine):
            def run_prepared(self, rows):
                raise EOFError("fixture failed after possible acceptance")
        worker = self.worker(Unknown)
        encoded = Occurrences("q", max_calls=8).encode(source())
        results = [row for row, _ in worker.run(encoded)]
        self.assertEqual({row["status"] for row in results}, {"unknown"})
        self.assertEqual({row["submission_confirmed"] for row in results}, {False})
        self.assertTrue(all(len(row["messages_sha256"]) == 64 for row in results))
        self.assertTrue(all(row["prompt_tokens"] is None for row in results))
        self.assertTrue(worker.broken)

    def test_failed_engine_null_token_component_preserves_other_known_usage(self):
        for missing in ("prompt", "completion"):
            with self.subTest(missing=missing):
                class PartialTokens(FixedEngine):
                    def run_prepared(self, rows):
                        values = super().run_prepared(rows)
                        values[0].update(status="failed", error_type="ValueError", text=None, total_tokens=None)
                        values[0][missing + "_tokens"] = None
                        values[0]["prompt_token_ids_json" if missing == "prompt" else "output_token_ids_json"] = "null"
                        return values
                worker = self.worker(PartialTokens)
                encoded = Occurrences("q", max_calls=8).encode(source())
                values = [row for row, _ in worker.run(encoded)]
                self.assertEqual(values[0]["status"], "failed")
                known = "completion" if missing == "prompt" else "prompt"
                self.assertEqual(values[0][known + "_tokens"], 2 if known == "completion" else 5)
                self.assertIsNone(values[0][missing + "_tokens"])

    def test_malformed_engine_item_cannot_erase_other_verified_terminals_in_same_return(self):
        for fault in ("status", "digest", "identity", "usage", "engine", "tokens", "count", "unbound_unknown"):
            with self.subTest(fault=fault):
                class MalformedEngine(FixedEngine):
                    def run_prepared(self, rows):
                        values = super().run_prepared(rows)
                        if fault == "unbound_unknown":
                            values[1].update(status="unknown", error_type="ValueError", messages_sha256=None,
                                             prompt_tokens=-1)
                            return values
                        key, value = {"status": ("status", "invalid"), "digest": ("messages_sha256", "wrong"),
                                      "identity": ("call_id", "wrong"), "usage": ("prompt_tokens", -1),
                                      "engine": ("engine_uid", "different-instance"),
                                      "tokens": ("prompt_token_ids_json", None),
                                      "count": ("output_token_ids_json", "[6]")}[fault]
                        values[1][key] = value
                        return values
                worker = self.worker(MalformedEngine)
                encoded = Occurrences("q", max_calls=8).encode(source())
                rows = [row for row, _ in worker.run(encoded)]
                self.assertEqual([row["status"] for row in rows], ["succeeded", "unknown", "succeeded"])
                self.assertEqual(sum(row["prompt_tokens"] or 0 for row in rows), 10)
                self.assertEqual(rows[2]["output_token_ids_json"], "[6,7]")
                self.assertEqual(rows[1]["submission_confirmed"], fault not in ("digest", "identity", "engine", "unbound_unknown"))
                self.assertTrue(worker.broken)

    def test_later_subbatch_prepare_failure_cannot_erase_prior_terminal(self):
        self.config["resources"]["model_batch_rows"] = 1
        class BrokenSuffix(Prepare):
            def prepare_rows(self, rows):
                if rows[0]["call_index"]:
                    raise ValueError("fixture invalid preparation suffix")
                return super().prepare_rows(rows)
        worker = self.worker(prepare=BrokenSuffix)
        encoded = Occurrences("q", max_calls=8).encode(source())
        rows = [row for row, _ in worker.run(encoded)]
        self.assertEqual([row["status"] for row in rows], ["succeeded", "failed", "failed"])
        self.assertEqual(rows[0]["prompt_tokens"], 5)
        self.assertEqual(len(worker.kernel.calls), 1)

    def test_malformed_or_short_prepare_suffix_keeps_prior_usage_and_returns_terminal_rows(self):
        self.config["resources"]["model_batch_rows"] = 1
        for fault in ("messages", "short", "identity", "evidence"):
            with self.subTest(fault=fault):
                class Malformed(Prepare):
                    def prepare_rows(self, rows):
                        values = super().prepare_rows(rows)
                        if rows[0]["call_index"]:
                            if fault == "messages": values[0]["messages_json"] = "invalid-json"
                            if fault == "short": values.clear()
                            if fault == "identity": values[0]["call_id"] = "wrong"
                            if fault == "evidence": values[0].pop("prepare_worker_pid")
                        return values
                worker = self.worker(prepare=Malformed)
                encoded = Occurrences("q", max_calls=8).encode(source())
                rows = [row for row, _ in worker.run(encoded)]
                self.assertEqual([row["status"] for row in rows], ["succeeded", "failed", "failed"])
                self.assertEqual(rows[0]["prompt_tokens"], 5)
                self.assertIsNone(rows[1]["prompt_tokens"])
                self.assertEqual(len(worker.kernel.calls), 1)

    def test_aliases_share_one_interpreter_slot_and_healthy_queries_have_separate_cancel_state(self):
        import pyarrow as pa
        from integrations.imlane_records import filter_udf
        from integrations.imlane_records.echo_udf import INPUT
        class Worker(filter_udf.FilterWorker):
            def __init__(self, config):
                super().__init__(config, prepare_type=Prepare, engine_type=FixedEngine)
        spec = importlib.util.spec_from_file_location("imlane_filter_other_alias", filter_udf.__file__)
        alias = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(alias)
        self.assertIs(alias._slot, filter_udf._slot)
        config_path = Path(self.root.name, "worker-config.json")
        with patch.object(filter_udf._slot, "worker", None), patch.object(filter_udf._slot, "fingerprint", None), \
             patch.object(filter_udf._slot, "finished", set()), patch.object(filter_udf, "FilterWorker", Worker), \
             patch.object(alias, "FilterWorker", Worker), patch.dict(os.environ, {"IMLANE_FILTER_CONFIG": str(config_path)}):
            config_path.write_text(json.dumps(self.config))
            encoded = Occurrences("q", max_calls=8).encode(source(count=1))
            filter_udf.infer_records(pa.Table.from_pylist(encoded, schema=INPUT))
            first = alias._slot.worker
            close = dict(call_id="close-q", row_id=0, query_id="q", stage_id="filter", record_json='{"operation":"close"}')
            alias.infer_records(pa.Table.from_pylist([close], schema=INPUT))
            self.assertTrue(first.closed)
            second_control = Path(self.root.name, "q2-control")
            second_control.mkdir()
            self.config.update(query_id="q2", control_dir=str(second_control))
            config_path.write_text(json.dumps(self.config))
            encoded = Occurrences("q2", max_calls=8).encode(source("q2", count=1))
            value = alias.infer_records(pa.Table.from_pylist(encoded, schema=INPUT)).to_pylist()[0]
            self.assertEqual(json.loads(value["outcome_json"])["query_id"], "q2")
            self.assertIsNot(first, filter_udf._slot.worker)
            self.assertNotEqual(first.control, filter_udf._slot.worker.control)
            self.config["method"]["model_name"] = "changed"
            config_path.write_text(json.dumps(self.config))
            with self.assertRaisesRegex(ValueError, "cannot change"):
                filter_udf.infer_records(pa.Table.from_pylist(encoded, schema=INPUT))
            filter_udf._slot.worker.close()

    def test_failed_author_finalization_is_not_closed_and_can_retry(self):
        values, _client = self.iterator(self.worker(), source())
        list(values)
        attempts = []
        def close_owner():
            attempts.append(True)
            return {"resources_confirmed": len(attempts) > 1}
        values.close_owner = close_owner
        with self.assertRaisesRegex(RuntimeError, "resources remain unconfirmed"):
            values.close()
        self.assertFalse(values.closed)
        values.close()
        self.assertTrue(values.closed)

    def test_model_gpu_exit_unknown_keeps_the_owned_lease(self):
        from integrations.imlane_records import verify_filter
        from unittest.mock import Mock
        lease = Mock()
        report = dict(passed=True, model_launch_possible=True, gpu_exit_confirmed=None)
        with patch.object(verify_filter.owner, "resource_state", return_value={"resources_confirmed": True}):
            verify_filter.settle_lease(lease, [object()], report)
        lease.release.assert_not_called()
        self.assertFalse(report["passed"])
        self.assertEqual(report["host_lease"]["status"], "held_unresolved")

    def test_late_monitor_log_cleanup_or_signal_failure_cannot_pass_qualification(self):
        from integrations.imlane_records.verify_filter import apply_final_verdict
        for key in ("sampler_errors", "stderr_capture_errors", "cleanup_errors", "final_cleanup_notices", "cleanup", "stop"):
            with self.subTest(key=key):
                report = dict(passed=True)
                if key != "stop": report[key] = [{"passed": False}]
                apply_final_verdict(report, stopped=key == "stop")
                self.assertFalse(report["passed"])

    @unittest.skipUnless(os.environ.get("SEMBENCH_CHECKOUT"), "requires fixed original SemBench checkout")
    def test_full_original_movie_query_and_evaluator_through_cpu_bridge(self):
        from src.experiments.lotus_records_movie import run_movie_query
        from src.semantic_methods.lotus.engine import LotusFilterMethod
        from tests.experiments.test_lotus_records_movie import fixture_source
        from tests.integrations.imlane_filter_fixture import FixedEngineProcess
        directory = Path(self.root.name, "source")
        directory.mkdir()
        dataset = fixture_source(directory)
        rows = [dict(identity("q", row["row_id"], row["source_position"]), reviewText=row["reviewText"])
                for row in dataset.rows() if row["id"] == "taken_3"]
        def execute():
            values, _client = self.iterator(self.worker(FixedEngineProcess, LotusFilterMethod), rows, block_rows=2)
            return values
        report = run_movie_query(dataset, os.environ["SEMBENCH_CHECKOUT"], FilterMethod("openai/local-fixture"),
                                 GenerationSpec(), execute, query_id="q")
        self.assertEqual(report["status"], "completed")
        self.assertEqual(report["result"], {"positive_review_cnt": 1})
        self.assertEqual(report["calls"]["counts"]["succeeded"], 3)
        self.assertEqual(report["calls"]["usage"]["known_prompt_tokens"], 15)
        self.assertTrue(report["backend_close"]["author"]["resources_confirmed"])
        self.assertEqual(report["quality"]["classification_audit"]["false_positive"], 0)

    @unittest.skipUnless(importlib.util.find_spec("lotus"), "requires the pinned LOTUS CPU environment")
    def test_original_formatter_private_child_parser_and_physical_exit(self):
        import psutil
        from src.semantic_methods.lotus.engine import LotusFilterMethod, interpret_results, make_lm
        from tests.integrations.imlane_filter_fixture import FixedEngineProcess
        worker = self.worker(FixedEngineProcess, LotusFilterMethod)
        values, _client = self.iterator(worker, source(), block_rows=2)
        results = list(values)
        self.assertTrue(all(row["status"] == "succeeded" for row in results))
        self.assertEqual(len({row["engine_uid"] for row in results}), 1)
        pid = worker.kernel.process.pid
        self.assertNotEqual(pid, os.getpid())
        self.assertFalse(worker.kernel.ready_report["lotus_imported"])
        lm = make_lm(FilterMethod("openai/local-fixture"), GenerationSpec())
        decisions = interpret_results(results, lm, FilterMethod("openai/local-fixture"))
        self.assertEqual(sum(row["decision"] for row in decisions), 2)
        values.close()
        self.assertFalse(psutil.pid_exists(pid))
        self.assertTrue(values.last_close_report["model"]["process_exit_confirmed"])
        self.assertIsNone(values.last_close_report["gpu_exit_confirmed"])


if __name__ == "__main__":
    unittest.main()
