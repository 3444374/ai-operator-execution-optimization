"""Real LOTUS/Arrow/Q3 evaluation with an explicit in-process CPU carrier fixture.

No author binary, model interpreter, HTTP server or GPU is launched. The fixture
uses the real FilterWorker, FilterResults and occurrence/close protocols; it does
not qualify the Linux author runtime or actual model token/template behavior.
"""

from dataclasses import asdict, replace
from copy import deepcopy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.infrastructure import runner_lease
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec, _prepared
from tests.experiments.test_lotus_records_movie import fixture_source


LIBRARIES = all(importlib.util.find_spec(name) for name in ("lotus", "pyarrow", "duckdb", "cdlib"))
CHECKOUT = os.environ.get("SEMBENCH_CHECKOUT")


class _CPUCarrier:
    """Exercise the real formatter/carrier wire with explicit artificial tokens."""

    def __init__(self, args, *, mode="normal", close_ok=True):
        from integrations.imlane_records.filter_udf import FilterWorker

        self.meta = dict(protocol="imlane.records.v2")
        self._terminal, self.pending = False, {}
        self.prepared, self.submitted = [], []
        self.close_ok = close_ok
        fixture = self

        class FixedEngine:
            def __init__(self, engine, *_):
                self.ready_report = dict(engine_uid="explicit-cpu-fixture", fixture=True, inference_calls=0)
                self.limit, self.used = engine.max_model_calls, 0

            def run_prepared(self, rows):
                self.used += len(rows)
                if self.used > self.limit:
                    raise AssertionError("fixture exceeded the actual engine cap")
                fixture.prepared.extend(deepcopy(rows))
                identities, _messages = _prepared(rows)
                results = []
                for item in identities:
                    status = ("unknown" if item["source_position"] == 3 else "failed") if (
                        mode == "partial" and item["source_position"] != 0) else "succeeded"
                    unknown = status == "unknown"
                    result = dict(item, status=status, submission_confirmed=not unknown,
                        text=("Answer: True" if item["source_position"] == 0 else "Answer: False")
                             if status == "succeeded" else None,
                        finish_reason="stop" if status == "succeeded" else None,
                        engine_uid=self.ready_report["engine_uid"], engine_request_id=item["call_id"],
                        prompt_tokens=None if unknown else 5, completion_tokens=None if unknown else 2,
                        total_tokens=None if unknown else 7,
                        prompt_token_ids_json=None if unknown else "[1,2,3,4,5]",
                        output_token_ids_json=None if unknown else "[6,7]", logprobs_json="null",
                        error_type=None if status == "succeeded" else "ExplicitCPUFailure",
                        usage_source="configured artificial CPU tokens; no inference")
                    if mode == "bad-digest":
                        result["messages_sha256"] = "0" * 64
                    results.append(result)
                return results

            def close(self):
                return dict(status="closed", process_exit_confirmed=True, cleanup_errors=[],
                    engine_api=dict(status="closed", engine_api_shutdown_confirmed=fixture.close_ok),
                    no_inference=True)

        self.worker = FilterWorker(json.loads(args.filter_config.read_text()), engine_type=FixedEngine)

    def begin(self, query_id, **_):
        if query_id != self.worker.query_id:
            raise AssertionError("fixture query identity changed")

    def submit(self, block, rows, **_):
        from integrations.imlane_records.echo_udf import INPUT, OUTPUT
        import pyarrow as pa

        table = pa.Table.from_pylist(rows, schema=INPUT)
        table.validate(full=True)
        sources = table.to_pylist()
        if len(sources) == 1 and json.loads(sources[0]["record_json"]) == {"operation": "close"}:
            values = [self.worker.envelope(sources[0], close=self.worker.close())]
        else:
            self.submitted.extend(deepcopy(sources))
            values = [self.worker.envelope(row, result=result, attempted=attempted)
                      for row, (result, attempted) in zip(sources, self.worker.run(sources), strict=True)]
        outputs = [dict(call_id=row["call_id"], row_id=row["row_id"], outcome_json=json.dumps(value))
                   for row, value in zip(sources, values, strict=True)]
        self.pending[block] = pa.Table.from_pylist(outputs, schema=OUTPUT).to_pylist()

    def wait_ready(self, **_):
        return list(self.pending)

    def collect(self, block, **_):
        return self.pending.pop(block)

    def end(self, **_):
        pass

    def close(self, **_):
        self._terminal = True

    def abort(self):
        self._terminal = True
        self.worker.close()


@unittest.skipUnless(LIBRARIES and CHECKOUT, "requires pinned CPU libraries and SEMBENCH_CHECKOUT")
class IMLaneMovieFormalTests(unittest.TestCase):
    def setUp(self):
        from src.experiments import imlane_movie_formal as adapter

        self.adapter = adapter
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = fixture_source(self.root)
        self.method, self.generation = FilterMethod("openai/local-fixture"), GenerationSpec()
        self.resources = RecordsResources(prepare_batch_rows=2, model_batch_rows=2)
        self.engine = EngineSpec(str(self.root / "unloaded-model"), sys.executable, sys.executable,
                                 max_model_calls=3)
        self.output = self.root / "output"
        self.output.mkdir()
        self.artifact = self.root / "artifacts"
        # Linux birth ticks are unavailable on the local macOS CPU driver.
        self.enterContext(patch.object(runner_lease, "_process_start_id", return_value="explicit-cpu-birth"))
        self.enterContext(patch("psutil.Process.parents", return_value=[]))
        lease = runner_lease.acquire_runner_lease(self.artifact / runner_lease.HOST_SCOPE_DIR_NAME,
            config_fingerprint=runner_lease.HOST_SCOPE_FINGERPRINT, repository_commit="explicit-cpu-fixture")
        self.addCleanup(lease.release)
        self.lease = lease
        preflight = self.root / "preflight.json"
        preflight.write_text(json.dumps(dict(status="ok")))
        self.ledger = CellBudgetLedger.create(self.root / "budget.sqlite", AttemptBudget("cpu-fixture", 12),
                                             deadline_utc=None)
        self.ledger.reserve_unit("q", 3)
        self.claim = self.ledger.claim_unit("q")
        self.context = dict(output_dir=self.output,
            runtime_binding=dict(imlane_artifact_root=str(self.artifact), imlane_author_root=str(self.root / "author"),
                imlane_method_python=sys.executable, imlane_bridge_binary=str(self.root / "bridge"),
                runtime_preflight_report=str(preflight), cuda_visible_devices="fixture-no-gpu"),
            owner_context=dict(artifact_root=str(self.artifact), lease_path=str(lease.path),
                lease_sha256=hashlib.sha256(lease.path.read_bytes()).hexdigest(), gpu_idle_max_mib=1,
                owner_resource_limits=dict(cpu_cores=8, rss_bytes=20 * 1024**3, shm_bytes=512 * 1024**2)),
            budget=dict(path=str(self.ledger.path), id="cpu-fixture", limit=12, unit_id="q"),
            claimed_budget=self.claim)
        self.events, self.carriers = [], []
        self.mode, self.close_ok = "normal", True
        self.gpu_confirmed = self.lease_confirmed = True
        self.cleanup_error = None
        test = self

        class FixtureScope:
            def __init__(self, args, mode, helper, report, *, kernel):
                self.args, self.report = args, report
                self.namespace, self.closed = "explicit-cpu-scope", False
                self.root = args.output / self.namespace
                self.root.mkdir()
                self.children = []
                args.scopes.append(self)

            def __enter__(self):
                self.env, self.port = {}, 0
                return self

            def track_root(self, process):
                pass

            def close(self):
                if self.closed:
                    return
                self.report["cleanup"].append(dict(namespace=self.namespace, passed=True,
                    owned_survivors=[], remaining_shm={}, owned_threads_stopped=True, fixture=True))
                self.closed = True
                if test.cleanup_error is not None:
                    raise test.cleanup_error

        self.scope = self.enterContext(patch.object(adapter.verify_filter, "FilterScope", FixtureScope))
        self.popen = self.enterContext(patch.object(adapter, "Popen", return_value=Mock(stderr=Mock())))
        self.enterContext(patch.object(adapter.verify, "_capture_stderr", return_value=Mock()))
        self.enterContext(patch.object(adapter.verify_filter, "preflight_method", return_value=[dict(fixture=True)]))
        self.enterContext(patch.object(adapter.verify_filter, "validate_author_build", return_value=dict(fixture=True)))
        self.enterContext(patch.object(adapter.verify_filter.os, "sched_getaffinity", return_value={0}, create=True))
        self.enterContext(patch.object(EngineSpec, "validate_driver_environment", return_value=dict(fixture=True)))
        self.enterContext(patch.object(adapter, "RecordsBridge", side_effect=self.carrier))
        self.guard = Mock()
        self.guard.finish.side_effect = lambda: dict(gpu_exit_confirmed=self.gpu_confirmed,
            external_lease_valid=self.lease_confirmed, fixture=True)
        self.guard_factory = self.enterContext(patch.object(adapter, "AuthorRunGuard", return_value=self.guard))

    def carrier(self, *_, **__):
        args = SimpleNamespace(filter_config=self.output / "imlane-owner/worker-0.json")
        client = _CPUCarrier(args, mode=self.mode, close_ok=self.close_ok)
        self.carriers.append(client)
        return client

    def run_query(self, **changes):
        handlers = {number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM)}
        try:
            return self.adapter.run(self.source, CHECKOUT, self.method, self.generation,
                changes.get("engine", self.engine), self.resources, query_id="q",
                profile=changes.get("profile", asdict(self.resources)),
                observer=changes.get("observer", self.events.append),
                context=changes.get("context", self.context))
        finally:
            self.assertEqual({n: signal.getsignal(n) for n in handlers}, handlers)
            self.assertTrue(self.lease.path.is_file())
            self.assertEqual(self.ledger.snapshot()["units"][0]["claimed"], 1)

    def test_original_q3_parser_count_evaluator_usage_and_actual_messages(self):
        from src.experiments.lotus_records_movie import fixed_response_qualification

        (self.output / "context.json").write_text("{}")
        (self.output / "supervisor.log").write_text("explicit CPU fixture")
        report = self.run_query()
        reference = fixed_response_qualification(self.source, CHECKOUT, self.method, self.generation,
            [dict(row_id="movie-row-" + str(i), text="Answer: True" if i == 0 else "Answer: False",
                  prompt_tokens=5, completion_tokens=2) for i in (0, 2, 3)], query_id="q")
        self.assertEqual(report["result"], {"positive_review_cnt": 1})
        self.assertEqual(report["quality"], reference["quality"])
        self.assertEqual(report["lm"]["stats"], reference["lm"]["stats"])
        self.assertEqual(report["calls"]["usage"]["total_tokens"], 21)
        actual, _ = _prepared(self.carriers[0].prepared)
        self.assertEqual([r["messages_sha256"] for r in report["calls"]["records"]],
                         [r["messages_sha256"] for r in actual])
        self.assertEqual(len(actual), 3)
        self.assertEqual(self.claim.attempts, 0)
        self.assertEqual(report["budget_scope"], self.adapter.BUDGET_SCOPE)
        self.assertFalse(report["external_host_lease_released_by_worker"])
        self.assertEqual(report["backend_close"]["status"], "closed")
        self.assertTrue(report["author_owner"]["scope_resources"][0]["resources_confirmed"])
        self.assertEqual([e for e in self.events if e in ("result_ready", "evaluation_start", "evaluation_end",
            "query_cleanup_end", "owner_closed")],
            ["result_ready", "evaluation_start", "evaluation_end", "query_cleanup_end", "owner_closed"])
        self.popen.assert_called_once()
        for row in self.carriers[0].submitted:
            payload = json.loads(json.loads(row["record_json"])["payload_json"])
            self.assertEqual(set(payload), {"reviewText"})

    def test_failed_and_unknown_calls_keep_known_usage_without_quality(self):
        self.mode = "partial"
        self.resources = replace(self.resources, prepare_batch_rows=3, model_batch_rows=3)
        with self.assertRaises(RuntimeError) as failure:
            self.run_query()
        report = failure.exception.query_report
        self.assertEqual([report["calls"]["counts"][k] for k in ("succeeded", "failed", "unknown")], [1, 1, 1])
        self.assertEqual(report["calls"]["usage"]["known_prompt_tokens"], 10)
        self.assertIsNone(report["calls"]["usage"]["total_tokens"])
        self.assertNotIn("quality", report)
        self.assertNotIn("result_ready", self.events)
        self.assertEqual(report["backend_close"]["status"], "closed")

    def test_close_api_failure_keeps_successful_calls_and_result_but_fails_query(self):
        self.close_ok = False
        with self.assertRaises(RuntimeError) as failure:
            self.run_query()
        report = failure.exception.query_report
        self.assertEqual(report["calls"]["counts"]["succeeded"], 3)
        self.assertEqual(report["calls"]["usage"]["total_tokens"], 21)
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["backend_close"]["status"], "unknown")
        self.assertNotIn("owner_closed", self.events)

    def test_changed_actual_messages_are_rejected_and_accounting_is_preserved(self):
        self.mode = "bad-digest"
        with self.assertRaises((RuntimeError, ValueError)) as failure:
            self.run_query()
        report = failure.exception.query_report
        self.assertEqual(report["status"], "failed")
        self.assertNotIn("quality", report)
        self.assertEqual(report["calls"]["counts"]["recorded"], 3)

    def test_final_exit_failure_preserves_primary_error_and_call_ledger(self):
        self.mode, self.gpu_confirmed = "partial", False
        self.resources = replace(self.resources, prepare_batch_rows=3, model_batch_rows=3)
        with self.assertRaises(RuntimeError) as failure:
            self.run_query()
        self.assertIn("Filter execution", str(failure.exception))
        self.assertTrue(any("physical exit" in note for note in failure.exception.__notes__))
        self.assertEqual(failure.exception.query_report["calls"]["usage"]["known_prompt_tokens"], 10)
        self.assertNotIn("owner_closed", self.events)

    def test_scope_startup_failure_keeps_unknown_calls_and_closes_the_started_scope(self):
        primary = OSError("explicit bridge startup fixture failure")
        self.popen.side_effect = primary
        with self.assertRaises(OSError) as failure:
            self.run_query()
        self.assertIs(failure.exception, primary)
        report = primary.query_report
        self.assertEqual(report["calls"]["counts"]["unknown"], 3)
        self.assertIsNone(report["calls"]["usage"]["total_tokens"])
        self.assertTrue(report["author_owner"]["scope_resources"][0]["resources_confirmed"])
        self.assertNotIn("owner_closed", self.events)

    def test_author_cleanup_failure_keeps_the_complete_call_ledger(self):
        self.cleanup_error = RuntimeError("explicit author cleanup fixture failure")
        with self.assertRaisesRegex(RuntimeError, "author cleanup") as failure:
            self.run_query()
        report = failure.exception.query_report
        self.assertEqual(report["calls"]["counts"]["succeeded"], 3)
        self.assertEqual(report["calls"]["usage"]["total_tokens"], 21)
        self.assertEqual(report["status"], "failed")
        self.assertNotIn("owner_closed", self.events)

    def test_failed_observer_cannot_erase_actual_calls_or_skip_cleanup(self):
        primary = OSError("explicit clock fixture failure")

        def failed_clock(event):
            self.events.append(event)
            if event == "query_cleanup_end":
                raise primary

        with self.assertRaises(OSError) as failure:
            self.run_query(observer=failed_clock)
        report = failure.exception.query_report
        self.assertIs(failure.exception, primary)
        self.assertEqual(report["calls"]["counts"]["succeeded"], 3)
        self.assertEqual(report["calls"]["usage"]["total_tokens"], 21)
        self.assertEqual(report["backend_close"]["status"], "closed")
        self.assertEqual(report["observer_errors"][0]["event"], "query_cleanup_end")
        self.assertIn("owner_closed", self.events)

    def test_preclaimed_unit_cap_and_profile_fail_before_any_owner_launch(self):
        for options in (dict(engine=replace(self.engine, max_model_calls=4)), dict(profile={})):
            with self.subTest(options=options), self.assertRaises(ValueError) as failure:
                self.run_query(**options)
            self.assertEqual(failure.exception.query_report["status"], "failed")
        self.context["budget"]["unit_id"] = "other"
        with self.assertRaises(ValueError):
            self.run_query()
        self.popen.assert_not_called()
        self.guard_factory.assert_not_called()

    def test_external_lease_change_and_reused_output_are_rejected_before_owner(self):
        self.context["owner_context"]["lease_sha256"] = "0" * 64
        with self.assertRaisesRegex(RuntimeError, "external host lease changed"):
            self.run_query()
        (self.output / "imlane-owner").mkdir()
        with self.assertRaisesRegex(ValueError, "fresh private"):
            self.run_query()
        self.popen.assert_not_called()

    def test_same_claim_cannot_start_another_engine_after_success_or_failure(self):
        for index, mode in enumerate(("normal", "partial")):
            with self.subTest(mode=mode):
                self.mode = mode
                self.resources = replace(self.resources, prepare_batch_rows=3, model_batch_rows=3)
                query = "repeat-" + str(index)
                self.ledger.reserve_unit(query, 3)
                claim = self.ledger.claim_unit(query)
                context = dict(self.context, claimed_budget=claim,
                               budget=dict(self.context["budget"], unit_id=query))
                for attempt in range(2):
                    output = self.root / (query + "-" + str(attempt))
                    output.mkdir()
                    context["output_dir"] = output
                    # The fixture reads the actual new worker path.
                    self.output = output
                    if attempt == 0 and mode == "normal":
                        report = self.adapter.run(self.source, CHECKOUT, self.method, self.generation,
                            self.engine, self.resources, query_id=query, profile=asdict(self.resources),
                            context=context)
                        self.assertEqual(report["status"], "completed")
                    else:
                        with self.assertRaisesRegex((RuntimeError, ValueError),
                                "Filter execution" if attempt == 0 else "retries are forbidden"):
                            self.adapter.run(self.source, CHECKOUT, self.method, self.generation,
                                self.engine, self.resources, query_id=query, profile=asdict(self.resources),
                                context=context)
                self.assertEqual(claim.attempts, 0)
        self.assertEqual(self.popen.call_count, 2)


if __name__ == "__main__":
    unittest.main()
