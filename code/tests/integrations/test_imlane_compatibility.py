"""Current method, independent cleanup window and interpreter-value contracts; CPU only."""

from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import os
import signal
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from integrations.imlane_records.filter_host import FilterResults
from integrations.imlane_records.filter_workload import Occurrences, input_record
from integrations.imlane_records.model_lifecycle import MODEL_CLOSE_GRACE_S, model_closed
from integrations.imlane_records.owner import AuthorScope, StopBudget
from tests.integrations import test_imlane_filter as fixtures


class CompatibilityTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.FilterTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)

    def test_worker_refuses_unbound_or_unprojected_columns_before_model_construction(self):
        from integrations.imlane_records.filter_udf import FilterWorker

        for columns in (None, ("paragraph",)):
            config = deepcopy(self.fixture.config)
            config["method"]["resolved_input_columns"] = columns
            with self.subTest(columns=columns), self.assertRaisesRegex(ValueError, "caller-bound"), \
                 patch.object(fixtures.FixedEngine, "__init__") as constructor:
                FilterWorker(config, prepare_type=fixtures.Prepare, engine_type=fixtures.FixedEngine)
            constructor.assert_not_called()

    def test_neutral_projection_preserves_only_declared_text_values(self):
        rows = [dict(fixtures.source(count=1)[0], paragraph="literal {}\n中文", topic="writing")]
        mapping = Occurrences("q", max_calls=1, columns=("topic", "paragraph"))
        encoded = mapping.encode(rows)
        self.assertEqual({k: input_record(encoded[0])[k] for k in mapping.columns},
                         {"topic": "writing", "paragraph": "literal {}\n中文"})
        self.assertNotIn("reviewText", encoded[0]["record_json"])
        self.assertNotIn("scoreSentiment", encoded[0]["record_json"])
        for column in ("row_id", "call_id", "query_id"):
            with self.subTest(column=column), self.assertRaises(ValueError):
                Occurrences("q", max_calls=1, columns=(column,))

    def test_successful_query_close_has_its_own_full_model_window(self):
        class Author(fixtures.LocalAuthor):
            def submit(self, block, rows, **kwargs):
                self.deadline = kwargs["deadline"]
                return super().submit(block, rows, **kwargs)

            def wait_ready(self, **kwargs):
                self.timeout_s = kwargs["timeout_s"]
                return super().wait_ready(**kwargs)

        client = Author(self.fixture.worker())
        values = FilterResults(fixtures.source(count=1), client, query_id="q", max_calls=8,
            columns=("reviewText",), control_dir=self.fixture.root.name, deadline=10**10,
            close_owner=lambda: {"resources_confirmed": True})
        list(values)
        values.deadline = 999  # Inference budget has ended; teardown is still owed.
        with patch("integrations.imlane_records.records_host.time.monotonic", return_value=1000):
            values.close()
        self.assertEqual(client.deadline, 1000 + MODEL_CLOSE_GRACE_S)
        self.assertEqual(client.timeout_s, MODEL_CLOSE_GRACE_S)
        self.assertTrue(values.closed)
        self.assertTrue(model_closed(values.model_close))
        self.assertIsNone(values.last_close_report["gpu_exit_confirmed"])

    def test_stop_signal_during_close_is_deferred_until_model_and_owner_return(self):
        budget = StopBudget(threading.Event())

        class Author(fixtures.LocalAuthor):
            def submit(self, block, rows, **kwargs):
                if block == "filter-close":
                    budget.signal(signal.SIGTERM, None)
                return super().submit(block, rows, **kwargs)

        finalized = []
        client = Author(self.fixture.worker())
        values = FilterResults(fixtures.source(count=1), client, query_id="q", max_calls=8,
            columns=("reviewText",), control_dir=self.fixture.root.name, deadline=10**10,
            cleanup_scope=budget.cleanup,
            close_owner=lambda: finalized.append(True) or {"resources_confirmed": True})
        list(values)
        values.close()
        self.assertTrue(budget.stopped.is_set())
        self.assertEqual(budget.cleanup_depth, 0)
        self.assertTrue(finalized)
        self.assertTrue(values.closed)
        self.assertEqual(client.aborts, 0)

    def test_process_exit_alone_cannot_close_a_started_model_and_retry_keeps_handle(self):
        class MissingAPI(fixtures.FixedEngine):
            def close(self):
                report = super().close()
                report.pop("engine_api")
                return report

        worker = self.fixture.worker(MissingAPI)
        encoded = Occurrences("q", max_calls=8, columns=("reviewText",)).encode(fixtures.source(count=1))
        worker.run(encoded)
        kernel = worker.kernel
        self.assertEqual(worker.close()["status"], "unknown")
        self.assertFalse(worker.closed)
        self.assertIs(worker.kernel, kernel)
        self.assertTrue(worker.broken)
        with patch.object(kernel, "close", return_value=fixtures.FixedEngine.close(kernel)):
            self.assertTrue(model_closed(worker.close()))
        self.assertTrue(worker.closed)
        for confirmed in (None, False, 1):
            report = fixtures.FixedEngine.close(kernel)
            report["engine_api"]["engine_api_shutdown_confirmed"] = confirmed
            self.assertFalse(model_closed(report))

    def test_filter_scope_stop_wait_does_not_reintroduce_the_echo_eight_second_limit(self):
        from integrations.imlane_records.verify_filter import FilterScope

        for cls, expected in ((AuthorScope, 8), (FilterScope, MODEL_CLOSE_GRACE_S)):
            scope = cls.__new__(cls)
            scope.roots, scope.signal_identity = {7: 11}, Mock()
            process, writer = Mock(pid=7), Mock()
            process.poll.return_value = None
            process.returncode = 0
            scope.stop_root(process, writer)
            process.wait.assert_called_once_with(timeout=expected)

    def test_method_preflight_checks_author_abi_and_the_selected_fixed_source_profile(self):
        from integrations.imlane_records import method_environment

        probe = dict(base="/cpu-fixture/python312", version=[3, 12], arrow="16.1.0")
        for chain, profile in ((False, "engine import validate_filter_source"),
                               (True, "record_chain import validate_chain_source")):
            with self.subTest(chain=chain), \
                 patch.object(method_environment.subprocess, "check_output", return_value=json.dumps(probe)) as abi, \
                 patch.object(method_environment.subprocess, "run") as source:
                result = method_environment.preflight_method("/method/bin/python", "/author", "/code", chain=chain)
            self.assertEqual(result, [probe, probe])
            self.assertEqual([call.args[0][0] for call in abi.call_args_list],
                             ["/author/venv/bin/python", "/method/bin/python"])
            self.assertTrue(all(call.args[0][1:4] == ["-I", "-B", "-c"] for call in abi.call_args_list))
            command = source.call_args.args[0]
            self.assertIn(profile, command[4])
            self.assertEqual(command[-1], "/code")
            self.assertEqual(source.call_args.kwargs, dict(check=True, capture_output=True, timeout=15))

    def test_chain_method_preflight_failure_never_enters_the_author_owner(self):
        from integrations.imlane_records import chain_host, method_environment

        scope = chain_host.ChainScope.__new__(chain_host.ChainScope)
        scope.args = SimpleNamespace(method_python="/method/bin/python", author_root=Path("/author"))
        author = dict(base="/cpu-fixture/python312", version=[3, 12], arrow="16.1.0")
        for changed in (dict(arrow="24.0.0"), dict(base="/other/python312"), dict(version=[3, 13])):
            with self.subTest(changed=changed), \
                 patch.object(method_environment.subprocess, "check_output",
                              side_effect=[json.dumps(author), json.dumps(dict(author, **changed))]), \
                 patch.object(method_environment.subprocess, "run") as source, \
                 patch.object(chain_host.verify.RecordScope, "__enter__") as enter, \
                 self.assertRaisesRegex(ValueError, "author's Python and Arrow ABI"):
                scope.__enter__()
            source.assert_not_called()
            enter.assert_not_called()
        with patch.object(chain_host, "preflight_method", side_effect=ValueError("fixed source differs")), \
             patch.object(chain_host.verify.RecordScope, "__enter__") as enter, \
             self.assertRaisesRegex(ValueError, "fixed source differs"):
            scope.__enter__()
        enter.assert_not_called()
        scope.args.method_python = None
        with patch.object(chain_host, "preflight_method") as preflight, \
             patch.object(chain_host.verify.RecordScope, "__enter__") as enter, \
             self.assertRaisesRegex(ValueError, "explicit isolated"):
            scope.__enter__()
        preflight.assert_not_called()
        enter.assert_not_called()

    def test_cleanup_window_rejects_bool_nonfinite_and_truncated_engine_budget(self):
        for timeout in (True, float("nan"), float("inf"), 0, 5, 15):
            with self.subTest(timeout=timeout), self.assertRaises(ValueError):
                FilterResults([], fixtures.LocalAuthor(self.fixture.worker()), query_id="q", max_calls=8,
                    columns=("reviewText",), control_dir=self.fixture.root.name, deadline=10**10,
                    close_timeout_s=timeout, close_owner=lambda: {"resources_confirmed": True})

    def test_only_one_engine_initialization_is_possible_including_concurrent_entry(self):
        from concurrent.futures import ThreadPoolExecutor

        attempts = []

        class SlowEngine(fixtures.FixedEngine):
            def __init__(self, *args):
                attempts.append(True)
                time.sleep(.03)
                super().__init__(*args)

        worker = self.fixture.worker(SlowEngine)
        with ThreadPoolExecutor(max_workers=2) as pool:
            engines = list(pool.map(lambda _: worker._kernel(), range(2)))
        self.assertIs(engines[0], engines[1])
        self.assertEqual(len(attempts), 1)
        worker.close()

        class Fails:
            def __init__(self, *_):
                attempts.append(False)
                raise OSError("self-written CPU constructor fixture")

        worker = self.fixture.worker(Fails)
        with self.assertRaises(OSError):
            worker._kernel()
        with self.assertRaisesRegex(RuntimeError, "cannot be retried"):
            worker._kernel()
        self.assertEqual(attempts, [True, False])

    def test_new_rss_allowance_is_explicit_and_old_echo_budget_is_unchanged(self):
        from integrations.imlane_records.resource_limits import owner_limits, check_resources

        old = owner_limits()
        self.assertEqual(old["rss_bytes"], 8 * 1024**3)
        value = dict(cpu_cores=8, rss_bytes=32 * 1024**3, shm_bytes=2 * 1024**3)
        limits = owner_limits(value)
        value["rss_bytes"] = 0
        self.assertEqual(limits["rss_bytes"], 32 * 1024**3)
        check_resources(limits["rss_bytes"], limits["shm_bytes"], limits)
        for rss, shm in ((limits["rss_bytes"] + 1, 0), (0, limits["shm_bytes"] + 1)):
            with self.subTest(rss=rss, shm=shm), self.assertRaisesRegex(RuntimeError, "allowance"):
                check_resources(rss, shm, limits)
        for number in (True, 0, -1, float("inf"), float("nan")):
            with self.subTest(number=number), self.assertRaises(ValueError):
                owner_limits(dict(limits, rss_bytes=number))
        from integrations.imlane_records import verify
        from types import SimpleNamespace
        scope = verify.RecordScope.__new__(verify.RecordScope)
        scope.args = SimpleNamespace(owner_resource_limits=limits, stop_budget=StopBudget(threading.Event()))
        scope.report, scope.namespace = {}, "self-written-cpu-allowance"
        scope.stop_sampler = Mock()
        scope.stop_sampler.wait.return_value = False
        scope.snapshot, scope.abort_owned = Mock(), Mock()
        scope.survivors = Mock(return_value=[(7, 11)])
        scope.helper = Mock()
        scope.helper.resources.return_value = {}
        process = Mock()
        process.memory_info.return_value = SimpleNamespace(rss=limits["rss_bytes"] + 1)
        with patch("psutil.Process", return_value=process), patch.object(verify.tools, "process_start_ticks", return_value=11):
            scope.sample()
        self.assertTrue(scope.report["resource_allowance_exceeded"])
        self.assertTrue(scope.args.stop_budget.stopped.is_set())
        scope.abort_owned.assert_called_once()
        scope.report, scope.abort_owned = {}, Mock()
        scope.args.owner_resource_limits = dict(limits, rss_bytes=True)
        scope.sample()
        self.assertIn("ValueError", scope.report["sampler_errors"][0])
        scope.abort_owned.assert_called_once()

    def test_fresh_cpu_child_clears_author_abi_and_uses_the_shared_code_path(self):
        from integrations.imlane_records.model_process import _AuthorModelEnvironment
        from tests.integrations.imlane_filter_fixture import FixedEngineProcess

        class AssignedCPU(_AuthorModelEnvironment, FixedEngineProcess):
            pass

        with patch.dict(os.environ, CUDA_VISIBLE_DEVICES="0", LD_LIBRARY_PATH="/author/arrow16",
                        PYTHONPATH="/author/physical-method-copy", VIRTUAL_ENV="/author/method-copy"):
            child = AssignedCPU(fixtures.EngineSpec(**self.fixture.config["engine"]), fixtures.GenerationSpec(),
                self.fixture.root.name, time.monotonic() + 30, 20,
                model_environment={"CUDA_VISIBLE_DEVICES": "0"})
            try:
                ready = child.ready_report
                self.assertEqual(Path(ready["pythonpath"]), Path(__file__).resolve().parents[2])
                self.assertEqual(set(ready["author_abi_environment"].values()), {None})
                self.assertFalse(ready["lotus_imported"])
                self.assertEqual(os.environ["LD_LIBRARY_PATH"], "/author/arrow16")
            finally:
                report = child.close()
            self.assertTrue(model_closed(report))
            self.assertIsNone(report["gpu_exit_confirmed"])

    def test_unknown_first_query_gpu_exit_prevents_a_second_query_owner(self):
        from integrations.imlane_records import verify_filter

        path, output = Path(self.fixture.root.name) / "run.json", Path(self.fixture.root.name) / "result"
        value = dict(mode="local-model", query_ids=["q1", "q2"], max_total_calls=4,
            owner_resource_limits=dict(cpu_cores=8, rss_bytes=32 * 1024**3, shm_bytes=2 * 1024**3),
            model_environment={"CUDA_VISIBLE_DEVICES": "0"},
            **{key: self.fixture.root.name for key in ("author_root", "method_python", "bridge_binary", "artifact_root")})
        path.write_text(json.dumps(value))
        lease = Mock(path=Path(self.fixture.root.name) / "lease")
        idle = dict(uuid="owned-gpu", memory_mib=1, compute_processes=[])

        def complete(_args, _value, report, _components, query):
            report["queries"].append(dict(query_id=query, status="completed"))

        engine = fixtures.EngineSpec(**dict(self.fixture.config["engine"], max_model_calls=2))
        with patch.object(verify_filter, "configuration", return_value=(None, engine, None, None, None)), \
             patch.object(verify_filter.verify, "_host_lease", return_value=lease), \
             patch.object(verify_filter.signal, "signal"), \
             patch.object(verify_filter, "run_one", side_effect=complete) as run_one, \
             patch.object(verify_filter, "gpu_snapshot", side_effect=[idle,
                 dict(idle, memory_mib=100, compute_processes=[["42", "owned-gpu", "99"]]), idle]):
            self.assertEqual(verify_filter.main(["--config", str(path), "--output", str(output)]), 1)
        self.assertEqual(run_one.call_count, 1)
        report = json.loads((output / "report.json").read_text())
        self.assertFalse(report["query_gpu_exit"][0]["gpu_exit_confirmed"])
        self.assertEqual(len(report["queries"]), 1)

    @unittest.skipUnless(importlib.util.find_spec("lotus"), "requires pinned LOTUS CPU environment")
    def test_bound_generic_source_original_formatter_and_parser_roundtrip_through_imlane(self):
        import csv
        import lotus
        import pandas as pd
        from integrations.imlane_records.filter_udf import FilterWorker
        from integrations.imlane_records.filter_workload import bind_source
        from src.semantic_methods.lotus.engine import LotusFilterMethod, engine_response, interpret_results, make_lm
        from src.semantic_methods.lotus.records import FilterMethod
        from src.serving.backends.vllm_engine import GenerationSpec
        from tests.semantic_methods.test_lotus_generic_filter import BoundTextFixtureSource

        path = Path(self.fixture.root.name) / "paragraphs.csv"
        with path.open("w", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(["row_id", "source_position", "category", "paragraph", "topic"])
            writer.writerows([["skip", 0, "skip", "outside", "outside"],
                              ["p1", 1, "keep", "Excellent writing", "writing"],
                              ["p2", 2, "keep", "", "writing"]])
        public = FilterMethod("openai/local-fixture", "Keep positive {paragraph} about {topic}", default=False)
        source = BoundTextFixtureSource(path, public)
        bound, scan = bind_source(source, public)
        self.assertIsNone(public.resolved_input_columns)
        config = dict(self.fixture.config, method=bound.describe(), scan=scan.describe())
        generation = GenerationSpec()
        with lotus.settings.context(enable_cache=False):
            worker = FilterWorker(config, prepare_type=LotusFilterMethod, engine_type=fixtures.FixedEngine)
            client = fixtures.LocalAuthor(worker)
            inputs = source.prepared_input("q")
            values = FilterResults(inputs, client, query_id="q", max_calls=8, columns=scan.payload_columns,
                control_dir=self.fixture.root.name, deadline=10**10,
                close_owner=lambda: {"resources_confirmed": True})
            results = list(values)
            actual_messages = [json.loads(row["messages_json"]) for row in worker.kernel.calls]
            lm = make_lm(public, generation)
            decisions = interpret_results(results, lm, public)
            native_lm, captured = make_lm(public, generation), []

            def respond(data, _options, *_):
                captured.extend(message for message, _ in data)
                return [engine_response(result, native_lm) for result in results]

            with patch.object(native_lm, "_process_uncached_messages", side_effect=respond), lotus.settings.context(lm=native_lm):
                native = pd.DataFrame([{name: row[name] for name in scan.payload_columns} for row in inputs]).sem_filter(
                    public.instruction, default=public.default, output_tokens=public.output_tokens)
            self.assertEqual(actual_messages, captured)
            self.assertEqual(len(native), sum(row["decision"] for row in decisions))
            self.assertEqual([row["call_id"] for row in results], [row["call_id"] for row in inputs])
            self.assertEqual(lm.stats.physical_usage.prompt_tokens, native_lm.stats.physical_usage.prompt_tokens)
            values.close()
            self.assertTrue(values.closed)


if __name__ == "__main__":
    unittest.main()
