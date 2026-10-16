"""Actual pinned PZ and SDK; fixture execution is explicitly not vendor/model qualification."""

import io
import gc
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch
import weakref

from src.baselines.text.frameworks.native_calls import NativeCall, NativeCallOutcome
from src.execution_provider.adapters.full_response import FullModelResponse
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.experiments.palimpzest.ledger import PalimpzestLedger
from src.experiments.palimpzest.model import LocalModelBinding, bind_local_completion, verify_model_metadata
from src.semantic_methods.palimpzest.backend import PalimpzestAdapterError
from src.semantic_methods.palimpzest.query_execution import (
    NativeAdaptedExecutor, PreparedExecutionStrategy, PreparedRequestRoute, QueryExecutionLimits, freeze_existing_plan,
    validate_frozen_plan, validate_prepared_plan,
)


def completion_body(model, answer="TRUE"):
    return dict(id="fixture", object="chat.completion", created=1, model=model,
        choices=[dict(index=0, finish_reason="stop", message=dict(role="assistant", content="ANSWER: "+answer+"\n---"))],
        usage=dict(prompt_tokens=11, completion_tokens=3, total_tokens=14))


class FixtureExecutor:
    max_batch_rows, max_batch_bytes = 16, 16 * 1024**2

    def __init__(self, model, *, answer="TRUE", status=200, response_model=None, delay=0):
        self.model, self.answer, self.status = model, answer, status
        self.response_model, self.delay = response_model or model, delay
        self.batches, self.timeline = [], []
        self.active = self.peak_active = 0
        self.last_close_report = None

    def iter_batch(self, calls, *, cancelled=None, timeout_s=120):
        self.active += 1
        self.peak_active = max(self.peak_active, self.active)
        self.batches.append(calls)
        received = 0
        try:
            for index, call in enumerate(reversed(calls)):
                if index and self.delay:
                    time.sleep(self.delay)
                values = json.loads(call.payload)
                self.timeline.append(("response", call.call_id))
                answer = self.answer(values) if callable(self.answer) else self.answer
                body = completion_body(self.response_model, answer) if self.status == 200 else {"error": {"message": "fixture 503"}}
                response = FullModelResponse(self.status, (("content-type", "application/json"),), json.dumps(body).encode())
                received += 1
                yield NativeCallOutcome(call.call_id, call.row_id, response)
        finally:
            self.active -= 1
            self.last_close_report = dict(clean=received == len(calls), unconfirmed_work=False,
                                          prepared=len(calls), received=received)


class PreparedLimitsTests(unittest.TestCase):
    def test_preparation_memory_is_bounded_and_independent_of_native_workers(self):
        self.assertEqual(QueryExecutionLimits(2, 3, 2).max_pending_calls, 3)
        with self.assertRaises(ValueError):
            QueryExecutionLimits(2, 2, 3)
        with self.assertRaises(ValueError):
            QueryExecutionLimits(2, 2, 2, max_retained_response_bytes=1)


@unittest.skipUnless(os.environ.get("SEMLOOM_PALIMPZEST_TEST") == "1", "requires actual pinned PZ/SDK/greenlet; no model calls")
class PreparedQueryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import litellm
        import palimpzest as pz
        from palimpzest.query.processor.query_processor_factory import QueryProcessorFactory
        cls.pz, cls.litellm, cls.factory = pz, litellm, QueryProcessorFactory

    def setUp(self):
        self.model = FixedModelConfig("http://127.0.0.1:8000/v1/chat/completions", "fixture-local-model", 1000)
        self.binding = LocalModelBinding(self.model)
        verified = verify_model_metadata(self.binding, dict(object="list", data=[
            dict(id=self.model.model_id, object="model", owned_by="vllm")]))
        self.local = bind_local_completion(self.litellm.completion, self.binding, verified)

    def query(self, rows=6, *, convert=False):
        source = self.pz.MemoryDataset("prepared-fixture", [dict(movieId="taken_3", text="fixture review "+str(i)) for i in range(rows)])
        result = source.filter(lambda row: row["movieId"] == "taken_3").sem_filter("the review is positive", depends_on=["text"])
        if convert:
            result = result.sem_add_columns([dict(name="rank", desc="extract the rank", type=str)], depends_on=["text"])
        return result.count()

    def processor(self, query, *, workers=2, batch_size=None):
        config = self.pz.QueryProcessorConfig(execution_strategy="parallel", optimizer_strategy="none",
            available_models=[self.pz.Model.VLLM_QWEN_1_5_0_5B_CHAT], max_workers=workers, join_parallelism=workers,
            batch_size=batch_size, progress=False, use_vertex=False, api_base=self.binding.api_base,
            allow_bonded_query=True, allow_model_selection=False, allow_rag_reduction=False,
            allow_mixtures=False, allow_critic=False, allow_split_merge=False)
        return self.factory.create_processor(query, config)

    def strategy(self, processor, executor, *, record=None, observer=None, limits=None):
        return PreparedExecutionStrategy(processor.execution_strategy, self.model,
            original_completion=self.local.original, request_kwargs=self.local.request_kwargs,
            validate_response=self.local.validate_response, executor=executor,
            limits=limits or QueryExecutionLimits(processor.execution_strategy.max_workers,
                processor.execution_strategy.max_workers, processor.execution_strategy.max_workers),
            query_id="query-fixture", observer=observer, record=record)

    def test_finish_failure_still_closes_idle_sdk_and_retains_first_error_and_report(self):
        strategy = self.strategy(self.processor(self.query()), FixtureExecutor(self.model.model_id))
        first, later = ValueError('finish fixture'), RuntimeError('SDK close fixture')
        strategy.progress = SimpleNamespace(finite_batches=False, active=False,
            finish=Mock(side_effect=first), last_close_report=None)
        strategy._sdk = Mock()
        strategy._sdk.close.side_effect = later
        with self.assertRaises(ValueError) as caught:
            strategy.close(method_complete=True)
        self.assertIs(caught.exception, first)
        strategy._sdk.close.assert_called_once()
        self.assertFalse(strategy.last_close_report['closed'])
        self.assertEqual(strategy.last_close_report['cleanup_errors'], ['ValueError', 'RuntimeError'])
        self.assertIn('PZ prepared cleanup also failed: RuntimeError', first.__notes__)
        strategy._sdk.close.side_effect = None
        with self.assertRaises(ValueError) as caught:
            strategy.close()
        self.assertIs(caught.exception, first)
        self.assertTrue(strategy.last_close_report['closed'])
        self.assertTrue(strategy.last_close_report['sdk_closed'])

    def test_active_reader_retains_sdk_owner_after_finish_failure_until_reader_stops(self):
        strategy = self.strategy(self.processor(self.query()), FixtureExecutor(self.model.model_id))
        first = ValueError('active finish fixture')
        strategy.progress = SimpleNamespace(finite_batches=False, active=True,
            finish=Mock(side_effect=first), last_close_report={'unconfirmed_work': True})
        strategy._sdk = Mock()
        with self.assertRaises(ValueError) as caught:
            strategy.close()
        self.assertIs(caught.exception, first)
        strategy._sdk.close.assert_not_called()
        self.assertTrue(strategy.last_close_report['sdk_close_deferred'])
        self.assertTrue(strategy.last_close_report['unconfirmed_work'])
        self.assertFalse(strategy.last_close_report['closed'])
        strategy.progress.active = False
        strategy.progress.finish.side_effect = None
        strategy.progress.finish.return_value = {'unconfirmed_work': False}
        with self.assertRaises(ValueError):
            strategy.close()
        strategy._sdk.close.assert_called_once()
        self.assertTrue(strategy.last_close_report['closed'])
        self.assertTrue(strategy.last_close_report['unconfirmed_work'])

    def test_prior_contract_error_keeps_priority_when_finish_fails_but_sdk_closes(self):
        strategy = self.strategy(self.processor(self.query()), FixtureExecutor(self.model.model_id))
        primary = PalimpzestAdapterError('earlier contract fixture')
        strategy.fail(primary)
        strategy.progress = SimpleNamespace(finite_batches=False, active=False,
            finish=Mock(side_effect=ValueError('finish fixture')), last_close_report=None)
        strategy._sdk = Mock()
        with self.assertRaises(PalimpzestAdapterError) as caught:
            strategy.close()
        self.assertIs(caught.exception, primary)
        strategy._sdk.close.assert_called_once()
        self.assertTrue(strategy.last_close_report['closed'])

    def test_error_observed_during_reader_drain_precedes_later_finish_failure(self):
        strategy = self.strategy(self.processor(self.query()), FixtureExecutor(self.model.model_id))
        first = PalimpzestAdapterError('earlier reader outcome fixture')
        strategy.progress = SimpleNamespace(finite_batches=True, active=True,
            finish=Mock(side_effect=ValueError('later finish fixture')), last_close_report=None)
        strategy._sdk = Mock()
        def drain(timeout):
            strategy.fail(first)
            strategy.progress.active = False
        with patch.object(strategy, '_event', side_effect=drain), self.assertRaises(PalimpzestAdapterError) as caught:
            strategy.close()
        self.assertIs(caught.exception, first)
        strategy._sdk.close.assert_called_once()
        self.assertTrue(strategy.last_close_report['closed'])

    def test_actual_completed_plan_finish_failure_still_closes_original_sdk(self):
        from src.semantic_methods.palimpzest.prepared_progress import FiniteBatchProgress
        from src.semantic_methods.palimpzest.prepared_sdk import PreparedSDK
        first, closes = ValueError('finish after reader stopped fixture'), []
        class FailingFinish(FiniteBatchProgress):
            def finish(self, **kwargs):
                super().finish(**kwargs)
                raise first
        processor = self.processor(self.query(rows=2))
        executor = FixtureExecutor(self.model.model_id)
        strategy = self.strategy(processor, executor)
        strategy.progress = FailingFinish(executor)
        original_close = PreparedSDK.close
        def close(sdk):
            closes.append(sdk)
            original_close(sdk)
        with patch.object(PreparedSDK, 'close', close), self.assertRaises(ValueError) as caught:
            strategy.execute_plan(processor.optimizer.optimize(processor.dataset)[0])
        self.assertIs(caught.exception, first)
        self.assertEqual(len(closes), 1)
        self.assertTrue(strategy.last_close_report['closed'])
        self.assertEqual(strategy.last_close_report['active_stacks'], 0)

    def test_finished_reader_does_not_close_sdk_while_native_sender_future_is_running(self):
        executor = NativeAdaptedExecutor(max_workers=1)
        strategy = self.strategy(self.processor(self.query()), executor,
            limits=QueryExecutionLimits(2, 3, 2, close_timeout_s=0.5))
        strategy._sdk = Mock()
        entered, release = threading.Event(), threading.Event()
        def sender(*args, **kwargs):
            entered.set()
            release.wait(3)
            raise RuntimeError('held original SDK sender fixture')
        call = NativeCall('held-sdk', 0, b'{}', self.model.endpoint_url, (), 1)
        executor.bind_sdk_request(call.call_id, object(), sender)
        strategy.progress.start((call,), identities={call.call_id: {}}, cancelled=lambda: False, timeout_s=0.01)
        try:
            self.assertTrue(entered.wait(1))
            with self.assertRaises(PalimpzestAdapterError):
                strategy.close()
            self.assertFalse(strategy.progress.active)
            self.assertTrue(executor.sdk_in_use)
            strategy._sdk.close.assert_not_called()
            self.assertFalse(strategy.last_close_report['closed'])
            future = executor._sender_futures[0]
            release.set()
            with self.assertRaisesRegex(RuntimeError, 'held original SDK sender'):
                future.result(timeout=1)
            with self.assertRaises(PalimpzestAdapterError):
                strategy.close()
            strategy._sdk.close.assert_called_once()
            self.assertTrue(strategy.last_close_report['closed'])
        finally:
            release.set()
            executor.close()

    def test_original_dag_code_and_sdk_complete_all_rows_with_bounded_reversed_results(self):
        from palimpzest.query.execution import parallel_execution_strategy as module
        from palimpzest.query.generators import generators
        processor = self.processor(self.query())
        plan = processor.optimizer.optimize(processor.dataset)[0]
        frozen = freeze_existing_plan(plan)
        executor, ledger, events = FixtureExecutor(self.model.model_id), PalimpzestLedger(max_calls=6), []
        strategy = self.strategy(processor, executor, record=events.append, observer=ledger)
        original_pool, original_generator, original_llm = module.ThreadPoolExecutor, generators.Generator.__call__, generators.litellm
        with patch.object(module, "ThreadPoolExecutor", side_effect=AssertionError("original PZ pool created")):
            records, stats = strategy.execute_plan(plan)
        self.assertEqual(records[0].count, 6)
        self.assertEqual(stats.plan_id, frozen.plan_id)
        self.assertIs(module.ThreadPoolExecutor, original_pool)
        self.assertIs(generators.Generator.__call__, original_generator)
        self.assertIs(generators.litellm, original_llm)
        self.assertIs(validate_frozen_plan(frozen), plan)
        snapshot = strategy.snapshot()
        self.assertEqual(snapshot["prepared_calls"], 6)
        self.assertEqual(snapshot["outcomes_received"], 6)
        self.assertEqual(snapshot["active_stacks"], 0)
        self.assertLessEqual(snapshot["peak_active_stacks"], 2)
        self.assertEqual(executor.peak_active, 1)
        self.assertTrue(any(len(batch) == 2 for batch in executor.batches))
        self.assertTrue(all(not call.follow_redirects for batch in executor.batches for call in batch))
        self.assertEqual(ledger.snapshot()["total_prompt_tokens"], 66)
        identities = [event for event in events if event["event"] == "pz_generation_identity"]
        self.assertEqual(len({event["call_id"] for event in identities}), 6)
        self.assertEqual({tuple(event["identity"]["source_indices"]) for event in identities},
                         {(f"prepared-fixture-{i}",) for i in range(6)})
        finished = [event for event in events if event["event"] == "pz_operator_finished"]
        self.assertEqual(len(finished), 6)
        self.assertTrue(all(len(event["output_children"]) == 1 for event in finished))

    def test_all_false_preserves_old_parallel_empty_count_relation(self):
        processor = self.processor(self.query(rows=2))
        executor = FixtureExecutor(self.model.model_id, answer="FALSE")
        strategy = self.strategy(processor, executor)
        records, _ = strategy.execute_plan(processor.optimizer.optimize(processor.dataset)[0])
        self.assertEqual(records, [])
        self.assertEqual(strategy.snapshot()["prepared_calls"], 2)

    def test_single_field_bonded_convert_preserves_original_records_and_native_method_stages(self):
        from palimpzest.query.operators.convert import LLMConvertBonded
        processor = self.processor(self.query(rows=4, convert=True), batch_size=2)
        plan = processor.optimizer.optimize(processor.dataset)[0]
        self.assertTrue(any(type(op) is LLMConvertBonded for op in plan))
        def answer(values):
            text = str(values["messages"])
            return '{"rank": "fixture-rank"}' if 'extract the rank' in text else "TRUE"
        executor, events = FixtureExecutor(self.model.model_id, answer=answer), []
        strategy = self.strategy(processor, executor, record=events.append)
        records, _ = strategy.execute_plan(plan)
        self.assertEqual(records[0].count, 4)
        self.assertEqual(strategy.snapshot()["prepared_calls"], 8)
        operators = {event["identity"]["physical_operator"] for event in events if event["event"] == "pz_generation_identity"}
        self.assertEqual(len(operators), 2)
        self.assertTrue(all(len(batch) <= 2 for batch in executor.batches))

    def test_wrong_response_model_is_shared_fatal_and_prevents_later_preparation(self):
        processor = self.processor(self.query(rows=2), workers=1)
        executor, ledger = FixtureExecutor(self.model.model_id, response_model="wrong"), PalimpzestLedger(max_calls=2)
        strategy = self.strategy(processor, executor, observer=ledger)
        with patch("sys.stdout", new_callable=io.StringIO), self.assertRaises(PalimpzestAdapterError):
            strategy.execute_plan(processor.optimizer.optimize(processor.dataset)[0])
        self.assertEqual(strategy.snapshot()["prepared_calls"], 1)
        self.assertEqual(strategy.snapshot()["outcomes_received"], 1)
        self.assertEqual(ledger.snapshot()["completion_failed"], 1)
        self.assertTrue(strategy.snapshot()["iterator_reader_stopped"])

    def test_provider_503_keeps_original_false_and_allows_the_next_row(self):
        processor = self.processor(self.query(rows=2), workers=1)
        executor, ledger = FixtureExecutor(self.model.model_id, status=503), PalimpzestLedger(max_calls=2)
        strategy = self.strategy(processor, executor, observer=ledger)
        with patch("sys.stdout", new_callable=io.StringIO):
            records, _ = strategy.execute_plan(processor.optimizer.optimize(processor.dataset)[0])
        self.assertEqual(records, [])
        self.assertEqual(strategy.snapshot()["prepared_calls"], 2)
        self.assertIsNone(strategy.snapshot()["fatal_error_type"])
        self.assertEqual(ledger.snapshot()["completion_failed"], 2)

    def test_limit_and_multifield_convert_fail_before_preparing_any_model_call(self):
        for query in (self.query(rows=2).limit(1), self.pz.MemoryDataset("multi", [{"text": "fixture"}]).sem_add_columns(
                [dict(name="a", desc="a", type=str), dict(name="b", desc="b", type=str)])):
            processor = self.processor(query)
            plan = processor.optimizer.optimize(processor.dataset)[0]
            with self.assertRaises(PalimpzestAdapterError):
                validate_prepared_plan(plan)

    def test_parser_failure_retains_native_default_and_error_file(self):
        processor = self.processor(self.query(rows=1), workers=1)
        executor, ledger = FixtureExecutor(self.model.model_id, answer="unparseable fixture"), PalimpzestLedger(max_calls=1)
        strategy = self.strategy(processor, executor, observer=ledger)
        previous = Path.cwd()
        with tempfile.TemporaryDirectory() as directory:
            try:
                os.chdir(directory)
                records, _ = strategy.execute_plan(processor.optimizer.optimize(processor.dataset)[0])
                self.assertEqual(len(list(Path("parse-answer-errors").iterdir())), 1)
            finally:
                os.chdir(previous)
        self.assertEqual(records, [])
        self.assertEqual(ledger.snapshot()["parser_default_retained"], 1)

    def test_preparation_slots_can_exceed_original_pz_workers_without_setting_backend_concurrency(self):
        processor = self.processor(self.query(rows=3), workers=1)
        executor = FixtureExecutor(self.model.model_id)
        strategy = self.strategy(processor, executor, limits=QueryExecutionLimits(1, 3, 3))
        records, _ = strategy.execute_plan(processor.optimizer.optimize(processor.dataset)[0])
        self.assertEqual(records[0].count, 3)
        self.assertEqual(strategy.snapshot()["peak_active_stacks"], 3)
        self.assertEqual([len(batch) for batch in executor.batches], [3])
        self.assertEqual(strategy.snapshot()["native_pz_workers_identity"], 1)

    def test_held_terminal_futures_release_prepared_calls_stacks_and_operator_closures(self):
        class BrokenFixture(FixtureExecutor):
            def iter_batch(self, calls, **kwargs):
                yield from super().iter_batch(calls[:1], **kwargs)
                self.last_close_report = dict(clean=False, unconfirmed_work=True)
        for cancelled in (False, True):
            with self.subTest(cancelled=cancelled):
                processor = self.processor(self.query(rows=4))
                executor = (BrokenFixture if cancelled else FixtureExecutor)(self.model.model_id)
                strategy = self.strategy(processor, executor)
                held, calls, stacks = [], [], []
                submit, prepared = strategy.submit, strategy.prepared
                def hold(operator, argument):
                    future = submit(operator, argument)
                    held.append(future)
                    return future
                def capture(call, *args):
                    calls.append(weakref.ref(call))
                    stacks.append(weakref.ref(strategy.current_invocation.get().stack))
                    return prepared(call, *args)
                strategy.submit, strategy.prepared = hold, capture
                plan = processor.optimizer.optimize(processor.dataset)[0]
                if cancelled:
                    with self.assertRaises(PalimpzestAdapterError):
                        strategy.execute_plan(plan)
                else:
                    records, _ = strategy.execute_plan(plan)
                    self.assertEqual(records[0].count, 4)
                executor.batches.clear()
                gc.collect()
                terminal = [future for future in held if future.call_id is not None]
                self.assertTrue(terminal)
                self.assertTrue(all(future.done() for future in terminal))
                self.assertTrue(all(future.call is None and future.stack is None
                                    and future.operator is None and future.argument is None for future in terminal))
                self.assertTrue(all(reference() is None for reference in calls))
                self.assertTrue(all(reference() is None for reference in stacks))
                self.assertEqual(strategy.snapshot()["retained_request_bytes"], 0)
                if cancelled:
                    self.assertTrue(strategy.snapshot()["unconfirmed_work"])

    def test_original_operator_finishes_before_the_finite_batch_eof(self):
        first_finished = threading.Event()
        model = self.model.model_id
        class IncrementalFixture(FixtureExecutor):
            def iter_batch(self, calls, **kwargs):
                self.batches.append(calls)
                for index, call in enumerate(calls):
                    if index and not first_finished.wait(2):
                        raise AssertionError("original operator waited for a whole-batch barrier")
                    response = FullModelResponse(200, (("content-type", "application/json"),),
                                                 json.dumps(completion_body(model)).encode())
                    yield NativeCallOutcome(call.call_id, call.row_id, response)
                self.last_close_report = dict(clean=True, unconfirmed_work=False)
        def record(event):
            if event["event"] == "pz_operator_finished":
                first_finished.set()
        processor = self.processor(self.query(rows=2))
        executor = IncrementalFixture(model)
        strategy = self.strategy(processor, executor, record=record)
        records, _ = strategy.execute_plan(processor.optimizer.optimize(processor.dataset)[0])
        self.assertEqual(records[0].count, 2)
        self.assertTrue(first_finished.is_set())

    def test_incomplete_iterator_preserves_completed_usage_and_unconfirmed_responsibility(self):
        model = self.model.model_id
        class BrokenFixture(FixtureExecutor):
            def iter_batch(self, calls, **kwargs):
                call = calls[0]
                response = FullModelResponse(200, (("content-type", "application/json"),),
                                             json.dumps(completion_body(model)).encode())
                try:
                    yield NativeCallOutcome(call.call_id, call.row_id, response)
                    raise ValueError("fixture incomplete iterator")
                finally:
                    self.last_close_report = dict(clean=False, unconfirmed_work=True)
        processor = self.processor(self.query(rows=2))
        executor, ledger = BrokenFixture(model), PalimpzestLedger(max_calls=2)
        strategy = self.strategy(processor, executor, observer=ledger)
        with self.assertRaises(PalimpzestAdapterError):
            strategy.execute_plan(processor.optimizer.optimize(processor.dataset)[0])
        self.assertEqual(ledger.snapshot()["completion_completed"], 2)
        self.assertEqual(ledger.snapshot()["known_prompt_tokens"], 11)
        self.assertEqual(strategy.snapshot()["outcomes_received"], 1)
        self.assertEqual(strategy.snapshot()["active_stacks"], 0)
        self.assertTrue(strategy.snapshot()["unconfirmed_work"])
        self.assertEqual(strategy.snapshot()["remote_http_stopped"], "unavailable")

    def test_complete_response_batch_with_unknown_close_report_forbids_the_next_batch(self):
        class UnknownClose(FixtureExecutor):
            def iter_batch(self, calls, **kwargs):
                try:
                    yield from super().iter_batch(calls, **kwargs)
                finally:
                    self.last_close_report = dict(clean=False, unconfirmed_work=True)
        processor = self.processor(self.query(rows=2), workers=1)
        executor, ledger = UnknownClose(self.model.model_id), PalimpzestLedger(max_calls=2)
        strategy = self.strategy(processor, executor, observer=ledger)
        with self.assertRaises(PalimpzestAdapterError):
            strategy.execute_plan(processor.optimizer.optimize(processor.dataset)[0])
        self.assertEqual(len(executor.batches), 1)
        self.assertEqual(sum(len(batch) for batch in executor.batches), 1)
        self.assertEqual(ledger.snapshot()["known_prompt_tokens"], 11)
        self.assertEqual(strategy.snapshot()["outcomes_received"], 1)
        self.assertTrue(strategy.snapshot()["unconfirmed_work"])

    def test_transport_error_keeps_original_default_then_stops_later_query_admission(self):
        class TransportFailure(FixtureExecutor):
            def iter_batch(self, calls, **kwargs):
                self.batches.append(calls)
                try:
                    call = calls[0]
                    yield NativeCallOutcome(call.call_id, call.row_id, None,
                                            transport_error="TimeoutError: fixture completion is unconfirmed")
                finally:
                    self.last_close_report = dict(clean=False, unconfirmed_work=True)
        processor = self.processor(self.query(rows=2), workers=1)
        executor, ledger = TransportFailure(self.model.model_id), PalimpzestLedger(max_calls=2)
        strategy = self.strategy(processor, executor, observer=ledger)
        with patch("sys.stdout", new_callable=io.StringIO), self.assertRaises(PalimpzestAdapterError):
            strategy.execute_plan(processor.optimizer.optimize(processor.dataset)[0])
        self.assertEqual(len(executor.batches), 1)
        self.assertEqual(strategy.snapshot()["prepared_calls"], 1)
        self.assertEqual(ledger.calls()[0]["native_passed_operator"], False)
        self.assertEqual(ledger.calls()[0]["completion_failed"], True)
        self.assertEqual(ledger.snapshot()["usage_status"], "unavailable")
        self.assertTrue(strategy.snapshot()["unconfirmed_work"])

    def test_unknown_only_at_progress_finish_rejects_completed_query_and_preserves_usage(self):
        from src.semantic_methods.palimpzest.prepared_progress import FiniteBatchProgress
        class UnknownFinish(FiniteBatchProgress):
            def finish(self, **kwargs):
                return dict(super().finish(**kwargs), unconfirmed_work=True)
        processor = self.processor(self.query(rows=1), workers=1)
        executor, ledger = FixtureExecutor(self.model.model_id), PalimpzestLedger(max_calls=1)
        strategy = PreparedExecutionStrategy(processor.execution_strategy, self.model,
            original_completion=self.local.original, request_kwargs=self.local.request_kwargs,
            validate_response=self.local.validate_response, executor=executor, progress=UnknownFinish(executor),
            limits=QueryExecutionLimits(1, 1, 1), query_id="query-fixture", observer=ledger)
        with self.assertRaises(PalimpzestAdapterError):
            strategy.execute_plan(processor.optimizer.optimize(processor.dataset)[0])
        self.assertEqual(ledger.snapshot()["completion_completed"], 1)
        self.assertEqual(ledger.snapshot()["total_prompt_tokens"], 11)
        self.assertTrue(strategy.snapshot()["unconfirmed_work"])
        self.assertEqual(strategy.snapshot()["fatal_error_type"], "PalimpzestAdapterError")

    def test_successful_response_without_sdk_send_is_rejected_as_a_bypass(self):
        processor = self.processor(self.query(rows=1), workers=1)
        executor, ledger = FixtureExecutor(self.model.model_id), PalimpzestLedger(max_calls=1)
        strategy = PreparedExecutionStrategy(processor.execution_strategy, self.model,
            original_completion=lambda **kwargs: self.litellm.ModelResponse(**completion_body(self.model.model_id)),
            request_kwargs=self.local.request_kwargs, validate_response=self.local.validate_response,
            executor=executor, limits=QueryExecutionLimits(1, 1, 1), query_id="query-fixture", observer=ledger)
        with patch("sys.stdout", new_callable=io.StringIO), self.assertRaises(PalimpzestAdapterError):
            strategy.execute_plan(processor.optimizer.optimize(processor.dataset)[0])
        self.assertEqual(strategy.snapshot()["prepared_calls"], 0)
        self.assertEqual(executor.batches, [])
        self.assertEqual(ledger.calls()[0]["completion_error_type"], "PalimpzestAdapterError")

    def test_nonredirect_profile_preserves_sdk_default_then_rejects_external_307(self):
        import httpx
        processor = self.processor(self.query(rows=2), workers=1)
        executor, ledger, sends = NativeAdaptedExecutor(max_workers=1), PalimpzestLedger(max_calls=2), []
        original_init = httpx.Client.__init__
        def transport(request):
            sends.append(str(request.url))
            return httpx.Response(307, headers={"Location": "https://undeclared.invalid/model"},
                                  stream=httpx.ByteStream(b""))
        def initialize(client, *args, **kwargs):
            if type(client).__name__ == "CaptureClient":
                kwargs["transport"] = httpx.MockTransport(transport)
            return original_init(client, *args, **kwargs)
        strategy = self.strategy(processor, executor, observer=ledger)
        try:
            with patch.object(httpx.Client, "__init__", initialize), patch("sys.stdout", new_callable=io.StringIO), \
                    self.assertRaises(PalimpzestAdapterError):
                strategy.execute_plan(processor.optimizer.optimize(processor.dataset)[0])
            self.assertEqual(sends, [self.model.endpoint_url])
            self.assertEqual(strategy.snapshot()["prepared_calls"], 1)
            self.assertFalse(strategy.snapshot()["unconfirmed_work"])
            self.assertEqual(ledger.calls()[0]["native_passed_operator"], False)
            self.assertTrue(ledger.calls()[0]["completion_failed"])
        finally:
            executor.close()

    def test_mainthread_progress_offers_downstream_llm_before_upstream_last_result(self):
        from palimpzest.query.operators.filter import LLMFilter
        processor = self.processor(self.query(rows=2, convert=True))
        plan = processor.optimizer.optimize(processor.dataset)[0]
        upstream = next(f"{index}-{op.get_full_op_id()}" for index, op in enumerate(plan) if type(op) is LLMFilter)
        model, control_thread = self.model.model_id, threading.get_ident()
        class MainThreadProgress:
            max_batch_rows, max_batch_bytes = 16, 16 * 1024**2
            finite_batches, accepts_ready = False, True
            def __init__(self):
                self.pending, self.timeline = {}, []
                self.upstream_responses = 0
                self.downstream_offered = False
                self.last_close_report = None
            @property
            def active(self):
                return bool(self.pending)
            def start(self, calls, *, identities, **kwargs):
                self.assert_thread()
                # Deliberately accept a prefix; adapter retains the ready suffix.
                call = calls[0]
                identity = identities[call.call_id]
                self.pending[call.call_id] = (call, identity)
                op = identity["physical_operator"]
                if op != upstream:
                    self.downstream_offered = True
                self.timeline.append(("offer", op))
                return 1
            def assert_thread(self):
                if threading.get_ident() != control_thread:
                    raise AssertionError("Core progress left query control thread")
            def poll(self, timeout_s):
                self.assert_thread()
                for call_id, (call, identity) in tuple(self.pending.items()):
                    op = identity["physical_operator"]
                    if op == upstream and self.upstream_responses and not self.downstream_offered:
                        continue
                    del self.pending[call_id]
                    if op == upstream:
                        self.upstream_responses += 1
                    self.timeline.append(("result", op))
                    answer = "TRUE" if op == upstream else '{"rank": "fixture-rank"}'
                    full = FullModelResponse(200, (("content-type", "application/json"),),
                                             json.dumps(completion_body(model, answer)).encode())
                    return "outcome", NativeCallOutcome(call.call_id, call.row_id, full)
                time.sleep(min(0.005, timeout_s))
                return None
            def finish(self, *, method_complete, timeout_s):
                self.assert_thread()
                self.last_close_report = dict(method_complete=method_complete, unconfirmed_work=bool(self.pending))
                return self.last_close_report
        progress = MainThreadProgress()
        strategy = PreparedExecutionStrategy(processor.execution_strategy, self.model,
            original_completion=self.local.original, request_kwargs=self.local.request_kwargs,
            validate_response=self.local.validate_response, executor=progress, progress=progress,
            limits=QueryExecutionLimits(2, 3, 3), query_id="query-fixture")
        records, _ = strategy.execute_plan(plan)
        self.assertEqual(records[0].count, 2)
        results = [index for index, event in enumerate(progress.timeline) if event == ("result", upstream)]
        downstream = next(index for index, event in enumerate(progress.timeline) if event[0] == "offer" and event[1] != upstream)
        self.assertLess(downstream, results[-1])
        self.assertTrue(progress.last_close_report["method_complete"])

    @unittest.skipUnless(os.environ.get("SEMLOOM_PALIMPZEST_HTTP_TEST") == "1", "explicit localhost HTTP fixture")
    def test_native_adapted_control_observes_only_actual_localhost_sdk_posts(self):
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        model = self.model.model_id
        actual_posts, before, after = [], [], []
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass
            def do_POST(self):
                actual_posts.append(json.loads(self.rfile.read(int(self.headers["content-length"]))))
                body = json.dumps(completion_body(model)).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        executor = NativeAdaptedExecutor(max_workers=1,
            before_send=lambda call, request: before.append(call.call_id) or call.call_id,
            after_send=lambda reservation, **values: after.append((reservation, values)))
        try:
            self.model = FixedModelConfig(f"http://127.0.0.1:{server.server_port}/v1/chat/completions", model, 1000)
            self.binding = LocalModelBinding(self.model)
            verified = verify_model_metadata(self.binding, dict(object="list", data=[dict(id=model, object="model", owned_by="vllm")]))
            self.local = bind_local_completion(self.litellm.completion, self.binding, verified)
            processor = self.processor(self.query(rows=2))
            strategy = self.strategy(processor, executor)
            records, _ = strategy.execute_plan(processor.optimizer.optimize(processor.dataset)[0])
            self.assertEqual(records[0].count, 2)
            self.assertEqual(len(actual_posts), 2)
            self.assertEqual(len(before), 2)
            self.assertEqual(len(after), 2)
            self.assertTrue(all(item[1]["response"].status_code == 200 and item[1]["error"] is None for item in after))
            self.assertEqual(executor.last_close_report["max_workers"], 1)
        finally:
            executor.close()
            server.shutdown()
            server.server_close()
            thread.join(1)

    @unittest.skipUnless(os.environ.get("SEMLOOM_PALIMPZEST_HTTP_TEST") == "1", "explicit localhost gateway fixture")
    def test_registered_gateway_is_the_actual_sdk_request_destination(self):
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        from src.observability.request_gateway import GatewayRoute, ObservationGateway
        model = self.model.model_id
        upstream_posts, sdk_posts = [], []
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass
            def do_POST(self):
                upstream_posts.append((self.path, json.loads(self.rfile.read(int(self.headers["content-length"])))))
                body = json.dumps(completion_body(model)).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        executor = NativeAdaptedExecutor(max_workers=1,
            before_send=lambda call, request: sdk_posts.append((call.endpoint, str(request.url))) or call.call_id,
            after_send=lambda reservation, **values: None)
        try:
            self.model = FixedModelConfig(f"http://127.0.0.1:{server.server_port}/v1/chat/completions", model, 1000)
            self.binding = LocalModelBinding(self.model)
            verified = verify_model_metadata(self.binding, dict(object="list", data=[dict(id=model, object="model", owned_by="vllm")]))
            self.local = bind_local_completion(self.litellm.completion, self.binding, verified)
            route = GatewayRoute("query-fixture", "model", self.model.endpoint_url)
            with tempfile.TemporaryDirectory() as directory:
                with ObservationGateway(routes=(route,), trace_path=Path(directory)/"gateway.jsonl", preserve_http=True) as gateway:
                    registered = PreparedRequestRoute.from_gateway(gateway, route)
                    with self.assertRaises(ValueError):
                        PreparedRequestRoute.from_gateway(gateway, GatewayRoute(route.job_id, route.endpoint_id,
                            "http://127.0.0.1:1/v1/chat/completions"))
                    def request_kwargs(values):
                        original = self.local.request_kwargs(values)
                        return dict(original, api_base=registered.effective_endpoint.removesuffix("/chat/completions"))
                    processor = self.processor(self.query(rows=1), workers=1)
                    strategy = PreparedExecutionStrategy(processor.execution_strategy, self.model,
                        original_completion=self.local.original, request_kwargs=request_kwargs,
                        validate_response=self.local.validate_response, executor=executor,
                        limits=QueryExecutionLimits(1, 1, 1), query_id="query-fixture", request_route=registered)
                    plan = processor.optimizer.optimize(processor.dataset)[0]
                    original_api_base = next(op.generator.api_base for op in plan if hasattr(op, "generator"))
                    records, _ = strategy.execute_plan(plan)
                    self.assertEqual(records[0].count, 1)
                    self.assertEqual(sdk_posts, [(registered.effective_endpoint, registered.effective_endpoint)])
                    self.assertEqual(len(upstream_posts), 1)
                    self.assertEqual(upstream_posts[0][0], "/v1/chat/completions")
                    self.assertEqual(original_api_base, self.binding.api_base)
                    self.assertEqual(strategy.snapshot()["request_route"]["canonical_endpoint"], self.model.endpoint_url)
        finally:
            executor.close()
            server.shutdown()
            server.server_close()
            thread.join(1)


if __name__ == "__main__":
    unittest.main()
