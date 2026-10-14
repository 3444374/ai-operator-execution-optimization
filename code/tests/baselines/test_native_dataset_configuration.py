"""Native API argument and outcome checks using graph doubles, without HTTP.

These checks do not measure vendor scheduling or a global concurrency limit.
Actual Daft/Ray graph tests remain separately opt-in in test_native_calls.py.
"""
import asyncio
from contextlib import contextmanager
from dataclasses import replace
import inspect
import json
import os
from pathlib import Path
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

from src.baselines.text.frameworks import native_calls as native
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.execution_provider.adapters.full_response import FullModelResponse


def calls():
    return tuple(native.NativeCall(f"occurrence:{i}", 10+i, b'{"text":"repeated"}',
        "http://localhost:1/unused", (), 1) for i in range(3))


def outcome(call):
    if call.row_id == 12:
        return native.NativeCallOutcome(call.call_id, call.row_id, None, "ConnectionError: fixture")
    response = FullModelResponse(status_code=429 if call.row_id == 11 else 200, headers=(),
                                 body=("reply:"+call.call_id).encode())
    return native.NativeCallOutcome(call.call_id, call.row_id, response)


def graph_results(values):
    yield from values


@contextmanager
def daft_graph(records, *, version="0.7.21", runner="native"):
    class Series(list):
        def to_pylist(self):
            return list(self)

    def batch(**options):
        records["udf_options"] = options
        def decorate(function):
            records["udf"] = function
            return lambda column: (function, column)
        return decorate

    class Frame:
        def with_column(self, name, expression):
            records["expression"] = (name, expression[1])
            self.function = expression[0]
            return self

        def select(self, name):
            records["projection"] = name
            return self

        def iter_rows(self, **options):
            records["iterator_options"] = options
            for wire in reversed(records["input"]["native_call"]):
                yield {"native_outcome": asyncio.run(self.function(Series([wire])))[0]}

    daft, runners = ModuleType("daft"), ModuleType("daft.runners")
    daft.__version__ = version
    daft.func = SimpleNamespace(batch=batch)
    daft.DataType = SimpleNamespace(string=lambda: "string")
    daft.Series = SimpleNamespace(from_pylist=Series)
    daft.col = lambda column: column
    daft.from_pydict = lambda values: records.update(input=values) or Frame()
    runners.get_or_create_runner = lambda: SimpleNamespace(name=runner)
    with patch.dict("sys.modules", {"daft": daft, "daft.runners": runners}):
        yield


@contextmanager
def ray_graph(records, *, async_batches=2, version="2.56.1", initialized=True):
    names = ("ray", "ray.data", "ray.data.llm", "ray.data._internal",
             "ray.data._internal.planner", "ray.data._internal.planner.plan_udf_map_op")
    modules = {name: ModuleType(name) for name in names}
    ray, data, llm = (modules[name] for name in names[:3])
    ray.__version__, ray.data = version, data
    ray.is_initialized = lambda: initialized
    modules[names[-1]].DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY = async_batches

    def from_items(items, **options):
        records.update(input=items, source_options=options)
        return items

    class Processor:
        def __init__(self):
            self.stages = {name: SimpleNamespace(map_batches_kwargs={}) for name in ("pre", "http", "post")}
            records["stages"] = self.stages

        def __call__(self, dataset):
            records["dataset"] = dataset
            executor = SimpleNamespace(shutdown=Mock())
            records["executor"] = executor
            return SimpleNamespace(_current_executor=executor, iter_rows=lambda: iter(
                {"native_outcome": native._outcome_wire(outcome(native._call_from_wire(row["native_call"])))}
                for row in reversed(dataset)))

    def build(config, **options):
        records.update(config=vars(config), processor_options=options)
        return Processor()

    data.from_items = from_items
    llm.HttpRequestProcessorConfig = lambda **options: SimpleNamespace(**options)
    llm.build_processor = build
    with patch.dict("sys.modules", modules):
        yield


class DaftConfigurationTests(unittest.TestCase):
    def test_daft_keeps_its_slot_rule_without_using_unselected_ray_sizes(self):
        with self.assertRaisesRegex(ValueError, 'at least two request slots'):
            native.NativeCallExecutor('daft', NativeGraphOptions(concurrency=1))
        options = NativeGraphOptions(concurrency=2, ray_batch_rows=256)
        records = {}
        with daft_graph(records), patch.object(native, '_request', AsyncMock(side_effect=lambda call, control: outcome(call))):
            returned = tuple(native.NativeCallExecutor('daft', options).iter_batch(calls()))
        self.assertEqual(len(returned), 3)
        self.assertEqual(records['udf_options']['max_concurrency'], 1)

    def test_async_udf_arguments_follow_the_selected_options_and_preserve_outcomes(self):
        for concurrency in (4, 8):
            with self.subTest(concurrency=concurrency):
                records, source = {}, calls()
                request = AsyncMock(side_effect=lambda call, control: outcome(call))
                with daft_graph(records), patch.object(native, "_request", request):
                    values = tuple(native._daft_rows(source, NativeGraphOptions(concurrency=concurrency),
                                                    native._GraphControl()))
                self.assertTrue(inspect.iscoroutinefunction(records["udf"]))
                self.assertEqual(records["udf_options"], dict(return_dtype="string", use_process=False,
                    batch_size=1, max_concurrency=concurrency-1, max_retries=0, on_error="raise"))
                self.assertEqual(records["iterator_options"], {"results_buffer_size": 1})
                self.assertEqual(records["expression"], ("native_outcome", "native_call"))
                self.assertEqual(values, tuple(outcome(call) for call in reversed(source)))
                self.assertEqual([args.args[0] for args in request.await_args_list], list(reversed(source)))
                self.assertFalse(native._DAFT_CONTROLS)

    def test_multirow_udf_input_is_rejected_before_a_request(self):
        records = {}
        with daft_graph(records), patch.object(native, "_request", new_callable=AsyncMock) as request:
            tuple(native._daft_rows((), NativeGraphOptions(), native._GraphControl()))
            with self.assertRaisesRegex(ValueError, "one complete call per batch"):
                asyncio.run(records["udf"](["first", "second"]))
        request.assert_not_awaited()

    def test_wrong_version_or_runner_is_rejected_before_graph_creation(self):
        for settings in ({"version": "0.7.20"}, {"runner": "ray"}):
            with self.subTest(settings=settings), daft_graph(records := {}, **settings):
                with self.assertRaisesRegex(RuntimeError, "Daft 0.7.21 with its native runner"):
                    tuple(native._daft_rows(calls(), NativeGraphOptions(), native._GraphControl()))
                self.assertNotIn("input", records)

    def test_udf_failure_is_visible_and_not_retried(self):
        failure = ValueError("complete response exceeded its allowance")
        with daft_graph(records := {}), patch.object(native, "_request", AsyncMock(side_effect=failure)) as request:
            with self.assertRaises(ValueError) as observed:
                tuple(native._daft_rows(calls(), NativeGraphOptions(), native._GraphControl()))
        self.assertIs(observed.exception, failure)
        self.assertEqual(request.await_count, 1)
        self.assertFalse(native._DAFT_CONTROLS)


class RayConfigurationTests(unittest.TestCase):
    def test_ray_envelope_is_checked_before_graph_creation_and_c1_can_be_selected(self):
        with ray_graph(records := {}, async_batches=1):
            with self.assertRaisesRegex(ValueError, 'batch request envelope'):
                native.NativeCallExecutor('ray-data', NativeGraphOptions(concurrency=1))
            options = NativeGraphOptions(concurrency=1, ray_actors=1, ray_async_batches_per_actor=1)
            returned = tuple(native.NativeCallExecutor('ray-data', options).iter_batch(calls()))
        self.assertEqual(len(returned), 3)
        self.assertEqual(records['config']['concurrency'], (1, 1))


    def test_close_report_separates_rows_slots_and_the_imported_setting(self):
        for batch_rows in (1, 4):
            options = NativeGraphOptions(concurrency=16, ray_batch_rows=batch_rows)
            # Changing the environment after import cannot change Ray's constant.
            with self.subTest(batch_rows=batch_rows), ray_graph({}), \
                    patch.dict(os.environ, {'RAY_DATA_DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY': '4'}):
                executor = native.NativeCallExecutor('ray-data', options)
                tuple(executor.iter_batch(calls()))
            report = executor.last_close_report['native_graph_parameters']
            self.assertEqual(report['async_batches_setting'], 2)
            self.assertEqual(report['async_batches_effective'], 2)
            self.assertEqual(report['request_row_envelope'], 4 * batch_rows)
            self.assertEqual(report['active_http_upper_bound'], 4)

    def test_batch_actor_and_source_parameters_are_forwarded_separately(self):
        selections = (NativeGraphOptions(concurrency=9, ray_actors=1, ray_async_batches_per_actor=3,
                                        ray_batch_rows=3, source_blocks=2),
                      NativeGraphOptions(concurrency=20, ray_actors=2, ray_async_batches_per_actor=2,
                                        ray_batch_rows=5, source_blocks=4))
        for options in selections:
            with self.subTest(options=options):
                with ray_graph(records := {}, async_batches=options.ray_async_batches_per_actor):
                    values = tuple(native._ray_rows(calls(), options, native._GraphControl()))
                config = records["config"]
                self.assertEqual(config["batch_size"], options.ray_batch_rows)
                self.assertEqual(config["concurrency"], (options.ray_actors, options.ray_actors))
                self.assertEqual(config["max_retries"], 0)
                self.assertIs(config["session_factory"], native._ProcessorSession)
                self.assertEqual(records["source_options"], {"override_num_blocks": min(3, options.source_blocks)})
                self.assertIs(records["processor_options"]["preprocess"], native._ray_preprocess)
                self.assertIs(records["processor_options"]["postprocess"], native._ray_postprocess)
                self.assertEqual(records["processor_options"]["preprocess_map_kwargs"], {"max_retries": 0})
                self.assertEqual(records["processor_options"]["postprocess_map_kwargs"], {"max_retries": 0})
                for stage in records["stages"].values():
                    self.assertEqual(stage.map_batches_kwargs, dict(max_restarts=0, max_task_retries=0, max_concurrency=1))
                self.assertEqual(values, tuple(outcome(call) for call in reversed(calls())))

    def test_import_time_async_mismatch_fails_before_any_dataset_is_created(self):
        with ray_graph(records := {}, async_batches=4):
            with self.assertRaisesRegex(RuntimeError, "asynchronous batch limit differs"):
                executor = native.NativeCallExecutor('ray-data')
                tuple(executor.iter_batch(calls()))
        self.assertEqual(records, {})
        report = executor.last_close_report['native_graph_parameters']
        self.assertEqual(report['async_batches_effective'], 4)
        self.assertIsNone(report['active_http_upper_bound'])

    def test_wrong_version_or_unowned_runtime_is_rejected_before_graph_creation(self):
        for settings in ({"version": "2.55.0"}, {"initialized": False}):
            with self.subTest(settings=settings), ray_graph(records := {}, **settings):
                with self.assertRaisesRegex(RuntimeError, "caller-owned Ray 2.56.1"):
                    tuple(native._ray_rows(calls(), NativeGraphOptions(), native._GraphControl()))
                self.assertEqual(records, {})

    def test_successful_carrier_keeps_real_http_error_and_transport_failure(self):
        async def check():
            source = calls()[1:]
            client = SimpleNamespace(close=AsyncMock())
            with patch.object(native, "_request", AsyncMock(side_effect=lambda call, **kwargs: outcome(call))) as request, \
                 patch.object(native, "_new_http_client", return_value=client):
                async with native._ProcessorSession() as session:
                    for call in source:
                        row = dict(call_id=call.call_id, row_id=call.row_id, native_call=native._call_wire(call))
                        response = await session.post(native._PROCESSOR_URL,
                            data=json.dumps(native._ray_preprocess(row)["payload"]), headers={})
                        self.assertEqual(response.status, 200)
                        result = native._ray_postprocess(dict(row, http_response=await response.json()))
                        self.assertEqual(native._outcome_from_wire(result["native_outcome"]), outcome(call))
                self.assertEqual(request.await_count, 2)
                self.assertTrue(all(call.kwargs['client'] is client for call in request.await_args_list))
                client.close.assert_awaited_once()
        asyncio.run(check())

    def test_postprocess_rejects_changed_call_or_row_identity(self):
        call = calls()[0]
        wire = native._outcome_wire(outcome(call))
        for identity in ({"call_id": "another", "row_id": call.row_id},
                         {"call_id": call.call_id, "row_id": call.row_id+1}):
            with self.subTest(identity=identity), self.assertRaisesRegex(ValueError, "occurrence identity"):
                native._ray_postprocess(dict(identity, http_response={"native_outcome": wire}))


class SharedConfigurationTests(unittest.TestCase):
    def test_prepared_graph_contexts_apply_their_own_checks_before_import(self):
        from src.baselines.text.frameworks.prepared_map import open_daft_calls, open_ray_calls
        for graph, message in ((open_daft_calls, 'at least two request slots'),
                               (open_ray_calls, 'batch request envelope')):
            with self.subTest(graph=graph.__name__), self.assertRaisesRegex(ValueError, message):
                with graph((), NativeGraphOptions(concurrency=1), None):
                    self.fail('an invalid selected graph must not be opened')

    def test_imlane_c1_is_not_rejected_by_daft_or_unused_ray_parameters(self):
        from src.baselines.text.frameworks.imlane_calls import IMLaneCallConfig
        options = NativeGraphOptions(concurrency=1, ray_batch_rows=256)
        config = IMLaneCallConfig('cpython', '/tmp/fixture-bridge.json', 123, 456, '/tmp/fixture-author.json')
        with patch('src.baselines.text.frameworks.imlane_calls.SemaIMLaneExecutor',
                side_effect=lambda *args, **kwargs: SimpleNamespace(config={'namespace': 'fixture'},
                    work_root=Path('/tmp'), limits=kwargs['limits'])):
            executor = native.NativeCallExecutor('imlane', options, imlane_config=config)
        self.assertIs(executor.options, options)
        self.assertIs(executor._imlane.delegate.limits, executor._imlane.limits)

    def test_common_sizes_still_reject_nonpositive_noninteger_and_unbounded_values(self):
        for values in ({'concurrency': 0}, {'num_threads': True}, {'ray_batch_rows': 257}):
            with self.subTest(values=values), self.assertRaisesRegex(ValueError, 'positive and bounded'):
                NativeGraphOptions(**values)

    def test_ray_core_c1_ignores_ray_data_batch_parameters(self):
        options = NativeGraphOptions(concurrency=1, ray_batch_rows=256)
        self.assertIs(native.NativeCallExecutor('ray', options).options, options)


class GraphConsumerIdentityTests(unittest.TestCase):
    def test_out_of_order_complete_calls_keep_duplicate_input_occurrences(self):
        for backend, graph in (("daft", "_daft_rows"), ("ray-data", "_ray_rows")):
            source = calls()
            expected = tuple(outcome(call) for call in reversed(source))
            with self.subTest(backend=backend), patch.object(native, graph, return_value=graph_results(expected)) as dispatch:
                returned = tuple(native.NativeCallExecutor(backend).iter_batch(source))
                self.assertEqual(returned, expected)
                dispatch.assert_called_once()

    def test_duplicate_mismatched_and_incomplete_outcomes_do_not_pass(self):
        source = calls()
        values = tuple(outcome(call) for call in source)
        for returned in ((values[0], values[0]), (replace(values[0], row_id=99),), (values[0],)):
            with self.subTest(returned=returned), patch.object(native, "_ray_rows", return_value=graph_results(returned)):
                with self.assertRaisesRegex(ValueError, "occurrence identity|incomplete"):
                    tuple(native.NativeCallExecutor("ray-data").iter_batch(source))

    def test_graph_failure_remains_the_first_error_without_resubmission(self):
        failure = RuntimeError("native graph execution failed")
        with patch.object(native, "_daft_rows", side_effect=failure) as dispatch:
            with self.assertRaises(RuntimeError) as observed:
                tuple(native.NativeCallExecutor("daft").iter_batch(calls()))
        self.assertIs(observed.exception, failure)
        dispatch.assert_called_once()


if __name__ == "__main__":
    unittest.main()
