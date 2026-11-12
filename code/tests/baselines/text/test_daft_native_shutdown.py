"""Real Daft executor/CPU relations with a zero-model engine stand-in."""

from contextlib import contextmanager
import asyncio
import multiprocessing
from pathlib import Path
import tempfile
import time
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks.daft_sembench_native import execute_prepared_map
from tests.baselines.text.native_sdk_fixtures import module_entries


def _wait_for_release(path):
    deadline = time.monotonic() + 10
    while not Path(path).exists() and time.monotonic() < deadline:
        time.sleep(.01)


@contextmanager
def native_cpu_api(directory, *, shutdown_error=None, construction_error=None):
    import daft
    from daft.execution.vllm import LocalVLLMExecutor
    from daft.recordbatch import RecordBatch

    engines, executors = [], []

    class Engine:
        def __init__(self):
            self.release = Path(directory) / ('release-' + str(len(engines)))
            self.process = multiprocessing.get_context('spawn').Process(
                target=_wait_for_release, args=(str(self.release),))
            self.process.start()
            self.shutdown_calls = []
            engines.append(self)

        async def add_request(self,*args,**kwargs):
            return None

        async def generate(self, prompt, sampling_params, request_id, **kwargs):
            yield SimpleNamespace(finished=True,request_id=request_id,
                outputs=[SimpleNamespace(text='Answer: True',token_ids=None,finish_reason='CPU_fixture')])

        async def do_log_stats(self):
            return None

        def shutdown(self, timeout=None):
            self.shutdown_calls.append(timeout)
            if shutdown_error is not None:
                raise shutdown_error
            self.release.write_text('public model shutdown')
            self.process.join(timeout)
            if self.process.is_alive():
                raise TimeoutError('CPU engine process still alive')

    class AsyncEngine:
        @classmethod
        def from_engine_args(cls, engine_args, **kwargs):
            engine = Engine()
            if construction_error is not None:
                raise construction_error
            return engine

    module = ModuleType('vllm')
    module.AsyncLLMEngine = AsyncEngine
    module.AsyncEngineArgs = lambda **kwargs: kwargs
    module.SamplingParams = lambda **kwargs: kwargs
    original_factory = AsyncEngine.__dict__['from_engine_args']

    def native_expression(*args, **kwargs):
        # This is the model construction and asynchronous submission performed
        # by the real native sink. No model or CUDA implementation is imported.
        executor = LocalVLLMExecutor('CPU-only', {}, dict(kwargs['generate_args']))
        executors.append(executor)
        executor.submit('', ['one complete request'], RecordBatch.from_pydict({'call_id': ['source-0']}))
        executor.finished_submitting()
        deadline = time.monotonic() + 2
        output = None
        while not executor.all_tasks_finished() and time.monotonic() < deadline:
            value = executor.poll()
            if value is not None:
                output = value
            time.sleep(.005)
        if output is None or output[0] != ['Answer: True'] or not executor.all_tasks_finished():
            raise AssertionError('CPU native request did not complete')
        return daft.lit(output[0][0])

    try:
        with module_entries({'vllm': module}), patch('daft.functions.ai.prompt', native_expression):
            yield SimpleNamespace(engines=engines, executors=executors, cls=AsyncEngine,
                                  original_factory=original_factory)
    finally:
        for engine in engines:
            engine.release.write_text('test finally cleanup')
            engine.process.join(3)
            if engine.process.is_alive():
                engine.process.terminate()
                engine.process.join(3)
        # The provider's thread is daemonized. Stop it in the CPU test process
        # as test hygiene; production uses an isolated query process.
        for executor in executors:
            executor.loop.call_soon_threadsafe(executor.loop.stop)
            executor.loop_thread.join(2)
            for task in asyncio.all_tasks(executor.loop):
                task.cancel()
            executor.loop.run_until_complete(asyncio.sleep(0))
            executor.loop.close()


class DaftNativeShutdownTests(unittest.TestCase):
    def run_query(self, **kwargs):
        return execute_prepared_map([dict(call_id='source-0',prompt='one complete request')],
            model='CPU-only',sampling_params={'max_tokens':8192},**kwargs)

    def test_result_return_also_releases_the_native_model_process(self):
        with tempfile.TemporaryDirectory() as directory, native_cpu_api(directory) as native:
            result = self.run_query()
            self.assertEqual(len(result['predictions']),1)
            self.assertEqual(len(native.engines), 1)
            self.assertFalse(native.engines[0].process.is_alive(), 'native query left its model process alive')
            self.assertEqual(len(native.engines[0].shutdown_calls), 1)
            self.assertIs(native.cls.__dict__['from_engine_args'], native.original_factory)

    def test_result_ready_callback_precedes_close_and_receipt_separates_physical_exit(self):
        with tempfile.TemporaryDirectory() as directory, native_cpu_api(directory) as native:
            observed = []
            def ready(result):
                observed.append((len(result['predictions']), native.engines[0].process.is_alive(),
                                 result['metadata']['result_ready_monotonic_ns']))
            result = self.run_query(on_result_ready=ready, close_timeout_s=3)
            self.assertEqual(observed[0][:2], (1, True))
            self.assertEqual(len(observed), 1)
            receipt = result['metadata']['model_close_receipt']
            self.assertTrue(receipt['engine_api_shutdown_confirmed'])
            self.assertIsNone(receipt['gpu_exit_confirmed'])
            self.assertEqual(receipt['captured_models'], 1)
            self.assertTrue(0 < native.engines[0].shutdown_calls[0] <= 3)

    def test_shutdown_error_preserves_predictions_and_is_not_success(self):
        from src.baselines.text.frameworks.daft_native_owner import NativeModelCloseError

        failure = RuntimeError('CPU shutdown failure')
        with tempfile.TemporaryDirectory() as directory, native_cpu_api(directory, shutdown_error=failure) as native:
            with self.assertRaises(NativeModelCloseError) as caught:
                self.run_query()
            self.assertIs(caught.exception.__cause__, failure)
            report = caught.exception.native_query_report
            self.assertEqual(len(report['predictions']),1)
            self.assertEqual(len(report['predictions']), 1)
            self.assertEqual(report['metadata']['model_close_receipt']['status'], 'unknown')
            self.assertFalse(report['metadata']['model_close_receipt']['engine_api_shutdown_confirmed'])
            self.assertIs(native.cls.__dict__['from_engine_args'], native.original_factory)

    def test_observation_error_remains_primary_when_shutdown_also_fails(self):
        primary = ValueError('CPU result artifact failure')
        with tempfile.TemporaryDirectory() as directory, native_cpu_api(
                directory, shutdown_error=RuntimeError('CPU secondary shutdown failure')) as native:
            def ready(result):
                raise primary
            with self.assertRaises(ValueError) as caught:
                self.run_query(on_result_ready=ready)
            self.assertIs(caught.exception, primary)
            self.assertEqual(len(primary.native_query_report['predictions']),1)
            self.assertEqual(primary.model_close_report['status'], 'unknown')
            self.assertIs(native.cls.__dict__['from_engine_args'], native.original_factory)

    def test_partial_engine_construction_does_not_claim_not_started_or_closed(self):
        failure = RuntimeError('CPU factory failed after starting its child')
        with tempfile.TemporaryDirectory() as directory, native_cpu_api(directory, construction_error=failure) as native:
            with self.assertRaises(RuntimeError) as caught:
                self.run_query()
            self.assertIs(caught.exception, failure)
            receipt = failure.native_query_report['metadata']['model_close_receipt']
            self.assertEqual(receipt['status'], 'unknown')
            self.assertEqual(receipt['construction_attempts'], 1)
            self.assertEqual(receipt['captured_models'], 0)
            self.assertEqual(receipt['unconfirmed_constructions'], 1)
            self.assertFalse(receipt['engine_api_shutdown_confirmed'])
            self.assertTrue(native.engines[0].process.is_alive())
            self.assertIs(native.cls.__dict__['from_engine_args'], native.original_factory)


if __name__ == '__main__':
    unittest.main()
