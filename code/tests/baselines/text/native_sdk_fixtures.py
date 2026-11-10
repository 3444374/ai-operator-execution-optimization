"""Replace selected SDK modules without discarding unrelated lazy imports."""

from contextlib import contextmanager
import sys


@contextmanager
def module_entries(values):
    missing = object()
    previous = {name: sys.modules.get(name, missing) for name in values}
    sys.modules.update(values)
    try:
        yield
    finally:
        for name, module in previous.items():
            if module is missing:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module


@contextmanager
def recorded_map_provider(requests, outputs):
    """Real Daft expression/CPU relation and executor; saved-output model stand-in.

    The native AI expression is constructed, not collected. Its software seam
    submits the same rows through the installed LocalVLLMExecutor and returns a
    CPU expression containing those saved answers. No real engine is built.
    """
    import asyncio
    import time
    from types import ModuleType, SimpleNamespace
    from unittest.mock import patch
    import daft
    from daft.functions.ai import prompt as native_prompt
    from daft.execution.vllm import LocalVLLMExecutor
    from daft.recordbatch import RecordBatch
    from src.baselines.text.frameworks.daft_native_trace import NativeMapTrace

    by_id = {row['call_id']: text for row, text in zip(requests, outputs, strict=True)}
    engines, executors, traces, expressions = [], [], [], []
    original_attach = NativeMapTrace.attach

    def attach(trace, engine):
        traces.append(trace)
        original_attach(trace, engine)

    class Engine:
        async def add_request(self, *args, **kwargs):
            return SimpleNamespace(request_id='CPU-internal-' + traces[-1].current_row.get())
        async def generate(self, prompt, sampling_params, request_id, **kwargs):
            collector = await self.add_request(request_id, prompt, sampling_params, **kwargs)
            call_id = traces[-1].current_row.get()
            yield SimpleNamespace(finished=True, request_id=collector.request_id,
                prompt_token_ids=None, outputs=[SimpleNamespace(text=by_id[call_id],
                    token_ids=None, finish_reason='CPU_recorded_output')])
        async def do_log_stats(self):
            return None
        def shutdown(self, *, timeout):
            self.closed = True

    class Factory:
        @classmethod
        def from_engine_args(cls, args, **kwargs):
            engine = Engine()
            engine.closed = False
            engines.append(engine)
            return engine

    module = ModuleType('vllm')
    module.AsyncLLMEngine = Factory
    module.AsyncEngineArgs = lambda **kwargs: kwargs
    module.SamplingParams = lambda **kwargs: kwargs
    structured = ModuleType('vllm.sampling_params')
    structured.StructuredOutputsParams = lambda **kwargs: SimpleNamespace(**kwargs)

    def recorded_prompt(*args, **kwargs):
        expressions.append(native_prompt(*args, **kwargs))
        executor = LocalVLLMExecutor(kwargs['model'], kwargs['engine_args'], dict(kwargs['generate_args']))
        executors.append(executor)
        executor.submit('', [row['prompt'] for row in requests],
            RecordBatch.from_pydict({'call_id': [row['call_id'] for row in requests]}))
        executor.finished_submitting()
        returned = {}
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            value = executor.poll()
            if value is not None:
                texts, rows = value
                returned.update(zip(rows.to_pydict()['call_id'], texts, strict=True))
            if len(returned) == len(requests) and executor.all_tasks_finished():
                break
            time.sleep(.005)
        if returned != by_id or not executor.all_tasks_finished():
            raise AssertionError('recorded native CPU executor did not return every occurrence')
        return daft.col('call_id').apply(lambda call_id: returned[call_id], return_dtype=daft.DataType.string())

    try:
        with module_entries({'vllm': module, 'vllm.sampling_params': structured}), \
                patch.object(NativeMapTrace, 'attach', attach), \
                patch('daft.functions.ai.prompt', recorded_prompt):
            yield SimpleNamespace(engines=engines, executors=executors, expressions=expressions)
    finally:
        for executor in executors:
            executor.loop.call_soon_threadsafe(executor.loop.stop)
            executor.loop_thread.join(2)
            for task in asyncio.all_tasks(executor.loop):
                task.cancel()
            executor.loop.run_until_complete(asyncio.sleep(0))
            executor.loop.close()
