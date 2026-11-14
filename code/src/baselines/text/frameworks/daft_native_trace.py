"""Scoped evidence at the native Daft row and vLLM API boundaries.

Delegation preserves arguments, yielded objects and iterator control. It does
not reserve capacity, retry, consume ahead, schedule tasks or call abort.
"""

from contextvars import ContextVar
import hashlib
import time
import traceback

from src.infrastructure.redact import redact_text

_MISSING = object()


class _ObservedStream:
    def __init__(self, stream, trace, record):
        self.stream, self.trace, self.record = stream, trace, record

    def __aiter__(self):
        return self

    async def _advance(self, operation, *args):
        if 'iteration_started_ns' not in self.record:
            self.record['iteration_started_ns'] = time.monotonic_ns()
            self.trace.emit('native_generate_iteration', self.record)
        try:
            value = await operation(*args)
        except StopAsyncIteration:
            self.record['iterator_exhausted_ns'] = time.monotonic_ns()
            raise
        except BaseException as error:
            self.record.update(error_type=type(error).__name__, error=redact_text(str(error)),
                               error_traceback=redact_text(''.join(traceback.format_exception(error))),
                               error_monotonic_ns=time.monotonic_ns())
            if error.__cause__ is not None:
                self.record.update(cause_type=type(error.__cause__).__name__,
                                   cause=redact_text(str(error.__cause__)))
            self.trace.emit('native_generate_error', self.record)
            raise
        self.trace.final(self.record, value)
        return value

    async def __anext__(self):
        return await self._advance(self.stream.__anext__)

    async def athrow(self, *args):
        return await self._advance(self.stream.athrow, *args)

    async def aclose(self):
        # Closing is a delegated operation, not a terminal model receipt.
        return await self.stream.aclose()


class NativeMapTrace:
    def __init__(self, emit=None, *, before_generate=None):
        self.sink = emit
        self.before_generate = before_generate
        self.current_row = ContextVar('native_daft_row', default=None)
        self.requests, self.errors, self.models = [], [], []
        self.ready = []

    def emit(self, event, record):
        if self.sink is not None:
            try:
                self.sink(event, **record)
            except Exception as error:
                self.errors.append(dict(boundary='native evidence sink', error_type=type(error).__name__))

    def __enter__(self):
        from daft.execution.vllm import LocalVLLMExecutor

        self.executor_type = LocalVLLMExecutor
        self.original_row = LocalVLLMExecutor._generate
        trace = self

        async def row(executor, prompt, rows):
            call_id = None
            try:
                call_id = rows.to_pydict()['call_id'][0]
            except Exception as error:
                trace.errors.append(dict(boundary='Daft row identity', error_type=type(error).__name__))
            token = trace.current_row.set(call_id)
            try:
                return await trace.original_row(executor, prompt, rows)
            finally:
                trace.current_row.reset(token)

        LocalVLLMExecutor._generate = row
        return self

    def attach(self, model):
        trace = self
        engine_id = len(self.models)
        self.models.append((model, model.__dict__.get('generate', _MISSING)))
        config = getattr(model, 'vllm_config', None)
        model_config = getattr(model, 'model_config', getattr(config, 'model_config', None))
        cache_config = getattr(model, 'cache_config', getattr(config, 'cache_config', None))
        scheduler_config = getattr(model, 'scheduler_config', getattr(config, 'scheduler_config', None))
        dtype = getattr(model_config, 'dtype', None)
        self.ready.append(dict(engine_id=engine_id, model_constructed_ns=time.monotonic_ns(),
            dtype=None if dtype is None else str(dtype),
            max_model_len=getattr(model_config, 'max_model_len', None),
            gpu_memory_utilization=getattr(cache_config, 'gpu_memory_utilization', None),
            enable_prefix_caching=getattr(cache_config, 'enable_prefix_caching', None),
            max_num_seqs=getattr(scheduler_config, 'max_num_seqs', None),
            max_num_batched_tokens=getattr(scheduler_config, 'max_num_batched_tokens', None)))
        self.emit('native_model_constructed', self.ready[-1])
        original_generate = model.generate

        def generate(*args, **kwargs):
            prompt = kwargs.get('prompt', args[0] if args else None)
            request_id = kwargs.get('request_id', args[2] if len(args) > 2 else None)
            sampling = kwargs.get('sampling_params', args[1] if len(args) > 1 else None)
            observed_sampling = {key: (sampling.get(key) if isinstance(sampling, dict)
                                      else getattr(sampling, key, None)) for key in (
                'n', 'max_tokens', 'min_tokens', 'temperature', 'top_p', 'top_k', 'seed',
                'stop', 'stop_token_ids', 'ignore_eos', 'presence_penalty', 'frequency_penalty',
                'repetition_penalty', 'logprobs', 'prompt_logprobs')}
            # A finite experiment allowance is enforced before delegation.
            # Unlike optional evidence, a reservation failure must propagate.
            if trace.before_generate is not None:
                trace.before_generate(trace.current_row.get(), request_id, prompt)
            record = dict(observation_id=len(trace.requests), engine_id=engine_id, call_id=trace.current_row.get(),
                caller_request_id=request_id,
                prompt_sha256=hashlib.sha256(prompt.encode()).hexdigest() if isinstance(prompt, str) else None,
                sampling_observed=observed_sampling,
                generate_called_ns=time.monotonic_ns())
            trace.requests.append(record)
            trace.emit('native_generate_called', record)
            # Call now, exactly as Daft does; iteration remains lazy.
            stream = original_generate(*args, **kwargs)
            return _ObservedStream(stream, trace, record)

        model.generate = generate

    def final(self, record, output):
        try:
            if getattr(output, 'finished', False):
                record.update(final_observed_ns=time.monotonic_ns(), finished=True,
                    native_output_request_id=getattr(output, 'request_id', None),
                    prompt_tokens=len(output.prompt_token_ids) if getattr(output, 'prompt_token_ids', None) is not None else None,
                    outputs=[dict(text_sha256=hashlib.sha256(item.text.encode()).hexdigest(),
                        completion_tokens=len(item.token_ids) if getattr(item, 'token_ids', None) is not None else None,
                        finish_reason=getattr(item, 'finish_reason', None)) for item in output.outputs])
                self.emit('native_final_observed', record)
        except Exception as error:
            self.errors.append(dict(boundary='native final observation', error_type=type(error).__name__))

    def report(self):
        return dict(scope='native generate/final API; no GPU kernel evidence',
            first_engine_accept_ns=None,
            engine_accept_reason='engine acceptance is not independently observed',
            model_ready_reason='factory returned an initialized native model; no additional warmup request',
            generate_calls=len(self.requests),
            generation_iterations=sum('iteration_started_ns' in row for row in self.requests),
            final_events=sum(row.get('finished') is True for row in self.requests),
            unresolved=sum(row.get('finished') is not True for row in self.requests),
            requests=[dict(row) for row in self.requests], errors=list(self.errors), ready=list(self.ready))

    def __exit__(self, *exception):
        self.executor_type._generate = self.original_row
        for model, original in reversed(self.models):
            if original is _MISSING:
                del model.generate
            else:
                model.generate = original
        self.models.clear()
        return False
