"""Pinned native graphs execute complete calls prepared by an external method.

The graphs own parallelism and buffering. Their HTTP kernels perform no
SemLoom submission, scheduling or method continuation.
"""
import base64
from contextlib import contextmanager
from dataclasses import dataclass
import json
import os
import sys

from src.experiments.request_identity import RAY_IDENTITY_FIELD
from src.experiments.execution_timing import is_timing_error, timing_span


RESPONSE_FIELD = '__semloom_complete_response'


def _cleanup_graph(backend, stream, iterator, *, result=None, record_stats=None):
    if not os.environ.get('SEMLOOM_DETAILED_TIMING_DIR'):
        stream.close()
        close = getattr(iterator, 'close', None)
        if close is not None:
            close()
        if record_stats is not None and result is not None:
            record_stats(result.stats())
        return
    primary = sys.exception()
    failures = []
    actions = [('native.stream_close', stream.close)]
    close = getattr(iterator, 'close', None)
    if close is not None:
        actions.append(('native.iterator_close', close))
    if record_stats is not None and result is not None:
        actions.append(('native.stats', lambda: record_stats(result.stats())))
    for operation, action in actions:
        action_error = None
        try:
            with timing_span(operation, backend=backend):
                try:
                    action()
                except BaseException as error:
                    action_error = error
                    raise
        except BaseException as error:
            failures.append((operation, error, action_error is not None))
    original = next((error for _, error, action in failures if action and not is_timing_error(error)), None)
    selected = (primary if primary is not None and not is_timing_error(primary)
                else original or primary or (failures[0][1] if failures else None))
    if selected is not None:
        if primary is not None and selected is not primary:
            selected.add_note('Detailed timing also failed: ' + type(primary).__name__)
        for operation, error, _ in failures:
            if error is not selected:
                selected.add_note(operation + ' also failed: ' + type(error).__name__)
        if selected is not primary:
            raise selected


def graph_input(call):
    return dict(row_id=call['row_id'], source_position=call['source_position'],
                call_id=call['call_id'], payload=json.loads(call['payload']))


def _graph_inputs(calls, max_batch_rows, max_batch_bytes):
    """Bound the fully retained input before building either vendor graph."""
    if any(type(value) is not int or value <= 0 for value in (max_batch_rows, max_batch_bytes)):
        raise ValueError('native prepared-call batch limits must be positive integers')
    inputs, payload_bytes = [], 0
    for call in calls:
        if len(inputs) == max_batch_rows:
            raise ValueError('native prepared-call input batch exceeds its declared allowance')
        payload = call['payload']
        payload_bytes += len(payload.encode() if isinstance(payload, str) else payload)
        if payload_bytes > max_batch_bytes:
            raise ValueError('native prepared-call input batch exceeds its declared allowance')
        inputs.append(graph_input(call))
    return tuple(inputs)


def graph_preprocess(row):
    identity = dict(row_id=row['row_id'], source_position=row['source_position'])
    if RAY_IDENTITY_FIELD in row['payload']:
        raise ValueError('method payload conflicts with native observation metadata')
    return dict(payload=dict(row['payload'], **{RAY_IDENTITY_FIELD:identity}))


def graph_postprocess(row):
    response = row['http_response']
    identity = dict(row_id=row['row_id'], source_position=row['source_position'])
    if response.get(RAY_IDENTITY_FIELD) != identity:
        raise ValueError('native graph response belongs to another input occurrence')
    wire = response.get(RESPONSE_FIELD)
    if not isinstance(wire, str):
        raise ValueError('native graph lacks a complete response')
    # Validate before a malformed representation enters native result storage.
    base64.b64decode(wire, validate=True)
    return dict(row_id=row['row_id'], call_id=row['call_id'], response=wire)


@dataclass(frozen=True)
class NativeGraphOptions:
    """Declared allowances and vendor graph settings, not measured parallelism."""
    concurrency: int = 4
    num_threads: int = 8
    ray_actors: int = 2
    ray_async_batches_per_actor: int = 2
    ray_batch_rows: int = 1
    source_blocks: int = 4

    def __post_init__(self):
        values = (self.concurrency, self.num_threads, self.ray_actors,
                  self.ray_async_batches_per_actor, self.ray_batch_rows, self.source_blocks)
        if any(type(v) is not int or not 1 <= v <= 256 for v in values):
            raise ValueError('native graph sizes must be positive and bounded')
    def validate_backend(self, backend):
        if backend == 'daft' and self.concurrency < 2:
            raise ValueError('pinned native Daft needs at least two request slots')
        if backend == 'ray-data' and self.ray_active_http_upper_bound > self.concurrency:
            raise ValueError('native Ray active HTTP upper bound exceeds the declared HTTP allowance')
        if backend == 'ray-data' and self.num_threads <= self.ray_actors:
            raise ValueError('native Ray Data needs a spare CPU slot for preprocessing and postprocessing')

    @property
    def ray_request_row_envelope(self):
        # HTTP-stage batch rows only; not active requests or whole-graph storage.
        return self.ray_actors * self.ray_async_batches_per_actor * self.ray_batch_rows

    @property
    def ray_active_http_upper_bound(self):
        # Pinned HTTP batches await each lazy post in turn; actor max_concurrency=1.
        return self.ray_actors * self.ray_async_batches_per_actor


@contextmanager
def open_daft_calls(calls, options, session_factory, *, headers=None,
                    max_batch_rows=4096, max_batch_bytes=32 * 1024 * 1024):
    options.validate_backend('daft')
    import daft
    if daft.__version__ != '0.7.21':
        raise RuntimeError('prepared native Map requires pinned Daft 0.7.21')
    iterator = None

    def rows():
        nonlocal iterator
        with timing_span('native.graph_input', backend='daft'):
            inputs = _graph_inputs(calls, max_batch_rows, max_batch_bytes)
        if not inputs:
            return
        with timing_span('native.graph_build', backend='daft', scope='adapter vendor graph API calls'):
            frame = daft.from_pydict({
                name:[(json.dumps(row[name], ensure_ascii=False) if name == 'payload' else row[name])
                      for row in inputs] for name in ('row_id','source_position','call_id','payload')})

            @daft.func.batch(return_dtype=daft.DataType.string(), use_process=False,
                            batch_size=1, max_concurrency=options.concurrency-1,
                            max_retries=0, on_error='raise')
            async def complete(row_ids, positions, payloads):
                if any(len(column) != 1 for column in (row_ids, positions, payloads)):
                    raise ValueError('native Daft graph requires one complete call per batch')
                row = dict(row_id=row_ids.to_pylist()[0], source_position=positions.to_pylist()[0],
                           payload=json.loads(payloads.to_pylist()[0]))
                request = graph_preprocess(row)
                async with session_factory() as session:
                    async with await session.post(session_factory.endpoint,
                            data=json.dumps(request['payload'],ensure_ascii=False),
                            headers={'Content-Type':'application/json','Accept-Encoding':'identity', **(headers or {})}) as response:
                        value = await response.json()
                        if response.status != 200:
                            raise RuntimeError('native Daft HTTP status ' + str(response.status))
                row['http_response'] = value
                row['call_id'] = ''
                return daft.Series.from_pylist([graph_postprocess(row)['response']])

            result = frame.with_column('response', complete(daft.col('row_id'),
                daft.col('source_position'), daft.col('payload'))).select('row_id','call_id','response')
        with timing_span('native.iterator_open', backend='daft',
                         scope='vendor iterator API call; actor readiness unavailable'):
            iterator = result.iter_rows()
        with timing_span('native.consume', backend='daft',
                         scope='native iterator through adapter result yields; includes consumer yield wait'):
            for row in iterator:
                yield row['call_id'], row['row_id'], base64.b64decode(row['response'],validate=True)

    stream = rows()
    try:
        yield stream
    finally:
        _cleanup_graph('daft', stream, iterator)


@contextmanager
def open_ray_calls(calls, options, session_factory, *, headers=None, record_stats=None,
                   max_batch_rows=4096, max_batch_bytes=32 * 1024 * 1024):
    options.validate_backend('ray-data')
    import ray
    from ray.data.llm import HttpRequestProcessorConfig, build_processor
    from ray.data._internal.planner.plan_udf_map_op import DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY
    if ray.__version__ != '2.56.1' or not ray.is_initialized():
        raise RuntimeError('prepared native Map requires caller-owned pinned Ray 2.56.1')
    if DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY != options.ray_async_batches_per_actor:
        raise RuntimeError('native Ray requires its declared asynchronous batch limit at import')
    iterator = result = None

    def rows():
        nonlocal iterator, result
        with timing_span('native.graph_input', backend='ray-data'):
            inputs = _graph_inputs(calls, max_batch_rows, max_batch_bytes)
        if not inputs:
            return
        with timing_span('native.graph_build', backend='ray-data', scope='adapter vendor graph API calls'):
            dataset = ray.data.from_items(list(inputs),
                override_num_blocks=min(len(inputs), options.source_blocks))
            processor = build_processor(HttpRequestProcessorConfig(
                url=session_factory.endpoint, headers={'Accept-Encoding':'identity',**(headers or {})},
                batch_size=options.ray_batch_rows,
                concurrency=(options.ray_actors,options.ray_actors), max_retries=0,
                session_factory=session_factory), preprocess=graph_preprocess,
                postprocess=graph_postprocess, preprocess_map_kwargs={'max_retries':0},
                postprocess_map_kwargs={'max_retries':0})
            for stage in processor.stages.values():
                stage.map_batches_kwargs.update(max_restarts=0, max_task_retries=0,max_concurrency=1)
            result = processor(dataset)
        with timing_span('native.iterator_open', backend='ray-data',
                         scope='vendor iterator API call; actor readiness unavailable'):
            iterator = result.iter_rows()
        with timing_span('native.consume', backend='ray-data',
                         scope='native iterator through adapter result yields; includes consumer yield wait'):
            for row in iterator:
                yield row['call_id'], row['row_id'], base64.b64decode(row['response'],validate=True)

    stream = rows()
    try:
        yield stream
    finally:
        _cleanup_graph('ray-data', stream, iterator, result=result, record_stats=record_stats)
