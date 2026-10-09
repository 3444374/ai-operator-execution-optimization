"""Daft Native owns SQL scanning and async batch-UDF scheduling.

The UDF is only the matched HTTP workload kernel; it has no feeder, semaphore,
task pool or SemLoom execution core. This is not the built-in prompt() path.
"""
from contextlib import contextmanager
import json

from .ray_data_pg_http import preprocess, postprocess


def prepare_runtime(num_threads):
    """Initialize the native runtime once in the query's fresh driver process."""
    import daft
    if daft.__version__ != '0.7.21':
        raise RuntimeError('native Daft Map requires pinned Daft 0.7.21')
    if type(num_threads) is not int or num_threads < 1:
        raise ValueError('native Daft runtime needs positive threads')
    daft.set_runner_native(num_threads=num_threads)


@contextmanager
def open_rows(inputs, plan, connection_factory, session_factory, *, concurrency,
              partitions, num_threads=8, headers=None, runtime_prepared=False):
    import daft
    if daft.__version__ != '0.7.21':
        raise RuntimeError('native Daft Map requires pinned Daft 0.7.21')
    if inputs.movie_id is not None:
        raise ValueError('native Daft matched Map requires a complete immutable source')
    iterator = None

    def rows():
        nonlocal iterator
        if not runtime_prepared:
            daft.set_runner_native(num_threads=num_threads)
        statement, parameters = inputs.select_sql(ordered=False)
        if parameters:
            raise ValueError('native Daft Map does not interpolate SQL parameters')
        frame = daft.read_sql(statement, connection_factory, partition_col='source_position',
                              num_partitions=partitions)

        # Daft 0.7.21 AsyncUdfSink spawns before draining while in-flight > K.
        # One-row batches and K=C-1 bound the observed request peak at C.
        # The candidate fixture checks this property for the pinned runtime.
        @daft.func.batch(return_dtype=daft.DataType.string(), use_process=False, batch_size=1,
                   max_concurrency=max(1,concurrency-1), max_retries=0, on_error='raise')
        async def complete(position, row_id, *values):
            columns=(position,row_id,*values)
            if any(len(column)!=1 for column in columns):
                raise ValueError('native Daft HTTP kernel requires its declared one-row batch')
            source = dict(zip(('source_position','row_id',*inputs.columns),
                              (column.to_pylist()[0] for column in columns)))
            request = preprocess(source, inputs, plan)
            # One native row invocation owns its client, matching Ray batch_rows=1.
            # Cleanup is lexical even when Daft cancels an in-flight coroutine.
            async with session_factory() as session:
                async with await session.post(session_factory.endpoint,
                        data=json.dumps(request['payload']), headers={"Content-Type":"application/json",**(headers or {})}) as response:
                    if response.status != 200:
                        raise RuntimeError('native Daft HTTP status '+str(response.status))
                    request['http_response'] = await response.json()
            return daft.Series.from_pylist([postprocess(request, plan)['output']])

        expression = complete(*(daft.col(c) for c in ('source_position','row_id',*inputs.columns)))
        result = frame.with_column('output', expression).select('row_id','output')
        iterator = result.iter_rows()
        for row in iterator:
            yield row['row_id'], row['output']

    stream = rows()
    try:
        yield stream
    finally:
        stream.close()
        close = getattr(iterator, 'close', None)
        if close is not None:
            close()
