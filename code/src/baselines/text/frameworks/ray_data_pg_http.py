"""Pinned Ray SQL reader to native HTTP Processor; caller records the iteration.

Plan construction, SQL support probes/COUNTs and message construction execute
inside open_rows. Native Ray owns sharding, actors, batching and backpressure.
"""
from contextlib import contextmanager
from dataclasses import dataclass

from src.experiments.postgresql.map_direct import request_body
from src.execution_provider.adapters.completion_response import decode_backend_completion
from src.execution_provider.semantic_map import completion_status, MapCompletionStatus


@dataclass(frozen=True)
class RaySqlHttpConfig:
    read_blocks: int
    read_concurrency: int
    actors: int
    batch_rows: int
    expected_version: str = '2.56.1'

    def __post_init__(self):
        if any(type(v) is not int or v < 1 for v in (
            self.read_blocks, self.read_concurrency, self.actors, self.batch_rows
        )):
            raise ValueError('positive Ray source and processor sizes required')


def preprocess(row, inputs, plan):
    raw = (int(row['source_position']), row['row_id'], *(row[c] for c in inputs.columns))
    converted = inputs.convert(raw)
    return dict(row_id=converted['source_example_id'], source_position=converted['source_position'],
                payload=request_body(plan, converted['input_text']))


def postprocess(row, plan):
    import json
    response = row['http_response']
    completion = decode_backend_completion(json.dumps(response).encode())
    if completion_status(plan, completion) != MapCompletionStatus.VALID:
        raise ValueError('native Ray completion violates declared Map semantics')
    return dict(row_id=row['row_id'], source_position=row['source_position'], output=completion.raw_output)


@contextmanager
def open_rows(inputs, plan, config, connection_factory, session_factory, *, headers=None, record_stats=None):
    import ray
    from ray.data.llm import HttpRequestProcessorConfig, build_processor
    if ray.__version__ != config.expected_version or not ray.is_initialized():
        raise RuntimeError('requires a caller-owned Ray runtime at the pinned version')
    statement, parameters = inputs.select_sql(ordered=False)
    dataset = ray.data.read_sql(statement, connection_factory, sql_params=parameters,
        shard_keys=['source_position'], shard_hash_fn='abs', override_num_blocks=config.read_blocks,
        concurrency=config.read_concurrency, ray_remote_args={'max_retries': 0})
    processor = build_processor(HttpRequestProcessorConfig(
        url=session_factory.endpoint, headers=headers, batch_size=config.batch_rows,
        concurrency=(config.actors, config.actors), max_retries=0, session_factory=session_factory),
        preprocess=lambda row: preprocess(row, inputs, plan),
        postprocess=lambda row: postprocess(row, plan),
        preprocess_map_kwargs={'max_retries': 0}, postprocess_map_kwargs={'max_retries': 0})
    # Pinned native actor options bound concurrent HTTP batches and disable
    # recovery replays; Ray still owns the actor pool and processing graph.
    for stage in processor.stages.values():
        stage.map_batches_kwargs.update(max_restarts=0, max_task_retries=0,max_concurrency=1)
    result = processor(dataset)
    iterator = result.iter_rows()
    try:
        yield ((row['row_id'], row['output']) for row in iterator)
    finally:
        close = getattr(iterator, 'close', None)
        if close is not None:
            close()
        if record_stats is not None:
            record_stats(result.stats())
