"""Native generated-text operators for the same application Map task.

Each system keeps its public prompt construction, parser and execution owner.
These are application comparisons, not identical-message execution controls.
"""
from contextlib import contextmanager
import hashlib
import importlib.metadata

from src.baselines.common.contracts import ChatRequest


ROLES = ('lotus-map', 'daft-prompt', 'duckdb-ai', 'sema-map')


def application_prompt(instruction, text):
    return instruction + '\n\nInput:\n' + text


def validate_source(rows, inputs):
    """Preserve repeated values and original IDs using unique row occurrences."""
    values = [inputs.convert(row) for row in rows]
    identities = [value['source_example_id'] for value in values]
    if len(values) != inputs.max_rows or len(set(identities)) != len(identities):
        raise ValueError('native application source is incomplete or repeats row occurrences')
    return values


def _lotus(values, plan, model, concurrency, tokenizer_path):
    import pandas as pd
    import lotus
    from lotus.models import LM
    if importlib.metadata.version('lotus-ai') != '1.2.4':
        raise RuntimeError('semantic Map comparison requires LOTUS1.2.4')
    tokenizer = None
    if tokenizer_path is not None:
        from tokenizers import Tokenizer
        tokenizer = Tokenizer.from_file(str(tokenizer_path))
    lm = LM('openai/' + model.model_id,
        api_base=model.endpoint_url.removesuffix('/chat/completions'),
        api_key=model.bearer_token or 'local-fixture', max_batch_size=concurrency,
        temperature=0, top_p=1, max_tokens=plan.max_tokens, num_retries=0,
        max_retries=0, timeout=model.timeout_ms / 1000, tokenizer=tokenizer)
    lotus.settings.configure(lm=lm, enable_cache=False)
    frame = pd.DataFrame({'row_id': [v['source_example_id'] for v in values],
                          'review_text': [v['input_text'] for v in values]})
    result = frame.sem_map(plan.instruction + '\nInput: {review_text}', suffix='_answer')
    if lm.stats.cache_hits:
        raise ValueError('native LOTUS unexpectedly reused a cached response')
    yield from zip(result['row_id'], result['_answer'])


def _daft(values, plan, model, concurrency, num_threads):
    import daft
    from daft.ai.openai.provider import OpenAIProvider
    from daft.functions import prompt
    if daft.__version__ != '0.7.21':
        raise RuntimeError('semantic Map comparison requires Daft0.7.21')
    daft.set_runner_native(num_threads=num_threads)
    provider = OpenAIProvider(base_url=model.endpoint_url.removesuffix('/chat/completions'),
                              api_key=model.bearer_token or 'local-fixture')
    frame = daft.from_pydict({'row_id': [v['source_example_id'] for v in values],
        'prompt': [application_prompt(plan.instruction, v['input_text']) for v in values]})
    expression = prompt(daft.col('prompt'), provider=provider, model=model.model_id,
        use_chat_completions=True, temperature=0, max_tokens=plan.max_tokens,
        max_retries=0, on_error='raise', concurrency=concurrency)
    result = frame.with_column('output', expression).select('row_id', 'output')
    iterator = result.iter_rows()
    try:
        for row in iterator:
            yield row['row_id'], row['output']
    finally:
        close = getattr(iterator, 'close', None)
        if close is not None:
            close()


def _duckdb(values, plan, model, concurrency):
    from src.baselines.text.products.duckdb_ai import (
        DuckDBAiConfig, inspect_duckdb_ai_runtime, run_duckdb_ai_complete)
    config = DuckDBAiConfig(
        endpoint_base_url=model.endpoint_url.removesuffix('/chat/completions'),
        model=model.model_id, api_key=model.bearer_token or 'local-fixture',
        max_tokens=plan.max_tokens, max_concurrent_requests=concurrency,
        timeout_seconds=max(1, model.timeout_ms // 1000))
    identity = inspect_duckdb_ai_runtime(config)
    if identity['duckdb_version'] != 'v1.5.4' or identity['duckdb_ai_extension_version'] != '0.4.14':
        raise RuntimeError('semantic Map comparison requires DuckDB1.5.4/ai0.4.14')
    requests = tuple(ChatRequest(index, application_prompt(plan.instruction, v['input_text']),
        0, 0, plan.max_tokens, plan.max_tokens,
        hashlib.sha256(v['input_text'].encode()).hexdigest(), 0)
        for index, v in enumerate(values))
    for result in run_duckdb_ai_complete(requests, config):
        if result.status != 'completed' or result.output_text is None:
            raise ValueError('native DuckDB AI returned a failed row')
        yield values[result.doc_id]['source_example_id'], result.output_text


@contextmanager
def open_rows(role, load_source, inputs, plan, model, *, concurrency, num_threads=8,
              tokenizer_path=None, sema_binary=None, artifact_root=None):
    """Read the bounded PG source after entry, then invoke the native operator."""
    if role not in ROLES or type(concurrency) is not int or not 1 <= concurrency <= 64:
        raise ValueError('unsupported semantic Map role or native concurrency')
    if inputs.max_rows > 4096 or not model.endpoint_url.endswith('/chat/completions'):
        raise ValueError('native Map requires a bounded source and chat endpoint')
    def rows():
        values = validate_source(load_source(), inputs)
        if role == 'lotus-map':
            yield from _lotus(values, plan, model, concurrency, tokenizer_path)
        elif role == 'daft-prompt':
            yield from _daft(values, plan, model, concurrency, num_threads)
        elif role == 'duckdb-ai':
            yield from _duckdb(values, plan, model, concurrency)
        else:
            from src.baselines.text.products.sema import run_projection
            if sema_binary is None or artifact_root is None:
                raise ValueError('Sema requires the pinned binary and a private artifact directory')
            yield from run_projection(values, plan, model, binary=sema_binary,
                                      root=artifact_root, num_threads=num_threads)
    stream = rows()
    try:
        yield stream
    finally:
        stream.close()


class PreparedRows:
    """A native source and runtime ready for a separately timed execute call."""

    def __init__(self, execute):
        self._execute = execute
        self._active = set()
        self._closed = False

    def execute(self):
        if self._closed:
            raise RuntimeError('prepared native Map source is closed')
        stream = iter(self._execute())
        self._active.add(stream)
        try:
            yield from stream
        finally:
            self._active.discard(stream)
            close = getattr(stream, 'close', None)
            if close is not None:
                close()

    def close(self):
        if not self._closed:
            self._closed = True
            for stream in tuple(self._active):
                close = getattr(stream, 'close', None)
                if close is not None:
                    close()
            self._active.clear()


@contextmanager
def _prepare_lotus(values, plan, model, concurrency, tokenizer_path):
    import pandas as pd
    import lotus
    from lotus.models import LM
    if importlib.metadata.version('lotus-ai') != '1.2.4':
        raise RuntimeError('semantic Map comparison requires LOTUS1.2.4')
    tokenizer = None
    if tokenizer_path is not None:
        from tokenizers import Tokenizer
        tokenizer = Tokenizer.from_file(str(tokenizer_path))
    lm = LM('openai/' + model.model_id,
        api_base=model.endpoint_url.removesuffix('/chat/completions'),
        api_key=model.bearer_token or 'local-fixture', max_batch_size=concurrency,
        temperature=0, top_p=1, max_tokens=plan.max_tokens, num_retries=0,
        max_retries=0, timeout=model.timeout_ms / 1000, tokenizer=tokenizer)
    frame = pd.DataFrame({'row_id': [v['source_example_id'] for v in values],
                          'review_text': [v['input_text'] for v in values]})
    previous_lm, previous_cache = lotus.settings.lm, lotus.settings.enable_cache
    lotus.settings.configure(lm=lm, enable_cache=False)
    def execute():
        result = frame.sem_map(plan.instruction + '\nInput: {review_text}', suffix='_answer')
        if lm.stats.cache_hits:
            raise ValueError('native LOTUS unexpectedly reused a cached response')
        yield from zip(result['row_id'], result['_answer'])
    prepared = PreparedRows(execute)
    try:
        yield prepared
    finally:
        prepared.close()
        lotus.settings.configure(lm=previous_lm, enable_cache=previous_cache)


@contextmanager
def _prepare_daft(values, plan, model, concurrency, num_threads):
    import daft
    from daft.ai.openai.provider import OpenAIProvider
    from daft.functions import prompt
    if daft.__version__ != '0.7.21':
        raise RuntimeError('semantic Map comparison requires Daft0.7.21')
    # Native SDK thread settings belong to the process and cannot be set twice.
    daft.set_runner_native(num_threads=num_threads)
    provider = OpenAIProvider(base_url=model.endpoint_url.removesuffix('/chat/completions'),
                              api_key=model.bearer_token or 'local-fixture')
    frame = daft.from_pydict({'row_id': [v['source_example_id'] for v in values],
                            'review_text': [v['input_text'] for v in values]})
    def execute():
        source_prompt = daft.lit(plan.instruction + '\n\nInput:\n') + daft.col('review_text')
        expression = prompt(source_prompt, provider=provider, model=model.model_id,
            use_chat_completions=True, temperature=0, max_tokens=plan.max_tokens,
            max_retries=0, on_error='raise', concurrency=concurrency)
        result = frame.with_column('output', expression).select('row_id', 'output')
        iterator = result.iter_rows()
        try:
            for row in iterator:
                yield row['row_id'], row['output']
        finally:
            close = getattr(iterator, 'close', None)
            if close is not None:
                close()
    prepared = PreparedRows(execute)
    try:
        yield prepared
    finally:
        prepared.close()


@contextmanager
def _prepare_duckdb(values, plan, model, concurrency):
    from src.baselines.text.products.duckdb_ai import (
        DuckDBAiConfig, prepare_duckdb_ai_projection)
    config = DuckDBAiConfig(
        endpoint_base_url=model.endpoint_url.removesuffix('/chat/completions'),
        model=model.model_id, api_key=model.bearer_token or 'local-fixture',
        max_tokens=plan.max_tokens, max_concurrent_requests=concurrency,
        timeout_seconds=max(1, model.timeout_ms // 1000))
    source = tuple((index, value['input_text']) for index, value in enumerate(values))
    with prepare_duckdb_ai_projection(source, plan.instruction, config) as projection:
        identity = projection.runtime_identity
        if identity['duckdb_version'] != 'v1.5.4' or identity['duckdb_ai_extension_version'] != '0.4.14':
            raise RuntimeError('semantic Map comparison requires DuckDB1.5.4/ai0.4.14')
        def execute():
            for index, output in projection.execute():
                yield values[index]['source_example_id'], output
        prepared = PreparedRows(execute)
        try:
            yield prepared
        finally:
            prepared.close()


@contextmanager
def prepare_rows(role, values, plan, model, *, concurrency, num_threads=8,
                 tokenizer_path=None, sema_binary=None, artifact_root=None):
    """Prepare validated raw rows; execute() owns query work and consumption.

    Source reads and raw relation construction finish before this context yields.
    Prompt assembly, native operators and their result iteration start only when
    execute() is consumed. No adapter scheduler or model warm-up is introduced.
    Daft owns its process-wide thread configuration: use one prepare per fresh
    driver and reuse execute(). A second prepare in that process is rejected by
    the native SDK rather than silently accepting an unknown thread setting.
    """
    if role not in ROLES or type(concurrency) is not int or not 1 <= concurrency <= 64:
        raise ValueError('unsupported semantic Map role or native concurrency')
    values = tuple(values)
    identities = [value['source_example_id'] for value in values]
    if (not values or len(values) > 4096 or len(set(identities)) != len(identities)
            or not model.endpoint_url.endswith('/chat/completions')):
        raise ValueError('prepared native Map requires bounded unique rows and a chat endpoint')
    if role == 'lotus-map':
        setup = _prepare_lotus(values, plan, model, concurrency, tokenizer_path)
    elif role == 'daft-prompt':
        setup = _prepare_daft(values, plan, model, concurrency, num_threads)
    elif role == 'duckdb-ai':
        setup = _prepare_duckdb(values, plan, model, concurrency)
    else:
        from src.baselines.text.products.sema import prepare_projection
        if sema_binary is None or artifact_root is None:
            raise ValueError('Sema requires the pinned binary and a private artifact directory')
        setup = prepare_projection(values, plan, model, binary=sema_binary,
                                   root=artifact_root, num_threads=num_threads)
    with setup as prepared:
        yield prepared
