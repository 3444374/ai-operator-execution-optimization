"""Original SemBench LOTUS queries on PG rows, with native LM ownership."""
from contextlib import contextmanager
from dataclasses import replace
import importlib.metadata

from src.baselines.text.sembench_movie import run_original_lotus_query


def configure_lm(model, concurrency, *, tokenizer_path=None):
    import lotus
    from lotus.models import LM
    if importlib.metadata.version('lotus-ai') != '1.2.4':
        raise RuntimeError('this native query adapter requires the pinned LOTUS1.2.4')
    if not model.endpoint_url.endswith('/chat/completions'):
        raise ValueError('LOTUS baseline requires an OpenAI-compatible chat endpoint')
    tokenizer = None
    if tokenizer_path is not None:
        from tokenizers import Tokenizer
        tokenizer = Tokenizer.from_file(str(tokenizer_path))
    lm = LM('openai/'+model.model_id,api_base=model.endpoint_url[:-len('/chat/completions')],
            api_key=model.bearer_token or 'local-fixture',temperature=0,top_p=1,max_tokens=8,
            max_batch_size=concurrency,stop=['\n'],num_retries=0,max_retries=0,
            timeout=model.timeout_ms/1000,tokenizer=tokenizer)
    lotus.settings.configure(lm=lm,enable_cache=False)
    return lm


def load_reviews(connection, inputs):
    """Native LOTUS materializes its source frame; enforce the declared row bound."""
    import pandas as pd
    # Keep the original Q2/Q3 relational filter in its native query method.
    source = replace(inputs,movie_id=None)
    statement, parameters = source.select_sql()
    rows = []
    with connection.transaction():
        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        with connection.cursor() as cursor:
            for raw in cursor.stream(statement,parameters):
                source.convert(raw)
                if len(rows) == source.max_rows:
                    raise ValueError('native LOTUS source exceeds its declared relation size')
                rows.append((raw[4],raw[2],raw[3],raw[1]))
    return pd.DataFrame(rows,columns=['reviewId','id','reviewText','_execution_row_id'])


@contextmanager
def observe_filter_rows(record, record_prompts):
    """Record native operator input/kept IDs; preserve its exact arguments/result."""
    from lotus.sem_ops.sem_filter import SemFilterDataframe
    from lotus.models import LM
    original = SemFilterDataframe.__call__
    original_lm = LM.__call__
    active_ids = None
    def observed(accessor,*args,**kwargs):
        nonlocal active_ids
        original_ids = list(accessor._obj['_execution_row_id'])
        active_ids = original_ids
        try:
            result = original(accessor,*args,**kwargs)
        finally:
            active_ids = None
        kept = set(result['_execution_row_id'])
        if len(set(original_ids)) != len(original_ids) or not kept <= set(original_ids):
            raise ValueError('native Filter row identities are ambiguous')
        record([(identity,identity in kept) for identity in original_ids])
        return result
    def observed_lm(lm,messages,*args,**kwargs):
        if active_ids is None or len(messages)!=len(active_ids):
            raise ValueError('native query does not have the declared one-prompt-per-row shape')
        record_prompts([dict(row_id=identity,messages=prompt) for identity,prompt in zip(active_ids,messages)])
        return original_lm(lm,messages,*args,**kwargs)
    SemFilterDataframe.__call__ = observed
    LM.__call__ = observed_lm
    try:
        yield
    finally:
        SemFilterDataframe.__call__ = original
        LM.__call__ = original_lm


@contextmanager
def open_rows(connection, inputs, checkout, query_id, record_decisions, record_prompts):
    decisions=[]
    original_ids={}
    def load():
        frame=load_reviews(connection,inputs)
        original_ids.update(zip(frame['_execution_row_id'],frame['reviewId']))
        return frame
    def record(values):
        decisions.extend(values)
        record_decisions(values)
    def rows():
        with observe_filter_rows(record,record_prompts):
            result = run_original_lotus_query(checkout,query_id,load)
        if query_id==3:
            yield (int(result.iloc[0,0]),)
        else:
            # The pinned original method takes head(5) in input order and projects
            # reviewId, which is not unique in the upstream data. Retain occurrence
            # identity for auditing while checking its exact original output values.
            kept=[key for key,value in decisions if value][:5]
            returned=list(result.iloc[:,0]) if len(result.columns) else []
            if returned!=[original_ids[key] for key in kept]:
                raise ValueError('original LIMIT output differs from its retained row occurrences')
            yield from ((identity,) for identity in kept)
    yield rows()
