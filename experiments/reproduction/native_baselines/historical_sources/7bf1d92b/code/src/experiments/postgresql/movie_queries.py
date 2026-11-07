"""SQL for the first Movie tasks and PG-owned Filter decision verification."""
import json

from src.baselines.text.sembench_movie import MOVIE_FILTER_INSTRUCTION
from .filter_bindings import verify_filter_decisions as _verify_filter_decisions


def movie_statement(inputs, query_id, model_id):
    from psycopg import sql
    if inputs.kind != 'movie' or query_id not in (1,2,3):
        raise ValueError('unsupported Movie SQL task')
    projection = sql.SQL('count(*)') if query_id==3 else sql.Identifier('row_id')
    statement = sql.SQL('SELECT {} FROM ONLY {} WHERE ').format(projection,sql.Identifier(inputs.table))
    if query_id in (2,3):
        statement += sql.SQL("movie_id='taken_3' AND ")
    statement += sql.SQL('ai_semantic.filter(review_text,{},{}::jsonb)').format(
        sql.Literal(MOVIE_FILTER_INSTRUCTION),
        sql.Literal(json.dumps(dict(model=model_id,temperature=0,max_tokens=8))))
    if query_id in (1,2):
        statement += sql.SQL(' LIMIT 5')
    return statement


def verify_filter_decisions(lines, raw_inputs, model_id, *, complete):
    """Compatibility entry point for the existing Movie evaluation contract."""
    return _verify_filter_decisions(lines, raw_inputs, model_id, complete=complete,
                                    instruction=MOVIE_FILTER_INSTRUCTION)
