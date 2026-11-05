"""Pinned SemBench Movie query/evaluator entry points and separate row audits.

The public checkout supplies unchanged query methods and original evaluators.
This adapter supplies raw PG input and result schemas; it owns no scheduler.
"""
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace

from .sembench_source import (
    SEMBENCH_COMMIT as SEMBENCH_COMMIT,
    load_module as _load,
    verify_checkout as verify_checkout,
)

def run_original_lotus_query(checkout, query_id, load_reviews):
    """Caller configures the native LM; query code loads PG rows after release."""
    if query_id not in (1,2,3,4,8,10):
        raise ValueError('supported original Movie queries are Q1/Q2/Q3/Q4/Q8/Q10')
    import pandas as pd
    module = _load(checkout,'sembench_movie_lotus',
                   'src/scenario/movie/runner/lotus_runner/lotus_runner.py')
    def load_data(name):
        if name != 'Reviews.csv':
            raise ValueError('unexpected source requested by original Movie query')
        return load_reviews()
    owner = SimpleNamespace(load_data=load_data,_get_empty_results_dataframe=lambda _:pd.DataFrame())
    # Do not call the benchmark constructor: its setup/warmup/approximate-model
    # initialization belongs outside query execution and this fixed-model task.
    return getattr(module.LotusRunner,f'_execute_q{query_id}')(owner)


def evaluate_original(checkout, query_id, system_results, labeled_reviews, *, labeled_movies=None):
    """Use original gold SQL and original Movie evaluator, only after execution."""
    if query_id not in (1,2,3,4,8,10):
        raise ValueError('unsupported original Movie evaluator')
    if query_id == 10 and labeled_movies is None:
        raise ValueError('Movie Q10 requires original Movies.csv audienceScore ground truth')
    import duckdb
    module = _load(checkout,'sembench_movie_evaluator','src/scenario/movie/evaluation/evaluate.py')
    with duckdb.connect() as connection:
        connection.register('Reviews',labeled_reviews)
        if query_id == 10:
            connection.register('Movies',labeled_movies)
        statement = (Path(checkout)/f'files/movie/query/gold_sql/Q{query_id}.sql').read_text()
        ground_truth = connection.execute(statement).fetchdf()
    evaluator = object.__new__(module.MovieEvaluator)
    metric = getattr(evaluator,f'_evaluate_q{query_id}')(system_results,ground_truth)
    result = dict(original_metric=asdict(metric),sembench_commit=SEMBENCH_COMMIT)
    if query_id in (1,2):
        returned = list(system_results.iloc[:,0]) if len(system_results.columns) else []
        truth = set(ground_truth.iloc[:,0])
        result['supplement'] = dict(returned_rows=len(returned),unique_rows=len(set(returned)),
            duplicates=len(returned)-len(set(returned)),invalid_rows=sum(row not in truth for row in returned),
            required_rows=min(5,len(truth)),limit_exceeded=len(returned)>5)
    return result


def classification_audit(labels, predictions, *, require_complete=True):
    """Expose false positives/negatives even when their COUNT errors cancel."""
    predictions = list(predictions)
    values = dict(predictions)
    if len(values) != len(predictions) or set(values)-set(labels):
        raise ValueError('duplicate or unknown prediction identity')
    if require_complete and set(values) != set(labels):
        raise ValueError('incomplete classification audit')
    if any(label not in ('POSITIVE','NEGATIVE') for label in labels.values()):
        raise ValueError('unsupported reference sentiment')
    counts = dict(true_positive=0,false_positive=0,true_negative=0,false_negative=0,invalid=0)
    for identity,value in values.items():
        if value not in ('POSITIVE','NEGATIVE'):
            counts['invalid'] += 1
        elif value == 'POSITIVE':
            counts['true_positive' if labels[identity]=='POSITIVE' else 'false_positive'] += 1
        else:
            counts['true_negative' if labels[identity]=='NEGATIVE' else 'false_negative'] += 1
    return dict(**counts,evaluated_rows=len(values),missing_rows=len(labels)-len(values),
                count_error_from_classifications=sum(v=='POSITIVE' for v in values.values())-
                    sum(labels[identity]=='POSITIVE' for identity in values))
