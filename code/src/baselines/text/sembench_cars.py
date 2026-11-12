"""Original Cars text queries and their unmodified gold/metric implementation."""

from dataclasses import asdict
from functools import partial
from pathlib import Path
import tempfile
from types import SimpleNamespace

from .sembench_evaluation import csv_result_frame
from .sembench_source import SEMBENCH_COMMIT, load_module


TEXT_QUERY_IDS = ('1', '4', '10')
FULL_SCALE_FACTOR = 157376


def _query_id(query_id):
    query = str(query_id).lower().removeprefix('q')
    if query not in TEXT_QUERY_IDS:
        raise ValueError('supported Cars text queries are Q1, Q4 and Q10')
    return query


def text_files(scale_factor=FULL_SCALE_FACTOR):
    """The author's scale selects CSV filenames, not model-call cardinality."""
    if type(scale_factor) is not int or scale_factor < 1:
        raise ValueError('Cars scale_factor must be a positive integer')
    sample = 'data/sf_' + str(scale_factor) + '/'
    return (sample + 'car_data_' + str(scale_factor) + '.csv',
            sample + 'text_complaints_data_' + str(scale_factor) + '.csv',
            'data/full_data/car_data_full.csv',
            'data/full_data/text_complaints_data_full.csv')


def bind_original_query(checkout, query_id, load_data, *, data_dir, scale_factor=FULL_SCALE_FACTOR):
    """Bind the unchanged query; only its file reader is rebound."""
    import pandas as pd

    query = _query_id(query_id)
    text_files(scale_factor)
    module = load_module(checkout, 'sembench_cars_q' + query, 'files/cars/query/lotus/Q' + query + '.py')
    root = Path(data_dir).resolve()

    def read_csv(path, **kwargs):
        return load_data(Path(path).resolve().relative_to(root).as_posix(), **kwargs)

    module.pd = SimpleNamespace(read_csv=read_csv, DataFrame=pd.DataFrame)
    return partial(module.run, str(root), scale_factor)


def evaluate_original(checkout, query_id, result, frames, *, scale_factor=FULL_SCALE_FACTOR):
    """Run only the chosen original gold/evaluator; do not require audio/images.

    Preserve the author's full-label sampling and Q4's unique-car gold mean even
    when the query's complaint join weights a car multiple times.
    """
    query = int(_query_id(query_id))
    cars, complaints, full_cars, full_complaints = text_files(scale_factor)
    module = load_module(checkout, 'sembench_cars_evaluator', 'src/scenario/cars/evaluation/evaluate.py')
    evaluator = object.__new__(module.CarsEvaluator)
    evaluator.scale_factor = scale_factor
    evaluator.text_df = frames[full_complaints]
    if query != 1:
        evaluator.cars_df = frames[full_cars]
    if scale_factor != FULL_SCALE_FACTOR:
        evaluator.text_df = evaluator.text_df.loc[
            evaluator.text_df['complaint_id'].isin(frames[complaints]['complaint_id'])]
        if query != 1:
            evaluator.cars_df = evaluator.cars_df.loc[evaluator.cars_df['car_id'].isin(frames[cars]['car_id'])]
    with tempfile.TemporaryDirectory(prefix='sembench-cars-gold-') as temporary:
        evaluator._results_path = Path(temporary)
        truth = evaluator._get_ground_truth(query)
        metric = evaluator._evaluate_single_query(query, csv_result_frame(result), truth)
    return dict(original_metric=asdict(metric), sembench_commit=SEMBENCH_COMMIT)
