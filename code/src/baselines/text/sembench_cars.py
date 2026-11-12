"""Original Cars queries and their unmodified gold/metric implementation."""

from dataclasses import asdict
from functools import partial
from pathlib import Path
import tempfile
from types import SimpleNamespace

from .sembench_evaluation import csv_result_frame
from .sembench_source import SEMBENCH_COMMIT, load_module
from src.infrastructure.private_artifacts import file_identity


TEXT_QUERY_IDS = ('1', '4', '10')
VISUAL_QUERY_IDS = ('3', '8')
FULL_SCALE_FACTOR = 157376


def _query_id(query_id):
    query = str(query_id).lower().removeprefix('q')
    if query not in (*TEXT_QUERY_IDS, *VISUAL_QUERY_IDS):
        raise ValueError('the pinned Cars LOTUS queries are Q1/Q3/Q4/Q8/Q10; other query files are absent')
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
    """Run only the chosen original gold/evaluator and its required modalities.

    Preserve the author's full-label sampling and Q4's unique-car gold mean even
    when the query's complaint join weights a car multiple times.
    """
    query = int(_query_id(query_id))
    module = load_module(checkout, 'sembench_cars_evaluator', 'src/scenario/cars/evaluation/evaluate.py')
    evaluator = object.__new__(module.CarsEvaluator)
    evaluator.scale_factor = scale_factor
    if str(query) in VISUAL_QUERY_IDS:
        cars, images, full_cars, full_images = visual_files(scale_factor)
        evaluator.cars_df = frames[full_cars]
        evaluator.image_df = frames[full_images]
        if scale_factor != FULL_SCALE_FACTOR:
            evaluator.cars_df = evaluator.cars_df.loc[evaluator.cars_df['car_id'].isin(frames[cars]['car_id'])]
            evaluator.image_df = evaluator.image_df.loc[evaluator.image_df['image_id'].isin(frames[images]['image_id'])]
    else:
        cars, complaints, full_cars, full_complaints = text_files(scale_factor)
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


def visual_files(scale_factor=FULL_SCALE_FACTOR):
    cars, _, full_cars, _ = text_files(scale_factor)
    return (cars, 'data/sf_' + str(scale_factor) + '/image_car_data_' + str(scale_factor) + '.csv',
            full_cars, 'data/full_data/image_data_full.csv')


def describe_visual_inputs(checkout, data_dir, query_id, *, scale_factor=FULL_SCALE_FACTOR):
    import pandas as pd
    from src.baselines.image.sembench_animals import image_identity
    from .sembench_text import capture_first_operator

    query = _query_id(query_id)
    if query not in VISUAL_QUERY_IDS:
        raise ValueError('Cars visual binding supports the original Q3/Q8')
    data = Path(data_dir).resolve()
    files = {}

    def load(name, **kwargs):
        before = file_identity(data / name)
        frame = pd.read_csv(data / name, **kwargs)
        if file_identity(data / name) != before:
            raise ValueError('Cars source changed during preparation')
        files[name] = before
        return frame

    execute = bind_original_query(checkout, query, load, data_dir=data, scale_factor=scale_factor)
    kind, frame, _, _ = capture_first_operator(execute)
    if kind != 'filter':
        raise ValueError('the selected original Cars visual query must use Filter')
    images = [dict(path=str(path), **image_identity(path)) for path in frame['image_path']]
    gold = {name: file_identity(data / name) for name in visual_files(scale_factor)[2:]}
    return dict(schema='semloom.sembench_cars_visual_inputs.v1', query='Q' + query,
        sembench_commit=SEMBENCH_COMMIT, data_dir=str(data), scale_factor=scale_factor,
        files=files, evaluation_files=gold, images=images,
        source_indices=frame.index.tolist(), rows=len(frame), expected_model_calls=len(frame),
        model_calls=0, policy='original full Filter before authored LIMIT; no pre-truncation',
        warmup_calls=0, retries=0)


def _verify_files(inputs):
    data = Path(inputs['data_dir'])
    for name, expected in (inputs['files'] | inputs['evaluation_files']).items():
        if file_identity(data / name) != expected:
            raise ValueError('Cars source or gold input changed during the original query')


def verify_visual_inputs(inputs):
    from src.baselines.image.sembench_animals import image_identity

    _verify_files(inputs)
    for image in inputs['images']:
        if image_identity(image['path']) != {key: value for key, value in image.items() if key != 'path'}:
            raise ValueError('Cars image changed during the original query')


def evaluate_visual_original(checkout, query_id, result, inputs):
    import pandas as pd

    frames = {name: pd.read_csv(Path(inputs['data_dir']) / name)
              for name in inputs['files'] | inputs['evaluation_files']}
    _verify_files(inputs)
    return evaluate_original(checkout, query_id, result, frames, scale_factor=inputs['scale_factor'])
