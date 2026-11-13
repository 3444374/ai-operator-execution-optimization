"""Original Medical text/image queries, authored CSV preparation and evaluation.

The pinned query scripts own their prompts, patient join and result schemas.
This adapter binds existing CSVs/images without benchmark setup or downloads.
"""

import csv
from dataclasses import asdict
from functools import partial
from pathlib import Path
import shutil
import tempfile
from types import SimpleNamespace

from .sembench_source import SEMBENCH_COMMIT, load_module, verify_checkout
from src.infrastructure.private_artifacts import (
    file_identity, new_private_directory, write_private_json,
)


TEXT_QUERY_IDS = ('1', '4', '10')
VISUAL_QUERY_IDS = ('3', '8', '9')
FULL_SCALE_FACTOR = 11112


def _query_id(query_id, supported=TEXT_QUERY_IDS + VISUAL_QUERY_IDS):
    if type(query_id) not in (str, int):
        raise ValueError('Medical query_id must identify an original supported query')
    query = str(query_id).lower().removeprefix('q')
    if query not in supported:
        raise ValueError('unsupported Medical query: ' + str(query_id))
    return query


def text_files(scale_factor=FULL_SCALE_FACTOR):
    """Keep the original run(data_dir, scale_factor) filenames and patient scale."""
    if type(scale_factor) is not int or scale_factor < 1:
        raise ValueError('Medical scale_factor must be a positive integer')
    suffix = '' if scale_factor == FULL_SCALE_FACTOR else '_' + str(scale_factor)
    return tuple('data/' + name + suffix + '.csv' for name in
                 ('text_symptoms_data', 'patient_data', 'patient_data_with_labels'))


def _csv_identity(path):
    with path.open(encoding='utf-8', newline='') as stream:
        reader = csv.DictReader(stream)
        columns = reader.fieldnames
        rows = sum(1 for _ in reader)
    return dict(file_identity(path), rows=rows, columns=columns)


def prepare_text_inputs(checkout, source_dir, output_dir, scale_factor=FULL_SCALE_FACTOR):
    """Copy three existing authored CSVs verbatim; do not generate a new sample."""
    names = text_files(scale_factor)
    verify_checkout(checkout)
    source, output = Path(source_dir).resolve(), Path(output_dir).resolve()
    source_files = {name: _csv_identity(source / Path(name).name) for name in names}
    new_private_directory(output)
    (output / 'data').mkdir(mode=0o700)
    files = {}
    for name in names:
        target = output / name
        shutil.copyfile(source / Path(name).name, target)
        target.chmod(0o600)
        files[name] = _csv_identity(target)
        if files[name] != source_files[name]:
            raise ValueError('Medical source changed while its authored CSV was copied')
    report = dict(schema='semloom.medical_text_inputs.v1', sembench_commit=SEMBENCH_COMMIT,
        scale_factor=scale_factor, scale_unit='patient rows; model rows are query-specific',
        source_dir=str(source), output_dir=str(output), source_files=source_files, files=files,
        model_calls=0, downloaded=False, images_generated=False, audio_generated=False,
        sampling='existing authored CSV bytes; no sampling or multimodal preparation')
    write_private_json(output / 'manifest.json', report)
    return report


def run_original_query(checkout, query_id, load_data, *, data_dir,
                       scale_factor=FULL_SCALE_FACTOR):
    """Call the original standalone script with only its CSV reader rebound."""
    return bind_original_query(checkout, query_id, load_data, data_dir=data_dir,
                               scale_factor=scale_factor)()


def bind_original_query(checkout, query_id, load_data, *, data_dir,
                        scale_factor=FULL_SCALE_FACTOR):
    """Bind the unchanged script and reader once, outside query execution."""
    import pandas as pd

    query = _query_id(query_id)
    text_files(scale_factor)
    module = load_module(checkout, 'sembench_medical_q' + query,
                         'files/medical/query/lotus/Q' + query + '.py')
    root = Path(data_dir).resolve()

    def read_csv(path, **kwargs):
        name = Path(path).resolve().relative_to(root).as_posix()
        return load_data(name, **kwargs)

    # Each source module has its own binding; pandas and its native accessors
    # remain untouched outside the existing single-query operator scope.
    module.pd = SimpleNamespace(read_csv=read_csv, DataFrame=pd.DataFrame)
    return partial(module.run, str(root), scale_factor=scale_factor)


def evaluate_original(checkout, query_id, result, labeled_patients, symptoms_text=None, *,
                      scale_factor=FULL_SCALE_FACTOR):
    """Use original gold derivation and metrics without reading audio/image CSVs."""
    import pandas as pd

    query = int(_query_id(query_id))
    text_files(scale_factor)
    module = load_module(checkout, 'sembench_medical_evaluator',
                         'src/scenario/medical/evaluation/evaluate.py')
    # The official runner persists Series results before evaluation reloads a
    # DataFrame. Preserve the Series result itself and that evaluation shape.
    if isinstance(result, pd.Series):
        result = result.to_frame()
    evaluator = object.__new__(module.MedicalEvaluator)
    evaluator.patient_df = labeled_patients
    if symptoms_text is not None:
        evaluator.symptoms_text_df = symptoms_text
    evaluator.scale_factor = scale_factor
    # Original gold methods write a CSV cache. A fresh temporary path keeps
    # their implementation intact without modifying source or reusing stale gold.
    with tempfile.TemporaryDirectory(prefix='sembench-medical-gold-') as temporary:
        evaluator._results_path = Path(temporary)
        truth = evaluator._get_ground_truth(query)
        # Generated Q3 gold is also a Series; the official CSV reload presents
        # its named patient_id column to the unchanged evaluator.
        if isinstance(truth, pd.Series):
            truth = evaluator._get_ground_truth(query)
        metric = evaluator._evaluate_single_query(query, result, truth)
    return dict(original_metric=asdict(metric), sembench_commit=SEMBENCH_COMMIT)


def visual_files(query_id, scale_factor=FULL_SCALE_FACTOR):
    query = _query_id(query_id, VISUAL_QUERY_IDS)
    _, patients, labels = text_files(scale_factor)
    suffix = '' if scale_factor == FULL_SCALE_FACTOR else '_' + str(scale_factor)
    modalities = ('image_x_ray_data',) if query == '3' else ('image_skin_data',) if query == '8' else (
        'image_skin_data', 'image_x_ray_data')
    return (patients, *(f'data/{name}{suffix}.csv' for name in modalities)), labels


def describe_visual_inputs(checkout, data_dir, query_id, *, scale_factor=FULL_SCALE_FACTOR):
    """Count the original pre-LIMIT relation, including repeated image occurrences."""
    import pandas as pd
    from lotus.dtype_extensions import ImageArray
    from src.baselines.image.sembench_animals import image_identity
    from .sembench_text import capture_first_operator

    query = _query_id(query_id, VISUAL_QUERY_IDS)
    _, labels = visual_files(query, scale_factor)
    data, files = Path(data_dir).resolve(), {}

    def load(name, **kwargs):
        before = file_identity(data / name)
        frame = pd.read_csv(data / name, **kwargs)
        if file_identity(data / name) != before:
            raise ValueError('Medical source changed during preparation')
        files[name] = before
        return frame

    execute = bind_original_query(checkout, query, load, data_dir=data, scale_factor=scale_factor)
    kind, frame, _, _ = capture_first_operator(execute)
    if kind != 'filter':
        raise ValueError('the selected original Medical visual query must use Filter')
    columns = [name for name in frame.columns if isinstance(frame[name].array, ImageArray)]
    images = [dict(path=str(path), **image_identity(path)) for name in columns for path in frame[name]]
    return dict(schema='semloom.sembench_medical_visual_inputs.v1', query='Q' + query,
        sembench_commit=SEMBENCH_COMMIT, data_dir=str(data), scale_factor=scale_factor,
        files=files, evaluation_files={labels: file_identity(data / labels)},
        images=images, image_columns=columns, images_per_occurrence=len(columns),
        source_indices=frame.index.tolist(), rows=len(frame), expected_model_calls=len(frame),
        scale_unit='authored patient CSV suffix; model calls follow original joins before LIMIT',
        model_calls=0, warmup_calls=0, retries=0,
        policy='original image Filter and relational sink; multiple images stay in one occurrence')


def verify_visual_inputs(inputs):
    from src.baselines.image.sembench_animals import image_identity

    data = Path(inputs['data_dir'])
    for name, expected in (inputs['files'] | inputs['evaluation_files']).items():
        if file_identity(data / name) != expected:
            raise ValueError('Medical source or gold input changed during the original query')
    for image in inputs['images']:
        if image_identity(image['path']) != {key: value for key, value in image.items() if key != 'path'}:
            raise ValueError('Medical image changed during the original query')


def evaluate_visual_original(checkout, query_id, result, inputs):
    import pandas as pd

    labels, = inputs['evaluation_files']
    patients = pd.read_csv(Path(inputs['data_dir']) / labels)
    if file_identity(Path(inputs['data_dir']) / labels) != inputs['evaluation_files'][labels]:
        raise ValueError('Medical gold input changed during evaluation')
    return evaluate_original(checkout, query_id, result, patients, scale_factor=inputs['scale_factor'])
