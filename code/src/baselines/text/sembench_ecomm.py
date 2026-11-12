"""Original E-Commerce queries and their authored SQL/accuracy metric.

Only the file reader is rebound. The official query owns nested-cell formatting,
ordinary selection and projection; existing native operator adapters execute it.
"""

from dataclasses import asdict
from functools import partial
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
import tomllib

from .sembench_source import SEMBENCH_COMMIT, load_module, verify_checkout
from src.infrastructure.private_artifacts import file_identity


TEXT_QUERY_IDS = ('3', '5', '7')
VISUAL_QUERY_IDS = ('10', '11', '12', '14')
TEXT_FILE = 'styles_details.parquet'


def _query_id(query_id):
    query = str(query_id).lower().removeprefix('q')
    if query not in (*TEXT_QUERY_IDS, *VISUAL_QUERY_IDS):
        raise ValueError('supported E-Commerce bindings are text Q3/Q5/Q7 and multimodal Q10/Q11/Q12/Q14')
    return query


def bind_original_query(checkout, query_id, load_data, *, data_dir):
    """Bind the author's function without executing any data or semantic work."""
    query = _query_id(query_id)
    module = load_module(checkout, 'sembench_ecomm_q' + query,
                         'files/ecomm/queries/dialects/lotus/q' + query + '.py')
    module._policy = 'exact'
    root = Path(data_dir).resolve()

    def read_parquet(path, **kwargs):
        return load_data(Path(path).resolve().relative_to(root).as_posix(), **kwargs)

    module.pd = SimpleNamespace(read_parquet=read_parquet)
    return partial(module.run, str(root))


def evaluate_original(checkout, query_id, result, *, data_dir, source_identity):
    """Run the author's gold SQL and the same GenericEvaluator used by Ecomm.

    EcommScenario.get_ground_truth also writes a cache into the upstream tree.
    Execute its unchanged SQL against the bound data without that cache write.
    """
    import duckdb
    import pandas as pd

    query = _query_id(query_id)
    data = Path(data_dir).resolve()
    expected = {key: source_identity[key] for key in ('sha256', 'bytes')}
    if file_identity(data / TEXT_FILE) != expected:
        raise ValueError('E-Commerce gold input changed after query preparation')
    root = verify_checkout(checkout)
    with (root / 'files/ecomm/queries' / ('q' + query + '.toml')).open('rb') as stream:
        definition = tomllib.load(stream)['definition']
    module = load_module(checkout, 'sembench_ecomm_generic_evaluator',
                         'src/evaluator/generic_evaluator.py')
    with duckdb.connect() as connection:
        connection.execute('SET file_search_path = ?', [str(data)])
        truth = connection.execute(definition['ground_truth']).df()
    if file_identity(data / TEXT_FILE) != expected:
        raise ValueError('E-Commerce gold input changed during evaluation')
    # Match GenericRunner.save_results(index=False) followed by the evaluator's
    # read_csv, including dtype/NA inference. Q14 returns string IDs while gold
    # SQL retains integer IDs; direct in-memory comparison changes its score.
    try:
        result = pd.read_csv(StringIO(result.to_csv(index=False)))
    except pd.errors.EmptyDataError:
        result = pd.DataFrame()
    metric = module.GenericEvaluator.compute_accuracy_score(
        definition['accuracy_metric'], truth, result)
    return dict(original_metric=asdict(metric), sembench_commit=SEMBENCH_COMMIT)


class _SourcePrepared(BaseException):
    """Stop before the original first semantic call; never simulate its output."""


def describe_visual_inputs(checkout, data_dir, query_id):
    """Bind the first real Filter frontier without simulating downstream results.

    The original function performs its own Parquet selection/merge. Capture only
    that first frontier; the real query later runs all native operators using
    actual intermediate relations. Only Q12 has the simple two-pass upper bound.
    """
    import pandas as pd
    from src.baselines.image.sembench_animals import image_identity
    from .sembench_text import _operators

    query = _query_id(query_id)
    if query not in VISUAL_QUERY_IDS:
        raise ValueError('this E-Commerce binding requires a multimodal query')
    data = Path(data_dir).resolve()
    files, source_rows, selected = {}, {}, []

    def load(name, **kwargs):
        path = data / name
        before = file_identity(path)
        frame = pd.read_parquet(path, **kwargs)
        if file_identity(path) != before:
            raise ValueError('E-Commerce source changed during preparation')
        files[name] = before
        source_rows[name] = len(frame)
        return frame

    def capture(kind, frame, _args, _kwargs, _invoke):
        if kind != 'filter':
            raise ValueError('the original multimodal source must precede its Filter')
        selected.append(frame)
        raise _SourcePrepared()

    execute = bind_original_query(checkout, query, load, data_dir=data)
    with _operators(capture):
        try:
            execute()
        except _SourcePrepared:
            pass
    frame, = selected
    images = [dict(path=str(data / 'images' / name), **image_identity(data / 'images' / name))
              for name in frame['filename']]
    return dict(schema='semloom.sembench_ecomm_visual_inputs.v1', query='Q' + query,
        sembench_commit=SEMBENCH_COMMIT, data_dir=str(data), files=files, source_rows=source_rows, images=images,
        filter_occurrences=len(frame), maximum_followup_extracts=len(frame) if query == '12' else 0,
        minimum_model_calls=len(frame), maximum_model_calls=2 * len(frame) if query == '12' else None,
        actual_followup_extracts=None if query == '12' else 0,
        source_indices=frame.index.tolist(), policy='unchanged complete native LOTUS query',
        budget_scope=('Filter over N original occurrences; Extract over actual M survivors, at most 2N'
                      if query == '12' else 'first Filter is a lower bound; later native branches/Join/TopK '
                      'depend on actual intermediate results; caller budget bounds all attempts'),
        model_calls=0, warmup_calls=0, retries=0)


def verify_visual_inputs(inputs):
    """Audit source identities after the full sink, outside query execution."""
    from src.baselines.image.sembench_animals import image_identity

    data = Path(inputs['data_dir'])
    for name, expected in inputs['files'].items():
        if file_identity(data / name) != expected:
            raise ValueError('E-Commerce source changed during the original query')
    for image in inputs['images']:
        if image_identity(image['path']) != {key: value for key, value in image.items() if key != 'path'}:
            raise ValueError('E-Commerce image changed during the original query')
