"""Original E-Commerce text queries and their authored SQL/accuracy metric.

Only the file reader is rebound. The official query owns nested-cell formatting,
ordinary selection and projection; existing native operator adapters execute it.
"""

from dataclasses import asdict
from functools import partial
from pathlib import Path
from types import SimpleNamespace
import tomllib

from .sembench_source import SEMBENCH_COMMIT, load_module, verify_checkout
from src.infrastructure.private_artifacts import file_identity


TEXT_QUERY_IDS = ('3', '5')
TEXT_FILE = 'styles_details.parquet'


def _text_query_id(query_id):
    query = str(query_id).lower().removeprefix('q')
    if query not in TEXT_QUERY_IDS:
        raise ValueError('supported E-Commerce text queries are Q3 and Q5')
    return query


def bind_original_query(checkout, query_id, load_data, *, data_dir):
    """Bind the author's function without executing any data or semantic work."""
    query = _text_query_id(query_id)
    module = load_module(checkout, 'sembench_ecomm_q' + query,
                         'files/ecomm/queries/dialects/lotus/q' + query + '.py')
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

    query = _text_query_id(query_id)
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
    # EcommEvaluator._evaluate_single_query delegates directly to this method.
    metric = module.GenericEvaluator.compute_accuracy_score(
        definition['accuracy_metric'], truth, result)
    return dict(original_metric=asdict(metric), sembench_commit=SEMBENCH_COMMIT)
