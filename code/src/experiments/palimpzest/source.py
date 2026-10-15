"""Read exact upstream checkouts and extract only the original Movie method."""
import ast
import importlib
from pathlib import Path
import subprocess
import sys
import threading
from types import SimpleNamespace

from src.baselines.text.sembench_movie import SEMBENCH_COMMIT, verify_checkout
from src.experiments.postgresql.query_workloads import file_identity


PALIMPZEST_COMMIT = '6efc19841c49d727ce42f5c2dd55e7433601d2d9'
PALIMPZEST_TAG = '0.8.2.sem_agg'
MOVIE_SOURCE = 'src/scenario/movie/runner/palimpzest_runner/palimpzest_runner.py'
_run_lock = threading.Lock()


def verify_palimpzest_checkout(checkout):
    root = Path(checkout).resolve()
    git = ['git', '-c', 'safe.directory='+str(root), '-C', str(root)]
    commit = subprocess.check_output(git+['rev-parse', 'HEAD'], text=True, timeout=10).strip()
    dirty = subprocess.check_output(git+['status', '--porcelain', '--untracked-files=all',
                                      '--', 'src', 'pyproject.toml'], text=True, timeout=10)
    if commit != PALIMPZEST_COMMIT or dirty:
        raise ValueError('Palimpzest checkout differs from its pinned source')
    return root


def source_identity(palimpzest_checkout, sembench_checkout, query_id):
    if type(query_id) is not int or query_id not in (2, 3):
        raise ValueError('this PZ slice supports Movie Q3 and optional Q2 only')
    pz_root, bench = verify_palimpzest_checkout(palimpzest_checkout), verify_checkout(sembench_checkout)
    dirty = subprocess.check_output(['git', '-c', 'safe.directory='+str(bench), '-C', str(bench),
        'status', '--porcelain', '--untracked-files=all', '--', 'src', 'files/movie/query',
        'requirements.txt', 'requirements/palimpzest.txt'], text=True, timeout=10)
    if dirty:
        raise ValueError('SemBench source or dependency manifest differs from its pinned checkout')
    pz_files = ['pyproject.toml', 'src/palimpzest/constants.py',
                'src/palimpzest/query/generators/generators.py',
                'src/palimpzest/query/processor/config.py',
                'src/palimpzest/query/processor/query_processor.py',
                'src/palimpzest/query/processor/query_processor_factory.py',
                'src/palimpzest/query/execution/parallel_execution_strategy.py']
    bench_files = [MOVIE_SOURCE, 'src/runner/generic_palimpzest_runner/generic_palimpzest_runner.py',
                   'requirements.txt', 'requirements/palimpzest.txt',
                   'src/scenario/movie/evaluation/evaluate.py',
                   f'files/movie/query/gold_sql/Q{query_id}.sql']
    return dict(palimpzest_commit=PALIMPZEST_COMMIT, palimpzest_tag=PALIMPZEST_TAG,
                sembench_commit=SEMBENCH_COMMIT, query_id=query_id,
                palimpzest_files={name: file_identity(pz_root/name) for name in pz_files},
                sembench_files={name: file_identity(bench/name) for name in bench_files},
                method_scope='unchanged original Movie method; only Dataset.run endpoint is captured',
                generic_model_mapping_used=False)


def load_palimpzest(checkout):
    source = verify_palimpzest_checkout(checkout)/'src'
    for name, module in list(sys.modules.items()):
        if name == 'palimpzest' or name.startswith('palimpzest.'):
            filename = getattr(module, '__file__', None)
            if filename is not None and not Path(filename).resolve().is_relative_to(source):
                raise RuntimeError('another Palimpzest source is loaded; use an isolated process')
    sys.path.insert(0, str(source))
    try:
        return importlib.import_module('palimpzest')
    finally:
        sys.path.remove(str(source))


def build_movie_query(checkout, pz, query_id, rows, config):
    """Run only the pinned query body, capturing its terminal .run(config) call.

    Avoid importing the generic runner: it mutates global LiteLLM settings and its
    constructor initializes remote model defaults. No operator body is rewritten.
    The caller invokes the captured dataset's original run or fixed-plan boundary.
    """
    if type(query_id) is not int or query_id not in (2, 3):
        raise ValueError('only original Movie Q3 and optional Q2 are supported')
    root = verify_checkout(checkout)
    module = ast.parse((root/MOVIE_SOURCE).read_text())
    owner_class = next(node for node in module.body if isinstance(node, ast.ClassDef)
                       and node.name == 'PalimpzestRunner')
    method = next(node for node in owner_class.body if isinstance(node, ast.FunctionDef)
                  and node.name == f'_execute_q{query_id}')
    namespace = {'pz': pz, 'DataRecordCollection': object}
    exec(compile(ast.Module(body=[method], type_ignores=[]), str(root/MOVIE_SOURCE), 'exec'), namespace)
    import pandas as pd
    frame = pd.DataFrame([(r['review_id'], r['movie_id'], r['text']) for r in rows],
                         columns=['reviewId', 'id', 'reviewText'])

    def load_data(name):
        if name != 'Reviews.csv':
            raise ValueError('original PZ method requested an unexpected source')
        return frame

    captured = []
    original_run = pz.Dataset.run

    def capture(dataset, selected_config=None, **kwargs):
        if selected_config is not config or kwargs or captured:
            raise ValueError('original PZ method changed its single explicit run(config) endpoint')
        captured.append(dataset)
        return dataset

    if not _run_lock.acquire(blocking=False):
        raise RuntimeError('original Movie construction must run in an isolated query process')
    try:
        pz.Dataset.run = capture
        returned = namespace[method.name](SimpleNamespace(load_data=load_data,
                                                         palimpzest_config=lambda: config))
    finally:
        pz.Dataset.run = original_run
        _run_lock.release()
    if len(captured) != 1 or returned is not captured[0]:
        raise ValueError('original PZ method did not return its captured dataset run')
    return captured[0]
